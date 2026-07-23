"""v1.2.26 Task 1a: PK-range partitioning for multi-pod intra-table parallelism.

Pure helpers (``naive_numeric_ranges``, ``ranges_from_splits``) plus the
DB-touching ``partition_pk_ranges`` entry point. Kept in a standalone module
(rather than inline in ``connections.py``) so the partition math is unit-
testable without spinning up the FastAPI app / DB.

The producer (``connections._enqueue_initial_load_tasks``) calls
``partition_pk_ranges`` once per stream to split the table's [min(pk), max(pk)]
range into K disjoint sub-ranges with roughly equal row counts, then enqueues
one ``initial_load`` task per range so KEDA can scale the transform-worker to
K concurrent pods (true intra-table parallelism).
"""
from __future__ import annotations

import logging
import os
from typing import Any, Optional

log = logging.getLogger(__name__)

# K clamp + default parallelism (mirrors connections.py module constants so
# this module is self-contained / importable without the app).
MAX_PARALLELISM = 16
DEFAULT_PARALLELISM = max(1, min(MAX_PARALLELISM, int(os.environ.get(
    "INITIAL_LOAD_DEFAULT_PARALLELISM", "4"))))
# Tables above this row count use approximate-percentile PK sampling; smaller
# tables use a naive even split of the numeric range.
PARTITION_SAMPLE_THRESHOLD = 1_000_000


def clamp_parallelism(k: Any) -> int:
    """Clamp a per-connection parallelism value to [1, MAX_PARALLELISM]."""
    try:
        k = int(k)
    except (TypeError, ValueError):
        k = DEFAULT_PARALLELISM
    return max(1, min(MAX_PARALLELISM, k))


def naive_numeric_ranges(mn, mx, k: int) -> list[tuple]:
    """Evenly split the numeric [mn, mx] range into K disjoint sub-ranges.

    Returns K ``(start, end)`` tuples. The first range's start is ``None``
    (open lower bound) and the last range's end is ``None`` (open upper bound)
    so the worker's ``WHERE pk > last_pk`` resume logic works at the
    boundaries. Interior bounds are inclusive on both ends — the worker
    fetches ``pk > last_pk``, so the next range's start equals the previous
    range's max and the worker naturally picks up at the boundary (no gaps,
    no overlaps).
    """
    if k <= 1 or mn is None or mx is None:
        return [(None, None)]
    if mx <= mn:
        return [(None, None)]
    span = (mx - mn) / k
    ranges: list[tuple] = []
    for i in range(k):
        start = None if i == 0 else mn + int(span * i)
        end = None if i == k - 1 else mn + int(span * (i + 1))
        ranges.append((start, end))
    return ranges


def ranges_from_splits(mn, mx, splits: list) -> list[tuple]:
    """Build K disjoint (start, end) ranges from min, max and K-1 split PKs.

    The first range starts at ``None`` (open) and the last ends at ``None``
    (open). Interior boundaries use the split PKs: range i ends at
    ``splits[i-1]`` and range i+1 starts at ``splits[i-1]`` (the worker's
    ``pk > last_pk`` advances past the boundary, so adjacent ranges are
    disjoint and cover the full space with no gaps or overlaps).
    """
    if not splits:
        return [(None, None)]
    if mn is None or mx is None:
        return [(None, None)]
    ranges: list[tuple] = [(None, splits[0])]
    for i in range(1, len(splits)):
        ranges.append((splits[i - 1], splits[i]))
    ranges.append((splits[-1], None))
    return ranges


def partition_pk_ranges(source: dict, schema_name: str, table_name: str,
                         pk_col: str, ctype: str, k: int) -> list[tuple]:
    """Partition a table's [min(pk), max(pk)] range into K disjoint sub-ranges.

    Returns a list of K ``(pk_start, pk_end)`` tuples (inclusive bounds) that
    together cover the full PK space. ``pk_start`` of the first range and
    ``pk_end`` of the last range are ``None`` (open bounds). On any error or
    when K<=1, returns ``[(None, None)]`` (single unbounded range — legacy
    v1.2.25 behaviour).

    Strategy:
      - MySQL/Postgres: ``SELECT MIN(pk), MAX(pk), COUNT(*)`` (one query).
        When count > PARTITION_SAMPLE_THRESHOLD, approximate-percentile
        sampling (``SELECT pk ... LIMIT 1 OFFSET o`` at K-1 evenly-spaced
        offsets) builds robust split points (resilient to PK gaps from
        deletes). Otherwise naive even split of the numeric range.
      - MongoDB: ObjectId is not numeric and lexicographic splitting skews,
        so K=1 (single range) — the worker still chunks internally on _id.
        Inter-table parallelism still applies.
    """
    if k <= 1:
        return [(None, None)]
    if not source or not table_name:
        return [(None, None)]
    host = source.get("host") or ""
    database = source.get("database_name") or source.get("database") or ""
    user = source.get("username") or source.get("user") or ""
    password = source.get("password") or ""
    port = source.get("port")
    if not host or not database:
        return [(None, None)]

    try:
        if ctype in ("postgres", "postgresql"):
            return _partition_pg(host, port or 5432, database, user, password,
                                  schema_name, table_name, pk_col, k)
        if ctype == "mysql":
            return _partition_mysql(host, port or 3306, database, user, password,
                                      schema_name, table_name, pk_col, k)
        if ctype == "mongodb":
            log.info("partition: mongodb _id is non-numeric — using K=1 for %s.%s",
                     schema_name, table_name)
            return [(None, None)]
    except Exception as exc:
        log.warning("partition: failed to partition %s.%s (k=%d): %s — falling back to single range",
                     schema_name, table_name, k, exc, exc_info=True)
    return [(None, None)]


def _partition_mysql(host, port, database, user, password,
                     schema_name, table_name, pk_col, k) -> list[tuple]:
    import pymysql
    qualified = (f"`{schema_name}`.`{table_name}`"
                 if schema_name else f"`{table_name}`")
    pk_q = f"`{pk_col}`"
    with pymysql.connect(host=host, port=int(port), database=database,
                          user=user, password=password,
                          connect_timeout=10, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MIN({pk_q}), MAX({pk_q}), COUNT(*) FROM {qualified}")
            mn, mx, cnt = cur.fetchone()
            if mn is None or mx is None or not cnt or k <= 1:
                return [(None, None)]
            if cnt <= PARTITION_SAMPLE_THRESHOLD:
                return naive_numeric_ranges(mn, mx, k)
            splits = _sample_pk_offsets_mysql(cur, qualified, pk_q, cnt, k)
            return ranges_from_splits(mn, mx, splits)
    return [(None, None)]


def _partition_pg(host, port, database, user, password,
                  schema_name, table_name, pk_col, k) -> list[tuple]:
    import psycopg2
    import psycopg2.extras
    qualified = (f'"{schema_name}"."{table_name}"'
                 if schema_name else f'"{table_name}"')
    pk_q = f'"{pk_col}"'
    with psycopg2.connect(host=host, port=port, dbname=database,
                          user=user, password=password,
                          connect_timeout=10,
                          application_name="fusion-cdc-partition") as conn:
        conn.autocommit = True
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("BEGIN READ ONLY")
            try:
                cur.execute(f"SELECT MIN({pk_q}), MAX({pk_q}), COUNT(*) FROM {qualified}")
                row = cur.fetchone()
                mn, mx, cnt = row["min"], row["max"], row["count"]
                if mn is None or mx is None or not cnt or k <= 1:
                    return [(None, None)]
                if cnt <= PARTITION_SAMPLE_THRESHOLD:
                    return naive_numeric_ranges(mn, mx, k)
                splits = _sample_pk_offsets_pg(cur, qualified, pk_q, cnt, k)
                return ranges_from_splits(mn, mx, splits)
            finally:
                cur.execute("COMMIT")
    return [(None, None)]


def _sample_pk_offsets_mysql(cur, qualified, pk_q, cnt, k) -> list:
    splits: list = []
    for i in range(1, k):
        offset = int((cnt * i) / k)
        if offset <= 0 or offset >= cnt:
            continue
        cur.execute(f"SELECT {pk_q} FROM {qualified} ORDER BY {pk_q} ASC LIMIT 1 OFFSET %s",
                    (offset,))
        row = cur.fetchone()
        if row and row[0] is not None:
            splits.append(row[0])
    return splits


def _sample_pk_offsets_pg(cur, qualified, pk_q, cnt, k) -> list:
    splits: list = []
    for i in range(1, k):
        offset = int((cnt * i) / k)
        if offset <= 0 or offset >= cnt:
            continue
        cur.execute(f"SELECT {pk_q} AS pk FROM {qualified} ORDER BY {pk_q} ASC LIMIT 1 OFFSET %s",
                    (offset,))
        row = cur.fetchone()
        if row and row["pk"] is not None:
            splits.append(row["pk"])
    return splits
