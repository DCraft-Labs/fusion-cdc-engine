"""v1.2.26 Task 1a / v1.2.27 P0 fix: PK-range partitioning for multi-pod
intra-table parallelism.

Pure helpers (``naive_numeric_ranges``, ``ranges_from_splits``) plus the
DB-touching ``partition_pk_ranges`` entry point. Kept in a standalone module
(rather than inline in ``connections.py``) so the partition math is unit-
testable without spinning up the FastAPI app / DB.

The producer (``connections._enqueue_initial_load_tasks``) calls
``partition_pk_ranges`` once per stream to split the table's [min(pk), max(pk)]
range into K disjoint sub-ranges with roughly equal row counts, then enqueues
one ``initial_load`` task per range so KEDA can scale the transform-worker to
K concurrent pods (true intra-table parallelism).

v1.2.27 P0 fix — non-blocking partitioning (production MySQL was tied up by a
stuck ``COUNT(*)`` on a 118M-row table for 20+ minutes, blocking the uvicorn
event loop with ``--workers 1``):
  - **No ``COUNT(*)`` / ``count_documents({})`` ever** — use
    ``information_schema.tables.table_rows`` (MySQL), ``pg_class.reltuples``
    (Postgres), ``db.collection.estimatedDocumentCount()`` (Mongo). All instant
    metadata lookups; the approximate count is good enough for partition math.
  - ``MIN(pk)`` / ``MAX(pk)`` only (uses the PK index's first/last leaf —
    instant on indexed PKs). No ``OFFSET`` sampling (``OFFSET 50M`` scans 50M
    rows).
  - Server-side timeout (``MAX_EXECUTION_TIME(30000)`` / ``statement_timeout``
    / ``maxTimeMS(30000)``) + client ``read_timeout=30``.
  - On timeout: **KILL the stuck query on the source** (``KILL CONNECTION
    <conn_id>`` / ``pg_terminate_backend(pid)`` / ``db.killOp(opid)``) to free
    the production DB, then fall back to plan B (first/last PK via
    ``ORDER BY pk LIMIT 1`` + information_schema count).
  - No PK index → K=1 with a warning (chunked fetch via v1.2.17 keyset
    pagination still works, just no parallelism).
  - The whole partitioning step is offloaded to a threadpool by the caller
    (``await asyncio.to_thread(partition_pk_ranges, ...)`` in
    ``connections._enqueue_initial_load_tasks``) so the uvicorn event loop
    stays responsive (health, login, other requests are served concurrently).
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
# tables use a naive even split of the numeric range. (v1.2.27: sampling is
# disabled — OFFSET on large tables is itself a slow scan. Kept for reference
# and unit-test compat.)
PARTITION_SAMPLE_THRESHOLD = 1_000_000

# v1.2.27: server-side + client-side timeouts for partitioning queries. The
# whole partitioning step must complete in < 60s or fall back to plan B.
PARTITION_QUERY_TIMEOUT_MS = 30_000
PARTITION_READ_TIMEOUT_S = 30
PARTITION_CONNECT_TIMEOUT_S = 10


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

    v1.2.27 strategy (non-blocking, no ``COUNT(*)``):
      - MySQL/Postgres: ``SELECT MIN(pk), MAX(pk) FROM table`` (instant on
        indexed PKs) + ``information_schema.tables.table_rows`` /
        ``pg_class.reltuples`` for the approximate count (instant). Naive even
        split of [min, max] into K ranges. Server-side timeout
        (``MAX_EXECUTION_TIME(30000)`` / ``statement_timeout=30s``) +
        client ``read_timeout=30``. On timeout: KILL the stuck query on the
        source, fall back to first/last PK via ``ORDER BY pk LIMIT 1``. If
        the PK has no index → K=1 with a warning.
      - MongoDB: ``_id`` is non-numeric and lexicographic splitting skews, so
        K=1 (single range) — the worker still chunks internally on ``_id``.
        ``estimatedDocumentCount()`` is used for the count log line (instant,
        metadata-based) — never ``count_documents({})``. Inter-table
        parallelism still applies.
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


# ===========================
# MySQL
# ===========================

def _mysql_has_pk_index(cur, schema_name: str, table_name: str, pk_col: str) -> bool:
    """Check whether ``table`` has an index on ``pk_col`` (PRIMARY or a unique
    index). Without one, ``MIN(pk)``/``MAX(pk)`` would full-scan the table —
    we fall back to K=1 instead."""
    cur.execute(
        "SELECT 1 FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s "
        "AND column_name = %s AND index_name = 'PRIMARY' "
        "LIMIT 1",
        (schema_name or None, table_name, pk_col),
    )
    if cur.fetchone():
        return True
    # Fallback: any index whose first column is pk_col.
    cur.execute(
        "SELECT 1 FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s "
        "AND column_name = %s AND seq_in_index = 1 "
        "LIMIT 1",
        (schema_name or None, table_name, pk_col),
    )
    return cur.fetchone() is not None


def _mysql_conn_id(cur) -> Optional[int]:
    """Return the server-side connection id of ``cur``'s connection, used to
    KILL the stuck query if it times out."""
    try:
        cur.execute("SELECT CONNECTION_ID()")
        row = cur.fetchone()
        if row:
            return int(row[0])
    except Exception:
        pass
    return None


def _mysql_kill(host, port, database, user, password, conn_id: int) -> None:
    """KILL the stuck query on ``conn_id`` from a *fresh* connection (you
    cannot KILL on the same connection that's stuck). Best-effort — logged on
    failure. Uses ``KILL QUERY`` (kills only the running statement, leaves the
    connection alive) to be conservative; the stuck connection is discarded by
    the caller anyway."""
    try:
        import pymysql
        with pymysql.connect(host=host, port=int(port), database=database,
                              user=user, password=password,
                              connect_timeout=PARTITION_CONNECT_TIMEOUT_S,
                              read_timeout=5, autocommit=True) as kill_conn:
            with kill_conn.cursor() as kcur:
                kcur.execute("KILL QUERY %s", (conn_id,))
        log.warning("partition: KILL QUERY %s succeeded (mysql %s.%s)",
                    conn_id, database, host)
    except Exception as exc:
        log.warning("partition: KILL QUERY %s failed: %s", conn_id, exc)


def _mysql_approx_count(host, port, database, user, password,
                        schema_name: str, table_name: str) -> Optional[int]:
    """Instant metadata lookup — ``information_schema.tables.table_rows`` is
    an approximate row count maintained by the InnoDB stats. Good enough for
    partition math (we only need a rough K-way split)."""
    try:
        import pymysql
        with pymysql.connect(host=host, port=int(port), database=database,
                              user=user, password=password,
                              connect_timeout=PARTITION_CONNECT_TIMEOUT_S,
                              read_timeout=PARTITION_READ_TIMEOUT_S,
                              autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT table_rows FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_name = %s",
                    (schema_name or database, table_name),
                )
                row = cur.fetchone()
                if row and row[0] is not None:
                    return int(row[0])
    except Exception as exc:
        log.warning("partition: information_schema.tables.table_rows lookup "
                    "failed for %s.%s: %s", schema_name, table_name, exc)
    return None


def _partition_mysql(host, port, database, user, password,
                     schema_name, table_name, pk_col, k) -> list[tuple]:
    import pymysql
    qualified = (f"`{schema_name}`.`{table_name}`"
                 if schema_name else f"`{table_name}`")
    pk_q = f"`{pk_col}`"
    timeout_hint = PARTITION_QUERY_TIMEOUT_MS

    # Step 1: PK index check. No index → K=1 (chunked fetch still works via
    # keyset pagination, just no parallelism).
    with pymysql.connect(host=host, port=int(port), database=database,
                          user=user, password=password,
                          connect_timeout=PARTITION_CONNECT_TIMEOUT_S,
                          read_timeout=PARTITION_READ_TIMEOUT_S,
                          autocommit=True) as conn:
        with conn.cursor() as cur:
            if not _mysql_has_pk_index(cur, schema_name, table_name, pk_col):
                log.warning(
                    "partition: table %s.%s has no PK index on %s; falling "
                    "back to single-partition load (no parallelism). Consider "
                    "adding an index on the PK for parallel loads.",
                    schema_name, table_name, pk_col,
                )
                return [(None, None)]
            conn_id = _mysql_conn_id(cur)
            # Step 2: MIN/MAX with server-side timeout. Instant on indexed PKs.
            try:
                cur.execute(
                    f"SELECT /*+ MAX_EXECUTION_TIME({timeout_hint}) */ "
                    f"MIN({pk_q}), MAX({pk_q}) FROM {qualified}"
                )
                mn, mx = cur.fetchone()
            except Exception as exc:
                # Step 3: timeout → KILL the stuck query, fall back to plan B.
                log.warning("partition: MIN/MAX timed out or failed for "
                            "%s.%s: %s — killing conn_id=%s and falling back "
                            "to first/last PK", schema_name, table_name, exc,
                            conn_id)
                if conn_id is not None:
                    _mysql_kill(host, port, database, user, password, conn_id)
                return _mysql_plan_b(host, port, database, user, password,
                                     schema_name, table_name, pk_col, k,
                                     qualified, pk_q, timeout_hint)
            if mn is None or mx is None:
                return [(None, None)]
            cnt = _mysql_approx_count(host, port, database, user, password,
                                       schema_name, table_name) or 0
            log.info("partition: mysql %s.%s min=%s max=%s approx_rows=%s k=%d",
                     schema_name, table_name, mn, mx, cnt, k)
            return naive_numeric_ranges(mn, mx, k)
    return [(None, None)]


def _mysql_plan_b(host, port, database, user, password,
                  schema_name, table_name, pk_col, k,
                  qualified, pk_q, timeout_hint) -> list[tuple]:
    """Plan B: first/last PK via ``ORDER BY pk LIMIT 1`` (uses the PK index,
    instant) + information_schema count. If even this times out, fall back to
    K=1."""
    import pymysql
    try:
        with pymysql.connect(host=host, port=int(port), database=database,
                              user=user, password=password,
                              connect_timeout=PARTITION_CONNECT_TIMEOUT_S,
                              read_timeout=PARTITION_READ_TIMEOUT_S,
                              autocommit=True) as conn:
            with conn.cursor() as cur:
                conn_id = _mysql_conn_id(cur)
                try:
                    cur.execute(
                        f"SELECT /*+ MAX_EXECUTION_TIME({timeout_hint}) */ "
                        f"{pk_q} FROM {qualified} ORDER BY {pk_q} ASC LIMIT 1"
                    )
                    first_row = cur.fetchone()
                    cur.execute(
                        f"SELECT /*+ MAX_EXECUTION_TIME({timeout_hint}) */ "
                        f"{pk_q} FROM {qualified} ORDER BY {pk_q} DESC LIMIT 1"
                    )
                    last_row = cur.fetchone()
                except Exception as exc:
                    log.warning("partition: plan B first/last PK also failed "
                                "for %s.%s: %s — falling back to K=1",
                                schema_name, table_name, exc)
                    if conn_id is not None:
                        _mysql_kill(host, port, database, user, password,
                                    conn_id)
                    return [(None, None)]
                if not first_row or not last_row:
                    return [(None, None)]
                mn, mx = first_row[0], last_row[0]
                if mn is None or mx is None:
                    return [(None, None)]
                cnt = _mysql_approx_count(host, port, database, user, password,
                                           schema_name, table_name) or 0
                log.info("partition: plan B mysql %s.%s first=%s last=%s "
                         "approx_rows=%s k=%d", schema_name, table_name,
                         mn, mx, cnt, k)
                return naive_numeric_ranges(mn, mx, k)
    except Exception as exc:
        log.warning("partition: plan B connection failed for %s.%s: %s — "
                    "K=1", schema_name, table_name, exc)
    return [(None, None)]


# ===========================
# Postgres
# ===========================

def _pg_has_pk_index(cur, schema_name: str, table_name: str, pk_col: str) -> bool:
    """Check whether ``table`` has an index whose first column is ``pk_col``
    (PRIMARY or a unique index). Without one, ``MIN(pk)``/``MAX(pk)`` would
    full-scan the table — we fall back to K=1 instead."""
    try:
        cur.execute(
            "SELECT 1 FROM pg_indexes i "
            "WHERE COALESCE(%s, current_schema())::text = i.schemaname "
            "AND %s = i.tablename "
            "AND EXISTS ("
            "  SELECT 1 FROM unnest(i.indexdef::text) "
            "  WHERE i.indexdef ILIKE %s"
            ") LIMIT 1",
            (schema_name, table_name, f"%({pk_col}%"),
        )
        return cur.fetchone() is not None
    except Exception:
        # Fallback: pg_class + pg_index join.
        try:
            cur.execute(
                "SELECT 1 FROM pg_index ix "
                "JOIN pg_class c ON c.oid = ix.indrelid "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = COALESCE(%s, current_schema()) "
                "AND c.relname = %s "
                "AND ix.indisvalid "
                "AND ix.indkey[0] = ("
                "  SELECT a.attnum FROM pg_attribute a "
                "  WHERE a.attrelid = c.oid AND a.attname = %s"
                ") LIMIT 1",
                (schema_name, table_name, pk_col),
            )
            return cur.fetchone() is not None
        except Exception:
            return False


def _pg_backend_pid(cur) -> Optional[int]:
    try:
        cur.execute("SELECT pg_backend_pid()")
        row = cur.fetchone()
        if row:
            return int(row["pid"] if isinstance(row, dict) else row[0])
    except Exception:
        pass
    return None


def _pg_kill(host, port, database, user, password, pid: int) -> None:
    """Cancel then terminate the stuck backend. Best-effort — logged on
    failure. Issued from a *fresh* connection."""
    try:
        import psycopg2
        with psycopg2.connect(host=host, port=port, dbname=database,
                              user=user, password=password,
                              connect_timeout=PARTITION_CONNECT_TIMEOUT_S,
                              application_name="fusion-cdc-partition-kill") as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                try:
                    cur.execute("SELECT pg_cancel_backend(%s)", (pid,))
                except Exception:
                    pass
                cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
        log.warning("partition: pg_terminate_backend(%s) issued (db=%s)",
                    pid, database)
    except Exception as exc:
        log.warning("partition: pg_terminate_backend(%s) failed: %s", pid, exc)


def _pg_approx_count(cur, schema_name: str, table_name: str) -> Optional[int]:
    """Instant metadata lookup — ``pg_class.reltuples`` is the planner's row
    estimate, refreshed by ANALYZE. Good enough for partition math."""
    try:
        cur.execute(
            "SELECT reltuples::bigint FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relname = %s AND n.nspname = COALESCE(%s, current_schema()) "
            "AND c.relkind = 'r'",
            (table_name, schema_name),
        )
        row = cur.fetchone()
        if row:
            v = row["reltuples"] if isinstance(row, dict) else row[0]
            if v is not None:
                return int(v)
    except Exception as exc:
        log.warning("partition: pg_class.reltuples lookup failed for %s.%s: %s",
                    schema_name, table_name, exc)
    return None


def _partition_pg(host, port, database, user, password,
                  schema_name, table_name, pk_col, k) -> list[tuple]:
    import psycopg2
    import psycopg2.extras
    qualified = (f'"{schema_name}"."{table_name}"'
                 if schema_name else f'"{table_name}"')
    pk_q = f'"{pk_col}"'
    with psycopg2.connect(host=host, port=port, dbname=database,
                          user=user, password=password,
                          connect_timeout=PARTITION_CONNECT_TIMEOUT_S,
                          application_name="fusion-cdc-partition") as conn:
        conn.autocommit = True
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Step 1: PK index check.
            if not _pg_has_pk_index(cur, schema_name, table_name, pk_col):
                log.warning(
                    "partition: table %s.%s has no PK index on %s; falling "
                    "back to single-partition load (no parallelism). Consider "
                    "adding an index on the PK for parallel loads.",
                    schema_name, table_name, pk_col,
                )
                return [(None, None)]
            pid = _pg_backend_pid(cur)
            # Set a 30s statement timeout for this transaction (READ ONLY).
            cur.execute("BEGIN READ ONLY")
            try:
                cur.execute("SET LOCAL statement_timeout = %s",
                            (f"{PARTITION_QUERY_TIMEOUT_MS}"))
                # Step 2: MIN/MAX (instant on indexed PKs).
                try:
                    cur.execute(f"SELECT MIN({pk_q}), MAX({pk_q}) FROM {qualified}")
                    row = cur.fetchone()
                    mn, mx = row["min"], row["max"]
                except Exception as exc:
                    # Step 3: timeout → KILL, fall back to plan B.
                    log.warning("partition: MIN/MAX timed out or failed for "
                                "%s.%s: %s — terminating pid=%s and falling "
                                "back to first/last PK", schema_name,
                                table_name, exc, pid)
                    if pid is not None:
                        _pg_kill(host, port, database, user, password, pid)
                    return _pg_plan_b(host, port, database, user, password,
                                      schema_name, table_name, pk_col, k,
                                      qualified, pk_q)
                if mn is None or mx is None:
                    return [(None, None)]
                cnt = _pg_approx_count(cur, schema_name, table_name) or 0
                log.info("partition: pg %s.%s min=%s max=%s approx_rows=%s k=%d",
                         schema_name, table_name, mn, mx, cnt, k)
                return naive_numeric_ranges(mn, mx, k)
            finally:
                try:
                    cur.execute("COMMIT")
                except Exception:
                    pass
    return [(None, None)]


def _pg_plan_b(host, port, database, user, password,
               schema_name, table_name, pk_col, k,
               qualified, pk_q) -> list[tuple]:
    """Plan B: first/last PK via ``ORDER BY pk LIMIT 1`` + pg_class.reltuples.
    If even this times out, fall back to K=1."""
    import psycopg2
    import psycopg2.extras
    try:
        with psycopg2.connect(host=host, port=port, dbname=database,
                              user=user, password=password,
                              connect_timeout=PARTITION_CONNECT_TIMEOUT_S,
                              application_name="fusion-cdc-partition-planb") as conn:
            conn.autocommit = True
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                pid = _pg_backend_pid(cur)
                cur.execute("BEGIN READ ONLY")
                try:
                    cur.execute("SET LOCAL statement_timeout = %s",
                                (f"{PARTITION_QUERY_TIMEOUT_MS}"))
                    try:
                        cur.execute(
                            f"SELECT {pk_q} AS pk FROM {qualified} "
                            f"ORDER BY {pk_q} ASC LIMIT 1"
                        )
                        first_row = cur.fetchone()
                        cur.execute(
                            f"SELECT {pk_q} AS pk FROM {qualified} "
                            f"ORDER BY {pk_q} DESC LIMIT 1"
                        )
                        last_row = cur.fetchone()
                    except Exception as exc:
                        log.warning("partition: plan B first/last PK also "
                                    "failed for %s.%s: %s — falling back to K=1",
                                    schema_name, table_name, exc)
                        if pid is not None:
                            _pg_kill(host, port, database, user, password, pid)
                        return [(None, None)]
                    if not first_row or not last_row:
                        return [(None, None)]
                    mn, mx = first_row["pk"], last_row["pk"]
                    if mn is None or mx is None:
                        return [(None, None)]
                    cnt = _pg_approx_count(cur, schema_name, table_name) or 0
                    log.info("partition: plan B pg %s.%s first=%s last=%s "
                             "approx_rows=%s k=%d", schema_name, table_name,
                             mn, mx, cnt, k)
                    return naive_numeric_ranges(mn, mx, k)
                finally:
                    try:
                        cur.execute("COMMIT")
                    except Exception:
                        pass
    except Exception as exc:
        log.warning("partition: plan B connection failed for %s.%s: %s — K=1",
                    schema_name, table_name, exc)
    return [(None, None)]


