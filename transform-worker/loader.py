"""
Transform Worker — Task runners for initial loads and CDC event transforms.
"""
from __future__ import annotations

import io
import logging
import os
import queue
import threading
import time
from typing import TYPE_CHECKING, Any

import psycopg2
import pyarrow as pa
import redis

if TYPE_CHECKING:
    from engine import DuckDBTransformEngine

log = logging.getLogger(__name__)

# v1.2.17: module-level stop event. The worker process sets this on SIGTERM /
# SIGINT so a long-running chunked initial-load loop can drain gracefully
# after the current chunk instead of being killed mid-table by k8s.
STOP_EVENT = threading.Event()

# Default chunk size for PK-bounded initial loads (rows per chunk). The
# producer can override per-task via ``chunk_size``; this default keeps
# memory bounded to ~a few MB per chunk for typical row widths.
DEFAULT_CHUNK_SIZE = 10000

# v1.2.25 Task 5: after every N chunks, compact the Iceberg manifest list +
# expire old snapshots so a long initial load (one snapshot per chunk) does
# not accumulate hundreds of manifests and degrade throughput ~30%.
# Configurable via the INITIAL_LOAD_COMPACTION_INTERVAL env var.
INITIAL_LOAD_COMPACTION_INTERVAL = int(os.environ.get("INITIAL_LOAD_COMPACTION_INTERVAL", "50"))

# v1.2.26 Task 4: adaptive chunk sizing bounds. The worker auto-tunes the
# chunk size at runtime based on observed per-chunk latency (fetch + convert
# + write): if latency < ADAPTIVE_FAST_LATENCY_S for ADAPTIVE_FAST_STREAK
# consecutive chunks, double the chunk size (cap = ADAPTIVE_MAX_CHUNK); if
# latency > ADAPTIVE_SLOW_LATENCY_S for ADAPTIVE_SLOW_STREAK consecutive
# chunks, halve it (floor = ADAPTIVE_MIN_CHUNK). This auto-tunes for
# different table sizes / source DB performance without operator tuning.
ADAPTIVE_MIN_CHUNK = int(os.environ.get("INITIAL_LOAD_ADAPTIVE_MIN_CHUNK", "1000"))
ADAPTIVE_MAX_CHUNK = int(os.environ.get("INITIAL_LOAD_ADAPTIVE_MAX_CHUNK", "100000"))
ADAPTIVE_FAST_LATENCY_S = float(os.environ.get("INITIAL_LOAD_ADAPTIVE_FAST_LATENCY", "2.0"))
ADAPTIVE_SLOW_LATENCY_S = float(os.environ.get("INITIAL_LOAD_ADAPTIVE_SLOW_LATENCY", "30.0"))
ADAPTIVE_FAST_STREAK = int(os.environ.get("INITIAL_LOAD_ADAPTIVE_FAST_STREAK", "5"))
ADAPTIVE_SLOW_STREAK = int(os.environ.get("INITIAL_LOAD_ADAPTIVE_SLOW_STREAK", "2"))

# v1.2.26 Task 5: max number of chunks buffered in the fetch/write overlap
# queue. Bounds memory to ~PIPELINE_QUEUE_SIZE chunks while hiding source
# DB fetch latency behind Iceberg/object-store write latency.
PIPELINE_QUEUE_SIZE = int(os.environ.get("INITIAL_LOAD_PIPELINE_QUEUE_SIZE", "2"))

# v1.2.26 Task 7: buffer N chunks into a single Iceberg append (commit) to
# reduce the commit count and the manifest-accumulation cost. 1 = commit
# every chunk (legacy v1.2.25 behaviour). Set to e.g. 5 to commit every 5
# chunks. Only applies to Iceberg destinations (Postgres COPY is already
# batched). The final partial batch is always flushed at the end of the
# range.
INITIAL_LOAD_COMMIT_BATCH = int(os.environ.get("INITIAL_LOAD_COMMIT_BATCH", "1"))


# ---------------------------------------------------------------------------
# Destination DSN builders — derive a SQLAlchemy-style DSN from a destination
# block produced by the control-plane transform-route endpoint. The block
# shape is:
#   {"connector_type": "postgresql" | "mysql" | "mongodb" | "iceberg" | ...,
#    "connection_config": {"host": ..., "port": ..., "database_name": ...,
#                          "username": ..., "password": <decrypted plaintext>}}
#
# Each builder returns "" when a required field is missing so the caller can
# log + drop the batch instead of raising. The dispatcher returns "" for
# unknown types and for "iceberg" (which is handled by a separate writer).
# ---------------------------------------------------------------------------

def _pg_dsn_from_dest(dest: dict) -> str:
    """Build a PostgreSQL DSN: postgresql://{user}:{password}@{host}:{port}/{database}."""
    cfg = (dest.get("connection_config") or dest.get("config") or {})
    host = cfg.get("host") or ""
    port = cfg.get("port") or 5432
    database = (cfg.get("database_name") or cfg.get("database")
                or cfg.get("dbname") or "")
    user = cfg.get("username") or cfg.get("user") or ""
    password = cfg.get("password") or ""
    if not host or not database or not user:
        return ""
    from urllib.parse import quote_plus
    return (
        f"postgresql://{quote_plus(user)}:{quote_plus(password)}@"
        f"{host}:{port}/{quote_plus(database)}"
    )


def _mysql_dsn_from_dest(dest: dict) -> str:
    """Build a MySQL DSN: mysql+pymysql://{user}:{password}@{host}:{port}/{database}."""
    cfg = (dest.get("connection_config") or dest.get("config") or {})
    host = cfg.get("host") or ""
    port = cfg.get("port") or 3306
    database = (cfg.get("database_name") or cfg.get("database")
                or cfg.get("dbname") or "")
    user = cfg.get("username") or cfg.get("user") or ""
    password = cfg.get("password") or ""
    if not host or not database or not user:
        return ""
    from urllib.parse import quote_plus
    return (
        f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}@"
        f"{host}:{port}/{quote_plus(database)}"
    )


def _mongo_dsn_from_dest(dest: dict) -> str:
    """Build a MongoDB URI: mongodb://{user}:{password}@{host}:{port}/{database}?authSource=admin.

    Mirrors the format already used by ``cdc_consumer._do_initial_load_mongodb``
    so the destination side stays consistent with the source side. Returns ""
    when host is missing.
    """
    cfg = (dest.get("connection_config") or dest.get("config") or {})
    host = cfg.get("host") or ""
    port = cfg.get("port") or 27017
    database = (cfg.get("database_name") or cfg.get("database") or "")
    user = cfg.get("username") or cfg.get("user") or ""
    password = cfg.get("password") or ""
    auth_source = (cfg.get("auth_source") if isinstance(cfg.get("auth_source"), str)
                   else "admin") or "admin"
    if not host:
        return ""
    from urllib.parse import quote_plus
    path = f"/{quote_plus(database)}" if database else "/"
    if user and password:
        return (
            f"mongodb://{quote_plus(user)}:{quote_plus(password)}@"
            f"{host}:{port}{path}?authSource={auth_source}"
        )
    return f"mongodb://{host}:{port}{path}?authSource={auth_source}"


def _dest_dsn_from_dest(dest: dict) -> str:
    """Dispatch on destination connector_type and return the right DSN.

    Returns "" for unknown types and for "iceberg" (the Iceberg writer is a
    separate code path that does not use a SQL DSN). Callers must treat an
    empty string as "cannot route this batch" and log + drop.
    """
    ctype = (dest.get("connector_type") or "").lower()
    if ctype in ("postgres", "postgresql"):
        return _pg_dsn_from_dest(dest)
    if ctype == "mysql":
        return _mysql_dsn_from_dest(dest)
    if ctype == "mongodb":
        return _mongo_dsn_from_dest(dest)
    # iceberg / unknown → no SQL DSN
    return ""


class InitialLoadTask:
    """v1.2.17: PK-bounded chunked initial load with checkpoint resume.

    v1.2.26: multi-pod INTRA-table parallelism. The producer enqueues K
    tasks per stream — one per disjoint PK-range partition (``chunk_seq``
    0..K-1, ``pk_start``/``pk_end`` bounds). Each task loops over
    PK-bounded chunks WITHIN its assigned range and checkpoints under the
    composite key ``(connection_id, stream_id, chunk_seq)`` so K concurrent
    pods do not stomp the same checkpoint row. The connection's overall
    ``initial_load_completed`` is set by the control-plane only when ALL K
    ranges report ``done``.

    v1.2.26 Task 4: adaptive chunk sizing — the chunk size is auto-tuned at
    runtime from observed per-chunk latency (doubled on fast streaks, halved
    on slow streaks, within [ADAPTIVE_MIN_CHUNK, ADAPTIVE_MAX_CHUNK]).

    v1.2.26 Task 5: fetch/write overlap — a background thread prefetches
    chunk N+1 from the source DB while the main thread converts + writes
    chunk N to the destination, hiding read latency behind write latency.

    v1.2.26 Task 7: commit batching — for Iceberg destinations, N chunks
    (``INITIAL_LOAD_COMMIT_BATCH``) are buffered into a single append to
    reduce the commit count and manifest-accumulation cost.

    On worker restart / OOM-kill, the task reads the last checkpoint for
    its ``chunk_seq`` from the control-plane and resumes from
    ``last_pk + 1`` instead of re-doing the whole range.
    """

    def __init__(self, engine: "DuckDBTransformEngine", redis_client: redis.Redis):
        self.engine = engine
        self.redis = redis_client

    def run(self, task: dict):
        connection_id = task["connection_id"]
        stream_id = task.get("stream_id")
        steps = task.get("transform_steps", [])
        source = task.get("source") or {}
        source_schema = task.get("source_schema") or ""
        source_table = task.get("source_table") or ""
        chunk_size = int(task.get("chunk_size") or DEFAULT_CHUNK_SIZE)
        # v1.2.26 Task 1c: PK-range partition bounds + composite checkpoint key.
        # ``chunk_seq`` is this task's partition index (0..K-1); ``pk_start``/
        # ``pk_end`` are the (possibly open) bounds of the disjoint PK range
        # this task owns. ``total_chunks`` is K — the control-plane uses it
        # to decide when ALL ranges are done and the connection's initial load
        # is complete.
        chunk_seq = int(task.get("chunk_seq") or 0)
        pk_start = task.get("pk_start")
        pk_end = task.get("pk_end")
        total_chunks = int(task.get("total_chunks") or 1)
        # Primary key column used for PK-bounded chunking. The producer sends
        # ``primary_key`` as a comma-joined string for composite PKs; we chunk
        # on the first PK column (correct for identity-style composite PKs).
        pk_raw = task.get("primary_key") or "id"
        pk_col = str(pk_raw).split(",")[0].strip() or "id"

        ctype = (source.get("connector_type") or "").lower()
        # MongoDB always chunks on the immutable _id field regardless of the
        # user-declared PK.
        if ctype == "mongodb":
            pk_col = "_id"

        dest = task.get("destination") or {}
        connector_type = dest.get("connector_type") or task.get("dest_connector_type", "postgres")
        dest_schema = task.get("dest_schema", "dw")
        dest_table = task.get("dest_table", "data")

        log.info("InitialLoad connection=%s table=%s.%s pk=%s chunk_seq=%d range=[%s,%s] total_chunks=%d chunk_size=%d dest=%s",
                 connection_id, source_schema, source_table, pk_col, chunk_seq,
                 pk_start, pk_end, total_chunks, chunk_size, connector_type)

        # ── v1.2.22 Bug A fix / Fix C1: fetch the source schema ONCE per
        # stream and reuse it for every chunk. This (a) gives all-NULL
        # columns their declared type so PyIceberg never sees pa.null(),
        # and (b) removes the per-chunk type-inference compute waste that
        # was blocking the source DB during the 118M-row load.
        cached_source_schema: "pa.Schema | None" = None
        if connector_type == "iceberg":
            try:
                from iceberg_writer import _get_source_schema
                cached_source_schema = _get_source_schema(source, source_schema, source_table)
                log.info("InitialLoad connection=%s fetched source schema (%d cols) — will reuse for all chunks",
                         connection_id, len(cached_source_schema))
            except Exception:
                log.exception("InitialLoad connection=%s _get_source_schema failed — falling back to per-chunk inference (Bug A may recur)",
                              connection_id)
                cached_source_schema = None

        # ── Resume: fetch the last checkpoint for THIS chunk_seq (composite
        # key connection_id+stream_id+chunk_seq) so a restarted pod resumes
        # its own range instead of colliding with a sibling pod's checkpoint.
        last_pk = None
        prior_rows = 0
        ckpt = self._get_last_checkpoint(connection_id, stream_id, chunk_seq)
        if ckpt is not None:
            if ckpt.get("status") == "completed":
                log.info("InitialLoad connection=%s stream=%s chunk_seq=%d already completed (%d rows) — skipping",
                         connection_id, stream_id, chunk_seq, ckpt.get("rows_written", 0))
                return
            last_pk = ckpt.get("last_pk")
            prior_rows = int(ckpt.get("rows_written") or 0)
            log.info("InitialLoad connection=%s stream=%s chunk_seq=%d resuming from last_pk=%s (%d rows already written)",
                     connection_id, stream_id, chunk_seq, last_pk, prior_rows)

        # v1.2.26 Task 1c: on a fresh start (no checkpoint), begin at the
        # partition's lower bound. _fetch_chunk uses ``WHERE pk > last_pk``,
        # so seeding last_pk = pk_start picks up rows strictly greater than
        # pk_start — correct for the partition's first chunk. When pk_start
        # is None (first partition or unbounded), last_pk stays None and the
        # fetch starts from the table's minimum PK.
        if last_pk is None and pk_start is not None:
            last_pk = pk_start

        total_rows = prior_rows
        # v1.2.22 Fix C4: stream, don't accumulate. Each chunk is converted to
        # Arrow, written to the destination, checkpointed, then released.
        # The transformed schema is captured from the first chunk (via
        # DuckDB's staging table) and reused for every subsequent chunk so
        # IcebergWriter never re-infers types.
        cached_transformed_schema: "pa.Schema | None" = None

        # v1.2.26 Task 4: adaptive chunk sizing runtime state. The initial
        # chunk size is the producer's configured ``chunk_size`` (capped at
        # ADAPTIVE_MAX_CHUNK); we do NOT clamp it up to ADAPTIVE_MIN_CHUNK —
        # an operator who explicitly sets a small chunk_size (or a test) gets
        # exactly that. The adaptive logic then doubles on fast streaks
        # (capped at ADAPTIVE_MAX_CHUNK) and halves on slow streaks (floored
        # at ADAPTIVE_MIN_CHUNK, but never raised above the current value).
        cur_chunk_size = max(1, min(ADAPTIVE_MAX_CHUNK, chunk_size))
        fast_streak = 0
        slow_streak = 0

        # v1.2.26 Task 7: commit batching — buffer N chunks worth of
        # transformed rows into a single Iceberg append. Only applies to
        # Iceberg destinations; Postgres COPY is already batched per chunk.
        commit_batch = max(1, INITIAL_LOAD_COMMIT_BATCH) if connector_type == "iceberg" else 1
        pending_rows: list[dict] = []       # buffered transformed rows
        pending_child: dict[str, list[dict]] = {}
        chunks_since_commit = 0

        # chunk_counter is the running count of chunks processed within THIS
        # partition (used for compaction timing + current_chunk reporting).
        # ``chunk_seq`` (the partition index) is the composite-key identifier
        # and stays fixed for the whole task.
        chunk_counter = 0

        # v1.2.26 Task 5: fetch/write overlap. A bounded queue holds at most
        # PIPELINE_QUEUE_SIZE prefetched chunks; a background fetch thread
        # produces chunks while the main thread consumes (convert + write).
        # This hides source-DB fetch latency behind Iceberg/object-store
        # write latency (different resources: network+DB vs object-store).
        prefetch_q: "queue.Queue[list[dict] | None]" = queue.Queue(maxsize=max(1, PIPELINE_QUEUE_SIZE))
        fetch_exc: list[Exception] = []

        def _fetch_and_put(cursor_pk, limit):
            try:
                rows = self._fetch_chunk(source, source_schema, source_table,
                                         pk_col, cursor_pk, limit, ctype, pk_end)
                prefetch_q.put(rows)
            except Exception as exc:  # noqa: BLE001
                fetch_exc.append(exc)
                prefetch_q.put(None)

        # Kick off the first fetch on a background thread (overlaps with
        # nothing yet, but primes the queue).
        first_fetch = threading.Thread(
            target=_fetch_and_put, args=(last_pk, cur_chunk_size), daemon=True,
        )
        first_fetch.start()

        # ── PK-bounded chunk loop (within this partition's [pk_start, pk_end]) ──
        while not STOP_EVENT.is_set():
            rows = prefetch_q.get()
            if rows is None:
                # Fetch thread failed — surface the exception.
                if fetch_exc:
                    log.exception("InitialLoad connection=%s fetch thread failed — stopping range %d",
                                  connection_id, chunk_seq, exc_info=fetch_exc[0])
                    self._report_checkpoint(connection_id, stream_id, source_table,
                                            chunk_seq, 0, last_pk, state="failed",
                                            total_chunks=total_chunks)
                break
            if not rows:
                log.info("InitialLoad connection=%s table=%s.%s chunk_seq=%d — no more rows in range, load complete",
                         connection_id, source_schema, source_table, chunk_seq)
                break

            next_pk = self._extract_pk(rows[-1], pk_col, ctype)

            # v1.2.26 Task 5: start the next fetch NOW (before the write) so
            # the source-DB read of chunk N+1 overlaps with the convert+write
            # of chunk N. Only prefetch when this chunk was full (a short
            # chunk means end-of-range).
            if len(rows) >= cur_chunk_size:
                threading.Thread(
                    target=_fetch_and_put, args=(next_pk, cur_chunk_size), daemon=True,
                ).start()

            t0 = time.monotonic()

            # Apply transforms
            if steps:
                transformed, child_tables, transformed_schema = self.engine.execute_pipeline(
                    rows, steps, schema=cached_source_schema,
                )
                if cached_transformed_schema is None and transformed_schema is not None:
                    cached_transformed_schema = transformed_schema
            else:
                transformed, child_tables = rows, {}
                transformed_schema = cached_source_schema

            # Write to destination
            if connector_type == "iceberg":
                # v1.2.26 Task 7: buffer into pending_rows and flush every
                # ``commit_batch`` chunks (or at end-of-range). One Iceberg
                # append per flush reduces the commit count and the
                # manifest-accumulation cost (~30% throughput degradation
                # per the v1.2.25 report).
                pending_rows.extend(transformed)
                for child_name, child_rows in child_tables.items():
                    if child_rows:
                        pending_child.setdefault(child_name, []).extend(child_rows)
                chunks_since_commit += 1
                rows_written = len(transformed)
                if chunks_since_commit >= commit_batch:
                    self._flush_iceberg_batch(
                        pending_rows, pending_child, dest, dest_table,
                        schema=cached_transformed_schema or cached_source_schema,
                    )
                    pending_rows = []
                    pending_child = {}
                    chunks_since_commit = 0
            else:
                dest_dsn = _dest_dsn_from_dest(dest)
                if not dest_dsn:
                    log.error(
                        "InitialLoad connection=%s cannot derive dest_dsn for "
                        "connector_type=%s — destination block missing/incomplete. "
                        "Stopping load after %d rows.",
                        connection_id, connector_type, total_rows,
                    )
                    self._report_checkpoint(connection_id, stream_id, source_table,
                                            chunk_seq, 0, last_pk, state="failed",
                                            total_chunks=total_chunks)
                    return
                rows_written = self._copy_to_postgres(transformed, dest_dsn, dest_schema, dest_table)
                for child_name, child_rows in child_tables.items():
                    if child_rows:
                        self._copy_to_postgres(child_rows, dest_dsn, dest_schema, child_name)

            latency = time.monotonic() - t0
            total_rows += rows_written
            chunk_counter += 1
            last_pk = next_pk

            # Report checkpoint as "running" so a restart resumes here. The
            # composite key (connection_id, stream_id, chunk_seq) means each
            # of the K pods writes its own row — no stomping.
            self._report_checkpoint(connection_id, stream_id, source_table,
                                    chunk_seq, rows_written, last_pk, state="running",
                                    total_chunks=total_chunks)
            log.info("InitialLoad connection=%s chunk_seq=%d chunk=%d done — %d rows (total %d) last_pk=%s latency=%.2fs cs=%d",
                     connection_id, chunk_seq, chunk_counter, rows_written, total_rows,
                     last_pk, latency, cur_chunk_size)

            # v1.2.26 Task 4: adaptive chunk sizing based on this chunk's latency.
            if latency < ADAPTIVE_FAST_LATENCY_S:
                fast_streak += 1
                slow_streak = 0
                if fast_streak >= ADAPTIVE_FAST_STREAK and cur_chunk_size < ADAPTIVE_MAX_CHUNK:
                    new_size = min(ADAPTIVE_MAX_CHUNK, cur_chunk_size * 2)
                    log.info("InitialLoad connection=%s adaptive: chunk_size %d -> %d (fast streak %d)",
                             connection_id, cur_chunk_size, new_size, fast_streak)
                    cur_chunk_size = new_size
                    fast_streak = 0
            elif latency > ADAPTIVE_SLOW_LATENCY_S:
                slow_streak += 1
                fast_streak = 0
                if slow_streak >= ADAPTIVE_SLOW_STREAK and cur_chunk_size > ADAPTIVE_MIN_CHUNK:
                    # Halve, floored at ADAPTIVE_MIN_CHUNK — but only when
                    # we're already above the min (never jump a small
                    # configured chunk_size UP to the min).
                    new_size = max(ADAPTIVE_MIN_CHUNK, cur_chunk_size // 2)
                    log.info("InitialLoad connection=%s adaptive: chunk_size %d -> %d (slow streak %d)",
                             connection_id, cur_chunk_size, new_size, slow_streak)
                    cur_chunk_size = new_size
                    slow_streak = 0
                elif slow_streak >= ADAPTIVE_SLOW_STREAK and cur_chunk_size > 1:
                    # Already at/below the min — just halve, floored at 1.
                    new_size = max(1, cur_chunk_size // 2)
                    log.info("InitialLoad connection=%s adaptive: chunk_size %d -> %d (slow streak %d, below min)",
                             connection_id, cur_chunk_size, new_size, slow_streak)
                    cur_chunk_size = new_size
                    slow_streak = 0

            # v1.2.25 Task 5: periodic manifest compaction for Iceberg
            # destinations. Every INITIAL_LOAD_COMPACTION_INTERVAL chunks,
            # compact the manifest list + expire old snapshots so a long
            # load does not degrade throughput ~30% from manifest/snapshot
            # accumulation. Skipped for non-iceberg destinations (postgres
            # has no manifests) and when the interval is <= 0 (disabled).
            if (connector_type == "iceberg"
                    and INITIAL_LOAD_COMPACTION_INTERVAL > 0
                    and chunk_counter % INITIAL_LOAD_COMPACTION_INTERVAL == 0):
                try:
                    from iceberg_writer import IcebergWriter
                    dest_config = dest.get("connection_config") or dest.get("config") or dest
                    IcebergWriter(dest_config).compact_manifests(dest_table)
                except Exception:
                    log.exception("InitialLoad connection=%s compaction failed for table=%s chunk=%d — continuing (non-fatal)",
                                  connection_id, dest_table, chunk_counter)

            # v1.2.26 Task 1c: stop when we've crossed this partition's upper
            # bound (closed upper bound — _fetch_chunk already clamps the
            # fetch to ``pk <= pk_end`` so the last chunk ends exactly at the
            # boundary; this guard is a belt-and-suspenders stop).
            if pk_end is not None and last_pk is not None and last_pk >= pk_end:
                break

            # A short chunk means we've reached the end of the table / range.
            if len(rows) < cur_chunk_size:
                break

            # Fix C4: release the chunk's memory before fetching the next.
            del rows, transformed, child_tables

        # v1.2.26 Task 7: flush any remaining buffered Iceberg batch.
        if connector_type == "iceberg" and pending_rows:
            self._flush_iceberg_batch(
                pending_rows, pending_child, dest, dest_table,
                schema=cached_transformed_schema or cached_source_schema,
            )
            pending_rows = []
            pending_child = {}
            chunks_since_commit = 0

        if STOP_EVENT.is_set():
            log.warning("InitialLoad connection=%s chunk_seq=%d stopped mid-load after chunk %d — checkpoint saved, will resume on restart",
                        connection_id, chunk_seq, chunk_counter)
            # Leave status as "running" so a restart resumes; do not mark completed.
            return

        # ── Mark this partition (chunk_seq) completed ──────────────────────
        # The control-plane sets connection.initial_load_completed=True only
        # when ALL K partitions (chunk_seq 0..K-1) reach "done".
        self._report_checkpoint(connection_id, stream_id, source_table,
                                chunk_seq, 0, last_pk, state="done",
                                total_chunks=total_chunks)
        log.info("InitialLoad connection=%s chunk_seq=%d DONE — %d rows across %d chunks",
                 connection_id, chunk_seq, total_rows, chunk_counter)

    def _flush_iceberg_batch(self, rows: list[dict], child_tables: dict, dest: dict,
                             table_name: str, schema: "pa.Schema | None" = None) -> int:
        """v1.2.26 Task 7: flush a buffered batch of rows to Iceberg in one
        ``table.append`` (one commit). Returns the row count written.

        Concurrent Iceberg writes from sibling pods (other chunk_seq ranges)
        are safe: PyIceberg's catalog commit is an optimistic
        compare-and-swap on the table metadata pointer — on conflict the
        loser reloads metadata and retries (tenacity-backed in
        ``iceberg_writer.load_catalog``). So K pods appending to the same
        Iceberg table is a supported pattern; do NOT serialize this.
        """
        if not rows:
            return 0
        written = self._write_to_iceberg(rows, dest, table_name, schema=schema)
        for child_name, child_rows in child_tables.items():
            if child_rows:
                self._write_to_iceberg(child_rows, dest, child_name)
        return written

    def _write_to_iceberg(self, rows: list[dict], dest: dict, table_name: str,
                          schema: "pa.Schema | None" = None) -> int:
        """Write rows to Iceberg via PyIceberg (DuckDB lake path).

        v1.2.22 Bug A fix: ``schema`` is the explicit source/transformed
        schema so all-NULL columns keep their declared type.
        """
        from iceberg_writer import IcebergWriter
        dest_config = dest.get("connection_config") or dest.get("config") or dest
        writer = IcebergWriter(dest_config)
        return writer.write_batch(rows, table_name=table_name, schema=schema)

    def _fetch_chunk(self, source: dict, schema_name: str, table_name: str,
                    pk_col: str, last_pk, chunk_size: int, ctype: str,
                    pk_end=None) -> list[dict]:
        """Fetch the next PK-bounded chunk of rows from the source DB.

        Returns up to ``chunk_size`` rows ordered by ``pk_col`` ASC, with
        ``pk_col > last_pk`` when ``last_pk`` is not None (resume). When
        ``pk_end`` is not None (v1.2.26 multi-pod intra-table parallelism),
        the fetch is additionally bounded by ``pk_col <= pk_end`` so each
        partition's fetches stay within its assigned disjoint range — this
        is what makes K concurrent pods safe (no row overlap across
        partitions). Returns ``[]`` when the table/range is empty /
        exhausted or the source block is incomplete.
        """
        if not source or not table_name:
            return []
        host = source.get("host") or ""
        port = source.get("port")
        database = source.get("database_name") or source.get("database") or ""
        user = source.get("username") or source.get("user") or ""
        password = source.get("password") or ""
        cfg = source.get("config") or {}
        if not host or not database:
            log.error("_fetch_chunk: source block missing host/database — cannot fetch")
            return []
        try:
            if ctype in ("postgres", "postgresql"):
                return self._fetch_pg_chunk(host, port or 5432, database, user, password,
                                             schema_name, table_name, pk_col, last_pk, chunk_size, pk_end)
            if ctype == "mysql":
                return self._fetch_mysql_chunk(host, port or 3306, database, user, password,
                                                schema_name, table_name, pk_col, last_pk, chunk_size, pk_end)
            if ctype == "mongodb":
                return self._fetch_mongo_chunk(host, port or 27017, database, user, password,
                                                cfg, table_name, last_pk, chunk_size)
            log.error("_fetch_chunk: unsupported source connector_type=%s", ctype)
            return []
        except Exception:
            log.exception("_fetch_chunk: failed to fetch from %s.%s on %s",
                         schema_name, table_name, ctype)
            return []

    def _fetch_pg_chunk(self, host, port, database, user, password,
                        schema_name, table_name, pk_col, last_pk, chunk_size,
                        pk_end=None) -> list[dict]:
        import psycopg2
        import psycopg2.extras
        qualified = (f'"{schema_name}"."{table_name}"'
                     if schema_name else f'"{table_name}"')
        pk_q = f'"{pk_col}"'
        # v1.2.26: range-bounded fetch. ``pk > last_pk`` (resume) AND
        # ``pk <= pk_end`` (partition upper bound) keeps each pod's fetches
        # within its disjoint PK range.
        where_parts: list[str] = []
        params: list = []
        if last_pk is not None:
            where_parts.append(f"{pk_q} > %s")
            params.append(last_pk)
        if pk_end is not None:
            where_parts.append(f"{pk_q} <= %s")
            params.append(pk_end)
        where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
        params.append(chunk_size)
        sql = f"SELECT * FROM {qualified} {where_clause} ORDER BY {pk_q} ASC LIMIT %s".replace("  ", " ")
        # v1.2.22 Fix C3: READ ONLY + autocommit so we never hold a long
        # transaction open across the chunk write to the destination (which
        # was blocking the source DB during the 118M-row load).
        with psycopg2.connect(host=host, port=port, dbname=database,
                              user=user, password=password,
                              connect_timeout=10,
                              application_name="fusion-cdc-initial-load") as conn:
            conn.autocommit = True
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("BEGIN READ ONLY")
                try:
                    cur.execute(sql, tuple(params))
                    return [dict(r) for r in cur.fetchall()]
                finally:
                    cur.execute("COMMIT")

    def _fetch_mysql_chunk(self, host, port, database, user, password,
                           schema_name, table_name, pk_col, last_pk, chunk_size,
                           pk_end=None) -> list[dict]:
        import pymysql
        import pymysql.cursors
        qualified = (f"`{schema_name}`.`{table_name}`"
                     if schema_name else f"`{table_name}`")
        pk_q = f"`{pk_col}`"
        where_parts: list[str] = []
        params: list = []
        if last_pk is not None:
            where_parts.append(f"{pk_q} > %s")
            params.append(last_pk)
        if pk_end is not None:
            where_parts.append(f"{pk_q} <= %s")
            params.append(pk_end)
        where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
        params.append(chunk_size)
        sql = f"SELECT * FROM {qualified} {where_clause} ORDER BY {pk_q} ASC LIMIT %s".replace("  ", " ")
        # v1.2.22 Fix C3: autocommit=True so the chunk SELECT does not start
        # a transaction that is held open across the destination write.
        with pymysql.connect(host=host, port=int(port), database=database,
                             user=user, password=password,
                             cursorclass=pymysql.cursors.DictCursor,
                             connect_timeout=10,
                             autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return list(cur.fetchall())

    def _fetch_mongo_chunk(self, host, port, database, user, password,
                           cfg, collection_name, last_id, chunk_size) -> list[dict]:
        from urllib.parse import quote_plus
        from pymongo import MongoClient
        from bson import ObjectId
        auth_source = (cfg.get("auth_source") if isinstance(cfg, dict) else None) or "admin"
        if user and password:
            uri = (f"mongodb://{quote_plus(user)}:{quote_plus(password)}@"
                   f"{host}:{port}/{database}?authSource={auth_source}")
        else:
            uri = f"mongodb://{host}:{port}/{database}?authSource={auth_source}"
        # ``last_id`` may arrive as a stringified ObjectId (from the row dict
        # or from the JSON checkpoint on resume). Convert it back to a real
        # ObjectId so the ``$gt`` comparison matches BSON type ordering
        # (ObjectId vs String would never match and silently terminate the
        # load after the first chunk).
        if last_id is not None and not isinstance(last_id, ObjectId):
            try:
                last_id = ObjectId(str(last_id))
            except Exception:
                last_id = None
        client = MongoClient(uri, serverSelectionTimeoutMS=10000)
        try:
            db = client[database]
            query = {"_id": {"$gt": last_id}} if last_id is not None else {}
            cursor = (db[collection_name].find(query)
                      .sort("_id", 1)
                      .batch_size(min(chunk_size, 1000))
                      .limit(chunk_size))
            out: list[dict] = []
            for d in cursor:
                row = {k: (str(v) if k == "_id" else v) for k, v in d.items()}
                out.append(row)
            return out
        finally:
            client.close()

    def _extract_pk(self, row: dict, pk_col: str, ctype: str) -> Any:
        """Pull the PK value from the last row of a chunk for resume."""
        if not row:
            return None
        if ctype == "mongodb" and "_id" in row:
            return row["_id"]
        return row.get(pk_col)

    def _copy_to_postgres(self, rows: list[dict], dsn: str, schema: str, table: str) -> int:
        if not rows:
            return 0
        columns = list(rows[0].keys())
        buf = io.StringIO()
        for row in rows:
            line = "\t".join("\\N" if v is None else str(v).replace("\t", " ") for v in row.values())
            buf.write(line + "\n")
        buf.seek(0)

        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                cols_sql = ", ".join(f"{c} TEXT" for c in columns)
                cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({cols_sql})")
                cur.copy_from(buf, f"{schema}.{table}", columns=columns, null="\\N")
                conn.commit()
        return len(rows)

    def _get_last_checkpoint(self, connection_id: str, stream_id, chunk_seq: int = 0) -> dict | None:
        """v1.2.17: fetch the last checkpoint for this stream so the worker
        can resume a chunked initial load from ``last_pk + 1``. Returns
        ``None`` when no checkpoint row exists yet (first run) or when the
        control-plane is unreachable (treated as a fresh start).

        v1.2.26: the checkpoint key is now composite —
        ``(connection_id, stream_id, chunk_seq)`` — so each of the K
        intra-table parallel pods reads its own range's checkpoint and does
        not collide with a sibling pod. ``chunk_seq`` defaults to 0
        (legacy single-range behaviour).
        """
        import requests
        if not stream_id:
            return None
        try:
            resp = requests.get(
                f"{self.engine.control_plane_url}/api/v1/internal/load-checkpoints/last/"
                f"{connection_id}/{stream_id}/{chunk_seq}",
                headers={"X-Worker-Token": os.environ.get("WORKER_SHARED_SECRET", "")},
                timeout=10,
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except Exception:
            log.warning("_get_last_checkpoint: failed to fetch checkpoint for connection=%s stream=%s chunk_seq=%s — treating as fresh start",
                        connection_id, stream_id, chunk_seq, exc_info=True)
            return None

    def _report_checkpoint(self, connection_id: str, stream_id, source_table: str,
                           chunk_seq: int, rows_written: int, last_pk,
                           state: str = "done", total_chunks: int = 1):
        """Report chunk progress to the control-plane
        ``/internal/load-checkpoints`` endpoint (added in v1.2.16, extended in
        v1.2.17 with ``last_pk`` / ``chunk_seq`` / ``current_chunk``).

        ``rows_written`` is the count for THIS chunk only — the endpoint
        accumulates into the cumulative stream total. ``state`` is one of
        ``running`` (mid-load), ``done`` (range completed), or ``failed``.

        v1.2.26: ``chunk_seq`` is the PARTITION index (composite key with
        connection_id+stream_id) — each of the K pods reports under its own
        chunk_seq. ``total_chunks`` is K — the control-plane uses it to
        decide when ALL K ranges are done and the connection's
        ``initial_load_completed`` can be set.
        """
        import requests
        try:
            requests.post(
                f"{self.engine.control_plane_url}/api/v1/internal/load-checkpoints",
                json={
                    "connection_id": connection_id,
                    "stream_id": stream_id,
                    "source_table": source_table,
                    "chunk_seq": chunk_seq,
                    "rows_written": rows_written,
                    "last_pk": last_pk,
                    "state": state,
                    "current_chunk": chunk_seq,
                    "total_chunks": total_chunks,
                },
                headers={"X-Worker-Token": os.environ.get("WORKER_SHARED_SECRET", "")},
                timeout=10,
            )
        except Exception:
            log.error("_report_checkpoint: failed to report checkpoint for connection=%s chunk_seq=%s "
                      "— re-raising so the worker retry/dead-letter path handles it (v1.2.25 Task 2)",
                      connection_id, chunk_seq, exc_info=True)
            raise


class CDCTransformTask:
    """
    Handles a batch of CDC events that have a transform pipeline:
      1. Receive event batch from Redis Streams
      2. Apply transform pipeline via DuckDB
      3. Upsert to destination Postgres
    """

    def __init__(self, engine: "DuckDBTransformEngine"):
        self.engine = engine

    def run(self, task: dict):
        connection_id = task["connection_id"]
        events = task.get("events", [])   # list of CDC row dicts
        steps = task.get("transform_steps", [])
        dest = task.get("destination") or {}
        connector_type = dest.get("connector_type") or task.get("dest_connector_type", "postgres")
        schema = task.get("dest_schema", "dw")
        table = task.get("dest_table", "data")
        pk_col = task.get("primary_key", "id")
        source = task.get("source") or {}
        source_schema_name = task.get("source_schema") or ""

        # Derive the destination DSN. Prefer an explicit dest_dsn on the task
        # (legacy path); otherwise build it from the destination block's
        # connection_config via the type-aware dispatcher — the control-plane
        # transform-route endpoint populates connection_config.password with
        # the decrypted plaintext so the worker can build a usable DSN for
        # Postgres / MySQL / MongoDB without the Fernet key. Without this,
        # CDC silently no-ops because dest_dsn is empty and the upsert branch
        # is skipped (see the `elif dest_dsn:` guard below). Unknown
        # destination types return "" so the batch is logged + dropped.
        dest_dsn = task.get("dest_dsn", "")
        if not dest_dsn and connector_type != "iceberg":
            dest_dsn = _dest_dsn_from_dest(dest)
            if dest_dsn:
                log.debug("CDCTransform derived dest_dsn from destination block for connection=%s", connection_id)

        log.info("CDCTransform connection=%s events=%d dest=%s", connection_id, len(events), connector_type)

        if not events:
            return

        # v1.2.22 Bug A fix / Fix C1: fetch the source schema ONCE per task
        # (not per event) and reuse it for every batch. CDC tasks are short
        # so this is a single round-trip to information_schema per task.
        cached_source_schema: "pa.Schema | None" = None
        if connector_type == "iceberg":
            try:
                from iceberg_writer import _get_source_schema
                cached_source_schema = _get_source_schema(source, source_schema_name, table)
            except Exception:
                log.exception("CDCTransform connection=%s _get_source_schema failed — falling back to per-batch inference",
                              connection_id)
                cached_source_schema = None

        # Separate INSERT/UPDATE rows from DELETEs
        to_upsert = [e["after"] for e in events if e.get("op") in ("INSERT", "UPDATE") and e.get("after")]
        to_delete_pks = [e["before"][pk_col] for e in events if e.get("op") == "DELETE" and e.get("before")]

        cached_transformed_schema: "pa.Schema | None" = None
        if to_upsert and steps:
            to_upsert, _, cached_transformed_schema = self.engine.execute_pipeline(
                to_upsert, steps, schema=cached_source_schema,
            )

        if connector_type == "iceberg":
            self._apply_to_iceberg(to_upsert, to_delete_pks, dest, table, pk_col,
                                   schema=cached_transformed_schema or cached_source_schema)
        elif dest_dsn:
            self._upsert(to_upsert, to_delete_pks, dest_dsn, schema, table, pk_col)
        else:
            log.error(
                "CDCTransform connection=%s cannot write to %s destination: "
                "no dest_dsn and destination block missing/incomplete or "
                "connector_type unsupported — dropping %d events",
                connection_id, connector_type, len(events),
            )

    def _apply_to_iceberg(self, rows: list[dict], delete_pks: list,
                          dest: dict, table: str, pk_col: str,
                          schema: "pa.Schema | None" = None):
        from iceberg_writer import IcebergWriter
        dest_config = dest.get("connection_config") or dest.get("config") or dest
        identifier_fields = dest_config.get("identifier_fields") or [pk_col]
        writer = IcebergWriter(dest_config)
        if rows:
            writer.upsert(rows, table_name=table, identifier_fields=identifier_fields,
                          schema=schema)
        if delete_pks:
            writer.delete(table_name=table,
                          identifier_fields=identifier_fields,
                          delete_keys=delete_pks)

    def _upsert(self, rows: list[dict], delete_pks: list,
                dsn: str, schema: str, table: str, pk_col: str):
        if not rows and not delete_pks:
            return

        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                if rows:
                    columns = list(rows[0].keys())
                    non_pk = [c for c in columns if c != pk_col]
                    placeholders = ", ".join(["%s"] * len(columns))
                    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in non_pk)
                    sql = (
                        f"INSERT INTO {schema}.{table} ({', '.join(columns)}) "
                        f"VALUES ({placeholders}) "
                        f"ON CONFLICT ({pk_col}) DO UPDATE SET {update_clause}"
                    )
                    cur.executemany(sql, [tuple(r.values()) for r in rows])

                if delete_pks:
                    cur.execute(
                        f"DELETE FROM {schema}.{table} WHERE {pk_col} = ANY(%s)",
                        (delete_pks,),
                    )
                conn.commit()
