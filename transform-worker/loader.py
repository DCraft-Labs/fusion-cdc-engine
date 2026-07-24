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

# v1.3.0 Fix 1: DuckDB ATTACH connection-string escaping. The previous code
# interpolated host/port/database/user/password into the ATTACH string via
# raw f-strings, so a password containing ``;``, ``=``, ``'``, ``\``, or
# control chars could break the key=value parse or inject extra kv pairs
# (connection-string injection). ``_duckdb_attach_kv`` escapes each value
# per DuckDB's libpq/mysql connector kv rules and joins with a space.
# v1.3.1 Fix 1: the v1.3.0 helper joined with ``;`` but DuckDB's
# mysql_scanner / postgres_scanner DSN parser expects SPACE-separated
# key=value pairs (``;`` triggers ``Unrecognized configuration parameter
# ""`` at parse time, silently re-breaking DuckDB bulk mode). Reverted
# the join separator to space; the escaping logic is unchanged.
# v1.3.2 Fix 4 (carried from v1.3.1 follow-up): live testing confirmed
# DuckDB's mysql_scanner DSN parser only accepts ``\\`` and ``\;`` as
# backslash escapes. The v1.3.0/v1.3.1 helper ALSO escaped ``=`` and
# ``'`` (``\=`` and ``\'``), but ``\=`` triggers
# ``Unrecognized configuration parameter`` and ``\'`` breaks the outer
# SQL string literal. Drop the ``\=`` / ``\'`` escaping; keep ``\\`` and
# ``\;`` (those are correct and required) and control-char ``\uXXXX``.
# IMPORTANT: this means spaces, ``=``, and ``'`` in passwords are NOT
# supported by the DuckDB mysql_scanner DSN parser — operators with such
# passwords must change the password or use a different mechanism (the
# parser uses unescaped ``=`` as the key/value split and unescaped spaces
# as the pair separator, so neither can appear inside a value).
_DUCKDB_ATTACH_ESCAPE_CHARS = {"\\": "\\\\", ";": "\\;"}


def _duckdb_attach_kv(**kwargs) -> str:
    """Build an escaped DuckDB ATTACH key=value connection string.

    Each keyword argument becomes ``key=escaped_value``; pairs are joined
    with a single space (DuckDB's mysql_scanner/postgres_scanner DSN
    parser expects space-separated key=value pairs). Values are coerced
    to ``str`` and escaped so that the metacharacters ``\\`` and ``;``
    and ASCII control chars cannot break out of their value position.
    ``None`` values are skipped (so optional keys like ``port`` can be
    omitted cleanly).

    v1.3.2 Fix 4: ``=`` and ``'`` are NO LONGER escaped — DuckDB's
    mysql_scanner DSN parser only honours ``\\`` and ``\\;`` as backslash
    escapes; ``\\=`` triggers ``Unrecognized configuration parameter`` and
    ``\\'`` breaks the outer SQL string literal. As a consequence,
    spaces, ``=``, and ``'`` in passwords are NOT supported by the
    DuckDB mysql_scanner DSN parser — operators with such passwords
    must change the password or use a different mechanism. The caller
    is expected to reject such passwords upstream (see
    ``test_v130_attach_escape.py`` for the documented unsupported-char
    behaviour)."""
    parts: list[str] = []
    for k, v in kwargs.items():
        if v is None:
            continue
        s = str(v)
        out = []
        for ch in s:
            if ch in _DUCKDB_ATTACH_ESCAPE_CHARS:
                out.append(_DUCKDB_ATTACH_ESCAPE_CHARS[ch])
            elif ord(ch) < 0x20:
                # Control chars -> backslash-u hex escape (rare in DSNs;
                # ensures they can never terminate a kv pair).
                out.append("\\u%04x" % ord(ch))
            else:
                out.append(ch)
        parts.append(f"{k}={''.join(out)}")
    return " ".join(parts)


def _duckdb_attach_unescape_kv(s: str) -> dict[str, str]:
    """Inverse of ``_duckdb_attach_kv`` for test round-trip verification:
    parse a space-separated ``k=v k=v`` string back into a dict,
    honouring backslash escapes. Exposed for tests; not used at runtime.

    v1.3.2 Fix 4: only ``\\`` and ``\\;`` are unescaped (matching the
    reduced escape set in ``_duckdb_attach_kv``). ``\\=`` and ``\\'`` are
    no longer produced by the encoder, so the decoder passes them
    through literally (the ``\\uXXXX`` control-char escape is still
    honoured)."""
    out: dict[str, str] = {}
    i = 0
    cur_key: list[str] = []
    cur_val: list[str] = []
    in_value = False
    while i < len(s):
        ch = s[i]
        if ch == "\\" and i + 1 < len(s):
            nxt = s[i + 1]
            if nxt == "\\":
                cur_val.append("\\") if in_value else cur_key.append("\\")
            elif nxt == ";":
                cur_val.append(";") if in_value else cur_key.append(";")
            elif nxt == "u" and i + 5 < len(s) + 1:
                # \uXXXX hex escape (4 hex digits follow)
                hexpart = s[i + 2:i + 6]
                if len(hexpart) == 4 and all(c in "0123456789abcdefABCDEF" for c in hexpart):
                    cur_val.append(chr(int(hexpart, 16))) if in_value else cur_key.append(chr(int(hexpart, 16)))
                    i += 6
                    continue
                cur_val.append(nxt) if in_value else cur_key.append(nxt)
            else:
                cur_val.append(nxt) if in_value else cur_key.append(nxt)
            i += 2
            continue
        if ch == "=" and not in_value:
            in_value = True
            i += 1
            continue
        if ch == " " and in_value:
            out["".join(cur_key)] = "".join(cur_val)
            cur_key = []
            cur_val = []
            in_value = False
            i += 1
            continue
        if in_value:
            cur_val.append(ch)
        else:
            cur_key.append(ch)
        i += 1
    if cur_key or in_value:
        out["".join(cur_key)] = "".join(cur_val)
    return out


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
# v1.2.33 Bug #22 fix 3 (IMMEDIATE MITIGATION): default commit batching to 1
# (one commit per chunk — the legacy v1.2.24 behavior). With commit_batch=1
# the checkpoint-advance-after-commit fix (Bug #22 fix 1) is trivially
# correct because there is no buffering — every chunk's write IS a commit,
# so last_pk advances exactly with durability. Operators who want higher
# throughput can opt in to larger batches via this env var, but the default
# is safe (no duplicate rows on retry-after-conflict).
# v1.2.37 §8 item 3: raise the default to 5. The prerequisites
# (checkpoint-after-commit, v1.2.33 Bug #22 fix 1; retry-gated dedup,
# v1.2.34 Bug #23 fix) are both in place, so a larger default is safe and
# pays the per-commit lock/reload/dedup tax ~5x less often per row. The
# env var still overrides for operators who want a different value.
INITIAL_LOAD_COMMIT_BATCH = int(os.environ.get("INITIAL_LOAD_COMMIT_BATCH", "5"))

# v1.2.29 Task 1: DuckDB native scanner bulk mode (default OFF). Operators
# opt in per connection via ``resource_limits.bulk_mode: "duckdb"`` or the
# env var. MongoDB has no DuckDB scanner and always uses the Python path.
BULK_MODE_DEFAULT = str(os.environ.get("INITIAL_LOAD_BULK_MODE", "none")).lower()
# v1.2.39 section 6: single-committer staging mode (default OFF). Operators
# opt in per connection via ``resource_limits.committer_mode: "staged"`` or
# the env var. When ``staged``, the iceberg write path writes Parquet files
# directly to ``table.location()/data/`` (NO catalog call) and RPUSHes the
# path onto the pending-files Redis list; a separate committer process
# (transform-worker/iceberg_committer.py) drains the list and registers
# all drained files in ONE ``table.transaction().add_files()`` commit. This
# is the standard "many writers, one table" pattern (Flink IcebergFiles-
# Committer, Adobe Consolidation Worker) and removes the v1.2.33-36 Redis
# mutex + dedup-on-PK workaround from the bulk-append path. The CDC
# upsert/delete path stays on the mutex (different conflict profile).
COMMITTER_MODE_DEFAULT = str(os.environ.get("INITIAL_LOAD_COMMITTER_MODE", "none")).lower()
# v1.2.29 Task 2: Prometheus metrics HTTP port for the transform-worker.
PROMETHEUS_PORT = int(os.environ.get("TRANSFORM_WORKER_PROMETHEUS_PORT", "9090"))


def _kill_mysql_thread(host, port, user, password, thread_id) -> None:
    """2026-07-24 fix: force-terminate a source-DB query we fired ourselves,
    once we've given up on it (timeout/exception), instead of just dropping
    our own client connection and hoping the server notices. Matters most
    against a shared/multi-tenant source (e.g. a live UAT MySQL instance
    also used by other teams) -- an abandoned client socket does not
    promptly free server-side query resources. Opens a brand-new,
    short-lived connection purely to issue ``KILL <thread_id>``; any
    failure here is logged and swallowed (this is best-effort cleanup,
    not allowed to mask the original error).

    Note: DuckDB bulk mode still lacks this protection — mysql_scanner
    does not expose the underlying MySQL thread id.
    """
    try:
        import pymysql
        killer = pymysql.connect(host=host, port=int(port), user=user,
                                  password=password, connect_timeout=5,
                                  read_timeout=5)
        try:
            with killer.cursor() as kc:
                kc.execute(f"KILL {int(thread_id)}")
            log.warning("KILLed orphaned source query thread_id=%s on %s:%s "
                        "after our client gave up on it", thread_id, host, port)
        finally:
            killer.close()
    except Exception as e:
        log.warning("failed to KILL source thread_id=%s on %s:%s (%s) "
                    "-- server will have to reclaim it on its own",
                    thread_id, host, port, e)


# ---------------------------------------------------------------------------
# v1.2.29 Task 2: per-chunk Prometheus metrics. Defined at import time so
# every worker pod shares the same metric registry. prometheus_client is
# already a transform-worker dependency; if it is absent we degrade to
# no-op metrics so the worker still runs.
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter as _PromCounter
    from prometheus_client import Histogram as _PromHistogram
    from prometheus_client import Gauge as _PromGauge

    INITIAL_LOAD_ROWS_TOTAL = _PromCounter(
        "initial_load_rows_total",
        "Rows written by the initial-load task, per chunk increment.",
        ["connection", "stream", "partition"],
    )
    INITIAL_LOAD_CHUNK_DURATION = _PromHistogram(
        "initial_load_chunk_duration_seconds",
        "Per-chunk latency of the initial-load fetch/convert/write phases.",
        ["connection", "stream", "phase"],
        buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0),
    )
    INITIAL_LOAD_CHUNKS_IN_FLIGHT = _PromGauge(
        "initial_load_chunks_in_flight",
        "1 while a chunk is being processed, 0 when idle.",
        ["connection", "stream", "partition"],
    )
    INITIAL_LOAD_CHECKPOINT_WRITES = _PromCounter(
        "initial_load_checkpoint_writes_total",
        "Checkpoint reports sent to the control-plane.",
        ["connection", "stream", "partition"],
    )
    INITIAL_LOAD_QUEUE_DEPTH = _PromGauge(
        "initial_load_queue_depth",
        "Number of prefetched chunks buffered in the fetch/write overlap queue.",
        ["connection", "stream", "partition"],
    )
    _PROM_AVAILABLE = True
except ImportError:  # pragma: no cover
    _PROM_AVAILABLE = False

    class _NoopMetric:
        def labels(self, *a, **kw):
            return self
        def inc(self, v=1):
            pass
        def observe(self, v):
            pass
        def set(self, v):
            pass

    INITIAL_LOAD_ROWS_TOTAL = _NoopMetric()
    INITIAL_LOAD_CHUNK_DURATION = _NoopMetric()
    INITIAL_LOAD_CHUNKS_IN_FLIGHT = _NoopMetric()
    INITIAL_LOAD_CHECKPOINT_WRITES = _NoopMetric()
    INITIAL_LOAD_QUEUE_DEPTH = _NoopMetric()


def _start_metrics_http_server() -> None:
    """v1.2.29 Task 2: start a background HTTP server exposing the Prometheus
    metrics on PROMETHEUS_PORT. Called once from worker.py main(). No-op when
    prometheus_client is missing or the port is 0.
    """
    if not _PROM_AVAILABLE or PROMETHEUS_PORT <= 0:
        return
    try:
        from prometheus_client import start_http_server
        start_http_server(PROMETHEUS_PORT)
        log.info("Prometheus metrics endpoint listening on :%d/metrics", PROMETHEUS_PORT)
    except Exception:
        log.exception("Failed to start Prometheus metrics server on port %d — continuing", PROMETHEUS_PORT)


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
        # v1.3.2 Fix 3: per-worker-process set tracking which (stream_id,
        # table_name) pairs have already emitted the "bulk mode bypasses
        # configured transform steps" warning. The warning fires once per
        # stream per worker, not per chunk (a 100-chunk load would otherwise
        # spam the log 100x). Lives on the InitialLoadTask instance; a new
        # instance is created per task (worker.py), so this is per-worker.
        self._bulk_transform_logged: set[str] = set()

    def _maybe_log_bulk_transform_run(self, stream_id, dest_table, steps) -> None:
        """v1.3.2 Fix 3b: one-time-per-stream INFO log when bulk mode runs transforms."""
        if not steps:
            return
        _key = f"{stream_id}:{dest_table}"
        if _key in self._bulk_transform_logged:
            return
        self._bulk_transform_logged.add(_key)
        log.info(
            "InitialLoad: bulk mode (kind=arrow) running %d transform "
            "step(s) on stream=%s table=%s (DuckDB in-place, no Python "
            "row-dict round-trip).",
            len(steps), stream_id, dest_table)

    def run(self, task: dict):
        connection_id = task["connection_id"]
        # v1.2.33 Bug #21 fix 3: stash the current connection_id on self so
        # the write-path helpers (_write_to_iceberg / _write_arrow_to_iceberg)
        # can pass it to IcebergWriter for the per-table commit mutex. A new
        # InitialLoadTask instance is created per task (see worker.py), so
        # this is safe — no cross-task leakage.
        self._current_connection_id = connection_id
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
        # v1.2.30 Defect C fix: the per-partition row estimate is now stamped
        # at ENQUEUE time by the control-plane (density-based: table_rows *
        # (pk_end - pk_start) / (max_pk - min_pk)). The worker stamps it on
        # the FIRST checkpoint for this partition and never overwrites it with
        # rows_written, so progress_pct = rows_written / rows_estimated * 100
        # reflects real progress instead of always reading 100%.
        rows_estimated = task.get("rows_estimated")
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

        # v1.2.33 Bug #22 fix 2: stash pk_col on self so the write-path helpers
        # can pass it to IcebergWriter for dedup-on-PK before append
        # (idempotency under retry-after-conflict).
        self._current_pk_col = pk_col
        # v1.2.34 Bug #23 fix: stash retry count so the dedup-on-PK delete is
        # ONLY run on retried tasks (retry_count > 0). On a first attempt there
        # is no prior commit to dedup against, so the delete scan is pure
        # overhead — and on unpartitioned Iceberg tables it scans every
        # manifest accumulated so far, growing 1:1 with commits and making
        # each commit progressively slower. Gating on retry_count > 0 removes
        # the cost from the common path while leaving dedup fully intact for
        # the exact retry-after-conflict scenario it was added to protect.
        self._current_retry_count = int(task.get("_retry_count", 0))

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

        # v1.2.33 Bug #22 fix 1 (PRIMARY): ``last_pk`` is the checkpoint cursor
        # and must only reflect rows that are DURABLE (committed to Iceberg /
        # COPY'd to Postgres). ``last_buffered_pk`` tracks rows that have been
        # fetched/transformed and buffered for a batched commit but NOT yet
        # committed. With commit_batch>1, advancing ``last_pk`` on every
        # buffered chunk would let a retry resume past rows that were never
        # committed — re-fetching and re-appending already-durable rows
        # (duplicate rows on retry-after-conflict). The checkpoint now advances
        # only after a successful commit; the final flush (after the loop)
        # promotes ``last_buffered_pk`` to ``last_pk``.
        last_buffered_pk = last_pk

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
        # v1.2.38 Finding B: the transformed+iceberg path now buffers Arrow
        # tables (not Python dicts) and flushes via ``write_arrow``. This
        # eliminates the ~47ms/10k-row chunk wasted Arrow↔Python round-trip
        # (master report §6f Finding B) and stacks with the v1.2.37 bulk-mode
        # fix. ``pending_child`` stays list[dict] (only json_flatten_child
        # produces child tables, a small/rare path).
        pending_arrow: list["pa.Table"] = []   # buffered transformed Arrow tables
        pending_child: dict[str, list[dict]] = {}
        chunks_since_commit = 0

        # chunk_counter is the running count of chunks processed within THIS
        # partition (used for compaction timing + current_chunk reporting).
        # ``chunk_seq`` (the partition index) is the composite-key identifier
        # and stays fixed for the whole task.
        chunk_counter = 0

        # v1.2.29 Task 1: DuckDB native scanner bulk mode. ``bulk_mode`` comes
        # from the task payload (producer reads it from the connection's
        # ``resource_limits.bulk_mode``) and falls back to the env default.
        # Only MySQL/Postgres sources are eligible — MongoDB has no DuckDB
        # scanner. When ``duckdb`` is selected the fetch returns a pa.Table
        # (zero-copy Arrow) and the convert step is skipped. On any scanner
        # failure we fall back to the Python path and log it.
        bulk_mode = str(task.get("bulk_mode") or BULK_MODE_DEFAULT).lower()
        if ctype == "mongodb":
            bulk_mode = "none"  # no DuckDB Mongo scanner
        use_duckdb_bulk = (bulk_mode == "duckdb") and ctype in ("postgres", "postgresql", "mysql")
        # v1.2.39 section 6: single-committer staging mode (opt-in). When
        # ``staged``, the iceberg write path stages Parquet files + RPUSHes
        # to the pending list instead of calling table.append/write_arrow.
        committer_mode = str(task.get("committer_mode") or COMMITTER_MODE_DEFAULT).lower()
        use_committer = (committer_mode == "staged") and (connector_type == "iceberg")
        duckdb_conn = None

        # v1.2.29 Task 5: connection pooling — open ONE source connection per
        # task and reuse it across all chunks in this partition. MongoDB
        # reuses its own client (pymongo pools internally). The fetchers
        # accept an optional ``conn`` param; when provided they reuse it.
        src_conn: "Any | None" = None
        if not use_duckdb_bulk and ctype in ("postgres", "postgresql", "mysql"):
            try:
                src_conn = self._open_source_connection(source, ctype)
            except Exception:
                log.exception("InitialLoad connection=%s pooled source connection open failed — falling back to per-chunk connect",
                              connection_id)
                src_conn = None

        # v1.2.29 Task 2: short metric label sets (avoid high cardinality).
        _m_conn = str(connection_id)
        _m_stream = str(stream_id or "")
        _m_part = str(chunk_seq)
        # v1.2.29 Task 6: backpressure — track how long the prefetch queue has
        # been full (writer behind reader). When > 60s we log a warning.
        queue_full_since: "float | None" = None

        # v1.2.26 Task 5: fetch/write overlap. A bounded queue holds at most
        # PIPELINE_QUEUE_SIZE prefetched chunks; a background fetch thread
        # produces chunks while the main thread consumes (convert + write).
        # This hides source-DB fetch latency behind Iceberg/object-store
        # write latency (different resources: network+DB vs object-store).
        prefetch_q: "queue.Queue[Any | None]" = queue.Queue(maxsize=max(1, PIPELINE_QUEUE_SIZE))
        fetch_exc: list[Exception] = []

        def _fetch_and_put(cursor_pk, limit):
            try:
                if use_duckdb_bulk and duckdb_conn is not None:
                    arrow_tbl = self._fetch_chunk_duckdb(
                        duckdb_conn, source, source_schema, source_table,
                        pk_col, cursor_pk, limit, ctype, pk_end,
                    )
                    # v1.2.33 Bug #20 fix: pass `limit` (the requested_size
                    # captured before the fetch) through the queue so the
                    # consumer's "was this chunk full?" check compares against
                    # the size that was actually requested — NOT the live
                    # cur_chunk_size, which the adaptive sizer may grow
                    # between the fetch and the check.
                    prefetch_q.put(("arrow", arrow_tbl, limit))
                else:
                    rows = self._fetch_chunk(source, source_schema, source_table,
                                             pk_col, cursor_pk, limit, ctype, pk_end,
                                             conn=src_conn)
                    prefetch_q.put(("rows", rows, limit))
            except Exception as exc:  # noqa: BLE001
                fetch_exc.append(exc)
                prefetch_q.put(None)

        # Prime the DuckDB scanner connection (Task 1). On failure, fall back
        # to the Python path for the rest of this task.
        if use_duckdb_bulk:
            try:
                duckdb_conn = self._open_duckdb_scanner(source, ctype)
            except Exception as exc:
                log.warning("InitialLoad connection=%s DuckDB scanner open failed (%s) — falling back to Python path",
                            connection_id, exc)
                use_duckdb_bulk = False
                duckdb_conn = None
                if ctype in ("postgres", "postgresql", "mysql"):
                    try:
                        src_conn = self._open_source_connection(source, ctype)
                    except Exception:
                        src_conn = None

        # Kick off the first fetch on a background thread (overlaps with
        # nothing yet, but primes the queue).
        first_fetch = threading.Thread(
            target=_fetch_and_put, args=(last_pk, cur_chunk_size), daemon=True,
        )
        first_fetch.start()

        # ── PK-bounded chunk loop (within this partition's [pk_start, pk_end]) ──
        # v1.2.30 Defect B fix: the loop body is wrapped in try/except below
        # so ANY exception (e.g. Iceberg "snapshot id changed" conflict from a
        # duplicate-dequeue sibling pod, or a transient write error) still
        # persists a "failed" checkpoint row for this chunk_seq before
        # re-raising to the worker's retry/dead-letter path. Without this, a
        # partition that crashed mid-load left NO checkpoint row, so the
        # control-plane could never report its real status and resume-on-
        # restart had nothing to resume from.
        while not STOP_EVENT.is_set():
            # v1.2.29 Task 6: surface queue depth + backpressure warning.
            INITIAL_LOAD_QUEUE_DEPTH.labels(_m_conn, _m_stream, _m_part).set(prefetch_q.qsize())
            if prefetch_q.qsize() >= max(1, PIPELINE_QUEUE_SIZE):
                if queue_full_since is None:
                    queue_full_since = time.monotonic()
                elif time.monotonic() - queue_full_since > 60.0:
                    log.warning("InitialLoad connection=%s chunk_seq=%d — prefetch queue full for >60s; writer is behind reader, destination may be the bottleneck",
                                connection_id, chunk_seq)
                    queue_full_since = time.monotonic()
            else:
                queue_full_since = None

            item = prefetch_q.get()
            INITIAL_LOAD_QUEUE_DEPTH.labels(_m_conn, _m_stream, _m_part).set(prefetch_q.qsize())
            if item is None:
                # Fetch thread failed — surface the exception.
                if fetch_exc:
                    log.exception("InitialLoad connection=%s fetch thread failed — stopping range %d",
                                  connection_id, chunk_seq, exc_info=fetch_exc[0])
                    self._report_checkpoint(connection_id, stream_id, source_table,
                                            chunk_seq, 0, last_pk, state="failed",
                                            total_chunks=total_chunks)
                break
            kind, payload, requested_size = item
            rows = payload if kind == "rows" else None
            arrow_tbl = payload if kind == "arrow" else None

            if (kind == "rows" and not rows) or (kind == "arrow" and arrow_tbl is None):
                log.info("InitialLoad connection=%s table=%s.%s chunk_seq=%d — no more rows in range, load complete",
                         connection_id, source_schema, source_table, chunk_seq)
                break

            # v1.2.29 Task 2: in-flight gauge.
            INITIAL_LOAD_CHUNKS_IN_FLIGHT.labels(_m_conn, _m_stream, _m_part).set(1)
            t_fetch_end = time.monotonic()

            if kind == "arrow":
                row_count = arrow_tbl.num_rows
                next_pk = self._extract_pk_from_arrow(arrow_tbl, pk_col)
            else:
                row_count = len(rows)
                next_pk = self._extract_pk(rows[-1], pk_col, ctype)

            # v1.2.30 Defect A fix: start the next fetch NOW (before the write)
            # so the source-DB read of chunk N+1 overlaps with the convert+write
            # of chunk N. v1.2.30 replaces the old "only prefetch when the chunk
            # was full" heuristic — for a BOUNDED partition (pk_end is not None)
            # a short chunk near the boundary is expected (the remaining PK range
            # is smaller than chunk_size) and does NOT mean the partition is
            # done. We now prefetch the next chunk unless we have positively
            # reached the end of this partition:
            #   (a) crossed the upper bound (next_pk >= pk_end), or
            #   (b) unbounded last partition (pk_end is None) AND short chunk
            #       (legacy end-of-table heuristic), or
            #   (c) the fetch returned 0 rows (handled above, but guarded here).
            # Without this, a bounded partition that returns a short chunk near
            # the boundary would deadlock on the next prefetch_q.get() because
            # no producer thread was started.
            reached_end = False
            if row_count == 0:
                reached_end = True
            elif pk_end is not None and next_pk is not None and next_pk >= pk_end:
                reached_end = True
            elif pk_end is None and row_count < requested_size:
                # v1.2.33 Bug #20 fix: compare against `requested_size` (the
                # size captured before the fetch), NOT the live cur_chunk_size.
                # The adaptive sizer may grow cur_chunk_size between the fetch
                # and this check (e.g. 10000 -> 20000 after a fast streak); a
                # full chunk that returned exactly 10000 rows would otherwise
                # be compared against the new 20000 target and falsely exit.
                reached_end = True
            if not reached_end:
                threading.Thread(
                    target=_fetch_and_put, args=(next_pk, cur_chunk_size), daemon=True,
                ).start()

            # v1.2.30 Defect B fix: wrap convert+write in try/except so an
            # exception (e.g. Iceberg "snapshot id changed" conflict from a
            # duplicate-dequeue sibling pod, or a transient write error)
            # persists a "failed" checkpoint for this chunk_seq before
            # re-raising to the worker retry/dead-letter path. Without this
            # a partition that crashed mid-load left NO checkpoint row.
            try:
                t0 = time.monotonic()
    
                # v1.2.29 Task 1: DuckDB bulk path — scanner already produced a
                # typed Arrow table; skip Python convert and write Arrow directly
                # to Iceberg (the fast path). Transform steps are NOT applied in
                # bulk mode (bulk = raw 1:1 snapshot for speed).
                # v1.3.2 Fix 3: observability — if transform ``steps`` are
                # configured for this stream, bulk mode silently drops them.
                # Emit a one-time-per-stream WARNING so operators can detect the
                # misconfiguration (either disable bulk mode or remove the
                # transform config). The warning helper is gated on ``if steps``.
                if kind == "arrow" and connector_type == "iceberg":
                    if steps:
                        self._maybe_log_bulk_transform_run(stream_id, dest_table, steps)
                        t_c0 = time.monotonic()
                        try:
                            arrow_tbl = self.engine.execute_pipeline_arrow_in_place(
                                arrow_tbl, steps, schema=cached_transformed_schema)
                            if cached_transformed_schema is None and arrow_tbl is not None:
                                cached_transformed_schema = arrow_tbl.schema
                        except Exception as bulk_xf_exc:
                            log.warning(
                                "InitialLoad: bulk-mode transform failed for "
                                "stream=%s table=%s chunk_seq=%d (%s); falling "
                                "back to Python-mode transform path for this "
                                "chunk.",
                                stream_id, dest_table, chunk_seq, bulk_xf_exc)
                            try:
                                rows_pylist = arrow_tbl.to_pylist()
                                arrow_tbl, _child_fb, _sch_fb = \
                                    self.engine.execute_pipeline_arrow(
                                        rows_pylist, steps,
                                        schema=cached_source_schema)
                                if cached_transformed_schema is None and _sch_fb is not None:
                                    cached_transformed_schema = _sch_fb
                            except Exception:
                                log.exception(
                                    "InitialLoad: Python-mode fallback also "
                                    "failed for stream=%s table=%s chunk_seq=%d; "
                                    "re-raising original bulk exception.",
                                    stream_id, dest_table, chunk_seq)
                                raise bulk_xf_exc
                        INITIAL_LOAD_CHUNK_DURATION.labels(_m_conn, _m_stream, "convert").observe(time.monotonic() - t_c0)
                    else:
                        INITIAL_LOAD_CHUNK_DURATION.labels(_m_conn, _m_stream, "convert").observe(0.0)
                    t_w0 = time.monotonic()
                    if use_committer:
                        # v1.2.39 section 6: stage Parquet file + RPUSH to
                        # pending list (NO catalog call). The committer
                        # process drains and commits in ONE add_files() call.
                        self._stage_arrow_to_pending(
                            arrow_tbl, dest, dest_table,
                            partition_id=str(chunk_seq), chunk_seq=chunk_seq,
                            pk_range=(last_pk, next_pk), stream_id=stream_id,
                            source_table=source_table, connection_id=connection_id,
                        )
                        rows_written = int(arrow_tbl.num_rows)
                    else:
                        rows_written = self._write_arrow_to_iceberg(arrow_tbl, dest, dest_table)
                    INITIAL_LOAD_CHUNK_DURATION.labels(_m_conn, _m_stream, "write").observe(time.monotonic() - t_w0)
                    child_tables = {}
                    # v1.2.33 Bug #22 fix 1: write_arrow IS the commit — advance
                    # the checkpoint cursor only after it succeeds. In committer
                    # mode, the file is staged (not yet committed) but the
                    # staged cursor advances so the fetch loop continues; the
                    # committer promotes the checkpoint state to "durable".
                    last_pk = next_pk
                elif kind == "arrow":
                    t_c0 = time.monotonic()
                    transformed = arrow_tbl.to_pylist()
                    INITIAL_LOAD_CHUNK_DURATION.labels(_m_conn, _m_stream, "convert").observe(time.monotonic() - t_c0)
                    child_tables = {}
                    transformed_schema = cached_source_schema
                    dest_dsn = _dest_dsn_from_dest(dest)
                    if not dest_dsn:
                        log.error("InitialLoad connection=%s cannot derive dest_dsn for connector_type=%s — destination block missing/incomplete. Stopping load after %d rows.",
                                  connection_id, connector_type, total_rows)
                        self._report_checkpoint(connection_id, stream_id, source_table,
                                                chunk_seq, 0, last_pk, state="failed",
                                                total_chunks=total_chunks)
                        return
                    t_w0 = time.monotonic()
                    rows_written = self._copy_to_postgres(transformed, dest_dsn, dest_schema, dest_table)
                    INITIAL_LOAD_CHUNK_DURATION.labels(_m_conn, _m_stream, "write").observe(time.monotonic() - t_w0)
                    # v1.2.33 Bug #22 fix 1: COPY IS the commit — advance
                    # the checkpoint cursor only after it succeeds.
                    last_pk = next_pk
                else:
                    # Apply transforms. v1.2.38 Finding B: branch on
                    # connector_type so the iceberg write path stays in Arrow
                    # (no .to_pylist() + _rows_to_arrow round-trip, ~47ms/10k
                    # rows saved per master report §6f Finding B), while the
                    # postgres COPY path keeps the list[dict] format it needs.
                    # Finding A: the underlying DuckDB connection is pooled
                    # on the engine instance either way.
                    if connector_type == "iceberg":
                        if steps:
                            t_c0 = time.monotonic()
                            arrow_out, child_tables, transformed_schema = self.engine.execute_pipeline_arrow(
                                rows, steps, schema=cached_source_schema,
                            )
                            INITIAL_LOAD_CHUNK_DURATION.labels(_m_conn, _m_stream, "convert").observe(time.monotonic() - t_c0)
                            if cached_transformed_schema is None and transformed_schema is not None:
                                cached_transformed_schema = transformed_schema
                        else:
                            # No transforms — wrap the Python rows in an Arrow
                            # table so the write path is uniform. (Bulk-mode
                            # already returns Arrow and never reaches here.)
                            arrow_out = pa.Table.from_pylist(rows, schema=cached_source_schema) \
                                if cached_source_schema is not None else pa.Table.from_pylist(rows)
                            child_tables = {}
                            transformed_schema = cached_source_schema

                        if use_committer:
                            # v1.2.39 section 6: stage each chunk's Arrow
                            # table to a Parquet file + RPUSH to the pending
                            # list (NO catalog call, NO batch buffering - each
                            # staged file is independently inert until the
                            # committer picks it up). The committer drains and
                            # commits in ONE add_files() call.
                            if arrow_out is not None and arrow_out.num_rows > 0:
                                self._stage_arrow_to_pending(
                                    arrow_out, dest, dest_table,
                                    partition_id=str(chunk_seq),
                                    chunk_seq=chunk_seq,
                                    pk_range=(last_pk, next_pk),
                                    stream_id=stream_id,
                                    source_table=source_table,
                                    connection_id=connection_id,
                                )
                            for child_name, child_rows in child_tables.items():
                                if child_rows:
                                    pending_child.setdefault(child_name, []).extend(child_rows)
                            chunks_since_commit += 1
                            rows_written = arrow_out.num_rows if arrow_out is not None else 0
                            # Staged cursor advances immediately (the file is
                            # written); the committer promotes the checkpoint
                            # state to "durable" after its commit confirms.
                            last_pk = next_pk
                            # Flush any child tables (still dict-based) at the
                            # batch boundary.
                            if chunks_since_commit >= commit_batch and pending_child:
                                for child_name, child_rows in pending_child.items():
                                    if child_rows:
                                        self._write_to_iceberg(child_rows, dest, child_name)
                                pending_child = {}
                                chunks_since_commit = 0
                        else:
                            if arrow_out is not None and arrow_out.num_rows > 0:
                                pending_arrow.append(arrow_out)
                            for child_name, child_rows in child_tables.items():
                                if child_rows:
                                    pending_child.setdefault(child_name, []).extend(child_rows)
                            chunks_since_commit += 1
                            rows_written = arrow_out.num_rows if arrow_out is not None else 0
                            if chunks_since_commit >= commit_batch:
                                t_w0 = time.monotonic()
                                self._flush_iceberg_batch_arrow(
                                    pending_arrow, pending_child, dest, dest_table,
                                    schema=cached_transformed_schema or cached_source_schema,
                                )
                                INITIAL_LOAD_CHUNK_DURATION.labels(_m_conn, _m_stream, "write").observe(time.monotonic() - t_w0)
                                pending_arrow = []
                                pending_child = {}
                                chunks_since_commit = 0
                                # v1.2.33 Bug #22 fix 1 (PRIMARY): the batched
                                # commit just succeeded — NOW it is safe to advance
                                # the checkpoint cursor. Advancing on every buffered
                                # chunk (the old behavior) let a retry resume past
                                # rows that were never committed, re-appending
                                # already-durable rows on retry-after-conflict.
                                last_pk = next_pk
                    else:
                        # Postgres COPY path: keep list[dict] (COPY expects
                        # Python rows, not Arrow).
                        if steps:
                            t_c0 = time.monotonic()
                            transformed, child_tables, transformed_schema = self.engine.execute_pipeline(
                                rows, steps, schema=cached_source_schema,
                            )
                            INITIAL_LOAD_CHUNK_DURATION.labels(_m_conn, _m_stream, "convert").observe(time.monotonic() - t_c0)
                            if cached_transformed_schema is None and transformed_schema is not None:
                                cached_transformed_schema = transformed_schema
                        else:
                            transformed, child_tables = rows, {}
                            transformed_schema = cached_source_schema

                        dest_dsn = _dest_dsn_from_dest(dest)
                        if not dest_dsn:
                            log.error("InitialLoad connection=%s cannot derive dest_dsn for connector_type=%s — destination block missing/incomplete. Stopping load after %d rows.",
                                      connection_id, connector_type, total_rows)
                            self._report_checkpoint(connection_id, stream_id, source_table,
                                                    chunk_seq, 0, last_pk, state="failed",
                                                    total_chunks=total_chunks)
                            return
                        t_w0 = time.monotonic()
                        rows_written = self._copy_to_postgres(transformed, dest_dsn, dest_schema, dest_table)
                        for child_name, child_rows in child_tables.items():
                            if child_rows:
                                self._copy_to_postgres(child_rows, dest_dsn, dest_schema, child_name)
                        INITIAL_LOAD_CHUNK_DURATION.labels(_m_conn, _m_stream, "write").observe(time.monotonic() - t_w0)
                        # v1.2.33 Bug #22 fix 1: COPY IS the commit — advance
                        # the checkpoint cursor only after it succeeds.
                        last_pk = next_pk
    
            except Exception:
                log.exception("InitialLoad connection=%s chunk_seq=%d convert/write failed - reporting failed checkpoint and re-raising",
                            connection_id, chunk_seq)
                try:
                    self._report_checkpoint(connection_id, stream_id, source_table,
                                            chunk_seq, 0, last_pk, state="failed",
                                            total_chunks=total_chunks)
                except Exception:
                    log.error("InitialLoad: failed-checkpoint report also failed for connection=%s chunk_seq=%s",
                              connection_id, chunk_seq, exc_info=True)
                raise
            # v1.2.29 Task 2: fetch-phase duration (loop-gap approximation).
            INITIAL_LOAD_CHUNK_DURATION.labels(_m_conn, _m_stream, "fetch").observe(max(0.0, t0 - t_fetch_end))

            latency = time.monotonic() - t0
            total_rows += rows_written
            chunk_counter += 1
            # v1.2.33 Bug #22 fix 1: track the buffered (possibly-uncommitted)
            # cursor separately. ``last_pk`` (the checkpoint cursor) is advanced
            # only inside each write path AFTER a successful commit (see above).
            # With commit_batch=1 (the default, Bug #22 fix 3) every chunk
            # commits so last_pk == last_buffered_pk every iteration; with
            # commit_batch>1 last_pk lags until the flush succeeds.
            last_buffered_pk = next_pk

            # v1.2.29 Task 2: per-chunk row counter.
            INITIAL_LOAD_ROWS_TOTAL.labels(_m_conn, _m_stream, _m_part).inc(rows_written)

            # Report checkpoint as "running" so a restart resumes here. The
            # composite key (connection_id, stream_id, chunk_seq) means each
            # of the K pods writes its own row — no stomping.
            # v1.2.29 Task 3: on the first chunk, also stamp the partition's
            # [pk_start, pk_end] bounds and a rough rows estimate so the
            # progress endpoint can compute % and ETA without re-querying.
            if chunk_counter == 1:
                self._report_checkpoint(connection_id, stream_id, source_table,
                                        chunk_seq, rows_written, last_pk, state="running",
                                        total_chunks=total_chunks,
                                        pk_start=pk_start, pk_end=pk_end,
                                        rows_estimated=rows_estimated)
            else:
                self._report_checkpoint(connection_id, stream_id, source_table,
                                        chunk_seq, rows_written, last_pk, state="running",
                                        total_chunks=total_chunks)
            INITIAL_LOAD_CHECKPOINT_WRITES.labels(_m_conn, _m_stream, _m_part).inc()
            log.info("InitialLoad connection=%s chunk_seq=%d chunk=%d done — %d rows (total %d) last_pk=%s latency=%.2fs cs=%d bulk=%s",
                     connection_id, chunk_seq, chunk_counter, rows_written, total_rows,
                     last_pk, latency, cur_chunk_size, "duckdb" if kind == "arrow" else "python")

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

            # v1.2.30 Defect A fix: replaced the single-partition-era
            # ``if row_count < cur_chunk_size: break`` heuristic with an
            # explicit end-of-partition check. For a BOUNDED partition
            # (pk_end is not None) the loop continues fetching while
            # ``last_pk < pk_end`` — a short chunk near the boundary is
            # expected (the remaining PK range is smaller than chunk_size)
            # and is NOT end-of-partition. The loop only stops when:
            #   (a) ``last_pk >= pk_end`` (crossed the upper bound — the
            #       fetch already clamps to ``pk <= pk_end`` so the last
            #       chunk ends exactly at the boundary), or
            #   (b) pk_end is None (unbounded last partition) AND a short
            #       chunk (legacy end-of-table heuristic).
            # The empty-payload case (row_count == 0) is handled above by
            # breaking out of the loop before reaching here. The prefetch
            # thread is primed above only when ``not reached_end``, so a
            # short chunk in a bounded partition still has a next fetch in
            # flight and the loop continues.
            if pk_end is not None:
                if last_pk is not None and last_pk >= pk_end:
                    break
                # Bounded partition, short chunk, last_pk < pk_end → continue
                # fetching (the next chunk starts from last_pk and is clamped
                # to pk <= pk_end). Do NOT break on ``row_count < cur_chunk_size``.
            else:
                # Unbounded last partition: short chunk means end of table.
                # v1.2.33 Bug #20 fix: compare against `requested_size` (the
                # size captured before the fetch), NOT the live cur_chunk_size
                # — the adaptive sizer may grow cur_chunk_size between the
                # fetch and this check, causing a full chunk to falsely exit.
                if row_count < requested_size:
                    break

            # Fix C4: release the chunk's memory before fetching the next.
            # v1.2.38 Finding B: the transformed+iceberg path now produces
            # ``arrow_out`` (pa.Table) instead of ``transformed`` (list[dict]);
            # the postgres path still produces ``transformed``. del each
            # path-specific binding if it was assigned this iteration.
            if kind == "arrow":
                del arrow_tbl, child_tables
                arrow_tbl = None
            else:
                del rows, child_tables
            try:
                del transformed
            except NameError:
                pass
            try:
                del arrow_out
            except NameError:
                pass
            # v1.2.29 Task 2: chunk done — back to idle.
            INITIAL_LOAD_CHUNKS_IN_FLIGHT.labels(_m_conn, _m_stream, _m_part).set(0)

        # v1.2.29 Task 5: close the pooled source connection and the DuckDB
        # scanner connection now that all chunks in this partition are read.
        if duckdb_conn is not None:
            try:
                duckdb_conn.close()
            except Exception:
                pass
        if src_conn is not None:
            try:
                src_conn.close()
            except Exception:
                pass

        # v1.2.26 Task 7: flush any remaining buffered Iceberg batch.
        if connector_type == "iceberg" and pending_arrow:
            self._flush_iceberg_batch_arrow(
                pending_arrow, pending_child, dest, dest_table,
                schema=cached_transformed_schema or cached_source_schema,
            )
            pending_arrow = []
            pending_child = {}
            chunks_since_commit = 0
            # v1.2.33 Bug #22 fix 1: the final buffered batch is now durable —
            # promote the buffered cursor to the checkpoint cursor.
            last_pk = last_buffered_pk

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

        v1.2.38 Finding B: the transformed+iceberg path now uses
        ``_flush_iceberg_batch_arrow`` (Arrow-native, no Python dict
        intermediate). This dict-based helper is retained for the CDC
        upsert path and any caller that already has list[dict] in hand.
        """
        if not rows:
            return 0
        written = self._write_to_iceberg(rows, dest, table_name, schema=schema)
        for child_name, child_rows in child_tables.items():
            if child_rows:
                self._write_to_iceberg(child_rows, dest, child_name)
        return written

    def _flush_iceberg_batch_arrow(self, arrow_tables: list, child_tables: dict,
                                   dest: dict, table_name: str,
                                   schema: "pa.Schema | None" = None) -> int:
        """v1.2.38 Finding B: flush a buffered batch of transformed Arrow
        tables to Iceberg in ONE ``table.append`` (one commit) via
        ``IcebergWriter.write_arrow`` — no Python dict intermediate.

        Concatenates the buffered Arrow tables (``pa.concat_tables`` with
        ``promote_options="default"`` so nullable new columns from schema
        drift across chunks merge cleanly), then delegates to
        ``_write_arrow_to_iceberg``. Child tables stay on the dict path
        (only ``json_flatten_child`` produces them, a small/rare path).
        Returns the row count written.
        """
        if not arrow_tables:
            return 0
        # Filter out empty tables (concat_tables errors on a list of all-empty
        # tables with mismatched schemas; an empty table contributes 0 rows
        # anyway).
        non_empty = [t for t in arrow_tables if t is not None and t.num_rows > 0]
        if not non_empty:
            return 0
        try:
            combined = pa.concat_tables(non_empty, promote_options="default")
        except Exception as e:
            # If concat fails (e.g. irreconcilable schema drift across
            # chunks), fall back to flushing each table individually so
            # the batch is still durably committed — just in N commits
            # instead of 1. Log and proceed.
            log.warning("InitialLoad: pa.concat_tables failed (%s) — flushing %d Arrow tables individually.",
                        e, len(non_empty))
            written = 0
            for t in non_empty:
                written += self._write_arrow_to_iceberg(t, dest, table_name)
        else:
            written = self._write_arrow_to_iceberg(combined, dest, table_name)
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
        writer = IcebergWriter(dest_config, redis_client=getattr(self, "redis", None),
                               connection_id=getattr(self, "_current_connection_id", None))
        # v1.2.33 Bug #22 fix 2: pass pk_col so IcebergWriter dedup-on-PK
        # (delete-then-append) before each batch — idempotency safeguard.
        # v1.2.34 Bug #23 fix: gate dedup on retry_count > 0. On a first
        # attempt there is no prior commit to dedup against, so the delete
        # scan is pure overhead (and on unpartitioned tables it scans every
        # manifest, growing 1:1 with commits). Only retried tasks can have
        # a prior successful commit whose rows need deleting.
        _pk = getattr(self, "_current_pk_col", None)
        _dedup_pk = _pk if getattr(self, "_current_retry_count", 0) > 0 else None
        return writer.write_batch(rows, table_name=table_name, schema=schema,
                                  pk_col=_dedup_pk)

    def _fetch_chunk(self, source: dict, schema_name: str, table_name: str,
                    pk_col: str, last_pk, chunk_size: int, ctype: str,
                    pk_end=None, conn=None) -> list[dict]:
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
                                             schema_name, table_name, pk_col, last_pk, chunk_size, pk_end, conn=conn)
            if ctype == "mysql":
                return self._fetch_mysql_chunk(host, port or 3306, database, user, password,
                                                schema_name, table_name, pk_col, last_pk, chunk_size, pk_end, conn=conn)
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
                        pk_end=None, conn=None) -> list[dict]:
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
        # v1.2.29 Task 5: reuse a pooled connection when provided (one per
        # InitialLoadTask partition); otherwise open+close per call (legacy).
        owns_conn = conn is None
        if owns_conn:
            conn = psycopg2.connect(host=host, port=port, dbname=database,
                                    user=user, password=password,
                                    connect_timeout=10,
                                    application_name="fusion-cdc-initial-load")
            conn.autocommit = True
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if owns_conn:
                    cur.execute("BEGIN READ ONLY")
                try:
                    cur.execute(sql, tuple(params))
                    return [dict(r) for r in cur.fetchall()]
                finally:
                    if owns_conn:
                        cur.execute("COMMIT")
        finally:
            if owns_conn:
                conn.close()

    def _fetch_mysql_chunk(self, host, port, database, user, password,
                           schema_name, table_name, pk_col, last_pk, chunk_size,
                           pk_end=None, conn=None) -> list[dict]:
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
        # v1.2.29 Task 5: reuse a pooled connection when provided.
        owns_conn = conn is None
        if owns_conn:
            conn = pymysql.connect(host=host, port=int(port), database=database,
                                   user=user, password=password,
                                   cursorclass=pymysql.cursors.DictCursor,
                                   connect_timeout=10, read_timeout=120,
                                   autocommit=True)
        # 2026-07-24 fix: this source may be a shared/multi-tenant DB (e.g. a
        # UAT instance also used by other teams) -- a query we fire must
        # never be left running server-side after our client gives up on it.
        # Note: DuckDB bulk mode still lacks kill-on-failure (no thread-id
        # hook from mysql_scanner).
        _thread_id = None
        try:
            _thread_id = conn.thread_id()
        except Exception:
            pass
        try:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return list(cur.fetchall())
        except Exception:
            if _thread_id is not None:
                _kill_mysql_thread(host, port, user, password, _thread_id)
            raise
        finally:
            if owns_conn:
                try:
                    conn.close()
                except Exception:
                    pass

    # ── v1.2.29 Task 1 + Task 5: DuckDB native scanner + pooled source conn ──

    def _open_source_connection(self, source: dict, ctype: str):
        """v1.2.29 Task 5: open ONE source DB connection per partition and
        reuse it across all chunks. Returns a connection object or None.

        v1.2.30 Defect E fix: the pooled connection now uses the EXACT same
        param extraction as the per-chunk ``_fetch_chunk`` path
        (``database_name``/``database`` and ``username``/``user``), the same
        ``connect_timeout=10``, and (for MySQL) the same ``autocommit=True``
        + ``DictCursor``. The previous implementation read ``source["database"]``
        and ``source["user"]`` which are NOT the keys the producer stamps
        (it stamps ``database_name`` and ``username``), so the pooled
        connection opened with ``database=None`` / ``user=None`` and
        ProxySQL rejected it with "Access denied" — the worker then fell
        back to per-chunk connects (non-fatal but defeated the pooling win).
        """
        try:
            host = source.get("host") or ""
            port = source.get("port")
            # Match _fetch_chunk's extraction (database_name first, then database).
            database = (source.get("database_name") or source.get("database") or "")
            # Match _fetch_chunk's extraction (username first, then user).
            user = source.get("username") or source.get("user") or ""
            password = source.get("password") or ""
            if not host or not database or not user:
                log.warning("InitialLoad: pooled source connection skipped — incomplete source block (host=%s database=%s user=%s)",
                            host, database, user)
                return None
            if ctype in ("postgres", "postgresql"):
                import psycopg2
                conn = psycopg2.connect(host=host, port=port, dbname=database,
                                         user=user, password=password,
                                         connect_timeout=10,
                                         application_name="fusion-cdc-initial-load")
                conn.autocommit = True
                return conn
            if ctype == "mysql":
                import pymysql
                import pymysql.cursors
                return pymysql.connect(host=host, port=int(port or 3306), database=database,
                                        user=user, password=password,
                                        cursorclass=pymysql.cursors.DictCursor,
                                        connect_timeout=10, autocommit=True)
        except Exception as e:
            log.warning("InitialLoad: pooled source connection open failed (%s) — will fall back to per-chunk connect.", e)
        return None

    def _open_duckdb_scanner(self, source: dict, ctype: str):
        """v1.2.29 Task 1: attach DuckDB's native MySQL/Postgres scanner to
        the source DB once per partition. Returns a DuckDB connection (with
        the source ATTACHed as ``src``) or None on any failure.

        v1.2.37 Bug #25 fix (§7b): use ``database_name``/``username`` with
        fallback to ``database``/``user`` — identical to the sibling
        ``_open_source_connection`` ("v1.2.30 Defect E fix"). The producer
        stamps ``database_name``/``username`` into every task's source dict
        (control-plane/app/api/connections.py:612-613), never
        ``database``/``user``, so the previous ``source.get("database")`` /
        ``source.get("user")`` left both as None and the ATTACH failed with
        a misleading "access denied" — fixing the extension alone (Bug #26)
        was not sufficient; both fixes are required together to engage bulk
        mode.

        v1.2.37 Bug #26 fix (§7a): set ``extension_directory`` to a fixed,
        non-``$HOME``-dependent path (``/opt/duckdb_extensions``) right after
        ``duckdb.connect()``. The image builds as root (``HOME=/root``) but
        runs as the non-root ``transform`` user (``HOME=/app``); a naive
        ``RUN INSTALL mysql`` at build time lands in ``/root/.duckdb/...``
        but the runtime process looks in ``/app/.duckdb/...`` and won't find
        it. The Dockerfile bakes the mysql extension into
        ``/opt/duckdb_extensions`` (owned by ``transform``) and this line
        makes the runtime connection look there. Pin stays
        ``duckdb==0.10.3`` — an unpinned/bumped duckdb package would
        invalidate the pre-baked extension (fails loudly, per GitHub #16337).
        """
        try:
            import duckdb
            host = source.get("host"); port = source.get("port")
            # Match _fetch_chunk / _open_source_connection extraction
            # (database_name first, then database; username first, then user).
            database = source.get("database_name") or source.get("database")
            user = source.get("username") or source.get("user")
            password = source.get("password")
            conn = duckdb.connect()
            # 2026-07-24 fix: duckdb.connect() defaults its internal execution
            # thread pool to the HOST's hardware_concurrency() (the Docker
            # Desktop VM's core count), not this pod's cgroup CPU limit
            # (1000m). The community mysql_scanner extension's scan operator
            # is not safe to run from more than one DuckDB execution thread
            # concurrently against a single MYSQL_RES handle -- when DuckDB
            # parallelizes even a single-relation scan across >1 internal
            # threads, two threads call mysql_fetch_row()/mysql_next() on the
            # same result set concurrently, producing a native C-level crash
            # surfaced as "InternalException: MySQLResult::Next called
            # without result". Confirmed this was NOT source-connection
            # instability (isolated pymysql fetches against the same host/
            # port succeed) -- it is entirely our own DuckDB connection's
            # default parallelism. Force single-threaded execution for this
            # connection; the ATTACHed scan is I/O-bound over the network
            # anyway, so this costs nothing.
            conn.execute("SET threads=1")
            # Bug #26: look for the baked extension in the fixed path, not $HOME.
            conn.execute("SET extension_directory='/opt/duckdb_extensions'")
            if ctype in ("postgres", "postgresql"):
                conn.execute("LOAD postgres;")
                attach_str = _duckdb_attach_kv(
                    host=host, port=port, dbname=database,
                    user=user, password=password)
                conn.execute(
                    f"ATTACH 'postgres:{attach_str}' AS src (READ_ONLY)"
                )
            elif ctype == "mysql":
                conn.execute("LOAD mysql;")
                attach_str = _duckdb_attach_kv(
                    host=host, port=port, database=database,
                    user=user, password=password)
                conn.execute(
                    f"ATTACH 'mysql:{attach_str}' AS src (READ_ONLY)"
                )
            else:
                return None
            return conn
        except Exception as e:
            log.warning("InitialLoad: DuckDB scanner open failed (%s) — falling back to Python fetch.", e)
            try:
                conn.close()  # noqa: F821
            except Exception:
                pass
            return None

    def _fetch_chunk_duckdb(self, duckdb_conn, source: dict, schema_name: str,
                            table_name: str, pk_col: str, last_pk, chunk_size: int,
                            ctype: str, pk_end=None):
        """v1.2.29 Task 1: run a range-bounded SELECT against the ATTACHed
        source via DuckDB and return the result as a pyarrow Table."""
        if ctype in ("postgres", "postgresql"):
            qualified = (f'src."{schema_name}"."{table_name}"'
                         if schema_name else f'src."{table_name}"')
            pk_q = f'"{pk_col}"'
        else:  # mysql
            qualified = (f'src.`{schema_name}`.`{table_name}`'
                         if schema_name else f'src.`{table_name}`')
            pk_q = f"`{pk_col}`"
        # 2026-07-24 fix: DuckDB's named-parameter binder rejects a params
        # dict containing ANY key with no matching $key placeholder in the
        # SQL text ("Parameter argument/count mismatch, identifiers of the
        # excess parameters: ..."). last_pk/pk_end are OPTIONAL WHERE
        # clauses (omitted when None -- first chunk has no last_pk, last
        # partition has no pk_end), but the previous code unconditionally
        # included both keys in the params dict regardless of whether the
        # SQL actually referenced them. Confirmed live: 100% failure on the
        # first chunk of every partition (last_pk is None) and on every
        # unbounded last partition (pk_end is None). A failed param-bind
        # also appears to leave the DuckDB connection's pending-query state
        # broken for the NEXT call on the same (per-partition, reused)
        # connection, which is the likely cause of the separate
        # "InvalidInputException: closed pending query result" seen on
        # subsequent chunks of the same partition -- fixing the root cause
        # here should eliminate both.
        where_parts: list[str] = []
        params: dict[str, Any] = {"chunk_size": chunk_size}
        if last_pk is not None:
            where_parts.append(f"{pk_q} > $last_pk")
            params["last_pk"] = last_pk
        if pk_end is not None:
            where_parts.append(f"{pk_q} <= $pk_end")
            params["pk_end"] = pk_end
        where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
        sql = (f"SELECT * FROM {qualified} {where_clause} "
               f"ORDER BY {pk_q} ASC LIMIT $chunk_size")
        rel = duckdb_conn.execute(sql, params)
        return rel.fetch_arrow_table()

    def _extract_pk_from_arrow(self, arrow_tbl, pk_col: str, ctype: str) -> Any:
        """Pull the PK from the last row of an Arrow table for resume."""
        if arrow_tbl is None or arrow_tbl.num_rows == 0:
            return None
        try:
            col = arrow_tbl.column(pk_col)
            return col[-1].as_py()
        except Exception:
            return None

    def _write_arrow_to_iceberg(self, arrow_tbl, dest: dict, table_name: str) -> int:
        """v1.2.29 Task 1: write an Arrow table directly to Iceberg (no Python
        row-dict intermediate). Delegates to IcebergWriter.write_arrow."""
        from iceberg_writer import IcebergWriter  # local import; module already imported at top in prod
        try:
            # v1.3.2 Bug A fix: unwrap the dest wrapper dict the same way the
            # sibling _write_to_iceberg does. The control-plane producer emits
            # dest as {"connector_type": "iceberg", "connection_config": {...}}
            # — passing the raw wrapper to IcebergWriter causes load_catalog()
            # to miss ``catalog_type`` (defaults to "rest") and crash with
            # KeyError: 'catalog_uri' on 100% of iceberg writes. This path is
            # the default for every iceberg write since v1.2.38.
            _dest_cfg = dest.get("connection_config") or dest.get("config") or dest
            writer = IcebergWriter(_dest_cfg, redis_client=getattr(self, "redis", None),
                                   connection_id=getattr(self, "_current_connection_id", None))
            # v1.2.33 Bug #22 fix 2: pass pk_col so IcebergWriter dedup-on-PK
            # (delete-then-append) before each batch — idempotency safeguard.
            # v1.2.34 Bug #23 fix: gate dedup on retry_count > 0 (see write_batch
            # call site above for rationale).
            _pk = getattr(self, "_current_pk_col", None)
            _dedup_pk = _pk if getattr(self, "_current_retry_count", 0) > 0 else None
            return writer.write_arrow(arrow_tbl, table_name=table_name,
                                      pk_col=_dedup_pk)
        except Exception as e:
            log.error("InitialLoad: _write_arrow_to_iceberg failed (%s) — falling back to flush.", e)
            raise

    def _stage_arrow_to_pending(self, arrow_tbl, dest: dict, table_name: str,
                                partition_id: str, chunk_seq: int,
                                pk_range, stream_id: str, source_table: str,
                                connection_id: str) -> str:
        """v1.2.39 section 6: single-committer staging path. Write the
        Arrow batch as a plain Parquet file directly to
        ``table.location()/data/<partition>/<chunk_seq>-<uuid>.parquet``
        (NO catalog call) and RPUSH the file path onto the pending-files
        Redis list. Returns the file path written (empty string if the
        table had to be bootstrapped via write_arrow, in which case the
        chunk is already durable and the caller should treat it as such).

        The caller is responsible for advancing the checkpoint to
        ``state="staged"`` (NOT ``durable``) after this returns — the
        committer (``iceberg_committer.py``) promotes to ``durable`` once
        its ``add_files()`` commit confirms. ``last_pk`` advances to the
        staged cursor (``next_pk``) so the fetch loop continues; the
        durable promotion does not change ``last_pk``, only the state
        label."""
        from iceberg_writer import IcebergWriter
        from iceberg_committer import enqueue_pending_file
        # v1.3.2 Bug B fix: same unwrap as _write_arrow_to_iceberg /
        # _write_to_iceberg. Without this, 100% of staged-committer iceberg
        # writes crash with KeyError: 'catalog_uri' once load_catalog() runs.
        _dest_cfg = dest.get("connection_config") or dest.get("config") or dest
        writer = IcebergWriter(_dest_cfg, redis_client=getattr(self, "redis", None),
                               connection_id=connection_id)
        path = writer.write_arrow_to_file(
            arrow_tbl, table_name=table_name,
            partition_id=partition_id, chunk_seq=chunk_seq,
            pk_range=pk_range,
            pk_col=getattr(self, "_current_pk_col", None),
        )
        if not path:
            # Table was bootstrapped via write_arrow (one commit) - the
            # chunk is already durable; no pending entry needed.
            return ""
        entry = {
            "table_name": table_name,
            "file_path": path,
            "row_count": int(arrow_tbl.num_rows),
            "pk_range": list(pk_range) if pk_range else [],
            "chunk_seq": chunk_seq,
            "partition_id": partition_id,
            "stream_id": stream_id,
            "source_table": source_table,
            # v1.3.1 Fix 2: thread the current chunk's PK column into the
            # entry so the committer's _dedup_overlapping_entries can run
            # dedup-on-PK for partial overlaps. v1.3.0 staged entries
            # without pk_col, so the committer always saw pk_col=None and
            # skipped dedup (the partial-overlap case was silently broken;
            # only the fully-contained duplicate case worked).
            "pk_col": getattr(self, "_current_pk_col", None),
        }
        enqueue_pending_file(getattr(self, "redis", None), connection_id,
                              table_name, entry)
        return path

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
                           state: str = "done", total_chunks: int = 1,
                           pk_start=None, pk_end=None, rows_estimated=None):
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

        v1.2.29 Task 3: ``pk_start`` / ``pk_end`` / ``rows_estimated`` are
        reported once (on the first chunk of a partition) so the control-plane
        can compute per-partition progress % and ETA without re-querying the
        source DB.
        """
        import requests
        try:
            body = {
                "connection_id": connection_id,
                "stream_id": stream_id,
                "source_table": source_table,
                "chunk_seq": chunk_seq,
                "rows_written": rows_written,
                "last_pk": last_pk,
                "state": state,
                "current_chunk": chunk_seq,
                "total_chunks": total_chunks,
            }
            if pk_start is not None:
                body["pk_start"] = pk_start
            if pk_end is not None:
                body["pk_end"] = pk_end
            if rows_estimated is not None:
                body["rows_estimated"] = rows_estimated
            requests.post(
                f"{self.engine.control_plane_url}/api/v1/internal/load-checkpoints",
                json=body,
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
