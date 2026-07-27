"""v1.2.39 section 6 - single-committer + add_files() redesign.

Instead of N concurrent workers each calling table.append() (which races on
the Iceberg metadata-pointer CAS and forces the v1.2.33-36 Redis mutex +
dedup-on-PK workaround), workers write Parquet files directly to the
table's data location (NO catalog call) and RPUSH the file path onto a
Redis list. ONE committer process drains the list and registers all
drained files in a SINGLE table.transaction() / tx.add_files() call - one
commit covering potentially dozens of chunks from all K partitions.

This is the standard "many writers, one table" pattern in the Iceberg
ecosystem (Apache Flink's IcebergFilesCommitter, Adobe's Consolidation
Worker). PyIceberg 0.7.1's add_files() was verified live against our own
Nessie/MinIO stack (master report section 6b): 3 workers' files -> 1
snapshot, all rows present.

At-most-once registration: PyIceberg 0.7.1 has NO check_duplicate_files
(added in 0.8.0). The committer guarantees at-most-once by:
  - deleting a file's queue entry ONLY after its add_files() call is
    confirmed committed (BRPOP removes from the list; entries are only
    re-enqueued on a pre-commit failure);
  - tracking every committed path in a Redis set
    (fusion:iceberg-committed-files:<conn>:<table>) so a committer
    restart never re-registers a path already in a manifest.

Crash recovery: a file written to the object store but never committed is
inert (add_files() hasn't run, so it's not in any manifest). The orphan
sweep reconciles these on committer startup.
"""
from __future__ import annotations

import json
import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

log = logging.getLogger(__name__)

# Bug found via live throughput testing (this session's investigation): the
# naive `for path in paths: tx.add_files(file_paths=[path])` loop below
# calls PyIceberg's `parquet_files_to_data_files()` once per file — a
# strictly SEQUENTIAL blocking-I/O loop (each iteration does its own
# `io.new_input(path).open()` + `pq.read_metadata()` round-trip to
# MinIO/S3). Per-file cost grows with manifest size (confirmed live:
# ~165ms/file early on, >800ms/file by the time the table reaches a few
# hundred committed files), so `add_files_ms` for a fixed-size batch climbs
# without bound as the table grows — eventually the committer falls behind
# workers no matter how fast they stage files. Building every file's
# DataFile object CONCURRENTLY (footer reads release the GIL — this is
# I/O-bound, not CPU-bound, so a thread pool gives a real speedup) and then
# doing ONE sequential `fast_append()` with the pre-built objects fixes
# this: verified in isolation this session at ~18ms/file (vs. the
# unbounded per-file growth above), and the full clean-restart throughput
# runs before this fix confirmed 60k-97k rows/sec sustained with zero
# backlog growth.
_ADD_FILES_MAX_WORKERS = int(os.environ.get("ICEBERG_COMMITTER_ADD_FILES_WORKERS", "16"))


def _add_files_fast(table, tx, file_paths: list, max_workers: int = _ADD_FILES_MAX_WORKERS) -> None:
    """Register ``file_paths`` into ``tx`` via ONE fast_append(), building
    each file's DataFile object concurrently first. Drop-in replacement for
    ``for p in file_paths: tx.add_files(file_paths=[p])``.

    Falls back to sequential ``tx.add_files`` only when the PyIceberg fast
    path isn't importable (unit-test stubs that mock ``pyiceberg`` as a
    non-package). Real import/runtime failures from a live pyiceberg still
    propagate.
    """
    if not file_paths:
        return
    try:
        from pyiceberg.io.pyarrow import parquet_files_to_data_files
        from pyiceberg.table import TableProperties
    except ImportError:
        for path in file_paths:
            tx.add_files(file_paths=[path])
        return

    if tx.table_metadata.name_mapping() is None:
        tx.set_properties(**{
            TableProperties.DEFAULT_NAME_MAPPING: tx.table_metadata.schema().name_mapping.model_dump_json()
        })

    def _build_one(path):
        return next(iter(parquet_files_to_data_files(
            io=table.io, table_metadata=tx.table_metadata, file_paths=iter([path]))))

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        data_files = list(ex.map(_build_one, file_paths))
    with tx.update_snapshot().fast_append() as update_snapshot:
        for data_file in data_files:
            update_snapshot.append_data_file(data_file)

# Redis key templates (kept as class attrs so tests can introspect).
_PENDING_KEY = "fusion:iceberg-pending-files:{conn}:{table}"
_COMMITTED_KEY = "fusion:iceberg-committed-files:{conn}:{table}"
# Phase 3b (control-plane committer resizing): the "cheap, frequent,
# no-restart" lever control-plane's committer_resizer reconcile loop
# drives is THIS process's own add_files() concurrency (see
# _add_files_fast's ThreadPoolExecutor above) — the only real per-cycle
# concurrency knob this committer has, since run_cycle()/run_loop() below
# drain each of a connection's tables strictly SEQUENTIALLY (a plain
# `for t in self.table_names` loop — there is no cross-table threading to
# tune). Control-plane writes a per-connection override to this key on its
# own cadence (see control-plane/app/services/committer_resizer.py);
# _refresh_add_files_concurrency() polls it once per drain cycle so a
# running committer picks up a new value WITHOUT a pod restart, unlike the
# CPU/memory resource lever (which necessarily restarts the pod).
_CONCURRENCY_KEY = "fusion:iceberg-committer-concurrency:{conn}"
# Defensive clamps applied on READ, independent of whatever bounds
# control-plane itself enforces before writing — this process never
# trusts an externally-written Redis value blindly.
_CONCURRENCY_MIN = 2
_CONCURRENCY_MAX = 32
# v1.3.0 Fix 3: sorted set of committed PK ranges, scored by min_pk, each
# member a JSON {min, max, file_path}. Used by the committer to detect
# overlap between an incoming file's pk_range and any already-committed
# range BEFORE add_files() runs — the checkpoint-race scenario (chunk
# staged + last_pk advanced in worker memory, checkpoint report fails, pod
# restarts from the stale checkpoint and re-stages the same PK range under
# a new UUID path) would otherwise produce genuine row-level duplicates
# because the path-based committed set wouldn't match the new path.
_COMMITTED_PK_RANGES_KEY = "fusion:iceberg-committed-pk-ranges:{conn}:{table}"
# v1.3.4 Fix 2: this is the ONE shared lock namespace used by BOTH the
# bootstrap path (iceberg_writer._commit_lock_key) and the committer
# (_acquire_lock below). Previously the writer used a different namespace
# (``fusion:iceberg-commit-lock:``) and the two commit paths provided zero
# mutual exclusion against each other — root cause of the
# ``FileNotFoundError: ...snap-...avro`` in commit() and the 110.6%
# duplicate overage. Do NOT rename without updating iceberg_writer.py.
_LOCK_KEY = "fusion:iceberg-committer-lock:{conn}:{table}"

# Defaults (overridable via env vars for operators).
_DEFAULT_LOCK_TTL_S = int(os.environ.get("ICEBERG_COMMITTER_LOCK_TTL_S", "30"))
_DEFAULT_DRAIN_BATCH = int(os.environ.get("ICEBERG_COMMITTER_DRAIN_BATCH", "100"))
_DEFAULT_DRAIN_TIMEOUT_MS = int(os.environ.get("ICEBERG_COMMITTER_DRAIN_TIMEOUT_MS", "5000"))
_DEFAULT_IDLE_SLEEP_S = float(os.environ.get("ICEBERG_COMMITTER_IDLE_SLEEP_S", "5.0"))


def pending_key(connection_id: str, table_name: str) -> str:
    return _PENDING_KEY.format(conn=connection_id, table=table_name)


def committed_key(connection_id: str, table_name: str) -> str:
    return _COMMITTED_KEY.format(conn=connection_id, table=table_name)


def lock_key(connection_id: str, table_name: str) -> str:
    return _LOCK_KEY.format(conn=connection_id, table=table_name)


def committed_pk_ranges_key(connection_id: str, table_name: str) -> str:
    return _COMMITTED_PK_RANGES_KEY.format(conn=connection_id, table=table_name)


def concurrency_key(connection_id: str) -> str:
    """Redis key control-plane's committer_resizer writes the desired
    add_files() concurrency to for this CONNECTION's committer process
    (per-connection, not per-table — the whole process shares one
    ThreadPoolExecutor sizing, same as the pre-Phase-3b env var did)."""
    return _CONCURRENCY_KEY.format(conn=connection_id)


def _pk_to_score(pk) -> float:
    """Coerce a PK value into a numeric score suitable for ZADD/ZRANGEBYSCORE.
    Ints/floats pass through; NUMERIC strings are converted to float (see
    ``_coerce_pk`` for why numeric PKs can arrive as strings — the same fix
    applies here); only genuinely non-numeric strings (real non-numeric
    PKs, e.g. UUIDs) fall back to a stable hash in [0, 1), which
    approximates ordering rather than preserving it exactly.

    Bug found via live testing: before this fix, ANY string PK — including
    a numeric PK that merely arrived as a string — was hashed, silently
    destroying its true numeric ordering. Unlike the ``_ranges_overlap``/
    ``_is_contained`` TypeError (which at least crashes loudly), this one
    is silent: two genuinely overlapping numeric ranges could score to
    unrelated random floats and never be detected as overlapping,
    defeating the checkpoint-race duplicate-prevention this whole
    committed-PK-ranges mechanism exists for.
    """
    if pk is None:
        return float("inf")
    if isinstance(pk, bool):
        return float(pk)
    if isinstance(pk, (int, float)):
        return float(pk)
    try:
        return float(pk)
    except (TypeError, ValueError):
        pass
    import hashlib
    h = hashlib.sha256(str(pk).encode("utf-8")).hexdigest()
    return (int(h[:16], 16) % (2 ** 32)) / float(2 ** 32)


def _coerce_pk(v):
    """Normalize a PK bound to a consistently-comparable type before any
    ``<``/``>`` comparison.

    Bug found via live testing: pk_range bounds arrive as a MIX of native
    int (bounds computed from an actual fetched batch's real min/max, e.g.
    via pandas/pyarrow) and str (bounds carried through from the task's
    coarse ``pk_start``/``pk_end`` fields, which come from
    ``partition_with_estimates()`` and are typed generically as strings so
    the same partitioning code also supports non-numeric PKs like UUIDs).
    Comparing a str against an int raises ``TypeError`` — this only
    surfaces once a table has entries from more than one origin (e.g. once
    ``resource_limits.bulk_mode=auto`` lets different partitions of the
    same table take different code paths), which is why it went unnoticed
    until then. Numeric-looking strings are cast to int for comparison;
    anything else (a real non-numeric PK) is left as-is so lexical
    comparison still applies consistently.
    """
    if v is None or isinstance(v, (int, float)):
        return v
    try:
        return int(v)
    except (TypeError, ValueError):
        return v


def _ranges_overlap(a_min, a_max, b_min, b_max) -> bool:
    """Return True if [a_min, a_max] overlaps [b_min, b_max]. None bounds
    are treated as unbounded on that side.

    2026-07-24 Bug #5: the previous second check restated the first
    (``b_max < a_min`` == ``a_min > b_max``) and never tested "A entirely
    before B" (``a_max`` vs ``b_min``), so disjoint lower partitions were
    wrongly flagged as overlapping.

    2026-07-24 Bug #6: the stored pk_range is a CURSOR-based half-open
    interval (last_pk_before, last_pk_after] -- last_pk_before is the
    EXCLUSIVE fetch cursor (``WHERE pk > last_pk``), last_pk_after is the
    actual last row's pk (INCLUSIVE). Consecutive chunks therefore always
    touch exactly at one boundary value (chunk N's rmax == chunk N+1's
    rmin) by construction -- that shared value is EXCLUDED from chunk
    N+1's own range, so it is not a real overlap. Strict ``>``/``<``
    treated an exact boundary touch as overlapping; ``>=``/``<=`` correctly
    treats a shared boundary as disjoint.
    """
    a_min, a_max, b_min, b_max = (_coerce_pk(a_min), _coerce_pk(a_max),
                                   _coerce_pk(b_min), _coerce_pk(b_max))
    if a_min is not None and b_max is not None and a_min >= b_max:
        return False
    if a_max is not None and b_min is not None and a_max <= b_min:
        return False
    return True


def _is_contained(c_min, c_max, outer_min, outer_max) -> bool:
    """Return True if [c_min, c_max] is fully contained in [outer_min, outer_max].
    None bounds are treated as unbounded on that side (so an unbounded candidate
    is never contained by a bounded outer range, and a bounded candidate is
    contained by an unbounded outer range)."""
    c_min, c_max, outer_min, outer_max = (_coerce_pk(c_min), _coerce_pk(c_max),
                                           _coerce_pk(outer_min), _coerce_pk(outer_max))
    if outer_min is not None and (c_min is None or c_min < outer_min):
        return False
    if outer_max is not None and (c_max is None or c_max > outer_max):
        return False
    return True


def _record_committed_pk_range(redis_client, connection_id: str,
                                table_name: str, pk_range, file_path: str) -> None:
    """Add a successfully-committed file's PK range to the sorted set."""
    if redis_client is None or pk_range is None:
        return
    try:
        rmin, rmax = pk_range[0], pk_range[1]
    except Exception:
        return
    member = json.dumps({"min": rmin, "max": rmax, "file_path": file_path},
                        default=str)
    score = _pk_to_score(rmin)
    key = committed_pk_ranges_key(connection_id, table_name)
    try:
        redis_client.zadd(key, {member: score})
    except Exception:
        log.warning("committer: ZADD committed pk-ranges failed "
                    "(PK overlap dedup degraded for table=%s)", table_name)


def _find_overlapping_committed_ranges(redis_client, connection_id: str,
                                        table_name: str, pk_range) -> list[dict]:
    """Return all committed PK-range entries overlapping ``pk_range``.

    The sorted set is scored by min_pk, but a committed range may START
    before the incoming range's min and still extend into it (e.g.
    committed [0,1000] vs incoming [200,300]). So we query all committed
    ranges with min_pk <= rmax (zrangebyscore -inf..rmax) and then filter
    by actual range overlap. The committed set is bounded (one entry per
    committed file per table) so this is cheap."""
    if redis_client is None or pk_range is None:
        return []
    try:
        rmin, rmax = pk_range[0], pk_range[1]
    except Exception:
        return []
    key = committed_pk_ranges_key(connection_id, table_name)
    hi = _pk_to_score(rmax)
    out: list[dict] = []
    try:
        # All committed ranges whose min_pk <= rmax.
        members = redis_client.zrangebyscore(key, "-inf", hi)
    except Exception:
        return out
    for m in members:
        try:
            entry = json.loads(m)
        except Exception:
            continue
        if _ranges_overlap(rmin, rmax, entry.get("min"), entry.get("max")):
            out.append(entry)
    return out


def enqueue_pending_file(redis_client, connection_id: str, table_name: str,
                         entry: dict) -> int:
    """Worker-side helper: RPUSH a pending-file entry onto the list.

    ``entry`` is a dict with at least ``file_path``; recommended fields:
    ``table_name, file_path, row_count, pk_range, chunk_seq, partition_id,
    stream_id, source_table``. The entry is JSON-encoded.
    """
    if redis_client is None:
        return 0
    key = pending_key(connection_id, table_name)
    payload = json.dumps(entry, default=str)
    return redis_client.rpush(key, payload)


def list_pending(redis_client, connection_id: str, table_name: str,
                 count: int = 1, timeout_ms: int = 0) -> list[dict]:
    """Drain up to ``count`` entries from the pending list. If
    ``timeout_ms`` > 0, blocks up to that long for the first entry
    (BRPOP); further entries up to ``count`` are drained non-blocking.
    Returns a list of decoded entry dicts (possibly empty)."""
    if redis_client is None:
        return []
    key = pending_key(connection_id, table_name)
    out: list[dict] = []
    if timeout_ms > 0:
        # BRPOP returns (key, value) or None on timeout.
        item = redis_client.brpop(key, timeout=timeout_ms // 1000)
        if item is not None:
            _k, v = item
            out.append(json.loads(v))
    # Drain more non-blocking up to count.
    while len(out) < count:
        item = redis_client.lpop(key)
        if item is None:
            break
        out.append(json.loads(item))
    return out


class IcebergCommitter:
    """Committer for a CONNECTION, draining one or more of its tables'
    staged Parquet files into Iceberg from a single shared process.

    v1.4.x Phase 1 (committer consolidation): previously one committer
    PROCESS existed per (connection_id, table_name) pair. That changed
    so ONE process now drains every table belonging to a connection —
    but only the PROCESS boundary moved. Every Redis key (pending list,
    lock, committed set, committed-pk-ranges) is still scoped per
    (connection_id, table_name) exactly as before, and each per-table
    method below takes an explicit ``table_name`` argument (defaulting to
    the first/only entry in ``table_names`` so single-table callers and
    the pre-consolidation test suite keep working unchanged).
    ``catalog.load_table()`` was already called per-table inside one
    process (to support the orphan sweep / dedup-on-PK paths), so one
    shared ``catalog`` instance draining multiple tables' pending lists
    is additive, not a redesign of that part.

    The committer is intentionally stateless across calls - all shared
    state lives in Redis (pending list, committed set, lock) so any pod
    that holds the lock can take over. ``mark_durable`` is a callback the
    caller wires to the control-plane checkpoint-report API; it receives
    each committed entry so the control-plane can promote the chunk's
    checkpoint from ``staged`` to ``durable`` (THIS is when ``last_pk``
    truly advances).
    """

    def __init__(self, catalog, redis_client, connection_id: str,
                 table_names, namespace: str = "fusion",
                 table_namespaces: dict | None = None,
                 lock_ttl_s: int = _DEFAULT_LOCK_TTL_S,
                 drain_batch: int = _DEFAULT_DRAIN_BATCH,
                 drain_timeout_ms: int = _DEFAULT_DRAIN_TIMEOUT_MS,
                 idle_sleep_s: float = _DEFAULT_IDLE_SLEEP_S,
                 mark_durable=None,
                 add_files_max_workers: int | None = None):
        self.catalog = catalog
        self.redis = redis_client
        self.connection_id = connection_id
        # Phase 3b: runtime-adjustable add_files() concurrency (see
        # _CONCURRENCY_KEY above). Seeded from the constructor arg (falls
        # back to the module-level env-var default, preserving the exact
        # pre-Phase-3b behavior for any caller that doesn't pass it) and
        # re-read from Redis once per drain cycle by
        # _refresh_add_files_concurrency() — no pod restart required to
        # change it, unlike CPU/memory (see committer_provisioner.py).
        self.add_files_max_workers = int(add_files_max_workers or _ADD_FILES_MAX_WORKERS)
        # Accept either a single table name (str — the pre-consolidation
        # shape, kept for backward compatibility with existing callers/
        # tests) or a list of table names (the new per-connection shape).
        # Always stored internally as a list.
        if isinstance(table_names, str):
            table_names = [table_names]
        self.table_names = list(table_names)
        self.namespace = namespace
        # A connection's streams can each override their destination
        # namespace (``stream.stream_namespace`` in the control-plane), so
        # even though one committer process now drains every table of a
        # connection, individual tables may still resolve to different
        # Iceberg namespaces. ``table_namespaces`` is an optional
        # ``{table_name: namespace}`` override map; any table absent from
        # it falls back to the shared ``namespace`` default.
        self.table_namespaces = dict(table_namespaces or {})
        self.lock_ttl_s = lock_ttl_s
        self.drain_batch = drain_batch
        self.drain_timeout_ms = drain_timeout_ms
        self.idle_sleep_s = idle_sleep_s
        self.mark_durable = mark_durable  # callable(entry) -> None
        self._lock_token = str(uuid.uuid4())

    def _namespace_for(self, table_name: str) -> str:
        """Resolve the Iceberg namespace for ``table_name``: an explicit
        per-table override from ``table_namespaces`` if set, else the
        committer's shared default ``namespace``."""
        return self.table_namespaces.get(table_name, self.namespace)

    @property
    def table_name(self):
        """Backward-compat accessor: the first (or only) configured table.
        Kept so pre-consolidation single-table callers/tests that read
        ``committer.table_name`` keep working."""
        return self.table_names[0] if self.table_names else None

    def _default_table(self, table_name) :
        table_name = table_name or self.table_name
        if table_name is None:
            raise ValueError("IcebergCommitter has no tables configured")
        return table_name

    # ── Runtime-adjustable concurrency (Phase 3b hot-reload) ─────────────
    def _refresh_add_files_concurrency(self) -> None:
        """Poll ``concurrency_key`` for a control-plane-written override of
        ``add_files_max_workers``. Best-effort: any Redis error or missing/
        invalid value leaves the current setting untouched (never raises,
        never blocks a drain cycle on this). Called once per
        ``run_cycle()`` — cheap (one GET) relative to the drain/commit work
        the rest of the cycle does, so polling every cycle (rather than on
        a coarser timer) is fine and keeps this genuinely "frequent"."""
        if self.redis is None:
            return
        try:
            raw = self.redis.get(concurrency_key(self.connection_id))
        except Exception:
            return
        if raw is None:
            return
        try:
            value = int(raw)
        except (TypeError, ValueError):
            log.warning("committer: ignoring non-integer concurrency override %r for connection=%s",
                        raw, self.connection_id)
            return
        clamped = max(_CONCURRENCY_MIN, min(_CONCURRENCY_MAX, value))
        if clamped != self.add_files_max_workers:
            log.info("committer: add_files concurrency %d -> %d (connection=%s, hot-reload, no restart)",
                      self.add_files_max_workers, clamped, self.connection_id)
            self.add_files_max_workers = clamped

    # ── Redis lock (short batching window, NOT per-chunk) ────────────────
    def _acquire_lock(self, table_name: str | None = None) -> bool:
        if self.redis is None:
            return True
        table_name = self._default_table(table_name)
        key = lock_key(self.connection_id, table_name)
        return bool(self.redis.set(key, self._lock_token, nx=True,
                                    ex=self.lock_ttl_s))

    def _release_lock(self, table_name: str | None = None) -> None:
        if self.redis is None:
            return
        table_name = self._default_table(table_name)
        key = lock_key(self.connection_id, table_name)
        # Only delete if we still hold the token (Lua compare-and-delete).
        script = (
            "if redis.call('get', KEYS[1]) == ARGV[1] then "
            "return redis.call('del', KEYS[1]) else return 0 end"
        )
        try:
            self.redis.eval(script, 1, key, self._lock_token)
        except Exception:
            # Fallback: plain del (safe in the common case where the
            # lock TTL already expired and no one else grabbed it).
            try:
                self.redis.delete(key)
            except Exception:
                pass

    # ── Core: drain + commit (one table) ─────────────────────────────────
    def drain_and_commit(self, table_name: str | None = None) -> dict:
        """Drain up to ``drain_batch`` pending entries for ``table_name``
        (defaults to the first/only configured table, for backward
        compatibility with single-table callers) and register them in
        ONE Iceberg transaction. Returns a summary dict:
        ``{drained, committed, skipped_duplicate, committed_paths,
        errors}``."""
        table_name = self._default_table(table_name)
        result = {"drained": 0, "committed": 0, "skipped_duplicate": 0,
                  "committed_paths": [], "errors": []}
        if not self._acquire_lock(table_name):
            log.debug("committer lock held by another pod - skipping this "
                      "cycle (table=%s)", table_name)
            return result
        try:
            entries = list_pending(self.redis, self.connection_id,
                                   table_name,
                                   count=self.drain_batch,
                                   timeout_ms=self.drain_timeout_ms)
            if not entries:
                return result
            result["drained"] = len(entries)
            committed = self._commit_entries(table_name, entries, result)
            # On success, mark each committed entry durable.
            if self.mark_durable is not None:
                for e in committed:
                    try:
                        self.mark_durable(e)
                    except Exception:
                        log.exception("mark_durable failed for entry %s", e)
        finally:
            self._release_lock(table_name)
        return result

    def _commit_entries(self, table_name: str, entries: list[dict], result: dict) -> list[dict]:
        """Filter duplicates, open ONE transaction, add_files() each path,
        commit once, SADD committed paths. Returns the list of entries
        that were actually committed (for durable-marking).

        v1.3.0 Fix 3: BEFORE add_files(), each entry's pk_range is checked
        against the committed-PK-ranges sorted set. If an overlap is found
        (checkpoint-race: a stale checkpoint re-staged the same PK range
        under a new UUID path), the committer either (a) runs dedup-on-PK
        against the overlapping rows before registering, or (b) skips the
        new file entirely if its range is fully contained in a committed
        range. The new range is recorded in the sorted set only after a
        successful commit."""
        if not entries:
            return []
        # At-most-once: skip any path already in the committed set (e.g.
        # a committer restart that re-discovered a file it already
        # registered in a prior cycle).
        committed_set = committed_key(self.connection_id, table_name)
        to_commit = []
        for e in entries:
            path = e.get("file_path")
            if not path:
                result["errors"].append({"entry": e, "reason": "missing file_path"})
                continue
            if self.redis is not None:
                try:
                    if self.redis.sismember(committed_set, path):
                        result["skipped_duplicate"] += 1
                        log.info("committer: skipping already-committed path %s", path)
                        continue
                except Exception:
                    pass
            to_commit.append(e)
        if not to_commit:
            return []
        # v1.3.0 Fix 3 + v1.3.4 Fix 3: partition entries into
        # (a) those that need dedup-on-PK before registration (overlap but
        # not fully contained — against committed ranges OR same-batch
        # entries), (b) those that are pure duplicates (fully contained in
        # a committed range OR a same-batch entry -> skip + delete), and
        # (c) those with no overlap (register normally).
        dedup_before_commit: list[dict] = []
        register_clean: list[dict] = []
        skipped_pk_dup: list[dict] = []
        # v1.3.4 Fix 3: same-batch PK-range overlap check. A retry can
        # re-stage a chunk (new UUID, same PK range) before the original's
        # file is committed; both land in the same drain cycle and the
        # committed-ranges check (which only sees already-committed
        # ranges) would let both register → row-level duplicates. Compare
        # each candidate against every other entry already accepted into
        # this batch (in-memory, ≤10k comparisons for drain_batch=100,
        # trivial cost vs the multi-second commit). On overlap:
        #   - candidate fully contained in an accepted entry → pure dup,
        #     skip + delete the candidate (the retry's file is inert);
        #   - accepted entry fully contained in candidate → evict the
        #     accepted entry (pure dup of the candidate), skip + delete it;
        #   - partial overlap → route the candidate to dedup-on-PK (same
        #     best-effort path as the committed-range partial-overlap case).
        accepted: list[dict] = []
        for e in to_commit:
            pk_range = e.get("pk_range")
            try:
                rmin, rmax = (pk_range or (None, None))[0], (pk_range or (None, None))[1]
            except Exception:
                rmin = rmax = None
            # Entries with no pk_range (None bounds) are not comparable —
            # treat them as non-overlapping with everything (the
            # committed-set path-dedup still applies).
            if rmin is None and rmax is None:
                accepted.append(e)
                continue
            candidate_contained = False
            evicted: list[dict] = []
            partial_overlap = False
            for a in accepted:
                arange = a.get("pk_range")
                try:
                    amin, amax = (arange or (None, None))[0], (arange or (None, None))[1]
                except Exception:
                    amin = amax = None
                if amin is None and amax is None:
                    continue
                if not _ranges_overlap(rmin, rmax, amin, amax):
                    continue
                if _is_contained(rmin, rmax, amin, amax):
                    candidate_contained = True
                    break
                if _is_contained(amin, amax, rmin, rmax):
                    evicted.append(a)
                else:
                    partial_overlap = True
            if candidate_contained:
                skipped_pk_dup.append(e)
                continue
            for a in evicted:
                if a in accepted:
                    accepted.remove(a)
                skipped_pk_dup.append(a)
            if partial_overlap:
                # Mark so the committed-ranges loop routes this entry to
                # dedup-on-PK (not register_clean) even when there's no
                # committed-range overlap. Keeps dedup_before_commit and
                # register_clean disjoint (avoids double-registration).
                e["_same_batch_partial_overlap"] = True
            accepted.append(e)
        to_commit = accepted
        # v1.3.0 Fix 3: PK-range overlap detection against committed ranges.
        for e in to_commit:
            pk_range = e.get("pk_range")
            overlaps = _find_overlapping_committed_ranges(
                self.redis, self.connection_id, table_name, pk_range)
            if not overlaps:
                # v1.3.4 Fix 3: a same-batch partial-overlap entry that has
                # no committed-range overlap still needs dedup-on-PK before
                # registration (the other same-batch entry it overlaps is
                # being registered in the same transaction). Route to
                # dedup_before_commit instead of register_clean so the
                # downstream dedup pass runs and the lists stay disjoint.
                if e.get("_same_batch_partial_overlap"):
                    dedup_before_commit.append(e)
                else:
                    register_clean.append(e)
                continue
            # Check if the incoming range is fully contained in any one
            # committed range -> pure duplicate, skip + delete.
            try:
                rmin, rmax = (pk_range or (None, None))[0], (pk_range or (None, None))[1]
            except Exception:
                rmin = rmax = None
            fully_contained = False
            for o in overlaps:
                omin, omax = o.get("min"), o.get("max")
                contained = True
                if omin is not None and (rmin is None or rmin < omin):
                    contained = False
                if omax is not None and (rmax is None or rmax > omax):
                    contained = False
                if contained:
                    fully_contained = True
                    break
            if fully_contained:
                skipped_pk_dup.append(e)
            else:
                dedup_before_commit.append(e)
        # (b) delete fully-contained pure duplicates from object store.
        for e in skipped_pk_dup:
            result["skipped_duplicate"] += 1
            log.info("committer: skipping PK-duplicate (fully contained) %s",
                     e.get("file_path"))
            self._delete_staged_file(table_name, e.get("file_path"), result)
        # (a) run dedup-on-PK for overlapping-but-not-contained entries.
        if dedup_before_commit:
            self._dedup_overlapping_entries(table_name, dedup_before_commit, result)
            register_clean.extend(dedup_before_commit)
        if not register_clean:
            return []
        # Open ONE transaction and add_files() each path.
        # v1.3.6: phase timing INFO logs (load_table / add_files / commit).
        try:
            _t0 = time.perf_counter()
            table = self.catalog.load_table(
                f"{self._namespace_for(table_name)}.{table_name}")
            load_table_ms = (time.perf_counter() - _t0) * 1000.0
        except Exception as e:
            result["errors"].append({"phase": "load_table", "error": str(e)})
            # Re-enqueue the entries so the next cycle retries.
            self._reenqueue(table_name, register_clean)
            return []
        try:
            _t1 = time.perf_counter()
            with table.transaction() as tx:
                # Phase 3b: max_workers is now this instance's runtime-
                # adjustable self.add_files_max_workers (see
                # _refresh_add_files_concurrency) rather than always the
                # module-level _ADD_FILES_MAX_WORKERS default.
                _add_files_fast(table, tx, [e["file_path"] for e in register_clean],
                                 max_workers=self.add_files_max_workers)
                add_files_ms = (time.perf_counter() - _t1) * 1000.0
                _t2 = time.perf_counter()
            # commit_transaction runs on context-manager exit
            commit_ms = (time.perf_counter() - _t2) * 1000.0
            log.info(
                "committer: commit phase timing table=%s files_in_batch=%d "
                "load_table_ms=%.1f add_files_ms=%.1f commit_ms=%.1f",
                table_name, len(register_clean),
                load_table_ms, add_files_ms, commit_ms,
            )
            # Commit succeeded - record each path in the committed set
            # and each range in the committed-PK-ranges sorted set.
            committed_paths = [e["file_path"] for e in register_clean]
            if self.redis is not None:
                try:
                    self.redis.sadd(committed_set, *committed_paths)
                except Exception:
                    log.warning("committer: SADD committed set failed "
                                "(dedup across restarts degraded)")
                for e in register_clean:
                    _record_committed_pk_range(
                        self.redis, self.connection_id, table_name,
                        e.get("pk_range"), e["file_path"])
            result["committed"] = len(register_clean)
            result["committed_paths"].extend(committed_paths)
            return register_clean
        except Exception as e:
            log.exception("committer: add_files transaction failed (%s) - "
                          "re-enqueueing %d entries for retry", e, len(register_clean))
            result["errors"].append({"phase": "add_files", "error": str(e)})
            self._reenqueue(table_name, register_clean)
            return []

    def _delete_staged_file(self, table_name: str, file_path: str, result: dict) -> None:
        """Best-effort delete of a staged-but-skipped Parquet file from the
        object store (pure PK duplicate). Failures are logged but not
        fatal — an orphan sweep will reconcile later."""
        if not file_path:
            return
        try:
            table = self.catalog.load_table(
                f"{self._namespace_for(table_name)}.{table_name}")
            try:
                table.io.delete(file_path)
            except Exception as de:
                log.debug("committer: delete staged file %s failed (%s) "
                          "(orphan sweep will reconcile)", file_path, de)
        except Exception as le:
            log.debug("committer: load_table for delete failed (%s)", le)

    def _dedup_overlapping_entries(self, table_name: str, entries: list[dict],
                                    result: dict) -> None:
        """Run dedup-on-PK against the table for the overlapping PK ranges
        so the incoming file's rows replace the already-committed rows in
        the overlap window (delete-then-register). Best-effort: failures
        are logged; the entry still registers (the overlap is rare and the
        next compaction will reconcile).

        v1.3.0 Fix 3: reuses the iceberg_writer._dedup_on_pk helper for
        the discrete-key path (when the staged file's PK column can be
        read) and falls back to a pyiceberg range delete (In -> delete by
        range) when the file can't be read. Both paths are best-effort
        and guarded so a failure never blocks registration."""
        pk_col = None
        for e in entries:
            pk_col = e.get("pk_col")
            if pk_col:
                break
        if not pk_col:
            log.warning("committer: PK overlap detected but no pk_col on "
                        "entry — skipping dedup-on-PK (entries will still "
                        "register; compaction will reconcile)")
            return
        try:
            table = self.catalog.load_table(
                f"{self._namespace_for(table_name)}.{table_name}")
        except Exception as e:
            log.warning("committer: dedup-on-PK load_table failed (%s) — "
                        "entries will register without dedup", e)
            return
        for e in entries:
            pk_range = e.get("pk_range")
            if not pk_range:
                continue
            try:
                rmin, rmax = pk_range[0], pk_range[1]
            except Exception:
                continue
            self._dedup_one_range(table_name, table, pk_col, rmin, rmax, e)

    def _dedup_one_range(self, table_name: str, table, pk_col: str, rmin, rmax,
                         entry: dict) -> None:
        """Delete rows in [rmin, rmax] from the table (delete-then-register).
        Tries the discrete-key path via iceberg_writer._dedup_on_pk (reading
        the staged Parquet file's PK column) and falls back to a pyiceberg
        range expression. All failures are logged and non-fatal.

        2026-07-24 Bug #7: BOTH delete paths are catastrophically expensive
        on an unpartitioned table (no file pruning for an IN() over up to
        ``chunk_size`` keys or a range predicate). Skip the expensive delete
        when the table has no partition fields and register as-is. When the
        table IS partitioned, keep the pruned delete path.
        """
        try:
            spec = table.spec()
            part_fields = getattr(spec, "fields", None) or ()
            is_partitioned = len(part_fields) > 0
        except Exception:
            is_partitioned = False
        if not is_partitioned:
            log.warning(
                "committer: overlap detected for %s (pk range [%s,%s]) — "
                "skipping expensive delete-dedup (unpartitioned table, "
                "checkpoint fix + correct overlap-detection make this rare); "
                "registering as-is",
                entry.get("file_path"), rmin, rmax,
            )
            return
        # Path 1: read the staged file's PK column and use the existing
        # _dedup_on_pk discrete-key delete.
        keys = self._extract_pk_values_from_staged_file(table_name, entry, pk_col)
        if keys:
            try:
                from iceberg_writer import _dedup_on_pk
                _dedup_on_pk(table, pk_col, rows=keys)
                return
            except Exception as de:
                log.warning("committer: _dedup_on_pk for %s failed (%s) — "
                            "trying range delete", entry.get("file_path"), de)
        # Path 2: pyiceberg range delete fallback.
        try:
            from pyiceberg.expressions import (
                GreaterThanOrEqual, LessThanOrEqual, And,
            )
            table.delete(And(GreaterThanOrEqual(pk_col, rmin),
                              LessThanOrEqual(pk_col, rmax)))
        except Exception as de:
            log.warning("committer: range delete for %s failed (%s) — "
                        "entry will register; compaction will reconcile",
                        entry.get("file_path"), de)

    def _extract_pk_values_from_staged_file(self, table_name: str, entry: dict,
                                             pk_col: str) -> list:
        """Best-effort read of the PK column from the staged Parquet file.
        Returns [] on any failure (the range-delete fallback handles it)."""
        file_path = entry.get("file_path")
        if not file_path:
            return []
        try:
            import pyarrow.parquet as pq
            table = self.catalog.load_table(
                f"{self._namespace_for(table_name)}.{table_name}")
            with table.io.open_input_file(file_path) as f:
                pf = pq.ParquetFile(f)
                tbl = pf.read()
            return [v for v in tbl.column(pk_col).to_pylist()
                    if v is not None]
        except Exception:
            return []

    def _reenqueue(self, table_name: str, entries: list[dict]) -> None:
        """Put entries back on the pending list (head) so the next cycle
        retries them. Uses LPUSH so they're processed before newer entries."""
        if self.redis is None or not entries:
            return
        key = pending_key(self.connection_id, table_name)
        for e in entries:
            try:
                self.redis.lpush(key, json.dumps(e, default=str))
            except Exception:
                log.exception("committer: re-enqueue failed for entry %s", e)

    # ── Orphan-file sweep (crash recovery) ───────────────────────────────
    def orphan_sweep(self, table_name: str | None = None, register: bool = True) -> dict:
        """List Parquet files under ``table.location()/data/`` not yet in
        any manifest, cross-check against the pending list + committed
        set, and either register orphans via add_files() (if register) or
        delete them. Returns a summary dict.

        ``table_name`` defaults to the first/only configured table (for
        backward compatibility with single-table callers); use
        ``orphan_sweep_all`` to sweep every table of a multi-table
        connection.

        This is the crash-recovery reconciliation pass: a file written to
        the object store but never committed is inert (add_files() hasn't
        run, so it's not in any manifest). On committer startup (or
        periodically), this pass either registers orphans via add_files()
        or deletes them if they correspond to a chunk that was
        re-processed.
        """
        table_name = self._default_table(table_name)
        result = {"orphans": [], "registered": 0, "deleted": 0, "errors": []}
        try:
            table = self.catalog.load_table(
                f"{self._namespace_for(table_name)}.{table_name}")
        except Exception as e:
            result["errors"].append({"phase": "load_table", "error": str(e)})
            return result
        # Files in the object store under data/.
        try:
            data_files = self._list_data_files(table)
        except Exception as e:
            result["errors"].append({"phase": "list_data", "error": str(e)})
            return result
        # Files already in manifests.
        manifest_files = self._list_manifest_files(table)
        orphans = [p for p in data_files if p not in manifest_files]
        # Cross-check committed set: a path in the committed set but not in
        # a manifest is a real orphan (commit happened in our bookkeeping
        # but the catalog lost it - rare, but handle it).
        committed_set = committed_key(self.connection_id, table_name)
        to_register = []
        for path in orphans:
            already_committed = False
            if self.redis is not None:
                try:
                    already_committed = bool(
                        self.redis.sismember(committed_set, path))
                except Exception:
                    pass
            result["orphans"].append(path)
            if not register:
                try:
                    table.io.delete(path)
                    result["deleted"] += 1
                except Exception as e:
                    result["errors"].append(
                        {"phase": "delete", "path": path, "error": str(e)})
                continue
            # Register the orphan via add_files(), unless it's already in
            # the committed set (in which case the manifest list is stale -
            # skip; the next table reload will pick it up).
            if already_committed:
                continue
            to_register.append(path)
        # v1.3.7: batch ALL orphans into ONE transaction via _add_files_fast
        # (concurrent footer reads + single fast_append) instead of one
        # separate table.transaction()/commit per file — the same fix as
        # the main commit path (_commit_entries), applied here since a
        # sweep after an extended outage can find hundreds of orphans and
        # the old per-file-commit loop would be just as slow here.
        if to_register:
            try:
                with table.transaction() as tx:
                    _add_files_fast(table, tx, to_register, max_workers=self.add_files_max_workers)
                if self.redis is not None:
                    try:
                        self.redis.sadd(committed_set, *to_register)
                    except Exception:
                        pass
                result["registered"] = len(to_register)
            except Exception as e:
                result["errors"].append(
                    {"phase": "register_orphans", "paths": to_register,
                     "error": str(e)})
        return result

    def _list_data_files(self, table) -> list[str]:
        """List all .parquet files under table.location()/data/."""
        loc = table.location()
        out: list[str] = []
        try:
            prefix = loc.rstrip("/") + "/data/"
            for f in table.io.iterate(prefix):
                name = str(f)
                if name.endswith(".parquet"):
                    out.append(name)
        except Exception:
            # Fallback: try a flat list under the data prefix.
            try:
                prefix = loc.rstrip("/") + "/data"
                for f in table.io.iterate(prefix):
                    name = str(f)
                    if name.endswith(".parquet"):
                        out.append(name)
            except Exception:
                pass
        return out

    def _list_manifest_files(self, table) -> set[str]:
        """Return the set of data-file paths referenced by the table's
        current manifest list (avro manifest entries). Empty if the table
        has no snapshot yet."""
        out: set[str] = set()
        try:
            snap = table.current_snapshot()
            if snap is None:
                return out
            for manifest in snap.manifests(table.io):
                for entry in manifest.fetch_manifest_entries(table.io):
                    try:
                        out.add(entry.data_file.file_path)
                    except Exception:
                        pass
        except Exception:
            pass
        return out

    def orphan_sweep_all(self, register: bool = True) -> dict:
        """Run ``orphan_sweep`` for every table in ``table_names``,
        isolating each table's failure from the others (a sweep exception
        for one table must not skip the sweep for the connection's other
        tables). Returns ``{table_name: result}``."""
        out: dict[str, dict] = {}
        for t in self.table_names:
            try:
                out[t] = self.orphan_sweep(t, register=register)
            except Exception as e:
                log.exception("committer: orphan_sweep failed for table=%s "
                              "(connection=%s) — continuing with the "
                              "connection's other tables", t, self.connection_id)
                out[t] = {"orphans": [], "registered": 0, "deleted": 0,
                          "errors": [{"phase": "orphan_sweep", "error": str(e)}]}
        return out

    # ── Run loop (for a dedicated committer process/sidecar) ────────────
    def run_cycle(self) -> dict:
        """Run ONE drain-and-commit cycle across EVERY table in
        ``table_names``. One table's commit failure is isolated (logged,
        recorded as an error result) and never stops the other tables in
        the connection from draining during the same cycle. Returns
        ``{table_name: result}``."""
        results: dict[str, dict] = {}
        # Phase 3b: pick up any control-plane-driven concurrency change
        # once per cycle, before draining any table this cycle.
        self._refresh_add_files_concurrency()
        for t in self.table_names:
            try:
                results[t] = self.drain_and_commit(t)
            except Exception as e:
                log.exception("committer: drain_and_commit failed for "
                              "table=%s (connection=%s) — continuing with "
                              "the connection's other tables", t,
                              self.connection_id)
                results[t] = {"drained": 0, "committed": 0,
                              "skipped_duplicate": 0, "committed_paths": [],
                              "errors": [{"phase": "drain_and_commit",
                                          "error": str(e)}]}
        return results

    def run_loop(self, max_cycles: int | None = None) -> None:
        """Drain-and-commit loop across every table this committer's
        connection owns. Sleeps ``idle_sleep_s`` when a cycle drained
        nothing for ANY table. If ``max_cycles`` is set, stops after that
        many cycles (useful for tests)."""
        cycle = 0
        while True:
            results = self.run_cycle()
            cycle += 1
            any_drained = any(r.get("drained", 0) for r in results.values())
            if not any_drained:
                time.sleep(self.idle_sleep_s)
            if max_cycles is not None and cycle >= max_cycles:
                return


if __name__ == "__main__":  # pragma: no cover - manual sidecar entry
    import argparse
    import sys
    # Bug found via live testing: this process never called basicConfig(),
    # so log.info()/.debug() calls were silent no-ops in the real deployed
    # container (Python's root logger has no handler by default — only
    # WARNING+ reaches stderr via the "handler of last resort"). Made this
    # process's actual behavior unobservable during a live investigation.
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    # Lazy imports so unit tests don't require pyiceberg/redis.
    from iceberg_writer import load_catalog
    import redis as redis_lib

    ap = argparse.ArgumentParser(description="Fusion CDC Iceberg committer")
    ap.add_argument("--connection-id", required=True)
    # v1.4.x Phase 1 (committer consolidation): one committer process now
    # drains ALL of a connection's iceberg-destined tables, not just one.
    # --tables replaces the old single --table flag; it takes a
    # comma-separated list (e.g. "orders,customers,line_items"). Each
    # table's pending-list/lock/pk-range Redis keys stay per-table
    # (unchanged) — only the process boundary moved to per-connection.
    ap.add_argument("--tables", required=True,
                    help="Comma-separated list of destination table names "
                         "this committer drains for the connection, e.g. "
                         "'orders,customers,line_items'.")
    ap.add_argument("--namespace", default="fusion",
                    help="Default Iceberg namespace for tables that don't "
                         "have a per-table override in --table-namespaces.")
    # A connection's streams can each override their destination namespace
    # (stream.stream_namespace), so even one committer process per
    # connection may need different namespaces for different tables.
    # Optional JSON map {table_name: namespace}; tables absent from it use
    # --namespace. Empty/unset means every table uses --namespace, matching
    # the pre-consolidation single-namespace-per-table behavior.
    ap.add_argument("--table-namespaces",
                    default=os.environ.get("ICEBERG_TABLE_NAMESPACES", "{}"),
                    help="JSON object mapping table name -> Iceberg "
                         "namespace override, for connections whose streams "
                         "target different namespaces per table.")
    ap.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    # Per-connection override of the Redis drain batch size (how many
    # pending file-commit entries are popped and committed together per
    # cycle). Falls back to _DEFAULT_DRAIN_BATCH (the ICEBERG_COMMITTER_
    # DRAIN_BATCH env var) when not passed — the chart wires this from
    # the connection's resource_limits.drain_batch when set, so a
    # per-connection value in the UI actually reaches the committer
    # instead of every connection sharing one cluster-wide setting.
    ap.add_argument("--drain-batch", type=int, default=_DEFAULT_DRAIN_BATCH)
    # v1.3.5 Fix 4: --catalog-config is REQUIRED. Previously it defaulted
    # to None → load_catalog({}) silently fell back to "rest" and then
    # KeyError'd on "catalog_uri" — a confusing crash-loop. Now fail
    # loudly up front with an actionable message. The chart template
    # wires this from the destination's connection_config Secret.
    ap.add_argument("--catalog-config", default=os.environ.get("ICEBERG_CATALOG_CONFIG"),
                    help="JSON dest config for load_catalog (required; "
                         "or set ICEBERG_CATALOG_CONFIG env var). The "
                         "chart mounts the destination's connection_config "
                         "Secret as this env var.")
    args = ap.parse_args()

    if not args.catalog_config:
        ap.error(
            "--catalog-config is required (or set ICEBERG_CATALOG_CONFIG). "
            "The committer needs the destination's connection_config to "
            "build a PyIceberg Catalog. The chart template wires this from "
            "the destination's connection_config Secret; for manual runs, "
            "pass the JSON dest config (e.g. "
            '{"catalog_type":"nessie","nessie_uri":"http://nessie:19120/api/v1",'
            '"warehouse":"s3://...","s3_endpoint":"...","s3_access_key_id":"...",'
            '"s3_secret_access_key":"..."}).'
        )

    tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    if not tables:
        ap.error("--tables must contain at least one non-empty table name "
                 "(got %r)" % (args.tables,))
    try:
        table_namespaces = json.loads(args.table_namespaces or "{}")
    except (TypeError, ValueError):
        ap.error("--table-namespaces must be a JSON object (got %r)" %
                 (args.table_namespaces,))

    rc = redis_lib.from_url(args.redis_url)
    catalog = load_catalog(json.loads(args.catalog_config))
    committer = IcebergCommitter(catalog, rc, args.connection_id,
                                  tables, namespace=args.namespace,
                                  table_namespaces=table_namespaces,
                                  drain_batch=args.drain_batch)
    log.info("IcebergCommitter starting for conn=%s tables=%s",
             args.connection_id, ",".join(tables))
    committer.orphan_sweep_all(register=True)
    committer.run_loop()
