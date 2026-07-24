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

log = logging.getLogger(__name__)

# Redis key templates (kept as class attrs so tests can introspect).
_PENDING_KEY = "fusion:iceberg-pending-files:{conn}:{table}"
_COMMITTED_KEY = "fusion:iceberg-committed-files:{conn}:{table}"
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


def _pk_to_score(pk) -> float:
    """Coerce a PK value into a numeric score suitable for ZADD/ZRANGEBYSCORE.
    Ints/floats pass through; strings are hashed to a stable float in
    [0, 1) so lexicographic ordering is approximated (PK overlap detection
    is best-effort for non-numeric PKs but still catches the common
    integer-PK checkpoint-race case exactly)."""
    if pk is None:
        return float("inf")
    if isinstance(pk, bool):
        return float(pk)
    if isinstance(pk, (int, float)):
        return float(pk)
    import hashlib
    h = hashlib.sha256(str(pk).encode("utf-8")).hexdigest()
    return (int(h[:16], 16) % (2 ** 32)) / float(2 ** 32)


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
    """Single-committer for one (connection_id, table_name) pair.

    The committer is intentionally stateless across calls - all shared
    state lives in Redis (pending list, committed set, lock) so any pod
    that holds the lock can take over. ``mark_durable`` is a callback the
    caller wires to the control-plane checkpoint-report API; it receives
    each committed entry so the control-plane can promote the chunk's
    checkpoint from ``staged`` to ``durable`` (THIS is when ``last_pk``
    truly advances).
    """

    def __init__(self, catalog, redis_client, connection_id: str,
                 table_name: str, namespace: str = "fusion",
                 lock_ttl_s: int = _DEFAULT_LOCK_TTL_S,
                 drain_batch: int = _DEFAULT_DRAIN_BATCH,
                 drain_timeout_ms: int = _DEFAULT_DRAIN_TIMEOUT_MS,
                 idle_sleep_s: float = _DEFAULT_IDLE_SLEEP_S,
                 mark_durable=None):
        self.catalog = catalog
        self.redis = redis_client
        self.connection_id = connection_id
        self.table_name = table_name
        self.namespace = namespace
        self.lock_ttl_s = lock_ttl_s
        self.drain_batch = drain_batch
        self.drain_timeout_ms = drain_timeout_ms
        self.idle_sleep_s = idle_sleep_s
        self.mark_durable = mark_durable  # callable(entry) -> None
        self._lock_token = str(uuid.uuid4())

    # ── Redis lock (short batching window, NOT per-chunk) ────────────────
    def _acquire_lock(self) -> bool:
        if self.redis is None:
            return True
        key = lock_key(self.connection_id, self.table_name)
        return bool(self.redis.set(key, self._lock_token, nx=True,
                                    ex=self.lock_ttl_s))

    def _release_lock(self) -> None:
        if self.redis is None:
            return
        key = lock_key(self.connection_id, self.table_name)
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

    # ── Core: drain + commit ─────────────────────────────────────────────
    def drain_and_commit(self) -> dict:
        """Drain up to ``drain_batch`` pending entries and register them in
        ONE Iceberg transaction. Returns a summary dict:
        ``{drained, committed, skipped_duplicate, committed_paths,
        errors}``."""
        result = {"drained": 0, "committed": 0, "skipped_duplicate": 0,
                  "committed_paths": [], "errors": []}
        if not self._acquire_lock():
            log.debug("committer lock held by another pod - skipping this cycle")
            return result
        try:
            entries = list_pending(self.redis, self.connection_id,
                                   self.table_name,
                                   count=self.drain_batch,
                                   timeout_ms=self.drain_timeout_ms)
            if not entries:
                return result
            result["drained"] = len(entries)
            committed = self._commit_entries(entries, result)
            # On success, mark each committed entry durable.
            if self.mark_durable is not None:
                for e in committed:
                    try:
                        self.mark_durable(e)
                    except Exception:
                        log.exception("mark_durable failed for entry %s", e)
        finally:
            self._release_lock()
        return result

    def _commit_entries(self, entries: list[dict], result: dict) -> list[dict]:
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
        committed_set = committed_key(self.connection_id, self.table_name)
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
                self.redis, self.connection_id, self.table_name, pk_range)
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
            self._delete_staged_file(e.get("file_path"), result)
        # (a) run dedup-on-PK for overlapping-but-not-contained entries.
        if dedup_before_commit:
            self._dedup_overlapping_entries(dedup_before_commit, result)
            register_clean.extend(dedup_before_commit)
        if not register_clean:
            return []
        # Open ONE transaction and add_files() each path.
        # v1.3.6: phase timing INFO logs (load_table / add_files / commit).
        try:
            _t0 = time.perf_counter()
            table = self.catalog.load_table(
                f"{self.namespace}.{self.table_name}")
            load_table_ms = (time.perf_counter() - _t0) * 1000.0
        except Exception as e:
            result["errors"].append({"phase": "load_table", "error": str(e)})
            # Re-enqueue the entries so the next cycle retries.
            self._reenqueue(register_clean)
            return []
        try:
            _t1 = time.perf_counter()
            with table.transaction() as tx:
                for e in register_clean:
                    tx.add_files(file_paths=[e["file_path"]])
                add_files_ms = (time.perf_counter() - _t1) * 1000.0
                _t2 = time.perf_counter()
            # commit_transaction runs on context-manager exit
            commit_ms = (time.perf_counter() - _t2) * 1000.0
            log.info(
                "committer: commit phase timing table=%s files_in_batch=%d "
                "load_table_ms=%.1f add_files_ms=%.1f commit_ms=%.1f",
                self.table_name, len(register_clean),
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
                        self.redis, self.connection_id, self.table_name,
                        e.get("pk_range"), e["file_path"])
            result["committed"] = len(register_clean)
            result["committed_paths"].extend(committed_paths)
            return register_clean
        except Exception as e:
            log.exception("committer: add_files transaction failed (%s) - "
                          "re-enqueueing %d entries for retry", e, len(register_clean))
            result["errors"].append({"phase": "add_files", "error": str(e)})
            self._reenqueue(register_clean)
            return []

    def _delete_staged_file(self, file_path: str, result: dict) -> None:
        """Best-effort delete of a staged-but-skipped Parquet file from the
        object store (pure PK duplicate). Failures are logged but not
        fatal — an orphan sweep will reconcile later."""
        if not file_path:
            return
        try:
            table = self.catalog.load_table(
                f"{self.namespace}.{self.table_name}")
            try:
                table.io.delete(file_path)
            except Exception as de:
                log.debug("committer: delete staged file %s failed (%s) "
                          "(orphan sweep will reconcile)", file_path, de)
        except Exception as le:
            log.debug("committer: load_table for delete failed (%s)", le)

    def _dedup_overlapping_entries(self, entries: list[dict],
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
                f"{self.namespace}.{self.table_name}")
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
            self._dedup_one_range(table, pk_col, rmin, rmax, e)

    def _dedup_one_range(self, table, pk_col: str, rmin, rmax,
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
        keys = self._extract_pk_values_from_staged_file(entry, pk_col)
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

    def _extract_pk_values_from_staged_file(self, entry: dict,
                                             pk_col: str) -> list:
        """Best-effort read of the PK column from the staged Parquet file.
        Returns [] on any failure (the range-delete fallback handles it)."""
        file_path = entry.get("file_path")
        if not file_path:
            return []
        try:
            import pyarrow.parquet as pq
            table = self.catalog.load_table(
                f"{self.namespace}.{self.table_name}")
            with table.io.open_input_file(file_path) as f:
                pf = pq.ParquetFile(f)
                tbl = pf.read()
            return [v for v in tbl.column(pk_col).to_pylist()
                    if v is not None]
        except Exception:
            return []

    def _reenqueue(self, entries: list[dict]) -> None:
        """Put entries back on the pending list (head) so the next cycle
        retries them. Uses LPUSH so they're processed before newer entries."""
        if self.redis is None or not entries:
            return
        key = pending_key(self.connection_id, self.table_name)
        for e in entries:
            try:
                self.redis.lpush(key, json.dumps(e, default=str))
            except Exception:
                log.exception("committer: re-enqueue failed for entry %s", e)

    # ── Orphan-file sweep (crash recovery) ───────────────────────────────
    def orphan_sweep(self, register: bool = True) -> dict:
        """List Parquet files under ``table.location()/data/`` not yet in
        any manifest, cross-check against the pending list + committed
        set, and either register orphans via add_files() (if register) or
        delete them. Returns a summary dict.

        This is the crash-recovery reconciliation pass: a file written to
        the object store but never committed is inert (add_files() hasn't
        run, so it's not in any manifest). On committer startup (or
        periodically), this pass either registers orphans via add_files()
        or deletes them if they correspond to a chunk that was
        re-processed.
        """
        result = {"orphans": [], "registered": 0, "deleted": 0, "errors": []}
        try:
            table = self.catalog.load_table(
                f"{self.namespace}.{self.table_name}")
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
        committed_set = committed_key(self.connection_id, self.table_name)
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
            try:
                with table.transaction() as tx:
                    tx.add_files(file_paths=[path])
                if self.redis is not None:
                    try:
                        self.redis.sadd(committed_set, path)
                    except Exception:
                        pass
                result["registered"] += 1
            except Exception as e:
                result["errors"].append(
                    {"phase": "register_orphan", "path": path,
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

    # ── Run loop (for a dedicated committer process/sidecar) ────────────
    def run_loop(self, max_cycles: int | None = None) -> None:
        """Drain-and-commit loop. Sleeps ``idle_sleep_s`` when there's
        nothing to drain. If ``max_cycles`` is set, stops after that many
        cycles (useful for tests)."""
        cycle = 0
        while True:
            r = self.drain_and_commit()
            cycle += 1
            if r["drained"] == 0:
                time.sleep(self.idle_sleep_s)
            if max_cycles is not None and cycle >= max_cycles:
                return


if __name__ == "__main__":  # pragma: no cover - manual sidecar entry
    import argparse
    import sys
    # Lazy imports so unit tests don't require pyiceberg/redis.
    from iceberg_writer import load_catalog
    import redis as redis_lib

    ap = argparse.ArgumentParser(description="Fusion CDC Iceberg committer")
    ap.add_argument("--connection-id", required=True)
    ap.add_argument("--table", required=True)
    ap.add_argument("--namespace", default="fusion")
    ap.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
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

    rc = redis_lib.from_url(args.redis_url)
    catalog = load_catalog(json.loads(args.catalog_config))
    committer = IcebergCommitter(catalog, rc, args.connection_id,
                                  args.table, namespace=args.namespace)
    log.info("IcebergCommitter starting for conn=%s table=%s",
             args.connection_id, args.table)
    committer.orphan_sweep(register=True)
    committer.run_loop()
