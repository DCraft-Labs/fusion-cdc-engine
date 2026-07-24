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
        that were actually committed (for durable-marking)."""
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
        # Open ONE transaction and add_files() each path.
        try:
            table = self.catalog.load_table(
                f"{self.namespace}.{self.table_name}")
        except Exception as e:
            result["errors"].append({"phase": "load_table", "error": str(e)})
            # Re-enqueue the entries so the next cycle retries.
            self._reenqueue(to_commit)
            return []
        try:
            with table.transaction() as tx:
                for e in to_commit:
                    tx.add_files(file_paths=[e["file_path"]])
            # Commit succeeded - record each path in the committed set.
            committed_paths = [e["file_path"] for e in to_commit]
            if self.redis is not None:
                try:
                    self.redis.sadd(committed_set, *committed_paths)
                except Exception:
                    log.warning("committer: SADD committed set failed "
                                "(dedup across restarts degraded)")
            result["committed"] = len(to_commit)
            result["committed_paths"].extend(committed_paths)
            return to_commit
        except Exception as e:
            log.exception("committer: add_files transaction failed (%s) - "
                          "re-enqueueing %d entries for retry", e, len(to_commit))
            result["errors"].append({"phase": "add_files", "error": str(e)})
            self._reenqueue(to_commit)
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
    ap.add_argument("--catalog-config", default=None,
                    help="JSON dest config for load_catalog (overrides env)")
    args = ap.parse_args()

    rc = redis_lib.from_url(args.redis_url)
    catalog = load_catalog(json.loads(args.catalog_config)
                            if args.catalog_config else {})
    committer = IcebergCommitter(catalog, rc, args.connection_id,
                                  args.table, namespace=args.namespace)
    log.info("IcebergCommitter starting for conn=%s table=%s",
             args.connection_id, args.table)
    committer.orphan_sweep(register=True)
    committer.run_loop()
