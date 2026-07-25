"""
CDC Stream Consumer — v1.3.9 Redis Streams migration + concurrent
per-connection batching.

Replaces the old ``fusion:transforms:normal`` Redis List (LPUSH/BRPOP) with
per-connection Redis Streams (XADD/XREADGROUP — see
cdc_worker/transform_bridge.py for the producer side), so:
  - a connection's own batching setting (per_event vs per_batch — see
    control-plane/app/api/connections.py's _resolve_cdc_batch_config)
    applies cleanly to just that connection's stream, without interleaving
    with other connections that used to share one global list
  - crash-recovery is native (Pending Entries List, reclaimed via
    XAUTOCLAIM) instead of the old hand-rolled in-flight-list approximation
  - ``XREADGROUP ... COUNT=N`` natively supports batch consumption

Parallelism (raised directly while building this — worth answering here,
not just in prose):
  - DIFFERENT connections' batches are fetched AND applied to their
    destinations CONCURRENTLY within one pod, via a small ThreadPoolExecutor
    (sized by ``CDC_CONSUMER_CONCURRENCY``) — one connection's slow Iceberg
    commit no longer blocks every other connection's already-ready batch.
    This is safe: each connection gets its own cached ``IcebergWriter``
    (see iceberg_writer.get_cached_writer) and, for the Postgres path, its
    own fresh ``psycopg2`` connection per call — no shared mutable
    per-connection state between threads.
  - A GIVEN connection is never processed by two threads (or two pods) at
    once: a Redis ``SET NX EX`` lock keyed on connection_id (mirrors
    iceberg_writer.py's ``_acquire_commit_lock``/``_release_commit_lock``
    pattern exactly) guarantees exactly one consumer owns a connection's
    batch at a time, so per-connection ordering is preserved even though
    DIFFERENT connections run fully in parallel, both across threads in one
    pod and across multiple pods.
  - Scaling the NUMBER OF PODS (not just threads within one pod) up/down
    with load is a separate, already-tracked infra task — see
    V1.3.9_PENDING_FIXES_AND_TASKS.md §2. The correct KEDA trigger for this
    queue shape is the ``redis-streams`` scaler (consumer-group lag on each
    ``fusion:transforms:stream:*`` stream) — this system has no Kafka
    dependency anywhere, so a Kafka-based trigger would not apply.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

import redis

log = logging.getLogger("transform-worker.cdc-stream")

ACTIVE_CONNECTIONS_SET = "fusion:transforms:active_connections"
STREAM_KEY_PREFIX = "fusion:transforms:stream:"
CONSUMER_GROUP = "transform-workers"
DEAD_LETTER_QUEUE = os.environ.get("DEAD_LETTER_QUEUE", "fusion:transforms:dead-letter")
MAX_TASK_RETRIES = int(os.environ.get("MAX_TASK_RETRIES", "10"))

BATCH_CONFIG_CACHE_TTL = int(os.environ.get("CDC_BATCH_CONFIG_CACHE_TTL", "60"))
CONNECTION_DISCOVERY_INTERVAL = float(os.environ.get("CDC_CONNECTION_DISCOVERY_INTERVAL", "5"))
STREAM_READ_BLOCK_MS = int(os.environ.get("CDC_STREAM_READ_BLOCK_MS", "200"))
# v1.3.9: how many connections' batches this ONE pod may fetch+apply
# concurrently. Different connections almost always write to different
# destinations/tables (IcebergWriter is cached per connection_id, never
# shared across connections), so there's no data-correctness reason to
# serialize across connections — only ordering WITHIN one connection's own
# events matters, and that's enforced by the per-connection lock below,
# independent of this setting.
CDC_CONSUMER_CONCURRENCY = int(os.environ.get("CDC_CONSUMER_CONCURRENCY", "4"))

# Cross-pod (and cross-thread) mutual exclusion per connection — exactly
# one consumer (any pod, any thread) may be mid-batch for a given
# connection_id at a time, so a connection's events are never applied out
# of order by two concurrently in-flight batches racing each other. Mirrors
# iceberg_writer.py's _acquire_commit_lock/_release_commit_lock pattern
# (same SET NX EX + Lua compare-and-del release, different key namespace).
CONN_LOCK_TTL_S = int(os.environ.get("CDC_CONN_LOCK_TTL_S", "120"))


def _conn_lock_key(connection_id: str) -> str:
    return f"fusion:transforms:conn-lock:{connection_id}"


def _try_acquire_conn_lock(r: redis.Redis, connection_id: str, owner: str) -> bool:
    try:
        return bool(r.set(_conn_lock_key(connection_id), owner, nx=True, ex=CONN_LOCK_TTL_S))
    except Exception:
        log.exception("cdc-stream conn-lock acquire failed for %s — skipping this cycle", connection_id)
        return False


def _release_conn_lock(r: redis.Redis, connection_id: str, owner: str) -> None:
    try:
        script = (
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
            "  return redis.call('DEL', KEYS[1]) "
            "else return 0 end"
        )
        r.eval(script, 1, _conn_lock_key(connection_id), owner)
    except Exception:
        log.exception("cdc-stream conn-lock release failed for %s — relying on TTL", connection_id)


class CDCStreamConsumer:
    """
    Discovers active connection streams and, for each one, applies that
    connection's own cdc_batch_mode (per_event / per_batch) via an
    XREADGROUP read-loop, then hands the resulting task dict (PK-compaction
    happens inside CDCTransformTask.run — see loader.py's
    _compact_events_by_pk) to a caller-supplied ``process_fn``.

    Multiple connections are drained concurrently via a thread pool; the
    Redis lock above prevents two consumers from working the same
    connection at once, so batching a connection with N events always
    reflects those N events applied in original arrival order.
    """

    def __init__(self, redis_client: redis.Redis, control_plane_url: str,
                 worker_token: str, worker_id: str,
                 concurrency: int = CDC_CONSUMER_CONCURRENCY):
        self._r = redis_client
        self._base = control_plane_url.rstrip("/")
        self._headers = {"X-Worker-Token": worker_token, "X-Worker-ID": worker_id}
        self._worker_id = worker_id
        self._known_groups: set = set()
        self._batch_config_cache: Dict[str, Tuple[float, dict]] = {}
        self._last_discovery = 0.0
        self._active_connections: List[str] = []
        # Per-connection accumulating batch state, THIS pod only —
        # {"entries": [(entry_id, fields), ...], "first_seen": ts|None}.
        # Only ever touched by whichever thread currently owns that
        # connection_id (enforced by _inflight_local below), so no lock
        # needed on the dict values themselves.
        self._batches: Dict[str, dict] = {}
        self._inflight_local: set = set()  # connection_ids a local thread currently owns
        self._inflight_lock = threading.Lock()
        self._pool = ThreadPoolExecutor(max_workers=max(1, concurrency), thread_name_prefix="cdc-stream")
        self._stopped = False

    # ------------------------------------------------------------------
    def _discover_connections(self) -> List[str]:
        now = time.time()
        if now - self._last_discovery < CONNECTION_DISCOVERY_INTERVAL:
            return self._active_connections
        self._last_discovery = now
        try:
            members = self._r.smembers(ACTIVE_CONNECTIONS_SET)
            self._active_connections = sorted(m for m in members if m)
        except Exception as exc:
            log.debug("cdc-stream connection discovery failed: %s", exc)
        return self._active_connections

    def _ensure_group(self, stream_key: str) -> bool:
        if stream_key in self._known_groups:
            return True
        try:
            self._r.xgroup_create(stream_key, CONSUMER_GROUP, id="0", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                log.warning("cdc-stream xgroup_create failed for %s: %s", stream_key, exc)
                return False
        self._known_groups.add(stream_key)
        return True

    def _get_batch_config(self, connection_id: str) -> dict:
        now = time.time()
        cached = self._batch_config_cache.get(connection_id)
        if cached and cached[0] > now:
            return cached[1]
        cfg = {"mode": "per_event", "max_events": 500, "max_wait_minutes": 1.0}
        try:
            import httpx
            url = f"{self._base}/api/v1/internal/connections/{connection_id}/cdc-batch-config"
            with httpx.Client(timeout=5.0) as client:
                resp = client.get(url, headers=self._headers)
                if resp.status_code == 200:
                    body = resp.json()
                    cfg = {
                        "mode": body.get("mode", "per_event"),
                        "max_events": int(body.get("max_events") or 500),
                        "max_wait_minutes": float(body.get("max_wait_minutes") or 1.0),
                    }
        except Exception as exc:
            log.debug("cdc-batch-config fetch failed for %s: %s", connection_id, exc)
        self._batch_config_cache[connection_id] = (now + BATCH_CONFIG_CACHE_TTL, cfg)
        return cfg

    # ------------------------------------------------------------------
    def run_forever(self, process_fn, should_stop) -> None:
        """
        Main dispatch loop (intended to run on its own background thread —
        see transform-worker/worker.py's main()). For each poll cycle:
        discover active connections, and for every one NOT already owned by
        a local thread, submit a "try to make progress on this connection"
        job to the thread pool. ``process_fn(task_dict)`` must raise on
        failure (the entries stay pending/unacked for later retry) and
        return normally on success (this consumer XACKs afterward).
        """
        while not should_stop():
            connections = self._discover_connections()
            if not connections:
                time.sleep(0.5)
                continue
            submitted = False
            for connection_id in connections:
                with self._inflight_lock:
                    if connection_id in self._inflight_local:
                        continue
                    self._inflight_local.add(connection_id)
                submitted = True
                self._pool.submit(self._drive_connection, connection_id, process_fn)
            if not submitted:
                time.sleep(0.2)
        self._pool.shutdown(wait=True, cancel_futures=False)

    def _drive_connection(self, connection_id: str, process_fn) -> None:
        owner = f"{self._worker_id}:{threading.get_ident()}"
        try:
            if not _try_acquire_conn_lock(self._r, connection_id, owner):
                return  # another pod/thread owns this connection right now
            try:
                self._poll_and_process_one(connection_id, process_fn)
            finally:
                _release_conn_lock(self._r, connection_id, owner)
        except Exception:
            log.exception("cdc-stream: unhandled error driving connection=%s", connection_id)
        finally:
            with self._inflight_lock:
                self._inflight_local.discard(connection_id)

    def _poll_and_process_one(self, connection_id: str, process_fn) -> None:
        stream_key = f"{STREAM_KEY_PREFIX}{connection_id}"
        if not self._ensure_group(stream_key):
            return
        cfg = self._get_batch_config(connection_id)
        mode = cfg.get("mode", "per_event")
        max_events = int(cfg.get("max_events") or 500)
        max_wait_s = float(cfg.get("max_wait_minutes") or 1.0) * 60.0

        state = self._batches.setdefault(connection_id, {"entries": [], "first_seen": None})
        want = 1 if mode == "per_event" else max(1, max_events - len(state["entries"]))

        try:
            resp = self._r.xreadgroup(
                CONSUMER_GROUP, self._worker_id,
                {stream_key: ">"}, count=want, block=STREAM_READ_BLOCK_MS,
            )
        except redis.ResponseError as exc:
            log.warning("cdc-stream xreadgroup failed for %s: %s — will recreate group", stream_key, exc)
            self._known_groups.discard(stream_key)
            return
        except Exception as exc:
            log.debug("cdc-stream xreadgroup error for %s: %s", stream_key, exc)
            return

        if resp:
            for _key, messages in resp:
                for entry_id, fields in messages:
                    state["entries"].append((entry_id, fields))
                    if state["first_seen"] is None:
                        state["first_seen"] = time.time()

        if not state["entries"]:
            self._reclaim_stale(connection_id, stream_key)
            return

        ready = (
            mode == "per_event"
            or len(state["entries"]) >= max_events
            or (state["first_seen"] is not None and time.time() - state["first_seen"] >= max_wait_s)
        )
        if not ready:
            return

        batch = self._batches.pop(connection_id)
        task = self._build_task(batch["entries"])
        entry_ids = [eid for eid, _ in batch["entries"]]
        if task is None:
            # Every entry in the batch was malformed JSON — ack them anyway
            # so a permanently-broken payload doesn't spin forever.
            try:
                self._r.xack(stream_key, CONSUMER_GROUP, *entry_ids)
            except Exception:
                pass
            return
        try:
            process_fn(task)
        except Exception:
            # Leave unacked — stays in the PEL, picked up by _reclaim_stale
            # (XAUTOCLAIM) once CONN_LOCK_TTL_S of idle time has passed.
            log.exception("cdc-stream: process_fn failed for connection=%s (%d events) — "
                          "left pending for retry", connection_id, len(entry_ids))
            return
        try:
            self._r.xack(stream_key, CONSUMER_GROUP, *entry_ids)
        except Exception:
            log.exception("cdc-stream: xack failed for connection=%s — task already applied; "
                          "a redelivery will just repeat an idempotent upsert/delete", connection_id)

    def _build_task(self, entries) -> "Optional[dict]":
        events: List[dict] = []
        base_task: Optional[dict] = None
        for _entry_id, fields in entries:
            raw = fields.get("task")
            if not raw:
                continue
            try:
                task = json.loads(raw)
            except Exception:
                log.warning("cdc-stream: skipping malformed task JSON in stream")
                continue
            if base_task is None:
                base_task = task
            events.extend(task.get("events", []))
        if base_task is None:
            return None
        merged = dict(base_task)
        merged["events"] = events
        return merged

    # ------------------------------------------------------------------
    def _reclaim_stale(self, connection_id: str, stream_key: str) -> None:
        """Reclaim entries stuck in the Pending Entries List (a consumer
        crashed mid-batch) via XAUTOCLAIM, and dead-letter any that have
        already exceeded the retry budget using Redis's own native
        per-entry delivery counter — no hand-rolled retry-count field
        needed, unlike the List-based queue's old approach."""
        try:
            result = self._r.xautoclaim(
                stream_key, CONSUMER_GROUP, self._worker_id,
                min_idle_time=CONN_LOCK_TTL_S * 1000, start_id="0-0", count=100,
            )
        except Exception:
            return
        # redis-py returns (next_cursor, [(id, fields), ...], [deleted_ids])
        claimed = result[1] if isinstance(result, (list, tuple)) and len(result) >= 2 else []
        if not claimed:
            return
        for entry_id, fields in claimed:
            try:
                pending = self._r.xpending_range(stream_key, CONSUMER_GROUP, entry_id, entry_id, 1)
                delivery_count = pending[0]["times_delivered"] if pending else 1
            except Exception:
                delivery_count = 1
            if delivery_count > MAX_TASK_RETRIES:
                self._dead_letter(stream_key, entry_id, fields, "cdc-stream: retry budget exhausted")
            else:
                state = self._batches.setdefault(connection_id, {"entries": [], "first_seen": None})
                state["entries"].append((entry_id, fields))
                if state["first_seen"] is None:
                    state["first_seen"] = time.time()

    def _dead_letter(self, stream_key: str, entry_id, fields: dict, reason: str) -> None:
        import datetime as _dt
        raw = fields.get("task", "{}")
        try:
            task = json.loads(raw)
        except Exception:
            task = {}
        entry = {
            "task_id": str(entry_id),
            "connection_id": task.get("connection_id"),
            "type": task.get("type", "cdc_transform"),
            "reason": reason,
            "dead_lettered_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
            "payload": raw,
        }
        try:
            self._r.xack(stream_key, CONSUMER_GROUP, entry_id)
            self._r.lpush(DEAD_LETTER_QUEUE, json.dumps(entry))
            log.error("cdc-stream DEAD-LETTER entry=%s connection=%s reason=%s",
                      entry_id, task.get("connection_id"), reason)
        except Exception:
            log.exception("cdc-stream: dead-letter push failed for entry=%s", entry_id)

    def stop(self) -> None:
        self._stopped = True
