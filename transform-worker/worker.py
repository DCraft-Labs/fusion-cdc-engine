#!/usr/bin/env python3
"""
Transform Worker — DuckDB-based transform engine (replaces Spark).
Pulls tasks from Redis queues, executes all 10 transform types, writes to Postgres/Iceberg.

Queue priority:
  fusion:transforms:high   → initial loads (100M rows, chunked PK ranges)
  fusion:transforms:normal → CDC events with column transforms

Scale-to-zero: KEDA starts this pod only when queue depth > 0.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time

import redis

from engine import DuckDBTransformEngine
from loader import InitialLoadTask, CDCTransformTask, STOP_EVENT

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("transform-worker")

REDIS_URL = os.environ["REDIS_URL"]
HIGH_QUEUE = os.environ.get("HIGH_PRIORITY_QUEUE", "fusion:transforms:high")
NORMAL_QUEUE = os.environ.get("NORMAL_PRIORITY_QUEUE", "fusion:transforms:normal")
# v1.2.25 Task 6: dead-letter list for tasks that exhausted their retry budget.
# Surfaced in /api/v1/monitoring/health and requeueable via
# POST /api/v1/tasks/dead-letter/{task_id}/requeue.
DEAD_LETTER_QUEUE = os.environ.get("DEAD_LETTER_QUEUE", "fusion:transforms:dead-letter")
# v1.2.30 Defect D fix: per-worker in-flight list. Each task is atomically
# moved (BLMOVE / BRPOPLPUSH) from the main queue to this list on dequeue,
# and only removed (LREM) on ack (success or dead-letter). This prevents two
# pods from dequeuing the same task_id concurrently — the old BRPOP path left
# the task in the main queue during retry/backoff, so a sibling pod could
# grab the re-enqueued copy while the original pod was still sleeping, causing
# "snapshot id changed" Iceberg conflicts on the same partition.
IN_FLIGHT_QUEUE = os.environ.get(
    "IN_FLIGHT_QUEUE", f"fusion:transforms:in-flight:{os.environ.get('WORKER_ID', 'transform-worker-0')}"
)
WORKER_ID = os.environ.get("WORKER_ID", "transform-worker-0")
CONTROL_PLANE_URL = os.environ.get("CONTROL_PLANE_URL", "http://fusion-control-plane-svc.fusion.svc.cluster.local:8000")
ENCRYPTION_KEY = os.environ["ENCRYPTION_KEY"]
# v1.2.18: renamed from METADATA_DB_DSN to DATABASE_URL to match the env var
# the Helm chart already injects via the fusion-cdc-secrets Secret. The old
# name broke the transform-worker on the public chart (CreateContainerConfigError
# / missing env var) unless the operator manually applied
# patch-cdc-worker-metadata-dsn.json. DATABASE_URL is now the canonical name;
# METADATA_DB_DSN is still accepted as a fallback for older deployments.
DATABASE_URL = os.environ.get("DATABASE_URL") or os.environ.get("METADATA_DB_DSN")
if not DATABASE_URL:
    raise RuntimeError(
        "transform-worker: DATABASE_URL env var is required "
        "(or METADATA_DB_DSN for legacy deployments)."
    )

# v1.2.25 Task 6: retry budget + exponential backoff.
# Before this, a permanently-failing task was retried in a tight loop (19
# failures in 40 seconds) with no backoff, no max-retry, no dead-letter —
# wasting worker CPU and flooding the logs. Now a task gets at most
# MAX_TASK_RETRIES attempts, with exponential backoff between them, and on
# exhaustion is moved to the dead-letter list for manual inspection/requeue.
MAX_TASK_RETRIES = int(os.environ.get("MAX_TASK_RETRIES", "10"))
# Backoff schedule (seconds): 1, 2, 4, 8, 16, 32, 60 (cap).
_BACKOFF_SCHEDULE = [1, 2, 4, 8, 16, 32, 60]

_shutdown = False


def _handle_signal(sig, _frame):
    global _shutdown
    log.info("Received signal %s — draining current chunk then shutting down", sig)
    _shutdown = True
    STOP_EVENT.set()


def _backoff_seconds(retry_count: int) -> int:
    """Exponential backoff: 1, 2, 4, 8, 16, 32, then 60s cap."""
    if retry_count < 0:
        return 0
    if retry_count < len(_BACKOFF_SCHEDULE):
        return _BACKOFF_SCHEDULE[retry_count]
    return _BACKOFF_SCHEDULE[-1]  # 60s cap


def _interruptible_sleep(seconds: float) -> None:
    """Sleep that returns early on SIGTERM/SIGINT so the worker can shut down
    mid-backoff without waiting the full delay."""
    deadline = time.monotonic() + seconds
    while not _shutdown and time.monotonic() < deadline:
        time.sleep(min(0.5, deadline - time.monotonic()))


def _dead_letter(r: redis.Redis, task: dict, raw_task: str, reason: str) -> None:
    """Move a task to the dead-letter list and log an alert.

    The dead-letter entry wraps the original task with the failure reason and
    the timestamp, so the GET /connections/{id}/tasks/dead-letter endpoint
    can surface it and the POST /tasks/dead-letter/{task_id}/requeue endpoint
    can re-enqueue the original payload.
    """
    import datetime as _dt
    entry = {
        "task_id": task.get("task_id"),
        "connection_id": task.get("connection_id"),
        "type": task.get("type", "cdc_transform"),
        "reason": reason,
        "dead_lettered_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "payload": raw_task,
    }
    r.lpush(DEAD_LETTER_QUEUE, json.dumps(entry))
    log.error("DEAD-LETTER task_id=%s connection=%s type=%s reason=%s — moved to %s after exhausting %d retries",
              task.get("task_id"), task.get("connection_id"), task.get("type"),
              reason, DEAD_LETTER_QUEUE, MAX_TASK_RETRIES)


def _atomic_dequeue(r: redis.Redis, timeout: int = 5):
    """v1.2.30 Defect D fix: atomically move one task from the main queue to
    this worker's in-flight list, so two pods can never dequeue the same
    task_id concurrently. Tries HIGH first (short 1s block), then NORMAL
    (remaining timeout). Returns ``(queue_name, raw_task)`` or ``None`` when
    no task arrives within ``timeout`` seconds.

    Uses BLMOVE (Redis 6.2+) with a BRPOPLPUSH fallback for older Redis. Both
    pop from the RIGHT of the source and push to the LEFT of the in-flight
    list, matching the legacy BRPOP semantics (oldest LPUSHed task first).
    """
    # Try the high-priority queue first (1s block so a pending high task is
    # picked up immediately, but we don't block the whole timeout on it).
    try:
        raw = r.blmove(HIGH_QUEUE, IN_FLIGHT_QUEUE, timeout=1, src="RIGHT", dest="LEFT")
    except redis.ResponseError:
        # Older Redis (<6.2) has no BLMOVE — fall back to BRPOPLPUSH.
        raw = r.brpoplpush(HIGH_QUEUE, IN_FLIGHT_QUEUE, timeout=1)
    if raw is not None:
        return HIGH_QUEUE, raw
    # Then the normal-priority queue for the remainder of the window.
    try:
        raw = r.blmove(NORMAL_QUEUE, IN_FLIGHT_QUEUE, timeout=max(1, timeout - 1), src="RIGHT", dest="LEFT")
    except redis.ResponseError:
        raw = r.brpoplpush(NORMAL_QUEUE, IN_FLIGHT_QUEUE, timeout=max(1, timeout - 1))
    if raw is not None:
        return NORMAL_QUEUE, raw
    return None


def _ack(r: redis.Redis, raw_task: str) -> None:
    """v1.2.30 Defect D fix: remove ``raw_task`` from this worker's in-flight
    list on successful completion (ack). LREM removes the first ``count``
    occurrences; we pass count=1 so only this worker's copy is removed."""
    r.lrem(IN_FLIGHT_QUEUE, 1, raw_task)


def _requeue_after_backoff(r: redis.Redis, raw_task: str, updated_task: dict) -> None:
    """v1.2.30 Defect D fix: after the backoff sleep, atomically move the
    task from this worker's in-flight list back to the high-priority queue
    with the updated payload (incremented retry_count). During the backoff
    the task stays ONLY in the in-flight list — no sibling pod can dequeue
    it, so the same partition is never processed by two pods at once."""
    r.lrem(IN_FLIGHT_QUEUE, 1, raw_task)
    r.lpush(HIGH_QUEUE, json.dumps(updated_task))


def _dead_letter_from_inflight(r: redis.Redis, raw_task: str, task: dict, reason: str) -> None:
    """v1.2.30 Defect D fix: remove the task from the in-flight list and move
    it to the dead-letter list (retry budget exhausted)."""
    r.lrem(IN_FLIGHT_QUEUE, 1, raw_task)
    _dead_letter(r, task, raw_task, reason)


def main():
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    # v1.2.28 Task 2: expose Prometheus metrics for the initial-load pipeline.
    try:
        from loader import _start_metrics_http_server
        _start_metrics_http_server()
    except Exception as e:
        log.warning("Prometheus metrics server not started: %s", e)

    r = redis.from_url(REDIS_URL, decode_responses=True)
    engine = DuckDBTransformEngine(
        metadata_db_dsn=DATABASE_URL,
        encryption_key=ENCRYPTION_KEY,
        control_plane_url=CONTROL_PLANE_URL,
        worker_id=WORKER_ID,
    )

    log.info("Transform worker %s started — watching queues %s | %s (max_retries=%d dead_letter=%s)",
             WORKER_ID, HIGH_QUEUE, NORMAL_QUEUE, MAX_TASK_RETRIES, DEAD_LETTER_QUEUE)

    while not _shutdown:
        # v1.2.30 Defect D fix: atomic dequeue (BLMOVE) from the main queue to
        # this worker's in-flight list. The task is removed from the main
        # queue atomically — two pods can never dequeue the same task_id
        # concurrently. It is removed from the in-flight list on ack (success
        # or dead-letter) or moved back to the main queue after backoff.
        dequeued = _atomic_dequeue(r, timeout=5)
        if dequeued is None:
            # No tasks — KEDA will scale us down soon
            continue

        queue_name, raw_task = dequeued
        try:
            task = json.loads(raw_task)
            task_type = task.get("type", "cdc_transform")
            log.info("Processing task type=%s id=%s from %s (retry=%s)",
                     task_type, task.get("task_id"), queue_name, task.get("_retry_count", 0))

            if task_type == "initial_load":
                loader = InitialLoadTask(engine=engine, redis_client=r)
                loader.run(task)
            elif task_type == "cdc_transform":
                cdc_task = CDCTransformTask(engine=engine)
                cdc_task.run(task)
            else:
                log.warning("Unknown task type: %s — skipping", task_type)

            # v1.2.30 Defect D fix: ack — remove the task from this worker's
            # in-flight list on successful completion.
            _ack(r, raw_task)

        except Exception as exc:
            retry_count = int(task.get("_retry_count", 0)) if isinstance(task, dict) else 0
            retry_count += 1
            if retry_count > MAX_TASK_RETRIES:
                # v1.2.25 Task 6: circuit-breaker — move to dead-letter.
                # v1.2.30 Defect D fix: remove from in-flight first.
                _dead_letter_from_inflight(r, raw_task, task, reason=str(exc)[:500])
                continue
            # Exponential backoff: 1, 2, 4, 8, 16, 32, 60 (cap).
            delay = _backoff_seconds(retry_count - 1)
            log.warning("Task id=%s failed (attempt %d/%d): %s — sleeping %ds then re-queuing with backoff",
                        task.get("task_id"), retry_count, MAX_TASK_RETRIES,
                        str(exc)[:200], delay)
            task["_retry_count"] = retry_count
            task["_last_error"] = str(exc)[:500]
            task["_last_failed_at"] = time.time()
            # v1.2.30 Defect D fix: the task STAYS in the in-flight list
            # during the backoff sleep (no sibling pod can dequeue it), then
            # is atomically moved back to the high-priority queue with the
            # updated retry_count.
            _interruptible_sleep(delay)
            _requeue_after_backoff(r, raw_task, task)

    log.info("Transform worker %s exiting cleanly", WORKER_ID)


if __name__ == "__main__":
    main()
