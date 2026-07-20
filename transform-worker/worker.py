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
from loader import InitialLoadTask, CDCTransformTask

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("transform-worker")

REDIS_URL = os.environ["REDIS_URL"]
HIGH_QUEUE = os.environ.get("HIGH_PRIORITY_QUEUE", "fusion:transforms:high")
NORMAL_QUEUE = os.environ.get("NORMAL_PRIORITY_QUEUE", "fusion:transforms:normal")
WORKER_ID = os.environ.get("WORKER_ID", "transform-worker-0")
CONTROL_PLANE_URL = os.environ.get("CONTROL_PLANE_URL", "http://fusion-control-plane-svc.fusion.svc.cluster.local:8000")
ENCRYPTION_KEY = os.environ["ENCRYPTION_KEY"]
METADATA_DB_DSN = os.environ["METADATA_DB_DSN"]

_shutdown = False


def _handle_signal(sig, _frame):
    global _shutdown
    log.info("Received signal %s — draining current task then shutting down", sig)
    _shutdown = True


def main():
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    r = redis.from_url(REDIS_URL, decode_responses=True)
    engine = DuckDBTransformEngine(
        metadata_db_dsn=METADATA_DB_DSN,
        encryption_key=ENCRYPTION_KEY,
        control_plane_url=CONTROL_PLANE_URL,
        worker_id=WORKER_ID,
    )

    log.info("Transform worker %s started — watching queues %s | %s", WORKER_ID, HIGH_QUEUE, NORMAL_QUEUE)

    while not _shutdown:
        # BRPOP with priority: high queue first, timeout 5s
        result = r.brpop([HIGH_QUEUE, NORMAL_QUEUE], timeout=5)
        if result is None:
            # No tasks — KEDA will scale us down soon
            continue

        queue_name, raw_task = result
        try:
            task = json.loads(raw_task)
            task_type = task.get("type", "cdc_transform")
            log.info("Processing task type=%s id=%s from %s", task_type, task.get("task_id"), queue_name)

            if task_type == "initial_load":
                loader = InitialLoadTask(engine=engine, redis_client=r)
                loader.run(task)
            elif task_type == "cdc_transform":
                cdc_task = CDCTransformTask(engine=engine)
                cdc_task.run(task)
            else:
                log.warning("Unknown task type: %s — skipping", task_type)

        except Exception:
            log.exception("Task failed: %s — re-queuing to high priority", raw_task[:200])
            # Re-queue failed task for retry (back to high priority so it's retried soon)
            r.lpush(HIGH_QUEUE, raw_task)
            time.sleep(1)

    log.info("Transform worker %s exiting cleanly", WORKER_ID)


if __name__ == "__main__":
    main()
