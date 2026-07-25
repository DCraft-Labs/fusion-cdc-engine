"""
P2.6 — SQLite-backed fallback queue.

When Redis is unavailable, events are written here so they can be
re-published once connectivity is restored.

Schema
------
fallback_events(
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    event_json TEXT    NOT NULL,
    routing_json TEXT NOT NULL,
    queued_at  INTEGER NOT NULL,
    flushed    INTEGER NOT NULL DEFAULT 0   -- 0=pending, 1=flushed
)
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cdc_worker.redis_publisher import RedisStreamPublisher

log = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS fallback_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    event_json   TEXT    NOT NULL,
    routing_json TEXT    NOT NULL,
    queued_at    INTEGER NOT NULL,
    flushed      INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_flushed ON fallback_events(flushed);

-- 2026-07-25 addition (Bug #21): TransformBridge.publish_event()'s LPUSH into
-- fusion:transforms:normal had NO fallback of its own -- confirmed live, a
-- ~5-minute Redis outage silently dropped every single CDC-driven change for
-- that window with zero recovery, even though the sibling XADD path above
-- (fallback_events) correctly buffered and replayed. drain() above replays
-- into RedisStreamPublisher.publish() only -- it never re-LPUSHes a bridge
-- task, so fallback_events being durable did nothing for the actual
-- Iceberg-writing pipeline (transform-worker only BRPOPs from
-- fusion:transforms:*, never reads the cdc:* streams). This second table
-- gives the bridge path the same durability, independently, since the two
-- failure domains are not interchangeable.
CREATE TABLE IF NOT EXISTS fallback_bridge_tasks (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_name   TEXT    NOT NULL,
    task_json    TEXT    NOT NULL,
    queued_at    INTEGER NOT NULL,
    flushed      INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_bridge_flushed ON fallback_bridge_tasks(flushed);
"""

class FallbackQueue:
    """
    Durable local queue for CDC events that could not be published to Redis.

    Parameters
    ----------
    db_path : str or Path
        SQLite file path.  Use ":memory:" in unit tests.
    """

    def __init__(self, db_path: str = ":memory:") -> None:
        self._path = str(db_path)
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enqueue(self, event, routing: dict) -> None:
        """
        Persist one event + its routing dict to the queue.

        Parameters
        ----------
        event : CDCEvent
        routing : dict  e.g. {"bank_id": ..., "tenant_id": ..., "source_id": ...}
        """
        event_json = json.dumps(event.to_redis_dict())
        routing_json = json.dumps(routing)
        self._conn.execute(
            "INSERT INTO fallback_events (event_json, routing_json, queued_at) VALUES (?,?,?)",
            (event_json, routing_json, int(time.time() * 1000)),
        )
        self._conn.commit()

    def drain(self, publisher: "RedisStreamPublisher", tenant: str = "unknown", source: str = "unknown") -> int:
        """
        Attempt to re-publish all pending events via publisher.

        Stops on the first publish failure to preserve ordering.

        Returns the number of successfully flushed events.
        """
        from cdc_worker.event_envelope import CDCEvent
        from cdc_worker.metrics import METRICS

        rows = self._conn.execute(
            "SELECT id, event_json, routing_json FROM fallback_events WHERE flushed=0 ORDER BY id"
        ).fetchall()

        flushed = 0
        for row_id, event_json, routing_json in rows:
            try:
                redis_dict = json.loads(event_json)
                event = CDCEvent.from_redis_dict(redis_dict)
                routing = json.loads(routing_json)
                ok = publisher.publish(event, routing=[routing])
            except Exception as exc:
                log.error("fallback drain failed to reconstruct event %d: %s", row_id, exc)
                break

            if not ok:
                break

            self._conn.execute(
                "UPDATE fallback_events SET flushed=1 WHERE id=?", (row_id,)
            )
            self._conn.commit()
            flushed += 1
            METRICS.fallback_queue_drained_total.labels(tenant=tenant, source=source).inc()

        return flushed

    def queue_length(self) -> int:
        """Return number of pending (unflushed) events."""
        row = self._conn.execute(
            "SELECT COUNT(*) FROM fallback_events WHERE flushed=0"
        ).fetchone()
        return row[0]

    # ------------------------------------------------------------------
    # Bridge-task fallback (Bug #21) — independent of the event/routing
    # methods above. TransformBridge.publish_event() enqueues here when its
    # XADD into a per-connection ``fusion:transforms:stream:{connection_id}``
    # fails; drain_bridge_tasks() replays directly via XADD once Redis is
    # reachable again.
    #
    # v1.3.9: the column is still named ``queue_name`` (schema-compat with
    # existing rows from before the Streams migration) but now holds a
    # Stream key rather than a List key — drain replays via XADD, not
    # LPUSH, to match transform_bridge.py's producer-side change.
    # ------------------------------------------------------------------

    def enqueue_bridge_task(self, queue_name: str, task: dict) -> None:
        """Persist one transform-worker task that failed to XADD."""
        self._conn.execute(
            "INSERT INTO fallback_bridge_tasks (queue_name, task_json, queued_at) VALUES (?,?,?)",
            (queue_name, json.dumps(task), int(time.time() * 1000)),
        )
        self._conn.commit()

    def drain_bridge_tasks(self, redis_client) -> int:
        """Attempt to re-XADD all pending bridge tasks in original order.

        Also re-registers each task's connection_id in the active-connections
        set (fusion:transforms:active_connections) so the transform-worker's
        stream consumer discovers the stream even if this is the very first
        successful publish for that connection since Redis recovered.

        Stops on the first XADD failure to preserve ordering (same
        contract as drain()). Returns the number successfully flushed.
        """
        rows = self._conn.execute(
            "SELECT id, queue_name, task_json FROM fallback_bridge_tasks "
            "WHERE flushed=0 ORDER BY id"
        ).fetchall()

        flushed = 0
        for row_id, queue_name, task_json in rows:
            try:
                redis_client.xadd(queue_name, {"task": task_json})
                try:
                    connection_id = json.loads(task_json).get("connection_id")
                    if connection_id:
                        redis_client.sadd("fusion:transforms:active_connections", connection_id)
                except Exception:
                    pass
            except Exception as exc:
                log.warning("bridge fallback drain: XADD failed for task %d, will retry later: %s",
                            row_id, exc)
                break
            self._conn.execute(
                "UPDATE fallback_bridge_tasks SET flushed=1 WHERE id=?", (row_id,)
            )
            self._conn.commit()
            flushed += 1
        return flushed

    def bridge_queue_length(self) -> int:
        """Return number of pending (unflushed) bridge tasks."""
        row = self._conn.execute(
            "SELECT COUNT(*) FROM fallback_bridge_tasks WHERE flushed=0"
        ).fetchone()
        return row[0]

    def close(self) -> None:
        self._conn.close()
