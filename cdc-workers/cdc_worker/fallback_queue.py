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

    def close(self) -> None:
        self._conn.close()
