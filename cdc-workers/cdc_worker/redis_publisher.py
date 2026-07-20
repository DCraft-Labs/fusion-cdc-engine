"""
P2.5 — Redis Stream Publisher.

Stream key format:
    cdc:{bank_id}:{tenant_id}:{source_id}:{schema_name}:{table_name}

Each event is written via XADD with approximate MAXLEN to cap memory.
Consumer groups are created automatically on first publish.
Falls back to FallbackQueue on any redis.RedisError.
"""

from __future__ import annotations

import logging
from typing import List, Optional

import redis.exceptions

from cdc_worker.event_envelope import CDCEvent

log = logging.getLogger(__name__)


def _stream_key(event: CDCEvent) -> str:
    return (
        f"cdc:{event.bank_id}:{event.tenant_id}:"
        f"{event.source_id}:{event.schema_name}:{event.table_name}"
    )


class RedisStreamPublisher:
    """
    Publishes CDCEvents to Redis Streams.

    Parameters
    ----------
    redis_url : str
        Redis connection URL (redis://host:port/db).
    maxlen : int
        Approximate stream length cap (XADD MAXLEN ~ maxlen).
    consumer_group : str
        Consumer group name — created automatically.
    fallback : FallbackQueue | None
        When provided, events that fail to publish are enqueued here.
    """

    def __init__(
        self,
        redis_url: str,
        maxlen: int = 100_000,
        consumer_group: str = "fusion-spark",
        fallback=None,
    ) -> None:
        import redis as redis_lib

        self._client = redis_lib.from_url(redis_url, decode_responses=True)
        self._maxlen = maxlen
        self._consumer_group = consumer_group
        self._fallback = fallback
        self._known_streams: set = set()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def publish(self, event: CDCEvent, routing: Optional[List[dict]] = None) -> bool:
        """
        Publish event to all applicable Redis streams.

        routing is a list of dicts like:
          [{"bank_id": ..., "tenant_id": ..., "source_id": ...}, ...]
        When None the event's own bank_id/tenant_id/source_id are used.

        Returns True if all publishes succeeded, False if any failed (and
        fallback was invoked).
        """
        targets = routing if routing is not None else [
            {
                "bank_id": event.bank_id,
                "tenant_id": event.tenant_id,
                "source_id": event.source_id,
            }
        ]

        all_ok = True
        for target in targets:
            # Construct a per-routing-target event copy (same data, different ids)
            routed_event = _routed(event, target)
            key = _stream_key(routed_event)
            ok = self._xadd(key, routed_event)
            if not ok:
                all_ok = False
                if self._fallback is not None:
                    self._fallback.enqueue(event, target)
        return all_ok

    def close(self) -> None:
        self._client.close()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ensure_consumer_group(self, key: str) -> None:
        if key in self._known_streams:
            return
        try:
            self._client.xgroup_create(key, self._consumer_group, id="0", mkstream=True)
        except redis.exceptions.ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                pass  # group already exists — fine
            else:
                raise
        self._known_streams.add(key)

    def _xadd(self, key: str, event: CDCEvent) -> bool:
        try:
            self._ensure_consumer_group(key)
            self._client.xadd(
                key,
                event.to_redis_dict(),
                maxlen=self._maxlen,
                approximate=True,
            )
            return True
        except redis.exceptions.RedisError as exc:
            log.error("XADD to %s failed: %s", key, exc)
            return False


# ---------------------------------------------------------------------------
# Helper: shallow copy event with different routing ids
# ---------------------------------------------------------------------------

def _routed(event: CDCEvent, target: dict) -> CDCEvent:
    """Return a copy of the event with bank_id/tenant_id/source_id overridden."""
    from dataclasses import replace
    return replace(
        event,
        bank_id=target.get("bank_id", event.bank_id),
        tenant_id=target.get("tenant_id", event.tenant_id),
        source_id=target.get("source_id", event.source_id),
    )
