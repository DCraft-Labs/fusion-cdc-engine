"""
CDC → Transform-Worker bridge.

Architectural note (the bug this fixes):
  - cdc-worker publishes CDC events to Redis STREAMS via XADD
    (keys like ``cdc:{bank}:{tenant}:{source}:{schema}:{table}``).
  - transform-worker consumes from Redis LISTS via BRPOP
    (``fusion:transforms:high`` / ``fusion:transforms:normal``).
  These are different Redis data structures and key namespaces, so before
  this bridge CDC events were never consumed and CDC never synced end-to-end.

This module closes that gap. For each CDC event the worker emits, it:
  1. Resolves the (source_id, schema, table) → active Connection/Destination/
     Stream routes via the control-plane internal endpoint
     ``GET /api/v1/internal/workers/{worker_id}/transform-route/...``.
  2. Builds a ``cdc_transform`` task (the shape transform-worker's
     ``CDCTransformTask`` expects) and LPUSHes it to
     ``fusion:transforms:normal``.

The original XADD to the ``cdc:*`` stream is kept for metrics/observability
and for any consumer that still reads streams. The bridge is additive.

Route resolution is cached per (source, schema, table) for
``ROUTE_CACHE_TTL`` seconds to avoid hammering the control plane on every
event.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional

import redis

from cdc_worker.event_envelope import CDCEvent

log = logging.getLogger(__name__)

# Redis list the transform-worker BRPOPs from (see transform-worker/worker.py).
DEFAULT_TRANSFORM_QUEUE = "fusion:transforms:normal"
ROUTE_CACHE_TTL = 60  # seconds

class TransformBridge:
    """Bridges CDCEvent → transform-worker ``cdc_transform`` tasks.

    Constructed once per worker. ``publish_event`` is called for every event
    after the stream XADD. Failures are non-fatal: a bridge miss must never
    drop CDC events or crash the source loop — the stream + fallback queue
    remain the source of truth.
    """

    def __init__(
        self,
        control_plane_url: str,
        worker_token: str,
        worker_id: str,
        redis_client: redis.Redis,
        queue_name: str = DEFAULT_TRANSFORM_QUEUE,
        cache_ttl: int = ROUTE_CACHE_TTL,
        fallback=None,
    ) -> None:
        self._base = control_plane_url.rstrip("/")
        self._headers = {
            "X-Worker-Token": worker_token,
            "X-Worker-ID": worker_id,
        }
        self._worker_id = worker_id
        self._redis = redis_client
        self._queue = queue_name
        self._cache_ttl = cache_ttl
        # (source_id, schema, table) -> (expires_at, routes)
        self._route_cache: Dict[tuple, tuple[float, List[dict]]] = {}
        # 2026-07-25 fix (Bug #21): a FallbackQueue instance (the same one
        # the worker already builds for RedisStreamPublisher) so an LPUSH
        # failure here durably persists the task instead of just logging and
        # dropping it -- see fallback_queue.py's enqueue_bridge_task/
        # drain_bridge_tasks for the full rationale.
        self._fallback = fallback

    # ------------------------------------------------------------------
    def _resolve_routes(self, source_id: str, schema: str, table: str) -> List[dict]:
        key = (source_id, schema, table)
        now = time.time()
        cached = self._route_cache.get(key)
        if cached and cached[0] > now:
            return cached[1]
        routes = self._fetch_routes(source_id, schema, table)
        self._route_cache[key] = (now + self._cache_ttl, routes)
        return routes

    def _fetch_routes(self, source_id: str, schema: str, table: str) -> List[dict]:
        try:
            import httpx
            url = (
                f"{self._base}/api/v1/internal/workers/{self._worker_id}"
                f"/transform-route/{source_id}/{schema}/{table}"
            )
            with httpx.Client(timeout=5.0) as client:
                resp = client.get(url, headers=self._headers)
                if resp.status_code == 404:
                    return []
                resp.raise_for_status()
                return resp.json() or []
        except Exception as exc:
            log.debug("transform-route fetch failed for %s/%s/%s: %s",
                      source_id, schema, table, exc)
            return []

    # ------------------------------------------------------------------
    def publish_event(self, event: CDCEvent) -> int:
        """Bridge one CDC event into transform-worker tasks.

        Returns the number of tasks LPUSHed (0 if no routes resolved or on
        error). Never raises — bridging is best-effort.
        """
        try:
            routes = self._resolve_routes(event.source_id, event.schema_name, event.table_name)
        except Exception as exc:
            log.debug("transform-route resolve error: %s", exc)
            return 0
        if not routes:
            return 0

        # CDCTransformTask (transform-worker/loader.py) expects per-event dicts:
        #   {"op": "INSERT"|"UPDATE"|"DELETE", "before": {...}|None, "after": {...}|None}
        op_map = {"c": "INSERT", "u": "UPDATE", "d": "DELETE"}
        evt = {
            "op": op_map.get(event.op, event.op.upper()),
            "before": event.before,
            "after": event.after,
        }
        pushed = 0
        for route in routes:
            task = {
                "type": "cdc_transform",
                "connection_id": route.get("connection_id"),
                "events": [evt],
                "transform_steps": route.get("transform_steps") or [],
                "dest_schema": route.get("dest_schema") or "dw",
                "dest_table": route.get("dest_table") or event.table_name,
                "primary_key": route.get("primary_key") or "id",
                "destination": route.get("destination") or {},
                "dest_connector_type": (route.get("destination") or {}).get("connector_type", "postgres"),
            }
            try:
                self._redis.lpush(self._queue, json.dumps(task))
                pushed += 1
            except Exception as exc:
                # 2026-07-25 fix (Bug #21): this used to just log and move on,
                # permanently dropping the task. Confirmed live: a ~5-minute
                # Redis outage (OOMKilled pod, no PVC) silently lost every
                # single CDC-driven change for that window with zero
                # recovery -- the sibling XADD path's fallback_events table
                # only replays into the cdc:* stream, never back into
                # fusion:transforms:*, so it did nothing for the actual
                # Iceberg-writing pipeline. Persist to the SAME durable
                # SQLite fallback the worker already has (independent table,
                # see fallback_queue.py) so periodic drain can safely LPUSH
                # it once Redis is reachable again, instead of losing it.
                if self._fallback is not None:
                    try:
                        self._fallback.enqueue_bridge_task(self._queue, task)
                        log.warning("transform bridge LPUSH failed, queued to durable "
                                    "fallback instead of dropping: %s", exc)
                    except Exception:
                        log.exception("transform bridge LPUSH failed AND fallback "
                                      "enqueue failed -- task genuinely lost")
                else:
                    log.warning("transform bridge LPUSH failed (no fallback configured "
                                "-- task lost): %s", exc)
        return pushed

    def flush_cache(self) -> None:
        self._route_cache.clear()
