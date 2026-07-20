"""
P2.7 — Async Worker main loop.

Responsibilities:
1. Load assigned sources from the control-plane API
2. Load routing table (schema.table → [tenant assignments])
3. Semaphore-gated connector coroutine startup
4. Per-source exception isolation
5. Background loops:
   - checkpoint_sync_loop  (every CHECKPOINT_SYNC_INTERVAL seconds)
   - fallback_drain_loop   (every FALLBACK_DRAIN_INTERVAL seconds)
   - heartbeat             (HeartbeatSender)
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Dict, List, Optional

from cdc_worker.checkpoint import LocalCheckpointManager, CentralCheckpointSync
from cdc_worker.config import settings
from cdc_worker.fallback_queue import FallbackQueue
from cdc_worker.heartbeat import HeartbeatSender
from cdc_worker.metrics import METRICS, start_metrics_server
from cdc_worker.redis_publisher import RedisStreamPublisher
from cdc_worker.routing import RoutingTable

log = logging.getLogger(__name__)

# Redis pub/sub channel for control-plane → worker commands
COMMAND_CHANNEL = "fusion:commands"


class Worker:
    """
    Main CDC Worker.

    Usage::

        worker = Worker()
        await worker.run()

    Or for controlled teardown::

        worker = Worker()
        task = asyncio.ensure_future(worker.run())
        # ...
        await worker.stop()
    """

    def __init__(
        self,
        config=None,
        local_checkpoint: Optional[LocalCheckpointManager] = None,
        fallback_queue: Optional[FallbackQueue] = None,
        publisher: Optional[RedisStreamPublisher] = None,
    ) -> None:
        self._cfg = config or settings
        self._local_ckpt = local_checkpoint or LocalCheckpointManager(self._cfg.CHECKPOINT_DB_PATH)
        self._fallback = fallback_queue or FallbackQueue(self._cfg.FALLBACK_DB_PATH)
        self._publisher = publisher or RedisStreamPublisher(
            redis_url=self._cfg.REDIS_URL,
            maxlen=self._cfg.REDIS_STREAM_MAXLEN,
            fallback=self._fallback,
        )
        self._semaphore = asyncio.Semaphore(self._cfg.MAX_CONCURRENT_TABLES)
        self._heartbeat = HeartbeatSender(
            control_plane_url=self._cfg.CONTROL_PLANE_URL,
            worker_token=self._cfg.WORKER_TOKEN,
            worker_id=self._cfg.WORKER_ID,
            interval=self._cfg.HEARTBEAT_INTERVAL,
        )
        self._central_sync = CentralCheckpointSync(
            control_plane_url=self._cfg.CONTROL_PLANE_URL,
            worker_token=self._cfg.WORKER_TOKEN,
            worker_id=self._cfg.WORKER_ID,
        )
        self._routing_table = RoutingTable(
            control_plane_url=self._cfg.CONTROL_PLANE_URL,
            worker_token=self._cfg.WORKER_TOKEN,
            worker_id=self._cfg.WORKER_ID,
        )
        self._running = False
        self._source_tasks: Dict[str, asyncio.Task] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Start the worker and block until stop() is called."""
        self._running = True
        log.info("Worker %s starting", self._cfg.WORKER_ID)

        self._heartbeat.start()

        # Seed the routing table from control plane; errors are non-fatal
        await self._routing_table.fetch_and_reload()
        self._routing_table.start_auto_refresh()

        sources = await self._fetch_sources()
        log.info("Loaded %d assigned source(s)", len(sources))

        # Launch one coroutine per source (isolated)
        for source in sources:
            task = asyncio.ensure_future(self._run_source(source))
            self._source_tasks[source.get("source_id", str(id(source)))] = task

        # Background loops
        sync_task = asyncio.ensure_future(self._checkpoint_sync_loop())
        drain_task = asyncio.ensure_future(self._fallback_drain_loop())

        # Redis pub/sub command listener (receives start-streaming / stop-streaming)
        cmd_task = asyncio.ensure_future(self._command_listener())

        # Lightweight HTTP server for /internal/start-streaming POST from control plane
        http_task = asyncio.ensure_future(self._start_http_server())

        # Wait until all tasks complete (or stop() cancels them)
        all_tasks = list(self._source_tasks.values()) + [sync_task, drain_task, cmd_task, http_task]
        try:
            await asyncio.gather(*all_tasks, return_exceptions=True)
        finally:
            await self._heartbeat.stop()
            log.info("Worker %s stopped", self._cfg.WORKER_ID)

    async def stop(self) -> None:
        """Signal graceful shutdown."""
        self._running = False
        for task in self._source_tasks.values():
            task.cancel()
        await self._heartbeat.stop()

    # ------------------------------------------------------------------
    # Source runner (exception-isolated)
    # ------------------------------------------------------------------

    async def _run_source(self, source: dict) -> None:
        source_id = source.get("source_id", "unknown")
        try:
            async with self._semaphore:
                connector = self._build_connector(source)
                await self._stream_source(source_id, connector)
        except asyncio.CancelledError:
            log.info("Source %s cancelled", source_id)
        except Exception as exc:
            log.error("Source %s crashed (isolated): %s", source_id, exc, exc_info=True)

    async def _stream_source(self, source_id: str, connector) -> None:
        """Iterate connector events, publish them, and update checkpoints."""
        async for event in connector.stream_events():
            if not self._running:
                break
            routing = await self._get_routing(source_id, event.schema_name, event.table_name)
            if routing:
                self._publisher.publish(event, routing=routing)
                # Record metrics for the event
                METRICS.record_event(
                    tenant=event.tenant_id,
                    source=event.source_id,
                    schema=event.schema_name,
                    table=event.table_name,
                    op=event.op,
                    ts_ms=event.ts_ms,
                )
            else:
                log.debug("No routing for %s.%s — skipping", event.schema_name, event.table_name)
            self._local_ckpt.set(source_id, event.schema_name, event.table_name, event.lsn)

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------

    async def _checkpoint_sync_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._cfg.CHECKPOINT_SYNC_INTERVAL)
            for source_id in list(self._source_tasks.keys()):
                try:
                    await self._central_sync.push(self._local_ckpt, source_id)
                except Exception as exc:
                    log.warning("checkpoint sync failed for %s: %s", source_id, exc)

    async def _fallback_drain_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._cfg.FALLBACK_DRAIN_INTERVAL)
            try:
                flushed = self._fallback.drain(self._publisher)
                if flushed:
                    log.info("Fallback drained %d event(s)", flushed)
            except Exception as exc:
                log.warning("fallback drain error: %s", exc)

    # ------------------------------------------------------------------
    # Redis pub/sub command listener
    # ------------------------------------------------------------------

    async def _command_listener(self) -> None:
        """
        Subscribe to the Redis `fusion:commands` channel.
        The control plane publishes JSON commands like:
          {"action": "start-streaming", "connection_id": "..."}
          {"action": "stop-streaming", "connection_id": "..."}
        """
        import redis.asyncio as aioredis

        try:
            r = aioredis.from_url(self._cfg.REDIS_URL, decode_responses=True)
            pubsub = r.pubsub()
            await pubsub.subscribe(COMMAND_CHANNEL)
            log.info("Subscribed to Redis channel %s", COMMAND_CHANNEL)

            async for message in pubsub.listen():
                if not self._running:
                    break
                if message["type"] != "message":
                    continue
                try:
                    cmd = json.loads(message["data"])
                    action = cmd.get("action", "")
                    connection_id = cmd.get("connection_id", "")
                    log.info("Received command: action=%s connection_id=%s", action, connection_id)

                    if action == "start-streaming":
                        await self._handle_start_streaming(connection_id)
                    elif action == "stop-streaming":
                        await self._handle_stop_streaming(connection_id)
                    else:
                        log.warning("Unknown command action: %s", action)
                except json.JSONDecodeError:
                    log.warning("Invalid JSON on command channel: %s", message["data"])
                except Exception as exc:
                    log.error("Error handling command: %s", exc, exc_info=True)

            await pubsub.unsubscribe(COMMAND_CHANNEL)
            await r.aclose()
        except Exception as exc:
            log.error("Command listener failed: %s", exc, exc_info=True)

    # ------------------------------------------------------------------
    # HTTP server for /internal/start-streaming POST
    # ------------------------------------------------------------------

    async def _start_http_server(self) -> None:
        """
        Lightweight HTTP server on port 8081 (configurable) so the control plane
        can POST /internal/start-streaming with {"connection_id": "..."}.
        """
        from aiohttp import web

        async def handle_start_streaming(request: web.Request) -> web.Response:
            try:
                body = await request.json()
                connection_id = body.get("connection_id", "")
                if not connection_id:
                    return web.json_response({"error": "connection_id required"}, status=400)
                await self._handle_start_streaming(connection_id)
                return web.json_response({"ok": True, "connection_id": connection_id})
            except Exception as exc:
                log.error("HTTP start-streaming error: %s", exc, exc_info=True)
                return web.json_response({"error": str(exc)}, status=500)

        async def handle_health(request: web.Request) -> web.Response:
            return web.json_response({
                "status": "healthy",
                "worker_id": self._cfg.WORKER_ID,
                "active_sources": list(self._source_tasks.keys()),
            })

        app = web.Application()
        app.router.add_post("/internal/start-streaming", handle_start_streaming)
        app.router.add_get("/health", handle_health)

        port = int(getattr(self._cfg, "HTTP_PORT", 8081))
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        try:
            await site.start()
            log.info("Worker HTTP server listening on port %d", port)
            # Keep running until worker stops
            while self._running:
                await asyncio.sleep(1)
        finally:
            await runner.cleanup()

    # ------------------------------------------------------------------
    # Dynamic source start/stop handlers
    # ------------------------------------------------------------------

    async def _handle_start_streaming(self, connection_id: str) -> None:
        """
        Handle a start-streaming command: re-fetch sources from the control plane
        and start any new sources not already running.
        """
        log.info("Handling start-streaming for connection_id=%s", connection_id)
        sources = await self._fetch_sources()
        for source in sources:
            source_id = source.get("source_id", "")
            if source_id and source_id not in self._source_tasks:
                log.info("Starting new source %s (triggered by connection %s)", source_id, connection_id)
                task = asyncio.ensure_future(self._run_source(source))
                self._source_tasks[source_id] = task
            elif source_id in self._source_tasks:
                log.info("Source %s already running", source_id)

    async def _handle_stop_streaming(self, connection_id: str) -> None:
        """
        Handle a stop-streaming command: cancel the source task if running.
        In the current model, connection_id maps to a source via the control plane.
        For now, we re-fetch sources and cancel any that are no longer assigned.
        """
        log.info("Handling stop-streaming for connection_id=%s", connection_id)
        sources = await self._fetch_sources()
        active_source_ids = {s.get("source_id") for s in sources}
        for source_id in list(self._source_tasks.keys()):
            if source_id not in active_source_ids:
                log.info("Stopping source %s (no longer assigned)", source_id)
                self._source_tasks[source_id].cancel()
                del self._source_tasks[source_id]

    # ------------------------------------------------------------------
    # Helpers (overridable in tests)
    # ------------------------------------------------------------------

    async def _fetch_sources(self) -> List[dict]:
        """Fetch assigned sources from the control plane.  Returns [] on error."""
        try:
            import httpx
            headers = {
                "X-Worker-Token": self._cfg.WORKER_TOKEN,
                "X-Worker-ID": self._cfg.WORKER_ID,
            }
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self._cfg.CONTROL_PLANE_URL}/api/v1/internal/workers/{self._cfg.WORKER_ID}/sources",
                    headers=headers,
                    timeout=10.0,
                )
                resp.raise_for_status()
                return resp.json()
        except Exception as exc:
            log.error("Could not fetch sources from control plane: %s", exc)
            return []

    async def _get_routing(self, source_id: str, schema_name: str, table_name: str) -> List[dict]:
        """Return routing targets for a (schema, table) from the RoutingTable."""
        entries = self._routing_table.lookup(schema_name, table_name)
        if entries:
            return entries
        # Fall back to self-routing (single-tenant / unconfigured case)
        return [{"source_id": source_id}]

    def _build_connector(self, source: dict):
        """Instantiate the right connector for a source config dict."""
        connector_type = source.get("connector_type", "").lower()
        if connector_type == "mysql":
            from connectors.mysql import MySQLConnector
            return MySQLConnector(source, self._local_ckpt)
        elif connector_type in ("postgresql", "postgres"):
            from connectors.postgres import PostgresConnector
            return PostgresConnector(source, self._local_ckpt)
        elif connector_type == "mongodb":
            from connectors.mongodb import MongoDBConnector
            return MongoDBConnector(source, self._local_ckpt)
        elif connector_type == "polling":
            from connectors.polling import PollingConnector
            return PollingConnector(source, self._local_ckpt)
        else:
            raise ValueError(f"Unknown connector_type: {connector_type!r}")


# ---------------------------------------------------------------------------
# CLI entry point — `python -m cdc_worker.worker`
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import signal

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    # Start metrics server (Prometheus)
    start_metrics_server()

    worker = Worker()

    def _shutdown(sig, frame):
        log.info("Received signal %s — initiating graceful shutdown", sig)
        asyncio.get_event_loop().create_task(worker.stop())

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    asyncio.run(worker.run())
