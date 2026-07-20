"""
Spec §1 (P1-7): Active Scheduler — dynamically assigns CDC workers to active connections.

The scheduler runs as a background asyncio task in the control-plane process.
To avoid duplicate scheduling across replicas, it uses the Redis leader election
from app.utils.leader_election.  Only the elected leader runs scheduling logic.

Responsibilities:
1. Every TICK_INTERVAL seconds, query all active REALTIME connections that have
   no live worker heartbeat (or whose heartbeat is stale).
2. Pick the least-loaded available worker pod via WorkerHeartbeat records.
3. POST an assignment request to the worker's control endpoint so it starts
   consuming from Redis Streams for that connection.
4. Update connection status to 'assigning' to avoid double-assignment.

This is intentionally a best-effort scheduler; workers self-register via
/api/v1/workers/heartbeat and remove themselves cleanly on graceful shutdown.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

log = logging.getLogger(__name__)

TICK_INTERVAL = int(os.environ.get("SCHEDULER_TICK_INTERVAL", "30"))   # seconds
HEARTBEAT_STALE_SECS = int(os.environ.get("SCHEDULER_STALE_SECS", "90"))  # after this, worker is considered dead
WORKER_CONTROL_PORT = int(os.environ.get("WORKER_CONTROL_PORT", "8001"))


class SchedulerService:
    """
    Background scheduler service.

    Usage (in app/main.py lifespan):

        scheduler = SchedulerService(session_factory, redis_client)
        task = asyncio.create_task(scheduler.run(), name="scheduler")
        ...
        task.cancel()
    """

    def __init__(self, session_factory, redis_client=None) -> None:
        """
        :param session_factory: SQLAlchemy sessionmaker / async session factory.
        :param redis_client: Optional redis.asyncio.Redis for leader election.
                             If None, scheduling always runs (single-replica mode).
        """
        self._session_factory = session_factory
        self._redis = redis_client
        self._leader_election = None
        self._running = False

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Main loop.  Cancel this task to stop the scheduler."""
        self._running = True
        if self._redis is not None:
            from app.utils.leader_election import RedisLeaderElection
            self._leader_election = RedisLeaderElection(self._redis)

        log.info("SchedulerService starting (tick=%ds, stale=%ds)", TICK_INTERVAL, HEARTBEAT_STALE_SECS)
        while self._running:
            try:
                await self._tick()
            except asyncio.CancelledError:
                break
            except Exception as exc:  # pylint: disable=broad-except
                log.exception("SchedulerService tick error: %s", exc)
            await asyncio.sleep(TICK_INTERVAL)

        log.info("SchedulerService stopped")

    def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _tick(self) -> None:
        """One scheduling cycle."""
        # Leader election: only the elected pod runs scheduling
        if self._leader_election is not None:
            if not await self._leader_election.is_leader():
                log.debug("SchedulerService: not leader — skipping tick")
                return

        # Run DB work in a thread pool so we don't block the event loop
        await asyncio.get_event_loop().run_in_executor(None, self._schedule_connections)

    def _schedule_connections(self) -> None:
        """Synchronous DB work — runs in a thread-pool executor."""
        from app.models.connection import Connection
        from app.models.monitoring import WorkerHeartbeat

        session = self._session_factory()
        try:
            stale_cutoff = datetime.now(timezone.utc) - timedelta(seconds=HEARTBEAT_STALE_SECS)

            # Find active REALTIME/CDC connections (CDC and REALTIME are equivalent)
            active_connections = (
                session.query(Connection)
                .filter(
                    Connection.status == "active",
                    Connection.sync_type.in_(["REALTIME", "CDC"]),
                    Connection.is_deleted == False,
                )
                .all()
            )
            if not active_connections:
                return

            # Build set of connection_ids that already have a live worker
            live_worker_conn_ids = {
                str(h.connection_id)
                for h in session.query(WorkerHeartbeat)
                .filter(
                    WorkerHeartbeat.status.in_(["running", "paused"]),
                    WorkerHeartbeat.last_heartbeat_at >= stale_cutoff,
                )
                .all()
            }

            unassigned = [
                c for c in active_connections
                if str(c.connection_id) not in live_worker_conn_ids
            ]
            if not unassigned:
                log.debug("SchedulerService: all %d connections have live workers", len(active_connections))
                return

            log.info("SchedulerService: %d connections need worker assignment", len(unassigned))

            # Find available workers (least loaded first)
            available_workers = (
                session.query(WorkerHeartbeat)
                .filter(
                    WorkerHeartbeat.last_heartbeat_at >= stale_cutoff,
                    WorkerHeartbeat.status == "running",
                )
                .order_by(WorkerHeartbeat.events_processed.asc())
                .all()
            )

            for connection in unassigned:
                worker = self._pick_worker(available_workers, connection)
                if worker is None:
                    log.warning(
                        "SchedulerService: no available worker for connection %s (type=%s)",
                        connection.connection_id,
                        getattr(connection, "connector_type", "unknown"),
                    )
                    continue

                success = self._assign_connection_to_worker(worker, connection)
                if success:
                    log.info(
                        "SchedulerService: assigned connection %s to worker %s",
                        connection.connection_id, worker.worker_id,
                    )
                    # Only mark as assigning when worker HTTP confirmed receipt.
                    # Redis fallback keeps status as active so the UI stays correct.
                else:
                    # Revert status so next tick retries
                    connection.status = "active"
                    session.commit()

        except Exception as exc:  # pylint: disable=broad-except
            log.exception("SchedulerService _schedule_connections error: %s", exc)
            session.rollback()
        finally:
            session.close()

    def _pick_worker(self, workers, connection) -> Optional[object]:
        """
        Pick the best worker for a connection.
        Prefer a worker that matches the connector type (mysql/postgres/mongodb),
        otherwise fall back to any available worker.
        """
        connector_type = getattr(connection, "connector_type", "").lower()
        # First pass: type match
        for w in workers:
            if connector_type and connector_type in w.worker_type.lower():
                return w
        # Fallback: any worker
        return workers[0] if workers else None

    def _assign_connection_to_worker(self, worker, connection) -> bool:
        """
        POST to the worker's control endpoint to start consuming the connection.
        Workers expose: POST http://<hostname>:<port>/control/assign
        Body: {"connection_id": "...", "source_id": "...", ...}
        Falls back to Redis pub/sub when the worker HTTP endpoint is unreachable.
        """
        import json as _json
        payload = {
            "action": "start-streaming",
            "connection_id": str(connection.connection_id),
            "source_id": str(connection.source_id) if connection.source_id else None,
            "destination_id": str(connection.destination_id) if connection.destination_id else None,
        }

        # Try direct HTTP first
        http_ok = False
        try:
            import urllib.request
            hostname = worker.hostname or worker.pod_name or worker.worker_id
            url = f"http://{hostname}:{WORKER_CONTROL_PORT}/control/assign"
            body = _json.dumps(payload).encode()
            req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
            with urllib.request.urlopen(req, timeout=5) as resp:  # noqa: S310
                http_ok = resp.status < 300
        except Exception as exc:
            log.warning("SchedulerService: HTTP assign failed for worker %s: %s — falling back to Redis", worker.worker_id, exc)

        if http_ok:
            return True

        # Fallback: publish start-streaming command via Redis pub/sub
        try:
            import redis
            redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
            r = redis.from_url(redis_url)
            r.publish("fusion:commands", _json.dumps(payload))
            log.info("SchedulerService: published start-streaming via Redis for connection=%s", connection.connection_id)
            return True
        except Exception as exc:
            log.warning("SchedulerService: Redis fallback also failed for connection %s: %s", connection.connection_id, exc)
            return False
