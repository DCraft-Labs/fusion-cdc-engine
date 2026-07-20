"""
P2.13 — Worker orchestration unit tests.

Tests:
  1. test_worker_start_stop
  2. test_semaphore_limits_concurrency
  3. test_events_routed_to_publisher
  4. test_checkpoint_updated_per_event
  5. test_routing_miss_still_routes_fallback
  6. test_connector_exception_isolated
  7. test_fallback_drain_on_tick
  8. test_checkpoint_sync_on_tick
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import AsyncIterator, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from cdc_worker.event_envelope import CDCEvent, build_event
from cdc_worker.worker import Worker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_event(schema: str = "public", table: str = "orders", op: str = "c") -> CDCEvent:
    return build_event(
        op=op,
        source_id="src-001",
        bank_id="bank-001",
        tenant_id="tenant-001",
        schema_name=schema,
        table_name=table,
        lsn="0/1000",
        ts_ms=1_700_000_000_000,
        pk_values={"id": 1},
        after={"id": 1, "name": "Alice"},
    )


class _MockConnector:
    """Async generator that yields a fixed list of events, then stops."""

    def __init__(self, events: List[CDCEvent]) -> None:
        self._events = events

    async def stream_events(self) -> AsyncIterator[CDCEvent]:
        for ev in self._events:
            yield ev


class _InfiniteConnector:
    """Yields events indefinitely until cancelled."""

    async def stream_events(self) -> AsyncIterator[CDCEvent]:
        while True:
            yield _make_event()
            await asyncio.sleep(0)


def _make_worker(**kwargs) -> Worker:
    """Build a Worker with mocked config and injected fakes."""
    cfg = MagicMock()
    cfg.CONTROL_PLANE_URL = "http://cp:8000"
    cfg.WORKER_TOKEN = "test-token"
    cfg.WORKER_ID = "worker-001"
    cfg.REDIS_URL = "redis://localhost:6379/0"
    cfg.REDIS_STREAM_MAXLEN = 100_000
    cfg.CHECKPOINT_DB_PATH = ":memory:"
    cfg.FALLBACK_DB_PATH = ":memory:"
    cfg.MAX_CONCURRENT_TABLES = kwargs.get("max_concurrent", 20)
    cfg.HEARTBEAT_INTERVAL = 30
    cfg.CHECKPOINT_SYNC_INTERVAL = 300
    cfg.FALLBACK_DRAIN_INTERVAL = 5

    local_ckpt = MagicMock()
    local_ckpt.get = MagicMock(return_value=None)
    local_ckpt.set = MagicMock()

    fallback = MagicMock()
    fallback.drain = MagicMock(return_value=0)

    publisher = MagicMock()
    publisher.publish = MagicMock()

    worker = Worker(
        config=cfg,
        local_checkpoint=local_ckpt,
        fallback_queue=fallback,
        publisher=publisher,
    )
    # Patch HeartbeatSender so it doesn't try real HTTP
    worker._heartbeat = MagicMock()
    worker._heartbeat.start = MagicMock()
    worker._heartbeat.stop = AsyncMock()
    # Patch routing table refresh
    worker._routing_table.fetch_and_reload = AsyncMock(return_value=True)
    worker._routing_table.start_auto_refresh = MagicMock()
    # Patch central sync
    worker._central_sync.push = AsyncMock()
    return worker


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestWorkerStartStop:
    async def test_worker_start_stop(self):
        """Worker starts, fetches 0 sources, and stops cleanly."""
        worker = _make_worker()
        worker._fetch_sources = AsyncMock(return_value=[])

        task = asyncio.ensure_future(worker.run())
        await asyncio.sleep(0)  # let run() kick off
        await worker.stop()
        await asyncio.wait_for(task, timeout=2.0)

        worker._heartbeat.start.assert_called_once()


class TestSemaphore:
    async def test_semaphore_limits_concurrency(self):
        """Only MAX_CONCURRENT_TABLES sources run at the same time."""
        max_concurrent = 2
        worker = _make_worker(max_concurrent=max_concurrent)

        enter_count = 0
        peak = 0

        async def fake_stream(source):
            nonlocal enter_count, peak
            enter_count += 1
            peak = max(peak, enter_count)
            await asyncio.sleep(0.05)
            enter_count -= 1

        worker._stream_source = AsyncMock(side_effect=fake_stream)
        worker._fetch_sources = AsyncMock(
            return_value=[{"source_id": f"s{i}", "connector_type": "mysql"} for i in range(4)]
        )
        worker._build_connector = MagicMock(return_value=_MockConnector([]))

        task = asyncio.ensure_future(worker.run())
        await asyncio.sleep(0.2)
        await worker.stop()
        try:
            await asyncio.wait_for(task, timeout=1.0)
        except asyncio.TimeoutError:
            pass

        assert peak <= max_concurrent


class TestEventRouting:
    async def test_events_routed_to_publisher(self):
        """Every event yielded by connector is published."""
        worker = _make_worker()
        events = [_make_event(table=f"t{i}") for i in range(3)]
        connector = _MockConnector(events)

        worker._running = True
        await worker._stream_source("src-001", connector)

        assert worker._publisher.publish.call_count == 3

    async def test_routing_miss_still_routes_fallback(self):
        """When routing table has no entry, falls back to self-routing (source_id)."""
        worker = _make_worker()
        # Empty routing table — lookup returns []
        worker._routing_table._table = {}
        ev = _make_event()
        connector = _MockConnector([ev])

        worker._running = True
        await worker._stream_source("src-001", connector)

        # Should still publish via fallback routing
        assert worker._publisher.publish.call_count == 1
        call_kwargs = worker._publisher.publish.call_args
        routing = call_kwargs[1].get("routing") or call_kwargs[0][1]
        assert routing[0]["source_id"] == "src-001"


class TestCheckpointing:
    async def test_checkpoint_updated_per_event(self):
        """Local checkpoint set is called once per event processed."""
        worker = _make_worker()
        events = [_make_event(table=f"t{i}", op="u") for i in range(5)]
        connector = _MockConnector(events)

        worker._running = True
        await worker._stream_source("src-001", connector)

        assert worker._local_ckpt.set.call_count == 5


class TestConnectorIsolation:
    async def test_connector_exception_isolated(self):
        """Exception in one source does not affect others."""
        worker = _make_worker()

        class _CrashingConnector:
            async def stream_events(self):
                raise RuntimeError("boom")
                yield  # make it a generator

        source = {"source_id": "crash-src", "connector_type": "mysql"}
        worker._build_connector = MagicMock(return_value=_CrashingConnector())

        # _run_source should complete without raising
        await worker._run_source(source)  # must not raise


class TestBackgroundLoops:
    async def test_fallback_drain_on_tick(self):
        """Fallback drain loop calls drain() at least once per interval."""
        worker = _make_worker()
        worker._cfg.FALLBACK_DRAIN_INTERVAL = 0.05  # fast for tests
        worker._running = True

        drain_task = asyncio.ensure_future(worker._fallback_drain_loop())
        await asyncio.sleep(0.15)
        worker._running = False
        drain_task.cancel()
        try:
            await drain_task
        except asyncio.CancelledError:
            pass

        assert worker._fallback.drain.call_count >= 2

    async def test_checkpoint_sync_on_tick(self):
        """Checkpoint sync loop calls push() for each active source."""
        worker = _make_worker()
        worker._cfg.CHECKPOINT_SYNC_INTERVAL = 0.05
        worker._running = True
        # Pretend two sources are running
        worker._source_tasks = {"src-1": MagicMock(), "src-2": MagicMock()}

        sync_task = asyncio.ensure_future(worker._checkpoint_sync_loop())
        await asyncio.sleep(0.15)
        worker._running = False
        sync_task.cancel()
        try:
            await sync_task
        except asyncio.CancelledError:
            pass

        assert worker._central_sync.push.call_count >= 2
