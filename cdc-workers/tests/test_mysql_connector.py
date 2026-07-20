"""
MySQL Connector unit tests (mock-based — no Docker required).

These tests exercise the connector logic by replacing
BinLogStreamReader with controlled fakes.

Tests:
  1. test_server_id_from_uuid_is_deterministic
  2. test_server_id_fits_in_32_bits
  3. test_slot_name_from_source_id
  4. test_write_event_mapped_to_c
  5. test_update_event_mapped_to_u
  6. test_delete_event_mapped_to_d
  7. test_assigned_table_filter_applied
  8. test_resume_position_from_checkpoint
  9. test_only_schemas_tables_filters_set_on_reader
 10. test_connector_stops_when_running_false
"""

from __future__ import annotations

import asyncio
from typing import AsyncIterator, List
from unittest.mock import MagicMock, patch, AsyncMock
import pytest

from connectors.mysql import MySQLConnector, _server_id_from_uuid


async def _collect_gen(gen, max_items: int = 100) -> list:
    """Drain an async generator into a list (up to max_items)."""
    items = []
    async for ev in gen:
        items.append(ev)
        if len(items) >= max_items:
            break
    return items


# ---------------------------------------------------------------------------
# Unit helpers
# ---------------------------------------------------------------------------

def _make_source(**overrides) -> dict:
    base = {
        "source_id": "aaaabbbb-cccc-dddd-eeee-000011112222",
        "bank_id": "bank-001",
        "tenant_id": "tenant-001",
        "host": "127.0.0.1",
        "port": 3306,
        "database_name": "testdb",
        "username": "root",
        "password": "root",
        "assigned_tables": [{"schema_name": "testdb", "table_name": "orders"}],
    }
    base.update(overrides)
    return base


def _make_write_row_event(schema: str, table: str, row_values: dict) -> MagicMock:
    from pymysqlreplication.row_event import WriteRowsEvent
    ev = MagicMock(spec=WriteRowsEvent)
    ev.schema = schema
    ev.table = table
    ev.timestamp = 1_700_000_000
    ev.rows = [{"values": row_values}]
    ev.table_map = {}
    return ev


def _make_update_row_event(schema: str, table: str, before: dict, after: dict) -> MagicMock:
    from pymysqlreplication.row_event import UpdateRowsEvent
    ev = MagicMock(spec=UpdateRowsEvent)
    ev.schema = schema
    ev.table = table
    ev.timestamp = 1_700_000_000
    ev.rows = [{"before_values": before, "after_values": after}]
    ev.table_map = {}
    return ev


def _make_delete_row_event(schema: str, table: str, row_values: dict) -> MagicMock:
    from pymysqlreplication.row_event import DeleteRowsEvent
    ev = MagicMock(spec=DeleteRowsEvent)
    ev.schema = schema
    ev.table = table
    ev.timestamp = 1_700_000_000
    ev.rows = [{"values": row_values}]
    ev.table_map = {}
    return ev


class _FakeBinLogStream:
    """
    Replaces BinLogStreamReader — yields fake events then stops.
    Stores kwargs so tests can inspect what was passed to the reader.
    Calls connector._running = False after exhausting events so the
    while loop terminates naturally (avoids Python 3.9 aclose() hang).
    """

    last_kwargs: dict = {}

    def __init__(self, events: List, **kwargs):
        self._events = list(events)
        self._idx = 0
        _FakeBinLogStream.last_kwargs = kwargs
        self.log_file = "mysql-bin.000001"
        self.log_pos = 1234

    def __iter__(self):
        return self

    def __next__(self):
        if self._idx < len(self._events):
            ev = self._events[self._idx]
            self._idx += 1
            return ev
        # Signal to the connector that we're done so while loop exits
        raise StopIteration

    def close(self):
        pass


# ---------------------------------------------------------------------------
# Pure-unit tests (no async)
# ---------------------------------------------------------------------------

class TestServerIdFromUuid:
    def test_server_id_from_uuid_is_deterministic(self):
        sid = _server_id_from_uuid("aaaabbbb-cccc-dddd-eeee-000011112222")
        assert sid == _server_id_from_uuid("aaaabbbb-cccc-dddd-eeee-000011112222")

    def test_server_id_fits_in_32_bits(self):
        sid = _server_id_from_uuid("aaaabbbb-cccc-dddd-eeee-000011112222")
        assert 0 <= sid < 2 ** 32

    def test_different_uuids_yield_different_server_ids(self):
        sid1 = _server_id_from_uuid("aaaabbbb-cccc-dddd-eeee-000011112222")
        sid2 = _server_id_from_uuid("11112222-3333-4444-5555-666677778888")
        # Not guaranteed unique but overwhelmingly likely for different inputs
        assert sid1 != sid2


# ---------------------------------------------------------------------------
# Integration-style tests (still mocked — no real MySQL)
# ---------------------------------------------------------------------------

class TestMySQLConnectorEventMapping:
    async def _run_connector_once(self, source, events) -> list:
        """Helper that creates a connector with fake stream and collects events."""
        ckpt = MagicMock()
        ckpt.get = MagicMock(return_value=None)
        connector = MySQLConnector(source, ckpt)

        collected = []

        def fake_stream_reader(**kwargs):
            reader = _FakeBinLogStream(events, **kwargs)
            return reader

        async def _collect():
            async for ev in connector.stream_events():
                collected.append(ev)
                if len(collected) >= len(events):
                    # Stop the connector so the while loop terminates
                    connector._running = False

        with patch("pymysqlreplication.BinLogStreamReader", side_effect=fake_stream_reader):
            try:
                await asyncio.wait_for(_collect(), timeout=3.0)
            except asyncio.TimeoutError:
                connector._running = False
            except Exception:
                pass

        return collected

    async def test_write_event_mapped_to_c(self):
        source = _make_source()
        fake_event = _make_write_row_event("testdb", "orders", {"id": 1, "name": "Alice"})
        collected = await self._run_connector_once(source, [fake_event])
        assert len(collected) == 1
        assert collected[0].op == "c"
        assert collected[0].after == {"id": 1, "name": "Alice"}

    async def test_update_event_mapped_to_u(self):
        source = _make_source()
        fake_event = _make_update_row_event(
            "testdb", "orders",
            before={"id": 1, "name": "Alice"},
            after={"id": 1, "name": "Bob"},
        )
        collected = await self._run_connector_once(source, [fake_event])
        assert len(collected) == 1
        assert collected[0].op == "u"
        assert collected[0].before == {"id": 1, "name": "Alice"}
        assert collected[0].after == {"id": 1, "name": "Bob"}

    async def test_delete_event_mapped_to_d(self):
        source = _make_source()
        fake_event = _make_delete_row_event("testdb", "orders", {"id": 1, "name": "Alice"})
        collected = await self._run_connector_once(source, [fake_event])
        assert len(collected) == 1
        assert collected[0].op == "d"
        assert collected[0].before == {"id": 1, "name": "Alice"}
        assert collected[0].after is None

    async def test_assigned_table_filter_applied(self):
        """Events on non-assigned tables should be skipped."""
        source = _make_source(
            assigned_tables=[{"schema_name": "testdb", "table_name": "orders"}]
        )
        assigned_event = _make_write_row_event("testdb", "orders", {"id": 1})
        skipped_event = _make_write_row_event("testdb", "users", {"id": 2})
        collected = await self._run_connector_once(source, [skipped_event, assigned_event])
        assert len(collected) == 1
        assert collected[0].table_name == "orders"


class TestMySQLConnectorResume:
    async def test_resume_position_from_checkpoint(self):
        """Connector should pass log_file/log_pos to BinLogStreamReader."""
        source = _make_source()
        ckpt = MagicMock()
        # Checkpoint stored as "mysql-bin.000003:999"
        ckpt.get = MagicMock(return_value="mysql-bin.000003:999")
        connector = MySQLConnector(source, ckpt)

        captured_kwargs: dict = {}

        def capture_kwargs(**kwargs):
            captured_kwargs.update(kwargs)
            connector._running = False  # Stop after first call
            return _FakeBinLogStream([], **kwargs)

        with patch("pymysqlreplication.BinLogStreamReader", side_effect=capture_kwargs):
            try:
                await asyncio.wait_for(
                    _collect_gen(connector.stream_events()), timeout=3.0
                )
            except (asyncio.TimeoutError, StopAsyncIteration, Exception):
                pass

        assert captured_kwargs.get("log_file") == "mysql-bin.000003"
        assert captured_kwargs.get("log_pos") == 999

    async def test_only_schemas_tables_filters_set_on_reader(self):
        """When assigned_tables set, only_schemas/only_tables should be passed."""
        source = _make_source(
            assigned_tables=[
                {"schema_name": "db1", "table_name": "orders"},
                {"schema_name": "db1", "table_name": "items"},
            ]
        )
        ckpt = MagicMock()
        ckpt.get = MagicMock(return_value=None)
        connector = MySQLConnector(source, ckpt)

        captured_kwargs: dict = {}

        def capture_kwargs(**kwargs):
            captured_kwargs.update(kwargs)
            connector._running = False  # Stop after first call
            return _FakeBinLogStream([], **kwargs)

        with patch("pymysqlreplication.BinLogStreamReader", side_effect=capture_kwargs):
            try:
                await asyncio.wait_for(
                    _collect_gen(connector.stream_events()), timeout=3.0
                )
            except (asyncio.TimeoutError, StopAsyncIteration, Exception):
                pass

        assert "only_schemas" in captured_kwargs
        assert "only_tables" in captured_kwargs
        assert set(captured_kwargs["only_schemas"]) == {"db1"}
        assert set(captured_kwargs["only_tables"]) == {"orders", "items"}
