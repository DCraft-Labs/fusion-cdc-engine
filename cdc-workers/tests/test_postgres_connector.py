"""
PostgreSQL Connector unit tests (mock-based — no Docker required).

Tests:
  1. test_slot_name_alphanum_only
  2. test_slot_name_max_20_chars
  3. test_slot_name_prefix
  4. test_parse_wal2json_insert
  5. test_parse_wal2json_update
  6. test_parse_wal2json_delete
  7. test_unknown_kind_returns_none
  8. test_feedback_interval_constant_is_10
  9. test_connector_drops_slot_on_close
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator, List
from unittest.mock import AsyncMock, MagicMock, patch, call
import pytest

from connectors.postgres import PostgresConnector, _slot_name, FEEDBACK_INTERVAL


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_source(**overrides) -> dict:
    base = {
        "source_id": "aaaabbbb-cccc-dddd-eeee-000011112222",
        "bank_id": "bank-001",
        "tenant_id": "tenant-001",
        "host": "127.0.0.1",
        "port": 5432,
        "database_name": "testdb",
        "username": "cdc",
        "password": "secret",
        "assigned_tables": [{"schema_name": "public", "table_name": "orders"}],
    }
    base.update(overrides)
    return base


def _wal2json_change(kind: str, schema: str, table: str, columns: dict, oldkeys: dict = None) -> dict:
    """Build a single wal2json change dict (not the outer payload)."""
    change: dict = {
        "kind": kind,  # insert / update / delete
        "schema": schema,
        "table": table,
        "columnnames": list(columns.keys()),
        "columnvalues": list(columns.values()),
    }
    if oldkeys:
        change["oldkeys"] = oldkeys
    return change


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


# ---------------------------------------------------------------------------
# Pure-unit: slot name generation
# ---------------------------------------------------------------------------

class TestSlotName:
    def test_slot_name_prefix(self):
        name = _slot_name("aaaabbbb-cccc-dddd-eeee-000011112222")
        assert name.startswith("fusion_")

    def test_slot_name_alphanum_only(self):
        name = _slot_name("aaaabbbb-cccc-dddd-eeee-000011112222")
        suffix = name[len("fusion_"):]
        assert suffix.isalnum()

    def test_slot_name_max_20_chars_suffix(self):
        name = _slot_name("aaaabbbb-cccc-dddd-eeee-000011112222")
        suffix = name[len("fusion_"):]
        assert len(suffix) <= 20


# ---------------------------------------------------------------------------
# Parsing logic tests
# ---------------------------------------------------------------------------

class TestWal2JsonParsing:
    """Tests the _parse_change static method."""

    def _parse(self, change: dict, source: dict):
        """Directly invoke the connector's _parse_change static method."""
        return PostgresConnector._parse_change(
            change=change,
            lsn="0/1000",
            source_id=source["source_id"],
            bank_id=source["bank_id"],
            tenant_id=source["tenant_id"],
        )

    def test_parse_wal2json_insert(self):
        source = _make_source()
        change = _wal2json_change("insert", "public", "orders", {"id": 1, "name": "Alice"})
        event = self._parse(change, source)
        assert event is not None
        assert event.op == "c"
        assert event.after["id"] == 1
        assert event.schema_name == "public"
        assert event.table_name == "orders"

    def test_parse_wal2json_update(self):
        source = _make_source()
        change = _wal2json_change(
            "update", "public", "orders",
            {"id": 1, "name": "Bob"},
            oldkeys={"keynames": ["id"], "keyvalues": [1]},
        )
        event = self._parse(change, source)
        assert event is not None
        assert event.op == "u"

    def test_parse_wal2json_delete(self):
        source = _make_source()
        change = _wal2json_change(
            "delete", "public", "orders", {},
            oldkeys={"keynames": ["id"], "keyvalues": [1]},
        )
        event = self._parse(change, source)
        assert event is not None
        assert event.op == "d"
        assert event.after is None

    def test_unknown_kind_returns_none(self):
        """Unknown change kinds should return None (graceful skip)."""
        source = _make_source()
        change = {"kind": "truncate", "schema": "public", "table": "orders"}
        event = self._parse(change, source)
        assert event is None


class TestPostgresConnectorBehavior:
    async def test_feedback_interval_constant_is_10(self):
        """FEEDBACK_INTERVAL must never exceed 10 seconds per spec."""
        assert FEEDBACK_INTERVAL <= 10

    async def test_connector_drops_slot_on_close(self):
        """close() should execute pg_drop_replication_slot."""
        source = _make_source()
        ckpt = MagicMock()
        ckpt.get = MagicMock(return_value=None)
        connector = PostgresConnector(source, ckpt)

        fake_cursor = MagicMock()
        connector._cursor = fake_cursor
        connector._conn = MagicMock()

        await connector.close()

        fake_cursor.execute.assert_called_once()
        call_sql = fake_cursor.execute.call_args[0][0]
        assert "pg_drop_replication_slot" in call_sql
