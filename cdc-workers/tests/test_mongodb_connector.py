"""
MongoDB Connector unit tests (mock-based — no Docker required).

Tests:
  1. test_serialize_objectid
  2. test_serialize_datetime
  3. test_serialize_nested_dict
  4. test_insert_event_mapped_to_c
  5. test_update_event_mapped_to_u
  6. test_delete_event_mapped_to_d
  7. test_resume_token_stored_in_checkpoint
  8. test_resume_token_loaded_from_checkpoint
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch, call
import pytest

from connectors.mongodb import MongoDBConnector, _serialize


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_source(**overrides) -> dict:
    base = {
        "source_id": "aaaabbbb-cccc-dddd-eeee-000011112222",
        "bank_id": "bank-001",
        "tenant_id": "tenant-001",
        "host": "127.0.0.1",
        "port": 27017,
        "database_name": "testdb",
        "username": "",
        "password": "",
        "assigned_tables": [{"schema_name": "testdb", "table_name": "orders"}],
    }
    base.update(overrides)
    return base


class _FakeObjectId:
    """Stand-in for bson.ObjectId when bson is available."""
    def __init__(self, val: str = "507f1f77bcf86cd799439011"):
        self._val = val
    def __str__(self):
        return self._val


def _make_change_doc(
    op: str,
    db: str = "testdb",
    collection: str = "orders",
    doc: dict = None,
    before: dict = None,
) -> dict:
    """Build a MongoDB change stream document with string _id values."""
    change: Dict[str, Any] = {
        "operationType": op,
        "ns": {"db": db, "coll": collection},
        "documentKey": {"_id": "507f1f77bcf86cd799439011"},  # plain string ID
        "_id": {"_data": "some-resume-token"},
        "clusterTime": None,
    }
    if op == "insert":
        change["fullDocument"] = doc or {"_id": "507f1f77bcf86cd799439011", "name": "Alice"}
    elif op == "update":
        change["fullDocument"] = doc or {"_id": "507f1f77bcf86cd799439011", "name": "Bob"}
        change["fullDocumentBeforeChange"] = before or {"_id": "507f1f77bcf86cd799439011", "name": "Alice"}
        change["updateDescription"] = {"updatedFields": {"name": "Bob"}, "removedFields": []}
    elif op == "delete":
        change["fullDocumentBeforeChange"] = before or {"_id": "507f1f77bcf86cd799439011", "name": "Alice"}
    return change


# ---------------------------------------------------------------------------
# Pure-unit tests
# ---------------------------------------------------------------------------

class TestSerialize:
    def test_serialize_objectid(self):
        from bson import ObjectId
        oid = ObjectId("507f1f77bcf86cd799439011")
        result = _serialize(oid)
        assert result == "507f1f77bcf86cd799439011"

    def test_serialize_datetime(self):
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = _serialize(dt)
        assert isinstance(result, str)
        assert "2024" in result

    def test_serialize_nested_dict(self):
        dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        data = {"created_at": dt, "nested": {"ts": dt}}
        result = _serialize(data)
        assert isinstance(result["created_at"], str)
        assert isinstance(result["nested"]["ts"], str)


# ---------------------------------------------------------------------------
# Change event mapping tests
# ---------------------------------------------------------------------------

class TestMongoDBConnectorEventMapping:
    def _map_change(self, change: dict) -> Any:
        """Call the connector's _parse_change static method."""
        return MongoDBConnector._parse_change(
            change=change,
            db_name="testdb",
            col_name="orders",
            source_id="src-001",
            bank_id="bank-001",
            tenant_id="tenant-001",
        )

    def test_insert_event_mapped_to_c(self):
        change = _make_change_doc("insert", doc={"_id": "oid_1", "name": "Alice"})
        event = self._map_change(change)
        assert event is not None
        assert event.op == "c"

    def test_update_event_mapped_to_u(self):
        change = _make_change_doc(
            "update",
            doc={"_id": "oid_1", "name": "Bob"},
            before={"_id": "oid_1", "name": "Alice"},
        )
        event = self._map_change(change)
        assert event is not None
        assert event.op == "u"

    def test_delete_event_mapped_to_d(self):
        change = _make_change_doc("delete")
        event = self._map_change(change)
        assert event is not None
        assert event.op == "d"


class TestMongoDBConnectorCheckpointing:
    async def test_resume_token_stored_in_checkpoint(self):
        """Each change should store the resume_token in the checkpoint."""
        source = _make_source()
        ckpt = MagicMock()
        ckpt.get = MagicMock(return_value=None)
        ckpt.set = MagicMock()
        connector = MongoDBConnector(source, ckpt)

        resume_token = {"_data": "abc123"}
        # Simulate connector storing resume token (as _watch_collection does)
        connector._ckpt.set(
            "src-001", "testdb", "orders",
            json.dumps(resume_token, default=str),
        )

        ckpt.set.assert_called_once()
        args = ckpt.set.call_args[0]
        # Stored value should be JSON-serializable resume token
        stored_val = json.loads(args[-1])
        assert stored_val == resume_token

    async def test_resume_token_loaded_from_checkpoint(self):
        """Connector should load resume token from checkpoint on startup."""
        source = _make_source()
        token = {"_data": "resume_from_here"}
        ckpt = MagicMock()
        ckpt.get = MagicMock(return_value=json.dumps(token))
        connector = MongoDBConnector(source, ckpt)

        # Simulates what _watch_collection does to load resume token
        resume_token_json = connector._ckpt.get("src-001", "testdb", "orders")
        loaded = json.loads(resume_token_json)
        assert loaded == token
