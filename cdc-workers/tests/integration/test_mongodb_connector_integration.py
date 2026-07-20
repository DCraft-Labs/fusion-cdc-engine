"""
Integration tests for MongoDB change-stream connector.

Requires mongo-source container from docker-compose.dev.yml:
    docker compose -f docker/docker-compose.dev.yml up -d mongo-source mongo-init

Run with:
    pytest tests/integration/test_mongodb_connector_integration.py -m integration

The replica set must be initialised before these tests.  The mongo-init
container in docker-compose.dev.yml handles that automatically.
"""
from __future__ import annotations

import asyncio
import os
import time
import pytest

MONGO_URI = os.getenv(
    "MONGO_URI", "mongodb://127.0.0.1:27018/?replicaSet=rs0"
)

pytestmark = pytest.mark.integration

SOURCE_CFG = {
    "source_id": "mongo-int-001",
    "bank_id": "bank-int",
    "tenant_id": "tenant-int",
    "uri": MONGO_URI,
    "database_name": "integration_test_db",
    "assigned_tables": [
        {"schema_name": "integration_test_db", "table_name": "cdc_test_sales"}
    ],
}


@pytest.fixture(scope="module")
def mongo_collection():
    """Return a live pymongo collection; skip if MongoDB unavailable."""
    try:
        from pymongo import MongoClient
    except ImportError:
        pytest.skip("pymongo not installed")

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except Exception as exc:
        pytest.skip(f"MongoDB not available — {exc}")

    db = client["integration_test_db"]
    col = db["cdc_test_sales"]
    col.delete_many({})
    yield col
    col.drop()
    client.close()


class FakeCheckpointManager:
    def load(self, _source_id):
        return {}

    async def save(self, _source_id, _data):
        pass


@pytest.mark.asyncio
async def test_mongodb_connector_emits_insert(mongo_collection):
    """Connector should emit op='c' for a new document."""
    from connectors.mongodb import MongoDBConnector

    cp = FakeCheckpointManager()
    connector = MongoDBConnector(SOURCE_CFG, cp)
    connector._running = True

    events = []

    async def _collect():
        async for event in connector.stream_events():
            events.append(event)
            if len(events) >= 1:
                connector._running = False
                break

    async def _insert():
        await asyncio.sleep(1.5)
        mongo_collection.insert_one({"product": "laptop", "qty": 3, "price": 999.0})

    try:
        await asyncio.wait_for(
            asyncio.gather(_collect(), _insert()),
            timeout=30,
        )
    except asyncio.TimeoutError:
        pytest.fail("MongoDB connector did not emit event within 30s")
    finally:
        await connector.close()

    assert len(events) >= 1
    ev = events[0]
    assert ev.op == "c"
    assert ev.table_name == "cdc_test_sales"
    assert ev.after is not None


@pytest.mark.asyncio
async def test_mongodb_connector_emits_update(mongo_collection):
    """Connector should emit op='u' for an update."""
    from connectors.mongodb import MongoDBConnector

    # Pre-insert
    result = mongo_collection.insert_one({"product": "phone", "qty": 1})
    doc_id = result.inserted_id

    cp = FakeCheckpointManager()
    connector = MongoDBConnector(SOURCE_CFG, cp)
    connector._running = True

    events = []

    async def _collect():
        async for event in connector.stream_events():
            if event.op == "u":
                events.append(event)
                connector._running = False
                break

    async def _update():
        await asyncio.sleep(1.5)
        mongo_collection.update_one(
            {"_id": doc_id}, {"$set": {"qty": 10}}
        )

    try:
        await asyncio.wait_for(
            asyncio.gather(_collect(), _update()),
            timeout=30,
        )
    except asyncio.TimeoutError:
        pytest.fail("No update event received within 30s")
    finally:
        await connector.close()

    assert len(events) >= 1
    assert events[0].op == "u"


@pytest.mark.asyncio
async def test_mongodb_connector_emits_delete(mongo_collection):
    """Connector should emit op='d' for a delete."""
    from connectors.mongodb import MongoDBConnector

    result = mongo_collection.insert_one({"product": "to-delete"})
    doc_id = result.inserted_id

    cp = FakeCheckpointManager()
    connector = MongoDBConnector(SOURCE_CFG, cp)
    connector._running = True

    events = []

    async def _collect():
        async for event in connector.stream_events():
            if event.op == "d":
                events.append(event)
                connector._running = False
                break

    async def _delete():
        await asyncio.sleep(1.5)
        mongo_collection.delete_one({"_id": doc_id})

    try:
        await asyncio.wait_for(
            asyncio.gather(_collect(), _delete()),
            timeout=30,
        )
    except asyncio.TimeoutError:
        pytest.fail("No delete event received within 30s")
    finally:
        await connector.close()

    assert len(events) >= 1
    assert events[0].op == "d"
