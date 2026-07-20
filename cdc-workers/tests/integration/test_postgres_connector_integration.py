"""
Integration tests for PostgreSQL logical replication connector.

Requires pg-source container from docker-compose.dev.yml:
    docker compose -f docker/docker-compose.dev.yml up -d pg-source

Run with:
    pytest tests/integration/test_postgres_connector_integration.py -m integration

The test creates a table, starts the connector, then INSERTs a row and
verifies a CDCEvent is received.
"""
from __future__ import annotations

import asyncio
import os
import pytest

PG_HOST = os.getenv("PG_SOURCE_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("PG_SOURCE_PORT", "5434"))
PG_USER = os.getenv("PG_SOURCE_USER", "cdc_user")
PG_PASSWORD = os.getenv("PG_SOURCE_PASSWORD", "cdc_password")
PG_DBNAME = os.getenv("PG_SOURCE_DB", "source_db")

pytestmark = pytest.mark.integration

SOURCE_CFG = {
    "source_id": "pg-int-001",
    "bank_id": "bank-int",
    "tenant_id": "tenant-int",
    "host": PG_HOST,
    "port": PG_PORT,
    "database_name": PG_DBNAME,
    "username": PG_USER,
    "password": PG_PASSWORD,
    "assigned_tables": [
        {"schema_name": "public", "table_name": "cdc_test_items"}
    ],
}


@pytest.fixture(scope="module")
def pg_conn():
    """Return a live psycopg2 connection; skip if pg-source unavailable."""
    try:
        import psycopg2
    except ImportError:
        pytest.skip("psycopg2 not installed")

    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DBNAME,
            user=PG_USER, password=PG_PASSWORD, connect_timeout=5,
        )
        conn.autocommit = True
    except Exception as exc:
        pytest.skip(f"Postgres not available at {PG_HOST}:{PG_PORT} — {exc}")

    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS cdc_test_items (
                id    SERIAL PRIMARY KEY,
                name  TEXT,
                value NUMERIC
            )
            """
        )
        cur.execute("ALTER TABLE cdc_test_items REPLICA IDENTITY FULL")
        cur.execute("DELETE FROM cdc_test_items")

    yield conn
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS cdc_test_items")
    conn.close()


class FakeCheckpointManager:
    def load(self, _source_id):
        return {}

    async def save(self, _source_id, _data):
        pass


@pytest.mark.asyncio
async def test_postgres_connector_emits_insert(pg_conn):
    """Connector should emit op='c' for a new row via wal2json."""
    from connectors.postgres import PostgresConnector

    cp = FakeCheckpointManager()
    connector = PostgresConnector(SOURCE_CFG, cp)
    connector._running = True

    events = []

    async def _collect():
        async for event in connector.stream_events():
            events.append(event)
            if len(events) >= 1:
                connector._running = False
                break

    async def _insert():
        import psycopg2
        await asyncio.sleep(2.0)
        conn2 = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DBNAME,
            user=PG_USER, password=PG_PASSWORD,
        )
        conn2.autocommit = True
        with conn2.cursor() as cur:
            cur.execute(
                "INSERT INTO cdc_test_items (name, value) VALUES (%s, %s)",
                ("widget", 9.99),
            )
        conn2.close()

    try:
        await asyncio.wait_for(
            asyncio.gather(_collect(), _insert()),
            timeout=30,
        )
    except asyncio.TimeoutError:
        pytest.fail("Connector did not emit event within 30s")
    finally:
        await connector.close()

    assert len(events) >= 1
    ev = events[0]
    assert ev.op == "c"
    assert ev.table_name == "cdc_test_items"


@pytest.mark.asyncio
async def test_postgres_connector_emits_delete(pg_conn):
    """Connector should emit op='d' for a DELETE."""
    import psycopg2
    from connectors.postgres import PostgresConnector

    # Pre-insert a row
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO cdc_test_items (name, value) VALUES (%s, %s)", ("to-delete", 1.0))

    await asyncio.sleep(0.5)

    cp = FakeCheckpointManager()
    connector = PostgresConnector(SOURCE_CFG, cp)
    connector._running = True

    events = []

    async def _collect():
        async for event in connector.stream_events():
            if event.op == "d":
                events.append(event)
                connector._running = False
                break

    async def _delete():
        await asyncio.sleep(2.0)
        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM cdc_test_items WHERE name=%s", ("to-delete",))

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
