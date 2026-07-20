"""
Integration tests for MySQL binlog connector.

Requires mysql-source container from docker-compose.dev.yml:
    docker compose -f docker/docker-compose.dev.yml up -d mysql-source

Run with:
    pytest tests/integration/test_mysql_connector_integration.py -m integration

The test creates a small table in the CDC source MySQL, INSERTs rows,
and verifies the connector emits corresponding CDCEvents.
"""
from __future__ import annotations

import asyncio
import os
import time
import pytest

MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3307"))
MYSQL_USER = os.getenv("MYSQL_USER", "fusion_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "fusion_password")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "fusion_cdc_metadata")

pytestmark = pytest.mark.integration

SOURCE_CFG = {
    "source_id": "mysql-int-001",
    "bank_id": "bank-int",
    "tenant_id": "tenant-int",
    "host": MYSQL_HOST,
    "port": MYSQL_PORT,
    "database_name": MYSQL_DATABASE,
    "username": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "assigned_tables": [
        {"schema_name": MYSQL_DATABASE, "table_name": "cdc_test_orders"}
    ],
}


@pytest.fixture(scope="module")
def mysql_conn():
    """Return a live pymysql connection; skip entire module if unavailable."""
    try:
        import pymysql
    except ImportError:
        pytest.skip("pymysql not installed")

    try:
        conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            connect_timeout=5,
        )
    except Exception as exc:
        pytest.skip(f"MySQL not available at {MYSQL_HOST}:{MYSQL_PORT} — {exc}")

    # Create test table
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS cdc_test_orders (
                id     INT PRIMARY KEY,
                amount DECIMAL(10,2),
                status VARCHAR(50)
            )
            """
        )
        cur.execute("DELETE FROM cdc_test_orders")
    conn.commit()
    yield conn
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS cdc_test_orders")
    conn.commit()
    conn.close()


class FakeCheckpointManager:
    def load(self, _source_id):
        return {}

    async def save(self, _source_id, _data):
        pass


@pytest.mark.asyncio
async def test_mysql_connector_emits_insert_event(mysql_conn):
    """Connector should emit a CDCEvent with op='c' for a new row."""
    import pymysql
    from connectors.mysql import MySQLConnector

    cp = FakeCheckpointManager()
    connector = MySQLConnector(SOURCE_CFG, cp)
    connector._running = True

    events = []

    async def _collect():
        async for event in connector.stream_events():
            events.append(event)
            if len(events) >= 1:
                connector._running = False
                break

    # Insert a row AFTER starting the collector (slight delay)
    async def _insert():
        await asyncio.sleep(1.5)
        with mysql_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cdc_test_orders VALUES (%s, %s, %s)",
                (1, 99.99, "pending"),
            )
        mysql_conn.commit()

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
    assert ev.table_name == "cdc_test_orders"
    assert ev.after is not None


@pytest.mark.asyncio
async def test_mysql_connector_emits_update_event(mysql_conn):
    """Connector should emit op='u' for an UPDATE."""
    import pymysql
    from connectors.mysql import MySQLConnector

    # Pre-insert a row
    with mysql_conn.cursor() as cur:
        cur.execute("INSERT IGNORE INTO cdc_test_orders VALUES (%s, %s, %s)", (2, 10.0, "open"))
    mysql_conn.commit()
    await asyncio.sleep(0.5)

    cp = FakeCheckpointManager()
    connector = MySQLConnector(SOURCE_CFG, cp)
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
        with mysql_conn.cursor() as cur:
            cur.execute(
                "UPDATE cdc_test_orders SET status=%s WHERE id=%s", ("closed", 2)
            )
        mysql_conn.commit()

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
    assert events[0].table_name == "cdc_test_orders"
