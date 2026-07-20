"""
End-to-end integration test: Redis → Spark consumer → Postgres writer.

Requires both redis and postgres-dest containers:
    docker compose -f docker/docker-compose.dev.yml up -d redis postgres-dest

Run with:
    pytest tests/integration/test_end_to_end.py -m integration
"""
from __future__ import annotations

import json
import os
import sys
import time
import pytest

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
PG_DSN = os.getenv(
    "PG_DEST_DSN",
    "host=127.0.0.1 port=5433 dbname=fusion_dw user=dw_user password=dw_password",
)

pytestmark = pytest.mark.integration

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="module")
def redis_client():
    import redis
    client = redis.from_url(REDIS_URL, socket_connect_timeout=3)
    try:
        client.ping()
    except Exception as exc:
        pytest.skip(f"Redis not available — {exc}")
    yield client
    client.close()


@pytest.fixture(scope="module")
def pg_conn():
    try:
        import psycopg2
    except ImportError:
        pytest.skip("psycopg2 not installed")
    try:
        conn = psycopg2.connect(PG_DSN, connect_timeout=5)
        conn.autocommit = True
    except Exception as exc:
        pytest.skip(f"postgres-dest not available — {exc}")
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS e2e_test_events (
                id      TEXT PRIMARY KEY,
                amount  TEXT,
                status  TEXT
            )
            """
        )
        cur.execute("DELETE FROM e2e_test_events")
    yield conn
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS e2e_test_events")
    conn.close()


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    _py = sys.executable
    session = (
        SparkSession.builder.master("local[1]")
        .appName("e2e-integration-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.pyspark.python", _py)
        .config("spark.pyspark.driver.python", _py)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _seed_redis(redis_client, stream_key: str, rows: list):
    """Push CDC event rows onto Redis stream."""
    for row in rows:
        redis_client.xadd(stream_key, {k: (json.dumps(v) if isinstance(v, dict) else str(v)) for k, v in row.items()})


class TestEndToEnd:
    def test_redis_to_postgres_pipeline(self, redis_client, spark, pg_conn):
        """
        Full pipeline: seed Redis → RedisStreamSource.read_batch →
        PostgresWriter.upsert_batch → assert rows in Postgres.
        """
        from consumer.redis_source import RedisStreamSource
        from writers.postgres_writer import PostgresWriter

        stream_key = "cdc:bank-e2e:tenant-e2e:src-e2e-001:mydb:e2e_test_events"
        redis_client.delete(stream_key)
        redis_client.xgroup_create(stream_key, "fusion-spark", id="0", mkstream=True)

        rows = [
            {
                "event_id": "e1", "op": "c", "bank_id": "bank-e2e",
                "tenant_id": "tenant-e2e", "source_id": "src-e2e-001",
                "schema_name": "mydb", "table_name": "e2e_test_events",
                "lsn": "bin:1", "ts_ms": str(int(time.time() * 1000)),
                "pk_values": json.dumps({"id": "r1"}),
                "before": "null",
                "after": json.dumps({"id": "r1", "amount": "150.00", "status": "new"}),
                "metadata": "{}",
            },
            {
                "event_id": "e2", "op": "c", "bank_id": "bank-e2e",
                "tenant_id": "tenant-e2e", "source_id": "src-e2e-001",
                "schema_name": "mydb", "table_name": "e2e_test_events",
                "lsn": "bin:2", "ts_ms": str(int(time.time() * 1000)),
                "pk_values": json.dumps({"id": "r2"}),
                "before": "null",
                "after": json.dumps({"id": "r2", "amount": "200.00", "status": "pending"}),
                "metadata": "{}",
            },
        ]
        _seed_redis(redis_client, stream_key, rows)

        # Step 1: read batch from Redis
        source = RedisStreamSource(
            redis_url=REDIS_URL,
            stream_keys=[stream_key],
            group="fusion-spark",
            consumer_name="e2e-consumer",
        )
        df = source.read_batch(spark, count=10, block_ms=2000)
        assert df is not None
        assert df.count() == 2

        # Step 2: write to Postgres via after JSON columns
        # Extract id/amount/status from the 'after' JSON field into a flat DataFrame
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType, StructField, StringType

        flat_schema = StructType([
            StructField("op", StringType()),
            StructField("id", StringType()),
            StructField("amount", StringType()),
            StructField("status", StringType()),
        ])

        flat_rows = []
        for row in df.collect():
            after = json.loads(row.after) if row.after and row.after != "null" else {}
            flat_rows.append((row.op, after.get("id"), after.get("amount"), after.get("status")))

        flat_df = spark.createDataFrame(flat_rows, schema=flat_schema)

        writer = PostgresWriter(pg_dsn=PG_DSN, table="e2e_test_events", pk_columns=["id"])
        writer.upsert_batch(flat_df, batch_id=0)

        # Step 3: ACK messages
        source.ack()
        assert source.pending_count() == 0

        # Step 4: verify Postgres
        with pg_conn.cursor() as cur:
            cur.execute("SELECT id, amount, status FROM e2e_test_events ORDER BY id")
            db_rows = {r[0]: r for r in cur.fetchall()}

        assert "r1" in db_rows
        assert "r2" in db_rows
        assert db_rows["r1"][1] == "150.00"
        assert db_rows["r2"][2] == "pending"

    def test_upsert_overwrites_existing_row(self, redis_client, spark, pg_conn):
        """An op='u' event should update the existing row via ON CONFLICT DO UPDATE."""
        from consumer.redis_source import RedisStreamSource
        from writers.postgres_writer import PostgresWriter
        from pyspark.sql.types import StructType, StructField, StringType

        stream_key = "cdc:bank-e2e:tenant-e2e:src-e2e-001:mydb:e2e_test_events"
        redis_client.delete(stream_key)
        redis_client.xgroup_create(stream_key, "fusion-spark", id="0", mkstream=True)

        # Seed an update event
        redis_client.xadd(stream_key, {
            "event_id": "e3", "op": "u", "bank_id": "bank-e2e",
            "tenant_id": "tenant-e2e", "source_id": "src-e2e-001",
            "schema_name": "mydb", "table_name": "e2e_test_events",
            "lsn": "bin:3", "ts_ms": str(int(time.time() * 1000)),
            "pk_values": json.dumps({"id": "r1"}),
            "before": json.dumps({"id": "r1", "amount": "150.00", "status": "new"}),
            "after": json.dumps({"id": "r1", "amount": "175.00", "status": "updated"}),
            "metadata": "{}",
        })

        source = RedisStreamSource(
            redis_url=REDIS_URL,
            stream_keys=[stream_key],
            group="fusion-spark",
            consumer_name="e2e-consumer-2",
        )
        df = source.read_batch(spark, count=5, block_ms=2000)

        flat_schema = StructType([
            StructField("op", StringType()),
            StructField("id", StringType()),
            StructField("amount", StringType()),
            StructField("status", StringType()),
        ])
        flat_rows = []
        for row in df.collect():
            after = json.loads(row.after) if row.after and row.after != "null" else {}
            flat_rows.append((row.op, after.get("id"), after.get("amount"), after.get("status")))

        flat_df = spark.createDataFrame(flat_rows, schema=flat_schema)
        writer = PostgresWriter(pg_dsn=PG_DSN, table="e2e_test_events", pk_columns=["id"])
        writer.upsert_batch(flat_df, batch_id=1)

        with pg_conn.cursor() as cur:
            cur.execute("SELECT amount, status FROM e2e_test_events WHERE id='r1'")
            row = cur.fetchone()

        assert row is not None
        assert row[0] == "175.00"
        assert row[1] == "updated"
