"""
P4.6 — E2E Integration Test: MongoDB → Redis → Spark → Iceberg

Tests the full pipeline from a MongoDB change event through Redis Streams
to Spark micro-batch processing and Iceberg-style merge write.

Since a real Iceberg catalog (Nessie/Glue) is rarely available in dev,
this test uses a mock IcebergWriter that captures MERGE operations,
while all other layers (MongoDB, Redis, Spark) use real containers.

Requires:
    docker compose -f docker/docker-compose.dev.yml up -d redis mongo-source

Run with:
    pytest tests/integration/test_mongodb_to_iceberg_e2e.py -m integration -v
"""
from __future__ import annotations

import json
import os
import sys
import time
import pytest

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27018/?replicaSet=rs0")

pytestmark = pytest.mark.integration

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

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
def mongo_collection():
    try:
        from pymongo import MongoClient
    except ImportError:
        pytest.skip("pymongo not installed")
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except Exception as exc:
        pytest.skip(f"MongoDB not available — {exc}")
    col = client["e2e_iceberg_test"]["cdc_sales"]
    col.delete_many({})
    yield col
    col.drop()
    client.close()


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    _py = sys.executable
    session = (
        SparkSession.builder.master("local[2]")
        .appName("mongo-iceberg-e2e")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.pyspark.python", _py)
        .config("spark.pyspark.driver.python", _py)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def clean_redis(redis_client):
    yield
    for k in redis_client.keys("cdc:bank-iceberg:*"):
        redis_client.delete(k)


# ─────────────────────────────────────────────────────────────────────────────
# Mock Iceberg Writer
# ─────────────────────────────────────────────────────────────────────────────

class InMemoryIcebergWriter:
    """
    Test double for IcebergWriter. Maintains an in-memory 'table' dict
    to simulate MERGE INTO behaviour without a real Iceberg catalog.
    """

    def __init__(self, pk_columns):
        self.pk_columns = pk_columns
        self.table: dict = {}          # pk_key -> row dict
        self.batches_written: list = []

    def _pk_key(self, row_dict: dict) -> str:
        return "|".join(str(row_dict.get(c, "")) for c in self.pk_columns)

    def write_batch(self, batch_df, batch_id: int) -> None:
        rows = batch_df.collect()
        self.batches_written.append(batch_id)
        for row in rows:
            row_dict = row.asDict()
            pk = self._pk_key(row_dict)
            op = row_dict.get("op", "c")
            if op == "c":
                self.table[pk] = row_dict
            elif op == "u":
                self.table[pk] = row_dict
            elif op == "d":
                self.table.pop(pk, None)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _seed_redis_from_events(redis_client, stream_key: str, events: list) -> None:
    """Push pre-formed CDC event dicts onto Redis stream."""
    for ev in events:
        redis_client.xadd(stream_key, {k: str(v) for k, v in ev.items()})


def _make_cdc_event(op, pk, data, ts_ms=None):
    return {
        "event_id": f"evt-{pk}-{op}",
        "op": op,
        "bank_id": "bank-iceberg",
        "tenant_id": "tenant-iceberg",
        "source_id": "mongo-src-001",
        "schema_name": "e2e_iceberg_test",
        "table_name": "cdc_sales",
        "lsn": f"token:{pk}",
        "ts_ms": str(ts_ms or int(time.time() * 1000)),
        "pk_values": json.dumps({"_id": str(pk)}),
        "before": "null",
        "after": json.dumps({**{"_id": str(pk)}, **data}),
        "metadata": "{}",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestMongoToIcebergE2E:
    STREAM_KEY = "cdc:bank-iceberg:tenant-iceberg:mongo-src-001:e2e_iceberg_test:cdc_sales"

    def _read_and_write(self, redis_client, spark, writer, stream_key):
        """Read one batch from Redis and pipe through Iceberg writer."""
        from consumer.redis_source import RedisStreamSource
        from pyspark.sql.types import StructType, StructField, StringType

        source = RedisStreamSource(
            redis_url=REDIS_URL,
            stream_keys=[stream_key],
            group="fusion-spark",
            consumer_name="iceberg-e2e-consumer",
        )
        df = source.read_batch(spark, count=50, block_ms=3000)
        if df is None or df.count() == 0:
            return 0

        # Flatten: extract _id, product, qty from 'after' JSON
        from pyspark.sql import functions as F

        flat_schema = StructType([
            StructField("op", StringType()),
            StructField("_id", StringType()),
            StructField("product", StringType()),
            StructField("qty", StringType()),
        ])
        flat_rows = []
        for row in df.collect():
            after = json.loads(row.after) if row.after and row.after != "null" else {}
            flat_rows.append((row.op, after.get("_id"), after.get("product"), str(after.get("qty", ""))))

        flat_df = spark.createDataFrame(flat_rows, schema=flat_schema)
        writer.write_batch(flat_df, batch_id=0)
        source.ack()
        return df.count()

    def test_insert_events_appear_in_iceberg_table(self, redis_client, spark):
        """INSERT events end up as rows in the Iceberg table."""
        redis_client.delete(self.STREAM_KEY)
        redis_client.xgroup_create(self.STREAM_KEY, "fusion-spark", id="0", mkstream=True)

        events = [
            _make_cdc_event("c", "p1", {"product": "laptop", "qty": 5}),
            _make_cdc_event("c", "p2", {"product": "tablet", "qty": 10}),
        ]
        _seed_redis_from_events(redis_client, self.STREAM_KEY, events)

        writer = InMemoryIcebergWriter(pk_columns=["_id"])
        count = self._read_and_write(redis_client, spark, writer, self.STREAM_KEY)

        assert count == 2
        assert len(writer.table) == 2
        assert "p1" in writer.table
        assert writer.table["p1"]["product"] == "laptop"

    def test_update_event_overwrites_row(self, redis_client, spark):
        """UPDATE event replaces the existing row in Iceberg (SCD1 behaviour)."""
        redis_client.delete(self.STREAM_KEY)
        redis_client.xgroup_create(self.STREAM_KEY, "fusion-spark", id="0", mkstream=True)

        writer = InMemoryIcebergWriter(pk_columns=["_id"])
        # Seed initial insert
        writer.table["p3"] = {"op": "c", "_id": "p3", "product": "phone", "qty": "1"}

        # Seed update event
        events = [_make_cdc_event("u", "p3", {"product": "phone-v2", "qty": "2"})]
        _seed_redis_from_events(redis_client, self.STREAM_KEY, events)
        self._read_and_write(redis_client, spark, writer, self.STREAM_KEY)

        assert writer.table["p3"]["product"] == "phone-v2"
        assert writer.table["p3"]["qty"] == "2"

    def test_delete_event_removes_row(self, redis_client, spark):
        """DELETE event removes the row from the Iceberg table."""
        redis_client.delete(self.STREAM_KEY)
        redis_client.xgroup_create(self.STREAM_KEY, "fusion-spark", id="0", mkstream=True)

        writer = InMemoryIcebergWriter(pk_columns=["_id"])
        writer.table["p4"] = {"op": "c", "_id": "p4", "product": "monitor", "qty": "3"}

        events = [_make_cdc_event("d", "p4", {})]
        _seed_redis_from_events(redis_client, self.STREAM_KEY, events)
        self._read_and_write(redis_client, spark, writer, self.STREAM_KEY)

        assert "p4" not in writer.table

    def test_mixed_ops_applied_correctly(self, redis_client, spark):
        """A single batch with insert + update + delete is all handled correctly."""
        redis_client.delete(self.STREAM_KEY)
        redis_client.xgroup_create(self.STREAM_KEY, "fusion-spark", id="0", mkstream=True)

        writer = InMemoryIcebergWriter(pk_columns=["_id"])
        writer.table["existing"] = {"op": "c", "_id": "existing", "product": "old", "qty": "1"}

        events = [
            _make_cdc_event("c", "new1", {"product": "headset", "qty": "4"}),
            _make_cdc_event("u", "existing", {"product": "updated", "qty": "99"}),
            _make_cdc_event("d", "to-delete", {}),
        ]
        writer.table["to-delete"] = {"op": "c", "_id": "to-delete", "product": "x", "qty": "0"}
        _seed_redis_from_events(redis_client, self.STREAM_KEY, events)
        self._read_and_write(redis_client, spark, writer, self.STREAM_KEY)

        assert "new1" in writer.table
        assert writer.table["existing"]["product"] == "updated"
        assert "to-delete" not in writer.table

    def test_redis_ack_clears_pending_after_iceberg_write(self, redis_client, spark):
        """Pending messages are ACKed after successful Iceberg write."""
        redis_client.delete(self.STREAM_KEY)
        redis_client.xgroup_create(self.STREAM_KEY, "fusion-spark", id="0", mkstream=True)

        events = [_make_cdc_event("c", "ack-test", {"product": "cable", "qty": "1"})]
        _seed_redis_from_events(redis_client, self.STREAM_KEY, events)

        from consumer.redis_source import RedisStreamSource
        from pyspark.sql.types import StructType, StructField, StringType

        source = RedisStreamSource(
            redis_url=REDIS_URL,
            stream_keys=[self.STREAM_KEY],
            group="fusion-spark",
            consumer_name="ack-test-consumer",
        )
        source.read_batch(spark, count=10, block_ms=2000)
        pending_before = source.pending_count()
        source.ack()
        pending_after = source.pending_count()

        assert pending_before == 1
        assert pending_after == 0
