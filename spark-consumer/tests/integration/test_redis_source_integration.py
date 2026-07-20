"""
Integration tests for RedisStreamSource (spark-consumer).

Requires Redis container:
    docker compose -f docker/docker-compose.dev.yml up -d redis

Run with:
    pytest tests/integration/test_redis_source_integration.py -m integration
"""
from __future__ import annotations

import os
import sys
import time
import pytest

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

pytestmark = pytest.mark.integration

# Ensure correct Python interpreter for Spark workers
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
def spark():
    from pyspark.sql import SparkSession
    _py = sys.executable
    session = (
        SparkSession.builder.master("local[1]")
        .appName("redis-source-integration-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.pyspark.python", _py)
        .config("spark.pyspark.driver.python", _py)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def clean_streams(redis_client):
    yield
    for key in redis_client.keys("cdc:bank-spark-int:*"):
        redis_client.delete(key)


class TestRedisSourceIntegration:
    def _seed_stream(self, redis_client, stream_key: str, n: int = 3):
        """Push n fake CDC messages onto a Redis stream."""
        for i in range(n):
            redis_client.xadd(
                stream_key,
                {
                    "event_id": f"evt-{i}",
                    "op": "c",
                    "bank_id": "bank-spark-int",
                    "tenant_id": "tenant-spark-int",
                    "source_id": "src-spark-int-001",
                    "schema_name": "mydb",
                    "table_name": "orders",
                    "lsn": f"bin:{i}",
                    "ts_ms": str(int(time.time() * 1000)),
                    "pk_values": '{"id": ' + str(i) + "}",
                    "before": "null",
                    "after": '{"id": ' + str(i) + ', "amount": ' + str(i * 10.0) + "}",
                    "metadata": "{}",
                },
            )

    def test_read_batch_returns_dataframe(self, redis_client, spark):
        from consumer.redis_source import RedisStreamSource

        stream_key = "cdc:bank-spark-int:tenant-spark-int:src-spark-int-001:mydb:orders"
        redis_client.delete(stream_key)  # fresh start
        redis_client.xgroup_create(stream_key, "fusion-spark", id="0", mkstream=True)
        self._seed_stream(redis_client, stream_key, n=3)

        source = RedisStreamSource(
            redis_url=REDIS_URL,
            stream_keys=[stream_key],
            group="fusion-spark",
            consumer_name="integration-consumer-1",
        )

        df = source.read_batch(spark, count=10, block_ms=1000)
        assert df is not None
        assert df.count() == 3
        cols = set(df.columns)
        assert {"op", "bank_id", "table_name", "after"}.issubset(cols)

    def test_ack_clears_pending_entries(self, redis_client, spark):
        from consumer.redis_source import RedisStreamSource

        stream_key = "cdc:bank-spark-int:tenant-spark-int:src-spark-int-001:mydb:orders"
        redis_client.delete(stream_key)
        redis_client.xgroup_create(stream_key, "fusion-spark", id="0", mkstream=True)
        self._seed_stream(redis_client, stream_key, n=2)

        source = RedisStreamSource(
            redis_url=REDIS_URL,
            stream_keys=[stream_key],
            group="fusion-spark",
            consumer_name="integration-consumer-2",
        )

        source.read_batch(spark, count=10, block_ms=1000)
        pending_before = source.pending_count()
        source.ack()
        pending_after = source.pending_count()

        assert pending_before == 2
        assert pending_after == 0

    def test_ensure_groups_creates_consumer_group(self, redis_client, spark):
        from consumer.redis_source import RedisStreamSource

        stream_key = "cdc:bank-spark-int:tenant-spark-int:src-spark-int-001:mydb:newstream"
        redis_client.delete(stream_key)

        source = RedisStreamSource(
            redis_url=REDIS_URL,
            stream_keys=[stream_key],
            group="fusion-spark",
            consumer_name="integration-consumer-3",
        )
        source.ensure_groups()

        groups = redis_client.xinfo_groups(stream_key)
        assert any(g["name"] == "fusion-spark" for g in groups)
