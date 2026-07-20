"""
Integration tests for RedisStreamPublisher.

Requires a real Redis instance — started via:
    docker compose -f docker/docker-compose.dev.yml up -d redis

Run with:
    pytest tests/integration/test_redis_publisher_integration.py -m integration
"""
from __future__ import annotations

import os
import time
import pytest

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def redis_client():
    """Return a live redis client; skip the whole module if Redis unavailable."""
    import redis
    client = redis.from_url(REDIS_URL, socket_connect_timeout=3)
    try:
        client.ping()
    except Exception as exc:
        pytest.skip(f"Redis not available at {REDIS_URL}: {exc}")
    yield client
    client.close()


@pytest.fixture(autouse=True)
def flush_keys(redis_client):
    """Delete test keys before each test."""
    yield
    for key in redis_client.keys("cdc:bank-int:*"):
        redis_client.delete(key)
    for key in redis_client.keys("dlq:bank-int:*"):
        redis_client.delete(key)


class TestRedisPublisherIntegration:
    def test_xadd_creates_stream_and_returns_message_id(self, redis_client):
        from cdc_worker.redis_publisher import RedisStreamPublisher
        from cdc_worker.event_envelope import build_event

        pub = RedisStreamPublisher(redis_url=REDIS_URL)
        event = build_event(
            op="c",
            source_id="src-int-001",
            bank_id="bank-int",
            tenant_id="tenant-int",
            schema_name="mydb",
            table_name="orders",
            lsn="bin:1",
            ts_ms=int(time.time() * 1000),
            pk_values={"id": 1},
            after={"id": 1, "amount": 99.50},
        )
        pub.publish(event)

        stream_key = f"cdc:bank-int:tenant-int:src-int-001:mydb:orders"
        length = redis_client.xlen(stream_key)
        assert length == 1

    def test_consumer_group_created_on_first_publish(self, redis_client):
        from cdc_worker.redis_publisher import RedisStreamPublisher
        from cdc_worker.event_envelope import build_event

        pub = RedisStreamPublisher(redis_url=REDIS_URL, consumer_group="fusion-spark")
        event = build_event(
            op="u",
            source_id="src-int-002",
            bank_id="bank-int",
            tenant_id="tenant-int",
            schema_name="mydb",
            table_name="products",
            lsn="bin:2",
            ts_ms=int(time.time() * 1000),
            pk_values={"id": 5},
            after={"id": 5, "price": 29.99},
        )
        pub.publish(event)

        stream_key = f"cdc:bank-int:tenant-int:src-int-002:mydb:products"
        groups = redis_client.xinfo_groups(stream_key)
        assert any(g["name"] == "fusion-spark" for g in groups)

    def test_publish_multiple_events_sequential(self, redis_client):
        from cdc_worker.redis_publisher import RedisStreamPublisher
        from cdc_worker.event_envelope import build_event

        pub = RedisStreamPublisher(redis_url=REDIS_URL)
        stream_key = "cdc:bank-int:tenant-int:src-int-003:mydb:invoices"

        for i in range(5):
            event = build_event(
                op="c",
                source_id="src-int-003",
                bank_id="bank-int",
                tenant_id="tenant-int",
                schema_name="mydb",
                table_name="invoices",
                lsn=f"bin:{i}",
                ts_ms=int(time.time() * 1000),
                pk_values={"id": i},
                after={"id": i, "total": i * 10.0},
            )
            pub.publish(event)

        assert redis_client.xlen(stream_key) == 5

    def test_xreadgroup_consumes_messages(self, redis_client):
        from cdc_worker.redis_publisher import RedisStreamPublisher
        from cdc_worker.event_envelope import build_event

        pub = RedisStreamPublisher(redis_url=REDIS_URL, consumer_group="fusion-spark")
        event = build_event(
            op="c",
            source_id="src-int-004",
            bank_id="bank-int",
            tenant_id="tenant-int",
            schema_name="mydb",
            table_name="shipments",
            lsn="bin:10",
            ts_ms=int(time.time() * 1000),
            pk_values={"id": 42},
            after={"id": 42, "status": "shipped"},
        )
        pub.publish(event)

        stream_key = "cdc:bank-int:tenant-int:src-int-004:mydb:shipments"
        messages = redis_client.xreadgroup(
            "fusion-spark", "consumer-1", {stream_key: ">"}, count=1
        )
        assert len(messages) == 1
        assert len(messages[0][1]) == 1
        redis_client.xack(stream_key, "fusion-spark", messages[0][1][0][0])
