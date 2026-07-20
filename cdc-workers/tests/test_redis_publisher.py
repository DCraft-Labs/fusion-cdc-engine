"""
P2.5 — Tests for redis_publisher.py (mock redis).
"""

import pytest
from unittest.mock import MagicMock, patch, call

from cdc_worker.event_envelope import build_event
from cdc_worker.redis_publisher import RedisStreamPublisher, _stream_key


DEFAULTS = dict(
    op="c",
    source_id="src-001",
    bank_id="bank-001",
    tenant_id="tenant-001",
    schema_name="testdb",
    table_name="orders",
    lsn="bin:1234",
    ts_ms=1_714_000_000_000,
    pk_values={"id": 1},
    after={"id": 1, "name": "Alice"},
)


def _make_publisher(mock_redis_client, fallback=None):
    pub = RedisStreamPublisher.__new__(RedisStreamPublisher)
    pub._client = mock_redis_client
    pub._maxlen = 100_000
    pub._consumer_group = "fusion-spark"
    pub._fallback = fallback
    pub._known_streams = set()
    return pub


class TestRedisPublisher:
    def test_publish_single_tenant_xadd_called(self):
        mock_client = MagicMock()
        pub = _make_publisher(mock_client)
        event = build_event(**DEFAULTS)

        pub.publish(event)

        mock_client.xadd.assert_called_once()
        args, kwargs = mock_client.xadd.call_args
        assert kwargs["maxlen"] == 100_000
        assert kwargs["approximate"] is True

    def test_publish_multi_tenant_two_xadd_calls(self):
        mock_client = MagicMock()
        pub = _make_publisher(mock_client)
        event = build_event(**DEFAULTS)
        routing = [
            {"bank_id": "bank-001", "tenant_id": "t1", "source_id": "src-001"},
            {"bank_id": "bank-001", "tenant_id": "t2", "source_id": "src-001"},
        ]
        pub.publish(event, routing=routing)
        assert mock_client.xadd.call_count == 2

    def test_stream_key_matches_format(self):
        event = build_event(**DEFAULTS)
        key = _stream_key(event)
        assert key == "cdc:bank-001:tenant-001:src-001:testdb:orders"

    def test_publish_falls_to_fallback_on_redis_error(self):
        import redis.exceptions
        mock_client = MagicMock()
        mock_client.xadd.side_effect = redis.exceptions.ConnectionError("down")
        mock_fallback = MagicMock()
        pub = _make_publisher(mock_client, fallback=mock_fallback)
        event = build_event(**DEFAULTS)

        result = pub.publish(event)

        assert result is False
        mock_fallback.enqueue.assert_called_once()

    def test_publish_returns_false_on_redis_down(self):
        import redis.exceptions
        mock_client = MagicMock()
        mock_client.xadd.side_effect = redis.exceptions.RedisError("timeout")
        pub = _make_publisher(mock_client)
        event = build_event(**DEFAULTS)

        result = pub.publish(event)

        assert result is False

    def test_xadd_maxlen_100000_approximate(self):
        mock_client = MagicMock()
        pub = _make_publisher(mock_client)
        event = build_event(**DEFAULTS)
        pub.publish(event)
        _, kwargs = mock_client.xadd.call_args
        assert kwargs["maxlen"] == 100_000
        assert kwargs["approximate"] is True

    def test_consumer_group_created_for_new_stream(self):
        mock_client = MagicMock()
        pub = _make_publisher(mock_client)
        event = build_event(**DEFAULTS)
        pub.publish(event)
        mock_client.xgroup_create.assert_called_once()

    def test_busygroup_error_swallowed(self):
        import redis.exceptions
        mock_client = MagicMock()
        mock_client.xgroup_create.side_effect = redis.exceptions.ResponseError("BUSYGROUP")
        pub = _make_publisher(mock_client)
        event = build_event(**DEFAULTS)
        # Must not raise
        result = pub.publish(event)
        assert result is True

    def test_all_fields_serialized_as_strings(self):
        mock_client = MagicMock()
        pub = _make_publisher(mock_client)
        event = build_event(**DEFAULTS)
        pub.publish(event)
        _, kwargs = mock_client.xadd.call_args
        fields = kwargs if "fields" not in kwargs else kwargs["fields"]
        # xadd is called as xadd(key, fields_dict, ...)
        # the second positional arg is the fields dict
        pos_args = mock_client.xadd.call_args[0]
        field_dict = pos_args[1] if len(pos_args) > 1 else {}
        for k, v in field_dict.items():
            assert isinstance(v, str), f"{k} is not str"
