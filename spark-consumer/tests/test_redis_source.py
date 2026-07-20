"""
Tests for consumer/redis_source.py — 7 tests

All Redis interactions are mocked; no real Redis instance required.
"""
from unittest.mock import MagicMock, patch, call

import pytest

from consumer.redis_source import RedisStreamSource, CDC_EVENT_FIELDS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_source(stream_keys=None):
    return RedisStreamSource(
        redis_url="redis://localhost:6379",
        stream_keys=stream_keys or ["cdc:b1:t1:src1:public:orders"],
        group="fusion-spark",
        consumer_name="test-consumer",
    )


def _fake_message(event_id="e1", op="c", stream_key="cdc:b1:t1:src1:public:orders"):
    fields = {f: f"val_{f}" for f in CDC_EVENT_FIELDS}
    fields["event_id"] = event_id
    fields["op"] = op
    return (stream_key, [("1-0", fields)])


def _mock_client(source):
    """Set source._client to a MagicMock (bypasses the property's lazy init)."""
    mock = MagicMock()
    source._client = mock
    return mock


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestRedisSourceSchema:
    def test_schema_contains_required_fields(self, spark):
        """Empty read returns a DataFrame with all CDC event fields as columns."""
        source = _make_source()
        mock = _mock_client(source)
        mock.xreadgroup.return_value = None

        df = source.read_batch(spark)

        assert set(df.columns) == set(CDC_EVENT_FIELDS)

    def test_empty_batch_returns_empty_dataframe(self, spark):
        source = _make_source()
        mock = _mock_client(source)
        mock.xreadgroup.return_value = []

        df = source.read_batch(spark)

        assert df.count() == 0
        assert set(df.columns) == set(CDC_EVENT_FIELDS)


class TestRedisSourceRead:
    def test_read_batch_returns_dataframe_with_rows(self, spark):
        """A single Redis message is parsed into one DataFrame row."""
        source = _make_source()
        mock = _mock_client(source)
        mock.xreadgroup.return_value = [_fake_message("evt-1", "c")]

        df = source.read_batch(spark, count=10)

        assert df.count() == 1
        row = df.collect()[0]
        assert row["event_id"] == "evt-1"
        assert row["op"] == "c"

    def test_multiple_stream_keys_combined_into_one_df(self, spark):
        """Messages from two stream keys are merged into a single DataFrame."""
        keys = ["cdc:b1:t1:src1:public:orders", "cdc:b1:t1:src1:public:accounts"]
        source = _make_source(stream_keys=keys)
        mock = _mock_client(source)
        mock.xreadgroup.return_value = [
            _fake_message("e1", "c", keys[0]),
            _fake_message("e2", "u", keys[1]),
        ]

        df = source.read_batch(spark)

        assert df.count() == 2

    def test_malformed_event_skipped_gracefully(self, spark):
        """A valid event is returned; no uncaught exceptions."""
        source = _make_source()
        mock = _mock_client(source)
        raw = [("cdc:b1:t1:src1:public:orders", [("1-0", {f: "x" for f in CDC_EVENT_FIELDS})])]
        mock.xreadgroup.return_value = raw

        df = source.read_batch(spark)

        assert df.count() == 1  # valid event survives


class TestRedisSourceAck:
    def test_ack_called_after_read(self, spark):
        """After read_batch(), ack() sends XACK for the returned message IDs."""
        source = _make_source()
        mock = _mock_client(source)
        mock.xreadgroup.return_value = [_fake_message("e1")]

        source.read_batch(spark)
        source.ack()

        mock.xack.assert_called_once_with(
            "cdc:b1:t1:src1:public:orders",
            "fusion-spark",
            "1-0",
        )

    def test_ensure_groups_creates_consumer_group(self):
        """ensure_groups() calls XGROUP CREATE MKSTREAM for each stream key."""
        source = _make_source(stream_keys=["cdc:b1:t1:src1:public:orders"])
        mock = _mock_client(source)

        source.ensure_groups()

        mock.xgroup_create.assert_called_once_with(
            "cdc:b1:t1:src1:public:orders",
            "fusion-spark",
            id="$",
            mkstream=True,
        )

    def test_ensure_groups_ignores_busygroup_error(self):
        """BUSYGROUP error (group already exists) is silently ignored."""
        source = _make_source()
        mock = _mock_client(source)
        mock.xgroup_create.side_effect = Exception("BUSYGROUP Consumer Group name already exists")

        # Should not raise
        source.ensure_groups()
