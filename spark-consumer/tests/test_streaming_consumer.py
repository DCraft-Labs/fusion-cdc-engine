"""
Tests for consumer/streaming_consumer.py — 5 tests

Spark, Redis, and DB interactions are all mocked.
"""
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from consumer.streaming_consumer import StreamingConsumer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _base_config():
    return {
        "redis_url": "redis://localhost:6379",
        "stream_keys": ["cdc:b1:t1:src1:public:orders"],
        "writer_type": "postgres",
        "writer_config": {
            "pg_dsn": "postgresql://user:pass@localhost/testdb",
            "table": "orders",
            "pk_columns": ["id"],
        },
        "schema_reload_port": 0,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestStreamingConsumerSetup:
    def test_setup_creates_redis_source_and_writer(self, spark):
        """After setup(), redis_source and writer are initialised."""
        consumer = StreamingConsumer(_base_config())
        consumer.setup(spark=spark)

        assert consumer.redis_source is not None
        assert consumer.writer is not None

    def test_setup_with_transform_spec_creates_executor(self, spark):
        """When transform_spec is provided, transform_executor is created."""
        config = _base_config()
        config["transform_spec"] = {"transforms": []}
        consumer = StreamingConsumer(config)
        consumer.setup(spark=spark)

        assert consumer.transform_executor is not None

    def test_setup_with_dq_policy_creates_executor(self, spark):
        """When dq_policy is provided, dq_executor is created."""
        config = _base_config()
        config["dq_policy"] = {"rules": [], "on_fail": "alert"}
        consumer = StreamingConsumer(config)
        consumer.setup(spark=spark)

        assert consumer.dq_executor is not None


class TestStreamingConsumerProcessBatch:
    def test_process_batch_calls_writer_upsert(self, spark):
        """process_batch() delegates to writer.upsert_batch()."""
        consumer = StreamingConsumer(_base_config())
        consumer.setup(spark=spark)

        mock_writer = MagicMock()
        consumer.writer = mock_writer

        df = spark.createDataFrame([("c", "1", "Alice")], ["op", "id", "name"])
        consumer.process_batch(df, batch_id=0)

        mock_writer.upsert_batch.assert_called_once()
        call_df, call_bid = mock_writer.upsert_batch.call_args[0]
        assert call_bid == 0

    def test_process_batch_applies_transforms(self, spark):
        """process_batch() runs the transform executor before writing."""
        config = _base_config()
        config["transform_spec"] = {"transforms": [
            {"type": "string_op", "column": "name", "op": "upper", "output_column": "name"}
        ]}
        consumer = StreamingConsumer(config)
        consumer.setup(spark=spark)

        written_dfs = []
        mock_writer = MagicMock(side_effect=lambda df, bid: written_dfs.append(df))
        consumer.writer = MagicMock()
        consumer.writer.upsert_batch = lambda df, bid: written_dfs.append(df)

        df = spark.createDataFrame([("c", "1", "alice")], ["op", "id", "name"])
        consumer.process_batch(df, batch_id=0)

        assert len(written_dfs) == 1
        row = written_dfs[0].collect()[0]
        assert row["name"] == "ALICE"
