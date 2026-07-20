"""
Tests for consumer/batch_consumer.py — 5 tests

Redis and DB interactions are mocked.
"""
from unittest.mock import MagicMock, patch

import pytest

from consumer.batch_consumer import BatchConsumer
from consumer.redis_source import CDC_EVENT_FIELDS


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


def _empty_df(spark):
    from pyspark.sql.types import StringType, StructField, StructType
    schema = StructType([StructField(f, StringType(), True) for f in CDC_EVENT_FIELDS])
    return spark.createDataFrame([], schema)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestBatchConsumerSetup:
    def test_setup_initialises_all_components(self, spark):
        consumer = BatchConsumer(_base_config())
        consumer.setup(spark=spark)

        assert consumer.spark is not None
        assert consumer.redis_source is not None
        assert consumer.writer is not None


class TestBatchConsumerRun:
    def test_run_returns_zero_when_no_events(self, spark):
        """When Redis has no pending events, result is {processed: 0, failed: 0}."""
        consumer = BatchConsumer(_base_config())
        consumer.setup(spark=spark)

        # Mock redis_source to return empty DF
        consumer.redis_source = MagicMock()
        consumer.redis_source.read_batch.return_value = _empty_df(spark)

        result = consumer.run()

        assert result["processed"] == 0
        assert result["failed"] == 0

    def test_run_calls_writer_when_events_present(self, spark):
        """Batch consumer writes processed rows to destination."""
        consumer = BatchConsumer(_base_config())
        consumer.setup(spark=spark)

        # Provide one CDC event row
        data = [{f: "x" for f in CDC_EVENT_FIELDS}]
        data[0]["op"] = "c"
        df = spark.createDataFrame(
            [tuple(data[0].values())],
            list(data[0].keys()),
        )
        consumer.redis_source = MagicMock()
        consumer.redis_source.read_batch.return_value = df
        consumer.redis_source.ensure_groups = MagicMock()

        mock_writer = MagicMock()
        consumer.writer = mock_writer

        consumer.run()

        mock_writer.upsert_batch.assert_called_once()

    def test_run_acks_redis_after_write(self, spark):
        """After a successful write, pending Redis messages are ACKed."""
        consumer = BatchConsumer(_base_config())
        consumer.setup(spark=spark)

        df = spark.createDataFrame(
            [tuple("x" for _ in CDC_EVENT_FIELDS)],
            CDC_EVENT_FIELDS,
        )
        consumer.redis_source = MagicMock()
        consumer.redis_source.read_batch.return_value = df
        consumer.redis_source.ensure_groups = MagicMock()
        consumer.writer = MagicMock()

        consumer.run()

        consumer.redis_source.ack.assert_called_once()

    def test_run_with_transform_applies_pipeline(self, spark):
        """Batch consumer applies the transform pipeline before writing."""
        config = _base_config()
        config["transform_spec"] = {"transforms": [
            {"type": "string_op", "column": "op", "op": "upper", "output_column": "op_upper"}
        ]}
        consumer = BatchConsumer(config)
        consumer.setup(spark=spark)

        written_dfs = []
        df = spark.createDataFrame(
            [tuple("x" for _ in CDC_EVENT_FIELDS)],
            CDC_EVENT_FIELDS,
        )
        consumer.redis_source = MagicMock()
        consumer.redis_source.read_batch.return_value = df
        consumer.redis_source.ensure_groups = MagicMock()
        consumer.redis_source.ack = MagicMock()

        mock_writer = MagicMock()
        mock_writer.upsert_batch = lambda d, batch_id: written_dfs.append(d)
        consumer.writer = mock_writer

        consumer.run()

        assert len(written_dfs) == 1
        assert "op_upper" in written_dfs[0].columns
