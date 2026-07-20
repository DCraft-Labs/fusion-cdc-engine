"""
Tests for writers/iceberg_writer.py — 5 tests

The SparkSession.sql() method is mocked so no real Iceberg catalog is needed.
"""
from unittest.mock import MagicMock, call, patch

import pytest

from writers.iceberg_writer import IcebergWriter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_writer(spark_mock, pk_columns=None):
    return IcebergWriter(
        spark=spark_mock,
        catalog="nessie",
        namespace="finance",
        table="orders",
        pk_columns=pk_columns or ["id"],
    )


def _make_mock_spark(spark, table_exists=False):
    """Wrap the real Spark session with a mock that intercepts .sql() calls."""
    mock = MagicMock(wraps=spark)
    if not table_exists:
        # First DESCRIBE TABLE raises to trigger auto-create
        mock.sql.side_effect = [Exception("Table not found"), None, None, None, None, None]
    else:
        mock.sql.side_effect = None
        mock.sql.return_value = MagicMock()
    return mock


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestMergeInsert:
    def test_merge_insert_calls_sql_with_merge_statement(self, spark):
        data = [("c", "1", "Alice", 100.0)]
        df = spark.createDataFrame(data, ["op", "id", "name", "amount"])

        mock_spark = MagicMock()
        writer = _make_writer(mock_spark)
        writer._table_created = True  # skip auto-create

        writer.write_batch(df, batch_id=0)

        mock_spark.sql.assert_called_once()
        sql = mock_spark.sql.call_args[0][0]
        assert "MERGE INTO" in sql
        assert "nessie.finance.orders" in sql
        assert "WHEN NOT MATCHED AND s.op = 'c' THEN INSERT *" in sql


class TestMergeUpdate:
    def test_merge_update_sql_contains_update_clause(self, spark):
        data = [("u", "1", "Bob", 200.0)]
        df = spark.createDataFrame(data, ["op", "id", "name", "amount"])

        mock_spark = MagicMock()
        writer = _make_writer(mock_spark)
        writer._table_created = True

        writer.write_batch(df, batch_id=1)

        sql = mock_spark.sql.call_args[0][0]
        assert "WHEN MATCHED AND s.op = 'u' THEN UPDATE SET *" in sql


class TestMergeDelete:
    def test_merge_delete_sql_contains_delete_clause(self, spark):
        from pyspark.sql.types import StringType, StructField, StructType
        schema = StructType([
            StructField("op", StringType(), True),
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("amount", StringType(), True),
        ])
        data = [("d", "42", None, None)]
        df = spark.createDataFrame(data, schema)

        mock_spark = MagicMock()
        writer = _make_writer(mock_spark)
        writer._table_created = True

        writer.write_batch(df, batch_id=2)

        sql = mock_spark.sql.call_args[0][0]
        assert "WHEN MATCHED AND s.op = 'd' THEN DELETE" in sql


class TestTableAutoCreated:
    def test_table_auto_created_on_first_batch(self, spark):
        """When DESCRIBE TABLE fails, CREATE TABLE is issued before MERGE INTO."""
        data = [("c", "1", "Alice", 100.0)]
        df = spark.createDataFrame(data, ["op", "id", "name", "amount"])

        mock_spark = MagicMock()
        # DESCRIBE → raises → triggers CREATE TABLE
        mock_spark.sql.side_effect = [Exception("Table does not exist"), None, None]

        writer = _make_writer(mock_spark)
        writer.write_batch(df, batch_id=0)

        sql_calls = [c[0][0].upper() for c in mock_spark.sql.call_args_list]
        assert any("DESCRIBE" in s for s in sql_calls)
        assert any("CREATE TABLE" in s for s in sql_calls)
        assert any("MERGE INTO" in s for s in sql_calls)


class TestWriteIsAtomic:
    def test_write_is_atomic_no_partial_writes(self, spark):
        """All rows in a batch are passed to a single MERGE INTO call (atomic)."""
        data = [("c", str(i), f"u{i}", float(i)) for i in range(10)]
        df = spark.createDataFrame(data, ["op", "id", "name", "amount"])

        mock_spark = MagicMock()
        writer = _make_writer(mock_spark)
        writer._table_created = True

        writer.write_batch(df, batch_id=0)

        # Only ONE sql call (the MERGE) — not one per row
        assert mock_spark.sql.call_count == 1
        sql = mock_spark.sql.call_args[0][0]
        assert "MERGE INTO" in sql
