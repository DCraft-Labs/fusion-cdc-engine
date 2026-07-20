"""
Tests for writers/postgres_writer.py — 6 tests

All database interactions are mocked via psycopg2.connect patch.
"""
import time
from unittest.mock import MagicMock, patch, call

import pytest

from writers.postgres_writer import PostgresWriter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DSN = "postgresql://user:pass@localhost/testdb"


def _mock_connect():
    """Return a MagicMock that behaves like psycopg2.connect()."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestUpsertInsert:
    def test_upsert_inserts_new_row(self, spark):
        """op=c → INSERT … ON CONFLICT DO UPDATE is executed."""
        data = [("c", "1", "Alice", 100.0)]
        df = spark.createDataFrame(data, ["op", "id", "name", "amount"])

        mock_conn, mock_cursor = _mock_connect()
        with patch("writers.postgres_writer.psycopg2.connect", return_value=mock_conn):
            writer = PostgresWriter(_DSN, "orders", ["id"])
            writer.upsert_batch(df, batch_id=0)

        assert mock_cursor.execute.called
        sql = mock_cursor.execute.call_args_list[0][0][0].upper()
        assert "INSERT INTO" in sql
        assert "ON CONFLICT" in sql
        mock_conn.commit.assert_called_once()


class TestUpsertUpdate:
    def test_upsert_updates_on_conflict(self, spark):
        """op=u → same INSERT … ON CONFLICT DO UPDATE path."""
        data = [("u", "1", "Bob", 200.0)]
        df = spark.createDataFrame(data, ["op", "id", "name", "amount"])

        mock_conn, mock_cursor = _mock_connect()
        with patch("writers.postgres_writer.psycopg2.connect", return_value=mock_conn):
            writer = PostgresWriter(_DSN, "orders", ["id"])
            writer.upsert_batch(df, batch_id=1)

        sql = mock_cursor.execute.call_args_list[0][0][0].upper()
        assert "INSERT INTO" in sql
        assert "DO UPDATE SET" in sql


class TestDelete:
    def test_delete_removes_row(self, spark):
        """op=d → DELETE FROM … WHERE pk = ? is executed."""
        from pyspark.sql.types import StringType, StructField, StructType
        schema = StructType([
            StructField("op", StringType(), True),
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("amount", StringType(), True),
        ])
        data = [("d", "42", None, None)]
        df = spark.createDataFrame(data, schema)

        mock_conn, mock_cursor = _mock_connect()
        with patch("writers.postgres_writer.psycopg2.connect", return_value=mock_conn):
            writer = PostgresWriter(_DSN, "orders", ["id"])
            writer.upsert_batch(df, batch_id=2)

        sql = mock_cursor.execute.call_args_list[0][0][0].upper()
        assert "DELETE FROM" in sql
        assert "WHERE" in sql


class TestIdempotency:
    def test_idempotent_same_batch_twice(self, spark):
        """Calling upsert_batch twice with the same data uses ON CONFLICT (idempotent)."""
        data = [("u", "1", "Alice", 100.0)]
        df = spark.createDataFrame(data, ["op", "id", "name", "amount"])

        mock_conn, mock_cursor = _mock_connect()
        with patch("writers.postgres_writer.psycopg2.connect", return_value=mock_conn):
            writer = PostgresWriter(_DSN, "orders", ["id"])
            writer.upsert_batch(df, batch_id=0)
            writer.upsert_batch(df, batch_id=0)

        # Both calls should use ON CONFLICT
        for c in mock_cursor.execute.call_args_list:
            assert "ON CONFLICT" in c[0][0].upper()


class TestThroughput:
    def test_10k_rows_processed_without_error(self, spark):
        """Verify 10k-row batch is handled within a reasonable time (DB is mocked)."""
        data = [("u", str(i), f"user{i}", float(i)) for i in range(10_000)]
        df = spark.createDataFrame(data, ["op", "id", "name", "amount"])

        mock_conn, mock_cursor = _mock_connect()
        with patch("writers.postgres_writer.psycopg2.connect", return_value=mock_conn):
            writer = PostgresWriter(_DSN, "orders", ["id"])
            start = time.time()
            writer.upsert_batch(df, batch_id=0)
            elapsed = time.time() - start

        assert elapsed < 30.0
        assert mock_cursor.execute.call_count == 10_000


class TestSCD2:
    def test_scd2_creates_history_row(self, spark):
        """SCD2 mode: an UPDATE emits both an UPDATE (close old) and INSERT (new row)."""
        data = [("u", "1", "Alice", 100.0)]
        df = spark.createDataFrame(data, ["op", "id", "name", "amount"])

        mock_conn, mock_cursor = _mock_connect()
        with patch("writers.postgres_writer.psycopg2.connect", return_value=mock_conn):
            writer = PostgresWriter(_DSN, "orders", ["id"], scd2_mode=True)
            writer.upsert_batch(df, batch_id=0)

        all_sql = [c[0][0].upper() for c in mock_cursor.execute.call_args_list]
        assert any("UPDATE" in s for s in all_sql), "Expected UPDATE to close old row"
        assert any("INSERT" in s for s in all_sql), "Expected INSERT for new row"
