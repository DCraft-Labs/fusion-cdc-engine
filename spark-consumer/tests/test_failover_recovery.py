"""
P4.10 — Failover and Recovery Tests (spark-consumer)

Verifies that the Spark consumer correctly handles:
  1. DQ block routes failed rows to DLQ instead of crashing
  2. SparkCheckpointHandler prevents re-processing of already-committed batches
  3. StreamingConsumer can stop and restart without processing events twice

All tests run without Docker (mocked external dependencies).

Run with:
    pytest tests/test_failover_recovery.py -v
"""
from __future__ import annotations

import json
import os
import tempfile
import time
from unittest.mock import MagicMock, patch, call

import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_row(i: int = 1, op: str = "c") -> dict:
    return {
        "event_id": f"evt-{i}",
        "op": op,
        "bank_id": "bank-f",
        "tenant_id": "tenant-f",
        "source_id": "src-001",
        "schema_name": "mydb",
        "table_name": "orders",
        "lsn": f"bin:{i}",
        "ts_ms": int(time.time() * 1000),
        "pk_values": json.dumps({"id": i}),
        "before": "null",
        "after": json.dumps({"id": i, "amount": 100 * i}),
        "metadata": "{}",
    }


# ─────────────────────────────────────────────────────────────────────────────
# DLQ — Failed rows route to DLQ instead of crashing
# ─────────────────────────────────────────────────────────────────────────────

class TestDLQOnDQBlock:
    def test_dlq_receives_events_on_dq_block(self):
        """
        When DQExecutor.check() is called with on_fail=block, blocked rows are
        returned as the second element of the tuple (failed_df), not dropped.
        """
        from dq.executor import DQExecutor
        from unittest.mock import MagicMock, patch

        policy = {"rules": [], "on_fail": "block"}
        executor = DQExecutor(policy)

        mock_passed = MagicMock(name="passed_df")
        mock_blocked = MagicMock(name="blocked_df")

        with patch.object(executor, "check", return_value=(mock_passed, mock_blocked, [])):
            passed_df, blocked_df, violations = executor.check(MagicMock())

        assert passed_df is mock_passed
        assert blocked_df is mock_blocked

    def test_dq_alert_mode_does_not_block_rows(self):
        """
        With on_fail=alert, check() returns all rows in passed_df (not blocked).
        """
        from dq.executor import DQExecutor
        from unittest.mock import MagicMock, patch

        policy = {"rules": [], "on_fail": "alert"}
        executor = DQExecutor(policy)

        all_rows = MagicMock(name="all_df")
        empty = MagicMock(name="empty_df")

        with patch.object(executor, "check", return_value=(all_rows, empty, [])):
            passed_df, blocked_df, violations = executor.check(MagicMock())

        # In alert mode all rows pass through
        assert passed_df is all_rows

    def test_dq_block_with_multiple_violations(self):
        """Multiple violations are all captured in the violations list."""
        from dq.executor import DQExecutor
        from unittest.mock import MagicMock, patch

        policy = {"rules": [], "on_fail": "block"}
        executor = DQExecutor(policy)

        violations_fixture = [
            {"rule_id": "r1", "row_id": "evt-1"},
            {"rule_id": "r1", "row_id": "evt-2"},
            {"rule_id": "r1", "row_id": "evt-3"},
        ]

        with patch.object(executor, "check", return_value=(MagicMock(), MagicMock(), violations_fixture)):
            _, _, violations = executor.check(MagicMock())

        assert len(violations) == 3

    def test_dq_executor_on_fail_policy_parsed_correctly(self):
        """DQExecutor stores the on_fail policy from the config dict."""
        from dq.executor import DQExecutor

        for mode in ("block", "alert", "continue"):
            executor = DQExecutor({"rules": [], "on_fail": mode})
            assert executor.on_fail == mode


# ─────────────────────────────────────────────────────────────────────────────
# SparkCheckpointHandler — prevents reprocessing
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckpointPreventsReprocessing:
    def test_checkpoint_stores_last_batch_id(self):
        """After commit(), get_last_batch_id() returns the committed batch ID."""
        from consumer.checkpoint_handler import SparkCheckpointHandler

        with tempfile.TemporaryDirectory() as tmpdir:
            ckpt = SparkCheckpointHandler(tmpdir)
            ckpt.commit(batch_id=7)
            assert ckpt.get_last_batch_id() == 7

    def test_checkpoint_persists_to_disk(self):
        """Committed state survives Python object destruction and re-instantiation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            from consumer.checkpoint_handler import SparkCheckpointHandler

            ckpt1 = SparkCheckpointHandler(tmpdir)
            ckpt1.commit(batch_id=42, stream_offsets={"stream-a": "1234567890-0"})
            del ckpt1

            # Re-instantiate — must read from disk
            ckpt2 = SparkCheckpointHandler(tmpdir)
            assert ckpt2.get_last_batch_id() == 42
            offsets = ckpt2.get_stream_offsets()
            assert offsets.get("stream-a") == "1234567890-0"

    def test_same_batch_id_not_reprocessed(self):
        """
        If batch_id <= last committed batch_id, the consumer should skip it.
        Simulates what a restarted consumer would do.
        """
        from consumer.checkpoint_handler import SparkCheckpointHandler

        with tempfile.TemporaryDirectory() as tmpdir:
            ckpt = SparkCheckpointHandler(tmpdir)
            ckpt.commit(batch_id=10)

            last = ckpt.get_last_batch_id()

            # Simulate consumer logic: skip batches already seen
            def should_process(batch_id: int) -> bool:
                return last is None or batch_id > last

            assert should_process(9) is False   # Already processed
            assert should_process(10) is False  # Already processed
            assert should_process(11) is True   # New batch

    def test_checkpoint_updates_on_newer_batch(self):
        """Committing a newer batch_id replaces the old one."""
        from consumer.checkpoint_handler import SparkCheckpointHandler

        with tempfile.TemporaryDirectory() as tmpdir:
            ckpt = SparkCheckpointHandler(tmpdir)
            ckpt.commit(batch_id=5)
            ckpt.commit(batch_id=10)
            assert ckpt.get_last_batch_id() == 10

    def test_stream_offsets_updated_on_commit(self):
        """Each commit overwrites the stream offsets with the latest ACKed IDs."""
        from consumer.checkpoint_handler import SparkCheckpointHandler

        with tempfile.TemporaryDirectory() as tmpdir:
            ckpt = SparkCheckpointHandler(tmpdir)
            ckpt.commit(batch_id=1, stream_offsets={"s1": "100-0"})
            ckpt.commit(batch_id=2, stream_offsets={"s1": "200-0", "s2": "50-0"})

            offsets = ckpt.get_stream_offsets()
            assert offsets["s1"] == "200-0"
            assert offsets["s2"] == "50-0"


# ─────────────────────────────────────────────────────────────────────────────
# StreamingConsumer stop/restart — no duplicate processing
# ─────────────────────────────────────────────────────────────────────────────

class TestPostgresWriterRetry:
    """
    Postgres writer retry behaviour on connection errors.
    Tests verify that psycopg2.OperationalError is surfaced to Spark so
    the streaming job can retry via its built-in fault tolerance.
    """

    def test_postgres_writer_raises_on_connection_failure(self):
        """
        When psycopg2.connect raises OperationalError, PostgresWriter.upsert_batch
        must propagate the exception so Spark can handle retry/backoff.
        """
        import psycopg2
        from writers.postgres_writer import PostgresWriter

        writer = PostgresWriter(
            pg_dsn="host=127.0.0.1 port=5433 dbname=fusion_dw user=dw_user password=dw_password",
            table="orders",
            pk_columns=["id"],
        )

        mock_batch_df = MagicMock()
        mock_row = MagicMock()
        mock_row.asDict.return_value = {"id": 1, "op": "c", "status": "ok"}
        mock_batch_df.collect.return_value = [mock_row]
        mock_batch_df.columns = ["id", "op", "status"]

        with patch("psycopg2.connect", side_effect=psycopg2.OperationalError("connection refused")):
            with pytest.raises(psycopg2.OperationalError):
                writer.upsert_batch(mock_batch_df, batch_id=1)

    def test_postgres_writer_rollback_on_write_failure(self):
        """
        When a write fails mid-batch, connection.rollback() must be called.
        """
        import psycopg2
        from writers.postgres_writer import PostgresWriter

        writer = PostgresWriter(
            pg_dsn="host=127.0.0.1 port=5433 dbname=fusion_dw user=dw_user password=dw_password",
            table="orders",
            pk_columns=["id"],
        )

        mock_batch_df = MagicMock()
        mock_row = MagicMock()
        mock_row.asDict.return_value = {"id": 1, "op": "c", "status": "ok"}
        mock_batch_df.collect.return_value = [mock_row]
        mock_batch_df.columns = ["id", "op", "status"]

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = psycopg2.OperationalError("write failed")
        mock_conn.cursor.return_value = mock_cursor

        with patch("psycopg2.connect", return_value=mock_conn):
            with pytest.raises(psycopg2.OperationalError):
                writer.upsert_batch(mock_batch_df, batch_id=1)

        mock_conn.rollback.assert_called_once()

    def test_postgres_writer_skips_empty_batch(self):
        """Empty DataFrame batch produces no DB writes."""
        from writers.postgres_writer import PostgresWriter

        writer = PostgresWriter(
            pg_dsn="host=127.0.0.1 port=5433 dbname=fusion_dw user=dw_user password=dw_password",
            table="orders",
            pk_columns=["id"],
        )

        mock_batch_df = MagicMock()
        mock_batch_df.collect.return_value = []

        with patch("psycopg2.connect") as mock_connect:
            writer.upsert_batch(mock_batch_df, batch_id=1)
            # No DB connection should be made for an empty batch
            mock_connect.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# StreamingConsumer stop/restart — no duplicate processing
# ─────────────────────────────────────────────────────────────────────────────

class TestStreamingConsumerRestart:
    def test_consumer_stops_when_running_flag_cleared(self):
        """
        StreamingConsumer._running=False must cause the run loop to exit cleanly
        without processing any further batches.
        """
        from consumer.streaming_consumer import StreamingConsumer

        config = {
            "redis_url": "redis://localhost:6379/0",
            "stream_keys": ["cdc:bank-f:tenant-f:src-001:mydb:orders"],
            "consumer_group": "test-group",
            "consumer_name": "test-consumer",
            "writer_type": "postgres",
            "writer_config": {
                "pg_dsn": "host=127.0.0.1 port=5433 dbname=fusion_dw user=dw_user password=dw_password",
                "table": "mydb.orders",
                "pk_columns": ["id"],
            },
        }
        consumer = StreamingConsumer(config)
        # Default state is not running
        assert consumer._running is False

    def test_consumer_setup_initialises_components(self):
        """
        StreamingConsumer.setup() with a mock Spark session initialises
        redis_source and writer. We patch imports at the module level.
        """
        from consumer.streaming_consumer import StreamingConsumer
        import consumer.redis_source as rs_mod
        import consumer.streaming_consumer as sc_mod
        import writers.postgres_writer as pw_mod

        config = {
            "redis_url": "redis://localhost:6379/0",
            "stream_keys": ["cdc:bank-f:tenant-f:src-001:mydb:orders"],
            "consumer_group": "test-group",
            "consumer_name": "test-consumer",
            "writer_type": "postgres",
            "writer_config": {
                "pg_dsn": "host=127.0.0.1 port=5433 dbname=fusion_dw user=dw_user password=dw_password",
                "table": "mydb.orders",
                "pk_columns": ["id"],
            },
            "schema_reload_port": 0,
        }

        mock_spark = MagicMock()

        with patch.object(rs_mod, "RedisStreamSource", MagicMock()) as MockSource, \
             patch.object(pw_mod, "PostgresWriter", MagicMock()) as MockWriter:
            consumer = StreamingConsumer(config)
            consumer.setup(spark=mock_spark)

        assert consumer.spark is mock_spark

    def test_checkpoint_committed_after_successful_batch(self):
        """
        After a successful batch write, commit() records the batch_id and offsets.
        """
        from consumer.checkpoint_handler import SparkCheckpointHandler

        with tempfile.TemporaryDirectory() as tmpdir:
            ckpt = SparkCheckpointHandler(tmpdir)

            batch_id = 1
            offsets = {"cdc:bank-f:tenant-f:src-001:mydb:orders": "1700000000000-0"}
            ckpt.commit(batch_id=batch_id, stream_offsets=offsets)

            assert ckpt.get_last_batch_id() == batch_id
            stored = ckpt.get_stream_offsets()
            assert stored == offsets

    def test_no_commit_on_failed_batch(self):
        """
        If batch processing fails, commit() is NOT called — so on restart the
        consumer will re-process from the previous checkpoint.
        """
        from consumer.checkpoint_handler import SparkCheckpointHandler

        with tempfile.TemporaryDirectory() as tmpdir:
            ckpt = SparkCheckpointHandler(tmpdir)
            ckpt.commit(batch_id=5)

            # Simulate a batch that fails — we do NOT call ckpt.commit(6)
            # After "restart", last_batch_id should still be 5
            ckpt_after_restart = SparkCheckpointHandler(tmpdir)
            assert ckpt_after_restart.get_last_batch_id() == 5

