"""v1.2.38 regression tests - engine perf Findings A + B (master report §6f).

Finding A: ``DuckDBTransformEngine.execute_pipeline`` previously opened a
fresh ``:memory:`` DuckDB connection on every chunk (~7.6ms/chunk of pure
connection-setup overhead, ~1s across a 1.29M-row table). The connection is
now pooled on the engine instance (``_get_conn`` / ``_pooled_conn``) and
reused across calls, with ``CREATE OR REPLACE TABLE staging`` keeping each
chunk's staging schema fresh.

Finding B: ``execute_pipeline`` previously did Arrow->DuckDB->Arrow->
``.to_pylist()``->``pending_rows.extend()``->``_rows_to_arrow()`` again -
~47ms/10k-row chunk of pure wasted conversion. The new
``execute_pipeline_arrow`` method returns the ``pa.Table`` directly, and
the transformed+iceberg write path in ``loader.py`` now buffers Arrow
tables and flushes via ``_flush_iceberg_batch_arrow`` -> ``write_arrow``
(no Python dict intermediate).
"""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

import pyarrow as pa

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _make_engine():
    from engine import DuckDBTransformEngine
    return DuckDBTransformEngine(
        metadata_db_dsn="sqlite://",
        encryption_key="k",
        control_plane_url="http://control-plane",
        worker_id="w-test",
    )


class TestFindingAConnectionPooling(unittest.TestCase):
    """Finding A: one DuckDB connection is reused across multiple
    execute_pipeline / execute_pipeline_arrow calls."""

    def test_pooled_conn_reused_across_calls(self):
        engine = _make_engine()
        rows1 = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        rows2 = [{"id": 3, "name": "c"}, {"id": 4, "name": "d"}]

        # First call lazily creates the pooled connection.
        engine.execute_pipeline(rows1, [])
        conn_after_first = engine._pooled_conn
        self.assertIsNotNone(conn_after_first, "pooled conn must be created on first call")

        # Second call must reuse the SAME connection (no new :memory: open).
        with patch("duckdb.connect") as mock_connect:
            engine.execute_pipeline(rows2, [])
            self.assertFalse(mock_connect.called,
                              "Finding A: duckdb.connect must NOT be called again - "
                              "the pooled connection is reused")
        self.assertIs(engine._pooled_conn, conn_after_first,
                       "the pooled connection object must be the same across calls")

    def test_pooled_conn_reused_for_arrow_path(self):
        engine = _make_engine()
        rows1 = [{"id": 1, "v": "a"}]
        rows2 = [{"id": 2, "v": "b"}]

        engine.execute_pipeline_arrow(rows1, [])
        conn_after_first = engine._pooled_conn
        self.assertIsNotNone(conn_after_first)

        with patch("duckdb.connect") as mock_connect:
            engine.execute_pipeline_arrow(rows2, [])
            self.assertFalse(mock_connect.called)
        self.assertIs(engine._pooled_conn, conn_after_first)

    def test_close_resets_pooled_conn(self):
        engine = _make_engine()
        engine.execute_pipeline([{"id": 1}], [])
        self.assertIsNotNone(engine._pooled_conn)
        engine.close()
        self.assertIsNone(engine._pooled_conn)
        # After close, a new call lazily creates a fresh connection.
        engine.execute_pipeline([{"id": 2}], [])
        self.assertIsNotNone(engine._pooled_conn)

    def test_staging_table_is_fresh_each_call(self):
        """CREATE OR REPLACE TABLE staging keeps the staging schema fresh
        across pooled-conn reuses - a column added by a transform in chunk 1
        must NOT leak into chunk 2's staging schema."""
        engine = _make_engine()
        rows1 = [{"id": 1, "name": "a"}]
        # chunk 1: add a column via a cast step
        steps1 = [{"type": "cast", "column": "id",
                   "to_type": "long", "output_column": "id_big"}]
        transformed1, _, schema1 = engine.execute_pipeline(rows1, steps1)
        self.assertIn("id_big", {f.name for f in schema1})

        # chunk 2: same rows, NO steps - staging must NOT have id_big.
        rows2 = [{"id": 2, "name": "b"}]
        transformed2, _, schema2 = engine.execute_pipeline(rows2, [])
        field_names = {f.name for f in schema2}
        self.assertNotIn("id_big", field_names,
                          "staging must be fresh per chunk (CREATE OR REPLACE)")


class TestFindingBExecutePipelineArrow(unittest.TestCase):
    """Finding B: execute_pipeline_arrow returns a pa.Table directly,
    skipping the .to_pylist() + _rows_to_arrow round-trip."""

    def test_returns_arrow_table_not_list(self):
        engine = _make_engine()
        rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        arrow_out, child_tables, schema = engine.execute_pipeline_arrow(rows, [])
        self.assertIsInstance(arrow_out, pa.Table,
                              "execute_pipeline_arrow must return a pa.Table")
        self.assertEqual(arrow_out.num_rows, 2)
        self.assertEqual(child_tables, {})
        self.assertEqual(schema.field("id").type, pa.int64())

    def test_empty_rows_returns_empty_arrow_table(self):
        engine = _make_engine()
        arrow_out, child_tables, schema = engine.execute_pipeline_arrow([], [])
        self.assertIsInstance(arrow_out, pa.Table)
        self.assertEqual(arrow_out.num_rows, 0)
        self.assertEqual(child_tables, {})

    def test_transforms_applied_in_arrow_path(self):
        """A cast step must produce the new column in the returned Arrow table."""
        engine = _make_engine()
        rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        steps = [{"type": "string_op", "column": "name", "op": "upper"}]
        arrow_out, _, schema = engine.execute_pipeline_arrow(rows, steps)
        self.assertEqual(arrow_out.num_rows, 2)
        # upper(name) column should hold "A", "B"
        col = arrow_out.column("name").to_pylist()
        self.assertEqual(col, ["A", "B"])

    def test_arrow_path_and_dict_path_produce_same_data(self):
        """execute_pipeline (list[dict]) and execute_pipeline_arrow (pa.Table)
        must produce equivalent data for the same input + steps."""
        engine = _make_engine()
        rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        steps = [{"type": "string_op", "column": "name", "op": "upper"}]
        dict_out, _, _ = engine.execute_pipeline(rows, steps)
        arrow_out, _, _ = engine.execute_pipeline_arrow(rows, steps)
        self.assertEqual(arrow_out.to_pylist(), dict_out)


class TestFindingBLoaderArrowFlushPath(unittest.TestCase):
    """Finding B: the transformed+iceberg write path in loader.py now
    buffers Arrow tables and flushes via _flush_iceberg_batch_arrow
    (write_arrow), not pending_rows + _flush_iceberg_batch (write_batch)."""

    def _make_task(self):
        from loader import InitialLoadTask
        engine = MagicMock()
        engine.control_plane_url = "http://cp"
        engine.metadata_db_dsn = "sqlite://"
        engine.encryption_key = "x" * 32
        engine.worker_id = "w"
        # execute_pipeline_arrow returns a real Arrow table so the loader's
        # pa.concat_tables path is exercised with real data.
        def fake_arrow(rows, steps, schema=None):
            return pa.Table.from_pylist(rows, schema=schema), {}, (
                schema if schema is not None
                else pa.Table.from_pylist(rows).schema
            )
        engine.execute_pipeline_arrow.side_effect = fake_arrow
        task = InitialLoadTask(engine=engine, redis_client=MagicMock())
        task._get_last_checkpoint = MagicMock(return_value=None)
        task._report_checkpoint = MagicMock()
        return task, engine

    def test_iceberg_path_uses_write_arrow_not_write_batch(self):
        from loader import InitialLoadTask, STOP_EVENT
        STOP_EVENT.clear()
        task, engine = self._make_task()

        chunks = [[{"id": 1, "n": "a"}, {"id": 2, "n": "b"}]]
        call_count = {"n": 0}

        def fake_fetch(source, schema, table, pk, last_pk, size, ctype, pk_end=None, conn=None):
            if call_count["n"] < len(chunks):
                r = chunks[call_count["n"]]
                call_count["n"] += 1
                return r
            return []
        task._fetch_chunk = MagicMock(side_effect=fake_fetch)
        task._extract_pk = lambda row, pk, ctype: row.get(pk)
        # The iceberg path must call _write_arrow_to_iceberg (Arrow-native).
        task._write_arrow_to_iceberg = MagicMock(return_value=2)
        task._write_to_iceberg = MagicMock(return_value=0)

        with patch("loader.INITIAL_LOAD_COMMIT_BATCH", 1), \
             patch("iceberg_writer._get_source_schema") as spy:
            spy.return_value = pa.schema([
                pa.field("id", pa.int64()), pa.field("n", pa.string())])
            task.run({
                "connection_id": "c1", "stream_id": "s1",
                "source": {"connector_type": "mysql", "host": "h",
                            "database_name": "db", "username": "u",
                            "password": "p"},
                "source_schema": "db", "source_table": "users",
                "destination": {"connector_type": "iceberg",
                                  "connection_config": {}},
                "dest_table": "users", "chunk_size": 2,
                # Add a transform step so execute_pipeline_arrow is exercised
                # (with no steps, the loader wraps rows in Arrow directly
                # without calling the engine).
                "transform_steps": [
                    {"type": "string_op", "column": "n", "op": "upper"},
                ],
            })

        # Finding B: the iceberg path must use the Arrow write path.
        self.assertGreater(task._write_arrow_to_iceberg.call_count, 0,
                           "iceberg path must call _write_arrow_to_iceberg")
        self.assertEqual(task._write_to_iceberg.call_count, 0,
                          "iceberg path must NOT call the dict-based _write_to_iceberg "
                          "(Finding B: transformed chunks stay in Arrow)")
        # And execute_pipeline_arrow (not execute_pipeline) was used.
        self.assertGreater(engine.execute_pipeline_arrow.call_count, 0)


if __name__ == "__main__":
    unittest.main()
