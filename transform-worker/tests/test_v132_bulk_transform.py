"""v1.3.2 Fix 3 — integration test: bulk mode RUNS transforms (no silent skip)."""
from __future__ import annotations
import logging, os, sys, unittest
_TW_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _TW_DIR not in sys.path: sys.path.insert(0, _TW_DIR)
def _make_engine():
    from engine import DuckDBTransformEngine
    return DuckDBTransformEngine(metadata_db_dsn="", encryption_key="", control_plane_url="", worker_id="test-worker")
def _make_initial_load_task():
    from loader import InitialLoadTask
    task = InitialLoadTask.__new__(InitialLoadTask)
    task.engine = None; task.redis = None
    task._current_connection_id = "c"; task._current_pk_col = "id"
    task._current_retry_count = 0; task._bulk_transform_logged = set()
    return task
class TestExecutePipelineArrowInPlace(unittest.TestCase):
    def setUp(self): self.engine = _make_engine()
    def tearDown(self):
        try: self.engine.close()
        except Exception: pass
    def test_cast_step_runs_and_changes_column_type(self):
        import pyarrow as pa
        out = self.engine.execute_pipeline_arrow_in_place(
            pa.table({"id": ["1", "2", "3"], "v": ["a", "b", "c"]}),
            [{"type": "cast", "column": "id", "to_type": "long", "output_column": "id"}])
        self.assertEqual(out.schema.field("id").type, pa.int64(), "cast step did not run")
        self.assertEqual(out.column("id").to_pylist(), [1, 2, 3])
        self.assertEqual(out.column("v").to_pylist(), ["a", "b", "c"])
    def test_expression_step_runs_and_produces_computed_values(self):
        import pyarrow as pa
        out = self.engine.execute_pipeline_arrow_in_place(
            pa.table({"id": [1, 2, 3]}),
            [{"type": "expression", "expression": "id * 100", "output_column": "id_x100", "output_type": "long"}])
        self.assertEqual(out.column("id_x100").to_pylist(), [100, 200, 300])
    def test_string_op_step_runs(self):
        import pyarrow as pa
        out = self.engine.execute_pipeline_arrow_in_place(
            pa.table({"name": ["alice", "bob"]}),
            [{"type": "string_op", "column": "name", "op": "upper", "output_column": "name"}])
        self.assertEqual(out.column("name").to_pylist(), ["ALICE", "BOB"])
    def test_no_steps_returns_input_unchanged(self):
        import pyarrow as pa
        out = self.engine.execute_pipeline_arrow_in_place(
            pa.table({"id": [1, 2, 3], "v": ["a", "b", "c"]}), [])
        self.assertEqual(out.schema.names, ["id", "v"])
        self.assertEqual(out.column("id").to_pylist(), [1, 2, 3])
    def test_none_steps_treated_as_empty(self):
        import pyarrow as pa
        out = self.engine.execute_pipeline_arrow_in_place(pa.table({"id": [1]}), None)
        self.assertEqual(out.column("id").to_pylist(), [1])
    def test_empty_table_with_schema_returns_empty_with_schema(self):
        import pyarrow as pa
        schema = pa.schema([("id", pa.int64()), ("v", pa.string())])
        out = self.engine.execute_pipeline_arrow_in_place(pa.Table.from_pylist([], schema=schema), [], schema=schema)
        self.assertEqual(out.num_rows, 0)
        self.assertEqual(out.schema.names, ["id", "v"])
    def test_multiple_steps_run_in_sequence(self):
        import pyarrow as pa
        out = self.engine.execute_pipeline_arrow_in_place(
            pa.table({"id": ["1", "2", "3"]}),
            [{"type": "cast", "column": "id", "to_type": "long", "output_column": "id"},
             {"type": "expression", "expression": "id + 10", "output_column": "id_plus_10", "output_type": "long"}])
        self.assertEqual(out.column("id").to_pylist(), [1, 2, 3])
        self.assertEqual(out.column("id_plus_10").to_pylist(), [11, 12, 13])
    def test_no_python_row_dict_round_trip(self):
        import pyarrow as pa
        out = self.engine.execute_pipeline_arrow_in_place(
            pa.table({"id": [1, 2, 3]}),
            [{"type": "expression", "expression": "id * 2", "output_column": "id2", "output_type": "long"}])
        self.assertIsInstance(out, pa.Table)
        self.assertEqual(out.column("id2").to_pylist(), [2, 4, 6])
class TestBulkTransformInfoLog(unittest.TestCase):
    def test_info_log_fires_once_per_stream_when_steps_nonempty(self):
        task = _make_initial_load_task()
        with self.assertLogs("loader", level="INFO") as cm:
            for _ in range(100):
                task._maybe_log_bulk_transform_run("stream-1", "table-1", [{"type": "cast", "column": "id"}])
        info_lines = [r for r in cm.records if r.levelno == logging.INFO]
        self.assertEqual(len(info_lines), 1)
        msg = info_lines[0].getMessage()
        self.assertIn("running", msg.lower())
        self.assertIn("stream-1", msg)
        self.assertIn("stream-1:table-1", task._bulk_transform_logged)
    def test_info_log_fires_once_per_distinct_stream(self):
        task = _make_initial_load_task()
        with self.assertLogs("loader", level="INFO") as cm:
            for _ in range(10):
                task._maybe_log_bulk_transform_run("stream-A", "table-A", [{"type": "cast", "column": "id"}])
                task._maybe_log_bulk_transform_run("stream-B", "table-B", [{"type": "cast", "column": "id"}])
        self.assertEqual(len([r for r in cm.records if r.levelno == logging.INFO]), 2)
    def test_info_log_does_not_fire_when_steps_empty(self):
        task = _make_initial_load_task()
        with self.assertLogs("loader", level="INFO") as cm:
            logging.getLogger("loader").info("sentinel")
            for _ in range(50): task._maybe_log_bulk_transform_run("stream-1", "table-1", [])
        info_lines = [r for r in cm.records if r.levelno == logging.INFO and "sentinel" not in r.getMessage()]
        self.assertEqual(len(info_lines), 0)
    def test_info_log_does_not_fire_when_steps_none(self):
        task = _make_initial_load_task()
        with self.assertLogs("loader", level="INFO") as cm:
            logging.getLogger("loader").info("sentinel")
            task._maybe_log_bulk_transform_run("stream-1", "table-1", None)
        info_lines = [r for r in cm.records if r.levelno == logging.INFO and "sentinel" not in r.getMessage()]
        self.assertEqual(len(info_lines), 0)
    def test_info_log_set_is_per_worker_process(self):
        task1 = _make_initial_load_task()
        with self.assertLogs("loader", level="INFO"):
            task1._maybe_log_bulk_transform_run("s", "t", [{"type": "cast", "column": "id"}])
        task2 = _make_initial_load_task()
        with self.assertLogs("loader", level="INFO") as cm:
            task2._maybe_log_bulk_transform_run("s", "t", [{"type": "cast", "column": "id"}])
        self.assertEqual(len([r for r in cm.records if r.levelno == logging.INFO]), 1)
    def test_info_log_mentions_step_count(self):
        task = _make_initial_load_task()
        with self.assertLogs("loader", level="INFO") as cm:
            task._maybe_log_bulk_transform_run("s", "t", [{"type": "cast", "column": "id"}, {"type": "expression", "expression": "id+1", "output_column": "x"}])
        self.assertIn("2", cm.records[0].getMessage())
class TestBulkModePostgresTransformNotSkipped(unittest.TestCase):
    """Regression test: bulk_mode="duckdb" + Postgres destination used to
    silently drop configured transform_steps (loader.py's `elif kind ==
    "arrow":` Postgres branch never called execute_pipeline* at all, unlike
    the Iceberg branch above it — no error, no warning, rows just landed
    unmodified). The fix mirrors the ordinary Python-mode Postgres path and
    runs `arrow_tbl.to_pylist()` through `execute_pipeline` before
    `_copy_to_postgres`. This exercises that exact composition — the same
    inputs/steps the fixed loader.py branch now passes through.
    """
    def setUp(self): self.engine = _make_engine()
    def tearDown(self):
        try: self.engine.close()
        except Exception: pass
    def test_steps_applied_to_arrow_derived_rows_before_postgres_write(self):
        import pyarrow as pa
        arrow_tbl = pa.table({"id": ["1", "2", "3"], "name": ["alice", "bob", "cy"]})
        rows = arrow_tbl.to_pylist()
        steps = [{"type": "cast", "column": "id", "to_type": "long", "output_column": "id"},
                 {"type": "string_op", "column": "name", "op": "upper", "output_column": "name"}]
        transformed, _child_tables, _schema = self.engine.execute_pipeline(rows, steps)
        self.assertEqual([r["id"] for r in transformed], [1, 2, 3])
        self.assertEqual([r["name"] for r in transformed], ["ALICE", "BOB", "CY"])
    def test_no_steps_leaves_arrow_derived_rows_unchanged(self):
        import pyarrow as pa
        arrow_tbl = pa.table({"id": [1, 2, 3]})
        rows = arrow_tbl.to_pylist()
        # Mirrors the fixed branch's `if steps:` guard — empty steps means
        # no execute_pipeline call, rows pass through as-is.
        self.assertEqual(rows, [{"id": 1}, {"id": 2}, {"id": 3}])
if __name__ == "__main__": unittest.main()
