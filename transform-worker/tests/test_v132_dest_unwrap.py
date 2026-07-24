"""v1.3.2 Bug A + Bug B — integration tests: dest unwrap before IcebergWriter."""
from __future__ import annotations
import os, sys, unittest
from unittest.mock import MagicMock, patch
_TW_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _TW_DIR not in sys.path: sys.path.insert(0, _TW_DIR)
_NESSIE_DEST = {"connector_type": "iceberg", "connection_config": {
    "catalog_type": "nessie", "nessie_uri": "http://nessie:19120/iceberg",
    "nessie_ref": "main", "warehouse": "s3://lake/wh",
    "s3_endpoint": "http://minio:9000", "s3_access_key_id": "minio",
    "s3_secret_access_key": "minio123", "s3_region": "us-east-1", "auth_mode": "static"}}
def _make_initial_load_task():
    from loader import InitialLoadTask
    task = InitialLoadTask.__new__(InitialLoadTask)
    task.engine = None; task.redis = None
    task._current_connection_id = "c"; task._current_pk_col = "id"
    task._current_retry_count = 0; task._bulk_transform_logged = set()
    return task
class _CapturingWriter:
    captured = None
    def __init__(self, dest_config, redis_client=None, connection_id=None):
        _CapturingWriter.captured = dest_config
    def write_arrow(self, arrow_tbl, table_name=None, pk_col=None):
        return int(arrow_tbl.num_rows)
    def write_arrow_to_file(self, arrow_tbl, table_name=None, partition_id=None, chunk_seq=None, pk_range=None, pk_col=None, **kwargs):
        return "/tmp/fake-0-uuid.parquet"
class TestWriteArrowToIcebergDestUnwrap(unittest.TestCase):
    def setUp(self): _CapturingWriter.captured = None
    def test_write_arrow_to_iceberg_unwraps_connection_config(self):
        import pyarrow as pa
        task = _make_initial_load_task()
        arrow_tbl = pa.table({"id": [1, 2, 3], "v": ["a", "b", "c"]})
        with patch("iceberg_writer.IcebergWriter", _CapturingWriter):
            n = task._write_arrow_to_iceberg(arrow_tbl, _NESSIE_DEST, "t")
        self.assertEqual(n, 3)
        self.assertIsNotNone(_CapturingWriter.captured)
        self.assertIn("catalog_type", _CapturingWriter.captured, "Bug A: RAW wrapper passed")
        self.assertEqual(_CapturingWriter.captured["catalog_type"], "nessie")
        self.assertNotIn("connector_type", _CapturingWriter.captured)
        self.assertIs(_CapturingWriter.captured, _NESSIE_DEST["connection_config"])
    def test_write_arrow_to_iceberg_falls_back_to_dest_when_flat(self):
        import pyarrow as pa
        task = _make_initial_load_task()
        flat_dest = {"catalog_type": "nessie", "nessie_uri": "http://x"}
        with patch("iceberg_writer.IcebergWriter", _CapturingWriter):
            task._write_arrow_to_iceberg(pa.table({"id": [1], "v": ["a"]}), flat_dest, "t")
        self.assertIs(_CapturingWriter.captured, flat_dest)
class TestStageArrowToPendingDestUnwrap(unittest.TestCase):
    def setUp(self): _CapturingWriter.captured = None
    def test_stage_arrow_to_pending_unwraps_connection_config(self):
        import pyarrow as pa
        task = _make_initial_load_task()
        arrow_tbl = pa.table({"id": [1, 2, 3], "v": ["a", "b", "c"]})
        with patch("iceberg_writer.IcebergWriter", _CapturingWriter), patch("iceberg_committer.enqueue_pending_file") as eq:
            path = task._stage_arrow_to_pending(arrow_tbl, dest=_NESSIE_DEST, table_name="t",
                partition_id="0", chunk_seq=0, pk_range=(1, 3), stream_id="s",
                source_table="src", connection_id="c")
        self.assertEqual(path, "/tmp/fake-0-uuid.parquet")
        self.assertIsNotNone(_CapturingWriter.captured)
        self.assertIn("catalog_type", _CapturingWriter.captured, "Bug B: RAW wrapper passed")
        self.assertEqual(_CapturingWriter.captured["catalog_type"], "nessie")
        self.assertNotIn("connector_type", _CapturingWriter.captured)
        self.assertIs(_CapturingWriter.captured, _NESSIE_DEST["connection_config"])
        self.assertTrue(eq.called)
class TestRealLoadCatalogDestUnwrap(unittest.TestCase):
    @staticmethod
    def _install_fake_pyiceberg():
        import types
        m = types.ModuleType("pyiceberg.catalog")
        fl = MagicMock(name="pyiceberg.load_catalog")
        m.load_catalog = fl
        pm = types.ModuleType("pyiceberg"); pm.catalog = m
        sys.modules["pyiceberg"] = pm; sys.modules["pyiceberg.catalog"] = m
        def _c():
            sys.modules.pop("pyiceberg", None); sys.modules.pop("pyiceberg.catalog", None)
        return fl, _c
    def test_unwrapped_dict_picks_nessie_branch_no_keyerror(self):
        from iceberg_writer import load_catalog
        fl, c = self._install_fake_pyiceberg()
        try: cat = load_catalog(_NESSIE_DEST["connection_config"])
        finally: c()
        self.assertIs(cat, fl.return_value); fl.assert_called_once()
        _, kw = fl.call_args
        self.assertEqual(kw.get("uri"), "http://nessie:19120/iceberg")
        self.assertEqual(kw.get("ref"), "main")
        self.assertEqual(kw.get("warehouse"), "s3://lake/wh")
    def test_wrapper_dict_raises_keyerror_catalog_uri(self):
        from iceberg_writer import load_catalog
        fl, c = self._install_fake_pyiceberg()
        try:
            with self.assertRaises(KeyError) as cm: load_catalog(_NESSIE_DEST)
        finally: c()
        self.assertEqual(str(cm.exception), "'catalog_uri'")
        fl.assert_not_called()
    def test_unwrapped_rest_dict_picks_rest_branch_no_keyerror(self):
        from iceberg_writer import load_catalog
        fl, c = self._install_fake_pyiceberg()
        try: cat = load_catalog({"catalog_type": "rest", "catalog_uri": "http://rest:8181", "warehouse": "s3://lake/wh"})
        finally: c()
        self.assertIs(cat, fl.return_value)
        _, kw = fl.call_args
        self.assertEqual(kw.get("uri"), "http://rest:8181")
if __name__ == "__main__": unittest.main()
