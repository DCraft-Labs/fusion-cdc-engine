"""v1.3.2 Bug A + Bug B — integration tests: dest unwrap before IcebergWriter."""
from __future__ import annotations
import os, sys, unittest
from unittest.mock import MagicMock, patch

_TW_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _TW_DIR not in sys.path:
    sys.path.insert(0, _TW_DIR)

_NESSIE_DEST = {
    "connector_type": "iceberg",
    "connection_config": {
        "catalog_type": "nessie",
        "nessie_uri": "http://nessie:19120/iceberg",
        "nessie_ref": "main",
        "warehouse": "s3://lake/wh",
        "s3_endpoint": "http://minio:9000",
        "s3_access_key_id": "minio",
        "s3_secret_access_key": "minio123",
        "s3_region": "us-east-1",
        "auth_mode": "static",
    },
}

def _make_initial_load_task():
    from loader import InitialLoadTask
    task = InitialLoadTask.__new__(InitialLoadTask)
    task.engine = None
    task.redis = None
    task._current_connection_id = "c"
    task._current_pk_col = "id"
    task._current_retry_count = 0
    task._bulk_transform_logged = set()
    return task

class _CapturingWriter:
    captured = None
    def __init__(self, dest_config, redis_client=None, connection_id=None):
        _CapturingWriter.captured = dest_config
    def write_arrow(self, arrow_tbl, table_name=None, pk_col=None):
        return int(arrow_tbl.num_rows)
    def write_arrow_to_file(self, arrow_tbl, table_name=None,
                            partition_id=None, chunk_seq=None, pk_range=None):
        return "/tmp/fake-0-uuid.parquet"

class TestWriteArrowToIcebergDestUnwrap(unittest.TestCase):
    def setUp(self):
        _CapturingWriter.captured = None
    def test_write_arrow_to_iceberg_unwraps_connection_config(self):
        import pyarrow as pa
        task = _make_initial_load_task()
        arrow_tbl = pa.table({"id": [1, 2, 3], "v": ["a", "b", "c"]})
        with patch("iceberg_writer.IcebergWriter", _CapturingWriter):
            n = task._write_arrow_to_iceberg(arrow_tbl, _NESSIE_DEST, "t")
        self.assertEqual(n, 3)
        self.assertIsNotNone(_CapturingWriter.captured)
        self.assertIn("catalog_type", _CapturingWriter.captured,
                       "Bug A: IcebergWriter received the RAW wrapper dict")
        self.assertEqual(_CapturingWriter.captured["catalog_type"], "nessie")
        self.assertNotIn("connector_type", _CapturingWriter.captured)
        self.assertIs(_CapturingWriter.captured, _NESSIE_DEST["connection_config"])
    def test_write_arrow_to_iceberg_falls_back_to_dest_when_flat(self):
        import pyarrow as pa
        task = _make_initial_load_task()
        flat_dest = {"catalog_type": "nessie", "nessie_uri": "http://x"}
        arrow_tbl = pa.table({"id": [1], "v": ["a"]})
        with patch("iceberg_writer.IcebergWriter", _CapturingWriter):
            task._write_arrow_to_iceberg(arrow_tbl, flat_dest, "t")
        self.assertIs(_CapturingWriter.captured, flat_dest)

class TestStageArrowToPendingDestUnwrap(unittest.TestCase):
    def setUp(self):
        _CapturingWriter.captured = None
    def test_stage_arrow_to_pending_unwraps_connection_config(self):
        import pyarrow as pa
        task = _make_initial_load_task()
        arrow_tbl = pa.table({"id": [1, 2, 3], "v": ["a", "b", "c"]})
        with patch("iceberg_writer.IcebergWriter", _CapturingWriter), \
             patch("iceberg_committer.enqueue_pending_file") as eq:
            path = task._stage_arrow_to_pending(
                arrow_tbl, dest=_NESSIE_DEST, table_name="t",
                partition_id="0", chunk_seq=0,
                pk_range=(1, 3), stream_id="s", source_table="src",
                connection_id="c")
        self.assertEqual(path, "/tmp/fake-0-uuid.parquet")
        self.assertIsNotNone(_CapturingWriter.captured)
        self.assertIn("catalog_type", _CapturingWriter.captured,
                       "Bug B: IcebergWriter received the RAW wrapper dict")
        self.assertEqual(_CapturingWriter.captured["catalog_type"], "nessie")
        self.assertNotIn("connector_type", _CapturingWriter.captured)
        self.assertIs(_CapturingWriter.captured, _NESSIE_DEST["connection_config"])
        self.assertTrue(eq.called)

class TestRealLoadCatalogDestUnwrap(unittest.TestCase):
    @staticmethod
    def _install_fake_pyiceberg():
        import types
        fake_catalog_mod = types.ModuleType("pyiceberg.catalog")
        fake_load = MagicMock(name="pyiceberg.load_catalog")
        fake_catalog_mod.load_catalog = fake_load
        fake_pyiceberg_mod = types.ModuleType("pyiceberg")
        fake_pyiceberg_mod.catalog = fake_catalog_mod
        sys.modules["pyiceberg"] = fake_pyiceberg_mod
        sys.modules["pyiceberg.catalog"] = fake_catalog_mod
        def _cleanup():
            sys.modules.pop("pyiceberg", None)
            sys.modules.pop("pyiceberg.catalog", None)
        return fake_load, _cleanup
    def test_unwrapped_dict_picks_nessie_branch_no_keyerror(self):
        from iceberg_writer import load_catalog
        unwrapped = _NESSIE_DEST["connection_config"]
        fake_load, cleanup = self._install_fake_pyiceberg()
        try:
            cat = load_catalog(unwrapped)
        finally:
            cleanup()
        self.assertIs(cat, fake_load.return_value)
        fake_load.assert_called_once()
        _, kwargs = fake_load.call_args
        self.assertEqual(kwargs.get("uri"), "http://nessie:19120/iceberg")
        self.assertEqual(kwargs.get("ref"), "main")
        self.assertEqual(kwargs.get("warehouse"), "s3://lake/wh")
    def test_wrapper_dict_raises_keyerror_catalog_uri(self):
        from iceberg_writer import load_catalog
        fake_load, cleanup = self._install_fake_pyiceberg()
        try:
            with self.assertRaises(KeyError) as cm:
                load_catalog(_NESSIE_DEST)
        finally:
            cleanup()
        self.assertEqual(str(cm.exception), "'catalog_uri'")
        fake_load.assert_not_called()
    def test_unwrapped_rest_dict_picks_rest_branch_no_keyerror(self):
        from iceberg_writer import load_catalog
        rest_flat = {"catalog_type": "rest", "catalog_uri": "http://rest:8181",
                     "warehouse": "s3://lake/wh"}
        fake_load, cleanup = self._install_fake_pyiceberg()
        try:
            cat = load_catalog(rest_flat)
        finally:
            cleanup()
        self.assertIs(cat, fake_load.return_value)
        _, kwargs = fake_load.call_args
        self.assertEqual(kwargs.get("uri"), "http://rest:8181")

if __name__ == "__main__":
    unittest.main()
