"""v1.2.36 regression test: verify the commit lock is acquired BEFORE
_get_or_create_table() in all three write paths (write_batch, write_arrow,
upsert). This is the Bug #24 fix — previously _get_or_create_table() ran
outside the lock, so the table object referenced a stale snapshot id by
the time the pod got into the lock, causing CommitFailedException:
snapshot id changed under real K=6 contention.

The test uses unittest.mock to record the call order of _acquire_commit_lock
vs _get_or_create_table, and asserts the lock is acquired first.
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestCommitLockOrdering(unittest.TestCase):
    """Bug #24: _acquire_commit_lock must be called BEFORE
    _get_or_create_table so the table object reflects the latest snapshot."""

    def _make_writer(self):
        """Construct an IcebergWriter with mocked dependencies."""
        from iceberg_writer import IcebergWriter
        writer = IcebergWriter.__new__(IcebergWriter)
        writer.catalog = MagicMock()
        writer.namespace = "fusion"
        writer.dest_config = {}
        writer.redis_client = MagicMock()
        writer.connection_id = "test-conn"
        return writer

    def test_write_batch_lock_before_table_load(self):
        """In write_batch, _acquire_commit_lock must be called before
        _get_or_create_table."""
        from iceberg_writer import IcebergWriter
        writer = self._make_writer()
        rows = [{"pkey": 1, "val": "a"}]

        call_order = []
        def record(name):
            call_order.append(name)
            return MagicMock()

        with patch("iceberg_writer._get_or_create_table", side_effect=lambda *a, **k: record("get_table")), \
             patch("iceberg_writer._acquire_commit_lock", side_effect=lambda *a, **k: record("acquire_lock")), \
             patch("iceberg_writer._release_commit_lock", side_effect=lambda *a, **k: record("release_lock")), \
             patch("iceberg_writer._rows_to_arrow", return_value=MagicMock(schema=MagicMock())), \
             patch("iceberg_writer._dedup_on_pk"):
            mock_table = MagicMock()
            mock_table.append = MagicMock()
            with patch("iceberg_writer._get_or_create_table", side_effect=lambda *a, **k: (call_order.append("get_table"), mock_table)[1]):
                writer.write_batch(rows, table_name="t", pk_col=None)

        # The lock MUST be acquired before the table is loaded.
        self.assertGreater(call_order.index("acquire_lock"), -1, "acquire_lock must be called")
        self.assertGreater(call_order.index("get_table"), -1, "get_table must be called")
        self.assertLess(call_order.index("acquire_lock"), call_order.index("get_table"),
                         "acquire_lock must be called BEFORE get_table — "
                         "otherwise the table object references a stale snapshot")

    def test_write_arrow_lock_before_table_load(self):
        """In write_arrow, _acquire_commit_lock must be called before
        _get_or_create_table."""
        from iceberg_writer import IcebergWriter
        writer = self._make_writer()
        arrow_tbl = MagicMock()
        arrow_tbl.schema = MagicMock()
        arrow_tbl.num_rows = 100

        call_order = []
        mock_table = MagicMock()
        mock_table.append = MagicMock()
        with patch("iceberg_writer._get_or_create_table", side_effect=lambda *a, **k: (call_order.append("get_table"), mock_table)[1]), \
             patch("iceberg_writer._acquire_commit_lock", side_effect=lambda *a, **k: call_order.append("acquire_lock")), \
             patch("iceberg_writer._release_commit_lock", side_effect=lambda *a, **k: call_order.append("release_lock")), \
             patch("iceberg_writer._dedup_on_pk"):
            writer.write_arrow(arrow_tbl, table_name="t", pk_col=None)

        self.assertLess(call_order.index("acquire_lock"), call_order.index("get_table"),
                         "acquire_lock must be called BEFORE get_table in write_arrow")

    def test_upsert_lock_before_table_load(self):
        """In upsert, _acquire_commit_lock must be called before
        _get_or_create_table."""
        from iceberg_writer import IcebergWriter
        writer = self._make_writer()
        rows = [{"pkey": 1, "val": "a"}]

        call_order = []
        mock_table = MagicMock()
        mock_table.append = MagicMock()
        mock_table.upsert = MagicMock()
        with patch("iceberg_writer._get_or_create_table", side_effect=lambda *a, **k: (call_order.append("get_table"), mock_table)[1]), \
             patch("iceberg_writer._acquire_commit_lock", side_effect=lambda *a, **k: call_order.append("acquire_lock")), \
             patch("iceberg_writer._release_commit_lock", side_effect=lambda *a, **k: call_order.append("release_lock")), \
             patch("iceberg_writer._rows_to_arrow", return_value=MagicMock(schema=MagicMock())):
            writer.upsert(rows, table_name="t", identifier_fields=["pkey"])

        self.assertLess(call_order.index("acquire_lock"), call_order.index("get_table"),
                         "acquire_lock must be called BEFORE get_table in upsert")


if __name__ == "__main__":
    unittest.main()
