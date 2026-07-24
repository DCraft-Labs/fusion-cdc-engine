"""v1.3.2 Fix 3 - integration test: bulk mode + configured transform
``steps`` emits a one-time-per-stream warning.

Before v1.3.2, when ``use_duckdb_bulk`` was true AND a stream had
``transform_overrides`` / ``steps`` configured, the per-chunk branch
``if kind == "arrow" and connector_type == "iceberg":`` skipped
``execute_pipeline_arrow`` / ``execute_pipeline`` entirely. The
transforms were dropped with zero indication - no warning log, no
metric, no error. The connection appeared to succeed and silently
produced untransformed data.

This test exercises the helper that gates the warning
(``_maybe_warn_bulk_transform_bypass``) and asserts:
  * the warning fires EXACTLY ONCE per ``(stream_id, dest_table)``
    when ``steps`` is non-empty (not per chunk - a 100-chunk load
    must not spam the log 100x);
  * the warning does NOT fire when ``steps`` is empty;
  * the warning does NOT fire in Python mode (``kind == "rows"``) -
    the helper is only invoked from the ``kind == "arrow"`` branch.
"""
from __future__ import annotations

import logging
import os
import sys
import unittest
from unittest.mock import MagicMock

_TW_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _TW_DIR not in sys.path:
    sys.path.insert(0, _TW_DIR)


def _make_initial_load_task():
    from loader import InitialLoadTask
    task = InitialLoadTask.__new__(InitialLoadTask)
    task.engine = None
    task.redis = None
    task._current_connection_id = "c"
    task._current_pk_col = "id"
    task._current_retry_count = 0
    task._bulk_transform_warned = set()
    return task


class TestBulkTransformWarning(unittest.TestCase):
    """v1.3.2 Fix 3: bulk mode + non-empty ``steps`` emits a
    one-time-per-stream warning."""

    def test_warning_fires_once_per_stream_when_steps_nonempty(self):
        task = _make_initial_load_task()
        with self.assertLogs("loader", level="WARNING") as cm:
            for _ in range(100):
                task._maybe_warn_bulk_transform_bypass(
                    "stream-1", "table-1", [{"op": "add_col"}])
        warning_lines = [r for r in cm.records if r.levelno == logging.WARNING]
        self.assertEqual(len(warning_lines), 1,
                          f"warning fired {len(warning_lines)} times for "
                          f"100 chunks - must fire exactly once per "
                          f"(stream_id, dest_table)")
        msg = warning_lines[0].getMessage()
        self.assertIn("bulk mode", msg.lower())
        self.assertIn("bypasses", msg.lower())
        self.assertIn("transform", msg.lower())
        self.assertIn("stream-1", msg)
        self.assertIn("table-1", msg)
        self.assertIn("stream-1:table-1", task._bulk_transform_warned)

    def test_warning_fires_once_per_distinct_stream(self):
        task = _make_initial_load_task()
        with self.assertLogs("loader", level="WARNING") as cm:
            for _ in range(10):
                task._maybe_warn_bulk_transform_bypass(
                    "stream-A", "table-A", [{"op": "x"}])
                task._maybe_warn_bulk_transform_bypass(
                    "stream-B", "table-B", [{"op": "y"}])
        warning_lines = [r for r in cm.records if r.levelno == logging.WARNING]
        self.assertEqual(len(warning_lines), 2,
                          f"expected 2 warnings (one per distinct "
                          f"stream), got {len(warning_lines)}")
        msgs = sorted(r.getMessage() for r in warning_lines)
        self.assertTrue(any("stream-A" in m and "table-A" in m for m in msgs))
        self.assertTrue(any("stream-B" in m and "table-B" in m for m in msgs))

    def test_warning_does_not_fire_when_steps_empty(self):
        task = _make_initial_load_task()
        with self.assertLogs("loader", level="INFO") as cm:
            logging.getLogger("loader").info("sentinel")
            for _ in range(50):
                task._maybe_warn_bulk_transform_bypass(
                    "stream-1", "table-1", [])
        warning_lines = [r for r in cm.records if r.levelno == logging.WARNING]
        self.assertEqual(len(warning_lines), 0,
                          "warning fired for empty steps - must NOT "
                          "fire when there are no transforms to drop")
        self.assertEqual(len(task._bulk_transform_warned), 0)

    def test_warning_does_not_fire_when_steps_none(self):
        task = _make_initial_load_task()
        with self.assertLogs("loader", level="INFO") as cm:
            logging.getLogger("loader").info("sentinel")
            task._maybe_warn_bulk_transform_bypass(
                "stream-1", "table-1", None)
        warning_lines = [r for r in cm.records if r.levelno == logging.WARNING]
        self.assertEqual(len(warning_lines), 0)

    def test_warning_set_is_per_worker_process(self):
        task1 = _make_initial_load_task()
        with self.assertLogs("loader", level="WARNING"):
            task1._maybe_warn_bulk_transform_bypass(
                "stream-1", "table-1", [{"op": "x"}])
        task2 = _make_initial_load_task()
        with self.assertLogs("loader", level="WARNING") as cm:
            task2._maybe_warn_bulk_transform_bypass(
                "stream-1", "table-1", [{"op": "x"}])
        warning_lines = [r for r in cm.records if r.levelno == logging.WARNING]
        self.assertEqual(len(warning_lines), 1,
                          "a new worker process must emit the warning "
                          "again - the set is per-worker, not global")

    def test_warning_message_mentions_both_remediation_paths(self):
        task = _make_initial_load_task()
        with self.assertLogs("loader", level="WARNING") as cm:
            task._maybe_warn_bulk_transform_bypass(
                "stream-1", "table-1", [{"op": "x"}])
        msg = cm.records[0].getMessage().lower()
        self.assertIn("bulk mode", msg)
        self.assertIn("disable", msg,
                      "warning must mention 'disable bulk mode' as a "
                      "remediation path")
        self.assertIn("remove", msg,
                      "warning must mention 'remove the transform_steps "
                      "config' as a remediation path")


if __name__ == "__main__":
    unittest.main()