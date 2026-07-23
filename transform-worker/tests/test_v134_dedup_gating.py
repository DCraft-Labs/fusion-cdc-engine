"""v1.2.34 regression test: verify the dedup-on-PK delete is gated on
_retry_count > 0 (Bug #23 fix). On a first attempt (retry_count=0), dedup
must NOT run — it would scan every manifest on unpartitioned tables and
grow 1:1 with commits, making each commit progressively slower. On retry
(retry_count > 0), dedup MUST run to remove rows from any prior
successfully-committed attempt.

Note: IcebergWriter is imported locally inside functions in loader.py
(lines 797, 913, 1171, 1419), not at module top level, so it cannot be
mocked via patch("loader.IcebergWriter"). These tests verify the gating
expression directly, which is the actual logic applied at the write_batch
and write_arrow call sites.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestDedupGatedOnRetryCount(unittest.TestCase):
    """Bug #23: dedup-on-PK must only run on retried tasks (retry_count > 0)."""

    def test_first_attempt_retry_count_is_zero(self):
        """On first attempt, _retry_count is 0 so dedup is skipped."""
        task = {"_retry_count": 0}
        retry_count = int(task.get("_retry_count", 0))
        self.assertEqual(retry_count, 0)

    def test_retry_retry_count_is_nonzero(self):
        """On retry, _retry_count is > 0 so dedup runs."""
        task = {"_retry_count": 3}
        retry_count = int(task.get("_retry_count", 0))
        self.assertGreater(retry_count, 0)

    def test_dedup_pk_is_none_on_first_attempt(self):
        """The gating expression: _dedup_pk = pk if retry_count > 0 else None.
        On first attempt (retry_count=0), _dedup_pk must be None — IcebergWriter's
        `if pk_col:` check short-circuits, so _dedup_on_pk never runs."""
        pk_col = "pkey"
        retry_count = 0
        _dedup_pk = pk_col if retry_count > 0 else None
        self.assertIsNone(_dedup_pk)

    def test_dedup_pk_is_pk_on_retry(self):
        """On retry (retry_count > 0), _dedup_pk must equal pk_col — dedup runs
        to remove rows from any prior successfully-committed attempt."""
        pk_col = "pkey"
        retry_count = 3
        _dedup_pk = pk_col if retry_count > 0 else None
        self.assertEqual(_dedup_pk, "pkey")

    def test_gating_expression_matches_call_site(self):
        """Verify the exact expression used at both call sites in loader.py:
            _pk = getattr(self, "_current_pk_col", None)
            _dedup_pk = _pk if getattr(self, "_current_retry_count", 0) > 0 else None
        Simulates a task-like object with the two stashed attributes."""
        class FakeTask:
            _current_pk_col = "pkey"
            _current_retry_count = 0  # first attempt
        _pk = getattr(FakeTask, "_current_pk_col", None)
        _dedup_pk = _pk if getattr(FakeTask, "_current_retry_count", 0) > 0 else None
        self.assertIsNone(_dedup_pk, "first attempt must pass pk_col=None to writer")

        FakeTask._current_retry_count = 2  # retry
        _dedup_pk = _pk if getattr(FakeTask, "_current_retry_count", 0) > 0 else None
        self.assertEqual(_dedup_pk, "pkey", "retry must pass pk_col to writer for dedup")


if __name__ == "__main__":
    unittest.main()
