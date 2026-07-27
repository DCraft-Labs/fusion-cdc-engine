"""Phase 3b: tests for IcebergCommitter's runtime-adjustable add_files()
concurrency (the "cheap, frequent, no-restart" lever control-plane's
committer_resizer reconcile loop drives).

Covers:
  - concurrency_key() produces the expected per-connection Redis key
  - _refresh_add_files_concurrency() picks up a valid override
  - the override is clamped to [_CONCURRENCY_MIN, _CONCURRENCY_MAX]
  - a missing/invalid Redis value leaves the current setting untouched
  - run_cycle() calls the refresh exactly once per cycle (before draining)

No live Redis/pyiceberg required — a minimal in-memory fake stands in for
Redis (get/set only, matching the small surface this feature needs).
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock


class _FakeRedisGetSet:
    """Minimal fake covering only get/set — the concurrency hot-reload
    surface. See transform-worker/tests/test_v139_committer.py's
    _FakeRedis for the fuller mock used by drain/commit tests; this one is
    intentionally narrower since concurrency hot-reload doesn't touch
    lists/sets/locks at all."""

    def __init__(self):
        self.kv: dict[str, str] = {}

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.kv:
            return None
        self.kv[key] = value
        return True


class TestConcurrencyKey(unittest.TestCase):
    def test_key_format(self):
        from iceberg_committer import concurrency_key
        self.assertEqual(
            concurrency_key("conn-123"),
            "fusion:iceberg-committer-concurrency:conn-123",
        )


class TestRefreshAddFilesConcurrency(unittest.TestCase):
    def _make_committer(self, redis_client):
        from iceberg_committer import IcebergCommitter
        catalog = MagicMock()
        return IcebergCommitter(catalog, redis_client, "conn-1", ["t1", "t2"])

    def test_default_concurrency_matches_env_default(self):
        from iceberg_committer import _ADD_FILES_MAX_WORKERS
        committer = self._make_committer(_FakeRedisGetSet())
        self.assertEqual(committer.add_files_max_workers, _ADD_FILES_MAX_WORKERS)

    def test_valid_override_is_applied(self):
        from iceberg_committer import concurrency_key
        rc = _FakeRedisGetSet()
        rc.set(concurrency_key("conn-1"), "24")
        committer = self._make_committer(rc)
        committer._refresh_add_files_concurrency()
        self.assertEqual(committer.add_files_max_workers, 24)

    def test_override_is_clamped_to_max(self):
        from iceberg_committer import concurrency_key, _CONCURRENCY_MAX
        rc = _FakeRedisGetSet()
        rc.set(concurrency_key("conn-1"), "999")
        committer = self._make_committer(rc)
        committer._refresh_add_files_concurrency()
        self.assertEqual(committer.add_files_max_workers, _CONCURRENCY_MAX)

    def test_override_is_clamped_to_min(self):
        from iceberg_committer import concurrency_key, _CONCURRENCY_MIN
        rc = _FakeRedisGetSet()
        rc.set(concurrency_key("conn-1"), "0")
        committer = self._make_committer(rc)
        committer._refresh_add_files_concurrency()
        self.assertEqual(committer.add_files_max_workers, _CONCURRENCY_MIN)

    def test_missing_key_leaves_setting_untouched(self):
        committer = self._make_committer(_FakeRedisGetSet())
        original = committer.add_files_max_workers
        committer._refresh_add_files_concurrency()
        self.assertEqual(committer.add_files_max_workers, original)

    def test_non_integer_value_is_ignored(self):
        from iceberg_committer import concurrency_key
        rc = _FakeRedisGetSet()
        rc.set(concurrency_key("conn-1"), "not-a-number")
        committer = self._make_committer(rc)
        original = committer.add_files_max_workers
        committer._refresh_add_files_concurrency()
        self.assertEqual(committer.add_files_max_workers, original)

    def test_no_redis_client_is_a_noop(self):
        committer = self._make_committer(None)
        original = committer.add_files_max_workers
        committer._refresh_add_files_concurrency()  # must not raise
        self.assertEqual(committer.add_files_max_workers, original)

    def test_redis_error_is_swallowed(self):
        rc = MagicMock()
        rc.get.side_effect = RuntimeError("boom")
        committer = self._make_committer(rc)
        original = committer.add_files_max_workers
        committer._refresh_add_files_concurrency()  # must not raise
        self.assertEqual(committer.add_files_max_workers, original)

    def test_constructor_arg_overrides_env_default(self):
        from iceberg_committer import IcebergCommitter
        catalog = MagicMock()
        committer = IcebergCommitter(catalog, _FakeRedisGetSet(), "conn-1", ["t1"],
                                      add_files_max_workers=8)
        self.assertEqual(committer.add_files_max_workers, 8)


class TestRunCycleRefreshesOncePerCycle(unittest.TestCase):
    def test_refresh_called_once_per_run_cycle(self):
        """run_cycle() must call _refresh_add_files_concurrency exactly
        once per cycle, BEFORE draining any table — not once per table."""
        from iceberg_committer import IcebergCommitter
        catalog = MagicMock()
        committer = IcebergCommitter(catalog, _FakeRedisGetSet(), "conn-1", ["t1", "t2", "t3"])
        committer._refresh_add_files_concurrency = MagicMock()
        committer.drain_and_commit = MagicMock(return_value={
            "drained": 0, "committed": 0, "skipped_duplicate": 0,
            "committed_paths": [], "errors": [],
        })
        committer.run_cycle()
        committer._refresh_add_files_concurrency.assert_called_once()
        self.assertEqual(committer.drain_and_commit.call_count, 3)


if __name__ == "__main__":
    unittest.main()
