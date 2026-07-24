"""v1.3.4 Fix 2 — lock unification tests.

Verifies that the bootstrap path (iceberg_writer._commit_lock_key) and
the committer (iceberg_committer.lock_key / _LOCK_KEY) use the SAME
Redis key namespace for a given (connection_id, table_name) pair, so
the two commit paths provide mutual exclusion against each other.
Previously the writer used ``fusion:iceberg-commit-lock:...`` and the
committer used ``fusion:iceberg-committer-lock:...`` — two
non-coordinating locks for the same table, root cause of
``FileNotFoundError: ...snap-...avro`` inside commit() and the 110.6%
duplicate overage.

All tests mock Redis (no live Redis required).
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock


class _FakeRedis:
    """Minimal in-memory Redis mock supporting SET NX EX + EVAL
    compare-and-del + GET, shared across all keys via one kv dict so
    the writer's lock and the committer's lock race against the SAME
    key when the namespaces are unified."""

    def __init__(self):
        self.kv: dict[str, object] = {}

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.kv:
            return None
        self.kv[key] = value
        return True

    def get(self, key):
        return self.kv.get(key)

    def delete(self, *keys):
        n = 0
        for k in keys:
            if k in self.kv:
                del self.kv[k]
                n += 1
        return n

    def eval(self, script, numkeys, *args):
        # Compare-and-del: KEYS[1]=key ARGV[1]=token
        key, token = args[0], args[1]
        if self.kv.get(key) == token:
            del self.kv[key]
            return 1
        return 0


class TestLockUnification(unittest.TestCase):
    def setUp(self):
        # Force the writer's lock-wait budget to 1s so a contended
        # acquire attempts SET NX once, fails, and returns degraded-True
        # quickly (instead of polling for the default 60s). The point of
        # these tests is namespace unification, not the writer's wait
        # behavior.
        import iceberg_writer
        self._prev_wait = iceberg_writer.COMMIT_LOCK_WAIT_S
        iceberg_writer.COMMIT_LOCK_WAIT_S = 1

    def tearDown(self):
        import iceberg_writer
        iceberg_writer.COMMIT_LOCK_WAIT_S = self._prev_wait

    def test_writer_and_committer_use_same_lock_key(self):
        """The writer's _commit_lock_key and the committer's lock_key
        MUST produce the same string for the same (conn, table)."""
        from iceberg_writer import _commit_lock_key
        from iceberg_committer import lock_key, _LOCK_KEY
        for conn, table in [("c1", "t1"), ("conn-pg", "customer"),
                            ("abc-123", "orders_2024")]:
            w = _commit_lock_key(conn, table)
            c = lock_key(conn, table)
            self.assertEqual(
                w, c,
                f"writer key {w!r} != committer key {c!r} for ({conn}, {table})",
            )
            # Both must be in the unified namespace.
            self.assertTrue(w.startswith("fusion:iceberg-committer-lock:"),
                            f"{w!r} not in unified committer-lock namespace")
            self.assertIn(conn, w)
            self.assertIn(table, w)
        # The committer's _LOCK_KEY template must be the unified namespace.
        self.assertIn("iceberg-committer-lock", _LOCK_KEY)

    def test_writer_and_committer_cannot_hold_lock_concurrently(self):
        """Acquiring the writer's commit lock must block the committer
        from acquiring its lock (and vice versa) for the same (conn,
        table). This is the core mutual-exclusion guarantee the
        unification provides."""
        from iceberg_writer import (_acquire_commit_lock,
                                     _release_commit_lock)
        from iceberg_committer import IcebergCommitter

        rc = _FakeRedis()
        # Writer holds the lock.
        got_writer = _acquire_commit_lock(rc, "c1", "t1",
                                          pod_id="writer-pod")
        self.assertTrue(got_writer)
        # Committer must NOT be able to acquire the same lock.
        committer = IcebergCommitter(MagicMock(), rc, "c1", "t1")
        got_committer = committer._acquire_lock()
        self.assertFalse(got_committer,
                         "committer acquired the lock while the writer held it "
                         "- lock namespaces are NOT unified")
        # Writer releases.
        _release_commit_lock(rc, "c1", "t1", pod_id="writer-pod")
        # Committer can now acquire.
        self.assertTrue(committer._acquire_lock())
        # And while the committer holds it, the writer is blocked (with
        # wait=0 the writer returns immediately; in unified-namespace
        # mode the SET NX fails so the writer's degraded-True path is
        # taken — but the lock key is NOT acquired by the writer).
        _acquire_commit_lock(rc, "c1", "t1", pod_id="writer-pod-2")
        # The committer's token is still the one in Redis (the writer's
        # degraded acquire did not overwrite it).
        self.assertEqual(rc.kv.get("fusion:iceberg-committer-lock:c1:t1"),
                         committer._lock_token)
        committer._release_lock()
        # Writer can acquire again.
        self.assertTrue(_acquire_commit_lock(rc, "c1", "t1",
                                             pod_id="writer-pod-3"))

    def test_writer_lock_release_uses_compare_and_del(self):
        """The writer's release must only delete the key if it still
        holds the token (Lua compare-and-del), so a TTL-expired lock
        that another pod grabbed is not silently released."""
        from iceberg_writer import _acquire_commit_lock, _release_commit_lock
        rc = _FakeRedis()
        _acquire_commit_lock(rc, "c1", "t1", pod_id="pod-A")
        # Simulate TTL expiry + pod-B grabbing the lock.
        del rc.kv[next(iter(rc.kv))]
        rc.kv["fusion:iceberg-committer-lock:c1:t1"] = "pod-B"
        # pod-A's release must NOT delete pod-B's lock.
        _release_commit_lock(rc, "c1", "t1", pod_id="pod-A")
        self.assertEqual(rc.kv.get("fusion:iceberg-committer-lock:c1:t1"),
                         "pod-B")

    def test_different_tables_have_independent_locks(self):
        """Unification is per (conn, table); different tables must not
        share a lock (otherwise the committer for table A would stall
        the bootstrap for table B)."""
        from iceberg_writer import _commit_lock_key
        a = _commit_lock_key("c1", "tA")
        b = _commit_lock_key("c1", "tB")
        self.assertNotEqual(a, b)
        from iceberg_committer import lock_key
        self.assertNotEqual(lock_key("c1", "tA"), lock_key("c2", "tA"))


if __name__ == "__main__":
    unittest.main()
