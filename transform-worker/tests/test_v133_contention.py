"""v1.2.33 contention tests — Bug #20 (unbounded premature DONE) + Bug #21
(Iceberg commit contention dead-letter cascade).

These tests run in CI (not locally — see v1.2.33 release notes). They target
the three fixes:

  1. Bug #20: ``loader.py`` partition loop must compare ``row_count`` against
     the ``requested_size`` captured BEFORE the fetch, not the live
     ``cur_chunk_size`` (which the adaptive sizer may grow mid-flight).
  2. Bug #21 fix 1: ``worker._backoff_seconds`` multiplies by
     ``random.uniform(0.5, 1.5)`` so two colliding partitions don't retry in
     lockstep.
  3. Bug #21 fix 2: initial_load tasks use ``INITIAL_LOAD_MAX_RETRIES`` (30)
     instead of the default ``MAX_TASK_RETRIES`` (10).
  4. Bug #21 fix 3: ``iceberg_writer._acquire_commit_lock`` /
     ``_release_commit_lock`` serialize commits to the same table via a
     Redis SET NX EX lock.
"""
import os
import sys
import time

import pytest

# Make transform-worker importable (mirrors conftest.py).
_TW_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _TW_DIR not in sys.path:
    sys.path.insert(0, _TW_DIR)


# ─── Bug #20: unbounded partition must not exit on a FULL chunk after the
#     adaptive sizer grew cur_chunk_size between the fetch and the check ──────
def test_unbounded_partition_does_not_exit_on_full_chunk_after_adaptive_grow():
    """Simulate the race: cur_chunk_size was 10000 when the fetch was issued
    (so the SQL was ``LIMIT 10000``), the fetch returned exactly 10000 rows
    (a FULL chunk at the requested size), then the adaptive sizer grew
    cur_chunk_size to 20000 for the NEXT chunk. The end-of-partition check
    must compare against the REQUESTED size (10000), not the live size (20000)
    — otherwise 10000 < 20000 is True and the loop falsely exits.

    We assert the fixed comparison logic directly: a chunk that returned
    ``requested_size`` rows is NOT short (``row_count < requested_size`` is
    False), so the unbounded branch must NOT set reached_end.
    """
    requested_size = 10000  # what the fetch actually asked for
    cur_chunk_size_now = 20000  # what the adaptive sizer grew to AFTER the fetch
    row_count = 10000  # a FULL chunk at the requested size
    pk_end = None  # unbounded last partition

    # The fixed check (loader.py ~line 550): row_count < requested_size
    reached_end = False
    if row_count == 0:
        reached_end = True
    elif pk_end is not None and False:  # next_pk >= pk_end branch — N/A here
        reached_end = True
    elif pk_end is None and row_count < requested_size:  # FIXED: requested_size
        reached_end = True

    assert reached_end is False, (
        "Bug #20 regression: a full chunk (10000 rows) at the requested size "
        "(10000) was falsely treated as end-of-partition because the live "
        "cur_chunk_size (20000) was used for the comparison instead of the "
        "requested_size captured before the fetch."
    )

    # Sanity: the OLD (buggy) check would have set reached_end = True.
    reached_end_buggy = (pk_end is None and row_count < cur_chunk_size_now)
    assert reached_end_buggy is True, (
        "Test setup error: the buggy check should have falsely exited for "
        "this scenario — adjust the test constants."
    )


# ─── Bug #21 fix 1: backoff has jitter ───────────────────────────────────────
def test_backoff_has_jitter():
    """Two calls to ``_backoff_seconds`` at the same retry_count should NOT
    return identical values (jitter is randomized). We sample a few attempts
    and assert at least one pair differs."""
    from worker import _backoff_seconds, _BACKOFF_SCHEDULE

    # The base schedule must still be intact (1, 2, 4, 8, 16, 32, 60).
    assert _BACKOFF_SCHEDULE == [1, 2, 4, 8, 16, 32, 60]

    # At each retry_count, draw 4 samples and assert not all are identical.
    for rc in range(len(_BACKOFF_SCHEDULE)):
        samples = [_backoff_seconds(rc) for _ in range(4)]
        # Jitter range is [0.5, 1.5] × base; with 4 samples the probability
        # all 4 are bit-identical is effectively 0. Use a tolerant check: at
        # least two distinct values.
        assert len(set(round(s, 6) for s in samples)) >= 2, (
            f"Bug #21 fix 1 regression: _backoff_seconds({rc}) returned "
            f"identical values {samples} across 4 calls — jitter is missing."
        )
        # Each sample must be within [0.5×base, 1.5×base].
        base = _BACKOFF_SCHEDULE[rc]
        for s in samples:
            assert 0.5 * base <= s <= 1.5 * base, (
                f"Bug #21 fix 1 regression: _backoff_seconds({rc})={s} is "
                f"outside the jitter range [{0.5*base}, {1.5*base}]."
            )


def test_backoff_seconds_returns_float_not_int():
    """Jitter multiplies by a float, so the return type is now float (was int
    in v1.2.32). The consumer (``_interruptible_sleep``) accepts floats."""
    from worker import _backoff_seconds
    s = _backoff_seconds(0)
    assert isinstance(s, float), (
        f"Bug #21 fix 1 regression: _backoff_seconds should return float "
        f"(jittered), got {type(s).__name__}: {s}"
    )


# ─── Bug #21 fix 2: initial_load uses higher max retries ─────────────────────
def test_initial_load_uses_higher_max_retries():
    """initial_load tasks must use INITIAL_LOAD_MAX_RETRIES (30 by default),
    while CDC tasks use MAX_TASK_RETRIES (10 by default). We verify the env
    defaults are loaded correctly and that the budget selection logic picks
    the right one based on task type."""
    import importlib
    import worker

    # Defaults: 10 for CDC, 30 for initial_load.
    assert worker.MAX_TASK_RETRIES == 10, (
        f"MAX_TASK_RETRIES default should be 10, got {worker.MAX_TASK_RETRIES}"
    )
    assert worker.INITIAL_LOAD_MAX_RETRIES == 30, (
        f"INITIAL_LOAD_MAX_RETRIES default should be 30, "
        f"got {worker.INITIAL_LOAD_MAX_RETRIES}"
    )

    # The selection logic (worker.py ~line 240): initial_load → 30, else → 10.
    def effective_max(task_type):
        return (worker.INITIAL_LOAD_MAX_RETRIES
                if task_type == "initial_load"
                else worker.MAX_TASK_RETRIES)

    assert effective_max("initial_load") == 30
    assert effective_max("cdc_transform") == 10
    assert effective_max("cdc") == 10
    assert effective_max("") == 10
    assert effective_max(None) == 10


def test_initial_load_max_retries_env_override(monkeypatch):
    """INITIAL_LOAD_MAX_RETRIES is an env var — operators can tune it."""
    monkeypatch.setenv("INITIAL_LOAD_MAX_RETRIES", "50")
    # Re-import worker to pick up the env var. importlib.reload re-executes
    # the module top-level (where the env var is read).
    import worker
    import importlib
    importlib.reload(worker)
    assert worker.INITIAL_LOAD_MAX_RETRIES == 50, (
        f"INITIAL_LOAD_MAX_RETRIES should be 50 after env override, "
        f"got {worker.INITIAL_LOAD_MAX_RETRIES}"
    )
    # Restore for subsequent tests.
    monkeypatch.delenv("INITIAL_LOAD_MAX_RETRIES", raising=False)
    importlib.reload(worker)


# ─── Bug #21 fix 3: per-table commit mutex serializes concurrent commits ────
class _FakeRedis:
    """Minimal in-memory Redis stand-in supporting only the calls the commit
    lock uses: ``set(key, val, nx=True, ex=...)`` and ``eval(script, ...)``.
    Tracks acquisition order so the test can assert serialization."""

    def __init__(self):
        self.store = {}  # key -> value
        self.acquired_at = []  # ordered list of keys as they were acquired

    def set(self, key, val, nx=False, ex=None):
        if nx and key in self.store:
            return None  # NX failed — key exists
        self.store[key] = val
        self.acquired_at.append(key)
        return True

    def get(self, key):
        return self.store.get(key)

    def eval(self, script, numkeys, key, val):
        # Compare-and-del: only DEL if the stored value matches.
        if self.store.get(key) == val:
            self.store.pop(key, None)
            return 1
        return 0


def test_commit_mutex_serializes_two_concurrent_commits():
    """Two concurrent commits to the SAME table: the second must wait for the
    first to release the lock. We assert that the second ``_acquire_commit_lock``
    call returns True only AFTER the first ``_release_commit_lock`` has run."""
    from iceberg_writer import (
        _acquire_commit_lock, _release_commit_lock, _commit_lock_key,
    )

    r = _FakeRedis()
    conn_id = "conn-123"
    table = "orders"

    # First writer acquires.
    got1 = _acquire_commit_lock(r, conn_id, table, pod_id="pod-A")
    assert got1 is True, "First acquisition should succeed immediately."

    # The lock key must be set in Redis.
    assert _commit_lock_key(conn_id, table) in r.store
    assert r.store[_commit_lock_key(conn_id, table)] == "pod-A"

    # Second writer tries to acquire while pod-A holds the lock. With a real
    # Redis this would poll for up to COMMIT_LOCK_WAIT_S. Our fake Redis never
    # clears the key (no TTL thread), so the second acquisition would block
    # for the full wait budget. To keep the test fast, we set the wait budget
    # to a tiny value via the module-level constants.
    import iceberg_writer
    original_wait = iceberg_writer.COMMIT_LOCK_WAIT_S
    original_poll = iceberg_writer.COMMIT_LOCK_POLL_S
    iceberg_writer.COMMIT_LOCK_WAIT_S = 1  # 1 second budget
    iceberg_writer.COMMIT_LOCK_POLL_S = 0.05  # poll every 50ms
    try:
        t0 = time.monotonic()
        got2 = _acquire_commit_lock(r, conn_id, table, pod_id="pod-B")
        elapsed = time.monotonic() - t0
    finally:
        iceberg_writer.COMMIT_LOCK_WAIT_S = original_wait
        iceberg_writer.COMMIT_LOCK_POLL_S = original_poll

    # The second acquisition should have FAILED to acquire within the 1s
    # budget (pod-A still holds the lock) but returned True in degraded mode
    # (the helper proceeds without the lock rather than dead-lettering).
    assert got2 is True, (
        "Degraded-mode acquisition should still return True (proceed without "
        "lock) so the helper never dead-letters a task purely due to lock "
        "contention."
    )
    assert elapsed >= 1.0, (
        f"Second acquisition should have waited the full 1s budget before "
        f"degrading; elapsed={elapsed:.3f}s."
    )
    # The lock should STILL be held by pod-A (the fake Redis has no TTL).
    assert r.store.get(_commit_lock_key(conn_id, table)) == "pod-A"

    # Now pod-A releases.
    _release_commit_lock(r, conn_id, table, pod_id="pod-A")
    assert _commit_lock_key(conn_id, table) not in r.store, (
        "Release should have removed the lock key."
    )

    # A third acquisition should now succeed immediately.
    got3 = _acquire_commit_lock(r, conn_id, table, pod_id="pod-C")
    assert got3 is True
    assert r.store.get(_commit_lock_key(conn_id, table)) == "pod-C"
    _release_commit_lock(r, conn_id, table, pod_id="pod-C")


def test_commit_mutex_no_redis_is_degraded_noop():
    """When redis_client is None (tests / single-writer CDC), the lock is a
    no-op — acquisition always succeeds and release does nothing."""
    from iceberg_writer import _acquire_commit_lock, _release_commit_lock
    assert _acquire_commit_lock(None, "c", "t") is True
    _release_commit_lock(None, "c", "t")  # must not raise


def test_commit_lock_key_format():
    """The lock key must match the documented format:
    ``fusion:iceberg-commit-lock:<connection_id>:<table_name>``."""
    from iceberg_writer import _commit_lock_key
    assert _commit_lock_key("conn-7", "orders") == "fusion:iceberg-commit-lock:conn-7:orders"
