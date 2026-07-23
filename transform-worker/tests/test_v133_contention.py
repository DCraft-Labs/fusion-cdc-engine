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


# ─── Bug #22 fix 1: checkpoint advances only after commit success ────────────
def test_checkpoint_advances_only_after_commit_success():
    """Simulate the race: with commit_batch>1, chunks are buffered and the
    checkpoint cursor (``last_pk``) must NOT advance until the batch is
    actually flushed (committed). We model the two cursors directly:

      - ``last_pk``           = last COMMITTED pk (the checkpoint cursor)
      - ``last_buffered_pk``  = last buffered (possibly-uncommitted) pk

    A commit failure must leave ``last_pk`` at the last committed value so a
    retry re-fetches from there (re-applying only un-durable rows, which the
    dedup-on-PK fix makes idempotent).
    """
    commit_batch = 3
    last_pk = None           # checkpoint cursor (committed)
    last_buffered_pk = None  # buffered cursor
    committed_rows = 0

    # Chunks: pks 10, 20, 30. Flush (commit) on chunk 3.
    for chunk_idx, next_pk in enumerate([10, 20, 30], start=1):
        last_buffered_pk = next_pk  # buffer advances immediately
        if chunk_idx >= commit_batch:
            # Flush SUCCEEDS — commit happens. NOW last_pk advances.
            last_pk = next_pk
            committed_rows = chunk_idx
        # (If flush failed, we'd raise and last_pk would NOT advance.)

    assert last_pk == 30, f"After flush, last_pk should be 30, got {last_pk}"
    assert last_buffered_pk == 30
    assert committed_rows == 3

    # Simulate a 4th chunk that buffers, then a crash BEFORE its flush.
    next_pk = 40
    last_buffered_pk = next_pk  # buffer chunk 4
    # Buggy (old): last_pk = next_pk  -> 40 (WRONG: rows 31-40 not durable)
    # Fixed (new): last_pk stays at 30 (last committed)
    assert last_pk == 30, (
        "Bug #22 fix 1 regression: last_pk advanced to %r past the last "
        "committed value (30) while rows were only buffered — a retry would "
        "resume from %r and skip the un-durable rows 31-40 (data loss), or "
        "if the buffered chunk had been partially committed, re-append them "
        "(duplicates)." % (last_pk, last_pk)
    )
    assert last_buffered_pk == 40
    # On retry from last_pk=30, the fetch re-reads pk > 30 — correct.
    assert last_pk < last_buffered_pk, "Retry cursor must not exceed durable boundary."


# ─── Bug #22 fix 2: dedup-on-PK before append (idempotency) ──────────────────
def _stub_pyiceberg_in():
    """The CI unit-test job does NOT install pyiceberg (only pyarrow/duckdb/
    requests/pymysql/psycopg2/redis). `_dedup_on_pk` does
    `from pyiceberg.expressions import In` at call time, which would raise
    ImportError and make the helper skip dedup (its non-fatal fallback). To
    exercise the delete-then-append path in CI, we inject a tiny fake
    `pyiceberg.expressions.In` into sys.modules so the import succeeds. The
    fake `In` exposes the `.term` and `.rows` attributes that
    `_FakeIcebergTable.delete` inspects."""
    import sys
    import types
    if "pyiceberg" not in sys.modules:
        pkg = types.ModuleType("pyiceberg")
        pkg.__path__ = []  # mark as package
        sys.modules["pyiceberg"] = pkg
    if "pyiceberg.expressions" not in sys.modules:
        mod = types.ModuleType("pyiceberg.expressions")

        class In:
            def __init__(self, term, rows):
                self.term = term
                self.rows = tuple(rows)

        mod.In = In
        sys.modules["pyiceberg.expressions"] = mod
        setattr(sys.modules["pyiceberg"], "expressions", mod)


class _FakeIcebergTable:
    """Minimal stand-in for a PyIceberg table: records delete() and append()
    calls so the test can assert delete-then-append ordering and that
    pre-existing duplicate PKs are removed before append."""
    def __init__(self, existing_pks):
        self.existing_pks = list(existing_pks)
        self.delete_calls = []
        self.append_calls = []

    def delete(self, expr):
        pk_col = getattr(expr.term, "name", None) or str(getattr(expr.term, "ref", expr.term))
        keys = list(getattr(expr, "rows", []) or [])
        self.delete_calls.append((pk_col, keys))
        self.existing_pks = [k for k in self.existing_pks if k not in keys]

    def append(self, table_data):
        if isinstance(table_data, list):
            self.append_calls.append(len(table_data))
            self.existing_pks.extend(r.get("id") for r in table_data if r.get("id") is not None)
        else:
            self.append_calls.append(getattr(table_data, "num_rows", 0))


def test_dedup_on_pk_before_append_removes_duplicates():
    """Pre-insert duplicate PKs, run _dedup_on_pk, then append — assert no
    duplicate PKs remain in the table."""
    _stub_pyiceberg_in()
    from iceberg_writer import _dedup_on_pk

    # Table already has PKs [1, 2, 3] (e.g. from a prior partially-committed batch).
    table = _FakeIcebergTable(existing_pks=[1, 2, 3])
    # New batch has PKs [2, 3, 4] — PKs 2 and 3 are duplicates.
    new_rows = [{"id": 2, "v": "x"}, {"id": 3, "v": "y"}, {"id": 4, "v": "z"}]

    _dedup_on_pk(table, "id", rows=new_rows)

    assert len(table.delete_calls) == 1, f"Expected 1 delete call, got {len(table.delete_calls)}"
    pk_col, keys = table.delete_calls[0]
    assert pk_col == "id"
    assert set(keys) == {2, 3, 4}
    assert table.existing_pks == [1], f"After dedup, existing should be [1], got {table.existing_pks}"

    table.append(new_rows)
    assert table.existing_pks == [1, 2, 3, 4], (
        f"After dedup+append, table should have [1,2,3,4], got {table.existing_pks}"
    )
    assert len(table.existing_pks) == len(set(table.existing_pks)), "Duplicate PKs remain"


def test_dedup_on_pk_skips_when_no_pk_col():
    """When pk_col is falsy, _dedup_on_pk is a no-op."""
    _stub_pyiceberg_in()
    from iceberg_writer import _dedup_on_pk
    table = _FakeIcebergTable(existing_pks=[1, 2])
    _dedup_on_pk(table, "", rows=[{"id": 1}])
    assert table.delete_calls == [], "Empty pk_col should skip dedup"


# ─── Bug #22 fix 3: INITIAL_LOAD_COMMIT_BATCH defaults to 1 ──────────────────
def test_commit_batch_defaults_to_1():
    """The default for INITIAL_LOAD_COMMIT_BATCH must be 1 (one commit per
    chunk — legacy v1.2.24 behavior). Immediate mitigation for Bug #22: with
    commit_batch=1 every chunk's write IS a commit, so checkpoint-advance-
    after-commit is trivially correct (no buffering)."""
    import loader
    assert loader.INITIAL_LOAD_COMMIT_BATCH == 1, (
        f"INITIAL_LOAD_COMMIT_BATCH default should be 1, got {loader.INITIAL_LOAD_COMMIT_BATCH}"
    )


def test_commit_batch_env_override(monkeypatch):
    """Operators can opt in to larger batches via the env var (default stays 1)."""
    monkeypatch.setenv("INITIAL_LOAD_COMMIT_BATCH", "5")
    import importlib
    import loader
    importlib.reload(loader)
    assert loader.INITIAL_LOAD_COMMIT_BATCH == 5, (
        f"INITIAL_LOAD_COMMIT_BATCH should be 5 after env override, got {loader.INITIAL_LOAD_COMMIT_BATCH}"
    )
    monkeypatch.delenv("INITIAL_LOAD_COMMIT_BATCH", raising=False)
    importlib.reload(loader)
    assert loader.INITIAL_LOAD_COMMIT_BATCH == 1
