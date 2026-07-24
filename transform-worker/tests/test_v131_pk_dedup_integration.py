"""v1.3.1 Fix 2 — integration test: the REAL ``_stage_arrow_to_pending``
entry shape feeds the REAL committer's ``_dedup_overlapping_entries``,
and the partial-overlap dedup actually runs (not the warning branch).

The v1.3.0 committer's ``_dedup_overlapping_entries`` reads
``e.get("pk_col")`` from each staged entry. But ``_stage_arrow_to_pending``
never set ``pk_col`` on the entry dict, so the committer always saw
``pk_col=None``, hit the warning branch, and returned without deduping
partial overlaps. The "fully contained" duplicate case worked (doesn't
need pk_col); the "partial overlap" case was silently broken.

The v1.3.0 committer unit tests (``test_v130_pk_dedup.py``) used a
``_entry(...)`` helper that hardcoded ``pk_col="id"``, masking the gap.
This test does NOT use such a helper — it builds the entry via the REAL
``_stage_arrow_to_pending`` method on a REAL ``InitialLoadTask`` instance
(with the Arrow/Parquet/Redis/catalog dependencies stubbed at the
boundary) and feeds the produced entry into the REAL committer's
``drain_and_commit``. The test asserts the partial-overlap dedup path is
invoked (``_dedup_one_range`` is called with the correct ``pk_col``,
``rmin``, ``rmax``), proving the wiring gap is closed.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

# Make transform-worker importable as top-level (mirrors conftest.py).
_TW_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _TW_DIR not in sys.path:
    sys.path.insert(0, _TW_DIR)


class _FakeRedis:
    """In-memory Redis mock covering rpush/lpop/brpop, set(nx,ex), eval,
    sismember/sadd, zadd/zrangebyscore, delete."""

    def __init__(self):
        self.kv: dict = {}
        self.lists: dict = {}
        self.sets: dict = {}
        self.zsets: dict = {}

    def rpush(self, key, *vals):
        self.lists.setdefault(key, []).extend(vals)
        return len(self.lists[key])

    def lpush(self, key, *vals):
        lst = self.lists.setdefault(key, [])
        lst[:0] = list(vals)
        return len(lst)

    def lpop(self, key):
        lst = self.lists.get(key)
        if not lst:
            return None
        return lst.pop(0)

    def brpop(self, key, timeout=0):
        lst = self.lists.get(key)
        if not lst:
            return None
        return (key, lst.pop(0))

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.kv:
            return None
        self.kv[key] = value
        return True

    def delete(self, *keys):
        n = 0
        for k in keys:
            for d in (self.kv, self.lists, self.sets, self.zsets):
                if k in d:
                    del d[k]; n += 1
        return n

    def sismember(self, key, member):
        return member in self.sets.get(key, set())

    def sadd(self, key, *members):
        s = self.sets.setdefault(key, set())
        for m in members:
            s.add(m)
        return len(members)

    def zadd(self, key, mapping):
        z = self.zsets.setdefault(key, {})
        for member, score in mapping.items():
            z[member] = float(score)
        return len(mapping)

    def zrangebyscore(self, key, lo, hi):
        z = self.zsets.get(key, {})
        lo_f = float("-inf") if lo == "-inf" else float(lo)
        hi_f = float("inf") if hi in ("+inf", "inf") else float(hi)
        out = []
        for member, score in z.items():
            if lo_f <= score <= hi_f:
                out.append(member)
        return out

    def eval(self, script, numkeys, *args):
        key, token = args[0], args[1]
        if self.kv.get(key) == token:
            del self.kv[key]
            return 1
        return 0


def _make_initial_load_task():
    """Construct a REAL ``InitialLoadTask`` instance with just enough
    state for ``_stage_arrow_to_pending`` to run. We do NOT exercise the
    fetch/convert/commit pipeline here — only the staging helper and
    the committer. The IcebergWriter dependency is stubbed at the
    ``write_arrow_to_file`` boundary so we control the produced file
    path; ``enqueue_pending_file`` runs for real (it just RPUSHes the
    entry onto the Redis list)."""
    from loader import InitialLoadTask
    task = InitialLoadTask.__new__(InitialLoadTask)
    # v1.3.1 Fix 2: _stage_arrow_to_pending reads self._current_pk_col
    # to thread into the entry. Set it as the real run() path does.
    task._current_pk_col = "id"
    task.redis = None  # set per-test
    return task


def _stub_writer_module(path_to_return: str):
    """Build a stub module that replaces ``iceberg_writer.IcebergWriter``
    so ``_stage_arrow_to_pending``'s ``writer.write_arrow_to_file`` returns
    a controlled path. Returns a (module_dict, cleanup) pair."""
    class _StubWriter:
        def __init__(self, dest, redis_client=None, connection_id=None):
            pass
        def write_arrow_to_file(self, arrow_tbl, table_name=None,
                                partition_id=None, chunk_seq=None,
                                pk_range=None, pk_col=None, **kwargs):
            return path_to_return
    return _StubWriter


class TestStageArrowToPendingPKColWiring(unittest.TestCase):
    """The REAL _stage_arrow_to_pending entry dict must carry pk_col so
    the REAL committer's _dedup_overlapping_entries can run dedup-on-PK
    for partial overlaps (not the warning branch)."""

    def test_staged_entry_carries_pk_col(self):
        """Direct unit-level check: the entry produced by the REAL
        _stage_arrow_to_pending has pk_col == self._current_pk_col.
        This is the wiring fix itself."""
        import pyarrow as pa
        from loader import InitialLoadTask
        from iceberg_committer import pending_key
        rc = _FakeRedis()
        task = _make_initial_load_task()
        task.redis = rc
        arrow_tbl = pa.table({"id": [1, 2, 3], "v": ["a", "b", "c"]})
        stub_writer = _stub_writer_module("/tmp/fake-0-uuid.parquet")
        with patch("iceberg_writer.IcebergWriter", stub_writer):
            path = task._stage_arrow_to_pending(
                arrow_tbl, dest={}, table_name="t",
                partition_id="0", chunk_seq=0,
                pk_range=(1, 3), stream_id="s", source_table="src",
                connection_id="c")
        self.assertEqual(path, "/tmp/fake-0-uuid.parquet")
        # The entry was RPUSHed onto the pending list — read it back.
        pending = rc.lists.get(pending_key("c", "t"), [])
        self.assertEqual(len(pending), 1)
        entry = json.loads(pending[0])
        # v1.3.1 Fix 2: pk_col IS set on the staged entry (was None in
        # v1.3.0, which silently broke the partial-overlap dedup path).
        self.assertIn("pk_col", entry,
                      "staged entry missing pk_col — committer cannot "
                      "run partial-overlap dedup-on-PK")
        self.assertEqual(entry["pk_col"], "id")
        # And the rest of the entry shape is intact.
        self.assertEqual(entry["table_name"], "t")
        self.assertEqual(entry["file_path"], "/tmp/fake-0-uuid.parquet")
        self.assertEqual(entry["row_count"], 3)
        self.assertEqual(entry["pk_range"], [1, 3])
        self.assertEqual(entry["chunk_seq"], 0)
        self.assertEqual(entry["partition_id"], "0")
        self.assertEqual(entry["stream_id"], "s")
        self.assertEqual(entry["source_table"], "src")

    def test_real_staging_feeds_real_committer_partial_overlap_dedup(self):
        """End-to-end-ish: stage a chunk via the REAL
        _stage_arrow_to_pending, then drain+commit via the REAL
        IcebergCommitter with a pre-seeded committed-PK-range that
        PARTIALLY overlaps the staged chunk. Assert the committer's
        _dedup_one_range is invoked (partial-overlap dedup runs), not
        the warning branch (which fires when pk_col is None).

        This is the integration test the v1.3.0 unit tests failed to
        provide: they used a _entry(...) helper that hardcoded
        pk_col="id", masking the fact that the real staging path never
        set pk_col. This test uses the real staging path."""
        import pyarrow as pa
        from loader import InitialLoadTask
        from iceberg_committer import (IcebergCommitter, pending_key,
                                         committed_pk_ranges_key)
        rc = _FakeRedis()
        # Pre-seed a committed PK range [0, 50] (file "old.parquet").
        rc.zadd(committed_pk_ranges_key("c", "t"),
                {json.dumps({"min": 0, "max": 50,
                             "file_path": "old.parquet"}): 0.0})
        # Stage a chunk with PK range [25, 75] — PARTIALLY overlaps
        # [0, 50] (overlap window [25, 50]). Not fully contained (75 > 50),
        # so the committer must run dedup-on-PK, not skip+delete.
        task = _make_initial_load_task()
        task.redis = rc
        task._current_pk_col = "id"
        arrow_tbl = pa.table({"id": [25, 30, 75], "v": ["x", "y", "z"]})
        stub_writer = _stub_writer_module("/tmp/fake-0-uuid.parquet")
        with patch("iceberg_writer.IcebergWriter", stub_writer):
            task._stage_arrow_to_pending(
                arrow_tbl, dest={}, table_name="t",
                partition_id="0", chunk_seq=0,
                pk_range=(25, 75), stream_id="s", source_table="src",
                connection_id="c")
        # Now run the REAL committer. Mock _dedup_one_range so we can
        # observe whether it was called (and with what pk_col).
        catalog = MagicMock()
        table = MagicMock()
        catalog.load_table.return_value = table
        tx = MagicMock()
        tx_cm = MagicMock()
        tx_cm.__enter__ = MagicMock(return_value=tx)
        tx_cm.__exit__ = MagicMock(return_value=False)
        table.transaction.return_value = tx_cm
        committer = IcebergCommitter(catalog, rc, "c", "t")
        dedup_calls = []
        def fake_dedup(t, pk_col, rmin, rmax, entry):
            dedup_calls.append((pk_col, rmin, rmax, entry.get("file_path")))
        with patch.object(IcebergCommitter, "_dedup_one_range",
                          side_effect=fake_dedup):
            r = committer.drain_and_commit()
        # The entry was committed (delete-then-register path).
        self.assertEqual(r["committed"], 1)
        tx.add_files.assert_called_once_with(
            file_paths=["/tmp/fake-0-uuid.parquet"])
        # CRITICAL: _dedup_one_range WAS called (partial-overlap dedup
        # ran). In v1.3.0 this would NOT have been called because
        # _stage_arrow_to_pending never set pk_col, so the committer hit
        # the "no pk_col" warning branch and returned without deduping.
        self.assertEqual(len(dedup_calls), 1,
                          "partial-overlap dedup did NOT run — the "
                          "committer hit the pk_col=None warning branch. "
                          "This is the v1.3.0 wiring gap; Fix 2 threads "
                          "pk_col from _stage_arrow_to_pending.")
        pk_col, rmin, rmax, path = dedup_calls[0]
        self.assertEqual(pk_col, "id",
                          "dedup was called with pk_col=None or wrong — "
                          "the real staging path did not thread pk_col")
        self.assertEqual(rmin, 25)
        self.assertEqual(rmax, 75)
        self.assertEqual(path, "/tmp/fake-0-uuid.parquet")

    def test_v130_wiring_gap_would_skip_dedup(self):
        """Guard / regression signature: if we simulate the v1.3.0 bug
        (staged entry WITHOUT pk_col), the committer's
        _dedup_overlapping_entries hits the warning branch and does NOT
        call _dedup_one_range. This pins the bug's signature so a future
        re-introduction is caught immediately."""
        from iceberg_committer import (IcebergCommitter, pending_key,
                                         committed_pk_ranges_key)
        rc = _FakeRedis()
        rc.zadd(committed_pk_ranges_key("c", "t"),
                {json.dumps({"min": 0, "max": 50,
                             "file_path": "old.parquet"}): 0.0})
        # Simulate the v1.3.0 bug: entry WITHOUT pk_col.
        buggy_entry = {
            "table_name": "t",
            "file_path": "/tmp/buggy-0-uuid.parquet",
            "row_count": 3,
            "pk_range": [25, 75],
            "chunk_seq": 0,
            "partition_id": "0",
            "stream_id": "s",
            "source_table": "src",
            # pk_col INTENTIONALLY MISSING — this is the v1.3.0 bug.
        }
        rc.rpush(pending_key("c", "t"), json.dumps(buggy_entry))
        catalog = MagicMock()
        table = MagicMock()
        catalog.load_table.return_value = table
        tx = MagicMock()
        tx_cm = MagicMock()
        tx_cm.__enter__ = MagicMock(return_value=tx)
        tx_cm.__exit__ = MagicMock(return_value=False)
        table.transaction.return_value = tx_cm
        committer = IcebergCommitter(catalog, rc, "c", "t")
        with patch.object(IcebergCommitter, "_dedup_one_range") as d:
            r = committer.drain_and_commit()
        # The entry was still committed (the warning branch doesn't
        # block registration).
        self.assertEqual(r["committed"], 1)
        # But _dedup_one_range was NOT called — the partial-overlap
        # dedup was skipped because pk_col was missing. This is the
        # v1.3.0 bug signature.
        d.assert_not_called()


if __name__ == "__main__":
    unittest.main()
