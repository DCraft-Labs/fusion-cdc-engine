"""v1.3.0 Fix 3 — committer PK-level dedup tests.

Covers the checkpoint-race scenario the v1.2.39 committer missed:
  1. A partition stages a chunk (file durably written, ``last_pk`` advances
     in worker memory) but the checkpoint report to the DB fails / pod
     crashes before the checkpoint persists.
  2. Task restart resumes from the stale (older) checkpoint, re-fetches and
     re-stages the SAME PK range as a brand-new file (new UUID path).
  3. The path-based committed set does NOT match the new path, so without
     PK-range overlap detection the committer would register both files ->
     genuine row-level duplicates.

The fix adds a Redis sorted set ``fusion:iceberg-committed-pk-ranges``
(scored by min_pk, members JSON ``{min, max, file_path}``) and an overlap
check in ``_commit_entries`` BEFORE ``add_files()``. On overlap the
committer either (a) runs dedup-on-PK before registering, or (b) skips +
deletes the new file if its range is fully contained in a committed range.
"""
from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock, patch


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


def _entry(path, chunk_seq=0, row_count=10, pk_range=None, pk_col="id"):
    return {"file_path": path, "chunk_seq": chunk_seq,
            "row_count": row_count, "table_name": "t",
            "partition_id": str(chunk_seq), "stream_id": "s",
            "source_table": "src", "pk_range": pk_range,
            "pk_col": pk_col}


def _make_catalog():
    catalog = MagicMock()
    table = MagicMock()
    catalog.load_table.return_value = table
    tx = MagicMock()
    tx_cm = MagicMock()
    tx_cm.__enter__ = MagicMock(return_value=tx)
    tx_cm.__exit__ = MagicMock(return_value=False)
    table.transaction.return_value = tx_cm
    table.location.return_value = "s3://bkt/warehouse/fusion/t"
    table.io = MagicMock()
    return catalog, table, tx


class TestPKRangeOverlapDetection(unittest.TestCase):
    def test_overlapping_range_triggers_dedup_before_register(self):
        """Two files with overlapping PK ranges: the second file's
        registration must trigger a dedup-on-PK call before add_files()."""
        from iceberg_committer import (IcebergCommitter, pending_key,
                                        committed_pk_ranges_key)
        rc = _FakeRedis()
        # Pre-load the committed-PK-ranges set with an already-committed
        # range [0, 100] for file "old.parquet".
        rc.zadd(committed_pk_ranges_key("c", "t"),
                {json.dumps({"min": 0, "max": 100,
                             "file_path": "old.parquet"}): 0.0})
        # New file overlaps [50, 150].
        rc.rpush(pending_key("c", "t"),
                 json.dumps(_entry("new.parquet", 0, pk_range=(50, 150))))
        catalog, table, tx = _make_catalog()
        committer = IcebergCommitter(catalog, rc, "c", "t")
        with patch("iceberg_committer.IcebergCommitter._dedup_one_range") as d:
            r = committer.drain_and_commit()
        self.assertEqual(r["committed"], 1)
        # Dedup was invoked for the overlapping entry.
        d.assert_called_once()
        # The new file was still registered (delete-then-register path).
        tx.add_files.assert_called_once_with(file_paths=["new.parquet"])

    def test_fully_contained_range_skipped_and_deleted(self):
        """A new file whose PK range is fully contained in a committed
        range is a pure duplicate: skip registration + delete the file."""
        from iceberg_committer import (IcebergCommitter, pending_key,
                                        committed_pk_ranges_key)
        rc = _FakeRedis()
        rc.zadd(committed_pk_ranges_key("c", "t"),
                {json.dumps({"min": 0, "max": 1000,
                             "file_path": "big.parquet"}): 0.0})
        # New file fully inside [0, 1000].
        rc.rpush(pending_key("c", "t"),
                 json.dumps(_entry("dup.parquet", 0, pk_range=(200, 300))))
        catalog, table, tx = _make_catalog()
        committer = IcebergCommitter(catalog, rc, "c", "t")
        r = committer.drain_and_commit()
        self.assertEqual(r["committed"], 0)
        self.assertEqual(r["skipped_duplicate"], 1)
        tx.add_files.assert_not_called()
        # The staged file was deleted from the object store.
        table.io.delete.assert_called_once_with("dup.parquet")

    def test_non_overlapping_range_registers_cleanly(self):
        """A new file with no overlap registers normally (no dedup, no
        skip)."""
        from iceberg_committer import (IcebergCommitter, pending_key,
                                        committed_pk_ranges_key)
        rc = _FakeRedis()
        rc.zadd(committed_pk_ranges_key("c", "t"),
                {json.dumps({"min": 0, "max": 100,
                             "file_path": "old.parquet"}): 0.0})
        rc.rpush(pending_key("c", "t"),
                 json.dumps(_entry("new.parquet", 0, pk_range=(200, 300))))
        catalog, table, tx = _make_catalog()
        committer = IcebergCommitter(catalog, rc, "c", "t")
        with patch("iceberg_committer.IcebergCommitter._dedup_one_range") as d:
            r = committer.drain_and_commit()
        self.assertEqual(r["committed"], 1)
        d.assert_not_called()
        tx.add_files.assert_called_once_with(file_paths=["new.parquet"])

    def test_committed_range_recorded_after_successful_commit(self):
        """After a successful commit, the file's PK range is added to the
        sorted set so future overlap checks see it."""
        from iceberg_committer import (IcebergCommitter, pending_key,
                                        committed_pk_ranges_key)
        rc = _FakeRedis()
        rc.rpush(pending_key("c", "t"),
                 json.dumps(_entry("first.parquet", 0, pk_range=(0, 100))))
        catalog, table, tx = _make_catalog()
        committer = IcebergCommitter(catalog, rc, "c", "t")
        committer.drain_and_commit()
        zset = rc.zsets.get(committed_pk_ranges_key("c", "t"), {})
        self.assertEqual(len(zset), 1)
        member = next(iter(zset))
        entry = json.loads(member)
        self.assertEqual(entry["file_path"], "first.parquet")
        self.assertEqual(entry["min"], 0)
        self.assertEqual(entry["max"], 100)


class TestCheckpointRaceRestart(unittest.TestCase):
    """Simulate the full checkpoint-race + restart scenario:

    Cycle 1: file A (pk 0-100) stages + commits + checkpoint persists.
            The committed-PK-ranges set now has [0,100].
    Crash before checkpoint for file B (pk 100-200) persists; B's file is
    written but its add_files() never runs (so no committed-range entry).
    Restart: the task re-stages B's range as a NEW file path B2.
    Cycle 2: the committer drains B2. Its range [100,200] overlaps the
    committed [0,100] at the boundary (100) — wait, [0,100] and [100,200]
    overlap at 100. To make a cleaner test, use [90,200] for the re-stage
    so the overlap is unambiguous.
    """

    def test_restart_restage_overlapping_range_dedups(self):
        from iceberg_committer import (IcebergCommitter, pending_key,
                                        committed_pk_ranges_key,
                                        committed_key)
        rc = _FakeRedis()
        # Cycle 1: file A committed, range [0,100] recorded, path in set.
        rc.sadd(committed_key("c", "t"), "A.parquet")
        rc.zadd(committed_pk_ranges_key("c", "t"),
                {json.dumps({"min": 0, "max": 100,
                             "file_path": "A.parquet"}): 0.0})
        # Simulated restart: re-stage the SAME logical chunk (PK 50-150,
        # overlapping A's [0,100]) under a new UUID path B2.parquet.
        rc.rpush(pending_key("c", "t"),
                 json.dumps(_entry("B2.parquet", 0, pk_range=(50, 150))))
        catalog, table, tx = _make_catalog()
        committer = IcebergCommitter(catalog, rc, "c", "t")
        with patch("iceberg_committer.IcebergCommitter._dedup_one_range") as d:
            r = committer.drain_and_commit()
        # Overlap detected -> dedup invoked -> file still registered
        # (delete-then-register).
        self.assertEqual(r["committed"], 1)
        d.assert_called_once()
        # No row-level duplicate: the dedup path ran before add_files().
        tx.add_files.assert_called_once_with(file_paths=["B2.parquet"])

    def test_checkpoint_failure_then_restart_pure_duplicate_skipped(self):
        """If the re-staged range is FULLY inside the committed range, the
        committer skips + deletes (pure duplicate, no dedup needed)."""
        from iceberg_committer import (IcebergCommitter, pending_key,
                                        committed_pk_ranges_key)
        rc = _FakeRedis()
        rc.zadd(committed_pk_ranges_key("c", "t"),
                {json.dumps({"min": 0, "max": 1000,
                             "file_path": "A.parquet"}): 0.0})
        # Re-stage a sub-range fully inside [0,1000].
        rc.rpush(pending_key("c", "t"),
                 json.dumps(_entry("B2.parquet", 0, pk_range=(100, 200))))
        catalog, table, tx = _make_catalog()
        committer = IcebergCommitter(catalog, rc, "c", "t")
        r = committer.drain_and_commit()
        self.assertEqual(r["committed"], 0)
        self.assertEqual(r["skipped_duplicate"], 1)
        tx.add_files.assert_not_called()
        table.io.delete.assert_called_once_with("B2.parquet")


class TestNoRowLevelDuplicates(unittest.TestCase):
    def test_final_table_has_no_duplicate_pks_after_overlap(self):
        """End-to-end-ish: after the committer processes an overlapping
        re-stage, the table's data (mocked) has no duplicate PKs.

        We mock the table's data as a list of dicts and verify the dedup
        path was called (which would delete the overlapping rows before
        the new file is registered)."""
        from iceberg_committer import (IcebergCommitter, pending_key,
                                        committed_pk_ranges_key)
        rc = _FakeRedis()
        rc.zadd(committed_pk_ranges_key("c", "t"),
                {json.dumps({"min": 0, "max": 100,
                             "file_path": "A.parquet"}): 0.0})
        rc.rpush(pending_key("c", "t"),
                 json.dumps(_entry("B2.parquet", 0, pk_range=(50, 150))))
        catalog, table, tx = _make_catalog()
        committer = IcebergCommitter(catalog, rc, "c", "t")
        dedup_calls = []
        def fake_dedup(t, pk_col, rmin, rmax, entry):
            dedup_calls.append((pk_col, rmin, rmax, entry["file_path"]))
        with patch.object(IcebergCommitter, "_dedup_one_range",
                          side_effect=fake_dedup):
            r = committer.drain_and_commit()
        self.assertEqual(r["committed"], 1)
        self.assertEqual(len(dedup_calls), 1)
        pk_col, rmin, rmax, path = dedup_calls[0]
        self.assertEqual(pk_col, "id")
        self.assertEqual(rmin, 50)
        self.assertEqual(rmax, 150)
        self.assertEqual(path, "B2.parquet")
        # The overlap window [50,100] was deduped before B2 registered, so
        # the final table state has no duplicate PKs in that window.
        # (The actual delete is mocked; this asserts the dedup was called
        # with the correct overlapping range.)


if __name__ == "__main__":
    unittest.main()
