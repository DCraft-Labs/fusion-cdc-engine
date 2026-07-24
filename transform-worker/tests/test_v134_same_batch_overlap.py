"""v1.3.4 Fix 3 — same-batch PK-range overlap dedup tests.

Verifies that ``IcebergCommitter._commit_entries`` checks each
candidate entry's pk_range against every OTHER entry already selected
for the same drain batch (in-memory), not just against already-
committed ranges. Without this check, a retry that re-stages a chunk
(new UUID, same PK range) before the original's file is committed
lands in the same drain cycle as the original and both get registered
→ row-level duplicates.

All tests mock the catalog and Redis (no live Nessie/MinIO required).
"""
from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock


class _FakeRedis:
    """In-memory Redis mock covering the ops the committer uses:
    rpush/lpop/brpop, set(nx, ex), eval (compare-and-delete),
    sismember/sadd, delete, zadd/zrangebyscore."""

    def __init__(self):
        self.kv: dict[str, object] = {}
        self.lists: dict[str, list[str]] = {}
        self.sets: dict[str, set[str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}

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
            removed = False
            if k in self.kv:
                del self.kv[k]; removed = True
            if k in self.lists:
                del self.lists[k]; removed = True
            if k in self.sets:
                del self.sets[k]; removed = True
            if k in self.zsets:
                del self.zsets[k]; removed = True
            if removed:
                n += 1
        return n

    def sismember(self, key, member):
        return member in self.sets.get(key, set())

    def sadd(self, key, *members):
        s = self.sets.setdefault(key, set())
        for m in members:
            s.add(m)
        return len(members)

    def eval(self, script, numkeys, *args):
        key, token = args[0], args[1]
        if self.kv.get(key) == token:
            del self.kv[key]
            return 1
        return 0

    def zadd(self, key, mapping):
        z = self.zsets.setdefault(key, {})
        for member, score in mapping.items():
            z[member] = float(score)
        return len(mapping)

    def zrangebyscore(self, key, lo, hi):
        z = self.zsets.get(key, {})
        out = []
        for member, score in z.items():
            if (lo == "-inf" or score >= float(lo)) and \
               (hi == "+inf" or hi == "inf" or score <= float(hi)):
                out.append(member)
        return out


def _entry(path, pk_range=None, chunk_seq=0, row_count=10, pk_col="id"):
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
    io = MagicMock()
    io.iterate.return_value = iter([])
    table.io = io
    snap = MagicMock()
    snap.manifests.return_value = iter([])
    table.current_snapshot.return_value = snap
    table.location.return_value = "s3://bkt/warehouse/fusion/t"
    return catalog, table, tx


class TestSameBatchOverlap(unittest.TestCase):
    def test_same_batch_exact_overlap_dedups(self):
        """Two entries with the SAME pk_range in the same drain batch
        (the retry-re-stage scenario): one is a pure duplicate and must
        be skipped + deleted, not registered."""
        from iceberg_committer import (IcebergCommitter, pending_key)
        rc = _FakeRedis()
        # Two entries with the same pk_range but different file paths
        # (the retry got a new UUID).
        rc.rpush(pending_key("c", "t"),
                 json.dumps(_entry("f-orig.parquet", pk_range=(100, 200))))
        rc.rpush(pending_key("c", "t"),
                 json.dumps(_entry("f-retry.parquet", pk_range=(100, 200))))
        catalog, table, tx = _make_catalog()
        committer = IcebergCommitter(catalog, rc, "c", "t")
        r = committer.drain_and_commit()
        self.assertEqual(r["drained"], 2)
        # Exactly one committed.
        self.assertEqual(r["committed"], 1)
        # One skipped as duplicate.
        self.assertEqual(r["skipped_duplicate"], 1)
        # ONE add_files call (one file registered).
        self.assertEqual(tx.add_files.call_count, 1)
        # The skipped file was deleted from object store.
        self.assertEqual(table.io.delete.call_count, 1)
        deleted_path = table.io.delete.call_args[0][0]
        self.assertIn(deleted_path, ("f-orig.parquet", "f-retry.parquet"))
        # Exactly one path in the committed set.
        committed_set = rc.sets.get("fusion:iceberg-committed-files:c:t", set())
        self.assertEqual(len(committed_set), 1)

    def test_same_batch_contained_dup_skips_smaller(self):
        """Candidate fully contained in an already-accepted entry →
        candidate is skipped + deleted (pure dup)."""
        from iceberg_committer import (IcebergCommitter, pending_key)
        rc = _FakeRedis()
        # Accepted first: wide range. Candidate second: narrow range
        # fully inside the wide one.
        rc.rpush(pending_key("c", "t"),
                 json.dumps(_entry("f-wide.parquet", pk_range=(100, 500))))
        rc.rpush(pending_key("c", "t"),
                 json.dumps(_entry("f-narrow.parquet", pk_range=(200, 300))))
        catalog, table, tx = _make_catalog()
        committer = IcebergCommitter(catalog, rc, "c", "t")
        r = committer.drain_and_commit()
        self.assertEqual(r["committed"], 1)
        self.assertEqual(r["skipped_duplicate"], 1)
        # The narrow (contained) file was deleted.
        deleted_path = table.io.delete.call_args[0][0]
        self.assertEqual(deleted_path, "f-narrow.parquet")

    def test_same_batch_non_overlapping_both_register(self):
        """Two entries with disjoint pk_ranges in the same batch: both
        register normally (no false-positive dedup)."""
        from iceberg_committer import (IcebergCommitter, pending_key)
        rc = _FakeRedis()
        rc.rpush(pending_key("c", "t"),
                 json.dumps(_entry("f-a.parquet", pk_range=(100, 200))))
        rc.rpush(pending_key("c", "t"),
                 json.dumps(_entry("f-b.parquet", pk_range=(300, 400))))
        catalog, table, tx = _make_catalog()
        committer = IcebergCommitter(catalog, rc, "c", "t")
        r = committer.drain_and_commit()
        self.assertEqual(r["committed"], 2)
        self.assertEqual(r["skipped_duplicate"], 0)
        self.assertEqual(tx.add_files.call_count, 2)
        self.assertEqual(table.io.delete.call_count, 0)

    def test_same_batch_partial_overlap_routes_to_dedup(self):
        """Partial overlap (neither contained in the other) routes the
        candidate to dedup-on-PK (best-effort, same as the committed-
        range partial-overlap case). Both register; dedup-on-PK runs."""
        from iceberg_committer import (IcebergCommitter, pending_key)
        rc = _FakeRedis()
        rc.rpush(pending_key("c", "t"),
                 json.dumps(_entry("f-a.parquet", pk_range=(100, 300))))
        rc.rpush(pending_key("c", "t"),
                 json.dumps(_entry("f-b.parquet", pk_range=(200, 400))))
        catalog, table, tx = _make_catalog()
        committer = IcebergCommitter(catalog, rc, "c", "t")
        r = committer.drain_and_commit()
        # Both register (partial overlap → dedup-on-PK, not skip).
        self.assertEqual(r["committed"], 2)
        self.assertEqual(r["skipped_duplicate"], 0)
        self.assertEqual(tx.add_files.call_count, 2)

    def test_same_batch_three_entries_two_dups(self):
        """Three entries, two sharing a pk_range with the first: only one
        of the duplicates survives, the other two are skipped."""
        from iceberg_committer import (IcebergCommitter, pending_key)
        rc = _FakeRedis()
        rc.rpush(pending_key("c", "t"),
                 json.dumps(_entry("f-1.parquet", pk_range=(100, 200))))
        rc.rpush(pending_key("c", "t"),
                 json.dumps(_entry("f-2.parquet", pk_range=(100, 200))))
        rc.rpush(pending_key("c", "t"),
                 json.dumps(_entry("f-3.parquet", pk_range=(100, 200))))
        catalog, table, tx = _make_catalog()
        committer = IcebergCommitter(catalog, rc, "c", "t")
        r = committer.drain_and_commit()
        self.assertEqual(r["committed"], 1)
        self.assertEqual(r["skipped_duplicate"], 2)
        self.assertEqual(tx.add_files.call_count, 1)
        # Two files deleted.
        self.assertEqual(table.io.delete.call_count, 2)

    def test_no_pk_range_skips_same_batch_check(self):
        """Entries with no pk_range (None) are not compared against
        each other (the same-batch check is best-effort for range-less
        entries; the committed-set path-dedup still applies)."""
        from iceberg_committer import (IcebergCommitter, pending_key)
        rc = _FakeRedis()
        rc.rpush(pending_key("c", "t"),
                 json.dumps(_entry("f-a.parquet", pk_range=None)))
        rc.rpush(pending_key("c", "t"),
                 json.dumps(_entry("f-b.parquet", pk_range=None)))
        catalog, table, tx = _make_catalog()
        committer = IcebergCommitter(catalog, rc, "c", "t")
        r = committer.drain_and_commit()
        self.assertEqual(r["committed"], 2)
        self.assertEqual(r["skipped_duplicate"], 0)


if __name__ == "__main__":
    unittest.main()
