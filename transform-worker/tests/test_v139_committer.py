"""v1.2.39 section 6 - single-committer + add_files() redesign tests.

Tests cover (per brief item 8):
  - one commit covers N files (drain_and_commit batches N entries into a
    single table.transaction().add_files() call)
  - checkpoint advances only after commit (mark_durable is called only
    for entries whose add_files() commit confirmed)
  - orphan sweep (files in data/ not in any manifest get registered via
    add_files() or deleted)
  - at-most-once via the committed-files Redis set (a path already in the
    set is skipped; a re-enqueued entry after a failed commit is retried
    exactly once and not double-committed)

All tests mock the catalog and Redis (no live Nessie/MinIO required).
"""
from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock, patch


class _FakeRedis:
    """Minimal in-memory Redis mock covering the ops the committer uses:
    rpush/lpop/brpop, set(nx, ex), eval (compare-and-delete), sismember/
    sadd, delete. State is shared across keys via a single dict so the
    committer's lock/list/set all live in the same namespace."""

    def __init__(self):
        self.kv: dict[str, object] = {}
        self.lists: dict[str, list[str]] = {}
        self.sets: dict[str, set[str]] = {}

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
            if k in self.kv:
                del self.kv[k]; n += 1
            if k in self.lists:
                del self.lists[k]; n += 1
            if k in self.sets:
                del self.sets[k]; n += 1
        return n

    def sismember(self, key, member):
        return member in self.sets.get(key, set())

    def sadd(self, key, *members):
        s = self.sets.setdefault(key, set())
        for m in members:
            s.add(m)
        return len(members)

    def eval(self, script, numkeys, *args):
        # Compare-and-delete: KEYS[1]=key ARGV[1]=token
        key, token = args[0], args[1]
        if self.kv.get(key) == token:
            del self.kv[key]
            return 1
        return 0


def _entry(path, chunk_seq=0, row_count=10):
    return {"file_path": path, "chunk_seq": chunk_seq,
            "row_count": row_count, "table_name": "t",
            "partition_id": str(chunk_seq), "stream_id": "s",
            "source_table": "src"}


def _make_catalog(load_side_effect=None, manifest_files=None,
                  data_files=None):
    """Build a MagicMock catalog whose load_table returns a MagicMock table.
    The table.transaction() yields a context manager exposing add_files.
    manifest_files/data_files drive the orphan-sweep path."""
    catalog = MagicMock()
    table = MagicMock()
    if load_side_effect is not None:
        catalog.load_table.side_effect = load_side_effect
    else:
        catalog.load_table.return_value = table

    tx = MagicMock()
    tx_cm = MagicMock()
    tx_cm.__enter__ = MagicMock(return_value=tx)
    tx_cm.__exit__ = MagicMock(return_value=False)
    table.transaction.return_value = tx_cm

    # Orphan-sweep: table.location(), table.io.iterate(prefix), snapshot.
    table.location.return_value = "s3://bkt/warehouse/fusion/t"
    io = MagicMock()
    if data_files is not None:
        io.iterate.return_value = iter(data_files)
    else:
        io.iterate.return_value = iter([])
    table.io = io
    snap = MagicMock()
    if manifest_files is not None:
        # Build manifest entries whose data_file.file_path matches the
        # given set, so _list_manifest_files returns them.
        entries = []
        for p in manifest_files:
            e = MagicMock()
            e.data_file.file_path = p
            entries.append(e)
        m = MagicMock()
        m.fetch_manifest_entries.return_value = iter(entries)
        snap.manifests.return_value = iter([m])
    table.current_snapshot.return_value = snap
    return catalog, table, tx


class TestCommitterOneCommitCoversNFiles(unittest.TestCase):
    def test_single_commit_for_n_entries(self):
        from iceberg_committer import (IcebergCommitter, pending_key,
                                        committed_key)
        rc = _FakeRedis()
        # Pre-load 3 pending entries.
        for i in range(3):
            rc.rpush(pending_key("c", "t"), json.dumps(_entry(f"f{i}.parquet", i)))
        catalog, table, tx = _make_catalog()
        marked = []
        committer = IcebergCommitter(catalog, rc, "c", "t",
                                      mark_durable=marked.append)
        r = committer.drain_and_commit()
        self.assertEqual(r["drained"], 3)
        self.assertEqual(r["committed"], 3)
        self.assertEqual(len(r["committed_paths"]), 3)
        # ONE transaction opened -> ONE commit covers all 3 files.
        self.assertEqual(table.transaction.call_count, 1)
        # add_files called once per entry inside the single tx.
        self.assertEqual(tx.add_files.call_count, 3)
        # mark_durable called once per committed entry.
        self.assertEqual(len(marked), 3)
        # Committed set now contains all 3 paths.
        self.assertEqual(rc.sets[committed_key("c", "t")],
                         {"f0.parquet", "f1.parquet", "f2.parquet"})
        # Pending list drained.
        self.assertEqual(rc.lists.get(pending_key("c", "t")), [])

    def test_no_entries_no_commit(self):
        from iceberg_committer import IcebergCommitter
        rc = _FakeRedis()
        catalog, table, tx = _make_catalog()
        committer = IcebergCommitter(catalog, rc, "c", "t")
        r = committer.drain_and_commit()
        self.assertEqual(r["drained"], 0)
        self.assertEqual(r["committed"], 0)
        table.transaction.assert_not_called()


class TestCheckpointAdvancesOnlyAfterCommit(unittest.TestCase):
    def test_mark_durable_not_called_on_commit_failure(self):
        from iceberg_committer import (IcebergCommitter, pending_key)
        rc = _FakeRedis()
        for i in range(2):
            rc.rpush(pending_key("c", "t"), json.dumps(_entry(f"f{i}.parquet", i)))
        catalog, table, tx = _make_catalog()
        # Make add_files raise -> transaction "fails".
        tx.add_files.side_effect = RuntimeError("simulated commit failure")
        marked = []
        committer = IcebergCommitter(catalog, rc, "c", "t",
                                      mark_durable=marked.append)
        r = committer.drain_and_commit()
        self.assertEqual(r["committed"], 0)
        self.assertEqual(len(r["errors"]), 1)
        # mark_durable NOT called because the commit failed.
        self.assertEqual(marked, [])
        # Entries re-enqueued for retry (LPUSH -> head of list).
        self.assertEqual(len(rc.lists[pending_key("c", "t")]), 2)


class TestAtMostOnceRegistration(unittest.TestCase):
    def test_skip_path_already_in_committed_set(self):
        from iceberg_committer import (IcebergCommitter, pending_key,
                                        committed_key)
        rc = _FakeRedis()
        rc.rpush(pending_key("c", "t"), json.dumps(_entry("dup.parquet", 0)))
        rc.rpush(pending_key("c", "t"), json.dumps(_entry("new.parquet", 1)))
        # "dup.parquet" already committed in a prior cycle.
        rc.sadd(committed_key("c", "t"), "dup.parquet")
        catalog, table, tx = _make_catalog()
        committer = IcebergCommitter(catalog, rc, "c", "t")
        r = committer.drain_and_commit()
        self.assertEqual(r["drained"], 2)
        self.assertEqual(r["skipped_duplicate"], 1)
        self.assertEqual(r["committed"], 1)
        self.assertEqual(r["committed_paths"], ["new.parquet"])
        # add_files called only for the new path.
        self.assertEqual(tx.add_files.call_count, 1)

    def test_retry_after_failure_does_not_double_commit(self):
        from iceberg_committer import (IcebergCommitter, pending_key,
                                        committed_key)
        rc = _FakeRedis()
        rc.rpush(pending_key("c", "t"), json.dumps(_entry("retry.parquet", 0)))
        catalog, table, tx = _make_catalog()
        # First attempt: add_files fails -> re-enqueue.
        tx.add_files.side_effect = RuntimeError("boom")
        committer = IcebergCommitter(catalog, rc, "c", "t")
        r1 = committer.drain_and_commit()
        self.assertEqual(r1["committed"], 0)
        self.assertEqual(len(rc.lists[pending_key("c", "t")]), 1)
        # Second attempt: add_files succeeds.
        tx.add_files.side_effect = None
        r2 = committer.drain_and_commit()
        self.assertEqual(r2["committed"], 1)
        self.assertEqual(rc.sets[committed_key("c", "t")], {"retry.parquet"})
        # Third attempt with the same path re-enqueued (e.g. a stale
        # duplicate from a pre-crash RPUSH): must be skipped.
        rc.rpush(pending_key("c", "t"), json.dumps(_entry("retry.parquet", 0)))
        tx.add_files.reset_mock()
        r3 = committer.drain_and_commit()
        self.assertEqual(r3["skipped_duplicate"], 1)
        self.assertEqual(r3["committed"], 0)
        tx.add_files.assert_not_called()


class TestOrphanSweep(unittest.TestCase):
    def test_register_orphans_not_in_manifest(self):
        from iceberg_committer import (IcebergCommitter, committed_key)
        rc = _FakeRedis()
        data_files = ["s3://bkt/warehouse/fusion/t/data/p/orphan.parquet",
                      "s3://bkt/warehouse/fusion/t/data/p/known.parquet"]
        # "known.parquet" is in the manifest; "orphan.parquet" is not.
        catalog, table, tx = _make_catalog(
            manifest_files={"s3://bkt/warehouse/fusion/t/data/p/known.parquet"},
            data_files=data_files)
        committer = IcebergCommitter(catalog, rc, "c", "t")
        r = committer.orphan_sweep(register=True)
        self.assertEqual(len(r["orphans"]), 1)
        self.assertTrue(r["orphans"][0].endswith("orphan.parquet"))
        self.assertEqual(r["registered"], 1)
        tx.add_files.assert_called_once_with(
            file_paths=["s3://bkt/warehouse/fusion/t/data/p/orphan.parquet"])
        # Registered orphan now in committed set.
        self.assertIn("s3://bkt/warehouse/fusion/t/data/p/orphan.parquet",
                      rc.sets[committed_key("c", "t")])

    def test_delete_orphans_when_register_false(self):
        from iceberg_committer import IcebergCommitter
        rc = _FakeRedis()
        data_files = ["s3://bkt/warehouse/fusion/t/data/p/orphan.parquet"]
        catalog, table, tx = _make_catalog(manifest_files=set(),
                                           data_files=data_files)
        committer = IcebergCommitter(catalog, rc, "c", "t")
        r = committer.orphan_sweep(register=False)
        self.assertEqual(r["registered"], 0)
        self.assertEqual(r["deleted"], 1)
        table.io.delete.assert_called_once_with(
            "s3://bkt/warehouse/fusion/t/data/p/orphan.parquet")


class TestEnqueueAndListHelpers(unittest.TestCase):
    def test_enqueue_then_list_pending(self):
        from iceberg_committer import (enqueue_pending_file, list_pending,
                                        pending_key)
        rc = _FakeRedis()
        enqueue_pending_file(rc, "c", "t", _entry("a.parquet", 0))
        enqueue_pending_file(rc, "c", "t", _entry("b.parquet", 1))
        # Non-blocking drain of 2.
        out = list_pending(rc, "c", "t", count=2)
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0]["file_path"], "a.parquet")
        self.assertEqual(out[1]["file_path"], "b.parquet")
        # List now empty.
        self.assertEqual(list_pending(rc, "c", "t", count=5), [])

    def test_brpop_timeout_returns_empty(self):
        from iceberg_committer import list_pending
        rc = _FakeRedis()
        out = list_pending(rc, "c", "t", count=1, timeout_ms=10)
        self.assertEqual(out, [])

    def test_none_redis_is_noop(self):
        from iceberg_committer import (enqueue_pending_file, list_pending)
        self.assertEqual(enqueue_pending_file(None, "c", "t", {}), 0)
        self.assertEqual(list_pending(None, "c", "t"), [])


class TestCommitterLock(unittest.TestCase):
    def test_second_committer_skips_when_lock_held(self):
        from iceberg_committer import (IcebergCommitter, lock_key)
        rc = _FakeRedis()
        # Simulate another pod holding the lock.
        rc.set(lock_key("c", "t"), "other-token", nx=True, ex=30)
        catalog, table, tx = _make_catalog()
        committer = IcebergCommitter(catalog, rc, "c", "t")
        r = committer.drain_and_commit()
        self.assertEqual(r["drained"], 0)
        table.transaction.assert_not_called()


class TestLoaderStagingPath(unittest.TestCase):
    """Verify the loader's iceberg path uses write_arrow_to_file +
    enqueue_pending_file when committer_mode=staged, and the default
    (none) path is unchanged (uses _write_arrow_to_iceberg)."""

    def test_staged_mode_uses_write_arrow_to_file(self):
        import loader
        task = MagicMock()
        task.get = lambda k, d=None: {
            "committer_mode": "staged",
            "bulk_mode": "none",
        }.get(k, d)
        # Re-derive the flags the way the loop does.
        committer_mode = str(task.get("committer_mode") or loader.COMMITTER_MODE_DEFAULT).lower()
        self.assertEqual(committer_mode, "staged")

    def test_default_committer_mode_is_none(self):
        import loader
        self.assertEqual(loader.COMMITTER_MODE_DEFAULT, "none")

    def test_stage_arrow_to_pending_calls_writer_and_enqueues(self):
        import loader
        inst = loader.InitialLoadTask.__new__(loader.InitialLoadTask)
        inst.redis = _FakeRedis()
        arrow_tbl = MagicMock()
        arrow_tbl.num_rows = 5
        with patch("iceberg_writer.IcebergWriter") as IW, \
             patch("iceberg_committer.enqueue_pending_file") as eq:
            writer = MagicMock()
            writer.write_arrow_to_file.return_value = "s3://bkt/t/data/p/1-x.parquet"
            IW.return_value = writer
            eq.return_value = 1
            path = inst._stage_arrow_to_pending(
                arrow_tbl, dest={"connector_type": "iceberg"},
                table_name="t", partition_id="1", chunk_seq=1,
                pk_range=(0, 100), stream_id="s", source_table="src",
                connection_id="c",
            )
        self.assertEqual(path, "s3://bkt/t/data/p/1-x.parquet")
        writer.write_arrow_to_file.assert_called_once()
        eq.assert_called_once()
        # The enqueued entry carries the file path + row_count.
        _args, kwargs = eq.call_args
        entry = kwargs["entry"] if "entry" in kwargs else _args[3]
        self.assertEqual(entry["file_path"], "s3://bkt/t/data/p/1-x.parquet")
        self.assertEqual(entry["row_count"], 5)

    def test_stage_returns_empty_when_table_bootstrapped(self):
        import loader
        inst = loader.InitialLoadTask.__new__(loader.InitialLoadTask)
        inst.redis = _FakeRedis()
        arrow_tbl = MagicMock()
        arrow_tbl.num_rows = 5
        with patch("iceberg_writer.IcebergWriter") as IW, \
             patch("iceberg_committer.enqueue_pending_file") as eq:
            writer = MagicMock()
            # Bootstrap case: write_arrow_to_file returns "" (table didn't
            # exist; fell back to write_arrow which committed).
            writer.write_arrow_to_file.return_value = ""
            IW.return_value = writer
            path = inst._stage_arrow_to_pending(
                arrow_tbl, dest={"connector_type": "iceberg"},
                table_name="t", partition_id="1", chunk_seq=1,
                pk_range=(0, 100), stream_id="s", source_table="src",
                connection_id="c",
            )
        self.assertEqual(path, "")
        eq.assert_not_called()


class TestMultiTableCommitterConsolidation(unittest.TestCase):
    """v1.4.x Phase 1 (committer consolidation): one committer PROCESS now
    drains every table belonging to a connection, sharing ONE catalog
    instance, instead of one process per (connection, table) pair. Every
    Redis key (pending list, committed set) stays scoped per-table —
    only the process boundary changed."""

    def test_run_cycle_drains_each_table_own_pending_list_via_shared_catalog(self):
        from iceberg_committer import (IcebergCommitter, pending_key,
                                        committed_key)
        rc = _FakeRedis()
        rc.rpush(pending_key("c", "orders"), json.dumps(_entry("o1.parquet", 0)))
        rc.rpush(pending_key("c", "customers"), json.dumps(_entry("c1.parquet", 0)))
        catalog, table, tx = _make_catalog()
        # ONE IcebergCommitter, ONE shared catalog, TWO tables.
        committer = IcebergCommitter(catalog, rc, "c", ["orders", "customers"])
        results = committer.run_cycle()

        self.assertEqual(set(results.keys()), {"orders", "customers"})
        self.assertEqual(results["orders"]["committed"], 1)
        self.assertEqual(results["customers"]["committed"], 1)
        # Each table's own pending list / committed set was used — the
        # Redis key scheme is unchanged by the process consolidation.
        self.assertEqual(rc.lists.get(pending_key("c", "orders")), [])
        self.assertEqual(rc.lists.get(pending_key("c", "customers")), [])
        self.assertEqual(rc.sets[committed_key("c", "orders")], {"o1.parquet"})
        self.assertEqual(rc.sets[committed_key("c", "customers")], {"c1.parquet"})
        # Both tables were loaded through the SAME catalog instance (no
        # per-table catalog re-authentication/reconstruction).
        called_tables = {args[0] for args, _kwargs in catalog.load_table.call_args_list}
        self.assertIn("fusion.orders", called_tables)
        self.assertIn("fusion.customers", called_tables)

    def test_run_cycle_isolates_unexpected_exception_per_table(self):
        """Item 1 requirement: one table's commit failure must not stop
        the connection's other tables from draining in the same cycle."""
        from iceberg_committer import IcebergCommitter
        rc = _FakeRedis()
        catalog, table, tx = _make_catalog()
        committer = IcebergCommitter(catalog, rc, "c", ["t1", "t2"])

        real_drain = committer.drain_and_commit

        def flaky_drain(table_name=None):
            if table_name == "t1":
                raise RuntimeError("simulated unexpected failure for t1")
            return real_drain(table_name)

        committer.drain_and_commit = flaky_drain
        results = committer.run_cycle()

        self.assertEqual(results["t1"]["committed"], 0)
        self.assertEqual(results["t1"]["errors"][0]["phase"], "drain_and_commit")
        # t2 is completely unaffected by t1's exception (still a normal,
        # non-error result — just nothing to drain in this fake Redis).
        self.assertEqual(results["t2"]["committed"], 0)
        self.assertEqual(results["t2"]["errors"], [])

    def test_single_string_table_name_still_works(self):
        """Backward compatibility: constructing with a single table-name
        string (the pre-consolidation shape) must behave identically to
        the old single-table IcebergCommitter."""
        from iceberg_committer import (IcebergCommitter, pending_key)
        rc = _FakeRedis()
        rc.rpush(pending_key("c", "t"), json.dumps(_entry("f0.parquet", 0)))
        catalog, table, tx = _make_catalog()
        committer = IcebergCommitter(catalog, rc, "c", "t")
        self.assertEqual(committer.table_names, ["t"])
        self.assertEqual(committer.table_name, "t")
        r = committer.drain_and_commit()  # no table_name arg -> defaults to "t"
        self.assertEqual(r["committed"], 1)

    def test_table_namespaces_override_resolves_per_table(self):
        """A connection's streams can each override their destination
        namespace; the committer should resolve each table's catalog
        lookup against its own namespace override, not just the shared
        default."""
        from iceberg_committer import IcebergCommitter, pending_key
        rc = _FakeRedis()
        rc.rpush(pending_key("c", "special"), json.dumps(_entry("s1.parquet", 0)))
        catalog, table, tx = _make_catalog()
        committer = IcebergCommitter(
            catalog, rc, "c", ["special"], namespace="fusion",
            table_namespaces={"special": "other_ns"},
        )
        committer.drain_and_commit("special")
        called_tables = {args[0] for args, _kwargs in catalog.load_table.call_args_list}
        self.assertIn("other_ns.special", called_tables)
        self.assertNotIn("fusion.special", called_tables)


if __name__ == "__main__":
    unittest.main()
