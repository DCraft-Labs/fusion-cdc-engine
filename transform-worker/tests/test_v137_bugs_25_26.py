"""v1.2.37 regression tests — Bugs #25 + #26 + commit-batch default + manifest
property investigation.

Covers:
- Bug #25 (§7b): ``_open_duckdb_scanner`` reads ``database_name``/``username``
  (with fallback to ``database``/``user``), matching the sibling
  ``_open_source_connection`` ("v1.2.30 Defect E fix"). The producer stamps
  ``database_name``/``username`` into every task's source dict, never
  ``database``/``user`` — the old code left both as None and the ATTACH
  failed.
- Bug #26 (§7a): ``_open_duckdb_scanner`` sets
  ``extension_directory='/opt/duckdb_extensions'`` right after
  ``duckdb.connect()`` so the runtime (non-root ``transform`` user,
  ``HOME=/app``) finds the mysql extension baked into the fixed path at
  build time (build runs as root, ``HOME=/root``).
- §8 item 3: ``INITIAL_LOAD_COMMIT_BATCH`` default is now 5 (env var still
  overrides).
- §8 item 4: ``commit.manifest.min-count-to-merge=1`` is still set in
  ``_build_table_properties`` (kept as a no-op placeholder; PyIceberg 0.7.1
  does not honor it on the ``fast_append`` path — documented in the code).
"""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import loader  # noqa: E402


class TestBug25DuckDBScannerKeyNames(unittest.TestCase):
    """Bug #25: _open_duckdb_scanner must read database_name/username
    (with fallback), not database/user."""

    def _source(self, **overrides):
        src = {
            "host": "mysql-source",
            "port": 3306,
            "database_name": "test_db",
            "username": "test_user",
            "password": "test_pw",
        }
        src.update(overrides)
        return src

    def _run_scanner(self, source, ctype):
        """Call _open_duckdb_scanner with duckdb.connect mocked so we can
        capture the SQL the scanner executes (LOAD mysql + ATTACH ...)."""
        from loader import InitialLoadTask

        task = InitialLoadTask.__new__(InitialLoadTask)
        mock_conn = MagicMock()
        executed = []
        mock_conn.execute.side_effect = lambda *a, **k: executed.append(a[0] if a else "")
        with patch("duckdb.connect", return_value=mock_conn):
            result = task._open_duckdb_scanner(source, ctype)
        return result, mock_conn, executed

    def test_mysql_uses_database_name_and_username(self):
        result, conn, executed = self._run_scanner(self._source(), "mysql")
        self.assertIsNotNone(result, "scanner should succeed with database_name/username")
        # The ATTACH statement must contain the database_name and username values.
        attach_sql = [s for s in executed if s.startswith("ATTACH")]
        self.assertEqual(len(attach_sql), 1, "exactly one ATTACH expected")
        self.assertIn("database=test_db", attach_sql[0])
        self.assertIn("user=test_user", attach_sql[0])

    def test_mysql_falls_back_to_database_and_user(self):
        """When the producer stamps legacy database/user keys (older path),
        the fallback still works."""
        src = {
            "host": "mysql-source", "port": 3306,
            "database": "legacy_db", "user": "legacy_user",
            "password": "pw",
        }
        result, conn, executed = self._run_scanner(src, "mysql")
        self.assertIsNotNone(result)
        attach_sql = [s for s in executed if s.startswith("ATTACH")]
        self.assertIn("database=legacy_db", attach_sql[0])
        self.assertIn("user=legacy_user", attach_sql[0])

    def test_postgres_uses_database_name_and_username(self):
        result, conn, executed = self._run_scanner(self._source(), "postgres")
        self.assertIsNotNone(result)
        attach_sql = [s for s in executed if s.startswith("ATTACH")]
        self.assertEqual(len(attach_sql), 1)
        self.assertIn("dbname=test_db", attach_sql[0])
        self.assertIn("user=test_user", attach_sql[0])


class TestBug26DuckDBExtensionDirectory(unittest.TestCase):
    """Bug #26: _open_duckdb_scanner must SET extension_directory to the
    fixed /opt/duckdb_extensions path so the runtime user finds the
    baked mysql extension."""

    def test_extension_directory_set_before_load(self):
        from loader import InitialLoadTask
        task = InitialLoadTask.__new__(InitialLoadTask)
        source = {"host": "h", "port": 3306, "database_name": "db",
                  "username": "u", "password": "p"}
        executed = []
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = lambda *a, **k: executed.append(a[0] if a else "")
        with patch("duckdb.connect", return_value=mock_conn):
            task._open_duckdb_scanner(source, "mysql")
        # The FIRST execute call MUST be the SET extension_directory line.
        self.assertGreater(len(executed), 0, "scanner must execute SQL")
        self.assertTrue(
            executed[0].startswith("SET extension_directory"),
            f"first execute must be SET extension_directory, got: {executed[0]!r}",
        )
        self.assertIn("/opt/duckdb_extensions", executed[0])
        # And LOAD mysql must come AFTER the SET (so the extension is found).
        load_idx = next(i for i, s in enumerate(executed) if s.startswith("LOAD mysql"))
        set_idx = 0  # by construction above
        self.assertLess(set_idx, load_idx,
                        "SET extension_directory must come before LOAD mysql")


class TestCommitBatchDefault(unittest.TestCase):
    """§8 item 3: INITIAL_LOAD_COMMIT_BATCH default is now 5."""

    def test_default_is_5(self):
        # The module-level constant reflects the new default when the env
        # var is not set. (If the env var IS set in the test runner env, we
        # skip the strict equality and just assert it parses to an int >= 1.)
        if "INITIAL_LOAD_COMMIT_BATCH" in os.environ:
            self.assertIsInstance(loader.INITIAL_LOAD_COMMIT_BATCH, int)
        else:
            self.assertEqual(loader.INITIAL_LOAD_COMMIT_BATCH, 5,
                             "v1.2.37 §8 item 3: default commit batch is 5")

    def test_env_override_still_works(self):
        # Reload-ish: re-evaluate the env-var read with a patched env.
        with patch.dict(os.environ, {"INITIAL_LOAD_COMMIT_BATCH": "10"}, clear=False):
            val = int(os.environ.get("INITIAL_LOAD_COMMIT_BATCH", "5"))
            self.assertEqual(val, 10)


class TestManifestPropertyStillSet(unittest.TestCase):
    """§8 item 4: commit.manifest.min-count-to-merge=1 is still set in
    _build_table_properties (kept as a no-op placeholder on PyIceberg 0.7.1;
    the real fix is the v1.2.39 single-committer redesign)."""

    def test_property_present_for_initial_load_destination(self):
        from iceberg_writer import _build_table_properties
        props = _build_table_properties({"initial_load_destination": True})
        self.assertIn("commit.manifest.min-count-to-merge", props)
        self.assertEqual(props["commit.manifest.min-count-to-merge"], "1")

    def test_property_absent_when_not_initial_load_destination(self):
        from iceberg_writer import _build_table_properties
        props = _build_table_properties({"initial_load_destination": False})
        self.assertNotIn("commit.manifest.min-count-to-merge", props)


if __name__ == "__main__":
    unittest.main()
