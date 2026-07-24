"""v1.2.40 section 6f - hardening tests (Findings C + D).

Finding C (SQL injection):
  - _apply_string_op: a malicious config value containing a single quote
    cannot escape the string literal (quotes are doubled).
  - _apply_string_op: a malicious column name (e.g. "id; DROP TABLE
    staging; --") is rejected by _validate_identifier.
  - _apply_expression: the FUSION_EXPRESSION_ALLOWLIST env var rejects
    expressions not matching any allowlisted substring.

Finding D (UDF cache):
  - _apply_udf fetches UDF source over HTTP at most once per
    (udf_name, udf_registry_url); subsequent calls hit the in-process
    cache (no HTTP, no exec).
  - clear_udf_cache resets the cache.
"""
from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock, patch

import duckdb


class TestStringOpSqlInjection(unittest.TestCase):
    def _run_string_op_real(self, step):
        """Run _apply_string_op against a REAL in-memory DuckDB connection
        with a ``staging`` table pre-populated. This is the gold-standard
        way to verify SQL injection is neutralized: if the payload escaped
        the string literal, DuckDB would either raise or actually execute
        the injected statement (and ``staging`` would be gone/altered)."""
        import engine
        conn = duckdb.connect()
        conn.execute("CREATE TABLE staging (name VARCHAR)")
        conn.execute("INSERT INTO staging VALUES ('hello')")
        engine._apply_string_op(conn, step)
        return conn

    def test_malicious_from_value_cannot_escape_string_literal(self):
        """A replace ``from`` value containing ``'`` must be escaped (quotes
        doubled) so it cannot break out of the SQL string literal. We verify
        by running against a real DuckDB: the UPDATE must succeed and the
        ``staging`` table must still exist (i.e. no injected DROP)."""
        import engine
        conn = self._run_string_op_real({
            "column": "name", "op": "replace",
            "params": {"from": "'; DROP TABLE staging; --", "to": "x"},
        })
        # Table still exists (no DROP executed) and has the row.
        rows = conn.execute("SELECT name FROM staging").fetchall()
        self.assertEqual(len(rows), 1)
        # The replace ran: the literal payload wasn't found in 'hello', so
        # the value is unchanged.
        self.assertEqual(rows[0][0], "hello")

    def test_malicious_to_value_escaped(self):
        import engine
        conn = self._run_string_op_real({
            "column": "name", "op": "replace",
            "params": {"from": "hello", "to": "b'); DROP TABLE staging; --"},
        })
        rows = conn.execute("SELECT name FROM staging").fetchall()
        self.assertEqual(len(rows), 1)
        # The replace ran and the to-value (with its quotes) was inserted
        # as a literal — the table is intact.
        self.assertEqual(rows[0][0], "b'); DROP TABLE staging; --")

    def test_concat_suffix_escaped(self):
        import engine
        conn = self._run_string_op_real({
            "column": "name", "op": "concat",
            "params": {"suffix": "' || (SELECT 999) || '"},
        })
        rows = conn.execute("SELECT name FROM staging").fetchall()
        self.assertEqual(len(rows), 1)
        # The suffix is a literal string, not executed SQL — so the result
        # is 'hello' + the literal text, NOT 'hello' || 999.
        self.assertEqual(rows[0][0], "hello' || (SELECT 999) || '")

    def test_lpad_pad_escaped(self):
        import engine
        conn = self._run_string_op_real({
            "column": "name", "op": "lpad",
            "params": {"length": 20, "pad": "'); DROP TABLE staging; --"},
        })
        rows = conn.execute("SELECT name FROM staging").fetchall()
        self.assertEqual(len(rows), 1)
        # Table intact; lpad ran with the pad literal (no DROP executed).
        self.assertEqual(len(rows[0][0]), 20)

    def test_malicious_column_name_rejected(self):
        """A column name like ``id; DROP TABLE staging; --`` must be rejected
        by _validate_identifier before any SQL is executed."""
        import engine
        conn = MagicMock()
        with self.assertRaises(ValueError) as cm:
            engine._apply_string_op(conn, {"column": "id; DROP TABLE staging; --",
                                            "op": "upper"})
        self.assertIn("invalid SQL identifier", str(cm.exception))

    def test_malicious_output_column_rejected(self):
        import engine
        conn = MagicMock()
        with self.assertRaises(ValueError):
            engine._apply_string_op(conn, {"column": "name", "op": "upper",
                                            "output_column": "x; DROP TABLE staging"})

    def test_valid_column_names_accepted(self):
        """Sanity: legitimate identifiers (letters, digits, underscores,
        leading underscore) are accepted."""
        import engine
        for name in ["name", "user_id", "_internal", "Col1", "a_b_c"]:
            conn = duckdb.connect()
            conn.execute(f"CREATE TABLE staging ({name} VARCHAR)")
            conn.execute(f"INSERT INTO staging VALUES ('hello')")
            engine._apply_string_op(conn, {"column": name, "op": "upper"})
            rows = conn.execute(f"SELECT {name} FROM staging").fetchall()
            self.assertEqual(rows[0][0], "HELLO")

    def test_substring_non_numeric_start_rejected(self):
        """A non-numeric substring ``start`` must be rejected (int() raises
        ValueError) rather than interpolated into the SQL."""
        import engine
        conn = MagicMock()
        with self.assertRaises((ValueError, TypeError)):
            engine._apply_string_op(conn, {"column": "name", "op": "substring",
                                            "params": {"start": "1; DROP TABLE x"}})


class TestExpressionAllowlist(unittest.TestCase):
    def _run_expr(self, step, allowlist=None):
        import engine
        conn = MagicMock()
        env = {"FUSION_EXPRESSION_ALLOWLIST": allowlist or ""}
        with patch.dict(os.environ, env, clear=False):
            engine._apply_expression(conn, step)
        return conn

    def test_allowlist_rejects_non_matching_expression(self):
        import engine
        with self.assertRaises(ValueError) as cm:
            self._run_expr({"expression": "DROP TABLE staging",
                             "output_column": "r"},
                            allowlist="CASE WHEN;CAST(")
        self.assertIn("allowlisted", str(cm.exception).lower())

    def test_allowlist_accepts_matching_expression(self):
        import engine
        conn = self._run_expr({"expression": "CASE WHEN status='active' THEN 1 ELSE 0 END",
                                "output_column": "r"},
                               allowlist="CASE WHEN;CAST(")
        conn.execute.assert_called()

    def test_no_allowlist_accepts_anything(self):
        """Without the env var set, any expression is accepted (the trust
        boundary is the admin role, documented in the docstring)."""
        import engine
        env = {"FUSION_EXPRESSION_ALLOWLIST": ""}
        with patch.dict(os.environ, env, clear=True):
            # Remove the key entirely so .strip() default "" applies.
            os.environ.pop("FUSION_EXPRESSION_ALLOWLIST", None)
            conn = MagicMock()
            engine._apply_expression(conn, {"expression": "1+1",
                                              "output_column": "r"})
            conn.execute.assert_called()

    def test_expression_output_column_validated(self):
        import engine
        with self.assertRaises(ValueError):
            self._run_expr({"expression": "1+1",
                             "output_column": "r; DROP TABLE staging"})


class TestUDFCache(unittest.TestCase):
    def setUp(self):
        import engine
        engine.clear_udf_cache()

    def tearDown(self):
        import engine
        engine.clear_udf_cache()

    def test_udf_fetched_once_per_name(self):
        """Two _apply_udf calls for the same UDF must result in exactly ONE
        HTTP fetch (the second call hits the in-process cache)."""
        import engine
        with patch("engine.requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.json.return_value = {
                "code": "def myudf(x):\n    return x.upper()",
                "version": "1",
            }
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            conn = MagicMock()
            step = {"function": "myudf", "args": ["name"],
                    "output_column": "r", "return_type": "string"}
            engine._apply_udf(conn, step, udf_registry_url="http://cp")
            engine._apply_udf(conn, step, udf_registry_url="http://cp")

        # Only ONE HTTP fetch despite two _apply_udf calls.
        self.assertEqual(mock_get.call_count, 1)

    def test_different_udf_names_fetched_separately(self):
        import engine
        with patch("engine.requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.json.side_effect = [
                {"code": "def a(x):\n    return x", "version": "1"},
                {"code": "def b(x):\n    return x", "version": "1"},
            ]
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            conn = MagicMock()
            engine._apply_udf(conn, {"function": "a", "args": ["c"],
                                       "output_column": "r"},
                               udf_registry_url="http://cp")
            engine._apply_udf(conn, {"function": "b", "args": ["c"],
                                       "output_column": "r"},
                               udf_registry_url="http://cp")
        self.assertEqual(mock_get.call_count, 2)

    def test_clear_udf_cache_forces_refetch(self):
        import engine
        with patch("engine.requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.json.return_value = {
                "code": "def myudf(x):\n    return x", "version": "1",
            }
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            conn = MagicMock()
            step = {"function": "myudf", "args": ["c"],
                    "output_column": "r", "return_type": "string"}
            engine._apply_udf(conn, step, udf_registry_url="http://cp")
            engine.clear_udf_cache()
            engine._apply_udf(conn, step, udf_registry_url="http://cp")
        self.assertEqual(mock_get.call_count, 2)

    def test_udf_output_column_validated(self):
        import engine
        with patch("engine.requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.json.return_value = {
                "code": "def myudf(x):\n    return x", "version": "1",
            }
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp
            conn = MagicMock()
            with self.assertRaises(ValueError):
                engine._apply_udf(conn, {"function": "myudf", "args": ["c"],
                                          "output_column": "r; DROP TABLE x"},
                                   udf_registry_url="http://cp")


if __name__ == "__main__":
    unittest.main()
