"""v1.3.1 Fix 1 — integration test: execute the ATTACH string produced by
``_duckdb_attach_kv`` against a real in-process DuckDB.

The v1.3.0 unit tests (``test_v130_attach_escape.py``) only verified the
escape/unescape round-trip in isolation. They missed the separator
regression: ``_duckdb_attach_kv`` joined key=value pairs with ``;``, but
DuckDB's mysql_scanner (and postgres_scanner) DSN parser expects
SPACE-separated key=value pairs. A ``;``-separated ATTACH is rejected at
parse time with ``Invalid Input Error: Unrecognized configuration
parameter ""``, silently re-breaking DuckDB bulk mode (the exact
capability Bug #25/#26 fixed).

This test spins a real in-process DuckDB, loads the mysql extension, and
executes the ATTACH string produced by ``_duckdb_attach_kv``. With a
valid (space-separated) syntax, DuckDB reaches the TCP-connect stage and
raises an IOException (connection refused / unknown host). With an
invalid (semicolon-separated) syntax, DuckDB raises an
InvalidInputException with the "Unrecognized configuration parameter"
message at parse time. The test asserts the produced string's ATTACH
does NOT trigger the parse error — i.e. the error is NOT
``Unrecognized configuration parameter``.

This is the regression guard the v1.3.0 unit tests failed to provide.
"""
from __future__ import annotations

import unittest


def _duckdb_available() -> bool:
    try:
        import duckdb  # noqa: F401
        return True
    except Exception:
        return False


def _mysql_extension_loadable() -> bool:
    """Probe whether the mysql extension can be loaded in this env.
    CI/dev environments without network access may not be able to
    INSTALL it; the transform-worker image bakes it in. We attempt
    LOAD and fall back to INSTALL+LOAD."""
    try:
        import duckdb
        c = duckdb.connect()
        try:
            c.execute("LOAD mysql")
            return True
        except Exception:
            try:
                c.execute("INSTALL mysql")
                c.execute("LOAD mysql")
                return True
            except Exception:
                return False
    except Exception:
        return False


@unittest.skipUnless(_duckdb_available(), "duckdb not installed")
@unittest.skipUnless(_mysql_extension_loadable(),
                     "mysql extension not loadable")
class TestAttachIntegrationRealDuckDB(unittest.TestCase):
    """Execute the ATTACH string from _duckdb_attach_kv against real
    DuckDB and assert the syntax is accepted (connection error, not a
    parse error)."""

    def _run_attach(self, attach_str: str) -> tuple[str, str]:
        """Run ATTACH 'mysql:<attach_str>' and return (exc_type, msg)."""
        import duckdb
        c = duckdb.connect()
        c.execute("LOAD mysql")
        sql = f"ATTACH 'mysql:{attach_str}' AS src (READ_ONLY)"
        try:
            c.execute(sql)
            return ("", "")
        except Exception as e:
            return (type(e).__name__, str(e))

    def test_space_separated_attach_is_accepted(self):
        """The fix: _duckdb_attach_kv produces space-separated output,
        which DuckDB's mysql_scanner accepts (connection error, not a
        parse error)."""
        from loader import _duckdb_attach_kv
        attach_str = _duckdb_attach_kv(
            host="nonexistent.invalid.example", port=3306,
            database="d", user="u", password="plainpass")
        # Sanity: space-separated, no semicolons.
        self.assertNotIn(";", attach_str)
        exc_type, msg = self._run_attach(attach_str)
        # Must NOT be the parse error that the v1.3.0 regression caused.
        self.assertNotIn("Unrecognized configuration parameter", msg,
                          f"ATTACH was rejected at parse time (regression "
                          f"re-introduced): {exc_type}: {msg}")
        # Must be a connection-level error (syntax accepted, DuckDB tried
        # to actually connect). Accept IOException or any non-parse
        # exception. (No-connection environments may raise IOException
        # with "Unknown MySQL server host" or "Can't connect".)
        self.assertNotEqual(exc_type, "",
                            "ATTACH unexpectedly succeeded (no error)")
        self.assertNotIn("Parser", exc_type,
                         f"ATTACH triggered a parser error: {msg}")
        self.assertNotIn("InvalidInput", exc_type,
                          f"ATTACH triggered an invalid-input parse "
                          f"error: {msg}")

    def test_semicolon_separated_attach_is_rejected_at_parse_time(self):
        """Guard: a semicolon-separated ATTACH (the v1.3.0 regression)
        IS rejected at parse time with 'Unrecognized configuration
        parameter'. This pins the regression signature so a future
        re-introduction is caught immediately."""
        # Manually build the v1.3.0-style semicolon-separated string.
        bad_attach = ("host=nonexistent.invalid.example;port=3306;"
                      "database=d;user=u;password=plainpass")
        exc_type, msg = self._run_attach(bad_attach)
        self.assertIn("Unrecognized configuration parameter", msg,
                      f"Expected the v1.3.0 parse-error signature, got "
                      f"{exc_type}: {msg}")

    def test_attach_with_semicolon_in_password_accepted(self):
        """A password containing ``;`` (escaped as ``\\;``) must survive
        the DuckDB parse and reach the connection stage. This is the
        injection-guard case the v1.3.0 escape logic was designed for;
        the v1.3.1 separator fix must not break it."""
        from loader import _duckdb_attach_kv
        attach_str = _duckdb_attach_kv(
            host="nonexistent.invalid.example", port=3306,
            database="d", user="u", password="p;injected")
        # The ``;`` in the password is escaped as ``\;``.
        self.assertIn("password=p\\;injected", attach_str)
        # And there is no unescaped ``;`` acting as a kv separator.
        # (All ``;`` are preceded by ``\``.)
        for i, ch in enumerate(attach_str):
            if ch == ";":
                self.assertGreater(i, 0, "leading ;")
                self.assertEqual(attach_str[i - 1], "\\",
                                 f"unescaped ; at {i}: {attach_str}")
        exc_type, msg = self._run_attach(attach_str)
        self.assertNotIn("Unrecognized configuration parameter", msg,
                          f"ATTACH with escaped-semicolon password was "
                          f"rejected at parse time: {exc_type}: {msg}")
        self.assertNotIn("Parser", exc_type,
                         f"parser error: {msg}")
        self.assertNotIn("InvalidInput", exc_type,
                         f"invalid-input parse error: {msg}")


class TestAttachFormatGuarantees(unittest.TestCase):
    """Format-level guarantees that don't require DuckDB to be loadable.
    These run in every environment (CI without the mysql extension,
    dev boxes, etc.) and provide a fast regression signal."""

    def test_space_separated_format_regex(self):
        """The produced string matches the space-separated key=value
        format (no semicolons as kv separators)."""
        import re
        from loader import _duckdb_attach_kv
        s = _duckdb_attach_kv(host="h", port=3306, database="d",
                             user="u", password="plainpass")
        # Each pair is key=value (value may contain escaped metachars
        # but no unescaped spaces). Pairs are joined by single spaces.
        # Assert no unescaped ``;`` and at least one space separator.
        self.assertNotIn(";", s.replace("\\;", ""))
        # The first ``=`` is the key/value split for the first pair.
        self.assertRegex(s, r"^host=h port=3306 database=d user=u password=plainpass$")

    def test_no_unescaped_semicolon_as_separator(self):
        """No ``;`` in the produced string is acting as a kv separator
        (all ``;`` are escaped as ``\\;`` inside values)."""
        from loader import _duckdb_attach_kv
        s = _duckdb_attach_kv(host="h", port=3306, database="d",
                             user="u", password="p;injected")
        # Strip escaped ``\;`` and confirm no raw ``;`` remains.
        stripped = s.replace("\\;", "")
        self.assertNotIn(";", stripped)


if __name__ == "__main__":
    unittest.main()
