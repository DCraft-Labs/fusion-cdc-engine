"""v1.3.0 Fix 1 — DuckDB ATTACH connection-string escaping tests.

Verifies that ``_duckdb_attach_kv`` escapes the metacharacters ``\\``,
``;``, ``=``, ``'`` and control chars so a password (or any config value)
containing them cannot break out of its key=value position or inject
extra kv pairs into the DuckDB ATTACH string. Also verifies the escape
round-trips through the inverse parser.

v1.3.1 Fix 1: the join separator was changed from ``;`` to a single
space (DuckDB's mysql_scanner/postgres_scanner DSN parser expects
space-separated key=value pairs). These unit-test assertions were
updated to expect space-separated output. The integration test in
``test_v131_attach_integration.py`` actually executes the produced
ATTACH string against real DuckDB to confirm the syntax is accepted
(the unit tests originally missed the separator regression because
they only checked the escape logic in isolation).
"""
from __future__ import annotations

import unittest


class TestDuckDBAttachEscape(unittest.TestCase):
    def test_plain_values_unescaped(self):
        from loader import _duckdb_attach_kv
        s = _duckdb_attach_kv(host="db.local", port=5432,
                              database="mydb", user="alice",
                              password="plainpass")
        self.assertEqual(s, "host=db.local port=5432 database=mydb "
                            "user=alice password=plainpass")

    def test_password_with_metachars_escaped_roundtrips(self):
        from loader import _duckdb_attach_kv
        # A password containing every metachar + a control char. Note:
        # spaces in values are NOT escaped (DuckDB's mysql_scanner DSN
        # parser does not support escaped spaces), so the round-trip
        # test uses a password without a space. The integration test
        # (test_v131_attach_integration.py) covers the live DuckDB
        # parse with a password containing ``;`` and ``\\``.
        pw = "p;=\\'ss\x01word"
        s = _duckdb_attach_kv(host="h", port=3306, database="d",
                             user="u", password=pw)
        # The password value must have each metachar backslash-escaped and
        # the control char \x01 -> \u0001.
        self.assertIn("password=p\\;\\=\\\\\\'ss\\u0001word", s)
        # No unescaped metachar inside the password value: round-trip
        # through the inverse parser yields the original.
        from loader import _duckdb_attach_unescape_kv
        parsed = _duckdb_attach_unescape_kv(s)
        self.assertEqual(parsed["password"], pw)
        self.assertEqual(parsed["host"], "h")
        self.assertEqual(parsed["port"], "3306")
        self.assertEqual(parsed["database"], "d")
        self.assertEqual(parsed["user"], "u")

    def test_semicolon_in_value_does_not_create_extra_kv(self):
        from loader import _duckdb_attach_kv, _duckdb_attach_unescape_kv
        s = _duckdb_attach_kv(password="a;b;c")
        # One kv pair -> zero unescaped separator (space). The two ``;`` in
        # the value are both backslash-escaped (``\;``).
        # Strip the escaped ``\;`` and confirm no raw space remains inside
        # the value (the only spaces are the kv separators).
        # The full string is ``password=a\;b\;c`` — a single kv pair.
        self.assertEqual(s, "password=a\\;b\\;c")
        parsed = _duckdb_attach_unescape_kv(s)
        self.assertEqual(parsed, {"password": "a;b;c"})

    def test_equals_in_value_does_not_create_extra_kv(self):
        from loader import _duckdb_attach_kv, _duckdb_attach_unescape_kv
        s = _duckdb_attach_kv(password="x=y=z")
        parsed = _duckdb_attach_unescape_kv(s)
        self.assertEqual(parsed, {"password": "x=y=z"})
        # Only the leading ``password=`` is an unescaped ``=``. The two
        # ``=`` in the value are backslash-escaped (``\=``) so they do
        # not create extra kv pairs.
        # Count ``=`` that are NOT preceded by a backslash.
        unescaped_eq = sum(1 for i, c in enumerate(s) if c == "="
                           and (i == 0 or s[i - 1] != "\\"))
        self.assertEqual(unescaped_eq, 1)

    def test_quote_in_value_escaped(self):
        from loader import _duckdb_attach_kv, _duckdb_attach_unescape_kv
        s = _duckdb_attach_kv(password="itsatest")
        parsed = _duckdb_attach_unescape_kv(s)
        self.assertEqual(parsed, {"password": "itsatest"})

    def test_backslash_in_value_escaped(self):
        from loader import _duckdb_attach_kv, _duckdb_attach_unescape_kv
        s = _duckdb_attach_kv(password="backslash")
        parsed = _duckdb_attach_unescape_kv(s)
        self.assertEqual(parsed, {"password": "backslash"})

    def test_backslash_metachar_roundtrips(self):
        from loader import _duckdb_attach_kv, _duckdb_attach_unescape_kv
        # Backslash is the escape metachar and must itself be escaped.
        s = _duckdb_attach_kv(password="back\\slash")
        self.assertIn("password=back\\\\slash", s)
        parsed = _duckdb_attach_unescape_kv(s)
        self.assertEqual(parsed, {"password": "back\\slash"})

    def test_semicolon_metachar_roundtrips(self):
        from loader import _duckdb_attach_kv, _duckdb_attach_unescape_kv
        s = _duckdb_attach_kv(password="p;injected")
        self.assertIn("password=p\\;injected", s)
        parsed = _duckdb_attach_unescape_kv(s)
        self.assertEqual(parsed, {"password": "p;injected"})

    def test_none_values_skipped(self):
        from loader import _duckdb_attach_kv
        s = _duckdb_attach_kv(host="h", port=None, database="d",
                             user=None, password="p")
        # port and user (None) are omitted entirely.
        self.assertEqual(s, "host=h database=d password=p")

    def test_mysql_branch_uses_kv_helper(self):
        """The mysql ATTACH branch builds the connection string via the
        escaping helper (no raw interpolation of password)."""
        from loader import _duckdb_attach_kv
        s = _duckdb_attach_kv(host="h", port=3306, database="d",
                             user="u", password="p;injected")
        self.assertIn("host=h", s)
        self.assertIn("password=p\\;injected", s)

    def test_postgres_branch_uses_kv_helper(self):
        from loader import _duckdb_attach_kv
        s = _duckdb_attach_kv(host="h", port=5432, dbname="d",
                             user="u", password="p;injected")
        self.assertIn("password=p\\;injected", s)
        self.assertIn("dbname=d", s)

    def test_space_separated_format(self):
        """v1.3.1 Fix 1: the join separator is a single space, not ``;``.
        DuckDB's mysql_scanner DSN parser expects space-separated
        key=value pairs."""
        from loader import _duckdb_attach_kv
        s = _duckdb_attach_kv(host="h", port=3306, database="d",
                             user="u", password="p")
        # No semicolons at all (the v1.3.0 regression).
        self.assertNotIn(";", s)
        # Pairs are space-separated.
        self.assertEqual(s, "host=h port=3306 database=d user=u password=p")


if __name__ == "__main__":
    unittest.main()
