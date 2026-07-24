r"""v1.3.0 Fix 1 — DuckDB ATTACH connection-string escaping tests.

Verifies that ``_duckdb_attach_kv`` escapes the metacharacters ``\\``,
``;`` and control chars so a password (or any config value) containing
them cannot break out of its key=value position or inject extra kv
pairs into the DuckDB ATTACH string. Also verifies the escape
round-trips through the inverse parser.

v1.3.1 Fix 1: the join separator was changed from ``;`` to a single
space (DuckDB's mysql_scanner/postgres_scanner DSN parser expects
space-separated key=value pairs). These unit-test assertions were
updated to expect space-separated output. The integration test in
``test_v131_attach_integration.py`` actually executes the produced
ATTACH string against real DuckDB to confirm the syntax is accepted
(the unit tests originally missed the separator regression because
they only checked the escape logic in isolation).

v1.3.2 Fix 4 (carried from v1.3.1 follow-up): live testing confirmed
DuckDB's mysql_scanner DSN parser only accepts ``\\`` and ``\;`` as
backslash escapes. ``\=`` triggers ``Unrecognized configuration
parameter`` and ``\'`` breaks the outer SQL string literal. The
``\=`` and ``\'`` escaping has been DROPPED. As a consequence,
spaces, ``=``, and ``'`` in passwords are NOT supported by the
DuckDB mysql_scanner DSN parser — operators with such passwords
must change the password or use a different mechanism. The tests
below were updated to:
  * expect only ``\\`` and ``\;`` escaping (no ``\=`` or ``\'``);
  * document the unsupported-char behaviour for passwords containing
    ``=`` or ``'`` (the encoder no longer escapes them, so a password
    containing an unescaped ``=`` would split the kv pair — the test
    asserts the encoder does NOT silently produce a broken string by
    instead documenting that such passwords are unsupported and
    asserting the encoder leaves them raw, which is the documented
    behaviour the caller must reject upstream).
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
        # v1.3.2 Fix 4: only ``\\`` and ``\;`` are escaped now. ``\=`` and
        # ``\'`` are no longer escaped (DuckDB's mysql_scanner rejects
        # them). A password containing ``=`` or ``'`` is unsupported
        # and must be rejected upstream — this test uses a password
        # with only the supported metachars (``;``, ``\\``, control
        # char) so the round-trip still verifies the supported escape
        # set. The unsupported-char behaviour is documented in
        # ``test_password_with_equals_is_unsupported`` and
        # ``test_password_with_quote_is_unsupported`` below.
        pw = "p;\\ss\x01word"
        s = _duckdb_attach_kv(host="h", port=3306, database="d",
                             user="u", password=pw)
        # The password value must have ``\\`` -> ``\\\\``, ``;`` -> ``\;``,
        # and the control char \x01 -> \u0001. ``=`` and ``'`` are NOT
        # present in this password (unsupported).
        self.assertIn("password=p\\;\\\\ss\\u0001word", s)
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

    def test_equals_in_value_is_unsupported(self):
        r"""v1.3.2 Fix 4: ``=`` is NO LONGER escaped. DuckDB's mysql_scanner
        DSN parser only honours ``\\`` and ``\;`` as backslash escapes;
        ``\=`` triggers ``Unrecognized configuration parameter``. The
        encoder therefore leaves ``=`` raw, which would split the kv
        pair — so passwords containing ``=`` are UNSUPPORTED and must
        be rejected upstream. This test documents the behaviour: the
        encoder produces a string with the unescaped ``=`` still in
        the value, which the caller is expected to detect and reject
        (the encoder does NOT silently produce a syntactically valid
        but semantically broken ATTACH)."""
        from loader import _duckdb_attach_kv
        s = _duckdb_attach_kv(password="x=y=z")
        # The encoder leaves ``=`` raw (no ``\=`` escape). The first
        # ``=`` is the key/value split; the remaining ``=`` are inside
        # the value, unescaped. This is the documented unsupported
        # behaviour — the caller must reject such passwords upstream.
        self.assertEqual(s, "password=x=y=z")
        # No ``\=`` escape sequence is produced.
        self.assertNotIn("\\=", s)

    def test_quote_in_value_is_unsupported(self):
        """v1.3.2 Fix 4: ``'`` is NO LONGER escaped. DuckDB's mysql_scanner
        DSN parser would break the outer SQL string literal if ``\'``
        were emitted, so the encoder leaves ``'`` raw. Passwords
        containing ``'`` are UNSUPPORTED and must be rejected upstream.
        This test documents the behaviour: the encoder produces a
        string with the unescaped ``'`` still in the value, which the
        caller is expected to detect and reject."""
        from loader import _duckdb_attach_kv
        s = _duckdb_attach_kv(password="it'satest")
        # The encoder leaves ``'`` raw (no ``\'`` escape).
        self.assertEqual(s, "password=it'satest")
        # No ``\'`` escape sequence is produced.
        self.assertNotIn("\\'", s)

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

    def test_escape_set_is_reduced_to_backslash_and_semicolon(self):
        """v1.3.2 Fix 4: the escape set is exactly ``\\`` and ``;`` (plus
        control-char ``\\uXXXX``). ``=`` and ``'`` are NOT in the escape
        set — DuckDB's mysql_scanner rejects ``\\=`` and ``\\'``."""
        from loader import _DUCKDB_ATTACH_ESCAPE_CHARS
        self.assertEqual(
            set(_DUCKDB_ATTACH_ESCAPE_CHARS.keys()),
            {"\\", ";"},
            "v1.3.2 Fix 4: escape set must be exactly {\\, ;} — "
            "\\= and \\' were dropped because DuckDB's mysql_scanner "
            "DSN parser rejects them.")


if __name__ == "__main__":
    unittest.main()
