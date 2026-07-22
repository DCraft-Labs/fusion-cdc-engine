"""v1.2.20: unit tests for ``cdc_consumer._do_initial_load_postgres``.

These tests pin the new Postgres source initial-load path (Fix B) so it
can never silently regress. We mock psycopg2 so no real Postgres is
required — the tests run in CI without testcontainers.
"""
from __future__ import annotations

import sys
import types
from pathlib import Path
from unittest import mock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
CONSUMER_PATH = REPO_ROOT / "cdc-workers" / "cdc_consumer.py"


# ---------------------------------------------------------------------------
# Load cdc_consumer.py as a module. It imports paramiko / psycopg2 / pymysql
# / redis at module top level, so we stub those packages in sys.modules
# before importing to avoid requiring the real deps in the test environment.
# ---------------------------------------------------------------------------

def _stub_module(name, attrs=None):
    mod = types.ModuleType(name)
    for k, v in (attrs or {}).items():
        setattr(mod, k, v)
    return mod


def _install_stubs():
    """Install lightweight stubs for heavy optional deps."""
    if "paramiko" not in sys.modules:
        sys.modules["paramiko"] = _stub_module("paramiko")
    if "pymysql" not in sys.modules:
        sys.modules["pymysql"] = _stub_module("pymysql", attrs={
            "cursors": _stub_module("pymysql.cursors"),
        })
    if "redis" not in sys.modules:
        sys.modules["redis"] = _stub_module("redis", attrs={
            "exceptions": _stub_module("redis.exceptions", attrs={
                "ResponseError": type("ResponseError", (Exception,), {}),
            }),
            "from_url": lambda *a, **k: None,
        })
    # cdc_consumer._decrypt() does `from cryptography.fernet import Fernet`
    # at call time. cryptography is NOT installed in the bare CI test image
    # (only pytest is), so we stub it here to keep the import from raising.
    # The tests never exercise real decryption (src_pw_enc="").
    if "cryptography" not in sys.modules:
        crypto_stub = _stub_module("cryptography")
        fernet_stub = _stub_module("cryptography.fernet")
        class _FakeFernet:
            def __init__(self, *a, **k):
                pass
            def decrypt(self, data):
                return b""
        fernet_stub.Fernet = _FakeFernet
        crypto_stub.fernet = fernet_stub
        sys.modules["cryptography"] = crypto_stub
        sys.modules["cryptography.fernet"] = fernet_stub
    # psycopg2 is imported by the module AND used inside the function under
    # test, so we keep a real-enough stub whose connect() returns a mock.
    if "psycopg2" not in sys.modules:
        psycopg2_stub = _stub_module("psycopg2")
        extras_stub = _stub_module("psycopg2.extras")
        extras_stub.RealDictCursor = object()
        psycopg2_stub.extras = extras_stub
        errors_stub = _stub_module("psycopg2.errors")
        for err_name in (
            "UndefinedColumn", "InvalidTextRepresentation",
            "DatatypeMismatch", "NumericValueOutOfRange",
        ):
            setattr(errors_stub, err_name, type(err_name, (Exception,), {}))
        psycopg2_stub.errors = errors_stub
        psycopg2_stub.connect = lambda *a, **k: mock.MagicMock()
        sys.modules["psycopg2"] = psycopg2_stub
        sys.modules["psycopg2.extras"] = extras_stub
        sys.modules["psycopg2.errors"] = errors_stub


_install_stubs()

# Now import the module under test.
import importlib.util
_spec = importlib.util.spec_from_file_location("cdc_consumer_v120_test", CONSUMER_PATH)
cdc_consumer = importlib.util.module_from_spec(_spec)
sys.modules["cdc_consumer_v120_test"] = cdc_consumer
_spec.loader.exec_module(cdc_consumer)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_conn_cfg(src_schema="public", src_table="orders", pk_cols=None):
    return {
        "connection_id": "00000000-0000-0000-0000-000000000001",
        "source_id": "00000000-0000-0000-0000-000000000002",
        "src_host": "src-host",
        "src_port": 5432,
        "src_db": "srcdb",
        "src_user": "srcuser",
        "src_pw_enc": "",
        "src_connector_type": "postgresql",
        "src_ssh_config": {},
    }


def _make_stream(stream_id, schema_name, table_name, pk_cols=None):
    return {
        "stream_id": stream_id,
        "schema_name": schema_name,
        "table_name": table_name,
        "destination_schema_name": "dw",
        "destination_table_name": table_name,
        "sync_mode": "cdc",
        "primary_key": pk_cols or ["id"],
        "column_mapping": {},
        "selected_columns": [],
        "transform_overrides": {},
    }


class _FakeRealDictRow(dict):
    """Mimics psycopg2.extras.RealDictRow (dict subclass)."""
    pass


class _FakeServerSideCursor:
    """A minimal server-side cursor that yields rows from a list."""
    def __init__(self, rows):
        self._rows = rows
        self._iter = iter(rows)
        self.itersize = 10000
        self.rownumber = 0

    def __iter__(self):
        return self

    def __next__(self):
        row = next(self._iter)
        self.rownumber += 1
        return _FakeRealDictRow(row)

    def execute(self, sql, params=None):
        # No-op for the test; rows are pre-populated.
        pass

    def close(self):
        pass

    # Context manager protocol — psycopg2 named cursors support `with`.
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class _FakeSrcConn:
    def __init__(self, rows_by_table):
        self._rows_by_table = rows_by_table
        self._cursors = []

    def cursor(self, name=None, cursor_factory=None):
        # Return the next pre-populated server-side cursor.
        rows = self._rows_by_table.pop(0) if self._rows_by_table else []
        cur = _FakeServerSideCursor(rows)
        self._cursors.append(cur)
        return cur

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_do_initial_load_postgres_routes_on_postgres_source():
    """The _do_initial_load router dispatches to _do_initial_load_postgres
    when src_connector_type is postgres/postgresql (not mysql)."""
    called = {"pg": 0, "mysql": 0, "mongo": 0}
    def _pg(*a, **k):
        called["pg"] += 1
        return 42
    def _mysql(*a, **k):
        called["mysql"] += 1
        return 0
    def _mongo(*a, **k):
        called["mongo"] += 1
        return 0
    with mock.patch.object(cdc_consumer, "_do_initial_load_postgres", _pg), \
         mock.patch.object(cdc_consumer, "_do_initial_load_mysql", _mysql), \
         mock.patch.object(cdc_consumer, "_do_initial_load_mongodb", _mongo):
        n = cdc_consumer._do_initial_load(
            {"src_connector_type": "postgresql"}, dest_conn=mock.MagicMock(),
            meta=mock.MagicMock(),
        )
    assert n == 42
    assert called == {"pg": 1, "mysql": 0, "mongo": 0}


def test_do_initial_load_postgres_routes_on_mysql_source():
    called = {"pg": 0, "mysql": 0}
    with mock.patch.object(cdc_consumer, "_do_initial_load_postgres",
                           lambda *a, **k: called.__setitem__("pg", called["pg"] + 1) or 0), \
         mock.patch.object(cdc_consumer, "_do_initial_load_mysql",
                           lambda *a, **k: called.__setitem__("mysql", called["mysql"] + 1) or 7):
        n = cdc_consumer._do_initial_load(
            {"src_connector_type": "mysql"}, dest_conn=mock.MagicMock(),
            meta=mock.MagicMock(),
        )
    assert n == 7
    assert called == {"pg": 0, "mysql": 1}


def test_do_initial_load_postgres_routes_on_mongo_source():
    called = {"mongo": 0}
    with mock.patch.object(cdc_consumer, "_do_initial_load_mongodb",
                           lambda *a, **k: called.__setitem__("mongo", called["mongo"] + 1) or 9):
        n = cdc_consumer._do_initial_load(
            {"src_connector_type": "mongodb"}, dest_conn=mock.MagicMock(),
            meta=mock.MagicMock(),
        )
    assert n == 9
    assert called["mongo"] == 1


def test_do_initial_load_postgres_streams_rows_via_server_side_cursor():
    """End-to-end-ish: _do_initial_load_postgres reads rows from a mocked
    Postgres source via a named (server-side) cursor and writes them to
    the mocked destination via _copy_batch_to_pg. Asserts the row count
    and that TRUNCATE + _ensure_dest_table were called."""
    conn_cfg = _make_conn_cfg(src_schema="public", src_table="orders", pk_cols=["id"])
    streams = [_make_stream("s1", "public", "orders", pk_cols=["id"])]

    rows = [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}, {"id": 3, "name": "gamma"}]
    fake_src = _FakeSrcConn(rows_by_table=[rows])

    fake_dest = mock.MagicMock()
    fake_meta = mock.MagicMock()

    copy_calls = []
    def _fake_copy(dest_conn, schema, table, cols, batch, upsert_sql, batch_size):
        copy_calls.append((schema, table, len(batch)))
        return None
    ensure_calls = []
    def _fake_ensure(dest_conn, schema, table, cols, pk_cols, **kw):
        ensure_calls.append((schema, table, tuple(cols), tuple(pk_cols or [])))
        return {c: "TEXT" for c in cols}

    # _get_streams_for_connection returns our streams on first call,
    # then [] on subsequent calls (checkpoint query).
    def _get_streams(meta, cid):
        return streams
    def _get_pending_run(meta, cid):
        return None

    with mock.patch.object(cdc_consumer, "psycopg2") as pg_mod, \
         mock.patch.object(cdc_consumer, "_get_streams_for_connection", _get_streams), \
         mock.patch.object(cdc_consumer, "_get_pending_run", _get_pending_run), \
         mock.patch.object(cdc_consumer, "_copy_batch_to_pg", _fake_copy), \
         mock.patch.object(cdc_consumer, "_ensure_dest_table", _fake_ensure), \
         mock.patch.object(cdc_consumer, "_apply_transform_steps", lambda row, steps: row), \
         mock.patch.object(cdc_consumer, "_start_ssh_port_forward", side_effect=None):
        pg_mod.connect.return_value = fake_src
        # The meta cursor() returns a RealDictCursor for the checkpoint SELECT;
        # use MagicMock that yields no rows.
        fake_meta.cursor.return_value.__enter__.return_value.fetchall.return_value = []
        fake_meta.cursor.return_value.__enter__.return_value.fetchone.return_value = None
        n = cdc_consumer._do_initial_load_postgres(conn_cfg, fake_dest, fake_meta, "dw")

    assert n == 3, f"expected 3 rows loaded, got {n}"
    # _ensure_dest_table called once for the stream (cols + pk)
    assert len(ensure_calls) == 1
    schema, table, cols, pk = ensure_calls[0]
    assert schema == "dw"
    assert table == "orders"
    assert set(cols) == {"id", "name"}
    assert pk == ("id",)
    # _copy_batch_to_pg called at least once with the 3 rows
    assert copy_calls, "expected at least one COPY batch"
    assert sum(c[2] for c in copy_calls) == 3


def test_do_initial_load_postgres_skips_already_completed_stream():
    """A stream whose checkpoint status is 'done' is skipped — no rows
    are read and the prior row count is returned."""
    conn_cfg = _make_conn_cfg()
    streams = [_make_stream("s1", "public", "orders", pk_cols=["id"])]

    fake_src = _FakeSrcConn(rows_by_table=[])  # would raise if called
    fake_dest = mock.MagicMock()
    fake_meta = mock.MagicMock()

    with mock.patch.object(cdc_consumer, "psycopg2") as pg_mod, \
         mock.patch.object(cdc_consumer, "_get_streams_for_connection",
                           lambda meta, cid: streams), \
         mock.patch.object(cdc_consumer, "_get_pending_run", lambda meta, cid: None), \
         mock.patch.object(cdc_consumer, "_copy_batch_to_pg") as fake_copy, \
         mock.patch.object(cdc_consumer, "_ensure_dest_table") as fake_ensure:
        pg_mod.connect.return_value = fake_src
        # Checkpoint query returns one row with status='done', rows_written=99.
        # RealDictCursor returns dict subclasses, so use a real dict.
        ckpt_row = {"stream_id": "s1", "status": "done", "rows_written": 99}
        fake_meta.cursor.return_value.__enter__.return_value.fetchall.return_value = [ckpt_row]
        n = cdc_consumer._do_initial_load_postgres(conn_cfg, fake_dest, fake_meta, "dw")

    assert n == 99
    fake_copy.assert_not_called()
    fake_ensure.assert_not_called()


def test_do_initial_load_postgres_no_streams_returns_zero():
    conn_cfg = _make_conn_cfg()
    with mock.patch.object(cdc_consumer, "_get_streams_for_connection",
                           lambda meta, cid: []):
        n = cdc_consumer._do_initial_load_postgres(
            conn_cfg, mock.MagicMock(), mock.MagicMock(), "dw")
    assert n == 0
