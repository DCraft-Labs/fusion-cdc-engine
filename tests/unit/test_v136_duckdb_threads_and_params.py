"""v1.3.6 Bugs #1 / #2 — DuckDB SET threads=1 + conditional named params."""
from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

LOADER = Path(__file__).resolve().parents[2] / "transform-worker" / "loader.py"


def _load_loader_module():
    import importlib.util
    import sys

    # Ensure transform-worker is importable as a sibling package path.
    tw = str(LOADER.parent)
    if tw not in sys.path:
        sys.path.insert(0, tw)
    spec = importlib.util.spec_from_file_location("loader_under_test", LOADER)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    # Stub heavy optional deps that loader imports at module level.
    for name in ("psycopg2", "pyarrow", "redis"):
        if name not in sys.modules:
            sys.modules[name] = MagicMock()
    if "pa" not in sys.modules:
        sys.modules["pa"] = MagicMock()
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def loader():
    return _load_loader_module()


def test_open_duckdb_scanner_sets_threads_one(loader):
    """Bug #1: force single-threaded DuckDB execution after connect()."""
    fake_conn = MagicMock()
    fake_duckdb = MagicMock()
    fake_duckdb.connect.return_value = fake_conn

    class _IL:
        pass

    # Build a minimal instance with just the method bound.
    inst = object.__new__(loader.InitialLoadTask)
    source = {
        "host": "h", "port": 3306, "database_name": "db",
        "username": "u", "password": "p",
    }
    with patch.dict("sys.modules", {"duckdb": fake_duckdb}):
        with patch.object(loader, "_duckdb_attach_kv", return_value="host=h"):
            # Re-import duckdb inside the method via the patched module.
            result = loader.InitialLoadTask._open_duckdb_scanner(inst, source, "mysql")

    assert result is fake_conn
    executed = [c.args[0] for c in fake_conn.execute.call_args_list]
    assert any(isinstance(s, str) and "SET threads=1" in s for s in executed)


def test_fetch_chunk_duckdb_omits_unused_params(loader):
    """Bug #2: params dict must only include keys referenced in SQL."""
    inst = object.__new__(loader.InitialLoadTask)
    duck = MagicMock()
    rel = MagicMock()
    duck.execute.return_value = rel
    rel.fetch_arrow_table.return_value = "arrow"

    # First chunk: no last_pk, no pk_end
    out = loader.InitialLoadTask._fetch_chunk_duckdb(
        inst, duck, {}, "s", "t", "id", None, 1000, "mysql", pk_end=None,
    )
    assert out == "arrow"
    sql, params = duck.execute.call_args[0]
    assert "$chunk_size" in sql
    assert "$last_pk" not in sql
    assert "$pk_end" not in sql
    assert params == {"chunk_size": 1000}

    duck.reset_mock()
    # Mid partition: last_pk set, pk_end set
    loader.InitialLoadTask._fetch_chunk_duckdb(
        inst, duck, {}, "s", "t", "id", 10, 500, "mysql", pk_end=99,
    )
    sql, params = duck.execute.call_args[0]
    assert "$last_pk" in sql and "$pk_end" in sql
    assert params == {"chunk_size": 500, "last_pk": 10, "pk_end": 99}

    duck.reset_mock()
    # Last unbounded partition: last_pk set, pk_end None
    loader.InitialLoadTask._fetch_chunk_duckdb(
        inst, duck, {}, "s", "t", "id", 50, 200, "mysql", pk_end=None,
    )
    sql, params = duck.execute.call_args[0]
    assert "$last_pk" in sql
    assert "$pk_end" not in sql
    assert params == {"chunk_size": 200, "last_pk": 50}
    assert "pk_end" not in params


def test_source_contains_threads_fix_comment():
    """Byte-level guard: SET threads=1 must remain in loader.py."""
    src = LOADER.read_text(encoding="utf-8")
    assert 'conn.execute("SET threads=1")' in src
    assert 'params: dict[str, Any] = {"chunk_size": chunk_size}' in src
