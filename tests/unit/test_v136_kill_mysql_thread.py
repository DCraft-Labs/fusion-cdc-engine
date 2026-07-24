"""v1.3.6 Bug #8 — kill orphaned MySQL thread on fetch failure."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

LOADER = Path(__file__).resolve().parents[2] / "transform-worker" / "loader.py"
WRITER = Path(__file__).resolve().parents[2] / "transform-worker" / "iceberg_writer.py"


def _load(path, name):
    tw = str(path.parent)
    if tw not in sys.path:
        sys.path.insert(0, tw)
    for dep in ("psycopg2", "pyarrow", "redis", "pa"):
        if dep not in sys.modules:
            sys.modules[dep] = MagicMock()
    # iceberg_writer imports real pyarrow as pa — provide a minimal stand-in
    # if needed by assigning module attribute after load via MagicMock packages.
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    if path.name == "iceberg_writer.py":
        # Provide a fake pyarrow module with .schema / .field used at import
        # time only inside functions we don't call; module-level uses `import
        # pyarrow as pa` and type hints. MagicMock is enough for import.
        sys.modules["pyarrow"] = MagicMock()
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def loader():
    return _load(LOADER, "loader_kill_ut")


def test_kill_mysql_thread_issues_kill(loader):
    fake_pymysql = MagicMock()
    killer = MagicMock()
    fake_pymysql.connect.return_value = killer
    cursor = MagicMock()
    killer.cursor.return_value.__enter__.return_value = cursor
    with patch.dict("sys.modules", {"pymysql": fake_pymysql}):
        loader._kill_mysql_thread("h", 3306, "u", "p", 4242)
    cursor.execute.assert_called_once_with("KILL 4242")
    killer.close.assert_called_once()


def test_fetch_mysql_chunk_kills_on_exception(loader):
    inst = object.__new__(loader.InitialLoadTask)
    fake_pymysql = MagicMock()
    fake_cursors = MagicMock()
    fake_pymysql.cursors = fake_cursors
    conn = MagicMock()
    conn.thread_id.return_value = 99
    fake_pymysql.connect.return_value = conn
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    cur.execute.side_effect = RuntimeError("read timeout")

    killed = []

    def _fake_kill(host, port, user, password, thread_id):
        killed.append(thread_id)

    with patch.dict("sys.modules", {"pymysql": fake_pymysql, "pymysql.cursors": fake_cursors}):
        with patch.object(loader, "_kill_mysql_thread", side_effect=_fake_kill):
            with pytest.raises(RuntimeError, match="read timeout"):
                loader.InitialLoadTask._fetch_mysql_chunk(
                    inst, "h", 3306, "db", "u", "p",
                    "s", "t", "id", None, 10,
                )
    assert killed == [99]
