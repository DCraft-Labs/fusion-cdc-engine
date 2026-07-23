"""v1.2.22 Fix C tests — compute efficiency: schema fetched ONCE per stream.

Covers:
- ``_get_source_schema`` is called exactly ONCE per ``InitialLoadTask.run``
  (not per chunk). We mock the source so it returns 3 chunks of rows and
  count how many times ``_get_source_schema`` is invoked.
- The Iceberg writer receives the cached transformed schema on every chunk
  (so it never re-infers types).
- Source DB fetches use READ ONLY / autocommit (Fix C3) — verified by
  inspecting the SQL executed on a mocked psycopg2 cursor.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest


def _make_engine():
    from engine import DuckDBTransformEngine
    return DuckDBTransformEngine(
        metadata_db_dsn="sqlite://",
        encryption_key="k",
        control_plane_url="http://control-plane",
        worker_id="w-test",
    )


def test_get_source_schema_called_once_per_initial_load():
    """Fix C1: the source schema is fetched ONCE per stream, not per chunk.

    We mock the source to return 3 chunks of 2 rows each (6 rows total)
    and assert ``_get_source_schema`` is invoked exactly once.
    """
    from loader import InitialLoadTask, STOP_EVENT
    from iceberg_writer import _get_source_schema

    STOP_EVENT.clear()
    engine = _make_engine()
    redis_client = MagicMock()
    redis_client.brpop.return_value = None
    task = InitialLoadTask(engine=engine, redis_client=redis_client)

    # Mock _get_last_checkpoint to return None (fresh start)
    task._get_last_checkpoint = MagicMock(return_value=None)
    task._report_checkpoint = MagicMock()

    # Mock _fetch_chunk to return 3 chunks then []
    chunks = [
        [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
        [{"id": 3, "name": "c"}, {"id": 4, "name": "d"}],
        [{"id": 5, "name": "e"}],  # short chunk → end
    ]
    call_count = {"n": 0}

    def fake_fetch(source, schema, table, pk, last_pk, size, ctype, pk_end=None):
        if call_count["n"] < len(chunks):
            r = chunks[call_count["n"]]
            call_count["n"] += 1
            return r
        return []
    task._fetch_chunk = MagicMock(side_effect=fake_fetch)
    task._extract_pk = lambda row, pk, ctype: row.get(pk)
    task._write_to_iceberg = MagicMock(return_value=2)

    with patch("iceberg_writer._get_source_schema") as inner_spy:
        inner_spy.return_value = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ])
        task.run({
            "connection_id": "c1",
            "stream_id": "s1",
            "source": {"connector_type": "mysql", "host": "h",
                        "database_name": "db", "username": "u",
                        "password": "p"},
            "source_schema": "db",
            "source_table": "users",
            "destination": {"connector_type": "iceberg",
                              "connection_config": {}},
            "dest_table": "users",
            "chunk_size": 2,
        })

    # The schema must be fetched exactly ONCE, regardless of chunk count.
    assert inner_spy.call_count == 1, (
        f"Fix C1 regression: _get_source_schema called {inner_spy.call_count} "
        f"times (expected 1) for {call_count['n']} chunks"
    )
    # And the writer must have been called for every chunk with a schema.
    assert task._write_to_iceberg.call_count == 3
    for call_args in task._write_to_iceberg.call_args_list:
        # The 3rd positional arg is the schema (kw or positional)
        schema_arg = call_args.kwargs.get("schema")
        assert schema_arg is not None, "IcebergWriter.write_batch must receive the cached schema"


def test_pg_chunk_fetch_uses_read_only_transaction():
    """Fix C3: the Postgres chunk fetch must BEGIN READ ONLY + autocommit
    so the source DB is not locked across the destination write.
    """
    from loader import InitialLoadTask

    engine = _make_engine()
    redis_client = MagicMock()
    task = InitialLoadTask(engine=engine, redis_client=redis_client)

    executed_sql: list[str] = []
    cursor = MagicMock()
    def capture(sql, *args, **kw):
        executed_sql.append(sql)
        if "SELECT" in sql:
            return [{"id": 1, "name": "a"}]
        return []
    cursor.execute.side_effect = capture
    cursor.fetchall.return_value = [{"id": 1, "name": "a"}]
    cursor.__enter__ = lambda self: self
    cursor.__exit__ = lambda self, *a: None
    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.__enter__ = lambda self: self
    conn.__exit__ = lambda self, *a: None
    # autocommit is a settable attribute on psycopg2 connections
    type(conn).autocommit = property(lambda self: True, lambda self, v: None)

    with patch("psycopg2.connect", return_value=conn):
        rows = task._fetch_pg_chunk("h", 5432, "db", "u", "p",
                                     "public", "users", "id", None, 100)

    assert rows == [{"id": 1, "name": "a"}]
    # Must have issued BEGIN READ ONLY and COMMIT
    assert any("BEGIN READ ONLY" in s for s in executed_sql), \
        f"Fix C3 regression: expected BEGIN READ ONLY, got {executed_sql}"
    assert any(s.strip().upper() == "COMMIT" for s in executed_sql), \
        f"Fix C3 regression: expected COMMIT, got {executed_sql}"


def test_mysql_chunk_fetch_uses_autocommit():
    """Fix C3: the MySQL chunk fetch must connect with autocommit=True."""
    from loader import InitialLoadTask

    engine = _make_engine()
    redis_client = MagicMock()
    task = InitialLoadTask(engine=engine, redis_client=redis_client)

    cursor = MagicMock()
    cursor.fetchall.return_value = [{"id": 1, "name": "a"}]
    cursor.__enter__ = lambda self: self
    cursor.__exit__ = lambda self, *a: None
    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.__enter__ = lambda self: self
    conn.__exit__ = lambda self, *a: None

    with patch("pymysql.connect", return_value=conn) as connect_mock:
        rows = task._fetch_mysql_chunk("h", 3306, "db", "u", "p",
                                        "db", "users", "id", None, 100)

    assert rows == [{"id": 1, "name": "a"}]
    # autocommit=True must be passed to pymysql.connect
    assert connect_mock.call_args.kwargs.get("autocommit") is True, \
        "Fix C3 regression: MySQL chunk fetch must use autocommit=True"
