"""v1.2.22 Bug A tests — all-NULL columns, type mapping, schema drift.

Covers:
- ``_rows_to_arrow`` with explicit schema → all-NULL column keeps its
  declared type instead of ``pa.null()``.
- ``_rows_to_arrow`` without schema → legacy inference (regression guard).
- MySQL / Postgres type normalisation maps the full source type set to
  the expected Arrow types.
- ``_get_source_schema`` for MySQL / Postgres / Mongo (mocked drivers).
- Schema drift: ``_evolve_schema_for_drift`` appends new columns.
"""
from __future__ import annotations

import datetime as _dt
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest


# ─── _rows_to_arrow: explicit schema keeps all-NULL column type ──────────────
def test_rows_to_arrow_with_schema_all_null_column_keeps_declared_type():
    """Bug A: an all-NULL column must NOT collapse to pa.null()."""
    from iceberg_writer import _rows_to_arrow

    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("nullable_str", pa.string()),  # all values None
        pa.field("nullable_int", pa.int32()),  # all values None
    ])
    rows = [
        {"id": 1, "nullable_str": None, "nullable_int": None},
        {"id": 2, "nullable_str": None, "nullable_int": None},
    ]
    tbl = _rows_to_arrow(rows, schema=schema)
    assert tbl.schema.field("nullable_str").type == pa.string()
    assert tbl.schema.field("nullable_int").type == pa.int32()
    assert tbl.schema.field("id").type == pa.int64()
    # All-null column should have all-null values
    assert tbl.column("nullable_str").null_count == 2


def test_rows_to_arrow_without_schema_legacy_inference():
    """Regression guard: when no schema is supplied, the old inference
    behaviour is preserved (all-NULL column → pa.null()). This documents
    WHY the explicit schema is required.
    """
    from iceberg_writer import _rows_to_arrow

    rows = [{"id": 1, "all_null_col": None}]
    tbl = _rows_to_arrow(rows)
    # Without explicit schema, pa.Table.from_pylist infers pa.null() for
    # the all-NULL column — this is the Bug A root cause.
    assert tbl.schema.field("all_null_col").type == pa.null()


def test_rows_to_arrow_empty_rows_with_schema():
    from iceberg_writer import _rows_to_arrow
    schema = pa.schema([pa.field("id", pa.int64())])
    tbl = _rows_to_arrow([], schema=schema)
    assert tbl.schema == schema
    assert tbl.num_rows == 0


def test_rows_to_arrow_empty_rows_without_schema():
    from iceberg_writer import _rows_to_arrow
    tbl = _rows_to_arrow([])
    assert tbl.num_rows == 0


# ─── Type mapping ───────────────────────────────────────────────────────────
def test_mysql_type_mapping_covers_core_types():
    from iceberg_writer import _normalize_mysql_type
    assert _normalize_mysql_type("tinyint") == pa.int8()
    assert _normalize_mysql_type("smallint") == pa.int16()
    assert _normalize_mysql_type("mediumint") == pa.int32()
    assert _normalize_mysql_type("int") == pa.int32()
    assert _normalize_mysql_type("bigint") == pa.int64()
    assert _normalize_mysql_type("decimal") == pa.decimal128(38, 18)
    assert _normalize_mysql_type("numeric") == pa.decimal128(38, 18)
    assert _normalize_mysql_type("varchar") == pa.string()
    assert _normalize_mysql_type("char") == pa.string()
    assert _normalize_mysql_type("text") == pa.string()
    assert _normalize_mysql_type("json") == pa.string()
    assert _normalize_mysql_type("enum") == pa.string()
    assert _normalize_mysql_type("datetime") == pa.timestamp("us")
    assert _normalize_mysql_type("timestamp") == pa.timestamp("us")
    assert _normalize_mysql_type("date") == pa.date32()
    assert _normalize_mysql_type("float") == pa.float32()
    assert _normalize_mysql_type("double") == pa.float64()
    assert _normalize_mysql_type("bit") == pa.binary()
    assert _normalize_mysql_type("varbinary") == pa.binary()
    assert _normalize_mysql_type("blob") == pa.binary()
    # Unknown type → string fallback (safe)
    assert _normalize_mysql_type("geometry") == pa.string()
    # Type with precision suffix → base type
    assert _normalize_mysql_type("decimal(10,2)") == pa.decimal128(38, 18)
    assert _normalize_mysql_type("varchar(255)") == pa.string()


def test_postgres_type_mapping_covers_core_types():
    from iceberg_writer import _normalize_pg_type
    assert _normalize_pg_type("smallint") == pa.int16()
    assert _normalize_pg_type("integer") == pa.int32()
    assert _normalize_pg_type("bigint") == pa.int64()
    assert _normalize_pg_type("decimal") == pa.decimal128(38, 18)
    assert _normalize_pg_type("numeric") == pa.decimal128(38, 18)
    assert _normalize_pg_type("real") == pa.float32()
    assert _normalize_pg_type("double precision") == pa.float64()
    assert _normalize_pg_type("boolean") == pa.bool_()
    assert _normalize_pg_type("text") == pa.string()
    assert _normalize_pg_type("character varying") == pa.string()
    assert _normalize_pg_type("varchar") == pa.string()
    assert _normalize_pg_type("bytea") == pa.binary()
    assert _normalize_pg_type("date") == pa.date32()
    assert _normalize_pg_type("timestamp") == pa.timestamp("us")
    assert _normalize_pg_type("timestamp with time zone") == pa.timestamp("us")
    assert _normalize_pg_type("jsonb") == pa.string()
    assert _normalize_pg_type("uuid") == pa.string()
    # Unknown type → string fallback
    assert _normalize_pg_type("tsvector") == pa.string()
    # Type with precision suffix
    assert _normalize_pg_type("varchar(255)") == pa.string()


def test_py_val_to_arrow_covers_bson_types():
    from iceberg_writer import _py_val_to_arrow
    assert _py_val_to_arrow(True) == pa.bool_()
    assert _py_val_to_arrow(42) == pa.int64()
    assert _py_val_to_arrow(3.14) == pa.float64()
    assert _py_val_to_arrow("hello") == pa.string()
    assert _py_val_to_arrow(b"bytes") == pa.binary()
    assert _py_val_to_arrow(_dt.datetime(2024, 1, 1)) == pa.timestamp("us")
    assert _py_val_to_arrow(_dt.date(2024, 1, 1)) == pa.date32()
    assert _py_val_to_arrow({"k": "v"}) == pa.string()
    assert _py_val_to_arrow([1, 2, 3]) == pa.string()


# ─── _get_source_schema (mocked drivers) ─────────────────────────────────────
def test_get_source_schema_mysql_mocked():
    from iceberg_writer import _get_source_schema
    fake_rows = [
        ("id", "bigint", 1),
        ("name", "varchar", 2),
        ("created_at", "datetime", 3),
        ("all_null_col", "text", 4),
    ]
    cursor = MagicMock()
    cursor.fetchall.return_value = fake_rows
    cursor.__enter__ = lambda self: self
    cursor.__exit__ = lambda self, *a: None
    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.__enter__ = lambda self: self
    conn.__exit__ = lambda self, *a: None
    with patch("iceberg_writer.pymysql", create=True) if False else patch(
        "pymysql.connect", return_value=conn
    ):
        schema = _get_source_schema(
            {"connector_type": "mysql", "host": "h", "port": 3306,
             "database_name": "db", "username": "u", "password": "p"},
            "db", "users",
        )
    assert schema.field("id").type == pa.int64()
    assert schema.field("name").type == pa.string()
    assert schema.field("created_at").type == pa.timestamp("us")
    assert schema.field("all_null_col").type == pa.string()  # NOT pa.null()


def test_get_source_schema_postgres_mocked():
    from iceberg_writer import _get_source_schema
    fake_rows = [
        ("id", "bigint"),
        ("email", "character varying"),
        ("is_active", "boolean"),
        ("all_null_col", "text"),
    ]
    cursor = MagicMock()
    cursor.fetchall.return_value = fake_rows
    cursor.__enter__ = lambda self: self
    cursor.__exit__ = lambda self, *a: None
    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.__enter__ = lambda self: self
    conn.__exit__ = lambda self, *a: None
    with patch("psycopg2.connect", return_value=conn):
        schema = _get_source_schema(
            {"connector_type": "postgres", "host": "h", "port": 5432,
             "database_name": "db", "username": "u", "password": "p"},
            "public", "users",
        )
    assert schema.field("id").type == pa.int64()
    assert schema.field("email").type == pa.string()
    assert schema.field("is_active").type == pa.bool_()
    assert schema.field("all_null_col").type == pa.string()


def test_get_source_schema_unsupported_returns_empty():
    from iceberg_writer import _get_source_schema
    schema = _get_source_schema(
        {"connector_type": "oracle", "host": "h"}, "s", "t",
    )
    assert len(schema) == 0


# ─── Schema drift ────────────────────────────────────────────────────────────
def test_evolve_schema_for_drift_appends_new_columns():
    from iceberg_writer import _evolve_schema_for_drift
    cached = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
    ])
    new_cols = {"email": pa.string(), "age": pa.int32()}
    # The mock table's update_schema() returns a context manager whose
    # add_column is a no-op (we only test the schema bookkeeping here).
    table = MagicMock()
    cm = MagicMock()
    cm.__enter__ = lambda self: cm
    cm.__exit__ = lambda self, *a: None
    table.update_schema.return_value = cm
    evolved = _evolve_schema_for_drift(table, cached, new_cols)
    assert "email" in {f.name for f in evolved}
    assert "age" in {f.name for f in evolved}
    assert evolved.field("email").type == pa.string()
    assert evolved.field("age").type == pa.int32()


def test_evolve_schema_for_drift_no_new_columns_is_noop():
    from iceberg_writer import _evolve_schema_for_drift
    cached = pa.schema([pa.field("id", pa.int64())])
    table = MagicMock()
    evolved = _evolve_schema_for_drift(table, cached, {})
    assert evolved == cached
    table.update_schema.assert_not_called()
