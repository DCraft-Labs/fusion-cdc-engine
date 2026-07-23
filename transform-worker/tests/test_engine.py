"""v1.2.22 Bug B tests — DuckDB parameter binding fix + all 10 step handlers.

Covers:
- ``execute_pipeline`` with an empty ``steps`` list → rows unchanged
  (schema preserved).
- ``execute_pipeline`` with each of the 10 step types → succeeds.
- The $1 binding bug is gone (the old code raised
  ``duckdb.InvalidInputException``; the new code registers an Arrow view).
- The transformed schema returned by ``execute_pipeline`` preserves the
  declared type of all-NULL columns.
- ``_apply_udf`` registers the function on the connection (not the module).
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

import duckdb


def _make_engine():
    from engine import DuckDBTransformEngine
    return DuckDBTransformEngine(
        metadata_db_dsn="sqlite://",
        encryption_key="k",
        control_plane_url="http://control-plane",
        worker_id="w-test",
    )


# ─── Bug B: $1 binding replaced by Arrow view ─────────────────────────────────
def test_execute_pipeline_empty_steps_returns_rows_unchanged():
    """Bug B regression: the old code raised on $1 binding even with no
    transforms. The new code registers an Arrow view and returns the rows.
    """
    engine = _make_engine()
    rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    transformed, child_tables, schema = engine.execute_pipeline(rows, [])
    assert len(transformed) == 2
    assert transformed[0]["id"] == 1
    assert child_tables == {}
    assert schema is not None
    assert schema.field("id").type == pa.int64()


def test_execute_pipeline_empty_rows_returns_empty():
    engine = _make_engine()
    transformed, child_tables, schema = engine.execute_pipeline([], [])
    assert transformed == []
    assert child_tables == {}


def test_execute_pipeline_preserves_all_null_column_type_via_schema():
    """Bug A + B together: pass an explicit source schema so the all-NULL
    column keeps its declared type through the DuckDB round-trip.
    """
    engine = _make_engine()
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("all_null_str", pa.string()),
    ])
    rows = [{"id": 1, "all_null_str": None}, {"id": 2, "all_null_str": None}]
    transformed, _, out_schema = engine.execute_pipeline(rows, [], schema=schema)
    assert out_schema.field("all_null_str").type == pa.string()
    assert all(r["all_null_str"] is None for r in transformed)


# ─── All 10 step handlers ─────────────────────────────────────────────────────
def test_step_cast():
    engine = _make_engine()
    rows = [{"id": "42", "name": "a"}]
    steps = [{"type": "cast", "column": "id", "to_type": "int"}]
    transformed, _, schema = engine.execute_pipeline(rows, steps)
    # The cast ALTER COLUMN should make id an INTEGER in DuckDB
    assert "id" in {f.name for f in schema}
    # DuckDB INTEGER → Arrow int32
    assert schema.field("id").type == pa.int32()
    assert transformed[0]["id"] == 42


def test_step_string_op_upper():
    engine = _make_engine()
    rows = [{"id": 1, "name": "alice"}]
    steps = [{"type": "string_op", "column": "name", "op": "upper"}]
    transformed, _, _ = engine.execute_pipeline(rows, steps)
    assert transformed[0]["name"] == "ALICE"


def test_step_string_op_trim():
    engine = _make_engine()
    rows = [{"id": 1, "name": "  alice  "}]
    steps = [{"type": "string_op", "column": "name", "op": "trim"}]
    transformed, _, _ = engine.execute_pipeline(rows, steps)
    assert transformed[0]["name"] == "alice"


def test_step_math_op():
    engine = _make_engine()
    rows = [{"id": 1, "a": 10, "b": 5}]
    steps = [{"type": "math_op", "column": "a", "expression": "a + b",
              "output_column": "sum", "output_type": "long"}]
    transformed, _, schema = engine.execute_pipeline(rows, steps)
    assert "sum" in {f.name for f in schema}
    assert transformed[0]["sum"] == 15


def test_step_date_op_year():
    engine = _make_engine()
    rows = [{"id": 1, "d": "2024-03-15"}]
    steps = [{"type": "date_op", "column": "d", "operation": "year"}]
    transformed, _, _ = engine.execute_pipeline(rows, steps)
    assert transformed[0]["d_year"] == "2024"


def test_step_json_extract():
    engine = _make_engine()
    rows = [{"id": 1, "payload": '{"name": "alice", "age": 30}'}]
    steps = [{"type": "json_extract", "column": "payload",
              "json_path": "$.name", "output_column": "name",
              "output_type": "string"}]
    transformed, _, _ = engine.execute_pipeline(rows, steps)
    assert transformed[0]["name"] == "alice"


def test_step_json_flatten_inline():
    engine = _make_engine()
    rows = [{"id": 1, "payload": '{"name": "alice", "age": 30}'}]
    steps = [{
        "type": "json_flatten_inline",
        "column": "payload",
        "json_schema": {"name": "string", "age": "int"},
    }]
    transformed, _, schema = engine.execute_pipeline(rows, steps)
    assert "payload_name" in {f.name for f in schema}
    assert "payload_age" in {f.name for f in schema}
    assert transformed[0]["payload_name"] == "alice"
    assert transformed[0]["payload_age"] == 30


def test_step_json_flatten_child():
    engine = _make_engine()
    rows = [{"id": 1, "tags": '["a", "b", "c"]'}]
    steps = [{
        "type": "json_flatten_child",
        "column": "tags",
        "child_table": "tags_child",
        "parent_pk": "id",
    }]
    _, child_tables, _ = engine.execute_pipeline(rows, steps)
    assert "tags_child" in child_tables
    assert len(child_tables["tags_child"]) == 3


def test_step_mask_last4():
    engine = _make_engine()
    rows = [{"id": 1, "ssn": "123456789"}]
    steps = [{"type": "mask", "column": "ssn", "strategy": "last4"}]
    transformed, _, _ = engine.execute_pipeline(rows, steps)
    assert transformed[0]["ssn"] == "*****6789"


def test_step_mask_hash():
    engine = _make_engine()
    rows = [{"id": 1, "ssn": "secret"}]
    steps = [{"type": "mask", "column": "ssn", "strategy": "hash"}]
    transformed, _, _ = engine.execute_pipeline(rows, steps)
    # SHA-256 of "secret" is a known 64-char hex string
    assert len(transformed[0]["ssn"]) == 64


def test_step_expression():
    engine = _make_engine()
    rows = [{"id": 1, "a": 10, "b": 5}]
    steps = [{
        "type": "expression",
        "expression": "a * b",
        "output_column": "product",
        "output_type": "long",
    }]
    transformed, _, schema = engine.execute_pipeline(rows, steps)
    assert "product" in {f.name for f in schema}
    assert transformed[0]["product"] == 50


def test_step_udf_registers_on_connection():
    """Bug B2: UDF must be registered on the connection, not the module.
    The old code called ``duckdb.create_function`` (module-level) which
    returned a function object that was never attached to the in-memory
    connection — the subsequent UPDATE raised ``Table Function not found``.
    """
    engine = _make_engine()
    rows = [{"id": 1, "name": "alice"}]
    udf_code = "def shout(s):\n    return s.upper() if s else s\n"
    steps = [{
        "type": "udf",
        "function": "shout",
        "args": ["name"],
        "output_column": "shouted",
        "return_type": "string",
    }]
    resp = MagicMock()
    resp.json.return_value = {"code": udf_code}
    resp.raise_for_status = lambda: None
    with patch("engine.requests.get", return_value=resp):
        transformed, _, schema = engine.execute_pipeline(rows, steps)
    assert "shouted" in {f.name for f in schema}
    assert transformed[0]["shouted"] == "ALICE"


# ─── Unknown step type is skipped, not fatal ─────────────────────────────────
def test_unknown_step_type_skipped():
    engine = _make_engine()
    rows = [{"id": 1, "name": "a"}]
    steps = [{"type": "nonexistent_op", "column": "name"}]
    transformed, _, _ = engine.execute_pipeline(rows, steps)
    # Rows pass through unchanged
    assert transformed[0]["name"] == "a"
