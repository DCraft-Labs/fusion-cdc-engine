"""
DuckDB Transform Engine — all 10 transform types.
Runs inside the transform-worker pod with zero external dependencies.
"""
from __future__ import annotations

import logging
import os
import textwrap
import tempfile
from typing import Any

import duckdb
import requests

log = logging.getLogger(__name__)

# DuckDB type map
_DUCK_TYPES = {
    "string":    "VARCHAR",
    "str":       "VARCHAR",
    "int":       "INTEGER",
    "long":      "BIGINT",
    "double":    "DOUBLE",
    "float":     "FLOAT",
    "boolean":   "BOOLEAN",
    "bool":      "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "date":      "DATE",
}


class DuckDBTransformEngine:
    """
    Stateless transform engine. One instance per worker process.
    All state lives in DuckDB in-memory + temp files under DUCKDB_SCRATCH_DIR.
    """

    def __init__(self, metadata_db_dsn: str, encryption_key: str,
                 control_plane_url: str, worker_id: str):
        self.metadata_db_dsn = metadata_db_dsn
        self.encryption_key = encryption_key
        self.control_plane_url = control_plane_url
        self.worker_id = worker_id
        self.scratch_dir = os.getenv("DUCKDB_SCRATCH_DIR", "/tmp/duckdb")
        self.threads = int(os.getenv("DUCKDB_THREADS", "2"))
        self.memory_limit = os.getenv("DUCKDB_MEMORY_LIMIT", "3GB")
        # Defined here (not as class-level attr) so all handler functions are in scope
        self.STEP_HANDLERS = {
            "cast":                _apply_cast,
            "string_op":           _apply_string_op,
            "math_op":             _apply_math_op,
            "date_op":             _apply_date_op,
            "json_extract":        _apply_json_extract,
            "json_flatten_inline": _apply_json_flatten_inline,
            "json_flatten_child":  _apply_json_flatten_child,
            "mask":                _apply_mask,
            "expression":          _apply_expression,
            "udf":                 _apply_udf,
        }
        self.control_plane_url = control_plane_url
        self.worker_id = worker_id
        self.scratch_dir = os.getenv("DUCKDB_SCRATCH_DIR", "/tmp/duckdb")
        self.threads = int(os.getenv("DUCKDB_THREADS", "2"))
        self.memory_limit = os.getenv("DUCKDB_MEMORY_LIMIT", "3GB")

    def execute_pipeline(self, rows: list[dict], steps: list[dict]) -> list[dict]:
        """
        Apply a sequence of transform steps to a list of row dicts.
        Returns transformed rows.
        All steps run against a single DuckDB in-memory connection.
        """
        if not rows:
            return []

        with duckdb.connect(database=":memory:", config={
            "threads": self.threads,
            "memory_limit": self.memory_limit,
        }) as conn:
            # Load rows into staging table
            conn.execute("CREATE TABLE staging AS SELECT * FROM $1", [rows])

            child_tables: dict[str, list[dict]] = {}

            for step in steps:
                step_type = step.get("type")
                handler = self.STEP_HANDLERS.get(step_type)
                if handler is None:
                    log.warning("Unknown transform step type: %s — skipping", step_type)
                    continue
                try:
                    result = handler(conn, step, udf_registry_url=self.control_plane_url)
                    if isinstance(result, dict) and "child_table" in result:
                        # json_flatten_child produces a named child table
                        child_tables[result["child_table_name"]] = result["child_table"]
                except Exception:
                    log.exception("Transform step failed: %s", step)
                    raise

            # Return transformed rows
            transformed = conn.execute("SELECT * FROM staging").df().to_dict(orient="records")

        return transformed, child_tables


# ─── Transform step implementations ──────────────────────────────────────────

def _apply_cast(conn, step, **_):
    """Type 1: Cast a column to a different SQL type."""
    col = step["column"]
    to_type = _DUCK_TYPES.get(step.get("to_type", "string"), "VARCHAR")
    out = step.get("output_column", col)
    if out == col:
        conn.execute(f"ALTER TABLE staging ALTER COLUMN {col} TYPE {to_type} USING CAST({col} AS {to_type})")
    else:
        conn.execute(f"ALTER TABLE staging ADD COLUMN IF NOT EXISTS {out} {to_type}")
        conn.execute(f"UPDATE staging SET {out} = CAST({col} AS {to_type})")


def _apply_string_op(conn, step, **_):
    """Type 2: String operations — upper, lower, trim, substring, replace, concat."""
    col = step["column"]
    # Frontend sends 'op', some callers use 'operation' — handle both
    op = step.get("op") or step.get("operation", "trim")
    out = step.get("output_column", col)
    extra = step.get("params") or step.get("extra") or {}

    op_expr = {
        "upper":     f"upper({col})",
        "lower":     f"lower({col})",
        "trim":      f"trim({col})",
        "ltrim":     f"ltrim({col})",
        "rtrim":     f"rtrim({col})",
        "substring": f"substring({col}, {extra.get('start', 1)}, {extra.get('length', 255)})",
        "replace":   f"replace({col}, '{extra.get('from', '')}', '{extra.get('to', '')}')",
        "concat":    f"concat({col}, '{extra.get('suffix', '')}')",
        "lpad":      f"lpad({col}, {extra.get('length', 10)}, '{extra.get('pad', ' ')}')",
        "rpad":      f"rpad({col}, {extra.get('length', 10)}, '{extra.get('pad', ' ')}')",
    }.get(op, f"trim({col})")

    if out == col:
        conn.execute(f"UPDATE staging SET {col} = {op_expr}")
    else:
        conn.execute(f"ALTER TABLE staging ADD COLUMN IF NOT EXISTS {out} VARCHAR")
        conn.execute(f"UPDATE staging SET {out} = {op_expr}")


def _apply_math_op(conn, step, **_):
    """Type 3: Mathematical expression — arithmetic on numeric columns."""
    col = step["column"]
    expression = step.get("expression", col)
    out = step.get("output_column", col)
    dtype = _DUCK_TYPES.get(step.get("output_type", "double"), "DOUBLE")

    if out == col:
        conn.execute(f"UPDATE staging SET {col} = ({expression})")
    else:
        conn.execute(f"ALTER TABLE staging ADD COLUMN IF NOT EXISTS {out} {dtype}")
        conn.execute(f"UPDATE staging SET {out} = ({expression})")


def _apply_date_op(conn, step, **_):
    """Type 4: Date/time operations — extract parts, arithmetic, formatting."""
    col = step["column"]
    op = step.get("operation", "year")
    out = step.get("output_column", f"{col}_{op}")
    extra = step.get("extra", {})

    op_expr = {
        "year":        f"year({col})",
        "month":       f"month({col})",
        "day":         f"dayofmonth({col})",
        "hour":        f"hour({col})",
        "minute":      f"minute({col})",
        "epoch":       f"epoch({col})",
        "date_format": f"strftime('{extra.get('format', '%Y-%m-%d')}', {col})",
        "date_add":    f"{col} + INTERVAL '{extra.get('value', 1)}' {extra.get('unit', 'DAY')}",
        "date_diff":   f"datediff('{extra.get('unit', 'day')}', {extra.get('other', col)}, {col})",
    }.get(op, f"year({col})")

    conn.execute(f"ALTER TABLE staging ADD COLUMN IF NOT EXISTS {out} VARCHAR")
    conn.execute(f"UPDATE staging SET {out} = CAST({op_expr} AS VARCHAR)")


def _apply_json_extract(conn, step, **_):
    """Type 5: Extract a single field from a JSON string column."""
    col = step["column"]
    # Frontend sends 'json_path', engine spec uses 'path' — handle both
    path = step.get("json_path") or step.get("path", "$.value")
    out = step.get("output_column", f"{col}_extracted")
    as_type = step.get("output_type", "string")
    duck_type = _DUCK_TYPES.get(as_type, "VARCHAR")

    conn.execute(f"ALTER TABLE staging ADD COLUMN IF NOT EXISTS {out} {duck_type}")
    if duck_type == "VARCHAR":
        conn.execute(f"UPDATE staging SET {out} = json_extract_string({col}, '{path}')")
    else:
        conn.execute(f"UPDATE staging SET {out} = CAST(json_extract({col}, '{path}') AS {duck_type})")


def _apply_json_flatten_inline(conn, step, **_):
    """Type 6: Flatten JSON object into multiple columns on the same row."""
    col = step["column"]
    schema = step.get("json_schema", {})   # {"field_name": "type"}
    output_columns = step.get("output_columns", {})  # optional rename map

    for field, dtype in schema.items():
        out_col = output_columns.get(field, f"{col}_{field}")
        duck_type = _DUCK_TYPES.get(dtype, "VARCHAR")
        conn.execute(f"ALTER TABLE staging ADD COLUMN IF NOT EXISTS {out_col} {duck_type}")
        if duck_type == "VARCHAR":
            conn.execute(f"UPDATE staging SET {out_col} = json_extract_string({col}, '$.{field}')")
        else:
            conn.execute(f"UPDATE staging SET {out_col} = CAST(json_extract({col}, '$.{field}') AS {duck_type})")


def _apply_json_flatten_child(conn, step, **_):
    """
    Type 7: Explode JSON array into a separate child table.
    Returns child rows separately — caller writes them to a different destination table.
    """
    col = step["column"]
    child_table_name = step.get("child_table", f"{col}_items")
    pk_col = step.get("parent_pk", "id")

    # Unnest JSON array; DuckDB UNNEST handles this natively
    child_rows = conn.execute(f"""
        SELECT
            parent.{pk_col} AS parent_{pk_col},
            unnest(from_json(parent.{col}, '[]')) AS item
        FROM staging AS parent
        WHERE parent.{col} IS NOT NULL AND parent.{col} != 'null'
    """).df().to_dict(orient="records")

    return {"child_table": child_rows, "child_table_name": child_table_name}


def _apply_mask(conn, step, **_):
    """Type 8: Data masking — last4, hash (SHA-256), or full null."""
    col = step["column"]
    strategy = step.get("strategy", "last4")
    out = step.get("output_column", col)

    if strategy == "last4":
        expr = f"""
            CASE
                WHEN length({col}) > 4
                THEN repeat('*', length({col}) - 4) || right({col}, 4)
                ELSE {col}
            END
        """
    elif strategy == "hash":
        expr = f"sha256({col}::BLOB)::VARCHAR"
    elif strategy == "null":
        expr = "NULL"
    elif strategy == "first4":
        expr = f"""
            CASE
                WHEN length({col}) > 4
                THEN left({col}, 4) || repeat('*', length({col}) - 4)
                ELSE {col}
            END
        """
    elif strategy == "email":
        # user@domain.com → u***@domain.com
        expr = f"""
            CASE
                WHEN {col} LIKE '%@%'
                THEN substring({col}, 1, 1) || '***' || substring({col}, strpos({col}, '@'))
                ELSE repeat('*', length({col}))
            END
        """
    else:
        expr = "NULL"

    if out == col:
        conn.execute(f"UPDATE staging SET {col} = {expr}")
    else:
        conn.execute(f"ALTER TABLE staging ADD COLUMN IF NOT EXISTS {out} VARCHAR")
        conn.execute(f"UPDATE staging SET {out} = {expr}")


def _apply_expression(conn, step, **_):
    """Type 9: Arbitrary SQL expression with full DuckDB SQL support."""
    expr = step["expression"]
    out = step.get("output_column", "expr_result")
    dtype = _DUCK_TYPES.get(step.get("output_type", "string"), "VARCHAR")

    conn.execute(f"ALTER TABLE staging ADD COLUMN IF NOT EXISTS {out} {dtype}")
    conn.execute(f"UPDATE staging SET {out} = ({expr})")


def _apply_udf(conn, step, udf_registry_url: str = "", **_):
    """
    Type 10: Python UDF registered with DuckDB — runs natively, zero JVM overhead.
    UDF code is fetched from the control-plane UDF registry.
    """
    fn_name = step["function"]
    args = step.get("args", [])
    out = step.get("output_column", f"{fn_name}_result")
    return_type = step.get("return_type", "string")
    duck_type = _DUCK_TYPES.get(return_type, "VARCHAR")

    # Fetch UDF code from control-plane registry
    udf_code_url = f"{udf_registry_url}/api/v1/udfs/{fn_name}"
    resp = requests.get(udf_code_url, timeout=10)
    resp.raise_for_status()
    udf_def = resp.json()
    code = udf_def["code"]  # Python function source

    # Execute UDF code in isolated namespace and register with DuckDB
    namespace: dict[str, Any] = {}
    exec(textwrap.dedent(code), namespace)  # nosec B102 — admin-registered UDFs only
    fn = namespace[fn_name]

    py_type_map = {"string": str, "int": int, "long": int, "double": float, "boolean": bool}
    duckdb.create_function(fn_name, fn, return_type=py_type_map.get(return_type, str))

    args_str = ", ".join(args) if args else ""
    conn.execute(f"ALTER TABLE staging ADD COLUMN IF NOT EXISTS {out} {duck_type}")
    conn.execute(f"UPDATE staging SET {out} = {fn_name}({args_str})")
