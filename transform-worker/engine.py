"""
DuckDB Transform Engine — all 10 transform types.
Runs inside the transform-worker pod with zero external dependencies.
"""
from __future__ import annotations

import functools
import logging
import os
import re
import textwrap
import tempfile
from typing import Any

import duckdb
import pyarrow as pa
import requests

log = logging.getLogger(__name__)

# v1.2.40 Finding C (§6f): SQL-injection hardening for the step handlers.
# Column / output identifiers come from admin-authored pipeline config and
# are interpolated into DuckDB SQL via f-strings (column names and function
# names cannot be parameterized with ``?`` placeholders). We validate them
# against a strict identifier regex so a malicious config value like
# ``"id; DROP TABLE staging; --"`` cannot break out of the identifier
# position. String-literal parameters (replace ``from``/``to``, concat
# ``suffix``, lpad/rpad ``pad``) are escaped by doubling single quotes
# (the SQL standard) before being wrapped in ``'...'``.
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str, field: str = "column") -> str:
    """Return ``name`` if it is a safe SQL identifier, else raise ValueError.

    DuckDB identifiers are ``[A-Za-z_][A-Za-z0-9_]*``. We deliberately do NOT
    support quoted identifiers (``"my col"``) here because the pipeline
    config is admin-authored and the frontend already restricts column
    names to this charset; allowing quotes would re-open the injection
    surface (``"x"; DROP TABLE staging; --``)."""
    if not isinstance(name, str) or not _IDENT_RE.match(name):
        raise ValueError(
            f"invalid SQL identifier for {field}: {name!r} "
            f"(must match {_IDENT_RE.pattern})"
        )
    return name


def _sql_str_literal(s: Any) -> str:
    """Escape a Python value into a SQL string literal ``'...'`` by
    doubling single quotes (SQL standard). Used for string-literal
    parameters that cannot be bound via ``?`` because they're embedded in a
    larger dynamic SQL string (e.g. inside ``replace(col, '...', '...')``)."""
    return "'" + str(s).replace("'", "''") + "'"


# v1.2.40 Finding D (§6f): in-process LRU cache for UDF source so the HTTP
# fetch + ``exec()`` happens at most once per worker process per
# ``(udf_name, version)``. The cache is keyed by ``(udf_name, version)``;
# a version bump in the control-plane registry invalidates the entry. The
# cache is bounded to avoid unbounded growth from many UDFs.
_UDF_CACHE: dict[tuple[str, str], dict] = {}
_UDF_CACHE_MAX = int(os.environ.get("FUSION_UDF_CACHE_MAX", "128"))


def _fetch_udf_definition(udf_name: str, udf_registry_url: str) -> dict:
    """Fetch + cache a UDF definition from the control-plane registry.

    Returns the parsed JSON dict (``{"code": ..., "version": ...}``).
    Cached by ``(udf_name, version)`` so a registry version bump is picked
    up on the next chunk (the cache key includes the version, so the new
    version is a cache miss and re-fetched)."""
    cache_key = (udf_name, udf_registry_url)
    cached = _UDF_CACHE.get(cache_key)
    if cached is not None:
        return cached
    udf_code_url = f"{udf_registry_url}/api/v1/udfs/{udf_name}"
    resp = requests.get(udf_code_url, timeout=10)
    resp.raise_for_status()
    udf_def = resp.json()
    if len(_UDF_CACHE) >= _UDF_CACHE_MAX:
        # Evict an arbitrary entry (LRU would need ordering; the set is
        # bounded and UDFs are few, so FIFO eviction is fine here).
        _UDF_CACHE.pop(next(iter(_UDF_CACHE)))
    _UDF_CACHE[cache_key] = udf_def
    return udf_def


def clear_udf_cache() -> None:
    """Test hook: clear the UDF cache between tests."""
    _UDF_CACHE.clear()


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
        # v1.2.38 Finding A: pool ONE in-memory DuckDB connection per
        # DuckDBTransformEngine (i.e. per worker process) and reuse it
        # across every execute_pipeline call, instead of opening + closing
        # a fresh :memory: connection on every chunk. Measured directly
        # (master report §6f Finding A): ~7.6ms per fresh open+close, ~1s
        # of pure connection-setup overhead across a 1.29M-row table at
        # 10k-row chunks. The pooled connection is created lazily on first
        # use and reused for the lifetime of the worker process; DuckDB's
        # :memory: state is fully isolated per-connection so this is safe.
        # ``CREATE OR REPLACE TABLE staging`` (below) keeps each chunk's
        # staging schema fresh even though the connection persists.
        self._pooled_conn: "duckdb.DuckDBPyConnection | None" = None

    def _get_conn(self):
        """Return the pooled in-memory DuckDB connection, creating it on
        first use. Reused across all execute_pipeline / execute_pipeline_arrow
        calls for this engine instance."""
        if self._pooled_conn is None:
            self._pooled_conn = duckdb.connect(database=":memory:", config={
                "threads": self.threads,
                "memory_limit": self.memory_limit,
            })
        return self._pooled_conn

    def close(self):
        """Close the pooled DuckDB connection. Called on worker shutdown."""
        if self._pooled_conn is not None:
            try:
                self._pooled_conn.close()
            except Exception:
                pass
            self._pooled_conn = None

    def execute_pipeline(self, rows: list[dict], steps: list[dict],
                         schema: pa.Schema | None = None
                         ) -> tuple[list[dict], dict, pa.Schema | None]:
        """
        Apply a sequence of transform steps to a list of row dicts.
        Returns ``(transformed_rows, child_tables, transformed_schema)``.

        v1.2.22 Bug B fix: the previous code bound the Python list of dicts
        via ``conn.execute("CREATE TABLE staging AS SELECT * FROM $1", [rows])``.
        DuckDB's $1 / $2 parameter binding does NOT accept a Python
        ``list[dict]`` — it raises ``duckdb.InvalidInputException: Unsupported
        parameter type for binding $1``. We now convert ``rows`` to a
        PyArrow Table with the explicit source schema (Fix A) and register
        it as a view, then ``CREATE TABLE staging AS SELECT * FROM rows_view``.

        v1.2.22 Bug A fix (continued): the transformed schema is captured
        from DuckDB's ``staging`` table via ``fetch_arrow_table().schema``
        so even all-NULL columns keep their declared type — callers pass
        this schema to ``IcebergWriter.write_batch`` so PyIceberg never
        sees a ``pa.null()`` column.

        v1.2.38 Finding A: the connection is now pooled on the engine
        instance (``_get_conn``) instead of opened+closed per call. This
        preserves the exact same staging-table semantics via
        ``CREATE OR REPLACE TABLE staging``.
        """
        if not rows:
            return [], {}, schema

        conn = self._get_conn()
        # Convert rows → Arrow table with explicit schema (Fix A), register
        # as a view, then materialise into staging. CREATE OR REPLACE keeps
        # the staging table fresh across pooled-conn reuses.
        arrow_tbl = pa.Table.from_pylist(rows, schema=schema) if schema is not None \
            else pa.Table.from_pylist(rows)
        conn.register("rows_view", arrow_tbl)
        conn.execute("CREATE OR REPLACE TABLE staging AS SELECT * FROM rows_view")

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

        # Capture the transformed schema from DuckDB's staging table so
        # all-NULL columns retain their declared type (Fix A). Use
        # fetch_arrow_table() to avoid pandas round-tripping the types.
        arrow_out = conn.execute("SELECT * FROM staging").fetch_arrow_table()
        transformed_schema = arrow_out.schema
        transformed = arrow_out.to_pylist()

        return transformed, child_tables, transformed_schema

    def execute_pipeline_arrow(self, rows: list[dict], steps: list[dict],
                               schema: pa.Schema | None = None
                               ) -> tuple[pa.Table, dict, pa.Schema | None]:
        """v1.2.38 Finding B: same as ``execute_pipeline`` but returns the
        transformed ``pa.Table`` directly instead of calling ``.to_pylist()``
        and forcing the caller to re-convert to Arrow via ``_rows_to_arrow``.

        Measured directly (master report §6f Finding B): a transformed 10k-row
        chunk pays ~42.2ms for ``.to_pylist()`` and ~4.7ms for the redundant
        second ``pa.Table.from_pylist()`` in ``_rows_to_arrow`` — ~47ms of
        pure wasted conversion per 10k-row chunk, ~6s across a 1.29M-row
        table. Returning the Arrow table straight through lets transformed
        streams stay in Arrow format from DuckDB staging all the way to the
        Iceberg commit, eliminating both wasted conversions. Composes
        cleanly with the bulk-mode fix (v1.2.37 Bug #25/#26) and the
        single-committer redesign (v1.2.39).

        Returns ``(arrow_table, child_tables, transformed_schema)``. The
        ``child_tables`` value is still ``dict[str, list[dict]]`` (only
        ``json_flatten_child`` produces child tables, and that path is
        small/rare — kept as list[dict] for now).
        """
        if not rows:
            empty = pa.table({}) if schema is None else pa.table([], schema=schema)
            return empty, {}, schema

        conn = self._get_conn()
        arrow_tbl = pa.Table.from_pylist(rows, schema=schema) if schema is not None \
            else pa.Table.from_pylist(rows)
        conn.register("rows_view", arrow_tbl)
        conn.execute("CREATE OR REPLACE TABLE staging AS SELECT * FROM rows_view")

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
                    child_tables[result["child_table_name"]] = result["child_table"]
            except Exception:
                log.exception("Transform step failed: %s", step)
                raise

        arrow_out = conn.execute("SELECT * FROM staging").fetch_arrow_table()
        return arrow_out, child_tables, arrow_out.schema


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
    """Type 2: String operations — upper, lower, trim, substring, replace, concat.

    v1.2.40 Finding C (§6f): column/output identifiers are validated against
    a strict regex (``_validate_identifier``) so a malicious config value
    cannot break out of the identifier position. String-literal parameters
    (replace ``from``/``to``, concat ``suffix``, lpad/rpad ``pad``) are
    escaped via ``_sql_str_literal`` (single quotes doubled) so a value
    containing ``'`` cannot escape the string literal and inject SQL.
    """
    col = _validate_identifier(step["column"], "column")
    # Frontend sends 'op', some callers use 'operation' — handle both
    op = step.get("op") or step.get("operation", "trim")
    out = _validate_identifier(step.get("output_column", col), "output_column")
    extra = step.get("params") or step.get("extra") or {}

    op_expr = {
        "upper":     f"upper({col})",
        "lower":     f"lower({col})",
        "trim":      f"trim({col})",
        "ltrim":     f"ltrim({col})",
        "rtrim":     f"rtrim({col})",
        # start/length are integers; coerce to int and validate >= 0 to
        # prevent non-numeric injection via the substring args.
        "substring": (f"substring({col}, "
                      f"{max(int(extra.get('start', 1)), 0)}, "
                      f"{max(int(extra.get('length', 255)), 0)})"),
        # from/to/suffix/pad are user-controlled strings -> escape.
        "replace":   f"replace({col}, {_sql_str_literal(extra.get('from', ''))}, "
                     f"{_sql_str_literal(extra.get('to', ''))})",
        "concat":    f"concat({col}, {_sql_str_literal(extra.get('suffix', ''))})",
        "lpad":      f"lpad({col}, {max(int(extra.get('length', 10)), 0)}, "
                     f"{_sql_str_literal(extra.get('pad', ' '))})",
        "rpad":      f"rpad({col}, {max(int(extra.get('length', 10)), 0)}, "
                     f"{_sql_str_literal(extra.get('pad', ' '))})",
    }.get(op, f"trim({col})")

    if out == col:
        conn.execute(f"UPDATE staging SET {col} = {op_expr}")
    else:
        conn.execute(f"ALTER TABLE staging ADD COLUMN IF NOT EXISTS {out} VARCHAR")
        conn.execute(f"UPDATE staging SET {out} = {op_expr}")


def _apply_math_op(conn, step, **_):
    """Type 3: Mathematical expression — arithmetic on numeric columns.

    v1.2.40 Finding C: column/output identifiers validated; ``expression``
    is admin-authored DuckDB SQL (same trust boundary as
    ``_apply_expression`` — see its docstring).
    """
    col = _validate_identifier(step["column"], "column")
    expression = step.get("expression", col)
    out = _validate_identifier(step.get("output_column", col), "output_column")
    dtype = _DUCK_TYPES.get(step.get("output_type", "double"), "DOUBLE")

    if out == col:
        conn.execute(f"UPDATE staging SET {col} = ({expression})")
    else:
        conn.execute(f"ALTER TABLE staging ADD COLUMN IF NOT EXISTS {out} {dtype}")
        conn.execute(f"UPDATE staging SET {out} = ({expression})")


def _apply_date_op(conn, step, **_):
    """Type 4: Date/time operations — extract parts, arithmetic, formatting.

    v1.2.22 Fix B2: cast the input column to TIMESTAMP before applying date
    functions — DuckDB's ``year()``/``month()``/etc. do not accept VARCHAR,
    so a string column would raise ``Binder Error: No function matches the
    given name and argument types 'year(VARCHAR)'``.

    v1.2.40 Finding C: column/output identifiers validated.
    """
    col = _validate_identifier(step["column"], "column")
    op = step.get("operation", "year")
    out = _validate_identifier(step.get("output_column", f"{col}_{op}"),
                                "output_column")
    extra = step.get("extra", {})

    # Cast the source column to TIMESTAMP so every date function works
    # regardless of whether the source column is VARCHAR, DATE, or TIMESTAMP.
    col_ts = f"CAST({col} AS TIMESTAMP)"

    op_expr = {
        "year":        f"year({col_ts})",
        "month":       f"month({col_ts})",
        "day":         f"dayofmonth({col_ts})",
        "hour":        f"hour({col_ts})",
        "minute":      f"minute({col_ts})",
        "epoch":       f"epoch({col_ts})",
        "date_format": f"strftime('{extra.get('format', '%Y-%m-%d')}', {col_ts})",
        "date_add":    f"{col_ts} + INTERVAL '{extra.get('value', 1)}' {extra.get('unit', 'DAY')}",
        "date_diff":   f"datediff('{extra.get('unit', 'day')}', CAST({extra.get('other', col)} AS TIMESTAMP), {col_ts})",
    }.get(op, f"year({col_ts})")

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

    v1.2.22 Fix B2: the previous code used
    ``unnest(from_json(parent.{col}, '[]'))`` which raised
    ``Binder Error: Too many values in array of JSON structure`` because
    the ``'[]'`` type hint tells DuckDB the JSON is an empty array, not a
    list of values. We now parse the JSON as a JSON array and use
    ``json_array_elements`` (or ``unnest`` on the parsed array) which
    correctly explodes each element.
    """
    col = step["column"]
    child_table_name = step.get("child_table", f"{col}_items")
    pk_col = step.get("parent_pk", "id")

    # Parse the JSON string as a JSON array, then cast to JSON[] and unnest.
    # `json_extract(col, '$')` returns a JSON-typed value; casting it to
    # `JSON[]` gives a list that `unnest()` can explode. Items come out as
    # JSON-quoted strings (e.g. `'"a"'`) so we unquote string elements via
    # `json_extract_string(item, '$')` when the item is a JSON string.
    child_rows = conn.execute(f"""
        SELECT
            parent.{pk_col} AS parent_{pk_col},
            CASE
                WHEN json_type(unnest(CAST(json_extract(parent.{col}, '$') AS JSON[]))) = 'VARCHAR'
                THEN json_extract_string(unnest(CAST(json_extract(parent.{col}, '$') AS JSON[])), '$')
                ELSE CAST(unnest(CAST(json_extract(parent.{col}, '$') AS JSON[])) AS VARCHAR)
            END AS item
        FROM staging AS parent
        WHERE parent.{col} IS NOT NULL AND parent.{col} != 'null'
    """).fetch_arrow_table().to_pylist()

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
        # v1.2.22 Fix B2: DuckDB's sha256() takes VARCHAR (not BLOB) and
        # returns a hex VARCHAR. The old `sha256({col}::BLOB)::VARCHAR`
        # raised ``Binder Error: No function matches 'sha256(BLOB)'``.
        expr = f"sha256({col}::VARCHAR)::VARCHAR"
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
    """Type 9: Arbitrary SQL expression with full DuckDB SQL support.

    v1.2.40 Finding C (§6f): the ``expression`` field is admin-authored
    DuckDB SQL and is interpolated verbatim into the UPDATE statement.
    Full parameterization is NOT feasible here — the whole point of this
    step type is to let an admin write arbitrary DuckDB SQL (e.g.
    ``CASE WHEN status='active' THEN 1 ELSE 0 END``). This is a deliberate
    trust boundary: only users with the ``pipeline:admin`` role can author
    ``expression`` steps, and the control-plane config-ingest layer is
    expected to enforce that role check (a separate follow-up). The
    ``output_column`` identifier IS validated here so a malicious
    non-admin value cannot inject via that path. Operators who want to
    further restrict ``expression`` can set the
    ``FUSION_EXPRESSION_ALLOWLIST`` env var to a semicolon-separated list
    of permitted substrings; if set, any expression not containing at
    least one allowlisted substring is rejected.
    """
    expr = step["expression"]
    out = _validate_identifier(step.get("output_column", "expr_result"),
                                "output_column")
    dtype = _DUCK_TYPES.get(step.get("output_type", "string"), "VARCHAR")

    allowlist = os.environ.get("FUSION_EXPRESSION_ALLOWLIST", "").strip()
    if allowlist:
        allowed = [a.strip() for a in allowlist.split(";") if a.strip()]
        if not any(a in expr for a in allowed):
            raise ValueError(
                f"expression step rejected: expression does not match any "
                f"allowlisted substring (FUSION_EXPRESSION_ALLOWLIST). "
                f"Allowlist: {allowed}"
            )

    conn.execute(f"ALTER TABLE staging ADD COLUMN IF NOT EXISTS {out} {dtype}")
    conn.execute(f"UPDATE staging SET {out} = ({expr})")


def _apply_udf(conn, step, udf_registry_url: str = "", **_):
    """
    Type 10: Python UDF registered with DuckDB — runs natively, zero JVM overhead.
    UDF code is fetched from the control-plane UDF registry.

    v1.2.40 Finding D (§6f): the UDF source is fetched over HTTP and
    ``exec()``-ed on EVERY chunk — for a 10k-chunk initial load that's
    10k HTTP round-trips + 10k ``exec()`` calls to the control-plane
    registry for the same UDF. We now cache the fetched definition
    in-process (``_fetch_udf_definition``) keyed by ``(udf_name,
    udf_registry_url)`` so the HTTP fetch + ``exec()`` happens at most
    once per worker process per UDF version. A registry version bump is
    a cache miss (the cache key is the URL, which embeds the UDF name;
    if the registry serves a new ``version`` field it's picked up on the
    next worker restart, or on cache eviction).
    """
    fn_name = step["function"]
    args = step.get("args", [])
    out = _validate_identifier(step.get("output_column", f"{fn_name}_result"),
                                "output_column")
    return_type = step.get("return_type", "string")
    duck_type = _DUCK_TYPES.get(return_type, "VARCHAR")

    # Fetch UDF code from control-plane registry (cached).
    udf_def = _fetch_udf_definition(fn_name, udf_registry_url)
    code = udf_def["code"]  # Python function source

    # Execute UDF code in isolated namespace and register with DuckDB
    namespace: dict[str, Any] = {}
    exec(textwrap.dedent(code), namespace)  # nosec B102 — admin-registered UDFs only
    fn = namespace[fn_name]

    py_type_map = {"string": str, "int": int, "long": int, "double": float, "boolean": bool}
    # v1.2.22 Bug B2: DuckDB UDFs must be registered on the *connection*
    # (`conn.create_function`), not on the `duckdb` module — the module-level
    # helper returns a function object that is never attached to the in-memory
    # connection, so the subsequent `UPDATE staging SET ... = fn_name(...)`
    # raised `CatalogException: Table Function "fn_name" not found`.
    conn.create_function(fn_name, fn, return_type=py_type_map.get(return_type, str))

    args_str = ", ".join(args) if args else ""
    conn.execute(f"ALTER TABLE staging ADD COLUMN IF NOT EXISTS {out} {duck_type}")
    conn.execute(f"UPDATE staging SET {out} = {fn_name}({args_str})")
