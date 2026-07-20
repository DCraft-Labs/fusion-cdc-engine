"""
TransformPipelineExecutor — registry-based Spark transform pipeline.

Pipeline spec (JSON / dict):
    {
        "transforms": [
            {"id": "...", "type": "<step_type>", ...step-specific fields...}
        ]
    }

Supported step types (10):
  cast, string_op, math_op, date_op, json_extract,
  json_flatten_inline, json_flatten_child, mask, expression, udf
"""
from __future__ import annotations

import logging
import textwrap
from typing import Dict, Optional

import requests

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type mapping: DSL string → PySpark type
# ---------------------------------------------------------------------------

_TYPE_MAP = {
    "string": StringType(),
    "str": StringType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "long": LongType(),
    "bigint": LongType(),
    "double": DoubleType(),
    "float": DoubleType(),
    "boolean": BooleanType(),
    "bool": BooleanType(),
    "timestamp": TimestampType(),
}


def _spark_type(type_str: str):
    return _TYPE_MAP.get(type_str.lower(), StringType())


# ---------------------------------------------------------------------------
# Step handler functions
# ---------------------------------------------------------------------------

def _apply_cast(df: DataFrame, step: dict, spark=None, udf_url=None) -> DataFrame:
    col_name = step["column"]
    to_type = step.get("to_type", "string")
    out_col = step.get("output_column", col_name)
    return df.withColumn(out_col, F.col(col_name).cast(_spark_type(to_type)))


def _apply_string_op(df: DataFrame, step: dict, spark=None, udf_url=None) -> DataFrame:
    col_name = step["column"]
    op = step["op"]
    out_col = step.get("output_column", col_name)
    params = step.get("params", {})

    if op == "upper":
        return df.withColumn(out_col, F.upper(F.col(col_name)))
    if op == "lower":
        return df.withColumn(out_col, F.lower(F.col(col_name)))
    if op == "trim":
        return df.withColumn(out_col, F.trim(F.col(col_name)))
    if op == "substring":
        start = int(params.get("start", 1))
        length = int(params.get("length", 255))
        return df.withColumn(out_col, F.substring(F.col(col_name), start, length))
    raise ValueError(f"Unknown string_op: {op!r}")


def _apply_math_op(df: DataFrame, step: dict, spark=None, udf_url=None) -> DataFrame:
    expression = step["expression"]
    out_col = step.get("output_column", "math_result")
    return df.withColumn(out_col, F.expr(expression))


def _apply_date_op(df: DataFrame, step: dict, spark=None, udf_url=None) -> DataFrame:
    col_name = step["column"]
    op = step["op"]
    out_col = step.get("output_column", col_name)
    params = step.get("params", {})

    if op == "year":
        return df.withColumn(out_col, F.year(F.col(col_name)))
    if op == "month":
        return df.withColumn(out_col, F.month(F.col(col_name)))
    if op == "day":
        return df.withColumn(out_col, F.dayofmonth(F.col(col_name)))
    if op == "date_add":
        days = int(params.get("days", 0))
        return df.withColumn(out_col, F.date_add(F.col(col_name), days))
    if op == "date_format":
        fmt = params.get("format", "yyyy-MM-dd")
        return df.withColumn(out_col, F.date_format(F.col(col_name), fmt))
    raise ValueError(f"Unknown date_op: {op!r}")


def _apply_json_extract(df: DataFrame, step: dict, spark=None, udf_url=None) -> DataFrame:
    col_name = step["column"]
    json_path = step["json_path"]
    out_col = step.get("output_column", col_name + "_extracted")
    to_type = step.get("to_type", "string")
    extracted = F.get_json_object(F.col(col_name), json_path)
    return df.withColumn(out_col, extracted.cast(_spark_type(to_type)))


def _apply_json_flatten_inline(df: DataFrame, step: dict, spark=None, udf_url=None) -> DataFrame:
    col_name = step["column"]
    json_schema_spec: dict = step.get("json_schema", {})
    output_columns: dict = step.get("output_columns", {})
    keep_original: bool = step.get("keep_original", True)

    # Build struct schema from spec
    fields = [
        StructField(k, _spark_type(v), True)
        for k, v in json_schema_spec.items()
    ]
    struct_type = StructType(fields)

    df = df.withColumn("_json_struct", F.from_json(F.col(col_name), struct_type))
    for src_field, dest_col in output_columns.items():
        df = df.withColumn(dest_col, F.col(f"_json_struct.{src_field}"))
    df = df.drop("_json_struct")

    if not keep_original:
        df = df.drop(col_name)
    return df


def _apply_json_flatten_child(df: DataFrame, step: dict, spark=None, udf_url=None) -> DataFrame:
    """
    Removes (or keeps) the JSON array column from the parent df.
    The child DataFrame is generated inside TransformPipelineExecutor.apply()
    which has access to self.child_tables.
    This handler only modifies the parent.
    """
    col_name = step["column"]
    keep_original: bool = step.get("keep_original", False)
    if not keep_original:
        return df.drop(col_name)
    return df


def _apply_mask(df: DataFrame, step: dict, spark=None, udf_url=None) -> DataFrame:
    col_name = step["column"]
    strategy = step.get("strategy", "last4")
    out_col = step.get("output_column", col_name)

    if strategy == "last4":
        # Replace everything except the last 4 characters with '*'
        col_expr = f"CAST({col_name} AS STRING)"
        masked = F.expr(
            f"CASE WHEN length({col_expr}) > 4 "
            f"THEN concat(lpad('', length({col_expr}) - 4, '*'), "
            f"substring({col_expr}, -4, 4)) "
            f"ELSE {col_expr} END"
        )
        return df.withColumn(out_col, masked)

    if strategy == "hash":
        return df.withColumn(out_col, F.sha2(F.col(col_name).cast("string"), 256))

    raise ValueError(f"Unknown mask strategy: {strategy!r}")


def _apply_expression(df: DataFrame, step: dict, spark=None, udf_url=None) -> DataFrame:
    expression = step["expression"]
    out_col = step.get("output_column", "expr_result")
    language = step.get("language", "spark_sql").lower()

    if language == "sel":
        # Translate SEL → Spark SQL then evaluate (spec §2 Expression DSL)
        from transform.sel_parser import sel_to_sql
        expression = sel_to_sql(expression)

    return df.withColumn(out_col, F.expr(expression))


def _apply_udf(df: DataFrame, step: dict, spark=None, udf_url=None) -> DataFrame:
    """
    Load a UDF from the control-plane registry, register it with Spark, and apply.

    SECURITY NOTE: The UDF code is fetched from an admin-controlled internal API
    (protected by WORKER_SHARED_SECRET).  exec() is intentional here — the code
    originates from the control-plane DB, not from end-user input at runtime.
    """

    function_name = step["function"]
    args = step.get("args", [])
    out_col = step.get("output_column", function_name + "_result")

    if spark is None:
        raise ValueError("spark session is required for 'udf' step")

    if udf_url:
        resp = requests.get(f"{udf_url}/api/v1/udfs/{function_name}", timeout=10)
        resp.raise_for_status()
        udf_def = resp.json()
        code: str = udf_def.get("code", "")
        return_type_str: str = udf_def.get("return_type", "string")
        return_type = _spark_type(return_type_str)

        namespace: dict = {}
        exec(textwrap.dedent(code), namespace)  # nosec B102
        fn = namespace.get(function_name)
        if fn is None:
            raise ValueError(f"Function '{function_name}' not defined in UDF code")

        spark_udf = F.udf(fn, return_type)
        spark.udf.register(function_name, fn, return_type)
        arg_cols = [F.col(a) for a in args]
        return df.withColumn(out_col, spark_udf(*arg_cols))

    # UDF already registered externally — call by name via expr()
    args_str = ", ".join(args)
    return df.withColumn(out_col, F.expr(f"{function_name}({args_str})"))


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class TransformPipelineExecutor:
    """
    Registry-based executor.  ``apply(df)`` iterates the pipeline spec and
    dispatches each step to the matching handler function.

    After apply(), ``self.child_tables`` contains any DataFrames produced by
    ``json_flatten_child`` steps: {child_table_name: DataFrame}.
    """

    STEP_HANDLERS = {
        "cast": _apply_cast,
        "string_op": _apply_string_op,
        "math_op": _apply_math_op,
        "date_op": _apply_date_op,
        "json_extract": _apply_json_extract,
        "json_flatten_inline": _apply_json_flatten_inline,
        "json_flatten_child": _apply_json_flatten_child,
        "mask": _apply_mask,
        "expression": _apply_expression,
        "udf": _apply_udf,
    }

    def __init__(
        self,
        pipeline_spec: dict,
        spark=None,
        udf_registry_url: Optional[str] = None,
    ) -> None:
        self.steps = pipeline_spec.get("transforms", [])
        self.spark = spark
        self.udf_registry_url = udf_registry_url
        self.child_tables: Dict[str, DataFrame] = {}

    def apply(self, df: DataFrame) -> DataFrame:
        """
        Apply all transform steps in order.  Returns the (possibly modified)
        parent DataFrame.  Side-effect: populates ``self.child_tables``.
        """
        self.child_tables = {}

        for step in self.steps:
            step_type = step.get("type")
            if step_type not in self.STEP_HANDLERS:
                raise ValueError(f"Unknown transform step type: {step_type!r}")

            if step_type == "json_flatten_child":
                # Build the child DataFrame before dropping the column
                col_name = step["column"]
                parent_keys = step.get("parent_keys", [])
                child_table_name = step.get("child_table", "child_table")
                output_columns: dict = step.get("output_columns", {})

                array_of_maps = ArrayType(MapType(StringType(), StringType()))
                child_df = df.select(
                    *parent_keys,
                    F.explode(
                        F.from_json(F.col(col_name), array_of_maps)
                    ).alias("_element"),
                )
                for src_field, dest_col in output_columns.items():
                    child_df = child_df.withColumn(dest_col, F.col("_element")[src_field])
                child_df = child_df.drop("_element")
                self.child_tables[child_table_name] = child_df

            df = self.STEP_HANDLERS[step_type](df, step, self.spark, self.udf_registry_url)

        return df
