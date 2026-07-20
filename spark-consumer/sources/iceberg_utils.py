"""
Iceberg table helpers for the fusion-cdc-engine batch ingest pipeline.

Functions:
  ensure_namespace       — create namespace if missing
  table_exists           — check if a table exists in catalog.namespace
  get_max_watermark      — MAX(cursor_field) high-water mark query
  get_columns            — column list from DESCRIBE TABLE
  add_column             — ALTER TABLE ADD COLUMN IF NOT EXISTS
  reconcile_schema       — align DataFrame schema with destination Iceberg table
"""

from __future__ import annotations

import logging
from typing import List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Namespace helpers
# ---------------------------------------------------------------------------

def ensure_namespace(spark, catalog: str, namespace: str) -> None:
    """Create the Iceberg namespace if it does not already exist."""
    try:
        existing = [
            row[0]
            for row in spark.sql(f"SHOW NAMESPACES IN {catalog}").collect()
        ]
        if namespace not in existing:
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
            logger.info("Created namespace %s.%s", catalog, namespace)
    except Exception as exc:
        logger.warning("Could not ensure namespace %s.%s: %s", catalog, namespace, exc)


# ---------------------------------------------------------------------------
# Table helpers
# ---------------------------------------------------------------------------

def table_exists(spark, catalog: str, namespace: str, table_name: str) -> bool:
    """Return True if the Iceberg table exists in catalog.namespace."""
    try:
        tables = [
            row.tableName
            for row in spark.sql(f"SHOW TABLES IN {catalog}.{namespace}").collect()
        ]
        return table_name in tables
    except Exception:
        return False


def get_max_watermark(
    spark,
    catalog: str,
    namespace: str,
    table_name: str,
    cursor_field: str,
) -> Optional[str]:
    """
    Return MAX(cursor_field) from the Iceberg table as a string,
    or None if the table is empty or the column contains only NULLs.
    """
    try:
        result = spark.sql(
            f"SELECT MAX(`{cursor_field}`) AS max_val "
            f"FROM {catalog}.{namespace}.`{table_name}`"
        ).collect()
        val = result[0]["max_val"] if result else None
        return str(val) if val is not None else None
    except Exception as exc:
        logger.warning(
            "Could not query max watermark for %s.%s.%s[%s]: %s",
            catalog, namespace, table_name, cursor_field, exc,
        )
        return None


def get_columns(
    spark, catalog: str, namespace: str, table_name: str
) -> List[str]:
    """Return ordered column names from DESCRIBE TABLE (excludes partition metadata rows)."""
    rows = spark.sql(
        f"DESCRIBE TABLE {catalog}.{namespace}.`{table_name}`"
    ).collect()
    return [
        row["col_name"]
        for row in rows
        if row["col_name"] and not row["col_name"].startswith("#")
    ]


def add_column(
    spark,
    catalog: str,
    namespace: str,
    table_name: str,
    column_name: str,
    data_type: str,
) -> None:
    """Add a new column to an existing Iceberg table (no-op if it already exists)."""
    sql = (
        f"ALTER TABLE {catalog}.{namespace}.`{table_name}` "
        f"ADD COLUMN IF NOT EXISTS `{column_name}` {data_type}"
    )
    spark.sql(sql)
    logger.info(
        "Added column `%s` %s to %s.%s.%s",
        column_name, data_type, catalog, namespace, table_name,
    )


# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------

def _spark_type_to_iceberg_ddl(spark_type) -> str:
    """Map a Spark DataType (or its string repr) to an Iceberg-compatible DDL type."""
    from pyspark.sql.types import (
        BooleanType, DateType, DecimalType, DoubleType,
        FloatType, IntegerType, LongType, ShortType,
        StringType, TimestampType,
    )

    type_map = {
        StringType: "STRING",
        IntegerType: "INT",
        ShortType: "SMALLINT",
        LongType: "BIGINT",
        DoubleType: "DOUBLE",
        FloatType: "FLOAT",
        TimestampType: "TIMESTAMP",
        BooleanType: "BOOLEAN",
        DateType: "DATE",
        DecimalType: "DECIMAL(38,10)",
    }
    for spark_cls, ddl in type_map.items():
        if isinstance(spark_type, spark_cls):
            return ddl
    return "STRING"


# ---------------------------------------------------------------------------
# Schema reconciliation
# ---------------------------------------------------------------------------

def reconcile_schema(
    spark,
    df,
    catalog: str,
    namespace: str,
    table_name: str,
):
    """
    Align the incoming DataFrame schema with the existing Iceberg destination table.

    Steps:
      1. For every column in df that is NOT in the destination → ALTER TABLE ADD COLUMN.
      2. For every column in the destination that is NOT in df → add NULL column to df.
      3. Reorder df columns to match the destination column order.
      4. Cast any void-typed columns to the Iceberg-declared type.

    Returns the aligned DataFrame.
    """
    from pyspark.sql.functions import lit

    dest_cols = get_columns(spark, catalog, namespace, table_name)
    src_cols = list(df.columns)

    # Step 1 — add new source columns to Iceberg destination
    for col in src_cols:
        if col not in dest_cols:
            iceberg_type = _spark_type_to_iceberg_ddl(df.schema[col].dataType)
            add_column(spark, catalog, namespace, table_name, col, iceberg_type)

    # Refresh destination column list after potential ALTER
    dest_cols = get_columns(spark, catalog, namespace, table_name)

    # Step 2 — pad df with NULL for columns that exist in destination but not in source
    for col in dest_cols:
        if col not in df.columns:
            df = df.withColumn(col, lit(None))

    # Step 3 — reorder df to match destination column order
    df = df.select(*dest_cols)

    # Step 4 — cast void-typed columns to the Iceberg-declared type
    void_cols = [c for c, dtype in df.dtypes if dtype == "void"]
    if void_cols:
        schema_rows = spark.sql(
            f"DESCRIBE TABLE {catalog}.{namespace}.`{table_name}`"
        ).collect()
        iceberg_types = {row["col_name"]: row["data_type"] for row in schema_rows}
        for col in void_cols:
            target_type = iceberg_types.get(col, "string")
            df = df.withColumn(col, df[col].cast(target_type))

    return df
