"""
IcebergWriter — writes CDC event batches to Apache Iceberg tables via MERGE INTO.

Supports catalogs: Nessie (default), Glue, Hive.

MERGE INTO logic:
    WHEN MATCHED AND s.op = 'u' THEN UPDATE SET *
    WHEN MATCHED AND s.op = 'd' THEN DELETE
    WHEN NOT MATCHED AND s.op = 'c' THEN INSERT *

Auto-creates the Iceberg table from the first batch schema if it doesn't exist.

Usage (Structured Streaming foreachBatch):
    writer = IcebergWriter(spark, "nessie", "finance", "orders", pk_columns=["id"])
    query = df.writeStream.foreachBatch(writer.write_batch).start()
"""
from __future__ import annotations

import logging
from typing import List

logger = logging.getLogger(__name__)


class IcebergWriter:
    """Writes CDC batches to an Iceberg table using Spark SQL MERGE INTO."""

    SUPPORTED_CATALOGS = ("nessie", "glue", "hive")

    def __init__(
        self,
        spark,
        catalog: str,
        namespace: str,
        table: str,
        pk_columns: List[str],
        catalog_type: str = "nessie",
        partition_spec: dict | None = None,
    ) -> None:
        """
        :param partition_spec: Optional per-connection Iceberg partition strategy.
            Spec §5 (P5-9): sourced from connection_config['iceberg_partition_spec'].
            Examples:
              {"type": "day",      "column": "ts"}          → PARTITIONED BY (day(ts))
              {"type": "month",    "column": "created_at"}  → PARTITIONED BY (month(created_at))
              {"type": "year",     "column": "event_date"}  → PARTITIONED BY (year(event_date))
              {"type": "hour",     "column": "ts"}          → PARTITIONED BY (hour(ts))
              {"type": "bucket",   "column": "id", "n": 16} → PARTITIONED BY (bucket(16, id))
              {"type": "truncate", "column": "id", "n": 4}  → PARTITIONED BY (truncate(4, id))
              {"type": "identity", "column": "tenant_id"}   → PARTITIONED BY (tenant_id)
            If None, no partitioning is used.
        """
        self.spark = spark
        self.catalog = catalog
        self.namespace = namespace
        self.table = table
        self.pk_columns = list(pk_columns)
        self.catalog_type = catalog_type.lower()
        self.partition_spec = partition_spec or {}
        self._table_created = False

    @property
    def full_table_name(self) -> str:
        return f"{self.catalog}.{self.namespace}.{self.table}"

    # ------------------------------------------------------------------
    # foreachBatch entry point
    # ------------------------------------------------------------------

    def write_batch(self, batch_df, batch_id: int) -> None:
        """Called by Spark foreachBatch.  MERGE INTO the Iceberg table."""
        if batch_df.rdd.isEmpty():
            logger.debug("Batch %d is empty — skipping Iceberg write", batch_id)
            return

        # Auto-create table on first batch
        if not self._table_created:
            self._ensure_table(batch_df)
            self._table_created = True

        batch_df.createOrReplaceTempView("cdc_updates")
        merge_sql = self._build_merge_sql()
        logger.debug("Executing MERGE for batch %d on %s", batch_id, self.full_table_name)
        self.spark.sql(merge_sql)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_table(self, df) -> None:
        """Create the Iceberg table if it doesn't exist yet."""
        # Build CREATE TABLE … AS SELECT with no rows to get schema
        # Real Iceberg: CREATE TABLE IF NOT EXISTS ... USING iceberg PARTITIONED BY ...
        try:
            self.spark.sql(f"DESCRIBE TABLE {self.full_table_name}")
            logger.debug("Table %s already exists", self.full_table_name)
        except Exception:
            # Table does not exist — create from the DataFrame schema
            logger.info("Auto-creating Iceberg table %s", self.full_table_name)
            create_sql = self._build_create_sql(df)
            self.spark.sql(create_sql)

    def _build_create_sql(self, df) -> str:
        cols = ", ".join(f"`{c}` STRING" for c in df.columns)
        partition_clause = self._build_partition_clause()
        return (
            f"CREATE TABLE IF NOT EXISTS {self.full_table_name} "
            f"({cols}) "
            f"USING iceberg"
            + (f" PARTITIONED BY ({partition_clause})" if partition_clause else "")
        )

    def _build_partition_clause(self) -> str:
        """
        Spec §5 (P5-9): translate iceberg_partition_spec dict → Iceberg DDL PARTITIONED BY clause.
        Returns empty string when no spec is configured.
        """
        spec = self.partition_spec
        if not spec:
            return ""
        ptype = spec.get("type", "identity").lower()
        col = spec.get("column", "")
        if not col:
            return ""
        if ptype == "identity":
            return f"`{col}`"
        if ptype in ("day", "month", "year", "hour"):
            return f"{ptype}(`{col}`)"
        if ptype == "bucket":
            n = int(spec.get("n", 16))
            return f"bucket({n}, `{col}`)"
        if ptype == "truncate":
            n = int(spec.get("n", 4))
            return f"truncate({n}, `{col}`)"
        logger.warning("Unknown iceberg partition type %r — no partitioning applied", ptype)
        return ""

    def _build_merge_sql(self) -> str:
        pk_join = " AND ".join(
            f"t.`{pk}` = s.`{pk}`" for pk in self.pk_columns
        )
        return (
            f"MERGE INTO {self.full_table_name} AS t "
            f"USING cdc_updates AS s "
            f"ON {pk_join} "
            f"WHEN MATCHED AND s.op = 'u' THEN UPDATE SET * "
            f"WHEN MATCHED AND s.op = 'd' THEN DELETE "
            f"WHEN NOT MATCHED AND s.op = 'c' THEN INSERT *"
        )
