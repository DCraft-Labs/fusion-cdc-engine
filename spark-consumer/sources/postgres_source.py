"""
PostgresSource — production-grade batch extractor for PostgreSQL → Iceberg / Postgres.

Load strategy:
  ┌───────────────────────────────────────────────────────────────────────────┐
  │ Destination table does NOT exist (or cursor_field is None)               │
  │   → FULL LOAD: SELECT * FROM schema.source_table                         │
  │   → Write: writeTo(dest).createOrReplace()                               │
  ├───────────────────────────────────────────────────────────────────────────┤
  │ Destination table EXISTS + cursor_field configured                        │
  │   → INCREMENTAL: SELECT * WHERE cursor_field >= MAX(cursor_field in dest) │
  │   → Write: staging-merge pattern (delete-by-PK then insert)              │
  └───────────────────────────────────────────────────────────────────────────┘

After fetching:
  - Empty strings normalized to NULL.
  - audit_updated_date (IST) added.
  - All column names lowercased.
  - Schema reconciled against the Iceberg destination.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class PostgresSource:
    """
    Reads a PostgreSQL table and loads it into an Iceberg or Postgres destination.

    Args:
        host:           Postgres host.
        port:           Postgres port (default 5432).
        username:       DB username.
        password:       DB password.
        database:       Database name.
        source_schema:  Schema in the source database (default 'public').
    """

    DRIVER = "org.postgresql.Driver"

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        source_schema: str = "public",
    ) -> None:
        self.host = host
        self.port = int(port)
        self.username = username
        self.password = password
        self.database = database
        self.source_schema = source_schema

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def ingest(
        self,
        spark,
        source_table: str,
        dest_table: str,
        cursor_field: Optional[str],
        primary_key: str,
        catalog: str,
        namespace: str,
        writer_type: str = "iceberg",
        pg_dsn: Optional[str] = None,
        pg_schema: str = "public",
    ) -> None:
        """
        Full ingest cycle: extract → clean → reconcile schema → write.

        Args:
            source_table:  Table name in Postgres to read from.
            dest_table:    Target table name in the destination.
            cursor_field:  Incremental watermark column (e.g. 'updated_at').
                           Pass None to always perform a full refresh.
            primary_key:   Primary key column (used for staging-merge dedup).
            catalog:       Iceberg catalog name (e.g. 'vp_terra').
            namespace:     Iceberg namespace (e.g. 'raw_bank').
            writer_type:   'iceberg' (default) or 'postgres'.
            pg_dsn:        Postgres DSN string (required when writer_type='postgres').
            pg_schema:     Target Postgres schema (default 'public').
        """
        from sources.iceberg_utils import (
            ensure_namespace,
            get_max_watermark,
            reconcile_schema,
            table_exists,
        )
        from writers.batch_writer import IcebergBatchWriter, PostgresBatchWriter

        start = datetime.now()
        logger.info(
            "PostgresSource.ingest: %s.%s.%s → %s.%s.%s [writer=%s]",
            self.database, self.source_schema, source_table,
            catalog, namespace, dest_table, writer_type,
        )

        # 1. Determine load mode
        table_present = table_exists(spark, catalog, namespace, dest_table)
        max_run_date: Optional[str] = None

        if cursor_field and table_present:
            max_run_date = get_max_watermark(
                spark, catalog, namespace, dest_table, cursor_field
            )
            logger.info("Incremental load — watermark: %s", max_run_date)
        else:
            logger.info(
                "Full load (table_present=%s, cursor_field=%s)",
                table_present, cursor_field,
            )

        # 2. Verify source table exists
        if not self._source_table_exists(source_table):
            logger.warning(
                "Source table %s.%s.%s not found — skipping.",
                self.database, self.source_schema, source_table,
            )
            return

        # 3. Extract
        df = self._fetch(spark, source_table, cursor_field, max_run_date)
        count = df.count()
        logger.info(
            "Fetched %d rows from %s.%s.%s",
            count, self.database, self.source_schema, source_table,
        )

        if count == 0:
            logger.info("No new data for %s — skipping write.", dest_table)
            return

        # 4. Clean + audit timestamp + lowercase columns
        df = self._clean(df)

        # 5. Schema reconciliation (only against existing Iceberg tables)
        if writer_type == "iceberg":
            ensure_namespace(spark, catalog, namespace)
            if table_present:
                df = reconcile_schema(spark, df, catalog, namespace, dest_table)

        # 6. Write
        if writer_type == "iceberg":
            IcebergBatchWriter(spark, catalog, namespace, dest_table).write(
                df, primary_key, cursor_field, max_run_date, table_present
            )
        else:
            if not pg_dsn:
                raise ValueError("pg_dsn is required when writer_type='postgres'")
            PostgresBatchWriter(pg_dsn, dest_table, primary_key, pg_schema).write(df)

        elapsed = (datetime.now() - start).total_seconds() / 60
        logger.info("PostgresSource.ingest complete in %.2f min", elapsed)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _source_table_exists(self, table_name: str) -> bool:
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                dbname=self.database,
            )
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_catalog = %s AND table_schema = %s AND table_name = %s",
                    (self.database, self.source_schema, table_name),
                )
                result = cur.fetchone()
            conn.close()
            return bool(result and result[0] > 0)
        except Exception as exc:
            logger.error("Error checking Postgres table existence: %s", exc)
            return False

    def _fetch(
        self,
        spark,
        table_name: str,
        cursor_field: Optional[str],
        max_run_date: Optional[str],
    ):
        url = (
            f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
            f"?options=--search_path%3D{self.source_schema}"
        )

        if cursor_field and max_run_date:
            query = (
                f'SELECT * FROM "{self.database}".{self.source_schema}.{table_name} '
                f"WHERE {cursor_field} >= '{max_run_date}'"
            )
        else:
            query = (
                f'SELECT * FROM "{self.database}".{self.source_schema}.{table_name}'
            )

        logger.debug("Postgres fetch query: %s", query)

        return (
            spark.read.format("jdbc")
            .option("driver", self.DRIVER)
            .option("url", url)
            .option("dbtable", f"({query}) t")
            .option("user", self.username)
            .option("password", self.password)
            .load()
        )

    @staticmethod
    def _clean(df):
        from pyspark.sql.functions import (
            col, expr, from_utc_timestamp, trim, when,
        )

        # Normalize empty strings to NULL
        for column in df.columns:
            df = df.withColumn(
                column, when(trim(col(column)) == "", None).otherwise(col(column))
            )

        # Audit timestamp in IST
        df = df.withColumn(
            "audit_updated_date",
            from_utc_timestamp(expr("current_timestamp()"), "Asia/Kolkata"),
        )

        # Lowercase all column names
        df = df.toDF(*[c.lower() for c in df.columns])

        return df
