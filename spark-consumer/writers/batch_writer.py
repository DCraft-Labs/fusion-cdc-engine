"""
IcebergBatchWriter and PostgresBatchWriter for the fusion-cdc-engine batch ingest pipeline.

NOTE: This is distinct from writers/iceberg_writer.py (which handles CDC streaming
      via foreachBatch / MERGE INTO).  This module handles bulk batch writes using
      the writeTo() API and a staging-merge pattern.

IcebergBatchWriter write strategy:
  ┌──────────────────────────────────────────────────────────────────────────────┐
  │ table_present=True + cursor_field + primary_key                             │
  │   → STAGING MERGE (incremental)                                             │
  │   1. df.writeTo(dest_staging).createOrReplace()                             │
  │   2. DELETE FROM dest WHERE pk IN (SELECT pk FROM dest_staging)             │
  │   3. INSERT INTO dest SELECT * FROM dest_staging                            │
  │   4. DROP TABLE dest_staging                                                │
  ├──────────────────────────────────────────────────────────────────────────────┤
  │ table_present=True + no cursor_field (or max_run_date is None)              │
  │   → FULL REFRESH: df.writeTo(dest).createOrReplace()                        │
  ├──────────────────────────────────────────────────────────────────────────────┤
  │ table_present=False (initial load)                                          │
  │   → CREATE: df.writeTo(dest).createOrReplace()                              │
  └──────────────────────────────────────────────────────────────────────────────┘

PostgresBatchWriter write strategy:
  - Full overwrite via Spark JDBC mode="overwrite".
  - For incremental use-cases, call write_incremental() which does:
      INSERT INTO … ON CONFLICT (pk) DO UPDATE SET …
"""

from __future__ import annotations

import logging
from typing import List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Iceberg batch writer
# ---------------------------------------------------------------------------

class IcebergBatchWriter:
    """Writes a DataFrame to an Apache Iceberg table using the batch writeTo() API."""

    def __init__(
        self,
        spark,
        catalog: str,
        namespace: str,
        table: str,
    ) -> None:
        self.spark = spark
        self.catalog = catalog
        self.namespace = namespace
        self.table = table

    @property
    def full_table_name(self) -> str:
        return f"{self.catalog}.{self.namespace}.{self.table}"

    @property
    def staging_table_name(self) -> str:
        return f"{self.catalog}.{self.namespace}.{self.table}_staging"

    def write(
        self,
        df,
        primary_key: str,
        cursor_field: Optional[str],
        max_run_date: Optional[str],
        table_present: bool,
    ) -> None:
        """
        Choose the correct write strategy and execute it.

        Args:
            df:            DataFrame to write.
            primary_key:   Primary key column name for deduplication.
            cursor_field:  Incremental cursor column name (None = full refresh).
            max_run_date:  High-water mark value (None = no prior data).
            table_present: Whether the destination Iceberg table already exists.
        """
        use_staging_merge = (
            table_present
            and cursor_field is not None
            and primary_key in df.columns
        )

        if use_staging_merge:
            logger.info(
                "STAGING MERGE into %s (pk=%s)", self.full_table_name, primary_key
            )
            self._staging_merge(df, primary_key)
        elif table_present and (cursor_field is None or max_run_date is None):
            logger.info("FULL REFRESH (overwrite) of %s", self.full_table_name)
            self._full_refresh(df)
        else:
            logger.info("INITIAL LOAD (create) %s", self.full_table_name)
            self._initial_load(df)

    # ------------------------------------------------------------------
    # Write strategies
    # ------------------------------------------------------------------

    def _initial_load(self, df) -> None:
        df.writeTo(self.full_table_name).createOrReplace()
        logger.info("Initial load complete: %s (%d rows)", self.full_table_name, df.count())

    def _full_refresh(self, df) -> None:
        df.writeTo(self.full_table_name).createOrReplace()
        logger.info("Full refresh complete: %s (%d rows)", self.full_table_name, df.count())

    def _staging_merge(self, df, primary_key: str) -> None:
        staging = self.staging_table_name
        dest = self.full_table_name

        # Step 1 — write new/updated rows to staging table
        df.writeTo(staging).createOrReplace()
        row_count = df.count()
        logger.info("Staging table %s written (%d rows)", staging, row_count)

        # Step 2 — count affected rows (informational)
        affected = self.spark.sql(
            f"SELECT COUNT(*) FROM {dest} "
            f"WHERE `{primary_key}` IN (SELECT `{primary_key}` FROM {staging})"
        ).collect()[0][0]
        logger.info("Rows to be replaced in %s: %d", dest, affected)

        # Step 3 — delete matching rows from destination
        self.spark.sql(
            f"DELETE FROM {dest} "
            f"WHERE `{primary_key}` IN (SELECT `{primary_key}` FROM {staging})"
        )

        # Step 4 — insert from staging into destination
        cols = ", ".join(f"`{c}`" for c in df.columns)
        self.spark.sql(
            f"INSERT INTO {dest} ({cols}) "
            f"SELECT {cols} FROM {staging}"
        )
        logger.info("Staging merge complete: %d rows inserted into %s", row_count, dest)

        # Step 5 — drop staging table
        self.spark.sql(f"DROP TABLE IF EXISTS {staging}")
        logger.info("Staging table %s dropped", staging)


# ---------------------------------------------------------------------------
# Postgres batch writer
# ---------------------------------------------------------------------------

class PostgresBatchWriter:
    """
    Writes a DataFrame to a Postgres table via psycopg2.

    Supports:
      write()              → full overwrite (DROP & CREATE via Spark JDBC mode=overwrite)
      write_incremental()  → INSERT … ON CONFLICT (pk) DO UPDATE SET …
    """

    def __init__(
        self,
        pg_dsn: str,
        table: str,
        primary_key: str,
        schema: str = "public",
    ) -> None:
        self.pg_dsn = pg_dsn
        self.table = table
        self.primary_key = primary_key
        self.schema = schema
        self._full_table = f"{schema}.{table}"

    def write(self, df) -> None:
        """Full overwrite — recreates the table from the DataFrame (Spark JDBC overwrite)."""
        # Parse DSN → JDBC URL
        jdbc_url, user, password = self._parse_dsn()
        (
            df.write.format("jdbc")
            .option("driver", "org.postgresql.Driver")
            .option("url", jdbc_url)
            .option("dbtable", self._full_table)
            .option("user", user)
            .option("password", password)
            .mode("overwrite")
            .save()
        )
        logger.info("PostgresBatchWriter: wrote %d rows to %s", df.count(), self._full_table)

    def write_incremental(self, df) -> None:
        """
        Upsert rows using INSERT … ON CONFLICT (pk) DO UPDATE SET …
        via psycopg2 (row-by-row, suitable for moderate volumes).
        """
        import psycopg2
        rows = df.collect()
        if not rows:
            return

        columns = df.columns
        set_clause = ", ".join(
            f'"{c}" = EXCLUDED."{c}"' for c in columns if c != self.primary_key
        )
        col_list = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join("%s" for _ in columns)

        sql = (
            f'INSERT INTO {self._full_table} ({col_list}) '
            f'VALUES ({placeholders}) '
            f'ON CONFLICT ("{self.primary_key}") DO UPDATE SET {set_clause}'
        )

        conn = psycopg2.connect(self.pg_dsn)
        try:
            cur = conn.cursor()
            for row in rows:
                cur.execute(sql, [row[c] for c in columns])
            conn.commit()
            cur.close()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        logger.info(
            "PostgresBatchWriter.write_incremental: upserted %d rows to %s",
            len(rows), self._full_table,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_dsn(self):
        """Extract JDBC URL, user, and password from a psycopg2-style DSN string."""
        import re
        host = re.search(r"host=(\S+)", self.pg_dsn)
        port = re.search(r"port=(\d+)", self.pg_dsn)
        dbname = re.search(r"(?:dbname|database)=(\S+)", self.pg_dsn)
        user = re.search(r"user=(\S+)", self.pg_dsn)
        password = re.search(r"password=(\S+)", self.pg_dsn)

        h = host.group(1) if host else "localhost"
        p = port.group(1) if port else "5432"
        db = dbname.group(1) if dbname else "postgres"
        u = user.group(1) if user else ""
        pw = password.group(1) if password else ""

        jdbc_url = f"jdbc:postgresql://{h}:{p}/{db}"
        return jdbc_url, u, pw
