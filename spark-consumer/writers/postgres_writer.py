"""
PostgresWriter — upserts CDC events into a Postgres data warehouse table.

Supports:
  • SCD Type 1: INSERT … ON CONFLICT (pk) DO UPDATE SET …
  • DELETE SCD1:  DELETE FROM table WHERE pk = ?
  • DELETE SCD2:  UPDATE old row valid_to=now(), set is_deleted=true (no hard delete)
  • SCD Type 2:  INSERT new row valid_from=now(), UPDATE old row valid_to=now()

Usage (Structured Streaming foreachBatch):
    writer = PostgresWriter(pg_dsn, table="orders", pk_columns=["id"])
    query = df.writeStream.foreachBatch(writer.upsert_batch).start()
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List, Optional

import psycopg2

logger = logging.getLogger(__name__)


class PostgresWriter:
    """Writes CDC event batches to a Postgres DW table via psycopg2."""

    def __init__(
        self,
        pg_dsn: str,
        table: str,
        pk_columns: List[str],
        schema: str = "public",
        scd2_mode: bool = False,
    ) -> None:
        self.pg_dsn = pg_dsn
        self.table = table
        self.pk_columns = list(pk_columns)
        self.schema = schema
        self.scd2_mode = scd2_mode
        self._full_table = f"{schema}.{table}"

    # ------------------------------------------------------------------
    # foreachBatch entry point
    # ------------------------------------------------------------------

    def upsert_batch(self, batch_df, batch_id: int) -> None:
        """Called by Spark foreachBatch.  Writes all rows in the batch."""
        rows = batch_df.collect()
        if not rows:
            logger.debug("Batch %d is empty — skipping", batch_id)
            return

        # Derive non-pk data columns (exclude CDC meta columns and op)
        all_cols = [c for c in batch_df.columns if c not in ("op",)]
        data_cols = [c for c in all_cols if c not in ("event_id", "lsn", "ts_ms", "metadata",
                                                        "tenant_id", "bank_id", "source_id",
                                                        "schema_name", "table_name", "before", "after")]

        conn = psycopg2.connect(self.pg_dsn)
        try:
            cur = conn.cursor()
            try:
                for row in rows:
                    r = row.asDict()
                    op = r.get("op", "u")
                    if op == "d":
                        if self.scd2_mode:
                            self._scd2_delete(cur, r)
                        else:
                            self._delete(cur, r)
                    elif self.scd2_mode:
                        self._scd2_upsert(cur, r, data_cols)
                    else:
                        self._upsert(cur, r, data_cols)
                conn.commit()
                logger.debug("Batch %d committed (%d rows)", batch_id, len(rows))
            finally:
                cur.close()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Internal write helpers
    # ------------------------------------------------------------------

    def _upsert(self, cur, row: dict, data_cols: List[str]) -> None:
        """INSERT … ON CONFLICT (pks) DO UPDATE SET …"""
        cols = [c for c in data_cols if c in row]
        if not cols:
            return

        placeholders = ", ".join(["%s"] * len(cols))
        col_list = ", ".join(cols)
        pk_list = ", ".join(self.pk_columns)
        update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in self.pk_columns)

        if not update_set:
            # All columns are PKs — just INSERT OR IGNORE
            sql = (
                f"INSERT INTO {self._full_table} ({col_list}) "
                f"VALUES ({placeholders}) "
                f"ON CONFLICT ({pk_list}) DO NOTHING"
            )
        else:
            sql = (
                f"INSERT INTO {self._full_table} ({col_list}) "
                f"VALUES ({placeholders}) "
                f"ON CONFLICT ({pk_list}) DO UPDATE SET {update_set}"
            )
        values = [row.get(c) for c in cols]
        cur.execute(sql, values)

    def _delete(self, cur, row: dict) -> None:
        """DELETE FROM table WHERE pk1 = %s AND pk2 = %s … (SCD Type 1 only)"""
        conditions = " AND ".join(f"{pk} = %s" for pk in self.pk_columns)
        sql = f"DELETE FROM {self._full_table} WHERE {conditions}"
        values = [row.get(pk) for pk in self.pk_columns]
        cur.execute(sql, values)

    def _scd2_delete(self, cur, row: dict) -> None:
        """
        SCD Type 2 delete — spec §5:
        'A delete closes the validity range of a record and optionally marks it as deleted.'
        Does NOT physically delete the row; instead closes valid_to and sets is_deleted=true.
        """
        now = datetime.now(timezone.utc).isoformat()
        pk_conditions = " AND ".join(f"{pk} = %s" for pk in self.pk_columns)
        pk_values = [row.get(pk) for pk in self.pk_columns]
        sql = (
            f"UPDATE {self._full_table} "
            f"SET valid_to = %s, is_deleted = true "
            f"WHERE {pk_conditions} AND valid_to IS NULL"
        )
        cur.execute(sql, [now] + pk_values)

    def _scd2_upsert(self, cur, row: dict, data_cols: List[str]) -> None:
        """
        SCD Type 2:
          1. UPDATE current row → set valid_to = now()
          2. INSERT new row    → set valid_from = now(), valid_to = NULL
        """
        now = datetime.now(timezone.utc).isoformat()
        pk_conditions = " AND ".join(f"{pk} = %s" for pk in self.pk_columns)
        pk_values = [row.get(pk) for pk in self.pk_columns]

        # Close the old row
        close_sql = (
            f"UPDATE {self._full_table} "
            f"SET valid_to = %s "
            f"WHERE {pk_conditions} AND valid_to IS NULL"
        )
        cur.execute(close_sql, [now] + pk_values)

        # Insert new row
        cols = [c for c in data_cols if c in row] + ["valid_from"]
        placeholders = ", ".join(["%s"] * len(cols))
        col_list = ", ".join(cols)
        values = [row.get(c) for c in data_cols if c in row] + [now]
        insert_sql = (
            f"INSERT INTO {self._full_table} ({col_list}) VALUES ({placeholders})"
        )
        cur.execute(insert_sql, values)
