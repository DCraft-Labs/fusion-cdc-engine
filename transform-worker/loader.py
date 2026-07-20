"""
Transform Worker — Task runners for initial loads and CDC event transforms.
"""
from __future__ import annotations

import io
import logging
import time
from typing import TYPE_CHECKING

import psycopg2
import redis

if TYPE_CHECKING:
    from engine import DuckDBTransformEngine

log = logging.getLogger(__name__)


class InitialLoadTask:
    """
    Handles one chunk of an initial 100M-row load:
      1. Fetch rows from source (via control-plane proxy or direct DSN)
      2. Apply all N transform steps via DuckDB engine
      3. Bulk COPY to destination Postgres
      4. Write checkpoint (last_pk processed)
    """

    def __init__(self, engine: "DuckDBTransformEngine", redis_client: redis.Redis):
        self.engine = engine
        self.redis = redis_client

    def run(self, task: dict):
        connection_id = task["connection_id"]
        chunk_seq = task.get("chunk_seq", 0)
        pk_start = task.get("pk_start")
        pk_end = task.get("pk_end")
        steps = task.get("transform_steps", [])

        log.info("InitialLoad connection=%s chunk=%d pk=[%s, %s]",
                 connection_id, chunk_seq, pk_start, pk_end)

        # Fetch source rows via control-plane proxy (avoids worker needing raw source DSN)
        rows = self._fetch_rows(connection_id, pk_start, pk_end)
        if not rows:
            log.info("No rows in range — chunk %d complete", chunk_seq)
            self._mark_chunk_done(connection_id, chunk_seq, 0)
            return

        # Apply transforms
        if steps:
            transformed, child_tables = self.engine.execute_pipeline(rows, steps)
        else:
            transformed, child_tables = rows, {}

        # Write to destination
        dest_dsn = self._get_dest_dsn(connection_id)
        schema = task.get("dest_schema", "dw")
        table = task.get("dest_table", "data")
        rows_written = self._copy_to_postgres(transformed, dest_dsn, schema, table)

        # Write child tables if json_flatten_child produced any
        for child_name, child_rows in child_tables.items():
            if child_rows:
                self._copy_to_postgres(child_rows, dest_dsn, schema, child_name)

        # Checkpoint
        self._mark_chunk_done(connection_id, chunk_seq, rows_written, last_pk=pk_end)
        log.info("InitialLoad chunk=%d done — %d rows written", chunk_seq, rows_written)

    def _fetch_rows(self, connection_id: str, pk_start, pk_end) -> list[dict]:
        """Fetch rows via control-plane data-proxy endpoint."""
        import requests
        url = f"{self.engine.control_plane_url}/internal/data-proxy/fetch"
        resp = requests.post(url, json={
            "connection_id": connection_id,
            "pk_start": pk_start,
            "pk_end": pk_end,
        }, timeout=300)
        resp.raise_for_status()
        return resp.json().get("rows", [])

    def _get_dest_dsn(self, connection_id: str) -> str:
        import requests
        url = f"{self.engine.control_plane_url}/internal/connections/{connection_id}/dest-dsn"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()["dsn"]

    def _copy_to_postgres(self, rows: list[dict], dsn: str, schema: str, table: str) -> int:
        if not rows:
            return 0
        columns = list(rows[0].keys())
        buf = io.StringIO()
        for row in rows:
            line = "\t".join("\\N" if v is None else str(v).replace("\t", " ") for v in row.values())
            buf.write(line + "\n")
        buf.seek(0)

        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                cols_sql = ", ".join(f"{c} TEXT" for c in columns)
                cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({cols_sql})")
                cur.copy_from(buf, f"{schema}.{table}", columns=columns, null="\\N")
                conn.commit()
        return len(rows)

    def _mark_chunk_done(self, connection_id: str, chunk_seq: int, rows_written: int, last_pk=None):
        import requests
        requests.post(
            f"{self.engine.control_plane_url}/internal/load-checkpoints",
            json={
                "connection_id": connection_id,
                "chunk_seq": chunk_seq,
                "rows_written": rows_written,
                "last_pk": last_pk,
                "state": "done",
            },
            timeout=10,
        )


class CDCTransformTask:
    """
    Handles a batch of CDC events that have a transform pipeline:
      1. Receive event batch from Redis / Kafka
      2. Apply transform pipeline via DuckDB
      3. Upsert to destination Postgres
    """

    def __init__(self, engine: "DuckDBTransformEngine"):
        self.engine = engine

    def run(self, task: dict):
        connection_id = task["connection_id"]
        events = task.get("events", [])   # list of CDC row dicts
        steps = task.get("transform_steps", [])
        dest_dsn = task.get("dest_dsn", "")
        schema = task.get("dest_schema", "dw")
        table = task.get("dest_table", "data")
        pk_col = task.get("primary_key", "id")

        log.info("CDCTransform connection=%s events=%d", connection_id, len(events))

        if not events:
            return

        # Separate INSERT/UPDATE rows from DELETEs
        to_upsert = [e["after"] for e in events if e.get("op") in ("INSERT", "UPDATE") and e.get("after")]
        to_delete_pks = [e["before"][pk_col] for e in events if e.get("op") == "DELETE" and e.get("before")]

        if to_upsert and steps:
            to_upsert, _ = self.engine.execute_pipeline(to_upsert, steps)

        if dest_dsn:
            self._upsert(to_upsert, to_delete_pks, dest_dsn, schema, table, pk_col)

    def _upsert(self, rows: list[dict], delete_pks: list,
                dsn: str, schema: str, table: str, pk_col: str):
        if not rows and not delete_pks:
            return

        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                if rows:
                    columns = list(rows[0].keys())
                    non_pk = [c for c in columns if c != pk_col]
                    placeholders = ", ".join(["%s"] * len(columns))
                    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in non_pk)
                    sql = (
                        f"INSERT INTO {schema}.{table} ({', '.join(columns)}) "
                        f"VALUES ({placeholders}) "
                        f"ON CONFLICT ({pk_col}) DO UPDATE SET {update_clause}"
                    )
                    cur.executemany(sql, [tuple(r.values()) for r in rows])

                if delete_pks:
                    cur.execute(
                        f"DELETE FROM {schema}.{table} WHERE {pk_col} = ANY(%s)",
                        (delete_pks,),
                    )
                conn.commit()
