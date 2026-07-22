"""
Transform Worker — Task runners for initial loads and CDC event transforms.
"""
from __future__ import annotations

import io
import logging
import os
import time
from typing import TYPE_CHECKING

import psycopg2
import redis

if TYPE_CHECKING:
    from engine import DuckDBTransformEngine

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Destination DSN builders — derive a SQLAlchemy-style DSN from a destination
# block produced by the control-plane transform-route endpoint. The block
# shape is:
#   {"connector_type": "postgresql" | "mysql" | "mongodb" | "iceberg" | ...,
#    "connection_config": {"host": ..., "port": ..., "database_name": ...,
#                          "username": ..., "password": <decrypted plaintext>}}
#
# Each builder returns "" when a required field is missing so the caller can
# log + drop the batch instead of raising. The dispatcher returns "" for
# unknown types and for "iceberg" (which is handled by a separate writer).
# ---------------------------------------------------------------------------

def _pg_dsn_from_dest(dest: dict) -> str:
    """Build a PostgreSQL DSN: postgresql://{user}:{password}@{host}:{port}/{database}."""
    cfg = (dest.get("connection_config") or dest.get("config") or {})
    host = cfg.get("host") or ""
    port = cfg.get("port") or 5432
    database = (cfg.get("database_name") or cfg.get("database")
                or cfg.get("dbname") or "")
    user = cfg.get("username") or cfg.get("user") or ""
    password = cfg.get("password") or ""
    if not host or not database or not user:
        return ""
    from urllib.parse import quote_plus
    return (
        f"postgresql://{quote_plus(user)}:{quote_plus(password)}@"
        f"{host}:{port}/{quote_plus(database)}"
    )


def _mysql_dsn_from_dest(dest: dict) -> str:
    """Build a MySQL DSN: mysql+pymysql://{user}:{password}@{host}:{port}/{database}."""
    cfg = (dest.get("connection_config") or dest.get("config") or {})
    host = cfg.get("host") or ""
    port = cfg.get("port") or 3306
    database = (cfg.get("database_name") or cfg.get("database")
                or cfg.get("dbname") or "")
    user = cfg.get("username") or cfg.get("user") or ""
    password = cfg.get("password") or ""
    if not host or not database or not user:
        return ""
    from urllib.parse import quote_plus
    return (
        f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}@"
        f"{host}:{port}/{quote_plus(database)}"
    )


def _mongo_dsn_from_dest(dest: dict) -> str:
    """Build a MongoDB URI: mongodb://{user}:{password}@{host}:{port}/{database}?authSource=admin.

    Mirrors the format already used by ``cdc_consumer._do_initial_load_mongodb``
    so the destination side stays consistent with the source side. Returns ""
    when host is missing.
    """
    cfg = (dest.get("connection_config") or dest.get("config") or {})
    host = cfg.get("host") or ""
    port = cfg.get("port") or 27017
    database = (cfg.get("database_name") or cfg.get("database") or "")
    user = cfg.get("username") or cfg.get("user") or ""
    password = cfg.get("password") or ""
    auth_source = (cfg.get("auth_source") if isinstance(cfg.get("auth_source"), str)
                   else "admin") or "admin"
    if not host:
        return ""
    from urllib.parse import quote_plus
    path = f"/{quote_plus(database)}" if database else "/"
    if user and password:
        return (
            f"mongodb://{quote_plus(user)}:{quote_plus(password)}@"
            f"{host}:{port}{path}?authSource={auth_source}"
        )
    return f"mongodb://{host}:{port}{path}?authSource={auth_source}"


def _dest_dsn_from_dest(dest: dict) -> str:
    """Dispatch on destination connector_type and return the right DSN.

    Returns "" for unknown types and for "iceberg" (the Iceberg writer is a
    separate code path that does not use a SQL DSN). Callers must treat an
    empty string as "cannot route this batch" and log + drop.
    """
    ctype = (dest.get("connector_type") or "").lower()
    if ctype in ("postgres", "postgresql"):
        return _pg_dsn_from_dest(dest)
    if ctype == "mysql":
        return _mysql_dsn_from_dest(dest)
    if ctype == "mongodb":
        return _mongo_dsn_from_dest(dest)
    # iceberg / unknown → no SQL DSN
    return ""


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
        source = task.get("source") or {}
        source_schema = task.get("source_schema") or ""
        source_table = task.get("source_table") or ""
        stream_id = task.get("stream_id")

        log.info("InitialLoad connection=%s chunk=%d pk=[%s, %s] table=%s.%s",
                 connection_id, chunk_seq, pk_start, pk_end, source_schema, source_table)

        # Fetch source rows directly from the source DB (the task payload
        # includes the source block with a decrypted plaintext password, so
        # the worker does not need to proxy through the control-plane). The
        # previous implementation called a non-existent
        # /internal/data-proxy/fetch endpoint and 404'd on every chunk — see
        # Gap 3 in the v1.2.16 release notes.
        rows = self._fetch_rows(source, source_schema, source_table, pk_start, pk_end)
        if not rows:
            log.info("No rows in range — chunk %d complete", chunk_seq)
            self._mark_chunk_done(connection_id, stream_id, source_table, chunk_seq, 0)
            return

        # Apply transforms
        if steps:
            transformed, child_tables = self.engine.execute_pipeline(rows, steps)
        else:
            transformed, child_tables = rows, {}

        # Write to destination — route by connector_type
        dest = task.get("destination") or {}
        connector_type = dest.get("connector_type") or task.get("dest_connector_type", "postgres")
        schema = task.get("dest_schema", "dw")
        table = task.get("dest_table", "data")

        if connector_type == "iceberg":
            rows_written = self._write_to_iceberg(transformed, dest, table)
            for child_name, child_rows in child_tables.items():
                if child_rows:
                    self._write_to_iceberg(child_rows, dest, child_name)
        else:
            # Derive the destination DSN from the destination block included
            # in the task payload (mirrors CDCTransformTask.run in v1.2.13).
            # The control-plane transform-route endpoint populates
            # connection_config.password with the decrypted plaintext, so the
            # worker never needs the Fernet key or a separate /dest-dsn call
            # (the previous implementation called a non-existent
            # /internal/connections/{id}/dest-dsn endpoint and 404'd on every
            # initial-load chunk — see Gap 1 in the v1.2.14 release notes).
            dest_dsn = _dest_dsn_from_dest(dest)
            if not dest_dsn:
                log.error(
                    "InitialLoad connection=%s chunk=%d cannot derive dest_dsn for "
                    "connector_type=%s — destination block missing/incomplete. "
                    "Dropping %d rows.",
                    connection_id, chunk_seq, connector_type, len(transformed),
                )
                self._mark_chunk_done(connection_id, stream_id, source_table, chunk_seq, 0, last_pk=pk_end)
                return
            rows_written = self._copy_to_postgres(transformed, dest_dsn, schema, table)
            for child_name, child_rows in child_tables.items():
                if child_rows:
                    self._copy_to_postgres(child_rows, dest_dsn, schema, child_name)

        # Checkpoint
        self._mark_chunk_done(connection_id, stream_id, source_table, chunk_seq, rows_written, last_pk=pk_end)
        log.info("InitialLoad chunk=%d done — %d rows written", chunk_seq, rows_written)

    def _write_to_iceberg(self, rows: list[dict], dest: dict, table_name: str) -> int:
        """Write rows to Iceberg via PyIceberg (DuckDB lake path)."""
        from iceberg_writer import IcebergWriter
        dest_config = dest.get("connection_config") or dest.get("config") or dest
        writer = IcebergWriter(dest_config)
        return writer.write_batch(rows, table_name=table_name)

    def _fetch_rows(self, source: dict, schema_name: str, table_name: str,
                    pk_start=None, pk_end=None) -> list[dict]:
        """Fetch rows directly from the source DB using the source block in
        the task payload.

        The source block shape:
          {"connector_type": "postgres"|"mysql"|"mongodb",
           "host": ..., "port": ..., "database_name": ..., "username": ...,
           "password": <decrypted plaintext>, "config": {...}}

        For Postgres/MySQL we issue a parameterised SELECT against
        ``{schema}.{table}`` (or just ``{table}`` when schema is empty). For
        MongoDB we cursor the collection and project documents to plain dicts.

        ``pk_start`` / ``pk_end`` are optional PK bounds. When both are None
        the entire table is fetched (used by the single-chunk initial-load
        producer in v1.2.16). Returns ``[]`` when the table is empty or when
        the source block is incomplete.
        """
        if not source or not table_name:
            return []

        ctype = (source.get("connector_type") or "").lower()
        host = source.get("host") or ""
        port = source.get("port")
        database = source.get("database_name") or source.get("database") or ""
        user = source.get("username") or source.get("user") or ""
        password = source.get("password") or ""
        cfg = source.get("config") or {}

        if not host or not database:
            log.error("_fetch_rows: source block missing host/database — cannot fetch")
            return []

        try:
            if ctype in ("postgres", "postgresql"):
                return self._fetch_pg(host, port or 5432, database, user, password,
                                      schema_name, table_name, pk_start, pk_end)
            if ctype == "mysql":
                return self._fetch_mysql(host, port or 3306, database, user, password,
                                          schema_name, table_name, pk_start, pk_end)
            if ctype == "mongodb":
                return self._fetch_mongo(host, port or 27017, database, user, password,
                                         cfg, table_name)
            log.error("_fetch_rows: unsupported source connector_type=%s", ctype)
            return []
        except Exception:
            log.exception("_fetch_rows: failed to fetch from %s.%s on %s",
                         schema_name, table_name, ctype)
            return []

    def _fetch_pg(self, host, port, database, user, password,
                  schema_name, table_name, pk_start, pk_end) -> list[dict]:
        import psycopg2
        import psycopg2.extras
        qualified = f"{schema_name}.{table_name}" if schema_name else table_name
        # v1.2.16 producer enqueues a single chunk per stream with pk bounds
        # unset, so the entire table is fetched in one SELECT. Chunked PK
        # ranges are a future enhancement; for now we keep the signature but
        # ignore the bounds.
        sql = f"SELECT * FROM {qualified}"
        with psycopg2.connect(host=host, port=port, dbname=database,
                              user=user, password=password,
                              connect_timeout=10) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                return [dict(r) for r in cur.fetchall()]

    def _fetch_mysql(self, host, port, database, user, password,
                     schema_name, table_name, pk_start, pk_end) -> list[dict]:
        import pymysql
        import pymysql.cursors
        qualified = f"`{schema_name}`.`{table_name}`" if schema_name else f"`{table_name}`"
        sql = f"SELECT * FROM {qualified}"
        with pymysql.connect(host=host, port=int(port), database=database,
                             user=user, password=password,
                             cursorclass=pymysql.cursors.DictCursor,
                             connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return list(cur.fetchall())

    def _fetch_mongo(self, host, port, database, user, password,
                     cfg, collection_name) -> list[dict]:
        from urllib.parse import quote_plus
        from pymongo import MongoClient
        auth_source = (cfg.get("auth_source") if isinstance(cfg, dict) else None) or "admin"
        if user and password:
            uri = (f"mongodb://{quote_plus(user)}:{quote_plus(password)}@"
                   f"{host}:{port}/{database}?authSource={auth_source}")
        else:
            uri = f"mongodb://{host}:{port}/{database}?authSource={auth_source}"
        client = MongoClient(uri, serverSelectionTimeoutMS=10000)
        try:
            db = client[database]
            docs = list(db[collection_name].find(no_cursor_timeout=True))
            # Convert ObjectId / datetime to serialisable forms
            out: list[dict] = []
            for d in docs:
                row = {k: (str(v) if k == "_id" else v) for k, v in d.items()}
                out.append(row)
            return out
        finally:
            client.close()

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

    def _mark_chunk_done(self, connection_id: str, stream_id, source_table: str,
                         chunk_seq: int, rows_written: int, last_pk=None):
        """Report chunk progress to the control-plane /internal/load-checkpoints
        endpoint (added in v1.2.16), which upserts into initial_load_checkpoints.

        The previous implementation called a non-existent endpoint and 404'd
        silently on every chunk. We now pass stream_id + source_table so the
        control-plane can upsert by (connection_id, stream_id).
        """
        import requests
        try:
            requests.post(
                f"{self.engine.control_plane_url}/internal/load-checkpoints",
                json={
                    "connection_id": connection_id,
                    "stream_id": stream_id,
                    "source_table": source_table,
                    "chunk_seq": chunk_seq,
                    "rows_written": rows_written,
                    "last_pk": last_pk,
                    "state": "done",
                },
                headers={"X-Worker-Token": os.environ.get("WORKER_SHARED_SECRET", "")},
                timeout=10,
            )
        except Exception:
            log.warning("_mark_chunk_done: failed to report checkpoint for connection=%s chunk=%s",
                        connection_id, chunk_seq, exc_info=True)


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
        dest = task.get("destination") or {}
        connector_type = dest.get("connector_type") or task.get("dest_connector_type", "postgres")
        schema = task.get("dest_schema", "dw")
        table = task.get("dest_table", "data")
        pk_col = task.get("primary_key", "id")

        # Derive the destination DSN. Prefer an explicit dest_dsn on the task
        # (legacy path); otherwise build it from the destination block's
        # connection_config via the type-aware dispatcher — the control-plane
        # transform-route endpoint populates connection_config.password with
        # the decrypted plaintext so the worker can build a usable DSN for
        # Postgres / MySQL / MongoDB without the Fernet key. Without this,
        # CDC silently no-ops because dest_dsn is empty and the upsert branch
        # is skipped (see the `elif dest_dsn:` guard below). Unknown
        # destination types return "" so the batch is logged + dropped.
        dest_dsn = task.get("dest_dsn", "")
        if not dest_dsn and connector_type != "iceberg":
            dest_dsn = _dest_dsn_from_dest(dest)
            if dest_dsn:
                log.debug("CDCTransform derived dest_dsn from destination block for connection=%s", connection_id)

        log.info("CDCTransform connection=%s events=%d dest=%s", connection_id, len(events), connector_type)

        if not events:
            return

        # Separate INSERT/UPDATE rows from DELETEs
        to_upsert = [e["after"] for e in events if e.get("op") in ("INSERT", "UPDATE") and e.get("after")]
        to_delete_pks = [e["before"][pk_col] for e in events if e.get("op") == "DELETE" and e.get("before")]

        if to_upsert and steps:
            to_upsert, _ = self.engine.execute_pipeline(to_upsert, steps)

        if connector_type == "iceberg":
            self._apply_to_iceberg(to_upsert, to_delete_pks, dest, table, pk_col)
        elif dest_dsn:
            self._upsert(to_upsert, to_delete_pks, dest_dsn, schema, table, pk_col)
        else:
            log.error(
                "CDCTransform connection=%s cannot write to %s destination: "
                "no dest_dsn and destination block missing/incomplete or "
                "connector_type unsupported — dropping %d events",
                connection_id, connector_type, len(events),
            )

    def _apply_to_iceberg(self, rows: list[dict], delete_pks: list,
                          dest: dict, table: str, pk_col: str):
        from iceberg_writer import IcebergWriter
        dest_config = dest.get("connection_config") or dest.get("config") or dest
        identifier_fields = dest_config.get("identifier_fields") or [pk_col]
        writer = IcebergWriter(dest_config)
        if rows:
            writer.upsert(rows, table_name=table, identifier_fields=identifier_fields)
        if delete_pks:
            writer.delete(table_name=table,
                          identifier_fields=identifier_fields,
                          delete_keys=delete_pks)

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
