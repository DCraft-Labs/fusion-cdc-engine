"""
P2.9 — MySQL Connector (binlog ROW format).

Reads from MySQL replication stream using python-mysql-replication.
Resumes from the last committed (log_file, log_pos) stored in SQLite.
Emits only tables in the assigned table list.

Prerequisites on the MySQL server:
  binlog_format = ROW
  GRANT REPLICATION SLAVE, SELECT ON *.* TO 'cdc_user'@'%';

server_id is derived from the source UUID to ensure uniqueness across workers.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import tempfile
import os
import time
from contextlib import contextmanager
from typing import AsyncIterator, Optional, Tuple

from connectors.base import BaseConnector
from cdc_worker.event_envelope import CDCEvent, build_event

log = logging.getLogger(__name__)


@contextmanager
def _ssh_tunnel_ctx(ssh_config: dict, target_host: str, target_port: int):
    """
    Context manager that creates an SSH tunnel when ssh_config is provided.
    Yields (local_host, local_port) — either the tunnel endpoint or
    the original (target_host, target_port) if no tunnel is needed.
    """
    if not ssh_config or not ssh_config.get("tunnel_host"):
        yield target_host, target_port
        return

    from sshtunnel import SSHTunnelForwarder

    tunnel_host = ssh_config["tunnel_host"]
    tunnel_port = int(ssh_config.get("tunnel_port", 22))
    tunnel_user = ssh_config.get("tunnel_username", "")
    auth_method = ssh_config.get("tunnel_auth_method", "key")

    # Write private key to a temp file if provided inline
    pkey_file = None
    pkey_path = None
    try:
        if auth_method == "key":
            key_material = ssh_config.get("tunnel_private_key", "")
            if key_material:
                pkey_file = tempfile.NamedTemporaryFile(
                    mode="w", suffix=".pem", delete=False
                )
                pkey_file.write(key_material)
                pkey_file.flush()
                pkey_file.close()
                pkey_path = pkey_file.name
                os.chmod(pkey_path, 0o600)

        ssh_kwargs = dict(
            ssh_address_or_host=(tunnel_host, tunnel_port),
            remote_bind_address=(target_host, target_port),
            ssh_username=tunnel_user,
        )
        if auth_method == "key" and pkey_path:
            ssh_kwargs["ssh_pkey"] = pkey_path
        elif auth_method == "password":
            ssh_kwargs["ssh_password"] = ssh_config.get("tunnel_password", "")

        server = SSHTunnelForwarder(**ssh_kwargs)
        server.start()
        log.info(
            "SSH tunnel established: localhost:%d -> %s:%d (via %s:%d)",
            server.local_bind_port, target_host, target_port, tunnel_host, tunnel_port,
        )
        try:
            yield "127.0.0.1", server.local_bind_port
        finally:
            server.stop()
            log.info("SSH tunnel closed")
    finally:
        if pkey_path and os.path.exists(pkey_path):
            os.unlink(pkey_path)


def _server_id_from_uuid(source_id: str) -> int:
    """Deterministic server_id from source UUID (fits in MySQL's 32-bit range)."""
    return int(hashlib.md5(source_id.encode()).hexdigest(), 16) % (2 ** 32)


class MySQLConnector(BaseConnector):
    """
    Streams CDC events from a MySQL binlog.

    source config keys:
        source_id, bank_id, tenant_id, host, port, database_name,
        username, password (plaintext, already decrypted),
        assigned_tables: [{"schema_name": ..., "table_name": ...}, ...]
    """

    def __init__(self, source: dict, checkpoint_manager) -> None:
        super().__init__(source, checkpoint_manager)
        self._stream = None
        self._running = False
        self._col_names: dict = {}  # (schema, table) -> [col_name, ...]
        self._pk_cols: dict  = {}  # (schema, table) -> [pk_col_name, ...]

    def _fetch_column_names(self, connection_settings: dict, ssh_config: dict = None) -> None:
        """Query INFORMATION_SCHEMA to get ordered column names for all tables."""
        import pymysql
        ssh_config = ssh_config or {}
        orig_host = connection_settings["host"]
        orig_port = connection_settings["port"]
        with _ssh_tunnel_ctx(ssh_config, orig_host, orig_port) as (tunnel_host, tunnel_port):
            conn = pymysql.connect(
                host=tunnel_host,
                port=tunnel_port,
                user=connection_settings["user"],
                password=connection_settings["passwd"],
            )
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION
                        FROM INFORMATION_SCHEMA.COLUMNS
                        ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
                    """)
                    for schema, table, col, pos in cur.fetchall():
                        key = (schema, table)
                        if key not in self._col_names:
                            self._col_names[key] = []
                        self._col_names[key].append(col)
                    # Load primary key columns
                    cur.execute("""
                        SELECT k.TABLE_SCHEMA, k.TABLE_NAME, k.COLUMN_NAME
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
                        JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                          ON tc.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
                         AND tc.TABLE_NAME = k.TABLE_NAME
                         AND tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                        ORDER BY k.TABLE_SCHEMA, k.TABLE_NAME, k.ORDINAL_POSITION
                    """)
                    for schema, table, col in cur.fetchall():
                        key = (schema, table)
                        if key not in self._pk_cols:
                            self._pk_cols[key] = []
                        self._pk_cols[key].append(col)
            finally:
                conn.close()
        log.info("Loaded column names for %d tables, PK info for %d tables from INFORMATION_SCHEMA",
                 len(self._col_names), len(self._pk_cols))

    def _remap_row(self, schema: str, table: str, row: dict) -> dict:
        """Replace UNKNOWN_COLn keys with real column names if available."""
        col_list = self._col_names.get((schema, table))
        if not col_list:
            return row
        # Only remap if keys look like UNKNOWN_COLn
        if not any(k.startswith("UNKNOWN_COL") for k in row):
            return row
        return {col_list[i]: v for i, v in enumerate(row.values()) if i < len(col_list)}

    # ------------------------------------------------------------------
    # stream_events
    # ------------------------------------------------------------------

    async def stream_events(self) -> AsyncIterator[CDCEvent]:
        from pymysqlreplication import BinLogStreamReader
        from pymysqlreplication.row_event import (
            WriteRowsEvent,
            UpdateRowsEvent,
            DeleteRowsEvent,
        )
        from pymysqlreplication.event import QueryEvent, HeartbeatLogEvent

        source = self._source
        source_id = source["source_id"]
        bank_id = source["bank_id"]
        tenant_id = source["tenant_id"]
        ssh_config = source.get("ssh_config") or {}

        assigned_tables = {
            (t["schema_name"], t["table_name"])
            for t in source.get("assigned_tables", [])
        }

        # Resume position
        log_file, log_pos = self._get_resume_position(source_id)

        orig_host = source["host"]
        orig_port = source.get("port", 3306)

        # Spec §5 (P5-10): TLS cert validation — pass ssl config when provided
        ssl_opts = {}
        if source.get("ssl_ca"):
            ssl_opts["ssl_ca"] = source["ssl_ca"]
        if source.get("ssl_cert"):
            ssl_opts["ssl_cert"] = source["ssl_cert"]
        if source.get("ssl_key"):
            ssl_opts["ssl_key"] = source["ssl_key"]

        with _ssh_tunnel_ctx(ssh_config, orig_host, orig_port) as (eff_host, eff_port):
            connection_settings = {
                "host": eff_host,
                "port": eff_port,
                "user": source["username"],
                "passwd": source["password"],
            }
            if ssl_opts:
                ssl_opts["ssl_verify_cert"] = source.get("ssl_verify", True)
                connection_settings["ssl"] = ssl_opts

            # Fetch real column names so UNKNOWN_COLn keys get replaced
            # (pass ssh_config=None since the tunnel is already established via eff_host/eff_port)
            try:
                self._fetch_column_names(connection_settings)
            except Exception as exc:
                log.warning("Could not fetch column names (will use raw keys): %s", exc)
            server_id = _server_id_from_uuid(source_id)

            kwargs = dict(
                connection_settings=connection_settings,
                server_id=server_id,
                only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, QueryEvent, HeartbeatLogEvent],
                resume_stream=True,
                blocking=False,
            )
            if log_file and log_pos:
                kwargs["log_file"] = log_file
                kwargs["log_pos"] = log_pos

            # Use BinLogStreamReader's built-in server-side filters for efficiency
            if assigned_tables:
                schemas = list({s for s, _ in assigned_tables})
                tables = list({t for _, t in assigned_tables})
                kwargs["only_schemas"] = schemas
                kwargs["only_tables"] = tables

            self._stream = BinLogStreamReader(**kwargs)
            self._running = True

            try:
                while self._running:
                    for binlog_event in self._stream:
                        if not self._running:
                            break

                        if isinstance(binlog_event, HeartbeatLogEvent):
                            log.debug("MySQL heartbeat received — stream alive at %s",
                                      getattr(binlog_event, "log_pos", "?"))
                            continue

                        if isinstance(binlog_event, QueryEvent):
                            query = getattr(binlog_event, "query", "") or ""
                            log.info("DDL event in binlog: %s", query)
                            if any(kw in query.upper() for kw in ("ALTER", "CREATE", "DROP", "RENAME")):
                                await self._notify_ddl_change(
                                    source_id=source_id,
                                    schema_name=getattr(binlog_event, "schema", source.get("database_name", "")),
                                    table_name="",
                                    ddl_query=query,
                                )
                            continue

                        schema = binlog_event.schema
                        table = binlog_event.table
                        if assigned_tables and (schema, table) not in assigned_tables:
                            continue

                        lsn = f"{self._stream.log_file}:{self._stream.log_pos}"
                        ts_ms = (binlog_event.timestamp or int(time.time())) * 1000

                        for row in binlog_event.rows:
                            if isinstance(binlog_event, WriteRowsEvent):
                                after = self._remap_row(schema, table, row["values"])
                                pk = self._extract_pk(binlog_event, after)
                                event = build_event(
                                    op="c", source_id=source_id, bank_id=bank_id,
                                    tenant_id=tenant_id, schema_name=schema, table_name=table,
                                    lsn=lsn, ts_ms=ts_ms, pk_values=pk,
                                    before=None, after=after,
                                )
                            elif isinstance(binlog_event, UpdateRowsEvent):
                                before = self._remap_row(schema, table, row["before_values"])
                                after = self._remap_row(schema, table, row["after_values"])
                                pk = self._extract_pk(binlog_event, after)
                                event = build_event(
                                    op="u", source_id=source_id, bank_id=bank_id,
                                    tenant_id=tenant_id, schema_name=schema, table_name=table,
                                    lsn=lsn, ts_ms=ts_ms, pk_values=pk,
                                    before=before, after=after,
                                )
                            else:  # DeleteRowsEvent
                                before = self._remap_row(schema, table, row["values"])
                                pk = self._extract_pk(binlog_event, before)
                                event = build_event(
                                    op="d", source_id=source_id, bank_id=bank_id,
                                    tenant_id=tenant_id, schema_name=schema, table_name=table,
                                    lsn=lsn, ts_ms=ts_ms, pk_values=pk,
                                    before=before, after=None,
                                )

                            yield event

                    # No events ready — yield control to the event loop briefly
                    await asyncio.sleep(0.05)

            except asyncio.CancelledError:
                raise
            finally:
                await self.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_resume_position(self, source_id: str):
        """Return (log_file, log_pos) from checkpoint, or (None, None)."""
        raw = self._ckpt.get(source_id, "__binlog__", "__position__")
        if raw:
            try:
                parts = raw.rsplit(":", 1)
                return parts[0], int(parts[1])
            except Exception:
                pass
        return None, None

    async def _notify_ddl_change(
        self, source_id: str, schema_name: str, table_name: str, ddl_query: str
    ) -> None:
        """Report a DDL change to the control plane for schema re-introspection.
        Spec §1: 'Handles DDL events by sending schema change notifications to the control plane.'
        """
        try:
            import httpx
            from cdc_worker.config import settings
            async with httpx.AsyncClient() as client:
                await client.post(
                    f"{settings.CONTROL_PLANE_URL}/api/v1/internal/report-ddl-change",
                    json={
                        "source_id": source_id,
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "ddl_query": ddl_query,
                        "change_type": "ddl_detected",
                    },
                    headers={"X-Worker-Token": settings.WORKER_TOKEN},
                    timeout=5.0,
                )
            log.info("DDL change reported to control plane for source=%s", source_id)
        except Exception as exc:
            log.warning("Could not report DDL change to control plane: %s", exc)

    def _extract_pk(self, event, row: dict) -> dict:
        """Extract primary key columns from the row using INFORMATION_SCHEMA PK data."""
        schema = getattr(event, 'schema', None) or getattr(event, 'table_map', {}).get(event.table, None)
        # Use INFORMATION_SCHEMA-loaded PK columns (most reliable)
        table_name = event.table
        schema_name = getattr(event, 'schema', None)
        if schema_name:
            pk_cols = self._pk_cols.get((schema_name, table_name))
            if pk_cols:
                result = {col: row.get(col) for col in pk_cols if col in row}
                if result:
                    return result
        # Fallback: try pymysqlreplication primary_key attribute (string names only)
        raw_pk = getattr(event.table_map.get(event.table, None), "primary_key", None)
        if raw_pk:
            str_pk = [c for c in raw_pk if isinstance(c, str) and c in row]
            if str_pk:
                return {col: row.get(col) for col in str_pk}
        # Final fallback: look for a column named 'id'
        if 'id' in row:
            return {'id': row['id']}
        return {}

    async def close(self) -> None:
        self._running = False
        if self._stream:
            self._stream.close()
            self._stream = None
