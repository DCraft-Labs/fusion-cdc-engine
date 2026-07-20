"""
P2.10 — PostgreSQL Logical Replication Connector.

Uses psycopg2 LogicalReplicationConnection with the wal2json plugin.

CRITICAL: send_feedback() must be called every ≤10 seconds even when idle.
Without this, WAL accumulates and can crash the source database.

Slot name: fusion_{source_id_alphanum[:20]}
On shutdown: drop replication slot via pg_drop_replication_slot() to release WAL.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import AsyncIterator, Optional

from connectors.base import BaseConnector
from cdc_worker.event_envelope import CDCEvent, build_event

log = logging.getLogger(__name__)

FEEDBACK_INTERVAL = 10  # seconds — DO NOT increase


def _slot_name(source_id: str) -> str:
    alphanum = re.sub(r"[^a-z0-9]", "", source_id.lower())
    return f"fusion_{alphanum[:20]}"


class PostgresConnector(BaseConnector):
    """
    Streams CDC events via PostgreSQL logical replication (wal2json).

    source config keys:
        source_id, bank_id, tenant_id, host, port, database_name,
        username, password (plaintext, already decrypted),
        assigned_tables: [{"schema_name": ..., "table_name": ...}]
    """

    def __init__(self, source: dict, checkpoint_manager) -> None:
        super().__init__(source, checkpoint_manager)
        self._conn: Optional[object] = None
        self._cursor: Optional[object] = None
        self._running = False

    # ------------------------------------------------------------------
    # stream_events
    # ------------------------------------------------------------------

    async def stream_events(self) -> AsyncIterator[CDCEvent]:
        import psycopg2
        from psycopg2.extras import LogicalReplicationConnection

        source = self._source
        source_id = source["source_id"]
        bank_id = source["bank_id"]
        tenant_id = source["tenant_id"]
        slot = _slot_name(source_id)

        dsn = (
            f"host={source['host']} port={source.get('port', 5432)} "
            f"dbname={source['database_name']} user={source['username']} "
            f"password={source['password']}"
        )
        # Spec §5 (P5-10): TLS cert validation
        if source.get("ssl_ca") or source.get("sslmode"):
            sslmode = source.get("sslmode", "verify-full")
            dsn += f" sslmode={sslmode}"
            if source.get("ssl_ca"):
                dsn += f" sslrootcert={source['ssl_ca']}"
            if source.get("ssl_cert"):
                dsn += f" sslcert={source['ssl_cert']}"
            if source.get("ssl_key"):
                dsn += f" sslkey={source['ssl_key']}"

        self._conn = psycopg2.connect(dsn, connection_factory=LogicalReplicationConnection)
        self._cursor = self._conn.cursor()

        # Create slot if it doesn't exist
        try:
            self._cursor.create_replication_slot(slot, output_plugin="wal2json")
            log.info("Created replication slot %s", slot)
        except psycopg2.errors.DuplicateObject:
            log.info("Replication slot %s already exists — resuming", slot)

        start_lsn = self._ckpt.get(source_id, "__wal__", "__lsn__") or "0/0"

        options = {
            "pretty-print": "0",
            "include-lsn": "1",
            "include-timestamp": "1",
        }
        self._cursor.start_replication(
            slot_name=slot,
            decode=True,
            start_lsn=start_lsn,
            options=options,
        )

        self._running = True
        last_feedback = time.monotonic()
        # Track known columns per table to detect schema changes mid-stream
        _known_columns: dict[str, set] = {}

        try:
            while self._running:
                msg = self._cursor.read_message()

                # CRITICAL: send feedback within FEEDBACK_INTERVAL regardless
                now = time.monotonic()
                if (now - last_feedback) >= FEEDBACK_INTERVAL:
                    self._cursor.send_feedback(reply=True)
                    last_feedback = now

                if msg is None:
                    await asyncio.sleep(0.05)
                    continue

                lsn = str(msg.data_start)
                payload = json.loads(msg.payload)

                for change in payload.get("change", []):
                    # Schema-mismatch detection: if new columns appear that were
                    # not seen before, notify the control plane.
                    # Spec §1: "Detects DDL messages and notifies the control plane."
                    schema_key = f"{change.get('schema', 'public')}.{change.get('table', '')}"
                    col_names = set(change.get("columnnames", []))
                    if col_names and schema_key in _known_columns:
                        new_cols = col_names - _known_columns[schema_key]
                        if new_cols:
                            await self._notify_schema_mismatch(
                                source_id=source_id,
                                schema_name=change.get("schema", "public"),
                                table_name=change.get("table", ""),
                                new_columns=sorted(new_cols),
                            )
                    if col_names:
                        _known_columns[schema_key] = col_names

                    event = self._parse_change(change, lsn, source_id, bank_id, tenant_id)
                    if event:
                        yield event

                self._cursor.send_feedback(flush_lsn=msg.data_start)
                last_feedback = time.monotonic()
                self._ckpt.set(source_id, "__wal__", "__lsn__", lsn)

        except asyncio.CancelledError:
            raise
        finally:
            await self.close()

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_change(
        change: dict, lsn: str, source_id: str, bank_id: str, tenant_id: str
    ) -> Optional[CDCEvent]:
        kind = change.get("kind", "")
        schema = change.get("schema", "public")
        table = change.get("table", "")
        ts_ms = int(time.time() * 1000)

        if kind == "insert":
            after = dict(zip(change.get("columnnames", []), change.get("columnvalues", [])))
            pk = {k: v for k, v in after.items() if k in change.get("keynames", [])} or after
            return build_event(
                op="c", source_id=source_id, bank_id=bank_id, tenant_id=tenant_id,
                schema_name=schema, table_name=table, lsn=lsn, ts_ms=ts_ms,
                pk_values=pk, before=None, after=after,
            )
        elif kind == "update":
            after = dict(zip(change.get("columnnames", []), change.get("columnvalues", [])))
            before_vals = change.get("oldkeys", {})
            before = (
                dict(zip(before_vals.get("keynames", []), before_vals.get("keyvalues", [])))
                if before_vals else None
            )
            pk = {k: v for k, v in after.items() if k in before_vals.get("keynames", [])} or after
            return build_event(
                op="u", source_id=source_id, bank_id=bank_id, tenant_id=tenant_id,
                schema_name=schema, table_name=table, lsn=lsn, ts_ms=ts_ms,
                pk_values=pk, before=before, after=after,
            )
        elif kind == "delete":
            before_vals = change.get("oldkeys", {})
            before = dict(zip(before_vals.get("keynames", []), before_vals.get("keyvalues", [])))
            pk = before
            return build_event(
                op="d", source_id=source_id, bank_id=bank_id, tenant_id=tenant_id,
                schema_name=schema, table_name=table, lsn=lsn, ts_ms=ts_ms,
                pk_values=pk, before=before, after=None,
            )
        return None

    # ------------------------------------------------------------------
    # Schema-mismatch notification
    # ------------------------------------------------------------------

    async def _notify_schema_mismatch(
        self, source_id: str, schema_name: str, table_name: str, new_columns: list
    ) -> None:
        """
        Report newly-appearing columns to the control plane so schema evolution
        can be applied.
        Spec §1 (Postgres): 'Detects DDL messages and notifies the control plane
        to trigger schema re-introspection.'
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
                        "ddl_query": f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN {', '.join(new_columns)}",
                        "change_type": "column_added",
                    },
                    headers={"X-Worker-Token": settings.WORKER_TOKEN},
                    timeout=5.0,
                )
            log.info(
                "Schema mismatch reported: new columns %s in %s.%s",
                new_columns, schema_name, table_name,
            )
        except Exception as exc:
            log.warning("Could not report schema mismatch to control plane: %s", exc)

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    async def close(self) -> None:
        self._running = False
        if self._cursor:
            try:
                slot = _slot_name(self._source["source_id"])
                self._cursor.execute(f"SELECT pg_drop_replication_slot('{slot}')")
                log.info("Dropped replication slot %s", slot)
            except Exception as exc:
                log.warning("Could not drop slot: %s", exc)
            self._cursor.close()
            self._cursor = None
        if self._conn:
            self._conn.close()
            self._conn = None
