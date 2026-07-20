"""
P2.11 — MongoDB Change Stream Connector.

Uses pymongo collection.watch() with:
  full_document="updateLookup"           — full doc after update
  full_document_before_change="whenAvailable"  — MongoDB 6+ only

Resume tokens are stored per (source_id, db, collection) in the checkpoint store.

ObjectId and datetime are serialized to str in the event payload.
ChangeStreamHistoryLost triggers a control-plane notification for resync.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, AsyncIterator, Dict, Optional

from connectors.base import BaseConnector
from cdc_worker.event_envelope import CDCEvent, build_event

log = logging.getLogger(__name__)

# ChangeStreamHistoryLost was added in pymongo 4.0 but may be missing in some
# builds. Define a local fallback so the import never crashes at startup.
try:
    from pymongo.errors import ChangeStreamHistoryLost
except ImportError:
    class ChangeStreamHistoryLost(Exception):  # type: ignore[no-redef]
        """Fallback stub — pymongo version does not expose this class."""


def _serialize(value: Any) -> Any:
    """Recursively convert non-JSON-serializable types to str."""
    from bson import ObjectId
    from datetime import datetime

    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _serialize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_serialize(i) for i in value]
    return value


class MongoDBConnector(BaseConnector):
    """
    Streams CDC events from MongoDB change streams.

    source config keys:
        source_id, bank_id, tenant_id, host, port, database_name,
        username (optional), password (optional),
        assigned_tables: [{"schema_name": <db>, "table_name": <collection>}]
    """

    def __init__(self, source: dict, checkpoint_manager) -> None:
        super().__init__(source, checkpoint_manager)
        self._client = None
        self._running = False

    # ------------------------------------------------------------------
    # stream_events
    # ------------------------------------------------------------------

    async def stream_events(self) -> AsyncIterator[CDCEvent]:
        import pymongo

        source = self._source
        source_id = source["source_id"]
        bank_id = source["bank_id"]
        tenant_id = source["tenant_id"]

        username = source.get("username")
        password = source.get("password")
        host = source["host"]
        port = source.get("port", 27017)
        db_name = source["database_name"]

        if username and password:
            uri = f"mongodb://{username}:{password}@{host}:{port}/{db_name}"
        else:
            uri = f"mongodb://{host}:{port}/{db_name}"

        # Spec §5 (P5-10): TLS cert validation
        client_kwargs: dict = {"serverSelectionTimeoutMS": 5000}
        if source.get("ssl_ca") or source.get("tls"):
            client_kwargs["tls"] = True
            if source.get("ssl_ca"):
                client_kwargs["tlsCAFile"] = source["ssl_ca"]
            if source.get("ssl_cert"):
                client_kwargs["tlsCertificateKeyFile"] = source["ssl_cert"]
            # ssl_verify=False allows self-signed certs (not recommended in prod)
            if not source.get("ssl_verify", True):
                client_kwargs["tlsAllowInvalidCertificates"] = True

        self._client = pymongo.MongoClient(uri, **client_kwargs)
        db = self._client[db_name]

        assigned = {
            t["table_name"]
            for t in source.get("assigned_tables", [])
        }

        self._running = True
        try:
            # Collect async generator objects (NOT futures/tasks)
            gens = [
                self._watch_collection(
                    db[col], col, db_name, source_id, bank_id, tenant_id
                )
                for col in assigned
            ]
            if not gens:
                # Watch the whole DB
                gens = [
                    self._watch_db(db, db_name, source_id, bank_id, tenant_id)
                ]

            # We need to yield from multiple async generators — use a queue
            queue: asyncio.Queue = asyncio.Queue()

            async def producer(async_gen):
                async for event in async_gen:
                    await queue.put(event)

            producer_tasks = [asyncio.ensure_future(producer(gen)) for gen in gens]

            while self._running:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=1.0)
                    yield event
                except asyncio.TimeoutError:
                    continue

        except asyncio.CancelledError:
            raise
        finally:
            await self.close()

    async def _watch_collection(
        self, collection, col_name: str, db_name: str,
        source_id: str, bank_id: str, tenant_id: str
    ) -> AsyncIterator[CDCEvent]:
        resume_token_json = self._ckpt.get(source_id, db_name, col_name)
        resume_after = json.loads(resume_token_json) if resume_token_json else None

        watch_kwargs: Dict[str, Any] = {
            "full_document": "updateLookup",
        }
        try:
            watch_kwargs["full_document_before_change"] = "whenAvailable"
        except Exception:
            pass
        if resume_after:
            watch_kwargs["resume_after"] = resume_after

        loop = asyncio.get_event_loop()
        stream = collection.watch(**watch_kwargs)
        try:
            while self._running:
                # Run blocking pymongo call in a thread so the event loop stays free
                change = await loop.run_in_executor(None, stream.try_next)
                if change is None:
                    await asyncio.sleep(0.05)
                    continue

                event = self._parse_change(
                    change, db_name, col_name, source_id, bank_id, tenant_id
                )
                if event:
                    # Save resume token
                    self._ckpt.set(
                        source_id, db_name, col_name,
                        json.dumps(stream.resume_token, default=str)
                    )
                    yield event

        except ChangeStreamHistoryLost:
            log.error(
                "ChangeStreamHistoryLost for %s.%s — flagging for resync", db_name, col_name
            )
            await self._notify_history_lost(source_id, db_name, col_name)
        finally:
            stream.close()

    async def _watch_db(self, db, db_name, source_id, bank_id, tenant_id):
        """Watch all collections in the database."""
        loop = asyncio.get_event_loop()
        stream = db.watch(full_document="updateLookup")
        try:
            while self._running:
                change = await loop.run_in_executor(None, stream.try_next)
                if change is None:
                    await asyncio.sleep(0.05)
                    continue
                ns = change.get("ns", {})
                col_name = ns.get("coll", "unknown")
                event = self._parse_change(
                    change, db_name, col_name, source_id, bank_id, tenant_id
                )
                if event:
                    yield event
        finally:
            stream.close()

    # ------------------------------------------------------------------
    # Change parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_change(
        change: dict, db_name: str, col_name: str,
        source_id: str, bank_id: str, tenant_id: str
    ) -> Optional[CDCEvent]:
        op_type = change.get("operationType", "")
        ts = change.get("clusterTime")
        ts_ms = int(ts.as_datetime().timestamp() * 1000) if ts else int(time.time() * 1000)
        lsn = json.dumps(change.get("_id"), default=str)

        doc_key = _serialize(change.get("documentKey", {}))
        pk_values = doc_key if doc_key else {"_id": str(change.get("_id", ""))}

        if op_type == "insert":
            after = _serialize(change.get("fullDocument") or {})
            return build_event(
                op="c", source_id=source_id, bank_id=bank_id, tenant_id=tenant_id,
                schema_name=db_name, table_name=col_name, lsn=lsn, ts_ms=ts_ms,
                pk_values=pk_values, before=None, after=after,
            )
        elif op_type in ("update", "replace"):
            after = _serialize(change.get("fullDocument") or {})
            before_raw = change.get("fullDocumentBeforeChange")
            before = _serialize(before_raw) if before_raw else None
            return build_event(
                op="u", source_id=source_id, bank_id=bank_id, tenant_id=tenant_id,
                schema_name=db_name, table_name=col_name, lsn=lsn, ts_ms=ts_ms,
                pk_values=pk_values, before=before, after=after,
            )
        elif op_type == "delete":
            before_raw = change.get("fullDocumentBeforeChange")
            before = _serialize(before_raw) if before_raw else pk_values
            return build_event(
                op="d", source_id=source_id, bank_id=bank_id, tenant_id=tenant_id,
                schema_name=db_name, table_name=col_name, lsn=lsn, ts_ms=ts_ms,
                pk_values=pk_values, before=before, after=None,
            )
        return None

    # ------------------------------------------------------------------

    async def _notify_history_lost(self, source_id: str, db_name: str, col_name: str) -> None:
        """Alert the control plane that this collection needs a resync."""
        try:
            import httpx
            from cdc_worker.config import settings
            async with httpx.AsyncClient() as client:
                await client.post(
                    f"{settings.CONTROL_PLANE_URL}/api/v1/internal/resync-request",
                    json={"source_id": source_id, "schema_name": db_name, "table_name": col_name},
                    headers={"X-Worker-Token": settings.WORKER_TOKEN},
                    timeout=5.0,
                )
        except Exception as exc:
            log.warning("Could not notify history lost: %s", exc)

    async def close(self) -> None:
        self._running = False
        if self._client:
            self._client.close()
            self._client = None
