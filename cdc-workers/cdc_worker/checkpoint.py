"""
P2.3 / P2.4 — Checkpoint management.

LocalCheckpointManager
  Stores (source_id, schema_name, table_name) → lsn in a local SQLite file.
  Used by connectors to resume from the last committed position on restart.
  Atomic batch writes via temp-table swap.

CentralCheckpointSync
  Pushes local checkpoints to the control-plane API (POST /internal/checkpoints/batch)
  and pulls authoritative checkpoints from it (GET /internal/checkpoints/{source_id}).
  Uses httpx for async HTTP.  Retries up to 3 times on 5xx/network errors.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# LocalCheckpointManager
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS checkpoints (
    source_id   TEXT    NOT NULL,
    schema_name TEXT    NOT NULL,
    table_name  TEXT    NOT NULL,
    lsn         TEXT    NOT NULL,
    updated_at  INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    PRIMARY KEY (source_id, schema_name, table_name)
);
"""


class LocalCheckpointManager:
    """
    Thread-safe (via check_same_thread=False) local SQLite checkpoint store.

    Parameters
    ----------
    db_path : str or Path
        Path to the SQLite file.  Use ":memory:" in tests.
    """

    def __init__(self, db_path: str = ":memory:") -> None:
        self._path = str(db_path)
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute(_SCHEMA)
        self._conn.commit()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, source_id: str, schema_name: str, table_name: str) -> Optional[str]:
        """Return the stored LSN, or None if no checkpoint exists."""
        row = self._conn.execute(
            "SELECT lsn FROM checkpoints WHERE source_id=? AND schema_name=? AND table_name=?",
            (source_id, schema_name, table_name),
        ).fetchone()
        return row["lsn"] if row else None

    def set(self, source_id: str, schema_name: str, table_name: str, lsn: str) -> None:
        """Upsert a single checkpoint."""
        self._conn.execute(
            """
            INSERT INTO checkpoints (source_id, schema_name, table_name, lsn)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(source_id, schema_name, table_name)
            DO UPDATE SET lsn=excluded.lsn,
                          updated_at=strftime('%s','now')
            """,
            (source_id, schema_name, table_name, lsn),
        )
        self._conn.commit()

    def set_batch_atomic(
        self, source_id: str, updates: List[Tuple[str, str, str]]
    ) -> None:
        """
        Atomically upsert a batch of (schema_name, table_name, lsn) tuples for one source.
        Uses a single transaction — all succeed or none do.
        """
        with self._conn:
            for schema_name, table_name, lsn in updates:
                self._conn.execute(
                    """
                    INSERT INTO checkpoints (source_id, schema_name, table_name, lsn)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(source_id, schema_name, table_name)
                    DO UPDATE SET lsn=excluded.lsn,
                                  updated_at=strftime('%s','now')
                    """,
                    (source_id, schema_name, table_name, lsn),
                )

    def get_all_for_source(self, source_id: str) -> Dict[Tuple[str, str], str]:
        """
        Return all checkpoints for a source as
        {(schema_name, table_name): lsn}
        """
        rows = self._conn.execute(
            "SELECT schema_name, table_name, lsn FROM checkpoints WHERE source_id=?",
            (source_id,),
        ).fetchall()
        return {(r["schema_name"], r["table_name"]): r["lsn"] for r in rows}

    def close(self) -> None:
        self._conn.close()


# ---------------------------------------------------------------------------
# CentralCheckpointSync
# ---------------------------------------------------------------------------

class CentralCheckpointSync:
    """
    Async client that syncs checkpoints to/from the control-plane internal API.

    Requires httpx and an asyncio event loop.
    Retries up to max_retries times on 5xx or connection errors (exponential
    back-off is intentionally omitted to keep worker loop fast — the next
    scheduled sync will retry automatically).
    """

    def __init__(
        self,
        control_plane_url: str,
        worker_token: str,
        worker_id: str,
        max_retries: int = 3,
    ) -> None:
        self._base = control_plane_url.rstrip("/")
        self._headers = {
            "X-Worker-Token": worker_token,
            "X-Worker-ID": worker_id,
            "Content-Type": "application/json",
        }
        self._max_retries = max_retries

    async def push(
        self,
        local: LocalCheckpointManager,
        source_id: str,
    ) -> bool:
        """
        Push all local checkpoints for source_id to the control plane.
        Returns True on success, False on permanent failure.
        """
        import httpx

        checkpoints = local.get_all_for_source(source_id)
        checkpoint_list = [
            {
                "schema_name": schema,
                "table_name": table,
                "lsn": lsn,
            }
            for (schema, table), lsn in checkpoints.items()
        ]
        if not checkpoint_list:
            return True

        # Extract worker_id from headers
        worker_id = self._headers.get("X-Worker-ID", "unknown")
        payload = {
            "worker_id": worker_id,
            "source_id": source_id,
            "checkpoints": checkpoint_list,
        }

        url = f"{self._base}/api/v1/internal/checkpoints/batch"
        for attempt in range(self._max_retries):
            try:
                async with httpx.AsyncClient() as client:
                    resp = await client.post(
                        url, json=payload, headers=self._headers, timeout=10.0
                    )
                    if resp.status_code < 500:
                        return resp.status_code in (200, 201, 204)
                    log.warning(
                        "push checkpoints attempt %d/%d: HTTP %d",
                        attempt + 1, self._max_retries, resp.status_code,
                    )
            except Exception as exc:
                log.warning(
                    "push checkpoints attempt %d/%d failed: %s",
                    attempt + 1, self._max_retries, exc,
                )
        return False

    async def pull(self, source_id: str) -> Dict[Tuple[str, str], str]:
        """
        Fetch authoritative checkpoints for source_id from the control plane.
        Returns {} on 404 or any error.
        """
        import httpx

        url = f"{self._base}/api/v1/internal/checkpoints/{source_id}"
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(url, headers=self._headers, timeout=10.0)
            if resp.status_code == 404:
                return {}
            if resp.status_code >= 400:
                return {}
            data = resp.json()
            return {
                (item["schema_name"], item["table_name"]): item["lsn"]
                for item in data
            }
        except Exception as exc:
            log.error("pull checkpoints for %s failed: %s", source_id, exc)
            return {}
