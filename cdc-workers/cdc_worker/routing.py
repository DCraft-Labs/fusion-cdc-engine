"""
P2.13 — Multi-tenant routing table.

Maps (schema_name, table_name) → list of tenant assignments.
Used by the worker to route events from a shared DB host to the
correct per-tenant Redis stream keys.

Key spec requirement (Section 9.2):
  "Multiple tenants can share the same physical table → event duplicated"

The routing table is populated from the control-plane internal API and
can be refreshed on a configurable interval.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

# Type alias: (schema_name, table_name) → list of routing entries
RoutingKey = Tuple[str, str]
RoutingEntry = Dict[str, str]  # {"bank_id", "tenant_id", "source_id"}


class RoutingTable:
    """
    In-memory routing table: (schema, table) → [tenant assignments].

    Thread-safe for concurrent reads; writes are rare (reload only).

    Parameters
    ----------
    control_plane_url : str
    worker_token : str
    worker_id : str
    refresh_interval : int
        Seconds between automatic refreshes (0 = no auto-refresh).
    """

    def __init__(
        self,
        control_plane_url: str = "",
        worker_token: str = "",
        worker_id: str = "",
        refresh_interval: int = 60,
    ) -> None:
        self._base = control_plane_url.rstrip("/")
        self._headers = {
            "X-Worker-Token": worker_token,
            "X-Worker-ID": worker_id,
        }
        self._worker_id = worker_id
        self._refresh_interval = refresh_interval
        self._table: Dict[RoutingKey, List[RoutingEntry]] = {}
        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def lookup(self, schema_name: str, table_name: str) -> List[RoutingEntry]:
        """
        Return all tenant routing entries for (schema_name, table_name).

        Checks the exact key, a schema-wildcard ("*", table_name), and a
        full wildcard ("*", "*") that matches all schemas and tables.
        Returns [] if no route is configured.
        """
        exact = self._table.get((schema_name, table_name), [])
        wildcard = self._table.get(("*", table_name), [])
        wildcard_all = self._table.get(("*", "*"), [])
        return exact + wildcard + wildcard_all

    def set(self, schema_name: str, table_name: str, entries: List[RoutingEntry]) -> None:
        """Directly set routing entries (used by tests and manual setup)."""
        self._table[(schema_name, table_name)] = entries

    def load_from_dict(self, routing_data: List[dict]) -> None:
        """
        Populate the routing table from a list of control-plane records.

        Expected dict shape:
          {"schema_name": ..., "table_name": ..., "bank_id": ...,
           "tenant_id": ..., "source_id": ...}
        """
        new_table: Dict[RoutingKey, List[RoutingEntry]] = {}
        for record in routing_data:
            key: RoutingKey = (record["schema_name"], record["table_name"])
            entry: RoutingEntry = {
                "bank_id": record["bank_id"],
                "tenant_id": record["tenant_id"],
                "source_id": record["source_id"],
            }
            new_table.setdefault(key, []).append(entry)
        self._table = new_table
        log.info("Routing table loaded: %d stream routes", sum(len(v) for v in new_table.values()))

    # ------------------------------------------------------------------
    # Async refresh from control plane
    # ------------------------------------------------------------------

    async def fetch_and_reload(self) -> bool:
        """
        Fetch routing data from the control-plane and reload the table.
        Returns True on success.
        """
        try:
            import httpx

            url = f"{self._base}/api/v1/internal/workers/{self._worker_id}/routing"
            async with httpx.AsyncClient() as client:
                resp = await client.get(url, headers=self._headers, timeout=10.0)
                if resp.status_code == 200:
                    self.load_from_dict(resp.json())
                    return True
                log.warning("Routing fetch returned HTTP %d", resp.status_code)
        except Exception as exc:
            log.error("Failed to fetch routing table: %s", exc)
        return False

    def start_auto_refresh(self) -> None:
        """Schedule periodic routing table refresh."""
        if self._refresh_interval > 0:
            self._task = asyncio.ensure_future(self._refresh_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _refresh_loop(self) -> None:
        while True:
            await asyncio.sleep(self._refresh_interval)
            await self.fetch_and_reload()
