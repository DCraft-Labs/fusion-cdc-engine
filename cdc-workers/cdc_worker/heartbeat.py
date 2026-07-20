"""
P2.8 — Async worker heartbeat.

Posts to the control-plane internal API every HEARTBEAT_INTERVAL seconds.
Exceptions are ALWAYS swallowed — a heartbeat failure must never crash the worker.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

log = logging.getLogger(__name__)


class HeartbeatSender:
    """
    Sends periodic heartbeat payloads to the control-plane internal API.

    Parameters
    ----------
    control_plane_url : str
    worker_token : str
    worker_id : str
    interval : int          Seconds between heartbeats (default 30).
    """

    def __init__(
        self,
        control_plane_url: str,
        worker_token: str,
        worker_id: str,
        interval: int = 30,
    ) -> None:
        self._base = control_plane_url.rstrip("/")
        self._headers = {
            "X-Worker-Token": worker_token,
            "X-Worker-ID": worker_id,
        }
        self._worker_id = worker_id
        self._interval = interval
        self._task: Optional[asyncio.Task] = None
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Schedule the heartbeat loop in the current asyncio event loop."""
        self._running = True
        self._task = asyncio.ensure_future(self._loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        while self._running:
            await self._send_once()
            await asyncio.sleep(self._interval)

    async def _send_once(self) -> None:
        """Send one heartbeat.  Any exception is swallowed."""
        try:
            import httpx

            payload = {
                "worker_id": self._worker_id,
                "ts_ms": int(time.time() * 1000),
                "status": "running",
            }
            async with httpx.AsyncClient() as client:
                await client.post(
                    f"{self._base}/api/v1/internal/workers/heartbeat",
                    json=payload,
                    headers=self._headers,
                    timeout=5.0,
                )
        except Exception as exc:
            # Intentional swallow — heartbeat failure must never crash the worker
            log.debug("heartbeat send failed (non-fatal): %s", exc)
