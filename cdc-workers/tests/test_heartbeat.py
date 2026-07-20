"""
P2.8 — Tests for heartbeat.py.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from cdc_worker.heartbeat import HeartbeatSender


class TestHeartbeatSender:
    @pytest.mark.asyncio
    async def test_heartbeat_posts_at_interval(self):
        sender = HeartbeatSender(
            control_plane_url="http://localhost:8000",
            worker_token="token",
            worker_id="w1",
            interval=1,
        )

        posted = []

        async def fake_post(*args, **kwargs):
            posted.append(kwargs.get("json", {}))
            resp = MagicMock()
            resp.status_code = 204
            return resp

        with patch("httpx.AsyncClient.post", new_callable=AsyncMock, side_effect=fake_post):
            sender.start()
            await asyncio.sleep(2.2)
            await sender.stop()

        # Should have sent at least 2 heartbeats in 2.2 seconds with 1s interval
        assert len(posted) >= 2

    @pytest.mark.asyncio
    async def test_heartbeat_exception_swallowed(self):
        """A network error must NOT propagate — heartbeat failure is non-fatal."""
        sender = HeartbeatSender(
            control_plane_url="http://localhost:8000",
            worker_token="token",
            worker_id="w1",
            interval=0,  # fire immediately
        )

        with patch("httpx.AsyncClient.post", new_callable=AsyncMock, side_effect=Exception("network down")):
            sender.start()
            await asyncio.sleep(0.1)
            await sender.stop()
        # No exception raised — test passes

    @pytest.mark.asyncio
    async def test_payload_contains_worker_id(self):
        sender = HeartbeatSender(
            control_plane_url="http://localhost:8000",
            worker_token="token",
            worker_id="my-worker-42",
            interval=0,
        )
        payloads = []

        async def capture(*args, **kwargs):
            payloads.append(kwargs.get("json", {}))
            resp = MagicMock()
            resp.status_code = 204
            return resp

        with patch("httpx.AsyncClient.post", new_callable=AsyncMock, side_effect=capture):
            sender.start()
            await asyncio.sleep(0.05)
            await sender.stop()

        assert any(p.get("worker_id") == "my-worker-42" for p in payloads)
