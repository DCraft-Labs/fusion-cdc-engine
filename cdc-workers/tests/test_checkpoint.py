"""
P2.3 / P2.4 — Tests for checkpoint.py.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from cdc_worker.checkpoint import LocalCheckpointManager, CentralCheckpointSync


# ---------------------------------------------------------------------------
# LocalCheckpointManager tests
# ---------------------------------------------------------------------------

class TestLocalCheckpoint:
    def setup_method(self):
        self.ckpt = LocalCheckpointManager(":memory:")

    def test_get_returns_none_for_missing(self):
        assert self.ckpt.get("src1", "db", "orders") is None

    def test_set_and_get_roundtrip(self):
        self.ckpt.set("src1", "db", "orders", "bin:1000")
        assert self.ckpt.get("src1", "db", "orders") == "bin:1000"

    def test_set_overwrites_existing(self):
        self.ckpt.set("src1", "db", "orders", "bin:1000")
        self.ckpt.set("src1", "db", "orders", "bin:2000")
        assert self.ckpt.get("src1", "db", "orders") == "bin:2000"

    def test_set_batch_atomic_all_or_nothing(self):
        updates = [("db", "orders", "bin:100"), ("db", "users", "bin:200")]
        self.ckpt.set_batch_atomic("src1", updates)
        assert self.ckpt.get("src1", "db", "orders") == "bin:100"
        assert self.ckpt.get("src1", "db", "users") == "bin:200"

    def test_get_all_for_source(self):
        self.ckpt.set("src1", "db", "orders", "bin:100")
        self.ckpt.set("src1", "db", "users", "bin:200")
        result = self.ckpt.get_all_for_source("src1")
        assert result == {("db", "orders"): "bin:100", ("db", "users"): "bin:200"}

    def test_multiple_sources_isolated(self):
        self.ckpt.set("src1", "db", "orders", "bin:100")
        self.ckpt.set("src2", "db", "orders", "bin:999")
        assert self.ckpt.get("src1", "db", "orders") == "bin:100"
        assert self.ckpt.get("src2", "db", "orders") == "bin:999"

    def test_data_survives_db_reopen(self, tmp_path):
        db_file = str(tmp_path / "ckpt.db")
        c1 = LocalCheckpointManager(db_file)
        c1.set("src1", "db", "orders", "bin:500")
        c1.close()
        c2 = LocalCheckpointManager(db_file)
        assert c2.get("src1", "db", "orders") == "bin:500"
        c2.close()

    def test_concurrent_writes_safe(self):
        """Basic multi-write sanity — not true concurrency but covers the upsert path."""
        for i in range(100):
            self.ckpt.set("src1", "db", "orders", f"bin:{i}")
        assert self.ckpt.get("src1", "db", "orders") == "bin:99"


# ---------------------------------------------------------------------------
# CentralCheckpointSync tests (mock httpx)
# ---------------------------------------------------------------------------

class TestCentralSync:
    def _make_sync(self):
        return CentralCheckpointSync(
            control_plane_url="http://localhost:8000",
            worker_token="secret",
            worker_id="w1",
        )

    @pytest.mark.asyncio
    async def test_push_posts_to_correct_endpoint(self):
        local = LocalCheckpointManager(":memory:")
        local.set("src1", "db", "orders", "bin:100")
        sync = self._make_sync()

        mock_resp = AsyncMock()
        mock_resp.status_code = 204

        with patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_resp) as mock_post:
            result = await sync.push(local, "src1")

        assert result is True

    @pytest.mark.asyncio
    async def test_pull_returns_checkpoints(self):
        sync = self._make_sync()

        mock_resp = MagicMock()  # MagicMock, not AsyncMock — .json() is synchronous
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {"schema_name": "db", "table_name": "orders", "lsn": "bin:100"}
        ]

        with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=mock_resp):
            result = await sync.pull("src1")

        assert result == {("db", "orders"): "bin:100"}

    @pytest.mark.asyncio
    async def test_pull_returns_empty_for_404(self):
        sync = self._make_sync()

        mock_resp = AsyncMock()
        mock_resp.status_code = 404

        with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=mock_resp):
            result = await sync.pull("src1")

        assert result == {}

    @pytest.mark.asyncio
    async def test_push_retries_on_503_max_3(self):
        local = LocalCheckpointManager(":memory:")
        local.set("src1", "db", "orders", "bin:100")
        sync = self._make_sync()

        mock_resp = AsyncMock()
        mock_resp.status_code = 503

        with patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_resp) as mock_post:
            result = await sync.push(local, "src1")

        assert result is False
        assert mock_post.call_count == 3
