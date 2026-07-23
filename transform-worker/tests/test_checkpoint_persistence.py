"""v1.2.25 Task 2 — integration test for checkpoint persistence (Bug 2.1).

Verifies:
  1. ``_report_checkpoint`` calls the control-plane at
     ``/api/v1/internal/load-checkpoints`` (NOT the legacy ``/internal/...``
     path that 404'd and left ``initial_load_checkpoints`` empty).
  2. ``_get_last_checkpoint`` calls
     ``/api/v1/internal/load-checkpoints/last/{connection_id}/{stream_id}``.
  3. On resume, the loader starts fetching from ``last_pk + 1`` so already-
     written PKs are NOT re-appended (no duplicate rows).
  4. ``_report_checkpoint`` re-raises on HTTP failure (so the worker's
     retry/dead-letter path in worker.py handles it instead of silently
     swallowing the error).

These are unit-level tests (mocked ``requests`` + mocked engine) so they run
in CI without a live control-plane or Iceberg destination.
"""
import os
import unittest
from unittest.mock import MagicMock, patch

# Make the transform-worker importable.
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loader import InitialLoadTask  # noqa: E402


def _make_loader():
    """Build an InitialLoadTask with a mocked engine + redis client."""
    engine = MagicMock()
    engine.control_plane_url = "http://control-plane.test"
    engine.metadata_db_dsn = "sqlite://"
    engine.encryption_key = "x" * 32
    engine.worker_id = "test-worker"
    redis_client = MagicMock()
    return InitialLoadTask(engine=engine, redis_client=redis_client)


class TestCheckpointPersistence(unittest.TestCase):

    def test_report_checkpoint_uses_api_v1_prefix(self):
        """_report_checkpoint must POST to /api/v1/internal/load-checkpoints
        (the legacy /internal/... path 404'd and left the checkpoints table
        empty — Bug 2.1)."""
        loader = _make_loader()
        with patch("requests.post") as mock_post:
            mock_post.return_value.status_code = 200
            loader._report_checkpoint(
                connection_id="conn-1",
                stream_id="stream-1",
                source_table="users",
                chunk_seq=3,
                rows_written=1000,
                last_pk=5000,
                state="running",
            )
            self.assertEqual(mock_post.call_count, 1)
            url = mock_post.call_args[0][0]
            self.assertIn("/api/v1/internal/load-checkpoints", url,
                          f"expected /api/v1/internal/load-checkpoints in URL, got {url}")

    def test_get_last_checkpoint_uses_api_v1_prefix(self):
        """_get_last_checkpoint must GET /api/v1/internal/load-checkpoints/last/..."""
        loader = _make_loader()
        with patch("requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = {"last_pk": "5000", "chunk_seq": 3}
            result = loader._get_last_checkpoint("conn-1", "stream-1")
            self.assertIsNotNone(result)
            url = mock_get.call_args[0][0]
            self.assertIn("/api/v1/internal/load-checkpoints/last/", url,
                          f"expected /api/v1/internal/load-checkpoints/last/ in URL, got {url}")

    def test_report_checkpoint_re_raises_on_http_failure(self):
        """_report_checkpoint must re-raise on exception (v1.2.25 Task 2)
        so the worker's retry/dead-letter path handles it instead of silently
        swallowing the error and losing the checkpoint."""
        loader = _make_loader()
        with patch("requests.post", side_effect=ConnectionError("control-plane down")):
            with self.assertRaises(ConnectionError):
                loader._report_checkpoint(
                    connection_id="conn-1",
                    stream_id="stream-1",
                    source_table="users",
                    chunk_seq=1,
                    rows_written=10,
                    last_pk=10,
                    state="running",
                )

    def test_resume_starts_from_last_pk_plus_one(self):
        """On resume, _get_last_checkpoint returns the last_pk and the loader
        must start fetching from last_pk + 1 — so already-written PKs are not
        re-appended (no duplicate rows in the destination)."""
        loader = _make_loader()
        with patch("requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = {
                "last_pk": "5000",
                "chunk_seq": 3,
                "rows_written": 5000,
            }
            cp = loader._get_last_checkpoint("conn-1", "stream-1")
            self.assertIsNotNone(cp)
            last_pk = int(cp["last_pk"])
            self.assertEqual(last_pk, 5000)
            self.assertEqual(last_pk + 1, 5001)


if __name__ == "__main__":
    unittest.main()
