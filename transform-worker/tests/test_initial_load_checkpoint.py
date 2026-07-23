"""v1.2.26 Task 1c tests: composite checkpoint key for multi-pod intra-table
parallelism. Verifies that ``InitialLoadTask._report_checkpoint`` sends
``chunk_seq`` (the partition index) and ``total_chunks`` (K) in the JSON body
to the control-plane, and that ``_get_last_checkpoint`` hits the 3-segment
composite-key URL ``/load-checkpoints/last/{connection_id}/{stream_id}/{chunk_seq}``
so each of the K concurrent pods reads/writes its own checkpoint row.

These are the wire-format invariants that make K pods safe: if two pods
reported under the same (connection_id, stream_id) key they would stomp each
other's ``last_pk`` and resume the wrong range on restart.
"""
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

_TW_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _TW_DIR not in sys.path:
    sys.path.insert(0, _TW_DIR)

from loader import InitialLoadTask  # noqa: E402


def _make_task():
    engine = MagicMock()
    engine.control_plane_url = "http://control-plane:8000"
    return InitialLoadTask(engine=engine, redis_client=MagicMock())


class TestReportCheckpointCompositeKey:
    def test_sends_chunk_seq_and_total_chunks(self):
        task = _make_task()
        with patch("requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            task._report_checkpoint(
                connection_id="11111111-1111-1111-1111-111111111111",
                stream_id="22222222-2222-2222-2222-222222222222",
                source_table="users",
                chunk_seq=3,
                rows_written=5000,
                last_pk=99999,
                state="running",
                total_chunks=8,
            )
        assert mock_post.called
        call = mock_post.call_args
        body = call.kwargs["json"]
        url = call.args[0]
        assert url.endswith("/api/v1/internal/load-checkpoints"), url
        # Composite-key fields must be present so each of the K pods writes
        # its own row (no stomping).
        assert body["chunk_seq"] == 3, body
        assert body["total_chunks"] == 8, body
        assert body["connection_id"] == "11111111-1111-1111-1111-111111111111"
        assert body["stream_id"] == "22222222-2222-2222-2222-222222222222"
        assert body["last_pk"] == 99999
        assert body["state"] == "running"

    def test_default_total_chunks_is_one(self):
        task = _make_task()
        with patch("requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            task._report_checkpoint(
                connection_id="11111111-1111-1111-1111-111111111111",
                stream_id="22222222-2222-2222-2222-222222222222",
                source_table="users",
                chunk_seq=0,
                rows_written=100,
                last_pk=42,
                state="done",
            )
        body = mock_post.call_args.kwargs["json"]
        assert body["total_chunks"] == 1
        assert body["state"] == "done"


class TestGetLastCheckpointCompositeKey:
    def test_hits_three_segment_url_with_chunk_seq(self):
        task = _make_task()
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(
                status_code=200,
                json=lambda: {"status": "running", "last_pk": 12345, "rows_written": 1000},
            )
            ckpt = task._get_last_checkpoint(
                connection_id="11111111-1111-1111-1111-111111111111",
                stream_id="22222222-2222-2222-2222-222222222222",
                chunk_seq=5,
            )
        url = mock_get.call_args.args[0]
        # Must include chunk_seq as the 3rd path segment (composite key).
        assert url.endswith(
            "/api/v1/internal/load-checkpoints/last/"
            "11111111-1111-1111-1111-111111111111/"
            "22222222-2222-2222-2222-222222222222/5"
        ), url
        assert ckpt is not None
        assert ckpt["last_pk"] == 12345

    def test_returns_none_on_404(self):
        task = _make_task()
        with patch("requests.get") as mock_get:
            resp = MagicMock()
            resp.status_code = 404
            resp.raise_for_status.side_effect = Exception("404")
            mock_get.return_value = resp
            ckpt = task._get_last_checkpoint(
                connection_id="11111111-1111-1111-1111-111111111111",
                stream_id="22222222-2222-2222-2222-222222222222",
                chunk_seq=0,
            )
        assert ckpt is None

    def test_returns_none_when_no_stream_id(self):
        task = _make_task()
        # No requests call should be made when stream_id is falsy.
        with patch("requests.get") as mock_get:
            ckpt = task._get_last_checkpoint(
                connection_id="11111111-1111-1111-1111-111111111111",
                stream_id=None,
                chunk_seq=0,
            )
        assert ckpt is None
        assert not mock_get.called
