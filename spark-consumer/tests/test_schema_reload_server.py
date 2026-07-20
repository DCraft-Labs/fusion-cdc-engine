"""
Tests for consumer/schema_reload_server.py — spec §3 schema-reload webhook.
"""
import json
import threading
import time
import urllib.request
import urllib.error

import pytest

from consumer.schema_reload_server import SchemaReloadServer


def _free_port() -> int:
    import socket
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture
def server():
    port = _free_port()
    srv = SchemaReloadServer(port=port)
    srv.start()
    time.sleep(0.1)   # let daemon thread bind
    yield srv, port
    srv.stop()


def _post(port, path, body: dict):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=data,
        method="POST",
        headers={"Content-Type": "application/json", "Content-Length": str(len(data))},
    )
    with urllib.request.urlopen(req, timeout=3) as resp:
        return resp.status, json.loads(resp.read())


def _get(port, path):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=3) as resp:
        return resp.status, json.loads(resp.read())


class TestSchemaReloadServer:

    def test_health_endpoint_returns_200(self, server):
        srv, port = server
        status, body = _get(port, "/health")
        assert status == 200
        assert body["status"] == "ok"

    def test_schema_reload_enqueues_connection(self, server):
        srv, port = server
        status, body = _post(port, "/internal/schema-reload", {
            "connection_id": "conn-abc-123",
            "change_id": "chg-xyz-456",
        })
        assert status == 200
        assert body["status"] == "queued"
        assert srv.has_pending_reload("conn-abc-123")

    def test_mark_reloaded_clears_pending(self, server):
        srv, port = server
        _post(port, "/internal/schema-reload", {"connection_id": "c1", "change_id": "x"})
        assert srv.has_pending_reload("c1")
        srv.mark_reloaded("c1")
        assert not srv.has_pending_reload("c1")

    def test_multiple_connections_independent(self, server):
        srv, port = server
        _post(port, "/internal/schema-reload", {"connection_id": "c1", "change_id": "a"})
        _post(port, "/internal/schema-reload", {"connection_id": "c2", "change_id": "b"})
        srv.mark_reloaded("c1")
        assert not srv.has_pending_reload("c1")
        assert srv.has_pending_reload("c2")

    def test_unknown_path_returns_404(self, server):
        srv, port = server
        try:
            _post(port, "/unknown/path", {})
            pytest.fail("Expected HTTP 404")
        except urllib.error.HTTPError as e:
            assert e.code == 404

    def test_missing_connection_id_returns_400(self, server):
        srv, port = server
        try:
            _post(port, "/internal/schema-reload", {"change_id": "abc"})
            pytest.fail("Expected HTTP 400")
        except urllib.error.HTTPError as e:
            assert e.code == 400

    def test_pending_reloads_returns_all_pending(self, server):
        srv, port = server
        _post(port, "/internal/schema-reload", {"connection_id": "c3", "change_id": "d"})
        _post(port, "/internal/schema-reload", {"connection_id": "c4", "change_id": "e"})
        pending = srv.pending_reloads()
        assert "c3" in pending
        assert "c4" in pending
