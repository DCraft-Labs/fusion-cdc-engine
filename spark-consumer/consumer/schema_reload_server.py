"""
Spark Consumer — Schema Reload Webhook Server (spec §3)

Runs a lightweight HTTP server in a daemon thread so the Spark Structured
Streaming job can receive schema-reload notifications from the control plane
without needing a separate deployment.

The control plane POSTs to:
    POST {SPARK_CONSUMER_URL}/internal/schema-reload
    Body: {"connection_id": "<uuid>", "change_id": "<uuid>"}

The server sets a threading.Event per connection_id so the Spark foreachBatch
callback (or a periodic probe) can call ``schema_reload_server.has_pending_reload()``
and act accordingly.

Usage
-----
    from consumer.schema_reload_server import SchemaReloadServer

    server = SchemaReloadServer(port=int(os.getenv("SCHEMA_RELOAD_PORT", "8080")))
    server.start()                         # starts daemon thread
    ...
    if server.has_pending_reload(connection_id):
        reload_transform_spec(connection_id)
        server.mark_reloaded(connection_id)
"""

from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict, Set

log = logging.getLogger(__name__)


class SchemaReloadServer:
    """
    Thread-safe lightweight HTTP webhook receiver.

    The server runs in a daemon thread; it does not block the Spark job.
    """

    def __init__(self, port: int = 8080) -> None:
        self._port = port
        self._pending: Set[str] = set()   # connection_ids that need reloading
        self._lock = threading.Lock()
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Public API consumed by the Spark foreachBatch handler
    # ------------------------------------------------------------------

    def has_pending_reload(self, connection_id: str) -> bool:
        """Return True if the control plane requested a schema reload for this connection."""
        with self._lock:
            return connection_id in self._pending

    def mark_reloaded(self, connection_id: str) -> None:
        """Call this after the Spark job has successfully reloaded the spec."""
        with self._lock:
            self._pending.discard(connection_id)

    def pending_reloads(self) -> list[str]:
        """Return a snapshot of all pending connection IDs."""
        with self._lock:
            return list(self._pending)

    # ------------------------------------------------------------------
    # Internal: enqueue (called by the HTTP handler)
    # ------------------------------------------------------------------

    def _enqueue(self, connection_id: str, change_id: str) -> None:
        with self._lock:
            self._pending.add(connection_id)
        log.info(
            "Schema reload enqueued: connection_id=%s change_id=%s",
            connection_id,
            change_id,
        )

    # ------------------------------------------------------------------
    # Daemon thread lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the webhook HTTP server in a daemon thread."""
        server_ref = self

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                if self.path != "/internal/schema-reload":
                    self.send_response(404)
                    self.end_headers()
                    return

                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length > 0 else b"{}"
                try:
                    data = json.loads(body)
                    connection_id = str(data.get("connection_id", ""))
                    change_id = str(data.get("change_id", ""))
                    if not connection_id:
                        raise ValueError("connection_id is required")
                    server_ref._enqueue(connection_id, change_id)
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(b'{"status":"queued"}')
                except Exception as exc:
                    log.warning("Schema reload webhook error: %s", exc)
                    self.send_response(400)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": str(exc)}).encode())

            def do_GET(self):  # noqa: N802
                if self.path == "/health":
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(b'{"status":"ok"}')
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, fmt, *args):
                log.debug("SchemaReloadServer: " + fmt, *args)

        self._server = HTTPServer(("0.0.0.0", self._port), _Handler)
        self._thread = threading.Thread(
            target=self._server.serve_forever, daemon=True, name="schema-reload-server"
        )
        self._thread.start()
        log.info("Schema reload webhook server listening on :%d", self._port)

    def stop(self) -> None:
        """Gracefully shut down the HTTP server."""
        if self._server:
            self._server.shutdown()
            self._server = None
        log.info("Schema reload webhook server stopped")
