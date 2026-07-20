"""Tests for Prometheus /metrics endpoint and request middleware (spec §3)."""

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture(scope="module")
def client():
    return TestClient(app, raise_server_exceptions=True)


# ============================================================================
# /metrics endpoint
# ============================================================================

class TestMetricsEndpoint:

    def test_metrics_endpoint_returns_200(self, client: TestClient):
        """GET /metrics returns 200 with plain-text body."""
        response = client.get("/metrics")
        assert response.status_code == 200

    def test_metrics_content_type_is_text_plain(self, client: TestClient):
        """Content-Type should start with 'text/plain'."""
        response = client.get("/metrics")
        assert response.headers["content-type"].startswith("text/plain")

    def test_metrics_body_contains_api_request_count(self, client: TestClient):
        """After making a request, api_request_count metric is present."""
        # trigger a known endpoint
        client.get("/health")
        resp = client.get("/metrics")
        assert resp.status_code == 200
        # Metric name may be absent if prometheus_client not installed — in that case
        # the body should contain the fallback comment.
        body = resp.text
        assert "api_request_count" in body or "prometheus_client not installed" in body

    def test_metrics_body_contains_api_request_duration(self, client: TestClient):
        """api_request_duration_seconds should appear in the metrics output."""
        client.get("/health")
        resp = client.get("/metrics")
        body = resp.text
        assert "api_request_duration_seconds" in body or "prometheus_client not installed" in body


# ============================================================================
# Trace-ID middleware
# ============================================================================

class TestTraceIdMiddleware:

    def test_trace_id_header_present_in_response(self, client: TestClient):
        """Every response should carry an X-Trace-Id header."""
        response = client.get("/health")
        assert "x-trace-id" in response.headers or "X-Trace-Id" in response.headers

    def test_trace_id_passed_through_when_provided(self, client: TestClient):
        """If X-Trace-Id is supplied, it should be echoed back unchanged."""
        custom_trace = "test-trace-abc123"
        response = client.get("/health", headers={"X-Trace-Id": custom_trace})
        trace_back = response.headers.get("x-trace-id") or response.headers.get("X-Trace-Id")
        assert trace_back == custom_trace

    def test_trace_id_auto_generated_when_absent(self, client: TestClient):
        """Without X-Trace-Id, a UUID is generated and returned."""
        import re
        response = client.get("/health")
        trace_back = response.headers.get("x-trace-id") or response.headers.get("X-Trace-Id", "")
        # Should match a UUID4 pattern (36 chars with dashes)
        assert re.match(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", trace_back), (
            f"Generated trace_id '{trace_back}' is not a UUID"
        )


# ============================================================================
# Health endpoints (sanity)
# ============================================================================

class TestHealthEndpoints:

    def test_health_returns_healthy(self, client: TestClient):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"

    def test_health_ready_returns_ready(self, client: TestClient):
        response = client.get("/health/ready")
        assert response.status_code == 200
        assert response.json()["status"] == "ready"

    def test_health_live_returns_alive(self, client: TestClient):
        response = client.get("/health/live")
        assert response.status_code == 200
        assert response.json()["status"] == "alive"
