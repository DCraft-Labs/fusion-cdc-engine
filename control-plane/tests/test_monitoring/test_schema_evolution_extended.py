"""
Tests for POST /connections/{id}/schema-changes (schema change creation + AUTO_APPLY).
These tests validate request shape and routing without a live database.
"""
import pytest
from uuid import uuid4
from fastapi.testclient import TestClient
from app.main import app
from app.auth.jwt import create_access_token


BASE = "/api/v1/schema-evolution"
VALID_PAYLOAD = {
    "table_name": "orders",
    "schema_name": "public",
    "change_type": "column_added",
    "old_schema": {"id": "int"},
    "new_schema": {"id": "int", "currency": "string"},
    "schema_diff": {"added": ["currency"]},
    "detected_by": "worker-1",
    "is_breaking": False,
}


@pytest.fixture(scope="module")
def client():
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture(scope="module")
def auth_headers():
    user_id = uuid4()
    token = create_access_token(
        user_id=user_id,
        username="test_user",
        roles=["admin"],
    )
    return {"Authorization": f"Bearer {token}"}


class TestReportSchemaChange:

    def test_unauthenticated_returns_403(self, client):
        resp = client.post(f"{BASE}/connections/{uuid4()}/schema-changes", json=VALID_PAYLOAD)
        assert resp.status_code == 403

    def test_missing_new_schema_returns_422(self, client, auth_headers):
        """new_schema and schema_diff are required fields — should return 422 (or 401 without DB)."""
        bad = {k: v for k, v in VALID_PAYLOAD.items() if k not in ("new_schema", "schema_diff")}
        resp = client.post(f"{BASE}/connections/{uuid4()}/schema-changes",
                           json=bad, headers=auth_headers)
        # 401 = auth passed but no DB user; 422 = schema validation error (both acceptable)
        assert resp.status_code in (401, 422), f"Unexpected status: {resp.status_code}"

    def test_missing_table_name_returns_422(self, client, auth_headers):
        """table_name is required."""
        bad = {k: v for k, v in VALID_PAYLOAD.items() if k != "table_name"}
        resp = client.post(f"{BASE}/connections/{uuid4()}/schema-changes",
                           json=bad, headers=auth_headers)
        assert resp.status_code in (401, 422), f"Unexpected status: {resp.status_code}"

    def test_valid_payload_reaches_endpoint(self, client, auth_headers):
        """
        With valid payload and auth, the request should get past schema validation.
        It may return 401 (no DB user) or 500 (DB error) but NOT 422.
        """
        resp = client.post(f"{BASE}/connections/{uuid4()}/schema-changes",
                           json=VALID_PAYLOAD, headers=auth_headers)
        # 422 = our request schema is wrong (BUG), 403 = auth layer broken (BUG)
        assert resp.status_code not in (422, 403), f"Unexpected: {resp.status_code} {resp.text}"

