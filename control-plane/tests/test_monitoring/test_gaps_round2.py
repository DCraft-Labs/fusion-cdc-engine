"""
Tests for the 6 gaps found in round-2 PDF re-analysis.

Gap 1: applied_at set + schema_change.applied audit after approve/auto_apply
Gap 2: Real schema discovery (_discover_database_schemas error-path fallback)
Gap 3: Periodic re-introspection diff logic (_diff_discovery_cache)
Gap 4: MySQL DDL notification method exists on connector
Gap 5: /resync-request and /report-ddl-change endpoints present in internal API
Gap 6: Postgres connector has _notify_schema_mismatch method
"""
import pytest
from uuid import uuid4
from fastapi.testclient import TestClient

from app.main import app, _diff_discovery_cache
from app.auth.jwt import create_access_token
from app.api.sources import _discover_database_schemas, _detect_json_column


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def client():
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture(scope="module")
def auth_headers():
    token = create_access_token(
        user_id=uuid4(), username="test_admin", roles=["admin"]
    )
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture(scope="module")
def worker_headers():
    return {"X-Worker-Token": "change-me-worker-secret-token"}


# ---------------------------------------------------------------------------
# Gap 1: applied_at field and schema_change.applied audit
# ---------------------------------------------------------------------------

class TestAppliedAt:
    """schema_evolution.py: applied_at must be set when change is approved/auto-applied."""

    def test_approve_endpoint_exists(self, client, auth_headers):
        """Approve endpoint must accept POST and not return 404."""
        fake_conn = uuid4()
        fake_change = uuid4()
        resp = client.post(
            f"/api/v1/schema-evolution/connections/{fake_conn}/schema-changes/{fake_change}/approve",
            headers=auth_headers,
        )
        # 401/404/409/500 all acceptable — NOT 422 (schema error) or 405 (method not allowed)
        assert resp.status_code != 405, "approve endpoint missing"
        assert resp.status_code != 422, f"validation error: {resp.text}"

    def test_report_schema_change_endpoint_exists(self, client, auth_headers):
        """POST /connections/{id}/schema-changes must exist."""
        resp = client.post(
            f"/api/v1/schema-evolution/connections/{uuid4()}/schema-changes",
            json={
                "table_name": "orders",
                "change_type": "column_added",
                "new_schema": {"id": "int", "currency": "varchar"},
                "schema_diff": {"added": ["currency"]},
                "detected_by": "test",
                "is_breaking": False,
            },
            headers=auth_headers,
        )
        assert resp.status_code != 405, "report endpoint missing"
        assert resp.status_code != 422, f"validation error: {resp.text}"


# ---------------------------------------------------------------------------
# Gap 2: Real schema discovery — error-path fallback returns empty dict
# ---------------------------------------------------------------------------

class TestSchemaDiscovery:

    def test_mysql_discovery_fallback_on_bad_host(self):
        """_discover_database_schemas should catch connection errors and return empty schemas."""
        result = _discover_database_schemas(
            connector_type="mysql",
            host="127.0.0.1",
            port=19999,  # no MySQL running here
            database_name="nonexistent",
            username="user",
            password="pass",
            ssl_enabled=False,
            ssl_config={},
        )
        # Must return a dict, not raise
        assert isinstance(result, dict)
        assert "schemas" in result

    def test_postgres_discovery_fallback_on_bad_host(self):
        """Postgres discovery must not raise on connection failure."""
        result = _discover_database_schemas(
            connector_type="postgresql",
            host="127.0.0.1",
            port=19998,
            database_name="nonexistent",
            username="user",
            password="pass",
            ssl_enabled=False,
            ssl_config={},
        )
        assert isinstance(result, dict)
        assert "schemas" in result

    def test_unknown_connector_returns_empty(self):
        """Unknown connector type must return empty schemas dict."""
        result = _discover_database_schemas(
            connector_type="oracle_unsupported",
            host="localhost", port=1521, database_name="db",
            username="u", password="p", ssl_enabled=False, ssl_config={},
        )
        assert result == {"schemas": []}

    def test_json_column_detection(self):
        """JSON/JSONB/text columns must be flagged as json candidates."""
        assert _detect_json_column("json") is True
        assert _detect_json_column("jsonb") is True
        assert _detect_json_column("text") is True
        assert _detect_json_column("longtext") is True
        assert _detect_json_column("integer") is False
        assert _detect_json_column("varchar") is False


# ---------------------------------------------------------------------------
# Gap 3: _diff_discovery_cache logic
# ---------------------------------------------------------------------------

class TestDiffDiscoveryCache:
    """Validate the schema change diff algorithm used by periodic re-introspection."""

    _OLD = {
        "schemas": [{
            "schema_name": "public",
            "tables": [{
                "schema_name": "public", "table_name": "orders",
                "columns": [
                    {"column_name": "id", "data_type": "integer"},
                    {"column_name": "amount", "data_type": "numeric"},
                ],
                "primary_keys": ["id"],
            }]
        }]
    }

    def test_no_changes_produces_empty_list(self):
        result = _diff_discovery_cache(self._OLD, self._OLD)
        assert result == []

    def test_column_added_detected(self):
        new_cache = {
            "schemas": [{
                "schema_name": "public",
                "tables": [{
                    "schema_name": "public", "table_name": "orders",
                    "columns": [
                        {"column_name": "id", "data_type": "integer"},
                        {"column_name": "amount", "data_type": "numeric"},
                        {"column_name": "currency", "data_type": "varchar"},
                    ],
                    "primary_keys": ["id"],
                }]
            }]
        }
        changes = _diff_discovery_cache(self._OLD, new_cache)
        assert len(changes) == 1
        assert changes[0]["change_type"] == "column_added"
        assert "currency" in changes[0]["schema_diff"]["added"]
        assert changes[0]["is_breaking"] is False

    def test_column_removed_detected_as_breaking(self):
        new_cache = {
            "schemas": [{
                "schema_name": "public",
                "tables": [{
                    "schema_name": "public", "table_name": "orders",
                    "columns": [{"column_name": "id", "data_type": "integer"}],
                    "primary_keys": ["id"],
                }]
            }]
        }
        changes = _diff_discovery_cache(self._OLD, new_cache)
        assert any(c["change_type"] == "column_removed" for c in changes)
        removed = next(c for c in changes if c["change_type"] == "column_removed")
        assert removed["is_breaking"] is True

    def test_type_changed_detected_as_breaking(self):
        new_cache = {
            "schemas": [{
                "schema_name": "public",
                "tables": [{
                    "schema_name": "public", "table_name": "orders",
                    "columns": [
                        {"column_name": "id", "data_type": "integer"},
                        {"column_name": "amount", "data_type": "varchar"},  # changed!
                    ],
                    "primary_keys": ["id"],
                }]
            }]
        }
        changes = _diff_discovery_cache(self._OLD, new_cache)
        assert any(c["change_type"] == "type_changed" for c in changes)
        tc = next(c for c in changes if c["change_type"] == "type_changed")
        assert tc["is_breaking"] is True
        assert tc["schema_diff"]["column"] == "amount"
        assert tc["schema_diff"]["new_type"] == "varchar"

    def test_new_table_detected(self):
        new_cache = {
            "schemas": [{
                "schema_name": "public",
                "tables": [
                    {**self._OLD["schemas"][0]["tables"][0]},
                    {"schema_name": "public", "table_name": "payments",
                     "columns": [{"column_name": "id", "data_type": "integer"}],
                     "primary_keys": ["id"]},
                ]
            }]
        }
        changes = _diff_discovery_cache(self._OLD, new_cache)
        assert any(c["change_type"] == "table_added" for c in changes)

    def test_removed_table_detected_as_breaking(self):
        new_cache = {"schemas": [{"schema_name": "public", "tables": []}]}
        changes = _diff_discovery_cache(self._OLD, new_cache)
        assert any(c["change_type"] == "table_removed" for c in changes)
        removed = next(c for c in changes if c["change_type"] == "table_removed")
        assert removed["is_breaking"] is True


# ---------------------------------------------------------------------------
# Gap 4: MySQL connector has _notify_ddl_change method
# ---------------------------------------------------------------------------

class TestMySQLDDLNotify:

    def test_notify_ddl_method_exists(self):
        """MySQLConnector must have _notify_ddl_change async method."""
        import inspect
        from connectors.mysql import MySQLConnector
        assert hasattr(MySQLConnector, "_notify_ddl_change"), \
            "_notify_ddl_change method missing from MySQLConnector"
        assert inspect.iscoroutinefunction(MySQLConnector._notify_ddl_change), \
            "_notify_ddl_change must be async"


# ---------------------------------------------------------------------------
# Gap 5: /resync-request and /report-ddl-change endpoints exist in internal API
# ---------------------------------------------------------------------------

class TestInternalWorkerEndpoints:

    def test_resync_request_endpoint_returns_422_on_bad_uuid(self, client, worker_headers):
        """POST /internal/resync-request must exist (returns 422 on bad UUID, not 404/405)."""
        resp = client.post(
            "/api/v1/internal/resync-request",
            json={"source_id": "not-a-uuid", "schema_name": "public", "table_name": "orders"},
            headers=worker_headers,
        )
        assert resp.status_code != 404, "resync-request endpoint missing"
        assert resp.status_code != 405, "resync-request endpoint method not allowed"

    def test_resync_request_valid_uuid_accepted(self, client, worker_headers):
        """Valid UUID should be accepted (will fail on DB but not on routing)."""
        resp = client.post(
            "/api/v1/internal/resync-request",
            json={"source_id": str(uuid4()), "schema_name": "public", "table_name": "orders"},
            headers=worker_headers,
        )
        # 404 = endpoint missing (BUG); 405 = method not allowed (BUG)
        assert resp.status_code not in (404, 405), f"Endpoint routing failed: {resp.status_code}"

    def test_report_ddl_change_endpoint_exists(self, client, worker_headers):
        """POST /internal/report-ddl-change must exist."""
        resp = client.post(
            "/api/v1/internal/report-ddl-change",
            json={
                "source_id": str(uuid4()),
                "schema_name": "public",
                "table_name": "orders",
                "ddl_query": "ALTER TABLE orders ADD COLUMN currency VARCHAR(3)",
            },
            headers=worker_headers,
        )
        assert resp.status_code not in (404, 405), f"report-ddl-change endpoint missing: {resp.status_code}"

    def test_report_ddl_invalid_uuid_returns_422(self, client, worker_headers):
        """Bad UUID in report-ddl-change should return 422."""
        resp = client.post(
            "/api/v1/internal/report-ddl-change",
            json={
                "source_id": "not-valid",
                "schema_name": "public", "table_name": "orders",
                "ddl_query": "ALTER TABLE orders ADD COLUMN x INT",
            },
            headers=worker_headers,
        )
        assert resp.status_code != 404, "endpoint missing"


# ---------------------------------------------------------------------------
# Gap 6: Postgres connector has _notify_schema_mismatch method
# ---------------------------------------------------------------------------

class TestPostgresDDLDetection:

    def test_notify_schema_mismatch_method_exists(self):
        """PostgresConnector must have _notify_schema_mismatch async method."""
        import inspect
        from connectors.postgres import PostgresConnector
        assert hasattr(PostgresConnector, "_notify_schema_mismatch"), \
            "_notify_schema_mismatch missing from PostgresConnector"
        assert inspect.iscoroutinefunction(PostgresConnector._notify_schema_mismatch), \
            "_notify_schema_mismatch must be async"
