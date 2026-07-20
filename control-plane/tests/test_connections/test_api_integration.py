"""Integration tests for Connection Management API

Tests all 17 REST API endpoints with comprehensive coverage:
- CRUD operations (5 endpoints)
- Connection validation (1 endpoint)
- Schedule configuration (2 endpoints)
- Connection actions (4 endpoints)
- Statistics (1 endpoint)
- Stream management (4 endpoints)
"""

import pytest
from uuid import uuid4
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models.auth import User
from app.models.source_destination import Source, Destination
from app.models.connection import Connection, Stream
from app.models.connector import ConnectorDefinition


# ============================================================================
# Test List Connections (GET /)
# ============================================================================

class TestListConnections:
    """Test GET / - List connections with filters and pagination"""
    
    def test_list_connections_success(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test listing connections successfully"""
        response = client.get("/api/v1/connections/", headers=admin_headers)
        
        assert response.status_code == 200
        data = response.json()
        assert "connections" in data
        assert "total" in data
        assert len(data["connections"]) >= 1
        
        # Verify connection structure
        conn = data["connections"][0]
        assert "connection_id" in conn
        assert "connection_name" in conn
        assert "source" in conn
        assert "destination" in conn
    
    def test_list_connections_filter_by_status(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test filtering by status"""
        response = client.get(
            "/api/v1/connections/",
            headers=admin_headers,
            params={"status": "active"}
        )
        
        assert response.status_code == 200
        data = response.json()
        for conn in data["connections"]:
            assert conn["status"] == "active"
    
    def test_list_connections_filter_by_sync_mode(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test filtering by sync_mode"""
        response = client.get(
            "/api/v1/connections/",
            headers=admin_headers,
            params={"sync_mode": "cdc"}
        )
        
        assert response.status_code == 200
        data = response.json()
        for conn in data["connections"]:
            assert conn["sync_mode"] == "cdc"
    
    def test_list_connections_filter_by_source(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test filtering by source_id"""
        response = client.get(
            "/api/v1/connections/",
            headers=admin_headers,
            params={"source_id": str(sample_connection.source_id)}
        )
        
        assert response.status_code == 200
        data = response.json()
        for conn in data["connections"]:
            assert conn["source_id"] == str(sample_connection.source_id)
    
    def test_list_connections_search(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test search functionality"""
        response = client.get(
            "/api/v1/connections/",
            headers=admin_headers,
            params={"search": "Test CDC"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert len(data["connections"]) >= 1
    
    def test_list_connections_pagination(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test pagination"""
        response = client.get(
            "/api/v1/connections/",
            headers=admin_headers,
            params={"page": 1, "page_size": 10}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["page"] == 1
        assert data["page_size"] == 10
        assert "total_pages" in data


# ============================================================================
# Test Create Connection (POST /)
# ============================================================================

class TestCreateConnection:
    """Test POST / - Create new connection"""
    
    def test_create_connection_success(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_source: Source,
        sample_destination: Destination,
    ):
        """Test creating connection successfully"""
        payload = {
            "connection_name": "Test New Connection",
            "source_id": str(sample_source.source_id),
            "destination_id": str(sample_destination.destination_id),
            "sync_mode": "cdc",
            "sync_frequency": "*/30 * * * *",
            "status": "draft",
        }
        
        response = client.post("/api/v1/connections/", json=payload, headers=admin_headers)
        
        assert response.status_code == 201
        data = response.json()
        assert data["connection_name"] == payload["connection_name"]
        assert data["sync_mode"] == "cdc"
        assert data["status"] == "draft"
        assert "connection_id" in data
    
    def test_create_connection_with_streams(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_source: Source,
        sample_destination: Destination,
    ):
        """Test creating connection with streams"""
        payload = {
            "connection_name": "Test Connection with Streams",
            "source_id": str(sample_source.source_id),
            "destination_id": str(sample_destination.destination_id),
            "sync_mode": "cdc",
            "sync_frequency": "*/15 * * * *",
            "status": "draft",
            "streams": [
                {
                    "stream_name": "orders",
                    "source_table_name": "orders",
                    "source_schema_name": "public",
                    "destination_table_name": "orders",
                    "destination_schema_name": "public",
                    "sync_mode": "cdc",
                    "primary_keys": ["order_id"],
                    "is_enabled": True,
                }
            ],
        }
        
        response = client.post("/api/v1/connections/", json=payload, headers=admin_headers)
        
        assert response.status_code == 201
        data = response.json()
        assert len(data["streams"]) == 1
        assert data["streams"][0]["stream_name"] == "orders"
    
    def test_create_connection_duplicate_name(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_connection: Connection,
        sample_source: Source,
        sample_destination: Destination,
    ):
        """Test creating connection with duplicate name"""
        payload = {
            "connection_name": sample_connection.connection_name,
            "source_id": str(sample_source.source_id),
            "destination_id": str(sample_destination.destination_id),
            "sync_mode": "cdc",
            "sync_frequency": "*/30 * * * *",
        }
        
        response = client.post("/api/v1/connections/", json=payload, headers=admin_headers)
        
        assert response.status_code == 400
        assert "already exists" in response.json()["detail"]
    
    def test_create_connection_invalid_source(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test creating connection with invalid source"""
        payload = {
            "connection_name": "Test Invalid Source",
            "source_id": str(uuid4()),
            "destination_id": str(sample_destination.destination_id),
            "sync_mode": "cdc",
            "sync_frequency": "*/30 * * * *",
        }
        
        response = client.post("/api/v1/connections/", json=payload, headers=admin_headers)
        
        assert response.status_code == 404
        assert "Source not found" in response.json()["detail"]
    
    def test_create_connection_validation_failure(
        self,
        client: TestClient,
        admin_headers: dict,
        db_session: Session,
        sample_source: Source,
        sample_destination: Destination,
        sample_tenant: uuid4,
    ):
        """Test creating active connection that fails validation"""
        # Create source without CDC support
        from app.models.connector import ConnectorDefinition
        
        non_cdc_connector = ConnectorDefinition(
            connector_id=uuid4(),
            connector_name="Test Non-CDC Source",
            connector_type="Source",
            version="1.0.0",
            config_schema={},
            capabilities={"supports_cdc": False, "supported_sync_modes": ["full_refresh"]},
            status="active",
            sub_tenant_id=sample_tenant,
            bank_id=uuid4(),
        )
        db_session.add(non_cdc_connector)
        
        non_cdc_source = Source(
            source_id=uuid4(),
            source_name="Test Non-CDC Source",
            connector_definition_id=non_cdc_connector.connector_id,
            connector_version="1.0.0",
            host="test.example.com",
            port=3306,
            database_name="test_db",
            username="test",
            password_encrypted="encrypted",
            status="active",
            connection_test_status="success",
            sub_tenant_id=sample_tenant,
            bank_id=uuid4(),
        )
        db_session.add(non_cdc_source)
        db_session.commit()
        
        payload = {
            "connection_name": "Test Invalid CDC Connection",
            "source_id": str(non_cdc_source.source_id),
            "destination_id": str(sample_destination.destination_id),
            "sync_mode": "cdc",
            "sync_frequency": "*/30 * * * *",
            "status": "active",
        }
        
        response = client.post("/api/v1/connections/", json=payload, headers=admin_headers)
        
        assert response.status_code == 400
        assert "validation failed" in response.json()["detail"].lower()
    
    def test_create_connection_requires_permission(
        self,
        client: TestClient,
        user_headers: dict,
        sample_source: Source,
        sample_destination: Destination,
    ):
        """Test creating connection requires permission"""
        payload = {
            "connection_name": "Test Unauthorized",
            "source_id": str(sample_source.source_id),
            "destination_id": str(sample_destination.destination_id),
            "sync_mode": "cdc",
            "sync_frequency": "*/30 * * * *",
        }
        
        response = client.post("/api/v1/connections/", json=payload, headers=user_headers)
        
        assert response.status_code == 403


# ============================================================================
# Test Get Connection (GET /{id})
# ============================================================================

class TestGetConnection:
    """Test GET /{id} - Get connection details"""
    
    def test_get_connection_success(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test getting connection successfully"""
        response = client.get(
            f"/api/v1/connections/{sample_connection.connection_id}",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["connection_id"] == str(sample_connection.connection_id)
        assert data["connection_name"] == sample_connection.connection_name
        assert "source" in data
        assert "destination" in data
        assert "streams" in data
    
    def test_get_connection_not_found(
        self, client: TestClient, admin_headers: dict
    ):
        """Test getting non-existent connection"""
        response = client.get(
            f"/api/v1/connections/{uuid4()}",
            headers=admin_headers
        )
        
        assert response.status_code == 404
    
    def test_get_connection_requires_auth(
        self, client: TestClient, sample_connection: Connection
    ):
        """Test getting connection requires authentication"""
        response = client.get(
            f"/api/v1/connections/{sample_connection.connection_id}"
        )
        
        assert response.status_code == 401


# ============================================================================
# Test Update Connection (PATCH /{id})
# ============================================================================

class TestUpdateConnection:
    """Test PATCH /{id} - Update connection"""
    
    def test_update_connection_success(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test updating connection successfully"""
        payload = {
            "connection_name": "Updated Connection Name",
            "sync_frequency": "*/60 * * * *",
        }
        
        response = client.patch(
            f"/api/v1/connections/{sample_connection.connection_id}",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["connection_name"] == payload["connection_name"]
        assert data["sync_frequency"] == payload["sync_frequency"]
    
    def test_update_connection_partial(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test partial update"""
        payload = {"sync_enabled": False}
        
        response = client.patch(
            f"/api/v1/connections/{sample_connection.connection_id}",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["sync_enabled"] == False
    
    def test_update_connection_not_found(
        self, client: TestClient, admin_headers: dict
    ):
        """Test updating non-existent connection"""
        payload = {"connection_name": "New Name"}
        
        response = client.patch(
            f"/api/v1/connections/{uuid4()}",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 404
    
    def test_update_connection_requires_permission(
        self, client: TestClient, user_headers: dict, sample_connection: Connection
    ):
        """Test updating connection requires permission"""
        payload = {"connection_name": "Unauthorized Update"}
        
        response = client.patch(
            f"/api/v1/connections/{sample_connection.connection_id}",
            json=payload,
            headers=user_headers
        )
        
        assert response.status_code == 403


# ============================================================================
# Test Delete Connection (DELETE /{id})
# ============================================================================

class TestDeleteConnection:
    """Test DELETE /{id} - Delete connection"""
    
    def test_delete_connection_success(
        self,
        client: TestClient,
        admin_headers: dict,
        db_session: Session,
        sample_source: Source,
        sample_destination: Destination,
        sample_tenant: uuid4,
    ):
        """Test deleting draft connection successfully"""
        # Create draft connection
        connection = Connection(
            connection_id=uuid4(),
            connection_name="Test Delete Connection",
            source_id=sample_source.source_id,
            destination_id=sample_destination.destination_id,
            sync_mode="cdc",
            sync_frequency="manual",
            status="draft",
            sub_tenant_id=sample_tenant,
            bank_id=uuid4(),
        )
        db_session.add(connection)
        db_session.commit()
        
        response = client.delete(
            f"/api/v1/connections/{connection.connection_id}",
            headers=admin_headers
        )
        
        assert response.status_code == 204
    
    def test_delete_active_connection_without_force(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test deleting active connection without force fails"""
        response = client.delete(
            f"/api/v1/connections/{sample_connection.connection_id}",
            headers=admin_headers
        )
        
        assert response.status_code == 400
        assert "Cannot delete active" in response.json()["detail"]
    
    def test_delete_active_connection_with_force(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test deleting active connection with force succeeds"""
        response = client.delete(
            f"/api/v1/connections/{sample_connection.connection_id}",
            headers=admin_headers,
            params={"force": True}
        )
        
        assert response.status_code == 204
    
    def test_delete_connection_requires_permission(
        self, client: TestClient, user_headers: dict, sample_connection: Connection
    ):
        """Test deleting connection requires permission"""
        response = client.delete(
            f"/api/v1/connections/{sample_connection.connection_id}",
            headers=user_headers
        )
        
        assert response.status_code == 403


# ============================================================================
# Test Connection Validation (POST /validate)
# ============================================================================

class TestConnectionValidation:
    """Test POST /validate - Validate connection compatibility"""
    
    def test_validate_connection_success(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_source: Source,
        sample_destination: Destination,
    ):
        """Test validating compatible connection"""
        payload = {
            "source_id": str(sample_source.source_id),
            "destination_id": str(sample_destination.destination_id),
            "sync_mode": "cdc",
        }
        
        response = client.post(
            "/api/v1/connections/validate",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["is_valid"] == True
        assert data["source_compatible"] == True
        assert data["destination_compatible"] == True
    
    def test_validate_connection_cdc_not_supported(
        self,
        client: TestClient,
        admin_headers: dict,
        db_session: Session,
        sample_destination: Destination,
        sample_tenant: uuid4,
    ):
        """Test validating connection with unsupported CDC"""
        # Create source without CDC
        from app.models.connector import ConnectorDefinition
        
        connector = ConnectorDefinition(
            connector_id=uuid4(),
            connector_name="Test Non-CDC Connector",
            connector_type="Source",
            version="1.0.0",
            config_schema={},
            capabilities={"supports_cdc": False},
            status="active",
            sub_tenant_id=sample_tenant,
            bank_id=uuid4(),
        )
        db_session.add(connector)
        
        source = Source(
            source_id=uuid4(),
            source_name="Test Non-CDC Source",
            connector_definition_id=connector.connector_id,
            connector_version="1.0.0",
            host="test.example.com",
            port=3306,
            database_name="test_db",
            username="test",
            password_encrypted="encrypted",
            status="active",
            sub_tenant_id=sample_tenant,
            bank_id=uuid4(),
        )
        db_session.add(source)
        db_session.commit()
        
        payload = {
            "source_id": str(source.source_id),
            "destination_id": str(sample_destination.destination_id),
            "sync_mode": "cdc",
        }
        
        response = client.post(
            "/api/v1/connections/validate",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["is_valid"] == False
        assert data["sync_mode_supported"] == False
        assert len(data["issues"]) > 0
    
    def test_validate_connection_test_failed(
        self,
        client: TestClient,
        admin_headers: dict,
        db_session: Session,
        sample_destination: Destination,
        sample_tenant: uuid4,
        mysql_source_connector: ConnectorDefinition,
    ):
        """Test validating connection with failed connection test"""
        # Create source with failed test
        source = Source(
            source_id=uuid4(),
            source_name="Test Failed Source",
            connector_definition_id=mysql_source_connector.connector_id,
            connector_version="1.0.0",
            host="test.example.com",
            port=3306,
            database_name="test_db",
            username="test",
            password_encrypted="encrypted",
            status="active",
            connection_test_status="failed",
            sub_tenant_id=sample_tenant,
            bank_id=uuid4(),
        )
        db_session.add(source)
        db_session.commit()
        
        payload = {
            "source_id": str(source.source_id),
            "destination_id": str(sample_destination.destination_id),
            "sync_mode": "cdc",
        }
        
        response = client.post(
            "/api/v1/connections/validate",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["is_valid"] == False
        assert len(data["issues"]) > 0


# ============================================================================
# Test Schedule Configuration (POST/GET /{id}/schedule)
# ============================================================================

class TestScheduleConfiguration:
    """Test schedule configuration endpoints"""
    
    def test_configure_schedule(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test configuring connection schedule"""
        payload = {
            "sync_frequency": "0 */2 * * *",
            "sync_enabled": True,
            "timezone": "America/New_York",
        }
        
        response = client.post(
            f"/api/v1/connections/{sample_connection.connection_id}/schedule",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["schedule_config"]["sync_frequency"] == payload["sync_frequency"]
        assert data["schedule_config"]["timezone"] == payload["timezone"]
    
    def test_get_schedule(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test getting connection schedule"""
        response = client.get(
            f"/api/v1/connections/{sample_connection.connection_id}/schedule",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "schedule_config" in data
        assert "sync_frequency" in data["schedule_config"]
    
    def test_update_schedule_frequency(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test updating schedule frequency"""
        payload = {"sync_frequency": "0 0 * * *"}
        
        response = client.post(
            f"/api/v1/connections/{sample_connection.connection_id}/schedule",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["schedule_config"]["sync_frequency"] == "0 0 * * *"
    
    def test_configure_manual_mode(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test configuring manual sync mode"""
        payload = {"sync_frequency": "manual", "sync_enabled": False}
        
        response = client.post(
            f"/api/v1/connections/{sample_connection.connection_id}/schedule",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["schedule_config"]["sync_frequency"] == "manual"


# ============================================================================
# Test Connection Actions (POST /{id}/activate, pause, resume, trigger-sync)
# ============================================================================

class TestConnectionActions:
    """Test connection action endpoints"""
    
    def test_activate_connection_with_validation(
        self,
        client: TestClient,
        admin_headers: dict,
        db_session: Session,
        sample_source: Source,
        sample_destination: Destination,
        sample_tenant: uuid4,
    ):
        """Test activating connection with validation"""
        # Create draft connection
        connection = Connection(
            connection_id=uuid4(),
            connection_name="Test Activate Connection",
            source_id=sample_source.source_id,
            destination_id=sample_destination.destination_id,
            sync_mode="cdc",
            sync_frequency="*/30 * * * *",
            status="draft",
            sub_tenant_id=sample_tenant,
            bank_id=uuid4(),
        )
        db_session.add(connection)
        db_session.commit()
        
        payload = {"validate_first": True, "skip_initial_sync": False}
        
        response = client.post(
            f"/api/v1/connections/{connection.connection_id}/activate",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "active"
        assert "validation_result" in data
    
    def test_activate_already_active(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test activating already active connection"""
        payload = {"validate_first": False}
        
        response = client.post(
            f"/api/v1/connections/{sample_connection.connection_id}/activate",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "already active" in data["message"].lower()
    
    def test_pause_connection(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test pausing active connection"""
        response = client.post(
            f"/api/v1/connections/{sample_connection.connection_id}/pause",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "paused"
    
    def test_resume_connection(
        self,
        client: TestClient,
        admin_headers: dict,
        db_session: Session,
        sample_source: Source,
        sample_destination: Destination,
        sample_tenant: uuid4,
    ):
        """Test resuming paused connection"""
        # Create paused connection
        connection = Connection(
            connection_id=uuid4(),
            connection_name="Test Paused Connection",
            source_id=sample_source.source_id,
            destination_id=sample_destination.destination_id,
            sync_mode="cdc",
            sync_frequency="*/30 * * * *",
            status="paused",
            sub_tenant_id=sample_tenant,
            bank_id=uuid4(),
        )
        db_session.add(connection)
        db_session.commit()
        
        response = client.post(
            f"/api/v1/connections/{connection.connection_id}/resume",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "active"
    
    def test_trigger_manual_sync(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test triggering manual sync"""
        payload = {"full_refresh": False}
        
        response = client.post(
            f"/api/v1/connections/{sample_connection.connection_id}/trigger-sync",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["sync_triggered"] == True
        assert "triggered_at" in data
    
    def test_trigger_sync_with_options(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection, sample_stream: Stream
    ):
        """Test triggering sync with full refresh and stream selection"""
        payload = {
            "full_refresh": True,
            "selected_streams": [str(sample_stream.stream_id)],
        }
        
        response = client.post(
            f"/api/v1/connections/{sample_connection.connection_id}/trigger-sync",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["sync_triggered"] == True
    
    def test_action_invalid_status_transition(
        self,
        client: TestClient,
        admin_headers: dict,
        db_session: Session,
        sample_source: Source,
        sample_destination: Destination,
        sample_tenant: uuid4,
    ):
        """Test invalid status transition"""
        # Create draft connection
        connection = Connection(
            connection_id=uuid4(),
            connection_name="Test Draft Connection",
            source_id=sample_source.source_id,
            destination_id=sample_destination.destination_id,
            sync_mode="cdc",
            sync_frequency="manual",
            status="draft",
            sub_tenant_id=sample_tenant,
            bank_id=uuid4(),
        )
        db_session.add(connection)
        db_session.commit()
        
        # Try to pause draft connection
        response = client.post(
            f"/api/v1/connections/{connection.connection_id}/pause",
            headers=admin_headers
        )
        
        assert response.status_code == 400


# ============================================================================
# Test Statistics (GET /{id}/stats)
# ============================================================================

class TestStatistics:
    """Test GET /{id}/stats - Get connection statistics"""
    
    def test_get_connection_stats(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test getting connection statistics"""
        response = client.get(
            f"/api/v1/connections/{sample_connection.connection_id}/stats",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["connection_id"] == str(sample_connection.connection_id)
        assert "sync_count" in data
        assert "data_volume" in data
        assert "performance" in data
        assert "error_stats" in data


# ============================================================================
# Test Stream Management (POST/GET/PATCH/DELETE /{id}/streams)
# ============================================================================

class TestStreamManagement:
    """Test stream management endpoints"""
    
    def test_add_stream(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test adding stream to connection"""
        payload = {
            "stream_name": "Test products stream",
            "source_table_name": "products",
            "source_schema_name": "public",
            "destination_table_name": "products",
            "destination_schema_name": "public",
            "sync_mode": "cdc",
            "primary_keys": ["product_id"],
            "is_enabled": True,
        }
        
        response = client.post(
            f"/api/v1/connections/{sample_connection.connection_id}/streams",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 201
        data = response.json()
        assert data["stream_name"] == payload["stream_name"]
        assert data["source_table_name"] == payload["source_table_name"]
    
    def test_list_streams(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection, sample_stream: Stream
    ):
        """Test listing connection streams"""
        response = client.get(
            f"/api/v1/connections/{sample_connection.connection_id}/streams",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 1
    
    def test_update_stream(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection, sample_stream: Stream
    ):
        """Test updating stream configuration"""
        payload = {
            "is_enabled": False,
            "sync_mode": "full_refresh",
        }
        
        response = client.patch(
            f"/api/v1/connections/{sample_connection.connection_id}/streams/{sample_stream.stream_id}",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["is_enabled"] == False
        assert data["sync_mode"] == "full_refresh"
    
    def test_delete_stream(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection, sample_stream: Stream
    ):
        """Test deleting stream"""
        response = client.delete(
            f"/api/v1/connections/{sample_connection.connection_id}/streams/{sample_stream.stream_id}",
            headers=admin_headers
        )
        
        assert response.status_code == 204
    
    def test_stream_not_found(
        self, client: TestClient, admin_headers: dict, sample_connection: Connection
    ):
        """Test accessing non-existent stream"""
        response = client.patch(
            f"/api/v1/connections/{sample_connection.connection_id}/streams/{uuid4()}",
            json={"is_enabled": False},
            headers=admin_headers
        )
        
        assert response.status_code == 404
