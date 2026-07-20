"""Integration tests for Destination API endpoints"""

import pytest
from uuid import uuid4
from fastapi import status
from fastapi.testclient import TestClient

from app.models.source_destination import Destination
from app.models.connector import ConnectorDefinition


# ===========================
# Test List Destinations
# ===========================

class TestListDestinations:
    """Tests for listing destinations"""
    
    def test_list_destinations_success(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test listing destinations"""
        response = client.get("/api/v1/destinations", headers=admin_headers)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "destinations" in data
        assert "total" in data
        assert data["total"] >= 1
        assert len(data["destinations"]) >= 1
        
        # Check destination structure
        dest = data["destinations"][0]
        assert "destination_id" in dest
        assert "destination_name" in dest
        assert "status" in dest
        assert dest["password"] == "********"  # Password should be masked
    
    def test_list_destinations_filter_by_status(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test filtering destinations by status"""
        response = client.get(
            "/api/v1/destinations?status=active",
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        
        # All returned destinations should have active status
        for dest in data["destinations"]:
            assert dest["status"] == "active"
    
    def test_list_destinations_filter_by_connector(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
        sample_connector_definition: ConnectorDefinition,
    ):
        """Test filtering destinations by connector definition"""
        response = client.get(
            f"/api/v1/destinations?connector_definition_id={sample_connector_definition.connector_id}",
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        
        # All returned destinations should use the specified connector
        for dest in data["destinations"]:
            assert dest["connector_definition_id"] == str(sample_connector_definition.connector_id)
    
    def test_list_destinations_search(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test searching destinations by name"""
        response = client.get(
            "/api/v1/destinations?search=Test",
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        
        # All returned destinations should match search
        for dest in data["destinations"]:
            assert "Test" in dest["destination_name"]
    
    def test_list_destinations_pagination(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test pagination in destination list"""
        response = client.get(
            "/api/v1/destinations?page=1&page_size=10",
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["page"] == 1
        assert data["page_size"] == 10
        assert "total_pages" in data
    
    def test_list_destinations_requires_auth(self, client: TestClient):
        """Test that listing destinations requires authentication"""
        response = client.get("/api/v1/destinations")
        assert response.status_code == status.HTTP_401_UNAUTHORIZED


# ===========================
# Test Create Destination
# ===========================

class TestCreateDestination:
    """Tests for creating destinations"""
    
    def test_create_destination_success(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_connector_definition: ConnectorDefinition,
    ):
        """Test creating a new destination"""
        payload = {
            "destination_name": "Test New PostgreSQL Dest",
            "connector_definition_id": str(sample_connector_definition.connector_id),
            "connector_version": "1.0.0",
            "host": "new-postgres.example.com",
            "port": 5432,
            "database_name": "new_db",
            "schema_name": "public",
            "username": "new_user",
            "password": "new_password",
            "ssl_enabled": True,
            "ssl_config": {"verify": True},
            "config": {},
            "status": "draft",
        }
        
        response = client.post(
            "/api/v1/destinations",
            json=payload,
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        assert data["destination_name"] == payload["destination_name"]
        assert data["host"] == payload["host"]
        assert data["password"] == "********"  # Password should be masked
        assert data["status"] == "draft"
        assert "destination_id" in data
    
    def test_create_s3_destination_success(
        self,
        client: TestClient,
        admin_headers: dict,
        s3_connector_definition: ConnectorDefinition,
    ):
        """Test creating an S3 destination"""
        payload = {
            "destination_name": "Test New S3 Dest",
            "connector_definition_id": str(s3_connector_definition.connector_id),
            "connector_version": "1.0.0",
            "bucket_name": "new-test-bucket",
            "region": "us-west-2",
            "path_prefix": "output/",
            "output_format": "parquet",
            "compression": "snappy",
            "config": {},
            "status": "draft",
        }
        
        response = client.post(
            "/api/v1/destinations",
            json=payload,
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        assert data["destination_name"] == payload["destination_name"]
        assert data["bucket_name"] == payload["bucket_name"]
        assert data["output_format"] == "parquet"
    
    def test_create_destination_duplicate_name(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
        sample_connector_definition: ConnectorDefinition,
    ):
        """Test creating destination with duplicate name fails"""
        payload = {
            "destination_name": sample_destination.destination_name,
            "connector_definition_id": str(sample_connector_definition.connector_id),
            "connector_version": "1.0.0",
            "host": "another-host.example.com",
            "port": 5432,
            "database_name": "another_db",
            "username": "another_user",
            "password": "another_password",
        }
        
        response = client.post(
            "/api/v1/destinations",
            json=payload,
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "already exists" in response.json()["detail"]
    
    def test_create_destination_invalid_connector(
        self,
        client: TestClient,
        admin_headers: dict,
    ):
        """Test creating destination with invalid connector fails"""
        payload = {
            "destination_name": "Test Invalid Dest",
            "connector_definition_id": str(uuid4()),
            "connector_version": "1.0.0",
            "host": "test.example.com",
            "port": 5432,
            "database_name": "test_db",
            "username": "test_user",
            "password": "test_password",
        }
        
        response = client.post(
            "/api/v1/destinations",
            json=payload,
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_create_destination_requires_permission(
        self,
        client: TestClient,
        user_headers: dict,
        sample_connector_definition: ConnectorDefinition,
    ):
        """Test creating destination requires permission"""
        payload = {
            "destination_name": "Test Unauthorized Dest",
            "connector_definition_id": str(sample_connector_definition.connector_id),
            "connector_version": "1.0.0",
            "host": "test.example.com",
            "port": 5432,
            "database_name": "test_db",
            "username": "test_user",
            "password": "test_password",
        }
        
        response = client.post(
            "/api/v1/destinations",
            json=payload,
            headers=user_headers,
        )
        
        assert response.status_code == status.HTTP_403_FORBIDDEN


# ===========================
# Test Get Destination
# ===========================

class TestGetDestination:
    """Tests for getting destination details"""
    
    def test_get_destination_success(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test getting destination by ID"""
        response = client.get(
            f"/api/v1/destinations/{sample_destination.destination_id}",
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["destination_id"] == str(sample_destination.destination_id)
        assert data["destination_name"] == sample_destination.destination_name
        assert data["password"] == "********"  # Password should be masked
    
    def test_get_destination_not_found(
        self,
        client: TestClient,
        admin_headers: dict,
    ):
        """Test getting non-existent destination returns 404"""
        response = client.get(
            f"/api/v1/destinations/{uuid4()}",
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_get_destination_requires_auth(
        self,
        client: TestClient,
        sample_destination: Destination,
    ):
        """Test getting destination requires authentication"""
        response = client.get(f"/api/v1/destinations/{sample_destination.destination_id}")
        assert response.status_code == status.HTTP_401_UNAUTHORIZED


# ===========================
# Test Update Destination
# ===========================

class TestUpdateDestination:
    """Tests for updating destinations"""
    
    def test_update_destination_success(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test updating destination"""
        payload = {
            "host": "updated-postgres.example.com",
            "port": 5433,
            "status": "active",
        }
        
        response = client.patch(
            f"/api/v1/destinations/{sample_destination.destination_id}",
            json=payload,
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["host"] == "updated-postgres.example.com"
        assert data["port"] == 5433
        assert data["status"] == "active"
    
    def test_update_destination_partial(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test partial update of destination"""
        payload = {"status": "inactive"}
        
        response = client.patch(
            f"/api/v1/destinations/{sample_destination.destination_id}",
            json=payload,
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["status"] == "inactive"
        # Other fields should remain unchanged
        assert data["host"] == sample_destination.host
    
    def test_update_destination_not_found(
        self,
        client: TestClient,
        admin_headers: dict,
    ):
        """Test updating non-existent destination returns 404"""
        payload = {"status": "inactive"}
        
        response = client.patch(
            f"/api/v1/destinations/{uuid4()}",
            json=payload,
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_update_destination_requires_permission(
        self,
        client: TestClient,
        user_headers: dict,
        sample_destination: Destination,
    ):
        """Test updating destination requires permission"""
        payload = {"status": "inactive"}
        
        response = client.patch(
            f"/api/v1/destinations/{sample_destination.destination_id}",
            json=payload,
            headers=user_headers,
        )
        
        assert response.status_code == status.HTTP_403_FORBIDDEN


# ===========================
# Test Delete Destination
# ===========================

class TestDeleteDestination:
    """Tests for deleting destinations"""
    
    def test_delete_destination_success(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test deleting destination"""
        response = client.delete(
            f"/api/v1/destinations/{sample_destination.destination_id}",
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_204_NO_CONTENT
        
        # Verify destination is soft deleted
        get_response = client.get(
            f"/api/v1/destinations/{sample_destination.destination_id}",
            headers=admin_headers,
        )
        assert get_response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_delete_destination_not_found(
        self,
        client: TestClient,
        admin_headers: dict,
    ):
        """Test deleting non-existent destination returns 404"""
        response = client.delete(
            f"/api/v1/destinations/{uuid4()}",
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_delete_destination_requires_permission(
        self,
        client: TestClient,
        user_headers: dict,
        sample_destination: Destination,
    ):
        """Test deleting destination requires permission"""
        response = client.delete(
            f"/api/v1/destinations/{sample_destination.destination_id}",
            headers=user_headers,
        )
        
        assert response.status_code == status.HTTP_403_FORBIDDEN


# ===========================
# Test Connection Testing
# ===========================

class TestConnectionTesting:
    """Tests for destination connection testing"""
    
    def test_test_connection_success(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test destination connection testing"""
        response = client.post(
            f"/api/v1/destinations/{sample_destination.destination_id}/test-connection",
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "status" in data
        assert "message" in data
        assert "latency_ms" in data
        assert data["status"] in ["success", "failed"]
    
    def test_test_connection_with_override(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test connection with override parameters"""
        payload = {
            "host": "override-host.example.com",
            "port": 5433,
            "username": "override_user",
            "password": "override_password",
        }
        
        response = client.post(
            f"/api/v1/destinations/{sample_destination.destination_id}/test-connection",
            json=payload,
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "status" in data
    
    def test_test_connection_not_found(
        self,
        client: TestClient,
        admin_headers: dict,
    ):
        """Test connection testing for non-existent destination"""
        response = client.post(
            f"/api/v1/destinations/{uuid4()}/test-connection",
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_validate_write_permissions(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test validating write permissions"""
        response = client.post(
            f"/api/v1/destinations/{sample_destination.destination_id}/validate-write-permissions",
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "has_write_permissions" in data
        assert "message" in data


# ===========================
# Test Write Mode Configuration
# ===========================

class TestWriteModeConfiguration:
    """Tests for write mode configuration"""
    
    def test_configure_write_mode_append(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test configuring append write mode"""
        payload = {
            "write_mode": "append",
            "create_table_if_not_exists": True,
        }
        
        response = client.post(
            f"/api/v1/destinations/{sample_destination.destination_id}/write-mode",
            json=payload,
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["write_mode_config"]["write_mode"] == "append"
    
    def test_configure_write_mode_upsert(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test configuring upsert write mode"""
        payload = {
            "write_mode": "upsert",
            "primary_keys": ["id", "tenant_id"],
            "update_columns": ["name", "email", "updated_at"],
        }
        
        response = client.post(
            f"/api/v1/destinations/{sample_destination.destination_id}/write-mode",
            json=payload,
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["write_mode_config"]["write_mode"] == "upsert"
        assert data["write_mode_config"]["primary_keys"] == ["id", "tenant_id"]
    
    def test_configure_write_mode_upsert_without_keys(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test configuring upsert without primary keys fails"""
        payload = {
            "write_mode": "upsert",
        }
        
        response = client.post(
            f"/api/v1/destinations/{sample_destination.destination_id}/write-mode",
            json=payload,
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
    
    def test_get_write_mode_config(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test getting write mode configuration"""
        # First configure write mode
        payload = {
            "write_mode": "replace",
            "truncate_before_load": True,
        }
        client.post(
            f"/api/v1/destinations/{sample_destination.destination_id}/write-mode",
            json=payload,
            headers=admin_headers,
        )
        
        # Then retrieve it
        response = client.get(
            f"/api/v1/destinations/{sample_destination.destination_id}/write-mode",
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["write_mode_config"]["write_mode"] == "replace"
    
    def test_configure_write_mode_requires_permission(
        self,
        client: TestClient,
        user_headers: dict,
        sample_destination: Destination,
    ):
        """Test write mode configuration requires permission"""
        payload = {
            "write_mode": "append",
        }
        
        response = client.post(
            f"/api/v1/destinations/{sample_destination.destination_id}/write-mode",
            json=payload,
            headers=user_headers,
        )
        
        assert response.status_code == status.HTTP_403_FORBIDDEN


# ===========================
# Test Schema Mapping
# ===========================

class TestSchemaMapping:
    """Tests for schema mapping configuration"""
    
    def test_configure_schema_mapping(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test configuring schema mapping"""
        payload = {
            "table_name": "users",
            "column_mappings": [
                {
                    "source_column": "user_id",
                    "destination_column": "id",
                    "data_type": "bigint",
                },
                {
                    "source_column": "user_name",
                    "destination_column": "name",
                    "data_type": "varchar(255)",
                },
                {
                    "source_column": "user_email",
                    "destination_column": "email",
                    "data_type": "varchar(255)",
                },
            ],
            "enable_auto_mapping": True,
            "case_sensitive": False,
        }
        
        response = client.post(
            f"/api/v1/destinations/{sample_destination.destination_id}/schema-mapping",
            json=payload,
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["table_name"] == "users"
        assert data["total_mappings"] == 3
        assert len(data["column_mappings"]) == 3
    
    def test_get_schema_mapping(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test getting schema mapping"""
        # First configure mapping
        payload = {
            "table_name": "products",
            "column_mappings": [
                {
                    "source_column": "product_id",
                    "destination_column": "id",
                },
                {
                    "source_column": "product_name",
                    "destination_column": "name",
                },
            ],
        }
        client.post(
            f"/api/v1/destinations/{sample_destination.destination_id}/schema-mapping",
            json=payload,
            headers=admin_headers,
        )
        
        # Then retrieve it
        response = client.get(
            f"/api/v1/destinations/{sample_destination.destination_id}/schema-mapping/products",
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["table_name"] == "products"
        assert data["total_mappings"] == 2
    
    def test_get_schema_mapping_not_found(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test getting non-existent schema mapping returns 404"""
        response = client.get(
            f"/api/v1/destinations/{sample_destination.destination_id}/schema-mapping/nonexistent",
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_404_NOT_FOUND


# ===========================
# Test Batch Settings
# ===========================

class TestBatchSettings:
    """Tests for batch settings configuration"""
    
    def test_configure_batch_settings(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test configuring batch settings"""
        payload = {
            "batch_size": 5000,
            "batch_timeout_seconds": 600,
            "max_parallel_batches": 10,
            "max_retries": 5,
            "retry_delay_seconds": 120,
            "continue_on_error": False,
            "error_threshold_percent": 10.0,
            "enable_compression": True,
            "buffer_memory_mb": 512,
        }
        
        response = client.post(
            f"/api/v1/destinations/{sample_destination.destination_id}/batch-settings",
            json=payload,
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["batch_settings"]["batch_size"] == 5000
        assert data["batch_settings"]["max_parallel_batches"] == 10
    
    def test_get_batch_settings(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test getting batch settings"""
        response = client.get(
            f"/api/v1/destinations/{sample_destination.destination_id}/batch-settings",
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "batch_settings" in data
        assert "batch_size" in data["batch_settings"]
    
    def test_configure_batch_settings_requires_permission(
        self,
        client: TestClient,
        user_headers: dict,
        sample_destination: Destination,
    ):
        """Test batch settings configuration requires permission"""
        payload = {
            "batch_size": 2000,
        }
        
        response = client.post(
            f"/api/v1/destinations/{sample_destination.destination_id}/batch-settings",
            json=payload,
            headers=user_headers,
        )
        
        assert response.status_code == status.HTTP_403_FORBIDDEN


# ===========================
# Test Destination Statistics
# ===========================

class TestDestinationStatistics:
    """Tests for destination statistics"""
    
    def test_get_destination_stats(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_destination: Destination,
    ):
        """Test getting destination statistics"""
        response = client.get(
            f"/api/v1/destinations/{sample_destination.destination_id}/stats",
            headers=admin_headers,
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "destination_id" in data
        assert "destination_name" in data
        assert "total_connections" in data
        assert "active_connections" in data
        assert "total_syncs" in data
        assert "successful_syncs" in data
        assert "failed_syncs" in data
        assert data["total_connections"] >= 0
        assert data["active_connections"] >= 0
