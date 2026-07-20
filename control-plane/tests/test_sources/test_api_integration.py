"""
Integration tests for Sources API
Tests all CRUD operations, connection testing, schema discovery, and CDC configuration
"""
import pytest
from uuid import uuid4
from fastapi import status


class TestListSources:
    """Test source listing with filters and pagination"""
    
    def test_list_sources_success(self, client, admin_headers, sample_source):
        """Test listing sources successfully"""
        response = client.get("/api/v1/sources/", headers=admin_headers)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "sources" in data
        assert "total" in data
        assert data["total"] >= 1
        assert len(data["sources"]) >= 1
        assert data["sources"][0]["source_name"] == "Test MySQL Source"
    
    def test_list_sources_filter_by_status(self, client, admin_headers, sample_source):
        """Test filtering sources by status"""
        response = client.get("/api/v1/sources/?status=draft", headers=admin_headers)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] >= 1
        for source in data["sources"]:
            assert source["status"] == "draft"
    
    def test_list_sources_filter_by_connector_type(self, client, admin_headers, sample_source):
        """Test filtering sources by connector type"""
        response = client.get("/api/v1/sources/?connector_type=mysql", headers=admin_headers)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] >= 1
        for source in data["sources"]:
            assert source["connector_definition_type"] == "mysql"
    
    def test_list_sources_search(self, client, admin_headers, sample_source):
        """Test searching sources by name"""
        response = client.get("/api/v1/sources/?search=MySQL", headers=admin_headers)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] >= 1
        assert "MySQL" in data["sources"][0]["source_name"]
    
    def test_list_sources_pagination(self, client, admin_headers, sample_source):
        """Test pagination"""
        response = client.get("/api/v1/sources/?page=1&page_size=10", headers=admin_headers)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["page"] == 1
        assert data["page_size"] == 10
        assert "total_pages" in data
    
    def test_list_sources_requires_auth(self, client):
        """Test that listing sources requires authentication"""
        response = client.get("/api/v1/sources/")
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED


class TestCreateSource:
    """Test source creation"""
    
    def test_create_source_success(self, client, admin_headers, sample_connector_definition):
        """Test creating a source successfully"""
        source_data = {
            "source_name": "New MySQL Source",
            "connector_definition_id": str(sample_connector_definition.connector_id),
            "connector_version": "1.0.0",
            "host": "db.example.com",
            "port": 3306,
            "database_name": "mydb",
            "username": "dbuser",
            "password": "dbpass123",
            "ssl_enabled": True,
            "ssl_config": {"verify_cert": True},
            "config": {"charset": "utf8mb4"}
        }
        
        response = client.post("/api/v1/sources/", json=source_data, headers=admin_headers)
        
        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        assert data["source_name"] == "New MySQL Source"
        assert data["host"] == "db.example.com"
        assert data["port"] == 3306
        assert data["status"] == "draft"
        assert "source_id" in data
        assert data["password"] == "********"  # Password should be hidden
    
    def test_create_source_duplicate_name(self, client, admin_headers, sample_source):
        """Test creating source with duplicate name fails"""
        source_data = {
            "source_name": "Test MySQL Source",  # Duplicate name
            "connector_definition_id": str(sample_source.connector_definition_id),
            "connector_version": "1.0.0",
            "host": "localhost",
            "port": 3306,
            "database_name": "testdb",
            "username": "testuser",
            "password": "testpass"
        }
        
        response = client.post("/api/v1/sources/", json=source_data, headers=admin_headers)
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "already exists" in response.json()["detail"]
    
    def test_create_source_invalid_connector(self, client, admin_headers):
        """Test creating source with invalid connector fails"""
        source_data = {
            "source_name": "Invalid Source",
            "connector_definition_id": str(uuid4()),  # Non-existent connector
            "connector_version": "1.0.0",
            "host": "localhost",
            "port": 3306,
            "database_name": "testdb",
            "username": "testuser",
            "password": "testpass"
        }
        
        response = client.post("/api/v1/sources/", json=source_data, headers=admin_headers)
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "not found" in response.json()["detail"].lower()
    
    def test_create_source_requires_permission(self, client, user_headers, sample_connector_definition):
        """Test creating source requires proper permission"""
        source_data = {
            "source_name": "Unauthorized Source",
            "connector_definition_id": str(sample_connector_definition.connector_id),
            "connector_version": "1.0.0",
            "host": "localhost",
            "port": 3306,
            "database_name": "testdb",
            "username": "testuser",
            "password": "testpass"
        }
        
        response = client.post("/api/v1/sources/", json=source_data, headers=user_headers)
        
        assert response.status_code == status.HTTP_403_FORBIDDEN


class TestGetSource:
    """Test getting source details"""
    
    def test_get_source_success(self, client, admin_headers, sample_source):
        """Test getting source details"""
        response = client.get(f"/api/v1/sources/{sample_source.source_id}", headers=admin_headers)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["source_id"] == str(sample_source.source_id)
        assert data["source_name"] == sample_source.source_name
        assert data["host"] == sample_source.host
        assert data["password"] == "********"  # Password should be hidden
    
    def test_get_source_not_found(self, client, admin_headers):
        """Test getting non-existent source"""
        fake_id = uuid4()
        response = client.get(f"/api/v1/sources/{fake_id}", headers=admin_headers)
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_get_source_requires_auth(self, client, sample_source):
        """Test getting source requires authentication"""
        response = client.get(f"/api/v1/sources/{sample_source.source_id}")
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED


class TestUpdateSource:
    """Test source updates"""
    
    def test_update_source_success(self, client, admin_headers, sample_source):
        """Test updating source successfully"""
        update_data = {
            "source_name": "Updated MySQL Source",
            "host": "newhost.example.com",
            "port": 3307
        }
        
        response = client.patch(
            f"/api/v1/sources/{sample_source.source_id}",
            json=update_data,
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["source_name"] == "Updated MySQL Source"
        assert data["host"] == "newhost.example.com"
        assert data["port"] == 3307
    
    def test_update_source_partial(self, client, admin_headers, sample_source):
        """Test partial source update"""
        update_data = {"status": "active"}
        
        response = client.patch(
            f"/api/v1/sources/{sample_source.source_id}",
            json=update_data,
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["status"] == "active"
        assert data["source_name"] == sample_source.source_name  # Unchanged
    
    def test_update_source_not_found(self, client, admin_headers):
        """Test updating non-existent source"""
        fake_id = uuid4()
        update_data = {"source_name": "Updated"}
        
        response = client.patch(f"/api/v1/sources/{fake_id}", json=update_data, headers=admin_headers)
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_update_source_requires_permission(self, client, user_headers, sample_source):
        """Test updating source requires proper permission"""
        update_data = {"source_name": "Updated"}
        
        response = client.patch(
            f"/api/v1/sources/{sample_source.source_id}",
            json=update_data,
            headers=user_headers
        )
        
        assert response.status_code == status.HTTP_403_FORBIDDEN


class TestDeleteSource:
    """Test source deletion"""
    
    def test_delete_source_success(self, client, admin_headers, sample_source):
        """Test deleting source successfully"""
        response = client.delete(f"/api/v1/sources/{sample_source.source_id}", headers=admin_headers)
        
        assert response.status_code == status.HTTP_204_NO_CONTENT
        
        # Verify source is soft-deleted
        get_response = client.get(f"/api/v1/sources/{sample_source.source_id}", headers=admin_headers)
        assert get_response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_delete_source_not_found(self, client, admin_headers):
        """Test deleting non-existent source"""
        fake_id = uuid4()
        response = client.delete(f"/api/v1/sources/{fake_id}", headers=admin_headers)
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_delete_source_requires_permission(self, client, user_headers, sample_source):
        """Test deleting source requires proper permission"""
        response = client.delete(f"/api/v1/sources/{sample_source.source_id}", headers=user_headers)
        
        assert response.status_code == status.HTTP_403_FORBIDDEN


class TestConnectionTesting:
    """Test connection testing functionality"""
    
    def test_test_connection_success(self, client, admin_headers, sample_source):
        """Test connection testing endpoint"""
        response = client.post(
            f"/api/v1/sources/{sample_source.source_id}/test-connection",
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "status" in data
        assert "message" in data
        assert "connection_test_at" in data
        assert data["status"] in ["success", "failure"]
    
    def test_test_connection_with_override(self, client, admin_headers, sample_source):
        """Test connection testing with override parameters"""
        test_data = {
            "host": "override.example.com",
            "port": 3307,
            "database_name": "overridedb"
        }
        
        response = client.post(
            f"/api/v1/sources/{sample_source.source_id}/test-connection",
            json=test_data,
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "status" in data
    
    def test_test_connection_not_found(self, client, admin_headers):
        """Test connection testing on non-existent source"""
        fake_id = uuid4()
        response = client.post(f"/api/v1/sources/{fake_id}/test-connection", headers=admin_headers)
        
        assert response.status_code == status.HTTP_404_NOT_FOUND


class TestSchemaDiscovery:
    """Test schema discovery functionality"""
    
    def test_discover_schemas_success(self, client, admin_headers, sample_source):
        """Test schema discovery"""
        response = client.post(
            f"/api/v1/sources/{sample_source.source_id}/discover-schemas",
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "schemas" in data
        assert "total_schemas" in data
        assert "total_tables" in data
        assert "last_discovery_at" in data
        assert isinstance(data["schemas"], list)
    
    def test_get_table_schema_success(self, client, admin_headers, sample_source):
        """Test getting table schema"""
        table_request = {
            "schema_name": "public",
            "table_name": "users"
        }
        
        response = client.post(
            f"/api/v1/sources/{sample_source.source_id}/table-schema",
            json=table_request,
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["schema_name"] == "public"
        assert data["table_name"] == "users"
        assert "columns" in data
        assert "primary_keys" in data
        assert isinstance(data["columns"], list)


class TestCDCConfiguration:
    """Test CDC configuration"""
    
    def test_configure_cdc_success(self, client, admin_headers, sample_source):
        """Test configuring CDC"""
        cdc_config = {
            "enable_cdc": True,
            "replication_method": "binlog",
            "replication_config": {
                "server_id": 1,
                "include_tables": ["users", "orders"]
            }
        }
        
        response = client.post(
            f"/api/v1/sources/{sample_source.source_id}/cdc-config",
            json=cdc_config,
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["enable_cdc"] == True
        assert data["replication_method"] == "binlog"
        assert data["cdc_status"] == "configured"
    
    def test_get_cdc_config(self, client, admin_headers, sample_source):
        """Test getting CDC configuration"""
        response = client.get(
            f"/api/v1/sources/{sample_source.source_id}/cdc-config",
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "enable_cdc" in data
        assert "cdc_status" in data
    
    def test_configure_cdc_requires_permission(self, client, user_headers, sample_source):
        """Test CDC configuration requires proper permission"""
        cdc_config = {
            "enable_cdc": True,
            "replication_method": "binlog",
            "replication_config": {}
        }
        
        response = client.post(
            f"/api/v1/sources/{sample_source.source_id}/cdc-config",
            json=cdc_config,
            headers=user_headers
        )
        
        assert response.status_code == status.HTTP_403_FORBIDDEN


class TestSourceStatistics:
    """Test source statistics"""
    
    def test_get_source_stats(self, client, admin_headers, sample_source):
        """Test getting source statistics"""
        response = client.get(
            f"/api/v1/sources/{sample_source.source_id}/stats",
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["source_id"] == str(sample_source.source_id)
        assert data["source_name"] == sample_source.source_name
        assert "total_connections" in data
        assert "active_connections" in data
        assert "total_syncs" in data
        assert "successful_syncs" in data
        assert "failed_syncs" in data
        assert isinstance(data["total_connections"], int)
