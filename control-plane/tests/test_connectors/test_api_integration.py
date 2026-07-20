"""Integration tests for connector definition API endpoints"""
import pytest
from fastapi import status


class TestListConnectors:
    """Test listing connector definitions"""
    
    def test_list_connectors_success(self, client, admin_headers, sample_connector):
        """Test successful connector list"""
        response = client.get("/api/v1/connector-definitions", headers=admin_headers)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "connectors" in data
        assert "total" in data
        assert data["total"] >= 1
        assert len(data["connectors"]) >= 1
    
    def test_list_connectors_filter_by_category(self, client, admin_headers, sample_connector, sample_destination_connector):
        """Test filtering by category"""
        response = client.get("/api/v1/connector-definitions?category=source", headers=admin_headers)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        for connector in data["connectors"]:
            assert connector["category"] == "source"
    
    def test_list_connectors_filter_by_type(self, client, admin_headers, sample_connector):
        """Test filtering by connector type"""
        response = client.get("/api/v1/connector-definitions?connector_type=mysql", headers=admin_headers)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        for connector in data["connectors"]:
            assert connector["connector_type"] == "mysql"
    
    def test_list_connectors_filter_by_cdc(self, client, admin_headers, sample_connector):
        """Test filtering by CDC support"""
        response = client.get("/api/v1/connector-definitions?supports_cdc=true", headers=admin_headers)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        for connector in data["connectors"]:
            assert connector["supports_cdc"] is True
    
    def test_list_connectors_search(self, client, admin_headers, sample_connector):
        """Test search functionality"""
        response = client.get("/api/v1/connector-definitions?search=MySQL", headers=admin_headers)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] >= 1
    
    def test_list_connectors_pagination(self, client, admin_headers, sample_connector):
        """Test pagination"""
        response = client.get("/api/v1/connector-definitions?page=1&page_size=10", headers=admin_headers)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["page"] == 1
        assert data["page_size"] == 10
    
    def test_list_connectors_without_auth(self, client, sample_connector):
        """Test listing without authentication"""
        response = client.get("/api/v1/connector-definitions")
        
        assert response.status_code == status.HTTP_403_FORBIDDEN


class TestCreateConnector:
    """Test creating connector definitions"""
    
    def test_create_connector_success(self, client, admin_headers):
        """Test successful connector creation"""
        connector_data = {
            "connector_name": "Test Connector",
            "connector_type": "test",
            "category": "source",
            "latest_version": "1.0.0",
            "default_config": {"port": 8080},
            "required_fields": ["host", "port"],
            "optional_fields": ["timeout"],
            "default_resource_limits": {"max_connections": 5},
            "supports_cdc": True,
            "supports_full_refresh": True,
            "supports_incremental": False,
        }
        
        response = client.post("/api/v1/connector-definitions", json=connector_data, headers=admin_headers)
        
        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        assert data["connector_name"] == "Test Connector"
        assert data["connector_type"] == "test"
        assert "connector_id" in data
    
    def test_create_connector_duplicate_name(self, client, admin_headers, sample_connector):
        """Test creating connector with duplicate name"""
        connector_data = {
            "connector_name": sample_connector.connector_name,
            "connector_type": "test",
            "category": "source",
            "latest_version": "1.0.0",
        }
        
        response = client.post("/api/v1/connector-definitions", json=connector_data, headers=admin_headers)
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "already exists" in response.json()["detail"].lower()
    
    def test_create_connector_invalid_category(self, client, admin_headers):
        """Test creating connector with invalid category"""
        connector_data = {
            "connector_name": "Invalid Connector",
            "connector_type": "test",
            "category": "invalid",
            "latest_version": "1.0.0",
        }
        
        response = client.post("/api/v1/connector-definitions", json=connector_data, headers=admin_headers)
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    
    def test_create_connector_without_permission(self, client, user_headers):
        """Test creating connector without permission"""
        connector_data = {
            "connector_name": "Test Connector",
            "connector_type": "test",
            "category": "source",
            "latest_version": "1.0.0",
        }
        
        response = client.post("/api/v1/connector-definitions", json=connector_data, headers=user_headers)
        
        assert response.status_code == status.HTTP_403_FORBIDDEN


class TestGetConnector:
    """Test getting connector definition"""
    
    def test_get_connector_success(self, client, admin_headers, sample_connector):
        """Test successful connector retrieval"""
        response = client.get(f"/api/v1/connector-definitions/{sample_connector.connector_id}", headers=admin_headers)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["connector_id"] == str(sample_connector.connector_id)
        assert data["connector_name"] == sample_connector.connector_name
    
    def test_get_connector_not_found(self, client, admin_headers):
        """Test getting non-existent connector"""
        from uuid import uuid4
        response = client.get(f"/api/v1/connector-definitions/{uuid4()}", headers=admin_headers)
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_get_connector_without_auth(self, client, sample_connector):
        """Test getting connector without authentication"""
        response = client.get(f"/api/v1/connector-definitions/{sample_connector.connector_id}")
        
        assert response.status_code == status.HTTP_403_FORBIDDEN


class TestUpdateConnector:
    """Test updating connector definition"""
    
    def test_update_connector_success(self, client, admin_headers, sample_connector):
        """Test successful connector update"""
        update_data = {
            "connector_name": "Updated MySQL Source",
            "latest_version": "2.0.0",
        }
        
        response = client.patch(
            f"/api/v1/connector-definitions/{sample_connector.connector_id}",
            json=update_data,
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["connector_name"] == "Updated MySQL Source"
        assert data["latest_version"] == "2.0.0"
    
    def test_update_connector_not_found(self, client, admin_headers):
        """Test updating non-existent connector"""
        from uuid import uuid4
        update_data = {"connector_name": "Updated"}
        
        response = client.patch(
            f"/api/v1/connector-definitions/{uuid4()}",
            json=update_data,
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_update_connector_without_permission(self, client, user_headers, sample_connector):
        """Test updating connector without permission"""
        update_data = {"connector_name": "Updated"}
        
        response = client.patch(
            f"/api/v1/connector-definitions/{sample_connector.connector_id}",
            json=update_data,
            headers=user_headers
        )
        
        assert response.status_code == status.HTTP_403_FORBIDDEN


class TestDeleteConnector:
    """Test deleting connector definition"""
    
    def test_delete_connector_success(self, client, admin_headers, sample_connector):
        """Test successful connector deletion"""
        response = client.delete(
            f"/api/v1/connector-definitions/{sample_connector.connector_id}",
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_204_NO_CONTENT
        
        # Verify deletion
        get_response = client.get(
            f"/api/v1/connector-definitions/{sample_connector.connector_id}",
            headers=admin_headers
        )
        assert get_response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_delete_connector_not_found(self, client, admin_headers):
        """Test deleting non-existent connector"""
        from uuid import uuid4
        response = client.delete(
            f"/api/v1/connector-definitions/{uuid4()}",
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_delete_connector_without_permission(self, client, user_headers, sample_connector):
        """Test deleting connector without permission"""
        response = client.delete(
            f"/api/v1/connector-definitions/{sample_connector.connector_id}",
            headers=user_headers
        )
        
        assert response.status_code == status.HTTP_403_FORBIDDEN


class TestConnectorVersions:
    """Test connector version management"""
    
    def test_list_versions_success(self, client, admin_headers, sample_connector, sample_connector_version):
        """Test listing connector versions"""
        response = client.get(
            f"/api/v1/connector-definitions/{sample_connector.connector_id}/versions",
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "versions" in data
        assert data["total"] >= 1
    
    def test_create_version_success(self, client, admin_headers, sample_connector):
        """Test creating connector version"""
        version_data = {
            "version": "2.0.0",
            "release_notes": "Major update",
            "breaking_changes": ["Changed API"],
            "new_features": ["New feature"],
            "bug_fixes": [],
            "docker_image": "fusion/mysql-source",
            "docker_tag": "2.0.0",
            "is_stable": True,
            "released_at": "2025-06-01T00:00:00Z",
        }
        
        response = client.post(
            f"/api/v1/connector-definitions/{sample_connector.connector_id}/versions",
            json=version_data,
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        assert data["version"] == "2.0.0"
    
    def test_create_version_duplicate(self, client, admin_headers, sample_connector, sample_connector_version):
        """Test creating duplicate version"""
        version_data = {
            "version": "1.0.0",
            "released_at": "2025-01-01T00:00:00Z",
        }
        
        response = client.post(
            f"/api/v1/connector-definitions/{sample_connector.connector_id}/versions",
            json=version_data,
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
    
    def test_get_version_success(self, client, admin_headers, sample_connector, sample_connector_version):
        """Test getting specific version"""
        response = client.get(
            f"/api/v1/connector-definitions/{sample_connector.connector_id}/versions/{sample_connector_version.version_id}",
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["version"] == "1.0.0"
    
    def test_update_version_success(self, client, admin_headers, sample_connector, sample_connector_version):
        """Test updating version"""
        update_data = {
            "is_stable": False,
            "release_notes": "Updated notes",
        }
        
        response = client.patch(
            f"/api/v1/connector-definitions/{sample_connector.connector_id}/versions/{sample_connector_version.version_id}",
            json=update_data,
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["is_stable"] is False


class TestConnectorCapabilities:
    """Test connector capabilities endpoints"""
    
    def test_get_capabilities(self, client, admin_headers, sample_connector):
        """Test getting connector capabilities"""
        response = client.get(
            f"/api/v1/connector-definitions/{sample_connector.connector_id}/capabilities",
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "supports_cdc" in data
        assert "supports_full_refresh" in data
        assert "supports_incremental" in data
    
    def test_get_config_schema(self, client, admin_headers, sample_connector):
        """Test getting config schema"""
        response = client.get(
            f"/api/v1/connector-definitions/{sample_connector.connector_id}/config-schema",
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "required_fields" in data
        assert "optional_fields" in data
        assert "default_config" in data
    
    def test_get_stats(self, client, admin_headers, sample_connector):
        """Test getting connector stats"""
        response = client.get(
            f"/api/v1/connector-definitions/{sample_connector.connector_id}/stats",
            headers=admin_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "connector_id" in data
        assert "connector_name" in data
        assert "total_sources" in data
        assert "total_destinations" in data
        assert "total_connections" in data
