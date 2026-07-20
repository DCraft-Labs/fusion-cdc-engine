"""Integration tests for UDF Catalog API

Covers all 5 REST endpoints:
  POST   /api/v1/udfs
  GET    /api/v1/udfs
  GET    /api/v1/udfs/{id}
  PATCH  /api/v1/udfs/{id}
  DELETE /api/v1/udfs/{id}
"""

import pytest
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models.transformation import UDFCatalog


BASE = "/api/v1/udfs"


# ============================================================================
# P1.12-T1 — Register UDF
# ============================================================================

class TestRegisterUDF:

    def test_register_udf_returns_201(
        self, client: TestClient, admin_headers: dict, valid_udf_payload: dict
    ):
        """POST returns 201 with a udf_id UUID."""
        response = client.post(BASE, json=valid_udf_payload, headers=admin_headers)

        assert response.status_code == 201, response.text
        data = response.json()
        assert "udf_id" in data
        assert data["udf_name"] == valid_udf_payload["udf_name"]
        assert data["is_active"] is True
        assert data["is_validated"] is False

    def test_register_udf_duplicate_name_returns_400(
        self,
        client: TestClient,
        admin_headers: dict,
        valid_udf_payload: dict,
        sample_udf: UDFCatalog,
    ):
        """Duplicate UDF name in same tenant → 400."""
        duplicate = dict(valid_udf_payload)
        duplicate["udf_name"] = sample_udf.udf_name
        response = client.post(BASE, json=duplicate, headers=admin_headers)

        assert response.status_code == 400
        assert "already exists" in response.json()["detail"].lower()

    def test_register_udf_invalid_language_returns_422(
        self, client: TestClient, admin_headers: dict, valid_udf_payload: dict
    ):
        """Invalid language → 422."""
        payload = dict(valid_udf_payload)
        payload["language"] = "ruby"
        response = client.post(BASE, json=payload, headers=admin_headers)
        assert response.status_code == 422

    def test_register_udf_missing_required_field_returns_422(
        self, client: TestClient, admin_headers: dict, valid_udf_payload: dict
    ):
        """Missing return_type → 422."""
        payload = {k: v for k, v in valid_udf_payload.items() if k != "return_type"}
        response = client.post(BASE, json=payload, headers=admin_headers)
        assert response.status_code == 422

    def test_register_udf_unauthenticated_returns_403(
        self, client: TestClient, valid_udf_payload: dict
    ):
        response = client.post(BASE, json=valid_udf_payload)
        assert response.status_code == 403


# ============================================================================
# P1.12-T2 — List UDFs
# ============================================================================

class TestListUDFs:

    def test_list_udfs_success(
        self, client: TestClient, admin_headers: dict, sample_udf: UDFCatalog
    ):
        """GET returns list with at least one UDF."""
        response = client.get(BASE, headers=admin_headers)

        assert response.status_code == 200
        data = response.json()
        assert "udfs" in data
        assert data["total"] >= 1

    def test_list_udfs_tenant_isolation(
        self,
        client: TestClient,
        other_tenant_headers: dict,
        sample_udf: UDFCatalog,
    ):
        """Other tenant cannot see this tenant's UDFs."""
        response = client.get(BASE, headers=other_tenant_headers)

        assert response.status_code == 200
        ids = [u["udf_id"] for u in response.json()["udfs"]]
        assert str(sample_udf.udf_id) not in ids

    def test_list_udfs_filter_by_language(
        self, client: TestClient, admin_headers: dict, sample_udf: UDFCatalog
    ):
        """Filter by language returns only matching UDFs."""
        response = client.get(BASE, params={"language": "python"}, headers=admin_headers)

        assert response.status_code == 200
        for udf in response.json()["udfs"]:
            assert udf["language"] == "python"

    def test_list_udfs_filter_by_category(
        self, client: TestClient, admin_headers: dict, sample_udf: UDFCatalog
    ):
        """Filter by category returns only matching UDFs."""
        response = client.get(
            BASE, params={"category": sample_udf.category}, headers=admin_headers
        )
        assert response.status_code == 200
        for udf in response.json()["udfs"]:
            assert udf["category"] == sample_udf.category

    def test_list_udfs_deleted_not_shown(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_udf: UDFCatalog,
    ):
        """Deactivated UDFs are excluded from the list."""
        # Deactivate via DELETE
        client.delete(f"{BASE}/{sample_udf.udf_id}", headers=admin_headers)
        response = client.get(BASE, headers=admin_headers)

        ids = [u["udf_id"] for u in response.json()["udfs"]]
        assert str(sample_udf.udf_id) not in ids


# ============================================================================
# P1.12-T3 — Get single UDF
# ============================================================================

class TestGetUDF:

    def test_get_udf_returns_function_body(
        self, client: TestClient, admin_headers: dict, sample_udf: UDFCatalog
    ):
        """GET /{id} returns the full function_code."""
        response = client.get(f"{BASE}/{sample_udf.udf_id}", headers=admin_headers)

        assert response.status_code == 200
        data = response.json()
        assert data["udf_id"] == str(sample_udf.udf_id)
        assert data["function_code"] == sample_udf.function_code

    def test_get_udf_not_found_returns_404(
        self, client: TestClient, admin_headers: dict
    ):
        """Non-existent UDF → 404."""
        response = client.get(f"{BASE}/{uuid4()}", headers=admin_headers)
        assert response.status_code == 404


# ============================================================================
# P1.12-T4 — Update UDF
# ============================================================================

class TestUpdateUDF:

    def test_update_udf_description(
        self, client: TestClient, admin_headers: dict, sample_udf: UDFCatalog
    ):
        """PATCH description updates successfully."""
        response = client.patch(
            f"{BASE}/{sample_udf.udf_id}",
            json={"description": "Updated Test Description"},
            headers=admin_headers,
        )
        assert response.status_code == 200
        assert response.json()["description"] == "Updated Test Description"

    def test_update_udf_code_resets_validation(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_udf: UDFCatalog,
        db_session: Session,
    ):
        """Changing function_code resets is_validated to False."""
        # Force is_validated=True first
        sample_udf.is_validated = True
        db_session.commit()

        response = client.patch(
            f"{BASE}/{sample_udf.udf_id}",
            json={"function_code": "def new_fn(x): return x + 1"},
            headers=admin_headers,
        )
        assert response.status_code == 200
        assert response.json()["is_validated"] is False

    def test_update_udf_not_found_returns_404(
        self, client: TestClient, admin_headers: dict
    ):
        response = client.patch(
            f"{BASE}/{uuid4()}", json={"description": "test"}, headers=admin_headers
        )
        assert response.status_code == 404


# ============================================================================
# P1.12-T5 — Delete (deactivate) UDF
# ============================================================================

class TestDeleteUDF:

    def test_delete_udf_soft_delete_not_in_list(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_udf: UDFCatalog,
        db_session: Session,
    ):
        """DELETE sets is_active=False; UDF no longer appears in list."""
        response = client.delete(f"{BASE}/{sample_udf.udf_id}", headers=admin_headers)
        assert response.status_code == 204

        # Not in list
        list_resp = client.get(BASE, headers=admin_headers)
        ids = [u["udf_id"] for u in list_resp.json()["udfs"]]
        assert str(sample_udf.udf_id) not in ids

        # Record still in DB with is_active=False
        db_session.refresh(sample_udf)
        assert sample_udf.is_active is False

    def test_delete_udf_then_register_same_name_succeeds(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_udf: UDFCatalog,
        valid_udf_payload: dict,
    ):
        """After deactivation, the same name can be re-registered."""
        client.delete(f"{BASE}/{sample_udf.udf_id}", headers=admin_headers)

        payload = dict(valid_udf_payload)
        payload["udf_name"] = sample_udf.udf_name
        response = client.post(BASE, json=payload, headers=admin_headers)
        assert response.status_code == 201

    def test_delete_udf_not_found_returns_404(
        self, client: TestClient, admin_headers: dict
    ):
        response = client.delete(f"{BASE}/{uuid4()}", headers=admin_headers)
        assert response.status_code == 404
