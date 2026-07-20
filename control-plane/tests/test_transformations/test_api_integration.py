"""Integration tests for Transformation Pipeline API

Covers all 6 REST endpoints:
  POST   /api/v1/transformations
  GET    /api/v1/transformations
  GET    /api/v1/transformations/{id}
  PUT    /api/v1/transformations/{id}
  DELETE /api/v1/transformations/{id}
  POST   /api/v1/transformations/{id}/validate
"""

import pytest
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models.transformation import TransformPipeline


BASE = "/api/v1/transformations"


# ============================================================================
# P1.11-T1 — Create transformation (happy path)
# ============================================================================

class TestCreateTransformation:

    def test_create_transformation_success(
        self, client: TestClient, admin_headers: dict, valid_pipeline_payload: dict
    ):
        """POST returns 201 with a pipeline_id UUID."""
        response = client.post(BASE, json=valid_pipeline_payload, headers=admin_headers)

        assert response.status_code == 201, response.text
        data = response.json()
        assert "pipeline_id" in data
        assert data["pipeline_name"] == valid_pipeline_payload["pipeline_name"]
        assert data["version"] == 1
        assert data["is_deleted"] is False

    def test_create_transformation_duplicate_name_returns_400(
        self,
        client: TestClient,
        admin_headers: dict,
        valid_pipeline_payload: dict,
        sample_pipeline: TransformPipeline,
    ):
        """Duplicate pipeline name in same tenant → 400."""
        duplicate = dict(valid_pipeline_payload)
        duplicate["pipeline_name"] = sample_pipeline.pipeline_name
        response = client.post(BASE, json=duplicate, headers=admin_headers)

        assert response.status_code == 400
        assert "already exists" in response.json()["detail"].lower()

    def test_create_transformation_invalid_pipeline_type_returns_422(
        self, client: TestClient, admin_headers: dict, valid_pipeline_payload: dict
    ):
        """Invalid pipeline_type → 422 validation error."""
        payload = dict(valid_pipeline_payload)
        payload["pipeline_type"] = "invalid_type"
        response = client.post(BASE, json=payload, headers=admin_headers)

        assert response.status_code == 422

    def test_create_transformation_missing_required_field_returns_422(
        self, client: TestClient, admin_headers: dict, valid_pipeline_payload: dict
    ):
        """Missing required field → 422."""
        payload = {k: v for k, v in valid_pipeline_payload.items() if k != "output_stream"}
        response = client.post(BASE, json=payload, headers=admin_headers)

        assert response.status_code == 422

    def test_create_transformation_unauthenticated_returns_403(
        self, client: TestClient, valid_pipeline_payload: dict
    ):
        """No auth header → 403."""
        response = client.post(BASE, json=valid_pipeline_payload)
        assert response.status_code == 403


# ============================================================================
# P1.11-T2 — List transformations
# ============================================================================

class TestListTransformations:

    def test_list_transformations_success(
        self, client: TestClient, admin_headers: dict, sample_pipeline: TransformPipeline
    ):
        """GET returns paginated list with at least one pipeline."""
        response = client.get(BASE, headers=admin_headers)

        assert response.status_code == 200
        data = response.json()
        assert "pipelines" in data
        assert "total" in data
        assert data["total"] >= 1

    def test_list_transformations_pagination(
        self,
        client: TestClient,
        admin_headers: dict,
        admin_user,
        db_session: Session,
    ):
        """Pagination returns correct page and page_size."""
        # Create 5 extra pipelines
        for i in range(5):
            p = TransformPipeline(
                pipeline_name=f"Test Paginate Pipeline {i}",
                pipeline_type="sql",
                transformation_code=f"SELECT {i} FROM dual",
                language="sql",
                input_streams=[],
                output_stream=f"out_{i}",
                execution_mode="batch",
                spark_config={},
                version=1,
                is_active=True,
                is_deleted=False,
                sub_tenant_id=admin_user.sub_tenant_id,
                bank_id=admin_user.bank_id,
            )
            db_session.add(p)
        db_session.commit()

        response = client.get(BASE, params={"page": 1, "page_size": 2}, headers=admin_headers)

        assert response.status_code == 200
        data = response.json()
        assert len(data["pipelines"]) <= 2
        assert data["page"] == 1
        assert data["page_size"] == 2

    def test_list_transformations_tenant_isolation(
        self,
        client: TestClient,
        other_tenant_headers: dict,
        sample_pipeline: TransformPipeline,
    ):
        """Other tenant cannot see this tenant's pipelines."""
        response = client.get(BASE, headers=other_tenant_headers)

        assert response.status_code == 200
        data = response.json()
        ids = [p["pipeline_id"] for p in data["pipelines"]]
        assert str(sample_pipeline.pipeline_id) not in ids

    def test_list_transformations_filter_by_pipeline_type(
        self, client: TestClient, admin_headers: dict, sample_pipeline: TransformPipeline
    ):
        """Filter by pipeline_type returns only matching pipelines."""
        response = client.get(
            BASE,
            params={"pipeline_type": sample_pipeline.pipeline_type},
            headers=admin_headers,
        )
        assert response.status_code == 200
        for p in response.json()["pipelines"]:
            assert p["pipeline_type"] == sample_pipeline.pipeline_type

    def test_list_transformations_filter_by_language(
        self, client: TestClient, admin_headers: dict, sample_pipeline: TransformPipeline
    ):
        """Filter by language returns only matching pipelines."""
        response = client.get(
            BASE,
            params={"language": sample_pipeline.language},
            headers=admin_headers,
        )
        assert response.status_code == 200
        for p in response.json()["pipelines"]:
            assert p["language"] == sample_pipeline.language


# ============================================================================
# P1.11-T3 — Get single transformation
# ============================================================================

class TestGetTransformation:

    def test_get_transformation_success(
        self, client: TestClient, admin_headers: dict, sample_pipeline: TransformPipeline
    ):
        """GET /{id} returns the correct pipeline."""
        response = client.get(f"{BASE}/{sample_pipeline.pipeline_id}", headers=admin_headers)

        assert response.status_code == 200
        data = response.json()
        assert data["pipeline_id"] == str(sample_pipeline.pipeline_id)
        assert data["pipeline_name"] == sample_pipeline.pipeline_name

    def test_get_transformation_not_found_returns_404(
        self, client: TestClient, admin_headers: dict
    ):
        """GET non-existent ID → 404."""
        response = client.get(f"{BASE}/{uuid4()}", headers=admin_headers)
        assert response.status_code == 404


# ============================================================================
# P1.11-T4 — Update transformation
# ============================================================================

class TestUpdateTransformation:

    def test_update_transformation_version_increment(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_pipeline: TransformPipeline,
    ):
        """Updating transformation_code bumps version."""
        original_version = sample_pipeline.version
        response = client.put(
            f"{BASE}/{sample_pipeline.pipeline_id}",
            json={"transformation_code": "SELECT id, amount FROM orders WHERE amount > 0"},
            headers=admin_headers,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["version"] == original_version + 1
        assert data["is_validated"] is False

    def test_update_transformation_no_code_change_no_version_bump(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_pipeline: TransformPipeline,
    ):
        """Non-code update does not bump version."""
        response = client.put(
            f"{BASE}/{sample_pipeline.pipeline_id}",
            json={"description": "Updated description only"},
            headers=admin_headers,
        )

        assert response.status_code == 200
        assert response.json()["version"] == sample_pipeline.version

    def test_update_transformation_not_found_returns_404(
        self, client: TestClient, admin_headers: dict
    ):
        response = client.put(
            f"{BASE}/{uuid4()}",
            json={"description": "update"},
            headers=admin_headers,
        )
        assert response.status_code == 404


# ============================================================================
# P1.11-T5 — Delete transformation
# ============================================================================

class TestDeleteTransformation:

    def test_delete_transformation_soft_delete(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_pipeline: TransformPipeline,
        db_session: Session,
    ):
        """DELETE soft-deletes: record exists in DB with is_deleted=True, not in list."""
        response = client.delete(
            f"{BASE}/{sample_pipeline.pipeline_id}", headers=admin_headers
        )
        assert response.status_code == 204

        # Should no longer appear in list
        list_response = client.get(BASE, headers=admin_headers)
        ids = [p["pipeline_id"] for p in list_response.json()["pipelines"]]
        assert str(sample_pipeline.pipeline_id) not in ids

        # But record exists in DB with is_deleted=True
        db_session.refresh(sample_pipeline)
        assert sample_pipeline.is_deleted is True

    def test_delete_transformation_not_found_returns_404(
        self, client: TestClient, admin_headers: dict
    ):
        response = client.delete(f"{BASE}/{uuid4()}", headers=admin_headers)
        assert response.status_code == 404


# ============================================================================
# P1.11-T6 — Validate transformation
# ============================================================================

class TestValidateTransformation:

    def test_validate_valid_sql_transformation(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_pipeline: TransformPipeline,
    ):
        """Valid SQL code → valid=True, errors=[]."""
        response = client.post(
            f"{BASE}/{sample_pipeline.pipeline_id}/validate", headers=admin_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert data["valid"] is True
        assert data["errors"] == []
        assert "validated_at" in data

    def test_validate_invalid_python_transformation(
        self,
        client: TestClient,
        admin_headers: dict,
        admin_user,
        db_session: Session,
    ):
        """Python syntax error → valid=False, errors non-empty."""
        bad_pipeline = TransformPipeline(
            pipeline_name="Test Invalid Python Pipeline",
            pipeline_type="python",
            transformation_code="def bad(:\n    pass",  # syntax error
            language="python",
            input_streams=[],
            output_stream="out",
            execution_mode="batch",
            spark_config={},
            version=1,
            is_active=True,
            is_deleted=False,
            sub_tenant_id=admin_user.sub_tenant_id,
            bank_id=admin_user.bank_id,
        )
        db_session.add(bad_pipeline)
        db_session.commit()

        response = client.post(
            f"{BASE}/{bad_pipeline.pipeline_id}/validate", headers=admin_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert data["valid"] is False
        assert len(data["errors"]) > 0

    def test_validate_invalid_sql_no_dml(
        self,
        client: TestClient,
        admin_headers: dict,
        admin_user,
        db_session: Session,
    ):
        """SQL without DML/DDL keyword → valid=False."""
        bad_pipeline = TransformPipeline(
            pipeline_name="Test SQL No DML",
            pipeline_type="sql",
            transformation_code="this is not sql",
            language="sql",
            input_streams=[],
            output_stream="out",
            execution_mode="batch",
            spark_config={},
            version=1,
            is_active=True,
            is_deleted=False,
            sub_tenant_id=admin_user.sub_tenant_id,
            bank_id=admin_user.bank_id,
        )
        db_session.add(bad_pipeline)
        db_session.commit()

        response = client.post(
            f"{BASE}/{bad_pipeline.pipeline_id}/validate", headers=admin_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert data["valid"] is False

    def test_validate_updates_is_validated_field(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_pipeline: TransformPipeline,
        db_session: Session,
    ):
        """After validation, is_validated and validated_at are persisted."""
        client.post(f"{BASE}/{sample_pipeline.pipeline_id}/validate", headers=admin_headers)
        db_session.refresh(sample_pipeline)
        assert sample_pipeline.is_validated is True
        assert sample_pipeline.validated_at is not None

    def test_validate_not_found_returns_404(
        self, client: TestClient, admin_headers: dict
    ):
        response = client.post(f"{BASE}/{uuid4()}/validate", headers=admin_headers)
        assert response.status_code == 404


# ============================================================================
# P1.11-T7 — Preview transformation  (spec §2)
# ============================================================================

class TestPreviewTransformation:
    """Tests for POST /{pipeline_id}/preview endpoint."""

    def test_preview_with_cast_step(
        self, client: TestClient, admin_headers: dict, sample_pipeline: TransformPipeline
    ):
        """Preview with a cast step transforms the sample rows."""
        spec = {
            "transforms": [
                {"id": "s1", "type": "cast", "column": "age", "to_type": "int", "output_column": "age_int"}
            ]
        }
        payload = {
            "sample_rows": [{"age": "25", "name": "Alice"}, {"age": "30", "name": "Bob"}],
            "transform_spec": spec,
        }
        response = client.post(f"{BASE}/{sample_pipeline.pipeline_id}/preview", json=payload, headers=admin_headers)

        assert response.status_code == 200, response.text
        data = response.json()
        assert "transformed_rows" in data
        assert data["step_count"] == 1
        rows = data["transformed_rows"]
        assert len(rows) == 2
        assert rows[0]["age_int"] == 25
        assert rows[1]["age_int"] == 30

    def test_preview_with_mask_step(
        self, client: TestClient, admin_headers: dict, sample_pipeline: TransformPipeline
    ):
        """Preview with a mask step hides sensitive data."""
        spec = {
            "transforms": [
                {"id": "s1", "type": "mask", "column": "card", "strategy": "last4", "output_column": "card_masked"}
            ]
        }
        payload = {
            "sample_rows": [{"card": "4111111111111234"}],
            "transform_spec": spec,
        }
        response = client.post(f"{BASE}/{sample_pipeline.pipeline_id}/preview", json=payload, headers=admin_headers)

        assert response.status_code == 200
        data = response.json()
        rows = data["transformed_rows"]
        assert rows[0]["card_masked"] == "****1234"

    def test_preview_with_string_op_upper(
        self, client: TestClient, admin_headers: dict, sample_pipeline: TransformPipeline
    ):
        """string_op upper converts to uppercase."""
        spec = {
            "transforms": [
                {"id": "s1", "type": "string_op", "column": "name", "op": "upper", "output_column": "name_upper"}
            ]
        }
        payload = {
            "sample_rows": [{"name": "alice"}, {"name": "bob"}],
            "transform_spec": spec,
        }
        response = client.post(f"{BASE}/{sample_pipeline.pipeline_id}/preview", json=payload, headers=admin_headers)

        assert response.status_code == 200
        data = response.json()
        rows = data["transformed_rows"]
        assert rows[0]["name_upper"] == "ALICE"
        assert rows[1]["name_upper"] == "BOB"

    def test_preview_unsupported_step_noted_in_errors(
        self, client: TestClient, admin_headers: dict, sample_pipeline: TransformPipeline
    ):
        """Steps requiring Spark are noted in errors list, not raised as 500."""
        spec = {
            "transforms": [
                {"id": "s1", "type": "udf", "column": "x", "udf_name": "my_udf", "output_column": "y"}
            ]
        }
        payload = {"sample_rows": [{"x": 1}], "transform_spec": spec}
        response = client.post(f"{BASE}/{sample_pipeline.pipeline_id}/preview", json=payload, headers=admin_headers)

        assert response.status_code == 200
        data = response.json()
        assert len(data["errors"]) >= 1
        assert "Spark" in data["errors"][0]

    def test_preview_empty_rows_returns_empty(
        self, client: TestClient, admin_headers: dict, sample_pipeline: TransformPipeline
    ):
        """Empty sample_rows returns an empty transformed_rows list."""
        spec = {"transforms": []}
        payload = {"sample_rows": [], "transform_spec": spec}
        response = client.post(f"{BASE}/{sample_pipeline.pipeline_id}/preview", json=payload, headers=admin_headers)

        assert response.status_code == 200
        data = response.json()
        assert data["transformed_rows"] == []
        assert data["step_count"] == 0

    def test_preview_not_found_returns_404(
        self, client: TestClient, admin_headers: dict
    ):
        """Non-existent pipeline → 404."""
        payload = {"sample_rows": [{"x": 1}], "transform_spec": {"transforms": []}}
        response = client.post(f"{BASE}/{uuid4()}/preview", json=payload, headers=admin_headers)
        assert response.status_code == 404

    def test_preview_unauthenticated_returns_403(
        self, client: TestClient, sample_pipeline: TransformPipeline
    ):
        """No auth → 403."""
        payload = {"sample_rows": [{"x": 1}], "transform_spec": {"transforms": []}}
        response = client.post(f"{BASE}/{sample_pipeline.pipeline_id}/preview", json=payload)
        assert response.status_code == 403

