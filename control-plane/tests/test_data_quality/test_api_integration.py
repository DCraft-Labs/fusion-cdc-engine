"""Integration tests for Data Quality Management API

Tests 18 REST API endpoints with comprehensive coverage:
- DQ Policies CRUD (5 endpoints)
- Rule Testing and Execution (3 endpoints)
- Violations Management (3 endpoints)
- Quality Metrics (2 endpoints)
- Data Profiling (1 endpoint)
"""

import pytest
from uuid import uuid4
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from decimal import Decimal

from app.models.auth import User
from app.models.connection import Connection, Stream
from app.models.data_quality import DQPolicy, DQViolation, DQRuleResult, DQViolationSample


# ============================================================================
# Test DQ Policy CRUD (5 endpoints)
# ============================================================================

class TestDQPolicyCRUD:
    """Test CRUD operations for DQ policies"""
    
    def test_create_policy_success(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_connection: Connection,
        sample_stream: Stream,
    ):
        """Test creating DQ policy successfully"""
        payload = {
            "policy_name": "Test Email Format Policy",
            "description": "Validates email format",
            "connection_id": str(sample_connection.connection_id),
            "stream_id": str(sample_stream.stream_id),
            "rule_type": "regex",
            "rule_definition": {
                "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            },
            "target_columns": ["email"],
            "severity": "error",
            "action_on_failure": "alert",
            "threshold_type": "percentage",
            "threshold_value": "5.0",
            "execution_schedule": "0 */6 * * *",
            "is_active": True,
        }
        
        response = client.post(
            "/api/v1/data-quality/policies",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 201
        data = response.json()
        assert data["policy_name"] == payload["policy_name"]
        assert data["rule_type"] == "regex"
        assert data["severity"] == "error"
        assert "policy_id" in data
    
    def test_create_policy_with_range_check(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_connection: Connection,
    ):
        """Test creating range check policy"""
        payload = {
            "policy_name": "Test Age Range Policy",
            "description": "Validates age is within reasonable range",
            "connection_id": str(sample_connection.connection_id),
            "rule_type": "range_check",
            "rule_definition": {
                "min_value": 18,
                "max_value": 120
            },
            "target_columns": ["age"],
            "severity": "warning",
            "action_on_failure": "log",
            "is_active": True,
        }
        
        response = client.post(
            "/api/v1/data-quality/policies",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 201
        data = response.json()
        assert data["rule_type"] == "range_check"
    
    def test_create_policy_duplicate_name(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_dq_policy: DQPolicy,
        sample_connection: Connection,
    ):
        """Test creating policy with duplicate name"""
        payload = {
            "policy_name": sample_dq_policy.policy_name,
            "connection_id": str(sample_connection.connection_id),
            "rule_type": "null_check",
            "rule_definition": {"check_type": "not_null"},
            "target_columns": ["email"],
            "severity": "error",
            "action_on_failure": "alert",
        }
        
        response = client.post(
            "/api/v1/data-quality/policies",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 400
        assert "already exists" in response.json()["detail"]
    
    def test_create_policy_invalid_connection(
        self,
        client: TestClient,
        admin_headers: dict,
    ):
        """Test creating policy with invalid connection"""
        payload = {
            "policy_name": "Test Invalid Connection Policy",
            "connection_id": str(uuid4()),
            "rule_type": "null_check",
            "rule_definition": {"check_type": "not_null"},
            "target_columns": ["email"],
            "severity": "error",
            "action_on_failure": "alert",
        }
        
        response = client.post(
            "/api/v1/data-quality/policies",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 404
        assert "Connection not found" in response.json()["detail"]
    
    def test_create_policy_invalid_rule_definition(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_connection: Connection,
    ):
        """Test creating policy with invalid rule definition"""
        payload = {
            "policy_name": "Test Invalid Rule Policy",
            "connection_id": str(sample_connection.connection_id),
            "rule_type": "range_check",
            "rule_definition": {},  # Missing required fields
            "target_columns": ["age"],
            "severity": "error",
            "action_on_failure": "alert",
        }
        
        response = client.post(
            "/api/v1/data-quality/policies",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 400
        assert "Invalid rule definition" in response.json()["detail"]
    
    def test_create_policy_requires_permission(
        self,
        client: TestClient,
        user_headers: dict,
        sample_connection: Connection,
    ):
        """Test creating policy requires permission"""
        payload = {
            "policy_name": "Test Unauthorized Policy",
            "connection_id": str(sample_connection.connection_id),
            "rule_type": "null_check",
            "rule_definition": {"check_type": "not_null"},
            "target_columns": ["email"],
            "severity": "error",
            "action_on_failure": "alert",
        }
        
        response = client.post(
            "/api/v1/data-quality/policies",
            json=payload,
            headers=user_headers
        )
        
        assert response.status_code == 403
    
    def test_list_policies_success(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_dq_policy: DQPolicy,
    ):
        """Test listing policies successfully"""
        response = client.get(
            "/api/v1/data-quality/policies",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "policies" in data
        assert "total" in data
        assert len(data["policies"]) >= 1
    
    def test_list_policies_filter_by_connection(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_dq_policy: DQPolicy,
        sample_connection: Connection,
    ):
        """Test filtering policies by connection"""
        response = client.get(
            "/api/v1/data-quality/policies",
            headers=admin_headers,
            params={"connection_id": str(sample_connection.connection_id)}
        )
        
        assert response.status_code == 200
        data = response.json()
        for policy in data["policies"]:
            assert policy["connection_id"] == str(sample_connection.connection_id)
    
    def test_list_policies_filter_by_rule_type(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_dq_policy: DQPolicy,
    ):
        """Test filtering by rule type"""
        response = client.get(
            "/api/v1/data-quality/policies",
            headers=admin_headers,
            params={"rule_type": "null_check"}
        )
        
        assert response.status_code == 200
        data = response.json()
        for policy in data["policies"]:
            assert policy["rule_type"] == "null_check"
    
    def test_list_policies_filter_by_severity(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_dq_policy: DQPolicy,
    ):
        """Test filtering by severity"""
        response = client.get(
            "/api/v1/data-quality/policies",
            headers=admin_headers,
            params={"severity": "error"}
        )
        
        assert response.status_code == 200
        data = response.json()
        for policy in data["policies"]:
            assert policy["severity"] == "error"
    
    def test_list_policies_search(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_dq_policy: DQPolicy,
    ):
        """Test search functionality"""
        response = client.get(
            "/api/v1/data-quality/policies",
            headers=admin_headers,
            params={"search": "Null Check"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert len(data["policies"]) >= 1
    
    def test_list_policies_pagination(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_dq_policy: DQPolicy,
    ):
        """Test pagination"""
        response = client.get(
            "/api/v1/data-quality/policies",
            headers=admin_headers,
            params={"page": 1, "page_size": 10}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["page"] == 1
        assert data["page_size"] == 10
        assert "total_pages" in data
    
    def test_get_policy_success(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_dq_policy: DQPolicy,
    ):
        """Test getting policy details"""
        response = client.get(
            f"/api/v1/data-quality/policies/{sample_dq_policy.policy_id}",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["policy_id"] == str(sample_dq_policy.policy_id)
        assert data["policy_name"] == sample_dq_policy.policy_name
        assert "violation_count" in data
        assert "active_violation_count" in data
    
    def test_get_policy_not_found(
        self,
        client: TestClient,
        admin_headers: dict,
    ):
        """Test getting non-existent policy"""
        response = client.get(
            f"/api/v1/data-quality/policies/{uuid4()}",
            headers=admin_headers
        )
        
        assert response.status_code == 404
    
    def test_update_policy_success(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_dq_policy: DQPolicy,
    ):
        """Test updating policy successfully"""
        payload = {
            "policy_name": "Updated Null Check Policy",
            "severity": "critical",
        }
        
        response = client.patch(
            f"/api/v1/data-quality/policies/{sample_dq_policy.policy_id}",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["policy_name"] == payload["policy_name"]
        assert data["severity"] == "critical"
    
    def test_update_policy_partial(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_dq_policy: DQPolicy,
    ):
        """Test partial update"""
        payload = {"is_active": False}
        
        response = client.patch(
            f"/api/v1/data-quality/policies/{sample_dq_policy.policy_id}",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["is_active"] == False
    
    def test_update_policy_not_found(
        self,
        client: TestClient,
        admin_headers: dict,
    ):
        """Test updating non-existent policy"""
        payload = {"policy_name": "New Name"}
        
        response = client.patch(
            f"/api/v1/data-quality/policies/{uuid4()}",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 404
    
    def test_update_policy_requires_permission(
        self,
        client: TestClient,
        user_headers: dict,
        sample_dq_policy: DQPolicy,
    ):
        """Test updating policy requires permission"""
        payload = {"severity": "critical"}
        
        response = client.patch(
            f"/api/v1/data-quality/policies/{sample_dq_policy.policy_id}",
            json=payload,
            headers=user_headers
        )
        
        assert response.status_code == 403
    
    def test_delete_policy_success(
        self,
        client: TestClient,
        admin_headers: dict,
        db_session: Session,
        sample_connection: Connection,
        sample_tenant: uuid4,
    ):
        """Test deleting inactive policy"""
        # Create inactive policy
        policy = DQPolicy(
            policy_id=uuid4(),
            policy_name="Test Delete Policy",
            connection_id=sample_connection.connection_id,
            rule_type="null_check",
            rule_definition={"check_type": "not_null"},
            target_columns=["email"],
            severity="warning",
            action_on_failure="log",
            is_active=False,
            sub_tenant_id=sample_tenant,
            bank_id=uuid4(),
        )
        db_session.add(policy)
        db_session.commit()
        
        response = client.delete(
            f"/api/v1/data-quality/policies/{policy.policy_id}",
            headers=admin_headers
        )
        
        assert response.status_code == 204
    
    def test_delete_active_policy_without_force(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_dq_policy: DQPolicy,
    ):
        """Test deleting active policy without force fails"""
        response = client.delete(
            f"/api/v1/data-quality/policies/{sample_dq_policy.policy_id}",
            headers=admin_headers
        )
        
        assert response.status_code == 400
        assert "Cannot delete active policy" in response.json()["detail"]
    
    def test_delete_active_policy_with_force(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_dq_policy: DQPolicy,
    ):
        """Test deleting active policy with force succeeds"""
        response = client.delete(
            f"/api/v1/data-quality/policies/{sample_dq_policy.policy_id}",
            headers=admin_headers,
            params={"force": True}
        )
        
        assert response.status_code == 204
    
    def test_delete_policy_requires_permission(
        self,
        client: TestClient,
        user_headers: dict,
        sample_dq_policy: DQPolicy,
    ):
        """Test deleting policy requires permission"""
        response = client.delete(
            f"/api/v1/data-quality/policies/{sample_dq_policy.policy_id}",
            headers=user_headers
        )
        
        assert response.status_code == 403


# ============================================================================
# Test Rule Testing and Execution
# ============================================================================

class TestRuleTestingExecution:
    """Test rule testing and execution endpoints"""
    
    def test_test_rule_success(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_connection: Connection,
        sample_stream: Stream,
    ):
        """Test rule testing successfully"""
        payload = {
            "rule_type": "regex",
            "rule_definition": {
                "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            },
            "connection_id": str(sample_connection.connection_id),
            "stream_id": str(sample_stream.stream_id),
            "target_columns": ["email"],
            "sample_size": 1000,
        }
        
        response = client.post(
            "/api/v1/data-quality/policies/test",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "test_passed" in data
        assert "records_tested" in data
        assert "records_passed" in data
        assert "records_failed" in data
        assert "execution_time_ms" in data
    
    def test_test_rule_invalid_connection(
        self,
        client: TestClient,
        admin_headers: dict,
    ):
        """Test rule testing with invalid connection"""
        payload = {
            "rule_type": "null_check",
            "rule_definition": {"check_type": "not_null"},
            "connection_id": str(uuid4()),
            "target_columns": ["email"],
            "sample_size": 100,
        }
        
        response = client.post(
            "/api/v1/data-quality/policies/test",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 404
    
    def test_test_rule_invalid_definition(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_connection: Connection,
    ):
        """Test rule testing with invalid definition"""
        payload = {
            "rule_type": "range_check",
            "rule_definition": {},  # Missing required fields
            "connection_id": str(sample_connection.connection_id),
            "target_columns": ["age"],
            "sample_size": 100,
        }
        
        response = client.post(
            "/api/v1/data-quality/policies/test",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 400
    
    def test_execute_policy_success(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_dq_policy: DQPolicy,
    ):
        """Test executing policy successfully"""
        payload = {
            "policy_id": str(sample_dq_policy.policy_id),
            "force_execution": False,
        }
        
        response = client.post(
            f"/api/v1/data-quality/policies/{sample_dq_policy.policy_id}/execute",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "result_id" in data
        assert "execution_id" in data
        assert "passed" in data
        assert data["policy_id"] == str(sample_dq_policy.policy_id)
    
    def test_execute_inactive_policy_without_force(
        self,
        client: TestClient,
        admin_headers: dict,
        db_session: Session,
        sample_connection: Connection,
        sample_tenant: uuid4,
    ):
        """Test executing inactive policy without force fails"""
        # Create inactive policy
        policy = DQPolicy(
            policy_id=uuid4(),
            policy_name="Test Inactive Policy",
            connection_id=sample_connection.connection_id,
            rule_type="null_check",
            rule_definition={"check_type": "not_null"},
            target_columns=["email"],
            severity="warning",
            action_on_failure="log",
            is_active=False,
            sub_tenant_id=sample_tenant,
            bank_id=uuid4(),
        )
        db_session.add(policy)
        db_session.commit()
        
        payload = {
            "policy_id": str(policy.policy_id),
            "force_execution": False,
        }
        
        response = client.post(
            f"/api/v1/data-quality/policies/{policy.policy_id}/execute",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 400
    
    def test_execute_policy_requires_permission(
        self,
        client: TestClient,
        user_headers: dict,
        sample_dq_policy: DQPolicy,
    ):
        """Test executing policy requires permission"""
        payload = {
            "policy_id": str(sample_dq_policy.policy_id),
            "force_execution": False,
        }
        
        response = client.post(
            f"/api/v1/data-quality/policies/{sample_dq_policy.policy_id}/execute",
            json=payload,
            headers=user_headers
        )
        
        assert response.status_code == 403
    
    def test_list_policy_results(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_dq_policy: DQPolicy,
        sample_rule_result: DQRuleResult,
    ):
        """Test listing policy execution results"""
        response = client.get(
            f"/api/v1/data-quality/policies/{sample_dq_policy.policy_id}/results",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "results" in data
        assert "total" in data
        assert len(data["results"]) >= 1


# ============================================================================
# Test Violations Management
# ============================================================================

class TestViolationsManagement:
    """Test violations management endpoints"""
    
    def test_list_violations_success(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_violation: DQViolation,
    ):
        """Test listing violations successfully"""
        response = client.get(
            "/api/v1/data-quality/violations",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "violations" in data
        assert "total" in data
        assert len(data["violations"]) >= 1
    
    def test_list_violations_filter_by_connection(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_violation: DQViolation,
        sample_connection: Connection,
    ):
        """Test filtering violations by connection"""
        response = client.get(
            "/api/v1/data-quality/violations",
            headers=admin_headers,
            params={"connection_id": str(sample_connection.connection_id)}
        )
        
        assert response.status_code == 200
        data = response.json()
        for violation in data["violations"]:
            assert violation["connection_id"] == str(sample_connection.connection_id)
    
    def test_list_violations_filter_by_policy(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_violation: DQViolation,
        sample_dq_policy: DQPolicy,
    ):
        """Test filtering by policy"""
        response = client.get(
            "/api/v1/data-quality/violations",
            headers=admin_headers,
            params={"policy_id": str(sample_dq_policy.policy_id)}
        )
        
        assert response.status_code == 200
        data = response.json()
        for violation in data["violations"]:
            assert violation["policy_id"] == str(sample_dq_policy.policy_id)
    
    def test_list_violations_filter_by_status(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_violation: DQViolation,
    ):
        """Test filtering by status"""
        response = client.get(
            "/api/v1/data-quality/violations",
            headers=admin_headers,
            params={"status": "active"}
        )
        
        assert response.status_code == 200
        data = response.json()
        for violation in data["violations"]:
            assert violation["status"] == "active"
    
    def test_get_violation_success(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_violation: DQViolation,
        sample_violation_sample: DQViolationSample,
    ):
        """Test getting violation details with samples"""
        response = client.get(
            f"/api/v1/data-quality/violations/{sample_violation.violation_id}",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["violation_id"] == str(sample_violation.violation_id)
        assert "samples" in data
        assert len(data["samples"]) >= 1
    
    def test_get_violation_not_found(
        self,
        client: TestClient,
        admin_headers: dict,
    ):
        """Test getting non-existent violation"""
        response = client.get(
            f"/api/v1/data-quality/violations/{uuid4()}",
            headers=admin_headers
        )
        
        assert response.status_code == 404
    
    def test_resolve_violation_success(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_violation: DQViolation,
    ):
        """Test resolving violation"""
        payload = {
            "status": "resolved",
            "resolution_notes": "Issue fixed in source data",
        }
        
        response = client.post(
            f"/api/v1/data-quality/violations/{sample_violation.violation_id}/resolve",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "resolved"
        assert data["resolution_notes"] == payload["resolution_notes"]
        assert data["resolved_at"] is not None
    
    def test_ignore_violation_success(
        self,
        client: TestClient,
        admin_headers: dict,
        db_session: Session,
        sample_dq_policy: DQPolicy,
        sample_connection: Connection,
    ):
        """Test ignoring violation"""
        # Create new violation to ignore
        violation = DQViolation(
            violation_id=uuid4(),
            policy_id=sample_dq_policy.policy_id,
            connection_id=sample_connection.connection_id,
            violation_count=10,
            total_records_checked=100,
            violation_percentage=Decimal("10.0"),
            status="active",
            violation_metadata={},
        )
        db_session.add(violation)
        db_session.commit()
        
        payload = {
            "status": "ignored",
            "resolution_notes": "Known issue, acceptable",
        }
        
        response = client.post(
            f"/api/v1/data-quality/violations/{violation.violation_id}/resolve",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ignored"
    
    def test_resolve_violation_requires_permission(
        self,
        client: TestClient,
        user_headers: dict,
        sample_violation: DQViolation,
    ):
        """Test resolving violation requires permission"""
        payload = {
            "status": "resolved",
            "resolution_notes": "Fixed",
        }
        
        response = client.post(
            f"/api/v1/data-quality/violations/{sample_violation.violation_id}/resolve",
            json=payload,
            headers=user_headers
        )
        
        assert response.status_code == 403


# ============================================================================
# Test Quality Metrics
# ============================================================================

class TestQualityMetrics:
    """Test quality metrics endpoints"""
    
    def test_get_connection_quality_metrics(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_connection: Connection,
        sample_dq_policy: DQPolicy,
    ):
        """Test getting quality metrics for connection"""
        response = client.get(
            f"/api/v1/data-quality/metrics/connection/{sample_connection.connection_id}",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["connection_id"] == str(sample_connection.connection_id)
        assert "quality_score" in data
        assert "completeness_score" in data
        assert "accuracy_score" in data
        assert "total_policies" in data
        assert "active_violations" in data
    
    def test_get_stream_quality_metrics(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_connection: Connection,
        sample_stream: Stream,
    ):
        """Test getting quality metrics for stream"""
        response = client.get(
            f"/api/v1/data-quality/metrics/connection/{sample_connection.connection_id}",
            headers=admin_headers,
            params={"stream_id": str(sample_stream.stream_id)}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["stream_id"] == str(sample_stream.stream_id)
    
    def test_get_metrics_invalid_connection(
        self,
        client: TestClient,
        admin_headers: dict,
    ):
        """Test getting metrics for invalid connection"""
        response = client.get(
            f"/api/v1/data-quality/metrics/connection/{uuid4()}",
            headers=admin_headers
        )
        
        assert response.status_code == 404
    
    def test_get_quality_dashboard(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_dq_policy: DQPolicy,
        sample_violation: DQViolation,
    ):
        """Test getting quality dashboard"""
        response = client.get(
            "/api/v1/data-quality/metrics/dashboard",
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "overall_score" in data
        assert "total_connections" in data
        assert "total_policies" in data
        assert "active_violations" in data
        assert "critical_violations" in data
        assert "top_failing_policies" in data
        assert "score_by_category" in data
        assert "trend" in data


# ============================================================================
# Test Data Profiling
# ============================================================================

class TestDataProfiling:
    """Test data profiling endpoints"""
    
    def test_profile_data_success(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_connection: Connection,
        sample_stream: Stream,
    ):
        """Test data profiling successfully"""
        payload = {
            "connection_id": str(sample_connection.connection_id),
            "stream_id": str(sample_stream.stream_id),
            "columns": ["email", "age"],
            "sample_size": 1000,
            "include_distributions": True,
            "include_patterns": True,
        }
        
        response = client.post(
            "/api/v1/data-quality/profiling/profile",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["connection_id"] == str(sample_connection.connection_id)
        assert "total_records" in data
        assert "column_profiles" in data
        assert "recommended_rules" in data
        assert len(data["column_profiles"]) > 0
    
    def test_profile_data_all_columns(
        self,
        client: TestClient,
        admin_headers: dict,
        sample_connection: Connection,
    ):
        """Test profiling all columns"""
        payload = {
            "connection_id": str(sample_connection.connection_id),
            "sample_size": 5000,
        }
        
        response = client.post(
            "/api/v1/data-quality/profiling/profile",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "column_profiles" in data
    
    def test_profile_data_invalid_connection(
        self,
        client: TestClient,
        admin_headers: dict,
    ):
        """Test profiling with invalid connection"""
        payload = {
            "connection_id": str(uuid4()),
            "sample_size": 1000,
        }
        
        response = client.post(
            "/api/v1/data-quality/profiling/profile",
            json=payload,
            headers=admin_headers
        )
        
        assert response.status_code == 404
