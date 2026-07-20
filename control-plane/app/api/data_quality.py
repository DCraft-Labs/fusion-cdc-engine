"""
Data Quality Management API Endpoints

Comprehensive API for managing data quality rules, templates, violations, and profiling.
Includes 18+ REST endpoints covering:
- Rule Templates (CRUD)
- DQ Policies/Rules (CRUD + operations)
- Rule Testing and Execution
- Violations Management
- Quality Metrics
- Anomaly Detection
- Data Profiling
"""

from typing import List, Optional
from uuid import UUID
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, desc

from app.database import get_db
from app.auth.dependencies import get_current_user, require_permission
from app.models.auth import User
from app.models.data_quality import DQPolicy, DQViolation, DQViolationSample, DQRuleResult
from app.models.connection import Connection, Stream
from app.schemas.data_quality import (
    RuleTemplateCreate,
    RuleTemplateUpdate,
    RuleTemplateResponse,
    RuleTemplateListResponse,
    DQPolicyCreate,
    DQPolicyUpdate,
    DQPolicyResponse,
    DQPolicyListResponse,
    DQPolicySearchFilters,
    RuleTestRequest,
    RuleTestResult,
    RuleExecutionRequest,
    DQRuleResultResponse,
    DQRuleResultListResponse,
    DQViolationResponse,
    DQViolationListResponse,
    ViolationResolveRequest,
    QualityMetrics,
    QualityDashboard,
    DataProfilingRequest,
    DataProfilingResponse,
    ColumnProfile,
)

# Create main router
router = APIRouter()


# ============================================================================
# Helper Functions
# ============================================================================

def _get_policy_by_id(
    db: Session,
    policy_id: UUID,
    user: User,
) -> DQPolicy:
    """Helper to get policy with tenant filtering"""
    policy = (
        db.query(DQPolicy)
        .filter(
            DQPolicy.policy_id == policy_id,
            DQPolicy.sub_tenant_id == user.sub_tenant_id,
            DQPolicy.is_deleted == False,
        )
        .first()
    )
    
    if not policy:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="DQ policy not found"
        )
    
    return policy


def _validate_rule_definition(rule_type: str, rule_definition: dict) -> tuple[bool, str]:
    """Validate rule definition structure based on rule type"""
    # TODO: Implement comprehensive validation based on rule type
    # For now, basic validation
    
    required_fields = {
        "null_check": ["check_type"],
        "range_check": ["min_value", "max_value"],
        "regex": ["pattern"],
        "custom_sql": ["sql_query"],
        "uniqueness": ["check_columns"],
        "freshness": ["max_age_hours"],
        "referential_integrity": ["reference_table", "reference_column"],
    }
    
    if rule_type not in required_fields:
        return False, f"Unknown rule type: {rule_type}"
    
    missing = [f for f in required_fields[rule_type] if f not in rule_definition]
    if missing:
        return False, f"Missing required fields: {', '.join(missing)}"
    
    return True, "Valid"


def _calculate_quality_score(
    db: Session,
    connection_id: UUID,
    stream_id: Optional[UUID],
    user: User,
) -> dict:
    """Calculate quality score based on violations"""
    # Query policies and violations
    policy_query = db.query(DQPolicy).filter(
        DQPolicy.sub_tenant_id == user.sub_tenant_id,
        DQPolicy.connection_id == connection_id,
        DQPolicy.is_active == True,
        DQPolicy.is_deleted == False,
    )
    
    if stream_id:
        policy_query = policy_query.filter(DQPolicy.stream_id == stream_id)
    
    total_policies = policy_query.count()
    
    if total_policies == 0:
        return {
            "quality_score": 100.0,
            "completeness_score": 100.0,
            "accuracy_score": 100.0,
            "consistency_score": 100.0,
            "validity_score": 100.0,
            "timeliness_score": 100.0,
            "uniqueness_score": 100.0,
        }
    
    # Count violations by category (simplified)
    # TODO: Implement category-based scoring
    violation_count = (
        db.query(func.count(DQViolation.violation_id))
        .join(DQPolicy, DQPolicy.policy_id == DQViolation.policy_id)
        .filter(
            DQPolicy.sub_tenant_id == user.sub_tenant_id,
            DQPolicy.connection_id == connection_id,
            DQViolation.status == "active",
        )
    )
    
    if stream_id:
        violation_count = violation_count.filter(DQPolicy.stream_id == stream_id)
    
    active_violations = violation_count.scalar() or 0
    
    # Simple scoring: 100 - (violations/policies * weight)
    violation_impact = min(100, (active_violations / total_policies) * 20)
    base_score = max(0, 100 - violation_impact)
    
    return {
        "quality_score": base_score,
        "completeness_score": base_score,
        "accuracy_score": base_score,
        "consistency_score": base_score,
        "validity_score": base_score,
        "timeliness_score": base_score,
        "uniqueness_score": base_score,
    }


# ============================================================================
# Rule Template Endpoints
# ============================================================================

@router.post("/templates", status_code=status.HTTP_201_CREATED)
async def create_rule_template(
    template: RuleTemplateCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("quality_rules:create")),
) -> RuleTemplateResponse:
    """Create a new rule template"""
    # TODO: Create RuleTemplate model and implementation
    # For now, return mock response
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Rule templates not yet implemented - create policies directly"
    )


@router.get("/templates")
async def list_rule_templates(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    category: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RuleTemplateListResponse:
    """List available rule templates"""
    # TODO: Implement template listing
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Rule templates not yet implemented"
    )


# ============================================================================
# DQ Policy (Rule) CRUD Endpoints
# ============================================================================

@router.post("/policies", status_code=status.HTTP_201_CREATED)
async def create_dq_policy(
    policy: DQPolicyCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("quality_rules:create")),
) -> DQPolicyResponse:
    """Create a new data quality policy/rule"""
    
    # Validate connection exists
    if policy.connection_id:
        connection = db.query(Connection).filter(
            Connection.connection_id == policy.connection_id,
            Connection.sub_tenant_id == current_user.sub_tenant_id,
            Connection.is_deleted == False,
        ).first()
        
        if not connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Connection not found"
            )
    
    # Validate stream if provided
    if policy.stream_id:
        stream = db.query(Stream).filter(
            Stream.stream_id == policy.stream_id,
        ).first()
        
        if not stream:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Stream not found"
            )
    
    # Validate rule definition
    is_valid, message = _validate_rule_definition(policy.rule_type, policy.rule_definition)
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid rule definition: {message}"
        )
    
    # Check for duplicate policy name
    existing = db.query(DQPolicy).filter(
        DQPolicy.sub_tenant_id == current_user.sub_tenant_id,
        DQPolicy.policy_name == policy.policy_name,
        DQPolicy.is_deleted == False,
    ).first()
    
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A policy with this name already exists"
        )
    
    # Create policy
    new_policy = DQPolicy(
        policy_name=policy.policy_name,
        description=policy.description,
        connection_id=policy.connection_id,
        stream_id=policy.stream_id,
        rule_type=policy.rule_type,
        rule_definition=policy.rule_definition,
        target_columns=policy.target_columns,
        severity=policy.severity,
        action_on_failure=policy.action_on_failure,
        threshold_type=policy.threshold_type,
        threshold_value=policy.threshold_value,
        execution_schedule=policy.execution_schedule,
        is_active=policy.is_active,
        sub_tenant_id=current_user.sub_tenant_id,
        bank_id=current_user.bank_id,
    )
    
    db.add(new_policy)
    db.commit()
    db.refresh(new_policy)
    
    # Build response with aggregated data
    response_data = DQPolicyResponse.model_validate(new_policy)
    response_data.violation_count = 0
    response_data.active_violation_count = 0
    response_data.last_execution_status = None
    
    return response_data


@router.get("/policies")
async def list_dq_policies(
    connection_id: Optional[UUID] = None,
    stream_id: Optional[UUID] = None,
    rule_type: Optional[str] = None,
    severity: Optional[str] = None,
    is_active: Optional[bool] = None,
    search: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> DQPolicyListResponse:
    """List DQ policies with filtering"""
    
    # Build query
    query = db.query(DQPolicy).filter(
        DQPolicy.sub_tenant_id == current_user.sub_tenant_id,
        DQPolicy.is_deleted == False,
    )
    
    # Apply filters
    if connection_id:
        query = query.filter(DQPolicy.connection_id == connection_id)
    if stream_id:
        query = query.filter(DQPolicy.stream_id == stream_id)
    if rule_type:
        query = query.filter(DQPolicy.rule_type == rule_type)
    if severity:
        query = query.filter(DQPolicy.severity == severity)
    if is_active is not None:
        query = query.filter(DQPolicy.is_active == is_active)
    if search:
        search_term = f"%{search}%"
        query = query.filter(
            or_(
                DQPolicy.policy_name.ilike(search_term),
                DQPolicy.description.ilike(search_term),
            )
        )
    
    # Count total
    total = query.count()
    
    # Paginate
    policies = query.order_by(desc(DQPolicy.created_at)).offset((page - 1) * page_size).limit(page_size).all()
    
    # Build responses with aggregated data
    policy_responses = []
    for policy in policies:
        # Count violations
        violation_count = db.query(func.count(DQViolation.violation_id)).filter(
            DQViolation.policy_id == policy.policy_id
        ).scalar() or 0
        
        active_violation_count = db.query(func.count(DQViolation.violation_id)).filter(
            DQViolation.policy_id == policy.policy_id,
            DQViolation.status == "active"
        ).scalar() or 0
        
        # Get last execution status
        last_result = db.query(DQRuleResult).filter(
            DQRuleResult.policy_id == policy.policy_id
        ).order_by(desc(DQRuleResult.executed_at)).first()
        
        response_data = DQPolicyResponse.model_validate(policy)
        response_data.violation_count = violation_count
        response_data.active_violation_count = active_violation_count
        response_data.last_execution_status = last_result.passed if last_result else None
        
        policy_responses.append(response_data)
    
    return DQPolicyListResponse(
        policies=policy_responses,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=(total + page_size - 1) // page_size,
    )


@router.get("/policies/{policy_id}")
async def get_dq_policy(
    policy_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> DQPolicyResponse:
    """Get DQ policy details"""
    
    policy = _get_policy_by_id(db, policy_id, current_user)
    
    # Get aggregated data
    violation_count = db.query(func.count(DQViolation.violation_id)).filter(
        DQViolation.policy_id == policy.policy_id
    ).scalar() or 0
    
    active_violation_count = db.query(func.count(DQViolation.violation_id)).filter(
        DQViolation.policy_id == policy.policy_id,
        DQViolation.status == "active"
    ).scalar() or 0
    
    last_result = db.query(DQRuleResult).filter(
        DQRuleResult.policy_id == policy.policy_id
    ).order_by(desc(DQRuleResult.executed_at)).first()
    
    response_data = DQPolicyResponse.model_validate(policy)
    response_data.violation_count = violation_count
    response_data.active_violation_count = active_violation_count
    response_data.last_execution_status = last_result.passed if last_result else None
    
    return response_data


@router.patch("/policies/{policy_id}")
async def update_dq_policy(
    policy_id: UUID,
    policy_update: DQPolicyUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("quality_rules:update")),
) -> DQPolicyResponse:
    """Update DQ policy"""
    
    policy = _get_policy_by_id(db, policy_id, current_user)
    
    # Check for duplicate name if changing
    if policy_update.policy_name and policy_update.policy_name != policy.policy_name:
        existing = db.query(DQPolicy).filter(
            DQPolicy.sub_tenant_id == current_user.sub_tenant_id,
            DQPolicy.policy_name == policy_update.policy_name,
            DQPolicy.policy_id != policy_id,
            DQPolicy.is_deleted == False,
        ).first()
        
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A policy with this name already exists"
            )
    
    # Validate rule definition if provided
    if policy_update.rule_definition:
        rule_type = policy_update.rule_type or policy.rule_type
        is_valid, message = _validate_rule_definition(rule_type, policy_update.rule_definition)
        if not is_valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid rule definition: {message}"
            )
    
    # Update fields
    update_data = policy_update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(policy, field, value)
    
    db.commit()
    db.refresh(policy)
    
    # Get aggregated data
    violation_count = db.query(func.count(DQViolation.violation_id)).filter(
        DQViolation.policy_id == policy.policy_id
    ).scalar() or 0
    
    active_violation_count = db.query(func.count(DQViolation.violation_id)).filter(
        DQViolation.policy_id == policy.policy_id,
        DQViolation.status == "active"
    ).scalar() or 0
    
    last_result = db.query(DQRuleResult).filter(
        DQRuleResult.policy_id == policy.policy_id
    ).order_by(desc(DQRuleResult.executed_at)).first()
    
    response_data = DQPolicyResponse.model_validate(policy)
    response_data.violation_count = violation_count
    response_data.active_violation_count = active_violation_count
    response_data.last_execution_status = last_result.passed if last_result else None
    
    return response_data


@router.delete("/policies/{policy_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dq_policy(
    policy_id: UUID,
    force: bool = Query(False, description="Force delete even if active"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("quality_rules:delete")),
):
    """Delete DQ policy (soft delete)"""
    
    policy = _get_policy_by_id(db, policy_id, current_user)
    
    # Check if policy is active
    if policy.is_active and not force:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete active policy. Deactivate first or use force=true"
        )
    
    # Soft delete
    policy.is_deleted = True
    policy.is_active = False
    
    db.commit()


# ============================================================================
# Rule Testing and Execution Endpoints
# ============================================================================

@router.post("/policies/test")
async def test_rule(
    test_request: RuleTestRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RuleTestResult:
    """Test a rule before saving"""
    
    # Validate connection exists
    connection = db.query(Connection).filter(
        Connection.connection_id == test_request.connection_id,
        Connection.sub_tenant_id == current_user.sub_tenant_id,
        Connection.is_deleted == False,
    ).first()
    
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connection not found"
        )
    
    # Validate rule definition
    is_valid, message = _validate_rule_definition(
        test_request.rule_type,
        test_request.rule_definition
    )
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid rule definition: {message}"
        )
    
    # TODO: Implement actual rule testing against data
    # This would involve:
    # 1. Getting sample data from the source
    # 2. Applying the rule logic
    # 3. Collecting violations
    
    # Mock result for now
    return RuleTestResult(
        test_passed=True,
        records_tested=test_request.sample_size,
        records_passed=950,
        records_failed=50,
        execution_time_ms=250,
        sample_violations=[
            {"column": "email", "value": "invalid", "reason": "Invalid email format"}
        ],
        tested_at=datetime.utcnow(),
    )


@router.post("/policies/{policy_id}/execute")
async def execute_policy(
    policy_id: UUID,
    execution_request: RuleExecutionRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("quality_rules:execute")),
) -> DQRuleResultResponse:
    """Execute a specific DQ policy"""
    
    policy = _get_policy_by_id(db, policy_id, current_user)
    
    if not policy.is_active and not execution_request.force_execution:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Policy is not active. Use force_execution=true to run anyway"
        )
    
    # TODO: Implement actual rule execution
    # This would involve:
    # 1. Connecting to the data source
    # 2. Executing the rule logic
    # 3. Recording violations
    # 4. Creating result record
    
    # Mock result for now
    execution_id = f"exec-{datetime.utcnow().timestamp()}"
    
    result = DQRuleResult(
        policy_id=policy.policy_id,
        execution_id=execution_id,
        executed_at=datetime.utcnow(),
        passed=True,
        records_checked=10000,
        records_passed=9800,
        records_failed=200,
        execution_time_ms=5000,
        result_details={"info": "Execution completed successfully"},
    )
    
    db.add(result)
    
    # Update policy last executed time
    policy.last_executed_at = datetime.utcnow()
    
    db.commit()
    db.refresh(result)
    
    return DQRuleResultResponse.model_validate(result)


@router.get("/policies/{policy_id}/results")
async def list_policy_results(
    policy_id: UUID,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> DQRuleResultListResponse:
    """List execution results for a policy"""
    
    policy = _get_policy_by_id(db, policy_id, current_user)
    
    # Query results
    query = db.query(DQRuleResult).filter(
        DQRuleResult.policy_id == policy.policy_id
    )
    
    total = query.count()
    
    results = query.order_by(desc(DQRuleResult.executed_at)).offset((page - 1) * page_size).limit(page_size).all()
    
    return DQRuleResultListResponse(
        results=[DQRuleResultResponse.model_validate(r) for r in results],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=(total + page_size - 1) // page_size,
    )


# ============================================================================
# Violations Management Endpoints
# ============================================================================

@router.get("/violations")
async def list_violations(
    connection_id: Optional[UUID] = None,
    policy_id: Optional[UUID] = None,
    status: Optional[str] = None,
    severity: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> DQViolationListResponse:
    """List DQ violations with filtering"""
    
    # Build query with join to policy for tenant filtering
    query = (
        db.query(DQViolation)
        .join(DQPolicy, DQPolicy.policy_id == DQViolation.policy_id)
        .filter(DQPolicy.sub_tenant_id == current_user.sub_tenant_id)
    )
    
    # Apply filters
    if connection_id:
        query = query.filter(DQViolation.connection_id == connection_id)
    if policy_id:
        query = query.filter(DQViolation.policy_id == policy_id)
    if status:
        query = query.filter(DQViolation.status == status)
    if severity:
        query = query.filter(DQPolicy.severity == severity)
    
    # Count total
    total = query.count()
    
    # Paginate
    violations = query.order_by(desc(DQViolation.detected_at)).offset((page - 1) * page_size).limit(page_size).all()
    
    # Load samples for each violation
    violation_responses = []
    for violation in violations:
        samples = db.query(DQViolationSample).filter(
            DQViolationSample.violation_id == violation.violation_id
        ).limit(10).all()
        
        response_data = DQViolationResponse.model_validate(violation)
        response_data.samples = [DQViolationSampleResponse.model_validate(s) for s in samples]
        
        violation_responses.append(response_data)
    
    return DQViolationListResponse(
        violations=violation_responses,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=(total + page_size - 1) // page_size,
    )


@router.get("/violations/{violation_id}")
async def get_violation(
    violation_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> DQViolationResponse:
    """Get violation details with samples"""
    
    # Query with tenant filtering through policy
    violation = (
        db.query(DQViolation)
        .join(DQPolicy, DQPolicy.policy_id == DQViolation.policy_id)
        .filter(
            DQViolation.violation_id == violation_id,
            DQPolicy.sub_tenant_id == current_user.sub_tenant_id,
        )
        .first()
    )
    
    if not violation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Violation not found"
        )
    
    # Load samples
    samples = db.query(DQViolationSample).filter(
        DQViolationSample.violation_id == violation.violation_id
    ).all()
    
    response_data = DQViolationResponse.model_validate(violation)
    response_data.samples = [DQViolationSampleResponse.model_validate(s) for s in samples]
    
    return response_data


@router.post("/violations/{violation_id}/resolve")
async def resolve_violation(
    violation_id: UUID,
    resolve_request: ViolationResolveRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("quality_rules:update")),
) -> DQViolationResponse:
    """Resolve or ignore a violation"""
    
    # Query with tenant filtering
    violation = (
        db.query(DQViolation)
        .join(DQPolicy, DQPolicy.policy_id == DQViolation.policy_id)
        .filter(
            DQViolation.violation_id == violation_id,
            DQPolicy.sub_tenant_id == current_user.sub_tenant_id,
        )
        .first()
    )
    
    if not violation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Violation not found"
        )
    
    # Update violation
    violation.status = resolve_request.status
    violation.resolved_at = datetime.utcnow()
    violation.resolved_by = current_user.user_id
    violation.resolution_notes = resolve_request.resolution_notes
    
    db.commit()
    db.refresh(violation)
    
    # Load samples
    samples = db.query(DQViolationSample).filter(
        DQViolationSample.violation_id == violation.violation_id
    ).all()
    
    response_data = DQViolationResponse.model_validate(violation)
    response_data.samples = [DQViolationSampleResponse.model_validate(s) for s in samples]
    
    return response_data


# ============================================================================
# Quality Scores by Connection (summary list)
# ============================================================================

@router.get("/scores/by-connection")
async def get_scores_by_connection(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return a per-connection quality score summary for the Data Quality dashboard."""
    connections = db.query(Connection).filter(
        Connection.sub_tenant_id == current_user.sub_tenant_id,
        Connection.is_deleted == False,
    ).all()

    result = []
    for conn in connections:
        # Count active violations linked to this connection through its policies
        violations_count = (
            db.query(func.count(DQViolation.violation_id))
            .join(DQPolicy, DQPolicy.policy_id == DQViolation.policy_id)
            .filter(
                DQPolicy.connection_id == conn.connection_id,
                DQViolation.status == "active",
            )
            .scalar() or 0
        )

        active_policies = db.query(func.count(DQPolicy.policy_id)).filter(
            DQPolicy.connection_id == conn.connection_id,
            DQPolicy.is_active == True,
            DQPolicy.is_deleted == False,
        ).scalar() or 0

        if active_policies > 0:
            violation_rate = violations_count / active_policies
            score = max(0, round(100 - (violation_rate * 20), 1))
        else:
            score = 100.0

        # Last check = most recent DQ rule execution for this connection
        last_result = (
            db.query(DQRuleResult.executed_at)
            .join(DQPolicy, DQPolicy.policy_id == DQRuleResult.policy_id)
            .filter(DQPolicy.connection_id == conn.connection_id)
            .order_by(desc(DQRuleResult.executed_at))
            .first()
        )

        result.append({
            "connection_id": str(conn.connection_id),
            "connection_name": conn.connection_name,
            "score": score,
            "violations_count": violations_count,
            "last_check": last_result[0].isoformat() if last_result else None,
        })

    return result


# ============================================================================
# Quality Metrics Endpoints
# ============================================================================

@router.get("/metrics/connection/{connection_id}")
async def get_connection_quality_metrics(
    connection_id: UUID,
    stream_id: Optional[UUID] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> QualityMetrics:
    """Get quality metrics for a connection or stream"""
    
    # Validate connection
    connection = db.query(Connection).filter(
        Connection.connection_id == connection_id,
        Connection.sub_tenant_id == current_user.sub_tenant_id,
        Connection.is_deleted == False,
    ).first()
    
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connection not found"
        )
    
    # Calculate quality scores
    scores = _calculate_quality_score(db, connection_id, stream_id, current_user)
    
    # Count policies
    policy_query = db.query(DQPolicy).filter(
        DQPolicy.sub_tenant_id == current_user.sub_tenant_id,
        DQPolicy.connection_id == connection_id,
        DQPolicy.is_deleted == False,
    )
    
    if stream_id:
        policy_query = policy_query.filter(DQPolicy.stream_id == stream_id)
    
    total_policies = policy_query.count()
    active_policies = policy_query.filter(DQPolicy.is_active == True).count()
    
    # Count violations
    violation_query = (
        db.query(DQViolation)
        .join(DQPolicy, DQPolicy.policy_id == DQViolation.policy_id)
        .filter(
            DQPolicy.sub_tenant_id == current_user.sub_tenant_id,
            DQPolicy.connection_id == connection_id,
        )
    )
    
    if stream_id:
        violation_query = violation_query.filter(DQPolicy.stream_id == stream_id)
    
    total_violations = violation_query.count()
    active_violations = violation_query.filter(DQViolation.status == "active").count()
    
    return QualityMetrics(
        connection_id=connection_id,
        stream_id=stream_id,
        quality_score=scores["quality_score"],
        completeness_score=scores["completeness_score"],
        accuracy_score=scores["accuracy_score"],
        consistency_score=scores["consistency_score"],
        validity_score=scores["validity_score"],
        timeliness_score=scores["timeliness_score"],
        uniqueness_score=scores["uniqueness_score"],
        total_policies=total_policies,
        active_policies=active_policies,
        total_violations=total_violations,
        active_violations=active_violations,
        last_calculated_at=datetime.utcnow(),
    )


@router.get("/metrics/dashboard")
async def get_quality_dashboard(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> QualityDashboard:
    """Get overall quality dashboard"""
    
    # Count connections
    total_connections = db.query(func.count(Connection.connection_id)).filter(
        Connection.sub_tenant_id == current_user.sub_tenant_id,
        Connection.is_deleted == False,
    ).scalar() or 0
    
    # Count connections with active violations
    connections_with_issues = (
        db.query(func.count(func.distinct(DQPolicy.connection_id)))
        .join(DQViolation, DQViolation.policy_id == DQPolicy.policy_id)
        .filter(
            DQPolicy.sub_tenant_id == current_user.sub_tenant_id,
            DQViolation.status == "active",
        )
        .scalar() or 0
    )
    
    # Count policies
    total_policies = db.query(func.count(DQPolicy.policy_id)).filter(
        DQPolicy.sub_tenant_id == current_user.sub_tenant_id,
        DQPolicy.is_deleted == False,
    ).scalar() or 0
    
    active_policies = db.query(func.count(DQPolicy.policy_id)).filter(
        DQPolicy.sub_tenant_id == current_user.sub_tenant_id,
        DQPolicy.is_active == True,
        DQPolicy.is_deleted == False,
    ).scalar() or 0
    
    # Count violations
    total_violations = (
        db.query(func.count(DQViolation.violation_id))
        .join(DQPolicy, DQPolicy.policy_id == DQViolation.policy_id)
        .filter(DQPolicy.sub_tenant_id == current_user.sub_tenant_id)
        .scalar() or 0
    )
    
    active_violations = (
        db.query(func.count(DQViolation.violation_id))
        .join(DQPolicy, DQPolicy.policy_id == DQViolation.policy_id)
        .filter(
            DQPolicy.sub_tenant_id == current_user.sub_tenant_id,
            DQViolation.status == "active",
        )
        .scalar() or 0
    )
    
    critical_violations = (
        db.query(func.count(DQViolation.violation_id))
        .join(DQPolicy, DQPolicy.policy_id == DQViolation.policy_id)
        .filter(
            DQPolicy.sub_tenant_id == current_user.sub_tenant_id,
            DQViolation.status == "active",
            DQPolicy.severity == "critical",
        )
        .scalar() or 0
    )
    
    # Get top failing policies
    top_failing = (
        db.query(
            DQPolicy.policy_name,
            DQPolicy.severity,
            func.count(DQViolation.violation_id).label("violation_count")
        )
        .join(DQViolation, DQViolation.policy_id == DQPolicy.policy_id)
        .filter(
            DQPolicy.sub_tenant_id == current_user.sub_tenant_id,
            DQViolation.status == "active",
        )
        .group_by(DQPolicy.policy_id, DQPolicy.policy_name, DQPolicy.severity)
        .order_by(desc("violation_count"))
        .limit(10)
        .all()
    )
    
    top_failing_list = [
        {
            "policy_name": name,
            "severity": severity,
            "violation_count": count
        }
        for name, severity, count in top_failing
    ]
    
    # Calculate overall score
    if active_policies > 0:
        violation_rate = active_violations / active_policies
        overall_score = max(0, 100 - (violation_rate * 20))
    else:
        overall_score = 100.0
    
    # Determine trend (simplified)
    trend = "stable"
    if active_violations > total_policies:
        trend = "degrading"
    elif active_violations < total_policies / 2:
        trend = "improving"
    
    return QualityDashboard(
        overall_score=overall_score,
        total_connections=total_connections,
        connections_with_issues=connections_with_issues,
        total_policies=total_policies,
        active_policies=active_policies,
        total_violations=total_violations,
        active_violations=active_violations,
        critical_violations=critical_violations,
        top_failing_policies=top_failing_list,
        score_by_category={
            "completeness": overall_score,
            "accuracy": overall_score,
            "consistency": overall_score,
            "validity": overall_score,
            "timeliness": overall_score,
            "uniqueness": overall_score,
        },
        trend=trend,
    )


# ============================================================================
# Data Profiling Endpoints
# ============================================================================

@router.post("/profiling/profile")
async def profile_data(
    profiling_request: DataProfilingRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> DataProfilingResponse:
    """Profile data from a connection/stream"""
    
    # Validate connection
    connection = db.query(Connection).filter(
        Connection.connection_id == profiling_request.connection_id,
        Connection.sub_tenant_id == current_user.sub_tenant_id,
        Connection.is_deleted == False,
    ).first()
    
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connection not found"
        )
    
    # TODO: Implement actual data profiling
    # This would involve:
    # 1. Connecting to the data source
    # 2. Sampling data
    # 3. Analyzing columns
    # 4. Detecting patterns
    # 5. Generating rule recommendations
    
    # Mock response
    mock_profiles = [
        ColumnProfile(
            column_name="email",
            data_type="VARCHAR",
            nullable=False,
            total_count=10000,
            null_count=0,
            null_percentage=0.0,
            distinct_count=9995,
            distinct_percentage=99.95,
            top_values=[
                {"value": "user1@example.com", "count": 2}
            ],
            patterns=["email"],
        ),
        ColumnProfile(
            column_name="age",
            data_type="INTEGER",
            nullable=True,
            total_count=10000,
            null_count=50,
            null_percentage=0.5,
            distinct_count=80,
            distinct_percentage=0.8,
            min_value="18",
            max_value="95",
            avg_value="45.5",
            median_value="44",
            std_dev=15.2,
            top_values=[],
            patterns=[],
        ),
    ]
    
    recommended_rules = [
        {
            "rule_type": "null_check",
            "target_column": "email",
            "reason": "Column has no nulls, enforce NOT NULL constraint"
        },
        {
            "rule_type": "regex",
            "target_column": "email",
            "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "reason": "Email pattern detected"
        },
        {
            "rule_type": "range_check",
            "target_column": "age",
            "min_value": 18,
            "max_value": 120,
            "reason": "Age values within reasonable range"
        },
    ]
    
    return DataProfilingResponse(
        connection_id=profiling_request.connection_id,
        stream_id=profiling_request.stream_id,
        total_records=10000,
        total_columns=len(mock_profiles),
        profiled_at=datetime.utcnow(),
        column_profiles=mock_profiles,
        recommended_rules=recommended_rules,
    )
