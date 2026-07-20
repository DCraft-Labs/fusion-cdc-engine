"""Data Quality Policies API endpoints.

Spec §3 (PDF3): Full CRUD for DQ policies (null_check, range_check, regex, custom_sql).
Each policy is scoped to a connection (and optionally a stream/table).
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.auth.dependencies import get_current_user
from app.database import get_db
from app.models.data_quality import DQPolicy, DQRuleResult, DQViolation

log = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class DQPolicyCreate(BaseModel):
    policy_name: str
    description: Optional[str] = None
    connection_id: Optional[UUID] = None
    stream_id: Optional[UUID] = None
    rule_type: str
    rule_definition: Dict[str, Any]
    target_columns: List[str] = []
    severity: str = "warning"
    action_on_failure: str = "log"
    threshold_type: Optional[str] = None
    threshold_value: Optional[Decimal] = None
    execution_schedule: Optional[str] = None
    is_active: bool = True


class DQPolicyUpdate(BaseModel):
    policy_name: Optional[str] = None
    description: Optional[str] = None
    rule_definition: Optional[Dict[str, Any]] = None
    target_columns: Optional[List[str]] = None
    severity: Optional[str] = None
    action_on_failure: Optional[str] = None
    threshold_type: Optional[str] = None
    threshold_value: Optional[Decimal] = None
    execution_schedule: Optional[str] = None
    is_active: Optional[bool] = None


class DQPolicyResponse(BaseModel):
    policy_id: UUID
    policy_name: str
    description: Optional[str]
    connection_id: Optional[UUID]
    stream_id: Optional[UUID]
    rule_type: str
    rule_definition: Dict[str, Any]
    target_columns: Any
    severity: str
    action_on_failure: str
    threshold_type: Optional[str]
    threshold_value: Optional[Any]
    execution_schedule: Optional[str]
    is_active: bool

    model_config = {"from_attributes": True}


class DQViolationResponse(BaseModel):
    violation_id: UUID
    policy_id: UUID
    connection_id: UUID
    stream_id: Optional[UUID]
    detected_at: Any
    violation_count: int
    total_records_checked: int
    violation_percentage: Any
    status: str

    model_config = {"from_attributes": True}


class DQRuleResultResponse(BaseModel):
    result_id: UUID
    policy_id: UUID
    connection_id: UUID
    executed_at: Any
    passed: bool
    records_checked: int
    violations_found: int

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_policy(db: Session, policy_id: UUID) -> DQPolicy:
    policy = db.query(DQPolicy).filter(DQPolicy.policy_id == policy_id).first()
    if not policy:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DQ policy {policy_id} not found",
        )
    return policy


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("", status_code=status.HTTP_201_CREATED, response_model=DQPolicyResponse)
async def create_dq_policy(
    payload: DQPolicyCreate,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Create a new data quality policy."""
    policy = DQPolicy(**payload.model_dump())
    db.add(policy)
    db.commit()
    db.refresh(policy)
    log.info("DQ policy created: %s by user %s", policy.policy_id, getattr(user, "user_id", "?"))
    return DQPolicyResponse.model_validate(policy)


@router.get("", response_model=Dict[str, Any])
async def list_dq_policies(
    connection_id: Optional[UUID] = Query(None),
    is_active: Optional[bool] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """List DQ policies, optionally filtered by connection or active status."""
    q = db.query(DQPolicy)
    if connection_id:
        q = q.filter(DQPolicy.connection_id == connection_id)
    if is_active is not None:
        q = q.filter(DQPolicy.is_active == is_active)

    total = q.count()
    policies = q.offset((page - 1) * page_size).limit(page_size).all()
    return {
        "policies": [DQPolicyResponse.model_validate(p) for p in policies],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
    }


@router.get("/{policy_id}", response_model=DQPolicyResponse)
async def get_dq_policy(
    policy_id: UUID,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get DQ policy details."""
    return DQPolicyResponse.model_validate(_get_policy(db, policy_id))


@router.put("/{policy_id}", response_model=DQPolicyResponse)
async def update_dq_policy(
    policy_id: UUID,
    payload: DQPolicyUpdate,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Update a DQ policy."""
    policy = _get_policy(db, policy_id)
    for field, value in payload.model_dump(exclude_none=True).items():
        setattr(policy, field, value)
    db.commit()
    db.refresh(policy)
    log.info("DQ policy %s updated by user %s", policy_id, getattr(user, "user_id", "?"))
    return DQPolicyResponse.model_validate(policy)


@router.delete("/{policy_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dq_policy(
    policy_id: UUID,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Delete (hard-delete) a DQ policy and all its violations/results."""
    policy = _get_policy(db, policy_id)
    db.delete(policy)
    db.commit()
    log.info("DQ policy %s deleted by user %s", policy_id, getattr(user, "user_id", "?"))


@router.get("/{policy_id}/executions", response_model=Dict[str, Any])
async def list_dq_executions(
    policy_id: UUID,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """List DQ rule execution results for a policy."""
    _get_policy(db, policy_id)
    q = db.query(DQRuleResult).filter(DQRuleResult.policy_id == policy_id)
    total = q.count()
    results = q.order_by(DQRuleResult.executed_at.desc()).offset((page - 1) * page_size).limit(page_size).all()
    return {
        "executions": [DQRuleResultResponse.model_validate(r) for r in results],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/{policy_id}/violations", response_model=Dict[str, Any])
async def list_dq_violations(
    policy_id: UUID,
    status_filter: Optional[str] = Query(None, alias="status"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """List DQ violations for a policy."""
    _get_policy(db, policy_id)
    q = db.query(DQViolation).filter(DQViolation.policy_id == policy_id)
    if status_filter:
        q = q.filter(DQViolation.status == status_filter)
    total = q.count()
    violations = q.order_by(DQViolation.detected_at.desc()).offset((page - 1) * page_size).limit(page_size).all()
    return {
        "violations": [DQViolationResponse.model_validate(v) for v in violations],
        "total": total,
        "page": page,
        "page_size": page_size,
    }
