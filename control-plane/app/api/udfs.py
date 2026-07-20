"""UDF Catalog API endpoints"""

import math
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.auth.dependencies import get_current_user
from app.database import get_db
from app.models.transformation import UDFCatalog
from app.schemas.transformation import (
    UDFCreate,
    UDFListResponse,
    UDFResponse,
    UDFUpdate,
)

router = APIRouter()


# ============================================================================
# Helpers
# ============================================================================

def _get_udf(db: Session, udf_id: UUID, user) -> UDFCatalog:
    """Fetch active UDF scoped to current tenant, raise 404 if missing."""
    udf = (
        db.query(UDFCatalog)
        .filter(
            UDFCatalog.udf_id == udf_id,
            UDFCatalog.sub_tenant_id == user.sub_tenant_id,
            UDFCatalog.is_active == True,
        )
        .first()
    )
    if not udf:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"UDF {udf_id} not found",
        )
    return udf


# ============================================================================
# Endpoints
# ============================================================================

@router.post("", status_code=status.HTTP_201_CREATED, response_model=UDFResponse)
async def register_udf(
    payload: UDFCreate,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Register a new UDF in the catalog."""
    existing = (
        db.query(UDFCatalog)
        .filter(
            UDFCatalog.sub_tenant_id == user.sub_tenant_id,
            UDFCatalog.udf_name == payload.udf_name,
            UDFCatalog.is_active == True,  # only block if still active
        )
        .first()
    )
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"A UDF named '{payload.udf_name}' already exists in this tenant",
        )
    # Reactivate a deactivated UDF record to avoid unique-constraint conflicts
    deactivated = (
        db.query(UDFCatalog)
        .filter(
            UDFCatalog.sub_tenant_id == user.sub_tenant_id,
            UDFCatalog.udf_name == payload.udf_name,
            UDFCatalog.is_active == False,
        )
        .first()
    )
    if deactivated:
        for field, value in {
            "function_code": payload.function_code,
            "language": payload.language,
            "return_type": payload.return_type,
            "parameters": payload.parameters,
            "category": payload.category,
            "tags": payload.tags,
            "description": payload.description,
            "is_validated": False,
            "is_active": True,
            "created_by": user.user_id,
        }.items():
            setattr(deactivated, field, value)
        db.commit()
        db.refresh(deactivated)
        return deactivated

    udf = UDFCatalog(
        udf_name=payload.udf_name,
        description=payload.description,
        function_code=payload.function_code,
        language=payload.language,
        return_type=payload.return_type,
        parameters=payload.parameters,
        category=payload.category,
        tags=payload.tags,
        is_validated=False,
        is_active=True,
        sub_tenant_id=user.sub_tenant_id,
        bank_id=user.bank_id,
        created_by=user.user_id,
    )
    db.add(udf)
    db.commit()
    db.refresh(udf)
    return udf


@router.get("", response_model=UDFListResponse)
async def list_udfs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    language: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """List UDFs for the current tenant with optional filters."""
    query = db.query(UDFCatalog).filter(
        UDFCatalog.sub_tenant_id == user.sub_tenant_id,
        UDFCatalog.is_active == True,
    )

    if language:
        query = query.filter(UDFCatalog.language == language)
    if category:
        query = query.filter(UDFCatalog.category == category)
    if search:
        query = query.filter(UDFCatalog.udf_name.ilike(f"%{search}%"))

    total = query.count()
    udfs = (
        query.order_by(UDFCatalog.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return UDFListResponse(
        udfs=udfs,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=math.ceil(total / page_size) if total > 0 else 1,
    )


@router.get("/{udf_id}", response_model=UDFResponse)
async def get_udf(
    udf_id: UUID,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get a UDF by ID."""
    return _get_udf(db, udf_id, user)


@router.patch("/{udf_id}", response_model=UDFResponse)
async def update_udf(
    udf_id: UUID,
    payload: UDFUpdate,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Update a UDF. Code changes reset validation status."""
    udf = _get_udf(db, udf_id, user)

    code_changed = (
        payload.function_code is not None
        and payload.function_code != udf.function_code
    )

    update_data = payload.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(udf, field, value)

    if code_changed:
        udf.is_validated = False
        udf.validation_errors = None

    db.commit()
    db.refresh(udf)
    return udf


@router.delete("/{udf_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_udf(
    udf_id: UUID,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Deactivate a UDF (soft delete via is_active=False)."""
    udf = _get_udf(db, udf_id, user)
    udf.is_active = False
    db.commit()

