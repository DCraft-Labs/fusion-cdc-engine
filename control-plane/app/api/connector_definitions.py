"""
Connector Definitions API endpoints
CRUD operations for connector definitions and versions
"""
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import select, func, or_
from typing import List, Optional
from uuid import UUID

from app.database import get_db
from app.models.connector import ConnectorDefinition, ConnectorVersion
from app.models.source_destination import Source, Destination
from app.models.connection import Connection
from app.schemas.connector import (
    ConnectorDefinitionCreate,
    ConnectorDefinitionUpdate,
    ConnectorDefinitionResponse,
    ConnectorDefinitionListResponse,
    ConnectorVersionCreate,
    ConnectorVersionUpdate,
    ConnectorVersionResponse,
    ConnectorVersionListResponse,
    ConnectorCapabilities,
    ConnectorConfigSchema,
    ConnectorSearchFilters,
    ConnectorStats,
)
from app.auth.dependencies import get_current_user, require_permission, CurrentUser


router = APIRouter()


# ============================================================================
# Connector Definition Endpoints
# ============================================================================

@router.get("", response_model=ConnectorDefinitionListResponse)
async def list_connector_definitions(
    category: Optional[str] = Query(None, description="Filter by category: source or destination"),
    connector_type: Optional[str] = Query(None, description="Filter by connector type"),
    supports_cdc: Optional[bool] = Query(None, description="Filter by CDC support"),
    is_active: Optional[bool] = Query(True, description="Filter by active status"),
    search: Optional[str] = Query(None, description="Search in connector name"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Page size"),
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
):
    """
    List all available connector definitions.
    
    Returns system-wide list of supported source and destination connectors
    with filtering, search, and pagination.
    """
    # Build query
    stmt = select(ConnectorDefinition)
    
    # Apply filters
    if category:
        stmt = stmt.where(ConnectorDefinition.category == category)
    if connector_type:
        stmt = stmt.where(ConnectorDefinition.connector_type == connector_type)
    if supports_cdc is not None:
        stmt = stmt.where(ConnectorDefinition.supports_cdc == supports_cdc)
    if is_active is not None:
        stmt = stmt.where(ConnectorDefinition.is_active == is_active)
    if search:
        stmt = stmt.where(
            or_(
                ConnectorDefinition.connector_name.ilike(f"%{search}%"),
                ConnectorDefinition.connector_type.ilike(f"%{search}%"),
            )
        )
    
    # Get total count
    count_stmt = select(func.count()).select_from(stmt.subquery())
    total = db.execute(count_stmt).scalar()
    
    # Apply pagination
    stmt = stmt.offset((page - 1) * page_size).limit(page_size)
    stmt = stmt.order_by(ConnectorDefinition.connector_name)
    
    # Execute query
    result = db.execute(stmt)
    connectors = result.scalars().all()
    
    return ConnectorDefinitionListResponse(
        connectors=connectors,
        total=total,
        page=page,
        page_size=page_size,
    )


@router.post("", response_model=ConnectorDefinitionResponse, status_code=status.HTTP_201_CREATED)
async def create_connector_definition(
    connector_data: ConnectorDefinitionCreate,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission("connector_definitions:create")),
):
    """
    Create a new connector definition.
    
    Requires: connector_definitions:create permission (superadmin only)
    """
    # Check if connector name already exists
    stmt = select(ConnectorDefinition).where(
        ConnectorDefinition.connector_name == connector_data.connector_name
    )
    existing = db.execute(stmt).scalar_one_or_none()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Connector with name '{connector_data.connector_name}' already exists",
        )
    
    # Create connector definition
    connector = ConnectorDefinition(
        **connector_data.model_dump(),
        created_by=current_user.user_id,
    )
    
    db.add(connector)
    db.commit()
    db.refresh(connector)
    
    return connector


@router.get("/{connector_id}", response_model=ConnectorDefinitionResponse)
async def get_connector_definition(
    connector_id: UUID,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
):
    """
    Get detailed connector definition including configuration schema.
    
    Returns complete connector metadata, capabilities, and configuration requirements.
    """
    stmt = select(ConnectorDefinition).where(ConnectorDefinition.connector_id == connector_id)
    connector = db.execute(stmt).scalar_one_or_none()
    
    if not connector:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connector definition {connector_id} not found",
        )
    
    return connector


@router.patch("/{connector_id}", response_model=ConnectorDefinitionResponse)
async def update_connector_definition(
    connector_id: UUID,
    connector_update: ConnectorDefinitionUpdate,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission("connector_definitions:update")),
):
    """
    Update connector definition.
    
    Requires: connector_definitions:update permission (superadmin only)
    """
    # Get connector
    stmt = select(ConnectorDefinition).where(ConnectorDefinition.connector_id == connector_id)
    connector = db.execute(stmt).scalar_one_or_none()
    
    if not connector:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connector definition {connector_id} not found",
        )
    
    # Update fields
    update_data = connector_update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(connector, field, value)
    
    db.commit()
    db.refresh(connector)
    
    return connector


@router.delete("/{connector_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connector_definition(
    connector_id: UUID,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission("connector_definitions:delete")),
):
    """
    Delete connector definition.
    
    Requires: connector_definitions:delete permission (superadmin only)
    Cannot delete if connector is in use by any sources or destinations.
    """
    # Get connector
    stmt = select(ConnectorDefinition).where(ConnectorDefinition.connector_id == connector_id)
    connector = db.execute(stmt).scalar_one_or_none()
    
    if not connector:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connector definition {connector_id} not found",
        )
    
    # Check if connector is in use
    source_count = db.query(func.count(Source.source_id)).filter(
        Source.connector_definition_id == connector_id
    ).scalar()
    
    dest_count = db.query(func.count(Destination.destination_id)).filter(
        Destination.connector_definition_id == connector_id
    ).scalar()
    
    if source_count > 0 or dest_count > 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot delete connector: in use by {source_count} sources and {dest_count} destinations",
        )
    
    db.delete(connector)
    db.commit()
    
    return None


# ============================================================================
# Connector Version Endpoints
# ============================================================================

@router.get("/{connector_id}/versions", response_model=ConnectorVersionListResponse)
async def list_connector_versions(
    connector_id: UUID,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
):
    """
    List all versions for a connector.
    
    Returns version history with release notes and stability status.
    """
    # Verify connector exists
    stmt = select(ConnectorDefinition).where(ConnectorDefinition.connector_id == connector_id)
    connector = db.execute(stmt).scalar_one_or_none()
    
    if not connector:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connector definition {connector_id} not found",
        )
    
    # Get versions
    stmt = select(ConnectorVersion).where(
        ConnectorVersion.connector_id == connector_id
    ).order_by(ConnectorVersion.released_at.desc())
    
    result = db.execute(stmt)
    versions = result.scalars().all()
    
    return ConnectorVersionListResponse(
        versions=versions,
        total=len(versions),
    )


@router.post("/{connector_id}/versions", response_model=ConnectorVersionResponse, status_code=status.HTTP_201_CREATED)
async def create_connector_version(
    connector_id: UUID,
    version_data: ConnectorVersionCreate,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission("connector_definitions:create")),
):
    """
    Create a new connector version.
    
    Requires: connector_definitions:create permission (superadmin only)
    """
    # Verify connector exists
    stmt = select(ConnectorDefinition).where(ConnectorDefinition.connector_id == connector_id)
    connector = db.execute(stmt).scalar_one_or_none()
    
    if not connector:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connector definition {connector_id} not found",
        )
    
    # Check if version already exists
    stmt = select(ConnectorVersion).where(
        ConnectorVersion.connector_id == connector_id,
        ConnectorVersion.version == version_data.version,
    )
    existing = db.execute(stmt).scalar_one_or_none()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Version '{version_data.version}' already exists for this connector",
        )
    
    # Create version
    version = ConnectorVersion(
        connector_id=connector_id,
        **version_data.model_dump(),
    )
    
    db.add(version)
    db.commit()
    db.refresh(version)
    
    return version


@router.get("/{connector_id}/versions/{version_id}", response_model=ConnectorVersionResponse)
async def get_connector_version(
    connector_id: UUID,
    version_id: UUID,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
):
    """Get specific connector version details"""
    stmt = select(ConnectorVersion).where(
        ConnectorVersion.connector_id == connector_id,
        ConnectorVersion.version_id == version_id,
    )
    version = db.execute(stmt).scalar_one_or_none()
    
    if not version:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Version {version_id} not found for connector {connector_id}",
        )
    
    return version


@router.patch("/{connector_id}/versions/{version_id}", response_model=ConnectorVersionResponse)
async def update_connector_version(
    connector_id: UUID,
    version_id: UUID,
    version_update: ConnectorVersionUpdate,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission("connector_definitions:update")),
):
    """
    Update connector version.
    
    Requires: connector_definitions:update permission (superadmin only)
    """
    # Get version
    stmt = select(ConnectorVersion).where(
        ConnectorVersion.connector_id == connector_id,
        ConnectorVersion.version_id == version_id,
    )
    version = db.execute(stmt).scalar_one_or_none()
    
    if not version:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Version {version_id} not found for connector {connector_id}",
        )
    
    # Update fields
    update_data = version_update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(version, field, value)
    
    db.commit()
    db.refresh(version)
    
    return version


# ============================================================================
# Connector Capability & Configuration Endpoints
# ============================================================================

@router.get("/{connector_id}/capabilities", response_model=ConnectorCapabilities)
async def get_connector_capabilities(
    connector_id: UUID,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
):
    """
    Get connector capabilities.
    
    Returns supported sync modes and features.
    """
    stmt = select(ConnectorDefinition).where(ConnectorDefinition.connector_id == connector_id)
    connector = db.execute(stmt).scalar_one_or_none()
    
    if not connector:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connector definition {connector_id} not found",
        )
    
    return ConnectorCapabilities(
        supports_cdc=connector.supports_cdc,
        supports_full_refresh=connector.supports_full_refresh,
        supports_incremental=connector.supports_incremental,
    )


@router.get("/{connector_id}/config-schema", response_model=ConnectorConfigSchema)
async def get_connector_config_schema(
    connector_id: UUID,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
):
    """
    Get connector configuration schema.
    
    Returns required fields, optional fields, and default configuration.
    """
    stmt = select(ConnectorDefinition).where(ConnectorDefinition.connector_id == connector_id)
    connector = db.execute(stmt).scalar_one_or_none()
    
    if not connector:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connector definition {connector_id} not found",
        )
    
    return ConnectorConfigSchema(
        required_fields=connector.required_fields,
        optional_fields=connector.optional_fields,
        default_config=connector.default_config,
    )


@router.get("/{connector_id}/stats", response_model=ConnectorStats)
async def get_connector_stats(
    connector_id: UUID,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
):
    """
    Get connector usage statistics.
    
    Returns count of sources, destinations, and connections using this connector.
    """
    stmt = select(ConnectorDefinition).where(ConnectorDefinition.connector_id == connector_id)
    connector = db.execute(stmt).scalar_one_or_none()
    
    if not connector:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connector definition {connector_id} not found",
        )
    
    # Count sources
    source_count = db.query(func.count(Source.source_id)).filter(
        Source.connector_definition_id == connector_id
    ).scalar()
    
    # Count destinations
    dest_count = db.query(func.count(Destination.destination_id)).filter(
        Destination.connector_definition_id == connector_id
    ).scalar()
    
    # Count connections (via sources or destinations)
    connection_count = db.query(func.count(Connection.connection_id.distinct())).filter(
        or_(
            Connection.source_id.in_(
                db.query(Source.source_id).filter(Source.connector_definition_id == connector_id)
            ),
            Connection.destination_id.in_(
                db.query(Destination.destination_id).filter(Destination.connector_definition_id == connector_id)
            ),
        )
    ).scalar()
    
    # Count active connections
    active_connection_count = db.query(func.count(Connection.connection_id.distinct())).filter(
        Connection.status == "running",
        or_(
            Connection.source_id.in_(
                db.query(Source.source_id).filter(Source.connector_definition_id == connector_id)
            ),
            Connection.destination_id.in_(
                db.query(Destination.destination_id).filter(Destination.connector_definition_id == connector_id)
            ),
        )
    ).scalar()
    
    return ConnectorStats(
        connector_id=connector.connector_id,
        connector_name=connector.connector_name,
        total_sources=source_count,
        total_destinations=dest_count,
        active_connections=active_connection_count,
        total_connections=connection_count,
    )


@router.get("/{connector_id}/usage")
async def get_connector_usage(
    connector_id: UUID,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
):
    """
    Get list of sources and destinations using this connector.
    """
    stmt = select(ConnectorDefinition).where(ConnectorDefinition.connector_id == connector_id)
    connector = db.execute(stmt).scalar_one_or_none()

    if not connector:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connector definition {connector_id} not found",
        )

    usage = []

    # Get sources
    sources = db.query(Source).filter(
        Source.connector_definition_id == connector_id,
        Source.is_deleted == False,
    ).all()
    for s in sources:
        usage.append({
            "id": str(s.source_id),
            "name": s.source_name,
            "type": "source",
            "status": s.status or "draft",
        })

    # Get destinations
    destinations = db.query(Destination).filter(
        Destination.connector_definition_id == connector_id,
        Destination.is_deleted == False,
    ).all()
    for d in destinations:
        usage.append({
            "id": str(d.destination_id),
            "name": d.destination_name,
            "type": "destination",
            "status": d.status or "draft",
        })

    return usage
