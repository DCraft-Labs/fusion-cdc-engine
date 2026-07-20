"""Connections API endpoints"""

from datetime import datetime, timedelta
from typing import Optional, List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy import func, and_
from sqlalchemy.orm import Session, joinedload

from app.database import get_db
from app.auth.dependencies import get_current_user, require_permission
from app.models.auth import AuditLog, User
from app.models.connection import Connection, Stream
from app.models.source_destination import Source, Destination
from app.models.connector import ConnectorDefinition
from app.models.monitoring import ConnectionRun, CheckpointState, InitialLoadCheckpoint
from app.schemas.connection import (
    ConnectionCreate,
    ConnectionUpdate,
    ConnectionResponse,
    ConnectionListResponse,
    ConnectionSearchFilters,
    ConnectionValidationRequest,
    ConnectionValidationResponse,
    ScheduleConfig,
    ScheduleConfigResponse,
    ConnectionActivateRequest,
    ConnectionActivateResponse,
    SyncTriggerRequest,
    SyncTriggerResponse,
    ConnectionStats,
    StreamCreate,
    StreamUpdate,
    StreamResponse,
)

router = APIRouter()


# ===========================
# Helper Functions
# ===========================

def _get_connection_by_id(
    db: Session,
    connection_id: UUID,
    user: User,
    include_relations: bool = True,
) -> Connection:
    """Get connection by ID with tenant filtering"""
    query = db.query(Connection).filter(
        Connection.connection_id == connection_id,
        Connection.sub_tenant_id == user.sub_tenant_id,
        Connection.is_deleted == False,
    )
    
    if include_relations:
        query = query.options(
            joinedload(Connection.source).joinedload(Source.connector_definition),
            joinedload(Connection.destination).joinedload(Destination.connector_definition),
            joinedload(Connection.streams),
        )
    
    connection = query.first()
    
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connection {connection_id} not found",
        )
    
    return connection


def _validate_connection_compatibility(
    db: Session,
    source: Source,
    destination: Destination,
    sync_mode: str,
) -> tuple[bool, str, List[str]]:
    """
    Validate if source and destination are compatible
    Returns: (is_valid, message, issues_list)
    """
    issues = []
    
    # Check if source and destination exist and are active
    if source.status not in ["active", "draft"]:
        issues.append(f"Source '{source.source_name}' is not active (status: {source.status})")
    
    if destination.status not in ["active", "draft"]:
        issues.append(f"Destination '{destination.destination_name}' is not active (status: {destination.status})")
    
    # Validate sync mode support
    source_connector = source.connector_definition
    dest_connector = destination.connector_definition
    
    # Check CDC support
    if sync_mode == "cdc":
        source_capabilities = source_connector.capabilities or {}
        if not source_capabilities.get("supports_cdc", False):
            issues.append(f"Source connector '{source_connector.connector_name}' does not support CDC")
    
    # Check write mode compatibility
    dest_capabilities = dest_connector.capabilities or {}
    dest_write_modes = dest_capabilities.get("supported_write_modes", ["append"])
    
    if not dest_write_modes:
        issues.append(f"Destination connector '{dest_connector.connector_name}' has no configured write modes")
    
    # Validate connection test results
    if source.connection_test_status == "failed":
        issues.append(f"Source connection test failed: {source.connection_test_error}")
    
    if destination.connection_test_status == "failed":
        issues.append(f"Destination connection test failed: {destination.connection_test_error}")
    
    is_valid = len(issues) == 0
    message = "Connection is valid and compatible" if is_valid else "Connection validation failed"
    
    return is_valid, message, issues


def _calculate_next_sync_time(cron_expression: str) -> Optional[datetime]:
    """
    Calculate next sync time from cron expression
    Returns None for 'manual' frequency
    """
    if cron_expression == "manual":
        return None
    
    # TODO: Implement actual cron parsing with croniter library
    # For now, return a simple estimate
    # Common patterns:
    # */15 * * * * = every 15 minutes
    # 0 * * * * = every hour
    # 0 0 * * * = daily at midnight
    
    # Simple estimation for testing
    now = datetime.utcnow()
    if "*/15" in cron_expression:
        return now + timedelta(minutes=15)
    elif "*/30" in cron_expression:
        return now + timedelta(minutes=30)
    elif "0 *" in cron_expression:
        return now + timedelta(hours=1)
    else:
        return now + timedelta(days=1)


def _trigger_dag_or_worker(connection: Connection, db: Session) -> None:
    """
    Spec §1 (P1-2/P1-3): Trigger an initial full load or manual sync.

    Sync type routing:
      BATCH / SCHEDULED → trigger Airflow DAG (runs BatchConsumer)
      CDC / REALTIME    → publish start-streaming command to cdc-worker via Redis
                          + optional HTTP POST to worker

    For manual trigger-sync, BATCH connections trigger an Airflow DAG run.
    For CDC/REALTIME connections, the worker is already streaming — trigger-sync
    re-fetches sources and starts any new ones.

    Failures are logged but must not abort the activate/trigger-sync response.
    """
    import os
    import json as _json
    import logging as _logging
    import httpx

    from app.config import settings

    log = _logging.getLogger(__name__)
    sync_type = (getattr(connection, "sync_type", "") or "").upper()
    connection_id = str(connection.connection_id)

    # Normalise sync_type: CDC → REALTIME, BATCH → SCHEDULED for routing
    is_batch = sync_type in ("SCHEDULED", "BATCH")

    if is_batch:
        # Trigger the Airflow DAG via REST API
        airflow_url = os.environ.get(
            "AIRFLOW_API_URL",
            getattr(settings, "AIRFLOW_API_URL", "http://localhost:8080"),
        )
        airflow_user = os.environ.get("AIRFLOW_USER", getattr(settings, "AIRFLOW_USER", "admin"))
        airflow_pass = os.environ.get("AIRFLOW_PASSWORD", getattr(settings, "AIRFLOW_PASSWORD", "admin"))
        dag_id = f"fusion_cdc_{connection_id.replace('-', '_')}"

        try:
            resp = httpx.post(
                f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns",
                json={
                    "conf": {
                        "connection_id": connection_id,
                        "sync_mode": connection.sync_mode,
                        "source_id": str(connection.source_id),
                        "destination_id": str(connection.destination_id),
                    },
                },
                auth=(airflow_user, airflow_pass),
                timeout=10.0,
            )
            log.info("Triggered Airflow DAG %s → HTTP %s", dag_id, resp.status_code)
            if resp.status_code >= 400:
                log.warning("Airflow DAG trigger response: %s", resp.text[:500])
        except Exception as exc:
            log.warning("Could not trigger Airflow DAG %s: %s", dag_id, exc)
    else:
        # CDC / REALTIME — notify the worker via Redis pub/sub AND HTTP
        worker_url = os.environ.get("WORKER_CONTROL_URL", getattr(settings, "WORKER_CONTROL_URL", ""))
        worker_token = os.environ.get("WORKER_SHARED_SECRET", getattr(settings, "WORKER_SHARED_SECRET", ""))

        # Build a rich command payload with source and connection context
        command_payload = _json.dumps({
            "action": "start-streaming",
            "connection_id": connection_id,
            "source_id": str(connection.source_id),
            "destination_id": str(connection.destination_id),
            "sync_mode": connection.sync_mode,
        })

        # Publish to Redis so the cdc-worker command listener picks it up
        try:
            import redis
            redis_url = os.environ.get("REDIS_URL", getattr(settings, "REDIS_URL", "redis://localhost:6379"))
            r = redis.from_url(redis_url)
            r.publish("fusion:commands", command_payload)
            log.info("Published start-streaming command to Redis for connection=%s", connection_id)
        except Exception as exc:
            log.warning("Could not publish to Redis (connection=%s): %s", connection_id, exc)

        # Also attempt HTTP POST directly to the worker
        if not worker_url:
            # Default to the docker service name in dev
            worker_url = os.environ.get("CDC_WORKER_URL", "http://localhost:8081")

        try:
            resp = httpx.post(
                f"{worker_url}/internal/start-streaming",
                json={"connection_id": connection_id},
                headers={"X-Worker-Token": worker_token},
                timeout=5.0,
            )
            log.info("Notified worker to start streaming connection=%s HTTP %s", connection_id, resp.status_code)
        except Exception as exc:
            log.warning("Could not notify worker via HTTP (connection=%s): %s", connection_id, exc)


def _check_worker_reachable() -> bool:
    """Quick connectivity check to the CDC worker HTTP endpoint."""
    import os
    import httpx
    from app.config import settings
    worker_url = os.environ.get("CDC_WORKER_URL", os.environ.get("WORKER_CONTROL_URL", "http://localhost:8081"))
    try:
        resp = httpx.get(f"{worker_url}/health", timeout=6.0)
        return resp.status_code < 500
    except Exception:
        return False


def _delete_redis_cdc_keys(source_id: str) -> None:
    """
    Delete all Redis CDC stream keys belonging to a given source_id.
    Key format: cdc:{bank_id}:{tenant_id}:{source_id}:{schema}:{table}
    Scans for 'cdc:*:{source_id}:*' and deletes them all.
    """
    import os, logging as _logging
    from app.config import settings
    log = _logging.getLogger(__name__)
    try:
        import redis
        redis_url = os.environ.get("REDIS_URL", getattr(settings, "REDIS_URL", "redis://localhost:6379"))
        r = redis.from_url(redis_url)
        cursor = 0
        deleted = 0
        while True:
            cursor, keys = r.scan(cursor, match=f"cdc:*:{source_id}:*", count=100)
            if keys:
                r.delete(*keys)
                deleted += len(keys)
            if cursor == 0:
                break
        if deleted:
            log.info("Deleted %d Redis CDC stream key(s) for source_id=%s", deleted, source_id)
    except Exception as exc:
        log.warning("Could not clean Redis CDC keys for source_id=%s: %s", source_id, exc)


def _stop_worker_streaming(connection: Connection) -> None:
    """
    Publish a stop-streaming command to the cdc-worker via Redis pub/sub.
    Used when pausing a CDC/REALTIME connection.
    """
    import os
    import json as _json
    import logging as _logging

    from app.config import settings

    log = _logging.getLogger(__name__)
    sync_type = (getattr(connection, "sync_type", "") or "").upper()
    connection_id = str(connection.connection_id)

    if sync_type in ("SCHEDULED", "BATCH"):
        return  # Nothing to stop for batch — Airflow handles its own lifecycle

    try:
        import redis
        redis_url = os.environ.get("REDIS_URL", getattr(settings, "REDIS_URL", "redis://localhost:6379"))
        r = redis.from_url(redis_url)
        r.publish("fusion:commands", _json.dumps({
            "action": "stop-streaming",
            "connection_id": connection_id,
            "source_id": str(connection.source_id),
        }))
        log.info("Published stop-streaming command to Redis for connection=%s", connection_id)
    except Exception as exc:
        log.warning("Could not publish stop-streaming to Redis (connection=%s): %s", connection_id, exc)


# ===========================
# CRUD Endpoints
# ===========================

@router.post("", status_code=status.HTTP_201_CREATED, response_model=ConnectionResponse)
async def create_connection(
    connection_data: ConnectionCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:create")),
):
    """
    Create a new connection between source and destination
    
    Requires: connections:create permission
    """
    # Validate source exists and is accessible
    source = db.query(Source).filter(
        Source.source_id == connection_data.source_id,
        Source.sub_tenant_id == current_user.sub_tenant_id,
        Source.is_deleted == False,
    ).first()
    
    if not source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Source {connection_data.source_id} not found",
        )
    
    # Validate destination exists and is accessible
    destination = db.query(Destination).filter(
        Destination.destination_id == connection_data.destination_id,
        Destination.sub_tenant_id == current_user.sub_tenant_id,
        Destination.is_deleted == False,
    ).first()
    
    if not destination:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Destination {connection_data.destination_id} not found",
        )
    
    # Validate compatibility
    is_valid, message, issues = _validate_connection_compatibility(
        db, source, destination, connection_data.sync_mode
    )
    
    if not is_valid and connection_data.status == "active":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot create active connection: {'; '.join(issues)}",
        )
    
    # Check for duplicate name
    existing = db.query(Connection).filter(
        Connection.sub_tenant_id == current_user.sub_tenant_id,
        Connection.connection_name == connection_data.connection_name,
        Connection.is_deleted == False,
    ).first()
    
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Connection with name '{connection_data.connection_name}' already exists",
        )
    
    # Calculate next sync time
    next_sync_at = None
    if connection_data.sync_frequency and connection_data.status == "active":
        next_sync_at = _calculate_next_sync_time(connection_data.sync_frequency)
    
    # Create connection
    # Inherit bank_id / sub_tenant_id from the source when the current user
    # doesn't have them set (super-admin scenario).
    src_obj = db.query(Source).filter(Source.source_id == connection_data.source_id).first()
    resolved_bank_id      = current_user.bank_id      or (src_obj.bank_id      if src_obj else None)
    resolved_sub_tenant_id = current_user.sub_tenant_id or (src_obj.sub_tenant_id if src_obj else None)

    connection = Connection(
        connection_name=connection_data.connection_name,
        source_id=connection_data.source_id,
        destination_id=connection_data.destination_id,
        sync_mode=connection_data.sync_mode,
        sync_type=getattr(connection_data, 'sync_type', None) or "BATCH",
        schedule_cron=connection_data.sync_frequency,
        resource_limits=connection_data.resource_limits,
        status=connection_data.status,
        sub_tenant_id=resolved_sub_tenant_id,
        bank_id=resolved_bank_id,
    )
    
    db.add(connection)
    db.flush()  # Get connection ID for streams
    
    # Resolve default destination schema once — used as fallback for any stream
    # that doesn't explicitly specify destination_schema_name.
    dest_obj = db.query(Destination).filter(Destination.destination_id == connection_data.destination_id).first()
    default_dest_schema = (dest_obj.schema_name if dest_obj else None) or "public"

    # Create streams if provided
    if connection_data.streams:
        for stream_data in connection_data.streams:
            stream = Stream(
                connection_id=connection.connection_id,
                stream_name=stream_data.stream_name,
                stream_namespace=stream_data.stream_namespace,
                source_table_name=stream_data.source_table_name or stream_data.stream_name,
                source_schema_name=stream_data.source_schema_name or stream_data.stream_namespace or "",
                destination_table_name=stream_data.destination_table_name or stream_data.source_table_name or stream_data.stream_name,
                # Auto-fill destination schema from destination config when not provided in request
                destination_schema_name=stream_data.destination_schema_name or default_dest_schema,
                sync_mode=stream_data.sync_mode,
                cursor_field=stream_data.cursor_field,
                primary_keys=stream_data.primary_keys or [],
                is_enabled=stream_data.is_enabled if stream_data.is_enabled is not None else True,
                column_mapping=stream_data.column_mapping or {},
                transform_overrides=stream_data.transform_steps or {},
            )
            db.add(stream)
    
    db.commit()
    db.refresh(connection)

    # Trigger initial sync automatically if connection is created as active
    if connection.status == "active":
        connection.initial_load_started_at = datetime.utcnow()
        _trigger_dag_or_worker(connection, db)
        db.commit()
    
    # Load relationships
    db.refresh(connection, ["source", "destination", "streams"])
    
    return ConnectionResponse.model_validate(connection)


@router.get("", response_model=ConnectionListResponse)
async def list_connections(
    status_filter: Optional[str] = Query(None, alias="status"),
    sync_mode: Optional[str] = Query(None),
    source_id: Optional[UUID] = Query(None),
    destination_id: Optional[UUID] = Query(None),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    List all connections for the current tenant
    
    Supports filtering by:
    - status: draft, active, paused, inactive
    - sync_mode: cdc, full_refresh, incremental
    - source_id: specific source
    - destination_id: specific destination
    - search: search in connection name
    
    Results are paginated.
    """
    # Build base query with tenant filtering
    query = (
        db.query(Connection)
        .options(
            joinedload(Connection.source).joinedload(Source.connector_definition),
            joinedload(Connection.destination).joinedload(Destination.connector_definition),
        )
        .filter(
            Connection.sub_tenant_id == current_user.sub_tenant_id,
            Connection.is_deleted == False,
        )
    )
    
    # Apply filters
    if status_filter:
        query = query.filter(Connection.status == status_filter)
    
    if sync_mode:
        query = query.filter(Connection.sync_mode == sync_mode)
    
    if source_id:
        query = query.filter(Connection.source_id == source_id)
    
    if destination_id:
        query = query.filter(Connection.destination_id == destination_id)
    
    if search:
        query = query.filter(Connection.connection_name.ilike(f"%{search}%"))
    
    # Get total count
    total = query.count()
    
    # Apply pagination
    offset = (page - 1) * page_size
    connections = query.order_by(Connection.created_at.desc()).offset(offset).limit(page_size).all()
    
    # Convert to response models
    connection_responses = [ConnectionResponse.model_validate(conn) for conn in connections]
    
    total_pages = (total + page_size - 1) // page_size
    
    return ConnectionListResponse(
        connections=connection_responses,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


@router.get("/{connection_id}", response_model=ConnectionResponse)
async def get_connection(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get connection details by ID"""
    connection = _get_connection_by_id(db, connection_id, current_user)
    return ConnectionResponse.model_validate(connection)


@router.patch("/{connection_id}", response_model=ConnectionResponse)
async def update_connection(
    connection_id: UUID,
    connection_data: ConnectionUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:update")),
):
    """
    Update connection configuration
    
    Requires: connections:update permission
    """
    connection = _get_connection_by_id(db, connection_id, current_user, include_relations=False)
    
    # Check for duplicate name if name is being changed
    if connection_data.connection_name and connection_data.connection_name != connection.connection_name:
        existing = db.query(Connection).filter(
            Connection.sub_tenant_id == current_user.sub_tenant_id,
            Connection.connection_name == connection_data.connection_name,
            Connection.is_deleted == False,
            Connection.connection_id != connection_id,
        ).first()
        
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Connection with name '{connection_data.connection_name}' already exists",
            )
    
    # Prevent status change to active if connection is running
    if connection_data.status and connection.status == "active" and connection_data.status != "active":
        # TODO: Check if sync is currently running
        pass
    
    # Update fields
    update_dict = connection_data.model_dump(exclude_unset=True)
    
    for field, value in update_dict.items():
        setattr(connection, field, value)
    
    # Update next sync time if frequency changed
    if connection_data.sync_frequency and connection.status == "active":
        connection.next_sync_at = _calculate_next_sync_time(connection_data.sync_frequency)
    
    db.commit()
    db.refresh(connection)
    
    # Load relationships
    db.refresh(connection, ["source", "destination", "streams"])
    
    return ConnectionResponse.model_validate(connection)


@router.delete("/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection(
    connection_id: UUID,
    force: bool = Query(False, description="Force delete even if active"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:delete")),
):
    """
    Delete connection (soft delete)
    
    Requires: connections:delete permission
    
    Will fail if connection is active unless force=true.
    """
    connection = _get_connection_by_id(db, connection_id, current_user, include_relations=False)
    
    # Check if connection is active
    if connection.status == "active" and not force:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete active connection. Pause or deactivate first, or use force=true",
        )
    
    # Soft delete
    connection.is_deleted = True
    connection.deleted_at = datetime.utcnow()
    connection.status = "inactive"

    # Stop the CDC worker from streaming this connection and clean Redis keys
    _stop_worker_streaming(connection)
    _delete_redis_cdc_keys(str(connection.source_id))

    # Disable all streams for this connection
    for stream in db.query(Stream).filter(Stream.connection_id == connection_id).all():
        stream.is_enabled = False

    # Cancel any running jobs
    from app.models.monitoring import ConnectionRun
    db.query(ConnectionRun).filter(
        ConnectionRun.connection_id == connection_id,
        ConnectionRun.status.in_(["running", "pending"]),
    ).update({"status": "cancelled", "updated_at": datetime.utcnow()}, synchronize_session=False)

    db.commit()


# ===========================
# Connection Validation
# ===========================

@router.post("/validate", response_model=ConnectionValidationResponse)
async def validate_connection(
    validation_request: ConnectionValidationRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Validate connection compatibility before creation
    
    Checks:
    - Source and destination accessibility
    - Connector compatibility
    - Sync mode support
    - Connection test results
    """
    # Get source
    source = db.query(Source).options(
        joinedload(Source.connector_definition)
    ).filter(
        Source.source_id == validation_request.source_id,
        Source.sub_tenant_id == current_user.sub_tenant_id,
        Source.is_deleted == False,
    ).first()
    
    if not source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Source {validation_request.source_id} not found",
        )
    
    # Get destination
    destination = db.query(Destination).options(
        joinedload(Destination.connector_definition)
    ).filter(
        Destination.destination_id == validation_request.destination_id,
        Destination.sub_tenant_id == current_user.sub_tenant_id,
        Destination.is_deleted == False,
    ).first()
    
    if not destination:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Destination {validation_request.destination_id} not found",
        )
    
    # Validate compatibility
    is_valid, message, issues = _validate_connection_compatibility(
        db, source, destination, validation_request.sync_mode
    )
    
    return ConnectionValidationResponse(
        is_valid=is_valid,
        message=message,
        issues=issues,
        source_compatible=source.status in ["active", "draft"],
        destination_compatible=destination.status in ["active", "draft"],
        sync_mode_supported=len([i for i in issues if "does not support" in i]) == 0,
        validated_at=datetime.utcnow(),
    )


# ===========================
# Schedule Configuration
# ===========================

@router.post("/{connection_id}/schedule", response_model=ScheduleConfigResponse)
async def configure_schedule(
    connection_id: UUID,
    schedule_config: ScheduleConfig,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:update")),
):
    """
    Configure connection sync schedule
    
    Requires: connections:update permission
    """
    connection = _get_connection_by_id(db, connection_id, current_user, include_relations=False)
    
    # Update schedule
    connection.sync_frequency = schedule_config.sync_frequency
    connection.sync_enabled = schedule_config.sync_enabled
    
    # Store timezone in config
    if "schedule" not in connection.config:
        connection.config["schedule"] = {}
    connection.config["schedule"]["timezone"] = schedule_config.timezone
    
    # Update next sync time
    if connection.status == "active" and schedule_config.sync_enabled:
        connection.next_sync_at = _calculate_next_sync_time(schedule_config.sync_frequency)
    else:
        connection.next_sync_at = None
    
    from sqlalchemy import flag_modified
    flag_modified(connection, "config")
    
    db.commit()
    db.refresh(connection)
    
    return ScheduleConfigResponse(
        connection_id=connection_id,
        schedule_config=schedule_config,
        next_sync_at=connection.next_sync_at,
        updated_at=connection.updated_at,
    )


@router.get("/{connection_id}/schedule", response_model=ScheduleConfigResponse)
async def get_schedule(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get current schedule configuration"""
    connection = _get_connection_by_id(db, connection_id, current_user, include_relations=False)
    
    timezone = connection.config.get("schedule", {}).get("timezone", "UTC")
    
    schedule_config = ScheduleConfig(
        sync_frequency=connection.sync_frequency or "manual",
        sync_enabled=connection.sync_enabled,
        timezone=timezone,
    )
    
    return ScheduleConfigResponse(
        connection_id=connection_id,
        schedule_config=schedule_config,
        next_sync_at=connection.next_sync_at,
        updated_at=connection.updated_at,
    )


# ===========================
# Connection Actions
# ===========================

@router.post("/{connection_id}/activate", response_model=ConnectionActivateResponse)
async def activate_connection(
    connection_id: UUID,
    activate_request: ConnectionActivateRequest = ConnectionActivateRequest(),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:update")),
):
    """
    Activate connection and start syncing
    
    Requires: connections:update permission
    """
    connection = _get_connection_by_id(db, connection_id, current_user)
    
    if connection.status == "active":
        return ConnectionActivateResponse(
            connection_id=connection_id,
            status="active",
            message="Connection is already active",
            activated_at=datetime.utcnow(),
        )
    
    validation_result = None
    
    # Validate if requested
    if activate_request.validate_first:
        is_valid, message, issues = _validate_connection_compatibility(
            db, connection.source, connection.destination, connection.sync_mode
        )
        
        validation_result = ConnectionValidationResponse(
            is_valid=is_valid,
            message=message,
            issues=issues,
            source_compatible=connection.source.status in ["active", "draft"],
            destination_compatible=connection.destination.status in ["active", "draft"],
            sync_mode_supported=len([i for i in issues if "does not support" in i]) == 0,
            validated_at=datetime.utcnow(),
        )
        
        if not is_valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Connection validation failed: {'; '.join(issues)}",
            )
    
    # Activate connection
    connection.status = "active"
    
    # TODO: Trigger initial sync if not skip_initial_sync
    if not activate_request.skip_initial_sync:
        # Mark that initial load is in progress; the Airflow DAG / Spark consumer
        # will call /internal/connections/{id}/run-complete when done.
        connection.initial_load_started_at = datetime.utcnow()
        _trigger_dag_or_worker(connection, db)
    
    db.commit()
    
    return ConnectionActivateResponse(
        connection_id=connection_id,
        status="active",
        message="Connection activated successfully",
        validation_result=validation_result,
        activated_at=datetime.utcnow(),
    )


@router.post("/{connection_id}/pause", response_model=dict)
async def pause_connection(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:update")),
):
    """
    Pause connection (stop scheduled syncs)
    
    Requires: connections:update permission
    """
    connection = _get_connection_by_id(db, connection_id, current_user, include_relations=False)
    
    if connection.status != "active":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot pause connection with status '{connection.status}'",
        )
    
    connection.status = "paused"
    connection.sync_enabled = False
    connection.next_sync_at = None

    # For CDC/REALTIME connections, notify the worker to stop streaming
    _stop_worker_streaming(connection)
    
    db.commit()
    
    return {
        "connection_id": connection_id,
        "status": "paused",
        "message": "Connection paused successfully",
        "paused_at": datetime.utcnow(),
    }


@router.post("/{connection_id}/resume", response_model=dict)
async def resume_connection(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:update")),
):
    """
    Resume paused connection
    
    Requires: connections:update permission
    """
    connection = _get_connection_by_id(db, connection_id, current_user, include_relations=False)
    
    if connection.status != "paused":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot resume connection with status '{connection.status}'",
        )
    
    connection.status = "active"
    connection.sync_enabled = True
    
    if connection.sync_frequency:
        connection.next_sync_at = _calculate_next_sync_time(connection.sync_frequency)

    # For CDC/REALTIME connections, notify the worker to resume streaming
    _trigger_dag_or_worker(connection, db)
    
    db.commit()
    
    return {
        "connection_id": connection_id,
        "status": "active",
        "message": "Connection resumed successfully",
        "resumed_at": datetime.utcnow(),
    }


@router.post("/{connection_id}/trigger-sync", response_model=SyncTriggerResponse)
async def trigger_manual_sync(
    connection_id: UUID,
    sync_request: SyncTriggerRequest = SyncTriggerRequest(),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:update")),
):
    """
    Trigger manual sync for connection
    
    Requires: connections:update permission
    """
    connection = _get_connection_by_id(db, connection_id, current_user)
    
    if connection.status not in ["active", "paused"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot trigger sync for connection with status '{connection.status}'",
        )
    
    sync_type = (connection.sync_type or "").upper()

    # Determine next run_number
    last_run = (
        db.query(ConnectionRun)
        .filter(ConnectionRun.connection_id == connection_id)
        .order_by(ConnectionRun.run_number.desc())
        .first()
    )
    next_run_number = (last_run.run_number + 1) if last_run else 1

    # Mark any stale "running" batch runs as failed before creating the new one.
    # CDC/streaming runs run indefinitely — never mark them stale here.
    if sync_type in ("BATCH", "SCHEDULED"):
        stale_cutoff = datetime.utcnow() - timedelta(minutes=10)
        stale_runs = (
            db.query(ConnectionRun)
            .filter(
                ConnectionRun.connection_id == connection_id,
                ConnectionRun.status == "running",
                ConnectionRun.started_at < stale_cutoff,
            )
            .all()
        )
        for stale in stale_runs:
            stale.status = "failed"
            stale.completed_at = datetime.utcnow()
            stale.error_message = "Run timed out — no worker response within 10 minutes"

    # Create a new ConnectionRun record
    is_first_sync = not connection.initial_load_completed
    run = ConnectionRun(
        connection_id=connection_id,
        run_number=next_run_number,
        trigger_type="manual",
        triggered_by=current_user.user_id,
        status="running",
        started_at=datetime.utcnow(),
        run_config={
            "sync_mode": connection.sync_mode,
            "sync_type": sync_type,
            "is_first_sync": is_first_sync,
            "orchestration": "airflow" if sync_type in ("BATCH", "SCHEDULED") else "streaming",
        },
    )
    db.add(run)

    # Mark initial load started if first sync
    if is_first_sync:
        connection.initial_load_started_at = datetime.utcnow()

    _trigger_dag_or_worker(connection, db)

    # For CDC/streaming connections: check worker health immediately.
    # A CDC "trigger" is fire-and-confirm — the run completes once the worker
    # acknowledges the source is being streamed.  The actual streaming lives in
    # the worker process indefinitely; the run record is just an audit entry.
    worker_reachable = _check_worker_reachable()
    if sync_type not in ("BATCH", "SCHEDULED"):
        if not worker_reachable:
            run.status = "failed"
            run.completed_at = datetime.utcnow()
            run.error_message = "CDC worker is not running. Start the cdc-worker service and retry."
        else:
            import httpx as _httpx, os as _os
            _worker_url = _os.environ.get("CDC_WORKER_URL", "http://localhost:8081")
            _source_id = str(connection.source_id)
            _health_data = {}
            try:
                _health_data = _httpx.get(f"{_worker_url}/health", timeout=6.0).json()
                _active = _health_data.get("active_sources", [])
                if _source_id in _active:
                    # Source already streaming — mark initial load complete.
                    if not connection.initial_load_completed:
                        connection.initial_load_completed = True
                        connection.initial_load_completed_at = datetime.utcnow()
            except Exception:
                _active = []  # Worker reachable but health endpoint failed — stream starting up

            # Capture a snapshot of the current CDC state for the run log
            _snapshot: dict = {
                "worker_id": _health_data.get("worker_id"),
                "worker_status": "healthy",
                "is_source_active": _source_id in _active,
                "tables": [],
                "binlog_position": None,
                "checkpoint_at": None,
                "redis_event_counts": {},
                "total_events": 0,
            }
            try:
                # Tables being monitored for this connection
                _streams_q = db.query(Stream).filter(
                    Stream.connection_id == connection_id,
                    Stream.is_enabled == True,
                ).all()
                _snapshot["tables"] = [f"{s.source_schema_name}.{s.source_table_name}" for s in _streams_q]

                # Latest checkpoint
                _ckpt = (
                    db.query(CheckpointState)
                    .filter(CheckpointState.source_id == connection.source_id)
                    .order_by(CheckpointState.checkpoint_at.desc())
                    .first()
                )
                if _ckpt:
                    _snapshot["binlog_position"] = _ckpt.lsn
                    _snapshot["checkpoint_at"] = (
                        _ckpt.checkpoint_at.isoformat() if _ckpt.checkpoint_at else None
                    )

                # Redis event counts per table
                import redis as _redis_lib
                _redis_url = _os.environ.get("REDIS_URL", getattr(settings, "REDIS_URL", "redis://localhost:6379"))
                _source_obj = db.query(Source).filter(Source.source_id == connection.source_id).first()
                if _source_obj:
                    _bank_id = str(_source_obj.bank_id)
                    _tenant_id = str(_source_obj.sub_tenant_id)
                    _r = _redis_lib.from_url(_redis_url, decode_responses=True)
                    _counts = {}
                    for _s in _streams_q:
                        _key = f"cdc:{_bank_id}:{_tenant_id}:{_source_id}:{_s.source_schema_name}:{_s.source_table_name}"
                        try:
                            _counts[f"{_s.source_schema_name}.{_s.source_table_name}"] = _r.xlen(_key)
                        except Exception:
                            _counts[f"{_s.source_schema_name}.{_s.source_table_name}"] = 0
                    _snapshot["redis_event_counts"] = _counts
                    _snapshot["total_events"] = sum(_counts.values())
            except Exception:
                pass  # snapshot enrichment is best-effort

            # Mark the run completed — streaming is handled by the worker process.
            # Each trigger is a point-in-time "start/verify streaming" action.
            run.status = "completed"
            run.completed_at = datetime.utcnow()
            run.error_message = None
            run.run_config = {**run.run_config, "snapshot": _snapshot}

    db.commit()

    return SyncTriggerResponse(
        connection_id=connection_id,
        sync_triggered=True,
        message=f"Manual sync triggered successfully via {'Airflow' if sync_type in ('BATCH', 'SCHEDULED') else 'streaming worker'}",
        triggered_at=datetime.utcnow(),
        estimated_duration_seconds=None,
    )


# ===========================
# Statistics
# ===========================

@router.get("/{connection_id}/stats", response_model=ConnectionStats)
async def get_connection_stats(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get connection usage statistics
    
    Returns sync count, data volume, and performance metrics.
    """
    connection = _get_connection_by_id(db, connection_id, current_user)

    # Count streams
    total_streams = len(connection.streams)
    active_streams = sum(1 for s in connection.streams if s.is_enabled)

    # Derive sync statistics from AuditLog entries (spec §5 P5-8)
    conn_id_str = str(connection_id)
    run_logs = (
        db.query(AuditLog)
        .filter(
            AuditLog.resource_type == "connection",
            AuditLog.resource_id == conn_id_str,
            AuditLog.action.like("connection.batch_run.%"),
        )
        .order_by(AuditLog.created_at.desc())
        .limit(1000)
        .all()
    )

    total_syncs = len(run_logs)
    successful_syncs = sum(1 for r in run_logs if r.action == "connection.batch_run.success")
    failed_syncs = total_syncs - successful_syncs
    total_rows_synced = sum(
        int((r.details or {}).get("rows_synced", 0) or 0) for r in run_logs
        if r.action == "connection.batch_run.success"
    )
    last_sync_at = run_logs[0].created_at if run_logs else None

    # Build human-readable last_sync string
    last_sync_str = None
    if last_sync_at:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        if last_sync_at.tzinfo is None:
            from datetime import timezone as tz
            last_sync_at_aware = last_sync_at.replace(tzinfo=tz.utc)
        else:
            last_sync_at_aware = last_sync_at
        delta = now - last_sync_at_aware
        if delta.total_seconds() < 60:
            last_sync_str = "Just now"
        elif delta.total_seconds() < 3600:
            last_sync_str = f"{int(delta.total_seconds() // 60)}m ago"
        elif delta.total_seconds() < 86400:
            last_sync_str = f"{int(delta.total_seconds() // 3600)}h ago"
        else:
            last_sync_str = f"{int(delta.days)}d ago"

    return ConnectionStats(
        connection_id=connection_id,
        connection_name=connection.connection_name,
        status=connection.status,
        total_syncs=total_syncs,
        successful_syncs=successful_syncs,
        failed_syncs=failed_syncs,
        last_sync_at=last_sync_at,
        last_sync_duration_seconds=None,
        total_rows_synced=total_rows_synced,
        total_bytes_synced=0,
        avg_sync_duration_seconds=None,
        avg_throughput_rows_per_sec=None,
        consecutive_failures=0,
        last_error=None,
        total_streams=total_streams,
        active_streams=active_streams,
        events_per_hour=total_rows_synced,
        lag_seconds=None,
        uptime_percent=100.0 if connection.status == "active" else 0.0,
        last_sync=last_sync_str,
    )


@router.get("/{connection_id}/runs")
async def get_connection_runs(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get sync run history for a connection, including initial load checkpoints."""
    connection = _get_connection_by_id(db, connection_id, current_user)

    runs_db = (
        db.query(ConnectionRun)
        .filter(ConnectionRun.connection_id == connection_id)
        .order_by(ConnectionRun.run_number.desc())
        .limit(100)
        .all()
    )

    stale_cutoff = datetime.utcnow() - timedelta(minutes=10)
    updated = False

    result = []
    for run in runs_db:
        effective_status = run.status
        error_msg = run.error_message

        _is_streaming = (run.run_config or {}).get("orchestration") == "streaming"
        if run.status == "running" and run.started_at and not _is_streaming:
            started_naive = run.started_at.replace(tzinfo=None) if run.started_at.tzinfo else run.started_at
            if started_naive < stale_cutoff:
                run.status = "failed"
                run.completed_at = datetime.utcnow()
                run.error_message = run.error_message or "Run timed out — no worker response within 10 minutes"
                effective_status = "failed"
                error_msg = run.error_message
                updated = True

        duration_sec = None
        if run.completed_at and run.started_at:
            started = run.started_at.replace(tzinfo=None) if run.started_at.tzinfo else run.started_at
            ended = run.completed_at.replace(tzinfo=None) if run.completed_at.tzinfo else run.completed_at
            duration_sec = int((ended - started).total_seconds())

        is_first = run.run_config.get("is_first_sync", False) if run.run_config else False

        result.append({
            "id": str(run.run_id),
            "run_number": run.run_number,
            "run_type": "initial_load" if is_first else "cdc",
            "trigger_type": run.trigger_type,
            "status": effective_status,
            "records_inserted": int(run.records_written or 0),
            "records_updated": 0,
            "records_deleted": 0,
            "records_synced": int(run.records_written or 0),
            "records_read": int(run.records_read or 0),
            "duration": duration_sec,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "completed_at": run.completed_at.isoformat() if run.completed_at else None,
            "error_message": error_msg,
            "is_first_sync": is_first,
            "tables": [],
            "run_config": run.run_config,
        })

    if updated:
        db.commit()

    # Always also include initial load checkpoints as synthetic "Run #0 – Initial Load" entries
    # even when connection_runs is empty (worker may not write a ConnectionRun record)
    checkpoints = (
        db.query(InitialLoadCheckpoint)
        .filter(InitialLoadCheckpoint.connection_id == connection_id)
        .order_by(InitialLoadCheckpoint.started_at)
        .all()
    )

    if checkpoints:
        # Group by start time (same second = same bulk run)
        total_rows = sum(int(c.rows_written or 0) for c in checkpoints)
        earliest_start = min((c.started_at for c in checkpoints if c.started_at), default=None)
        latest_end = max((c.completed_at for c in checkpoints if c.completed_at), default=None)
        all_done = all(c.status in ("done", "completed") for c in checkpoints)
        any_error = any(c.status == "error" for c in checkpoints)
        duration_sec_il = None
        if earliest_start and latest_end:
            s = earliest_start.replace(tzinfo=None) if earliest_start.tzinfo else earliest_start
            e = latest_end.replace(tzinfo=None) if latest_end.tzinfo else latest_end
            duration_sec_il = int((e - s).total_seconds())

        # Only add synthetic entry if there's no existing initial_load run in connection_runs
        has_il_run = any(r.get("is_first_sync") for r in result)
        if not has_il_run:
            table_details = [
                {
                    "table_name": c.source_table,
                    "rows_inserted": int(c.rows_written or 0),
                    "rows_updated": 0,
                    "rows_deleted": 0,
                    "status": "completed" if c.status in ("done", "completed") else c.status,
                    "started_at": c.started_at.isoformat() if c.started_at else None,
                    "completed_at": c.completed_at.isoformat() if c.completed_at else None,
                    "error": c.error,
                }
                for c in checkpoints
            ]
            result.append({
                "id": f"il-{connection_id}",
                "run_number": 0,
                "run_type": "initial_load",
                "trigger_type": "initial_load",
                "status": "completed" if all_done else ("failed" if any_error else "running"),
                "records_inserted": total_rows,
                "records_updated": 0,
                "records_deleted": 0,
                "records_synced": total_rows,
                "records_read": total_rows,
                "duration": duration_sec_il,
                "started_at": earliest_start.isoformat() if earliest_start else None,
                "completed_at": latest_end.isoformat() if latest_end else None,
                "error_message": None,
                "is_first_sync": True,
                "tables": table_details,
                "run_config": {},
            })

    # Sort: most recent first
    result.sort(key=lambda r: r.get("started_at") or "", reverse=True)
    return result


@router.get("/{connection_id}/initial-load")
async def get_initial_load_status(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get initial load checkpoint details per table for a connection."""
    connection = _get_connection_by_id(db, connection_id, current_user)
    checkpoints = (
        db.query(InitialLoadCheckpoint)
        .filter(InitialLoadCheckpoint.connection_id == connection_id)
        .order_by(InitialLoadCheckpoint.started_at)
        .all()
    )
    tables = []
    for c in checkpoints:
        duration_sec = None
        if c.started_at and c.completed_at:
            s = c.started_at.replace(tzinfo=None) if c.started_at.tzinfo else c.started_at
            e = c.completed_at.replace(tzinfo=None) if c.completed_at.tzinfo else c.completed_at
            duration_sec = int((e - s).total_seconds())
        tables.append({
            "checkpoint_id": str(c.checkpoint_id),
            "source_table": c.source_table,
            "rows_written": int(c.rows_written or 0),
            "status": "completed" if c.status in ("done", "completed") else c.status,
            "started_at": c.started_at.isoformat() if c.started_at else None,
            "completed_at": c.completed_at.isoformat() if c.completed_at else None,
            "duration_seconds": duration_sec,
            "error": c.error,
        })
    total_rows = sum(t["rows_written"] for t in tables)
    completed = sum(1 for t in tables if t["status"] == "completed")
    return {
        "connection_id": str(connection_id),
        "initial_load_completed": connection.initial_load_completed,
        "initial_load_started_at": connection.initial_load_started_at.isoformat() if connection.initial_load_started_at else None,
        "initial_load_completed_at": connection.initial_load_completed_at.isoformat() if connection.initial_load_completed_at else None,
        "total_rows_written": total_rows,
        "tables_total": len(tables),
        "tables_completed": completed,
        "tables": tables,
    }


@router.get("/{connection_id}/health")
async def get_connection_health(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get connection health status with lag and throughput history."""
    connection = _get_connection_by_id(db, connection_id, current_user)

    worker_status = "no_worker"
    last_heartbeat = None

    # Try to get heartbeat data if model exists
    try:
        from app.models.monitoring import WorkerHeartbeat, CDCLagMetrics
        latest_hb = (
            db.query(WorkerHeartbeat)
            .filter(WorkerHeartbeat.connection_id == connection_id)
            .order_by(WorkerHeartbeat.last_heartbeat_at.desc())
            .first()
        )
        if latest_hb:
            worker_status = latest_hb.status
            last_heartbeat = str(latest_hb.last_heartbeat_at)
    except Exception:
        pass

    return {
        "connection_id": str(connection_id),
        "status": worker_status if connection.status != "active" else (worker_status if worker_status != "no_worker" else "healthy"),
        "last_heartbeat_at": last_heartbeat,
        "lag_seconds": None,
        "lag_events": None,
        "lag_history": [],
        "throughput_history": [],
    }


# ===========================
# Stream Management
# ===========================

@router.post("/{connection_id}/streams", status_code=status.HTTP_201_CREATED, response_model=StreamResponse)
async def add_stream(
    connection_id: UUID,
    stream_data: StreamCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:update")),
):
    """
    Add a new stream to connection
    
    Requires: connections:update permission
    """
    connection = _get_connection_by_id(db, connection_id, current_user, include_relations=False)

    # Resolve default destination schema from the connection's destination config
    dest_obj = db.query(Destination).filter(Destination.destination_id == connection.destination_id).first()
    default_dest_schema = (dest_obj.schema_name if dest_obj else None) or "public"

    # Create stream
    stream = Stream(
        connection_id=connection_id,
        stream_name=stream_data.stream_name,
        stream_namespace=stream_data.stream_namespace,
        source_table_name=stream_data.source_table_name or stream_data.stream_name,
        source_schema_name=stream_data.source_schema_name or stream_data.stream_namespace or "",
        destination_table_name=stream_data.destination_table_name or stream_data.source_table_name or stream_data.stream_name,
        # Auto-fill destination schema from destination config when not provided in request
        destination_schema_name=stream_data.destination_schema_name or default_dest_schema,
        sync_mode=stream_data.sync_mode,
        cursor_field=stream_data.cursor_field,
        primary_keys=stream_data.primary_keys or [],
        is_enabled=stream_data.is_enabled if stream_data.is_enabled is not None else True,
        column_mapping=stream_data.column_mapping or {},
        transform_overrides=stream_data.transform_steps or {},
    )
    
    db.add(stream)
    db.commit()
    db.refresh(stream)
    
    return StreamResponse.model_validate(stream)


@router.get("/{connection_id}/streams", response_model=List[StreamResponse])
async def list_streams(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List all streams for a connection"""
    connection = _get_connection_by_id(db, connection_id, current_user)
    
    return [StreamResponse.model_validate(stream) for stream in connection.streams]


@router.patch("/{connection_id}/streams/{stream_id}", response_model=StreamResponse)
async def update_stream(
    connection_id: UUID,
    stream_id: UUID,
    stream_data: StreamUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:update")),
):
    """
    Update stream configuration
    
    Requires: connections:update permission
    """
    # Verify connection exists and user has access
    connection = _get_connection_by_id(db, connection_id, current_user, include_relations=False)
    
    # Get stream
    stream = db.query(Stream).filter(
        Stream.stream_id == stream_id,
        Stream.connection_id == connection_id,
    ).first()
    
    if not stream:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Stream {stream_id} not found in connection {connection_id}",
        )
    
    # Update fields — map transform_steps → transform_overrides
    update_dict = stream_data.model_dump(exclude_unset=True)
    if 'transform_steps' in update_dict:
        update_dict['transform_overrides'] = update_dict.pop('transform_steps') or {}
    
    for field, value in update_dict.items():
        setattr(stream, field, value)
    
    db.commit()
    db.refresh(stream)
    
    return StreamResponse.model_validate(stream)


@router.delete("/{connection_id}/streams/{stream_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_stream(
    connection_id: UUID,
    stream_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:update")),
):
    """
    Delete stream from connection
    
    Requires: connections:update permission
    """
    # Verify connection exists and user has access
    connection = _get_connection_by_id(db, connection_id, current_user, include_relations=False)
    
    # Get stream
    stream = db.query(Stream).filter(
        Stream.stream_id == stream_id,
        Stream.connection_id == connection_id,
    ).first()
    
    if not stream:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Stream {stream_id} not found in connection {connection_id}",
        )
    
    db.delete(stream)
    db.commit()
