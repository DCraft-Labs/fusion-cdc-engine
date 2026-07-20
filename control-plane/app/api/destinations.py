"""Destinations API endpoints"""

import time
from datetime import datetime
from typing import Optional, List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy import func, and_
from sqlalchemy.orm import Session, joinedload

from app.database import get_db
from app.auth.dependencies import get_current_user, require_permission
from app.models.auth import AuditLog, User
from app.models.source_destination import Destination
from app.models.connector import ConnectorDefinition
from app.models.connection import Connection
from app.schemas.destination import (
    DestinationCreate,
    DestinationUpdate,
    DestinationResponse,
    DestinationListResponse,
    DestinationSearchFilters,
    ConnectionTestRequest,
    ConnectionTestResponse,
    WriteModeConfig,
    WriteModeConfigResponse,
    SchemaMappingRequest,
    SchemaMappingResponse,
    BatchSettings,
    BatchSettingsResponse,
    DestinationStats,
)

router = APIRouter()


# ===========================
# Helper Functions
# ===========================

def _encrypt_password(password: str) -> str:
    """Encrypt password using Fernet symmetric encryption."""
    from app.utils.crypto import encrypt_secret
    return encrypt_secret(password)


def _decrypt_password(encrypted_password: str) -> str:
    """Decrypt password from Fernet ciphertext (also handles legacy stub format)."""
    from app.utils.crypto import decrypt_secret
    return decrypt_secret(encrypted_password)


def _get_destination_by_id(
    db: Session,
    destination_id: UUID,
    user: User,
) -> Destination:
    """Get destination by ID with tenant filtering"""
    destination = (
        db.query(Destination)
        .options(joinedload(Destination.connector_definition))
        .filter(
            Destination.destination_id == destination_id,
            Destination.sub_tenant_id == user.sub_tenant_id,
            Destination.is_deleted == False,
        )
        .first()
    )
    
    if not destination:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Destination {destination_id} not found",
        )
    
    return destination


def _maybe_activate_destination(destination: Destination, db: Session) -> bool:
    """
    Auto-activate a destination if all readiness criteria are met:
    1. Connection test has passed
    2. Write permissions validated (stored in connection_config.write_permissions_validated)
    Returns True if status was changed to active.
    """
    if destination.status != "draft":
        return False

    connection_ok = destination.connection_test_status == "success"

    conn_config = destination.connection_config if isinstance(destination.connection_config, dict) else {}
    write_perms_ok = conn_config.get("write_permissions_validated", False)

    if connection_ok and write_perms_ok:
        destination.status = "active"
        destination.updated_at = datetime.utcnow()
        db.flush()
        return True
    return False


def _test_database_connection(
    destination: Destination,
    override_params: Optional[dict] = None,
) -> tuple[bool, str, Optional[int]]:
    """
    Test database/storage connectivity through SSH tunnel if configured.
    Returns: (success, message, latency_ms)
    """
    import time as _time
    from app.utils.db_tester import _ssh_tunnel

    start = _time.time()
    connector_type = ""
    if destination.connector_definition:
        connector_type = (destination.connector_definition.connector_type or "").lower()
    config = destination.connection_config or {}
    if override_params:
        config = {**config, **override_params}

    host = config.get("host", "")
    port = int(config.get("port", 5432))
    database = config.get("database_name") or config.get("database") or config.get("dbname", "")
    user = config.get("username") or config.get("user", "")
    password = config.get("password", "")
    if not password and config.get("password_encrypted"):
        try:
            password = _decrypt_password(config["password_encrypted"])
        except Exception:
            password = ""
    ssh_config = config.get("ssh_config") or {}

    try:
        with _ssh_tunnel(ssh_config, host, port) as (bind_host, bind_port):
            if "postgres" in connector_type or "postgresql" in connector_type:
                import psycopg2
                conn = psycopg2.connect(
                    host=bind_host, port=bind_port, dbname=database,
                    user=user, password=password, connect_timeout=10,
                )
                conn.close()
            elif "mysql" in connector_type:
                import pymysql
                conn = pymysql.connect(
                    host=bind_host, port=bind_port, database=database,
                    user=user, password=password, connect_timeout=10,
                )
                conn.close()
            elif "mongo" in connector_type:
                import pymongo
                from urllib.parse import quote_plus as _qp, unquote as _uq
                _auth_src = config.get("auth_source") or config.get("authSource") or "admin"
                _rs = config.get("replica_set") or config.get("replicaSet") or ""
                _rp = config.get("read_preference") or config.get("readPreference") or ""
                _extra_hosts = (config.get("extra_hosts") or "").strip()
                _extra_params = (config.get("extra_uri_params") or "").strip().lstrip("?&")
                _using_tunnel = bool(ssh_config.get("tunnel_host"))
                _host_part = f"{bind_host}:{bind_port}" if (_using_tunnel or not _extra_hosts) else f"{bind_host}:{bind_port},{_extra_hosts}"
                if user:
                    uri = f"mongodb://{_qp(_uq(user))}:{_qp(_uq(password))}@{_host_part}/{database}?authSource={_auth_src}"
                else:
                    uri = f"mongodb://{_host_part}/{database}?"
                if _using_tunnel:
                    uri += "&directConnection=true"
                elif _extra_hosts or _rs:
                    if _rs:
                        uri += f"&replicaSet={_rs}"
                else:
                    uri += "&directConnection=true"
                if _rp:
                    uri += f"&readPreference={_rp}"
                if _extra_params:
                    uri += f"&{_extra_params}"
                client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=10000)
                client.server_info()
                client.close()
            else:
                import socket
                s = socket.create_connection((bind_host, bind_port), timeout=10)
                s.close()

        latency_ms = int((_time.time() - start) * 1000)
        return True, "Connection successful", latency_ms
    except Exception as exc:
        latency_ms = int((_time.time() - start) * 1000)
        return False, f"Connection failed: {exc}", latency_ms


def _validate_write_permissions(
    destination: Destination,
    test_params: Optional[dict] = None,
) -> tuple[bool, str]:
    """
    Validate destination has write permissions through SSH tunnel if configured.
    """
    from app.utils.db_tester import _ssh_tunnel

    config = destination.connection_config or {}
    if test_params:
        config = {**config, **test_params}
    connector_type = ""
    if destination.connector_definition:
        connector_type = (destination.connector_definition.connector_type or "").lower()

    host = config.get("host", "")
    port = int(config.get("port", 5432))
    database = config.get("database_name") or config.get("database", "")
    user = config.get("username") or config.get("user", "")
    password = config.get("password", "")
    if not password and config.get("password_encrypted"):
        try:
            password = _decrypt_password(config["password_encrypted"])
        except Exception:
            password = ""
    ssh_config = config.get("ssh_config") or {}

    try:
        with _ssh_tunnel(ssh_config, host, port) as (bind_host, bind_port):
            if "postgres" in connector_type or "postgresql" in connector_type:
                import psycopg2
                conn = psycopg2.connect(
                    host=bind_host, port=bind_port, dbname=database,
                    user=user, password=password, connect_timeout=10,
                )
                with conn.cursor() as cur:
                    cur.execute("CREATE TABLE IF NOT EXISTS _fusion_write_probe (id INT)")
                    cur.execute("INSERT INTO _fusion_write_probe VALUES (1)")
                    cur.execute("DROP TABLE _fusion_write_probe")
                conn.rollback()
                conn.close()
            elif "mysql" in connector_type:
                import pymysql
                conn = pymysql.connect(
                    host=bind_host, port=bind_port, database=database,
                    user=user, password=password, connect_timeout=10,
                )
                with conn.cursor() as cur:
                    cur.execute("CREATE TABLE IF NOT EXISTS _fusion_write_probe (id INT)")
                    cur.execute("INSERT INTO _fusion_write_probe VALUES (1)")
                    cur.execute("DROP TABLE _fusion_write_probe")
                conn.rollback()
                conn.close()
            elif "mongo" in connector_type:
                import pymongo
                from urllib.parse import quote_plus as _qp, unquote as _uq
                _auth_src = config.get("auth_source") or config.get("authSource") or "admin"
                _rs = config.get("replica_set") or config.get("replicaSet") or ""
                _rp = config.get("read_preference") or config.get("readPreference") or ""
                _extra_hosts = (config.get("extra_hosts") or "").strip()
                _extra_params = (config.get("extra_uri_params") or "").strip().lstrip("?&")
                _using_tunnel = bool(ssh_config.get("tunnel_host"))
                _host_part = f"{bind_host}:{bind_port}" if (_using_tunnel or not _extra_hosts) else f"{bind_host}:{bind_port},{_extra_hosts}"
                if user:
                    uri = f"mongodb://{_qp(_uq(user))}:{_qp(_uq(password))}@{_host_part}/{database}?authSource={_auth_src}"
                else:
                    uri = f"mongodb://{_host_part}/{database}?"
                if _using_tunnel:
                    uri += "&directConnection=true"
                elif _extra_hosts or _rs:
                    if _rs:
                        uri += f"&replicaSet={_rs}"
                else:
                    uri += "&directConnection=true"
                if _rp:
                    uri += f"&readPreference={_rp}"
                if _extra_params:
                    uri += f"&{_extra_params}"
                client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=10000)
                col = client[database]["_fusion_write_probe"]
                result = col.insert_one({"probe": 1})
                col.delete_one({"_id": result.inserted_id})
                client.close()
        return True, "Write permissions validated"
    except Exception as exc:
        return False, f"Write permission check failed: {exc}"


# ===========================
# CRUD Endpoints
# ===========================

@router.post("", status_code=status.HTTP_201_CREATED, response_model=DestinationResponse)
async def create_destination(
    destination_data: DestinationCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("destinations:create")),
):
    """
    Create a new destination configuration
    
    Requires: destinations:create permission
    """
    # Validate connector definition exists and is destination type
    connector = db.query(ConnectorDefinition).filter(
        ConnectorDefinition.connector_id == destination_data.connector_definition_id,
        ConnectorDefinition.category == "destination",
    ).first()
    
    if not connector:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Destination connector {destination_data.connector_definition_id} not found",
        )
    
    # Check for duplicate name in tenant
    existing = db.query(Destination).filter(
        Destination.destination_name == destination_data.destination_name,
        Destination.sub_tenant_id == current_user.sub_tenant_id,
        Destination.is_deleted == False,
    ).first()
    
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Destination with name '{destination_data.destination_name}' already exists",
        )
    
    # Encrypt password if provided
    encrypted_password = None
    if destination_data.password:
        encrypted_password = _encrypt_password(destination_data.password)
    
    # Build connection_config JSONB from individual fields + extra config
    # Merge connector-specific config (Iceberg Spark settings, write_mode, etc.)
    # directly into connection_config so they are readable without nesting.
    connection_config: dict = {}
    if destination_data.config:
        connection_config.update(destination_data.config)

    # Standard connection fields (override any config keys with the same name)
    if destination_data.host is not None:
        connection_config["host"] = destination_data.host
    if destination_data.port is not None:
        connection_config["port"] = destination_data.port
    if destination_data.database_name is not None:
        connection_config["database_name"] = destination_data.database_name
    if destination_data.schema_name is not None:
        connection_config["schema_name"] = destination_data.schema_name
    if destination_data.username is not None:
        connection_config["username"] = destination_data.username
    if encrypted_password:
        connection_config["password_encrypted"] = encrypted_password
    if destination_data.ssl_enabled:
        connection_config["ssl_enabled"] = destination_data.ssl_enabled
    if destination_data.ssl_config:
        connection_config["ssl_config"] = destination_data.ssl_config
    if destination_data.ssh_config:
        connection_config["ssh_config"] = destination_data.ssh_config
    
    # Create destination
    destination = Destination(
        destination_name=destination_data.destination_name,
        connector_definition_id=destination_data.connector_definition_id,
        connector_version=destination_data.connector_version,
        connection_config=connection_config,
        status=destination_data.status or "draft",
        sub_tenant_id=current_user.sub_tenant_id,
        bank_id=current_user.bank_id,
    )
    
    db.add(destination)
    db.commit()
    db.refresh(destination)
    
    # Load connector definition for response
    db.refresh(destination, ["connector_definition"])
    
    # Build response - convert connector_definition ORM to dict
    dest_dict = {
        "destination_id": destination.destination_id,
        "destination_name": destination.destination_name,
        "connector_definition_id": destination.connector_definition_id,
        "connector_version": destination.connector_version,
        "status": destination.status,
        "host": destination.host,
        "port": destination.port,
        "database_name": destination.database_name,
        "schema_name": destination.schema_name,
        "username": destination.username,
        "password": "********",
        "ssl_enabled": destination.ssl_enabled,
        "connection_test_status": destination.connection_test_status,
        "connection_test_error": destination.connection_test_error,
        "connection_test_at": destination.connection_test_at,
        "sub_tenant_id": destination.sub_tenant_id,
        "bank_id": destination.bank_id,
        "created_at": destination.created_at,
        "updated_at": destination.updated_at,
        "is_deleted": destination.is_deleted,
        "deleted_at": destination.deleted_at,
        "connector_definition": {
            "connector_id": str(connector.connector_id),
            "connector_name": connector.connector_name,
            "connector_type": connector.connector_type,
            "category": connector.category,
        } if connector else None,
    }
    response_data = DestinationResponse(**dest_dict)
    
    return response_data


@router.get("", response_model=DestinationListResponse)
async def list_destinations(
    status_filter: Optional[str] = Query(None, alias="status"),
    connector_type: Optional[str] = Query(None),
    connector_definition_id: Optional[UUID] = Query(None),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    List all destinations for the current tenant
    
    Supports filtering by:
    - status: draft, active, inactive
    - connector_type: MySQL Destination, PostgreSQL Destination, etc.
    - connector_definition_id: specific connector
    - search: search in destination name
    
    Results are paginated.
    """
    # Build base query with tenant filtering
    query = (
        db.query(Destination)
        .options(joinedload(Destination.connector_definition))
        .filter(
            Destination.sub_tenant_id == current_user.sub_tenant_id,
            Destination.is_deleted == False,
        )
    )
    
    # Apply filters
    if status_filter:
        query = query.filter(Destination.status == status_filter)
    
    if connector_definition_id:
        query = query.filter(Destination.connector_definition_id == connector_definition_id)
    
    if connector_type:
        query = query.join(ConnectorDefinition).filter(
            ConnectorDefinition.connector_name.ilike(f"%{connector_type}%")
        )
    
    if search:
        query = query.filter(
            Destination.destination_name.ilike(f"%{search}%")
        )
    
    # Get total count
    total = query.count()
    
    # Apply pagination
    offset = (page - 1) * page_size
    destinations = query.order_by(Destination.created_at.desc()).offset(offset).limit(page_size).all()
    
    # Mask passwords
    destination_responses = []
    for dest in destinations:
        dest_response = DestinationResponse.model_validate(dest)
        dest_response.password = "********"
        destination_responses.append(dest_response)
    
    total_pages = (total + page_size - 1) // page_size
    
    return DestinationListResponse(
        destinations=destination_responses,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


@router.get("/{destination_id}", response_model=DestinationResponse)
async def get_destination(
    destination_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get destination details by ID"""
    destination = _get_destination_by_id(db, destination_id, current_user)
    
    # Mask password
    response_data = DestinationResponse.model_validate(destination)
    response_data.password = "********"
    
    return response_data


@router.patch("/{destination_id}", response_model=DestinationResponse)
async def update_destination(
    destination_id: UUID,
    destination_data: DestinationUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("destinations:update")),
):
    """
    Update destination configuration
    
    Requires: destinations:update permission
    """
    destination = _get_destination_by_id(db, destination_id, current_user)
    
    # Check for duplicate name if name is being changed
    if destination_data.destination_name and destination_data.destination_name != destination.destination_name:
        existing = db.query(Destination).filter(
            Destination.sub_tenant_id == current_user.sub_tenant_id,
            Destination.destination_name == destination_data.destination_name,
            Destination.is_deleted == False,
            Destination.destination_id != destination_id,
        ).first()
        
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Destination with name '{destination_data.destination_name}' already exists",
            )
    
    # Update fields
    update_data = destination_data.model_dump(exclude_unset=True)
    
    # Handle password encryption
    if "password" in update_data and update_data["password"]:
        update_data.pop("password")
        # Store encrypted password inside connection_config
        encrypted = _encrypt_password(destination_data.password)
        current_cfg = dict(destination.connection_config or {})
        current_cfg["password_encrypted"] = encrypted
        destination.connection_config = current_cfg

    # Handle connector-specific config — merge into connection_config directly
    if "config" in update_data:
        new_config = update_data.pop("config") or {}
        current_cfg = dict(destination.connection_config or {})
        current_cfg.update(new_config)
        destination.connection_config = current_cfg

    # Handle standard connection fields that live inside connection_config
    conn_fields = {"host", "port", "database_name", "schema_name", "username",
                   "ssl_enabled", "ssl_config", "bucket_name", "region",
                   "path_prefix", "output_format", "compression"}
    conn_updates = {k: v for k, v in update_data.items() if k in conn_fields}
    if conn_updates:
        current_cfg = dict(destination.connection_config or {})
        current_cfg.update(conn_updates)
        destination.connection_config = current_cfg
        update_data = {k: v for k, v in update_data.items() if k not in conn_fields}

    # Apply remaining top-level ORM fields (destination_name, connector_version, status)
    for field, value in update_data.items():
        if hasattr(destination, field):
            setattr(destination, field, value)
    
    db.commit()
    db.refresh(destination)
    
    # Mask password
    response_data = DestinationResponse.model_validate(destination)
    response_data.password = "********"
    
    return response_data


@router.delete("/{destination_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_destination(
    destination_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("destinations:delete")),
):
    """
    Delete destination (soft delete)
    
    Requires: destinations:delete permission
    
    Will fail if there are active connections using this destination.
    """
    destination = _get_destination_by_id(db, destination_id, current_user)
    
    # Check for active connections
    active_connections = db.query(Connection).filter(
        Connection.destination_id == destination_id,
        Connection.status.in_(["active", "running"]),
        Connection.is_deleted == False,
    ).count()
    
    if active_connections > 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot delete destination with {active_connections} active connections",
        )
    
    # Soft delete
    destination.is_deleted = True
    destination.deleted_at = datetime.utcnow()
    
    db.commit()


# ===========================
# Connection Testing
# ===========================

@router.post("/test-tunnel", response_model=ConnectionTestResponse)
async def test_destination_tunnel_adhoc(
    body: dict,
    current_user: User = Depends(get_current_user),
):
    """
    Ad-hoc SSH tunnel test without a saved destination.
    Send: {"ssh_config": {tunnel_host, tunnel_port, tunnel_username, tunnel_auth_method, ...}}
    """
    from app.utils.db_tester import test_tunnel_connection
    ssh_config = body.get("ssh_config") or body.get("ssl_config") or body
    success, message, latency_ms = test_tunnel_connection(ssh_config)
    return ConnectionTestResponse(
        status="success" if success else "failed",
        message=message,
        error_details=None if success else message,
        connection_test_at=datetime.utcnow(),
        latency_ms=latency_ms,
    )


@router.post("/{destination_id}/test-tunnel", response_model=ConnectionTestResponse)
async def test_destination_tunnel(
    destination_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Test only the SSH tunnel for a saved destination."""
    from app.utils.db_tester import test_tunnel_connection
    destination = _get_destination_by_id(db, destination_id, current_user)
    ssh_config = destination.ssh_config or {}
    success, message, latency_ms = test_tunnel_connection(ssh_config)
    return ConnectionTestResponse(
        status="success" if success else "failed",
        message=message,
        error_details=None if success else message,
        connection_test_at=datetime.utcnow(),
        latency_ms=latency_ms,
    )


@router.post("/{destination_id}/test-connection", response_model=ConnectionTestResponse)
async def test_destination_connection(
    destination_id: UUID,
    test_params: Optional[ConnectionTestRequest] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Test destination connectivity
    
    Can provide override parameters for testing without saving them.
    """
    destination = _get_destination_by_id(db, destination_id, current_user)
    
    # Build test parameters
    override_params = None
    if test_params:
        override_params = test_params.model_dump(exclude_unset=True)
    
    # Test connection
    success, message, latency_ms = _test_database_connection(destination, override_params)
    
    # Update destination if test used actual credentials (no override)
    if not override_params:
        destination.connection_test_status = "success" if success else "failed"
        destination.connection_test_error = None if success else message
        destination.connection_test_at = datetime.utcnow()
        _maybe_activate_destination(destination, db)
        db.commit()
    
    return ConnectionTestResponse(
        status="success" if success else "failed",
        message=message,
        error_details=None if success else message,
        connection_test_at=datetime.utcnow(),
        latency_ms=latency_ms,
    )


@router.post("/{destination_id}/validate-write-permissions", response_model=dict)
async def validate_write_permissions(
    destination_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Validate destination has write permissions
    
    Checks if the destination credentials can:
    - Create tables (if required)
    - Insert data
    - Update data (for upsert mode)
    """
    destination = _get_destination_by_id(db, destination_id, current_user)
    
    success, message = _validate_write_permissions(destination)
    
    # Store result in connection_config so auto-activate can check it
    if success:
        if not destination.connection_config:
            destination.connection_config = {}
        destination.connection_config = {**destination.connection_config, "write_permissions_validated": True}
        _maybe_activate_destination(destination, db)
        db.commit()
    
    return {
        "destination_id": destination_id,
        "has_write_permissions": success,
        "message": message,
        "validated_at": datetime.utcnow(),
    }


# ===========================
# Write Mode Configuration
# ===========================

@router.post("/{destination_id}/write-mode", response_model=WriteModeConfigResponse)
async def configure_write_mode(
    destination_id: UUID,
    write_mode_config: WriteModeConfig,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("destinations:update")),
):
    """
    Configure write mode for destination
    
    Requires: destinations:update permission
    
    Write modes:
    - append: Add new records to existing data
    - replace: Drop/truncate and reload all data
    - upsert: Insert new records, update existing based on primary keys
    - merge: Complex merge logic with custom rules
    """
    destination = _get_destination_by_id(db, destination_id, current_user)
    
    # Validate upsert configuration
    if write_mode_config.write_mode in ["upsert", "merge"]:
        if not write_mode_config.primary_keys:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"primary_keys required for {write_mode_config.write_mode} mode",
            )
    
    # Store write mode config in destination.config
    if "write_mode" not in destination.config:
        destination.config["write_mode"] = {}
    
    destination.config["write_mode"] = write_mode_config.model_dump()
    
    # Mark config as updated (SQLAlchemy doesn't auto-detect JSONB changes)
    from sqlalchemy import flag_modified
    flag_modified(destination, "config")
    
    db.commit()
    db.refresh(destination)
    
    return WriteModeConfigResponse(
        destination_id=destination_id,
        write_mode_config=write_mode_config,
        updated_at=destination.updated_at,
    )


@router.get("/{destination_id}/write-mode", response_model=WriteModeConfigResponse)
async def get_write_mode_config(
    destination_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get current write mode configuration"""
    destination = _get_destination_by_id(db, destination_id, current_user)
    
    write_mode_data = destination.config.get("write_mode", {})
    
    if not write_mode_data:
        # Return default config
        write_mode_data = {
            "write_mode": "append",
            "create_table_if_not_exists": True,
            "drop_table_before_load": False,
            "truncate_before_load": False,
        }
    
    write_mode_config = WriteModeConfig(**write_mode_data)
    
    return WriteModeConfigResponse(
        destination_id=destination_id,
        write_mode_config=write_mode_config,
        updated_at=destination.updated_at,
    )


# ===========================
# Schema Mapping
# ===========================

@router.post("/{destination_id}/schema-mapping", response_model=SchemaMappingResponse)
async def configure_schema_mapping(
    destination_id: UUID,
    mapping_request: SchemaMappingRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("destinations:update")),
):
    """
    Configure schema mapping for destination table
    
    Requires: destinations:update permission
    
    Maps source columns to destination columns with optional transformations.
    """
    destination = _get_destination_by_id(db, destination_id, current_user)
    
    # Store mapping in destination.config
    if "schema_mappings" not in destination.config:
        destination.config["schema_mappings"] = {}
    
    destination.config["schema_mappings"][mapping_request.table_name] = {
        "column_mappings": [m.model_dump() for m in mapping_request.column_mappings],
        "enable_auto_mapping": mapping_request.enable_auto_mapping,
        "case_sensitive": mapping_request.case_sensitive,
        "drop_unmapped_columns": mapping_request.drop_unmapped_columns,
        "updated_at": datetime.utcnow().isoformat(),
    }
    
    from sqlalchemy import flag_modified
    flag_modified(destination, "config")
    
    db.commit()
    db.refresh(destination)
    
    return SchemaMappingResponse(
        destination_id=destination_id,
        table_name=mapping_request.table_name,
        column_mappings=mapping_request.column_mappings,
        total_mappings=len(mapping_request.column_mappings),
        created_at=destination.created_at,
        updated_at=destination.updated_at,
    )


@router.get("/{destination_id}/schema-mapping/{table_name}", response_model=SchemaMappingResponse)
async def get_schema_mapping(
    destination_id: UUID,
    table_name: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get schema mapping for specific table"""
    destination = _get_destination_by_id(db, destination_id, current_user)
    
    mappings = destination.config.get("schema_mappings", {}).get(table_name)
    
    if not mappings:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No schema mapping found for table '{table_name}'",
        )
    
    from app.schemas.destination import ColumnMapping
    column_mappings = [ColumnMapping(**m) for m in mappings["column_mappings"]]
    
    return SchemaMappingResponse(
        destination_id=destination_id,
        table_name=table_name,
        column_mappings=column_mappings,
        total_mappings=len(column_mappings),
        created_at=destination.created_at,
        updated_at=destination.updated_at,
    )


# ===========================
# Batch Settings
# ===========================

@router.post("/{destination_id}/batch-settings", response_model=BatchSettingsResponse)
async def configure_batch_settings(
    destination_id: UUID,
    batch_settings: BatchSettings,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("destinations:update")),
):
    """
    Configure batch write settings
    
    Requires: destinations:update permission
    
    Controls how data is batched and written to the destination.
    """
    destination = _get_destination_by_id(db, destination_id, current_user)
    
    # Store batch settings in destination.config
    destination.config["batch_settings"] = batch_settings.model_dump()
    
    from sqlalchemy import flag_modified
    flag_modified(destination, "config")
    
    db.commit()
    db.refresh(destination)
    
    return BatchSettingsResponse(
        destination_id=destination_id,
        batch_settings=batch_settings,
        updated_at=destination.updated_at,
    )


@router.get("/{destination_id}/batch-settings", response_model=BatchSettingsResponse)
async def get_batch_settings(
    destination_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get current batch settings"""
    destination = _get_destination_by_id(db, destination_id, current_user)
    
    batch_data = destination.config.get("batch_settings", {})
    
    if not batch_data:
        # Return default settings
        batch_data = BatchSettings().model_dump()
    
    batch_settings = BatchSettings(**batch_data)
    
    return BatchSettingsResponse(
        destination_id=destination_id,
        batch_settings=batch_settings,
        updated_at=destination.updated_at,
    )


# ===========================
# Statistics
# ===========================

@router.get("/{destination_id}/stats", response_model=DestinationStats)
async def get_destination_stats(
    destination_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get destination usage statistics
    
    Returns connection count, sync statistics, and performance metrics.
    """
    destination = _get_destination_by_id(db, destination_id, current_user)

    # Count connections
    total_connections = db.query(Connection).filter(
        Connection.destination_id == destination_id,
        Connection.is_deleted == False,
    ).count()

    active_connections = db.query(Connection).filter(
        Connection.destination_id == destination_id,
        Connection.status == "active",
        Connection.is_deleted == False,
    ).count()

    # Derive sync stats from audit_logs (spec §5 P5-8)
    dest_id_str = str(destination_id)
    conn_ids = [
        str(c.connection_id)
        for c in db.query(Connection.connection_id).filter(
            Connection.destination_id == destination_id,
            Connection.is_deleted == False,
        ).all()
    ]
    total_syncs = 0
    successful_syncs = 0
    failed_syncs = 0
    total_rows_written = 0
    last_sync_at = None

    if conn_ids:
        run_logs = (
            db.query(AuditLog)
            .filter(
                AuditLog.resource_type == "connection",
                AuditLog.resource_id.in_(conn_ids),
                AuditLog.action.like("connection.batch_run.%"),
            )
            .order_by(AuditLog.created_at.desc())
            .limit(5000)
            .all()
        )
        total_syncs = len(run_logs)
        successful_syncs = sum(1 for r in run_logs if r.action == "connection.batch_run.success")
        failed_syncs = total_syncs - successful_syncs
        total_rows_written = sum(
            int((r.details or {}).get("rows_synced", 0) or 0)
            for r in run_logs if r.action == "connection.batch_run.success"
        )
        last_sync_at = run_logs[0].created_at if run_logs else None

    return DestinationStats(
        destination_id=destination_id,
        destination_name=destination.destination_name,
        total_connections=total_connections,
        active_connections=active_connections,
        total_syncs=total_syncs,
        successful_syncs=successful_syncs,
        failed_syncs=failed_syncs,
        last_sync_at=last_sync_at,
        total_rows_written=total_rows_written,
        total_bytes_written=0,
        avg_write_latency_ms=None,
        avg_batch_size=None,
        avg_throughput_rows_per_sec=None,
    )
