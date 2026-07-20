"""Pydantic schemas for Connection and Stream configuration"""

from datetime import datetime
from typing import Optional, Dict, Any, List
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator


# ===========================
# Stream Schemas
# ===========================

class StreamBase(BaseModel):
    """Base schema for stream configuration"""
    
    stream_name: str = Field(..., min_length=1, max_length=255, description="Stream identifier")
    stream_namespace: Optional[str] = Field(None, max_length=255, description="Stream namespace")
    
    # Source Configuration
    source_table_name: str = Field(..., max_length=255, description="Source table/collection name")
    source_schema_name: Optional[str] = Field(None, max_length=255, description="Source schema name")
    
    # Destination Mapping
    destination_table_name: str = Field(..., max_length=255, description="Destination table name")
    destination_schema_name: Optional[str] = Field(None, max_length=255, description="Destination schema")
    
    # Sync Configuration
    sync_mode: str = Field(..., description="cdc, full_refresh, incremental")
    cursor_field: Optional[str] = Field(None, description="Field for incremental sync")
    primary_keys: List[str] = Field(default_factory=list, description="Primary key columns")
    
    # Schema and Mapping
    source_schema: Optional[Dict[str, Any]] = Field(None, description="Source table schema")
    destination_schema: Optional[Dict[str, Any]] = Field(None, description="Destination table schema")
    selected_columns: Optional[List[str]] = Field(None, description="Columns to sync (null = all)")
    column_mapping: Dict[str, str] = Field(default_factory=dict, description="Column name mappings")
    
    # Status
    is_enabled: bool = Field(True, description="Enable/disable this stream")
    
    # Per-column transform pipeline steps (assembled from the UI column-level config).
    # Structure: {"transforms": [{"id", "type", "column", ...}, ...]}
    # Saved into Stream.transform_overrides on the server.
    transform_steps: Optional[Dict[str, Any]] = Field(
        None,
        description="Transform pipeline spec for this stream (per-column steps + table UDFs from UI)"
    )
    
    @field_validator("sync_mode")
    @classmethod
    def validate_sync_mode(cls, v: str) -> str:
        """Validate sync mode"""
        if v not in ["cdc", "full_refresh", "incremental"]:
            raise ValueError("Sync mode must be one of: cdc, full_refresh, incremental")
        return v


class StreamCreate(StreamBase):
    """Schema for creating a new stream"""
    pass


class StreamUpdate(BaseModel):
    """Schema for updating a stream"""
    
    stream_name: Optional[str] = Field(None, min_length=1, max_length=255)
    stream_namespace: Optional[str] = Field(None, max_length=255)
    destination_table_name: Optional[str] = Field(None, max_length=255)
    destination_schema_name: Optional[str] = Field(None, max_length=255)
    sync_mode: Optional[str] = None
    cursor_field: Optional[str] = None
    primary_keys: Optional[List[str]] = None
    selected_columns: Optional[List[str]] = None
    column_mapping: Optional[Dict[str, str]] = None
    transform_steps: Optional[Dict[str, Any]] = None  # updates transform_overrides
    is_enabled: Optional[bool] = None
    
    @field_validator("sync_mode")
    @classmethod
    def validate_sync_mode(cls, v: Optional[str]) -> Optional[str]:
        """Validate sync mode"""
        if v and v not in ["cdc", "full_refresh", "incremental"]:
            raise ValueError("Sync mode must be one of: cdc, full_refresh, incremental")
        return v


class StreamResponse(BaseModel):
    """Schema for stream response — maps from DB model columns"""
    
    stream_id: UUID
    connection_id: UUID
    stream_name: Optional[str] = None
    stream_namespace: Optional[str] = None
    source_table_name: Optional[str] = None
    source_schema_name: Optional[str] = None
    destination_table_name: Optional[str] = None
    destination_schema_name: Optional[str] = None
    sync_mode: Optional[str] = None
    cursor_field: Optional[str] = None
    primary_keys: Optional[List[str]] = Field(default_factory=list)
    selected_columns: Optional[List[str]] = None
    column_mapping: Optional[Dict[str, Any]] = None
    transform_overrides: Optional[Dict[str, Any]] = None  # full transform spec from UI
    is_enabled: bool = True
    sync_status: Optional[str] = None
    last_synced_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True
    
    @model_validator(mode="before")
    @classmethod
    def map_model_fields(cls, data):
        """Map ORM model columns to response field names"""
        if hasattr(data, 'source_table_name'):
            # ORM object with new column names
            return {
                'stream_id': data.stream_id,
                'connection_id': data.connection_id,
                'stream_name': getattr(data, 'stream_name', None),
                'stream_namespace': getattr(data, 'stream_namespace', None),
                'source_table_name': getattr(data, 'source_table_name', None),
                'source_schema_name': getattr(data, 'source_schema_name', None),
                'destination_table_name': getattr(data, 'destination_table_name', None),
                'destination_schema_name': getattr(data, 'destination_schema_name', None),
                'sync_mode': data.sync_mode,
                'cursor_field': data.cursor_field,
                'primary_keys': data.primary_keys if isinstance(data.primary_keys, list) else [],
                'selected_columns': getattr(data, 'selected_columns', None),
                'column_mapping': getattr(data, 'column_mapping', None),
                'transform_overrides': getattr(data, 'transform_overrides', None),
                'is_enabled': data.is_enabled if data.is_enabled is not None else True,
                'created_at': getattr(data, 'created_at', None),
                'updated_at': getattr(data, 'updated_at', None),
            }
        return data


# ===========================
# Connection Schemas
# ===========================

class ConnectionBase(BaseModel):
    """Base schema for connection configuration"""
    
    connection_name: str = Field(..., min_length=1, max_length=255, description="Unique connection name")
    source_id: UUID = Field(..., description="Source configuration ID")
    destination_id: UUID = Field(..., description="Destination configuration ID")
    
    # Sync Configuration
    sync_mode: str = Field(..., description="cdc, full_refresh, incremental")
    sync_type: Optional[str] = Field("BATCH", description="BATCH/SCHEDULED → Airflow, CDC/REALTIME → streaming worker")
    sync_frequency: Optional[str] = Field(None, description="Cron expression or 'manual'")
    sync_enabled: bool = Field(True, description="Enable automatic syncs")
    
    # Resource Limits
    resource_limits: Dict[str, Any] = Field(
        default_factory=dict,
        description="Resource constraints (max_memory, max_cpu, max_parallelism)"
    )
    
    # Replication Settings (for CDC)
    replication_slot: Optional[str] = Field(None, description="Database replication slot name")
    publication: Optional[str] = Field(None, description="Database publication name")
    
    # Namespace Mapping
    namespace_definition: Optional[str] = Field(None, description="source, destination, custom")
    namespace_format: Optional[str] = Field(None, description="Namespace format string")
    stream_prefix: Optional[str] = Field(None, description="Prefix for stream names")
    
    # Additional Configuration
    config: Dict[str, Any] = Field(default_factory=dict, description="Additional settings")
    
    @field_validator("sync_mode")
    @classmethod
    def validate_sync_mode(cls, v: str) -> str:
        """Validate sync mode"""
        v = v.lower()
        if v not in ["cdc", "full_refresh", "incremental"]:
            raise ValueError("Sync mode must be one of: cdc, full_refresh, incremental")
        return v
    
    @field_validator("namespace_definition")
    @classmethod
    def validate_namespace_definition(cls, v: Optional[str]) -> Optional[str]:
        """Validate namespace definition"""
        if v and v not in ["source", "destination", "custom"]:
            raise ValueError("Namespace definition must be one of: source, destination, custom")
        return v


class ConnectionCreate(ConnectionBase):
    """Schema for creating a new connection"""
    
    status: Optional[str] = Field("draft", description="Initial status: draft, active, paused, inactive")
    streams: Optional[List[StreamCreate]] = Field(None, description="Initial stream configurations")
    
    @field_validator("status")
    @classmethod
    def validate_status(cls, v: Optional[str]) -> str:
        """Validate status"""
        if v and v not in ["draft", "active", "paused", "inactive"]:
            raise ValueError("Status must be one of: draft, active, paused, inactive")
        return v or "draft"


class ConnectionUpdate(BaseModel):
    """Schema for updating a connection"""
    
    connection_name: Optional[str] = Field(None, min_length=1, max_length=255)
    sync_mode: Optional[str] = None
    sync_frequency: Optional[str] = None
    sync_enabled: Optional[bool] = None
    resource_limits: Optional[Dict[str, Any]] = None
    replication_slot: Optional[str] = None
    publication: Optional[str] = None
    namespace_definition: Optional[str] = None
    namespace_format: Optional[str] = None
    stream_prefix: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    status: Optional[str] = None
    
    @field_validator("sync_mode")
    @classmethod
    def validate_sync_mode(cls, v: Optional[str]) -> Optional[str]:
        """Validate sync mode"""
        if v:
            v = v.lower()
            if v not in ["cdc", "full_refresh", "incremental"]:
                raise ValueError("Sync mode must be one of: cdc, full_refresh, incremental")
        return v
    
    @field_validator("status")
    @classmethod
    def validate_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate status"""
        if v and v not in ["draft", "active", "paused", "inactive"]:
            raise ValueError("Status must be one of: draft, active, paused, inactive")
        return v


class ConnectionResponse(ConnectionBase):
    """Schema for connection response"""
    
    connection_id: UUID
    status: str
    last_sync_status: Optional[str] = None
    last_sync_at: Optional[datetime] = None
    next_sync_at: Optional[datetime] = None
    error_message: Optional[str] = None
    consecutive_failures: int = 0
    
    # Override parent fields that may not exist on model
    sync_frequency: Optional[str] = None
    sync_type: Optional[str] = None
    sync_enabled: Optional[bool] = True
    replication_slot: Optional[str] = None
    publication: Optional[str] = None
    namespace_definition: Optional[str] = None
    namespace_format: Optional[str] = None
    stream_prefix: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    
    # Tenant Information
    sub_tenant_id: Optional[UUID] = None
    bank_id: Optional[UUID] = None
    
    # Timestamps
    created_at: datetime
    updated_at: datetime
    is_deleted: bool = False
    deleted_at: Optional[datetime] = None
    
    # Related Objects
    source: Optional[Any] = None
    destination: Optional[Any] = None
    streams: Optional[List[StreamResponse]] = None
    
    @field_validator("source", mode="before")
    @classmethod
    def serialize_source(cls, v):
        if v is None:
            return None
        if isinstance(v, dict):
            return v
        return {
            "source_id": str(v.source_id),
            "source_name": v.source_name,
            "status": v.status,
            "connector_type": getattr(v, 'connector_type', None) or (
                v.connector_definition.connector_type
                if getattr(v, 'connector_definition', None) else None
            ),
        }
    
    @field_validator("destination", mode="before")
    @classmethod
    def serialize_destination(cls, v):
        if v is None:
            return None
        if isinstance(v, dict):
            return v
        return {
            "destination_id": str(v.destination_id),
            "destination_name": v.destination_name,
            "status": v.status,
            "connector_type": getattr(v, 'connector_type', None) or (
                v.connector_definition.connector_type
                if getattr(v, 'connector_definition', None) else None
            ),
        }
    
    class Config:
        from_attributes = True


class ConnectionListResponse(BaseModel):
    """Schema for paginated connection list"""
    
    connections: List[ConnectionResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class ConnectionSearchFilters(BaseModel):
    """Schema for connection search filters"""
    
    status: Optional[str] = Field(None, description="Filter by status")
    sync_mode: Optional[str] = Field(None, description="Filter by sync mode")
    source_id: Optional[UUID] = Field(None, description="Filter by source")
    destination_id: Optional[UUID] = Field(None, description="Filter by destination")
    search: Optional[str] = Field(None, description="Search in connection name")
    page: int = Field(1, ge=1, description="Page number")
    page_size: int = Field(50, ge=1, le=100, description="Items per page")


# ===========================
# Connection Validation Schemas
# ===========================

class ConnectionValidationRequest(BaseModel):
    """Schema for validating connection compatibility"""
    
    source_id: UUID
    destination_id: UUID
    sync_mode: str
    
    @field_validator("sync_mode")
    @classmethod
    def validate_sync_mode(cls, v: str) -> str:
        """Validate sync mode"""
        if v not in ["cdc", "full_refresh", "incremental"]:
            raise ValueError("Sync mode must be one of: cdc, full_refresh, incremental")
        return v


class ConnectionValidationResponse(BaseModel):
    """Schema for connection validation result"""
    
    is_valid: bool
    message: str
    issues: List[str] = Field(default_factory=list)
    source_compatible: bool
    destination_compatible: bool
    sync_mode_supported: bool
    validated_at: datetime


# ===========================
# Schedule Configuration Schemas
# ===========================

class ScheduleConfig(BaseModel):
    """Schema for configuring connection schedule"""
    
    sync_frequency: str = Field(..., description="Cron expression or 'manual'")
    sync_enabled: bool = Field(True, description="Enable scheduled syncs")
    timezone: str = Field("UTC", description="Timezone for scheduling")
    
    @field_validator("sync_frequency")
    @classmethod
    def validate_sync_frequency(cls, v: str) -> str:
        """Basic cron validation"""
        if v == "manual":
            return v
        # Basic cron format check (5 or 6 fields)
        parts = v.split()
        if len(parts) not in [5, 6]:
            raise ValueError("Invalid cron expression format")
        return v


class ScheduleConfigResponse(BaseModel):
    """Schema for schedule configuration response"""
    
    connection_id: UUID
    schedule_config: ScheduleConfig
    next_sync_at: Optional[datetime] = None
    updated_at: datetime


# ===========================
# Connection Actions Schemas
# ===========================

class ConnectionActivateRequest(BaseModel):
    """Schema for activating a connection"""
    
    validate_first: bool = Field(True, description="Validate before activation")
    skip_initial_sync: bool = Field(False, description="Skip initial full sync")


class ConnectionActivateResponse(BaseModel):
    """Schema for connection activation response"""
    
    connection_id: UUID
    status: str
    message: str
    validation_result: Optional[ConnectionValidationResponse] = None
    activated_at: datetime


class SyncTriggerRequest(BaseModel):
    """Schema for triggering manual sync"""
    
    full_refresh: bool = Field(False, description="Force full refresh")
    selected_streams: Optional[List[UUID]] = Field(None, description="Sync specific streams only")


class SyncTriggerResponse(BaseModel):
    """Schema for sync trigger response"""
    
    connection_id: UUID
    sync_triggered: bool
    message: str
    triggered_at: datetime
    estimated_duration_seconds: Optional[int] = None


# ===========================
# Connection Statistics Schemas
# ===========================

class ConnectionStats(BaseModel):
    """Schema for connection usage statistics"""
    
    connection_id: UUID
    connection_name: str
    status: str
    
    # Sync Statistics
    total_syncs: int = Field(0, description="Total sync operations")
    successful_syncs: int = Field(0, description="Successful syncs")
    failed_syncs: int = Field(0, description="Failed syncs")
    last_sync_at: Optional[datetime] = None
    last_sync_duration_seconds: Optional[int] = None
    
    # Data Volume
    total_rows_synced: int = Field(0, description="Total rows synced")
    total_bytes_synced: int = Field(0, description="Total bytes synced")
    
    # Performance Metrics
    avg_sync_duration_seconds: Optional[float] = None
    avg_throughput_rows_per_sec: Optional[float] = None
    
    # Error Statistics
    consecutive_failures: int = 0
    last_error: Optional[str] = None
    
    # Stream Statistics
    total_streams: int = Field(0, description="Total configured streams")
    active_streams: int = Field(0, description="Currently syncing streams")
    
    # Frontend display fields
    events_per_hour: Optional[int] = Field(0, description="Events processed per hour")
    lag_seconds: Optional[float] = Field(None, description="Current replication lag in seconds")
    uptime_percent: Optional[float] = Field(None, description="Connection uptime percentage")
    last_sync: Optional[str] = Field(None, description="Human-readable last sync time")


# ===========================
# Stream Statistics Schemas
# ===========================

class StreamStats(BaseModel):
    """Schema for individual stream statistics"""
    
    stream_id: UUID
    stream_name: str
    
    # Sync Statistics
    total_syncs: int = 0
    successful_syncs: int = 0
    failed_syncs: int = 0
    last_synced_at: Optional[datetime] = None
    
    # Data Volume
    total_rows_synced: int = 0
    total_bytes_synced: int = 0
    
    # Performance
    avg_sync_duration_seconds: Optional[float] = None
