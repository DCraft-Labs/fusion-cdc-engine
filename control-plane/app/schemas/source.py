"""
Source Configuration Schemas
Pydantic models for source configuration API requests and responses
"""
from typing import Optional, Dict, Any, List
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field, validator


# ============================================================================
# Base Schemas
# ============================================================================

class SourceBase(BaseModel):
    """Base schema for Source with common fields"""
    source_name: str = Field(..., min_length=1, max_length=255, description="Name of the source")
    connector_definition_id: UUID = Field(..., description="ID of the connector definition to use")
    connector_version: str = Field(..., min_length=1, max_length=50, description="Version of connector")
    
    # Connection details
    host: str = Field(..., min_length=1, max_length=500, description="Host address")
    port: int = Field(..., ge=1, le=65535, description="Port number")
    database_name: str = Field(..., min_length=1, max_length=255, description="Database name")
    username: Optional[str] = Field("", max_length=255, description="Username for authentication")
    password: Optional[str] = Field("", description="Password (will be encrypted)")
    
    # SSL/TLS Configuration
    ssl_enabled: bool = Field(default=False, description="Enable SSL/TLS")
    ssl_config: Optional[Dict[str, Any]] = Field(default_factory=dict,
        description="TLS settings: ssl_mode (disable|allow|prefer|require|verify-ca|verify-full), ssl_ca, ssl_cert, ssl_key")

    # SSH Tunnel Configuration (separate from SSL)
    ssh_config: Optional[Dict[str, Any]] = Field(default_factory=dict,
        description="SSH tunnel settings: tunnel_host, tunnel_port, tunnel_username, tunnel_auth_method, tunnel_password|tunnel_private_key, tunnel_passphrase")

    # Additional configuration
    config: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional connector config")


class SourceCreate(SourceBase):
    """Schema for creating a new source"""
    pass


class SourceUpdate(BaseModel):
    """Schema for updating an existing source"""
    source_name: Optional[str] = Field(None, min_length=1, max_length=255)
    connector_version: Optional[str] = Field(None, min_length=1, max_length=50)
    
    # Connection details (all optional for partial updates)
    host: Optional[str] = Field(None, min_length=1, max_length=500)
    port: Optional[int] = Field(None, ge=1, le=65535)
    database_name: Optional[str] = Field(None, min_length=1, max_length=255)
    username: Optional[str] = Field(None, min_length=1, max_length=255)
    password: Optional[str] = Field(None, min_length=1)
    
    # SSL/TLS Configuration
    ssl_enabled: Optional[bool] = None
    ssl_config: Optional[Dict[str, Any]] = None

    # SSH Tunnel Configuration
    ssh_config: Optional[Dict[str, Any]] = None

    # Additional configuration
    config: Optional[Dict[str, Any]] = None
    status: Optional[str] = Field(None, description="Status: draft, active, inactive")
    
    @validator('status')
    def validate_status(cls, v):
        if v is not None and v not in ['draft', 'active', 'inactive']:
            raise ValueError('Status must be draft, active, or inactive')
        return v


class SourceResponse(SourceBase):
    """Schema for source response"""
    source_id: UUID
    status: str
    
    # Override password - ORM stores password_encrypted, not password
    password: Optional[str] = Field(default="***", description="Password (masked)")
    
    # Discovery cache
    discovery_cache: Optional[Dict[str, Any]] = None
    last_discovery_at: Optional[datetime] = None
    
    # Connection test
    connection_test_status: Optional[str] = None
    connection_test_error: Optional[str] = None
    connection_test_at: Optional[datetime] = None
    
    # Multi-tenancy
    bank_id: Optional[UUID] = None
    sub_tenant_id: Optional[UUID] = None
    
    # Soft delete
    is_deleted: bool = False
    deleted_at: Optional[datetime] = None
    
    # Timestamps
    created_at: datetime
    updated_at: datetime
    created_by: Optional[UUID] = None
    
    # Relationships
    connector_definition_name: Optional[str] = None
    connector_definition_type: Optional[str] = None
    
    class Config:
        from_attributes = True
        
    @validator('password', pre=True, always=True)
    def hide_password(cls, v):
        """Never expose password in responses"""
        return "********"


class SourceListResponse(BaseModel):
    """Schema for paginated source list"""
    sources: List[SourceResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class SourceSearchFilters(BaseModel):
    """Schema for source search filters"""
    status: Optional[str] = Field(None, description="Filter by status")
    connector_type: Optional[str] = Field(None, description="Filter by connector type")
    connector_definition_id: Optional[UUID] = Field(None, description="Filter by connector definition")
    search: Optional[str] = Field(None, description="Search in name")
    
    @validator('status')
    def validate_status(cls, v):
        if v is not None and v not in ['draft', 'active', 'inactive']:
            raise ValueError('Status must be draft, active, or inactive')
        return v


# ============================================================================
# Connection Testing Schemas
# ============================================================================

class ConnectionTestRequest(BaseModel):
    """Schema for connection test request (optional override credentials)"""
    host: Optional[str] = None
    port: Optional[int] = None
    database_name: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    ssl_enabled: Optional[bool] = None
    ssl_config: Optional[Dict[str, Any]] = None


class ConnectionTestResponse(BaseModel):
    """Schema for connection test response"""
    status: str = Field(..., description="success or failure")
    message: str = Field(..., description="Human-readable message")
    error_details: Optional[str] = None
    connection_test_at: datetime
    latency_ms: Optional[int] = Field(None, description="Connection latency in milliseconds")


# ============================================================================
# Schema Discovery Schemas
# ============================================================================

class TableInfo(BaseModel):
    """Schema for table information"""
    schema_name: str
    table_name: str
    table_type: str = Field(..., description="TABLE, VIEW, etc.")
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None


class ColumnInfo(BaseModel):
    """Schema for column information"""
    column_name: str
    data_type: str
    is_nullable: bool
    is_primary_key: bool
    default_value: Optional[str] = None
    character_maximum_length: Optional[int] = None


class SchemaInfo(BaseModel):
    """Schema for database schema information"""
    schema_name: str
    tables: List[TableInfo]


class SchemaDiscoveryResponse(BaseModel):
    """Schema for schema discovery response"""
    schemas: List[SchemaInfo]
    total_schemas: int
    total_tables: int
    last_discovery_at: datetime


class TableSchemaRequest(BaseModel):
    """Schema for requesting table schema details"""
    schema_name: str = Field(..., min_length=1)
    table_name: str = Field(..., min_length=1)


class TableSchemaResponse(BaseModel):
    """Schema for table schema response"""
    schema_name: str
    table_name: str
    columns: List[ColumnInfo]
    primary_keys: List[str]
    indexes: Optional[List[Dict[str, Any]]] = None


# ============================================================================
# CDC Configuration Schemas
# ============================================================================

class CDCConfigRequest(BaseModel):
    """Schema for CDC configuration request"""
    enable_cdc: bool = Field(..., description="Enable CDC on this source")
    replication_method: str = Field(..., description="CDC method: binlog, wal, change_streams, etc.")
    replication_config: Dict[str, Any] = Field(
        default_factory=dict,
        description="Replication-specific config (slot_name, server_id, etc.)"
    )
    
    @validator('replication_method')
    def validate_replication_method(cls, v):
        valid_methods = ['binlog', 'wal', 'change_streams', 'log_based', 'trigger_based']
        if v not in valid_methods:
            raise ValueError(f'Replication method must be one of: {", ".join(valid_methods)}')
        return v


class CDCConfigResponse(BaseModel):
    """Schema for CDC configuration response"""
    enable_cdc: bool
    replication_method: Optional[str] = None
    replication_config: Dict[str, Any]
    cdc_status: str = Field(..., description="not_configured, configured, active, error")
    last_updated_at: datetime


# ============================================================================
# Source Statistics Schemas
# ============================================================================

class SourceStats(BaseModel):
    """Schema for source statistics"""
    source_id: UUID
    source_name: str
    total_connections: int = Field(..., description="Number of connections using this source")
    active_connections: int = Field(..., description="Number of active connections")
    total_syncs: int = Field(..., description="Total sync runs")
    successful_syncs: int = Field(..., description="Successful sync runs")
    failed_syncs: int = Field(..., description="Failed sync runs")
    last_sync_at: Optional[datetime] = None
    total_rows_extracted: int = Field(default=0, description="Total rows extracted across all syncs")
    total_bytes_extracted: int = Field(default=0, description="Total bytes extracted")
