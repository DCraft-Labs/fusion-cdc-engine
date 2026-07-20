"""Pydantic schemas for Destination configuration"""

from datetime import datetime
from typing import Optional, Dict, Any, List
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


# ===========================
# Base Schemas
# ===========================

class DestinationBase(BaseModel):
    """Base schema for destination configuration"""
    
    destination_name: str = Field(..., min_length=1, max_length=255, description="Unique destination name")
    connector_definition_id: UUID = Field(..., description="Connector definition ID")
    
    # Database/Warehouse Connection
    host: Optional[str] = Field(None, max_length=500, description="Database host")
    port: Optional[int] = Field(None, ge=1, le=65535, description="Database port")
    database_name: Optional[str] = Field(None, max_length=255, description="Database name")
    schema_name: Optional[str] = Field(None, max_length=255, description="Schema name")
    username: Optional[str] = Field(None, max_length=255, description="Username")
    password: Optional[str] = Field(None, description="Password (will be encrypted)")
    
    # Cloud Storage
    bucket_name: Optional[str] = Field(None, max_length=500, description="S3/GCS/Azure bucket name")
    region: Optional[str] = Field(None, max_length=100, description="Cloud region")
    path_prefix: Optional[str] = Field(None, max_length=1000, description="Path prefix in storage")
    
    # SSL/Security
    ssl_enabled: bool = Field(False, description="Enable SSL/TLS")
    ssl_config: Dict[str, Any] = Field(default_factory=dict, description="SSL/TLS config: ssl_mode, ssl_ca, ssl_cert, ssl_key")
    ssh_config: Optional[Dict[str, Any]] = Field(default_factory=dict, description="SSH tunnel config: tunnel_host, tunnel_port, tunnel_username, tunnel_auth_method, tunnel_password/tunnel_private_key, tunnel_passphrase")
    
    # Format Configuration (for file-based destinations)
    output_format: Optional[str] = Field(None, description="Output format: parquet, avro, json, csv")
    compression: Optional[str] = Field(None, description="Compression: gzip, snappy, lz4, zstd")
    
    # Additional Configuration
    config: Dict[str, Any] = Field(default_factory=dict, description="Additional connector-specific config")


class DestinationCreate(DestinationBase):
    """Schema for creating a new destination"""
    
    connector_version: str = Field(..., description="Connector version to use")
    status: Optional[str] = Field("draft", description="Initial status: draft, active, inactive")
    
    @field_validator("status")
    @classmethod
    def validate_status(cls, v: Optional[str]) -> str:
        """Validate status values"""
        if v and v not in ["draft", "active", "inactive"]:
            raise ValueError("Status must be one of: draft, active, inactive")
        return v or "draft"
    
    @field_validator("output_format")
    @classmethod
    def validate_output_format(cls, v: Optional[str]) -> Optional[str]:
        """Validate output format values"""
        if v and v not in ["parquet", "avro", "json", "csv", "orc"]:
            raise ValueError("Output format must be one of: parquet, avro, json, csv, orc")
        return v
    
    @field_validator("compression")
    @classmethod
    def validate_compression(cls, v: Optional[str]) -> Optional[str]:
        """Validate compression values"""
        if v and v not in ["gzip", "snappy", "lz4", "zstd", "bzip2", "none"]:
            raise ValueError("Compression must be one of: gzip, snappy, lz4, zstd, bzip2, none")
        return v


class DestinationUpdate(BaseModel):
    """Schema for updating a destination"""
    
    destination_name: Optional[str] = Field(None, min_length=1, max_length=255)
    host: Optional[str] = Field(None, max_length=500)
    port: Optional[int] = Field(None, ge=1, le=65535)
    database_name: Optional[str] = Field(None, max_length=255)
    schema_name: Optional[str] = Field(None, max_length=255)
    username: Optional[str] = Field(None, max_length=255)
    password: Optional[str] = None
    bucket_name: Optional[str] = Field(None, max_length=500)
    region: Optional[str] = Field(None, max_length=100)
    path_prefix: Optional[str] = Field(None, max_length=1000)
    ssl_enabled: Optional[bool] = None
    ssl_config: Optional[Dict[str, Any]] = None
    ssh_config: Optional[Dict[str, Any]] = None
    output_format: Optional[str] = None
    compression: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    status: Optional[str] = None
    connector_version: Optional[str] = None
    
    @field_validator("status")
    @classmethod
    def validate_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate status values"""
        if v and v not in ["draft", "active", "inactive"]:
            raise ValueError("Status must be one of: draft, active, inactive")
        return v


class DestinationResponse(DestinationBase):
    """Schema for destination response"""
    
    destination_id: UUID
    connector_version: str
    status: str
    password: Optional[str] = Field("********", description="Password is always masked")
    
    # Full connection config (contains all stored fields including Spark/Iceberg config)
    connection_config: Optional[Dict[str, Any]] = None
    
    # Connection Test Info
    connection_test_status: Optional[str] = None
    connection_test_error: Optional[str] = None
    connection_test_at: Optional[datetime] = None
    
    # Tenant Information
    sub_tenant_id: Optional[UUID] = None
    bank_id: Optional[UUID] = None
    
    # Timestamps
    created_at: datetime
    updated_at: datetime
    is_deleted: bool = False
    deleted_at: Optional[datetime] = None
    
    # Related Objects
    connector_definition: Optional[Any] = None
    
    @field_validator('connector_definition', mode='before')
    @classmethod
    def convert_connector_def(cls, v):
        if v is None:
            return None
        if isinstance(v, dict):
            return v
        # Convert ORM object to dict
        return {
            "connector_id": str(v.connector_id) if hasattr(v, 'connector_id') else None,
            "connector_name": v.connector_name if hasattr(v, 'connector_name') else None,
            "connector_type": v.connector_type if hasattr(v, 'connector_type') else None,
            "category": v.category if hasattr(v, 'category') else None,
        }
    
    class Config:
        from_attributes = True


class DestinationListResponse(BaseModel):
    """Schema for paginated destination list"""
    
    destinations: List[DestinationResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class DestinationSearchFilters(BaseModel):
    """Schema for destination search filters"""
    
    status: Optional[str] = Field(None, description="Filter by status: draft, active, inactive")
    connector_type: Optional[str] = Field(None, description="Filter by connector type")
    connector_definition_id: Optional[UUID] = Field(None, description="Filter by specific connector")
    search: Optional[str] = Field(None, description="Search in destination name")
    page: int = Field(1, ge=1, description="Page number")
    page_size: int = Field(50, ge=1, le=100, description="Items per page")


# ===========================
# Connection Testing Schemas
# ===========================

class ConnectionTestRequest(BaseModel):
    """Schema for testing destination connection with optional overrides"""
    
    host: Optional[str] = None
    port: Optional[int] = Field(None, ge=1, le=65535)
    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    bucket_name: Optional[str] = None
    region: Optional[str] = None
    ssl_enabled: Optional[bool] = None
    ssl_config: Optional[Dict[str, Any]] = None


class ConnectionTestResponse(BaseModel):
    """Schema for connection test result"""
    
    status: str = Field(..., description="success or failed")
    message: str
    error_details: Optional[str] = None
    connection_test_at: datetime
    latency_ms: Optional[int] = None


# ===========================
# Write Mode Configuration Schemas
# ===========================

class WriteModeConfig(BaseModel):
    """Schema for configuring write behavior"""
    
    write_mode: str = Field(..., description="append, replace, upsert, merge")
    
    # Upsert/Merge Configuration
    primary_keys: Optional[List[str]] = Field(None, description="Primary key columns for upsert")
    update_columns: Optional[List[str]] = Field(None, description="Columns to update in upsert")
    
    # Partitioning
    partition_by: Optional[List[str]] = Field(None, description="Partition columns")
    partition_type: Optional[str] = Field(None, description="daily, monthly, yearly, custom")
    
    # Table Options
    create_table_if_not_exists: bool = Field(True, description="Auto-create destination table")
    drop_table_before_load: bool = Field(False, description="Drop table before loading (replace mode)")
    truncate_before_load: bool = Field(False, description="Truncate before loading (replace mode)")
    
    @field_validator("write_mode")
    @classmethod
    def validate_write_mode(cls, v: str) -> str:
        """Validate write mode values"""
        if v not in ["append", "replace", "upsert", "merge"]:
            raise ValueError("Write mode must be one of: append, replace, upsert, merge")
        return v
    
    @field_validator("partition_type")
    @classmethod
    def validate_partition_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate partition type values"""
        if v and v not in ["daily", "monthly", "yearly", "custom"]:
            raise ValueError("Partition type must be one of: daily, monthly, yearly, custom")
        return v


class WriteModeConfigResponse(BaseModel):
    """Schema for write mode configuration response"""
    
    destination_id: UUID
    write_mode_config: WriteModeConfig
    updated_at: datetime


# ===========================
# Schema Mapping Schemas
# ===========================

class ColumnMapping(BaseModel):
    """Schema for individual column mapping"""
    
    source_column: str = Field(..., description="Source column name")
    destination_column: str = Field(..., description="Destination column name")
    data_type: Optional[str] = Field(None, description="Target data type")
    transform: Optional[str] = Field(None, description="Transformation expression")
    nullable: bool = Field(True, description="Allow null values")
    default_value: Optional[Any] = Field(None, description="Default value if null")


class SchemaMappingRequest(BaseModel):
    """Schema for configuring destination schema mapping"""
    
    table_name: str = Field(..., description="Destination table name")
    column_mappings: List[ColumnMapping] = Field(..., description="Column mapping definitions")
    
    # Advanced Options
    enable_auto_mapping: bool = Field(True, description="Auto-map columns with same names")
    case_sensitive: bool = Field(False, description="Case-sensitive column matching")
    drop_unmapped_columns: bool = Field(False, description="Drop source columns not in mapping")


class SchemaMappingResponse(BaseModel):
    """Schema for schema mapping response"""
    
    destination_id: UUID
    table_name: str
    column_mappings: List[ColumnMapping]
    total_mappings: int
    created_at: datetime
    updated_at: datetime


# ===========================
# Batch Settings Schemas
# ===========================

class BatchSettings(BaseModel):
    """Schema for configuring batch write settings"""
    
    # Batch Size Configuration
    batch_size: int = Field(1000, ge=1, le=100000, description="Records per batch")
    batch_timeout_seconds: int = Field(300, ge=1, le=3600, description="Max time per batch")
    
    # Parallelism
    max_parallel_batches: int = Field(5, ge=1, le=50, description="Max concurrent batches")
    
    # Retry Configuration
    max_retries: int = Field(3, ge=0, le=10, description="Max retry attempts")
    retry_delay_seconds: int = Field(60, ge=1, le=600, description="Delay between retries")
    
    # Error Handling
    continue_on_error: bool = Field(False, description="Continue on batch errors")
    error_threshold_percent: float = Field(5.0, ge=0.0, le=100.0, description="Max error % before stopping")
    
    # Performance
    enable_compression: bool = Field(True, description="Compress batch data")
    buffer_memory_mb: int = Field(256, ge=64, le=2048, description="Buffer memory per batch")


class BatchSettingsResponse(BaseModel):
    """Schema for batch settings response"""
    
    destination_id: UUID
    batch_settings: BatchSettings
    updated_at: datetime


# ===========================
# Destination Statistics Schemas
# ===========================

class DestinationStats(BaseModel):
    """Schema for destination usage statistics"""
    
    destination_id: UUID
    destination_name: str
    
    # Connection Statistics
    total_connections: int = Field(0, description="Total connections using this destination")
    active_connections: int = Field(0, description="Currently active connections")
    
    # Sync Statistics
    total_syncs: int = Field(0, description="Total sync operations")
    successful_syncs: int = Field(0, description="Successful syncs")
    failed_syncs: int = Field(0, description="Failed syncs")
    last_sync_at: Optional[datetime] = None
    
    # Data Volume
    total_rows_written: int = Field(0, description="Total rows written")
    total_bytes_written: int = Field(0, description="Total bytes written")
    avg_write_latency_ms: Optional[float] = None
    
    # Performance Metrics
    avg_batch_size: Optional[float] = None
    avg_throughput_rows_per_sec: Optional[float] = None
