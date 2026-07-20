"""
Pydantic schemas for Connector Definitions
Request/response models for connector management
"""
from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any
from uuid import UUID
from datetime import datetime


# ============================================================================
# Connector Definition Schemas
# ============================================================================

class ConnectorDefinitionBase(BaseModel):
    """Base connector definition schema"""
    connector_name: str = Field(..., min_length=3, max_length=255)
    connector_type: str = Field(..., max_length=50, description="Type: mysql, postgres, kafka, s3, etc.")
    category: str = Field(..., description="Category: source or destination")
    latest_version: str = Field(..., max_length=50)
    default_config: Dict[str, Any] = Field(default_factory=dict)
    required_fields: List[str] = Field(default_factory=list)
    optional_fields: List[str] = Field(default_factory=list)
    default_resource_limits: Dict[str, Any] = Field(default_factory=dict)
    supports_cdc: bool = Field(default=False)
    supports_full_refresh: bool = Field(default=True)
    supports_incremental: bool = Field(default=False)
    documentation_url: Optional[str] = Field(None, max_length=1000)
    icon_url: Optional[str] = Field(None, max_length=1000)
    
    @validator('category')
    def validate_category(cls, v):
        """Validate category is source or destination"""
        if v not in ['source', 'destination']:
            raise ValueError('Category must be either "source" or "destination"')
        return v


class ConnectorDefinitionCreate(ConnectorDefinitionBase):
    """Create connector definition request"""
    pass


class ConnectorDefinitionUpdate(BaseModel):
    """Update connector definition request"""
    connector_name: Optional[str] = Field(None, min_length=3, max_length=255)
    latest_version: Optional[str] = Field(None, max_length=50)
    default_config: Optional[Dict[str, Any]] = None
    required_fields: Optional[List[str]] = None
    optional_fields: Optional[List[str]] = None
    default_resource_limits: Optional[Dict[str, Any]] = None
    supports_cdc: Optional[bool] = None
    supports_full_refresh: Optional[bool] = None
    supports_incremental: Optional[bool] = None
    documentation_url: Optional[str] = Field(None, max_length=1000)
    icon_url: Optional[str] = Field(None, max_length=1000)
    is_active: Optional[bool] = None


class ConnectorDefinitionResponse(ConnectorDefinitionBase):
    """Connector definition response"""
    connector_id: UUID
    is_active: bool
    created_at: datetime
    updated_at: datetime
    created_by: Optional[UUID]
    
    class Config:
        from_attributes = True


class ConnectorDefinitionListResponse(BaseModel):
    """List of connector definitions"""
    connectors: List[ConnectorDefinitionResponse]
    total: int
    page: int
    page_size: int


# ============================================================================
# Connector Version Schemas
# ============================================================================

class ConnectorVersionBase(BaseModel):
    """Base connector version schema"""
    version: str = Field(..., max_length=50)
    release_notes: Optional[str] = None
    breaking_changes: List[str] = Field(default_factory=list)
    new_features: List[str] = Field(default_factory=list)
    bug_fixes: List[str] = Field(default_factory=list)
    docker_image: Optional[str] = Field(None, max_length=500)
    docker_tag: Optional[str] = Field(None, max_length=100)
    is_stable: bool = Field(default=False)
    released_at: datetime


class ConnectorVersionCreate(ConnectorVersionBase):
    """Create connector version request"""
    pass


class ConnectorVersionUpdate(BaseModel):
    """Update connector version request"""
    release_notes: Optional[str] = None
    is_stable: Optional[bool] = None
    deprecated_at: Optional[datetime] = None


class ConnectorVersionResponse(ConnectorVersionBase):
    """Connector version response"""
    version_id: UUID
    connector_id: UUID
    deprecated_at: Optional[datetime] = None
    created_at: datetime
    
    class Config:
        from_attributes = True


class ConnectorVersionListResponse(BaseModel):
    """List of connector versions"""
    versions: List[ConnectorVersionResponse]
    total: int


# ============================================================================
# Connector Capability Schemas
# ============================================================================

class ConnectorCapabilities(BaseModel):
    """Connector capabilities"""
    supports_cdc: bool
    supports_full_refresh: bool
    supports_incremental: bool
    sync_modes: List[str] = Field(default_factory=list)

    def model_post_init(self, __context) -> None:
        if not self.sync_modes:
            modes = []
            if self.supports_full_refresh:
                modes.append("full_refresh")
            if self.supports_cdc:
                modes.append("cdc")
            if self.supports_incremental:
                modes.append("incremental")
            self.sync_modes = modes


class ConnectorConfigSchema(BaseModel):
    """Connector configuration schema"""
    required_fields: List[str]
    optional_fields: List[str]
    default_config: Dict[str, Any]
    field_descriptions: Dict[str, str] = Field(default_factory=dict)
    field_validations: Dict[str, Any] = Field(default_factory=dict)


class ConnectorSearchFilters(BaseModel):
    """Search filters for connectors"""
    category: Optional[str] = Field(None, description="Filter by category: source or destination")
    connector_type: Optional[str] = Field(None, description="Filter by connector type")
    supports_cdc: Optional[bool] = Field(None, description="Filter by CDC support")
    is_active: Optional[bool] = Field(True, description="Filter by active status")
    search: Optional[str] = Field(None, description="Search in connector name")


class ConnectorStats(BaseModel):
    """Connector usage statistics"""
    connector_id: UUID
    connector_name: str
    total_sources: int = 0
    total_destinations: int = 0
    active_connections: int = 0
    total_connections: int = 0
