"""
Connector Definitions Model
Defines available source and destination connector types
"""
from sqlalchemy import Column, String, Boolean, Text, ForeignKey, UniqueConstraint, text, DateTime
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from app.models.base import BaseModel, TimestampMixin


class ConnectorDefinition(BaseModel, TimestampMixin):
    """Connector definition model for source and destination types"""
    
    __tablename__ = "connector_definitions"
    
    # Primary Key
    connector_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Basic Information
    connector_name = Column(String(255), nullable=False, unique=True)
    connector_type = Column(String(50), nullable=False, index=True)
    category = Column(String(50), nullable=False, index=True)  # 'source' or 'destination'
    latest_version = Column(String(50), nullable=False)
    
    # Configuration
    default_config = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    required_fields = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    optional_fields = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    default_resource_limits = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    # Capabilities
    supports_cdc = Column(Boolean, nullable=False, server_default=text("false"))
    supports_full_refresh = Column(Boolean, nullable=False, server_default=text("true"))
    supports_incremental = Column(Boolean, nullable=False, server_default=text("false"))
    
    # Documentation
    documentation_url = Column(Text, nullable=True)
    icon_url = Column(Text, nullable=True)
    
    # Status
    is_active = Column(Boolean, nullable=False, server_default=text("true"))
    
    # Audit
    created_by = Column(UUID(as_uuid=True), nullable=True)
    
    # Relationships
    versions = relationship(
        "ConnectorVersion",
        back_populates="connector",
        cascade="all, delete-orphan",
    )
    sources = relationship("Source", back_populates="connector_definition")
    destinations = relationship("Destination", back_populates="connector_definition")
    
    @property
    def capabilities(self):
        """Return capabilities as a dict for backward compatibility"""
        return {
            "supports_cdc": self.supports_cdc,
            "supports_full_refresh": self.supports_full_refresh,
            "supports_incremental": self.supports_incremental,
            "supported_write_modes": ["append", "upsert", "replace"],
        }
    
    def __repr__(self) -> str:
        return f"<ConnectorDefinition(name={self.connector_name}, type={self.connector_type}, category={self.category})>"


class ConnectorVersion(BaseModel):
    """Connector version history"""
    
    __tablename__ = "connector_versions"
    
    # Primary Key
    version_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Foreign Keys
    connector_id = Column(
        UUID(as_uuid=True),
        ForeignKey("connector_definitions.connector_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    
    # Version Information
    version = Column(String(50), nullable=False)
    release_notes = Column(Text, nullable=True)
    breaking_changes = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    new_features = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    bug_fixes = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    
    # Docker Image
    docker_image = Column(String(500), nullable=True)
    docker_tag = Column(String(100), nullable=True)
    
    # Status
    is_stable = Column(Boolean, nullable=False, server_default=text("false"))
    released_at = Column(DateTime(timezone=True), nullable=False)
    deprecated_at = Column(DateTime(timezone=True), nullable=True)
    
    # Audit
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    
    # Relationships
    connector = relationship("ConnectorDefinition", back_populates="versions")
    
    __table_args__ = (
        UniqueConstraint("connector_id", "version", name="uq_connector_version"),
    )
    
    def __repr__(self) -> str:
        return f"<ConnectorVersion(connector_id={self.connector_id}, version={self.version})>"
