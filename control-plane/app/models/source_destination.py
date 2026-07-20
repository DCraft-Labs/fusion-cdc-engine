"""
Source Database Models
Represents source database configurations for CDC
"""
from typing import Optional
from sqlalchemy import Column, String, Integer, Boolean, Text, ForeignKey, UniqueConstraint, text, DateTime
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from app.models.base import BaseModel, TimestampMixin, SoftDeleteMixin, MultiTenancyMixin


class Source(BaseModel, TimestampMixin, SoftDeleteMixin, MultiTenancyMixin):
    """Source database configuration"""
    
    __tablename__ = "sources"
    
    # Primary Key
    source_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Basic Information
    source_name = Column(String(255), nullable=False)
    
    # Connector Information
    connector_definition_id = Column(
        UUID(as_uuid=True),
        ForeignKey("connector_definitions.connector_id"),
        nullable=False,
        index=True,
    )
    connector_version = Column(String(50), nullable=False)
    
    # Connection Details
    host = Column(String(500), nullable=False)
    port = Column(Integer, nullable=False)
    database_name = Column(String(255), nullable=False)
    username = Column(String(255), nullable=False)
    password_encrypted = Column(Text, nullable=False)
    
    # SSL/TLS Configuration
    ssl_enabled = Column(Boolean, nullable=False, server_default=text("false"))
    ssl_config = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    # SSH Tunnel Configuration (separate from SSL)
    ssh_config = Column(JSONB, nullable=True, server_default=text("'{}'::jsonb"))
    
    # Additional Configuration
    config = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    # Discovery Cache
    discovery_cache = Column(JSONB, nullable=True)
    last_discovery_at = Column(DateTime(timezone=True), nullable=True)
    
    # Status
    status = Column(
        String(50),
        nullable=False,
        server_default=text("'draft'::character varying"),
    )
    connection_test_status = Column(String(50), nullable=True)
    connection_test_error = Column(Text, nullable=True)
    connection_test_at = Column(DateTime(timezone=True), nullable=True)
    
    # Relationships
    connector_definition = relationship("ConnectorDefinition", back_populates="sources")
    connections = relationship(
        "Connection",
        back_populates="source",
        cascade="all, delete-orphan",
    )
    
    __table_args__ = (
        UniqueConstraint("sub_tenant_id", "source_name", name="uq_source_name"),
    )
    
    @property
    def password(self) -> str:
        """Return masked password for serialization"""
        return "********"
    
    def __repr__(self) -> str:
        return f"<Source(name={self.source_name}, host={self.host}, status={self.status})>"


class Destination(BaseModel, TimestampMixin, SoftDeleteMixin, MultiTenancyMixin):
    """Destination database/warehouse configuration"""
    
    __tablename__ = "destinations"
    
    # Primary Key
    destination_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Basic Information
    destination_name = Column(String(255), nullable=False)
    
    # Connector Information
    connector_definition_id = Column(
        UUID(as_uuid=True),
        ForeignKey("connector_definitions.connector_id"),
        nullable=False,
        index=True,
    )
    connector_version = Column(String(50), nullable=False)
    
    # Connection Configuration (JSONB containing host, port, credentials, etc.)
    connection_config = Column(JSONB, nullable=False)
    
    # Status
    status = Column(
        String(50),
        nullable=False,
        server_default=text("'draft'::character varying"),
    )
    connection_test_status = Column(String(50), nullable=True)
    connection_test_error = Column(Text, nullable=True)
    connection_test_at = Column(DateTime(timezone=True), nullable=True)
    
    # Relationships
    connector_definition = relationship("ConnectorDefinition", back_populates="destinations")
    connections = relationship(
        "Connection",
        back_populates="destination",
        cascade="all, delete-orphan",
    )
    
    __table_args__ = (
        UniqueConstraint("sub_tenant_id", "destination_name", name="uq_destination_name"),
    )
    
    @property
    def host(self) -> Optional[str]:
        return (self.connection_config or {}).get("host")
    
    @property
    def port(self) -> Optional[int]:
        return (self.connection_config or {}).get("port")
    
    @property
    def database_name(self) -> Optional[str]:
        return (self.connection_config or {}).get("database_name")
    
    @property
    def schema_name(self) -> Optional[str]:
        return (self.connection_config or {}).get("schema_name")
    
    @property
    def username(self) -> Optional[str]:
        return (self.connection_config or {}).get("username")
    
    @property
    def password(self) -> Optional[str]:
        return "********" if (self.connection_config or {}).get("password_encrypted") else None
    
    @property
    def ssl_enabled(self) -> bool:
        return (self.connection_config or {}).get("ssl_enabled", False)

    @property
    def ssl_config(self) -> dict:
        return (self.connection_config or {}).get("ssl_config") or {}

    @property
    def ssh_config(self) -> dict:
        return (self.connection_config or {}).get("ssh_config") or {}
    
    def __repr__(self) -> str:
        return f"<Destination(name={self.destination_name}, status={self.status})>"
