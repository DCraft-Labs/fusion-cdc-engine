"""
Connection and Stream Models
Represents connections between sources and destinations, and stream configurations
"""
from sqlalchemy import Column, String, Integer, Boolean, Text, ForeignKey, UniqueConstraint, text, DateTime, Numeric
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from app.models.base import BaseModel, TimestampMixin, SoftDeleteMixin, MultiTenancyMixin


class Connection(BaseModel, TimestampMixin, SoftDeleteMixin, MultiTenancyMixin):
    """Connection between source and destination"""
    
    __tablename__ = "connections"
    
    # Primary Key
    connection_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Basic Information
    connection_name = Column(String(255), nullable=False)
    
    # Source and Destination
    source_id = Column(
        UUID(as_uuid=True),
        ForeignKey("sources.source_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    destination_id = Column(
        UUID(as_uuid=True),
        ForeignKey("destinations.destination_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    
    # Sync Configuration
    sync_mode = Column(String(50), nullable=False)  # full_refresh, incremental, cdc
    sync_type = Column(String(50), nullable=False)  # REALTIME (CDC), SCHEDULED (BATCH)
    schedule_cron = Column(String(100), nullable=True)  # cron expression
    
    # Pipeline References
    transform_pipeline_id = Column(UUID(as_uuid=True), nullable=True)
    dq_policy_id = Column(UUID(as_uuid=True), nullable=True)
    schema_evolution_policy = Column(String(50), server_default=text("'MANUAL_APPROVAL'::character varying"))
    
    # Status and Control
    status = Column(String(50), server_default=text("'draft'::character varying"))
    paused_at = Column(DateTime(timezone=True), nullable=True)
    paused_by = Column(UUID(as_uuid=True), nullable=True)
    pause_reason = Column(Text, nullable=True)
    resumed_at = Column(DateTime(timezone=True), nullable=True)
    resumed_by = Column(UUID(as_uuid=True), nullable=True)
    
    # Resource Limits
    resource_limits = Column(JSONB, server_default=text("'{}'::jsonb"))
    
    # Initial Load Tracking
    initial_load_completed = Column(Boolean, server_default=text("false"))
    initial_load_started_at = Column(DateTime(timezone=True), nullable=True)
    initial_load_completed_at = Column(DateTime(timezone=True), nullable=True)
    
    # Relationships
    source = relationship("Source", back_populates="connections")
    destination = relationship("Destination", back_populates="connections")
    streams = relationship(
        "Stream",
        back_populates="connection",
        cascade="all, delete-orphan",
    )
    connection_runs = relationship(
        "ConnectionRun",
        back_populates="connection",
        cascade="all, delete-orphan",
    )
    health_checks = relationship(
        "ConnectionHealthCheck",
        back_populates="connection",
        cascade="all, delete-orphan",
    )
    alert_webhooks = relationship(
        "ConnectionAlertWebhook",
        back_populates="connection",
        cascade="all, delete-orphan",
    )
    
    __table_args__ = (
        UniqueConstraint("sub_tenant_id", "connection_name", name="uq_connection_name"),
    )
    
    def __repr__(self) -> str:
        return f"<Connection(name={self.connection_name}, sync_mode={self.sync_mode}, status={self.status})>"


class Stream(BaseModel, TimestampMixin):
    """Stream configuration for individual tables/collections"""
    
    __tablename__ = "streams"
    
    # Primary Key
    stream_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Foreign Key
    connection_id = Column(
        UUID(as_uuid=True),
        ForeignKey("connections.connection_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    
    # Stream Identification (matches actual DB column names)
    stream_name = Column(String(255), nullable=False)
    stream_namespace = Column(String(255), nullable=True)
    source_table_name = Column(String(255), nullable=False)
    source_schema_name = Column(String(255), nullable=True)
    destination_table_name = Column(String(255), nullable=False)
    destination_schema_name = Column(String(255), nullable=True)
    
    # Sync Configuration
    is_enabled = Column(Boolean, server_default=text("true"), nullable=False)
    sync_mode = Column(String(50), nullable=True)
    cursor_field = Column(String(255), nullable=True)
    primary_keys = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    
    # Schema / Mapping
    source_schema = Column(JSONB, nullable=True)
    destination_schema = Column(JSONB, nullable=True)
    selected_columns = Column(JSONB, nullable=True)
    column_mapping = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    # Per-stream transform pipeline spec from the UI wizard.
    # Structure: {"transforms": [{"id", "type", "column", ...}, ...]}
    transform_overrides = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    # Relationships
    connection = relationship("Connection", back_populates="streams")
    
    __table_args__ = (
        UniqueConstraint("connection_id", "source_schema_name", "source_table_name", name="uq_stream"),
    )
    
    def __repr__(self) -> str:
        return f"<Stream(table={self.source_table_name}, schema={self.source_schema_name}, enabled={self.is_enabled})>"


class SyncModeConfig(BaseModel, TimestampMixin):
    """Sync mode configuration and history"""
    
    __tablename__ = "sync_mode_config"
    
    # Primary Key
    config_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Foreign Key
    stream_id = Column(
        UUID(as_uuid=True),
        ForeignKey("streams.stream_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    
    # Sync Mode
    sync_mode = Column(String(50), nullable=False)
    effective_from = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    effective_to = Column(DateTime(timezone=True), nullable=True)
    
    # Configuration
    config = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    # Audit
    changed_by = Column(UUID(as_uuid=True), nullable=True)
    change_reason = Column(Text, nullable=True)
    
    def __repr__(self) -> str:
        return f"<SyncModeConfig(stream_id={self.stream_id}, mode={self.sync_mode})>"
