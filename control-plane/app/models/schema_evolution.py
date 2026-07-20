"""
Schema Evolution Models
Track schema changes, JSON schema evolution, and flatten rules
"""
from sqlalchemy import Column, String, Integer, Boolean, Text, ForeignKey, text, DateTime
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from app.models.base import BaseModel, TimestampMixin


class SchemaChangeEvent(BaseModel, TimestampMixin):
    """Schema change detection and tracking"""
    
    __tablename__ = "schema_change_events"
    
    # Primary Key
    event_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Scope
    source_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    stream_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    
    # Change Information
    table_name = Column(String(255), nullable=False)
    schema_name = Column(String(255), nullable=True)
    change_type = Column(String(50), nullable=False)  # column_added, column_removed, type_changed, etc.
    
    # Schema Details
    old_schema = Column(JSONB, nullable=True)
    new_schema = Column(JSONB, nullable=False)
    schema_diff = Column(JSONB, nullable=False)
    
    # Detection
    detected_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True)
    detected_by = Column(String(100), nullable=False)  # worker_id or system
    
    # Approval Workflow
    status = Column(String(50), nullable=False, server_default=text("'pending'::character varying"))  # pending, approved, rejected, auto_approved
    reviewed_at = Column(DateTime(timezone=True), nullable=True)
    reviewed_by = Column(UUID(as_uuid=True), nullable=True)
    review_notes = Column(Text, nullable=True)
    
    # Impact Assessment
    is_breaking = Column(Boolean, nullable=False)
    impact_assessment = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    # Application
    applied_at = Column(DateTime(timezone=True), nullable=True)
    
    def __repr__(self) -> str:
        return f"<SchemaChangeEvent(table={self.table_name}, type={self.change_type}, status={self.status})>"


class JSONSchemaCache(BaseModel, TimestampMixin):
    """Cached JSON schemas for nested data"""
    
    __tablename__ = "json_schema_cache"
    
    # Primary Key
    cache_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Identification
    source_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    table_name = Column(String(255), nullable=False)
    column_name = Column(String(255), nullable=False)
    
    # Schema
    json_schema = Column(JSONB, nullable=False)
    sample_count = Column(Integer, nullable=False)
    
    # Statistics
    first_seen_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    last_seen_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    occurrence_count = Column(Integer, nullable=False, server_default=text("1"))
    
    def __repr__(self) -> str:
        return f"<JSONSchemaCache(table={self.table_name}, column={self.column_name})>"


class JSONSchemaEvolution(BaseModel, TimestampMixin):
    """Track JSON schema changes over time"""
    
    __tablename__ = "json_schema_evolution"
    
    # Primary Key
    evolution_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Identification
    source_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    table_name = Column(String(255), nullable=False)
    column_name = Column(String(255), nullable=False)
    
    # Schema Change
    old_schema = Column(JSONB, nullable=True)
    new_schema = Column(JSONB, nullable=False)
    schema_diff = Column(JSONB, nullable=False)
    
    # Detection
    detected_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True)
    
    # Auto-handling
    auto_applied = Column(Boolean, nullable=False, server_default=text("false"))
    application_result = Column(Text, nullable=True)
    
    def __repr__(self) -> str:
        return f"<JSONSchemaEvolution(table={self.table_name}, column={self.column_name})>"


class JSONFlattenRule(BaseModel, TimestampMixin):
    """Rules for flattening JSON columns"""
    
    __tablename__ = "json_flatten_rules"
    
    # Primary Key
    rule_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Scope
    stream_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    source_column = Column(String(255), nullable=False)
    
    # Flatten Configuration
    flatten_strategy = Column(String(50), nullable=False)  # full, selective, prefix
    target_columns = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    json_path_expressions = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    # Naming
    column_prefix = Column(String(100), nullable=True)
    separator = Column(String(10), nullable=False, server_default=text("'_'::character varying"))
    
    # Status
    is_active = Column(Boolean, nullable=False, server_default=text("true"))
    
    def __repr__(self) -> str:
        return f"<JSONFlattenRule(stream={self.stream_id}, column={self.source_column})>"
