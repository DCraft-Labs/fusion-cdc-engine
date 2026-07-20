"""
System Configuration and Management Models
System config, feature flags, alerts, and maintenance windows
"""
from sqlalchemy import Column, String, Integer, Boolean, Text, ForeignKey, text, DateTime, Numeric
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from app.models.base import BaseModel, TimestampMixin


class SystemConfig(BaseModel, TimestampMixin):
    """System-wide configuration"""
    
    __tablename__ = "system_config"
    
    # Primary Key
    config_key = Column(String(255), primary_key=True)
    
    # Value
    config_value = Column(Text, nullable=False)
    value_type = Column(String(50), nullable=False, server_default=text("'string'::character varying"))  # string, integer, boolean, json
    
    # Metadata
    description = Column(Text, nullable=True)
    category = Column(String(100), nullable=True)
    is_sensitive = Column(Boolean, nullable=False, server_default=text("false"))
    
    # Validation
    validation_rule = Column(String(500), nullable=True)
    
    # Audit
    updated_by = Column(UUID(as_uuid=True), nullable=True)
    
    def __repr__(self) -> str:
        return f"<SystemConfig(key={self.config_key}, value={self.config_value})>"


class FeatureFlag(BaseModel, TimestampMixin):
    """Feature flag management"""
    
    __tablename__ = "feature_flags"
    
    # Primary Key
    flag_name = Column(String(255), primary_key=True)
    
    # Flag Configuration
    is_enabled = Column(Boolean, nullable=False, server_default=text("false"))
    description = Column(Text, nullable=True)
    
    # Rollout Strategy
    rollout_percentage = Column(Integer, nullable=False, server_default=text("100"))
    target_tenants = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    
    # Metadata
    category = Column(String(100), nullable=True)
    dependencies = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    
    # Audit
    updated_by = Column(UUID(as_uuid=True), nullable=True)
    
    def __repr__(self) -> str:
        return f"<FeatureFlag(name={self.flag_name}, enabled={self.is_enabled})>"


class Alert(BaseModel):
    """System and connection alerts"""
    
    __tablename__ = "alerts"
    
    # Primary Key
    alert_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Scope
    connection_id = Column(UUID(as_uuid=True), nullable=True)
    sub_tenant_id = Column(UUID(as_uuid=True), nullable=True)
    bank_id = Column(UUID(as_uuid=True), nullable=True)
    
    # Alert Information
    alert_type = Column(String(100), nullable=False)
    severity = Column(String(50), nullable=False, index=True)
    title = Column(String(500), nullable=False)
    message = Column(Text, nullable=False)
    alert_context = Column(JSONB, nullable=True)
    
    # Timing
    triggered_at = Column(DateTime(timezone=True), server_default=text("now()"), index=True)
    
    # Acknowledgment
    acknowledged = Column(Boolean, server_default=text("false"))
    acknowledged_at = Column(DateTime(timezone=True), nullable=True)
    acknowledged_by = Column(UUID(as_uuid=True), nullable=True)
    
    # Resolution
    resolved = Column(Boolean, server_default=text("false"))
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    resolution_notes = Column(Text, nullable=True)
    
    # Webhook
    webhook_sent = Column(Boolean, server_default=text("false"))
    webhook_sent_at = Column(DateTime(timezone=True), nullable=True)
    webhook_response_status = Column(Integer, nullable=True)
    
    @property
    def status(self) -> str:
        """Computed status based on acknowledged and resolved flags"""
        if self.resolved:
            return "resolved"
        elif self.acknowledged:
            return "acknowledged"
        else:
            return "active"
    
    def __repr__(self) -> str:
        return f"<Alert(type={self.alert_type}, severity={self.severity}, status={self.status})>"


class MaintenanceWindow(BaseModel, TimestampMixin):
    """Scheduled maintenance windows"""
    
    __tablename__ = "maintenance_windows"
    
    # Primary Key
    window_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Window Information
    window_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    
    # Schedule
    start_time = Column(DateTime(timezone=True), nullable=False, index=True)
    end_time = Column(DateTime(timezone=True), nullable=False, index=True)
    recurrence_rule = Column(String(500), nullable=True)  # iCal RRULE format
    
    # Scope
    affects_all = Column(Boolean, nullable=False, server_default=text("false"))
    affected_connections = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    affected_tenants = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    
    # Actions
    pause_syncs = Column(Boolean, nullable=False, server_default=text("true"))
    send_notifications = Column(Boolean, nullable=False, server_default=text("true"))
    
    # Status
    status = Column(String(50), nullable=False, server_default=text("'scheduled'::character varying"))  # scheduled, active, completed, cancelled
    
    # Audit
    created_by = Column(UUID(as_uuid=True), nullable=True)
    
    def __repr__(self) -> str:
        return f"<MaintenanceWindow(name={self.window_name}, start={self.start_time}, status={self.status})>"


class EventDeadLetterQueue(BaseModel, TimestampMixin):
    """Dead letter queue for failed events"""
    
    __tablename__ = "event_dead_letter_queue"
    
    # Primary Key
    dlq_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Event Information
    event_id = Column(String(255), nullable=False, index=True)
    event_type = Column(String(100), nullable=False)
    event_data = Column(JSONB, nullable=False)
    
    # Source
    source_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    stream_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    connection_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    
    # Failure Information
    failed_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True)
    failure_reason = Column(Text, nullable=False)
    error_stack_trace = Column(Text, nullable=True)
    
    # Retry Information
    retry_count = Column(Integer, nullable=False, server_default=text("0"))
    last_retry_at = Column(DateTime(timezone=True), nullable=True)
    next_retry_at = Column(DateTime(timezone=True), nullable=True)
    
    # Status
    status = Column(String(50), nullable=False, server_default=text("'failed'::character varying"))  # failed, retrying, resolved, discarded
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    
    # Tenant
    bank_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    sub_tenant_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    
    # Relationships
    retry_history = relationship(
        "EventDLQRetryHistory",
        back_populates="dlq_event",
        cascade="all, delete-orphan",
    )
    
    def __repr__(self) -> str:
        return f"<EventDeadLetterQueue(event_id={self.event_id}, status={self.status}, retries={self.retry_count})>"


class EventDLQRetryHistory(BaseModel, TimestampMixin):
    """Retry history for DLQ events"""
    
    __tablename__ = "event_dlq_retry_history"
    
    # Primary Key
    retry_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Foreign Key
    dlq_id = Column(
        UUID(as_uuid=True),
        ForeignKey("event_dead_letter_queue.dlq_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    
    # Retry Information
    retry_number = Column(Integer, nullable=False)
    attempted_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    
    # Result
    success = Column(Boolean, nullable=False)
    error_message = Column(Text, nullable=True)
    
    # Relationships
    dlq_event = relationship("EventDeadLetterQueue", back_populates="retry_history")
    
    def __repr__(self) -> str:
        return f"<EventDLQRetryHistory(dlq={self.dlq_id}, attempt={self.retry_number}, success={self.success})>"


class ResourceUsage(BaseModel, TimestampMixin):
    """Resource usage tracking per tenant"""
    
    __tablename__ = "resource_usage"
    
    # Primary Key
    usage_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Tenant
    bank_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    sub_tenant_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    
    # Resource Type
    resource_type = Column(String(50), nullable=False)  # cpu, memory, storage, network
    
    # Usage Metrics
    usage_amount = Column(Numeric, nullable=False)
    usage_unit = Column(String(50), nullable=False)  # cores, MB, GB, etc.
    
    # Time Window
    recorded_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True)
    window_start = Column(DateTime(timezone=True), nullable=False)
    window_end = Column(DateTime(timezone=True), nullable=False)
    
    # Metadata
    resource_metadata = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    def __repr__(self) -> str:
        return f"<ResourceUsage(tenant={self.sub_tenant_id}, type={self.resource_type}, amount={self.usage_amount})>"


class ResourceQuotaViolation(BaseModel, TimestampMixin):
    """Track resource quota violations"""
    
    __tablename__ = "resource_quota_violations"
    
    # Primary Key
    violation_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Tenant
    bank_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    sub_tenant_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    
    # Violation Information
    resource_type = Column(String(50), nullable=False)
    quota_limit = Column(Numeric, nullable=False)
    actual_usage = Column(Numeric, nullable=False)
    
    # Detection
    detected_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True)
    
    # Action Taken
    action_taken = Column(String(50), nullable=False)  # throttle, alert, block
    action_details = Column(Text, nullable=True)
    
    # Status
    status = Column(String(50), nullable=False, server_default=text("'active'::character varying"))
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    
    def __repr__(self) -> str:
        return f"<ResourceQuotaViolation(tenant={self.sub_tenant_id}, type={self.resource_type}, status={self.status})>"


class TenantDailyUsage(BaseModel, TimestampMixin):
    """Daily usage summary per tenant"""
    
    __tablename__ = "tenant_daily_usage"
    
    # Primary Key
    usage_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Tenant
    bank_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    sub_tenant_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    
    # Date
    usage_date = Column(DateTime(timezone=True), nullable=False, index=True)
    
    # Sync Metrics
    total_syncs = Column(Integer, nullable=False, server_default=text("0"))
    successful_syncs = Column(Integer, nullable=False, server_default=text("0"))
    failed_syncs = Column(Integer, nullable=False, server_default=text("0"))
    
    # Data Metrics
    records_synced = Column(Numeric, nullable=False, server_default=text("0"))
    bytes_synced = Column(Numeric, nullable=False, server_default=text("0"))
    
    # Resource Metrics
    cpu_hours = Column(Numeric, nullable=False, server_default=text("0"))
    memory_gb_hours = Column(Numeric, nullable=False, server_default=text("0"))
    
    # Active Resources
    active_connections = Column(Integer, nullable=False, server_default=text("0"))
    active_workers = Column(Integer, nullable=False, server_default=text("0"))
    
    def __repr__(self) -> str:
        return f"<TenantDailyUsage(tenant={self.sub_tenant_id}, date={self.usage_date}, syncs={self.total_syncs})>"
