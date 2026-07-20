"""
Alerting and Notification Models - REBUILT FROM ACTUAL DATABASE SCHEMA
Based on create_alerting_tables.sql - matches database exactly
"""
from sqlalchemy import Column, String, Integer, Boolean, Text, ForeignKey, text, DateTime, Numeric, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY
from sqlalchemy.orm import relationship
from app.models.base import BaseModel, TimestampMixin


class NotificationChannel(BaseModel, TimestampMixin):
    """Notification channels for alerts"""
    
    __tablename__ = "notification_channels"
    
    # Primary Key
    channel_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Tenant
    bank_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    sub_tenant_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    
    # Channel Information
    channel_name = Column(String(255), nullable=False)
    channel_type = Column(String(50), nullable=False, index=True)  # email, slack, webhook, pagerduty, msteams
    description = Column(Text, nullable=True)
    
    # Configuration
    config = Column(JSONB, nullable=False)  # Channel-specific config (recipients, webhook_url, etc.)
    auth_config = Column(JSONB, nullable=True)  # Authentication details if needed
    
    # Status
    is_active = Column(Boolean, nullable=False, server_default=text("true"))
    is_verified = Column(Boolean, nullable=False, server_default=text("false"))
    verified_at = Column(DateTime(timezone=True), nullable=True)
    
    # Testing
    last_test_at = Column(DateTime(timezone=True), nullable=True)
    last_test_status = Column(String(50), nullable=True)
    last_test_error = Column(Text, nullable=True)
    
    # Rate Limiting
    rate_limit_per_hour = Column(Integer, nullable=True)
    rate_limit_per_day = Column(Integer, nullable=True)
    
    # Metadata
    tags = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    
    # Audit
    created_by = Column(UUID(as_uuid=True), nullable=True)
    updated_by = Column(UUID(as_uuid=True), nullable=True)
    is_deleted = Column(Boolean, nullable=False, server_default=text("false"))
    
    # Relationships
    alert_rules = relationship(
        "AlertRule",
        secondary="alert_rule_channels",
        back_populates="notification_channels",
    )


class AlertRule(BaseModel, TimestampMixin):
    """Alert rules for monitoring and notifications"""
    
    __tablename__ = "alert_rules"
    
    # Primary Key
    rule_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Tenant
    bank_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    sub_tenant_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    
    # Rule Information
    rule_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    
    # Alert Type and Severity
    alert_type = Column(String(50), nullable=False, index=True)
    severity = Column(String(50), nullable=False, index=True)
    
    # Scope
    scope_type = Column(String(50), nullable=False)
    scope_id = Column(UUID(as_uuid=True), nullable=True)
    connection_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    source_id = Column(UUID(as_uuid=True), nullable=True)
    destination_id = Column(UUID(as_uuid=True), nullable=True)
    
    # Condition
    condition_type = Column(String(50), nullable=False)
    condition_definition = Column(JSONB, nullable=False)
    threshold_value = Column(Numeric, nullable=True)
    
    # Evaluation Settings
    consecutive_failures_required = Column(Integer, nullable=False, server_default=text("1"))
    evaluation_window_minutes = Column(Integer, nullable=False, server_default=text("5"))
    cooldown_minutes = Column(Integer, nullable=False, server_default=text("15"))
    auto_resolve = Column(Boolean, nullable=False, server_default=text("false"))
    auto_resolve_after_minutes = Column(Integer, nullable=True)
    
    # Status
    is_active = Column(Boolean, nullable=False, server_default=text("true"))
    
    # Audit
    created_by = Column(UUID(as_uuid=True), nullable=True)
    updated_by = Column(UUID(as_uuid=True), nullable=True)
    is_deleted = Column(Boolean, nullable=False, server_default=text("false"))
    
    # Relationships
    notification_channels = relationship(
        "NotificationChannel",
        secondary="alert_rule_channels",
        back_populates="alert_rules",
    )
    escalation_policies = relationship(
        "AlertEscalationPolicy",
        back_populates="alert_rule",
        cascade="all, delete-orphan",
    )
    evaluations = relationship(
        "AlertEvaluation",
        back_populates="alert_rule",
        cascade="all, delete-orphan",
    )


class AlertRuleChannel(BaseModel):
    """Many-to-many relationship between alert rules and notification channels"""
    
    __tablename__ = "alert_rule_channels"
    
    # Primary Key
    rule_channel_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Foreign Keys
    rule_id = Column(
        UUID(as_uuid=True),
        ForeignKey("alert_rules.rule_id"),
        nullable=False,
        index=True,
    )
    channel_id = Column(
        UUID(as_uuid=True),
        ForeignKey("notification_channels.channel_id"),
        nullable=False,
        index=True,
    )
    
    # Channel-specific settings
    priority = Column(Integer, nullable=False, server_default=text("1"))
    severity_filter = Column(ARRAY(String(50)), nullable=True)
    
    # Only created_at (no updated_at per SQL schema)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    
    __table_args__ = (
        UniqueConstraint("rule_id", "channel_id"),
    )


class AlertEscalationPolicy(BaseModel, TimestampMixin):
    """Escalation policies for unacknowledged alerts"""
    
    __tablename__ = "alert_escalation_policies"
    
    # Primary Key
    policy_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Foreign Key
    rule_id = Column(
        UUID(as_uuid=True),
        ForeignKey("alert_rules.rule_id"),
        nullable=False,
        index=True,
    )
    
    # Escalation Configuration
    escalation_level = Column(Integer, nullable=False)
    escalate_after_minutes = Column(Integer, nullable=False)
    channel_ids = Column(ARRAY(UUID(as_uuid=True)), nullable=False)
    message_template = Column(Text, nullable=True)
    
    # Relationship
    alert_rule = relationship("AlertRule", back_populates="escalation_policies")


class AlertEvaluation(BaseModel):
    """Alert evaluation history"""
    
    __tablename__ = "alert_evaluations"
    
    # Primary Key
    evaluation_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Foreign Key
    rule_id = Column(
        UUID(as_uuid=True),
        ForeignKey("alert_rules.rule_id"),
        nullable=False,
        index=True,
    )
    
    # Evaluation Results
    evaluated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True)
    passed = Column(Boolean, nullable=False)
    metric_value = Column(Numeric, nullable=True)
    threshold_value = Column(Numeric, nullable=True)
    consecutive_failures = Column(Integer, nullable=False, server_default=text("0"))
    evaluation_details = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    execution_time_ms = Column(Integer, nullable=True)
    
    # Only created_at (no updated_at per SQL schema)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    
    # Relationship
    alert_rule = relationship("AlertRule", back_populates="evaluations")


class AlertHistory(BaseModel):
    """History of alert status changes"""
    
    __tablename__ = "alert_history"
    
    # Primary Key
    history_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Foreign Key (no FK constraint in SQL schema)
    alert_id = Column(
        UUID(as_uuid=True),
        nullable=False,
        index=True,
    )
    
    # Change Information
    old_status = Column(String(50), nullable=True)
    new_status = Column(String(50), nullable=False)
    changed_by = Column(UUID(as_uuid=True), nullable=True)
    changed_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True)
    change_reason = Column(Text, nullable=True)
    
    # Use column name override for reserved word 'metadata'
    metadata_json = Column("metadata", JSONB, server_default=text("'{}'::jsonb"))
    
    # Only created_at (no updated_at per SQL schema)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))


class AlertNotificationLog(BaseModel):
    """Log of all alert notifications sent"""
    
    __tablename__ = "alert_notification_logs"
    
    # Primary Key
    log_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Foreign Keys (no FK constraints in SQL schema for alert_id)
    alert_id = Column(
        UUID(as_uuid=True),
        nullable=False,
        index=True,
    )
    channel_id = Column(
        UUID(as_uuid=True),
        ForeignKey("notification_channels.channel_id"),
        nullable=False,
        index=True,
    )
    
    # Notification Details
    sent_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True)
    status = Column(String(50), nullable=False)
    retry_count = Column(Integer, nullable=False, server_default=text("0"))
    delivery_time_ms = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
    response_data = Column(JSONB, nullable=True)
    
    # Only created_at (no updated_at per SQL schema)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))


class AlertSuppression(BaseModel, TimestampMixin):
    """Temporary suppression of alerts"""
    
    __tablename__ = "alert_suppressions"
    
    # Primary Key
    suppression_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Tenant
    bank_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    sub_tenant_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    
    # Suppression Details
    suppression_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    
    # Scope (uses arrays as per SQL schema)
    scope_type = Column(String(50), nullable=False)
    rule_ids = Column(ARRAY(UUID(as_uuid=True)), nullable=True)
    connection_ids = Column(ARRAY(UUID(as_uuid=True)), nullable=True)
    
    # Timing
    start_time = Column(DateTime(timezone=True), nullable=False, index=True)
    end_time = Column(DateTime(timezone=True), nullable=False, index=True)
    is_recurring = Column(Boolean, nullable=False, server_default=text("false"))
    recurrence_pattern = Column(JSONB, nullable=True)
    
    # Status
    is_active = Column(Boolean, nullable=False, server_default=text("true"))
    
    # Audit
    created_by = Column(UUID(as_uuid=True), nullable=True)
    updated_by = Column(UUID(as_uuid=True), nullable=True)
