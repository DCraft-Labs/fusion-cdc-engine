"""
Alerting and Notification Models
Alert rules, notification channels, alert history, and escalation policies
"""
from sqlalchemy import Column, String, Integer, Boolean, Text, ForeignKey, text, DateTime, Numeric
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY
from sqlalchemy.orm import relationship
from app.models.base import BaseModel, TimestampMixin


class NotificationChannel(BaseModel, TimestampMixin):
    """Notification channels for alert delivery"""
    
    __tablename__ = "notification_channels"
    
    # Primary Key
    channel_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Tenant
    bank_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    sub_tenant_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    
    # Channel Information
    channel_name = Column(String(255), nullable=False)
    channel_type = Column(String(50), nullable=False, index=True)  # email, slack, webhook, pagerduty, msteams, sms
    description = Column(Text, nullable=True)
    
    # Configuration (channel-specific settings)
    config = Column(JSONB, nullable=False)  # email: {recipients, cc, bcc}, slack: {webhook_url, channel}, etc.
    
    # Authentication (encrypted credentials)
    auth_config = Column(JSONB, nullable=True)  # API keys, tokens, etc.
    
    # Status
    is_active = Column(Boolean, nullable=False, server_default=text("true"))
    is_verified = Column(Boolean, nullable=False, server_default=text("false"))
    verified_at = Column(DateTime(timezone=True), nullable=True)
    
    # Test Status
    last_test_at = Column(DateTime(timezone=True), nullable=True)
    last_test_status = Column(String(50), nullable=True)  # success, failure
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
    
    def __repr__(self) -> str:
        return f"<NotificationChannel(name={self.channel_name}, type={self.channel_type}, active={self.is_active})>"


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
    bank_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    sub_tenant_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    
    # Rule Information
    rule_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    
    # Alert Type and Severity
    alert_type = Column(String(50), nullable=False, index=True)  # connection_failure, high_lag, dq_violation, etc.
    severity = Column(String(50), nullable=False, index=True)  # info, warning, error, critical
    
    # Scope
    scope_type = Column(String(50), nullable=False)  # global, connection, source, destination, stream
    scope_id = Column(UUID(as_uuid=True), nullable=True)  # Generic scope ID
    connection_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    source_id = Column(UUID(as_uuid=True), nullable=True)
    destination_id = Column(UUID(as_uuid=True), nullable=True)
    
    # Condition (evaluation criteria)
    condition_type = Column(String(50), nullable=False)  # threshold, change, anomaly, pattern
    condition_definition = Column(JSONB, nullable=False)  # metric, operator, value, window, etc.
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
    
    def __repr__(self) -> str:
        return f"<AlertRule(name={self.rule_name}, type={self.alert_type}, severity={self.severity}, active={self.is_active})>"


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
    severity_filter = Column(ARRAY(String(50)), nullable=True)  # Array of severities to filter
    
    # Only created_at, no updated_at
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    
    def __repr__(self) -> str:
        return f"<AlertRuleChannel(rule={self.rule_id}, channel={self.channel_id})>"


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
        ForeignKey("alert_rules.rule_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    
    # Escalation Level
    escalation_level = Column(Integer, nullable=False)  # 1, 2, 3, etc.
    
    # Timing
    escalate_after_minutes = Column(Integer, nullable=False)  # Escalate if not acknowledged within this time
    
    # Target Channels
    notification_channel_ids = Column(JSONB, nullable=False)  # Array of channel IDs
    
    # Message Customization
    message_template = Column(Text, nullable=True)  # Custom message for this escalation level
    
    # Relationships
    alert_rule = relationship("AlertRule", back_populates="escalation_policies")
    
    def __repr__(self) -> str:
        return f"<AlertEscalationPolicy(rule={self.rule_id}, level={self.escalation_level}, after={self.escalate_after_minutes}m)>"


class AlertEvaluation(BaseModel, TimestampMixin):
    """History of alert rule evaluations"""
    
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
        ForeignKey("alert_rules.rule_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    
    # Evaluation Details
    evaluated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True)
    evaluation_duration_ms = Column(Integer, nullable=True)
    
    # Result
    triggered = Column(Boolean, nullable=False)
    condition_met = Column(Boolean, nullable=False)
    consecutive_failures_count = Column(Integer, nullable=False, server_default=text("0"))
    
    # Data
    evaluated_value = Column(Numeric, nullable=True)  # The actual metric value evaluated
    threshold_value = Column(Numeric, nullable=True)  # The threshold it was compared against
    evaluation_data = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))  # Additional context
    
    # Alert Created
    alert_id = Column(UUID(as_uuid=True), nullable=True, index=True)  # If an alert was created
    
    # Error Handling
    evaluation_error = Column(Text, nullable=True)
    
    # Relationships
    alert_rule = relationship("AlertRule", back_populates="evaluations")
    
    def __repr__(self) -> str:
        return f"<AlertEvaluation(rule={self.rule_id}, triggered={self.triggered}, at={self.evaluated_at})>"


class AlertHistory(BaseModel):
    """History of alert status changes"""
    
    __tablename__ = "alert_history"
    
    # Primary Key
    history_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Foreign Key
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
    metadata_json = Column("metadata", JSONB, server_default=text("'{}'::jsonb"))  # Use column override for reserved word
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    
    def __repr__(self) -> str:
        return f"<AlertHistory(alert={self.alert_id}, {self.previous_status} -> {self.new_status})>"


class AlertNotificationLog(BaseModel):
    """Log of all alert notifications sent"""
    
    __tablename__ = "alert_notification_logs"
    
    # Primary Key
    log_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Foreign Keys
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
    status = Column(String(50), nullable=False)  # sent, failed, retrying
    retry_count = Column(Integer, nullable=False, server_default=text("0"))
    delivery_time_ms = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
    response_data = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    
    def __repr__(self) -> str:
        return f"<AlertNotificationLog(alert={self.alert_id}, channel={self.channel_type}, status={self.status})>"


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
    bank_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    sub_tenant_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    
    # Suppression Details
    suppression_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    
    # Scope
    scope_type = Column(String(50), nullable=False)  # all, rule, connection, alert_type
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
    
    def __repr__(self) -> str:
        return f"<AlertSuppression(name={self.suppression_name}, scope={self.scope_type}, active={self.is_active})>"
