"""
Alert Management Schemas
Pydantic schemas for alert rules, notification channels, alert history, and escalation
"""
from datetime import datetime
from typing import Optional, List, Dict, Any
from uuid import UUID
from pydantic import BaseModel, Field, validator, EmailStr


# ============================================================================
# Notification Channel Schemas
# ============================================================================

class NotificationChannelBase(BaseModel):
    """Base schema for notification channels"""
    channel_name: str = Field(..., min_length=1, max_length=255, description="Channel name")
    channel_type: str = Field(..., description="Channel type: email, slack, webhook, pagerduty, msteams, sms")
    description: Optional[str] = Field(None, description="Channel description")
    config: Dict[str, Any] = Field(..., description="Channel-specific configuration")
    auth_config: Optional[Dict[str, Any]] = Field(None, description="Authentication configuration")
    is_active: bool = Field(True, description="Whether channel is active")
    rate_limit_per_hour: Optional[int] = Field(None, ge=1, description="Rate limit per hour")
    rate_limit_per_day: Optional[int] = Field(None, ge=1, description="Rate limit per day")
    tags: List[str] = Field(default_factory=list, description="Channel tags")
    
    @validator("channel_type")
    def validate_channel_type(cls, v):
        valid_types = ["email", "slack", "webhook", "pagerduty", "msteams", "sms"]
        if v not in valid_types:
            raise ValueError(f"channel_type must be one of: {', '.join(valid_types)}")
        return v
    
    @validator("config")
    def validate_config(cls, v, values):
        """Validate channel-specific configuration"""
        channel_type = values.get("channel_type")
        
        if channel_type == "email":
            required_fields = ["recipients"]
            if not all(field in v for field in required_fields):
                raise ValueError(f"Email channel requires: {', '.join(required_fields)}")
            if not isinstance(v["recipients"], list) or not v["recipients"]:
                raise ValueError("recipients must be a non-empty list")
        
        elif channel_type == "slack":
            required_fields = ["webhook_url"]
            if not all(field in v for field in required_fields):
                raise ValueError(f"Slack channel requires: {', '.join(required_fields)}")
        
        elif channel_type == "webhook":
            required_fields = ["url", "method"]
            if not all(field in v for field in required_fields):
                raise ValueError(f"Webhook channel requires: {', '.join(required_fields)}")
            if v["method"] not in ["GET", "POST", "PUT", "PATCH"]:
                raise ValueError("method must be GET, POST, PUT, or PATCH")
        
        elif channel_type == "pagerduty":
            required_fields = ["integration_key"]
            if not all(field in v for field in required_fields):
                raise ValueError(f"PagerDuty channel requires: {', '.join(required_fields)}")
        
        elif channel_type == "msteams":
            required_fields = ["webhook_url"]
            if not all(field in v for field in required_fields):
                raise ValueError(f"MS Teams channel requires: {', '.join(required_fields)}")
        
        return v


class NotificationChannelCreate(NotificationChannelBase):
    """Schema for creating a notification channel"""
    pass


class NotificationChannelUpdate(BaseModel):
    """Schema for updating a notification channel"""
    channel_name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    auth_config: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None
    rate_limit_per_hour: Optional[int] = Field(None, ge=1)
    rate_limit_per_day: Optional[int] = Field(None, ge=1)
    tags: Optional[List[str]] = None


class NotificationChannelResponse(NotificationChannelBase):
    """Schema for notification channel response"""
    channel_id: UUID
    bank_id: UUID
    sub_tenant_id: UUID
    is_verified: bool
    verified_at: Optional[datetime]
    last_test_at: Optional[datetime]
    last_test_status: Optional[str]
    last_test_error: Optional[str]
    created_at: datetime
    updated_at: datetime
    created_by: Optional[UUID]
    updated_by: Optional[UUID]
    
    class Config:
        from_attributes = True


class NotificationChannelListResponse(BaseModel):
    """Schema for paginated notification channel list"""
    channels: List[NotificationChannelResponse]
    total: int
    page: int
    page_size: int
    
    class Config:
        from_attributes = True


class NotificationChannelTestRequest(BaseModel):
    """Schema for testing a notification channel"""
    test_message: Optional[str] = Field("Test notification from Fusion", description="Test message to send")


class NotificationChannelTestResponse(BaseModel):
    """Schema for notification channel test result"""
    success: bool
    status: str
    message: str
    delivery_time_ms: Optional[int]
    tested_at: datetime


# ============================================================================
# Alert Rule Schemas
# ============================================================================

class AlertRuleBase(BaseModel):
    """Base schema for alert rules"""
    rule_name: str = Field(..., min_length=1, max_length=255, description="Rule name")
    description: Optional[str] = Field(None, description="Rule description")
    alert_type: str = Field(..., description="Alert type")
    severity: str = Field(..., description="Severity: info, warning, error, critical")
    scope_type: str = Field("global", description="Scope: global, connection, source, destination, stream")
    connection_id: Optional[UUID] = Field(None, description="Connection ID for scoped rules")
    source_id: Optional[UUID] = Field(None, description="Source ID for scoped rules")
    destination_id: Optional[UUID] = Field(None, description="Destination ID for scoped rules")
    stream_id: Optional[UUID] = Field(None, description="Stream ID for scoped rules")
    condition_type: str = Field(..., description="Condition type: threshold, change, anomaly, pattern")
    condition_definition: Dict[str, Any] = Field(..., description="Condition definition")
    evaluation_interval_minutes: int = Field(5, ge=1, le=1440, description="Evaluation interval in minutes")
    evaluation_window_minutes: int = Field(15, ge=1, le=1440, description="Evaluation window in minutes")
    consecutive_failures: int = Field(1, ge=1, description="Consecutive failures before alerting")
    auto_resolve: bool = Field(True, description="Auto-resolve when condition clears")
    auto_resolve_after_minutes: Optional[int] = Field(None, ge=1, description="Auto-resolve after minutes")
    group_by: List[str] = Field(default_factory=list, description="Fields to group alerts by")
    suppression_window_minutes: Optional[int] = Field(None, ge=1, description="Suppression window in minutes")
    is_active: bool = Field(True, description="Whether rule is active")
    tags: List[str] = Field(default_factory=list, description="Rule tags")
    custom_labels: Dict[str, str] = Field(default_factory=dict, description="Custom labels")
    
    @validator("alert_type")
    def validate_alert_type(cls, v):
        valid_types = [
            "connection_failure", "high_lag", "sync_failure", "schema_change",
            "dq_violation", "resource_limit", "authentication_failure", "api_error",
            "data_freshness", "replication_lag", "disk_space", "custom"
        ]
        if v not in valid_types:
            raise ValueError(f"alert_type must be one of: {', '.join(valid_types)}")
        return v
    
    @validator("severity")
    def validate_severity(cls, v):
        valid_severities = ["info", "warning", "error", "critical"]
        if v not in valid_severities:
            raise ValueError(f"severity must be one of: {', '.join(valid_severities)}")
        return v
    
    @validator("scope_type")
    def validate_scope_type(cls, v):
        valid_scopes = ["global", "connection", "source", "destination", "stream"]
        if v not in valid_scopes:
            raise ValueError(f"scope_type must be one of: {', '.join(valid_scopes)}")
        return v
    
    @validator("condition_type")
    def validate_condition_type(cls, v):
        valid_types = ["threshold", "change", "anomaly", "pattern"]
        if v not in valid_types:
            raise ValueError(f"condition_type must be one of: {', '.join(valid_types)}")
        return v
    
    @validator("condition_definition")
    def validate_condition_definition(cls, v, values):
        """Validate condition definition based on condition type"""
        condition_type = values.get("condition_type")
        
        if condition_type == "threshold":
            required_fields = ["metric", "operator", "value"]
            if not all(field in v for field in required_fields):
                raise ValueError(f"Threshold condition requires: {', '.join(required_fields)}")
            if v["operator"] not in ["gt", "gte", "lt", "lte", "eq", "ne"]:
                raise ValueError("operator must be one of: gt, gte, lt, lte, eq, ne")
        
        elif condition_type == "change":
            required_fields = ["metric", "change_type", "threshold"]
            if not all(field in v for field in required_fields):
                raise ValueError(f"Change condition requires: {', '.join(required_fields)}")
            if v["change_type"] not in ["increase", "decrease", "change"]:
                raise ValueError("change_type must be one of: increase, decrease, change")
        
        elif condition_type == "anomaly":
            required_fields = ["metric", "sensitivity"]
            if not all(field in v for field in required_fields):
                raise ValueError(f"Anomaly condition requires: {', '.join(required_fields)}")
            if v["sensitivity"] not in ["low", "medium", "high"]:
                raise ValueError("sensitivity must be one of: low, medium, high")
        
        elif condition_type == "pattern":
            required_fields = ["metric", "pattern"]
            if not all(field in v for field in required_fields):
                raise ValueError(f"Pattern condition requires: {', '.join(required_fields)}")
        
        return v


class AlertRuleCreate(AlertRuleBase):
    """Schema for creating an alert rule"""
    notification_channel_ids: List[UUID] = Field(..., min_items=1, description="Notification channel IDs")


class AlertRuleUpdate(BaseModel):
    """Schema for updating an alert rule"""
    rule_name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    severity: Optional[str] = None
    condition_definition: Optional[Dict[str, Any]] = None
    evaluation_interval_minutes: Optional[int] = Field(None, ge=1, le=1440)
    evaluation_window_minutes: Optional[int] = Field(None, ge=1, le=1440)
    consecutive_failures: Optional[int] = Field(None, ge=1)
    auto_resolve: Optional[bool] = None
    auto_resolve_after_minutes: Optional[int] = Field(None, ge=1)
    group_by: Optional[List[str]] = None
    suppression_window_minutes: Optional[int] = Field(None, ge=1)
    is_active: Optional[bool] = None
    tags: Optional[List[str]] = None
    custom_labels: Optional[Dict[str, str]] = None
    notification_channel_ids: Optional[List[UUID]] = None


class AlertRuleResponse(AlertRuleBase):
    """Schema for alert rule response"""
    rule_id: UUID
    bank_id: UUID
    sub_tenant_id: UUID
    last_evaluated_at: Optional[datetime] = None
    last_triggered_at: Optional[datetime] = None
    notification_channel_ids: List[UUID] = Field(default_factory=list)
    active_alerts_count: int = Field(0, description="Number of active alerts")
    total_evaluations: int = Field(0, description="Total evaluations")
    total_triggers: int = Field(0, description="Total triggers")
    created_at: datetime
    updated_at: datetime
    created_by: Optional[UUID]
    updated_by: Optional[UUID]
    
    class Config:
        from_attributes = True


class AlertRuleListResponse(BaseModel):
    """Schema for paginated alert rule list"""
    rules: List[AlertRuleResponse]
    total: int
    page: int
    page_size: int
    
    class Config:
        from_attributes = True


class AlertRuleSearchFilters(BaseModel):
    """Schema for alert rule search filters"""
    alert_type: Optional[str] = None
    severity: Optional[str] = None
    scope_type: Optional[str] = None
    connection_id: Optional[UUID] = None
    is_active: Optional[bool] = None
    search: Optional[str] = Field(None, description="Search in name/description")
    page: int = Field(1, ge=1)
    page_size: int = Field(50, ge=1, le=100)


# ============================================================================
# Alert Rule Testing Schemas
# ============================================================================

class AlertRuleTestRequest(BaseModel):
    """Schema for testing an alert rule"""
    condition_type: str = Field(..., description="Condition type: threshold, change, anomaly, pattern")
    condition_definition: Dict[str, Any] = Field(..., description="Condition definition")
    connection_id: Optional[UUID] = Field(None, description="Connection ID for testing")
    use_sample_data: bool = Field(True, description="Use sample data for testing")


class AlertRuleTestResponse(BaseModel):
    """Schema for alert rule test result"""
    test_passed: bool
    condition_met: bool
    evaluated_value: Optional[float]
    threshold_value: Optional[float]
    evaluation_time_ms: int
    test_data: Dict[str, Any]
    message: str
    tested_at: datetime


# ============================================================================
# Alert Evaluation Schemas
# ============================================================================

class AlertEvaluationResponse(BaseModel):
    """Schema for alert evaluation response"""
    evaluation_id: UUID
    rule_id: UUID
    evaluated_at: datetime
    evaluation_duration_ms: Optional[int]
    triggered: bool
    condition_met: bool
    consecutive_failures_count: int
    evaluated_value: Optional[float]
    threshold_value: Optional[float]
    evaluation_data: Dict[str, Any]
    alert_id: Optional[UUID]
    evaluation_error: Optional[str]
    created_at: datetime
    
    class Config:
        from_attributes = True


class AlertEvaluationListResponse(BaseModel):
    """Schema for paginated alert evaluation list"""
    evaluations: List[AlertEvaluationResponse]
    total: int
    page: int
    page_size: int
    
    class Config:
        from_attributes = True


# ============================================================================
# Alert Schemas (extending existing Alert model)
# ============================================================================

class AlertResponse(BaseModel):
    """Schema for alert response"""
    alert_id: UUID
    alert_type: str
    severity: str
    title: str
    message: str
    bank_id: UUID
    sub_tenant_id: UUID
    connection_id: Optional[UUID]
    alert_context: Optional[Dict[str, Any]]
    status: str  # Computed property
    triggered_at: datetime
    acknowledged: bool
    acknowledged_at: Optional[datetime]
    acknowledged_by: Optional[UUID]
    resolved: bool
    resolved_at: Optional[datetime]
    resolution_notes: Optional[str]
    webhook_sent: bool
    webhook_sent_at: Optional[datetime]
    webhook_response_status: Optional[int]
    
    class Config:
        from_attributes = True


class AlertListResponse(BaseModel):
    """Schema for paginated alert list"""
    alerts: List[AlertResponse]
    total: int
    page: int
    page_size: int
    
    class Config:
        from_attributes = True


class AlertSearchFilters(BaseModel):
    """Schema for alert search filters"""
    alert_type: Optional[str] = None
    severity: Optional[str] = None
    status: Optional[str] = None
    connection_id: Optional[UUID] = None
    source_id: Optional[UUID] = None
    destination_id: Optional[UUID] = None
    search: Optional[str] = Field(None, description="Search in title/message")
    page: int = Field(1, ge=1)
    page_size: int = Field(50, ge=1, le=100)


class AlertAcknowledgeRequest(BaseModel):
    """Schema for acknowledging an alert"""
    notes: Optional[str] = Field(None, description="Acknowledgement notes")


class AlertResolveRequest(BaseModel):
    """Schema for resolving an alert"""
    notes: Optional[str] = Field(None, description="Resolution notes")


# ============================================================================
# Alert History Schemas
# ============================================================================

class AlertHistoryResponse(BaseModel):
    """Schema for alert history response"""
    history_id: UUID
    alert_id: UUID
    old_status: Optional[str]
    new_status: str
    changed_by: Optional[UUID]
    changed_at: datetime
    change_reason: Optional[str]
    metadata_json: Optional[Dict[str, Any]]
    created_at: datetime
    
    class Config:
        from_attributes = True


class AlertHistoryListResponse(BaseModel):
    """Schema for paginated alert history list"""
    history: List[AlertHistoryResponse]
    total: int
    page: int
    page_size: int
    
    class Config:
        from_attributes = True


# ============================================================================
# Alert Notification Log Schemas
# ============================================================================

class AlertNotificationLogResponse(BaseModel):
    """Schema for alert notification log response"""
    log_id: UUID
    alert_id: UUID
    channel_id: Optional[UUID]
    rule_id: Optional[UUID]
    sent_at: datetime
    channel_type: str
    status: str
    retry_count: int
    delivery_status: Optional[str]
    response_code: Optional[str]
    error_message: Optional[str]
    delivery_time_ms: Optional[int]
    notification_content: Dict[str, Any]
    notification_metadata: Dict[str, Any]
    created_at: datetime
    
    class Config:
        from_attributes = True


class AlertNotificationLogListResponse(BaseModel):
    """Schema for paginated alert notification log list"""
    logs: List[AlertNotificationLogResponse]
    total: int
    page: int
    page_size: int
    
    class Config:
        from_attributes = True


# ============================================================================
# Escalation Policy Schemas
# ============================================================================

class AlertEscalationPolicyBase(BaseModel):
    """Base schema for escalation policy"""
    escalation_level: int = Field(..., ge=1, description="Escalation level (1, 2, 3, etc.)")
    escalate_after_minutes: int = Field(..., ge=1, description="Escalate after minutes")
    notification_channel_ids: List[UUID] = Field(..., min_items=1, description="Channel IDs for this level")
    message_template: Optional[str] = Field(None, description="Custom message template")


class AlertEscalationPolicyCreate(AlertEscalationPolicyBase):
    """Schema for creating an escalation policy"""
    pass


class AlertEscalationPolicyResponse(AlertEscalationPolicyBase):
    """Schema for escalation policy response"""
    policy_id: UUID
    rule_id: UUID
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


# ============================================================================
# Alert Suppression Schemas
# ============================================================================

class AlertSuppressionBase(BaseModel):
    """Base schema for alert suppression"""
    suppression_name: str = Field(..., min_length=1, max_length=255, description="Suppression name")
    description: Optional[str] = Field(None, description="Suppression description")
    scope_type: str = Field(..., description="Scope: all, rule, connection, alert_type")
    rule_ids: Optional[List[UUID]] = Field(None, description="Rule IDs for scoped suppression")
    connection_ids: Optional[List[UUID]] = Field(None, description="Connection IDs for scoped suppression")
    start_time: datetime = Field(..., description="Suppression start time")
    end_time: datetime = Field(..., description="Suppression end time")
    is_recurring: bool = Field(False, description="Whether suppression recurs")
    recurrence_pattern: Optional[Dict[str, Any]] = Field(None, description="Recurrence pattern (JSONB)")
    
    @validator("scope_type")
    def validate_scope_type(cls, v):
        valid_scopes = ["all", "rule", "connection"]
        if v not in valid_scopes:
            raise ValueError(f"scope_type must be one of: {', '.join(valid_scopes)}")
        return v
    
    @validator("end_time")
    def validate_end_time(cls, v, values):
        start_time = values.get("start_time")
        if start_time and v <= start_time:
            raise ValueError("end_time must be after start_time")
        return v


class AlertSuppressionCreate(AlertSuppressionBase):
    """Schema for creating an alert suppression"""
    pass


class AlertSuppressionUpdate(BaseModel):
    """Schema for updating an alert suppression"""
    suppression_name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    end_time: Optional[datetime] = None
    is_active: Optional[bool] = None


class AlertSuppressionResponse(AlertSuppressionBase):
    """Schema for alert suppression response"""
    suppression_id: UUID
    bank_id: UUID
    sub_tenant_id: UUID
    is_active: bool
    created_by: Optional[UUID]
    updated_by: Optional[UUID]
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class AlertSuppressionListResponse(BaseModel):
    """Schema for paginated alert suppression list"""
    suppressions: List[AlertSuppressionResponse]
    total: int
    page: int
    page_size: int
    
    class Config:
        from_attributes = True


# ============================================================================
# Alert Statistics Schemas
# ============================================================================

class AlertStatistics(BaseModel):
    """Schema for alert statistics"""
    total_alerts: int = Field(0, description="Total alerts")
    active_alerts: int = Field(0, description="Active alerts")
    acknowledged_alerts: int = Field(0, description="Acknowledged alerts")
    resolved_alerts: int = Field(0, description="Resolved alerts")
    critical_alerts: int = Field(0, description="Critical severity alerts")
    error_alerts: int = Field(0, description="Error severity alerts")
    warning_alerts: int = Field(0, description="Warning severity alerts")
    info_alerts: int = Field(0, description="Info severity alerts")
    alerts_by_type: Dict[str, int] = Field(default_factory=dict, description="Alerts grouped by type")
    alerts_last_24h: int = Field(0, description="Alerts in last 24 hours")
    alerts_last_7d: int = Field(0, description="Alerts in last 7 days")
    avg_resolution_time_minutes: Optional[float] = Field(None, description="Average resolution time")
    
    class Config:
        from_attributes = True


class AlertDashboard(BaseModel):
    """Schema for alert dashboard"""
    statistics: AlertStatistics
    top_alert_types: List[Dict[str, Any]] = Field(default_factory=list, description="Top alert types")
    recent_critical_alerts: List[AlertResponse] = Field(default_factory=list, description="Recent critical alerts")
    unacknowledged_critical: int = Field(0, description="Unacknowledged critical alerts")
    trend: str = Field("stable", description="Alert trend: improving, stable, worsening")
    
    class Config:
        from_attributes = True
