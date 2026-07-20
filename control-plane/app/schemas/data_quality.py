"""
Pydantic schemas for Data Quality Rules Management

Includes schemas for:
- DQ Policies (rules)
- Rule templates
- Rule execution and results
- Violations and samples
- Quality metrics and scoring
- Anomaly detection
- Data profiling
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field, field_validator
from decimal import Decimal


# ============================================================================
# Rule Template Schemas
# ============================================================================

class RuleTemplateBase(BaseModel):
    """Base schema for rule templates"""
    template_name: str = Field(..., min_length=1, max_length=255, description="Template name")
    template_type: str = Field(..., description="Template type: null_check, range_check, regex, custom_sql, uniqueness, freshness, referential_integrity")
    description: Optional[str] = Field(None, description="Template description")
    rule_definition_schema: Dict[str, Any] = Field(..., description="JSON schema for rule configuration")
    default_severity: str = Field(default="warning", description="Default severity: warning, error, critical")
    default_action: str = Field(default="log", description="Default action: log, quarantine, reject, alert")
    category: str = Field(..., description="Rule category: completeness, accuracy, consistency, validity, timeliness, uniqueness")
    is_active: bool = Field(default=True, description="Template is active")
    
    @field_validator("template_type")
    @classmethod
    def validate_template_type(cls, v: str) -> str:
        allowed = ["null_check", "range_check", "regex", "custom_sql", "uniqueness", "freshness", "referential_integrity", "statistical_outlier", "format_check", "enum_check"]
        if v not in allowed:
            raise ValueError(f"template_type must be one of {allowed}")
        return v
    
    @field_validator("default_severity")
    @classmethod
    def validate_severity(cls, v: str) -> str:
        allowed = ["info", "warning", "error", "critical"]
        if v not in allowed:
            raise ValueError(f"severity must be one of {allowed}")
        return v
    
    @field_validator("default_action")
    @classmethod
    def validate_action(cls, v: str) -> str:
        allowed = ["log", "quarantine", "reject", "alert", "block"]
        if v not in allowed:
            raise ValueError(f"action must be one of {allowed}")
        return v
    
    @field_validator("category")
    @classmethod
    def validate_category(cls, v: str) -> str:
        allowed = ["completeness", "accuracy", "consistency", "validity", "timeliness", "uniqueness"]
        if v not in allowed:
            raise ValueError(f"category must be one of {allowed}")
        return v


class RuleTemplateCreate(RuleTemplateBase):
    """Schema for creating rule template"""
    pass


class RuleTemplateUpdate(BaseModel):
    """Schema for updating rule template"""
    template_name: Optional[str] = Field(None, min_length=1, max_length=255)
    template_type: Optional[str] = None
    description: Optional[str] = None
    rule_definition_schema: Optional[Dict[str, Any]] = None
    default_severity: Optional[str] = None
    default_action: Optional[str] = None
    category: Optional[str] = None
    is_active: Optional[bool] = None


class RuleTemplateResponse(RuleTemplateBase):
    """Schema for rule template response"""
    template_id: UUID
    usage_count: int = Field(default=0, description="Number of policies using this template")
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class RuleTemplateListResponse(BaseModel):
    """Paginated list of rule templates"""
    templates: List[RuleTemplateResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


# ============================================================================
# DQ Policy (Rule) Schemas
# ============================================================================

class DQPolicyBase(BaseModel):
    """Base schema for DQ policy"""
    policy_name: str = Field(..., min_length=1, max_length=255, description="Policy name")
    description: Optional[str] = Field(None, description="Policy description")
    connection_id: Optional[UUID] = Field(None, description="Connection to apply rule to")
    stream_id: Optional[UUID] = Field(None, description="Stream to apply rule to")
    rule_type: str = Field(..., description="Rule type")
    rule_definition: Dict[str, Any] = Field(..., description="Rule configuration")
    target_columns: List[str] = Field(default_factory=list, description="Target columns for validation")
    severity: str = Field(default="warning", description="Severity level")
    action_on_failure: str = Field(default="log", description="Action on failure")
    threshold_type: Optional[str] = Field(None, description="Threshold type: percentage, count")
    threshold_value: Optional[Decimal] = Field(None, description="Threshold value")
    execution_schedule: Optional[str] = Field(None, description="Cron expression for scheduled execution")
    is_active: bool = Field(default=True, description="Policy is active")
    
    @field_validator("rule_type")
    @classmethod
    def validate_rule_type(cls, v: str) -> str:
        allowed = ["null_check", "range_check", "regex", "custom_sql", "uniqueness", "freshness", "referential_integrity", "statistical_outlier", "format_check", "enum_check"]
        if v not in allowed:
            raise ValueError(f"rule_type must be one of {allowed}")
        return v
    
    @field_validator("severity")
    @classmethod
    def validate_severity(cls, v: str) -> str:
        allowed = ["info", "warning", "error", "critical"]
        if v not in allowed:
            raise ValueError(f"severity must be one of {allowed}")
        return v
    
    @field_validator("action_on_failure")
    @classmethod
    def validate_action(cls, v: str) -> str:
        allowed = ["log", "quarantine", "reject", "alert", "block"]
        if v not in allowed:
            raise ValueError(f"action_on_failure must be one of {allowed}")
        return v
    
    @field_validator("threshold_type")
    @classmethod
    def validate_threshold_type(cls, v: Optional[str]) -> Optional[str]:
        if v and v not in ["percentage", "count"]:
            raise ValueError("threshold_type must be 'percentage' or 'count'")
        return v


class DQPolicyCreate(DQPolicyBase):
    """Schema for creating DQ policy"""
    template_id: Optional[UUID] = Field(None, description="Template to base policy on")


class DQPolicyUpdate(BaseModel):
    """Schema for updating DQ policy"""
    policy_name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    connection_id: Optional[UUID] = None
    stream_id: Optional[UUID] = None
    rule_type: Optional[str] = None
    rule_definition: Optional[Dict[str, Any]] = None
    target_columns: Optional[List[str]] = None
    severity: Optional[str] = None
    action_on_failure: Optional[str] = None
    threshold_type: Optional[str] = None
    threshold_value: Optional[Decimal] = None
    execution_schedule: Optional[str] = None
    is_active: Optional[bool] = None


class DQPolicyResponse(DQPolicyBase):
    """Schema for DQ policy response"""
    policy_id: UUID
    last_executed_at: Optional[datetime] = None
    sub_tenant_id: UUID
    bank_id: UUID
    created_at: datetime
    updated_at: datetime
    
    # Aggregated data
    violation_count: int = Field(default=0, description="Total violations")
    active_violation_count: int = Field(default=0, description="Active violations")
    last_execution_status: Optional[bool] = Field(None, description="Last execution passed/failed")
    
    class Config:
        from_attributes = True


class DQPolicyListResponse(BaseModel):
    """Paginated list of DQ policies"""
    policies: List[DQPolicyResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class DQPolicySearchFilters(BaseModel):
    """Search and filter parameters for policies"""
    connection_id: Optional[UUID] = None
    stream_id: Optional[UUID] = None
    rule_type: Optional[str] = None
    severity: Optional[str] = None
    is_active: Optional[bool] = None
    search: Optional[str] = Field(None, description="Search in name/description")
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=100)


# ============================================================================
# Rule Testing and Validation Schemas
# ============================================================================

class RuleTestRequest(BaseModel):
    """Request to test a rule before saving"""
    rule_type: str
    rule_definition: Dict[str, Any]
    connection_id: UUID
    stream_id: Optional[UUID] = None
    target_columns: List[str]
    sample_size: int = Field(default=1000, ge=1, le=10000, description="Number of records to test")


class RuleTestResult(BaseModel):
    """Result of rule testing"""
    test_passed: bool
    records_tested: int
    records_passed: int
    records_failed: int
    execution_time_ms: int
    sample_violations: List[Dict[str, Any]] = Field(default_factory=list, description="Sample violation records")
    error_message: Optional[str] = None
    tested_at: datetime


# ============================================================================
# Rule Execution and Results Schemas
# ============================================================================

class RuleExecutionRequest(BaseModel):
    """Request to execute a specific rule"""
    policy_id: UUID
    force_execution: bool = Field(default=False, description="Force execution even if scheduled")


class DQRuleResultBase(BaseModel):
    """Base schema for rule result"""
    execution_id: str
    passed: bool
    records_checked: int
    records_passed: int
    records_failed: int
    execution_time_ms: int
    result_details: Dict[str, Any] = Field(default_factory=dict)


class DQRuleResultResponse(DQRuleResultBase):
    """Schema for rule result response"""
    result_id: UUID
    policy_id: UUID
    executed_at: datetime
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class DQRuleResultListResponse(BaseModel):
    """Paginated list of rule results"""
    results: List[DQRuleResultResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


# ============================================================================
# Violation Schemas
# ============================================================================

class DQViolationSampleBase(BaseModel):
    """Base schema for violation sample"""
    record_id: Optional[str] = None
    record_data: Dict[str, Any]
    violated_column: Optional[str] = None
    expected_value: Optional[str] = None
    actual_value: Optional[str] = None


class DQViolationSampleResponse(DQViolationSampleBase):
    """Schema for violation sample response"""
    sample_id: UUID
    violation_id: UUID
    captured_at: datetime
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class DQViolationBase(BaseModel):
    """Base schema for violation"""
    violation_count: int
    total_records_checked: int
    violation_percentage: Decimal
    status: str = Field(default="active", description="Violation status: active, resolved, ignored")
    
    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        allowed = ["active", "resolved", "ignored"]
        if v not in allowed:
            raise ValueError(f"status must be one of {allowed}")
        return v


class DQViolationResponse(DQViolationBase):
    """Schema for violation response"""
    violation_id: UUID
    policy_id: UUID
    connection_id: UUID
    stream_id: Optional[UUID] = None
    detected_at: datetime
    resolved_at: Optional[datetime] = None
    resolved_by: Optional[UUID] = None
    resolution_notes: Optional[str] = None
    violation_metadata: Dict[str, Any] = Field(default_factory=dict)
    samples: List[DQViolationSampleResponse] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class DQViolationListResponse(BaseModel):
    """Paginated list of violations"""
    violations: List[DQViolationResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class ViolationResolveRequest(BaseModel):
    """Request to resolve a violation"""
    status: str = Field(..., description="New status: resolved, ignored")
    resolution_notes: Optional[str] = Field(None, description="Resolution notes")
    
    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        if v not in ["resolved", "ignored"]:
            raise ValueError("status must be 'resolved' or 'ignored'")
        return v


# ============================================================================
# Quality Metrics and Scoring Schemas
# ============================================================================

class QualityMetrics(BaseModel):
    """Overall quality metrics for connection/stream"""
    connection_id: UUID
    stream_id: Optional[UUID] = None
    quality_score: Decimal = Field(..., ge=0, le=100, description="Overall quality score (0-100)")
    completeness_score: Decimal = Field(..., ge=0, le=100)
    accuracy_score: Decimal = Field(..., ge=0, le=100)
    consistency_score: Decimal = Field(..., ge=0, le=100)
    validity_score: Decimal = Field(..., ge=0, le=100)
    timeliness_score: Decimal = Field(..., ge=0, le=100)
    uniqueness_score: Decimal = Field(..., ge=0, le=100)
    total_policies: int
    active_policies: int
    total_violations: int
    active_violations: int
    last_calculated_at: datetime


class QualityScoreHistory(BaseModel):
    """Historical quality scores"""
    connection_id: UUID
    stream_id: Optional[UUID] = None
    history: List[Dict[str, Any]] = Field(
        ...,
        description="Time series of quality scores"
    )
    period: str = Field(..., description="Period: day, week, month")


class QualityDashboard(BaseModel):
    """Dashboard with quality overview"""
    overall_score: Decimal
    total_connections: int
    connections_with_issues: int
    total_policies: int
    active_policies: int
    total_violations: int
    active_violations: int
    critical_violations: int
    top_failing_policies: List[Dict[str, Any]]
    score_by_category: Dict[str, Decimal]
    trend: str = Field(..., description="Trend: improving, stable, degrading")


# ============================================================================
# Anomaly Detection Schemas
# ============================================================================

class AnomalyDetectionConfig(BaseModel):
    """Configuration for anomaly detection"""
    connection_id: UUID
    stream_id: Optional[UUID] = None
    detection_type: str = Field(..., description="Type: statistical, ml_based, threshold")
    columns: List[str] = Field(..., description="Columns to monitor")
    sensitivity: str = Field(default="medium", description="Sensitivity: low, medium, high")
    baseline_period_days: int = Field(default=7, ge=1, le=90)
    alert_on_anomaly: bool = Field(default=True)
    config: Dict[str, Any] = Field(default_factory=dict, description="Additional configuration")
    
    @field_validator("detection_type")
    @classmethod
    def validate_detection_type(cls, v: str) -> str:
        allowed = ["statistical", "ml_based", "threshold", "pattern"]
        if v not in allowed:
            raise ValueError(f"detection_type must be one of {allowed}")
        return v
    
    @field_validator("sensitivity")
    @classmethod
    def validate_sensitivity(cls, v: str) -> str:
        allowed = ["low", "medium", "high"]
        if v not in allowed:
            raise ValueError(f"sensitivity must be one of {allowed}")
        return v


class AnomalyDetectionResponse(AnomalyDetectionConfig):
    """Response for anomaly detection configuration"""
    config_id: UUID
    is_active: bool
    last_detection_at: Optional[datetime] = None
    anomalies_detected: int = Field(default=0)
    created_at: datetime
    updated_at: datetime


class AnomalyRecord(BaseModel):
    """Detected anomaly record"""
    anomaly_id: UUID
    config_id: UUID
    connection_id: UUID
    stream_id: Optional[UUID] = None
    detected_at: datetime
    column_name: str
    expected_value: Optional[str] = None
    actual_value: Optional[str] = None
    anomaly_score: Decimal = Field(..., ge=0, le=1, description="Anomaly confidence score")
    anomaly_type: str = Field(..., description="Type: outlier, spike, drop, pattern_break")
    metadata: Dict[str, Any] = Field(default_factory=dict)
    status: str = Field(default="new", description="Status: new, confirmed, false_positive, resolved")


# ============================================================================
# Data Profiling Schemas
# ============================================================================

class DataProfilingRequest(BaseModel):
    """Request to profile data"""
    connection_id: UUID
    stream_id: Optional[UUID] = None
    columns: Optional[List[str]] = Field(None, description="Specific columns to profile, or all if None")
    sample_size: int = Field(default=10000, ge=1, le=1000000)
    include_distributions: bool = Field(default=True)
    include_patterns: bool = Field(default=True)


class ColumnProfile(BaseModel):
    """Profile for a single column"""
    column_name: str
    data_type: str
    nullable: bool
    total_count: int
    null_count: int
    null_percentage: Decimal
    distinct_count: int
    distinct_percentage: Decimal
    min_value: Optional[str] = None
    max_value: Optional[str] = None
    avg_value: Optional[str] = None
    median_value: Optional[str] = None
    std_dev: Optional[Decimal] = None
    top_values: List[Dict[str, Any]] = Field(default_factory=list, description="Most frequent values")
    patterns: List[str] = Field(default_factory=list, description="Detected patterns (e.g., email, phone)")
    distribution: Optional[Dict[str, Any]] = Field(None, description="Value distribution")


class DataProfilingResponse(BaseModel):
    """Response for data profiling"""
    connection_id: UUID
    stream_id: Optional[UUID] = None
    total_records: int
    total_columns: int
    profiled_at: datetime
    column_profiles: List[ColumnProfile]
    recommended_rules: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Recommended quality rules based on profiling"
    )


class ProfilingHistoryResponse(BaseModel):
    """Historical profiling data"""
    connection_id: UUID
    stream_id: Optional[UUID] = None
    profiles: List[DataProfilingResponse]
    total: int
