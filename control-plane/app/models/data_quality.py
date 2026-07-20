"""
Data Quality Models
DQ policies, violations, and rule results
"""
from sqlalchemy import Column, String, Integer, Boolean, Text, ForeignKey, UniqueConstraint, text, DateTime, Numeric
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from app.models.base import BaseModel, TimestampMixin, MultiTenancyMixin, SoftDeleteMixin


class DQPolicy(BaseModel, TimestampMixin, MultiTenancyMixin, SoftDeleteMixin):
    """Data quality policy definition"""
    
    __tablename__ = "dq_policies"
    
    # Primary Key
    policy_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Basic Information
    policy_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    
    # Scope
    connection_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    stream_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    
    # Rule Configuration
    rule_type = Column(String(50), nullable=False)  # null_check, range_check, regex, custom_sql
    rule_definition = Column(JSONB, nullable=False)
    
    # Target
    target_columns = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    
    # Severity and Action
    severity = Column(String(50), nullable=False)  # warning, error, critical
    action_on_failure = Column(String(50), nullable=False)  # log, quarantine, reject, alert
    
    # Thresholds
    threshold_type = Column(String(50), nullable=True)  # percentage, count
    threshold_value = Column(Numeric, nullable=True)
    
    # Schedule
    execution_schedule = Column(String(100), nullable=True)  # cron expression
    
    # Status
    is_active = Column(Boolean, nullable=False, server_default=text("true"))
    last_executed_at = Column(DateTime(timezone=True), nullable=True)
    
    # Relationships
    violations = relationship(
        "DQViolation",
        back_populates="policy",
        cascade="all, delete-orphan",
    )
    rule_results = relationship(
        "DQRuleResult",
        back_populates="policy",
        cascade="all, delete-orphan",
    )
    
    __table_args__ = (
        UniqueConstraint("sub_tenant_id", "policy_name", name="uq_dq_policy_name"),
    )
    
    def __repr__(self) -> str:
        return f"<DQPolicy(name={self.policy_name}, rule_type={self.rule_type}, severity={self.severity})>"


class DQViolation(BaseModel, TimestampMixin):
    """Data quality violation records"""
    
    __tablename__ = "dq_violations"
    
    # Primary Key
    violation_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Foreign Key
    policy_id = Column(
        UUID(as_uuid=True),
        ForeignKey("dq_policies.policy_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    
    # Scope
    connection_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    stream_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    
    # Violation Details
    detected_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True)
    violation_count = Column(Integer, nullable=False)
    total_records_checked = Column(Integer, nullable=False)
    violation_percentage = Column(Numeric, nullable=False)
    
    # Status
    status = Column(String(50), nullable=False)  # active, resolved, ignored
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    resolved_by = Column(UUID(as_uuid=True), nullable=True)
    resolution_notes = Column(Text, nullable=True)
    
    # Metadata
    violation_metadata = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    # Relationships
    policy = relationship("DQPolicy", back_populates="violations")
    samples = relationship(
        "DQViolationSample",
        back_populates="violation",
        cascade="all, delete-orphan",
    )
    
    def __repr__(self) -> str:
        return f"<DQViolation(policy={self.policy_id}, count={self.violation_count}, status={self.status})>"


class DQViolationSample(BaseModel, TimestampMixin):
    """Sample records that violated DQ rules"""
    
    __tablename__ = "dq_violation_samples"
    
    # Primary Key
    sample_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Foreign Key
    violation_id = Column(
        UUID(as_uuid=True),
        ForeignKey("dq_violations.violation_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    
    # Record Information
    record_id = Column(String(500), nullable=True)
    record_data = Column(JSONB, nullable=False)
    
    # Violation Details
    violated_column = Column(String(255), nullable=True)
    expected_value = Column(Text, nullable=True)
    actual_value = Column(Text, nullable=True)
    
    # Metadata
    captured_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    
    # Relationships
    violation = relationship("DQViolation", back_populates="samples")
    
    def __repr__(self) -> str:
        return f"<DQViolationSample(violation={self.violation_id}, column={self.violated_column})>"


class DQRuleResult(BaseModel, TimestampMixin):
    """DQ rule execution results"""
    
    __tablename__ = "dq_rule_results"
    
    # Primary Key
    result_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Foreign Key
    policy_id = Column(
        UUID(as_uuid=True),
        ForeignKey("dq_policies.policy_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    
    # Execution Information
    execution_id = Column(String(255), nullable=False, index=True)
    executed_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True)
    
    # Results
    passed = Column(Boolean, nullable=False)
    records_checked = Column(Integer, nullable=False)
    records_passed = Column(Integer, nullable=False)
    records_failed = Column(Integer, nullable=False)
    
    # Metrics
    execution_time_ms = Column(Integer, nullable=False)
    
    # Details
    result_details = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    # Relationships
    policy = relationship("DQPolicy", back_populates="rule_results")
    
    def __repr__(self) -> str:
        return f"<DQRuleResult(policy={self.policy_id}, passed={self.passed}, records_checked={self.records_checked})>"
