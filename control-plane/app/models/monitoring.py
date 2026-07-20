"""
CDC and Monitoring Models
Checkpoint state, CDC metrics, and worker heartbeats
"""
from sqlalchemy import Column, String, Integer, Boolean, Text, ForeignKey, text, DateTime, Numeric, BigInteger
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from app.models.base import BaseModel, TimestampMixin


class CheckpointState(BaseModel, TimestampMixin):
    """CDC checkpoint state tracking"""
    
    __tablename__ = "checkpoint_state"
    
    # Primary Key
    checkpoint_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Identification
    source_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    stream_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    worker_id = Column(String(255), nullable=False, index=True)
    
    # Position Information (MySQL binlog, PostgreSQL LSN, MongoDB timestamp)
    binlog_file = Column(String(255), nullable=True)
    binlog_position = Column(BigInteger, nullable=True)
    gtid_set = Column(Text, nullable=True)
    lsn = Column(Text, nullable=True)  # PostgreSQL Log Sequence Number / MongoDB resume token
    resume_token = Column(Text, nullable=True)  # MongoDB resume token
    
    # Generic checkpoint data
    checkpoint_data = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    # Metadata
    checkpoint_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    records_processed = Column(BigInteger, nullable=False, server_default=text("0"))
    bytes_processed = Column(Numeric, nullable=False, server_default=text("0"))
    
    def __repr__(self) -> str:
        return f"<CheckpointState(worker={self.worker_id}, source={self.source_id})>"


class CDCPositionHistory(BaseModel, TimestampMixin):
    """Historical CDC position tracking"""
    
    __tablename__ = "cdc_position_history"
    
    # Primary Key
    history_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Identification
    source_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    worker_id = Column(String(255), nullable=False, index=True)
    
    # Position snapshot
    position_data = Column(JSONB, nullable=False)
    captured_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True)
    
    # Metrics
    lag_seconds = Column(Integer, nullable=True)
    events_processed = Column(BigInteger, nullable=False, server_default=text("0"))
    
    def __repr__(self) -> str:
        return f"<CDCPositionHistory(source={self.source_id}, captured_at={self.captured_at})>"


class CDCLagMetrics(BaseModel, TimestampMixin):
    """CDC replication lag metrics"""
    
    __tablename__ = "cdc_lag_metrics"
    
    # Primary Key
    metric_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Identification
    connection_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    stream_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    worker_id = Column(String(255), nullable=False)
    
    # Lag Metrics
    lag_seconds = Column(Integer, nullable=False)
    lag_events = Column(BigInteger, nullable=False)
    lag_bytes = Column(Numeric, nullable=False)
    
    # Throughput
    events_per_second = Column(Numeric, nullable=False)
    bytes_per_second = Column(Numeric, nullable=False)
    
    # Timestamp
    measured_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True)
    
    def __repr__(self) -> str:
        return f"<CDCLagMetrics(connection={self.connection_id}, lag_seconds={self.lag_seconds})>"


class WorkerHeartbeat(BaseModel, TimestampMixin):
    """CDC worker health tracking"""
    
    __tablename__ = "worker_heartbeats"
    
    # Primary Key
    heartbeat_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Worker Identification
    worker_id = Column(String(255), nullable=False, unique=True, index=True)
    worker_type = Column(String(50), nullable=False)  # mysql, postgresql, mongodb
    connection_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    
    # Status
    status = Column(String(50), nullable=False)  # running, paused, stopping, error
    last_heartbeat_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True)
    
    # Resource Usage
    cpu_usage_percent = Column(Numeric, nullable=True)
    memory_usage_mb = Column(Integer, nullable=True)
    
    # Worker Info
    worker_version = Column(String(50), nullable=True)
    hostname = Column(String(255), nullable=True)
    pod_name = Column(String(255), nullable=True)
    namespace = Column(String(255), nullable=True)
    
    # Metrics
    events_processed = Column(BigInteger, nullable=False, server_default=text("0"))
    errors_count = Column(Integer, nullable=False, server_default=text("0"))
    last_error = Column(Text, nullable=True)
    last_error_at = Column(DateTime(timezone=True), nullable=True)
    
    # Additional Info
    worker_metadata = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    def __repr__(self) -> str:
        return f"<WorkerHeartbeat(worker_id={self.worker_id}, status={self.status})>"


class RedisStreamTracking(BaseModel, TimestampMixin):
    """Redis Stream metadata and tracking"""
    
    __tablename__ = "redis_stream_tracking"
    
    # Primary Key
    tracking_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Stream Identification
    stream_key = Column(String(500), nullable=False, unique=True, index=True)
    connection_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    stream_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    
    # Tenant Information
    bank_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    sub_tenant_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    
    # Stream Metrics
    total_messages = Column(BigInteger, nullable=False, server_default=text("0"))
    last_message_id = Column(String(100), nullable=True)
    last_message_at = Column(DateTime(timezone=True), nullable=True)
    
    # Consumer Groups
    consumer_groups = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    
    # Status
    is_active = Column(Boolean, nullable=False, server_default=text("true"))
    retention_hours = Column(Integer, nullable=False, server_default=text("24"))
    
    def __repr__(self) -> str:
        return f"<RedisStreamTracking(stream_key={self.stream_key}, messages={self.total_messages})>"


class ConnectionRun(BaseModel, TimestampMixin):
    """Connection sync run history"""
    
    __tablename__ = "connection_runs"
    
    # Primary Key
    run_id = Column(
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
    
    # Run Information
    run_number = Column(Integer, nullable=False)
    trigger_type = Column(String(50), nullable=False)  # scheduled, manual, retry
    triggered_by = Column(UUID(as_uuid=True), nullable=True)
    
    # Status
    status = Column(String(50), nullable=False)  # running, completed, failed, cancelled
    started_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    
    # Metrics
    records_read = Column(BigInteger, nullable=False, server_default=text("0"))
    records_written = Column(BigInteger, nullable=False, server_default=text("0"))
    bytes_processed = Column(Numeric, nullable=False, server_default=text("0"))
    
    # Error Information
    error_message = Column(Text, nullable=True)
    error_stack_trace = Column(Text, nullable=True)
    
    # Metadata
    run_config = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    # Relationships
    connection = relationship("Connection", back_populates="connection_runs")
    
    def __repr__(self) -> str:
        return f"<ConnectionRun(connection={self.connection_id}, run_number={self.run_number}, status={self.status})>"


class ConnectionHealthCheck(BaseModel, TimestampMixin):
    """Connection health check history"""
    
    __tablename__ = "connection_health_checks"
    
    # Primary Key
    check_id = Column(
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
    
    # Check Information
    check_type = Column(String(50), nullable=False)  # source, destination, end_to_end
    checked_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True)
    
    # Results
    is_healthy = Column(Boolean, nullable=False)
    response_time_ms = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
    
    # Details
    check_details = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    # Relationships
    connection = relationship("Connection", back_populates="health_checks")
    
    def __repr__(self) -> str:
        return f"<ConnectionHealthCheck(connection={self.connection_id}, healthy={self.is_healthy})>"


class ConnectionAlertWebhook(BaseModel, TimestampMixin):
    """Webhook configurations for connection alerts"""
    
    __tablename__ = "connection_alert_webhooks"
    
    # Primary Key
    webhook_id = Column(
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
    
    # Webhook Configuration
    webhook_url = Column(String(1000), nullable=False)
    webhook_secret = Column(String(255), nullable=True)
    
    # Alert Types
    alert_on_failure = Column(Boolean, nullable=False, server_default=text("true"))
    alert_on_lag = Column(Boolean, nullable=False, server_default=text("false"))
    alert_on_schema_change = Column(Boolean, nullable=False, server_default=text("false"))
    
    # Thresholds
    lag_threshold_seconds = Column(Integer, nullable=True)
    failure_threshold_count = Column(Integer, nullable=True)
    
    # Status
    is_active = Column(Boolean, nullable=False, server_default=text("true"))
    last_triggered_at = Column(DateTime(timezone=True), nullable=True)
    
    # Relationships
    connection = relationship("Connection", back_populates="alert_webhooks")
    
    def __repr__(self) -> str:
        return f"<ConnectionAlertWebhook(connection={self.connection_id}, url={self.webhook_url})>"


class InitialLoadCheckpoint(BaseModel):
    """Tracks per-table initial load progress from initial_load_checkpoints table."""

    __tablename__ = "initial_load_checkpoints"

    checkpoint_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    connection_id = Column(
        UUID(as_uuid=True),
        ForeignKey("connections.connection_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    stream_id = Column(
        UUID(as_uuid=True),
        ForeignKey("streams.stream_id", ondelete="CASCADE"),
        nullable=False,
    )
    source_table = Column(String(255), nullable=False)
    rows_written = Column(BigInteger, nullable=False, server_default=text("0"))
    status = Column(String(50), nullable=False, server_default=text("'running'::character varying"))
    started_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    completed_at = Column(DateTime(timezone=True), nullable=True)
    error = Column(Text, nullable=True)

    def __repr__(self) -> str:
        return f"<InitialLoadCheckpoint(connection={self.connection_id}, table={self.source_table})>"
