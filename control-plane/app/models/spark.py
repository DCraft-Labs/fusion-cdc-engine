"""
Spark Job Models
Spark applications, executors, and job queue
"""
from sqlalchemy import Column, String, Integer, Boolean, Text, ForeignKey, text, DateTime, Numeric
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from app.models.base import BaseModel, TimestampMixin


class SparkJobQueue(BaseModel, TimestampMixin):
    """Spark job queue for transformation execution"""
    
    __tablename__ = "spark_job_queue"
    
    # Primary Key
    job_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Job Information
    job_name = Column(String(255), nullable=False)
    job_type = Column(String(50), nullable=False)  # transformation, aggregation, dq_check
    
    # References
    pipeline_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    connection_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    
    # Priority and Scheduling
    priority = Column(Integer, nullable=False, server_default=text("5"))
    scheduled_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True)
    
    # Job Configuration
    spark_config = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    input_params = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    # Status
    status = Column(String(50), nullable=False, server_default=text("'queued'::character varying"))  # queued, running, completed, failed
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    
    # Error Information
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, nullable=False, server_default=text("0"))
    max_retries = Column(Integer, nullable=False, server_default=text("3"))
    
    # Tenant Information
    bank_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    sub_tenant_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    
    def __repr__(self) -> str:
        return f"<SparkJobQueue(name={self.job_name}, status={self.status})>"


class SparkApplication(BaseModel, TimestampMixin):
    """Spark application metadata"""
    
    __tablename__ = "spark_applications"
    
    # Primary Key
    application_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Spark Information
    spark_application_id = Column(String(255), nullable=False, unique=True, index=True)
    spark_application_name = Column(String(255), nullable=False)
    
    # Job Reference
    job_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    
    # Status
    status = Column(String(50), nullable=False)  # running, completed, failed, killed
    started_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    
    # Resource Usage
    driver_host = Column(String(255), nullable=True)
    driver_port = Column(Integer, nullable=True)
    cores_allocated = Column(Integer, nullable=False)
    memory_allocated_mb = Column(Integer, nullable=False)
    
    # Metrics
    duration_ms = Column(Integer, nullable=True)
    input_records = Column(Integer, nullable=False, server_default=text("0"))
    output_records = Column(Integer, nullable=False, server_default=text("0"))
    
    # Configuration
    spark_conf = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    # UI
    spark_ui_url = Column(String(500), nullable=True)
    
    # Relationships
    executors = relationship(
        "SparkExecutor",
        back_populates="application",
        cascade="all, delete-orphan",
    )
    
    def __repr__(self) -> str:
        return f"<SparkApplication(spark_id={self.spark_application_id}, status={self.status})>"


class SparkExecutor(BaseModel, TimestampMixin):
    """Spark executor instances"""
    
    __tablename__ = "spark_executors"
    
    # Primary Key
    executor_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Foreign Key
    application_id = Column(
        UUID(as_uuid=True),
        ForeignKey("spark_applications.application_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    
    # Executor Information
    executor_number = Column(String(50), nullable=False)
    host = Column(String(255), nullable=False)
    port = Column(Integer, nullable=False)
    
    # Resources
    cores = Column(Integer, nullable=False)
    memory_mb = Column(Integer, nullable=False)
    
    # Status
    status = Column(String(50), nullable=False)  # active, removed, lost
    started_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    removed_at = Column(DateTime(timezone=True), nullable=True)
    
    # Metrics
    task_count = Column(Integer, nullable=False, server_default=text("0"))
    failed_task_count = Column(Integer, nullable=False, server_default=text("0"))
    
    # Relationships
    application = relationship("SparkApplication", back_populates="executors")
    
    def __repr__(self) -> str:
        return f"<SparkExecutor(number={self.executor_number}, host={self.host}, status={self.status})>"


class SparkExecutorHistory(BaseModel, TimestampMixin):
    """Historical Spark executor metrics"""
    
    __tablename__ = "spark_executor_history"
    
    # Primary Key
    history_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # References
    application_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    executor_number = Column(String(50), nullable=False)
    
    # Resource Metrics
    cpu_usage_percent = Column(Numeric, nullable=False)
    memory_used_mb = Column(Integer, nullable=False)
    disk_used_mb = Column(Integer, nullable=False)
    
    # Task Metrics
    active_tasks = Column(Integer, nullable=False)
    completed_tasks = Column(Integer, nullable=False)
    failed_tasks = Column(Integer, nullable=False)
    
    # I/O Metrics
    input_bytes = Column(Numeric, nullable=False, server_default=text("0"))
    output_bytes = Column(Numeric, nullable=False, server_default=text("0"))
    shuffle_read_bytes = Column(Numeric, nullable=False, server_default=text("0"))
    shuffle_write_bytes = Column(Numeric, nullable=False, server_default=text("0"))
    
    # Timestamp
    recorded_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True)
    
    def __repr__(self) -> str:
        return f"<SparkExecutorHistory(app={self.application_id}, executor={self.executor_number})>"
