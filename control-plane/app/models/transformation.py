"""
Transformation Pipeline Models
SQL/Python transformations and UDF catalog
"""
from sqlalchemy import Column, String, Integer, Boolean, Text, ForeignKey, UniqueConstraint, text, DateTime
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from app.models.base import BaseModel, TimestampMixin, SoftDeleteMixin, MultiTenancyMixin


class TransformPipeline(BaseModel, TimestampMixin, SoftDeleteMixin, MultiTenancyMixin):
    """Transformation pipeline definition"""
    
    __tablename__ = "transform_pipelines"
    
    # Primary Key
    pipeline_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Basic Information
    pipeline_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    
    # Pipeline Configuration
    pipeline_type = Column(String(50), nullable=False)  # sql, python, spark
    transformation_code = Column(Text, nullable=False)
    language = Column(String(50), nullable=False)  # sql, python, scala
    
    # Dependencies
    input_streams = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    output_stream = Column(String(255), nullable=False)
    
    # Execution Configuration
    execution_mode = Column(String(50), nullable=False)  # batch, streaming
    spark_config = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    # Version Control
    version = Column(Integer, nullable=False, server_default=text("1"))
    is_published = Column(Boolean, nullable=False, server_default=text("false"))
    
    # Validation
    is_validated = Column(Boolean, nullable=False, server_default=text("false"))
    validation_errors = Column(JSONB, nullable=True)
    validated_at = Column(DateTime(timezone=True), nullable=True)
    
    # Status
    is_active = Column(Boolean, nullable=False, server_default=text("true"))
    last_executed_at = Column(DateTime(timezone=True), nullable=True)
    
    # Relationships
    dependencies = relationship(
        "TransformationDependency",
        foreign_keys="[TransformationDependency.dependent_pipeline_id]",
        back_populates="dependent_pipeline",
        cascade="all, delete-orphan",
    )
    logs = relationship(
        "TransformationLog",
        back_populates="pipeline",
        cascade="all, delete-orphan",
    )
    
    __table_args__ = (
        UniqueConstraint("sub_tenant_id", "pipeline_name", "version", name="uq_pipeline_version"),
    )
    
    def __repr__(self) -> str:
        return f"<TransformPipeline(name={self.pipeline_name}, type={self.pipeline_type}, version={self.version})>"


class TransformationDependency(BaseModel, TimestampMixin):
    """Pipeline dependency tracking"""
    
    __tablename__ = "transformation_dependencies"
    
    # Primary Key
    dependency_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Foreign Keys
    dependent_pipeline_id = Column(
        UUID(as_uuid=True),
        ForeignKey("transform_pipelines.pipeline_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    depends_on_pipeline_id = Column(
        UUID(as_uuid=True),
        ForeignKey("transform_pipelines.pipeline_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    
    # Dependency Type
    dependency_type = Column(String(50), nullable=False)  # input, trigger
    
    # Relationships
    dependent_pipeline = relationship(
        "TransformPipeline",
        foreign_keys=[dependent_pipeline_id],
        back_populates="dependencies",
    )
    
    __table_args__ = (
        UniqueConstraint("dependent_pipeline_id", "depends_on_pipeline_id", name="uq_pipeline_dependency"),
    )
    
    def __repr__(self) -> str:
        return f"<TransformationDependency(dependent={self.dependent_pipeline_id}, depends_on={self.depends_on_pipeline_id})>"


class TransformationLog(BaseModel, TimestampMixin):
    """Transformation execution logs"""
    
    __tablename__ = "transformation_logs"
    
    # Primary Key
    log_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Foreign Key
    pipeline_id = Column(
        UUID(as_uuid=True),
        ForeignKey("transform_pipelines.pipeline_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    
    # Execution Information
    execution_id = Column(String(255), nullable=False, index=True)
    spark_application_id = Column(String(255), nullable=True)
    
    # Status
    status = Column(String(50), nullable=False)  # running, completed, failed
    started_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    
    # Metrics
    input_records = Column(Integer, nullable=False, server_default=text("0"))
    output_records = Column(Integer, nullable=False, server_default=text("0"))
    execution_time_ms = Column(Integer, nullable=True)
    
    # Error Information
    error_message = Column(Text, nullable=True)
    error_stack_trace = Column(Text, nullable=True)
    
    # Relationships
    pipeline = relationship("TransformPipeline", back_populates="logs")
    
    def __repr__(self) -> str:
        return f"<TransformationLog(pipeline={self.pipeline_id}, status={self.status})>"


class UDFCatalog(BaseModel, TimestampMixin, MultiTenancyMixin):
    """User-defined function catalog"""
    
    __tablename__ = "udf_catalog"
    
    # Primary Key
    udf_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Basic Information
    udf_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    
    # Function Definition
    function_code = Column(Text, nullable=False)
    language = Column(String(50), nullable=False)  # python, scala, java
    return_type = Column(String(100), nullable=False)
    parameters = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    
    # Classification
    category = Column(String(100), nullable=True)  # date, string, math, custom
    tags = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    
    # Validation
    is_validated = Column(Boolean, nullable=False, server_default=text("false"))
    validation_errors = Column(JSONB, nullable=True)
    
    # Status
    is_active = Column(Boolean, nullable=False, server_default=text("true"))
    
    # Relationships
    execution_stats = relationship(
        "UDFExecutionStats",
        back_populates="udf",
        cascade="all, delete-orphan",
    )
    
    __table_args__ = (
        UniqueConstraint("sub_tenant_id", "udf_name", name="uq_udf_name"),
    )
    
    def __repr__(self) -> str:
        return f"<UDFCatalog(name={self.udf_name}, language={self.language})>"


class UDFExecutionStats(BaseModel, TimestampMixin):
    """UDF execution statistics"""
    
    __tablename__ = "udf_execution_stats"
    
    # Primary Key
    stat_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Foreign Key
    udf_id = Column(
        UUID(as_uuid=True),
        ForeignKey("udf_catalog.udf_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    
    # Execution Metrics
    execution_count = Column(Integer, nullable=False, server_default=text("0"))
    total_execution_time_ms = Column(Integer, nullable=False, server_default=text("0"))
    avg_execution_time_ms = Column(Integer, nullable=False, server_default=text("0"))
    max_execution_time_ms = Column(Integer, nullable=False, server_default=text("0"))
    
    # Error Tracking
    error_count = Column(Integer, nullable=False, server_default=text("0"))
    last_error = Column(Text, nullable=True)
    last_error_at = Column(DateTime(timezone=True), nullable=True)
    
    # Time Window
    stats_date = Column(DateTime(timezone=True), nullable=False, server_default=text("CURRENT_DATE"), index=True)
    
    # Relationships
    udf = relationship("UDFCatalog", back_populates="execution_stats")
    
    def __repr__(self) -> str:
        return f"<UDFExecutionStats(udf={self.udf_id}, executions={self.execution_count})>"
