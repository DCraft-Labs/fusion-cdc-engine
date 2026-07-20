"""
SQLAlchemy ORM Models for Fusion CDC Platform
Complete model definitions for all 42 database tables
"""

# Base classes and mixins
from app.models.base import (
    TimestampMixin,
    SoftDeleteMixin,
    MultiTenancyMixin,
    BaseModel,
)

# Connector models
from app.models.connector import (
    ConnectorDefinition,
    ConnectorVersion,
)

# Source and Destination models
from app.models.source_destination import (
    Source,
    Destination,
)

# Connection models
from app.models.connection import (
    Connection,
    Stream,
    SyncModeConfig,
)

# Monitoring models
from app.models.monitoring import (
    CheckpointState,
    CDCPositionHistory,
    CDCLagMetrics,
    WorkerHeartbeat,
    RedisStreamTracking,
    ConnectionRun,
    ConnectionHealthCheck,
    ConnectionAlertWebhook,
)

# Transformation models
from app.models.transformation import (
    TransformPipeline,
    TransformationDependency,
    TransformationLog,
    UDFCatalog,
    UDFExecutionStats,
)

# Data Quality models
from app.models.data_quality import (
    DQPolicy,
    DQViolation,
    DQViolationSample,
    DQRuleResult,
)

# Schema Evolution models
from app.models.schema_evolution import (
    SchemaChangeEvent,
    JSONSchemaCache,
    JSONSchemaEvolution,
    JSONFlattenRule,
)

# Spark models
from app.models.spark import (
    SparkJobQueue,
    SparkApplication,
    SparkExecutor,
    SparkExecutorHistory,
)

# System models
from app.models.system import (
    SystemConfig,
    FeatureFlag,
    Alert,
    MaintenanceWindow,
    EventDeadLetterQueue,
    EventDLQRetryHistory,
    ResourceUsage,
    ResourceQuotaViolation,
    TenantDailyUsage,
)

# Authentication models
from app.models.auth import (
    User,
    Role,
    Permission,
    RefreshToken,
    AuditLog,
)

__all__ = [
    # Base
    "TimestampMixin",
    "SoftDeleteMixin",
    "MultiTenancyMixin",
    "BaseModel",
    # Connectors
    "ConnectorDefinition",
    "ConnectorVersion",
    # Sources & Destinations
    "Source",
    "Destination",
    # Connections
    "Connection",
    "Stream",
    "SyncModeConfig",
    # Monitoring
    "CheckpointState",
    "CDCPositionHistory",
    "CDCLagMetrics",
    "WorkerHeartbeat",
    "RedisStreamTracking",
    "ConnectionRun",
    "ConnectionHealthCheck",
    "ConnectionAlertWebhook",
    # Transformations
    "TransformPipeline",
    "TransformationDependency",
    "TransformationLog",
    "UDFCatalog",
    "UDFExecutionStats",
    # Data Quality
    "DQPolicy",
    "DQViolation",
    "DQViolationSample",
    "DQRuleResult",
    # Schema Evolution
    "SchemaChangeEvent",
    "JSONSchemaCache",
    "JSONSchemaEvolution",
    "JSONFlattenRule",
    # Spark
    "SparkJobQueue",
    "SparkApplication",
    "SparkExecutor",
    "SparkExecutorHistory",
    # System
    "SystemConfig",
    "FeatureFlag",
    "Alert",
    "MaintenanceWindow",
    "EventDeadLetterQueue",
    "EventDLQRetryHistory",
    "ResourceUsage",
    "ResourceQuotaViolation",
    "TenantDailyUsage",
    # Authentication
    "User",
    "Role",
    "Permission",
    "RefreshToken",
    "AuditLog",
]
