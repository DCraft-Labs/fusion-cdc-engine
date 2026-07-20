"""
Initialize connector definitions
Seeds the database with built-in source and destination connector types.
"""
import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy.orm import Session
from sqlalchemy import select
from app.database import SessionLocal
from app.models.connector import ConnectorDefinition, ConnectorVersion


# ============================================================================
# Source Connector Definitions
# ============================================================================

SOURCE_CONNECTORS = [
    {
        "connector_name": "MySQL CDC",
        "connector_type": "mysql",
        "category": "source",
        "latest_version": "1.0.0",
        "supports_cdc": True,
        "supports_full_refresh": True,
        "supports_incremental": True,
        "documentation_url": "https://docs.fusion.dev/connectors/mysql",
        "icon_url": "/icons/mysql.svg",
        "required_fields": ["host", "port", "database_name", "username", "password"],
        "optional_fields": ["ssl_enabled", "ssl_config", "server_id", "binlog_position", "gtid_mode"],
        "default_config": {
            "port": 3306,
            "ssl_enabled": False,
            "server_id": 1,
            "gtid_mode": False,
            "snapshot_mode": "initial",
            "binlog_format": "ROW",
            "max_batch_size": 10000,
        },
        "default_resource_limits": {
            "max_memory_mb": 512,
            "max_connections": 5,
            "batch_size": 10000,
        },
        "version_info": {
            "version": "1.0.0",
            "release_notes": "Initial release with Binlog CDC, GTID support, and full refresh modes.",
            "new_features": ["Binlog-based CDC", "GTID support", "Full refresh mode", "Incremental snapshots"],
            "is_stable": True,
        },
    },
    {
        "connector_name": "MongoDB Change Streams",
        "connector_type": "mongodb",
        "category": "source",
        "latest_version": "1.0.0",
        "supports_cdc": True,
        "supports_full_refresh": True,
        "supports_incremental": True,
        "documentation_url": "https://docs.fusion.dev/connectors/mongodb",
        "icon_url": "/icons/mongodb.svg",
        "required_fields": ["host", "port", "database_name", "username", "password"],
        "optional_fields": ["ssl_enabled", "ssl_config", "replica_set", "auth_source", "auth_mechanism"],
        "default_config": {
            "port": 27017,
            "ssl_enabled": False,
            "auth_source": "admin",
            "auth_mechanism": "SCRAM-SHA-256",
            "read_preference": "secondaryPreferred",
            "max_batch_size": 10000,
        },
        "default_resource_limits": {
            "max_memory_mb": 512,
            "max_connections": 5,
            "batch_size": 10000,
        },
        "version_info": {
            "version": "1.0.0",
            "release_notes": "Initial release with Change Streams CDC and full collection scans.",
            "new_features": ["Change Streams CDC", "Resume token tracking", "Full refresh mode", "Document flattening"],
            "is_stable": True,
        },
    },
    {
        "connector_name": "PostgreSQL Logical Replication",
        "connector_type": "postgresql",
        "category": "source",
        "latest_version": "1.0.0",
        "supports_cdc": True,
        "supports_full_refresh": True,
        "supports_incremental": True,
        "documentation_url": "https://docs.fusion.dev/connectors/postgresql",
        "icon_url": "/icons/postgresql.svg",
        "required_fields": ["host", "port", "database_name", "username", "password"],
        "optional_fields": ["ssl_enabled", "ssl_config", "replication_slot", "publication_name", "schema_filter"],
        "default_config": {
            "port": 5432,
            "ssl_enabled": False,
            "replication_slot": "fusion_slot",
            "publication_name": "fusion_pub",
            "wal_level": "logical",
            "max_batch_size": 10000,
        },
        "default_resource_limits": {
            "max_memory_mb": 512,
            "max_connections": 5,
            "batch_size": 10000,
        },
        "version_info": {
            "version": "1.0.0",
            "release_notes": "Initial release with logical replication and WAL-based CDC.",
            "new_features": ["Logical replication CDC", "Publication-based filtering", "Full refresh mode", "WAL position tracking"],
            "is_stable": True,
        },
    },
    {
        "connector_name": "Polling (REST API)",
        "connector_type": "polling",
        "category": "source",
        "latest_version": "1.0.0",
        "supports_cdc": False,
        "supports_full_refresh": True,
        "supports_incremental": True,
        "documentation_url": "https://docs.fusion.dev/connectors/polling",
        "icon_url": "/icons/rest-api.svg",
        "required_fields": ["host", "port", "database_name", "username", "password"],
        "optional_fields": ["cursor_field", "poll_interval_seconds", "api_key", "headers"],
        "default_config": {
            "port": 443,
            "ssl_enabled": True,
            "poll_interval_seconds": 300,
            "cursor_field": "updated_at",
            "max_batch_size": 1000,
        },
        "default_resource_limits": {
            "max_memory_mb": 256,
            "max_connections": 3,
            "batch_size": 1000,
        },
        "version_info": {
            "version": "1.0.0",
            "release_notes": "Initial release with timestamp-based incremental polling.",
            "new_features": ["Timestamp-based polling", "Cursor tracking", "Full refresh mode"],
            "is_stable": True,
        },
    },
]


# ============================================================================
# Destination Connector Definitions
# ============================================================================

DESTINATION_CONNECTORS = [
    {
        "connector_name": "PostgreSQL Destination",
        "connector_type": "postgresql",
        "category": "destination",
        "latest_version": "1.0.0",
        "supports_cdc": True,
        "supports_full_refresh": True,
        "supports_incremental": True,
        "documentation_url": "https://docs.fusion.dev/connectors/postgresql-dest",
        "icon_url": "/icons/postgresql.svg",
        "required_fields": ["host", "port", "database_name", "username", "password"],
        "optional_fields": ["ssl_enabled", "ssl_config", "schema_name", "write_mode", "batch_size"],
        "default_config": {
            "port": 5432,
            "ssl_enabled": False,
            "schema_name": "public",
            "write_mode": "scd1",
            "batch_size": 5000,
            "create_table_if_missing": True,
        },
        "default_resource_limits": {
            "max_memory_mb": 512,
            "max_connections": 10,
            "batch_size": 5000,
        },
        "version_info": {
            "version": "1.0.0",
            "release_notes": "Initial release with SCD1/SCD2 write modes and upsert support.",
            "new_features": ["SCD1 overwrite", "SCD2 history tracking", "Upsert support", "Auto table creation"],
            "is_stable": True,
        },
    },
    {
        "connector_name": "Apache Iceberg",
        "connector_type": "iceberg",
        "category": "destination",
        "latest_version": "1.0.0",
        "supports_cdc": True,
        "supports_full_refresh": True,
        "supports_incremental": True,
        "documentation_url": "https://docs.fusion.dev/connectors/iceberg",
        "icon_url": "/icons/iceberg.svg",
        "required_fields": ["catalog_type", "catalog_name", "namespace"],
        "optional_fields": ["storage_type", "container", "warehouse_path", "s3_endpoint", "partition_spec"],
        "default_config": {
            "catalog_type": "nessie",
            "storage_type": "azure",
            "file_format": "parquet",
            "partition_spec": {},
            "merge_on_read": True,
        },
        "default_resource_limits": {
            "max_memory_mb": 1024,
            "max_connections": 5,
            "batch_size": 50000,
        },
        "version_info": {
            "version": "1.0.0",
            "release_notes": "Initial release with Nessie catalog and Azure/S3 storage support.",
            "new_features": ["Nessie catalog", "Azure Blob storage", "S3 storage", "Parquet format", "Merge-on-read"],
            "is_stable": True,
        },
    },
]


def seed_connectors(db: Session):
    """Seed all connector definitions and their initial versions."""
    all_connectors = SOURCE_CONNECTORS + DESTINATION_CONNECTORS

    print("=" * 60)
    print("Seeding Connector Definitions")
    print("=" * 60)

    for connector_data in all_connectors:
        version_info = connector_data.pop("version_info")

        # Check if connector already exists
        stmt = select(ConnectorDefinition).where(
            ConnectorDefinition.connector_name == connector_data["connector_name"]
        )
        existing = db.execute(stmt).scalar_one_or_none()

        if existing:
            print(f"  ✓ Connector '{connector_data['connector_name']}' already exists — skipping")
            # Ensure version exists
            _ensure_version(db, existing.connector_id, version_info)
            continue

        # Create connector definition
        connector = ConnectorDefinition(**connector_data)
        db.add(connector)
        db.flush()

        # Create initial version
        version = ConnectorVersion(
            connector_id=connector.connector_id,
            version=version_info["version"],
            release_notes=version_info.get("release_notes"),
            new_features=version_info.get("new_features", []),
            is_stable=version_info.get("is_stable", True),
            released_at=datetime.now(timezone.utc),
        )
        db.add(version)
        db.flush()

        print(f"  ✓ Created connector '{connector_data['connector_name']}' ({connector_data['category']})")

    db.commit()
    print(f"\n✅ Seeded {len(all_connectors)} connector definitions")


def _ensure_version(db: Session, connector_id, version_info: dict):
    """Ensure a version exists for a connector."""
    stmt = select(ConnectorVersion).where(
        ConnectorVersion.connector_id == connector_id,
        ConnectorVersion.version == version_info["version"],
    )
    existing = db.execute(stmt).scalar_one_or_none()
    if not existing:
        version = ConnectorVersion(
            connector_id=connector_id,
            version=version_info["version"],
            release_notes=version_info.get("release_notes"),
            new_features=version_info.get("new_features", []),
            is_stable=version_info.get("is_stable", True),
            released_at=datetime.now(timezone.utc),
        )
        db.add(version)
        db.flush()


def main():
    """Run connector seeding."""
    db = SessionLocal()
    try:
        seed_connectors(db)
    except Exception as e:
        db.rollback()
        print(f"\n❌ Error seeding connectors: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
