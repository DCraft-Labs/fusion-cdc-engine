"""
Initialize connector definitions
Create default connector definitions for common sources and destinations
"""
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy.orm import Session
from sqlalchemy import select

from app.database import SessionLocal
from app.models.connector import ConnectorDefinition, ConnectorVersion


# Default connector definitions
CONNECTORS = [
    # ========== SOURCE CONNECTORS ==========
    {
        "connector_name": "MySQL Source",
        "connector_type": "mysql",
        "category": "source",
        "latest_version": "1.0.0",
        "default_config": {
            "port": 3306,
            "ssl": False,
            "connection_timeout": 30,
        },
        "required_fields": ["host", "port", "database", "username", "password"],
        "optional_fields": ["ssl", "ssl_ca", "ssl_cert", "ssl_key", "connection_timeout", "include_tables", "exclude_tables"],
        "default_resource_limits": {
            "max_connections": 10,
            "query_timeout": 60,
        },
        "supports_cdc": True,
        "supports_full_refresh": True,
        "supports_incremental": True,
        "documentation_url": "https://docs.dcraftfusion.io/connectors/mysql-source",
        "icon_url": "https://cdn.dcraftfusion.io/icons/mysql.svg",
    },
    {
        "connector_name": "PostgreSQL Source",
        "connector_type": "postgres",
        "category": "source",
        "latest_version": "1.0.0",
        "default_config": {
            "port": 5432,
            "ssl_mode": "prefer",
        },
        "required_fields": ["host", "port", "database", "username", "password"],
        "optional_fields": ["ssl_mode", "schema", "include_tables", "exclude_tables", "replication_slot"],
        "default_resource_limits": {
            "max_connections": 10,
            "query_timeout": 60,
        },
        "supports_cdc": True,
        "supports_full_refresh": True,
        "supports_incremental": True,
        "documentation_url": "https://docs.dcraftfusion.io/connectors/postgres-source",
        "icon_url": "https://cdn.dcraftfusion.io/icons/postgresql.svg",
    },
    {
        "connector_name": "MongoDB Source",
        "connector_type": "mongodb",
        "category": "source",
        "latest_version": "1.0.0",
        "default_config": {
            "port": 27017,
            "auth_source": "admin",
        },
        "required_fields": ["host", "port", "database", "username", "password"],
        "optional_fields": ["auth_source", "replica_set", "include_collections", "exclude_collections"],
        "default_resource_limits": {
            "max_connections": 5,
            "query_timeout": 60,
        },
        "supports_cdc": True,
        "supports_full_refresh": True,
        "supports_incremental": False,
        "documentation_url": "https://docs.dcraftfusion.io/connectors/mongodb-source",
        "icon_url": "https://cdn.dcraftfusion.io/icons/mongodb.svg",
    },
    {
        "connector_name": "Kafka Source",
        "connector_type": "kafka",
        "category": "source",
        "latest_version": "1.0.0",
        "default_config": {
            "bootstrap_servers": "localhost:9092",
            "group_id": "fusion-consumer",
            "auto_offset_reset": "earliest",
        },
        "required_fields": ["bootstrap_servers", "topics"],
        "optional_fields": ["group_id", "auto_offset_reset", "security_protocol", "sasl_mechanism", "sasl_username", "sasl_password"],
        "default_resource_limits": {
            "max_poll_records": 500,
            "session_timeout_ms": 30000,
        },
        "supports_cdc": True,
        "supports_full_refresh": False,
        "supports_incremental": False,
        "documentation_url": "https://docs.dcraftfusion.io/connectors/kafka-source",
        "icon_url": "https://cdn.dcraftfusion.io/icons/kafka.svg",
    },
    {
        "connector_name": "Amazon S3 Source",
        "connector_type": "s3",
        "category": "source",
        "latest_version": "1.0.0",
        "default_config": {
            "region": "us-east-1",
            "format": "json",
        },
        "required_fields": ["bucket", "aws_access_key_id", "aws_secret_access_key"],
        "optional_fields": ["region", "prefix", "format", "compression", "endpoint_url"],
        "default_resource_limits": {
            "max_files_per_sync": 1000,
            "file_size_limit_mb": 100,
        },
        "supports_cdc": False,
        "supports_full_refresh": True,
        "supports_incremental": True,
        "documentation_url": "https://docs.dcraftfusion.io/connectors/s3-source",
        "icon_url": "https://cdn.dcraftfusion.io/icons/s3.svg",
    },
    # ========== DESTINATION CONNECTORS ==========
    {
        "connector_name": "PostgreSQL Destination",
        "connector_type": "postgres",
        "category": "destination",
        "latest_version": "1.0.0",
        "default_config": {
            "port": 5432,
            "ssl_mode": "prefer",
            "schema": "public",
        },
        "required_fields": ["host", "port", "database", "username", "password"],
        "optional_fields": ["ssl_mode", "schema", "ssl_cert", "ssl_key", "ssl_root_cert"],
        "default_resource_limits": {
            "max_connections": 5,
            "batch_size": 1000,
        },
        "supports_cdc": True,
        "supports_full_refresh": True,
        "supports_incremental": True,
        "documentation_url": "https://docs.dcraftfusion.io/connectors/postgres-destination",
        "icon_url": "https://cdn.dcraftfusion.io/icons/postgresql.svg",
    },
    {
        "connector_name": "Snowflake Destination",
        "connector_type": "snowflake",
        "category": "destination",
        "latest_version": "1.0.0",
        "default_config": {
            "warehouse": "COMPUTE_WH",
            "role": "ACCOUNTADMIN",
        },
        "required_fields": ["account", "username", "password", "database", "schema", "warehouse"],
        "optional_fields": ["role", "authentication"],
        "default_resource_limits": {
            "batch_size": 10000,
            "max_parallel_loads": 4,
        },
        "supports_cdc": True,
        "supports_full_refresh": True,
        "supports_incremental": True,
        "documentation_url": "https://docs.dcraftfusion.io/connectors/snowflake-destination",
        "icon_url": "https://cdn.dcraftfusion.io/icons/snowflake.svg",
    },
    {
        "connector_name": "Amazon S3 Destination",
        "connector_type": "s3",
        "category": "destination",
        "latest_version": "1.0.0",
        "default_config": {
            "region": "us-east-1",
            "format": "parquet",
            "compression": "snappy",
        },
        "required_fields": ["bucket", "aws_access_key_id", "aws_secret_access_key"],
        "optional_fields": ["region", "prefix", "format", "compression", "partition_by", "endpoint_url"],
        "default_resource_limits": {
            "max_file_size_mb": 100,
            "batch_size": 10000,
        },
        "supports_cdc": True,
        "supports_full_refresh": True,
        "supports_incremental": True,
        "documentation_url": "https://docs.dcraftfusion.io/connectors/s3-destination",
        "icon_url": "https://cdn.dcraftfusion.io/icons/s3.svg",
    },
    {
        "connector_name": "BigQuery Destination",
        "connector_type": "bigquery",
        "category": "destination",
        "latest_version": "1.0.0",
        "default_config": {
            "location": "US",
            "loading_method": "Standard",
        },
        "required_fields": ["project_id", "dataset_id", "credentials_json"],
        "optional_fields": ["location", "loading_method", "transformation_priority"],
        "default_resource_limits": {
            "batch_size": 10000,
            "max_parallel_loads": 4,
        },
        "supports_cdc": True,
        "supports_full_refresh": True,
        "supports_incremental": True,
        "documentation_url": "https://docs.dcraftfusion.io/connectors/bigquery-destination",
        "icon_url": "https://cdn.dcraftfusion.io/icons/bigquery.svg",
    },
    {
        "connector_name": "Kafka Destination",
        "connector_type": "kafka",
        "category": "destination",
        "latest_version": "1.0.0",
        "default_config": {
            "bootstrap_servers": "localhost:9092",
            "compression_type": "gzip",
            "acks": "all",
        },
        "required_fields": ["bootstrap_servers", "topic_prefix"],
        "optional_fields": ["compression_type", "acks", "security_protocol", "sasl_mechanism", "sasl_username", "sasl_password"],
        "default_resource_limits": {
            "batch_size": 1000,
            "linger_ms": 100,
        },
        "supports_cdc": True,
        "supports_full_refresh": False,
        "supports_incremental": False,
        "documentation_url": "https://docs.dcraftfusion.io/connectors/kafka-destination",
        "icon_url": "https://cdn.dcraftfusion.io/icons/kafka.svg",
    },
]


# Default versions for each connector
def get_default_version(connector_name: str) -> dict:
    """Get default version info for connector"""
    return {
        "version": "1.0.0",
        "release_notes": f"Initial release of {connector_name}",
        "breaking_changes": [],
        "new_features": [
            "Full refresh sync mode",
            "Incremental sync mode",
            "CDC support" if "CDC" in connector_name or "Source" in connector_name else "Batch loading",
        ],
        "bug_fixes": [],
        "docker_image": f"fusion/{connector_name.lower().replace(' ', '-')}",
        "docker_tag": "1.0.0",
        "is_stable": True,
        "released_at": datetime(2025, 1, 1),
    }


def create_connectors(db: Session):
    """Create default connector definitions"""
    print("Creating connector definitions...")
    
    for connector_data in CONNECTORS:
        # Check if connector already exists
        stmt = select(ConnectorDefinition).where(
            ConnectorDefinition.connector_name == connector_data["connector_name"]
        )
        existing = db.execute(stmt).scalar_one_or_none()
        
        if existing:
            print(f"  ✓ Connector '{connector_data['connector_name']}' already exists")
            connector = existing
        else:
            connector = ConnectorDefinition(**connector_data)
            db.add(connector)
            db.flush()
            print(f"  ✓ Created connector '{connector_data['connector_name']}'")
        
        # Create default version
        stmt = select(ConnectorVersion).where(
            ConnectorVersion.connector_id == connector.connector_id,
            ConnectorVersion.version == "1.0.0",
        )
        existing_version = db.execute(stmt).scalar_one_or_none()
        
        if not existing_version:
            version_data = get_default_version(connector_data["connector_name"])
            version = ConnectorVersion(
                connector_id=connector.connector_id,
                **version_data,
            )
            db.add(version)
            print(f"    ✓ Created version 1.0.0 for '{connector_data['connector_name']}'")
    
    db.commit()


def main():
    """Initialize connector definitions"""
    print("=" * 60)
    print("Initializing Connector Definitions")
    print("=" * 60)
    
    db = SessionLocal()
    
    try:
        # Create connectors
        create_connectors(db)
        
        print("\n" + "=" * 60)
        print("✅ Connector definitions initialized successfully!")
        print("=" * 60)
        print(f"\nCreated {len(CONNECTORS)} connector definitions:")
        
        # Group by category
        sources = [c for c in CONNECTORS if c["category"] == "source"]
        destinations = [c for c in CONNECTORS if c["category"] == "destination"]
        
        print(f"\n📥 Sources ({len(sources)}):")
        for c in sources:
            print(f"  - {c['connector_name']} ({c['connector_type']})")
        
        print(f"\n📤 Destinations ({len(destinations)}):")
        for c in destinations:
            print(f"  - {c['connector_name']} ({c['connector_type']})")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
