"""Pytest fixtures for destination API tests"""

import pytest
from uuid import uuid4
from datetime import datetime
from typing import Generator

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

from app.main import app
from app.database import get_db
from app.models.auth import User, Role, Permission, user_roles, role_permissions
from app.models.connector import ConnectorDefinition
from app.models.source_destination import Destination
from app.auth.jwt import create_access_token

# Test database configuration
TEST_DATABASE_URL = "postgresql://fusion_user:fusion_password@localhost:5432/fusion_cdc_metadata"

# Create test engine
test_engine = create_engine(TEST_DATABASE_URL)
TestSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)


@pytest.fixture(scope="function")
def db_session() -> Generator[Session, None, None]:
    """Create a test database session"""
    session = TestSessionLocal()
    try:
        yield session
    finally:
        # Cleanup test data
        session.execute(text("DELETE FROM destinations WHERE destination_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM connector_definitions WHERE connector_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM role_permissions WHERE role_id IN (SELECT role_id FROM roles WHERE role_name LIKE 'Test%')"))
        session.execute(text("DELETE FROM roles WHERE role_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM permissions WHERE permission_name LIKE 'test:%'"))
        session.execute(text("DELETE FROM users WHERE username LIKE 'test_%'"))
        session.commit()
        session.close()


@pytest.fixture
def client(db_session: Session) -> TestClient:
    """Create a test client with overridden database dependency"""
    
    def override_get_db():
        try:
            yield db_session
        finally:
            pass
    
    app.dependency_overrides[get_db] = override_get_db
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


@pytest.fixture
def sample_tenant() -> uuid4:
    """Create a sample tenant UUID"""
    return uuid4()


@pytest.fixture
def sample_permissions(db_session: Session) -> dict:
    """Create sample permissions for destination management"""
    permissions = [
        Permission(
            permission_id=uuid4(),
            permission_name="destinations:create",
            display_name="Create Destinations",
            description="Create new destinations",
            resource="destinations",
            action="create",
            scope="tenant",
            is_active=True,
        ),
        Permission(
            permission_id=uuid4(),
            permission_name="destinations:read",
            display_name="Read Destinations",
            description="View destinations",
            resource="destinations",
            action="read",
            scope="tenant",
            is_active=True,
        ),
        Permission(
            permission_id=uuid4(),
            permission_name="destinations:update",
            display_name="Update Destinations",
            description="Update destinations",
            resource="destinations",
            action="update",
            scope="tenant",
            is_active=True,
        ),
        Permission(
            permission_id=uuid4(),
            permission_name="destinations:delete",
            display_name="Delete Destinations",
            description="Delete destinations",
            resource="destinations",
            action="delete",
            scope="tenant",
            is_active=True,
        ),
    ]
    
    for perm in permissions:
        db_session.add(perm)
    db_session.commit()
    for perm in permissions:
        db_session.refresh(perm)
    
    return {perm.permission_name: perm for perm in permissions}


@pytest.fixture
def admin_role(db_session: Session, sample_permissions: dict) -> Role:
    """Create admin role with all permissions"""
    role = Role(
        role_id=uuid4(),
        role_name="Test Admin",
        display_name="Administrator",
        description="Test admin role",
        role_level="tenant_admin",
        is_active=True,
        is_system_role=False,
    )
    db_session.add(role)
    db_session.commit()
    
    # Add all permissions
    for perm in sample_permissions.values():
        db_session.execute(
            role_permissions.insert().values(
                role_id=role.role_id,
                permission_id=perm.permission_id
            )
        )
    db_session.commit()
    db_session.refresh(role)
    
    return role


@pytest.fixture
def user_role(db_session: Session, sample_permissions: dict) -> Role:
    """Create user role with read-only permissions"""
    role = Role(
        role_id=uuid4(),
        role_name="Test User",
        display_name="User",
        description="Test user role",
        role_level="user",
        is_active=True,
        is_system_role=False,
    )
    db_session.add(role)
    db_session.commit()
    
    # Add only read permission
    read_perm = sample_permissions["destinations:read"]
    db_session.execute(
        role_permissions.insert().values(
            role_id=role.role_id,
            permission_id=read_perm.permission_id
        )
    )
    db_session.commit()
    db_session.refresh(role)
    
    return role


@pytest.fixture
def admin_user(db_session: Session, admin_role: Role, sample_tenant: uuid4) -> User:
    """Create test admin user"""
    user = User(
        user_id=uuid4(),
        username="test_admin",
        email="test_admin@example.com",
        password_hash="hashed_password",
        is_active=True,
        is_superuser=True,
        sub_tenant_id=sample_tenant,
        bank_id=uuid4(),
    )
    db_session.add(user)
    db_session.commit()
    
    # Assign admin role
    db_session.execute(
        user_roles.insert().values(
            user_id=user.user_id,
            role_id=admin_role.role_id
        )
    )
    db_session.commit()
    db_session.refresh(user)
    
    return user


@pytest.fixture
def regular_user(db_session: Session, user_role: Role, sample_tenant: uuid4) -> User:
    """Create test regular user"""
    user = User(
        user_id=uuid4(),
        username="test_user",
        email="test_user@example.com",
        password_hash="hashed_password",
        is_active=True,
        is_superuser=False,
        sub_tenant_id=sample_tenant,
        bank_id=uuid4(),
    )
    db_session.add(user)
    db_session.commit()
    
    # Assign user role
    db_session.execute(
        user_roles.insert().values(
            user_id=user.user_id,
            role_id=user_role.role_id
        )
    )
    db_session.commit()
    db_session.refresh(user)
    
    return user


@pytest.fixture
def admin_token(admin_user: User) -> str:
    """Create JWT token for admin user"""
    return create_access_token(data={"sub": str(admin_user.user_id)})


@pytest.fixture
def user_token(regular_user: User) -> str:
    """Create JWT token for regular user"""
    return create_access_token(data={"sub": str(regular_user.user_id)})


@pytest.fixture
def admin_headers(admin_token: str) -> dict:
    """Create auth headers for admin user"""
    return {"Authorization": f"Bearer {admin_token}"}


@pytest.fixture
def user_headers(user_token: str) -> dict:
    """Create auth headers for regular user"""
    return {"Authorization": f"Bearer {user_token}"}


@pytest.fixture
def sample_connector_definition(db_session: Session, sample_tenant: uuid4) -> ConnectorDefinition:
    """Create sample PostgreSQL Destination connector definition"""
    connector = ConnectorDefinition(
        connector_id=uuid4(),
        connector_name="Test PostgreSQL Destination",
        connector_type="Destination",
        version="1.0.0",
        description="Test PostgreSQL destination connector",
        icon_url="https://example.com/postgres.png",
        documentation_url="https://example.com/docs/postgres",
        config_schema={
            "type": "object",
            "properties": {
                "host": {"type": "string"},
                "port": {"type": "integer"},
                "database": {"type": "string"},
                "schema": {"type": "string"},
                "username": {"type": "string"},
                "password": {"type": "string"},
            },
        },
        capabilities={
            "supported_write_modes": ["append", "replace", "upsert"],
            "supports_batching": True,
            "max_batch_size": 10000,
        },
        status="active",
        sub_tenant_id=sample_tenant,
        bank_id=uuid4(),
    )
    
    db_session.add(connector)
    db_session.commit()
    
    return connector


@pytest.fixture
def s3_connector_definition(db_session: Session, sample_tenant: uuid4) -> ConnectorDefinition:
    """Create sample S3 Destination connector definition"""
    connector = ConnectorDefinition(
        connector_id=uuid4(),
        connector_name="Test S3 Destination",
        connector_type="Destination",
        version="1.0.0",
        description="Test S3 destination connector",
        icon_url="https://example.com/s3.png",
        documentation_url="https://example.com/docs/s3",
        config_schema={
            "type": "object",
            "properties": {
                "bucket_name": {"type": "string"},
                "region": {"type": "string"},
                "path_prefix": {"type": "string"},
                "output_format": {"type": "string"},
            },
        },
        capabilities={
            "supported_write_modes": ["append", "replace"],
            "supports_batching": True,
            "max_batch_size": 50000,
            "supported_formats": ["parquet", "json", "csv"],
        },
        status="active",
        sub_tenant_id=sample_tenant,
        bank_id=uuid4(),
    )
    
    db_session.add(connector)
    db_session.commit()
    
    return connector


@pytest.fixture
def sample_destination(
    db_session: Session,
    sample_connector_definition: ConnectorDefinition,
    sample_tenant: uuid4,
) -> Destination:
    """Create sample destination"""
    destination = Destination(
        destination_id=uuid4(),
        destination_name="Test PostgreSQL Dest",
        connector_definition_id=sample_connector_definition.connector_id,
        connector_version="1.0.0",
        host="test-postgres.example.com",
        port=5432,
        database_name="test_db",
        schema_name="public",
        username="test_user",
        password_encrypted="encrypted_test_password",
        ssl_enabled=True,
        ssl_config={"verify": True},
        config={},
        status="active",
        sub_tenant_id=sample_tenant,
        bank_id=uuid4(),
    )
    
    db_session.add(destination)
    db_session.commit()
    
    return destination


@pytest.fixture
def sample_s3_destination(
    db_session: Session,
    s3_connector_definition: ConnectorDefinition,
    sample_tenant: uuid4,
) -> Destination:
    """Create sample S3 destination"""
    destination = Destination(
        destination_id=uuid4(),
        destination_name="Test S3 Dest",
        connector_definition_id=s3_connector_definition.connector_id,
        connector_version="1.0.0",
        bucket_name="test-bucket",
        region="us-east-1",
        path_prefix="data/",
        output_format="parquet",
        compression="snappy",
        config={},
        status="active",
        sub_tenant_id=sample_tenant,
        bank_id=uuid4(),
    )
    
    db_session.add(destination)
    db_session.commit()
    
    return destination
