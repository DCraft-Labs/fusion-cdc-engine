"""Test configuration and fixtures for connector tests"""
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy.dialects import sqlite
from sqlalchemy.types import TypeDecorator, CHAR
from fastapi.testclient import TestClient
from uuid import uuid4, UUID as PythonUUID
from datetime import datetime

from app.main import app
from app.database import Base, get_db
from app.models.connector import ConnectorDefinition, ConnectorVersion
from app.models.auth import User, Role, Permission
from app.auth.password import hash_password
from app.auth.jwt import create_access_token


# UUID type for SQLite
class GUID(TypeDecorator):
    """Platform-independent GUID type.
    Uses CHAR(32), storing as stringified hex values.
    """
    impl = CHAR
    cache_ok = True

    def load_dialect_impl(self, dialect):
        return dialect.type_descriptor(CHAR(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'sqlite':
            return str(value).replace('-', '')
        else:
            return str(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        if dialect.name == 'sqlite':
            # Convert 32-char string to UUID
            value = value[:8] + '-' + value[8:12] + '-' + value[12:16] + '-' + value[16:20] + '-' + value[20:]
        return PythonUUID(value)


# Test database setup (in-memory SQLite)
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)

# Replace UUID type with GUID for SQLite
@event.listens_for(Base.metadata, "before_create")
def receive_before_create(target, connection, **kw):
    """Replace PostgreSQL UUID columns with SQLite-compatible GUID"""
    pass
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture(scope="function")
def db_session():
    """Create a test database session"""
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(bind=engine)


@pytest.fixture(scope="function")
def client(db_session):
    """Create a test client with database override"""
    def override_get_db():
        try:
            yield db_session
        finally:
            pass
    
    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


@pytest.fixture
def sample_superuser(db_session):
    """Create a sample superuser"""
    # Create permission
    permission = Permission(
        permission_name="connector_definitions:read",
        display_name="Read Connector Definitions",
        resource="connector_definitions",
        action="read",
        scope="global",
    )
    db_session.add(permission)
    
    # Create role
    role = Role(
        role_name="superadmin",
        display_name="Super Administrator",
        role_level="superadmin",
        is_system_role=True,
    )
    role.permissions.append(permission)
    db_session.add(role)
    
    # Create user
    superuser = User(
        username="admin",
        email="admin@example.com",
        password_hash=hash_password("AdminPassword123!"),
        first_name="Admin",
        last_name="User",
        is_superuser=True,
        is_active=True,
    )
    superuser.roles.append(role)
    db_session.add(superuser)
    db_session.commit()
    db_session.refresh(superuser)
    return superuser


@pytest.fixture
def sample_user(db_session):
    """Create a sample regular user"""
    # Create permission
    permission = Permission(
        permission_name="connector_definitions:read",
        display_name="Read Connector Definitions",
        resource="connector_definitions",
        action="read",
        scope="global",
    )
    db_session.add(permission)
    
    # Create role
    role = Role(
        role_name="user",
        display_name="User",
        role_level="user",
        is_system_role=True,
    )
    role.permissions.append(permission)
    db_session.add(role)
    
    # Create user
    user = User(
        username="testuser",
        email="test@example.com",
        password_hash=hash_password("TestPassword123!"),
        first_name="Test",
        last_name="User",
        bank_id=uuid4(),
        sub_tenant_id=uuid4(),
        is_superuser=False,
        is_active=True,
    )
    user.roles.append(role)
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


@pytest.fixture
def admin_token(sample_superuser):
    """Generate admin access token"""
    return create_access_token(
        user_id=str(sample_superuser.user_id),
        username=sample_superuser.username,
        roles=["superadmin"],
        permissions=["connector_definitions:read", "connector_definitions:create", "connector_definitions:update", "connector_definitions:delete"],
        is_superuser=True,
    )


@pytest.fixture
def user_token(sample_user):
    """Generate regular user access token"""
    return create_access_token(
        user_id=str(sample_user.user_id),
        username=sample_user.username,
        bank_id=str(sample_user.bank_id),
        sub_tenant_id=str(sample_user.sub_tenant_id),
        roles=["user"],
        permissions=["connector_definitions:read"],
        is_superuser=False,
    )


@pytest.fixture
def admin_headers(admin_token):
    """Generate admin authorization headers"""
    return {"Authorization": f"Bearer {admin_token}"}


@pytest.fixture
def user_headers(user_token):
    """Generate user authorization headers"""
    return {"Authorization": f"Bearer {user_token}"}


@pytest.fixture
def sample_connector(db_session):
    """Create a sample connector definition"""
    connector = ConnectorDefinition(
        connector_name="MySQL Source",
        connector_type="mysql",
        category="source",
        latest_version="1.0.0",
        default_config={"port": 3306},
        required_fields=["host", "port", "database", "username", "password"],
        optional_fields=["ssl", "ssl_ca"],
        default_resource_limits={"max_connections": 10},
        supports_cdc=True,
        supports_full_refresh=True,
        supports_incremental=True,
        documentation_url="https://docs.example.com/mysql",
        icon_url="https://cdn.example.com/mysql.svg",
    )
    db_session.add(connector)
    db_session.commit()
    db_session.refresh(connector)
    return connector


@pytest.fixture
def sample_connector_version(db_session, sample_connector):
    """Create a sample connector version"""
    version = ConnectorVersion(
        connector_id=sample_connector.connector_id,
        version="1.0.0",
        release_notes="Initial release",
        breaking_changes=[],
        new_features=["CDC support", "Full refresh"],
        bug_fixes=[],
        docker_image="fusion/mysql-source",
        docker_tag="1.0.0",
        is_stable=True,
        released_at=datetime(2025, 1, 1),
    )
    db_session.add(version)
    db_session.commit()
    db_session.refresh(version)
    return version


@pytest.fixture
def sample_destination_connector(db_session):
    """Create a sample destination connector"""
    connector = ConnectorDefinition(
        connector_name="PostgreSQL Destination",
        connector_type="postgres",
        category="destination",
        latest_version="1.0.0",
        default_config={"port": 5432, "schema": "public"},
        required_fields=["host", "port", "database", "username", "password"],
        optional_fields=["ssl_mode", "schema"],
        default_resource_limits={"batch_size": 1000},
        supports_cdc=True,
        supports_full_refresh=True,
        supports_incremental=True,
    )
    db_session.add(connector)
    db_session.commit()
    db_session.refresh(connector)
    return connector
