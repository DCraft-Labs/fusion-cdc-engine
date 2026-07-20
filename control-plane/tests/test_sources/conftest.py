"""
Test fixtures for source API tests
"""
import pytest
from datetime import datetime
from uuid import uuid4
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient

from app.main import app
from app.database import get_db, Base
from app.models.auth import User, Role, Permission, user_roles, role_permissions
from app.models.connector import ConnectorDefinition
from app.models.source_destination import Source
from app.auth.password import hash_password
from app.auth.jwt import create_access_token


# Test database URL (PostgreSQL required for UUID support)
# Using main database for testing (schema is already created)
TEST_DATABASE_URL = "postgresql://fusion_user:fusion_password@localhost:5432/fusion_cdc_metadata"


@pytest.fixture(scope="function")
def db_session():
    """Create a test database session (using existing schema)"""
    engine = create_engine(TEST_DATABASE_URL)
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    session = TestingSessionLocal()
    try:
        yield session
        # Roll back any changes after test
        session.rollback()
    finally:
        # Clean up test data
        session.execute(text("DELETE FROM sources WHERE source_name LIKE '%Test%' OR source_name LIKE '%New%'"))
        session.execute(text("DELETE FROM users WHERE username IN ('admin', 'user')"))
        session.execute(text("DELETE FROM roles WHERE role_name IN ('admin', 'user')"))
        session.execute(text("DELETE FROM permissions WHERE permission_name LIKE 'sources:%'"))
        session.execute(text("DELETE FROM connector_definitions WHERE connector_name LIKE '%Test%'"))
        session.commit()
        session.close()


@pytest.fixture(scope="function")
def client(db_session):
    """Create a test client with overridden database dependency"""
    def override_get_db():
        try:
            yield db_session
        finally:
            pass
    
    app.dependency_overrides[get_db] = override_get_db
    
    with TestClient(app) as test_client:
        yield test_client
    
    app.dependency_overrides.clear()


@pytest.fixture(scope="function")
def sample_tenant():
    """Create a sample tenant ID (simplified without SubTenant model)"""
    return uuid4()


@pytest.fixture(scope="function")
def sample_permissions(db_session):
    """Create sample permissions"""
    permissions = [
        Permission(
            permission_id=uuid4(),
            permission_name="sources:create",
            display_name="Create Sources",
            description="Create new sources",
            resource="sources",
            action="create",
            scope="tenant",
            is_active=True
        ),
        Permission(
            permission_id=uuid4(),
            permission_name="sources:read",
            display_name="Read Sources",
            description="View sources",
            resource="sources",
            action="read",
            scope="tenant",
            is_active=True
        ),
        Permission(
            permission_id=uuid4(),
            permission_name="sources:update",
            display_name="Update Sources",
            description="Update sources",
            resource="sources",
            action="update",
            scope="tenant",
            is_active=True
        ),
        Permission(
            permission_id=uuid4(),
            permission_name="sources:delete",
            display_name="Delete Sources",
            description="Delete sources",
            resource="sources",
            action="delete",
            scope="tenant",
            is_active=True
        ),
    ]
    for perm in permissions:
        db_session.add(perm)
    db_session.commit()
    for perm in permissions:
        db_session.refresh(perm)
    return {perm.permission_name: perm for perm in permissions}


@pytest.fixture(scope="function")
def admin_role(db_session, sample_permissions):
    """Create admin role with all permissions"""
    role = Role(
        role_id=uuid4(),
        role_name="admin",
        display_name="Administrator",
        description="Full access",
        role_level="tenant_admin",
        is_active=True,
        is_system_role=True
    )
    db_session.add(role)
    db_session.commit()
    
    # Assign all permissions
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


@pytest.fixture(scope="function")
def user_role(db_session, sample_permissions):
    """Create user role with read-only permissions"""
    role = Role(
        role_id=uuid4(),
        role_name="user",
        display_name="User",
        description="Read-only access",
        role_level="user",
        is_active=True,
        is_system_role=True
    )
    db_session.add(role)
    db_session.commit()
    
    # Assign only read permission
    db_session.execute(
        role_permissions.insert().values(
            role_id=role.role_id,
            permission_id=sample_permissions["sources:read"].permission_id
        )
    )
    db_session.commit()
    db_session.refresh(role)
    return role


@pytest.fixture(scope="function")
def admin_user(db_session, sample_tenant, admin_role):
    """Create an admin user"""
    user = User(
        user_id=uuid4(),
        username="admin",
        email="admin@test.com",
        password_hash=hash_password("admin123"),
        full_name="Admin User",
        sub_tenant_id=sample_tenant,
        is_active=True,
        is_superuser=True,
        email_verified=True
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


@pytest.fixture(scope="function")
def regular_user(db_session, sample_tenant, user_role):
    """Create a regular user"""
    user = User(
        user_id=uuid4(),
        username="user",
        email="user@test.com",
        password_hash=hash_password("user123"),
        full_name="Regular User",
        sub_tenant_id=sample_tenant,
        is_active=True,
        is_superuser=False,
        email_verified=True
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


@pytest.fixture(scope="function")
def admin_token(admin_user):
    """Generate JWT token for admin user"""
    return create_access_token({"sub": str(admin_user.user_id)})


@pytest.fixture(scope="function")
def user_token(regular_user):
    """Generate JWT token for regular user"""
    return create_access_token({"sub": str(regular_user.user_id)})


@pytest.fixture(scope="function")
def admin_headers(admin_token):
    """Create authorization headers for admin"""
    return {"Authorization": f"Bearer {admin_token}"}


@pytest.fixture(scope="function")
def user_headers(user_token):
    """Create authorization headers for regular user"""
    return {"Authorization": f"Bearer {user_token}"}


@pytest.fixture(scope="function")
def sample_connector_definition(db_session):
    """Create a sample connector definition"""
    connector = ConnectorDefinition(
        connector_id=uuid4(),
        connector_name="MySQL Source",
        connector_type="mysql",
        category="source",
        latest_version="1.0.0",
        default_config={"port": 3306, "ssl": False},
        required_fields=["host", "port", "database", "username", "password"],
        optional_fields=["ssl", "ssl_ca"],
        default_resource_limits={"max_connections": 10},
        supports_cdc=True,
        supports_full_refresh=True,
        supports_incremental=True,
        documentation_url="https://docs.dcraftfusion.io/connectors/mysql-source",
        is_active=True
    )
    db_session.add(connector)
    db_session.commit()
    db_session.refresh(connector)
    return connector


@pytest.fixture(scope="function")
def sample_source(db_session, sample_tenant, sample_connector_definition, admin_user):
    """Create a sample source"""
    source = Source(
        source_id=uuid4(),
        source_name="Test MySQL Source",
        connector_definition_id=sample_connector_definition.connector_id,
        connector_version="1.0.0",
        host="localhost",
        port=3306,
        database_name="testdb",
        username="testuser",
        password_encrypted="encrypted_testpass",
        ssl_enabled=False,
        ssl_config={},
        config={},
        status="draft",
        sub_tenant_id=sample_tenant,
        created_by=admin_user.user_id,
        is_deleted=False
    )
    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)
    return source
