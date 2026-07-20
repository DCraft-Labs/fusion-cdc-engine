"""Pytest fixtures for Data Quality API tests"""

import pytest
from uuid import uuid4
from datetime import datetime
from typing import Generator
from decimal import Decimal

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

from app.main import app
from app.database import get_db
from app.models.auth import User, Role, Permission, user_roles, role_permissions
from app.models.connector import ConnectorDefinition
from app.models.source_destination import Source, Destination
from app.models.connection import Connection, Stream
from app.models.data_quality import DQPolicy, DQViolation, DQViolationSample, DQRuleResult
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
        session.execute(text("DELETE FROM dq_violation_samples WHERE violated_column LIKE '%Test%'"))
        session.execute(text("DELETE FROM dq_violations WHERE violation_count > 0"))
        session.execute(text("DELETE FROM dq_rule_results WHERE execution_id LIKE '%test%'"))
        session.execute(text("DELETE FROM dq_policies WHERE policy_name LIKE '%Test%'"))
        session.execute(text("DELETE FROM streams WHERE stream_name LIKE '%Test%'"))
        session.execute(text("DELETE FROM connections WHERE connection_name LIKE '%Test%'"))
        session.execute(text("DELETE FROM destinations WHERE destination_name LIKE '%Test%'"))
        session.execute(text("DELETE FROM sources WHERE source_name LIKE '%Test%'"))
        session.execute(text("DELETE FROM connector_definitions WHERE connector_name LIKE '%Test%'"))
        session.execute(text("DELETE FROM users WHERE username LIKE 'test_%'"))
        session.execute(text("DELETE FROM roles WHERE role_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM permissions WHERE permission_name LIKE 'quality_rules:%'"))
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
    """Create sample permissions for data quality management"""
    permissions = [
        Permission(
            permission_id=uuid4(),
            permission_name="quality_rules:create",
            display_name="Create Quality Rules",
            description="Create new quality rules",
            resource="quality_rules",
            action="create",
            scope="tenant",
            is_active=True,
        ),
        Permission(
            permission_id=uuid4(),
            permission_name="quality_rules:read",
            display_name="Read Quality Rules",
            description="View quality rules",
            resource="quality_rules",
            action="read",
            scope="tenant",
            is_active=True,
        ),
        Permission(
            permission_id=uuid4(),
            permission_name="quality_rules:update",
            display_name="Update Quality Rules",
            description="Update quality rules",
            resource="quality_rules",
            action="update",
            scope="tenant",
            is_active=True,
        ),
        Permission(
            permission_id=uuid4(),
            permission_name="quality_rules:delete",
            display_name="Delete Quality Rules",
            description="Delete quality rules",
            resource="quality_rules",
            action="delete",
            scope="tenant",
            is_active=True,
        ),
        Permission(
            permission_id=uuid4(),
            permission_name="quality_rules:execute",
            display_name="Execute Quality Rules",
            description="Execute quality rules",
            resource="quality_rules",
            action="execute",
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
        role_name="Test DQ Admin",
        display_name="DQ Administrator",
        description="Test DQ admin role",
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
        role_name="Test DQ User",
        display_name="DQ User",
        description="Test DQ user role",
        role_level="user",
        is_active=True,
        is_system_role=False,
    )
    db_session.add(role)
    db_session.commit()
    
    # Add only read permission
    read_perm = sample_permissions["quality_rules:read"]
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
        username="test_dq_admin",
        email="test_dq_admin@example.com",
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
        username="test_dq_user",
        email="test_dq_user@example.com",
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
def mysql_source_connector(db_session: Session, sample_tenant: uuid4) -> ConnectorDefinition:
    """Create MySQL Source connector definition"""
    connector = ConnectorDefinition(
        connector_id=uuid4(),
        connector_name="Test MySQL DQ Source",
        connector_type="Source",
        version="1.0.0",
        description="Test MySQL source for DQ testing",
        icon_url="https://example.com/mysql.png",
        documentation_url="https://example.com/docs/mysql",
        config_schema={"type": "object"},
        capabilities={
            "supports_cdc": True,
            "supports_incremental": True,
            "supported_sync_modes": ["cdc", "full_refresh", "incremental"],
        },
        status="active",
        sub_tenant_id=sample_tenant,
        bank_id=uuid4(),
    )
    
    db_session.add(connector)
    db_session.commit()
    
    return connector


@pytest.fixture
def postgres_dest_connector(db_session: Session, sample_tenant: uuid4) -> ConnectorDefinition:
    """Create PostgreSQL Destination connector definition"""
    connector = ConnectorDefinition(
        connector_id=uuid4(),
        connector_name="Test PostgreSQL DQ Destination",
        connector_type="Destination",
        version="1.0.0",
        description="Test PostgreSQL destination for DQ testing",
        icon_url="https://example.com/postgres.png",
        documentation_url="https://example.com/docs/postgres",
        config_schema={"type": "object"},
        capabilities={
            "supported_write_modes": ["append", "replace", "upsert"],
            "supports_batching": True,
        },
        status="active",
        sub_tenant_id=sample_tenant,
        bank_id=uuid4(),
    )
    
    db_session.add(connector)
    db_session.commit()
    
    return connector


@pytest.fixture
def sample_source(
    db_session: Session,
    mysql_source_connector: ConnectorDefinition,
    sample_tenant: uuid4,
) -> Source:
    """Create sample MySQL source"""
    source = Source(
        source_id=uuid4(),
        source_name="Test DQ MySQL Source",
        connector_definition_id=mysql_source_connector.connector_id,
        connector_version="1.0.0",
        host="test-mysql-dq.example.com",
        port=3306,
        database_name="test_dq_db",
        username="test_user",
        password_encrypted="encrypted_password",
        ssl_enabled=False,
        config={},
        status="active",
        connection_test_status="success",
        sub_tenant_id=sample_tenant,
        bank_id=uuid4(),
    )
    
    db_session.add(source)
    db_session.commit()
    
    return source


@pytest.fixture
def sample_destination(
    db_session: Session,
    postgres_dest_connector: ConnectorDefinition,
    sample_tenant: uuid4,
) -> Destination:
    """Create sample PostgreSQL destination"""
    destination = Destination(
        destination_id=uuid4(),
        destination_name="Test DQ PostgreSQL Dest",
        connector_definition_id=postgres_dest_connector.connector_id,
        connector_version="1.0.0",
        host="test-postgres-dq.example.com",
        port=5432,
        database_name="test_dq_dest_db",
        schema_name="public",
        username="test_user",
        password_encrypted="encrypted_password",
        ssl_enabled=True,
        config={},
        status="active",
        connection_test_status="success",
        sub_tenant_id=sample_tenant,
        bank_id=uuid4(),
    )
    
    db_session.add(destination)
    db_session.commit()
    
    return destination


@pytest.fixture
def sample_connection(
    db_session: Session,
    sample_source: Source,
    sample_destination: Destination,
    sample_tenant: uuid4,
) -> Connection:
    """Create sample connection for DQ testing"""
    connection = Connection(
        connection_id=uuid4(),
        connection_name="Test DQ Connection",
        source_id=sample_source.source_id,
        destination_id=sample_destination.destination_id,
        sync_mode="cdc",
        sync_frequency="*/15 * * * *",
        sync_enabled=True,
        resource_limits={},
        config={},
        status="active",
        consecutive_failures=0,
        sub_tenant_id=sample_tenant,
        bank_id=uuid4(),
    )
    
    db_session.add(connection)
    db_session.commit()
    
    return connection


@pytest.fixture
def sample_stream(
    db_session: Session,
    sample_connection: Connection,
) -> Stream:
    """Create sample stream for DQ testing"""
    stream = Stream(
        stream_id=uuid4(),
        connection_id=sample_connection.connection_id,
        stream_name="Test DQ users stream",
        source_table_name="users",
        source_schema_name="public",
        destination_table_name="users",
        destination_schema_name="public",
        sync_mode="cdc",
        primary_keys=["id"],
        selected_columns=None,
        column_mapping={},
        is_enabled=True,
    )
    
    db_session.add(stream)
    db_session.commit()
    
    return stream


@pytest.fixture
def sample_dq_policy(
    db_session: Session,
    sample_connection: Connection,
    sample_stream: Stream,
    sample_tenant: uuid4,
) -> DQPolicy:
    """Create sample DQ policy"""
    policy = DQPolicy(
        policy_id=uuid4(),
        policy_name="Test Null Check Policy",
        description="Test policy for null checking",
        connection_id=sample_connection.connection_id,
        stream_id=sample_stream.stream_id,
        rule_type="null_check",
        rule_definition={"check_type": "not_null"},
        target_columns=["email", "username"],
        severity="error",
        action_on_failure="alert",
        threshold_type="percentage",
        threshold_value=Decimal("5.0"),
        execution_schedule="0 */6 * * *",
        is_active=True,
        sub_tenant_id=sample_tenant,
        bank_id=uuid4(),
    )
    
    db_session.add(policy)
    db_session.commit()
    
    return policy


@pytest.fixture
def sample_violation(
    db_session: Session,
    sample_dq_policy: DQPolicy,
    sample_connection: Connection,
    sample_stream: Stream,
) -> DQViolation:
    """Create sample DQ violation"""
    violation = DQViolation(
        violation_id=uuid4(),
        policy_id=sample_dq_policy.policy_id,
        connection_id=sample_connection.connection_id,
        stream_id=sample_stream.stream_id,
        detected_at=datetime.utcnow(),
        violation_count=50,
        total_records_checked=1000,
        violation_percentage=Decimal("5.0"),
        status="active",
        violation_metadata={"severity": "error"},
    )
    
    db_session.add(violation)
    db_session.commit()
    
    return violation


@pytest.fixture
def sample_violation_sample(
    db_session: Session,
    sample_violation: DQViolation,
) -> DQViolationSample:
    """Create sample violation record"""
    sample = DQViolationSample(
        sample_id=uuid4(),
        violation_id=sample_violation.violation_id,
        record_id="12345",
        record_data={"id": 12345, "email": None, "username": "testuser"},
        violated_column="email",
        expected_value="NOT NULL",
        actual_value="NULL",
        captured_at=datetime.utcnow(),
    )
    
    db_session.add(sample)
    db_session.commit()
    
    return sample


@pytest.fixture
def sample_rule_result(
    db_session: Session,
    sample_dq_policy: DQPolicy,
) -> DQRuleResult:
    """Create sample rule execution result"""
    result = DQRuleResult(
        result_id=uuid4(),
        policy_id=sample_dq_policy.policy_id,
        execution_id="test-exec-123",
        executed_at=datetime.utcnow(),
        passed=False,
        records_checked=1000,
        records_passed=950,
        records_failed=50,
        execution_time_ms=250,
        result_details={"info": "Test execution"},
    )
    
    db_session.add(result)
    db_session.commit()
    
    return result
