"""
Test fixtures for alerting tests
"""
import pytest
from uuid import uuid4
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient
from typing import Optional, Dict, Any

from app.main import app
from app.database import get_db
from app.models.connector import ConnectorDefinition
from app.models.source_destination import Source, Destination
from app.models.connection import Connection, Stream
from app.models.alerting import (
    NotificationChannel, AlertRule, AlertRuleChannel, 
    AlertEscalationPolicy, AlertEvaluation, AlertSuppression
)
from app.models.system import Alert
from app.models import alerting as alerting_models


# Mock JWT token for testing (bypasses actual auth)
MOCK_ADMIN_TOKEN = "mock_admin_token_for_testing"
MOCK_USER_TOKEN = "mock_user_token_for_testing"


# Test database configuration
TEST_DATABASE_URL = "postgresql://fusion_user:fusion_password@localhost:5432/fusion_master"

engine = create_engine(TEST_DATABASE_URL)
TestSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture(scope="function")
def db_session():
    """Create a new database session for testing"""
    session = TestSessionLocal()
    
    try:
        yield session
    finally:
        # Cleanup test data
        session.execute(text("DELETE FROM alert_notification_logs WHERE log_id IS NOT NULL"))
        session.execute(text("DELETE FROM alert_history WHERE history_id IS NOT NULL"))
        session.execute(text("DELETE FROM alert_evaluations WHERE evaluation_id IS NOT NULL"))
        session.execute(text("DELETE FROM alert_escalation_policies WHERE policy_id IS NOT NULL"))
        session.execute(text("DELETE FROM alert_rule_channels WHERE rule_id IS NOT NULL"))
        session.execute(text("DELETE FROM alert_rules WHERE rule_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM notification_channels WHERE channel_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM alerts WHERE title LIKE 'Test%'"))
        session.execute(text("DELETE FROM alert_suppressions WHERE suppression_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM streams WHERE stream_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM connections WHERE connection_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM sources WHERE source_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM destinations WHERE destination_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM connector_definitions WHERE connector_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM user_roles WHERE user_id IN (SELECT user_id FROM users WHERE username LIKE 'test_alert_%')"))
        session.execute(text("DELETE FROM users WHERE username LIKE 'test_alert_%'"))
        session.execute(text("DELETE FROM permissions WHERE permission_name LIKE 'alerts:%'"))
        session.execute(text("DELETE FROM roles WHERE role_name LIKE 'Test Alert%'"))
        session.commit()
        session.close()


@pytest.fixture(scope="function")
def client(db_session):
    """Create test client with database session override"""
    def override_get_db():
        try:
            yield db_session
        finally:
            pass
    
    app.dependency_overrides[get_db] = override_get_db
    
    with TestClient(app) as test_client:
        yield test_client
    
    app.dependency_overrides.clear()


# ============================================================================
# Auth Fixtures
# ============================================================================

@pytest.fixture(scope="function")
def sample_tenant():
    """Sample tenant UUID"""
    return uuid4()


@pytest.fixture(scope="function")
def sample_permissions(db_session):
    """Create sample permissions for alerting"""
    permissions = [
        Permission(
            code="alerts:create",
            name="Create Alerts",
            description="Create alerts and rules",
            category="GENERAL",
            is_active=True,
            display_order=1,
        ),
        Permission(
            code="alerts:read",
            name="View Alerts",
            description="View alerts and rules",
            category="GENERAL",
            is_active=True,
            display_order=2,
        ),
        Permission(
            code="alerts:update",
            name="Update Alerts",
            description="Update alerts and rules",
            category="GENERAL",
            is_active=True,
            display_order=3,
        ),
        Permission(
            code="alerts:delete",
            name="Delete Alerts",
            description="Delete alerts and rules",
            category="GENERAL",
            is_active=True,
            display_order=4,
        ),
        Permission(
            code="alerts:execute",
            name="Execute Alert Tests",
            description="Execute alert tests",
            category="GENERAL",
            is_active=True,
            display_order=5,
        ),
    ]
    
    for perm in permissions:
        db_session.add(perm)
    
    db_session.commit()
    
    return {perm.code: perm for perm in permissions}


@pytest.fixture(scope="function")
def admin_role(db_session, sample_permissions):
    """Create admin role with all alert permissions"""
    role = Role(
        role_name="Test Alert Admin",
        description="Test role with all alert permissions",
    )
    
    db_session.add(role)
    db_session.flush()
    
    # Add all permissions
    for perm in sample_permissions.values():
        role.permissions.append(perm)
    
    db_session.commit()
    
    return role


@pytest.fixture(scope="function")
def user_role(db_session, sample_permissions):
    """Create user role with read-only permissions"""
    role = Role(
        role_name="Test Alert User",
        description="Test role with read-only alert permissions",
    )
    
    db_session.add(role)
    db_session.flush()
    
    # Add only read permission
    role.permissions.append(sample_permissions["alerts:read"])
    
    db_session.commit()
    
    return role


@pytest.fixture(scope="function")
def admin_user(db_session, admin_role, sample_tenant):
    """Create admin user with all permissions"""
    user = User(
        username="test_alert_admin",
        email="alert_admin@test.com",
        password_hash="hashed_password",
        bank_id=sample_tenant,
        sub_tenant_id=sample_tenant,
        is_active=True,
    )
    
    db_session.add(user)
    db_session.commit()
    
    # Assign role
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
def regular_user(db_session, user_role, sample_tenant):
    """Create regular user with limited permissions"""
    user = User(
        username="test_alert_user",
        email="alert_user@test.com",
        password_hash="hashed_password",
        bank_id=sample_tenant,
        sub_tenant_id=sample_tenant,
        is_active=True,
    )
    
    db_session.add(user)
    db_session.commit()
    
    # Assign role
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
    """Create JWT token for admin user"""
    return create_access_token(
        data={
            "sub": str(admin_user.user_id),
            "username": admin_user.username,
            "bank_id": str(admin_user.bank_id),
            "sub_tenant_id": str(admin_user.sub_tenant_id),
        }
    )


@pytest.fixture(scope="function")
def user_token(regular_user):
    """Create JWT token for regular user"""
    return create_access_token(
        data={
            "sub": str(regular_user.user_id),
            "username": regular_user.username,
            "bank_id": str(regular_user.bank_id),
            "sub_tenant_id": str(regular_user.sub_tenant_id),
        }
    )


@pytest.fixture(scope="function")
def admin_headers(admin_token):
    """Create auth headers for admin user"""
    return {"Authorization": f"Bearer {admin_token}"}


@pytest.fixture(scope="function")
def user_headers(user_token):
    """Create auth headers for regular user"""
    return {"Authorization": f"Bearer {user_token}"}


# ============================================================================
# Connector and Connection Fixtures
# ============================================================================

@pytest.fixture(scope="function")
def mysql_source_connector(db_session):
    """Create MySQL source connector"""
    connector = ConnectorDefinition(
        connector_name="Test Alert MySQL Source",
        connector_type="source",
        technology="mysql",
        version="1.0.0",
        cdc_support=True,
        batch_support=True,
        config_schema={},
        is_active=True,
    )
    
    db_session.add(connector)
    db_session.commit()
    db_session.refresh(connector)
    
    return connector


@pytest.fixture(scope="function")
def postgres_dest_connector(db_session):
    """Create PostgreSQL destination connector"""
    connector = ConnectorDefinition(
        connector_name="Test Alert PostgreSQL Destination",
        connector_type="destination",
        technology="postgresql",
        version="1.0.0",
        config_schema={},
        is_active=True,
    )
    
    db_session.add(connector)
    db_session.commit()
    db_session.refresh(connector)
    
    return connector


@pytest.fixture(scope="function")
def sample_source(db_session, mysql_source_connector, sample_tenant):
    """Create sample source"""
    source = Source(
        bank_id=sample_tenant,
        sub_tenant_id=sample_tenant,
        source_name="Test Alert MySQL Source",
        connector_def_id=mysql_source_connector.connector_def_id,
        config={"host": "localhost", "port": 3306, "database": "test_alert_db"},
        is_active=True,
        connection_test_status="success",
    )
    
    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)
    
    return source


@pytest.fixture(scope="function")
def sample_destination(db_session, postgres_dest_connector, sample_tenant):
    """Create sample destination"""
    destination = Destination(
        bank_id=sample_tenant,
        sub_tenant_id=sample_tenant,
        destination_name="Test Alert PostgreSQL Dest",
        connector_def_id=postgres_dest_connector.connector_def_id,
        config={"host": "localhost", "port": 5432, "database": "test_alert_dest"},
        is_active=True,
        connection_test_status="success",
    )
    
    db_session.add(destination)
    db_session.commit()
    db_session.refresh(destination)
    
    return destination


@pytest.fixture(scope="function")
def sample_connection(db_session, sample_source, sample_destination, sample_tenant):
    """Create sample connection"""
    connection = Connection(
        bank_id=sample_tenant,
        sub_tenant_id=sample_tenant,
        connection_name="Test Alert Connection",
        source_id=sample_source.source_id,
        destination_id=sample_destination.destination_id,
        connection_mode="cdc",
        is_active=True,
    )
    
    db_session.add(connection)
    db_session.commit()
    db_session.refresh(connection)
    
    return connection


@pytest.fixture(scope="function")
def sample_stream(db_session, sample_connection, sample_source, sample_tenant):
    """Create sample stream"""
    stream = Stream(
        bank_id=sample_tenant,
        sub_tenant_id=sample_tenant,
        connection_id=sample_connection.connection_id,
        source_id=sample_source.source_id,
        stream_name="Test Alert users",
        source_schema="public",
        source_table="users",
        destination_table="users_synced",
        is_active=True,
    )
    
    db_session.add(stream)
    db_session.commit()
    db_session.refresh(stream)
    
    return stream


# ============================================================================
# Notification Channel Fixtures
# ============================================================================

@pytest.fixture(scope="function")
def sample_email_channel(db_session, admin_user, sample_tenant):
    """Create sample email notification channel"""
    channel = NotificationChannel(
        bank_id=sample_tenant,
        sub_tenant_id=sample_tenant,
        channel_name="Test Alert Email Channel",
        channel_type="email",
        description="Test email notification channel",
        config={
            "recipients": ["alert@test.com", "ops@test.com"],
            "cc": ["manager@test.com"],
            "subject_prefix": "[ALERT]"
        },
        is_active=True,
        is_verified=True,
        verified_at=datetime.utcnow(),
        created_by=admin_user.user_id,
    )
    
    db_session.add(channel)
    db_session.commit()
    db_session.refresh(channel)
    
    return channel


@pytest.fixture(scope="function")
def sample_slack_channel(db_session, admin_user, sample_tenant):
    """Create sample Slack notification channel"""
    channel = NotificationChannel(
        bank_id=sample_tenant,
        sub_tenant_id=sample_tenant,
        channel_name="Test Alert Slack Channel",
        channel_type="slack",
        description="Test Slack notification channel",
        config={
            "webhook_url": "https://hooks.slack.com/services/TEST/WEBHOOK/URL",
            "channel": "#alerts",
            "username": "Fusion Alert Bot"
        },
        is_active=True,
        is_verified=False,
        created_by=admin_user.user_id,
    )
    
    db_session.add(channel)
    db_session.commit()
    db_session.refresh(channel)
    
    return channel


# ============================================================================
# Alert Rule Fixtures
# ============================================================================

@pytest.fixture(scope="function")
def sample_alert_rule(db_session, admin_user, sample_tenant, sample_connection, sample_email_channel):
    """Create sample alert rule"""
    rule = AlertRule(
        bank_id=sample_tenant,
        sub_tenant_id=sample_tenant,
        rule_name="Test Alert High Lag Rule",
        description="Alert when replication lag exceeds threshold",
        alert_type="high_lag",
        severity="warning",
        scope_type="connection",
        connection_id=sample_connection.connection_id,
        condition_type="threshold",
        condition_definition={
            "metric": "lag_seconds",
            "operator": "gt",
            "value": 300
        },
        evaluation_interval_minutes=5,
        evaluation_window_minutes=15,
        consecutive_failures=2,
        auto_resolve=True,
        auto_resolve_after_minutes=30,
        is_active=True,
        created_by=admin_user.user_id,
    )
    
    db_session.add(rule)
    db_session.flush()
    
    # Associate with email channel
    rule_channel = AlertRuleChannel(
        rule_id=rule.rule_id,
        channel_id=sample_email_channel.channel_id,
    )
    db_session.add(rule_channel)
    
    db_session.commit()
    db_session.refresh(rule)
    
    return rule


@pytest.fixture(scope="function")
def sample_alert(db_session, sample_tenant, sample_connection):
    """Create sample alert"""
    alert = Alert(
        bank_id=sample_tenant,
        sub_tenant_id=sample_tenant,
        alert_type="connection_failure",
        severity="critical",
        title="Test Alert Connection Failed",
        message="Connection to MySQL source has failed",
        connection_id=sample_connection.connection_id,
        status="active",
        triggered_at=datetime.utcnow(),
        alert_data={
            "error_code": "CONNECTION_TIMEOUT",
            "retry_count": 3
        },
        notification_sent=False,
        notification_channels=["email"],
    )
    
    db_session.add(alert)
    db_session.commit()
    db_session.refresh(alert)
    
    return alert


@pytest.fixture(scope="function")
def sample_alert_evaluation(db_session, sample_alert_rule):
    """Create sample alert evaluation"""
    evaluation = AlertEvaluation(
        rule_id=sample_alert_rule.rule_id,
        evaluated_at=datetime.utcnow(),
        evaluation_duration_ms=250,
        triggered=True,
        condition_met=True,
        consecutive_failures_count=2,
        evaluated_value=450.5,
        threshold_value=300.0,
        evaluation_data={
            "current_lag": 450.5,
            "threshold": 300.0,
            "connection_id": str(sample_alert_rule.connection_id)
        },
    )
    
    db_session.add(evaluation)
    db_session.commit()
    db_session.refresh(evaluation)
    
    return evaluation


@pytest.fixture(scope="function")
def sample_alert_suppression(db_session, admin_user, sample_tenant, sample_connection):
    """Create sample alert suppression"""
    suppression = AlertSuppression(
        bank_id=sample_tenant,
        sub_tenant_id=sample_tenant,
        suppression_name="Test Alert Maintenance Suppression",
        description="Suppress alerts during maintenance window",
        scope_type="connection",
        connection_id=sample_connection.connection_id,
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow() + timedelta(hours=2),
        is_active=True,
        reason="Scheduled maintenance",
        created_by=admin_user.user_id,
    )
    
    db_session.add(suppression)
    db_session.commit()
    db_session.refresh(suppression)
    
    return suppression
