"""
Simplified test fixtures for alerting tests (bypassing auth requirements)
"""
import pytest
from uuid import uuid4, UUID
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient
from typing import Dict

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

# Test database configuration (using postgres superuser for full permissions)
TEST_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/fusion_master"

engine = create_engine(TEST_DATABASE_URL)
TestSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Mock tenant data
MOCK_TENANT_ID = uuid4()
MOCK_BANK_ID = uuid4()
MOCK_USER_ID = uuid4()


@pytest.fixture(scope="function")
def db_session():
    """Create a new database session for testing"""
    session = TestSessionLocal()
    
    # Cleanup at start to handle any leftover data from failed tests
    try:
        session.execute(text("DELETE FROM alert_notification_logs WHERE log_id IS NOT NULL"))
        session.execute(text("DELETE FROM alert_history WHERE history_id IS NOT NULL"))
        session.execute(text("DELETE FROM alert_evaluations WHERE evaluation_id IS NOT NULL"))
        session.execute(text("DELETE FROM alert_escalation_policies WHERE policy_id IS NOT NULL"))
        session.execute(text("DELETE FROM alert_rule_channels WHERE rule_id IS NOT NULL"))
        session.execute(text("DELETE FROM alert_rules WHERE rule_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM notification_channels WHERE channel_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM alerts WHERE title LIKE 'Test%'"))
        session.execute(text("DELETE FROM alert_suppressions WHERE suppression_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM streams WHERE table_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM connections WHERE connection_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM sources WHERE source_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM destinations WHERE destination_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM connector_definitions WHERE connector_name LIKE 'Test%'"))
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Cleanup warning at start: {e}")
    
    try:
        yield session
    finally:
        # Cleanup test data in correct order (respecting foreign keys)
        try:
            session.execute(text("DELETE FROM alert_notification_logs WHERE log_id IS NOT NULL"))
            session.execute(text("DELETE FROM alert_history WHERE history_id IS NOT NULL"))
            session.execute(text("DELETE FROM alert_evaluations WHERE evaluation_id IS NOT NULL"))
            session.execute(text("DELETE FROM alert_escalation_policies WHERE policy_id IS NOT NULL"))
            session.execute(text("DELETE FROM alert_rule_channels WHERE rule_id IS NOT NULL"))
            session.execute(text("DELETE FROM alert_rules WHERE rule_name LIKE 'Test%'"))
            session.execute(text("DELETE FROM notification_channels WHERE channel_name LIKE 'Test%'"))
            session.execute(text("DELETE FROM alerts WHERE title LIKE 'Test%'"))
            session.execute(text("DELETE FROM alert_suppressions WHERE suppression_name LIKE 'Test%'"))
            # Use correct column name: table_name instead of stream_name
            session.execute(text("DELETE FROM streams WHERE table_name LIKE 'Test%'"))
            session.execute(text("DELETE FROM connections WHERE connection_name LIKE 'Test%'"))
            session.execute(text("DELETE FROM sources WHERE source_name LIKE 'Test%'"))
            session.execute(text("DELETE FROM destinations WHERE destination_name LIKE 'Test%'"))
            session.execute(text("DELETE FROM connector_definitions WHERE connector_name LIKE 'Test%'"))
            session.commit()
        except Exception as e:
            # Rollback on error and continue
            session.rollback()
            print(f"Cleanup warning: {e}")
        finally:
            session.close()


# Mock auth dependency override
class MockUser:
    """Mock user object for bypassing auth"""
    def __init__(self, user_id: UUID, username: str, email: str, is_admin: bool = False):
        self.user_id = user_id
        self.username = username
        self.email = email
        self.is_admin = is_admin
        self.is_superuser = is_admin  # Superuser bypasses all permission checks
        self.sub_tenant_id = MOCK_TENANT_ID
        self.bank_id = MOCK_BANK_ID
        self.is_active = True
        self.email_verified = True


@pytest.fixture(scope="function")
def admin_user():
    """Mock admin user"""
    return MockUser(
        user_id=MOCK_USER_ID,
        username="test_admin",
        email="test_admin@example.com",
        is_admin=True
    )


@pytest.fixture(scope="function")
def regular_user():
    """Mock regular user (non-admin)"""
    return MockUser(
        user_id=uuid4(),
        username="test_user",
        email="test_user@example.com",
        is_admin=False
    )


@pytest.fixture(scope="function")
def test_client(db_session, admin_user):
    """Create test client with database session override and mock auth"""
    from app.auth.dependencies import get_current_user
    
    def override_get_db():
        try:
            yield db_session
        finally:
            pass
    
    def override_get_current_user():
        return admin_user
    
    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_current_user] = override_get_current_user
    
    with TestClient(app) as client:
        yield client
    
    app.dependency_overrides.clear()


# Alias for compatibility with tests
@pytest.fixture(scope="function")
def client(test_client):
    """Alias for test_client"""
    return test_client


@pytest.fixture(scope="function")
def admin_headers():
    """Headers for admin requests"""
    return {"Authorization": "Bearer mock_admin_token"}


@pytest.fixture(scope="function")
def user_headers():
    """Headers for regular user requests"""
    return {"Authorization": "Bearer mock_user_token"}


# Connector fixtures
@pytest.fixture
def mysql_source_connector(db_session):
    """Create MySQL source connector"""
    connector = ConnectorDefinition(
        connector_name="Test MySQL",
        connector_type="mysql",
        category="source",
        latest_version="1.0.0",
        is_active=True,
    )
    db_session.add(connector)
    db_session.commit()
    db_session.refresh(connector)
    return connector


@pytest.fixture
def postgres_dest_connector(db_session):
    """Create PostgreSQL destination connector"""
    connector = ConnectorDefinition(
        connector_name="Test PostgreSQL",
        connector_type="postgresql",
        category="destination",
        latest_version="1.0.0",
        is_active=True,
    )
    db_session.add(connector)
    db_session.commit()
    db_session.refresh(connector)
    return connector


# Source/Destination fixtures
@pytest.fixture
def sample_source(db_session, mysql_source_connector):
    """Create sample source"""
    source = Source(
        source_name="Test MySQL Source",
        connector_definition_id=mysql_source_connector.connector_id,
        connector_version="1.0.0",
        bank_id=MOCK_BANK_ID,
        sub_tenant_id=MOCK_TENANT_ID,
        host="localhost",
        port=3306,
        database_name="test_db",
        username="testuser",
        password_encrypted="encrypted_password",
        status="active",
    )
    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)
    return source


@pytest.fixture
def sample_destination(db_session, postgres_dest_connector):
    """Create sample destination"""
    destination = Destination(
        destination_name="Test PostgreSQL Destination",
        connector_definition_id=postgres_dest_connector.connector_id,
        connector_version="1.0.0",
        bank_id=MOCK_BANK_ID,
        sub_tenant_id=MOCK_TENANT_ID,
        connection_config={
            "host": "localhost",
            "port": 5432,
            "database_name": "test_dest_db",
            "username": "destuser",
            "password": "testpass"
        },
        status="active",
    )
    db_session.add(destination)
    db_session.commit()
    db_session.refresh(destination)
    return destination


# Connection fixtures
@pytest.fixture
def sample_connection(db_session, sample_source, sample_destination):
    """Create sample connection"""
    connection = Connection(
        connection_name="Test Connection",
        source_id=sample_source.source_id,
        destination_id=sample_destination.destination_id,
        bank_id=MOCK_BANK_ID,
        sub_tenant_id=MOCK_TENANT_ID,
        sync_mode="full_refresh",
        sync_type="REALTIME",  # Changed from 'manual' to satisfy check constraint
        status="active",
    )
    db_session.add(connection)
    db_session.commit()
    db_session.refresh(connection)
    return connection


@pytest.fixture
def sample_stream(db_session, sample_connection):
    """Create sample stream"""
    stream = Stream(
        connection_id=sample_connection.connection_id,
        schema_name="public",
        table_name="Test_users",
        enabled=True,
        sync_mode="full_refresh",
    )
    db_session.add(stream)
    db_session.commit()
    db_session.refresh(stream)
    return stream


# Notification channel fixtures
@pytest.fixture
def sample_email_channel(db_session):
    """Create sample email notification channel"""
    channel = NotificationChannel(
        channel_name="Test Email Channel",
        channel_type="email",
        bank_id=MOCK_BANK_ID,
        sub_tenant_id=MOCK_TENANT_ID,
        config={"recipients": ["admin@example.com"]},
        is_active=True,
    )
    db_session.add(channel)
    db_session.commit()
    db_session.refresh(channel)
    return channel


@pytest.fixture
def sample_slack_channel(db_session):
    """Create sample Slack notification channel"""
    channel = NotificationChannel(
        channel_name="Test Slack Channel",
        channel_type="slack",
        bank_id=MOCK_BANK_ID,
        sub_tenant_id=MOCK_TENANT_ID,
        config={"webhook_url": "https://hooks.slack.com/services/TEST/TEST/TEST"},
        is_active=True,
    )
    db_session.add(channel)
    db_session.commit()
    db_session.refresh(channel)
    return channel


# Alert rule fixtures
@pytest.fixture
def sample_alert_rule(db_session, sample_connection, sample_email_channel):
    """Create sample alert rule"""
    rule = AlertRule(
        rule_name="Test Alert Rule",
        alert_type="connection_failure",
        severity="high",
        bank_id=MOCK_BANK_ID,
        sub_tenant_id=MOCK_TENANT_ID,
        scope_type="connection",
        scope_id=sample_connection.connection_id,
        condition_type="threshold",
        condition_definition={"metric": "failure_count", "operator": ">", "threshold": 3},
        description="Test alert rule for connection failures",
        is_active=True,  # Changed from is_enabled to is_active
        created_by=MOCK_USER_ID,
    )
    db_session.add(rule)
    db_session.flush()
    
    # Associate with channel
    rule_channel = AlertRuleChannel(
        rule_id=rule.rule_id,
        channel_id=sample_email_channel.channel_id,
        priority=1,
    )
    db_session.add(rule_channel)
    db_session.commit()
    db_session.refresh(rule)
    return rule


# Alert fixtures
@pytest.fixture
def sample_alert(db_session, sample_alert_rule):
    """Create sample alert"""
    alert = Alert(
        title="Test Alert",
        alert_type="connection_failure",
        severity="high",
        bank_id=MOCK_BANK_ID,
        sub_tenant_id=MOCK_TENANT_ID,
        connection_id=sample_alert_rule.scope_id,
        message="Test alert message",
        acknowledged=False,
        resolved=False,
    )
    db_session.add(alert)
    db_session.commit()
    db_session.refresh(alert)
    return alert


# Alert evaluation fixtures
@pytest.fixture
def sample_alert_evaluation(db_session, sample_alert_rule):
    """Create sample alert evaluation"""
    evaluation = AlertEvaluation(
        rule_id=sample_alert_rule.rule_id,
        passed=True,  # Changed from evaluation_result to match SQL schema
        metric_value=5.0,
        threshold_value=3.0,
    )
    db_session.add(evaluation)
    db_session.commit()
    db_session.refresh(evaluation)
    return evaluation


# Alert suppression fixtures
@pytest.fixture
def sample_alert_suppression(db_session, sample_connection):
    """Create sample alert suppression"""
    suppression = AlertSuppression(
        suppression_name="Test Suppression",
        bank_id=MOCK_BANK_ID,
        sub_tenant_id=MOCK_TENANT_ID,
        scope_type="connection",
        connection_ids=[sample_connection.connection_id],  # Changed to array
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow() + timedelta(hours=2),
        created_by=MOCK_USER_ID,
    )
    db_session.add(suppression)
    db_session.commit()
    db_session.refresh(suppression)
    return suppression


# Mock tenant fixture
@pytest.fixture
def sample_tenant():
    """Return mock tenant information"""
    return {
        "tenant_id": MOCK_TENANT_ID,
        "bank_id": MOCK_BANK_ID,
        "tenant_name": "Test Tenant",
    }
