"""Test configuration and fixtures for authentication tests"""
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from fastapi.testclient import TestClient
from uuid import uuid4
from datetime import datetime, timedelta

from app.main import app
from app.database import Base, get_db
from app.models.auth import User, Role, Permission, RefreshToken
from app.auth.password import hash_password
from app.config import JWT_SECRET_KEY, JWT_ALGORITHM


# Test database setup (in-memory SQLite)
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
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
def sample_bank_id():
    """Sample bank ID"""
    return uuid4()


@pytest.fixture
def sample_tenant_id():
    """Sample tenant ID"""
    return uuid4()


@pytest.fixture
def sample_permission(db_session):
    """Create a sample permission"""
    permission = Permission(
        permission_name="sources:read",
        display_name="Read Sources",
        resource="sources",
        action="read",
        scope="tenant",
    )
    db_session.add(permission)
    db_session.commit()
    db_session.refresh(permission)
    return permission


@pytest.fixture
def sample_role(db_session, sample_permission):
    """Create a sample role with permissions"""
    role = Role(
        role_name="user",
        display_name="User",
        role_level="user",
        is_system_role=True,
    )
    role.permissions.append(sample_permission)
    db_session.add(role)
    db_session.commit()
    db_session.refresh(role)
    return role


@pytest.fixture
def sample_user(db_session, sample_role, sample_bank_id, sample_tenant_id):
    """Create a sample user with role"""
    user = User(
        username="testuser",
        email="test@example.com",
        password_hash=hash_password("TestPassword123!"),
        first_name="Test",
        last_name="User",
        bank_id=sample_bank_id,
        sub_tenant_id=sample_tenant_id,
        is_superuser=False,
        is_active=True,
    )
    user.roles.append(sample_role)
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


@pytest.fixture
def sample_superuser(db_session):
    """Create a sample superuser"""
    superuser = User(
        username="admin",
        email="admin@example.com",
        password_hash=hash_password("AdminPassword123!"),
        first_name="Admin",
        last_name="User",
        is_superuser=True,
        is_active=True,
    )
    db_session.add(superuser)
    db_session.commit()
    db_session.refresh(superuser)
    return superuser


@pytest.fixture
def sample_inactive_user(db_session, sample_bank_id, sample_tenant_id):
    """Create an inactive user"""
    user = User(
        username="inactive",
        email="inactive@example.com",
        password_hash=hash_password("InactivePassword123!"),
        bank_id=sample_bank_id,
        sub_tenant_id=sample_tenant_id,
        is_active=False,
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


@pytest.fixture
def valid_access_token(sample_user):
    """Generate a valid access token"""
    from app.auth.jwt import create_access_token
    return create_access_token(
        user_id=str(sample_user.user_id),
        username=sample_user.username,
        bank_id=str(sample_user.bank_id),
        sub_tenant_id=str(sample_user.sub_tenant_id),
        roles=["user"],
        permissions=["sources:read"],
        is_superuser=False,
    )


@pytest.fixture
def expired_access_token(sample_user):
    """Generate an expired access token"""
    from jose import jwt
    from datetime import datetime, timedelta
    
    payload = {
        "sub": str(sample_user.user_id),
        "username": sample_user.username,
        "exp": datetime.utcnow() - timedelta(hours=1),  # Expired 1 hour ago
        "type": "access",
    }
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


@pytest.fixture
def valid_refresh_token(db_session, sample_user):
    """Generate a valid refresh token"""
    from app.auth.jwt import create_refresh_token
    import hashlib
    
    token = create_refresh_token(
        user_id=str(sample_user.user_id),
        username=sample_user.username,
    )
    
    # Store in database
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    refresh_token_record = RefreshToken(
        user_id=str(sample_user.user_id),
        token_hash=token_hash,
        expires_at=datetime.utcnow() + timedelta(days=7),
    )
    db_session.add(refresh_token_record)
    db_session.commit()
    
    return token


@pytest.fixture
def auth_headers(valid_access_token):
    """Generate authorization headers with valid token"""
    return {"Authorization": f"Bearer {valid_access_token}"}
