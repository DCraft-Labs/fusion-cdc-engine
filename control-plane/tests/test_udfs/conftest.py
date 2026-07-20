"""Pytest fixtures for UDF Catalog API tests"""

import pytest
from typing import Generator
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.auth.jwt import create_access_token
from app.database import get_db
from app.main import app
from app.models.auth import Permission, Role, User, role_permissions, user_roles
from app.models.transformation import UDFCatalog

TEST_DATABASE_URL = (
    "postgresql://fusion_user:fusion_password@localhost:5432/fusion_cdc_metadata"
)

test_engine = create_engine(TEST_DATABASE_URL)
TestSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)


@pytest.fixture(scope="function")
def db_session() -> Generator[Session, None, None]:
    session = TestSessionLocal()
    try:
        yield session
    finally:
        session.execute(text("DELETE FROM udf_execution_stats WHERE udf_id IN (SELECT udf_id FROM udf_catalog WHERE udf_name LIKE '%Test%')"))
        session.execute(text("DELETE FROM udf_catalog WHERE udf_name LIKE '%Test%'"))
        session.execute(text("DELETE FROM users WHERE username LIKE 'test_%'"))
        session.execute(text("DELETE FROM roles WHERE role_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM permissions WHERE permission_name LIKE 'udfs:%'"))
        session.commit()
        session.close()


@pytest.fixture
def client(db_session: Session) -> TestClient:
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
    return uuid4()


@pytest.fixture
def sample_bank() -> uuid4:
    return uuid4()


@pytest.fixture
def sample_permissions(db_session: Session) -> dict:
    suffix = str(uuid4())[:8]
    perms = [
        Permission(
            permission_id=uuid4(),
            permission_name=f"udfs:create:{suffix}",
            display_name="Create UDFs",
            description="Register new UDFs",
            resource="udfs",
            action="create",
            scope="tenant",
            is_active=True,
        ),
        Permission(
            permission_id=uuid4(),
            permission_name=f"udfs:read:{suffix}",
            display_name="Read UDFs",
            description="View UDFs",
            resource="udfs",
            action="read",
            scope="tenant",
            is_active=True,
        ),
        Permission(
            permission_id=uuid4(),
            permission_name=f"udfs:update:{suffix}",
            display_name="Update UDFs",
            description="Update UDFs",
            resource="udfs",
            action="update",
            scope="tenant",
            is_active=True,
        ),
        Permission(
            permission_id=uuid4(),
            permission_name=f"udfs:delete:{suffix}",
            display_name="Delete UDFs",
            description="Deactivate UDFs",
            resource="udfs",
            action="delete",
            scope="tenant",
            is_active=True,
        ),
    ]
    for perm in perms:
        db_session.add(perm)
    db_session.commit()
    for perm in perms:
        db_session.refresh(perm)
    return {p.permission_name: p for p in perms}


@pytest.fixture
def admin_role(db_session: Session, sample_permissions: dict) -> Role:
    role = Role(
        role_id=uuid4(),
        role_name="Test UDF Admin",
        display_name="UDF Admin",
        description="Admin role for UDF tests",
        role_level="tenant_admin",
        is_active=True,
        is_system_role=False,
    )
    db_session.add(role)
    db_session.commit()
    for perm in sample_permissions.values():
        db_session.execute(
            role_permissions.insert().values(role_id=role.role_id, permission_id=perm.permission_id)
        )
    db_session.commit()
    db_session.refresh(role)
    return role


@pytest.fixture
def admin_user(db_session: Session, admin_role: Role, sample_tenant, sample_bank) -> User:
    user = User(
        user_id=uuid4(),
        username="test_udf_admin",
        email="test_udf_admin@example.com",
        password_hash="hashed_password",
        is_active=True,
        is_superuser=True,
        sub_tenant_id=sample_tenant,
        bank_id=sample_bank,
    )
    db_session.add(user)
    db_session.commit()
    db_session.execute(
        user_roles.insert().values(user_id=user.user_id, role_id=admin_role.role_id)
    )
    db_session.commit()
    db_session.refresh(user)
    return user


@pytest.fixture
def other_tenant_user(db_session: Session, admin_role: Role) -> User:
    user = User(
        user_id=uuid4(),
        username="test_udf_other_tenant",
        email="test_udf_other@example.com",
        password_hash="hashed_password",
        is_active=True,
        is_superuser=True,
        sub_tenant_id=uuid4(),
        bank_id=uuid4(),
    )
    db_session.add(user)
    db_session.commit()
    db_session.execute(
        user_roles.insert().values(user_id=user.user_id, role_id=admin_role.role_id)
    )
    db_session.commit()
    db_session.refresh(user)
    return user


@pytest.fixture
def admin_headers(admin_user: User) -> dict:
    token = create_access_token(
        user_id=admin_user.user_id,
        username=admin_user.username,
        bank_id=admin_user.bank_id,
        sub_tenant_id=admin_user.sub_tenant_id,
    )
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def other_tenant_headers(other_tenant_user: User) -> dict:
    token = create_access_token(
        user_id=other_tenant_user.user_id,
        username=other_tenant_user.username,
        bank_id=other_tenant_user.bank_id,
        sub_tenant_id=other_tenant_user.sub_tenant_id,
    )
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def sample_udf(db_session: Session, admin_user: User) -> UDFCatalog:
    udf = UDFCatalog(
        udf_name="Test Mask Email UDF",
        description="Masks email addresses",
        function_code="def mask_email(email):\n    user, domain = email.split('@')\n    return user[0] + '***@' + domain",
        language="python",
        return_type="string",
        parameters=[{"name": "email", "type": "string", "description": "Email to mask"}],
        category="string",
        tags=["pii", "masking"],
        is_validated=False,
        is_active=True,
        sub_tenant_id=admin_user.sub_tenant_id,
        bank_id=admin_user.bank_id,
        created_by=admin_user.user_id,
    )
    db_session.add(udf)
    db_session.commit()
    db_session.refresh(udf)
    return udf


@pytest.fixture
def valid_udf_payload() -> dict:
    return {
        "udf_name": "Test Hash Column UDF",
        "description": "Hashes a column value",
        "function_code": "def hash_col(val):\n    import hashlib\n    return hashlib.sha256(str(val).encode()).hexdigest()",
        "language": "python",
        "return_type": "string",
        "parameters": [{"name": "val", "type": "any", "description": "Value to hash"}],
        "category": "security",
        "tags": ["hashing"],
    }
