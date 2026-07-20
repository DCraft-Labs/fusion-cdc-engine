"""Pytest fixtures for Transformation Pipeline API tests"""

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
from app.models.transformation import TransformPipeline

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
        session.execute(text("DELETE FROM transformation_logs WHERE pipeline_id IN (SELECT pipeline_id FROM transform_pipelines WHERE pipeline_name LIKE '%Test%')"))
        session.execute(text("DELETE FROM transformation_dependencies WHERE dependent_pipeline_id IN (SELECT pipeline_id FROM transform_pipelines WHERE pipeline_name LIKE '%Test%')"))
        session.execute(text("DELETE FROM transform_pipelines WHERE pipeline_name LIKE '%Test%'"))
        session.execute(text("DELETE FROM users WHERE username LIKE 'test_%'"))
        session.execute(text("DELETE FROM roles WHERE role_name LIKE 'Test%'"))
        session.execute(text("DELETE FROM permissions WHERE permission_name LIKE 'transformations:%'"))
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
            permission_name=f"transformations:create:{suffix}",
            display_name="Create Transformations",
            description="Create transformation pipelines",
            resource="transformations",
            action="create",
            scope="tenant",
            is_active=True,
        ),
        Permission(
            permission_id=uuid4(),
            permission_name=f"transformations:read:{suffix}",
            display_name="Read Transformations",
            description="View transformation pipelines",
            resource="transformations",
            action="read",
            scope="tenant",
            is_active=True,
        ),
        Permission(
            permission_id=uuid4(),
            permission_name=f"transformations:update:{suffix}",
            display_name="Update Transformations",
            description="Update transformation pipelines",
            resource="transformations",
            action="update",
            scope="tenant",
            is_active=True,
        ),
        Permission(
            permission_id=uuid4(),
            permission_name=f"transformations:delete:{suffix}",
            display_name="Delete Transformations",
            description="Delete transformation pipelines",
            resource="transformations",
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
        role_name="Test Transformation Admin",
        display_name="Transformation Admin",
        description="Admin role for transformation tests",
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
        username="test_transform_admin",
        email="test_transform_admin@example.com",
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
    """A user in a DIFFERENT tenant — used to test isolation."""
    user = User(
        user_id=uuid4(),
        username="test_other_tenant_user",
        email="test_other_tenant@example.com",
        password_hash="hashed_password",
        is_active=True,
        is_superuser=True,
        sub_tenant_id=uuid4(),  # different tenant
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
def sample_pipeline(db_session: Session, admin_user: User) -> TransformPipeline:
    """A pre-created pipeline for the admin user's tenant."""
    pipeline = TransformPipeline(
        pipeline_name="Test SQL Pipeline",
        description="A test SQL transformation",
        pipeline_type="sql",
        transformation_code="SELECT id, name FROM orders",
        language="sql",
        input_streams=["orders"],
        output_stream="orders_transformed",
        execution_mode="batch",
        spark_config={},
        version=1,
        is_published=False,
        is_validated=False,
        is_active=True,
        is_deleted=False,
        sub_tenant_id=admin_user.sub_tenant_id,
        bank_id=admin_user.bank_id,
        created_by=admin_user.user_id,
    )
    db_session.add(pipeline)
    db_session.commit()
    db_session.refresh(pipeline)
    return pipeline


@pytest.fixture
def valid_pipeline_payload() -> dict:
    return {
        "pipeline_name": "Test Create Pipeline",
        "description": "Test pipeline for creation",
        "pipeline_type": "python",
        "transformation_code": "df = df.withColumn('new_col', col('old_col') * 2)",
        "language": "python",
        "input_streams": ["source_table"],
        "output_stream": "dest_table",
        "execution_mode": "batch",
        "spark_config": {},
    }
