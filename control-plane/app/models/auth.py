"""
Authentication and Authorization Models
User accounts, roles, permissions, and authentication tokens
"""
from sqlalchemy import Column, String, Boolean, ForeignKey, Table, event, text, DateTime
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from app.models.base import BaseModel, TimestampMixin


# Many-to-many relationship between users and roles
user_roles = Table(
    'user_roles',
    BaseModel.metadata,
    Column('user_id', UUID(as_uuid=True), ForeignKey('users.user_id', ondelete='CASCADE'), primary_key=True),
    Column('role_id', UUID(as_uuid=True), ForeignKey('roles.role_id', ondelete='CASCADE'), primary_key=True),
)

# Many-to-many relationship between roles and permissions
role_permissions = Table(
    'role_permissions',
    BaseModel.metadata,
    Column('role_id', UUID(as_uuid=True), ForeignKey('roles.role_id', ondelete='CASCADE'), primary_key=True),
    Column('permission_id', UUID(as_uuid=True), ForeignKey('permissions.permission_id', ondelete='CASCADE'), primary_key=True),
)


class User(BaseModel, TimestampMixin):
    """User accounts with multi-tenancy"""
    
    __tablename__ = "users"
    
    # Primary Key
    user_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Authentication
    username = Column(String(255), nullable=False, unique=True, index=True)
    email = Column(String(255), nullable=False, unique=True, index=True)
    password_hash = Column(String(255), nullable=False)
    
    # User Information
    first_name = Column(String(255), nullable=True)
    last_name = Column(String(255), nullable=True)
    full_name = Column(String(500), nullable=True)
    
    # Multi-Tenancy
    bank_id = Column(UUID(as_uuid=True), nullable=True, index=True)  # Null for super admins
    sub_tenant_id = Column(UUID(as_uuid=True), nullable=True, index=True)  # Null for bank admins
    
    # Status
    is_active = Column(Boolean, nullable=False, server_default=text("true"))
    is_superuser = Column(Boolean, nullable=False, server_default=text("false"))
    is_email_verified = Column(Boolean, nullable=False, server_default=text("false"))
    
    # Security
    last_login_at = Column(DateTime(timezone=True), nullable=True)
    password_changed_at = Column(DateTime(timezone=True), nullable=True)
    failed_login_attempts = Column(String(50), nullable=False, server_default=text("'0'"))
    locked_until = Column(DateTime(timezone=True), nullable=True)
    
    # Preferences
    preferences = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    # Relationships
    roles = relationship(
        "Role",
        secondary=user_roles,
        back_populates="users",
    )
    refresh_tokens = relationship(
        "RefreshToken",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    
    def __repr__(self) -> str:
        return f"<User(username={self.username}, email={self.email})>"


class Role(BaseModel, TimestampMixin):
    """Roles for RBAC"""
    
    __tablename__ = "roles"
    
    # Primary Key
    role_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Role Information
    role_name = Column(String(100), nullable=False, unique=True, index=True)
    display_name = Column(String(255), nullable=False)
    description = Column(String(500), nullable=True)
    
    # Hierarchy
    role_level = Column(String(50), nullable=False, index=True)  # superadmin, bank_admin, tenant_admin, user, viewer
    
    # Scope (null means global)
    bank_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    sub_tenant_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    
    # Status
    is_active = Column(Boolean, nullable=False, server_default=text("true"))
    is_system_role = Column(Boolean, nullable=False, server_default=text("false"))  # Cannot be deleted
    
    # Relationships
    users = relationship(
        "User",
        secondary=user_roles,
        back_populates="roles",
    )
    permissions = relationship(
        "Permission",
        secondary=role_permissions,
        back_populates="roles",
    )
    
    def __repr__(self) -> str:
        return f"<Role(name={self.role_name}, level={self.role_level})>"


class Permission(BaseModel, TimestampMixin):
    """Permissions for fine-grained access control"""
    
    __tablename__ = "permissions"
    
    # Primary Key
    permission_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Permission Information
    permission_name = Column(String(100), nullable=False, unique=True, index=True)
    display_name = Column(String(255), nullable=False)
    description = Column(String(500), nullable=True)
    
    # Resource and Action
    resource = Column(String(100), nullable=False, index=True)  # sources, destinations, connections, etc.
    action = Column(String(50), nullable=False, index=True)  # read, create, update, delete, execute
    
    # Scope
    scope = Column(String(50), nullable=False, server_default=text("'tenant'::character varying"))  # global, bank, tenant
    
    # Status
    is_active = Column(Boolean, nullable=False, server_default=text("true"))
    
    # Relationships
    roles = relationship(
        "Role",
        secondary=role_permissions,
        back_populates="permissions",
    )
    
    def __repr__(self) -> str:
        return f"<Permission(name={self.permission_name}, resource={self.resource}, action={self.action})>"


class RefreshToken(BaseModel, TimestampMixin):
    """Refresh tokens for JWT authentication"""
    
    __tablename__ = "refresh_tokens"
    
    # Primary Key
    token_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Foreign Key
    user_id = Column(
        UUID(as_uuid=True),
        ForeignKey("users.user_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    
    # Token
    token_hash = Column(String(255), nullable=False, unique=True, index=True)
    
    # Metadata
    expires_at = Column(DateTime(timezone=True), nullable=False, index=True)
    is_revoked = Column(Boolean, nullable=False, server_default=text("false"))
    revoked_at = Column(DateTime(timezone=True), nullable=True)
    
    # Device Information
    device_info = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    ip_address = Column(String(50), nullable=True)
    user_agent = Column(String(500), nullable=True)
    
    # Relationships
    user = relationship("User", back_populates="refresh_tokens")
    
    def __repr__(self) -> str:
        return f"<RefreshToken(user_id={self.user_id}, expires_at={self.expires_at}, revoked={self.is_revoked})>"


class AuditLog(BaseModel, TimestampMixin):
    """Audit log for security-critical actions"""
    
    __tablename__ = "audit_logs"
    
    # Primary Key
    log_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    
    # Actor
    user_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    username = Column(String(255), nullable=True)
    
    # Action
    action = Column(String(100), nullable=False, index=True)  # login, logout, create_source, delete_connection, etc.
    resource_type = Column(String(100), nullable=True, index=True)
    resource_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    
    # Context
    bank_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    sub_tenant_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    
    # Details
    status = Column(String(50), nullable=False)  # success, failure, error
    details = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    error_message = Column(String(500), nullable=True)
    
    # Request Context
    ip_address = Column(String(50), nullable=True)
    user_agent = Column(String(500), nullable=True)
    request_id = Column(String(255), nullable=True, index=True)
    
    def __repr__(self) -> str:
        return f"<AuditLog(action={self.action}, user={self.username}, status={self.status})>"


# ---------------------------------------------------------------------------
# Spec §5 (P5-11): Audit log immutability — SQLAlchemy event listeners that
# raise an error if application code tries to UPDATE or DELETE an AuditLog row.
# In production, complement this with a DB trigger:
#   CREATE RULE no_update_audit AS ON UPDATE TO audit_logs DO INSTEAD NOTHING;
#   CREATE RULE no_delete_audit AS ON DELETE TO audit_logs DO INSTEAD NOTHING;
# ---------------------------------------------------------------------------

@event.listens_for(AuditLog, "before_update")
def _prevent_audit_update(mapper, connection, target):  # noqa: ARG001
    raise RuntimeError(
        "audit_logs is append-only: UPDATE is forbidden. "
        "Spec §5 (P5-11): audit log entries must be immutable."
    )


@event.listens_for(AuditLog, "before_delete")
def _prevent_audit_delete(mapper, connection, target):  # noqa: ARG001
    raise RuntimeError(
        "audit_logs is append-only: DELETE is forbidden. "
        "Spec §5 (P5-11): audit log entries must be immutable."
    )
