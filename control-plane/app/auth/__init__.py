"""
Authentication utilities package
Password hashing, JWT tokens, RBAC, and FastAPI dependencies
"""
from app.auth.password import hash_password, verify_password, needs_rehash
from app.auth.jwt import (
    create_access_token,
    create_refresh_token,
    decode_token,
    verify_token,
    extract_user_id,
    extract_tenant_context,
)
from app.auth.rbac import (
    ROLE_HIERARCHY,
    get_user_roles,
    get_user_permissions,
    has_permission,
    has_any_permission,
    has_all_permissions,
    has_role,
    has_role_level,
    can_access_bank,
    can_access_tenant,
    get_accessible_tenants,
)
from app.auth.dependencies import (
    CurrentUser,
    get_current_user,
    get_current_superuser,
    require_permission,
    require_any_permission,
    require_role,
    require_role_level,
    require_bank_access,
    require_tenant_access,
    get_optional_current_user,
)

__all__ = [
    # Password
    "hash_password",
    "verify_password",
    "needs_rehash",
    # JWT
    "create_access_token",
    "create_refresh_token",
    "decode_token",
    "verify_token",
    "extract_user_id",
    "extract_tenant_context",
    # RBAC
    "ROLE_HIERARCHY",
    "get_user_roles",
    "get_user_permissions",
    "has_permission",
    "has_any_permission",
    "has_all_permissions",
    "has_role",
    "has_role_level",
    "can_access_bank",
    "can_access_tenant",
    "get_accessible_tenants",
    # Dependencies
    "CurrentUser",
    "get_current_user",
    "get_current_superuser",
    "require_permission",
    "require_any_permission",
    "require_role",
    "require_role_level",
    "require_bank_access",
    "require_tenant_access",
    "get_optional_current_user",
]
