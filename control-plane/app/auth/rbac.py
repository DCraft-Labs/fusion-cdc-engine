"""
RBAC (Role-Based Access Control) utilities
Check user permissions and role hierarchies.

All public functions are async so they can be called uniformly from both
sync and async contexts (FastAPI endpoints + pytest-asyncio tests).
"""
from typing import List, Optional, Set
from uuid import UUID
from sqlalchemy.orm import Session
from app.models.auth import User, Role, Permission


# Role hierarchy levels (higher number = more privileges)
# Spec (PDF4 §4): tenant_admin, parent_admin (bank_admin), viewer, operator
ROLE_HIERARCHY = {
    "superadmin": 100,
    "bank_admin": 80,    # also known as parent_admin in the spec
    "tenant_admin": 60,
    "operator": 50,      # platform operator — can view all metrics / manage workers
    "user": 40,
    "viewer": 20,
}


async def get_user_roles(db: Session, user_id) -> List[str]:
    """Return role name strings for the given user."""
    user = db.query(User).filter(User.user_id == user_id).first()
    if not user:
        return []
    return [role.role_name for role in user.roles]


async def get_user_permissions(db: Session, user_id) -> List[str]:
    """Return permission name strings aggregated from all active roles."""
    user = db.query(User).filter(User.user_id == user_id).first()
    if not user:
        return []

    permissions: Set[str] = set()
    for role in user.roles:
        if role.is_active:
            for perm in role.permissions:
                if perm.is_active:
                    permissions.add(perm.permission_name)

    return list(permissions)


async def has_permission(
    db: Session,
    user_id,
    permission_name: Optional[str],
) -> bool:
    """Check if user has a specific permission."""
    if permission_name is None:
        return False
    user = db.query(User).filter(User.user_id == user_id).first()
    if not user or not user.is_active:
        return False

    if user.is_superuser:
        return True

    permissions = await get_user_permissions(db, user_id)
    return permission_name in permissions


async def has_any_permission(
    db: Session,
    user_id,
    permission_names: List[str],
) -> bool:
    """Check if user has any of the specified permissions."""
    if not permission_names:
        return False
    user = db.query(User).filter(User.user_id == user_id).first()
    if not user or not user.is_active:
        return False

    if user.is_superuser:
        return True

    user_permissions = await get_user_permissions(db, user_id)
    return any(perm in user_permissions for perm in permission_names)


async def has_all_permissions(
    db: Session,
    user_id,
    permission_names: List[str],
) -> bool:
    """Check if user has all of the specified permissions."""
    if not permission_names:
        return False
    user = db.query(User).filter(User.user_id == user_id).first()
    if not user or not user.is_active:
        return False

    if user.is_superuser:
        return True

    user_permissions = await get_user_permissions(db, user_id)
    return all(perm in user_permissions for perm in permission_names)


async def has_role(
    db: Session,
    user_id,
    role_name: str,
) -> bool:
    """Check if user has a specific role by name."""
    user = db.query(User).filter(User.user_id == user_id).first()
    if not user or not user.is_active:
        return False

    return any(r.role_name == role_name and r.is_active for r in user.roles)


async def has_role_level(
    db: Session,
    user_id,
    required_level: str,
) -> bool:
    """Check if user has an active role at or above the required hierarchy level.

    Note: the is_superuser flag is intentionally NOT considered here — only
    explicit role assignments matter for level checks.
    """
    user = db.query(User).filter(User.user_id == user_id).first()
    if not user or not user.is_active:
        return False

    required_score = ROLE_HIERARCHY.get(required_level, 0)

    for role in user.roles:
        if role.is_active:
            role_score = ROLE_HIERARCHY.get(role.role_level, 0)
            if role_score >= required_score:
                return True

    return False


async def can_access_bank(
    db: Session,
    user_id,
    bank_id,
) -> bool:
    """Check if user can access resources in a specific bank."""
    user = db.query(User).filter(User.user_id == user_id).first()
    if not user or not user.is_active:
        return False

    if user.is_superuser:
        return True

    if str(user.bank_id) == str(bank_id):
        return True

    for role in user.roles:
        if (role.is_active
                and role.role_level == "bank_admin"
                and str(role.bank_id) == str(bank_id)):
            return True

    return False


async def can_access_tenant(
    db: Session,
    user_id,
    sub_tenant_id,
) -> bool:
    """Check if user can access resources in a specific tenant (sub_tenant_id)."""
    user = db.query(User).filter(User.user_id == user_id).first()
    if not user or not user.is_active:
        return False

    if user.is_superuser:
        return True

    # Bank admins can access all tenants within their bank
    for role in user.roles:
        if role.is_active and role.role_level in ("bank_admin", "tenant_admin"):
            return True

    if str(user.sub_tenant_id) == str(sub_tenant_id):
        return True

    return False


async def get_accessible_tenants(
    db: Session,
    user_id,
) -> List[str]:
    """Return list of sub_tenant_id strings the user can access.

    An empty list means the user can access *all* tenants (superuser / bank_admin).
    """
    user = db.query(User).filter(User.user_id == user_id).first()
    if not user or not user.is_active:
        return []

    if user.is_superuser:
        return []

    for role in user.roles:
        if role.is_active and role.role_level in ("bank_admin",):
            return []

    accessible: List[str] = []
    if user.sub_tenant_id:
        accessible.append(str(user.sub_tenant_id))

    return accessible
