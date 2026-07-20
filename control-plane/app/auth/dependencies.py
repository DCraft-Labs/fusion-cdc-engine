"""
FastAPI dependencies for authentication and authorization
Reusable dependencies for protecting routes and extracting user context
"""
from typing import Optional, List
from uuid import UUID
from fastapi import Depends, HTTPException, status, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from jose import JWTError

from app.database import get_db
from app.models.auth import User
from app.auth.jwt import verify_token, decode_token
from app.auth.oidc import oidc_enabled, validate_oidc_token, extract_oidc_user_info
from app.auth.rbac import (
    has_permission,
    has_any_permission,
    has_role,
    has_role_level,
    can_access_bank,
    can_access_tenant,
)


# HTTP Bearer token scheme
security = HTTPBearer()


class CurrentUser:
    """Current authenticated user context"""
    
    def __init__(
        self,
        user_id: UUID,
        username: str,
        email: str,
        bank_id: Optional[UUID],
        sub_tenant_id: Optional[UUID],
        is_superuser: bool,
        roles: List[str],
        permissions: List[str],
    ):
        self.user_id = user_id
        self.username = username
        self.email = email
        self.bank_id = bank_id
        self.sub_tenant_id = sub_tenant_id
        self.is_superuser = is_superuser
        self.roles = roles
        self.permissions = permissions
    
    def __repr__(self) -> str:
        return f"<CurrentUser(username={self.username}, tenant={self.sub_tenant_id})>"


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
) -> CurrentUser:
    """
    Dependency to get the current authenticated user from JWT token
    
    Args:
        credentials: Bearer token from Authorization header
        db: Database session
        
    Returns:
        CurrentUser object with user context
        
    Raises:
        HTTPException: If token is invalid or user not found
    """
    token = credentials.credentials

    # Spec §5 (P5-3): Try OIDC/external-IdP token first when OIDC is configured.
    if oidc_enabled():
        oidc_claims = validate_oidc_token(token)
        if oidc_claims is not None:
            info = extract_oidc_user_info(oidc_claims)
            # Look up local user by email (created on first OIDC login via SSO)
            user = db.query(User).filter(User.email == info["email"]).first()
            if user and user.is_active:
                roles = [r.role_name for r in getattr(user, "roles", [])]
                perms = [
                    p.permission_name
                    for r in getattr(user, "roles", [])
                    for p in getattr(r, "permissions", [])
                ]
                return CurrentUser(
                    user_id=user.user_id,
                    username=info["username"] or user.username,
                    email=user.email,
                    bank_id=user.bank_id if hasattr(user, "bank_id") else None,
                    sub_tenant_id=user.sub_tenant_id if hasattr(user, "sub_tenant_id") else None,
                    is_superuser=user.is_superuser,
                    roles=roles,
                    permissions=perms,
                )
            # OIDC token valid but no matching local user → reject
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="OIDC token valid but no matching local user account found",
                headers={"WWW-Authenticate": "Bearer"},
            )

    # Fall through to local JWT validation
    # Verify token
    payload = verify_token(token, token_type="access")
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Extract user ID
    try:
        user_id = UUID(payload.get("sub"))
    except (ValueError, TypeError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Get user from database
    user = db.query(User).filter(User.user_id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Check if user is active
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is inactive",
        )
    
    # Extract tenant context - prefer token values, fall back to DB user
    bank_id = None
    sub_tenant_id = None
    if payload.get("bank_id"):
        bank_id = UUID(payload["bank_id"])
    elif hasattr(user, "bank_id") and user.bank_id:
        bank_id = user.bank_id
    if payload.get("sub_tenant_id"):
        sub_tenant_id = UUID(payload["sub_tenant_id"])
    elif hasattr(user, "sub_tenant_id") and user.sub_tenant_id:
        sub_tenant_id = user.sub_tenant_id
    
    # Create current user context
    return CurrentUser(
        user_id=user_id,
        username=payload.get("username"),
        email=user.email,
        bank_id=bank_id,
        sub_tenant_id=sub_tenant_id,
        is_superuser=user.is_superuser,
        roles=payload.get("roles", []),
        permissions=payload.get("permissions", []),
    )


async def get_current_active_user(
    current_user: CurrentUser = Depends(get_current_user),
) -> CurrentUser:
    """
    Dependency to get current active user (alias for get_current_user)
    """
    return current_user


async def get_current_superuser(
    current_user: CurrentUser = Depends(get_current_user),
) -> CurrentUser:
    """
    Dependency to require superuser privileges
    
    Args:
        current_user: Current authenticated user
        
    Returns:
        CurrentUser if superuser
        
    Raises:
        HTTPException: If user is not a superuser
    """
    if not current_user.is_superuser:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Superuser privileges required",
        )
    return current_user


def require_permission(permission_name: str):
    """
    Dependency factory to require a specific permission
    
    Args:
        permission_name: Name of required permission
        
    Returns:
        Dependency function that checks permission
    """
    async def check_permission(
        current_user: CurrentUser = Depends(get_current_user),
        db: Session = Depends(get_db),
    ) -> CurrentUser:
        # Superusers bypass permission checks
        if current_user.is_superuser:
            return current_user
        
        # Check if user has permission
        if not await has_permission(db, current_user.user_id, permission_name):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission required: {permission_name}",
            )
        
        return current_user
    
    return check_permission


def require_any_permission(permission_names: List[str]):
    """
    Dependency factory to require any of the specified permissions
    
    Args:
        permission_names: List of permission names (any one required)
        
    Returns:
        Dependency function that checks permissions
    """
    async def check_permissions(
        current_user: CurrentUser = Depends(get_current_user),
        db: Session = Depends(get_db),
    ) -> CurrentUser:
        # Superusers bypass permission checks
        if current_user.is_superuser:
            return current_user
        
        # Check if user has any permission
        if not await has_any_permission(db, current_user.user_id, permission_names):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"One of these permissions required: {', '.join(permission_names)}",
            )
        
        return current_user
    
    return check_permissions


def require_role(role_name: str):
    """
    Dependency factory to require a specific role
    
    Args:
        role_name: Name of required role
        
    Returns:
        Dependency function that checks role
    """
    async def check_role(
        current_user: CurrentUser = Depends(get_current_user),
        db: Session = Depends(get_db),
    ) -> CurrentUser:
        # Superusers bypass role checks
        if current_user.is_superuser:
            return current_user
        
        # Check if user has role
        if not await has_role(db, current_user.user_id, role_name):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role required: {role_name}",
            )
        
        return current_user
    
    return check_role


def require_role_level(role_level: str):
    """
    Dependency factory to require a minimum role level
    
    Args:
        role_level: Minimum role level required (viewer, user, tenant_admin, bank_admin, superadmin)
        
    Returns:
        Dependency function that checks role level
    """
    async def check_role_level(
        current_user: CurrentUser = Depends(get_current_user),
        db: Session = Depends(get_db),
    ) -> CurrentUser:
        # Superusers bypass role checks
        if current_user.is_superuser:
            return current_user
        
        # Check if user has sufficient role level
        if not await has_role_level(db, current_user.user_id, role_level):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Minimum role level required: {role_level}",
            )
        
        return current_user
    
    return check_role_level


def require_bank_access(bank_id: UUID):
    """
    Dependency factory to require access to a specific bank
    
    Args:
        bank_id: Bank UUID to check access for
        
    Returns:
        Dependency function that checks bank access
    """
    async def check_bank_access(
        current_user: CurrentUser = Depends(get_current_user),
        db: Session = Depends(get_db),
    ) -> CurrentUser:
        # Superusers bypass access checks
        if current_user.is_superuser:
            return current_user
        
        # Check if user can access bank
        if not can_access_bank(db, current_user.user_id, bank_id):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access to this bank is not allowed",
            )
        
        return current_user
    
    return check_bank_access


def require_tenant_access(bank_id: UUID, sub_tenant_id: UUID):
    """
    Dependency factory to require access to a specific tenant
    
    Args:
        bank_id: Bank UUID
        sub_tenant_id: Sub-tenant UUID to check access for
        
    Returns:
        Dependency function that checks tenant access
    """
    async def check_tenant_access(
        current_user: CurrentUser = Depends(get_current_user),
        db: Session = Depends(get_db),
    ) -> CurrentUser:
        # Superusers bypass access checks
        if current_user.is_superuser:
            return current_user
        
        # Check if user can access tenant
        if not can_access_tenant(db, current_user.user_id, bank_id, sub_tenant_id):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access to this tenant is not allowed",
            )
        
        return current_user
    
    return check_tenant_access


async def get_optional_current_user(
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db),
) -> Optional[CurrentUser]:
    """
    Dependency to optionally get the current user (for public endpoints)
    Returns None if no valid token is provided
    
    Args:
        authorization: Optional Authorization header
        db: Database session
        
    Returns:
        CurrentUser if authenticated, None otherwise
    """
    if not authorization or not authorization.startswith("Bearer "):
        return None
    
    token = authorization.replace("Bearer ", "")
    
    try:
        # Verify token
        payload = verify_token(token, token_type="access")
        if not payload:
            return None
        
        # Extract user ID
        user_id = UUID(payload.get("sub"))
        
        # Get user from database
        user = db.query(User).filter(User.user_id == user_id).first()
        if not user or not user.is_active:
            return None
        
        # Extract tenant context
        bank_id = None
        sub_tenant_id = None
        if payload.get("bank_id"):
            bank_id = UUID(payload["bank_id"])
        if payload.get("sub_tenant_id"):
            sub_tenant_id = UUID(payload["sub_tenant_id"])
        
        return CurrentUser(
            user_id=user_id,
            username=payload.get("username"),
            email=user.email,
            bank_id=bank_id,
            sub_tenant_id=sub_tenant_id,
            is_superuser=user.is_superuser,
            roles=payload.get("roles", []),
            permissions=payload.get("permissions", []),
        )
    except Exception:
        return None
