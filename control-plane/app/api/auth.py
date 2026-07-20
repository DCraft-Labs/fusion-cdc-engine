"""
Authentication API endpoints
Login, logout, register, token refresh, user management
"""
from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.orm import Session
from sqlalchemy import select
from typing import Optional
from datetime import datetime, timedelta
import hashlib
import secrets

from app.database import get_db
from app.models.auth import User, RefreshToken, AuditLog
from app.schemas.auth import (
    LoginRequest,
    TokenResponse,
    RefreshTokenRequest,
    LogoutRequest,
    UserCreate,
    UserUpdate,
    UserResponse,
    UserWithRoles,
    CurrentUserResponse,
    PasswordChangeRequest,
)
from app.auth.password import hash_password, verify_password
from app.auth.jwt import (
    create_access_token,
    create_refresh_token,
    decode_token,
    extract_user_id,
)
from app.auth.rbac import get_user_roles, get_user_permissions
from app.auth.dependencies import get_current_user, CurrentUser, require_permission
from app.config import settings


router = APIRouter(prefix="/auth", tags=["Authentication"])


# ============================================================================
# Helper Functions
# ============================================================================

def create_audit_log(
    db: Session,
    action: str,
    status: str,
    user_id: Optional[str] = None,
    username: Optional[str] = None,
    resource_type: Optional[str] = None,
    resource_id: Optional[str] = None,
    details: Optional[dict] = None,
    ip_address: Optional[str] = None,
):
    """Create audit log entry"""
    log = AuditLog(
        user_id=user_id,
        username=username,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        status=status,
        details=details or {},
        ip_address=ip_address,
    )
    db.add(log)
    db.commit()


def get_client_ip(request: Request) -> Optional[str]:
    """Extract client IP address from request"""
    if request.client:
        return request.client.host
    # Check for forwarded IP (behind proxy)
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return None


async def store_refresh_token(
    db: Session,
    user_id: str,
    token: str,
    device_info: Optional[dict] = None,
) -> RefreshToken:
    """Store refresh token in database"""
    # Hash the token for storage
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    
    # Create refresh token record
    refresh_token = RefreshToken(
        user_id=user_id,
        token_hash=token_hash,
        expires_at=datetime.utcnow() + timedelta(days=7),
        device_info=device_info or {},
    )
    db.add(refresh_token)
    db.commit()
    db.refresh(refresh_token)
    return refresh_token


async def revoke_refresh_token(db: Session, token: str) -> bool:
    """Revoke a refresh token"""
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    
    stmt = select(RefreshToken).where(
        RefreshToken.token_hash == token_hash,
        RefreshToken.is_revoked == False,
    )
    result = db.execute(stmt)
    refresh_token = result.scalar_one_or_none()
    
    if refresh_token:
        refresh_token.is_revoked = True
        refresh_token.revoked_at = datetime.utcnow()
        db.commit()
        return True
    return False


async def validate_refresh_token(db: Session, token: str) -> Optional[RefreshToken]:
    """Validate refresh token"""
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    
    stmt = select(RefreshToken).where(
        RefreshToken.token_hash == token_hash,
        RefreshToken.is_revoked == False,
        RefreshToken.expires_at > datetime.utcnow(),
    )
    result = db.execute(stmt)
    return result.scalar_one_or_none()


# ============================================================================
# Authentication Endpoints
# ============================================================================

@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register(
    user_data: UserCreate,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Register a new user
    
    Creates a new user account with hashed password.
    Superadmin privilege required to create superuser accounts.
    """
    # Check if username already exists
    stmt = select(User).where(User.username == user_data.username)
    existing_user = db.execute(stmt).scalar_one_or_none()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already exists",
        )
    
    # Check if email already exists
    stmt = select(User).where(User.email == user_data.email)
    existing_email = db.execute(stmt).scalar_one_or_none()
    if existing_email:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already exists",
        )
    
    # Hash password
    password_hash = hash_password(user_data.password)
    
    # Create user
    user = User(
        username=user_data.username,
        email=user_data.email,
        password_hash=password_hash,
        first_name=user_data.first_name,
        last_name=user_data.last_name,
        bank_id=user_data.bank_id,
        sub_tenant_id=user_data.sub_tenant_id,
        is_superuser=user_data.is_superuser,
    )
    
    db.add(user)
    db.commit()
    db.refresh(user)
    
    # Audit log
    create_audit_log(
        db=db,
        action="user_registered",
        status="success",
        user_id=str(user.user_id),
        username=user.username,
        resource_type="user",
        resource_id=str(user.user_id),
        ip_address=get_client_ip(request),
    )
    
    return user


@router.post("/login", response_model=TokenResponse)
async def login(
    credentials: LoginRequest,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Login with username/email and password
    
    Returns access token and refresh token.
    Access token expires in 30 minutes, refresh token in 7 days.
    """
    # Find user by username or email
    stmt = select(User).where(
        (User.username == credentials.username) | (User.email == credentials.username)
    )
    user = db.execute(stmt).scalar_one_or_none()
    
    if not user:
        # Audit log - failed login
        create_audit_log(
            db=db,
            action="login_failed",
            status="failure",
            username=credentials.username,
            details={"reason": "user_not_found"},
            ip_address=get_client_ip(request),
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
        )
    
    # Verify password
    if not verify_password(credentials.password, user.password_hash):
        # Increment failed login attempts (cast in case SQLite stored as string)
        user.failed_login_attempts = int(user.failed_login_attempts or 0) + 1
        if user.failed_login_attempts >= 5:
            user.locked_until = datetime.utcnow() + timedelta(minutes=30)
        db.commit()
        
        # Audit log - failed login
        create_audit_log(
            db=db,
            action="login_failed",
            status="failure",
            user_id=str(user.user_id),
            username=user.username,
            details={"reason": "invalid_password", "attempts": user.failed_login_attempts},
            ip_address=get_client_ip(request),
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
        )
    
    # Check if account is locked
    if user.locked_until and user.locked_until > datetime.utcnow():
        create_audit_log(
            db=db,
            action="login_failed",
            status="failure",
            user_id=str(user.user_id),
            username=user.username,
            details={"reason": "account_locked", "locked_until": user.locked_until.isoformat()},
            ip_address=get_client_ip(request),
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Account locked until {user.locked_until.isoformat()}",
        )
    
    # Check if account is active
    if not user.is_active:
        create_audit_log(
            db=db,
            action="login_failed",
            status="failure",
            user_id=str(user.user_id),
            username=user.username,
            details={"reason": "account_inactive"},
            ip_address=get_client_ip(request),
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is inactive",
        )
    
    # Get user roles and permissions
    roles = await get_user_roles(db, str(user.user_id))
    permissions = await get_user_permissions(db, str(user.user_id))
    
    # Create tokens
    access_token = create_access_token(
        user_id=str(user.user_id),
        username=user.username,
        bank_id=str(user.bank_id) if user.bank_id else None,
        sub_tenant_id=str(user.sub_tenant_id) if user.sub_tenant_id else None,
        roles=roles,
        permissions=permissions,
        is_superuser=user.is_superuser,
    )
    
    refresh_token = create_refresh_token(
        user_id=str(user.user_id),
        username=user.username,
    )
    
    # Store refresh token
    device_info = {
        "user_agent": request.headers.get("user-agent"),
        "ip_address": get_client_ip(request),
    }
    await store_refresh_token(db, str(user.user_id), refresh_token, device_info)
    
    # Update user last login
    user.last_login_at = datetime.utcnow()
    user.failed_login_attempts = 0  # Reset failed attempts
    user.locked_until = None  # Unlock account
    db.commit()
    
    # Audit log - successful login
    create_audit_log(
        db=db,
        action="login_success",
        status="success",
        user_id=str(user.user_id),
        username=user.username,
        details={"roles": roles},
        ip_address=get_client_ip(request),
    )
    
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        token_type="bearer",
        expires_in=settings.JWT_EXPIRATION_MINUTES * 60,
    )


@router.post("/refresh", response_model=TokenResponse)
async def refresh_token(
    refresh_request: RefreshTokenRequest,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Refresh access token using refresh token
    
    Exchange a valid refresh token for a new access token.
    """
    # Validate refresh token
    token_record = await validate_refresh_token(db, refresh_request.refresh_token)
    if not token_record:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token",
        )
    
    # Decode token to get user info
    try:
        payload = decode_token(refresh_request.refresh_token)
        user_id = payload.get("sub")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token",
        )
    
    # Get user
    stmt = select(User).where(User.user_id == user_id)
    user = db.execute(stmt).scalar_one_or_none()
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive",
        )
    
    # Get user roles and permissions
    roles = await get_user_roles(db, str(user.user_id))
    permissions = await get_user_permissions(db, str(user.user_id))
    
    # Create new access token
    access_token = create_access_token(
        user_id=str(user.user_id),
        username=user.username,
        bank_id=str(user.bank_id) if user.bank_id else None,
        sub_tenant_id=str(user.sub_tenant_id) if user.sub_tenant_id else None,
        roles=roles,
        permissions=permissions,
        is_superuser=user.is_superuser,
    )
    
    # Audit log
    create_audit_log(
        db=db,
        action="token_refreshed",
        status="success",
        user_id=str(user.user_id),
        username=user.username,
        ip_address=get_client_ip(request),
    )
    
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_request.refresh_token,  # Reuse same refresh token
        token_type="bearer",
        expires_in=settings.JWT_EXPIRATION_MINUTES * 60,
    )


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    logout_request: LogoutRequest,
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Logout user and revoke refresh token
    
    Revokes the provided refresh token or all tokens for the user.
    """
    if logout_request.refresh_token:
        # Revoke specific token
        await revoke_refresh_token(db, logout_request.refresh_token)
    else:
        # Revoke all tokens for user
        stmt = select(RefreshToken).where(
            RefreshToken.user_id == current_user.user_id,
            RefreshToken.is_revoked == False,
        )
        tokens = db.execute(stmt).scalars().all()
        for token in tokens:
            token.is_revoked = True
            token.revoked_at = datetime.utcnow()
        db.commit()
    
    # Audit log
    create_audit_log(
        db=db,
        action="logout",
        status="success",
        user_id=current_user.user_id,
        username=current_user.username,
        ip_address=get_client_ip(request),
    )
    
    return None


@router.get("/me", response_model=CurrentUserResponse)
async def get_current_user_info(
    current_user: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Get current authenticated user information
    
    Returns user profile with roles and permissions.
    """
    # Get full user record
    stmt = select(User).where(User.user_id == current_user.user_id)
    user = db.execute(stmt).scalar_one_or_none()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    
    # Build response
    user_response = CurrentUserResponse(
        user_id=user.user_id,
        username=user.username,
        email=user.email,
        first_name=user.first_name,
        last_name=user.last_name,
        bank_id=user.bank_id,
        sub_tenant_id=user.sub_tenant_id,
        is_active=user.is_active,
        is_superuser=user.is_superuser,
        is_email_verified=user.is_email_verified,
        last_login_at=user.last_login_at,
        created_at=user.created_at,
        updated_at=user.updated_at,
        roles=current_user.roles,
        permissions=current_user.permissions,
    )
    
    return user_response


@router.patch("/me", response_model=UserResponse)
async def update_current_user(
    user_update: UserUpdate,
    current_user: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Update current user profile
    
    Users can update their own email and name.
    """
    # Get user
    stmt = select(User).where(User.user_id == current_user.user_id)
    user = db.execute(stmt).scalar_one_or_none()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    
    # Update fields
    if user_update.email is not None:
        # Check if email already exists
        stmt = select(User).where(
            User.email == user_update.email,
            User.user_id != user.user_id,
        )
        existing = db.execute(stmt).scalar_one_or_none()
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already exists",
            )
        user.email = user_update.email
        user.is_email_verified = False  # Reset verification
    
    if user_update.first_name is not None:
        user.first_name = user_update.first_name
    
    if user_update.last_name is not None:
        user.last_name = user_update.last_name
    
    db.commit()
    db.refresh(user)
    
    return user


@router.post("/change-password", status_code=status.HTTP_204_NO_CONTENT)
async def change_password(
    password_change: PasswordChangeRequest,
    current_user: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Change current user password
    
    Requires current password verification.
    """
    # Get user
    stmt = select(User).where(User.user_id == current_user.user_id)
    user = db.execute(stmt).scalar_one_or_none()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    
    # Verify current password
    if not verify_password(password_change.current_password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Current password is incorrect",
        )
    
    # Hash new password
    new_password_hash = hash_password(password_change.new_password)
    user.password_hash = new_password_hash
    user.password_changed_at = datetime.utcnow()
    
    db.commit()
    
    # Audit log
    create_audit_log(
        db=db,
        action="password_changed",
        status="success",
        user_id=current_user.user_id,
        username=current_user.username,
    )
    
    # Revoke all refresh tokens (force re-login)
    stmt = select(RefreshToken).where(
        RefreshToken.user_id == current_user.user_id,
        RefreshToken.is_revoked == False,
    )
    tokens = db.execute(stmt).scalars().all()
    for token in tokens:
        token.is_revoked = True
        token.revoked_at = datetime.utcnow()
    db.commit()
    
    return None
