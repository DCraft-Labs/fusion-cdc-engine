"""
Pydantic schemas for authentication endpoints
Request/response models for login, register, token refresh, etc.
"""
from pydantic import BaseModel, EmailStr, Field, validator
from typing import Optional, List
from uuid import UUID
from datetime import datetime


# ============================================================================
# Authentication Schemas
# ============================================================================

class LoginRequest(BaseModel):
    """Login request with username/email and password"""
    username: str = Field(..., description="Username or email")
    password: str = Field(..., min_length=8, description="Password")
    
    class Config:
        json_schema_extra = {
            "example": {
                "username": "john.doe@example.com",
                "password": "SecurePassword123!",
            }
        }


class TokenResponse(BaseModel):
    """Token response after successful login"""
    access_token: str = Field(..., description="JWT access token")
    refresh_token: str = Field(..., description="JWT refresh token")
    token_type: str = Field(default="bearer", description="Token type")
    expires_in: int = Field(..., description="Access token expiration in seconds")
    
    class Config:
        json_schema_extra = {
            "example": {
                "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                "token_type": "bearer",
                "expires_in": 1800,
            }
        }


class RefreshTokenRequest(BaseModel):
    """Refresh token request"""
    refresh_token: str = Field(..., description="Refresh token to exchange for new access token")
    
    class Config:
        json_schema_extra = {
            "example": {
                "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
            }
        }


class LogoutRequest(BaseModel):
    """Logout request (revoke refresh token)"""
    refresh_token: Optional[str] = Field(None, description="Refresh token to revoke (optional)")


# ============================================================================
# User Schemas
# ============================================================================

class UserBase(BaseModel):
    """Base user schema"""
    username: str = Field(..., min_length=3, max_length=255)
    email: EmailStr
    first_name: Optional[str] = Field(None, max_length=255)
    last_name: Optional[str] = Field(None, max_length=255)


class UserCreate(UserBase):
    """Create user request"""
    password: str = Field(..., min_length=8, description="Strong password required")
    bank_id: Optional[UUID] = Field(None, description="Bank ID (null for superadmins)")
    sub_tenant_id: Optional[UUID] = Field(None, description="Sub-tenant ID (null for bank admins)")
    is_superuser: bool = Field(default=False)
    
    @validator('password')
    def validate_password_strength(cls, v):
        """Validate password strength"""
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters')
        if not any(c.isupper() for c in v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not any(c.islower() for c in v):
            raise ValueError('Password must contain at least one lowercase letter')
        if not any(c.isdigit() for c in v):
            raise ValueError('Password must contain at least one digit')
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "username": "john.doe",
                "email": "john.doe@example.com",
                "first_name": "John",
                "last_name": "Doe",
                "password": "SecurePass123!",
                "bank_id": "123e4567-e89b-12d3-a456-426614174000",
                "sub_tenant_id": "123e4567-e89b-12d3-a456-426614174001",
            }
        }


class UserUpdate(BaseModel):
    """Update user request"""
    email: Optional[EmailStr] = None
    first_name: Optional[str] = Field(None, max_length=255)
    last_name: Optional[str] = Field(None, max_length=255)
    is_active: Optional[bool] = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "email": "john.new@example.com",
                "first_name": "John",
                "last_name": "Doe Updated",
            }
        }


class PasswordChangeRequest(BaseModel):
    """Change password request"""
    current_password: str = Field(..., description="Current password")
    new_password: str = Field(..., min_length=8, description="New password")
    
    @validator('new_password')
    def validate_password_strength(cls, v):
        """Validate password strength"""
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters')
        if not any(c.isupper() for c in v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not any(c.islower() for c in v):
            raise ValueError('Password must contain at least one lowercase letter')
        if not any(c.isdigit() for c in v):
            raise ValueError('Password must contain at least one digit')
        return v


class UserResponse(UserBase):
    """User response"""
    user_id: UUID
    bank_id: Optional[UUID]
    sub_tenant_id: Optional[UUID]
    is_active: bool
    is_superuser: bool
    is_email_verified: bool
    last_login_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "username": "john.doe",
                "email": "john.doe@example.com",
                "first_name": "John",
                "last_name": "Doe",
                "bank_id": "123e4567-e89b-12d3-a456-426614174001",
                "sub_tenant_id": "123e4567-e89b-12d3-a456-426614174002",
                "is_active": True,
                "is_superuser": False,
                "is_email_verified": True,
                "last_login_at": "2025-12-08T10:30:00Z",
                "created_at": "2025-12-01T10:00:00Z",
                "updated_at": "2025-12-08T10:30:00Z",
            }
        }


class UserWithRoles(UserResponse):
    """User response with roles"""
    roles: List[str] = Field(default_factory=list, description="List of role names")
    permissions: List[str] = Field(default_factory=list, description="List of permission names")


# ============================================================================
# Role Schemas
# ============================================================================

class RoleBase(BaseModel):
    """Base role schema"""
    role_name: str = Field(..., min_length=3, max_length=100)
    display_name: str = Field(..., max_length=255)
    description: Optional[str] = Field(None, max_length=500)
    role_level: str = Field(..., description="Role level: superadmin, bank_admin, tenant_admin, user, viewer")


class RoleCreate(RoleBase):
    """Create role request"""
    bank_id: Optional[UUID] = None
    sub_tenant_id: Optional[UUID] = None
    is_system_role: bool = Field(default=False)


class RoleUpdate(BaseModel):
    """Update role request"""
    display_name: Optional[str] = Field(None, max_length=255)
    description: Optional[str] = Field(None, max_length=500)
    is_active: Optional[bool] = None


class RoleResponse(RoleBase):
    """Role response"""
    role_id: UUID
    bank_id: Optional[UUID]
    sub_tenant_id: Optional[UUID]
    is_active: bool
    is_system_role: bool
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


# ============================================================================
# Permission Schemas
# ============================================================================

class PermissionBase(BaseModel):
    """Base permission schema"""
    permission_name: str = Field(..., min_length=3, max_length=100)
    display_name: str = Field(..., max_length=255)
    description: Optional[str] = Field(None, max_length=500)
    resource: str = Field(..., max_length=100, description="Resource type: sources, connections, etc.")
    action: str = Field(..., max_length=50, description="Action: read, create, update, delete, execute")
    scope: str = Field(default="tenant", description="Scope: global, bank, tenant")


class PermissionCreate(PermissionBase):
    """Create permission request"""
    pass


class PermissionResponse(PermissionBase):
    """Permission response"""
    permission_id: UUID
    is_active: bool
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


# ============================================================================
# Current User Schemas
# ============================================================================

class CurrentUserResponse(UserWithRoles):
    """Current authenticated user response"""
    pass


# ============================================================================
# Audit Log Schemas
# ============================================================================

class AuditLogResponse(BaseModel):
    """Audit log response"""
    log_id: UUID
    user_id: Optional[UUID]
    username: Optional[str]
    action: str
    resource_type: Optional[str]
    resource_id: Optional[UUID]
    status: str
    details: dict
    ip_address: Optional[str]
    created_at: datetime
    
    class Config:
        from_attributes = True
