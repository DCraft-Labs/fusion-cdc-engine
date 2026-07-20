"""
JWT token generation, validation, and management
Handles access tokens and refresh tokens with proper expiration
"""
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from jose import JWTError, jwt
from uuid import UUID
from app.config import settings


def create_access_token(
    user_id,
    username: str,
    bank_id=None,
    sub_tenant_id=None,
    roles: list = None,
    permissions: list = None,
    is_superuser: bool = False,
    expires_delta: Optional[timedelta] = None,
) -> str:
    """
    Create a JWT access token with user context
    
    Args:
        user_id: User's UUID
        username: Username
        bank_id: Bank ID for multi-tenancy
        sub_tenant_id: Sub-tenant ID for multi-tenancy
        roles: List of role names
        permissions: List of permission names
        expires_delta: Custom expiration time, defaults to config value
        
    Returns:
        Encoded JWT token string
    """
    if roles is None:
        roles = []
    if permissions is None:
        permissions = []
        
    # Calculate expiration
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.JWT_EXPIRATION_MINUTES)
    
    # Build token payload
    payload = {
        "sub": str(user_id),
        "username": username,
        "exp": expire,
        "iat": datetime.utcnow(),
        "type": "access",
        "roles": roles,
        "permissions": permissions,
        "is_superuser": is_superuser,
    }
    
    # Add tenant context if provided
    if bank_id:
        payload["bank_id"] = str(bank_id)
    if sub_tenant_id:
        payload["sub_tenant_id"] = str(sub_tenant_id)
    
    # Encode token
    encoded_jwt = jwt.encode(
        payload,
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM
    )
    
    return encoded_jwt


def create_refresh_token(
    user_id,
    username: str = "",
    expires_delta: Optional[timedelta] = None,
) -> str:
    """
    Create a JWT refresh token
    
    Args:
        user_id: User's UUID
        expires_delta: Custom expiration time, defaults to 7 days
        
    Returns:
        Encoded JWT refresh token string
    """
    # Refresh tokens last longer (7 days by default)
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(days=7)
    
    # Build token payload (minimal for refresh tokens)
    payload = {
        "sub": str(user_id),
        "username": username,
        "exp": expire,
        "iat": datetime.utcnow(),
        "type": "refresh",
    }
    
    # Encode token
    encoded_jwt = jwt.encode(
        payload,
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM
    )
    
    return encoded_jwt


def decode_token(token: str) -> Dict[str, Any]:
    """
    Decode and validate a JWT token
    
    Args:
        token: JWT token string
        
    Returns:
        Decoded token payload
        
    Raises:
        JWTError: If token is invalid or expired
    """
    try:
        payload = jwt.decode(
            token,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM]
        )
        return payload
    except JWTError as e:
        raise JWTError(f"Token validation failed: {str(e)}")


def verify_token(token: str, token_type: str = "access", expected_type: str = None) -> Optional[Dict[str, Any]]:
    """
    Verify a JWT token and check its type.

    Args:
        token: JWT token string
        token_type: Expected token type ("access" or "refresh") — legacy param
        expected_type: Alias for token_type — preferred param name in tests

    Returns:
        Decoded payload if valid

    Raises:
        ValueError: If token type doesn't match expected type
        JWTError: If token is invalid or expired
    """
    check_type = expected_type if expected_type is not None else token_type
    try:
        payload = decode_token(token)

        # Verify token type
        if payload.get("type") != check_type:
            raise ValueError(f"Invalid token type: expected '{check_type}', got '{payload.get('type')}'") 

        # Verify expiration (already checked by decode, but explicit)
        exp = payload.get("exp")
        if exp and datetime.fromtimestamp(exp) < datetime.utcnow():
            return None

        return payload
    except ValueError:
        raise
    except JWTError:
        return None


def extract_user_id(token: str) -> Optional[str]:
    """
    Extract user ID string from a JWT token.

    Returns the raw string (not UUID) to match how callers typically compare.
    """
    try:
        payload = decode_token(token)
        return payload.get("sub")
    except (JWTError, ValueError):
        return None


def extract_tenant_context(token: str) -> Dict[str, Any]:
    """
    Extract full tenant context from a JWT token.

    Returns a dict with: user_id, bank_id, sub_tenant_id, is_superuser.
    All UUID values are returned as strings.
    """
    try:
        payload = decode_token(token)
        return {
            "user_id": payload.get("sub"),
            "bank_id": payload.get("bank_id"),
            "sub_tenant_id": payload.get("sub_tenant_id"),
            "is_superuser": payload.get("is_superuser", False),
        }
    except (JWTError, ValueError):
        return {}
