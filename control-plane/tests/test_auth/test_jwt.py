"""Unit tests for JWT token utilities"""
import pytest
from datetime import datetime, timedelta
from jose import jwt, JWTError
from uuid import uuid4

from app.auth.jwt import (
    create_access_token,
    create_refresh_token,
    decode_token,
    verify_token,
    extract_user_id,
    extract_tenant_context,
)
from app.config import JWT_SECRET_KEY, JWT_ALGORITHM, JWT_EXPIRATION_MINUTES


class TestTokenCreation:
    """Test JWT token creation"""
    
    def test_create_access_token_basic(self):
        """Test creating basic access token"""
        user_id = str(uuid4())
        username = "testuser"
        
        token = create_access_token(
            user_id=user_id,
            username=username,
        )
        
        assert isinstance(token, str)
        assert len(token) > 0
        
        # Decode and verify payload
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        assert payload["sub"] == user_id
        assert payload["username"] == username
        assert payload["type"] == "access"
    
    def test_create_access_token_with_tenant_context(self):
        """Test creating access token with tenant context"""
        user_id = str(uuid4())
        bank_id = str(uuid4())
        tenant_id = str(uuid4())
        
        token = create_access_token(
            user_id=user_id,
            username="testuser",
            bank_id=bank_id,
            sub_tenant_id=tenant_id,
        )
        
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        assert payload["bank_id"] == bank_id
        assert payload["sub_tenant_id"] == tenant_id
    
    def test_create_access_token_with_roles_and_permissions(self):
        """Test creating access token with roles and permissions"""
        user_id = str(uuid4())
        roles = ["admin", "user"]
        permissions = ["sources:read", "sources:create"]
        
        token = create_access_token(
            user_id=user_id,
            username="testuser",
            roles=roles,
            permissions=permissions,
        )
        
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        assert payload["roles"] == roles
        assert payload["permissions"] == permissions
    
    def test_create_access_token_with_superuser(self):
        """Test creating access token for superuser"""
        user_id = str(uuid4())
        
        token = create_access_token(
            user_id=user_id,
            username="admin",
            is_superuser=True,
        )
        
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        assert payload["is_superuser"] is True
    
    def test_create_access_token_expiration(self):
        """Test that access token has correct expiration"""
        user_id = str(uuid4())
        
        token = create_access_token(
            user_id=user_id,
            username="testuser",
        )
        
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        exp_timestamp = payload["exp"]
        
        # Check expiration is approximately JWT_EXPIRATION_MINUTES in the future
        expected_exp = datetime.utcnow() + timedelta(minutes=JWT_EXPIRATION_MINUTES)
        actual_exp = datetime.fromtimestamp(exp_timestamp)
        
        time_diff = abs((expected_exp - actual_exp).total_seconds())
        assert time_diff < 5  # Within 5 seconds
    
    def test_create_refresh_token_basic(self):
        """Test creating refresh token"""
        user_id = str(uuid4())
        username = "testuser"
        
        token = create_refresh_token(
            user_id=user_id,
            username=username,
        )
        
        assert isinstance(token, str)
        assert len(token) > 0
        
        # Decode and verify payload
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        assert payload["sub"] == user_id
        assert payload["username"] == username
        assert payload["type"] == "refresh"
    
    def test_create_refresh_token_expiration(self):
        """Test that refresh token has longer expiration"""
        user_id = str(uuid4())
        
        token = create_refresh_token(
            user_id=user_id,
            username="testuser",
        )
        
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        exp_timestamp = payload["exp"]
        
        # Refresh token should expire in 7 days
        expected_exp = datetime.utcnow() + timedelta(days=7)
        actual_exp = datetime.fromtimestamp(exp_timestamp)
        
        time_diff = abs((expected_exp - actual_exp).total_seconds())
        assert time_diff < 5  # Within 5 seconds


class TestTokenDecoding:
    """Test JWT token decoding"""
    
    def test_decode_token_valid(self):
        """Test decoding valid token"""
        user_id = str(uuid4())
        token = create_access_token(user_id=user_id, username="testuser")
        
        payload = decode_token(token)
        assert payload["sub"] == user_id
        assert payload["type"] == "access"
    
    def test_decode_token_invalid_signature(self):
        """Test decoding token with invalid signature"""
        token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
        
        with pytest.raises(JWTError):
            decode_token(token)
    
    def test_decode_token_malformed(self):
        """Test decoding malformed token"""
        token = "not.a.valid.jwt.token"
        
        with pytest.raises(JWTError):
            decode_token(token)
    
    def test_decode_token_expired(self):
        """Test decoding expired token"""
        user_id = str(uuid4())
        
        # Create token that expired 1 hour ago
        payload = {
            "sub": user_id,
            "username": "testuser",
            "exp": datetime.utcnow() - timedelta(hours=1),
            "type": "access",
        }
        token = jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
        
        with pytest.raises(JWTError):
            decode_token(token)
    
    def test_verify_token_access_type(self):
        """Test verifying token with correct type"""
        user_id = str(uuid4())
        token = create_access_token(user_id=user_id, username="testuser")
        
        payload = verify_token(token, expected_type="access")
        assert payload["type"] == "access"
    
    def test_verify_token_wrong_type(self):
        """Test verifying token with wrong type"""
        user_id = str(uuid4())
        token = create_access_token(user_id=user_id, username="testuser")
        
        with pytest.raises(ValueError, match="Invalid token type"):
            verify_token(token, expected_type="refresh")
    
    def test_verify_token_refresh_type(self):
        """Test verifying refresh token"""
        user_id = str(uuid4())
        token = create_refresh_token(user_id=user_id, username="testuser")
        
        payload = verify_token(token, expected_type="refresh")
        assert payload["type"] == "refresh"


class TestTokenExtraction:
    """Test token data extraction"""
    
    def test_extract_user_id(self):
        """Test extracting user ID from token"""
        user_id = str(uuid4())
        token = create_access_token(user_id=user_id, username="testuser")
        
        extracted_id = extract_user_id(token)
        assert extracted_id == user_id
    
    def test_extract_user_id_invalid_token(self):
        """Test extracting user ID from invalid token"""
        token = "invalid.jwt.token"
        
        extracted_id = extract_user_id(token)
        assert extracted_id is None
    
    def test_extract_tenant_context_complete(self):
        """Test extracting complete tenant context"""
        user_id = str(uuid4())
        bank_id = str(uuid4())
        tenant_id = str(uuid4())
        
        token = create_access_token(
            user_id=user_id,
            username="testuser",
            bank_id=bank_id,
            sub_tenant_id=tenant_id,
            is_superuser=False,
        )
        
        context = extract_tenant_context(token)
        assert context["user_id"] == user_id
        assert context["bank_id"] == bank_id
        assert context["sub_tenant_id"] == tenant_id
        assert context["is_superuser"] is False
    
    def test_extract_tenant_context_superuser(self):
        """Test extracting tenant context for superuser"""
        user_id = str(uuid4())
        
        token = create_access_token(
            user_id=user_id,
            username="admin",
            is_superuser=True,
        )
        
        context = extract_tenant_context(token)
        assert context["user_id"] == user_id
        assert context["bank_id"] is None
        assert context["sub_tenant_id"] is None
        assert context["is_superuser"] is True
    
    def test_extract_tenant_context_invalid_token(self):
        """Test extracting tenant context from invalid token"""
        token = "invalid.jwt.token"
        
        context = extract_tenant_context(token)
        assert context == {}


class TestTokenSecurity:
    """Test token security aspects"""
    
    def test_token_signature_required(self):
        """Test that token requires valid signature"""
        # Create unsigned token
        payload = {
            "sub": str(uuid4()),
            "username": "testuser",
            "exp": datetime.utcnow() + timedelta(minutes=30),
            "type": "access",
        }
        # Encode with wrong secret
        token = jwt.encode(payload, "wrong_secret", algorithm=JWT_ALGORITHM)
        
        with pytest.raises(JWTError):
            decode_token(token)
    
    def test_token_cannot_be_modified(self):
        """Test that token payload cannot be modified"""
        user_id = str(uuid4())
        token = create_access_token(user_id=user_id, username="testuser")
        
        # Try to decode, modify, and re-encode
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        payload["is_superuser"] = True  # Attempt privilege escalation
        
        # Re-encode with wrong secret (attacker doesn't have real secret)
        modified_token = jwt.encode(payload, "attacker_secret", algorithm=JWT_ALGORITHM)
        
        # Should fail verification
        with pytest.raises(JWTError):
            decode_token(modified_token)
    
    def test_token_expiration_enforced(self):
        """Test that expired tokens are rejected"""
        user_id = str(uuid4())
        
        # Create expired token
        payload = {
            "sub": user_id,
            "username": "testuser",
            "exp": datetime.utcnow() - timedelta(seconds=1),
            "type": "access",
        }
        expired_token = jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
        
        with pytest.raises(JWTError):
            decode_token(expired_token)
    
    def test_token_type_isolation(self):
        """Test that access and refresh tokens are isolated"""
        user_id = str(uuid4())
        
        access_token = create_access_token(user_id=user_id, username="testuser")
        refresh_token = create_refresh_token(user_id=user_id, username="testuser")
        
        # Access token should not verify as refresh token
        with pytest.raises(ValueError):
            verify_token(access_token, expected_type="refresh")
        
        # Refresh token should not verify as access token
        with pytest.raises(ValueError):
            verify_token(refresh_token, expected_type="access")
