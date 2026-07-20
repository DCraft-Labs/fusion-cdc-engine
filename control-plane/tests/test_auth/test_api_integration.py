"""Integration tests for authentication API endpoints"""
import pytest
from fastapi import status
from datetime import datetime, timedelta


class TestUserRegistration:
    """Test user registration endpoint"""
    
    def test_register_user_success(self, client, sample_bank_id, sample_tenant_id):
        """Test successful user registration"""
        user_data = {
            "username": "newuser",
            "email": "newuser@example.com",
            "password": "NewPassword123!",
            "first_name": "New",
            "last_name": "User",
            "bank_id": str(sample_bank_id),
            "sub_tenant_id": str(sample_tenant_id),
        }
        
        response = client.post("/api/v1/auth/register", json=user_data)
        
        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        assert data["username"] == "newuser"
        assert data["email"] == "newuser@example.com"
        assert "password" not in data
        assert "password_hash" not in data
    
    def test_register_user_duplicate_username(self, client, sample_user, sample_bank_id, sample_tenant_id):
        """Test registration with duplicate username"""
        user_data = {
            "username": sample_user.username,  # Duplicate
            "email": "different@example.com",
            "password": "NewPassword123!",
            "bank_id": str(sample_bank_id),
            "sub_tenant_id": str(sample_tenant_id),
        }
        
        response = client.post("/api/v1/auth/register", json=user_data)
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "already exists" in response.json()["detail"].lower()
    
    def test_register_user_duplicate_email(self, client, sample_user, sample_bank_id, sample_tenant_id):
        """Test registration with duplicate email"""
        user_data = {
            "username": "differentuser",
            "email": sample_user.email,  # Duplicate
            "password": "NewPassword123!",
            "bank_id": str(sample_bank_id),
            "sub_tenant_id": str(sample_tenant_id),
        }
        
        response = client.post("/api/v1/auth/register", json=user_data)
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "already exists" in response.json()["detail"].lower()
    
    def test_register_user_weak_password(self, client, sample_bank_id, sample_tenant_id):
        """Test registration with weak password"""
        user_data = {
            "username": "weakpass",
            "email": "weakpass@example.com",
            "password": "weak",  # Too short, no uppercase, no digit
            "bank_id": str(sample_bank_id),
            "sub_tenant_id": str(sample_tenant_id),
        }
        
        response = client.post("/api/v1/auth/register", json=user_data)
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    
    def test_register_user_invalid_email(self, client, sample_bank_id, sample_tenant_id):
        """Test registration with invalid email"""
        user_data = {
            "username": "invalidemail",
            "email": "not-an-email",
            "password": "ValidPassword123!",
            "bank_id": str(sample_bank_id),
            "sub_tenant_id": str(sample_tenant_id),
        }
        
        response = client.post("/api/v1/auth/register", json=user_data)
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


class TestUserLogin:
    """Test user login endpoint"""
    
    def test_login_success_with_username(self, client, sample_user):
        """Test successful login with username"""
        login_data = {
            "username": sample_user.username,
            "password": "TestPassword123!",
        }
        
        response = client.post("/api/v1/auth/login", json=login_data)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "access_token" in data
        assert "refresh_token" in data
        assert data["token_type"] == "bearer"
        assert "expires_in" in data
    
    def test_login_success_with_email(self, client, sample_user):
        """Test successful login with email"""
        login_data = {
            "username": sample_user.email,  # Can use email as username
            "password": "TestPassword123!",
        }
        
        response = client.post("/api/v1/auth/login", json=login_data)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "access_token" in data
        assert "refresh_token" in data
    
    def test_login_wrong_password(self, client, sample_user):
        """Test login with wrong password"""
        login_data = {
            "username": sample_user.username,
            "password": "WrongPassword123!",
        }
        
        response = client.post("/api/v1/auth/login", json=login_data)
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
        assert "invalid credentials" in response.json()["detail"].lower()
    
    def test_login_nonexistent_user(self, client):
        """Test login with nonexistent user"""
        login_data = {
            "username": "nonexistent",
            "password": "Password123!",
        }
        
        response = client.post("/api/v1/auth/login", json=login_data)
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    
    def test_login_inactive_user(self, client, sample_inactive_user):
        """Test login with inactive account"""
        login_data = {
            "username": sample_inactive_user.username,
            "password": "InactivePassword123!",
        }
        
        response = client.post("/api/v1/auth/login", json=login_data)
        
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "inactive" in response.json()["detail"].lower()
    
    def test_login_missing_fields(self, client):
        """Test login with missing fields"""
        login_data = {"username": "testuser"}  # Missing password
        
        response = client.post("/api/v1/auth/login", json=login_data)
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


class TestTokenRefresh:
    """Test token refresh endpoint"""
    
    def test_refresh_token_success(self, client, valid_refresh_token):
        """Test successful token refresh"""
        refresh_data = {"refresh_token": valid_refresh_token}
        
        response = client.post("/api/v1/auth/refresh", json=refresh_data)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "access_token" in data
        assert "refresh_token" in data
        assert data["token_type"] == "bearer"
    
    def test_refresh_token_invalid(self, client):
        """Test refresh with invalid token"""
        refresh_data = {"refresh_token": "invalid.jwt.token"}
        
        response = client.post("/api/v1/auth/refresh", json=refresh_data)
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    
    def test_refresh_token_expired(self, client, db_session, sample_user):
        """Test refresh with expired token"""
        from app.auth.jwt import create_refresh_token
        from jose import jwt
        from app.config import JWT_SECRET_KEY, JWT_ALGORITHM
        import hashlib
        from app.models.auth import RefreshToken
        
        # Create expired token
        payload = {
            "sub": str(sample_user.user_id),
            "username": sample_user.username,
            "exp": datetime.utcnow() - timedelta(hours=1),
            "type": "refresh",
        }
        expired_token = jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
        
        # Store in database
        token_hash = hashlib.sha256(expired_token.encode()).hexdigest()
        refresh_token_record = RefreshToken(
            user_id=str(sample_user.user_id),
            token_hash=token_hash,
            expires_at=datetime.utcnow() - timedelta(hours=1),
        )
        db_session.add(refresh_token_record)
        db_session.commit()
        
        refresh_data = {"refresh_token": expired_token}
        
        response = client.post("/api/v1/auth/refresh", json=refresh_data)
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED


class TestUserLogout:
    """Test user logout endpoint"""
    
    def test_logout_success(self, client, auth_headers, valid_refresh_token):
        """Test successful logout"""
        logout_data = {"refresh_token": valid_refresh_token}
        
        response = client.post("/api/v1/auth/logout", json=logout_data, headers=auth_headers)
        
        assert response.status_code == status.HTTP_204_NO_CONTENT
    
    def test_logout_without_token(self, client, auth_headers):
        """Test logout without refresh token (revokes all)"""
        logout_data = {}
        
        response = client.post("/api/v1/auth/logout", json=logout_data, headers=auth_headers)
        
        assert response.status_code == status.HTTP_204_NO_CONTENT
    
    def test_logout_without_auth(self, client):
        """Test logout without authentication"""
        logout_data = {}
        
        response = client.post("/api/v1/auth/logout", json=logout_data)
        
        assert response.status_code == status.HTTP_403_FORBIDDEN


class TestGetCurrentUser:
    """Test get current user endpoint"""
    
    def test_get_current_user_success(self, client, auth_headers, sample_user):
        """Test getting current user info"""
        response = client.get("/api/v1/auth/me", headers=auth_headers)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["username"] == sample_user.username
        assert data["email"] == sample_user.email
        assert "roles" in data
        assert "permissions" in data
    
    def test_get_current_user_without_auth(self, client):
        """Test getting current user without authentication"""
        response = client.get("/api/v1/auth/me")
        
        assert response.status_code == status.HTTP_403_FORBIDDEN
    
    def test_get_current_user_invalid_token(self, client):
        """Test getting current user with invalid token"""
        headers = {"Authorization": "Bearer invalid.jwt.token"}
        
        response = client.get("/api/v1/auth/me", headers=headers)
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    
    def test_get_current_user_expired_token(self, client, expired_access_token):
        """Test getting current user with expired token"""
        headers = {"Authorization": f"Bearer {expired_access_token}"}
        
        response = client.get("/api/v1/auth/me", headers=headers)
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED


class TestUpdateCurrentUser:
    """Test update current user endpoint"""
    
    def test_update_current_user_success(self, client, auth_headers):
        """Test updating current user info"""
        update_data = {
            "first_name": "Updated",
            "last_name": "Name",
        }
        
        response = client.patch("/api/v1/auth/me", json=update_data, headers=auth_headers)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["first_name"] == "Updated"
        assert data["last_name"] == "Name"
    
    def test_update_current_user_email(self, client, auth_headers):
        """Test updating email"""
        update_data = {"email": "newemail@example.com"}
        
        response = client.patch("/api/v1/auth/me", json=update_data, headers=auth_headers)
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["email"] == "newemail@example.com"
        assert data["is_email_verified"] is False  # Should reset verification
    
    def test_update_current_user_duplicate_email(self, client, auth_headers, sample_superuser):
        """Test updating to existing email"""
        update_data = {"email": sample_superuser.email}  # Email already exists
        
        response = client.patch("/api/v1/auth/me", json=update_data, headers=auth_headers)
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST


class TestChangePassword:
    """Test change password endpoint"""
    
    def test_change_password_success(self, client, auth_headers, sample_user):
        """Test successful password change"""
        password_data = {
            "current_password": "TestPassword123!",
            "new_password": "NewPassword456!",
        }
        
        response = client.post("/api/v1/auth/change-password", json=password_data, headers=auth_headers)
        
        assert response.status_code == status.HTTP_204_NO_CONTENT
        
        # Verify can login with new password
        login_data = {
            "username": sample_user.username,
            "password": "NewPassword456!",
        }
        login_response = client.post("/api/v1/auth/login", json=login_data)
        assert login_response.status_code == status.HTTP_200_OK
    
    def test_change_password_wrong_current(self, client, auth_headers):
        """Test password change with wrong current password"""
        password_data = {
            "current_password": "WrongPassword123!",
            "new_password": "NewPassword456!",
        }
        
        response = client.post("/api/v1/auth/change-password", json=password_data, headers=auth_headers)
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
    
    def test_change_password_weak_new(self, client, auth_headers):
        """Test password change with weak new password"""
        password_data = {
            "current_password": "TestPassword123!",
            "new_password": "weak",
        }
        
        response = client.post("/api/v1/auth/change-password", json=password_data, headers=auth_headers)
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


class TestAuthenticationFlow:
    """Test complete authentication flows"""
    
    def test_full_registration_login_flow(self, client, sample_bank_id, sample_tenant_id):
        """Test complete registration and login flow"""
        # 1. Register
        user_data = {
            "username": "flowuser",
            "email": "flowuser@example.com",
            "password": "FlowPassword123!",
            "bank_id": str(sample_bank_id),
            "sub_tenant_id": str(sample_tenant_id),
        }
        
        reg_response = client.post("/api/v1/auth/register", json=user_data)
        assert reg_response.status_code == status.HTTP_201_CREATED
        
        # 2. Login
        login_data = {
            "username": "flowuser",
            "password": "FlowPassword123!",
        }
        
        login_response = client.post("/api/v1/auth/login", json=login_data)
        assert login_response.status_code == status.HTTP_200_OK
        
        tokens = login_response.json()
        access_token = tokens["access_token"]
        
        # 3. Access protected endpoint
        headers = {"Authorization": f"Bearer {access_token}"}
        me_response = client.get("/api/v1/auth/me", headers=headers)
        assert me_response.status_code == status.HTTP_200_OK
        assert me_response.json()["username"] == "flowuser"
    
    def test_token_refresh_flow(self, client, sample_user, valid_refresh_token):
        """Test token refresh flow"""
        # 1. Get new access token using refresh token
        refresh_data = {"refresh_token": valid_refresh_token}
        
        refresh_response = client.post("/api/v1/auth/refresh", json=refresh_data)
        assert refresh_response.status_code == status.HTTP_200_OK
        
        tokens = refresh_response.json()
        new_access_token = tokens["access_token"]
        
        # 2. Use new access token
        headers = {"Authorization": f"Bearer {new_access_token}"}
        me_response = client.get("/api/v1/auth/me", headers=headers)
        assert me_response.status_code == status.HTTP_200_OK
        assert me_response.json()["username"] == sample_user.username
    
    def test_logout_invalidates_refresh_token(self, client, auth_headers, valid_refresh_token):
        """Test that logout invalidates refresh token"""
        # 1. Logout
        logout_data = {"refresh_token": valid_refresh_token}
        logout_response = client.post("/api/v1/auth/logout", json=logout_data, headers=auth_headers)
        assert logout_response.status_code == status.HTTP_204_NO_CONTENT
        
        # 2. Try to use refresh token (should fail)
        refresh_data = {"refresh_token": valid_refresh_token}
        refresh_response = client.post("/api/v1/auth/refresh", json=refresh_data)
        assert refresh_response.status_code == status.HTTP_401_UNAUTHORIZED
