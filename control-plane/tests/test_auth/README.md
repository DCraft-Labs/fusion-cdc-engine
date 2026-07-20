# Authentication Tests

Comprehensive unit and integration tests for the authentication system.

## Test Structure

```
tests/test_auth/
├── conftest.py                 # Test fixtures and configuration
├── test_password.py            # Password hashing tests
├── test_jwt.py                 # JWT token tests
├── test_rbac.py                # RBAC permission tests
└── test_api_integration.py     # API endpoint integration tests
```

## Running Tests

### Run all authentication tests
```bash
pytest tests/test_auth/ -v
```

### Run with coverage
```bash
pytest tests/test_auth/ --cov=app.auth --cov=app.api.auth --cov-report=html
```

### Run specific test file
```bash
pytest tests/test_auth/test_password.py -v
pytest tests/test_auth/test_jwt.py -v
pytest tests/test_auth/test_rbac.py -v
pytest tests/test_auth/test_api_integration.py -v
```

### Run specific test class
```bash
pytest tests/test_auth/test_password.py::TestPasswordHashing -v
```

### Run specific test
```bash
pytest tests/test_auth/test_jwt.py::TestTokenCreation::test_create_access_token_basic -v
```

## Test Coverage

### Password Hashing (`test_password.py`)
- ✅ Hash generation and verification
- ✅ Salt uniqueness
- ✅ Special characters and unicode
- ✅ Security aspects (one-way, timing attacks)
- ✅ Password rehashing detection

### JWT Tokens (`test_jwt.py`)
- ✅ Access token creation with tenant context
- ✅ Refresh token creation
- ✅ Token expiration
- ✅ Token decoding and validation
- ✅ Token type verification
- ✅ Security (signature, modification, expiration)
- ✅ User ID and tenant context extraction

### RBAC (`test_rbac.py`)
- ✅ Role hierarchy validation
- ✅ User roles and permissions retrieval
- ✅ Permission checking (single, any, all)
- ✅ Role checking and level comparison
- ✅ Tenant access control
- ✅ Bank access control
- ✅ Superuser privileges
- ✅ Edge cases and error handling

### API Integration (`test_api_integration.py`)
- ✅ User registration (success, duplicates, validation)
- ✅ User login (username, email, wrong password, inactive)
- ✅ Token refresh (valid, invalid, expired)
- ✅ User logout (with/without token)
- ✅ Get current user info
- ✅ Update current user
- ✅ Change password
- ✅ Complete authentication flows

## Test Fixtures

### Database Fixtures
- `db_session` - In-memory SQLite database session
- `client` - FastAPI test client with database override

### Sample Data Fixtures
- `sample_bank_id` - Sample bank UUID
- `sample_tenant_id` - Sample tenant UUID
- `sample_permission` - Sample "sources:read" permission
- `sample_role` - Sample "user" role with permissions
- `sample_user` - Sample active user with role
- `sample_superuser` - Sample superuser
- `sample_inactive_user` - Sample inactive user

### Token Fixtures
- `valid_access_token` - Valid JWT access token
- `expired_access_token` - Expired JWT access token
- `valid_refresh_token` - Valid refresh token (stored in DB)
- `auth_headers` - Authorization headers with valid token

## Test Database

Tests use an in-memory SQLite database for isolation and speed:
- Each test gets a fresh database
- No external dependencies required
- Fast execution
- Automatic cleanup

## Example Test Run

```bash
$ pytest tests/test_auth/ -v

tests/test_auth/test_password.py::TestPasswordHashing::test_hash_password_creates_different_hashes PASSED
tests/test_auth/test_password.py::TestPasswordHashing::test_verify_password_with_correct_password PASSED
tests/test_auth/test_jwt.py::TestTokenCreation::test_create_access_token_basic PASSED
tests/test_auth/test_jwt.py::TestTokenCreation::test_create_access_token_with_tenant_context PASSED
tests/test_auth/test_rbac.py::TestRoleHierarchy::test_role_hierarchy_completeness PASSED
tests/test_auth/test_rbac.py::TestPermissionChecking::test_has_permission_granted PASSED
tests/test_auth/test_api_integration.py::TestUserRegistration::test_register_user_success PASSED
tests/test_auth/test_api_integration.py::TestUserLogin::test_login_success_with_username PASSED
tests/test_auth/test_api_integration.py::TestTokenRefresh::test_refresh_token_success PASSED
tests/test_auth/test_api_integration.py::TestAuthenticationFlow::test_full_registration_login_flow PASSED

===================== 90+ tests passed in 2.50s ======================
```

## CI/CD Integration

Add to GitHub Actions / CI pipeline:

```yaml
- name: Run authentication tests
  run: |
    pytest tests/test_auth/ --cov=app.auth --cov=app.api.auth --cov-report=xml
    
- name: Upload coverage
  uses: codecov/codecov-action@v3
  with:
    file: ./coverage.xml
```

## Security Testing

### Password Security Tests
- Hash uniqueness (salt)
- One-way hashing
- Timing attack resistance
- Length consistency

### JWT Security Tests
- Signature verification
- Tampering detection
- Expiration enforcement
- Token type isolation

### RBAC Security Tests
- Permission isolation
- Role hierarchy enforcement
- Tenant isolation
- Superuser bypass checks

## Adding New Tests

1. Create test file in `tests/test_auth/`
2. Import fixtures from `conftest.py`
3. Use descriptive test names: `test_<what>_<scenario>`
4. Add docstrings explaining test purpose
5. Follow AAA pattern: Arrange, Act, Assert

Example:
```python
def test_feature_success(self, client, auth_headers):
    """Test successful feature execution"""
    # Arrange
    data = {"key": "value"}
    
    # Act
    response = client.post("/endpoint", json=data, headers=auth_headers)
    
    # Assert
    assert response.status_code == 200
    assert response.json()["key"] == "value"
```

## Debugging Tests

Run with verbose output and print statements:
```bash
pytest tests/test_auth/test_api_integration.py::TestUserLogin::test_login_success -v -s
```

Run with debugger on failure:
```bash
pytest tests/test_auth/ --pdb
```

## Performance Testing

Time individual tests:
```bash
pytest tests/test_auth/ --durations=10
```

## Next Steps

- Add performance/load tests for authentication endpoints
- Add security penetration tests
- Add rate limiting tests
- Add audit log verification tests
- Add multi-factor authentication tests (when implemented)
