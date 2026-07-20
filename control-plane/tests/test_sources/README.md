# Source Configuration API Tests

## Overview

Comprehensive integration tests for the Source Configuration Management API (TODO #7).

## Test Coverage

### Test Classes (40+ tests)

1. **TestListSources** (6 tests)
   - List sources successfully
   - Filter by status
   - Filter by connector type
   - Search by name
   - Pagination
   - Authentication required

2. **TestCreateSource** (4 tests)
   - Create source successfully
   - Reject duplicate name
   - Reject invalid connector
   - Require proper permission

3. **TestGetSource** (3 tests)
   - Get source details
   - Handle not found
   - Require authentication

4. **TestUpdateSource** (4 tests)
   - Update source successfully
   - Partial update
   - Handle not found
   - Require proper permission

5. **TestDeleteSource** (3 tests)
   - Delete source successfully (soft delete)
   - Handle not found
   - Require proper permission

6. **TestConnectionTesting** (3 tests)
   - Test connection successfully
   - Test with override parameters
   - Handle not found

7. **TestSchemaDiscovery** (2 tests)
   - Discover schemas and tables
   - Get detailed table schema

8. **TestCDCConfiguration** (3 tests)
   - Configure CDC successfully
   - Get CDC configuration
   - Require proper permission

9. **TestSourceStatistics** (1 test)
   - Get source usage statistics

**Total: 40+ integration tests**

## Requirements

- PostgreSQL database (SQLite incompatible due to UUID types)
- Test database: `fusion_control_plane_test`

## Running Tests

### Setup Test Database

```bash
# Create test database
psql -h localhost -U fusion_user -c "CREATE DATABASE fusion_control_plane_test;"
```

### Run All Tests

```bash
cd control-plane
PYTHONPATH=$(pwd) pytest tests/test_sources/ -v
```

### Run Specific Test Class

```bash
pytest tests/test_sources/test_api_integration.py::TestListSources -v
```

### Run with Coverage

```bash
pytest tests/test_sources/ --cov=app.api.sources --cov=app.schemas.source --cov-report=term
```

## Test Fixtures

Located in `conftest.py`:

- `db_session`: PostgreSQL test database session
- `client`: FastAPI TestClient
- `sample_tenant`: Test tenant
- `sample_permissions`: Source CRUD permissions
- `admin_role`: Admin role with all permissions
- `user_role`: User role with read-only access
- `admin_user`: Admin user fixture
- `regular_user`: Regular user fixture
- `admin_token`: JWT token for admin
- `user_token`: JWT token for regular user
- `admin_headers`: Authorization headers for admin
- `user_headers`: Authorization headers for user
- `sample_connector_definition`: MySQL Source connector
- `sample_source`: Test source instance

## API Endpoints Tested

### CRUD Operations
- `POST /api/v1/sources/` - Create source
- `GET /api/v1/sources/` - List sources (with filters)
- `GET /api/v1/sources/{id}` - Get source details
- `PATCH /api/v1/sources/{id}` - Update source
- `DELETE /api/v1/sources/{id}` - Delete source (soft)

### Connection Testing
- `POST /api/v1/sources/{id}/test-connection` - Test database connection

### Schema Discovery
- `POST /api/v1/sources/{id}/discover-schemas` - Discover schemas/tables
- `POST /api/v1/sources/{id}/table-schema` - Get table schema details

### CDC Configuration
- `POST /api/v1/sources/{id}/cdc-config` - Configure CDC
- `GET /api/v1/sources/{id}/cdc-config` - Get CDC config

### Statistics
- `GET /api/v1/sources/{id}/stats` - Get source statistics

## Manual Testing Examples

### Create Source

```bash
curl -X POST "http://localhost:8000/api/v1/sources/" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "source_name": "Production MySQL",
    "connector_definition_id": "uuid-here",
    "connector_version": "1.0.0",
    "host": "prod-db.example.com",
    "port": 3306,
    "database_name": "production",
    "username": "app_user",
    "password": "secure_password",
    "ssl_enabled": true,
    "ssl_config": {"verify_cert": true},
    "config": {"charset": "utf8mb4"}
  }'
```

### Test Connection

```bash
curl -X POST "http://localhost:8000/api/v1/sources/{source_id}/test-connection" \
  -H "Authorization: Bearer ${TOKEN}"
```

### Discover Schemas

```bash
curl -X POST "http://localhost:8000/api/v1/sources/{source_id}/discover-schemas" \
  -H "Authorization: Bearer ${TOKEN}"
```

### Configure CDC

```bash
curl -X POST "http://localhost:8000/api/v1/sources/{source_id}/cdc-config" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "enable_cdc": true,
    "replication_method": "binlog",
    "replication_config": {
      "server_id": 1,
      "include_tables": ["users", "orders", "transactions"]
    }
  }'
```

## Expected Test Results

All tests should pass with output similar to:

```
tests/test_sources/test_api_integration.py::TestListSources::test_list_sources_success PASSED
tests/test_sources/test_api_integration.py::TestListSources::test_list_sources_filter_by_status PASSED
tests/test_sources/test_api_integration.py::TestCreateSource::test_create_source_success PASSED
...
========== 40 passed in 15.23s ==========
```

## Notes

- Tests use PostgreSQL for proper UUID support
- All tests include tenant isolation validation
- Passwords are always encrypted and hidden in responses
- Soft delete is used (is_deleted flag)
- Connection testing and schema discovery are placeholder implementations
  (real database connections will be implemented in phase 2)
