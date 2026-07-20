# Destination API Integration Tests

## Overview

Comprehensive integration tests for the Destination Configuration Management API (TODO #8).

## Test Coverage

### 1. **List Destinations** (6 tests)
- ✅ List all destinations with pagination
- ✅ Filter by status (draft, active, inactive)
- ✅ Filter by connector definition
- ✅ Search by destination name
- ✅ Pagination (page, page_size)
- ✅ Authentication requirement

### 2. **Create Destination** (5 tests)
- ✅ Create database destination (PostgreSQL)
- ✅ Create storage destination (S3)
- ✅ Duplicate name validation
- ✅ Invalid connector validation
- ✅ Permission requirement (destinations:create)

### 3. **Get Destination** (3 tests)
- ✅ Get by ID
- ✅ Not found (404)
- ✅ Authentication requirement

### 4. **Update Destination** (4 tests)
- ✅ Full update
- ✅ Partial update
- ✅ Not found (404)
- ✅ Permission requirement (destinations:update)

### 5. **Delete Destination** (3 tests)
- ✅ Soft delete
- ✅ Not found (404)
- ✅ Permission requirement (destinations:delete)

### 6. **Connection Testing** (4 tests)
- ✅ Test with default credentials
- ✅ Test with override parameters
- ✅ Not found (404)
- ✅ Validate write permissions

### 7. **Write Mode Configuration** (5 tests)
- ✅ Configure append mode
- ✅ Configure upsert mode with primary keys
- ✅ Upsert validation (requires primary keys)
- ✅ Get write mode config
- ✅ Permission requirement (destinations:update)

### 8. **Schema Mapping** (3 tests)
- ✅ Configure column mappings
- ✅ Get schema mapping
- ✅ Not found (404)

### 9. **Batch Settings** (3 tests)
- ✅ Configure batch settings
- ✅ Get batch settings
- ✅ Permission requirement (destinations:update)

### 10. **Statistics** (1 test)
- ✅ Get destination usage statistics

**Total: 37 comprehensive tests** covering all API endpoints and error scenarios.

## Running Tests

### Prerequisites

1. **PostgreSQL Database**: Tests use `fusion_cdc_metadata` database
   ```bash
   # Ensure database is running
   docker ps | grep postgres
   
   # Check database exists
   docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata -c "SELECT 1"
   ```

2. **Python Environment**: Activate virtual environment
   ```bash
   cd control-plane
   source .venv/bin/activate
   ```

### Run All Tests

```bash
# Run all destination tests
pytest tests/test_destinations/ -v

# Run with detailed output
pytest tests/test_destinations/ -vv

# Run specific test class
pytest tests/test_destinations/test_api_integration.py::TestCreateDestination -v

# Run specific test
pytest tests/test_destinations/test_api_integration.py::TestCreateDestination::test_create_destination_success -v

# Run with coverage
pytest tests/test_destinations/ --cov=app.api.destinations --cov-report=term-missing
```

### Run Tests by Category

```bash
# CRUD operations
pytest tests/test_destinations/test_api_integration.py::TestListDestinations -v
pytest tests/test_destinations/test_api_integration.py::TestCreateDestination -v
pytest tests/test_destinations/test_api_integration.py::TestGetDestination -v
pytest tests/test_destinations/test_api_integration.py::TestUpdateDestination -v
pytest tests/test_destinations/test_api_integration.py::TestDeleteDestination -v

# Connection testing
pytest tests/test_destinations/test_api_integration.py::TestConnectionTesting -v

# Write mode configuration
pytest tests/test_destinations/test_api_integration.py::TestWriteModeConfiguration -v

# Schema mapping
pytest tests/test_destinations/test_api_integration.py::TestSchemaMapping -v

# Batch settings
pytest tests/test_destinations/test_api_integration.py::TestBatchSettings -v

# Statistics
pytest tests/test_destinations/test_api_integration.py::TestDestinationStatistics -v
```

## Test Fixtures

### Database Fixtures
- `db_session`: PostgreSQL test session with automatic cleanup
- `client`: FastAPI TestClient with database override

### Authentication Fixtures
- `sample_tenant`: Tenant UUID for isolation
- `sample_permissions`: Destination permissions (create, read, update, delete)
- `admin_role`: Role with all destination permissions
- `user_role`: Role with read-only permission
- `admin_user`: Superuser with full permissions
- `regular_user`: Regular user with limited permissions
- `admin_token` / `user_token`: JWT access tokens
- `admin_headers` / `user_headers`: Authorization headers

### Connector Fixtures
- `sample_connector_definition`: PostgreSQL Destination connector
- `s3_connector_definition`: S3 Destination connector

### Destination Fixtures
- `sample_destination`: Test PostgreSQL destination
- `sample_s3_destination`: Test S3 destination

## API Endpoints Tested

### CRUD Operations (5 endpoints)
- `POST /api/v1/destinations` - Create destination
- `GET /api/v1/destinations` - List destinations (with filters)
- `GET /api/v1/destinations/{id}` - Get destination details
- `PATCH /api/v1/destinations/{id}` - Update destination
- `DELETE /api/v1/destinations/{id}` - Delete destination (soft)

### Connection Testing (2 endpoints)
- `POST /api/v1/destinations/{id}/test-connection` - Test connectivity
- `POST /api/v1/destinations/{id}/validate-write-permissions` - Validate write access

### Write Mode Configuration (2 endpoints)
- `POST /api/v1/destinations/{id}/write-mode` - Configure write mode
- `GET /api/v1/destinations/{id}/write-mode` - Get write mode config

### Schema Mapping (2 endpoints)
- `POST /api/v1/destinations/{id}/schema-mapping` - Configure column mappings
- `GET /api/v1/destinations/{id}/schema-mapping/{table}` - Get mappings

### Batch Settings (2 endpoints)
- `POST /api/v1/destinations/{id}/batch-settings` - Configure batch settings
- `GET /api/v1/destinations/{id}/batch-settings` - Get batch settings

### Statistics (1 endpoint)
- `GET /api/v1/destinations/{id}/stats` - Get usage statistics

**Total: 14 REST API endpoints**

## Expected Test Results

All 37 tests should pass with:
- ✅ Proper authentication and authorization
- ✅ Tenant isolation (users only see their own destinations)
- ✅ Password masking in all responses
- ✅ Proper validation of input data
- ✅ Correct error codes (400, 401, 403, 404)
- ✅ Soft delete behavior
- ✅ JSONB configuration storage
- ✅ Connector validation

## Manual Testing with cURL

### Create Destination
```bash
curl -X POST http://localhost:8000/api/v1/destinations \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "destination_name": "My PostgreSQL Dest",
    "connector_definition_id": "CONNECTOR_UUID",
    "connector_version": "1.0.0",
    "host": "postgres.example.com",
    "port": 5432,
    "database_name": "mydb",
    "schema_name": "public",
    "username": "dbuser",
    "password": "dbpassword",
    "ssl_enabled": true,
    "status": "draft"
  }'
```

### List Destinations
```bash
curl http://localhost:8000/api/v1/destinations?status=active&page=1&page_size=20 \
  -H "Authorization: Bearer YOUR_TOKEN"
```

### Configure Write Mode
```bash
curl -X POST http://localhost:8000/api/v1/destinations/DEST_UUID/write-mode \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "write_mode": "upsert",
    "primary_keys": ["id", "tenant_id"],
    "update_columns": ["name", "updated_at"]
  }'
```

### Configure Schema Mapping
```bash
curl -X POST http://localhost:8000/api/v1/destinations/DEST_UUID/schema-mapping \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "table_name": "users",
    "column_mappings": [
      {"source_column": "user_id", "destination_column": "id"},
      {"source_column": "user_name", "destination_column": "name"}
    ]
  }'
```

## Test Database Requirements

The tests require:
1. **Database**: `fusion_cdc_metadata`
2. **User**: `fusion_user` with password `fusion_password`
3. **Tables**: 42 tables from schema (destinations, connector_definitions, users, roles, permissions, etc.)
4. **Extensions**: `uuid-ossp` for UUID generation

## Troubleshooting

### Database Connection Errors
```bash
# Check PostgreSQL is running
docker ps | grep postgres

# Verify database credentials
docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata -c "SELECT current_database()"

# Check database exists
docker exec fusion-postgres psql -U fusion_user -l | grep fusion
```

### Import Errors
```bash
# Ensure virtual environment is activated
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Test Failures
```bash
# Run tests with full error output
pytest tests/test_destinations/ -vv --tb=long

# Run single failing test
pytest tests/test_destinations/test_api_integration.py::TestName::test_name -vv
```

## Success Criteria

All tests must pass with:
- ✅ 0 failures
- ✅ All 37 tests passing
- ✅ Proper cleanup (no leftover test data)
- ✅ No database connection errors
- ✅ All assertions validating correctly
