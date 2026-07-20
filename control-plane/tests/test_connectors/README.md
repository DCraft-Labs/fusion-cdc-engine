# Connector Definition Tests

Comprehensive integration tests for connector definition management API.

## Test Coverage

### API Endpoints Tested
- ✅ List connectors with filtering and pagination
- ✅ Create connector definition
- ✅ Get connector definition
- ✅ Update connector definition  
- ✅ Delete connector definition
- ✅ List connector versions
- ✅ Create connector version
- ✅ Get connector version
- ✅ Update connector version
- ✅ Get connector capabilities
- ✅ Get connector config schema
- ✅ Get connector usage statistics

### Test Categories
1. **ListConnectors** - Listing with filters, search, pagination
2. **CreateConnector** - Creation, validation, permissions
3. **GetConnector** - Retrieval, not found cases
4. **UpdateConnector** - Updates, permissions
5. **DeleteConnector** - Deletion, in-use checks, permissions
6. **ConnectorVersions** - Version management
7. **ConnectorCapabilities** - Capability queries

## Running Tests

### With PostgreSQL Database
```bash
# Start PostgreSQL
docker-compose up -d postgres

# Run migrations
alembic upgrade head

# Run tests
PYTHONPATH=. pytest tests/test_connectors/ -v
```

### Test Requirements
- PostgreSQL database (UUID support required)
- Authentication system configured
- Test database access

## Test Fixtures

- `sample_connector` - MySQL Source connector
- `sample_destination_connector` - PostgreSQL Destination connector  
- `sample_connector_version` - Version 1.0.0
- `admin_token` - Superadmin access token
- `user_token` - Regular user access token
- `admin_headers` - Admin authorization headers
- `user_headers` - User authorization headers

## Manual Testing

Initialize default connectors:
```bash
python scripts/init_connectors.py
```

Test API endpoints:
```bash
# List connectors
curl http://localhost:8000/api/v1/connector-definitions \\
  -H "Authorization: Bearer <token>"

# Get connector
curl http://localhost:8000/api/v1/connector-definitions/{connector_id} \\
  -H "Authorization: Bearer <token>"

# Create connector (superadmin only)
curl -X POST http://localhost:8000/api/v1/connector-definitions \\
  -H "Authorization: Bearer <admin_token>" \\
  -H "Content-Type: application/json" \\
  -d '{
    "connector_name": "Test Connector",
    "connector_type": "test",
    "category": "source",
    "latest_version": "1.0.0"
  }'
```

## Expected Test Results

When running with PostgreSQL:
```
tests/test_connectors/test_api_integration.py::TestListConnectors::test_list_connectors_success PASSED
tests/test_connectors/test_api_integration.py::TestListConnectors::test_list_connectors_filter_by_category PASSED
tests/test_connectors/test_api_integration.py::TestCreateConnector::test_create_connector_success PASSED
tests/test_connectors/test_api_integration.py::TestGetConnector::test_get_connector_success PASSED
... (35+ tests)

======================== 35 passed in 2.50s ========================
```

## Note on SQLite

SQLite does not support native UUID types. Tests must be run with PostgreSQL for full compatibility with the production schema.
