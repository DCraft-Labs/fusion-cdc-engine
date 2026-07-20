# Fusion CDC Engine - Complete API Specification

## 📋 Overview

This document provides the complete REST API specification for the Fusion CDC Engine Control Plane. All endpoints use the **existing multi-tenancy structure** with foreign keys to `banks`, `sub_tenants`, and `users` tables.

**Base URL:** `http://localhost:8000/api/v1`  
**Authentication:** JWT Bearer Token (Keycloak)  
**Content-Type:** `application/json`

---

## 🔐 Authentication & Multi-Tenancy

### JWT Token Structure
```json
{
  "sub": "user-uuid",
  "email": "user@example.com",
  "realm_roles": ["SUPERADMIN", "ADMIN", "VIEWER"],
  "bank_id": "bank-uuid",
  "sub_tenant_id": "subtenant-uuid"
}
```

### Access Control Rules
- **SUPERADMIN**: Access to all banks and sub-tenants
- **ADMIN**: Access only to their assigned bank_id
- **VIEWER**: Read-only access to their sub_tenant_id

### Tenant Isolation
All endpoints automatically filter by:
- `bank_id` from JWT token
- `sub_tenant_id` from JWT token (except for bank admins)

---

## 📚 API Endpoints Summary

### 1. Connector Definitions (System)
- `GET /connector-definitions` - List available connectors
- `GET /connector-definitions/{connector_id}` - Get connector details
- `GET /connector-definitions/{connector_id}/versions` - List versions

### 2. Sources
- `POST /sources` - Create new source
- `GET /sources` - List sources (tenant-filtered)
- `GET /sources/{source_id}` - Get source details
- `PUT /sources/{source_id}` - Update source
- `DELETE /sources/{source_id}` - Soft delete source
- `POST /sources/{source_id}/test-connection` - Test connection
- `POST /sources/{source_id}/discover-schemas` - Discover tables

### 3. Destinations
- `POST /destinations` - Create new destination
- `GET /destinations` - List destinations (tenant-filtered)
- `GET /destinations/{destination_id}` - Get destination details
- `PUT /destinations/{destination_id}` - Update destination
- `DELETE /destinations/{destination_id}` - Soft delete destination
- `POST /destinations/{destination_id}/test-connection` - Test connection

### 4. Connections
- `POST /connections` - Create new connection
- `GET /connections` - List connections (tenant-filtered)
- `GET /connections/{connection_id}` - Get connection details
- `PUT /connections/{connection_id}` - Update connection
- `DELETE /connections/{connection_id}` - Delete connection
- `POST /connections/{connection_id}/activate` - Activate connection
- `POST /connections/{connection_id}/pause` - Pause connection
- `POST /connections/{connection_id}/trigger-sync` - Manual sync trigger

### 5. Streams
- `GET /connections/{connection_id}/streams` - List streams
- `PUT /connections/{connection_id}/streams/{stream_id}` - Update stream config
- `POST /connections/{connection_id}/streams/{stream_id}/enable` - Enable stream
- `POST /connections/{connection_id}/streams/{stream_id}/disable` - Disable stream

### 6. Transformations
- `POST /transformations` - Create transformation pipeline
- `GET /transformations` - List transformations (tenant-filtered)
- `GET /transformations/{transform_id}` - Get transformation details
- `PUT /transformations/{transform_id}` - Update transformation
- `DELETE /transformations/{transform_id}` - Delete transformation
- `POST /transformations/{transform_id}/validate` - Validate spec

### 7. Data Quality
- `POST /dq-policies` - Create DQ policy
- `GET /dq-policies` - List DQ policies (tenant-filtered)
- `GET /dq-policies/{policy_id}` - Get policy details
- `PUT /dq-policies/{policy_id}` - Update policy
- `DELETE /dq-policies/{policy_id}` - Delete policy
- `GET /dq-policies/{policy_id}/executions` - List executions
- `GET /dq-policies/{policy_id}/violations` - List violations

### 8. Connection Runs (Monitoring)
- `GET /connections/{connection_id}/runs` - List connection runs
- `GET /runs/{run_id}` - Get run details
- `GET /runs/{run_id}/logs` - Get run logs
- `GET /runs/{run_id}/metrics` - Get run metrics

### 9. Monitoring & Metrics
- `GET /monitoring/health` - System health check
- `GET /monitoring/connections/{connection_id}/health` - Connection health
- `GET /monitoring/connections/{connection_id}/lag` - CDC lag metrics
- `GET /monitoring/connections/{connection_id}/throughput` - Event throughput
- `GET /monitoring/resource-usage` - Resource usage (tenant-filtered)

### 10. Schema Evolution
- `GET /connections/{connection_id}/schema-changes` - List schema changes
- `POST /connections/{connection_id}/schema-changes/{change_id}/approve` - Approve change
- `POST /connections/{connection_id}/schema-changes/{change_id}/reject` - Reject change

### 11. UDF Catalog
- `POST /udfs` - Register new UDF
- `GET /udfs` - List UDFs (tenant-filtered)
- `GET /udfs/{udf_id}` - Get UDF details
- `DELETE /udfs/{udf_id}` - Delete UDF

---

## 📖 Detailed API Endpoints

## 1. Connector Definitions API

### GET /connector-definitions
List all available connector definitions (system-wide).

**Authentication:** Required  
**Authorization:** Any authenticated user

**Response:**
```json
{
  "connectors": [
    {
      "connector_id": "mysql-source-v1",
      "connector_name": "MySQL Source",
      "connector_type": "mysql",
      "category": "source",
      "latest_version": "1.2.3",
      "supports_cdc": true,
      "supports_full_refresh": true,
      "supports_incremental": false,
      "documentation_url": "https://docs.dcraftfusion.io/connectors/mysql",
      "icon_url": "https://cdn.dcraftfusion.io/icons/mysql.svg"
    },
    {
      "connector_id": "postgres-source-v1",
      "connector_name": "PostgreSQL Source",
      "connector_type": "postgresql",
      "category": "source",
      "latest_version": "2.0.1",
      "supports_cdc": true,
      "supports_full_refresh": true,
      "supports_incremental": true
    }
  ]
}
```

### GET /connector-definitions/{connector_id}
Get detailed connector configuration.

**Response:**
```json
{
  "connector_id": "mysql-source-v1",
  "connector_name": "MySQL Source",
  "connector_type": "mysql",
  "category": "source",
  "latest_version": "1.2.3",
  "default_config": {
    "port": 3306,
    "ssl_mode": "preferred",
    "server_id": 100
  },
  "required_fields": ["host", "port", "database", "username", "password"],
  "optional_fields": ["ssl_ca_cert", "connection_timeout", "server_id"],
  "default_resource_limits": {
    "cpu_request": "500m",
    "cpu_limit": "2000m",
    "memory_request": "1Gi",
    "memory_limit": "4Gi"
  }
}
```

---

## 2. Sources API

### POST /sources
Create a new source connection.

**Authentication:** Required  
**Authorization:** ADMIN, SUPERADMIN

**Request Body:**
```json
{
  "sub_tenant_id": "subtenant-uuid",
  "bank_id": "bank-uuid",
  "source_name": "Production MySQL DB",
  "connector_definition_id": "mysql-source-v1",
  "connector_version": "1.2.3",
  "host": "mysql.production.com",
  "port": 3306,
  "database_name": "orders_db",
  "username": "cdc_user",
  "password": "secure_password",
  "ssl_enabled": true,
  "ssl_config": {
    "mode": "require",
    "ca_cert": "-----BEGIN CERTIFICATE-----\n..."
  },
  "config": {
    "server_id": 100,
    "gtid_enabled": true
  }
}
```

**Response (201 Created):**
```json
{
  "source_id": "src-uuid",
  "sub_tenant_id": "subtenant-uuid",
  "bank_id": "bank-uuid",
  "source_name": "Production MySQL DB",
  "connector_definition_id": "mysql-source-v1",
  "connector_version": "1.2.3",
  "host": "mysql.production.com",
  "port": 3306,
  "database_name": "orders_db",
  "username": "cdc_user",
  "password_encrypted": "[ENCRYPTED]",
  "ssl_enabled": true,
  "status": "draft",
  "connection_test_status": null,
  "created_at": "2025-12-08T10:00:00Z",
  "created_by": "user-uuid"
}
```

**Validation Rules:**
- `sub_tenant_id` must exist in `sub_tenants` table
- `bank_id` must match JWT token (except SUPERADMIN)
- `sub_tenant_id` must belong to `bank_id`
- `source_name` must be unique within sub_tenant
- Password is automatically encrypted (AES-256-GCM)

### GET /sources
List all sources for the authenticated user's tenant.

**Query Parameters:**
- `status` (optional): Filter by status (draft, testing, active, paused, error)
- `connector_type` (optional): Filter by connector type (mysql, postgresql, mongodb)
- `page` (optional, default=1): Page number
- `page_size` (optional, default=20): Items per page

**Response:**
```json
{
  "sources": [
    {
      "source_id": "src-uuid",
      "source_name": "Production MySQL DB",
      "connector_type": "mysql",
      "host": "mysql.production.com",
      "database_name": "orders_db",
      "status": "active",
      "connection_test_status": "success",
      "last_discovery_at": "2025-12-08T09:00:00Z",
      "created_at": "2025-12-08T08:00:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_items": 5,
    "total_pages": 1
  }
}
```

**Tenant Filtering:**
- Automatically filters by `bank_id` and `sub_tenant_id` from JWT
- SUPERADMIN can query across all banks with `?bank_id=bank-uuid` parameter

### POST /sources/{source_id}/test-connection
Test source database connectivity.

**Response:**
```json
{
  "status": "success",
  "message": "Connection successful",
  "connection_test_at": "2025-12-08T10:30:00Z",
  "details": {
    "server_version": "8.0.35",
    "binlog_enabled": true,
    "binlog_format": "ROW",
    "gtid_mode": "ON"
  }
}
```

**Error Response (400 Bad Request):**
```json
{
  "status": "failed",
  "message": "Connection failed",
  "error": "Access denied for user 'cdc_user'@'%' (using password: YES)",
  "connection_test_at": "2025-12-08T10:30:00Z"
}
```

### POST /sources/{source_id}/discover-schemas
Discover available schemas and tables.

**Response:**
```json
{
  "discovery_cache": {
    "schemas": [
      {
        "schema_name": "orders_db",
        "tables": [
          {
            "table_name": "orders",
            "row_count_estimate": 1500000,
            "columns": [
              {
                "column_name": "order_id",
                "data_type": "INT",
                "is_primary_key": true,
                "is_nullable": false
              },
              {
                "column_name": "customer_id",
                "data_type": "INT",
                "is_nullable": false
              },
              {
                "column_name": "order_date",
                "data_type": "TIMESTAMP",
                "is_nullable": false
              },
              {
                "column_name": "metadata_json",
                "data_type": "JSON",
                "is_nullable": true,
                "json_detected": true
              }
            ]
          },
          {
            "table_name": "order_items",
            "row_count_estimate": 5000000,
            "columns": [...]
          }
        ]
      }
    ]
  },
  "last_discovery_at": "2025-12-08T11:00:00Z"
}
```

---

## 3. Destinations API

### POST /destinations
Create a new destination.

**Request Body (PostgreSQL Warehouse):**
```json
{
  "sub_tenant_id": "subtenant-uuid",
  "bank_id": "bank-uuid",
  "destination_name": "Analytics Warehouse",
  "connector_definition_id": "postgres-destination-v1",
  "connector_version": "2.0.0",
  "destination_type": "postgres_warehouse",
  "host": "warehouse.analytics.com",
  "port": 5432,
  "database_name": "analytics",
  "schema_name": "cdc_orders",
  "username": "warehouse_writer",
  "password": "secure_password",
  "ssl_enabled": true,
  "config": {
    "pool_size": 10,
    "statement_timeout": 30000
  }
}
```

**Request Body (Iceberg on S3):**
```json
{
  "sub_tenant_id": "subtenant-uuid",
  "bank_id": "bank-uuid",
  "destination_name": "Iceberg Datalake",
  "connector_definition_id": "iceberg-destination-v1",
  "connector_version": "1.0.0",
  "destination_type": "iceberg_s3",
  "config": {
    "catalog_type": "glue",
    "warehouse_path": "s3://my-bucket/warehouse/",
    "database_name": "cdc_analytics",
    "aws_region": "us-east-1",
    "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
    "aws_secret_access_key": "[ENCRYPTED]"
  }
}
```

**Response (201 Created):**
```json
{
  "destination_id": "dest-uuid",
  "destination_name": "Analytics Warehouse",
  "destination_type": "postgres_warehouse",
  "status": "draft",
  "created_at": "2025-12-08T12:00:00Z"
}
```

---

## 4. Connections API

### POST /connections
Create a new connection between source and destination.

**Request Body:**
```json
{
  "sub_tenant_id": "subtenant-uuid",
  "bank_id": "bank-uuid",
  "connection_name": "Orders CDC Pipeline",
  "source_id": "src-uuid",
  "destination_id": "dest-uuid",
  "sync_mode": "CDC_INCREMENTAL_DEDUPED_HISTORY",
  "sync_type": "REALTIME",
  "schedule": null,
  "schema_evolution_policy": "AUTO_APPLY",
  "transform_pipeline_id": null,
  "dq_policy_id": null,
  "streams": [
    {
      "schema_name": "orders_db",
      "table_name": "orders",
      "enabled": true,
      "sync_mode_override": null,
      "primary_keys": ["order_id"],
      "cursor_field": "updated_at",
      "json_columns": ["metadata_json"],
      "json_flatten_config": {
        "metadata_json": {
          "mode": "inline",
          "json_schema": {
            "payment_method": "string",
            "shipping_address": "string",
            "discount_code": "string"
          },
          "output_columns": {
            "payment_method": "payment_method",
            "shipping_address": "shipping_address",
            "discount_code": "discount_code"
          },
          "keep_original": true
        }
      }
    },
    {
      "schema_name": "orders_db",
      "table_name": "order_items",
      "enabled": true,
      "primary_keys": ["item_id"]
    }
  ]
}
```

**Response (201 Created):**
```json
{
  "connection_id": "conn-uuid",
  "connection_name": "Orders CDC Pipeline",
  "source_id": "src-uuid",
  "destination_id": "dest-uuid",
  "sync_mode": "CDC_INCREMENTAL_DEDUPED_HISTORY",
  "sync_type": "REALTIME",
  "status": "inactive",
  "created_at": "2025-12-08T13:00:00Z",
  "streams": [
    {
      "stream_id": "stream-uuid-1",
      "table_name": "orders",
      "enabled": true,
      "status": "pending_initial_load"
    },
    {
      "stream_id": "stream-uuid-2",
      "table_name": "order_items",
      "enabled": true,
      "status": "pending_initial_load"
    }
  ]
}
```

**Sync Modes:**
- `FULL_REFRESH_OVERWRITE`: Truncate and reload
- `FULL_REFRESH_APPEND`: Append all rows
- `INCREMENTAL_APPEND`: New rows only (cursor-based)
- `INCREMENTAL_APPEND_DEDUPED`: Incremental with upsert
- `CDC_INCREMENTAL_DEDUPED_HISTORY`: SCD Type 2 with history

**Sync Types:**
- `REALTIME`: CDC worker continuously streams changes
- `SCHEDULED`: Airflow DAG runs on schedule (cron)

### POST /connections/{connection_id}/activate
Activate a connection to start CDC streaming.

**Response:**
```json
{
  "connection_id": "conn-uuid",
  "status": "activating",
  "message": "Connection activation in progress",
  "steps": [
    {
      "step": "initial_snapshot",
      "status": "queued",
      "description": "Full snapshot of source tables"
    },
    {
      "step": "worker_assignment",
      "status": "pending",
      "description": "Assign CDC worker to DB host"
    },
    {
      "step": "cdc_streaming",
      "status": "pending",
      "description": "Start real-time CDC capture"
    }
  ]
}
```

**Activation Flow:**
1. Trigger initial full snapshot (Airflow DAG)
2. Assign CDC worker to source DB host
3. Worker starts reading binlog/WAL
4. Events flow to Redis → Spark → Destination
5. Connection status changes to `active`

---

## 5. Transformations API

### POST /transformations
Create a transformation pipeline.

**Request Body:**
```json
{
  "sub_tenant_id": "subtenant-uuid",
  "bank_id": "bank-uuid",
  "pipeline_name": "Orders Enrichment",
  "description": "Mask PII, flatten JSON, compute risk scores",
  "transforms": [
    {
      "id": "cast_created_at",
      "type": "cast",
      "column": "created_at",
      "to_type": "timestamp",
      "output_column": "created_at"
    },
    {
      "id": "mask_card_number",
      "type": "mask",
      "column": "card_number",
      "strategy": "last4",
      "output_column": "card_number"
    },
    {
      "id": "flatten_metadata",
      "type": "json_flatten_inline",
      "column": "metadata_json",
      "json_schema": {
        "payment_method": "string",
        "shipping_address": "string"
      },
      "output_columns": {
        "payment_method": "payment_method",
        "shipping_address": "shipping_address"
      },
      "keep_original": false
    },
    {
      "id": "compute_risk",
      "type": "udf",
      "function": "compute_risk_score",
      "args": ["amount", "country", "device_id"],
      "output_column": "risk_score"
    }
  ]
}
```

**Response (201 Created):**
```json
{
  "transform_pipeline_id": "transform-uuid",
  "pipeline_name": "Orders Enrichment",
  "version": 1,
  "status": "active",
  "created_at": "2025-12-08T14:00:00Z"
}
```

---

## 6. Data Quality API

### POST /dq-policies
Create a data quality policy.

**Request Body:**
```json
{
  "sub_tenant_id": "subtenant-uuid",
  "bank_id": "bank-uuid",
  "policy_name": "Orders Table Quality",
  "table_name": "orders",
  "rules": [
    {
      "rule_id": "row_count_check",
      "rule_type": "row_count",
      "operator": ">=",
      "threshold": 1000,
      "severity": "warning",
      "description": "Minimum daily order count"
    },
    {
      "rule_id": "null_check_customer_id",
      "rule_type": "null_ratio",
      "column": "customer_id",
      "operator": "<=",
      "threshold": 0.01,
      "severity": "critical",
      "description": "customer_id should not be null"
    },
    {
      "rule_id": "freshness_check",
      "rule_type": "freshness",
      "column": "created_at",
      "threshold_minutes": 60,
      "severity": "warning",
      "description": "Data should be less than 1 hour old"
    },
    {
      "rule_id": "custom_sql_check",
      "rule_type": "custom_sql",
      "sql": "SELECT COUNT(*) FROM orders WHERE amount < 0",
      "operator": "=",
      "threshold": 0,
      "severity": "critical",
      "description": "No negative amounts allowed"
    }
  ],
  "execution_frequency": "hourly"
}
```

**Response (201 Created):**
```json
{
  "policy_id": "policy-uuid",
  "policy_name": "Orders Table Quality",
  "rules_count": 4,
  "status": "active",
  "created_at": "2025-12-08T15:00:00Z"
}
```

---

## 7. Monitoring API

### GET /monitoring/connections/{connection_id}/lag
Get CDC lag metrics.

**Response:**
```json
{
  "connection_id": "conn-uuid",
  "streams": [
    {
      "stream_id": "stream-uuid",
      "table_name": "orders",
      "lag_seconds": 2.5,
      "lag_events": 125,
      "current_lsn": "16/CAFE1234",
      "latest_source_lsn": "16/CAFE1300",
      "events_per_second": 50.2,
      "last_event_at": "2025-12-08T16:00:00Z"
    }
  ],
  "overall_lag_seconds": 2.5,
  "overall_events_per_second": 50.2
}
```

### GET /monitoring/resource-usage
Get resource usage for tenant.

**Response:**
```json
{
  "sub_tenant_id": "subtenant-uuid",
  "period": "today",
  "cpu_hours": 12.5,
  "memory_gb_hours": 48.0,
  "events_processed": 1500000,
  "storage_gb": 150.5,
  "estimated_cost_usd": 45.30,
  "quota_status": {
    "cpu_used_percent": 62.5,
    "memory_used_percent": 60.0,
    "events_used_percent": 75.0,
    "within_limits": true
  }
}
```

---

## 8. Schema Evolution API

### GET /connections/{connection_id}/schema-changes
List detected schema changes.

**Response:**
```json
{
  "schema_changes": [
    {
      "change_id": "change-uuid",
      "stream_id": "stream-uuid",
      "table_name": "orders",
      "change_type": "ADD_COLUMN",
      "change_details": {
        "column_name": "delivery_date",
        "data_type": "DATE",
        "is_nullable": true,
        "position": 15
      },
      "detected_at": "2025-12-08T17:00:00Z",
      "status": "pending_approval",
      "auto_apply_eligible": true
    },
    {
      "change_id": "change-uuid-2",
      "stream_id": "stream-uuid",
      "table_name": "orders",
      "change_type": "DROP_COLUMN",
      "change_details": {
        "column_name": "legacy_field"
      },
      "detected_at": "2025-12-08T17:05:00Z",
      "status": "pending_approval",
      "auto_apply_eligible": false
    }
  ]
}
```

### POST /connections/{connection_id}/schema-changes/{change_id}/approve
Approve a schema change.

**Response:**
```json
{
  "change_id": "change-uuid",
  "status": "approved",
  "applied_at": "2025-12-08T17:10:00Z",
  "message": "Schema change applied successfully"
}
```

---

## 9. UDF Catalog API

### POST /udfs
Register a new user-defined function.

**Request Body:**
```json
{
  "sub_tenant_id": "subtenant-uuid",
  "bank_id": "bank-uuid",
  "udf_name": "compute_risk_score",
  "description": "Calculate fraud risk score based on transaction attributes",
  "language": "python",
  "code": "def compute_risk_score(amount, country, device_id):\n    risk = 0.0\n    if amount > 10000:\n        risk += 0.3\n    if country not in ['US', 'CA', 'UK']:\n        risk += 0.2\n    return risk",
  "input_parameters": [
    {
      "name": "amount",
      "type": "double"
    },
    {
      "name": "country",
      "type": "string"
    },
    {
      "name": "device_id",
      "type": "string"
    }
  ],
  "return_type": "double"
}
```

**Response (201 Created):**
```json
{
  "udf_id": "udf-uuid",
  "udf_name": "compute_risk_score",
  "version": 1,
  "status": "active",
  "created_at": "2025-12-08T18:00:00Z"
}
```

---

## 🔒 Error Responses

### Standard Error Format
```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "Validation failed",
    "details": {
      "field": "source_name",
      "reason": "Source name already exists for this tenant"
    }
  }
}
```

### Error Codes
- `AUTHENTICATION_FAILED` (401): Invalid or missing JWT token
- `AUTHORIZATION_FAILED` (403): Insufficient permissions
- `RESOURCE_NOT_FOUND` (404): Resource does not exist
- `INVALID_REQUEST` (400): Validation error
- `TENANT_ISOLATION_VIOLATION` (403): Attempting to access another tenant's resource
- `RESOURCE_CONFLICT` (409): Duplicate resource
- `INTERNAL_SERVER_ERROR` (500): Unexpected error

---

## 📊 Integration with Existing Multi-Tenancy

### Foreign Key Relationships
All CDC Engine tables maintain referential integrity:

```sql
-- sources table
ALTER TABLE sources 
  ADD CONSTRAINT fk_sources_sub_tenant 
  FOREIGN KEY (sub_tenant_id) REFERENCES sub_tenants(id);

ALTER TABLE sources 
  ADD CONSTRAINT fk_sources_bank 
  FOREIGN KEY (bank_id) REFERENCES banks(id);

ALTER TABLE sources 
  ADD CONSTRAINT fk_sources_created_by 
  FOREIGN KEY (created_by) REFERENCES users(id);
```

### Tenant Isolation Enforcement
Middleware automatically filters all queries:

```python
# Automatic tenant filtering in FastAPI
@app.get("/api/v1/sources")
async def list_sources(current_user: User = Depends(get_current_user)):
    query = db.query(Source)
    
    # Filter by bank
    if current_user.role != "SUPERADMIN":
        query = query.filter(Source.bank_id == current_user.bank_id)
    
    # Filter by sub-tenant (for non-admins)
    if current_user.role == "VIEWER":
        query = query.filter(Source.sub_tenant_id == current_user.sub_tenant_id)
    
    return query.all()
```

---

## 🚀 Next Steps

1. **Implement FastAPI skeleton** with all endpoint stubs
2. **Create SQLAlchemy models** matching the 42-table schema
3. **Implement authentication middleware** with JWT validation
4. **Build CRUD operations** for each endpoint
5. **Add tenant isolation** middleware and validators
6. **Generate OpenAPI documentation** (automatic with FastAPI)
7. **Create Postman collection** for API testing

---

**Status:** Ready for implementation  
**Priority:** Phase 1 (Weeks 1-4)  
**Dependencies:** Database schema already exists
