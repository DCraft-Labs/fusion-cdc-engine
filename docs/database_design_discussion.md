# Database-Agnostic Metadata Schema Design Discussion

## Context
We need to design a metadata database schema that is:
1. **Database agnostic** - Works on Postgres, MySQL, and potentially others
2. **Production-ready** - Supports all features (multi-tenancy, resource limits, cost tracking)
3. **Industry-proven** - Learn from Airbyte's battle-tested design

## What We Know From Airbyte

### Core Concepts
Airbyte uses these key entities:
- **Actor Definition** - Template for a connector (MySQL source, Postgres destination)
- **Actor** (Source/Destination) - Instance of a connector with specific credentials
- **Connection** - Links source → destination with sync configuration
- **Workspace** - Multi-tenancy isolation (similar to our "tenant")
- **Job** - Execution history (similar to our "connection_runs")
- **State** - Checkpointing (per-stream or global)

### Architecture Insight
- **Config Database** (`airbyte-db`) - Stores configuration and job history
- **Temporal** - Workflow orchestration (similar to our Airflow)
- **Workload API** - Launches discrete pods for connector operations

## Database Compatibility Issues

### Common Pitfalls (Postgres → MySQL Migration)

#### 1. **Data Types**
| Feature | Postgres | MySQL | Solution |
|---------|----------|-------|----------|
| JSON | `JSONB` (binary, indexed) | `JSON` (text-based) | Use `JSON` type, avoid JSONB-specific ops |
| UUID | `UUID` native type | `CHAR(36)` or `BINARY(16)` | Use `CHAR(36)` or `VARCHAR(36)` |
| Boolean | `BOOLEAN` native | `TINYINT(1)` | Use `TINYINT(1)` or `CHAR(1)` |
| Timestamp | `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP` (no timezone) | Store UTC, handle timezone in app |
| Arrays | `TEXT[]`, `INT[]` | No native array type | Use `JSON` or separate table |
| ENUM | Native `ENUM` type | Native `ENUM` (but different syntax) | Use `VARCHAR` with CHECK constraint (if supported) |

#### 2. **Constraints & Indexes**
| Feature | Postgres | MySQL | Solution |
|---------|----------|-------|----------|
| Partial Index | `WHERE status = 'active'` | Not supported | Create full index, filter in app |
| GIN Index | For JSONB | Not available | Use regular index on JSON path |
| Check Constraints | Fully supported | Supported (MySQL 8.0.16+) | Use triggers for older MySQL |
| Deferrable Constraints | Supported | Not supported | Avoid or use triggers |

#### 3. **Advanced Features**
| Feature | Postgres | MySQL | Solution |
|---------|----------|-------|----------|
| Table Partitioning | RANGE, LIST, HASH | RANGE, LIST, HASH (different syntax) | Use ORM abstraction or separate DDLs |
| RETURNING clause | `INSERT ... RETURNING id` | Not supported | Use `LAST_INSERT_ID()` |
| Window Functions | Full support | Partial support (8.0+) | Avoid or use subqueries |
| CTEs (WITH) | Recursive CTEs supported | Recursive CTEs (8.0+) | Avoid recursion or use stored procs |

## Proposed Database-Agnostic Design Principles

### 1. Use Lowest Common Denominator Types
```sql
-- ❌ Postgres-specific
CREATE TABLE tenants (
    tenant_id UUID PRIMARY KEY,
    config JSONB,
    tags TEXT[],
    active BOOLEAN
);

-- ✅ Database-agnostic
CREATE TABLE tenants (
    tenant_id VARCHAR(36) PRIMARY KEY,  -- UUID as string
    config JSON,                        -- Plain JSON (not JSONB)
    tags JSON,                          -- Store array as JSON
    active TINYINT(1)                   -- 0/1 instead of true/false
);
```

### 2. Avoid Database-Specific Features
- **No partial indexes** - Use full indexes
- **No GIN/GiST indexes** - Use B-tree only
- **No arrays** - Use JSON or junction tables
- **No RETURNING** - Query back after INSERT
- **No advisory locks** - Use application-level locking

### 3. Handle Encryption Differently
```sql
-- ❌ Postgres pgcrypto
password_encrypted TEXT -- encrypted with pgp_sym_encrypt()

-- ✅ Database-agnostic
password_encrypted TEXT -- encrypted by application (AES-256-GCM)
-- Encrypt/decrypt in Python using cryptography library
```

### 4. Timestamp Handling
```sql
-- ❌ Postgres-specific
created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()

-- ✅ Database-agnostic (option 1: UTC everywhere)
created_at DATETIME DEFAULT CURRENT_TIMESTAMP  -- Always store UTC
-- Application converts to/from UTC

-- ✅ Database-agnostic (option 2: explicit UTC column)
created_at_utc BIGINT  -- Unix timestamp (milliseconds)
```

### 5. Table Partitioning
```sql
-- ❌ Postgres-specific syntax
CREATE TABLE audit_log (...) PARTITION BY RANGE (created_at);
CREATE TABLE audit_log_2025_11 PARTITION OF audit_log 
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');

-- ✅ Database-agnostic approach
-- Option 1: Don't partition, use indexes and archival strategy
-- Option 2: Use database-specific DDL scripts (pg_schema.sql, mysql_schema.sql)
-- Option 3: Manual table sharding (audit_log_2025_11, audit_log_2025_12)
```

## Airbyte's Approach (From Research)

Based on protocol documentation, Airbyte uses:

1. **Workspace** (multi-tenancy)
   - Similar to our `tenants`
   - Isolation boundary for sources/destinations/connections

2. **Actor** (Source/Destination instance)
   - Has `actor_definition_id` (template/connector type)
   - Has `configuration` (JSON blob with credentials)
   - Has `workspace_id` (tenant isolation)

3. **Connection**
   - Links `source_id` → `destination_id`
   - Has `ConfiguredAirbyteCatalog` (which streams to sync, sync modes)
   - Has `schedule` (cron expression or manual trigger)
   - Has `resource_requirements` (CPU/memory limits!)

4. **State** (checkpointing)
   - Three types: STREAM, GLOBAL, LEGACY
   - STREAM: Per-stream state isolation (best for parallelization)
   - GLOBAL: Shared state across streams (e.g., CDC WAL position)

5. **Job** (execution history)
   - Tracks `job_id`, `connection_id`, `status`, `started_at`, `completed_at`
   - Stores logs, metrics (rows synced, bytes transferred)

## Questions for Design Discussion

### 1. **Primary Database Choice?**
- **Option A**: Design for Postgres first, provide MySQL DDL later
- **Option B**: Design for lowest common denominator (works on both from day 1)
- **Option C**: Use ORM (SQLAlchemy) to abstract differences

**My Recommendation**: Option B for production-readiness

### 2. **UUID vs String Primary Keys?**
```sql
-- Option A: Postgres-native UUID
tenant_id UUID PRIMARY KEY DEFAULT uuid_generate_v4()

-- Option B: String UUID (portable)
tenant_id CHAR(36) PRIMARY KEY
-- Generated by application: str(uuid.uuid4())

-- Option C: Integer + UUID
tenant_id BIGINT AUTO_INCREMENT PRIMARY KEY,
tenant_uuid CHAR(36) UNIQUE NOT NULL
```

**Trade-offs**:
- **UUID**: Distributed generation, no collisions, but 36-char strings vs 16-byte binary
- **Integer**: Smaller, faster joins, but requires DB-level ID generation
- **Hybrid**: Best of both (integer PK for performance, UUID for external APIs)

### 3. **JSON Storage Strategy?**
```sql
-- Option A: Store entire config as JSON
connection_config JSON  -- {"jdbc_url": "...", "username": "...", ...}

-- Option B: Hybrid (common fields + JSON blob)
destination_type VARCHAR(50),  -- postgres_warehouse, bigquery, etc.
host VARCHAR(255),
port INT,
extra_config JSON  -- {"ssl_mode": "require", "connection_timeout": 30}
```

**Trade-offs**:
- **Full JSON**: Flexible, schema evolution easy, but hard to query/index
- **Hybrid**: Common fields indexed, rare fields in JSON

### 4. **Encryption Approach?**
```sql
-- Option A: Database-level (pgcrypto in Postgres, AES_ENCRYPT in MySQL)
password_encrypted BYTEA  -- Different per database

-- Option B: Application-level (Python cryptography library)
password_encrypted TEXT  -- Base64-encoded ciphertext, works everywhere
```

**My Recommendation**: Option B for portability

### 5. **Partitioning Strategy?**
For high-volume tables like `resource_usage`, `audit_log`:

- **Option A**: Native partitioning (database-specific DDL)
- **Option B**: Manual table sharding (`audit_log_2025_11`, `audit_log_2025_12`)
- **Option C**: No partitioning, rely on indexes + archival jobs

### 6. **State Management - Stream vs Global?**
Following Airbyte's approach:

```sql
CREATE TABLE checkpoint_state (
    checkpoint_id VARCHAR(36) PRIMARY KEY,
    source_id VARCHAR(36) NOT NULL,
    connection_id VARCHAR(36),
    
    -- Airbyte's approach: state_type ENUM('STREAM', 'GLOBAL', 'LEGACY')
    state_type VARCHAR(20) NOT NULL,  -- STREAM or GLOBAL
    
    -- For STREAM type: one row per (source, schema, table)
    schema_name VARCHAR(255),
    table_name VARCHAR(255),
    stream_state JSON,  -- Black box per stream
    
    -- For GLOBAL type: shared state across all streams
    global_shared_state JSON,
    global_stream_states JSON,  -- Array of per-stream states
    
    checkpoint_ts DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

**Question**: Do we need GLOBAL state for CDC (shared WAL position across tables)?

## Proposed Unified Schema Approach

### Core Tables (Database-Agnostic)
```sql
-- 1. Multi-tenancy
parents
tenants

-- 2. Connections
sources (with actor_definition_id pattern?)
destinations
connections
streams

-- 3. Execution
connection_runs (jobs)
checkpoint_state

-- 4. Transformations
transform_pipelines
dq_policies
udf_catalog

-- 5. Metadata
schema_change_events

-- 6. Resource Management
resource_usage
tenant_daily_usage

-- 7. Observability
alerts
audit_log
```

### Data Type Mapping
| Logical Type | Postgres | MySQL | Agnostic Solution |
|--------------|----------|-------|-------------------|
| ID (UUID) | `UUID` | `CHAR(36)` | `VARCHAR(36)` or `CHAR(36)` |
| Boolean | `BOOLEAN` | `TINYINT(1)` | `TINYINT(1)` with 0/1 |
| JSON | `JSONB` | `JSON` | `JSON` (plain, not binary) |
| Timestamp | `TIMESTAMPTZ` | `DATETIME` | `DATETIME` (store UTC) |
| Large Text | `TEXT` | `TEXT` | `TEXT` |
| Encrypted | `BYTEA` + pgcrypto | `BLOB` + AES_ENCRYPT | `TEXT` (app-level encryption) |

## Next Steps - What We Should Discuss

1. **Database Choice**
   - Primary: Postgres or MySQL?
   - Secondary: Support both from day 1?

2. **ID Strategy**
   - UUIDs as strings everywhere?
   - Or integer PKs with UUID columns?

3. **JSON Fields**
   - Full JSON configs or hybrid approach?
   - How to handle querying JSON fields?

4. **Encryption**
   - Database-level or application-level?
   - Key management strategy?

5. **Partitioning**
   - Native partitioning or manual sharding?
   - Which tables need partitioning?

6. **State Management**
   - Follow Airbyte's STREAM/GLOBAL pattern?
   - Or simpler per-table state?

7. **Resource Limits**
   - Store as JSON or separate columns?
   - How to enforce limits?

8. **Cost Tracking**
   - Hourly rollup granularity OK?
   - Daily aggregation sufficient for billing?

## My Recommendations (For Discussion)

1. **Design for Postgres first** (superior JSON support, better for CDC use case)
2. **Provide MySQL DDL as secondary** (convert data types, avoid Postgres-specific features)
3. **Use VARCHAR(36) for UUIDs** (portable, works everywhere)
4. **Application-level encryption** (Python cryptography library, not DB-specific)
5. **JSON for flexible configs** (connection config, resource limits, transform specs)
6. **Hybrid columns + JSON** (index common fields like `tenant_id`, `status`, put extras in JSON)
7. **STREAM state by default** (Airbyte's best practice, allows parallelization)
8. **Timestamp as DATETIME (UTC)** (application handles timezone conversion)

What do you think? Should we discuss each of these decisions before writing any DDL?
