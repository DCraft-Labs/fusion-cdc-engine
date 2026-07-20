# Database Setup Guide

Complete guide for setting up PostgreSQL or MySQL databases for the Fusion CDC Engine.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [PostgreSQL Setup](#postgresql-setup)
- [MySQL Setup](#mysql-setup)
- [Environment Variables](#environment-variables)
- [Seed Data](#seed-data)
- [Migrations](#migrations)
- [Verification](#verification)
- [Troubleshooting](#troubleshooting)
- [Performance Tuning](#performance-tuning)
- [Backup & Recovery](#backup--recovery)

---

## Overview

The Fusion CDC Engine supports **both PostgreSQL and MySQL** as metadata databases:

- **PostgreSQL** (Recommended): Optimized with UUID types, JSONB, native booleans, and table partitioning
- **MySQL**: Compatible alternative using CHAR(36) for UUIDs, JSON type, and TINYINT(1) for booleans

**Schema Files:**
- `schema_postgres.sql` - PostgreSQL DDL (1,853 lines, 42 tables)
- `schema_mysql.sql` - MySQL DDL (1,692 lines, 42 tables)
- `postgres_jsonb_gin_indexes.sql` - Optional JSONB performance indexes (PostgreSQL only)
- `seed_data.sql` - Sample connector definitions and system config

**Integration with Main App:**
- Foreign keys to: `banks.id`, `sub_tenants.id`, `users.id`
- **NO** separate `parents` or `tenants` tables (uses existing infrastructure)

---

## Prerequisites

### For PostgreSQL

**PostgreSQL 14+** (tested with 14, 15, 16)

```bash
# macOS (Homebrew)
brew install postgresql@16
brew services start postgresql@16

# Ubuntu/Debian
sudo apt update
sudo apt install postgresql-16 postgresql-contrib-16
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Docker
docker run --name fusion-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  -d postgres:16

# Verify installation
psql --version  # Should show 16.x
```

### For MySQL

**MySQL 8.0+** (requires JSON support)

```bash
# macOS (Homebrew)
brew install mysql@8.0
brew services start mysql@8.0

# Ubuntu/Debian
sudo apt update
sudo apt install mysql-server-8.0
sudo systemctl start mysql
sudo systemctl enable mysql

# Docker
docker run --name fusion-mysql \
  -e MYSQL_ROOT_PASSWORD=mysql \
  -p 3306:3306 \
  -d mysql:8.0

# Verify installation
mysql --version  # Should show 8.0.x or higher
```

---

## PostgreSQL Setup

### Step 1: Create Database and User

```bash
# Connect as superuser
psql postgres

# Or for Docker:
# docker exec -it fusion-postgres psql -U postgres
```

```sql
-- Create database
CREATE DATABASE fusion_cdc_metadata;

-- Create user with strong password
CREATE USER fusion_user WITH ENCRYPTED PASSWORD 'your_secure_password_here';

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE fusion_cdc_metadata TO fusion_user;

-- Connect to the new database
\c fusion_cdc_metadata

-- Grant schema privileges
GRANT ALL ON SCHEMA public TO fusion_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO fusion_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO fusion_user;

-- Exit
\q
```

### Step 2: Apply Schema

```bash
# Navigate to schemas directory
cd /Users/rishikeshsrinivas/Workspace/fusion-cdc-engine/schemas

# Apply main schema (creates all 42 tables)
psql -U fusion_user -d fusion_cdc_metadata -f schema_postgres.sql

# Verify tables created
psql -U fusion_user -d fusion_cdc_metadata -c "\dt"
```

### Step 3: Apply JSONB GIN Indexes (Optional)

Only apply if you have heavy JSONB querying requirements:

```bash
psql -U fusion_user -d fusion_cdc_metadata -f postgres_jsonb_gin_indexes.sql
```

### Step 4: Load Seed Data

```bash
psql -U fusion_user -d fusion_cdc_metadata -f seed_data.sql
```

### Step 5: Verify Installation

```bash
psql -U fusion_user -d fusion_cdc_metadata
```

```sql
-- Check table count (should be 42)
SELECT COUNT(*) FROM information_schema.tables 
WHERE table_schema = 'public' AND table_type = 'BASE TABLE';

-- Check connector definitions (should show 11 connectors)
SELECT connector_name, connector_type, category, latest_version 
FROM connector_definitions
ORDER BY category, connector_name;

-- Check system config
SELECT config_key, config_value, description 
FROM system_config
ORDER BY config_key;

-- Check feature flags
SELECT flag_name, is_enabled, description 
FROM feature_flags;

-- Exit
\q
```

---

## MySQL Setup

### Step 1: Create Database and User

```bash
# Connect as root
mysql -u root -p

# Or for Docker:
# docker exec -it fusion-mysql mysql -u root -p
```

```sql
-- Create database with UTF-8 support
CREATE DATABASE fusion_cdc_metadata 
  CHARACTER SET utf8mb4 
  COLLATE utf8mb4_unicode_ci;

-- Create user with strong password
CREATE USER 'fusion_user'@'localhost' 
  IDENTIFIED BY 'your_secure_password_here';

-- For Docker or remote access:
CREATE USER 'fusion_user'@'%' 
  IDENTIFIED BY 'your_secure_password_here';

-- Grant privileges
GRANT ALL PRIVILEGES ON fusion_cdc_metadata.* 
  TO 'fusion_user'@'localhost';

-- For Docker or remote:
GRANT ALL PRIVILEGES ON fusion_cdc_metadata.* 
  TO 'fusion_user'@'%';

FLUSH PRIVILEGES;

-- Exit
EXIT;
```

### Step 2: Apply Schema

```bash
# Navigate to schemas directory
cd /Users/rishikeshsrinivas/Workspace/fusion-cdc-engine/schemas

# Apply main schema (creates all 42 tables)
mysql -u fusion_user -p fusion_cdc_metadata < schema_mysql.sql

# Verify tables created
mysql -u fusion_user -p fusion_cdc_metadata -e "SHOW TABLES;"
```

### Step 3: Load Seed Data

**Note:** The seed_data.sql file uses PostgreSQL UUID functions. For MySQL, you need to modify UUID generation:

```bash
# Create MySQL-compatible version
sed 's/uuid_generate_v4()/UUID()/g' seed_data.sql > seed_data_mysql.sql

# Manual edit for static UUIDs - replace the INSERT statements' UUID values with UUID() calls
# Or run the modified file:
mysql -u fusion_user -p fusion_cdc_metadata < seed_data_mysql.sql
```

### Step 4: Verify Installation

```bash
mysql -u fusion_user -p fusion_cdc_metadata
```

```sql
-- Check table count (should be 42)
SELECT COUNT(*) FROM information_schema.tables 
WHERE table_schema = 'fusion_cdc_metadata' 
  AND table_type = 'BASE TABLE';

-- Check connector definitions (should show 11 connectors)
SELECT connector_name, connector_type, category, latest_version 
FROM connector_definitions
ORDER BY category, connector_name;

-- Check system config
SELECT config_key, config_value, description 
FROM system_config
ORDER BY config_key;

-- Check feature flags
SELECT flag_name, is_enabled, description 
FROM feature_flags;

-- Exit
EXIT;
```

---

## Environment Variables

Configure your application with these environment variables:

### Database Connection

```bash
# PostgreSQL Configuration
export DB_TYPE=postgresql
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=fusion_cdc_metadata
export DB_USER=fusion_user
export DB_PASSWORD=your_secure_password_here
export DB_SSL_MODE=prefer  # Options: disable, allow, prefer, require, verify-ca, verify-full
export DB_POOL_SIZE=20
export DB_MAX_OVERFLOW=10

# MySQL Configuration (alternative)
# export DB_TYPE=mysql
# export DB_HOST=localhost
# export DB_PORT=3306
# export DB_NAME=fusion_cdc_metadata
# export DB_USER=fusion_user
# export DB_PASSWORD=your_secure_password_here
# export DB_SSL_MODE=PREFERRED  # Options: DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY
# export DB_POOL_SIZE=20
# export DB_MAX_OVERFLOW=10
```

### Retention Periods (Configured in system_config table)

```bash
# Data Quality violations retention (default: 90 days)
export DQ_VIOLATION_RETENTION_DAYS=90

# Dead Letter Queue events retention (default: 7 days)
export EVENT_DLQ_RETENTION_DAYS=7

# Spark UI availability after job completion (default: 48 hours)
export SPARK_UI_RETENTION_HOURS=48
```

### Health Monitoring

```bash
# Health check frequency (default: 60 seconds)
export HEALTH_CHECK_INTERVAL_SECONDS=60

# Worker heartbeat timeout (default: 30 seconds)
export WORKER_HEARTBEAT_TIMEOUT_SECONDS=30
```

### Resource Limits

```bash
# Maximum concurrent connections per tenant
export MAX_CONCURRENT_CONNECTIONS_PER_TENANT=50

# Default parallel stream sync count
export DEFAULT_SYNC_PARALLELISM=4
```

### External Services

```bash
# Redis (for event streaming and tracking)
export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_PASSWORD=
export REDIS_DB=0
export REDIS_STREAM_RETENTION_HOURS=24

# Kafka (optional - for event streaming)
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_SECURITY_PROTOCOL=PLAINTEXT

# Amazon S3 (for Spark logs and large payloads)
export S3_BUCKET_LOGS=fusion-cdc-logs
export S3_REGION=us-east-1
export AWS_ACCESS_KEY_ID=your_aws_access_key
export AWS_SECRET_ACCESS_KEY=your_aws_secret_key

# Prometheus (for metrics)
export PROMETHEUS_PUSHGATEWAY_URL=http://localhost:9091
```

### Main Fusion Application Integration

```bash
# Main application database (for foreign key relationships)
export MAIN_APP_DB_HOST=localhost
export MAIN_APP_DB_PORT=5432
export MAIN_APP_DB_NAME=fusion_main
export MAIN_APP_DB_USER=fusion_main_user
export MAIN_APP_DB_PASSWORD=main_app_password

# Keycloak Authentication (managed by main app)
export KEYCLOAK_URL=http://localhost:8080
export KEYCLOAK_REALM=fusion
export KEYCLOAK_CLIENT_ID=fusion-cdc-engine
export KEYCLOAK_CLIENT_SECRET=your_keycloak_secret
```

---

## Seed Data

### What's Included

The `seed_data.sql` file includes:

**11 Connector Definitions:**
- **Sources (5):** MySQL, PostgreSQL, MongoDB, Oracle, SQL Server
- **Destinations (6):** Snowflake, BigQuery, Databricks, PostgreSQL Warehouse, Amazon S3, Redshift

**3 Connector Versions:**
- MySQL connector version history (8.0.35, 8.0.33, 8.0.30)

**8 System Configuration Entries:**
- DQ violation retention, DLQ retention, Spark UI retention
- Health check intervals, worker heartbeat timeout
- Connection limits, sync parallelism
- Kafka, Redis, S3, Prometheus settings

**4 Feature Flags:**
- JSON auto-flatten, schema auto-apply, Spark autoscaling, DQ auto-remediation

### Required Main App Data

Before using the CDC Engine, ensure these tables exist in the **main Fusion application database**:

```sql
-- banks table (must exist in main app)
CREATE TABLE banks (
    id UUID PRIMARY KEY,
    bank_name VARCHAR(255) NOT NULL,
    status VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE,
    -- ... other columns
);

-- sub_tenants table (must exist in main app)
CREATE TABLE sub_tenants (
    id UUID PRIMARY KEY,
    bank_id UUID NOT NULL REFERENCES banks(id),
    sub_tenant_name VARCHAR(255) NOT NULL,
    database_name VARCHAR(255) NOT NULL,
    database_host VARCHAR(500) NOT NULL,
    database_port INTEGER NOT NULL,
    status VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE,
    -- ... other columns
);

-- users table (must exist in main app)
CREATE TABLE users (
    id UUID PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    username VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE,
    -- ... other columns
);
```

### Creating Test Data (Optional)

```sql
-- Insert test bank (in main app database)
INSERT INTO banks (id, bank_name, status, created_at) VALUES 
    ('550e8400-e29b-41d4-a716-446655440000', 'Test Bank', 'active', NOW());

-- Insert test sub-tenant (in main app database)
INSERT INTO sub_tenants (
    id, bank_id, sub_tenant_name, 
    database_name, database_host, database_port, 
    status, created_at
) VALUES (
    '660e8400-e29b-41d4-a716-446655440001', 
    '550e8400-e29b-41d4-a716-446655440000',
    'Test Tenant', 
    'tenant_db', 'localhost', 5432,
    'active', NOW()
);

-- Insert test user (in main app database)
INSERT INTO users (id, email, username, created_at) VALUES 
    ('770e8400-e29b-41d4-a716-446655440002', 
     'admin@testbank.com', 'admin', NOW());
```

---

## Migrations

### PostgreSQL Migrations (Using Alembic)

#### Setup Alembic

```bash
# Install Alembic
pip install alembic psycopg2-binary

# Initialize Alembic in your project
cd /Users/rishikeshsrinivas/Workspace/fusion-cdc-engine
alembic init migrations

# Edit alembic.ini
# Set database URL:
# sqlalchemy.url = postgresql://fusion_user:password@localhost:5432/fusion_cdc_metadata
```

#### Create Initial Migration

```bash
# Generate initial migration from current schema
alembic revision --autogenerate -m "Initial schema with 42 tables"

# Review the generated migration file in migrations/versions/

# Apply migrations
alembic upgrade head

# Check current version
alembic current

# View migration history
alembic history
```

#### Future Schema Changes

```bash
# After modifying your models, generate migration
alembic revision --autogenerate -m "Add new column to connections table"

# Apply migration
alembic upgrade head

# Rollback one version
alembic downgrade -1

# Rollback to specific version
alembic downgrade <revision_id>
```

### MySQL Migrations (Using Flyway)

#### Setup Flyway

```bash
# Download Flyway
# Visit: https://flywaydb.org/download/

# Or install via Homebrew (macOS)
brew install flyway

# Or use Docker
docker pull flyway/flyway
```

#### Configure Flyway

Create `flyway.conf`:

```properties
flyway.url=jdbc:mysql://localhost:3306/fusion_cdc_metadata
flyway.user=fusion_user
flyway.password=your_secure_password_here
flyway.locations=filesystem:./migrations/mysql
flyway.baselineOnMigrate=true
flyway.validateOnMigrate=true
```

#### Create Migration Directory

```bash
cd /Users/rishikeshsrinivas/Workspace/fusion-cdc-engine
mkdir -p migrations/mysql

# Copy initial schema as first migration
cp schemas/schema_mysql.sql migrations/mysql/V1__initial_schema.sql
```

#### Run Migrations

```bash
# Baseline existing database (if already has schema)
flyway baseline

# Run migrations
flyway migrate

# Check migration status
flyway info

# Validate applied migrations
flyway validate
```

#### Future Schema Changes

```bash
# Create new migration file with version number
# Format: V{version}__{description}.sql
# Example: V2__add_connection_tags_column.sql

cat > migrations/mysql/V2__add_connection_tags_column.sql << 'EOF'
ALTER TABLE connections 
ADD COLUMN tags JSON DEFAULT NULL COMMENT 'User-defined tags for connections';

CREATE INDEX idx_connections_tags ON connections ((CAST(tags AS CHAR(255) ARRAY)));
EOF

# Apply new migration
flyway migrate

# Check status
flyway info
```

---

## Verification

### Check Table Count

**PostgreSQL:**
```sql
SELECT COUNT(*) AS table_count
FROM information_schema.tables 
WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
-- Expected: 42 (or 45 if audit_log partitions counted separately)
```

**MySQL:**
```sql
SELECT COUNT(*) AS table_count
FROM information_schema.tables 
WHERE table_schema = 'fusion_cdc_metadata' AND table_type = 'BASE TABLE';
-- Expected: 42 (or 45 if audit_log partitions counted separately)
```

### Verify All 42 Tables Exist

**PostgreSQL:**
```sql
SELECT tablename 
FROM pg_tables 
WHERE schemaname = 'public'
ORDER BY tablename;
```

**MySQL:**
```sql
SHOW TABLES;
```

**Expected tables:**
1. alerts
2. audit_log (+ partitions)
3. cdc_lag_metrics
4. cdc_position_history
5. checkpoint_state
6. connection_alert_webhooks
7. connection_health_checks
8. connection_runs
9. connections
10. connector_definitions
11. connector_versions
12. destinations
13. dq_policies
14. dq_rule_results
15. dq_violation_samples
16. dq_violations
17. event_dead_letter_queue
18. event_dlq_retry_history
19. feature_flags
20. json_flatten_rules
21. json_schema_cache
22. json_schema_evolution
23. maintenance_windows
24. redis_stream_tracking
25. resource_quota_violations
26. resource_usage
27. schema_change_events
28. sources
29. spark_applications
30. spark_executor_history
31. spark_executors
32. spark_job_queue
33. streams
34. sync_mode_config
35. system_config
36. tenant_daily_usage
37. transform_pipelines
38. transformation_dependencies
39. transformation_logs
40. udf_catalog
41. udf_execution_stats
42. worker_heartbeats

### Check Foreign Key Constraints

**PostgreSQL:**
```sql
SELECT
    tc.constraint_name,
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
ORDER BY tc.table_name, tc.constraint_name;
```

**MySQL:**
```sql
SELECT 
    CONSTRAINT_NAME,
    TABLE_NAME,
    COLUMN_NAME,
    REFERENCED_TABLE_NAME,
    REFERENCED_COLUMN_NAME
FROM information_schema.KEY_COLUMN_USAGE
WHERE CONSTRAINT_SCHEMA = 'fusion_cdc_metadata'
  AND REFERENCED_TABLE_NAME IS NOT NULL
ORDER BY TABLE_NAME, CONSTRAINT_NAME;
```

### Test Application Connection

**Python Example (PostgreSQL):**
```python
import psycopg2
from psycopg2.extras import RealDictCursor

# Connect
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="fusion_cdc_metadata",
    user="fusion_user",
    password="your_secure_password_here"
)

cursor = conn.cursor(cursor_factory=RealDictCursor)

# Test queries
cursor.execute("SELECT COUNT(*) as count FROM connector_definitions")
print(f"Connector definitions: {cursor.fetchone()['count']}")

cursor.execute("SELECT config_key, config_value FROM system_config ORDER BY config_key")
for row in cursor.fetchall():
    print(f"{row['config_key']}: {row['config_value']}")

cursor.close()
conn.close()
```

**Python Example (MySQL):**
```python
import pymysql
from pymysql.cursors import DictCursor

# Connect
conn = pymysql.connect(
    host="localhost",
    port=3306,
    database="fusion_cdc_metadata",
    user="fusion_user",
    password="your_secure_password_here",
    cursorclass=DictCursor
)

cursor = conn.cursor()

# Test queries
cursor.execute("SELECT COUNT(*) as count FROM connector_definitions")
print(f"Connector definitions: {cursor.fetchone()['count']}")

cursor.execute("SELECT config_key, config_value FROM system_config ORDER BY config_key")
for row in cursor.fetchall():
    print(f"{row['config_key']}: {row['config_value']}")

cursor.close()
conn.close()
```

---

## Troubleshooting

### PostgreSQL Issues

#### Problem: `uuid-ossp` extension not available

```bash
# Install PostgreSQL contrib package
sudo apt install postgresql-contrib-16  # Ubuntu/Debian
brew install postgresql@16  # macOS (includes contrib)

# Enable extension
psql -U fusion_user -d fusion_cdc_metadata \
  -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";"
```

#### Problem: Permission denied for schema public

```sql
-- Grant all privileges on schema
\c fusion_cdc_metadata
GRANT ALL ON SCHEMA public TO fusion_user;
GRANT ALL ON ALL TABLES IN SCHEMA public TO fusion_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO fusion_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO fusion_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO fusion_user;
```

#### Problem: Connection refused

```bash
# Check PostgreSQL is running
sudo systemctl status postgresql

# Check listening address in postgresql.conf
# Should have: listen_addresses = '*' or '127.0.0.1'

# Check pg_hba.conf allows connections
sudo nano /etc/postgresql/16/main/pg_hba.conf
# Add: host    all    all    127.0.0.1/32    md5

# Restart PostgreSQL
sudo systemctl restart postgresql
```

#### Problem: Table partitioning fails

```sql
-- Check PostgreSQL version (must be 10+)
SELECT version();

-- Manually create audit_log partitions if needed
CREATE TABLE audit_log_2025_11 PARTITION OF audit_log
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
```

### MySQL Issues

#### Problem: JSON type not supported

```bash
# Check MySQL version (must be 8.0+)
mysql --version

# If < 8.0, upgrade MySQL
# Ubuntu/Debian: sudo apt install mysql-server-8.0
# macOS: brew upgrade mysql@8.0
```

#### Problem: UUID() function not found

```sql
-- Check MySQL version (UUID() added in 8.0.13+)
SELECT VERSION();

-- Test UUID generation
SELECT UUID();

-- If error, upgrade to MySQL 8.0.13+
```

#### Problem: Access denied for user

```sql
-- Re-grant privileges
GRANT ALL PRIVILEGES ON fusion_cdc_metadata.* TO 'fusion_user'@'localhost';
FLUSH PRIVILEGES;

-- Verify grants
SHOW GRANTS FOR 'fusion_user'@'localhost';

-- If using remote access (Docker, etc.)
GRANT ALL PRIVILEGES ON fusion_cdc_metadata.* TO 'fusion_user'@'%';
FLUSH PRIVILEGES;
```

#### Problem: Character set issues

```sql
-- Check database character set
SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME
FROM INFORMATION_SCHEMA.SCHEMATA
WHERE SCHEMA_NAME = 'fusion_cdc_metadata';

-- Should be: utf8mb4, utf8mb4_unicode_ci

-- If incorrect, recreate database
DROP DATABASE IF EXISTS fusion_cdc_metadata;
CREATE DATABASE fusion_cdc_metadata 
  CHARACTER SET utf8mb4 
  COLLATE utf8mb4_unicode_ci;
```

#### Problem: Max packet size exceeded

```sql
-- Increase max_allowed_packet in my.cnf
[mysqld]
max_allowed_packet=256M

-- Or set dynamically
SET GLOBAL max_allowed_packet=268435456;
```

### General Issues

#### Problem: Foreign key constraint fails

```
Error: Cannot add foreign key constraint
```

**Cause:** Main app tables (banks, sub_tenants, users) don't exist in referenced database.

**Solution:**
1. Set up main Fusion application database first
2. Or temporarily comment out FK constraints for testing
3. Ensure `MAIN_APP_DB_*` environment variables point to correct database

#### Problem: Schema files not found

```bash
# Ensure you're in correct directory
cd /Users/rishikeshsrinivas/Workspace/fusion-cdc-engine/schemas
ls -la *.sql

# Should see:
# schema_postgres.sql
# schema_mysql.sql
# postgres_jsonb_gin_indexes.sql
# seed_data.sql
```

---

## Performance Tuning

### PostgreSQL Optimization

```bash
# Edit postgresql.conf
sudo nano /etc/postgresql/16/main/postgresql.conf
```

```properties
# Memory settings (adjust based on available RAM)
shared_buffers = 4GB                    # 25% of RAM
effective_cache_size = 12GB             # 75% of RAM
maintenance_work_mem = 1GB
work_mem = 64MB

# Checkpoint settings
checkpoint_completion_target = 0.9
wal_buffers = 16MB
max_wal_size = 4GB
min_wal_size = 1GB

# Query optimization
default_statistics_target = 100
random_page_cost = 1.1                  # For SSD
effective_io_concurrency = 200          # For SSD

# Parallel query
max_parallel_workers_per_gather = 4
max_parallel_workers = 8
max_worker_processes = 8

# Connection pooling
max_connections = 200

# JIT compilation (PostgreSQL 11+)
jit = on
```

```bash
# Restart PostgreSQL
sudo systemctl restart postgresql
```

**Regular Maintenance:**
```sql
-- Analyze tables after bulk data load
ANALYZE;

-- Vacuum regularly (automated by autovacuum)
VACUUM ANALYZE;

-- Reindex if needed
REINDEX DATABASE fusion_cdc_metadata;
```

### MySQL Optimization

```bash
# Edit my.cnf
sudo nano /etc/mysql/my.cnf
```

```properties
[mysqld]
# Memory settings (adjust based on available RAM)
innodb_buffer_pool_size = 8G           # 70% of RAM
innodb_log_file_size = 1G
innodb_log_buffer_size = 256M
innodb_flush_log_at_trx_commit = 2

# Query cache (be cautious - can hurt write performance)
query_cache_type = 0                   # Disabled by default in MySQL 8.0

# Connection settings
max_connections = 500
max_connect_errors = 100

# Performance schema
performance_schema = ON

# Binary logging (for replication)
log_bin = mysql-bin
binlog_format = ROW
expire_logs_days = 7

# Character set
character_set_server = utf8mb4
collation_server = utf8mb4_unicode_ci

# Table settings
table_open_cache = 4000
tmp_table_size = 256M
max_heap_table_size = 256M
```

```bash
# Restart MySQL
sudo systemctl restart mysql
```

**Regular Maintenance:**
```sql
-- Analyze tables after bulk data load
ANALYZE TABLE connector_definitions, sources, destinations, connections, streams;

-- Optimize tables periodically
OPTIMIZE TABLE connector_definitions, sources, destinations, connections;

-- Check table status
SHOW TABLE STATUS;
```

---

## Backup & Recovery

### PostgreSQL Backup

**Full Database Backup:**
```bash
# Backup to compressed custom format
pg_dump -U fusion_user \
  -d fusion_cdc_metadata \
  -F c \
  -f backup_$(date +%Y%m%d_%H%M%S).dump

# Backup to SQL format
pg_dump -U fusion_user \
  -d fusion_cdc_metadata \
  > backup_$(date +%Y%m%d_%H%M%S).sql
```

**Schema Only:**
```bash
pg_dump -U fusion_user \
  -d fusion_cdc_metadata \
  --schema-only \
  > schema_backup_$(date +%Y%m%d).sql
```

**Specific Tables:**
```bash
pg_dump -U fusion_user \
  -d fusion_cdc_metadata \
  -t connections -t sources -t destinations \
  > critical_tables_backup.sql
```

**Restore:**
```bash
# From custom format
pg_restore -U fusion_user \
  -d fusion_cdc_metadata \
  -c \
  backup_20251130_120000.dump

# From SQL format
psql -U fusion_user \
  -d fusion_cdc_metadata \
  < backup_20251130_120000.sql
```

**Automated Backup Script:**
```bash
#!/bin/bash
# backup_postgres.sh

BACKUP_DIR="/backups/postgres"
DB_NAME="fusion_cdc_metadata"
DB_USER="fusion_user"
RETENTION_DAYS=30

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Backup filename with timestamp
BACKUP_FILE="$BACKUP_DIR/${DB_NAME}_$(date +%Y%m%d_%H%M%S).dump"

# Perform backup
pg_dump -U "$DB_USER" -d "$DB_NAME" -F c -f "$BACKUP_FILE"

# Compress backup
gzip "$BACKUP_FILE"

# Remove old backups
find "$BACKUP_DIR" -name "${DB_NAME}_*.dump.gz" -mtime +$RETENTION_DAYS -delete

echo "Backup completed: ${BACKUP_FILE}.gz"
```

**Schedule with cron:**
```bash
# Daily backup at 2 AM
0 2 * * * /path/to/backup_postgres.sh >> /var/log/postgres_backup.log 2>&1
```

### MySQL Backup

**Full Database Backup:**
```bash
# Backup to SQL file
mysqldump -u fusion_user -p \
  fusion_cdc_metadata \
  > backup_$(date +%Y%m%d_%H%M%S).sql

# Backup with compression
mysqldump -u fusion_user -p \
  fusion_cdc_metadata \
  | gzip > backup_$(date +%Y%m%d_%H%M%S).sql.gz
```

**Schema Only:**
```bash
mysqldump -u fusion_user -p \
  --no-data \
  fusion_cdc_metadata \
  > schema_backup_$(date +%Y%m%d).sql
```

**Specific Tables:**
```bash
mysqldump -u fusion_user -p \
  fusion_cdc_metadata \
  connections sources destinations \
  > critical_tables_backup.sql
```

**Restore:**
```bash
# From uncompressed SQL
mysql -u fusion_user -p \
  fusion_cdc_metadata \
  < backup_20251130_120000.sql

# From compressed SQL
gunzip < backup_20251130_120000.sql.gz | \
  mysql -u fusion_user -p fusion_cdc_metadata
```

**Automated Backup Script:**
```bash
#!/bin/bash
# backup_mysql.sh

BACKUP_DIR="/backups/mysql"
DB_NAME="fusion_cdc_metadata"
DB_USER="fusion_user"
DB_PASS="your_password"  # Or use ~/.my.cnf for security
RETENTION_DAYS=30

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Backup filename with timestamp
BACKUP_FILE="$BACKUP_DIR/${DB_NAME}_$(date +%Y%m%d_%H%M%S).sql"

# Perform backup
mysqldump -u "$DB_USER" -p"$DB_PASS" \
  "$DB_NAME" > "$BACKUP_FILE"

# Compress backup
gzip "$BACKUP_FILE"

# Remove old backups
find "$BACKUP_DIR" -name "${DB_NAME}_*.sql.gz" -mtime +$RETENTION_DAYS -delete

echo "Backup completed: ${BACKUP_FILE}.gz"
```

**Schedule with cron:**
```bash
# Daily backup at 2 AM
0 2 * * * /path/to/backup_mysql.sh >> /var/log/mysql_backup.log 2>&1
```

---

## Additional Resources

- **Main Application Integration:** See `user-management/README.md` for main Fusion app architecture
- **Schema Documentation:** See `docs/schema_implementation_plan.md` for complete table specifications
- **Overall Architecture:** See root `README.md` for CDC Engine overview

---

## Support

For issues or questions related to database setup:
1. Check this guide's Troubleshooting section
2. Review schema implementation plan: `docs/schema_implementation_plan.md`
3. Verify main application integration requirements
4. Check PostgreSQL/MySQL official documentation

---

**Last Updated:** November 30, 2025  
**Schema Version:** 1.0.0  
**Compatible Databases:** PostgreSQL 14+, MySQL 8.0+
