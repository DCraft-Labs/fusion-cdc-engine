# Flyway Migrations for Fusion CDC Engine (MySQL)

This directory contains database migration scripts for the Fusion CDC Engine MySQL metadata database using Flyway.

## Overview

- **Database:** MySQL 8.0+
- **Migration Tool:** Flyway 11.x
- **Total Tables:** 42 tables across 8 categories
- **Schema Location:** `sql/V1__initial_schema.sql`

## Directory Structure

```
flyway/
├── flyway.conf              # Flyway configuration file
├── sql/                     # Migration scripts directory
│   └── V1__initial_schema.sql   # Initial schema (42 tables)
└── README.md               # This file
```

## Initial Setup

### 1. Install Flyway

**macOS (Homebrew):**
```bash
brew install flyway
```

**Linux:**
```bash
wget -qO- https://download.red-gate.com/maven/release/com/redgate/flyway/flyway-commandline/11.18.0/flyway-commandline-11.18.0-linux-x64.tar.gz | tar -xvz
sudo mv flyway-11.18.0 /opt/flyway
export PATH=$PATH:/opt/flyway
```

**Windows:**
Download from https://flywaydb.org/download and add to PATH.

### 2. Configure Database Connection

Edit `flyway.conf` and update these settings:

```properties
flyway.url=jdbc:mysql://localhost:3306/fusion_cdc_metadata?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC
flyway.user=fusion_user
flyway.password=your_actual_password
```

**Recommended:** Use environment variables instead:

```bash
export FLYWAY_URL="jdbc:mysql://localhost:3306/fusion_cdc_metadata?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
export FLYWAY_USER="fusion_user"
export FLYWAY_PASSWORD="your_secure_password"
```

### 3. Create MySQL Database

```sql
-- Connect to MySQL
mysql -u root -p

-- Create database
CREATE DATABASE fusion_cdc_metadata CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Create user
CREATE USER 'fusion_user'@'localhost' IDENTIFIED BY 'your_password';

-- Grant permissions
GRANT ALL PRIVILEGES ON fusion_cdc_metadata.* TO 'fusion_user'@'localhost';
FLUSH PRIVILEGES;

-- Verify
SHOW DATABASES;
```

### 4. Apply Migrations

Navigate to the flyway directory:

```bash
cd /path/to/fusion-cdc-engine/flyway
```

Run Flyway commands:

```bash
# View migration status (pending migrations)
flyway info

# Apply all pending migrations
flyway migrate

# Verify migration history
flyway info
```

## Flyway Commands

### Information Commands

```bash
# Show current migration status
flyway info

# Validate applied migrations against available migrations
flyway validate
```

### Migration Commands

```bash
# Apply all pending migrations
flyway migrate

# Migrate to specific version
flyway migrate -target=1

# Undo last migration (Flyway Teams feature)
flyway undo

# Redo last migration
flyway undo && flyway migrate
```

### Baseline Commands

Use baseline when applying Flyway to an **existing database**:

```bash
# Mark current database state as baseline
flyway baseline

# Set specific baseline version
flyway baseline -baselineVersion=1 -baselineDescription="Existing schema"

# Then apply remaining migrations
flyway migrate
```

### Repair Commands

```bash
# Repair migration history table
# Useful when migration checksums change
flyway repair
```

### Clean Command (⚠️ DANGEROUS - Development Only!)

```bash
# Drop all database objects (NEVER use in production!)
flyway clean
```

## Migration File Naming Convention

Flyway uses specific naming patterns:

### Versioned Migrations
Format: `V<version>__<description>.sql`

Examples:
- `V1__initial_schema.sql` - Initial 42-table schema
- `V2__add_kafka_connector.sql` - Add Kafka connector support
- `V3__modify_connection_pooling.sql` - Update connection pool settings
- `V4.1__add_column_to_sources.sql` - Minor version increment

### Repeatable Migrations
Format: `R__<description>.sql`

Examples:
- `R__create_views.sql` - Recreate views (runs every time checksum changes)
- `R__update_stored_procedures.sql` - Update stored procedures

## Creating New Migrations

### 1. Create New Migration File

Create a new file in the `sql/` directory:

```bash
# Manual creation
touch sql/V2__add_kafka_connector.sql
```

### 2. Write Migration SQL

```sql
-- V2__add_kafka_connector.sql
-- Add Kafka streaming connector

INSERT INTO connector_definitions (
    connector_id,
    connector_name,
    connector_type,
    category,
    latest_version,
    supports_cdc,
    is_active
) VALUES (
    UUID(),
    'Kafka',
    'kafka',
    'destination',
    '3.7.0',
    1,
    1
);

-- Add Kafka-specific configuration table
CREATE TABLE kafka_topic_mappings (
    mapping_id CHAR(36) PRIMARY KEY,
    connection_id CHAR(36) NOT NULL,
    source_table VARCHAR(255) NOT NULL,
    kafka_topic VARCHAR(255) NOT NULL,
    partition_strategy VARCHAR(50) DEFAULT 'round_robin',
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

### 3. Test Migration

```bash
# Check status
flyway info

# Apply migration
flyway migrate

# Verify
flyway info
```

### 4. Verify in Database

```bash
mysql -u fusion_user -p fusion_cdc_metadata

# Check migration history
SELECT * FROM flyway_schema_history ORDER BY installed_rank;

# Verify new objects
SHOW TABLES;
DESCRIBE kafka_topic_mappings;
```

## Integration with Main Application

The CDC Engine metadata database has foreign key relationships with the main Fusion application:

- `sub_tenant_id` → `sub_tenants.id` (main app)
- `bank_id` → `banks.id` (main app)
- `created_by`, `updated_by` → `users.id` (main app)

**Important:** Ensure the main application database is running and accessible before applying migrations.

## Troubleshooting

### Migration Fails with "Checksum mismatch"

This occurs when a migration file was modified after being applied.

**Solution 1: Repair migration history**
```bash
flyway repair
```

**Solution 2: Create a new migration**
```bash
# Don't modify V1, create V2 instead
touch sql/V2__fix_previous_migration.sql
```

### Connection Refused

Check MySQL is running:
```bash
mysql -u fusion_user -p fusion_cdc_metadata
```

Verify connection string in `flyway.conf`:
```properties
flyway.url=jdbc:mysql://localhost:3306/fusion_cdc_metadata?...
```

### Permission Denied

Grant proper permissions:
```sql
GRANT ALL PRIVILEGES ON fusion_cdc_metadata.* TO 'fusion_user'@'localhost';
FLUSH PRIVILEGES;
```

### Baseline Already Exists

If you see "baseline already exists" error:
```bash
# Check current state
flyway info

# Apply remaining migrations
flyway migrate
```

### Failed Migration (Partial Apply)

Flyway tracks failed migrations. Options:

**Option 1: Fix and repair**
```bash
# Fix the SQL in migration file
# Repair migration history
flyway repair

# Retry
flyway migrate
```

**Option 2: Manual cleanup**
```bash
# Connect to MySQL
mysql -u fusion_user -p fusion_cdc_metadata

# Check failed migration
SELECT * FROM flyway_schema_history WHERE success = 0;

# Manually revert changes from failed migration
# Then delete failed entry
DELETE FROM flyway_schema_history WHERE version = '2' AND success = 0;

# Fix SQL file and retry
flyway migrate
```

## Best Practices

### 1. Version Control
- Always commit migration files to Git
- Never modify applied migrations
- Create new migrations for changes

### 2. Testing
- Test migrations in development first
- Use separate databases for dev/staging/prod
- Backup production before migration

### 3. Rollback Strategy
- Create `undo` migrations for critical changes
- Document rollback procedures
- Test rollbacks in staging

### 4. Security
- Never commit passwords in `flyway.conf`
- Use environment variables for credentials
- Restrict database user permissions

### 5. Migration Scripts
- Keep migrations small and focused
- Use transactions where possible
- Add comments explaining complex changes
- Include rollback instructions in comments

### 6. Naming
- Use descriptive migration names
- Follow semantic versioning (V1, V2, V3...)
- Use timestamps for conflict resolution (V2025_01_15_...)

## Monitoring Migration History

### View All Migrations

```sql
SELECT 
    installed_rank,
    version,
    description,
    type,
    script,
    installed_on,
    execution_time,
    success
FROM flyway_schema_history 
ORDER BY installed_rank;
```

### Check Latest Migration

```sql
SELECT * FROM flyway_schema_history 
WHERE success = 1 
ORDER BY installed_rank DESC 
LIMIT 1;
```

### Count Tables

```sql
SELECT COUNT(*) as table_count 
FROM information_schema.tables 
WHERE table_schema = 'fusion_cdc_metadata' 
AND table_type = 'BASE TABLE';
-- Expected: 42 tables (+ flyway_schema_history)
```

## Environment-Specific Configurations

### Development
```bash
export FLYWAY_URL="jdbc:mysql://localhost:3306/fusion_cdc_dev"
export FLYWAY_USER="dev_user"
export FLYWAY_PASSWORD="dev_password"
```

### Staging
```bash
export FLYWAY_URL="jdbc:mysql://staging-db:3306/fusion_cdc_staging"
export FLYWAY_USER="staging_user"
export FLYWAY_PASSWORD="staging_secure_password"
```

### Production
```bash
export FLYWAY_URL="jdbc:mysql://prod-db.example.com:3306/fusion_cdc_metadata"
export FLYWAY_USER="prod_user"
export FLYWAY_PASSWORD="$(cat /secure/path/db_password)"
export FLYWAY_BASELINE_ON_MIGRATE=false  # Never auto-baseline in prod
```

## Automation

### CI/CD Pipeline Example

```yaml
# .github/workflows/flyway-migrate.yml
name: Database Migration

on:
  push:
    branches: [main]

jobs:
  migrate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Install Flyway
        run: |
          wget -qO- https://download.red-gate.com/maven/release/com/redgate/flyway/flyway-commandline/11.18.0/flyway-commandline-11.18.0-linux-x64.tar.gz | tar -xvz
          sudo mv flyway-11.18.0 /opt/flyway
          export PATH=$PATH:/opt/flyway
      
      - name: Run Migrations
        env:
          FLYWAY_URL: ${{ secrets.FLYWAY_URL }}
          FLYWAY_USER: ${{ secrets.FLYWAY_USER }}
          FLYWAY_PASSWORD: ${{ secrets.FLYWAY_PASSWORD }}
        run: |
          cd flyway
          flyway info
          flyway migrate
          flyway info
```

### Backup Before Migration Script

```bash
#!/bin/bash
# backup-and-migrate.sh

DB_NAME="fusion_cdc_metadata"
BACKUP_DIR="/backups/mysql"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create backup
echo "Creating backup..."
mysqldump -u fusion_user -p $DB_NAME > "$BACKUP_DIR/${DB_NAME}_${TIMESTAMP}.sql"

# Run migration
echo "Running Flyway migration..."
cd /path/to/flyway
flyway migrate

# Verify
if [ $? -eq 0 ]; then
    echo "Migration successful!"
    flyway info
else
    echo "Migration failed! Restore from backup:"
    echo "mysql -u fusion_user -p $DB_NAME < $BACKUP_DIR/${DB_NAME}_${TIMESTAMP}.sql"
    exit 1
fi
```

## See Also

- Schema documentation: `../docs/schema_implementation_plan.md`
- Database setup guide: `../schemas/DATABASE_SETUP.md`
- Seed data: `../schemas/seed_data.sql`
- PostgreSQL migrations: `../migrations/README`
- Main application integration: `../user-management/README.md`

## Resources

- Flyway Documentation: https://flywaydb.org/documentation/
- Flyway CLI Commands: https://flywaydb.org/documentation/usage/commandline/
- MySQL JDBC URL: https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-jdbc-url-format.html
