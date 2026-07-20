# Database Quick Reference

## Connection Strings

### PostgreSQL (Metadata)
```
postgresql://fusion_user:fusion_password@localhost:5432/fusion_cdc_metadata
```

### MySQL (Source Support)
```
mysql://fusion_user:fusion_password@localhost:3306/fusion_cdc_metadata
```

### Redis (Event Streaming)
```
redis://localhost:6379/0
```

## Docker Commands

### Start All Containers
```bash
docker start fusion-postgres fusion-mysql fusion-redis
```

### Stop All Containers
```bash
docker stop fusion-postgres fusion-mysql fusion-redis
```

### Remove All Containers
```bash
docker rm -f fusion-postgres fusion-mysql fusion-redis
```

### View Container Status
```bash
docker ps | grep fusion
```

### View Logs
```bash
docker logs fusion-postgres
docker logs fusion-mysql
docker logs fusion-redis
```

## Database Access

### PostgreSQL CLI
```bash
docker exec -it fusion-postgres psql -U fusion_user -d fusion_cdc_metadata
```

### MySQL CLI
```bash
docker exec -it fusion-mysql mysql -u fusion_user -pfusion_password fusion_cdc_metadata
```

### Redis CLI
```bash
docker exec -it fusion-redis redis-cli
```

## Common Queries

### List All Tables
```sql
-- PostgreSQL
SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename;

-- OR use psql command
\dt
```

### Count Tables
```sql
SELECT COUNT(*) FROM information_schema.tables 
WHERE table_schema='public' AND table_type='BASE TABLE';
```

### View Connectors
```sql
SELECT connector_name, connector_type, category, supports_cdc 
FROM connector_definitions 
ORDER BY category, connector_name;
```

### View System Config
```sql
SELECT config_key, config_value 
FROM system_config 
ORDER BY config_key;
```

### View Feature Flags
```sql
SELECT flag_name, is_enabled, description 
FROM feature_flags 
ORDER BY flag_name;
```

### Check Migration Status
```sql
SELECT version_num FROM alembic_version;
```

## Verification

### Run Verification Script
```bash
./verify_database.sh
```

### Run Test Suite
```bash
cd control-plane
source .venv/bin/activate
pytest tests/test_database_setup.py -v
```

### Test API Health
```bash
curl http://localhost:8000/health
curl http://localhost:8000/health/ready
```

## Alembic Migrations

### View Current Version
```bash
alembic current
```

### View Migration History
```bash
alembic history
```

### Upgrade to Latest
```bash
alembic upgrade head
```

### Downgrade One Version
```bash
alembic downgrade -1
```

### Create New Migration
```bash
alembic revision -m "description"
```

### Auto-generate Migration
```bash
alembic revision --autogenerate -m "description"
```

## Troubleshooting

### PostgreSQL Connection Issues
```bash
# Check if container is running
docker ps | grep fusion-postgres

# Check logs
docker logs fusion-postgres

# Restart container
docker restart fusion-postgres

# Test connection
docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata -c "SELECT 1"
```

### Reset Database
```bash
# WARNING: This will delete all data
docker exec fusion-postgres psql -U postgres -c "DROP DATABASE fusion_cdc_metadata;"
docker exec fusion-postgres psql -U postgres -c "CREATE DATABASE fusion_cdc_metadata OWNER fusion_user;"
alembic upgrade head
docker exec -i fusion-postgres psql -U fusion_user -d fusion_cdc_metadata < schemas/seed_data.sql
```

### Permission Issues
```sql
-- Grant all privileges
GRANT ALL PRIVILEGES ON DATABASE fusion_cdc_metadata TO fusion_user;
GRANT ALL ON SCHEMA public TO fusion_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO fusion_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO fusion_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO fusion_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO fusion_user;
```

## Database Stats

- **Total Tables**: 42
- **Connectors**: 11 (5 sources, 6 destinations)
- **System Configs**: 13
- **Feature Flags**: 4
- **Connector Versions**: 3

## Environment Variables

Add to `.env` file:
```env
DATABASE_URL=postgresql://fusion_user:fusion_password@localhost:5432/fusion_cdc_metadata
REDIS_URL=redis://localhost:6379/0
```

## Next Steps

After database setup, proceed to:
1. Create SQLAlchemy models (TODO #4)
2. Implement API endpoints (TODO #6-14)
3. Build CDC workers (TODO #15-18)
