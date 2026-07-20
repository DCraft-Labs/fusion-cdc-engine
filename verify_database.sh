#!/bin/bash
# ============================================================================
# Database Verification Script
# Verifies that PostgreSQL, MySQL, and Redis are properly set up
# ============================================================================

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "============================================================================"
echo "Fusion CDC Engine - Database Verification"
echo "============================================================================"

# Function to check container status
check_container() {
    local container_name=$1
    if docker ps --format '{{.Names}}' | grep -q "^${container_name}$"; then
        echo -e "${GREEN}✓${NC} Container '${container_name}' is running"
        return 0
    else
        echo -e "${RED}✗${NC} Container '${container_name}' is not running"
        return 1
    fi
}

# Function to test database connection
test_postgres_connection() {
    if docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata -c "SELECT 1" >/dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} PostgreSQL connection successful"
        return 0
    else
        echo -e "${RED}✗${NC} PostgreSQL connection failed"
        return 1
    fi
}

test_mysql_connection() {
    if docker exec fusion-mysql mysql -u fusion_user -pfusion_password -e "SELECT 1" >/dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} MySQL connection successful"
        return 0
    else
        echo -e "${RED}✗${NC} MySQL connection failed"
        return 1
    fi
}

test_redis_connection() {
    if docker exec fusion-redis redis-cli PING | grep -q "PONG"; then
        echo -e "${GREEN}✓${NC} Redis connection successful"
        return 0
    else
        echo -e "${RED}✗${NC} Redis connection failed"
        return 1
    fi
}

# Check containers
echo -e "\n${BLUE}[1/5] Checking Docker Containers...${NC}"
check_container "fusion-postgres"
check_container "fusion-mysql"
check_container "fusion-redis"

# Test connections
echo -e "\n${BLUE}[2/5] Testing Database Connections...${NC}"
test_postgres_connection
test_mysql_connection
test_redis_connection

# Check PostgreSQL tables
echo -e "\n${BLUE}[3/5] Verifying PostgreSQL Schema...${NC}"
TABLE_COUNT=$(docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';")
TABLE_COUNT=$(echo $TABLE_COUNT | tr -d ' ')

if [ "$TABLE_COUNT" -eq 42 ]; then
    echo -e "${GREEN}✓${NC} All 42 tables exist"
else
    echo -e "${RED}✗${NC} Expected 42 tables but found $TABLE_COUNT"
fi

# Check seed data
echo -e "\n${BLUE}[4/5] Verifying Seed Data...${NC}"

# Check connector_definitions
CONNECTOR_COUNT=$(docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata -t -c "SELECT COUNT(*) FROM connector_definitions;")
CONNECTOR_COUNT=$(echo $CONNECTOR_COUNT | tr -d ' ')
if [ "$CONNECTOR_COUNT" -ge 11 ]; then
    echo -e "${GREEN}✓${NC} Connector definitions populated ($CONNECTOR_COUNT connectors)"
else
    echo -e "${RED}✗${NC} Expected at least 11 connectors but found $CONNECTOR_COUNT"
fi

# Check system_config
CONFIG_COUNT=$(docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata -t -c "SELECT COUNT(*) FROM system_config;")
CONFIG_COUNT=$(echo $CONFIG_COUNT | tr -d ' ')
if [ "$CONFIG_COUNT" -ge 10 ]; then
    echo -e "${GREEN}✓${NC} System config populated ($CONFIG_COUNT configs)"
else
    echo -e "${RED}✗${NC} Expected at least 10 configs but found $CONFIG_COUNT"
fi

# Check feature_flags
FLAG_COUNT=$(docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata -t -c "SELECT COUNT(*) FROM feature_flags;")
FLAG_COUNT=$(echo $FLAG_COUNT | tr -d ' ')
if [ "$FLAG_COUNT" -ge 4 ]; then
    echo -e "${GREEN}✓${NC} Feature flags populated ($FLAG_COUNT flags)"
else
    echo -e "${RED}✗${NC} Expected at least 4 flags but found $FLAG_COUNT"
fi

# Check Alembic migration
echo -e "\n${BLUE}[5/5] Verifying Alembic Migration...${NC}"
MIGRATION_VERSION=$(docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata -t -c "SELECT version_num FROM alembic_version;")
MIGRATION_VERSION=$(echo $MIGRATION_VERSION | tr -d ' ')
if [ "$MIGRATION_VERSION" == "04aff4ce3106" ]; then
    echo -e "${GREEN}✓${NC} Alembic migration applied (version: $MIGRATION_VERSION)"
else
    echo -e "${YELLOW}⚠${NC}  Unexpected migration version: $MIGRATION_VERSION"
fi

# Summary
echo -e "\n${GREEN}============================================================================${NC}"
echo -e "${GREEN}Database Verification Complete!${NC}"
echo -e "${GREEN}============================================================================${NC}"

echo -e "\n${YELLOW}Database Details:${NC}"
echo "  PostgreSQL: postgresql://fusion_user:***@localhost:5432/fusion_cdc_metadata"
echo "  MySQL:      mysql://fusion_user:***@localhost:3306/fusion_cdc_metadata"
echo "  Redis:      redis://localhost:6379/0"

echo -e "\n${YELLOW}Quick Stats:${NC}"
echo "  Total Tables:      $TABLE_COUNT"
echo "  Connectors:        $CONNECTOR_COUNT"
echo "  System Configs:    $CONFIG_COUNT"
echo "  Feature Flags:     $FLAG_COUNT"
echo "  Migration Version: $MIGRATION_VERSION"

echo -e "\n${BLUE}Next Steps:${NC}"
echo "  Run tests: cd control-plane && source .venv/bin/activate && pytest tests/test_database_setup.py -v"
echo "  Start API: cd control-plane && source .venv/bin/activate && python -m uvicorn app.main:app --reload"

echo -e "\n${GREEN}✓ Ready for development!${NC}"
echo "============================================================================"
