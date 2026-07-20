#!/bin/bash
# ============================================================================
# Docker-based Database Setup for Fusion CDC Engine
# ============================================================================
# This script sets up PostgreSQL and MySQL using Docker containers
# Requires: Docker Desktop installed and running
# ============================================================================

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "============================================================================"
echo "Fusion CDC Engine - Docker Database Setup"
echo "============================================================================"

# Check if Docker is running
echo -e "\n${BLUE}Checking Docker status...${NC}"
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}✗ Docker is not running!${NC}"
    echo "Please start Docker Desktop and try again."
    exit 1
fi
echo -e "${GREEN}✓ Docker is running${NC}"

# ============================================================================
# POSTGRESQL SETUP
# ============================================================================
echo -e "\n${YELLOW}[1/2] Setting up PostgreSQL container...${NC}"

# Check if PostgreSQL container already exists
if docker ps -a --format '{{.Names}}' | grep -q "^fusion-postgres$"; then
    echo "PostgreSQL container 'fusion-postgres' already exists"
    
    # Check if it's running
    if docker ps --format '{{.Names}}' | grep -q "^fusion-postgres$"; then
        echo -e "${GREEN}✓ Container is already running${NC}"
    else
        echo "Starting existing container..."
        docker start fusion-postgres
        echo -e "${GREEN}✓ Container started${NC}"
    fi
else
    echo "Creating PostgreSQL container..."
    docker run -d \
        --name fusion-postgres \
        -e POSTGRES_USER=fusion_user \
        -e POSTGRES_PASSWORD=fusion_password \
        -e POSTGRES_DB=fusion_cdc_metadata \
        -p 5432:5432 \
        postgres:14-alpine
    
    echo -e "${GREEN}✓ PostgreSQL container created and started${NC}"
    echo "Waiting for PostgreSQL to be ready..."
    sleep 5
fi

# Verify PostgreSQL connection
echo "Verifying PostgreSQL connection..."
for i in {1..30}; do
    if docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata -c "SELECT 1" >/dev/null 2>&1; then
        echo -e "${GREEN}✓ PostgreSQL is ready!${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}✗ PostgreSQL failed to start${NC}"
        exit 1
    fi
    echo -n "."
    sleep 1
done

# ============================================================================
# MYSQL SETUP
# ============================================================================
echo -e "\n${YELLOW}[2/2] Setting up MySQL container...${NC}"

# Check if MySQL container already exists
if docker ps -a --format '{{.Names}}' | grep -q "^fusion-mysql$"; then
    echo "MySQL container 'fusion-mysql' already exists"
    
    # Check if it's running
    if docker ps --format '{{.Names}}' | grep -q "^fusion-mysql$"; then
        echo -e "${GREEN}✓ Container is already running${NC}"
    else
        echo "Starting existing container..."
        docker start fusion-mysql
        echo -e "${GREEN}✓ Container started${NC}"
    fi
else
    echo "Creating MySQL container..."
    docker run -d \
        --name fusion-mysql \
        -e MYSQL_DATABASE=fusion_cdc_metadata \
        -e MYSQL_USER=fusion_user \
        -e MYSQL_PASSWORD=fusion_password \
        -e MYSQL_ROOT_PASSWORD=root_password \
        -p 3306:3306 \
        mysql:8.0 \
        --character-set-server=utf8mb4 \
        --collation-server=utf8mb4_unicode_ci
    
    echo -e "${GREEN}✓ MySQL container created and started${NC}"
    echo "Waiting for MySQL to be ready..."
    sleep 10
fi

# Verify MySQL connection
echo "Verifying MySQL connection..."
for i in {1..30}; do
    if docker exec fusion-mysql mysql -u fusion_user -pfusion_password -e "SELECT 1" >/dev/null 2>&1; then
        echo -e "${GREEN}✓ MySQL is ready!${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}✗ MySQL failed to start${NC}"
        exit 1
    fi
    echo -n "."
    sleep 1
done

# ============================================================================
# SUMMARY
# ============================================================================
echo -e "\n${GREEN}============================================================================${NC}"
echo -e "${GREEN}Docker Database Setup Complete!${NC}"
echo -e "${GREEN}============================================================================${NC}"

echo -e "\n${BLUE}Running Containers:${NC}"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep fusion

echo -e "\n${YELLOW}PostgreSQL:${NC}"
echo "  Database: fusion_cdc_metadata"
echo "  User: fusion_user"
echo "  Password: fusion_password"
echo "  Host: localhost"
echo "  Port: 5432"
echo "  Connection String: postgresql://fusion_user:fusion_password@localhost:5432/fusion_cdc_metadata"

echo -e "\n${YELLOW}MySQL:${NC}"
echo "  Database: fusion_cdc_metadata"
echo "  User: fusion_user"
echo "  Password: fusion_password"
echo "  Host: localhost"
echo "  Port: 3306"
echo "  Connection String: mysql://fusion_user:fusion_password@localhost:3306/fusion_cdc_metadata"

echo -e "\n${BLUE}Docker Commands:${NC}"
echo "  Stop containers:  docker stop fusion-postgres fusion-mysql"
echo "  Start containers: docker start fusion-postgres fusion-mysql"
echo "  Remove containers: docker rm -f fusion-postgres fusion-mysql"
echo "  View logs: docker logs fusion-postgres"
echo "             docker logs fusion-mysql"

echo -e "\n${YELLOW}Next Steps:${NC}"
echo "  1. For PostgreSQL migrations:"
echo "     cd /Users/rishikeshsrinivas/Workspace/fusion-cdc-engine"
echo "     /Users/rishikeshsrinivas/Library/Python/3.9/bin/alembic upgrade head"
echo ""
echo "  2. For MySQL migrations:"
echo "     cd /Users/rishikeshsrinivas/Workspace/fusion-cdc-engine/flyway"
echo "     flyway migrate"
echo ""
echo "  3. Connect to databases:"
echo "     PostgreSQL: docker exec -it fusion-postgres psql -U fusion_user -d fusion_cdc_metadata"
echo "     MySQL:      docker exec -it fusion-mysql mysql -u fusion_user -pfusion_password fusion_cdc_metadata"

echo -e "\n${GREEN}✓ Ready to run migrations!${NC}"
echo "============================================================================"
