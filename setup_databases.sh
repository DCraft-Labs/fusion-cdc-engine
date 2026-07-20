#!/bin/bash
# ============================================================================
# Database Setup Script for Fusion CDC Engine
# ============================================================================
# This script creates the PostgreSQL and MySQL databases if they don't exist
# Run this script before applying migrations
# ============================================================================

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "============================================================================"
echo "Fusion CDC Engine - Database Setup"
echo "============================================================================"

# ============================================================================
# POSTGRESQL SETUP
# ============================================================================
echo -e "\n${YELLOW}[1/2] Setting up PostgreSQL database...${NC}"

# Check if PostgreSQL is running
if ! psql -U postgres -c "SELECT 1" >/dev/null 2>&1; then
    echo -e "${YELLOW}PostgreSQL is not running or connection failed.${NC}"
    echo "Trying to connect with current user..."
    
    # Try with current user
    if psql -c "SELECT 1" >/dev/null 2>&1; then
        PGUSER=$(whoami)
        echo -e "${GREEN}✓ Connected as user: $PGUSER${NC}"
    else
        echo -e "${RED}✗ Cannot connect to PostgreSQL. Please ensure it's running.${NC}"
        echo "  Start PostgreSQL with: brew services start postgresql@14"
        exit 1
    fi
else
    PGUSER="postgres"
    echo -e "${GREEN}✓ Connected as user: postgres${NC}"
fi

# Create database if it doesn't exist
echo "Creating database 'fusion_cdc_metadata'..."
psql -U $PGUSER -tc "SELECT 1 FROM pg_database WHERE datname = 'fusion_cdc_metadata'" | grep -q 1 || \
    psql -U $PGUSER -c "CREATE DATABASE fusion_cdc_metadata;"

echo -e "${GREEN}✓ Database 'fusion_cdc_metadata' is ready${NC}"

# Create user if it doesn't exist (optional)
echo "Checking user 'fusion_user'..."
if ! psql -U $PGUSER -tc "SELECT 1 FROM pg_roles WHERE rolname='fusion_user'" | grep -q 1; then
    echo "Creating user 'fusion_user'..."
    psql -U $PGUSER -c "CREATE USER fusion_user WITH PASSWORD 'fusion_password';"
    echo -e "${GREEN}✓ User 'fusion_user' created${NC}"
else
    echo -e "${GREEN}✓ User 'fusion_user' already exists${NC}"
fi

# Grant privileges
echo "Granting privileges..."
psql -U $PGUSER -c "GRANT ALL PRIVILEGES ON DATABASE fusion_cdc_metadata TO fusion_user;"
psql -U $PGUSER -d fusion_cdc_metadata -c "GRANT ALL ON SCHEMA public TO fusion_user;"
echo -e "${GREEN}✓ PostgreSQL setup complete!${NC}"

# ============================================================================
# MYSQL SETUP
# ============================================================================
echo -e "\n${YELLOW}[2/2] Setting up MySQL database...${NC}"

# Check if MySQL is running
if ! mysql -u root -e "SELECT 1" >/dev/null 2>&1; then
    echo -e "${YELLOW}MySQL is not running or root connection failed.${NC}"
    echo "Trying without password..."
    
    if ! mysql -e "SELECT 1" >/dev/null 2>&1; then
        echo -e "${RED}✗ Cannot connect to MySQL. Please ensure it's running.${NC}"
        echo "  Start MySQL with: brew services start mysql"
        echo "  Or skip MySQL setup (will continue with PostgreSQL only)"
        read -p "Continue without MySQL? (y/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
        echo -e "${YELLOW}⚠ Skipping MySQL setup${NC}"
    else
        MYSQL_USER=$(whoami)
        MYSQL_CMD="mysql"
        echo -e "${GREEN}✓ Connected as current user${NC}"
        
        # Create database
        echo "Creating database 'fusion_cdc_metadata'..."
        $MYSQL_CMD -e "CREATE DATABASE IF NOT EXISTS fusion_cdc_metadata CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
        echo -e "${GREEN}✓ Database 'fusion_cdc_metadata' is ready${NC}"
        
        # Create user if needed
        echo "Creating user 'fusion_user'..."
        $MYSQL_CMD -e "CREATE USER IF NOT EXISTS 'fusion_user'@'localhost' IDENTIFIED BY 'fusion_password';" 2>/dev/null || true
        $MYSQL_CMD -e "GRANT ALL PRIVILEGES ON fusion_cdc_metadata.* TO 'fusion_user'@'localhost';"
        $MYSQL_CMD -e "FLUSH PRIVILEGES;"
        echo -e "${GREEN}✓ MySQL setup complete!${NC}"
    fi
else
    MYSQL_CMD="mysql -u root"
    echo -e "${GREEN}✓ Connected as user: root${NC}"
    
    # Create database
    echo "Creating database 'fusion_cdc_metadata'..."
    $MYSQL_CMD -e "CREATE DATABASE IF NOT EXISTS fusion_cdc_metadata CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    echo -e "${GREEN}✓ Database 'fusion_cdc_metadata' is ready${NC}"
    
    # Create user
    echo "Creating user 'fusion_user'..."
    $MYSQL_CMD -e "CREATE USER IF NOT EXISTS 'fusion_user'@'localhost' IDENTIFIED BY 'fusion_password';" 2>/dev/null || true
    $MYSQL_CMD -e "GRANT ALL PRIVILEGES ON fusion_cdc_metadata.* TO 'fusion_user'@'localhost';"
    $MYSQL_CMD -e "FLUSH PRIVILEGES;"
    echo -e "${GREEN}✓ MySQL setup complete!${NC}"
fi

# ============================================================================
# SUMMARY
# ============================================================================
echo -e "\n${GREEN}============================================================================${NC}"
echo -e "${GREEN}Database Setup Complete!${NC}"
echo -e "${GREEN}============================================================================${NC}"
echo -e "\n${YELLOW}PostgreSQL:${NC}"
echo "  Database: fusion_cdc_metadata"
echo "  User: fusion_user"
echo "  Password: fusion_password"
echo "  Connection: postgresql://fusion_user:fusion_password@localhost:5432/fusion_cdc_metadata"
echo -e "\n${YELLOW}MySQL:${NC}"
echo "  Database: fusion_cdc_metadata"
echo "  User: fusion_user"
echo "  Password: fusion_password"
echo "  Connection: mysql://fusion_user:fusion_password@localhost:3306/fusion_cdc_metadata"

echo -e "\n${YELLOW}Next Steps:${NC}"
echo "  1. For PostgreSQL migrations:"
echo "     cd /Users/rishikeshsrinivas/Workspace/fusion-cdc-engine"
echo "     /Users/rishikeshsrinivas/Library/Python/3.9/bin/alembic upgrade head"
echo ""
echo "  2. For MySQL migrations:"
echo "     cd /Users/rishikeshsrinivas/Workspace/fusion-cdc-engine/flyway"
echo "     flyway migrate"
echo ""
echo -e "${GREEN}✓ Ready to run migrations!${NC}"
echo "============================================================================"
