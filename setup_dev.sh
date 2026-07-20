#!/bin/bash

# Fusion CDC Engine - Development Setup Script

set -e

echo "========================================="
echo "Fusion CDC Engine - Development Setup"
echo "========================================="

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "\n${YELLOW}Step 1: Setting up database...${NC}"
cd schemas
./setup_docker_databases.sh
cd ..

echo -e "\n${YELLOW}Step 2: Applying migrations...${NC}"
cd migrations
alembic upgrade head
cd ..

echo -e "\n${YELLOW}Step 3: Setting up Python virtual environment for Control Plane...${NC}"
cd control-plane
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo -e "\n${YELLOW}Step 4: Creating .env file...${NC}"
cp .env.example .env
echo -e "${GREEN}✓ Created .env file. Please update with your actual credentials.${NC}"

echo -e "\n${GREEN}=========================================${NC}"
echo -e "${GREEN}Setup Complete!${NC}"
echo -e "${GREEN}=========================================${NC}"

echo -e "\n${YELLOW}To start the Control Plane API:${NC}"
echo "cd control-plane"
echo "source .venv/bin/activate"
echo "python -m app.main"
echo ""
echo "API will be available at: http://localhost:8000"
echo "API Documentation: http://localhost:8000/api/docs"
