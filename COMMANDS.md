# Quick Command Reference

## 🚀 First Time Setup
```bash
# Run this ONCE to setup everything
./setup_dev.sh
```

## 🔧 Daily Development Commands

### Start Control Plane API
```bash
cd control-plane
source .venv/bin/activate
python -m app.main

# OR with auto-reload
uvicorn app.main:app --reload --port 8000
```

### Run Tests
```bash
cd control-plane
source .venv/bin/activate
pytest tests/ -v
pytest tests/ --cov=app --cov-report=html
```

### Database Operations
```bash
# Apply migrations
cd migrations
alembic upgrade head

# Create new migration
alembic revision --autogenerate -m "description"

# Rollback migration
alembic downgrade -1

# Check migration status
alembic current
alembic history
```

### Database Access
```bash
# PostgreSQL
psql -h localhost -U fusion_user -d fusion_cdc_metadata

# Useful SQL
\dt                           # List tables
\d sources                    # Describe sources table
SELECT * FROM sources;        # Query sources
```

### Docker Operations
```bash
# Start database containers
cd schemas
./setup_docker_databases.sh

# View running containers
docker ps | grep fusion

# View logs
docker logs fusion-postgres
docker logs fusion-mysql

# Stop containers
docker stop fusion-postgres fusion-mysql

# Remove containers
docker rm fusion-postgres fusion-mysql
```

### Code Quality
```bash
cd control-plane

# Format code
black app/

# Lint code
flake8 app/
pylint app/

# Type checking
mypy app/
```

### API Testing
```bash
# Health check
curl http://localhost:8000/health

# List sources (will need auth token later)
curl http://localhost:8000/api/v1/sources

# Create source (example)
curl -X POST http://localhost:8000/api/v1/sources \
  -H "Content-Type: application/json" \
  -d '{"source_name": "Test", "connector_type": "mysql"}'
```

## 📝 Git Workflow
```bash
# Create feature branch
git checkout -b feature/sources-api

# Commit changes
git add .
git commit -m "feat: implement sources API"

# Push branch
git push origin feature/sources-api
```

## 🐍 Python Environment
```bash
# Activate virtualenv
cd control-plane
source .venv/bin/activate

# Deactivate
deactivate

# Install new package
pip install package-name
pip freeze > requirements.txt

# Update dependencies
pip install --upgrade -r requirements.txt
```

## 📊 Monitoring
```bash
# View API logs (when running)
tail -f logs/app.log

# Monitor database connections
psql -h localhost -U fusion_user -d fusion_cdc_metadata \
  -c "SELECT * FROM pg_stat_activity;"
```

## 🔍 Debugging
```bash
# Run with debugger
python -m pdb app/main.py

# Interactive Python shell with app context
cd control-plane
source .venv/bin/activate
python
>>> from app.database import SessionLocal
>>> from app.models import Source
>>> db = SessionLocal()
>>> sources = db.query(Source).all()
```

## 🎯 Quick URLs
- API Docs: http://localhost:8000/api/docs
- ReDoc: http://localhost:8000/api/redoc
- Health: http://localhost:8000/health
- OpenAPI JSON: http://localhost:8000/api/openapi.json

## 📦 Common Tasks

### Add new API endpoint
1. Create function in `app/api/<router>.py`
2. Add route decorator (`@router.get`, `@router.post`, etc.)
3. Define Pydantic schemas
4. Implement business logic in `app/services/`
5. Add tests in `tests/test_<feature>.py`

### Add new database model
1. Create class in `app/models/<model>.py`
2. Inherit from `Base`
3. Define columns and relationships
4. Create migration: `alembic revision --autogenerate -m "add <model>"`
5. Apply migration: `alembic upgrade head`

### Add authentication
1. Extract JWT from request header
2. Validate token with Keycloak
3. Extract user info (bank_id, sub_tenant_id, roles)
4. Add to request.state
5. Use in endpoints with `Depends(get_current_user)`

## 🆘 Troubleshooting

### Port already in use
```bash
lsof -i :8000
kill -9 <PID>
```

### Database connection issues
```bash
# Check if containers are running
docker ps | grep fusion

# Restart database
docker restart fusion-postgres

# Check logs
docker logs fusion-postgres --tail 50
```

### Python import errors
```bash
# Ensure virtualenv is activated
which python  # Should show .venv path

# Reinstall dependencies
pip install -r requirements.txt
```

### Migration conflicts
```bash
# Reset to specific migration
alembic downgrade <revision>

# Force stamp
alembic stamp head
```
