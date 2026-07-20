# Fusion CDC Engine

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed and running
- Python 3.11+
- PostgreSQL 16+ (or use Docker)
- Redis 7+ (or use Docker)

### Setup Development Environment

```bash
# 1. Clone repository (already done)
cd fusion-cdc-engine

# 2. Run setup script
chmod +x setup_dev.sh
./setup_dev.sh

# 3. Start Control Plane API
cd control-plane
source .venv/bin/activate
python -m app.main
```

### Access API Documentation
- **Swagger UI**: http://localhost:8000/api/docs
- **ReDoc**: http://localhost:8000/api/redoc
- **Health Check**: http://localhost:8000/health

## 📚 Project Structure

```
fusion-cdc-engine/
├── control-plane/           # FastAPI Control Plane
│   ├── app/
│   │   ├── api/            # API endpoints
│   │   ├── models/         # SQLAlchemy models
│   │   ├── services/       # Business logic
│   │   ├── middleware/     # Auth, tenant isolation
│   │   ├── config.py       # Configuration
│   │   ├── database.py     # DB connection
│   │   └── main.py         # FastAPI app
│   ├── tests/              # Unit tests
│   ├── requirements.txt    # Python dependencies
│   └── .env.example        # Environment variables
│
├── cdc-workers/             # CDC Worker Service
│   ├── cdc_worker/         # Core worker code
│   └── connectors/         # MySQL, Postgres, MongoDB
│
├── spark-consumer/          # Spark CDC Consumer
│   ├── jobs/               # Spark jobs
│   └── lib/                # Transform, DQ engines
│
├── schemas/                 # Database DDL
│   ├── schema_postgres.sql
│   ├── schema_mysql.sql
│   └── seed_data.sql
│
├── migrations/              # Alembic migrations
├── docs/                    # Documentation
├── DEVELOPMENT_PLAN.md      # Development roadmap
└── API_SPECIFICATION.md     # Complete API specs
```

## 📖 Documentation

- **[Development Plan](./DEVELOPMENT_PLAN.md)** - Comprehensive development roadmap
- **[API Specification](./API_SPECIFICATION.md)** - Complete API documentation
- **[Database Schema](./schemas/DATABASE_SETUP.md)** - Database setup guide
- **[Integration Testing](./docs/INTEGRATION_TESTING_PLAN.md)** - Testing strategy

## 🎯 Current Status

### ✅ Completed
- Database schema design (42 tables)
- Development plan and TODO list
- Complete API specification
- Control Plane skeleton (FastAPI)
- All API endpoint stubs
- Project structure

### 🚧 In Progress
- SQLAlchemy models
- Authentication middleware
- API implementation

### 📋 Next Steps
1. Implement SQLAlchemy models (TODO #4)
2. Implement JWT authentication (TODO #5)
3. Implement Sources API (TODO #6)
4. Implement other CRUD endpoints
5. Start CDC workers implementation

## 🔧 Development Commands

### Start Control Plane
```bash
cd control-plane
source .venv/bin/activate
python -m app.main
# or with auto-reload
uvicorn app.main:app --reload --port 8000
```

### Run Tests
```bash
cd control-plane
pytest tests/
```

### Database Migrations
```bash
cd migrations
alembic upgrade head          # Apply migrations
alembic revision --autogenerate -m "description"  # Create new migration
```

## 🐳 Docker Support (Coming Soon)
```bash
docker-compose up -d          # Start all services
docker-compose logs -f        # View logs
docker-compose down           # Stop all services
```

## 📊 API Endpoints Summary

### Available Now
- ✅ `GET /health` - Health check
- ✅ `GET /api/docs` - Swagger documentation
- ✅ All endpoint stubs returning mock data

### Implementation Priority
1. Sources API (Create, List, Test Connection, Discover Schemas)
2. Destinations API (Create, List, Test Connection)
3. Connections API (Create, List, Activate, Pause)
4. Monitoring API (Health, Lag, Throughput)
5. Transformations API
6. Data Quality API

## 🤝 Contributing

See [DEVELOPMENT_PLAN.md](./DEVELOPMENT_PLAN.md) for detailed implementation guidelines.

## 📝 License

Proprietary - Fusion CDC Engine

---

**Version:** 0.1.0  
**Last Updated:** 8 December 2025  
**Status:** Active Development - Backend & API First
