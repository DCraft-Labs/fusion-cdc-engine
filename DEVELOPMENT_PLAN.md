# Fusion CDC Engine - Development Plan

## 📋 Document Overview

This document provides a comprehensive roadmap for implementing the Fusion CDC Engine. It covers architecture decisions, development phases, connector strategy, and detailed implementation steps.

**Last Updated:** 8 December 2025  
**Status:** Active Development - Backend & API First  
**Note:** Frontend development deferred to later phase. Focus on complete API implementation with existing multi-tenancy structure.

---

## 🎯 Development Phases (Backend-First Approach)

### Phase 1: Foundation & Control Plane (Weeks 1-4)
**Goal:** Setup infrastructure, database, and complete REST API (NO FRONTEND)
- Repository structure
- Database setup with existing multi-tenancy
- Complete FastAPI implementation
- All CRUD endpoints operational
- JWT authentication with Keycloak
- API documentation (Swagger/OpenAPI)

### Phase 2: Core CDC Functionality (Weeks 5-9)
**Goal:** Implement connectors, workers, and basic event flow
- Base connector framework
- MySQL, PostgreSQL, MongoDB connectors
- Dual-layer checkpointing
- Redis Stream integration
- Worker orchestration

### Phase 3: Processing & Transformation (Weeks 10-14)
**Goal:** Build Spark consumers, transformation engine, and destination writers
- Spark Structured Streaming consumer
- Transformation pipeline executor
- Data quality framework
- PostgreSQL warehouse writer
- Iceberg writer

### Phase 4: Integration & Testing (Weeks 15-18)
**Goal:** End-to-end integration, testing, monitoring
- Integration tests
- Multi-tenant isolation verification
- Prometheus metrics
- Grafana dashboards
- Load testing

### Phase 5: Production Deployment (Weeks 19-20)
**Goal:** Docker images, Kubernetes manifests, documentation
- Unified connector Docker image
- Kubernetes deployment manifests
- Complete API documentation
- Runbooks and troubleshooting guides

### Phase 6: Frontend (Future - Week 21+)
**Goal:** React UI for tenant management (Deferred)
- Tenant dashboard
- Connection wizard
- Monitoring views

---

## 🏗️ Architecture Stack

### Backend Services
```yaml
Control Plane API:
  Technology: FastAPI (Python) or Spring Boot (Java)
  Reason: Fast development, native async support, OpenAPI docs
  
CDC Workers:
  Technology: Python 3.11+
  Reason: Rich ecosystem for database connectors
  
Spark Consumer:
  Technology: PySpark 3.4+
  Reason: Structured streaming, transformation capabilities
```

### Infrastructure
```yaml
Metadata Database: PostgreSQL 16
Message Broker: Redis 7.x with Redis Streams
Orchestration: Apache Airflow 2.8+
Container Runtime: Docker + Kubernetes
Monitoring: Prometheus + Grafana
Secret Management: HashiCorp Vault or Kubernetes Secrets
```

### Frontend
```yaml
Framework: React 18 + TypeScript
UI Library: Material-UI or Ant Design
State Management: React Query + Zustand
Build Tool: Vite
```

---

## 🐳 Connector Architecture Strategy

### Decision: Plugin-Based Connector Architecture

Instead of monolithic images, we'll use a **modular plugin system**:

#### **Base Image Approach**

```
fusion-cdc-worker-base
├── Core CDC Framework
│   ├── Event normalization
│   ├── Checkpoint management
│   ├── Redis stream publishing
│   ├── Fallback queue
│   └── Tenant routing
└── Plugin Loader
    └── Dynamic connector loading
```

#### **Connector Plugin Images**

```
fusion-connector-mysql
├── Extends: cdc-worker-base
├── Dependencies: python-mysql-replication
└── Implementation: MySQLConnector class

fusion-connector-postgres
├── Extends: cdc-worker-base
├── Dependencies: psycopg[binary]
└── Implementation: PostgreSQLConnector class

fusion-connector-mongodb
├── Extends: cdc-worker-base
├── Dependencies: pymongo
└── Implementation: MongoDBConnector class
```

### Connector Image Strategy

#### **Option 1: Unified Image (Recommended for MVP)**

**Single Docker image** containing all connectors:

```dockerfile
FROM python:3.11-slim

# Install all connector dependencies
RUN pip install \
    python-mysql-replication \
    psycopg[binary] \
    pymongo \
    redis \
    sqlalchemy

# Copy application code
COPY ./cdc_worker /app/cdc_worker
COPY ./connectors /app/connectors

# Runtime selects connector based on env var
ENV CONNECTOR_TYPE=mysql
CMD ["python", "-m", "cdc_worker.main"]
```

**Pros:**
- ✅ Simpler deployment (one image to manage)
- ✅ Faster development iteration
- ✅ Easier local testing
- ✅ Shared dependencies reduce build time

**Cons:**
- ❌ Larger image size (~800MB vs ~300MB each)
- ❌ All dependencies present even if unused

**When to use:** MVP, small-medium deployments, development phase

#### **Option 2: Separate Images per Connector**

**Individual Docker images** for each connector type:

```dockerfile
# fusion-connector-mysql/Dockerfile
FROM fusion-cdc-worker-base:latest

RUN pip install python-mysql-replication==0.36.0
COPY ./connectors/mysql /app/connectors/mysql

ENV CONNECTOR_TYPE=mysql
CMD ["python", "-m", "cdc_worker.main"]
```

**Pros:**
- ✅ Smaller individual images
- ✅ Better security (minimal attack surface)
- ✅ Independent versioning per connector
- ✅ Easier to update single connector

**Cons:**
- ❌ More images to build/maintain
- ❌ More complex deployment
- ❌ Need to manage base image updates

**When to use:** Production at scale, enterprise deployments

#### **Option 3: Sidecar Pattern**

**Base worker + connector sidecars** in same pod:

```yaml
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: cdc-worker
    image: fusion-cdc-worker-base:latest
    
  - name: mysql-connector
    image: fusion-connector-mysql:latest
    volumeMounts:
    - name: shared-plugins
      mountPath: /plugins
```

**Pros:**
- ✅ Ultimate modularity
- ✅ Hot-swap connectors without worker restart
- ✅ Different connectors can use different languages

**Cons:**
- ❌ Complex inter-process communication
- ❌ Higher resource overhead
- ❌ More moving parts

**When to use:** Advanced use cases, multi-language connectors

---

## 📦 Recommended Approach for Development

### **Phase 1-3: Unified Image**
Start with a single image containing all connectors for rapid development.

### **Phase 4-5: Transition to Separate Images**
Split into individual connector images for production deployment.

### **Future: Plugin System**
Implement dynamic plugin loading for marketplace-style connector distribution.

---

## 🔧 Detailed Implementation Plan

### Phase 1: Foundation (Weeks 1-3)

#### Week 1: Project Setup

**1.1 Repository Structure**
```bash
mkdir -p fusion-cdc-v2/{control-plane,cdc-workers,spark-consumer,connectors,frontend,infra}

fusion-cdc-v2/
├── control-plane/          # FastAPI/Spring Boot
│   ├── app/
│   │   ├── api/           # REST endpoints
│   │   ├── models/        # SQLAlchemy models
│   │   ├── services/      # Business logic
│   │   └── main.py
│   ├── tests/
│   ├── Dockerfile
│   └── requirements.txt
│
├── cdc-workers/           # CDC worker service
│   ├── cdc_worker/
│   │   ├── core/          # Base framework
│   │   ├── checkpoint/    # Checkpoint management
│   │   ├── publisher/     # Redis publisher
│   │   └── main.py
│   ├── connectors/
│   │   ├── base.py        # BaseConnector interface
│   │   ├── mysql/
│   │   ├── postgres/
│   │   └── mongodb/
│   ├── tests/
│   ├── Dockerfile.unified
│   ├── Dockerfile.base
│   └── requirements.txt
│
├── spark-consumer/        # Spark CDC consumer
│   ├── jobs/
│   │   ├── cdc_consumer.py
│   │   └── transformations.py
│   ├── lib/
│   │   ├── transform_engine/
│   │   ├── dq_engine/
│   │   └── writers/
│   ├── Dockerfile
│   └── requirements.txt
│
├── connectors/            # Connector Docker contexts
│   ├── mysql/
│   │   └── Dockerfile
│   ├── postgres/
│   │   └── Dockerfile
│   └── mongodb/
│       └── Dockerfile
│
├── frontend/              # React UI
│   ├── src/
│   │   ├── pages/
│   │   ├── components/
│   │   └── services/
│   ├── package.json
│   └── Dockerfile
│
├── infra/                 # Infrastructure as Code
│   ├── kubernetes/
│   │   ├── base/
│   │   └── overlays/
│   ├── docker-compose/
│   │   ├── dev.yml
│   │   └── prod.yml
│   ├── terraform/
│   └── helm/
│
├── docs/
├── schemas/               # (Already exists)
├── migrations/            # (Already exists)
└── README.md
```

**1.2 Development Environment Setup**

```bash
# Install dependencies
brew install docker docker-compose kubectl helm terraform

# Setup Python virtual environments
cd cdc-workers
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cd ../control-plane
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Setup Node environment
cd ../frontend
nvm install 20
npm install
```

**1.3 CI/CD Pipeline**

```yaml
# .github/workflows/ci.yml
name: CI/CD Pipeline

on: [push, pull_request]

jobs:
  test-control-plane:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: |
          cd control-plane
          pip install -r requirements.txt
          pytest tests/
  
  test-cdc-workers:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: |
          cd cdc-workers
          pip install -r requirements.txt
          pytest tests/
  
  build-images:
    needs: [test-control-plane, test-cdc-workers]
    runs-on: ubuntu-latest
    steps:
      - uses: docker/build-push-action@v4
        with:
          context: ./control-plane
          push: true
          tags: fusion/control-plane:${{ github.sha }}
      
      - uses: docker/build-push-action@v4
        with:
          context: ./cdc-workers
          file: ./cdc-workers/Dockerfile.unified
          push: true
          tags: fusion/cdc-worker:${{ github.sha }}
```

#### Week 2: Database Setup

**2.1 Apply Migrations**

```bash
# PostgreSQL setup
cd schemas
./setup_docker_databases.sh

# Apply Alembic migrations
cd ../migrations
alembic upgrade head

# Verify tables
psql -h localhost -U fusion_user -d fusion_cdc_metadata -c "\dt"
```

**2.2 Seed Data**

```bash
psql -h localhost -U fusion_user -d fusion_cdc_metadata < schemas/seed_data.sql
```

**2.3 Create Test Data**

```sql
-- Insert test bank and sub-tenant
INSERT INTO banks (id, name) VALUES ('bank-001', 'Test Bank');
INSERT INTO sub_tenants (id, bank_id, name) VALUES ('tenant-001', 'bank-001', 'Test Tenant');

-- Insert test users
INSERT INTO users (id, email, role) VALUES 
  ('user-001', 'admin@testbank.com', 'ADMIN'),
  ('user-002', 'viewer@testbank.com', 'VIEWER');
```

#### Week 3: Base Frameworks

**3.1 Control Plane API Skeleton**

```python
# control-plane/app/main.py
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from app.api import sources, destinations, connections, streams
from app.database import engine
from app.models import Base

app = FastAPI(title="Fusion CDC Control Plane")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(sources.router, prefix="/api/v1/sources", tags=["sources"])
app.include_router(destinations.router, prefix="/api/v1/destinations", tags=["destinations"])
app.include_router(connections.router, prefix="/api/v1/connections", tags=["connections"])
app.include_router(streams.router, prefix="/api/v1/streams", tags=["streams"])

@app.on_event("startup")
async def startup():
    Base.metadata.create_all(bind=engine)

@app.get("/health")
async def health_check():
    return {"status": "healthy"}
```

**3.2 CDC Worker Base Framework**

```python
# cdc-workers/cdc_worker/core/base_worker.py
from abc import ABC, abstractmethod
from typing import Iterator, Dict, Any
import logging

logger = logging.getLogger(__name__)

class BaseConnector(ABC):
    """Base class for all CDC connectors"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.source_id = config['source_id']
        self.tenant_id = config['tenant_id']
        self.bank_id = config['bank_id']
    
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to source database"""
        pass
    
    @abstractmethod
    def read_events(self) -> Iterator[Dict[str, Any]]:
        """
        Yield normalized CDC events.
        
        Event format:
        {
            'tenant_id': str,
            'bank_id': str,
            'source_id': str,
            'schema': str,
            'table': str,
            'op': str,  # 'c', 'u', 'd'
            'before': dict,
            'after': dict,
            'ts_ms': int,
            'lsn': str,
            'event_id': str
        }
        """
        pass
    
    @abstractmethod
    def checkpoint_position(self) -> Dict[str, Any]:
        """Return current position for checkpointing"""
        pass
    
    @abstractmethod
    def discover_schemas(self) -> list:
        """Discover available schemas/tables"""
        pass
    
    def normalize_event(self, raw_event: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize raw event to canonical format"""
        # Common normalization logic
        pass
```

---

### Phase 2: Core CDC Functionality (Weeks 4-8)

#### Week 4-5: MySQL Connector

**Implementation Steps:**

1. **Install Dependencies**
```bash
pip install python-mysql-replication==0.36.0
```

2. **Implement MySQL Connector**

```python
# cdc-workers/connectors/mysql/connector.py
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
from cdc_worker.core.base_worker import BaseConnector
import hashlib
import json

class MySQLConnector(BaseConnector):
    
    def connect(self):
        self.stream = BinLogStreamReader(
            connection_settings={
                'host': self.config['host'],
                'port': self.config['port'],
                'user': self.config['username'],
                'passwd': self.decrypt_password(self.config['password_encrypted'])
            },
            server_id=self.config.get('server_id', 100),
            blocking=True,
            resume_stream=True,
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
            only_schemas=[self.config['database_name']],
            only_tables=self.config.get('tables')
        )
    
    def read_events(self):
        for binlog_event in self.stream:
            for row in binlog_event.rows:
                event = {
                    'tenant_id': self.tenant_id,
                    'bank_id': self.bank_id,
                    'source_id': self.source_id,
                    'schema': binlog_event.schema,
                    'table': binlog_event.table,
                    'op': self._map_operation(binlog_event),
                    'ts_ms': binlog_event.timestamp * 1000,
                    'lsn': f"{binlog_event.packet.log_file}:{binlog_event.packet.log_pos}",
                    'event_id': self._compute_event_id(binlog_event, row)
                }
                
                if isinstance(binlog_event, WriteRowsEvent):
                    event['after'] = row['values']
                    event['before'] = None
                elif isinstance(binlog_event, UpdateRowsEvent):
                    event['before'] = row['before_values']
                    event['after'] = row['after_values']
                elif isinstance(binlog_event, DeleteRowsEvent):
                    event['before'] = row['values']
                    event['after'] = None
                
                yield event
    
    def checkpoint_position(self):
        return {
            'log_file': self.stream.log_file,
            'log_pos': self.stream.log_pos
        }
    
    def _map_operation(self, event):
        if isinstance(event, WriteRowsEvent):
            return 'c'
        elif isinstance(event, UpdateRowsEvent):
            return 'u'
        elif isinstance(event, DeleteRowsEvent):
            return 'd'
    
    def _compute_event_id(self, event, row):
        """Generate deterministic event ID for deduplication"""
        parts = [
            self.source_id,
            event.schema,
            event.table,
            str(event.timestamp),
            str(event.packet.log_pos),
            json.dumps(row, sort_keys=True)
        ]
        return hashlib.sha256('|'.join(parts).encode()).hexdigest()
```

3. **Test MySQL Connector**

```python
# cdc-workers/tests/test_mysql_connector.py
import pytest
from connectors.mysql.connector import MySQLConnector

@pytest.fixture
def mysql_config():
    return {
        'source_id': 'test-source-001',
        'tenant_id': 'tenant-001',
        'bank_id': 'bank-001',
        'host': 'localhost',
        'port': 3306,
        'database_name': 'test_db',
        'username': 'root',
        'password_encrypted': 'encrypted_password',
        'server_id': 100
    }

def test_mysql_connector_connect(mysql_config):
    connector = MySQLConnector(mysql_config)
    connector.connect()
    assert connector.stream is not None

def test_mysql_read_events(mysql_config):
    connector = MySQLConnector(mysql_config)
    connector.connect()
    
    events = []
    for event in connector.read_events():
        events.append(event)
        if len(events) >= 10:
            break
    
    assert len(events) > 0
    assert events[0]['tenant_id'] == 'tenant-001'
```

#### Week 6: PostgreSQL Connector

**Implementation similar to MySQL, using psycopg library**

```python
# cdc-workers/connectors/postgres/connector.py
import psycopg
from psycopg.replication import LogicalReplicationConnection
import json
from cdc_worker.core.base_worker import BaseConnector

class PostgreSQLConnector(BaseConnector):
    # Implementation details...
    pass
```

#### Week 7: MongoDB Connector

```python
# cdc-workers/connectors/mongodb/connector.py
from pymongo import MongoClient
from bson import json_util
from cdc_worker.core.base_worker import BaseConnector

class MongoDBConnector(BaseConnector):
    # Implementation details...
    pass
```

#### Week 8: Worker Orchestration & Redis Integration

**8.1 Redis Publisher**

```python
# cdc-workers/cdc_worker/publisher/redis_publisher.py
import redis
import json
import logging

logger = logging.getLogger(__name__)

class RedisStreamPublisher:
    
    def __init__(self, redis_url: str):
        self.client = redis.from_url(redis_url)
    
    def publish_event(self, event: dict) -> bool:
        """
        Publish event to Redis Stream
        Key format: cdc:<bank_id>:<tenant_id>:<source_id>:<schema>:<table>
        """
        stream_key = self._generate_stream_key(event)
        
        try:
            self.client.xadd(
                name=stream_key,
                fields={'event': json.dumps(event)},
                id='*',  # Auto-generate ID
                maxlen=100000,  # Keep last 100k events
                approximate=True
            )
            logger.info(f"Published event to {stream_key}")
            return True
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            return False
    
    def _generate_stream_key(self, event: dict) -> str:
        return f"cdc:{event['bank_id']}:{event['tenant_id']}:{event['source_id']}:{event['schema']}:{event['table']}"
```

**8.2 Main Worker Loop**

```python
# cdc-workers/cdc_worker/main.py
import os
import logging
from cdc_worker.core.base_worker import BaseConnector
from cdc_worker.publisher.redis_publisher import RedisStreamPublisher
from cdc_worker.checkpoint.manager import CheckpointManager
from connectors.mysql.connector import MySQLConnector
from connectors.postgres.connector import PostgreSQLConnector
from connectors.mongodb.connector import MongoDBConnector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONNECTOR_MAP = {
    'mysql': MySQLConnector,
    'postgresql': PostgreSQLConnector,
    'mongodb': MongoDBConnector
}

def main():
    # Load configuration from environment
    config = {
        'source_id': os.getenv('SOURCE_ID'),
        'tenant_id': os.getenv('TENANT_ID'),
        'bank_id': os.getenv('BANK_ID'),
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT')),
        'database_name': os.getenv('DB_NAME'),
        'username': os.getenv('DB_USER'),
        'password_encrypted': os.getenv('DB_PASSWORD'),
        'connector_type': os.getenv('CONNECTOR_TYPE', 'mysql')
    }
    
    # Initialize components
    connector_class = CONNECTOR_MAP[config['connector_type']]
    connector = connector_class(config)
    publisher = RedisStreamPublisher(os.getenv('REDIS_URL', 'redis://localhost:6379'))
    checkpoint_mgr = CheckpointManager(config['source_id'])
    
    # Connect and start reading
    connector.connect()
    logger.info(f"Connected to {config['connector_type']} source: {config['source_id']}")
    
    event_count = 0
    for event in connector.read_events():
        # Publish to Redis
        success = publisher.publish_event(event)
        
        if success:
            event_count += 1
            
            # Checkpoint every 100 events
            if event_count % 100 == 0:
                position = connector.checkpoint_position()
                checkpoint_mgr.save_checkpoint(position)
                logger.info(f"Checkpointed at position: {position}")

if __name__ == '__main__':
    main()
```

---

### Phase 3: Processing & Transformation (Weeks 9-12)

#### Week 9-10: Spark CDC Consumer

```python
# spark-consumer/jobs/cdc_consumer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def main():
    spark = SparkSession.builder \
        .appName("Fusion CDC Consumer") \
        .config("spark.redis.host", "localhost") \
        .config("spark.redis.port", "6379") \
        .getOrCreate()
    
    # Read from Redis Streams
    df = spark.readStream \
        .format("redis") \
        .option("stream.keys", "cdc:*") \
        .option("stream.read.batch.size", 100) \
        .load()
    
    # Parse event JSON
    event_schema = StructType([
        StructField("tenant_id", StringType()),
        StructField("bank_id", StringType()),
        StructField("source_id", StringType()),
        StructField("schema", StringType()),
        StructField("table", StringType()),
        StructField("op", StringType()),
        # ... more fields
    ])
    
    events = df.select(from_json(col("event"), event_schema).alias("data")).select("data.*")
    
    # Apply transformations
    # (to be implemented in Week 11)
    
    # Write to destination
    query = events.writeStream \
        .foreachBatch(write_to_destination) \
        .outputMode("append") \
        .start()
    
    query.awaitTermination()

def write_to_destination(batch_df, batch_id):
    # Write logic
    pass

if __name__ == '__main__':
    main()
```

#### Week 11: Transformation Engine

*(Implementation of transformation pipeline executor)*

#### Week 12: Destination Writers

*(PostgreSQL warehouse and Iceberg writers)*

---

### Phase 4: Control Plane & Orchestration (Weeks 13-16)

*(API completion, scheduler, monitoring)*

---

### Phase 5: Production Readiness (Weeks 17-20)

*(Testing, documentation, deployment)*

---

## 🐳 Docker Image Build Strategy

### Unified Image (MVP)

```dockerfile
# cdc-workers/Dockerfile.unified
FROM python:3.11-slim AS base

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    default-libmysqlclient-dev \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY cdc_worker/ ./cdc_worker/
COPY connectors/ ./connectors/

# Runtime configuration
ENV PYTHONUNBUFFERED=1
ENV CONNECTOR_TYPE=mysql

CMD ["python", "-m", "cdc_worker.main"]
```

### Build & Publish

```bash
# Build unified image
cd cdc-workers
docker build -f Dockerfile.unified -t fusion/cdc-worker:latest .
docker push fusion/cdc-worker:latest

# Tag versions
docker tag fusion/cdc-worker:latest fusion/cdc-worker:v0.1.0
docker push fusion/cdc-worker:v0.1.0
```

### Kubernetes Deployment

```yaml
# infra/kubernetes/base/cdc-worker-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cdc-worker-mysql
spec:
  replicas: 1
  selector:
    matchLabels:
      app: cdc-worker
      connector: mysql
  template:
    metadata:
      labels:
        app: cdc-worker
        connector: mysql
    spec:
      containers:
      - name: worker
        image: fusion/cdc-worker:latest
        env:
        - name: CONNECTOR_TYPE
          value: "mysql"
        - name: SOURCE_ID
          valueFrom:
            configMapKeyRef:
              name: cdc-config
              key: source_id
        - name: DB_HOST
          valueFrom:
            secretKeyRef:
              name: source-credentials
              key: host
        # ... more env vars
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "2000m"
```

---

## 🚀 Quick Start Commands

### Start Development Environment

```bash
# 1. Start infrastructure
docker-compose -f infra/docker-compose/dev.yml up -d

# 2. Apply migrations
cd migrations && alembic upgrade head

# 3. Start control plane
cd control-plane
source .venv/bin/activate
uvicorn app.main:app --reload

# 4. Start CDC worker (in another terminal)
cd cdc-workers
source .venv/bin/activate
export CONNECTOR_TYPE=mysql
export SOURCE_ID=test-source-001
python -m cdc_worker.main

# 5. Start frontend (in another terminal)
cd frontend
npm run dev
```

### Run Tests

```bash
# Backend tests
cd control-plane && pytest
cd cdc-workers && pytest

# Frontend tests
cd frontend && npm test

# Integration tests
pytest tests/integration/
```

---

## 📊 Success Metrics

### Phase 1 Success Criteria
- [ ] All infrastructure services running
- [ ] Database migrations applied successfully
- [ ] Base API responds to health checks
- [ ] CI/CD pipeline green

### Phase 2 Success Criteria
- [ ] MySQL connector captures INSERT/UPDATE/DELETE
- [ ] Events published to Redis streams
- [ ] Checkpointing working (local + central)
- [ ] Multi-tenant event routing verified

### Phase 3 Success Criteria
- [ ] Spark consumer reads from Redis
- [ ] Transformations applied correctly
- [ ] Data written to PostgreSQL warehouse
- [ ] End-to-end data flow working

### Phase 4 Success Criteria
- [ ] Full CRUD API for all entities
- [ ] UI can create connections end-to-end
- [ ] Monitoring dashboards showing metrics
- [ ] Scheduled syncs working via Airflow

### Phase 5 Success Criteria
- [ ] All integration tests passing
- [ ] Load testing completed (10K events/sec)
- [ ] Documentation complete
- [ ] Production deployment successful

---

## 🔄 Iterative Development Approach

1. **Start Simple**: Begin with MySQL connector only, single tenant, no transformations
2. **Add Complexity Gradually**: Add PostgreSQL, then MongoDB, then transformations
3. **Test Continuously**: Write tests for each component before moving to next
4. **Deploy Early**: Deploy to development Kubernetes cluster by Week 8
5. **Get Feedback**: Demo to stakeholders every 2 weeks

---

## 📚 Reference Documentation

- [Architecture Design](./README.md)
- [Database Schema](./schemas/DATABASE_SETUP.md)
- [Integration Testing](./docs/INTEGRATION_TESTING_PLAN.md)
- [User Management](./user-management/README.md)

---

## 👥 Team Structure Recommendation

- **Backend Lead**: Control plane API, orchestration
- **CDC Engineer**: Connectors, workers, checkpointing
- **Data Engineer**: Spark consumer, transformations, destinations
- **Frontend Engineer**: React UI, dashboards
- **DevOps Engineer**: Kubernetes, monitoring, CI/CD
- **QA Engineer**: Integration testing, load testing

---

**Next Steps:**
1. Review and approve this plan
2. Setup development environment
3. Begin Phase 1: Foundation
4. Schedule daily standups and weekly demos
