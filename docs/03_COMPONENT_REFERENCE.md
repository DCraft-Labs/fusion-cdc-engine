# Fusion CDC Engine — Component Reference

> **Audience:** Engineers implementing, extending, or debugging individual components. This is the detailed specification for each module, class, and API endpoint.

---

## Table of Contents

1. [Control Plane Components](#1-control-plane-components)
2. [CDC Worker Components](#2-cdc-worker-components)
3. [Connector Specifications](#3-connector-specifications)
4. [REST API Reference](#4-rest-api-reference)
5. [Internal API (Worker-Facing)](#5-internal-api-worker-facing)
6. [Data Models Reference](#6-data-models-reference)
7. [Frontend Component Map](#7-frontend-component-map)
8. [Configuration Reference](#8-configuration-reference)
9. [Metrics Catalogue](#9-metrics-catalogue)
10. [Alert Rules Reference](#10-alert-rules-reference)

---

## 1. Control Plane Components

### 1.1 FastAPI Application (`app/main.py`)

The entry point for the control plane. Responsibilities:

- Register all API routers with prefix `/api/v1`
- Mount GraphQL at `/graphql`
- Apply middleware (CORS, Auth, TenantIsolation)
- Expose `/metrics` Prometheus endpoint
- Start background tasks on lifespan startup

**Middleware stack (outermost first):**
```
CORSMiddleware         → allow frontend origins (localhost:3000, localhost:5173)
AuthMiddleware         → JWT validation, extract user/tenant context
TenantIsolationMiddleware → inject sub_tenant_id into DB session context
```

### 1.2 Auth Middleware (`app/middleware/auth.py`)

Validates every request:
```python
# Bearer token flow (users):
Authorization: Bearer <JWT>
  → jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
  → extract: sub (user_id), tenant_id, bank_id, roles

# Worker token flow (CDC worker):
X-Worker-Token: dev-worker-token
  → compare against settings.WORKER_TOKEN
  → set request.state.is_worker = True

# Public paths (no auth required):
  /api/v1/auth/login
  /health
  /metrics
  /docs
  /openapi.json
```

### 1.3 Tenant Isolation Middleware (`app/middleware/tenant_isolation.py`)

After auth succeeds, this middleware:
- Injects `request.state.tenant_id` into the SQLAlchemy session context
- All ORM queries that go through tenant-aware base classes automatically add `WHERE sub_tenant_id = :tenant_id`
- Prevents cross-tenant data leakage at the DB query level

### 1.4 Database Session (`app/database.py`)

```python
# Session factory
def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# All API endpoints use:
db: Session = Depends(get_db)
```

Connection: `DATABASE_URL=postgresql://fusion_user:fusion_password@localhost:5432/fusion_cdc_metadata`

### 1.5 Settings (`app/config.py`)

All configuration is loaded from environment variables with defaults:

| Setting | Env Var | Default | Purpose |
|---------|---------|---------|---------|
| `DATABASE_URL` | `DATABASE_URL` | — | PostgreSQL connection string |
| `REDIS_URL` | `REDIS_URL` | `redis://localhost:6379/0` | Redis connection |
| `JWT_SECRET_KEY` | `JWT_SECRET_KEY` | — | HMAC key for JWT signing |
| `JWT_ALGORITHM` | `JWT_ALGORITHM` | `HS256` | JWT signature algorithm |
| `JWT_EXPIRATION_MINUTES` | `JWT_EXPIRATION_MINUTES` | `30` | Token lifetime |
| `ENCRYPTION_KEY` | `ENCRYPTION_KEY` | — | AES-256 key for credential encryption |
| `LOG_LEVEL` | `LOG_LEVEL` | `INFO` | Logging verbosity |
| `WORKER_TOKEN` | `WORKER_TOKEN` | `dev-worker-token` | Shared secret for worker auth |
| `CDC_WORKER_URL` | `CDC_WORKER_URL` | `http://localhost:8081` | Worker HTTP URL |
| `SCHEMA_REINTROSPECT_INTERVAL_HOURS` | — | `24` | Schema check frequency |

---

## 2. CDC Worker Components

### 2.1 Worker (`cdc_worker/worker.py`)

**Class:** `Worker`

The main orchestrator. Starts one asyncio coroutine per assigned source, plus background loops.

| Method | Purpose |
|--------|---------|
| `run()` | Start worker, block until stop() |
| `stop()` | Cancel all tasks, graceful shutdown |
| `_run_source(source)` | Exception-isolated wrapper for one source |
| `_stream_source(source_id, connector)` | Iterate connector events → publish → checkpoint |
| `_checkpoint_sync_loop()` | Every 60s push checkpoints to central Postgres |
| `_fallback_drain_loop()` | Every 30s retry queued events to Redis |
| `_command_listener()` | Redis pub/sub listener for control plane commands |
| `_start_http_server()` | aiohttp server on :8081 |
| `_fetch_sources()` | GET /api/v1/internal/workers/{id}/sources |
| `_build_connector(source)` | Instantiate MySQLConnector/PostgresConnector/etc |

**Key configuration:**
```python
MAX_CONCURRENT_TABLES = 20  # asyncio.Semaphore value
CHECKPOINT_SYNC_INTERVAL = 60  # seconds
FALLBACK_DRAIN_INTERVAL = 30   # seconds
HEARTBEAT_INTERVAL = 30        # seconds
```

**HTTP Server routes:**
```
GET  /health               → {"status":"healthy","worker_id":"...","active_sources":[...]}
POST /internal/start-streaming  → {"connection_id":"..."} → start streaming source
```

**Redis command listener:**
```
Channel: "fusion:commands"
Messages: {"action": "start-streaming", "connection_id": "..."}
          {"action": "stop-streaming",  "connection_id": "..."}
```

### 2.2 CDCEvent Envelope (`cdc_worker/event_envelope.py`)

**Function:** `compute_event_id(pk_values, lsn) → str`
- Returns `SHA256(json.dumps({"pk": pk_values, "lsn": lsn}, sort_keys=True))`
- 64 lowercase hex characters
- Deterministic — same inputs always yield the same ID

**Class:** `CDCEvent`
- Immutable dataclass
- `__post_init__` validation: op must be c/u/d, insert must have before=None, delete must have after=None
- `to_redis_dict()` — serialise all fields to `Dict[str, str]` for XADD

### 2.3 Redis Publisher (`cdc_worker/redis_publisher.py`)

**Class:** `RedisStreamPublisher`

| Method | Purpose |
|--------|---------|
| `publish(event, routing)` | XADD to all routing-target stream keys |
| `_ensure_consumer_group(key)` | XGROUP CREATE if stream is new |
| `_publish_one(key, fields)` | XADD with MAXLEN ~ maxlen |

Stream key formula:
```python
f"cdc:{bank_id}:{tenant_id}:{source_id}:{schema_name}:{table_name}"
```

On `redis.exceptions.RedisError` → calls `self._fallback.enqueue(event)` and returns `False`.

### 2.4 Checkpoint Manager (`cdc_worker/checkpoint.py`)

**Class:** `LocalCheckpointManager`

SQLite-backed, thread-safe checkpoint store.

| Method | Purpose |
|--------|---------|
| `set(source_id, schema, table, lsn)` | Update in-memory + periodic SQLite flush |
| `get(source_id, schema, table) → str` | Read from SQLite |
| `all_for_source(source_id) → list` | Get all checkpoints for a source |
| `flush()` | Force SQLite write |

SQLite schema:
```sql
CREATE TABLE checkpoints (
    source_id   TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    lsn         TEXT NOT NULL,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_id, schema_name, table_name)
);
```

**Class:** `CentralCheckpointSync`

| Method | Purpose |
|--------|---------|
| `push(local_ckpt, source_id)` | POST /api/v1/internal/checkpoints/batch |

Payload format:
```json
{
  "worker_id": "worker-dev-1",
  "source_id": "uuid",
  "checkpoints": [{"schema_name":"...", "table_name":"...", "lsn":"..."}]
}
```

### 2.5 Routing Table (`cdc_worker/routing.py`)

**Class:** `RoutingTable`

| Method | Purpose |
|--------|---------|
| `fetch_and_reload()` | GET /api/v1/internal/workers/{id}/routing, update in-memory table |
| `start_auto_refresh()` | Start background task to refresh every N seconds |
| `lookup(schema, table) → list[dict]` | Return routing entries for this (schema, table) |

Lookup priority: exact → (schema, *) → (*, *) → []

### 2.6 Fallback Queue (`cdc_worker/fallback_queue.py`)

**Class:** `FallbackQueue`

SQLite-backed disk queue for events that fail to publish to Redis.

| Method | Purpose |
|--------|---------|
| `enqueue(event)` | INSERT event JSON into fallback_events table |
| `drain(publisher) → int` | Re-publish queued events, return count flushed |
| `size() → int` | Count pending (unflushed) events |

SQLite schema:
```sql
CREATE TABLE fallback_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_key  TEXT NOT NULL,
    event_json  TEXT NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    flushed     INTEGER DEFAULT 0
);
```

### 2.7 Heartbeat Sender (`cdc_worker/heartbeat.py`)

**Class:** `HeartbeatSender`

Posts to `POST /api/v1/internal/workers/{id}/heartbeat` every `HEARTBEAT_INTERVAL` seconds.

Payload: `{"worker_id": "...", "active_sources": ["src1", "src2"], "stats": {...}}`

Control plane updates `worker_heartbeats` table. Stale workers (no heartbeat for N minutes) are flagged as unhealthy.

### 2.8 Metrics (`cdc_worker/metrics.py`)

**Class:** `WorkerMetrics`

Wraps Prometheus `Counter`, `Gauge`, `Histogram` objects.

| Metric | Type | Labels |
|--------|------|--------|
| `cdc_events_total` | Counter | tenant, source, schema, table, op |
| `cdc_stream_lag_seconds` | Gauge | tenant, source, schema, table |
| `cdc_errors_total` | Counter | tenant, source, error_type |
| `cdc_checkpoint_age_seconds` | Gauge | tenant, source |
| `cdc_fallback_queue_length` | Gauge | tenant, source |

**Function:** `start_metrics_server(port)` — starts Prometheus HTTP server on given port (default 9090).

---

## 3. Connector Specifications

### 3.1 Base Connector (`connectors/base.py`)

```python
class BaseConnector(ABC):
    def __init__(self, source: dict, checkpoint_manager: LocalCheckpointManager)
    
    @abstractmethod
    async def stream_events(self) -> AsyncIterator[CDCEvent]:
        """Yield CDCEvent objects indefinitely."""
```

Source dict keys:
```python
{
    "source_id":      "uuid",
    "bank_id":        "uuid",
    "tenant_id":      "uuid",
    "source_type":    "mysql" | "postgresql" | "mongodb" | "polling",
    "host":           "hostname",
    "port":           3306,
    "database_name":  "mydb",
    "username":       "cdc_user",
    "password":       "decrypted_password",
    "assigned_tables": [
        {"schema_name": "public", "table_name": "orders"},
        {"schema_name": "public", "table_name": "customers"}
    ]
}
```

### 3.2 MySQL Connector (`connectors/mysql.py`)

**Class:** `MySQLConnector`

**Prerequisites on MySQL server:**
```sql
SET GLOBAL binlog_format = 'ROW';
SET GLOBAL binlog_row_image = 'FULL';
GRANT REPLICATION SLAVE, SELECT ON *.* TO 'cdc_user'@'%';
```

**Startup sequence:**
1. Connect via `pymysql` to `INFORMATION_SCHEMA`
2. Load all column names and primary key columns for assigned tables (stores in `_col_names`, `_pk_cols`)
3. Create `BinLogStreamReader` with `server_id` derived from `MD5(source_id) % 2^32`
4. Start from last checkpoint `(log_file, log_pos)` or current position if no checkpoint

**Event handling:**
```python
WriteRowsEvent  → op="c", before=None, after=row
UpdateRowsEvent → op="u", before=row["before"], after=row["after"]
DeleteRowsEvent → op="d", before=row, after=None
QueryEvent      → DDL: notify control plane, invalidate schema cache
```

**Heartbeat:** MySQL sends heartbeat binlog events every ~60ms when idle. These are `WARNING` log lines from `python-mysql-replication` — this is **normal** and expected behaviour (not errors).

**Column remapping:** Binlog may return `UNKNOWN_COL0, UNKNOWN_COL1, ...` for tables where metadata is not available. The connector remaps these to real column names using the INFORMATION_SCHEMA data loaded at startup.

### 3.3 PostgreSQL Connector (`connectors/postgres.py`)

**Class:** `PostgresConnector`

**Prerequisites on PostgreSQL server:**
```sql
ALTER SYSTEM SET wal_level = 'logical';  -- requires restart
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;
-- Install extension:
CREATE EXTENSION IF NOT EXISTS wal2json;
-- Grant:
ALTER USER cdc_user REPLICATION;
```

**Startup sequence:**
1. Connect with `LogicalReplicationConnection`
2. Try to create replication slot `fusion_{source_id}` (OK if already exists)
3. Start replication from last checkpoint LSN
4. Consume WAL messages via `cursor.consume_stream(msg_handler)`

**⚠ CRITICAL — WAL Feedback:**
```python
# Must be called at least every 10 seconds during idle periods!
msg.cursor.send_feedback(flush_lsn=msg.data_start)
```
If `send_feedback` is not called, Postgres accumulates WAL on disk indefinitely and will eventually crash the DB server.

**Replication slot management:**
- Monitor with: `SELECT * FROM pg_replication_slots WHERE active = false;`
- Drop orphaned slots: `SELECT pg_drop_replication_slot('slot_name');`

### 3.4 MongoDB Connector (`connectors/mongodb.py`)

**Class:** `MongoDBConnector`

**Prerequisites:**
- MongoDB must run as a replica set (not standalone)
- User needs `readAnyDatabase` or specific collection read permissions

**Startup sequence:**
1. Connect via `pymongo` to replica set
2. Load last resume token from checkpoint (= MongoDB `_id` of last change event)
3. Open change stream: `collection.watch(pipeline, full_document='updateLookup', resume_after=token)`

**Event type mapping:**
```python
"insert"  → op="c"
"update"  → op="u" (has full document via updateLookup)
"replace" → op="u"
"delete"  → op="d" (no after document)
```

**Error handling:**
```python
ChangeStreamHistoryLost → resume token expired (oplog window exceeded)
                        → log critical error, emit alert
                        → requires full resync of affected collections
NetworkTimeout          → retry with exponential backoff
```

### 3.5 Polling Connector (`connectors/polling.py`)

**Class:** `PollingConnector`

Fallback for managed databases that don't expose CDC logs.

**Mechanism:**
```sql
SELECT * FROM table
WHERE {cursor_column} > {last_cursor}
ORDER BY {cursor_column}
LIMIT {batch_size};
```

**Limitations vs CDC:**
- Cannot detect deletes (only detects new/updated rows via cursor column)
- Higher source database load
- Latency proportional to poll interval (default: 60 seconds)

**When to use:**
- Cloud-managed databases with disabled binlog/WAL
- Tables without a suitable timestamp/ID cursor column
- Development/testing where CDC logs are not configured

---

## 4. REST API Reference

Base path: `http://localhost:8000/api/v1`

### 4.1 Authentication (`/auth`)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| POST | `/auth/login` | None | Login with username/password, returns JWT |
| POST | `/auth/refresh` | Bearer JWT | Refresh JWT token |
| GET | `/auth/me` | Bearer JWT | Current user info |
| POST | `/auth/users` | Admin JWT | Create new user |

**Login request:**
```json
POST /api/v1/auth/login
{
  "username": "admin@fusion.dev",
  "password": "admin_password"
}
```
**Response:**
```json
{
  "access_token": "eyJ...",
  "token_type": "bearer",
  "expires_in": 1800,
  "user": {"user_id": "...", "email": "...", "roles": ["admin"]}
}
```

### 4.2 Sources (`/sources`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/sources` | List sources for tenant |
| POST | `/sources` | Create source |
| GET | `/sources/{id}` | Get source detail |
| PATCH | `/sources/{id}` | Update source |
| DELETE | `/sources/{id}` | Soft-delete source |
| POST | `/sources/{id}/test-connection` | Test DB connectivity |
| GET | `/sources/{id}/schema` | Introspect and return schema |

**Create source body:**
```json
{
  "name": "MySQL Production",
  "source_type": "mysql",
  "host": "10.0.0.1",
  "port": 3307,
  "database_name": "myapp",
  "username": "cdc_user",
  "password": "secret",
  "cdc_config": {
    "server_id": 1001,
    "binlog_format": "ROW"
  }
}
```

### 4.3 Connections (`/connections`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/connections` | List connections for tenant |
| POST | `/connections` | Create connection |
| GET | `/connections/{id}` | Get connection detail |
| PATCH | `/connections/{id}` | Update connection |
| DELETE | `/connections/{id}` | Delete connection |
| POST | `/connections/{id}/trigger-sync` | Trigger sync run |
| POST | `/connections/{id}/pause` | Pause connection |
| POST | `/connections/{id}/resume` | Resume connection |
| GET | `/connections/{id}/runs` | List sync run history (latest 100) |
| GET | `/connections/{id}/health` | CDC health (lag, throughput, checkpoint) |
| GET | `/connections/{id}/stats` | Aggregated statistics |

**Trigger-sync response:**
```json
{
  "connection_id": "79b50f04-...",
  "sync_triggered": true,
  "message": "Manual sync triggered successfully via streaming worker",
  "triggered_at": "2026-05-05T10:30:00Z"
}
```

**Run history item:**
```json
{
  "id": "run_uuid",
  "run_number": 5,
  "trigger_type": "manual",
  "status": "completed",
  "records_synced": 0,
  "duration": 3,
  "started_at": "2026-05-05T10:30:00Z",
  "completed_at": "2026-05-05T10:30:03Z",
  "run_config": {
    "sync_mode": "cdc",
    "sync_type": "REALTIME",
    "orchestration": "streaming",
    "snapshot": {
      "worker_id": "worker-dev-1",
      "worker_status": "healthy",
      "is_source_active": true,
      "binlog_position": "mysql-bin.000003:10708",
      "checkpoint_at": "2026-05-05T10:25:00Z",
      "tables": ["fusion_cdc_metadata.customers"],
      "redis_event_counts": {"fusion_cdc_metadata.customers": 4},
      "total_events": 4
    }
  }
}
```

### 4.4 Streams (`/streams`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/streams/connections/{id}/streams` | List streams for connection |
| POST | `/streams/connections/{id}/streams` | Add stream (table) to connection |
| PATCH | `/streams/connections/{id}/streams/{stream_id}` | Update stream config |
| DELETE | `/streams/connections/{id}/streams/{stream_id}` | Remove stream |

**Stream object:**
```json
{
  "stream_id": "uuid",
  "connection_id": "uuid",
  "schema_name": "fusion_cdc_metadata",
  "table_name": "customers",
  "sync_mode": "cdc",
  "enabled": true,
  "pk_columns": ["id"],
  "cursor_column": "updated_at",
  "sync_mode_override": null
}
```

### 4.5 Monitoring (`/monitoring`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/monitoring/connections/{id}/lag` | Replication lag history |
| GET | `/monitoring/connections/{id}/throughput` | Events/sec history |
| GET | `/monitoring/connections/{id}/errors` | Recent error events |
| GET | `/monitoring/worker/{id}/health` | Worker health details |

### 4.6 Alerting (`/alerting`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/alerting/rules` | List alert rules |
| POST | `/alerting/rules` | Create alert rule |
| GET | `/alerting/channels` | List notification channels |
| POST | `/alerting/channels` | Create notification channel (webhook/Slack) |

---

## 5. Internal API (Worker-Facing)

Base path: `/api/v1/internal`  
Auth: `X-Worker-Token: {WORKER_TOKEN}` header (no JWT required)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/workers/{id}/sources` | Sources assigned to this worker |
| GET | `/workers/{id}/routing` | Routing table: (schema, table) → tenant keys |
| POST | `/workers/{id}/heartbeat` | Update heartbeat timestamp |
| POST | `/checkpoints/batch` | Batch upsert checkpoint state |
| GET | `/checkpoints/{source_id}` | Get all checkpoints for source |

**Routing table response:**
```json
[
  {
    "schema_name": "*",
    "table_name": "*",
    "bank_id": "00000000-0000-4000-a000-000000000001",
    "tenant_id": "00000000-0000-4000-a000-000000000002",
    "source_id": "1dda26ae-67ff-46b7-9134-94685055bc30"
  }
]
```

**Checkpoint batch upsert request:**
```json
POST /api/v1/internal/checkpoints/batch
{
  "worker_id": "worker-dev-1",
  "source_id": "1dda26ae-67ff-46b7-9134-94685055bc30",
  "checkpoints": [
    {
      "schema_name": "fusion_cdc_metadata",
      "table_name": "customers",
      "lsn": "mysql-bin.000003:10708"
    }
  ]
}
```
**Response:** `{"upserted": 1}`

---

## 6. Data Models Reference

### 6.1 Connection Model

```python
class Connection(Base):
    connection_id:              UUID (PK)
    name:                       str
    source_id:                  UUID → sources
    destination_id:             UUID → destinations
    sub_tenant_id:              UUID → tenants (tenant isolation)
    bank_id:                    UUID
    sync_mode:                  str  # "cdc", "full_refresh", "incremental"
    sync_type:                  str  # "REALTIME", "SCHEDULED", "BATCH"
    schedule:                   str  # cron expression (if SCHEDULED)
    status:                     str  # "active", "paused", "error", "draft"
    transform_pipeline_id:      UUID → transform_pipelines
    dq_policy_id:               UUID → dq_policies
    schema_evolution_policy:    str  # "auto_apply", "manual_approval"
    initial_load_completed:     bool
    initial_load_started_at:    datetime
    initial_load_completed_at:  datetime
    created_at:                 datetime
    updated_at:                 datetime
```

### 6.2 ConnectionRun Model

```python
class ConnectionRun(Base):
    run_id:           UUID (PK)
    connection_id:    UUID → connections
    run_number:       int  # sequential counter per connection
    trigger_type:     str  # "manual", "scheduled", "api"
    triggered_by:     UUID → users
    status:           str  # "running", "completed", "failed", "cancelled"
    started_at:       datetime
    completed_at:     datetime
    records_read:     int
    records_written:  int
    bytes_processed:  Numeric
    error_message:    str
    error_stack_trace:str
    run_config:       JSONB  # includes snapshot for CDC runs
    created_at:       datetime
```

### 6.3 CheckpointState Model

```python
class CheckpointState(Base):
    checkpoint_id:     UUID (PK)
    source_id:         UUID → sources
    worker_id:         str
    lsn:               str   # "mysql-bin.000003:10708" | "16/CAFE1234" | resume_token
    checkpoint_data:   JSONB # {schema_name, table_name, lsn}
    checkpoint_at:     datetime
    records_processed: int
    bytes_processed:   Numeric
    created_at:        datetime
    updated_at:        datetime
```

### 6.4 Stream Model

```python
class Stream(Base):
    stream_id:        UUID (PK)
    connection_id:    UUID → connections
    schema_name:      str
    table_name:       str
    sync_mode:        str   # "cdc", "full_refresh", "incremental"
    enabled:          bool
    pk_columns:       JSONB # ["id"]
    cursor_column:    str   # for incremental/polling
    sync_mode_override: str
    created_at:       datetime
```

### 6.5 Source Model

```python
class Source(Base):
    source_id:      UUID (PK)
    name:           str
    source_type:    str   # "mysql", "postgresql", "mongodb"
    bank_id:        UUID
    sub_tenant_id:  UUID
    host:           str
    port:           int
    database_name:  str
    username:       str
    password_encrypted: bytes  # AES-256 encrypted
    cdc_config:     JSONB  # server_id, binlog_format, etc.
    config:         JSONB  # routing_tables, assigned_tables
    status:         str   # "active", "draft", "inactive"
    discovery_cache:JSONB  # last introspected schema
    is_deleted:     bool
    created_at:     datetime
```

---

## 7. Frontend Component Map

```
frontend/src/
  pages/
    auth/
      LoginPage.tsx           ← JWT login form
    connections/
      ConnectionsPage.tsx     ← List all connections with status badges
      ConnectionDetailPage.tsx← Connection detail: streams, runs, health, charts
      CreateConnectionPage.tsx← Multi-step connection wizard
    sources/
      SourcesPage.tsx         ← Source list with test-connection
    destinations/
      DestinationsPage.tsx    ← Destination list
    transformations/
      TransformDetailPage.tsx ← Transform pipeline editor
    data-quality/
      DQPolicyDetailPage.tsx  ← DQ rule editor
    alerts/
      AlertChannelDetailPage.tsx
    schema-evolution/
      (pending changes view)
    settings/
      AuditLogsPage.tsx
    spark-jobs/
      SparkJobDetailPage.tsx
  components/
    ui/                       ← shadcn/ui components (Button, Badge, Card, etc.)
  lib/
    api.ts                    ← axios instance, fetchList helper
```

### ConnectionDetailPage.tsx — Tabs

| Tab | Content |
|-----|---------|
| Overview | Connection metadata, source/destination info, status badge |
| Streams | Table list: schema.table, sync mode, enabled toggle, event count |
| Sync Runs | Run history table with expandable detail rows (snapshot data) |
| Health | Replication lag chart, throughput chart, checkpoint state card |
| Transformations | Attached pipeline (read-only view) |
| Data Quality | Attached DQ policy, violation history |

### Run Row Expansion (Sync Runs Tab)

Clicking a row with a `▼` chevron expands a detail panel showing the CDC snapshot:

```
▼ #5   manual   ● Completed   4 events   3s   5/5/2026 10:30

  Worker: worker-dev-1   Source active: yes   Binlog: mysql-bin.000003:10708
  Checkpoint: 5/5/2026 10:25:00
  Tables:
    [fusion_cdc_metadata.customers  4 events]
    [fusion_cdc_metadata.orders     0 events]
```

---

## 8. Configuration Reference

### 8.1 CDC Worker Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CONTROL_PLANE_URL` | `http://localhost:8000` | Control plane base URL |
| `WORKER_TOKEN` | `dev-worker-token` | Auth token for internal API |
| `WORKER_ID` | `worker-dev-1` | Unique worker identifier |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `CHECKPOINT_DB_PATH` | `/var/lib/cdc/checkpoints.db` | Local SQLite path |
| `FALLBACK_DB_PATH` | `/var/lib/cdc/fallback.db` | Fallback queue SQLite path |
| `MAX_CONCURRENT_TABLES` | `20` | asyncio.Semaphore limit |
| `HEARTBEAT_INTERVAL` | `30` | Heartbeat frequency (seconds) |
| `CHECKPOINT_SYNC_INTERVAL` | `60` | Central sync frequency (seconds) |
| `HTTP_PORT` | `8081` | Worker HTTP server port |
| `REDIS_STREAM_MAXLEN` | `100000` | Approximate stream size cap |
| `CONSUMER_GROUP` | `fusion-spark` | Redis consumer group name |
| `ENCRYPTION_KEY` | — | AES-256 key (must match control plane) |

### 8.2 Docker Compose Service Map

```yaml
# Start everything:
docker compose -f docker/docker-compose.dev.yml up -d

# Start only infrastructure (no workers):
docker compose -f docker/docker-compose.dev.yml up -d postgres-meta postgres-dest pg-source mysql-source mongo-source redis

# Start CDC worker:
docker compose -f docker/docker-compose.dev.yml up -d cdc-worker

# Rebuild and restart worker after code changes:
docker compose -f docker/docker-compose.dev.yml build cdc-worker
docker compose -f docker/docker-compose.dev.yml up -d cdc-worker

# View logs:
docker logs fusion-cdc-worker -f
docker logs fusion-postgres -f
docker logs fusion-redis -f
```

### 8.3 Alembic Migrations

```bash
# Apply all pending migrations:
cd control-plane
alembic upgrade head

# Create new migration:
alembic revision --autogenerate -m "add_my_column"

# Downgrade:
alembic downgrade -1
```

---

## 9. Metrics Catalogue

Exposed at `GET http://localhost:8000/metrics` (control plane) and `GET http://localhost:9090/metrics` (worker).

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cdc_events_total` | Counter | tenant, source, schema, table, op | Total CDC events published to Redis |
| `cdc_stream_lag_seconds` | Gauge | tenant, source, schema, table | `now() - max(event.ts_ms)` for this stream |
| `cdc_stream_throughput_per_second` | Gauge | tenant, source, schema, table | Events published per second (1-min rolling avg) |
| `cdc_errors_total` | Counter | tenant, source, error_type | Errors by type (redis_timeout, binlog_error, etc.) |
| `cdc_checkpoint_age_seconds` | Gauge | tenant, source | Seconds since last checkpoint push to central Postgres |
| `cdc_sqlite_checkpoint_size_bytes` | Gauge | tenant, source | Size of local checkpoint SQLite file |
| `cdc_fallback_queue_length` | Gauge | tenant, source | Number of events queued to fallback SQLite |
| `dq_rule_status` | Gauge | tenant, connection, rule_type | 1=PASS, 0=WARN, -1=FAIL |
| `dq_rule_violation_total` | Counter | tenant, connection, rule_type | Cumulative DQ violations |
| `dq_freshness_seconds` | Gauge | tenant, connection | Max timestamp column age |
| `api_request_duration_seconds` | Histogram | method, endpoint | Control plane API latency |
| `api_request_count` | Counter | method, endpoint, status | API request volume |

---

## 10. Alert Rules Reference

Defined in `monitoring/alerts/cdc_alerts.yaml`:

| Alert Name | Condition | Severity | For Duration |
|-----------|-----------|----------|-------------|
| `CDCStreamHighLag` | `cdc_stream_lag_seconds > 300` | warning | 2m |
| `CDCErrorSpike` | `rate(cdc_errors_total[5m]) > 10` | critical | 1m |
| `DQRuleFail` | `dq_rule_status == -1` | critical | immediately |
| `CDCCheckpointStale` | `cdc_checkpoint_age_seconds > 600` | warning | 5m |
| `CDCWorkerDown` | Worker heartbeat missing | critical | 2m |
| `CDCFallbackQueueHigh` | `cdc_fallback_queue_length > 10000` | warning | 5m |
| `CDCRedisMemoryHigh` | Redis `used_memory > maxmemory * 0.7` | warning | 2m |
| `CDCPGSlotOrphan` | `pg_replication_slots.active = false > 60m` | critical | immediately |

**Notification channels:** Webhook, Slack, PagerDuty (configurable per alert rule).
