"""Fusion CDC Engine - Control Plane FastAPI Application"""

import asyncio
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response

from app.config import settings
from app.middleware.auth import AuthMiddleware
from app.middleware.tenant_isolation import TenantIsolationMiddleware
from app.api import (
    auth,
    connector_definitions,
    sources,
    destinations,
    connections,
    streams,
    transformations,
    data_quality,
    alerting,
    monitoring,
    schema_evolution,
    udfs,
    internal,
)
from app.api import dlq
from app.api import settings as settings_api
from app.api.graphql import graphql_app as _graphql_router, graphql_rest_router as _graphql_rest_router

# ---------------------------------------------------------------------------
# Prometheus metrics — spec §3 requires api_request_duration_seconds and
# api_request_count; graceful fallback when prometheus_client is absent.
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

    _api_request_duration = Histogram(
        "api_request_duration_seconds",
        "Latency of control plane API calls",
        ["method", "endpoint"],
        buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
    )
    _api_request_count = Counter(
        "api_request_count",
        "Number of API requests",
        ["method", "endpoint", "status"],
    )
    _PROM_AVAILABLE = True
except ImportError:
    _PROM_AVAILABLE = False
    generate_latest = None
    CONTENT_TYPE_LATEST = "text/plain"

    class _Noop:
        def labels(self, **_):
            return self
        def observe(self, v):
            pass
        def inc(self, v=1):
            pass

    _api_request_duration = _Noop()
    _api_request_count = _Noop()


# ---------------------------------------------------------------------------
# Structured JSON logging (spec §3: tenant_id, trace_id, log_level, message)
# ---------------------------------------------------------------------------
class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_dict = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S.%03dZ"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "trace_id": getattr(record, "trace_id", None),
            "tenant_id": getattr(record, "tenant_id", None),
        }
        if record.exc_info:
            log_dict["exc"] = self.formatException(record.exc_info)
        return json.dumps(log_dict, default=str)


def _configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, settings.LOG_LEVEL, logging.INFO))


_configure_logging()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Periodic re-introspection (spec §3):
# "The engine periodically (e.g. daily) performs a light re-introspection to
# detect schema changes."
# ---------------------------------------------------------------------------

async def _periodic_reintrospection() -> None:
    """
    Background asyncio task: re-introspects all active sources every
    SCHEMA_REINTROSPECT_INTERVAL_HOURS hours (default 24), creates
    SchemaChangeEvents for any differences found.
    """
    interval_seconds = getattr(settings, "SCHEMA_REINTROSPECT_INTERVAL_HOURS", 24) * 3600
    await asyncio.sleep(60)  # Brief startup delay before first run
    while True:
        try:
            from app.database import SessionLocal
            from app.models.source_destination import Source
            from app.models.schema_evolution import SchemaChangeEvent
            from app.api.sources import (
                _discover_database_schemas,
                _decrypt_password,
            )
            from datetime import datetime, timezone

            db = SessionLocal()
            try:
                sources_all = db.query(Source).filter(
                    Source.is_deleted.is_(False),
                    Source.status == 'active'
                ).all()
                logger.info("Periodic re-introspection: checking %d sources", len(sources_all))

                for source in sources_all:
                    try:
                        creds = source.config or {}
                        password = _decrypt_password(creds.get("password_encrypted", ""))
                        new_cache = _discover_database_schemas(
                            connector_type=source.connector_type or "",
                            host=source.host or "",
                            port=source.port or 0,
                            database_name=source.database_name or "",
                            username=source.username or "",
                            password=password,
                            ssl_enabled=bool(creds.get("ssl_enabled", False)),
                            ssl_config=creds.get("ssl_config", {}),
                        )
                        old_cache = source.discovery_cache or {}
                        changes = _diff_discovery_cache(old_cache, new_cache)
                        for change in changes:
                            ev = SchemaChangeEvent(
                                source_id=source.source_id,
                                table_name=change["table_name"],
                                schema_name=change["schema_name"],
                                change_type=change["change_type"],
                                old_schema=change.get("old_schema"),
                                new_schema=change.get("new_schema", {}),
                                schema_diff=change.get("schema_diff", {}),
                                detected_at=datetime.now(timezone.utc),
                                detected_by="periodic-reintrospection",
                                is_breaking=change.get("is_breaking", False),
                                impact_assessment={},
                                status="pending",
                            )
                            db.add(ev)
                            logger.info(
                                "Schema change detected (source=%s table=%s.%s type=%s)",
                                source.source_id, change["schema_name"],
                                change["table_name"], change["change_type"],
                            )
                        if changes:
                            source.discovery_cache = new_cache
                            source.last_discovery_at = datetime.now(timezone.utc)
                            db.commit()
                    except Exception as src_exc:
                        logger.warning(
                            "Re-introspection failed for source %s: %s",
                            source.source_id, src_exc,
                        )
            finally:
                db.close()

        except Exception as exc:
            logger.error("Periodic re-introspection error: %s", exc)

        await asyncio.sleep(interval_seconds)


def _diff_discovery_cache(old_cache: dict, new_cache: dict) -> list:
    """
    Compare old vs new discovery caches and return a list of change dicts.
    Detects: new columns, removed columns, type changes, table additions/removals.
    """
    changes = []
    old_tables = {
        (s["schema_name"], t["table_name"]): t
        for s in old_cache.get("schemas", [])
        for t in s.get("tables", [])
    }
    new_tables = {
        (s["schema_name"], t["table_name"]): t
        for s in new_cache.get("schemas", [])
        for t in s.get("tables", [])
    }

    # New tables
    for key in new_tables:
        if key not in old_tables:
            changes.append({
                "schema_name": key[0], "table_name": key[1],
                "change_type": "table_added",
                "new_schema": {c["column_name"]: c["data_type"]
                               for c in new_tables[key].get("columns", [])},
                "schema_diff": {"added_table": f"{key[0]}.{key[1]}"},
                "is_breaking": False,
            })

    # Removed tables
    for key in old_tables:
        if key not in new_tables:
            changes.append({
                "schema_name": key[0], "table_name": key[1],
                "change_type": "table_removed",
                "old_schema": {c["column_name"]: c["data_type"]
                               for c in old_tables[key].get("columns", [])},
                "new_schema": {},
                "schema_diff": {"removed_table": f"{key[0]}.{key[1]}"},
                "is_breaking": True,
            })

    # Column-level changes in existing tables
    for key in old_tables:
        if key not in new_tables:
            continue
        old_cols = {c["column_name"]: c for c in old_tables[key].get("columns", [])}
        new_cols = {c["column_name"]: c for c in new_tables[key].get("columns", [])}

        for col_name, col_def in new_cols.items():
            if col_name not in old_cols:
                changes.append({
                    "schema_name": key[0], "table_name": key[1],
                    "change_type": "column_added",
                    "old_schema": {k: v["data_type"] for k, v in old_cols.items()},
                    "new_schema": {k: v["data_type"] for k, v in new_cols.items()},
                    "schema_diff": {"added": [col_name]},
                    "is_breaking": False,
                })
            elif old_cols[col_name].get("data_type") != col_def.get("data_type"):
                changes.append({
                    "schema_name": key[0], "table_name": key[1],
                    "change_type": "type_changed",
                    "old_schema": {k: v["data_type"] for k, v in old_cols.items()},
                    "new_schema": {k: v["data_type"] for k, v in new_cols.items()},
                    "schema_diff": {
                        "column": col_name,
                        "old_type": old_cols[col_name].get("data_type"),
                        "new_type": col_def.get("data_type"),
                    },
                    "is_breaking": True,
                })

        for col_name in old_cols:
            if col_name not in new_cols:
                changes.append({
                    "schema_name": key[0], "table_name": key[1],
                    "change_type": "column_removed",
                    "old_schema": {k: v["data_type"] for k, v in old_cols.items()},
                    "new_schema": {k: v["data_type"] for k, v in new_cols.items()},
                    "schema_diff": {"removed": [col_name]},
                    "is_breaking": True,
                })

    return changes


# ---------------------------------------------------------------------------
# FastAPI lifespan — start/stop background tasks
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start periodic re-introspection task and scheduler on startup; cancel on shutdown."""
    task = asyncio.create_task(_periodic_reintrospection())
    logger.info("Periodic schema re-introspection task started")

    # Spec §1 (P1-7): Start the active worker scheduler
    from app.database import SessionLocal
    from app.services.scheduler import SchedulerService
    _redis = None
    try:
        import redis.asyncio as aioredis
        redis_url = settings.REDIS_URL
        if redis_url:
            _redis = aioredis.from_url(redis_url, decode_responses=True)
    except Exception:
        pass  # Redis optional for scheduler leader-election
    scheduler = SchedulerService(session_factory=SessionLocal, redis_client=_redis)
    scheduler_task = asyncio.create_task(scheduler.run(), name="scheduler")
    logger.info("Worker scheduler task started")

    try:
        yield
    finally:
        task.cancel()
        scheduler_task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        try:
            await scheduler_task
        except asyncio.CancelledError:
            pass
        if _redis:
            await _redis.aclose()
        logger.info("Background tasks stopped")


# Create FastAPI application
app = FastAPI(
    title="Fusion CDC Engine - Control Plane",
    description="Multi-tenant Change Data Capture Platform API",
    version="0.1.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    redirect_slashes=False,
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add tenant isolation middleware (extracts tenant context from JWT)
app.add_middleware(TenantIsolationMiddleware)

# ---------------------------------------------------------------------------
# Request middleware: structured JSON logging + Prometheus metrics + trace_id
# ---------------------------------------------------------------------------
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Emit Prometheus metrics and structured JSON logs per request."""
    trace_id = request.headers.get("X-Trace-Id") or str(uuid.uuid4())
    start_time = time.time()

    # Make trace_id available to downstream log calls via request.state
    request.state.trace_id = trace_id

    response = await call_next(request)

    duration = time.time() - start_time
    # Normalise path to avoid high-cardinality labels (strip UUIDs)
    import re
    endpoint = re.sub(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        ":id",
        request.url.path,
    )
    _api_request_duration.labels(method=request.method, endpoint=endpoint).observe(duration)
    _api_request_count.labels(
        method=request.method, endpoint=endpoint, status=str(response.status_code)
    ).inc()

    # Add trace_id to response header for client correlation
    response.headers["X-Trace-Id"] = trace_id

    logger.info(
        "%s %s %s %.3fs",
        request.method,
        request.url.path,
        response.status_code,
        duration,
        extra={"trace_id": trace_id},
    )
    return response

# Include API routers
# Auth routes (no /api/v1 prefix - public endpoints)
app.include_router(
    auth.router,
    prefix="/api/v1",
    tags=["Authentication"]
)

app.include_router(
    connector_definitions.router,
    prefix="/api/v1/connector-definitions",
    tags=["Connector Definitions"]
)

app.include_router(
    sources.router,
    prefix="/api/v1/sources",
    tags=["Sources"]
)

app.include_router(
    destinations.router,
    prefix="/api/v1/destinations",
    tags=["Destinations"]
)

app.include_router(
    connections.router,
    prefix="/api/v1/connections",
    tags=["Connections"]
)

app.include_router(
    streams.router,
    prefix="/api/v1/streams",
    tags=["Streams"]
)

app.include_router(
    transformations.router,
    prefix="/api/v1/transformations",
    tags=["Transformations"]
)

app.include_router(
    data_quality.router,
    prefix="/api/v1/data-quality",
    tags=["Data Quality"]
)

app.include_router(
    alerting.router,
    prefix="/api/v1",
    tags=["Alerts"]
)

app.include_router(
    monitoring.router,
    prefix="/api/v1/monitoring",
    tags=["Monitoring"]
)

app.include_router(
    schema_evolution.router,
    prefix="/api/v1/schema-evolution",
    tags=["Schema Evolution"]
)

app.include_router(
    udfs.router,
    prefix="/api/v1/udfs",
    tags=["UDFs"]
)

app.include_router(
    dlq.router,
    prefix="/api/v1/dlq",
    tags=["Dead Letter Queue"]
)

app.include_router(
    internal.router,
    prefix="/api/v1/internal",
    tags=["Internal Worker API"],
)

app.include_router(
    settings_api.router,
    prefix="/api/v1/settings",
    tags=["Settings"],
)

# Spec §1 (P1-5): GraphQL endpoint
app.include_router(_graphql_router, prefix="/graphql", tags=["GraphQL"])
# REST proxy for programmatic GraphQL access (standard JSON body)
app.include_router(_graphql_rest_router, prefix="/api/v1", tags=["GraphQL"])

# Health check endpoints
@app.get("/health", tags=["Health"])
async def health_check():
    """Basic health check"""
    return {
        "status": "healthy",
        "service": "fusion-cdc-control-plane",
        "version": "0.1.0"
    }

@app.get("/health/ready", tags=["Health"])
async def readiness_check():
    """Kubernetes readiness probe"""
    # TODO: Check database connectivity, Redis connectivity
    return {
        "status": "ready",
        "checks": {
            "database": "connected",
            "redis": "connected"
        }
    }

@app.get("/health/live", tags=["Health"])
async def liveness_check():
    """Kubernetes liveness probe"""
    return {"status": "alive"}

# Root endpoint
@app.get("/", tags=["Root"])
async def root():
    """API root"""
    return {
        "message": "Fusion CDC Engine - Control Plane API",
        "version": "0.1.0",
        "docs": "/api/docs",
        "health": "/health"
    }

# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle all unhandled exceptions"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "INTERNAL_SERVER_ERROR",
                "message": "An unexpected error occurred",
                "details": str(exc) if settings.APP_ENV == "development" else None
            }
        }
    )

@app.get("/metrics", tags=["Monitoring"], include_in_schema=False)
async def metrics_endpoint():
    """Prometheus metrics endpoint — scraped by prometheus-operator."""
    if _PROM_AVAILABLE and generate_latest is not None:
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
    return Response(content="# prometheus_client not installed\n", media_type="text/plain")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.APP_PORT,
        reload=settings.APP_ENV == "development"
    )
