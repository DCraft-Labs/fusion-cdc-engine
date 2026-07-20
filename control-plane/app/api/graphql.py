"""
Spec §1 (P1-5): GraphQL API for Fusion CDC Engine.

Provides a read/write GraphQL interface mirroring the key REST resources:
  Queries:  sources, destinations, connections, streams, dq_policies
  Mutations: createConnection, updateConnection, deleteConnection,
             createSource, createDestination

Mounted at /graphql in main.py:
    from app.api.graphql import graphql_app
    app.include_router(graphql_app, prefix="/graphql")

Requires: strawberry-graphql[fastapi] (see requirements.txt)
Falls back gracefully with a 503 router if strawberry is not installed.
"""
from __future__ import annotations

import logging
from typing import List, Optional
from uuid import UUID

from fastapi import Depends
from app.auth.dependencies import get_current_user

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Graceful fallback if strawberry is not installed
# ---------------------------------------------------------------------------
try:
    import strawberry
    from strawberry.fastapi import GraphQLRouter
    _STRAWBERRY_AVAILABLE = True
except ImportError:
    _STRAWBERRY_AVAILABLE = False
    log.warning(
        "strawberry-graphql not installed — GraphQL endpoint disabled. "
        "Run: pip install 'strawberry-graphql[fastapi]'"
    )

if not _STRAWBERRY_AVAILABLE:
    from fastapi import APIRouter
    graphql_app = APIRouter()
    graphql_rest_router = APIRouter()

    @graphql_app.get("/")
    @graphql_app.post("/")
    async def _graphql_unavailable():
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=503,
            content={"error": "GraphQL requires strawberry-graphql[fastapi]. Install it and restart."},
        )

    @graphql_rest_router.post("/graphql")
    async def _graphql_rest_unavailable():
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=503,
            content={"errors": [{"message": "GraphQL requires strawberry-graphql[fastapi]"}]},
        )
else:
    # ------------------------------------------------------------------
    # Types
    # ------------------------------------------------------------------
    @strawberry.type
    class SourceType:
        source_id: strawberry.ID
        source_name: str
        connector_type: Optional[str]
        status: Optional[str]
        description: Optional[str]

    @strawberry.type
    class DestinationType:
        destination_id: strawberry.ID
        destination_name: str
        connector_type: Optional[str]
        status: Optional[str]

    @strawberry.type
    class ConnectionType:
        connection_id: strawberry.ID
        connection_name: str
        source_id: Optional[strawberry.ID]
        destination_id: Optional[strawberry.ID]
        status: Optional[str]
        sync_mode: Optional[str]
        sync_type: Optional[str]

    @strawberry.type
    class StreamType:
        stream_id: strawberry.ID
        stream_name: str
        source_id: Optional[strawberry.ID]
        connection_id: Optional[strawberry.ID]
        status: Optional[str]
        sync_mode: Optional[str]

    @strawberry.type
    class DQPolicyType:
        policy_id: strawberry.ID
        policy_name: str
        connection_id: Optional[strawberry.ID]
        is_active: bool

    # ------------------------------------------------------------------
    # Input types for mutations
    # ------------------------------------------------------------------
    @strawberry.input
    class CreateConnectionInput:
        connection_name: str
        source_id: strawberry.ID
        destination_id: strawberry.ID
        sync_mode: str = "incremental"
        sync_type: str = "REALTIME"
        description: Optional[str] = None

    @strawberry.input
    class UpdateConnectionInput:
        connection_id: strawberry.ID
        connection_name: Optional[str] = None
        status: Optional[str] = None
        sync_mode: Optional[str] = None

    # ------------------------------------------------------------------
    # Helpers — convert SQLAlchemy model → GraphQL type
    # ------------------------------------------------------------------
    def _source_from_model(m) -> SourceType:
        ctype = m.connector_definition.connector_type if m.connector_definition else None
        return SourceType(
            source_id=str(m.source_id),
            source_name=m.source_name,
            connector_type=ctype,
            status=m.status,
            description=getattr(m, "description", None),
        )

    def _dest_from_model(m) -> DestinationType:
        ctype = m.connector_definition.connector_type if m.connector_definition else None
        return DestinationType(
            destination_id=str(m.destination_id),
            destination_name=m.destination_name,
            connector_type=ctype,
            status=getattr(m, "status", None),
        )

    def _conn_from_model(m) -> ConnectionType:
        return ConnectionType(
            connection_id=str(m.connection_id),
            connection_name=m.connection_name,
            source_id=str(m.source_id) if m.source_id else None,
            destination_id=str(m.destination_id) if m.destination_id else None,
            status=m.status,
            sync_mode=m.sync_mode,
            sync_type=getattr(m, "sync_type", None),
        )

    def _stream_from_model(m) -> StreamType:
        return StreamType(
            stream_id=str(m.stream_id),
            stream_name=m.stream_name,
            source_id=str(m.source_id) if getattr(m, "source_id", None) else None,
            connection_id=str(m.connection_id) if getattr(m, "connection_id", None) else None,
            status=getattr(m, "status", None),
            sync_mode=getattr(m, "sync_mode", None),
        )

    def _dqp_from_model(m) -> DQPolicyType:
        return DQPolicyType(
            policy_id=str(m.policy_id),
            policy_name=m.policy_name,
            connection_id=str(m.connection_id) if getattr(m, "connection_id", None) else None,
            is_active=bool(getattr(m, "is_active", True)),
        )

    # ------------------------------------------------------------------
    # Context helper — get DB session from request
    # ------------------------------------------------------------------
    def _db(info):
        return info.context["db"]

    def _user(info):
        return info.context.get("user")

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------
    @strawberry.type
    class Query:
        @strawberry.field
        def sources(self, info, limit: int = 50, offset: int = 0) -> List[SourceType]:
            from app.models.source_destination import Source
            from sqlalchemy.orm import joinedload
            db = _db(info)
            rows = (
                db.query(Source)
                .options(joinedload(Source.connector_definition))
                .filter(Source.is_deleted == False)
                .limit(limit).offset(offset).all()
            )
            return [_source_from_model(r) for r in rows]

        @strawberry.field
        def source(self, info, source_id: strawberry.ID) -> Optional[SourceType]:
            from app.models.source_destination import Source
            from sqlalchemy.orm import joinedload
            db = _db(info)
            m = db.query(Source).options(joinedload(Source.connector_definition)).filter(
                Source.source_id == UUID(str(source_id)), Source.is_deleted == False
            ).first()
            return _source_from_model(m) if m else None

        @strawberry.field
        def destinations(self, info, limit: int = 50, offset: int = 0) -> List[DestinationType]:
            from app.models.source_destination import Destination
            from sqlalchemy.orm import joinedload
            db = _db(info)
            rows = (
                db.query(Destination)
                .options(joinedload(Destination.connector_definition))
                .filter(Destination.is_deleted == False)
                .limit(limit).offset(offset).all()
            )
            return [_dest_from_model(r) for r in rows]

        @strawberry.field
        def connections(self, info, limit: int = 50, offset: int = 0) -> List[ConnectionType]:
            from app.models.connection import Connection
            db = _db(info)
            rows = (
                db.query(Connection)
                .filter(Connection.is_deleted == False)
                .limit(limit).offset(offset).all()
            )
            return [_conn_from_model(r) for r in rows]

        @strawberry.field
        def connection(self, info, connection_id: strawberry.ID) -> Optional[ConnectionType]:
            from app.models.connection import Connection
            db = _db(info)
            m = db.query(Connection).filter(
                Connection.connection_id == UUID(str(connection_id)), Connection.is_deleted == False
            ).first()
            return _conn_from_model(m) if m else None

        @strawberry.field
        def streams(self, info, connection_id: Optional[strawberry.ID] = None, limit: int = 50) -> List[StreamType]:
            from app.models.stream import Stream
            db = _db(info)
            q = db.query(Stream).filter(Stream.is_deleted == False)
            if connection_id:
                q = q.filter(Stream.connection_id == UUID(str(connection_id)))
            return [_stream_from_model(m) for m in q.limit(limit).all()]

        @strawberry.field
        def dq_policies(self, info, connection_id: Optional[strawberry.ID] = None, limit: int = 50) -> List[DQPolicyType]:
            from app.models.dq import DQPolicy
            db = _db(info)
            q = db.query(DQPolicy)
            if connection_id:
                q = q.filter(DQPolicy.connection_id == UUID(str(connection_id)))
            return [_dqp_from_model(m) for m in q.limit(limit).all()]

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------
    @strawberry.type
    class Mutation:
        @strawberry.mutation
        def create_connection(self, info, input: CreateConnectionInput) -> ConnectionType:
            from app.models.connection import Connection
            import uuid as _uuid_mod
            db = _db(info)
            m = Connection(
                connection_id=_uuid_mod.uuid4(),
                connection_name=input.connection_name,
                source_id=UUID(str(input.source_id)),
                destination_id=UUID(str(input.destination_id)),
                sync_mode=input.sync_mode,
                sync_type=input.sync_type,
                status="draft",
            )
            db.add(m)
            db.commit()
            db.refresh(m)
            return _conn_from_model(m)

        @strawberry.mutation
        def update_connection(self, info, input: UpdateConnectionInput) -> Optional[ConnectionType]:
            from app.models.connection import Connection
            db = _db(info)
            m = db.query(Connection).filter(
                Connection.connection_id == UUID(str(input.connection_id)),
                Connection.is_deleted == False,
            ).first()
            if not m:
                return None
            if input.connection_name is not None:
                m.connection_name = input.connection_name
            if input.status is not None:
                m.status = input.status
            if input.sync_mode is not None:
                m.sync_mode = input.sync_mode
            db.commit()
            db.refresh(m)
            return _conn_from_model(m)

        @strawberry.mutation
        def delete_connection(self, info, connection_id: strawberry.ID) -> bool:
            from app.models.connection import Connection
            db = _db(info)
            m = db.query(Connection).filter(
                Connection.connection_id == UUID(str(connection_id)),
                Connection.is_deleted == False,
            ).first()
            if not m:
                return False
            m.is_deleted = True
            m.status = "deleted"
            db.commit()
            return True

    # ------------------------------------------------------------------
    # Context getter — injects DB session + authenticated user
    # ------------------------------------------------------------------
    async def _get_context(request, db=None):
        from app.database import get_db as _get_db
        # Manually obtain a session since we're outside normal FastAPI DI
        if db is None:
            gen = _get_db()
            db_session = next(gen)
        else:
            db_session = db
        return {"request": request, "db": db_session, "user": None}

    # ------------------------------------------------------------------
    # Build the Strawberry schema and FastAPI router
    # ------------------------------------------------------------------
    schema = strawberry.Schema(query=Query, mutation=Mutation)
    graphql_app = GraphQLRouter(schema, context_getter=_get_context)

    # ------------------------------------------------------------------
    # REST proxy — POST /api/v1/graphql  {"query": "..."}
    # Bypasses Strawberry's HTTP routing (which requires ?request= param)
    # and executes directly against the schema.
    # ------------------------------------------------------------------
    from fastapi import APIRouter as _APIRouter
    from fastapi.responses import JSONResponse as _JSONResponse
    from pydantic import BaseModel as _BaseModel

    class _GraphQLBody(_BaseModel):
        query: str
        variables: Optional[dict] = None
        operationName: Optional[str] = None

    graphql_rest_router = _APIRouter()

    @graphql_rest_router.post("/graphql")
    async def graphql_rest_endpoint(
        body: _GraphQLBody,
        user=Depends(get_current_user),
    ):
        """Execute a GraphQL query via REST (standard JSON body)."""
        from app.database import get_db as _get_db
        gen = _get_db()
        db_session = next(gen)
        try:
            result = await schema.execute(
                body.query,
                variable_values=body.variables or {},
                context_value={"db": db_session, "user": user},
            )
            response: dict = {}
            if result.data is not None:
                response["data"] = result.data
            if result.errors:
                response["errors"] = [
                    {"message": str(e), "locations": getattr(e, "locations", None)}
                    for e in result.errors
                ]
            return _JSONResponse(content=response)
        finally:
            db_session.close()

