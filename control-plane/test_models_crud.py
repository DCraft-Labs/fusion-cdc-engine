"""
Test basic CRUD operations with SQLAlchemy models
Validates model creation, relationships, and database operations
"""
import asyncio
from uuid import uuid4
from datetime import datetime
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

# Import models
from app.models import (
    ConnectorDefinition,
    Source,
    Destination,
    Connection,
    Stream,
)

# Database URL
DATABASE_URL = "postgresql+asyncpg://fusion_user:fusion_pass@localhost:5432/fusion_cdc_metadata"


async def test_crud_operations():
    """Test basic CRUD operations"""
    
    # Create async engine and session
    engine = create_async_engine(DATABASE_URL, echo=False)
    async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    async with async_session() as session:
        print("🔄 Testing CRUD operations...\n")
        
        # Test 1: Create a ConnectorDefinition
        print("1️⃣  Creating ConnectorDefinition...")
        connector = ConnectorDefinition(
            connector_name="mysql-cdc-test",
            connector_type="mysql",
            category="database",
            latest_version="1.0.0",
            supports_cdc=True,
            supports_full_refresh=True,
            supports_incremental=True,
            documentation_url="https://docs.example.com",
            created_by=uuid4(),
        )
        session.add(connector)
        await session.flush()  # Flush to get the ID
        print(f"   ✅ Created connector: {connector.connector_id}")
        
        # Test 2: Create a Source
        print("\n2️⃣  Creating Source...")
        bank_id = uuid4()
        sub_tenant_id = uuid4()
        
        source = Source(
            connector_id=connector.connector_id,
            connector_type="mysql",
            source_name="test-mysql-source",
            host="localhost",
            port=3306,
            database_name="test_db",
            username="test_user",
            password="encrypted_password",
            bank_id=bank_id,
            sub_tenant_id=sub_tenant_id,
            created_by=uuid4(),
        )
        session.add(source)
        await session.flush()
        print(f"   ✅ Created source: {source.source_id}")
        
        # Test 3: Create a Destination
        print("\n3️⃣  Creating Destination...")
        destination = Destination(
            connector_id=connector.connector_id,
            connector_type="postgresql",
            destination_name="test-postgres-destination",
            host="localhost",
            port=5432,
            database_name="target_db",
            username="target_user",
            password="encrypted_password",
            bank_id=bank_id,
            sub_tenant_id=sub_tenant_id,
            created_by=uuid4(),
        )
        session.add(destination)
        await session.flush()
        print(f"   ✅ Created destination: {destination.destination_id}")
        
        # Test 4: Create a Connection
        print("\n4️⃣  Creating Connection...")
        connection = Connection(
            source_id=source.source_id,
            destination_id=destination.destination_id,
            connection_name="test-connection",
            sync_mode="cdc",
            sync_frequency="continuous",
            status="active",
            bank_id=bank_id,
            sub_tenant_id=sub_tenant_id,
            created_by=uuid4(),
        )
        session.add(connection)
        await session.flush()
        print(f"   ✅ Created connection: {connection.connection_id}")
        
        # Test 5: Create a Stream
        print("\n5️⃣  Creating Stream...")
        stream = Stream(
            connection_id=connection.connection_id,
            source_table_name="users",
            destination_table_name="users_replica",
            is_enabled=True,
            sync_mode="cdc",
            cursor_field="updated_at",
            primary_keys=["id"],
        )
        session.add(stream)
        await session.flush()
        print(f"   ✅ Created stream: {stream.stream_id}")
        
        # Test 6: Query with relationships
        print("\n6️⃣  Testing relationships...")
        
        # Query connection with joined relationships
        stmt = select(Connection).where(Connection.connection_id == connection.connection_id)
        result = await session.execute(stmt)
        conn = result.scalar_one()
        
        # Access relationships
        await session.refresh(conn, ["source", "destination", "streams"])
        print(f"   ✅ Connection has source: {conn.source.source_name}")
        print(f"   ✅ Connection has destination: {conn.destination.destination_name}")
        print(f"   ✅ Connection has {len(conn.streams)} stream(s)")
        
        # Test 7: Update operation
        print("\n7️⃣  Testing update...")
        stream.is_enabled = False
        await session.flush()
        print(f"   ✅ Updated stream is_enabled to False")
        
        # Test 8: Query by tenant
        print("\n8️⃣  Testing multi-tenancy filter...")
        stmt = select(Source).where(Source.sub_tenant_id == sub_tenant_id)
        result = await session.execute(stmt)
        sources = result.scalars().all()
        print(f"   ✅ Found {len(sources)} source(s) for tenant {sub_tenant_id}")
        
        # Cleanup: Rollback to avoid polluting database
        await session.rollback()
        print("\n9️⃣  Rolled back test data (cleanup)")
        
    await engine.dispose()
    
    print("\n✅ All CRUD tests passed!\n")


if __name__ == "__main__":
    asyncio.run(test_crud_operations())
