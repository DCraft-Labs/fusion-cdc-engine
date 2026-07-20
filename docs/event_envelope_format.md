# Event Envelope Format - Design Decision

## Overview

The **Event Envelope** is the canonical message format for CDC events flowing through Redis Streams. This is a critical architectural decision that affects:

- **Query-ability**: How easily can we filter/search events?
- **Payload size**: Impacts Redis memory usage and network bandwidth
- **Parsing complexity**: Affects worker performance
- **Schema evolution**: How do we handle schema changes?
- **Large payloads**: How do we handle BLOBs/TEXT columns?

---

## Option 1: Flat JSON Envelope (Airbyte-style)

### Structure
```json
{
  "event_id": "c7f8a2e1-4b3d-9f8a-7e6d-5c4b3a2f1e0d",
  "trace_id": "trace_abc123",
  "tenant_id": "tenant_001",
  "source_id": "550e8400-e29b-41d4-a716-446655440000",
  "connection_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "schema": "public",
  "table": "orders",
  "operation": "INSERT",
  "operation_ts": "2025-11-30T10:15:30.123Z",
  "captured_at": "2025-11-30T10:15:30.456Z",
  "source_metadata": {
    "log_file": "mysql-bin.000123",
    "log_pos": 45678,
    "gtid": "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
  },
  "before": {
    "id": 1001,
    "customer_id": 42,
    "order_date": "2025-11-30",
    "total": 299.99,
    "status": "pending"
  },
  "after": {
    "id": 1001,
    "customer_id": 42,
    "order_date": "2025-11-30",
    "total": 299.99,
    "status": "shipped"
  },
  "pk": ["id"],
  "schema_hash": "a3f9c8e7d6b5a4f3e2d1c0b9a8f7e6d5"
}
```

### Pros
✅ **Simple parsing** - Direct JSON deserialization  
✅ **Easy filtering** - Can query by `tenant_id`, `operation`, etc. in Redis  
✅ **Good for small tables** - Entire row fits in envelope  
✅ **Schema evolution visible** - `before` and `after` show exact changes  
✅ **Debugging friendly** - Human-readable in Redis CLI  

### Cons
❌ **Large payload size** - Entire row duplicated in `before` and `after`  
❌ **BLOB/TEXT problem** - 10MB BLOB would bloat envelope to 20MB  
❌ **Redundant data** - For UPDATE, unchanged columns duplicated  
❌ **No compression** - JSON is verbose  

### Best For
- Small tables (< 50 columns, < 1KB per row)
- High query-ability requirements
- Debugging and development environments

---

## Option 2: Compact Envelope with Delta (Debezium-style)

### Structure
```json
{
  "event_id": "c7f8a2e1-4b3d-9f8a-7e6d-5c4b3a2f1e0d",
  "trace_id": "trace_abc123",
  "tenant_id": "tenant_001",
  "source_id": "550e8400-e29b-41d4-a716-446655440000",
  "connection_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "schema": "public",
  "table": "orders",
  "operation": "UPDATE",
  "operation_ts": "2025-11-30T10:15:30.123Z",
  "captured_at": "2025-11-30T10:15:30.456Z",
  "source_metadata": {
    "log_file": "mysql-bin.000123",
    "log_pos": 45678
  },
  "pk": {"id": 1001},
  "changed_columns": ["status"],
  "before": {"status": "pending"},
  "after": {"status": "shipped"},
  "full_row": null,
  "schema_hash": "a3f9c8e7d6b5a4f3e2d1c0b9a8f7e6d5"
}
```

### Explanation
- **For INSERT**: `full_row` contains entire row, `before` is null
- **For UPDATE**: Only changed columns in `before`/`after`, `pk` identifies row
- **For DELETE**: `before` contains full row, `after` is null

### Pros
✅ **Smaller payload** - Only changed columns for UPDATEs (80% of traffic)  
✅ **Still query-able** - Can filter by `pk`, `changed_columns`  
✅ **Schema evolution efficient** - Only store deltas  
✅ **Good compression ratio** - Less redundant data  

### Cons
❌ **Reconstruction complexity** - Spark needs to merge delta with cached state  
❌ **BLOB still problematic** - Large BLOB in changed column still bloats envelope  
❌ **Requires state store** - Need to cache full row for reconstruction  

### Best For
- Medium-sized tables (50-200 columns)
- High UPDATE frequency (e.g., order status changes)
- Scenarios where most UPDATEs change < 10% of columns

---

## Option 3: Reference-based Envelope (Large Payload Support)

### Structure
```json
{
  "event_id": "c7f8a2e1-4b3d-9f8a-7e6d-5c4b3a2f1e0d",
  "trace_id": "trace_abc123",
  "tenant_id": "tenant_001",
  "source_id": "550e8400-e29b-41d4-a716-446655440000",
  "connection_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "schema": "public",
  "table": "documents",
  "operation": "INSERT",
  "operation_ts": "2025-11-30T10:15:30.123Z",
  "captured_at": "2025-11-30T10:15:30.456Z",
  "source_metadata": {
    "log_file": "mysql-bin.000123",
    "log_pos": 45678
  },
  "pk": {"doc_id": "DOC-12345"},
  "payload_type": "reference",
  "payload_ref": "s3://fusion-large-payloads/tenant_001/2025/11/30/c7f8a2e1-4b3d-9f8a-7e6d-5c4b3a2f1e0d.parquet",
  "schema_hash": "a3f9c8e7d6b5a4f3e2d1c0b9a8f7e6d5",
  "large_columns": ["pdf_content", "attachment"]
}
```

### Explanation
- **Small columns** (< 1KB each): Stored inline in `before`/`after`
- **Large columns** (> 1KB): Stored in S3/MinIO, referenced by URL
- **Spark reads large payloads** lazily from S3 when processing

### Pros
✅ **Handles BLOB/TEXT** - 10MB BLOB → 200 byte S3 reference  
✅ **Redis stays lightweight** - No memory pressure from large payloads  
✅ **Cost effective** - S3 storage cheaper than Redis memory  
✅ **Flexible threshold** - Can configure size threshold per column  

### Cons
❌ **External dependency** - Requires S3/MinIO setup  
❌ **Latency increase** - Spark must fetch S3 objects (adds 50-200ms)  
❌ **Complexity** - Worker must detect large columns and upload to S3  
❌ **Cleanup required** - Need TTL/lifecycle policy for S3 objects  

### Best For
- Tables with BLOB/TEXT columns (e.g., `documents.pdf_content`, `emails.body`)
- Large JSON columns (e.g., 5MB `payload_json`)
- Scenarios where < 5% of events contain large payloads

---

## Option 4: Hybrid Envelope (Recommended for Fusion)

### Structure
```json
{
  "event_id": "c7f8a2e1-4b3d-9f8a-7e6d-5c4b3a2f1e0d",
  "trace_id": "trace_abc123",
  "tenant_id": "tenant_001",
  "source_id": "550e8400-e29b-41d4-a716-446655440000",
  "connection_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "schema": "public",
  "table": "orders",
  "operation": "UPDATE",
  "operation_ts": "2025-11-30T10:15:30.123Z",
  "captured_at": "2025-11-30T10:15:30.456Z",
  "source_metadata": {
    "log_file": "mysql-bin.000123",
    "log_pos": 45678
  },
  "pk": {"id": 1001},
  "changed_columns": ["status", "updated_at"],
  "before": {
    "status": "pending",
    "updated_at": "2025-11-30T09:00:00Z"
  },
  "after": {
    "status": "shipped",
    "updated_at": "2025-11-30T10:15:30Z"
  },
  "full_row": null,
  "payload_type": "inline",
  "payload_ref": null,
  "large_columns_omitted": [],
  "schema_hash": "a3f9c8e7d6b5a4f3e2d1c0b9a8f7e6d5"
}
```

### Decision Rules (Worker Logic)
1. **Small UPDATE** (changed columns < 10KB total): Use **compact delta** (Option 2)
2. **Large UPDATE** (changed columns > 10KB): Use **reference** (Option 3) for large columns
3. **INSERT/DELETE** (< 10KB): Store **full row inline** (Option 1)
4. **INSERT/DELETE** (> 10KB): Use **reference** (Option 3)

### Example with Large Payload
```json
{
  "event_id": "...",
  "operation": "INSERT",
  "pk": {"doc_id": "DOC-12345"},
  "after": {
    "doc_id": "DOC-12345",
    "title": "Contract Agreement",
    "created_at": "2025-11-30T10:15:30Z",
    "pdf_content": "<LARGE_PAYLOAD_REF>",
    "metadata_json": "{\"author\": \"John Doe\"}"
  },
  "payload_type": "hybrid",
  "payload_ref": "s3://fusion-large-payloads/tenant_001/2025/11/30/c7f8a2e1.parquet",
  "large_columns_omitted": ["pdf_content"]
}
```

### Pros
✅ **Best of all worlds** - Compact for small rows, reference for large  
✅ **Automatic optimization** - Worker decides based on payload size  
✅ **Query-able** - Metadata always inline, large payloads offloaded  
✅ **Cost effective** - Only pay S3 costs when needed  

### Cons
❌ **Worker complexity** - Need size detection and S3 upload logic  
❌ **Dual storage** - Both Redis and S3  

---

## Event ID Generation Strategy

### Option A: UUID v4 (Random)
```python
event_id = str(uuid.uuid4())
# "c7f8a2e1-4b3d-9f8a-7e6d-5c4b3a2f1e0d"
```
**Pros**: Simple, globally unique  
**Cons**: No ordering, not deterministic (idempotency issues)

### Option B: Deterministic Hash (Recommended)
```python
# For exactly-once processing guarantee
key = f"{tenant_id}:{source_id}:{schema}:{table}:{pk_value}:{operation_ts}"
event_id = hashlib.sha256(key.encode()).hexdigest()[:32]
# "a3f9c8e7d6b5a4f3e2d1c0b9a8f7e6d5"
```
**Pros**: Idempotent - replaying same binlog event produces same `event_id`  
**Cons**: Requires stable `operation_ts` from source

### Option C: ULID (Sortable UUID)
```python
from ulid import ULID
event_id = str(ULID())
# "01ARZ3NDEKTSV4RRFFQ69G5FAV"
```
**Pros**: Sortable by time, compact (26 chars vs 36 for UUID)  
**Cons**: Requires additional library

**Recommendation**: **Option B (Deterministic Hash)** for exactly-once semantics.

---

## Schema Hash Strategy

Detect schema changes by hashing table structure:

```python
def compute_schema_hash(columns: List[Dict]) -> str:
    """
    columns = [
        {"name": "id", "type": "int", "nullable": false},
        {"name": "name", "type": "varchar(255)", "nullable": true},
        ...
    ]
    """
    # Sort by column name for deterministic hash
    sorted_cols = sorted(columns, key=lambda c: c['name'])
    
    schema_str = json.dumps(sorted_cols, sort_keys=True)
    return hashlib.sha256(schema_str.encode()).hexdigest()[:32]
```

**Usage**:
- Worker computes `schema_hash` when reading table metadata
- Stores in envelope
- Spark compares `schema_hash` with cached value
- If different → trigger schema evolution workflow

---

## Compression Strategy

### Option 1: No Compression (Default)
- Redis Streams supports ~1MB messages
- For small events (< 10KB), compression overhead > savings

### Option 2: zstd Compression (For Large Events)
```python
import zstandard as zstd

def compress_envelope(envelope: dict) -> bytes:
    json_bytes = json.dumps(envelope).encode('utf-8')
    if len(json_bytes) > 10240:  # 10KB threshold
        cctx = zstd.ZstdCompressor(level=3)
        return cctx.compress(json_bytes)
    return json_bytes
```

**Compression Ratio**: ~60-70% reduction for JSON with repetitive keys

**Trade-off**: CPU cost vs. network/memory savings

**Recommendation**: Enable compression for events > 10KB.

---

## Final Recommendation for Fusion

### **Hybrid Envelope (Option 4)** with:

1. **Compact delta** for small UPDATEs (80% of events)
2. **S3 reference** for large payloads (BLOB/TEXT columns)
3. **Deterministic event_id** for idempotency
4. **zstd compression** for events > 10KB
5. **schema_hash** for schema evolution detection

### Configuration (per connection)
```json
{
  "envelope_config": {
    "large_column_threshold_bytes": 10240,
    "compression_enabled": true,
    "compression_threshold_bytes": 10240,
    "s3_bucket": "fusion-large-payloads",
    "s3_ttl_days": 7,
    "event_id_strategy": "deterministic_hash"
  }
}
```

### Worker Pseudocode
```python
def build_envelope(event: CDCEvent) -> dict:
    envelope = {
        "event_id": compute_event_id(event),
        "trace_id": generate_trace_id(),
        "tenant_id": event.tenant_id,
        "source_id": event.source_id,
        "schema": event.schema,
        "table": event.table,
        "operation": event.operation,
        "operation_ts": event.operation_ts,
        "captured_at": datetime.utcnow().isoformat(),
        "source_metadata": event.source_metadata,
        "pk": event.pk,
        "schema_hash": compute_schema_hash(event.table_schema)
    }
    
    # Detect large columns
    large_columns = []
    if event.operation == "UPDATE":
        envelope["changed_columns"] = event.changed_columns
        envelope["before"] = {}
        envelope["after"] = {}
        
        for col in event.changed_columns:
            before_size = len(str(event.before[col]))
            after_size = len(str(event.after[col]))
            
            if before_size > 10240 or after_size > 10240:
                large_columns.append(col)
                # Upload to S3
                s3_key = upload_to_s3(event, col)
                envelope["before"][col] = f"<LARGE_PAYLOAD_REF:{s3_key}>"
                envelope["after"][col] = f"<LARGE_PAYLOAD_REF:{s3_key}>"
            else:
                envelope["before"][col] = event.before[col]
                envelope["after"][col] = event.after[col]
    else:
        # INSERT or DELETE - include full row
        envelope["full_row"] = event.after if event.operation == "INSERT" else event.before
        # Check for large columns
        for col, value in envelope["full_row"].items():
            if len(str(value)) > 10240:
                large_columns.append(col)
                s3_key = upload_to_s3(event, col)
                envelope["full_row"][col] = f"<LARGE_PAYLOAD_REF:{s3_key}>"
    
    envelope["large_columns_omitted"] = large_columns
    envelope["payload_type"] = "hybrid" if large_columns else "inline"
    
    return envelope
```

---

## Questions for You

1. **S3/MinIO Setup**: Do you want S3 (cloud) or MinIO (on-prem) for large payload storage? Or support both?

2. **Compression**: Enable zstd compression by default, or make it configurable per connection?

3. **Event ID Strategy**: Deterministic hash (idempotency) or ULID (sortable)?

4. **Large Column Threshold**: 10KB default? Or configurable (1KB, 10KB, 100KB)?

5. **S3 TTL**: How long to keep large payloads in S3? 7 days? 30 days?

Let me know your preferences, and I'll update the implementation accordingly!
