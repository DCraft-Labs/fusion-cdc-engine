# Iceberg / S3 Destination — Full Option Surface

This document is the canonical reference for the **DuckDB/PyIceberg lake path** in Fusion CDC.
Spark is **optional / legacy** and is not required for any of the options below.

## Architecture

```
Source CDC workers → Redis Streams → transform-worker (DuckDB + PyIceberg) → Iceberg table on S3
```

- **Engine:** DuckDB for in-process transforms + Arrow batching; PyIceberg for catalog + table commits.
- **No Spark required.** Spark profile (`cdc-spark`) remains for >1–2 TB scale-out only.
- **Image:** `transform-worker` carries `pyiceberg[s3,glue]`, `pyarrow`, `duckdb`, `boto3`.

---

## A. Catalog options (destination-level)

| Field | Values / notes | Required when |
|-------|----------------|---------------|
| `catalog_type` | `glue` \| `rest` \| `hive` \| `sql` \| `nessie` \| `dynamodb` | Always |
| `catalog_name` | Logical name in PyIceberg | Always |
| `namespace` | Iceberg namespace / Glue DB | Always |
| `warehouse` | `s3://bucket/prefix/` (accept `s3a://`, normalized to `s3://`) | Always |
| **REST** | `catalog_uri`, optional `catalog_oauth_token`, `rest_sigv4` | rest |
| **Nessie** | `nessie_uri`, `nessie_ref` (branch, default `main`) | nessie |
| **Hive** | `hive_uri` thrift://host:9083, optional `ugi` | hive |
| **Glue** | `glue_region`, `glue_endpoint`, `glue_account_id`, `glue_skip_archive` | glue |
| **SQL catalog** | `sql_catalog_uri` (sqlite file or postgres DSN) | local/dev |
| **DynamoDB** | `dynamodb_table` | rare AWS |

---

## B. Auth options (encrypted secrets)

| Mode | UI fields | Maps to PyIceberg |
|------|-----------|-------------------|
| **(a) Access key** | `aws_access_key_id`, `aws_secret_access_key`, optional `aws_session_token`, `aws_region` | `s3.*` and/or `glue.*` |
| **(b) STS assume (parent → target)** | `parent_credential_mode`, `parent_role_arn` (optional), `target_role_arn`, `external_id`, `role_session_name`, `assume_role_timeout_sec`, `sts_region` | STS AssumeRole → temp creds set on `s3.*` / `glue.*`; or `client.role-arn` direct |
| **(c) IRSA / workload identity** | `service_account_role_arn` (doc only; K8s SA annotation), empty static keys | boto default chain + web identity; `automountServiceAccountToken: true` |

- `same_creds_for_catalog_and_s3` (default `true`) → unified `client.*` / `s3.*` creds.
- When `false`, separate `s3_access_key_id` / `s3_secret_access_key` / `s3_session_token` are used for S3.

### Local MinIO recipe (mode a)

```
auth_mode=access_key
aws_access_key_id=minio
aws_secret_access_key=minio123
aws_region=us-east-1
s3_endpoint=http://minio:9000
s3_path_style=true
warehouse=s3://iceberg-warehouse/fusion-cdc/
catalog_type=nessie
nessie_uri=http://nessie:19120/api/v2
catalog_name=fusion_cdc
namespace=fusion
```

### IAM trust examples

**Mode (b) — parent role trusts target role:**

```json
// Target role trust policy (bucket/catalog role)
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "AWS": "arn:aws:iam::<acct>:role/<parent-role>" },
    "Action": "sts:AssumeRole",
    "Condition": { "StringEquals": { "sts:ExternalId": "<external_id>" } }
  }]
}
```

**Mode (c) — IRSA trust:**

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Federated": "arn:aws:iam::<acct>:oidc-provider/<oidc-issuer>" },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {
        "<oidc-issuer>:sub": "system:serviceaccount:<ns>:<sa>"
      }
    }
  }]
}
```

K8s annotation: `eks.amazonaws.com/role-arn: arn:aws:iam::<acct>:role/<target-role>`.

---

## C. S3 / object-store options

| Field | Purpose |
|-------|---------|
| `s3_endpoint` | MinIO / VPC / custom |
| `s3_region` | Required for AWS |
| `s3_path_style` | MinIO needs `true` |
| `s3_force_virtual_addressing` | AWS-hosted buckets only |
| `s3_proxy_uri`, `s3_connect_timeout`, `s3_request_timeout` | Enterprise networks |
| `s3_anonymous` | Public buckets (rare) |
| **SSE** | `sse_type`: `none` \| `sse-s3` \| `sse-kms` \| `dsse-kms`; `sse_kms_key_id` |
| Bucket naming | Shared warehouse prefix across tables helps S3 scaling |

**Storage backends in v1:** AWS S3 + MinIO. Azure (`adls.*`) / GCS (`gcs.*`) = UI stubs labeled "Coming soon".

---

## D. Partitioning + identity (connection / stream level)

Collected when creating/editing **connection streams** (not only destination):

| Field | Guidance |
|-------|----------|
| `partition_spec` | List of `{source_column, transform, name}` |
| Transforms | `identity`, `year`, `month`, `day`, `hour` (discourage), `bucket(N)`, `truncate(W)` |
| Defaults | Prefer **`day(event_ts)`** or business date; or **`bucket(16–128, pk)`** for high-cardinality keys |
| Avoid | Hourly partitions on high-volume CDC → small files |
| `identifier_fields` | PK columns for PyIceberg **`table.upsert()`** |
| Evolution | v1 create-time only; `update_spec` planned |

Iceberg tracks partitions in metadata (not Hive folders alone). Set `partition_spec` at table create.

---

## E. Table / write properties (destination defaults + connection overrides)

| Property | Default for Fusion CDC |
|----------|------------------------|
| `format-version` | `2` (V3 deletion vectors = follow-on) |
| CDC apply | **`upsert`** on identifier fields + **`delete`** for CDC deletes |
| `write.parquet.compression-codec` | `zstd` |
| `write.object-storage.enabled` | `true` for S3 (hash prefixes → less throttling) |
| `write.object-storage.partitioned-paths` | `true` |
| Target file size | 128–512 MB data files via batching; schedule compaction |
| `write.metadata.delete-after-commit.enabled` | optional advanced |
| Sort order | Optional `sort_order` on create |

---

## F. Connection-level options

| Field | Notes |
|-------|--------|
| `destination_type` | `postgres` \| `iceberg` |
| `namespace` / table mapping | `{namespace}.{stream}`; per-stream `iceberg_namespace` override |
| `partition_spec`, `identifier_fields` | Per stream |
| `initial_sync_mode` | `serial_snapshot` \| `parallel_pk_ranges` |
| `write_batch_rows` / `write_batch_bytes` | Tune memory vs S3 PUT rate |
| `schema_evolution_policy` | `add_columns` \| `manual_approval` |

---

## G. Ops / maintenance

Without these, 400GB+ CDC lakes rot (small files):

- Compaction (rewrite data files → 128–512 MB)
- Manifest rewrite / expire snapshots
- Orphan file cleanup
- S3 lifecycle (optional Glacier for old snapshots)
- Metrics: files/commit, avg file size, commit conflicts, S3 503s

These are planned as documented hooks / jobs in v1.2.x; v1.2.0 ships the writer + connection test only.

---

## H. Writer implementation

- `transform-worker/iceberg_writer.py`:
  - `load_catalog(dest_config)` → PyIceberg catalog (glue/rest/hive/sql/nessie/dynamodb)
  - `IcebergWriter.write_batch()` / `.upsert()` / `.delete()`
  - `test_connection(dest_config)` → catalog + namespace + HeadBucket
- `transform-worker/loader.py` routes by `connector_type` (`iceberg` vs `postgres`)
- `transform-worker/worker.py` unchanged (queue dispatch)

---

## 400 GB initial-sync time bands

| Environment | Sustained end-to-end | 400 GB source rough wall time |
|-------------|---------------------|------------------------------|
| Laptop / Docker Desktop (~8c) | 15–40 MB/s | ~3–7.5 hours |
| Single beefy VM same-region (16c, 64GB) | 50–120 MB/s | ~1–2.5 hours |
| Parallel snapshot (4 workers) | 150–300 MB/s aggregate | ~25–45 minutes |
| Large Spark EMR/Glue cluster | Variable; startup tax | Often 1–4 hours at similar net |

Rules of thumb for the DuckDB/PyIceberg path:
- Plan capacity as **~50–100 GB/hour per well-tuned worker** to S3 same-region.
- Cross-region / VPN / laptop Wi-Fi: cut rates 2–5× → 400 GB can become 8–24+ hours.
- After initial sync, CDC lag for steady state is minutes/seconds (binlog), not hours.

Product implications: parallel initial load by PK ranges; progress % in `connection_runs`; backpressure; do not open 400 GB fully in DuckDB memory (stream batches). For multi-TB, keep Spark as **optional** scale-out profile — not default.
