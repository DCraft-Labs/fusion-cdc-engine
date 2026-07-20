# Connector Seed Reference

The seed file `scripts/seed-admin.sql` is idempotent and creates:

## Roles / users
- Role: `superadmin`
- User: `admin` / `Admin@123` (superuser, email-verified)

## Connector definitions

### Sources
| Connector name | `connector_type` | `category` | CDC | Full refresh | Incremental |
|----------------|------------------|------------|-----|---------------|-------------|
| PostgreSQL     | `postgres`       | source     | ✓   | ✓             | ✓           |
| MySQL          | `mysql`          | source     | ✓   | ✓             | ✓           |
| MongoDB        | `mongodb`        | source     | ✓   | ✓             | —           |

### Destinations
| Connector name | `connector_type` | `category` | CDC | Full refresh | Incremental |
|----------------|------------------|------------|-----|---------------|-------------|
| PostgreSQL Destination | `postgresql` | destination | — | ✓ | ✓ |
| Apache Iceberg         | `iceberg`    | destination | ✓ | ✓ | ✓ |
| Amazon S3              | `s3`         | destination | ✓ | ✓ | ✓ |

### Iceberg destination — required + optional fields

**Required:** `catalog_type`, `catalog_name`, `namespace`, `warehouse`, `auth_mode`

**Optional (catalog-type-specific):** `nessie_uri`, `nessie_ref`, `catalog_uri`, `catalog_oauth_token`, `rest_sigv4`, `hive_uri`, `glue_region`, `glue_endpoint`, `glue_account_id`, `glue_skip_archive`, `sql_catalog_uri`, `dynamodb_table`

**Optional (S3 / object store):** `s3_endpoint`, `s3_region`, `s3_path_style`, `s3_force_virtual_addressing`, `s3_proxy_uri`, `s3_anonymous`, `sse_type`, `sse_kms_key_id`

**Optional (auth a/b/c):** `aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`, `aws_region`, `aws_profile`, `parent_credential_mode`, `parent_role_arn`, `target_role_arn`, `external_id`, `role_session_name`, `assume_role_timeout_sec`, `sts_region`, `service_account_role_arn`, `same_creds_for_catalog_and_s3`, `s3_access_key_id`, `s3_secret_access_key`, `s3_session_token`

**Optional (table defaults):** `format_version`, `parquet_compression`, `object_storage_enabled`, `partitioned_paths`, `cdc_apply_strategy`, `write_metadata_delete_after_commit`

**Optional (legacy Spark):** `spark_master`, `spark_image`

## Sample destinations seeded
- `Local PostgreSQL Destination` → `postgres-dest:5432/fusion_dw`
- `Local Iceberg (MinIO + Nessie)` → `s3://iceberg-warehouse/fusion-cdc/` via `http://nessie:19120/api/v2`, MinIO `http://minio:9000` (path-style, access keys `minio` / `minio123`)

## Sample connection seeded
- `pg-source → pg-dest (REALTIME)` — CDC, slot `fusion_slot`, pub `fusion_pub`

## Wiring

### K8s (Docker Desktop)
Apply `infra/local-dev/k8s/seed-connectors-job.yaml` (ConfigMap + post-install Hook). The `deploy.ps1` script hydrates the ConfigMap from `scripts/seed-admin.sql` before `helm upgrade`.

### Compose
The `transform-worker` service in `docker/docker-compose.dev.yml` mounts `scripts/seed-admin.sql` and runs it on startup via the `postgres-meta` healthcheck dependency. Alternatively, run manually:

```bash
docker compose -f docker/docker-compose.dev.yml exec postgres-meta \
  psql -U fusion_user -d fusion_cdc_metadata -f /seed/seed-admin.sql
```
