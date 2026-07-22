/**
 * Iceberg destination config — types, defaults, builders, validators.
 *
 * Field names match the backend (transform-worker/iceberg_writer.py + control-plane
 * destination schema) 1:1. Do NOT rename without updating both sides.
 */

export type CatalogType = "glue" | "rest" | "hive" | "sql" | "nessie" | "dynamodb";
export type AuthMode = "access_key" | "sts_assume" | "irsa";
export type SseType = "none" | "sse-s3" | "sse-kms" | "dsse-kms";
export type PartitionTransform =
  | "identity"
  | "year"
  | "month"
  | "day"
  | "hour"
  | "bucket"
  | "truncate";

export interface PartitionField {
  source_column: string;
  transform: PartitionTransform;
  name?: string;
  width?: number; // for bucket / truncate
}

export interface IcebergDestinationConfig {
  // Catalog
  catalog_type: CatalogType;
  catalog_name: string;
  namespace: string;
  warehouse: string;

  // Catalog-type-specific
  catalog_uri?: string;          // rest
  catalog_oauth_token?: string;  // rest
  rest_sigv4?: boolean;
  nessie_uri?: string;           // nessie
  nessie_ref?: string;           // nessie
  hive_uri?: string;             // hive
  glue_region?: string;          // glue
  glue_endpoint?: string;        // glue
  glue_account_id?: string;      // glue
  glue_skip_archive?: boolean;   // glue
  sql_catalog_uri?: string;      // sql
  dynamodb_table?: string;       // dynamodb

  // S3 / object store
  s3_endpoint?: string;
  s3_region?: string;
  s3_path_style?: boolean;
  s3_force_virtual_addressing?: boolean;
  s3_proxy_uri?: string;
  s3_connect_timeout?: number;
  s3_request_timeout?: number;
  s3_anonymous?: boolean;
  sse_type?: SseType;
  sse_kms_key_id?: string;

  // Auth (mode a/b/c)
  auth_mode: AuthMode;
  aws_access_key_id?: string;
  aws_secret_access_key?: string;
  aws_session_token?: string;
  aws_region?: string;
  aws_profile?: string;

  // STS assume (mode b)
  parent_credential_mode?: "keys" | "irsa" | "profile";
  parent_role_arn?: string;
  target_role_arn?: string;
  external_id?: string;
  role_session_name?: string;
  assume_role_timeout_sec?: number;
  sts_region?: string;

  // IRSA (mode c)
  service_account_role_arn?: string;

  // Cred unification
  same_creds_for_catalog_and_s3?: boolean;
  s3_access_key_id?: string;
  s3_secret_access_key?: string;
  s3_session_token?: string;

  // Table defaults
  format_version?: 2 | 3;
  parquet_compression?: "snappy" | "zstd" | "gzip";
  object_storage_enabled?: boolean;
  partitioned_paths?: boolean;
  cdc_apply_strategy?: "upsert" | "append";
  write_metadata_delete_after_commit?: boolean;

  // v1.2.19: snapshot mode — where the initial-load snapshot runs.
  //   inline            → cdc_consumer.py performs the snapshot (default, production path via kubernetes/base/cdc-consumer.yaml)
  //   transform_worker  → transform-worker performs the snapshot (opt-in for Iceberg/lake destinations)
  snapshot_mode?: "transform_worker" | "inline";

  // Advanced (optional Spark legacy)
  spark_master?: string;
  spark_image?: string;
}

export const ICEBERG_DEFAULTS: IcebergDestinationConfig = {
  catalog_type: "nessie",
  catalog_name: "fusion_cdc",
  namespace: "fusion",
  warehouse: "",
  auth_mode: "access_key",
  aws_region: "us-east-1",
  same_creds_for_catalog_and_s3: true,
  format_version: 2,
  parquet_compression: "zstd",
  object_storage_enabled: true,
  partitioned_paths: true,
  cdc_apply_strategy: "upsert",
  sse_type: "none",
  s3_path_style: false,
  // v1.2.19: inline is the default snapshot path (cdc_consumer.py).
  snapshot_mode: "inline",
};

/** Normalize `s3a://` → `s3://` for PyIceberg. */
export function normalizeWarehouse(warehouse: string): string {
  if (!warehouse) return warehouse;
  if (warehouse.startsWith("s3a://")) return "s3://" + warehouse.slice("s3a://".length);
  return warehouse;
}

/**
 * Catalog-type-aware warehouse placeholder + help text.
 *
 * For `nessie` / `rest` catalogs the `warehouse` field is the
 * Nessie-registered warehouse NAME (e.g. `iceberg-warehouse`), NOT an S3
 * path — Nessie resolves the name to the physical S3 location via its own
 * config. For `hive` / `glue` / `sql` / `dynamodb` the warehouse is an S3
 * path used as the default location for new tables (accepts `s3://` or
 * `s3a://`, normalized to `s3://`).
 */
export function warehouseHint(catalogType: CatalogType): { placeholder: string; help: string } {
  switch (catalogType) {
    case "nessie":
    case "rest":
      return {
        placeholder: "iceberg-warehouse",
        help:
          "Nessie-registered warehouse name (e.g. `iceberg-warehouse`), not an S3 path — Nessie resolves it to the physical S3 location.",
      };
    case "hive":
    case "glue":
    case "sql":
    case "dynamodb":
      return {
        placeholder: "s3://iceberg-warehouse/fusion-cdc/",
        help: "S3 warehouse path (accepts s3:// or s3a://, normalized to s3://).",
      };
    default:
      return {
        placeholder: "s3://iceberg-warehouse/fusion-cdc/",
        help: "Accepts s3:// or s3a:// (normalized to s3://).",
      };
  }
}

export function buildConnectionConfig(form: IcebergDestinationConfig): IcebergDestinationConfig {
  return {
    ...form,
    warehouse: normalizeWarehouse(form.warehouse),
  };
}

export function validateIcebergForm(form: IcebergDestinationConfig): string[] {
  const errors: string[] = [];
  if (!form.catalog_name) errors.push("Catalog name is required");
  if (!form.namespace) errors.push("Namespace is required");
  if (!form.warehouse) errors.push("Warehouse is required");

  switch (form.catalog_type) {
    case "rest":
      if (!form.catalog_uri) errors.push("REST catalog URI is required");
      break;
    case "nessie":
      if (!form.nessie_uri) errors.push("Nessie URI is required");
      break;
    case "hive":
      if (!form.hive_uri) errors.push("Hive thrift URI is required");
      break;
    case "glue":
      if (!form.glue_region) errors.push("Glue region is required");
      break;
    case "sql":
      if (!form.sql_catalog_uri) errors.push("SQL catalog URI is required");
      break;
    case "dynamodb":
      if (!form.dynamodb_table) errors.push("DynamoDB table is required");
      break;
  }

  if (form.auth_mode === "access_key") {
    if (!form.aws_access_key_id) errors.push("AWS access key id is required");
    if (!form.aws_secret_access_key) errors.push("AWS secret access key is required");
  } else if (form.auth_mode === "sts_assume") {
    if (!form.target_role_arn) errors.push("Target role ARN is required");
  } else if (form.auth_mode === "irsa") {
    if (!form.service_account_role_arn) errors.push("Service account role ARN is required (or set in K8s annotation)");
  }

  if (form.sse_type === "sse-kms" || form.sse_type === "dsse-kms") {
    if (!form.sse_kms_key_id) errors.push("KMS key id is required for SSE-KMS");
  }
  return errors;
}
