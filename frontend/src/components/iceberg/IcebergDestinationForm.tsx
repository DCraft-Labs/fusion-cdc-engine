import React from "react";
import {
  IcebergDestinationConfig,
  CatalogType,
  AuthMode,
  SseType,
  ICEBERG_DEFAULTS,
} from "../../lib/iceberg-config";

interface FieldProps {
  label: string;
  value: string | number | boolean | undefined;
  onChange: (v: any) => void;
  type?: "text" | "password" | "number" | "checkbox";
  placeholder?: string;
  help?: string;
  required?: boolean;
}

function Field({ label, value, onChange, type = "text", placeholder, help, required }: FieldProps) {
  return (
    <div style={{ marginBottom: 12 }}>
      <label style={{ display: "block", fontWeight: 600, fontSize: 13, marginBottom: 4 }}>
        {label} {required && <span style={{ color: "#dc2626" }}>*</span>}
      </label>
      {type === "checkbox" ? (
        <input
          type="checkbox"
          checked={Boolean(value)}
          onChange={(e) => onChange(e.target.checked)}
        />
      ) : (
        <input
          type={type}
          value={(value ?? "") as any}
          onChange={(e) => onChange(e.target.value)}
          placeholder={placeholder}
          style={{ width: "100%", padding: "8px 10px", border: "1px solid #d1d5db", borderRadius: 6 }}
        />
      )}
      {help && <div style={{ fontSize: 12, color: "#6b7280", marginTop: 4 }}>{help}</div>}
    </div>
  );
}

interface Props {
  form: IcebergDestinationConfig;
  setForm: (f: IcebergDestinationConfig) => void;
}

export default function IcebergDestinationForm({ form, setForm }: Props) {
  const set = (patch: Partial<IcebergDestinationConfig>) =>
    setForm({ ...form, ...patch });

  return (
    <div>
      {/* ─── Catalog ─── */}
      <h3 style={{ fontSize: 16, marginTop: 0 }}>Catalog</h3>
      <Field label="Catalog type" value={form.catalog_type} onChange={(v) => set({ catalog_type: v as CatalogType })} required
        help="Glue / REST / Hive / SQL / Nessie / DynamoDB" />
      <Field label="Catalog name" value={form.catalog_name} onChange={(v) => set({ catalog_name: v })} required />
      <Field label="Namespace" value={form.namespace} onChange={(v) => set({ namespace: v })} required
        help="Iceberg namespace / Glue database" />

      {form.catalog_type === "rest" && (
        <>
          <Field label="REST URI" value={form.catalog_uri} onChange={(v) => set({ catalog_uri: v })} required placeholder="https://catalog.example.com/api" />
          <Field label="OAuth token (optional)" type="password" value={form.catalog_oauth_token} onChange={(v) => set({ catalog_oauth_token: v })} />
          <Field label="Use SigV4 signing" type="checkbox" value={form.rest_sigv4} onChange={(v) => set({ rest_sigv4: v })} />
        </>
      )}
      {form.catalog_type === "nessie" && (
        <>
          <Field label="Nessie URI" value={form.nessie_uri} onChange={(v) => set({ nessie_uri: v })} required placeholder="http://nessie:19120/api/v2" />
          <Field label="Nessie ref (branch)" value={form.nessie_ref} onChange={(v) => set({ nessie_ref: v })} placeholder="main" />
        </>
      )}
      {form.catalog_type === "hive" && (
        <Field label="Hive thrift URI" value={form.hive_uri} onChange={(v) => set({ hive_uri: v })} required placeholder="thrift://host:9083" />
      )}
      {form.catalog_type === "glue" && (
        <>
          <Field label="Glue region" value={form.glue_region} onChange={(v) => set({ glue_region: v })} required placeholder="us-east-1" />
          <Field label="Glue endpoint (optional)" value={form.glue_endpoint} onChange={(v) => set({ glue_endpoint: v })} />
          <Field label="Glue account id (optional)" value={form.glue_account_id} onChange={(v) => set({ glue_account_id: v })} />
          <Field label="Skip archive" type="checkbox" value={form.glue_skip_archive} onChange={(v) => set({ glue_skip_archive: v })} />
        </>
      )}
      {form.catalog_type === "sql" && (
        <Field label="SQL catalog URI" value={form.sql_catalog_uri} onChange={(v) => set({ sql_catalog_uri: v })} required placeholder="sqlite:///var/lib/iceberg/catalog.db" />
      )}
      {form.catalog_type === "dynamodb" && (
        <Field label="DynamoDB catalog table" value={form.dynamodb_table} onChange={(v) => set({ dynamodb_table: v })} required />
      )}

      {/* ─── Storage ─── */}
      <h3 style={{ fontSize: 16, marginTop: 20 }}>Storage (S3 / MinIO)</h3>
      <Field label="Warehouse" value={form.warehouse} onChange={(v) => set({ warehouse: v })} required
        placeholder="s3://iceberg-warehouse/fusion-cdc/" help="Accepts s3:// or s3a:// (normalized to s3://)" />
      <Field label="S3 region" value={form.s3_region} onChange={(v) => set({ s3_region: v })} placeholder="us-east-1" />
      <Field label="S3 endpoint (MinIO / VPC)" value={form.s3_endpoint} onChange={(v) => set({ s3_endpoint: v })} placeholder="http://minio:9000" />
      <Field label="Path-style access" type="checkbox" value={form.s3_path_style} onChange={(v) => set({ s3_path_style: v })}
        help="Required for MinIO" />
      <Field label="Force virtual addressing" type="checkbox" value={form.s3_force_virtual_addressing} onChange={(v) => set({ s3_force_virtual_addressing: v })}
        help="AWS-hosted buckets only" />
      <Field label="SSE type" value={form.sse_type} onChange={(v) => set({ sse_type: v as SseType })} />
      {(form.sse_type === "sse-kms" || form.sse_type === "dsse-kms") && (
        <Field label="KMS key id" value={form.sse_kms_key_id} onChange={(v) => set({ sse_kms_key_id: v })} required />
      )}
      <Field label="Proxy URI (optional)" value={form.s3_proxy_uri} onChange={(v) => set({ s3_proxy_uri: v })} />
      <Field label="Anonymous (public bucket)" type="checkbox" value={form.s3_anonymous} onChange={(v) => set({ s3_anonymous: v })} />

      {/* Azure / GCS — coming soon */}
      <div style={{ marginTop: 12, padding: 10, border: "1px dashed #d1d5db", borderRadius: 6, color: "#6b7280", fontSize: 13 }}>
        Azure ADLS / GCS storage: <strong>coming soon</strong> (backend not yet implemented).
      </div>

      <IcebergAuthFields form={form} set={set} />
      <IcebergTableDefaultsFields form={form} set={set} />

      {/* ─── Advanced (Spark demoted) ─── */}
      <details style={{ marginTop: 20 }}>
        <summary style={{ cursor: "pointer", fontWeight: 600, fontSize: 14 }}>Advanced (optional / legacy Spark)</summary>
        <div style={{ marginTop: 10, padding: 10, background: "#f9fafb", borderRadius: 6 }}>
          <p style={{ fontSize: 12, color: "#6b7280", marginTop: 0 }}>
            Spark is <strong>not required</strong> for the DuckDB/PyIceberg lake path. Use only for
            multi-TB scale-out profiles.
          </p>
          <Field label="Spark master (legacy)" value={form.spark_master} onChange={(v) => set({ spark_master: v })} placeholder="k8s://..." />
          <Field label="Spark image (legacy)" value={form.spark_image} onChange={(v) => set({ spark_image: v })} />
        </div>
      </details>
    </div>
  );
}

function IcebergAuthFields({ form, set }: { form: IcebergDestinationConfig; set: (p: Partial<IcebergDestinationConfig>) => void }) {
  const mode = form.auth_mode;
  return (
    <div style={{ marginTop: 20 }}>
      <h3 style={{ fontSize: 16, marginTop: 0 }}>Authentication</h3>
      <div style={{ display: "flex", gap: 12, marginBottom: 12 }}>
        {(["access_key", "sts_assume", "irsa"] as AuthMode[]).map((m) => (
          <label key={m} style={{ fontSize: 13 }}>
            <input type="radio" checked={mode === m} onChange={() => set({ auth_mode: m })} />
            {" "}
            {m === "access_key" ? "Access keys" : m === "sts_assume" ? "STS assume role" : "IRSA / workload identity"}
          </label>
        ))}
      </div>

      {mode === "access_key" && (
        <>
          <Field label="AWS access key id" value={form.aws_access_key_id} onChange={(v) => set({ aws_access_key_id: v })} required />
          <Field label="AWS secret access key" type="password" value={form.aws_secret_access_key} onChange={(v) => set({ aws_secret_access_key: v })} required />
          <Field label="Session token (optional)" type="password" value={form.aws_session_token} onChange={(v) => set({ aws_session_token: v })} />
          <Field label="AWS region" value={form.aws_region} onChange={(v) => set({ aws_region: v })} />
          <Field label="AWS profile (optional)" value={form.aws_profile} onChange={(v) => set({ aws_profile: v })} />
        </>
      )}

      {mode === "sts_assume" && (
        <>
          <Field label="Parent credential mode" value={form.parent_credential_mode} onChange={(v) => set({ parent_credential_mode: v })} placeholder="keys | irsa | profile" />
          <Field label="Parent role ARN (optional)" value={form.parent_role_arn} onChange={(v) => set({ parent_role_arn: v })} />
          <Field label="Target role ARN" value={form.target_role_arn} onChange={(v) => set({ target_role_arn: v })} required
            help="Role with access to bucket + catalog" />
          <Field label="External id" value={form.external_id} onChange={(v) => set({ external_id: v })} />
          <Field label="Role session name" value={form.role_session_name} onChange={(v) => set({ role_session_name: v })} placeholder="fusion-cdc" />
          <Field label="Assume role timeout (sec)" type="number" value={form.assume_role_timeout_sec} onChange={(v) => set({ assume_role_timeout_sec: Number(v) })} />
          <Field label="STS region" value={form.sts_region} onChange={(v) => set({ sts_region: v })} />
        </>
      )}

      {mode === "irsa" && (
        <>
          <Field label="Service account role ARN" value={form.service_account_role_arn} onChange={(v) => set({ service_account_role_arn: v })} required
            help="Set K8s SA annotation eks.amazonaws.com/role-arn; leave static keys empty" />
          <Field label="AWS region" value={form.aws_region} onChange={(v) => set({ aws_region: v })} />
        </>
      )}

      <Field label="Use same creds for catalog + S3" type="checkbox" value={form.same_creds_for_catalog_and_s3} onChange={(v) => set({ same_creds_for_catalog_and_s3: v })}
        help="If off, supply separate S3 keys below" />
      {!form.same_creds_for_catalog_and_s3 && (
        <>
          <Field label="S3 access key id (separate)" value={form.s3_access_key_id} onChange={(v) => set({ s3_access_key_id: v })} />
          <Field label="S3 secret access key (separate)" type="password" value={form.s3_secret_access_key} onChange={(v) => set({ s3_secret_access_key: v })} />
          <Field label="S3 session token (separate)" type="password" value={form.s3_session_token} onChange={(v) => set({ s3_session_token: v })} />
        </>
      )}
    </div>
  );
}

function IcebergTableDefaultsFields({ form, set }: { form: IcebergDestinationConfig; set: (p: Partial<IcebergDestinationConfig>) => void }) {
  return (
    <div style={{ marginTop: 20 }}>
      <h3 style={{ fontSize: 16, marginTop: 0 }}>Table defaults</h3>
      <Field label="Iceberg format version" type="number" value={form.format_version} onChange={(v) => set({ format_version: Number(v) as 2 | 3 })} />
      <Field label="Parquet compression" value={form.parquet_compression} onChange={(v) => set({ parquet_compression: v as any })} placeholder="zstd" />
      <Field label="Object-storage layout enabled" type="checkbox" value={form.object_storage_enabled} onChange={(v) => set({ object_storage_enabled: v })}
        help="Hash prefixes — reduces S3 throttling on large tables" />
      <Field label="Partitioned paths" type="checkbox" value={form.partitioned_paths} onChange={(v) => set({ partitioned_paths: v })} />
      <Field label="CDC apply strategy" value={form.cdc_apply_strategy} onChange={(v) => set({ cdc_apply_strategy: v as any })} placeholder="upsert" />
      <Field label="Delete metadata after commit" type="checkbox" value={form.write_metadata_delete_after_commit} onChange={(v) => set({ write_metadata_delete_after_commit: v })} />
    </div>
  );
}
