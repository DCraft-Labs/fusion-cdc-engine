import {
  type IcebergDestinationConfig,
  type CatalogType,
  type AuthMode,
  type SseType,
  warehouseHint,
} from "@/lib/iceberg-config";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectTrigger,
  SelectValue,
  SelectContent,
  SelectItem,
} from "@/components/ui/select";

interface Props {
  form: IcebergDestinationConfig;
  setForm: (f: IcebergDestinationConfig) => void;
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="space-y-3">
      <h3 className="text-sm font-semibold text-foreground">{title}</h3>
      <div className="space-y-4">{children}</div>
    </div>
  );
}

function Field({
  label,
  required,
  help,
  children,
}: {
  label: string;
  required?: boolean;
  help?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-1.5">
      <Label>
        {label} {required && <span className="text-destructive">*</span>}
      </Label>
      {children}
      {help && <p className="text-xs text-muted-foreground">{help}</p>}
    </div>
  );
}

export default function IcebergDestinationForm({ form, setForm }: Props) {
  const set = (patch: Partial<IcebergDestinationConfig>) =>
    setForm({ ...form, ...patch });

  return (
    <div className="space-y-6">
      <Section title="Catalog">
        <Field label="Catalog type" required help="Glue / REST / Hive / SQL / Nessie / DynamoDB">
          <Select value={form.catalog_type} onValueChange={(v) => set({ catalog_type: v as CatalogType })}>
            <SelectTrigger><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="nessie">Nessie</SelectItem>
              <SelectItem value="rest">REST</SelectItem>
              <SelectItem value="glue">Glue</SelectItem>
              <SelectItem value="hive">Hive</SelectItem>
              <SelectItem value="sql">SQL</SelectItem>
              <SelectItem value="dynamodb">DynamoDB</SelectItem>
            </SelectContent>
          </Select>
        </Field>
        <Field label="Catalog name" required>
          <Input value={form.catalog_name} onChange={(e) => set({ catalog_name: e.target.value })} />
        </Field>
        <Field label="Namespace" required help="Iceberg namespace / Glue database">
          <Input value={form.namespace} onChange={(e) => set({ namespace: e.target.value })} />
        </Field>

        {form.catalog_type === "rest" && (
          <>
            <Field label="REST URI" required>
              <Input value={form.catalog_uri ?? ""} onChange={(e) => set({ catalog_uri: e.target.value })} placeholder="https://catalog.example.com/api" />
            </Field>
            <Field label="OAuth token (optional)">
              <Input type="password" value={form.catalog_oauth_token ?? ""} onChange={(e) => set({ catalog_oauth_token: e.target.value })} />
            </Field>
            <label className="flex items-center gap-2 text-sm cursor-pointer">
              <input type="checkbox" checked={Boolean(form.rest_sigv4)} onChange={(e) => set({ rest_sigv4: e.target.checked })} />
              Use SigV4 signing
            </label>
          </>
        )}
        {form.catalog_type === "nessie" && (
          <>
            <Field label="Nessie URI" required>
              <Input value={form.nessie_uri ?? ""} onChange={(e) => set({ nessie_uri: e.target.value })} placeholder="http://nessie:19120/api/v2" />
            </Field>
            <Field label="Nessie ref (branch)">
              <Input value={form.nessie_ref ?? ""} onChange={(e) => set({ nessie_ref: e.target.value })} placeholder="main" />
            </Field>
          </>
        )}
        {form.catalog_type === "hive" && (
          <Field label="Hive thrift URI" required>
            <Input value={form.hive_uri ?? ""} onChange={(e) => set({ hive_uri: e.target.value })} placeholder="thrift://host:9083" />
          </Field>
        )}
        {form.catalog_type === "glue" && (
          <>
            <Field label="Glue region" required>
              <Input value={form.glue_region ?? ""} onChange={(e) => set({ glue_region: e.target.value })} placeholder="us-east-1" />
            </Field>
            <Field label="Glue endpoint (optional)">
              <Input value={form.glue_endpoint ?? ""} onChange={(e) => set({ glue_endpoint: e.target.value })} />
            </Field>
            <Field label="Glue account id (optional)">
              <Input value={form.glue_account_id ?? ""} onChange={(e) => set({ glue_account_id: e.target.value })} />
            </Field>
            <label className="flex items-center gap-2 text-sm cursor-pointer">
              <input type="checkbox" checked={Boolean(form.glue_skip_archive)} onChange={(e) => set({ glue_skip_archive: e.target.checked })} />
              Skip archive
            </label>
          </>
        )}
        {form.catalog_type === "sql" && (
          <Field label="SQL catalog URI" required>
            <Input value={form.sql_catalog_uri ?? ""} onChange={(e) => set({ sql_catalog_uri: e.target.value })} placeholder="sqlite:///var/lib/iceberg/catalog.db" />
          </Field>
        )}
        {form.catalog_type === "dynamodb" && (
          <Field label="DynamoDB catalog table" required>
            <Input value={form.dynamodb_table ?? ""} onChange={(e) => set({ dynamodb_table: e.target.value })} />
          </Field>
        )}
      </Section>

      <Section title="Storage (S3 / MinIO)">
        <Field label="Warehouse" required help={warehouseHint(form.catalog_type).help}>
          <Input value={form.warehouse} onChange={(e) => set({ warehouse: e.target.value })} placeholder={warehouseHint(form.catalog_type).placeholder} />
        </Field>
        <Field label="S3 region">
          <Input value={form.s3_region ?? ""} onChange={(e) => set({ s3_region: e.target.value })} placeholder="us-east-1" />
        </Field>
        <Field label="S3 endpoint (MinIO / VPC)">
          <Input value={form.s3_endpoint ?? ""} onChange={(e) => set({ s3_endpoint: e.target.value })} placeholder="http://minio:9000" />
        </Field>
        <label className="flex items-center gap-2 text-sm cursor-pointer">
          <input type="checkbox" checked={Boolean(form.s3_path_style)} onChange={(e) => set({ s3_path_style: e.target.checked })} />
          Path-style access (required for MinIO)
        </label>
        <label className="flex items-center gap-2 text-sm cursor-pointer">
          <input type="checkbox" checked={Boolean(form.s3_force_virtual_addressing)} onChange={(e) => set({ s3_force_virtual_addressing: e.target.checked })} />
          Force virtual addressing (AWS-hosted buckets only)
        </label>
        <Field label="SSE type">
          <Select value={form.sse_type ?? "none"} onValueChange={(v) => set({ sse_type: v as SseType })}>
            <SelectTrigger><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="none">none</SelectItem>
              <SelectItem value="sse-s3">sse-s3</SelectItem>
              <SelectItem value="sse-kms">sse-kms</SelectItem>
              <SelectItem value="dsse-kms">dsse-kms</SelectItem>
            </SelectContent>
          </Select>
        </Field>
        {(form.sse_type === "sse-kms" || form.sse_type === "dsse-kms") && (
          <Field label="KMS key id" required>
            <Input value={form.sse_kms_key_id ?? ""} onChange={(e) => set({ sse_kms_key_id: e.target.value })} />
          </Field>
        )}
        <Field label="Proxy URI (optional)">
          <Input value={form.s3_proxy_uri ?? ""} onChange={(e) => set({ s3_proxy_uri: e.target.value })} />
        </Field>
        <label className="flex items-center gap-2 text-sm cursor-pointer">
          <input type="checkbox" checked={Boolean(form.s3_anonymous)} onChange={(e) => set({ s3_anonymous: e.target.checked })} />
          Anonymous (public bucket)
        </label>
        <div className="rounded-md border border-dashed p-3 text-xs text-muted-foreground">
          Azure ADLS / GCS storage: <strong>coming soon</strong> (backend not yet implemented).
        </div>
      </Section>

      <Section title="Authentication">
        <div className="flex gap-4 flex-wrap">
          {(["access_key", "sts_assume", "irsa"] as AuthMode[]).map((m) => (
            <label key={m} className="flex items-center gap-2 text-sm cursor-pointer">
              <input
                type="radio"
                name="iceberg_auth_mode"
                checked={form.auth_mode === m}
                onChange={() => set({ auth_mode: m })}
              />
              {m === "access_key" ? "Access keys" : m === "sts_assume" ? "STS assume role" : "IRSA / workload identity"}
            </label>
          ))}
        </div>

        {form.auth_mode === "access_key" && (
          <>
            <Field label="AWS access key id" required>
              <Input value={form.aws_access_key_id ?? ""} onChange={(e) => set({ aws_access_key_id: e.target.value })} />
            </Field>
            <Field label="AWS secret access key" required>
              <Input type="password" value={form.aws_secret_access_key ?? ""} onChange={(e) => set({ aws_secret_access_key: e.target.value })} />
            </Field>
            <Field label="Session token (optional)">
              <Input type="password" value={form.aws_session_token ?? ""} onChange={(e) => set({ aws_session_token: e.target.value })} />
            </Field>
            <Field label="AWS region">
              <Input value={form.aws_region ?? ""} onChange={(e) => set({ aws_region: e.target.value })} />
            </Field>
            <Field label="AWS profile (optional)">
              <Input value={form.aws_profile ?? ""} onChange={(e) => set({ aws_profile: e.target.value })} />
            </Field>
          </>
        )}

        {form.auth_mode === "sts_assume" && (
          <>
            <Field label="Parent credential mode">
              <Input value={form.parent_credential_mode ?? ""} onChange={(e) => set({ parent_credential_mode: e.target.value as "keys" | "irsa" | "profile" })} placeholder="keys | irsa | profile" />
            </Field>
            <Field label="Parent role ARN (optional)">
              <Input value={form.parent_role_arn ?? ""} onChange={(e) => set({ parent_role_arn: e.target.value })} />
            </Field>
            <Field label="Target role ARN" required help="Role with access to bucket + catalog">
              <Input value={form.target_role_arn ?? ""} onChange={(e) => set({ target_role_arn: e.target.value })} />
            </Field>
            <Field label="External id">
              <Input value={form.external_id ?? ""} onChange={(e) => set({ external_id: e.target.value })} />
            </Field>
            <Field label="Role session name">
              <Input value={form.role_session_name ?? ""} onChange={(e) => set({ role_session_name: e.target.value })} placeholder="fusion-cdc" />
            </Field>
            <Field label="Assume role timeout (sec)">
              <Input type="number" value={form.assume_role_timeout_sec ?? ""} onChange={(e) => set({ assume_role_timeout_sec: Number(e.target.value) })} />
            </Field>
            <Field label="STS region">
              <Input value={form.sts_region ?? ""} onChange={(e) => set({ sts_region: e.target.value })} />
            </Field>
          </>
        )}

        {form.auth_mode === "irsa" && (
          <>
            <Field label="Service account role ARN" required help="Set K8s SA annotation eks.amazonaws.com/role-arn; leave static keys empty">
              <Input value={form.service_account_role_arn ?? ""} onChange={(e) => set({ service_account_role_arn: e.target.value })} />
            </Field>
            <Field label="AWS region">
              <Input value={form.aws_region ?? ""} onChange={(e) => set({ aws_region: e.target.value })} />
            </Field>
          </>
        )}

        <label className="flex items-center gap-2 text-sm cursor-pointer">
          <input
            type="checkbox"
            checked={Boolean(form.same_creds_for_catalog_and_s3)}
            onChange={(e) => set({ same_creds_for_catalog_and_s3: e.target.checked })}
          />
          Use same creds for catalog + S3 (if off, supply separate S3 keys below)
        </label>
        {!form.same_creds_for_catalog_and_s3 && (
          <>
            <Field label="S3 access key id (separate)">
              <Input value={form.s3_access_key_id ?? ""} onChange={(e) => set({ s3_access_key_id: e.target.value })} />
            </Field>
            <Field label="S3 secret access key (separate)">
              <Input type="password" value={form.s3_secret_access_key ?? ""} onChange={(e) => set({ s3_secret_access_key: e.target.value })} />
            </Field>
            <Field label="S3 session token (separate)">
              <Input type="password" value={form.s3_session_token ?? ""} onChange={(e) => set({ s3_session_token: e.target.value })} />
            </Field>
          </>
        )}
      </Section>

      <Section title="Table defaults">
        <Field label="Iceberg format version">
          <Select value={String(form.format_version ?? 2)} onValueChange={(v) => set({ format_version: Number(v) as 2 | 3 })}>
            <SelectTrigger><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="2">2</SelectItem>
              <SelectItem value="3">3</SelectItem>
            </SelectContent>
          </Select>
        </Field>
        <Field label="Parquet compression">
          <Select value={form.parquet_compression ?? "zstd"} onValueChange={(v) => set({ parquet_compression: v as "snappy" | "zstd" | "gzip" })}>
            <SelectTrigger><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="snappy">snappy</SelectItem>
              <SelectItem value="zstd">zstd</SelectItem>
              <SelectItem value="gzip">gzip</SelectItem>
            </SelectContent>
          </Select>
        </Field>
        <label className="flex items-center gap-2 text-sm cursor-pointer">
          <input type="checkbox" checked={Boolean(form.object_storage_enabled)} onChange={(e) => set({ object_storage_enabled: e.target.checked })} />
          Object-storage layout enabled (hash prefixes — reduces S3 throttling)
        </label>
        <label className="flex items-center gap-2 text-sm cursor-pointer">
          <input type="checkbox" checked={Boolean(form.partitioned_paths)} onChange={(e) => set({ partitioned_paths: e.target.checked })} />
          Partitioned paths
        </label>
        <Field label="CDC apply strategy">
          <Select value={form.cdc_apply_strategy ?? "upsert"} onValueChange={(v) => set({ cdc_apply_strategy: v as "upsert" | "append" })}>
            <SelectTrigger><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="upsert">upsert</SelectItem>
              <SelectItem value="append">append</SelectItem>
            </SelectContent>
          </Select>
        </Field>
        <label className="flex items-center gap-2 text-sm cursor-pointer">
          <input type="checkbox" checked={Boolean(form.write_metadata_delete_after_commit)} onChange={(e) => set({ write_metadata_delete_after_commit: e.target.checked })} />
          Delete metadata after commit
        </label>
      </Section>

      <details className="rounded-md border p-3">
        <summary className="cursor-pointer text-sm font-medium">Advanced (optional / legacy Spark)</summary>
        <div className="mt-3 space-y-4">
          <p className="text-xs text-muted-foreground">
            Spark is <strong>not required</strong> for the DuckDB/PyIceberg lake path. Use only for multi-TB scale-out profiles.
          </p>
          <Field label="Spark master (legacy)">
            <Input value={form.spark_master ?? ""} onChange={(e) => set({ spark_master: e.target.value })} placeholder="k8s://..." />
          </Field>
          <Field label="Spark image (legacy)">
            <Input value={form.spark_image ?? ""} onChange={(e) => set({ spark_image: e.target.value })} />
          </Field>
        </div>
      </details>
    </div>
  );
}
