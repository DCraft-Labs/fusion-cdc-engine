import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useMutation, useQuery } from "@tanstack/react-query";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { CheckCircle, Circle, Loader2, Check, X, Minus } from "lucide-react";
import IcebergDestinationForm from "@/components/iceberg/IcebergDestinationForm";
import {
  ICEBERG_DEFAULTS,
  type IcebergDestinationConfig,
  buildConnectionConfig,
  validateIcebergForm,
} from "@/lib/iceberg-config";

const STEPS = ["Select Type", "Configure", "Test Connection"];

type TestCheck = { label: string; status: "pending" | "running" | "success" | "error"; message?: string };

export function CreateDestinationWizard() {
  const navigate = useNavigate();
  const [step, setStep] = useState(0);
  const [destType, setDestType] = useState("");
  const [pgForm, setPgForm] = useState({
    name: "", host: "localhost", port: "5432", database: "", schema: "public", username: "", password: "", write_mode: "scd1",
    // SSL/TLS (separate from SSH)
    ssl_enabled: false,
    ssl_mode: "require" as "disable" | "allow" | "prefer" | "require" | "verify-ca" | "verify-full",
    ssl_ca: "", ssl_cert: "", ssl_key: "",
    // SSH Tunnel (separate from SSL)
    ssh_enabled: false,
    tunnel_host: "", tunnel_port: "22", tunnel_username: "",
    tunnel_auth_method: "password" as "password" | "key",
    tunnel_password: "", tunnel_private_key: "", tunnel_passphrase: "",
  });
  const [icebergForm, setIcebergForm] = useState<IcebergDestinationConfig>({
    ...ICEBERG_DEFAULTS,
    catalog_type: "nessie",
    catalog_name: "fusion_cdc",
    namespace: "fusion",
    nessie_uri: "",
    nessie_ref: "main",
    warehouse: "",
    s3_endpoint: "",
    s3_region: "us-east-1",
    auth_mode: "access_key",
  });
  const [icebergDestName, setIcebergDestName] = useState("");
  const [destId, setDestId] = useState<string | null>(null);
  const [testChecks, setTestChecks] = useState<TestCheck[]>([]);
  const [sslOpen, setSslOpen] = useState(false);
  const [tunnelTestResult, setTunnelTestResult] = useState<any>(null);

  const { data: connectors = [] } = useQuery({
    queryKey: ["connector-definitions"],
    queryFn: () => fetchList("/connector-definitions", "connectors"),
  });

  const destConnectors = connectors
    .filter((c: any) => c.category === "destination" && c.is_active !== false)
    .map((c: any) => ({
      id: c.connector_type ?? c.type,
      connectorId: c.connector_id ?? c.id,
      name: c.connector_name ?? c.name,
      icon: c.connector_type === "postgresql" || c.connector_type === "postgres" ? "🐘"
        : c.connector_type === "iceberg" ? "🧊"
        : c.connector_type === "snowflake" ? "❄️"
        : c.connector_type === "bigquery" ? "📊"
        : "🔌",
      description: [
        c.supports_cdc && "CDC",
        c.supports_full_refresh && "Full Refresh",
        c.supports_incremental && "Incremental",
      ].filter(Boolean).join(" · ") || c.connector_name,
      disabled: false,
    }));

  const selectedConnector = connectors.find((c: any) => c.category === "destination" && (c.connector_type ?? c.type) === destType);

  const createMutation = useMutation({
    mutationFn: () => {
      if (destType === "postgresql" || destType === "postgres") {
        return api.post("/destinations", {
          destination_name: pgForm.name,
          connector_definition_id: selectedConnector?.connector_id ?? selectedConnector?.id,
          connector_version: selectedConnector?.latest_version ?? "1.0.0",
          host: pgForm.host,
          port: parseInt(pgForm.port),
          database_name: pgForm.database,
          schema_name: pgForm.schema,
          username: pgForm.username,
          password: pgForm.password,
          ssl_enabled: pgForm.ssl_enabled,
          ssl_config: pgForm.ssl_enabled ? {
            ssl_mode: pgForm.ssl_mode,
            ssl_ca: pgForm.ssl_ca || undefined,
            ssl_cert: pgForm.ssl_cert || undefined,
            ssl_key: pgForm.ssl_key || undefined,
          } : {},
          ssh_config: pgForm.ssh_enabled ? {
            tunnel_host: pgForm.tunnel_host || undefined,
            tunnel_port: pgForm.tunnel_port ? parseInt(pgForm.tunnel_port) : 22,
            tunnel_username: pgForm.tunnel_username || undefined,
            tunnel_auth_method: pgForm.tunnel_auth_method,
            tunnel_password: pgForm.tunnel_auth_method === "password" ? pgForm.tunnel_password || undefined : undefined,
            tunnel_private_key: pgForm.tunnel_auth_method === "key" ? pgForm.tunnel_private_key || undefined : undefined,
            tunnel_passphrase: pgForm.tunnel_auth_method === "key" ? pgForm.tunnel_passphrase || undefined : undefined,
          } : {},
          config: { write_mode: pgForm.write_mode },
        });
      }
      return api.post("/destinations", {
        destination_name: icebergDestName,
        connector_definition_id: selectedConnector?.connector_id ?? selectedConnector?.id,
        connector_version: "1.0.0",
        config: buildConnectionConfig(icebergForm),
      });
    },
    onSuccess: (res) => { setDestId(res.data.destination_id ?? res.data.id); setStep(2); },
  });

  const testMutation = useMutation({
    mutationFn: () => api.post(`/destinations/${destId}/test-connection`),
    onMutate: () => {
      if (destType === "iceberg") {
        setTestChecks([
          { label: "Resolve Iceberg catalog", status: "running" },
          { label: "List namespace", status: "pending" },
          { label: "S3 HeadBucket / warehouse prefix", status: "pending" },
        ]);
      } else {
        setTestChecks([
          { label: "Network connectivity", status: "running" },
          { label: "Authentication", status: "pending" },
          { label: "Write permission", status: "pending" },
        ]);
      }
    },
    onSuccess: (res) => {
      const d = res.data ?? {};
      const isSuccess = d.status === "success";
      const checks = d.checks ?? [];
      if (destType === "iceberg" && checks.length) {
        setTestChecks(checks.map((c: any) => ({
          label: c.label,
          status: c.ok ? "success" : "error",
          message: c.message,
        })));
        return;
      }
      const msg = d.message ?? d.error_details ?? undefined;
      setTestChecks([
        { label: "Network connectivity", status: isSuccess ? "success" : "error", message: isSuccess ? undefined : msg },
        { label: "Authentication", status: isSuccess ? "success" : "error" },
        { label: "Write permission", status: isSuccess ? "success" : "error" },
      ]);
    },
    onError: (err: any) => {
      setTestChecks([
        { label: "Network connectivity", status: "error", message: err.response?.data?.detail ?? err.message },
        { label: "Authentication", status: "pending" },
        { label: "Write permission", status: "pending" },
      ]);
    },
  });

  const tunnelTestMutation = useMutation({
    mutationFn: () => {
      if (destId) {
        return api.post(`/destinations/${destId}/test-tunnel`);
      }
      return api.post("/destinations/test-tunnel", {
        ssh_config: {
          tunnel_host: pgForm.tunnel_host,
          tunnel_port: pgForm.tunnel_port ? parseInt(pgForm.tunnel_port) : 22,
          tunnel_username: pgForm.tunnel_username,
          tunnel_auth_method: pgForm.tunnel_auth_method,
          tunnel_password: pgForm.tunnel_auth_method === "password" ? pgForm.tunnel_password : undefined,
          tunnel_private_key: pgForm.tunnel_auth_method === "key" ? pgForm.tunnel_private_key : undefined,
          tunnel_passphrase: pgForm.tunnel_auth_method === "key" ? pgForm.tunnel_passphrase : undefined,
        },
      });
    },
    onSuccess: (res) => setTunnelTestResult(res.data),
    onError: (err: any) => setTunnelTestResult({ status: "failed", message: err.response?.data?.detail ?? err.message }),
  });

  const canProceed = () => {
    if (step === 0) return !!destType && !destConnectors.find((d: any) => d.id === destType)?.disabled;
    if (step === 1) {
      if (destType === "postgresql" || destType === "postgres") return pgForm.name.trim() !== "" && pgForm.host.trim() !== "" && pgForm.database.trim() !== "" && pgForm.username.trim() !== "" && pgForm.password.trim() !== "";
      if (destType === "iceberg") {
        const errs = validateIcebergForm(icebergForm);
        return icebergDestName.trim() !== "" && errs.length === 0;
      }
    }
    return false;
  };

  const checkIcon = (status: TestCheck["status"]) => {
    switch (status) {
      case "success": return <Check className="h-4 w-4 text-green-500" />;
      case "error": return <X className="h-4 w-4 text-destructive" />;
      case "running": return <Loader2 className="h-4 w-4 animate-spin text-primary" />;
      default: return <Minus className="h-4 w-4 text-muted-foreground" />;
    }
  };

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Create New Destination</h1>

      {/* Step indicator */}
      <div className="flex items-center gap-2">
        {STEPS.map((s, i) => (
          <div key={s} className="flex items-center gap-2">
            {i > 0 && <div className={`h-0.5 w-8 ${i <= step ? "bg-primary" : "bg-border"}`} />}
            <div className="flex items-center gap-1.5">
              {i < step ? <CheckCircle className="h-5 w-5 text-primary" /> : i === step ? <Circle className="h-5 w-5 text-primary fill-primary" /> : <Circle className="h-5 w-5 text-muted-foreground" />}
              <span className={`text-sm ${i === step ? "font-medium" : "text-muted-foreground"}`}>{s}</span>
            </div>
          </div>
        ))}
      </div>

      {/* Step 1: Select Type */}
      {step === 0 && (
        destConnectors.length === 0 ? (
          <Card className="p-8 text-center">
            <p className="text-muted-foreground">No destination connectors available.</p>
            <p className="text-sm text-muted-foreground mt-1">Create connector definitions first in <a href="/connectors" className="text-primary underline">Connectors</a>.</p>
          </Card>
        ) : (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          {destConnectors.map((d: any) => (
            <Card
              key={d.id}
              className={`transition-all ${d.disabled ? "opacity-50 cursor-not-allowed" : "cursor-pointer hover:shadow-md"} ${destType === d.id ? "ring-2 ring-primary" : ""}`}
              onClick={() => { if (!d.disabled) setDestType(d.id); }}
            >
              <CardContent className="flex flex-col items-center gap-2 p-6">
                <span className="text-3xl">{d.icon}</span>
                <span className="font-medium">{d.name}</span>
                <span className="text-xs text-muted-foreground text-center">{d.description}</span>
                {d.disabled && <Badge variant="secondary">Coming Soon</Badge>}
                {destType === d.id && !d.disabled && <Badge variant="default">Selected</Badge>}
              </CardContent>
            </Card>
          ))}
        </div>
        )
      )}

      {/* Step 2: Configure - PostgreSQL */}
      {step === 1 && (destType === "postgresql" || destType === "postgres") && (
        <Card>
          <CardHeader><CardTitle>Configure PostgreSQL Destination</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">Destination Name *</label>
              <Input value={pgForm.name} onChange={(e) => setPgForm({ ...pgForm, name: e.target.value })} placeholder="e.g. Production Analytics DB" />
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Host *</label>
                <Input value={pgForm.host} onChange={(e) => setPgForm({ ...pgForm, host: e.target.value })} placeholder="localhost" />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Port *</label>
                <Input value={pgForm.port} onChange={(e) => setPgForm({ ...pgForm, port: e.target.value })} placeholder="5432" />
              </div>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Database *</label>
              <Input value={pgForm.database} onChange={(e) => setPgForm({ ...pgForm, database: e.target.value })} placeholder="fusion_dw" />
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Username *</label>
                <Input value={pgForm.username} onChange={(e) => setPgForm({ ...pgForm, username: e.target.value })} placeholder="dw_user" />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Password *</label>
                <Input type="password" value={pgForm.password} onChange={(e) => setPgForm({ ...pgForm, password: e.target.value })} placeholder="••••••" />
              </div>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Schema</label>
              <Input value={pgForm.schema} onChange={(e) => setPgForm({ ...pgForm, schema: e.target.value })} placeholder="public" />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Write Mode</label>
              <div className="space-y-2">
                {([["scd1", "SCD Type 1 (Upsert)", "Latest value wins — overwrites on primary key match"], ["scd2", "SCD Type 2 (History)", "Tracks changes with valid_from/valid_to columns"], ["append", "Append Only", "Inserts all events without deduplication"]] as const).map(([val, label, desc]) => (
                  <label key={val} className={`flex items-start gap-3 rounded-md border p-3 cursor-pointer ${pgForm.write_mode === val ? "border-primary bg-primary/5" : ""}`}>
                    <input type="radio" name="write_mode" value={val} checked={pgForm.write_mode === val} onChange={() => setPgForm({ ...pgForm, write_mode: val })} className="mt-0.5" />
                    <div>
                      <span className="text-sm font-medium">{label}</span>
                      <p className="text-xs text-muted-foreground">{desc}</p>
                    </div>
                  </label>
                ))}
              </div>
            </div>
            <details className="rounded-md border p-3" open={sslOpen} onToggle={(e) => setSslOpen((e.target as HTMLDetailsElement).open)}>
              <summary className="cursor-pointer text-sm font-medium">SSL / TLS Encryption</summary>
              <div className="mt-3 space-y-4">
                <div className="flex items-center gap-2">
                  <input type="checkbox" id="dst_ssl_enabled" checked={pgForm.ssl_enabled}
                    onChange={(e) => setPgForm({ ...pgForm, ssl_enabled: e.target.checked })} />
                  <label htmlFor="dst_ssl_enabled" className="text-sm font-medium">Enable SSL/TLS</label>
                </div>
                {pgForm.ssl_enabled && (
                  <div className="space-y-4 pl-1">
                    <div className="space-y-1">
                      <label className="text-xs font-medium">SSL Mode</label>
                      <select
                        className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-1 focus:ring-ring"
                        value={pgForm.ssl_mode}
                        onChange={(e) => setPgForm({ ...pgForm, ssl_mode: e.target.value as typeof pgForm.ssl_mode })}
                      >
                        <option value="disable">disable — no TLS</option>
                        <option value="allow">allow — prefer plain, fallback to TLS</option>
                        <option value="prefer">prefer — prefer TLS, fallback to plain</option>
                        <option value="require">require — TLS required, skip cert verify</option>
                        <option value="verify-ca">verify-ca — verify server cert against CA</option>
                        <option value="verify-full">verify-full — verify cert + hostname</option>
                      </select>
                    </div>
                    {(pgForm.ssl_mode === "verify-ca" || pgForm.ssl_mode === "verify-full") && (
                      <div className="space-y-1">
                        <label className="text-xs font-medium">CA Certificate (PEM)</label>
                        <textarea className="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono min-h-[80px] resize-y focus:outline-none focus:ring-1 focus:ring-ring"
                          value={pgForm.ssl_ca} onChange={(e) => setPgForm({ ...pgForm, ssl_ca: e.target.value })}
                          placeholder="-----BEGIN CERTIFICATE-----&#10;...&#10;-----END CERTIFICATE-----" />
                      </div>
                    )}
                    <div className="space-y-1">
                      <label className="text-xs font-medium">Client Certificate (PEM) <span className="text-muted-foreground">(optional)</span></label>
                      <textarea className="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono min-h-[80px] resize-y focus:outline-none focus:ring-1 focus:ring-ring"
                        value={pgForm.ssl_cert} onChange={(e) => setPgForm({ ...pgForm, ssl_cert: e.target.value })}
                        placeholder="-----BEGIN CERTIFICATE-----&#10;...&#10;-----END CERTIFICATE-----" />
                    </div>
                    <div className="space-y-1">
                      <label className="text-xs font-medium">Client Private Key (PEM) <span className="text-muted-foreground">(optional)</span></label>
                      <textarea className="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono min-h-[80px] resize-y focus:outline-none focus:ring-1 focus:ring-ring"
                        value={pgForm.ssl_key} onChange={(e) => setPgForm({ ...pgForm, ssl_key: e.target.value })}
                        placeholder="-----BEGIN RSA PRIVATE KEY-----&#10;...&#10;-----END RSA PRIVATE KEY-----" />
                    </div>
                  </div>
                )}
              </div>
            </details>

            <details className="rounded-md border p-3">
              <summary className="cursor-pointer text-sm font-medium">SSH Tunnel (Jump Host / Bastion)</summary>
              <div className="mt-3 space-y-4">
                <div className="flex items-center gap-2">
                  <input type="checkbox" id="dst_ssh_enabled" checked={pgForm.ssh_enabled}
                    onChange={(e) => setPgForm({ ...pgForm, ssh_enabled: e.target.checked })} />
                  <label htmlFor="dst_ssh_enabled" className="text-sm font-medium">Enable SSH Tunnel</label>
                </div>
                {pgForm.ssh_enabled && (
                  <div className="space-y-4 pl-1">
                    <p className="text-xs text-muted-foreground">Connection goes through the SSH bastion. SSL/TLS is configured separately above.</p>
                    <div className="grid grid-cols-3 gap-3">
                      <div className="col-span-2 space-y-1">
                        <label className="text-xs font-medium">Tunnel Host *</label>
                        <Input value={pgForm.tunnel_host} onChange={(e) => setPgForm({ ...pgForm, tunnel_host: e.target.value })} placeholder="bastion.example.com" />
                      </div>
                      <div className="space-y-1">
                        <label className="text-xs font-medium">Port</label>
                        <Input value={pgForm.tunnel_port} onChange={(e) => setPgForm({ ...pgForm, tunnel_port: e.target.value })} placeholder="22" />
                      </div>
                    </div>
                    <div className="space-y-1">
                      <label className="text-xs font-medium">SSH Username *</label>
                      <Input value={pgForm.tunnel_username} onChange={(e) => setPgForm({ ...pgForm, tunnel_username: e.target.value })} placeholder="ubuntu" />
                    </div>
                    <div className="space-y-2">
                      <label className="text-xs font-medium">Authentication</label>
                      <div className="flex gap-4">
                        <label className="flex items-center gap-2 cursor-pointer">
                          <input type="radio" name="dst_tunnel_auth" value="password" checked={pgForm.tunnel_auth_method === "password"} onChange={() => setPgForm({ ...pgForm, tunnel_auth_method: "password" })} />
                          <span className="text-sm">Password</span>
                        </label>
                        <label className="flex items-center gap-2 cursor-pointer">
                          <input type="radio" name="dst_tunnel_auth" value="key" checked={pgForm.tunnel_auth_method === "key"} onChange={() => setPgForm({ ...pgForm, tunnel_auth_method: "key" })} />
                          <span className="text-sm">Private Key</span>
                        </label>
                      </div>
                    </div>
                    {pgForm.tunnel_auth_method === "password" ? (
                      <div className="space-y-1">
                        <label className="text-xs font-medium">SSH Password *</label>
                        <Input type="password" value={pgForm.tunnel_password} onChange={(e) => setPgForm({ ...pgForm, tunnel_password: e.target.value })} placeholder="••••••" />
                      </div>
                    ) : (
                      <div className="space-y-3">
                        <div className="space-y-1">
                          <label className="text-xs font-medium">Private Key (PEM) *</label>
                          <textarea
                            className="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono min-h-[100px] resize-y focus:outline-none focus:ring-1 focus:ring-ring"
                            value={pgForm.tunnel_private_key}
                            onChange={(e) => setPgForm({ ...pgForm, tunnel_private_key: e.target.value })}
                            placeholder={"-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----"}
                          />
                        </div>
                        <div className="space-y-1">
                          <label className="text-xs font-medium">Passphrase <span className="text-muted-foreground">(optional)</span></label>
                          <Input type="password" value={pgForm.tunnel_passphrase} onChange={(e) => setPgForm({ ...pgForm, tunnel_passphrase: e.target.value })} placeholder="Leave blank if no passphrase" />
                        </div>
                      </div>
                    )}
                    {pgForm.tunnel_host && (
                      <div className="space-y-2 pt-1">
                        <Button type="button" size="sm" variant="outline"
                          onClick={() => { setTunnelTestResult(null); tunnelTestMutation.mutate(); }}
                          disabled={tunnelTestMutation.isPending}>
                          {tunnelTestMutation.isPending ? "Testing tunnel…" : "Test SSH Tunnel"}
                        </Button>
                        {tunnelTestResult && (
                          tunnelTestResult.status === "success"
                            ? <p className="text-xs text-green-600">âœ“ {tunnelTestResult.message}</p>
                            : <p className="text-xs text-destructive">âœ— {tunnelTestResult.message}</p>
                        )}
                      </div>
                    )}
                  </div>
                )}
              </div>
            </details>
          </CardContent>
        </Card>
      )}
      {step === 1 && destType === "iceberg" && (
        <Card>
          <CardHeader><CardTitle>Configure Apache Iceberg Destination</CardTitle></CardHeader>
          <CardContent className="space-y-6">
            <div className="space-y-2">
              <label className="text-sm font-medium">Destination Name *</label>
              <Input value={icebergDestName} onChange={(e) => setIcebergDestName(e.target.value)} placeholder="e.g. Data Lake - Iceberg" />
            </div>
            <IcebergDestinationForm form={icebergForm} setForm={setIcebergForm} />
          </CardContent>
        </Card>
      )}

      {/* Step 3: Test Connection */}
      {step === 2 && (
        <Card>
          <CardHeader><CardTitle>Test Connection</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-3">
              {testChecks.length > 0 ? (
                testChecks.map((check) => (
                  <div key={check.label} className="flex items-center gap-3 rounded-md border p-3">
                    {checkIcon(check.status)}
                    <div>
                      <span className="text-sm font-medium">{check.label}</span>
                      {check.message && <p className="text-xs text-muted-foreground">{check.message}</p>}
                    </div>
                  </div>
                ))
              ) : (
                <p className="text-sm text-muted-foreground">Click "Run Test" to verify your destination connection.</p>
              )}
            </div>
            <div className="flex gap-2">
              <Button onClick={() => testMutation.mutate()} disabled={testMutation.isPending}>
                {testMutation.isPending ? <><Loader2 className="mr-2 h-4 w-4 animate-spin" />Testing...</> : "Run Test"}
              </Button>
              <Button variant="outline" onClick={() => navigate(`/destinations/${destId}`)}>Finish Setup</Button>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Navigation */}
      <div className="flex justify-between">
        <Button variant="outline" onClick={() => step > 0 ? setStep(step - 1) : navigate("/destinations")}>
          {step === 0 ? "Cancel" : "← Back"}
        </Button>
        {step < 2 && (
          <Button onClick={() => { if (step === 0) setStep(1); else if (step === 1) createMutation.mutate(); }} disabled={!canProceed() || createMutation.isPending}>
            {createMutation.isPending ? <><Loader2 className="mr-2 h-4 w-4 animate-spin" />Creating...</> : "Next →"}
          </Button>
        )}
      </div>
    </div>
  );
}
