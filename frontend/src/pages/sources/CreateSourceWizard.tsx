import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useMutation, useQuery } from "@tanstack/react-query";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { CheckCircle, Circle, Loader2, Plug, Plus, X } from "lucide-react";

const STEPS = ["Select Connector", "Configure", "Test Connection", "Discover Tables"];

export function CreateSourceWizard() {
  const navigate = useNavigate();
  const [step, setStep] = useState(0);
  const [connectorType, setConnectorType] = useState("");
  const [form, setForm] = useState({
    name: "",
    host: "",
    port: "3306",
    database: "",
    username: "",
    password: "",
    // SSL/TLS (separate from SSH tunnel)
    ssl_enabled: false,
    ssl_mode: "require" as "disable" | "allow" | "prefer" | "require" | "verify-ca" | "verify-full",
    ssl_ca: "",
    ssl_cert: "",
    ssl_key: "",
    // SSH Tunnel (separate from SSL)
    ssh_enabled: false,
    tunnel_host: "",
    tunnel_port: "22",
    tunnel_username: "",
    tunnel_auth_method: "password" as "password" | "key",
    tunnel_password: "",
    tunnel_private_key: "",
    tunnel_passphrase: "",
    // Batch ingestion / Spark JDBC config
    cursor_field: "",
    primary_key: "",
    fetch_size: "10000",
    num_partitions: "4",
    partition_column: "",
    lower_bound: "",
    upper_bound: "",
    // MongoDB-specific
    auth_source: "admin",
    replica_set: "",
    read_preference: "",
    extra_uri_params: "",
    sample_size: "500",
  });
  const [mongoExtraHosts, setMongoExtraHosts] = useState<Array<{host: string; port: string}>>([]);
  const [batchOpen, setBatchOpen] = useState(false);
  const [sslOpen, setSslOpen] = useState(false);
  const [sourceId, setSourceId] = useState<string | null>(null);
  const [testResult, setTestResult] = useState<any>(null);
  const [tunnelTestResult, setTunnelTestResult] = useState<any>(null);
  const [tables, setTables] = useState<any[]>([]);
  const [selectedTables, setSelectedTables] = useState<string[]>([]);

  const { data: connectors = [] } = useQuery({
    queryKey: ["connector-definitions"],
    queryFn: () => fetchList("/connector-definitions", "connectors"),
  });

  const selectedConnector = connectors.find((c: any) => (c.connector_type ?? c.type) === connectorType);

  const createMutation = useMutation({
    mutationFn: () => api.post("/sources", {
      source_name: form.name,
      connector_definition_id: selectedConnector?.connector_id ?? selectedConnector?.id,
      connector_version: selectedConnector?.latest_version ?? "1.0.0",
      host: form.host,
      port: parseInt(form.port),
      database_name: form.database,
      username: form.username,
      password: form.password,
      ssl_enabled: form.ssl_enabled,
      ssl_config: form.ssl_enabled ? {
        ssl_mode: form.ssl_mode,
        ssl_ca: form.ssl_ca || undefined,
        ssl_cert: form.ssl_cert || undefined,
        ssl_key: form.ssl_key || undefined,
      } : {},
      ssh_config: form.ssh_enabled ? {
        tunnel_host: form.tunnel_host || undefined,
        tunnel_port: form.tunnel_port ? parseInt(form.tunnel_port) : 22,
        tunnel_username: form.tunnel_username || undefined,
        tunnel_auth_method: form.tunnel_auth_method,
        tunnel_password: form.tunnel_auth_method === "password" ? form.tunnel_password || undefined : undefined,
        tunnel_private_key: form.tunnel_auth_method === "key" ? form.tunnel_private_key || undefined : undefined,
        tunnel_passphrase: form.tunnel_auth_method === "key" ? form.tunnel_passphrase || undefined : undefined,
      } : {},
      config: {
        cursor_field: form.cursor_field || undefined,
        primary_key: form.primary_key || undefined,
        fetch_size: form.fetch_size ? parseInt(form.fetch_size) : undefined,
        num_partitions: form.num_partitions ? parseInt(form.num_partitions) : undefined,
        partition_column: form.partition_column || undefined,
        lower_bound: form.lower_bound || undefined,
        upper_bound: form.upper_bound || undefined,
        ...(connectorType === "mongodb" ? {
          auth_source: form.auth_source || "admin",
          replica_set: form.replica_set || undefined,
          read_preference: form.read_preference || undefined,
          extra_hosts: mongoExtraHosts.filter(h => h.host.trim()).map(h => `${h.host.trim()}:${h.port || "27017"}`).join(",") || undefined,
          extra_uri_params: form.extra_uri_params || undefined,
          sample_size: form.sample_size ? parseInt(form.sample_size) : 500,
        } : {}),
      },
    }),
    onSuccess: (res) => {
      setSourceId(res.data.source_id ?? res.data.id);
      setStep(2);
    },
  });

  const testMutation = useMutation({
    mutationFn: () => api.post(`/sources/${sourceId}/test-connection`),
    onSuccess: (res) => setTestResult(res.data),
  });

  // Test only the SSH tunnel
  const tunnelTestMutation = useMutation({
    mutationFn: () => {
      if (sourceId) {
        return api.post(`/sources/${sourceId}/test-tunnel`);
      }
      return api.post("/sources/test-tunnel", {
        ssh_config: {
          tunnel_host: form.tunnel_host,
          tunnel_port: form.tunnel_port ? parseInt(form.tunnel_port) : 22,
          tunnel_username: form.tunnel_username,
          tunnel_auth_method: form.tunnel_auth_method,
          tunnel_password: form.tunnel_auth_method === "password" ? form.tunnel_password : undefined,
          tunnel_private_key: form.tunnel_auth_method === "key" ? form.tunnel_private_key : undefined,
          tunnel_passphrase: form.tunnel_auth_method === "key" ? form.tunnel_passphrase : undefined,
        },
      });
    },
    onSuccess: (res) => setTunnelTestResult(res.data),
    onError: (err: any) => setTunnelTestResult({ status: "failure", message: err.response?.data?.detail ?? err.message }),
  });

  const discoverMutation = useMutation({
    mutationFn: () => api.post(`/sources/${sourceId}/discover-schemas`),
    onSuccess: (res) => {
      const data = res.data;
      if (Array.isArray(data?.tables)) {
        setTables(data.tables);
      } else if (Array.isArray(data?.schemas)) {
        const flat = data.schemas.flatMap((s: any) => (s.tables ?? []).map((t: any) => ({ ...t, schema_name: s.schema_name, name: t.table_name ?? t.name })));
        setTables(flat);
      } else if (Array.isArray(data)) {
        setTables(data);
      } else {
        setTables([]);
      }
    },
  });

  const sourceConnectors = connectors
    .filter((c: any) => c.category === "source" && c.is_active !== false)
    .map((c: any) => ({
      id: c.connector_id ?? c.id,
      name: c.connector_name ?? c.name,
      type: c.connector_type ?? c.type,
      description: [
        c.supports_cdc && "CDC",
        c.supports_full_refresh && "Full Refresh",
        c.supports_incremental && "Incremental",
      ].filter(Boolean).join(" · ") || "Full Refresh",
    }));

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Create New Source</h1>

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

      {/* Step 1: Select Connector */}
      {step === 0 && (
        sourceConnectors.length === 0 ? (
          <Card className="p-8 text-center">
            <p className="text-muted-foreground">No source connectors available.</p>
            <p className="text-sm text-muted-foreground mt-1">Create connector definitions first in <a href="/connectors" className="text-primary underline">Connectors</a>.</p>
          </Card>
        ) : (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          {sourceConnectors.map((c: any) => (
            <Card
              key={c.id ?? c.type}
              className={`cursor-pointer transition-all hover:shadow-md ${connectorType === (c.type ?? c.id) ? "ring-2 ring-primary" : ""}`}
              onClick={() => setConnectorType(c.type ?? c.id)}
            >
              <CardContent className="flex flex-col items-center gap-2 p-6">
                <Plug className="h-8 w-8 text-primary" />
                <span className="font-medium">{c.name}</span>
                <span className="text-xs text-muted-foreground">{c.description}</span>
                {connectorType === (c.type ?? c.id) && <Badge variant="default">Selected</Badge>}
              </CardContent>
            </Card>
          ))}
        </div>
        )
      )}

      {/* Step 2: Configure */}
      {step === 1 && (
        <Card>
          <CardHeader><CardTitle>Configure {connectorType} Connection</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">Source Name *</label>
              <Input value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} placeholder="My Source" required />
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Host *</label>
                <Input value={form.host} onChange={(e) => setForm({ ...form, host: e.target.value })} required />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Port *</label>
                <Input value={form.port} onChange={(e) => setForm({ ...form, port: e.target.value })} required />
              </div>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Database *</label>
              <Input value={form.database} onChange={(e) => setForm({ ...form, database: e.target.value })} required />
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Username *</label>
                <Input value={form.username} onChange={(e) => setForm({ ...form, username: e.target.value })} required />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Password *</label>
                <Input type="password" value={form.password} onChange={(e) => setForm({ ...form, password: e.target.value })} required />
              </div>
            </div>
            {connectorType === "mongodb" && (
              <details className="rounded-md border p-3" open>
                <summary className="cursor-pointer text-sm font-medium">MongoDB Options</summary>
                <div className="mt-3 space-y-4">
                  {/* Auth Source + Replica Set name */}
                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-1">
                      <label className="text-xs font-medium">Auth Source</label>
                      <Input value={form.auth_source} onChange={(e) => setForm({ ...form, auth_source: e.target.value })} placeholder="admin" />
                      <p className="text-xs text-muted-foreground">DB where the user is defined (usually "admin").</p>
                    </div>
                    <div className="space-y-1">
                      <label className="text-xs font-medium">Replica Set Name <span className="text-muted-foreground">(optional)</span></label>
                      <Input value={form.replica_set} onChange={(e) => setForm({ ...form, replica_set: e.target.value })} placeholder="rs0" />
                      <p className="text-xs text-muted-foreground">Leave blank for standalone.</p>
                    </div>
                  </div>
                  {/* Read Preference */}
                  <div className="space-y-1">
                    <label className="text-xs font-medium">Read Preference</label>
                    <select
                      className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-1 focus:ring-ring"
                      value={form.read_preference}
                      onChange={(e) => setForm({ ...form, read_preference: e.target.value })}
                    >
                      <option value="">primary (default)</option>
                      <option value="primaryPreferred">primaryPreferred</option>
                      <option value="secondary">secondary</option>
                      <option value="secondaryPreferred">secondaryPreferred</option>
                      <option value="nearest">nearest</option>
                    </select>
                    <p className="text-xs text-muted-foreground">Which replica to route reads to.</p>
                  </div>
                  {/* Extra replica set members */}
                  <div className="space-y-2">
                    <label className="text-xs font-medium">Additional Replica Set Members <span className="text-muted-foreground">(optional)</span></label>
                    <p className="text-xs text-muted-foreground">
                      The <strong>Host / Port</strong> above is the primary connection target (and SSH tunnel endpoint).
                      Add other replica members here for direct (non-tunneled) seed-list connections.
                    </p>
                    {mongoExtraHosts.map((h, i) => (
                      <div key={i} className="flex items-center gap-2">
                        <Input
                          className="flex-1"
                          value={h.host}
                          onChange={(e) => {
                            const updated = [...mongoExtraHosts];
                            updated[i] = { ...updated[i], host: e.target.value };
                            setMongoExtraHosts(updated);
                          }}
                          placeholder="10.1.2.3"
                        />
                        <Input
                          className="w-28"
                          value={h.port}
                          onChange={(e) => {
                            const updated = [...mongoExtraHosts];
                            updated[i] = { ...updated[i], port: e.target.value };
                            setMongoExtraHosts(updated);
                          }}
                          placeholder="27017"
                        />
                        <Button
                          type="button" size="sm" variant="ghost"
                          onClick={() => setMongoExtraHosts(mongoExtraHosts.filter((_, j) => j !== i))}
                        >
                          <X className="h-4 w-4" />
                        </Button>
                      </div>
                    ))}
                    <Button
                      type="button" size="sm" variant="outline"
                      onClick={() => setMongoExtraHosts([...mongoExtraHosts, { host: "", port: "27017" }])}
                    >
                      <Plus className="h-4 w-4 mr-1" /> Add Member
                    </Button>
                  </div>
                  {/* Extra URI params */}
                  <div className="space-y-1">
                    <label className="text-xs font-medium">Additional URI Parameters <span className="text-muted-foreground">(optional)</span></label>
                    <Input
                      value={form.extra_uri_params}
                      onChange={(e) => setForm({ ...form, extra_uri_params: e.target.value })}
                      placeholder="connectTimeoutMS=10000&socketTimeoutMS=30000"
                    />
                    <p className="text-xs text-muted-foreground">Raw key=value pairs appended to the connection URI. Separate multiple with &amp;.</p>
                  </div>
                  {/* Schema discovery sample size */}
                  <div className="space-y-1">
                    <label className="text-xs font-medium">Schema Discovery Sample Size</label>
                    <Input
                      type="number"
                      min={50}
                      max={10000}
                      value={form.sample_size}
                      onChange={(e) => setForm({ ...form, sample_size: e.target.value })}
                      placeholder="500"
                    />
                    <p className="text-xs text-muted-foreground">
                      Number of documents sampled per collection to infer field types (50 – 10,000).
                      Uses <code>$sample</code> aggregation — fast even on large collections.
                    </p>
                  </div>
                </div>
              </details>
            )}
            <details className="rounded-md border p-3" open={sslOpen} onToggle={(e) => setSslOpen((e.target as HTMLDetailsElement).open)}>
              <summary className="cursor-pointer text-sm font-medium">SSL / TLS Encryption</summary>
              <div className="mt-3 space-y-4">
                <div className="flex items-center gap-2">
                  <input type="checkbox" id="src_ssl_enabled" checked={form.ssl_enabled}
                    onChange={(e) => setForm({ ...form, ssl_enabled: e.target.checked })} />
                  <label htmlFor="src_ssl_enabled" className="text-sm font-medium">Enable SSL/TLS</label>
                </div>
                {form.ssl_enabled && (
                  <div className="space-y-4 pl-1">
                    <div className="space-y-1">
                      <label className="text-xs font-medium">SSL Mode</label>
                      <select
                        className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-1 focus:ring-ring"
                        value={form.ssl_mode}
                        onChange={(e) => setForm({ ...form, ssl_mode: e.target.value as typeof form.ssl_mode })}
                      >
                        <option value="disable">disable — no TLS</option>
                        <option value="allow">allow — prefer plain, fallback to TLS</option>
                        <option value="prefer">prefer — prefer TLS, fallback to plain</option>
                        <option value="require">require — TLS required, skip cert verify</option>
                        <option value="verify-ca">verify-ca — verify server cert against CA</option>
                        <option value="verify-full">verify-full — verify cert + hostname</option>
                      </select>
                    </div>
                    {(form.ssl_mode === "verify-ca" || form.ssl_mode === "verify-full") && (
                      <div className="space-y-1">
                        <label className="text-xs font-medium">CA Certificate (PEM)</label>
                        <textarea className="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono min-h-[80px] resize-y focus:outline-none focus:ring-1 focus:ring-ring"
                          value={form.ssl_ca} onChange={(e) => setForm({ ...form, ssl_ca: e.target.value })}
                          placeholder="-----BEGIN CERTIFICATE-----&#10;...&#10;-----END CERTIFICATE-----" />
                      </div>
                    )}
                    <div className="space-y-1">
                      <label className="text-xs font-medium">Client Certificate (PEM) <span className="text-muted-foreground">(optional)</span></label>
                      <textarea className="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono min-h-[80px] resize-y focus:outline-none focus:ring-1 focus:ring-ring"
                        value={form.ssl_cert} onChange={(e) => setForm({ ...form, ssl_cert: e.target.value })}
                        placeholder="-----BEGIN CERTIFICATE-----&#10;...&#10;-----END CERTIFICATE-----" />
                    </div>
                    <div className="space-y-1">
                      <label className="text-xs font-medium">Client Private Key (PEM) <span className="text-muted-foreground">(optional)</span></label>
                      <textarea className="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono min-h-[80px] resize-y focus:outline-none focus:ring-1 focus:ring-ring"
                        value={form.ssl_key} onChange={(e) => setForm({ ...form, ssl_key: e.target.value })}
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
                  <input type="checkbox" id="src_ssh_enabled" checked={form.ssh_enabled}
                    onChange={(e) => setForm({ ...form, ssh_enabled: e.target.checked })} />
                  <label htmlFor="src_ssh_enabled" className="text-sm font-medium">Enable SSH Tunnel</label>
                </div>
                {form.ssh_enabled && (
                  <div className="space-y-4 pl-1">
                    <p className="text-xs text-muted-foreground">The database connection is established through the SSH bastion. SSL/TLS is a separate option above.</p>
                    <div className="grid grid-cols-3 gap-3">
                      <div className="col-span-2 space-y-1">
                        <label className="text-xs font-medium">Tunnel Host *</label>
                        <Input value={form.tunnel_host} onChange={(e) => setForm({ ...form, tunnel_host: e.target.value })} placeholder="bastion.example.com" />
                      </div>
                      <div className="space-y-1">
                        <label className="text-xs font-medium">Port</label>
                        <Input value={form.tunnel_port} onChange={(e) => setForm({ ...form, tunnel_port: e.target.value })} placeholder="22" />
                      </div>
                    </div>
                    <div className="space-y-1">
                      <label className="text-xs font-medium">SSH Username *</label>
                      <Input value={form.tunnel_username} onChange={(e) => setForm({ ...form, tunnel_username: e.target.value })} placeholder="ubuntu" />
                    </div>
                    <div className="space-y-2">
                      <label className="text-xs font-medium">Authentication</label>
                      <div className="flex gap-4">
                        <label className="flex items-center gap-2 cursor-pointer">
                          <input type="radio" name="src_tunnel_auth" value="password" checked={form.tunnel_auth_method === "password"} onChange={() => setForm({ ...form, tunnel_auth_method: "password" })} />
                          <span className="text-sm">Password</span>
                        </label>
                        <label className="flex items-center gap-2 cursor-pointer">
                          <input type="radio" name="src_tunnel_auth" value="key" checked={form.tunnel_auth_method === "key"} onChange={() => setForm({ ...form, tunnel_auth_method: "key" })} />
                          <span className="text-sm">Private Key</span>
                        </label>
                      </div>
                    </div>
                    {form.tunnel_auth_method === "password" ? (
                      <div className="space-y-1">
                        <label className="text-xs font-medium">SSH Password *</label>
                        <Input type="password" value={form.tunnel_password} onChange={(e) => setForm({ ...form, tunnel_password: e.target.value })} placeholder="••••••" />
                      </div>
                    ) : (
                      <div className="space-y-3">
                        <div className="space-y-1">
                          <label className="text-xs font-medium">Private Key (PEM) *</label>
                          <textarea className="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono min-h-[100px] resize-y focus:outline-none focus:ring-1 focus:ring-ring"
                            value={form.tunnel_private_key} onChange={(e) => setForm({ ...form, tunnel_private_key: e.target.value })}
                            placeholder="-----BEGIN RSA PRIVATE KEY-----&#10;...&#10;-----END RSA PRIVATE KEY-----" />
                        </div>
                        <div className="space-y-1">
                          <label className="text-xs font-medium">Passphrase <span className="text-muted-foreground">(optional)</span></label>
                          <Input type="password" value={form.tunnel_passphrase} onChange={(e) => setForm({ ...form, tunnel_passphrase: e.target.value })} placeholder="Leave blank if no passphrase" />
                        </div>
                      </div>
                    )}
                    {form.tunnel_host && (
                      <div className="space-y-2 pt-1">
                        <Button type="button" size="sm" variant="outline"
                          onClick={() => { setTunnelTestResult(null); tunnelTestMutation.mutate(); }}
                          disabled={tunnelTestMutation.isPending}>
                          {tunnelTestMutation.isPending ? "Testing tunnel…" : "Test SSH Tunnel"}
                        </Button>
                        {tunnelTestResult && (
                          tunnelTestResult.status === "success"
                            ? <p className="text-xs text-green-600">✓ {tunnelTestResult.message}</p>
                            : <p className="text-xs text-destructive">✗ {tunnelTestResult.message}</p>
                        )}
                      </div>
                    )}
                  </div>
                )}
              </div>
            </details>

            {/* Batch / Spark Ingestion Settings */}
            <details className="rounded-md border p-3" open={batchOpen} onToggle={(e) => setBatchOpen((e.target as HTMLDetailsElement).open)}>
              <summary className="cursor-pointer text-sm font-medium">Batch &amp; Spark Ingestion Settings</summary>
              <div className="mt-4 space-y-4">
                <p className="text-xs text-muted-foreground">Configure how Spark reads this source for batch (initial load + incremental) ingestion jobs.</p>
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Cursor Field</label>
                    <Input value={form.cursor_field} onChange={(e) => setForm({ ...form, cursor_field: e.target.value })} placeholder="updated_at" />
                    <p className="text-xs text-muted-foreground">Column used for incremental loads (e.g. updated_at, modified_date).</p>
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Primary Key</label>
                    <Input value={form.primary_key} onChange={(e) => setForm({ ...form, primary_key: e.target.value })} placeholder="id" />
                    <p className="text-xs text-muted-foreground">Used for upsert / staging merge on the destination.</p>
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <label className="text-sm font-medium">JDBC Fetch Size</label>
                    <Input value={form.fetch_size} onChange={(e) => setForm({ ...form, fetch_size: e.target.value })} type="number" placeholder="10000" />
                    <p className="text-xs text-muted-foreground">Number of rows fetched per JDBC round-trip.</p>
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Num Partitions</label>
                    <Input value={form.num_partitions} onChange={(e) => setForm({ ...form, num_partitions: e.target.value })} type="number" placeholder="4" />
                    <p className="text-xs text-muted-foreground">Parallel JDBC read tasks (Spark parallelism).</p>
                  </div>
                </div>
                <div className="space-y-2">
                  <label className="text-sm font-medium">Partition Column</label>
                  <Input value={form.partition_column} onChange={(e) => setForm({ ...form, partition_column: e.target.value })} placeholder="id" />
                  <p className="text-xs text-muted-foreground">Numeric column used to split JDBC reads across partitions.</p>
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Lower Bound</label>
                    <Input value={form.lower_bound} onChange={(e) => setForm({ ...form, lower_bound: e.target.value })} placeholder="0" />
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Upper Bound</label>
                    <Input value={form.upper_bound} onChange={(e) => setForm({ ...form, upper_bound: e.target.value })} placeholder="1000000" />
                  </div>
                </div>
              </div>
            </details>
          </CardContent>
        </Card>
      )}

      {/* Step 3: Test */}
      {step === 2 && (
        <Card>
          <CardHeader><CardTitle>Test Connectivity</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            {testMutation.isPending && <div className="flex items-center gap-2"><Loader2 className="h-4 w-4 animate-spin" /> Testing connection...</div>}
            {testResult && (
              <div className="space-y-2">
                {testResult.status === "success" ? (
                  <div className="rounded-md bg-success/10 p-4 text-sm text-success">✓ Connection successful</div>
                ) : (
                  <div className="rounded-md bg-destructive/10 p-4 text-sm text-destructive">✗ Connection failed: {testResult.message ?? testResult.error_details ?? testResult.error ?? "Unknown error"}</div>
                )}
              </div>
            )}
            <Button onClick={() => testMutation.mutate()} disabled={testMutation.isPending}>
              {testResult ? "Test Again" : "Run Test"}
            </Button>
          </CardContent>
        </Card>
      )}

      {/* Step 4: Discover */}
      {step === 3 && (
        <Card>
          <CardHeader><CardTitle>Select Tables to Sync</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            {tables.length === 0 ? (
              <div className="space-y-2">
                <p className="text-sm text-muted-foreground">Click discover to find available tables.</p>
                <Button onClick={() => discoverMutation.mutate()} disabled={discoverMutation.isPending}>
                  {discoverMutation.isPending ? "Discovering..." : "Discover Schemas"}
                </Button>
              </div>
            ) : (
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">{selectedTables.length} of {tables.length} tables selected</span>
                  <Button variant="outline" size="sm" onClick={() => setSelectedTables(tables.map((t: any) => t.name ?? t))}>Select All</Button>
                </div>
                <div className="max-h-64 overflow-auto rounded-md border">
                  {tables.map((t: any) => {
                    const name = t.name ?? t;
                    return (
                      <label key={name} className="flex items-center gap-3 border-b px-4 py-2 last:border-0 hover:bg-muted/50 cursor-pointer">
                        <input type="checkbox" checked={selectedTables.includes(name)} onChange={(e) => setSelectedTables(e.target.checked ? [...selectedTables, name] : selectedTables.filter((n) => n !== name))} />
                        <span className="font-mono text-sm">{name}</span>
                        {t.row_count && <span className="ml-auto text-xs text-muted-foreground">{t.row_count.toLocaleString()} rows</span>}
                      </label>
                    );
                  })}
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Navigation */}
      <div className="flex justify-between">
        <Button variant="outline" onClick={() => step > 0 ? setStep(step - 1) : navigate("/sources")}>
          {step === 0 ? "Cancel" : "← Back"}
        </Button>
        <Button
          onClick={() => {
            if (step === 0 && connectorType) setStep(1);
            else if (step === 1) {
              if (!form.name.trim()) {
                alert("Source Name is required.");
                return;
              }
              if (!form.host.trim() || !form.database.trim() || !form.username.trim()) {
                alert("Host, Database, and Username are required.");
                return;
              }
              createMutation.mutate();
            }
            else if (step === 2) { setStep(3); }
            else if (step === 3) navigate(`/sources/${sourceId}`);
          }}
          disabled={(step === 0 && !connectorType) || (step === 1 && createMutation.isPending)}
        >
          {step === 3 ? "Finish" : step === 1 && createMutation.isPending ? "Creating..." : "Next →"}
        </Button>
      </div>
    </div>
  );
}
