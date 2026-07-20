import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useState, useEffect } from "react";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { ChevronDown, ChevronRight, TestTube, Radar } from "lucide-react";

export function EditSourcePage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [sshOpen, setSshOpen] = useState(false);
  const [sslOpen, setSslOpen] = useState(false);
  const [poolOpen, setPoolOpen] = useState(false);

  const [form, setForm] = useState({
    name: "",
    host: "",
    port: "",
    database: "",
    username: "",
    password: "",
    ssl_enabled: false,
    // CDC-specific per connector type
    // PostgreSQL
    publication: "",
    replication_slot: "",
    replication_plugin: "pgoutput",
    // MySQL
    server_id: "",
    // MongoDB
    auth_source: "admin",
    auth_mechanism: "SCRAM-SHA-256",
    // SSH Tunnel
    ssh_enabled: false,
    tunnel_host: "",
    tunnel_port: "22",
    tunnel_username: "",
    tunnel_auth_method: "key",
    tunnel_private_key: "",
    tunnel_password: "",
    // SSL
    ssl_ca_cert: "",
    ssl_client_cert: "",
    ssl_client_key: "",
    // Pool
    connection_pool_min: "",
    connection_pool_max: "",
  });

  const { data: source, isLoading } = useQuery({
    queryKey: ["sources", id],
    queryFn: () => api.get(`/sources/${id}`).then((r) => r.data),
  });

  // Streams attached to connections that use this source
  const { data: connections = [] } = useQuery({
    queryKey: ["connections"],
    queryFn: () => fetchList("/connections", "connections"),
  });

  const connectorType: string = source?.connector_definition_type ?? "";
  const isPg = connectorType === "postgresql" || connectorType === "postgres";
  const isMysql = connectorType === "mysql";
  const isMongo = connectorType === "mongodb";

  useEffect(() => {
    if (!source) return;
    const cfg = source.config ?? {};
    const ssh = source.ssh_config ?? {};
    const ssl = source.ssl_config ?? {};
    setForm({
      name: source.source_name ?? "",
      host: source.host ?? "",
      port: String(source.port ?? ""),
      database: source.database_name ?? "",
      username: source.username ?? "",
      password: "",
      ssl_enabled: source.ssl_enabled ?? false,
      // PostgreSQL CDC
      publication: cfg.publication ?? "",
      replication_slot: cfg.replication_slot ?? "",
      replication_plugin: cfg.replication_plugin ?? "pgoutput",
      // MySQL CDC
      server_id: String(cfg.server_id ?? ""),
      // MongoDB
      auth_source: cfg.auth_source ?? "admin",
      auth_mechanism: cfg.auth_mechanism ?? "SCRAM-SHA-256",
      // SSH
      ssh_enabled: !!ssh.tunnel_host,
      tunnel_host: ssh.tunnel_host ?? "",
      tunnel_port: String(ssh.tunnel_port ?? "22"),
      tunnel_username: ssh.tunnel_username ?? "",
      tunnel_auth_method: ssh.tunnel_auth_method ?? "key",
      tunnel_private_key: ssh.tunnel_private_key ?? "",
      tunnel_password: "",
      // SSL
      ssl_ca_cert: ssl.ca_cert ?? "",
      ssl_client_cert: ssl.client_cert ?? "",
      ssl_client_key: ssl.client_key ?? "",
      // Pool
      connection_pool_min: String(cfg.connection_pool_min ?? ""),
      connection_pool_max: String(cfg.connection_pool_max ?? ""),
    });
  }, [source]);

  const updateMutation = useMutation({
    mutationFn: () =>
      api.patch(`/sources/${id}`, {
        source_name: form.name,
        host: form.host,
        port: parseInt(form.port) || undefined,
        database_name: form.database,
        username: form.username,
        password: form.password || undefined,
        ssl_enabled: form.ssl_enabled,
        ssl_config: form.ssl_enabled ? {
          ca_cert: form.ssl_ca_cert || undefined,
          client_cert: form.ssl_client_cert || undefined,
          client_key: form.ssl_client_key || undefined,
        } : {},
        ssh_config: form.ssh_enabled ? {
          tunnel_host: form.tunnel_host,
          tunnel_port: parseInt(form.tunnel_port) || 22,
          tunnel_username: form.tunnel_username,
          tunnel_auth_method: form.tunnel_auth_method,
          tunnel_private_key: form.tunnel_auth_method === "key" ? (form.tunnel_private_key || undefined) : undefined,
          tunnel_password: form.tunnel_auth_method === "password" ? (form.tunnel_password || undefined) : undefined,
        } : {},
        config: {
          ...(isPg ? {
            publication: form.publication || undefined,
            replication_slot: form.replication_slot || undefined,
            replication_plugin: form.replication_plugin,
          } : {}),
          ...(isMysql ? {
            server_id: form.server_id ? parseInt(form.server_id) : undefined,
          } : {}),
          ...(isMongo ? {
            auth_source: form.auth_source,
            auth_mechanism: form.auth_mechanism,
          } : {}),
          connection_pool_min: form.connection_pool_min ? parseInt(form.connection_pool_min) : undefined,
          connection_pool_max: form.connection_pool_max ? parseInt(form.connection_pool_max) : undefined,
        },
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["sources", id] });
      navigate(`/sources/${id}`);
    },
  });

  const testMutation = useMutation({
    mutationFn: () => api.post(`/sources/${id}/test-connection`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["sources", id] }),
  });

  const discoverMutation = useMutation({
    mutationFn: () => api.post(`/sources/${id}/discover-schemas`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["sources", id] }),
  });

  if (isLoading) return <div className="flex items-center justify-center h-64">Loading...</div>;
  if (!source) return <div className="text-center text-muted-foreground">Source not found</div>;

  // Find connections that use this source
  const relatedConnections = (connections as any[]).filter(
    (c: any) => c.source_id === id || c.source?.source_id === id
  );

  return (
    <div className="space-y-6 max-w-4xl">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Edit Source: {source.source_name}</h1>
        <div className="flex gap-2">
          <Button variant="outline" onClick={() => testMutation.mutate()} disabled={testMutation.isPending}>
            <TestTube className="mr-2 h-4 w-4" />
            {testMutation.isPending ? "Testing..." : "Test Connection"}
          </Button>
          <Button variant="outline" onClick={() => discoverMutation.mutate()} disabled={discoverMutation.isPending}>
            <Radar className="mr-2 h-4 w-4" />
            {discoverMutation.isPending ? "Discovering..." : "Discover Schemas"}
          </Button>
        </div>
      </div>

      {testMutation.isSuccess && (
        <div className="p-3 rounded-md bg-green-500/10 border border-green-200 text-sm text-green-700">
          ✓ Connection test successful!
        </div>
      )}
      {testMutation.isError && (
        <div className="p-3 rounded-md bg-destructive/10 border border-destructive/30 text-sm text-destructive">
          ✗ Connection test failed. Check credentials and network.
        </div>
      )}

      <Tabs defaultValue="connection">
        <TabsList>
          <TabsTrigger value="connection">Connection Settings</TabsTrigger>
          <TabsTrigger value="cdc">CDC Settings</TabsTrigger>
          <TabsTrigger value="streams">Tables & Streams ({relatedConnections.length} connections)</TabsTrigger>
        </TabsList>

        {/* ── Connection Settings tab ─────────────────────────────── */}
        <TabsContent value="connection">
          <Card>
            <CardHeader><CardTitle>Connection Configuration</CardTitle></CardHeader>
            <CardContent>
              <form
                onSubmit={(e) => { e.preventDefault(); updateMutation.mutate(); }}
                className="space-y-4"
              >
                <div className="space-y-2">
                  <label className="text-sm font-medium">Source Name *</label>
                  <Input value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} required />
                </div>

                <div className="grid grid-cols-3 gap-4">
                  <div className="col-span-2 space-y-2">
                    <label className="text-sm font-medium">Host *</label>
                    <Input value={form.host} onChange={(e) => setForm({ ...form, host: e.target.value })} required />
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Port *</label>
                    <Input value={form.port} onChange={(e) => setForm({ ...form, port: e.target.value })} type="number" required />
                  </div>
                </div>

                <div className="space-y-2">
                  <label className="text-sm font-medium">{isMongo ? "Database / Auth DB" : "Database *"}</label>
                  <Input value={form.database} onChange={(e) => setForm({ ...form, database: e.target.value })} />
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Username</label>
                    <Input value={form.username} onChange={(e) => setForm({ ...form, username: e.target.value })} />
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Password</label>
                    <Input
                      value={form.password}
                      onChange={(e) => setForm({ ...form, password: e.target.value })}
                      type="password"
                      placeholder="leave blank to keep current"
                    />
                  </div>
                </div>

                <div className="flex items-center gap-2">
                  <input type="checkbox" id="ssl-toggle" checked={form.ssl_enabled}
                    onChange={(e) => setForm({ ...form, ssl_enabled: e.target.checked })}
                    className="h-4 w-4 rounded" />
                  <label htmlFor="ssl-toggle" className="text-sm font-medium">Enable SSL/TLS</label>
                </div>

                {/* SSL certificates */}
                {form.ssl_enabled && (
                  <div className="border rounded-md">
                    <button type="button" className="flex items-center gap-2 w-full p-3 text-sm font-medium text-left hover:bg-muted/50"
                      onClick={() => setSslOpen(!sslOpen)}>
                      {sslOpen ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                      SSL Certificates
                    </button>
                    {sslOpen && (
                      <div className="p-4 pt-0 space-y-3 border-t">
                        {[
                          { label: "CA Certificate", field: "ssl_ca_cert" },
                          { label: "Client Certificate", field: "ssl_client_cert" },
                          { label: "Client Key", field: "ssl_client_key" },
                        ].map(({ label, field }) => (
                          <div key={field} className="space-y-1">
                            <label className="text-sm font-medium">{label}</label>
                            <textarea className="w-full rounded-md border p-2 text-xs font-mono min-h-[70px] resize-y"
                              value={(form as any)[field]}
                              onChange={(e) => setForm({ ...form, [field]: e.target.value })}
                              placeholder="-----BEGIN CERTIFICATE-----" />
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}

                {/* SSH Tunnel */}
                <div className="border rounded-md">
                  <div className="flex items-center gap-3 p-3">
                    <input type="checkbox" id="ssh-toggle" checked={form.ssh_enabled}
                      onChange={(e) => setForm({ ...form, ssh_enabled: e.target.checked })}
                      className="h-4 w-4 rounded" />
                    <label htmlFor="ssh-toggle" className="text-sm font-medium cursor-pointer">SSH Tunnel</label>
                    <button type="button" className="ml-auto hover:bg-muted/50 p-1 rounded" onClick={() => setSshOpen(!sshOpen)}>
                      {sshOpen ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                    </button>
                  </div>
                  {form.ssh_enabled && sshOpen && (
                    <div className="p-4 pt-0 space-y-3 border-t">
                      <div className="grid grid-cols-3 gap-3">
                        <div className="col-span-2 space-y-1">
                          <label className="text-sm font-medium">Tunnel Host</label>
                          <Input value={form.tunnel_host} onChange={(e) => setForm({ ...form, tunnel_host: e.target.value })} />
                        </div>
                        <div className="space-y-1">
                          <label className="text-sm font-medium">Port</label>
                          <Input value={form.tunnel_port} onChange={(e) => setForm({ ...form, tunnel_port: e.target.value })} type="number" />
                        </div>
                      </div>
                      <div className="grid grid-cols-2 gap-3">
                        <div className="space-y-1">
                          <label className="text-sm font-medium">Username</label>
                          <Input value={form.tunnel_username} onChange={(e) => setForm({ ...form, tunnel_username: e.target.value })} />
                        </div>
                        <div className="space-y-1">
                          <label className="text-sm font-medium">Auth Method</label>
                          <Select value={form.tunnel_auth_method} onValueChange={(v) => setForm({ ...form, tunnel_auth_method: v })}>
                            <SelectTrigger><SelectValue /></SelectTrigger>
                            <SelectContent>
                              <SelectItem value="key">Private Key</SelectItem>
                              <SelectItem value="password">Password</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                      </div>
                      {form.tunnel_auth_method === "key" ? (
                        <div className="space-y-1">
                          <label className="text-sm font-medium">Private Key</label>
                          <textarea className="w-full rounded-md border p-2 text-xs font-mono min-h-[100px] resize-y"
                            value={form.tunnel_private_key}
                            onChange={(e) => setForm({ ...form, tunnel_private_key: e.target.value })}
                            placeholder="-----BEGIN RSA PRIVATE KEY-----" />
                          <p className="text-xs text-muted-foreground">Paste private key (leave blank to keep existing)</p>
                        </div>
                      ) : (
                        <div className="space-y-1">
                          <label className="text-sm font-medium">Tunnel Password</label>
                          <Input value={form.tunnel_password} onChange={(e) => setForm({ ...form, tunnel_password: e.target.value })}
                            type="password" placeholder="leave blank to keep current" />
                        </div>
                      )}
                    </div>
                  )}
                </div>

                {/* Connection Pool */}
                <div className="border rounded-md">
                  <button type="button" className="flex items-center gap-2 w-full p-3 text-sm font-medium text-left hover:bg-muted/50"
                    onClick={() => setPoolOpen(!poolOpen)}>
                    {poolOpen ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                    Connection Pool Settings
                  </button>
                  {poolOpen && (
                    <div className="p-4 pt-0 grid grid-cols-2 gap-3 border-t">
                      <div className="space-y-1">
                        <label className="text-sm font-medium">Min Connections</label>
                        <Input value={form.connection_pool_min} onChange={(e) => setForm({ ...form, connection_pool_min: e.target.value })}
                          type="number" placeholder="1" />
                      </div>
                      <div className="space-y-1">
                        <label className="text-sm font-medium">Max Connections</label>
                        <Input value={form.connection_pool_max} onChange={(e) => setForm({ ...form, connection_pool_max: e.target.value })}
                          type="number" placeholder="10" />
                      </div>
                    </div>
                  )}
                </div>

                <div className="flex gap-2 pt-2">
                  <Button type="button" variant="ghost" onClick={() => navigate(`/sources/${id}`)}>Cancel</Button>
                  <Button type="submit" disabled={updateMutation.isPending}>
                    {updateMutation.isPending ? "Saving..." : "Save Changes"}
                  </Button>
                </div>
                {updateMutation.isError && (
                  <p className="text-sm text-destructive">Failed to save. Check logs.</p>
                )}
              </form>
            </CardContent>
          </Card>
        </TabsContent>

        {/* ── CDC Settings tab ──────────────────────────────────────── */}
        <TabsContent value="cdc">
          <Card>
            <CardHeader>
              <CardTitle>CDC Configuration — {connectorType || "Unknown connector"}</CardTitle>
            </CardHeader>
            <CardContent>
              <form onSubmit={(e) => { e.preventDefault(); updateMutation.mutate(); }} className="space-y-4">
                {isPg && (
                  <>
                    <div className="p-3 rounded-md bg-blue-500/10 border border-blue-200 text-sm text-blue-700">
                      PostgreSQL CDC uses logical replication. Ensure the publication and replication slot exist on the source DB.
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <label className="text-sm font-medium">Publication Name</label>
                        <Input value={form.publication} onChange={(e) => setForm({ ...form, publication: e.target.value })}
                          placeholder="fusion_pub" />
                        <p className="text-xs text-muted-foreground">Created with: <code>CREATE PUBLICATION fusion_pub FOR ALL TABLES;</code></p>
                      </div>
                      <div className="space-y-2">
                        <label className="text-sm font-medium">Replication Slot Name</label>
                        <Input value={form.replication_slot} onChange={(e) => setForm({ ...form, replication_slot: e.target.value })}
                          placeholder="fusion_slot" />
                        <p className="text-xs text-muted-foreground">Created with: <code>SELECT pg_create_logical_replication_slot(...);</code></p>
                      </div>
                    </div>
                    <div className="space-y-2">
                      <label className="text-sm font-medium">Replication Plugin</label>
                      <Select value={form.replication_plugin} onValueChange={(v) => setForm({ ...form, replication_plugin: v })}>
                        <SelectTrigger className="w-[200px]"><SelectValue /></SelectTrigger>
                        <SelectContent>
                          <SelectItem value="pgoutput">pgoutput (recommended)</SelectItem>
                          <SelectItem value="wal2json">wal2json</SelectItem>
                          <SelectItem value="decoderbufs">decoderbufs</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </>
                )}

                {isMysql && (
                  <>
                    <div className="p-3 rounded-md bg-orange-500/10 border border-orange-200 text-sm text-orange-700">
                      MySQL CDC uses binary logging (binlog). Ensure <code>binlog_format=ROW</code> and <code>binlog_row_image=FULL</code> are set.
                    </div>
                    <div className="space-y-2">
                      <label className="text-sm font-medium">Server ID</label>
                      <Input value={form.server_id} onChange={(e) => setForm({ ...form, server_id: e.target.value })}
                        type="number" placeholder="1234" className="w-[200px]" />
                      <p className="text-xs text-muted-foreground">Unique server ID for the CDC replica connection (must differ from other replicas).</p>
                    </div>
                  </>
                )}

                {isMongo && (
                  <>
                    <div className="p-3 rounded-md bg-green-500/10 border border-green-200 text-sm text-green-700">
                      MongoDB CDC uses Change Streams. Requires MongoDB 4.0+ on a replica set or sharded cluster.
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <label className="text-sm font-medium">Auth Source</label>
                        <Input value={form.auth_source} onChange={(e) => setForm({ ...form, auth_source: e.target.value })}
                          placeholder="admin" />
                      </div>
                      <div className="space-y-2">
                        <label className="text-sm font-medium">Auth Mechanism</label>
                        <Select value={form.auth_mechanism} onValueChange={(v) => setForm({ ...form, auth_mechanism: v })}>
                          <SelectTrigger><SelectValue /></SelectTrigger>
                          <SelectContent>
                            <SelectItem value="SCRAM-SHA-256">SCRAM-SHA-256</SelectItem>
                            <SelectItem value="SCRAM-SHA-1">SCRAM-SHA-1</SelectItem>
                            <SelectItem value="MONGODB-X509">X.509</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                    </div>
                  </>
                )}

                {!isPg && !isMysql && !isMongo && (
                  <p className="text-muted-foreground text-sm">No CDC-specific settings for connector type: <strong>{connectorType || "unknown"}</strong></p>
                )}

                <div className="flex gap-2 pt-2">
                  <Button type="button" variant="ghost" onClick={() => navigate(`/sources/${id}`)}>Cancel</Button>
                  <Button type="submit" disabled={updateMutation.isPending}>
                    {updateMutation.isPending ? "Saving..." : "Save CDC Settings"}
                  </Button>
                </div>
              </form>
            </CardContent>
          </Card>
        </TabsContent>

        {/* ── Streams & Tables tab ──────────────────────────────────── */}
        <TabsContent value="streams">
          <div className="space-y-4">
            {relatedConnections.length === 0 ? (
              <Card>
                <CardContent className="py-10 text-center text-muted-foreground">
                  No connections use this source yet. Create a connection to start syncing tables.
                </CardContent>
              </Card>
            ) : (
              relatedConnections.map((conn: any) => (
                <ConnectionStreams key={conn.connection_id} conn={conn} />
              ))
            )}

            {/* Also show discovered schema if available */}
            {source.discovery_cache?.schemas?.length > 0 && (
              <Card>
                <CardHeader><CardTitle>Discovered Schema</CardTitle></CardHeader>
                <CardContent>
                  <p className="text-sm text-muted-foreground mb-3">
                    All tables discovered from the source database. Last discovery: {source.last_discovery_at ? new Date(source.last_discovery_at).toLocaleString() : "never"}
                  </p>
                  <div className="space-y-3">
                    {source.discovery_cache.schemas.map((schema: any, si: number) => (
                      <div key={si}>
                        {schema.schema_name && (
                          <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wide mb-1">{schema.schema_name}</p>
                        )}
                        <div className="rounded-md border overflow-hidden">
                          <Table>
                            <TableHeader>
                              <TableRow>
                                <TableHead>Table</TableHead>
                                <TableHead>Columns</TableHead>
                                <TableHead>Primary Keys</TableHead>
                              </TableRow>
                            </TableHeader>
                            <TableBody>
                              {(schema.tables ?? []).map((tbl: any, ti: number) => (
                                <TableRow key={ti}>
                                  <TableCell className="font-mono font-medium">{tbl.table_name}</TableCell>
                                  <TableCell>{tbl.columns?.length ?? 0}</TableCell>
                                  <TableCell className="font-mono text-xs">
                                    {tbl.columns?.filter((c: any) => c.is_primary_key).map((c: any) => c.column_name).join(", ") || "—"}
                                  </TableCell>
                                </TableRow>
                              ))}
                            </TableBody>
                          </Table>
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}

function ConnectionStreams({ conn }: { conn: any }) {
  const { data: streams = [], isLoading } = useQuery({
    queryKey: ["streams", conn.connection_id],
    queryFn: () =>
      fetch(`/api/v1/streams/connections/${conn.connection_id}/streams`, {
        headers: { Authorization: `Bearer ${localStorage.getItem("auth_token")}` },
      }).then((r) => r.json()).then((d) => d.streams ?? []),
  });

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between text-base">
          <span>{conn.connection_name}</span>
          <Badge variant={conn.status === "active" ? "success" : "secondary"}>{conn.status}</Badge>
        </CardTitle>
      </CardHeader>
      <CardContent className="p-0">
        {isLoading ? (
          <p className="p-4 text-sm text-muted-foreground">Loading streams...</p>
        ) : streams.length === 0 ? (
          <p className="p-4 text-sm text-muted-foreground">No streams configured for this connection.</p>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Enabled</TableHead>
                <TableHead>Schema</TableHead>
                <TableHead>Table</TableHead>
                <TableHead>Primary Keys</TableHead>
                <TableHead>Destination Schema</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {streams.map((s: any) => (
                <TableRow key={s.stream_id}>
                  <TableCell>
                    <span className={s.is_enabled !== false ? "text-green-600" : "text-muted-foreground"}>
                      {s.is_enabled !== false ? "● Enabled" : "○ Disabled"}
                    </span>
                  </TableCell>
                  <TableCell className="text-muted-foreground font-mono text-sm">{s.source_schema_name ?? "—"}</TableCell>
                  <TableCell className="font-mono font-medium text-sm">{s.stream_name}</TableCell>
                  <TableCell className="font-mono text-xs">{Array.isArray(s.primary_keys) ? s.primary_keys.join(", ") : (s.primary_keys ?? "—")}</TableCell>
                  <TableCell className="text-muted-foreground font-mono text-sm">{s.destination_schema_name ?? "—"}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </CardContent>
    </Card>
  );
}
