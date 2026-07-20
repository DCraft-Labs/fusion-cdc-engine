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
import { ChevronDown, ChevronRight, TestTube } from "lucide-react";

export function EditDestinationPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [sshOpen, setSshOpen] = useState(false);
  const [sslOpen, setSslOpen] = useState(false);

  const [pgForm, setPgForm] = useState({
    name: "",
    host: "",
    port: "5432",
    database: "",
    schema_name: "public",
    username: "",
    password: "",
    write_mode: "scd1",
    ssl_enabled: false,
  });

  const [icebergForm, setIcebergForm] = useState({
    name: "",
    catalog_type: "nessie",
    catalog_name: "",
    namespace: "",
    nessie_uri: "",
    nessie_ref: "main",
    storage_type: "aws",
    warehouse: "",
    s3_endpoint: "",
    s3_region: "",
    aws_credentials_provider: "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
  });

  // Shared SSH state
  const [sshForm, setSshForm] = useState({
    ssh_enabled: false,
    tunnel_host: "",
    tunnel_port: "22",
    tunnel_username: "",
    tunnel_auth_method: "key",
    tunnel_private_key: "",
    tunnel_password: "",
  });

  const { data: dest, isLoading } = useQuery({
    queryKey: ["destinations", id],
    queryFn: () => api.get(`/destinations/${id}`).then((r) => r.data),
  });

  const { data: connections = [] } = useQuery({
    queryKey: ["connections"],
    queryFn: () => fetchList("/connections", "connections"),
  });

  const destType = dest?.connector_definition?.connector_type ?? dest?.connector_type ?? dest?.type ?? "";
  const isPostgres = destType === "postgres" || destType === "postgresql";

  useEffect(() => {
    if (!dest) return;
    const cfg = dest.connection_config ?? dest.config ?? {};
    const ssh = dest.ssh_config ?? {};

    // SSH
    setSshForm({
      ssh_enabled: !!ssh.tunnel_host,
      tunnel_host: ssh.tunnel_host ?? "",
      tunnel_port: String(ssh.tunnel_port ?? "22"),
      tunnel_username: ssh.tunnel_username ?? "",
      tunnel_auth_method: ssh.tunnel_auth_method ?? "key",
      tunnel_private_key: ssh.tunnel_private_key ?? "",
      tunnel_password: "",
    });

    if (isPostgres) {
      setPgForm({
        name: dest.destination_name ?? dest.name ?? "",
        host: dest.host ?? cfg.host ?? "",
        port: String(dest.port ?? cfg.port ?? "5432"),
        database: dest.database_name ?? cfg.database_name ?? "",
        schema_name: cfg.schema_name ?? cfg.default_schema ?? "public",
        username: dest.username ?? cfg.username ?? "",
        password: "",
        write_mode: cfg.write_mode ?? "scd1",
        ssl_enabled: dest.ssl_enabled ?? false,
      });
    } else {
      setIcebergForm({
        name: dest.destination_name ?? dest.name ?? "",
        catalog_type: cfg.catalog_type ?? "nessie",
        catalog_name: cfg.catalog_name ?? "",
        namespace: cfg.namespace ?? "",
        nessie_uri: cfg.nessie_uri ?? "",
        nessie_ref: cfg.nessie_ref ?? "main",
        storage_type: cfg.storage_type ?? "aws",
        warehouse: cfg.warehouse ?? cfg.storage ?? "",
        s3_endpoint: cfg.s3_endpoint ?? "",
        s3_region: cfg.s3_region ?? "",
        aws_credentials_provider: cfg.aws_credentials_provider ?? "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
      });
    }
  }, [dest, isPostgres]);

  const buildSshConfig = () =>
    sshForm.ssh_enabled ? {
      tunnel_host: sshForm.tunnel_host,
      tunnel_port: parseInt(sshForm.tunnel_port) || 22,
      tunnel_username: sshForm.tunnel_username,
      tunnel_auth_method: sshForm.tunnel_auth_method,
      tunnel_private_key: sshForm.tunnel_auth_method === "key" ? (sshForm.tunnel_private_key || undefined) : undefined,
      tunnel_password: sshForm.tunnel_auth_method === "password" ? (sshForm.tunnel_password || undefined) : undefined,
    } : {};

  const updateMutation = useMutation({
    mutationFn: () => {
      if (isPostgres) {
        return api.patch(`/destinations/${id}`, {
          destination_name: pgForm.name,
          host: pgForm.host,
          port: parseInt(pgForm.port) || undefined,
          database_name: pgForm.database,
          username: pgForm.username,
          password: pgForm.password || undefined,
          ssl_enabled: pgForm.ssl_enabled,
          ssh_config: buildSshConfig(),
          config: {
            schema_name: pgForm.schema_name,
            write_mode: pgForm.write_mode,
          },
        });
      }
      return api.patch(`/destinations/${id}`, {
        destination_name: icebergForm.name,
        ssh_config: buildSshConfig(),
        config: {
          catalog_type: icebergForm.catalog_type,
          catalog_name: icebergForm.catalog_name,
          namespace: icebergForm.namespace,
          nessie_uri: icebergForm.nessie_uri,
          nessie_ref: icebergForm.nessie_ref,
          storage_type: icebergForm.storage_type,
          warehouse: icebergForm.warehouse,
          s3_endpoint: icebergForm.s3_endpoint,
          s3_region: icebergForm.s3_region,
          aws_credentials_provider: icebergForm.aws_credentials_provider,
        },
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["destinations", id] });
      navigate(`/destinations/${id}`);
    },
  });

  const testMutation = useMutation({
    mutationFn: () => api.post(`/destinations/${id}/test-connection`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["destinations", id] }),
  });

  if (isLoading) return <div className="flex items-center justify-center h-64">Loading...</div>;
  if (!dest) return <div className="text-center text-muted-foreground">Destination not found</div>;

  const relatedConnections = (connections as any[]).filter(
    (c: any) => c.destination_id === id || c.destination?.destination_id === id
  );

  const SshSection = () => (
    <div className="border rounded-md">
      <div className="flex items-center gap-3 p-3">
        <input type="checkbox" id="ssh-dest-toggle" checked={sshForm.ssh_enabled}
          onChange={(e) => setSshForm({ ...sshForm, ssh_enabled: e.target.checked })}
          className="h-4 w-4 rounded" />
        <label htmlFor="ssh-dest-toggle" className="text-sm font-medium cursor-pointer">SSH Tunnel</label>
        <button type="button" className="ml-auto hover:bg-muted/50 p-1 rounded" onClick={() => setSshOpen(!sshOpen)}>
          {sshOpen ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
        </button>
      </div>
      {sshForm.ssh_enabled && sshOpen && (
        <div className="p-4 pt-0 space-y-3 border-t">
          <div className="grid grid-cols-3 gap-3">
            <div className="col-span-2 space-y-1">
              <label className="text-sm font-medium">Tunnel Host</label>
              <Input value={sshForm.tunnel_host} onChange={(e) => setSshForm({ ...sshForm, tunnel_host: e.target.value })} />
            </div>
            <div className="space-y-1">
              <label className="text-sm font-medium">Port</label>
              <Input value={sshForm.tunnel_port} onChange={(e) => setSshForm({ ...sshForm, tunnel_port: e.target.value })} type="number" />
            </div>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-1">
              <label className="text-sm font-medium">Username</label>
              <Input value={sshForm.tunnel_username} onChange={(e) => setSshForm({ ...sshForm, tunnel_username: e.target.value })} />
            </div>
            <div className="space-y-1">
              <label className="text-sm font-medium">Auth Method</label>
              <Select value={sshForm.tunnel_auth_method} onValueChange={(v) => setSshForm({ ...sshForm, tunnel_auth_method: v })}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="key">Private Key</SelectItem>
                  <SelectItem value="password">Password</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
          {sshForm.tunnel_auth_method === "key" ? (
            <div className="space-y-1">
              <label className="text-sm font-medium">Private Key</label>
              <textarea className="w-full rounded-md border p-2 text-xs font-mono min-h-[100px] resize-y"
                value={sshForm.tunnel_private_key}
                onChange={(e) => setSshForm({ ...sshForm, tunnel_private_key: e.target.value })}
                placeholder="-----BEGIN RSA PRIVATE KEY-----" />
              <p className="text-xs text-muted-foreground">Paste private key (leave blank to keep existing)</p>
            </div>
          ) : (
            <div className="space-y-1">
              <label className="text-sm font-medium">Tunnel Password</label>
              <Input value={sshForm.tunnel_password} onChange={(e) => setSshForm({ ...sshForm, tunnel_password: e.target.value })}
                type="password" placeholder="leave blank to keep current" />
            </div>
          )}
        </div>
      )}
    </div>
  );

  return (
    <div className="space-y-6 max-w-4xl">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Edit Destination: {dest?.destination_name ?? dest?.name}</h1>
        <Button variant="outline" onClick={() => testMutation.mutate()} disabled={testMutation.isPending}>
          <TestTube className="mr-2 h-4 w-4" />
          {testMutation.isPending ? "Testing..." : "Test Connection"}
        </Button>
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

      <Tabs defaultValue="config">
        <TabsList>
          <TabsTrigger value="config">Configuration</TabsTrigger>
          <TabsTrigger value="connections">Connected Pipelines ({relatedConnections.length})</TabsTrigger>
        </TabsList>

        <TabsContent value="config">
          <Card>
            <CardHeader>
              <CardTitle>{isPostgres ? "PostgreSQL Configuration" : "Iceberg / Lakehouse Configuration"}</CardTitle>
            </CardHeader>
            <CardContent>
              <form onSubmit={(e) => { e.preventDefault(); updateMutation.mutate(); }} className="space-y-4">
                {isPostgres ? (
                  <>
                    <div className="space-y-2">
                      <label className="text-sm font-medium">Name *</label>
                      <Input value={pgForm.name} onChange={(e) => setPgForm({ ...pgForm, name: e.target.value })} required />
                    </div>
                    <div className="grid grid-cols-3 gap-4">
                      <div className="col-span-2 space-y-2">
                        <label className="text-sm font-medium">Host *</label>
                        <Input value={pgForm.host} onChange={(e) => setPgForm({ ...pgForm, host: e.target.value })} required />
                      </div>
                      <div className="space-y-2">
                        <label className="text-sm font-medium">Port</label>
                        <Input value={pgForm.port} onChange={(e) => setPgForm({ ...pgForm, port: e.target.value })} type="number" />
                      </div>
                    </div>
                    <div className="space-y-2">
                      <label className="text-sm font-medium">Database</label>
                      <Input value={pgForm.database} onChange={(e) => setPgForm({ ...pgForm, database: e.target.value })} />
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <label className="text-sm font-medium">Schema</label>
                        <Input value={pgForm.schema_name} onChange={(e) => setPgForm({ ...pgForm, schema_name: e.target.value })} placeholder="public" />
                      </div>
                      <div className="space-y-2">
                        <label className="text-sm font-medium">Write Mode</label>
                        <Select value={pgForm.write_mode} onValueChange={(v) => setPgForm({ ...pgForm, write_mode: v })}>
                          <SelectTrigger><SelectValue /></SelectTrigger>
                          <SelectContent>
                            <SelectItem value="scd1">SCD Type 1 (Upsert)</SelectItem>
                            <SelectItem value="scd2">SCD Type 2 (History)</SelectItem>
                            <SelectItem value="append">Append Only</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <label className="text-sm font-medium">Username</label>
                        <Input value={pgForm.username} onChange={(e) => setPgForm({ ...pgForm, username: e.target.value })} />
                      </div>
                      <div className="space-y-2">
                        <label className="text-sm font-medium">Password</label>
                        <Input value={pgForm.password} onChange={(e) => setPgForm({ ...pgForm, password: e.target.value })}
                          type="password" placeholder="leave blank to keep current" />
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <input type="checkbox" id="dest-ssl-toggle" checked={pgForm.ssl_enabled}
                        onChange={(e) => setPgForm({ ...pgForm, ssl_enabled: e.target.checked })} className="h-4 w-4 rounded" />
                      <label htmlFor="dest-ssl-toggle" className="text-sm font-medium">Enable SSL/TLS</label>
                    </div>
                    <SshSection />
                  </>
                ) : (
                  <>
                    <div className="space-y-2">
                      <label className="text-sm font-medium">Name *</label>
                      <Input value={icebergForm.name} onChange={(e) => setIcebergForm({ ...icebergForm, name: e.target.value })} required />
                    </div>

                    {/* Catalog */}
                    <div className="space-y-3 pt-2">
                      <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">Iceberg Catalog</h3>
                      <div className="grid grid-cols-2 gap-4">
                        <div className="space-y-2">
                          <label className="text-sm font-medium">Catalog Type</label>
                          <Select value={icebergForm.catalog_type} onValueChange={(v) => setIcebergForm({ ...icebergForm, catalog_type: v })}>
                            <SelectTrigger><SelectValue /></SelectTrigger>
                            <SelectContent>
                              <SelectItem value="nessie">Nessie</SelectItem>
                              <SelectItem value="hive">Hive</SelectItem>
                              <SelectItem value="glue">AWS Glue</SelectItem>
                              <SelectItem value="rest">REST</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                        <div className="space-y-2">
                          <label className="text-sm font-medium">Catalog Name</label>
                          <Input value={icebergForm.catalog_name} onChange={(e) => setIcebergForm({ ...icebergForm, catalog_name: e.target.value })} placeholder="vp_terra" />
                        </div>
                      </div>
                      <div className="space-y-2">
                        <label className="text-sm font-medium">Namespace</label>
                        <Input value={icebergForm.namespace} onChange={(e) => setIcebergForm({ ...icebergForm, namespace: e.target.value })} placeholder="raw_bank" />
                      </div>
                    </div>

                    {/* Nessie */}
                    {icebergForm.catalog_type === "nessie" && (
                      <div className="space-y-3 pt-2">
                        <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">Nessie Settings</h3>
                        <div className="space-y-2">
                          <label className="text-sm font-medium">Nessie URI</label>
                          <Input value={icebergForm.nessie_uri} onChange={(e) => setIcebergForm({ ...icebergForm, nessie_uri: e.target.value })} placeholder="http://nessie-service:19120/api/v2" />
                        </div>
                        <div className="space-y-2">
                          <label className="text-sm font-medium">Nessie Branch / Ref</label>
                          <Input value={icebergForm.nessie_ref} onChange={(e) => setIcebergForm({ ...icebergForm, nessie_ref: e.target.value })} placeholder="main" />
                        </div>
                      </div>
                    )}

                    {/* Storage */}
                    <div className="space-y-3 pt-2">
                      <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">Storage &amp; Warehouse</h3>
                      <div className="space-y-2">
                        <label className="text-sm font-medium">Storage Type</label>
                        <div className="flex gap-4 flex-wrap">
                          {([ ["aws", "AWS S3"], ["azure", "Azure Blob"], ["gcs", "Google GCS"] ] as [string, string][]).map(([val, label]) => (
                            <label key={val} className={`flex items-center gap-2 rounded-md border px-3 py-2 cursor-pointer text-sm ${icebergForm.storage_type === val ? "border-primary bg-primary/5" : ""}`}>
                              <input type="radio" name="edit_storage_type" value={val} checked={icebergForm.storage_type === val} onChange={() => setIcebergForm({ ...icebergForm, storage_type: val })} />
                              {label}
                            </label>
                          ))}
                        </div>
                      </div>
                      <div className="space-y-2">
                        <label className="text-sm font-medium">Warehouse Path</label>
                        <Input value={icebergForm.warehouse} onChange={(e) => setIcebergForm({ ...icebergForm, warehouse: e.target.value })} placeholder="s3a://my-bucket/warehouse/" />
                      </div>
                      {icebergForm.storage_type === "aws" && (
                        <>
                          <div className="grid grid-cols-2 gap-4">
                            <div className="space-y-2">
                              <label className="text-sm font-medium">S3 Endpoint</label>
                              <Input value={icebergForm.s3_endpoint} onChange={(e) => setIcebergForm({ ...icebergForm, s3_endpoint: e.target.value })} placeholder="https://s3.ap-south-1.amazonaws.com" />
                            </div>
                            <div className="space-y-2">
                              <label className="text-sm font-medium">S3 Region</label>
                              <Input value={icebergForm.s3_region} onChange={(e) => setIcebergForm({ ...icebergForm, s3_region: e.target.value })} placeholder="ap-south-1" />
                            </div>
                          </div>
                          <div className="space-y-2">
                            <label className="text-sm font-medium">AWS Credentials Provider</label>
                            <Input value={icebergForm.aws_credentials_provider}
                              onChange={(e) => setIcebergForm({ ...icebergForm, aws_credentials_provider: e.target.value })}
                              placeholder="com.amazonaws.auth.WebIdentityTokenCredentialsProvider" />
                            <p className="text-xs text-muted-foreground">IAM credentials provider class for S3 / Iceberg access.</p>
                          </div>
                        </>
                      )}
                    </div>

                    <SshSection />
                  </>
                )}

                <div className="flex gap-2 pt-4">
                  <Button type="button" variant="ghost" onClick={() => navigate(`/destinations/${id}`)}>Cancel</Button>
                  <Button type="submit" disabled={updateMutation.isPending}>
                    {updateMutation.isPending ? "Saving..." : "Save Changes"}
                  </Button>
                </div>
                {updateMutation.isError && (
                  <p className="text-sm text-destructive">Failed to save changes. Please try again.</p>
                )}
              </form>
            </CardContent>
          </Card>
        </TabsContent>

        {/* ── Connected pipelines tab ───────────────────────────── */}
        <TabsContent value="connections">
          {relatedConnections.length === 0 ? (
            <Card>
              <CardContent className="py-10 text-center text-muted-foreground">
                No connections use this destination yet.
              </CardContent>
            </Card>
          ) : (
            <Card>
              <CardContent className="p-0">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Connection Name</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Source</TableHead>
                      <TableHead>Created</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {relatedConnections.map((c: any) => (
                      <TableRow key={c.connection_id} className="cursor-pointer hover:bg-muted/40"
                        onClick={() => navigate(`/connections/${c.connection_id}`)}>
                        <TableCell className="font-medium">{c.connection_name}</TableCell>
                        <TableCell>
                          <Badge variant={c.status === "active" ? "success" : "secondary"}>{c.status}</Badge>
                        </TableCell>
                        <TableCell className="text-muted-foreground">{c.source?.source_name ?? c.source_id ?? "—"}</TableCell>
                        <TableCell className="text-muted-foreground text-sm">
                          {c.created_at ? new Date(c.created_at).toLocaleDateString() : "—"}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          )}
        </TabsContent>
      </Tabs>
    </div>
  );
}
