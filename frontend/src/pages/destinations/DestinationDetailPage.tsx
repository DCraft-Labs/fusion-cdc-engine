import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { TestTube, Pencil, Trash2, CheckCircle2, Circle, Info, ShieldCheck } from "lucide-react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";

export function DestinationDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: destination, isLoading } = useQuery({
    queryKey: ["destinations", id],
    queryFn: () => api.get(`/destinations/${id}`).then((r) => r.data),
  });

  const testMutation = useMutation({
    mutationFn: () => api.post(`/destinations/${id}/test-connection`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["destinations", id] }),
  });

  const validatePermsMutation = useMutation({
    mutationFn: () => api.post(`/destinations/${id}/validate-write-permissions`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["destinations", id] }),
  });

  const deleteMutation = useMutation({
    mutationFn: () => api.delete(`/destinations/${id}`),
    onSuccess: () => navigate("/destinations"),
  });

  const { data: stats } = useQuery({
    queryKey: ["destinations", id, "stats"],
    queryFn: () => api.get(`/destinations/${id}/stats`).then((r) => r.data),
    enabled: !!id,
  });

  if (isLoading) return <div className="flex items-center justify-center h-64">Loading...</div>;
  if (!destination) return <div className="text-center text-muted-foreground">Destination not found</div>;

  const connectorType = destination.connector_definition?.connector_type ?? "";
  const connectorIcon = connectorType === "postgresql" ? "🐘" : connectorType === "iceberg" ? "🧊" : "💾";
  const writeModeLabel = (mode: string) => {
    switch (mode) {
      case "scd1": return "SCD Type 1 (Upsert)";
      case "scd2": return "SCD Type 2 (History)";
      case "append": return "Append Only";
      default: return mode ?? "SCD Type 1 (Upsert)";
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <span className="text-2xl">{connectorIcon}</span>
          <div>
            <h1 className="text-2xl font-bold">{destination.destination_name}</h1>
            <p className="text-muted-foreground">{connectorType}</p>
          </div>
        </div>
        <Badge variant={destination.status === "active" ? "success" : destination.status === "error" ? "destructive" : "secondary"}>
          {destination.status === "active" ? "● Active" : destination.status === "error" ? "⚠ Error" : destination.status}
        </Badge>
      </div>

      {/* Readiness Checklist — shown when draft */}
      {destination.status === "draft" && (() => {
        const connectionPassed = destination.connection_test_status === "success";
        const writePermsOk = destination.connection_config?.write_permissions_validated === true;
        return (
          <Card className="border-amber-300 bg-amber-50 dark:bg-amber-950/20 dark:border-amber-800">
            <CardContent className="py-4 space-y-3">
              <div className="flex items-center gap-2">
                <Info className="h-4 w-4 text-amber-600" />
                <span className="text-sm font-medium text-amber-800 dark:text-amber-300">
                  This destination is in Draft — complete these steps to activate:
                </span>
              </div>
              <div className="space-y-2 pl-6">
                <div className="flex items-center gap-2 text-sm">
                  {connectionPassed
                    ? <CheckCircle2 className="h-4 w-4 text-green-600" />
                    : <Circle className="h-4 w-4 text-muted-foreground" />}
                  <span className={connectionPassed ? "text-green-700 dark:text-green-400" : ""}>
                    Test Connection
                  </span>
                  {!connectionPassed && destination.connection_test_status === "failed" && (
                    <span className="text-xs text-destructive ml-1">— Failed: {destination.connection_test_error || "check credentials"}</span>
                  )}
                  {!connectionPassed && !destination.connection_test_status && (
                    <span className="text-xs text-muted-foreground ml-1">— not tested yet</span>
                  )}
                </div>
                <div className="flex items-center gap-2 text-sm">
                  {writePermsOk
                    ? <CheckCircle2 className="h-4 w-4 text-green-600" />
                    : <Circle className="h-4 w-4 text-muted-foreground" />}
                  <span className={writePermsOk ? "text-green-700 dark:text-green-400" : ""}>
                    Validate Write Permissions
                  </span>
                  {!writePermsOk && (
                    <Button size="sm" variant="link" className="h-auto p-0 ml-1 text-xs" onClick={() => validatePermsMutation.mutate()} disabled={validatePermsMutation.isPending || !connectionPassed}>
                      {validatePermsMutation.isPending ? "Validating..." : "Run Now"}
                    </Button>
                  )}
                </div>
              </div>
              {connectionPassed && writePermsOk && (
                <p className="text-xs text-green-700 pl-6">All checks passed — destination will activate automatically.</p>
              )}
            </CardContent>
          </Card>
        );
      })()}

      {/* Connection Info */}
      <Card>
        <CardContent className="py-4">
          <div className="flex flex-wrap gap-6 text-sm">
            {destination.host && <div><span className="text-muted-foreground">Host:</span> <span className="font-mono">{destination.host}</span></div>}
            {destination.port && <div><span className="text-muted-foreground">Port:</span> <span className="font-mono">{destination.port}</span></div>}
            {destination.database_name && <div><span className="text-muted-foreground">Database:</span> <span className="font-mono">{destination.database_name}</span></div>}
            {destination.schema_name && (
              <div><span className="text-muted-foreground">Schema:</span> <span className="font-mono">{destination.schema_name}</span></div>
            )}
          </div>
        </CardContent>
      </Card>

      <Tabs defaultValue="overview">
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="write-mode">Write Mode</TabsTrigger>
          <TabsTrigger value="schema">Schema Mapping</TabsTrigger>
          <TabsTrigger value="batch">Batch Settings</TabsTrigger>
          <TabsTrigger value="stats">Statistics</TabsTrigger>
        </TabsList>

        <TabsContent value="overview">
          <Card>
            <CardHeader><CardTitle>Destination Details</CardTitle></CardHeader>
            <CardContent className="space-y-6">
              <dl className="grid grid-cols-2 gap-4 text-sm">
                <div><dt className="text-muted-foreground">ID</dt><dd className="font-mono">{destination.destination_id}</dd></div>
                <div><dt className="text-muted-foreground">Type</dt><dd>{connectorType}</dd></div>
                <div><dt className="text-muted-foreground">Status</dt><dd>{destination.status}</dd></div>
                <div><dt className="text-muted-foreground">Created</dt><dd>{new Date(destination.created_at).toLocaleDateString()}</dd></div>
                <div><dt className="text-muted-foreground">Last Test</dt><dd>{destination.connection_test_at ? new Date(destination.connection_test_at).toLocaleString() : "Never"}</dd></div>
                <div><dt className="text-muted-foreground">Last Test Result</dt><dd>{destination.connection_test_status === "success" ? "✓ Passed" : destination.connection_test_status === "failed" ? "✗ Failed" : "—"}</dd></div>
              </dl>
              <div className="flex gap-2">
                <Button size="sm" variant="outline" onClick={() => testMutation.mutate()} disabled={testMutation.isPending}>
                  <TestTube className="mr-2 h-4 w-4" />{testMutation.isPending ? "Testing..." : "Test Connection"}
                </Button>
                <Button size="sm" variant="outline" onClick={() => validatePermsMutation.mutate()} disabled={validatePermsMutation.isPending}>
                  <ShieldCheck className="mr-2 h-4 w-4" />{validatePermsMutation.isPending ? "Validating..." : "Validate Permissions"}
                </Button>
                <Button size="sm" variant="outline" onClick={() => navigate(`/destinations/${id}/edit`)}>
                  <Pencil className="mr-2 h-4 w-4" />Edit
                </Button>
                <Button size="sm" variant="destructive" onClick={() => { if (confirm("Delete this destination?")) deleteMutation.mutate(); }}>
                  <Trash2 className="mr-2 h-4 w-4" />Delete
                </Button>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="write-mode">
          <Card>
            <CardHeader><CardTitle>Write Configuration</CardTitle></CardHeader>
            <CardContent className="space-y-4">
              <dl className="space-y-3 text-sm">
                <div>
                  <dt className="text-muted-foreground">Current Write Mode</dt>
                  <dd className="font-medium text-lg mt-1">{writeModeLabel(destination.config?.write_mode)}</dd>
                </div>
              </dl>
              <div className="rounded-md border p-4 space-y-2">
                <p className="text-sm font-medium">Available Write Modes</p>
                <div className="space-y-1 text-sm text-muted-foreground">
                  <p><strong>SCD Type 1:</strong> Latest value wins — overwrites existing rows on primary key match</p>
                  <p><strong>SCD Type 2:</strong> Maintains full history with valid_from/valid_to columns</p>
                  <p><strong>Append Only:</strong> Insert all events without deduplication</p>
                </div>
              </div>
              <Button size="sm" variant="outline" onClick={() => navigate(`/destinations/${id}/edit`)}>
                <Pencil className="mr-2 h-4 w-4" />Change Write Mode
              </Button>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="schema">
          <Card>
            <CardHeader><CardTitle>Schema Mapping</CardTitle></CardHeader>
            <CardContent>
              {destination.config?.schema_mapping && Object.keys(destination.config.schema_mapping).length > 0 ? (
                <pre className="rounded-md bg-muted p-4 text-sm overflow-auto">{JSON.stringify(destination.config.schema_mapping, null, 2)}</pre>
              ) : (
                <p className="text-muted-foreground text-sm">No schema mappings configured. Source tables will be written with their original schema.</p>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="batch">
          <Card>
            <CardHeader><CardTitle>Batch Settings</CardTitle></CardHeader>
            <CardContent className="space-y-4">
              <dl className="grid grid-cols-3 gap-6 text-sm">
                <div className="rounded-md border p-4">
                  <dt className="text-muted-foreground">Batch Size</dt>
                  <dd className="text-2xl font-bold mt-1">{destination.config?.batch_size ?? 1000}</dd>
                  <p className="text-xs text-muted-foreground mt-1">events per batch</p>
                </div>
                <div className="rounded-md border p-4">
                  <dt className="text-muted-foreground">Flush Interval</dt>
                  <dd className="text-2xl font-bold mt-1">{destination.config?.flush_interval_seconds ?? 5}</dd>
                  <p className="text-xs text-muted-foreground mt-1">seconds</p>
                </div>
                <div className="rounded-md border p-4">
                  <dt className="text-muted-foreground">Max Retries</dt>
                  <dd className="text-2xl font-bold mt-1">{destination.config?.max_retry ?? 3}</dd>
                  <p className="text-xs text-muted-foreground mt-1">attempts on failure</p>
                </div>
              </dl>
              <Button size="sm" variant="outline" onClick={() => navigate(`/destinations/${id}/edit`)}>
                <Pencil className="mr-2 h-4 w-4" />Edit Batch Settings
              </Button>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="stats">
          <Card>
            <CardHeader><CardTitle>Statistics</CardTitle></CardHeader>
            <CardContent className="space-y-6">
              {stats?.events_written_history?.length > 0 ? (
                <>
                  <div className="grid grid-cols-3 gap-4">
                    <div className="rounded-md border p-4 text-center">
                      <p className="text-muted-foreground text-sm">Events Written</p>
                      <p className="text-2xl font-bold">{stats.total_events_written?.toLocaleString() ?? 0}</p>
                    </div>
                    <div className="rounded-md border p-4 text-center">
                      <p className="text-muted-foreground text-sm">Errors</p>
                      <p className="text-2xl font-bold text-destructive">{stats.total_errors ?? 0}</p>
                    </div>
                    <div className="rounded-md border p-4 text-center">
                      <p className="text-muted-foreground text-sm">Uptime</p>
                      <p className="text-2xl font-bold">{stats.uptime_hours ?? "—"}h</p>
                    </div>
                  </div>
                  <div className="h-64">
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={stats.events_written_history}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="time" />
                        <YAxis />
                        <Tooltip />
                        <Line type="monotone" dataKey="count" stroke="hsl(var(--primary))" strokeWidth={2} dot={false} />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </>
              ) : (
                <p className="text-muted-foreground text-sm">Write statistics will appear once the destination is actively receiving data.</p>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
