import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Table, TableHeader, TableRow, TableHead, TableBody, TableCell } from "@/components/ui/table";
import { Database, RefreshCw, Plug, Trash2, Pencil, Activity, AlertTriangle, Clock, Server, CheckCircle2, Circle, Info } from "lucide-react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";

const statusVariant = (status: string) => {
  switch (status) {
    case "active": return "success";
    case "inactive": return "destructive";
    default: return "secondary";
  }
};

const connectorIcon = (type: string) => {
  switch (type) {
    case "mysql": return <Database className="h-5 w-5 text-blue-500" />;
    case "postgres": return <Database className="h-5 w-5 text-indigo-500" />;
    default: return <Server className="h-5 w-5 text-muted-foreground" />;
  }
};

export function SourceDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: source, isLoading } = useQuery({
    queryKey: ["sources", id],
    queryFn: () => api.get(`/sources/${id}`).then((r) => r.data),
  });

  const { data: schema, refetch: refetchSchema, isFetching: schemaFetching } = useQuery({
    queryKey: ["sources", id, "schema"],
    queryFn: () => api.get(`/sources/${id}/schemas`).then((r) => r.data),
    enabled: false,
  });

  const { data: stats } = useQuery({
    queryKey: ["sources", id, "stats"],
    queryFn: () => api.get(`/sources/${id}/stats`).then((r) => r.data),
  });

  const testMutation = useMutation({
    mutationFn: () => api.post(`/sources/${id}/test-connection`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["sources", id] }),
  });

  const deleteMutation = useMutation({
    mutationFn: () => api.delete(`/sources/${id}`),
    onSuccess: () => navigate("/sources"),
  });

  const discoverMutation = useMutation({
    mutationFn: () => api.post(`/sources/${id}/discover`),
    onSuccess: () => refetchSchema(),
  });

  if (isLoading) return <div className="flex items-center justify-center h-64">Loading...</div>;
  if (!source) return <div className="text-center text-muted-foreground">Source not found</div>;

  const config = { host: source.host, port: source.port, database: source.database_name, ...source.config };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          {connectorIcon(source.connector_definition_type)}
          <div>
            <h1 className="text-2xl font-bold">{source.source_name}</h1>
            <p className="text-sm text-muted-foreground capitalize">{source.connector_definition_type}</p>
          </div>
        </div>
        <Badge variant={statusVariant(source.status)}>
          {source.status?.charAt(0).toUpperCase() + source.status?.slice(1)}
        </Badge>
      </div>

      <Tabs defaultValue="overview">
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="schema">Schema Discovery</TabsTrigger>
          <TabsTrigger value="cdc-config">CDC Config</TabsTrigger>
          <TabsTrigger value="statistics">Statistics</TabsTrigger>
        </TabsList>

        {/* Overview Tab */}
        <TabsContent value="overview" className="space-y-4">
          {/* Readiness Checklist — shown when draft */}
          {source.status === "draft" && (() => {
            const connectionPassed = source.connection_test_status === "success";
            const cache = source.discovery_cache || {};
            const totalTables = (cache.schemas || []).reduce((sum: number, s: any) => sum + (s.tables?.length || 0), 0);
            const schemasDiscovered = !!source.last_discovery_at && totalTables > 0;
            return (
              <Card className="border-amber-300 bg-amber-50 dark:bg-amber-950/20 dark:border-amber-800">
                <CardContent className="py-4 space-y-3">
                  <div className="flex items-center gap-2">
                    <Info className="h-4 w-4 text-amber-600" />
                    <span className="text-sm font-medium text-amber-800 dark:text-amber-300">
                      This source is in Draft — complete these steps to activate:
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
                      {!connectionPassed && source.connection_test_status === "failure" && (
                        <span className="text-xs text-destructive ml-1">— Failed: {source.connection_test_error || "check credentials"}</span>
                      )}
                      {!connectionPassed && !source.connection_test_status && (
                        <span className="text-xs text-muted-foreground ml-1">— not tested yet</span>
                      )}
                    </div>
                    <div className="flex items-center gap-2 text-sm">
                      {schemasDiscovered
                        ? <CheckCircle2 className="h-4 w-4 text-green-600" />
                        : <Circle className="h-4 w-4 text-muted-foreground" />}
                      <span className={schemasDiscovered ? "text-green-700 dark:text-green-400" : ""}>
                        Discover Schemas ({totalTables} table{totalTables !== 1 ? "s" : ""} found)
                      </span>
                      {!schemasDiscovered && !source.last_discovery_at && (
                        <span className="text-xs text-muted-foreground ml-1">— not discovered yet</span>
                      )}
                      {!schemasDiscovered && source.last_discovery_at && totalTables === 0 && (
                        <span className="text-xs text-destructive ml-1">— 0 tables found</span>
                      )}
                    </div>
                  </div>
                  {connectionPassed && schemasDiscovered && (
                    <p className="text-xs text-green-700 pl-6">All checks passed — source will activate automatically.</p>
                  )}
                </CardContent>
              </Card>
            );
          })()}

          <Card>
            <CardHeader><CardTitle>Connection Info</CardTitle></CardHeader>
            <CardContent>
              <dl className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                <div>
                  <dt className="text-muted-foreground">Host</dt>
                  <dd className="font-mono">{config.host ?? "—"}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Port</dt>
                  <dd className="font-mono">{config.port ?? "—"}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Database</dt>
                  <dd className="font-mono">{config.database ?? "—"}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Connector Type</dt>
                  <dd className="capitalize">{source.connector_definition_type}</dd>
                </div>
              </dl>
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle>Last Connection Test</CardTitle></CardHeader>
            <CardContent className="flex items-center gap-4">
              {source.connection_test_status ? (
                <>
                  <Badge variant={source.connection_test_status === "success" ? "success" : "destructive"}>
                    {source.connection_test_status}
                  </Badge>
                  <span className="text-sm text-muted-foreground">
                    {source.connection_test_at ? new Date(source.connection_test_at).toLocaleString() : ""}
                  </span>
                  {source.connection_test_error && (
                    <span className="text-xs text-destructive">{source.connection_test_error}</span>
                  )}
                </>
              ) : (
                <span className="text-sm text-muted-foreground">No test performed yet</span>
              )}
            </CardContent>
          </Card>

          <div className="flex flex-wrap gap-2">
            <Button onClick={() => testMutation.mutate()} disabled={testMutation.isPending}>
              <Plug className="h-4 w-4 mr-2" />
              {testMutation.isPending ? "Testing..." : "Test Connection"}
            </Button>
            <Button variant="outline" onClick={() => discoverMutation.mutate()} disabled={discoverMutation.isPending}>
              <RefreshCw className="h-4 w-4 mr-2" />
              Discover Schemas
            </Button>
            <Button variant="outline" onClick={() => navigate(`/sources/${id}/edit`)}>
              <Pencil className="h-4 w-4 mr-2" />
              Edit
            </Button>
            <Button variant="destructive" onClick={() => { if (confirm("Delete this source?")) deleteMutation.mutate(); }}>
              <Trash2 className="h-4 w-4 mr-2" />
              Delete
            </Button>
          </div>
          {testMutation.isSuccess && <p className="text-sm text-green-600">✓ Connection successful</p>}
          {testMutation.isError && <p className="text-sm text-destructive">✗ Connection failed</p>}
        </TabsContent>

        {/* Schema Discovery Tab */}
        <TabsContent value="schema" className="space-y-4">
          <div className="flex items-center justify-between">
            <h3 className="text-lg font-semibold">Discovered Tables & Columns</h3>
            <Button variant="outline" onClick={() => refetchSchema()} disabled={schemaFetching}>
              <RefreshCw className={`h-4 w-4 mr-2 ${schemaFetching ? "animate-spin" : ""}`} />
              Refresh Schema
            </Button>
          </div>
          {schema?.tables?.length ? (
            schema.tables.map((table: any) => (
              <Card key={table.table_name ?? table.name}>
                <CardHeader className="py-3">
                  <CardTitle className="text-base font-medium">{table.schema_name ? `${table.schema_name}.` : ""}{table.table_name ?? table.name}</CardTitle>
                </CardHeader>
                <CardContent className="p-0">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Column</TableHead>
                        <TableHead>Type</TableHead>
                        <TableHead>Nullable</TableHead>
                        <TableHead>Key</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {table.columns?.map((col: any, idx: number) => (
                        <TableRow key={col.column_name ?? idx}>
                          <TableCell className="font-mono text-sm">{col.column_name}</TableCell>
                          <TableCell className="text-sm">{col.data_type}</TableCell>
                          <TableCell>{col.is_nullable ? "Yes" : "No"}</TableCell>
                          <TableCell>
                            {col.is_primary_key && <Badge variant="default" className="text-xs">PK</Badge>}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </CardContent>
              </Card>
            ))
          ) : (
            <Card>
              <CardContent className="py-8 text-center text-muted-foreground">
                No schema discovered yet. Click "Refresh Schema" to discover tables.
              </CardContent>
            </Card>
          )}
        </TabsContent>

        {/* CDC Config Tab */}
        <TabsContent value="cdc-config" className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <Card>
              <CardHeader className="pb-2"><CardTitle className="text-sm text-muted-foreground">Binlog Format</CardTitle></CardHeader>
              <CardContent><p className="text-lg font-semibold">{config.binlog_format ?? source.cdc_config?.binlog_format ?? "ROW"}</p></CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2"><CardTitle className="text-sm text-muted-foreground">Server ID</CardTitle></CardHeader>
              <CardContent><p className="text-lg font-semibold font-mono">{config.server_id ?? source.cdc_config?.server_id ?? "—"}</p></CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2"><CardTitle className="text-sm text-muted-foreground">GTID Mode</CardTitle></CardHeader>
              <CardContent><p className="text-lg font-semibold">{config.gtid_mode ?? source.cdc_config?.gtid_mode ?? "OFF"}</p></CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2"><CardTitle className="text-sm text-muted-foreground">Replication Lag</CardTitle></CardHeader>
              <CardContent>
                <p className="text-lg font-semibold">
                  {stats?.replication_lag_ms != null ? `${stats.replication_lag_ms}ms` : "—"}
                </p>
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        {/* Statistics Tab */}
        <TabsContent value="statistics" className="space-y-4">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <Card>
              <CardContent className="pt-6">
                <div className="flex items-center gap-2">
                  <Activity className="h-4 w-4 text-blue-500" />
                  <span className="text-sm text-muted-foreground">Events Captured</span>
                </div>
                <p className="text-2xl font-bold mt-1">{stats?.events_captured?.toLocaleString() ?? "0"}</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="pt-6">
                <div className="flex items-center gap-2">
                  <Database className="h-4 w-4 text-green-500" />
                  <span className="text-sm text-muted-foreground">Tables Monitored</span>
                </div>
                <p className="text-2xl font-bold mt-1">{stats?.tables_monitored ?? "0"}</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="pt-6">
                <div className="flex items-center gap-2">
                  <AlertTriangle className="h-4 w-4 text-red-500" />
                  <span className="text-sm text-muted-foreground">Errors (24h)</span>
                </div>
                <p className="text-2xl font-bold mt-1">{stats?.errors_24h ?? "0"}</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="pt-6">
                <div className="flex items-center gap-2">
                  <Clock className="h-4 w-4 text-purple-500" />
                  <span className="text-sm text-muted-foreground">Uptime %</span>
                </div>
                <p className="text-2xl font-bold mt-1">{stats?.uptime_percent != null ? `${stats.uptime_percent}%` : "—"}</p>
              </CardContent>
            </Card>
          </div>

          <Card>
            <CardHeader><CardTitle>Events Over Time</CardTitle></CardHeader>
            <CardContent>
              {stats?.events_timeline?.length ? (
                <ResponsiveContainer width="100%" height={300}>
                  <LineChart data={stats.events_timeline}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="time" tick={{ fontSize: 12 }} />
                    <YAxis tick={{ fontSize: 12 }} />
                    <Tooltip />
                    <Line type="monotone" dataKey="count" stroke="#3b82f6" strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              ) : (
                <p className="text-center text-muted-foreground py-8">No event data available yet.</p>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
