import React, { useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import {
  Pause,
  Play,
  RefreshCw,
  Pencil,
  Trash2,
  ArrowRight,
  Activity,
  Clock,
  TrendingUp,
  Timer,
  ChevronDown,
  ChevronUp,
} from "lucide-react";
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";

const CONNECTOR_ICONS: Record<string, string> = {
  mysql: "🐬",
  postgresql: "🐘",
  mongodb: "🍃",
  iceberg: "🧊",
  kafka: "📨",
  s3: "☁️",
};

function getConnectorIcon(type: string) {
  return CONNECTOR_ICONS[type?.toLowerCase()] ?? "🔗";
}

export function ConnectionDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [expandedRuns, setExpandedRuns] = useState<Set<string>>(new Set());

  const toggleRunExpand = (runKey: string) => {
    setExpandedRuns((prev) => {
      const next = new Set(prev);
      if (next.has(runKey)) next.delete(runKey);
      else next.add(runKey);
      return next;
    });
  };

  const { data: connection, isLoading } = useQuery({
    queryKey: ["connections", id],
    queryFn: () => api.get(`/connections/${id}`).then((r) => r.data),
  });

  const { data: streams = [] } = useQuery({
    queryKey: ["connections", id, "streams"],
    queryFn: () => fetchList(`/streams/connections/${id}/streams`, "streams").catch(() => []),
    enabled: !!id,
  });

  const { data: stats } = useQuery({
    queryKey: ["connections", id, "stats"],
    queryFn: () => api.get(`/connections/${id}/stats`).then((r) => r.data).catch(() => null),
    enabled: !!id,
    refetchInterval: 15000,
  });

  const { data: runs = [] } = useQuery({
    queryKey: ["connections", id, "runs"],
    queryFn: () => fetchList(`/connections/${id}/runs`).catch(() => []),
    enabled: !!id,
    // Auto-refresh while any run is still in "running" state
    refetchInterval: (query) => {
      const data = query.state.data as any[] | undefined;
      const hasRunning = (data ?? []).some((r: any) => r.status === "running");
      return hasRunning ? 3000 : false;
    },
  });

  const { data: health } = useQuery({
    queryKey: ["connections", id, "health"],
    queryFn: () => api.get(`/connections/${id}/health`).then((r) => r.data).catch(() => null),
    enabled: !!id,
    refetchInterval: 30000,
  });

  const pauseMutation = useMutation({
    mutationFn: () => api.post(`/connections/${id}/pause`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["connections", id] }),
  });

  const resumeMutation = useMutation({
    mutationFn: () => api.post(`/connections/${id}/resume`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["connections", id] }),
  });

  const syncMutation = useMutation({
    mutationFn: () => api.post(`/connections/${id}/trigger-sync`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["connections", id] }),
  });

  const deleteMutation = useMutation({
    mutationFn: () => api.delete(`/connections/${id}`),
    onSuccess: () => navigate("/connections"),
  });

  const toggleStreamMutation = useMutation({
    mutationFn: ({ streamId, enabled }: { streamId: string; enabled: boolean }) =>
      api.patch(`/connections/${id}/streams/${streamId}`, { enabled }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["connections", id, "streams"] }),
  });

  if (isLoading) return <div className="flex items-center justify-center h-64">Loading...</div>;
  if (!connection) return <div className="text-center text-muted-foreground">Connection not found</div>;

  const isActive = connection.status === "active" || connection.status === "running";
  const isPaused = connection.status === "paused";

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-2xl font-bold">{connection.connection_name}</h1>
          {isActive && <Badge className="bg-green-500/10 text-green-600 border-green-200"><span className="mr-1">●</span>Active</Badge>}
          {isPaused && <Badge variant="secondary"><span className="mr-1">⏸</span>Paused</Badge>}
          {connection.status === "error" && <Badge variant="destructive">Error</Badge>}
        </div>
      </div>

      {/* Pipeline Diagram */}
      <Card>
        <CardContent className="py-6">
          <div className="flex items-center justify-center gap-4">
            <div className="flex flex-col items-center gap-1 rounded-lg border p-4 min-w-[140px]">
              <span className="text-2xl">{getConnectorIcon(connection.source?.connector_type)}</span>
              <span className="font-medium text-sm">{connection.source?.connector_type?.toUpperCase() ?? "SOURCE"}</span>
              <span className="text-xs text-muted-foreground">{connection.source?.source_name}</span>
            </div>
            <ArrowRight className="h-5 w-5 text-muted-foreground" />
            <div className="flex flex-col items-center gap-1 rounded-lg border border-dashed p-4 min-w-[140px]">
              <Activity className="h-6 w-6 text-primary" />
              <span className="font-medium text-sm">Pipeline</span>
              <span className="text-xs text-muted-foreground">Transform + DQ</span>
            </div>
            <ArrowRight className="h-5 w-5 text-muted-foreground" />
            <div className="flex flex-col items-center gap-1 rounded-lg border p-4 min-w-[140px]">
              <span className="text-2xl">{getConnectorIcon(connection.destination?.connector_type)}</span>
              <span className="font-medium text-sm">{connection.destination?.connector_type?.toUpperCase() ?? "DESTINATION"}</span>
              <span className="text-xs text-muted-foreground">{connection.destination?.destination_name}</span>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Stat Cards */}
      <div className="grid grid-cols-4 gap-4">
        <Card>
          <CardContent className="pt-4 pb-4">
            <div className="flex items-center gap-2 text-muted-foreground text-sm mb-1">
              <TrendingUp className="h-4 w-4" />Events/hr
            </div>
            <p className="text-2xl font-bold">{stats?.events_per_hour != null ? stats.events_per_hour.toLocaleString() : "—"}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-4 pb-4">
            <div className="flex items-center gap-2 text-muted-foreground text-sm mb-1">
              <Timer className="h-4 w-4" />Lag
            </div>
            <p className="text-2xl font-bold">{stats?.lag_seconds != null ? (stats.lag_seconds < 1 ? "< 1s" : `${stats.lag_seconds}s`) : "—"}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-4 pb-4">
            <div className="flex items-center gap-2 text-muted-foreground text-sm mb-1">
              <Activity className="h-4 w-4" />Uptime
            </div>
            <p className="text-2xl font-bold">{stats?.uptime_percent != null ? `${stats.uptime_percent}%` : "—"}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-4 pb-4">
            <div className="flex items-center gap-2 text-muted-foreground text-sm mb-1">
              <Clock className="h-4 w-4" />Last Sync
            </div>
            <p className="text-2xl font-bold">{stats?.last_sync ?? "—"}</p>
          </CardContent>
        </Card>
      </div>

      {/* Action Bar */}
      <div className="flex items-center gap-2">
        {isActive ? (
          <Button variant="outline" onClick={() => pauseMutation.mutate()} disabled={pauseMutation.isPending}>
            <Pause className="mr-2 h-4 w-4" />Pause
          </Button>
        ) : (
          <Button variant="outline" onClick={() => resumeMutation.mutate()} disabled={resumeMutation.isPending}>
            <Play className="mr-2 h-4 w-4" />Resume
          </Button>
        )}
        <Button variant="outline" onClick={() => syncMutation.mutate()} disabled={syncMutation.isPending}>
          <RefreshCw className="mr-2 h-4 w-4" />Trigger Sync
        </Button>
        <Button variant="outline" onClick={() => navigate(`/connections/${id}/edit`)}>
          <Pencil className="mr-2 h-4 w-4" />Edit
        </Button>
        <Button variant="destructive" onClick={() => { if (confirm("Delete this connection?")) deleteMutation.mutate(); }}>
          <Trash2 className="mr-2 h-4 w-4" />Delete
        </Button>
      </div>

      {/* Quick Config */}
      <Card>
        <CardHeader><CardTitle className="text-sm font-medium text-muted-foreground">Quick Config</CardTitle></CardHeader>
        <CardContent>
          <dl className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
            <div><dt className="text-muted-foreground">Sync Mode</dt><dd className="font-medium">{connection.sync_mode?.toUpperCase()}</dd></div>
            <div><dt className="text-muted-foreground">Schedule</dt><dd className="font-medium">{connection.sync_frequency ?? "Continuous"}</dd></div>
            <div><dt className="text-muted-foreground">Schema Policy</dt><dd className="font-medium">{connection.schema_evolution_policy ?? "Auto Apply"}</dd></div>
            <div><dt className="text-muted-foreground">Created</dt><dd className="font-medium">{connection.created_at ? new Date(connection.created_at).toLocaleString() : "—"}</dd></div>
          </dl>
        </CardContent>
      </Card>

      {/* Tabs */}
      <Tabs defaultValue="overview">
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="streams">Streams</TabsTrigger>
          <TabsTrigger value="transforms">Transforms</TabsTrigger>
          <TabsTrigger value="data-quality">Data Quality</TabsTrigger>
          <TabsTrigger value="schema-evolution">Schema Evolution</TabsTrigger>
          <TabsTrigger value="sync-runs">Sync Runs</TabsTrigger>
          <TabsTrigger value="health">Health</TabsTrigger>
        </TabsList>

        <TabsContent value="overview">
          <Card>
            <CardHeader><CardTitle>Connection Details</CardTitle></CardHeader>
            <CardContent>
              <dl className="grid grid-cols-2 gap-4 text-sm">
                <div><dt className="text-muted-foreground">ID</dt><dd className="font-mono">{connection.connection_id}</dd></div>
                <div><dt className="text-muted-foreground">Source</dt><dd>{connection.source?.source_name ?? connection.source_id}</dd></div>
                <div><dt className="text-muted-foreground">Destination</dt><dd>{connection.destination?.destination_name ?? connection.destination_id}</dd></div>
                <div><dt className="text-muted-foreground">Mode</dt><dd>{connection.sync_mode}</dd></div>
                <div><dt className="text-muted-foreground">Created by</dt><dd>{connection.created_by ?? "—"}</dd></div>
                <div><dt className="text-muted-foreground">Streams</dt><dd>{streams.length} configured</dd></div>
              </dl>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="streams">
          <Card>
            <CardHeader><CardTitle>Configured Streams</CardTitle></CardHeader>
            <CardContent className="p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Enabled</TableHead>
                    <TableHead>Schema</TableHead>
                    <TableHead>Table</TableHead>
                    <TableHead>Sync Mode</TableHead>
                    <TableHead>Primary Key</TableHead>
                    <TableHead>Cursor Field</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {streams.length === 0 ? (
                    <TableRow><TableCell colSpan={6} className="text-center text-muted-foreground">No streams configured</TableCell></TableRow>
                  ) : (
                    streams.map((stream: any) => (
                      <TableRow key={stream.stream_id}>
                        <TableCell>
                          <button
                            className={`w-10 h-5 rounded-full relative transition-colors ${stream.is_enabled !== false ? "bg-green-500" : "bg-gray-300"}`}
                            onClick={() => toggleStreamMutation.mutate({ streamId: stream.stream_id, enabled: stream.is_enabled === false })}
                          >
                            <span className={`absolute top-0.5 w-4 h-4 rounded-full bg-white shadow transition-transform ${stream.is_enabled !== false ? "left-5" : "left-0.5"}`} />
                          </button>
                        </TableCell>
                        <TableCell className="text-muted-foreground">{stream.source_schema_name ?? "—"}</TableCell>
                        <TableCell className="font-medium font-mono text-sm">{stream.stream_name ?? stream.table_name}</TableCell>
                        <TableCell><Badge variant="outline">{stream.sync_mode ?? "CDC"}</Badge></TableCell>
                        <TableCell className="font-mono text-xs">{Array.isArray(stream.primary_keys) ? stream.primary_keys.join(", ") : (stream.primary_keys ?? stream.primary_key ?? "—")}</TableCell>
                        <TableCell className="text-xs text-muted-foreground">{stream.cursor_field ?? "(auto)"}</TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="transforms">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center justify-between">Transform Pipeline
                <Button size="sm" onClick={() => navigate("/transformations/new")}>Attach Pipeline</Button>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-muted-foreground text-sm">
                No transform pipeline configured for this connection. Create a pipeline in the Transformations section and attach it here.
              </p>
              <Button variant="link" className="p-0 h-auto mt-2 text-sm" onClick={() => navigate("/transformations")}>Browse Transformations →</Button>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="data-quality">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center justify-between">Data Quality
                <Button size="sm" onClick={() => navigate("/data-quality/policies/new")}>Create Policy</Button>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-muted-foreground text-sm">
                No data quality policy attached. Create a policy in the Data Quality section.
              </p>
              <Button variant="link" className="p-0 h-auto mt-2 text-sm" onClick={() => navigate("/data-quality")}>Browse DQ Policies →</Button>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="schema-evolution">
          <Card>
            <CardHeader><CardTitle>Schema Evolution</CardTitle></CardHeader>
            <CardContent>
              <div className="flex items-center gap-2 text-sm">
                <span>Policy:</span>
                <Badge variant="outline">{connection.schema_evolution_policy ?? "auto_apply"}</Badge>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="sync-runs">
          <div className="space-y-4">
            {runs.length === 0 ? (
              <Card>
                <CardContent className="py-10 text-center text-muted-foreground">
                  No sync runs recorded yet. Trigger a sync or wait for the initial load to complete.
                </CardContent>
              </Card>
            ) : (
              runs.map((run: any) => {
                const runKey = run.id ?? String(run.run_number);
                const isExpanded = expandedRuns.has(runKey);
                const isInitial = run.run_type === "initial_load" || run.is_first_sync;
                const hasTables = run.tables?.length > 0;

                const fmtDuration = (sec: number | null) => {
                  if (sec == null) return "—";
                  if (sec >= 3600) return `${Math.floor(sec/3600)}h ${Math.floor((sec%3600)/60)}m`;
                  if (sec >= 60) return `${Math.floor(sec/60)}m ${sec%60}s`;
                  return `${sec}s`;
                };
                const fmtNum = (n: number) => n?.toLocaleString() ?? "0";

                return (
                  <Card key={runKey} className="overflow-hidden">
                    <CardHeader
                      className="pb-3 cursor-pointer hover:bg-muted/30"
                      onClick={() => toggleRunExpand(runKey)}
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-3">
                          <div className="flex items-center gap-2 font-semibold">
                            {isExpanded
                              ? <ChevronUp className="h-4 w-4 text-muted-foreground" />
                              : <ChevronDown className="h-4 w-4 text-muted-foreground" />}
                            {isInitial ? (
                              <span>🚀 Initial Load</span>
                            ) : (
                              <span>Run #{run.run_number}</span>
                            )}
                          </div>
                          {run.status === "completed" && <Badge className="bg-green-500/10 text-green-600 border-green-200">● Completed</Badge>}
                          {run.status === "success" && <Badge className="bg-green-500/10 text-green-600 border-green-200">● Success</Badge>}
                          {run.status === "failed" && <Badge variant="destructive">✗ Failed</Badge>}
                          {run.status === "running" && <Badge variant="secondary" className="animate-pulse">⟳ Running</Badge>}
                          {run.status === "cancelled" && <Badge variant="outline">⊘ Cancelled</Badge>}
                          {isInitial && <Badge variant="outline" className="text-xs">Full Table Copy</Badge>}
                        </div>
                        <div className="flex items-center gap-6 text-sm text-muted-foreground">
                          <span>
                            {run.started_at ? new Date(run.started_at).toLocaleString() : "—"}
                            {run.completed_at ? ` → ${new Date(run.completed_at).toLocaleString()}` : ""}
                          </span>
                          <span className="font-medium text-foreground">{fmtDuration(run.duration)}</span>
                        </div>
                      </div>

                      {/* Summary row */}
                      <div className="flex flex-wrap items-center gap-6 mt-2 text-sm">
                        <div className="flex items-center gap-1.5">
                          <span className="text-green-600 font-bold text-lg">{fmtNum(run.records_inserted ?? run.records_synced ?? 0)}</span>
                          <span className="text-muted-foreground">inserted</span>
                        </div>
                        {!isInitial && (
                          <>
                            <div className="flex items-center gap-1.5">
                              <span className="text-blue-600 font-bold text-lg">{fmtNum(run.records_updated ?? 0)}</span>
                              <span className="text-muted-foreground">updated</span>
                            </div>
                            <div className="flex items-center gap-1.5">
                              <span className="text-red-500 font-bold text-lg">{fmtNum(run.records_deleted ?? 0)}</span>
                              <span className="text-muted-foreground">deleted</span>
                            </div>
                          </>
                        )}
                        <div className="flex items-center gap-1.5">
                          <span className="font-medium">{fmtNum(run.records_read ?? run.records_synced ?? 0)}</span>
                          <span className="text-muted-foreground">rows read</span>
                        </div>
                        {run.trigger_type && (
                          <Badge variant="outline" className="text-xs capitalize">{run.trigger_type.replace(/_/g, " ")}</Badge>
                        )}
                      </div>
                    </CardHeader>

                    {isExpanded && (
                      <CardContent className="pt-0 border-t bg-muted/20">
                        {run.error_message && (
                          <div className="mb-4 p-3 rounded-md bg-destructive/10 border border-destructive/30 text-sm text-destructive">
                            <strong>Error:</strong> {run.error_message}
                          </div>
                        )}

                        {hasTables && (
                          <div className="mt-3">
                            <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wide mb-2">Table-level Details</p>
                            <div className="rounded-md border overflow-hidden">
                              <table className="w-full text-sm">
                                <thead className="bg-muted/50">
                                  <tr>
                                    <th className="text-left px-3 py-2 font-medium">Table</th>
                                    <th className="text-right px-3 py-2 font-medium text-green-600">Inserted</th>
                                    <th className="text-right px-3 py-2 font-medium text-blue-600">Updated</th>
                                    <th className="text-right px-3 py-2 font-medium text-red-500">Deleted</th>
                                    <th className="text-left px-3 py-2 font-medium">Status</th>
                                    <th className="text-left px-3 py-2 font-medium">Started</th>
                                    <th className="text-left px-3 py-2 font-medium">Completed</th>
                                    <th className="text-right px-3 py-2 font-medium">Duration</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {run.tables.map((t: any, idx: number) => {
                                    const tDur = t.started_at && t.completed_at
                                      ? Math.round((new Date(t.completed_at).getTime() - new Date(t.started_at).getTime()) / 1000)
                                      : null;
                                    return (
                                      <tr key={t.table_name ?? idx} className="border-t hover:bg-muted/30">
                                        <td className="px-3 py-2 font-mono font-medium">{t.table_name}</td>
                                        <td className="px-3 py-2 text-right text-green-600 font-semibold">{fmtNum(t.rows_inserted ?? 0)}</td>
                                        <td className="px-3 py-2 text-right text-blue-600 font-semibold">{fmtNum(t.rows_updated ?? 0)}</td>
                                        <td className="px-3 py-2 text-right text-red-500 font-semibold">{fmtNum(t.rows_deleted ?? 0)}</td>
                                        <td className="px-3 py-2">
                                          {t.status === "completed" ? <span className="text-green-600">✓ Done</span>
                                            : t.status === "failed" ? <span className="text-destructive">✗ Failed</span>
                                            : <span className="text-amber-500">⟳ {t.status}</span>}
                                        </td>
                                        <td className="px-3 py-2 text-xs text-muted-foreground">
                                          {t.started_at ? new Date(t.started_at).toLocaleTimeString() : "—"}
                                        </td>
                                        <td className="px-3 py-2 text-xs text-muted-foreground">
                                          {t.completed_at ? new Date(t.completed_at).toLocaleTimeString() : "—"}
                                        </td>
                                        <td className="px-3 py-2 text-right text-xs">{fmtDuration(tDur)}</td>
                                      </tr>
                                    );
                                  })}
                                </tbody>
                              </table>
                            </div>
                          </div>
                        )}

                        {/* CDC run: show snapshot details if available */}
                        {!isInitial && run.run_config?.snapshot && (
                          <div className="mt-3 text-xs font-mono space-y-1 text-muted-foreground">
                            {run.run_config.snapshot.worker_id && (
                              <p>Worker: {run.run_config.snapshot.worker_id}</p>
                            )}
                            {run.run_config.snapshot.binlog_position && (
                              <p>Binlog: {run.run_config.snapshot.binlog_position}</p>
                            )}
                            {run.run_config.snapshot.checkpoint_at && (
                              <p>Checkpoint: {new Date(run.run_config.snapshot.checkpoint_at).toLocaleString()}</p>
                            )}
                          </div>
                        )}
                      </CardContent>
                    )}
                  </Card>
                );
              })
            )}
          </div>
        </TabsContent>

        <TabsContent value="health">
          <div className="space-y-4">
            {/* Lag Chart */}
            <Card>
              <CardHeader><CardTitle>Replication Lag</CardTitle></CardHeader>
              <CardContent>
                {health?.lag_history?.length > 0 ? (
                  <ResponsiveContainer width="100%" height={200}>
                    <LineChart data={health.lag_history}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="time" tick={{ fontSize: 11 }} />
                      <YAxis unit="s" tick={{ fontSize: 11 }} />
                      <Tooltip />
                      <Line type="monotone" dataKey="lag" stroke="hsl(var(--primary))" strokeWidth={2} dot={false} />
                    </LineChart>
                  </ResponsiveContainer>
                ) : (
                  <p className="text-sm text-muted-foreground">No lag data available yet.</p>
                )}
              </CardContent>
            </Card>

            {/* Throughput Chart */}
            <Card>
              <CardHeader><CardTitle>Throughput (events/sec)</CardTitle></CardHeader>
              <CardContent>
                {health?.throughput_history?.length > 0 ? (
                  <ResponsiveContainer width="100%" height={200}>
                    <BarChart data={health.throughput_history}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="time" tick={{ fontSize: 11 }} />
                      <YAxis tick={{ fontSize: 11 }} />
                      <Tooltip />
                      <Bar dataKey="events" fill="hsl(var(--primary))" radius={[2, 2, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                ) : (
                  <p className="text-sm text-muted-foreground">No throughput data available yet.</p>
                )}
              </CardContent>
            </Card>

            {/* Checkpoint Info */}
            <Card>
              <CardHeader><CardTitle>Checkpoint State</CardTitle></CardHeader>
              <CardContent>
                <dl className="grid grid-cols-2 gap-4 text-sm">
                  <div><dt className="text-muted-foreground">Binlog File</dt><dd className="font-mono">{health?.checkpoint?.binlog_file ?? "—"}</dd></div>
                  <div><dt className="text-muted-foreground">Position</dt><dd className="font-mono">{health?.checkpoint?.position ?? "—"}</dd></div>
                  <div><dt className="text-muted-foreground">GTID</dt><dd className="font-mono text-xs">{health?.checkpoint?.gtid ?? "—"}</dd></div>
                  <div><dt className="text-muted-foreground">Updated</dt><dd>{health?.checkpoint?.updated_at ? new Date(health.checkpoint.updated_at).toLocaleString() : "—"}</dd></div>
                </dl>
              </CardContent>
            </Card>
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}
