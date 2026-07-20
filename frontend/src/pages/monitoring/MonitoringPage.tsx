import { useState, useEffect, useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import { api, fetchList } from "@/lib/api";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { Activity, Server, Database, RefreshCw, Terminal, Box, ExternalLink } from "lucide-react";

// Known static infrastructure services (always shown)
const STATIC_SERVICES = [
  { name: "PostgreSQL (Metadata)", key: "database", icon: "🐘" },
  { name: "Redis (Cache/Queue)", key: "redis", icon: "🔴" },
  { name: "Kafka (Event Bus)", key: "kafka", icon: "📨" },
];

function statusColor(status: string | undefined) {
  if (status === "healthy" || status === "Running" || status === "running") return "bg-green-500";
  if (status === "degraded" || status === "Pending") return "bg-yellow-500";
  return "bg-red-500";
}

function timeSince(iso: string) {
  if (!iso) return "—";
  const diff = Date.now() - new Date(iso).getTime();
  const s = Math.floor(diff / 1000);
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  return `${Math.floor(s / 3600)}h ago`;
}

export function MonitoringPage() {
  const [activeTab, setActiveTab] = useState("overview");
  const [logPod, setLogPod] = useState("");
  const [logLines, setLogLines] = useState(100);
  const [logsData, setLogsData] = useState<{ lines: string[]; error?: string } | null>(null);
  const [logsLoading, setLogsLoading] = useState(false);
  const logsRef = useRef<HTMLDivElement>(null);

  const { data: health, refetch: refetchHealth } = useQuery({
    queryKey: ["monitoring", "health"],
    queryFn: () => api.get("/monitoring/health").then((r) => r.data),
    refetchInterval: 15000,
  });

  const { data: workersRaw, refetch: refetchWorkers } = useQuery({
    queryKey: ["monitoring", "workers"],
    queryFn: () => api.get("/monitoring/workers").then((r) => r.data),
    refetchInterval: 15000,
  });

  const { data: podsRaw, refetch: refetchPods } = useQuery({
    queryKey: ["monitoring", "pods"],
    queryFn: () => api.get("/monitoring/pods").then((r) => r.data).catch(() => ({ pods: [] })),
    refetchInterval: 30000,
  });

  const { data: resourcesRaw } = useQuery({
    queryKey: ["monitoring", "resource-usage"],
    queryFn: () => api.get("/monitoring/resource-usage").then((r) => r.data).catch(() => ({})),
    refetchInterval: 15000,
  });

  const servicesObj: Record<string, string> = health?.services ?? {};
  const workers: any[] = workersRaw?.workers ?? [];
  const pods: any[] = podsRaw?.pods ?? [];

  const fetchLogs = async (pod?: string) => {
    const target = pod ?? logPod;
    if (!target) return;
    setLogsLoading(true);
    try {
      const r = await api.get(`/monitoring/logs/${target}?lines=${logLines}`);
      setLogsData(r.data);
    } catch (e: any) {
      setLogsData({ lines: [], error: e.message ?? "Failed to fetch logs" });
    } finally {
      setLogsLoading(false);
    }
  };

  useEffect(() => {
    if (logsRef.current) logsRef.current.scrollTop = logsRef.current.scrollHeight;
  }, [logsData]);

  // All known pods from k8s + workers
  const allPodNames = [
    ...pods.map((p: any) => p.name),
    ...workers.map((w: any) => w.worker_id).filter((id: string) => !pods.some((p: any) => p.name === id)),
  ].filter(Boolean);

  const activeWorkers = workers.filter((w) => {
    if (!w.last_heartbeat_at) return false;
    const age = Date.now() - new Date(w.last_heartbeat_at).getTime();
    return age < 5 * 60 * 1000; // heartbeat within last 5 min
  });

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Activity className="h-6 w-6" />
          <h1 className="text-2xl font-bold">System Monitoring</h1>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs text-muted-foreground">Auto-refresh 15s</span>
          <Button size="sm" variant="outline" onClick={() => { refetchHealth(); refetchWorkers(); refetchPods(); }}>
            <RefreshCw className="h-4 w-4 mr-1" />Refresh
          </Button>
          <a href="http://localhost:30800/graphql?request=query%7B__typename%7D" target="_blank" rel="noreferrer">
            <Button size="sm" variant="outline">
              <ExternalLink className="h-4 w-4 mr-1" />GraphQL Explorer
            </Button>
          </a>
        </div>
      </div>

      {/* Quick stats row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="pt-4">
            <p className="text-xs text-muted-foreground mb-1">System Status</p>
            <div className="flex items-center gap-2">
              <span className={`h-3 w-3 rounded-full ${health?.status === "healthy" ? "bg-green-500" : "bg-red-500"}`} />
              <span className="font-semibold capitalize">{health?.status ?? "—"}</span>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-4">
            <p className="text-xs text-muted-foreground mb-1">Active Workers</p>
            <p className="text-2xl font-bold">{activeWorkers.length} <span className="text-sm font-normal text-muted-foreground">/ {workers.length}</span></p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-4">
            <p className="text-xs text-muted-foreground mb-1">Running Pods</p>
            <p className="text-2xl font-bold">{pods.filter((p: any) => p.phase === "Running").length} <span className="text-sm font-normal text-muted-foreground">/ {pods.length}</span></p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-4">
            <p className="text-xs text-muted-foreground mb-1">Events Processed</p>
            <p className="text-2xl font-bold">{workers.reduce((a, w) => a + (w.events_processed ?? 0), 0).toLocaleString()}</p>
          </CardContent>
        </Card>
      </div>

      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="workers">Workers ({workers.length})</TabsTrigger>
          <TabsTrigger value="pods">Platform Pods ({pods.length})</TabsTrigger>
          <TabsTrigger value="logs">Live Logs</TabsTrigger>
        </TabsList>

        {/* ── Overview tab ──────────────────────────────────── */}
        <TabsContent value="overview">
          <div className="space-y-4">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <Server className="h-4 w-4" />Core Services
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                  {STATIC_SERVICES.map((svc) => {
                    const status = servicesObj[svc.key] ?? "unknown";
                    return (
                      <div key={svc.key} className="flex items-center gap-3 rounded-lg border p-4">
                        <span className="text-2xl">{svc.icon}</span>
                        <div className="flex-1">
                          <p className="font-medium text-sm">{svc.name}</p>
                          <div className="flex items-center gap-1.5 mt-1">
                            <span className={`h-2.5 w-2.5 rounded-full ${statusColor(status)}`} />
                            <span className="text-xs capitalize text-muted-foreground">{status}</span>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <Activity className="h-4 w-4" />Worker Summary
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-3 gap-4">
                  {["mysql", "postgres", "mongo"].map((type) => {
                    const ww = workers.filter((w) => w.worker_id?.includes(type) || w.worker_type?.includes(type));
                    const active = ww.filter((w) => {
                      if (!w.last_heartbeat_at) return false;
                      return Date.now() - new Date(w.last_heartbeat_at).getTime() < 5 * 60 * 1000;
                    });
                    return (
                      <div key={type} className="rounded-lg border p-4 text-center">
                        <p className="text-xs text-muted-foreground uppercase tracking-wide mb-1">{type} CDC</p>
                        <p className="text-2xl font-bold">{active.length}</p>
                        <p className="text-xs text-muted-foreground">{ww.length} total workers</p>
                        <div className="flex justify-center gap-1 mt-2 flex-wrap">
                          {ww.map((w, i) => (
                            <span key={i} className={`h-2 w-2 rounded-full inline-block ${Date.now() - new Date(w.last_heartbeat_at).getTime() < 5 * 60 * 1000 ? "bg-green-500" : "bg-muted"}`} title={w.worker_id} />
                          ))}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <Database className="h-4 w-4" />Aggregate Stats
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-3 gap-4 text-sm">
                  <div>
                    <p className="text-muted-foreground">Total Workers</p>
                    <p className="text-xl font-semibold">{resourcesRaw?.worker_count ?? workers.length}</p>
                  </div>
                  <div>
                    <p className="text-muted-foreground">CPU Usage (all workers)</p>
                    <p className="text-xl font-semibold">{resourcesRaw?.total_cpu_percent?.toFixed(1) ?? "0"}%</p>
                  </div>
                  <div>
                    <p className="text-muted-foreground">Memory Usage</p>
                    <p className="text-xl font-semibold">{resourcesRaw?.total_memory_mb ?? 0} MB</p>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        {/* ── Workers tab ──────────────────────────────────── */}
        <TabsContent value="workers">
          <Card>
            <CardContent className="p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Worker</TableHead>
                    <TableHead>Type</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Last Heartbeat</TableHead>
                    <TableHead>Events</TableHead>
                    <TableHead>Errors</TableHead>
                    <TableHead>Last Error</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {workers.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={7} className="text-center text-muted-foreground py-8">No workers found</TableCell>
                    </TableRow>
                  ) : (
                    workers.map((w: any) => {
                      const isStale = !w.last_heartbeat_at || Date.now() - new Date(w.last_heartbeat_at).getTime() > 5 * 60 * 1000;
                      return (
                        <TableRow key={w.heartbeat_id}>
                          <TableCell>
                            <p className="font-mono text-xs truncate max-w-[200px]">{w.worker_id}</p>
                          </TableCell>
                          <TableCell><Badge variant="outline">{w.worker_type}</Badge></TableCell>
                          <TableCell>
                            <span className="flex items-center gap-1.5">
                              <span className={`h-2.5 w-2.5 rounded-full ${isStale ? "bg-red-500" : w.status === "running" ? "bg-green-500" : "bg-yellow-500"}`} />
                              <span className="text-sm">{isStale ? "stale" : w.status}</span>
                            </span>
                          </TableCell>
                          <TableCell className="text-muted-foreground text-sm">{timeSince(w.last_heartbeat_at)}</TableCell>
                          <TableCell>{(w.events_processed ?? 0).toLocaleString()}</TableCell>
                          <TableCell>
                            {(w.errors_count ?? 0) > 0 ? (
                              <span className="text-destructive font-medium">{w.errors_count}</span>
                            ) : (
                              <span className="text-muted-foreground">0</span>
                            )}
                          </TableCell>
                          <TableCell className="text-xs text-muted-foreground max-w-[180px] truncate">
                            {w.last_error ?? "—"}
                          </TableCell>
                        </TableRow>
                      );
                    })
                  )}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>

        {/* ── Platform Pods tab ──────────────────────────── */}
        <TabsContent value="pods">
          {podsRaw?.error && (
            <div className="mb-3 p-3 rounded-md bg-yellow-500/10 border border-yellow-200 text-sm text-yellow-700">
              ⚠ Could not reach Kubernetes API: {podsRaw.error}. Pod list unavailable.
            </div>
          )}
          <Card>
            <CardContent className="p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Pod Name</TableHead>
                    <TableHead>Phase</TableHead>
                    <TableHead>Ready</TableHead>
                    <TableHead>Restarts</TableHead>
                    <TableHead>Started</TableHead>
                    <TableHead>Node</TableHead>
                    <TableHead></TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {pods.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={7} className="text-center text-muted-foreground py-8">
                        {podsRaw?.error ? "Kubernetes API unreachable — RBAC permissions may be required" : "No pods found"}
                      </TableCell>
                    </TableRow>
                  ) : (
                    pods.map((p: any) => (
                      <TableRow key={p.name}>
                        <TableCell className="font-mono text-sm">{p.name}</TableCell>
                        <TableCell>
                          <Badge variant={p.phase === "Running" ? "success" : "secondary"}>{p.phase}</Badge>
                        </TableCell>
                        <TableCell>
                          <span className={p.ready ? "text-green-600 text-sm" : "text-destructive text-sm"}>
                            {p.ready ? "● Ready" : "○ Not ready"}
                          </span>
                        </TableCell>
                        <TableCell>{p.restart_count ?? 0}</TableCell>
                        <TableCell className="text-muted-foreground text-sm">{p.start_time ? new Date(p.start_time).toLocaleDateString() : "—"}</TableCell>
                        <TableCell className="text-muted-foreground text-sm">{p.node ?? "—"}</TableCell>
                        <TableCell>
                          <Button size="sm" variant="ghost" onClick={() => { setLogPod(p.name); setActiveTab("logs"); fetchLogs(p.name); }}>
                            <Terminal className="h-3 w-3 mr-1" />Logs
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>

        {/* ── Live Logs tab ──────────────────────────────── */}
        <TabsContent value="logs">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-base">
                <Terminal className="h-4 w-4" />Pod Logs
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="flex items-center gap-3 flex-wrap">
                <Select value={logPod} onValueChange={setLogPod}>
                  <SelectTrigger className="w-[280px]">
                    <SelectValue placeholder="Select a pod..." />
                  </SelectTrigger>
                  <SelectContent>
                    {allPodNames.map((name) => (
                      <SelectItem key={name} value={name}>{name}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Select value={String(logLines)} onValueChange={(v) => setLogLines(parseInt(v))}>
                  <SelectTrigger className="w-[120px]"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="50">50 lines</SelectItem>
                    <SelectItem value="100">100 lines</SelectItem>
                    <SelectItem value="250">250 lines</SelectItem>
                    <SelectItem value="500">500 lines</SelectItem>
                  </SelectContent>
                </Select>
                <Button onClick={() => fetchLogs()} disabled={!logPod || logsLoading}>
                  {logsLoading ? "Loading..." : "Fetch Logs"}
                </Button>
                {logsData && (
                  <Button variant="ghost" size="sm" onClick={() => setLogsData(null)}>Clear</Button>
                )}
              </div>

              {logsData?.error && (
                <div className="p-3 rounded-md bg-destructive/10 border border-destructive/30 text-sm text-destructive">
                  ✗ {logsData.error}
                  {logsData.error.includes("forbidden") && (
                    <p className="mt-1 text-xs">Add a ClusterRole with <code>pods/log</code> get permission to the control-plane service account.</p>
                  )}
                </div>
              )}

              {logsData && !logsData.error && (
                <div
                  ref={logsRef}
                  className="bg-black text-green-400 font-mono text-xs p-4 rounded-md overflow-auto"
                  style={{ maxHeight: "500px", minHeight: "200px" }}
                >
                  {logsData.lines.length === 0 ? (
                    <span className="text-muted-foreground">No log output</span>
                  ) : (
                    logsData.lines.map((line, i) => (
                      <div key={i} className="leading-5 hover:bg-white/5">
                        {line}
                      </div>
                    ))
                  )}
                </div>
              )}

              {!logsData && !logsLoading && (
                <div className="rounded-md border border-dashed p-8 text-center text-sm text-muted-foreground">
                  Select a pod and click "Fetch Logs" to view recent log output.
                  <br />
                  <span className="text-xs">Note: requires the control-plane service account to have <code>pods/log</code> RBAC permission in the fusion namespace.</span>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
