import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api, fetchList } from "@/lib/api";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Activity, Cable, Clock, ShieldCheck, ArrowUpRight, Minus, RefreshCw, Rocket } from "lucide-react";

export function DashboardPage() {
  const navigate = useNavigate();
  const [timeRange, setTimeRange] = useState("24h");

  const { data: health, refetch: refetchHealth } = useQuery({
    queryKey: ["monitoring", "health"],
    queryFn: () => api.get("/monitoring/health").then((r) => r.data),
    refetchInterval: 30000,
  });

  const { data: workersData } = useQuery({
    queryKey: ["monitoring", "workers"],
    queryFn: () => api.get("/monitoring/workers").then((r) => r.data).catch(() => null),
    refetchInterval: 30000,
  });

  const { data: connections = [] } = useQuery({
    queryKey: ["connections"],
    queryFn: () => fetchList("/connections", "connections"),
    refetchInterval: 10000,
  });

  const { data: alerts = [] } = useQuery({
    queryKey: ["alerts", "recent"],
    queryFn: () => fetchList("/alerts", "alerts", { page_size: 5, status: "active" }).catch(() => []),
  });

  const { data: dqMetrics } = useQuery({
    queryKey: ["data-quality", "dashboard"],
    queryFn: () => api.get("/data-quality/metrics/dashboard").then((r) => r.data).catch(() => null),
  });

  const activeConns = connections.filter((c: any) => c.status === "active").length;
  const pausedConns = connections.filter((c: any) => c.status === "paused").length;
  const errorConns  = connections.filter((c: any) => c.status === "error").length;
  const workerCount = workersData?.workers?.length ?? workersData?.length ?? 0;

  // Derive health service status from API response
  const dbStatus      = health?.services?.database ?? "unknown";
  const redisStatus   = health?.services?.redis     ?? "unknown";
  const workersStatus = workerCount > 0 ? "healthy" : "unknown";

  const dqScore = dqMetrics?.overall_score
    ? `${parseFloat(dqMetrics.overall_score).toFixed(1)}%`
    : "—";

  const stats = [
    { label: "Active Connections", value: activeConns, icon: Cable, color: "text-emerald-500" },
    { label: "Events/sec", value: "—", icon: Activity, color: "text-blue-500" },
    { label: "CDC Lag", value: "—", icon: Clock, color: "text-amber-500" },
    { label: "DQ Score", value: dqScore, icon: ShieldCheck, color: "text-violet-500" },
  ];

  if (connections.length === 0) {
    return (
      <div className="flex min-h-[60vh] items-center justify-center">
        <Card className="w-full max-w-lg text-center">
          <CardContent className="space-y-4 p-8">
            <Rocket className="mx-auto h-12 w-12 text-primary" />
            <h2 className="text-2xl font-bold">Welcome to Fusion</h2>
            <p className="text-muted-foreground">Set up your first CDC pipeline in minutes.</p>
            <ol className="space-y-2 text-left text-sm text-muted-foreground">
              <li>1. Add a Source (MySQL, MongoDB, PostgreSQL)</li>
              <li>2. Add a Destination (PostgreSQL, Iceberg)</li>
              <li>3. Create a Connection to start syncing</li>
            </ol>
            <Button onClick={() => navigate("/sources/new")} className="mt-4">
              Get Started — Add Source →
            </Button>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Dashboard</h1>
        <div className="flex items-center gap-2">
          <Select value={timeRange} onValueChange={setTimeRange}>
            <SelectTrigger className="w-32"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="1h">Last 1h</SelectItem>
              <SelectItem value="6h">Last 6h</SelectItem>
              <SelectItem value="24h">Last 24h</SelectItem>
              <SelectItem value="7d">Last 7d</SelectItem>
            </SelectContent>
          </Select>
          <Button variant="ghost" size="icon" onClick={() => refetchHealth()}>
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>
      </div>

      {/* Stat Cards */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        {stats.map((stat) => (
          <Card key={stat.label}>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">{stat.label}</CardTitle>
              <stat.icon className={`h-4 w-4 ${stat.color}`} />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stat.value}</div>
              <div className="flex items-center gap-1 text-xs mt-1">
                <Minus className="h-3 w-3 text-muted-foreground" />
                <span className="text-muted-foreground">Live data</span>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Pipeline Status + Alerts */}
      <div className="grid gap-4 lg:grid-cols-3">
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle className="flex items-center justify-between">
              Pipeline Status
              <div className="flex gap-2 text-xs font-normal">
                <Badge variant="success">{activeConns} active</Badge>
                <Badge variant="secondary">{pausedConns} paused</Badge>
                {errorConns > 0 && <Badge variant="destructive">{errorConns} error</Badge>}
              </div>
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-1">
              <div className="flex h-3 overflow-hidden rounded-full bg-muted">
                {connections.length > 0 && activeConns > 0 && (
                  <div className="bg-emerald-500" style={{ width: `${(activeConns / connections.length) * 100}%` }} />
                )}
                {connections.length > 0 && pausedConns > 0 && (
                  <div className="bg-gray-400" style={{ width: `${(pausedConns / connections.length) * 100}%` }} />
                )}
                {connections.length > 0 && errorConns > 0 && (
                  <div className="bg-red-500" style={{ width: `${(errorConns / connections.length) * 100}%` }} />
                )}
              </div>
            </div>
            <div className="mt-4 space-y-2">
              {connections.slice(0, 5).map((conn: any) => (
                <div
                  key={conn.connection_id}
                  className="flex items-center justify-between rounded-md border px-3 py-2 hover:bg-muted/50 cursor-pointer"
                  onClick={() => navigate(`/connections/${conn.connection_id}`)}
                >
                  <div className="flex items-center gap-2">
                    <div className={`h-2 w-2 rounded-full ${
                      conn.status === "active" ? "bg-emerald-500"
                      : conn.status === "paused" ? "bg-gray-400"
                      : "bg-red-500"
                    }`} />
                    <span className="text-sm font-medium">{conn.connection_name}</span>
                  </div>
                  <div className="flex items-center gap-3">
                    <Badge variant={conn.status === "active" ? "success" : conn.status === "error" ? "destructive" : "secondary"} className="text-xs">
                      {conn.status === "active" ? "● Active" : conn.status === "paused" ? "⏸ Paused" : "⚠ Error"}
                    </Badge>
                    {conn.last_sync_at && (
                      <span className="text-xs text-muted-foreground">
                        {new Date(conn.last_sync_at).toLocaleTimeString()}
                      </span>
                    )}
                  </div>
                </div>
              ))}
            </div>
            {connections.length > 5 && (
              <Button variant="link" className="mt-2 p-0 h-auto" onClick={() => navigate("/connections")}>
                View all →
              </Button>
            )}
          </CardContent>
        </Card>

        <div className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Recent Alerts</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {(alerts as any[]).length > 0 ? (alerts as any[]).slice(0, 4).map((alert: any) => (
                <div key={alert.alert_id ?? alert.id} className="flex items-start gap-2 cursor-pointer" onClick={() => navigate(`/alerts/${alert.alert_id ?? alert.id}`)}>
                  <span className="mt-0.5">
                    {alert.severity === "critical" ? "🔴" : alert.severity === "warning" ? "🟡" : "🟢"}
                  </span>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium truncate">{alert.alert_name ?? alert.title ?? alert.message ?? "Alert"}</p>
                    <p className="text-xs text-muted-foreground">{alert.connection_name ?? ""}</p>
                  </div>
                </div>
              )) : (
                <p className="text-sm text-muted-foreground">No active alerts</p>
              )}
              <Button variant="link" className="p-0 h-auto text-xs" onClick={() => navigate("/alerts")}>
                View all →
              </Button>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-base">System Health</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2">
              {[
                { name: "PostgreSQL Meta DB", status: dbStatus },
                { name: "Redis", status: redisStatus },
                { name: "CDC Workers", status: workersStatus },
              ].map((svc) => (
                <div key={svc.name} className="flex items-center justify-between text-sm">
                  <div className="flex items-center gap-2">
                    <div className={`h-2 w-2 rounded-full ${
                      svc.status === "healthy" || svc.status === "ok" ? "bg-emerald-500" : "bg-amber-500"
                    }`} />
                    <span>{svc.name}</span>
                  </div>
                  <span className={`text-xs font-medium ${
                    svc.status === "healthy" || svc.status === "ok" ? "text-emerald-600" : "text-amber-600"
                  }`}>
                    {svc.status === "healthy" || svc.status === "ok" ? "OK" : svc.status === "unknown" ? "—" : svc.status.toUpperCase()}
                  </span>
                </div>
              ))}
              <div className="flex items-center justify-between text-sm pt-1 border-t mt-1">
                <span className="text-muted-foreground">Active Workers</span>
                <span className="text-xs font-medium">{workerCount}</span>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>

      {/* DQ Summary */}
      {dqMetrics && (
        <Card>
          <CardHeader>
            <CardTitle>Data Quality Overview</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-3 md:grid-cols-6 gap-4">
              {Object.entries(dqMetrics.score_by_category ?? {}).map(([cat, score]: [string, any]) => (
                <div key={cat} className="text-center">
                  <div className="text-lg font-bold">{parseFloat(score).toFixed(0)}%</div>
                  <div className="text-xs text-muted-foreground capitalize">{cat}</div>
                </div>
              ))}
            </div>
            <div className="mt-4 text-sm text-muted-foreground">
              {dqMetrics.total_connections} connections monitored · {dqMetrics.total_violations ?? 0} active violations
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
