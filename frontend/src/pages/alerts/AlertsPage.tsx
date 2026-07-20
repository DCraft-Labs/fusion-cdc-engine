import { useState, useMemo } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api, fetchList } from "@/lib/api";
import { Card, CardContent } from "@/components/ui/card";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { AlertCircle, CheckCircle2, Clock, Search } from "lucide-react";

interface Alert {
  id: string;
  title: string;
  message: string;
  severity: "critical" | "warning" | "info";
  status: "active" | "acknowledged" | "resolved";
  connection_name?: string;
  metric_detail?: string;
  rule_name?: string;
  triggered_at: string;
}

function timeAgo(date: string): string {
  const seconds = Math.floor((Date.now() - new Date(date).getTime()) / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
}

function SeverityIcon({ severity }: { severity: string }) {
  if (severity === "critical") return <span className="text-lg">🔴</span>;
  if (severity === "warning") return <span className="text-lg">🟡</span>;
  return <span className="text-lg">🟢</span>;
}

export function AlertsPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [severityFilter, setSeverityFilter] = useState("all");
  const [statusFilter, setStatusFilter] = useState("all");
  const [connectionFilter, setConnectionFilter] = useState("all");
  const [search, setSearch] = useState("");

  const { data: alerts = [], isLoading } = useQuery<Alert[]>({
    queryKey: ["alerts"],
    queryFn: () => fetchList("/alerts", "alerts"),
  });

  const acknowledgeMutation = useMutation({
    mutationFn: (id: string) => api.post(`/alerts/${id}/acknowledge`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["alerts"] }),
  });

  const resolveMutation = useMutation({
    mutationFn: (id: string) => api.post(`/alerts/${id}/resolve`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["alerts"] }),
  });

  const filtered = useMemo(() => {
    return alerts.filter((a) => {
      if (severityFilter !== "all" && a.severity !== severityFilter) return false;
      if (statusFilter !== "all" && a.status !== statusFilter) return false;
      if (connectionFilter !== "all" && a.connection_name !== connectionFilter) return false;
      if (search && !a.title?.toLowerCase().includes(search.toLowerCase()) && !a.message?.toLowerCase().includes(search.toLowerCase())) return false;
      return true;
    });
  }, [alerts, severityFilter, statusFilter, connectionFilter, search]);

  const activeAlerts = filtered.filter((a) => a.status === "active");
  const acknowledgedAlerts = filtered.filter((a) => a.status === "acknowledged");
  const resolvedAlerts = filtered.filter((a) => a.status === "resolved");

  const criticalCount = alerts.filter((a) => a.severity === "critical" && a.status !== "resolved").length;
  const warningCount = alerts.filter((a) => a.severity === "warning" && a.status !== "resolved").length;

  const uniqueConnections = [...new Set(alerts.map((a) => a.connection_name).filter(Boolean))];

  function AlertCard({ alert }: { alert: Alert }) {
    return (
      <Card className="hover:shadow-md transition-shadow cursor-pointer" onClick={() => navigate(`/alerts/${alert.id}`)}>
        <CardContent className="p-4">
          <div className="flex items-start gap-3">
            <SeverityIcon severity={alert.severity} />
            <div className="flex-1 min-w-0">
              <div className="flex items-center justify-between gap-2">
                <h3 className="font-semibold truncate">{alert.title ?? alert.message}</h3>
                <div className="flex gap-1 shrink-0">
                  {alert.status === "active" && (
                    <>
                      <Button size="sm" variant="outline" onClick={(e) => { e.stopPropagation(); acknowledgeMutation.mutate(alert.id); }}>
                        Acknowledge
                      </Button>
                      <Button size="sm" variant="outline" onClick={(e) => { e.stopPropagation(); resolveMutation.mutate(alert.id); }}>
                        Resolve
                      </Button>
                    </>
                  )}
                  {alert.status === "acknowledged" && (
                    <Button size="sm" variant="outline" onClick={(e) => { e.stopPropagation(); resolveMutation.mutate(alert.id); }}>
                      Resolve
                    </Button>
                  )}
                </div>
              </div>
              <div className="flex items-center gap-2 mt-1 text-sm text-muted-foreground">
                {alert.connection_name && <span className="font-medium">{alert.connection_name}</span>}
                {alert.metric_detail && <span>• {alert.metric_detail}</span>}
              </div>
              <div className="flex items-center gap-2 mt-1 text-xs text-muted-foreground">
                <Clock className="h-3 w-3" />
                <span>{timeAgo(alert.triggered_at)}</span>
                {alert.rule_name && <span>• Rule: {alert.rule_name}</span>}
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (isLoading) return <div className="flex items-center justify-center h-64">Loading alerts...</div>;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Alerts</h1>
          <div className="flex items-center gap-4 mt-1 text-sm">
            {criticalCount > 0 && <span className="flex items-center gap-1">🔴 {criticalCount} Critical</span>}
            {warningCount > 0 && <span className="flex items-center gap-1">🟡 {warningCount} Warning</span>}
            {criticalCount === 0 && warningCount === 0 && <span className="text-muted-foreground">No active alerts</span>}
          </div>
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-3">
        <Select value={severityFilter} onValueChange={setSeverityFilter}>
          <SelectTrigger className="w-[140px]"><SelectValue placeholder="Severity" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Severities</SelectItem>
            <SelectItem value="critical">Critical</SelectItem>
            <SelectItem value="warning">Warning</SelectItem>
            <SelectItem value="info">Info</SelectItem>
          </SelectContent>
        </Select>
        <Select value={statusFilter} onValueChange={setStatusFilter}>
          <SelectTrigger className="w-[140px]"><SelectValue placeholder="Status" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Statuses</SelectItem>
            <SelectItem value="active">Active</SelectItem>
            <SelectItem value="acknowledged">Acknowledged</SelectItem>
            <SelectItem value="resolved">Resolved</SelectItem>
          </SelectContent>
        </Select>
        <Select value={connectionFilter} onValueChange={setConnectionFilter}>
          <SelectTrigger className="w-[160px]"><SelectValue placeholder="Connection" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Connections</SelectItem>
            {uniqueConnections.map((c) => (
              <SelectItem key={c} value={c!}>{c}</SelectItem>
            ))}
          </SelectContent>
        </Select>
        <div className="relative flex-1 min-w-[200px]">
          <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
          <Input placeholder="Search alerts..." value={search} onChange={(e) => setSearch(e.target.value)} className="pl-9" />
        </div>
      </div>

      <Tabs defaultValue="active">
        <TabsList>
          <TabsTrigger value="active" className="gap-1">
            <AlertCircle className="h-3.5 w-3.5" /> Active ({activeAlerts.length})
          </TabsTrigger>
          <TabsTrigger value="acknowledged" className="gap-1">
            <Clock className="h-3.5 w-3.5" /> Acknowledged ({acknowledgedAlerts.length})
          </TabsTrigger>
          <TabsTrigger value="resolved" className="gap-1">
            <CheckCircle2 className="h-3.5 w-3.5" /> Resolved ({resolvedAlerts.length})
          </TabsTrigger>
        </TabsList>

        <TabsContent value="active" className="space-y-3 mt-4">
          {activeAlerts.length === 0 ? (
            <p className="text-center text-muted-foreground py-8">No active alerts</p>
          ) : (
            activeAlerts.map((alert) => <AlertCard key={alert.id} alert={alert} />)
          )}
        </TabsContent>

        <TabsContent value="acknowledged" className="space-y-3 mt-4">
          {acknowledgedAlerts.length === 0 ? (
            <p className="text-center text-muted-foreground py-8">No acknowledged alerts</p>
          ) : (
            acknowledgedAlerts.map((alert) => <AlertCard key={alert.id} alert={alert} />)
          )}
        </TabsContent>

        <TabsContent value="resolved" className="space-y-3 mt-4">
          {resolvedAlerts.length === 0 ? (
            <p className="text-center text-muted-foreground py-8">No resolved alerts</p>
          ) : (
            resolvedAlerts.map((alert) => <AlertCard key={alert.id} alert={alert} />)
          )}
        </TabsContent>
      </Tabs>
    </div>
  );
}
