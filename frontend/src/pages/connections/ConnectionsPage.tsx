import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import {
  Plus,
  Pause,
  Play,
  RefreshCw,
  MoreHorizontal,
  Search,
  ArrowRight,
  Database,
  AlertTriangle,
  ChevronLeft,
  ChevronRight,
  Loader2,
  Pencil,
  Trash2,
} from "lucide-react";

interface ConnectionSource {
  source_id: string;
  source_name: string;
  status: string;
  connector_type?: string;
}

interface ConnectionDestination {
  destination_id: string;
  destination_name: string;
  status: string;
  connector_type?: string;
}

interface Connection {
  connection_id: string;
  connection_name: string;
  source_id: string;
  destination_id: string;
  sync_mode: string;
  sync_type: string;
  status: string;
  lag_seconds?: number;
  events_per_hour?: number;
  last_sync_at?: string;
  source?: ConnectionSource;
  destination?: ConnectionDestination;
}

const CONNECTOR_ICONS: Record<string, string> = {
  mysql: "🐬",
  postgresql: "🐘",
  postgres: "🐘",
  mongodb: "🍃",
  iceberg: "🧊",
  kafka: "📨",
  s3: "☁️",
};

function getConnectorIcon(type?: string) {
  return CONNECTOR_ICONS[type?.toLowerCase() ?? ""] ?? "🔗";
}

function formatLag(seconds?: number) {
  if (seconds == null) return "—";
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  return `${Math.round(seconds / 60)}m`;
}

function formatThroughput(eventsPerHour?: number) {
  if (eventsPerHour == null) return "—";
  if (eventsPerHour >= 1000) return `${(eventsPerHour / 1000).toFixed(1)}K/hr`;
  return `${eventsPerHour}/hr`;
}

function StatusBadge({ status }: { status: string }) {
  switch (status) {
    case "active":
      return <Badge className="bg-green-500/10 text-green-600 border-green-200"><span className="mr-1">●</span>Active</Badge>;
    case "paused":
      return <Badge variant="secondary"><span className="mr-1">⏸</span>Paused</Badge>;
    case "error":
    case "failed":
      return <Badge variant="destructive"><AlertTriangle className="mr-1 h-3 w-3" />Error</Badge>;
    case "assigning":
    case "starting":
      return <Badge className="bg-blue-500/10 text-blue-600 border-blue-200"><Loader2 className="mr-1 h-3 w-3 animate-spin inline" />Starting</Badge>;
    case "draft":
      return <Badge variant="outline">Draft</Badge>;
    case "inactive":
      return <Badge variant="outline" className="text-muted-foreground">Inactive</Badge>;
    default:
      return <Badge variant="outline">{status}</Badge>;
  }
}

const PAGE_SIZE = 10;

export function ConnectionsPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [modeFilter, setModeFilter] = useState("all");
  const [sourceFilter, setSourceFilter] = useState("all");
  const [destFilter, setDestFilter] = useState("all");
  const [page, setPage] = useState(1);
  const [actionMenuId, setActionMenuId] = useState<string | null>(null);

  const { data: connections = [], isLoading } = useQuery<Connection[]>({
    queryKey: ["connections"],
    queryFn: () => fetchList("/connections", "connections"),
    refetchInterval: 10000,
  });

  const { data: sources = [] } = useQuery({
    queryKey: ["sources"],
    queryFn: () => fetchList("/sources", "sources"),
  });

  const { data: destinations = [] } = useQuery({
    queryKey: ["destinations"],
    queryFn: () => fetchList("/destinations", "destinations"),
  });

  const pauseMutation = useMutation({
    mutationFn: (id: string) => api.post(`/connections/${id}/pause`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["connections"] }),
  });

  const resumeMutation = useMutation({
    mutationFn: (id: string) => api.post(`/connections/${id}/resume`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["connections"] }),
  });

  const activateMutation = useMutation({
    mutationFn: (id: string) => api.post(`/connections/${id}/activate`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["connections"] }),
  });

  const syncMutation = useMutation({
    mutationFn: (id: string) => api.post(`/connections/${id}/trigger-sync`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["connections"] }),
  });

  const deleteMutation = useMutation({
    mutationFn: (id: string) => api.delete(`/connections/${id}`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["connections"] }),
  });

  const filtered = connections.filter((conn) => {
    const name = conn.connection_name ?? "";
    if (search && !name.toLowerCase().includes(search.toLowerCase())) return false;
    if (statusFilter !== "all" && conn.status !== statusFilter) return false;
    if (modeFilter !== "all" && conn.sync_mode !== modeFilter) return false;
    if (sourceFilter !== "all" && conn.source_id !== sourceFilter) return false;
    if (destFilter !== "all" && conn.destination_id !== destFilter) return false;
    return true;
  });

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const paginated = filtered.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Connections</h1>
        <Button onClick={() => navigate("/connections/new")}>
          <Plus className="mr-2 h-4 w-4" />New Connection
        </Button>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3">
        <Select value={statusFilter} onValueChange={(v) => { setStatusFilter(v); setPage(1); }}>
          <SelectTrigger className="w-[140px]"><SelectValue placeholder="Status" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Status</SelectItem>
            <SelectItem value="active">Active</SelectItem>
            <SelectItem value="paused">Paused</SelectItem>
            <SelectItem value="error">Error</SelectItem>
            <SelectItem value="draft">Draft</SelectItem>
            <SelectItem value="assigning">Starting</SelectItem>
          </SelectContent>
        </Select>

        <Select value={modeFilter} onValueChange={(v) => { setModeFilter(v); setPage(1); }}>
          <SelectTrigger className="w-[150px]"><SelectValue placeholder="Sync Mode" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Modes</SelectItem>
            <SelectItem value="cdc">CDC</SelectItem>
            <SelectItem value="incremental">Incremental</SelectItem>
            <SelectItem value="full_refresh">Full Refresh</SelectItem>
          </SelectContent>
        </Select>

        <Select value={sourceFilter} onValueChange={(v) => { setSourceFilter(v); setPage(1); }}>
          <SelectTrigger className="w-[160px]"><SelectValue placeholder="Source" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Sources</SelectItem>
            {sources.map((s: any) => (
              <SelectItem key={s.source_id} value={s.source_id}>{s.source_name}</SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Select value={destFilter} onValueChange={(v) => { setDestFilter(v); setPage(1); }}>
          <SelectTrigger className="w-[160px]"><SelectValue placeholder="Destination" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Destinations</SelectItem>
            {destinations.map((d: any) => (
              <SelectItem key={d.destination_id} value={d.destination_id}>{d.destination_name}</SelectItem>
            ))}
          </SelectContent>
        </Select>

        <div className="relative ml-auto">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            className="pl-9 w-[250px]"
            placeholder="Search connections..."
            value={search}
            onChange={(e) => { setSearch(e.target.value); setPage(1); }}
          />
        </div>
      </div>

      {/* Connection Cards */}
      {isLoading ? (
        <div className="flex items-center justify-center h-64 text-muted-foreground">
          <Loader2 className="mr-2 h-5 w-5 animate-spin" />Loading connections...
        </div>
      ) : paginated.length === 0 ? (
        <Card>
          <CardContent className="flex flex-col items-center justify-center py-16 text-muted-foreground">
            <Database className="h-12 w-12 mb-4 opacity-40" />
            <p className="text-lg font-medium">No connections found</p>
            <p className="text-sm">Create a new connection to get started.</p>
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-3">
          {paginated.map((conn) => (
            <Card
              key={conn.connection_id}
              className="cursor-pointer hover:border-primary/40 transition-colors"
              onClick={() => navigate(`/connections/${conn.connection_id}`)}
            >
              <CardContent className="p-4">
                <div className="flex items-start justify-between">
                  <div className="space-y-2">
                    <h3 className="font-semibold text-base">{conn.connection_name}</h3>
                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                      <span>{getConnectorIcon(conn.source?.connector_type)} {conn.source?.source_name ?? conn.source_id}</span>
                      <ArrowRight className="h-3 w-3" />
                      <span>{getConnectorIcon(conn.destination?.connector_type)} {conn.destination?.destination_name ?? conn.destination_id}</span>
                    </div>
                    <div className="flex items-center gap-4 text-sm">
                      <span>Mode: <Badge variant="outline" className="ml-1">{(conn.sync_mode ?? conn.sync_type)?.toUpperCase()}</Badge></span>
                      <span className="flex items-center gap-1.5">
                        Status: <StatusBadge status={conn.status} />
                      </span>
                      <span>Lag: <strong>{formatLag(conn.lag_seconds)}</strong></span>
                      <span>Events: <strong>{formatThroughput(conn.events_per_hour)}</strong></span>
                    </div>
                  </div>

                  <div className="relative flex items-center gap-1" onClick={(e) => e.stopPropagation()}>
                    {conn.status === "active" ? (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => pauseMutation.mutate(conn.connection_id)}
                        disabled={pauseMutation.isPending}
                      >
                        <Pause className="h-4 w-4 mr-1" />Pause
                      </Button>
                    ) : conn.status === "paused" ? (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => resumeMutation.mutate(conn.connection_id)}
                        disabled={resumeMutation.isPending}
                      >
                        <Play className="h-4 w-4 mr-1" />Resume
                      </Button>
                    ) : conn.status === "draft" || conn.status === "inactive" ? (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => activateMutation.mutate(conn.connection_id)}
                        disabled={activateMutation.isPending}
                      >
                        <Play className="h-4 w-4 mr-1" />Activate
                      </Button>
                    ) : null}
                    {(conn.status === "active" || conn.status === "paused") && (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => syncMutation.mutate(conn.connection_id)}
                        disabled={syncMutation.isPending}
                      >
                        <RefreshCw className="h-4 w-4 mr-1" />Sync
                      </Button>
                    )}
                    <Button variant="ghost" size="icon" onClick={(e) => { e.stopPropagation(); setActionMenuId(actionMenuId === conn.connection_id ? null : conn.connection_id); }}>
                      <MoreHorizontal className="h-4 w-4" />
                    </Button>
                    {actionMenuId === conn.connection_id && (
                      <div className="absolute right-0 top-8 z-50 w-48 rounded-md border bg-popover p-1 shadow-md" onMouseLeave={() => setActionMenuId(null)}>
                        <button className="flex w-full items-center gap-2 rounded-sm px-3 py-2 text-sm hover:bg-accent" onClick={(e) => { e.stopPropagation(); navigate(`/connections/${conn.connection_id}/edit`); setActionMenuId(null); }}>
                          <Pencil className="h-4 w-4" /> Edit
                        </button>
                        <button className="flex w-full items-center gap-2 rounded-sm px-3 py-2 text-sm text-destructive hover:bg-accent" onClick={(e) => { e.stopPropagation(); if (confirm(`Delete "${conn.connection_name}"?`)) deleteMutation.mutate(conn.connection_id); setActionMenuId(null); }}>
                          <Trash2 className="h-4 w-4" /> Delete
                        </button>
                      </div>
                    )}
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* Pagination */}
      {filtered.length > PAGE_SIZE && (
        <div className="flex items-center justify-between text-sm text-muted-foreground">
          <span>Showing {(page - 1) * PAGE_SIZE + 1}–{Math.min(page * PAGE_SIZE, filtered.length)} of {filtered.length}</span>
          <div className="flex items-center gap-1">
            <Button variant="ghost" size="icon" disabled={page === 1} onClick={() => setPage(page - 1)}>
              <ChevronLeft className="h-4 w-4" />
            </Button>
            {Array.from({ length: totalPages }, (_, i) => i + 1).slice(
              Math.max(0, page - 3),
              Math.min(totalPages, page + 2)
            ).map((p) => (
              <Button
                key={p}
                variant={p === page ? "default" : "ghost"}
                size="sm"
                className="w-8 h-8"
                onClick={() => setPage(p)}
              >
                {p}
              </Button>
            ))}
            <Button variant="ghost" size="icon" disabled={page === totalPages} onClick={() => setPage(page + 1)}>
              <ChevronRight className="h-4 w-4" />
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}
