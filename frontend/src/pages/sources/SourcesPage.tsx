import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { Input } from "@/components/ui/input";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Plus, TestTube, Search, MoreHorizontal, Pencil, Trash2, Radar } from "lucide-react";
import { useNavigate } from "react-router-dom";

export function SourcesPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [typeFilter, setTypeFilter] = useState("all");
  const [actionMenuId, setActionMenuId] = useState<string | null>(null);

  const { data: sources, isLoading } = useQuery({
    queryKey: ["sources"],
    queryFn: () => fetchList("/sources", "sources"),
  });

  const testMutation = useMutation({
    mutationFn: (id: string) => api.post(`/sources/${id}/test-connection`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["sources"] }),
  });

  const deleteMutation = useMutation({
    mutationFn: (id: string) => api.delete(`/sources/${id}`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["sources"] }),
  });

  const discoverMutation = useMutation({
    mutationFn: (id: string) => api.post(`/sources/${id}/discover-schemas`),
  });

  const filtered = sources?.filter((s: any) => {
    if (search && !s.source_name.toLowerCase().includes(search.toLowerCase())) return false;
    if (statusFilter !== "all" && s.status !== statusFilter) return false;
    if (typeFilter !== "all" && s.connector_definition_type !== typeFilter) return false;
    return true;
  }) ?? [];

  const connectorIcon = (type: string) => {
    switch (type) {
      case "mysql": return "🐬";
      case "mongodb": return "🍃";
      case "postgresql": return "🐘";
      default: return "🔄";
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Sources</h1>
        <Button onClick={() => navigate("/sources/new")}>
          <Plus className="mr-2 h-4 w-4" />New Source
        </Button>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3">
        <Select value={statusFilter} onValueChange={setStatusFilter}>
          <SelectTrigger className="w-32"><SelectValue placeholder="Status" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Status</SelectItem>
            <SelectItem value="active">Active</SelectItem>
            <SelectItem value="draft">Draft</SelectItem>
            <SelectItem value="inactive">Inactive</SelectItem>
          </SelectContent>
        </Select>
        <Select value={typeFilter} onValueChange={setTypeFilter}>
          <SelectTrigger className="w-40"><SelectValue placeholder="Connector Type" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Types</SelectItem>
            <SelectItem value="mysql">MySQL</SelectItem>
            <SelectItem value="mongodb">MongoDB</SelectItem>
            <SelectItem value="postgresql">PostgreSQL</SelectItem>
            <SelectItem value="polling">Polling</SelectItem>
          </SelectContent>
        </Select>
        <div className="relative flex-1 max-w-sm">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input placeholder="Search sources..." value={search} onChange={(e) => setSearch(e.target.value)} className="pl-9" />
        </div>
      </div>

      {/* Table */}
      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-10"></TableHead>
                <TableHead>Name</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Last Test</TableHead>
                <TableHead className="w-24">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow><TableCell colSpan={6} className="text-center py-8">Loading sources...</TableCell></TableRow>
              ) : filtered.length === 0 ? (
                <TableRow><TableCell colSpan={6} className="text-center py-8 text-muted-foreground">No sources found</TableCell></TableRow>
              ) : (
                filtered.map((source: any) => (
                  <TableRow key={source.source_id} className="cursor-pointer hover:bg-muted/50" onClick={() => navigate(`/sources/${source.source_id}`)}>
                    <TableCell className="text-lg">{connectorIcon(source.connector_definition_type)}</TableCell>
                    <TableCell className="font-medium">{source.source_name}</TableCell>
                    <TableCell className="text-muted-foreground">{source.connector_definition_type}</TableCell>
                    <TableCell>
                      <Badge variant={source.status === "active" ? "success" : source.status === "error" ? "destructive" : "secondary"}>
                        {source.status === "active" ? "● Active" : source.status === "error" ? "⚠ Error" : source.status}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-sm text-muted-foreground">
                      {source.connection_test_at ? (source.connection_test_status === "success" ? "✓ " : "✗ ") + new Date(source.connection_test_at).toLocaleString() : "—"}
                    </TableCell>
                    <TableCell>
                      <div className="relative">
                        <Button variant="ghost" size="sm" onClick={(e) => { e.stopPropagation(); setActionMenuId(actionMenuId === source.source_id ? null : source.source_id); }}>
                          <MoreHorizontal className="h-4 w-4" />
                        </Button>
                        {actionMenuId === source.source_id && (
                          <div className="absolute right-0 top-8 z-50 w-48 rounded-md border bg-popover p-1 shadow-md" onMouseLeave={() => setActionMenuId(null)}>
                            <button className="flex w-full items-center gap-2 rounded-sm px-3 py-2 text-sm hover:bg-accent" onClick={(e) => { e.stopPropagation(); testMutation.mutate(source.source_id); setActionMenuId(null); }}>
                              <TestTube className="h-4 w-4" /> Test Connection
                            </button>
                            <button className="flex w-full items-center gap-2 rounded-sm px-3 py-2 text-sm hover:bg-accent" onClick={(e) => { e.stopPropagation(); discoverMutation.mutate(source.source_id); setActionMenuId(null); }}>
                              <Radar className="h-4 w-4" /> Discover Schemas
                            </button>
                            <button className="flex w-full items-center gap-2 rounded-sm px-3 py-2 text-sm hover:bg-accent" onClick={(e) => { e.stopPropagation(); navigate(`/sources/${source.source_id}/edit`); }}>
                              <Pencil className="h-4 w-4" /> Edit
                            </button>
                            <button className="flex w-full items-center gap-2 rounded-sm px-3 py-2 text-sm text-destructive hover:bg-accent" onClick={(e) => { e.stopPropagation(); if (confirm("Delete this source?")) deleteMutation.mutate(source.source_id); setActionMenuId(null); }}>
                              <Trash2 className="h-4 w-4" /> Delete
                            </button>
                          </div>
                        )}
                      </div>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* Pagination */}
      {filtered.length > 0 && (
        <div className="flex items-center justify-between text-sm text-muted-foreground">
          <span>Showing 1-{filtered.length} of {filtered.length}</span>
        </div>
      )}
    </div>
  );
}
