import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Download, ChevronLeft, ChevronRight } from "lucide-react";

export function AuditLogsPage() {
  const [filter, setFilter] = useState({
    user: "_all_",
    action: "_all_",
    resource: "_all_",
    date_from: "",
    date_to: "",
  });
  const [page, setPage] = useState(1);
  const pageSize = 25;

  const { data, isLoading } = useQuery({
    queryKey: ["settings", "audit-logs", filter, page],
    queryFn: () =>
      api.get("/settings/audit-logs", {
        params: {
          user: filter.user !== "_all_" ? filter.user : undefined,
          action: filter.action !== "_all_" ? filter.action : undefined,
          resource: filter.resource !== "_all_" ? filter.resource : undefined,
          date_from: filter.date_from || undefined,
          date_to: filter.date_to || undefined,
          page,
          page_size: pageSize,
        },
      }).then((r) => r.data),
  });

  const logs = Array.isArray(data) ? data : data?.items ?? [];
  const totalPages = (data?.total_pages ?? Math.ceil((data?.total ?? logs.length) / pageSize)) || 1;

  const exportCsv = () => {
    const headers = ["Timestamp", "User", "Action", "Resource", "Status"];
    const rows = logs.map((l: any) => [
      new Date(l.timestamp).toISOString(),
      l.user_email ?? l.user ?? "",
      l.action ?? "",
      `${l.resource_type ?? ""}/${l.resource_name ?? l.resource_id ?? ""}`,
      l.status ?? "OK",
    ]);
    const csv = [headers, ...rows].map((r) => r.join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `audit-logs-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Audit Logs</h1>
          <p className="text-muted-foreground">Track all system activity and user actions</p>
        </div>
        <Button variant="outline" onClick={exportCsv}>
          <Download className="mr-2 h-4 w-4" />Export CSV
        </Button>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap gap-3">
        <Select value={filter.user} onValueChange={(v) => { setFilter({ ...filter, user: v }); setPage(1); }}>
          <SelectTrigger className="w-44"><SelectValue placeholder="All Users" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="_all_">All Users</SelectItem>
          </SelectContent>
        </Select>

        <Select value={filter.action} onValueChange={(v) => { setFilter({ ...filter, action: v }); setPage(1); }}>
          <SelectTrigger className="w-44"><SelectValue placeholder="All Actions" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="_all_">All Actions</SelectItem>
            <SelectItem value="CREATE">CREATE</SelectItem>
            <SelectItem value="UPDATE">UPDATE</SelectItem>
            <SelectItem value="DELETE">DELETE</SelectItem>
            <SelectItem value="PAUSE">PAUSE</SelectItem>
            <SelectItem value="RESUME">RESUME</SelectItem>
          </SelectContent>
        </Select>

        <Select value={filter.resource} onValueChange={(v) => { setFilter({ ...filter, resource: v }); setPage(1); }}>
          <SelectTrigger className="w-44"><SelectValue placeholder="All Resources" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="_all_">All Resources</SelectItem>
            <SelectItem value="connection">Connection</SelectItem>
            <SelectItem value="source">Source</SelectItem>
            <SelectItem value="destination">Destination</SelectItem>
            <SelectItem value="transform">Transform</SelectItem>
            <SelectItem value="user">User</SelectItem>
          </SelectContent>
        </Select>

        <Input
          type="date"
          value={filter.date_from}
          onChange={(e) => { setFilter({ ...filter, date_from: e.target.value }); setPage(1); }}
          className="w-40"
          placeholder="From"
        />
        <Input
          type="date"
          value={filter.date_to}
          onChange={(e) => { setFilter({ ...filter, date_to: e.target.value }); setPage(1); }}
          className="w-40"
          placeholder="To"
        />
      </div>

      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Timestamp</TableHead>
                <TableHead>User</TableHead>
                <TableHead>Action</TableHead>
                <TableHead>Resource</TableHead>
                <TableHead>Status</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow><TableCell colSpan={5} className="text-center py-8">Loading audit logs...</TableCell></TableRow>
              ) : logs.length === 0 ? (
                <TableRow><TableCell colSpan={5} className="text-center py-8 text-muted-foreground">No audit logs found</TableCell></TableRow>
              ) : (
                logs.map((l: any, i: number) => (
                  <TableRow key={l.id ?? i}>
                    <TableCell className="text-muted-foreground whitespace-nowrap text-sm">
                      {new Date(l.timestamp).toLocaleString()}
                    </TableCell>
                    <TableCell className="font-medium">{l.user_email ?? l.user ?? "—"}</TableCell>
                    <TableCell>
                      <Badge variant="outline">{l.action}</Badge>
                    </TableCell>
                    <TableCell>
                      <span className="font-mono text-sm">{l.resource_type}</span>
                      {(l.resource_name || l.resource_id) && (
                        <span className="text-muted-foreground text-sm"> · {l.resource_name ?? l.resource_id}</span>
                      )}
                    </TableCell>
                    <TableCell>
                      {l.status === "error" || l.status === "ERROR" ? (
                        <span className="text-destructive font-medium">✗ Error</span>
                      ) : (
                        <span className="text-green-600 font-medium">✓ OK</span>
                      )}
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* Pagination */}
      <div className="flex items-center justify-between">
        <p className="text-sm text-muted-foreground">
          Page {page} of {totalPages}
        </p>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            disabled={page <= 1}
            onClick={() => setPage((p) => p - 1)}
          >
            <ChevronLeft className="h-4 w-4" /> Previous
          </Button>
          <Button
            variant="outline"
            size="sm"
            disabled={page >= totalPages}
            onClick={() => setPage((p) => p + 1)}
          >
            Next <ChevronRight className="h-4 w-4" />
          </Button>
        </div>
      </div>
    </div>
  );
}
