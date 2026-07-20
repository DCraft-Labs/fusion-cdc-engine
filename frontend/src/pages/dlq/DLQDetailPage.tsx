import { useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Table, TableHeader, TableBody, TableRow, TableHead, TableCell,
} from "@/components/ui/table";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { RotateCcw, Trash2, ArrowLeft } from "lucide-react";

interface DLQEvent {
  id: string;
  table: string;
  reason: string;
  status: string;
  retry_count: number;
  max_retries: number;
  failed_at: string;
}

export function DLQDetailPage() {
  const { connectionId } = useParams<{ connectionId: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [reasonFilter, setReasonFilter] = useState<string>("all");
  const [statusFilter, setStatusFilter] = useState<string>("all");
  const [dateRange, setDateRange] = useState<string>("");
  const [selected, setSelected] = useState<Set<string>>(new Set());

  const { data, isLoading } = useQuery<{ connection_name: string; events: DLQEvent[] }>({
    queryKey: ["dlq", connectionId, reasonFilter, statusFilter, dateRange],
    queryFn: () =>
      api
        .get(`/dlq/${connectionId}/events`, {
          params: {
            reason: reasonFilter !== "all" ? reasonFilter : undefined,
            status: statusFilter !== "all" ? statusFilter : undefined,
            date_range: dateRange || undefined,
          },
        })
        .then((r) => r.data),
  });

  const retryAllMutation = useMutation({
    mutationFn: () => api.post(`/dlq/${connectionId}/retry-all`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["dlq"] });
      setSelected(new Set());
    },
  });

  const purgeMutation = useMutation({
    mutationFn: () => api.delete(`/dlq/${connectionId}/purge`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["dlq"] }),
  });

  const retrySelectedMutation = useMutation({
    mutationFn: (ids: string[]) => api.post(`/dlq/${connectionId}/retry`, { event_ids: ids }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["dlq"] });
      setSelected(new Set());
    },
  });

  const deleteSelectedMutation = useMutation({
    mutationFn: (ids: string[]) => api.post(`/dlq/${connectionId}/delete`, { event_ids: ids }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["dlq"] });
      setSelected(new Set());
    },
  });

  const events = data?.events ?? [];
  const connectionName = data?.connection_name ?? connectionId;

  const toggleSelect = (id: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const toggleAll = () => {
    if (selected.size === events.length) setSelected(new Set());
    else setSelected(new Set(events.map((e) => e.id)));
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Button variant="ghost" size="icon" onClick={() => navigate("/dlq")}>
            <ArrowLeft className="h-4 w-4" />
          </Button>
          <h1 className="text-2xl font-bold">DLQ — {connectionName}</h1>
        </div>
        <div className="flex gap-2">
          <Button onClick={() => retryAllMutation.mutate()} disabled={retryAllMutation.isPending}>
            <RotateCcw className="mr-2 h-4 w-4" />
            Retry All
          </Button>
          <Button variant="destructive" onClick={() => purgeMutation.mutate()} disabled={purgeMutation.isPending}>
            <Trash2 className="mr-2 h-4 w-4" />
            Purge
          </Button>
        </div>
      </div>

      <div className="flex gap-3 items-center">
        <Select value={reasonFilter} onValueChange={setReasonFilter}>
          <SelectTrigger className="w-[160px]">
            <SelectValue placeholder="Reason" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Reasons</SelectItem>
            <SelectItem value="dq">Data Quality</SelectItem>
            <SelectItem value="write">Write Failure</SelectItem>
            <SelectItem value="transform">Transform Error</SelectItem>
          </SelectContent>
        </Select>
        <Select value={statusFilter} onValueChange={setStatusFilter}>
          <SelectTrigger className="w-[160px]">
            <SelectValue placeholder="Status" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Statuses</SelectItem>
            <SelectItem value="pending">Pending</SelectItem>
            <SelectItem value="retried">Retried</SelectItem>
            <SelectItem value="expired">Expired</SelectItem>
          </SelectContent>
        </Select>
        <Input
          type="date"
          className="w-[180px]"
          value={dateRange}
          onChange={(e) => setDateRange(e.target.value)}
          placeholder="Date range"
        />
      </div>

      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-[40px]">
                  <input
                    type="checkbox"
                    checked={events.length > 0 && selected.size === events.length}
                    onChange={toggleAll}
                    className="rounded border-muted-foreground"
                  />
                </TableHead>
                <TableHead>Event ID</TableHead>
                <TableHead>Table</TableHead>
                <TableHead>Reason</TableHead>
                <TableHead>Retries</TableHead>
                <TableHead>Failed At</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow>
                  <TableCell colSpan={6} className="text-center">Loading...</TableCell>
                </TableRow>
              ) : !events.length ? (
                <TableRow>
                  <TableCell colSpan={6} className="text-center text-muted-foreground py-8">
                    No DLQ events match filters
                  </TableCell>
                </TableRow>
              ) : (
                events.map((evt) => (
                  <TableRow
                    key={evt.id}
                    className="cursor-pointer"
                    onClick={() => navigate(`/dlq/${connectionId}/${evt.id}`)}
                  >
                    <TableCell onClick={(e) => e.stopPropagation()}>
                      <input
                        type="checkbox"
                        checked={selected.has(evt.id)}
                        onChange={() => toggleSelect(evt.id)}
                        className="rounded border-muted-foreground"
                      />
                    </TableCell>
                    <TableCell className="font-mono text-xs">{evt.id.slice(0, 12)}...</TableCell>
                    <TableCell>{evt.table}</TableCell>
                    <TableCell>
                      <Badge variant="outline">{evt.reason}</Badge>
                    </TableCell>
                    <TableCell>{evt.retry_count}/{evt.max_retries ?? 3}</TableCell>
                    <TableCell className="text-muted-foreground">
                      {new Date(evt.failed_at).toLocaleString()}
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {selected.size > 0 && (
        <div className="flex items-center gap-3">
          <span className="text-sm text-muted-foreground">{selected.size} selected</span>
          <Button
            size="sm"
            onClick={() => retrySelectedMutation.mutate([...selected])}
            disabled={retrySelectedMutation.isPending}
          >
            <RotateCcw className="mr-2 h-3 w-3" />
            Retry Selected
          </Button>
          <Button
            size="sm"
            variant="destructive"
            onClick={() => deleteSelectedMutation.mutate([...selected])}
            disabled={deleteSelectedMutation.isPending}
          >
            <Trash2 className="mr-2 h-3 w-3" />
            Delete Selected
          </Button>
        </div>
      )}
    </div>
  );
}
