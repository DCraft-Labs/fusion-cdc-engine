import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Table, TableHeader, TableBody, TableRow, TableHead, TableCell,
} from "@/components/ui/table";
import { Inbox, RotateCcw, Trash2, AlertTriangle, CheckCircle2, Clock } from "lucide-react";

interface DLQStats {
  total: number;
  pending_retry: number;
  retried_success: number;
  expired: number;
}

interface DLQConnectionSummary {
  connection_id: string;
  connection_name: string;
  failed_count: number;
  reason: string;
  oldest_event_time: string;
}

export function DLQPage() {
  const queryClient = useQueryClient();
  const navigate = useNavigate();

  const { data: stats } = useQuery<DLQStats>({
    queryKey: ["dlq", "stats"],
    queryFn: () => api.get("/dlq/stats").then((r) => r.data).catch(() => ({ total: 0, by_connection: [] })),
  });

  const { data: connections, isLoading } = useQuery<DLQConnectionSummary[]>({
    queryKey: ["dlq", "connections"],
    queryFn: () => fetchList("/dlq/connections").catch(() => []),
  });

  const retryAllMutation = useMutation({
    mutationFn: () => api.post("/dlq/retry-all"),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["dlq"] }),
  });

  const purgeExpiredMutation = useMutation({
    mutationFn: () => api.post("/dlq/purge-expired"),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["dlq"] }),
  });

  const statCards = [
    { label: "Total Events", value: stats?.total ?? 0, icon: Inbox, color: "text-foreground" },
    { label: "Pending Retry", value: stats?.pending_retry ?? 0, icon: Clock, color: "text-yellow-600" },
    { label: "Retried Success", value: stats?.retried_success ?? 0, icon: CheckCircle2, color: "text-green-600" },
    { label: "Expired (TTL)", value: stats?.expired ?? 0, icon: AlertTriangle, color: "text-red-600" },
  ];

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Dead Letter Queue</h1>

      <div className="grid gap-4 md:grid-cols-4">
        {statCards.map((s) => (
          <Card key={s.label}>
            <CardContent className="pt-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-muted-foreground">{s.label}</p>
                  <p className="text-2xl font-bold">{s.value.toLocaleString()}</p>
                </div>
                <s.icon className={`h-8 w-8 ${s.color} opacity-70`} />
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Connection</TableHead>
                <TableHead>Failed Count</TableHead>
                <TableHead>Reason</TableHead>
                <TableHead>Oldest Event</TableHead>
                <TableHead>Action</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow>
                  <TableCell colSpan={5} className="text-center">Loading...</TableCell>
                </TableRow>
              ) : !connections?.length ? (
                <TableRow>
                  <TableCell colSpan={5} className="text-center text-muted-foreground">
                    <div className="flex flex-col items-center gap-2 py-8">
                      <Inbox className="h-8 w-8 text-muted-foreground/50" />
                      <span>No dead letter events</span>
                    </div>
                  </TableCell>
                </TableRow>
              ) : (
                connections.map((conn) => (
                  <TableRow key={conn.connection_id}>
                    <TableCell className="font-medium">{conn.connection_name}</TableCell>
                    <TableCell>
                      <Badge variant="destructive">{conn.failed_count}</Badge>
                    </TableCell>
                    <TableCell>
                      <Badge variant="outline">{conn.reason}</Badge>
                    </TableCell>
                    <TableCell className="text-muted-foreground">
                      {conn.oldest_event_time
                        ? new Date(conn.oldest_event_time).toLocaleString()
                        : "—"}
                    </TableCell>
                    <TableCell>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => navigate(`/dlq/${conn.connection_id}`)}
                      >
                        View
                      </Button>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <div className="flex gap-2">
        <Button
          onClick={() => retryAllMutation.mutate()}
          disabled={retryAllMutation.isPending}
        >
          <RotateCcw className="mr-2 h-4 w-4" />
          Retry All Pending
        </Button>
        <Button
          variant="destructive"
          onClick={() => purgeExpiredMutation.mutate()}
          disabled={purgeExpiredMutation.isPending}
        >
          <Trash2 className="mr-2 h-4 w-4" />
          Purge Expired
        </Button>
      </div>
    </div>
  );
}
