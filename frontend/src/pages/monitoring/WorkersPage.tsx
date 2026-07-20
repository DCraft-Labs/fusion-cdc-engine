import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from "@/components/ui/table";
import { RotateCw, FileText } from "lucide-react";

interface Worker {
  id: string;
  type: string;
  status: string;
  cpu_percent: number;
  memory_percent: number;
  active_connections: number;
  last_heartbeat: string;
}

export function WorkersPage() {
  const queryClient = useQueryClient();

  const { data: workers, isLoading } = useQuery<Worker[]>({
    queryKey: ["monitoring", "workers"],
    queryFn: () => fetchList("/monitoring/workers", "workers"),
    refetchInterval: 10000,
  });

  const restartMutation = useMutation({
    mutationFn: (workerId: string) => api.post(`/monitoring/workers/${workerId}/restart`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["monitoring", "workers"] }),
  });

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Workers</h1>

      <Card>
        <CardHeader>
          <CardTitle>Worker Status</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Worker ID</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>CPU</TableHead>
                <TableHead>Memory</TableHead>
                <TableHead>Active Connections</TableHead>
                <TableHead>Last Heartbeat</TableHead>
                <TableHead>Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow>
                  <TableCell colSpan={8} className="text-center">Loading...</TableCell>
                </TableRow>
              ) : !workers || workers.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={8} className="text-center text-muted-foreground">
                    No workers registered
                  </TableCell>
                </TableRow>
              ) : (
                workers.map((w) => (
                  <TableRow key={w.id}>
                    <TableCell className="font-mono text-sm">{w.id}</TableCell>
                    <TableCell>
                      <Badge variant="outline">{w.type}</Badge>
                    </TableCell>
                    <TableCell>
                      <Badge
                        variant={
                          w.status === "active"
                            ? "success"
                            : w.status === "unhealthy"
                            ? "destructive"
                            : "secondary"
                        }
                      >
                        {w.status}
                      </Badge>
                    </TableCell>
                    <TableCell>{w.cpu_percent}%</TableCell>
                    <TableCell>{w.memory_percent}%</TableCell>
                    <TableCell>{w.active_connections}</TableCell>
                    <TableCell className="text-muted-foreground">
                      {w.last_heartbeat
                        ? new Date(w.last_heartbeat).toLocaleString()
                        : "—"}
                    </TableCell>
                    <TableCell>
                      <div className="flex gap-1">
                        <Button
                          size="sm"
                          variant="ghost"
                          onClick={() => restartMutation.mutate(w.id)}
                          disabled={restartMutation.isPending}
                        >
                          <RotateCw className="h-4 w-4 mr-1" />
                          Restart
                        </Button>
                        <Button
                          size="sm"
                          variant="ghost"
                          onClick={() =>
                            window.open(`/monitoring/workers/${w.id}/logs`, "_blank")
                          }
                        >
                          <FileText className="h-4 w-4 mr-1" />
                          View Logs
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
