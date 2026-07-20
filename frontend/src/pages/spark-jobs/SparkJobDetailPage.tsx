import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Table, TableHeader, TableBody, TableRow, TableHead, TableCell,
} from "@/components/ui/table";
import { ArrowLeft, XCircle, FileText } from "lucide-react";

interface Executor {
  id: string;
  status: string;
  cpu_percent: number;
  memory: string;
  tasks_completed: number;
  tasks_total: number;
}

interface SparkJobDetail {
  id: string;
  connection_name: string;
  type: string;
  status: string;
  submitted_at: string;
  started_at: string | null;
  duration: string | null;
  app_id: string | null;
  master: string | null;
  driver_memory: string | null;
  executor_memory: string | null;
  executors: Executor[];
  records_processed: number;
  records_total: number | null;
  logs: string[];
}

const statusBadgeVariant = (status: string) => {
  switch (status) {
    case "running": return "default";
    case "queued": return "secondary";
    case "completed": return "outline";
    case "failed": return "destructive";
    default: return "secondary";
  }
};

export function SparkJobDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: job, isLoading } = useQuery<SparkJobDetail>({
    queryKey: ["spark-jobs", id],
    queryFn: () => api.get(`/spark-jobs/${id}`).then((r) => r.data),
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      return status === "running" ? 5000 : false;
    },
  });

  const cancelMutation = useMutation({
    mutationFn: () => api.post(`/spark-jobs/${id}/cancel`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["spark-jobs", id] }),
  });

  if (isLoading) return <div className="flex items-center justify-center h-64">Loading...</div>;
  if (!job) return <div className="text-center text-muted-foreground">Job not found</div>;

  const progressPercent = job.records_total
    ? Math.round((job.records_processed / job.records_total) * 100)
    : null;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Button variant="ghost" size="icon" onClick={() => navigate("/spark-jobs")}>
            <ArrowLeft className="h-4 w-4" />
          </Button>
          <h1 className="text-2xl font-bold">Spark Job: {id?.slice(0, 12)}</h1>
          <Badge variant={statusBadgeVariant(job.status)} className="capitalize">
            {job.status}
          </Badge>
        </div>
        <div className="flex gap-2">
          {(job.status === "running" || job.status === "queued") && (
            <Button
              variant="destructive"
              size="sm"
              onClick={() => cancelMutation.mutate()}
              disabled={cancelMutation.isPending}
            >
              <XCircle className="mr-2 h-4 w-4" />
              Cancel Job
            </Button>
          )}
          <Button variant="outline" size="sm" onClick={() => navigate(`/spark-jobs/${id}/logs`)}>
            <FileText className="mr-2 h-4 w-4" />
            View Full Logs
          </Button>
          <Button variant="ghost" size="sm" onClick={() => navigate("/spark-jobs")}>
            <ArrowLeft className="mr-2 h-4 w-4" />
            Back
          </Button>
        </div>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Job Information</CardTitle>
        </CardHeader>
        <CardContent>
          <dl className="grid grid-cols-2 md:grid-cols-3 gap-4 text-sm">
            <div>
              <dt className="text-muted-foreground">Connection</dt>
              <dd className="font-medium">{job.connection_name}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Type</dt>
              <dd className="capitalize">{job.type}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Submitted</dt>
              <dd>{new Date(job.submitted_at).toLocaleString()}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Started</dt>
              <dd>{job.started_at ? new Date(job.started_at).toLocaleString() : "—"}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Duration</dt>
              <dd>
                {job.duration ?? (job.status === "running" ? "Running..." : "—")}
              </dd>
            </div>
          </dl>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Application Info</CardTitle>
        </CardHeader>
        <CardContent>
          <dl className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
            <div>
              <dt className="text-muted-foreground">App ID</dt>
              <dd className="font-mono text-xs">{job.app_id ?? "—"}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Master</dt>
              <dd>{job.master ?? "—"}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Driver Memory</dt>
              <dd>{job.driver_memory ?? "—"}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Executor Memory</dt>
              <dd>{job.executor_memory ?? "—"}</dd>
            </div>
          </dl>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Executors</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Executor ID</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>CPU%</TableHead>
                <TableHead>Memory</TableHead>
                <TableHead>Tasks Completed</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {!job.executors?.length ? (
                <TableRow>
                  <TableCell colSpan={5} className="text-center text-muted-foreground">
                    No executor data available
                  </TableCell>
                </TableRow>
              ) : (
                job.executors.map((exec) => (
                  <TableRow key={exec.id}>
                    <TableCell className="font-mono text-xs">{exec.id}</TableCell>
                    <TableCell>
                      <span className="flex items-center gap-2">
                        <span
                          className={`h-2 w-2 rounded-full ${
                            exec.status === "active" ? "bg-green-500" : "bg-gray-400"
                          }`}
                        />
                        {exec.status}
                      </span>
                    </TableCell>
                    <TableCell>{exec.cpu_percent}%</TableCell>
                    <TableCell>{exec.memory}</TableCell>
                    <TableCell>{exec.tasks_completed}/{exec.tasks_total}</TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Progress</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            <div className="flex justify-between text-sm">
              <span>Records Processed</span>
              <span className="font-mono">
                {job.records_processed.toLocaleString()}
                {job.records_total ? ` / ~${job.records_total.toLocaleString()}` : ""}
                {progressPercent !== null && ` (${progressPercent}%)`}
              </span>
            </div>
            <div className="h-3 w-full rounded-full bg-muted overflow-hidden">
              <div
                className="h-full rounded-full bg-primary transition-all"
                style={{ width: `${progressPercent ?? 0}%` }}
              />
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Logs (last 20 lines)</CardTitle>
        </CardHeader>
        <CardContent>
          <pre className="rounded-md bg-muted p-4 text-xs overflow-auto max-h-80 font-mono leading-relaxed">
            {job.logs?.length
              ? job.logs.slice(-20).join("\n")
              : "No logs available"}
          </pre>
        </CardContent>
      </Card>
    </div>
  );
}
