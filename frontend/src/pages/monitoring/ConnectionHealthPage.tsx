import { useState } from "react";
import { useParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Clock, Activity, Database } from "lucide-react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";

type TimeRange = "1h" | "6h" | "24h" | "7d";

interface LagDataPoint {
  timestamp: string;
  lag_seconds: number;
}

interface Throughput {
  events_per_sec: number;
  bytes_per_sec: number;
}

interface CheckpointState {
  worker_name: string;
  binlog_file: string;
  binlog_position: number;
  gtid: string;
  last_checkpoint_time: string;
}

export function ConnectionHealthPage() {
  const { id } = useParams<{ id: string }>();
  const [timeRange, setTimeRange] = useState<TimeRange>("1h");

  const { data: health, isLoading } = useQuery({
    queryKey: ["monitoring", "connections", id, "health"],
    queryFn: () => api.get(`/monitoring/connections/${id}/health`).then((r) => r.data),
  });

  const { data: lagData } = useQuery<LagDataPoint[]>({
    queryKey: ["monitoring", "connections", id, "lag", timeRange],
    queryFn: () =>
      api
        .get(`/monitoring/connections/${id}/lag`, { params: { range: timeRange } })
        .then((r) => r.data),
  });

  const { data: throughput } = useQuery<Throughput>({
    queryKey: ["monitoring", "connections", id, "throughput"],
    queryFn: () =>
      api.get(`/monitoring/connections/${id}/throughput`).then((r) => r.data),
  });

  const { data: checkpoint } = useQuery<CheckpointState>({
    queryKey: ["monitoring", "connections", id, "checkpoints"],
    queryFn: () =>
      api.get(`/monitoring/connections/${id}/checkpoints`).then((r) => r.data),
  });

  if (isLoading) {
    return <div className="flex items-center justify-center h-64">Loading...</div>;
  }

  const ranges: TimeRange[] = ["1h", "6h", "24h", "7d"];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">{health?.connection_name ?? id}</h1>
        <Badge
          variant={
            health?.status === "healthy"
              ? "success"
              : health?.status === "degraded"
              ? "warning"
              : "destructive"
          }
        >
          {health?.status ?? "unknown"}
        </Badge>
      </div>

      {/* Time Range Selector */}
      <div className="flex gap-2">
        {ranges.map((r) => (
          <Button
            key={r}
            size="sm"
            variant={timeRange === r ? "default" : "outline"}
            onClick={() => setTimeRange(r)}
          >
            {r}
          </Button>
        ))}
      </div>

      {/* CDC Lag Chart */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Clock className="h-4 w-4" /> CDC Lag
          </CardTitle>
        </CardHeader>
        <CardContent>
          {lagData && lagData.length > 0 ? (
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={lagData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="timestamp"
                  tickFormatter={(v) => new Date(v).toLocaleTimeString()}
                  fontSize={12}
                />
                <YAxis
                  label={{ value: "Lag (s)", angle: -90, position: "insideLeft" }}
                  fontSize={12}
                />
                <Tooltip
                  labelFormatter={(v) => new Date(v as string).toLocaleString()}
                  formatter={(value) => [`${value}s`, "Lag"]}
                />
                <Line
                  type="monotone"
                  dataKey="lag_seconds"
                  stroke="#3b82f6"
                  strokeWidth={2}
                  dot={false}
                />
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <p className="text-muted-foreground text-center py-8">No lag data available</p>
          )}
        </CardContent>
      </Card>

      {/* Throughput */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Activity className="h-4 w-4" /> Throughput
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <div className="flex items-center justify-between text-sm">
              <span className="font-medium">Events/sec</span>
              <span className="text-muted-foreground">
                {throughput?.events_per_sec ?? 0}
              </span>
            </div>
            <div className="h-3 w-full rounded-full bg-muted overflow-hidden">
              <div
                className="h-full bg-blue-500 transition-all"
                style={{
                  width: `${Math.min((throughput?.events_per_sec ?? 0) / 100, 1) * 100}%`,
                }}
              />
            </div>
          </div>
          <div className="space-y-2">
            <div className="flex items-center justify-between text-sm">
              <span className="font-medium">Bytes/sec</span>
              <span className="text-muted-foreground">
                {throughput?.bytes_per_sec != null
                  ? `${(throughput.bytes_per_sec / 1024).toFixed(1)} KB/s`
                  : "0"}
              </span>
            </div>
            <div className="h-3 w-full rounded-full bg-muted overflow-hidden">
              <div
                className="h-full bg-purple-500 transition-all"
                style={{
                  width: `${Math.min(
                    (throughput?.bytes_per_sec ?? 0) / (1024 * 1024),
                    1
                  ) * 100}%`,
                }}
              />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Checkpoint State */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Database className="h-4 w-4" /> Checkpoint State
          </CardTitle>
        </CardHeader>
        <CardContent>
          {checkpoint ? (
            <dl className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <dt className="text-sm text-muted-foreground">Worker</dt>
                <dd className="font-medium">{checkpoint.worker_name}</dd>
              </div>
              <div>
                <dt className="text-sm text-muted-foreground">Binlog File</dt>
                <dd className="font-mono text-sm">{checkpoint.binlog_file}</dd>
              </div>
              <div>
                <dt className="text-sm text-muted-foreground">Binlog Position</dt>
                <dd className="font-mono text-sm">{checkpoint.binlog_position}</dd>
              </div>
              <div>
                <dt className="text-sm text-muted-foreground">GTID</dt>
                <dd className="font-mono text-sm break-all">
                  {checkpoint.gtid || "—"}
                </dd>
              </div>
              <div className="md:col-span-2">
                <dt className="text-sm text-muted-foreground">Last Checkpoint</dt>
                <dd className="font-medium">
                  {checkpoint.last_checkpoint_time
                    ? new Date(checkpoint.last_checkpoint_time).toLocaleString()
                    : "—"}
                </dd>
              </div>
            </dl>
          ) : (
            <p className="text-muted-foreground">No checkpoint data available</p>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
