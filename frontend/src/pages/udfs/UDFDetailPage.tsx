import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";
import { Trash2 } from "lucide-react";

export function UDFDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: udf, isLoading } = useQuery({
    queryKey: ["udfs", id],
    queryFn: () => api.get(`/udfs/${id}`).then((r) => r.data),
  });

  const { data: execStats } = useQuery({
    queryKey: ["udfs", id, "stats"],
    queryFn: () => api.get(`/udfs/${id}/stats`).then((r) => r.data).catch(() => null),
  });

  const saveMutation = useMutation({
    mutationFn: () => api.patch(`/udfs/${id}`, udf),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["udfs", id] }),
  });

  const validateMutation = useMutation({
    mutationFn: () => api.post(`/udfs/${id}/validate`, { code: udf?.code }),
  });

  const deleteMutation = useMutation({
    mutationFn: () => api.delete(`/udfs/${id}`),
    onSuccess: () => navigate("/udfs"),
  });

  if (isLoading) return <div className="flex items-center justify-center h-64">Loading...</div>;
  if (!udf) return <div className="text-center text-muted-foreground">UDF not found</div>;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-2xl font-bold">{udf.name}</h1>
          <Badge variant="outline">{udf.language ?? "python"}</Badge>
        </div>
        <div className="flex gap-2">
          <Button onClick={() => saveMutation.mutate()} disabled={saveMutation.isPending}>
            {saveMutation.isPending ? "Saving..." : "Save"}
          </Button>
          <Button variant="outline" onClick={() => validateMutation.mutate()} disabled={validateMutation.isPending}>
            Validate
          </Button>
          <Button variant="destructive" size="icon" onClick={() => deleteMutation.mutate()}>
            <Trash2 className="h-4 w-4" />
          </Button>
        </div>
      </div>

      <Card>
        <CardContent className="pt-4">
          <div className="grid grid-cols-3 gap-4 text-sm">
            <div>
              <span className="text-muted-foreground">Description</span>
              <p className="font-medium">{udf.description || "—"}</p>
            </div>
            <div>
              <span className="text-muted-foreground">Category</span>
              <p className="font-medium capitalize">{udf.category ?? "—"}</p>
            </div>
            <div>
              <span className="text-muted-foreground">Return Type</span>
              <p className="font-medium">{udf.return_type ?? "—"}</p>
            </div>
          </div>
        </CardContent>
      </Card>

      {validateMutation.data && (
        <div className={`rounded-md p-3 text-sm ${validateMutation.data.data?.valid !== false ? "bg-green-500/10 text-green-600" : "bg-destructive/10 text-destructive"}`}>
          {validateMutation.data.data?.valid !== false ? "✓ Validation passed" : `✗ ${validateMutation.data.data?.error ?? "Invalid"}`}
        </div>
      )}

      <Tabs defaultValue="code">
        <TabsList>
          <TabsTrigger value="code">Code</TabsTrigger>
          <TabsTrigger value="params">Parameters</TabsTrigger>
          <TabsTrigger value="stats">Execution Stats</TabsTrigger>
        </TabsList>

        <TabsContent value="code">
          <Card>
            <CardContent className="pt-6">
              <pre className="rounded-md bg-muted p-4 font-mono text-sm overflow-auto max-h-[400px] whitespace-pre-wrap">
                <code>{udf.code || "// No code defined"}</code>
              </pre>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="params">
          <Card>
            <CardHeader><CardTitle>Parameters</CardTitle></CardHeader>
            <CardContent className="p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Name</TableHead>
                    <TableHead>Type</TableHead>
                    <TableHead>Description</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {(udf.parameters ?? []).length === 0 ? (
                    <TableRow><TableCell colSpan={3} className="text-center text-muted-foreground">No parameters defined</TableCell></TableRow>
                  ) : (
                    (udf.parameters ?? []).map((p: any, i: number) => (
                      <TableRow key={i}>
                        <TableCell className="font-mono">{p.name}</TableCell>
                        <TableCell><Badge variant="outline">{p.type}</Badge></TableCell>
                        <TableCell className="text-muted-foreground">{p.description ?? "—"}</TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="stats" className="space-y-4">
          <div className="grid gap-4 md:grid-cols-4">
            {[
              { label: "Total Calls", value: execStats?.total_calls ?? udf.stats?.total_calls ?? "—" },
              { label: "Avg Exec Time", value: (execStats?.avg_exec_ms ?? udf.stats?.avg_exec_ms) ? `${execStats?.avg_exec_ms ?? udf.stats?.avg_exec_ms}ms` : "—" },
              { label: "Errors (24h)", value: execStats?.errors_24h ?? udf.stats?.errors_24h ?? 0 },
              { label: "Used By", value: `${execStats?.connection_count ?? udf.stats?.connection_count ?? 0} connections` },
            ].map((s) => (
              <Card key={s.label}>
                <CardContent className="pt-6">
                  <p className="text-sm text-muted-foreground">{s.label}</p>
                  <p className="text-2xl font-bold">{s.value}</p>
                </CardContent>
              </Card>
            ))}
          </div>

          {(execStats?.time_series ?? []).length > 0 && (
            <Card>
              <CardHeader><CardTitle className="text-sm">Execution Time Trend</CardTitle></CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={250}>
                  <LineChart data={execStats.time_series}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="timestamp" tick={{ fontSize: 12 }} />
                    <YAxis tick={{ fontSize: 12 }} />
                    <Tooltip />
                    <Line type="monotone" dataKey="avg_ms" stroke="hsl(var(--primary))" strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>
          )}

          {(execStats?.connections ?? []).length > 0 && (
            <Card>
              <CardHeader><CardTitle className="text-sm">Used in Connections</CardTitle></CardHeader>
              <CardContent className="p-0">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Connection</TableHead>
                      <TableHead>Calls</TableHead>
                      <TableHead>Last Used</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {execStats.connections.map((conn: any, i: number) => (
                      <TableRow key={i}>
                        <TableCell className="font-medium">{conn.name}</TableCell>
                        <TableCell>{conn.call_count?.toLocaleString()}</TableCell>
                        <TableCell className="text-muted-foreground">
                          {conn.last_used ? new Date(conn.last_used).toLocaleString() : "—"}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          )}
        </TabsContent>
      </Tabs>
    </div>
  );
}
