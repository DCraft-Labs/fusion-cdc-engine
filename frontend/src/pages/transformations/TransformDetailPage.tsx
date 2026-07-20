import { useParams } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { useState } from "react";
import { CheckCircle, Play, Save, ArrowUpDown } from "lucide-react";

export function TransformDetailPage() {
  const { id } = useParams<{ id: string }>();
  const queryClient = useQueryClient();
  const [code, setCode] = useState("");
  const [logStatusFilter, setLogStatusFilter] = useState("all");
  const [logConnectionFilter, setLogConnectionFilter] = useState("all");

  const { data: transform, isLoading } = useQuery({
    queryKey: ["transformations", id],
    queryFn: () => api.get(`/transformations/${id}`).then((r) => r.data),
    select: (data) => {
      if (!code && data?.transformation_code) setCode(data.transformation_code);
      return data;
    },
  });

  const { data: logs } = useQuery({
    queryKey: ["transformations", id, "logs"],
    queryFn: () => api.get(`/transformations/${id}/logs`).then((r) => r.data).catch(() => []),
  });

  const { data: dependencies } = useQuery({
    queryKey: ["transformations", id, "dependencies"],
    queryFn: () => api.get(`/transformations/${id}/dependencies`).then((r) => r.data).catch(() => ({ upstream: [], downstream: [] })),
  });

  const validateMutation = useMutation({
    mutationFn: () => api.post(`/transformations/${id}/validate`, { code }),
  });

  const saveMutation = useMutation({
    mutationFn: () => api.patch(`/transformations/${id}`, { transformation_code: code }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["transformations", id] }),
  });

  const publishMutation = useMutation({
    mutationFn: () => api.patch(`/transformations/${id}`, { transformation_code: code, is_published: true }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["transformations", id] }),
  });

  const previewMutation = useMutation({
    mutationFn: () => api.post(`/transformations/${id}/preview`, { transformation_code: code }),
  });

  if (isLoading) return <div className="flex items-center justify-center h-64">Loading...</div>;
  if (!transform) return <div className="text-center text-muted-foreground">Transform not found</div>;

  const filteredLogs = (logs ?? []).filter((log: any) => {
    if (logStatusFilter !== "all" && log.status !== logStatusFilter) return false;
    if (logConnectionFilter !== "all" && log.connection_name !== logConnectionFilter) return false;
    return true;
  });

  const successRate = logs?.length
    ? Math.round((logs.filter((l: any) => l.status === "success").length / logs.length) * 100)
    : 0;
  const avgDuration = logs?.length
    ? Math.round(logs.reduce((sum: number, l: any) => sum + (l.duration_ms ?? 0), 0) / logs.length)
    : 0;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-2xl font-bold">{transform.pipeline_name}</h1>
          <Badge variant="outline">v{transform.version ?? 1}</Badge>
          <Badge variant={transform.is_published ? "success" : "secondary"}>
            {transform.is_published ? "published" : "draft"}
          </Badge>
        </div>
        <Button variant="outline" onClick={() => validateMutation.mutate()} disabled={validateMutation.isPending}>
          <CheckCircle className="mr-2 h-4 w-4" />Validate
        </Button>
      </div>

      <Tabs defaultValue="editor">
        <TabsList>
          <TabsTrigger value="editor">Code Editor</TabsTrigger>
          <TabsTrigger value="visual">Visual Builder</TabsTrigger>
          <TabsTrigger value="logs">Execution Log</TabsTrigger>
          <TabsTrigger value="deps">Dependencies</TabsTrigger>
        </TabsList>

        <TabsContent value="editor" className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm">Transform Code</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="relative">
                  <div className="absolute left-0 top-0 bottom-0 w-10 bg-muted/50 rounded-l-md flex flex-col items-center pt-3 text-xs text-muted-foreground font-mono select-none">
                    {(code || "").split("\n").map((_, i) => (
                      <div key={i} className="leading-5">{i + 1}</div>
                    ))}
                  </div>
                  <textarea
                    className="w-full min-h-[350px] rounded-md border bg-muted p-3 pl-12 font-mono text-sm leading-5 focus:outline-none focus:ring-1 focus:ring-ring resize-y"
                    value={code || transform.transformation_code || ""}
                    onChange={(e) => setCode(e.target.value)}
                  />
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm">Output Preview</CardTitle>
              </CardHeader>
              <CardContent>
                {previewMutation.data ? (
                  <div className="space-y-4">
                    <div>
                      <h4 className="text-xs font-medium text-muted-foreground mb-1">Input Sample</h4>
                      <div className="rounded-md bg-muted p-2 text-xs overflow-auto max-h-[140px]">
                        <Table>
                          <TableBody>
                            {(previewMutation.data.data?.input ?? []).slice(0, 5).map((row: any, i: number) => (
                              <TableRow key={i}>
                                {Object.values(row).map((v: any, j: number) => (
                                  <TableCell key={j} className="py-1 px-2 text-xs">{String(v)}</TableCell>
                                ))}
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </div>
                    </div>
                    <div>
                      <h4 className="text-xs font-medium text-muted-foreground mb-1">Output Sample</h4>
                      <div className="rounded-md bg-muted p-2 text-xs overflow-auto max-h-[140px]">
                        <Table>
                          <TableBody>
                            {(previewMutation.data.data?.output ?? []).slice(0, 5).map((row: any, i: number) => (
                              <TableRow key={i}>
                                {Object.values(row).map((v: any, j: number) => (
                                  <TableCell key={j} className="py-1 px-2 text-xs">{String(v)}</TableCell>
                                ))}
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </div>
                    </div>
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">Click "Run Preview" to see transform output.</p>
                )}
                {validateMutation.data && (
                  <div className={`mt-3 rounded-md p-2 text-sm ${validateMutation.data.data?.valid !== false ? "bg-green-500/10 text-green-600" : "bg-destructive/10 text-destructive"}`}>
                    {validateMutation.data.data?.valid !== false ? "✓ Validation passed" : `✗ ${validateMutation.data.data?.error ?? "Validation failed"}`}
                  </div>
                )}
              </CardContent>
            </Card>
          </div>

          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Badge variant="outline">{transform.language ?? "python"}</Badge>
              <Badge variant="outline">{transform.pipeline_type ?? "spark"}</Badge>
            </div>
            <Button variant="outline" onClick={() => previewMutation.mutate()} disabled={previewMutation.isPending}>
              <Play className="mr-2 h-4 w-4" />Run Preview
            </Button>
          </div>

          <Card>
            <CardContent className="pt-4">
              <div className="grid grid-cols-3 gap-4 text-sm">
                <div>
                  <span className="text-muted-foreground">Linked Connections:</span>
                  <p className="font-medium">{transform.linked_connections?.join(", ") || "None"}</p>
                </div>
                <div>
                  <span className="text-muted-foreground">Last Executed:</span>
                  <p className="font-medium">{transform.last_executed ? new Date(transform.last_executed).toLocaleString() : "Never"}</p>
                </div>
                <div>
                  <span className="text-muted-foreground">Execution Mode:</span>
                  <p className="font-medium capitalize">{transform.execution_mode ?? "streaming"}</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <div className="flex gap-2">
            <Button onClick={() => saveMutation.mutate()} disabled={saveMutation.isPending}>
              <Save className="mr-2 h-4 w-4" />{saveMutation.isPending ? "Saving..." : "Save Draft"}
            </Button>
            <Button variant="outline" onClick={() => validateMutation.mutate()} disabled={validateMutation.isPending}>
              Validate
            </Button>
            <Button variant="outline" onClick={() => previewMutation.mutate()} disabled={previewMutation.isPending}>
              Preview
            </Button>
            <Button variant="default" onClick={() => publishMutation.mutate()} disabled={publishMutation.isPending}>
              {publishMutation.isPending ? "Publishing..." : "Publish"}
            </Button>
          </div>
        </TabsContent>

        <TabsContent value="visual">
          <Card>
            <CardContent className="flex items-center justify-center h-64 text-muted-foreground">
              Visual Builder coming soon. Use the Code Editor tab to define transformations.
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="logs" className="space-y-4">
          <div className="flex items-center gap-4">
            <Card className="flex-1">
              <CardContent className="pt-4 pb-4">
                <p className="text-sm text-muted-foreground">Success Rate</p>
                <p className="text-2xl font-bold">{successRate}%</p>
              </CardContent>
            </Card>
            <Card className="flex-1">
              <CardContent className="pt-4 pb-4">
                <p className="text-sm text-muted-foreground">Avg Duration</p>
                <p className="text-2xl font-bold">{avgDuration ? `${(avgDuration / 1000).toFixed(1)}s` : "—"}</p>
              </CardContent>
            </Card>
          </div>

          <div className="flex items-center gap-4">
            <Select value={logStatusFilter} onValueChange={setLogStatusFilter}>
              <SelectTrigger className="w-[150px]"><SelectValue placeholder="Status" /></SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Statuses</SelectItem>
                <SelectItem value="success">Success</SelectItem>
                <SelectItem value="failed">Failed</SelectItem>
              </SelectContent>
            </Select>
            <Select value={logConnectionFilter} onValueChange={setLogConnectionFilter}>
              <SelectTrigger className="w-[180px]"><SelectValue placeholder="Connection" /></SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Connections</SelectItem>
                {[...new Set((logs ?? []).map((l: any) => l.connection_name).filter(Boolean))].map((c: any) => (
                  <SelectItem key={c} value={c}>{c}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <Card>
            <CardContent className="p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Timestamp</TableHead>
                    <TableHead>Connection</TableHead>
                    <TableHead>Rows</TableHead>
                    <TableHead>Duration</TableHead>
                    <TableHead>Status</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredLogs.length === 0 ? (
                    <TableRow><TableCell colSpan={5} className="text-center text-muted-foreground">No execution logs</TableCell></TableRow>
                  ) : (
                    filteredLogs.map((log: any, i: number) => (
                      <TableRow key={i}>
                        <TableCell className="text-sm">{new Date(log.executed_at).toLocaleString()}</TableCell>
                        <TableCell>{log.connection_name ?? "—"}</TableCell>
                        <TableCell>{log.row_count?.toLocaleString() ?? "—"}</TableCell>
                        <TableCell>{log.duration_ms ? `${(log.duration_ms / 1000).toFixed(1)}s` : "—"}</TableCell>
                        <TableCell><Badge variant={log.status === "success" ? "success" : "destructive"}>{log.status}</Badge></TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="deps">
          <Card>
            <CardHeader><CardTitle>Dependencies</CardTitle></CardHeader>
            <CardContent className="space-y-6">
              <div>
                <h4 className="font-medium text-sm mb-2 flex items-center gap-2">
                  <ArrowUpDown className="h-4 w-4" />Upstream (depends on)
                </h4>
                {(dependencies?.upstream ?? transform.depends_on ?? []).length > 0 ? (
                  <div className="flex flex-wrap gap-2">
                    {(dependencies?.upstream ?? transform.depends_on ?? []).map((dep: string, i: number) => (
                      <Badge key={i} variant="outline">{dep}</Badge>
                    ))}
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">No upstream dependencies (root transform)</p>
                )}
              </div>
              <div>
                <h4 className="font-medium text-sm mb-2 flex items-center gap-2">
                  <ArrowUpDown className="h-4 w-4" />Downstream (used by)
                </h4>
                {(dependencies?.downstream ?? transform.used_by ?? []).length > 0 ? (
                  <div className="flex flex-wrap gap-2">
                    {(dependencies?.downstream ?? transform.used_by ?? []).map((dep: string, i: number) => (
                      <Badge key={i} variant="outline">{dep}</Badge>
                    ))}
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">No downstream consumers</p>
                )}
              </div>
              {(dependencies?.execution_order ?? []).length > 0 && (
                <div>
                  <h4 className="font-medium text-sm mb-2">Execution Order</h4>
                  <div className="flex items-center gap-2 flex-wrap">
                    {dependencies.execution_order.map((step: string, i: number) => (
                      <div key={i} className="flex items-center gap-2">
                        <Badge variant={step === transform.name ? "default" : "secondary"}>{step}</Badge>
                        {i < dependencies.execution_order.length - 1 && <span className="text-muted-foreground">→</span>}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
