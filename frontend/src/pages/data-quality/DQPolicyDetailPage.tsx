import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { Play, Pencil, PowerOff, Trash2, Download } from "lucide-react";

export function DQPolicyDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: policy, isLoading } = useQuery({
    queryKey: ["data-quality", "policies", id],
    queryFn: () => api.get(`/data-quality/policies/${id}`).then((r) => r.data),
  });

  const { data: results } = useQuery({
    queryKey: ["data-quality", "policies", id, "results"],
    queryFn: () => api.get(`/data-quality/policies/${id}/results`).then((r) => r.data).catch(() => []),
    enabled: !!id,
  });

  const executeMutation = useMutation({
    mutationFn: () => api.post(`/data-quality/policies/${id}/execute`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["data-quality", "policies", id] }),
  });

  const disableMutation = useMutation({
    mutationFn: () => api.patch(`/data-quality/policies/${id}`, { enabled: !policy?.enabled }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["data-quality", "policies", id] }),
  });

  const deleteMutation = useMutation({
    mutationFn: () => api.delete(`/data-quality/policies/${id}`),
    onSuccess: () => navigate("/data-quality/policies"),
  });

  if (isLoading) return <div className="flex items-center justify-center h-64">Loading...</div>;
  if (!policy) return <div className="text-center text-muted-foreground">Policy not found</div>;

  const passRate = policy.pass_rate ?? (results?.length ? Math.round((results.filter((r: any) => r.passed).length / results.length) * 100) : 0);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-2xl font-bold">{policy.name}</h1>
          <Badge variant={policy.enabled ? "default" : "secondary"}>
            {policy.enabled ? "Active" : "Disabled"}
          </Badge>
          <Badge variant={policy.severity === "critical" ? "destructive" : policy.severity === "error" ? "destructive" : "secondary"}>
            {policy.severity ?? "warning"}
          </Badge>
        </div>
        <div className="flex gap-2">
          <Button onClick={() => executeMutation.mutate()} disabled={executeMutation.isPending}>
            <Play className="mr-2 h-4 w-4" />{executeMutation.isPending ? "Running..." : "Execute Now"}
          </Button>
          <Button variant="outline" onClick={() => navigate(`/data-quality/policies/${id}/edit`)}>
            <Pencil className="mr-2 h-4 w-4" />Edit
          </Button>
          <Button variant="outline" onClick={() => disableMutation.mutate()}>
            <PowerOff className="mr-2 h-4 w-4" />{policy.enabled ? "Disable" : "Enable"}
          </Button>
          <Button variant="destructive" onClick={() => { if (confirm("Delete this policy?")) deleteMutation.mutate(); }}>
            <Trash2 className="mr-2 h-4 w-4" />Delete
          </Button>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-4">
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Connection</p>
            <p className="font-medium">{policy.connection_name ?? "All"}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Stream</p>
            <p className="font-medium">{policy.stream ?? "\u2014"}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Pass Rate</p>
            <p className="font-medium">{passRate}%</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Total Violations</p>
            <p className="font-medium">{policy.total_violations ?? 0}</p>
          </CardContent>
        </Card>
      </div>

      <Tabs defaultValue="overview">
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="results">Execution Results</TabsTrigger>
          <TabsTrigger value="config">Configuration</TabsTrigger>
        </TabsList>

        <TabsContent value="overview">
          <Card>
            <CardHeader><CardTitle>Policy Details</CardTitle></CardHeader>
            <CardContent>
              <dl className="grid grid-cols-2 gap-4 text-sm">
                <div>
                  <dt className="text-muted-foreground">Rule Type</dt>
                  <dd className="font-mono">{policy.rule_type ?? policy.rules?.[0]?.type ?? "\u2014"}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Severity</dt>
                  <dd><Badge variant={policy.severity === "critical" ? "destructive" : "secondary"}>{policy.severity}</Badge></dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Action on Failure</dt>
                  <dd>{policy.action ?? "reject"}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Threshold</dt>
                  <dd>{policy.threshold != null ? `${(policy.threshold * 100).toFixed(1)}%` : "\u2014"}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Target Columns</dt>
                  <dd className="font-mono">{policy.columns?.join(", ") ?? policy.rules?.[0]?.columns?.join(", ") ?? "All"}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Last Run</dt>
                  <dd>{policy.last_run ? new Date(policy.last_run).toLocaleString() : "Never"}</dd>
                </div>
              </dl>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="results">
          <div className="space-y-4">
            <div className="flex justify-end gap-2">
              <Button onClick={() => executeMutation.mutate()} disabled={executeMutation.isPending}>
                <Play className="mr-2 h-4 w-4" />Execute Now
              </Button>
              <Button variant="outline">
                <Download className="mr-2 h-4 w-4" />Export Results
              </Button>
            </div>
            <Card>
              <CardContent className="p-0">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Run Time</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Checked</TableHead>
                      <TableHead>Failed</TableHead>
                      <TableHead>Action Taken</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {(results ?? []).length === 0 ? (
                      <TableRow><TableCell colSpan={5} className="text-center text-muted-foreground">No results yet</TableCell></TableRow>
                    ) : (
                      (results ?? []).map((r: any, i: number) => (
                        <TableRow key={i}>
                          <TableCell>{new Date(r.executed_at).toLocaleString()}</TableCell>
                          <TableCell>
                            <Badge variant={r.passed ? "default" : "destructive"}>
                              {r.passed ? "Passed" : "Failed"}
                            </Badge>
                          </TableCell>
                          <TableCell>{r.records_checked?.toLocaleString() ?? "\u2014"}</TableCell>
                          <TableCell>{r.records_failed?.toLocaleString() ?? "\u2014"}</TableCell>
                          <TableCell>{r.action_taken ?? "\u2014"}</TableCell>
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        <TabsContent value="config">
          <Card>
            <CardHeader><CardTitle>Rule Configuration</CardTitle></CardHeader>
            <CardContent>
              <pre className="rounded-md bg-muted p-4 text-sm overflow-auto">
                {JSON.stringify(policy.rules ?? policy.config ?? {}, null, 2)}
              </pre>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
