import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { ArrowLeft } from "lucide-react";

export function AlertRuleDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: rule, isLoading } = useQuery({
    queryKey: ["alerts", "rules", id],
    queryFn: () => api.get(`/alerts/rules/${id}`).then((r) => r.data),
  });

  const { data: evaluations = [] } = useQuery({
    queryKey: ["alerts", "rules", id, "evaluations"],
    queryFn: () => api.get(`/alerts/rules/${id}/evaluations`).then((r) => r.data).catch(() => []),
    enabled: !!id,
  });

  const toggleMutation = useMutation({
    mutationFn: () => api.patch(`/alerts/rules/${id}`, { enabled: !rule?.enabled }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["alerts", "rules", id] }),
  });

  if (isLoading) return <div className="flex items-center justify-center h-64">Loading...</div>;
  if (!rule) return <div className="text-center text-muted-foreground py-16">Rule not found</div>;

  const severityVariant = rule.severity === "critical" ? "destructive" : rule.severity === "error" ? "warning" : rule.severity === "warning" ? "warning" : "secondary";

  return (
    <div className="space-y-6">
      <Button variant="ghost" size="sm" onClick={() => navigate("/alerts/rules")} className="gap-1">
        <ArrowLeft className="h-4 w-4" /> Back to Rules
      </Button>

      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-bold">{rule.name}</h1>
          <p className="text-sm text-muted-foreground mt-1">{rule.description ?? `Monitors ${rule.metric ?? rule.alert_type}`}</p>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant={severityVariant}>{rule.severity}</Badge>
          <Badge variant={rule.enabled ? "success" : "secondary"}>{rule.enabled ? "Active" : "Muted"}</Badge>
          <Button variant="outline" size="sm" onClick={() => toggleMutation.mutate()}>
            {rule.enabled ? "Mute" : "Activate"}
          </Button>
        </div>
      </div>

      <Tabs defaultValue="configuration">
        <TabsList>
          <TabsTrigger value="configuration">Configuration</TabsTrigger>
          <TabsTrigger value="evaluations">Evaluations</TabsTrigger>
        </TabsList>

        <TabsContent value="configuration" className="space-y-6 mt-4">
          <Card>
            <CardHeader><CardTitle>Rule Configuration</CardTitle></CardHeader>
            <CardContent>
              <dl className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                <div>
                  <dt className="text-muted-foreground">Type</dt>
                  <dd className="font-medium mt-0.5">{rule.alert_type ?? rule.metric ?? "—"}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Severity</dt>
                  <dd className="mt-0.5"><Badge variant={severityVariant}>{rule.severity}</Badge></dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Scope</dt>
                  <dd className="font-medium mt-0.5">{rule.scope ?? "All connections"}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Condition</dt>
                  <dd className="font-mono mt-0.5">
                    {rule.metric ?? "metric"} {rule.operator ?? ">"} {rule.threshold ?? "—"}
                    {rule.duration ? ` for ${rule.duration}s` : ""}
                  </dd>
                </div>
              </dl>
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle>Notification Channels</CardTitle></CardHeader>
            <CardContent>
              {(rule.channels ?? []).length === 0 ? (
                <p className="text-sm text-muted-foreground">No channels configured</p>
              ) : (
                <div className="flex flex-wrap gap-2">
                  {(rule.channels ?? []).map((ch: string, i: number) => (
                    <Badge key={i} variant="outline">{ch}</Badge>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle>Behavior</CardTitle></CardHeader>
            <CardContent>
              <dl className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                <div>
                  <dt className="text-muted-foreground">Cooldown</dt>
                  <dd className="font-medium mt-0.5">{rule.cooldown_minutes ? `${rule.cooldown_minutes} minutes` : "No cooldown"}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Auto-resolve</dt>
                  <dd className="font-medium mt-0.5">{rule.auto_resolve_minutes ? `After ${rule.auto_resolve_minutes} minutes below threshold` : "Manual resolution"}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Evaluation Interval</dt>
                  <dd className="font-medium mt-0.5">{rule.evaluation_interval ?? "Every 5 minutes"}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Escalation</dt>
                  <dd className="font-medium mt-0.5">
                    {rule.escalation_minutes ? `After ${rule.escalation_minutes} min → ${rule.escalation_channel ?? "default"}` : "None"}
                  </dd>
                </div>
              </dl>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="evaluations" className="mt-4">
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle>Recent Evaluations</CardTitle>
                <p className="text-sm text-muted-foreground">
                  Interval: {rule.evaluation_interval ?? "every 5 minutes"}
                </p>
              </div>
            </CardHeader>
            <CardContent className="p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Evaluated At</TableHead>
                    <TableHead>Connection</TableHead>
                    <TableHead>Result</TableHead>
                    <TableHead>Metric Value</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {evaluations.length === 0 ? (
                    <TableRow><TableCell colSpan={4} className="text-center text-muted-foreground py-8">No evaluations yet</TableCell></TableRow>
                  ) : (
                    evaluations.map((e: any, i: number) => (
                      <TableRow key={i}>
                        <TableCell className="text-muted-foreground">{new Date(e.evaluated_at).toLocaleString()}</TableCell>
                        <TableCell>{e.connection_name ?? e.connection_id ?? "—"}</TableCell>
                        <TableCell>
                          {e.fired ? (
                            <Badge variant="destructive">🔴 FIRED</Badge>
                          ) : (
                            <Badge variant="success">● OK</Badge>
                          )}
                        </TableCell>
                        <TableCell className="font-mono text-sm">
                          {e.metric_value !== undefined ? e.metric_value : "—"}
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
