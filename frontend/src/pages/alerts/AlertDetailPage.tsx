import { useParams, useNavigate, Link } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ArrowLeft, ExternalLink } from "lucide-react";

interface AlertEvent {
  action: string;
  timestamp: string;
  actor?: string;
}

export function AlertDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: alert, isLoading } = useQuery({
    queryKey: ["alerts", id],
    queryFn: () => api.get(`/alerts/${id}`).then((r) => r.data),
  });

  const acknowledgeMutation = useMutation({
    mutationFn: () => api.post(`/alerts/${id}/acknowledge`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["alerts", id] }),
  });

  const resolveMutation = useMutation({
    mutationFn: () => api.post(`/alerts/${id}/resolve`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["alerts", id] }),
  });

  const suppressMutation = useMutation({
    mutationFn: () => api.post(`/alerts/${id}/suppress`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["alerts", id] }),
  });

  if (isLoading) return <div className="flex items-center justify-center h-64">Loading...</div>;
  if (!alert) return <div className="text-center text-muted-foreground py-16">Alert not found</div>;

  const severityVariant = alert.severity === "critical" ? "destructive" : alert.severity === "warning" ? "warning" : "secondary";
  const statusVariant = alert.status === "resolved" ? "success" : alert.status === "acknowledged" ? "outline" : "destructive";

  const timeline: AlertEvent[] = [];
  if (alert.triggered_at) timeline.push({ action: "Triggered", timestamp: alert.triggered_at });
  if (alert.acknowledged_at) timeline.push({ action: "Acknowledged", timestamp: alert.acknowledged_at, actor: alert.acknowledged_by });
  if (alert.resolved_at) timeline.push({ action: "Resolved", timestamp: alert.resolved_at, actor: alert.resolved_by });

  return (
    <div className="space-y-6">
      <Button variant="ghost" size="sm" onClick={() => navigate("/alerts")} className="gap-1">
        <ArrowLeft className="h-4 w-4" /> Back to Alerts
      </Button>

      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-bold">{alert.title ?? alert.rule_name ?? "Alert"}</h1>
          <p className="text-muted-foreground mt-1">{alert.message}</p>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant={severityVariant}>{alert.severity}</Badge>
          <Badge variant={statusVariant}>{alert.status}</Badge>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader><CardTitle>Alert Details</CardTitle></CardHeader>
          <CardContent>
            <dl className="space-y-3 text-sm">
              <div className="flex justify-between">
                <dt className="text-muted-foreground">Rule</dt>
                <dd className="font-medium">{alert.rule_name ?? "—"}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-muted-foreground">Connection</dt>
                <dd>
                  {alert.connection_id ? (
                    <Link to={`/connections/${alert.connection_id}`} className="text-primary hover:underline inline-flex items-center gap-1">
                      {alert.connection_name ?? alert.connection_id}
                      <ExternalLink className="h-3 w-3" />
                    </Link>
                  ) : "—"}
                </dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-muted-foreground">Triggered</dt>
                <dd>{alert.triggered_at ? new Date(alert.triggered_at).toLocaleString() : "—"}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-muted-foreground">Notification Sent</dt>
                <dd>{alert.notification_sent ? "Yes" : "No"}</dd>
              </div>
            </dl>
          </CardContent>
        </Card>

        <Card>
          <CardHeader><CardTitle>Timeline</CardTitle></CardHeader>
          <CardContent>
            {timeline.length === 0 ? (
              <p className="text-sm text-muted-foreground">No timeline events</p>
            ) : (
              <ol className="relative border-l border-border ml-3 space-y-4">
                {timeline.map((event, i) => (
                  <li key={i} className="ml-4">
                    <div className="absolute -left-1.5 mt-1.5 h-3 w-3 rounded-full border bg-background border-border" />
                    <p className="text-sm font-medium">{event.action}</p>
                    <p className="text-xs text-muted-foreground">{new Date(event.timestamp).toLocaleString()}</p>
                    {event.actor && <p className="text-xs text-muted-foreground">by {event.actor}</p>}
                  </li>
                ))}
              </ol>
            )}
          </CardContent>
        </Card>
      </div>

      {alert.context && (
        <Card>
          <CardHeader><CardTitle>Context</CardTitle></CardHeader>
          <CardContent>
            <pre className="rounded-md bg-muted p-4 text-xs font-mono overflow-auto max-h-64">
              {JSON.stringify(alert.context, null, 2)}
            </pre>
          </CardContent>
        </Card>
      )}

      <Card>
        <CardHeader><CardTitle>Related Metrics</CardTitle></CardHeader>
        <CardContent>
          <div className="h-48 flex items-center justify-center border border-dashed rounded-md text-muted-foreground text-sm">
            Metric chart at time of alert (coming soon)
          </div>
        </CardContent>
      </Card>

      <div className="flex gap-2">
        {alert.status === "active" && (
          <>
            <Button onClick={() => acknowledgeMutation.mutate()} disabled={acknowledgeMutation.isPending}>
              Acknowledge
            </Button>
            <Button variant="outline" onClick={() => resolveMutation.mutate()} disabled={resolveMutation.isPending}>
              Resolve
            </Button>
          </>
        )}
        {alert.status === "acknowledged" && (
          <Button onClick={() => resolveMutation.mutate()} disabled={resolveMutation.isPending}>
            Resolve
          </Button>
        )}
        {alert.status !== "resolved" && (
          <Button variant="secondary" onClick={() => suppressMutation.mutate()} disabled={suppressMutation.isPending}>
            Suppress
          </Button>
        )}
      </div>
    </div>
  );
}
