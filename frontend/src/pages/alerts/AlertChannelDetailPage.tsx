import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { ArrowLeft } from "lucide-react";

function maskUrl(url?: string): string {
  if (!url) return "—";
  if (url.length <= 20) return url;
  return url.slice(0, 15) + "•••" + url.slice(-8);
}

export function AlertChannelDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: channel, isLoading } = useQuery({
    queryKey: ["alerts", "channels", id],
    queryFn: () => api.get(`/alerts/channels/${id}`).then((r) => r.data),
  });

  const { data: testHistory = [] } = useQuery({
    queryKey: ["alerts", "channels", id, "tests"],
    queryFn: () => api.get(`/alerts/channels/${id}/tests`).then((r) => r.data).catch(() => []),
    enabled: !!id,
  });

  const testMutation = useMutation({
    mutationFn: () => api.post(`/alerts/channels/${id}/test`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["alerts", "channels", id, "tests"] }),
  });

  const deleteMutation = useMutation({
    mutationFn: () => api.delete(`/alerts/channels/${id}`),
    onSuccess: () => navigate("/alerts/channels"),
  });

  if (isLoading) return <div className="flex items-center justify-center h-64">Loading...</div>;
  if (!channel) return <div className="text-center text-muted-foreground py-16">Channel not found</div>;

  return (
    <div className="space-y-6">
      <Button variant="ghost" size="sm" onClick={() => navigate("/alerts/channels")} className="gap-1">
        <ArrowLeft className="h-4 w-4" /> Back to Channels
      </Button>

      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-bold">{channel.name}</h1>
          <div className="flex items-center gap-2 mt-1">
            <Badge variant="outline">{channel.type}</Badge>
            <Badge variant={channel.enabled !== false ? "success" : "secondary"}>
              {channel.enabled !== false ? "Active" : "Disabled"}
            </Badge>
          </div>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" size="sm" onClick={() => testMutation.mutate()} disabled={testMutation.isPending}>
            {testMutation.isPending ? "Sending..." : "Test Channel"}
          </Button>
          <Button variant="outline" size="sm" onClick={() => navigate(`/alerts/channels/${id}/edit`)}>
            Edit
          </Button>
          <Button variant="destructive" size="sm" onClick={() => { if (confirm("Delete this channel?")) deleteMutation.mutate(); }}>
            Delete
          </Button>
        </div>
      </div>

      {testMutation.isSuccess && <p className="text-sm text-green-600">Test notification sent successfully</p>}
      {testMutation.isError && <p className="text-sm text-destructive">Test failed</p>}

      <Card>
        <CardHeader><CardTitle>Configuration</CardTitle></CardHeader>
        <CardContent>
          <dl className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
            <div>
              <dt className="text-muted-foreground">Name</dt>
              <dd className="font-medium mt-0.5">{channel.name}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Type</dt>
              <dd className="mt-0.5"><Badge variant="outline">{channel.type}</Badge></dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Endpoint</dt>
              <dd className="font-mono text-xs mt-0.5">{maskUrl(channel.webhook_url ?? channel.endpoint ?? channel.email)}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Rate Limit</dt>
              <dd className="mt-0.5">
                {channel.rate_limit_per_hour ? `${channel.rate_limit_per_hour}/hour` : "Unlimited"}
                {channel.rate_limit_per_day ? `, ${channel.rate_limit_per_day}/day` : ""}
              </dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Created</dt>
              <dd className="mt-0.5">{channel.created_at ? new Date(channel.created_at).toLocaleString() : "—"}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Used By</dt>
              <dd className="mt-0.5">{channel.rule_count ?? 0} rules</dd>
            </div>
          </dl>
        </CardContent>
      </Card>

      <Card>
        <CardHeader><CardTitle>Test History</CardTitle></CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Sent At</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Response</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {testHistory.length === 0 ? (
                <TableRow><TableCell colSpan={3} className="text-center text-muted-foreground py-6">No test history</TableCell></TableRow>
              ) : (
                testHistory.map((t: any, i: number) => (
                  <TableRow key={i}>
                    <TableCell className="text-muted-foreground">{new Date(t.sent_at).toLocaleString()}</TableCell>
                    <TableCell>
                      <Badge variant={t.success ? "success" : "destructive"}>
                        {t.success ? "Success" : "Failed"}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-muted-foreground text-xs font-mono truncate max-w-[200px]">{t.response ?? "—"}</TableCell>
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
