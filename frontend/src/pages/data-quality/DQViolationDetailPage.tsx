import { useState } from "react";
import { useParams } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";

export function DQViolationDetailPage() {
  const { id } = useParams<{ id: string }>();
  const queryClient = useQueryClient();
  const [notes, setNotes] = useState("");

  const { data: violation, isLoading } = useQuery({
    queryKey: ["data-quality", "violations", id],
    queryFn: () => api.get(`/data-quality/violations/${id}`).then((r) => r.data),
  });

  const resolveMutation = useMutation({
    mutationFn: (action: string) => api.patch(`/data-quality/violations/${id}`, { status: action, notes }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["data-quality", "violations", id] });
      setNotes("");
    },
  });

  if (isLoading) return <div className="flex items-center justify-center h-64">Loading...</div>;
  if (!violation) return <div className="text-center text-muted-foreground">Violation not found</div>;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Violation Detail</h1>
        <Badge variant={violation.status === "resolved" ? "default" : violation.status === "ignored" ? "secondary" : "destructive"}>
          {violation.status}
        </Badge>
      </div>

      <Card>
        <CardHeader><CardTitle>Metadata</CardTitle></CardHeader>
        <CardContent>
          <dl className="grid grid-cols-2 gap-4 text-sm">
            <div>
              <dt className="text-muted-foreground">Violation ID</dt>
              <dd className="font-mono text-xs">{violation.id}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Policy</dt>
              <dd className="font-medium">{violation.policy_name}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Connection</dt>
              <dd>{violation.connection_name ?? "\u2014"}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Stream</dt>
              <dd className="font-mono">{violation.stream ?? "\u2014"}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Column</dt>
              <dd className="font-mono">{violation.column ?? "\u2014"}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Severity</dt>
              <dd><Badge variant={violation.severity === "critical" ? "destructive" : "secondary"}>{violation.severity}</Badge></dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Detected At</dt>
              <dd>{new Date(violation.detected_at).toLocaleString()}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Violation Count</dt>
              <dd>{violation.count ?? 1}</dd>
            </div>
          </dl>
        </CardContent>
      </Card>

      {violation.sample_records && violation.sample_records.length > 0 && (
        <Card>
          <CardHeader><CardTitle>Sample Failing Records</CardTitle></CardHeader>
          <CardContent>
            {Array.isArray(violation.sample_records) && typeof violation.sample_records[0] === "object" ? (
              <div className="overflow-auto max-h-64">
                <Table>
                  <TableHeader>
                    <TableRow>
                      {Object.keys(violation.sample_records[0]).map((key) => (
                        <TableHead key={key}>{key}</TableHead>
                      ))}
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {violation.sample_records.map((record: any, i: number) => (
                      <TableRow key={i}>
                        {Object.values(record).map((val: any, j: number) => (
                          <TableCell key={j} className="font-mono text-xs">{String(val ?? "null")}</TableCell>
                        ))}
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            ) : (
              <pre className="rounded-md bg-muted p-4 text-sm overflow-auto max-h-64">
                {JSON.stringify(violation.sample_records, null, 2)}
              </pre>
            )}
          </CardContent>
        </Card>
      )}

      {violation.status === "active" && (
        <Card>
          <CardHeader><CardTitle>Resolution</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">Notes</label>
              <textarea
                className="flex min-h-[80px] w-full rounded-md border border-input bg-background px-3 py-2 text-sm placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                value={notes}
                onChange={(e) => setNotes(e.target.value)}
                placeholder="Add resolution notes..."
              />
            </div>
            <div className="flex gap-2">
              <Button onClick={() => resolveMutation.mutate("resolved")} disabled={resolveMutation.isPending}>
                Resolve
              </Button>
              <Button variant="outline" onClick={() => resolveMutation.mutate("ignored")} disabled={resolveMutation.isPending}>
                Ignore
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      {violation.timeline && violation.timeline.length > 0 && (
        <Card>
          <CardHeader><CardTitle>Timeline</CardTitle></CardHeader>
          <CardContent>
            <div className="space-y-3">
              {violation.timeline.map((event: any, i: number) => (
                <div key={i} className="flex items-start gap-3 text-sm">
                  <div className="h-2 w-2 rounded-full bg-primary mt-1.5 shrink-0" />
                  <div>
                    <p className="font-medium">{event.action}</p>
                    <p className="text-muted-foreground">{new Date(event.timestamp).toLocaleString()}</p>
                    {event.notes && <p className="text-muted-foreground mt-1">{event.notes}</p>}
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
