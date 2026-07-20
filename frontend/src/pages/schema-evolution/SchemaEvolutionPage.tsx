import { useState } from "react";
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
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { GitBranch, Clock, CheckCircle, XCircle } from "lucide-react";

interface SchemaChange {
  id: string;
  connection_name: string;
  connection_id: string;
  table_name: string;
  description: string;
  change_type: "ADD_COLUMN" | "ALTER_TYPE" | "DROP_COLUMN";
  is_breaking: boolean;
  status: "pending" | "applied" | "rejected";
  detected_at: string;
  diff?: string;
  impact_assessment?: string;
}

export function SchemaEvolutionPage() {
  const queryClient = useQueryClient();
  const [reviewEvent, setReviewEvent] = useState<SchemaChange | null>(null);
  const [reviewNotes, setReviewNotes] = useState("");

  const { data: events, isLoading } = useQuery<SchemaChange[]>({
    queryKey: ["schema-evolution", "events"],
    queryFn: () => fetchList("/schema-evolution/events", "schema_changes").catch(() => []),
  });

  const approveMutation = useMutation({
    mutationFn: (changeId: string) =>
      api.post(`/schema-evolution/events/${changeId}/approve`, { notes: reviewNotes }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["schema-evolution"] });
      setReviewEvent(null);
      setReviewNotes("");
    },
  });

  const rejectMutation = useMutation({
    mutationFn: (changeId: string) =>
      api.post(`/schema-evolution/events/${changeId}/reject`, { notes: reviewNotes }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["schema-evolution"] });
      setReviewEvent(null);
      setReviewNotes("");
    },
  });

  const allEvents = events ?? [];
  const pendingCount = allEvents.filter((e) => e.status === "pending").length;
  const appliedCount = allEvents.filter((e) => e.status === "applied").length;
  const rejectedCount = allEvents.filter((e) => e.status === "rejected").length;

  const statusIcon = (status: string) => {
    switch (status) {
      case "pending":
        return <Clock className="h-4 w-4 text-yellow-500" />;
      case "applied":
        return <CheckCircle className="h-4 w-4 text-green-500" />;
      case "rejected":
        return <XCircle className="h-4 w-4 text-red-500" />;
      default:
        return null;
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <GitBranch className="h-6 w-6" />
        <h1 className="text-2xl font-bold">Schema Evolution</h1>
      </div>

      {/* Summary Stats */}
      <div className="grid gap-4 md:grid-cols-3">
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Pending Approval</p>
            <p className="text-2xl font-bold text-yellow-600">{pendingCount}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Auto-Applied (24h)</p>
            <p className="text-2xl font-bold text-green-600">{appliedCount}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Rejected</p>
            <p className="text-2xl font-bold text-red-600">{rejectedCount}</p>
          </CardContent>
        </Card>
      </div>

      {/* Events Table */}
      <Card>
        <CardHeader>
          <CardTitle>Schema Change Events</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Status</TableHead>
                <TableHead>Connection</TableHead>
                <TableHead>Change</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Breaking?</TableHead>
                <TableHead>Action</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow>
                  <TableCell colSpan={6} className="text-center">Loading...</TableCell>
                </TableRow>
              ) : allEvents.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} className="text-center text-muted-foreground">
                    No schema changes detected
                  </TableCell>
                </TableRow>
              ) : (
                allEvents.map((evt) => (
                  <TableRow key={evt.id}>
                    <TableCell>{statusIcon(evt.status)}</TableCell>
                    <TableCell className="font-medium">
                      {evt.connection_name ?? evt.connection_id}
                    </TableCell>
                    <TableCell className="max-w-xs truncate">{evt.description}</TableCell>
                    <TableCell>
                      <Badge variant="outline">{evt.change_type}</Badge>
                    </TableCell>
                    <TableCell>
                      {evt.is_breaking ? (
                        <Badge variant="destructive">Yes</Badge>
                      ) : (
                        <span className="text-muted-foreground text-sm">No</span>
                      )}
                    </TableCell>
                    <TableCell>
                      {evt.status === "pending" && (
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => {
                            setReviewEvent(evt);
                            setReviewNotes("");
                          }}
                        >
                          Review
                        </Button>
                      )}
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* Review Dialog */}
      <Dialog open={!!reviewEvent} onOpenChange={(open) => !open && setReviewEvent(null)}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle>Review Schema Change</DialogTitle>
          </DialogHeader>
          {reviewEvent && (
            <div className="space-y-4">
              <dl className="grid grid-cols-2 gap-3 text-sm">
                <div>
                  <dt className="text-muted-foreground">Connection</dt>
                  <dd className="font-medium">
                    {reviewEvent.connection_name ?? reviewEvent.connection_id}
                  </dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Table</dt>
                  <dd className="font-mono">{reviewEvent.table_name}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Detected</dt>
                  <dd>{new Date(reviewEvent.detected_at).toLocaleString()}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Change Type</dt>
                  <dd>
                    <Badge variant="outline">{reviewEvent.change_type}</Badge>
                  </dd>
                </div>
                <div className="col-span-2">
                  <dt className="text-muted-foreground">Breaking Change</dt>
                  <dd>
                    {reviewEvent.is_breaking ? (
                      <Badge variant="destructive">Yes</Badge>
                    ) : (
                      <Badge variant="secondary">No</Badge>
                    )}
                  </dd>
                </div>
              </dl>

              {/* Diff View */}
              {reviewEvent.diff && (
                <div>
                  <p className="text-sm font-medium mb-1">Diff</p>
                  <pre className="rounded-md bg-muted p-3 text-xs overflow-auto max-h-40 font-mono">
                    {reviewEvent.diff}
                  </pre>
                </div>
              )}

              {/* Impact Assessment */}
              {reviewEvent.impact_assessment && (
                <div>
                  <p className="text-sm font-medium mb-1">Impact Assessment</p>
                  <p className="text-sm text-muted-foreground rounded-md bg-muted p-3">
                    {reviewEvent.impact_assessment}
                  </p>
                </div>
              )}

              {/* Review Notes */}
              <div>
                <label className="text-sm font-medium">Review Notes</label>
                <Input
                  className="mt-1"
                  placeholder="Optional notes..."
                  value={reviewNotes}
                  onChange={(e) => setReviewNotes(e.target.value)}
                />
              </div>
            </div>
          )}
          <DialogFooter className="gap-2">
            <Button
              variant="destructive"
              onClick={() => reviewEvent && rejectMutation.mutate(reviewEvent.id)}
              disabled={rejectMutation.isPending}
            >
              Reject
            </Button>
            <Button
              onClick={() => reviewEvent && approveMutation.mutate(reviewEvent.id)}
              disabled={approveMutation.isPending}
            >
              Approve & Apply
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
