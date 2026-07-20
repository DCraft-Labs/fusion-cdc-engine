import { useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Table, TableHeader, TableBody, TableRow, TableHead, TableCell,
} from "@/components/ui/table";
import {
  Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter,
} from "@/components/ui/dialog";
import { ArrowLeft, RotateCcw, Pencil, Trash2 } from "lucide-react";

interface RetryHistoryEntry {
  attempt: number;
  status: string;
  error_message: string;
}

interface DLQEventData {
  id: string;
  connection_id: string;
  connection_name: string;
  table: string;
  operation: string;
  failed_at: string;
  reason: string;
  retry_count: number;
  max_retries: number;
  status: string;
  payload: {
    op: string;
    before: Record<string, unknown> | null;
    after: Record<string, unknown> | null;
    ts_ms: number;
    source: Record<string, unknown>;
  };
  retry_history: RetryHistoryEntry[];
}

export function DLQEventDetailPage() {
  const { connectionId, eventId } = useParams<{ connectionId: string; eventId: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [editDialogOpen, setEditDialogOpen] = useState(false);
  const [editedPayload, setEditedPayload] = useState("");

  const { data: event, isLoading } = useQuery<DLQEventData>({
    queryKey: ["dlq", connectionId, eventId],
    queryFn: () => api.get(`/dlq/${connectionId}/${eventId}`).then((r) => r.data),
  });

  const retryMutation = useMutation({
    mutationFn: () => api.post(`/dlq/${connectionId}/${eventId}/retry`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["dlq"] }),
  });

  const editRetryMutation = useMutation({
    mutationFn: (payload: string) =>
      api.post(`/dlq/${connectionId}/${eventId}/retry`, { payload: JSON.parse(payload) }),
    onSuccess: () => {
      setEditDialogOpen(false);
      queryClient.invalidateQueries({ queryKey: ["dlq"] });
    },
  });

  const deleteMutation = useMutation({
    mutationFn: () => api.delete(`/dlq/${connectionId}/${eventId}`),
    onSuccess: () => navigate(`/dlq/${connectionId}`),
  });

  if (isLoading) return <div className="flex items-center justify-center h-64">Loading...</div>;
  if (!event) return <div className="text-center text-muted-foreground">Event not found</div>;

  const openEditDialog = () => {
    setEditedPayload(JSON.stringify(event.payload, null, 2));
    setEditDialogOpen(true);
  };

  const operationColor = {
    INSERT: "bg-green-100 text-green-800",
    UPDATE: "bg-blue-100 text-blue-800",
    DELETE: "bg-red-100 text-red-800",
  }[event.operation] ?? "bg-gray-100 text-gray-800";

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <Button variant="ghost" size="icon" onClick={() => navigate(`/dlq/${connectionId}`)}>
          <ArrowLeft className="h-4 w-4" />
        </Button>
        <h1 className="text-2xl font-bold">DLQ Event Detail</h1>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Event Information</CardTitle>
        </CardHeader>
        <CardContent>
          <dl className="grid grid-cols-2 md:grid-cols-3 gap-4 text-sm">
            <div>
              <dt className="text-muted-foreground">Connection</dt>
              <dd className="font-medium">{event.connection_name}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Table</dt>
              <dd className="font-medium">{event.table}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Operation</dt>
              <dd>
                <span className={`inline-block px-2 py-0.5 rounded text-xs font-semibold ${operationColor}`}>
                  {event.operation}
                </span>
              </dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Failed At</dt>
              <dd>{new Date(event.failed_at).toLocaleString()}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Reason</dt>
              <dd><Badge variant="outline">{event.reason}</Badge></dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Retry Count</dt>
              <dd className="font-mono">{event.retry_count}/{event.max_retries ?? 3}</dd>
            </div>
          </dl>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Event Payload</CardTitle>
        </CardHeader>
        <CardContent>
          <pre className="rounded-md bg-muted p-4 text-sm overflow-auto max-h-96 font-mono">
            {JSON.stringify(event.payload, null, 2)}
          </pre>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Retry History</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Attempt</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Error Message</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {!event.retry_history?.length ? (
                <TableRow>
                  <TableCell colSpan={3} className="text-center text-muted-foreground">
                    No retry attempts yet
                  </TableCell>
                </TableRow>
              ) : (
                event.retry_history.map((entry) => (
                  <TableRow key={entry.attempt}>
                    <TableCell className="font-mono">{entry.attempt}</TableCell>
                    <TableCell>
                      <Badge variant={entry.status === "success" ? "default" : "destructive"}>
                        {entry.status}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-sm text-muted-foreground max-w-md truncate">
                      {entry.error_message || "—"}
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <div className="flex gap-2">
        <Button onClick={() => retryMutation.mutate()} disabled={retryMutation.isPending}>
          <RotateCcw className="mr-2 h-4 w-4" />
          Retry Now
        </Button>
        <Button variant="outline" onClick={openEditDialog}>
          <Pencil className="mr-2 h-4 w-4" />
          Edit & Retry
        </Button>
        <Button variant="destructive" onClick={() => deleteMutation.mutate()} disabled={deleteMutation.isPending}>
          <Trash2 className="mr-2 h-4 w-4" />
          Delete
        </Button>
        <Button variant="ghost" onClick={() => navigate(`/dlq/${connectionId}`)}>
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back to List
        </Button>
      </div>

      <Dialog open={editDialogOpen} onOpenChange={setEditDialogOpen}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Edit Event Payload & Retry</DialogTitle>
          </DialogHeader>
          <textarea
            className="w-full h-80 rounded-md border bg-muted p-3 font-mono text-sm resize-none focus:outline-none focus:ring-2 focus:ring-ring"
            value={editedPayload}
            onChange={(e) => setEditedPayload(e.target.value)}
          />
          <DialogFooter>
            <Button variant="ghost" onClick={() => setEditDialogOpen(false)}>Cancel</Button>
            <Button
              onClick={() => editRetryMutation.mutate(editedPayload)}
              disabled={editRetryMutation.isPending}
            >
              <RotateCcw className="mr-2 h-4 w-4" />
              Retry with Edits
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
