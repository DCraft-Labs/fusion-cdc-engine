import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { Loader2 } from "lucide-react";

export function DQProfilingPage() {
  const [connectionId, setConnectionId] = useState("");
  const [table, setTable] = useState("");

  const { data: connections } = useQuery({
    queryKey: ["connections"],
    queryFn: () => fetchList("/connections", "connections").catch(() => []),
  });

  const { data: streams } = useQuery({
    queryKey: ["connections", connectionId, "streams"],
    queryFn: () => fetchList(`/streams/connections/${connectionId}/streams`, "streams").catch(() => []),
    enabled: !!connectionId,
  });

  const profileMutation = useMutation({
    mutationFn: () => api.post("/data-quality/profiling/profile", { connection_id: connectionId, table_name: table }),
  });

  const results = profileMutation.data?.data?.columns ?? profileMutation.data?.data;

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Data Profiling</h1>

      <Card>
        <CardHeader><CardTitle>Profile a Table</CardTitle></CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">Connection</label>
              <Select value={connectionId} onValueChange={(v) => { setConnectionId(v); setTable(""); }}>
                <SelectTrigger><SelectValue placeholder="Select connection" /></SelectTrigger>
                <SelectContent>
                  {(connections ?? []).map((c: any) => (
                    <SelectItem key={c.id} value={c.id}>{c.name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Table</label>
              <Select value={table} onValueChange={setTable} disabled={!connectionId}>
                <SelectTrigger><SelectValue placeholder={connectionId ? "Select table" : "Select connection first"} /></SelectTrigger>
                <SelectContent>
                  {(streams ?? []).map((s: any) => (
                    <SelectItem key={s.name ?? s} value={s.name ?? s}>{s.name ?? s}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
          <Button
            onClick={() => profileMutation.mutate()}
            disabled={profileMutation.isPending || !connectionId || !table}
          >
            {profileMutation.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            {profileMutation.isPending ? "Profiling..." : "Run Profiling"}
          </Button>
        </CardContent>
      </Card>

      {profileMutation.isPending && (
        <Card>
          <CardContent className="flex items-center justify-center py-12">
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            <span className="ml-3 text-muted-foreground">Profiling in progress...</span>
          </CardContent>
        </Card>
      )}

      {results && !profileMutation.isPending && (
        <Card>
          <CardHeader><CardTitle>Column Statistics</CardTitle></CardHeader>
          <CardContent className="p-0">
            {Array.isArray(results) ? (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Column</TableHead>
                    <TableHead>Type</TableHead>
                    <TableHead>Nulls %</TableHead>
                    <TableHead>Distinct</TableHead>
                    <TableHead>Min</TableHead>
                    <TableHead>Max</TableHead>
                    <TableHead>Mean</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {results.map((col: any) => (
                    <TableRow key={col.name ?? col.column_name}>
                      <TableCell className="font-mono text-sm">{col.name ?? col.column_name}</TableCell>
                      <TableCell>{col.type ?? col.data_type ?? "\u2014"}</TableCell>
                      <TableCell>{col.null_percentage != null ? `${col.null_percentage.toFixed(1)}%` : "\u2014"}</TableCell>
                      <TableCell>{col.distinct_count?.toLocaleString() ?? "\u2014"}</TableCell>
                      <TableCell className="font-mono text-xs">{col.min != null ? String(col.min) : "\u2014"}</TableCell>
                      <TableCell className="font-mono text-xs">{col.max != null ? String(col.max) : "\u2014"}</TableCell>
                      <TableCell>{col.mean != null ? Number(col.mean).toFixed(2) : "\u2014"}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <div className="p-4">
                <pre className="rounded-md bg-muted p-4 text-sm overflow-auto max-h-96">
                  {JSON.stringify(results, null, 2)}
                </pre>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {profileMutation.isError && (
        <Card>
          <CardContent className="py-6 text-center text-destructive">
            Failed to profile table. Please check the connection and table name.
          </CardContent>
        </Card>
      )}
    </div>
  );
}
