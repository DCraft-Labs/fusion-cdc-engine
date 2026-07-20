import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api, fetchList } from "@/lib/api";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";

export function DQViolationsPage() {
  const navigate = useNavigate();
  const [statusFilter, setStatusFilter] = useState("all");
  const [connectionFilter, setConnectionFilter] = useState("all");
  const [policyFilter, setPolicyFilter] = useState("all");

  const { data: violations, isLoading } = useQuery({
    queryKey: ["data-quality", "violations"],
    queryFn: () => fetchList("/data-quality/violations", "violations"),
  });

  const { data: connections } = useQuery({
    queryKey: ["connections"],
    queryFn: () => fetchList("/connections", "connections").catch(() => []),
  });

  const { data: policies } = useQuery({
    queryKey: ["data-quality", "policies"],
    queryFn: () => fetchList("/data-quality/policies", "policies").catch(() => []),
  });

  const filtered = (violations ?? []).filter((v: any) => {
    if (statusFilter !== "all" && v.status !== statusFilter) return false;
    if (connectionFilter !== "all" && v.connection_id !== connectionFilter) return false;
    if (policyFilter !== "all" && v.policy_id !== policyFilter) return false;
    return true;
  });

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">DQ Violations</h1>

      <div className="flex gap-3">
        <Select value={statusFilter} onValueChange={setStatusFilter}>
          <SelectTrigger className="w-[150px]"><SelectValue placeholder="Status" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Status</SelectItem>
            <SelectItem value="active">Active</SelectItem>
            <SelectItem value="resolved">Resolved</SelectItem>
            <SelectItem value="ignored">Ignored</SelectItem>
          </SelectContent>
        </Select>
        <Select value={connectionFilter} onValueChange={setConnectionFilter}>
          <SelectTrigger className="w-[180px]"><SelectValue placeholder="Connection" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Connections</SelectItem>
            {(connections ?? []).map((c: any) => (
              <SelectItem key={c.id} value={c.id}>{c.name}</SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={policyFilter} onValueChange={setPolicyFilter}>
          <SelectTrigger className="w-[180px]"><SelectValue placeholder="Policy" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Policies</SelectItem>
            {(policies ?? []).map((p: any) => (
              <SelectItem key={p.id} value={p.id}>{p.name}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Violation ID</TableHead>
                <TableHead>Policy</TableHead>
                <TableHead>Connection</TableHead>
                <TableHead>Detected At</TableHead>
                <TableHead>Count</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow><TableCell colSpan={7} className="text-center">Loading...</TableCell></TableRow>
              ) : filtered.length === 0 ? (
                <TableRow><TableCell colSpan={7} className="text-center text-muted-foreground">No violations found</TableCell></TableRow>
              ) : (
                filtered.map((v: any) => (
                  <TableRow key={v.id} className="cursor-pointer" onClick={() => navigate(`/data-quality/violations/${v.id}`)}>
                    <TableCell className="font-mono text-xs">{v.id?.slice(0, 8) ?? "\u2014"}</TableCell>
                    <TableCell className="font-medium">{v.policy_name ?? "\u2014"}</TableCell>
                    <TableCell>{v.connection_name ?? "\u2014"}</TableCell>
                    <TableCell className="text-muted-foreground">{new Date(v.detected_at).toLocaleString()}</TableCell>
                    <TableCell>{v.count ?? 1}</TableCell>
                    <TableCell>
                      <Badge variant={v.status === "resolved" ? "default" : v.status === "ignored" ? "secondary" : "destructive"}>
                        {v.status}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <Button variant="ghost" size="sm" onClick={(e) => { e.stopPropagation(); navigate(`/data-quality/violations/${v.id}`); }}>
                        View
                      </Button>
                    </TableCell>
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
