import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Plus, ShieldCheck } from "lucide-react";

export function DQPoliciesPage() {
  const navigate = useNavigate();
  const [connectionFilter, setConnectionFilter] = useState("all");
  const [ruleTypeFilter, setRuleTypeFilter] = useState("all");
  const [statusFilter, setStatusFilter] = useState("all");

  const { data: policies, isLoading } = useQuery({
    queryKey: ["data-quality", "policies"],
    queryFn: () => fetchList("/data-quality/policies", "policies"),
  });

  const { data: connections } = useQuery({
    queryKey: ["connections"],
    queryFn: () => fetchList("/connections", "connections").catch(() => []),
  });

  const filtered = (policies ?? []).filter((p: any) => {
    if (connectionFilter !== "all" && p.connection_id !== connectionFilter) return false;
    if (ruleTypeFilter !== "all" && p.rule_type !== ruleTypeFilter) return false;
    if (statusFilter === "active" && !p.enabled) return false;
    if (statusFilter === "disabled" && p.enabled) return false;
    return true;
  });

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">DQ Policies</h1>
        <Button onClick={() => navigate("/data-quality/policies/new")}>
          <Plus className="mr-2 h-4 w-4" />Create Policy
        </Button>
      </div>

      <div className="flex gap-3">
        <Select value={connectionFilter} onValueChange={setConnectionFilter}>
          <SelectTrigger className="w-[180px]"><SelectValue placeholder="Connection" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Connections</SelectItem>
            {(connections ?? []).map((c: any) => (
              <SelectItem key={c.id} value={c.id}>{c.name}</SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={ruleTypeFilter} onValueChange={setRuleTypeFilter}>
          <SelectTrigger className="w-[180px]"><SelectValue placeholder="Rule Type" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Rule Types</SelectItem>
            <SelectItem value="null_check">Null Check</SelectItem>
            <SelectItem value="null_ratio_check">Null Ratio Check</SelectItem>
            <SelectItem value="range_check">Range Check</SelectItem>
            <SelectItem value="regex_check">Regex Check</SelectItem>
            <SelectItem value="freshness_check">Freshness Check</SelectItem>
            <SelectItem value="row_count_match">Row Count Match</SelectItem>
            <SelectItem value="enum_check">Enum Check</SelectItem>
            <SelectItem value="custom_sql">Custom SQL</SelectItem>
            <SelectItem value="uniqueness">Uniqueness</SelectItem>
            <SelectItem value="referential_integrity">Referential Integrity</SelectItem>
          </SelectContent>
        </Select>
        <Select value={statusFilter} onValueChange={setStatusFilter}>
          <SelectTrigger className="w-[150px]"><SelectValue placeholder="Status" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Status</SelectItem>
            <SelectItem value="active">Active</SelectItem>
            <SelectItem value="disabled">Disabled</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Connection</TableHead>
                <TableHead>Rule Type</TableHead>
                <TableHead>Severity</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Last Run</TableHead>
                <TableHead>Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow><TableCell colSpan={7} className="text-center">Loading...</TableCell></TableRow>
              ) : filtered.length === 0 ? (
                <TableRow><TableCell colSpan={7} className="text-center text-muted-foreground">No policies found</TableCell></TableRow>
              ) : (
                filtered.map((p: any) => (
                  <TableRow key={p.id} className="cursor-pointer" onClick={() => navigate(`/data-quality/policies/${p.id}`)}>
                    <TableCell className="font-medium">
                      <div className="flex items-center gap-2">
                        <ShieldCheck className="h-4 w-4 text-muted-foreground" />{p.name}
                      </div>
                    </TableCell>
                    <TableCell>{p.connection_name ?? "All"}</TableCell>
                    <TableCell className="font-mono text-xs">{p.rule_type ?? p.rules?.[0]?.type ?? "\u2014"}</TableCell>
                    <TableCell>
                      <Badge variant={p.severity === "critical" ? "destructive" : p.severity === "error" ? "destructive" : "secondary"}>
                        {p.severity ?? "warning"}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <Badge variant={p.enabled ? "default" : "secondary"}>
                        {p.enabled ? "Active" : "Disabled"}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-muted-foreground">
                      {p.last_run ? new Date(p.last_run).toLocaleString() : "Never"}
                    </TableCell>
                    <TableCell>
                      <Button variant="ghost" size="sm" onClick={(e) => { e.stopPropagation(); navigate(`/data-quality/policies/${p.id}`); }}>
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
