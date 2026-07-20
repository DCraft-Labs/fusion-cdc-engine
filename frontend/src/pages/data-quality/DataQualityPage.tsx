import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { ShieldCheck, AlertTriangle, CheckCircle2, Activity } from "lucide-react";

export function DataQualityPage() {
  const navigate = useNavigate();

  const { data: summary } = useQuery({
    queryKey: ["data-quality", "summary"],
    queryFn: () => api.get("/data-quality/metrics/dashboard").then((r) => r.data).catch(() => ({})),
  });

  const { data: connectionScores, isLoading } = useQuery({
    queryKey: ["data-quality", "connections-scores"],
    queryFn: () => fetchList("/data-quality/scores/by-connection").catch(() => []),
  });

  const overallScore = summary?.overall_score ?? 0;
  const activePolicies = summary?.active_policies ?? 0;
  const openViolations = summary?.open_violations ?? 0;
  const rulesPassing = summary?.rules_passing ?? 0;
  const rulesTotal = summary?.rules_total ?? 0;

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Data Quality</h1>

      <div className="grid gap-4 md:grid-cols-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium">Overall Score</CardTitle>
            <Activity className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{overallScore}%</div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium">Active Policies</CardTitle>
            <ShieldCheck className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{activePolicies}</div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium">Open Violations</CardTitle>
            <AlertTriangle className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{openViolations}</div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium">Rules Passing</CardTitle>
            <CheckCircle2 className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{rulesPassing}/{rulesTotal}</div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Quality by Connection</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Connection</TableHead>
                <TableHead>Score</TableHead>
                <TableHead>Violations</TableHead>
                <TableHead>Last Check</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow><TableCell colSpan={4} className="text-center">Loading...</TableCell></TableRow>
              ) : (connectionScores ?? []).length === 0 ? (
                <TableRow><TableCell colSpan={4} className="text-center text-muted-foreground">No data available</TableCell></TableRow>
              ) : (
                (connectionScores ?? []).map((c: any) => (
                  <TableRow key={c.connection_id}>
                    <TableCell className="font-medium">{c.connection_name}</TableCell>
                    <TableCell>{c.score}%</TableCell>
                    <TableCell>{c.violations_count}</TableCell>
                    <TableCell className="text-muted-foreground">
                      {c.last_check ? new Date(c.last_check).toLocaleString() : "Never"}
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <div className="flex gap-3">
        <Button onClick={() => navigate("/data-quality/policies")}>View Policies</Button>
        <Button variant="outline" onClick={() => navigate("/data-quality/violations")}>View Violations</Button>
        <Button variant="outline" onClick={() => navigate("/data-quality/profiling")}>Data Profiling</Button>
      </div>
    </div>
  );
}
