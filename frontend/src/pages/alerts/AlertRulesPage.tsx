import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { Plus } from "lucide-react";

export function AlertRulesPage() {
  const navigate = useNavigate();

  const { data: rules = [], isLoading } = useQuery({
    queryKey: ["alerts", "rules"],
    queryFn: () => fetchList("/alerts/rules", "rules"),
  });

  function severityBadge(severity: string) {
    const variant = severity === "critical" ? "destructive" : severity === "error" ? "warning" : severity === "warning" ? "warning" : "secondary";
    return <Badge variant={variant}>{severity}</Badge>;
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Alert Rules</h1>
        <Button onClick={() => navigate("/alerts/rules/new")}>
          <Plus className="mr-2 h-4 w-4" />Create Rule
        </Button>
      </div>

      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Rule Name</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Severity</TableHead>
                <TableHead>Scope</TableHead>
                <TableHead>Status</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow><TableCell colSpan={5} className="text-center py-8">Loading...</TableCell></TableRow>
              ) : rules.length === 0 ? (
                <TableRow><TableCell colSpan={5} className="text-center text-muted-foreground py-8">No alert rules configured</TableCell></TableRow>
              ) : (
                rules.map((rule: any) => (
                  <TableRow
                    key={rule.id}
                    className="cursor-pointer hover:bg-muted/50"
                    onClick={() => navigate(`/alerts/rules/${rule.id}`)}
                  >
                    <TableCell className="font-medium">{rule.name}</TableCell>
                    <TableCell className="text-muted-foreground">{rule.alert_type ?? rule.metric ?? "—"}</TableCell>
                    <TableCell>{severityBadge(rule.severity)}</TableCell>
                    <TableCell className="text-muted-foreground">{rule.scope ?? "All connections"}</TableCell>
                    <TableCell>
                      <Badge variant={rule.enabled ? "success" : "secondary"}>
                        {rule.enabled ? "Active" : "Muted"}
                      </Badge>
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
