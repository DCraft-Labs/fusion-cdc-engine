import { useQuery } from "@tanstack/react-query";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Pencil, History } from "lucide-react";

interface TenantUsage {
  tenant: string;
  events_used: number;
  events_limit: number;
  storage_used_gb: number;
  storage_limit_gb: number;
  workers_used: number;
  workers_limit: number;
}

interface QuotaViolation {
  tenant: string;
  resource: string;
  limit: number;
  actual: number;
  timestamp: string;
}

function getQuotaStatus(used: number, limit: number): { label: string; variant: "default" | "secondary" | "destructive" | "outline" } {
  if (limit === 0) return { label: "No Limit", variant: "outline" };
  const ratio = used / limit;
  if (ratio >= 1) return { label: "Exceeded", variant: "destructive" };
  if (ratio >= 0.8) return { label: "Near Limit", variant: "default" };
  return { label: "Within", variant: "secondary" };
}

function formatUsage(used: number, limit: number, unit?: string): string {
  const u = unit ? `${used.toLocaleString()} ${unit}` : used.toLocaleString();
  const l = limit > 0 ? (unit ? `${limit.toLocaleString()} ${unit}` : limit.toLocaleString()) : "∞";
  return `${u} / ${l}`;
}

export function ResourceQuotasPage() {
  const { data: quotaData, isLoading } = useQuery({
    queryKey: ["settings", "resource-quotas"],
    queryFn: () => fetchList("/settings/resource-quotas").catch(() => []),
  });

  const { data: violationsData, isLoading: violationsLoading } = useQuery({
    queryKey: ["settings", "quota-violations"],
    queryFn: () => fetchList("/settings/quota-violations").catch(() => []),
  });

  const tenants: TenantUsage[] = Array.isArray(quotaData) ? quotaData : (quotaData as any)?.tenants ?? [];
  const violations: QuotaViolation[] = Array.isArray(violationsData) ? violationsData : (violationsData as any)?.items ?? [];

  const getWorstStatus = (t: TenantUsage) => {
    const statuses = [
      getQuotaStatus(t.events_used, t.events_limit),
      getQuotaStatus(t.storage_used_gb, t.storage_limit_gb),
      getQuotaStatus(t.workers_used, t.workers_limit),
    ];
    if (statuses.some((s) => s.label === "Exceeded")) return getQuotaStatus(1, 0.5);
    if (statuses.some((s) => s.label === "Near Limit")) return { label: "Near Limit", variant: "default" as const };
    return { label: "Within", variant: "secondary" as const };
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Resource Quotas</h1>
          <p className="text-muted-foreground">Monitor and manage tenant resource usage and limits (superadmin only)</p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline">
            <Pencil className="mr-2 h-4 w-4" />Edit Quotas
          </Button>
          <Button variant="outline">
            <History className="mr-2 h-4 w-4" />View History
          </Button>
        </div>
      </div>

      <Tabs defaultValue="usage">
        <TabsList>
          <TabsTrigger value="usage">Tenant Usage</TabsTrigger>
          <TabsTrigger value="violations">
            Quota Violations
            {violations.length > 0 && (
              <Badge variant="destructive" className="ml-2 h-5 px-1.5 text-xs">{violations.length}</Badge>
            )}
          </TabsTrigger>
        </TabsList>

        <TabsContent value="usage" className="mt-4">
          <Card>
            <CardContent className="p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Tenant</TableHead>
                    <TableHead>Events (used / limit)</TableHead>
                    <TableHead>Storage (used / limit)</TableHead>
                    <TableHead>Workers (used / limit)</TableHead>
                    <TableHead>Quota Status</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {isLoading ? (
                    <TableRow><TableCell colSpan={5} className="text-center py-8">Loading quotas...</TableCell></TableRow>
                  ) : tenants.length === 0 ? (
                    <TableRow><TableCell colSpan={5} className="text-center py-8 text-muted-foreground">No tenant quota data</TableCell></TableRow>
                  ) : (
                    tenants.map((t) => {
                      const overallStatus = getWorstStatus(t);
                      return (
                        <TableRow key={t.tenant}>
                          <TableCell className="font-medium">{t.tenant}</TableCell>
                          <TableCell>
                            <div className="space-y-1">
                              <span className="text-sm">{formatUsage(t.events_used, t.events_limit)}</span>
                              <div className="w-24 h-1.5 rounded-full bg-muted overflow-hidden">
                                <div
                                  className={`h-full rounded-full ${t.events_limit > 0 && t.events_used / t.events_limit >= 0.8 ? "bg-red-500" : "bg-primary"}`}
                                  style={{ width: `${Math.min(100, t.events_limit > 0 ? (t.events_used / t.events_limit) * 100 : 0)}%` }}
                                />
                              </div>
                            </div>
                          </TableCell>
                          <TableCell>
                            <div className="space-y-1">
                              <span className="text-sm">{formatUsage(t.storage_used_gb, t.storage_limit_gb, "GB")}</span>
                              <div className="w-24 h-1.5 rounded-full bg-muted overflow-hidden">
                                <div
                                  className={`h-full rounded-full ${t.storage_limit_gb > 0 && t.storage_used_gb / t.storage_limit_gb >= 0.8 ? "bg-red-500" : "bg-primary"}`}
                                  style={{ width: `${Math.min(100, t.storage_limit_gb > 0 ? (t.storage_used_gb / t.storage_limit_gb) * 100 : 0)}%` }}
                                />
                              </div>
                            </div>
                          </TableCell>
                          <TableCell>
                            <div className="space-y-1">
                              <span className="text-sm">{formatUsage(t.workers_used, t.workers_limit)}</span>
                              <div className="w-24 h-1.5 rounded-full bg-muted overflow-hidden">
                                <div
                                  className={`h-full rounded-full ${t.workers_limit > 0 && t.workers_used / t.workers_limit >= 0.8 ? "bg-red-500" : "bg-primary"}`}
                                  style={{ width: `${Math.min(100, t.workers_limit > 0 ? (t.workers_used / t.workers_limit) * 100 : 0)}%` }}
                                />
                              </div>
                            </div>
                          </TableCell>
                          <TableCell>
                            <Badge variant={overallStatus.variant}>{overallStatus.label}</Badge>
                          </TableCell>
                        </TableRow>
                      );
                    })
                  )}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="violations" className="mt-4">
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Quota Violations (Last 7 Days)</CardTitle>
            </CardHeader>
            <CardContent className="p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Tenant</TableHead>
                    <TableHead>Resource</TableHead>
                    <TableHead>Limit</TableHead>
                    <TableHead>Actual</TableHead>
                    <TableHead>When</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {violationsLoading ? (
                    <TableRow><TableCell colSpan={5} className="text-center py-8">Loading violations...</TableCell></TableRow>
                  ) : violations.length === 0 ? (
                    <TableRow><TableCell colSpan={5} className="text-center py-8 text-muted-foreground">No quota violations in the last 7 days</TableCell></TableRow>
                  ) : (
                    violations.map((v, i) => (
                      <TableRow key={i}>
                        <TableCell className="font-medium">{v.tenant}</TableCell>
                        <TableCell>{v.resource}</TableCell>
                        <TableCell className="font-mono text-sm">{v.limit.toLocaleString()}</TableCell>
                        <TableCell className="font-mono text-sm text-destructive">{v.actual.toLocaleString()}</TableCell>
                        <TableCell className="text-sm text-muted-foreground">
                          {new Date(v.timestamp).toLocaleString()}
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
