import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Plus } from "lucide-react";

interface FeatureFlag {
  id?: string;
  name: string;
  key?: string;
  scope: "global" | "tenant";
  enabled: boolean;
  rollout_percentage: number;
  description?: string;
}

export function FeatureFlagsPage() {
  const queryClient = useQueryClient();
  const [showCreate, setShowCreate] = useState(false);
  const [form, setForm] = useState({
    name: "",
    scope: "global" as "global" | "tenant",
    rollout_percentage: 100,
    description: "",
  });

  const { data: flags, isLoading } = useQuery({
    queryKey: ["settings", "feature-flags"],
    queryFn: () => fetchList("/settings/feature-flags").catch(() => []),
  });

  const flagList: FeatureFlag[] = Array.isArray(flags) ? flags : [];

  const toggleMutation = useMutation({
    mutationFn: (flag: FeatureFlag) =>
      api.patch(`/settings/feature-flags/${flag.key ?? flag.id ?? flag.name}`, { enabled: !flag.enabled }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["settings", "feature-flags"] }),
  });

  const createMutation = useMutation({
    mutationFn: () => api.post("/settings/feature-flags", form),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["settings", "feature-flags"] });
      setShowCreate(false);
      setForm({ name: "", scope: "global", rollout_percentage: 100, description: "" });
    },
  });

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Feature Flags</h1>
          <p className="text-muted-foreground">Control feature rollouts and toggle capabilities (superadmin only)</p>
        </div>
        <Button onClick={() => setShowCreate(true)}>
          <Plus className="mr-2 h-4 w-4" />New Flag
        </Button>
      </div>

      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Flag Name</TableHead>
                <TableHead>Scope</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Rollout %</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow><TableCell colSpan={5} className="text-center py-8">Loading feature flags...</TableCell></TableRow>
              ) : flagList.length === 0 ? (
                <TableRow><TableCell colSpan={5} className="text-center py-8 text-muted-foreground">No feature flags configured</TableCell></TableRow>
              ) : (
                flagList.map((f) => (
                  <TableRow key={f.key ?? f.id ?? f.name}>
                    <TableCell>
                      <div>
                        <p className="font-mono font-medium text-sm">{f.key ?? f.name}</p>
                        {f.description && <p className="text-xs text-muted-foreground">{f.description}</p>}
                      </div>
                    </TableCell>
                    <TableCell>
                      <Badge variant={f.scope === "global" ? "default" : "secondary"}>
                        {f.scope === "global" ? "Global" : "Tenant"}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      {/* Toggle switch visual */}
                      <button
                        className="relative inline-flex items-center"
                        onClick={() => toggleMutation.mutate(f)}
                        disabled={toggleMutation.isPending}
                      >
                        <div className={`w-10 h-5 rounded-full transition-colors ${f.enabled ? "bg-green-500" : "bg-gray-300"}`}>
                          <div
                            className={`absolute top-0.5 h-4 w-4 rounded-full bg-white shadow transition-transform ${f.enabled ? "translate-x-5" : "translate-x-0.5"}`}
                          />
                        </div>
                        <span className={`ml-2 text-xs font-medium ${f.enabled ? "text-green-600" : "text-muted-foreground"}`}>
                          {f.enabled ? "ON" : "OFF"}
                        </span>
                      </button>
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center gap-2">
                        <div className="w-16 h-2 rounded-full bg-muted overflow-hidden">
                          <div
                            className="h-full bg-primary rounded-full"
                            style={{ width: `${f.rollout_percentage ?? 100}%` }}
                          />
                        </div>
                        <span className="text-xs text-muted-foreground">{f.rollout_percentage ?? 100}%</span>
                      </div>
                    </TableCell>
                    <TableCell className="text-right">
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => toggleMutation.mutate(f)}
                        disabled={toggleMutation.isPending}
                      >
                        {f.enabled ? "Disable" : "Enable"}
                      </Button>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* Create Flag Dialog */}
      <Dialog open={showCreate} onOpenChange={setShowCreate}>
        <DialogContent>
          <DialogHeader><DialogTitle>Create Feature Flag</DialogTitle></DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">Name</label>
              <Input
                value={form.name}
                onChange={(e) => setForm({ ...form, name: e.target.value })}
                placeholder="e.g., enable_new_dashboard"
                className="font-mono"
              />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Scope</label>
              <Select value={form.scope} onValueChange={(v: "global" | "tenant") => setForm({ ...form, scope: v })}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="global">Global</SelectItem>
                  <SelectItem value="tenant">Tenant</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Rollout Percentage</label>
              <div className="flex items-center gap-3">
                <Input
                  value={form.rollout_percentage}
                  onChange={(e) => setForm({ ...form, rollout_percentage: Math.min(100, Math.max(0, parseInt(e.target.value) || 0)) })}
                  type="number"
                  min={0}
                  max={100}
                  className="w-24"
                />
                <span className="text-sm text-muted-foreground">%</span>
              </div>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Description</label>
              <Input
                value={form.description}
                onChange={(e) => setForm({ ...form, description: e.target.value })}
                placeholder="What does this flag control?"
              />
            </div>
            <Button
              onClick={() => createMutation.mutate()}
              disabled={createMutation.isPending || !form.name}
              className="w-full"
            >
              {createMutation.isPending ? "Creating..." : "Create Flag"}
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
}
