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
import { Plus, AlertTriangle, CheckCircle2, Clock, Info } from "lucide-react";

interface MaintenanceWindow {
  id: string;
  name: string;
  start_time: string;
  end_time: string;
  scope: string;
  recurring?: boolean;
  status?: string;
}

function getWindowStatus(w: MaintenanceWindow): { label: string; variant: "default" | "secondary" | "destructive" | "outline" } {
  const now = new Date();
  const start = new Date(w.start_time);
  const end = new Date(w.end_time);
  if (now >= start && now <= end) return { label: "Active", variant: "destructive" };
  if (now > end) return { label: "Completed", variant: "secondary" };
  return { label: "Scheduled", variant: "outline" };
}

export function MaintenanceWindowsPage() {
  const queryClient = useQueryClient();
  const [showCreate, setShowCreate] = useState(false);
  const [form, setForm] = useState({
    name: "",
    start_time: "",
    end_time: "",
    scope: "all",
    recurring: false,
  });

  const { data: windows, isLoading } = useQuery({
    queryKey: ["settings", "maintenance-windows"],
    queryFn: () => fetchList("/settings/maintenance-windows").catch(() => []),
  });

  const windowList: MaintenanceWindow[] = Array.isArray(windows) ? windows : [];

  const activeWindows = windowList.filter((w) => {
    const now = new Date();
    return new Date(w.start_time) <= now && new Date(w.end_time) >= now;
  });

  const upcomingWindows = windowList.filter((w) => new Date(w.start_time) > new Date());

  const createMutation = useMutation({
    mutationFn: () => api.post("/settings/maintenance-windows", form),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["settings", "maintenance-windows"] });
      setShowCreate(false);
      setForm({ name: "", start_time: "", end_time: "", scope: "all", recurring: false });
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (id: string) => api.delete(`/settings/maintenance-windows/${id}`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["settings", "maintenance-windows"] }),
  });

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Maintenance Windows</h1>
          <p className="text-muted-foreground">Schedule and manage platform maintenance periods (superadmin only)</p>
        </div>
        <Button onClick={() => setShowCreate(true)}>
          <Plus className="mr-2 h-4 w-4" />Schedule Maintenance
        </Button>
      </div>

      {/* Status Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Card>
          <CardContent className="flex items-center gap-4 p-4">
            {activeWindows.length > 0 ? (
              <>
                <AlertTriangle className="h-8 w-8 text-destructive" />
                <div>
                  <p className="font-semibold text-destructive">Maintenance Active</p>
                  <p className="text-sm text-muted-foreground">{activeWindows.length} window(s) currently active</p>
                </div>
              </>
            ) : (
              <>
                <CheckCircle2 className="h-8 w-8 text-green-500" />
                <div>
                  <p className="font-semibold text-green-600">No Active Maintenance</p>
                  <p className="text-sm text-muted-foreground">System operating normally</p>
                </div>
              </>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardContent className="flex items-center gap-4 p-4">
            <Clock className="h-8 w-8 text-muted-foreground" />
            <div>
              <p className="font-semibold">{upcomingWindows.length} Upcoming</p>
              <p className="text-sm text-muted-foreground">Scheduled maintenance windows</p>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Windows Table */}
      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Window Name</TableHead>
                <TableHead>Time</TableHead>
                <TableHead>Scope</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow><TableCell colSpan={5} className="text-center py-8">Loading...</TableCell></TableRow>
              ) : windowList.length === 0 ? (
                <TableRow><TableCell colSpan={5} className="text-center py-8 text-muted-foreground">No maintenance windows scheduled</TableCell></TableRow>
              ) : (
                windowList.map((w) => {
                  const status = getWindowStatus(w);
                  return (
                    <TableRow key={w.id}>
                      <TableCell className="font-medium">{w.name}</TableCell>
                      <TableCell className="text-sm">
                        <div>{new Date(w.start_time).toLocaleString()}</div>
                        <div className="text-muted-foreground">→ {new Date(w.end_time).toLocaleString()}</div>
                      </TableCell>
                      <TableCell>
                        <Badge variant="outline">{w.scope === "all" ? "All Connections" : w.scope}</Badge>
                      </TableCell>
                      <TableCell>
                        <Badge variant={status.variant}>{status.label}</Badge>
                      </TableCell>
                      <TableCell className="text-right">
                        {status.label !== "Completed" && (
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => deleteMutation.mutate(w.id)}
                            className="text-destructive"
                          >
                            Cancel
                          </Button>
                        )}
                      </TableCell>
                    </TableRow>
                  );
                })
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* Info Section */}
      <Card className="border-blue-200 bg-blue-50/50">
        <CardContent className="flex items-start gap-3 p-4">
          <Info className="h-5 w-5 text-blue-600 mt-0.5" />
          <div className="text-sm">
            <p className="font-medium text-blue-900">During maintenance windows:</p>
            <ul className="mt-1 space-y-1 text-blue-800 list-disc list-inside">
              <li>CDC pipelines are automatically paused</li>
              <li>Alerts are suppressed to avoid false positives</li>
              <li>UI displays a maintenance banner to all users</li>
              <li>Pipelines resume automatically when maintenance ends</li>
            </ul>
          </div>
        </CardContent>
      </Card>

      {/* Create Dialog */}
      <Dialog open={showCreate} onOpenChange={setShowCreate}>
        <DialogContent>
          <DialogHeader><DialogTitle>Schedule Maintenance Window</DialogTitle></DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">Name</label>
              <Input
                value={form.name}
                onChange={(e) => setForm({ ...form, name: e.target.value })}
                placeholder="e.g., Database Migration"
              />
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Start Time</label>
                <Input
                  value={form.start_time}
                  onChange={(e) => setForm({ ...form, start_time: e.target.value })}
                  type="datetime-local"
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">End Time</label>
                <Input
                  value={form.end_time}
                  onChange={(e) => setForm({ ...form, end_time: e.target.value })}
                  type="datetime-local"
                />
              </div>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Scope</label>
              <Select value={form.scope} onValueChange={(v) => setForm({ ...form, scope: v })}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Connections</SelectItem>
                  <SelectItem value="specific">Specific Connection</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <label className="flex items-center gap-3">
              <input
                type="checkbox"
                className="h-4 w-4 rounded border-gray-300"
                checked={form.recurring}
                onChange={(e) => setForm({ ...form, recurring: e.target.checked })}
              />
              <span className="text-sm">Recurring (repeat weekly)</span>
            </label>
            <Button
              onClick={() => createMutation.mutate()}
              disabled={createMutation.isPending || !form.name || !form.start_time || !form.end_time}
              className="w-full"
            >
              {createMutation.isPending ? "Scheduling..." : "Schedule Maintenance"}
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
}
