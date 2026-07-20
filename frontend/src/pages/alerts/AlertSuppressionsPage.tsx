import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Plus } from "lucide-react";

export function AlertSuppressionsPage() {
  const queryClient = useQueryClient();
  const [showCreate, setShowCreate] = useState(false);
  const [form, setForm] = useState({
    name: "",
    scope_type: "all",
    scope_id: "",
    severities: [] as string[],
    window_type: "one_time",
    start_at: "",
    end_at: "",
    recurring_days: [] as string[],
    recurring_start_time: "",
    recurring_end_time: "",
    reason: "",
  });

  const { data: suppressions = [], isLoading } = useQuery({
    queryKey: ["alerts", "suppressions"],
    queryFn: () => fetchList("/alerts/suppressions", "suppressions"),
  });

  const { data: rules = [] } = useQuery({
    queryKey: ["alerts", "rules"],
    queryFn: () => fetchList("/alerts/rules", "rules").catch(() => []),
  });

  const { data: connections = [] } = useQuery({
    queryKey: ["connections"],
    queryFn: () => fetchList("/connections", "connections").catch(() => []),
  });

  const createMutation = useMutation({
    mutationFn: () => api.post("/alerts/suppressions", {
      name: form.name,
      scope_type: form.scope_type,
      scope_id: form.scope_type !== "all" ? form.scope_id : undefined,
      severities: form.severities.length > 0 ? form.severities : undefined,
      window_type: form.window_type,
      start_at: form.start_at || undefined,
      end_at: form.end_at || undefined,
      recurring_days: form.window_type === "recurring" ? form.recurring_days : undefined,
      recurring_start_time: form.window_type === "recurring" ? form.recurring_start_time : undefined,
      recurring_end_time: form.window_type === "recurring" ? form.recurring_end_time : undefined,
      reason: form.reason || undefined,
    }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["alerts", "suppressions"] });
      setShowCreate(false);
      setForm({ name: "", scope_type: "all", scope_id: "", severities: [], window_type: "one_time", start_at: "", end_at: "", recurring_days: [], recurring_start_time: "", recurring_end_time: "", reason: "" });
    },
  });

  function toggleSeverity(sev: string) {
    setForm((prev) => ({
      ...prev,
      severities: prev.severities.includes(sev) ? prev.severities.filter((s) => s !== sev) : [...prev.severities, sev],
    }));
  }

  function toggleDay(day: string) {
    setForm((prev) => ({
      ...prev,
      recurring_days: prev.recurring_days.includes(day) ? prev.recurring_days.filter((d) => d !== day) : [...prev.recurring_days, day],
    }));
  }

  function getStatus(suppression: any): { label: string; variant: string } {
    const now = new Date();
    if (suppression.end_at && new Date(suppression.end_at) < now) return { label: "Expired", variant: "secondary" };
    if (suppression.start_at && new Date(suppression.start_at) > now) return { label: "Scheduled", variant: "outline" };
    return { label: "Active", variant: "success" };
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Alert Suppressions</h1>
          <p className="text-sm text-muted-foreground mt-1">
            Suppressions temporarily silence alerts during maintenance windows or known incidents.
          </p>
        </div>
        <Dialog open={showCreate} onOpenChange={setShowCreate}>
          <DialogTrigger asChild>
            <Button><Plus className="mr-2 h-4 w-4" />Create Suppression</Button>
          </DialogTrigger>
          <DialogContent className="max-w-lg max-h-[85vh] overflow-y-auto">
            <DialogHeader><DialogTitle>Create Suppression</DialogTitle></DialogHeader>
            <div className="space-y-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Name *</label>
                <Input value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} placeholder="e.g. Weekend Maintenance" />
              </div>

              <div className="space-y-2">
                <label className="text-sm font-medium">Scope</label>
                <Select value={form.scope_type} onValueChange={(v) => setForm({ ...form, scope_type: v })}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All alerts</SelectItem>
                    <SelectItem value="connection">Specific connection</SelectItem>
                    <SelectItem value="rule">Specific rule</SelectItem>
                  </SelectContent>
                </Select>
                {form.scope_type === "connection" && (
                  <Select value={form.scope_id} onValueChange={(v) => setForm({ ...form, scope_id: v })}>
                    <SelectTrigger className="mt-2"><SelectValue placeholder="Select connection..." /></SelectTrigger>
                    <SelectContent>
                      {connections.map((c: any) => <SelectItem key={c.id} value={c.id}>{c.name}</SelectItem>)}
                    </SelectContent>
                  </Select>
                )}
                {form.scope_type === "rule" && (
                  <Select value={form.scope_id} onValueChange={(v) => setForm({ ...form, scope_id: v })}>
                    <SelectTrigger className="mt-2"><SelectValue placeholder="Select rule..." /></SelectTrigger>
                    <SelectContent>
                      {rules.map((r: any) => <SelectItem key={r.id} value={r.id}>{r.name}</SelectItem>)}
                    </SelectContent>
                  </Select>
                )}
              </div>

              <div className="space-y-2">
                <label className="text-sm font-medium">Suppress Severities</label>
                <div className="flex gap-3">
                  {["info", "warning", "error", "critical"].map((sev) => (
                    <label key={sev} className="flex items-center gap-1.5 cursor-pointer">
                      <input type="checkbox" checked={form.severities.includes(sev)} onChange={() => toggleSeverity(sev)} className="accent-primary" />
                      <span className="text-sm capitalize">{sev}</span>
                    </label>
                  ))}
                </div>
                <p className="text-xs text-muted-foreground">Leave unchecked to suppress all severities</p>
              </div>

              <div className="space-y-2">
                <label className="text-sm font-medium">Time Window</label>
                <div className="flex gap-4">
                  <label className="flex items-center gap-1.5 cursor-pointer">
                    <input type="radio" name="window_type" checked={form.window_type === "one_time"} onChange={() => setForm({ ...form, window_type: "one_time" })} className="accent-primary" />
                    <span className="text-sm">One-time</span>
                  </label>
                  <label className="flex items-center gap-1.5 cursor-pointer">
                    <input type="radio" name="window_type" checked={form.window_type === "recurring"} onChange={() => setForm({ ...form, window_type: "recurring" })} className="accent-primary" />
                    <span className="text-sm">Recurring</span>
                  </label>
                </div>
              </div>

              {form.window_type === "one_time" && (
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Start</label>
                    <Input type="datetime-local" value={form.start_at} onChange={(e) => setForm({ ...form, start_at: e.target.value })} />
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">End</label>
                    <Input type="datetime-local" value={form.end_at} onChange={(e) => setForm({ ...form, end_at: e.target.value })} />
                  </div>
                </div>
              )}

              {form.window_type === "recurring" && (
                <div className="space-y-3">
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Days</label>
                    <div className="flex flex-wrap gap-2">
                      {["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"].map((day) => (
                        <label key={day} className="flex items-center gap-1 cursor-pointer">
                          <input type="checkbox" checked={form.recurring_days.includes(day)} onChange={() => toggleDay(day)} className="accent-primary" />
                          <span className="text-xs">{day}</span>
                        </label>
                      ))}
                    </div>
                  </div>
                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <label className="text-sm font-medium">Start Time</label>
                      <Input type="time" value={form.recurring_start_time} onChange={(e) => setForm({ ...form, recurring_start_time: e.target.value })} />
                    </div>
                    <div className="space-y-2">
                      <label className="text-sm font-medium">End Time</label>
                      <Input type="time" value={form.recurring_end_time} onChange={(e) => setForm({ ...form, recurring_end_time: e.target.value })} />
                    </div>
                  </div>
                </div>
              )}

              <div className="space-y-2">
                <label className="text-sm font-medium">Reason</label>
                <textarea
                  className="flex min-h-[80px] w-full rounded-md border border-input bg-background px-3 py-2 text-sm placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  value={form.reason}
                  onChange={(e) => setForm({ ...form, reason: e.target.value })}
                  placeholder="Reason for suppression..."
                />
              </div>

              <div className="flex gap-2 justify-end">
                <Button variant="outline" onClick={() => setShowCreate(false)}>Cancel</Button>
                <Button onClick={() => createMutation.mutate()} disabled={createMutation.isPending || !form.name}>
                  {createMutation.isPending ? "Creating..." : "Create Suppression"}
                </Button>
              </div>
            </div>
          </DialogContent>
        </Dialog>
      </div>

      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Scope</TableHead>
                <TableHead>Window</TableHead>
                <TableHead>Status</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow><TableCell colSpan={4} className="text-center py-8">Loading...</TableCell></TableRow>
              ) : suppressions.length === 0 ? (
                <TableRow><TableCell colSpan={4} className="text-center text-muted-foreground py-8">No suppressions configured</TableCell></TableRow>
              ) : (
                suppressions.map((s: any) => {
                  const status = getStatus(s);
                  return (
                    <TableRow key={s.id}>
                      <TableCell className="font-medium">{s.name ?? s.id}</TableCell>
                      <TableCell className="text-muted-foreground">
                        {s.scope_type === "all" ? "All alerts" : s.scope_type === "connection" ? `Connection: ${s.scope_name ?? s.scope_id}` : `Rule: ${s.scope_name ?? s.scope_id}`}
                      </TableCell>
                      <TableCell className="text-muted-foreground text-sm">
                        {s.window_type === "recurring"
                          ? `${(s.recurring_days ?? []).join(", ")} ${s.recurring_start_time ?? ""}–${s.recurring_end_time ?? ""}`
                          : s.start_at && s.end_at
                            ? `${new Date(s.start_at).toLocaleDateString()} – ${new Date(s.end_at).toLocaleDateString()}`
                            : s.end_at ? `Until ${new Date(s.end_at).toLocaleDateString()}` : "Indefinite"
                        }
                      </TableCell>
                      <TableCell>
                        <Badge variant={status.variant as any}>{status.label}</Badge>
                      </TableCell>
                    </TableRow>
                  );
                })
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
