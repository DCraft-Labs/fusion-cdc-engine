import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useMutation, useQuery } from "@tanstack/react-query";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";

const ALERT_TYPES = [
  { value: "cdc_lag", label: "CDC Lag" },
  { value: "worker_health", label: "Worker Health" },
  { value: "dq_violation", label: "DQ Violation" },
  { value: "error_rate", label: "Error Rate" },
  { value: "throughput_drop", label: "Throughput Drop" },
  { value: "schema_change", label: "Schema Change" },
  { value: "connection_status", label: "Connection Status" },
];

const METRICS = [
  { value: "replication_lag_seconds", label: "Replication Lag (seconds)" },
  { value: "error_rate", label: "Error Rate (%)" },
  { value: "throughput_events_per_sec", label: "Throughput (events/sec)" },
  { value: "dlq_size", label: "DLQ Size" },
  { value: "worker_cpu_percent", label: "Worker CPU (%)" },
  { value: "worker_memory_percent", label: "Worker Memory (%)" },
  { value: "dq_failure_count", label: "DQ Failure Count" },
];

const OPERATORS = [
  { value: ">", label: "> Greater than" },
  { value: "<", label: "< Less than" },
  { value: "=", label: "= Equals" },
  { value: ">=", label: ">= Greater or equal" },
  { value: "<=", label: "<= Less or equal" },
];

export function CreateAlertRulePage() {
  const navigate = useNavigate();

  const [form, setForm] = useState({
    name: "",
    alert_type: "cdc_lag",
    severity: "warning",
    scope_type: "all",
    scope_connection_id: "",
    metric: "replication_lag_seconds",
    operator: ">",
    threshold: "",
    duration_minutes: "5",
    channels: [] as string[],
    cooldown_minutes: "15",
    auto_resolve_minutes: "",
    escalation_minutes: "",
    escalation_channel: "",
  });

  const { data: connections = [] } = useQuery({
    queryKey: ["connections"],
    queryFn: () => fetchList("/connections", "connections").catch(() => []),
  });

  const { data: channels = [] } = useQuery({
    queryKey: ["alerts", "channels"],
    queryFn: () => fetchList("/alerts/channels", "channels").catch(() => []),
  });

  const createMutation = useMutation({
    mutationFn: () => api.post("/alerts/rules", {
      name: form.name,
      alert_type: form.alert_type,
      severity: form.severity,
      scope: form.scope_type === "all" ? "all" : form.scope_connection_id,
      scope_type: form.scope_type,
      metric: form.metric,
      operator: form.operator,
      threshold: parseFloat(form.threshold),
      duration: parseInt(form.duration_minutes) * 60,
      channels: form.channels,
      cooldown_minutes: parseInt(form.cooldown_minutes) || undefined,
      auto_resolve_minutes: parseInt(form.auto_resolve_minutes) || undefined,
      escalation_minutes: parseInt(form.escalation_minutes) || undefined,
      escalation_channel: form.escalation_channel || undefined,
    }),
    onSuccess: () => navigate("/alerts/rules"),
  });

  function toggleChannel(channelId: string) {
    setForm((prev) => ({
      ...prev,
      channels: prev.channels.includes(channelId)
        ? prev.channels.filter((c) => c !== channelId)
        : [...prev.channels, channelId],
    }));
  }

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Create Alert Rule</h1>

      <form onSubmit={(e) => { e.preventDefault(); createMutation.mutate(); }} className="space-y-6">
        <Card>
          <CardHeader><CardTitle>Basic Information</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">Rule Name *</label>
              <Input value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} placeholder="e.g. High Replication Lag" required />
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Alert Type</label>
                <Select value={form.alert_type} onValueChange={(v) => setForm({ ...form, alert_type: v })}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {ALERT_TYPES.map((t) => <SelectItem key={t.value} value={t.value}>{t.label}</SelectItem>)}
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Severity</label>
                <div className="flex gap-2 pt-1">
                  {["info", "warning", "error", "critical"].map((s) => (
                    <label key={s} className="flex items-center gap-1.5 cursor-pointer">
                      <input type="radio" name="severity" value={s} checked={form.severity === s} onChange={() => setForm({ ...form, severity: s })} className="accent-primary" />
                      <span className="text-sm capitalize">{s}</span>
                    </label>
                  ))}
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader><CardTitle>Scope</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            <div className="flex flex-col gap-2">
              {[
                { value: "all", label: "All connections" },
                { value: "connection", label: "Specific connection" },
                { value: "source", label: "Specific source" },
                { value: "destination", label: "Specific destination" },
              ].map((opt) => (
                <label key={opt.value} className="flex items-center gap-2 cursor-pointer">
                  <input type="radio" name="scope_type" value={opt.value} checked={form.scope_type === opt.value} onChange={() => setForm({ ...form, scope_type: opt.value })} className="accent-primary" />
                  <span className="text-sm">{opt.label}</span>
                </label>
              ))}
            </div>
            {form.scope_type !== "all" && (
              <Select value={form.scope_connection_id} onValueChange={(v) => setForm({ ...form, scope_connection_id: v })}>
                <SelectTrigger><SelectValue placeholder="Select connection..." /></SelectTrigger>
                <SelectContent>
                  {connections.map((c: any) => <SelectItem key={c.id} value={c.id}>{c.name}</SelectItem>)}
                </SelectContent>
              </Select>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader><CardTitle>Condition</CardTitle></CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Metric</label>
                <Select value={form.metric} onValueChange={(v) => setForm({ ...form, metric: v })}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {METRICS.map((m) => <SelectItem key={m.value} value={m.value}>{m.label}</SelectItem>)}
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Operator</label>
                <Select value={form.operator} onValueChange={(v) => setForm({ ...form, operator: v })}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {OPERATORS.map((o) => <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>)}
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Threshold *</label>
                <Input value={form.threshold} onChange={(e) => setForm({ ...form, threshold: e.target.value })} type="number" step="any" required />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Duration (minutes)</label>
                <Input value={form.duration_minutes} onChange={(e) => setForm({ ...form, duration_minutes: e.target.value })} type="number" min="1" placeholder="For X consecutive min" />
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader><CardTitle>Notification Channels</CardTitle></CardHeader>
          <CardContent>
            {channels.length === 0 ? (
              <p className="text-sm text-muted-foreground">No channels configured. Create one in Alert Channels first.</p>
            ) : (
              <div className="grid grid-cols-2 md:grid-cols-3 gap-2">
                {channels.map((ch: any) => (
                  <label key={ch.id} className="flex items-center gap-2 p-2 border rounded-md cursor-pointer hover:bg-muted/50">
                    <input type="checkbox" checked={form.channels.includes(ch.id)} onChange={() => toggleChannel(ch.id)} className="accent-primary" />
                    <span className="text-sm">{ch.name}</span>
                    <span className="text-xs text-muted-foreground ml-auto">{ch.type}</span>
                  </label>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader><CardTitle>Behavior</CardTitle></CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Cooldown (minutes)</label>
                <Input value={form.cooldown_minutes} onChange={(e) => setForm({ ...form, cooldown_minutes: e.target.value })} type="number" min="1" placeholder="Min between re-fires" />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Auto-resolve (minutes)</label>
                <Input value={form.auto_resolve_minutes} onChange={(e) => setForm({ ...form, auto_resolve_minutes: e.target.value })} type="number" min="1" placeholder="Minutes below threshold" />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Escalation after (minutes)</label>
                <Input value={form.escalation_minutes} onChange={(e) => setForm({ ...form, escalation_minutes: e.target.value })} type="number" min="1" placeholder="Optional" />
              </div>
            </div>
            {form.escalation_minutes && (
              <div className="mt-4 space-y-2">
                <label className="text-sm font-medium">Escalation Channel</label>
                <Select value={form.escalation_channel} onValueChange={(v) => setForm({ ...form, escalation_channel: v })}>
                  <SelectTrigger><SelectValue placeholder="Select channel..." /></SelectTrigger>
                  <SelectContent>
                    {channels.map((ch: any) => <SelectItem key={ch.id} value={ch.id}>{ch.name}</SelectItem>)}
                  </SelectContent>
                </Select>
              </div>
            )}
          </CardContent>
        </Card>

        <div className="flex gap-2">
          <Button type="button" variant="outline" onClick={() => navigate("/alerts/rules")}>Cancel</Button>
          <Button type="submit" disabled={createMutation.isPending}>
            {createMutation.isPending ? "Creating..." : "Create Rule"}
          </Button>
        </div>
      </form>
    </div>
  );
}
