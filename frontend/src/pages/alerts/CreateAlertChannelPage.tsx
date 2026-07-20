import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useMutation } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";

const CHANNEL_TYPES = [
  { value: "email", label: "Email" },
  { value: "slack", label: "Slack" },
  { value: "teams", label: "Microsoft Teams" },
  { value: "webhook", label: "Webhook" },
  { value: "pagerduty", label: "PagerDuty" },
];

export function CreateAlertChannelPage() {
  const navigate = useNavigate();
  const [form, setForm] = useState({
    name: "",
    type: "slack",
    webhook_url: "",
    slack_channel: "",
    slack_token: "",
    email_recipients: "",
    pagerduty_key: "",
    rate_limit_per_hour: "",
    rate_limit_per_day: "",
  });

  const createMutation = useMutation({
    mutationFn: () => {
      const payload: Record<string, any> = {
        name: form.name,
        type: form.type,
        rate_limit_per_hour: parseInt(form.rate_limit_per_hour) || undefined,
        rate_limit_per_day: parseInt(form.rate_limit_per_day) || undefined,
      };
      if (form.type === "slack") {
        payload.slack_channel = form.slack_channel;
        payload.slack_token = form.slack_token;
      } else if (form.type === "teams" || form.type === "webhook") {
        payload.webhook_url = form.webhook_url;
      } else if (form.type === "email") {
        payload.recipients = form.email_recipients.split(",").map((e) => e.trim()).filter(Boolean);
      } else if (form.type === "pagerduty") {
        payload.integration_key = form.pagerduty_key;
      }
      return api.post("/alerts/channels", payload);
    },
    onSuccess: () => navigate("/alerts/channels"),
  });

  const testMutation = useMutation({
    mutationFn: () => api.post("/alerts/channels/test", { ...form }),
  });

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Create Notification Channel</h1>

      <form onSubmit={(e) => { e.preventDefault(); createMutation.mutate(); }} className="space-y-6">
        <Card>
          <CardHeader><CardTitle>Channel Configuration</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Channel Name *</label>
                <Input value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} placeholder="e.g. Ops Slack Channel" required />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Type</label>
                <Select value={form.type} onValueChange={(v) => setForm({ ...form, type: v })}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {CHANNEL_TYPES.map((t) => <SelectItem key={t.value} value={t.value}>{t.label}</SelectItem>)}
                  </SelectContent>
                </Select>
              </div>
            </div>

            {form.type === "slack" && (
              <div className="space-y-4">
                <div className="space-y-2">
                  <label className="text-sm font-medium">Slack Channel *</label>
                  <Input value={form.slack_channel} onChange={(e) => setForm({ ...form, slack_channel: e.target.value })} placeholder="#alerts" required />
                </div>
                <div className="space-y-2">
                  <label className="text-sm font-medium">Bot Token *</label>
                  <Input value={form.slack_token} onChange={(e) => setForm({ ...form, slack_token: e.target.value })} placeholder="xoxb-..." type="password" required />
                </div>
              </div>
            )}

            {form.type === "teams" && (
              <div className="space-y-2">
                <label className="text-sm font-medium">Webhook URL *</label>
                <Input value={form.webhook_url} onChange={(e) => setForm({ ...form, webhook_url: e.target.value })} placeholder="https://outlook.office.com/webhook/..." type="url" required />
              </div>
            )}

            {form.type === "webhook" && (
              <div className="space-y-2">
                <label className="text-sm font-medium">Webhook URL *</label>
                <Input value={form.webhook_url} onChange={(e) => setForm({ ...form, webhook_url: e.target.value })} placeholder="https://your-endpoint.com/webhook" type="url" required />
              </div>
            )}

            {form.type === "email" && (
              <div className="space-y-2">
                <label className="text-sm font-medium">Recipients (comma-separated) *</label>
                <Input value={form.email_recipients} onChange={(e) => setForm({ ...form, email_recipients: e.target.value })} placeholder="ops@company.com, oncall@company.com" required />
              </div>
            )}

            {form.type === "pagerduty" && (
              <div className="space-y-2">
                <label className="text-sm font-medium">Integration Key *</label>
                <Input value={form.pagerduty_key} onChange={(e) => setForm({ ...form, pagerduty_key: e.target.value })} placeholder="PagerDuty Events API v2 key" type="password" required />
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader><CardTitle>Rate Limiting</CardTitle></CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Max per hour</label>
                <Input value={form.rate_limit_per_hour} onChange={(e) => setForm({ ...form, rate_limit_per_hour: e.target.value })} type="number" min="1" placeholder="Unlimited" />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Max per day</label>
                <Input value={form.rate_limit_per_day} onChange={(e) => setForm({ ...form, rate_limit_per_day: e.target.value })} type="number" min="1" placeholder="Unlimited" />
              </div>
            </div>
          </CardContent>
        </Card>

        <div className="flex gap-2">
          <Button type="button" variant="ghost" onClick={() => navigate("/alerts/channels")}>Cancel</Button>
          <Button type="button" variant="outline" onClick={() => testMutation.mutate()} disabled={testMutation.isPending}>
            {testMutation.isPending ? "Testing..." : "Test Channel"}
          </Button>
          <Button type="submit" disabled={createMutation.isPending}>
            {createMutation.isPending ? "Creating..." : "Create"}
          </Button>
        </div>

        {testMutation.isSuccess && <p className="text-sm text-green-600">Test notification sent successfully</p>}
        {testMutation.isError && <p className="text-sm text-destructive">Test failed — check configuration</p>}
      </form>
    </div>
  );
}
