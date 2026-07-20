import { useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { useMutation, useQuery } from "@tanstack/react-query";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";

// Rule types accepted by DQPolicyBase validator
const RULE_TYPES = [
  { value: "null_check", label: "Null Check", desc: "Fail if any column value is NULL" },
  { value: "range_check", label: "Range Check", desc: "Fail if numeric column is outside min/max bounds" },
  { value: "regex", label: "Regex Match", desc: "Fail if column value doesn't match a pattern" },
  { value: "uniqueness", label: "Uniqueness", desc: "Fail if duplicate values detected in a column" },
  { value: "freshness", label: "Freshness", desc: "Fail if data is older than a threshold (hours)" },
  { value: "enum_check", label: "Enum Check", desc: "Fail if column value is not in the allowed set" },
  { value: "format_check", label: "Format Check", desc: "Fail if column value has an unexpected format" },
  { value: "referential_integrity", label: "Referential Integrity", desc: "Fail if FK value doesn't exist in parent table" },
  { value: "statistical_outlier", label: "Statistical Outlier", desc: "Fail if column value is a statistical outlier (z-score)" },
  { value: "custom_sql", label: "Custom SQL", desc: "Run a custom SQL expression — must return 0 for pass" },
];

interface DQExample {
  label: string;
  description: string;
  rule_type: string;
  columns: string;
  severity: string;
  action_on_failure: string;
  threshold_type: string;
  threshold_value: string;
  rule_definition: Record<string, any>;
}

const EXAMPLES: DQExample[] = [
  {
    label: "No Null Emails",
    description: "Reject records where email is NULL",
    rule_type: "null_check",
    columns: "email",
    severity: "error",
    action_on_failure: "quarantine",
    threshold_type: "percentage",
    threshold_value: "0",
    rule_definition: {},
  },
  {
    label: "Positive Amount",
    description: "Amount field must be > 0 and < 1,000,000",
    rule_type: "range_check",
    columns: "amount",
    severity: "error",
    action_on_failure: "reject",
    threshold_type: "percentage",
    threshold_value: "0",
    rule_definition: { min: 0, max: 1000000 },
  },
  {
    label: "Valid Phone Format",
    description: "Phone must match E.164 format (+91XXXXXXXXXX)",
    rule_type: "regex",
    columns: "phone",
    severity: "warning",
    action_on_failure: "log",
    threshold_type: "percentage",
    threshold_value: "5",
    rule_definition: { pattern: "^\\+[1-9]\\d{1,14}$" },
  },
  {
    label: "Unique Transaction ID",
    description: "transaction_id must be unique across all records",
    rule_type: "uniqueness",
    columns: "transaction_id",
    severity: "critical",
    action_on_failure: "block",
    threshold_type: "count",
    threshold_value: "0",
    rule_definition: {},
  },
  {
    label: "Status Enum",
    description: "status field must be one of: pending, active, completed, cancelled",
    rule_type: "enum_check",
    columns: "status",
    severity: "error",
    action_on_failure: "quarantine",
    threshold_type: "percentage",
    threshold_value: "0",
    rule_definition: { allowed_values: ["pending", "active", "completed", "cancelled"] },
  },
  {
    label: "Fresh Data (< 24h)",
    description: "Data must have arrived within the last 24 hours",
    rule_type: "freshness",
    columns: "created_at",
    severity: "warning",
    action_on_failure: "alert",
    threshold_type: "percentage",
    threshold_value: "0",
    rule_definition: { max_age_hours: 24 },
  },
];

export function CreateDQPolicyPage() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();

  const [form, setForm] = useState({
    policy_name: "",
    description: "",
    connection_id: "",
    stream_id: "",
    rule_type: searchParams.get("rule_type") ?? "null_check",
    target_columns: "",
    severity: "error",
    action_on_failure: "quarantine",
    threshold_type: "percentage",
    threshold_value: "0",
    rule_definition: {} as Record<string, any>,
  });

  const { data: connections } = useQuery({
    queryKey: ["connections"],
    queryFn: () => fetchList("/connections", "connections").catch(() => []),
  });

  const { data: streams } = useQuery({
    queryKey: ["streams", form.connection_id],
    queryFn: () =>
      api.get(`/streams/connections/${form.connection_id}/streams`)
        .then((r) => r.data?.streams ?? [])
        .catch(() => []),
    enabled: !!form.connection_id,
  });

  const createMutation = useMutation({
    mutationFn: () =>
      api.post("/data-quality/policies", {
        policy_name: form.policy_name,
        description: form.description || undefined,
        connection_id: form.connection_id || undefined,
        stream_id: form.stream_id || undefined,
        rule_type: form.rule_type,
        rule_definition: buildRuleDefinition(),
        target_columns: form.target_columns.split(",").map((c) => c.trim()).filter(Boolean),
        severity: form.severity,
        action_on_failure: form.action_on_failure,
        threshold_type: form.threshold_value ? form.threshold_type : undefined,
        threshold_value: form.threshold_value ? parseFloat(form.threshold_value) : undefined,
        is_active: true,
      }),
    onSuccess: (res) => navigate(`/data-quality/policies/${res.data.policy_id ?? res.data.id}`),
  });

  const testMutation = useMutation({
    mutationFn: () =>
      api.post("/data-quality/policies/test", {
        rule_type: form.rule_type,
        rule_definition: buildRuleDefinition(),
        target_columns: form.target_columns.split(",").map((c) => c.trim()).filter(Boolean),
        connection_id: form.connection_id || undefined,
        stream_id: form.stream_id || undefined,
      }),
  });

  function buildRuleDefinition() {
    const base = { ...form.rule_definition };
    return base;
  }

  const loadExample = (ex: DQExample) => {
    setForm((f) => ({
      ...f,
      policy_name: ex.label,
      description: ex.description,
      rule_type: ex.rule_type,
      target_columns: ex.columns,
      severity: ex.severity,
      action_on_failure: ex.action_on_failure,
      threshold_type: ex.threshold_type,
      threshold_value: ex.threshold_value,
      rule_definition: ex.rule_definition,
    }));
  };

  const ruleInfo = RULE_TYPES.find((r) => r.value === form.rule_type);

  return (
    <div className="space-y-6 max-w-3xl">
      <h1 className="text-2xl font-bold">Create DQ Policy</h1>

      {/* Example templates */}
      <Card>
        <CardHeader><CardTitle>Example Templates</CardTitle></CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground mb-3">
            Click an example to pre-fill the form.
          </p>
          <div className="flex flex-wrap gap-2">
            {EXAMPLES.map((ex) => (
              <Button key={ex.label} type="button" variant="outline" size="sm" onClick={() => loadExample(ex)}>
                {ex.label}
              </Button>
            ))}
          </div>
        </CardContent>
      </Card>

      <form onSubmit={(e) => { e.preventDefault(); createMutation.mutate(); }} className="space-y-6">
        {/* Basic Info */}
        <Card>
          <CardHeader><CardTitle>Basic Information</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">Policy Name *</label>
              <Input
                value={form.policy_name}
                onChange={(e) => setForm({ ...form, policy_name: e.target.value })}
                required
                placeholder="e.g. No Null Emails"
              />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Description</label>
              <Input
                value={form.description}
                onChange={(e) => setForm({ ...form, description: e.target.value })}
                placeholder="Describe what this policy checks"
              />
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Connection (optional)</label>
                <Select
                  value={form.connection_id || "_all_"}
                  onValueChange={(v) => setForm({ ...form, connection_id: v === "_all_" ? "" : v, stream_id: "" })}
                >
                  <SelectTrigger><SelectValue placeholder="All connections" /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="_all_">All connections</SelectItem>
                    {(Array.isArray(connections) ? connections : []).map((c: any) => (
                      <SelectItem key={c.connection_id} value={c.connection_id}>
                        {c.connection_name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <p className="text-xs text-muted-foreground">Leave empty to apply globally</p>
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Stream / Table (optional)</label>
                <Select
                  value={form.stream_id || "_all_"}
                  onValueChange={(v) => setForm({ ...form, stream_id: v === "_all_" ? "" : v })}
                  disabled={!form.connection_id}
                >
                  <SelectTrigger><SelectValue placeholder="All streams" /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="_all_">All streams</SelectItem>
                    {(Array.isArray(streams) ? streams : []).map((s: any) => (
                      <SelectItem key={s.stream_id} value={s.stream_id}>
                        {s.source_schema_name ? `${s.source_schema_name}.` : ""}{s.stream_name ?? s.source_table_name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <p className="text-xs text-muted-foreground">
                  {form.connection_id ? "Select a specific table to target" : "Pick a connection first"}
                </p>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Rule Configuration */}
        <Card>
          <CardHeader><CardTitle>Rule Configuration</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">Rule Type *</label>
              <Select value={form.rule_type} onValueChange={(v) => setForm({ ...form, rule_type: v, rule_definition: {} })}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  {RULE_TYPES.map((r) => (
                    <SelectItem key={r.value} value={r.value}>{r.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {ruleInfo && (
                <p className="text-xs text-muted-foreground">{ruleInfo.desc}</p>
              )}
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Target Columns</label>
              <Input
                value={form.target_columns}
                onChange={(e) => setForm({ ...form, target_columns: e.target.value })}
                placeholder="email, phone (comma-separated)"
              />
              <p className="text-xs text-muted-foreground">Comma-separated column names to apply this rule to</p>
            </div>

            {/* Rule-type-specific parameters */}
            {form.rule_type === "range_check" && (
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <label className="text-sm font-medium">Minimum Value</label>
                  <Input
                    type="number"
                    value={form.rule_definition.min ?? ""}
                    onChange={(e) => setForm({ ...form, rule_definition: { ...form.rule_definition, min: parseFloat(e.target.value) } })}
                    placeholder="0"
                  />
                </div>
                <div className="space-y-2">
                  <label className="text-sm font-medium">Maximum Value</label>
                  <Input
                    type="number"
                    value={form.rule_definition.max ?? ""}
                    onChange={(e) => setForm({ ...form, rule_definition: { ...form.rule_definition, max: parseFloat(e.target.value) } })}
                    placeholder="1000000"
                  />
                </div>
              </div>
            )}
            {form.rule_type === "regex" && (
              <div className="space-y-2">
                <label className="text-sm font-medium">Regex Pattern</label>
                <Input
                  value={form.rule_definition.pattern ?? ""}
                  onChange={(e) => setForm({ ...form, rule_definition: { ...form.rule_definition, pattern: e.target.value } })}
                  placeholder="^\+[1-9]\d{1,14}$"
                  className="font-mono"
                />
              </div>
            )}
            {form.rule_type === "enum_check" && (
              <div className="space-y-2">
                <label className="text-sm font-medium">Allowed Values (comma-separated)</label>
                <Input
                  value={(form.rule_definition.allowed_values ?? []).join(", ")}
                  onChange={(e) => setForm({ ...form, rule_definition: { ...form.rule_definition, allowed_values: e.target.value.split(",").map((s) => s.trim()).filter(Boolean) } })}
                  placeholder="pending, active, completed, cancelled"
                />
              </div>
            )}
            {form.rule_type === "freshness" && (
              <div className="space-y-2">
                <label className="text-sm font-medium">Max Age (hours)</label>
                <Input
                  type="number"
                  value={form.rule_definition.max_age_hours ?? ""}
                  onChange={(e) => setForm({ ...form, rule_definition: { ...form.rule_definition, max_age_hours: parseInt(e.target.value) } })}
                  placeholder="24"
                  className="w-[150px]"
                />
              </div>
            )}
            {form.rule_type === "statistical_outlier" && (
              <div className="space-y-2">
                <label className="text-sm font-medium">Z-Score Threshold</label>
                <Input
                  type="number"
                  step="0.1"
                  value={form.rule_definition.z_score_threshold ?? ""}
                  onChange={(e) => setForm({ ...form, rule_definition: { ...form.rule_definition, z_score_threshold: parseFloat(e.target.value) } })}
                  placeholder="3.0"
                  className="w-[150px]"
                />
                <p className="text-xs text-muted-foreground">Values with |z-score| above this are flagged as outliers</p>
              </div>
            )}
            {form.rule_type === "custom_sql" && (
              <div className="space-y-2">
                <label className="text-sm font-medium">SQL Expression</label>
                <textarea
                  className="w-full min-h-[80px] rounded-md border bg-muted p-2 font-mono text-sm"
                  value={form.rule_definition.sql ?? ""}
                  onChange={(e) => setForm({ ...form, rule_definition: { ...form.rule_definition, sql: e.target.value } })}
                  placeholder="SELECT COUNT(*) FROM {{table}} WHERE amount < 0"
                />
                <p className="text-xs text-muted-foreground">Must return 0 rows for the check to pass</p>
              </div>
            )}
          </CardContent>
        </Card>

        {/* Severity & Action */}
        <Card>
          <CardHeader><CardTitle>Severity &amp; Action on Failure</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Severity</label>
                <Select value={form.severity} onValueChange={(v) => setForm({ ...form, severity: v })}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="info">Info — log only, no alert</SelectItem>
                    <SelectItem value="warning">Warning — alert but continue</SelectItem>
                    <SelectItem value="error">Error — flag and quarantine</SelectItem>
                    <SelectItem value="critical">Critical — stop pipeline</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Action on Failure</label>
                <Select value={form.action_on_failure} onValueChange={(v) => setForm({ ...form, action_on_failure: v })}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="log">Log — write to audit log only</SelectItem>
                    <SelectItem value="alert">Alert — send notification</SelectItem>
                    <SelectItem value="quarantine">Quarantine — move to bad-data table</SelectItem>
                    <SelectItem value="reject">Reject — drop the record</SelectItem>
                    <SelectItem value="block">Block — pause the entire stream</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Threshold</label>
              <div className="flex items-center gap-3">
                <Select value={form.threshold_type} onValueChange={(v) => setForm({ ...form, threshold_type: v })}>
                  <SelectTrigger className="w-[140px]"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="percentage">Percentage (%)</SelectItem>
                    <SelectItem value="count">Row Count (#)</SelectItem>
                  </SelectContent>
                </Select>
                <Input
                  type="number"
                  min="0"
                  value={form.threshold_value}
                  onChange={(e) => setForm({ ...form, threshold_value: e.target.value })}
                  className="w-[100px]"
                />
                <span className="text-sm text-muted-foreground">
                  {form.threshold_type === "percentage" ? "% of rows can fail before triggering action" : "rows can fail before triggering action"}
                </span>
              </div>
              <p className="text-xs text-muted-foreground">
                Set to 0 for zero-tolerance. This policy runs automatically on every CDC batch for the selected stream.
              </p>
            </div>
          </CardContent>
        </Card>

        {/* Test result */}
        {testMutation.isSuccess && (
          <div className="p-3 rounded-md bg-green-500/10 border border-green-200 text-sm text-green-700">
            ✓ Test passed — rule configuration is valid.
          </div>
        )}
        {testMutation.isError && (
          <div className="p-3 rounded-md bg-destructive/10 border border-destructive/30 text-sm text-destructive">
            ✗ Test failed — check rule configuration.
          </div>
        )}

        <div className="flex gap-3">
          <Button type="button" variant="outline" onClick={() => navigate("/data-quality")}>Cancel</Button>
          <Button
            type="button"
            variant="outline"
            disabled={testMutation.isPending || !form.connection_id}
            onClick={() => testMutation.mutate()}
          >
            {testMutation.isPending ? "Testing..." : "Test Rule"}
          </Button>
          <Button type="submit" disabled={createMutation.isPending}>
            {createMutation.isPending ? "Creating..." : "Create Policy"}
          </Button>
        </div>

        {createMutation.isError && (
          <p className="text-sm text-destructive">Failed to create policy. Check all required fields.</p>
        )}
      </form>
    </div>
  );
}
