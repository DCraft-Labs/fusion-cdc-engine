import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useState, useEffect } from "react";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { ChevronDown, ChevronRight, Plus, X, Zap } from "lucide-react";

// ── Transform types (mirrors CreateConnectionWizard) ─────────────────────────
type TransformType =
  | "cast" | "string_op" | "math_op" | "mask"
  | "json_extract" | "json_flatten_inline" | "json_flatten_child"
  | "expression" | "udf";

interface TransformStep {
  id: string;
  type: TransformType;
  output_column: string;
  to_type?: string;
  op?: string;
  params?: Record<string, any>;
  strategy?: string;
  expression?: string;
  language?: string;
  function_name?: string;
  udf_args?: string[];
  keep_original?: boolean;
  child_table?: string;
  array_path?: string;
}

interface ColumnMeta {
  column_name: string;
  data_type: string;
  is_primary_key: boolean;
  is_nullable: boolean;
  is_json_candidate: boolean;
  transforms: TransformStep[];
}

interface EditStreamState {
  stream_id: string;
  stream_name: string;
  source_schema_name: string;
  destination_table_name: string;
  destination_schema_name: string;
  primary_keys: string[];
  cursor_field: string;
  is_enabled: boolean;
  sync_mode: string;
  columns: ColumnMeta[];
  expanded: boolean;
  table_udfs: Array<{ function_name: string; args: string; output_column: string }>;
}

// ── Helpers ──────────────────────────────────────────────────────────────────
function makeId() { return Math.random().toString(36).slice(2, 8); }

function buildTransformSpec(s: EditStreamState): Record<string, any> | undefined {
  const steps: any[] = [];
  for (const col of s.columns) {
    for (const t of col.transforms) {
      const step: any = { id: t.id, type: t.type, column: col.column_name, output_column: t.output_column || col.column_name };
      if (t.type === "cast") step.to_type = t.to_type || "string";
      if (t.type === "string_op") { step.op = t.op || "upper"; step.params = t.params || {}; }
      if (t.type === "math_op") step.expression = t.expression || "";
      if (t.type === "mask") step.strategy = t.strategy || "last4";
      if (t.type === "expression") { step.expression = t.expression || ""; step.language = t.language || "spark_sql"; }
      if (t.type === "udf") { step.function = t.function_name || ""; step.args = t.udf_args || []; }
      if (t.type === "json_flatten_inline") step.keep_original = t.keep_original ?? false;
      if (t.type === "json_flatten_child") { step.child_table = t.child_table || ""; step.array_path = t.array_path || "$[*]"; }
      if (t.type === "json_extract") step.json_path = t.params?.json_path || "$";
      steps.push(step);
    }
  }
  for (const udf of s.table_udfs) {
    if (!udf.function_name) continue;
    steps.push({ id: makeId(), type: "udf", function: udf.function_name, args: udf.args.split(",").map((x) => x.trim()).filter(Boolean), output_column: udf.output_column || udf.function_name + "_result" });
  }
  return steps.length > 0 ? { transforms: steps } : undefined;
}

const TRANSFORM_LABEL: Record<TransformType, string> = {
  cast: "Cast Type", string_op: "String Op", math_op: "Math", mask: "Mask",
  json_extract: "JSON Extract", json_flatten_inline: "Flatten JSON (inline)",
  json_flatten_child: "Flatten JSON (child table)", expression: "SQL Expression", udf: "UDF",
};
const TRANSFORM_COLOR: Record<TransformType, string> = {
  cast: "bg-blue-100 text-blue-700 border-blue-200",
  string_op: "bg-purple-100 text-purple-700 border-purple-200",
  math_op: "bg-orange-100 text-orange-700 border-orange-200",
  mask: "bg-red-100 text-red-700 border-red-200",
  json_extract: "bg-teal-100 text-teal-700 border-teal-200",
  json_flatten_inline: "bg-emerald-100 text-emerald-700 border-emerald-200",
  json_flatten_child: "bg-cyan-100 text-cyan-700 border-cyan-200",
  expression: "bg-violet-100 text-violet-700 border-violet-200",
  udf: "bg-amber-100 text-amber-700 border-amber-200",
};

// ── TransformStepEditor ───────────────────────────────────────────────────────
function TransformStepEditor({ step, col, udfs, onChange, onRemove }: {
  step: TransformStep; col: ColumnMeta; udfs: any[];
  onChange: (u: TransformStep) => void; onRemove: () => void;
}) {
  const set = (p: Partial<TransformStep>) => onChange({ ...step, ...p });
  return (
    <div className={`rounded-md border px-3 py-2 text-xs space-y-2 ${TRANSFORM_COLOR[step.type]}`}>
      <div className="flex items-center justify-between">
        <span className="font-semibold">{TRANSFORM_LABEL[step.type]}</span>
        <button onClick={onRemove} className="hover:opacity-70"><X className="h-3 w-3" /></button>
      </div>
      {step.type !== "json_flatten_child" && (
        <div className="flex items-center gap-2">
          <span className="text-muted-foreground w-20 shrink-0">Output col:</span>
          <Input className="h-6 text-xs py-0" value={step.output_column} onChange={(e) => set({ output_column: e.target.value })} placeholder={col.column_name} />
        </div>
      )}
      {step.type === "cast" && (
        <div className="flex items-center gap-2">
          <span className="text-muted-foreground w-20 shrink-0">Cast to:</span>
          <Select value={step.to_type || "string"} onValueChange={(v) => set({ to_type: v })}>
            <SelectTrigger className="h-6 text-xs py-0"><SelectValue /></SelectTrigger>
            <SelectContent>{["string","integer","long","float","double","boolean","timestamp","date"].map((t) => <SelectItem key={t} value={t}>{t}</SelectItem>)}</SelectContent>
          </Select>
        </div>
      )}
      {step.type === "string_op" && (
        <div className="flex items-center gap-2">
          <span className="text-muted-foreground w-20 shrink-0">Operation:</span>
          <Select value={step.op || "upper"} onValueChange={(v) => set({ op: v })}>
            <SelectTrigger className="h-6 text-xs py-0"><SelectValue /></SelectTrigger>
            <SelectContent>{["upper","lower","trim","ltrim","rtrim","substring","replace","reverse"].map((o) => <SelectItem key={o} value={o}>{o}</SelectItem>)}</SelectContent>
          </Select>
        </div>
      )}
      {step.type === "string_op" && step.op === "substring" && (
        <div className="grid grid-cols-2 gap-2">
          <div className="flex items-center gap-1"><span className="text-muted-foreground w-10 shrink-0">Start:</span><Input className="h-6 text-xs py-0" type="number" value={step.params?.start ?? 0} onChange={(e) => set({ params: { ...step.params, start: parseInt(e.target.value) || 0 } })} /></div>
          <div className="flex items-center gap-1"><span className="text-muted-foreground w-14 shrink-0">Length:</span><Input className="h-6 text-xs py-0" type="number" value={step.params?.length ?? ""} onChange={(e) => set({ params: { ...step.params, length: parseInt(e.target.value) || undefined } })} /></div>
        </div>
      )}
      {step.type === "math_op" && (
        <div className="flex items-center gap-2"><span className="text-muted-foreground w-20 shrink-0">Expression:</span><Input className="h-6 text-xs py-0 font-mono" value={step.expression || ""} onChange={(e) => set({ expression: e.target.value })} placeholder="amount * 1.18" /></div>
      )}
      {step.type === "mask" && (
        <div className="flex items-center gap-2">
          <span className="text-muted-foreground w-20 shrink-0">Strategy:</span>
          <Select value={step.strategy || "last4"} onValueChange={(v) => set({ strategy: v })}>
            <SelectTrigger className="h-6 text-xs py-0"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="last4">last4 (keep last 4 chars)</SelectItem>
              <SelectItem value="hash">SHA-256 hash</SelectItem>
              <SelectItem value="redact">Full redact (***)</SelectItem>
              <SelectItem value="first2last4">first2 + **** + last4</SelectItem>
            </SelectContent>
          </Select>
        </div>
      )}
      {step.type === "expression" && (
        <>
          <div className="flex items-center gap-2">
            <span className="text-muted-foreground w-20 shrink-0">Language:</span>
            <Select value={step.language || "spark_sql"} onValueChange={(v) => set({ language: v })}>
              <SelectTrigger className="h-6 text-xs py-0"><SelectValue /></SelectTrigger>
              <SelectContent><SelectItem value="spark_sql">SQL Expression</SelectItem><SelectItem value="sel">SEL (simple)</SelectItem></SelectContent>
            </Select>
          </div>
          <Input className="h-6 text-xs py-0 font-mono" value={step.expression || ""} onChange={(e) => set({ expression: e.target.value })} placeholder="CASE WHEN amount > 0 THEN 'pos' ELSE 'neg' END" />
        </>
      )}
      {step.type === "json_extract" && (
        <div className="flex items-center gap-2"><span className="text-muted-foreground w-20 shrink-0">JSON path:</span><Input className="h-6 text-xs py-0 font-mono" value={step.params?.json_path || "$"} onChange={(e) => set({ params: { ...step.params, json_path: e.target.value } })} placeholder="$.amount" /></div>
      )}
      {step.type === "json_flatten_inline" && (
        <div className="flex items-center gap-2"><input type="checkbox" checked={step.keep_original ?? false} onChange={(e) => set({ keep_original: e.target.checked })} id={`keep-${step.id}`} /><label htmlFor={`keep-${step.id}`} className="text-muted-foreground">Keep original JSON column</label></div>
      )}
      {step.type === "json_flatten_child" && (
        <>
          <div className="flex items-center gap-2"><span className="text-muted-foreground w-24 shrink-0">Child table:</span><Input className="h-6 text-xs py-0" value={step.child_table || ""} onChange={(e) => set({ child_table: e.target.value })} placeholder="line_items" /></div>
          <div className="flex items-center gap-2"><span className="text-muted-foreground w-24 shrink-0">Array path:</span><Input className="h-6 text-xs py-0 font-mono" value={step.array_path || "$[*]"} onChange={(e) => set({ array_path: e.target.value })} placeholder="$[*]" /></div>
        </>
      )}
      {step.type === "udf" && (
        <>
          <div className="flex items-center gap-2">
            <span className="text-muted-foreground w-20 shrink-0">Function:</span>
            {udfs.length > 0 ? (
              <Select value={step.function_name || ""} onValueChange={(v) => set({ function_name: v })}>
                <SelectTrigger className="h-6 text-xs py-0"><SelectValue placeholder="Select UDF" /></SelectTrigger>
                <SelectContent>{udfs.map((u: any) => <SelectItem key={u.udf_id ?? u.id} value={u.function_name ?? u.udf_name ?? u.name}>{u.function_name ?? u.udf_name ?? u.name}</SelectItem>)}</SelectContent>
              </Select>
            ) : (
              <Input className="h-6 text-xs py-0" value={step.function_name || ""} onChange={(e) => set({ function_name: e.target.value })} placeholder="compute_risk_score" />
            )}
          </div>
          <div className="flex items-center gap-2"><span className="text-muted-foreground w-20 shrink-0">Args (cols):</span><Input className="h-6 text-xs py-0" value={step.udf_args?.join(", ") || ""} onChange={(e) => set({ udf_args: e.target.value.split(",").map((s) => s.trim()).filter(Boolean) })} placeholder="amount_usd, country_code" /></div>
        </>
      )}
    </div>
  );
}

// ── ColumnRow ─────────────────────────────────────────────────────────────────
function ColumnRow({ col, udfs, onUpdate }: { col: ColumnMeta; udfs: any[]; onUpdate: (c: ColumnMeta) => void }) {
  const [addingType, setAddingType] = useState<TransformType | "">("");
  const addTransform = () => {
    if (!addingType) return;
    onUpdate({ ...col, transforms: [...col.transforms, { id: makeId(), type: addingType as TransformType, output_column: col.column_name }] });
    setAddingType("");
  };
  return (
    <div className={`px-4 py-2 border-b last:border-0 ${col.transforms.length > 0 ? "bg-muted/20" : ""}`}>
      <div className="flex items-start gap-3">
        <div className="w-44 shrink-0 flex items-center gap-1.5">
          {col.is_primary_key && <span title="Primary Key" className="text-xs text-amber-600">🔑</span>}
          <span className="font-mono text-sm font-medium">{col.column_name}</span>
        </div>
        <div className="w-28 shrink-0">
          <span className={`text-xs px-1.5 py-0.5 rounded font-mono ${col.is_json_candidate ? "bg-teal-100 text-teal-700" : "bg-muted text-muted-foreground"}`}>{col.data_type}</span>
        </div>
        <div className="flex-1 space-y-1">
          {col.transforms.map((step, si) => (
            <TransformStepEditor
              key={step.id} step={step} col={col} udfs={udfs}
              onChange={(u) => { const t = [...col.transforms]; t[si] = u; onUpdate({ ...col, transforms: t }); }}
              onRemove={() => onUpdate({ ...col, transforms: col.transforms.filter((_, i) => i !== si) })}
            />
          ))}
          <div className="flex items-center gap-2 mt-1">
            <Select value={addingType} onValueChange={(v) => setAddingType(v as TransformType)}>
              <SelectTrigger className="h-7 text-xs w-52"><SelectValue placeholder="+ Add transform…" /></SelectTrigger>
              <SelectContent>
                <SelectItem value="cast">Cast Type</SelectItem>
                <SelectItem value="string_op">String Operation</SelectItem>
                <SelectItem value="math_op">Math Expression</SelectItem>
                <SelectItem value="mask">Mask / Redact</SelectItem>
                {col.is_json_candidate && <SelectItem value="json_extract">JSON Extract</SelectItem>}
                {col.is_json_candidate && <SelectItem value="json_flatten_inline">Flatten JSON → columns (inline)</SelectItem>}
                {col.is_json_candidate && <SelectItem value="json_flatten_child">Flatten JSON → child table</SelectItem>}
                <SelectItem value="expression">SQL Expression</SelectItem>
                <SelectItem value="udf">UDF (custom function)</SelectItem>
              </SelectContent>
            </Select>
            {addingType && (
              <Button size="sm" variant="outline" className="h-7 text-xs px-2" onClick={addTransform}>
                <Plus className="h-3 w-3 mr-1" />Add
              </Button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────
export function EditConnectionPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [form, setForm] = useState({
    name: "",
    sync_mode: "cdc",
    schedule_type: "continuous",
    cron_expression: "",
    schema_evolution_policy: "auto_apply",
    transform_pipeline_id: "",
    dq_policy_id: "",
    max_events_per_sec: "",
    max_memory_mb: "",
    max_workers: "",
  });

  const [editStreams, setEditStreams] = useState<EditStreamState[]>([]);
  const [addTableOpen, setAddTableOpen] = useState(false);
  const [selectedNew, setSelectedNew] = useState<Record<string, boolean>>({});
  const [addSyncMode, setAddSyncMode] = useState("cdc");
  // Pending new-table configs while user is configuring before clicking "Add"
  const [pendingConfigs, setPendingConfigs] = useState<Record<string, {
    dest_table: string; dest_schema: string; primary_keys: string[]; cursor_field: string;
    columns: ColumnMeta[]; expanded: boolean; table_udfs: Array<{function_name: string; args: string; output_column: string}>;
  }>>({});

  const { data: connection, isLoading } = useQuery({
    queryKey: ["connections", id],
    queryFn: () => api.get(`/connections/${id}`).then((r) => r.data),
  });

  const { data: streamsData } = useQuery({
    queryKey: ["streams", id],
    queryFn: () => api.get(`/connections/${id}/streams`).then((r) => r.data),
    enabled: !!id,
  });

  const { data: sourceData } = useQuery({
    queryKey: ["sources", connection?.source_id],
    queryFn: () => api.get(`/sources/${connection.source_id}`).then((r) => r.data),
    enabled: !!connection?.source_id,
  });

  // Also try to get live schema from the source
  const { data: liveSchemas } = useQuery({
    queryKey: ["sources", connection?.source_id, "schemas"],
    queryFn: () => api.get(`/sources/${connection.source_id}/schemas`).then((r) => r.data),
    enabled: !!connection?.source_id,
  });

  const { data: pipelines } = useQuery({
    queryKey: ["transformations"],
    queryFn: () => api.get("/transformations").then((r) => r.data),
  });

  const { data: dqPolicies } = useQuery({
    queryKey: ["dq-policies"],
    queryFn: () => api.get("/data-quality/policies").then((r) => r.data),
  });

  const { data: udfsData } = useQuery({
    queryKey: ["udfs"],
    queryFn: () => api.get("/udfs").then((r) => r.data),
  });
  const udfs: any[] = udfsData?.udfs ?? [];

  useEffect(() => {
    if (connection) {
      setForm({
        name: connection.connection_name ?? "",
        sync_mode: connection.sync_mode ?? "cdc",
        schedule_type: connection.schedule?.type ?? "continuous",
        cron_expression: connection.schedule?.cron_expression ?? "",
        schema_evolution_policy: connection.schema_evolution_policy ?? "auto_apply",
        transform_pipeline_id: connection.transform_pipeline_id ?? "",
        dq_policy_id: connection.dq_policy_id ?? "",
        max_events_per_sec: String(connection.resource_limits?.max_events_per_sec ?? ""),
        max_memory_mb: String(connection.resource_limits?.max_memory_mb ?? ""),
        max_workers: String(connection.resource_limits?.max_workers ?? ""),
      });
    }
  }, [connection]);

  // Helper to get columns for a table from discovery cache
  const getColumnsForTable = (schemaName: string, tableName: string): ColumnMeta[] => {
    // Try live schema first
    const schemasArr = liveSchemas?.schemas ?? sourceData?.discovery_cache?.schemas ?? [];
    for (const sc of schemasArr) {
      if (schemaName && sc.schema_name && sc.schema_name !== schemaName) continue;
      const tbl = (sc.tables ?? []).find((t: any) => t.table_name === tableName);
      if (tbl?.columns?.length) {
        return tbl.columns.map((c: any): ColumnMeta => ({
          column_name: c.column_name,
          data_type: c.data_type ?? "unknown",
          is_primary_key: c.is_primary_key ?? false,
          is_nullable: c.is_nullable ?? true,
          is_json_candidate: c.is_json_candidate ?? false,
          transforms: [],
        }));
      }
    }
    return [];
  };

  // Build editStreams from loaded stream data
  useEffect(() => {
    const streams: any[] = Array.isArray(streamsData) ? streamsData : (streamsData?.streams ?? []);
    if (streams.length === 0) return;
    setEditStreams((prev) => {
      // If already have data, only backfill columns
      if (prev.length > 0) {
        return prev.map((s) => {
          if (s.columns.length > 0) return s;
          return { ...s, columns: getColumnsForTable(s.source_schema_name, s.stream_name) };
        });
      }
      // Fresh load
      return streams.map((s: any): EditStreamState => {
        // Rebuild column transforms from transform_overrides
        const rawCols = getColumnsForTable(s.source_schema_name ?? "", s.source_table_name ?? s.stream_name);
        const overrides = s.transform_overrides ?? {};
        const columns: ColumnMeta[] = rawCols.map((col) => {
          const ov = overrides[col.column_name];
          if (!ov) return col;
          // legacy simple {udf_id, udf_name} → wrap as udf TransformStep
          if (ov.udf_id || ov.udf_name) {
            return { ...col, transforms: [{ id: makeId(), type: "udf" as TransformType, output_column: col.column_name, function_name: ov.udf_name ?? ov.udf_id }] };
          }
          // full transform spec
          if (Array.isArray(ov.transforms)) {
            return { ...col, transforms: ov.transforms };
          }
          return col;
        });
        return {
          stream_id: s.stream_id,
          stream_name: s.stream_name ?? s.source_table_name,
          source_schema_name: s.source_schema_name ?? "",
          destination_table_name: s.destination_table_name ?? s.stream_name ?? s.source_table_name,
          destination_schema_name: s.destination_schema_name ?? "",
          primary_keys: Array.isArray(s.primary_keys) ? s.primary_keys : [],
          cursor_field: s.cursor_field ?? "",
          is_enabled: s.is_enabled !== false,
          sync_mode: s.sync_mode ?? "cdc",
          columns,
          expanded: false,
          table_udfs: [],
        };
      });
    });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [streamsData, liveSchemas, sourceData]);

  // Backfill columns when discovery arrives after streams loaded
  useEffect(() => {
    if (!liveSchemas && !sourceData) return;
    setEditStreams((prev) => prev.map((s) => {
      if (s.columns.length > 0) return s;
      return { ...s, columns: getColumnsForTable(s.source_schema_name, s.stream_name) };
    }));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [liveSchemas, sourceData]);

  const updateStream = (streamId: string, patch: Partial<EditStreamState>) =>
    setEditStreams((prev) => prev.map((s) => s.stream_id === streamId ? { ...s, ...patch } : s));

  const updateColumn = (streamId: string, colIdx: number, col: ColumnMeta) =>
    setEditStreams((prev) => prev.map((s) => {
      if (s.stream_id !== streamId) return s;
      const cols = [...s.columns]; cols[colIdx] = col;
      return { ...s, columns: cols };
    }));

  const updateMutation = useMutation({
    mutationFn: () =>
      api.patch(`/connections/${id}`, {
        connection_name: form.name,
        sync_mode: form.sync_mode,
        schedule: { type: form.schedule_type, cron_expression: form.schedule_type === "scheduled" ? form.cron_expression : undefined },
        schema_evolution_policy: form.schema_evolution_policy,
        transform_pipeline_id: form.transform_pipeline_id || null,
        dq_policy_id: form.dq_policy_id || null,
        resource_limits: {
          max_events_per_sec: form.max_events_per_sec ? parseInt(form.max_events_per_sec) : undefined,
          max_memory_mb: form.max_memory_mb ? parseInt(form.max_memory_mb) : undefined,
          max_workers: form.max_workers ? parseInt(form.max_workers) : undefined,
        },
      }),
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: ["connections", id] }); navigate(`/connections/${id}`); },
  });

  // Save all stream changes (enable/disable, sync mode, transforms, dest name, PKs)
  const saveStreamsMutation = useMutation({
    mutationFn: async () => {
      for (const s of editStreams) {
        const spec = buildTransformSpec(s);
        await api.patch(`/connections/${id}/streams/${s.stream_id}`, {
          is_enabled: s.is_enabled,
          sync_mode: s.sync_mode,
          destination_table_name: s.destination_table_name || undefined,
          destination_schema_name: s.destination_schema_name || undefined,
          primary_keys: s.primary_keys.length ? s.primary_keys : undefined,
          cursor_field: s.cursor_field || undefined,
          transform_steps: spec ?? {},
        });
      }
    },
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: ["streams", id] }); navigate(`/connections/${id}`); },
  });

  // Add new tables
  const addStreamsMutation = useMutation({
    mutationFn: async () => {
      const keys = Object.keys(selectedNew).filter((k) => selectedNew[k]);
      for (const key of keys) {
        const [schema, table] = key.includes(".") ? key.split(".", 2) : ["", key];
        const cfg = pendingConfigs[key];
        const columns = cfg?.columns ?? getColumnsForTable(schema, table);
        const pks = cfg?.primary_keys?.length ? cfg.primary_keys
          : columns.filter((c) => c.is_primary_key).map((c) => c.column_name);
        // Build transform spec from pending column configs
        const fakeStream: EditStreamState = {
          stream_id: "", stream_name: table, source_schema_name: schema,
          destination_table_name: cfg?.dest_table || table,
          destination_schema_name: cfg?.dest_schema || schema,
          primary_keys: pks, cursor_field: cfg?.cursor_field ?? "",
          is_enabled: true, sync_mode: addSyncMode,
          columns: cfg?.columns ?? columns,
          expanded: false, table_udfs: cfg?.table_udfs ?? [],
        };
        const spec = buildTransformSpec(fakeStream);
        await api.post(`/connections/${id}/streams`, {
          stream_name: table,
          source_table_name: table,
          source_schema_name: schema || undefined,
          destination_table_name: cfg?.dest_table || table,
          destination_schema_name: cfg?.dest_schema || schema || undefined,
          sync_mode: addSyncMode,
          primary_keys: pks,
          cursor_field: cfg?.cursor_field || undefined,
          column_mapping: {},
          transform_steps: spec ?? {},
          is_enabled: true,
        });
      }
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["streams", id] });
      setSelectedNew({}); setAddTableOpen(false); setPendingConfigs({});
    },
  });

  if (isLoading) return <div className="flex items-center justify-center h-64">Loading…</div>;

  const pipelineList = pipelines?.pipelines ?? [];
  const dqPolicyList = dqPolicies?.policies ?? [];

  // Tables available to add
  const schemasArr = liveSchemas?.schemas ?? sourceData?.discovery_cache?.schemas ?? [];
  const allDiscoveredTables: { key: string; schema: string; table: string; pks: string; columns: ColumnMeta[] }[] = [];
  schemasArr.forEach((sc: any) => {
    const sn = sc.schema_name ?? "";
    (sc.tables ?? []).forEach((tbl: any) => {
      const key = sn ? `${sn}.${tbl.table_name}` : tbl.table_name;
      const cols = (tbl.columns ?? []).map((c: any): ColumnMeta => ({
        column_name: c.column_name, data_type: c.data_type ?? "unknown",
        is_primary_key: c.is_primary_key ?? false, is_nullable: c.is_nullable ?? true,
        is_json_candidate: c.is_json_candidate ?? false, transforms: [],
      }));
      const pks = cols.filter((c: ColumnMeta) => c.is_primary_key).map((c: ColumnMeta) => c.column_name).join(", ") || "—";
      allDiscoveredTables.push({ key, schema: sn, table: tbl.table_name, pks, columns: cols });
    });
  });
  const existingKeys = new Set(editStreams.map((s) => s.source_schema_name ? `${s.source_schema_name}.${s.stream_name}` : s.stream_name));
  const addableTables = allDiscoveredTables.filter((t) => !existingKeys.has(t.key));

  const totalTransforms = editStreams.reduce((a, s) => a + s.columns.reduce((b, c) => b + c.transforms.length, 0) + s.table_udfs.filter((u) => u.function_name).length, 0);

  return (
    <div className="space-y-6 max-w-5xl">
      <h1 className="text-2xl font-bold">Edit Connection: {connection?.connection_name}</h1>

      <Tabs defaultValue="streams">
        <TabsList>
          <TabsTrigger value="streams">Streams &amp; Transforms ({editStreams.length})</TabsTrigger>
          <TabsTrigger value="config">Configuration</TabsTrigger>
          <TabsTrigger value="limits">Resource Limits</TabsTrigger>
        </TabsList>

        {/* ── Streams & Transforms tab ──────────────────────────────── */}
        <TabsContent value="streams">
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground">Click a table row to expand and configure per-column transforms.</p>
              </div>
              <div className="flex items-center gap-3">
                {totalTransforms > 0 && (
                  <Badge variant="outline" className="gap-1">
                    <Zap className="h-3 w-3" />{totalTransforms} transform{totalTransforms !== 1 ? "s" : ""}
                  </Badge>
                )}
                <Button size="sm" variant="outline" onClick={() => setAddTableOpen(!addTableOpen)}>
                  <Plus className="mr-1 h-4 w-4" />Add Tables
                </Button>
                <Button size="sm" disabled={saveStreamsMutation.isPending} onClick={() => saveStreamsMutation.mutate()}>
                  {saveStreamsMutation.isPending ? "Saving…" : "Save All Changes"}
                </Button>
              </div>
            </div>
            {saveStreamsMutation.isError && <p className="text-sm text-destructive">Failed to save some streams. Check logs.</p>}

            {editStreams.length === 0 ? (
              <Card><CardContent className="py-10 text-center text-muted-foreground">No streams configured. Use "Add Tables" to add from the source schema.</CardContent></Card>
            ) : (
              <div className="space-y-2">
                {editStreams.map((stream) => {
                  const transformCount = stream.columns.reduce((a, c) => a + c.transforms.length, 0) + stream.table_udfs.filter((u) => u.function_name).length;
                  return (
                    <Card key={stream.stream_id} className={stream.expanded ? "ring-1 ring-primary" : ""}>
                      {/* Table header row */}
                      <div className="flex items-center gap-3 px-4 py-3">
                        <input
                          type="checkbox"
                          checked={stream.is_enabled}
                          onChange={(e) => updateStream(stream.stream_id, { is_enabled: e.target.checked })}
                          className="h-4 w-4"
                          title="Enable/disable this stream"
                        />
                        <button
                          className="flex items-center gap-1 text-muted-foreground hover:text-foreground"
                          onClick={() => updateStream(stream.stream_id, { expanded: !stream.expanded })}
                        >
                          {stream.expanded ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                        </button>
                        <span className="font-mono text-sm font-semibold flex-1">
                          {stream.source_schema_name ? `${stream.source_schema_name}.` : ""}{stream.stream_name}
                        </span>
                        {transformCount > 0 && (
                          <Badge variant="outline" className="text-xs gap-1">
                            <Zap className="h-3 w-3" />{transformCount}
                          </Badge>
                        )}
                        <Select value={stream.sync_mode} onValueChange={(v) => updateStream(stream.stream_id, { sync_mode: v })}>
                          <SelectTrigger className="h-7 text-xs w-36"><SelectValue /></SelectTrigger>
                          <SelectContent>
                            <SelectItem value="cdc">CDC (realtime)</SelectItem>
                            <SelectItem value="incremental">Incremental</SelectItem>
                            <SelectItem value="full_refresh">Full Refresh</SelectItem>
                          </SelectContent>
                        </Select>
                        <span className="text-xs text-muted-foreground w-24 text-right">
                          PK: <span className="font-mono">{stream.primary_keys.join(", ") || "—"}</span>
                        </span>
                      </div>

                      {/* Expanded config */}
                      {stream.expanded && (
                        <div className="border-t">
                          {/* Destination + cursor row */}
                          <div className="flex items-center gap-3 px-4 py-2 bg-muted/30 text-xs border-b flex-wrap">
                            <span className="text-muted-foreground w-28 shrink-0">Destination table:</span>
                            <Input
                              className="h-6 text-xs py-0 w-48"
                              value={stream.destination_table_name}
                              onChange={(e) => updateStream(stream.stream_id, { destination_table_name: e.target.value })}
                              placeholder={stream.stream_name}
                            />
                            <span className="text-muted-foreground ml-2 w-28 shrink-0">Dest schema:</span>
                            <Input
                              className="h-6 text-xs py-0 w-32"
                              value={stream.destination_schema_name}
                              onChange={(e) => updateStream(stream.stream_id, { destination_schema_name: e.target.value })}
                              placeholder={stream.source_schema_name}
                            />
                            <span className="text-muted-foreground ml-2 w-20 shrink-0">Cursor field:</span>
                            <Input
                              className="h-6 text-xs py-0 w-36"
                              value={stream.cursor_field}
                              onChange={(e) => updateStream(stream.stream_id, { cursor_field: e.target.value })}
                              placeholder={stream.sync_mode === "cdc" ? "(auto — binlog)" : "updated_at"}
                            />
                            <span className="text-muted-foreground ml-2 w-20 shrink-0">Primary keys:</span>
                            <Input
                              className="h-6 text-xs py-0 w-40 font-mono"
                              value={stream.primary_keys.join(", ")}
                              onChange={(e) => updateStream(stream.stream_id, { primary_keys: e.target.value.split(",").map((k) => k.trim()).filter(Boolean) })}
                              placeholder="id, uuid"
                            />
                          </div>

                          {/* Column list */}
                          {stream.columns.length === 0 ? (
                            <div className="px-4 py-3 text-xs text-muted-foreground">
                              No column metadata. Run "Discover Schemas" on the source first.
                            </div>
                          ) : (
                            <>
                              <div className="flex items-center gap-3 px-4 py-1.5 bg-muted/50 text-xs font-medium text-muted-foreground border-b">
                                <span className="w-44">Column</span>
                                <span className="w-28">Type</span>
                                <span className="flex-1">Transforms</span>
                              </div>
                              {stream.columns.map((col, ci) => (
                                <ColumnRow
                                  key={col.column_name}
                                  col={col}
                                  udfs={udfs}
                                  onUpdate={(updated) => updateColumn(stream.stream_id, ci, updated)}
                                />
                              ))}
                            </>
                          )}

                          {/* Table-level UDFs */}
                          <div className="border-t px-4 py-3 bg-amber-50/50 space-y-2">
                            <div className="flex items-center justify-between">
                              <span className="text-xs font-semibold text-amber-700">Table-level UDFs</span>
                              <span className="text-xs text-muted-foreground">Applied to the whole stream after column transforms</span>
                              <Button size="sm" variant="ghost" className="h-6 text-xs text-amber-700"
                                onClick={() => updateStream(stream.stream_id, { table_udfs: [...stream.table_udfs, { function_name: "", args: "", output_column: "" }] })}>
                                <Plus className="h-3 w-3 mr-1" />Add UDF
                              </Button>
                            </div>
                            {stream.table_udfs.map((udf, ui) => (
                              <div key={ui} className="flex items-center gap-2 text-xs">
                                {udfs.length > 0 ? (
                                  <Select value={udf.function_name} onValueChange={(v) => { const upd = [...stream.table_udfs]; upd[ui] = { ...upd[ui], function_name: v }; updateStream(stream.stream_id, { table_udfs: upd }); }}>
                                    <SelectTrigger className="h-6 text-xs w-44"><SelectValue placeholder="Select UDF" /></SelectTrigger>
                                    <SelectContent>{udfs.map((u: any) => <SelectItem key={u.udf_id} value={u.udf_name ?? u.function_name}>{u.udf_name ?? u.function_name}</SelectItem>)}</SelectContent>
                                  </Select>
                                ) : (
                                  <Input className="h-6 text-xs w-44" value={udf.function_name} placeholder="udf_name" onChange={(e) => { const upd = [...stream.table_udfs]; upd[ui] = { ...upd[ui], function_name: e.target.value }; updateStream(stream.stream_id, { table_udfs: upd }); }} />
                                )}
                                <Input className="h-6 text-xs flex-1" value={udf.args} placeholder="args: col1, col2" onChange={(e) => { const upd = [...stream.table_udfs]; upd[ui] = { ...upd[ui], args: e.target.value }; updateStream(stream.stream_id, { table_udfs: upd }); }} />
                                <Input className="h-6 text-xs w-32" value={udf.output_column} placeholder="output_col" onChange={(e) => { const upd = [...stream.table_udfs]; upd[ui] = { ...upd[ui], output_column: e.target.value }; updateStream(stream.stream_id, { table_udfs: upd }); }} />
                                <button onClick={() => updateStream(stream.stream_id, { table_udfs: stream.table_udfs.filter((_, i) => i !== ui) })} className="text-muted-foreground hover:text-destructive"><X className="h-3 w-3" /></button>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </Card>
                  );
                })}
              </div>
            )}

            {/* ── Add Tables panel ── */}
            {addTableOpen && (
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center justify-between text-base">
                    <span>Add Tables from Source</span>
                    <div className="flex items-center gap-2">
                      <span className="text-sm text-muted-foreground font-normal">{Object.values(selectedNew).filter(Boolean).length} selected</span>
                      <Select value={addSyncMode} onValueChange={setAddSyncMode}>
                        <SelectTrigger className="h-8 w-36"><SelectValue /></SelectTrigger>
                        <SelectContent>
                          <SelectItem value="cdc">CDC (realtime)</SelectItem>
                          <SelectItem value="incremental">Incremental</SelectItem>
                          <SelectItem value="full_refresh">Full Refresh</SelectItem>
                        </SelectContent>
                      </Select>
                      <Button size="sm" disabled={Object.values(selectedNew).filter(Boolean).length === 0 || addStreamsMutation.isPending}
                        onClick={() => addStreamsMutation.mutate()}>
                        {addStreamsMutation.isPending ? "Adding…" : `Add ${Object.values(selectedNew).filter(Boolean).length} table(s)`}
                      </Button>
                    </div>
                  </CardTitle>
                </CardHeader>
                {addStreamsMutation.isError && (
                  <div className="mx-6 mb-2 p-2 rounded bg-destructive/10 text-sm text-destructive">Failed to add tables.</div>
                )}
                {schemasArr.length === 0 ? (
                  <CardContent><p className="text-sm text-muted-foreground text-center py-4">No discovered schema. Run "Discover Schemas" on the source first.</p></CardContent>
                ) : addableTables.length === 0 ? (
                  <CardContent><p className="text-sm text-muted-foreground text-center py-4">All discovered tables are already added.</p></CardContent>
                ) : (
                  <CardContent className="p-0">
                    {/* Select all */}
                    <div className="p-3 border-b flex items-center gap-2">
                      <input type="checkbox" id="select-all" className="h-4 w-4"
                        checked={addableTables.every((t) => selectedNew[t.key])}
                        onChange={(e) => {
                          const next: Record<string, boolean> = {};
                          if (e.target.checked) addableTables.forEach((t) => { next[t.key] = true; });
                          setSelectedNew(next);
                        }}
                      />
                      <label htmlFor="select-all" className="text-sm font-medium">Select all ({addableTables.length})</label>
                    </div>
                    <div className="divide-y">
                      {addableTables.map((t) => {
                        const isChecked = !!selectedNew[t.key];
                        const cfg = pendingConfigs[t.key] ?? { dest_table: "", dest_schema: "", primary_keys: [], cursor_field: "", columns: t.columns, expanded: false, table_udfs: [] };
                        const pkCols = t.columns.filter((c) => c.is_primary_key);
                        const updateCfg = (patch: any) => setPendingConfigs((prev) => ({ ...prev, [t.key]: { ...cfg, ...patch } }));
                        return (
                          <div key={t.key}>
                            {/* Row */}
                            <div
                              className={`flex items-center gap-3 px-4 py-2.5 cursor-pointer hover:bg-muted/40 ${isChecked ? "bg-muted/20" : ""}`}
                              onClick={() => setSelectedNew({ ...selectedNew, [t.key]: !isChecked })}
                            >
                              <input type="checkbox" checked={isChecked}
                                onChange={(e) => setSelectedNew({ ...selectedNew, [t.key]: e.target.checked })}
                                className="h-4 w-4 rounded" onClick={(e) => e.stopPropagation()} />
                              <button className="flex items-center gap-1 text-muted-foreground hover:text-foreground"
                                disabled={!isChecked}
                                onClick={(e) => { e.stopPropagation(); updateCfg({ expanded: !cfg.expanded }); }}>
                                {cfg.expanded ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                              </button>
                              <span className="font-mono text-sm font-semibold flex-1">
                                {t.schema ? `${t.schema}.` : ""}{t.table}
                              </span>
                              <span className="text-xs text-muted-foreground">PK: <span className="font-mono">{t.pks}</span></span>
                            </div>

                            {/* Expanded config for new table */}
                            {isChecked && cfg.expanded && (
                              <div className="border-t border-b bg-muted/20">
                                {/* Dest + cursor */}
                                <div className="flex items-center gap-3 px-4 py-2 bg-muted/30 text-xs border-b flex-wrap">
                                  <span className="text-muted-foreground w-28 shrink-0">Destination table:</span>
                                  <Input className="h-6 text-xs py-0 w-48" value={cfg.dest_table} onChange={(e) => updateCfg({ dest_table: e.target.value })} placeholder={t.table} />
                                  <span className="text-muted-foreground ml-2 w-28 shrink-0">Dest schema:</span>
                                  <Input className="h-6 text-xs py-0 w-32" value={cfg.dest_schema} onChange={(e) => updateCfg({ dest_schema: e.target.value })} placeholder={t.schema || "public"} />
                                  {addSyncMode === "incremental" && (
                                    <>
                                      <span className="text-muted-foreground ml-2 w-20 shrink-0">Cursor field:</span>
                                      <Input className="h-6 text-xs py-0 w-36" value={cfg.cursor_field} onChange={(e) => updateCfg({ cursor_field: e.target.value })} placeholder="updated_at" />
                                    </>
                                  )}
                                </div>

                                {/* PK selection from columns */}
                                {pkCols.length > 0 && (
                                  <div className="px-4 py-2 flex items-center gap-2 flex-wrap border-b bg-muted/10 text-xs">
                                    <span className="text-muted-foreground font-medium mr-1">Primary Keys:</span>
                                    {t.columns.map((col) => {
                                      const curPKs = cfg.primary_keys.length > 0 ? cfg.primary_keys : pkCols.map((c) => c.column_name);
                                      return (
                                        <label key={col.column_name} className="flex items-center gap-1 px-2 py-0.5 rounded border cursor-pointer hover:bg-muted/50">
                                          <input type="checkbox" className="h-3 w-3"
                                            checked={curPKs.includes(col.column_name)}
                                            onChange={(e) => {
                                              const cur = cfg.primary_keys.length > 0 ? cfg.primary_keys : pkCols.map((c) => c.column_name);
                                              updateCfg({ primary_keys: e.target.checked ? [...cur, col.column_name] : cur.filter((k: string) => k !== col.column_name) });
                                            }}
                                          />
                                          <code>{col.column_name}</code>
                                        </label>
                                      );
                                    })}
                                  </div>
                                )}

                                {/* Column transforms */}
                                {cfg.columns.length > 0 && (
                                  <>
                                    <div className="flex items-center gap-3 px-4 py-1.5 bg-muted/50 text-xs font-medium text-muted-foreground border-b">
                                      <span className="w-44">Column</span>
                                      <span className="w-28">Type</span>
                                      <span className="flex-1">Transforms</span>
                                    </div>
                                    {cfg.columns.map((col: ColumnMeta, ci: number) => (
                                      <ColumnRow
                                        key={col.column_name}
                                        col={col}
                                        udfs={udfs}
                                        onUpdate={(updated) => {
                                          const cols = [...cfg.columns]; cols[ci] = updated;
                                          updateCfg({ columns: cols });
                                        }}
                                      />
                                    ))}
                                  </>
                                )}

                                {/* Table-level UDFs */}
                                <div className="border-t px-4 py-3 bg-amber-50/50 space-y-2">
                                  <div className="flex items-center justify-between">
                                    <span className="text-xs font-semibold text-amber-700">Table-level UDFs</span>
                                    <span className="text-xs text-muted-foreground">Applied to the whole stream after column transforms</span>
                                    <Button size="sm" variant="ghost" className="h-6 text-xs text-amber-700"
                                      onClick={() => updateCfg({ table_udfs: [...(cfg.table_udfs ?? []), { function_name: "", args: "", output_column: "" }] })}>
                                      <Plus className="h-3 w-3 mr-1" />Add UDF
                                    </Button>
                                  </div>
                                  {(cfg.table_udfs ?? []).map((udf: { function_name: string; args: string; output_column: string }, ui: number) => (
                                    <div key={ui} className="flex items-center gap-2 text-xs">
                                      {udfs.length > 0 ? (
                                        <Select value={udf.function_name} onValueChange={(v) => { const upd = [...cfg.table_udfs]; upd[ui] = { ...upd[ui], function_name: v }; updateCfg({ table_udfs: upd }); }}>
                                          <SelectTrigger className="h-6 text-xs w-44"><SelectValue placeholder="Select UDF" /></SelectTrigger>
                                          <SelectContent>{udfs.map((u: any) => <SelectItem key={u.udf_id} value={u.udf_name ?? u.function_name}>{u.udf_name ?? u.function_name}</SelectItem>)}</SelectContent>
                                        </Select>
                                      ) : (
                                        <Input className="h-6 text-xs w-44" value={udf.function_name} placeholder="udf_name"
                                          onChange={(e) => { const upd = [...cfg.table_udfs]; upd[ui] = { ...upd[ui], function_name: e.target.value }; updateCfg({ table_udfs: upd }); }} />
                                      )}
                                      <Input className="h-6 text-xs flex-1" value={udf.args} placeholder="args: col1, col2"
                                        onChange={(e) => { const upd = [...cfg.table_udfs]; upd[ui] = { ...upd[ui], args: e.target.value }; updateCfg({ table_udfs: upd }); }} />
                                      <Input className="h-6 text-xs w-32" value={udf.output_column} placeholder="output_col"
                                        onChange={(e) => { const upd = [...cfg.table_udfs]; upd[ui] = { ...upd[ui], output_column: e.target.value }; updateCfg({ table_udfs: upd }); }} />
                                      <button onClick={() => updateCfg({ table_udfs: cfg.table_udfs.filter((_: any, i: number) => i !== ui) })}
                                        className="text-muted-foreground hover:text-destructive"><X className="h-3 w-3" /></button>
                                    </div>
                                  ))}
                                </div>
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  </CardContent>
                )}
              </Card>
            )}
          </div>
        </TabsContent>

        {/* ── Configuration tab ──────────────────────────────────────── */}
        <TabsContent value="config">
          <Card>
            <CardHeader><CardTitle>Pipeline Configuration</CardTitle></CardHeader>
            <CardContent>
              <form onSubmit={(e) => { e.preventDefault(); updateMutation.mutate(); }} className="space-y-4">
                <div className="space-y-2">
                  <label className="text-sm font-medium">Name *</label>
                  <Input value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} required />
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Sync Mode</label>
                    <Select value={form.sync_mode} onValueChange={(v) => setForm({ ...form, sync_mode: v })}>
                      <SelectTrigger><SelectValue /></SelectTrigger>
                      <SelectContent>
                        <SelectItem value="cdc">CDC (Real-time)</SelectItem>
                        <SelectItem value="incremental">Incremental</SelectItem>
                        <SelectItem value="full_refresh">Full Refresh</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Schedule</label>
                    <Select value={form.schedule_type} onValueChange={(v) => setForm({ ...form, schedule_type: v })}>
                      <SelectTrigger><SelectValue /></SelectTrigger>
                      <SelectContent>
                        <SelectItem value="continuous">Continuous (Streaming)</SelectItem>
                        <SelectItem value="scheduled">Scheduled (Cron)</SelectItem>
                        <SelectItem value="manual">Manual</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </div>
                {form.schedule_type === "scheduled" && (
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Cron Expression</label>
                    <Input value={form.cron_expression} onChange={(e) => setForm({ ...form, cron_expression: e.target.value })} placeholder="*/5 * * * *" />
                  </div>
                )}
                <div className="space-y-2">
                  <label className="text-sm font-medium">Schema Evolution Policy</label>
                  <Select value={form.schema_evolution_policy} onValueChange={(v) => setForm({ ...form, schema_evolution_policy: v })}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="auto_apply">Auto Apply</SelectItem>
                      <SelectItem value="manual_approval">Manual Approval</SelectItem>
                      <SelectItem value="ignore">Ignore</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <label className="text-sm font-medium">Transform Pipeline</label>
                  <Select value={form.transform_pipeline_id || "_none_"} onValueChange={(v) => setForm({ ...form, transform_pipeline_id: v === "_none_" ? "" : v })}>
                    <SelectTrigger><SelectValue placeholder="None" /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="_none_">None</SelectItem>
                      {pipelineList.map((p: any) => <SelectItem key={p.pipeline_id} value={p.pipeline_id}>{p.pipeline_name}</SelectItem>)}
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <label className="text-sm font-medium">Data Quality Policy</label>
                  <Select value={form.dq_policy_id || "_none_"} onValueChange={(v) => setForm({ ...form, dq_policy_id: v === "_none_" ? "" : v })}>
                    <SelectTrigger><SelectValue placeholder="None" /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="_none_">None</SelectItem>
                      {dqPolicyList.map((p: any) => <SelectItem key={p.policy_id} value={p.policy_id}>{p.policy_name}</SelectItem>)}
                    </SelectContent>
                  </Select>
                </div>
                <div className="flex gap-2 pt-4">
                  <Button type="button" variant="ghost" onClick={() => navigate(`/connections/${id}`)}>Cancel</Button>
                  <Button type="submit" disabled={updateMutation.isPending}>{updateMutation.isPending ? "Saving…" : "Save Changes"}</Button>
                </div>
                {updateMutation.isError && <p className="text-sm text-destructive">Failed to save.</p>}
              </form>
            </CardContent>
          </Card>
        </TabsContent>

        {/* ── Resource Limits tab ──────────────────────────────────── */}
        <TabsContent value="limits">
          <Card>
            <CardHeader><CardTitle>Resource Limits</CardTitle></CardHeader>
            <CardContent>
              <form onSubmit={(e) => { e.preventDefault(); updateMutation.mutate(); }} className="space-y-4">
                <div className="grid grid-cols-3 gap-4">
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Max Events / sec</label>
                    <Input value={form.max_events_per_sec} onChange={(e) => setForm({ ...form, max_events_per_sec: e.target.value })} type="number" placeholder="10000" />
                    <p className="text-xs text-muted-foreground">Throttle the CDC event rate</p>
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Max Memory (MB)</label>
                    <Input value={form.max_memory_mb} onChange={(e) => setForm({ ...form, max_memory_mb: e.target.value })} type="number" placeholder="2048" />
                    <p className="text-xs text-muted-foreground">Heap limit for the worker</p>
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Max Workers</label>
                    <Input value={form.max_workers} onChange={(e) => setForm({ ...form, max_workers: e.target.value })} type="number" placeholder="4" />
                    <p className="text-xs text-muted-foreground">Parallel write workers</p>
                  </div>
                </div>
                <div className="flex gap-2 pt-4">
                  <Button type="button" variant="ghost" onClick={() => navigate(`/connections/${id}`)}>Cancel</Button>
                  <Button type="submit" disabled={updateMutation.isPending}>{updateMutation.isPending ? "Saving…" : "Save Resource Limits"}</Button>
                </div>
              </form>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
