import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { useMutation, useQuery } from "@tanstack/react-query";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { CheckCircle, Circle, ChevronDown, ChevronRight, Plus, X, Zap } from "lucide-react";

const STEPS = ["Source", "Destination", "Streams & Transforms", "Config", "Review"];

// ── Transform step types ──────────────────────────────────────────────────────

type TransformType =
  | "cast"
  | "string_op"
  | "math_op"
  | "mask"
  | "json_extract"
  | "json_flatten_inline"
  | "json_flatten_child"
  | "expression"
  | "udf";

interface TransformStep {
  id: string;
  type: TransformType;
  output_column: string;
  // cast
  to_type?: string;
  // string_op
  op?: string;
  params?: Record<string, any>;
  // mask
  strategy?: string;
  // expression
  expression?: string;
  language?: string;
  // udf
  function_name?: string;
  udf_args?: string[];
  // json_flatten_inline
  keep_original?: boolean;
  // json_flatten_child
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

interface StreamConfig {
  name: string;
  schema_name?: string;
  sync_mode: string;
  primary_key?: string;
  cursor_field?: string;
  selected: boolean;
  dest_table_name?: string;
  columns: ColumnMeta[];
  expanded: boolean;
  // table-level UDFs applied after column transforms
  table_udfs: Array<{ function_name: string; args: string; output_column: string }>;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeStepId() {
  return Math.random().toString(36).slice(2, 8);
}

function buildTransformSpec(stream: StreamConfig): Record<string, any> | undefined {
  const steps: any[] = [];

  for (const col of stream.columns) {
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

  for (const udf of stream.table_udfs) {
    if (!udf.function_name) continue;
    steps.push({
      id: makeStepId(), type: "udf",
      function: udf.function_name,
      args: udf.args.split(",").map((s) => s.trim()).filter(Boolean),
      output_column: udf.output_column || udf.function_name + "_result",
    });
  }

  return steps.length > 0 ? { transforms: steps } : undefined;
}

const TRANSFORM_LABEL: Record<TransformType, string> = {
  cast: "Cast Type",
  string_op: "String Op",
  math_op: "Math",
  mask: "Mask",
  json_extract: "JSON Extract",
  json_flatten_inline: "Flatten JSON (inline)",
  json_flatten_child: "Flatten JSON (child table)",
  expression: "SQL Expression",
  udf: "UDF",
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

// ── Inline transform step editor ──────────────────────────────────────────────

function TransformStepEditor({
  step, col, allColumns, udfs, onChange, onRemove,
}: {
  step: TransformStep;
  col: ColumnMeta;
  allColumns: ColumnMeta[];
  udfs: any[];
  onChange: (updated: TransformStep) => void;
  onRemove: () => void;
}) {
  const set = (patch: Partial<TransformStep>) => onChange({ ...step, ...patch });

  return (
    <div className={`rounded-md border px-3 py-2 text-xs space-y-2 ${TRANSFORM_COLOR[step.type]}`}>
      <div className="flex items-center justify-between">
        <span className="font-semibold">{TRANSFORM_LABEL[step.type]}</span>
        <button onClick={onRemove} className="hover:opacity-70"><X className="h-3 w-3" /></button>
      </div>

      {/* Output column name */}
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
            <SelectContent>
              {["string","integer","long","float","double","boolean","timestamp","date"].map((t) => (
                <SelectItem key={t} value={t}>{t}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      )}

      {step.type === "string_op" && (
        <div className="flex items-center gap-2">
          <span className="text-muted-foreground w-20 shrink-0">Operation:</span>
          <Select value={step.op || "upper"} onValueChange={(v) => set({ op: v })}>
            <SelectTrigger className="h-6 text-xs py-0"><SelectValue /></SelectTrigger>
            <SelectContent>
              {["upper","lower","trim","ltrim","rtrim","substring","replace","reverse"].map((o) => (
                <SelectItem key={o} value={o}>{o}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      )}

      {step.type === "string_op" && step.op === "substring" && (
        <div className="grid grid-cols-2 gap-2">
          <div className="flex items-center gap-1">
            <span className="text-muted-foreground w-10 shrink-0">Start:</span>
            <Input className="h-6 text-xs py-0" type="number" value={step.params?.start ?? 0} onChange={(e) => set({ params: { ...step.params, start: parseInt(e.target.value) || 0 } })} />
          </div>
          <div className="flex items-center gap-1">
            <span className="text-muted-foreground w-14 shrink-0">Length:</span>
            <Input className="h-6 text-xs py-0" type="number" value={step.params?.length ?? ""} onChange={(e) => set({ params: { ...step.params, length: parseInt(e.target.value) || undefined } })} />
          </div>
        </div>
      )}

      {step.type === "math_op" && (
        <div className="flex items-center gap-2">
          <span className="text-muted-foreground w-20 shrink-0">Expression:</span>
          <Input className="h-6 text-xs py-0 font-mono" value={step.expression || ""} onChange={(e) => set({ expression: e.target.value })} placeholder="amount * 1.18" />
        </div>
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
              <SelectContent>
                <SelectItem value="spark_sql">SQL Expression</SelectItem>
                <SelectItem value="sel">SEL (simple)</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <Input className="h-6 text-xs py-0 font-mono" value={step.expression || ""} onChange={(e) => set({ expression: e.target.value })} placeholder="CASE WHEN amount > 0 THEN 'pos' ELSE 'neg' END" />
        </>
      )}

      {step.type === "json_extract" && (
        <div className="flex items-center gap-2">
          <span className="text-muted-foreground w-20 shrink-0">JSON path:</span>
          <Input className="h-6 text-xs py-0 font-mono" value={step.params?.json_path || "$"} onChange={(e) => set({ params: { ...step.params, json_path: e.target.value } })} placeholder="$.amount" />
        </div>
      )}

      {step.type === "json_flatten_inline" && (
        <div className="flex items-center gap-2">
          <input type="checkbox" checked={step.keep_original ?? false} onChange={(e) => set({ keep_original: e.target.checked })} id={`keep-${step.id}`} />
          <label htmlFor={`keep-${step.id}`} className="text-muted-foreground">Keep original JSON column</label>
        </div>
      )}

      {step.type === "json_flatten_child" && (
        <>
          <div className="flex items-center gap-2">
            <span className="text-muted-foreground w-24 shrink-0">Child table:</span>
            <Input className="h-6 text-xs py-0" value={step.child_table || ""} onChange={(e) => set({ child_table: e.target.value })} placeholder="line_items" />
          </div>
          <div className="flex items-center gap-2">
            <span className="text-muted-foreground w-24 shrink-0">Array path:</span>
            <Input className="h-6 text-xs py-0 font-mono" value={step.array_path || "$[*]"} onChange={(e) => set({ array_path: e.target.value })} placeholder="$[*]" />
          </div>
        </>
      )}

      {step.type === "udf" && (
        <>
          <div className="flex items-center gap-2">
            <span className="text-muted-foreground w-20 shrink-0">Function:</span>
            {udfs.length > 0 ? (
              <Select value={step.function_name || ""} onValueChange={(v) => set({ function_name: v })}>
                <SelectTrigger className="h-6 text-xs py-0"><SelectValue placeholder="Select UDF" /></SelectTrigger>
                <SelectContent>
                  {udfs.map((u: any) => <SelectItem key={u.udf_id ?? u.id} value={u.function_name ?? u.name}>{u.function_name ?? u.name}</SelectItem>)}
                </SelectContent>
              </Select>
            ) : (
              <Input className="h-6 text-xs py-0" value={step.function_name || ""} onChange={(e) => set({ function_name: e.target.value })} placeholder="compute_risk_score" />
            )}
          </div>
          <div className="flex items-center gap-2">
            <span className="text-muted-foreground w-20 shrink-0">Args (cols):</span>
            <Input className="h-6 text-xs py-0" value={step.udf_args?.join(", ") || ""} onChange={(e) => set({ udf_args: e.target.value.split(",").map((s) => s.trim()).filter(Boolean) })} placeholder="amount_usd, country_code" />
          </div>
        </>
      )}
    </div>
  );
}

// ── Column row with inline transforms ─────────────────────────────────────────

function ColumnRow({
  col, allColumns, udfs, streamIdx, colIdx, onUpdate,
}: {
  col: ColumnMeta;
  allColumns: ColumnMeta[];
  udfs: any[];
  streamIdx: number;
  colIdx: number;
  onUpdate: (updated: ColumnMeta) => void;
}) {
  const [addingType, setAddingType] = useState<TransformType | "">("");

  const addTransform = () => {
    if (!addingType) return;
    const step: TransformStep = {
      id: makeStepId(), type: addingType as TransformType,
      output_column: col.column_name,
    };
    onUpdate({ ...col, transforms: [...col.transforms, step] });
    setAddingType("");
  };

  const updateStep = (idx: number, updated: TransformStep) => {
    const steps = [...col.transforms];
    steps[idx] = updated;
    onUpdate({ ...col, transforms: steps });
  };

  const removeStep = (idx: number) => {
    onUpdate({ ...col, transforms: col.transforms.filter((_, i) => i !== idx) });
  };

  return (
    <div className={`px-4 py-2 border-b last:border-0 ${col.transforms.length > 0 ? "bg-muted/20" : ""}`}>
      <div className="flex items-start gap-3">
        {/* Column info */}
        <div className="w-44 shrink-0 flex items-center gap-1.5">
          {col.is_primary_key && <span title="Primary Key" className="text-xs text-amber-600">🔑</span>}
          <span className="font-mono text-sm font-medium">{col.column_name}</span>
        </div>
        <div className="w-28 shrink-0">
          <span className={`text-xs px-1.5 py-0.5 rounded font-mono ${col.is_json_candidate ? "bg-teal-100 text-teal-700" : "bg-muted text-muted-foreground"}`}>
            {col.data_type}
          </span>
        </div>

        {/* Applied transforms */}
        <div className="flex-1 space-y-1">
          {col.transforms.map((step, si) => (
            <TransformStepEditor
              key={step.id}
              step={step}
              col={col}
              allColumns={allColumns}
              udfs={udfs}
              onChange={(updated) => updateStep(si, updated)}
              onRemove={() => removeStep(si)}
            />
          ))}

          {/* Add transform row */}
          <div className="flex items-center gap-2 mt-1">
            <Select value={addingType} onValueChange={(v) => setAddingType(v as TransformType)}>
              <SelectTrigger className="h-7 text-xs w-52">
                <SelectValue placeholder="+ Add transform…" />
              </SelectTrigger>
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

// ── Main wizard component ─────────────────────────────────────────────────────

export function CreateConnectionWizard() {
  const navigate = useNavigate();
  const [step, setStep] = useState(0);
  const [sourceId, setSourceId] = useState("");
  const [destinationId, setDestinationId] = useState("");
  const [streamConfigs, setStreamConfigs] = useState<StreamConfig[]>([]);
  const [activateImmediately, setActivateImmediately] = useState(true);
  const [config, setConfig] = useState({
    name: "",
    sync_mode: "cdc",
    schedule: "continuous",
    cron: "",
    dq_policy_id: "",
    schema_policy: "auto_apply",
    generate_dag: false,
    max_events_sec: "",
    max_memory_mb: "",
  });

  const { data: sources = [] } = useQuery({ queryKey: ["sources"], queryFn: () => fetchList("/sources", "sources") });
  const { data: destinations = [] } = useQuery({ queryKey: ["destinations"], queryFn: () => fetchList("/destinations", "destinations") });
  const { data: policies = [] } = useQuery({ queryKey: ["data-quality", "policies"], queryFn: () => fetchList("/data-quality/policies", "policies") });
  const { data: udfs = [] } = useQuery({ queryKey: ["udfs"], queryFn: () => fetchList("/udfs", "udfs").catch(() => []) });

  const { data: availableStreams, isFetching: streamsLoading } = useQuery({
    queryKey: ["sources", sourceId, "schemas"],
    queryFn: async () => {
      const res = await api.get(`/sources/${sourceId}/schemas`);
      // The /schemas endpoint returns {schemas: [{schema_name, tables: [{table_name, columns: [...], primary_keys: [...]}]}]}
      const schemas = res.data?.schemas ?? [];
      const tables: any[] = [];
      for (const schema of schemas) {
        for (const tbl of schema.tables ?? []) {
          tables.push({
            name: tbl.table_name ?? tbl.name ?? tbl,
            schema_name: tbl.schema_name ?? schema.schema_name ?? "",
            primary_key: (tbl.primary_keys ?? [])[0] ?? "",
            cursor_field: "",
            columns: (tbl.columns ?? []).map((c: any): ColumnMeta => ({
              column_name: c.column_name,
              data_type: c.data_type ?? "unknown",
              is_primary_key: c.is_primary_key ?? false,
              is_nullable: c.is_nullable ?? true,
              is_json_candidate: c.is_json_candidate ?? false,
              transforms: [],
            })),
          });
        }
      }
      // Fallback: API returns {tables: [...]} flat (not nested in schemas)
      if (tables.length === 0) {
        const flat = res.data?.tables ?? res.data ?? [];
        return (Array.isArray(flat) ? flat : []).map((t: any) => ({
          name: t.name ?? t.table_name ?? t,
          schema_name: t.schema_name ?? t.schema ?? "",
          primary_key: t.primary_key ?? (Array.isArray(t.primary_keys) ? t.primary_keys[0] : "") ?? "",
          cursor_field: t.cursor_field ?? "",
          columns: (t.columns ?? []).map((c: any): ColumnMeta => ({
            column_name: c.column_name,
            data_type: c.data_type ?? "unknown",
            is_primary_key: c.is_primary_key ?? false,
            is_nullable: c.is_nullable ?? true,
            is_json_candidate: c.is_json_candidate ?? false,
            transforms: [],
          })),
        }));
      }
      return tables;
    },
    enabled: !!sourceId && step >= 2,
  });

  const initStreamConfigs = () => {
    if (!availableStreams) return;
    if (streamConfigs.length === 0) {
      // First load
      setStreamConfigs(
        availableStreams.map((t: any): StreamConfig => ({
          name: t.name,
          schema_name: t.schema_name,
          sync_mode: "cdc",
          primary_key: t.primary_key,
          cursor_field: t.cursor_field,
          selected: false,
          dest_table_name: t.name,
          columns: t.columns ?? [],
          expanded: false,
          table_udfs: [],
        }))
      );
    } else {
      // Columns may have been empty (discovery ran after init) — backfill columns only
      setStreamConfigs((prev) =>
        prev.map((s) => {
          if (s.columns.length > 0) return s;
          const fresh = availableStreams.find((t: any) => t.name === s.name);
          if (!fresh || !fresh.columns?.length) return s;
          return { ...s, columns: fresh.columns };
        })
      );
    }
  };

  // (Re)initialise stream configs whenever availableStreams arrives or changes.
  // On first load: populate list. On re-load after discovery: backfill columns.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => {
    if (step < 2 || !availableStreams) return;
    initStreamConfigs();
  }, [availableStreams]);

  const updateStream = (idx: number, patch: Partial<StreamConfig>) => {
    setStreamConfigs((prev) => {
      const next = [...prev];
      next[idx] = { ...next[idx], ...patch };
      return next;
    });
  };

  const updateColumn = (streamIdx: number, colIdx: number, col: ColumnMeta) => {
    setStreamConfigs((prev) => {
      const next = [...prev];
      const cols = [...next[streamIdx].columns];
      cols[colIdx] = col;
      next[streamIdx] = { ...next[streamIdx], columns: cols };
      return next;
    });
  };

  const selectedStreams = streamConfigs.filter((s) => s.selected);

  const createMutation = useMutation({
    mutationFn: () =>
      api.post("/connections", {
        connection_name: config.name,
        source_id: sourceId,
        destination_id: destinationId,
        sync_mode: config.sync_mode,
        sync_type: config.schedule === "continuous" ? "CDC" : "BATCH",
        sync_frequency: config.schedule === "cron" ? config.cron : config.schedule === "manual" ? "manual" : null,
        streams: selectedStreams.map((s) => {
          const transform_steps = buildTransformSpec(s);
          return {
            stream_name: s.name,
            stream_namespace: s.schema_name || null,
            source_table_name: s.name,
            source_schema_name: s.schema_name || null,
            destination_table_name: s.dest_table_name || s.name,
            sync_mode: s.sync_mode,
            primary_keys: s.primary_key ? [s.primary_key] : [],
            cursor_field: s.cursor_field || null,
            is_enabled: true,
            transform_steps: transform_steps ?? null,
          };
        }),
        resource_limits:
          config.max_events_sec || config.max_memory_mb
            ? {
                max_events_sec: config.max_events_sec ? Number(config.max_events_sec) : undefined,
                max_memory_mb: config.max_memory_mb ? Number(config.max_memory_mb) : undefined,
              }
            : {},
        status: activateImmediately ? "active" : "draft",
      }),
    onSuccess: (res) => navigate(`/connections/${res.data.connection_id}`),
  });

  const selectedSource = sources.find((s: any) => s.source_id === sourceId);
  const selectedDest = destinations.find((d: any) => d.destination_id === destinationId);

  const sourcesByType = sources.reduce((acc: Record<string, any[]>, s: any) => {
    const type = s.connector_definition_type ?? "other";
    if (!acc[type]) acc[type] = [];
    acc[type].push(s);
    return acc;
  }, {});

  const canNext = () => {
    if (step === 0) return !!sourceId;
    if (step === 1) return !!destinationId;
    if (step === 2) return selectedStreams.length > 0;
    if (step === 3) return !!config.name;
    return true;
  };

  const totalTransformCount = selectedStreams.reduce(
    (acc, s) => acc + s.columns.reduce((a2, c) => a2 + c.transforms.length, 0) + s.table_udfs.filter((u) => u.function_name).length,
    0
  );

  return (
    <div className="space-y-6 max-w-5xl mx-auto">
      <h1 className="text-2xl font-bold">Create Connection</h1>

      {/* Step Indicator */}
      <div className="flex items-center gap-2 flex-wrap">
        {STEPS.map((s, i) => (
          <div key={s} className="flex items-center gap-2">
            {i > 0 && <div className={`h-0.5 w-8 ${i <= step ? "bg-primary" : "bg-border"}`} />}
            <div className="flex items-center gap-1.5">
              {i < step ? <CheckCircle className="h-5 w-5 text-primary" /> : i === step ? <Circle className="h-5 w-5 text-primary fill-primary" /> : <Circle className="h-5 w-5 text-muted-foreground" />}
              <span className={`text-sm ${i === step ? "font-semibold" : "text-muted-foreground"}`}>{s}</span>
            </div>
          </div>
        ))}
      </div>

      {/* ── Step 1: Source ── */}
      {step === 0 && (
        <Card>
          <CardHeader><CardTitle>Select Source</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            {Object.entries(sourcesByType).length === 0 && <p className="text-muted-foreground">No sources available. Create one first.</p>}
            {Object.entries(sourcesByType).map(([type, srcs]) => (
              <div key={type}>
                <h4 className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-2">{type}</h4>
                <div className="space-y-2">
                  {(srcs as any[]).map((s: any) => (
                    <label key={s.source_id} className={`flex items-center gap-3 rounded-lg border p-4 cursor-pointer transition-colors ${sourceId === s.source_id ? "border-primary bg-primary/5 ring-1 ring-primary" : "hover:bg-muted/50"}`}>
                      <input type="radio" name="source" value={s.source_id} checked={sourceId === s.source_id} onChange={() => { setSourceId(s.source_id); setStreamConfigs([]); }} className="sr-only" />
                      <div className={`w-4 h-4 rounded-full border-2 flex items-center justify-center ${sourceId === s.source_id ? "border-primary" : "border-muted-foreground"}`}>
                        {sourceId === s.source_id && <div className="w-2 h-2 rounded-full bg-primary" />}
                      </div>
                      <div className="flex-1">
                        <span className="font-medium">{s.source_name}</span>
                        <div className="text-xs text-muted-foreground mt-0.5">{s.host && <span>{s.host}</span>}{s.database_name && <span> / {s.database_name}</span>}</div>
                      </div>
                      <Badge variant={s.status === "active" ? "success" : "secondary"}>{s.status}</Badge>
                    </label>
                  ))}
                </div>
              </div>
            ))}
          </CardContent>
        </Card>
      )}

      {/* ── Step 2: Destination ── */}
      {step === 1 && (
        <Card>
          <CardHeader><CardTitle>Select Destination</CardTitle></CardHeader>
          <CardContent className="space-y-2">
            {destinations.length === 0 && <p className="text-muted-foreground">No destinations available. Create one first.</p>}
            {destinations.map((d: any) => (
              <label key={d.destination_id} className={`flex items-center gap-3 rounded-lg border p-4 cursor-pointer transition-colors ${destinationId === d.destination_id ? "border-primary bg-primary/5 ring-1 ring-primary" : "hover:bg-muted/50"}`}>
                <input type="radio" name="dest" value={d.destination_id} checked={destinationId === d.destination_id} onChange={() => setDestinationId(d.destination_id)} className="sr-only" />
                <div className={`w-4 h-4 rounded-full border-2 flex items-center justify-center ${destinationId === d.destination_id ? "border-primary" : "border-muted-foreground"}`}>
                  {destinationId === d.destination_id && <div className="w-2 h-2 rounded-full bg-primary" />}
                </div>
                <div className="flex-1">
                  <span className="font-medium">{d.destination_name}</span>
                  <div className="text-xs text-muted-foreground mt-0.5">{d.host} {d.database_name && `/ ${d.database_name}`}</div>
                </div>
                <Badge variant={d.status === "active" ? "success" : "secondary"}>{d.status}</Badge>
              </label>
            ))}
          </CardContent>
        </Card>
      )}

      {/* ── Step 3: Streams & Column-Level Transforms ── */}
      {step === 2 && (
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-lg font-semibold">Select Tables & Configure Transforms</h2>
              <p className="text-sm text-muted-foreground">Select tables to sync. Expand each table to configure per-column transforms.</p>
            </div>
            <div className="flex items-center gap-3">
              {totalTransformCount > 0 && (
                <Badge variant="outline" className="gap-1">
                  <Zap className="h-3 w-3" />{totalTransformCount} transform{totalTransformCount !== 1 ? "s" : ""}
                </Badge>
              )}
              <span className="text-sm text-muted-foreground">{selectedStreams.length} selected</span>
              <Button variant="outline" size="sm" onClick={() => setStreamConfigs(streamConfigs.map((s) => ({ ...s, selected: true })))}>
                Select All
              </Button>
            </div>
          </div>

          {streamsLoading ? (
            <Card className="p-8 text-center text-muted-foreground">Discovering tables from source…</Card>
          ) : streamConfigs.length === 0 ? (
            <Card className="p-8 text-center text-muted-foreground">No tables discovered. You can add streams after creation.</Card>
          ) : (
            <div className="space-y-2">
              {streamConfigs.map((stream, idx) => {
                const transformCount = stream.columns.reduce((a, c) => a + c.transforms.length, 0) + stream.table_udfs.filter((u) => u.function_name).length;
                return (
                  <Card key={stream.name} className={stream.selected ? "ring-1 ring-primary" : ""}>
                    {/* Table header row */}
                    <div className="flex items-center gap-3 px-4 py-3">
                      <input
                        type="checkbox"
                        checked={stream.selected}
                        onChange={(e) => updateStream(idx, { selected: e.target.checked, expanded: e.target.checked ? stream.expanded : false })}
                        className="h-4 w-4"
                      />
                      <button
                        className="flex items-center gap-1 text-muted-foreground hover:text-foreground disabled:opacity-30"
                        disabled={!stream.selected}
                        onClick={() => updateStream(idx, { expanded: !stream.expanded })}
                      >
                        {stream.expanded ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                      </button>
                      <span className="font-mono text-sm font-semibold flex-1">
                        {stream.schema_name ? `${stream.schema_name}.` : ""}{stream.name}
                      </span>
                      {stream.selected && transformCount > 0 && (
                        <Badge variant="outline" className="text-xs gap-1">
                          <Zap className="h-3 w-3" />{transformCount}
                        </Badge>
                      )}
                      <Select
                        value={stream.sync_mode}
                        onValueChange={(v) => updateStream(idx, { sync_mode: v })}
                      >
                        <SelectTrigger className="h-7 text-xs w-32"><SelectValue /></SelectTrigger>
                        <SelectContent>
                          <SelectItem value="cdc">CDC (realtime)</SelectItem>
                          <SelectItem value="incremental">Incremental</SelectItem>
                          <SelectItem value="full_refresh">Full Refresh</SelectItem>
                        </SelectContent>
                      </Select>
                      <span className="text-xs text-muted-foreground w-20 text-right">
                        PK: <span className="font-mono">{stream.primary_key || "—"}</span>
                      </span>
                    </div>

                    {/* Expanded: columns + transforms */}
                    {stream.selected && stream.expanded && (
                      <div className="border-t">
                        {/* Dest table name override */}
                        <div className="flex items-center gap-3 px-4 py-2 bg-muted/30 text-xs border-b">
                          <span className="text-muted-foreground w-32 shrink-0">Destination table:</span>
                          <Input
                            className="h-6 text-xs py-0 max-w-xs"
                            value={stream.dest_table_name ?? stream.name}
                            onChange={(e) => updateStream(idx, { dest_table_name: e.target.value })}
                          />
                          <span className="text-muted-foreground ml-4 w-20 shrink-0">Cursor field:</span>
                          <Input
                            className="h-6 text-xs py-0 max-w-xs"
                            value={stream.cursor_field ?? ""}
                            onChange={(e) => updateStream(idx, { cursor_field: e.target.value })}
                            placeholder={stream.sync_mode === "cdc" ? "(auto — binlog)" : "updated_at"}
                          />
                        </div>

                        {/* Column list with transforms */}
                        {stream.columns.length === 0 ? (
                          <div className="px-4 py-3 text-xs text-muted-foreground">
                            No column metadata available. Run schema discovery on the source first.
                          </div>
                        ) : (
                          <>
                            {/* Header */}
                            <div className="flex items-center gap-3 px-4 py-1.5 bg-muted/50 text-xs font-medium text-muted-foreground border-b">
                              <span className="w-44">Column</span>
                              <span className="w-28">Type</span>
                              <span className="flex-1">Transforms</span>
                            </div>
                            {stream.columns.map((col, ci) => (
                              <ColumnRow
                                key={col.column_name}
                                col={col}
                                allColumns={stream.columns}
                                udfs={udfs as any[]}
                                streamIdx={idx}
                                colIdx={ci}
                                onUpdate={(updated) => updateColumn(idx, ci, updated)}
                              />
                            ))}
                          </>
                        )}

                        {/* Table-level UDFs */}
                        <div className="border-t px-4 py-3 bg-amber-50/50 space-y-2">
                          <div className="flex items-center justify-between">
                            <span className="text-xs font-semibold text-amber-700">Table-level UDFs</span>
                            <span className="text-xs text-muted-foreground">Applied to the whole stream after column transforms</span>
                            <Button
                              size="sm"
                              variant="ghost"
                              className="h-6 text-xs text-amber-700"
                              onClick={() => updateStream(idx, { table_udfs: [...stream.table_udfs, { function_name: "", args: "", output_column: "" }] })}
                            >
                              <Plus className="h-3 w-3 mr-1" />Add UDF
                            </Button>
                          </div>
                          {stream.table_udfs.map((udf, ui) => (
                            <div key={ui} className="flex items-center gap-2 text-xs">
                              {udfs.length > 0 ? (
                                <Select value={udf.function_name} onValueChange={(v) => {
                                  const upd = [...stream.table_udfs]; upd[ui] = { ...upd[ui], function_name: v };
                                  updateStream(idx, { table_udfs: upd });
                                }}>
                                  <SelectTrigger className="h-6 text-xs w-44"><SelectValue placeholder="Select UDF" /></SelectTrigger>
                                  <SelectContent>
                                    {(udfs as any[]).map((u: any) => <SelectItem key={u.udf_id ?? u.id} value={u.function_name ?? u.name}>{u.function_name ?? u.name}</SelectItem>)}
                                  </SelectContent>
                                </Select>
                              ) : (
                                <Input className="h-6 text-xs w-44" value={udf.function_name} placeholder="udf_name" onChange={(e) => { const upd = [...stream.table_udfs]; upd[ui] = { ...upd[ui], function_name: e.target.value }; updateStream(idx, { table_udfs: upd }); }} />
                              )}
                              <Input className="h-6 text-xs flex-1" value={udf.args} placeholder="args: col1, col2" onChange={(e) => { const upd = [...stream.table_udfs]; upd[ui] = { ...upd[ui], args: e.target.value }; updateStream(idx, { table_udfs: upd }); }} />
                              <Input className="h-6 text-xs w-32" value={udf.output_column} placeholder="output_col" onChange={(e) => { const upd = [...stream.table_udfs]; upd[ui] = { ...upd[ui], output_column: e.target.value }; updateStream(idx, { table_udfs: upd }); }} />
                              <button onClick={() => updateStream(idx, { table_udfs: stream.table_udfs.filter((_, i) => i !== ui) })} className="text-muted-foreground hover:text-destructive"><X className="h-3 w-3" /></button>
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
        </div>
      )}

      {/* ── Step 4: Pipeline Configuration ── */}
      {step === 3 && (
        <Card>
          <CardHeader><CardTitle>Pipeline Configuration</CardTitle></CardHeader>
          <CardContent className="space-y-5">
            <div className="space-y-2">
              <label className="text-sm font-medium">Connection Name *</label>
              <Input value={config.name} onChange={(e) => setConfig({ ...config, name: e.target.value })} placeholder="my_connection" />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Sync Mode</label>
              <div className="flex gap-4 flex-wrap">
                {([["cdc", "CDC (real-time)"], ["incremental", "Incremental"], ["full_refresh", "Full Refresh"]] as const).map(([val, label]) => (
                  <label key={val} className="flex items-center gap-2 text-sm cursor-pointer">
                    <input type="radio" name="sync_mode" value={val} checked={config.sync_mode === val} onChange={() => setConfig({ ...config, sync_mode: val })} />
                    {label}
                  </label>
                ))}
              </div>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Schedule</label>
              <div className="space-y-2">
                {([["continuous", "Continuous (streaming)"], ["cron", "Scheduled (Cron)"], ["manual", "Manual trigger only"]] as const).map(([val, label]) => (
                  <label key={val} className="flex items-center gap-2 text-sm cursor-pointer">
                    <input type="radio" name="schedule" value={val} checked={config.schedule === val} onChange={() => setConfig({ ...config, schedule: val })} />
                    {label}
                  </label>
                ))}
              </div>
              {config.schedule === "cron" && (
                <div className="ml-6 space-y-2 mt-2">
                  <Input value={config.cron} onChange={(e) => setConfig({ ...config, cron: e.target.value })} placeholder="*/15 * * * *" />
                  <label className="flex items-center gap-2 text-sm cursor-pointer">
                    <input type="checkbox" checked={config.generate_dag} onChange={(e) => setConfig({ ...config, generate_dag: e.target.checked })} />
                    Generate Airflow DAG automatically
                  </label>
                </div>
              )}
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Data Quality Policy (optional)</label>
              <Select value={config.dq_policy_id || "none"} onValueChange={(v) => setConfig({ ...config, dq_policy_id: v === "none" ? "" : v })}>
                <SelectTrigger><SelectValue placeholder="None" /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="none">None</SelectItem>
                  {policies.map((p: any) => <SelectItem key={p.id ?? p.policy_id} value={p.id ?? p.policy_id}>{p.name ?? p.policy_name}</SelectItem>)}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Schema Evolution Policy</label>
              <div className="flex gap-4">
                {([["auto_apply", "Auto Apply"], ["manual", "Manual Approval"]] as const).map(([val, label]) => (
                  <label key={val} className="flex items-center gap-2 text-sm cursor-pointer">
                    <input type="radio" name="schema_policy" value={val} checked={config.schema_policy === val} onChange={() => setConfig({ ...config, schema_policy: val })} />
                    {label}
                  </label>
                ))}
              </div>
            </div>
            <details className="group">
              <summary className="text-sm font-medium cursor-pointer text-muted-foreground hover:text-foreground">▸ Resource Limits (optional)</summary>
              <div className="mt-3 grid grid-cols-2 gap-4">
                <div className="space-y-1"><label className="text-xs text-muted-foreground">Max events/sec</label><Input type="number" value={config.max_events_sec} onChange={(e) => setConfig({ ...config, max_events_sec: e.target.value })} placeholder="10000" /></div>
                <div className="space-y-1"><label className="text-xs text-muted-foreground">Max memory (MB)</label><Input type="number" value={config.max_memory_mb} onChange={(e) => setConfig({ ...config, max_memory_mb: e.target.value })} placeholder="2048" /></div>
              </div>
            </details>
          </CardContent>
        </Card>
      )}

      {/* ── Step 5: Review ── */}
      {step === 4 && (
        <Card>
          <CardHeader><CardTitle>Review Connection</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            <div className="rounded-lg border p-5 space-y-3">
              <h3 className="font-semibold text-lg">{config.name || "Unnamed Connection"}</h3>
              <dl className="grid grid-cols-2 gap-y-3 gap-x-6 text-sm">
                <div><dt className="text-muted-foreground">Source</dt><dd className="font-medium">{selectedSource?.source_name ?? sourceId}</dd></div>
                <div><dt className="text-muted-foreground">Destination</dt><dd className="font-medium">{selectedDest?.destination_name ?? destinationId}</dd></div>
                <div><dt className="text-muted-foreground">Sync Mode</dt><dd className="font-medium">{config.sync_mode.toUpperCase()} ({config.schedule})</dd></div>
                <div><dt className="text-muted-foreground">Tables</dt><dd className="font-medium">{selectedStreams.length} streams</dd></div>
                {totalTransformCount > 0 && <div><dt className="text-muted-foreground">Transforms</dt><dd className="font-medium">{totalTransformCount} steps across {selectedStreams.filter((s) => s.columns.some((c) => c.transforms.length > 0) || s.table_udfs.some((u) => u.function_name)).length} tables</dd></div>}
                <div><dt className="text-muted-foreground">DQ Policy</dt><dd className="font-medium">{config.dq_policy_id ? policies.find((p: any) => (p.id ?? p.policy_id) === config.dq_policy_id)?.name ?? config.dq_policy_id : "None"}</dd></div>
                <div><dt className="text-muted-foreground">Schema Evolution</dt><dd className="font-medium">{config.schema_policy === "auto_apply" ? "Auto Apply" : "Manual Approval"}</dd></div>
              </dl>
              {selectedStreams.length > 0 && (
                <div className="pt-3 border-t space-y-2">
                  <p className="text-xs font-medium text-muted-foreground">Selected Tables:</p>
                  {selectedStreams.map((s) => {
                    const tc = s.columns.reduce((a, c) => a + c.transforms.length, 0) + s.table_udfs.filter((u) => u.function_name).length;
                    return (
                      <div key={s.name} className="flex items-center gap-2">
                        <Badge variant="outline" className="font-mono text-xs">{s.schema_name ? `${s.schema_name}.` : ""}{s.name}</Badge>
                        <span className="text-xs text-muted-foreground">→ {s.dest_table_name || s.name}</span>
                        {tc > 0 && <Badge variant="secondary" className="text-xs gap-1"><Zap className="h-3 w-3" />{tc} transforms</Badge>}
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
            <label className="flex items-center gap-2 text-sm cursor-pointer">
              <input type="checkbox" checked={activateImmediately} onChange={(e) => setActivateImmediately(e.target.checked)} />
              Activate immediately after creation
            </label>
            {createMutation.isError && (
              <p className="text-sm text-destructive">Failed to create connection. Please check your configuration.</p>
            )}
          </CardContent>
        </Card>
      )}

      {/* Navigation */}
      <div className="flex justify-between pt-2">
        <div className="flex gap-2">
          <Button variant="outline" onClick={() => (step > 0 ? setStep(step - 1) : navigate("/connections"))}>
            {step === 0 ? "Cancel" : "← Back"}
          </Button>
          {step > 0 && <Button variant="ghost" onClick={() => navigate("/connections")}>Cancel</Button>}
        </div>
        <Button
          onClick={() => { if (step < 4) setStep(step + 1); else createMutation.mutate(); }}
          disabled={!canNext() || (step === 4 && createMutation.isPending)}
        >
          {step === 4 ? (createMutation.isPending ? "Creating…" : activateImmediately ? "Create & Activate" : "Create Connection") : "Next →"}
        </Button>
      </div>
    </div>
  );
}
