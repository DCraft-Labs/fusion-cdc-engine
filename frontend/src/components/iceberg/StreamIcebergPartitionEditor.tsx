import React from "react";
import { PartitionField, PartitionTransform } from "../../lib/iceberg-config";

interface Props {
  partitionSpec: PartitionField[];
  setPartitionSpec: (spec: PartitionField[]) => void;
  identifierFields: string[];
  setIdentifierFields: (fields: string[]) => void;
  availableColumns: string[];
  sourcePrimaryKeys?: string[];
}

const TRANSFORMS: PartitionTransform[] = ["identity", "year", "month", "day", "hour", "bucket", "truncate"];

export default function StreamIcebergPartitionEditor({
  partitionSpec,
  setPartitionSpec,
  identifierFields,
  setIdentifierFields,
  availableColumns,
  sourcePrimaryKeys = [],
}: Props) {
  const addRow = () => setPartitionSpec([
    ...partitionSpec,
    { source_column: availableColumns[0] ?? "", transform: "day", name: "" },
  ]);
  const updateRow = (i: number, patch: Partial<PartitionField>) =>
    setPartitionSpec(partitionSpec.map((p, idx) => (idx === i ? { ...p, ...patch } : p)));
  const removeRow = (i: number) => setPartitionSpec(partitionSpec.filter((_, idx) => idx !== i));

  return (
    <div style={{ border: "1px solid #e5e7eb", borderRadius: 8, padding: 12, marginTop: 12, background: "#fafafa" }}>
      <h4 style={{ marginTop: 0, marginBottom: 8, fontSize: 14 }}>Iceberg partition spec</h4>
      <p style={{ fontSize: 12, color: "#6b7280", marginTop: 0 }}>
        Prefer <code>day(event_ts)</code> or <code>bucket(16-128, pk)</code>. Avoid <code>hour</code> on
        high-volume CDC — it produces small files.
      </p>

      {partitionSpec.length === 0 && (
        <div style={{ fontSize: 13, color: "#6b7280", marginBottom: 8 }}>No partition fields (unpartitioned table).</div>
      )}
      {partitionSpec.map((p, i) => (
        <div key={i} style={{ display: "flex", gap: 8, marginBottom: 6, alignItems: "center" }}>
          <select value={p.source_column} onChange={(e) => updateRow(i, { source_column: e.target.value })} style={{ flex: 1, padding: "6px" }}>
            {availableColumns.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
          <select value={p.transform} onChange={(e) => updateRow(i, { transform: e.target.value as PartitionTransform })} style={{ flex: 1, padding: "6px" }}>
            {TRANSFORMS.map((t) => <option key={t} value={t}>{t}</option>)}
          </select>
          {(p.transform === "bucket" || p.transform === "truncate") && (
            <input
              type="number"
              value={p.width ?? 16}
              onChange={(e) => updateRow(i, { width: Number(e.target.value) })}
              placeholder="width"
              style={{ width: 80, padding: "6px" }}
            />
          )}
          <input
            value={p.name ?? ""}
            onChange={(e) => updateRow(i, { name: e.target.value })}
            placeholder="optional name"
            style={{ flex: 1, padding: "6px" }}
          />
          <button type="button" onClick={() => removeRow(i)} style={{ padding: "6px 10px" }}>Remove</button>
        </div>
      ))}
      <button type="button" onClick={addRow} style={{ marginTop: 6, padding: "6px 12px" }}>+ Add partition field</button>

      <h4 style={{ marginTop: 16, marginBottom: 8, fontSize: 14 }}>Identifier fields (PK for upsert)</h4>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
        {availableColumns.map((c) => {
          const checked = identifierFields.includes(c);
          return (
            <label key={c} style={{ fontSize: 13 }}>
              <input
                type="checkbox"
                checked={checked}
                onChange={(e) =>
                  setIdentifierFields(e.target.checked
                    ? [...identifierFields, c]
                    : identifierFields.filter((f) => f !== c))
                }
              />{" "}
              {c}
              {sourcePrimaryKeys.includes(c) && <span style={{ color: "#6b7280" }}> (PK)</span>}
            </label>
          );
        })}
      </div>
    </div>
  );
}
