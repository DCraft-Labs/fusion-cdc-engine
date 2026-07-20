import { type PartitionField, type PartitionTransform } from "@/lib/iceberg-config";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectTrigger,
  SelectValue,
  SelectContent,
  SelectItem,
} from "@/components/ui/select";
import { X, Plus } from "lucide-react";

interface Props {
  partitionSpec: PartitionField[];
  setPartitionSpec: (spec: PartitionField[]) => void;
  identifierFields: string[];
  setIdentifierFields: (fields: string[]) => void;
  availableColumns: string[];
  sourcePrimaryKeys?: string[];
}

const TRANSFORMS: PartitionTransform[] = [
  "identity",
  "year",
  "month",
  "day",
  "hour",
  "bucket",
  "truncate",
];

export default function StreamIcebergPartitionEditor({
  partitionSpec,
  setPartitionSpec,
  identifierFields,
  setIdentifierFields,
  availableColumns,
  sourcePrimaryKeys = [],
}: Props) {
  const addRow = () =>
    setPartitionSpec([
      ...partitionSpec,
      { source_column: availableColumns[0] ?? "", transform: "day", name: "" },
    ]);
  const updateRow = (i: number, patch: Partial<PartitionField>) =>
    setPartitionSpec(
      partitionSpec.map((p, idx) => (idx === i ? { ...p, ...patch } : p))
    );
  const removeRow = (i: number) =>
    setPartitionSpec(partitionSpec.filter((_, idx) => idx !== i));

  return (
    <div className="rounded-md border bg-muted/30 p-3 space-y-3">
      <div>
        <h4 className="text-sm font-semibold">Iceberg partition spec</h4>
        <p className="text-xs text-muted-foreground mt-0.5">
          Prefer <code>day(event_ts)</code> or <code>bucket(16-128, pk)</code>. Avoid{" "}
          <code>hour</code> on high-volume CDC — it produces small files.
        </p>
      </div>

      {partitionSpec.length === 0 && (
        <p className="text-xs text-muted-foreground">
          No partition fields (unpartitioned table).
        </p>
      )}

      {partitionSpec.map((p, i) => (
        <div key={i} className="flex items-center gap-2">
          <Select
            value={p.source_column}
            onValueChange={(v) => updateRow(i, { source_column: v })}
          >
            <SelectTrigger className="h-7 text-xs flex-1">
              <SelectValue placeholder="Column" />
            </SelectTrigger>
            <SelectContent>
              {availableColumns.map((c) => (
                <SelectItem key={c} value={c}>
                  {c}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select
            value={p.transform}
            onValueChange={(v) => updateRow(i, { transform: v as PartitionTransform })}
          >
            <SelectTrigger className="h-7 text-xs flex-1">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {TRANSFORMS.map((t) => (
                <SelectItem key={t} value={t}>
                  {t}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          {(p.transform === "bucket" || p.transform === "truncate") && (
            <Input
              type="number"
              className="h-7 text-xs w-20"
              value={p.width ?? 16}
              onChange={(e) => updateRow(i, { width: Number(e.target.value) })}
              placeholder="width"
            />
          )}
          <Input
            className="h-7 text-xs flex-1"
            value={p.name ?? ""}
            onChange={(e) => updateRow(i, { name: e.target.value })}
            placeholder="optional name"
          />
          <Button
            type="button"
            size="icon"
            variant="ghost"
            className="h-7 w-7"
            onClick={() => removeRow(i)}
          >
            <X className="h-3.5 w-3.5" />
          </Button>
        </div>
      ))}

      <Button
        type="button"
        size="sm"
        variant="outline"
        className="h-7 text-xs"
        onClick={addRow}
      >
        <Plus className="h-3 w-3 mr-1" /> Add partition field
      </Button>

      <div className="border-t pt-3 space-y-2">
        <h4 className="text-sm font-semibold">Identifier fields (PK for upsert)</h4>
        <div className="flex flex-wrap gap-x-4 gap-y-2">
          {availableColumns.map((c) => {
            const checked = identifierFields.includes(c);
            return (
              <label key={c} className="flex items-center gap-1.5 text-xs cursor-pointer">
                <input
                  type="checkbox"
                  checked={checked}
                  onChange={(e) =>
                    setIdentifierFields(
                      e.target.checked
                        ? [...identifierFields, c]
                        : identifierFields.filter((f) => f !== c)
                    )
                  }
                />
                <span className="font-mono">{c}</span>
                {sourcePrimaryKeys.includes(c) && (
                  <span className="text-muted-foreground">(PK)</span>
                )}
              </label>
            );
          })}
        </div>
      </div>
    </div>
  );
}
