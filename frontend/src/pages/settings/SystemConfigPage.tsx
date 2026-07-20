import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent } from "@/components/ui/card";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Plus, Pencil } from "lucide-react";

interface ConfigEntry {
  key: string;
  value: string;
}

export function SystemConfigPage() {
  const queryClient = useQueryClient();
  const [editingKey, setEditingKey] = useState<string | null>(null);
  const [editValue, setEditValue] = useState("");
  const [showAdd, setShowAdd] = useState(false);
  const [newKey, setNewKey] = useState("");
  const [newValue, setNewValue] = useState("");

  const { data: configData, isLoading } = useQuery({
    queryKey: ["settings", "system-config"],
    queryFn: () => api.get("/settings/system-config").then((r) => r.data).catch(() => ({})),
  });

  const configs: ConfigEntry[] = Array.isArray(configData)
    ? configData
    : configData
      ? Object.entries(configData).map(([key, value]) => ({ key, value: String(value) }))
      : [];

  const updateMutation = useMutation({
    mutationFn: ({ key, value }: { key: string; value: string }) =>
      api.put(`/settings/system-config/${key}`, { value }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["settings", "system-config"] });
      setEditingKey(null);
    },
  });

  const addMutation = useMutation({
    mutationFn: () => api.post("/settings/system-config", { key: newKey, value: newValue }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["settings", "system-config"] });
      setShowAdd(false);
      setNewKey("");
      setNewValue("");
    },
  });

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">System Configuration</h1>
          <p className="text-muted-foreground">Platform-wide configuration parameters (superadmin only)</p>
        </div>
        <Button onClick={() => setShowAdd(true)}>
          <Plus className="mr-2 h-4 w-4" />Add Config Key
        </Button>
      </div>

      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Key</TableHead>
                <TableHead>Value</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow><TableCell colSpan={3} className="text-center py-8">Loading configuration...</TableCell></TableRow>
              ) : configs.length === 0 ? (
                <TableRow><TableCell colSpan={3} className="text-center py-8 text-muted-foreground">No configuration entries</TableCell></TableRow>
              ) : (
                configs.map((c) => (
                  <TableRow key={c.key}>
                    <TableCell className="font-mono text-sm font-medium">{c.key}</TableCell>
                    <TableCell>
                      {editingKey === c.key ? (
                        <div className="flex items-center gap-2">
                          <Input
                            value={editValue}
                            onChange={(e) => setEditValue(e.target.value)}
                            className="w-48"
                            autoFocus
                          />
                          <Button
                            size="sm"
                            onClick={() => updateMutation.mutate({ key: c.key, value: editValue })}
                            disabled={updateMutation.isPending}
                          >
                            Save
                          </Button>
                          <Button size="sm" variant="outline" onClick={() => setEditingKey(null)}>
                            Cancel
                          </Button>
                        </div>
                      ) : (
                        <span className="font-mono text-sm">{c.value}</span>
                      )}
                    </TableCell>
                    <TableCell className="text-right">
                      {editingKey !== c.key && (
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => { setEditingKey(c.key); setEditValue(c.value); }}
                        >
                          <Pencil className="h-4 w-4" />
                        </Button>
                      )}
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* Add Config Dialog */}
      <Dialog open={showAdd} onOpenChange={setShowAdd}>
        <DialogContent>
          <DialogHeader><DialogTitle>Add Configuration Key</DialogTitle></DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">Key</label>
              <Input
                value={newKey}
                onChange={(e) => setNewKey(e.target.value)}
                placeholder="e.g., max_connections_per_src"
                className="font-mono"
              />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Value</label>
              <Input
                value={newValue}
                onChange={(e) => setNewValue(e.target.value)}
                placeholder="Enter value"
              />
            </div>
            <Button
              onClick={() => addMutation.mutate()}
              disabled={addMutation.isPending || !newKey || !newValue}
              className="w-full"
            >
              {addMutation.isPending ? "Adding..." : "Add Configuration"}
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
}
