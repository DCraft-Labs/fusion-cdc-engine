import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Plus, Search } from "lucide-react";
import { Textarea } from "@/components/ui/textarea";

const CONNECTOR_ICONS: Record<string, string> = {
  mysql: "🐬",
  mongodb: "🍃",
  postgresql: "🐘",
  postgres: "🐘",
  iceberg: "🧊",
};

function getConnectorIcon(type: string) {
  return CONNECTOR_ICONS[type?.toLowerCase()] ?? "🔌";
}

interface ConnectorForm {
  connector_name: string;
  connector_type: string;
  category: "source" | "destination";
  latest_version: string;
  documentation_url: string;
  supports_cdc: boolean;
  supports_full_refresh: boolean;
  supports_incremental: boolean;
  required_fields: string;
  optional_fields: string;
  default_config: string;
  default_resource_limits: string;
}

const emptyForm: ConnectorForm = {
  connector_name: "",
  connector_type: "",
  category: "source",
  latest_version: "1.0.0",
  documentation_url: "",
  supports_cdc: false,
  supports_full_refresh: true,
  supports_incremental: false,
  required_fields: "",
  optional_fields: "",
  default_config: "{}",
  default_resource_limits: "{}",
};

export function ConnectorsPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [showCreate, setShowCreate] = useState(false);
  const [form, setForm] = useState<ConnectorForm>(emptyForm);
  const [filter, setFilter] = useState<"all" | "source" | "destination">("all");
  const [search, setSearch] = useState("");
  const [jsonErrors, setJsonErrors] = useState<Record<string, string>>({});

  const { data: connectors = [], isLoading } = useQuery({
    queryKey: ["connector-definitions"],
    queryFn: () => fetchList("/connector-definitions", "connectors"),
  });

  const createMutation = useMutation({
    mutationFn: (data: ConnectorForm) => {
      const payload: any = {
        connector_name: data.connector_name,
        connector_type: data.connector_type,
        category: data.category,
        latest_version: data.latest_version,
        documentation_url: data.documentation_url || undefined,
        supports_cdc: data.supports_cdc,
        supports_full_refresh: data.supports_full_refresh,
        supports_incremental: data.supports_incremental,
        required_fields: data.required_fields.split(",").map((s) => s.trim()).filter(Boolean),
        optional_fields: data.optional_fields.split(",").map((s) => s.trim()).filter(Boolean),
        default_config: JSON.parse(data.default_config || "{}"),
        default_resource_limits: JSON.parse(data.default_resource_limits || "{}"),
      };
      return api.post("/connector-definitions", payload);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["connector-definitions"] });
      setShowCreate(false);
      setForm(emptyForm);
    },
  });

  const filtered = connectors.filter((c: any) => {
    if (filter !== "all" && c.category !== filter) return false;
    if (search && !c.connector_name.toLowerCase().includes(search.toLowerCase())) return false;
    return true;
  });

  const sourceCount = connectors.filter((c: any) => c.category === "source").length;
  const destCount = connectors.filter((c: any) => c.category === "destination").length;

  if (isLoading) return <div className="flex items-center justify-center h-64">Loading...</div>;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Connector Definitions</h1>
        <Button onClick={() => setShowCreate(true)}>
          <Plus className="h-4 w-4 mr-2" />
          New Connector
        </Button>
      </div>

      <div className="flex items-center gap-4">
        <Tabs value={filter} onValueChange={(v) => setFilter(v as any)}>
          <TabsList>
            <TabsTrigger value="all">All ({connectors?.length ?? 0})</TabsTrigger>
            <TabsTrigger value="source">Sources ({sourceCount})</TabsTrigger>
            <TabsTrigger value="destination">Dest. ({destCount})</TabsTrigger>
          </TabsList>
        </Tabs>
        <div className="relative ml-auto">
          <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search connectors..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="pl-9 w-64"
          />
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {filtered.length === 0 ? (
          <p className="text-muted-foreground col-span-full text-center py-8">No connectors found</p>
        ) : (
          filtered.map((conn: any) => (
            <Card
              key={conn.connector_id}
              className="hover:shadow-md transition-shadow cursor-pointer"
              onClick={() => navigate(`/connectors/${conn.connector_id}`)}
            >
              <CardHeader className="pb-3">
                <div className="flex items-center gap-3">
                  <span className="text-2xl">{getConnectorIcon(conn.connector_type)}</span>
                  <div className="min-w-0">
                    <CardTitle className="text-base truncate">{conn.connector_name}</CardTitle>
                    <p className="text-sm text-muted-foreground">
                      {conn.connector_type} · <Badge variant="outline" className="text-xs">{conn.category}</Badge>
                      {conn.latest_version && <span className="ml-1">v{conn.latest_version}</span>}
                    </p>
                  </div>
                </div>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="flex flex-wrap gap-1.5">
                  {conn.supports_cdc && <Badge variant="default" className="text-xs">CDC ✓</Badge>}
                  {conn.supports_full_refresh && <Badge variant="secondary" className="text-xs">Full Refresh ✓</Badge>}
                  {conn.supports_incremental ? (
                    <Badge variant="secondary" className="text-xs">Incremental ✓</Badge>
                  ) : (
                    <Badge variant="outline" className="text-xs text-muted-foreground">Incremental ✗</Badge>
                  )}
                </div>
                <p className="text-xs text-muted-foreground">
                  Used by: {conn.usage_count ?? 0} {conn.category === "source" ? "sources" : "destinations"}
                </p>
              </CardContent>
            </Card>
          ))
        )}
      </div>

      <Dialog open={showCreate} onOpenChange={setShowCreate}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle>Create Connector Definition</DialogTitle>
          </DialogHeader>
          <form
            className="space-y-4 max-h-[70vh] overflow-y-auto pr-1"
            onSubmit={(e) => {
              e.preventDefault();
              const errors: Record<string, string> = {};
              try { JSON.parse(form.default_config || "{}"); } catch { errors.default_config = "Invalid JSON"; }
              try { JSON.parse(form.default_resource_limits || "{}"); } catch { errors.default_resource_limits = "Invalid JSON"; }
              if (Object.keys(errors).length > 0) { setJsonErrors(errors); return; }
              setJsonErrors({});
              createMutation.mutate(form);
            }}
          >
            <div className="space-y-2">
              <label className="text-sm font-medium">Name *</label>
              <Input
                value={form.connector_name}
                onChange={(e) => setForm({ ...form, connector_name: e.target.value })}
                placeholder="e.g. MySQL CDC"
                required
              />
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Type *</label>
                <Input
                  value={form.connector_type}
                  onChange={(e) => setForm({ ...form, connector_type: e.target.value })}
                  placeholder="e.g. mysql"
                  required
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Version *</label>
                <Input
                  value={form.latest_version}
                  onChange={(e) => setForm({ ...form, latest_version: e.target.value })}
                  placeholder="1.0.0"
                  required
                />
              </div>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Category *</label>
              <Select value={form.category} onValueChange={(v) => setForm({ ...form, category: v as any })}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="source">Source</SelectItem>
                  <SelectItem value="destination">Destination</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Capabilities</label>
              <div className="flex flex-wrap gap-4">
                <label className="flex items-center gap-2 text-sm">
                  <input type="checkbox" checked={form.supports_cdc} onChange={(e) => setForm({ ...form, supports_cdc: e.target.checked })} />
                  CDC
                </label>
                <label className="flex items-center gap-2 text-sm">
                  <input type="checkbox" checked={form.supports_full_refresh} onChange={(e) => setForm({ ...form, supports_full_refresh: e.target.checked })} />
                  Full Refresh
                </label>
                <label className="flex items-center gap-2 text-sm">
                  <input type="checkbox" checked={form.supports_incremental} onChange={(e) => setForm({ ...form, supports_incremental: e.target.checked })} />
                  Incremental
                </label>
              </div>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Documentation URL</label>
              <Input
                value={form.documentation_url}
                onChange={(e) => setForm({ ...form, documentation_url: e.target.value })}
                placeholder="https://docs.example.com/mysql"
                type="url"
              />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Required Fields</label>
              <Input
                value={form.required_fields}
                onChange={(e) => setForm({ ...form, required_fields: e.target.value })}
                placeholder="host, port, database_name, username, password"
              />
              <p className="text-xs text-muted-foreground">Comma-separated field names users must provide</p>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Optional Fields</label>
              <Input
                value={form.optional_fields}
                onChange={(e) => setForm({ ...form, optional_fields: e.target.value })}
                placeholder="ssl_enabled, ssl_config, replication_slot"
              />
              <p className="text-xs text-muted-foreground">Comma-separated optional field names</p>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Default Configuration</label>
              <Textarea
                value={form.default_config}
                onChange={(e) => { setForm({ ...form, default_config: e.target.value }); setJsonErrors({ ...jsonErrors, default_config: "" }); }}
                className="font-mono text-sm min-h-[100px]"
                placeholder='{"port": 3306, "ssl_enabled": false}'
              />
              {jsonErrors.default_config && <p className="text-xs text-destructive">{jsonErrors.default_config}</p>}
              <p className="text-xs text-muted-foreground">JSON defaults pre-filled when creating sources/destinations</p>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Resource Limits</label>
              <Textarea
                value={form.default_resource_limits}
                onChange={(e) => { setForm({ ...form, default_resource_limits: e.target.value }); setJsonErrors({ ...jsonErrors, default_resource_limits: "" }); }}
                className="font-mono text-sm min-h-[80px]"
                placeholder='{"max_memory_mb": 512, "max_connections": 5}'
              />
              {jsonErrors.default_resource_limits && <p className="text-xs text-destructive">{jsonErrors.default_resource_limits}</p>}
            </div>
            <div className="flex justify-end gap-2 pt-2">
              <Button type="button" variant="outline" onClick={() => setShowCreate(false)}>Cancel</Button>
              <Button type="submit" disabled={createMutation.isPending}>Create</Button>
            </div>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
