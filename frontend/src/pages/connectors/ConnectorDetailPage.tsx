import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { Textarea } from "@/components/ui/textarea";
import { ExternalLink, ChevronDown, ChevronRight, Pencil, Save, X } from "lucide-react";
import { useState } from "react";

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

export function ConnectorDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [expandedVersion, setExpandedVersion] = useState<string | null>(null);
  const [editing, setEditing] = useState(false);
  const [editForm, setEditForm] = useState<any>({});
  const [jsonErrors, setJsonErrors] = useState<Record<string, string>>({});

  const { data: connector, isLoading } = useQuery({
    queryKey: ["connector-definitions", id],
    queryFn: () => api.get(`/connector-definitions/${id}`).then((r) => r.data),
  });

  const { data: versionsData } = useQuery({
    queryKey: ["connector-definitions", id, "versions"],
    queryFn: () => api.get(`/connector-definitions/${id}/versions`).then((r) => r.data),
    enabled: !!id,
  });
  const versions = versionsData?.versions ?? versionsData ?? [];

  const { data: usage } = useQuery({
    queryKey: ["connector-definitions", id, "usage"],
    queryFn: () => api.get(`/connector-definitions/${id}/usage`).then((r) => r.data).catch(() => []),
    enabled: !!id,
  });

  const updateMutation = useMutation({
    mutationFn: (data: any) => api.patch(`/connector-definitions/${id}`, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["connector-definitions", id] });
      queryClient.invalidateQueries({ queryKey: ["connector-definitions"] });
      setEditing(false);
    },
  });

  const startEditing = () => {
    setEditForm({
      connector_name: connector.connector_name,
      latest_version: connector.latest_version,
      documentation_url: connector.documentation_url || "",
      icon_url: connector.icon_url || "",
      supports_cdc: connector.supports_cdc,
      supports_full_refresh: connector.supports_full_refresh,
      supports_incremental: connector.supports_incremental,
      required_fields: (connector.required_fields || []).join(", "),
      optional_fields: (connector.optional_fields || []).join(", "),
      default_config: JSON.stringify(connector.default_config || {}, null, 2),
      default_resource_limits: JSON.stringify(connector.default_resource_limits || {}, null, 2),
    });
    setJsonErrors({});
    setEditing(true);
  };

  const saveEdits = () => {
    const errors: Record<string, string> = {};
    let parsedConfig: any;
    let parsedLimits: any;

    try {
      parsedConfig = JSON.parse(editForm.default_config || "{}");
    } catch {
      errors.default_config = "Invalid JSON";
    }
    try {
      parsedLimits = JSON.parse(editForm.default_resource_limits || "{}");
    } catch {
      errors.default_resource_limits = "Invalid JSON";
    }

    if (Object.keys(errors).length > 0) {
      setJsonErrors(errors);
      return;
    }

    const payload: any = {
      connector_name: editForm.connector_name,
      latest_version: editForm.latest_version,
      documentation_url: editForm.documentation_url || null,
      icon_url: editForm.icon_url || null,
      supports_cdc: editForm.supports_cdc,
      supports_full_refresh: editForm.supports_full_refresh,
      supports_incremental: editForm.supports_incremental,
      required_fields: editForm.required_fields
        .split(",")
        .map((s: string) => s.trim())
        .filter(Boolean),
      optional_fields: editForm.optional_fields
        .split(",")
        .map((s: string) => s.trim())
        .filter(Boolean),
      default_config: parsedConfig,
      default_resource_limits: parsedLimits,
    };

    updateMutation.mutate(payload);
  };

  if (isLoading) return <div className="flex items-center justify-center h-64">Loading...</div>;
  if (!connector) return <div className="text-center text-muted-foreground">Connector not found</div>;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <span className="text-3xl">{getConnectorIcon(connector.connector_type)}</span>
          <div>
            <h1 className="text-2xl font-bold">{connector.connector_name}</h1>
            <p className="text-muted-foreground">
              {connector.connector_type} · <Badge variant="outline">{connector.category}</Badge>
              {connector.latest_version && <span className="ml-2">v{connector.latest_version}</span>}
            </p>
          </div>
        </div>
        <div className="flex gap-2">
          {!editing ? (
            <Button onClick={startEditing} variant="outline">
              <Pencil className="h-4 w-4 mr-2" />Edit
            </Button>
          ) : (
            <>
              <Button onClick={() => setEditing(false)} variant="outline">
                <X className="h-4 w-4 mr-2" />Cancel
              </Button>
              <Button onClick={saveEdits} disabled={updateMutation.isPending}>
                <Save className="h-4 w-4 mr-2" />
                {updateMutation.isPending ? "Saving..." : "Save"}
              </Button>
            </>
          )}
        </div>
      </div>

      {updateMutation.isError && (
        <div className="rounded-md bg-destructive/10 p-3 text-sm text-destructive">
          Failed to save: {(updateMutation.error as any)?.response?.data?.detail ?? "Unknown error"}
        </div>
      )}

      <Tabs defaultValue="overview">
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="config">Configuration</TabsTrigger>
          <TabsTrigger value="versions">Versions ({Array.isArray(versions) ? versions.length : 0})</TabsTrigger>
          <TabsTrigger value="usage">Usage</TabsTrigger>
        </TabsList>

        {/* ============ OVERVIEW TAB ============ */}
        <TabsContent value="overview" className="space-y-4">
          <div className="grid gap-4 md:grid-cols-2">
            {/* Basic Info */}
            <Card>
              <CardHeader><CardTitle className="text-base">Basic Info</CardTitle></CardHeader>
              <CardContent className="space-y-3">
                {editing ? (
                  <>
                    <div className="space-y-1">
                      <label className="text-xs font-medium text-muted-foreground">Name</label>
                      <Input value={editForm.connector_name} onChange={(e) => setEditForm({ ...editForm, connector_name: e.target.value })} />
                    </div>
                    <div className="space-y-1">
                      <label className="text-xs font-medium text-muted-foreground">Version</label>
                      <Input value={editForm.latest_version} onChange={(e) => setEditForm({ ...editForm, latest_version: e.target.value })} />
                    </div>
                    <div className="space-y-1">
                      <label className="text-xs font-medium text-muted-foreground">Documentation URL</label>
                      <Input value={editForm.documentation_url} onChange={(e) => setEditForm({ ...editForm, documentation_url: e.target.value })} placeholder="https://..." />
                    </div>
                  </>
                ) : (
                  <dl className="space-y-2 text-sm">
                    <div className="flex justify-between"><dt className="text-muted-foreground">Type</dt><dd className="font-mono">{connector.connector_type}</dd></div>
                    <div className="flex justify-between"><dt className="text-muted-foreground">Category</dt><dd><Badge variant="outline">{connector.category}</Badge></dd></div>
                    <div className="flex justify-between"><dt className="text-muted-foreground">Version</dt><dd className="font-mono">{connector.latest_version}</dd></div>
                    <div className="flex justify-between"><dt className="text-muted-foreground">Status</dt><dd>{connector.is_active ? <Badge variant="default">Active</Badge> : <Badge variant="secondary">Inactive</Badge>}</dd></div>
                    {connector.documentation_url && (
                      <div className="flex justify-between items-center">
                        <dt className="text-muted-foreground">Docs</dt>
                        <dd><a href={connector.documentation_url} target="_blank" rel="noopener noreferrer" className="text-primary hover:underline inline-flex items-center gap-1 text-sm">View Docs<ExternalLink className="h-3 w-3" /></a></dd>
                      </div>
                    )}
                  </dl>
                )}
              </CardContent>
            </Card>

            {/* Capabilities */}
            <Card>
              <CardHeader><CardTitle className="text-base">Capabilities</CardTitle></CardHeader>
              <CardContent>
                {editing ? (
                  <div className="space-y-3">
                    {([
                      ["supports_cdc", "CDC (Change Data Capture)", "Real-time streaming of database changes"],
                      ["supports_full_refresh", "Full Refresh", "Complete table scan and replacement"],
                      ["supports_incremental", "Incremental", "Append new/changed rows using a cursor field"],
                    ] as const).map(([key, label, desc]) => (
                      <label key={key} className={`flex items-start gap-3 rounded-md border p-3 cursor-pointer ${editForm[key] ? "border-primary bg-primary/5" : ""}`}>
                        <input type="checkbox" checked={editForm[key]} onChange={(e) => setEditForm({ ...editForm, [key]: e.target.checked })} className="mt-0.5" />
                        <div><span className="text-sm font-medium">{label}</span><p className="text-xs text-muted-foreground">{desc}</p></div>
                      </label>
                    ))}
                  </div>
                ) : (
                  <div className="space-y-2">
                    {([
                      ["supports_cdc", "CDC"],
                      ["supports_full_refresh", "Full Refresh"],
                      ["supports_incremental", "Incremental"],
                    ] as const).map(([key, label]) => (
                      <div key={key} className="flex items-center justify-between text-sm">
                        <span>{label}</span>
                        {connector[key] ? <Badge variant="default">{label} ✓</Badge> : <Badge variant="outline" className="text-muted-foreground">{label} ✗</Badge>}
                      </div>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>
          </div>

          {/* Required & Optional Fields */}
          <div className="grid gap-4 md:grid-cols-2">
            <Card>
              <CardHeader><CardTitle className="text-base">Required Fields</CardTitle></CardHeader>
              <CardContent>
                {editing ? (
                  <div className="space-y-1">
                    <Input
                      value={editForm.required_fields}
                      onChange={(e) => setEditForm({ ...editForm, required_fields: e.target.value })}
                      placeholder="host, port, database_name, username, password"
                    />
                    <p className="text-xs text-muted-foreground">Comma-separated field names required when creating a source/destination</p>
                  </div>
                ) : (
                  connector.required_fields?.length > 0 ? (
                    <div className="flex flex-wrap gap-2">
                      {connector.required_fields.map((f: string) => (
                        <Badge key={f} variant="secondary" className="font-mono text-xs">{f}</Badge>
                      ))}
                    </div>
                  ) : <p className="text-sm text-muted-foreground">None defined</p>
                )}
              </CardContent>
            </Card>
            <Card>
              <CardHeader><CardTitle className="text-base">Optional Fields</CardTitle></CardHeader>
              <CardContent>
                {editing ? (
                  <div className="space-y-1">
                    <Input
                      value={editForm.optional_fields}
                      onChange={(e) => setEditForm({ ...editForm, optional_fields: e.target.value })}
                      placeholder="ssl_enabled, ssl_config, replication_slot"
                    />
                    <p className="text-xs text-muted-foreground">Comma-separated optional field names</p>
                  </div>
                ) : (
                  connector.optional_fields?.length > 0 ? (
                    <div className="flex flex-wrap gap-2">
                      {connector.optional_fields.map((f: string) => (
                        <Badge key={f} variant="outline" className="font-mono text-xs">{f}</Badge>
                      ))}
                    </div>
                  ) : <p className="text-sm text-muted-foreground">None defined</p>
                )}
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        {/* ============ CONFIGURATION TAB ============ */}
        <TabsContent value="config" className="space-y-4">
          <Card>
            <CardHeader><CardTitle className="text-base">Default Configuration</CardTitle></CardHeader>
            <CardContent>
              {editing ? (
                <div className="space-y-1">
                  <Textarea
                    value={editForm.default_config}
                    onChange={(e) => { setEditForm({ ...editForm, default_config: e.target.value }); setJsonErrors({ ...jsonErrors, default_config: "" }); }}
                    className="font-mono text-sm min-h-[200px]"
                    placeholder='{"port": 3306, "ssl_enabled": false}'
                  />
                  {jsonErrors.default_config && <p className="text-xs text-destructive">{jsonErrors.default_config}</p>}
                  <p className="text-xs text-muted-foreground">
                    Pre-filled values when a user creates a new source/destination with this connector. These become the starting config that users can override.
                  </p>
                </div>
              ) : (
                <pre className="rounded-md bg-muted p-4 text-sm overflow-auto max-h-64 font-mono">
                  <code>{JSON.stringify(connector.default_config, null, 2)}</code>
                </pre>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle className="text-base">Resource Limits</CardTitle></CardHeader>
            <CardContent>
              {editing ? (
                <div className="space-y-1">
                  <Textarea
                    value={editForm.default_resource_limits}
                    onChange={(e) => { setEditForm({ ...editForm, default_resource_limits: e.target.value }); setJsonErrors({ ...jsonErrors, default_resource_limits: "" }); }}
                    className="font-mono text-sm min-h-[120px]"
                    placeholder='{"max_memory_mb": 512, "max_connections": 5, "batch_size": 10000}'
                  />
                  {jsonErrors.default_resource_limits && <p className="text-xs text-destructive">{jsonErrors.default_resource_limits}</p>}
                  <p className="text-xs text-muted-foreground">
                    Default memory, connection pool, and batch size limits applied to workers using this connector.
                  </p>
                </div>
              ) : (
                <pre className="rounded-md bg-muted p-4 text-sm overflow-auto max-h-64 font-mono">
                  <code>{JSON.stringify(connector.default_resource_limits, null, 2)}</code>
                </pre>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* ============ VERSIONS TAB ============ */}
        <TabsContent value="versions">
          <Card>
            <CardContent className="p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-8"></TableHead>
                    <TableHead>Version</TableHead>
                    <TableHead>Release Date</TableHead>
                    <TableHead>Status</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {Array.isArray(versions) && versions.length > 0 ? (
                    versions.map((v: any) => (
                      <TableRow
                        key={v.version_id ?? v.version}
                        className="cursor-pointer"
                        onClick={() => setExpandedVersion(expandedVersion === v.version ? null : v.version)}
                      >
                        <TableCell>
                          {expandedVersion === v.version ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                        </TableCell>
                        <TableCell className="font-mono">{v.version}</TableCell>
                        <TableCell>{v.released_at ? new Date(v.released_at).toLocaleDateString() : "—"}</TableCell>
                        <TableCell><Badge variant={v.is_stable ? "default" : "secondary"}>{v.is_stable ? "Stable" : "Beta"}</Badge></TableCell>
                      </TableRow>
                    ))
                  ) : (
                    <TableRow>
                      <TableCell colSpan={4} className="text-center text-muted-foreground py-8">No versions</TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
              {Array.isArray(versions) && versions.length > 0 && expandedVersion && (() => {
                const v = versions.find((ver: any) => ver.version === expandedVersion);
                if (!v) return null;
                return (
                  <div className="border-t bg-muted/50 px-6 py-4 space-y-2">
                    {v.release_notes && <div><p className="text-xs font-medium text-muted-foreground">Release Notes</p><p className="text-sm">{v.release_notes}</p></div>}
                    {v.new_features?.length > 0 && (
                      <div>
                        <p className="text-xs font-medium text-muted-foreground">New Features</p>
                        <ul className="list-disc list-inside text-sm">{v.new_features.map((f: string, i: number) => <li key={i}>{f}</li>)}</ul>
                      </div>
                    )}
                  </div>
                );
              })()}
            </CardContent>
          </Card>
        </TabsContent>

        {/* ============ USAGE TAB ============ */}
        <TabsContent value="usage">
          <Card>
            <CardHeader><CardTitle className="text-base">Sources & Destinations Using This Connector</CardTitle></CardHeader>
            <CardContent>
              {usage?.length ? (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Name</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Status</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {usage.map((item: any) => (
                      <TableRow key={item.id} className="cursor-pointer hover:bg-muted/50" onClick={() => navigate(item.type === "source" ? `/sources/${item.id}` : `/destinations/${item.id}`)}>
                        <TableCell className="font-medium">{item.name}</TableCell>
                        <TableCell><Badge variant="outline">{item.type}</Badge></TableCell>
                        <TableCell><Badge variant={item.status === "active" ? "default" : "secondary"}>{item.status}</Badge></TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              ) : (
                <p className="text-sm text-muted-foreground text-center py-8">No sources or destinations are using this connector yet.</p>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
