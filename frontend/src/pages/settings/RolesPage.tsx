import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Plus, Pencil, Trash2 } from "lucide-react";

const PERMISSION_GROUPS = {
  sources: ["sources:read", "sources:create", "sources:update", "sources:delete"],
  destinations: ["destinations:read", "destinations:create", "destinations:update", "destinations:delete"],
  connections: ["connections:read", "connections:create", "connections:update", "connections:delete", "connections:pause", "connections:resume"],
  transforms: ["transforms:read", "transforms:create", "transforms:update", "transforms:delete"],
  alerts: ["alerts:read", "alerts:create", "alerts:update", "alerts:delete", "alerts:acknowledge"],
  admin: ["admin:users", "admin:roles", "admin:audit", "admin:config", "admin:feature-flags", "admin:maintenance"],
};

interface RoleForm {
  name: string;
  level: number;
  description: string;
  permissions: string[];
}

const emptyForm: RoleForm = { name: "", level: 0, description: "", permissions: [] };

export function RolesPage() {
  const queryClient = useQueryClient();
  const [showDialog, setShowDialog] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [form, setForm] = useState<RoleForm>(emptyForm);

  const { data: roles, isLoading } = useQuery({
    queryKey: ["settings", "roles"],
    queryFn: () => fetchList("/auth/roles", "roles").catch(() => []),
  });

  const createMutation = useMutation({
    mutationFn: () => api.post("/settings/roles", form),
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: ["settings", "roles"] }); closeDialog(); },
  });

  const updateMutation = useMutation({
    mutationFn: () => api.put(`/settings/roles/${editingId}`, form),
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: ["settings", "roles"] }); closeDialog(); },
  });

  const deleteMutation = useMutation({
    mutationFn: (id: string) => api.delete(`/settings/roles/${id}`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["settings", "roles"] }),
  });

  const closeDialog = () => {
    setShowDialog(false);
    setEditingId(null);
    setForm(emptyForm);
  };

  const openEdit = (role: any) => {
    setEditingId(role.id ?? role.name);
    setForm({
      name: role.name,
      level: role.level ?? 0,
      description: role.description ?? "",
      permissions: role.permissions ?? [],
    });
    setShowDialog(true);
  };

  const togglePermission = (perm: string) => {
    setForm((f) => ({
      ...f,
      permissions: f.permissions.includes(perm)
        ? f.permissions.filter((p) => p !== perm)
        : [...f.permissions, perm],
    }));
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Roles & Permissions</h1>
          <p className="text-muted-foreground">Define access levels and permissions for your team</p>
        </div>
        <Dialog open={showDialog} onOpenChange={(open) => { if (!open) closeDialog(); else setShowDialog(true); }}>
          <DialogTrigger asChild>
            <Button onClick={() => { setEditingId(null); setForm(emptyForm); }}>
              <Plus className="mr-2 h-4 w-4" />Create Role
            </Button>
          </DialogTrigger>
          <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
            <DialogHeader>
              <DialogTitle>{editingId ? "Edit Role" : "Create Role"}</DialogTitle>
            </DialogHeader>
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <label className="text-sm font-medium">Role Name</label>
                  <Input
                    value={form.name}
                    onChange={(e) => setForm({ ...form, name: e.target.value })}
                    placeholder="e.g., Data Engineer"
                  />
                </div>
                <div className="space-y-2">
                  <label className="text-sm font-medium">Level</label>
                  <Input
                    value={form.level}
                    onChange={(e) => setForm({ ...form, level: parseInt(e.target.value) || 0 })}
                    type="number"
                    placeholder="0-100"
                  />
                </div>
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Description</label>
                <Input
                  value={form.description}
                  onChange={(e) => setForm({ ...form, description: e.target.value })}
                  placeholder="Describe this role's purpose"
                />
              </div>

              {/* Permission Matrix */}
              <div className="space-y-4">
                <p className="text-sm font-medium">Permissions</p>
                {Object.entries(PERMISSION_GROUPS).map(([group, perms]) => (
                  <div key={group} className="space-y-2">
                    <p className="text-xs font-semibold uppercase text-muted-foreground tracking-wider">{group}</p>
                    <div className="grid grid-cols-2 md:grid-cols-3 gap-2">
                      {perms.map((perm) => (
                        <label key={perm} className="flex items-center gap-2 text-sm">
                          <input
                            type="checkbox"
                            className="h-3.5 w-3.5 rounded border-gray-300"
                            checked={form.permissions.includes(perm)}
                            onChange={() => togglePermission(perm)}
                          />
                          <span className="font-mono text-xs">{perm.split(":")[1]}</span>
                        </label>
                      ))}
                    </div>
                  </div>
                ))}
              </div>

              <div className="flex gap-2 pt-2">
                <Button
                  onClick={() => editingId ? updateMutation.mutate() : createMutation.mutate()}
                  disabled={createMutation.isPending || updateMutation.isPending || !form.name}
                >
                  {editingId ? "Update Role" : "Create Role"}
                </Button>
                <Button variant="outline" onClick={closeDialog}>Cancel</Button>
              </div>
            </div>
          </DialogContent>
        </Dialog>
      </div>

      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Role Name</TableHead>
                <TableHead>Level</TableHead>
                <TableHead>Description</TableHead>
                <TableHead>User Count</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow><TableCell colSpan={5} className="text-center py-8">Loading roles...</TableCell></TableRow>
              ) : (roles ?? []).length === 0 ? (
                <TableRow><TableCell colSpan={5} className="text-center py-8 text-muted-foreground">No roles defined</TableCell></TableRow>
              ) : (
                (roles ?? []).map((r: any) => (
                  <TableRow key={r.id ?? r.name}>
                    <TableCell className="font-medium">{r.name}</TableCell>
                    <TableCell>
                      <Badge variant="outline">{r.level ?? 0}</Badge>
                    </TableCell>
                    <TableCell className="text-muted-foreground max-w-[200px] truncate">
                      {r.description ?? "—"}
                    </TableCell>
                    <TableCell>{r.user_count ?? 0}</TableCell>
                    <TableCell className="text-right">
                      <div className="flex items-center justify-end gap-1">
                        <Button variant="ghost" size="sm" onClick={() => openEdit(r)}>
                          <Pencil className="h-4 w-4" />
                        </Button>
                        {!r.system && (
                          <Button variant="ghost" size="sm" onClick={() => deleteMutation.mutate(r.id ?? r.name)}>
                            <Trash2 className="h-4 w-4 text-destructive" />
                          </Button>
                        )}
                      </div>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
