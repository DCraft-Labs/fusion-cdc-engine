import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Plus, MoreHorizontal, UserCog, UserMinus, KeyRound } from "lucide-react";

export function UsersPage() {
  const queryClient = useQueryClient();
  const [showInvite, setShowInvite] = useState(false);
  const [form, setForm] = useState({ email: "", role: "viewer" });
  const [actionsOpen, setActionsOpen] = useState<string | null>(null);

  const { data: users, isLoading } = useQuery({
    queryKey: ["settings", "users"],
    queryFn: () => fetchList("/auth/users", "users").catch(() => []),
  });

  const inviteMutation = useMutation({
    mutationFn: () => api.post("/settings/users/invite", form),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["settings", "users"] });
      setShowInvite(false);
      setForm({ email: "", role: "viewer" });
    },
  });

  const deactivateMutation = useMutation({
    mutationFn: (userId: string) => api.patch(`/settings/users/${userId}`, { active: false }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["settings", "users"] }),
  });

  const resetPasswordMutation = useMutation({
    mutationFn: (userId: string) => api.post(`/settings/users/${userId}/reset-password`),
  });

  const getRoleBadgeVariant = (role: string) => {
    switch (role) {
      case "superadmin": return "destructive";
      case "admin": return "default";
      case "editor": return "secondary";
      default: return "outline";
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">User Management</h1>
          <p className="text-muted-foreground">Manage team members and their access</p>
        </div>
        <Dialog open={showInvite} onOpenChange={setShowInvite}>
          <DialogTrigger asChild>
            <Button><Plus className="mr-2 h-4 w-4" />Invite User</Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader><DialogTitle>Invite User</DialogTitle></DialogHeader>
            <div className="space-y-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Email Address</label>
                <Input
                  value={form.email}
                  onChange={(e) => setForm({ ...form, email: e.target.value })}
                  type="email"
                  placeholder="user@company.com"
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Role</label>
                <Select value={form.role} onValueChange={(v) => setForm({ ...form, role: v })}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="admin">Admin</SelectItem>
                    <SelectItem value="editor">Editor</SelectItem>
                    <SelectItem value="viewer">Viewer</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <Button
                onClick={() => inviteMutation.mutate()}
                disabled={inviteMutation.isPending || !form.email}
                className="w-full"
              >
                {inviteMutation.isPending ? "Sending Invite..." : "Send Invite"}
              </Button>
              {inviteMutation.isError && (
                <p className="text-sm text-destructive">Failed to send invite</p>
              )}
            </div>
          </DialogContent>
        </Dialog>
      </div>

      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>User</TableHead>
                <TableHead>Email</TableHead>
                <TableHead>Role</TableHead>
                <TableHead>Active</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow><TableCell colSpan={5} className="text-center py-8">Loading users...</TableCell></TableRow>
              ) : (users ?? []).length === 0 ? (
                <TableRow><TableCell colSpan={5} className="text-center py-8 text-muted-foreground">No users found</TableCell></TableRow>
              ) : (
                (users ?? []).map((u: any) => (
                  <TableRow key={u.id}>
                    <TableCell className="font-medium">{(u.name ?? `${u.first_name ?? ""} ${u.last_name ?? ""}`.trim()) || "—"}</TableCell>
                    <TableCell className="text-muted-foreground">{u.email}</TableCell>
                    <TableCell>
                      <Badge variant={getRoleBadgeVariant(u.role)}>{u.role}</Badge>
                    </TableCell>
                    <TableCell>
                      <span className={`inline-flex items-center gap-1.5 text-sm ${u.active !== false ? "text-green-600" : "text-muted-foreground"}`}>
                        <span className={`h-2 w-2 rounded-full ${u.active !== false ? "bg-green-500" : "bg-gray-300"}`} />
                        {u.active !== false ? "Active" : "Inactive"}
                      </span>
                    </TableCell>
                    <TableCell className="text-right">
                      <div className="relative inline-block">
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => setActionsOpen(actionsOpen === u.id ? null : u.id)}
                        >
                          <MoreHorizontal className="h-4 w-4" />
                        </Button>
                        {actionsOpen === u.id && (
                          <div className="absolute right-0 top-full z-50 mt-1 w-48 rounded-md border bg-popover p-1 shadow-md">
                            <button
                              className="flex w-full items-center gap-2 rounded-sm px-2 py-1.5 text-sm hover:bg-accent"
                              onClick={() => { setActionsOpen(null); }}
                            >
                              <UserCog className="h-4 w-4" /> Edit Roles
                            </button>
                            <button
                              className="flex w-full items-center gap-2 rounded-sm px-2 py-1.5 text-sm hover:bg-accent"
                              onClick={() => { deactivateMutation.mutate(u.id); setActionsOpen(null); }}
                            >
                              <UserMinus className="h-4 w-4" /> Deactivate
                            </button>
                            <button
                              className="flex w-full items-center gap-2 rounded-sm px-2 py-1.5 text-sm hover:bg-accent"
                              onClick={() => { resetPasswordMutation.mutate(u.id); setActionsOpen(null); }}
                            >
                              <KeyRound className="h-4 w-4" /> Reset Password
                            </button>
                          </div>
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
