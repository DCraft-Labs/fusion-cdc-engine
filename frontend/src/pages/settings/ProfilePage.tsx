import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Upload, Sun, Moon } from "lucide-react";

const TIMEZONES = [
  "UTC",
  "America/New_York",
  "America/Chicago",
  "America/Denver",
  "America/Los_Angeles",
  "Europe/London",
  "Europe/Berlin",
  "Asia/Kolkata",
  "Asia/Tokyo",
  "Australia/Sydney",
];

export function ProfilePage() {
  const queryClient = useQueryClient();
  const [form, setForm] = useState({
    first_name: "",
    last_name: "",
    email: "",
  });
  const [preferences, setPreferences] = useState({
    theme: "light" as "light" | "dark",
    timezone: "UTC",
    notify_email: true,
    notify_slack: false,
    notify_in_app: true,
  });

  const { data: user } = useQuery({
    queryKey: ["auth", "me"],
    queryFn: () => api.get("/auth/me").then((r) => r.data),
  });

  useEffect(() => {
    if (user) {
      setForm({
        first_name: user.first_name ?? user.name?.split(" ")[0] ?? "",
        last_name: user.last_name ?? user.name?.split(" ").slice(1).join(" ") ?? "",
        email: user.email ?? "",
      });
      if (user.preferences) {
        setPreferences((p) => ({ ...p, ...user.preferences }));
      }
    }
  }, [user]);

  const updateMutation = useMutation({
    mutationFn: () =>
      api.patch("/auth/me", {
        first_name: form.first_name,
        last_name: form.last_name,
        email: form.email,
      }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["auth", "me"] }),
  });

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Profile Settings</h1>

      <Card>
        <CardHeader><CardTitle>Personal Information</CardTitle></CardHeader>
        <CardContent className="space-y-6">
          {/* Avatar Upload */}
          <div className="flex items-center gap-6">
            <div className="h-20 w-20 rounded-full bg-muted flex items-center justify-center text-2xl font-bold text-muted-foreground">
              {form.first_name?.[0]?.toUpperCase() ?? "U"}
            </div>
            <Button variant="outline" size="sm">
              <Upload className="mr-2 h-4 w-4" />Upload Avatar
            </Button>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">First Name</label>
              <Input
                value={form.first_name}
                onChange={(e) => setForm({ ...form, first_name: e.target.value })}
              />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Last Name</label>
              <Input
                value={form.last_name}
                onChange={(e) => setForm({ ...form, last_name: e.target.value })}
              />
            </div>
          </div>

          <div className="space-y-2">
            <label className="text-sm font-medium">Email</label>
            <Input
              value={form.email}
              onChange={(e) => setForm({ ...form, email: e.target.value })}
              type="email"
            />
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader><CardTitle>Preferences</CardTitle></CardHeader>
        <CardContent className="space-y-6">
          {/* Theme Toggle */}
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium">Theme</p>
              <p className="text-sm text-muted-foreground">Select your preferred appearance</p>
            </div>
            <div className="flex items-center gap-2 rounded-lg border p-1">
              <button
                className={`flex items-center gap-1 rounded px-3 py-1.5 text-sm ${preferences.theme === "light" ? "bg-primary text-primary-foreground" : "text-muted-foreground"}`}
                onClick={() => setPreferences({ ...preferences, theme: "light" })}
              >
                <Sun className="h-4 w-4" /> Light
              </button>
              <button
                className={`flex items-center gap-1 rounded px-3 py-1.5 text-sm ${preferences.theme === "dark" ? "bg-primary text-primary-foreground" : "text-muted-foreground"}`}
                onClick={() => setPreferences({ ...preferences, theme: "dark" })}
              >
                <Moon className="h-4 w-4" /> Dark
              </button>
            </div>
          </div>

          {/* Timezone */}
          <div className="space-y-2">
            <label className="text-sm font-medium">Timezone</label>
            <Select value={preferences.timezone} onValueChange={(v) => setPreferences({ ...preferences, timezone: v })}>
              <SelectTrigger className="w-64"><SelectValue /></SelectTrigger>
              <SelectContent>
                {TIMEZONES.map((tz) => (
                  <SelectItem key={tz} value={tz}>{tz}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          {/* Notification Preferences */}
          <div className="space-y-3">
            <p className="text-sm font-medium">Notifications</p>
            <label className="flex items-center gap-3">
              <input
                type="checkbox"
                className="h-4 w-4 rounded border-gray-300"
                checked={preferences.notify_email}
                onChange={(e) => setPreferences({ ...preferences, notify_email: e.target.checked })}
              />
              <span className="text-sm">Email notifications</span>
            </label>
            <label className="flex items-center gap-3">
              <input
                type="checkbox"
                className="h-4 w-4 rounded border-gray-300"
                checked={preferences.notify_slack}
                onChange={(e) => setPreferences({ ...preferences, notify_slack: e.target.checked })}
              />
              <span className="text-sm">Slack notifications</span>
            </label>
            <label className="flex items-center gap-3">
              <input
                type="checkbox"
                className="h-4 w-4 rounded border-gray-300"
                checked={preferences.notify_in_app}
                onChange={(e) => setPreferences({ ...preferences, notify_in_app: e.target.checked })}
              />
              <span className="text-sm">In-app notifications</span>
            </label>
          </div>
        </CardContent>
      </Card>

      <div className="flex justify-end">
        <Button onClick={() => updateMutation.mutate()} disabled={updateMutation.isPending}>
          {updateMutation.isPending ? "Saving..." : "Save Changes"}
        </Button>
      </div>
      {updateMutation.isSuccess && <p className="text-sm text-green-600">Profile updated successfully</p>}
      {updateMutation.isError && <p className="text-sm text-destructive">Failed to update profile</p>}
    </div>
  );
}
