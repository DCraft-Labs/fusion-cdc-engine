import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Lock } from "lucide-react";

function getPasswordStrength(pw: string): { label: string; color: string; width: string; bgColor: string } {
  if (pw.length === 0) return { label: "", color: "", width: "0%", bgColor: "" };
  if (pw.length < 8) return { label: "Weak", color: "text-red-500", width: "25%", bgColor: "bg-red-500" };
  const hasUpper = /[A-Z]/.test(pw);
  const hasLower = /[a-z]/.test(pw);
  const hasNumber = /[0-9]/.test(pw);
  const hasSpecial = /[^A-Za-z0-9]/.test(pw);
  const score = [hasUpper, hasLower, hasNumber, hasSpecial].filter(Boolean).length;
  if (score >= 4 && pw.length >= 12) return { label: "Strong", color: "text-green-600", width: "100%", bgColor: "bg-green-500" };
  if (score >= 3 && pw.length >= 10) return { label: "Good", color: "text-emerald-500", width: "75%", bgColor: "bg-emerald-500" };
  if (score >= 2) return { label: "Fair", color: "text-amber-500", width: "50%", bgColor: "bg-amber-500" };
  return { label: "Weak", color: "text-red-500", width: "25%", bgColor: "bg-red-500" };
}

export function ChangePasswordPage() {
  const [form, setForm] = useState({ current_password: "", new_password: "", confirm_password: "" });
  const [error, setError] = useState("");

  const mutation = useMutation({
    mutationFn: () => {
      if (form.new_password !== form.confirm_password) {
        return Promise.reject(new Error("Passwords do not match"));
      }
      if (form.new_password.length < 8) {
        return Promise.reject(new Error("Password must be at least 8 characters"));
      }
      return api.post("/auth/change-password", {
        current_password: form.current_password,
        new_password: form.new_password,
      });
    },
    onSuccess: () => {
      setForm({ current_password: "", new_password: "", confirm_password: "" });
      setError("");
    },
    onError: (err: any) => setError(err?.response?.data?.detail ?? err.message ?? "Failed to change password"),
  });

  const strength = getPasswordStrength(form.new_password);
  const passwordsMatch = form.confirm_password === "" || form.new_password === form.confirm_password;

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Change Password</h1>

      <Card className="max-w-lg">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Lock className="h-5 w-5" /> Update your password
          </CardTitle>
        </CardHeader>
        <CardContent>
          <form
            onSubmit={(e) => { e.preventDefault(); mutation.mutate(); }}
            className="space-y-5"
          >
            <div className="space-y-2">
              <label className="text-sm font-medium">Current Password</label>
              <Input
                value={form.current_password}
                onChange={(e) => setForm({ ...form, current_password: e.target.value })}
                type="password"
                required
                placeholder="Enter current password"
              />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">New Password</label>
              <Input
                value={form.new_password}
                onChange={(e) => setForm({ ...form, new_password: e.target.value })}
                type="password"
                required
                placeholder="Enter new password"
              />
              {form.new_password && (
                <div className="space-y-1">
                  <div className="h-2 w-full rounded-full bg-muted overflow-hidden">
                    <div
                      className={`h-full rounded-full transition-all ${strength.bgColor}`}
                      style={{ width: strength.width }}
                    />
                  </div>
                  <p className={`text-xs ${strength.color}`}>Strength: {strength.label}</p>
                </div>
              )}
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Confirm New Password</label>
              <Input
                value={form.confirm_password}
                onChange={(e) => setForm({ ...form, confirm_password: e.target.value })}
                type="password"
                required
                placeholder="Re-enter new password"
              />
              {!passwordsMatch && (
                <p className="text-xs text-destructive">Passwords do not match</p>
              )}
            </div>

            {error && <p className="text-sm text-destructive">{error}</p>}
            {mutation.isSuccess && (
              <p className="text-sm text-green-600">✓ Password changed successfully</p>
            )}

            <Button
              type="submit"
              disabled={mutation.isPending || !passwordsMatch}
              className="w-full"
            >
              {mutation.isPending ? "Updating..." : "Change Password"}
            </Button>
          </form>
        </CardContent>
      </Card>
    </div>
  );
}
