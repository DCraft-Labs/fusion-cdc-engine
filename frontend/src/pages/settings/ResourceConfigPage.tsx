import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Cpu, Server } from "lucide-react";
import { useToast } from "@/hooks/use-toast";

// Contract (agreed with the backend team building GET/PUT /resource-config
// in parallel): this is the tenant-wide compute pool fusion-cdc admission
// control checks connections' initial loads against (see
// POST /connections/{id}/admission-preview in CreateConnectionWizard.tsx).
interface ResourceConfig {
  resource_configured: boolean;
  total_cpu_min: string;
  total_cpu_max: string;
  total_memory_min: string;
  total_memory_max: string;
  instance_type: string;
  instance_count: number;
  pool_scope: "dedicated" | "shared";
}

const DEFAULT_CONFIG: ResourceConfig = {
  resource_configured: false,
  total_cpu_min: "",
  total_cpu_max: "",
  total_memory_min: "",
  total_memory_max: "",
  instance_type: "",
  instance_count: 1,
  pool_scope: "dedicated",
};

export function ResourceConfigPage() {
  const { toast } = useToast();
  const queryClient = useQueryClient();
  const navigate = useNavigate();

  const { data: saved, isLoading } = useQuery<ResourceConfig>({
    queryKey: ["resource-config"],
    queryFn: () => api.get("/resource-config").then((r) => r.data).catch(() => DEFAULT_CONFIG),
  });

  const [form, setForm] = useState<ResourceConfig | null>(null);
  const config = form ?? saved ?? DEFAULT_CONFIG;

  const set = (field: keyof ResourceConfig, value: any) =>
    setForm((prev) => ({ ...(prev ?? config), [field]: value }));

  const saveMutation = useMutation({
    mutationFn: (payload: ResourceConfig) => api.put("/resource-config", payload),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["resource-config"] });
      setForm(null);
      toast({ title: "Resource configuration saved", description: "Admission control will use these limits for new connections." });
      // This page is often reached via the forced first-login gate in
      // MainLayout.tsx — once saved there's no reason to keep the user
      // parked here, so head to the dashboard like any other "created"
      // flow in this app (e.g. CreateConnectionWizard navigating away
      // after success).
      navigate("/dashboard");
    },
    onError: () => {
      toast({ title: "Failed to save", description: "Check your values and try again.", variant: "destructive" });
    },
  });

  const isValid =
    config.total_cpu_min.trim() !== "" &&
    config.total_cpu_max.trim() !== "" &&
    config.total_memory_min.trim() !== "" &&
    config.total_memory_max.trim() !== "" &&
    config.instance_type.trim() !== "" &&
    config.instance_count > 0;

  const handleSave = () => saveMutation.mutate(config);
  const isDirty = form !== null;

  return (
    <div className="space-y-6 max-w-3xl">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Cpu className="h-6 w-6 text-primary" />
            Resource Configuration
          </h1>
          <p className="text-muted-foreground mt-1">
            Tell fusion-cdc how much compute is available so it can pick a safe speed mode for each connection's
            initial load.
          </p>
        </div>
        {!isLoading && !saved?.resource_configured && (
          <Badge variant="secondary" className="text-orange-600 border-orange-300 bg-orange-50">
            Required before continuing
          </Badge>
        )}
      </div>

      {isLoading ? (
        <div className="text-center py-16 text-muted-foreground">Loading configuration…</div>
      ) : (
        <>
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <Cpu className="h-4 w-4" /> Total Cluster Resources
              </CardTitle>
              <CardDescription>
                The minimum and maximum CPU / memory fusion-cdc can draw on across all of its connections.
              </CardDescription>
            </CardHeader>
            <CardContent className="grid grid-cols-2 gap-4">
              <div className="space-y-1.5">
                <Label>Total CPU (min)</Label>
                <Input
                  value={config.total_cpu_min}
                  onChange={(e) => set("total_cpu_min", e.target.value)}
                  placeholder="e.g. 2"
                />
              </div>
              <div className="space-y-1.5">
                <Label>Total CPU (max)</Label>
                <Input
                  value={config.total_cpu_max}
                  onChange={(e) => set("total_cpu_max", e.target.value)}
                  placeholder="e.g. 16"
                />
              </div>
              <div className="space-y-1.5">
                <Label>Total Memory (min)</Label>
                <Input
                  value={config.total_memory_min}
                  onChange={(e) => set("total_memory_min", e.target.value)}
                  placeholder="e.g. 4Gi"
                />
              </div>
              <div className="space-y-1.5">
                <Label>Total Memory (max)</Label>
                <Input
                  value={config.total_memory_max}
                  onChange={(e) => set("total_memory_max", e.target.value)}
                  placeholder="e.g. 64Gi"
                />
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <Server className="h-4 w-4" /> Instances
              </CardTitle>
              <CardDescription>The compute instances fusion-cdc's workers and committers will run on.</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-1.5">
                  <Label>Instance Type</Label>
                  <Input
                    value={config.instance_type}
                    onChange={(e) => set("instance_type", e.target.value)}
                    placeholder="e.g. m5.2xlarge"
                  />
                </div>
                <div className="space-y-1.5">
                  <Label>Instance Count</Label>
                  <Input
                    type="number"
                    min={1}
                    value={config.instance_count}
                    onChange={(e) => set("instance_count", Math.max(1, Number(e.target.value) || 1))}
                    placeholder="1"
                  />
                </div>
              </div>

              <div className="space-y-2">
                <Label>Pool Scope</Label>
                <div className="space-y-2">
                  {([
                    ["dedicated", "Dedicated to fusion-cdc", "These instances are reserved for fusion-cdc only."],
                    ["shared", "Shared with other workloads", "These instances also run other workloads — fusion-cdc will admit connections more conservatively."],
                  ] as const).map(([val, label, hint]) => (
                    <label
                      key={val}
                      className={`flex items-start gap-3 rounded-lg border p-3 cursor-pointer transition-colors ${
                        config.pool_scope === val ? "border-primary bg-primary/5 ring-1 ring-primary" : "hover:bg-muted/50"
                      }`}
                    >
                      <input
                        type="radio"
                        name="pool_scope"
                        value={val}
                        checked={config.pool_scope === val}
                        onChange={() => set("pool_scope", val)}
                        className="sr-only"
                      />
                      <div
                        className={`mt-0.5 w-4 h-4 rounded-full border-2 flex items-center justify-center shrink-0 ${
                          config.pool_scope === val ? "border-primary" : "border-muted-foreground"
                        }`}
                      >
                        {config.pool_scope === val && <div className="w-2 h-2 rounded-full bg-primary" />}
                      </div>
                      <div>
                        <span className="text-sm font-medium">{label}</span>
                        <p className="text-xs text-muted-foreground">{hint}</p>
                      </div>
                    </label>
                  ))}
                </div>
              </div>
            </CardContent>
          </Card>

          <div className="flex items-center justify-between">
            {isDirty && (
              <Badge variant="secondary" className="text-orange-600 border-orange-300 bg-orange-50">
                Unsaved changes
              </Badge>
            )}
            <div className="ml-auto">
              <Button onClick={handleSave} disabled={!isValid || saveMutation.isPending}>
                {saveMutation.isPending ? "Saving…" : "Save Resource Configuration"}
              </Button>
            </div>
          </div>
          {saveMutation.isError && (
            <p className="text-sm text-destructive text-right">Failed to save. Please check your values and try again.</p>
          )}
        </>
      )}
    </div>
  );
}
