import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { Zap, Server, Cpu, MemoryStick, Plus, Trash2, Save, RefreshCw } from "lucide-react";
import { useToast } from "@/hooks/use-toast";

interface SparkConfig {
  master: string;
  deploy_mode: string;
  namespace: string;
  image_pull_policy: string;
  driver_cores: string;
  driver_memory: string;
  executor_cores: string;
  executor_memory: string;
  executor_instances: string;
  dynamic_allocation_enabled: boolean;
  dynamic_allocation_min: string;
  dynamic_allocation_max: string;
  checkpoint_dir: string;
  service_account: string;
  image_registry: string;
  image_tag: string;
  extra_conf: Record<string, string>;
}

const DEFAULT_CONFIG: SparkConfig = {
  master: "k8s://https://kubernetes.default.svc.cluster.local:443",
  deploy_mode: "cluster",
  namespace: "spark",
  image_pull_policy: "IfNotPresent",
  driver_cores: "1",
  driver_memory: "1g",
  executor_cores: "1",
  executor_memory: "1g",
  executor_instances: "2",
  dynamic_allocation_enabled: false,
  dynamic_allocation_min: "1",
  dynamic_allocation_max: "5",
  checkpoint_dir: "/tmp/spark-checkpoints",
  service_account: "spark",
  image_registry: "",
  image_tag: "3.4.1",
  extra_conf: {},
};

export function SparkConfigPage() {
  const { toast } = useToast();
  const queryClient = useQueryClient();

  const { data: saved, isLoading } = useQuery<SparkConfig>({
    queryKey: ["settings", "spark-config"],
    queryFn: () => api.get("/settings/spark-config").then((r) => r.data).catch(() => DEFAULT_CONFIG),
  });

  const [form, setForm] = useState<SparkConfig | null>(null);
  const config = form ?? saved ?? DEFAULT_CONFIG;

  // new extra-conf row state
  const [newKey, setNewKey] = useState("");
  const [newVal, setNewVal] = useState("");

  const set = (field: keyof SparkConfig, value: any) =>
    setForm((prev) => ({ ...(prev ?? config), [field]: value }));

  const setExtra = (key: string, value: string) =>
    setForm((prev) => ({
      ...(prev ?? config),
      extra_conf: { ...(prev ?? config).extra_conf, [key]: value },
    }));

  const removeExtra = (key: string) =>
    setForm((prev) => {
      const extra = { ...(prev ?? config).extra_conf };
      delete extra[key];
      return { ...(prev ?? config), extra_conf: extra };
    });

  const addExtra = () => {
    if (!newKey.trim()) return;
    setExtra(newKey.trim(), newVal.trim());
    setNewKey("");
    setNewVal("");
  };

  const saveMutation = useMutation({
    mutationFn: (payload: SparkConfig) => api.put("/settings/spark-config", payload),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["settings", "spark-config"] });
      setForm(null);
      toast({ title: "Spark configuration saved", description: "Changes will apply to new jobs." });
    },
    onError: () => {
      toast({ title: "Failed to save", description: "Check your permissions or values.", variant: "destructive" });
    },
  });

  const handleSave = () => saveMutation.mutate(config);

  const handleReset = () => setForm(null);

  const isDirty = form !== null;

  return (
    <div className="space-y-6 max-w-4xl">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Zap className="h-6 w-6 text-yellow-500" />
            Spark Configuration
          </h1>
          <p className="text-muted-foreground mt-1">
            Configure Spark compute settings for this environment. Applied to all new Spark jobs.
          </p>
        </div>
        <div className="flex items-center gap-2">
          {isDirty && (
            <Badge variant="secondary" className="text-orange-600 border-orange-300 bg-orange-50">
              Unsaved changes
            </Badge>
          )}
          <Button variant="outline" size="sm" onClick={handleReset} disabled={!isDirty}>
            <RefreshCw className="h-4 w-4 mr-1" /> Reset
          </Button>
          <Button size="sm" onClick={handleSave} disabled={saveMutation.isPending || isLoading}>
            <Save className="h-4 w-4 mr-1" />
            {saveMutation.isPending ? "Saving..." : "Save Changes"}
          </Button>
        </div>
      </div>

      {isLoading ? (
        <div className="text-center py-16 text-muted-foreground">Loading configuration...</div>
      ) : (
        <>
          {/* Cluster */}
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <Server className="h-4 w-4" /> Cluster
              </CardTitle>
              <CardDescription>Spark master endpoint and deployment mode</CardDescription>
            </CardHeader>
            <CardContent className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="md:col-span-2 space-y-1.5">
                <Label>Spark Master URL</Label>
                <Input
                  value={config.master}
                  onChange={(e) => set("master", e.target.value)}
                  placeholder="k8s://https://kubernetes.default.svc.cluster.local:443"
                  className="font-mono text-sm"
                />
                <p className="text-xs text-muted-foreground">
                  For on-cluster K8s: <code>k8s://https://kubernetes.default.svc.cluster.local:443</code>
                </p>
              </div>
              <div className="space-y-1.5">
                <Label>Deploy Mode</Label>
                <Input
                  value={config.deploy_mode}
                  onChange={(e) => set("deploy_mode", e.target.value)}
                  placeholder="cluster"
                />
              </div>
              <div className="space-y-1.5">
                <Label>Kubernetes Namespace</Label>
                <Input
                  value={config.namespace}
                  onChange={(e) => set("namespace", e.target.value)}
                  placeholder="spark"
                />
              </div>
              <div className="space-y-1.5">
                <Label>Service Account</Label>
                <Input
                  value={config.service_account}
                  onChange={(e) => set("service_account", e.target.value)}
                  placeholder="spark"
                />
              </div>
              <div className="space-y-1.5">
                <Label>Checkpoint Directory</Label>
                <Input
                  value={config.checkpoint_dir}
                  onChange={(e) => set("checkpoint_dir", e.target.value)}
                  placeholder="/tmp/spark-checkpoints"
                  className="font-mono text-sm"
                />
              </div>
            </CardContent>
          </Card>

          {/* Image */}
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base">Container Image</CardTitle>
              <CardDescription>Image registry, tag, and pull policy for Spark pods</CardDescription>
            </CardHeader>
            <CardContent className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="space-y-1.5">
                <Label>Image Registry</Label>
                <Input
                  value={config.image_registry}
                  onChange={(e) => set("image_registry", e.target.value)}
                  placeholder="ghcr.io/dcraftlabs"
                  className="font-mono text-sm"
                />
              </div>
              <div className="space-y-1.5">
                <Label>Image Tag</Label>
                <Input
                  value={config.image_tag}
                  onChange={(e) => set("image_tag", e.target.value)}
                  placeholder="3.4.1"
                  className="font-mono text-sm"
                />
              </div>
              <div className="space-y-1.5">
                <Label>Pull Policy</Label>
                <Input
                  value={config.image_pull_policy}
                  onChange={(e) => set("image_pull_policy", e.target.value)}
                  placeholder="IfNotPresent"
                />
              </div>
            </CardContent>
          </Card>

          {/* Resources */}
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <Cpu className="h-4 w-4" /> Resources
              </CardTitle>
              <CardDescription>CPU, memory and executor count for driver and executors</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div className="space-y-1.5">
                  <Label className="text-xs text-muted-foreground uppercase tracking-wide">Driver Cores</Label>
                  <Input value={config.driver_cores} onChange={(e) => set("driver_cores", e.target.value)} placeholder="1" />
                </div>
                <div className="space-y-1.5">
                  <Label className="text-xs text-muted-foreground uppercase tracking-wide">Driver Memory</Label>
                  <Input value={config.driver_memory} onChange={(e) => set("driver_memory", e.target.value)} placeholder="1g" />
                </div>
                <div className="space-y-1.5">
                  <Label className="text-xs text-muted-foreground uppercase tracking-wide">Executor Cores</Label>
                  <Input value={config.executor_cores} onChange={(e) => set("executor_cores", e.target.value)} placeholder="1" />
                </div>
                <div className="space-y-1.5">
                  <Label className="text-xs text-muted-foreground uppercase tracking-wide">Executor Memory</Label>
                  <Input value={config.executor_memory} onChange={(e) => set("executor_memory", e.target.value)} placeholder="1g" />
                </div>
              </div>

              <Separator className="my-4" />

              {/* Dynamic Allocation */}
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="font-medium text-sm">Dynamic Allocation</p>
                    <p className="text-xs text-muted-foreground">Automatically scale executors based on workload</p>
                  </div>
                  <Switch
                    checked={config.dynamic_allocation_enabled}
                    onCheckedChange={(v) => set("dynamic_allocation_enabled", v)}
                  />
                </div>

                {!config.dynamic_allocation_enabled ? (
                  <div className="space-y-1.5">
                    <Label className="text-xs text-muted-foreground uppercase tracking-wide">Fixed Executor Instances</Label>
                    <Input
                      value={config.executor_instances}
                      onChange={(e) => set("executor_instances", e.target.value)}
                      placeholder="2"
                      className="max-w-xs"
                    />
                  </div>
                ) : (
                  <div className="grid grid-cols-2 gap-4 max-w-sm">
                    <div className="space-y-1.5">
                      <Label className="text-xs text-muted-foreground uppercase tracking-wide">Min Executors</Label>
                      <Input
                        value={config.dynamic_allocation_min}
                        onChange={(e) => set("dynamic_allocation_min", e.target.value)}
                        placeholder="1"
                      />
                    </div>
                    <div className="space-y-1.5">
                      <Label className="text-xs text-muted-foreground uppercase tracking-wide">Max Executors</Label>
                      <Input
                        value={config.dynamic_allocation_max}
                        onChange={(e) => set("dynamic_allocation_max", e.target.value)}
                        placeholder="5"
                      />
                    </div>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>

          {/* Extra Spark conf */}
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base">Additional Spark Properties</CardTitle>
              <CardDescription>
                Extra <code>spark.*</code> properties passed directly to the job. E.g.{" "}
                <code>spark.sql.shuffle.partitions</code>
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-3">
              {Object.entries(config.extra_conf).map(([k, v]) => (
                <div key={k} className="flex items-center gap-2">
                  <Input value={k} readOnly className="font-mono text-sm bg-muted flex-1" />
                  <Input
                    value={v}
                    onChange={(e) => setExtra(k, e.target.value)}
                    className="font-mono text-sm flex-1"
                  />
                  <Button variant="ghost" size="icon" onClick={() => removeExtra(k)}>
                    <Trash2 className="h-4 w-4 text-destructive" />
                  </Button>
                </div>
              ))}

              {/* Add new row */}
              <div className="flex items-center gap-2 pt-1">
                <Input
                  value={newKey}
                  onChange={(e) => setNewKey(e.target.value)}
                  placeholder="spark.property.key"
                  className="font-mono text-sm flex-1"
                  onKeyDown={(e) => e.key === "Enter" && addExtra()}
                />
                <Input
                  value={newVal}
                  onChange={(e) => setNewVal(e.target.value)}
                  placeholder="value"
                  className="font-mono text-sm flex-1"
                  onKeyDown={(e) => e.key === "Enter" && addExtra()}
                />
                <Button variant="outline" size="icon" onClick={addExtra} disabled={!newKey.trim()}>
                  <Plus className="h-4 w-4" />
                </Button>
              </div>
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}
