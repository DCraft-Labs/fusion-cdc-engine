import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useState, useEffect } from "react";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Plus, X, Save } from "lucide-react";

interface Param {
  name: string;
  type: string;
  description: string;
}

export function EditUDFPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [form, setForm] = useState({
    name: "",
    description: "",
    language: "python",
    category: "transform",
    return_type: "StringType",
    code: "",
  });
  const [params, setParams] = useState<Param[]>([]);

  const { data: udf, isLoading } = useQuery({
    queryKey: ["udfs", id],
    queryFn: () => api.get(`/udfs/${id}`).then((r) => r.data),
  });

  useEffect(() => {
    if (udf) {
      setForm({
        name: udf.name ?? "",
        description: udf.description ?? "",
        language: udf.language ?? "python",
        category: udf.category ?? "transform",
        return_type: udf.return_type ?? "StringType",
        code: udf.code ?? "",
      });
      setParams(udf.parameters ?? []);
    }
  }, [udf]);

  const updateMutation = useMutation({
    mutationFn: () => api.put(`/udfs/${id}`, { ...form, parameters: params }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["udfs", id] });
      navigate(`/udfs/${id}`);
    },
  });

  const validateMutation = useMutation({
    mutationFn: () => api.post(`/udfs/${id}/validate`, { code: form.code }),
  });

  const addParam = () => setParams([...params, { name: "", type: "string", description: "" }]);
  const removeParam = (i: number) => setParams(params.filter((_, idx) => idx !== i));
  const updateParam = (i: number, field: keyof Param, value: string) =>
    setParams(params.map((p, idx) => (idx === i ? { ...p, [field]: value } : p)));

  if (isLoading) return <div className="flex items-center justify-center h-64">Loading...</div>;

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Edit UDF: {udf?.name}</h1>
      <Card>
        <CardHeader><CardTitle>UDF Configuration</CardTitle></CardHeader>
        <CardContent>
          <form onSubmit={(e) => { e.preventDefault(); updateMutation.mutate(); }} className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Name *</label>
                <Input value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} required />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Description</label>
                <Input value={form.description} onChange={(e) => setForm({ ...form, description: e.target.value })} />
              </div>
            </div>
            <div className="grid grid-cols-3 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Language</label>
                <Select value={form.language} onValueChange={(v) => setForm({ ...form, language: v })}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="python">Python</SelectItem>
                    <SelectItem value="scala">Scala</SelectItem>
                    <SelectItem value="java">Java</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Category</label>
                <Select value={form.category} onValueChange={(v) => setForm({ ...form, category: v })}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="transform">Transform</SelectItem>
                    <SelectItem value="security">Security</SelectItem>
                    <SelectItem value="validation">Validation</SelectItem>
                    <SelectItem value="enrichment">Enrichment</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Return Type</label>
                <Select value={form.return_type} onValueChange={(v) => setForm({ ...form, return_type: v })}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="StringType">String</SelectItem>
                    <SelectItem value="IntegerType">Integer</SelectItem>
                    <SelectItem value="FloatType">Float</SelectItem>
                    <SelectItem value="BooleanType">Boolean</SelectItem>
                    <SelectItem value="ArrayType">Array</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <label className="text-sm font-medium">Parameters</label>
                <Button type="button" variant="outline" size="sm" onClick={addParam}>
                  <Plus className="mr-1 h-3 w-3" />Add
                </Button>
              </div>
              {params.length > 0 && (
                <div className="space-y-2">
                  {params.map((p, i) => (
                    <div key={i} className="flex items-center gap-2">
                      <Input
                        placeholder="name"
                        value={p.name}
                        onChange={(e) => updateParam(i, "name", e.target.value)}
                        className="flex-1"
                      />
                      <Input
                        placeholder="type"
                        value={p.type}
                        onChange={(e) => updateParam(i, "type", e.target.value)}
                        className="flex-1"
                      />
                      <Input
                        placeholder="description"
                        value={p.description}
                        onChange={(e) => updateParam(i, "description", e.target.value)}
                        className="flex-[2]"
                      />
                      <Button type="button" variant="ghost" size="icon" onClick={() => removeParam(i)}>
                        <X className="h-4 w-4" />
                      </Button>
                    </div>
                  ))}
                </div>
              )}
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Code *</label>
              <textarea
                className="w-full min-h-[250px] rounded-md border bg-muted p-3 font-mono text-sm focus:outline-none focus:ring-1 focus:ring-ring"
                value={form.code}
                onChange={(e) => setForm({ ...form, code: e.target.value })}
                required
              />
            </div>

            {validateMutation.data && (
              <div className={`rounded-md p-3 text-sm ${validateMutation.data.data?.valid !== false ? "bg-green-500/10 text-green-600" : "bg-destructive/10 text-destructive"}`}>
                {validateMutation.data.data?.valid !== false ? "✓ Valid" : `✗ ${validateMutation.data.data?.error ?? "Invalid"}`}
              </div>
            )}

            <div className="flex gap-2 pt-4">
              <Button type="submit" disabled={updateMutation.isPending}>
                <Save className="mr-2 h-4 w-4" />{updateMutation.isPending ? "Saving..." : "Save Changes"}
              </Button>
              <Button type="button" variant="outline" onClick={() => validateMutation.mutate()} disabled={validateMutation.isPending}>
                Validate
              </Button>
              <Button type="button" variant="ghost" onClick={() => navigate(`/udfs/${id}`)}>Cancel</Button>
            </div>
          </form>
        </CardContent>
      </Card>
    </div>
  );
}
