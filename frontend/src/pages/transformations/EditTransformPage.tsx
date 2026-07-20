import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useState, useEffect } from "react";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Save, CheckCircle, Play } from "lucide-react";

export function EditTransformPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [form, setForm] = useState({
    pipeline_name: "",
    pipeline_type: "spark",
    language: "python",
    execution_mode: "batch",
    transformation_code: "",
    output_stream: "",
    input_streams: "" as string,
  });

  const { data: transform, isLoading } = useQuery({
    queryKey: ["transformations", id],
    queryFn: () => api.get(`/transformations/${id}`).then((r) => r.data),
  });

  useEffect(() => {
    if (transform) {
      setForm({
        pipeline_name: transform.pipeline_name ?? "",
        pipeline_type: transform.pipeline_type ?? "spark",
        language: transform.language ?? "python",
        execution_mode: transform.execution_mode ?? "batch",
        transformation_code: transform.transformation_code ?? "",
        output_stream: transform.output_stream ?? "",
        input_streams: (transform.input_streams ?? []).join(", "),
      });
    }
  }, [transform]);

  const updateMutation = useMutation({
    mutationFn: () => api.patch(`/transformations/${id}`, {
      pipeline_name: form.pipeline_name,
      pipeline_type: form.pipeline_type,
      language: form.language,
      execution_mode: form.execution_mode,
      transformation_code: form.transformation_code,
      output_stream: form.output_stream,
      input_streams: form.input_streams.split(",").map((s) => s.trim()).filter(Boolean),
    }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["transformations", id] });
      navigate(`/transformations/${id}`);
    },
  });

  const validateMutation = useMutation({
    mutationFn: () => api.post(`/transformations/${id}/validate`, { transformation_code: form.transformation_code }),
  });

  const previewMutation = useMutation({
    mutationFn: () => api.post(`/transformations/${id}/preview`, { transformation_code: form.transformation_code }),
  });

  if (isLoading) return <div className="flex items-center justify-center h-64">Loading...</div>;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-2xl font-bold">Edit: {transform?.pipeline_name}</h1>
          <Badge variant="outline">v{transform?.version ?? 1}</Badge>
          <Badge variant={transform?.is_published ? "success" : "secondary"}>
            {transform?.is_published ? "published" : "draft"}
          </Badge>
        </div>
        <Button variant="outline" onClick={() => validateMutation.mutate()} disabled={validateMutation.isPending}>
          <CheckCircle className="mr-2 h-4 w-4" />Validate
        </Button>
      </div>

      <Card>
        <CardHeader><CardTitle>Transform Configuration</CardTitle></CardHeader>
        <CardContent>
          <form onSubmit={(e) => { e.preventDefault(); updateMutation.mutate(); }} className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">Pipeline Name *</label>
              <Input value={form.pipeline_name} onChange={(e) => setForm({ ...form, pipeline_name: e.target.value })} required />
            </div>
            <div className="grid grid-cols-3 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Type</label>
                <Select value={form.pipeline_type} onValueChange={(v) => setForm({ ...form, pipeline_type: v })}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="spark">Spark</SelectItem>
                    <SelectItem value="sql">SQL</SelectItem>
                    <SelectItem value="python">Python</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Language</label>
                <Select value={form.language} onValueChange={(v) => setForm({ ...form, language: v })}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="python">Python</SelectItem>
                    <SelectItem value="sql">SQL</SelectItem>
                    <SelectItem value="scala">Scala</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Execution Mode</label>
                <Select value={form.execution_mode} onValueChange={(v) => setForm({ ...form, execution_mode: v })}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="streaming">Streaming</SelectItem>
                    <SelectItem value="batch">Batch</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Output Stream</label>
                <Input value={form.output_stream} onChange={(e) => setForm({ ...form, output_stream: e.target.value })} placeholder="enriched_orders" />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Input Streams (comma-separated)</label>
                <Input value={form.input_streams} onChange={(e) => setForm({ ...form, input_streams: e.target.value })} placeholder="orders, customers" />
              </div>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Transformation Code *</label>
              <div className="relative">
                <div className="absolute left-0 top-0 bottom-0 w-10 bg-muted/50 rounded-l-md flex flex-col items-center pt-3 text-xs text-muted-foreground font-mono select-none">
                  {form.transformation_code.split("\n").map((_, i) => (
                    <div key={i} className="leading-5">{i + 1}</div>
                  ))}
                </div>
                <textarea
                  className="w-full min-h-[350px] rounded-md border bg-muted p-3 pl-12 font-mono text-sm leading-5 focus:outline-none focus:ring-1 focus:ring-ring resize-y"
                  value={form.transformation_code}
                  onChange={(e) => setForm({ ...form, transformation_code: e.target.value })}
                  required
                />
              </div>
            </div>

            {previewMutation.data && (
              <Card>
                <CardContent className="pt-4">
                  <h4 className="text-sm font-medium mb-2">Preview Output</h4>
                  <pre className="rounded-md bg-muted p-3 text-xs overflow-auto max-h-[200px]">
                    {JSON.stringify(previewMutation.data.data, null, 2)}
                  </pre>
                </CardContent>
              </Card>
            )}

            {validateMutation.data && (
              <div className={`rounded-md p-3 text-sm ${validateMutation.data.data?.valid !== false ? "bg-green-500/10 text-green-600" : "bg-destructive/10 text-destructive"}`}>
                {validateMutation.data.data?.valid !== false ? "✓ Validation passed" : `✗ ${validateMutation.data.data?.error ?? "Validation failed"}`}
              </div>
            )}

            <div className="flex gap-2 pt-4">
              <Button type="submit" disabled={updateMutation.isPending}>
                <Save className="mr-2 h-4 w-4" />{updateMutation.isPending ? "Saving..." : "Save Changes"}
              </Button>
              <Button type="button" variant="outline" onClick={() => validateMutation.mutate()} disabled={validateMutation.isPending}>
                Validate
              </Button>
              <Button type="button" variant="outline" onClick={() => previewMutation.mutate()} disabled={previewMutation.isPending}>
                <Play className="mr-2 h-4 w-4" />Preview
              </Button>
              <Button type="button" variant="ghost" onClick={() => navigate(`/transformations/${id}`)}>Cancel</Button>
            </div>
          </form>
        </CardContent>
      </Card>
    </div>
  );
}
