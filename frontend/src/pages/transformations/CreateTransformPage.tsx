import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useMutation } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";

export function CreateTransformPage() {
  const navigate = useNavigate();
  const [form, setForm] = useState({
    pipeline_name: "",
    pipeline_type: "spark",
    language: "python",
    execution_mode: "batch",
    transformation_code: "",
    output_stream: "",
    input_streams: [] as string[],
  });

  const createMutation = useMutation({
    mutationFn: () => api.post("/transformations", {
      pipeline_name: form.pipeline_name,
      pipeline_type: form.pipeline_type,
      language: form.language,
      execution_mode: form.execution_mode,
      transformation_code: form.transformation_code,
      output_stream: form.output_stream,
      input_streams: form.input_streams,
    }),
    onSuccess: (res) => navigate(`/transformations/${res.data.pipeline_id}`),
  });

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Create Transformation</h1>
      <Card>
        <CardHeader><CardTitle>Transform Details</CardTitle></CardHeader>
        <CardContent>
          <form onSubmit={(e) => { e.preventDefault(); createMutation.mutate(); }} className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">Pipeline Name *</label>
              <Input
                value={form.pipeline_name}
                onChange={(e) => setForm({ ...form, pipeline_name: e.target.value })}
                placeholder="my_transform"
                required
              />
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
                    <SelectItem value="batch">Batch</SelectItem>
                    <SelectItem value="streaming">Streaming</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Output Stream *</label>
                <Input
                  value={form.output_stream}
                  onChange={(e) => setForm({ ...form, output_stream: e.target.value })}
                  placeholder="enriched_orders"
                  required
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Input Streams (comma-separated)</label>
                <Input
                  value={form.input_streams.join(", ")}
                  onChange={(e) => setForm({ ...form, input_streams: e.target.value.split(",").map((s) => s.trim()).filter(Boolean) })}
                  placeholder="orders, customers"
                />
              </div>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Transformation Code</label>
              <textarea
                className="w-full min-h-[250px] rounded-md border bg-muted p-3 font-mono text-sm focus:outline-none focus:ring-1 focus:ring-ring"
                value={form.transformation_code}
                onChange={(e) => setForm({ ...form, transformation_code: e.target.value })}
                placeholder={form.language === "python" ? "def transform(df):\n    return df" : "SELECT * FROM source_table"}
              />
            </div>
            <div className="flex gap-2">
              <Button type="button" variant="outline" onClick={() => navigate("/transformations")}>Cancel</Button>
              <Button type="submit" disabled={createMutation.isPending}>
                {createMutation.isPending ? "Creating..." : "Create"}
              </Button>
            </div>
          </form>
        </CardContent>
      </Card>
    </div>
  );
}
