import { useState } from "react";
import { useParams } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from "@/components/ui/table";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import {
  Select,
  SelectTrigger,
  SelectValue,
  SelectContent,
  SelectItem,
} from "@/components/ui/select";
import { Plus, Clock, CheckCircle, XCircle, Trash2 } from "lucide-react";

interface SchemaChange {
  id: string;
  description: string;
  change_type: string;
  table_name: string;
  is_breaking: boolean;
  status: "pending" | "applied" | "rejected";
  detected_at: string;
}

interface FlattenRule {
  id: string;
  source_column: string;
  json_path: string;
  dest_column: string;
  cast_type: string;
}

export function ConnectionSchemaPage() {
  const { connectionId } = useParams<{ connectionId: string }>();
  const queryClient = useQueryClient();
  const [createRuleOpen, setCreateRuleOpen] = useState(false);
  const [newRule, setNewRule] = useState({
    source_column: "",
    json_path: "",
    dest_column: "",
    cast_type: "String",
  });

  const { data: schema } = useQuery({
    queryKey: ["schema-evolution", connectionId],
    queryFn: () => api.get(`/schema-evolution/${connectionId}`).then((r) => r.data),
  });

  const { data: changes } = useQuery<SchemaChange[]>({
    queryKey: ["schema-evolution", connectionId, "changes"],
    queryFn: () =>
      api
        .get(`/schema-evolution/${connectionId}/changes`)
        .then((r) => r.data)
        .catch(() => []),
  });

  const { data: flattenRules } = useQuery<FlattenRule[]>({
    queryKey: ["schema-evolution", connectionId, "flatten-rules"],
    queryFn: () =>
      api
        .get(`/schema-evolution/${connectionId}/flatten-rules`)
        .then((r) => r.data)
        .catch(() => []),
  });

  const { data: jsonSchemas } = useQuery({
    queryKey: ["schema-evolution", connectionId, "json-schemas"],
    queryFn: () =>
      api
        .get(`/schema-evolution/${connectionId}/json-schemas`)
        .then((r) => r.data)
        .catch(() => null),
  });

  const createRuleMutation = useMutation({
    mutationFn: (rule: typeof newRule) =>
      api.post(`/schema-evolution/${connectionId}/flatten-rules`, rule),
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ["schema-evolution", connectionId, "flatten-rules"],
      });
      setCreateRuleOpen(false);
      setNewRule({ source_column: "", json_path: "", dest_column: "", cast_type: "String" });
    },
  });

  const deleteRuleMutation = useMutation({
    mutationFn: (ruleId: string) =>
      api.delete(`/schema-evolution/${connectionId}/flatten-rules/${ruleId}`),
    onSuccess: () =>
      queryClient.invalidateQueries({
        queryKey: ["schema-evolution", connectionId, "flatten-rules"],
      }),
  });

  const statusIcon = (status: string) => {
    switch (status) {
      case "pending":
        return <Clock className="h-4 w-4 text-yellow-500" />;
      case "applied":
        return <CheckCircle className="h-4 w-4 text-green-500" />;
      case "rejected":
        return <XCircle className="h-4 w-4 text-red-500" />;
      default:
        return null;
    }
  };

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">
        {schema?.connection_name ?? connectionId}
      </h1>

      <Tabs defaultValue="changes">
        <TabsList>
          <TabsTrigger value="changes">Schema Changes</TabsTrigger>
          <TabsTrigger value="flatten">JSON Flatten Rules</TabsTrigger>
          <TabsTrigger value="schemas">JSON Schemas</TabsTrigger>
        </TabsList>

        {/* Schema Changes Tab */}
        <TabsContent value="changes">
          <Card>
            <CardContent className="p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Status</TableHead>
                    <TableHead>Table</TableHead>
                    <TableHead>Change</TableHead>
                    <TableHead>Type</TableHead>
                    <TableHead>Breaking?</TableHead>
                    <TableHead>Detected</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {!changes || changes.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={6} className="text-center text-muted-foreground">
                        No schema changes for this connection
                      </TableCell>
                    </TableRow>
                  ) : (
                    changes.map((c) => (
                      <TableRow key={c.id}>
                        <TableCell>{statusIcon(c.status)}</TableCell>
                        <TableCell className="font-mono text-sm">{c.table_name}</TableCell>
                        <TableCell className="max-w-xs truncate">{c.description}</TableCell>
                        <TableCell>
                          <Badge variant="outline">{c.change_type}</Badge>
                        </TableCell>
                        <TableCell>
                          {c.is_breaking ? (
                            <Badge variant="destructive">Yes</Badge>
                          ) : (
                            <span className="text-sm text-muted-foreground">No</span>
                          )}
                        </TableCell>
                        <TableCell className="text-muted-foreground">
                          {new Date(c.detected_at).toLocaleString()}
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>

        {/* JSON Flatten Rules Tab */}
        <TabsContent value="flatten">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between">
              <CardTitle>JSON Flatten Rules</CardTitle>
              <Button size="sm" onClick={() => setCreateRuleOpen(true)}>
                <Plus className="h-4 w-4 mr-1" /> Add Rule
              </Button>
            </CardHeader>
            <CardContent className="p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Source Column</TableHead>
                    <TableHead>JSON Path</TableHead>
                    <TableHead>Dest Column</TableHead>
                    <TableHead>Type</TableHead>
                    <TableHead>Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {!flattenRules || flattenRules.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={5} className="text-center text-muted-foreground">
                        No flatten rules configured
                      </TableCell>
                    </TableRow>
                  ) : (
                    flattenRules.map((rule) => (
                      <TableRow key={rule.id}>
                        <TableCell className="font-mono text-sm">{rule.source_column}</TableCell>
                        <TableCell className="font-mono text-sm">{rule.json_path}</TableCell>
                        <TableCell className="font-mono text-sm">{rule.dest_column}</TableCell>
                        <TableCell>
                          <Badge variant="outline">{rule.cast_type}</Badge>
                        </TableCell>
                        <TableCell>
                          <Button
                            size="sm"
                            variant="ghost"
                            onClick={() => deleteRuleMutation.mutate(rule.id)}
                          >
                            <Trash2 className="h-4 w-4 text-destructive" />
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>

        {/* JSON Schemas Tab */}
        <TabsContent value="schemas">
          <Card>
            <CardHeader>
              <CardTitle>JSON Schemas</CardTitle>
            </CardHeader>
            <CardContent>
              {jsonSchemas ? (
                <pre className="rounded-md bg-muted p-4 text-sm overflow-auto max-h-[600px] font-mono">
                  {JSON.stringify(jsonSchemas, null, 2)}
                </pre>
              ) : (
                <p className="text-muted-foreground">No JSON schemas available</p>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>

      {/* Create Rule Dialog */}
      <Dialog open={createRuleOpen} onOpenChange={setCreateRuleOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Create Flatten Rule</DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div>
              <label className="text-sm font-medium">Source Column</label>
              <Input
                className="mt-1"
                placeholder="e.g. payload"
                value={newRule.source_column}
                onChange={(e) => setNewRule({ ...newRule, source_column: e.target.value })}
              />
            </div>
            <div>
              <label className="text-sm font-medium">JSON Path</label>
              <Input
                className="mt-1"
                placeholder="e.g. $.user.name"
                value={newRule.json_path}
                onChange={(e) => setNewRule({ ...newRule, json_path: e.target.value })}
              />
            </div>
            <div>
              <label className="text-sm font-medium">Destination Column Name</label>
              <Input
                className="mt-1"
                placeholder="e.g. user_name"
                value={newRule.dest_column}
                onChange={(e) => setNewRule({ ...newRule, dest_column: e.target.value })}
              />
            </div>
            <div>
              <label className="text-sm font-medium">Cast Type</label>
              <Select
                value={newRule.cast_type}
                onValueChange={(val) => setNewRule({ ...newRule, cast_type: val })}
              >
                <SelectTrigger className="mt-1">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="String">String</SelectItem>
                  <SelectItem value="Integer">Integer</SelectItem>
                  <SelectItem value="Float">Float</SelectItem>
                  <SelectItem value="Boolean">Boolean</SelectItem>
                  <SelectItem value="Timestamp">Timestamp</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
          <DialogFooter className="gap-2">
            <Button variant="outline" onClick={() => setCreateRuleOpen(false)}>
              Cancel
            </Button>
            <Button
              variant="outline"
              onClick={() => {
                /* Preview would open a preview pane - placeholder */
              }}
            >
              Preview
            </Button>
            <Button
              onClick={() => createRuleMutation.mutate(newRule)}
              disabled={
                !newRule.source_column ||
                !newRule.json_path ||
                !newRule.dest_column ||
                createRuleMutation.isPending
              }
            >
              Create Rule
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
