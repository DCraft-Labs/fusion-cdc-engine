import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import {
  Table, TableHeader, TableBody, TableRow, TableHead, TableCell,
} from "@/components/ui/table";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import { Search, XCircle } from "lucide-react";

interface SparkJob {
  id: string;
  connection_id: string;
  connection_name: string;
  type: string;
  status: string;
  submitted_at: string;
}

const statusBadgeVariant = (status: string) => {
  switch (status) {
    case "running": return "default";
    case "queued": return "secondary";
    case "completed": return "outline";
    case "failed": return "destructive";
    default: return "secondary";
  }
};

export function SparkJobsPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [tab, setTab] = useState("queued");
  const [connectionFilter, setConnectionFilter] = useState("all");
  const [typeFilter, setTypeFilter] = useState("all");
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState<Set<string>>(new Set());

  const { data: jobs = [], isLoading } = useQuery<SparkJob[]>({
    queryKey: ["spark-jobs"],
    queryFn: () => fetchList("/spark-jobs", "jobs").catch(() => []),
  });

  const cancelMutation = useMutation({
    mutationFn: (ids: string[]) => api.post("/spark-jobs/cancel", { job_ids: ids }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["spark-jobs"] });
      setSelected(new Set());
    },
  });

  const counts = {
    queued: jobs.filter((j) => j.status === "queued").length,
    running: jobs.filter((j) => j.status === "running").length,
    completed: jobs.filter((j) => j.status === "completed").length,
    failed: jobs.filter((j) => j.status === "failed").length,
  };

  const filtered = jobs.filter((j) => {
    if (j.status !== tab) return false;
    if (connectionFilter !== "all" && j.connection_id !== connectionFilter) return false;
    if (typeFilter !== "all" && j.type !== typeFilter) return false;
    if (search && !j.id.includes(search) && !j.connection_name?.toLowerCase().includes(search.toLowerCase())) return false;
    return true;
  });

  const connections = [...new Map(jobs.map((j) => [j.connection_id, j.connection_name])).entries()];

  const toggleSelect = (id: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const toggleAll = () => {
    if (selected.size === filtered.length) setSelected(new Set());
    else setSelected(new Set(filtered.map((j) => j.id)));
  };

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Spark Jobs</h1>

      <Tabs value={tab} onValueChange={(v) => { setTab(v); setSelected(new Set()); }}>
        <TabsList>
          <TabsTrigger value="queued">Queue ({counts.queued})</TabsTrigger>
          <TabsTrigger value="running">Running ({counts.running})</TabsTrigger>
          <TabsTrigger value="completed">Completed ({counts.completed})</TabsTrigger>
          <TabsTrigger value="failed">Failed ({counts.failed})</TabsTrigger>
        </TabsList>

        <div className="flex gap-3 items-center mt-4">
          <Select value={connectionFilter} onValueChange={setConnectionFilter}>
            <SelectTrigger className="w-[180px]">
              <SelectValue placeholder="Connection" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Connections</SelectItem>
              {connections.map(([id, name]) => (
                <SelectItem key={id} value={id}>{name}</SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select value={typeFilter} onValueChange={setTypeFilter}>
            <SelectTrigger className="w-[140px]">
              <SelectValue placeholder="Type" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Types</SelectItem>
              <SelectItem value="batch">Batch</SelectItem>
              <SelectItem value="backfill">Backfill</SelectItem>
            </SelectContent>
          </Select>
          <div className="relative flex-1 max-w-xs">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              className="pl-9"
              placeholder="Search by Job ID or Connection..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
          </div>
        </div>

        {["queued", "running", "completed", "failed"].map((status) => (
          <TabsContent key={status} value={status}>
            <Card>
              <CardContent className="p-0">
                <Table>
                  <TableHeader>
                    <TableRow>
                      {(tab === "queued" || tab === "running") && (
                        <TableHead className="w-[40px]">
                          <input
                            type="checkbox"
                            checked={filtered.length > 0 && selected.size === filtered.length}
                            onChange={toggleAll}
                            className="rounded border-muted-foreground"
                          />
                        </TableHead>
                      )}
                      <TableHead>Job ID</TableHead>
                      <TableHead>Connection</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Submitted</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {isLoading ? (
                      <TableRow>
                        <TableCell colSpan={6} className="text-center">Loading...</TableCell>
                      </TableRow>
                    ) : !filtered.length ? (
                      <TableRow>
                        <TableCell colSpan={6} className="text-center text-muted-foreground py-8">
                          No jobs
                        </TableCell>
                      </TableRow>
                    ) : (
                      filtered.map((job) => (
                        <TableRow
                          key={job.id}
                          className="cursor-pointer"
                          onClick={() => navigate(`/spark-jobs/${job.id}`)}
                        >
                          {(tab === "queued" || tab === "running") && (
                            <TableCell onClick={(e) => e.stopPropagation()}>
                              <input
                                type="checkbox"
                                checked={selected.has(job.id)}
                                onChange={() => toggleSelect(job.id)}
                                className="rounded border-muted-foreground"
                              />
                            </TableCell>
                          )}
                          <TableCell className="font-mono text-xs">{job.id.slice(0, 12)}</TableCell>
                          <TableCell>{job.connection_name}</TableCell>
                          <TableCell>
                            <Badge variant="outline" className="capitalize">{job.type}</Badge>
                          </TableCell>
                          <TableCell>
                            <Badge variant={statusBadgeVariant(job.status)} className="capitalize">
                              {job.status}
                            </Badge>
                          </TableCell>
                          <TableCell className="text-muted-foreground">
                            {new Date(job.submitted_at).toLocaleString()}
                          </TableCell>
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </TabsContent>
        ))}
      </Tabs>

      {selected.size > 0 && (tab === "queued" || tab === "running") && (
        <div className="flex items-center gap-3">
          <span className="text-sm text-muted-foreground">{selected.size} selected</span>
          <Button
            variant="destructive"
            size="sm"
            onClick={() => cancelMutation.mutate([...selected])}
            disabled={cancelMutation.isPending}
          >
            <XCircle className="mr-2 h-4 w-4" />
            Cancel Selected
          </Button>
        </div>
      )}
    </div>
  );
}
