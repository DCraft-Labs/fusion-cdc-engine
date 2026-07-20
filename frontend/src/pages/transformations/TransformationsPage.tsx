import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Plus, Shuffle } from "lucide-react";

export function TransformationsPage() {
  const navigate = useNavigate();
  const [typeFilter, setTypeFilter] = useState("all");
  const [languageFilter, setLanguageFilter] = useState("all");
  const [statusFilter, setStatusFilter] = useState("all");

  const { data: transforms, isLoading } = useQuery({
    queryKey: ["transformations"],
    queryFn: () => fetchList("/transformations", "pipelines").catch(() => []),
  });

  const filtered = (Array.isArray(transforms) ? transforms : []).filter((t: any) => {
    if (typeFilter !== "all" && t.pipeline_type !== typeFilter) return false;
    if (languageFilter !== "all" && t.language !== languageFilter) return false;
    const status = t.is_published ? "published" : "draft";
    if (statusFilter !== "all" && status !== statusFilter) return false;
    return true;
  });

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Transformations</h1>
        <Button onClick={() => navigate("/transformations/new")}>
          <Plus className="mr-2 h-4 w-4" />New Transform
        </Button>
      </div>

      <div className="flex items-center gap-4">
        <Select value={typeFilter} onValueChange={setTypeFilter}>
          <SelectTrigger className="w-[150px]"><SelectValue placeholder="Type" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Types</SelectItem>
            <SelectItem value="sql">SQL</SelectItem>
            <SelectItem value="python">Python</SelectItem>
          </SelectContent>
        </Select>
        <Select value={languageFilter} onValueChange={setLanguageFilter}>
          <SelectTrigger className="w-[150px]"><SelectValue placeholder="Language" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Languages</SelectItem>
            <SelectItem value="python">Python</SelectItem>
            <SelectItem value="sql">SQL</SelectItem>
          </SelectContent>
        </Select>
        <Select value={statusFilter} onValueChange={setStatusFilter}>
          <SelectTrigger className="w-[150px]"><SelectValue placeholder="Status" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Statuses</SelectItem>
            <SelectItem value="published">Published</SelectItem>
            <SelectItem value="draft">Draft</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Language</TableHead>
                <TableHead>Version</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow><TableCell colSpan={6} className="text-center">Loading...</TableCell></TableRow>
              ) : filtered.length === 0 ? (
                <TableRow><TableCell colSpan={6} className="text-center text-muted-foreground">No transformations found</TableCell></TableRow>
              ) : (
                filtered.map((t: any) => (
                  <TableRow
                    key={t.pipeline_id}
                    className="cursor-pointer hover:bg-muted/50"
                    onClick={() => navigate(`/transformations/${t.pipeline_id}`)}
                  >
                    <TableCell className="font-medium">
                      <div className="flex items-center gap-2">
                        <Shuffle className="h-4 w-4 text-muted-foreground" />
                        {t.pipeline_name}
                      </div>
                    </TableCell>
                    <TableCell><Badge variant="outline">{t.pipeline_type ?? "spark"}</Badge></TableCell>
                    <TableCell><Badge variant="outline">{t.language ?? "python"}</Badge></TableCell>
                    <TableCell className="text-muted-foreground">v{t.version ?? 1}</TableCell>
                    <TableCell>
                      <Badge variant={t.is_published ? "success" : "secondary"}>
                        {t.is_published ? "published" : "draft"}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <Button
                        size="sm"
                        variant="ghost"
                        onClick={(e) => { e.stopPropagation(); navigate(`/transformations/${t.pipeline_id}/edit`); }}
                      >
                        Edit
                      </Button>
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
