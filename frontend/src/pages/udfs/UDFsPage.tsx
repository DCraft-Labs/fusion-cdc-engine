import { useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { Plus, Code2 } from "lucide-react";

export function UDFsPage() {
  const navigate = useNavigate();

  const { data: udfs, isLoading } = useQuery({
    queryKey: ["udfs"],
    queryFn: () => fetchList("/udfs", "udfs"),
  });

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">User-Defined Functions</h1>
        <Button onClick={() => navigate("/udfs/new")}>
          <Plus className="mr-2 h-4 w-4" />Register UDF
        </Button>
      </div>

      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Language</TableHead>
                <TableHead>Category</TableHead>
                <TableHead>Return Type</TableHead>
                <TableHead>Status</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow><TableCell colSpan={5} className="text-center">Loading...</TableCell></TableRow>
              ) : (udfs ?? []).length === 0 ? (
                <TableRow><TableCell colSpan={5} className="text-center text-muted-foreground">No UDFs registered</TableCell></TableRow>
              ) : (
                (udfs ?? []).map((udf: any) => (
                  <TableRow
                    key={udf.id}
                    className="cursor-pointer hover:bg-muted/50"
                    onClick={() => navigate(`/udfs/${udf.id}`)}
                  >
                    <TableCell className="font-medium">
                      <div className="flex items-center gap-2">
                        <Code2 className="h-4 w-4 text-muted-foreground" />
                        {udf.name}
                      </div>
                    </TableCell>
                    <TableCell><Badge variant="outline">{udf.language ?? "python"}</Badge></TableCell>
                    <TableCell className="text-muted-foreground capitalize">{udf.category ?? "—"}</TableCell>
                    <TableCell className="text-muted-foreground">{udf.return_type ?? "—"}</TableCell>
                    <TableCell>
                      <Badge variant={udf.status === "published" ? "success" : "secondary"}>
                        {udf.status ?? "draft"}
                      </Badge>
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
