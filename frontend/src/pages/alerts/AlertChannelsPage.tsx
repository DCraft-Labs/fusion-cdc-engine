import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { Plus } from "lucide-react";

export function AlertChannelsPage() {
  const navigate = useNavigate();

  const { data: channels = [], isLoading } = useQuery({
    queryKey: ["alerts", "channels"],
    queryFn: () => fetchList("/alerts/channels", "channels"),
  });

  function typeBadge(type: string) {
    const colors: Record<string, string> = {
      email: "secondary",
      slack: "outline",
      teams: "outline",
      webhook: "outline",
      pagerduty: "destructive",
    };
    return <Badge variant={(colors[type] ?? "outline") as any}>{type}</Badge>;
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Notification Channels</h1>
        <Button onClick={() => navigate("/alerts/channels/new")}>
          <Plus className="mr-2 h-4 w-4" />Create Channel
        </Button>
      </div>

      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Channel Name</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Rate Limit</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow><TableCell colSpan={5} className="text-center py-8">Loading...</TableCell></TableRow>
              ) : channels.length === 0 ? (
                <TableRow><TableCell colSpan={5} className="text-center text-muted-foreground py-8">No notification channels configured</TableCell></TableRow>
              ) : (
                channels.map((ch: any) => (
                  <TableRow
                    key={ch.id}
                    className="cursor-pointer hover:bg-muted/50"
                    onClick={() => navigate(`/alerts/channels/${ch.id}`)}
                  >
                    <TableCell className="font-medium">{ch.name}</TableCell>
                    <TableCell>{typeBadge(ch.type)}</TableCell>
                    <TableCell className="text-muted-foreground">
                      {ch.rate_limit_per_hour ? `${ch.rate_limit_per_hour}/hr` : "Unlimited"}
                      {ch.rate_limit_per_day ? `, ${ch.rate_limit_per_day}/day` : ""}
                    </TableCell>
                    <TableCell>
                      <Badge variant={ch.enabled !== false ? "success" : "secondary"}>
                        {ch.enabled !== false ? "Active" : "Disabled"}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <Button variant="ghost" size="sm" onClick={(e) => { e.stopPropagation(); navigate(`/alerts/channels/${ch.id}`); }}>
                        View
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
