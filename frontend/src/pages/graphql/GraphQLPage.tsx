import { useState } from "react";
import { api } from "@/lib/api";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { ExternalLink, Play, Code2 } from "lucide-react";

const GRAPHQL_URL = "http://localhost:30800/graphql";

const EXAMPLE_QUERIES = [
  {
    label: "List Connections",
    query: `query {
  connections {
    connectionId
    connectionName
    status
    syncMode
  }
}`,
  },
  {
    label: "List Sources",
    query: `query {
  sources {
    sourceId
    sourceName
    connectorType
    status
  }
}`,
  },
  {
    label: "List Destinations",
    query: `query {
  destinations {
    destinationId
    destinationName
    connectorType
    status
  }
}`,
  },
  {
    label: "Streams for Connection",
    query: `query {
  streams(connectionId: "REPLACE_WITH_CONNECTION_ID") {
    streamId
    streamName
    syncMode
    status
  }
}`,
  },
  {
    label: "DQ Policies",
    query: `query {
  dqPolicies {
    policyId
    policyName
    isActive
    connectionId
  }
}`,
  },
  {
    label: "Schema Introspection",
    query: `{
  __schema {
    queryType { name }
    mutationType { name }
    types {
      name
      kind
    }
  }
}`,
  },
  {
    label: "Create Connection (mutation)",
    query: `mutation {
  createConnection(input: {
    connectionName: "My Connection"
    sourceId: "REPLACE_WITH_SOURCE_ID"
    destinationId: "REPLACE_WITH_DEST_ID"
    syncMode: "incremental"
    syncType: "REALTIME"
  }) {
    connectionId
    connectionName
    status
  }
}`,
  },
];

export function GraphQLPage() {
  const [query, setQuery] = useState(EXAMPLE_QUERIES[0].query);
  const [result, setResult] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const runQuery = async () => {
    setLoading(true);
    setResult(null);
    setError(null);
    try {
      const resp = await api.post("/graphql", { query });
      if (resp.data?.errors?.length) {
        setError(resp.data.errors.map((e: any) => e.message).join("\n"));
        setResult(JSON.stringify(resp.data, null, 2));
      } else {
        setResult(JSON.stringify(resp.data, null, 2));
      }
    } catch (e: any) {
      const msg = e.response?.data?.detail ?? e.response?.data ?? e.message ?? "Query failed";
      setError(typeof msg === "string" ? msg : JSON.stringify(msg, null, 2));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Code2 className="h-6 w-6" />
          <h1 className="text-2xl font-bold">GraphQL Explorer</h1>
        </div>
        <a href={GRAPHQL_URL} target="_blank" rel="noreferrer">
          <Button variant="outline" size="sm">
            <ExternalLink className="h-4 w-4 mr-1" />Open GraphiQL UI
          </Button>
        </a>
      </div>

      <Card>
        <CardContent className="pt-4">
          <p className="text-sm text-muted-foreground mb-2">
            The Fusion control plane exposes a{" "}
            <strong>Strawberry GraphQL</strong> endpoint at{" "}
            <code className="text-xs bg-muted px-1 py-0.5 rounded">{GRAPHQL_URL}</code>.
            Use the interactive GraphiQL UI (button above) or run queries directly below.
          </p>
          <div className="flex gap-2 flex-wrap">
            <Badge variant="outline">POST /graphql</Badge>
            <Badge variant="outline">Strawberry GraphQL</Badge>
            <Badge variant="outline">Bearer token auth</Badge>
          </div>
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        {/* Examples panel */}
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Examples</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 p-3">
            {EXAMPLE_QUERIES.map((ex) => (
              <Button
                key={ex.label}
                variant="ghost"
                size="sm"
                className="w-full justify-start text-left text-xs"
                onClick={() => setQuery(ex.query)}
              >
                {ex.label}
              </Button>
            ))}
          </CardContent>
        </Card>

        {/* Editor + results */}
        <div className="md:col-span-3 space-y-3">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm flex items-center justify-between">
                Query Editor
                <Button size="sm" onClick={runQuery} disabled={loading}>
                  <Play className="h-3.5 w-3.5 mr-1" />
                  {loading ? "Running..." : "Run Query"}
                </Button>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <Textarea
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                className="font-mono text-sm min-h-[200px] resize-y"
                placeholder="Enter GraphQL query..."
              />
            </CardContent>
          </Card>

          {error && (
            <Card className="border-destructive">
              <CardContent className="pt-4">
                <p className="text-destructive text-sm font-medium mb-1">Error</p>
                <pre className="text-xs text-destructive/80 whitespace-pre-wrap">{error}</pre>
              </CardContent>
            </Card>
          )}

          {result && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm">Result</CardTitle>
              </CardHeader>
              <CardContent>
                <pre className="bg-muted rounded-md p-3 text-xs font-mono overflow-auto max-h-[400px] whitespace-pre-wrap">
                  {result}
                </pre>
              </CardContent>
            </Card>
          )}
        </div>
      </div>
    </div>
  );
}
