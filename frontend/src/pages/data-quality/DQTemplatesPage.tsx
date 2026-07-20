import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api, fetchList } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardHeader, CardTitle, CardContent, CardFooter } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

const BUILTIN_TEMPLATES = [
  { id: "null_check", name: "Null Check", rule_type: "null_check", description: "Ensure specified columns contain no null values", default_config: { threshold: 0 } },
  { id: "null_ratio", name: "Null Ratio Check", rule_type: "null_ratio_check", description: "Allow nulls up to a specified percentage threshold", default_config: { threshold: 0.05 } },
  { id: "range", name: "Range Validation", rule_type: "range_check", description: "Validate numeric values fall within an expected min/max range", default_config: { min: 0, max: 100 } },
  { id: "regex", name: "Regex Pattern Match", rule_type: "regex_check", description: "Match column values against a regex pattern (email, phone, etc.)", default_config: { pattern: "^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$" } },
  { id: "freshness", name: "Freshness Check", rule_type: "freshness_check", description: "Ensure data has been updated within a specified time window", default_config: { max_age_hours: 24 } },
  { id: "row_count", name: "Row Count Match", rule_type: "row_count_match", description: "Compare row counts between source and destination to detect data loss", default_config: { tolerance_percent: 1 } },
  { id: "enum", name: "Enum Validation", rule_type: "enum_check", description: "Ensure column values belong to a predefined set of allowed values", default_config: { allowed_values: [] } },
  { id: "custom_sql", name: "Custom SQL", rule_type: "custom_sql", description: "Write a custom SQL query that must return 0 rows to pass", default_config: { query: "" } },
  { id: "uniqueness", name: "Uniqueness Check", rule_type: "uniqueness", description: "Ensure column values are unique with no duplicates", default_config: { threshold: 0 } },
  { id: "referential", name: "Referential Integrity", rule_type: "referential_integrity", description: "Validate foreign key references exist in the target table", default_config: { reference_table: "", reference_column: "" } },
];

export function DQTemplatesPage() {
  const navigate = useNavigate();

  const { data: apiTemplates } = useQuery({
    queryKey: ["data-quality", "templates"],
    queryFn: () => fetchList("/data-quality/templates", "templates").catch(() => []),
  });

  const templates = (apiTemplates ?? []).length > 0 ? apiTemplates! : BUILTIN_TEMPLATES;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Rule Templates</h1>
        <p className="text-muted-foreground">Pre-built rule templates to quickly create data quality policies</p>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {templates.map((t: any) => (
          <Card key={t.id ?? t.name} className="flex flex-col">
            <CardHeader>
              <CardTitle className="text-base">{t.name}</CardTitle>
              <Badge variant="outline" className="w-fit">{t.rule_type}</Badge>
            </CardHeader>
            <CardContent className="flex-1">
              <p className="text-sm text-muted-foreground">{t.description}</p>
              {t.default_config && (
                <pre className="mt-3 rounded-md bg-muted p-2 text-xs overflow-auto">
                  {JSON.stringify(t.default_config, null, 2)}
                </pre>
              )}
            </CardContent>
            <CardFooter>
              <Button
                variant="outline"
                className="w-full"
                onClick={() => navigate(`/data-quality/policies/new?template=${t.id ?? t.name}&rule_type=${t.rule_type}`)}
              >
                Use Template
              </Button>
            </CardFooter>
          </Card>
        ))}
      </div>
    </div>
  );
}
