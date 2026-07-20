import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useMutation } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Plus, X } from "lucide-react";

interface Param {
  name: string;
  type: string;
  description: string;
}

interface UDFExample {
  label: string;
  category: string;
  return_type: string;
  language: string;
  description: string;
  params: Param[];
  code: string;
}

const UDF_EXAMPLES: UDFExample[] = [
  {
    label: "Mask PII (email)",
    category: "security",
    return_type: "StringType",
    language: "python",
    description: "Redact an email address to show only domain",
    params: [{ name: "email", type: "string", description: "Raw email address" }],
    code: `def mask_email(email: str) -> str:
    """Return only the domain part of an email, e.g. ***@example.com"""
    if not email or "@" not in email:
        return "***"
    _, domain = email.rsplit("@", 1)
    return f"***@{domain}"
`,
  },
  {
    label: "Phone Last-4",
    category: "security",
    return_type: "StringType",
    language: "python",
    description: "Keep only last 4 digits of a phone number",
    params: [{ name: "phone", type: "string", description: "Raw phone number" }],
    code: `def phone_last4(phone: str) -> str:
    """Return ****XXXX format keeping only last 4 digits."""
    digits = "".join(c for c in (phone or "") if c.isdigit())
    return f"****{digits[-4:]}" if len(digits) >= 4 else "****"
`,
  },
  {
    label: "Normalize Amount",
    category: "transform",
    return_type: "FloatType",
    language: "python",
    description: "Parse and round a currency string to 2 decimal places",
    params: [{ name: "amount_str", type: "string", description: "Raw amount e.g. '$1,234.50'" }],
    code: `def normalize_amount(amount_str: str) -> float:
    """Strip currency symbols/commas and return a float rounded to 2dp."""
    import re
    cleaned = re.sub(r"[^0-9.\\-]", "", str(amount_str or "0"))
    try:
        return round(float(cleaned), 2)
    except ValueError:
        return 0.0
`,
  },
  {
    label: "Enrich Country ISO",
    category: "enrichment",
    return_type: "StringType",
    language: "python",
    description: "Map a 2-letter ISO country code to its full name",
    params: [{ name: "iso2", type: "string", description: "2-letter ISO code, e.g. 'IN'" }],
    code: `_COUNTRY_MAP = {
    "IN": "India", "US": "United States", "GB": "United Kingdom",
    "AE": "United Arab Emirates", "SG": "Singapore", "AU": "Australia",
    "OM": "Oman", "BH": "Bahrain", "KW": "Kuwait", "QA": "Qatar",
}

def enrich_country_name(iso2: str) -> str:
    """Return the full country name for a 2-letter ISO code."""
    return _COUNTRY_MAP.get((iso2 or "").upper(), iso2 or "Unknown")
`,
  },
  {
    label: "Validate IBAN",
    category: "validation",
    return_type: "BooleanType",
    language: "python",
    description: "Check that a string is a structurally valid IBAN",
    params: [{ name: "iban", type: "string", description: "IBAN string" }],
    code: `def validate_iban(iban: str) -> bool:
    """Return True if the string passes basic IBAN structure checks."""
    import re
    iban = (iban or "").replace(" ", "").upper()
    if not re.fullmatch(r"[A-Z]{2}[0-9]{2}[A-Z0-9]{11,30}", iban):
        return False
    rearranged = iban[4:] + iban[:4]
    numeric = "".join(str(ord(c) - 55) if c.isalpha() else c for c in rearranged)
    return int(numeric) % 97 == 1
`,
  },
];

export function CreateUDFPage() {
  const navigate = useNavigate();
  const [form, setForm] = useState({
    name: "",
    description: "",
    language: "python",
    category: "transform",
    return_type: "StringType",
    code: "",
  });
  const [params, setParams] = useState<Param[]>([]);

  const createMutation = useMutation({
    mutationFn: () => api.post("/udfs", { ...form, parameters: params }),
    onSuccess: (res) => navigate(`/udfs/${res.data.id}`),
  });

  const addParam = () => setParams([...params, { name: "", type: "string", description: "" }]);
  const removeParam = (i: number) => setParams(params.filter((_, idx) => idx !== i));
  const updateParam = (i: number, field: keyof Param, value: string) =>
    setParams(params.map((p, idx) => (idx === i ? { ...p, [field]: value } : p)));

  const loadExample = (ex: UDFExample) => {
    setForm({
      name: ex.label.toLowerCase().replace(/\s+/g, "_"),
      description: ex.description,
      language: ex.language,
      category: ex.category,
      return_type: ex.return_type,
      code: ex.code,
    });
    setParams(ex.params);
  };

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Register UDF</h1>

      {/* Example templates */}
      <Card>
        <CardHeader><CardTitle>Example Templates</CardTitle></CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground mb-3">
            Click an example to pre-fill the form. UDFs run inside the DuckDB transform worker — write plain Python and return the correct type.
          </p>
          <div className="flex flex-wrap gap-2">
            {UDF_EXAMPLES.map((ex) => (
              <Button
                key={ex.label}
                type="button"
                variant="outline"
                size="sm"
                onClick={() => loadExample(ex)}
              >
                {ex.label}
              </Button>
            ))}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader><CardTitle>UDF Details</CardTitle></CardHeader>
        <CardContent>
          <form onSubmit={(e) => { e.preventDefault(); createMutation.mutate(); }} className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Name *</label>
                <Input
                  value={form.name}
                  onChange={(e) => setForm({ ...form, name: e.target.value })}
                  placeholder="mask_pii"
                  required
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Language</label>
                <Select value={form.language} onValueChange={(v) => setForm({ ...form, language: v })}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="python">Python</SelectItem>
                  </SelectContent>
                </Select>
                <p className="text-xs text-muted-foreground">Only Python UDFs are supported in the DuckDB transform worker.</p>
              </div>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Description</label>
              <Input value={form.description} onChange={(e) => setForm({ ...form, description: e.target.value })} />
            </div>
            <div className="grid grid-cols-2 gap-4">
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
                className="w-full min-h-[200px] rounded-md border bg-muted p-3 font-mono text-sm focus:outline-none focus:ring-1 focus:ring-ring"
                value={form.code}
                onChange={(e) => setForm({ ...form, code: e.target.value })}
                placeholder="def my_udf(value: str) -> str:&#10;    return value"
                required
              />
              <p className="text-xs text-muted-foreground">
                Write a top-level Python function. The function name must match the UDF name above.
                Available packages: <code>re</code>, <code>json</code>, <code>datetime</code>, <code>hashlib</code>, <code>uuid</code>.
              </p>
            </div>
            <div className="flex gap-2">
              <Button type="button" variant="outline" onClick={() => navigate("/udfs")}>Cancel</Button>
              <Button type="submit" disabled={createMutation.isPending}>
                {createMutation.isPending ? "Creating..." : "Create UDF"}
              </Button>
            </div>
          </form>
        </CardContent>
      </Card>
    </div>
  );
}
