/**
 * API error formatter — handles FastAPI 422 `detail` arrays and string details
 * without crashing the page (fixes Register blank-page bug).
 */

export function formatApiDetail(err: any): string {
  if (!err) return "Unknown error";
  const detail = err?.response?.data?.detail ?? err?.detail ?? err?.message ?? err;
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    return detail
      .map((d: any) => {
        if (typeof d === "string") return d;
        const loc = Array.isArray(d?.loc) ? d.loc.join(".") : d?.loc ?? "";
        const msg = d?.msg ?? d?.message ?? "";
        return loc ? `${loc}: ${msg}` : msg;
      })
      .filter(Boolean)
      .join("; ");
  }
  if (detail && typeof detail === "object") {
    return JSON.stringify(detail);
  }
  return String(detail);
}
