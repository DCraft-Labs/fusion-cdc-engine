/**
 * API error formatter — handles FastAPI 422 `detail` arrays and string details
 * without crashing the page (fixes Register blank-page bug).
 */

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

function getDetail(err: unknown): unknown {
  if (isObject(err)) {
    const resp = err.response;
    if (isObject(resp) && isObject(resp.data) && "detail" in resp.data) {
      return resp.data.detail;
    }
    if ("detail" in err) return err.detail;
    if ("message" in err) return err.message;
  }
  return err;
}

export function formatApiDetail(err: unknown): string {
  if (!err) return "Unknown error";
  const detail = getDetail(err);
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    return detail
      .map((d) => {
        if (typeof d === "string") return d;
        if (isObject(d)) {
          const loc = Array.isArray(d.loc)
            ? d.loc.join(".")
            : typeof d.loc === "string"
              ? d.loc
              : "";
          const msg = typeof d.msg === "string" ? d.msg : typeof d.message === "string" ? d.message : "";
          return loc ? `${loc}: ${msg}` : msg;
        }
        return "";
      })
      .filter(Boolean)
      .join("; ");
  }
  if (isObject(detail)) return JSON.stringify(detail);
  return String(detail);
}
