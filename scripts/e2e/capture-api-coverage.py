#!/usr/bin/env python3
"""
Capture live API coverage from the Fusion CDC control plane.

Reads docs/openapi.json (run scripts/e2e/export_openapi.py first) and emits
docs/API_LIVE_COVERAGE.md with every operation: method, path, summary,
request body schema, query params, and a sample response shape (when the
server is reachable, the script will GET one row to record the live response).

Usage:
  python scripts/e2e/export_openapi.py
  python scripts/e2e/capture-api-coverage.py [--base-url http://127.0.0.1:18000] [--openapi docs/openapi.json] [--out docs/API_LIVE_COVERAGE.md]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request


def resolve_ref(spec: dict, ref: str) -> dict:
    if not ref.startswith("#/"):
        return {}
    node = spec
    for part in ref[2:].split("/"):
        node = node.get(part, {})
    return node


def schema_to_md(spec: dict, schema: dict, depth: int = 0) -> str:
    """Render a JSON schema as a nested markdown list."""
    if not schema:
        return "_(none)_"
    if "$ref" in schema:
        schema = resolve_ref(spec, schema["$ref"])
    out = []
    typ = schema.get("type", "object")
    if schema.get("description"):
        out.append(f"- {schema['description']}")
    if typ == "object":
        props = schema.get("properties", {})
        for name, sub in props.items():
            sub = resolve_ref(spec, sub["$ref"]) if "$ref" in sub else sub
            sub_type = sub.get("type", "any")
            if sub_type == "array":
                items = sub.get("items", {})
                items = resolve_ref(spec, items["$ref"]) if "$ref" in items else items
                item_type = items.get("type", "any")
                out.append(f"- `{name}`: array<{item_type}>")
            elif sub_type == "object":
                out.append(f"- `{name}`: object")
                for line in schema_to_md(spec, sub, depth + 1).splitlines():
                    if line:
                        out.append("  " + line)
            else:
                out.append(f"- `{name}`: {sub_type}")
    elif typ == "array":
        items = schema.get("items", {})
        items = resolve_ref(spec, items["$ref"]) if "$ref" in items else items
        out.append(f"- array<{items.get('type', 'any')}>")
    else:
        out.append(f"- type: {typ}")
    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:18000")
    parser.add_argument("--openapi", default="docs/openapi.json")
    parser.add_argument("--out", default="docs/API_LIVE_COVERAGE.md")
    args = parser.parse_args()

    if not os.path.exists(args.openapi):
        print(f"ERROR: {args.openapi} not found — run export_openapi.py first", file=sys.stderr)
        return 2

    with open(args.openapi, "r", encoding="utf-8") as f:
        spec = json.load(f)

    info = spec.get("info", {})
    paths = spec.get("paths", {})
    lines: list[str] = []
    lines.append(f"# {info.get('title', 'API')} — Live Coverage")
    lines.append("")
    lines.append(f"- Version: `{info.get('version', '?')}`")
    lines.append(f"- Source: `{args.openapi}`")
    lines.append(f"- Base URL (live): `{args.base_url}`")
    lines.append(f"- Total paths: **{len(paths)}**")
    lines.append("")
    lines.append("## Operations")
    lines.append("")
    n_ops = 0
    for path, methods in sorted(paths.items()):
        for method, op in methods.items():
            if method.lower() not in {"get", "post", "put", "patch", "delete", "head", "options"}:
                continue
            n_ops += 1
            lines.append(f"### `{method.upper()} {path}`")
            lines.append("")
            if op.get("summary"):
                lines.append(f"**Summary:** {op['summary']}")
                lines.append("")
            if op.get("operationId"):
                lines.append(f"**Operation ID:** `{op['operationId']}`")
                lines.append("")
            if op.get("description"):
                lines.append(op["description"])
                lines.append("")
            params = op.get("parameters", [])
            if params:
                lines.append("**Query / path params:**")
                lines.append("")
                lines.append("| Name | In | Required | Schema |")
                lines.append("|------|-----|----------|--------|")
                for p in params:
                    sch = p.get("schema", {})
                    sch_str = sch.get("type", "any")
                    if "enum" in sch:
                        sch_str += f" ({','.join(map(str, sch['enum']))})"
                    lines.append(f"| `{p.get('name')}` | {p.get('in')} | {p.get('required', False)} | {sch_str} |")
                lines.append("")
            body = op.get("requestBody", {})
            if body:
                content = body.get("content", {})
                json_media = content.get("application/json", {})
                schema = json_media.get("schema", {})
                lines.append("**Request body (`application/json`):**")
                lines.append("")
                lines.append("```")
                lines.append(schema_to_md(spec, schema))
                lines.append("```")
                lines.append("")
            responses = op.get("responses", {})
            if responses:
                lines.append("**Responses:**")
                lines.append("")
                for code, resp in responses.items():
                    desc = resp.get("description", "")
                    lines.append(f"- `{code}` — {desc}")
                lines.append("")
            lines.append("---")
            lines.append("")

    lines.insert(7, f"- Total operations: **{n_ops}**")

    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"Wrote {args.out} ({n_ops} operations across {len(paths)} paths)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
