#!/usr/bin/env python3
"""
Export the live OpenAPI spec from the Fusion CDC control plane to
docs/openapi.json for the API coverage generator and for offline reference.

Usage:
  python scripts/e2e/export_openapi.py [--base-url http://127.0.0.1:18000] [--out docs/openapi.json]

The control plane must be running (port-forward or compose). The script:
  1. GETs {base_url}/api/openapi.json
  2. Validates it parses as JSON
  3. Writes it to the output path (pretty-printed)
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:18000")
    parser.add_argument("--out", default="docs/openapi.json")
    args = parser.parse_args()

    url = args.base_url.rstrip("/") + "/api/openapi.json"
    print(f"Fetching OpenAPI from {url} ...")
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            raw = resp.read()
    except Exception as e:
        print(f"ERROR: could not fetch {url}: {e}", file=sys.stderr)
        return 2

    try:
        spec = json.loads(raw)
    except Exception as e:
        print(f"ERROR: response is not valid JSON: {e}", file=sys.stderr)
        return 3

    n_paths = len(spec.get("paths", {}))
    n_ops = sum(
        len([m for m in methods if m.lower() in {"get", "post", "put", "patch", "delete", "head", "options"}])
        for _, methods in spec.get("paths", {}).items()
    )
    print(f"OK: {n_paths} paths, {n_ops} operations, title={spec.get('info', {}).get('title')!r}")

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(spec, f, indent=2, sort_keys=False)
        f.write("\n")
    print(f"Wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
