# API Live Coverage

This file is **generated** by `scripts/e2e/capture-api-coverage.py` from the live
OpenAPI spec exported by `scripts/e2e/export_openapi.py`.

## Regenerate

```bash
# 1. Port-forward the CDC control plane
kubectl -n dcraft-local port-forward svc/fusion-cdc-control-plane 18000:8000

# 2. Export the live OpenAPI spec
python scripts/e2e/export_openapi.py --base-url http://127.0.0.1:18000 --out docs/openapi.json

# 3. Generate this markdown
python scripts/e2e/capture-api-coverage.py --base-url http://127.0.0.1:18000 --openapi docs/openapi.json --out docs/API_LIVE_COVERAGE.md
```

The generated file lists every operation with:
- HTTP method + path
- Summary + operationId
- Query / path parameters (name, in, required, schema)
- Request body schema (rendered as a nested list)
- Response codes + descriptions

> The committed version of this file is a placeholder. Run the steps above to
> regenerate after every API change before release.
