# Fusion CDC Engine (private)

Proprietary CDC execution engine for DCraft Fusion.

**Public consumers** install via Helm/OCI charts and pull images from:

- `ghcr.io/dcraft-labs/fusion-cdc-control-plane`
- `ghcr.io/dcraft-labs/fusion-cdc-worker`
- `ghcr.io/dcraft-labs/fusion-cdc-frontend`
- `ghcr.io/dcraft-labs/fusion-cdc-spark-consumer`
- `ghcr.io/dcraft-labs/fusion-cdc-transform-worker`

Source in this repository is **not** Apache-2.0. Do not publish the source tree.

## Relationship to DCraft Fusion

The public monorepo `DCraft-Labs/dcraft-fusion` contains the Apache-2.0 control plane.
This private repo builds and publishes the CDC images consumed by the public `fusion-cdc` Helm chart.
