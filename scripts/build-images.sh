#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/build-images.sh
# Build all Docker images for Fusion CDC Engine (local tags)
#
# Usage:
#   ./scripts/build-images.sh             # build all
#   ./scripts/build-images.sh control     # build only control-plane
#   ./scripts/build-images.sh worker      # build only cdc-worker
#   ./scripts/build-images.sh frontend    # build only frontend
#   ./scripts/build-images.sh transform   # build only transform-worker
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TAG="${IMAGE_TAG:-local}"

build_control() {
  echo "==> Building control-plane:${TAG}"
  docker build \
    --file docker/Dockerfile.control-plane \
    --tag "fusion/control-plane:${TAG}" \
    --target runtime \
    .
  echo "    ✓ fusion/control-plane:${TAG}"
}

build_worker() {
  echo "==> Building cdc-worker:${TAG}"
  docker build \
    --file docker/Dockerfile.cdc-worker \
    --tag "fusion/cdc-worker:${TAG}" \
    --target runtime \
    .
  echo "    ✓ fusion/cdc-worker:${TAG}"
}

build_frontend() {
  echo "==> Building frontend:${TAG}"
  docker build \
    --file docker/Dockerfile.frontend \
    --tag "fusion/frontend:${TAG}" \
    --target production \
    .
  echo "    ✓ fusion/frontend:${TAG}"
}

build_transform() {
  echo "==> Building transform-worker:${TAG}"
  docker build \
    --file docker/Dockerfile.transform-worker \
    --tag "fusion/transform-worker:${TAG}" \
    --target runtime \
    .
  echo "    ✓ fusion/transform-worker:${TAG}"
}

case "${1:-all}" in
  control)   build_control ;;
  worker)    build_worker ;;
  frontend)  build_frontend ;;
  transform) build_transform ;;
  all)
    build_control
    build_worker
    build_frontend
    build_transform
    echo ""
    echo "All images built:"
    docker images | grep "fusion/"
    ;;
  *)
    echo "Unknown target: $1"
    echo "Usage: $0 [all|control|worker|frontend|transform]"
    exit 1
    ;;
esac
