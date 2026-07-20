#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/k8s-dev-down.sh
# Tear down Fusion CDC Engine from Docker Desktop Kubernetes
#
# Usage:
#   ./scripts/k8s-dev-down.sh              # delete all resources, keep PVCs
#   ./scripts/k8s-dev-down.sh --purge      # delete resources AND all PVCs (data loss!)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

K8S_NAMESPACE="fusion"
OVERLAY="kubernetes/overlays/local"
PURGE=false

for arg in "$@"; do
  [[ "$arg" == "--purge" ]] && PURGE=true
done

echo "==> Deleting Kubernetes resources from overlay: $OVERLAY"
kubectl delete -k "$OVERLAY" --ignore-not-found=true || true

if $PURGE; then
  echo ""
  read -r -p "WARNING: This will DELETE all persistent volume claims (data loss). Continue? [y/N] " answer
  [[ "$answer" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 1; }
  echo "==> Deleting PVCs in namespace $K8S_NAMESPACE"
  kubectl delete pvc --all -n "$K8S_NAMESPACE" --ignore-not-found=true
  echo "==> Deleting namespace $K8S_NAMESPACE"
  kubectl delete namespace "$K8S_NAMESPACE" --ignore-not-found=true
  echo "Purge complete."
else
  echo ""
  echo "Resources deleted. PVCs preserved (re-apply will reuse existing data)."
  echo "To also delete data: $0 --purge"
fi
