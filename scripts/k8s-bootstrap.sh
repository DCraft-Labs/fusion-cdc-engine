#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/k8s-bootstrap.sh
# One-shot bootstrap: install KEDA + apply all manifests
# Run this ONCE after enabling Kubernetes in Docker Desktop.
#
# Usage:
#   ./scripts/k8s-bootstrap.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

K8S_NS="fusion"
KEDA_NS="keda"
KEDA_VERSION="2.13.1"

info()  { echo "[INFO]  $*"; }
step()  { echo ""; echo "▶  $1"; }
fatal() { echo "[ERROR] $*" >&2; exit 1; }

# ── Guard: K8s must be running ─────────────────────────────────────────────
step "Checking Kubernetes..."
if ! kubectl cluster-info &>/dev/null; then
  echo ""
  echo "  Kubernetes is not running or not configured."
  echo "  Enable it in Docker Desktop → Settings → Kubernetes → Enable → Apply & Restart"
  exit 1
fi
info "Kubernetes OK: $(kubectl config current-context)"

# ── Namespaces ─────────────────────────────────────────────────────────────
step "Creating namespaces..."
kubectl apply -f kubernetes/base/namespaces.yaml
info "Namespaces ready"

# ── KEDA ───────────────────────────────────────────────────────────────────
step "Installing KEDA ${KEDA_VERSION}..."
if kubectl get deployment keda-operator -n "$KEDA_NS" &>/dev/null; then
  info "KEDA already installed"
else
  helm repo add kedacore https://kedacore.github.io/charts 2>/dev/null || true
  helm repo update kedacore
  helm install keda kedacore/keda \
    --namespace "$KEDA_NS" \
    --create-namespace \
    --version "$KEDA_VERSION" \
    --set watchNamespace="$K8S_NS" \
    --set prometheus.operator.enabled=false
  info "Waiting for KEDA operator..."
  kubectl rollout status deployment/keda-operator -n "$KEDA_NS" --timeout=120s
fi

# ── Apply manifests ─────────────────────────────────────────────────────────
step "Applying kubernetes/overlays/local/..."
kubectl apply -k kubernetes/overlays/local/
info "Manifests applied"

# ── Wait ────────────────────────────────────────────────────────────────────
step "Waiting for core infrastructure (postgres-meta, redis, kafka)..."
for sset in fusion-postgres-meta fusion-redis fusion-kafka; do
  kubectl rollout status statefulset/"$sset" -n "$K8S_NS" --timeout=300s || \
    echo "  [WARN] $sset not yet ready — check: kubectl logs -n $K8S_NS statefulset/$sset"
done

step "Waiting for control-plane..."
kubectl rollout status deployment/fusion-control-plane -n "$K8S_NS" --timeout=120s || \
  echo "  [WARN] control-plane not ready — check: kubectl logs -n $K8S_NS -l app.kubernetes.io/name=control-plane"

step "Waiting for CDC workers..."
for dep in mysql-cdc-worker postgres-cdc-worker mongo-cdc-worker; do
  kubectl rollout status deployment/"$dep" -n "$K8S_NS" --timeout=120s || \
    echo "  [WARN] $dep not ready"
done

# ── Summary ─────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  Fusion CDC Engine — Running on Kubernetes               ║"
echo "╠══════════════════════════════════════════════════════════════════╣"
echo "║                                                                  ║"
echo "║  Frontend:       http://localhost:30080                          ║"
echo "║  Control Plane:  http://localhost:30800                          ║"
echo "║  API Docs:       http://localhost:30800/docs                     ║"
echo "║  Kafka:          localhost:30092  (external broker port)         ║"
echo "║                                                                  ║"
echo "║  Commands:                                                       ║"
echo "║  kubectl get pods -n fusion                                  ║"
echo "║  kubectl get scaledobjects -n fusion                         ║"
echo "║  kubectl logs -f -n fusion deploy/fusion-control-plane   ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""
kubectl get pods -n "$K8S_NS" 2>/dev/null || true
