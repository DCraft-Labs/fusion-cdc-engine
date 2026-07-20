#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/k8s-dev-up.sh
# Start Fusion CDC Engine on Docker Desktop Kubernetes
#
# Pre-requisites (ONE-TIME setup, run manually if missing):
#   1. Enable Kubernetes in Docker Desktop Preferences > Kubernetes > Enable
#   2. kubectl config use-context docker-desktop
#
# This script does:
#   1. Stops existing Docker Compose stack (releases ports)
#   2. Builds local Docker images
#   3. Installs KEDA (if not already installed)
#   4. Applies kubernetes/overlays/local/ via kubectl
#   5. Waits for all pods to be ready
#   6. Prints service endpoints
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# ── Config ─────────────────────────────────────────────────────────────────
K8S_NAMESPACE="fusion"
KEDA_NAMESPACE="keda"
KEDA_VERSION="2.13.1"
OVERLAY="kubernetes/overlays/local"

# ── Helpers ────────────────────────────────────────────────────────────────
info()  { echo "[INFO]  $*"; }
warn()  { echo "[WARN]  $*" >&2; }
fatal() { echo "[ERROR] $*" >&2; exit 1; }

step() {
  echo ""
  echo "══════════════════════════════════════════════════════════"
  echo "  $1"
  echo "══════════════════════════════════════════════════════════"
}

# ── 1. Verify kubectl context ───────────────────────────────────────────────
step "1/6  Verifying kubectl context"
CURRENT_CONTEXT=$(kubectl config current-context 2>/dev/null || echo "none")
if [[ "$CURRENT_CONTEXT" == "none" ]]; then
  echo ""
  echo "  ┌──────────────────────────────────────────────────────────────┐"
  echo "  │  Kubernetes is NOT enabled in Docker Desktop.                │"
  echo "  │                                                              │"
  echo "  │  To enable:                                                  │"
  echo "  │  1. Open Docker Desktop                                      │"
  echo "  │  2. Settings → Kubernetes → ✓ Enable Kubernetes             │"
  echo "  │  3. Click \"Apply & Restart\" and wait ~2 minutes              │"
  echo "  │  4. Re-run this script                                       │"
  echo "  └──────────────────────────────────────────────────────────────┘"
  exit 1
fi

if [[ "$CURRENT_CONTEXT" != "docker-desktop" ]]; then
  warn "Current kubectl context is: $CURRENT_CONTEXT"
  warn "Expected: docker-desktop"
  warn "Run: kubectl config use-context docker-desktop"
  read -r -p "Continue anyway? [y/N] " answer
  [[ "$answer" =~ ^[Yy]$ ]] || fatal "Aborted."
fi
info "Using context: $CURRENT_CONTEXT"

# ── 2. Stop Docker Compose stack ───────────────────────────────────────────
step "2/6  Stopping Docker Compose stack"
COMPOSE_FILES=()
for f in docker/docker-compose.yml docker/docker-compose.dev.yml; do
  [[ -f "$f" ]] && COMPOSE_FILES+=("-f" "$f")
done
if [[ ${#COMPOSE_FILES[@]} -gt 0 ]]; then
  info "docker compose ${COMPOSE_FILES[*]} down --remove-orphans"
  docker compose "${COMPOSE_FILES[@]}" down --remove-orphans 2>/dev/null || true
  info "Docker Compose stopped"
else
  info "No docker-compose files found, skipping"
fi

# ── 3. Build images ─────────────────────────────────────────────────────────
step "3/6  Building local images"
bash scripts/build-images.sh all

# ── 4. Install KEDA ─────────────────────────────────────────────────────────
step "4/6  Installing KEDA"
if kubectl get deployment keda-operator -n "$KEDA_NAMESPACE" &>/dev/null; then
  info "KEDA already installed in namespace $KEDA_NAMESPACE"
else
  info "Installing KEDA ${KEDA_VERSION}..."
  helm repo add kedacore https://kedacore.github.io/charts 2>/dev/null || true
  helm repo update
  helm install keda kedacore/keda \
    --namespace "$KEDA_NAMESPACE" \
    --create-namespace \
    --version "$KEDA_VERSION" \
    --set watchNamespace="$K8S_NAMESPACE"
  info "Waiting for KEDA to be ready..."
  kubectl rollout status deployment/keda-operator -n "$KEDA_NAMESPACE" --timeout=120s
fi

# ── 5. Apply manifests ───────────────────────────────────────────────────────
step "5/6  Applying manifests: $OVERLAY"
# Namespaces must be created first (they cannot live in the same kustomization
# that sets namespace: fusion — kustomize would attempt to rename them)
kubectl apply -f kubernetes/base/namespaces.yaml
  kubectl apply -k kubernetes/overlays/local/
info "Manifests applied"

# ── 6. Wait for pods ────────────────────────────────────────────────────────
step "6/6  Waiting for pods to be ready"
info "Waiting up to 5 minutes for all pods..."
kubectl wait \
  --for=condition=ready pod \
  --all \
  --namespace="$K8S_NAMESPACE" \
  --timeout=300s \
  2>/dev/null || {
    warn "Some pods may not be ready — check with: kubectl get pods -n $K8S_NAMESPACE"
  }

# ── Summary ─────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Fusion CDC Engine is running on Kubernetes           ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Service Endpoints (Docker Desktop NodePort):                ║"
echo "║                                                              ║"
echo "║  Frontend:       http://localhost:30080                      ║"
echo "║  Control Plane:  http://localhost:30800                      ║"
echo "║  API Docs:       http://localhost:30800/docs                 ║"
echo "║                                                              ║"
echo "║  Source DBs (dev only):                                      ║"
echo "║  pg-source:      localhost:30534                             ║"
echo "║  postgres-dest:  localhost:30533                             ║"
echo "║  mysql-source:   localhost:30307                             ║"
echo "║  mongo-source:   localhost:30018                             ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  kubectl get pods -n fusion                              ║"
echo "║  kubectl logs -f -n fusion -l app.kubernetes.io/name=... ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
kubectl get pods -n "$K8S_NAMESPACE"
