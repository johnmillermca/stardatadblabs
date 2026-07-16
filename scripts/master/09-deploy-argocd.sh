#!/usr/bin/env bash
# =============================================================================
# 09-deploy-argocd.sh
# Run on MASTER. Installs ArgoCD via Helm with a custom values file.
# =============================================================================
set -euo pipefail
export PATH="/usr/local/bin:${PATH}"

ARGOCD_NAMESPACE="argocd"
ARGOCD_CHART_VERSION="6.11.1"   # argo-helm chart version (matches ArgoCD 2.11.x)
MASTER_IP="192.168.1.50"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# ── Namespace ─────────────────────────────────────────────────────────────────
kubectl create namespace "${ARGOCD_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

# ── Install via Helm ──────────────────────────────────────────────────────────
log "Deploying ArgoCD ${ARGOCD_CHART_VERSION}..."
helm upgrade --install argocd argo/argo-cd \
  --namespace "${ARGOCD_NAMESPACE}" \
  --version "${ARGOCD_CHART_VERSION}" \
  --values "$(dirname "$0")/../../helm/argocd/values.yaml" \
  --wait --timeout 10m

# ── Wait for all pods ─────────────────────────────────────────────────────────
log "Waiting for ArgoCD pods..."
kubectl rollout status deployment/argocd-server        -n "${ARGOCD_NAMESPACE}" --timeout=300s
kubectl rollout status deployment/argocd-repo-server   -n "${ARGOCD_NAMESPACE}" --timeout=300s
kubectl rollout status deployment/argocd-applicationset-controller -n "${ARGOCD_NAMESPACE}" --timeout=300s

# ── Get initial admin password ────────────────────────────────────────────────
log "Fetching initial ArgoCD admin password..."
ARGOCD_PWD=$(kubectl -n "${ARGOCD_NAMESPACE}" get secret argocd-initial-admin-secret \
  -o jsonpath='{.data.password}' | base64 -d)

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ArgoCD UI:       https://${MASTER_IP}:30443"
echo "  Username:        admin"
echo "  Initial password: ${ARGOCD_PWD}"
echo "  Change with:     argocd account update-password"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

log "ArgoCD deployment complete."
