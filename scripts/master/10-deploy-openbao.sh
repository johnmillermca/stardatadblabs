#!/usr/bin/env bash
# =============================================================================
# 10-deploy-openbao.sh
# Run on MASTER. Installs OpenBao (open-source Vault fork) via Helm.
# =============================================================================
set -euo pipefail
export PATH="/usr/local/bin:${PATH}"

OPENBAO_NAMESPACE="openbao"
MASTER_IP="192.168.1.50"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# ── Add OpenBao Helm repo ─────────────────────────────────────────────────────
helm repo add openbao https://openbao.github.io/openbao-helm || true
helm repo update

# ── Namespace ─────────────────────────────────────────────────────────────────
kubectl create namespace "${OPENBAO_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

# ── Install ───────────────────────────────────────────────────────────────────
log "Deploying OpenBao..."
helm upgrade --install openbao openbao/openbao \
  --namespace "${OPENBAO_NAMESPACE}" \
  --values "$(dirname "$0")/../../helm/openbao/values.yaml" \
  --wait --timeout 10m

# ── Wait for pod ──────────────────────────────────────────────────────────────
log "Waiting for OpenBao pod..."
kubectl wait pod -l "app.kubernetes.io/name=openbao" \
  -n "${OPENBAO_NAMESPACE}" \
  --for=condition=Ready --timeout=300s

# ── Initialize OpenBao ────────────────────────────────────────────────────────
log "Initializing OpenBao..."
sleep 10

# Initialize with 5 key shares, threshold 3
INIT_OUTPUT=$(kubectl exec -n "${OPENBAO_NAMESPACE}" \
  "$(kubectl get pod -n "${OPENBAO_NAMESPACE}" -l app.kubernetes.io/name=openbao -o jsonpath='{.items[0].metadata.name}')" \
  -- bao operator init -key-shares=5 -key-threshold=3 -format=json 2>/dev/null || echo "{}")

if echo "${INIT_OUTPUT}" | grep -q "unseal_keys_b64"; then
  echo "${INIT_OUTPUT}" > /root/openbao-init-keys.json
  chmod 600 /root/openbao-init-keys.json
  log "OpenBao initialized. Keys saved to /root/openbao-init-keys.json"
  log "IMPORTANT: Back up this file and delete it from the server!"

  # Auto-unseal for dev/lab (NOT for production)
  ROOT_TOKEN=$(echo "${INIT_OUTPUT}" | python3 -c "import sys,json; print(json.load(sys.stdin)['root_token'])")
  UNSEAL_KEYS=$(echo "${INIT_OUTPUT}" | python3 -c "
import sys,json
d=json.load(sys.stdin)
for k in d['unseal_keys_b64'][:3]:
    print(k)")

  BAO_POD=$(kubectl get pod -n "${OPENBAO_NAMESPACE}" -l app.kubernetes.io/name=openbao \
    -o jsonpath='{.items[0].metadata.name}')

  while IFS= read -r KEY; do
    kubectl exec -n "${OPENBAO_NAMESPACE}" "${BAO_POD}" -- bao operator unseal "${KEY}" || true
  done <<< "${UNSEAL_KEYS}"

  log "OpenBao unsealed (using 3 of 5 keys)."

  # Enable KV v2 secrets engine
  kubectl exec -n "${OPENBAO_NAMESPACE}" "${BAO_POD}" -- \
    sh -c "BAO_TOKEN=${ROOT_TOKEN} bao secrets enable -version=2 -path=secret kv" || true

  # Enable Kubernetes auth
  kubectl exec -n "${OPENBAO_NAMESPACE}" "${BAO_POD}" -- \
    sh -c "BAO_TOKEN=${ROOT_TOKEN} bao auth enable kubernetes" || true

  # Configure Kubernetes auth
  kubectl exec -n "${OPENBAO_NAMESPACE}" "${BAO_POD}" -- \
    sh -c "BAO_TOKEN=${ROOT_TOKEN} bao write auth/kubernetes/config \
      kubernetes_host=https://\$KUBERNETES_SERVICE_HOST:\$KUBERNETES_SERVICE_PORT" || true

  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  OpenBao UI:   http://${MASTER_IP}:30820"
  echo "  Root Token:   ${ROOT_TOKEN}"
  echo "  Keys file:    /root/openbao-init-keys.json"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
else
  log "OpenBao appears to already be initialized or init failed."
  log "Check: kubectl exec -n ${OPENBAO_NAMESPACE} <pod> -- bao status"
fi

log "OpenBao deployment complete."
