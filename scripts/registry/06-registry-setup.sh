#!/usr/bin/env bash
# =============================================================================
# 06-registry-setup.sh
# Run on MASTER node.
# Deploys a private Docker registry as a Kubernetes Deployment + Service,
# generates a self-signed TLS certificate, and configures containerd on all
# nodes to trust it.
#
# Registry endpoint: registry.local:30500  (or 192.168.1.50:30500)
# =============================================================================
set -euo pipefail

REGISTRY_NAMESPACE="registry"
REGISTRY_HOST="192.168.1.50"
REGISTRY_PORT="30500"
REGISTRY_FQDN="registry.local"
CERT_DIR="/etc/k8s-registry-certs"
MASTER_CERT_DIR="${CERT_DIR}"

SCRIPTS_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DEFAULT_INVENTORY="${SCRIPTS_DIR}/workers.conf"
INVENTORY_FILE="${DEFAULT_INVENTORY}"
SSH_USER="${SUDO_USER:-root}"
SSH_KEY_DIR="$(eval echo "~${SSH_USER}")/.ssh"
SSH_KEY="${SSH_KEY_DIR}/id_ed25519"
[[ -f "${SSH_KEY}" ]] || SSH_KEY="${SSH_KEY_DIR}/id_rsa"
SSH_OPTS="-o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=10 -i ${SSH_KEY} -o UserKnownHostsFile=${SSH_KEY_DIR}/known_hosts"
SSH_AS() { "$@"; }

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# ── 1. Generate self-signed TLS cert ─────────────────────────────────────────
log "Generating self-signed TLS certificate..."
mkdir -p "${CERT_DIR}"

openssl req -x509 -newkey rsa:4096 -nodes -days 3650 \
  -keyout "${CERT_DIR}/registry.key" \
  -out    "${CERT_DIR}/registry.crt" \
  -subj   "/CN=${REGISTRY_FQDN}/O=K8sRegistry" \
  -addext "subjectAltName=DNS:${REGISTRY_FQDN},IP:${REGISTRY_HOST}"

log "Certificate generated at ${CERT_DIR}/registry.{crt,key}"

# ── 2. Create namespace + TLS secret ─────────────────────────────────────────
log "Creating registry namespace and TLS secret..."
kubectl create namespace "${REGISTRY_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret tls registry-tls \
  --cert="${CERT_DIR}/registry.crt" \
  --key="${CERT_DIR}/registry.key" \
  -n "${REGISTRY_NAMESPACE}" \
  --dry-run=client -o yaml | kubectl apply -f -

# ── 3. Deploy registry ────────────────────────────────────────────────────────
log "Deploying registry..."
kubectl apply -f "$(dirname "$0")/../../manifests/registry/registry-deployment.yaml"
kubectl apply -f "$(dirname "$0")/../../manifests/registry/registry-service.yaml"
kubectl apply -f "$(dirname "$0")/../../manifests/registry/registry-pvc.yaml"

kubectl rollout status deployment/private-registry \
  -n "${REGISTRY_NAMESPACE}" --timeout=120s

# ── 4. Trust cert on MASTER (containerd + system) ────────────────────────────
log "Configuring containerd trust on master..."
_trust_cert_on_node() {
  local NODE_IP="$1"
  local NODE_USER="$2"
  local IS_MASTER="${3:-false}"

  if [ "${IS_MASTER}" = "true" ]; then
    _configure_trust_local
  else
    # Use a unique temp file to avoid permission issues from previous root-owned /tmp/registry.crt
    local REMOTE_TMP
    REMOTE_TMP=$(SSH_AS ssh ${SSH_OPTS} "${NODE_USER}@${NODE_IP}" "mktemp /tmp/registry.XXXXXX.crt")
    SSH_AS scp ${SSH_OPTS} \
      "${CERT_DIR}/registry.crt" \
      "${NODE_USER}@${NODE_IP}:${REMOTE_TMP}"
    SSH_AS ssh ${SSH_OPTS} "${NODE_USER}@${NODE_IP}" bash <<REMOTE
$(declare -f _configure_trust_remote)
_configure_trust_remote "${REGISTRY_HOST}" "${REGISTRY_PORT}" "${REGISTRY_FQDN}" "${REMOTE_TMP}"
REMOTE
  fi
}

_configure_trust_local() {
  # Already running as root on master — no sudo needed
  local HOST="${1:-${REGISTRY_HOST}}"
  local PORT="${2:-${REGISTRY_PORT}}"
  local FQDN="${3:-${REGISTRY_FQDN}}"

  if [ -d /etc/pki/ca-trust/source/anchors ]; then
    cp /tmp/registry.crt /etc/pki/ca-trust/source/anchors/registry.crt
    update-ca-trust extract
  elif [ -d /usr/local/share/ca-certificates ]; then
    cp /tmp/registry.crt /usr/local/share/ca-certificates/registry.crt
    update-ca-certificates
  fi

  mkdir -p "/etc/containerd/certs.d/${HOST}:${PORT}"
  mkdir -p "/etc/containerd/certs.d/${FQDN}:${PORT}"

  cat >"/etc/containerd/certs.d/${HOST}:${PORT}/hosts.toml" <<EOF
server = "https://${HOST}:${PORT}"
[host."https://${HOST}:${PORT}"]
  capabilities = ["pull", "resolve", "push"]
  ca = "/etc/ssl/certs/registry.crt"
  skip_verify = false
EOF
  cp "/etc/containerd/certs.d/${HOST}:${PORT}/hosts.toml" \
     "/etc/containerd/certs.d/${FQDN}:${PORT}/hosts.toml"

  install -m 0644 /tmp/registry.crt /etc/ssl/certs/registry.crt
  systemctl restart containerd
  echo "containerd configured on $(hostname)"
}

_configure_trust_remote() {
  local HOST="$1"; local PORT="$2"; local FQDN="$3"; local CERT="${4:-/tmp/registry.crt}"
  # System trust
  if [ -d /etc/pki/ca-trust/source/anchors ]; then
    sudo cp "${CERT}" /etc/pki/ca-trust/source/anchors/registry.crt
    sudo update-ca-trust extract
  elif [ -d /usr/local/share/ca-certificates ]; then
    sudo cp "${CERT}" /usr/local/share/ca-certificates/registry.crt
    sudo update-ca-certificates
  fi

  # containerd mirror config
  sudo mkdir -p "/etc/containerd/certs.d/${HOST}:${PORT}"
  sudo mkdir -p "/etc/containerd/certs.d/${FQDN}:${PORT}"

  sudo tee "/etc/containerd/certs.d/${HOST}:${PORT}/hosts.toml" > /dev/null <<EOF
server = "https://${HOST}:${PORT}"
[host."https://${HOST}:${PORT}"]
  capabilities = ["pull", "resolve", "push"]
  ca = "/etc/ssl/certs/registry.crt"
  skip_verify = false
EOF
  sudo cp "/etc/containerd/certs.d/${HOST}:${PORT}/hosts.toml" \
          "/etc/containerd/certs.d/${FQDN}:${PORT}/hosts.toml"

  # Copy cert to permanent location and clean up temp file
  sudo install -m 0644 "${CERT}" /etc/ssl/certs/registry.crt
  rm -f "${CERT}"

  sudo systemctl restart containerd
  echo "containerd configured on $(hostname)"
}

# Copy cert locally for the helper function
cp "${CERT_DIR}/registry.crt" /tmp/registry.crt
_configure_trust_local

# ── 5. Trust cert on all workers ─────────────────────────────────────────────
while IFS= read -r line <&3; do
  line="${line%%#*}"
  [[ -z "${line// }" ]] && continue
  read -r WORKER_IP WORKER_USER <<<"${line}"
  [[ -z "${WORKER_IP}" || -z "${WORKER_USER}" ]] && continue
  log "Configuring containerd trust on worker ${WORKER_IP} (user: ${WORKER_USER})..."
  (
    _trust_cert_on_node "${WORKER_IP}" "${WORKER_USER}" false
  ) || log "WARNING: Failed to configure worker ${WORKER_IP} – configure manually"
done 3< "${INVENTORY_FILE}"

# ── 6. Add /etc/hosts entry on all nodes ─────────────────────────────────────
log "Adding registry.local to /etc/hosts on all nodes..."
grep -q "${REGISTRY_FQDN}" /etc/hosts || \
  echo "${REGISTRY_HOST}  ${REGISTRY_FQDN}" >> /etc/hosts

while IFS= read -r line <&3; do
  line="${line%%#*}"
  [[ -z "${line// }" ]] && continue
  read -r WORKER_IP WORKER_USER <<<"${line}"
  [[ -z "${WORKER_IP}" || -z "${WORKER_USER}" ]] && continue
  SSH_AS ssh ${SSH_OPTS} "${WORKER_USER}@${WORKER_IP}" \
    "grep -q '${REGISTRY_FQDN}' /etc/hosts || echo '${REGISTRY_HOST}  ${REGISTRY_FQDN}' | sudo tee -a /etc/hosts" || true
done 3< "${INVENTORY_FILE}"

# ── 7. Smoke test ─────────────────────────────────────────────────────────────
log "Smoke-testing registry..."
sleep 5
curl -sk --cacert "${CERT_DIR}/registry.crt" \
  "https://${REGISTRY_HOST}:${REGISTRY_PORT}/v2/" && \
  log "Registry is responding!" || \
  log "WARNING: Registry not responding yet – check pod logs"

log "Registry setup complete."
log "Push images to: ${REGISTRY_HOST}:${REGISTRY_PORT}/<name>:<tag>"
