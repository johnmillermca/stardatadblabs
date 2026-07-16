#!/usr/bin/env bash
# =============================================================================
# 01-kubeadm-init.sh
# Run ONLY on the master node (192.168.1.50) after 00-common-prereqs.sh.
# =============================================================================
set -euo pipefail

MASTER_IP="192.168.1.50"
POD_CIDR="10.244.0.0/16"        # Flannel default; Calico also accepts this
SVC_CIDR="10.96.0.0/12"
CALICO_VERSION="v3.27.3"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# ── Pre-flight ────────────────────────────────────────────────────────────────
log "Pulling kubeadm images (speeds up init)..."
kubeadm config images pull

# ── Init ──────────────────────────────────────────────────────────────────────
log "Running kubeadm init..."
kubeadm init \
  --apiserver-advertise-address="${MASTER_IP}" \
  --pod-network-cidr="${POD_CIDR}" \
  --service-cidr="${SVC_CIDR}" \
  --upload-certs \
  2>&1 | tee /root/kubeadm-init.log

# ── kubeconfig for root ───────────────────────────────────────────────────────
log "Setting up kubeconfig..."
mkdir -p "$HOME/.kube"
cp /etc/kubernetes/admin.conf "$HOME/.kube/config"
chown "$(id -u):$(id -g)" "$HOME/.kube/config"

# ── kubeconfig for a non-root user (if SUDO_USER is set) ─────────────────────
if [ -n "${SUDO_USER:-}" ]; then
  USER_HOME=$(getent passwd "${SUDO_USER}" | cut -d: -f6)
  mkdir -p "${USER_HOME}/.kube"
  cp /etc/kubernetes/admin.conf "${USER_HOME}/.kube/config"
  chown -R "${SUDO_USER}:${SUDO_USER}" "${USER_HOME}/.kube"
  log "kubeconfig also written to ${USER_HOME}/.kube/config"
fi

# ── Install Calico CNI ────────────────────────────────────────────────────────
log "Installing Calico CNI ${CALICO_VERSION}..."
kubectl apply -f \
  "https://raw.githubusercontent.com/projectcalico/calico/${CALICO_VERSION}/manifests/calico.yaml"

# ── Wait for master to become Ready ──────────────────────────────────────────
log "Waiting for master node to become Ready..."
kubectl wait --for=condition=Ready node --all --timeout=180s

# ── Print join command ────────────────────────────────────────────────────────
log "Generating worker join command..."
JOIN_CMD=$(kubeadm token create --print-join-command)
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  WORKER JOIN COMMAND (copy and run on each worker node)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ${JOIN_CMD}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Save join command to file for use by worker provisioning script
echo "${JOIN_CMD}" > /root/worker-join-cmd.sh
chmod 600 /root/worker-join-cmd.sh
log "Join command saved to /root/worker-join-cmd.sh"

# ── Untaint master (optional – allows workloads on master) ───────────────────
# Uncomment if you want workloads on the master node too:
# kubectl taint nodes --all node-role.kubernetes.io/control-plane-

# ── Cluster health check ──────────────────────────────────────────────────────
log "Cluster health:"
kubectl get nodes -o wide
kubectl get pods -A

log "Master init complete."
