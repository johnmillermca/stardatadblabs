#!/usr/bin/env bash
# =============================================================================
# 02-worker-join.sh
# Run on each worker node (192.168.1.51-54) AFTER:
#   1. 00-common-prereqs.sh has been run on this worker
#   2. 01-kubeadm-init.sh has completed on the master
#
# Usage: sudo ./02-worker-join.sh <master-ip>
#   OR:  sudo ./02-worker-join.sh   (defaults to 192.168.1.50)
# =============================================================================
set -euo pipefail

MASTER_IP="${1:-192.168.1.50}"

log() { echo "[$(date '+%H:%M:%S')] $*"; }
die() { echo "[ERROR] $*" >&2; exit 1; }

# ── Fetch join command from master ────────────────────────────────────────────
log "Fetching join command from master ${MASTER_IP}..."
# Requires SSH key-based access from this worker to master as root.
# Alternatively, manually copy the join command from /root/worker-join-cmd.sh
# on the master and paste it below.

if ssh -o BatchMode=yes -o StrictHostKeyChecking=no "root@${MASTER_IP}" true 2>/dev/null; then
  JOIN_CMD=$(ssh -o StrictHostKeyChecking=no "root@${MASTER_IP}" \
    "kubeadm token create --print-join-command")
else
  log "SSH to master not available. Trying local file..."
  [ -f /tmp/worker-join-cmd.sh ] || die \
    "No SSH access and no /tmp/worker-join-cmd.sh found.
    Copy /root/worker-join-cmd.sh from master to /tmp/worker-join-cmd.sh on this node."
  JOIN_CMD=$(cat /tmp/worker-join-cmd.sh)
fi

log "Executing join command..."
eval "${JOIN_CMD}"

log "Worker joined. Verify on master with: kubectl get nodes"
