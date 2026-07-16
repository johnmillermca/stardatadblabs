#!/usr/bin/env bash
# =============================================================================
# 03-label-workers.sh
# Run on MASTER after all workers have joined.
# Labels workers and verifies cluster health.
# =============================================================================
set -euo pipefail

WORKERS=(
  "192.168.1.51"
  "192.168.1.52"
  "192.168.1.53"
  "192.168.1.54"
)

log() { echo "[$(date '+%H:%M:%S')] $*"; }

log "Current node list:"
kubectl get nodes -o wide

# Label worker nodes (by their IP or hostname as registered)
# Kubernetes registers nodes by their hostname; resolve by InternalIP
for IP in "${WORKERS[@]}"; do
  NODE=$(kubectl get nodes -o json | \
    python3 -c "
import sys, json
items = json.load(sys.stdin)['items']
for n in items:
    for addr in n['status']['addresses']:
        if addr['type'] == 'InternalIP' and addr['address'] == '${IP}':
            print(n['metadata']['name'])
" 2>/dev/null || true)
  if [ -n "${NODE}" ]; then
    kubectl label node "${NODE}" node-role.kubernetes.io/worker=worker --overwrite
    log "Labeled ${NODE} (${IP}) as worker"
  else
    log "WARNING: Node with IP ${IP} not found yet"
  fi
done

# ── Verify cluster health ─────────────────────────────────────────────────────
log "Waiting for all nodes to be Ready..."
kubectl wait --for=condition=Ready node --all --timeout=300s

log "Final cluster state:"
kubectl get nodes -o wide
kubectl get pods -A --field-selector=status.phase!=Running 2>/dev/null | head -30 || true

log "Worker labeling complete."
