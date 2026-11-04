#!/usr/bin/env bash
# =============================================================================
# 18-setup-stardata-nodes.sh
# Dedicate worker2, worker3, worker4 exclusively to Apache Stardata.
#
# What this does:
#   1. Taints each node with dedicated=stardata:NoSchedule so that only
#      pods with the matching toleration can be scheduled there.
#   2. Labels each node with stardata=true for use in nodeAffinity rules.
#
# Run once after the cluster is up, before applying stardata manifests.
#
# Usage:
#   bash scripts/master/18-setup-stardata-nodes.sh
# =============================================================================
set -euo pipefail

NODES=(worker2.local worker3.local worker4.local)
TAINT_KEY="dedicated"
TAINT_VALUE="stardata"
TAINT_EFFECT="NoSchedule"
LABEL_KEY="stardata"
LABEL_VALUE="true"

ok()   { echo "  ✓ $*"; }
info() { echo "  → $*"; }

echo "=== Stardata node setup ==="
echo ""

for NODE in "${NODES[@]}"; do
  info "Configuring ${NODE} ..."

  # Apply taint — idempotent (overwrite if already set)
  kubectl taint node "${NODE}" \
    "${TAINT_KEY}=${TAINT_VALUE}:${TAINT_EFFECT}" \
    --overwrite
  ok "Taint applied: ${TAINT_KEY}=${TAINT_VALUE}:${TAINT_EFFECT}"

  # Apply label
  kubectl label node "${NODE}" \
    "${LABEL_KEY}=${LABEL_VALUE}" \
    --overwrite
  ok "Label applied: ${LABEL_KEY}=${LABEL_VALUE}"

  echo ""
done

echo "=== Node status ==="
kubectl get nodes -o custom-columns=\
"NAME:.metadata.name,STATUS:.status.conditions[-1].type,TAINTS:.spec.taints[*].key" \
  | grep -E "NAME|worker[234]"

echo ""
echo "Done. worker2/3/4 are now exclusively dedicated to Stardata."
echo "Apply manifests with:"
echo "  kubectl apply -f manifests/stardata/ -n prod"
