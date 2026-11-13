#!/usr/bin/env bash
# =============================================================================
# 08_patch_fe_conf_auto_partition.sh
# Permanently enable AUTO PARTITION in the StarData FE ConfigMap.
#
# Problem
# ───────
# ADMIN SET FRONTEND CONFIG is in-memory only.  When a FE pod restarts
# (upgrade, node eviction, rolling restart) the setting reverts to the
# fe.conf default (false).
#
# Solution
# ────────
# This script patches the stardata-fe-config ConfigMap to append:
#
#   enable_auto_partition  = true
#   max_auto_partition_num = 50000
#
# to fe.conf.  Those lines are read by every FE pod at startup — the setting
# is therefore permanent and survives all future pod restarts, upgrades, and
# cluster rebuilds, as long as the ConfigMap is applied from this repo via
# ArgoCD / kubectl.
#
# After patching the ConfigMap, the script triggers a rolling restart of the
# stardata-fe StatefulSet so the change takes effect without downtime
# (BdbJE quorum with 3 FEs tolerates one pod restarting at a time).
#
# Usage
# ─────
#   bash manifests/stardata/setup/08_patch_fe_conf_auto_partition.sh
#
# Dry-run (shows the patched ConfigMap without applying):
#   DRY_RUN=1 bash manifests/stardata/setup/08_patch_fe_conf_auto_partition.sh
#
# Idempotent — safe to run multiple times; already-present lines are not
# duplicated.
# =============================================================================
set -euo pipefail

NAMESPACE="${NAMESPACE:-prod}"
CONFIGMAP="${CONFIGMAP:-stardata-fe-config}"
STATEFULSET="${STATEFULSET:-stardata-fe}"
DRY_RUN="${DRY_RUN:-0}"

green()  { printf '\033[0;32m%s\033[0m\n' "$*"; }
yellow() { printf '\033[0;33m%s\033[0m\n' "$*"; }
red()    { printf '\033[0;31m%s\033[0m\n' "$*"; }
bold()   { printf '\033[1m%s\033[0m\n'    "$*"; }

bold "=== Patch fe.conf: enable_auto_partition (permanent) ==="
echo ""

# ── Step 1: Fetch current fe.conf from ConfigMap ──────────────────────────────
yellow "→ Reading current ${CONFIGMAP} from namespace ${NAMESPACE} …"
CURRENT_CONF="$(kubectl get configmap "${CONFIGMAP}" -n "${NAMESPACE}" \
    -o jsonpath='{.data.fe\.conf}')"

# ── Step 2: Check idempotency ─────────────────────────────────────────────────
if echo "${CURRENT_CONF}" | grep -q "enable_auto_partition"; then
    green "✓ enable_auto_partition already present in fe.conf — no patch needed."
    echo ""
    echo "  Current value:"
    echo "${CURRENT_CONF}" | grep "auto_partition"
    echo ""
    echo "  To force a re-patch, remove the line from the ConfigMap and re-run."
    exit 0
fi

# ── Step 3: Build the patched fe.conf ─────────────────────────────────────────
PATCHED_CONF="${CURRENT_CONF}
    # ── Auto-partition (snap_id) — added by 08_patch_fe_conf_auto_partition.sh ─
    enable_auto_partition  = true
    max_auto_partition_num = 50000"

yellow "→ Patched fe.conf (last 10 lines):"
echo "${PATCHED_CONF}" | tail -10
echo ""

# ── Step 4: Apply the patched ConfigMap ──────────────────────────────────────
if [[ "${DRY_RUN}" == "1" ]]; then
    yellow "  [DRY RUN] Would patch ConfigMap ${CONFIGMAP} in namespace ${NAMESPACE}"
    yellow "  [DRY RUN] Would rollout restart statefulset/${STATEFULSET}"
    exit 0
fi

yellow "→ Applying patched ConfigMap …"
kubectl patch configmap "${CONFIGMAP}" -n "${NAMESPACE}" \
    --type merge \
    -p "{\"data\":{\"fe.conf\":$(echo "${PATCHED_CONF}" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))')}}"

green "✓ ConfigMap patched"

# ── Step 5: Rolling restart of FE StatefulSet ─────────────────────────────────
yellow "→ Triggering rolling restart of statefulset/${STATEFULSET} …"
yellow "  (BdbJE quorum is maintained — 3 FE pods, one restarts at a time)"
kubectl rollout restart statefulset/"${STATEFULSET}" -n "${NAMESPACE}"

yellow "→ Waiting for rollout to complete (timeout 5 min) …"
kubectl rollout status statefulset/"${STATEFULSET}" -n "${NAMESPACE}" \
    --timeout=300s

green "✓ FE StatefulSet rollout complete"

# ── Step 6: Verify via ADMIN SHOW FRONTEND CONFIG ────────────────────────────
yellow "→ Verifying via Doris FE config query …"
export STARDATA_PASS
if [[ -z "${STARDATA_PASS:-}" ]]; then
    STARDATA_PASS="$(kubectl get secret stardata-credentials -n prod \
        -o jsonpath='{.data.admin-password}' | base64 -d)"
fi

mysql -h 192.168.1.50 -P 9030 -u root -p"${STARDATA_PASS}" \
      --default-character-set=utf8mb4 \
      -e "ADMIN SHOW FRONTEND CONFIG LIKE '%auto_partition%';" 2>/dev/null \
&& green "✓ Verification query succeeded" \
|| yellow "  Could not verify via mysql — check manually with:"
echo "    ADMIN SHOW FRONTEND CONFIG LIKE '%auto_partition%';"

echo ""
bold "════════════════════════════════════════════════════════════════"
green "  enable_auto_partition is now PERMANENT in StarData fe.conf."
bold "════════════════════════════════════════════════════════════════"
echo ""
echo "  Every FE pod restart will load:"
echo "    enable_auto_partition  = true"
echo "    max_auto_partition_num = 50000"
echo ""
echo "  This applies to ALL future databases and tables cluster-wide."
