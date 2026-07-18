#!/usr/bin/env bash
# =============================================================================
# fix-iptables-modules.sh
#
# Loads the iptables kernel modules that are required by both kube-proxy
# (iptables mode) and Calico Felix on RHEL 10 / kernel 6.12.
#
# Root cause:
#   RHEL 10 ships with nf_tables as the native kernel filter. The iptables
#   compatibility modules (iptable_filter, iptable_nat) exist on disk but are
#   NOT auto-loaded. Without them:
#     - kube-proxy mode="" crashes: "No iptables support for family IPv4"
#     - Calico Felix panics: "iptables-save failed: incompatible nft rules"
#
# This is the definitive fix — load the modules, persist them, restart
# kube-proxy and calico-node on master so they re-programme their chains.
#
# Usage: sudo bash scripts/master/fix-iptables-modules.sh
# Safe to re-run — all steps are idempotent.
# =============================================================================
set -euo pipefail

log() { echo "[$(date '+%H:%M:%S')] $*"; }
ok()  { echo "  ✓ $*"; }

[[ $EUID -eq 0 ]] || { echo "[ERROR] Run as root: sudo bash $0" >&2; exit 1; }

# ── 1. Load iptables modules NOW ─────────────────────────────────────────────
log "Loading iptables kernel modules..."
for mod in ip_tables iptable_filter iptable_nat iptable_mangle nf_nat nf_conntrack; do
  modprobe "${mod}" 2>/dev/null && ok "loaded ${mod}" || echo "  - ${mod} already loaded or not needed"
done

# ── 2. Persist across reboots ─────────────────────────────────────────────────
log "Persisting modules in /etc/modules-load.d/iptables.conf..."
cat > /etc/modules-load.d/iptables.conf <<'EOF'
# Required by kube-proxy (iptables mode) and Calico Felix on RHEL 10
ip_tables
iptable_filter
iptable_nat
iptable_mangle
nf_nat
nf_conntrack
EOF
ok "persisted to /etc/modules-load.d/iptables.conf"

# ── 3. Verify modules are now loaded ─────────────────────────────────────────
log "Verifying modules..."
for mod in ip_tables iptable_filter iptable_nat; do
  if lsmod | grep -q "^${mod}"; then
    ok "${mod} loaded"
  else
    echo "  ✗ ${mod} STILL NOT loaded — check: modinfo ${mod}"
  fi
done

# ── 4. Restart kube-proxy (all nodes pick up via daemonset rollout) ───────────
log "Restarting kube-proxy daemonset..."
kubectl -n kube-system rollout restart daemonset/kube-proxy
sleep 20
PROXY_STATUS=$(kubectl -n kube-system get pods -l k8s-app=kube-proxy -o wide --no-headers 2>/dev/null)
echo "${PROXY_STATUS}"
MASTER_PROXY=$(echo "${PROXY_STATUS}" | grep master.local | awk '{print $2}')
if [[ "${MASTER_PROXY}" == "1/1" ]]; then
  ok "kube-proxy on master: Running 1/1"
else
  echo "  ⚠ kube-proxy on master still: ${MASTER_PROXY} — waiting 30s more..."
  sleep 30
  kubectl -n kube-system get pods -l k8s-app=kube-proxy -o wide --no-headers 2>/dev/null
fi

# ── 5. Restart calico-node on master so Felix re-programmes cali chains ───────
log "Restarting calico-node on master..."
CALICO_POD=$(kubectl get pod -n kube-system -l k8s-app=calico-node \
  -o wide --no-headers 2>/dev/null | grep master.local | awk '{print $1}')
if [[ -n "${CALICO_POD}" ]]; then
  kubectl delete pod "${CALICO_POD}" -n kube-system
  ok "calico-node pod ${CALICO_POD} deleted — waiting for restart..."
  for i in $(seq 1 24); do
    STATUS=$(kubectl get pod -n kube-system -l k8s-app=calico-node \
      -o wide --no-headers 2>/dev/null | grep master.local | awk '{print $2}')
    echo "  [${i}] calico-node master: ${STATUS:-starting}"
    [[ "${STATUS}" == "1/1" ]] && break
    sleep 5
  done
else
  echo "  ⚠ Could not find calico-node pod on master"
fi

# ── 6. Confirm cali dispatch chains are programmed for openbao pod ────────────
log "Checking Calico dispatch chains..."
sleep 5
KUBE_PROXY_POD=$(kubectl get pod -n kube-system -l k8s-app=kube-proxy \
  -o wide --no-headers 2>/dev/null | grep master.local | awk '{print $1}')
if [[ -n "${KUBE_PROXY_POD}" ]]; then
  echo "  Cali interfaces in Calico dispatch chains:"
  kubectl exec -n kube-system "${KUBE_PROXY_POD}" -- \
    nft list table ip filter 2>/dev/null | grep "iifname.*cali" | \
    grep -oE '"cali[a-z0-9]+"' | sort -u | sed 's/^/    /'
  echo "  Cali interfaces on host:"
  ip link show | grep -oE 'cali[a-z0-9]+' | sort -u | sed 's/^/    /'
fi

# ── 7. Unseal OpenBao if sealed (pod restarted during this fix) ───────────────
log "Checking OpenBao seal status..."
BAO_STATUS=$(kubectl exec openbao-0 -n prod -- bao status 2>/dev/null | grep "^Sealed" || echo "Sealed check failed")
if echo "${BAO_STATUS}" | grep -q "true"; then
  log "OpenBao is sealed — unsealing with saved keys..."
  KEYS_FILE="/home/star_master/openbao-init-keys.json"
  if [[ -f "${KEYS_FILE}" ]]; then
    python3 -c "
import json
d = json.load(open('${KEYS_FILE}'))
for k in d['unseal_keys_b64'][:3]:
    print(k)
" | while read key; do
      kubectl exec openbao-0 -n prod -- bao operator unseal "${key}" 2>/dev/null | \
        grep -E "Sealed|Progress" || true
    done
    ok "unseal commands sent"
  else
    echo "  ⚠ Keys file not found: ${KEYS_FILE} — unseal manually"
  fi
else
  ok "OpenBao already unsealed"
fi

# ── 8. Final connectivity test ────────────────────────────────────────────────
log "Final connectivity test..."
sleep 10

test_url() {
  local label="$1" url="$2"
  result=$(curl -sf --max-time 5 "${url}" 2>/dev/null || echo "")
  if echo "${result}" | grep -q "initialized"; then
    SEALED=$(echo "${result}" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["sealed"])' 2>/dev/null)
    ok "${label} → initialized=true sealed=${SEALED}"
  else
    echo "  ✗ ${label} — still not reachable"
  fi
}

POD_IP=$(kubectl get pod openbao-0 -n prod -o jsonpath='{.status.podIP}' 2>/dev/null || echo "")
CIP=$(kubectl get svc openbao -n prod -o jsonpath='{.spec.clusterIP}' 2>/dev/null || echo "")
[[ -n "${POD_IP}" ]] && test_url "Pod IP    ${POD_IP}:8200"     "http://${POD_IP}:8200/v1/sys/health"
[[ -n "${CIP}" ]]    && test_url "ClusterIP ${CIP}:8200"        "http://${CIP}:8200/v1/sys/health"
test_url             "NodePort  192.168.1.50:30820"              "http://192.168.1.50:30820/v1/sys/health"

echo ""
log "=== fix-iptables-modules.sh complete ==="
echo ""
echo "  If NodePort still fails, run the quick health check:"
echo "    curl -s http://192.168.1.50:30820/v1/sys/health | python3 -m json.tool"
