#!/usr/bin/env bash
# =============================================================================
# fix-cleanup-nftables.sh
#
# Removes the leftover kube-proxy nftables tables created when kube-proxy was
# temporarily switched to nftables mode. These tables cause Calico Felix to
# panic continuously with:
#   "iptables-save failed because there are incompatible nft rules in the table"
#
# After cleanup, restarts kube-proxy (iptables mode) and calico-node on master
# so both re-programme their chains cleanly.
#
# Usage: sudo bash scripts/master/fix-cleanup-nftables.sh
# Safe to re-run.
# =============================================================================
set -euo pipefail

log() { echo "[$(date '+%H:%M:%S')] $*"; }
ok()  { echo "  ✓ $*"; }

[[ $EUID -eq 0 ]] || { echo "[ERROR] Run as root: sudo bash $0" >&2; exit 1; }

# ── 1. Remove stale kube-proxy nftables tables ────────────────────────────────
log "Removing leftover kube-proxy nftables tables..."
nft delete table ip kube-proxy  2>/dev/null && ok "deleted table ip kube-proxy"  || ok "table ip kube-proxy not present"
nft delete table ip6 kube-proxy 2>/dev/null && ok "deleted table ip6 kube-proxy" || ok "table ip6 kube-proxy not present"

# Verify
REMAINING=$(nft list tables 2>/dev/null | grep kube-proxy || echo "none")
echo "  Remaining kube-proxy tables: ${REMAINING}"

# ── 2. Ensure iptables modules are loaded ────────────────────────────────────
log "Ensuring iptables modules are loaded..."
for mod in ip_tables iptable_filter iptable_nat iptable_mangle nf_nat nf_conntrack; do
  modprobe "${mod}" 2>/dev/null || true
done
ok "iptables modules loaded"

# ── 3. Restart kube-proxy on master ──────────────────────────────────────────
log "Restarting kube-proxy daemonset..."
kubectl -n kube-system rollout restart daemonset/kube-proxy
sleep 20
MASTER_STATUS=$(kubectl -n kube-system get pods -l k8s-app=kube-proxy \
  -o wide --no-headers 2>/dev/null | grep master.local | awk '{print $2}')
if [[ "${MASTER_STATUS}" == "1/1" ]]; then
  ok "kube-proxy master: 1/1 Running"
else
  echo "  ⚠ kube-proxy master: ${MASTER_STATUS} — waiting 20s more..."
  sleep 20
  kubectl -n kube-system get pods -l k8s-app=kube-proxy -o wide --no-headers 2>/dev/null | grep master.local
fi

# ── 4. Restart calico-node on master ──────────────────────────────────────────
log "Restarting calico-node on master..."
CALICO_POD=$(kubectl get pod -n kube-system -l k8s-app=calico-node \
  -o wide --no-headers 2>/dev/null | grep master.local | awk '{print $1}')
if [[ -n "${CALICO_POD}" ]]; then
  kubectl delete pod "${CALICO_POD}" -n kube-system
  ok "calico-node ${CALICO_POD} deleted"
  for i in $(seq 1 30); do
    STATUS=$(kubectl get pod -n kube-system -l k8s-app=calico-node \
      -o wide --no-headers 2>/dev/null | grep master.local | awk '{print $2}')
    echo "  [${i}] calico-node master: ${STATUS:-starting}"
    [[ "${STATUS}" == "1/1" ]] && break
    sleep 5
  done
fi

# ── 5. Verify Calico is no longer panicking ───────────────────────────────────
log "Checking calico-node logs on master..."
NEW_CALICO=$(kubectl get pod -n kube-system -l k8s-app=calico-node \
  -o wide --no-headers 2>/dev/null | grep master.local | awk '{print $1}')
if [[ -n "${NEW_CALICO}" ]]; then
  sleep 10
  ERRORS=$(kubectl logs "${NEW_CALICO}" -n kube-system --tail=10 2>/dev/null | \
    grep -cE "panic|incompatible nft" || echo 0)
  if [[ "${ERRORS}" == "0" ]]; then
    ok "calico-node no longer panicking"
  else
    echo "  ⚠ calico-node still has errors:"
    kubectl logs "${NEW_CALICO}" -n kube-system --tail=5 2>/dev/null | grep -E "error|panic|fail" | head -5
  fi
fi

# ── 6. Confirm cali dispatch chains are now programmed ───────────────────────
log "Checking Calico cali dispatch chains..."
sleep 5
KUBE_PROXY_POD=$(kubectl get pod -n kube-system -l k8s-app=kube-proxy \
  -o wide --no-headers 2>/dev/null | grep master.local | awk '{print $1}')
if [[ -n "${KUBE_PROXY_POD}" ]]; then
  echo "  Interfaces in Calico dispatch chains:"
  kubectl exec -n kube-system "${KUBE_PROXY_POD}" -- \
    nft list table ip filter 2>/dev/null | grep "iifname.*cali" | \
    grep -oE '"cali[a-z0-9]+"' | sort -u | sed 's/^/    /' || \
  echo "    (nft filter table — checking iptables)"
  echo "  Cali interfaces on host:"
  ip link show | grep -oE 'cali[a-z0-9]+' | sort -u | sed 's/^/    /'
fi

# ── 7. Unseal OpenBao if sealed ───────────────────────────────────────────────
log "Checking OpenBao seal status..."
BAO_STATUS=$(kubectl exec openbao-0 -n prod -- bao status 2>/dev/null | grep "^Sealed" || echo "Sealed     true")
if echo "${BAO_STATUS}" | grep -q "true"; then
  log "OpenBao is sealed — unsealing..."
  KEYS_FILE="/home/star_master/openbao-init-keys.json"
  [[ -f "${KEYS_FILE}" ]] || KEYS_FILE="/root/openbao-init-keys.json"
  if [[ -f "${KEYS_FILE}" ]]; then
    python3 -c "
import json
d = json.load(open('${KEYS_FILE}'))
for k in d['unseal_keys_b64'][:3]: print(k)
" | while read key; do
      kubectl exec openbao-0 -n prod -- bao operator unseal "${key}" 2>/dev/null | \
        grep -E "Sealed|Progress" || true
    done
    sleep 5
    ok "unseal commands sent"
  fi
fi

# ── 8. Final connectivity test ────────────────────────────────────────────────
log "Final connectivity test..."
sleep 10

test_url() {
  local label="$1" url="$2"
  result=$(curl -sf --max-time 5 "${url}" 2>/dev/null || echo "")
  if echo "${result}" | grep -q "initialized"; then
    SEALED=$(echo "${result}" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["sealed"])' 2>/dev/null || echo "?")
    ok "${label} → initialized=true sealed=${SEALED}"
  else
    echo "  ✗ ${label} — not reachable"
  fi
}

POD_IP=$(kubectl get pod openbao-0 -n prod -o jsonpath='{.status.podIP}' 2>/dev/null || echo "")
CIP=$(kubectl get svc openbao -n prod -o jsonpath='{.spec.clusterIP}' 2>/dev/null || echo "")
[[ -n "${POD_IP}" ]] && test_url "Pod IP    ${POD_IP}:8200"  "http://${POD_IP}:8200/v1/sys/health"
[[ -n "${CIP}" ]]    && test_url "ClusterIP ${CIP}:8200"     "http://${CIP}:8200/v1/sys/health"
test_url "NodePort  192.168.1.50:30820"                       "http://192.168.1.50:30820/v1/sys/health"

echo ""
log "=== fix-cleanup-nftables.sh complete ==="
