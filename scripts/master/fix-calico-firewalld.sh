#!/usr/bin/env bash
# =============================================================================
# fix-calico-firewalld.sh
#
# Adds Calico veth interfaces (cali+) and the tunnel (tunl0) to firewalld's
# trusted zone so pod traffic returning to the host is accepted.
#
# Root cause (documented in docs/platform-guide.md §14.4):
#   firewalld filter_INPUT_POLICIES has a final "reject" for unmatched interfaces.
#   Calico veth interfaces (caliXXXX) are not in any firewalld zone, so return
#   packets from pods to the host (e.g. OpenBao pod → master) are rejected.
#   The fix: add the interface pattern to the trusted zone permanently.
#
# Safe to re-run — all commands are idempotent.
# Run: sudo bash scripts/master/fix-calico-firewalld.sh
# =============================================================================
set -euo pipefail

log() { echo "[$(date '+%H:%M:%S')] $*"; }
ok()  { echo "  ✓ $*"; }

[[ $EUID -eq 0 ]] || { echo "[ERROR] Run as root: sudo bash $0" >&2; exit 1; }

log "Adding cali+ interfaces and tunl0 to firewalld trusted zone..."

# Add wildcard interface pattern for all calico veth interfaces
firewall-cmd --permanent --zone=trusted --add-interface=cali+ 2>/dev/null || \
  firewall-cmd --permanent --zone=trusted --add-interface=cali0  2>/dev/null || true

# Add tunl0 (Calico IPIP tunnel)
firewall-cmd --permanent --zone=trusted --add-interface=tunl0 2>/dev/null || true

# Also add each existing cali interface right now (belt-and-suspenders)
for iface in $(ip link show | grep -oE 'cali[a-z0-9]+' | sort -u); do
  firewall-cmd --permanent --zone=trusted --add-interface="${iface}" 2>/dev/null || true
  firewall-cmd --zone=trusted --add-interface="${iface}" 2>/dev/null || true
  ok "added ${iface} to trusted zone"
done

firewall-cmd --reload
ok "firewalld reloaded"

# Add a dispatcher script to auto-add new cali interfaces to trusted zone as they appear
cat > /etc/NetworkManager/dispatcher.d/98-cali-trusted.sh <<'DISP'
#!/bin/bash
# Auto-add Calico veth interfaces to firewalld trusted zone
IFACE="$1"
ACTION="$2"
if [[ "$ACTION" == "up" ]] && [[ "$IFACE" == cali* ]]; then
  firewall-cmd --zone=trusted --add-interface="${IFACE}" 2>/dev/null || true
fi
DISP
chmod +x /etc/NetworkManager/dispatcher.d/98-cali-trusted.sh
ok "NM dispatcher script installed: /etc/NetworkManager/dispatcher.d/98-cali-trusted.sh"

# Install a systemd watcher as belt-and-suspenders (cali interfaces created by
# Calico CNI, not NetworkManager, so NM dispatcher may not fire)
cat > /usr/local/bin/fix-cali-firewalld.sh <<'WATCH'
#!/bin/bash
# Watch for new cali interfaces and add them to firewalld trusted zone
declare -A SEEN
while true; do
  for iface in $(ls /proc/sys/net/ipv4/conf/ | grep cali 2>/dev/null); do
    if [[ -z "${SEEN[$iface]:-}" ]]; then
      SEEN[$iface]=1
      firewall-cmd --zone=trusted --add-interface="${iface}" 2>/dev/null || true
      echo 0 > /proc/sys/net/ipv4/conf/${iface}/rp_filter 2>/dev/null || true
    fi
  done
  sleep 3
done
WATCH
chmod +x /usr/local/bin/fix-cali-firewalld.sh

cat > /etc/systemd/system/fix-cali-firewalld.service <<'SVC'
[Unit]
Description=Add Calico veth interfaces to firewalld trusted zone
After=network.target firewalld.service

[Service]
Type=simple
ExecStart=/usr/local/bin/fix-cali-firewalld.sh
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
SVC

systemctl daemon-reload
systemctl enable --now fix-cali-firewalld.service
ok "fix-cali-firewalld.service enabled and started"

# ── Verify ────────────────────────────────────────────────────────────────────
log "Verifying..."
sleep 2

echo ""
echo "  Trusted zone interfaces:"
firewall-cmd --zone=trusted --list-interfaces 2>/dev/null | tr ' ' '\n' | sed 's/^/    /'
echo ""
echo "  Trusted zone sources:"
firewall-cmd --zone=trusted --list-sources 2>/dev/null | tr ' ' '\n' | sed 's/^/    /'
echo ""

POD_IP=$(kubectl get pod openbao-0 -n prod -o jsonpath='{.status.podIP}' 2>/dev/null || echo "")
CLUSTER_IP=$(kubectl get svc openbao -n prod -o jsonpath='{.spec.clusterIP}' 2>/dev/null || echo "")

test_url() {
  local label="$1" url="$2"
  result=$(curl -sf --max-time 4 "${url}" 2>&1)
  if echo "${result}" | grep -q "initialized"; then
    echo "  ✓ ${label}: $(echo ${result} | python3 -c 'import sys,json; d=json.loads(sys.stdin.read()); print(f\"initialized={d[\"initialized\"]} sealed={d[\"sealed\"]}\")')"
  else
    echo "  ✗ ${label}: still not reachable (exit $?)"
  fi
}

[[ -n "${POD_IP}" ]]    && test_url "Pod IP    ${POD_IP}:8200"    "http://${POD_IP}:8200/v1/sys/health"
[[ -n "${CLUSTER_IP}" ]] && test_url "ClusterIP ${CLUSTER_IP}:8200" "http://${CLUSTER_IP}:8200/v1/sys/health"
test_url "NodePort  192.168.1.50:30820" "http://192.168.1.50:30820/v1/sys/health"

echo ""
log "=== fix-calico-firewalld.sh complete ==="
