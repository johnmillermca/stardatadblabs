#!/usr/bin/env bash
# =============================================================================
# fix-master-firewall.sh
#
# Fixes two issues on the master node that prevent NodePort/ClusterIP access
# from the master itself (documented in docs/platform-guide.md §14.4 & §14.6):
#
#   §14.4  firewalld blocks pod CIDR (10.244.0.0/16) and service CIDR
#          (10.96.0.0/12) — NodePort and ClusterIP calls time out
#   §14.6  rp_filter=1 on Calico veth interfaces drops return packets
#          (already fixed live; this persists it and installs a watchdog)
#
# Run: sudo bash scripts/master/fix-master-firewall.sh
# Safe to re-run — all commands are idempotent.
# =============================================================================
set -euo pipefail

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "  ✓ $*"; }

[[ $EUID -eq 0 ]] || { echo "[ERROR] Run as root: sudo bash $0" >&2; exit 1; }

# ── §14.4  Trust pod + service CIDRs in firewalld ────────────────────────────
log "§14.4 — Trusting pod CIDR (10.244.0.0/16) and service CIDR (10.96.0.0/12)..."

firewall-cmd --permanent --add-source=10.244.0.0/16 --zone=trusted 2>/dev/null || true
firewall-cmd --permanent --add-source=10.96.0.0/12  --zone=trusted 2>/dev/null || true
ok "pod and service CIDRs added to trusted zone"

# ── Also trust ClusterIP of OpenBao directly (belt-and-suspenders) ────────────
OPENBAO_CIP=$(kubectl get svc openbao -n prod -o jsonpath='{.spec.clusterIP}' 2>/dev/null || true)
if [[ -n "${OPENBAO_CIP}" ]]; then
  firewall-cmd --permanent --add-source="${OPENBAO_CIP}/32" --zone=trusted 2>/dev/null || true
  ok "OpenBao ClusterIP ${OPENBAO_CIP} added to trusted zone"
fi

# ── Reload firewalld to apply permanent rules ─────────────────────────────────
log "Reloading firewalld..."
firewall-cmd --reload
ok "firewalld reloaded"

# ── §14.6  rp_filter — persist 0 for all + default template ─────────────────
log "§14.6 — Persisting rp_filter=0 for all Calico interfaces..."
sysctl -w net.ipv4.conf.all.rp_filter=0     > /dev/null
sysctl -w net.ipv4.conf.default.rp_filter=0 > /dev/null
sysctl -w net.ipv4.conf.tunl0.rp_filter=0   > /dev/null

cat > /etc/sysctl.d/99-calico-ipvs.conf <<'EOF'
net.ipv4.conf.all.rp_filter     = 0
net.ipv4.conf.default.rp_filter = 0
net.ipv4.conf.tunl0.rp_filter   = 0
net.ipv4.conf.all.accept_local  = 1
EOF
sysctl -p /etc/sysctl.d/99-calico-ipvs.conf > /dev/null
ok "rp_filter sysctl persisted to /etc/sysctl.d/99-calico-ipvs.conf"

# Fix any existing cali interfaces right now
for iface in $(ls /proc/sys/net/ipv4/conf/ | grep cali); do
  echo 0 > /proc/sys/net/ipv4/conf/${iface}/rp_filter 2>/dev/null || true
done
ok "rp_filter=0 applied to all current cali interfaces"

# ── §14.6  Install watchdog service (fixes new cali veths as they appear) ────
log "Installing fix-cali-rp-filter watchdog service..."
cat > /usr/local/bin/fix-cali-rp-filter.sh <<'SCRIPT'
#!/bin/bash
while true; do
  for iface in $(ls /proc/sys/net/ipv4/conf/ | grep cali 2>/dev/null); do
    val=$(cat /proc/sys/net/ipv4/conf/${iface}/rp_filter 2>/dev/null || echo 0)
    [ "$val" = "1" ] && echo 0 > /proc/sys/net/ipv4/conf/${iface}/rp_filter
  done
  sleep 2
done
SCRIPT
chmod +x /usr/local/bin/fix-cali-rp-filter.sh

cat > /etc/systemd/system/fix-cali-rp-filter.service <<'EOF'
[Unit]
Description=Fix rp_filter=0 for Calico veth interfaces (master node)
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/fix-cali-rp-filter.sh
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now fix-cali-rp-filter.service
ok "fix-cali-rp-filter.service enabled and started"

# ── Verify ────────────────────────────────────────────────────────────────────
log "Verifying..."
sleep 2

echo ""
echo "  Trusted zones:"
firewall-cmd --zone=trusted --list-sources | sed 's/^/    /'
echo ""

# Test pod IP directly
POD_IP=$(kubectl get pod openbao-0 -n prod -o jsonpath='{.status.podIP}' 2>/dev/null || echo "")
if [[ -n "${POD_IP}" ]]; then
  if curl -sf --max-time 5 "http://${POD_IP}:8200/v1/sys/health" > /dev/null 2>&1; then
    ok "Direct pod IP ${POD_IP}:8200 — reachable"
  else
    echo "  ✗ Pod IP ${POD_IP}:8200 still unreachable — check 'kubectl describe pod openbao-0 -n prod'"
  fi
fi

# Test NodePort
if curl -sf --max-time 5 http://192.168.1.50:30820/v1/sys/health > /dev/null 2>&1; then
  ok "NodePort 192.168.1.50:30820 — reachable  ✓"
  curl -s http://192.168.1.50:30820/v1/sys/health | python3 -m json.tool | grep -E "initialized|sealed"
else
  echo ""
  echo "  ✗ NodePort 30820 still not responding."
  echo ""
  echo "  Next: check if OpenBao pod is sealed:"
  echo "    kubectl exec openbao-0 -n prod -- bao status"
  echo "  If sealed, unseal with:"
  echo "    kubectl exec openbao-0 -n prod -- bao operator unseal <key1>"
  echo "    kubectl exec openbao-0 -n prod -- bao operator unseal <key2>"
  echo "    kubectl exec openbao-0 -n prod -- bao operator unseal <key3>"
  echo "  Keys are in: /home/star_master/openbao-init-keys.json"
fi

echo ""
log "=== fix-master-firewall.sh complete ==="
