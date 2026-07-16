#!/usr/bin/env bash
# =============================================================================
# fix-gateway-routing.sh
#
# PURPOSE
#   192.168.1.50 is BOTH the Kubernetes master node AND the NAT gateway for
#   the cluster.  Workers (.51–.54) reach the internet through .50.
#   .50 itself reaches the internet through the upstream router at 192.168.1.254
#   via wlp2s0 (WiFi).
#
# PROBLEMS THIS FIXES
#   1. eno1 has a stray 0.0.0.0/32 host route that black-holes traffic
#   2. Calico/kube-proxy iptables MASQUERADE rules SNAT outbound packets
#      from .50 itself using the k8s node IP (192.168.1.50 on eno1) as the
#      source — so reply packets come back on eno1 but the connection was
#      initiated via wlp2s0 → packets are dropped (asymmetric routing)
#   3. ip_forward enabled but MASQUERADE for worker→internet not set on wlp2s0
#
# WHAT IT DOES
#   a) Removes the stray 0.0.0.0/32 route from the eno1 NM connection
#   b) Adds a dedicated iptables MASQUERADE rule on wlp2s0 for ALL outbound
#      internet traffic (both from .50 itself and forwarded from workers)
#   c) Ensures ip_forward is on persistently
#   d) Adds a kernel RPFILTER exception so asymmetric reply traffic is accepted
#   e) Makes everything survive reboots via NetworkManager dispatcher + sysctl
#
# USAGE
#   sudo bash scripts/master/fix-gateway-routing.sh
# =============================================================================
set -euo pipefail

WAN_IF="wlp2s0"          # interface that reaches 192.168.1.254 (upstream router)
LAN_IF="eno1"             # interface for k8s cluster LAN (.50–.54)
LAN_NET="192.168.1.0/24" # local subnet
GW="192.168.1.254"        # upstream router

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "  ✓ $*"; }
warn() { echo "  ⚠ $*"; }

if [[ $EUID -ne 0 ]]; then
  echo "[ERROR] Run as root: sudo bash $0" >&2
  exit 1
fi

# ── 1. Remove the stray 0.0.0.0/32 host route from eno1 ─────────────────────
log "Step 1 — Remove stray 0.0.0.0/32 route from eno1 NM connection..."
CURRENT_ROUTES=$(nmcli -g ipv4.routes connection show eno1 2>/dev/null || true)
if echo "${CURRENT_ROUTES}" | grep -q "0.0.0.0/32\|0.0.0.0/0"; then
  nmcli connection modify eno1 ipv4.routes ""
  ok "Cleared stray routes from eno1"
else
  ok "eno1 routes already clean: ${CURRENT_ROUTES:-none}"
fi

# ── 2. Ensure ip_forward is on (persistent via sysctl) ────────────────────────
log "Step 2 — Ensure ip_forward is enabled persistently..."
sysctl -w net.ipv4.ip_forward=1 > /dev/null
if ! grep -q "^net.ipv4.ip_forward\s*=\s*1" /etc/sysctl.d/99-k8s-forward.conf 2>/dev/null; then
  cat > /etc/sysctl.d/99-k8s-forward.conf <<'EOF'
# Required: k8s master acts as NAT gateway for worker nodes
net.ipv4.ip_forward = 1
# Allow asymmetric routing replies (needed when .50 originates traffic via wlp2s0)
net.ipv4.conf.all.rp_filter = 2
net.ipv4.conf.wlp2s0.rp_filter = 2
net.ipv4.conf.eno1.rp_filter = 2
EOF
  sysctl -p /etc/sysctl.d/99-k8s-forward.conf > /dev/null
  ok "sysctl 99-k8s-forward.conf written and applied"
else
  ok "sysctl ip_forward already configured"
fi

# ── 3. Set rp_filter=2 (loose) live ───────────────────────────────────────────
log "Step 3 — Set rp_filter=2 (loose mode) on all interfaces..."
sysctl -w net.ipv4.conf.all.rp_filter=2 > /dev/null
sysctl -w net.ipv4.conf."${WAN_IF}".rp_filter=2 > /dev/null
sysctl -w net.ipv4.conf."${LAN_IF}".rp_filter=2 > /dev/null
ok "rp_filter set to loose (2) on all, ${WAN_IF}, ${LAN_IF}"

# ── 4. Add MASQUERADE rule on WAN interface ────────────────────────────────────
log "Step 4 — Add MASQUERADE on ${WAN_IF} for outbound internet traffic..."

# Remove any duplicate rules first (idempotent)
iptables -t nat -D POSTROUTING -o "${WAN_IF}" -j MASQUERADE 2>/dev/null || true
# Add fresh rule — matches ALL traffic going out WAN (both .50 itself and workers)
iptables -t nat -A POSTROUTING -o "${WAN_IF}" -j MASQUERADE
ok "MASQUERADE rule added: -o ${WAN_IF} -j MASQUERADE"

# ── 5. Allow FORWARD traffic between LAN and WAN ──────────────────────────────
log "Step 5 — Ensure FORWARD rules allow LAN↔WAN..."
# Allow established/related (return traffic from internet back to workers)
iptables -C FORWARD -i "${WAN_IF}" -o "${LAN_IF}" -m state --state RELATED,ESTABLISHED -j ACCEPT 2>/dev/null || \
  iptables -I FORWARD 1 -i "${WAN_IF}" -o "${LAN_IF}" -m state --state RELATED,ESTABLISHED -j ACCEPT
# Allow new outbound from LAN to WAN
iptables -C FORWARD -i "${LAN_IF}" -o "${WAN_IF}" -j ACCEPT 2>/dev/null || \
  iptables -I FORWARD 2 -i "${LAN_IF}" -o "${WAN_IF}" -j ACCEPT
ok "FORWARD rules set: ${LAN_IF}→${WAN_IF} ACCEPT, ${WAN_IF}→${LAN_IF} ESTABLISHED ACCEPT"

# ── 6. Persist iptables rules via NetworkManager dispatcher ────────────────────
log "Step 6 — Persist iptables rules across reboots..."

# Save rules file
iptables-save > /etc/iptables-gateway.rules
ok "Rules saved to /etc/iptables-gateway.rules"

# Create a systemd service to restore on boot
cat > /etc/systemd/system/iptables-gateway.service <<EOF
[Unit]
Description=Restore iptables gateway rules for k8s-platform
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=/sbin/iptables-restore /etc/iptables-gateway.rules
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable iptables-gateway.service
ok "iptables-gateway.service enabled (runs on boot after network is up)"

# ── 7. Fix the NM wlp2s0 default route metric (lower = preferred) ────────────
log "Step 7 — Set wlp2s0 metric to 100 (preferred WAN path)..."
nmcli connection modify twinkly36 ipv4.route-metric 100 2>/dev/null && ok "twinkly36 metric set to 100" || warn "Could not set metric on twinkly36 — set manually if needed"

# ── 8. Reload NM connections to apply changes ─────────────────────────────────
log "Step 8 — Reload NetworkManager connections..."
nmcli connection reload
# Re-up eno1 to flush the stray route
nmcli connection up eno1 2>/dev/null && ok "eno1 connection reloaded" || warn "eno1 reload failed (may already be up)"

# ── 9. Test connectivity ──────────────────────────────────────────────────────
log "Step 9 — Testing internet connectivity..."
sleep 2
if ping -c 2 -W 4 8.8.8.8 &>/dev/null; then
  ok "ping 8.8.8.8 — SUCCESS ✓"
else
  warn "ping 8.8.8.8 still failing — see troubleshooting below"
fi

if curl -sf --max-time 8 https://api.github.com/zen &>/dev/null; then
  ok "HTTPS to github.com — SUCCESS ✓"
  echo ""
  echo "╔══════════════════════════════════════════════════════════════╗"
  echo "║  Network gateway is working!                                  ║"
  echo "║  Run: bash scripts/git-sync-github.sh \"your message here\"    ║"
  echo "╚══════════════════════════════════════════════════════════════╝"
else
  warn "HTTPS to github.com still failing"
  echo ""
  echo "  Remaining troubleshooting steps:"
  echo "  a) Check upstream router firewall — does .254 block outbound 443?"
  echo "  b) Try SSH push instead of HTTPS — see docs/github-backup.md"
  echo "  c) Check: sudo iptables -t nat -L POSTROUTING -n -v"
fi

log "=== fix-gateway-routing.sh complete ==="
