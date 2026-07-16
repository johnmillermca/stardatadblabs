#!/usr/bin/env bash
# =============================================================================
# fix-masquerade.sh
# Configures persistent internet masquerade on master so worker nodes
# (192.168.1.51-54) can reach the internet through master (192.168.1.50).
#
# Strategy:
#   - Discover which firewalld zone wlp2s0 is actually in at runtime
#     (NM controls this; it may be 'public' or 'external' depending on profile)
#   - Enable masquerade on that zone
#   - Create/update InternalToExternal forwarding policy to use that zone
#   - Everything via --permanent + reload so it survives reboots
#
#   eno1  (192.168.1.50)  = internal LAN / Kubernetes interface → 'internal' zone
#   wlp2s0 (192.168.1.132) = WiFi uplink to internet            → runtime zone
#
# Safe to run multiple times.
# =============================================================================
set -euo pipefail

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
die()  { echo "[ERROR] $*" >&2; exit 1; }
ok()   { echo "  ✓ $*"; }
fail() { echo "  ✗ $*"; }

[[ $EUID -eq 0 ]] || die "Run as root: sudo bash $0"

INTERNAL_IF="eno1"
EXTERNAL_IF="wlp2s0"
LAN_CIDR="192.168.1.0/24"

# ── 1. Ensure ip_forward is on ────────────────────────────────────────────────
log "Enabling ip_forward..."
sysctl -w net.ipv4.ip_forward=1 >/dev/null
grep -q "^net.ipv4.ip_forward" /etc/sysctl.d/99-ip-forward.conf 2>/dev/null \
  || echo "net.ipv4.ip_forward = 1" > /etc/sysctl.d/99-ip-forward.conf

# ── 2. Discover which zone wlp2s0 is actually in right now ───────────────────
log "Discovering active zone for ${EXTERNAL_IF}..."
EXT_ZONE=$(firewall-cmd --get-zone-of-interface="${EXTERNAL_IF}" 2>/dev/null || true)
if [[ -z "${EXT_ZONE}" ]]; then
  # Interface not in any zone yet — assign to external
  EXT_ZONE="external"
  firewall-cmd --permanent --zone="${EXT_ZONE}" --add-interface="${EXTERNAL_IF}" 2>/dev/null || true
  nmcli con mod "${EXTERNAL_IF}" connection.zone "${EXT_ZONE}" 2>/dev/null || true
fi
log "  ${EXTERNAL_IF} is in zone: ${EXT_ZONE}"

# Also ensure eno1 is in internal zone
INT_ZONE=$(firewall-cmd --get-zone-of-interface="${INTERNAL_IF}" 2>/dev/null || echo "internal")
log "  ${INTERNAL_IF} is in zone: ${INT_ZONE}"
if [[ "${INT_ZONE}" != "internal" ]]; then
  firewall-cmd --permanent --zone=internal --add-interface="${INTERNAL_IF}" 2>/dev/null || true
  nmcli con mod "${INTERNAL_IF}" connection.zone internal 2>/dev/null || true
  INT_ZONE="internal"
fi

# ── 3. Enable masquerade on the external interface's zone (persistent) ────────
log "Enabling masquerade on '${EXT_ZONE}' zone..."
firewall-cmd --permanent --zone="${EXT_ZONE}" --add-masquerade
# Also enable runtime immediately (no reload needed for this part)
firewall-cmd --zone="${EXT_ZONE}" --add-masquerade 2>/dev/null || true

# ── 4. Remove stale policies targeting the wrong zone, recreate correctly ─────
log "Configuring forwarding policies ${INT_ZONE} → ${EXT_ZONE}..."

# Remove both directions if they exist (they may have wrong egress zone)
for pol in InternalToExternal ExternalToInternal; do
  if firewall-cmd --get-policies 2>/dev/null | grep -qw "${pol}"; then
    firewall-cmd --permanent --delete-policy="${pol}" 2>/dev/null || true
  fi
done

# InternalToExternal: LAN → internet
firewall-cmd --permanent --new-policy=InternalToExternal
firewall-cmd --permanent --policy=InternalToExternal --set-target=ACCEPT
firewall-cmd --permanent --policy=InternalToExternal --add-ingress-zone="${INT_ZONE}"
firewall-cmd --permanent --policy=InternalToExternal --add-egress-zone="${EXT_ZONE}"

# ExternalToInternal: return traffic internet → LAN
firewall-cmd --permanent --new-policy=ExternalToInternal
firewall-cmd --permanent --policy=ExternalToInternal --set-target=ACCEPT
firewall-cmd --permanent --policy=ExternalToInternal --add-ingress-zone="${EXT_ZONE}"
firewall-cmd --permanent --policy=ExternalToInternal --add-egress-zone="${INT_ZONE}"

# ── 5. Kubernetes CIDRs in trusted zone ───────────────────────────────────────
log "Ensuring pod/service CIDRs are trusted..."
firewall-cmd --permanent --zone=trusted --add-source=10.244.0.0/16  2>/dev/null || true
firewall-cmd --permanent --zone=trusted --add-source=10.96.0.0/12   2>/dev/null || true
firewall-cmd --permanent --zone=trusted --add-source="${LAN_CIDR}"  2>/dev/null || true
firewall-cmd --permanent --zone=internal --add-protocol=ipip        2>/dev/null || true
firewall-cmd --permanent --zone=trusted  --add-protocol=ipip        2>/dev/null || true

# ── 6. Reload to apply all permanent changes ──────────────────────────────────
log "Reloading firewalld..."
firewall-cmd --reload

# ── 7. Force NM to re-apply zone for wlp2s0 (wlp2s0 may drift back to public) ─
log "Re-applying NetworkManager zone for ${EXTERNAL_IF}..."
CONN_NAME=$(nmcli -t -f NAME,DEVICE con show --active 2>/dev/null \
  | grep ":${EXTERNAL_IF}$" | cut -d: -f1 | head -1)
if [[ -n "${CONN_NAME}" ]]; then
  log "  NM connection: '${CONN_NAME}'"
  # Set zone on the NM connection profile permanently
  nmcli con mod "${CONN_NAME}" connection.zone "${EXT_ZONE}"
  # Bounce the connection so NM tells firewalld the new zone immediately
  nmcli con up "${CONN_NAME}" 2>/dev/null || true
  sleep 2
else
  log "  WARNING: could not find active NM connection for ${EXTERNAL_IF}"
fi

# ── 8. Update NM dispatcher to use firewalld-reload (not raw nft) ─────────────
log "Updating NetworkManager dispatcher script..."
cat > /etc/NetworkManager/dispatcher.d/99-masquerade.sh << 'DISPATCHER'
#!/bin/bash
# Re-apply firewalld zone and masquerade when wlp2s0 comes up.
# Uses firewalld exclusively (raw nft rules are wiped by firewalld on reload).
if [ "$1" = "wlp2s0" ] && [ "$2" = "up" ]; then
  EXT_ZONE=$(firewall-cmd --get-zone-of-interface="wlp2s0" 2>/dev/null || echo "public")
  firewall-cmd --zone="${EXT_ZONE}" --add-masquerade 2>/dev/null || true
  firewall-cmd --reload 2>/dev/null || true
fi
DISPATCHER
chmod 755 /etc/NetworkManager/dispatcher.d/99-masquerade.sh

# ── 9. Final verification ─────────────────────────────────────────────────────
log "Verifying..."
ACTUAL_EXT_ZONE=$(firewall-cmd --get-zone-of-interface="${EXTERNAL_IF}" 2>/dev/null || echo "UNKNOWN")
MASQ=$(firewall-cmd --zone="${ACTUAL_EXT_ZONE}" --query-masquerade 2>/dev/null || echo "no")
echo ""
echo "  ip_forward         : $(cat /proc/sys/net/ipv4/ip_forward)"
echo "  ${EXTERNAL_IF} zone      : ${ACTUAL_EXT_ZONE}"
echo "  masquerade (${ACTUAL_EXT_ZONE}) : ${MASQ}"
echo "  active zones       :"
firewall-cmd --get-active-zones 2>/dev/null | sed 's/^/    /'
echo "  policies           : $(firewall-cmd --get-policies 2>/dev/null)"
echo ""

if ping -c2 -W3 -I "${INTERNAL_IF}" 8.8.8.8 &>/dev/null; then
  ok "Connectivity test (${INTERNAL_IF} → 8.8.8.8): PASS"
else
  fail "Connectivity test (${INTERNAL_IF} → 8.8.8.8): FAIL"
  log "  Dumping active firewalld config for ${ACTUAL_EXT_ZONE} zone:"
  firewall-cmd --zone="${ACTUAL_EXT_ZONE}" --list-all 2>/dev/null | sed 's/^/    /'
fi

log "Masquerade configuration complete."
