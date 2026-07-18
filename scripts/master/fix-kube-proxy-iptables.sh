#!/usr/bin/env bash
# =============================================================================
# fix-kube-proxy-iptables.sh
#
# Fixes kube-proxy CrashLoopBackOff on RHEL 10 master node.
#
# Root cause: /proc/net/ip_tables_names exists but is EMPTY — no iptables
# tables registered in the kernel yet. kube-proxy reads this file at startup
# and concludes "No iptables support for IPv4" → crashes.
#
# Fix:
#   1. Insert a no-op iptables rule in the FORWARD chain → registers the
#      'filter' table → populates /proc/net/ip_tables_names
#   2. Ensure the rule survives reboots via a systemd unit
#   3. Delete the crashing kube-proxy pod on master → fresh start reads
#      the now-populated proc file → starts successfully
#   4. Verify calico-node on master is healthy (it was panicking on the
#      stale nftables kube-proxy table — check if still present)
#
# Usage: sudo bash scripts/master/fix-kube-proxy-iptables.sh
# Safe to re-run.
# =============================================================================
set -euo pipefail

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "  ✓ $*"; }
warn() { echo "  ⚠ $*"; }
fail() { echo "  ✗ $*"; exit 1; }

[[ $EUID -eq 0 ]] || fail "Run as root: sudo bash $0"

# ── 1. Remove stale kube-proxy nftables tables (Felix panic trigger) ──────────
log "Step 1: Removing stale kube-proxy nftables tables..."
nft delete table ip  kube-proxy 2>/dev/null && ok "deleted ip kube-proxy table"  || ok "ip kube-proxy table not present"
nft delete table ip6 kube-proxy 2>/dev/null && ok "deleted ip6 kube-proxy table" || ok "ip6 kube-proxy table not present"

REMAINING=$(nft list tables 2>/dev/null | grep kube-proxy || echo "none")
ok "Remaining kube-proxy nft tables: ${REMAINING}"

# ── 2. Load iptables kernel modules ──────────────────────────────────────────
log "Step 2: Loading iptables kernel modules..."
for mod in ip_tables iptable_filter iptable_nat iptable_mangle nf_nat nf_conntrack; do
    modprobe "${mod}" 2>/dev/null && ok "loaded ${mod}" || warn "modprobe ${mod} returned non-zero (may already be built-in)"
done

# Persist modules across reboots
cat > /etc/modules-load.d/iptables-kube.conf << 'EOF'
ip_tables
iptable_filter
iptable_nat
iptable_mangle
nf_nat
nf_conntrack
EOF
ok "module persistence written to /etc/modules-load.d/iptables-kube.conf"

# ── 3. Insert a no-op rule to register the filter table in the kernel ─────────
log "Step 3: Registering iptables filter table in kernel..."
# Use iptables-nft (which is what /usr/sbin/iptables points to on RHEL 10)
# -C checks for existing, -A appends only if missing
if ! iptables -t filter -C FORWARD -m comment --comment "kube-proxy-init" -j RETURN 2>/dev/null; then
    iptables -t filter -A FORWARD -m comment --comment "kube-proxy-init" -j RETURN
    ok "inserted FORWARD no-op rule"
else
    ok "FORWARD no-op rule already present"
fi

# Also register nat and mangle tables (kube-proxy uses all three)
if ! iptables -t nat -C POSTROUTING -m comment --comment "kube-proxy-init" -j RETURN 2>/dev/null; then
    iptables -t nat -A POSTROUTING -m comment --comment "kube-proxy-init" -j RETURN
    ok "inserted nat POSTROUTING no-op rule"
else
    ok "nat POSTROUTING no-op rule already present"
fi

if ! iptables -t mangle -C POSTROUTING -m comment --comment "kube-proxy-init" -j RETURN 2>/dev/null; then
    iptables -t mangle -A POSTROUTING -m comment --comment "kube-proxy-init" -j RETURN
    ok "inserted mangle POSTROUTING no-op rule"
else
    ok "mangle POSTROUTING no-op rule already present"
fi

# ── 4. Verify /proc/net/ip_tables_names is now populated ──────────────────────
log "Step 4: Verifying /proc/net/ip_tables_names..."
TABLES=$(cat /proc/net/ip_tables_names 2>/dev/null || echo "")
if [[ -z "$TABLES" ]]; then
    fail "/proc/net/ip_tables_names is still empty — iptables tables not registered"
fi
ok "/proc/net/ip_tables_names contains: $(echo $TABLES | tr '\n' ' ')"

# ── 5. Create systemd unit to persist the no-op rules across reboots ──────────
log "Step 5: Installing systemd unit for iptables init on boot..."
cat > /etc/systemd/system/kube-proxy-iptables-init.service << 'EOF'
[Unit]
Description=Register iptables tables for kube-proxy (RHEL 10 workaround)
Before=kubelet.service
After=network.target
DefaultDependencies=no

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/bin/bash -c '\
  modprobe ip_tables iptable_filter iptable_nat iptable_mangle nf_nat nf_conntrack 2>/dev/null || true; \
  iptables -t filter -C FORWARD -m comment --comment kube-proxy-init -j RETURN 2>/dev/null || \
    iptables -t filter -A FORWARD -m comment --comment kube-proxy-init -j RETURN; \
  iptables -t nat -C POSTROUTING -m comment --comment kube-proxy-init -j RETURN 2>/dev/null || \
    iptables -t nat -A POSTROUTING -m comment --comment kube-proxy-init -j RETURN; \
  iptables -t mangle -C POSTROUTING -m comment --comment kube-proxy-init -j RETURN 2>/dev/null || \
    iptables -t mangle -A POSTROUTING -m comment --comment kube-proxy-init -j RETURN'

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable kube-proxy-iptables-init.service
systemctl start  kube-proxy-iptables-init.service
ok "systemd unit kube-proxy-iptables-init.service enabled and started"

# ── 6. Verify kube-proxy configmap has mode: iptables ─────────────────────────
log "Step 6: Ensuring kube-proxy configmap mode is 'iptables'..."
CURRENT_MODE=$(kubectl get configmap kube-proxy -n kube-system -o json | \
    python3 -c "import json,sys; cm=json.load(sys.stdin); conf=cm['data']['config.conf']; \
    [print(l.strip().split(': ')[1].strip('\"')) for l in conf.split('\n') if l.strip().startswith('mode:')]" \
    2>/dev/null | head -1)
echo "  Current mode: '${CURRENT_MODE}'"

if [[ "$CURRENT_MODE" != "iptables" ]]; then
    warn "mode is '${CURRENT_MODE}', patching to 'iptables'..."
    kubectl get configmap kube-proxy -n kube-system -o json | \
        python3 -c "
import json, sys
cm = json.load(sys.stdin)
conf = cm['data']['config.conf']
import re
conf = re.sub(r'mode: \"[^\"]*\"', 'mode: \"iptables\"', conf)
cm['data']['config.conf'] = conf
cm['metadata'].get('annotations', {}).pop('kubeadm.kubernetes.io/component-config.hash', None)
print(json.dumps(cm))
" | kubectl apply -f -
    ok "configmap patched to iptables mode"
else
    ok "configmap already set to iptables mode"
fi

# ── 7. Restart master kube-proxy pod ─────────────────────────────────────────
log "Step 7: Restarting kube-proxy on master..."
MASTER_POD=$(kubectl get pods -n kube-system -o wide | grep kube-proxy | grep master.local | awk '{print $1}')
if [[ -n "$MASTER_POD" ]]; then
    kubectl delete pod -n kube-system "$MASTER_POD"
    ok "deleted pod $MASTER_POD"
else
    warn "no master kube-proxy pod found"
fi

# Wait for new pod
log "Waiting for new kube-proxy pod on master..."
for i in $(seq 1 30); do
    NEW_POD=$(kubectl get pods -n kube-system -o wide 2>/dev/null | grep kube-proxy | grep master.local | awk '{print $1}')
    if [[ -n "$NEW_POD" && "$NEW_POD" != "$MASTER_POD" ]]; then
        STATUS=$(kubectl get pod -n kube-system "$NEW_POD" -o jsonpath='{.status.phase}' 2>/dev/null)
        READY=$(kubectl get pod -n kube-system "$NEW_POD" -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null)
        echo "  Pod: $NEW_POD  Status: $STATUS  Ready: $READY"
        if [[ "$READY" == "true" ]]; then
            ok "kube-proxy on master is Running and Ready!"
            break
        fi
    fi
    sleep 3
done

NEW_POD=$(kubectl get pods -n kube-system -o wide | grep kube-proxy | grep master.local | awk '{print $1}')
log "kube-proxy pod logs:"
kubectl logs -n kube-system "$NEW_POD" --tail=10 2>/dev/null || echo "  (pod not ready yet)"

# ── 8. Restart calico-node on master ─────────────────────────────────────────
log "Step 8: Restarting calico-node on master (clears Felix panic state)..."
CALICO_POD=$(kubectl get pods -n kube-system -o wide | grep calico-node | grep master.local | awk '{print $1}')
if [[ -n "$CALICO_POD" ]]; then
    kubectl delete pod -n kube-system "$CALICO_POD"
    ok "deleted calico-node pod $CALICO_POD"
else
    warn "no master calico-node pod found"
fi

log "Waiting for calico-node on master to be Ready (up to 90s)..."
for i in $(seq 1 30); do
    NEW_CALICO=$(kubectl get pods -n kube-system -o wide 2>/dev/null | grep calico-node | grep master.local | awk '{print $1}')
    if [[ -n "$NEW_CALICO" && "$NEW_CALICO" != "$CALICO_POD" ]]; then
        READY=$(kubectl get pod -n kube-system "$NEW_CALICO" -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null)
        echo "  calico-node: $NEW_CALICO  Ready: $READY"
        if [[ "$READY" == "true" ]]; then
            ok "calico-node on master is Ready!"
            break
        fi
    fi
    sleep 3
done

# ── 9. Test OpenBao NodePort reachability ─────────────────────────────────────
log "Step 9: Testing OpenBao NodePort 30820..."
sleep 5
UNSEAL_KEYS_FILE="/home/star_master/openbao-init-keys.json"
if [[ -f "$UNSEAL_KEYS_FILE" ]]; then
    ROOT_TOKEN=$(python3 -c "import json; d=json.load(open('$UNSEAL_KEYS_FILE')); print(d['root_token'])")
    # Try NodePort
    RESULT=$(curl -sk --max-time 5 \
        -H "X-Vault-Token: ${ROOT_TOKEN}" \
        "http://192.168.1.50:30820/v1/sys/health" 2>/dev/null || echo "TIMEOUT")
    echo "  NodePort 30820 result: $RESULT"
    if echo "$RESULT" | grep -q '"initialized"'; then
        ok "OpenBao NodePort 30820 is REACHABLE! ✓"
    else
        warn "OpenBao NodePort 30820 not yet reachable (Calico chains may need more time)"
        echo "  Run: curl -sk http://192.168.1.50:30820/v1/sys/health"
    fi
else
    warn "No unseal keys file found at $UNSEAL_KEYS_FILE"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo " SUMMARY"
echo "============================================================"
kubectl get pods -n kube-system | grep -E "kube-proxy|calico-node"
echo "----"
kubectl get pods -n prod
echo ""
log "Done. If kube-proxy is still crashing, run:"
echo "  kubectl logs -n kube-system \$(kubectl get pods -n kube-system -o wide | grep kube-proxy | grep master | awk '{print \$1}')"
