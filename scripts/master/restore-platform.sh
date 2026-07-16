#!/usr/bin/env bash
# =============================================================================
# restore-platform.sh
# Restores a k8s-platform backup created by backup-platform.sh
#
# Usage: sudo bash scripts/master/restore-platform.sh <backup-archive.tar.gz>
#
# ⚠️  WARNING: etcd restore is DESTRUCTIVE. It will stop kubelet and replace
#    the etcd data directory. A confirmation prompt is shown before proceeding.
# =============================================================================
set -euo pipefail

ARCHIVE="${1:-}"
RESTORE_TMP="/tmp/k8s-restore"
REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

log()    { echo "[$(date '+%H:%M:%S')] $*"; }
warn()   { echo "[WARN]  $*"; }
die()    { echo "[ERROR] $*" >&2; exit 1; }
confirm(){ read -r -p "$1 [type 'yes-restore-etcd' to confirm]: " ans; [[ "${ans}" == "yes-restore-etcd" ]]; }

[[ $EUID -eq 0 ]]         || die "Run as root: sudo bash $0 <archive>"
[[ -n "${ARCHIVE}" ]]     || die "Usage: sudo bash $0 <platform-backup-TIMESTAMP.tar.gz>"
[[ -f "${ARCHIVE}" ]]     || die "Archive not found: ${ARCHIVE}"

log "=== Platform Restore ==="
log "Archive: ${ARCHIVE}"

# ── Extract archive ───────────────────────────────────────────────────────────
log "Extracting archive..."
rm -rf "${RESTORE_TMP}"
mkdir -p "${RESTORE_TMP}"
tar -xzf "${ARCHIVE}" -C "${RESTORE_TMP}" --strip-components=1
log "  Extracted to ${RESTORE_TMP}"

# ── 1. Helm values + manifests + ArgoCD apps ─────────────────────────────────
log "Step 1: Restoring Helm values, manifests, ArgoCD apps..."
[[ -d "${RESTORE_TMP}/helm"       ]] && cp -r "${RESTORE_TMP}/helm/"       "${REPO_DIR}/helm/"
[[ -d "${RESTORE_TMP}/manifests"  ]] && cp -r "${RESTORE_TMP}/manifests/"  "${REPO_DIR}/manifests/"
[[ -d "${RESTORE_TMP}/argocd-apps" ]] && cp -r "${RESTORE_TMP}/argocd-apps/" "${REPO_DIR}/argocd-apps/"
log "  Files restored to repo."

# ── 2. etcd restore (interactive — destructive) ───────────────────────────────
if [[ -f "${RESTORE_TMP}/etcd-snapshot.db" ]]; then
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  ⚠️  ETCD RESTORE — THIS WILL:"
  echo "     1. Stop kubelet"
  echo "     2. Move /var/lib/etcd to /var/lib/etcd.bak-$(date +%s)"
  echo "     3. Restore etcd from snapshot"
  echo "     4. Restart kubelet"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  if confirm "Proceed with etcd restore?"; then
    log "Step 2: Restoring etcd..."
    systemctl stop kubelet
    mv /var/lib/etcd "/var/lib/etcd.bak-$(date +%s)" || true
    ETCDCTL_API=3 etcdctl snapshot restore "${RESTORE_TMP}/etcd-snapshot.db" \
      --data-dir=/var/lib/etcd \
      --cacert=/etc/kubernetes/pki/etcd/ca.crt \
      --cert=/etc/kubernetes/pki/etcd/server.crt \
      --key=/etc/kubernetes/pki/etcd/server.key
    systemctl start kubelet
    log "  etcd restore: OK — waiting 30s for cluster to stabilise..."
    sleep 30
  else
    warn "  etcd restore skipped by user."
  fi
else
  warn "  No etcd snapshot found in archive — skipping etcd restore."
fi

# ── 3. OpenBao keys ───────────────────────────────────────────────────────────
log "Step 3: Restoring OpenBao keys..."
if [[ -f "${RESTORE_TMP}/openbao-init-keys.json" ]]; then
  cp "${RESTORE_TMP}/openbao-init-keys.json" /root/openbao-init-keys.json
  chmod 600 /root/openbao-init-keys.json
  log "  OpenBao keys restored to /root/openbao-init-keys.json"
else
  warn "  No OpenBao keys in archive."
fi

# ── 4. Re-seed OpenBao secrets + K8s Secrets ─────────────────────────────────
log "Step 4: Re-seeding OpenBao secrets..."
if [[ -f /root/openbao-init-keys.json ]]; then
  bash "${REPO_DIR}/scripts/master/12-seed-openbao-secrets.sh" && log "  Secrets reseeded: OK" \
    || warn "  Secret re-seeding failed — check OpenBao status"
else
  warn "  OpenBao keys missing — cannot re-seed secrets. Restore them manually."
fi

# ── 5. Restore K8s secrets dump (belt-and-suspenders) ────────────────────────
log "Step 5: Restoring raw K8s secrets dump..."
if [[ -f "${RESTORE_TMP}/all-secrets.yaml" ]]; then
  kubectl apply -f "${RESTORE_TMP}/all-secrets.yaml" --force 2>/dev/null || \
    warn "  Some secrets could not be applied (may conflict with newly seeded ones)"
  log "  K8s secrets applied."
fi

# ── Cleanup ───────────────────────────────────────────────────────────────────
rm -rf "${RESTORE_TMP}"

# ── Post-restore checklist ────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║           Restore Complete — Verify these steps:            ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  1.  kubectl get nodes                                       ║"
echo "║  2.  kubectl get pods -A | grep -v Running                   ║"
echo "║  3.  kubectl get applications -n argocd                      ║"
echo "║  4.  Unseal OpenBao if sealed:                               ║"
echo "║      kubectl exec -n openbao openbao-0 -- bao status        ║"
echo "║  5.  Re-push ArgoCD apps if needed:                          ║"
echo "║      kubectl apply -f argocd-apps/                           ║"
echo "║  6.  git-push if repo files were updated                     ║"
echo "╚══════════════════════════════════════════════════════════════╝"
