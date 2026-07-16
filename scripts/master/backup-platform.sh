#!/usr/bin/env bash
# =============================================================================
# backup-platform.sh
# Full backup of the k8s-platform: etcd, Helm values, ArgoCD apps,
# manifests, OpenBao keys, Kubernetes secrets, and PVC data.
#
# Usage: sudo bash scripts/master/backup-platform.sh
# Output: /opt/k8s-backups/platform-backup-<timestamp>.tar.gz
# Retention: keeps last 7 archives
# =============================================================================
set -euo pipefail
export PATH="/usr/local/bin:${PATH}"

TIMESTAMP=$(date '+%Y%m%d-%H%M%S')
BACKUP_ROOT="/opt/k8s-backups"
WORK_DIR="${BACKUP_ROOT}/${TIMESTAMP}"
ARCHIVE="${BACKUP_ROOT}/platform-backup-${TIMESTAMP}.tar.gz"
LOG="${BACKUP_ROOT}/backup.log"
REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

log()  { echo "[$(date '+%H:%M:%S')] $*" | tee -a "${LOG}"; }
warn() { echo "[WARN]  $*" | tee -a "${LOG}"; }
die()  { echo "[ERROR] $*" | tee -a "${LOG}" >&2; exit 1; }

[[ $EUID -eq 0 ]] || die "Run as root: sudo bash $0"

mkdir -p "${WORK_DIR}"
log "=== Platform Backup Started: ${TIMESTAMP} ==="
log "Working directory: ${WORK_DIR}"

# ── 1. etcd snapshot ──────────────────────────────────────────────────────────
log "Step 1: etcd snapshot..."
ETCD_SNAPSHOT="${WORK_DIR}/etcd-snapshot.db"
ETCD_CERTS="/etc/kubernetes/pki/etcd"

if command -v etcdctl &>/dev/null; then
  ETCDCTL_API=3 etcdctl snapshot save "${ETCD_SNAPSHOT}" \
    --endpoints=https://127.0.0.1:2379 \
    --cacert="${ETCD_CERTS}/ca.crt" \
    --cert="${ETCD_CERTS}/server.crt" \
    --key="${ETCD_CERTS}/server.key" \
    && log "  etcd snapshot: OK ($(du -sh "${ETCD_SNAPSHOT}" | cut -f1))" \
    || warn "  etcd snapshot failed — continuing"
else
  # Fallback: exec into etcd pod
  ETCD_POD=$(kubectl get pod -n kube-system -l component=etcd -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [[ -n "${ETCD_POD}" ]]; then
    kubectl exec -n kube-system "${ETCD_POD}" -- \
      etcdctl snapshot save /tmp/etcd-snapshot.db \
      --endpoints=https://127.0.0.1:2379 \
      --cacert=/etc/kubernetes/pki/etcd/ca.crt \
      --cert=/etc/kubernetes/pki/etcd/server.crt \
      --key=/etc/kubernetes/pki/etcd/server.key
    kubectl cp "kube-system/${ETCD_POD}:/tmp/etcd-snapshot.db" "${ETCD_SNAPSHOT}"
    log "  etcd snapshot via pod exec: OK"
  else
    warn "  etcd snapshot skipped — etcdctl not found and no etcd pod"
  fi
fi

# ── 2. Helm values ────────────────────────────────────────────────────────────
log "Step 2: Helm values..."
cp -r "${REPO_DIR}/helm" "${WORK_DIR}/helm"
log "  helm values: OK"

# ── 3. ArgoCD apps ────────────────────────────────────────────────────────────
log "Step 3: ArgoCD apps..."
cp -r "${REPO_DIR}/argocd-apps" "${WORK_DIR}/argocd-apps"
# Also export live Application objects from cluster
mkdir -p "${WORK_DIR}/argocd-live"
kubectl get applications -n argocd -o yaml > "${WORK_DIR}/argocd-live/applications.yaml" 2>/dev/null || warn "  Could not export live ArgoCD apps"
log "  ArgoCD apps: OK"

# ── 4. Manifests ─────────────────────────────────────────────────────────────
log "Step 4: Manifests..."
cp -r "${REPO_DIR}/manifests" "${WORK_DIR}/manifests"
cp -r "${REPO_DIR}/docker"    "${WORK_DIR}/docker" 2>/dev/null || true
log "  manifests: OK"

# ── 5. OpenBao init keys ──────────────────────────────────────────────────────
log "Step 5: OpenBao keys..."
if [[ -f /root/openbao-init-keys.json ]]; then
  cp /root/openbao-init-keys.json "${WORK_DIR}/openbao-init-keys.json"
  chmod 600 "${WORK_DIR}/openbao-init-keys.json"
  log "  OpenBao keys: OK"
else
  warn "  /root/openbao-init-keys.json not found — skipped"
fi

# ── 6. All Kubernetes secrets ─────────────────────────────────────────────────
log "Step 6: Kubernetes secrets dump..."
kubectl get secret -A -o yaml > "${WORK_DIR}/all-secrets.yaml"
chmod 600 "${WORK_DIR}/all-secrets.yaml"
log "  K8s secrets: OK"

# ── 7. PVC data (local-path volumes) ─────────────────────────────────────────
log "Step 7: PVC data from local-path volumes..."
LOCAL_PATH_DIR=$(cat /etc/k8s-storage-path 2>/dev/null || echo "/opt/local-path-provisioner")
PVC_BACKUP_DIR="${WORK_DIR}/pvc-data"
mkdir -p "${PVC_BACKUP_DIR}"

for ns in prod monitoring; do
  for pvc in $(kubectl get pvc -n "${ns}" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    # Find the local PV directory
    PV_NAME=$(kubectl get pvc "${pvc}" -n "${ns}" -o jsonpath='{.spec.volumeName}' 2>/dev/null || true)
    PV_PATH=$(find "${LOCAL_PATH_DIR}" -maxdepth 2 -name "${PV_NAME}*" -type d 2>/dev/null | head -1)
    if [[ -n "${PV_PATH}" && -d "${PV_PATH}" ]]; then
      TAR_NAME="${ns}-${pvc}.tar.gz"
      tar -czf "${PVC_BACKUP_DIR}/${TAR_NAME}" -C "$(dirname "${PV_PATH}")" "$(basename "${PV_PATH}")" 2>/dev/null \
        && log "  PVC ${ns}/${pvc}: OK (${PV_PATH})" \
        || warn "  PVC ${ns}/${pvc}: tar failed"
    else
      warn "  PVC ${ns}/${pvc}: PV path not found under ${LOCAL_PATH_DIR}"
    fi
  done
done

# ── 8. Git state ──────────────────────────────────────────────────────────────
log "Step 8: Git state..."
{
  echo "=== git remote ==="
  git -C "${REPO_DIR}" remote -v 2>/dev/null || true
  echo "=== git branch ==="
  git -C "${REPO_DIR}" branch --show-current 2>/dev/null || true
  echo "=== git log (last 20) ==="
  git -C "${REPO_DIR}" log --oneline -20 2>/dev/null || true
  echo "=== git status ==="
  git -C "${REPO_DIR}" status 2>/dev/null || true
} > "${WORK_DIR}/git-state.txt"
log "  git state: OK"

# ── Compress archive ─────────────────────────────────────────────────────────
log "Compressing archive..."
tar -czf "${ARCHIVE}" -C "${BACKUP_ROOT}" "${TIMESTAMP}"
rm -rf "${WORK_DIR}"
ARCHIVE_SIZE=$(du -sh "${ARCHIVE}" | cut -f1)
log "Archive: ${ARCHIVE} (${ARCHIVE_SIZE})"

# ── Retention: keep last 7 archives ──────────────────────────────────────────
log "Applying retention policy (keep last 7)..."
ls -t "${BACKUP_ROOT}"/platform-backup-*.tar.gz 2>/dev/null | tail -n +8 | while read -r old; do
  rm -f "${old}"
  log "  Removed old archive: ${old}"
done

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
printf "║  Backup:  %-50s║\n" "${ARCHIVE}"
printf "║  Size:    %-50s║\n" "${ARCHIVE_SIZE}"
printf "║  Log:     %-50s║\n" "${LOG}"
echo "╚══════════════════════════════════════════════════════════════╝"
log "=== Backup Complete ==="
