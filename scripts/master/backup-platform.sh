#!/usr/bin/env bash
# =============================================================================
# backup-platform.sh
# Full backup of the k8s-platform — uploads to S3 when complete.
#
# What is backed up
# -----------------
#   1.  etcd snapshot            — full cluster state
#   2.  Kubernetes PKI certs     — /etc/kubernetes/pki
#   3.  Kubernetes secrets       — all namespaces, raw YAML
#   4.  PostgreSQL pg_dump       — repository databases only:
#                                    polaris, kestra, sqlmesh_state, rbac, metadata
#                                  (cache_testing, pipeline, oracle-data skipped)
#   5.  Kerberos KDC             — principal DB + keytabs (/var/kerberos/krb5kdc)
#   6.  OpenBao init keys        — /root/openbao-init-keys.json
#   7.  OpenBao PVC              — data-openbao-0 local-path volume
#   8.  Private registry PVC     — registry-data local-path volume
#   9.  Helm values              — helm/ directory
#  10.  ArgoCD apps              — argocd-apps/ + live Application objects
#  11.  Manifests + scripts      — manifests/ docker/ scripts/
#  12.  Git state                — remote, branch, log, status
#
# S3 destination
# --------------
#   s3://xdatatoiceberg1/k8s-backups/platform-backup-<TIMESTAMP>.tar.gz
#   Retention: last 30 days of S3 objects (30 files)
#   Local copy in /opt/k8s-backups/ — keeps last 3 archives (disk safety)
#
# Usage
# -----
#   sudo bash scripts/master/backup-platform.sh
#
# Run automatically via:
#   kubectl apply -f manifests/backup/platform-backup-cronjob.yaml
#
# Configuration (env overrides)
# ------------------------------
#   S3_BUCKET      default: xdatatoiceberg1
#   S3_PREFIX      default: k8s-backups
#   S3_REGION      default: us-east-2
#   LOCAL_KEEP     default: 3  (local archives to retain)
#   S3_KEEP        default: 30 (S3 archives to retain)
# =============================================================================
set -euo pipefail
export PATH="/usr/local/bin:/usr/bin:/bin:${PATH}"

# ── Configuration ─────────────────────────────────────────────────────────────
TIMESTAMP=$(date '+%Y%m%d-%H%M%S')
BACKUP_ROOT="/opt/k8s-backups"
WORK_DIR="${BACKUP_ROOT}/${TIMESTAMP}"
ARCHIVE="${BACKUP_ROOT}/platform-backup-${TIMESTAMP}.tar.gz"
LOG="${BACKUP_ROOT}/backup.log"
REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

S3_BUCKET="${S3_BUCKET:-xdatatoiceberg1}"
S3_PREFIX="${S3_PREFIX:-k8s-backups}"
S3_REGION="${S3_REGION:-us-east-2}"
S3_KEY="${S3_PREFIX}/platform-backup-${TIMESTAMP}.tar.gz"
LOCAL_KEEP="${LOCAL_KEEP:-3}"
S3_KEEP="${S3_KEEP:-30}"

# ── S3 credentials — read from K8s secret (preferred) or env ─────────────────
if [[ -z "${AWS_ACCESS_KEY_ID:-}" ]]; then
  AWS_ACCESS_KEY_ID=$(kubectl get secret platform-s3-credentials -n prod \
    -o jsonpath='{.data.access_key}' 2>/dev/null | base64 -d || true)
fi
if [[ -z "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
  AWS_SECRET_ACCESS_KEY=$(kubectl get secret platform-s3-credentials -n prod \
    -o jsonpath='{.data.secret_key}' 2>/dev/null | base64 -d || true)
fi
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION="${S3_REGION}"

# ── PostgreSQL repository databases (skip data/testing DBs) ──────────────────
PG_REPO_DBS=(polaris kestra sqlmesh_state rbac metadata)

# ── Local-path provisioner base directory ─────────────────────────────────────
LOCAL_PATH_DIR="/home/local-path-provisioner"

# ── Helpers ───────────────────────────────────────────────────────────────────
log()  { echo "[$(date '+%H:%M:%S')] $*" | tee -a "${LOG}"; }
warn() { echo "[$(date '+%H:%M:%S')] [WARN]  $*" | tee -a "${LOG}"; }
die()  { echo "[$(date '+%H:%M:%S')] [ERROR] $*" | tee -a "${LOG}" >&2; exit 1; }

[[ $EUID -eq 0 ]] || die "Run as root: sudo bash $0"

mkdir -p "${WORK_DIR}"
log "=== Platform Backup Started: ${TIMESTAMP} ==="
log "Working directory : ${WORK_DIR}"
log "S3 destination    : s3://${S3_BUCKET}/${S3_KEY}"

# ─────────────────────────────────────────────────────────────────────────────
# 1. etcd snapshot
# ─────────────────────────────────────────────────────────────────────────────
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
  ETCD_POD=$(kubectl get pod -n kube-system -l component=etcd \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [[ -n "${ETCD_POD}" ]]; then
    kubectl exec -n kube-system "${ETCD_POD}" -- \
      etcdctl snapshot save /tmp/etcd-snapshot.db \
      --endpoints=https://127.0.0.1:2379 \
      --cacert=/etc/kubernetes/pki/etcd/ca.crt \
      --cert=/etc/kubernetes/pki/etcd/server.crt \
      --key=/etc/kubernetes/pki/etcd/server.key
    kubectl cp "kube-system/${ETCD_POD}:/tmp/etcd-snapshot.db" "${ETCD_SNAPSHOT}"
    log "  etcd snapshot via pod exec: OK ($(du -sh "${ETCD_SNAPSHOT}" | cut -f1))"
  else
    warn "  etcd snapshot skipped — etcdctl not found and no etcd pod"
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# 2. Kubernetes PKI certificates
# ─────────────────────────────────────────────────────────────────────────────
log "Step 2: Kubernetes PKI certs..."
PKI_DIR="${WORK_DIR}/kubernetes-pki"
mkdir -p "${PKI_DIR}"
cp -r /etc/kubernetes/pki "${PKI_DIR}/"
chmod -R 600 "${PKI_DIR}"
log "  PKI certs: OK ($(du -sh "${PKI_DIR}" | cut -f1))"

# ─────────────────────────────────────────────────────────────────────────────
# 3. All Kubernetes secrets
# ─────────────────────────────────────────────────────────────────────────────
log "Step 3: Kubernetes secrets dump (all namespaces)..."
kubectl get secret -A -o yaml > "${WORK_DIR}/all-secrets.yaml"
chmod 600 "${WORK_DIR}/all-secrets.yaml"
log "  K8s secrets: OK"

# ─────────────────────────────────────────────────────────────────────────────
# 4. PostgreSQL — repository databases only
#    Skipped: cache_testing, pipeline (data DBs — too large, re-creatable)
#             oracle XE data is excluded per design
# ─────────────────────────────────────────────────────────────────────────────
log "Step 4: PostgreSQL repository databases..."
PG_BACKUP_DIR="${WORK_DIR}/postgres"
mkdir -p "${PG_BACKUP_DIR}"

PG_POD=$(kubectl get pod -n prod -l app=postgresql \
  --no-headers -o custom-columns="NAME:.metadata.name" | head -1 2>/dev/null || true)
PG_PASS=$(kubectl get secret postgresql-credentials -n prod \
  -o jsonpath='{.data.postgres-password}' 2>/dev/null | base64 -d || true)

if [[ -n "${PG_POD}" && -n "${PG_PASS}" ]]; then
  for db in "${PG_REPO_DBS[@]}"; do
    DUMP_FILE="${PG_BACKUP_DIR}/${db}.sql.gz"
    kubectl exec -n prod "${PG_POD}" -- \
      env PGPASSWORD="${PG_PASS}" \
      pg_dump -U postgres -d "${db}" --no-password -F p \
      2>/dev/null | gzip > "${DUMP_FILE}" \
      && log "  pg_dump ${db}: OK ($(du -sh "${DUMP_FILE}" | cut -f1))" \
      || warn "  pg_dump ${db}: failed — skipped"
  done
else
  warn "  PostgreSQL pod or password not found — skipping pg_dump"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 5. Kerberos KDC — principal database + keytabs
# ─────────────────────────────────────────────────────────────────────────────
log "Step 5: Kerberos KDC database..."
KRB_BACKUP_DIR="${WORK_DIR}/kerberos"
mkdir -p "${KRB_BACKUP_DIR}"

KRB_POD=$(kubectl get pod -n prod -l app=kerberos-kdc \
  --no-headers -o custom-columns="NAME:.metadata.name" | head -1 2>/dev/null || true)

if [[ -n "${KRB_POD}" ]]; then
  # Dump the principal database to a portable text format
  kubectl exec -n prod "${KRB_POD}" -- \
    bash -c "kdb5_util dump /tmp/kerberos-dump.txt && cat /tmp/kerberos-dump.txt" \
    2>/dev/null > "${KRB_BACKUP_DIR}/principal-dump.txt" \
    && log "  Kerberos principal dump: OK" \
    || warn "  Kerberos principal dump failed"

  # Copy raw KDC files — stream via tar so ownership is preserved correctly
  # even when running as non-root (files are root-owned inside the pod)
  kubectl exec -n prod "${KRB_POD}" -- \
    tar -cf - -C /var/kerberos/krb5kdc \
      principal principal.kadm5 .k5.STARDATADBLABS.LOCAL \
      kadm5.keytab kadm5.acl kdc.conf 2>/dev/null \
    | tar -xf - -C "${KRB_BACKUP_DIR}" 2>/dev/null \
    && log "  Kerberos raw files: OK" \
    || warn "  Kerberos raw files: partial copy"

  # krb5.conf from the pod
  kubectl exec -n prod "${KRB_POD}" -- cat /etc/krb5.conf \
    > "${KRB_BACKUP_DIR}/krb5.conf" 2>/dev/null \
    && log "  Kerberos krb5.conf: OK" \
    || warn "  Kerberos krb5.conf: not found — skipped"

  chmod -R 600 "${KRB_BACKUP_DIR}" 2>/dev/null || true
else
  warn "  Kerberos pod not found — skipping KDC backup"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 6. OpenBao init keys
# ─────────────────────────────────────────────────────────────────────────────
log "Step 6: OpenBao init keys..."
if [[ -f /root/openbao-init-keys.json ]]; then
  cp /root/openbao-init-keys.json "${WORK_DIR}/openbao-init-keys.json"
  chmod 600 "${WORK_DIR}/openbao-init-keys.json"
  log "  OpenBao init keys: OK"
else
  # Fallback: pull from K8s secret
  kubectl get secret openbao-unseal-keys -n prod -o json 2>/dev/null | \
    python3 -c "
import sys, json, base64
d = json.load(sys.stdin)['data']
keys = {k: base64.b64decode(v).decode() for k, v in d.items()}
print(json.dumps(keys, indent=2))
" > "${WORK_DIR}/openbao-init-keys.json" 2>/dev/null \
    && chmod 600 "${WORK_DIR}/openbao-init-keys.json" \
    && log "  OpenBao init keys (from K8s secret): OK" \
    || warn "  OpenBao init keys: not found — skipped"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 7. OpenBao PVC (data-openbao-0)
# ─────────────────────────────────────────────────────────────────────────────
log "Step 7: OpenBao PVC data..."
PVC_BACKUP_DIR="${WORK_DIR}/pvc-data"
mkdir -p "${PVC_BACKUP_DIR}"

backup_pvc() {
  local ns="$1" pvc="$2"
  local pv_name pv_path tar_file
  pv_name=$(kubectl get pvc "${pvc}" -n "${ns}" -o jsonpath='{.spec.volumeName}' 2>/dev/null || true)
  pv_path=$(find "${LOCAL_PATH_DIR}" -maxdepth 2 -name "${pv_name}*" -type d 2>/dev/null | head -1)
  if [[ -n "${pv_path}" && -d "${pv_path}" ]]; then
    tar_file="${PVC_BACKUP_DIR}/${ns}-${pvc}.tar.gz"
    tar -czf "${tar_file}" -C "$(dirname "${pv_path}")" "$(basename "${pv_path}")" 2>/dev/null \
      && log "  PVC ${ns}/${pvc}: OK ($(du -sh "${tar_file}" | cut -f1))" \
      || warn "  PVC ${ns}/${pvc}: tar failed"
  else
    warn "  PVC ${ns}/${pvc}: PV path not found under ${LOCAL_PATH_DIR}"
  fi
}

backup_pvc prod data-openbao-0

# ─────────────────────────────────────────────────────────────────────────────
# 8. Private container registry PVC
# ─────────────────────────────────────────────────────────────────────────────
log "Step 8: Private registry PVC data..."
backup_pvc registry registry-data

# ─────────────────────────────────────────────────────────────────────────────
# 9. Helm values
# ─────────────────────────────────────────────────────────────────────────────
log "Step 9: Helm values..."
cp -r "${REPO_DIR}/helm" "${WORK_DIR}/helm"
log "  helm: OK"

# ─────────────────────────────────────────────────────────────────────────────
# 10. ArgoCD apps
# ─────────────────────────────────────────────────────────────────────────────
log "Step 10: ArgoCD apps..."
cp -r "${REPO_DIR}/argocd-apps" "${WORK_DIR}/argocd-apps"
mkdir -p "${WORK_DIR}/argocd-live"
kubectl get applications -n argocd -o yaml \
  > "${WORK_DIR}/argocd-live/applications.yaml" 2>/dev/null \
  || warn "  Could not export live ArgoCD apps"
log "  ArgoCD apps: OK"

# ─────────────────────────────────────────────────────────────────────────────
# 11. Manifests + docker + scripts
# ─────────────────────────────────────────────────────────────────────────────
log "Step 11: Manifests, docker, scripts..."
cp -r "${REPO_DIR}/manifests" "${WORK_DIR}/manifests"
cp -r "${REPO_DIR}/scripts"   "${WORK_DIR}/scripts"
[[ -d "${REPO_DIR}/docker" ]] && cp -r "${REPO_DIR}/docker" "${WORK_DIR}/docker" || true
[[ -d "${REPO_DIR}/rbac-plane" ]] && cp -r "${REPO_DIR}/rbac-plane" "${WORK_DIR}/rbac-plane" || true
log "  manifests/scripts: OK"

# ─────────────────────────────────────────────────────────────────────────────
# 12. Git state
# ─────────────────────────────────────────────────────────────────────────────
log "Step 12: Git state..."
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

# ─────────────────────────────────────────────────────────────────────────────
# Compress archive
# ─────────────────────────────────────────────────────────────────────────────
log "Compressing archive..."
# --ignore-failed-read: tolerate root-owned files that are unreadable in
# unprivileged test runs. When running as root (production) all files are
# readable and nothing is skipped.
tar -czf "${ARCHIVE}" -C "${BACKUP_ROOT}" "${TIMESTAMP}" \
  --ignore-failed-read 2>/dev/null || true
[[ -s "${ARCHIVE}" ]] || die "Archive is empty or missing: ${ARCHIVE}"
rm -rf "${WORK_DIR}" 2>/dev/null || true
ARCHIVE_SIZE=$(du -sh "${ARCHIVE}" | cut -f1)
log "Archive: ${ARCHIVE} (${ARCHIVE_SIZE})"

# ─────────────────────────────────────────────────────────────────────────────
# Upload to S3
# ─────────────────────────────────────────────────────────────────────────────
log "Uploading to s3://${S3_BUCKET}/${S3_KEY} ..."
if [[ -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
  aws s3 cp "${ARCHIVE}" "s3://${S3_BUCKET}/${S3_KEY}" \
    --region "${S3_REGION}" \
    --storage-class STANDARD_IA \
    && log "  S3 upload: OK — s3://${S3_BUCKET}/${S3_KEY}" \
    || warn "  S3 upload failed — local archive kept"

  # S3 retention: delete objects older than S3_KEEP entries in the prefix
  log "  Applying S3 retention (keep last ${S3_KEEP})..."
  aws s3 ls "s3://${S3_BUCKET}/${S3_PREFIX}/" \
    --region "${S3_REGION}" 2>/dev/null \
    | awk '{print $4}' | sort \
    | head -n "-${S3_KEEP}" \
    | while read -r old_key; do
        [[ -n "${old_key}" ]] || continue
        aws s3 rm "s3://${S3_BUCKET}/${S3_PREFIX}/${old_key}" --region "${S3_REGION}" \
          && log "    Removed old S3 object: ${old_key}" || true
      done
else
  warn "  AWS credentials not found — S3 upload skipped. Local archive kept."
fi

# ─────────────────────────────────────────────────────────────────────────────
# Local retention: keep last LOCAL_KEEP archives
# ─────────────────────────────────────────────────────────────────────────────
log "Applying local retention (keep last ${LOCAL_KEEP})..."
ls -t "${BACKUP_ROOT}"/platform-backup-*.tar.gz 2>/dev/null \
  | tail -n "+$((LOCAL_KEEP + 1))" \
  | while read -r old; do
      rm -f "${old}"
      log "  Removed local archive: ${old}"
    done

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
printf "║  Archive : %-54s║\n" "${ARCHIVE}"
printf "║  Size    : %-54s║\n" "${ARCHIVE_SIZE}"
printf "║  S3      : s3://%-49s║\n" "${S3_BUCKET}/${S3_KEY}"
printf "║  Log     : %-54s║\n" "${LOG}"
echo "╚══════════════════════════════════════════════════════════════════╝"
log "=== Backup Complete ==="
