#!/usr/bin/env bash
# =============================================================================
# 15-seed-rbac-principals.sh
#
# Creates Kerberos principals for all RBAC users and service accounts,
# then exports keytabs into Kubernetes Secrets so each service can authenticate
# to the KDC without a password prompt.
#
# Run order: after 12-seed-openbao-secrets.sh and after KDC pod is Ready.
#
# Usage: sudo bash scripts/master/15-seed-rbac-principals.sh
# Safe to re-run — addprinc is skipped if principal already exists.
#
# User principals:
#   account_admin, caching_admin, caching_dev,
#   processing_admin, processing_dev,
#   streaming_admin, streaming_dev
#
# Service principals:
#   svc/doris, svc/spark, svc/sqlmesh, svc/kestra,
#   svc/kafka, svc/schema-registry, svc/debezium, svc/akhq
# =============================================================================
set -euo pipefail

NAMESPACE="prod"
KDC_DEPLOY="kerberos-kdc"
REALM="STARDATADBLABS.LOCAL"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "  ✓ $*"; }
die()  { echo "[ERROR] $*" >&2; exit 1; }

# ── Wait for KDC to be ready ──────────────────────────────────────────────────
log "Waiting for Kerberos KDC pod to be Ready..."
kubectl rollout status deployment/${KDC_DEPLOY} -n ${NAMESPACE} --timeout=120s
KDC_POD=$(kubectl get pod -n ${NAMESPACE} -l app=${KDC_DEPLOY} \
  -o jsonpath='{.items[0].metadata.name}')
[[ -n "${KDC_POD}" ]] || die "KDC pod not found in namespace ${NAMESPACE}"
log "KDC pod: ${KDC_POD}"

# ── Helper: create principal if missing ──────────────────────────────────────
addprinc_if_missing() {
  local principal="$1"
  local pw_flag="$2"   # "-randkey" or "-pw <password>"
  local exists
  exists=$(kubectl exec -n ${NAMESPACE} ${KDC_POD} -- \
    kadmin.local -q "getprinc ${principal}@${REALM}" 2>&1 || true)
  if echo "${exists}" | grep -q "Principal does not exist"; then
    # shellcheck disable=SC2086
    kubectl exec -n ${NAMESPACE} ${KDC_POD} -- \
      kadmin.local -q "addprinc ${pw_flag} ${principal}@${REALM}"
    ok "Created principal: ${principal}@${REALM}"
  else
    ok "Principal already exists: ${principal}@${REALM} (skipped)"
  fi
}

# ── Helper: export keytab and store as Kubernetes Secret ─────────────────────
export_keytab() {
  local principal="$1"      # e.g. svc/doris
  local secret_name="$2"    # e.g. doris-keytab
  local secret_ns="${3:-${NAMESPACE}}"
  local safe_name="${principal//\//-}"   # svc/doris → svc-doris

  local keytab_path="/tmp/${safe_name}.keytab"

  # Export from KDC
  kubectl exec -n ${NAMESPACE} ${KDC_POD} -- \
    kadmin.local -q "ktadd -k ${keytab_path} ${principal}@${REALM}" 2>/dev/null

  # Pull the keytab bytes out of the pod and create/update the K8s Secret
  kubectl exec -n ${NAMESPACE} ${KDC_POD} -- cat "${keytab_path}" \
    | kubectl create secret generic "${secret_name}" \
        --namespace="${secret_ns}" \
        --from-file="keytab=/dev/stdin" \
        --dry-run=client -o yaml \
    | kubectl apply -f -

  # Clean up temp file inside pod
  kubectl exec -n ${NAMESPACE} ${KDC_POD} -- rm -f "${keytab_path}"
  ok "Keytab secret: ${secret_name} in ns/${secret_ns}"
}

# ─── Read user passwords from rbac-users secret (seeded by 12-seed script) ───
log "Reading RBAC user passwords from Kubernetes secret rbac-users..."
get_pass() {
  kubectl get secret rbac-users -n ${NAMESPACE} \
    -o jsonpath="{.data.${1}}" 2>/dev/null | base64 -d
}

# ── 1. Service principals (randkey — auth via keytab only) ────────────────────
log "=== Creating service principals ==="
for svc in doris spark sqlmesh kestra kafka schema-registry debezium akhq opensearch polaris; do
  addprinc_if_missing "svc/${svc}" "-randkey"
done

# ── 2. Human user principals ──────────────────────────────────────────────────
log "=== Creating human user principals ==="

# account_admin group users
for user in platform_admin; do
  pw=$(get_pass "${user}-password" || echo "ChangeMe123!")
  addprinc_if_missing "${user}" "-pw ${pw}"
done

# caching layer users
for user in caching_admin_user caching_dev_user; do
  pw=$(get_pass "${user}-password" || echo "ChangeMe123!")
  addprinc_if_missing "${user}" "-pw ${pw}"
done

# processing layer users
for user in processing_admin_user processing_dev_user; do
  pw=$(get_pass "${user}-password" || echo "ChangeMe123!")
  addprinc_if_missing "${user}" "-pw ${pw}"
done

# streaming layer users
for user in streaming_admin_user streaming_dev_user; do
  pw=$(get_pass "${user}-password" || echo "ChangeMe123!")
  addprinc_if_missing "${user}" "-pw ${pw}"
done

# ── 3. Export service keytabs into Kubernetes Secrets ─────────────────────────
log "=== Exporting service keytabs ==="
export_keytab "svc/doris"           "doris-keytab"           "${NAMESPACE}"
export_keytab "svc/spark"           "spark-keytab"           "${NAMESPACE}"
export_keytab "svc/sqlmesh"         "sqlmesh-keytab"         "${NAMESPACE}"
export_keytab "svc/kestra"          "kestra-keytab"          "${NAMESPACE}"
export_keytab "svc/kafka"           "kafka-keytab"           "${NAMESPACE}"
export_keytab "svc/schema-registry" "schema-registry-keytab" "${NAMESPACE}"
export_keytab "svc/debezium"        "debezium-keytab"        "${NAMESPACE}"
export_keytab "svc/akhq"            "akhq-keytab"            "${NAMESPACE}"
export_keytab "svc/opensearch"      "opensearch-keytab"      "${NAMESPACE}"
export_keytab "svc/polaris"         "polaris-keytab"         "${NAMESPACE}"

# ── 4. Verify ──────────────────────────────────────────────────────────────────
log "=== Verifying principals ==="
kubectl exec -n ${NAMESPACE} ${KDC_POD} -- \
  kadmin.local -q "listprincs" | grep -E "svc/|_user@|_admin@" | sort

log ""
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "  Kerberos RBAC principals seeded successfully."
log "  Next step: run  bash scripts/master/17-verify-rbac.sh"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
