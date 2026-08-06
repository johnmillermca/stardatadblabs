#!/usr/bin/env bash
# =============================================================================
# 17-verify-rbac.sh
#
# Verifies the RBAC setup:
#   1. Kerberos: all principals exist
#   2. Keytab secrets exist
#
# Usage: sudo bash scripts/master/17-verify-rbac.sh
# =============================================================================
set -euo pipefail

NAMESPACE="prod"
REALM="STARDATADBLABS.LOCAL"
PASS=0
FAIL=0

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
pass() { echo "  [PASS] $*"; PASS=$((PASS+1)); }
fail() { echo "  [FAIL] $*"; FAIL=$((FAIL+1)); }

# ─────────────────────────────────────────────────────────────────────────────
# 1. Kerberos principals
# ─────────────────────────────────────────────────────────────────────────────
log "=== Checking Kerberos Principals ==="
KDC_POD=$(kubectl get pod -n ${NAMESPACE} -l app=kerberos-kdc \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

if [[ -z "${KDC_POD}" ]]; then
  fail "KDC pod not found — skipping Kerberos checks"
else
  for principal in \
      "svc/doris" "svc/spark" "svc/sqlmesh" "svc/kestra" \
      "svc/kafka" "svc/schema-registry" "svc/debezium" "svc/akhq" "svc/opensearch" \
      "platform_admin" \
      "caching_admin_user" "caching_dev_user" \
      "processing_admin_user" "processing_dev_user" \
      "streaming_admin_user" "streaming_dev_user"; do
    result=$(kubectl exec -n ${NAMESPACE} ${KDC_POD} -- \
      kadmin.local -q "getprinc ${principal}@${REALM}" 2>&1 || true)
    if echo "${result}" | grep -q "Principal does not exist"; then
      fail "Kerberos principal missing: ${principal}@${REALM}"
    else
      pass "Kerberos principal exists: ${principal}@${REALM}"
    fi
  done
fi

# ─────────────────────────────────────────────────────────────────────────────
# 2. Keytab secrets exist
# ─────────────────────────────────────────────────────────────────────────────
log "=== Checking Keytab Secrets ==="
for secret in doris-keytab spark-keytab sqlmesh-keytab kestra-keytab \
              kafka-keytab schema-registry-keytab debezium-keytab akhq-keytab \
              opensearch-keytab polaris-keytab; do
  if kubectl get secret "${secret}" -n ${NAMESPACE} -o name \
      > /dev/null 2>&1; then
    pass "Keytab secret exists: ${secret}"
  else
    fail "Keytab secret MISSING: ${secret} — run 15-seed-rbac-principals.sh"
  fi
done

# ─────────────────────────────────────────────────────────────────────────────
# 3. Doris access check — caching_dev can connect
# ─────────────────────────────────────────────────────────────────────────────
log "=== Checking Doris Access (functional) ==="
DEV_PASS_RAW=$(kubectl get secret rbac-users -n ${NAMESPACE} \
  -o jsonpath='{.data.caching_dev_user-password}' | base64 -d 2>/dev/null | tr -d '\n\r' || echo "")

if [[ -z "${DEV_PASS_RAW}" ]]; then
  fail "Cannot read caching_dev_user password from rbac-users secret — skipping Doris check"
else
  FE_POD=$(kubectl get pod -n ${NAMESPACE} -l app=doris-fe \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
  if [[ -z "${FE_POD}" ]]; then
    fail "Doris FE pod not found — skipping Doris functional check"
  else
    DORIS_ADMIN_PASS=$(kubectl get secret doris-credentials -n ${NAMESPACE} \
      -o jsonpath='{.data.admin-password}' | base64 -d 2>/dev/null | tr -d '\n\r' || echo "")

    kubectl exec -n ${NAMESPACE} ${FE_POD} -- \
      mysql -h 127.0.0.1 -P 9030 -u root -p"${DORIS_ADMIN_PASS}" \
        -e "CREATE USER IF NOT EXISTS 'caching_dev_user'@'%' IDENTIFIED BY '${DEV_PASS_RAW}';" \
        > /dev/null 2>&1 || true

    SELECT_RESULT=$(kubectl exec -n ${NAMESPACE} ${FE_POD} -- \
      mysql -h 127.0.0.1 -P 9030 -u caching_dev_user -p"${DEV_PASS_RAW}" \
        -e "SHOW DATABASES;" 2>&1 || true)
    if echo "${SELECT_RESULT}" | grep -qi "error 1045\|Access denied"; then
      fail "Doris: caching_dev_user cannot connect — check user creation or password"
    else
      pass "Doris: caching_dev_user can connect and list databases"
    fi
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────
echo ""
TOTAL=$((PASS+FAIL))
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "  Results: ${PASS}/${TOTAL} passed  |  ${FAIL} failed"
if [[ ${FAIL} -eq 0 ]]; then
  log "  ✓ All RBAC checks passed."
else
  log "  ✗ Some checks failed. Review the [FAIL] lines above."
fi
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
[[ ${FAIL} -eq 0 ]]
