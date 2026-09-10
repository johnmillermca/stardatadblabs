#!/usr/bin/env bash
# =============================================================================
# 04_grant_polaris_catalog_content.sh
#
# Grant CATALOG_MANAGE_CONTENT to the catalog_admin role in every Polaris
# catalog that Doris reads.  Without this privilege Doris can discover catalog
# metadata but fails when it tries to access table data, returning:
#
#   errCode = 2, detailMessage = Failed to check view exist,
#   error message is: Error occurred while processing HEAD request
#
# This script is idempotent — Polaris returns HTTP 200 if the grant already
# exists, HTTP 201 when it is freshly applied.  It is safe to run on every
# re-setup.
#
# Prerequisites:
#   - POLARIS_IP must resolve to the polaris-rest ClusterIP or NodePort.
#   - Polaris management credentials (spark_svc_id / spark_svc_secret)
#     must be available.  The script reads them from OpenBao automatically
#     if BAO_TOKEN is set, otherwise expects POLARIS_ID / POLARIS_SECRET
#     to be exported by the caller.
#
# Usage (from cluster master):
#   BAO_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
#     -o jsonpath='{.data.root-token}' | base64 -d)
#   bash manifests/doris/setup/04_grant_polaris_catalog_content.sh
#
# Catalogs patched:
#   IcebergCatalog  (polaris)    — already had the grant; idempotent
#   star_lakehouse  (databricks)
#   pg_lakehouse    (postgres)
#   ora_lakehouse   (oracle)
#   mgo_lakehouse   (mongodb)
# =============================================================================

set -euo pipefail

# ── Resolve Polaris ClusterIP ─────────────────────────────────────────────────
POLARIS_IP=$(kubectl get svc polaris-rest -n prod -o jsonpath='{.spec.clusterIP}')
POLARIS_BASE="http://${POLARIS_IP}:8181"

# ── Load credentials ──────────────────────────────────────────────────────────
if [[ -z "${POLARIS_ID:-}" || -z "${POLARIS_SECRET:-}" ]]; then
  if [[ -z "${BAO_TOKEN:-}" ]]; then
    BAO_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
      -o jsonpath='{.data.root-token}' | base64 -d)
  fi
  BAO_BASE="http://192.168.1.50:30820"
  POLARIS_ID=$(curl -s -H "X-Vault-Token: ${BAO_TOKEN}" \
    "${BAO_BASE}/v1/secret/data/platform/polaris" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['data']['spark_svc_id'])")
  POLARIS_SECRET=$(curl -s -H "X-Vault-Token: ${BAO_TOKEN}" \
    "${BAO_BASE}/v1/secret/data/platform/polaris" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['data']['spark_svc_secret'])")
fi

# ── Obtain management token ───────────────────────────────────────────────────
TOKEN=$(curl -s -X POST "${POLARIS_BASE}/api/catalog/v1/oauth/tokens" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=${POLARIS_ID}&client_secret=${POLARIS_SECRET}&scope=PRINCIPAL_ROLE:ALL" \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])")

echo "Obtained Polaris management token."

# ── Grant CATALOG_MANAGE_CONTENT to catalog_admin in each catalog ─────────────
#
# All 5 are listed here so the script is fully idempotent on a fresh cluster.
# Polaris returns:
#   201 — grant was newly applied
#   200 — grant already existed (idempotent no-op)
#   4xx — error (printed with body for diagnosis)
#
CATALOGS=(
  IcebergCatalog
  star_lakehouse
  pg_lakehouse
  ora_lakehouse
  mgo_lakehouse
)

GRANT_PAYLOAD='{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}'

ALL_OK=true
for CAT in "${CATALOGS[@]}"; do
  HTTP=$(curl -s -o /tmp/_polaris_grant_resp.json -w "%{http_code}" \
    -X PUT \
    "${POLARIS_BASE}/api/management/v1/catalogs/${CAT}/catalog-roles/catalog_admin/grants" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "${GRANT_PAYLOAD}")

  BODY=$(cat /tmp/_polaris_grant_resp.json)

  if [[ "$HTTP" == "201" ]]; then
    echo "  [GRANTED]    ${CAT}/catalog_admin ← CATALOG_MANAGE_CONTENT"
  elif [[ "$HTTP" == "200" ]]; then
    echo "  [ALREADY OK] ${CAT}/catalog_admin — CATALOG_MANAGE_CONTENT already present"
  else
    echo "  [ERROR]      ${CAT}/catalog_admin — HTTP ${HTTP}: ${BODY}" >&2
    ALL_OK=false
  fi
done

# ── Verify ────────────────────────────────────────────────────────────────────
echo ""
echo "Verifying grants..."
for CAT in "${CATALOGS[@]}"; do
  HAS=$(curl -s \
    "${POLARIS_BASE}/api/management/v1/catalogs/${CAT}/catalog-roles/catalog_admin/grants" \
    -H "Authorization: Bearer ${TOKEN}" \
    | python3 -c "
import json, sys
grants = json.load(sys.stdin).get('grants', [])
privs = [g['privilege'] for g in grants]
print('OK' if 'CATALOG_MANAGE_CONTENT' in privs else 'MISSING')
")
  echo "  ${CAT}: CATALOG_MANAGE_CONTENT = ${HAS}"
done

if [[ "$ALL_OK" == "true" ]]; then
  echo ""
  echo "All grants applied successfully."
else
  echo ""
  echo "One or more grants failed — review errors above." >&2
  exit 1
fi
