#!/usr/bin/env bash
# =============================================================================
# recreate_snowflake_jdbc_catalog.sh
#
# Recreates the Doris snowflake_jdbc external catalog using credentials
# read entirely from OpenBao. No passwords are hardcoded.
#
# Run this after:
#   - Snowflake password rotation
#   - Doris FE pod restart (catalog persists, but re-run if needed)
#   - Changing the target Snowflake database / schema
#
# Usage:
#   ./recreate_snowflake_jdbc_catalog.sh [--db DATABASE] [--schema SCHEMA]
#
# Defaults:
#   --db     SNOWFLAKE_SAMPLE_DATA
#   --schema TPCDS_SF10TCL
# =============================================================================
set -euo pipefail

BAO_ADDR="${BAO_ADDR:-http://192.168.1.50:30820}"
NAMESPACE="${NAMESPACE:-prod}"
SF_DB="${SF_DB:-SNOWFLAKE_SAMPLE_DATA}"
SF_SCHEMA="${SF_SCHEMA:-TPCDS_SF10TCL}"

# Parse optional args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --db)     SF_DB="$2";     shift 2 ;;
    --schema) SF_SCHEMA="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

# ── 1. OpenBao token ─────────────────────────────────────────────────────────
echo "[catalog] Fetching credentials from OpenBao..."
BAO_TOKEN=$(kubectl get secret openbao-unseal-keys -n "${NAMESPACE}" \
  -o jsonpath='{.data.root-token}' | base64 -d)

_bao() {
  curl -sf --max-time 10 \
    -H "X-Vault-Token: ${BAO_TOKEN}" \
    "${BAO_ADDR}/v1/secret/data/${1}" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['${2}'])"
}

# ── 2. Read all needed values from OpenBao ───────────────────────────────────
DORIS_PWD=$(_bao "platform/doris"     "admin_password")
SF_USER=$(_bao   "platform/snowflake" "user")
SF_PWD=$(_bao    "platform/snowflake" "password")
SF_ACCT=$(_bao   "platform/snowflake" "account")
SF_WH=$(_bao     "platform/snowflake" "warehouse")
echo "[catalog] Credentials fetched: account=${SF_ACCT}  user=${SF_USER}  db=${SF_DB}  schema=${SF_SCHEMA}"

# ── 3. Build SQL ─────────────────────────────────────────────────────────────
SQL=$(cat <<ENDSQL
DROP CATALOG IF EXISTS snowflake_jdbc;

CREATE CATALOG snowflake_jdbc
COMMENT 'Snowflake ${SF_DB}.${SF_SCHEMA} via JDBC — credentials from OpenBao'
PROPERTIES (
  'type'                  = 'jdbc',
  'user'                  = '${SF_USER}',
  'password'              = '${SF_PWD}',
  'jdbc_url'              = 'jdbc:snowflake://${SF_ACCT}.snowflakecomputing.com/?warehouse=${SF_WH}&db=${SF_DB}&schema=${SF_SCHEMA}&authenticator=snowflake_jwt&private_key_file=/opt/apache-doris/fe/doris-meta/jdbc_drivers/sf_rsa_key_pkcs8.pem',
  'driver_url'            = 'http://doris-fe-0.doris-fe-headless.prod.svc.cluster.local:8888/snowflake-jdbc-3.15.1.jar',
  'driver_class'          = 'net.snowflake.client.jdbc.SnowflakeDriver',
  'test_connection'       = 'false',
  'lower_case_meta_names' = 'true'
);

SHOW DATABASES FROM snowflake_jdbc;
ENDSQL
)

# ── 4. Execute via kubectl exec ──────────────────────────────────────────────
echo "[catalog] Recreating snowflake_jdbc catalog in Doris..."
kubectl exec -n "${NAMESPACE}" doris-fe-0 -c doris-fe -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${DORIS_PWD}" \
  -e "${SQL}" 2>/dev/null

echo "[catalog] Done. snowflake_jdbc catalog is live."
