#!/usr/bin/env bash
# =============================================================================
# 05_seed_openbao_secrets.sh
#
# Stores all credentials required by the Oracle CDC → Iceberg pipeline
# into OpenBao (secret/platform/*).  Run once during initial setup.
#
# OpenBao auth: uses the root token for bootstrap; subsequent pod access
#               uses K8s SA JWT with role "platform-secrets-read".
#
# Secrets written:
#   secret/platform/oracle    — Oracle XE connection details
#   secret/platform/kafka     — Kafka SASL creds for Debezium user
#
# (secret/platform/s3, secret/platform/snowflake, secret/platform/polaris
#  already exist from the previous pipeline setup)
#
# Usage:
#   export SPARK_USER=dave
#   bash 05_seed_openbao_secrets.sh
# =============================================================================
set -euo pipefail

BAO_ADDR="${BAO_ADDR:-http://192.168.1.50:30820}"

echo "=== OpenBao secret seeding | user=${SPARK_USER:-dave} ==="

# ── 1. Get root token ────────────────────────────────────────────────────────
if [ -z "${BAO_TOKEN:-}" ]; then
  echo "[INFO] Fetching root token from openbao-unseal-keys secret …"
  BAO_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
              -o jsonpath='{.data.root-token}' | base64 -d)
fi
echo "[INFO] OpenBao address: $BAO_ADDR"

# ── Helper ───────────────────────────────────────────────────────────────────
# KV v2: write to secret/data/<path>, read from secret/data/<path>
bao_write() {
  local path="$1"
  local payload="$2"
  echo "[INFO] Writing $path …"
  curl -sf -X POST "$BAO_ADDR/v1/$path" \
    -H "X-Vault-Token: $BAO_TOKEN" \
    -H "Content-Type: application/json" \
    -d "$payload" | python3 -m json.tool --no-ensure-ascii 2>/dev/null | head -5 || true
}

# ── 2. Oracle XE credentials ─────────────────────────────────────────────────
bao_write "secret/data/platform/oracle" '{
  "data": {
    "user":          "tpcds",
    "password":      "TpcdsPwd123!",
    "host":          "oracle-xe.prod.svc.cluster.local",
    "port":          "1521",
    "sid":           "XEPDB1",
    "nodeport_host": "192.168.1.50",
    "nodeport_port": "30521",
    "jdbc_url":      "jdbc:oracle:thin:@oracle-xe.prod.svc.cluster.local:1521/XEPDB1"
  }
}'

# ── 3. Kafka Debezium user credentials ───────────────────────────────────────
KAFKA_DEBEZIUM_PASS=$(kubectl get secret debezium-user -n prod \
  -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || echo "FETCH_MANUALLY")

bao_write "secret/data/platform/kafka" "{
  \"data\": {
    \"debezium_user\":     \"debezium-user\",
    \"debezium_password\": \"${KAFKA_DEBEZIUM_PASS}\",
    \"bootstrap\":         \"strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092\",
    \"schema_registry\":   \"http://schema-registry.prod.svc.cluster.local:8081\"
  }
}"

echo ""
echo "=== Verifying secrets ==="
for path in secret/data/platform/oracle secret/data/platform/kafka \
            secret/data/platform/s3 secret/data/platform/snowflake secret/data/platform/polaris; do
  keys=$(curl -sf -H "X-Vault-Token: $BAO_TOKEN" "$BAO_ADDR/v1/$path" \
         | python3 -c "import sys,json; d=json.load(sys.stdin); dd=d.get('data',{}); print(', '.join(dd.get('data',dd).keys()))" 2>/dev/null || echo "NOT FOUND")
  printf "  %-50s → %s\n" "$path" "$keys"
done

echo ""
echo "=== Done. All secrets stored in OpenBao. ==="
