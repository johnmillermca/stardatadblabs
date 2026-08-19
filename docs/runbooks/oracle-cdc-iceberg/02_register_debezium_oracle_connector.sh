#!/usr/bin/env bash
# =============================================================================
# 02_register_debezium_oracle_connector.sh
#
# Registers the Debezium Oracle LogMiner CDC connector via the Kafka Connect
# REST API.
#
# CDC sync-point design
# ----------------------
# This script must run AFTER the Snowflake → Iceberg bulk copy has completed
# for all target tables.  The bulk-copy job writes a watermark row per table
# into the `pipeline` PostgreSQL database (pipeline_watermarks table) with:
#
#   sf_extraction_ts  — Snowflake CURRENT_TIMESTAMP() captured immediately
#                       before the first batch SELECT for that table.
#
# This script:
#   1. Reads sf_extraction_ts from pipeline_watermarks for each table.
#   2. Converts it to an Oracle SCN via TIMESTAMP_TO_SCN() in Oracle.
#   3. Stores oracle_start_scn back in pipeline_watermarks.
#   4. Registers the Debezium connector with:
#        snapshot.mode       = schema_only   (no full row snapshot — rows already in Iceberg)
#        snapshot.offset.scn = <min SCN>     (start streaming from this exact point)
#
# This guarantees ZERO gap and ZERO duplication between the bulk-copy data
# and the CDC stream.  See docs/runbooks/cdc-sync-point/DESIGN.md for the
# full architecture.
#
# User: dave (data_admin / iceberg_engineer role in RBAC plane)
# CDC source: Oracle XE 21c (prod ns, port 30521, XEPDB1)
# Kafka: SASL_PLAINTEXT SCRAM-SHA-512 (strimzi-kafka port 9092)
# Schema Registry: Confluent 7.9.0 (port 30810)
# Debezium Connect REST: http://192.168.1.54:30083
#
# Requires:
#   curl, jq, kubectl, psql, sqlplus  (run from any node with access to OpenBao)
#
# Usage:
#   export SPARK_USER=dave
#   bash 02_register_debezium_oracle_connector.sh
# =============================================================================
set -euo pipefail

DEBEZIUM_URL="http://192.168.1.54:30083"
BAO_ADDR="${BAO_ADDR:-http://192.168.1.50:30820}"
CONNECT_NAME="oracle-tpcds-cdc"

# Tables to CDC — must match those in pipeline_watermarks
CDC_TABLES=(
  income_band ship_mode warehouse reason call_center
  web_site web_page household_demographics catalog_page promotion
)

echo "=== Debezium Oracle CDC Registration ==="
echo "User      : ${SPARK_USER:-dave}"
echo "Connect   : $DEBEZIUM_URL"
echo "OpenBao   : $BAO_ADDR"
echo "Mode      : schema_only (bulk copy already completed)"
echo ""

# ── 1. Fetch OpenBao root token (bootstrap only — SA JWT used inside pods) ──
if [ -z "${BAO_TOKEN:-}" ]; then
  echo "[INFO] Fetching OpenBao token from K8s secret …"
  BAO_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
              -o jsonpath='{.data.root-token}' | base64 -d)
fi

# ── 2. Read Oracle credentials from OpenBao ──────────────────────────────────
echo "[INFO] Reading secret/platform/oracle …"
ORACLE_SECRET=$(curl -sf -H "X-Vault-Token: $BAO_TOKEN" \
  "$BAO_ADDR/v1/secret/data/platform/oracle" | jq -r '.data.data')
ORACLE_USER=$(echo "$ORACLE_SECRET" | jq -r '.user    // "tpcds"')
ORACLE_PASS=$(echo "$ORACLE_SECRET" | jq -r '.password')
ORACLE_HOST=$(echo "$ORACLE_SECRET" | jq -r '.host    // "oracle-xe.prod.svc.cluster.local"')
ORACLE_PORT=$(echo "$ORACLE_SECRET" | jq -r '.port    // "1521"')
ORACLE_SID=$(echo  "$ORACLE_SECRET" | jq -r '.sid     // "XEPDB1"')

# ── 3. Read Kafka SASL credentials from OpenBao ───────────────────────────────
echo "[INFO] Reading secret/platform/kafka …"
KAFKA_SECRET=$(curl -sf -H "X-Vault-Token: $BAO_TOKEN" \
  "$BAO_ADDR/v1/secret/data/platform/kafka" | jq -r '.data.data')
KAFKA_USER=$(echo "$KAFKA_SECRET" | jq -r '.debezium_user     // "debezium-user"')
KAFKA_PASS=$(echo "$KAFKA_SECRET" | jq -r '.debezium_password')
KAFKA_BOOTSTRAP="strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092"

# ── 4. Read pipeline DB credentials from OpenBao ─────────────────────────────
echo "[INFO] Reading secret/platform/pipeline_db …"
PG_SECRET=$(curl -sf -H "X-Vault-Token: $BAO_TOKEN" \
  "$BAO_ADDR/v1/secret/data/platform/pipeline_db" | jq -r '.data.data')
PG_HOST=$(echo "$PG_SECRET" | jq -r '.host')
PG_PORT=$(echo "$PG_SECRET" | jq -r '.port     // "5432"')
PG_DB=$(echo   "$PG_SECRET" | jq -r '.database // "pipeline"')
PG_USER=$(echo "$PG_SECRET" | jq -r '.user     // "pipeline"')
PG_PASS=$(echo "$PG_SECRET" | jq -r '.password')

# ── 5. Verify all tables have watermarks ─────────────────────────────────────
echo "[INFO] Checking pipeline_watermarks for all tables …"
MISSING=()
for tbl in "${CDC_TABLES[@]}"; do
  TS=$(PGPASSWORD="$PG_PASS" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" \
       -d "$PG_DB" -At \
       -c "SELECT sf_extraction_ts FROM pipeline_watermarks
           WHERE source_db='SNOWFLAKE_SAMPLE_DATA'
             AND source_schema='TPCDS_SF10TCL'
             AND table_name='${tbl}'" 2>/dev/null || true)
  if [ -z "$TS" ]; then
    MISSING+=("$tbl")
    echo "  [WARN] No watermark found for table: $tbl"
  else
    echo "  [OK]   $tbl → sf_extraction_ts=$TS"
  fi
done

if [ ${#MISSING[@]} -gt 0 ]; then
  echo ""
  echo "[ERROR] The following tables have no watermark in pipeline_watermarks:"
  printf '  - %s\n' "${MISSING[@]}"
  echo "[ERROR] Run the Snowflake → Iceberg bulk copy FIRST, then re-run this script."
  exit 1
fi

# ── 6. Resolve sf_extraction_ts → Oracle SCN for each table ──────────────────
echo ""
echo "[INFO] Resolving Oracle SCNs via TIMESTAMP_TO_SCN() …"

# Oracle NodePort accessible from this node
ORACLE_NODEPORT_HOST="192.168.1.50"
ORACLE_NODEPORT_PORT="30521"

# Find the minimum SCN across all tables — Debezium starts from the earliest
# point so no table misses any change.
MIN_SCN=0

for tbl in "${CDC_TABLES[@]}"; do
  SF_TS=$(PGPASSWORD="$PG_PASS" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" \
          -d "$PG_DB" -At \
          -c "SELECT sf_extraction_ts FROM pipeline_watermarks
              WHERE source_db='SNOWFLAKE_SAMPLE_DATA'
                AND source_schema='TPCDS_SF10TCL'
                AND table_name='${tbl}'")

  # Convert ISO-8601 to Oracle-friendly format: "2026-08-18 04:01:39.123456"
  ORA_TS="${SF_TS/T/ }"
  ORA_TS="${ORA_TS/Z/}"

  SCN=$(sqlplus -s "${ORACLE_USER}/${ORACLE_PASS}@${ORACLE_NODEPORT_HOST}:${ORACLE_NODEPORT_PORT}/${ORACLE_SID}" <<SQLEOF 2>/dev/null | tr -d ' \r\n'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 VERIFY OFF
SELECT TIMESTAMP_TO_SCN(
    TO_TIMESTAMP('${ORA_TS}', 'YYYY-MM-DD HH24:MI:SS.FF6')
    AT TIME ZONE 'UTC'
) FROM DUAL;
EXIT;
SQLEOF
)

  if [ -z "$SCN" ] || ! [[ "$SCN" =~ ^[0-9]+$ ]]; then
    echo "  [WARN] Could not resolve SCN for $tbl (sf_ts=$SF_TS) — ORA-08181 likely (timestamp too old)."
    echo "         Ensure you run this script within ${ORACLE_UNDO_RETENTION_MINUTES:-15} min of the bulk copy."
    echo "         Falling back to current SCN for $tbl."
    SCN=$(sqlplus -s "${ORACLE_USER}/${ORACLE_PASS}@${ORACLE_NODEPORT_HOST}:${ORACLE_NODEPORT_PORT}/${ORACLE_SID}" <<SQLEOF2 2>/dev/null | tr -d ' \r\n'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 VERIFY OFF
SELECT CURRENT_SCN FROM V\$DATABASE;
EXIT;
SQLEOF2
)
  fi

  echo "  [SCN]  $tbl → $SCN  (sf_ts=$SF_TS)"

  # Store resolved SCN back into pipeline_watermarks
  PGPASSWORD="$PG_PASS" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -q \
    -c "UPDATE pipeline_watermarks
        SET oracle_start_scn=$SCN, updated_at=NOW()
        WHERE source_db='SNOWFLAKE_SAMPLE_DATA'
          AND source_schema='TPCDS_SF10TCL'
          AND table_name='${tbl}'"

  # Track minimum SCN across all tables (Debezium uses one SCN for all)
  if [ "$MIN_SCN" -eq 0 ] || [ "$SCN" -lt "$MIN_SCN" ]; then
    MIN_SCN="$SCN"
  fi
done

echo ""
echo "[INFO] Minimum SCN across all tables: $MIN_SCN"
echo "[INFO] Debezium will start streaming from SCN $MIN_SCN"

# ── 7. Schema Registry URL ───────────────────────────────────────────────────
SR_URL="http://schema-registry.prod.svc.cluster.local:8081"

# ── 8. Build and POST the connector config ────────────────────────────────────
echo ""
echo "[INFO] Registering connector: $CONNECT_NAME …"

curl -sf -X POST "$DEBEZIUM_URL/connectors" \
  -H "Content-Type: application/json" \
  -d "$(cat <<CONNECTOR_JSON
{
  "name": "${CONNECT_NAME}",
  "config": {
    "connector.class": "io.debezium.connector.oracle.OracleConnector",
    "tasks.max": "1",

    "database.hostname":        "${ORACLE_HOST}",
    "database.port":            "${ORACLE_PORT}",
    "database.user":            "${ORACLE_USER}",
    "database.password":        "${ORACLE_PASS}",
    "database.dbname":          "${ORACLE_SID}",
    "database.pdb.name":        "${ORACLE_SID}",
    "database.server.name":     "oracle-tpcds",

    "table.include.list": "TPCDS.INCOME_BAND,TPCDS.SHIP_MODE,TPCDS.WAREHOUSE,TPCDS.REASON,TPCDS.CALL_CENTER,TPCDS.WEB_SITE,TPCDS.WEB_PAGE,TPCDS.HOUSEHOLD_DEMOGRAPHICS,TPCDS.CATALOG_PAGE,TPCDS.PROMOTION",

    "database.history.kafka.bootstrap.servers": "${KAFKA_BOOTSTRAP}",
    "database.history.kafka.topic":             "schema-changes.oracle-tpcds",
    "database.history.consumer.security.protocol":     "SASL_PLAINTEXT",
    "database.history.consumer.sasl.mechanism":        "SCRAM-SHA-512",
    "database.history.consumer.sasl.jaas.config":      "org.apache.kafka.common.security.scram.ScramLoginModule required username=\\"${KAFKA_USER}\\" password=\\"${KAFKA_PASS}\\";",
    "database.history.producer.security.protocol":     "SASL_PLAINTEXT",
    "database.history.producer.sasl.mechanism":        "SCRAM-SHA-512",
    "database.history.producer.sasl.jaas.config":      "org.apache.kafka.common.security.scram.ScramLoginModule required username=\\"${KAFKA_USER}\\" password=\\"${KAFKA_PASS}\\";",

    "log.mining.strategy":        "online_catalog",
    "log.mining.continuous.mine": "true",

    "key.converter":                          "io.confluent.kafka.serializers.KafkaAvroSerializer",
    "key.converter.schema.registry.url":      "${SR_URL}",
    "value.converter":                        "io.confluent.kafka.serializers.KafkaAvroSerializer",
    "value.converter.schema.registry.url":    "${SR_URL}",

    "decimal.handling.mode": "double",
    "time.precision.mode":   "connect",
    "tombstones.on.delete":  "false",

    "topic.prefix": "oracle-tpcds",

    "producer.security.protocol":     "SASL_PLAINTEXT",
    "producer.sasl.mechanism":        "SCRAM-SHA-512",
    "producer.sasl.jaas.config":      "org.apache.kafka.common.security.scram.ScramLoginModule required username=\\"${KAFKA_USER}\\" password=\\"${KAFKA_PASS}\\";",

    "heartbeat.interval.ms": "5000",

    "snapshot.mode":        "schema_only",
    "snapshot.offset.scn":  "${MIN_SCN}",
    "snapshot.locking.mode": "none"
  }
}
CONNECTOR_JSON
)"

echo ""
echo "[INFO] Connector registration complete."
echo "[INFO] Debezium will stream Oracle changes from SCN $MIN_SCN onwards."
echo "[INFO] Checking status …"
sleep 3
curl -sf "$DEBEZIUM_URL/connectors/$CONNECT_NAME/status" | jq .

echo ""
echo "[INFO] Current watermark summary:"
PGPASSWORD="$PG_PASS" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
  -c "SELECT table_name, sf_extraction_ts, oracle_start_scn, rows_copied, updated_at
      FROM pipeline_watermarks
      WHERE source_db='SNOWFLAKE_SAMPLE_DATA' AND source_schema='TPCDS_SF10TCL'
      ORDER BY table_name;"
