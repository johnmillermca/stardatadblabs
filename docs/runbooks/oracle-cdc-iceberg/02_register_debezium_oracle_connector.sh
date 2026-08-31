#!/usr/bin/env bash
# =============================================================================
# 02_register_debezium_oracle_connector.sh
#
# Registers the Debezium Oracle LogMiner CDC connector via the Kafka Connect
# REST API.
#
# ── CDC sync-point ────────────────────────────────────────────────────────────
# This script must run AFTER the Snowflake → Iceberg bulk copy has completed.
# The bulk-copy job writes sf_extraction_ts per table into the `pipeline`
# PostgreSQL DB.  This script converts that timestamp to an Oracle SCN via
# TIMESTAMP_TO_SCN() and starts Debezium from that exact point.
# See docs/runbooks/cdc-sync-point/DESIGN.md for the full architecture.
#
# ── Performance design ────────────────────────────────────────────────────────
# The tuning targets two scenarios that stress Oracle LogMiner differently:
#
# 1. Concurrent batch loads (e.g. bulk INSERT/UPDATE from ETL jobs)
#    Problem: LogMiner reads large redo-log slices; row-by-row mining
#             is slow and holds DB resources for longer than necessary.
#    Fix:     Increase log.mining.batch.size.max, session duration, and
#             in-memory result buffer so LogMiner processes large SCN ranges
#             in one pass instead of many small ones.
#
# 2. Normal steady-state streaming (few rows/sec)
#    Problem: LogMiner session is kept open indefinitely — wastes DB
#             PGA memory.  Small batch sizes cause constant reconnect
#             overhead.
#    Fix:     log.mining.sleep.time.* controls how aggressively Debezium
#             backs off when the log is idle, freeing Oracle resources.
#
# ── Kafka producer tuning ─────────────────────────────────────────────────────
# During a batch-load burst Debezium produces many messages in rapid
# succession.  Kafka producer batching (linger.ms + batch.size) groups
# these into fewer network round-trips.  compression.type=lz4 cuts
# network and broker disk I/O by ~60-70% for CDC Avro payloads.
#
# ── Single-broker constraints ─────────────────────────────────────────────────
# This cluster has ONE Kafka broker (replication.factor=1).  Parameters that
# require multiple brokers (min.insync.replicas>1, acks=all on multi-broker)
# are set to broker-safe values.
#
# User:    dave (data_admin / iceberg_engineer in RBAC plane)
# Source:  Oracle XE 21c  (2 vCPU, 6 GiB)
# Connect: Debezium 2.7   (2 vCPU, 2 GiB)
# Kafka:   Strimzi 4.2.0  (single broker, replication.factor=1)
#
# Requires: curl jq kubectl psql sqlplus
# Usage:    export SPARK_USER=dave && bash 02_register_debezium_oracle_connector.sh
# =============================================================================
set -euo pipefail

DEBEZIUM_URL="http://192.168.1.54:30083"
BAO_ADDR="${BAO_ADDR:-http://192.168.1.50:30820}"
CONNECT_NAME="oracle-tpcds-cdc"

# Tables to CDC (must match pipeline_watermarks)
CDC_TABLES=(
  income_band ship_mode warehouse reason call_center
  web_site web_page household_demographics catalog_page promotion
)

echo "=== Debezium Oracle CDC Registration (performance-tuned) ==="
echo "User      : ${SPARK_USER:-dave}"
echo "Connect   : $DEBEZIUM_URL"
echo "OpenBao   : $BAO_ADDR"
echo "Mode      : schema_only + SCN watermark (bulk copy already done)"
echo ""

# ── 1. OpenBao token ──────────────────────────────────────────────────────────
if [ -z "${BAO_TOKEN:-}" ]; then
  echo "[INFO] Fetching OpenBao token …"
  BAO_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
              -o jsonpath='{.data.root-token}' | base64 -d)
fi

# ── 2. Oracle credentials ─────────────────────────────────────────────────────
echo "[INFO] Reading secret/platform/oracle …"
ORACLE_SECRET=$(curl -sf -H "X-Vault-Token: $BAO_TOKEN" \
  "$BAO_ADDR/v1/secret/data/platform/oracle" | jq -r '.data.data')
ORACLE_USER=$(echo "$ORACLE_SECRET" | jq -r '.user     // "tpcds"')
ORACLE_PASS=$(echo "$ORACLE_SECRET" | jq -r '.password')
ORACLE_HOST=$(echo "$ORACLE_SECRET" | jq -r '.host     // "oracle-xe.prod.svc.cluster.local"')
ORACLE_PORT=$(echo "$ORACLE_SECRET" | jq -r '.port     // "1521"')
ORACLE_SID=$(echo  "$ORACLE_SECRET" | jq -r '.sid      // "XEPDB1"')

# ── 3. Kafka SASL credentials ─────────────────────────────────────────────────
echo "[INFO] Reading secret/platform/kafka …"
KAFKA_SECRET=$(curl -sf -H "X-Vault-Token: $BAO_TOKEN" \
  "$BAO_ADDR/v1/secret/data/platform/kafka" | jq -r '.data.data')
KAFKA_USER=$(echo "$KAFKA_SECRET" | jq -r '.debezium_user     // "debezium-user"')
KAFKA_PASS=$(echo "$KAFKA_SECRET" | jq -r '.debezium_password')
KAFKA_BOOTSTRAP="strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092"

# ── 4. Pipeline DB credentials ────────────────────────────────────────────────
echo "[INFO] Reading secret/platform/pipeline_db …"
PG_SECRET=$(curl -sf -H "X-Vault-Token: $BAO_TOKEN" \
  "$BAO_ADDR/v1/secret/data/platform/pipeline_db" | jq -r '.data.data')
PG_HOST=$(echo "$PG_SECRET" | jq -r '.host')
PG_PORT=$(echo "$PG_SECRET" | jq -r '.port     // "5432"')
PG_DB=$(echo   "$PG_SECRET" | jq -r '.database // "pipeline"')
PG_USER=$(echo "$PG_SECRET" | jq -r '.user     // "pipeline"')
PG_PASS=$(echo "$PG_SECRET" | jq -r '.password')

# ── 5. Verify all watermarks exist ────────────────────────────────────────────
echo "[INFO] Checking pipeline_watermarks …"
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
    echo "  [WARN] No watermark: $tbl"
  else
    echo "  [OK]   $tbl → sf_extraction_ts=$TS"
  fi
done
if [ ${#MISSING[@]} -gt 0 ]; then
  echo "[ERROR] Missing watermarks for: ${MISSING[*]}"
  echo "[ERROR] Run starpump.py first."
  exit 1
fi

# ── 6. Resolve Oracle SCN for each table ─────────────────────────────────────
echo ""
echo "[INFO] Resolving Oracle SCNs …"
ORACLE_NP_HOST="192.168.1.50"
ORACLE_NP_PORT="30521"
MIN_SCN=0

for tbl in "${CDC_TABLES[@]}"; do
  SF_TS=$(PGPASSWORD="$PG_PASS" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" \
          -d "$PG_DB" -At \
          -c "SELECT sf_extraction_ts FROM pipeline_watermarks
              WHERE source_db='SNOWFLAKE_SAMPLE_DATA'
                AND source_schema='TPCDS_SF10TCL'
                AND table_name='${tbl}'")
  ORA_TS="${SF_TS/T/ }"; ORA_TS="${ORA_TS/Z/}"

  SCN=$(sqlplus -s "${ORACLE_USER}/${ORACLE_PASS}@${ORACLE_NP_HOST}:${ORACLE_NP_PORT}/${ORACLE_SID}" \
    <<SQLEOF 2>/dev/null | tr -d ' \r\n'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 VERIFY OFF
SELECT TIMESTAMP_TO_SCN(TO_TIMESTAMP('${ORA_TS}','YYYY-MM-DD HH24:MI:SS.FF6') AT TIME ZONE 'UTC') FROM DUAL;
EXIT;
SQLEOF
)
  if [ -z "$SCN" ] || ! [[ "$SCN" =~ ^[0-9]+$ ]]; then
    echo "  [WARN] SCN resolution failed for $tbl — falling back to CURRENT_SCN."
    SCN=$(sqlplus -s "${ORACLE_USER}/${ORACLE_PASS}@${ORACLE_NP_HOST}:${ORACLE_NP_PORT}/${ORACLE_SID}" \
      <<SQLEOF2 2>/dev/null | tr -d ' \r\n'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 VERIFY OFF
SELECT CURRENT_SCN FROM V\$DATABASE;
EXIT;
SQLEOF2
)
  fi
  echo "  [SCN]  $tbl → $SCN"
  PGPASSWORD="$PG_PASS" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -q \
    -c "UPDATE pipeline_watermarks SET oracle_start_scn=$SCN, updated_at=NOW()
        WHERE source_db='SNOWFLAKE_SAMPLE_DATA' AND source_schema='TPCDS_SF10TCL'
          AND table_name='${tbl}'"
  if [ "$MIN_SCN" -eq 0 ] || [ "$SCN" -lt "$MIN_SCN" ]; then MIN_SCN="$SCN"; fi
done

echo ""
echo "[INFO] Starting SCN for Debezium: $MIN_SCN"
SR_URL="http://schema-registry.prod.svc.cluster.local:8081"

# ── 7. Register connector ─────────────────────────────────────────────────────
echo "[INFO] Registering connector: $CONNECT_NAME …"

curl -sf -X POST "$DEBEZIUM_URL/connectors" \
  -H "Content-Type: application/json" \
  -d "$(cat <<CONNECTOR_JSON
{
  "name": "${CONNECT_NAME}",
  "config": {

    "connector.class": "io.debezium.connector.oracle.OracleConnector",
    "tasks.max": "1",

    "database.hostname":    "${ORACLE_HOST}",
    "database.port":        "${ORACLE_PORT}",
    "database.user":        "${ORACLE_USER}",
    "database.password":    "${ORACLE_PASS}",
    "database.dbname":      "${ORACLE_SID}",
    "database.pdb.name":    "${ORACLE_SID}",
    "database.server.name": "oracle-tpcds",

    "table.include.list": "TPCDS.INCOME_BAND,TPCDS.SHIP_MODE,TPCDS.WAREHOUSE,TPCDS.REASON,TPCDS.CALL_CENTER,TPCDS.WEB_SITE,TPCDS.WEB_PAGE,TPCDS.HOUSEHOLD_DEMOGRAPHICS,TPCDS.CATALOG_PAGE,TPCDS.PROMOTION",

    "topic.prefix": "oracle-tpcds",

    "snapshot.mode":         "schema_only",
    "snapshot.offset.scn":   "${MIN_SCN}",
    "snapshot.locking.mode": "none",

    "database.history.kafka.bootstrap.servers": "${KAFKA_BOOTSTRAP}",
    "database.history.kafka.topic":             "schema-changes.oracle-tpcds",
    "database.history.consumer.security.protocol":  "SASL_PLAINTEXT",
    "database.history.consumer.sasl.mechanism":     "SCRAM-SHA-512",
    "database.history.consumer.sasl.jaas.config":   "org.apache.kafka.common.security.scram.ScramLoginModule required username=\\"${KAFKA_USER}\\" password=\\"${KAFKA_PASS}\\";",
    "database.history.producer.security.protocol":  "SASL_PLAINTEXT",
    "database.history.producer.sasl.mechanism":     "SCRAM-SHA-512",
    "database.history.producer.sasl.jaas.config":   "org.apache.kafka.common.security.scram.ScramLoginModule required username=\\"${KAFKA_USER}\\" password=\\"${KAFKA_PASS}\\";",

    "key.converter":                       "io.confluent.kafka.serializers.KafkaAvroSerializer",
    "key.converter.schema.registry.url":   "${SR_URL}",
    "value.converter":                     "io.confluent.kafka.serializers.KafkaAvroSerializer",
    "value.converter.schema.registry.url": "${SR_URL}",

    "decimal.handling.mode":  "double",
    "time.precision.mode":    "connect",
    "tombstones.on.delete":   "false",

    "heartbeat.interval.ms":       "10000",
    "heartbeat.action.query":      "SELECT 1 FROM DUAL",

    "producer.security.protocol": "SASL_PLAINTEXT",
    "producer.sasl.mechanism":    "SCRAM-SHA-512",
    "producer.sasl.jaas.config":  "org.apache.kafka.common.security.scram.ScramLoginModule required username=\\"${KAFKA_USER}\\" password=\\"${KAFKA_PASS}\\";",

    "log.mining.strategy":          "online_catalog",

    "log.mining.continuous.mine":   "false",

    "log.mining.batch.size.default": "20000",
    "log.mining.batch.size.min":     "1000",
    "log.mining.batch.size.max":     "100000",

    "log.mining.sleep.time.default.ms": "1000",
    "log.mining.sleep.time.min.ms":     "0",
    "log.mining.sleep.time.max.ms":     "3000",
    "log.mining.sleep.time.increment.ms": "500",

    "log.mining.session.max.ms":    "1800000",

    "log.mining.transaction.retention.ms": "3600000",

    "log.mining.query.filter.mode": "in",

    "log.mining.buffer.type":            "memory",
    "log.mining.buffer.transaction.events.total": "100000",

    "producer.acks":          "1",
    "producer.linger.ms":     "50",
    "producer.batch.size":    "262144",
    "producer.buffer.memory": "67108864",
    "producer.compression.type": "lz4",
    "producer.max.request.size": "5242880",
    "producer.request.timeout.ms": "30000",
    "producer.retries":           "5",
    "producer.retry.backoff.ms":  "500",
    "producer.delivery.timeout.ms": "120000",

    "max.queue.size":                "81920",
    "max.queue.size.in.bytes":       "104857600",
    "max.batch.size":                "8192",
    "poll.interval.ms":              "500",

    "event.processing.failure.handling.mode": "warn",

    "skipped.operations": "none"
  }
}
CONNECTOR_JSON
)"

echo ""
echo "[INFO] Connector registered. Checking status in 5 s …"
sleep 5
curl -sf "$DEBEZIUM_URL/connectors/$CONNECT_NAME/status" | jq .

echo ""
echo "[INFO] Watermark summary (pipeline DB):"
PGPASSWORD="$PG_PASS" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
  -c "SELECT table_name, sf_extraction_ts, oracle_start_scn, rows_copied, updated_at
      FROM pipeline_watermarks
      WHERE source_db='SNOWFLAKE_SAMPLE_DATA' AND source_schema='TPCDS_SF10TCL'
      ORDER BY table_name;"
