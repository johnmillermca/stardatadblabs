#!/usr/bin/env bash
# =============================================================================
# register_oracle_connector.sh
#
# Register the Debezium Oracle LogMiner CDC connector via Kafka Connect REST.
#
# ── CDC sync-point ────────────────────────────────────────────────────────────
# Must run AFTER starpump oracle initial full load completes.
# Reads sf_extraction_ts from pipeline_watermarks, converts to Oracle SCN via
# TIMESTAMP_TO_SCN(), stores it back in pipeline_watermarks.oracle_start_scn,
# and starts Debezium from that exact SCN.
#
# ── Performance tuning ────────────────────────────────────────────────────────
# Oracle LogMiner: max.batch.size=8192, log.mining.batch.size.max=100000
# Kafka producer: linger.ms=5, batch.size=65536, compression.type=lz4, acks=1
#
# Usage:
#   export SPARK_USER=dave
#   bash register_oracle_connector.sh
#
# Source: Oracle XEPDB1.TPCDS.* → topics: oracle.tpcds.<table>
# =============================================================================
set -euo pipefail

DEBEZIUM_URL="${DEBEZIUM_URL:-http://192.168.1.54:30083}"
BAO_ADDR="${BAO_ADDR:-http://openbao.prod.svc.cluster.local:8200}"
CONNECT_NAME="oracle-tpcds-cdc"
SOURCE_DB="XEPDB1"
SOURCE_SCHEMA="TPCDS"
KAFKA_BOOTSTRAP="strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092"
SR_URL="http://schema-registry.prod.svc.cluster.local:8081"
ICEBERG_SOURCE_DB="XEPDB1"
ICEBERG_SOURCE_SCHEMA="tpcds"   # lower-cased for pipeline_watermarks lookup

CDC_TABLES=(
  call_center catalog_page household_demographics income_band promotion
  reason ship_mode warehouse web_page web_site
)

echo "=== Debezium Oracle CDC Registration (performance-tuned) ==="
echo "User     : ${SPARK_USER:-dave}"
echo "Connect  : $DEBEZIUM_URL"
echo "Source   : $SOURCE_DB.$SOURCE_SCHEMA"
echo "Tables   : ${CDC_TABLES[*]}"
echo ""

# ── 1. OpenBao token ──────────────────────────────────────────────────────────
if [ -z "${BAO_TOKEN:-}" ]; then
  BAO_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
              -o jsonpath='{.data.root-token}' | base64 -d)
fi

bao_read() {
  curl -sf -H "X-Vault-Token: $BAO_TOKEN" "$BAO_ADDR/v1/$1" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); print(json.dumps(d.get('data',{}).get('data',d.get('data',{}))))"
}

# ── 2. Oracle credentials ─────────────────────────────────────────────────────
echo "[INFO] Reading secret/platform/oracle …"
ORA_SECRET=$(bao_read "secret/data/platform/oracle")
ORA_HOST=$(echo "$ORA_SECRET"  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('host','oracle-xe.prod.svc.cluster.local'))")
ORA_PORT=$(echo "$ORA_SECRET"  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('port','1521'))")
ORA_USER=$(echo "$ORA_SECRET"  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('user','tpcds'))")
ORA_PASS=$(echo "$ORA_SECRET"  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('password',''))")
ORA_SID=$(echo  "$ORA_SECRET"  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('sid','XEPDB1'))")
ORA_NP_HOST=$(echo "$ORA_SECRET" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('nodeport_host','192.168.1.50'))")
ORA_NP_PORT=$(echo "$ORA_SECRET" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('nodeport_port','30521'))")

# ── 3. Kafka credentials ──────────────────────────────────────────────────────
echo "[INFO] Reading secret/platform/kafka …"
KAFKA_SECRET=$(bao_read "secret/data/platform/kafka")
KAFKA_USER=$(echo "$KAFKA_SECRET" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('debezium_user','debezium-user'))")
KAFKA_PASS=$(echo "$KAFKA_SECRET" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('debezium_password',''))")

# ── 4. Pipeline DB credentials ────────────────────────────────────────────────
echo "[INFO] Reading secret/platform/pipeline_db …"
PIPE_SECRET=$(bao_read "secret/data/platform/pipeline_db")
PIPE_HOST=$(echo "$PIPE_SECRET" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('host',''))")
PIPE_PORT=$(echo "$PIPE_SECRET" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('port','5432'))")
PIPE_DB=$(echo   "$PIPE_SECRET" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('database','pipeline'))")
PIPE_USER=$(echo "$PIPE_SECRET" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('user','pipeline'))")
PIPE_PASS=$(echo "$PIPE_SECRET" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('password',''))")

# ── 5. Verify watermarks exist ────────────────────────────────────────────────
echo "[INFO] Verifying pipeline_watermarks …"
MISSING=()
for tbl in "${CDC_TABLES[@]}"; do
  TS=$(PGPASSWORD="$PIPE_PASS" psql -h "$PIPE_HOST" -p "$PIPE_PORT" \
       -U "$PIPE_USER" -d "$PIPE_DB" -At \
       -c "SELECT sf_extraction_ts FROM pipeline_watermarks
           WHERE source_db='${ICEBERG_SOURCE_DB}'
             AND source_schema='${ICEBERG_SOURCE_SCHEMA}'
             AND table_name='${tbl}'" 2>/dev/null || true)
  if [ -z "$TS" ]; then
    MISSING+=("$tbl")
    echo "  [WARN] No watermark found for $tbl"
  else
    echo "  [OK]   $tbl → sf_extraction_ts=$TS"
  fi
done
if [ ${#MISSING[@]} -gt 0 ]; then
  echo "[ERROR] Missing watermarks for: ${MISSING[*]}"
  echo "[ERROR] Run starpump oracle first."
  exit 1
fi

# ── 6. Resolve Oracle SCN for each table ─────────────────────────────────────
echo ""
echo "[INFO] Resolving Oracle SCNs from watermark timestamps …"
MIN_SCN=0

for tbl in "${CDC_TABLES[@]}"; do
  SF_TS=$(PGPASSWORD="$PIPE_PASS" psql -h "$PIPE_HOST" -p "$PIPE_PORT" \
          -U "$PIPE_USER" -d "$PIPE_DB" -At \
          -c "SELECT sf_extraction_ts FROM pipeline_watermarks
              WHERE source_db='${ICEBERG_SOURCE_DB}'
                AND source_schema='${ICEBERG_SOURCE_SCHEMA}'
                AND table_name='${tbl}'")
  # Strip the 'Z' suffix and convert T to space for Oracle TO_TIMESTAMP
  ORA_TS="${SF_TS/T/ }"; ORA_TS="${ORA_TS/Z/}"

  SCN=$(sqlplus -s "${ORA_USER}/${ORA_PASS}@${ORA_NP_HOST}:${ORA_NP_PORT}/${ORA_SID}" \
    <<SQLEOF 2>/dev/null | tr -d ' \r\n'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 VERIFY OFF TRIMSPOOL ON
SELECT TIMESTAMP_TO_SCN(TO_TIMESTAMP('${ORA_TS}','YYYY-MM-DD HH24:MI:SS.FF6') AT TIME ZONE 'UTC') FROM DUAL;
EXIT;
SQLEOF
)

  if [ -z "$SCN" ] || ! [[ "$SCN" =~ ^[0-9]+$ ]]; then
    echo "  [WARN] SCN resolution failed for $tbl (got '$SCN') — using CURRENT_SCN."
    SCN=$(sqlplus -s "${ORA_USER}/${ORA_PASS}@${ORA_NP_HOST}:${ORA_NP_PORT}/${ORA_SID}" \
      <<SQLEOF2 2>/dev/null | tr -d ' \r\n'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 VERIFY OFF TRIMSPOOL ON
SELECT CURRENT_SCN FROM V\$DATABASE;
EXIT;
SQLEOF2
)
  fi
  echo "  [SCN]  $tbl → $SCN"

  PGPASSWORD="$PIPE_PASS" psql -h "$PIPE_HOST" -p "$PIPE_PORT" \
    -U "$PIPE_USER" -d "$PIPE_DB" -q \
    -c "UPDATE pipeline_watermarks SET oracle_start_scn=${SCN}, updated_at=NOW()
        WHERE source_db='${ICEBERG_SOURCE_DB}' AND source_schema='${ICEBERG_SOURCE_SCHEMA}'
          AND table_name='${tbl}'"

  if [ "$MIN_SCN" -eq 0 ] || [ "$SCN" -lt "$MIN_SCN" ]; then MIN_SCN="$SCN"; fi
done

echo ""
echo "[INFO] Starting Debezium from SCN: $MIN_SCN"

# ── 7. Delete existing connector if present ───────────────────────────────────
EXISTING=$(curl -sf "$DEBEZIUM_URL/connectors/$CONNECT_NAME" 2>/dev/null | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(d.get('name',''))" 2>/dev/null || true)
if [ -n "$EXISTING" ]; then
  echo "[INFO] Removing existing connector $CONNECT_NAME …"
  curl -sf -X DELETE "$DEBEZIUM_URL/connectors/$CONNECT_NAME"
  sleep 2
fi

# ── 8. Build table include list ───────────────────────────────────────────────
TABLE_INCLUDE=""
for tbl in "${CDC_TABLES[@]}"; do
  TABLE_INCLUDE="${TABLE_INCLUDE}${SOURCE_SCHEMA}.${tbl^^},"
done
TABLE_INCLUDE="${TABLE_INCLUDE%,}"

JAAS_CFG="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${KAFKA_USER}\" password=\"${KAFKA_PASS}\";"

# ── 9. Register connector ─────────────────────────────────────────────────────
echo "[INFO] Registering connector: $CONNECT_NAME …"

curl -sf -X POST "$DEBEZIUM_URL/connectors" \
  -H "Content-Type: application/json" \
  -d "$(cat <<CONNECTOR_JSON
{
  "name": "${CONNECT_NAME}",
  "config": {
    "connector.class": "io.debezium.connector.oracle.OracleConnector",
    "tasks.max": "1",

    "database.hostname":    "${ORA_HOST}",
    "database.port":        "${ORA_PORT}",
    "database.user":        "${ORA_USER}",
    "database.password":    "${ORA_PASS}",
    "database.dbname":      "${ORA_SID}",
    "database.pdb.name":    "${ORA_SID}",
    "database.server.name": "oracle",

    "table.include.list": "${TABLE_INCLUDE}",

    "topic.prefix": "oracle",
    "topic.naming.strategy": "io.debezium.schema.SchemaTopicNamingStrategy",

    "snapshot.mode":         "schema_only",
    "snapshot.offset.scn":   "${MIN_SCN}",
    "snapshot.locking.mode": "none",

    "schema.history.internal.kafka.bootstrap.servers": "${KAFKA_BOOTSTRAP}",
    "schema.history.internal.kafka.topic":             "schema-changes.oracle",
    "schema.history.internal.consumer.security.protocol":  "SASL_PLAINTEXT",
    "schema.history.internal.consumer.sasl.mechanism":     "SCRAM-SHA-512",
    "schema.history.internal.consumer.sasl.jaas.config":   "${JAAS_CFG}",
    "schema.history.internal.producer.security.protocol":  "SASL_PLAINTEXT",
    "schema.history.internal.producer.sasl.mechanism":     "SCRAM-SHA-512",
    "schema.history.internal.producer.sasl.jaas.config":   "${JAAS_CFG}",

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
    "producer.sasl.jaas.config":  "${JAAS_CFG}",

    "producer.acks":              "1",
    "producer.linger.ms":         "5",
    "producer.batch.size":        "65536",
    "producer.buffer.memory":     "33554432",
    "producer.compression.type":  "lz4",
    "producer.max.request.size":  "5242880",
    "producer.request.timeout.ms": "30000",
    "producer.retries":           "5",
    "producer.retry.backoff.ms":  "500",
    "producer.delivery.timeout.ms": "120000",

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
    "log.mining.buffer.type":       "memory",
    "log.mining.buffer.transaction.events.total": "100000",

    "max.queue.size":             "16384",
    "max.queue.size.in.bytes":    "104857600",
    "max.batch.size":             "8192",
    "poll.interval.ms":           "500",

    "event.processing.failure.handling.mode": "warn",
    "skipped.operations": "none"
  }
}
CONNECTOR_JSON
)"

echo ""
echo "[INFO] Connector registered. Checking status in 5 s …"
sleep 5
curl -sf "$DEBEZIUM_URL/connectors/$CONNECT_NAME/status" | python3 -m json.tool

echo ""
echo "[INFO] Watermark summary (pipeline DB):"
PGPASSWORD="$PIPE_PASS" psql -h "$PIPE_HOST" -p "$PIPE_PORT" -U "$PIPE_USER" -d "$PIPE_DB" \
  -c "SELECT table_name, sf_extraction_ts, oracle_start_scn, rows_copied, updated_at
      FROM pipeline_watermarks
      WHERE source_db='${ICEBERG_SOURCE_DB}' AND source_schema='${ICEBERG_SOURCE_SCHEMA}'
      ORDER BY table_name;"
