#!/usr/bin/env bash
# =============================================================================
# register_oracle_cache_testing_connector.sh
#
# Register the Debezium Oracle CDC connector for the CACHE_TESTING schema
# in XEPDB1 via the Kafka Connect REST API.
#
# ── CDC sync-point ────────────────────────────────────────────────────────────
# Must run AFTER starpump oracle initial full load completes for CACHE_TESTING.
# Uses snapshot.mode=no_data — Debezium reads schema only, then streams
# changes from the current SCN (post full-load position).
#
# ── Pre-requisites ─────────────────────────────────────────────────────────────
# 1. Oracle ARCHIVELOG mode enabled (ALTER DATABASE ARCHIVELOG)
# 2. Supplemental logging enabled:
#      ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
#      ALTER TABLE CACHE_TESTING.<table> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
# 3. CDB-common user c##dbzcdc exists with LogMiner privileges (see OpenBao
#    secret/data/platform/oracle cdc_user / cdc_password keys)
# 4. c##dbzcdc granted SELECT on all CACHE_TESTING tables
#
# ── Connector naming ──────────────────────────────────────────────────────────
# Connector  : oracle-cache-testing-cdc
# Topics     : oracle.cache_testing.<table>   (always lowercase — enforced by
#              LowerCaseTopicNamingStrategy; Oracle uppercases identifiers in its
#              data dictionary so SchemaTopicNamingStrategy would produce
#              oracle.CACHE_TESTING.CUSTOMERS.  LowerCaseTopicNamingStrategy
#              normalises every segment to lowercase unconditionally, which means
#              any future table added to table.include.list automatically lands on
#              oracle.cache_testing.<table> with no extra work.)
# Schema hist: schema-changes.oracle-cache-testing
#
# ── Performance tuning ────────────────────────────────────────────────────────
# LogMiner: online_catalog strategy, batch 20k–100k, memory buffer
# Kafka producer: linger.ms=5, batch.size=65536, compression.type=lz4, acks=1
#
# Usage:
#   export SPARK_USER=dave
#   bash register_oracle_cache_testing_connector.sh
# =============================================================================
set -euo pipefail

DEBEZIUM_URL="${DEBEZIUM_URL:-http://192.168.1.54:30083}"
BAO_ADDR="${BAO_ADDR:-http://openbao.prod.svc.cluster.local:8200}"
CONNECT_NAME="oracle-cache-testing-cdc"
SOURCE_DB="XEPDB1"
SOURCE_SCHEMA="CACHE_TESTING"
KAFKA_BOOTSTRAP="strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092"

CDC_TABLES=(customers inventory_events order_items orders products product_reviews)

echo "=== Debezium Oracle CDC Registration (CACHE_TESTING) ==="
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

# ── 2. Oracle credentials (CDB Debezium user) ─────────────────────────────────
echo "[INFO] Reading secret/platform/oracle …"
ORA_SECRET=$(bao_read "secret/data/platform/oracle")
ORA_HOST=$(echo "$ORA_SECRET" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('host','oracle-xe.prod.svc.cluster.local'))")
ORA_PORT=$(echo "$ORA_SECRET" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('port','1521'))")
# Use dedicated CDB Debezium user (c##dbzcdc) for LogMiner
ORA_USER=$(echo "$ORA_SECRET" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('cdc_user','c##dbzcdc'))")
ORA_PASS=$(echo "$ORA_SECRET" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('cdc_password',''))")

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

# ── 5. Verify watermarks exist (starpump must have run first) ─────────────────
echo "[INFO] Verifying pipeline_watermarks for XEPDB1.cache_testing …"
MISSING=()
for tbl in "${CDC_TABLES[@]}"; do
  TS=$(python3 -c "
import psycopg2, sys
try:
    conn = psycopg2.connect(host='${PIPE_HOST}', port=${PIPE_PORT},
                            dbname='${PIPE_DB}', user='${PIPE_USER}',
                            password='${PIPE_PASS}')
    cur = conn.cursor()
    cur.execute(\"SELECT sf_extraction_ts FROM pipeline_watermarks \
                  WHERE source_db='XEPDB1' AND source_schema='cache_testing' \
                    AND table_name='${tbl}'\")
    row = cur.fetchone()
    print(row[0] if row else '')
    conn.close()
except Exception as e:
    print('', end='')
" 2>/dev/null || true)
  if [ -z "$TS" ]; then
    MISSING+=("$tbl")
    echo "  [WARN] No watermark for $tbl"
  else
    echo "  [OK]   $tbl → sf_extraction_ts=$TS"
  fi
done
if [ ${#MISSING[@]} -gt 0 ]; then
  echo "[ERROR] Missing watermarks: ${MISSING[*]}"
  echo "[ERROR] Run starpump oracle first."
  exit 1
fi

# ── 6. Delete existing connector if present ───────────────────────────────────
EXISTING=$(curl -sf "$DEBEZIUM_URL/connectors/$CONNECT_NAME" 2>/dev/null | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(d.get('name',''))" 2>/dev/null || true)
if [ -n "$EXISTING" ]; then
  echo "[INFO] Removing existing connector $CONNECT_NAME …"
  curl -sf -X DELETE "$DEBEZIUM_URL/connectors/$CONNECT_NAME"
  sleep 2
fi

# ── 7. Build table include list (SCHEMA.TABLE uppercase for Oracle) ───────────
# Oracle stores identifiers in uppercase in the data dictionary, so
# table.include.list must use uppercase (CACHE_TESTING.CUSTOMERS).
# LowerCaseTopicNamingStrategy then normalises the resulting topic name to
# oracle.cache_testing.customers regardless of the dictionary casing.
TABLE_INCLUDE=""
for tbl in "${CDC_TABLES[@]}"; do
  TABLE_INCLUDE="${TABLE_INCLUDE}${SOURCE_SCHEMA}.${tbl^^},"
done
TABLE_INCLUDE="${TABLE_INCLUDE%,}"

JAAS_CFG="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${KAFKA_USER}\" password=\"${KAFKA_PASS}\";"

# ── 8. Register connector ─────────────────────────────────────────────────────
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
    "database.dbname":      "XE",
    "database.pdb.name":    "XEPDB1",
    "database.server.name": "oracle",

    "table.include.list": "${TABLE_INCLUDE}",

    "topic.prefix": "oracle",
    "topic.naming.strategy": "io.debezium.connector.common.LowerCaseTopicNamingStrategy",

    "snapshot.mode":         "no_data",
    "snapshot.locking.mode": "none",

    "schema.history.internal.kafka.bootstrap.servers": "${KAFKA_BOOTSTRAP}",
    "schema.history.internal.kafka.topic":             "schema-changes.oracle-cache-testing",
    "schema.history.internal.consumer.security.protocol":  "SASL_PLAINTEXT",
    "schema.history.internal.consumer.sasl.mechanism":     "SCRAM-SHA-512",
    "schema.history.internal.consumer.sasl.jaas.config":   "${JAAS_CFG}",
    "schema.history.internal.producer.security.protocol":  "SASL_PLAINTEXT",
    "schema.history.internal.producer.sasl.mechanism":     "SCRAM-SHA-512",
    "schema.history.internal.producer.sasl.jaas.config":   "${JAAS_CFG}",

    "key.converter":                       "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable":        "false",
    "value.converter":                     "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable":      "false",

    "decimal.handling.mode":  "double",
    "time.precision.mode":    "adaptive_time_microseconds",
    "tombstones.on.delete":   "false",

    "heartbeat.interval.ms":       "10000",

    "producer.security.protocol": "SASL_PLAINTEXT",
    "producer.sasl.mechanism":    "SCRAM-SHA-512",
    "producer.sasl.jaas.config":  "${JAAS_CFG}",
    "producer.acks":              "1",
    "producer.linger.ms":         "5",
    "producer.batch.size":        "65536",
    "producer.compression.type":  "lz4",

    "log.mining.strategy":              "redo_log_catalog",
    "log.mining.continuous.mine":       "false",
    "log.mining.batch.size.default":    "20000",
    "log.mining.batch.size.max":        "100000",
    "log.mining.sleep.time.default.ms": "1000",
    "log.mining.sleep.time.max.ms":     "3000",
    "log.mining.session.max.ms":        "1800000",
    "log.mining.buffer.type":           "memory",

    "max.queue.size":          "16384",
    "max.batch.size":          "8192",
    "poll.interval.ms":        "500",

    "event.processing.failure.handling.mode": "warn",
    "skipped.operations": "none"
  }
}
CONNECTOR_JSON
)"

echo ""
echo "[INFO] Connector registered. Checking status in 15 s …"
sleep 15
curl -sf "$DEBEZIUM_URL/connectors/$CONNECT_NAME/status" | python3 -m json.tool

echo ""
echo "[INFO] Watermark summary (pipeline DB — XEPDB1.cache_testing):"
python3 -c "
import psycopg2
conn = psycopg2.connect(host='${PIPE_HOST}', port=${PIPE_PORT},
                        dbname='${PIPE_DB}', user='${PIPE_USER}',
                        password='${PIPE_PASS}')
cur = conn.cursor()
cur.execute('''SELECT table_name, sf_extraction_ts, rows_copied, updated_at
               FROM pipeline_watermarks
               WHERE source_db='XEPDB1' AND source_schema='cache_testing'
               ORDER BY table_name''')
rows = cur.fetchall()
print(f'{\"table_name\":<25} {\"sf_extraction_ts\":<35} {\"rows_copied\":>12}')
print('-'*75)
for r in rows:
    print(f'{r[0]:<25} {str(r[1]):<35} {r[2]:>12,}')
conn.close()
"
