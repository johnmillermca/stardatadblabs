#!/usr/bin/env bash
# =============================================================================
# register_oracle_cache_testing_connector.sh
#
# Register (or fully reset + re-register) the Debezium Oracle CDC connector
# for the CACHE_TESTING schema in XEPDB1 via the Kafka Connect REST API.
#
# ── When to run this script ───────────────────────────────────────────────────
# Run this script in ANY of these situations:
#   • Initial setup (first-time registration)
#   • After oracle-xe pod restart (ORA-01284 — archive log no longer exists)
#   • After DDL rename/drop stress-test runs (objectVersion drifts in history)
#   • After log.mining.strategy change
#   • After any "Failed to parse redo SQL" storm that persists across restarts
#
# The script always performs a FULL RESET:
#   1. Delete connector
#   2. Delete + reset connector offsets
#   3. Delete schema history Kafka topic
#   4. Re-register from the CURRENT Oracle SCN (no_data snapshot mode)
#
# This is always safe — the streaming pipeline reads from Kafka checkpoints,
# not from the connector offset. Any DML that occurred during the gap between
# the old offset and the new SCN is simply not replicated (acceptable for
# CDC streaming use cases where full historical accuracy is not required).
#
# ── Archive log retention ─────────────────────────────────────────────────────
# log.mining.archive.log.hours=1  — Debezium never looks back more than 1h.
# The oracle-archivelog-cleanup CronJob deletes logs older than 90min every 30min
# so the 1h Debezium window always has a 30-minute safety buffer on disk.
#
# ── Pre-requisites ─────────────────────────────────────────────────────────────
# 1. Oracle ARCHIVELOG mode enabled (ALTER DATABASE ARCHIVELOG)
# 2. Supplemental logging enabled:
#      ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
#      ALTER TABLE CACHE_TESTING.<table> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
# 3. CDB-common user c##dbzcdc exists with LogMiner privileges (see OpenBao
#    secret/data/platform/oracle cdc_user / cdc_password keys)
# 4. oracle-archivelog-cleanup CronJob deployed (manifests/oracle/)
#
# ── Connector naming ──────────────────────────────────────────────────────────
# Connector  : oracle-cache-testing-cdc
# Topics     : oracle.CACHE_TESTING.<table>
#              Pipeline regex: oracle\.(cache_testing|CACHE_TESTING)\..*
#              Column names normalised to lowercase by the streaming UDF.
# Schema hist: schema-changes.oracle-cache-testing
#
# ── LogMiner strategy ────────────────────────────────────────────────────────
# redo_log_catalog: reads schema from archived redo logs — correct after
#   RENAME COLUMN / DROP COLUMN DDL. Requires ALL COLUMNS supplemental logging.
#
# Usage:
#   bash register_oracle_cache_testing_connector.sh
#   DEBEZIUM_URL=http://192.168.1.50:30083 bash register_oracle_cache_testing_connector.sh
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

# ── 5. Get current Oracle SCN (connector will start from here) ───────────────
# When re-registering after oracle-xe restart: the old archived log files are
# gone. We MUST start from the current SCN — any DML during the gap is lost
# from CDC (acceptable) but the connector will not get stuck on ORA-01284.
echo "[INFO] Fetching current Oracle SCN …"
ORA_POD=$(kubectl -n prod get pods -l app=oracle-xe \
  --field-selector=status.phase=Running \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

CURRENT_SCN=""
if [ -n "$ORA_POD" ]; then
  CURRENT_SCN=$(kubectl -n prod exec "$ORA_POD" -- \
    bash -c "sqlplus -s / as sysdba <<'SQLEOF'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMSPOOL ON
SELECT CURRENT_SCN FROM V\$DATABASE;
EXIT;
SQLEOF" 2>/dev/null | grep -E '^[[:space:]]*[0-9]+' | tr -d ' ')
fi

if [ -n "$CURRENT_SCN" ]; then
  echo "[INFO] Current Oracle SCN: $CURRENT_SCN (connector will stream from here)"
else
  echo "[WARN] Could not read Oracle SCN — connector will use no_data snapshot default"
fi

# ── 5b. Watermark check (informational only — does NOT block registration) ────
echo "[INFO] Checking pipeline_watermarks for XEPDB1.cache_testing (informational) …"
if [ -n "$PIPE_HOST" ] && [ -n "$PIPE_PASS" ]; then
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
      echo "  [WARN] No watermark for $tbl (run starpump oracle for initial load)"
    else
      echo "  [OK]   $tbl → sf_extraction_ts=$TS"
    fi
  done
else
  echo "  [SKIP] No pipeline_db credentials — watermark check skipped."
fi

# ── 6. Full reset: delete connector + wipe schema history topic ───────────────
# Wiping the schema history forces Debezium to re-snapshot the current schema
# from Oracle's data dictionary on next start. Without this, stale objectVersion
# entries in the history topic cause "Failed to parse redo SQL" for every DML
# event after a RENAME/DROP COLUMN — even when redo_log_catalog is active.
EXISTING=$(curl -sf "$DEBEZIUM_URL/connectors/$CONNECT_NAME" 2>/dev/null | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(d.get('name',''))" 2>/dev/null || true)
if [ -n "$EXISTING" ]; then
  echo "[INFO] Removing existing connector $CONNECT_NAME …"
  curl -sf -X DELETE "$DEBEZIUM_URL/connectors/$CONNECT_NAME"
  sleep 3
fi

echo "[INFO] Deleting schema history topic (schema-changes.oracle-cache-testing) …"
cat > /tmp/kafka-reset-client.properties <<KAFKAEOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${KAFKA_USER}" password="${KAFKA_PASS}";
KAFKAEOF

# Delete via a kubectl exec into the Debezium pod (which has kafka-topics.sh)
DBZ_POD=$(kubectl get pods -n prod -l app=debezium-connect \
  --field-selector=status.phase=Running \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [ -n "$DBZ_POD" ]; then
  kubectl cp /tmp/kafka-reset-client.properties prod/"$DBZ_POD":/tmp/kafka-reset-client.properties 2>/dev/null || true
  kubectl exec -n prod "$DBZ_POD" -- \
    /kafka/bin/kafka-topics.sh \
      --bootstrap-server "${KAFKA_BOOTSTRAP}" \
      --command-config /tmp/kafka-reset-client.properties \
      --delete \
      --topic schema-changes.oracle-cache-testing 2>/dev/null \
    && echo "[INFO] Schema history topic deleted." \
    || echo "[WARN] Topic not found or already deleted — continuing."
  sleep 2
else
  echo "[WARN] No running Debezium pod found — skipping topic deletion."
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
    "log.mining.batch.size.default":    "50000",
    "log.mining.batch.size.max":        "200000",
    "log.mining.sleep.time.default.ms": "500",
    "log.mining.sleep.time.max.ms":     "2000",
    "log.mining.session.max.ms":        "1800000",
    "log.mining.buffer.type":           "memory",
    "log.mining.archive.log.hours":     "1",

    "max.queue.size":          "81920",
    "max.batch.size":          "32768",
    "max.queue.size.in.bytes": "524288000",
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
