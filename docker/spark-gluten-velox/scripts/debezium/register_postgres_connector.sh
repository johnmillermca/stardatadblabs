#!/usr/bin/env bash
# =============================================================================
# register_postgres_connector.sh
#
# Register the Debezium PostgreSQL CDC connector via the Kafka Connect REST API.
#
# ── CDC sync-point ────────────────────────────────────────────────────────────
# Must run AFTER starpump initial full load completes.
# starpump writes sf_extraction_ts into pipeline_watermarks for each table.
# This script reads those timestamps, verifies they exist, and starts Debezium
# at the correct logical replication slot position (snapshot.mode=never →
# Debezium reads from the existing replication slot's current LSN, which
# corresponds to changes AFTER the initial load timestamp).
#
# For PostgreSQL CDC we use the logical replication slot approach:
#   - snapshot.mode=never   → Debezium only streams changes, no initial snapshot
#   - slot.name             → a pre-created pgoutput/decoderbufs replication slot
#   - publication.name      → a PostgreSQL publication covering the target tables
#
# ── Performance tuning ────────────────────────────────────────────────────────
# Kafka producer:
#   linger.ms=5, batch.size=65536, compression.type=lz4, acks=1
# Debezium:
#   max.batch.size=8192, max.queue.size=16384
# Single-broker: replication.factor=1, acks=1
#
# Usage:
#   export SPARK_USER=dave
#   bash register_postgres_connector.sh
#
# Source: PostgreSQL cache_testing DB → topics: postgres.cache_testing.<table>
# =============================================================================
set -euo pipefail

DEBEZIUM_URL="${DEBEZIUM_URL:-http://192.168.1.54:30083}"
BAO_ADDR="${BAO_ADDR:-http://openbao.prod.svc.cluster.local:8200}"
CONNECT_NAME="postgres-cache-testing-cdc"
SOURCE_DB="cache_testing"
SOURCE_SCHEMA="public"
KAFKA_BOOTSTRAP="strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092"
SR_URL="http://schema-registry.prod.svc.cluster.local:8081"

# Tables to CDC — must match pipeline_watermarks entries written by starpump
CDC_TABLES=(customers products product_reviews orders)

echo "=== Debezium PostgreSQL CDC Registration ==="
echo "User     : ${SPARK_USER:-dave}"
echo "Connect  : $DEBEZIUM_URL"
echo "Source   : $SOURCE_DB.$SOURCE_SCHEMA"
echo "Tables   : ${CDC_TABLES[*]}"
echo ""

# ── 1. OpenBao token ──────────────────────────────────────────────────────────
if [ -z "${BAO_TOKEN:-}" ]; then
  echo "[INFO] Fetching OpenBao token …"
  BAO_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
              -o jsonpath='{.data.root-token}' | base64 -d)
fi

bao_read() {
  curl -sf -H "X-Vault-Token: $BAO_TOKEN" "$BAO_ADDR/v1/$1" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); print(json.dumps(d.get('data',{}).get('data',d.get('data',{}))))"
}

# ── 2. PostgreSQL credentials ─────────────────────────────────────────────────
echo "[INFO] Reading secret/platform/postgres …"
PG_SECRET=$(bao_read "secret/data/platform/postgres")
PG_HOST=$(echo "$PG_SECRET"     | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('host','postgresql.prod.svc.cluster.local'))")
PG_PORT=$(echo "$PG_SECRET"     | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('port','5432'))")
PG_USER=$(echo "$PG_SECRET"     | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('user','postgres'))")
PG_PASS=$(echo "$PG_SECRET"     | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('password',''))")

# ── 3. Kafka SASL credentials ─────────────────────────────────────────────────
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
echo "[INFO] Verifying pipeline_watermarks …"
MISSING=()
for tbl in "${CDC_TABLES[@]}"; do
  TS=$(PGPASSWORD="$PIPE_PASS" psql -h "$PIPE_HOST" -p "$PIPE_PORT" \
       -U "$PIPE_USER" -d "$PIPE_DB" -At \
       -c "SELECT sf_extraction_ts FROM pipeline_watermarks
           WHERE source_db='${SOURCE_DB}' AND source_schema='${SOURCE_SCHEMA}'
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
  echo "[ERROR] Run starpump postgres first."
  exit 1
fi

# ── 6. Ensure replication slot and publication exist ──────────────────────────
# PostgreSQL logical replication requires:
#   a) A replication slot using the pgoutput plugin (built-in since PG10)
#   b) A publication covering the CDC tables
echo "[INFO] Ensuring replication slot 'debezium_cache_testing' and publication …"

TABLE_LIST_QUAL=""
for tbl in "${CDC_TABLES[@]}"; do
  TABLE_LIST_QUAL="${TABLE_LIST_QUAL}${SOURCE_SCHEMA}.${tbl},"
done
TABLE_LIST_QUAL="${TABLE_LIST_QUAL%,}"   # strip trailing comma

PGPASSWORD="$PG_PASS" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" \
  -d "$SOURCE_DB" -c "
DO \$\$
BEGIN
  -- Create replication slot if it does not exist
  IF NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'debezium_cache_testing'
  ) THEN
    PERFORM pg_create_logical_replication_slot('debezium_cache_testing', 'pgoutput');
    RAISE NOTICE 'Replication slot created.';
  ELSE
    RAISE NOTICE 'Replication slot already exists.';
  END IF;

  -- Create publication if it does not exist
  IF NOT EXISTS (
    SELECT 1 FROM pg_publication WHERE pubname = 'debezium_pub_cache_testing'
  ) THEN
    EXECUTE 'CREATE PUBLICATION debezium_pub_cache_testing FOR TABLE ${TABLE_LIST_QUAL}';
    RAISE NOTICE 'Publication created.';
  ELSE
    RAISE NOTICE 'Publication already exists.';
  END IF;
END
\$\$;
" || echo "[WARN] Could not verify slot/publication — proceeding (may already exist)."

# ── 7. Delete existing connector if present (idempotent re-registration) ──────
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
  TABLE_INCLUDE="${TABLE_INCLUDE}${SOURCE_SCHEMA}.${tbl},"
done
TABLE_INCLUDE="${TABLE_INCLUDE%,}"

# ── 9. Register connector ─────────────────────────────────────────────────────
echo "[INFO] Registering connector: $CONNECT_NAME …"

JAAS_CFG="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${KAFKA_USER}\" password=\"${KAFKA_PASS}\";"

curl -sf -X POST "$DEBEZIUM_URL/connectors" \
  -H "Content-Type: application/json" \
  -d "$(cat <<CONNECTOR_JSON
{
  "name": "${CONNECT_NAME}",
  "config": {
    "connector.class":  "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max":        "1",

    "database.hostname": "${PG_HOST}",
    "database.port":     "${PG_PORT}",
    "database.user":     "${PG_USER}",
    "database.password": "${PG_PASS}",
    "database.dbname":   "${SOURCE_DB}",
    "database.server.name": "postgres",

    "plugin.name":          "pgoutput",
    "slot.name":            "debezium_cache_testing",
    "publication.name":     "debezium_pub_cache_testing",

    "table.include.list":   "${TABLE_INCLUDE}",

    "topic.prefix":         "postgres.cache_testing",
    "topic.naming.strategy": "io.debezium.schema.DefaultTopicNamingStrategy",

    "snapshot.mode":        "never",

    "schema.history.internal.kafka.bootstrap.servers": "${KAFKA_BOOTSTRAP}",
    "schema.history.internal.kafka.topic":             "schema-changes.postgres",
    "schema.history.internal.consumer.security.protocol":  "SASL_PLAINTEXT",
    "schema.history.internal.consumer.sasl.mechanism":     "SCRAM-SHA-512",
    "schema.history.internal.consumer.sasl.jaas.config":   "${JAAS_CFG}",
    "schema.history.internal.producer.security.protocol":  "SASL_PLAINTEXT",
    "schema.history.internal.producer.sasl.mechanism":     "SCRAM-SHA-512",
    "schema.history.internal.producer.sasl.jaas.config":   "${JAAS_CFG}",

    "key.converter":               "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter":             "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",

    "decimal.handling.mode": "double",
    "time.precision.mode":   "connect",
    "tombstones.on.delete":  "false",

    "heartbeat.interval.ms":  "10000",
    "heartbeat.action.query": "SELECT 1",

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

    "max.queue.size":             "16384",
    "max.queue.size.in.bytes":    "104857600",
    "max.batch.size":             "8192",
    "poll.interval.ms":           "500",

    "event.processing.failure.handling.mode": "warn",
    "skipped.operations": "none",

    "include.schema.changes": "true",
    "include.unknown.datatypes": "false"
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
  -c "SELECT table_name, sf_extraction_ts, rows_copied, updated_at
      FROM pipeline_watermarks
      WHERE source_db='${SOURCE_DB}' AND source_schema='${SOURCE_SCHEMA}'
      ORDER BY table_name;"
