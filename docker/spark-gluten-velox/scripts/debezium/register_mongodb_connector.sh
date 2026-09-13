#!/usr/bin/env bash
# =============================================================================
# register_mongodb_connector.sh
#
# Register the Debezium MongoDB CDC connector via the Kafka Connect REST API.
#
# ── CDC sync-point ────────────────────────────────────────────────────────────
# Must run AFTER starpump mongodb initial full load completes.
# MongoDB CDC uses change streams (requires MongoDB >= 4.0 replica set or
# standalone with replSet initiated).  The connector resumes from the change
# stream's current position (snapshot.mode=never) after the initial load.
#
# Debezium stores its resume token in the Kafka offsets topic, which persists
# across restarts — no manual SCN/LSN calculation required.
#
# ── Performance tuning ────────────────────────────────────────────────────────
# max.batch.size=8192, max.queue.size=16384
# Kafka producer: linger.ms=5, batch.size=65536, compression.type=lz4, acks=1
#
# Usage:
#   export SPARK_USER=dave
#   bash register_mongodb_connector.sh
#
# Source: MongoDB cache_testing.* → topics: mongodb.cache_testing.<collection>
# =============================================================================
set -euo pipefail

DEBEZIUM_URL="${DEBEZIUM_URL:-http://192.168.1.54:30083}"
BAO_ADDR="${BAO_ADDR:-http://openbao.prod.svc.cluster.local:8200}"
CONNECT_NAME="mongodb-cache-testing-cdc"
SOURCE_DB="cache_testing"
KAFKA_BOOTSTRAP="strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092"
SR_URL="http://schema-registry.prod.svc.cluster.local:8081"

CDC_COLLECTIONS=(customers products)

echo "=== Debezium MongoDB CDC Registration ==="
echo "User        : ${SPARK_USER:-dave}"
echo "Connect     : $DEBEZIUM_URL"
echo "Source DB   : $SOURCE_DB"
echo "Collections : ${CDC_COLLECTIONS[*]}"
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

# ── 2. MongoDB credentials ────────────────────────────────────────────────────
echo "[INFO] Reading secret/platform/mongodb …"
MGO_SECRET=$(bao_read "secret/data/platform/mongodb")
MGO_HOST=$(echo "$MGO_SECRET"  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('host','mongodb.prod.svc.cluster.local'))")
MGO_PORT=$(echo "$MGO_SECRET"  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('port','27017'))")
MGO_USER=$(echo "$MGO_SECRET"  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('user',''))")
MGO_PASS=$(echo "$MGO_SECRET"  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('password',''))")
MGO_AUTH=$(echo "$MGO_SECRET"  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('auth_source','admin'))")

# Build MongoDB connection string (Debezium uses mongodb.connection.string)
if [ -n "$MGO_USER" ] && [ -n "$MGO_PASS" ]; then
  MGO_CONN_STR="mongodb://${MGO_USER}:${MGO_PASS}@${MGO_HOST}:${MGO_PORT}/?authSource=${MGO_AUTH}"
else
  MGO_CONN_STR="mongodb://${MGO_HOST}:${MGO_PORT}/"
fi

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
for col in "${CDC_COLLECTIONS[@]}"; do
  TS=$(PGPASSWORD="$PIPE_PASS" psql -h "$PIPE_HOST" -p "$PIPE_PORT" \
       -U "$PIPE_USER" -d "$PIPE_DB" -At \
       -c "SELECT sf_extraction_ts FROM pipeline_watermarks
           WHERE source_db='${SOURCE_DB}' AND source_schema='${SOURCE_DB}'
             AND table_name='${col}'" 2>/dev/null || true)
  if [ -z "$TS" ]; then
    MISSING+=("$col")
    echo "  [WARN] No watermark found for $col"
  else
    echo "  [OK]   $col → sf_extraction_ts=$TS"
  fi
done
if [ ${#MISSING[@]} -gt 0 ]; then
  echo "[ERROR] Missing watermarks for: ${MISSING[*]}"
  echo "[ERROR] Run starpump mongodb first."
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

# ── 7. Build collection include list ─────────────────────────────────────────
COLL_INCLUDE=""
for col in "${CDC_COLLECTIONS[@]}"; do
  COLL_INCLUDE="${COLL_INCLUDE}${SOURCE_DB}.${col},"
done
COLL_INCLUDE="${COLL_INCLUDE%,}"

JAAS_CFG="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${KAFKA_USER}\" password=\"${KAFKA_PASS}\";"

# ── 8. Register connector ─────────────────────────────────────────────────────
echo "[INFO] Registering connector: $CONNECT_NAME …"

curl -sf -X POST "$DEBEZIUM_URL/connectors" \
  -H "Content-Type: application/json" \
  -d "$(cat <<CONNECTOR_JSON
{
  "name": "${CONNECT_NAME}",
  "config": {
    "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
    "tasks.max": "1",

    "mongodb.connection.string": "${MGO_CONN_STR}",
    "mongodb.name": "mongodb",

    "collection.include.list": "${COLL_INCLUDE}",

    "topic.prefix": "mongodb",
    "topic.naming.strategy": "io.debezium.schema.SchemaTopicNamingStrategy",

    "snapshot.mode": "never",

    "schema.history.internal.kafka.bootstrap.servers": "${KAFKA_BOOTSTRAP}",
    "schema.history.internal.kafka.topic":             "schema-changes.mongodb",
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

    "tombstones.on.delete":   "false",
    "heartbeat.interval.ms":  "10000",

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

    "capture.mode": "change_streams_update_full"
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
      WHERE source_db='${SOURCE_DB}' AND source_schema='${SOURCE_DB}'
      ORDER BY table_name;"
