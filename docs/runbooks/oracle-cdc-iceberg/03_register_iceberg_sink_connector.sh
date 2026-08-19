#!/usr/bin/env bash
# =============================================================================
# 03_register_iceberg_sink_connector.sh
#
# Registers the Apache Iceberg Sink Kafka Connector for all 10 Oracle TPC-DS
# CDC topics.  Consumes Avro CDC events from Debezium, writes them to Polaris-
# managed Iceberg tables on S3.
#
# ── Performance design ────────────────────────────────────────────────────────
# The sink tuning addresses two bottlenecks:
#
# 1. Consumer throughput during burst / catch-up
#    When Oracle had a batch load while the connector was down, Kafka topics
#    accumulate a large backlog.  The consumer must drain this backlog quickly
#    without OOM'ing the Connect worker (2 GiB limit).
#
#    fetch.min.bytes        — wait until 1 MB is available before returning a
#                             fetch, reducing broker round-trips 10×.
#    fetch.max.bytes        — cap per-fetch at 50 MB so the 2 GiB heap is safe.
#    max.poll.records       — pull 2 000 records per poll loop to fill Iceberg
#                             write batches efficiently.
#    max.partition.fetch.bytes — per-partition cap to prevent one hot partition
#                             from starving others.
#
# 2. Iceberg commit frequency vs. latency trade-off
#    Each Iceberg commit creates a new snapshot and a manifest file.  Too many
#    small commits = S3 small-file problem.  Too infrequent = latency.
#
#    iceberg.tables.commitIntervalMs = 30 000  (30 s)
#      → batches 30 s worth of CDC events into one Iceberg snapshot.
#        At 1 000 events/s that is ~30 k rows per file — well above the 256 MB
#        target file size threshold.
#    iceberg.tables.commitTimeoutMs  = 30 000  (must be ≤ commitIntervalMs)
#      → hard deadline for the commit RPC; prevents stuck tasks.
#
# 3. Parallelism
#    tasks.max = 4 (limited by Debezium worker 2 vCPU; 4 tasks share the CPU).
#    Each task handles a subset of the 10 topics.  The CDC source connector has
#    tasks.max=1 (Oracle LogMiner is single-threaded), so the sink tasks
#    independently drain per-topic partitions in parallel.
#
# 4. Consumer session / heartbeat (prevents unwanted rebalances under load)
#    Iceberg commits can take up to commitTimeoutMs (30 s).  The consumer must
#    not be considered dead during a slow commit.
#    session.timeout.ms     = 120 000  (2 min, must be within broker max)
#    heartbeat.interval.ms  = 20 000   (1/6 of session timeout per KIP-62)
#    max.poll.interval.ms   = 300 000  (5 min — time between poll() calls;
#                             set high to allow for large Iceberg commits)
#
# ── Single-broker constraints ─────────────────────────────────────────────────
# replication.factor=1, min.insync.replicas=1, acks=1 throughout.
#
# Converter : Confluent Avro (KafkaAvroDeserializer) + Schema Registry
# Catalog   : Polaris REST (IcebergCatalog / namespace = tpcds)
# User      : dave  (can_write_iceberg=true, can_admin_catalog=true)
# Credentials: all from OpenBao
#
# Requires: curl jq kubectl
# Usage:    export SPARK_USER=dave && bash 03_register_iceberg_sink_connector.sh
# =============================================================================
set -euo pipefail

DEBEZIUM_URL="http://192.168.1.54:30083"
BAO_ADDR="${BAO_ADDR:-http://192.168.1.50:30820}"
CONNECT_NAME="iceberg-sink-tpcds"
POLARIS_CATALOG_NAME="IcebergCatalog"
ICEBERG_NAMESPACE="tpcds"

echo "=== Iceberg Sink Connector Registration (performance-tuned) ==="
echo "User      : ${SPARK_USER:-dave}"
echo "Connect   : $DEBEZIUM_URL"
echo "Catalog   : $POLARIS_CATALOG_NAME / $ICEBERG_NAMESPACE"
echo ""

# ── 1. OpenBao token ─────────────────────────────────────────────────────────
if [ -z "${BAO_TOKEN:-}" ]; then
  BAO_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
              -o jsonpath='{.data.root-token}' | base64 -d)
fi

# ── 2. Polaris credentials ────────────────────────────────────────────────────
echo "[INFO] Reading secret/platform/polaris …"
POLARIS=$(curl -sf -H "X-Vault-Token: $BAO_TOKEN" \
  "$BAO_ADDR/v1/secret/data/platform/polaris" | jq -r '.data.data')
POLARIS_ID=$(echo  "$POLARIS" | jq -r '.spark_svc_id')
POLARIS_SEC=$(echo "$POLARIS" | jq -r '.spark_svc_secret')

# ── 3. S3 credentials ─────────────────────────────────────────────────────────
echo "[INFO] Reading secret/platform/s3 …"
S3=$(curl -sf -H "X-Vault-Token: $BAO_TOKEN" \
  "$BAO_ADDR/v1/secret/data/platform/s3" | jq -r '.data.data')
S3_KEY=$(echo    "$S3" | jq -r '.access_key')
S3_SEC=$(echo    "$S3" | jq -r '.secret_key')
S3_BUCKET=$(echo "$S3" | jq -r '.bucket')
S3_REGION=$(echo "$S3" | jq -r '.region')
S3_ENDPOINT=$(echo "$S3" | jq -r '.endpoint')

# ── 4. Kafka SASL credentials ─────────────────────────────────────────────────
echo "[INFO] Reading secret/platform/kafka …"
KAFKA_SECRET=$(curl -sf -H "X-Vault-Token: $BAO_TOKEN" \
  "$BAO_ADDR/v1/secret/data/platform/kafka" | jq -r '.data.data')
KAFKA_USER=$(echo "$KAFKA_SECRET" | jq -r '.debezium_user     // "debezium-user"')
KAFKA_PASS=$(echo "$KAFKA_SECRET" | jq -r '.debezium_password')

SR_URL="http://schema-registry.prod.svc.cluster.local:8081"
POLARIS_URI="http://polaris-rest.prod.svc.cluster.local:8181/api/catalog"

# ── 5. Register connector ─────────────────────────────────────────────────────
echo "[INFO] Registering connector: $CONNECT_NAME …"

curl -sf -X POST "$DEBEZIUM_URL/connectors" \
  -H "Content-Type: application/json" \
  -d "$(cat <<SINK_JSON
{
  "name": "${CONNECT_NAME}",
  "config": {

    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",

    "tasks.max": "4",

    "topics": "oracle-tpcds.TPCDS.INCOME_BAND,oracle-tpcds.TPCDS.SHIP_MODE,oracle-tpcds.TPCDS.WAREHOUSE,oracle-tpcds.TPCDS.REASON,oracle-tpcds.TPCDS.CALL_CENTER,oracle-tpcds.TPCDS.WEB_SITE,oracle-tpcds.TPCDS.WEB_PAGE,oracle-tpcds.TPCDS.HOUSEHOLD_DEMOGRAPHICS,oracle-tpcds.TPCDS.CATALOG_PAGE,oracle-tpcds.TPCDS.PROMOTION",

    "key.converter":                        "io.confluent.kafka.serializers.KafkaAvroDeserializer",
    "key.converter.schema.registry.url":    "${SR_URL}",
    "value.converter":                      "io.confluent.kafka.serializers.KafkaAvroDeserializer",
    "value.converter.schema.registry.url":  "${SR_URL}",
    "value.converter.specific.avro.reader": "false",

    "iceberg.catalog":                      "${POLARIS_CATALOG_NAME}",
    "iceberg.catalog.type":                 "rest",
    "iceberg.catalog.uri":                  "${POLARIS_URI}",
    "iceberg.catalog.credential":           "${POLARIS_ID}:${POLARIS_SEC}",
    "iceberg.catalog.scope":                "PRINCIPAL_ROLE:ALL",
    "iceberg.catalog.warehouse":            "${POLARIS_CATALOG_NAME}",
    "iceberg.catalog.client.region":        "${S3_REGION}",
    "iceberg.catalog.s3.access-key-id":     "${S3_KEY}",
    "iceberg.catalog.s3.secret-access-key": "${S3_SEC}",
    "iceberg.catalog.s3.endpoint":          "${S3_ENDPOINT}",
    "iceberg.catalog.s3.path-style-access": "true",

    "iceberg.tables.auto-create-enabled": "true",
    "iceberg.tables.default-namespace":   "${ICEBERG_NAMESPACE}",
    "iceberg.tables.upsert-mode-enabled": "true",
    "iceberg.tables.cdcField":            "op",

    "iceberg.tables.commitIntervalMs":  "30000",
    "iceberg.tables.commitTimeoutMs":   "30000",

    "iceberg.tables.routeField":  "source.table",
    "iceberg.tables.routeRegex":  "([A-Z_]+)$",

    "iceberg.tables.write.format.default":         "parquet",
    "iceberg.tables.write.target-file-size-bytes": "268435456",

    "iceberg.control.topic":    "iceberg-control-tpcds",
    "iceberg.control.group-id": "iceberg-sink-tpcds-ctrl",

    "consumer.override.auto.offset.reset": "earliest",

    "consumer.override.fetch.min.bytes":          "1048576",
    "consumer.override.fetch.max.bytes":          "52428800",
    "consumer.override.max.partition.fetch.bytes": "10485760",
    "consumer.override.max.poll.records":          "2000",

    "consumer.override.session.timeout.ms":   "120000",
    "consumer.override.heartbeat.interval.ms": "20000",
    "consumer.override.max.poll.interval.ms":  "300000",

    "consumer.override.receive.buffer.bytes": "1048576",

    "consumer.override.security.protocol": "SASL_PLAINTEXT",
    "consumer.override.sasl.mechanism":    "SCRAM-SHA-512",
    "consumer.override.sasl.jaas.config":  "org.apache.kafka.common.security.scram.ScramLoginModule required username=\\"${KAFKA_USER}\\" password=\\"${KAFKA_PASS}\\";"
  }
}
SINK_JSON
)"

echo ""
echo "[INFO] Iceberg Sink connector registered. Checking status in 5 s …"
sleep 5
curl -sf "$DEBEZIUM_URL/connectors/$CONNECT_NAME/status" | jq .
