#!/usr/bin/env bash
# =============================================================================
# 03_register_iceberg_sink_connector.sh
#
# Registers the Apache Iceberg Sink Kafka Connector for all 10 Oracle TPC-DS
# CDC topics.  Consumes Avro events from Debezium, writes them to Polaris-
# managed Iceberg tables on S3.
#
# Converter : Confluent Avro (KafkaAvroDeserializer) + Schema Registry
# Catalog   : Polaris REST (IcebergCatalog / namespace = tpcds)
# User      : dave  (can_write_iceberg=true, can_admin_catalog=true)
# Credentials: all from OpenBao
#
# Requires the tabular/iceberg-kafka-connect image to be present in Debezium
# Connect pod's plugin.path (/kafka/connect).  Image tag used in the platform:
#   192.168.1.50:30500/tabular/iceberg-kafka-connect:0.6.19
#
# Usage:
#   export SPARK_USER=dave
#   bash 03_register_iceberg_sink_connector.sh
# =============================================================================
set -euo pipefail

DEBEZIUM_URL="http://192.168.1.54:30083"
BAO_ADDR="${BAO_ADDR:-http://192.168.1.50:30820}"
CONNECT_NAME="iceberg-sink-tpcds"
POLARIS_CATALOG_NAME="IcebergCatalog"
ICEBERG_NAMESPACE="tpcds"

echo "=== Iceberg Sink Connector Registration ==="
echo "User      : ${SPARK_USER:-dave}"
echo "Connect   : $DEBEZIUM_URL"
echo "Catalog   : $POLARIS_CATALOG_NAME / $ICEBERG_NAMESPACE"
echo ""

# ── 1. Get OpenBao token ─────────────────────────────────────────────────────
if [ -z "${BAO_TOKEN:-}" ]; then
  BAO_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
              -o jsonpath='{.data.root-token}' | base64 -d)
fi

# ── 2. Polaris credentials from OpenBao ──────────────────────────────────────
echo "[INFO] Reading secret/platform/polaris …"
POLARIS=$(curl -sf -H "X-Vault-Token: $BAO_TOKEN" \
  "$BAO_ADDR/v1/secret/platform/polaris")
POLARIS_ID=$(echo "$POLARIS"  | jq -r '.data.spark_svc_id')
POLARIS_SEC=$(echo "$POLARIS" | jq -r '.data.spark_svc_secret')

# ── 3. S3 credentials from OpenBao ───────────────────────────────────────────
echo "[INFO] Reading secret/platform/s3 …"
S3=$(curl -sf -H "X-Vault-Token: $BAO_TOKEN" "$BAO_ADDR/v1/secret/platform/s3")
S3_KEY=$(echo "$S3"    | jq -r '.data.access_key')
S3_SEC=$(echo "$S3"    | jq -r '.data.secret_key')
S3_BUCKET=$(echo "$S3" | jq -r '.data.bucket')
S3_REGION=$(echo "$S3" | jq -r '.data.region')

# Schema Registry URL
SR_URL="http://schema-registry.prod.svc.cluster.local:8081"
POLARIS_URI="http://polaris-rest.prod.svc.cluster.local:8181/api/catalog"

# ── 4. Register Iceberg Sink connector ───────────────────────────────────────
echo "[INFO] Registering connector: $CONNECT_NAME …"

curl -sf -X POST "$DEBEZIUM_URL/connectors" \
  -H "Content-Type: application/json" \
  -d "$(cat <<SINK_JSON
{
  "name": "${CONNECT_NAME}",
  "config": {
    "connector.class":  "org.apache.iceberg.connect.IcebergSinkConnector",
    "tasks.max":        "4",

    "topics": "oracle-tpcds.TPCDS.INCOME_BAND,oracle-tpcds.TPCDS.SHIP_MODE,oracle-tpcds.TPCDS.WAREHOUSE,oracle-tpcds.TPCDS.REASON,oracle-tpcds.TPCDS.CALL_CENTER,oracle-tpcds.TPCDS.WEB_SITE,oracle-tpcds.TPCDS.WEB_PAGE,oracle-tpcds.TPCDS.HOUSEHOLD_DEMOGRAPHICS,oracle-tpcds.TPCDS.CATALOG_PAGE,oracle-tpcds.TPCDS.PROMOTION",

    "key.converter":                        "io.confluent.kafka.serializers.KafkaAvroDeserializer",
    "key.converter.schema.registry.url":    "${SR_URL}",
    "value.converter":                      "io.confluent.kafka.serializers.KafkaAvroDeserializer",
    "value.converter.schema.registry.url":  "${SR_URL}",
    "value.converter.specific.avro.reader": "false",

    "iceberg.catalog":                    "${POLARIS_CATALOG_NAME}",
    "iceberg.catalog.type":               "rest",
    "iceberg.catalog.uri":                "${POLARIS_URI}",
    "iceberg.catalog.credential":         "${POLARIS_ID}:${POLARIS_SEC}",
    "iceberg.catalog.scope":              "PRINCIPAL_ROLE:ALL",
    "iceberg.catalog.warehouse":          "${POLARIS_CATALOG_NAME}",
    "iceberg.catalog.client.region":      "${S3_REGION}",
    "iceberg.catalog.s3.access-key-id":   "${S3_KEY}",
    "iceberg.catalog.s3.secret-access-key": "${S3_SEC}",
    "iceberg.catalog.s3.endpoint":        "https://s3.${S3_REGION}.amazonaws.com",

    "iceberg.tables.auto-create-enabled": "true",
    "iceberg.tables.default-namespace":   "${ICEBERG_NAMESPACE}",
    "iceberg.tables.upsert-mode-enabled": "true",
    "iceberg.tables.cdcField":            "op",
    "iceberg.tables.commitIntervalMs":    "5000",

    "iceberg.tables.routeField":  "source.table",
    "iceberg.tables.routeRegex":  "([A-Z_]+)$",

    "iceberg.tables.write.format.default":         "parquet",
    "iceberg.tables.write.target-file-size-bytes": "268435456",

    "iceberg.control.topic":  "iceberg-control-tpcds",
    "iceberg.control.group-id": "iceberg-sink-tpcds-ctrl",

    "consumer.override.auto.offset.reset": "earliest"
  }
}
SINK_JSON
)"

echo ""
echo "[INFO] Iceberg Sink connector registered."
sleep 3
curl -sf "$DEBEZIUM_URL/connectors/$CONNECT_NAME/status" | jq .
