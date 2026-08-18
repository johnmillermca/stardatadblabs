#!/usr/bin/env python3
"""
04_schema_evolution_handler.py
================================
Automatic schema evolution: Oracle DDL → Kafka Schema Registry → Iceberg ALTER TABLE.

How it works
------------
1. Debezium captures Oracle DDL events and publishes them to the schema-changes
   topic (schema-changes.oracle-tpcds) in Avro + Schema Registry format.
2. This script runs as a long-lived Kafka consumer reading that topic.
3. For each DDL event (ADD COLUMN / DROP COLUMN / MODIFY COLUMN):
   a. Parse the Debezium DDL event to extract table + column changes.
   b. Fetch the new Avro schema from Confluent Schema Registry (latest version).
   c. Apply the matching ALTER TABLE statement to the Iceberg table via Spark SQL.
4. Running user: dave (can_admin_catalog=true, can_write_iceberg=true).
5. All credentials from OpenBao.

Supported DDL operations
------------------------
  ADD COLUMN col_name data_type [NOT NULL]
  DROP COLUMN col_name
  MODIFY COLUMN col_name new_data_type  (mapped to ALTER TABLE ALTER COLUMN)

Schema Registry integration
----------------------------
After Debezium detects a DDL change it automatically registers a new schema
version in the Schema Registry for the affected topic.  This handler fetches
that new schema version, diffs it against the previous version, and converts
the diff to an Iceberg ALTER TABLE DDL statement.

Usage
-----
  export SPARK_USER=dave
  python3 04_schema_evolution_handler.py

  # Dry-run (log changes, do not apply ALTER TABLE):
  DRY_RUN=1 python3 04_schema_evolution_handler.py
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType, ByteType, DateType, DecimalType, DoubleType,
    FloatType, IntegerType, LongType, ShortType, StringType, TimestampType,
)

from bao_spark_init import BaoSparkInit
from spark_iceberg_utils import IcebergTableBuilder

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("schema-evolution")

# ── Config ────────────────────────────────────────────────────────────────────
SPARK_USER        = os.environ.get("SPARK_USER", "dave")
ICEBERG_CATALOG   = os.environ.get("ICEBERG_CATALOG", "polaris")
ICEBERG_NAMESPACE = os.environ.get("ICEBERG_NAMESPACE", "tpcds")
DDL_TOPIC         = "schema-changes.oracle-tpcds"
SR_URL            = "http://schema-registry.prod.svc.cluster.local:8081"
KAFKA_BOOTSTRAP   = "strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092"
DRY_RUN           = os.environ.get("DRY_RUN", "0") == "1"

# ── Oracle → Iceberg/Spark type map ──────────────────────────────────────────
_ORA_TYPE_MAP: dict[str, Any] = {
    "VARCHAR2": StringType(),    "VARCHAR": StringType(),
    "CHAR":     StringType(),    "NCHAR":   StringType(),
    "NVARCHAR2":StringType(),    "CLOB":    StringType(),
    "NUMBER":   DecimalType(38,10), "INTEGER": LongType(),
    "FLOAT":    DoubleType(),    "BINARY_FLOAT": FloatType(),
    "BINARY_DOUBLE": DoubleType(), "DATE":   TimestampType(),
    "TIMESTAMP":TimestampType(), "SMALLINT": ShortType(),
    "BOOLEAN":  BooleanType(),
}


def _ora_to_iceberg(type_str: str) -> str:
    """Map Oracle SQL type to Iceberg DDL type string."""
    upper = type_str.upper().strip()
    base  = upper.split("(")[0].strip()
    if base in ("NUMBER", "DECIMAL", "NUMERIC") and "(" in upper:
        inner = upper[upper.index("(") + 1: upper.index(")")]
        parts = inner.split(",")
        p = int(parts[0].strip())
        s = int(parts[1].strip()) if len(parts) > 1 else 0
        return f"decimal({p},{s})"
    mapping = {
        "VARCHAR2": "string",  "VARCHAR": "string",
        "CHAR":     "string",  "NCHAR":   "string",
        "NVARCHAR2":"string",  "CLOB":    "string",
        "NUMBER":   "decimal(38,10)", "INTEGER": "bigint",
        "FLOAT":    "double",  "BINARY_FLOAT": "float",
        "BINARY_DOUBLE": "double", "DATE": "timestamp",
        "TIMESTAMP":"timestamp", "SMALLINT": "smallint",
        "BOOLEAN":  "boolean",
    }
    return mapping.get(base, "string")


# ── Schema Registry helpers ───────────────────────────────────────────────────

def _get_latest_schema(sr_client: SchemaRegistryClient, topic: str, is_key: bool = False) -> dict:
    """Fetch the latest Avro schema for a topic from Schema Registry."""
    suffix = "-key" if is_key else "-value"
    subject = f"{topic}{suffix}"
    try:
        schema = sr_client.get_latest_version(subject)
        return json.loads(schema.schema.schema_str)
    except Exception as exc:
        logger.warning("Could not fetch schema for subject %s: %s", subject, exc)
        return {}


def _diff_avro_schemas(old_schema: dict, new_schema: dict) -> list[dict]:
    """
    Diff two Avro record schemas.
    Returns a list of change dicts:
      {"op": "add",    "name": "col", "avro_type": ...}
      {"op": "remove", "name": "col"}
      {"op": "modify", "name": "col", "avro_type": ...}
    """
    def _fields_map(schema: dict) -> dict[str, Any]:
        return {f["name"]: f["type"] for f in schema.get("fields", [])}

    old_fields = _fields_map(old_schema)
    new_fields = _fields_map(new_schema)
    changes = []
    for name, avro_type in new_fields.items():
        if name not in old_fields:
            changes.append({"op": "add", "name": name, "avro_type": avro_type})
        elif old_fields[name] != avro_type:
            changes.append({"op": "modify", "name": name, "avro_type": avro_type})
    for name in old_fields:
        if name not in new_fields:
            changes.append({"op": "remove", "name": name})
    return changes


def _avro_type_to_iceberg(avro_type: Any) -> str:
    """Convert an Avro type (string or union) to an Iceberg DDL type string."""
    if isinstance(avro_type, list):
        # union like ["null","string"] — take the non-null type
        non_null = [t for t in avro_type if t != "null"]
        avro_type = non_null[0] if non_null else "string"
    if isinstance(avro_type, dict):
        lt = avro_type.get("logicalType", "")
        tp = avro_type.get("type", "")
        if lt in ("timestamp-millis", "timestamp-micros", "date"):
            return "timestamp"
        if lt == "decimal":
            p = avro_type.get("precision", 38)
            s = avro_type.get("scale", 10)
            return f"decimal({p},{s})"
        return {"int": "int", "long": "bigint", "float": "float",
                "double": "double", "bytes": "binary", "string": "string"}.get(tp, "string")
    return {
        "string": "string",  "int":    "int",   "long":   "bigint",
        "float":  "float",   "double": "double","boolean":"boolean",
        "bytes":  "binary",  "null":   "string",
    }.get(str(avro_type), "string")


# ── Core evolution applier ────────────────────────────────────────────────────

def apply_schema_evolution(
    spark:      SparkSession,
    builder:    IcebergTableBuilder,
    sr_client:  SchemaRegistryClient,
    table_name: str,          # lower-case Iceberg table name, e.g. "income_band"
    old_schema: dict,
    new_schema: dict,
) -> None:
    """
    Diff old vs new Avro schemas and apply Iceberg ALTER TABLE statements.
    """
    changes = _diff_avro_schemas(old_schema, new_schema)
    if not changes:
        logger.info("[%s] No schema changes detected.", table_name)
        return

    fqn = f"`{ICEBERG_CATALOG}`.`{ICEBERG_NAMESPACE}`.`{table_name}`"
    for chg in changes:
        op        = chg["op"]
        col       = chg["name"]
        ice_type  = _avro_type_to_iceberg(chg.get("avro_type", "string")) if op != "remove" else ""

        if op == "add":
            ddl = f"ALTER TABLE {fqn} ADD COLUMN `{col}` {ice_type}"
        elif op == "remove":
            ddl = f"ALTER TABLE {fqn} DROP COLUMN `{col}`"
        elif op == "modify":
            ddl = f"ALTER TABLE {fqn} ALTER COLUMN `{col}` TYPE {ice_type}"
        else:
            continue

        logger.info("[%s] Schema change (%s): %s", table_name, op, ddl)

        if DRY_RUN:
            logger.info("[%s] DRY_RUN — not executing.", table_name)
        else:
            spark.sql(ddl)
            logger.info("[%s] ALTER TABLE applied: %s %s %s", table_name, op, col, ice_type)


# ── Kafka consumer loop ───────────────────────────────────────────────────────

def run(spark: SparkSession, builder: IcebergTableBuilder, bao: BaoSparkInit) -> None:
    """Main consumer loop."""
    # Schema Registry client
    sr_client = SchemaRegistryClient({"url": SR_URL})

    # Kafka consumer — Kerberos not required here because we run inside the cluster
    # on the SCRAM-SHA-512 port 9092.  Credentials from OpenBao.
    kafka_secret = bao._read_secret("secret/platform/kafka")
    kafka_user   = kafka_secret.get("debezium_user",     "debezium-user")
    kafka_pass   = kafka_secret.get("debezium_password", "")

    consumer = Consumer({
        "bootstrap.servers":     KAFKA_BOOTSTRAP,
        "security.protocol":     "SASL_PLAINTEXT",
        "sasl.mechanism":        "SCRAM-SHA-512",
        "sasl.username":         kafka_user,
        "sasl.password":         kafka_pass,
        "group.id":              "schema-evolution-handler",
        "auto.offset.reset":     "earliest",
        "enable.auto.commit":    "true",
    })
    consumer.subscribe([DDL_TOPIC])
    logger.info("Subscribed to topic: %s", DDL_TOPIC)

    # Cache: table_name → last known Avro schema
    schema_cache: dict[str, dict] = {}

    try:
        while True:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka error: %s", msg.error())
                continue

            try:
                # Deserialise the DDL history event (JSON, not Avro)
                value_bytes = msg.value()
                if value_bytes is None:
                    continue
                event = json.loads(value_bytes.decode("utf-8"))

                # Debezium schema-changes events have a "databaseName" / "ddl" field
                ddl_text  = event.get("ddl", "")
                source    = event.get("source", {})
                db_table  = source.get("table", "").lower()

                if not db_table or not ddl_text:
                    continue

                # Only react to ALTER TABLE events
                ddl_upper = ddl_text.upper().strip()
                if "ALTER TABLE" not in ddl_upper:
                    logger.debug("Ignoring non-ALTER DDL: %s", ddl_text[:120])
                    continue

                logger.info("DDL event for table '%s': %s", db_table, ddl_text[:200])

                # Fetch old + new Avro schema from Schema Registry
                cdc_topic   = f"oracle-tpcds.TPCDS.{db_table.upper()}"
                old_schema  = schema_cache.get(db_table, {})
                new_schema  = _get_latest_schema(sr_client, cdc_topic)

                if not new_schema:
                    logger.warning("[%s] No Avro schema found in SR — skipping.", db_table)
                    continue

                apply_schema_evolution(
                    spark       = spark,
                    builder     = builder,
                    sr_client   = sr_client,
                    table_name  = db_table,
                    old_schema  = old_schema,
                    new_schema  = new_schema,
                )

                # Update cache
                schema_cache[db_table] = new_schema

            except Exception as exc:
                logger.error("Error processing DDL event: %s", exc, exc_info=True)

    finally:
        consumer.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    os.environ["SPARK_USER"] = SPARK_USER

    logger.info("=== Schema Evolution Handler | user=%s | namespace=%s | dry_run=%s ===",
                SPARK_USER, ICEBERG_NAMESPACE, DRY_RUN)

    bao   = BaoSparkInit()
    conf  = bao.spark_conf(app_name="schema-evolution-handler")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    builder = IcebergTableBuilder(spark, running_user=SPARK_USER)

    try:
        run(spark, builder, bao)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
