#!/usr/bin/env python3
"""
04_schema_evolution_handler.py
================================
Automatic schema evolution: Debezium DDL → Kafka Schema Registry → Iceberg ALTER TABLE.

Supports all three CDC sources:
  • postgres  — schema-changes.postgres topic, namespace cache_testing
  • oracle    — schema-changes.oracle   topic, namespace tpcds
  • mongodb   — schema-changes.mongodb  topic, namespace cache_testing

How it works
------------
1. Debezium captures DDL events (ADD/DROP/MODIFY COLUMN) and publishes them to
   a schema-changes.<source> topic in JSON format.
2. This script runs as a long-lived multi-source Kafka consumer per source
   (one thread per source topic) or can target a single source via SOURCE env var.
3. For each DDL event:
   a. Parse the Debezium DDL payload to extract table + column changes.
   b. Fetch the new Avro schema from Schema Registry (latest version for the
      affected table topic).
   c. Diff old vs new schema to identify ADD/DROP/MODIFY changes.
   d. Apply matching ALTER TABLE to the Iceberg table via Spark SQL.
   e. Update the Schema Registry subject with the new Avro schema.
4. Running user: dave (can_admin_catalog=true, can_write_iceberg=true).
5. All credentials from OpenBao.

Source-catalog mapping
----------------------
  postgres  → catalog=postgres,  namespace=cache_testing
  oracle    → catalog=oracle,    namespace=tpcds
  mongodb   → catalog=mongodb,   namespace=cache_testing

Supported DDL operations
------------------------
  ADD COLUMN col_name data_type [NOT NULL]
  DROP COLUMN col_name
  MODIFY/ALTER COLUMN col_name new_data_type

Schema Registry integration
----------------------------
After Debezium detects a DDL change it automatically registers a new schema
version in the Schema Registry for the affected topic.  This handler fetches
the new schema, diffs against the cached previous version, and converts the
diff to an Iceberg ALTER TABLE DDL statement.

Usage
-----
  # Watch all three sources concurrently (default):
  SPARK_USER=dave python3 04_schema_evolution_handler.py

  # Watch a single source only:
  SPARK_USER=dave SOURCE=postgres python3 04_schema_evolution_handler.py

  # Dry-run (log changes, do not apply ALTER TABLE):
  DRY_RUN=1 SPARK_USER=dave python3 04_schema_evolution_handler.py
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
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
    format="%(asctime)s [%(threadName)s] [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("schema-evolution")

# ── Config ────────────────────────────────────────────────────────────────────
SPARK_USER  = os.environ.get("SPARK_USER", "dave")
DRY_RUN     = os.environ.get("DRY_RUN", "0") == "1"
SR_URL      = "http://schema-registry.prod.svc.cluster.local:8081"
KAFKA_BOOTSTRAP = "strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092"

# SOURCE: if set, only run for that source; otherwise all three.
_SOURCE_FILTER = os.environ.get("SOURCE", "").lower()

# ── Source descriptor ─────────────────────────────────────────────────────────
# Each entry describes one CDC source:
#   ddl_topic     — schema-changes.<source> topic name
#   catalog       — Iceberg catalog name (= technology name)
#   namespace     — Iceberg namespace (= source database name, lower-cased)
#   topic_prefix  — prefix for data topics (e.g. "postgres.cache_testing.")
#   source_key    — identifier for consumer group + logging
_SOURCES: list[dict[str, str]] = [
    {
        "source_key":   "postgres",
        "ddl_topic":    "schema-changes.postgres",
        "catalog":      "postgres",
        "namespace":    "cache_testing",
        "topic_prefix": "postgres.cache_testing.",
    },
    {
        "source_key":   "oracle",
        "ddl_topic":    "schema-changes.oracle",
        "catalog":      "oracle",
        "namespace":    "tpcds",
        "topic_prefix": "oracle.tpcds.",
    },
    {
        "source_key":   "mongodb",
        "ddl_topic":    "schema-changes.mongodb",
        "catalog":      "mongodb",
        "namespace":    "cache_testing",
        "topic_prefix": "mongodb.cache_testing.",
    },
]

if _SOURCE_FILTER:
    _SOURCES = [s for s in _SOURCES if s["source_key"] == _SOURCE_FILTER]
    if not _SOURCES:
        raise ValueError(
            f"SOURCE={_SOURCE_FILTER!r} is not a known source. "
            "Choose: postgres, oracle, mongodb"
        )

# ── Type maps ─────────────────────────────────────────────────────────────────

_PG_TYPE_MAP: dict[str, str] = {
    "character varying": "string",  "varchar": "string",  "text": "string",
    "char": "string",               "uuid": "string",     "json": "string",
    "jsonb": "string",              "xml": "string",      "citext": "string",
    "integer": "int",               "int": "int",         "int4": "int",
    "smallint": "smallint",         "int2": "smallint",   "bigint": "bigint",
    "int8": "bigint",               "serial": "int",      "bigserial": "bigint",
    "real": "float",                "float4": "float",    "double precision": "double",
    "float8": "double",             "numeric": "decimal(38,10)",
    "decimal": "decimal(38,10)",    "boolean": "boolean", "bool": "boolean",
    "date": "date",                 "timestamp": "timestamp",
    "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamp",
    "time": "string",               "bytea": "binary",
}

_ORA_TYPE_MAP: dict[str, str] = {
    "varchar2": "string",    "varchar": "string",    "char": "string",
    "nchar": "string",       "nvarchar2": "string",  "clob": "string",
    "nclob": "string",       "long": "string",
    "number": "decimal(38,10)", "integer": "bigint", "float": "double",
    "binary_float": "float", "binary_double": "double",
    "smallint": "smallint",  "date": "timestamp",    "timestamp": "timestamp",
    "boolean": "boolean",    "raw": "binary",        "blob": "binary",
}

_MGO_TYPE_MAP: dict[str, str] = {
    "string": "string",   "objectid": "string",   "bindata": "binary",
    "int32": "int",       "int64": "bigint",       "int": "int",
    "double": "double",   "decimal128": "decimal(38,10)",
    "bool": "boolean",    "date": "timestamp",     "timestamp": "timestamp",
    "array": "string",    "object": "string",
}


def _source_type_to_iceberg(source_key: str, type_str: str) -> str:
    """Map a source database type string to an Iceberg DDL type string."""
    if not type_str:
        return "string"
    upper = type_str.upper().strip()
    lower = type_str.lower().strip()
    base  = lower.split("(")[0].strip()

    # Handle DECIMAL/NUMBER with precision/scale
    if base in ("decimal", "numeric", "number") and "(" in lower:
        inner = lower[lower.index("(") + 1: lower.index(")")]
        parts = inner.split(",")
        p = int(parts[0].strip()) if parts[0].strip().isdigit() else 38
        s = int(parts[1].strip()) if len(parts) > 1 and parts[1].strip().isdigit() else 10
        return f"decimal({p},{s})"

    if source_key == "postgres":
        return _PG_TYPE_MAP.get(base, _PG_TYPE_MAP.get(lower, "string"))
    elif source_key == "oracle":
        return _ORA_TYPE_MAP.get(base, _ORA_TYPE_MAP.get(upper.split("(")[0].strip().lower(), "string"))
    elif source_key == "mongodb":
        return _MGO_TYPE_MAP.get(base, "string")
    return "string"


# ── Avro type → Iceberg type ──────────────────────────────────────────────────

def _avro_type_to_iceberg(avro_type: Any) -> str:
    """Convert an Avro type (string, union, or record) to an Iceberg DDL type string."""
    if isinstance(avro_type, list):
        # union like ["null","string"] — take the non-null type
        non_null = [t for t in avro_type if t != "null"]
        avro_type = non_null[0] if non_null else "string"
    if isinstance(avro_type, dict):
        lt = avro_type.get("logicalType", "")
        tp = avro_type.get("type", "")
        if lt in ("timestamp-millis", "timestamp-micros"):
            return "timestamp"
        if lt == "date":
            return "date"
        if lt in ("time-millis", "time-micros"):
            return "string"
        if lt == "decimal":
            p = avro_type.get("precision", 38)
            s = avro_type.get("scale", 10)
            return f"decimal({p},{s})"
        if lt == "uuid":
            return "string"
        return {
            "int":    "int",     "long":   "bigint",
            "float":  "float",   "double": "double",
            "bytes":  "binary",  "string": "string",
            "record": "string",  "array":  "string",
            "map":    "string",
        }.get(tp, "string")
    return {
        "string": "string",  "int":    "int",   "long":   "bigint",
        "float":  "float",   "double": "double","boolean":"boolean",
        "bytes":  "binary",  "null":   "string",
    }.get(str(avro_type), "string")


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


def _update_schema_registry(
    sr_client:  SchemaRegistryClient,
    topic:      str,
    new_schema: dict,
) -> None:
    """
    Register a new schema version for the topic value subject in Schema Registry.

    If the schema is already compatible the call is a no-op (SR deduplicates).
    """
    from confluent_kafka.schema_registry import Schema
    subject = f"{topic}-value"
    try:
        schema_str = json.dumps(new_schema)
        sr_client.register_schema(subject, Schema(schema_str, schema_type="AVRO"))
        logger.info("Schema Registry: registered new schema for subject '%s'.", subject)
    except Exception as exc:
        logger.warning("Could not update Schema Registry subject %s: %s", subject, exc)


# ── Schema diff ───────────────────────────────────────────────────────────────

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


# ── Core evolution applier ────────────────────────────────────────────────────

def apply_schema_evolution(
    spark:      SparkSession,
    builder:    IcebergTableBuilder,
    sr_client:  SchemaRegistryClient,
    source_key: str,
    catalog:    str,
    namespace:  str,
    table_name: str,
    old_schema: dict,
    new_schema: dict,
    cdc_topic:  str,
) -> None:
    """
    Diff old vs new Avro schemas and apply Iceberg ALTER TABLE statements.
    Also updates the Schema Registry subject with the new schema.
    """
    changes = _diff_avro_schemas(old_schema, new_schema)
    if not changes:
        logger.info("[%s/%s] No schema changes detected.", source_key, table_name)
        return

    # Platform snap columns are never subject to evolution
    _snap_cols = {"snap_id", "snap_timestamp"}
    changes = [c for c in changes if c["name"].lower() not in _snap_cols]

    fqn = f"`{catalog}`.`{namespace}`.`{table_name}`"
    logger.info(
        "[%s/%s] Applying %d schema change(s) to %s",
        source_key, table_name, len(changes), fqn,
    )

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

        logger.info("[%s/%s] DDL: %s", source_key, table_name, ddl)

        if DRY_RUN:
            logger.info("[%s/%s] DRY_RUN — not executing.", source_key, table_name)
        else:
            try:
                spark.sql(ddl)
                logger.info(
                    "[%s/%s] Applied: %s %s %s",
                    source_key, table_name, op, col, ice_type,
                )
            except Exception as exc:
                logger.warning(
                    "[%s/%s] ALTER TABLE failed (non-fatal, source may not support this op): %s",
                    source_key, table_name, exc,
                )

    # Update Schema Registry with new schema
    if not DRY_RUN:
        _update_schema_registry(sr_client, cdc_topic, new_schema)


# ── Kafka consumer loop (per-source) ─────────────────────────────────────────

def _run_source_consumer(
    source:    dict[str, str],
    spark:     SparkSession,
    builder:   IcebergTableBuilder,
    bao:       BaoSparkInit,
) -> None:
    """
    Long-lived consumer loop for a single source's schema-changes topic.
    Runs in its own thread.
    """
    source_key = source["source_key"]
    ddl_topic  = source["ddl_topic"]
    catalog    = source["catalog"]
    namespace  = source["namespace"]
    topic_pfx  = source["topic_prefix"]

    thread_name = f"evo-{source_key}"
    threading.current_thread().name = thread_name

    sr_client = SchemaRegistryClient({"url": SR_URL})

    kafka_secret = bao._read_secret("secret/data/platform/kafka")
    kafka_user   = kafka_secret.get("debezium_user",     "debezium-user")
    kafka_pass   = kafka_secret.get("debezium_password", "")

    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "security.protocol":  "SASL_PLAINTEXT",
        "sasl.mechanism":     "SCRAM-SHA-512",
        "sasl.username":      kafka_user,
        "sasl.password":      kafka_pass,
        "group.id":           f"schema-evolution-{source_key}",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": "true",
        # Consumer performance tuning (librdkafka property names)
        "fetch.min.bytes":    "65536",
        "fetch.wait.max.ms":  "500",
    })
    consumer.subscribe([ddl_topic])
    logger.info("[%s] Subscribed to DDL topic: %s", source_key, ddl_topic)

    # Cache: table_name → last known Avro schema dict
    schema_cache: dict[str, dict] = {}

    try:
        while True:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("[%s] Kafka error: %s", source_key, msg.error())
                continue

            try:
                value_bytes = msg.value()
                if value_bytes is None:
                    continue
                event = json.loads(value_bytes.decode("utf-8"))

                # Debezium schema-changes events have "ddl" and "source" fields
                ddl_text = event.get("ddl", "")
                src_meta = event.get("source", {})
                db_table = (
                    src_meta.get("table", "")
                    or src_meta.get("collection", "")  # MongoDB uses 'collection'
                ).lower()

                if not db_table or not ddl_text:
                    continue

                # Only react to ALTER TABLE events
                ddl_upper = ddl_text.upper().strip()
                if "ALTER TABLE" not in ddl_upper and "ALTER COLLECTION" not in ddl_upper:
                    logger.debug("[%s] Ignoring non-ALTER DDL: %.120s", source_key, ddl_text)
                    continue

                logger.info(
                    "[%s] DDL event for table '%s': %.200s",
                    source_key, db_table, ddl_text,
                )

                # Derive the CDC data topic to look up schema in SR
                # e.g. "postgres.cache_testing.customers"
                cdc_topic = f"{topic_pfx}{db_table}"

                old_schema = schema_cache.get(db_table, {})
                new_schema = _get_latest_schema(sr_client, cdc_topic)

                if not new_schema:
                    logger.warning(
                        "[%s/%s] No Avro schema in SR for topic '%s' — skipping.",
                        source_key, db_table, cdc_topic,
                    )
                    continue

                apply_schema_evolution(
                    spark      = spark,
                    builder    = builder,
                    sr_client  = sr_client,
                    source_key = source_key,
                    catalog    = catalog,
                    namespace  = namespace,
                    table_name = db_table,
                    old_schema = old_schema,
                    new_schema = new_schema,
                    cdc_topic  = cdc_topic,
                )

                schema_cache[db_table] = new_schema

            except Exception as exc:
                logger.error(
                    "[%s] Error processing DDL event: %s", source_key, exc, exc_info=True,
                )

    finally:
        consumer.close()
        logger.info("[%s] Consumer closed.", source_key)


# ── Main ──────────────────────────────────────────────────────────────────────

def run(spark: SparkSession, builder: IcebergTableBuilder, bao: BaoSparkInit) -> None:
    """
    Start one consumer thread per configured source.
    Blocks until all threads exit (they run until interrupted).
    """
    if len(_SOURCES) == 1:
        # Single-source mode: run in the main thread (simpler stack traces)
        logger.info("Single-source mode: %s", _SOURCES[0]["source_key"])
        _run_source_consumer(_SOURCES[0], spark, builder, bao)
    else:
        # Multi-source mode: one daemon thread per source
        threads = []
        for source in _SOURCES:
            t = threading.Thread(
                target=_run_source_consumer,
                args=(source, spark, builder, bao),
                name=f"evo-{source['source_key']}",
                daemon=True,
            )
            t.start()
            threads.append(t)
            logger.info("Started consumer thread for source: %s", source["source_key"])

        # Wait for all threads (they run indefinitely)
        for t in threads:
            t.join()


def main() -> None:
    os.environ["SPARK_USER"] = SPARK_USER

    sources_str = ", ".join(s["source_key"] for s in _SOURCES)
    logger.info(
        "=== Schema Evolution Handler | user=%s | sources=[%s] | dry_run=%s ===",
        SPARK_USER, sources_str, DRY_RUN,
    )

    bao   = BaoSparkInit()
    conf  = bao.spark_conf(app_name="schema-evolution-handler")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    builder = IcebergTableBuilder(spark, running_user=SPARK_USER)

    try:
        run(spark, builder, bao)
    except KeyboardInterrupt:
        logger.info("Interrupted — shutting down schema evolution handler.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
