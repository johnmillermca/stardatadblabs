#!/usr/bin/env python3
"""
05_kafka_to_iceberg_streaming.py
================================
Spark Structured Streaming consumer — Debezium CDC → Iceberg via Polaris REST.

Reads CDC events from all three source Kafka topic patterns, deserialises
Avro payloads via Confluent Schema Registry, and writes INSERT/UPDATE/DELETE
events to Iceberg format-version 2 tables with snap_id + snap_timestamp columns.

Source → topic → Iceberg target mapping
----------------------------------------
  postgres  : postgres.cache_testing.*  → postgres.cache_testing.<table>
  oracle    : oracle.tpcds.*            → oracle.tpcds.<table>
  mongodb   : mongodb.cache_testing.*   → mongodb.cache_testing.<table>

Architecture
------------
• One Spark Structured Streaming query per source (three queries total), each
  reading from a regex topic pattern.
• Avro deserialisation via from_avro() with Schema Registry integration.
• Each micro-batch: flatten Debezium envelope, inject snap_id + snap_timestamp,
  route rows to the correct Iceberg table based on the Kafka topic name.
• Checkpoint: S3 (s3://xdatatoiceberg1/checkpoints/streaming/<source>)
• Schema evolution: DDL change events on schema-changes.<source> are handled
  by 04_schema_evolution_handler.py running as a separate process.  The
  streaming consumer handles schema evolution inline via mergeSchema=true on
  the writeTo().append() call — unknown columns are silently added.
• Iceberg format-version 2, parquet, 256 MB target file size.
• Partition spec: hours(snap_timestamp) + bucket(16, <pk_col>)

Performance tuning
------------------
• Kafka source: startingOffsets=latest (CDC, not replay), maxOffsetsPerTrigger=50000
• Spark streaming trigger: ProcessingTime 10 seconds
• Debezium Avro payload size is small (~500B avg); 50k msgs/trigger is safe.
• lz4 compression on Kafka → decompressed in Spark executor before from_avro().

Credentials
-----------
All from OpenBao via BaoSparkInit — never hardcoded.

Usage
-----
  # Run all three sources (default):
  SPARK_USER=dave python3 05_kafka_to_iceberg_streaming.py

  # Run a single source only:
  SPARK_USER=dave SOURCE=postgres python3 05_kafka_to_iceberg_streaming.py

  # Dry-run (start stream, do not write to Iceberg):
  DRY_RUN=1 SPARK_USER=dave python3 05_kafka_to_iceberg_streaming.py
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, current_timestamp, from_json, lit,
    monotonically_increasing_id, schema_of_json,
    struct, to_json, udf,
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    BooleanType, LongType, StringType, StructField, StructType, TimestampType,
)

from bao_spark_init import BaoSparkInit
from spark_iceberg_utils import IcebergTableBuilder
from importlib import import_module as _imod

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("kafka-to-iceberg")

# ── Config ────────────────────────────────────────────────────────────────────
SPARK_USER = os.environ.get("SPARK_USER", "dave")
DRY_RUN    = os.environ.get("DRY_RUN", "0") == "1"
_SOURCE_FILTER = os.environ.get("SOURCE", "").lower()

KAFKA_BOOTSTRAP = "strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092"
SR_URL          = "http://schema-registry.prod.svc.cluster.local:8081"
S3_BUCKET       = "xdatatoiceberg1"

# Streaming micro-batch trigger interval
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "10 seconds")
# Max Kafka offsets consumed per trigger (back-pressure)
MAX_OFFSETS_PER_TRIGGER = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "50000"))


# ── Source descriptor ─────────────────────────────────────────────────────────

class _StreamingSource:
    """Describes a single CDC source for the streaming pipeline."""
    def __init__(
        self,
        source_key:    str,
        topic_pattern: str,        # regex: subscribed by Kafka source
        catalog:       str,
        namespace:     str,
        pk_col:        str,        # primary key column name for bucket partitioning
        s3_prefix:     str,        # path under s3://<bucket>/
    ) -> None:
        self.source_key    = source_key
        self.topic_pattern = topic_pattern
        self.catalog       = catalog
        self.namespace     = namespace
        self.pk_col        = pk_col
        self.s3_prefix     = s3_prefix
        self.checkpoint    = (
            f"s3://{S3_BUCKET}/checkpoints/streaming/{source_key}"
        )


_ALL_SOURCES: list[_StreamingSource] = [
    _StreamingSource(
        source_key    = "postgres",
        topic_pattern = "postgres\\.cache_testing\\..*",
        catalog       = "postgres",
        namespace     = "cache_testing",
        pk_col        = "id",
        s3_prefix     = "iceberg/pg_lakehouse",
    ),
    _StreamingSource(
        source_key    = "oracle",
        topic_pattern = "oracle\\.tpcds\\..*",
        catalog       = "oracle",
        namespace     = "tpcds",
        pk_col        = "id",
        s3_prefix     = "iceberg/ora_lakehouse",
    ),
    _StreamingSource(
        source_key    = "mongodb",
        topic_pattern = "mongodb\\.cache_testing\\..*",
        catalog       = "mongodb",
        namespace     = "cache_testing",
        pk_col        = "_id",
        s3_prefix     = "iceberg/mgo_lakehouse",
    ),
]

if _SOURCE_FILTER:
    _ALL_SOURCES = [s for s in _ALL_SOURCES if s.source_key == _SOURCE_FILTER]
    if not _ALL_SOURCES:
        print(
            f"ERROR: SOURCE={_SOURCE_FILTER!r} not known. "
            "Choose: postgres, oracle, mongodb",
            file=sys.stderr,
        )
        sys.exit(1)


# ── Debezium envelope schema ───────────────────────────────────────────────────
# Debezium wraps CDC events in an envelope:
#   { "before": {...}, "after": {...}, "op": "c/u/d/r", "source": {...} }
# We use a string-typed schema here (payload arrives as JSON string after
# Avro deserialisation because Debezium uses a nested JSON-in-Avro pattern
# for the payload fields when schema.registry is in use).
# We flatten by parsing the "after" field (INSERT/UPDATE) or "before" (DELETE).

_DEBEZIUM_ENVELOPE_SCHEMA = StructType([
    StructField("before",  StringType(), True),   # JSON string of before image
    StructField("after",   StringType(), True),   # JSON string of after  image
    StructField("op",      StringType(), True),   # c=create, u=update, d=delete, r=read
    StructField("source",  StringType(), True),   # JSON string with source metadata
    StructField("ts_ms",   LongType(),   True),   # event timestamp in millis
])


# ── Schema Registry Avro helper ───────────────────────────────────────────────

def _build_avro_deserialize_udf(sr_client: Any) -> Any:
    """
    Build a Python UDF that deserialises a Confluent Avro-encoded byte array
    (with the 5-byte magic header) to a JSON string.

    The UDF is registered as 'avro_to_json' and called per-row.
    This approach avoids the spark-avro from_avro() limitation of requiring a
    static schema — instead it looks up the schema ID from the message header
    dynamically, as Confluent Schema Registry clients do.

    Note: UDFs are serialised to each executor. The SchemaRegistryClient is
    lightweight (HTTP calls are lazy and cached per-instance) so the per-
    executor startup cost is low.
    """
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.serialization import MessageField, SerializationContext
    import io, struct as _struct

    _sr = SchemaRegistryClient({"url": SR_URL})

    def avro_to_json(topic: str, raw_bytes: bytes) -> str | None:
        if raw_bytes is None:
            return None
        try:
            # Confluent wire format: magic byte (0x00) + 4-byte schema ID + avro payload
            if len(raw_bytes) < 5 or raw_bytes[0] != 0:
                return raw_bytes.decode("utf-8", errors="replace")
            schema_id = _struct.unpack(">I", raw_bytes[1:5])[0]
            registered = _sr.get_schema(schema_id)
            import avro.io as _aio
            import avro.schema as _aschema
            schema_def = _aschema.parse(registered.schema_str)
            decoder = _aio.BinaryDecoder(io.BytesIO(raw_bytes[5:]))
            reader  = _aio.DatumReader(schema_def)
            record  = reader.read(decoder)
            return json.dumps(record)
        except Exception as exc:
            logger.warning("avro_to_json failed for topic %s: %s", topic, exc)
            return None

    return udf(avro_to_json, StringType())


# ── Table routing ─────────────────────────────────────────────────────────────

def _topic_to_table(topic: str, source: _StreamingSource) -> str:
    """
    Derive the Iceberg table name from a Kafka topic.
    e.g. "postgres.cache_testing.customers" → "customers"
    """
    prefix = source.topic_pattern.replace("\\.", ".").replace(".*", "")
    return topic.replace(prefix, "").lower()


# ── Micro-batch writer ────────────────────────────────────────────────────────

def _write_micro_batch(
    spark:   SparkSession,
    builder: IcebergTableBuilder,
    source:  _StreamingSource,
) -> Any:
    """
    Return a foreachBatch function for the given source.

    The returned function is called by Spark Structured Streaming for each
    micro-batch DataFrame.  It:
      1. Flattens the Debezium envelope (parses "after" / "before" JSON).
      2. Routes rows to their Iceberg tables by Kafka topic.
      3. Creates the target Iceberg table if it does not exist yet (idempotent).
      4. Appends rows with snap_id + snap_timestamp injected.
    """
    def _foreach_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        if DRY_RUN:
            count = batch_df.count()
            logger.info(
                "[%s] DRY_RUN batch_id=%d rows=%d — skipping write.",
                source.source_key, batch_id, count,
            )
            return

        # Deduplicate by topic to minimise schema-inference calls
        try:
            topics = [r[0] for r in batch_df.select("topic").distinct().collect()]
        except Exception:
            logger.warning("[%s] Could not collect topics.", source.source_key)
            return

        for topic in topics:
            table_name = _topic_to_table(topic, source)
            if not table_name:
                continue

            topic_df = batch_df.filter(col("topic") == topic)

            # ── Extract Debezium payload ─────────────────────────────────────
            # value is a binary Avro payload; we parse the JSON string from value_json.
            # The Avro deserialization UDF is applied upstream in the stream setup;
            # here we work with the 'value_json' string column.
            env_df = topic_df.select(
                from_json(col("value_json"), _DEBEZIUM_ENVELOPE_SCHEMA).alias("env"),
                col("topic"),
                col("timestamp").alias("kafka_ts"),
            )

            # Filter: skip tombstone (null value) and schema-change events
            env_df = env_df.filter(col("env").isNotNull())

            # For INSERT/UPDATE: use "after"; for DELETE: use "before"
            # op: c=create, u=update, r=read(snapshot), d=delete
            payload_df = env_df.select(
                F.when(
                    col("env.op").isin("c", "u", "r"),
                    col("env.after"),
                ).when(
                    col("env.op") == "d",
                    col("env.before"),
                ).alias("payload_json"),
                col("env.op").alias("_op"),
                col("kafka_ts"),
            ).filter(col("payload_json").isNotNull())

            if payload_df.isEmpty():
                continue

            # ── Infer schema from a sample payload ───────────────────────────
            # Use schema_of_json on the first non-null payload to infer the
            # target schema.  This is safe because Debezium schema is stable
            # within a single micro-batch (schema changes trigger a separate
            # schema-changes event handled by 04_schema_evolution_handler.py).
            try:
                sample_json = payload_df.select("payload_json").limit(1).collect()[0][0]
                if sample_json is None:
                    continue
                inferred_schema = spark.read.json(
                    payload_df.select("payload_json").rdd.map(lambda r: r[0])
                ).schema
            except Exception as exc:
                logger.warning(
                    "[%s/%s] Could not infer schema: %s — skipping batch.",
                    source.source_key, table_name, exc,
                )
                continue

            # ── Create Iceberg table if needed ────────────────────────────────
            fqn_backtick = f"`{source.catalog}`.`{source.namespace}`.`{table_name}`"
            fqn_plain    = f"{source.catalog}.{source.namespace}.{table_name}"
            try:
                spark.sql(f"DESCRIBE TABLE {fqn_backtick}")
                table_exists = True
            except Exception:
                table_exists = False

            if not table_exists:
                # Derive partition key: try source.pk_col, fall back to snap_id
                pk_col_exists = any(
                    f.name.lower() == source.pk_col.lower()
                    for f in inferred_schema.fields
                )
                pk_for_bucket = source.pk_col if pk_col_exists else "snap_id"
                partition_spec = [
                    IcebergTableBuilder.hours("snap_timestamp"),
                    IcebergTableBuilder.bucket(pk_for_bucket, 16),
                ]
                s3_location = (
                    f"s3://{S3_BUCKET}/{source.s3_prefix}"
                    f"/{source.namespace}/{table_name}"
                )
                try:
                    builder.create_table(
                        catalog        = source.catalog,
                        namespace      = source.namespace,
                        table          = table_name,
                        schema         = inferred_schema,
                        partition_spec = partition_spec,
                        location       = s3_location,
                    )
                    logger.info(
                        "[%s/%s] Created Iceberg table at %s.",
                        source.source_key, table_name, fqn_backtick,
                    )
                except Exception as create_exc:
                    logger.warning(
                        "[%s/%s] Table creation failed (may already exist): %s",
                        source.source_key, table_name, create_exc,
                    )

            # ── Parse payload JSON → DataFrame ────────────────────────────────
            try:
                row_df = payload_df.select(
                    from_json(col("payload_json"), inferred_schema).alias("data"),
                ).select("data.*")
            except Exception as exc:
                logger.warning(
                    "[%s/%s] JSON parse failed: %s", source.source_key, table_name, exc,
                )
                continue

            # ── Inject snap_id + snap_timestamp ──────────────────────────────
            final_df = (
                row_df
                .withColumn("snap_id",        monotonically_increasing_id().cast(LongType()))
                .withColumn("snap_timestamp", current_timestamp())
            )

            # ── Write to Iceberg ──────────────────────────────────────────────
            try:
                (
                    final_df
                    .writeTo(fqn_plain)
                    .option("mergeSchema", "true")
                    .append()
                )
                logger.info(
                    "[%s/%s] batch_id=%d rows=%d written.",
                    source.source_key, table_name, batch_id, final_df.count(),
                )
            except Exception as write_exc:
                logger.error(
                    "[%s/%s] Write failed: %s", source.source_key, table_name, write_exc,
                )

    return _foreach_batch


# ── Stream builder ────────────────────────────────────────────────────────────

def _start_source_stream(
    spark:   SparkSession,
    builder: IcebergTableBuilder,
    source:  _StreamingSource,
    bao:     BaoSparkInit,
) -> StreamingQuery:
    """
    Build and start the Structured Streaming query for a single CDC source.

    Pipeline:
      Kafka (Avro) → from_avro UDF → flatten envelope → foreachBatch → Iceberg
    """
    kafka_secret = bao.kafka_creds()
    kafka_user   = kafka_secret.get("debezium_user",     "debezium-user")
    kafka_pass   = kafka_secret.get("debezium_password", "")

    jaas_cfg = (
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
        f"username=\"{kafka_user}\" password=\"{kafka_pass}\";"
    )

    kafka_options = {
        "kafka.bootstrap.servers":                    KAFKA_BOOTSTRAP,
        "kafka.security.protocol":                    "SASL_PLAINTEXT",
        "kafka.sasl.mechanism":                       "SCRAM-SHA-512",
        "kafka.sasl.jaas.config":                     jaas_cfg,
        "subscribePattern":                           source.topic_pattern,
        "startingOffsets":                            "latest",
        "maxOffsetsPerTrigger":                       str(MAX_OFFSETS_PER_TRIGGER),
        "failOnDataLoss":                             "false",
        # Consumer performance tuning
        "kafka.fetch.min.bytes":                      "65536",
        "kafka.fetch.wait.max.ms":                    "500",
        "kafka.max.poll.records":                     "500",
    }

    raw_stream = (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .load()
    )

    # Apply the Avro deserialisation UDF to the binary value column
    # The UDF converts the Confluent Avro wire-format bytes → JSON string
    # We register it as a non-deterministic UDF (schema may change per message)
    try:
        from confluent_kafka.schema_registry import SchemaRegistryClient
        _sr = SchemaRegistryClient({"url": SR_URL})
        avro_udf = _build_avro_deserialize_udf(_sr)
    except ImportError:
        # confluent-kafka not available — fall back to treating value as UTF-8 JSON
        logger.warning(
            "[%s] confluent-kafka not available; treating Kafka values as plain JSON.",
            source.source_key,
        )
        avro_udf = udf(
            lambda topic, b: b.decode("utf-8", errors="replace") if b else None,
            StringType(),
        )

    decoded_stream = raw_stream.withColumn(
        "value_json",
        avro_udf(col("topic").cast(StringType()), col("value")),
    )

    query = (
        decoded_stream
        .writeStream
        .queryName(f"cdc-{source.source_key}")
        .foreachBatch(_write_micro_batch(spark, builder, source))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", source.checkpoint)
        .start()
    )

    logger.info(
        "[%s] Streaming query started | pattern=%s | checkpoint=%s",
        source.source_key, source.topic_pattern, source.checkpoint,
    )
    return query


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    os.environ["SPARK_USER"] = SPARK_USER

    logger.info(
        "=== Kafka→Iceberg Streaming | user=%s | sources=%s | dry_run=%s ===",
        SPARK_USER,
        [s.source_key for s in _ALL_SOURCES],
        DRY_RUN,
    )

    bao  = BaoSparkInit()
    conf = bao.spark_conf(app_name="kafka-to-iceberg-streaming")

    # Add spark-sql-kafka connector JAR (must be in image at this path)
    # spark-sql-kafka-0-10 is typically part of the Spark distribution
    conf.set("spark.jars.packages",
             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ── Catalog bootstrap pre-flight ──────────────────────────────────────────
    try:
        cb = _imod("00_catalog_bootstrap")
        cb.bootstrap_all_catalogs(spark, bao, fail_fast=False)
    except Exception as exc:
        logger.warning("[bootstrap] Pre-flight warning: %s", exc)

    builder = IcebergTableBuilder(spark, running_user=SPARK_USER)

    # Ensure namespaces exist for all active sources
    for src in _ALL_SOURCES:
        try:
            builder.ensure_namespace(src.catalog, src.namespace)
        except Exception as exc:
            logger.warning("[%s] Could not ensure namespace: %s", src.source_key, exc)

    # ── Start streaming queries ───────────────────────────────────────────────
    queries: list[StreamingQuery] = []
    for src in _ALL_SOURCES:
        try:
            q = _start_source_stream(spark, builder, src, bao)
            queries.append(q)
        except Exception as exc:
            logger.error("[%s] Failed to start stream: %s", src.source_key, exc, exc_info=True)

    if not queries:
        logger.error("No streaming queries started — exiting.")
        sys.exit(1)

    logger.info("All %d streaming queries active. Awaiting termination …", len(queries))

    # Block until all queries complete (or one terminates with an error)
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Interrupted — stopping all queries.")
        for q in queries:
            try:
                q.stop()
            except Exception:
                pass
    finally:
        spark.stop()
        logger.info("Streaming job stopped.")


if __name__ == "__main__":
    main()
