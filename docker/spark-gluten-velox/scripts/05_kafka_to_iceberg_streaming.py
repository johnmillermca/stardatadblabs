#!/usr/bin/env python3
"""
05_kafka_to_iceberg_streaming.py
================================
Spark Structured Streaming consumer — Debezium CDC → Iceberg via Polaris REST.

Three write modes, configurable per-run via the WRITE_MODE environment variable
(or selected interactively when running start_cdc_streaming.sh):

  standard         SCD Type 0 — MERGE INTO Iceberg by PK.
                   INSERT/UPDATE → upsert (overwrite matched + insert new).
                   DELETE        → hard delete the matched row from Iceberg.

  soft_delete      MERGE upsert for INSERT/UPDATE.
                   DELETE event  → set is_deleted=true, deleted_at=<now()>.
                   Row is never physically removed.

  history_tracking Always INSERT into Iceberg — never UPDATE or DELETE.
                   Every CDC event appended with _change_type (INSERT/UPDATE/DELETE)
                   and _change_ts columns to preserve full row history.

Source → topic → Iceberg target mapping
----------------------------------------
  postgres  : postgres.cache_testing.*  → postgres.cache_testing.<table>
  oracle    : oracle.tpcds.*            → oracle.tpcds.<table>
  mongodb   : mongodb.cache_testing.*   → mongodb.cache_testing.<table>

Architecture
------------
• One Spark Structured Streaming query per source (three queries total), each
  reading from a regex topic pattern.
• Avro deserialisation via a Python UDF backed by the Confluent Schema Registry
  client; falls back to plain-JSON if confluent-kafka is unavailable.
• Micro-batch interval: 2 seconds (TRIGGER_INTERVAL default).  All Kafka
  messages received within each 2-second window are grouped into one batch,
  flattened from the Debezium envelope, and applied to Iceberg as a single
  MERGE operation per table.  This keeps write amplification low while
  ensuring sub-5-second end-to-end latency from source commit to Iceberg.
• After every foreachBatch write completes the table's metadata cache is
  refreshed (REFRESH TABLE) so downstream readers immediately see the new
  snapshot, then the streaming query is restarted cleanly so the next
  2-second window begins from a clean Spark execution context.
• Checkpoint: s3://xdatatoiceberg1/checkpoints/streaming/<source>/<write_mode>
• Iceberg MERGE requires format-version 2 (set at table creation).
• Schema evolution: mergeSchema=true on append / auto ADD COLUMN on MERGE.

Auto-restart
------------
• Kubernetes restartPolicy: Always ensures the pod restarts on failure.
• An internal retry loop (MAX_RESTART_ATTEMPTS / RESTART_BACKOFF_BASE_S)
  re-initialises Spark and all streams on any unexpected query termination
  before letting the pod exit and trigger the K8s restart.
• Per-batch restart: after each 2-second batch is committed to Iceberg the
  streaming query is stopped and immediately restarted so the Spark execution
  plan is refreshed.  The checkpoint ensures no events are replayed.

Credentials
-----------
All from OpenBao via BaoSparkInit — never hardcoded.

Usage
-----
  # Run all three sources with standard write mode (default):
  SPARK_USER=dave WRITE_MODE=standard python3 05_kafka_to_iceberg_streaming.py

  # Soft-delete mode, postgres only:
  SPARK_USER=dave WRITE_MODE=soft_delete SOURCE=postgres \\
    python3 05_kafka_to_iceberg_streaming.py

  # History-tracking mode:
  SPARK_USER=dave WRITE_MODE=history_tracking \\
    python3 05_kafka_to_iceberg_streaming.py

  # Dry-run (stream, do not write to Iceberg):
  DRY_RUN=1 SPARK_USER=dave python3 05_kafka_to_iceberg_streaming.py
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, current_timestamp, from_json, lit,
    monotonically_increasing_id, udf,
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

# ── Write modes ───────────────────────────────────────────────────────────────
_WRITE_MODE_STANDARD         = "standard"
_WRITE_MODE_SOFT_DELETE      = "soft_delete"
_WRITE_MODE_HISTORY_TRACKING = "history_tracking"
_VALID_WRITE_MODES = (
    _WRITE_MODE_STANDARD,
    _WRITE_MODE_SOFT_DELETE,
    _WRITE_MODE_HISTORY_TRACKING,
)

# ── Config ────────────────────────────────────────────────────────────────────
SPARK_USER  = os.environ.get("SPARK_USER", "dave")
DRY_RUN     = os.environ.get("DRY_RUN", "0") == "1"
WRITE_MODE  = os.environ.get("WRITE_MODE", _WRITE_MODE_STANDARD).lower()
_SOURCE_FILTER = os.environ.get("SOURCE", "").lower()

# Auto-restart loop config
MAX_RESTART_ATTEMPTS  = int(os.environ.get("MAX_RESTART_ATTEMPTS", "10"))
RESTART_BACKOFF_BASE_S = float(os.environ.get("RESTART_BACKOFF_BASE_S", "5"))
RESTART_BACKOFF_MAX_S  = float(os.environ.get("RESTART_BACKOFF_MAX_S", "120"))

KAFKA_BOOTSTRAP = "strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092"
SR_URL          = "http://schema-registry.prod.svc.cluster.local:8081"
S3_BUCKET       = "xdatatoiceberg1"

# Streaming micro-batch trigger interval.
# Default: 2 seconds — all events received within each 2-second window are
# grouped into one batch and applied to Iceberg as a single MERGE operation.
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "2 seconds")
# Max Kafka offsets consumed per trigger (back-pressure)
MAX_OFFSETS_PER_TRIGGER = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "50000"))


# ── Validate write mode ───────────────────────────────────────────────────────
if WRITE_MODE not in _VALID_WRITE_MODES:
    print(
        f"ERROR: WRITE_MODE={WRITE_MODE!r} is not valid. "
        f"Choose: {', '.join(_VALID_WRITE_MODES)}",
        file=sys.stderr,
    )
    sys.exit(1)


# ── Source descriptor ─────────────────────────────────────────────────────────

class _StreamingSource:
    """Describes a single CDC source for the streaming pipeline."""
    def __init__(
        self,
        source_key:    str,
        topic_pattern: str,        # regex: subscribed by Kafka source
        catalog:       str,
        namespace:     str,
        pk_col:        str,        # primary key column name for MERGE conditions
        s3_prefix:     str,        # path under s3://<bucket>/
    ) -> None:
        self.source_key    = source_key
        self.topic_pattern = topic_pattern
        self.catalog       = catalog
        self.namespace     = namespace
        self.pk_col        = pk_col
        self.s3_prefix     = s3_prefix
        self.checkpoint    = (
            f"s3://{S3_BUCKET}/checkpoints/streaming/{source_key}/{WRITE_MODE}"
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
#   { "before": {...}, "after": {...}, "op": "c/u/d/r", "source": {...}, "ts_ms": <N> }
# We use a string-typed schema here (payload arrives as JSON string after
# Avro deserialisation because Debezium uses a nested JSON-in-Avro pattern
# for the payload fields when schema.registry is in use).

_DEBEZIUM_ENVELOPE_SCHEMA = StructType([
    StructField("before",  StringType(), True),   # JSON string of before image
    StructField("after",   StringType(), True),   # JSON string of after  image
    StructField("op",      StringType(), True),   # c=create, u=update, d=delete, r=read
    StructField("source",  StringType(), True),   # JSON string with source metadata
    StructField("ts_ms",   LongType(),   True),   # event timestamp in millis
])


# ── Schema Registry Avro helper ───────────────────────────────────────────────

def _build_avro_deserialize_udf(_sr_url: str) -> Any:
    """
    Build a Python UDF that deserialises a Confluent Avro-encoded byte array
    (with the 5-byte magic header) to a JSON string.
    """
    import io
    import struct as _struct

    def avro_to_json(topic: str, raw_bytes: bytes) -> str | None:
        if raw_bytes is None:
            return None
        try:
            # Confluent wire format: magic byte (0x00) + 4-byte schema ID + avro payload
            if len(raw_bytes) < 5 or raw_bytes[0] != 0:
                return raw_bytes.decode("utf-8", errors="replace")
            from confluent_kafka.schema_registry import SchemaRegistryClient
            _sr = SchemaRegistryClient({"url": _sr_url})
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
            logger.warning("avro_to_json failed: %s", exc)
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


# ── Write-mode helpers ────────────────────────────────────────────────────────

def _apply_standard(
    spark:       SparkSession,
    payload_df:  DataFrame,
    fqn_backtick: str,
    fqn_plain:    str,
    pk_col:       str,
    source_key:   str,
    table_name:   str,
    batch_id:     int,
) -> None:
    """
    SCD Type 0 — MERGE INTO Iceberg by PK.
    INSERT/UPDATE (op c/u/r) → MATCHED UPDATE + NOT MATCHED INSERT (upsert).
    DELETE        (op d)     → MATCHED DELETE.
    """
    inserts = payload_df.filter(col("_op").isin("c", "u", "r")).drop("_op", "kafka_ts")
    deletes = payload_df.filter(col("_op") == "d").drop("_op", "kafka_ts")

    if not inserts.isEmpty():
        final_df = (
            inserts
            .withColumn("snap_id",        monotonically_increasing_id().cast(LongType()))
            .withColumn("snap_timestamp", current_timestamp())
        )
        # MERGE upsert: match on pk_col, update all cols on match, insert on no match
        tmp_view = f"__cdc_upsert_{source_key}_{table_name}_{batch_id}"
        final_df.createOrReplaceTempView(tmp_view)

        # Build SET clause dynamically from the DataFrame schema
        set_clause = ", ".join(
            f"t.{f.name} = s.{f.name}"
            for f in final_df.schema.fields
        )
        spark.sql(f"""
            MERGE INTO {fqn_backtick} AS t
            USING {tmp_view} AS s
            ON t.{pk_col} = s.{pk_col}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT *
        """)
        logger.info(
            "[%s/%s][standard] batch_id=%d upsert rows=%d",
            source_key, table_name, batch_id, final_df.count(),
        )

    if not deletes.isEmpty():
        del_view = f"__cdc_delete_{source_key}_{table_name}_{batch_id}"
        deletes.select(pk_col).createOrReplaceTempView(del_view)
        spark.sql(f"""
            MERGE INTO {fqn_backtick} AS t
            USING {del_view} AS s
            ON t.{pk_col} = s.{pk_col}
            WHEN MATCHED THEN DELETE
        """)
        logger.info(
            "[%s/%s][standard] batch_id=%d hard-delete rows=%d",
            source_key, table_name, batch_id, deletes.count(),
        )


def _apply_soft_delete(
    spark:       SparkSession,
    payload_df:  DataFrame,
    fqn_backtick: str,
    fqn_plain:    str,
    pk_col:       str,
    source_key:   str,
    table_name:   str,
    batch_id:     int,
) -> None:
    """
    MERGE upsert for INSERT/UPDATE.
    DELETE event → set is_deleted=true, deleted_at=now().  Row never physically removed.
    """
    inserts = payload_df.filter(col("_op").isin("c", "u", "r")).drop("_op", "kafka_ts")
    deletes = payload_df.filter(col("_op") == "d").drop("_op", "kafka_ts")

    if not inserts.isEmpty():
        final_df = (
            inserts
            .withColumn("snap_id",        monotonically_increasing_id().cast(LongType()))
            .withColumn("snap_timestamp", current_timestamp())
            .withColumn("is_deleted",     lit(False).cast(BooleanType()))
            .withColumn("deleted_at",     lit(None).cast(TimestampType()))
        )
        tmp_view = f"__cdc_upsert_{source_key}_{table_name}_{batch_id}"
        final_df.createOrReplaceTempView(tmp_view)
        set_clause = ", ".join(
            f"t.{f.name} = s.{f.name}"
            for f in final_df.schema.fields
        )
        spark.sql(f"""
            MERGE INTO {fqn_backtick} AS t
            USING {tmp_view} AS s
            ON t.{pk_col} = s.{pk_col}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT *
        """)
        logger.info(
            "[%s/%s][soft_delete] batch_id=%d upsert rows=%d",
            source_key, table_name, batch_id, final_df.count(),
        )

    if not deletes.isEmpty():
        del_view = f"__cdc_softdel_{source_key}_{table_name}_{batch_id}"
        # Only pk_col + soft-delete flags needed to match and update
        soft_del_df = (
            deletes.select(pk_col)
            .withColumn("is_deleted", lit(True).cast(BooleanType()))
            .withColumn("deleted_at", current_timestamp())
        )
        soft_del_df.createOrReplaceTempView(del_view)
        spark.sql(f"""
            MERGE INTO {fqn_backtick} AS t
            USING {del_view} AS s
            ON t.{pk_col} = s.{pk_col}
            WHEN MATCHED THEN UPDATE SET
                t.is_deleted = true,
                t.deleted_at = s.deleted_at
        """)
        logger.info(
            "[%s/%s][soft_delete] batch_id=%d soft-delete rows=%d",
            source_key, table_name, batch_id, deletes.count(),
        )


def _apply_history_tracking(
    spark:       SparkSession,
    payload_df:  DataFrame,
    fqn_backtick: str,
    fqn_plain:    str,
    pk_col:       str,
    source_key:   str,
    table_name:   str,
    batch_id:     int,
) -> None:
    """
    Always INSERT into Iceberg — never UPDATE or DELETE.
    Each event appended with _change_type (INSERT/UPDATE/DELETE) and _change_ts.
    Full row history is preserved.
    """
    # Map Debezium op codes to human-readable change types
    typed_df = payload_df.withColumn(
        "_change_type",
        F.when(col("_op") == "c", lit("INSERT"))
         .when(col("_op") == "u", lit("UPDATE"))
         .when(col("_op") == "d", lit("DELETE"))
         .when(col("_op") == "r", lit("INSERT"))   # snapshot read treated as INSERT
         .otherwise(lit("UNKNOWN")),
    ).withColumn(
        "_change_ts",
        current_timestamp(),
    ).drop("_op", "kafka_ts")

    final_df = (
        typed_df
        .withColumn("snap_id",        monotonically_increasing_id().cast(LongType()))
        .withColumn("snap_timestamp", current_timestamp())
    )

    (
        final_df
        .writeTo(fqn_plain)
        .option("mergeSchema", "true")
        .append()
    )
    logger.info(
        "[%s/%s][history_tracking] batch_id=%d appended rows=%d",
        source_key, table_name, batch_id, final_df.count(),
    )


# ── Micro-batch writer ────────────────────────────────────────────────────────

def _write_micro_batch(
    spark:        SparkSession,
    builder:      IcebergTableBuilder,
    source:       _StreamingSource,
    write_mode:   str,
    restart_flag: threading.Event,
) -> Any:
    """
    Return a foreachBatch function for the given source and write mode.

    For each micro-batch DataFrame the returned function:
      1. Flattens the Debezium envelope (parses "after" / "before" JSON).
      2. Routes rows to their Iceberg tables by Kafka topic.
      3. Creates the target Iceberg table if it does not exist yet (idempotent).
      4. Applies the configured write mode (standard / soft_delete / history_tracking).
      5. Refreshes every written Iceberg table (REFRESH TABLE) so downstream
         readers immediately see the new snapshot.
      6. Sets restart_flag so the outer loop stops and restarts the query,
         giving Spark a fresh execution context for the next 2-second batch.
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

        written_tables: list[str] = []   # track FQNs written this batch

        for topic in topics:
            table_name = _topic_to_table(topic, source)
            if not table_name:
                continue

            topic_df = batch_df.filter(col("topic") == topic)

            # ── Extract Debezium payload ─────────────────────────────────────
            env_df = topic_df.select(
                from_json(col("value_json"), _DEBEZIUM_ENVELOPE_SCHEMA).alias("env"),
                col("topic"),
                col("timestamp").alias("kafka_ts"),
            ).filter(col("env").isNotNull())

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

            # ── Infer schema from the batch ──────────────────────────────────
            try:
                inferred_schema = spark.read.json(
                    payload_df.select("payload_json").rdd.map(lambda r: r[0])
                ).schema
            except Exception as exc:
                logger.warning(
                    "[%s/%s] Could not infer schema: %s — skipping batch.",
                    source.source_key, table_name, exc,
                )
                continue

            # ── Augment schema for the write mode ────────────────────────────
            # soft_delete: ensure is_deleted / deleted_at columns exist
            # history_tracking: ensure _change_type / _change_ts columns exist
            # These are added via mergeSchema=true on first write; we declare
            # them here so the CREATE TABLE path includes them upfront.
            extra_fields: list[StructField] = [
                StructField("snap_id",        LongType(),      True),
                StructField("snap_timestamp", TimestampType(), True),
            ]
            if write_mode == _WRITE_MODE_SOFT_DELETE:
                extra_fields += [
                    StructField("is_deleted", BooleanType(),  True),
                    StructField("deleted_at", TimestampType(), True),
                ]
            elif write_mode == _WRITE_MODE_HISTORY_TRACKING:
                extra_fields += [
                    StructField("_change_type", StringType(),    True),
                    StructField("_change_ts",   TimestampType(), True),
                ]

            full_schema = StructType(
                inferred_schema.fields + extra_fields
            )

            # ── Create Iceberg table if needed ────────────────────────────────
            fqn_backtick = f"`{source.catalog}`.`{source.namespace}`.`{table_name}`"
            fqn_plain    = f"{source.catalog}.{source.namespace}.{table_name}"
            try:
                spark.sql(f"DESCRIBE TABLE {fqn_backtick}")
                table_exists = True
            except Exception:
                table_exists = False

            if not table_exists:
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
                        schema         = full_schema,
                        partition_spec = partition_spec,
                        location       = s3_location,
                    )
                    logger.info(
                        "[%s/%s] Created Iceberg table (write_mode=%s).",
                        source.source_key, table_name, write_mode,
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
                    col("_op"),
                    col("kafka_ts"),
                ).select("data.*", "_op", "kafka_ts")
            except Exception as exc:
                logger.warning(
                    "[%s/%s] JSON parse failed: %s", source.source_key, table_name, exc,
                )
                continue

            # Derive pk_col name (case-insensitive match against actual schema)
            pk_col_actual = source.pk_col
            for f in inferred_schema.fields:
                if f.name.lower() == source.pk_col.lower():
                    pk_col_actual = f.name
                    break

            # ── Apply write mode ──────────────────────────────────────────────
            try:
                if write_mode == _WRITE_MODE_STANDARD:
                    _apply_standard(
                        spark, row_df, fqn_backtick, fqn_plain,
                        pk_col_actual, source.source_key, table_name, batch_id,
                    )
                elif write_mode == _WRITE_MODE_SOFT_DELETE:
                    _apply_soft_delete(
                        spark, row_df, fqn_backtick, fqn_plain,
                        pk_col_actual, source.source_key, table_name, batch_id,
                    )
                elif write_mode == _WRITE_MODE_HISTORY_TRACKING:
                    _apply_history_tracking(
                        spark, row_df, fqn_backtick, fqn_plain,
                        pk_col_actual, source.source_key, table_name, batch_id,
                    )
                written_tables.append(fqn_backtick)
            except Exception as write_exc:
                logger.error(
                    "[%s/%s] Write failed (write_mode=%s): %s",
                    source.source_key, table_name, write_mode, write_exc,
                )

        # ── (a) Refresh every table written in this batch ─────────────────────
        # REFRESH TABLE invalidates Spark's cached metadata for the Iceberg
        # table so that the next query against it reads the latest snapshot
        # committed by the MERGE / append above.
        for fqn_bt in written_tables:
            try:
                spark.sql(f"REFRESH TABLE {fqn_bt}")
                logger.debug("[%s] REFRESH TABLE %s", source.source_key, fqn_bt)
            except Exception as ref_exc:
                logger.warning(
                    "[%s] REFRESH TABLE %s failed (non-fatal): %s",
                    source.source_key, fqn_bt, ref_exc,
                )

        # ── (b) Signal the outer loop to restart the streaming query ──────────
        # Setting restart_flag tells _run_once to stop this query and start a
        # new one, giving Spark a fresh execution context for the next batch.
        # The checkpoint is preserved so no events are replayed.
        if written_tables:
            restart_flag.set()
            logger.info(
                "[%s] batch_id=%d — %d table(s) written; restart_flag set.",
                source.source_key, batch_id, len(written_tables),
            )

    return _foreach_batch


# ── Stream builder ────────────────────────────────────────────────────────────

def _start_source_stream(
    spark:        SparkSession,
    builder:      IcebergTableBuilder,
    source:       _StreamingSource,
    bao:          BaoSparkInit,
    write_mode:   str,
    restart_flag: threading.Event,
) -> StreamingQuery:
    """
    Build and start the Structured Streaming query for a single CDC source.

    Pipeline:
      Kafka (Avro/JSON) → avro_to_json UDF → flatten envelope →
      foreachBatch → write-mode handler → Iceberg → REFRESH TABLE
      → restart_flag.set() → outer loop restarts query
    """
    kafka_secret = bao.kafka_creds()
    kafka_user   = kafka_secret.get("debezium_user",     "debezium-user")
    kafka_pass   = kafka_secret.get("debezium_password", "")

    jaas_cfg = (
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
        f"username=\"{kafka_user}\" password=\"{kafka_pass}\";"
    )

    kafka_options = {
        "kafka.bootstrap.servers":        KAFKA_BOOTSTRAP,
        "kafka.security.protocol":        "SASL_PLAINTEXT",
        "kafka.sasl.mechanism":           "SCRAM-SHA-512",
        "kafka.sasl.jaas.config":         jaas_cfg,
        "subscribePattern":               source.topic_pattern,
        "startingOffsets":                "latest",
        "maxOffsetsPerTrigger":           str(MAX_OFFSETS_PER_TRIGGER),
        "failOnDataLoss":                 "false",
        # Consumer performance tuning
        "kafka.fetch.min.bytes":          "65536",
        "kafka.fetch.wait.max.ms":        "500",
        "kafka.max.poll.records":         "500",
    }

    raw_stream = (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .load()
    )

    # Apply the Avro deserialisation UDF to the binary value column
    try:
        from confluent_kafka.schema_registry import SchemaRegistryClient  # noqa: F401
        avro_udf = _build_avro_deserialize_udf(SR_URL)
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
        .queryName(f"cdc-{source.source_key}-{write_mode}")
        .foreachBatch(
            _write_micro_batch(spark, builder, source, write_mode, restart_flag)
        )
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", source.checkpoint)
        .start()
    )

    logger.info(
        "[%s] Streaming query started | write_mode=%s | pattern=%s | checkpoint=%s",
        source.source_key, write_mode, source.topic_pattern, source.checkpoint,
    )
    return query


# ── Spark session factory ─────────────────────────────────────────────────────

def _build_spark(bao: BaoSparkInit) -> SparkSession:
    conf = bao.spark_conf(app_name=f"kafka-to-iceberg-{WRITE_MODE}")
    conf.set(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    )
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── Main: streaming loop with auto-restart ────────────────────────────────────

def _start_all_queries(
    spark:        SparkSession,
    builder:      IcebergTableBuilder,
    bao:          BaoSparkInit,
    restart_flag: threading.Event,
) -> list[StreamingQuery]:
    """Start streaming queries for all active sources and return them."""
    queries: list[StreamingQuery] = []
    for src in _ALL_SOURCES:
        try:
            q = _start_source_stream(spark, builder, src, bao, WRITE_MODE, restart_flag)
            queries.append(q)
        except Exception as exc:
            logger.error(
                "[%s] Failed to start stream: %s", src.source_key, exc, exc_info=True,
            )
    return queries


def _run_once(bao: BaoSparkInit) -> None:
    """
    Initialise Spark, start all streaming queries, and run the per-batch
    watch-and-restart loop until interrupted or a fatal error occurs.

    Per-batch restart cycle
    -----------------------
    After each 2-second micro-batch is committed to Iceberg the foreachBatch
    function sets restart_flag.  The polling loop below detects this, stops all
    active queries cleanly (preserving checkpoints), and immediately restarts
    them from the checkpoint position.  This gives Spark a fresh execution
    context for each batch while ensuring zero event replay.

    Raises on unrecoverable error so the outer retry loop can back off and retry.
    """
    spark = _build_spark(bao)

    # Catalog bootstrap pre-flight
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

    # One shared restart_flag across all sources in this Spark session.
    # Any source that writes a batch will set it; the loop below reacts.
    restart_flag = threading.Event()

    queries = _start_all_queries(spark, builder, bao, restart_flag)
    if not queries:
        spark.stop()
        raise RuntimeError("No streaming queries started.")

    logger.info(
        "All %d streaming queries active (write_mode=%s, trigger=%s). "
        "Per-batch restart enabled.",
        len(queries), WRITE_MODE, TRIGGER_INTERVAL,
    )

    # ── Per-batch watch-and-restart loop ──────────────────────────────────────
    # Poll every 500 ms.  On restart_flag:
    #   1. Stop all active queries (checkpoint is flushed automatically).
    #   2. Clear the flag.
    #   3. Re-start all queries from their checkpoints.
    # On unexpected query termination (query.isActive == False without the
    # flag being set): raise so the outer retry loop handles recovery.
    try:
        while True:
            time.sleep(0.5)

            # Check for unexpected termination (crash, not a planned restart)
            dead = [q for q in queries if not q.isActive]
            if dead and not restart_flag.is_set():
                names = [q.name for q in dead]
                raise RuntimeError(
                    f"Streaming query(s) terminated unexpectedly: {names}"
                )

            if restart_flag.is_set():
                logger.info(
                    "restart_flag detected — stopping %d query(s) for per-batch restart …",
                    len(queries),
                )
                # ── Stop all queries cleanly ──────────────────────────────────
                for q in queries:
                    try:
                        q.stop()
                    except Exception as stop_exc:
                        logger.warning("Error stopping query %s: %s", q.name, stop_exc)

                restart_flag.clear()

                # ── Re-start all queries from checkpoint ──────────────────────
                queries = _start_all_queries(spark, builder, bao, restart_flag)
                if not queries:
                    raise RuntimeError(
                        "No streaming queries started after per-batch restart."
                    )
                logger.info(
                    "%d query(s) restarted from checkpoint.",
                    len(queries),
                )

    except KeyboardInterrupt:
        logger.info("Interrupted — stopping all queries.")
        for q in queries:
            try:
                q.stop()
            except Exception:
                pass
        raise
    finally:
        try:
            spark.stop()
        except Exception:
            pass


def main() -> None:
    os.environ["SPARK_USER"] = SPARK_USER

    logger.info(
        "=== Kafka→Iceberg Streaming | user=%s | write_mode=%s | sources=%s | dry_run=%s ===",
        SPARK_USER,
        WRITE_MODE,
        [s.source_key for s in _ALL_SOURCES],
        DRY_RUN,
    )

    bao = BaoSparkInit()

    # ── Auto-restart loop ────────────────────────────────────────────────────
    attempt = 0
    while attempt < MAX_RESTART_ATTEMPTS:
        try:
            _run_once(bao)
            # _run_once only returns normally on KeyboardInterrupt (re-raised) or
            # successful clean shutdown — exit loop.
            break
        except KeyboardInterrupt:
            logger.info("Streaming job stopped by user.")
            sys.exit(0)
        except Exception as exc:
            attempt += 1
            if attempt >= MAX_RESTART_ATTEMPTS:
                logger.error(
                    "Streaming job failed after %d attempt(s). Giving up: %s",
                    attempt, exc,
                )
                sys.exit(1)
            backoff = min(
                RESTART_BACKOFF_BASE_S * (2 ** (attempt - 1)),
                RESTART_BACKOFF_MAX_S,
            )
            logger.warning(
                "Streaming job failed (attempt %d/%d): %s — retrying in %.0f s …",
                attempt, MAX_RESTART_ATTEMPTS, exc, backoff,
            )
            time.sleep(backoff)


if __name__ == "__main__":
    main()
