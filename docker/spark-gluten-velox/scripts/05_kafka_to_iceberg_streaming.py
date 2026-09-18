#!/usr/bin/env python3
"""
05_kafka_to_iceberg_streaming.py
================================
Spark Structured Streaming consumer — Debezium CDC → Iceberg via Polaris REST.

Three write modes, configurable per-run via the WRITE_MODE environment variable:

  standard         SCD Type 0 — MERGE INTO Iceberg by PK.
                   INSERT/UPDATE (op c/u/r) → upsert (match update + insert new).
                   DELETE        (op d)     → hard delete the matched row.

  soft_delete      MERGE upsert for INSERT/UPDATE.
                   DELETE event  → set is_deleted=true, deleted_at=<now()>.
                   Row is never physically removed.

  history_tracking Always INSERT into Iceberg — never UPDATE or DELETE.
                   Every CDC event appended with:
                     _change_type  (INSERT/UPDATE/DELETE)
                     _change_ts    (wall-clock at pipeline processing time)
                     before_*      columns (before image from source DB for UPDATE/DELETE)
                     after_*       columns (after  image from source DB for INSERT/UPDATE)
                   Full row-level before/after history preserved.

Source → topic → Iceberg target mapping
----------------------------------------
  postgres  : postgres.cache_testing.*  → postgres.cache_testing.<table>
  oracle    : oracle.tpcds.*            → oracle.tpcds.<table>
              oracle.cache_testing.*    → oracle.cache_testing.<table>
  mongodb   : mongodb.cache_testing.*   → mongodb.cache_testing.<table>

star_transform integration
--------------------------
A TRANSFORM_PIPELINE env-var (comma-separated step names) allows optional
StarTransform steps to run on every micro-batch before the write-mode handler:

  TRANSFORM_PIPELINE=deduplicate,add_processing_time,mask_pii

Available built-in pipeline step names (see TRANSFORM_REGISTRY below):
  deduplicate          — last-write-wins dedup per PK within the batch
  add_processing_time  — inject proc_time TIMESTAMP
  add_op_label         — inject human-readable op_label (INSERT/UPDATE/DELETE)
  add_source_tag       — inject source_system STRING
  mask_pii             — SHA-256 hash columns listed in PII_COLUMNS env-var

For custom transformations call StarTransform functions directly inside a
custom foreachBatch hook and pass it via the CUSTOM_TRANSFORM_MODULE env-var.

Performance (peak-hour) tuning
-------------------------------
• MAX_OFFSETS_PER_TRIGGER   — cap Kafka offsets per micro-batch (back-pressure)
• TRIGGER_INTERVAL          — micro-batch window (default 2 seconds)
• MERGE_PARALLELISM         — spark.sql.shuffle.partitions for MERGE operations
• COALESCE_BEFORE_MERGE     — coalesce batch partitions before MERGE (reduces
                              small-file write amplification under high load)
• ADAPTIVE_COALESCE_TARGET  — target bytes per post-shuffle partition (AQE)

Snap columns
------------
• snap_id        BIGINT    — monotonically_increasing_id() per row (unique within batch)
• snap_timestamp TIMESTAMP — current_timestamp() at write time (same for all rows in batch)
Both are injected per write-mode handler, not by IcebergTableBuilder.write_append(),
so MERGE operations (standard/soft_delete) can include them in SET clauses.

Iceberg partitioning (applied at table creation)
-------------------------------------------------
  hours(snap_timestamp)   — hourly partition for time-range pruning
  bucket(16, <pk_col>)    — 16 hash buckets within each hour for data distribution

Checkpoints
-----------
  s3://xdatatoiceberg1/checkpoints/streaming/<source>/<write_mode>

Auto-restart
------------
• Kubernetes restartPolicy: Always (pod-level).
• Internal exponential-backoff retry loop (MAX_RESTART_ATTEMPTS=0 → infinite;
  set to a positive integer to cap retries for debugging).
• Per-batch restart: after each committed micro-batch the streaming query is
  stopped and immediately restarted from checkpoint — fresh Spark context per batch.
• HTTP health server on HEALTH_PORT (default 8080): GET / returns 200 OK when
  all streaming queries are active, 503 when they are all dead (used by the
  Kubernetes livenessProbe).

Credentials
-----------
All from OpenBao via BaoSparkInit — never hardcoded.

Usage
-----
  SPARK_USER=dave WRITE_MODE=standard python3 05_kafka_to_iceberg_streaming.py
  SPARK_USER=dave WRITE_MODE=soft_delete SOURCE=postgres \\
      python3 05_kafka_to_iceberg_streaming.py
  SPARK_USER=dave WRITE_MODE=history_tracking \\
      TRANSFORM_PIPELINE=deduplicate,add_processing_time \\
      python3 05_kafka_to_iceberg_streaming.py
  DRY_RUN=1 SPARK_USER=dave python3 05_kafka_to_iceberg_streaming.py
"""

from __future__ import annotations

import http.server
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
from star_transform import StarTransform as ST
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

# Optional namespace override — when set, ALL topics from ALL sources are written
# into this Iceberg namespace instead of the namespace derived from the Kafka topic.
# Use-case: E2E testing — set TARGET_NAMESPACE=e2e_testing so that
#   postgres.cache_testing.customers  → postgres.e2e_testing.customers
#   oracle.cache_testing.CUSTOMERS    → oracle.e2e_testing.customers
#   mongodb.cache_testing.customers   → mongodb.e2e_testing.customers
# Leave empty ("") in production so each topic routes to its own namespace.
_TARGET_NAMESPACE = os.environ.get("TARGET_NAMESPACE", "").strip()

# Auto-restart loop config
# 0 = infinite retries (default — Kubernetes is the only termination gate).
# Set to a positive integer to cap retries during debugging.
MAX_RESTART_ATTEMPTS   = int(os.environ.get("MAX_RESTART_ATTEMPTS", "0"))
RESTART_BACKOFF_BASE_S = float(os.environ.get("RESTART_BACKOFF_BASE_S", "5"))
RESTART_BACKOFF_MAX_S  = float(os.environ.get("RESTART_BACKOFF_MAX_S", "120"))

# HTTP health server port — serves liveness probe endpoint GET /
HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8080"))

KAFKA_BOOTSTRAP = "strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092"
SR_URL          = "http://schema-registry.prod.svc.cluster.local:8081"
S3_BUCKET       = "xdatatoiceberg1"

# Streaming micro-batch trigger interval.
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "2 seconds")

# Back-pressure: max Kafka offsets consumed per trigger.
MAX_OFFSETS_PER_TRIGGER = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "50000"))

# Peak-hour performance tuning
# Number of shuffle partitions used during MERGE operations.
# Lower = fewer tasks = faster for small-medium batches; raise for large peak batches.
MERGE_PARALLELISM = int(os.environ.get("MERGE_PARALLELISM", "8"))

# Coalesce batch DataFrame partitions to this count before MERGE.
# Avoids writing many small Parquet files under high write amplification.
COALESCE_BEFORE_MERGE = int(os.environ.get("COALESCE_BEFORE_MERGE", "4"))

# AQE adaptive coalesce target bytes per post-shuffle partition (64 MB default).
ADAPTIVE_COALESCE_TARGET = os.environ.get("ADAPTIVE_COALESCE_TARGET", "67108864")

# ── StarTransform pipeline config ─────────────────────────────────────────────
# Comma-separated list of built-in transform step names to apply before
# each write-mode handler.  Example: "deduplicate,add_processing_time"
_TRANSFORM_PIPELINE_ENV = os.environ.get("TRANSFORM_PIPELINE", "").strip()
_TRANSFORM_STEPS = [s.strip() for s in _TRANSFORM_PIPELINE_ENV.split(",") if s.strip()]

# PII columns to hash when the "mask_pii" step is in TRANSFORM_PIPELINE.
_PII_COLUMNS = [
    c.strip()
    for c in os.environ.get("PII_COLUMNS", "email,phone,phone_number,ssn,credit_card").split(",")
    if c.strip()
]


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
        topic_pattern: str,
        catalog:       str,
        namespace:     str,
        pk_col:        str,
        s3_prefix:     str,
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
        topic_pattern = "oracle\\.(tpcds|cache_testing|TPCDS|CACHE_TESTING)\\..*",
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
# Debezium envelope: { before, after, op, source, ts_ms }
# before and after arrive as JSON strings (nested JSON-in-Avro pattern).

_DEBEZIUM_ENVELOPE_SCHEMA = StructType([
    StructField("before",  StringType(), True),   # JSON: before image (UPDATE/DELETE)
    StructField("after",   StringType(), True),   # JSON: after  image (INSERT/UPDATE)
    StructField("op",      StringType(), True),   # c=create, u=update, d=delete, r=read
    StructField("source",  StringType(), True),   # JSON: source metadata
    StructField("ts_ms",   LongType(),   True),   # source-side event timestamp millis
])


# ── Schema Registry Avro helper ───────────────────────────────────────────────

# ── Executor-level Schema Registry cache ─────────────────────────────────────
# These dicts live in the executor Python process and survive across UDF calls
# within the same executor.  They are NOT shared across executors (each executor
# process has its own copy), but they eliminate the per-row HTTP round-trip to
# the Schema Registry and the per-row Avro schema parse.
#
# _SR_CLIENT_CACHE  : { sr_url -> SchemaRegistryClient }  — one client per SR URL
# _SR_SCHEMA_CACHE  : { schema_id -> avro.schema.Schema } — parsed schema objects
# _SR_READER_CACHE  : { schema_id -> avro.io.DatumReader } — pre-built readers
#
# Cache is populated lazily on first access per schema_id.  A CDC pipeline with
# 22 topics will typically see 22–44 distinct schema IDs (key + value per topic);
# the cache converges within the first micro-batch and stays warm for the
# lifetime of the executor.
_SR_CLIENT_CACHE: dict = {}
_SR_SCHEMA_CACHE: dict = {}
_SR_READER_CACHE: dict = {}


def _build_avro_deserialize_udf(_sr_url: str) -> Any:
    """
    Build a Python UDF that deserialises a Confluent Avro-encoded byte array
    (5-byte magic header: 0x00 + 4-byte schema ID + avro payload) to a JSON string.
    Falls back to UTF-8 decode if the magic byte is absent (plain JSON mode).

    Efficiency design
    -----------------
    • SchemaRegistryClient is created ONCE per executor process and reused across
      all UDF invocations (stored in _SR_CLIENT_CACHE keyed by SR URL).
    • Parsed avro.schema.Schema objects are cached by schema_id (_SR_SCHEMA_CACHE).
    • avro.io.DatumReader objects are cached by schema_id (_SR_READER_CACHE).
    • A BytesIO + BinaryDecoder is the only object created per row — unavoidable
      because the payload bytes differ per message.
    • Module-level imports (io, struct, avro.*) are resolved once at UDF build
      time, not inside the closure body.

    Cache lifetime: executor process lifetime (survives across micro-batches on
    the same executor; reset only on executor restart or pod restart).
    """
    import io as _io
    import struct as _struct

    # Resolve avro modules once at UDF build time (not per row)
    try:
        import avro.io as _aio
        import avro.schema as _aschema
        _avro_available = True
    except ImportError:
        _avro_available = False

    def avro_to_json(topic: str, raw_bytes: bytes) -> str | None:
        if raw_bytes is None:
            return None
        try:
            if len(raw_bytes) < 5 or raw_bytes[0] != 0:
                # Not Confluent wire format — plain UTF-8 JSON
                return raw_bytes.decode("utf-8", errors="replace")

            if not _avro_available:
                # avro library not present — decode as UTF-8 best-effort
                return raw_bytes[5:].decode("utf-8", errors="replace")

            schema_id = _struct.unpack(">I", raw_bytes[1:5])[0]

            # ── Executor-level SR client (created once per executor) ──────────
            if _sr_url not in _SR_CLIENT_CACHE:
                from confluent_kafka.schema_registry import SchemaRegistryClient
                _SR_CLIENT_CACHE[_sr_url] = SchemaRegistryClient({"url": _sr_url})
            sr = _SR_CLIENT_CACHE[_sr_url]

            # ── Executor-level DatumReader (created once per schema_id) ───────
            if schema_id not in _SR_READER_CACHE:
                registered  = sr.get_schema(schema_id)          # one HTTP GET per new schema
                schema_def  = _aschema.parse(registered.schema_str)
                _SR_SCHEMA_CACHE[schema_id] = schema_def
                _SR_READER_CACHE[schema_id] = _aio.DatumReader(schema_def)

            reader  = _SR_READER_CACHE[schema_id]
            decoder = _aio.BinaryDecoder(_io.BytesIO(raw_bytes[5:]))  # per-row (payload differs)
            record  = reader.read(decoder)
            return json.dumps(record)

        except Exception as exc:
            logger.warning("avro_to_json failed (schema_id=%s): %s",
                           _struct.unpack(">I", raw_bytes[1:5])[0] if len(raw_bytes) >= 5 else "?",
                           exc)
            return None

    return udf(avro_to_json, StringType())


# ── Table routing ─────────────────────────────────────────────────────────────

def _topic_to_table(topic: str, source: _StreamingSource) -> str:
    """
    Derive the Iceberg table name from a Kafka topic name.
    e.g. "postgres.cache_testing.customers" → "customers"
         "oracle.tpcds.CALL_CENTER"         → "call_center"
    """
    import re
    # Strip the source prefix (e.g. "oracle.(tpcds|cache_testing).")
    # Use a simple split: take the last segment and lowercase it.
    parts = topic.split(".")
    return parts[-1].lower() if parts else ""


def _topic_to_namespace(topic: str, source: _StreamingSource) -> str:
    """
    Derive the Iceberg namespace from a Kafka topic name.

    When TARGET_NAMESPACE is set (e.g. "e2e_testing"), that value is returned
    unconditionally for every topic and every source — all three databases
    (postgres, oracle, mongodb) write into the same target namespace:
        postgres.cache_testing.customers  → postgres.e2e_testing.customers
        oracle.cache_testing.CUSTOMERS    → oracle.e2e_testing.customers
        mongodb.cache_testing.customers   → mongodb.e2e_testing.customers

    When TARGET_NAMESPACE is empty the namespace is derived from the topic:
        e.g. "oracle.tpcds.INCOME_BAND"           → "tpcds"
             "oracle.cache_testing.ORDERS"        → "cache_testing"
             "postgres.cache_testing.orders"      → "cache_testing"
    """
    if _TARGET_NAMESPACE:
        return _TARGET_NAMESPACE
    parts = topic.split(".")
    # parts[0]=prefix (oracle/postgres/mongodb), parts[1]=namespace, parts[2]=table
    return parts[1].lower() if len(parts) >= 3 else source.namespace


# ── StarTransform built-in pipeline registry ──────────────────────────────────

def _build_transform_pipeline(
    source_key: str,
    pk_col:     str,
) -> list[tuple[Any, dict]]:
    """
    Build the list of (fn, kwargs) steps from TRANSFORM_STEPS env config.
    Called once per source at stream startup.
    """
    registry: dict[str, tuple[Any, dict]] = {
        "deduplicate":         (ST.deduplicate,         {"pk": pk_col, "order_col": "kafka_ts"}),
        "add_processing_time": (ST.add_processing_time, {}),
        "add_op_label":        (ST.add_op_label,        {}),
        "add_source_tag":      (ST.add_source_tag,      {"source_system": source_key}),
        "mask_pii":            (ST.mask_columns,        {"columns": _PII_COLUMNS}),
    }
    steps = []
    for step_name in _TRANSFORM_STEPS:
        if step_name in registry:
            steps.append(registry[step_name])
        else:
            logger.warning(
                "[%s] Unknown TRANSFORM_PIPELINE step %r — skipped. "
                "Available: %s",
                source_key, step_name, list(registry.keys()),
            )
    if steps:
        logger.info(
            "[%s] StarTransform pipeline: %s",
            source_key, [s for s in _TRANSFORM_STEPS if s in registry],
        )
    return steps


# ── Write-mode handlers ────────────────────────────────────────────────────────

def _apply_standard(
    spark:        SparkSession,
    payload_df:   DataFrame,
    fqn_backtick: str,
    pk_col:       str,
    source_key:   str,
    table_name:   str,
    batch_id:     int,
) -> None:
    """
    SCD Type 0 — MERGE INTO Iceberg by PK.

    INSERT/UPDATE/snapshot (op c/u/r):
      MERGE MATCHED     → UPDATE all columns including snap_id + snap_timestamp
      MERGE NOT MATCHED → INSERT all columns

    DELETE (op d):
      MERGE MATCHED → DELETE row from Iceberg (hard delete)
    """
    spark.conf.set("spark.sql.shuffle.partitions", str(MERGE_PARALLELISM))

    inserts = payload_df.filter(col("_op").isin("c", "u", "r")).drop("_op", "kafka_ts")
    deletes = payload_df.filter(col("_op") == "d").drop("_op", "kafka_ts")

    if not inserts.isEmpty():
        final_df = (
            inserts
            .coalesce(COALESCE_BEFORE_MERGE)
            .withColumn("snap_id",        monotonically_increasing_id().cast(LongType()))
            .withColumn("snap_timestamp", current_timestamp())
        )
        tmp_view = f"__cdc_upsert_{source_key}_{table_name}_{batch_id}"
        final_df.createOrReplaceTempView(tmp_view)
        set_clause = ", ".join(
            f"t.`{f.name}` = s.`{f.name}`"
            for f in final_df.schema.fields
        )
        spark.sql(f"""
            MERGE INTO {fqn_backtick} AS t
            USING {tmp_view} AS s
            ON t.`{pk_col}` = s.`{pk_col}`
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT *
        """)
        logger.info(
            "[%s/%s][standard] batch=%d upsert rows=%d",
            source_key, table_name, batch_id, final_df.count(),
        )

    if not deletes.isEmpty():
        del_view = f"__cdc_delete_{source_key}_{table_name}_{batch_id}"
        deletes.select(pk_col).coalesce(1).createOrReplaceTempView(del_view)
        spark.sql(f"""
            MERGE INTO {fqn_backtick} AS t
            USING {del_view} AS s
            ON t.`{pk_col}` = s.`{pk_col}`
            WHEN MATCHED THEN DELETE
        """)
        logger.info(
            "[%s/%s][standard] batch=%d hard-delete rows=%d",
            source_key, table_name, batch_id, deletes.count(),
        )


def _apply_soft_delete(
    spark:        SparkSession,
    payload_df:   DataFrame,
    fqn_backtick: str,
    pk_col:       str,
    source_key:   str,
    table_name:   str,
    batch_id:     int,
) -> None:
    """
    SCD soft-delete — MERGE upsert for INSERT/UPDATE; flag-only for DELETE.

    INSERT/UPDATE/snapshot (op c/u/r):
      MERGE MATCHED     → UPDATE all columns; is_deleted=false, deleted_at=NULL
      MERGE NOT MATCHED → INSERT with is_deleted=false, deleted_at=NULL

    DELETE (op d):
      MERGE MATCHED → UPDATE SET is_deleted=true, deleted_at=<now>
      Row is never physically removed from Iceberg.
    """
    spark.conf.set("spark.sql.shuffle.partitions", str(MERGE_PARALLELISM))

    inserts = payload_df.filter(col("_op").isin("c", "u", "r")).drop("_op", "kafka_ts")
    deletes = payload_df.filter(col("_op") == "d").drop("_op", "kafka_ts")

    if not inserts.isEmpty():
        final_df = (
            inserts
            .coalesce(COALESCE_BEFORE_MERGE)
            .withColumn("snap_id",        monotonically_increasing_id().cast(LongType()))
            .withColumn("snap_timestamp", current_timestamp())
            .withColumn("is_deleted",     lit(False).cast(BooleanType()))
            .withColumn("deleted_at",     lit(None).cast(TimestampType()))
        )
        tmp_view = f"__cdc_upsert_{source_key}_{table_name}_{batch_id}"
        final_df.createOrReplaceTempView(tmp_view)
        set_clause = ", ".join(
            f"t.`{f.name}` = s.`{f.name}`"
            for f in final_df.schema.fields
        )
        spark.sql(f"""
            MERGE INTO {fqn_backtick} AS t
            USING {tmp_view} AS s
            ON t.`{pk_col}` = s.`{pk_col}`
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT *
        """)
        logger.info(
            "[%s/%s][soft_delete] batch=%d upsert rows=%d",
            source_key, table_name, batch_id, final_df.count(),
        )

    if not deletes.isEmpty():
        del_view = f"__cdc_softdel_{source_key}_{table_name}_{batch_id}"
        soft_del_df = (
            deletes.select(pk_col)
            .coalesce(1)
            .withColumn("is_deleted",     lit(True).cast(BooleanType()))
            .withColumn("deleted_at",     current_timestamp())
            .withColumn("snap_id",        monotonically_increasing_id().cast(LongType()))
            .withColumn("snap_timestamp", current_timestamp())
        )
        soft_del_df.createOrReplaceTempView(del_view)
        spark.sql(f"""
            MERGE INTO {fqn_backtick} AS t
            USING {del_view} AS s
            ON t.`{pk_col}` = s.`{pk_col}`
            WHEN MATCHED THEN UPDATE SET
                t.is_deleted     = s.is_deleted,
                t.deleted_at     = s.deleted_at,
                t.snap_id        = s.snap_id,
                t.snap_timestamp = s.snap_timestamp
        """)
        logger.info(
            "[%s/%s][soft_delete] batch=%d soft-delete rows=%d",
            source_key, table_name, batch_id, deletes.count(),
        )


def _apply_history_tracking(
    spark:        SparkSession,
    payload_df:   DataFrame,
    fqn_backtick: str,
    fqn_plain:    str,
    pk_col:       str,
    source_key:   str,
    table_name:   str,
    batch_id:     int,
    before_schema: Any,
    after_schema:  Any,
) -> None:
    """
    Append-only history tracking — every CDC event is a new Iceberg row.

    Columns injected on every row:
      _change_type   STRING     — INSERT / UPDATE / DELETE
      _change_ts     TIMESTAMP  — pipeline processing time
      snap_id        BIGINT     — unique row id within batch
      snap_timestamp TIMESTAMP  — write-time wall clock

    For UPDATE and DELETE events the before image (source DB state prior to
    the change) is preserved alongside the after image:
      before_<col>   — value before the change  (NULL for INSERT)
      after_<col>    — value after  the change  (NULL for DELETE)

    This produces a complete audit trail: from any snapshot of the Iceberg
    table you can reconstruct the full change history for any row.
    """
    # Map op codes to human-readable change types
    typed_df = payload_df.withColumn(
        "_change_type",
        F.when(col("_op") == "c", lit("INSERT"))
         .when(col("_op") == "u", lit("UPDATE"))
         .when(col("_op") == "d", lit("DELETE"))
         .when(col("_op") == "r", lit("INSERT"))
         .otherwise(lit("UNKNOWN")),
    ).withColumn(
        "_change_ts",
        current_timestamp(),
    )

    # Expand before / after JSON strings into typed columns.
    # "after"  is present for INSERT and UPDATE (the new row state).
    # "before" is present for UPDATE and DELETE (the old row state).
    result_df = typed_df
    if before_schema is not None and "before" in typed_df.columns:
        parsed_before = from_json(col("before"), before_schema)
        for field in before_schema.fields:
            result_df = result_df.withColumn(
                f"before_{field.name}", parsed_before[field.name]
            )
        result_df = result_df.drop("before")

    if after_schema is not None and "after" in result_df.columns:
        parsed_after = from_json(col("after"), after_schema)
        for field in after_schema.fields:
            result_df = result_df.withColumn(
                f"after_{field.name}", parsed_after[field.name]
            )
        result_df = result_df.drop("after")

    final_df = (
        result_df
        .drop("_op", "kafka_ts")
        .coalesce(COALESCE_BEFORE_MERGE)
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
        "[%s/%s][history_tracking] batch=%d appended rows=%d",
        source_key, table_name, batch_id, final_df.count(),
    )


# ── Micro-batch writer ────────────────────────────────────────────────────────

def _write_micro_batch(
    spark:        SparkSession,
    builder:      IcebergTableBuilder,
    source:       _StreamingSource,
    write_mode:   str,
    restart_flag: threading.Event,
    transform_steps: list[tuple[Any, dict]],
) -> Any:
    """
    Return a foreachBatch function for the given source and write mode.

    For each micro-batch:
      1. Route rows by topic.
      2. Decode Debezium envelope (before / after / op / ts_ms).
      3. Apply optional StarTransform pipeline steps.
      4. Auto-create Iceberg table if it does not exist.
      5. Apply write-mode handler (standard / soft_delete / history_tracking).
      6. REFRESH TABLE for all written tables.
      7. Set restart_flag for per-batch Spark context refresh.
    """
    def _foreach_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        if DRY_RUN:
            logger.info(
                "[%s] DRY_RUN batch=%d rows=%d — skipping write.",
                source.source_key, batch_id, batch_df.count(),
            )
            return

        # Route by topic — one DataFrame per Kafka topic (= one Iceberg table)
        topic_map = ST.route_by_topic(batch_df)
        if not topic_map:
            return

        written_tables: list[str] = []

        for topic, topic_df in topic_map.items():
            table_name = _topic_to_table(topic, source)
            namespace  = _topic_to_namespace(topic, source)
            if not table_name:
                continue

            # ── Decode Debezium envelope ──────────────────────────────────────
            env_df = topic_df.select(
                from_json(col("value_json"), _DEBEZIUM_ENVELOPE_SCHEMA).alias("env"),
                col("topic"),
                col("timestamp").alias("kafka_ts"),
            ).filter(col("env").isNotNull())

            # Carry both before and after through the pipeline:
            # history_tracking uses both; standard/soft_delete only need "after"
            # (for INSERT/UPDATE) and pk from "before" (for DELETE).
            full_envelope_df = env_df.select(
                col("env.before").alias("before"),
                col("env.after").alias("after"),
                col("env.op").alias("_op"),
                col("env.ts_ms").alias("ts_ms"),
                col("kafka_ts"),
            ).filter(col("_op").isNotNull())

            # For standard / soft_delete: the working payload is "after" for
            # INSERT/UPDATE and "before" for DELETE (pk lookup only).
            payload_df = full_envelope_df.select(
                F.when(
                    col("_op").isin("c", "u", "r"),
                    col("after"),
                ).when(
                    col("_op") == "d",
                    col("before"),
                ).alias("payload_json"),
                col("_op"),
                col("kafka_ts"),
            ).filter(col("payload_json").isNotNull())

            if payload_df.isEmpty():
                continue

            # ── Infer schema from batch ───────────────────────────────────────
            try:
                inferred_schema = spark.read.json(
                    payload_df.select("payload_json").rdd.map(lambda r: r[0])
                ).schema
            except Exception as exc:
                logger.warning(
                    "[%s/%s] Schema inference failed: %s — skipping.",
                    source.source_key, table_name, exc,
                )
                continue

            # ── Parse payload JSON → typed DataFrame ──────────────────────────
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

            # ── Apply StarTransform pipeline steps ────────────────────────────
            if transform_steps:
                try:
                    row_df = ST.apply_pipeline(row_df, transform_steps)
                except Exception as exc:
                    logger.error(
                        "[%s/%s] StarTransform pipeline failed: %s",
                        source.source_key, table_name, exc,
                    )
                    continue

            # ── PK column name (case-insensitive) ─────────────────────────────
            pk_col_actual = source.pk_col
            for f in inferred_schema.fields:
                if f.name.lower() == source.pk_col.lower():
                    pk_col_actual = f.name
                    break

            # ── Build write-mode-specific extra schema fields ─────────────────
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
                # before_* and after_* columns are added via mergeSchema=true
                # at append time — not declared at table creation (schema evolves).

            full_schema = StructType(inferred_schema.fields + extra_fields)

            # ── Auto-create Iceberg table if needed ───────────────────────────
            fqn_backtick = f"`{source.catalog}`.`{namespace}`.`{table_name}`"
            fqn_plain    = f"{source.catalog}.{namespace}.{table_name}"

            table_exists = builder.table_exists(source.catalog, namespace, table_name)
            if not table_exists:
                pk_col_exists = any(
                    f.name.lower() == source.pk_col.lower()
                    for f in inferred_schema.fields
                )
                pk_for_bucket = pk_col_actual if pk_col_exists else "snap_id"
                s3_location = (
                    f"s3://{S3_BUCKET}/{source.s3_prefix}"
                    f"/{namespace}/{table_name}"
                )
                # For history_tracking tables add "_hist" suffix to distinguish
                # them from the standard/soft-delete tables for the same source.
                effective_table = (
                    f"{table_name}_hist"
                    if write_mode == _WRITE_MODE_HISTORY_TRACKING
                    else table_name
                )
                effective_fqn_bt = f"`{source.catalog}`.`{namespace}`.`{effective_table}`"
                effective_fqn_pl = f"{source.catalog}.{namespace}.{effective_table}"
                effective_loc    = (
                    f"s3://{S3_BUCKET}/{source.s3_prefix}"
                    f"/{namespace}/{effective_table}"
                )
                try:
                    builder.create_table(
                        catalog        = source.catalog,
                        namespace      = namespace,
                        table          = effective_table,
                        schema         = full_schema,
                        partition_spec = [
                            IcebergTableBuilder.hours("snap_timestamp"),
                            IcebergTableBuilder.bucket(pk_for_bucket, 16),
                        ],
                        location       = effective_loc,
                        extra_properties={
                            "pipeline.write-mode": write_mode,
                            "pipeline.source":     source.source_key,
                        },
                    )
                    logger.info(
                        "[%s/%s] Created Iceberg table (mode=%s).",
                        source.source_key, effective_table, write_mode,
                    )
                    fqn_backtick = effective_fqn_bt
                    fqn_plain    = effective_fqn_pl
                    table_name   = effective_table
                except Exception as create_exc:
                    logger.warning(
                        "[%s/%s] Table creation failed (may already exist): %s",
                        source.source_key, effective_table, create_exc,
                    )
            else:
                # For history_tracking use the _hist suffixed table name
                if write_mode == _WRITE_MODE_HISTORY_TRACKING:
                    effective_table = f"{table_name}_hist"
                    if not builder.table_exists(source.catalog, namespace, effective_table):
                        logger.warning(
                            "[%s/%s] Expected _hist table not found; using base table.",
                            source.source_key, effective_table,
                        )
                    else:
                        fqn_backtick = f"`{source.catalog}`.`{namespace}`.`{effective_table}`"
                        fqn_plain    = f"{source.catalog}.{namespace}.{effective_table}"
                        table_name   = effective_table

            # ── Apply write mode ──────────────────────────────────────────────
            try:
                if write_mode == _WRITE_MODE_STANDARD:
                    _apply_standard(
                        spark, row_df, fqn_backtick,
                        pk_col_actual, source.source_key, table_name, batch_id,
                    )
                elif write_mode == _WRITE_MODE_SOFT_DELETE:
                    _apply_soft_delete(
                        spark, row_df, fqn_backtick,
                        pk_col_actual, source.source_key, table_name, batch_id,
                    )
                elif write_mode == _WRITE_MODE_HISTORY_TRACKING:
                    # For history_tracking we pass the full envelope rows
                    # (before + after) from the envelope DataFrame.
                    envelope_rows = full_envelope_df.filter(
                        F.col("_op").isin("c", "u", "d", "r")
                    )
                    # Enrich envelope rows with typed before/after schemas
                    try:
                        after_schema = spark.read.json(
                            envelope_rows
                            .filter(col("after").isNotNull())
                            .select("after")
                            .rdd.map(lambda r: r[0])
                        ).schema if not envelope_rows.filter(col("after").isNotNull()).isEmpty() else None
                    except Exception:
                        after_schema = None

                    try:
                        before_schema = spark.read.json(
                            envelope_rows
                            .filter(col("before").isNotNull())
                            .select("before")
                            .rdd.map(lambda r: r[0])
                        ).schema if not envelope_rows.filter(col("before").isNotNull()).isEmpty() else None
                    except Exception:
                        before_schema = None

                    _apply_history_tracking(
                        spark, envelope_rows, fqn_backtick, fqn_plain,
                        pk_col_actual, source.source_key, table_name, batch_id,
                        before_schema=before_schema,
                        after_schema=after_schema,
                    )

                written_tables.append(fqn_backtick)
            except Exception as write_exc:
                logger.error(
                    "[%s/%s] Write failed (mode=%s): %s",
                    source.source_key, table_name, write_mode, write_exc,
                    exc_info=True,
                )

        # ── Refresh written tables ────────────────────────────────────────────
        for fqn_bt in written_tables:
            try:
                spark.sql(f"REFRESH TABLE {fqn_bt}")
                logger.debug("[%s] REFRESH TABLE %s", source.source_key, fqn_bt)
            except Exception as ref_exc:
                logger.warning(
                    "[%s] REFRESH TABLE %s failed (non-fatal): %s",
                    source.source_key, fqn_bt, ref_exc,
                )

        # ── Signal per-batch restart ──────────────────────────────────────────
        if written_tables:
            restart_flag.set()
            logger.info(
                "[%s] batch=%d — %d table(s) written; restart_flag set.",
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
      Kafka (Avro/JSON) → avro_to_json UDF → flatten Debezium envelope →
      optional StarTransform steps → write-mode handler → Iceberg →
      REFRESH TABLE → restart_flag.set()
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
        # Consumer throughput tuning for peak-hour workloads
        "kafka.fetch.min.bytes":          "131072",    # 128 KB min fetch (reduced round-trips)
        "kafka.fetch.wait.max.ms":        "500",       # max wait for min bytes
        "kafka.max.poll.records":         "2000",      # records per poll (up from 500)
        "kafka.max.partition.fetch.bytes": "2097152",  # 2 MB per partition per fetch
        "kafka.receive.buffer.bytes":     "1048576",   # 1 MB socket receive buffer
    }

    raw_stream = (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .load()
    )

    # Avro deserialisation UDF
    try:
        from confluent_kafka.schema_registry import SchemaRegistryClient  # noqa: F401
        avro_udf = _build_avro_deserialize_udf(SR_URL)
    except ImportError:
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

    # Build the StarTransform pipeline for this source
    transform_steps = _build_transform_pipeline(source.source_key, source.pk_col)

    query = (
        decoded_stream
        .writeStream
        .queryName(f"cdc-{source.source_key}-{write_mode}")
        .foreachBatch(
            _write_micro_batch(
                spark, builder, source, write_mode, restart_flag, transform_steps
            )
        )
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", source.checkpoint)
        .start()
    )

    logger.info(
        "[%s] Streaming query started | mode=%s | pattern=%s | checkpoint=%s",
        source.source_key, write_mode, source.topic_pattern, source.checkpoint,
    )
    return query


# ── Spark session factory ─────────────────────────────────────────────────────

def _build_spark(bao: BaoSparkInit) -> SparkSession:
    conf = bao.spark_conf(app_name=f"kafka-to-iceberg-{WRITE_MODE}")
    # spark-sql-kafka and its kafka-clients dependency are baked into the image
    # at /opt/spark/jars/ (copied in Dockerfile).  Using spark.jars.packages would
    # trigger a Maven/Ivy download at session start, which (a) requires outbound
    # internet from the driver pod, (b) only lands the jar on the driver's local
    # /root/.ivy2/ — executors on worker nodes never receive it, so every micro-batch
    # that touches a Kafka DataSource fails with ClassNotFoundException.
    # spark.jars is not needed here because /opt/spark/jars/ is already on the
    # default classpath for both driver and all executor JVMs on this cluster.
    # Peak-hour AQE tuning
    conf.set("spark.sql.adaptive.enabled",                               "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled",            "true")
    conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize",   "33554432")   # 32 MB
    conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes",          ADAPTIVE_COALESCE_TARGET)
    conf.set("spark.sql.adaptive.skewJoin.enabled",                      "true")
    conf.set("spark.sql.shuffle.partitions",                             str(MERGE_PARALLELISM))
    # Iceberg write performance
    conf.set("spark.sql.iceberg.write.fanout.enabled",                   "true")
    conf.set("spark.sql.iceberg.merge.cardinality-check.enabled",        "false")
    # spark-defaults.conf in the spark-gluten-velox image sets KryoSerializer
    # cluster-wide (needed for Gluten/Velox + JDBC batch jobs).  The Kafka
    # DataSourceV2 (DataSourceRDDPartition) uses Java serialisation internally;
    # Kryo cannot deserialise its List$SerializationProxy → Seq and crashes every
    # micro-batch with a ClassCastException.  Override back to JavaSerializer here
    # so only this streaming session is unaffected; Gluten/JDBC jobs keep Kryo.
    conf.set("spark.serializer",
             "org.apache.spark.serializer.JavaSerializer")

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
    """
    spark = _build_spark(bao)

    try:
        cb = _imod("00_catalog_bootstrap")
        cb.bootstrap_all_catalogs(spark, bao, fail_fast=False)
    except Exception as exc:
        logger.warning("[bootstrap] Pre-flight warning: %s", exc)

    builder = IcebergTableBuilder(spark, running_user=SPARK_USER)

    for src in _ALL_SOURCES:
        try:
            builder.ensure_namespace(src.catalog, src.namespace)
        except Exception as exc:
            logger.warning("[%s] Could not ensure namespace: %s", src.source_key, exc)

    # When TARGET_NAMESPACE is set, pre-create that namespace in every active
    # catalog so the first micro-batch does not fail on a missing namespace.
    if _TARGET_NAMESPACE:
        logger.info(
            "TARGET_NAMESPACE=%r — ensuring override namespace in all active catalogs.",
            _TARGET_NAMESPACE,
        )
        for src in _ALL_SOURCES:
            try:
                builder.ensure_namespace(src.catalog, _TARGET_NAMESPACE)
                logger.info(
                    "[%s] Namespace '%s' ready.", src.source_key, _TARGET_NAMESPACE
                )
            except Exception as exc:
                logger.warning(
                    "[%s] Could not ensure namespace %r: %s",
                    src.source_key, _TARGET_NAMESPACE, exc,
                )

    restart_flag = threading.Event()
    queries = _start_all_queries(spark, builder, bao, restart_flag)
    if not queries:
        spark.stop()
        raise RuntimeError("No streaming queries started.")

    logger.info(
        "All %d streaming queries active (mode=%s, trigger=%s, "
        "merge_parallelism=%d, coalesce=%d). Per-batch restart enabled.",
        len(queries), WRITE_MODE, TRIGGER_INTERVAL,
        MERGE_PARALLELISM, COALESCE_BEFORE_MERGE,
    )

    try:
        while True:
            time.sleep(0.5)

            dead = [q for q in queries if not q.isActive]
            if dead and not restart_flag.is_set():
                names = [q.name for q in dead]
                raise RuntimeError(
                    f"Streaming query(s) terminated unexpectedly: {names}"
                )

            if restart_flag.is_set():
                logger.info(
                    "restart_flag — stopping %d query(s) for per-batch restart …",
                    len(queries),
                )
                for q in queries:
                    try:
                        q.stop()
                    except Exception as stop_exc:
                        logger.warning("Error stopping %s: %s", q.name, stop_exc)

                restart_flag.clear()
                queries = _start_all_queries(spark, builder, bao, restart_flag)
                if not queries:
                    raise RuntimeError("No streaming queries started after restart.")
                logger.info("%d query(s) restarted from checkpoint.", len(queries))

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


# ── HTTP health server ─────────────────────────────────────────────────────────
# _HEALTH_STATE is set by main() — True when at least one streaming query is
# active (or the process is still starting up), False only when _run_once()
# exits without active queries and we are between retry backoffs.
# The Kubernetes livenessProbe hits GET / on HEALTH_PORT:
#   200 OK  → process is alive and queries are running (or starting)
#   503     → all queries are dead and the backoff retry is sleeping
_HEALTH_STATE: dict = {"healthy": True}


class _HealthHandler(http.server.BaseHTTPRequestHandler):
    """Minimal HTTP handler — returns 200 or 503 based on _HEALTH_STATE."""

    def do_GET(self) -> None:  # noqa: N802
        if _HEALTH_STATE["healthy"]:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK\n")
        else:
            self.send_response(503)
            self.end_headers()
            self.wfile.write(b"UNHEALTHY\n")

    def log_message(self, fmt: str, *args: object) -> None:  # noqa: N802
        # Suppress default request logging — it floods the pod log.
        pass


def _start_health_server() -> None:
    """Start the HTTP health server in a daemon thread."""
    server = http.server.HTTPServer(("0.0.0.0", HEALTH_PORT), _HealthHandler)
    t = threading.Thread(target=server.serve_forever, name="health-server", daemon=True)
    t.start()
    logger.info("Health server listening on port %d", HEALTH_PORT)


def main() -> None:
    os.environ["SPARK_USER"] = SPARK_USER

    logger.info(
        "=== Kafka→Iceberg | user=%s | mode=%s | sources=%s | "
        "transform=%s | dry_run=%s | trigger=%s | max_offsets=%d "
        "| max_restart_attempts=%s ===",
        SPARK_USER, WRITE_MODE,
        [s.source_key for s in _ALL_SOURCES],
        _TRANSFORM_STEPS or "none",
        DRY_RUN, TRIGGER_INTERVAL, MAX_OFFSETS_PER_TRIGGER,
        MAX_RESTART_ATTEMPTS if MAX_RESTART_ATTEMPTS > 0 else "∞",
    )

    _start_health_server()

    bao = BaoSparkInit()
    attempt = 0
    # MAX_RESTART_ATTEMPTS == 0  → infinite retry loop (never gives up).
    # MAX_RESTART_ATTEMPTS  > 0  → cap at that many attempts then sys.exit(1)
    #                              so Kubernetes restartPolicy=Always triggers.
    while True:
        _HEALTH_STATE["healthy"] = True
        try:
            _run_once(bao)
            break
        except KeyboardInterrupt:
            logger.info("Streaming job stopped by user.")
            sys.exit(0)
        except Exception as exc:
            attempt += 1
            if MAX_RESTART_ATTEMPTS > 0 and attempt >= MAX_RESTART_ATTEMPTS:
                logger.error(
                    "Streaming job failed after %d attempt(s). Giving up: %s",
                    attempt, exc,
                )
                _HEALTH_STATE["healthy"] = False
                sys.exit(1)
            backoff = min(
                RESTART_BACKOFF_BASE_S * (2 ** min(attempt - 1, 10)),
                RESTART_BACKOFF_MAX_S,
            )
            cap_info = (
                f"{attempt}/{MAX_RESTART_ATTEMPTS}"
                if MAX_RESTART_ATTEMPTS > 0
                else f"{attempt}/∞"
            )
            logger.warning(
                "Streaming job failed (attempt %s): %s — retrying in %.0f s …",
                cap_info, exc, backoff,
            )
            # Mark unhealthy during backoff sleep so liveness probe fires if
            # the pod is stuck in a backoff spiral longer than failureThreshold
            # * periodSeconds (configured in the Kubernetes deployment).
            _HEALTH_STATE["healthy"] = False
            time.sleep(backoff)


if __name__ == "__main__":
    main()
