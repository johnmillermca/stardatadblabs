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
  oracle    : oracle.cache_testing.*    → oracle.cache_testing.<table>
  mongodb   : mongodb.cache_testing.*   → mongodb.cache_testing.<table>

DDL handling
------------
Source schema changes (ALTER TABLE / new MongoDB fields) are handled automatically.
New columns are picked up on the next batch after the table schema cache refreshes.

Table auto-creation
-------------------
When a new table appears in Kafka (new Debezium source registration), this script
auto-creates the corresponding Iceberg table on the first batch using the schema
inferred from that batch. The schema is then read from Iceberg (DESCRIBE TABLE)
on all subsequent batches — batch inference is never re-run after first creation.

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
• snap_id        BIGINT    — globally unique per row: (batch_id * 10_000_000) + monotonically_increasing_id()
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
    BooleanType, DoubleType, FloatType, IntegerType,
    LongType, StringType, StructField, StructType, TimestampType,
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

# Number of shuffle partitions used during MERGE operations.
MERGE_PARALLELISM = int(os.environ.get("MERGE_PARALLELISM", "4"))

# Coalesce batch DataFrame partitions before MERGE.
COALESCE_BEFORE_MERGE = int(os.environ.get("COALESCE_BEFORE_MERGE", "1"))

# AQE adaptive coalesce target bytes per post-shuffle partition (64 MB default).
ADAPTIVE_COALESCE_TARGET = os.environ.get("ADAPTIVE_COALESCE_TARGET", "67108864")

# Executor sizing
EXECUTOR_INSTANCES = int(os.environ.get("EXECUTOR_INSTANCES", "1"))
EXECUTOR_CORES     = int(os.environ.get("EXECUTOR_CORES",     "1"))
EXECUTOR_MEMORY    = os.environ.get("EXECUTOR_MEMORY",        "2g")
EXECUTOR_OFFHEAP   = os.environ.get("EXECUTOR_OFFHEAP",       "512m")

# Maximum executors the dynamic allocator may scale up to under sustained load.
MAX_EXECUTORS = int(os.environ.get("MAX_EXECUTORS", "3"))

# How long (seconds) the scheduler backlog must be sustained before the dynamic
# allocator requests an additional executor.
BURST_BACKLOG_TIMEOUT_S = int(os.environ.get("BURST_BACKLOG_TIMEOUT_S", "60"))

# ── StarTransform pipeline config ─────────────────────────────────────────────
_TRANSFORM_PIPELINE_ENV = os.environ.get("TRANSFORM_PIPELINE", "").strip()
_TRANSFORM_STEPS = [s.strip() for s in _TRANSFORM_PIPELINE_ENV.split(",") if s.strip()]

_PII_COLUMNS = [
    c.strip()
    for c in os.environ.get("PII_COLUMNS", "email,phone,phone_number,ssn,credit_card").split(",")
    if c.strip()
]

# ── Per-table Iceberg schema cache ────────────────────────────────────────────
# Keyed by (source_key, table_name) → StructType.
# Populated on the first batch for each table from DESCRIBE TABLE (Iceberg is the
# type authority). Never re-inferred from batch data — DDL changes are handled
# exclusively by ddl_apply.py.
_SCHEMA_CACHE: dict[tuple[str, str], "StructType"] = {}

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
        topic_pattern = "oracle\\.(cache_testing|CACHE_TESTING)\\..*",
        catalog       = "oracle",
        namespace     = "cache_testing",
        pk_col        = "customer_id",
        s3_prefix     = "iceberg/ora_lakehouse",
    ),
    _StreamingSource(
        source_key    = "mongodb",
        topic_pattern = "mongodb\\.cache_testing\\..*",
        catalog       = "mongodb",
        namespace     = "cache_testing",
        pk_col        = "customer_id",
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
_DEBEZIUM_ENVELOPE_SCHEMA = StructType([
    StructField("before",  StringType(), True),
    StructField("after",   StringType(), True),
    StructField("op",      StringType(), True),
    StructField("source",  StringType(), True),
    StructField("ts_ms",   LongType(),   True),
])


# ── Schema Registry Avro helper ───────────────────────────────────────────────
_SR_CLIENT_CACHE: dict = {}
_SR_SCHEMA_CACHE: dict = {}
_SR_READER_CACHE: dict = {}


def _build_avro_deserialize_udf(_sr_url: str) -> Any:
    """
    Build a Python UDF that deserialises a Confluent Avro-encoded byte array
    (5-byte magic header: 0x00 + 4-byte schema ID + avro payload) to a JSON string.
    Falls back to UTF-8 decode if the magic byte is absent (plain JSON mode).
    """
    import io as _io
    import struct as _struct

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
                return raw_bytes.decode("utf-8", errors="replace")

            if not _avro_available:
                return raw_bytes[5:].decode("utf-8", errors="replace")

            schema_id = _struct.unpack(">I", raw_bytes[1:5])[0]

            if _sr_url not in _SR_CLIENT_CACHE:
                from confluent_kafka.schema_registry import SchemaRegistryClient
                _SR_CLIENT_CACHE[_sr_url] = SchemaRegistryClient({"url": _sr_url})
            sr = _SR_CLIENT_CACHE[_sr_url]

            if schema_id not in _SR_READER_CACHE:
                registered  = sr.get_schema(schema_id)
                schema_def  = _aschema.parse(registered.schema_str)
                _SR_SCHEMA_CACHE[schema_id] = schema_def
                _SR_READER_CACHE[schema_id] = _aio.DatumReader(schema_def)

            reader  = _SR_READER_CACHE[schema_id]
            decoder = _aio.BinaryDecoder(_io.BytesIO(raw_bytes[5:]))
            record  = reader.read(decoder)
            return json.dumps(record)

        except Exception as exc:
            logger.warning("avro_to_json failed: %s", exc)
            return None

    return udf(avro_to_json, StringType())


# ── Table routing ─────────────────────────────────────────────────────────────

def _topic_to_table(topic: str, source: _StreamingSource) -> str:
    parts = topic.split(".")
    return parts[-1].lower() if parts else ""


def _topic_to_namespace(topic: str, source: _StreamingSource) -> str:
    if _TARGET_NAMESPACE:
        return _TARGET_NAMESPACE
    parts = topic.split(".")
    return parts[1].lower() if len(parts) >= 3 else source.namespace


# ── StarTransform built-in pipeline registry ──────────────────────────────────

def _build_transform_pipeline(
    source_key: str,
    pk_col:     str,
) -> list[tuple[Any, dict]]:
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
                "[%s] Unknown TRANSFORM_PIPELINE step %r — skipped.",
                source_key, step_name,
            )
    if steps:
        logger.info(
            "[%s] StarTransform pipeline: %s",
            source_key, [s for s in _TRANSFORM_STEPS if s in registry],
        )
    return steps


# ── Iceberg type helpers ───────────────────────────────────────────────────────

_ICE_TO_SPARK: dict[str, Any] = {
    "bigint":    LongType(),
    "long":      LongType(),
    "int":       IntegerType(),
    "integer":   IntegerType(),
    "smallint":  IntegerType(),
    "tinyint":   IntegerType(),
    "string":    StringType(),
    "varchar":   StringType(),
    "boolean":   BooleanType(),
    "timestamp": TimestampType(),
    "double":    DoubleType(),
    "float":     FloatType(),
}

_PY_TO_ICEBERG: dict[str, str] = {
    "LongType":      "BIGINT",
    "IntegerType":   "INT",
    "StringType":    "STRING",
    "DoubleType":    "DOUBLE",
    "FloatType":     "FLOAT",
    "BooleanType":   "BOOLEAN",
    "TimestampType": "TIMESTAMP",
    "DateType":      "DATE",
}


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
      MERGE MATCHED     → UPDATE all CDC columns (snap_id/snap_timestamp preserved)
      MERGE NOT MATCHED → INSERT CDC columns + fresh snap_id/snap_timestamp

    DELETE (op d):
      MERGE MATCHED → DELETE row from Iceberg (hard delete)
    """
    spark.conf.set("spark.sql.shuffle.partitions", str(MERGE_PARALLELISM))

    _SNAP_COLS = {"snap_id", "snap_timestamp"}

    inserts = payload_df.filter(col("_op").isin("c", "u", "r")).drop("_op", "kafka_ts")
    deletes = payload_df.filter(col("_op") == "d").drop("_op", "kafka_ts")

    if not inserts.isEmpty():
        raw_df = (
            inserts
            .coalesce(COALESCE_BEFORE_MERGE)
            .withColumn("snap_id",        (lit(batch_id).cast(LongType()) * lit(10_000_000).cast(LongType())
                                           + monotonically_increasing_id().cast(LongType())))
            .withColumn("snap_timestamp", current_timestamp())
        )
        rows      = raw_df.collect()
        final_df  = spark.createDataFrame(rows, raw_df.schema)
        row_count = len(rows)
        tmp_view  = f"__cdc_upsert_{source_key}_{table_name}_{batch_id}"
        final_df.createOrReplaceGlobalTempView(tmp_view)
        set_clause = ", ".join(
            f"t.`{f.name}` = s.`{f.name}`"
            for f in final_df.schema.fields
            if f.name not in _SNAP_COLS
        )
        col_list = ", ".join(f"`{f.name}`" for f in final_df.schema.fields)
        val_list  = ", ".join(f"s.`{f.name}`" for f in final_df.schema.fields)
        spark.sql(f"""
            MERGE INTO {fqn_backtick} AS t
            USING global_temp.{tmp_view} AS s
            ON t.`{pk_col}` = s.`{pk_col}`
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list})
        """)
        logger.info(
            "[%s/%s][standard] batch=%d upsert rows=%d",
            source_key, table_name, batch_id, row_count,
        )

    if not deletes.isEmpty():
        del_view = f"__cdc_delete_{source_key}_{table_name}_{batch_id}"
        deletes.select(pk_col).coalesce(1).createOrReplaceGlobalTempView(del_view)
        spark.sql(f"""
            MERGE INTO {fqn_backtick} AS t
            USING global_temp.{del_view} AS s
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
    """
    spark.conf.set("spark.sql.shuffle.partitions", str(MERGE_PARALLELISM))

    _SNAP_COLS = {"snap_id", "snap_timestamp"}

    inserts = payload_df.filter(col("_op").isin("c", "u", "r")).drop("_op", "kafka_ts")
    deletes = payload_df.filter(col("_op") == "d").drop("_op", "kafka_ts")

    if not inserts.isEmpty():
        raw_df = (
            inserts
            .coalesce(COALESCE_BEFORE_MERGE)
            .withColumn("snap_id",        (lit(batch_id).cast(LongType()) * lit(10_000_000).cast(LongType())
                                           + monotonically_increasing_id().cast(LongType())))
            .withColumn("snap_timestamp", current_timestamp())
            .withColumn("is_deleted", lit(False).cast(BooleanType()))
            .withColumn("deleted_at", lit(None).cast(TimestampType()))
        )
        rows      = raw_df.collect()
        final_df  = spark.createDataFrame(rows, raw_df.schema)
        row_count = len(rows)
        tmp_view  = f"__cdc_upsert_{source_key}_{table_name}_{batch_id}"
        final_df.createOrReplaceGlobalTempView(tmp_view)
        set_clause = ", ".join(
            f"t.`{f.name}` = s.`{f.name}`"
            for f in final_df.schema.fields
            if f.name not in _SNAP_COLS
        )
        col_list = ", ".join(f"`{f.name}`" for f in final_df.schema.fields)
        val_list  = ", ".join(f"s.`{f.name}`" for f in final_df.schema.fields)
        spark.sql(f"""
            MERGE INTO {fqn_backtick} AS t
            USING global_temp.{tmp_view} AS s
            ON t.`{pk_col}` = s.`{pk_col}`
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list})
        """)
        logger.info(
            "[%s/%s][soft_delete] batch=%d upsert rows=%d",
            source_key, table_name, batch_id, row_count,
        )

    if not deletes.isEmpty():
        del_view = f"__cdc_softdel_{source_key}_{table_name}_{batch_id}"
        soft_del_df = (
            deletes.select(pk_col)
            .coalesce(1)
            .withColumn("is_deleted", lit(True).cast(BooleanType()))
            .withColumn("deleted_at", current_timestamp())
        )
        soft_del_df.createOrReplaceGlobalTempView(del_view)
        spark.sql(f"""
            MERGE INTO {fqn_backtick} AS t
            USING global_temp.{del_view} AS s
            ON t.`{pk_col}` = s.`{pk_col}`
            WHEN MATCHED THEN UPDATE SET
                t.is_deleted = s.is_deleted,
                t.deleted_at = s.deleted_at
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
    row_schema:    Any = None,
) -> None:
    """
    Append-only history tracking — every CDC event is a new Iceberg row.
    """
    # ── 1. Map op codes ───────────────────────────────────────────────────────
    typed_df = payload_df.withColumn(
        "_change_type",
        F.when(col("_op") == "c", lit("INSERT"))
         .when(col("_op") == "u", lit("UPDATE"))
         .when(col("_op") == "d", lit("DELETE"))
         .when(col("_op") == "r", lit("INSERT"))
         .otherwise(lit("UNKNOWN")),
    ).withColumn("_change_ts", current_timestamp())

    # ── 2. Expand before image ────────────────────────────────────────────────
    result_df = typed_df
    if before_schema is not None and "before" in typed_df.columns:
        parsed_before = from_json(col("before"), before_schema)
        for field in before_schema.fields:
            result_df = result_df.withColumn(
                f"before_{field.name}", parsed_before[field.name]
            )
    if "before" in result_df.columns:
        result_df = result_df.drop("before")

    # ── 3. Expand after image ─────────────────────────────────────────────────
    if after_schema is not None and "after" in result_df.columns:
        parsed_after = from_json(col("after"), after_schema)
        for field in after_schema.fields:
            result_df = result_df.withColumn(
                f"after_{field.name}", parsed_after[field.name]
            )
    if "after" in result_df.columns:
        result_df = result_df.drop("after")

    # ── 4. Backfill missing image columns with typed NULLs ───────────────────
    if row_schema is not None:
        for field in row_schema.fields:
            for prefix in ("after_", "before_"):
                img_col = f"{prefix}{field.name}"
                if img_col not in result_df.columns:
                    result_df = result_df.withColumn(
                        img_col, lit(None).cast(field.dataType)
                    )

    # ── 4b. Inject top-level PK column ───────────────────────────────────────
    after_pk  = f"after_{pk_col}"
    before_pk = f"before_{pk_col}"
    if after_pk in result_df.columns or before_pk in result_df.columns:
        _after_expr  = col(after_pk)  if after_pk  in result_df.columns else lit(None)
        _before_expr = col(before_pk) if before_pk in result_df.columns else lit(None)
        result_df = result_df.withColumn(pk_col, F.coalesce(_after_expr, _before_expr))

    # ── 4c. MongoDB history_tracking post-processing ──────────────────────────
    if source_key == "mongodb":
        _oid_cols = [c for c in result_df.columns if c in ("after__id", "before__id")]
        if _oid_cols:
            result_df = result_df.drop(*_oid_cols)
        _TS_SUFFIXES_HIST = ("_at", "_ts", "_time", "_date")
        from pyspark.sql.types import StructType as _HistST
        for _c in list(result_df.columns):
            if not ((_c.startswith("after_") or _c.startswith("before_"))
                    and any(_c.endswith(s) for s in _TS_SUFFIXES_HIST)):
                continue
            _dtype = result_df.schema[_c].dataType
            if isinstance(_dtype, _HistST):
                result_df = result_df.withColumn(
                    _c,
                    (col(f"`{_c}`").getField("$date") / lit(1_000)).cast(TimestampType()),
                )
            elif isinstance(_dtype, StringType):
                result_df = result_df.withColumn(
                    _c,
                    F.when(
                        col(_c).rlike(r"^\d{10,13}$"),
                        (col(_c).cast(LongType()) / lit(1_000)).cast(TimestampType()),
                    ).otherwise(col(_c).cast(TimestampType())),
                )
            elif isinstance(_dtype, LongType):
                result_df = result_df.withColumn(
                    _c, (col(_c) / lit(1_000)).cast(TimestampType()),
                )

    # ── 5. Drop internal envelope columns ────────────────────────────────────
    _ENVELOPE_COLS = {"_op", "kafka_ts", "ts_ms"}
    result_df = result_df.drop(*[c for c in _ENVELOPE_COLS if c in result_df.columns])

    # ── 6. Break streaming lineage ────────────────────────────────────────────
    rows      = result_df.coalesce(COALESCE_BEFORE_MERGE).collect()
    final_df  = (
        spark.createDataFrame(rows, result_df.schema)
        .withColumn("snap_id",        (lit(batch_id).cast(LongType()) * lit(10_000_000).cast(LongType())
                                       + monotonically_increasing_id().cast(LongType())))
        .withColumn("snap_timestamp", current_timestamp())
    )
    row_count = len(rows)

    # ── 7. Lazy table creation ────────────────────────────────────────────────
    if not spark.catalog.tableExists(fqn_plain):
        col_defs = ", ".join(
            f"`{f.name}` {_PY_TO_ICEBERG.get(type(f.dataType).__name__, 'STRING')}"
            for f in final_df.schema.fields
        )
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {fqn_backtick} (
                {col_defs}
            )
            USING iceberg
            PARTITIONED BY (hours(snap_timestamp))
            TBLPROPERTIES (
                'pipeline.write-mode'              = 'history_tracking',
                'pipeline.source'                  = '{source_key}',
                'write.spark.accept-any-schema'    = 'true'
            )
        """)
        logger.info(
            "[%s/%s][history_tracking] Created Iceberg table.",
            source_key, table_name,
        )

    # ── 7b. Cast batch columns to match existing Iceberg hist schema ──────────
    # history_tracking tables are USER-MANAGED for DDL (run ddl_apply.py).
    # We cast existing columns to their Iceberg types and drop any batch column
    # that is not yet in the hist table (will be NULL in existing rows — correct
    # for an append-only table).
    _hist_existing_type_map: dict[str, str] = {}
    try:
        _hist_rows = spark.sql(f"DESCRIBE TABLE {fqn_backtick}").collect()
        _hist_existing_type_map = {
            row["col_name"].lower(): row["data_type"].lower()
            for row in _hist_rows
            # Stop at the blank separator row — 'Part 0'/'Part 1' partition rows follow
            if row["col_name"]
            and not row["col_name"].startswith(("#", "Part "))
        }
    except Exception as _hist_desc_exc:
        logger.debug(
            "[%s/%s][history_tracking] DESCRIBE TABLE skipped: %s",
            source_key, table_name, _hist_desc_exc,
        )

    if _hist_existing_type_map:
        # Build a name→dataType map of the DataFrame for fast lookup.
        _df_type_map = {f.name.lower(): f.dataType for f in final_df.schema.fields}
        # Iterate in TABLE schema order (not DataFrame order) so the SELECT
        # produces columns in exactly the same sequence as the Iceberg table.
        # Iceberg rejects appends where columns arrive out of order even when
        # write.spark.accept-any-schema is set.
        cols_to_keep = []
        for _col_name, _ice_type in _hist_existing_type_map.items():
            if _col_name not in _df_type_map:
                logger.debug(
                    "[%s/%s][history_tracking] col '%s' in hist table but absent from batch — will land as NULL",
                    source_key, table_name, _col_name,
                )
                continue
            _target_spark_type = _ICE_TO_SPARK.get(_ice_type)
            _actual_dtype = _df_type_map[_col_name]
            if _target_spark_type and not isinstance(_actual_dtype, type(_target_spark_type)):
                final_df = final_df.withColumn(
                    _col_name,
                    col(f"`{_col_name}`").cast(_target_spark_type),
                )
            cols_to_keep.append(_col_name)
        final_df = final_df.select(*[f"`{c}`" for c in cols_to_keep])

    # ── 8. Write ──────────────────────────────────────────────────────────────
    write_rows = final_df.collect()
    write_df   = spark.createDataFrame(write_rows, final_df.schema)
    (
        write_df
        .writeTo(fqn_plain)
        .option("mergeSchema", "true")
        .append()
    )
    logger.info(
        "[%s/%s][history_tracking] batch=%d appended rows=%d",
        source_key, table_name, batch_id, row_count,
    )


# ── Micro-batch writer ────────────────────────────────────────────────────────

def _write_micro_batch(
    spark:        SparkSession,
    builder:      IcebergTableBuilder,
    source:       _StreamingSource,
    write_mode:   str,
    transform_steps: list[tuple[Any, dict]],
) -> Any:
    """
    Return a foreachBatch function for the given source and write mode.

    For each micro-batch:
      1. Route rows by topic.
      2. Decode Debezium envelope (before / after / op / ts_ms).
      3. Apply optional StarTransform pipeline steps.
      4. Auto-create Iceberg table if it does not exist (first batch only).
         Schema is read from Iceberg (DESCRIBE TABLE) and cached — never
         re-inferred from batch data.
      5. Apply write-mode handler (standard / soft_delete / history_tracking).

    DDL handling
    ------------
    This function contains NO DDL detection, no ALTER TABLE, no schema evolution.
    If a batch contains columns not present in the Iceberg table, those columns
    are dropped from the batch before writing (rows written with NULL for the
    missing column). Run ddl_apply.py to evolve the Iceberg schema first.
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

            full_envelope_df = env_df.select(
                col("env.before").alias("before"),
                col("env.after").alias("after"),
                col("env.op").alias("_op"),
                col("env.ts_ms").alias("ts_ms"),
                col("kafka_ts"),
            ).filter(col("_op").isNotNull())

            # For standard / soft_delete the working payload is "after" for
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

            # ── Oracle column-name normalisation ──────────────────────────────
            # Oracle stores identifiers in uppercase; Debezium emits uppercase
            # JSON keys. Lowercase everything before any schema/MERGE operation.
            if source.source_key == "oracle":
                _TS_SUFFIXES_ORA = ("_at", "_ts", "_time", "_date", "_updated", "_created")

                def _normalise_oracle_payload(s):
                    if not s:
                        return s
                    d = json.loads(s)
                    out = {}
                    for k, v in d.items():
                        lk = k.lower()
                        out[lk] = v
                    return json.dumps(out)

                _lower_payload = F.udf(_normalise_oracle_payload, StringType())
                payload_df = payload_df.withColumn(
                    "payload_json", _lower_payload(col("payload_json"))
                )
                full_envelope_df = full_envelope_df.withColumn(
                    "before",
                    F.when(col("before").isNotNull(), _lower_payload(col("before"))),
                ).withColumn(
                    "after",
                    F.when(col("after").isNotNull(), _lower_payload(col("after"))),
                )

            # ── Schema: read from Iceberg on first batch, cache forever ────────
            # Iceberg is the single source of truth for column types.
            # Schema is NEVER re-inferred from batch data.
            # DDL changes require running ddl_apply.py before they take effect here.
            cache_key = (source.source_key, table_name)
            inferred_schema = _SCHEMA_CACHE.get(cache_key)

            effective_table = (
                f"{table_name}_hist" if write_mode == _WRITE_MODE_HISTORY_TRACKING
                else f"{table_name}_sd" if write_mode == _WRITE_MODE_SOFT_DELETE
                else table_name
            )
            _fqn_describe = f"`{source.catalog}`.`{namespace}`.`{effective_table}`"

            if inferred_schema is None:
                # First batch for this table — try to seed schema from Iceberg.
                try:
                    _ice_rows = spark.sql(f"DESCRIBE TABLE {_fqn_describe}").collect()
                    _ice_fields = []
                    for row in _ice_rows:
                        # DESCRIBE TABLE returns real columns first, then a blank
                        # separator row, then partition rows ('Part 0', 'Part 1', …)
                        # and '# Partitioning' / '# Partition Information' headers.
                        # Stop at the blank separator — everything after it is metadata.
                        if not row["col_name"]:
                            break
                        if row["col_name"].startswith("#"):
                            continue
                        _ice_t = row["data_type"].lower().split("(")[0].strip()
                        _spark_t = _ICE_TO_SPARK.get(_ice_t, StringType())
                        _ice_fields.append(StructField(row["col_name"].lower(), _spark_t, True))
                    if _ice_fields:
                        inferred_schema = StructType(_ice_fields)
                        _SCHEMA_CACHE[cache_key] = inferred_schema
                        logger.info(
                            "[%s/%s] Schema loaded from Iceberg (%d fields).",
                            source.source_key, effective_table, len(_ice_fields),
                        )
                except Exception:
                    pass  # Table doesn't exist yet — will be inferred from batch below

            if inferred_schema is None:
                # Table doesn't exist yet — infer schema from the first batch.
                # This only runs ONCE (the very first time this table is seen).
                try:
                    _fresh = spark.read.json(
                        payload_df.select("payload_json").rdd.map(lambda r: r[0])
                    ).schema

                    # Lowercase Oracle field names
                    if source.source_key == "oracle":
                        _fresh = StructType([
                            StructField(f.name.lower(), f.dataType, f.nullable)
                            for f in _fresh.fields
                        ])
                        # Fix Oracle timestamp columns (microseconds → TimestampType)
                        _needs_ts_fix = {
                            f.name.lower()
                            for f in _fresh.fields
                            if isinstance(f.dataType, LongType)
                            and any(f.name.lower().endswith(s) for s in _TS_SUFFIXES_ORA)
                        }
                        if _needs_ts_fix:
                            _fresh = StructType([
                                StructField(
                                    f.name,
                                    TimestampType() if f.name in _needs_ts_fix else f.dataType,
                                    f.nullable,
                                )
                                for f in _fresh.fields
                            ])

                    # Strip BSON _id and any struct with '$' sub-fields (MongoDB)
                    _fresh = StructType([
                        f for f in _fresh.fields
                        if f.name.lower() != "_id"
                        and not (
                            hasattr(f.dataType, "fields")
                            and any("$" in sf.name for sf in f.dataType.fields)
                        )
                    ])

                    inferred_schema = _fresh
                    _SCHEMA_CACHE[cache_key] = inferred_schema
                    logger.info(
                        "[%s/%s] Schema inferred from first batch (%d fields) — "
                        "table will be auto-created.",
                        source.source_key, effective_table, len(inferred_schema.fields),
                    )
                except Exception as exc:
                    logger.warning(
                        "[%s/%s] Schema inference failed: %s — skipping batch.",
                        source.source_key, table_name, exc,
                    )
                    continue

            # ── Oracle timestamp fix: divide microsecond fields by 1000 ────────
            if source.source_key == "oracle":
                _needs_ts_fix_names = {
                    f.name for f in inferred_schema.fields
                    if isinstance(f.dataType, TimestampType)
                }
                if _needs_ts_fix_names:
                    def _fix_ora_ts(s, _fix=_needs_ts_fix_names):
                        if not s:
                            return s
                        d = json.loads(s)
                        for k in list(d.keys()):
                            if k.lower() in _fix and isinstance(d[k], (int, float)):
                                d[k] = d[k] / 1000
                        return json.dumps(d)

                    _fix_udf = F.udf(_fix_ora_ts, StringType())
                    payload_df = payload_df.withColumn(
                        "payload_json", _fix_udf(col("payload_json"))
                    )

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

            # ── PK column — always lowercase for Oracle ───────────────────────
            if source.source_key == "oracle":
                pk_col_actual = source.pk_col.lower()
            else:
                pk_col_actual = source.pk_col
                for f in inferred_schema.fields:
                    if f.name.lower() == source.pk_col.lower():
                        pk_col_actual = f.name
                        break

            # ── Build write-mode-specific extra schema fields ─────────────────
            extra_fields: list[StructField] = []
            if write_mode == _WRITE_MODE_SOFT_DELETE:
                extra_fields = [
                    StructField("is_deleted", BooleanType(),  True),
                    StructField("deleted_at", TimestampType(), True),
                ]
            elif write_mode == _WRITE_MODE_HISTORY_TRACKING:
                extra_fields = [
                    StructField("_change_type", StringType(),    True),
                    StructField("_change_ts",   TimestampType(), True),
                ]

            full_schema = StructType(inferred_schema.fields + extra_fields)

            # ── Effective table name per write mode ───────────────────────────
            effective_fqn_bt = f"`{source.catalog}`.`{namespace}`.`{effective_table}`"
            effective_fqn_pl = f"{source.catalog}.{namespace}.{effective_table}"
            fqn_backtick = effective_fqn_bt
            fqn_plain    = effective_fqn_pl
            table_name_eff = effective_table

            # ── Auto-create Iceberg table if needed ───────────────────────────
            if write_mode != _WRITE_MODE_HISTORY_TRACKING:
                table_exists = builder.table_exists(source.catalog, namespace, effective_table)
                if not table_exists:
                    pk_col_exists = any(
                        f.name.lower() == pk_col_actual.lower()
                        for f in full_schema.fields
                    )
                    pk_for_bucket = pk_col_actual if pk_col_exists else "snap_id"
                    _BUCKET_COMPATIBLE_TYPES = (
                        "LongType", "IntegerType", "ShortType", "ByteType", "StringType",
                    )
                    _pk_field_type = next(
                        (f.dataType for f in full_schema.fields
                         if f.name.lower() == pk_for_bucket.lower()),
                        None,
                    )
                    _pk_type_name = type(_pk_field_type).__name__ if _pk_field_type else "unknown"
                    if _pk_type_name in _BUCKET_COMPATIBLE_TYPES:
                        _pk_partition = IcebergTableBuilder.bucket(pk_for_bucket, 16)
                    else:
                        logger.info(
                            "[%s/%s] PK %r has type %s — not bucket-compatible; "
                            "using identity(snap_timestamp) partition.",
                            source.source_key, effective_table, pk_for_bucket, _pk_type_name,
                        )
                        _pk_partition = IcebergTableBuilder.identity("snap_timestamp")
                    effective_loc = (
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
                                _pk_partition,
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
                    except Exception as create_exc:
                        logger.warning(
                            "[%s/%s] Table creation failed (may already exist): %s",
                            source.source_key, effective_table, create_exc,
                        )
                    if not builder.table_exists(source.catalog, namespace, effective_table):
                        logger.error(
                            "[%s/%s] Table does not exist after create attempt — "
                            "skipping batch %d.",
                            source.source_key, effective_table, batch_id,
                        )
                        continue

                # ── Rename uppercase Oracle PK column to lowercase in Iceberg ─
                if source.source_key == "oracle":
                    try:
                        _desc_rows = spark.sql(f"DESCRIBE TABLE {fqn_backtick}").collect()
                        for _dr in _desc_rows:
                            _cn = _dr["col_name"]
                            if _cn.startswith("#"):
                                continue
                            if _cn != _cn.lower():
                                try:
                                    spark.sql(
                                        f"ALTER TABLE {fqn_backtick} "
                                        f"RENAME COLUMN `{_cn}` TO `{_cn.lower()}`"
                                    )
                                    logger.info(
                                        "[%s/%s] Renamed uppercase column `%s` → `%s`",
                                        source.source_key, effective_table, _cn, _cn.lower(),
                                    )
                                except Exception:
                                    pass
                    except Exception as _rename_exc:
                        logger.debug(
                            "[%s/%s] Column case normalisation skipped: %s",
                            source.source_key, effective_table, _rename_exc,
                        )

            # ── Apply write mode ──────────────────────────────────────────────
            try:
                if write_mode == _WRITE_MODE_STANDARD:
                    _apply_standard(
                        spark, row_df, fqn_backtick,
                        pk_col_actual, source.source_key, table_name_eff, batch_id,
                    )
                elif write_mode == _WRITE_MODE_SOFT_DELETE:
                    _apply_soft_delete(
                        spark, row_df, fqn_backtick,
                        pk_col_actual, source.source_key, table_name_eff, batch_id,
                    )
                elif write_mode == _WRITE_MODE_HISTORY_TRACKING:
                    envelope_rows = full_envelope_df.filter(
                        F.col("_op").isin("c", "u", "d", "r")
                    )
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
                        pk_col_actual, source.source_key, table_name_eff, batch_id,
                        before_schema=before_schema,
                        after_schema=after_schema,
                        row_schema=inferred_schema,
                    )

                written_tables.append(fqn_backtick)
            except Exception as write_exc:
                logger.error(
                    "[%s/%s] Write failed (mode=%s): %s",
                    source.source_key, table_name_eff, write_mode, write_exc,
                    exc_info=True,
                )

        if written_tables:
            logger.info(
                "[%s] batch=%d — %d table(s) written.",
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
) -> StreamingQuery:
    """
    Build and start the Structured Streaming query for a single CDC source.
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
        "startingOffsets":                "earliest",
        "maxOffsetsPerTrigger":           str(MAX_OFFSETS_PER_TRIGGER),
        "failOnDataLoss":                 "false",
        "kafka.fetch.min.bytes":          "131072",
        "kafka.fetch.wait.max.ms":        "500",
        "kafka.max.poll.records":         "2000",
        "kafka.max.partition.fetch.bytes": "2097152",
        "kafka.receive.buffer.bytes":     "1048576",
    }

    # ── Checkpoint-topic mismatch guard ──────────────────────────────────────
    # When new Kafka topics appear (new table added to Debezium), Spark's
    # SubscribePattern picks them up on restart. If the checkpoint only knows old
    # topic-partitions, Spark hangs indefinitely. Clear the checkpoint so Spark
    # restarts from earliest on the new topics.
    try:
        import re as _re
        from confluent_kafka.admin import AdminClient as _AdminClient

        _admin = _AdminClient({
            "bootstrap.servers":  KAFKA_BOOTSTRAP,
            "security.protocol":  "SASL_PLAINTEXT",
            "sasl.mechanism":     "SCRAM-SHA-512",
            "sasl.username":      kafka_user,
            "sasl.password":      kafka_pass,
            "socket.timeout.ms":  "8000",
        })
        _meta          = _admin.list_topics(timeout=10)
        _pat           = _re.compile(source.topic_pattern)
        _live_topics   = {t for t in _meta.topics if _pat.match(t)}

        _s3_bao   = BaoSparkInit()
        _s3_creds = _s3_bao.s3_creds()
        import boto3 as _boto3
        _s3 = _boto3.client(
            "s3",
            endpoint_url          = _s3_creds["endpoint"],
            aws_access_key_id     = _s3_creds["access_key"],
            aws_secret_access_key = _s3_creds["secret_key"],
            region_name           = _s3_creds.get("region", "us-east-1"),
        )
        _bucket        = S3_BUCKET
        _offset_prefix = source.checkpoint.replace(f"s3://{_bucket}/", "") + "/offsets/"
        _paginator     = _s3.get_paginator("list_objects_v2")
        _offset_files  = sorted(
            [o["Key"] for p in _paginator.paginate(Bucket=_bucket, Prefix=_offset_prefix)
             for o in p.get("Contents", [])],
            reverse=True,
        )

        if _offset_files:
            _latest  = _s3.get_object(Bucket=_bucket, Key=_offset_files[0])
            _lines   = _latest["Body"].read().decode().strip().splitlines()
            if len(_lines) >= 3:
                _ckpt_topics = set(json.loads(_lines[2]).keys())
                _new_topics  = _live_topics - _ckpt_topics
                if _new_topics:
                    logger.warning(
                        "[%s] New topics not in checkpoint: %s. Clearing checkpoint.",
                        source.source_key, sorted(_new_topics),
                    )
                    _ckpt_prefix = source.checkpoint.replace(f"s3://{_bucket}/", "") + "/"
                    _all_ckpt    = [
                        o["Key"]
                        for p in _paginator.paginate(Bucket=_bucket, Prefix=_ckpt_prefix)
                        for o in p.get("Contents", [])
                    ]
                    for _i in range(0, len(_all_ckpt), 1000):
                        _s3.delete_objects(
                            Bucket=_bucket,
                            Delete={"Objects": [{"Key": k} for k in _all_ckpt[_i:_i+1000]]},
                        )
                    logger.warning(
                        "[%s] Checkpoint cleared (%d objects). Restarting from earliest.",
                        source.source_key, len(_all_ckpt),
                    )
                else:
                    logger.info(
                        "[%s] Checkpoint-topic guard: OK (live=%d, ckpt=%d).",
                        source.source_key, len(_live_topics), len(_ckpt_topics),
                    )
    except Exception as _ckpt_guard_exc:
        logger.warning(
            "[%s] Checkpoint-topic guard failed (non-fatal): %s",
            source.source_key, _ckpt_guard_exc,
        )

    raw_stream = (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .load()
    )

    try:
        from confluent_kafka.schema_registry import SchemaRegistryClient  # noqa: F401
        avro_udf = _build_avro_deserialize_udf(SR_URL)
    except ImportError:
        logger.warning(
            "[%s] confluent-kafka not available; treating values as plain JSON.",
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

    transform_steps = _build_transform_pipeline(source.source_key, source.pk_col)

    query = (
        decoded_stream
        .writeStream
        .queryName(f"cdc-{source.source_key}-{write_mode}")
        .foreachBatch(
            _write_micro_batch(
                spark, builder, source, write_mode, transform_steps
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

    conf.set("spark.cores.max",          str(MAX_EXECUTORS * EXECUTOR_CORES))
    conf.set("spark.executor.instances", str(EXECUTOR_INSTANCES))
    conf.set("spark.executor.cores",     str(EXECUTOR_CORES))

    conf.set("spark.dynamicAllocation.enabled",                          "true")
    conf.set("spark.dynamicAllocation.minExecutors",                     "0")
    conf.set("spark.dynamicAllocation.maxExecutors",                     str(MAX_EXECUTORS))
    conf.set("spark.dynamicAllocation.executorIdleTimeout",              "30s")
    conf.set("spark.dynamicAllocation.schedulerBacklogTimeout",          "1s")
    conf.set("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", f"{BURST_BACKLOG_TIMEOUT_S}s")
    conf.set("spark.dynamicAllocation.shuffleTracking.enabled",          "true")

    conf.set("spark.executor.memory",    EXECUTOR_MEMORY)
    conf.set("spark.memory.offHeap.size", EXECUTOR_OFFHEAP)
    conf.set("spark.sql.adaptive.enabled",                               "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled",            "true")
    conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize",   "33554432")
    conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes",          ADAPTIVE_COALESCE_TARGET)
    conf.set("spark.sql.adaptive.skewJoin.enabled",                      "true")
    conf.set("spark.sql.shuffle.partitions",                             str(MERGE_PARALLELISM))
    conf.set("spark.sql.iceberg.write.fanout.enabled",                   "true")
    conf.set("spark.sql.iceberg.merge.cardinality-check.enabled",        "false")
    conf.set("spark.serializer",
             "org.apache.spark.serializer.JavaSerializer")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── Main: streaming loop with auto-restart ────────────────────────────────────

def _start_all_queries(
    spark:   SparkSession,
    builder: IcebergTableBuilder,
    bao:     BaoSparkInit,
) -> list[StreamingQuery]:
    queries: list[StreamingQuery] = []
    for src in _ALL_SOURCES:
        try:
            q = _start_source_stream(spark, builder, src, bao, WRITE_MODE)
            queries.append(q)
        except Exception as exc:
            logger.error(
                "[%s] Failed to start stream: %s", src.source_key, exc, exc_info=True,
            )
    return queries


def _run_once(bao: BaoSparkInit) -> None:
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

    if _TARGET_NAMESPACE:
        logger.info(
            "TARGET_NAMESPACE=%r — ensuring override namespace in all active catalogs.",
            _TARGET_NAMESPACE,
        )
        for src in _ALL_SOURCES:
            try:
                builder.ensure_namespace(src.catalog, _TARGET_NAMESPACE)
                logger.info("[%s] Namespace '%s' ready.", src.source_key, _TARGET_NAMESPACE)
            except Exception as exc:
                logger.warning(
                    "[%s] Could not ensure namespace %r: %s",
                    src.source_key, _TARGET_NAMESPACE, exc,
                )

    queries = _start_all_queries(spark, builder, bao)
    if not queries:
        spark.stop()
        raise RuntimeError("No streaming queries started.")

    logger.info(
        "All %d streaming queries active (mode=%s, trigger=%s, "
        "merge_parallelism=%d, coalesce=%d).",
        len(queries), WRITE_MODE, TRIGGER_INTERVAL,
        MERGE_PARALLELISM, COALESCE_BEFORE_MERGE,
    )

    try:
        while True:
            time.sleep(1)
            for q in list(queries):
                if not q.isActive:
                    raise RuntimeError(
                        f"Streaming query '{q.name}' terminated unexpectedly."
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


# ── HTTP health server ─────────────────────────────────────────────────────────
_HEALTH_STATE: dict = {"healthy": True}


class _HealthHandler(http.server.BaseHTTPRequestHandler):
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
        pass


def _start_health_server() -> None:
    server = http.server.HTTPServer(("0.0.0.0", HEALTH_PORT), _HealthHandler)
    t = threading.Thread(target=server.serve_forever, name="health-server", daemon=True)
    t.start()
    logger.info("Health server listening on port %d", HEALTH_PORT)


def main() -> None:
    os.environ["SPARK_USER"] = SPARK_USER

    logger.info(
        "=== Kafka→Iceberg | user=%s | mode=%s | sources=%s | "
        "transform=%s | dry_run=%s | trigger=%s | max_offsets=%d "
        "| executors=0→1 core (burst up to %d after %ds backlog) "
        "| mem=%s off-heap=%s | max_restart_attempts=%s ===",
        SPARK_USER, WRITE_MODE,
        [s.source_key for s in _ALL_SOURCES],
        _TRANSFORM_STEPS or "none",
        DRY_RUN, TRIGGER_INTERVAL, MAX_OFFSETS_PER_TRIGGER,
        MAX_EXECUTORS, BURST_BACKLOG_TIMEOUT_S,
        EXECUTOR_MEMORY, EXECUTOR_OFFHEAP,
        MAX_RESTART_ATTEMPTS if MAX_RESTART_ATTEMPTS > 0 else "∞",
    )

    _start_health_server()

    bao = BaoSparkInit()
    attempt = 0
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
            _HEALTH_STATE["healthy"] = False
            time.sleep(backoff)


if __name__ == "__main__":
    main()
