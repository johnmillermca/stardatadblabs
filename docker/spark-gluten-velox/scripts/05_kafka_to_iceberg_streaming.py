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
• snap_id        BIGINT    — globally unique per row: (batch_id * 10_000_000) + monotonically_increasing_id()
                             batch_id is the Spark Structured Streaming micro-batch counter (monotonically
                             increasing per streaming query lifetime), ensuring uniqueness even when a batch
                             contains only 1 row (where monotonically_increasing_id alone repeats).
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

# Peak-hour performance tuning
# Number of shuffle partitions used during MERGE operations.
# CDC batches are small (1–1000 rows); keep low to avoid task scheduling overhead.
# Raise to 16–32 only for bulk-load catch-up scenarios.
MERGE_PARALLELISM = int(os.environ.get("MERGE_PARALLELISM", "4"))

# Coalesce batch DataFrame partitions before MERGE.
# 1 is optimal for low-row-count CDC batches — avoids shuffle overhead.
COALESCE_BEFORE_MERGE = int(os.environ.get("COALESCE_BEFORE_MERGE", "1"))

# AQE adaptive coalesce target bytes per post-shuffle partition (64 MB default).
ADAPTIVE_COALESCE_TARGET = os.environ.get("ADAPTIVE_COALESCE_TARGET", "67108864")

# Executor sizing — keep small so the streaming job does not starve other Spark
# jobs on the cluster.  CDC micro-batches are tiny (1–1000 rows); a single
# executor with 1 core is plenty.  Raise via env vars for bulk-load catch-up.
EXECUTOR_INSTANCES = int(os.environ.get("EXECUTOR_INSTANCES", "1"))
EXECUTOR_CORES     = int(os.environ.get("EXECUTOR_CORES",     "1"))
EXECUTOR_MEMORY    = os.environ.get("EXECUTOR_MEMORY",        "2g")
EXECUTOR_OFFHEAP   = os.environ.get("EXECUTOR_OFFHEAP",       "512m")

# Maximum executors the dynamic allocator may scale up to under sustained load.
# Default 3: baseline = 1 executor (1 core); burst = up to 3 executors (3 cores)
# after BURST_BACKLOG_TIMEOUT_S seconds of sustained task backlog.
# Set to 1 to disable burst (hard cap at 1 core always).
MAX_EXECUTORS = int(os.environ.get("MAX_EXECUTORS", "3"))

# How long (seconds) the scheduler backlog must be sustained before the dynamic
# allocator requests an additional executor (2nd and beyond).
# Default 60: a job must be backlogged for 60 s before getting an extra core,
# so short CDC micro-batches never consume more than 1 core unnecessarily.
BURST_BACKLOG_TIMEOUT_S = int(os.environ.get("BURST_BACKLOG_TIMEOUT_S", "60"))

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


# ── Per-table schema cache ────────────────────────────────────────────────────
# Keyed by (source_key, table_name) → StructType.
# Schema inference via spark.read.json(rdd) costs ~2 s per batch because it
# launches a full Spark job to sample the JSON.  After the first inference the
# schema is stable for the lifetime of the pipeline, so cache and reuse it.
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
        pk_col:        "str | None",
        s3_prefix:     str,
    ) -> None:
        self.source_key    = source_key
        self.topic_pattern = topic_pattern
        self.catalog       = catalog
        self.namespace     = namespace
        self.pk_col        = pk_col   # None → resolved dynamically per-batch from schema
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
        topic_pattern = "oracle\\.(tpcds|cache_testing)\\..*",
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
        pk_col        = None,            # Debezium MongoDB _id is a BSON struct<$oid:string>
                                         # which cannot be used as an Iceberg MERGE key.
                                         # pk_col=None → _resolve_pk() detects the business PK
                                         # dynamically from the inferred schema at batch time,
                                         # making this source-config table-agnostic.
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


# ── Dynamic PK resolver ────────────────────────────────────────────────────────

def _resolve_pk(schema: "StructType", table_name: str) -> str:
    """
    Determine the business primary-key column name from an inferred schema.

    Used when _StreamingSource.pk_col is None (e.g. MongoDB, where _id is an
    unserializable BSON struct and cannot serve as the Iceberg MERGE key).

    Resolution order — first match wins:
      1. ``id``                         — canonical single-column PK
      2. ``<singular(table_name)>_id``  — e.g. table "customers" → "customer_id"
                                          table "orders"          → "order_id"
      3. ``<table_name>_id``            — direct table-name prefix match
      4. First column whose name ends with ``_id`` (excluding ``_id`` itself)
      5. ``snap_id`` excluded — it is a pipeline audit column, not a source PK
      6. Fallback: first non-``_id`` column in the schema (last resort — logs a warning)

    The comparison is case-insensitive; the returned name preserves the original
    casing from the schema so downstream MERGE ON clauses use the correct identifier.
    """
    col_names = [f.name for f in schema.fields]
    lower_map = {f.name.lower(): f.name for f in schema.fields}  # lower → original

    # 1. "id"
    if "id" in lower_map:
        return lower_map["id"]

    # 2. singular(<table>)_id  — strip common plural suffixes
    singular = table_name.rstrip("s")  # "customers" → "customer", "orders" → "order"
    candidate = f"{singular}_id"
    if candidate in lower_map:
        return lower_map[candidate]

    # 3. <table>_id  (table name without modification)
    candidate = f"{table_name}_id"
    if candidate in lower_map:
        return lower_map[candidate]

    # 4. first *_id col that isn't "_id" or "snap_id"
    _EXCLUDE = {"_id", "snap_id"}
    for name in col_names:
        if name.lower().endswith("_id") and name.lower() not in _EXCLUDE:
            return name

    # 5. fallback — use first non-_id column (very unlikely, emit a warning)
    for name in col_names:
        if name.lower() != "_id":
            import logging as _logging
            _logging.getLogger(__name__).warning(
                "Could not determine PK for table %r from schema %s — "
                "falling back to first non-_id column %r. "
                "Consider setting pk_col explicitly on the _StreamingSource.",
                table_name, [f.name for f in schema.fields], name,
            )
            return name

    raise ValueError(
        f"Cannot determine a PK column for table {table_name!r}; "
        f"schema has no usable columns: {[f.name for f in schema.fields]}"
    )


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
    pk_col:     "str | None",
) -> list[tuple[Any, dict]]:
    """
    Build the list of (fn, kwargs) steps from TRANSFORM_STEPS env config.
    Called once per source at stream startup.

    When ``pk_col`` is None (dynamic PK source such as MongoDB), the
    ``deduplicate`` step is omitted from the returned list — it will be
    injected per-batch inside ``_write_micro_batch`` once the PK has been
    resolved from the inferred schema via ``_resolve_pk()``.
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
        if step_name == "deduplicate" and pk_col is None:
            # PK not known at startup — defer to per-batch injection
            logger.debug(
                "[%s] 'deduplicate' step deferred to per-batch (pk_col=None, "
                "will be resolved dynamically from inferred schema).",
                source_key,
            )
            continue
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
      MERGE MATCHED     → UPDATE all CDC columns (snap_id/snap_timestamp untouched —
                          excluded from SET so existing audit values are preserved)
      MERGE NOT MATCHED → INSERT CDC columns + fresh snap_id/snap_timestamp

    DELETE (op d):
      MERGE MATCHED → DELETE row from Iceberg (hard delete)

    snap_id / snap_timestamp injection strategy
    -------------------------------------------
    We call .withColumn() on the source DataFrame, then immediately materialise
    it with .cache() + .count() BEFORE createOrReplaceGlobalTempView().

    Why materialise?
      Spark's Iceberg MERGE planner (ReplaceData path) walks the FULL logical
      plan of the USING source — including the plan of any global temp view it
      references.  If monotonically_increasing_id() or current_timestamp() are
      still present as unevaluated expressions anywhere in that plan tree, Spark
      raises INVALID_NON_DETERMINISTIC_EXPRESSIONS even though those expressions
      are in the source, not the join condition.

      .cache() + .count() forces Spark to execute the DataFrame and store the
      result as an InMemoryRelation.  The global temp view then points to that
      static relation — the MERGE planner sees no live non-deterministic
      functions and proceeds normally.

      The cached DataFrame is unpersisted immediately after the MERGE to avoid
      memory pressure between batches.

    snap_id / snap_timestamp are excluded from the MATCHED SET clause so an
    UPDATE never overwrites the audit values stamped at INSERT time.
    """
    spark.conf.set("spark.sql.shuffle.partitions", str(MERGE_PARALLELISM))

    _SNAP_COLS = {"snap_id", "snap_timestamp"}

    # ── Dedup within batch: last-write-wins per PK ────────────────────────────
    # MongoDB CDC (and any high-frequency source) can produce multiple events for
    # the same PK within one micro-batch (e.g. INSERT followed immediately by an
    # UPDATE, or two updates to the same document).  MERGE INTO Iceberg raises
    # MERGE_CARDINALITY_VIOLATION when the source has >1 row matching a single
    # target row on the join key.  Deduplicate here *before* dropping kafka_ts
    # (we need it to pick the latest event per PK).  It is a no-op for sources
    # that never emit duplicate PKs in one batch.
    _inserts_raw = payload_df.filter(col("_op").isin("c", "u", "r"))
    _deletes_raw = payload_df.filter(col("_op") == "d")

    if not _inserts_raw.isEmpty() and pk_col in _inserts_raw.columns:
        from pyspark.sql import Window as _W
        _win = _W.partitionBy(col(f"`{pk_col}`")).orderBy(col("kafka_ts").desc())
        _inserts_raw = (
            _inserts_raw
            .withColumn("_rn", F.row_number().over(_win))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )
    if not _deletes_raw.isEmpty() and pk_col in _deletes_raw.columns:
        _ins_pks = {r[0] for r in _inserts_raw.select(pk_col).collect()} if not _inserts_raw.isEmpty() else set()
        _deletes_raw = _deletes_raw.dropDuplicates([pk_col])
        # If a PK appears in both inserts and deletes in the same batch,
        # the insert (later event) wins — drop the delete for that PK.
        if _ins_pks:
            _deletes_raw = _deletes_raw.filter(~col(f"`{pk_col}`").isin(list(_ins_pks)))

    inserts = _inserts_raw.drop("_op", "kafka_ts")
    deletes = _deletes_raw.drop("_op", "kafka_ts")

    if not inserts.isEmpty():
        raw_df = (
            inserts
            .coalesce(COALESCE_BEFORE_MERGE)
            .withColumn("snap_id",        (lit(batch_id).cast(LongType()) * lit(10_000_000).cast(LongType())
                                           + monotonically_increasing_id().cast(LongType())))
            .withColumn("snap_timestamp", current_timestamp())
        )
        # Fully break streaming lineage before registering the global temp view.
        #
        # Problem: createOrReplaceGlobalTempView() registers the *logical plan*
        # of the DataFrame, not its data.  When the source DataFrame still carries
        # the streaming LogicalRDD in its lineage (even after .cache()+.count()),
        # Spark's Iceberg MERGE planner (ReplaceData path) traverses that full
        # plan tree and flags monotonically_increasing_id() / current_timestamp()
        # as INVALID_NON_DETERMINISTIC_EXPRESSIONS.
        #
        # Fix: .collect() pulls the rows to the driver, then spark.createDataFrame()
        # builds a brand-new static DataFrame backed by a LocalRelation — completely
        # detached from the streaming LogicalRDD.  The MERGE planner sees only a
        # plain local table with no live expressions in its lineage.
        rows      = raw_df.collect()
        final_df  = spark.createDataFrame(rows, raw_df.schema)
        row_count = len(rows)
        tmp_view  = f"__cdc_upsert_{source_key}_{table_name}_{batch_id}"
        final_df.createOrReplaceGlobalTempView(tmp_view)
        # Exclude snap columns from SET — preserve the values written at INSERT time.
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

    INSERT/UPDATE/snapshot (op c/u/r):
      MERGE MATCHED     → UPDATE CDC columns + is_deleted=false, deleted_at=NULL
                          (snap_id/snap_timestamp excluded from SET — preserved)
      MERGE NOT MATCHED → INSERT CDC columns + is_deleted=false, deleted_at=NULL
                          + fresh snap_id/snap_timestamp

    DELETE (op d):
      MERGE MATCHED → UPDATE SET is_deleted=true, deleted_at=<now>
      Row is never physically removed from Iceberg.

    Same .cache()/.count() materialisation strategy as _apply_standard —
    see that function's docstring for the full rationale.
    """
    spark.conf.set("spark.sql.shuffle.partitions", str(MERGE_PARALLELISM))

    _SNAP_COLS = {"snap_id", "snap_timestamp"}

    # ── Dedup within batch + nullable PK (same pattern as _apply_standard) ────
    _inserts_raw = payload_df.filter(col("_op").isin("c", "u", "r"))
    _deletes_raw = payload_df.filter(col("_op") == "d")

    if not _inserts_raw.isEmpty() and pk_col in _inserts_raw.columns:
        from pyspark.sql import Window as _W
        _win = _W.partitionBy(col(f"`{pk_col}`")).orderBy(col("kafka_ts").desc())
        _inserts_raw = (
            _inserts_raw
            .withColumn("_rn", F.row_number().over(_win))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )
    if not _deletes_raw.isEmpty() and pk_col in _deletes_raw.columns:
        _ins_pks = {r[0] for r in _inserts_raw.select(pk_col).collect()} if not _inserts_raw.isEmpty() else set()
        _deletes_raw = _deletes_raw.dropDuplicates([pk_col])
        if _ins_pks:
            _deletes_raw = _deletes_raw.filter(~col(f"`{pk_col}`").isin(list(_ins_pks)))

    inserts = _inserts_raw.drop("_op", "kafka_ts")
    deletes = _deletes_raw.drop("_op", "kafka_ts")

    # ── Make PK column nullable to avoid Velox NullPointerException ───────────
    # MongoDB documents may arrive without the PK field (e.g. partial update
    # events that set a new field on a document but don't include customer_id
    # in the after image).  If the Iceberg table was created with pk NOT NULL
    # (inferred from first batch where all rows had the PK), Velox/Gluten will
    # crash with "Null value appeared in non-nullable field: <pk>".
    # Cast the PK to nullable here so the MERGE source always allows NULLs —
    # MERGE ON condition handles NULL safely (NULL != anything → no match →
    # row is skipped, not inserted or updated).
    if pk_col in inserts.columns:
        pk_type = inserts.schema[pk_col].dataType
        inserts = inserts.withColumn(pk_col, col(f"`{pk_col}`").cast(pk_type))
        # Force nullable via schema reconstruction
        from pyspark.sql.types import StructField as _SF, StructType as _ST
        new_fields = [
            _SF(f.name, f.dataType, True) if f.name == pk_col else f
            for f in inserts.schema.fields
        ]
        inserts = spark.createDataFrame(inserts.collect(), _ST(new_fields))

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
        # Same streaming-lineage break as _apply_standard — see that docstring.
        rows      = raw_df.collect()
        final_df  = spark.createDataFrame(rows, raw_df.schema)
        row_count = len(rows)
        tmp_view  = f"__cdc_upsert_{source_key}_{table_name}_{batch_id}"
        final_df.createOrReplaceGlobalTempView(tmp_view)
        # Exclude snap columns from SET — preserve the values written at INSERT time.
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

    Columns written on every row:
      _change_type    STRING     — INSERT / UPDATE / DELETE
      _change_ts      TIMESTAMP  — pipeline processing time
      after_<col>     typed      — row state after  the change (NULL for DELETE)
      before_<col>    typed      — row state before the change (NULL for INSERT)
      snap_id         BIGINT     — unique row id within batch
      snap_timestamp  TIMESTAMP  — write-time wall clock

    Schema stability guarantee
    --------------------------
    Every batch always produces the full column set regardless of which op
    types (INSERT / UPDATE / DELETE) are present:

    • after_* NULLed for DELETE batches  (no "after" image in Debezium envelope)
    • before_* NULLed for INSERT batches (no "before" image in Debezium envelope)

    Both image column sets are backfilled from row_schema (the inferred source
    schema cached on the first batch).  This ensures the DataFrame schema is
    identical on every call so writeTo().append() never encounters a column
    mismatch regardless of batch composition.

    Table-creation strategy
    -----------------------
    writeTo().append() requires the table to exist.  On the first batch we
    build a CREATE TABLE DDL directly from the full final_df schema (which
    already contains both after_* and before_* columns) so the table is
    created once with the complete stable schema.  No mergeSchema surprises.
    """
    _PY_TO_ICEBERG = {
        "LongType":      "BIGINT",
        "IntegerType":   "INT",
        "StringType":    "STRING",
        "DoubleType":    "DOUBLE",
        "FloatType":     "FLOAT",
        "BooleanType":   "BOOLEAN",
        "TimestampType": "TIMESTAMP",
        "DateType":      "DATE",
    }

    # ── 1. Map op codes to human-readable change types ────────────────────────
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

    # ── 2. Expand "before" image (UPDATE / DELETE rows only) ─────────────────
    result_df = typed_df
    if before_schema is not None and "before" in typed_df.columns:
        parsed_before = from_json(col("before"), before_schema)
        for field in before_schema.fields:
            result_df = result_df.withColumn(
                f"before_{field.name}", parsed_before[field.name]
            )
    if "before" in result_df.columns:
        result_df = result_df.drop("before")

    # ── 3. Expand "after" image (INSERT / UPDATE rows only) ──────────────────
    if after_schema is not None and "after" in result_df.columns:
        parsed_after = from_json(col("after"), after_schema)
        for field in after_schema.fields:
            result_df = result_df.withColumn(
                f"after_{field.name}", parsed_after[field.name]
            )
    if "after" in result_df.columns:
        result_df = result_df.drop("after")

    # ── 4. Backfill missing image columns with typed NULLs ───────────────────
    # after_* absent on DELETE batches; before_* absent on INSERT batches.
    # Both must be present on every batch so the schema never drifts between
    # calls — writeTo().append() fails if the DataFrame is missing any column
    # that already exists in the Iceberg table.
    if row_schema is not None:
        for field in row_schema.fields:
            for prefix in ("after_", "before_"):
                img_col = f"{prefix}{field.name}"
                if img_col not in result_df.columns:
                    result_df = result_df.withColumn(
                        img_col, lit(None).cast(field.dataType)
                    )

    # ── 4b. Inject top-level PK column ───────────────────────────────────────
    # DELETE events have after=null so after_<pk> is always NULL on DELETE rows.
    # Coalesce after_<pk> and before_<pk> into a single top-level identity
    # column (<pk_col>) so every row carries the entity PK regardless of op.
    after_pk  = f"after_{pk_col}"
    before_pk = f"before_{pk_col}"
    if after_pk in result_df.columns or before_pk in result_df.columns:
        _after_expr  = col(after_pk)  if after_pk  in result_df.columns else lit(None)
        _before_expr = col(before_pk) if before_pk in result_df.columns else lit(None)
        result_df = result_df.withColumn(pk_col, F.coalesce(_after_expr, _before_expr))

    # ── 4c. MongoDB history_tracking post-processing ──────────────────────────
    # Debezium MongoDB connector emits BSON extended-JSON types inside the
    # "before" / "after" JSON strings.  After JSON expansion via from_json():
    #
    #   _id        → STRUCT<$oid:STRING>   → drop after__id / before__id entirely
    #   created_at → STRUCT<$date:BIGINT>  → extract epoch_ms / 1000 → TIMESTAMP
    #   updated_at → STRUCT<$date:BIGINT>  → extract epoch_ms / 1000 → TIMESTAMP
    #   (any _at / _ts / _time col)        → same $date struct handling
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
                # STRUCT<$date:BIGINT> — extract the $date sub-field (epoch_ms)
                result_df = result_df.withColumn(
                    _c,
                    (col(f"`{_c}`").getField("$date") / lit(1_000)).cast(TimestampType()),
                )
            elif isinstance(_dtype, StringType):
                # Plain epoch-ms string or ISO-8601 string
                result_df = result_df.withColumn(
                    _c,
                    F.when(
                        col(_c).rlike(r"^\d{10,13}$"),
                        (col(_c).cast(LongType()) / lit(1_000)).cast(TimestampType()),
                    ).otherwise(col(_c).cast(TimestampType())),
                )
            elif isinstance(_dtype, LongType):
                # Raw epoch_ms integer (rare but handled)
                result_df = result_df.withColumn(
                    _c, (col(_c) / lit(1_000)).cast(TimestampType()),
                )

    # ── 5. Drop internal envelope columns ────────────────────────────────────
    _ENVELOPE_COLS = {"_op", "kafka_ts", "ts_ms"}
    result_df = result_df.drop(*[c for c in _ENVELOPE_COLS if c in result_df.columns])

    # ── 6. Break streaming lineage (identical pattern to _apply_standard) ─────
    # .collect() pulls rows to driver; spark.createDataFrame() builds a fresh
    # LocalRelation with zero lineage to the streaming source.
    # snap_id / snap_timestamp are then added to this static DataFrame so
    # monotonically_increasing_id() and current_timestamp() are evaluated
    # against a plain LocalRelation — no INVALID_NON_DETERMINISTIC_EXPRESSIONS.
    rows      = result_df.coalesce(COALESCE_BEFORE_MERGE).collect()
    final_df  = (
        spark.createDataFrame(rows, result_df.schema)
        .withColumn("snap_id",        (lit(batch_id).cast(LongType()) * lit(10_000_000).cast(LongType())
                                       + monotonically_increasing_id().cast(LongType())))
        .withColumn("snap_timestamp", current_timestamp())
    )
    row_count = len(rows)

    # ── 7. Lazy table creation ────────────────────────────────────────────────
    # writeTo().append() requires the table to already exist.
    # Create it once using the full final_df schema — which already contains
    # both after_* and before_* columns — so no mergeSchema surprises later.
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
            "[%s/%s][history_tracking] Created Iceberg table (mode=history_tracking).",
            source_key, table_name,
        )

    # ── 7b. DDL evolution for history_tracking ────────────────────────────────
    # mergeSchema=true on the REST catalog requires 'write.spark.accept-any-schema'
    # tblproperty.  For reliability on both new and existing tables, explicitly
    # issue ALTER TABLE ADD COLUMN for every after_*/before_* column in the
    # current batch that isn't already in the hist table.  This is the same
    # strategy used by standard/soft_delete modes and avoids INSERT_COLUMN_ARITY_MISMATCH.
    try:
        _hist_existing_cols = {
            row["col_name"].lower()
            for row in spark.sql(f"DESCRIBE TABLE {fqn_backtick}").collect()
            if not row["col_name"].startswith("#")
        }
        for _hf in final_df.schema.fields:
            if _hf.name.lower() in _hist_existing_cols:
                continue
            _hf_ice_type = _PY_TO_ICEBERG.get(type(_hf.dataType).__name__, "STRING")
            # Skip BSON-style names
            if "$" in _hf.name or "$" in _hf_ice_type:
                continue
            try:
                spark.sql(
                    f"ALTER TABLE {fqn_backtick} ADD COLUMN `{_hf.name}` {_hf_ice_type}"
                )
                logger.info(
                    "[%s/%s][history_tracking] ALTER TABLE ADD COLUMN `%s` %s — OK",
                    source_key, table_name, _hf.name, _hf_ice_type,
                )
            except Exception as _hf_exc:
                logger.debug(
                    "[%s/%s][history_tracking] ALTER TABLE ADD COLUMN `%s` skipped: %s",
                    source_key, table_name, _hf.name, _hf_exc,
                )
    except Exception as _hist_evo_exc:
        # Table doesn't exist yet — will be created above or on next batch
        logger.debug(
            "[%s/%s][history_tracking] Schema evolution check skipped: %s",
            source_key, table_name, _hist_evo_exc,
        )

    # ── 7c. Drop Iceberg columns absent from the current batch ────────────────
    # When a column is dropped at the source (e.g. after a DDL stress test),
    # Debezium stops emitting it.  The hist table still has the old column and
    # writeTo().append() raises CANNOT_FIND_DATA because Iceberg expects a value
    # for every existing column.  Permanently fix this by issuing ALTER TABLE
    # DROP COLUMN for every Iceberg column not present in the current batch's
    # DataFrame.  Only after_*/before_* test columns are eligible — core metadata
    # columns (snap_id, snap_timestamp, _change_type, _change_ts, pk_col) are
    # always present and never dropped.
    _PROTECTED_COLS = {
        "snap_id", "snap_timestamp", "_change_type", "_change_ts",
        pk_col.lower(), f"after_{pk_col}".lower(), f"before_{pk_col}".lower(),
    }
    try:
        _batch_cols_lower = {f.name.lower() for f in final_df.schema.fields}
        _iceberg_cols = {
            row["col_name"].lower()
            for row in spark.sql(f"DESCRIBE TABLE {fqn_backtick}").collect()
            if not row["col_name"].startswith("#")
        }
        _stale = _iceberg_cols - _batch_cols_lower - _PROTECTED_COLS
        for _stale_col in sorted(_stale):
            if "$" in _stale_col:
                continue
            try:
                spark.sql(
                    f"ALTER TABLE {fqn_backtick} DROP COLUMN `{_stale_col}`"
                )
                logger.info(
                    "[%s/%s][history_tracking] ALTER TABLE DROP COLUMN `%s` "
                    "(no longer in source schema) — OK",
                    source_key, table_name, _stale_col,
                )
            except Exception as _drop_exc:
                logger.debug(
                    "[%s/%s][history_tracking] ALTER TABLE DROP COLUMN `%s` skipped: %s",
                    source_key, table_name, _stale_col, _drop_exc,
                )
    except Exception as _stale_exc:
        logger.debug(
            "[%s/%s][history_tracking] Stale column check skipped: %s",
            source_key, table_name, _stale_exc,
        )

    # ── 8. Write ──────────────────────────────────────────────────────────────
    # Collect again after snap col injection so write_df is a clean LocalRelation
    # with the evaluated snap_id / snap_timestamp values baked in — same
    # two-collect pattern used by _apply_standard and _apply_soft_delete.
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
      4. Auto-create Iceberg table if it does not exist (schema cached after first batch).
      5. Apply write-mode handler (standard / soft_delete / history_tracking).
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

            # ── Infer schema from batch (cached per table) ────────────────────
            # DDL evolution detection strategy
            # ─────────────────────────────────────────────────────────────────
            # We must detect ADD COLUMN and RENAME COLUMN events on every batch
            # so Iceberg is evolved immediately, not after up to 9 missed batches.
            #
            # Cost model:
            #   • spark.read.json(rdd) costs ~2 s because it launches a full
            #     Spark job to sample the JSON payload.
            #   • A cheap O(1) key-scan of the first non-null payload row costs
            #     nothing — it parses only one JSON string on the driver.
            #
            # Algorithm (event-driven, zero periodic overhead):
            #   1. Extract field names from the first non-null payload row (O(1),
            #      no Spark job).
            #   2. If names == cached names (stable schema) → skip re-inference.
            #      Zero cost per batch on stable schema.
            #   3. If names differ (ADD/RENAME/DROP detected) → full re-inference
            #      via spark.read.json.  Apply DDL evolution and update cache.
            #   4. On first batch (cache miss) → always do full re-inference and
            #      seed from Iceberg schema as type authority.
            #
            # This replaces the old "% 10" periodic timer, which missed up to
            # 9 consecutive batches of DDL changes (rename storms fail silently).
            #
            # Type-stability rule (Fix: last_login_at STRING→BIGINT regression)
            # ─────────────────────────────────────────────────────────────────
            # Spark's JSON inference defaults NULL values to StringType.  A batch
            # where a nullable TIMESTAMP/BIGINT column is NULL on every row will
            # infer that column as STRING, silently downgrading the cached type.
            # Rule: NEVER replace an existing cached field's type from re-inference.
            # Only APPEND fields that are genuinely new (not in the cached schema).
            # On first inference (cache miss), seed from the live Iceberg schema
            # (DESCRIBE TABLE) when available — that schema was written from real
            # data and is authoritative.  Fall back to batch inference only when
            # the table doesn't exist yet.
            #
            # BSON _id exclusion (Fix: MongoDB $oid struct ALTER TABLE failure)
            # ─────────────────────────────────────────────────────────────────
            # The Debezium MongoDB connector emits _id as a BSON extended-JSON
            # struct<$oid:string>.  Iceberg column names cannot contain '$', so
            # ALTER TABLE ADD COLUMN `_id` struct<$oid:string> always fails with
            # PARSE_SYNTAX_ERROR.  _id is not a business column — exclude it from
            # all schema evolution paths (cache, ALTER TABLE, from_json parse).
            _MGO_BSON_EXCLUDE = {"_id"}   # fields to strip from inferred schema

            def _is_bson_struct(field) -> bool:
                """True for struct types whose sub-field names contain '$'."""
                from pyspark.sql.types import StructType as _ST2
                return (
                    isinstance(field.dataType, _ST2)
                    and any("$" in sf.name for sf in field.dataType.fields)
                )

            def _safe_iceberg_fields(fields):
                """
                Filter out fields that cannot be represented as Iceberg columns:
                  • _id (MongoDB BSON ObjectId)
                  • any struct whose sub-fields contain '$' (BSON extended JSON)
                """
                return [
                    f for f in fields
                    if f.name.lower() not in _MGO_BSON_EXCLUDE
                    and not _is_bson_struct(f)
                ]

            def _batch_field_names() -> frozenset[str]:
                """
                O(1) key scan: extract field names from the first non-null payload
                row without launching a Spark job.  Returns a frozenset of
                lowercase field names, excluding BSON-invalid names.
                """
                try:
                    first = payload_df.select("payload_json").first()
                    if first and first[0]:
                        raw_keys = set(json.loads(first[0]).keys())
                        return frozenset(
                            k.lower() for k in raw_keys
                            if k.lower() not in _MGO_BSON_EXCLUDE and "$" not in k
                        )
                except Exception:
                    pass
                return frozenset()

            cache_key = (source.source_key, table_name)
            inferred_schema = _SCHEMA_CACHE.get(cache_key)

            if inferred_schema is None:
                # First batch — always do full re-inference
                _should_reinfer = True
            else:
                # Cheap O(1) key scan: only re-infer when field names differ
                _batch_names  = _batch_field_names()
                _cached_names_set = frozenset(f.name.lower() for f in inferred_schema.fields)
                _should_reinfer = bool(_batch_names and _batch_names != _cached_names_set)
                if _should_reinfer:
                    logger.info(
                        "[%s/%s] batch=%d: field names changed — triggering re-inference. "
                        "new=%s  dropped=%s",
                        source.source_key, table_name, batch_id,
                        sorted(_batch_names - _cached_names_set),
                        sorted(_cached_names_set - _batch_names),
                    )

            if _should_reinfer:
                try:
                    _fresh_schema_raw = spark.read.json(
                        payload_df.select("payload_json").rdd.map(lambda r: r[0])
                    ).schema
                    # Strip BSON/invalid fields before any cache or evolution logic
                    _fresh_schema = StructType(_safe_iceberg_fields(_fresh_schema_raw.fields))

                    # Iceberg→Spark type map — shared by first-batch seed and DDL evolution path
                    _ICE_TO_SPARK = {
                        "bigint": LongType(), "long": LongType(),
                        "int": IntegerType(), "integer": IntegerType(),
                        "smallint": IntegerType(), "tinyint": IntegerType(),
                        "string": StringType(), "varchar": StringType(),
                        "boolean": BooleanType(),
                        "timestamp": TimestampType(),
                        "double": DoubleType(), "float": FloatType(),
                    }

                    if inferred_schema is None:
                        # ── First batch: prefer Iceberg schema as type authority ──
                        # Iceberg schema was written from real non-NULL data; batch
                        # inference may have NULL-only columns inferred as STRING.
                        #
                        # After seeding from Iceberg, also merge any NEW fields that
                        # the current batch has but Iceberg doesn't yet — this handles
                        # the pod-restart-after-DDL-ADD case where the ADD event was
                        # already consumed from Kafka (checkpoint advanced) but the
                        # Iceberg schema was never evolved because the pod died before
                        # writing.  Without this merge, those new columns are silently
                        # dropped from the cache until the next % 10 re-inference.
                        _evo_tbl_init = (
                            f"{table_name}_sd" if write_mode == _WRITE_MODE_SOFT_DELETE
                            else (f"{table_name}_hist" if write_mode == _WRITE_MODE_HISTORY_TRACKING
                                  else table_name)
                        )
                        _fqn_init = f"`{source.catalog}`.`{namespace}`.`{_evo_tbl_init}`"
                        try:
                            _ice_rows = spark.sql(f"DESCRIBE TABLE {_fqn_init}").collect()
                            _ice_type_map = {
                                row["col_name"].lower(): row["data_type"].lower()
                                for row in _ice_rows
                                if not row["col_name"].startswith("#")
                            }
                            _patched = []
                            for _f in _fresh_schema.fields:
                                _ice_t = _ice_type_map.get(_f.name.lower())
                                if _ice_t and _ice_t in _ICE_TO_SPARK:
                                    _patched.append(StructField(_f.name, _ICE_TO_SPARK[_ice_t], _f.nullable))
                                else:
                                    _patched.append(_f)
                            # Merge any NEW fields from the batch that Iceberg doesn't have.
                            # This catches DDL ADD COLUMN events that arrived in Kafka after
                            # the previous pod death (checkpoint advanced but Iceberg never
                            # evolved).  Types come from batch inference — they're new columns
                            # so Iceberg has no authoritative type yet.
                            _patched_names = {f.name.lower() for f in _patched}
                            _new_from_batch = [
                                _f for _f in _fresh_schema.fields
                                if _f.name.lower() not in _patched_names
                            ]
                            if _new_from_batch:
                                logger.info(
                                    "[%s/%s] First-batch seed: batch has %d extra field(s) "
                                    "not yet in Iceberg — appending to cache: %s",
                                    source.source_key, table_name,
                                    len(_new_from_batch),
                                    [f.name for f in _new_from_batch],
                                )
                                _patched.extend(_new_from_batch)
                            inferred_schema = StructType(_patched)
                            logger.info(
                                "[%s/%s] Schema seeded from Iceberg (%d fields) — "
                                "batch inference types overridden by Iceberg authority.",
                                source.source_key, table_name, len(inferred_schema.fields),
                            )
                        except Exception:
                            # Table doesn't exist yet — use batch inference as-is
                            inferred_schema = _fresh_schema
                            logger.info(
                                "[%s/%s] Schema inferred from batch (%d fields) — "
                                "table not yet in Iceberg.",
                                source.source_key, table_name, len(inferred_schema.fields),
                            )
                        _SCHEMA_CACHE[cache_key] = inferred_schema

                    elif len(_fresh_schema.fields) != len(inferred_schema.fields):
                        # Field count changed — DDL evolution detected.
                        # Keep ALL cached fields (types are authoritative).
                        # Only append fields that are genuinely new.
                        # Type-stability: for newly added fields, prefer the Iceberg-
                        # resident type if the column already exists there (e.g. a
                        # rename was processed by another pod and Iceberg already has
                        # it as BIGINT; a NULL-only batch here infers it as STRING).
                        _cached_names = {f.name.lower(): f for f in inferred_schema.fields}
                        # Fetch live Iceberg column types once for the evolution table
                        _evo_tbl_evolve = (
                            f"{table_name}_sd" if write_mode == _WRITE_MODE_SOFT_DELETE
                            else (f"{table_name}_hist" if write_mode == _WRITE_MODE_HISTORY_TRACKING
                                  else table_name)
                        )
                        try:
                            _ice_evolve_rows = spark.sql(
                                f"DESCRIBE TABLE `{source.catalog}`.`{namespace}`"
                                f".`{_evo_tbl_evolve}`"
                            ).collect()
                            _ice_evolve_map = {
                                r["col_name"].lower(): r["data_type"].lower()
                                for r in _ice_evolve_rows
                                if not r["col_name"].startswith("#")
                            }
                        except Exception:
                            _ice_evolve_map = {}
                        _merged = list(inferred_schema.fields)
                        _added = []
                        for _nf in _fresh_schema.fields:
                            if _nf.name.lower() not in _cached_names:
                                # Use Iceberg type if already known; else use inferred
                                _ice_t2 = _ice_evolve_map.get(_nf.name.lower())
                                if _ice_t2 and _ice_t2 in _ICE_TO_SPARK:
                                    _nf = StructField(_nf.name, _ICE_TO_SPARK[_ice_t2], _nf.nullable)
                                _merged.append(_nf)
                                _added.append(_nf.name)
                        logger.info(
                            "[%s/%s] DDL evolution detected in batch %d: "
                            "cached=%d fields, fresh=%d fields — adding %s to cache.",
                            source.source_key, table_name, batch_id,
                            len(inferred_schema.fields), len(_fresh_schema.fields),
                            _added,
                        )
                        inferred_schema = StructType(_merged)
                        _SCHEMA_CACHE[cache_key] = inferred_schema
                    else:
                        logger.debug(
                            "[%s/%s] Periodic schema re-check (batch %d): "
                            "no new fields (%d fields).",
                            source.source_key, table_name, batch_id,
                            len(inferred_schema.fields),
                        )
                except Exception as exc:
                    if inferred_schema is None:
                        logger.warning(
                            "[%s/%s] Schema inference failed: %s — skipping.",
                            source.source_key, table_name, exc,
                        )
                        continue
                    # Non-fatal on periodic re-check: keep existing cached schema
                    logger.debug(
                        "[%s/%s] Periodic schema re-check failed (batch %d): "
                        "%s — keeping cache.",
                        source.source_key, table_name, batch_id, exc,
                    )

            # ── Schema-cache invalidation: ALTER TABLE for new cols in Iceberg ─
            # After DDL evolution is detected (fresh schema has new fields that
            # the Iceberg table lacks), issue ALTER TABLE ADD COLUMN inline before
            # the MERGE so the MERGE never hits UNRESOLVED_COLUMN.
            # history_tracking handles its own ALTER TABLE inside _apply_history_tracking
            # (where the after_*/before_* prefixed final_df schema is known).
            # BSON struct fields (e.g. _id struct<$oid:string>) are excluded —
            # Iceberg column names cannot contain '$'.
            if write_mode != _WRITE_MODE_HISTORY_TRACKING:
                _evo_table = (
                    f"{table_name}_sd" if write_mode == _WRITE_MODE_SOFT_DELETE
                    else table_name
                )
                _evo_fqn = f"`{source.catalog}`.`{namespace}`.`{_evo_table}`"
                try:
                    _iceberg_cols = {
                        row["col_name"].lower()
                        for row in spark.sql(f"DESCRIBE TABLE {_evo_fqn}").collect()
                        if not row["col_name"].startswith("#")
                    }
                    _batch_cols = {f.name.lower() for f in inferred_schema.fields}
                    _new_cols = _batch_cols - _iceberg_cols - {"snap_id", "snap_timestamp"}
                    if _new_cols:
                        logger.info(
                            "[%s/%s] DDL evolution: %d new column(s) to add to Iceberg: %s",
                            source.source_key, _evo_table, len(_new_cols), sorted(_new_cols),
                        )
                        for _nc in sorted(_new_cols):
                            _nc_field = next(
                                f for f in inferred_schema.fields
                                if f.name.lower() == _nc
                            )
                            # Skip any field whose Iceberg type string would be
                            # unparseable (e.g. struct<$oid:string>)
                            _ice_type = _nc_field.dataType.simpleString()
                            if "$" in _ice_type or _is_bson_struct(_nc_field):
                                logger.debug(
                                    "[%s/%s] Skipping ALTER TABLE for BSON field `%s` %s",
                                    source.source_key, _evo_table, _nc_field.name, _ice_type,
                                )
                                continue
                            _alter_ddl = (
                                f"ALTER TABLE {_evo_fqn} "
                                f"ADD COLUMN `{_nc_field.name}` {_ice_type}"
                            )
                            try:
                                spark.sql(_alter_ddl)
                                logger.info(
                                    "[%s/%s] ALTER TABLE ADD COLUMN `%s` %s — OK",
                                    source.source_key, _evo_table, _nc_field.name, _ice_type,
                                )
                            except Exception as _alt_exc:
                                logger.warning(
                                    "[%s/%s] ALTER TABLE ADD COLUMN `%s` failed "
                                    "(may already exist): %s",
                                    source.source_key, _evo_table,
                                    _nc_field.name, _alt_exc,
                                )
                except Exception as _evo_exc:
                    # DESCRIBE TABLE fails when table doesn't exist yet — safe to ignore
                    logger.debug(
                        "[%s/%s] Schema evolution ALTER check skipped: %s",
                        source.source_key, _evo_table, _evo_exc,
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

            # ── PK column name (case-insensitive, dynamic for MongoDB) ─────────
            # When source.pk_col is None the business PK is inferred from the
            # batch schema — this makes the pipeline table-agnostic (any MongoDB
            # collection, not just "customers").
            if source.pk_col is None:
                try:
                    pk_col_actual = _resolve_pk(inferred_schema, table_name)
                except ValueError as _pk_exc:
                    logger.error(
                        "[%s/%s] Cannot resolve PK — skipping batch: %s",
                        source.source_key, table_name, _pk_exc,
                    )
                    continue
                logger.info(
                    "[%s/%s] Dynamic PK resolved: %r",
                    source.source_key, table_name, pk_col_actual,
                )
            else:
                pk_col_actual = source.pk_col
                for f in inferred_schema.fields:
                    if f.name.lower() == source.pk_col.lower():
                        pk_col_actual = f.name
                        break

            # ── Apply StarTransform pipeline steps ────────────────────────────
            # For sources with dynamic PK (source.pk_col is None), inject the
            # deduplicate step here with the now-resolved pk_col_actual so that
            # dedup runs with the correct column name for this specific table.
            effective_steps = list(transform_steps)
            if source.pk_col is None and "deduplicate" in _TRANSFORM_STEPS:
                effective_steps.insert(0, (ST.deduplicate, {"pk": pk_col_actual, "order_col": "kafka_ts"}))

            if effective_steps:
                try:
                    row_df = ST.apply_pipeline(row_df, effective_steps)
                except Exception as exc:
                    logger.error(
                        "[%s/%s] StarTransform pipeline failed: %s",
                        source.source_key, table_name, exc,
                    )
                    continue

            # ── Build write-mode-specific extra schema fields ─────────────────
            # snap_id / snap_timestamp are intentionally omitted here — they are
            # injected by IcebergTableBuilder.create_table() via _inject_snap_cols()
            # automatically.  Including them in extra_fields would duplicate them.
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
                # before_* and after_* columns are added via mergeSchema=true
                # at append time — not declared at table creation (schema evolves).

            full_schema = StructType(inferred_schema.fields + extra_fields)

            # ── Auto-create Iceberg table if needed ───────────────────────────
            fqn_backtick = f"`{source.catalog}`.`{namespace}`.`{table_name}`"
            fqn_plain    = f"{source.catalog}.{namespace}.{table_name}"

            # Each write mode writes into its own dedicated Iceberg table so
            # standard, soft_delete and history_tracking never share a target:
            #
            #   standard         → <table>          (SCD Type 0, hard deletes)
            #   soft_delete      → <table>_sd        (is_deleted flag, row never removed)
            #   history_tracking → <table>_hist      (append-only full history)
            #
            # Compute the effective_table name BEFORE the existence check so we
            # always check and write to the correct table, never to the base table
            # created by a different write mode.
            if write_mode == _WRITE_MODE_HISTORY_TRACKING:
                effective_table = f"{table_name}_hist"
            elif write_mode == _WRITE_MODE_SOFT_DELETE:
                effective_table = f"{table_name}_sd"
            else:
                effective_table = table_name

            # history_tracking uses writeTo().option("mergeSchema","true").append()
            # which auto-creates the table on first write with the exact DataFrame
            # schema — including before_* / after_* / _change_type / _change_ts /
            # snap_id / snap_timestamp.  Pre-creating the table here would produce
            # a schema mismatch (base cols only vs. full envelope cols) so we skip
            # create_table() for this mode entirely and let Iceberg handle it.
            effective_fqn_bt = f"`{source.catalog}`.`{namespace}`.`{effective_table}`"
            effective_fqn_pl = f"{source.catalog}.{namespace}.{effective_table}"
            fqn_backtick = effective_fqn_bt
            fqn_plain    = effective_fqn_pl
            table_name   = effective_table

            if write_mode != _WRITE_MODE_HISTORY_TRACKING:
                table_exists = builder.table_exists(source.catalog, namespace, effective_table)
                if not table_exists:
                    # pk_col_actual is already resolved (statically from source.pk_col
                    # or dynamically via _resolve_pk) — use it directly here.
                    pk_col_exists = any(
                        f.name.lower() == pk_col_actual.lower()
                        for f in inferred_schema.fields
                    )
                    pk_for_bucket = pk_col_actual if pk_col_exists else "snap_id"
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
                    except Exception as create_exc:
                        logger.warning(
                            "[%s/%s] Table creation failed (may already exist): %s",
                            source.source_key, effective_table, create_exc,
                        )

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
                        row_schema=inferred_schema,
                    )

                written_tables.append(fqn_backtick)
            except Exception as write_exc:
                logger.error(
                    "[%s/%s] Write failed (mode=%s): %s",
                    source.source_key, table_name, write_mode, write_exc,
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

    Pipeline:
      Kafka (JSON) → flatten Debezium envelope →
      optional StarTransform steps → write-mode handler → Iceberg.
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
        # "earliest" not "latest":
        # Once an S3 checkpoint exists, Spark ignores startingOffsets entirely —
        # the checkpoint committed offset is used unconditionally.
        # This setting only matters on the very first start (no checkpoint yet).
        # "latest" = silently skip all messages Debezium wrote before the pod started.
        # "earliest" = read everything from the beginning on first start, so no
        # messages are ever lost due to a gap between Debezium producing and the
        # pod consuming for the first time.
        "startingOffsets":                "earliest",
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
    # spark-sql-kafka and its kafka-clients dependency are baked into the image
    # at /opt/spark/jars/ (copied in Dockerfile).  Using spark.jars.packages would
    # trigger a Maven/Ivy download at session start, which (a) requires outbound
    # internet from the driver pod, (b) only lands the jar on the driver's local
    # /root/.ivy2/ — executors on worker nodes never receive it, so every micro-batch
    # that touches a Kafka DataSource fails with ClassNotFoundException.
    # spark.jars is not needed here because /opt/spark/jars/ is already on the
    # default classpath for both driver and all executor JVMs on this cluster.

    # ── Core cap ─────────────────────────────────────────────────────────────────
    # spark.cores.max is the static registration ceiling for this application in
    # Spark standalone mode.  It is fixed at session start and cannot change at
    # runtime — it is NOT the live core consumption figure.
    #
    # We set it to MAX_EXECUTORS × EXECUTOR_CORES (default 3 × 1 = 3) so the
    # dynamic allocator has room to burst up to 3 cores when sustained load
    # demands it, while the executor count at idle drops to 0 (see below).
    conf.set("spark.cores.max",          str(MAX_EXECUTORS * EXECUTOR_CORES))
    conf.set("spark.executor.instances", str(EXECUTOR_INSTANCES))
    conf.set("spark.executor.cores",     str(EXECUTOR_CORES))

    # ── Graduated dynamic allocation ─────────────────────────────────────────────
    #
    # Goal: 0 cores consumed at idle, exactly 1 core during normal CDC processing,
    # up to 3 cores if the job is backlogged for > BURST_BACKLOG_TIMEOUT_S (60 s).
    #
    # spark.cores.max is a static ceiling (immutable after session start).
    # The dynamic allocator is what controls the ACTUAL live executor count.
    #
    # minExecutors = 0
    #   Scale all the way to zero.  After executorIdleTimeout (30 s) of no tasks
    #   the executor process is removed from the worker — 0 CPU used at the OS
    #   level.  The app stays registered on the master (necessary for the
    #   streaming query to remain alive) but holds no live resources.
    #
    # maxExecutors = MAX_EXECUTORS (default 3)
    #   Hard ceiling on scale-up.  Normal CDC micro-batches need only 1 executor.
    #   The allocator will not add a 2nd executor unless the task backlog persists
    #   beyond sustainedSchedulerBacklogTimeout (see below).
    #
    # executorIdleTimeout = 30s
    #   Kill an executor that has had no tasks for 30 s.  Short enough to free
    #   the core quickly during quiet periods; long enough not to thrash on the
    #   2-second trigger interval.
    #
    # schedulerBacklogTimeout = 1s
    #   Request the FIRST executor within 1 s of tasks queuing up.  This keeps
    #   CDC latency low — when a Kafka message arrives after an idle period, the
    #   executor is back in ~1 s.
    #
    # sustainedSchedulerBacklogTimeout = BURST_BACKLOG_TIMEOUT_S (default 60s)
    #   Only request a 2nd (and 3rd) executor if the backlog has been sustained
    #   for 60 consecutive seconds.  This is the "wait 1 minute before bursting"
    #   rule.  A short spike that clears in < 60 s stays on 1 core.  A heavy
    #   batch load that persists for > 60 s gets a 2nd core; if still backlogged
    #   after another 60 s it gets a 3rd, up to maxExecutors.
    #
    # shuffleTracking.enabled = true
    #   Required for Structured Streaming + dynamic allocation in Spark 3.x.
    #   Allows the allocator to safely remove executors that previously served
    #   shuffle reads without losing shuffle data.
    #
    # Behaviour summary
    # ──────────────────────────────────────────────────────────────────────────
    #   State                       Executors   Cores on worker
    #   ─────────────────────────── ─────────── ───────────────
    #   Idle (no Kafka events)      0 (after 30s)    0
    #   Active CDC micro-batch      1                1
    #   Backlog < 60 s              1                1   (no burst yet)
    #   Backlog 60–119 s            2                2
    #   Backlog ≥ 120 s             3 (max)          3
    #   Backlog clears              scales back to 1, then 0 after 30 s idle
    # ──────────────────────────────────────────────────────────────────────────
    conf.set("spark.dynamicAllocation.enabled",                          "true")
    conf.set("spark.dynamicAllocation.minExecutors",                     "0")
    conf.set("spark.dynamicAllocation.maxExecutors",                     str(MAX_EXECUTORS))
    conf.set("spark.dynamicAllocation.executorIdleTimeout",              "30s")
    conf.set("spark.dynamicAllocation.schedulerBacklogTimeout",          "1s")
    conf.set("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", f"{BURST_BACKLOG_TIMEOUT_S}s")
    conf.set("spark.dynamicAllocation.shuffleTracking.enabled",          "true")

    # Peak-hour AQE tuning
    conf.set("spark.executor.memory",    EXECUTOR_MEMORY)
    conf.set("spark.memory.offHeap.size", EXECUTOR_OFFHEAP)
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
    spark:   SparkSession,
    builder: IcebergTableBuilder,
    bao:     BaoSparkInit,
) -> list[StreamingQuery]:
    """Start one continuous streaming query per CDC source."""
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
    """
    Initialise Spark, start all streaming queries, and block until a query
    dies unexpectedly (triggering an outer retry) or the process is interrupted.

    Queries run continuously — no per-batch restart.  Spark Structured Streaming
    commits the offset checkpoint atomically after each successful foreachBatch,
    so restart-on-failure is safe: the next _run_once call resumes from exactly
    the last committed offset.
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
            # If any query dies unexpectedly, surface the error so the outer
            # retry loop in main() can restart the entire Spark session cleanly.
            for q in list(queries):
                if not q.isActive:
                    # q.exception() contains the actual Spark/JVM cause — log it
                    # at ERROR level BEFORE raising so it is always visible in
                    # pod logs even when the outer handler truncates the message.
                    spark_exc = None
                    try:
                        spark_exc = q.exception()
                    except Exception:
                        pass
                    if spark_exc:
                        logger.error(
                            "Streaming query '%s' Spark exception: %s",
                            q.name, spark_exc,
                        )
                    raise RuntimeError(
                        f"Streaming query '{q.name}' terminated unexpectedly."
                        + (f" Cause: {spark_exc}" if spark_exc else "")
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
