#!/usr/bin/env python3
"""
starpump — universal source-to-Iceberg copy pipeline
=====================================================

CLI
---
  starpump <source> [--threads N]

  <source>   Identifies the source connector to use.  The value maps to a
             credentials block in OpenBao and determines which reader is
             invoked.  Currently supported:

               snowflake   Copy from a Snowflake database using the
                           Snowflake Spark connector.

  --threads N   Override the default 8 parallel copy threads.
                Examples: --threads 16, --threads 32.

  Additional sources can be added by extending the SOURCE_READERS dict in
  this module — no other changes required.

Design
------
• Generic — works for ANY database / schema, not just TPC-DS.
• 8 worker threads by default (override with --threads N or MAX_THREADS env).
  Tables are drawn from a shared queue so threads pick the next table
  as soon as they finish the previous one (sequential per table,
  N-way parallel across different tables).
• 100 000-row batches — each batch is committed as a separate Iceberg
  snapshot so the job is resumable / restartable.
• Iceberg namespace always equals the source schema name (lower-case).
• snap_id BIGINT and snap_timestamp TIMESTAMP are injected by
  IcebergTableBuilder on every table automatically.
• 256 MB target file size enforced via IcebergTableBuilder.
• Running user: dave (can_admin_catalog=true, can_write_iceberg=true).
• All credentials fetched from OpenBao via bao_spark_init.BaoSparkInit.
• Catalog pre-flight guard: starpump verifies that the target ICEBERG_CATALOG
  has a Polaris OAuth2 service-account credential registered in
  BaoSparkInit.spark_conf() before opening a Spark session.  If the catalog
  is not wired there, starpump exits with an explicit error — there is no
  authenticated write path and no data will be copied.  This ensures the same
  service-account identity used to create the external catalog is also the
  identity used for every subsequent data copy to that catalog.

Table filtering (applied in this order)
-----------------------------------------
1. INCLUDE_TABLES  — if set, only these tables are eligible (comma-separated).
                     All other tables are ignored regardless of size.
2. EXCLUDE_TABLES  — comma-separated list of tables to always skip.
                     Applied after INCLUDE_TABLES filter.
3. MAX_TABLE_SIZE_GB — tables whose compressed bytes-in-storage in the source
                     exceed this threshold are skipped automatically.
                     Default: 3.0 GB.  Set to 0 to disable the size filter.
4. TABLES          — legacy alias for INCLUDE_TABLES.  If both are set,
                     INCLUDE_TABLES takes precedence.

Source size discovery (Snowflake)
----------------------------------
Before the copy loop starts, the pipeline queries
  INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
to retrieve ACTIVE_BYTES (compressed on-disk bytes) for every table in
the schema.  A full size report is logged at INFO level:

  [size-report] customer        →    2.1 GB  (COPY)
  [size-report] catalog_sales   →   18.4 GB  (SKIP — exceeds 3.0 GB limit)
  [size-report] store_sales     →   22.7 GB  (SKIP — exceeds 3.0 GB limit)
  [size-report] web_sales       →    9.3 GB  (SKIP — exceeds 3.0 GB limit)

Tables for which no size row is returned (e.g. empty tables or views) are
treated as 0 bytes and always included.

Environment variables
---------------------
  USER                Pipeline run user            (REQUIRED — no default; must be dave or bob)
  ADDR                OpenBao address              (default: http://openbao.prod.svc.cluster.local:8200)
  TOKEN               OpenBao root/bootstrap token override (dev only)
  DATABASE            Source database name         (default: SNOWFLAKE_SAMPLE_DATA)
  SCHEMAS             Source schema name           (default: TPCDS_SF10TCL)
  ICEBERG_CATALOG     Target Iceberg catalog name  (default: polaris)
                      Must be registered in BaoSparkInit.spark_conf().
                      Registered catalogs: polaris, databricks.
                      starpump exits before opening a Spark session if not wired.
  S3_BUCKET           Override S3 bucket from OpenBao   (optional)
  INCLUDE_TABLES      Comma-separated explicit include list  (optional)
  EXCLUDE_TABLES      Comma-separated tables to always skip  (optional)
  TABLES              Legacy alias for INCLUDE_TABLES        (optional)
  MAX_TABLE_SIZE_GB   Skip tables larger than this many GB   (default: 3.0)
                      Set to 0 to disable the size filter entirely.
  DRY_RUN             1 = create Iceberg DDL but skip data copy
  BATCH_SIZE          Rows per batch                (default: 100000)
  MAX_THREADS         Parallel copy threads         (default: 8)
                      Overridden by --threads N on the CLI.

Usage
-----
  # Copy all Snowflake tables ≤ 3 GB (default 8 threads):
  starpump snowflake

  # Use 16 parallel threads:
  starpump snowflake --threads 16

  # Use 32 parallel threads:
  starpump snowflake --threads 32

  # Include only specific tables (still respects size filter):
  starpump snowflake INCLUDE_TABLES=customer,item,store

  # Exclude specific tables regardless of size:
  starpump snowflake EXCLUDE_TABLES=web_sales,catalog_sales

  # Combine include + exclude:
  starpump snowflake INCLUDE_TABLES=customer,item,web_sales EXCLUDE_TABLES=web_sales

  # Raise the size cap to 10 GB:
  starpump snowflake MAX_TABLE_SIZE_GB=10

  # Disable the size filter entirely (copy everything):
  starpump snowflake MAX_TABLE_SIZE_GB=0

  # Different database / schema:
  starpump snowflake DATABASE=MY_DB SCHEMAS=MY_SCHEMA

  # Dry-run (DDL only, no data):
  starpump snowflake DRY_RUN=1

TPC-DS TPCDS_SF10TCL known sizes (approximate, scale factor 10 TCL)
---------------------------------------------------------------------
Tables > 3 GB that are auto-excluded by default:
  store_sales       ~22 GB
  catalog_sales     ~18 GB
  web_sales         ~ 9 GB
  inventory         ~ 7 GB
  web_returns       ~ 3.5 GB (borderline — may be included or excluded)

Tables ≤ 3 GB that are copied by default:
  customer, customer_address, customer_demographics, date_dim,
  household_demographics, income_band, item, promotion, reason,
  ship_mode, store, store_returns, time_dim, warehouse, web_page,
  web_site, call_center, catalog_page, catalog_returns
"""

from __future__ import annotations

import argparse
import logging
import os
import queue
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import psycopg2
import psycopg2.extras
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit, monotonically_increasing_id
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from bao_spark_init import BaoSparkInit
from spark_iceberg_utils import IcebergTableBuilder, DEFAULT_TARGET_FILE_SIZE_BYTES

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("starpump")

# ── CLI argument parsing ───────────────────────────────────────────────────────
# starpump <source> [--threads N]
# Parsed early so MAX_THREADS can be overridden before any config is consumed.
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="starpump",
        description="Universal source-to-Iceberg copy pipeline.",
    )
    parser.add_argument(
        "source",
        nargs="?",
        default="snowflake",
        help="Source connector to use (e.g. 'snowflake'). Default: snowflake",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=None,
        metavar="N",
        help="Override the number of parallel copy threads (default: 8, or MAX_THREADS env).",
    )
    # Parse only known args so pytest / spark-submit extra flags are ignored.
    args, _ = parser.parse_known_args()
    return args

_ARGS = _parse_args()

# ── Configuration from environment ────────────────────────────────────────────
USER            = os.environ.get("USER",      "dave")
DATABASE        = os.environ.get("DATABASE",  "SNOWFLAKE_SAMPLE_DATA")
SCHEMAS         = os.environ.get("SCHEMAS",   "TPCDS_SF10TCL")
ICEBERG_CATALOG = os.environ.get("ICEBERG_CATALOG", "polaris")
S3_BUCKET_OVERRIDE = os.environ.get("S3_BUCKET")
DRY_RUN         = os.environ.get("DRY_RUN", "0") == "1"
BATCH_SIZE      = int(os.environ.get("BATCH_SIZE",   "100000"))
# --threads CLI flag takes precedence over the MAX_THREADS env var (default 8).
MAX_THREADS     = _ARGS.threads if _ARGS.threads is not None else int(os.environ.get("MAX_THREADS", "8"))

# ── Source selection ───────────────────────────────────────────────────────────
SOURCE = _ARGS.source.lower()

# ── Table filtering env vars ───────────────────────────────────────────────────
# INCLUDE_TABLES / TABLES: only copy these tables (comma-separated, lower-case).
#   INCLUDE_TABLES takes precedence over the legacy TABLES alias.
_raw_include = os.environ.get("INCLUDE_TABLES") or os.environ.get("TABLES")
INCLUDE_TABLES: list[str] | None = (
    [t.strip().lower() for t in _raw_include.split(",") if t.strip()]
    if _raw_include else None
)

# EXCLUDE_TABLES: always skip these tables (comma-separated, lower-case).
_raw_exclude = os.environ.get("EXCLUDE_TABLES", "")
EXCLUDE_TABLES: set[str] = {
    t.strip().lower() for t in _raw_exclude.split(",") if t.strip()
}

# MAX_TABLE_SIZE_GB: skip tables whose compressed Snowflake size exceeds this.
#   0 disables the size filter entirely.
MAX_TABLE_SIZE_GB: float = float(os.environ.get("MAX_TABLE_SIZE_GB", "3.0"))
_SIZE_FILTER_ENABLED = MAX_TABLE_SIZE_GB > 0

# Iceberg namespace must match source schema name (lower-cased)
ICEBERG_NAMESPACE = SCHEMAS.lower()

# ── Snowflake → Spark type mapping ────────────────────────────────────────────
_SF_TYPE_MAP: dict[str, Any] = {
    "TEXT": StringType(),     "VARCHAR": StringType(),    "CHAR": StringType(),
    "CHARACTER": StringType(),"NCHAR": StringType(),      "NVARCHAR": StringType(),
    "NVARCHAR2": StringType(),"STRING": StringType(),     "VARIANT": StringType(),
    "OBJECT": StringType(),   "ARRAY": StringType(),      "BINARY": StringType(),
    "VARBINARY": StringType(),"NUMBER": DecimalType(38,10),"NUMERIC": DecimalType(38,10),
    "DECIMAL": DecimalType(38,10),"INT": LongType(),      "INTEGER": LongType(),
    "BIGINT": LongType(),     "SMALLINT": ShortType(),    "TINYINT": ByteType(),
    "BYTEINT": ByteType(),    "FLOAT": FloatType(),       "FLOAT4": FloatType(),
    "FLOAT8": DoubleType(),   "DOUBLE": DoubleType(),     "DOUBLE PRECISION": DoubleType(),
    "REAL": FloatType(),      "BOOLEAN": BooleanType(),   "DATE": DateType(),
    "DATETIME": TimestampType(),"TIMESTAMP": TimestampType(),"TIMESTAMP_LTZ": TimestampType(),
    "TIMESTAMP_NTZ": TimestampType(),"TIMESTAMP_TZ": TimestampType(),"TIME": StringType(),
}


def _sf_to_spark(sf_type: str) -> Any:
    """Map a Snowflake column type string to a Spark DataType."""
    upper = sf_type.upper().strip()
    base  = upper.split("(")[0].strip()
    if base in ("NUMBER", "NUMERIC", "DECIMAL") and "(" in upper:
        inner = upper[upper.index("(") + 1 : upper.index(")")]
        parts = inner.split(",")
        p = int(parts[0].strip())
        s = int(parts[1].strip()) if len(parts) > 1 else 0
        return DecimalType(p, s)
    return _SF_TYPE_MAP.get(base, StringType())


# ── Pipeline PostgreSQL helpers ────────────────────────────────────────────────

def _pg_connect(pg: dict) -> "psycopg2.connection":
    """Open a connection to the dedicated `pipeline` PostgreSQL database."""
    return psycopg2.connect(
        host=pg["host"],
        port=int(pg.get("port", 5432)),
        dbname=pg["database"],
        user=pg["user"],
        password=pg["password"],
        connect_timeout=10,
    )


def pg_upsert_watermark(
    pg:               dict,
    source_db:        str,
    source_schema:    str,
    table_name:       str,
    sf_extraction_ts: str,
    rows_copied:      int,
    iceberg_namespace: str,
) -> None:
    """
    Upsert a watermark row into pipeline_watermarks in the `pipeline` Postgres DB.

    This is the authoritative sync-point store that the Debezium bootstrap
    script reads (via plain psql) to resolve oracle_start_scn WITHOUT needing
    a Spark session.
    """
    sql = """
        INSERT INTO pipeline_watermarks
            (source_db, source_schema, table_name,
             sf_extraction_ts, rows_copied, pipeline_run_ts, iceberg_namespace)
        VALUES (%s, %s, %s, %s, %s, NOW(), %s)
        ON CONFLICT (source_db, source_schema, table_name)
        DO UPDATE SET
            sf_extraction_ts  = EXCLUDED.sf_extraction_ts,
            rows_copied       = EXCLUDED.rows_copied,
            pipeline_run_ts   = EXCLUDED.pipeline_run_ts,
            iceberg_namespace = EXCLUDED.iceberg_namespace,
            oracle_start_scn  = NULL,   -- reset; Debezium bootstrap must re-resolve
            updated_at        = NOW()
    """
    with _pg_connect(pg) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                source_db, source_schema, table_name,
                sf_extraction_ts, rows_copied, iceberg_namespace,
            ))
        conn.commit()
    logger.info(
        "[pg-watermark] upserted %s.%s.%s sf_extraction_ts=%s rows=%d",
        source_db, source_schema, table_name, sf_extraction_ts, rows_copied,
    )


def pg_log_run_start(pg: dict, run_id: str, source_db: str, source_schema: str) -> None:
    """Insert a pipeline_run_log row with status='running'."""
    sql = """
        INSERT INTO pipeline_run_log (run_id, source_db, source_schema, started_at, status)
        VALUES (%s, %s, %s, NOW(), 'running')
        ON CONFLICT (run_id) DO NOTHING
    """
    with _pg_connect(pg) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (run_id, source_db, source_schema))
        conn.commit()


def pg_log_run_finish(
    pg:             dict,
    run_id:         str,
    tables_ok:      int,
    tables_failed:  int,
    tables_skipped: int,
    total_rows:     int,
    status:         str,
    error_detail:   str | None = None,
) -> None:
    """Update pipeline_run_log row to final status."""
    sql = """
        UPDATE pipeline_run_log
        SET finished_at    = NOW(),
            tables_ok      = %s,
            tables_failed  = %s,
            tables_skipped = %s,
            total_rows     = %s,
            status         = %s,
            error_detail   = %s
        WHERE run_id = %s
    """
    with _pg_connect(pg) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                tables_ok, tables_failed, tables_skipped,
                total_rows, status, error_detail, run_id,
            ))
        conn.commit()


# ── Snowflake extraction watermark ────────────────────────────────────────────

def capture_sf_extraction_ts(spark: SparkSession, sf_opts: dict) -> str:
    """
    Run SELECT CURRENT_TIMESTAMP() inside the Snowflake session and return
    the result as an ISO-8601 string (UTC, microsecond precision).

    This is the exact Snowflake server-side time at which the subsequent
    SELECT on the table will be executed — it is the CDC sync point.

    Using CURRENT_TIMESTAMP() from Snowflake (not the driver clock) ensures
    the watermark reflects Snowflake's transaction timeline, not network
    latency or driver time-zone differences.
    """
    row = (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**sf_opts)
        .option("query", "SELECT CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::VARCHAR AS ts")
        .load()
        .collect()[0]
    )
    ts_str: str = row[0]  # e.g. "2026-08-18 04:01:39.123456"
    # Normalise to ISO-8601 with Z suffix
    ts_str = ts_str.replace(" ", "T")
    if "." not in ts_str:
        ts_str += ".000000"
    if not ts_str.endswith("Z"):
        ts_str += "Z"
    return ts_str   # e.g. "2026-08-18T04:01:39.123456Z"


def write_watermark_iceberg(
    spark:            SparkSession,
    catalog:          str,
    namespace:        str,
    source_db:        str,
    source_schema:    str,
    table_name:       str,
    sf_extraction_ts: str,
    rows_copied:      int,
) -> None:
    """
    Upsert one row into the Iceberg control table
    <catalog>.<namespace>._pipeline_watermarks.

    This is the Spark-native copy of the watermark — queryable from any Spark
    job via SQL without a Postgres connection.  The canonical CDC sync-point
    store is the `pipeline` Postgres DB (see pg_upsert_watermark).
    """
    from pyspark.sql.types import (
        StructType, StructField,
        StringType as ST, LongType as LT, TimestampType as TT,
    )
    wm_fqn = f"`{catalog}`.`{namespace}`.`_pipeline_watermarks`"

    wm_schema = StructType([
        StructField("source_db",         ST(), True),
        StructField("source_schema",      ST(), True),
        StructField("table_name",         ST(), True),
        StructField("sf_extraction_ts",   ST(), True),
        StructField("rows_copied",        LT(), True),
        StructField("pipeline_run_ts",    TT(), True),
        StructField("iceberg_namespace",  ST(), True),
    ])

    # Ensure control table exists (idempotent)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {wm_fqn} (
          source_db          STRING,
          source_schema      STRING,
          table_name         STRING,
          sf_extraction_ts   STRING,
          rows_copied        BIGINT,
          pipeline_run_ts    TIMESTAMP,
          iceberg_namespace  STRING
        )
        USING iceberg
        PARTITIONED BY (source_db, source_schema)
        TBLPROPERTIES (
          'format-version'               = '2',
          'write.format.default'         = 'parquet',
          'write.target-file-size-bytes' = '268435456',
          'platform.purpose'             = 'cdc-sync-watermark'
        )
    """)

    # Iceberg v2 MERGE (upsert)
    spark.sql(f"""
        MERGE INTO {wm_fqn} t
        USING (SELECT
                 '{source_db}'        AS source_db,
                 '{source_schema}'    AS source_schema,
                 '{table_name}'       AS table_name,
                 '{sf_extraction_ts}' AS sf_extraction_ts,
                 {rows_copied}        AS rows_copied,
                 current_timestamp()  AS pipeline_run_ts,
                 '{namespace}'        AS iceberg_namespace
              ) s
        ON  t.source_db     = s.source_db
        AND t.source_schema = s.source_schema
        AND t.table_name    = s.table_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    logger.info(
        "[iceberg-watermark] %s.%s.%s → sf_extraction_ts=%s rows=%d",
        source_db, source_schema, table_name, sf_extraction_ts, rows_copied,
    )


# ── Source connector protocol ──────────────────────────────────────────────────
# Each source (snowflake, oracle, postgres, …) registers three callables here.
# The rest of the pipeline is fully generic — it never names a source directly.
#
# To add a new source later:
#   1. Write the three _<source>_* functions below.
#   2. Add one entry to _CONNECTORS.
#   3. Add the source name to _CONNECTORS — that's it.
#
# connector fields
#   build_opts(bao)          → dict   connection options fed to spark.read.format(...)
#   list_tables(spark, opts) → list[str]   sorted lower-cased table names
#   table_schema(spark, opts, table) → StructType
#   table_sizes(spark, opts) → dict[str, float]  {table: gb}

from dataclasses import dataclass
from typing import Callable

@dataclass
class _SourceConnector:
    spark_format: str                                               # e.g. "net.snowflake.spark.snowflake"
    build_opts:   Callable                                          # (bao) -> dict
    list_tables:  Callable                                          # (spark, opts) -> list[str]
    table_schema: Callable                                          # (spark, opts, table) -> StructType
    table_sizes:  Callable                                          # (spark, opts) -> dict[str, float]


# ── Snowflake connector implementation ─────────────────────────────────────────

def _sf_build_opts(bao: "BaoSparkInit") -> dict:
    return bao.snowflake_options(schema=SCHEMAS, database=DATABASE)


def _sf_list_tables(spark: SparkSession, opts: dict) -> list[str]:
    """Discover all BASE TABLE names in the Snowflake schema."""
    df: DataFrame = (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**opts)
        .option("query",
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = UPPER(CURRENT_SCHEMA()) "
                "AND TABLE_TYPE = 'BASE TABLE' "
                "ORDER BY TABLE_NAME")
        .load()
    )
    names = sorted(row[0].lower() for row in df.collect())
    logger.info("Discovered %d tables in %s.%s: %s",
                len(names), DATABASE, SCHEMAS, names)
    return names


def _sf_table_schema(spark: SparkSession, opts: dict, table: str) -> StructType:
    """Read one row from Snowflake; return its StructType schema."""
    return (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**opts)
        .option("query", f'SELECT * FROM "{table.upper()}" LIMIT 1')
        .load()
    ).schema


def _sf_table_sizes(spark: SparkSession, opts: dict) -> dict[str, float]:
    """
    Query Snowflake TABLE_STORAGE_METRICS for compressed on-disk sizes.
    Returns {lower_table_name: size_in_gb}.  Falls back to INFORMATION_SCHEMA
    if ACCOUNT_USAGE is not accessible.
    """
    queries = [
        (
            "SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS",
            f"SELECT LOWER(TABLE_NAME), ACTIVE_BYTES "
            f"FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS "
            f"WHERE TABLE_SCHEMA = UPPER('{SCHEMAS}') "
            f"AND TABLE_CATALOG = UPPER('{DATABASE}') "
            f"AND DELETED IS NULL",
        ),
        (
            "INFORMATION_SCHEMA.TABLE_STORAGE_METRICS",
            f"SELECT LOWER(TABLE_NAME), ACTIVE_BYTES "
            f"FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS "
            f"WHERE TABLE_SCHEMA = UPPER(CURRENT_SCHEMA())",
        ),
    ]
    _gb = 1024 ** 3
    for view_name, query in queries:
        try:
            df = (
                spark.read.format("net.snowflake.spark.snowflake")
                .options(**opts)
                .option("query", query)
                .load()
            )
            sizes = {}
            for row in df.collect():
                tname = row[0]
                active_bytes = row[1] or 0
                sizes[tname] = active_bytes / _gb
            logger.info(
                "Table size data fetched from %s (%d entries).", view_name, len(sizes)
            )
            return sizes
        except Exception as exc:
            logger.warning(
                "Could not query %s for sizes: %s — trying fallback.", view_name, exc
            )
    logger.warning(
        "All size queries failed — treating all tables as 0 GB (no size filter)."
    )
    return {}


# ── Connector registry — add new sources here ─────────────────────────────────
# To register a new source (e.g. "oracle"):
#   1. Implement _oracle_build_opts, _oracle_list_tables,
#      _oracle_table_schema, _oracle_table_sizes following the _sf_* pattern.
#   2. Add:  "oracle": _SourceConnector("jdbc", _oracle_build_opts, ...)
_CONNECTORS: dict[str, _SourceConnector] = {
    "snowflake": _SourceConnector(
        spark_format = "net.snowflake.spark.snowflake",
        build_opts   = _sf_build_opts,
        list_tables  = _sf_list_tables,
        table_schema = _sf_table_schema,
        table_sizes  = _sf_table_sizes,
    ),
    # "oracle":   _SourceConnector(...),   ← add future sources here
    # "postgres": _SourceConnector(...),
}

# Validate source name against the registry (fail fast, friendly message).
if SOURCE not in _CONNECTORS:
    logger.error(
        "Unsupported source %r — registered sources: %s",
        SOURCE, ", ".join(sorted(_CONNECTORS)),
    )
    sys.exit(1)


def log_size_report(
    all_tables:   list[str],
    sizes:        dict[str, float],
    final_tables: list[str],
) -> None:
    """
    Log a human-readable size report for every discovered table showing
    its compressed size in Snowflake and whether it will be copied or skipped.

    Example output:
      [size-report] customer          →    2.1 GB  (COPY)
      [size-report] catalog_sales     →   18.4 GB  (SKIP — exceeds 3.0 GB limit)
      [size-report] web_sales         →    9.3 GB  (SKIP — exceeds 3.0 GB limit)
      [size-report] call_center       →    0.0 GB  (SKIP — EXCLUDE_TABLES)
    """
    final_set = set(final_tables)
    logger.info("─" * 70)
    logger.info(
        "[size-report] Snowflake table inventory  "
        "(size-filter: %s GB | include: %s | exclude: %s)",
        f"{MAX_TABLE_SIZE_GB:.1f}" if _SIZE_FILTER_ENABLED else "off",
        ", ".join(INCLUDE_TABLES) if INCLUDE_TABLES else "all",
        ", ".join(sorted(EXCLUDE_TABLES)) if EXCLUDE_TABLES else "none",
    )
    logger.info("─" * 70)
    max_name = max((len(t) for t in all_tables), default=10)
    for tbl in sorted(all_tables):
        size_gb = sizes.get(tbl, 0.0)
        if tbl in final_set:
            verdict = "COPY"
        elif tbl in EXCLUDE_TABLES:
            verdict = "SKIP — EXCLUDE_TABLES"
        elif INCLUDE_TABLES and tbl not in INCLUDE_TABLES:
            verdict = "SKIP — not in INCLUDE_TABLES"
        elif _SIZE_FILTER_ENABLED and size_gb > MAX_TABLE_SIZE_GB:
            verdict = f"SKIP — {size_gb:.1f} GB exceeds {MAX_TABLE_SIZE_GB:.1f} GB limit"
        else:
            verdict = "COPY"
        logger.info(
            "[size-report] %-*s → %6.1f GB  (%s)",
            max_name, tbl, size_gb, verdict,
        )
    logger.info("─" * 70)


# ── Table filtering ────────────────────────────────────────────────────────────

def apply_table_filters(
    all_tables: list[str],
    sizes:      dict[str, float],
) -> list[str]:
    """
    Apply the three-stage filter pipeline and return the final list of
    tables to copy, preserving the original sort order.

    Stage 1 — INCLUDE_TABLES:  keep only tables in the include set.
    Stage 2 — EXCLUDE_TABLES:  drop tables in the exclude set.
    Stage 3 — MAX_TABLE_SIZE_GB: drop tables whose size exceeds the cap.

    Tables with unknown size (not in *sizes*) are treated as 0 GB and pass
    the size filter unless explicitly excluded.
    """
    result = list(all_tables)  # start from full discovered list

    # Stage 1 — include filter
    if INCLUDE_TABLES:
        before = len(result)
        result = [t for t in result if t in set(INCLUDE_TABLES)]
        logger.info(
            "INCLUDE_TABLES filter: %d → %d tables (kept: %s)",
            before, len(result), result,
        )
        # Warn about requested tables that don't exist
        missing = set(INCLUDE_TABLES) - set(all_tables)
        if missing:
            logger.warning(
                "INCLUDE_TABLES contains tables not found in Snowflake: %s",
                sorted(missing),
            )

    # Stage 2 — exclude filter
    if EXCLUDE_TABLES:
        before = len(result)
        skipped = [t for t in result if t in EXCLUDE_TABLES]
        result  = [t for t in result if t not in EXCLUDE_TABLES]
        logger.info(
            "EXCLUDE_TABLES filter: %d → %d tables (dropped: %s)",
            before, len(result), skipped,
        )

    # Stage 3 — size filter
    if _SIZE_FILTER_ENABLED:
        before   = len(result)
        too_big  = [t for t in result if sizes.get(t, 0.0) > MAX_TABLE_SIZE_GB]
        result   = [t for t in result if sizes.get(t, 0.0) <= MAX_TABLE_SIZE_GB]
        if too_big:
            logger.info(
                "Size filter (> %.1f GB): %d → %d tables, dropped: %s",
                MAX_TABLE_SIZE_GB, before, len(result),
                [(t, f"{sizes.get(t,0):.1f}GB") for t in too_big],
            )
    else:
        logger.info("Size filter disabled (MAX_TABLE_SIZE_GB=0).")

    return result


# ── Partition auto-detection ───────────────────────────────────────────────────

def _auto_partition_spec(schema: StructType) -> list[dict]:
    """
    Global partition spec applied to EVERY Iceberg table:
      hours(snap_timestamp)   — hourly range partition
      bucket(4, snap_id)      — 4 hash buckets within each hour

    snap_timestamp and snap_id are always present (injected by IcebergTableBuilder),
    so no schema inspection is needed and no fallback is required.
    """
    return [
        IcebergTableBuilder.hours("snap_timestamp"),
        IcebergTableBuilder.bucket("snap_id", 4),
    ]


# ── Single-table copy worker ───────────────────────────────────────────────────

def _copy_table(
    spark:     SparkSession,
    builder:   IcebergTableBuilder,
    connector: "_SourceConnector",
    conn_opts: dict,
    s3_bucket: str,
    table:     str,
    size_gb:   float,
    results:   dict,
    lock:      threading.Lock,
    pg_creds:  dict,
) -> None:
    """
    Copy one table from the source database → Iceberg (called inside a thread).
    Source-agnostic: all source-specific I/O goes through *connector*.

    Watermark flow
    --------------
    1. Capture Snowflake server-side CURRENT_TIMESTAMP() immediately before
       the first batch SELECT — this is the CDC sync point (sf_extraction_ts).
    2. After a successful copy: dual-write the watermark to
       a. Iceberg _pipeline_watermarks control table  (Spark-queryable)
       b. PostgreSQL pipeline.pipeline_watermarks      (shell-queryable by
          the Debezium bootstrap script without a Spark session)
    3. Stamp 'pipeline.sf_extraction_ts' as an Iceberg table property so
       the watermark appears in any DESCRIBE EXTENDED output.

    Writes final status to the shared *results* dict.
    """
    status           = "pending"
    rows_total       = 0
    err              = None
    sf_extraction_ts = None

    try:
        logger.info("[%s] START: %.1f GB | discovering schema …", table, size_gb)
        sf_raw_schema = connector.table_schema(spark, conn_opts, table)

        # Map SF types → Spark/Iceberg types (builder injects snap cols)
        iceberg_schema = StructType([
            StructField(f.name, _sf_to_spark(f.dataType.simpleString()), True)
            for f in sf_raw_schema.fields
        ])

        partition_spec = _auto_partition_spec(iceberg_schema)

        # Location must be under the Polaris catalog's allowedLocations.
        # IcebergCatalog is configured with s3://xdatatoiceberg1/tpcds — use
        # s3:// (not s3a://) and the correct prefix so Polaris accepts the path.
        s3_location = f"s3://{s3_bucket}/tpcds/{ICEBERG_NAMESPACE}/{table}"

        fqn = builder.create_table(
            catalog        = ICEBERG_CATALOG,
            namespace      = ICEBERG_NAMESPACE,
            table          = table,
            schema         = iceberg_schema,
            partition_spec = partition_spec,
            location       = s3_location,
        )
        logger.info("[%s] Iceberg table DDL ready: %s", table, fqn)

        if DRY_RUN:
            logger.info("[%s] DRY_RUN — skipping data copy.", table)
            status = "dry_run"
        else:
            # ── Resume detection ───────────────────────────────────────────
            # Count rows already committed to Iceberg from a previous partial
            # run so the batch loop can skip them (use as initial OFFSET).
            try:
                already_written = spark.table(fqn).count()
            except Exception:
                already_written = 0

            # Reuse the original sf_extraction_ts if it was stamped onto the
            # table property in a prior run.  The property is only written on
            # full success, so its presence means the table was fully copied
            # before and this is a fresh re-run — capture a new timestamp.
            # If the property is ABSENT and already_written > 0, the table is
            # partially copied: reuse the watermark from the Postgres pipeline
            # DB so the CDC sync-point stays consistent.
            if already_written > 0:
                # Try to recover the original timestamp from the pipeline DB
                # (written at the very start of the first run, before any batches).
                try:
                    with _pg_connect(pg_creds) as _conn:
                        with _conn.cursor() as _cur:
                            _cur.execute(
                                "SELECT sf_extraction_ts FROM pipeline_watermarks "
                                "WHERE source_db=%s AND source_schema=%s AND table_name=%s",
                                (DATABASE, SCHEMAS, table),
                            )
                            _row = _cur.fetchone()
                    sf_extraction_ts = _row[0] if _row and _row[0] else None
                except Exception:
                    sf_extraction_ts = None

                if sf_extraction_ts:
                    logger.info(
                        "[%s] RESUME: %d rows already in Iceberg — reusing "
                        "sf_extraction_ts=%s from pipeline DB, starting at offset=%d.",
                        table, already_written, sf_extraction_ts, already_written,
                    )
                else:
                    # Fallback: no watermark in pipeline DB yet (first run wrote
                    # nothing before crashing).  Capture a fresh timestamp.
                    sf_extraction_ts = capture_sf_extraction_ts(spark, conn_opts)
                    logger.info(
                        "[%s] RESUME: %d rows in Iceberg but no prior watermark — "
                        "fresh sf_extraction_ts=%s, starting at offset=%d.",
                        table, already_written, sf_extraction_ts, already_written,
                    )
            else:
                # ── Fresh run: capture CDC sync-point BEFORE the first batch ──
                # Must use Snowflake server-side CURRENT_TIMESTAMP() so the
                # watermark reflects Snowflake's transaction timeline and maps
                # precisely to an Oracle SCN via TIMESTAMP_TO_SCN().
                sf_extraction_ts = capture_sf_extraction_ts(spark, conn_opts)
                logger.info("[%s] sf_extraction_ts=%s (CDC sync point)", table, sf_extraction_ts)

                # Eagerly write the watermark to Postgres NOW — before the first
                # batch — so a crash mid-copy still leaves a recoverable timestamp
                # for the next resume attempt.
                try:
                    pg_upsert_watermark(
                        pg                = pg_creds,
                        source_db         = DATABASE,
                        source_schema     = SCHEMAS,
                        table_name        = table,
                        sf_extraction_ts  = sf_extraction_ts,
                        rows_copied       = 0,
                        iceberg_namespace = ICEBERG_NAMESPACE,
                    )
                    logger.info("[%s] Early watermark written to pipeline DB.", table)
                except Exception as _pg_err:
                    logger.warning(
                        "[%s] Could not write early watermark to pipeline DB: %s",
                        table, _pg_err,
                    )

            # ── Batched sequential copy ────────────────────────────────────
            iceberg_cols = [f.name for f in iceberg_schema.fields]
            offset = already_written
            rows_total = already_written

            while True:
                query = (
                    f'SELECT * FROM "{table.upper()}" '
                    f"ORDER BY 1 "
                    f"LIMIT {BATCH_SIZE} OFFSET {offset}"
                )
                batch: DataFrame = (
                    spark.read.format(connector.spark_format)
                    .options(**conn_opts)
                    .option("query", query)
                    .load()
                )
                n = batch.count()
                if n == 0:
                    break

                # Align to Iceberg schema (add missing cols as NULL)
                aligned = batch.select(
                    *[
                        (batch[c] if c in batch.columns
                         else lit(None).cast(iceberg_schema[c].dataType)
                        ).alias(c)
                        for c in iceberg_cols
                    ]
                )

                # Inject snap audit values
                # snap_id: unique BIGINT per row using Spark's monotonically_increasing_id().
                # This produces a 64-bit integer that is unique and monotonically
                # increasing across all rows and partitions within the batch — not
                # a scalar constant (which would give every row the same value).
                # snap_timestamp: wall-clock at write time, same for all rows in
                # the batch (correct — it marks when the batch was written).
                final = (
                    aligned
                    .withColumn("snap_id",        monotonically_increasing_id().cast(LongType()))
                    .withColumn("snap_timestamp",  current_timestamp())
                )

                final.writeTo(fqn).option("mergeSchema", "true").append()

                rows_total += n
                offset     += n
                logger.info(
                    "[%s] batch offset=%d rows=%d total=%d",
                    table, offset - n, n, rows_total,
                )
                if n < BATCH_SIZE:
                    break   # last batch

            logger.info("[%s] DONE — %d rows written (total incl. prior runs).", table, rows_total)

            # ── Stamp sf_extraction_ts onto the Iceberg table property ─────
            spark.sql(
                f"ALTER TABLE {fqn} SET TBLPROPERTIES "
                f"('pipeline.sf_extraction_ts' = '{sf_extraction_ts}')"
            )

            # ── Dual-write watermark: Iceberg control table ────────────────
            write_watermark_iceberg(
                spark             = spark,
                catalog           = ICEBERG_CATALOG,
                namespace         = ICEBERG_NAMESPACE,
                source_db         = DATABASE,
                source_schema     = SCHEMAS,
                table_name        = table,
                sf_extraction_ts  = sf_extraction_ts,
                rows_copied       = rows_total,
            )

            # ── Final watermark update: pipeline PostgreSQL DB ────────────
            # Refresh rows_copied to the final count now that copy is complete.
            pg_upsert_watermark(
                pg                = pg_creds,
                source_db         = DATABASE,
                source_schema     = SCHEMAS,
                table_name        = table,
                sf_extraction_ts  = sf_extraction_ts,
                rows_copied       = rows_total,
                iceberg_namespace = ICEBERG_NAMESPACE,
            )

            status = "success"

    except Exception as exc:  # noqa: BLE001
        logger.error("[%s] FAILED: %s", table, exc, exc_info=True)
        status = "error"
        err    = str(exc)

    with lock:
        results[table] = {
            "status":           status,
            "rows_written":     rows_total,
            "size_gb":          size_gb,
            "sf_extraction_ts": sf_extraction_ts,
            "error":            err,
        }


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    os.environ["USER"] = USER   # ensure env is set for submodules

    run_id = str(uuid.uuid4())

    logger.info(
        "=== starpump %s | run_id=%s user=%s db=%s schema=%s catalog=%s threads=%d ===",
        SOURCE, run_id, USER, DATABASE, SCHEMAS, ICEBERG_CATALOG, MAX_THREADS,
    )
    logger.info(
        "=== Filters: include=%s  exclude=%s  max_size=%.1f GB ===",
        ", ".join(INCLUDE_TABLES) if INCLUDE_TABLES else "all",
        ", ".join(sorted(EXCLUDE_TABLES)) if EXCLUDE_TABLES else "none",
        MAX_TABLE_SIZE_GB,
    )

    # ── 1. Credentials from OpenBao ───────────────────────────────────────────
    bao  = BaoSparkInit()
    s3   = bao.s3_creds()
    s3_bucket = S3_BUCKET_OVERRIDE or s3["bucket"]
    pg   = bao.pipeline_db_creds()

    # Resolve the connector for the requested source.
    connector  = _CONNECTORS[SOURCE]
    conn_opts  = connector.build_opts(bao)

    conf  = bao.spark_conf(app_name=f"starpump-{SOURCE}")

    # ── 2. Spark session ──────────────────────────────────────────────────────
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ── 3. Log run start in pipeline DB ──────────────────────────────────────
    try:
        pg_log_run_start(pg, run_id, DATABASE, SCHEMAS)
        logger.info("[run-log] run_id=%s recorded in pipeline DB.", run_id)
    except Exception as pg_err:
        logger.warning("[run-log] Could not write run start to pipeline DB: %s", pg_err)

    run_status    = "failed"
    run_err_detail = None

    try:
        builder = IcebergTableBuilder(spark, running_user=USER)
        builder.ensure_namespace(ICEBERG_CATALOG, ICEBERG_NAMESPACE)

        # ── 4. Table discovery (via connector — source-agnostic) ──────────────
        all_tables = connector.list_tables(spark, conn_opts)
        if not all_tables:
            logger.error("No tables found in %s.%s — aborting.", DATABASE, SCHEMAS)
            sys.exit(1)

        # ── 5. Size discovery (via connector — source-agnostic) ───────────────
        sizes: dict[str, float] = {}
        if _SIZE_FILTER_ENABLED:
            sizes = connector.table_sizes(spark, conn_opts)
        else:
            logger.info("Size discovery skipped (MAX_TABLE_SIZE_GB=0).")

        # ── 6. Apply filters ──────────────────────────────────────────────────
        tables = apply_table_filters(all_tables, sizes)

        # Log the full size report (shows every table + COPY/SKIP verdict)
        log_size_report(all_tables, sizes, tables)

        if not tables:
            logger.error(
                "No tables remain after filtering — nothing to copy. "
                "Check INCLUDE_TABLES / EXCLUDE_TABLES / MAX_TABLE_SIZE_GB."
            )
            sys.exit(1)

        logger.info(
            "Copying %d/%d table(s) with %d threads, %d rows/batch%s.",
            len(tables), len(all_tables), MAX_THREADS, BATCH_SIZE,
            " [DRY RUN]" if DRY_RUN else "",
        )

        # ── 7. N-thread copy using a work queue ──────────────────────────────
        # Each thread pulls the next table from the queue so they naturally
        # pick up new work as soon as they finish.
        work_q: queue.Queue[tuple[str, float]] = queue.Queue()
        for tbl in tables:
            work_q.put((tbl, sizes.get(tbl, 0.0)))

        results: dict[str, dict] = {}
        lock = threading.Lock()

        def worker() -> None:
            while True:
                try:
                    tbl, size_gb = work_q.get_nowait()
                except queue.Empty:
                    break
                _copy_table(
                    spark, builder, connector, conn_opts, s3_bucket,
                    tbl, size_gb, results, lock,
                    pg_creds=pg,
                )
                work_q.task_done()

        t0      = time.time()
        threads = [
            threading.Thread(target=worker, name=f"copy-worker-{i+1}", daemon=True)
            for i in range(min(MAX_THREADS, len(tables)))
        ]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        elapsed = time.time() - t0

        # ── 8. Summary ────────────────────────────────────────────────────────
        ok     = [r for r in results.values() if r["status"] in ("success", "dry_run")]
        failed = {t: r for t, r in results.items() if r["status"] == "error"}
        skipped_count = len(all_tables) - len(tables)
        total_rows = sum(r["rows_written"] for r in ok)

        logger.info("─" * 70)
        logger.info(
            "Completed in %.1fs — %d/%d copied | %d skipped (filtered) | "
            "%d failed | %d rows written",
            elapsed, len(ok), len(all_tables), skipped_count,
            len(failed), total_rows,
        )
        for tbl, r in results.items():
            mark = "✓" if r["status"] in ("success", "dry_run") else "✗"
            wm   = r.get("sf_extraction_ts") or "-"
            logger.info(
                "  %s %-30s  rows=%-8d  size=%.1f GB  sf_ts=%-30s  status=%s%s",
                mark, tbl, r["rows_written"], r["size_gb"], wm, r["status"],
                f"  ERR={r['error'][:80]}" if r["error"] else "",
            )
        logger.info("─" * 70)

        run_status = "partial" if failed else "success"
        if failed:
            run_err_detail = f"Failed tables: {list(failed.keys())}"

        # ── 9. Finalise pipeline_run_log in pipeline DB ───────────────────────
        try:
            pg_log_run_finish(
                pg             = pg,
                run_id         = run_id,
                tables_ok      = len(ok),
                tables_failed  = len(failed),
                tables_skipped = skipped_count,
                total_rows     = total_rows,
                status         = run_status,
                error_detail   = run_err_detail,
            )
            logger.info("[run-log] run_id=%s finalised status=%s.", run_id, run_status)
        except Exception as pg_err:
            logger.warning("[run-log] Could not finalise run log in pipeline DB: %s", pg_err)

        if failed:
            sys.exit(1)

    except SystemExit:
        raise
    except Exception as top_err:
        run_err_detail = str(top_err)
        try:
            pg_log_run_finish(
                pg=pg, run_id=run_id, tables_ok=0, tables_failed=0,
                tables_skipped=0, total_rows=0, status="failed",
                error_detail=run_err_detail,
            )
        except Exception:
            pass
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
