#!/usr/bin/env python3
"""
starpump — universal source-to-Iceberg copy pipeline
=====================================================

CLI
---
  starpump <source> [--threads N]

  <source>   Identifies the source connector to use.  The value maps to a
             credentials block in OpenBao and determines which reader is
             invoked.  Registered sources:

              snowflake    Copy from a Snowflake database using the
                           Snowflake Spark connector.

              databricks   Copy from a Databricks SQL Warehouse via JDBC
                           (Simba JDBC driver).  Tables are read from the
                           Unity Catalog and written to the Polaris Iceberg
                           catalog as databricks.<schema>.<table>.
                           If the target Iceberg table does not exist it is
                           created automatically before the first batch copy.

  --threads N   Override the default 8 parallel copy threads.
                Examples: --threads 16, --threads 32.

  Adding a new source
  -------------------
  1. Store its credentials in OpenBao under secret/data/platform/<source>.
  2. Add a databricks_<source>_creds() / <source>_options() method to
     BaoSparkInit (bao_spark_init.py) if needed.
  3. Implement the five _<source>_* functions following the _sf_* / _db_*
     pattern below.
  4. Register a _SourceConnector entry in _CONNECTORS — that's it.
  No changes elsewhere in the pipeline are required.

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

Row-level filtering — QUERY_FILTER
------------------------------------
QUERY_FILTER lets you copy only the rows that match one or more predicates.
Filters are comma-separated; each filter is one of two forms:

  Schema-level  (applies to EVERY table in the schema):
    <column><op><value>

  Table-level   (applies to one named table only):
    <table>.<column><op><value>

Supported operators:  =  !=  <>  >=  <=  >  <  LIKE  NOT LIKE  IN  NOT IN
                       IS NULL  IS NOT NULL

The value is passed verbatim into the WHERE clause so quote strings yourself.

Examples:
  # Copy only active products (schema-level — applied to every table):
  QUERY_FILTER="is_active=1"                         starpump databricks

  # Table-level: only product rows with unit_price > 50:
  QUERY_FILTER="product.unit_price>50"               starpump databricks

  # Multiple predicates on different tables:
  QUERY_FILTER="product.unit_price>=100,orders.status='OPEN'"  starpump snowflake

  # LIKE pattern (quote the value):
  QUERY_FILTER="product.product_name LIKE 'Star%'"   starpump databricks

  # IN list:
  QUERY_FILTER="product.category IN ('Electronics','Sports')"  starpump databricks

  # IS NULL / IS NOT NULL (no value needed):
  QUERY_FILTER="product.snap_id IS NULL"             starpump databricks

  # Combine table-level and schema-level:
  QUERY_FILTER="product.unit_price>100,is_active=1"  starpump databricks

  # Date range (Snowflake):
  QUERY_FILTER="orders.created_at>='2026-01-01'"     starpump snowflake

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
  USER                Pipeline run user            (REQUIRED — no default)
  ADDR                OpenBao address              (default: http://openbao.prod.svc.cluster.local:8200)
  TOKEN               OpenBao root/bootstrap token override (dev only)
  DATABASE            Source database / catalog    (default: SNOWFLAKE_SAMPLE_DATA; for databricks: lakehouse)
  SCHEMAS             Source schema name           (default: TPCDS_SF10TCL; for databricks: lakehouse_db)
  ICEBERG_CATALOG     Target Iceberg catalog name  (default: polaris; for databricks: databricks)
  S3_BUCKET           Override S3 bucket from OpenBao   (optional)
  INCLUDE_TABLES      Comma-separated explicit include list  (optional)
  EXCLUDE_TABLES      Comma-separated tables to always skip  (optional)
  TABLES              Legacy alias for INCLUDE_TABLES        (optional)
  MAX_TABLE_SIZE_GB   Skip tables larger than this many GB   (default: 3.0)
                      Set to 0 to disable the size filter entirely.
  QUERY_FILTER        Row-level predicate filter (see "Row-level filtering" above).
                      Comma-separated list of <[table.]column><op><value> expressions.
                      Applied as a WHERE clause on the source query for every connector.
  DRY_RUN             1 = create Iceberg DDL but skip data copy
  BATCH_SIZE          Rows per batch                (default: 100000)
  MAX_ROWS            Hard cap on total NEW rows written per table in this run.
                      0 (default) = no cap — copy until source is exhausted.
                      Example: MAX_ROWS=1000 appends exactly 1 000 rows then stops,
                      regardless of how many rows are already in Iceberg.
  MAX_THREADS         Parallel copy threads         (default: 8)
                      Overridden by --threads N on the CLI.

Update / delete tracking (incremental mode only)
-------------------------------------------------
  WRITE_MODE          Controls how updates and deletes are materialised in Iceberg.
                      CLI: --write-mode  (takes precedence over WRITE_MODE env var).

                      standard (default — SCD Type 0)
                        Each incremental batch performs a MERGE INTO by primary key:
                          • source rows present  → UPDATE existing Iceberg row (upsert)
                          • source rows absent   → hard DELETE from Iceberg
                        After every upsert batch the pipeline runs a delete-detection
                        pass: collects live PKs from the source window and removes any
                        Iceberg row whose PK is not present.

                      soft_delete
                        Same MERGE upsert pass as standard.
                        Rows that vanish from the source window are NOT physically
                        deleted; instead starpump sets:
                          is_deleted = true
                          deleted_at = current_timestamp()
                        These two columns are added to the Iceberg table automatically
                        on the first run (mergeSchema=true).  Consumers filter with
                          WHERE is_deleted IS NOT TRUE
                        to see the live dataset.

                      history  (SCD Type 2 / audit log)
                        Every row read from the source is appended as a NEW Iceberg
                        row.  No rows are ever updated or deleted.  Two extra columns
                        are added:
                          _change_type STRING   — always 'INSERT' in batch mode
                          _change_ts   TIMESTAMP
                        Use this mode when you need a full audit trail of all states.

  PK_COLS             Comma-separated primary key column(s) used for the MERGE JOIN
                      and for ORDER BY on source reads (index-friendly pagination).
                      CLI: --pk-cols  (takes precedence over PK_COLS env var).
                      Auto-detected when not set: id → <table>_id → first column.
                      Example: PK_COLS=order_id,line_id

  DELETED_AT_COL      Column name written by soft_delete mode (default: deleted_at).
                      Change if your schema already uses a different column name.

  WATERMARK_COL       Timestamp column for incremental delta extraction.
                      CLI: --watermark-col  (takes precedence over WATERMARK_COL env).
                      Auto-detected when not set: updated_at → created_at.
                      Note: starpump uses >= (inclusive) on the watermark so that any
                      row updated at the exact boundary timestamp is never skipped.

Usage
-----
  # Copy all Snowflake tables ≤ 3 GB (default 8 threads):
  starpump snowflake

  # Copy all Databricks tables from lakehouse.lakehouse_db → Iceberg:
  starpump databricks

  # Copy specific Databricks tables:
  DATABASE=lakehouse SCHEMAS=lakehouse_db starpump databricks INCLUDE_TABLES=customer,orders

  # Dry-run (DDL only, no data copy):
  starpump databricks DRY_RUN=1

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

# ── CDC / incremental mode imports (available when confluent-kafka is installed)
try:
    from confluent_kafka.schema_registry import SchemaRegistryClient
    _SR_AVAILABLE = True
except ImportError:
    _SR_AVAILABLE = False

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

from bao_spark_init import BaoSparkInit, _DATABRICKS_JDBC_JAR
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
    parser.add_argument(
        "--mode",
        choices=["full", "incremental", "custom_sql"],
        default=None,
        help=(
            "Copy mode: full (default), incremental (watermark-based), "
            "or custom_sql (user-supplied JOIN query)."
        ),
    )
    parser.add_argument(
        "--write-mode",
        choices=["standard", "soft_delete", "history"],
        default=None,
        metavar="MODE",
        help=(
            "How updates and deletes are materialised in Iceberg (incremental only).\n"
            "  standard    — MERGE INTO: upsert by PK, hard-delete matching rows (SCD Type 0).\n"
            "  soft_delete — MERGE INTO: upsert by PK, mark deletes with is_deleted=true + deleted_at.\n"
            "  history     — INSERT ALL events (inserts, updates, deletes) as new Iceberg rows;\n"
            "                adds _change_type (INSERT/UPDATE/DELETE) + _change_ts columns.\n"
            "Default: standard."
        ),
    )
    parser.add_argument(
        "--pk-cols",
        default=None,
        metavar="COLS",
        help=(
            "Comma-separated primary key column(s) used for MERGE JOIN and ordered reads.\n"
            "Example: --pk-cols id  or  --pk-cols order_id,line_id\n"
            "If omitted, starpump auto-detects: id → <table>_id → first column.\n"
            "Override via PK_COLS env var (same format)."
        ),
    )
    parser.add_argument(
        "--custom-sql",
        default=None,
        metavar="SQL",
        help=(
            "SQL query to execute in custom_sql mode. "
            "Result is written to --target-table in the Iceberg catalog. "
            "Enclose in single quotes on the shell."
        ),
    )
    parser.add_argument(
        "--target-table",
        default=None,
        metavar="TABLE",
        help="Target Iceberg table name for custom_sql mode.",
    )
    parser.add_argument(
        "--watermark-col",
        default=None,
        metavar="COL",
        help=(
            "Timestamp column for incremental mode watermark comparison "
            "(default: updated_at, falling back to created_at)."
        ),
    )
    # Parse only known args so pytest / spark-submit extra flags are ignored.
    args, _ = parser.parse_known_args()
    return args

_ARGS = _parse_args()

# ── Configuration from environment ────────────────────────────────────────────
# USER is required — no default.  starpump refuses to run without it so that
# every pipeline run is explicitly attributed to an operator.
_USER_RAW = os.environ.get("USER", "").strip()
if not _USER_RAW:
    print(
        "ERROR: USER environment variable is not set.\n"
        "  Set it before running starpump, e.g.:\n"
        "    USER=alice starpump databricks\n"
        "    kubectl exec ... -- env USER=alice TOKEN=... starpump databricks",
        file=sys.stderr,
    )
    sys.exit(1)
USER = _USER_RAW

# Source defaults are driven by _CONNECTORS[source].defaults — populated after
# the connector registry is defined below.  DATABASE / SCHEMAS / ICEBERG_CATALOG
# env vars always override the connector defaults.
_SOURCE_RESOLVED = _ARGS.source.lower()
DATABASE        = os.environ.get("DATABASE")        # resolved after _CONNECTORS
SCHEMAS         = os.environ.get("SCHEMAS")         # resolved after _CONNECTORS
ICEBERG_CATALOG = os.environ.get("ICEBERG_CATALOG") # resolved after _CONNECTORS
S3_BUCKET_OVERRIDE = os.environ.get("S3_BUCKET")
DRY_RUN         = os.environ.get("DRY_RUN", "0") == "1"
BATCH_SIZE      = int(os.environ.get("BATCH_SIZE",   "100000"))
# MAX_ROWS: hard cap on NEW rows written in this run (across all batches, per table).
#   0 = no cap.  Example: MAX_ROWS=1000 appends exactly 1 000 new rows then stops.
MAX_ROWS        = int(os.environ.get("MAX_ROWS", "0"))
# WRITE_MAX_RETRIES: how many times to retry a failed writeTo().append() before
#   giving up on the batch.  Each retry sleeps WRITE_RETRY_SLEEP_S seconds.
#   Fixes: java.lang.IllegalStateException — Iceberg snapshot conflict when
#   multiple threads commit to the same table concurrently.
WRITE_MAX_RETRIES   = int(os.environ.get("WRITE_MAX_RETRIES", "5"))
WRITE_RETRY_SLEEP_S = int(os.environ.get("WRITE_RETRY_SLEEP_S", "3"))
# READ_MAX_RETRIES: how many times to retry a failed read_batch() before giving up.
#   On each retry the batch size is halved so the executor never OOMs on large tables.
#   Fixes: org.apache.spark.SparkException: Job aborted on large tables (>1 GB).
READ_MAX_RETRIES    = int(os.environ.get("READ_MAX_RETRIES", "3"))
# --threads CLI flag takes precedence over the MAX_THREADS env var (default 8).
MAX_THREADS     = _ARGS.threads if _ARGS.threads is not None else int(os.environ.get("MAX_THREADS", "8"))

# ── Mode configuration ─────────────────────────────────────────────────────────
# MODE: full (default) | incremental | custom_sql
# CLI --mode takes precedence over MODE env var.
_raw_mode = (_ARGS.mode or os.environ.get("MODE", "full")).lower()
if _raw_mode not in ("full", "incremental", "custom_sql"):
    print(f"ERROR: Unknown MODE {_raw_mode!r}. Choose: full, incremental, custom_sql", file=sys.stderr)
    sys.exit(1)
MODE: str = _raw_mode

# CUSTOM_SQL: user-supplied SQL query for custom_sql mode.
# CLI --custom-sql takes precedence over CUSTOM_SQL env var.
CUSTOM_SQL: str | None = _ARGS.custom_sql or os.environ.get("CUSTOM_SQL")

# TARGET_TABLE: Iceberg table name for custom_sql results.
# CLI --target-table takes precedence over TARGET_TABLE env var.
TARGET_TABLE: str | None = _ARGS.target_table or os.environ.get("TARGET_TABLE")

# WATERMARK_COL: timestamp column used for incremental delta detection.
# Overridable; defaults are tried in order: updated_at → created_at.
WATERMARK_COL: str | None = _ARGS.watermark_col or os.environ.get("WATERMARK_COL")

# WRITE_MODE: controls how updates/deletes are materialised in Iceberg (incremental mode).
#   standard    — MERGE INTO by PK: upsert live rows, hard-delete removed rows (SCD Type 0).
#   soft_delete — MERGE INTO by PK: upsert live rows, set is_deleted=true + deleted_at on
#                 rows that are no longer in the source window (no physical row removal).
#   history     — INSERT every read row as a new Iceberg row, tagging each with
#                 _change_type (INSERT / UPDATE / DELETE) and _change_ts.  Keeps full
#                 history; never deletes or overwrites Iceberg rows.
# CLI --write-mode takes precedence over WRITE_MODE env var.
_raw_write_mode = (_ARGS.write_mode or os.environ.get("WRITE_MODE", "standard")).lower()
if _raw_write_mode not in ("standard", "soft_delete", "history"):
    print(
        f"ERROR: Unknown WRITE_MODE {_raw_write_mode!r}. "
        "Choose: standard, soft_delete, history",
        file=sys.stderr,
    )
    sys.exit(1)
WRITE_MODE: str = _raw_write_mode

# PRIMARY_KEYS: comma-separated column name(s) used as the MERGE join key and
# for ORDER BY on source reads (index-friendly pagination).
# CLI --pk-cols takes precedence over PK_COLS env var.
# When neither is set, _resolve_primary_keys() auto-detects per table at copy time.
_raw_pk_cols = _ARGS.pk_cols or os.environ.get("PK_COLS", "")
PRIMARY_KEYS: list[str] = (
    [c.strip().lower() for c in _raw_pk_cols.split(",") if c.strip()]
    if _raw_pk_cols else []
)

# DELETED_AT_COL: timestamp column written when WRITE_MODE=soft_delete marks a row deleted.
DELETED_AT_COL: str = os.environ.get("DELETED_AT_COL", "deleted_at")

# DDL_DRIFT_DETECT: compare source schema vs Iceberg before copy and emit ALTER TABLEs.
DDL_DRIFT_DETECT: bool = os.environ.get("DDL_DRIFT_DETECT", "1") == "1"

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

# MAX_TABLE_SIZE_GB: skip tables whose compressed source size exceeds this.
#   0 disables the size filter entirely.
MAX_TABLE_SIZE_GB: float = float(os.environ.get("MAX_TABLE_SIZE_GB", "3.0"))
_SIZE_FILTER_ENABLED = MAX_TABLE_SIZE_GB > 0

# ── QUERY_FILTER — row-level predicate pushed into the source SQL WHERE clause ─
# Raw value is parsed by _parse_query_filters() after _CONNECTORS is defined.
_RAW_QUERY_FILTER = os.environ.get("QUERY_FILTER", "").strip()

# SOURCE, DATABASE, SCHEMAS, ICEBERG_CATALOG, ICEBERG_NAMESPACE are all
# finalised after _CONNECTORS is defined below (they depend on connector defaults).

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


# ── Source-side extraction timestamp helpers ──────────────────────────────────
# Each connector supplies its own capture_ts() that queries the source database
# for its server-side wall-clock.  This keeps the CDC sync-point on the source's
# own transaction timeline rather than the driver/node clock.

def _normalize_ts(ts_str: str) -> str:
    """Normalise any timestamp string to ISO-8601 with Z suffix."""
    ts_str = ts_str.strip().replace(" ", "T")
    if "." not in ts_str:
        ts_str += ".000000"
    if not ts_str.endswith("Z"):
        ts_str += "Z"
    return ts_str


def _sf_capture_ts(spark: SparkSession, opts: dict) -> str:
    """
    Query Snowflake's server-side CURRENT_TIMESTAMP() via the Snowflake
    Spark connector.  Returns ISO-8601Z string.
    CONVERT_TIMEZONE('UTC',...) normalises to UTC regardless of warehouse TZ.
    """
    row = (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**opts)
        .option("query", "SELECT CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::VARCHAR AS ts")
        .load()
        .collect()[0]
    )
    return _normalize_ts(row[0])


def _db_capture_ts(spark: SparkSession, opts: dict) -> str:
    """
    Query Databricks SQL Warehouse server-side NOW() via JDBC.
    Returns ISO-8601Z string.
    """
    row = (
        spark.read.format("jdbc")
        .options(**opts)
        .option("query", "SELECT CAST(NOW() AS STRING) AS ts")
        .load()
        .collect()[0]
    )
    return _normalize_ts(row[0])


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
# Every source registered in _CONNECTORS is fully self-describing.
# The rest of the pipeline is source-agnostic: it never branches on SOURCE.
#
# To register a new source:
#   1. Store credentials in OpenBao at secret/data/platform/<source>.
#   2. Implement the five _<source>_* functions below.
#   3. Add one _SourceConnector entry to _CONNECTORS with its defaults.
#   No changes elsewhere in the pipeline are needed.
#
# Fields
#   spark_format        Spark data-source format string
#   build_opts(bao)     (bao) -> dict   — connection opts for spark.read.format(...)
#   list_tables         (spark, opts) -> list[str]   lower-cased table names
#   table_schema        (spark, opts, table) -> StructType
#   table_sizes         (spark, opts) -> dict[str, float]  {table: gb}
#   capture_ts          (spark, opts) -> str   ISO-8601Z server-side timestamp
#   s3_prefix           path segment under s3://<bucket>/  e.g. "iceberg/warehouse"
#   default_database    default DATABASE env value for this source
#   default_schema      default SCHEMAS  env value for this source
#   default_catalog     default ICEBERG_CATALOG env value for this source
#   map_schema          (raw_schema) -> StructType  — map source types to Spark types

from dataclasses import dataclass
from typing import Callable

@dataclass
class _SourceConnector:
    spark_format:          str
    build_opts:            Callable   # (bao) -> dict
    list_tables:           Callable   # (spark, opts) -> list[str]
    table_schema:          Callable   # (spark, opts, table) -> StructType
    table_sizes:           Callable   # (spark, opts) -> dict[str, float]
    capture_ts:            Callable   # (spark, opts) -> str ISO-8601Z
    read_batch:            Callable   # (spark, opts, table, offset, batch_size, where_clause="") -> DataFrame
    primary_keys:          Callable   # (spark, opts, table) -> list[str]  — real PK from source catalog
    s3_prefix:             str        # path under s3://<bucket>/
    default_database:      str
    default_schema:        str
    default_catalog:       str
    map_schema:            Callable   # (StructType) -> StructType
    supports_offset_resume: bool = True
    # True  → source supports LIMIT/OFFSET pagination; partial copies can be
    #         resumed from the last committed Iceberg row count.  (Snowflake)
    # False → source must always be read in full from offset 0; LIMIT/OFFSET
    #         is unreliable or unsupported.  (Databricks JDBC)


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


def _sf_primary_keys(spark: SparkSession, opts: dict, table: str) -> list[str]:
    """
    Discover primary key columns for a Snowflake table using SHOW PRIMARY KEYS.

    SHOW PRIMARY KEYS IN TABLE <name> is a Snowflake-native metadata command —
    it does not execute a query plan, reads no data, and returns results in
    key_sequence order so composite PKs come back correctly ordered.
    The result set always contains a 'column_name' column regardless of schema,
    database, or naming convention.
    """
    try:
        rows = (
            spark.read.format("net.snowflake.spark.snowflake")
            .options(**opts)
            .option("query", f'SHOW PRIMARY KEYS IN TABLE "{table.upper()}"')
            .load()
            .collect()
        )
        # SHOW PRIMARY KEYS result columns: created_on, database_name, schema_name,
        # table_name, column_name, key_sequence, constraint_name, rely, comment
        return [r["column_name"].lower() for r in rows]
    except Exception as exc:
        logger.debug("[%s] Snowflake SHOW PRIMARY KEYS failed (%s) — will fall back.", table, exc)
        return []


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


def _sf_read_batch(
    spark: SparkSession,
    opts: dict,
    table: str,
    offset: int,
    batch_size: int,
    where_clause: str = "",
    order_clause: str = "",
) -> DataFrame:
    """
    Read one batch from Snowflake.
    where_clause — optional SQL predicate (without WHERE keyword) from QUERY_FILTER.
    order_clause — optional ORDER BY fragment from _build_pk_order_clause(); when
                   provided it replaces the generic ORDER BY (SELECT NULL) so reads
                   walk the PK index for stable LIMIT/OFFSET pagination.
    """
    where = f" WHERE {where_clause}" if where_clause else ""
    order = order_clause if order_clause else "ORDER BY (SELECT NULL)"
    query = (
        f'SELECT * FROM "{table.upper()}"{where} '
        f"{order} "
        f"LIMIT {batch_size} OFFSET {offset}"
    )
    return (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**opts)
        .option("query", query)
        .load()
    )


# ── Snowflake schema mapper ────────────────────────────────────────────────────

def _sf_map_schema(raw_schema: StructType) -> StructType:
    return StructType([
        StructField(f.name, _sf_to_spark(f.dataType.simpleString()), True)
        for f in raw_schema.fields
    ])


# ── Databricks → Spark/Iceberg type mapping ────────────────────────────────────
# JDBC getColumnType() returns java.sql.Types integers; Databricks Simba driver
# also exposes type names as strings in ResultSetMetaData.getColumnTypeName().
# We map by type name string (upper-cased) identical to the Snowflake pattern.
_DB_TYPE_MAP: dict[str, Any] = {
    "STRING":    StringType(),   "VARCHAR":   StringType(),   "CHAR":      StringType(),
    "TEXT":      StringType(),   "BINARY":    StringType(),   "VARIANT":   StringType(),
    "ARRAY":     StringType(),   "MAP":       StringType(),   "STRUCT":    StringType(),
    "TINYINT":   ByteType(),     "SMALLINT":  ShortType(),    "INT":       IntegerType(),
    "INTEGER":   IntegerType(),  "BIGINT":    LongType(),     "LONG":      LongType(),
    "FLOAT":     FloatType(),    "REAL":      FloatType(),    "DOUBLE":    DoubleType(),
    "DECIMAL":   DecimalType(38, 10),         "NUMERIC":     DecimalType(38, 10),
    "BOOLEAN":   BooleanType(),  "DATE":      DateType(),
    "TIMESTAMP": TimestampType(),             "TIMESTAMP_NTZ": TimestampType(),
    "TIMESTAMP_LTZ": TimestampType(),
}


def _db_to_spark(db_type: str) -> Any:
    """Map a Databricks JDBC column type string to a Spark DataType."""
    upper = db_type.upper().strip().split("(")[0].strip()
    if upper in ("DECIMAL", "NUMERIC") and "(" in db_type.upper():
        inner = db_type.upper()[db_type.upper().index("(") + 1 : db_type.upper().index(")")]
        parts = inner.split(",")
        p = int(parts[0].strip())
        s = int(parts[1].strip()) if len(parts) > 1 else 0
        return DecimalType(p, s)
    return _DB_TYPE_MAP.get(upper, StringType())


def _db_map_schema(raw_schema: StructType) -> StructType:
    # _db_table_schema() already maps via JDBC metadata; pass through unchanged.
    return raw_schema


# ── Databricks connector implementation ────────────────────────────────────────

def _jdbc_primary_keys(spark: SparkSession, opts: dict, table: str) -> list[str]:
    """
    Discover primary key columns for any JDBC-based source using the standard
    java.sql.DatabaseMetaData.getPrimaryKeys() API.

    This is the single universal implementation shared by PostgreSQL, Oracle,
    and Databricks.  It requires no knowledge of the source database type,
    schema naming convention, or catalog structure — the JDBC driver itself
    resolves the metadata entirely from the connection URL and the table name.

    DatabaseMetaData.getPrimaryKeys(catalog, schema, table) returns one row per
    PK column with KEY_SEQ (1-based sequence) and COLUMN_NAME.  We sort by
    KEY_SEQ so composite PKs come back in declaration order regardless of the
    driver's default sort.

    The connection is opened via py4j using the same URLClassLoader pattern
    already used by _db_list_tables() — this avoids DriverManager classloader
    isolation issues that arise when Spark loads the JAR into its own
    MutableURLClassLoader.

    Returns [] when:
      • the table has no PK constraint defined
      • getPrimaryKeys() raises (e.g. insufficient privileges)
    In both cases _resolve_primary_keys() falls through to the name heuristic.
    """
    try:
        jvm  = spark.sparkContext._jvm
        gw   = spark.sparkContext._gateway

        props = jvm.java.util.Properties()
        for k, v in opts.items():
            if k not in ("url", "driver"):
                props.setProperty(k, str(v))

        # Load the driver via its own URLClassLoader so Class.forName() works
        # regardless of which classloader Spark used to load the JAR.
        # _DATABRICKS_JDBC_JAR is only the Databricks path; for other drivers
        # the JVM already has the JAR on the system classpath (they are baked
        # into /opt/spark/jars/ and loaded by Spark at startup), so we can
        # use DriverManager.getConnection() directly for non-Databricks drivers.
        driver_cls = opts.get("driver", "")
        if "databricks" in driver_cls.lower():
            _jar_url_arr    = gw.new_array(jvm.java.net.URL, 1)
            _jar_url_arr[0] = jvm.java.net.URL("file://" + _DATABRICKS_JDBC_JAR)
            _ucl = jvm.java.net.URLClassLoader(
                _jar_url_arr, jvm.ClassLoader.getSystemClassLoader()
            )
            _drv_cls  = jvm.Class.forName(driver_cls, True, _ucl)
            _drv_inst = _drv_cls.newInstance()
            conn = _drv_inst.connect(opts["url"], props)
        else:
            # PostgreSQL and Oracle JARs are on Spark's system classpath —
            # DriverManager resolves them automatically from the URL prefix.
            conn = jvm.java.sql.DriverManager.getConnection(
                opts["url"], opts.get("user", ""), opts.get("password", "")
            )

        meta = conn.getMetaData()
        # getPrimaryKeys(catalog, schema, table) — pass None for catalog and
        # schema so the driver resolves them from the active connection context
        # (set via currentSchema / sessionInitStatement in the JDBC URL).
        # table is lower-cased throughout starpump; Oracle's JDBC driver
        # requires identifiers in uppercase to match ALL_CONSTRAINTS.
        # PostgreSQL accepts both cases, so uppercasing is safe for all drivers.
        rs = meta.getPrimaryKeys(None, None, table.upper())
        pk_rows: list[tuple[int, str]] = []
        while rs.next():
            seq  = rs.getInt("KEY_SEQ")
            col  = rs.getString("COLUMN_NAME").lower()
            pk_rows.append((seq, col))
        rs.close()
        conn.close()

        pk_rows.sort(key=lambda x: x[0])   # sort by KEY_SEQ (declaration order)
        return [col for _, col in pk_rows]

    except Exception as exc:
        logger.debug("[%s] JDBC getPrimaryKeys() failed (%s) — will fall back.", table, exc)
        return []


def _db_build_opts(bao: "BaoSparkInit") -> dict:
    """Return JDBC options for the Databricks SQL Warehouse."""
    return bao.databricks_jdbc_options(catalog=DATABASE, schema=SCHEMAS)


def _db_list_tables(spark: SparkSession, opts: dict) -> list[str]:
    """
    List all base tables in the Databricks schema via JDBC metadata API.

    Neither SHOW TABLES nor INFORMATION_SCHEMA work reliably via Spark JDBC:
    - SHOW TABLES: Spark wraps the query in SELECT * FROM (...) WHERE 1=0 to
      infer the result schema, which Databricks rejects for DDL-like statements.
    - INFORMATION_SCHEMA: Unity Catalog's JDBC layer does not expose it via the
      SQL interface when the catalog is specified in ConnCatalog on the URL.

    Using java.sql.DatabaseMetaData.getTables() via a JDBC connection bypasses
    both issues — it queries the catalog metadata layer directly.
    """
    # Access the JDBC driver via py4j / Spark JVM gateway.
    # py4j does not auto-convert Python lists to Java arrays, so we build the
    # String[] type-hint explicitly using the JVM gateway's java_array helper.
    jvm   = spark.sparkContext._jvm
    gw    = spark.sparkContext._gateway

    props = jvm.java.util.Properties()
    for k, v in opts.items():
        if k not in ("url", "driver"):
            props.setProperty(k, str(v))

    # Class.forName() fails when spark.jars loads the JAR into Spark's
    # MutableURLClassLoader but that classloader is not the thread context
    # classloader at driver-side metadata query time.
    # Fix: build a URLClassLoader pointing at the JAR, load the Driver class,
    # instantiate it directly, and call Driver.connect() — bypassing
    # DriverManager entirely so classloader isolation does not matter.
    _jar_url_arr = gw.new_array(jvm.java.net.URL, 1)
    _jar_url_arr[0] = jvm.java.net.URL("file://" + _DATABRICKS_JDBC_JAR)
    _ucl = jvm.java.net.URLClassLoader(_jar_url_arr, jvm.ClassLoader.getSystemClassLoader())
    _driver_cls  = jvm.Class.forName(opts["driver"], True, _ucl)
    # py4j does not support getDeclaredConstructor() — use newInstance() directly.
    _driver_inst = _driver_cls.newInstance()
    conn  = _driver_inst.connect(opts["url"], props)
    meta  = conn.getMetaData()

    # getTables(catalog, schemaPattern, tableNamePattern, types[])
    # catalog=None means "all catalogs"; we pass the Unity Catalog name directly.
    # schemaPattern must be exact (Unity Catalog schemas are case-sensitive).
    types = gw.new_array(jvm.java.lang.String, 1)
    types[0] = "TABLE"
    rs    = meta.getTables(DATABASE, SCHEMAS, "%", types)
    names = []
    while rs.next():
        names.append(rs.getString("TABLE_NAME").lower())
    rs.close()
    conn.close()

    names = sorted(names)
    logger.info(
        "Discovered %d tables in %s.%s: %s", len(names), DATABASE, SCHEMAS, names
    )
    return names


def _db_table_schema(spark: SparkSession, opts: dict, table: str) -> StructType:
    """
    Retrieve the Databricks table schema via JDBC metadata (no row scan).

    Using .option("dbtable", ...) with .load() causes Spark JDBC to call
    JDBC DatabaseMetaData.getColumns() for schema inference — no SQL query
    is executed and no data rows are fetched.  This avoids the Databricks
    JDBC driver (3.4.x) bug where a paginated SELECT returns the column
    header row as a data row when LIMIT/OFFSET is used.
    """
    raw_schema = (
        spark.read.format("jdbc")
        .options(**opts)
        .option("dbtable", f"(SELECT * FROM `{DATABASE}`.`{SCHEMAS}`.`{table}` WHERE 1=0) t")
        .option("numPartitions", "1")
        .load()
    ).schema

    mapped_fields = []
    for f in raw_schema.fields:
        mapped_fields.append(
            StructField(f.name.lower(), _db_to_spark(f.dataType.simpleString()), True)
        )
    return StructType(mapped_fields)


def _db_table_sizes(spark: SparkSession, opts: dict) -> dict[str, float]:
    """
    Databricks Delta table sizes are not queryable via Spark JDBC.
    DESCRIBE DETAIL is a Spark SQL command, not a JDBC-passthrough query;
    Spark wraps any .option("query", ...) in SELECT * FROM (...) WHERE 1=0
    to infer the schema, which Databricks rejects for DESCRIBE statements.

    Databricks tables are typically small relative to the 3 GB default filter
    (Delta format is efficient and products/customers tables are hundreds of MB
    at most).  Returning an empty dict means all tables are treated as 0 GB
    and the size filter never drops them — the correct behaviour.

    Override MAX_TABLE_SIZE_GB=0 to disable the size filter entirely if needed.
    """
    return {}


def _db_read_batch(
    spark: SparkSession,
    opts: dict,
    table: str,
    offset: int,
    batch_size: int,
    where_clause: str = "",
    order_clause: str = "",
) -> DataFrame:
    """
    Read one batch from Databricks via py4j JDBC on the driver, then create a
    Spark DataFrame from the collected rows.

    Databricks JDBC 3.4.x has a systematic bug when used through Spark's JDBC
    partition reader (JdbcUtils.resultSetToRows): the driver returns the column
    header row as the first data row of the result set, causing:
        NumberFormatException: For input string: "product_id"
    This happens regardless of fetchsize or dbtable vs query options because
    Spark always calls Statement.setFetchSize() internally in its JDBC layer.

    Fix: bypass Spark JDBC entirely for the data read.  Use the same py4j
    URLClassLoader approach that _db_list_tables uses (which proved stable), run
    a SELECT with a server-side LIMIT/OFFSET via the driver directly on the
    driver JVM, collect all rows into a Python list, then hand the result to
    spark.createDataFrame() for distributed processing.  The driver JVM never
    calls setFetchSize so the header-row bug is never triggered.

    Note: Databricks SQL does not support reliable cursor-based pagination via
    LIMIT/OFFSET on large result sets (results are unordered unless an ORDER BY
    is present, and OFFSET scans from the start each time).  The connector
    therefore always reads ALL rows in a single pass regardless of offset.
    The offset argument is kept for API compatibility with the generic batch
    loop but is not used to skip rows — the loop breaks after the first
    (and only) non-empty batch.

    where_clause is an optional SQL predicate fragment (without WHERE keyword)
    injected from QUERY_FILTER to copy only matching rows.
    order_clause is an optional ORDER BY fragment from _build_pk_order_clause().
    """
    schema = spark.read.format("jdbc") \
        .options(**opts) \
        .option("dbtable",
                f"(SELECT * FROM `{DATABASE}`.`{SCHEMAS}`.`{table}` WHERE 1=0) t") \
        .load().schema

    jvm  = spark.sparkContext._jvm
    gw   = spark.sparkContext._gateway

    props = jvm.java.util.Properties()
    for k, v in opts.items():
        if k not in ("url", "driver"):
            props.setProperty(k, str(v))

    _jar_url_arr = gw.new_array(jvm.java.net.URL, 1)
    _jar_url_arr[0] = jvm.java.net.URL("file://" + _DATABRICKS_JDBC_JAR)
    _ucl = jvm.java.net.URLClassLoader(_jar_url_arr, jvm.ClassLoader.getSystemClassLoader())
    _driver_cls  = jvm.Class.forName(opts["driver"], True, _ucl)
    _driver_inst = _driver_cls.newInstance()
    conn = _driver_inst.connect(opts["url"], props)

    where = f" WHERE {where_clause}" if where_clause else ""
    order = f" {order_clause}" if order_clause else ""
    sql = (
        f"SELECT * FROM `{DATABASE}`.`{SCHEMAS}`.`{table}`{where}{order} "
        f"LIMIT {batch_size} OFFSET {offset}"
    )
    stmt = conn.createStatement()
    rs   = stmt.executeQuery(sql)
    meta = rs.getMetaData()
    ncols = meta.getColumnCount()

    import datetime as _dt, decimal as _dec

    def _to_python(val):
        """Convert a py4j Java object returned by rs.getObject() to a Python native."""
        if val is None:
            return None
        cls = type(val).__name__
        if cls == "JavaObject":
            # Inspect the Java class name to decide the conversion.
            jcls = val.getClass().getName()
            if jcls == "java.sql.Timestamp":
                # Timestamp.toInstant().toEpochMilli() → UTC datetime
                millis = val.getTime()
                return _dt.datetime.utcfromtimestamp(millis / 1000.0)
            if jcls == "java.sql.Date":
                millis = val.getTime()
                return _dt.date.fromtimestamp(millis / 1000.0)
            if jcls == "java.math.BigDecimal":
                return _dec.Decimal(val.toPlainString())
            # Fallback: let py4j str-convert whatever remains
            return str(val)
        return val

    rows = []
    while rs.next():
        row = []
        for i in range(1, ncols + 1):
            row.append(_to_python(rs.getObject(i)))
        rows.append(tuple(row))
    rs.close()
    conn.close()

    return spark.createDataFrame(rows, schema=schema)


# ═══════════════════════════════════════════════════════════════════════════════
# ── PostgreSQL connector ───────────────────────────────────────────────────────
# Standard Spark JDBC + postgresql-42.7.4.jar (already in image).
# LIMIT/OFFSET ORDER BY is fully reliable on PostgreSQL.
# ═══════════════════════════════════════════════════════════════════════════════

def _pg_build_opts(bao: "BaoSparkInit") -> dict:
    return bao.postgres_jdbc_options(database=DATABASE, schema=SCHEMAS)


def _pg_list_tables(spark: SparkSession, opts: dict) -> list[str]:
    """List all base tables in the PostgreSQL schema via INFORMATION_SCHEMA."""
    query = (
        f"SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = '{SCHEMAS}' AND table_type = 'BASE TABLE' "
        f"ORDER BY table_name"
    )
    rows = (
        spark.read.format("jdbc")
        .options(**opts)
        .option("query", query)
        .load()
        .collect()
    )
    names = sorted(r[0].lower() for r in rows)
    logger.info("Discovered %d tables in %s.%s: %s", len(names), DATABASE, SCHEMAS, names)
    return names




def _pg_table_schema(spark: SparkSession, opts: dict, table: str) -> StructType:
    return (
        spark.read.format("jdbc")
        .options(**opts)
        .option("dbtable", f'(SELECT * FROM "{SCHEMAS}"."{table}" WHERE 1=0) t')
        .load()
    ).schema


def _pg_table_sizes(spark: SparkSession, opts: dict) -> dict[str, float]:
    """Query pg_total_relation_size for table sizes in GB."""
    query = (
        f"SELECT relname, pg_total_relation_size(c.oid) "
        f"FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        f"WHERE n.nspname = '{SCHEMAS}' AND c.relkind = 'r'"
    )
    try:
        rows = (
            spark.read.format("jdbc")
            .options(**opts)
            .option("query", query)
            .load()
            .collect()
        )
        _gb = 1024 ** 3
        return {r[0].lower(): (r[1] or 0) / _gb for r in rows}
    except Exception as exc:
        logger.warning("Could not query pg table sizes: %s — treating all as 0 GB.", exc)
        return {}


def _pg_capture_ts(spark: SparkSession, opts: dict) -> str:
    row = (
        spark.read.format("jdbc")
        .options(**opts)
        .option("query", "SELECT TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS ts")
        .load()
        .collect()[0]
    )
    return _normalize_ts(row[0])


def _pg_read_batch(
    spark: SparkSession,
    opts: dict,
    table: str,
    offset: int,
    batch_size: int,
    where_clause: str = "",
    order_clause: str = "",
) -> DataFrame:
    where = f" WHERE {where_clause}" if where_clause else ""
    order = order_clause if order_clause else "ORDER BY 1"
    query = (
        f'SELECT * FROM "{SCHEMAS}"."{table}"{where} '
        f"{order} "
        f"LIMIT {batch_size} OFFSET {offset}"
    )
    return (
        spark.read.format("jdbc")
        .options(**opts)
        .option("query", query)
        .load()
    )


def _pg_map_schema(raw_schema: StructType) -> StructType:
    # Spark JDBC infers PostgreSQL types natively — pass through unchanged.
    return raw_schema


# ═══════════════════════════════════════════════════════════════════════════════
# ── Oracle connector ───────────────────────────────────────────────────────────
# Standard Spark JDBC + ojdbc11-23.4.0.24.05.jar.
# SCHEMAS maps to the Oracle schema/owner name (upper-cased).
# LIMIT/OFFSET uses FETCH FIRST / OFFSET (Oracle 12c+ SQL syntax).
# ═══════════════════════════════════════════════════════════════════════════════

def _ora_build_opts(bao: "BaoSparkInit") -> dict:
    return bao.oracle_jdbc_options(schema=SCHEMAS)


def _ora_list_tables(spark: SparkSession, opts: dict) -> list[str]:
    """List all tables owned by the schema/user in Oracle ALL_TABLES."""
    query = (
        f"SELECT LOWER(table_name) FROM all_tables "
        f"WHERE owner = UPPER('{SCHEMAS}') ORDER BY table_name"
    )
    rows = (
        spark.read.format("jdbc")
        .options(**opts)
        .option("query", query)
        .load()
        .collect()
    )
    names = sorted(r[0] for r in rows)
    logger.info("Discovered %d tables in %s.%s: %s", len(names), DATABASE, SCHEMAS, names)
    return names




def _ora_table_schema(spark: SparkSession, opts: dict, table: str) -> StructType:
    return (
        spark.read.format("jdbc")
        .options(**opts)
        .option("dbtable",
                f'(SELECT * FROM "{SCHEMAS.upper()}"."{table.upper()}" WHERE 1=0)')
        .load()
    ).schema


def _ora_table_sizes(spark: SparkSession, opts: dict) -> dict[str, float]:
    """Query DBA_SEGMENTS for table sizes (falls back gracefully)."""
    query = (
        f"SELECT LOWER(segment_name), bytes FROM dba_segments "
        f"WHERE owner = UPPER('{SCHEMAS}') AND segment_type = 'TABLE'"
    )
    try:
        rows = (
            spark.read.format("jdbc")
            .options(**opts)
            .option("query", query)
            .load()
            .collect()
        )
        _gb = 1024 ** 3
        return {r[0]: (r[1] or 0) / _gb for r in rows}
    except Exception as exc:
        logger.warning("Could not query Oracle table sizes: %s — treating all as 0 GB.", exc)
        return {}


def _ora_capture_ts(spark: SparkSession, opts: dict) -> str:
    row = (
        spark.read.format("jdbc")
        .options(**opts)
        .option("query",
                "SELECT TO_CHAR(SYS_EXTRACT_UTC(SYSTIMESTAMP), "
                "'YYYY-MM-DD\"T\"HH24:MI:SS.FF6\"Z\"') AS ts FROM dual")
        .load()
        .collect()[0]
    )
    return _normalize_ts(row[0])


def _ora_read_batch(
    spark: SparkSession,
    opts: dict,
    table: str,
    offset: int,
    batch_size: int,
    where_clause: str = "",
    order_clause: str = "",
) -> DataFrame:
    where = f" WHERE {where_clause}" if where_clause else ""
    order = order_clause if order_clause else "ORDER BY 1"
    # Oracle 12c+ OFFSET/FETCH syntax (standard SQL:2008)
    query = (
        f'SELECT * FROM "{SCHEMAS.upper()}"."{table.upper()}"{where} '
        f"{order} "
        f"OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
    )
    return (
        spark.read.format("jdbc")
        .options(**opts)
        .option("query", query)
        .load()
    )


def _ora_map_schema(raw_schema: StructType) -> StructType:
    # Oracle JDBC maps DATE → TimestampType and NUMBER → DecimalType automatically.
    # Pass through — Spark infers correctly.
    return raw_schema


# ═══════════════════════════════════════════════════════════════════════════════
# ── MongoDB connector ──────────────────────────────────────────────────────────
# Uses the MongoDB Spark connector 10.x (format: "mongodb").
# Collections = tables.  SCHEMAS is the database name.
# No reliable OFFSET on MongoDB cursors — full collection read each run.
# ═══════════════════════════════════════════════════════════════════════════════

def _mgo_build_opts(bao: "BaoSparkInit") -> dict:
    return bao.mongodb_options(database=DATABASE)


def _mgo_list_tables(spark: SparkSession, opts: dict) -> list[str]:
    """
    List all collections in the MongoDB database.
    The MongoDB Spark connector exposes listCollections via the
    'spark.mongodb.read.collection' option set to '*' (wildcard).
    We use the connector's catalog API via a helper query instead.
    """
    mg = {k: v for k, v in opts.items()}
    # Use the Python pymongo driver (available via mongo-spark-connector env)
    # to list collections — Spark connector 10.x has no native listCollections.
    try:
        from pymongo import MongoClient  # type: ignore
        uri = mg.get("spark.mongodb.read.connection.uri", "")
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        names = sorted(
            c.lower()
            for c in client[DATABASE].list_collection_names()
            if not c.startswith("system.")
        )
        client.close()
    except Exception as exc:
        logger.warning(
            "pymongo not available (%s) — falling back to single-collection mode. "
            "Set DATABASE to the collection name and SCHEMAS to the database name.", exc
        )
        # Fallback: treat SCHEMAS as database, DATABASE as collection name.
        names = [DATABASE.lower()]
    logger.info("Discovered %d collections in %s: %s", len(names), SCHEMAS, names)
    return names


def _mgo_primary_keys(spark: SparkSession, opts: dict, table: str) -> list[str]:
    """
    MongoDB always uses _id as its primary key — it is the only indexed,
    unique, non-nullable field that every document is guaranteed to have.
    No catalog query needed.
    """
    return ["_id"]


def _mgo_table_schema(spark: SparkSession, opts: dict, table: str) -> StructType:
    """
    Infer schema by sampling the collection.

    Uses SinglePartitionPartitioner + a $limit:1000 aggregation pipeline so the
    connector opens exactly one cursor and fetches at most 1 000 documents
    server-side — instead of scanning the full collection to satisfy
    sample.size client-side.
    """
    return (
        spark.read.format("mongodb")
        .options(**opts)
        .option("collection", table)
        .option("spark.mongodb.read.sample.size", "1000")
        .option("spark.mongodb.read.aggregation.pipeline", '[{"$limit": 1000}]')
        .option("spark.mongodb.read.partitioner",
                "com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner")
        .load()
    ).schema


def _mgo_table_sizes(spark: SparkSession, opts: dict) -> dict[str, float]:
    """MongoDB collection sizes — not queryable via Spark; return empty (treat as 0 GB)."""
    return {}


def _mgo_capture_ts(spark: SparkSession, opts: dict) -> str:
    """Use driver wall-clock for MongoDB (no SQL server-time query available)."""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def _mgo_build_pipeline(where_clause: str, batch_size: int) -> str:
    """
    Build a MongoDB aggregation pipeline JSON string from a SQL-style where_clause.

    Supported simple forms (covers QUERY_FILTER quick-test cases):
      field<=value   field>=value   field<value   field>value
      field=value    field!=value

    For anything more complex, pass a raw JSON pipeline via the environment variable
    MGO_PIPELINE (e.g. MGO_PIPELINE='[{"$match":{"tier":"PLATINUM"}},{"$limit":50}]')
    which is injected verbatim.

    Always appends {"$limit": batch_size} so the connector never reads beyond
    one batch worth of documents — essential for quick tests (small batch_size)
    and safe for full copies (batch_size=100000 caps each Spark job).
    """
    import re, json

    # Check for raw pipeline override via env
    raw = os.environ.get("MGO_PIPELINE", "").strip()
    if raw:
        return raw

    stages: list[dict] = []

    if where_clause:
        # Strip outer parentheses added by _get_where_clause() — e.g. "(product_id<=50)"
        clause = where_clause.strip().lstrip("(").rstrip(")")
        # Parse "field OP value" — field name may contain dots (nested docs)
        m = re.match(
            r"^([\w.]+)\s*(<=|>=|!=|<|>|=)\s*(.+)$",
            clause.strip(),
        )
        if m:
            field, op, raw_val = m.group(1), m.group(2), m.group(3).strip().strip("'\"")
            # Try numeric coercion, fall back to string
            try:
                val: object = int(raw_val)
            except ValueError:
                try:
                    val = float(raw_val)
                except ValueError:
                    val = raw_val
            mongo_op = {"<=": "$lte", ">=": "$gte", "<": "$lt", ">": "$gt",
                        "=": "$eq", "!=": "$ne"}[op]
            stages.append({"$match": {field: {mongo_op: val}}})
        else:
            logger.warning(
                "[mgo] Cannot parse QUERY_FILTER '%s' into $match — "
                "full collection scan. Use MGO_PIPELINE for complex filters.",
                where_clause,
            )

    stages.append({"$limit": batch_size})
    return json.dumps(stages)


def _mgo_read_batch(
    spark: SparkSession,
    opts: dict,
    table: str,
    offset: int,
    batch_size: int,
    where_clause: str = "",
    order_clause: str = "",   # MongoDB has no SQL ORDER BY; parameter kept for API compatibility
) -> DataFrame:
    """
    Read one batch from a MongoDB collection via the Spark MongoDB connector 10.x.

    Key design decisions:
    - No reliable OFFSET cursor on MongoDB — we always read from the start.
    - QUERY_FILTER is translated to a real MongoDB $match aggregation stage so
      the filter runs server-side, not as a post-load Spark filter.
    - $limit is always appended (= batch_size) so the connector never scans
      beyond one batch — critical for quick tests with small batch_size.
    - For complex predicates set MGO_PIPELINE env var with raw JSON pipeline.
    """
    pipeline = _mgo_build_pipeline(where_clause, batch_size)
    if where_clause or batch_size < 100_000:
        logger.info("[%s] MongoDB pipeline: %s", table, pipeline)
    read = (
        spark.read.format("mongodb")
        .options(**opts)
        .option("collection", table)
        .option("spark.mongodb.read.aggregation.pipeline", pipeline)
    )
    # The MongoDB Spark connector applies $limit per partition, not globally.
    # Force a single partition whenever the pipeline contains a $limit so the
    # cap is honoured exactly (e.g. MAX_ROWS=1000 → exactly 1 000 docs returned).
    if batch_size < 100_000:
        read = read.option("spark.mongodb.read.partitioner",
                           "com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner")
    return read.load()


def _mgo_map_schema(raw_schema: StructType) -> StructType:
    # MongoDB connector infers schema from sampled documents; pass through.
    # _id (ObjectId) is mapped to StringType by the connector.
    return raw_schema


# ── Connector registry ────────────────────────────────────────────────────────
# Each entry is fully self-describing.  The pipeline never branches on SOURCE.
_CONNECTORS: dict[str, _SourceConnector] = {
    "snowflake": _SourceConnector(
        spark_format           = "net.snowflake.spark.snowflake",
        build_opts             = _sf_build_opts,
        list_tables            = _sf_list_tables,
        table_schema           = _sf_table_schema,
        table_sizes            = _sf_table_sizes,
        capture_ts             = _sf_capture_ts,
        read_batch             = _sf_read_batch,
        primary_keys           = _sf_primary_keys,
        s3_prefix              = "tpcds",
        default_database       = "SNOWFLAKE_SAMPLE_DATA",
        default_schema         = "TPCDS_SF10TCL",
        default_catalog        = "polaris",
        map_schema             = _sf_map_schema,
        supports_offset_resume = True,   # LIMIT/OFFSET ORDER BY is reliable
    ),
    "databricks": _SourceConnector(
        spark_format           = "jdbc",
        build_opts             = _db_build_opts,
        list_tables            = _db_list_tables,
        table_schema           = _db_table_schema,
        table_sizes            = _db_table_sizes,
        capture_ts             = _db_capture_ts,
        read_batch             = _db_read_batch,
        primary_keys           = _jdbc_primary_keys,
        s3_prefix              = "iceberg/warehouse",
        default_database       = "lakehouse",
        default_schema         = "lakehouse_db",
        default_catalog        = "databricks",
        map_schema             = _db_map_schema,
        supports_offset_resume = False,  # full re-read every run
    ),
    "postgres": _SourceConnector(
        spark_format           = "jdbc",
        build_opts             = _pg_build_opts,
        list_tables            = _pg_list_tables,
        table_schema           = _pg_table_schema,
        table_sizes            = _pg_table_sizes,
        capture_ts             = _pg_capture_ts,
        read_batch             = _pg_read_batch,
        primary_keys           = _jdbc_primary_keys,
        s3_prefix              = "iceberg/pg_lakehouse",   # must match Polaris warehouse allowedLocations
        default_database       = "cache_testing",
        default_schema         = "public",
        default_catalog        = "postgres",
        map_schema             = _pg_map_schema,
        supports_offset_resume = True,   # LIMIT/OFFSET ORDER BY is reliable
    ),
    "oracle": _SourceConnector(
        spark_format           = "jdbc",
        build_opts             = _ora_build_opts,
        list_tables            = _ora_list_tables,
        table_schema           = _ora_table_schema,
        table_sizes            = _ora_table_sizes,
        capture_ts             = _ora_capture_ts,
        read_batch             = _ora_read_batch,
        primary_keys           = _jdbc_primary_keys,
        s3_prefix              = "iceberg/ora_lakehouse",  # must match Polaris warehouse allowedLocations
        default_database       = "XEPDB1",
        default_schema         = "TPCDS",
        default_catalog        = "oracle",
        map_schema             = _ora_map_schema,
        supports_offset_resume = True,   # OFFSET/FETCH NEXT supported on Oracle 12c+
    ),
    "mongodb": _SourceConnector(
        spark_format           = "mongodb",
        build_opts             = _mgo_build_opts,
        list_tables            = _mgo_list_tables,
        table_schema           = _mgo_table_schema,
        table_sizes            = _mgo_table_sizes,
        capture_ts             = _mgo_capture_ts,
        read_batch             = _mgo_read_batch,
        primary_keys           = _mgo_primary_keys,
        s3_prefix              = "iceberg/mgo_lakehouse",  # must match Polaris warehouse allowedLocations
        default_database       = "cache_testing",
        default_schema         = "cache_testing",
        default_catalog        = "mongodb",
        map_schema             = _mgo_map_schema,
        supports_offset_resume = False,  # full collection read every run
    ),
}

# Validate source name against the registry (fail fast, friendly message).
if _SOURCE_RESOLVED not in _CONNECTORS:
    print(
        f"ERROR: Unknown source {_SOURCE_RESOLVED!r}.\n"
        f"  Registered sources: {', '.join(sorted(_CONNECTORS))}\n"
        f"  Usage: starpump <source>   e.g.  starpump databricks",
        file=sys.stderr,
    )
    sys.exit(1)

SOURCE = _SOURCE_RESOLVED

# ── Resolve DATABASE / SCHEMAS / ICEBERG_CATALOG against connector defaults ───
# env vars set above take precedence; fall back to the connector's defaults so
# that `starpump databricks` works without any extra env configuration.
_conn_defaults = _CONNECTORS[SOURCE]
if DATABASE is None:
    DATABASE = _conn_defaults.default_database
if SCHEMAS is None:
    SCHEMAS = _conn_defaults.default_schema
if ICEBERG_CATALOG is None:
    ICEBERG_CATALOG = _conn_defaults.default_catalog

# Iceberg namespace = source schema lower-cased (reassign after SCHEMAS resolved)
ICEBERG_NAMESPACE = SCHEMAS.lower()


# ── QUERY_FILTER parser ────────────────────────────────────────────────────────
import re as _re

def _parse_query_filters(raw: str) -> dict[str, str]:
    """
    Parse the QUERY_FILTER env-var into a mapping of
      { table_name_lower: "WHERE clause fragment" }

    The special key "" (empty string) holds schema-level predicates that
    apply to every table.  Table-level predicates are stored under their
    lower-cased table name and override/extend the schema-level predicate
    for that table (combined with AND).

    Grammar for each comma-separated token (spaces around the comma are ignored):

      Schema-level:
        <column> <op> <value>
        <column> IS [NOT] NULL

      Table-level:
        <table>.<column> <op> <value>
        <table>.<column> IS [NOT] NULL

    Supported operators:
      =   !=   <>   >=   <=   >   <
      LIKE   NOT LIKE
      IN (...)   NOT IN (...)
      IS NULL    IS NOT NULL

    The value is taken verbatim — quote strings in the env-var value itself:
      QUERY_FILTER="product.category IN ('Electronics','Sports')"

    Returns {} when raw is empty (no filtering applied).
    """
    if not raw:
        return {}

    # Tokenise on commas that are NOT inside parentheses.
    # e.g.  "product.category IN ('a','b'),is_active=1"
    # should split into two tokens, not three.
    tokens: list[str] = []
    depth = 0
    cur: list[str] = []
    for ch in raw:
        if ch == "(":
            depth += 1
            cur.append(ch)
        elif ch == ")":
            depth -= 1
            cur.append(ch)
        elif ch == "," and depth == 0:
            tokens.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if cur:
        tokens.append("".join(cur).strip())

    # Ordered list of (regex, template) pairs.
    # Each regex must capture groups:
    #   group 1 — optional "table." prefix (may be empty)
    #   group 2 — column name
    #   group 3 — full predicate fragment (op + value, or IS [NOT] NULL)
    _OPS = (
        # IS NOT NULL / IS NULL  (no value — must come before IN to avoid conflict)
        (r"^([a-z0-9_]+\.)?([a-z0-9_]+)\s+(IS\s+NOT\s+NULL|IS\s+NULL)\s*$", None),
        # NOT IN (...)
        (r"^([a-z0-9_]+\.)?([a-z0-9_]+)\s+(NOT\s+IN\s*\(.*\))\s*$",          None),
        # IN (...)
        (r"^([a-z0-9_]+\.)?([a-z0-9_]+)\s+(IN\s*\(.*\))\s*$",                None),
        # NOT LIKE
        (r"^([a-z0-9_]+\.)?([a-z0-9_]+)\s+(NOT\s+LIKE\s+\S.*)\s*$",          None),
        # LIKE
        (r"^([a-z0-9_]+\.)?([a-z0-9_]+)\s+(LIKE\s+\S.*)\s*$",                None),
        # Comparison operators  !=  <>  >=  <=  >  <  =
        (r"^([a-z0-9_]+\.)?([a-z0-9_]+)\s*(!= |<> |>= |<= |> |< |= |!=|<>|>=|<=|>|<|=)(.*)\s*$", None),
    )

    # schema-level predicates accumulate here; table-level under their key
    schema_parts: list[str] = []
    table_parts:  dict[str, list[str]] = {}

    for token in tokens:
        token_ci = token.strip()   # keep original case for values/operators
        token_lc = token_ci.lower()

        matched = False
        for pattern, _ in _OPS:
            m = _re.match(pattern, token_lc, _re.IGNORECASE)
            if m:
                matched = True
                table_prefix = m.group(1) or ""           # e.g. "product." or ""
                col_name     = m.group(2)                 # lower-cased column
                # Reconstruct predicate from original token to preserve value case.
                # We strip only the optional "table." prefix from the front.
                prefix_len   = len(table_prefix)
                predicate    = token_ci[prefix_len:].strip()   # "column op value"
                tbl          = table_prefix.rstrip(".").lower() if table_prefix else ""

                if tbl:
                    table_parts.setdefault(tbl, []).append(predicate)
                else:
                    schema_parts.append(predicate)
                break

        if not matched:
            raise ValueError(
                f"QUERY_FILTER token {token!r} could not be parsed.\n"
                f"  Expected: [table.]column <op> value  OR  [table.]column IS [NOT] NULL\n"
                f"  Supported operators: =  !=  <>  >=  <=  >  <  LIKE  NOT LIKE  "
                f"IN (...)  NOT IN (...)  IS NULL  IS NOT NULL"
            )

    # Build the output dict.
    # schema-level predicates are combined with AND into key "".
    result: dict[str, str] = {}
    if schema_parts:
        result[""] = " AND ".join(schema_parts)

    for tbl, parts in table_parts.items():
        result[tbl] = " AND ".join(parts)

    if result:
        logger.info(
            "[query-filter] Parsed QUERY_FILTER → %s",
            {k or "<schema-level>": v for k, v in result.items()},
        )
    return result


def _get_where_clause(table: str) -> str:
    """
    Return the WHERE clause fragment (without the word WHERE) for *table*.

    Merges the schema-level predicate (key "") and the table-level predicate
    (key table_name) from the global QUERY_FILTERS dict with AND.
    Returns "" when no filter applies to this table.
    """
    parts: list[str] = []
    schema_pred = QUERY_FILTERS.get("", "")
    table_pred  = QUERY_FILTERS.get(table.lower(), "")
    if schema_pred:
        parts.append(f"({schema_pred})")
    if table_pred:
        parts.append(f"({table_pred})")
    return " AND ".join(parts)


# Resolve QUERY_FILTERS after connectors are defined (parser uses logger).
QUERY_FILTERS: dict[str, str] = _parse_query_filters(_RAW_QUERY_FILTER)


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
    spark:      SparkSession,
    builder:    IcebergTableBuilder,
    connector:  "_SourceConnector",
    conn_opts:  dict,
    s3_bucket:  str,
    table:      str,
    size_gb:    float,
    results:    dict,
    lock:       threading.Lock,
    pg_creds:   dict,
    pk_cols:    list[str] | None = None,
    write_mode: str | None       = None,
) -> None:
    """
    Copy one table from the source database → Iceberg (called inside a thread).
    Source-agnostic: all source-specific I/O goes through *connector*.

    Write modes (WRITE_MODE / --write-mode)
    ----------------------------------------
    standard (default, SCD Type 0)
      Incremental run: MERGE INTO Iceberg by PK.
        - Rows present in the source window → UPDATE (upsert).
        - Rows that vanished from the source window → hard DELETE from Iceberg.
      Full run: plain append (no prior Iceberg rows exist).

    soft_delete
      Same MERGE upsert pass as standard.
      Vanished rows are NOT physically deleted — instead starpump sets
        is_deleted = true
        deleted_at = current_timestamp()
      on any Iceberg row whose PK is no longer in the source window.
      Adds is_deleted BOOLEAN + deleted_at TIMESTAMP columns to Iceberg DDL
      automatically on first run (via mergeSchema=true).

    history (SCD Type 2 / audit log)
      Every source row in the window is appended as a new Iceberg row tagged with
        _change_type STRING  (INSERT / UPDATE / DELETE — not set in batch mode;
                              always INSERT here since we see only live rows)
        _change_ts   TIMESTAMP
      Rows are never updated or deleted in Iceberg; the full history accumulates.

    Primary key resolution (pk_cols)
    ---------------------------------
    pk_cols is resolved before calling _copy_table by the caller (incremental
    worker).  If None, _resolve_primary_keys() is called here as a fallback.
    PK columns are used for:
      1. The ON clause of MERGE INTO.
      2. ORDER BY on source reads so LIMIT/OFFSET is index-friendly.

    Watermark flow
    --------------
    1. Capture source server-side timestamp immediately before the first batch
       SELECT — this is the CDC sync point (extraction_ts).
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

    # Resolve write_mode / pk_cols for this call (caller may override globals)
    _write_mode = (write_mode or WRITE_MODE).lower()
    # pk_cols resolved after schema discovery below — placeholder here
    _pk_cols: list[str] = pk_cols if pk_cols is not None else []

    # ── Write with snapshot-conflict retry ─────────────────────────────────
    # Defined at function scope (not inside the `else: not DRY_RUN` branch) so
    # it is always bound before the delete-detection pass can reference it.
    # Fixes: UnboundLocalError — cannot access local variable
    #        '_iceberg_write_with_retry' where it is not associated with a value.
    def _iceberg_write_with_retry(write_fn, label: str) -> None:
        """Execute write_fn(), retrying on snapshot-conflict errors."""
        attempt = 0
        while True:
            try:
                write_fn()
                break
            except Exception as _w_err:  # noqa: BLE001
                attempt += 1
                err_str = str(_w_err)
                is_retryable = any(k in err_str for k in (
                    "IllegalStateException",
                    "CommitFailedException",
                    "ValidationException",
                    "Cannot commit",
                    "concurrent",
                    "conflict",
                ))
                if attempt > WRITE_MAX_RETRIES or not is_retryable:
                    raise
                sleep_s = WRITE_RETRY_SLEEP_S * attempt
                logger.warning(
                    "[%s] %s conflict (attempt %d/%d): %s "
                    "— retrying in %ds …",
                    table, label, attempt, WRITE_MAX_RETRIES,
                    err_str[:120], sleep_s,
                )
                time.sleep(sleep_s)

    try:
        logger.info(
            "[%s] START: %.1f GB | discovering schema … (write_mode=%s)",
            table, size_gb, _write_mode,
        )
        raw_schema = connector.table_schema(spark, conn_opts, table)

        # Map source types → Spark/Iceberg types via the connector's own mapper.
        # Each connector defines exactly how its native types translate.
        iceberg_schema = connector.map_schema(raw_schema)

        # Resolve PK now that schema is known
        if not _pk_cols:
            _pk_cols = _resolve_primary_keys(table, iceberg_schema, connector, spark, conn_opts)
        logger.info(
            "[%s] PK cols: %s  (source=%s schema=%s)",
            table, _pk_cols or "(none — append-only)", SOURCE, SCHEMAS,
        )

        partition_spec = _auto_partition_spec(iceberg_schema)

        # S3 location is derived from the connector's s3_prefix — no per-source
        # branching needed anywhere in the pipeline.
        s3_location = f"s3://{s3_bucket}/{connector.s3_prefix}/{ICEBERG_NAMESPACE}/{table}"

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
            # ── Resume detection (offset-capable sources only) ─────────────
            # Snowflake supports reliable LIMIT/OFFSET pagination so a partial
            # copy can be resumed from the last committed Iceberg row count.
            # Databricks JDBC does not (no guaranteed order without ORDER BY),
            # so it always reads from offset 0 regardless of prior runs.
            #
            # IMPORTANT: Resume-at-offset only makes sense for FULL copies.
            # In incremental mode a watermark WHERE clause is already injected
            # into QUERY_FILTERS, so the paginated read is over the *filtered
            # window* — not the full table.  spark.table(fqn).count() returns
            # the total Iceberg row count across ALL prior runs, which is
            # meaningless as an offset into the current (much smaller) filtered
            # window.  Using it as such causes the batch loop to skip the
            # entire window (offset > window size → first batch is empty →
            # 0 new rows written) AND the watermark to be written back with
            # the same old extraction_ts, so no rows ever advance.
            # Fix: skip resume-at-offset whenever a watermark filter is active.
            _has_wm_filter = bool(_get_where_clause(table))
            already_written = 0
            if connector.supports_offset_resume and not _has_wm_filter:
                try:
                    already_written = spark.table(fqn).count()
                except Exception:
                    already_written = 0

            if already_written > 0:
                # Partial resume: recover the original extraction_ts from
                # the pipeline DB so the CDC sync-point stays consistent.
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
                        "extraction_ts=%s from pipeline DB, starting at offset=%d.",
                        table, already_written, sf_extraction_ts, already_written,
                    )
                else:
                    # No prior watermark (first run crashed before writing one).
                    sf_extraction_ts = connector.capture_ts(spark, conn_opts)
                    logger.info(
                        "[%s] RESUME: %d rows in Iceberg but no prior watermark — "
                        "fresh extraction_ts=%s, starting at offset=%d.",
                        table, already_written, sf_extraction_ts, already_written,
                    )
                    try:
                        pg_upsert_watermark(
                            pg                = pg_creds,
                            source_db         = DATABASE,
                            source_schema     = SCHEMAS,
                            table_name        = table,
                            sf_extraction_ts  = sf_extraction_ts,
                            rows_copied       = already_written,
                            iceberg_namespace = ICEBERG_NAMESPACE,
                        )
                        logger.info("[%s] Early watermark written to pipeline DB (resume fallback).", table)
                    except Exception as _pg_err:
                        logger.warning(
                            "[%s] Could not write early watermark to pipeline DB: %s",
                            table, _pg_err,
                        )
            else:
                # Incremental run (watermark filter active) or fresh full copy:
                # always capture a fresh CDC sync-point from the source NOW,
                # before the first batch.  This becomes the new watermark that
                # gets written at the end of the run, advancing the window.
                sf_extraction_ts = connector.capture_ts(spark, conn_opts)
                logger.info("[%s] extraction_ts=%s (CDC sync point)", table, sf_extraction_ts)

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
            iceberg_cols  = [f.name for f in iceberg_schema.fields]
            offset        = already_written
            rows_total    = already_written
            where_clause  = _get_where_clause(table)
            if where_clause:
                logger.info("[%s] QUERY_FILTER active — WHERE %s", table, where_clause)

            # PK-ordered reads: replace generic "ORDER BY 1" used by the connectors
            # with "ORDER BY pk1, pk2 …" so LIMIT/OFFSET walks the PK index.
            # Passed to the connector via a connector-level ORDER BY override stored
            # in conn_opts temporarily; connectors that don't use it ignore the key.
            # We inject it only when the source supports offset-resume (SQL sources).
            _pk_order = (
                _build_pk_order_clause(_pk_cols, source_key=SOURCE)
                if connector.supports_offset_resume else ""
            )
            if _pk_order:
                logger.info("[%s] PK-ordered reads: %s", table, _pk_order)

            while True:
                # Shrink batch to never write more than MAX_ROWS new rows total.
                effective_batch = BATCH_SIZE
                if MAX_ROWS > 0:
                    new_so_far = rows_total - already_written
                    remaining  = MAX_ROWS - new_so_far
                    if remaining <= 0:
                        break
                    effective_batch = min(BATCH_SIZE, remaining)

                # ── Read with adaptive batch-halving retry ─────────────────────
                # Fixes: SparkException: Job aborted on large tables (>1 GB).
                # On each failure the batch size is halved so the executor memory
                # pressure is reduced — the same data is fetched in smaller chunks.
                read_attempt  = 0
                read_batch_sz = effective_batch
                batch         = None
                while read_attempt <= READ_MAX_RETRIES:
                    try:
                        batch = connector.read_batch(
                            spark, conn_opts, table, offset, read_batch_sz,
                            where_clause=where_clause,
                            order_clause=_pk_order,
                        )
                        # Cache before count() so the source cursor is opened once.
                        # Without this, count() + writeTo().append() each trigger a
                        # full connector read — doubling network I/O per batch.
                        batch.cache()
                        _ = batch.count()   # force materialisation; raises on failure
                        break
                    except Exception as read_err:  # noqa: BLE001
                        if batch is not None:
                            try:
                                batch.unpersist()
                            except Exception:
                                pass
                            batch = None
                        read_attempt += 1
                        if read_attempt > READ_MAX_RETRIES:
                            raise
                        new_sz = max(read_batch_sz // 2, 1000)
                        logger.warning(
                            "[%s] read_batch failed (attempt %d/%d): %s — "
                            "halving batch size %d → %d and retrying …",
                            table, read_attempt, READ_MAX_RETRIES,
                            read_err, read_batch_sz, new_sz,
                        )
                        read_batch_sz = new_sz
                        time.sleep(2)

                n = batch.count()
                if n == 0:
                    batch.unpersist()
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
                # snap_timestamp: wall-clock at write time, same for all rows in batch.
                final = (
                    aligned
                    .withColumn("snap_id",        monotonically_increasing_id().cast(LongType()))
                    .withColumn("snap_timestamp",  current_timestamp())
                )

                # ── history mode: tag with _change_type / _change_ts then INSERT ──
                if _write_mode == "history":
                    final = (
                        final
                        .withColumn("_change_type", lit("INSERT"))
                        .withColumn("_change_ts",   current_timestamp())
                    )

                if _write_mode == "history" or not _pk_cols:
                    # history mode OR no PK available → plain append, no merge
                    _iceberg_write_with_retry(
                        lambda: final.writeTo(fqn).option("mergeSchema", "true").append(),
                        "writeTo().append()",
                    )
                else:
                    # standard / soft_delete — MERGE upsert pass ─────────────
                    # Materialise `final` before registering the temp view.
                    # `final` contains monotonically_increasing_id() and
                    # current_timestamp() — both non-deterministic.  Iceberg's
                    # MERGE planner requires the source side to be deterministic
                    # (INVALID_NON_DETERMINISTIC_EXPRESSIONS).  Caching forces
                    # Spark to evaluate those expressions once into concrete
                    # values; the temp view then references only stored data.
                    final.cache()
                    final.count()   # force materialisation
                    _tmp = f"_starpump_src_{table.replace('.','_')}"
                    final.createOrReplaceTempView(_tmp)
                    merge_sql = _build_merge_sql(
                        fqn        = fqn,
                        pk_cols    = _pk_cols,
                        data_cols  = [],    # UPDATE SET * — data_cols unused for upsert
                        write_mode = _write_mode,
                        tmp_view   = _tmp,
                    )
                    _iceberg_write_with_retry(
                        lambda: spark.sql(merge_sql),
                        "MERGE INTO (upsert)",
                    )
                    spark.catalog.dropTempView(_tmp)
                    final.unpersist()

                batch.unpersist()

                rows_total += n
                offset     += n
                logger.info(
                    "[%s] batch offset=%d rows=%d total=%d",
                    table, offset - n, n, rows_total,
                )
                if MAX_ROWS > 0 and (rows_total - already_written) >= MAX_ROWS:
                    break   # MAX_ROWS new-row cap reached
                if n < effective_batch:
                    break   # last batch (partial batch means source is exhausted)
                if not connector.supports_offset_resume:
                    break   # MongoDB: no server-side offset — one pass only

            logger.info("[%s] DONE — %d rows written (total incl. prior runs).", table, rows_total)

            # ── Delete-detection pass (standard / soft_delete, incremental only) ─
            # After the MERGE upsert loop we have pushed all live rows from the
            # source window (updated_at >= last_ts) into Iceberg.  Any Iceberg row
            # whose PK is NOT in that live window was deleted from the source since
            # the last run and must be actioned now.
            #
            # Strategy:
            #   1. Re-read the full source window (same WHERE clause) and collect
            #      PKs into a temp view called _starpump_live_pks_<table>.
            #   2. MERGE INTO Iceberg: any Iceberg row whose PK is absent from
            #      the live-PK view is treated as deleted.
            #
            # This pass runs only when:
            #   • write_mode is standard or soft_delete  (history never deletes)
            #   • we have at least one PK column
            #   • a watermark was used (where_clause is not empty, meaning this
            #     is an incremental run — not a first-time full copy)
            #   • source supports SQL (not MongoDB — no reliable full-scan PK extract)
            _wc_for_del = _get_where_clause(table)   # current effective where clause
            _should_del_pass = (
                _write_mode in ("standard", "soft_delete")
                and _pk_cols
                and _wc_for_del                        # only when a watermark is active
                and connector.supports_offset_resume   # SQL sources only
                and not DRY_RUN
            )
            if _should_del_pass:
                logger.info(
                    "[%s] Delete-detection pass (write_mode=%s) — "
                    "collecting live PKs from source window …",
                    table, _write_mode,
                )
                try:
                    # Read only the PK columns from the source for the whole window.
                    # This is a single full-window scan — not paginated.
                    pk_select = ", ".join(f'"{c.upper() if SOURCE=="oracle" else c}"' for c in _pk_cols)
                    del_where = f" WHERE {_wc_for_del}"
                    if SOURCE == "postgres":
                        pk_query = f'SELECT {pk_select} FROM "{SCHEMAS}"."{table}"{del_where}'
                    elif SOURCE == "oracle":
                        pk_query = f'SELECT {pk_select} FROM "{SCHEMAS.upper()}"."{table.upper()}"{del_where}'
                    elif SOURCE in ("snowflake",):
                        pk_select_sf = ", ".join(f'"{c.upper()}"' for c in _pk_cols)
                        pk_query = f'SELECT {pk_select_sf} FROM "{table.upper()}"{del_where}'
                    else:
                        pk_query = f'SELECT {pk_select} FROM `{DATABASE}`.`{SCHEMAS}`.`{table}`{del_where}'

                    live_pk_df = (
                        spark.read.format(connector.spark_format)
                        .options(**conn_opts)
                        .option("query" if connector.spark_format != "jdbc"
                                else "query", pk_query)
                        .load()
                    )
                    live_pk_df.cache()
                    live_pk_count = live_pk_df.count()
                    logger.info(
                        "[%s] Live PK count in source window: %d",
                        table, live_pk_count,
                    )

                    _live_tmp = f"_starpump_live_pks_{table.replace('.','_')}"
                    live_pk_df.createOrReplaceTempView(_live_tmp)

                    on_clause = " AND ".join(
                        f"t.`{c}` = s.`{c}`" for c in _pk_cols
                    )

                    if _write_mode == "standard":
                        # Hard DELETE: remove Iceberg rows not in live-PK set
                        del_sql = f"""
                            MERGE INTO {fqn} t
                            USING (
                                SELECT t2.*
                                FROM {fqn} t2
                                LEFT ANTI JOIN {_live_tmp} s
                                ON {on_clause.replace('t.`', 't2.`')}
                                WHERE t2.`is_deleted` IS NULL OR t2.`is_deleted` = false
                            ) gone
                            ON {on_clause.replace('s.`', 'gone.`')}
                            WHEN MATCHED THEN DELETE
                        """.strip()
                    else:
                        # soft_delete: mark vanished rows is_deleted=true
                        del_sql = _build_merge_sql(
                            fqn        = fqn,
                            pk_cols    = _pk_cols,
                            data_cols  = [],   # signals soft-delete pass
                            write_mode = "soft_delete",
                            tmp_view   = (
                                f"(SELECT t2.* FROM {fqn} t2 "
                                f"LEFT ANTI JOIN {_live_tmp} s "
                                f"ON {on_clause.replace('t.`','t2.`').replace('s.`','s.`')} "
                                f"WHERE t2.`is_deleted` IS NULL OR t2.`is_deleted` = false)"
                            ),
                        )

                    def _del_fn() -> None:
                        spark.sql(del_sql)

                    _iceberg_write_with_retry(_del_fn, "MERGE INTO (delete pass)")
                    live_pk_df.unpersist()
                    spark.catalog.dropTempView(_live_tmp)
                    logger.info("[%s] Delete-detection pass complete.", table)

                except Exception as _del_err:
                    logger.warning(
                        "[%s] Delete-detection pass failed (non-fatal): %s",
                        table, _del_err,
                    )

            # ── Stamp sf_extraction_ts onto the Iceberg table property ─────
            spark.sql(
                f"ALTER TABLE {fqn} SET TBLPROPERTIES "
                f"('pipeline.sf_extraction_ts' = '{sf_extraction_ts}')"
            )

            # ── Dual-write watermark: Iceberg control table ────────────────
            # Must hold the shared lock: _pipeline_watermarks is a single
            # Iceberg table written by all 8 threads.  Concurrent MERGE INTO
            # operations on the same table via the same SparkSession cause:
            #   ValidationException  — conflicting files under serializable isolation
            #   IllegalStateException — snapshot modified between scan and commit
            # Serialising through the lock makes each MERGE atomic w.r.t. peers.
            with lock:
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


# ═══════════════════════════════════════════════════════════════════════════════
# ── Incremental load helpers ──────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def _pg_read_watermark(pg: dict, source_db: str, source_schema: str, table_name: str) -> str | None:
    """
    Read the stored sf_extraction_ts watermark for a table from the pipeline DB.
    Returns None if no watermark exists (table never been copied incrementally).
    """
    sql = (
        "SELECT sf_extraction_ts FROM pipeline_watermarks "
        "WHERE source_db = %s AND source_schema = %s AND table_name = %s"
    )
    with _pg_connect(pg) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source_db, source_schema, table_name))
            row = cur.fetchone()
    return row[0] if row and row[0] else None


def _resolve_watermark_col(schema: StructType, override: str | None) -> str | None:
    """
    Return the best timestamp column to use as the incremental watermark.

    Priority:
      1. Explicit WATERMARK_COL / --watermark-col override
      2. updated_at (present in postgres.customers, postgres.orders)
      3. created_at (fallback for append-only tables like product_reviews)
      4. None → table has no usable watermark column; skip incremental for it
    """
    col_names = {f.name.lower() for f in schema.fields}
    if override and override.lower() in col_names:
        return override.lower()
    if "updated_at" in col_names:
        return "updated_at"
    if "created_at" in col_names:
        return "created_at"
    return None


def _resolve_primary_keys(
    table:     str,
    schema:    StructType,
    connector: "_SourceConnector | None" = None,
    spark:     "SparkSession | None"     = None,
    opts:      "dict | None"             = None,
) -> list[str]:
    """
    Return the primary key column list for *table*, in priority order:

    1. Global PRIMARY_KEYS env/CLI override (--pk-cols / PK_COLS env).
    2. connector.primary_keys(spark, opts, table) — asks the source directly:
         PostgreSQL / Oracle  → java.sql.DatabaseMetaData.getPrimaryKeys() via
                                py4j; no SQL, no schema-name assumptions, works
                                for any table name and any PK arity.
         Databricks           → same DatabaseMetaData path (PKs informational
                                only in Unity Catalog; usually returns []).
         Snowflake            → SHOW PRIMARY KEYS IN TABLE — native metadata
                                command, no data scan, no schema filter.
         MongoDB              → always ["_id"] — enforced by the storage engine.
       Composite PKs are returned in KEY_SEQ / key_sequence order.
    3. Name-heuristic fallback (only when step 2 returns nothing):
         a. column named 'id'
         b. column named '<table>_id'   e.g. 'order_id' for table 'orders'
         c. first column in the schema  (last resort — warning logged)
    4. Empty list when schema has no columns (should never happen).

    The returned names are lower-cased to match Iceberg column names.
    Used for:
      - MERGE INTO … ON t.pk = s.pk   (standard / soft_delete modes)
      - ORDER BY pk on source reads   (index-friendly LIMIT/OFFSET pagination)
    """
    # ── Priority 1: explicit CLI / env override ────────────────────────────────
    if PRIMARY_KEYS:
        return PRIMARY_KEYS

    # ── Priority 2: ask the source catalog ────────────────────────────────────
    if connector is not None and spark is not None and opts is not None:
        try:
            catalog_pks = connector.primary_keys(spark, opts, table)
            if catalog_pks:
                logger.info(
                    "[%s] PK cols resolved from source catalog: %s  (source=%s schema=%s table=%s)",
                    table, catalog_pks, SOURCE, SCHEMAS, table,
                )
                return catalog_pks
        except Exception as exc:
            logger.debug(
                "[%s] connector.primary_keys() raised unexpectedly (%s) — "
                "falling back to name heuristic.",
                table, exc,
            )

    # ── Priority 3: name heuristic (fallback) ─────────────────────────────────
    col_names = [f.name.lower() for f in schema.fields]
    col_set   = set(col_names)

    if "id" in col_set:
        return ["id"]
    table_id = f"{table.lower()}_id"
    if table_id in col_set:
        return [table_id]
    if col_names:
        logger.warning(
            "[%s] PK not found in source catalog (source=%s schema=%s) — "
            "using first column '%s' as PK. "
            "Override with --pk-cols or PK_COLS env var.",
            table, SOURCE, SCHEMAS, col_names[0],
        )
        return [col_names[0]]
    return []


def _incremental_where_clause(wm_col: str, last_ts: str | None) -> str:
    """
    Build a WHERE clause fragment for incremental extraction.

    When last_ts is provided:  wm_col >= '<last_ts>'
    When last_ts is None (first incremental run): copy all rows (empty clause).

    '>=' (inclusive) is intentional — it re-reads rows at the exact watermark
    boundary so that any row whose updated_at equals the last watermark is
    picked up again.  Without this, a row updated in the same microsecond as
    the previous run's capture_ts could be skipped forever.

    The MERGE INTO in standard/soft_delete modes is idempotent for the
    boundary rows (they simply overwrite themselves), so the slight re-read
    of the boundary second carries no correctness risk.

    IMPORTANT — deleted rows:
    A WHERE updated_at >= last_ts clause only fetches rows still present in
    the source.  Rows hard-deleted from the source vanish and will NOT appear
    in this query result.  starpump handles this per WRITE_MODE:
      standard    — after the MERGE upsert pass, a second DELETE pass removes
                    Iceberg rows whose PK is no longer in the source window
                    (rows where updated_at >= last_ts are the "live window").
      soft_delete — same second pass, but marks rows with is_deleted=true
                    instead of physically deleting them.
      history     — no delete handling needed; all rows are inserted as-is.
    """
    if not last_ts:
        return ""  # first run — full copy
    return f"{wm_col} >= '{last_ts}'"


def _build_pk_order_clause(pk_cols: list[str], source_key: str = "") -> str:
    """
    Build an ORDER BY fragment from a list of PK column names.

    Used to make LIMIT/OFFSET pagination index-friendly on sources that have
    a B-tree index on the PK (PostgreSQL, Oracle, Snowflake, Databricks).
    The ORDER BY guarantees a stable cursor: each page starts exactly where
    the previous one ended, so OFFSET N skips the right rows even if new rows
    are inserted concurrently.

    Returns empty string for MongoDB (no SQL ORDER BY) or when pk_cols is empty.
    """
    if not pk_cols or source_key == "mongodb":
        return ""
    cols = ", ".join(f'"{c}"' for c in pk_cols)
    return f"ORDER BY {cols}"


def _build_merge_sql(
    fqn:        str,
    pk_cols:    list[str],
    data_cols:  list[str],
    write_mode: str,
    tmp_view:   str = "_starpump_src",
) -> str:
    """
    Generate an Iceberg MERGE INTO statement for standard or soft_delete modes.

    Parameters
    ----------
    fqn         Fully-qualified Iceberg table name (backtick-quoted).
    pk_cols     Primary key column(s) — used in the ON clause.
    data_cols   All non-PK, non-snap data columns (used in UPDATE SET).
    write_mode  'standard' or 'soft_delete'.
    tmp_view    Name of the Spark temporary view holding the source batch.

    Generated SQL structure (standard)
    ------------------------------------
        MERGE INTO <fqn> t
        USING <tmp_view> s
        ON  t.pk1 = s.pk1 AND t.pk2 = s.pk2
        WHEN MATCHED THEN UPDATE SET t.col = s.col, …, t.snap_timestamp = s.snap_timestamp
        WHEN NOT MATCHED THEN INSERT *

    Generated SQL structure (soft_delete — called for the "delete" pass)
    -----------------------------------------------------------------------
        MERGE INTO <fqn> t
        USING <tmp_view> s          ← source = PKs of deleted rows
        ON  t.pk1 = s.pk1
        WHEN MATCHED AND t.is_deleted IS DISTINCT FROM true
        THEN UPDATE SET t.is_deleted = true, t.deleted_at = current_timestamp()

    Note: the "delete pass" SQL is returned only when write_mode='soft_delete'
    AND the caller passes data_cols=[] to signal this is the deletion sub-pass.
    The main upsert pass is always the same UPDATE SET * / INSERT * form.
    """
    on_clause = " AND ".join(f"t.`{c}` = s.`{c}`" for c in pk_cols)

    if write_mode == "soft_delete" and not data_cols:
        # ── Soft-delete pass: mark vanished rows as deleted ──────────────────
        return f"""
            MERGE INTO {fqn} t
            USING {tmp_view} s
            ON {on_clause}
            WHEN MATCHED AND (t.`is_deleted` IS NULL OR t.`is_deleted` = false)
            THEN UPDATE SET
                t.`is_deleted`          = true,
                t.`{DELETED_AT_COL}`    = current_timestamp(),
                t.`snap_timestamp`      = current_timestamp()
        """.strip()

    # ── Upsert pass: UPDATE existing rows, INSERT new rows ───────────────────
    # Always use UPDATE SET * — Iceberg resolves column alignment by name.
    # This handles schema evolution (new source columns added) automatically.
    return f"""
        MERGE INTO {fqn} t
        USING {tmp_view} s
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """.strip()


def _detect_ddl_drift(
    spark:     SparkSession,
    connector: "_SourceConnector",
    conn_opts: dict,
    table:     str,
) -> list[dict]:
    """
    Compare the source table schema against the live Iceberg table schema.

    Returns a list of change dicts (same format as _diff_avro_schemas):
      {"op": "add",    "name": "col", "spark_type": DataType}
      {"op": "remove", "name": "col"}
      {"op": "modify", "name": "col", "spark_type": DataType}

    Empty list = no drift detected.
    Only called when DDL_DRIFT_DETECT=1 (default) and the target Iceberg table exists.
    """
    fqn = f"`{ICEBERG_CATALOG}`.`{ICEBERG_NAMESPACE}`.`{table}`"

    try:
        ice_schema: StructType = spark.table(fqn).schema
    except Exception:
        # Table doesn't exist yet — no drift to detect
        return []

    try:
        src_schema: StructType = connector.table_schema(spark, conn_opts, table)
        src_schema = connector.map_schema(src_schema)
    except Exception as exc:
        logger.warning("[%s] DDL drift: could not read source schema: %s", table, exc)
        return []

    # Exclude platform-injected snap columns from drift detection
    _snap_cols = {"snap_id", "snap_timestamp"}
    src_cols = {f.name.lower(): f.dataType for f in src_schema.fields if f.name.lower() not in _snap_cols}
    ice_cols  = {f.name.lower(): f.dataType for f in ice_schema.fields  if f.name.lower() not in _snap_cols}

    changes: list[dict] = []
    for name, dtype in src_cols.items():
        if name not in ice_cols:
            changes.append({"op": "add",    "name": name, "spark_type": dtype})
        elif str(dtype) != str(ice_cols[name]):
            changes.append({"op": "modify", "name": name, "spark_type": dtype})
    for name in ice_cols:
        if name not in src_cols:
            changes.append({"op": "remove", "name": name})

    if changes:
        logger.info(
            "[%s] DDL drift detected — %d change(s): %s",
            table, len(changes),
            [(c["op"], c["name"]) for c in changes],
        )
    else:
        logger.debug("[%s] No DDL drift.", table)
    return changes


def _apply_ddl_drift(
    spark:   SparkSession,
    table:   str,
    changes: list[dict],
) -> None:
    """
    Apply ALTER TABLE statements to the Iceberg table for each detected change.

    Supported operations:
      add    → ALTER TABLE … ADD COLUMN col type
      remove → ALTER TABLE … DROP COLUMN col
      modify → ALTER TABLE … ALTER COLUMN col TYPE type

    Skipped in DRY_RUN mode (logs intent only).
    """
    if not changes:
        return

    fqn = f"`{ICEBERG_CATALOG}`.`{ICEBERG_NAMESPACE}`.`{table}`"

    for chg in changes:
        op  = chg["op"]
        col = chg["name"]

        if op == "add":
            ice_type = chg["spark_type"].simpleString()
            ddl = f"ALTER TABLE {fqn} ADD COLUMN `{col}` {ice_type}"
        elif op == "remove":
            ddl = f"ALTER TABLE {fqn} DROP COLUMN `{col}`"
        elif op == "modify":
            ice_type = chg["spark_type"].simpleString()
            ddl = f"ALTER TABLE {fqn} ALTER COLUMN `{col}` TYPE {ice_type}"
        else:
            continue

        logger.info("[%s] DDL drift ALTER: %s", table, ddl)
        if DRY_RUN:
            logger.info("[%s] DRY_RUN — skipping ALTER TABLE.", table)
        else:
            try:
                spark.sql(ddl)
                logger.info("[%s] DDL drift applied: %s %s", table, op, col)
            except Exception as exc:
                logger.warning("[%s] DDL drift ALTER failed (non-fatal): %s", table, exc)


# ═══════════════════════════════════════════════════════════════════════════════
# ── Custom SQL mode ───────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def _run_custom_sql(
    spark:      SparkSession,
    builder:    "IcebergTableBuilder",
    connector:  "_SourceConnector",
    conn_opts:  dict,
    s3_bucket:  str,
    sql_query:  str,
    target_tbl: str,
    pg_creds:   dict,
    lock:       threading.Lock,
) -> dict:
    """
    Execute a user-supplied SQL query against the source database and write
    results to an Iceberg table.

    Supports multi-table JOINs.  The query is passed verbatim to the
    connector's JDBC/native read path via the 'query' option.

    For MongoDB (which does not support SQL JOINs), the sql_query is
    treated as a raw aggregation pipeline JSON string if it starts with '['.

    Returns a result dict compatible with the main copy results format.
    """
    logger.info("[custom-sql] Target table: %s | Query: %.200s …", target_tbl, sql_query)

    try:
        # Read via connector's underlying format with the custom query.
        # PostgreSQL, Oracle, Databricks: jdbc with 'query' option.
        # MongoDB: aggregation pipeline via the mongodb format.
        if connector.spark_format == "mongodb":
            # Treat sql_query as a MongoDB aggregation pipeline
            df: DataFrame = (
                spark.read.format("mongodb")
                .options(**conn_opts)
                .option("spark.mongodb.read.aggregation.pipeline", sql_query)
                .load()
            )
        else:
            # JDBC sources: wrap in subquery for Spark JDBC compatibility
            # (Spark JDBC requires a subquery alias when 'dbtable' is a subselect)
            subquery = f"({sql_query}) custom_sql_result"
            df = (
                spark.read.format("jdbc")
                .options(**conn_opts)
                .option("dbtable", subquery)
                .load()
            )

        raw_schema  = df.schema
        mapped      = connector.map_schema(raw_schema)

        # Auto-detect partition key: first column in result
        pk_col = raw_schema.fields[0].name if raw_schema.fields else "snap_id"
        partition_spec = [
            IcebergTableBuilder.hours("snap_timestamp"),
            IcebergTableBuilder.bucket(pk_col, 16),
        ]

        s3_location = (
            f"s3://{s3_bucket}/{connector.s3_prefix}/{ICEBERG_NAMESPACE}/{target_tbl}"
        )
        fqn = builder.create_table(
            catalog        = ICEBERG_CATALOG,
            namespace      = ICEBERG_NAMESPACE,
            table          = target_tbl,
            schema         = mapped,
            partition_spec = partition_spec,
            location       = s3_location,
        )

        if DRY_RUN:
            logger.info("[custom-sql] DRY_RUN — skipping data copy.")
            rows_written = 0
        else:
            df.cache()
            rows_written = builder.write_append(df, ICEBERG_CATALOG, ICEBERG_NAMESPACE, target_tbl)
            df.unpersist()

            # Write a watermark entry for the custom-sql result
            sf_ts = connector.capture_ts(spark, conn_opts)
            with lock:
                write_watermark_iceberg(
                    spark             = spark,
                    catalog           = ICEBERG_CATALOG,
                    namespace         = ICEBERG_NAMESPACE,
                    source_db         = DATABASE,
                    source_schema     = SCHEMAS,
                    table_name        = target_tbl,
                    sf_extraction_ts  = sf_ts,
                    rows_copied       = rows_written,
                )
            pg_upsert_watermark(
                pg                = pg_creds,
                source_db         = DATABASE,
                source_schema     = SCHEMAS,
                table_name        = target_tbl,
                sf_extraction_ts  = sf_ts,
                rows_copied       = rows_written,
                iceberg_namespace = ICEBERG_NAMESPACE,
            )

        logger.info("[custom-sql] Done — %d rows written to %s.", rows_written, fqn)
        return {"status": "success", "rows_written": rows_written, "size_gb": 0.0,
                "sf_extraction_ts": None, "error": None}

    except Exception as exc:
        logger.error("[custom-sql] FAILED: %s", exc, exc_info=True)
        return {"status": "error", "rows_written": 0, "size_gb": 0.0,
                "sf_extraction_ts": None, "error": str(exc)}


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    os.environ["USER"] = USER   # ensure env is set for submodules

    run_id = str(uuid.uuid4())

    logger.info(
        "=== starpump %s | run_id=%s user=%s db=%s schema=%s catalog=%s threads=%d mode=%s ===",
        SOURCE, run_id, USER, DATABASE, SCHEMAS, ICEBERG_CATALOG, MAX_THREADS, MODE,
    )
    logger.info(
        "=== Filters: include=%s  exclude=%s  max_size=%.1f GB ===",
        ", ".join(INCLUDE_TABLES) if INCLUDE_TABLES else "all",
        ", ".join(sorted(EXCLUDE_TABLES)) if EXCLUDE_TABLES else "none",
        MAX_TABLE_SIZE_GB,
    )

    # Validate custom_sql mode requirements early
    if MODE == "custom_sql":
        if not CUSTOM_SQL:
            print(
                "ERROR: custom_sql mode requires --custom-sql or CUSTOM_SQL env var.",
                file=sys.stderr,
            )
            sys.exit(1)
        if not TARGET_TABLE:
            print(
                "ERROR: custom_sql mode requires --target-table or TARGET_TABLE env var.",
                file=sys.stderr,
            )
            sys.exit(1)

    # ── 1. Credentials from OpenBao ───────────────────────────────────────────
    bao  = BaoSparkInit()
    pg   = bao.pipeline_db_creds()

    # S3 bucket resolution (priority: CLI override → source secret → platform/s3):
    # Each source may write to a different bucket (e.g. snowflake → xdatatoiceberg1,
    # databricks → stardata-databricks).  The connector's own secret is checked first
    # so that no extra env var is needed when running against any registered source.
    if S3_BUCKET_OVERRIDE:
        s3_bucket = S3_BUCKET_OVERRIDE
    else:
        try:
            src_creds = bao._read_secret(f"secret/data/platform/{SOURCE}")
            s3_bucket = src_creds.get("s3_bucket") or bao.s3_creds()["bucket"]
        except Exception:
            s3_bucket = bao.s3_creds()["bucket"]

    logger.info("S3 bucket: %s (source=%s)", s3_bucket, SOURCE)

    # Resolve the connector for the requested source.
    connector  = _CONNECTORS[SOURCE]
    conn_opts  = connector.build_opts(bao)

    conf  = bao.spark_conf(app_name=f"starpump-{SOURCE}")

    # ── 1b. Catalog pre-flight: target catalog must have a wired credential ───
    # starpump writes exclusively through the Polaris service-account credential
    # that was registered for ICEBERG_CATALOG in spark_conf().  If no such entry
    # exists the catalog was never set up and starpump must not proceed — there is
    # no authenticated write path for that catalog name.
    try:
        catalog_svc_id = bao.catalog_credential(ICEBERG_CATALOG, conf)
        logger.info(
            "[catalog-check] '%s' is registered (svc_id=%s). Proceeding.",
            ICEBERG_CATALOG, catalog_svc_id,
        )
    except ValueError as _cred_err:
        logger.error(
            "ERROR: %s\n"
            "  starpump requires a Spark external catalog to be wired in "
            "BaoSparkInit.spark_conf() before data can be copied.\n"
            "  Target catalog: %s\n"
            "  To fix: add a 'spark.sql.catalog.%s.*' block to "
            "docker/spark-gluten-velox/scripts/bao_spark_init.py",
            _cred_err, ICEBERG_CATALOG, ICEBERG_CATALOG,
        )
        sys.exit(1)

    # ── 2. Spark session ──────────────────────────────────────────────────────
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ── 2b. Catalog namespace bootstrap (pre-flight) ──────────────────────────
    # Ensure the target Iceberg namespace exists before any table operation.
    # Uses 00_catalog_bootstrap.py logic directly — no separate process.
    try:
        from importlib import import_module as _imod
        _cb = _imod("00_catalog_bootstrap")
        _cb.bootstrap_single_catalog(spark, ICEBERG_CATALOG, ICEBERG_NAMESPACE)
    except Exception as _cb_err:
        # Non-fatal: namespace may already exist; log and continue.
        logger.warning("[catalog-bootstrap] Pre-flight bootstrap warning: %s", _cb_err)

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

        lock    = threading.Lock()
        results: dict[str, dict] = {}
        t0      = time.time()

        # ── custom_sql mode ───────────────────────────────────────────────────
        if MODE == "custom_sql":
            logger.info("[mode=custom_sql] Running user-supplied SQL query.")
            result = _run_custom_sql(
                spark      = spark,
                builder    = builder,
                connector  = connector,
                conn_opts  = conn_opts,
                s3_bucket  = s3_bucket,
                sql_query  = CUSTOM_SQL,        # type: ignore[arg-type]
                target_tbl = TARGET_TABLE,      # type: ignore[arg-type]
                pg_creds   = pg,
                lock       = lock,
            )
            results[TARGET_TABLE] = result  # type: ignore[index]
            all_tables = [TARGET_TABLE]
            tables     = [TARGET_TABLE]

        else:
            # ── 4. Table discovery (via connector — source-agnostic) ──────────
            all_tables = connector.list_tables(spark, conn_opts)
            if not all_tables:
                logger.error("No tables found in %s.%s — aborting.", DATABASE, SCHEMAS)
                sys.exit(1)

            # ── 5. Size discovery (via connector — source-agnostic) ───────────
            sizes: dict[str, float] = {}
            if _SIZE_FILTER_ENABLED:
                sizes = connector.table_sizes(spark, conn_opts)
            else:
                logger.info("Size discovery skipped (MAX_TABLE_SIZE_GB=0).")

            # ── 6. Apply filters ──────────────────────────────────────────────
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
                "Copying %d/%d table(s) with %d threads, %d rows/batch%s [mode=%s].",
                len(tables), len(all_tables), MAX_THREADS, BATCH_SIZE,
                " [DRY RUN]" if DRY_RUN else "",
                MODE,
            )

            # ── 7. N-thread copy using a work queue ──────────────────────────
            # Each thread pulls the next table from the queue so they naturally
            # pick up new work as soon as they finish.
            work_q: queue.Queue[tuple[str, float]] = queue.Queue()

            if MODE == "incremental":
                # incremental: inject watermark WHERE clause per table
                for tbl in tables:
                    work_q.put((tbl, sizes.get(tbl, 0.0)))

                def worker_incremental() -> None:
                    while True:
                        try:
                            tbl, size_gb = work_q.get_nowait()
                        except queue.Empty:
                            break

                        # ── DDL drift detection ───────────────────────────────
                        if DDL_DRIFT_DETECT:
                            drift = _detect_ddl_drift(spark, connector, conn_opts, tbl)
                            _apply_ddl_drift(spark, tbl, drift)

                        # ── Watermark-based incremental copy ──────────────────
                        _tbl_pk: list[str] = []
                        try:
                            raw_schema = connector.table_schema(spark, conn_opts, tbl)
                            mapped     = connector.map_schema(raw_schema)
                            wm_col     = _resolve_watermark_col(mapped, WATERMARK_COL)

                            # Resolve PK here so it is available for both the
                            # ORDER BY on reads and the MERGE ON clause in _copy_table.
                            _tbl_pk = _resolve_primary_keys(
                                tbl, mapped, connector, spark, conn_opts
                            )

                            last_ts = None
                            if wm_col:
                                try:
                                    last_ts = _pg_read_watermark(
                                        pg, DATABASE, SCHEMAS, tbl,
                                    )
                                except Exception as _wm_err:
                                    logger.warning(
                                        "[%s] Could not read watermark from pipeline DB: %s",
                                        tbl, _wm_err,
                                    )

                            if wm_col:
                                incr_clause = _incremental_where_clause(wm_col, last_ts)
                                logger.info(
                                    "[%s] Incremental mode: col=%s last_ts=%s "
                                    "clause='%s' pk=%s write_mode=%s",
                                    tbl, wm_col, last_ts or "None (full)",
                                    incr_clause, _tbl_pk, WRITE_MODE,
                                )
                            else:
                                incr_clause = ""
                                logger.info(
                                    "[%s] No watermark column found — performing full copy.",
                                    tbl,
                                )

                            # Override QUERY_FILTER for this table with watermark clause.
                            # Combine with any existing table-level QUERY_FILTER predicates.
                            existing_clause = _get_where_clause(tbl)
                            if incr_clause and existing_clause:
                                combined_clause = f"({incr_clause}) AND ({existing_clause})"
                            elif incr_clause:
                                combined_clause = incr_clause
                            else:
                                combined_clause = existing_clause

                            # Temporarily patch QUERY_FILTERS for this table's copy.
                            with lock:
                                _saved = QUERY_FILTERS.get(tbl.lower(), "")
                                QUERY_FILTERS[tbl.lower()] = combined_clause

                        except Exception as _setup_err:
                            logger.error(
                                "[%s] Incremental setup failed: %s — falling back to full copy.",
                                tbl, _setup_err,
                            )
                            with lock:
                                _saved = QUERY_FILTERS.get(tbl.lower(), "")

                        _copy_table(
                            spark, builder, connector, conn_opts, s3_bucket,
                            tbl, size_gb, results, lock,
                            pg_creds   = pg,
                            pk_cols    = _tbl_pk or None,
                            write_mode = WRITE_MODE,
                        )

                        # Restore the original QUERY_FILTERS entry
                        with lock:
                            QUERY_FILTERS[tbl.lower()] = _saved

                        work_q.task_done()

                copy_threads = [
                    threading.Thread(
                        target=worker_incremental,
                        name=f"incr-worker-{i+1}",
                        daemon=True,
                    )
                    for i in range(min(MAX_THREADS, len(tables)))
                ]

            else:
                # full mode — original logic
                # ── DDL drift detection (full mode) ───────────────────────────
                # Run before spawning threads so the schema is settled before
                # any thread opens its table-copy.  Sequential to avoid concurrent
                # ALTER TABLE races on the same table from parallel threads.
                if DDL_DRIFT_DETECT:
                    for tbl in tables:
                        drift = _detect_ddl_drift(spark, connector, conn_opts, tbl)
                        _apply_ddl_drift(spark, tbl, drift)

                for tbl in tables:
                    work_q.put((tbl, sizes.get(tbl, 0.0)))

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

                copy_threads = [
                    threading.Thread(target=worker, name=f"copy-worker-{i+1}", daemon=True)
                    for i in range(min(MAX_THREADS, len(tables)))
                ]

            for th in copy_threads:
                th.start()
            for th in copy_threads:
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
            "%d failed | %d rows written [mode=%s]",
            elapsed, len(ok), len(all_tables), skipped_count,
            len(failed), total_rows, MODE,
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
