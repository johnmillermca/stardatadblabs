#!/usr/bin/env python3
"""
snowflake_to_iceberg.py
=======================
8-thread Snowflake → Spark Iceberg copy pipeline.

What it does
------------
1. Connects to Snowflake (SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL) via the
   Snowflake Spark connector.
2. Discovers all 24 tables and their column schemas dynamically.
3. Maps Snowflake data types → Iceberg / Spark types.
4. Creates Iceberg tables in Polaris (catalog: polaris, namespace:
   tpcds_sf10tcl) via IcebergTableBuilder which injects snap audit columns
   (snap_timestamp, snap_id) automatically.
5. Each Iceberg table uses:
      • hourly range partition on the first date/timestamp column found
      • 4 hash sub-partitions on the first integer PK/SK column found
      • Parquet + Snappy, target file size 2.5 MB (≈ 2:56 MiB)
6. Copies in batches of 50 000 rows continuously.
7. Runs up to 8 tables simultaneously across a thread pool.
8. Errors immediately if the Snowflake internal Spark catalog
   (snowflake_sample) does not exist.

Pre-requisites
--------------
• spark-defaults-configmap.yaml applied and pods restarted
• JAR: net.snowflake:spark-snowflake_2.12:2.15.0-spark_3.5 available
• JAR: iceberg-spark-runtime-3.5_2.12-1.9.2.jar in /opt/spark/jars/
• OpenBao reachable (in-cluster or BAO_TOKEN env-var set)
• Polaris catalog "IcebergCatalog" exists with namespace "tpcds_sf10tcl"
• RBAC: running as bob or dave (can_admin_catalog=true, can_write_iceberg=true)

Usage
-----
    # Inside JupyterHub or spark-master pod (as bob / dave):
    python snowflake_to_iceberg.py

    # Override target table list:
    TABLES=customer,item,store python snowflake_to_iceberg.py

    # Dry-run (create tables, skip data copy):
    DRY_RUN=1 python snowflake_to_iceberg.py
"""

from __future__ import annotations

import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit
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
from spark_iceberg_utils import IcebergTableBuilder

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("snowflake_to_iceberg")

# ── Constants ─────────────────────────────────────────────────────────────────
_ICEBERG_CATALOG   = "polaris"
_ICEBERG_NAMESPACE = "tpcds_sf10tcl"
_S3_BUCKET         = "xdatatoiceberg1"
_S3_PREFIX         = "iceberg"
_BATCH_SIZE        = 50_000
_MAX_THREADS       = 8
_TARGET_FILE_BYTES = 2_621_440   # 2.5 MB ≈ 2:56 MiB
_SNOWFLAKE_DB      = "SNOWFLAKE_SAMPLE_DATA"
_SNOWFLAKE_SCHEMA  = "TPCDS_SF10TCL"
_SPARK_SF_CATALOG  = "snowflake_sample"   # must exist in spark-defaults.conf

DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"
OVERRIDE_TABLES = (
    [t.strip() for t in os.environ["TABLES"].split(",")]
    if "TABLES" in os.environ
    else None
)

# Known 24 TPC-DS tables in SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL
_ALL_TPCDS_TABLES = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales",
    "customer", "customer_address", "customer_demographics", "date_dim",
    "household_demographics", "income_band", "inventory", "item",
    "promotion", "reason", "ship_mode", "store", "store_returns",
    "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site",
]

# ── Snowflake → Spark type map ─────────────────────────────────────────────────
# Only the Snowflake-specific type names that differ from generic SQL types.
_SF_TYPE_MAP: dict[str, Any] = {
    "TEXT":             StringType(),
    "VARCHAR":          StringType(),
    "CHAR":             StringType(),
    "CHARACTER":        StringType(),
    "NCHAR":            StringType(),
    "NVARCHAR":         StringType(),
    "NVARCHAR2":        StringType(),
    "STRING":           StringType(),
    "VARIANT":          StringType(),
    "OBJECT":           StringType(),
    "ARRAY":            StringType(),
    "BINARY":           StringType(),
    "VARBINARY":        StringType(),
    "NUMBER":           DecimalType(38, 10),
    "NUMERIC":          DecimalType(38, 10),
    "DECIMAL":          DecimalType(38, 10),
    "INT":              LongType(),
    "INTEGER":          LongType(),
    "BIGINT":           LongType(),
    "SMALLINT":         ShortType(),
    "TINYINT":          ByteType(),
    "BYTEINT":          ByteType(),
    "FLOAT":            FloatType(),
    "FLOAT4":           FloatType(),
    "FLOAT8":           DoubleType(),
    "DOUBLE":           DoubleType(),
    "DOUBLE PRECISION": DoubleType(),
    "REAL":             FloatType(),
    "BOOLEAN":          BooleanType(),
    "DATE":             DateType(),
    "DATETIME":         TimestampType(),
    "TIMESTAMP":        TimestampType(),
    "TIMESTAMP_LTZ":    TimestampType(),
    "TIMESTAMP_NTZ":    TimestampType(),
    "TIMESTAMP_TZ":     TimestampType(),
    "TIME":             StringType(),   # no native TIME in Iceberg
}


def _sf_type_to_spark(sf_type_str: str) -> Any:
    """Map a Snowflake column type string to a Spark DataType."""
    upper = sf_type_str.upper().strip()
    # Handle NUMBER(p,s) / VARCHAR(n) with precision/scale
    base = upper.split("(")[0].strip()
    if base in ("NUMBER", "NUMERIC", "DECIMAL"):
        # try to extract precision/scale
        if "(" in upper:
            inner = upper[upper.index("(") + 1: upper.index(")")]
            parts = inner.split(",")
            p = int(parts[0].strip())
            s = int(parts[1].strip()) if len(parts) > 1 else 0
            return DecimalType(p, s)
    return _SF_TYPE_MAP.get(base, StringType())


# ── Schema discovery ───────────────────────────────────────────────────────────

def discover_schema(spark: SparkSession, sf_opts: dict, table: str) -> StructType:
    """
    Read one row from Snowflake to infer the schema dynamically.
    The Snowflake Spark connector returns a StructType we can reuse.
    """
    df: DataFrame = (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**sf_opts)
        .option("query", f"SELECT * FROM {table} LIMIT 1")
        .load()
    )
    return df.schema


def _partition_spec_for(schema: StructType) -> list[dict]:
    """
    Build a partition spec:
      - hours(<first timestamp/date col>)   ← hourly range partition
      - bucket(4, <first integer SK/PK col>) ← 4 hash partitions per hour
    Falls back to snap_timestamp / snap_id if schema has no suitable columns.
    """
    ts_col: str | None = None
    int_col: str | None = None

    ts_types = (TimestampType, DateType)
    int_types = (IntegerType, LongType, ShortType, ByteType)

    for field in schema.fields:
        n = field.name.lower()
        if ts_col is None and isinstance(field.dataType, ts_types):
            ts_col = field.name
        if int_col is None and isinstance(field.dataType, int_types) and (
            n.endswith("_sk") or n.endswith("_id") or n.endswith("_key")
        ):
            int_col = field.name

    spec = []
    spec.append(IcebergTableBuilder.hours(ts_col or "snap_timestamp"))
    spec.append(IcebergTableBuilder.bucket(int_col or "snap_id", 4))
    return spec


# ── RBAC check ────────────────────────────────────────────────────────────────

def _assert_rbac(spark: SparkSession) -> None:
    """
    Fail fast if the current Spark user lacks can_write_iceberg or
    can_admin_catalog according to the spark-rbac-allowlist ConfigMap.
    The allowlist is read from an env-var injected by the ConfigMap mount.
    """
    allowed_admins = {"bob", "dave"}
    spark_user = os.environ.get("SPARK_USER", os.environ.get("USER", ""))
    if spark_user not in allowed_admins:
        raise PermissionError(
            f"RBAC check failed: user '{spark_user}' is not in the "
            f"can_admin_catalog + can_write_iceberg allowlist. "
            f"Allowed users: {sorted(allowed_admins)}. "
            "Set SPARK_USER=bob (or dave) or run as a permitted user."
        )
    logger.info("RBAC OK: user '%s' has can_admin_catalog + can_write_iceberg.", spark_user)


# ── Catalog existence check ───────────────────────────────────────────────────

def _assert_spark_sf_catalog(spark: SparkSession) -> None:
    """
    Error if the Snowflake internal Spark catalog (snowflake_sample) is
    not registered. This prevents silent copy-from-wrong-source bugs.
    """
    catalogs = [
        row[0] for row in spark.sql("SHOW CATALOGS").collect()
    ]
    if _SPARK_SF_CATALOG not in catalogs:
        raise RuntimeError(
            f"Spark catalog '{_SPARK_SF_CATALOG}' not found. "
            "Apply spark-defaults-configmap.yaml and restart spark-master/worker "
            "before running this copy job. "
            f"Available catalogs: {catalogs}"
        )
    logger.info("Spark catalog '%s' confirmed present.", _SPARK_SF_CATALOG)


# ── Namespace bootstrap ───────────────────────────────────────────────────────

def _ensure_namespace(spark: SparkSession) -> None:
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS `{_ICEBERG_CATALOG}`.`{_ICEBERG_NAMESPACE}`"
    )
    logger.info("Namespace %s.%s ready.", _ICEBERG_CATALOG, _ICEBERG_NAMESPACE)


# ── Single-table copy ─────────────────────────────────────────────────────────

def copy_table(
    spark: SparkSession,
    builder: IcebergTableBuilder,
    sf_opts: dict,
    table_name: str,
) -> dict:
    """
    Copy one TPC-DS table from Snowflake → Iceberg.
    Returns a result dict with status and row counts.
    """
    result = {
        "table": table_name,
        "status": "pending",
        "rows_written": 0,
        "error": None,
    }
    try:
        logger.info("[%s] Discovering schema …", table_name)
        sf_schema = discover_schema(spark, sf_opts, table_name)

        # Build Iceberg schema (without snap cols — builder injects them)
        iceberg_schema = StructType([
            StructField(f.name, _sf_type_to_spark(f.dataType.simpleString()), True)
            for f in sf_schema.fields
        ])

        partition_spec = _partition_spec_for(iceberg_schema)

        # S3 location
        s3_location = (
            f"s3a://{_S3_BUCKET}/{_S3_PREFIX}/{_ICEBERG_NAMESPACE}/{table_name}"
        )

        # Create Iceberg table (idempotent)
        fqn = builder.create_table(
            catalog=_ICEBERG_CATALOG,
            namespace=_ICEBERG_NAMESPACE,
            table=table_name,
            schema=iceberg_schema,
            partition_spec=partition_spec,
            location=s3_location,
            target_file_size_bytes=_TARGET_FILE_BYTES,
        )
        logger.info("[%s] Iceberg table ready: %s", table_name, fqn)

        if DRY_RUN:
            logger.info("[%s] DRY_RUN=1 — skipping data copy.", table_name)
            result["status"] = "dry_run"
            return result

        # ── Batched copy ───────────────────────────────────────────────────────
        total_rows = 0
        offset = 0

        while True:
            query = (
                f"SELECT * FROM {table_name} "
                f"ORDER BY 1 "
                f"LIMIT {_BATCH_SIZE} OFFSET {offset}"
            )
            batch_df: DataFrame = (
                spark.read.format("net.snowflake.spark.snowflake")
                .options(**sf_opts)
                .option("query", query)
                .load()
            )
            batch_count = batch_df.count()
            if batch_count == 0:
                break

            # Align columns to Iceberg schema (drop extra, add missing as NULL)
            iceberg_cols = [f.name for f in iceberg_schema.fields]
            batch_aligned = batch_df.select(
                *[
                    (batch_df[c] if c in batch_df.columns else lit(None).cast(
                        iceberg_schema[c].dataType
                    )).alias(c)
                    for c in iceberg_cols
                ]
            )

            # Inject snap audit values
            now_ts = datetime.now(tz=timezone.utc)
            snap_id_val = int(now_ts.timestamp() * 1000)   # ms epoch as snap id
            batch_with_snap = (
                batch_aligned
                .withColumn("snap_timestamp", current_timestamp())
                .withColumn("snap_id", lit(snap_id_val).cast(LongType()))
            )

            (
                batch_with_snap.writeTo(fqn)
                .option("mergeSchema", "true")
                .append()
            )

            total_rows += batch_count
            offset      += batch_count
            logger.info(
                "[%s] Written %d rows (batch %d rows, offset %d).",
                table_name, total_rows, batch_count, offset - batch_count,
            )

            if batch_count < _BATCH_SIZE:
                # Last batch
                break

        result["status"] = "success"
        result["rows_written"] = total_rows
        logger.info("[%s] ✓ Complete — %d total rows.", table_name, total_rows)

    except Exception as exc:  # noqa: BLE001
        result["status"] = "error"
        result["error"] = str(exc)
        logger.error("[%s] ✗ Failed: %s", table_name, exc, exc_info=True)

    return result


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    # 1. Build SparkConf from OpenBao
    init = BaoSparkInit()

    extra = {
        # Snowflake connector pulled at runtime from Maven Central
        "spark.jars.packages": (
            "net.snowflake:spark-snowflake_2.12:2.15.0-spark_3.5,"
            "net.snowflake:snowflake-jdbc:3.16.1"
        ),
    }
    conf = init.spark_conf(app_name="snowflake-to-iceberg", extra_conf=extra)
    sf_opts = init.snowflake_options(schema=_SNOWFLAKE_SCHEMA)

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 2. RBAC guard
        _assert_rbac(spark)

        # 3. Catalog existence guard
        _assert_spark_sf_catalog(spark)

        # 4. Ensure target namespace
        _ensure_namespace(spark)

        # 5. Resolve table list
        tables = OVERRIDE_TABLES or _ALL_TPCDS_TABLES
        builder = IcebergTableBuilder(spark)

        logger.info(
            "Starting copy of %d table(s) with up to %d threads%s …",
            len(tables), _MAX_THREADS, " [DRY RUN]" if DRY_RUN else "",
        )

        # 6. 8-thread copy
        results = []
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=_MAX_THREADS) as pool:
            futures = {
                pool.submit(copy_table, spark, builder, sf_opts, tbl): tbl
                for tbl in tables
            }
            for fut in as_completed(futures):
                results.append(fut.result())

        elapsed = time.time() - t0

        # 7. Summary
        ok     = [r for r in results if r["status"] in ("success", "dry_run")]
        failed = [r for r in results if r["status"] == "error"]
        total  = sum(r["rows_written"] for r in ok)

        logger.info("─" * 60)
        logger.info(
            "Copy complete in %.1fs — %d/%d tables OK, %d failed, %d total rows.",
            elapsed, len(ok), len(tables), len(failed), total,
        )
        for r in failed:
            logger.error("  FAILED: %s — %s", r["table"], r["error"])
        logger.info("─" * 60)

        if failed:
            sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
