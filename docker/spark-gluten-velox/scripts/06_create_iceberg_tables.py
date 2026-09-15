#!/usr/bin/env python3
"""
06_create_iceberg_tables.py
===========================
Bootstrap script — creates all Iceberg tables for the CDC → Iceberg pipeline.

Creates tables for ALL three write modes × ALL three source systems:

  standard         — SCD Type 0 upsert + hard delete
                     Tables: <catalog>.<namespace>.<table>
                     Columns: source cols + snap_id + snap_timestamp

  soft_delete      — Upsert + soft-delete flags
                     Tables: <catalog>.<namespace>.<table>  (same table name as standard)
                     Extra columns: is_deleted BOOLEAN, deleted_at TIMESTAMP
                     NOTE: standard and soft_delete share the same table; the
                     pipeline writes to it in the configured mode. The extra
                     columns are always present so the table works with either mode.

  history_tracking — Append-only full history
                     Tables: <catalog>.<namespace>.<table>_hist
                     Extra columns: _change_type STRING, _change_ts TIMESTAMP,
                                    before_* (schema-evolved at runtime),
                                    after_*  (schema-evolved at runtime)

Partitioning (all tables)
--------------------------
  hours(snap_timestamp)   — hourly partitions for time-range pruning
  bucket(16, <pk_col>)    — 16 hash buckets within each hour

snap columns
------------
  snap_id        BIGINT     — unique row id injected at write time
  snap_timestamp TIMESTAMP  — write-time wall clock (hourly partition key)

Usage
-----
  # Create all tables (dry-run first to preview DDL):
  SPARK_USER=dave DRY_RUN=1 python3 06_create_iceberg_tables.py
  SPARK_USER=dave python3 06_create_iceberg_tables.py

  # Create tables for one source only:
  SPARK_USER=dave SOURCE=postgres python3 06_create_iceberg_tables.py

  # Create tables for one write mode only:
  SPARK_USER=dave WRITE_MODE=history_tracking python3 06_create_iceberg_tables.py
"""

from __future__ import annotations

import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType, DoubleType, DateType,
)

from bao_spark_init import BaoSparkInit
from spark_iceberg_utils import IcebergTableBuilder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("create-iceberg-tables")

SPARK_USER = os.environ.get("SPARK_USER", "dave")
DRY_RUN    = os.environ.get("DRY_RUN", "0") == "1"
SOURCE_FILTER    = os.environ.get("SOURCE", "").lower()
WRITE_MODE_FILTER = os.environ.get("WRITE_MODE", "").lower()
S3_BUCKET  = "xdatatoiceberg1"

# ── Table definitions ──────────────────────────────────────────────────────────
# Each entry: (catalog, namespace, table, pk_col, s3_prefix, schema)
# schema = source-only columns (snap_id + snap_timestamp added automatically).
# soft_delete extra cols (is_deleted, deleted_at) added below.
# history_tracking extra cols (_change_type, _change_ts) added below.
# before_* / after_* for history_tracking evolve at runtime via mergeSchema.

_S = StructField  # alias for brevity

# ── PostgreSQL: cache_testing ──────────────────────────────────────────────────
_PG_CUSTOMERS = StructType([
    _S("id",            LongType(),      False),
    _S("name",          StringType(),    True),
    _S("email",         StringType(),    True),
    _S("phone",         StringType(),    True),
    _S("address",       StringType(),    True),
    _S("city",          StringType(),    True),
    _S("country",       StringType(),    True),
    _S("created_at",    TimestampType(), True),
    _S("updated_at",    TimestampType(), True),
])

_PG_PRODUCTS = StructType([
    _S("id",            LongType(),      False),
    _S("name",          StringType(),    True),
    _S("category",      StringType(),    True),
    _S("price",         DoubleType(),    True),
    _S("stock",         IntegerType(),   True),
    _S("created_at",    TimestampType(), True),
    _S("updated_at",    TimestampType(), True),
])

_PG_PRODUCT_REVIEWS = StructType([
    _S("id",            LongType(),      False),
    _S("product_id",    LongType(),      True),
    _S("customer_id",   LongType(),      True),
    _S("rating",        IntegerType(),   True),
    _S("review_text",   StringType(),    True),
    _S("created_at",    TimestampType(), True),
])

_PG_ORDERS = StructType([
    _S("id",            LongType(),      False),
    _S("customer_id",   LongType(),      True),
    _S("status",        StringType(),    True),
    _S("total_amount",  DoubleType(),    True),
    _S("created_at",    TimestampType(), True),
    _S("updated_at",    TimestampType(), True),
])

# ── Oracle: CACHE_TESTING ──────────────────────────────────────────────────────
_ORA_CT_CUSTOMERS = StructType([
    _S("id",             LongType(),      False),
    _S("name",           StringType(),    True),
    _S("email",          StringType(),    True),
    _S("phone",          StringType(),    True),
    _S("address",        StringType(),    True),
    _S("city",           StringType(),    True),
    _S("country",        StringType(),    True),
    _S("created_at",     TimestampType(), True),
    _S("updated_at",     TimestampType(), True),
])

_ORA_CT_PRODUCTS = StructType([
    _S("id",             LongType(),      False),
    _S("name",           StringType(),    True),
    _S("category",       StringType(),    True),
    _S("price",          DoubleType(),    True),
    _S("stock",          IntegerType(),   True),
    _S("created_at",     TimestampType(), True),
    _S("updated_at",     TimestampType(), True),
])

_ORA_CT_ORDERS = StructType([
    _S("id",             LongType(),      False),
    _S("customer_id",    LongType(),      True),
    _S("status",         StringType(),    True),
    _S("total_amount",   DoubleType(),    True),
    _S("created_at",     TimestampType(), True),
    _S("updated_at",     TimestampType(), True),
])

_ORA_CT_ORDER_ITEMS = StructType([
    _S("id",             LongType(),      False),
    _S("order_id",       LongType(),      True),
    _S("product_id",     LongType(),      True),
    _S("quantity",       IntegerType(),   True),
    _S("unit_price",     DoubleType(),    True),
])

_ORA_CT_PRODUCT_REVIEWS = StructType([
    _S("id",             LongType(),      False),
    _S("product_id",     LongType(),      True),
    _S("customer_id",    LongType(),      True),
    _S("rating",         IntegerType(),   True),
    _S("review_text",    StringType(),    True),
    _S("created_at",     TimestampType(), True),
])

_ORA_CT_INVENTORY_EVENTS = StructType([
    _S("id",             LongType(),      False),
    _S("product_id",     LongType(),      True),
    _S("event_type",     StringType(),    True),
    _S("quantity_delta", IntegerType(),   True),
    _S("event_ts",       TimestampType(), True),
])

# ── Oracle: TPCDS ──────────────────────────────────────────────────────────────
_ORA_TPCDS_INCOME_BAND = StructType([
    _S("ib_income_band_sk", LongType(),   False),
    _S("ib_lower_bound",    LongType(),   True),
    _S("ib_upper_bound",    LongType(),   True),
])

_ORA_TPCDS_SHIP_MODE = StructType([
    _S("sm_ship_mode_sk",   LongType(),   False),
    _S("sm_ship_mode_id",   StringType(), True),
    _S("sm_type",           StringType(), True),
    _S("sm_code",           StringType(), True),
    _S("sm_carrier",        StringType(), True),
    _S("sm_contract",       StringType(), True),
])

_ORA_TPCDS_WAREHOUSE = StructType([
    _S("w_warehouse_sk",    LongType(),   False),
    _S("w_warehouse_id",    StringType(), True),
    _S("w_warehouse_name",  StringType(), True),
    _S("w_warehouse_sq_ft", LongType(),   True),
    _S("w_city",            StringType(), True),
    _S("w_county",          StringType(), True),
    _S("w_state",           StringType(), True),
    _S("w_zip",             StringType(), True),
    _S("w_country",         StringType(), True),
    _S("w_gmt_offset",      DoubleType(), True),
])

_ORA_TPCDS_REASON = StructType([
    _S("r_reason_sk",       LongType(),   False),
    _S("r_reason_id",       StringType(), True),
    _S("r_reason_desc",     StringType(), True),
])

_ORA_TPCDS_CALL_CENTER = StructType([
    _S("cc_call_center_sk", LongType(),   False),
    _S("cc_call_center_id", StringType(), True),
    _S("cc_name",           StringType(), True),
    _S("cc_class",          StringType(), True),
    _S("cc_employees",      LongType(),   True),
    _S("cc_sq_ft",          LongType(),   True),
    _S("cc_city",           StringType(), True),
    _S("cc_county",         StringType(), True),
    _S("cc_state",          StringType(), True),
    _S("cc_zip",            StringType(), True),
    _S("cc_country",        StringType(), True),
    _S("cc_gmt_offset",     DoubleType(), True),
    _S("cc_tax_percentage", DoubleType(), True),
])

_ORA_TPCDS_WEB_SITE = StructType([
    _S("web_site_sk",       LongType(),   False),
    _S("web_site_id",       StringType(), True),
    _S("web_name",          StringType(), True),
    _S("web_class",         StringType(), True),
    _S("web_employees",     LongType(),   True),
    _S("web_city",          StringType(), True),
    _S("web_county",        StringType(), True),
    _S("web_state",         StringType(), True),
    _S("web_zip",           StringType(), True),
    _S("web_country",       StringType(), True),
    _S("web_gmt_offset",    DoubleType(), True),
    _S("web_tax_percentage",DoubleType(), True),
])

_ORA_TPCDS_WEB_PAGE = StructType([
    _S("wp_web_page_sk",    LongType(),   False),
    _S("wp_web_page_id",    StringType(), True),
    _S("wp_char_count",     LongType(),   True),
    _S("wp_link_count",     LongType(),   True),
    _S("wp_image_count",    LongType(),   True),
    _S("wp_max_ad_count",   LongType(),   True),
    _S("wp_type",           StringType(), True),
])

_ORA_TPCDS_HOUSEHOLD_DEMOGRAPHICS = StructType([
    _S("hd_demo_sk",           LongType(),   False),
    _S("hd_income_band_sk",    LongType(),   True),
    _S("hd_buy_potential",     StringType(), True),
    _S("hd_dep_count",         LongType(),   True),
    _S("hd_vehicle_count",     LongType(),   True),
])

_ORA_TPCDS_CATALOG_PAGE = StructType([
    _S("cp_catalog_page_sk",   LongType(),   False),
    _S("cp_catalog_page_id",   StringType(), True),
    _S("cp_department",        StringType(), True),
    _S("cp_catalog_number",    LongType(),   True),
    _S("cp_catalog_page_number",LongType(),  True),
    _S("cp_description",       StringType(), True),
    _S("cp_type",              StringType(), True),
])

_ORA_TPCDS_PROMOTION = StructType([
    _S("p_promo_sk",           LongType(),   False),
    _S("p_promo_id",           StringType(), True),
    _S("p_promo_name",         StringType(), True),
    _S("p_channel_dmail",      StringType(), True),
    _S("p_channel_email",      StringType(), True),
    _S("p_channel_catalog",    StringType(), True),
    _S("p_channel_tv",         StringType(), True),
    _S("p_channel_radio",      StringType(), True),
    _S("p_channel_press",      StringType(), True),
    _S("p_channel_event",      StringType(), True),
    _S("p_channel_demo",       StringType(), True),
    _S("p_discount_active",    StringType(), True),
    _S("p_cost",               DoubleType(), True),
    _S("p_response_target",    LongType(),   True),
])

# ── MongoDB: cache_testing ─────────────────────────────────────────────────────
_MGO_CUSTOMERS = StructType([
    _S("_id",           StringType(),    False),
    _S("name",          StringType(),    True),
    _S("email",         StringType(),    True),
    _S("phone",         StringType(),    True),
    _S("address",       StringType(),    True),
    _S("city",          StringType(),    True),
    _S("country",       StringType(),    True),
    _S("created_at",    TimestampType(), True),
    _S("updated_at",    TimestampType(), True),
])

_MGO_PRODUCTS = StructType([
    _S("_id",           StringType(),    False),
    _S("name",          StringType(),    True),
    _S("category",      StringType(),    True),
    _S("price",         DoubleType(),    True),
    _S("stock",         IntegerType(),   True),
    _S("created_at",    TimestampType(), True),
    _S("updated_at",    TimestampType(), True),
])

# ── Master table registry ──────────────────────────────────────────────────────
# (source_key, catalog, namespace, table, pk_col, s3_prefix, schema)
_TABLE_REGISTRY = [
    # PostgreSQL
    ("postgres", "postgres", "cache_testing", "customers",       "id",            "iceberg/pg_lakehouse",  _PG_CUSTOMERS),
    ("postgres", "postgres", "cache_testing", "products",        "id",            "iceberg/pg_lakehouse",  _PG_PRODUCTS),
    ("postgres", "postgres", "cache_testing", "product_reviews", "id",            "iceberg/pg_lakehouse",  _PG_PRODUCT_REVIEWS),
    ("postgres", "postgres", "cache_testing", "orders",          "id",            "iceberg/pg_lakehouse",  _PG_ORDERS),
    # Oracle CACHE_TESTING
    ("oracle",   "oracle",   "cache_testing", "customers",       "id",            "iceberg/ora_lakehouse", _ORA_CT_CUSTOMERS),
    ("oracle",   "oracle",   "cache_testing", "products",        "id",            "iceberg/ora_lakehouse", _ORA_CT_PRODUCTS),
    ("oracle",   "oracle",   "cache_testing", "orders",          "id",            "iceberg/ora_lakehouse", _ORA_CT_ORDERS),
    ("oracle",   "oracle",   "cache_testing", "order_items",     "id",            "iceberg/ora_lakehouse", _ORA_CT_ORDER_ITEMS),
    ("oracle",   "oracle",   "cache_testing", "product_reviews", "id",            "iceberg/ora_lakehouse", _ORA_CT_PRODUCT_REVIEWS),
    ("oracle",   "oracle",   "cache_testing", "inventory_events","id",            "iceberg/ora_lakehouse", _ORA_CT_INVENTORY_EVENTS),
    # Oracle TPCDS
    ("oracle",   "oracle",   "tpcds",         "income_band",            "ib_income_band_sk",   "iceberg/ora_lakehouse", _ORA_TPCDS_INCOME_BAND),
    ("oracle",   "oracle",   "tpcds",         "ship_mode",              "sm_ship_mode_sk",     "iceberg/ora_lakehouse", _ORA_TPCDS_SHIP_MODE),
    ("oracle",   "oracle",   "tpcds",         "warehouse",              "w_warehouse_sk",      "iceberg/ora_lakehouse", _ORA_TPCDS_WAREHOUSE),
    ("oracle",   "oracle",   "tpcds",         "reason",                 "r_reason_sk",         "iceberg/ora_lakehouse", _ORA_TPCDS_REASON),
    ("oracle",   "oracle",   "tpcds",         "call_center",            "cc_call_center_sk",   "iceberg/ora_lakehouse", _ORA_TPCDS_CALL_CENTER),
    ("oracle",   "oracle",   "tpcds",         "web_site",               "web_site_sk",         "iceberg/ora_lakehouse", _ORA_TPCDS_WEB_SITE),
    ("oracle",   "oracle",   "tpcds",         "web_page",               "wp_web_page_sk",      "iceberg/ora_lakehouse", _ORA_TPCDS_WEB_PAGE),
    ("oracle",   "oracle",   "tpcds",         "household_demographics",  "hd_demo_sk",          "iceberg/ora_lakehouse", _ORA_TPCDS_HOUSEHOLD_DEMOGRAPHICS),
    ("oracle",   "oracle",   "tpcds",         "catalog_page",           "cp_catalog_page_sk",  "iceberg/ora_lakehouse", _ORA_TPCDS_CATALOG_PAGE),
    ("oracle",   "oracle",   "tpcds",         "promotion",              "p_promo_sk",          "iceberg/ora_lakehouse", _ORA_TPCDS_PROMOTION),
    # MongoDB
    ("mongodb",  "mongodb",  "cache_testing", "customers",       "_id",           "iceberg/mgo_lakehouse", _MGO_CUSTOMERS),
    ("mongodb",  "mongodb",  "cache_testing", "products",        "_id",           "iceberg/mgo_lakehouse", _MGO_PRODUCTS),
]


# ── Schema builders per write mode ────────────────────────────────────────────

_SOFT_DELETE_EXTRA = [
    StructField("is_deleted", BooleanType(),  True),
    StructField("deleted_at", TimestampType(), True),
]

_HISTORY_EXTRA = [
    StructField("_change_type", StringType(),    True),
    StructField("_change_ts",   TimestampType(), True),
    # before_* / after_* columns evolve at runtime via mergeSchema — not declared here
]


def _schema_for_mode(base_schema: StructType, write_mode: str) -> StructType:
    """Return the full schema for the given write mode (base + mode extras)."""
    extra: list[StructField] = []
    if write_mode == "soft_delete":
        extra = _SOFT_DELETE_EXTRA
    elif write_mode == "history_tracking":
        extra = _HISTORY_EXTRA
    # standard: no extras beyond snap columns (added by create_table)
    existing_names = {f.name for f in base_schema.fields}
    return StructType(
        base_schema.fields + [f for f in extra if f.name not in existing_names]
    )


def _table_name_for_mode(table: str, write_mode: str) -> str:
    """history_tracking tables get a _hist suffix."""
    return f"{table}_hist" if write_mode == "history_tracking" else table


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    os.environ["SPARK_USER"] = SPARK_USER

    logger.info(
        "=== Create Iceberg Tables | user=%s | dry_run=%s | source=%s | mode=%s ===",
        SPARK_USER, DRY_RUN,
        SOURCE_FILTER or "all",
        WRITE_MODE_FILTER or "all",
    )

    bao   = BaoSparkInit()
    conf  = bao.spark_conf(app_name="create-iceberg-tables")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    builder = IcebergTableBuilder(spark, running_user=SPARK_USER)

    write_modes = (
        [WRITE_MODE_FILTER] if WRITE_MODE_FILTER
        else ["standard", "soft_delete", "history_tracking"]
    )

    # Ensure all namespaces exist
    seen_ns: set[tuple] = set()
    for (src, catalog, namespace, table, pk, s3pfx, schema) in _TABLE_REGISTRY:
        if SOURCE_FILTER and src != SOURCE_FILTER:
            continue
        ns_key = (catalog, namespace)
        if ns_key not in seen_ns:
            if not DRY_RUN:
                builder.ensure_namespace(catalog, namespace)
            else:
                logger.info("[DRY_RUN] Would ensure namespace `%s`.`%s`", catalog, namespace)
            seen_ns.add(ns_key)

    created = 0
    skipped = 0
    errors  = 0

    for write_mode in write_modes:
        logger.info("─── Write mode: %s ───", write_mode)

        for (src, catalog, namespace, base_table, pk_col, s3_prefix, base_schema) in _TABLE_REGISTRY:
            if SOURCE_FILTER and src != SOURCE_FILTER:
                continue

            table    = _table_name_for_mode(base_table, write_mode)
            schema   = _schema_for_mode(base_schema, write_mode)
            location = f"s3://{S3_BUCKET}/{s3_prefix}/{namespace}/{table}"
            fqn      = f"`{catalog}`.`{namespace}`.`{table}`"

            if builder.table_exists(catalog, namespace, table):
                logger.info("  [EXISTS]  %s", fqn)
                skipped += 1
                continue

            partition_spec = [
                IcebergTableBuilder.hours("snap_timestamp"),
                IcebergTableBuilder.bucket(pk_col, 16),
            ]

            if DRY_RUN:
                # Print the DDL that would be executed
                aug_schema = StructType(
                    schema.fields + [
                        StructField("snap_id",        LongType(),      True),
                        StructField("snap_timestamp", TimestampType(), True),
                    ]
                )
                col_ddl = "\n".join(
                    f"    {f.name} {f.dataType.simpleString()}"
                    for f in aug_schema.fields
                )
                logger.info(
                    "[DRY_RUN] Would CREATE TABLE %s (\n%s\n)"
                    "\n  PARTITIONED BY (hours(snap_timestamp), bucket(16, %s))"
                    "\n  LOCATION '%s'",
                    fqn, col_ddl, pk_col, location,
                )
                created += 1
                continue

            try:
                builder.create_table(
                    catalog           = catalog,
                    namespace         = namespace,
                    table             = table,
                    schema            = schema,
                    partition_spec    = partition_spec,
                    location          = location,
                    extra_properties  = {
                        "pipeline.write-mode": write_mode,
                        "pipeline.source":     src,
                        "pipeline.pk-col":     pk_col,
                    },
                )
                logger.info("  [CREATED] %s", fqn)
                created += 1
            except Exception as exc:
                logger.error("  [ERROR]   %s — %s", fqn, exc)
                errors += 1

    logger.info("")
    logger.info("═══════════════════════════════════════")
    logger.info("  Tables created : %d", created)
    logger.info("  Already exist  : %d (skipped)", skipped)
    logger.info("  Errors         : %d", errors)
    logger.info("═══════════════════════════════════════")

    spark.stop()

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
