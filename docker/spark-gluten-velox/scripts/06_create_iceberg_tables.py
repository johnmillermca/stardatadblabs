#!/usr/bin/env python3
"""
06_create_iceberg_tables.py
===========================
Bootstrap script — creates ALL Iceberg tables for the CDC → Iceberg pipeline.

Table layout
============

Production tables  (namespace: cache_testing / tpcds)
------------------------------------------------------
These replicate live CDC data and are intended to stay permanently.

  <catalog>.cache_testing.<table>_std    — standard  (SCD Type 0 upsert + hard delete)
  <catalog>.cache_testing.<table>_sd     — soft_delete (upsert + is_deleted flag)
  <catalog>.cache_testing.<table>_hist   — history_tracking (append-only, before/after)

Each source has its own copy of every table, so the same Kafka topic can fan-out
into three independent Iceberg tables simultaneously when all three write-mode
deployments are active.

Test & StarTransform tables  (namespace: e2e_testing — single namespace per catalog)
--------------------------------------------------------------------------------------
All E2E test tables and StarTransform function test tables share ONE namespace
so they are easy to browse and reset without touching production data.
Write mode and transform function are encoded in the table name suffix.

  Write-mode test tables (fed from same source tables):
    <catalog>.e2e_testing.customers_std         — standard mode (SCD Type 0)
    <catalog>.e2e_testing.customers_sd          — soft_delete mode
    <catalog>.e2e_testing.customers_hist        — history_tracking mode
    <catalog>.e2e_testing.orders_std            — standard mode
    <catalog>.e2e_testing.orders_sd             — soft_delete mode
    <catalog>.e2e_testing.orders_hist           — history_tracking mode

  StarTransform test tables (PostgreSQL only, fed from customers/products/orders):
    postgres.e2e_testing.customers_dedup        — deduplicate() test
    postgres.e2e_testing.customers_masked       — mask_columns() (PII hashed) test
    postgres.e2e_testing.customers_proc_time    — add_processing_time() test
    postgres.e2e_testing.customers_op_label     — add_op_label() test
    postgres.e2e_testing.customers_source_tag   — add_source_tag() test
    postgres.e2e_testing.customers_filter_ins   — filter_op(["c","u"]) inserts/updates only
    postgres.e2e_testing.customers_filter_del   — filter_op(["d"]) deletes only
    postgres.e2e_testing.orders_enriched        — enrich_from_broadcast() join products
    postgres.e2e_testing.customers_before_after — pivot_before_after() before/after cols
    postgres.e2e_testing.customers_nullcoal     — null_coalesce() test
    postgres.e2e_testing.event_counts           — aggregate_counts() test

Naming convention
-----------------
  _std              = standard write mode
  _sd               = soft_delete write mode
  _hist             = history_tracking write mode (always append)
  _dedup            = deduplicate() transform
  _masked           = mask_columns() transform
  _proc_time        = add_processing_time() transform
  _op_label         = add_op_label() transform
  _source_tag       = add_source_tag() transform
  _filter_ins       = filter_op(["c","u"]) transform
  _filter_del       = filter_op(["d"]) transform
  _enriched         = enrich_from_broadcast() transform
  _before_after     = pivot_before_after() transform
  _nullcoal         = null_coalesce() transform
  event_counts      = aggregate_counts() transform

Partitioning (all tables)
--------------------------
  hours(snap_timestamp)   — hourly partitions for time-range pruning
  bucket(16, <pk_col>)    — 16-way hash bucket on primary key

Snap columns (all tables)
--------------------------
  snap_id        BIGINT     — unique row id injected at write time
  snap_timestamp TIMESTAMP  — write-time wall clock (hourly partition key)

Usage
-----
  # Dry-run — preview all DDL without writing anything:
  SPARK_USER=dave DRY_RUN=1 python3 06_create_iceberg_tables.py

  # Create all tables:
  SPARK_USER=dave python3 06_create_iceberg_tables.py

  # Only production tables for one source:
  SPARK_USER=dave TABLE_GROUP=prod SOURCE=postgres python3 06_create_iceberg_tables.py

  # Only test/transform tables:
  SPARK_USER=dave TABLE_GROUP=test python3 06_create_iceberg_tables.py

  # Only StarTransform test tables:
  SPARK_USER=dave TABLE_GROUP=transforms python3 06_create_iceberg_tables.py
"""

from __future__ import annotations

import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType, DoubleType,
)

from bao_spark_init import BaoSparkInit
from spark_iceberg_utils import IcebergTableBuilder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("create-iceberg-tables")

SPARK_USER    = os.environ.get("SPARK_USER", "dave")
DRY_RUN       = os.environ.get("DRY_RUN", "0") == "1"
SOURCE_FILTER = os.environ.get("SOURCE", "").lower()
# TABLE_GROUP: all | prod | test | transforms
TABLE_GROUP   = os.environ.get("TABLE_GROUP", "all").lower()
S3_BUCKET     = "xdatatoiceberg1"

# Single namespace for all E2E test and StarTransform tables
E2E_NS = "e2e_testing"

_S = StructField  # brevity alias


# ═══════════════════════════════════════════════════════════════════════════════
# BASE SCHEMAS  (source columns only — snap_id/snap_timestamp added by builder)
# ═══════════════════════════════════════════════════════════════════════════════

# ── PostgreSQL ─────────────────────────────────────────────────────────────────
_PG_CUSTOMERS = StructType([
    _S("id",         LongType(),      False),
    _S("name",       StringType(),    True),
    _S("email",      StringType(),    True),
    _S("phone",      StringType(),    True),
    _S("address",    StringType(),    True),
    _S("city",       StringType(),    True),
    _S("country",    StringType(),    True),
    _S("created_at", TimestampType(), True),
    _S("updated_at", TimestampType(), True),
])

_PG_PRODUCTS = StructType([
    _S("id",         LongType(),      False),
    _S("name",       StringType(),    True),
    _S("category",   StringType(),    True),
    _S("price",      DoubleType(),    True),
    _S("stock",      IntegerType(),   True),
    _S("created_at", TimestampType(), True),
    _S("updated_at", TimestampType(), True),
])

_PG_PRODUCT_REVIEWS = StructType([
    _S("id",          LongType(),      False),
    _S("product_id",  LongType(),      True),
    _S("customer_id", LongType(),      True),
    _S("rating",      IntegerType(),   True),
    _S("review_text", StringType(),    True),
    _S("created_at",  TimestampType(), True),
])

_PG_ORDERS = StructType([
    _S("id",           LongType(),      False),
    _S("customer_id",  LongType(),      True),
    _S("status",       StringType(),    True),
    _S("total_amount", DoubleType(),    True),
    _S("created_at",   TimestampType(), True),
    _S("updated_at",   TimestampType(), True),
])

# ── Oracle CACHE_TESTING ───────────────────────────────────────────────────────
_ORA_CT_CUSTOMERS = StructType([
    _S("id",         LongType(),      False),
    _S("name",       StringType(),    True),
    _S("email",      StringType(),    True),
    _S("phone",      StringType(),    True),
    _S("address",    StringType(),    True),
    _S("city",       StringType(),    True),
    _S("country",    StringType(),    True),
    _S("created_at", TimestampType(), True),
    _S("updated_at", TimestampType(), True),
])
_ORA_CT_PRODUCTS = StructType([
    _S("id",         LongType(),      False),
    _S("name",       StringType(),    True),
    _S("category",   StringType(),    True),
    _S("price",      DoubleType(),    True),
    _S("stock",      IntegerType(),   True),
    _S("created_at", TimestampType(), True),
    _S("updated_at", TimestampType(), True),
])
_ORA_CT_ORDERS = StructType([
    _S("id",           LongType(),      False),
    _S("customer_id",  LongType(),      True),
    _S("status",       StringType(),    True),
    _S("total_amount", DoubleType(),    True),
    _S("created_at",   TimestampType(), True),
    _S("updated_at",   TimestampType(), True),
])
_ORA_CT_ORDER_ITEMS = StructType([
    _S("id",         LongType(),    False),
    _S("order_id",   LongType(),    True),
    _S("product_id", LongType(),    True),
    _S("quantity",   IntegerType(), True),
    _S("unit_price", DoubleType(),  True),
])
_ORA_CT_PRODUCT_REVIEWS = StructType([
    _S("id",          LongType(),      False),
    _S("product_id",  LongType(),      True),
    _S("customer_id", LongType(),      True),
    _S("rating",      IntegerType(),   True),
    _S("review_text", StringType(),    True),
    _S("created_at",  TimestampType(), True),
])
_ORA_CT_INVENTORY_EVENTS = StructType([
    _S("id",             LongType(),      False),
    _S("product_id",     LongType(),      True),
    _S("event_type",     StringType(),    True),
    _S("quantity_delta", IntegerType(),   True),
    _S("event_ts",       TimestampType(), True),
])

# ── Oracle TPCDS ───────────────────────────────────────────────────────────────
_ORA_TPCDS_INCOME_BAND = StructType([
    _S("ib_income_band_sk", LongType(), False),
    _S("ib_lower_bound",    LongType(), True),
    _S("ib_upper_bound",    LongType(), True),
])
_ORA_TPCDS_SHIP_MODE = StructType([
    _S("sm_ship_mode_sk", LongType(),   False),
    _S("sm_ship_mode_id", StringType(), True),
    _S("sm_type",         StringType(), True),
    _S("sm_code",         StringType(), True),
    _S("sm_carrier",      StringType(), True),
    _S("sm_contract",     StringType(), True),
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
    _S("r_reason_sk",   LongType(),   False),
    _S("r_reason_id",   StringType(), True),
    _S("r_reason_desc", StringType(), True),
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
    _S("web_site_sk",        LongType(),   False),
    _S("web_site_id",        StringType(), True),
    _S("web_name",           StringType(), True),
    _S("web_class",          StringType(), True),
    _S("web_employees",      LongType(),   True),
    _S("web_city",           StringType(), True),
    _S("web_county",         StringType(), True),
    _S("web_state",          StringType(), True),
    _S("web_zip",            StringType(), True),
    _S("web_country",        StringType(), True),
    _S("web_gmt_offset",     DoubleType(), True),
    _S("web_tax_percentage", DoubleType(), True),
])
_ORA_TPCDS_WEB_PAGE = StructType([
    _S("wp_web_page_sk",  LongType(),   False),
    _S("wp_web_page_id",  StringType(), True),
    _S("wp_char_count",   LongType(),   True),
    _S("wp_link_count",   LongType(),   True),
    _S("wp_image_count",  LongType(),   True),
    _S("wp_max_ad_count", LongType(),   True),
    _S("wp_type",         StringType(), True),
])
_ORA_TPCDS_HOUSEHOLD_DEMOGRAPHICS = StructType([
    _S("hd_demo_sk",        LongType(),   False),
    _S("hd_income_band_sk", LongType(),   True),
    _S("hd_buy_potential",  StringType(), True),
    _S("hd_dep_count",      LongType(),   True),
    _S("hd_vehicle_count",  LongType(),   True),
])
_ORA_TPCDS_CATALOG_PAGE = StructType([
    _S("cp_catalog_page_sk",     LongType(),   False),
    _S("cp_catalog_page_id",     StringType(), True),
    _S("cp_department",          StringType(), True),
    _S("cp_catalog_number",      LongType(),   True),
    _S("cp_catalog_page_number", LongType(),   True),
    _S("cp_description",         StringType(), True),
    _S("cp_type",                StringType(), True),
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

# ── MongoDB ────────────────────────────────────────────────────────────────────
_MGO_CUSTOMERS = StructType([
    _S("_id",        StringType(),    False),
    _S("name",       StringType(),    True),
    _S("email",      StringType(),    True),
    _S("phone",      StringType(),    True),
    _S("address",    StringType(),    True),
    _S("city",       StringType(),    True),
    _S("country",    StringType(),    True),
    _S("created_at", TimestampType(), True),
    _S("updated_at", TimestampType(), True),
])
_MGO_PRODUCTS = StructType([
    _S("_id",        StringType(),    False),
    _S("name",       StringType(),    True),
    _S("category",   StringType(),    True),
    _S("price",      DoubleType(),    True),
    _S("stock",      IntegerType(),   True),
    _S("created_at", TimestampType(), True),
    _S("updated_at", TimestampType(), True),
])


# ═══════════════════════════════════════════════════════════════════════════════
# MODE-SPECIFIC EXTRA COLUMNS
# ═══════════════════════════════════════════════════════════════════════════════

_SD_EXTRA = [                                          # soft_delete additions
    _S("is_deleted", BooleanType(),  True),
    _S("deleted_at", TimestampType(), True),
]
_HIST_EXTRA = [                                        # history_tracking additions
    _S("_change_type", StringType(),    True),         # INSERT / UPDATE / DELETE
    _S("_change_ts",   TimestampType(), True),         # pipeline processing time
    # before_* / after_* columns added at runtime via mergeSchema
]


def _with_extra(base: StructType, extras: list[StructField]) -> StructType:
    existing = {f.name for f in base.fields}
    return StructType(base.fields + [f for f in extras if f.name not in existing])


# ═══════════════════════════════════════════════════════════════════════════════
# TABLE REGISTRIES
# ═══════════════════════════════════════════════════════════════════════════════
# Each entry is a dict:
#   group      : "prod" | "test" | "transforms"
#   source_key : "postgres" | "oracle" | "mongodb"
#   catalog    : Polaris catalog name
#   namespace  : Iceberg namespace
#   table      : Iceberg table name (no backticks)
#   pk_col     : primary key column for MERGE + bucket partitioning
#   s3_prefix  : path under s3://<S3_BUCKET>/
#   schema     : StructType (base + mode extras already merged)
#   write_mode : "standard" | "soft_delete" | "history_tracking"
#   purpose    : short description shown in dry-run output
# ───────────────────────────────────────────────────────────────────────────────

def _build_registry() -> list[dict]:
    reg: list[dict] = []

    # ── helper to add all three mode variants for a production table ───────────
    def _add_prod(src: str, cat: str, ns: str, base_tbl: str, pk: str, s3pfx: str,
                  base_schema: StructType, purpose: str = "") -> None:
        # standard  → <table>_std
        reg.append(dict(
            group="prod", source_key=src, catalog=cat, namespace=ns,
            table=f"{base_tbl}_std", pk_col=pk,
            s3_prefix=f"{s3pfx}/{ns}/{base_tbl}_std",
            schema=base_schema,
            write_mode="standard",
            purpose=purpose or f"{src} {base_tbl} standard (SCD Type 0)",
        ))
        # soft_delete → <table>_sd
        reg.append(dict(
            group="prod", source_key=src, catalog=cat, namespace=ns,
            table=f"{base_tbl}_sd", pk_col=pk,
            s3_prefix=f"{s3pfx}/{ns}/{base_tbl}_sd",
            schema=_with_extra(base_schema, _SD_EXTRA),
            write_mode="soft_delete",
            purpose=purpose or f"{src} {base_tbl} soft_delete",
        ))
        # history_tracking → <table>_hist
        reg.append(dict(
            group="prod", source_key=src, catalog=cat, namespace=ns,
            table=f"{base_tbl}_hist", pk_col=pk,
            s3_prefix=f"{s3pfx}/{ns}/{base_tbl}_hist",
            schema=_with_extra(base_schema, _HIST_EXTRA),
            write_mode="history_tracking",
            purpose=purpose or f"{src} {base_tbl} history_tracking",
        ))

    # ── Production tables ───────────────────────────────────────────────────────

    # PostgreSQL
    _add_prod("postgres","postgres","cache_testing","customers",       "id",  "iceberg/pg_lakehouse",  _PG_CUSTOMERS)
    _add_prod("postgres","postgres","cache_testing","products",        "id",  "iceberg/pg_lakehouse",  _PG_PRODUCTS)
    _add_prod("postgres","postgres","cache_testing","product_reviews", "id",  "iceberg/pg_lakehouse",  _PG_PRODUCT_REVIEWS)
    _add_prod("postgres","postgres","cache_testing","orders",          "id",  "iceberg/pg_lakehouse",  _PG_ORDERS)

    # Oracle CACHE_TESTING
    _add_prod("oracle","oracle","cache_testing","customers",        "id","iceberg/ora_lakehouse",_ORA_CT_CUSTOMERS)
    _add_prod("oracle","oracle","cache_testing","products",         "id","iceberg/ora_lakehouse",_ORA_CT_PRODUCTS)
    _add_prod("oracle","oracle","cache_testing","orders",           "id","iceberg/ora_lakehouse",_ORA_CT_ORDERS)
    _add_prod("oracle","oracle","cache_testing","order_items",      "id","iceberg/ora_lakehouse",_ORA_CT_ORDER_ITEMS)
    _add_prod("oracle","oracle","cache_testing","product_reviews",  "id","iceberg/ora_lakehouse",_ORA_CT_PRODUCT_REVIEWS)
    _add_prod("oracle","oracle","cache_testing","inventory_events", "id","iceberg/ora_lakehouse",_ORA_CT_INVENTORY_EVENTS)

    # Oracle TPCDS
    _add_prod("oracle","oracle","tpcds","income_band",           "ib_income_band_sk", "iceberg/ora_lakehouse",_ORA_TPCDS_INCOME_BAND)
    _add_prod("oracle","oracle","tpcds","ship_mode",             "sm_ship_mode_sk",   "iceberg/ora_lakehouse",_ORA_TPCDS_SHIP_MODE)
    _add_prod("oracle","oracle","tpcds","warehouse",             "w_warehouse_sk",    "iceberg/ora_lakehouse",_ORA_TPCDS_WAREHOUSE)
    _add_prod("oracle","oracle","tpcds","reason",                "r_reason_sk",       "iceberg/ora_lakehouse",_ORA_TPCDS_REASON)
    _add_prod("oracle","oracle","tpcds","call_center",           "cc_call_center_sk", "iceberg/ora_lakehouse",_ORA_TPCDS_CALL_CENTER)
    _add_prod("oracle","oracle","tpcds","web_site",              "web_site_sk",       "iceberg/ora_lakehouse",_ORA_TPCDS_WEB_SITE)
    _add_prod("oracle","oracle","tpcds","web_page",              "wp_web_page_sk",    "iceberg/ora_lakehouse",_ORA_TPCDS_WEB_PAGE)
    _add_prod("oracle","oracle","tpcds","household_demographics","hd_demo_sk",        "iceberg/ora_lakehouse",_ORA_TPCDS_HOUSEHOLD_DEMOGRAPHICS)
    _add_prod("oracle","oracle","tpcds","catalog_page",          "cp_catalog_page_sk","iceberg/ora_lakehouse",_ORA_TPCDS_CATALOG_PAGE)
    _add_prod("oracle","oracle","tpcds","promotion",             "p_promo_sk",        "iceberg/ora_lakehouse",_ORA_TPCDS_PROMOTION)

    # MongoDB
    _add_prod("mongodb","mongodb","cache_testing","customers","_id","iceberg/mgo_lakehouse",_MGO_CUSTOMERS)
    _add_prod("mongodb","mongodb","cache_testing","products", "_id","iceberg/mgo_lakehouse",_MGO_PRODUCTS)

    # ── E2E test tables — all in a single namespace: e2e_testing ───────────────
    # Write-mode variants for each source table, all in <catalog>.e2e_testing.
    # Table suffix encodes the write mode:  _std | _sd | _hist
    # Safe to truncate / reset between test runs without touching production.

    for src, cat, base_tbl, pk, s3pfx, base_schema in [
        ("postgres","postgres","customers",       "id", "iceberg/pg_e2e",  _PG_CUSTOMERS),
        ("postgres","postgres","products",        "id", "iceberg/pg_e2e",  _PG_PRODUCTS),
        ("postgres","postgres","orders",          "id", "iceberg/pg_e2e",  _PG_ORDERS),
        ("oracle",  "oracle",  "customers",       "id", "iceberg/ora_e2e", _ORA_CT_CUSTOMERS),
        ("oracle",  "oracle",  "orders",          "id", "iceberg/ora_e2e", _ORA_CT_ORDERS),
        ("mongodb", "mongodb", "customers",       "_id","iceberg/mgo_e2e", _MGO_CUSTOMERS),
    ]:
        # standard
        reg.append(dict(
            group="test", source_key=src, catalog=cat, namespace=E2E_NS,
            table=f"{base_tbl}_std", pk_col=pk,
            s3_prefix=f"{s3pfx}/{E2E_NS}/{base_tbl}_std",
            schema=base_schema,
            write_mode="standard",
            purpose=f"[E2E] standard mode — {src}.{base_tbl}",
        ))
        # soft_delete
        reg.append(dict(
            group="test", source_key=src, catalog=cat, namespace=E2E_NS,
            table=f"{base_tbl}_sd", pk_col=pk,
            s3_prefix=f"{s3pfx}/{E2E_NS}/{base_tbl}_sd",
            schema=_with_extra(base_schema, _SD_EXTRA),
            write_mode="soft_delete",
            purpose=f"[E2E] soft_delete mode — {src}.{base_tbl}",
        ))
        # history_tracking
        reg.append(dict(
            group="test", source_key=src, catalog=cat, namespace=E2E_NS,
            table=f"{base_tbl}_hist", pk_col=pk,
            s3_prefix=f"{s3pfx}/{E2E_NS}/{base_tbl}_hist",
            schema=_with_extra(base_schema, _HIST_EXTRA),
            write_mode="history_tracking",
            purpose=f"[E2E] history_tracking mode — {src}.{base_tbl}",
        ))

    # ── StarTransform test tables — also in postgres.e2e_testing ───────────────
    # One table per StarTransform function.  All fed from postgres source tables.
    # Suffix encodes the transform being tested (see naming convention at top).

    # customers_dedup — deduplicate(pk="id", order_col="kafka_ts")
    reg.append(dict(
        group="transforms", source_key="postgres", catalog="postgres",
        namespace=E2E_NS, table="customers_dedup", pk_col="id",
        s3_prefix=f"iceberg/pg_e2e/{E2E_NS}/customers_dedup",
        schema=_PG_CUSTOMERS,
        write_mode="standard",
        purpose="[TRANSFORM] deduplicate() — last-write-wins per customer id",
    ))

    # customers_masked — mask_columns(["email","phone"])
    reg.append(dict(
        group="transforms", source_key="postgres", catalog="postgres",
        namespace=E2E_NS, table="customers_masked", pk_col="id",
        s3_prefix=f"iceberg/pg_e2e/{E2E_NS}/customers_masked",
        schema=_PG_CUSTOMERS,
        write_mode="standard",
        purpose="[TRANSFORM] mask_columns() — email + phone SHA-256 hashed",
    ))

    # customers_proc_time — add_processing_time(col_name="proc_time")
    reg.append(dict(
        group="transforms", source_key="postgres", catalog="postgres",
        namespace=E2E_NS, table="customers_proc_time", pk_col="id",
        s3_prefix=f"iceberg/pg_e2e/{E2E_NS}/customers_proc_time",
        schema=_with_extra(_PG_CUSTOMERS, [_S("proc_time", TimestampType(), True)]),
        write_mode="standard",
        purpose="[TRANSFORM] add_processing_time() — proc_time TIMESTAMP injected",
    ))

    # customers_op_label — add_op_label()
    reg.append(dict(
        group="transforms", source_key="postgres", catalog="postgres",
        namespace=E2E_NS, table="customers_op_label", pk_col="id",
        s3_prefix=f"iceberg/pg_e2e/{E2E_NS}/customers_op_label",
        schema=_with_extra(_PG_CUSTOMERS, [_S("op_label", StringType(), True)]),
        write_mode="standard",
        purpose="[TRANSFORM] add_op_label() — INSERT/UPDATE/DELETE string column",
    ))

    # customers_source_tag — add_source_tag(source_system="postgres")
    reg.append(dict(
        group="transforms", source_key="postgres", catalog="postgres",
        namespace=E2E_NS, table="customers_source_tag", pk_col="id",
        s3_prefix=f"iceberg/pg_e2e/{E2E_NS}/customers_source_tag",
        schema=_with_extra(_PG_CUSTOMERS, [_S("source_system", StringType(), True)]),
        write_mode="standard",
        purpose="[TRANSFORM] add_source_tag() — source_system STRING literal",
    ))

    # customers_filter_ins — filter_op(ops=["c","u"])  inserts + updates only
    reg.append(dict(
        group="transforms", source_key="postgres", catalog="postgres",
        namespace=E2E_NS, table="customers_filter_ins", pk_col="id",
        s3_prefix=f"iceberg/pg_e2e/{E2E_NS}/customers_filter_ins",
        schema=_PG_CUSTOMERS,
        write_mode="standard",
        purpose="[TRANSFORM] filter_op(['c','u']) — only inserts/updates land here",
    ))

    # customers_filter_del — filter_op(ops=["d"])  deletes only
    reg.append(dict(
        group="transforms", source_key="postgres", catalog="postgres",
        namespace=E2E_NS, table="customers_filter_del", pk_col="id",
        s3_prefix=f"iceberg/pg_e2e/{E2E_NS}/customers_filter_del",
        schema=_PG_CUSTOMERS,
        write_mode="standard",
        purpose="[TRANSFORM] filter_op(['d']) — only delete events land here",
    ))

    # orders_enriched — enrich_from_broadcast(products_dim, join_col="product_id")
    _ORDERS_ENRICHED = _with_extra(_PG_ORDERS, [
        _S("product_id",       LongType(),   True),   # join key
        _S("product_name",     StringType(), True),   # from products broadcast
        _S("product_category", StringType(), True),   # from products broadcast
    ])
    reg.append(dict(
        group="transforms", source_key="postgres", catalog="postgres",
        namespace=E2E_NS, table="orders_enriched", pk_col="id",
        s3_prefix=f"iceberg/pg_e2e/{E2E_NS}/orders_enriched",
        schema=_ORDERS_ENRICHED,
        write_mode="standard",
        purpose="[TRANSFORM] enrich_from_broadcast() — orders joined with products dim",
    ))

    # customers_before_after — pivot_before_after() on history_tracking stream
    _BEFORE_AFTER = StructType([
        _S("_change_type",  StringType(),    True),
        _S("_change_ts",    TimestampType(), True),
        _S("before_id",     LongType(),      True),
        _S("before_name",   StringType(),    True),
        _S("before_email",  StringType(),    True),
        _S("before_status", StringType(),    True),
        _S("after_id",      LongType(),      True),
        _S("after_name",    StringType(),    True),
        _S("after_email",   StringType(),    True),
        _S("after_status",  StringType(),    True),
    ])
    reg.append(dict(
        group="transforms", source_key="postgres", catalog="postgres",
        namespace=E2E_NS, table="customers_before_after", pk_col="after_id",
        s3_prefix=f"iceberg/pg_e2e/{E2E_NS}/customers_before_after",
        schema=_BEFORE_AFTER,
        write_mode="history_tracking",
        purpose="[TRANSFORM] pivot_before_after() — before_* + after_* side by side",
    ))

    # customers_nullcoal — null_coalesce({"country": "N/A", "phone": "UNKNOWN"})
    reg.append(dict(
        group="transforms", source_key="postgres", catalog="postgres",
        namespace=E2E_NS, table="customers_nullcoal", pk_col="id",
        s3_prefix=f"iceberg/pg_e2e/{E2E_NS}/customers_nullcoal",
        schema=_PG_CUSTOMERS,
        write_mode="standard",
        purpose="[TRANSFORM] null_coalesce() — NULL country/phone replaced with defaults",
    ))

    # event_counts — aggregate_counts(pk_col="id", op_col="_op", out_col="event_count")
    _EVENT_COUNTS = StructType([
        _S("id",          LongType(),   False),
        _S("_op",         StringType(), True),
        _S("event_count", LongType(),   True),
    ])
    reg.append(dict(
        group="transforms", source_key="postgres", catalog="postgres",
        namespace=E2E_NS, table="event_counts", pk_col="id",
        s3_prefix=f"iceberg/pg_e2e/{E2E_NS}/event_counts",
        schema=_EVENT_COUNTS,
        write_mode="standard",
        purpose="[TRANSFORM] aggregate_counts() — events per (customer_id, op)",
    ))

    return reg


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    os.environ["SPARK_USER"] = SPARK_USER

    logger.info(
        "=== Create Iceberg Tables | user=%s | dry_run=%s | source=%s | group=%s ===",
        SPARK_USER, DRY_RUN, SOURCE_FILTER or "all", TABLE_GROUP,
    )

    bao   = BaoSparkInit()
    conf  = bao.spark_conf(app_name="create-iceberg-tables")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    builder = IcebergTableBuilder(spark, running_user=SPARK_USER)

    registry = _build_registry()

    # Apply filters
    if SOURCE_FILTER:
        registry = [r for r in registry if r["source_key"] == SOURCE_FILTER]
    if TABLE_GROUP != "all":
        registry = [r for r in registry if r["group"] == TABLE_GROUP]

    # Ensure namespaces
    seen_ns: set[tuple] = set()
    for r in registry:
        key = (r["catalog"], r["namespace"])
        if key not in seen_ns:
            if not DRY_RUN:
                builder.ensure_namespace(r["catalog"], r["namespace"])
            else:
                logger.info("[DRY_RUN] namespace `%s`.`%s`", r["catalog"], r["namespace"])
            seen_ns.add(key)

    created = skipped = errors = 0

    for r in registry:
        cat, ns, tbl = r["catalog"], r["namespace"], r["table"]
        pk_col       = r["pk_col"]
        schema       = r["schema"]
        location     = f"s3://{S3_BUCKET}/{r['s3_prefix']}"
        fqn          = f"`{cat}`.`{ns}`.`{tbl}`"

        if builder.table_exists(cat, ns, tbl):
            logger.info("  [EXISTS]  %-60s  %s", fqn, r["purpose"])
            skipped += 1
            continue

        partition_spec = [
            IcebergTableBuilder.hours("snap_timestamp"),
            IcebergTableBuilder.bucket(pk_col, 16),
        ]

        if DRY_RUN:
            aug = StructType(schema.fields + [
                _S("snap_id",        LongType(),      True),
                _S("snap_timestamp", TimestampType(), True),
            ])
            col_ddl = "\n".join(f"    {f.name} {f.dataType.simpleString()}" for f in aug.fields)
            logger.info(
                "[DRY_RUN] CREATE TABLE %s (\n%s\n)"
                "\n  PARTITIONED BY (hours(snap_timestamp), bucket(16, %s))"
                "\n  LOCATION '%s'  -- %s",
                fqn, col_ddl, pk_col, location, r["purpose"],
            )
            created += 1
            continue

        try:
            builder.create_table(
                catalog          = cat,
                namespace        = ns,
                table            = tbl,
                schema           = schema,
                partition_spec   = partition_spec,
                location         = location,
                extra_properties = {
                    "pipeline.write-mode": r["write_mode"],
                    "pipeline.source":     r["source_key"],
                    "pipeline.group":      r["group"],
                    "pipeline.pk-col":     pk_col,
                    "pipeline.purpose":    r["purpose"],
                },
            )
            logger.info("  [CREATED] %-60s  %s", fqn, r["purpose"])
            created += 1
        except Exception as exc:
            logger.error("  [ERROR]   %-60s  %s", fqn, exc)
            errors += 1

    # ── Summary ────────────────────────────────────────────────────────────────
    logger.info("")
    logger.info("═" * 70)
    logger.info("  Group filter   : %s", TABLE_GROUP)
    logger.info("  Source filter  : %s", SOURCE_FILTER or "all")
    logger.info("  Total entries  : %d", len(registry))
    logger.info("  Created        : %d", created)
    logger.info("  Already exist  : %d (skipped)", skipped)
    logger.info("  Errors         : %d", errors)
    logger.info("═" * 70)
    logger.info("")
    logger.info("  Table groups in this run:")
    for grp in sorted({r["group"] for r in registry}):
        ns_list = sorted({f"`{r['catalog']}`.`{r['namespace']}`" for r in registry if r["group"] == grp})
        logger.info("    %-12s → namespaces: %s", grp, ", ".join(ns_list))

    spark.stop()
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
