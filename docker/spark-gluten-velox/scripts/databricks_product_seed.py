#!/usr/bin/env python3
"""
databricks_product_seed.py
==========================
Create the `product` table in Databricks Unity Catalog
(lakehouse.lakehouse_db.product) and insert 500 synthetic product rows.

This script runs against Databricks via JDBC — it does NOT use a Spark
cluster.  It connects directly to the Databricks SQL Warehouse using the
Simba JDBC driver through the standard Python JDBC bridge (jaydebeapi) or,
alternatively, the databricks-sql-connector Python package.

Since this script's only job is to CREATE TABLE + INSERT rows in Databricks
(not write to Iceberg), it uses the databricks-sql-connector package which
is lighter than spinning up a full Spark session.

Pipeline
--------
  1. Fetch Databricks credentials from OpenBao.
  2. Connect to the Databricks SQL Warehouse via databricks-sql-connector.
  3. CREATE TABLE IF NOT EXISTS lakehouse.lakehouse_db.product.
  4. INSERT 500 synthetic product rows in batches of 100.
  5. SELECT COUNT(*) to confirm.

Product schema
--------------
  product_id        INT           NOT NULL   — unique product identifier
  product_name      STRING                   — e.g. "Wireless Headphones Pro"
  category          STRING                   — Electronics / Clothing / etc.
  sub_category      STRING                   — e.g. "Audio"
  brand             STRING                   — e.g. "SoundWave"
  sku               STRING                   — e.g. "SKU-00042-ABCD"
  unit_price        DOUBLE                   — e.g. 149.99
  cost_price        DOUBLE                   — e.g. 62.50
  stock_quantity    INT                      — e.g. 320
  reorder_level     INT                      — e.g. 50
  weight_kg         DOUBLE                   — e.g. 0.45
  is_active         INT                      — 1 / 0
  created_at        TIMESTAMP
  updated_at        TIMESTAMP
  snap_id           BIGINT                   — NULL here; starpump injects on copy
  snap_timestamp    TIMESTAMP                — NULL here; starpump injects on copy

Running
-------
  # From any machine with Python + databricks-sql-connector installed:
  pip install databricks-sql-connector

  TOKEN=<openbao-root-token> python3 databricks_product_seed.py

  # Environment overrides (all optional):
  ROWS=500        rows to insert (default: 500)
  DRY_RUN=1       print DDL + first batch only, do not execute
"""

from __future__ import annotations

import datetime
import hashlib
import logging
import os
import random

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("product-seed")

# ── Configuration ─────────────────────────────────────────────────────────────
CATALOG   = "lakehouse"
SCHEMA    = "lakehouse_db"
TABLE     = "product"
ROWS      = int(os.environ.get("ROWS", "500"))
DRY_RUN   = os.environ.get("DRY_RUN", "0") == "1"
BATCH_SIZE = 100

# ── OpenBao credential fetch ──────────────────────────────────────────────────
import json, urllib.request

def _bao(path: str, field: str) -> str:
    token = os.environ.get("TOKEN") or os.environ.get("BAO_TOKEN")
    if not token:
        raise RuntimeError("Set TOKEN env-var to your OpenBao root/bootstrap token.")
    req = urllib.request.Request(
        f"http://openbao.prod.svc.cluster.local:8200/v1/{path}",
        headers={"X-Vault-Token": token},
    )
    data = json.loads(urllib.request.urlopen(req, timeout=10).read())
    return data["data"]["data"][field]

# ── Synthetic data fixtures ───────────────────────────────────────────────────
_CATEGORIES = {
    "Electronics":  ["Audio", "Cameras", "Laptops", "Phones", "Tablets", "Wearables"],
    "Clothing":     ["Men", "Women", "Kids", "Sportswear", "Footwear"],
    "Home":         ["Kitchen", "Furniture", "Bedding", "Decor", "Garden"],
    "Sports":       ["Outdoor", "Fitness", "Cycling", "Swimming", "Team Sports"],
    "Books":        ["Fiction", "Science", "History", "Children", "Cooking"],
    "Toys":         ["Board Games", "Action Figures", "Puzzles", "Outdoor Play", "Educational"],
    "Beauty":       ["Skincare", "Haircare", "Fragrance", "Makeup", "Wellness"],
    "Automotive":   ["Parts", "Tools", "Accessories", "Care", "Safety"],
}

_BRANDS = [
    "NovaTech", "SoundWave", "BrightLife", "AeroFit", "PineCrest",
    "UrbanEdge", "SwiftLine", "PureForm", "EcoWave", "StarCore",
    "BluePeak", "IronCraft", "SilverLeaf", "TerraGear", "ZenFlow",
]

_ADJECTIVES = [
    "Pro", "Ultra", "Max", "Lite", "Plus", "Elite", "Prime", "Sport",
    "Smart", "Flex", "Boost", "Core", "Edge", "Swift", "Nano",
]

_NOUNS = [
    "Headphones", "Backpack", "Watch", "Sneakers", "Jacket", "Desk Lamp",
    "Water Bottle", "Keyboard", "Yoga Mat", "Coffee Maker", "Camera",
    "Notebook", "Tent", "Blender", "Monitor", "Helmet", "Sunglasses",
    "Gloves", "Speaker", "Charger",
]


def _gen_rows(n: int) -> list[dict]:
    """Generate *n* deterministic synthetic product rows."""
    rng  = random.Random(77)
    rows = []
    base_dt = datetime.datetime(2025, 1, 1, 0, 0, 0)
    categories = list(_CATEGORIES.keys())

    for i in range(1, n + 1):
        cat      = rng.choice(categories)
        sub_cat  = rng.choice(_CATEGORIES[cat])
        brand    = rng.choice(_BRANDS)
        adj      = rng.choice(_ADJECTIVES)
        noun     = rng.choice(_NOUNS)
        name     = f"{brand} {noun} {adj}"
        tag      = hashlib.md5(f"{name}{i}".encode()).hexdigest()[:4].upper()
        sku      = f"SKU-{i:05d}-{tag}"
        unit_price = round(rng.uniform(5.0, 999.99), 2)
        cost_price = round(unit_price * rng.uniform(0.3, 0.65), 2)
        stock      = rng.randint(0, 2000)
        reorder    = rng.randint(10, 200)
        weight     = round(rng.uniform(0.05, 25.0), 3)
        created    = base_dt + datetime.timedelta(days=rng.randint(0, 500))
        updated    = created  + datetime.timedelta(days=rng.randint(0, 60))

        rows.append({
            "product_id":     i,
            "product_name":   name,
            "category":       cat,
            "sub_category":   sub_cat,
            "brand":          brand,
            "sku":            sku,
            "unit_price":     unit_price,
            "cost_price":     cost_price,
            "stock_quantity": stock,
            "reorder_level":  reorder,
            "weight_kg":      weight,
            "is_active":      1,
            "created_at":     created.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at":     updated.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return rows


_CREATE_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{TABLE}` (
    product_id        INT           NOT NULL,
    product_name      STRING,
    category          STRING,
    sub_category      STRING,
    brand             STRING,
    sku               STRING,
    unit_price        DOUBLE,
    cost_price        DOUBLE,
    stock_quantity    INT,
    reorder_level     INT,
    weight_kg         DOUBLE,
    is_active         INT,
    created_at        TIMESTAMP,
    updated_at        TIMESTAMP,
    snap_id           BIGINT,
    snap_timestamp    TIMESTAMP
)
USING DELTA
COMMENT 'Product catalogue — source table for starpump Iceberg copy'
"""

_INSERT_SQL = f"""
INSERT INTO `{CATALOG}`.`{SCHEMA}`.`{TABLE}`
  (product_id, product_name, category, sub_category, brand, sku,
   unit_price, cost_price, stock_quantity, reorder_level, weight_kg,
   is_active, created_at, updated_at, snap_id, snap_timestamp)
VALUES
"""


def main() -> None:
    logger.info(
        "=== product-seed | catalog=%s schema=%s table=%s rows=%d ===",
        CATALOG, SCHEMA, TABLE, ROWS,
    )

    # ── Fetch Databricks creds from OpenBao ───────────────────────────────────
    host      = _bao("secret/data/platform/databricks", "host")
    http_path = _bao("secret/data/platform/databricks", "http_path")
    token     = _bao("secret/data/platform/databricks", "token")

    logger.info("Credentials loaded — host=%s", host)

    if DRY_RUN:
        logger.info("DRY_RUN=1 — printing DDL only, not executing.")
        print(_CREATE_TABLE_DDL)
        rows = _gen_rows(BATCH_SIZE)
        vals = _build_values(rows)
        print(_INSERT_SQL + vals + ";")
        return

    # ── Connect via databricks-sql-connector ──────────────────────────────────
    try:
        from databricks import sql as dbsql
    except ImportError:
        raise SystemExit(
            "databricks-sql-connector not installed.\n"
            "Run: pip install databricks-sql-connector"
        )

    with dbsql.connect(
        server_hostname = host,
        http_path       = http_path,
        access_token    = token,
    ) as conn:
        with conn.cursor() as cur:

            # (1) Create schema if missing
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
            logger.info("Schema `%s`.`%s` ready.", CATALOG, SCHEMA)

            # (2) Create table
            cur.execute(_CREATE_TABLE_DDL)
            logger.info("Table `%s`.`%s`.`%s` ready.", CATALOG, SCHEMA, TABLE)

            # (3) Insert rows in batches
            rows = _gen_rows(ROWS)
            total_inserted = 0
            for start in range(0, len(rows), BATCH_SIZE):
                batch = rows[start : start + BATCH_SIZE]
                vals  = _build_values(batch)
                cur.execute(_INSERT_SQL + vals)
                total_inserted += len(batch)
                logger.info(
                    "Inserted batch offset=%d size=%d  (total=%d)",
                    start, len(batch), total_inserted,
                )

            # (4) Confirm
            cur.execute(
                f"SELECT COUNT(*) AS total FROM `{CATALOG}`.`{SCHEMA}`.`{TABLE}`"
            )
            result = cur.fetchone()
            logger.info("✅ Total rows in %s.%s.%s: %s", CATALOG, SCHEMA, TABLE, result[0])


def _build_values(rows: list[dict]) -> str:
    """Build the VALUES (...), (...) string for a batch INSERT."""
    parts = []
    for r in rows:
        parts.append(
            f"({r['product_id']}, "
            f"'{_esc(r['product_name'])}', "
            f"'{r['category']}', "
            f"'{r['sub_category']}', "
            f"'{r['brand']}', "
            f"'{r['sku']}', "
            f"{r['unit_price']}, "
            f"{r['cost_price']}, "
            f"{r['stock_quantity']}, "
            f"{r['reorder_level']}, "
            f"{r['weight_kg']}, "
            f"{r['is_active']}, "
            f"TIMESTAMP'{r['created_at']}', "
            f"TIMESTAMP'{r['updated_at']}', "
            f"NULL, NULL)"
        )
    return ",\n".join(parts)


def _esc(s: str) -> str:
    """Escape single quotes for SQL string literals."""
    return s.replace("'", "''")


if __name__ == "__main__":
    main()
