#!/usr/bin/env python3
"""
stargraph_iceberg_engine.py
============================
DuckDB/Iceberg → Memgraph (Stargraph) loader.

Reads data from Iceberg tables via DuckDB and loads it into Memgraph
as graph nodes and edges using Cypher MERGE statements over Bolt.

Source (Iceberg via DuckDB):
  polaris.tpcds_sf10tcl.customer      → (:Customer) nodes
  polaris.tpcds_sf10tcl.item          → (:Item) nodes
  polaris.tpcds_sf10tcl.store_sales   → (:Customer)-[:PURCHASED]->(:Item) edges
  polaris.tpcds_sf10tcl.store         → (:Store) nodes
  polaris.tpcds_sf10tcl.store_sales   → (:Customer)-[:BOUGHT_AT]->(:Store) edges

Target (Memgraph via Bolt):
  bolt://192.168.1.58:7687

Usage:
  # Full initial load
  python3 stargraph_iceberg_engine.py --mode full

  # Incremental (watermark-based, checks snap_timestamp)
  python3 stargraph_iceberg_engine.py --mode incremental

  # Single target
  python3 stargraph_iceberg_engine.py --mode full --target customers
"""

import argparse
import logging
import multiprocessing
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from neo4j import GraphDatabase, Driver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("iceberg-loader")

# ── Configuration ─────────────────────────────────────────────────────────────
MEMGRAPH_HOST  = os.getenv("MEMGRAPH_HOST", "192.168.1.58")
MEMGRAPH_PORT  = int(os.getenv("MEMGRAPH_PORT", "7687"))
MEMGRAPH_USER  = os.getenv("MEMGRAPH_USER", "")
MEMGRAPH_PASS  = os.getenv("MEMGRAPH_PASS", "")

DUCKDB_DB      = os.getenv("DUCKDB_DATABASE",   "/data/stargraph_iceberg.duckdb")
POLARIS_URI    = os.getenv("POLARIS_URI",        "http://polaris-auth-proxy.prod.svc.cluster.local:8283/api/catalog")
POLARIS_WH     = os.getenv("POLARIS_WAREHOUSE",  "IcebergCatalog")
S3_KEY         = os.getenv("AWS_ACCESS_KEY_ID",     "")
S3_SECRET      = os.getenv("AWS_SECRET_ACCESS_KEY", "")
S3_REGION      = os.getenv("AWS_DEFAULT_REGION",    "us-east-2")

BATCH_SIZE     = int(os.getenv("ICEBERG_BATCH_SIZE", "500"))
WATERMARK_DIR  = Path(os.getenv("WATERMARK_DIR", "/data/watermarks"))
WATERMARK_DIR.mkdir(parents=True, exist_ok=True)

# Source schema
SRC = "polaris.tpcds_sf10tcl"


# ── DuckDB connection ─────────────────────────────────────────────────────────

def make_duckdb_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(DUCKDB_DB)
    for ext in ("iceberg", "httpfs", "aws"):
        conn.execute(f"LOAD {ext}")
    conn.execute(f"""
        CREATE OR REPLACE SECRET s3_hmac (
            TYPE      s3,
            KEY_ID    '{S3_KEY}',
            SECRET    '{S3_SECRET}',
            REGION    '{S3_REGION}',
            ENDPOINT  's3.{S3_REGION}.amazonaws.com',
            URL_STYLE 'path'
        )
    """)
    threads = max(2, multiprocessing.cpu_count() - 1)
    conn.execute(f"PRAGMA threads={threads}")
    conn.execute( "PRAGMA memory_limit='4GB'")
    conn.execute(f"""
        ATTACH '{POLARIS_WH}' AS polaris (
            TYPE               iceberg,
            URI                '{POLARIS_URI}',
            AUTHORIZATION_TYPE 'none'
        )
    """)
    log.info("DuckDB connected to Polaris catalog")
    return conn


# ── Memgraph Bolt connection ──────────────────────────────────────────────────

def make_driver() -> Driver:
    uri  = f"bolt://{MEMGRAPH_HOST}:{MEMGRAPH_PORT}"
    auth = (MEMGRAPH_USER, MEMGRAPH_PASS) if MEMGRAPH_USER else ("", "")
    driver = GraphDatabase.driver(uri, auth=auth)
    driver.verify_connectivity()
    log.info("Connected to Memgraph at %s", uri)
    return driver


# ── Watermark helpers ─────────────────────────────────────────────────────────

def load_watermark(name: str) -> str | None:
    wf = WATERMARK_DIR / f"{name}.watermark"
    return wf.read_text().strip() if wf.exists() else None


def save_watermark(name: str, ts: str) -> None:
    (WATERMARK_DIR / f"{name}.watermark").write_text(ts)


# ── Memgraph batch writer ─────────────────────────────────────────────────────

def run_cypher_batch(driver: Driver, cypher: str, batch: list[dict]) -> None:
    """Execute a Cypher statement for each row in the batch using UNWIND."""
    with driver.session() as session:
        session.run(f"UNWIND $rows AS row {cypher}", rows=batch)


# ── Loaders ──────────────────────────────────────────────────────────────────

def load_customers(conn: duckdb.DuckDBPyConnection, driver: Driver, mode: str) -> None:
    """
    Read Customer rows from Iceberg, MERGE into Memgraph as (:Customer) nodes.
    """
    wm = load_watermark("customers") if mode == "incremental" else None
    where = f"WHERE snap_timestamp > TIMESTAMPTZ '{wm}'" if wm else ""

    sql = f"""
        SELECT
            CAST(C_CUSTOMER_SK AS VARCHAR)  AS customer_sk,
            C_CUSTOMER_ID                   AS customer_id,
            C_FIRST_NAME                    AS first_name,
            C_LAST_NAME                     AS last_name,
            C_EMAIL_ADDRESS                 AS email,
            C_BIRTH_COUNTRY                 AS country,
            C_PREFERRED_CUST_FLAG           AS preferred,
            CAST(C_BIRTH_YEAR AS VARCHAR)   AS birth_year,
            CAST(snap_timestamp AS VARCHAR) AS snap_ts
        FROM {SRC}.customer
        {where}
        ORDER BY snap_timestamp
    """

    cypher = """
        MERGE (c:Customer {customer_sk: row.customer_sk})
        SET c.customer_id  = row.customer_id,
            c.first_name   = row.first_name,
            c.last_name    = row.last_name,
            c.email        = row.email,
            c.country      = row.country,
            c.preferred    = row.preferred,
            c.birth_year   = row.birth_year,
            c.updated_at   = row.snap_ts
    """

    _batch_load(conn, driver, sql, cypher, "customers")


def load_items(conn: duckdb.DuckDBPyConnection, driver: Driver, mode: str) -> None:
    """
    Read Item rows from Iceberg, MERGE into Memgraph as (:Item) nodes.
    """
    wm = load_watermark("items") if mode == "incremental" else None
    where = f"WHERE snap_timestamp > TIMESTAMPTZ '{wm}'" if wm else ""

    sql = f"""
        SELECT
            CAST(I_ITEM_SK AS VARCHAR)      AS item_sk,
            I_ITEM_ID                       AS item_id,
            I_ITEM_DESC                     AS description,
            I_CURRENT_PRICE                 AS price,
            I_CLASS                         AS class,
            I_CATEGORY                      AS category,
            I_BRAND                         AS brand,
            CAST(snap_timestamp AS VARCHAR) AS snap_ts
        FROM {SRC}.item
        {where}
        ORDER BY snap_timestamp
    """

    cypher = """
        MERGE (i:Item {item_sk: row.item_sk})
        SET i.item_id     = row.item_id,
            i.description = row.description,
            i.price       = row.price,
            i.class       = row.class,
            i.category    = row.category,
            i.brand       = row.brand,
            i.updated_at  = row.snap_ts
    """

    _batch_load(conn, driver, sql, cypher, "items")


def load_stores(conn: duckdb.DuckDBPyConnection, driver: Driver, mode: str) -> None:
    """
    Read Store rows from Iceberg, MERGE into Memgraph as (:Store) nodes.
    """
    wm = load_watermark("stores") if mode == "incremental" else None
    where = f"WHERE snap_timestamp > TIMESTAMPTZ '{wm}'" if wm else ""

    sql = f"""
        SELECT
            CAST(S_STORE_SK AS VARCHAR)     AS store_sk,
            S_STORE_ID                      AS store_id,
            S_STORE_NAME                    AS name,
            S_CITY                          AS city,
            S_STATE                         AS state,
            S_COUNTRY                       AS country,
            CAST(snap_timestamp AS VARCHAR) AS snap_ts
        FROM {SRC}.store
        {where}
        ORDER BY snap_timestamp
    """

    cypher = """
        MERGE (s:Store {store_sk: row.store_sk})
        SET s.store_id   = row.store_id,
            s.name       = row.name,
            s.city       = row.city,
            s.state      = row.state,
            s.country    = row.country,
            s.updated_at = row.snap_ts
    """

    _batch_load(conn, driver, sql, cypher, "stores")


def load_purchases(conn: duckdb.DuckDBPyConnection, driver: Driver, mode: str) -> None:
    """
    Read store_sales from Iceberg, create (:Customer)-[:PURCHASED]->(:Item)
    and (:Customer)-[:BOUGHT_AT]->(:Store) edges in Memgraph.
    Nodes must already exist (run load_customers/items/stores first).
    """
    wm = load_watermark("purchases") if mode == "incremental" else None
    where = f"WHERE snap_timestamp > TIMESTAMPTZ '{wm}'" if wm else ""

    sql = f"""
        SELECT
            CAST(SS_CUSTOMER_SK AS VARCHAR)  AS customer_sk,
            CAST(SS_ITEM_SK AS VARCHAR)      AS item_sk,
            CAST(SS_STORE_SK AS VARCHAR)     AS store_sk,
            CAST(SS_TICKET_NUMBER AS VARCHAR) AS ticket,
            SS_QUANTITY                      AS quantity,
            SS_SALES_PRICE                   AS price,
            SS_NET_PROFIT                    AS profit,
            CAST(snap_timestamp AS VARCHAR)  AS snap_ts
        FROM {SRC}.store_sales
        WHERE SS_CUSTOMER_SK IS NOT NULL
          AND SS_ITEM_SK     IS NOT NULL
        {('AND ' + where.replace('WHERE ','')) if wm else ''}
        ORDER BY snap_timestamp
    """

    cypher_purchased = """
        MATCH (c:Customer {customer_sk: row.customer_sk})
        MATCH (i:Item     {item_sk:     row.item_sk})
        MERGE (c)-[r:PURCHASED {ticket: row.ticket}]->(i)
        SET r.quantity   = row.quantity,
            r.price      = row.price,
            r.profit     = row.profit,
            r.updated_at = row.snap_ts
    """

    cypher_bought_at = """
        MATCH (c:Customer {customer_sk: row.customer_sk})
        MATCH (s:Store    {store_sk:    row.store_sk})
        MERGE (c)-[r:BOUGHT_AT {ticket: row.ticket}]->(s)
        SET r.updated_at = row.snap_ts
    """

    log.info("Loading PURCHASED edges ...")
    _batch_load(conn, driver, sql, cypher_purchased, "purchases")
    log.info("Loading BOUGHT_AT edges ...")
    # Re-query for store edges (store_sk may be NULL for some rows)
    sql_store = sql.replace(
        "AND SS_ITEM_SK     IS NOT NULL",
        "AND SS_ITEM_SK     IS NOT NULL\n          AND SS_STORE_SK    IS NOT NULL"
    )
    _batch_load(conn, driver, sql_store, cypher_bought_at, None)


# ── Core batch loop ───────────────────────────────────────────────────────────

def _batch_load(
    conn: duckdb.DuckDBPyConnection,
    driver: Driver,
    sql: str,
    cypher: str,
    watermark_name: str | None,
) -> None:
    """
    Execute SQL via DuckDB, stream results in batches, write each batch
    to Memgraph via Cypher UNWIND.
    """
    rel     = conn.execute(sql)
    cols    = [d[0] for d in rel.description]
    total   = 0
    last_ts = None

    while True:
        rows = rel.fetchmany(BATCH_SIZE)
        if not rows:
            break
        batch = [dict(zip(cols, row)) for row in rows]
        run_cypher_batch(driver, cypher, batch)
        total += len(batch)
        # Track last snap_ts for watermark
        if watermark_name and "snap_ts" in batch[-1]:
            last_ts = batch[-1]["snap_ts"]
        if total % 10000 == 0:
            log.info("  ... %d rows written", total)

    if watermark_name and last_ts:
        save_watermark(watermark_name, last_ts)

    log.info("Done — %d rows → Memgraph", total)


# ── Main ──────────────────────────────────────────────────────────────────────

TARGETS = {
    "customers": load_customers,
    "items":     load_items,
    "stores":    load_stores,
    "purchases": load_purchases,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="DuckDB/Iceberg → Memgraph loader")
    parser.add_argument("--mode",   choices=["full", "incremental"], default="incremental")
    parser.add_argument("--target", default=None,
                        help="Single target: customers|items|stores|purchases (default: all in order)")
    args = parser.parse_args()

    # Purchases must come after nodes are loaded
    run_order = ["customers", "items", "stores", "purchases"]
    targets   = [args.target] if args.target else run_order

    log.info("DuckDB/Iceberg → Memgraph loader  mode=%s  targets=%s", args.mode, targets)
    t0 = time.perf_counter()

    conn   = make_duckdb_conn()
    driver = make_driver()
    try:
        for target in targets:
            if target not in TARGETS:
                log.error("Unknown target: %s", target)
                continue
            log.info("── Loading %s ──", target)
            TARGETS[target](conn, driver, args.mode)
    finally:
        conn.close()
        driver.close()

    log.info("All targets loaded in %.1fs", time.perf_counter() - t0)


if __name__ == "__main__":
    main()
