# Databricks notebook source
# nb_multi_table_auto_reader.py
#
# PURPOSE  : Single automated notebook that reads parquet data from ANY number
#            of Iceberg S3 folders.  For each configured table it:
#              1. Lists the metadata/ prefix and picks the newest *.metadata.json
#              2. Parses the JSON to confirm the active snapshot (diagnostic only)
#              3. Creates the view ONCE via CREATE VIEW IF NOT EXISTS — never
#                 replaced again, so in-flight user queries are never interrupted
#              4. Atomically overwrites the Delta audit table (INSERT OVERWRITE)
#                 so users querying the Delta table see no gap (Delta MVCC)
#
# ZERO-DOWNTIME DESIGN
# ─────────────────────
# The view points at the whole  data/  directory, not at a specific snapshot
# file.  Iceberg always appends new *.parquet files into that same directory,
# so read_files() automatically includes them on the next query — no DDL
# change to the view is ever needed after the initial creation.
#
# CREATE VIEW IF NOT EXISTS  (Cell 4) → only fires on first run per table.
# INSERT OVERWRITE Delta      (Cell 5) → single atomic transaction; concurrent
#   reads see the old data until the commit, then instantly see the new data.
#
# TO ADD A NEW TABLE: add one entry to TABLE_CONFIGS below, nothing else.
#
# CATALOG  : lakehouse  (Unity Catalog)
# SCHEMA   : lakehouse.lakehouse_db

# COMMAND ----------

# =============================================================================
# Cell 1 — Configuration: all Iceberg tables to refresh
# =============================================================================
# Each entry is:
#   "logical_name": {
#       "meta_path"  : S3 URI of the metadata/ directory (trailing slash required).
#                      Used to resolve the latest snapshot for diagnostics and for
#                      refreshing the Delta audit table.
#       "data_path"  : S3 URI of the data/ directory (stable, never changes).
#                      The VIEW is permanently pointed here — read_files() on a
#                      directory glob picks up every new *.parquet file on each
#                      query without any DDL change to the view.
#       "view"       : fully-qualified Databricks view name (created once, never
#                      replaced — zero downtime for concurrent users).
#       "audit_tbl"  : fully-qualified Delta table name (overwritten atomically
#                      on every refresh — no read gap due to Delta MVCC).
#   }
#
# Add or remove entries here to control which tables are refreshed.

TABLE_CONFIGS = {
    "customer": {
        "meta_path"  : "s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/metadata/",
        "data_path"  : "s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/data/",
        "view"       : "lakehouse.lakehouse_db.vw_customer_latest",
        "audit_tbl"  : "lakehouse.lakehouse_db.customer_snapshot_audit",
    },
    "customer_orders": {
        "meta_path"  : "s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer_orders/metadata/",
        "data_path"  : "s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer_orders/data/",
        "view"       : "lakehouse.lakehouse_db.vw_customer_orders_latest",
        "audit_tbl"  : "lakehouse.lakehouse_db.customer_orders_snapshot_audit",
    },
    # ── add more tables below ──────────────────────────────────────────────
    # "product": {
    #     "meta_path"  : "s3://stardata-databricks/iceberg/warehouse/lakehouse_db/product/metadata/",
    #     "data_path"  : "s3://stardata-databricks/iceberg/warehouse/lakehouse_db/product/data/",
    #     "view"       : "lakehouse.lakehouse_db.vw_product_latest",
    #     "audit_tbl"  : "lakehouse.lakehouse_db.product_snapshot_audit",
    # },
}

CATALOG_SCHEMA = "lakehouse.lakehouse_db"

print(f"Tables configured : {list(TABLE_CONFIGS.keys())}")
print(f"Target schema     : {CATALOG_SCHEMA}")

# COMMAND ----------

# =============================================================================
# Cell 2 — Helper: resolve the latest metadata JSON for one table
# =============================================================================
# Returns a dict with data_path, snapshot_id, last_updated_ts, and the
# raw metadata dict.  Raises RuntimeError if no metadata files are found.

import json, datetime

def resolve_latest_snapshot(table_name: str, meta_path: str) -> dict:
    """
    List *.metadata.json files under meta_path, pick the one with the
    highest modificationTime, parse it, and return the resolved data path
    plus diagnostic fields.
    """
    all_files  = dbutils.fs.ls(meta_path)
    meta_files = [f for f in all_files if f.name.endswith(".metadata.json")]

    if not meta_files:
        raise RuntimeError(
            f"[{table_name}] No *.metadata.json files found under {meta_path}"
        )

    meta_files.sort(key=lambda f: f.modificationTime, reverse=True)
    latest = meta_files[0]

    raw       = spark.read.text(latest.path, wholetext=True).collect()[0][0]
    meta      = json.loads(raw)

    data_path    = meta["location"].rstrip("/") + "/data/"
    snapshot_id  = meta.get("current-snapshot-id", "unknown")
    last_updated = meta.get("last-updated-ms", 0)
    ts = (
        datetime.datetime.utcfromtimestamp(last_updated / 1000)
        .strftime("%Y-%m-%d %H:%M:%S UTC")
        if last_updated else "unknown"
    )

    print(
        f"  [{table_name}] {len(meta_files)} snapshot(s) found\n"
        f"    Latest file  : {latest.name}\n"
        f"    Snapshot ID  : {snapshot_id}\n"
        f"    Last updated : {ts}\n"
        f"    Data path    : {data_path}"
    )
    return {
        "table_name"  : table_name,
        "data_path"   : data_path,
        "snapshot_id" : snapshot_id,
        "last_updated": ts,
        "meta_name"   : latest.name,
    }

print("✅ Helper function defined")

# COMMAND ----------

# =============================================================================
# Cell 3 — Resolve latest snapshot for every configured table
# =============================================================================
# Loops over TABLE_CONFIGS and calls resolve_latest_snapshot() for each.
# Results are collected into SNAPSHOTS so later cells can use them without
# re-reading S3.

print("─" * 60)
print("Resolving latest Iceberg snapshots …")
print("─" * 60)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_SCHEMA}")

SNAPSHOTS = {}
errors    = []

for tbl, cfg in TABLE_CONFIGS.items():
    try:
        SNAPSHOTS[tbl] = {**cfg, **resolve_latest_snapshot(tbl, cfg["meta_path"])}
        print()
    except Exception as exc:
        errors.append((tbl, str(exc)))
        print(f"  ⚠️  [{tbl}] SKIPPED — {exc}\n")

print("─" * 60)
if errors:
    print(f"⚠️  {len(errors)} table(s) skipped due to errors:")
    for tbl, msg in errors:
        print(f"   • {tbl}: {msg}")
else:
    print(f"✅ All {len(SNAPSHOTS)} table(s) resolved successfully")

# COMMAND ----------

# =============================================================================
# Cell 4 — Create views (once only, zero-downtime)
# =============================================================================
# ZERO-DOWNTIME STRATEGY
# ──────────────────────
# The view is pointed at the entire  data/  directory (TABLE_CONFIGS data_path),
# NOT at a specific snapshot sub-path.  Iceberg always writes new parquet files
# into that same directory, so read_files() automatically includes them on
# every fresh query — no DDL change to the view is ever needed.
#
# We use  CREATE VIEW IF NOT EXISTS  so:
#   • First run  → view is created.
#   • Every subsequent run  → the IF NOT EXISTS clause makes this a no-op;
#     the view is never replaced and in-flight user queries are never interrupted.
#
# The Delta audit table (Cell 5) is what gets refreshed on every run.
# If you genuinely need to update the view definition (e.g. add a column),
# run the  ALTER VIEW  block at the bottom of this cell.

print("Ensuring views exist (zero-downtime, CREATE IF NOT EXISTS) …")
print("─" * 60)

for tbl, snap in SNAPSHOTS.items():
    view      = snap["view"]
    data_path = snap["data_path"]          # stable directory — never changes

    # Check whether the view already exists
    view_parts   = view.split(".")         # ["lakehouse", "lakehouse_db", "vw_..."]
    catalog, schema, view_name = view_parts
    existing = spark.sql(
        f"SHOW VIEWS IN {catalog}.{schema} LIKE '{view_name}'"
    ).count()

    if existing == 0:
        # First-time creation only
        spark.sql(f"""
            CREATE VIEW IF NOT EXISTS {view}
            COMMENT 'Iceberg parquet for {tbl} — directory-glob view, never needs DDL refresh'
            AS
            SELECT
                *,
                _metadata.file_path AS snap_file,
                _metadata.file_size AS snap_file_size
            FROM read_files(
                '{data_path}',
                format      => 'parquet',
                mergeSchema => true
            )
        """)
        action = "CREATED"
    else:
        # View already exists and points at the right directory — nothing to do.
        # New parquet files Iceberg appended since last run are picked up
        # automatically by read_files() on the next user query.
        action = "EXISTS (no DDL change — zero downtime preserved)"

    row_count = spark.sql(f"SELECT COUNT(*) AS n FROM {view}").collect()[0]["n"]
    print(f"  ✅ {view}")
    print(f"     status={action}")
    print(f"     rows={row_count:,}  src={data_path}")
    print()

print("─" * 60)
print(f"✅ {len(SNAPSHOTS)} view(s) verified")
print()
print("  ┌─ HOW NEW DATA BECOMES VISIBLE ──────────────────────────────┐")
print("  │  The view uses read_files() on the data/ directory.         │")
print("  │  When Spark appends a new Iceberg snapshot, it writes new   │")
print("  │  *.parquet files into that same directory.  The next query  │")
print("  │  against the view picks them up automatically — no notebook │")
print("  │  re-run and no DDL change is ever required for the view.    │")
print("  │                                                             │")
print("  │  Run Cell 5 to refresh the Delta audit table (zero-downtime │")
print("  │  atomic overwrite via Delta MVCC).                          │")
print("  └─────────────────────────────────────────────────────────────┘")
print()
print("  ALTER VIEW (only if you need to change the view definition):")
print("  spark.sql('ALTER VIEW <view> AS SELECT ...')")

# COMMAND ----------

# =============================================================================
# Cell 5 — Refresh Delta audit tables for all resolved tables
# =============================================================================
# For each table:
#   a) CREATE TABLE IF NOT EXISTS  <audit_tbl>  USING DELTA  (schema inferred
#      on first run; overwriteSchema=true handles column additions later)
#   b) Read parquet from data_path, attach snap_file / snap_file_size /
#      refreshed_at audit columns, then INSERT OVERWRITE the Delta table.
#
# Each refresh is a single atomic Delta transaction so there is no dirty read
# window — the old data is replaced atomically by the new snapshot.

from pyspark.sql.functions import col, current_timestamp, lit

print("Refreshing Delta audit tables …")
print("─" * 60)

for tbl, snap in SNAPSHOTS.items():
    audit_tbl = snap["audit_tbl"]
    data_path = snap["data_path"]

    # Read the parquet snapshot
    df = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(data_path)
        .withColumn("snap_file",      col("_metadata.file_path"))
        .withColumn("snap_file_size", col("_metadata.file_size"))
        .withColumn("refreshed_at",   current_timestamp())
        .withColumn("source_table",   lit(tbl))
        .drop("_metadata")
    )

    # Atomic overwrite into the Delta table
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("mergeSchema",     "true")
        .saveAsTable(audit_tbl)
    )

    row_count = spark.sql(
        f"SELECT COUNT(*) AS n FROM {audit_tbl}"
    ).collect()[0]["n"]

    print(f"  ✅ {audit_tbl}")
    print(f"     rows={row_count:,}  src={data_path}")
    print()

print("─" * 60)
print(f"✅ {len(SNAPSHOTS)} audit table(s) refreshed")

# COMMAND ----------

# =============================================================================
# Cell 6 — Summary report
# =============================================================================
# Prints a consolidated table showing every object that was updated, its
# row count, and the snapshot timestamp.  Run after Cells 3–5.

print("\n" + "═" * 70)
print("  REFRESH SUMMARY")
print("═" * 70)
print(f"  {'TABLE':<22} {'OBJECT':<14} {'ROWS':>8}  {'SNAPSHOT UPDATED'}")
print("─" * 70)

for tbl, snap in SNAPSHOTS.items():
    view      = snap["view"].split(".")[-1]
    audit_tbl = snap["audit_tbl"].split(".")[-1]

    view_rows  = spark.sql(f"SELECT COUNT(*) AS n FROM {snap['view']}").collect()[0]["n"]
    audit_rows = spark.sql(f"SELECT COUNT(*) AS n FROM {snap['audit_tbl']}").collect()[0]["n"]

    print(f"  {tbl:<22} {'view':<14} {view_rows:>8,}  {snap['last_updated']}")
    print(f"  {'':<22} {'delta table':<14} {audit_rows:>8,}")

print("═" * 70)
print("  Re-run Cells 3–5 any time new data lands in S3.")
print("═" * 70)

# COMMAND ----------

# =============================================================================
# Cell 7 — Optional: OPTIMIZE all audit tables
# =============================================================================
# Compacts small Delta files for faster SQL queries.
# Run after a large refresh or when query times increase.

print("Running OPTIMIZE on all audit tables …")
for tbl, snap in SNAPSHOTS.items():
    spark.sql(f"OPTIMIZE {snap['audit_tbl']}")
    print(f"  ✅ OPTIMIZE {snap['audit_tbl']}")

print("✅ Done")
