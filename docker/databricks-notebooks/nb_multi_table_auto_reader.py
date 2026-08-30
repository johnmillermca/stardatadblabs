# Databricks notebook source
# nb_multi_table_auto_reader.py
#
# PURPOSE  : Single automated notebook that reads parquet data from ANY number
#            of Iceberg S3 folders.  For each configured table it:
#              1. Lists the metadata/ prefix and picks the newest *.metadata.json
#              2. Parses the JSON to resolve the data/ path for that snapshot
#              3. CREATE OR REPLACE VIEW  <catalog>.<schema>.vw_<table>_latest
#              4. CREATE TABLE IF NOT EXISTS + INSERT OVERWRITE into a Delta
#                 audit table  <catalog>.<schema>.<table>_snapshot_audit
#
#            Re-run this notebook after any Spark/Iceberg write — it handles
#            all tables in one pass, no manual path edits ever required.
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
#       "meta_path" : S3 URI of the metadata/ directory (trailing slash required),
#       "view"      : fully-qualified Databricks view name to create/replace,
#       "audit_tbl" : fully-qualified Delta table name to materialise,
#   }
#
# Add or remove entries here to control which tables are refreshed.

TABLE_CONFIGS = {
    "customer": {
        "meta_path" : "s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/metadata/",
        "view"      : "lakehouse.lakehouse_db.vw_customer_latest",
        "audit_tbl" : "lakehouse.lakehouse_db.customer_snapshot_audit",
    },
    "customer_orders": {
        "meta_path" : "s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer_orders/metadata/",
        "view"      : "lakehouse.lakehouse_db.vw_customer_orders_latest",
        "audit_tbl" : "lakehouse.lakehouse_db.customer_orders_snapshot_audit",
    },
    # ── add more tables below ──────────────────────────────────────────────
    # "product": {
    #     "meta_path" : "s3://stardata-databricks/iceberg/warehouse/lakehouse_db/product/metadata/",
    #     "view"      : "lakehouse.lakehouse_db.vw_product_latest",
    #     "audit_tbl" : "lakehouse.lakehouse_db.product_snapshot_audit",
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
# Cell 4 — Create / replace views for all resolved tables
# =============================================================================
# For each table that resolved successfully, runs:
#   CREATE OR REPLACE VIEW <view> AS SELECT * FROM read_files('<data_path>', ...)
#
# The view always reflects the snapshot found in Cell 3.  Every subsequent
# query against the view sees that snapshot's data with no further action.

print("Creating / replacing views …")
print("─" * 60)

for tbl, snap in SNAPSHOTS.items():
    view      = snap["view"]
    data_path = snap["data_path"]

    spark.sql(f"""
        CREATE OR REPLACE VIEW {view}
        COMMENT 'Latest Iceberg snapshot of {tbl} — auto-refreshed via nb_multi_table_auto_reader'
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

    row_count = spark.sql(f"SELECT COUNT(*) AS n FROM {view}").collect()[0]["n"]
    print(f"  ✅ {view}")
    print(f"     rows={row_count:,}  snapshot={snap['snapshot_id']}  src={data_path}")
    print()

print("─" * 60)
print(f"✅ {len(SNAPSHOTS)} view(s) refreshed")

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
