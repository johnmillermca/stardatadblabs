# Databricks notebook source
# nb_multi_table_auto_reader.py
#
# PURPOSE  : Single automated notebook that discovers every Iceberg table under
#            a given S3 warehouse prefix and, for each one:
#              1. Auto-discovers all table folders under the warehouse prefix
#                 via dbutils.fs.ls() — no table names need to be hardcoded
#              2. Picks the newest *.metadata.json in each table's metadata/ dir
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
# TO ADD A NEW TABLE: just create the Iceberg table with Spark — the next
#   notebook run discovers it automatically.  No code changes required.
#
# CATALOG  : lakehouse  (Unity Catalog)
# SCHEMA   : derived from the database folder name under the warehouse prefix

# COMMAND ----------

# =============================================================================
# Cell 1 — Configuration: warehouse root only
# =============================================================================
# Set the two values below.  Everything else is auto-discovered by scanning
# the S3 directory tree — no table names ever need to be hardcoded.
#
# WAREHOUSE_ROOT  : S3 URI that contains one sub-folder per database.
#                   Each database folder contains one sub-folder per table.
#                   Structure expected:
#                     <WAREHOUSE_ROOT>/
#                       <db_name>/          ← one folder per Iceberg database
#                         <table_name>/     ← one folder per Iceberg table
#                           metadata/       ← *.metadata.json files
#                           data/           ← *.parquet files
#
# DATABRICKS_CATALOG : Unity Catalog catalog name where views and Delta audit
#                      tables will be created.
#
# SKIP_TABLES : set of "<db>.<table>" names to exclude from auto-discovery
#               (e.g. system tables, staging tables you don't want views for).

WAREHOUSE_ROOT     = "s3://stardata-databricks/iceberg/warehouse/"
DATABRICKS_CATALOG = "lakehouse"
SKIP_TABLES        = set()   # e.g. {"lakehouse_db.staging", "lakehouse_db._temp"}

print(f"Warehouse root     : {WAREHOUSE_ROOT}")
print(f"Databricks catalog : {DATABRICKS_CATALOG}")

# COMMAND ----------

# =============================================================================
# Cell 2 — Auto-discover all Iceberg tables under the warehouse root
# =============================================================================
# Walks two levels deep:
#   Level 1 → database folders  (e.g. lakehouse_db/, analytics_db/)
#   Level 2 → table folders     (e.g. customer/, customer_orders/, product/)
#
# A folder is treated as a valid Iceberg table only when it contains a
# metadata/ sub-directory.  Folders without metadata/ are silently skipped
# (e.g. _delta_log/, _checkpoints/, or any non-Iceberg folder).
#
# Result: TABLE_CONFIGS dict with the same shape as before, built entirely
# from S3 directory listings — no hardcoding required.

TABLE_CONFIGS = {}
discovery_skipped = []

db_entries = dbutils.fs.ls(WAREHOUSE_ROOT)

for db_entry in db_entries:
    if not db_entry.isDir():
        continue                              # skip stray files at root level

    db_name = db_entry.name.rstrip("/")       # e.g. "lakehouse_db"

    try:
        table_entries = dbutils.fs.ls(db_entry.path)
    except Exception:
        continue                              # no permission / empty prefix

    for tbl_entry in table_entries:
        if not tbl_entry.isDir():
            continue

        tbl_name = tbl_entry.name.rstrip("/") # e.g. "customer"
        key      = f"{db_name}.{tbl_name}"    # e.g. "lakehouse_db.customer"

        if key in SKIP_TABLES:
            discovery_skipped.append(key)
            continue

        meta_path = tbl_entry.path.rstrip("/") + "/metadata/"
        data_path = tbl_entry.path.rstrip("/") + "/data/"

        # Confirm metadata/ exists before including this folder
        try:
            ls_check = dbutils.fs.ls(meta_path)
            has_meta = any(f.name.endswith(".metadata.json") for f in ls_check)
        except Exception:
            has_meta = False

        if not has_meta:
            continue                          # not an Iceberg table — skip

        TABLE_CONFIGS[key] = {
            "db_name"    : db_name,
            "table_name" : tbl_name,
            "meta_path"  : meta_path,
            "data_path"  : data_path,
            # view name:      vw_<table>_latest
            # audit tbl name: <table>_snapshot_audit
            "view"       : f"{DATABRICKS_CATALOG}.{db_name}.vw_{tbl_name}_latest",
            "audit_tbl"  : f"{DATABRICKS_CATALOG}.{db_name}.{tbl_name}_snapshot_audit",
        }

print("─" * 60)
print(f"Auto-discovered {len(TABLE_CONFIGS)} Iceberg table(s):")
for key, cfg in TABLE_CONFIGS.items():
    print(f"  {key:<35}  view → {cfg['view'].split('.')[-1]}")
if discovery_skipped:
    print(f"\nSkipped (SKIP_TABLES): {discovery_skipped}")
print("─" * 60)

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
# Cell 4 — Resolve latest snapshot for every discovered table
# =============================================================================
# Loops over TABLE_CONFIGS (built by Cell 2) and calls resolve_latest_snapshot()
# for each table.  Results are collected into SNAPSHOTS so later cells can use
# them without re-reading S3.
#
# Also ensures each database schema exists in the Databricks catalog so that
# subsequent CREATE VIEW and saveAsTable calls don't fail on a missing schema.

print("─" * 60)
print("Resolving latest Iceberg snapshots …")
print("─" * 60)

# Create every schema discovered (one CREATE SCHEMA per unique db_name)
for db_name in {cfg["db_name"] for cfg in TABLE_CONFIGS.values()}:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABRICKS_CATALOG}.{db_name}")

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
# Cell 5 — Create views (once only, zero-downtime)
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
# Cell 6 — Refresh Delta audit tables for all resolved tables
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
# Cell 7 — Summary report
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
# Cell 8 — Optional: OPTIMIZE all audit tables
# =============================================================================
# Compacts small Delta files for faster SQL queries.
# Run after a large refresh or when query times increase.

print("Running OPTIMIZE on all audit tables …")
for tbl, snap in SNAPSHOTS.items():
    spark.sql(f"OPTIMIZE {snap['audit_tbl']}")
    print(f"  ✅ OPTIMIZE {snap['audit_tbl']}")

print("✅ Done")
