# Databricks notebook source
# nb_multi_table_auto_reader.py
#
# PURPOSE  : Single automated notebook that discovers every Iceberg table under
#            a given S3 warehouse prefix and, for each one:
#              1. Auto-discovers all table folders under the warehouse prefix
#                 via dbutils.fs.ls() — no table names need to be hardcoded
#              2. Picks the newest *.metadata.json in each table's metadata/ dir
#              3. Creates a persistent view ONCE via CREATE VIEW IF NOT EXISTS —
#                 never replaced again, so in-flight user queries are never
#                 interrupted
#
# ZERO-DOWNTIME DESIGN
# ─────────────────────
# The view points at the whole  data/  directory, not at a specific snapshot
# file.  Iceberg always appends new *.parquet files into that same directory,
# so read_files() automatically includes them on the next query — no DDL
# change to the view is ever needed after the initial creation.
#
# CREATE VIEW IF NOT EXISTS  (Cell 5) → only fires on first run per table.
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
# DATABRICKS_CATALOG : Unity Catalog catalog name where views will be created.
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
# metadata/ sub-directory with at least one *.metadata.json file.
# Folders without metadata/ are silently skipped (e.g. _delta_log/,
# _checkpoints/, or any non-Iceberg folder).
#
# Result: TABLE_CONFIGS dict built entirely from S3 directory listings.

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

        # Confirm metadata/ exists and contains at least one metadata.json
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
            # view name: vw_<table>_latest
            "view"       : f"{DATABRICKS_CATALOG}.{db_name}.vw_{tbl_name}_latest",
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
# Cell 3 — Helper: resolve the latest metadata JSON for one table
# =============================================================================
# Lists *.metadata.json files under meta_path, picks the one with the
# highest modificationTime, parses it, and returns snapshot diagnostics.

import json, datetime

def resolve_latest_snapshot(table_name: str, meta_path: str) -> dict:
    """
    Pick the newest *.metadata.json, parse it, return snapshot_id and
    last_updated timestamp for diagnostic output.
    """
    all_files  = dbutils.fs.ls(meta_path)
    meta_files = [f for f in all_files if f.name.endswith(".metadata.json")]

    if not meta_files:
        raise RuntimeError(
            f"[{table_name}] No *.metadata.json files found under {meta_path}"
        )

    meta_files.sort(key=lambda f: f.modificationTime, reverse=True)
    latest = meta_files[0]

    raw          = spark.read.text(latest.path, wholetext=True).collect()[0][0]
    meta         = json.loads(raw)
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
        f"    Last updated : {ts}"
    )
    return {
        "table_name"  : table_name,
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
# for diagnostics.  Also ensures each Databricks schema exists so that
# subsequent CREATE VIEW calls don't fail on a missing schema.

print("─" * 60)
print("Resolving latest Iceberg snapshots …")
print("─" * 60)

# Ensure every discovered schema exists in the Databricks catalog
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
# The view points at the entire data/ directory (not a specific snapshot path).
# Iceberg always writes new parquet files into that same directory, so
# read_files() automatically includes them on every fresh query — no DDL
# change to the view is ever needed after the initial creation.
#
# CREATE VIEW IF NOT EXISTS:
#   • First run  → view is created.
#   • Every subsequent run  → view already exists, this is a no-op;
#     in-flight user queries are never interrupted.
#
# To change the view definition (e.g. add a column):
#   spark.sql("ALTER VIEW <view> AS SELECT ...")

print("Ensuring views exist (zero-downtime, CREATE VIEW IF NOT EXISTS) …")
print("─" * 60)

for tbl, snap in SNAPSHOTS.items():
    view      = snap["view"]
    data_path = snap["data_path"]          # stable directory — never changes

    # spark.catalog.tableExists() accepts a fully-qualified 3-part name and
    # works on Serverless compute.  SHOW VIEWS IN {catalog}.{schema} raises
    # CROSS_CATALOG_SCHEMA_REFERENCE_NOT_SUPPORTED on Serverless and was removed.
    view_exists = spark.catalog.tableExists(view)

    if not view_exists:
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
        # View already exists — new parquet files appended by Iceberg are
        # picked up automatically by read_files() on the next user query.
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
print("  └─────────────────────────────────────────────────────────────┘")

# COMMAND ----------

# =============================================================================
# Cell 6 — Summary report
# =============================================================================

print("\n" + "═" * 70)
print("  REFRESH SUMMARY")
print("═" * 70)
print(f"  {'TABLE':<35} {'ROWS':>8}  {'SNAPSHOT UPDATED'}")
print("─" * 70)

for tbl, snap in SNAPSHOTS.items():
    view_rows = spark.sql(
        f"SELECT COUNT(*) AS n FROM {snap['view']}"
    ).collect()[0]["n"]
    print(f"  {tbl:<35} {view_rows:>8,}  {snap['last_updated']}")

print("═" * 70)
print("  Re-run Cells 2 + 4 any time new data lands in S3.")
print("  (Views auto-reflect new parquet files — no Cell 5 re-run needed.)")
print("═" * 70)

# COMMAND ----------

# =============================================================================
# Cell 7 — Optional: cache a view into NVMe disk cache
# =============================================================================
# Databricks Photon clusters expose a local NVMe-backed disk cache.
# Running CACHE SELECT scans the view once and stores the decompressed
# columnar data on the executor's local NVMe, so subsequent queries against
# the same view skip the S3 round-trip entirely.
#
# WHEN TO USE:
#   - The same view is queried repeatedly by many users / dashboards
#   - Query latency matters more than the one-off cache-fill cost
#   - The cluster is Photon-enabled (Standard or higher tier)
#
# CACHE LIFETIME:
#   - Cache survives across queries within the same cluster lifetime
#   - Cache is invalidated automatically when the cluster restarts
#   - After a new Iceberg snapshot (new parquet files), run REFRESH TABLE
#     followed by CACHE SELECT again to warm the cache for the new data
#
# CACHE SIZE:
#   - Check available NVMe cache with:
#       spark.sql("DESCRIBE DETAIL <view>") — not available for views
#       Use: spark.conf.get("spark.databricks.io.cache.maxDiskUsage")

# ── Cache one specific view ────────────────────────────────────────────────
VIEW_TO_CACHE = "lakehouse.lakehouse_db.vw_customer_latest"

print(f"Caching {VIEW_TO_CACHE} into NVMe disk cache …")
spark.sql(f"CACHE SELECT * FROM {VIEW_TO_CACHE}")
print(f"✅ Cache warm for {VIEW_TO_CACHE}")

# ── Or cache ALL discovered views in one loop ──────────────────────────────
# Uncomment the block below to cache every view discovered in Cell 2.
#
# print("Caching all discovered views into NVMe disk cache …")
# for tbl, snap in SNAPSHOTS.items():
#     print(f"  Caching {snap['view']} …")
#     spark.sql(f"CACHE SELECT * FROM {snap['view']}")
#     print(f"  ✅ Done")
# print("✅ All views cached")

# COMMAND ----------

# =============================================================================
# Cell 8 — Optional: invalidate NVMe cache after a new Iceberg snapshot
# =============================================================================
# After Spark writes new parquet files into the data/ directory, the NVMe
# cache still holds the old decompressed data.  Run this cell to evict the
# stale cache entries and re-warm them with the new snapshot.
#
# Step 1: UNCACHE removes the old cached data for the view.
# Step 2: CACHE SELECT re-scans the view (now including the new parquet files)
#         and writes the fresh decompressed data to NVMe.

VIEW_TO_RECACHE = "lakehouse.lakehouse_db.vw_customer_latest"

print(f"Re-warming NVMe cache for {VIEW_TO_RECACHE} …")
spark.sql(f"UNCACHE TABLE IF EXISTS {VIEW_TO_RECACHE}")
spark.sql(f"CACHE SELECT * FROM {VIEW_TO_RECACHE}")
print(f"✅ NVMe cache refreshed for {VIEW_TO_RECACHE}")

# ── Or re-warm ALL views ───────────────────────────────────────────────────
# Uncomment the block below to evict and re-warm every discovered view.
#
# for tbl, snap in SNAPSHOTS.items():
#     print(f"  Re-warming {snap['view']} …")
#     spark.sql(f"UNCACHE TABLE IF EXISTS {snap['view']}")
#     spark.sql(f"CACHE SELECT * FROM {snap['view']}")
#     print(f"  ✅ Done")
# print("✅ All views re-warmed")

# COMMAND ----------

# =============================================================================
# Cell 9 — Sample: manually create the view for ONE new table on first refresh
# =============================================================================
# USE THIS WHEN:
#   • You just created a new Iceberg table in Spark (e.g. analytics_db.product)
#   • You want the Databricks view to exist immediately — without waiting for
#     the next full notebook run (Cells 1 → 6).
#
# HOW IT WORKS:
#   1. Set NEW_TABLE_KEY  to "<db_name>.<table_name>"  (matches the S3 folder names)
#   2. Run this cell — it creates the schema if missing, then creates the view
#      using CREATE VIEW IF NOT EXISTS (safe to re-run; no-op if already exists).
#   3. Subsequent full notebook runs (Cells 2 + 4) detect the view via
#      spark.catalog.tableExists() and skip DDL — zero downtime preserved.
#
# AFTER THIS CELL:
#   • The view is live in Unity Catalog immediately.
#   • New parquet files appended by future Spark writes are picked up
#     automatically by read_files() — no further DDL is ever needed.
#
# NOTE: This cell is intentionally standalone. You do NOT need to run
#       Cells 1–8 first. Just set the two variables below and run.

# ── Configuration ─────────────────────────────────────────────────────────────
NEW_TABLE_KEY      = "analytics_db.product"          # "<db_name>.<table_name>"
WAREHOUSE_ROOT     = "s3://stardata-databricks/iceberg/warehouse/"  # same as Cell 1
DATABRICKS_CATALOG = "lakehouse"                     # same as Cell 1
# ──────────────────────────────────────────────────────────────────────────────

db_name, table_name = NEW_TABLE_KEY.split(".", 1)

data_path  = f"{WAREHOUSE_ROOT.rstrip('/')}/{db_name}/{table_name}/data/"
view       = f"{DATABRICKS_CATALOG}.{db_name}.vw_{table_name}_latest"

print(f"New table  : {NEW_TABLE_KEY}")
print(f"Data path  : {data_path}")
print(f"Target view: {view}")
print()

# Step 1 — ensure the schema exists in Unity Catalog
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABRICKS_CATALOG}.{db_name}")
print(f"✅ Schema {DATABRICKS_CATALOG}.{db_name} ready")

# Step 2 — create the view (only fires if it does not already exist)
if spark.catalog.tableExists(view):
    row_count = spark.sql(f"SELECT COUNT(*) AS n FROM {view}").collect()[0]["n"]
    print(f"✅ View already exists — skipping DDL (zero downtime preserved)")
    print(f"   rows={row_count:,}  src={data_path}")
else:
    spark.sql(f"""
        CREATE VIEW IF NOT EXISTS {view}
        COMMENT 'Iceberg parquet for {NEW_TABLE_KEY} — directory-glob view, never needs DDL refresh'
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
    print(f"✅ View CREATED")
    print(f"   rows={row_count:,}  src={data_path}")

print()
print("  The view is now live. Future Spark appends to this table are")
print("  visible automatically on the next query — no DDL change needed.")
