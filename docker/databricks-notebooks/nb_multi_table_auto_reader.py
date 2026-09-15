# Databricks notebook source
# nb_multi_table_auto_reader.py
#
# PURPOSE  : Single automated notebook that discovers every Iceberg table under
#            a given S3 warehouse prefix and, for each one:
#              1. Auto-discovers all table folders under the warehouse prefix
#                 via dbutils.fs.ls() — no table names need to be hardcoded
#              2. Finds the latest metadata.json by modificationTime DESC
#              3. Reads the table via spark.read.format("iceberg") pinned to
#                 that exact metadata file — Spark's Iceberg engine handles
#                 data files, delete files, and merge-on-read correctly
#              4. Writes the result as a Unity Catalog Delta table (snap_*)
#              5. Creates or replaces a UC view (vw_*) over the Delta table
#
# WHY spark.read.format("iceberg").option("metadata-location", ...) ?
# ────────────────────────────────────────────────────────────────────
# The previous approach manually walked Iceberg manifests and read raw parquet
# files.  This works for INSERT-only (copy-on-write) tables but fails silently
# for tables written by Presto or any engine that uses merge-on-read DELETEs:
#
#   Merge-on-read DELETE writes delete files (content=1/2) alongside the
#   original data files.  The data files remain EXISTING in the manifest.
#   A raw parquet read of those files returns ALL rows including deleted ones.
#   Only the Iceberg engine knows how to apply the delete files at read time.
#
# spark.read.format("iceberg").option("metadata-location", path) pins the
# reader to a specific metadata.json without needing a catalog registration.
# The Iceberg engine then:
#   • Resolves current-snapshot-id from the metadata file
#   • Reads the manifest-list and all manifests
#   • Applies position/equality delete files against data files
#   • Returns only the live rows for the current snapshot
#
# This works correctly for Spark-written, Presto-written, and Flink-written
# Iceberg tables regardless of whether they use copy-on-write or merge-on-read.
#
# METADATA FILE SELECTION
# ────────────────────────
# Lists metadata/*.metadata.json via dbutils.fs.ls() and sorts by
# modificationTime DESC.  The most recently modified file is always the one
# written by the latest transaction — no version-hint.text needed.
#
# CATALOG  : lakehouse  (Unity Catalog)
# SCHEMA   : derived from the database folder name under the warehouse prefix

# COMMAND ----------

# =============================================================================
# Cell 1 — Configuration
# =============================================================================
WAREHOUSE_ROOT     = "s3://stardata-databricks/iceberg/warehouse/"
DATABRICKS_CATALOG = "lakehouse"
SKIP_TABLES        = set()   # e.g. {"lakehouse_db.staging", "lakehouse_db._temp"}
NOTEBOOK_VERSION   = "2026-09-15-v10"  # bump on every upload to confirm correct version

print(f"Notebook version   : {NOTEBOOK_VERSION}")
print(f"Warehouse root     : {WAREHOUSE_ROOT}")
print(f"Databricks catalog : {DATABRICKS_CATALOG}")

# COMMAND ----------

# =============================================================================
# Cell 2 — Auto-discover all Iceberg tables under the warehouse root
# =============================================================================
# Walks two levels deep: Level 1 → database folders, Level 2 → table folders.
# A folder is a valid Iceberg table only when metadata/ contains at least one
# *.metadata.json file.

TABLE_CONFIGS     = {}
discovery_skipped = []

db_entries = dbutils.fs.ls(WAREHOUSE_ROOT)

for db_entry in db_entries:
    if not db_entry.isDir():
        continue

    db_name = db_entry.name.rstrip("/")

    try:
        table_entries = dbutils.fs.ls(db_entry.path)
    except Exception:
        continue

    for tbl_entry in table_entries:
        if not tbl_entry.isDir():
            continue

        tbl_name = tbl_entry.name.rstrip("/")
        key      = f"{db_name}.{tbl_name}"

        if key in SKIP_TABLES:
            discovery_skipped.append(key)
            continue

        meta_path = tbl_entry.path.rstrip("/") + "/metadata/"

        try:
            ls_check = dbutils.fs.ls(meta_path)
            has_meta = any(f.name.endswith(".metadata.json") for f in ls_check)
        except Exception:
            has_meta = False

        if not has_meta:
            continue

        # Sanitise name segments — dots and leading underscores break UC identifiers
        safe_db  = db_name.replace(".", "_").lstrip("_")
        safe_tbl = tbl_name.replace(".", "_").lstrip("_")

        TABLE_CONFIGS[key] = {
            "db_name"   : db_name,
            "tbl_name"  : tbl_name,
            "safe_db"   : safe_db,
            "safe_tbl"  : safe_tbl,
            "meta_path" : meta_path,
            "temp_view" : f"{db_name}__{tbl_name}__latest",
            "uc_table"  : f"{DATABRICKS_CATALOG}.{safe_db}.snap_{safe_tbl}_latest",
            "uc_view"   : f"{DATABRICKS_CATALOG}.{safe_db}.vw_{safe_tbl}_latest",
        }

print("─" * 60)
print(f"Auto-discovered {len(TABLE_CONFIGS)} Iceberg table(s):")
for key, cfg in TABLE_CONFIGS.items():
    print(f"  {key:<40}  uc_view → {cfg['uc_view']}")
if discovery_skipped:
    print(f"\nSkipped (SKIP_TABLES): {discovery_skipped}")
print("─" * 60)

# COMMAND ----------

# =============================================================================
# Cell 3 — Resolve latest metadata file per table (modificationTime DESC)
# =============================================================================
# For each discovered table, list metadata/*.metadata.json and select the file
# with the highest modificationTime.  This is the file written by the most
# recent transaction — regardless of the writer engine (Spark, Presto, Flink).
#
# No version-hint.text needed.  No filename parsing.  Just timestamp sort.

import datetime


def latest_metadata_file(meta_path: str) -> tuple:
    """
    Returns (s3_path, filename, modification_ts_str) for the most recently
    modified *.metadata.json under meta_path.
    Raises RuntimeError if no metadata files are found.
    """
    all_files  = dbutils.fs.ls(meta_path)
    meta_files = [f for f in all_files if f.name.endswith(".metadata.json")]
    if not meta_files:
        raise RuntimeError(f"No *.metadata.json found under {meta_path}")

    meta_files.sort(key=lambda f: f.modificationTime, reverse=True)
    top  = meta_files[0]
    path = top.path.replace("s3a://", "s3://")
    ts   = datetime.datetime.utcfromtimestamp(
               top.modificationTime / 1000
           ).strftime("%Y-%m-%d %H:%M:%S UTC")
    return path, top.name, ts


print("✅ latest_metadata_file() defined")

# COMMAND ----------

# =============================================================================
# Cell 4 — Read each Iceberg table via metadata-pinned Iceberg reader
# =============================================================================
# spark.read.format("iceberg").option("metadata-location", path).load()
#
# Pins the Iceberg reader to the exact metadata.json selected in Cell 3.
# The Spark Iceberg engine:
#   • Reads current-snapshot-id from the metadata file
#   • Walks manifest-list → manifests → data files
#   • Applies position-delete and equality-delete files (merge-on-read)
#   • Returns only the live rows for the current snapshot
#
# This correctly handles:
#   • Spark copy-on-write tables (old files marked DELETED in manifest)
#   • Presto / merge-on-read tables (delete files applied at read time)
#   • Any mix of the above across tables in the same warehouse
#
# The DataFrame is registered as a Spark temp view for in-session queries,
# AND written to a Unity Catalog Delta table for cross-session access.

spark.sql(f"USE CATALOG {DATABRICKS_CATALOG}")

for db_name in {cfg["safe_db"] for cfg in TABLE_CONFIGS.values()}:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABRICKS_CATALOG}.{db_name}")

print("─" * 60)
print("Reading Iceberg tables via metadata-pinned Iceberg reader …")
print("─" * 60)

SNAPSHOTS   = {}
VIEW_RESULTS = {}
errors      = []

for key, cfg in TABLE_CONFIGS.items():
    print(f"\n  [{key}]")
    try:
        # ── Step 1: find latest metadata file ───────────────────────────────
        meta_s3, meta_name, meta_ts = latest_metadata_file(cfg["meta_path"])
        print(f"    metadata file : {meta_name}  ({meta_ts})")

        # ── Step 2: read via Iceberg engine (handles delete files) ───────────
        df = (
            spark.read
                 .format("iceberg")
                 .option("metadata-location", meta_s3)
                 .load()
        )

        # ── Step 3: register as Spark temp view ──────────────────────────────
        df.createOrReplaceTempView(cfg["temp_view"])
        row_count = spark.sql(
            f"SELECT COUNT(*) AS n FROM {cfg['temp_view']}"
        ).collect()[0]["n"]
        print(f"    rows          : {row_count:,}")
        print(f"    temp view     : {cfg['temp_view']}  ✅")

        SNAPSHOTS[key]    = {**cfg, "meta_name": meta_name, "meta_ts": meta_ts, "rows": row_count}
        VIEW_RESULTS[key] = {"rows": row_count}

    except Exception as exc:
        errors.append((key, str(exc)))
        print(f"    ⚠️  SKIPPED — {exc}")

print()
print("─" * 60)
if errors:
    print(f"⚠️  {len(errors)} table(s) skipped due to errors:")
    for tbl, msg in errors:
        print(f"   • {tbl}: {msg}")
else:
    print(f"✅ All {len(SNAPSHOTS)} table(s) read successfully")

# COMMAND ----------

# =============================================================================
# Cell 5 — Write Unity Catalog Delta tables + recreate UC views
# =============================================================================
# For each table that was successfully read:
#   • Overwrite snap_<table>_latest Delta table with the current Iceberg data
#   • DROP / CREATE VIEW vw_<table>_latest → SELECT * FROM snap_*
#
# The Delta table is the durable cross-session copy.
# The vw_* view is the stable name that SQL Editor users and dashboards query.
# After this cell, both always reflect the latest Iceberg snapshot.

spark.sql(f"USE CATALOG {DATABRICKS_CATALOG}")

print("Writing Unity Catalog Delta tables and refreshing UC views …")
print("─" * 60)

for key, snap in SNAPSHOTS.items():
    uc_table = snap["uc_table"]
    uc_view  = snap["uc_view"]
    temp_view = snap["temp_view"]

    # Read from the temp view (already resolved by Cell 4 Iceberg reader)
    df = spark.table(temp_view)

    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(uc_table)
    )

    row_count = spark.sql(f"SELECT COUNT(*) AS n FROM {uc_table}").collect()[0]["n"]

    # DROP and recreate the UC view so it always points at the current Delta table
    spark.sql(f"DROP VIEW IF EXISTS {uc_view}")
    spark.sql(f"CREATE VIEW {uc_view} AS SELECT * FROM {uc_table}")

    print(f"  ✅ {uc_table}")
    print(f"     {snap['meta_name']}  ({snap['meta_ts']})")
    print(f"     rows={row_count:,}")
    print(f"     view → {uc_view}  ✅")
    print()

print("─" * 60)
print(f"✅ {len(SNAPSHOTS)} Delta table(s) and UC view(s) refreshed")
print()
print("  ⚠️  These are point-in-time snapshots.")
print("  Re-run Cells 2 → 5 after any Iceberg write (INSERT/UPDATE/DELETE).")

# COMMAND ----------

# =============================================================================
# Cell 6 — Summary report
# =============================================================================

print("\n" + "═" * 70)
print("  REFRESH SUMMARY")
print("═" * 70)
print(f"  {'TABLE':<40} {'ROWS':>8}  {'METADATA FILE TIMESTAMP'}")
print("─" * 70)

for key, snap in SNAPSHOTS.items():
    print(f"  {key:<40} {snap['rows']:>8,}  {snap['meta_ts']}")

if errors:
    print()
    print(f"  ⚠️  {len(errors)} table(s) skipped:")
    for tbl, msg in errors:
        print(f"     • {tbl}: {msg}")

print("═" * 70)
print()
print("  HOW UPDATES AND DELETES WORK (this version)")
print("  ─────────────────────────────────────────────")
print("  • Latest metadata.json selected by modificationTime DESC")
print("  • Iceberg engine reads it — applies delete files natively")
print("  • Works for Spark (copy-on-write) AND Presto (merge-on-read)")
print("  • Delta table overwritten with correct live rows")
print("  • vw_* view recreated → always reflects current snapshot")
print("═" * 70)

# COMMAND ----------

# =============================================================================
# Cell 7 — Optional: cache a view into NVMe disk cache
# =============================================================================
# ⚠️  DO NOT run this cell as part of a full notebook run-all.
#     Run it manually ONLY when you explicitly want to warm the NVMe cache.

# ── Uncomment and run manually to cache a single view ─────────────────────
# VIEW_TO_CACHE = "lakehouse_db__customer__latest"
# print(f"Caching {VIEW_TO_CACHE} into NVMe disk cache …")
# spark.sql(f"CACHE TABLE {VIEW_TO_CACHE}")
# print(f"✅ Cache warm for {VIEW_TO_CACHE}")

# ── Or uncomment to cache ALL discovered views ─────────────────────────────
# for key, snap in SNAPSHOTS.items():
#     print(f"  Caching {snap['temp_view']} …")
#     spark.sql(f"CACHE TABLE {snap['temp_view']}")
# print("✅ All views cached")

print("Cell 7 — NVMe cache warm: SKIPPED (manual-only cell, all code commented out)")

# COMMAND ----------

# =============================================================================
# Cell 8 — Optional: invalidate NVMe cache after a new Iceberg snapshot
# =============================================================================
# ⚠️  DO NOT run this cell as part of a full notebook run-all.

# ── Uncomment and run manually to re-warm a single view ───────────────────
# VIEW_TO_RECACHE = "lakehouse_db__customer__latest"
# print(f"Re-warming NVMe cache for {VIEW_TO_RECACHE} …")
# spark.sql(f"UNCACHE TABLE IF EXISTS {VIEW_TO_RECACHE}")
# spark.sql(f"CACHE TABLE {VIEW_TO_RECACHE}")
# print(f"✅ NVMe cache refreshed for {VIEW_TO_RECACHE}")

# ── Or uncomment to re-warm ALL views ─────────────────────────────────────
# for key, snap in SNAPSHOTS.items():
#     print(f"  Re-warming {snap['temp_view']} …")
#     spark.sql(f"UNCACHE TABLE IF EXISTS {snap['temp_view']}")
#     spark.sql(f"CACHE TABLE {snap['temp_view']}")
# print("✅ All views re-warmed")

print("Cell 8 — NVMe cache re-warm: SKIPPED (manual-only cell, all code commented out)")

# COMMAND ----------

# =============================================================================
# Cell 9 — Sample: manually register one new table on first run
# =============================================================================
# ⚠️  DO NOT run this cell as part of a full notebook run-all.
#     Run it manually ONLY when you need to register a single new table
#     without doing a full discovery pass.

# NEW_TABLE_KEY      = "lakehouse_db.my_new_table"   # ← change to your table
# WAREHOUSE_ROOT     = "s3://stardata-databricks/iceberg/warehouse/"
# DATABRICKS_CATALOG = "lakehouse"
#
# db_name, tbl_name = NEW_TABLE_KEY.split(".", 1)
# safe_db   = db_name.replace(".", "_").lstrip("_")
# safe_tbl  = tbl_name.replace(".", "_").lstrip("_")
# meta_path = f"{WAREHOUSE_ROOT.rstrip('/')}/{db_name}/{tbl_name}/metadata/"
# temp_view = f"{db_name}__{tbl_name}__latest"
# uc_table  = f"{DATABRICKS_CATALOG}.{safe_db}.snap_{safe_tbl}_latest"
# uc_view   = f"{DATABRICKS_CATALOG}.{safe_db}.vw_{safe_tbl}_latest"
#
# meta_s3, meta_name, meta_ts = latest_metadata_file(meta_path)
# print(f"metadata file : {meta_name}  ({meta_ts})")
#
# df = spark.read.format("iceberg").option("metadata-location", meta_s3).load()
# df.createOrReplaceTempView(temp_view)
# row_count = spark.sql(f"SELECT COUNT(*) AS n FROM {temp_view}").collect()[0]["n"]
# print(f"rows : {row_count:,}")
#
# spark.sql(f"USE CATALOG {DATABRICKS_CATALOG}")
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABRICKS_CATALOG}.{safe_db}")
# df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(uc_table)
# spark.sql(f"DROP VIEW IF EXISTS {uc_view}")
# spark.sql(f"CREATE VIEW {uc_view} AS SELECT * FROM {uc_table}")
# print(f"✅ {uc_table}")
# print(f"✅ {uc_view}")

print("Cell 9 — single-table registration: SKIPPED (manual-only cell, all code commented out)")
