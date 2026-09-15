# Databricks notebook source
# nb_multi_table_auto_reader.py
#
# PURPOSE  : Single automated notebook that discovers every Iceberg table under
#            a given S3 warehouse prefix and, for each one:
#              1. Auto-discovers all table folders under the warehouse prefix
#                 via dbutils.fs.ls() — no table names need to be hardcoded
#              2. Finds the latest metadata.json by modificationTime DESC
#              3. Walks manifest-list → manifests to resolve the exact set of
#                 live data files for the current snapshot
#              4. Writes the result as a Unity Catalog Delta table (snap_*)
#              5. Creates or replaces a UC view (vw_*) over the Delta table
#
# SUPPORTED TABLES
# ─────────────────
# Copy-on-write tables (Spark / Polaris) — fully supported.
#   DELETE / UPDATE rewrites the affected data file and marks the old file
#   as DELETED (status=0) in the manifest.  The manifest walk excludes it.
#
# Merge-on-read tables (Presto / Trino) — AUTO-SKIPPED with a warning.
#   DELETE writes position-delete files (content=1) alongside the original
#   data files.  Applying delete files requires the Iceberg engine JAR which
#   is not available on this Databricks runtime.  These tables are detected
#   automatically (any delete manifest content=1 in the manifest-list) and
#   skipped — their existing snap_* / vw_* tables are left unchanged.
#   Add them to SKIP_TABLES to suppress the warning.
#
# METADATA FILE SELECTION
# ────────────────────────
# Lists metadata/*.metadata.json via dbutils.fs.ls() and sorts by
# modificationTime DESC.  The most recently modified file is always the one
# written by the latest transaction.
#
# CATALOG  : lakehouse  (Unity Catalog)
# SCHEMA   : derived from the database folder name under the warehouse prefix

# COMMAND ----------

# =============================================================================
# Cell 1 — Configuration
# =============================================================================
WAREHOUSE_ROOT     = "s3://stardata-databricks/iceberg/warehouse/"
DATABRICKS_CATALOG = "lakehouse"
SKIP_TABLES        = set()   # e.g. {"lakehouse_db.customer_test"}
NOTEBOOK_VERSION   = "2026-09-15-v11"  # bump on every upload to confirm correct version

print(f"Notebook version   : {NOTEBOOK_VERSION}")
print(f"Warehouse root     : {WAREHOUSE_ROOT}")
print(f"Databricks catalog : {DATABRICKS_CATALOG}")

# COMMAND ----------

# =============================================================================
# Cell 2 — Auto-discover all Iceberg tables under the warehouse root
# =============================================================================

TABLE_CONFIGS     = {}
discovery_skipped = []

for db_entry in dbutils.fs.ls(WAREHOUSE_ROOT):
    if not db_entry.isDir():
        continue
    db_name = db_entry.name.rstrip("/")
    try:
        tbl_entries = dbutils.fs.ls(db_entry.path)
    except Exception:
        continue
    for tbl_entry in tbl_entries:
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
        safe_db  = db_name.replace(".", "_").lstrip("_")
        safe_tbl = tbl_name.replace(".", "_").lstrip("_")
        TABLE_CONFIGS[key] = {
            "db_name"  : db_name,
            "tbl_name" : tbl_name,
            "safe_db"  : safe_db,
            "safe_tbl" : safe_tbl,
            "meta_path": meta_path,
            "temp_view": f"{db_name}__{tbl_name}__latest",
            "uc_table" : f"{DATABRICKS_CATALOG}.{safe_db}.snap_{safe_tbl}_latest",
            "uc_view"  : f"{DATABRICKS_CATALOG}.{safe_db}.vw_{safe_tbl}_latest",
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
# Cell 3 — resolve_live_files() — manifest walk for copy-on-write tables
# =============================================================================
#
# Returns a dict:
#   live_files   : list[str]  — s3:// paths of live DATA parquet files
#   meta_name    : str        — metadata filename used
#   meta_ts      : str        — modificationTime of that file (UTC)
#   snapshot_id  : int        — current-snapshot-id
#   last_updated : str        — last-updated-ms from metadata (UTC)
#   mor_skip     : bool       — True if table has delete files (merge-on-read)
#
# Merge-on-read detection
# ────────────────────────
# After reading the manifest-list, check whether any manifest has content=1
# (delete manifest).  If so, set mor_skip=True and return empty live_files.
# The caller (Cell 4) will skip these tables and leave existing snap_*/vw_*
# tables unchanged rather than overwriting them with wrong data.

import json
import datetime


def resolve_live_files(table_name: str, meta_path: str) -> dict:

    def _norm(p):
        return p.replace("s3a://", "s3://") if p else p

    # ── Step 1: latest metadata.json by modificationTime DESC ────────────
    all_files  = dbutils.fs.ls(meta_path)
    meta_files = [f for f in all_files if f.name.endswith(".metadata.json")]
    if not meta_files:
        raise RuntimeError(f"[{table_name}] No *.metadata.json under {meta_path}")

    meta_files.sort(key=lambda f: f.modificationTime, reverse=True)
    top      = meta_files[0]
    meta_s3  = _norm(top.path)
    meta_name = top.name
    meta_ts  = datetime.datetime.utcfromtimestamp(
                   top.modificationTime / 1000
               ).strftime("%Y-%m-%d %H:%M:%S UTC")

    # ── Step 2: parse metadata JSON ──────────────────────────────────────
    raw  = spark.read.text(meta_s3, wholetext=True).collect()[0][0]
    meta = json.loads(raw)

    current_snapshot_id = meta.get("current-snapshot-id")
    last_updated_ms     = meta.get("last-updated-ms", 0)
    ts = (
        datetime.datetime.utcfromtimestamp(last_updated_ms / 1000)
                         .strftime("%Y-%m-%d %H:%M:%S UTC")
        if last_updated_ms else "unknown"
    )

    _base = {
        "table_name"  : table_name,
        "meta_name"   : meta_name,
        "meta_ts"     : meta_ts,
        "snapshot_id" : current_snapshot_id,
        "last_updated": ts,
        "mor_skip"    : False,
        "live_files"  : [],
    }

    if current_snapshot_id is None:
        print(f"  [{table_name}] No current snapshot — table is empty")
        return _base

    snapshots = meta.get("snapshots", [])
    current   = next((s for s in snapshots if s.get("snapshot-id") == current_snapshot_id), None)
    if current is None:
        raise RuntimeError(
            f"[{table_name}] Snapshot {current_snapshot_id} not found in snapshots[]"
        )

    # ── Step 3: total-records=0 early exit ───────────────────────────────
    summary       = current.get("summary", {})
    total_records = int(summary.get("total-records", -1))
    if total_records == 0:
        print(f"  [{table_name}] total-records=0 — table empty after DELETE")
        return _base

    # ── Step 4: read manifest-list ───────────────────────────────────────
    manifest_list_path = _norm(current.get("manifest-list", ""))
    if not manifest_list_path:
        raise RuntimeError(f"[{table_name}] Snapshot has no manifest-list")

    ml_df          = spark.read.format("avro").load(manifest_list_path)
    manifest_rows  = ml_df.select("manifest_path", "content").collect()

    # ── Step 5: merge-on-read detection ──────────────────────────────────
    # Any manifest with content=1 means delete files exist.
    # We cannot apply them without the Iceberg engine JAR — skip the table.
    has_delete_manifests = any(r["content"] == 1 for r in manifest_rows)
    if has_delete_manifests:
        print(
            f"  [{table_name}] ⚠️  MERGE-ON-READ table detected "
            f"(delete manifests present) — SKIPPED"
        )
        return {**_base, "mor_skip": True}

    # ── Step 6: read all DATA manifests in ONE Spark job ─────────────────
    manifest_paths = [_norm(r["manifest_path"]) for r in manifest_rows if r["content"] == 0]
    if not manifest_paths:
        print(f"  [{table_name}] No data manifests — table is empty")
        return _base

    all_manifests_df = spark.read.format("avro").load(manifest_paths)

    if "data_file" not in all_manifests_df.columns:
        raise RuntimeError(f"[{table_name}] Manifest schema missing data_file column")

    from pyspark.sql import functions as F
    rows = (
        all_manifests_df
        .select(
            F.coalesce(F.col("status"),           F.lit(1)).alias("status"),
            F.col("data_file.file_path").alias("file_path"),
            F.coalesce(F.col("data_file.content"), F.lit(0)).alias("content"),
        )
        .collect()
    )

    # ── Step 7: DELETED-wins dedup ────────────────────────────────────────
    # status: 0=DELETED, 1=EXISTING, 2=ADDED   content: 0=DATA, 1/2=delete file
    file_status = {}
    for row in rows:
        if row["content"] != 0:
            continue
        fp = _norm(row["file_path"])
        if not fp:
            continue
        s  = row["status"]
        ex = file_status.get(fp)
        if ex is None:
            file_status[fp] = s
        elif s == 0:
            file_status[fp] = 0
        elif ex != 0 and s == 2:
            file_status[fp] = 2

    live_files      = [p for p, s in file_status.items() if s in (1, 2)]
    skipped_deleted = sum(1 for s in file_status.values() if s == 0)

    print(
        f"  [{table_name}]  snapshot={current_snapshot_id}\n"
        f"    Meta file    : {meta_name}  ({meta_ts})\n"
        f"    Last updated : {ts}\n"
        f"    Manifests    : {len(manifest_paths)}  (read in 1 Spark job)\n"
        f"    Live files   : {len(live_files)}\n"
        f"    Dead files   : {skipped_deleted} (excluded)"
    )

    return {**_base, "live_files": live_files}


print("✅ resolve_live_files() defined")

# COMMAND ----------

# =============================================================================
# Cell 4 — Resolve snapshots for every discovered table
# =============================================================================

spark.sql(f"USE CATALOG {DATABRICKS_CATALOG}")

for safe_db in {cfg["safe_db"] for cfg in TABLE_CONFIGS.values()}:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABRICKS_CATALOG}.{safe_db}")

print("─" * 60)
print("Resolving Iceberg snapshots …")
print("─" * 60)

SNAPSHOTS = {}
mor_skipped = []
errors      = []

for key, cfg in TABLE_CONFIGS.items():
    print()
    try:
        snap = resolve_live_files(key, cfg["meta_path"])
        if snap["mor_skip"]:
            mor_skipped.append(key)
        else:
            SNAPSHOTS[key] = {**cfg, **snap}
    except Exception as exc:
        errors.append((key, str(exc)))
        print(f"  ⚠️  [{key}] ERROR — {exc}")

print()
print("─" * 60)
if mor_skipped:
    print(f"⏭️  {len(mor_skipped)} merge-on-read table(s) skipped (Presto/Trino):")
    for t in mor_skipped:
        print(f"   • {t}  (add to SKIP_TABLES to suppress this warning)")
if errors:
    print(f"⚠️  {len(errors)} table(s) errored:")
    for t, m in errors:
        print(f"   • {t}: {m}")
print(f"✅ {len(SNAPSHOTS)} copy-on-write table(s) resolved successfully")

# COMMAND ----------

# =============================================================================
# Cell 5 — Create Spark temp views over live snapshot files
# =============================================================================

print("Creating Spark temp views …")
print("─" * 60)

VIEW_RESULTS = {}

for key, snap in SNAPSHOTS.items():
    temp_view  = snap["temp_view"]
    live_files = snap.get("live_files", [])

    if not live_files:
        spark.createDataFrame([], schema="snap_file STRING").createOrReplaceTempView(temp_view)
        row_count = 0
        action    = "REGISTERED (empty)"
    else:
        (
            spark.read
                 .option("mergeSchema", "true")
                 .parquet(*live_files)
                 .createOrReplaceTempView(temp_view)
        )
        row_count = spark.sql(f"SELECT COUNT(*) AS n FROM {temp_view}").collect()[0]["n"]
        action    = f"REFRESHED — snapshot {snap['snapshot_id']}"

    VIEW_RESULTS[key] = {"rows": row_count}
    print(f"  ✅ {temp_view}")
    print(f"     {action}")
    print(f"     rows={row_count:,}  live_files={len(live_files)}")
    print()

print("─" * 60)
print(f"✅ {len(SNAPSHOTS)} temp view(s) created/refreshed")

# COMMAND ----------

# =============================================================================
# Cell 5b — Write Unity Catalog Delta tables + recreate UC views
# =============================================================================

spark.sql(f"USE CATALOG {DATABRICKS_CATALOG}")

print("Writing Unity Catalog Delta tables and refreshing UC views …")
print("─" * 60)

for key, snap in SNAPSHOTS.items():
    uc_table  = snap["uc_table"]
    uc_view   = snap["uc_view"]
    temp_view = snap["temp_view"]
    live_files = snap.get("live_files", [])

    if not live_files:
        empty_df = spark.createDataFrame([], schema="snap_file STRING")
        (
            empty_df.write
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .saveAsTable(uc_table)
        )
        row_count = 0
    else:
        df = spark.read.option("mergeSchema", "true").parquet(*live_files)
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .saveAsTable(uc_table)
        )
        row_count = spark.sql(f"SELECT COUNT(*) AS n FROM {uc_table}").collect()[0]["n"]

    spark.sql(f"DROP VIEW IF EXISTS {uc_view}")
    spark.sql(f"CREATE VIEW {uc_view} AS SELECT * FROM {uc_table}")

    print(f"  ✅ {uc_table}")
    print(f"     {snap['meta_name']}  ({snap['meta_ts']})")
    print(f"     rows={row_count:,}  live_files={len(live_files)}")
    print(f"     view → {uc_view}  ✅")
    print()

print("─" * 60)
print(f"✅ {len(SNAPSHOTS)} Delta table(s) and UC view(s) refreshed")
print()
print("  ⚠️  Point-in-time snapshots — re-run Cells 2 → 5b after any Iceberg write.")

# COMMAND ----------

# =============================================================================
# Cell 6 — Summary report
# =============================================================================

print("\n" + "═" * 70)
print("  REFRESH SUMMARY")
print("═" * 70)
print(f"  {'TABLE':<40} {'ROWS':>8}  {'METADATA TIMESTAMP'}")
print("─" * 70)

for key, snap in SNAPSHOTS.items():
    print(f"  {key:<40} {VIEW_RESULTS[key]['rows']:>8,}  {snap['meta_ts']}")

if mor_skipped:
    print()
    print(f"  ⏭️  Skipped (merge-on-read / Presto): {', '.join(mor_skipped)}")
if errors:
    print()
    for t, m in errors:
        print(f"  ⚠️  {t}: {m}")

print("═" * 70)
print()
print("  Copy-on-write (Spark/Polaris) : manifest walk → parquet read  ✅")
print("  Merge-on-read (Presto/Trino)  : auto-detected → skipped       ⏭️")
print("═" * 70)

# COMMAND ----------

# =============================================================================
# Cell 7 — Optional: NVMe cache warm (manual only)
# =============================================================================
# ⚠️  DO NOT run as part of run-all. Manual only.

# VIEW_TO_CACHE = "lakehouse_db__customer__latest"
# spark.sql(f"CACHE TABLE {VIEW_TO_CACHE}")

print("Cell 7 — NVMe cache warm: SKIPPED (manual-only cell)")

# COMMAND ----------

# =============================================================================
# Cell 8 — Optional: NVMe cache re-warm (manual only)
# =============================================================================
# ⚠️  DO NOT run as part of run-all. Manual only.

# VIEW_TO_RECACHE = "lakehouse_db__customer__latest"
# spark.sql(f"UNCACHE TABLE IF EXISTS {VIEW_TO_RECACHE}")
# spark.sql(f"CACHE TABLE {VIEW_TO_RECACHE}")

print("Cell 8 — NVMe cache re-warm: SKIPPED (manual-only cell)")

# COMMAND ----------

# =============================================================================
# Cell 9 — Optional: single-table registration (manual only)
# =============================================================================
# ⚠️  DO NOT run as part of run-all. Manual only.

# NEW_TABLE_KEY      = "lakehouse_db.my_table"
# db_name, tbl_name = NEW_TABLE_KEY.split(".", 1)
# safe_db   = db_name.replace(".", "_").lstrip("_")
# safe_tbl  = tbl_name.replace(".", "_").lstrip("_")
# meta_path = f"{WAREHOUSE_ROOT.rstrip('/')}/{db_name}/{tbl_name}/metadata/"
# snap      = resolve_live_files(NEW_TABLE_KEY, meta_path)
# live_files = snap["live_files"]
# uc_table  = f"{DATABRICKS_CATALOG}.{safe_db}.snap_{safe_tbl}_latest"
# uc_view   = f"{DATABRICKS_CATALOG}.{safe_db}.vw_{safe_tbl}_latest"
# spark.sql(f"USE CATALOG {DATABRICKS_CATALOG}")
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABRICKS_CATALOG}.{safe_db}")
# df = spark.read.option("mergeSchema","true").parquet(*live_files) if live_files else spark.createDataFrame([], "snap_file STRING")
# df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(uc_table)
# spark.sql(f"DROP VIEW IF EXISTS {uc_view}")
# spark.sql(f"CREATE VIEW {uc_view} AS SELECT * FROM {uc_table}")
# print(f"✅ {uc_table}")
# print(f"✅ {uc_view}")

print("Cell 9 — single-table registration: SKIPPED (manual-only cell)")
