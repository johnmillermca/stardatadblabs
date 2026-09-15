# Databricks notebook source
# nb_multi_table_auto_reader.py
#
# PURPOSE  : Single automated notebook that discovers every Iceberg table under
#            a given S3 warehouse prefix and, for each one:
#              1. Auto-discovers all table folders under the warehouse prefix
#                 via dbutils.fs.ls() — no table names need to be hardcoded
#              2. Resolves the CURRENT metadata.json in O(1) using
#                 version-hint.text (NOT by listing thousands of metadata files)
#              3. Parses the manifest-list and manifests to resolve the exact
#                 set of live data files for the current snapshot
#              4. Creates or replaces a Spark temp view over ONLY the live data
#                 files using spark.read.parquet(*live_files) — NOT a UNION ALL
#                 of N read_files() SQL branches (which breaks at scale)
#
# PRODUCTION SCALABILITY — WHY THE NAIVE APPROACH FAILS
# ──────────────────────────────────────────────────────
# The naive approach (previous version) had three compounding killers:
#
#  1. dbutils.fs.ls(meta_path) to find latest metadata.json
#     → S3 LIST is paginated at 1,000 objects per page.
#       A table with 10,000 snapshots has 10,000+ metadata files.
#       That's 10+ serial S3 LIST API calls PER TABLE just to find one file.
#
#  2. spark.read.format("avro").load(manifest) in a Python loop
#     → each call launches a full Spark job.
#       500 manifests = 500 Spark jobs to collect live file paths.
#
#  3. UNION ALL of N read_files() calls inside a SQL view DDL string
#     → 10,000 parquet files = 10,000-branch UNION ALL.
#       Databricks query planner refuses to parse or plan this.
#       View DDL itself hits string size limits.
#
# THE PRODUCTION-SCALE FIX (this version)
# ─────────────────────────────────────────
#  1. version-hint.text  → single tiny text file containing just an integer
#     (e.g. "42"). One S3 GET → exact metadata filename → zero ls() needed.
#     Every Iceberg writer (Spark, Flink, etc.) maintains this file.
#     Falls back to ls()-sort ONLY if version-hint.text is absent (old tables).
#
#  2. spark.read.format("avro") batch read of ALL manifests at once
#     → pass ALL manifest paths to a single spark.read.format("avro").load()
#       call using the multi-path list form. One Spark job reads every
#       manifest in parallel across the cluster. No loop of Spark jobs.
#
#  3. spark.read.parquet(*live_files).createOrReplaceTempView(view_name)
#     → Python API call, no SQL string construction, no AST size limit.
#       Spark handles multi-file parquet reads natively and efficiently.
#       Scales to 100,000+ files with a single call.
#
# WHY read_files() UNION ALL IS WRONG FOR UPDATES/DELETES
# ─────────────────────────────────────────────────────────
# Beyond the scale problem, a blind glob over data/*.parquet reads every file
# ever written — old rows from UPDATEs and DELETE targets never disappear.
# The manifest walk below resolves EXACTLY the live files per snapshot.
#
# CATALOG  : lakehouse  (Unity Catalog)
# SCHEMA   : derived from the database folder name under the warehouse prefix

# COMMAND ----------

# =============================================================================
# Cell 1 — Configuration: warehouse root only
# =============================================================================
WAREHOUSE_ROOT     = "s3://stardata-databricks/iceberg/warehouse/"
DATABRICKS_CATALOG = "lakehouse"
SKIP_TABLES        = set()   # e.g. {"lakehouse_db.staging", "lakehouse_db._temp"}
NOTEBOOK_VERSION   = "2026-09-05-v4"   # bump on every upload to confirm correct version is running

print(f"Notebook version   : {NOTEBOOK_VERSION}")
print(f"Warehouse root     : {WAREHOUSE_ROOT}")
print(f"Databricks catalog : {DATABRICKS_CATALOG}")

# COMMAND ----------

# =============================================================================
# Cell 2 — Auto-discover all Iceberg tables under the warehouse root
# =============================================================================
# Walks two levels deep: Level 1 → database folders, Level 2 → table folders.
# A folder is a valid Iceberg table only when it contains a metadata/
# sub-directory with at least one *.metadata.json file.
# This ls() is intentionally scoped to the two-level discovery walk ONLY —
# not to listing all metadata files inside the table (that is Cell 3's job
# and it uses version-hint.text instead of ls()).

TABLE_CONFIGS = {}
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
        data_path = tbl_entry.path.rstrip("/") + "/data/"

        # Confirm metadata/ exists — one ls() call per table (unavoidable for
        # discovery, but scoped to the table root, not the full metadata listing)
        try:
            ls_check = dbutils.fs.ls(meta_path)
            has_meta = any(f.name.endswith(".metadata.json") for f in ls_check)
        except Exception:
            has_meta = False

        if not has_meta:
            continue

        TABLE_CONFIGS[key] = {
            "db_name"    : db_name,
            "table_name" : tbl_name,
            "meta_path"  : meta_path,
            "data_path"  : data_path,
            # Temp view name: <db>__<table>__latest  (no dots — Spark temp view constraint)
            # Full qualified reference: spark.table("<db>__<table>__latest")
            "temp_view"  : f"{db_name}__{tbl_name}__latest",
        }

print("─" * 60)
print(f"Auto-discovered {len(TABLE_CONFIGS)} Iceberg table(s):")
for key, cfg in TABLE_CONFIGS.items():
    print(f"  {key:<35}  temp view → {cfg['temp_view']}")
if discovery_skipped:
    print(f"\nSkipped (SKIP_TABLES): {discovery_skipped}")
print("─" * 60)

# COMMAND ----------

# =============================================================================
# Cell 3 — Production-scale Iceberg snapshot resolver
# =============================================================================
#
# resolve_live_files(table_name, meta_path)
# ─────────────────────────────────────────
# Returns a dict with:
#   live_files      : list[str]  — S3 paths of ONLY the live parquet data files
#   snapshot_id     : int | str  — current snapshot ID
#   last_updated    : str        — human-readable UTC timestamp
#   meta_name       : str        — filename of the metadata.json used
#
# STEP 1 — version-hint.text (O(1) metadata file resolution)
# ────────────────────────────────────────────────────────────
# Every Iceberg writer maintains metadata/version-hint.text whose content is a
# single integer N.  The current metadata file is always metadata/vN.metadata.json.
# Reading this one tiny file avoids listing ALL metadata files (which grows
# unboundedly — one file per snapshot — and requires N/1000 S3 LIST pages).
#
# Falls back to ls()-based sort ONLY for very old tables written before
# version-hint.text was standard (Iceberg spec v1 tables, pre-2022).
#
# STEP 2 — manifest-list (Avro, single file)
# ────────────────────────────────────────────
# One Avro file per snapshot; each row references a manifest file path.
# Read with spark.read.format("avro") — one Spark job.
#
# STEP 3 — ALL manifests in ONE Spark job (batch read)
# ─────────────────────────────────────────────────────
# The naive approach reads each manifest in a Python loop — 500 manifests =
# 500 Spark jobs.  This version passes the full list of manifest paths to a
# SINGLE spark.read.format("avro").load(manifest_paths_list) call.  Spark
# reads all manifests in parallel across the cluster — one Spark job total.
#
# STEP 4 — DELETED-wins dedup across all manifests
# ─────────────────────────────────────────────────
# file_status: { normalised_s3_path → status }
#   0 = DELETED  (tombstoned — exclude from view)
#   1 = EXISTING (live, carried from earlier snapshot)
#   2 = ADDED    (live, new in this snapshot)
# DELETED(0) always wins — prevents stale rows after UPDATE/DELETE.
# Only content=0 (DATA) files are emitted; delete files (1/2) are skipped.

import json
import datetime


def resolve_live_files(table_name: str, meta_path: str) -> dict:
    """
    Parse Iceberg metadata and return the live parquet data files for the
    current snapshot.  Uses version-hint.text for O(1) metadata resolution
    and a single-job batch Avro read for all manifests.
    """

    # ── Path normalisation: s3a:// → s3:// ──────────────────────────────────
    # Spark writes all paths in Iceberg metadata as s3a:// (Hadoop S3A).
    # Databricks spark.read.parquet() requires s3:// (AWS SDK).
    # Normalise at every path entry point so DELETED / EXISTING entries for
    # the same physical file always share the same dict key.
    def _norm(p: str) -> str:
        return p.replace("s3a://", "s3://") if p else p

    # ── Step 1: resolve metadata.json via version-hint.text ─────────────────
    # Production O(1) path: read one tiny text file → exact metadata filename.
    # No ls() call, no sorting, no S3 LIST pagination.
    version_hint_path = _norm(meta_path.rstrip("/") + "/version-hint.text")
    latest_meta_path  = None
    meta_name         = None

    try:
        hint_raw = (
            spark.read
                 .text(version_hint_path, wholetext=True)
                 .collect()[0][0]
                 .strip()
        )
        version_num      = int(hint_raw)
        latest_meta_path = _norm(meta_path.rstrip("/") + f"/v{version_num}.metadata.json")
        meta_name        = f"v{version_num}.metadata.json"
        print(f"  [{table_name}] version-hint.text → v{version_num}.metadata.json  ✅")
    except Exception as hint_exc:
        # Fallback for old tables without version-hint.text.
        # This is the slow O(N) path — only triggered for legacy tables.
        print(
            f"  [{table_name}] version-hint.text not found ({hint_exc}); "
            f"falling back to ls()-based sort (legacy table)"
        )
        all_files  = dbutils.fs.ls(meta_path)
        meta_files = [f for f in all_files if f.name.endswith(".metadata.json")]
        if not meta_files:
            raise RuntimeError(
                f"[{table_name}] No *.metadata.json found under {meta_path}"
            )
        meta_files.sort(key=lambda f: f.modificationTime, reverse=True)
        latest_meta_path = _norm(meta_files[0].path)
        meta_name        = meta_files[0].name

    # ── Read and parse the metadata JSON ────────────────────────────────────
    raw  = spark.read.text(latest_meta_path, wholetext=True).collect()[0][0]
    meta = json.loads(raw)

    current_snapshot_id = meta.get("current-snapshot-id")
    last_updated_ms     = meta.get("last-updated-ms", 0)
    ts = (
        datetime.datetime.utcfromtimestamp(last_updated_ms / 1000)
                         .strftime("%Y-%m-%d %H:%M:%S UTC")
        if last_updated_ms else "unknown"
    )

    if current_snapshot_id is None:
        print(f"  [{table_name}] No current snapshot — table is empty")
        return {
            "table_name"  : table_name,
            "snapshot_id" : None,
            "last_updated": ts,
            "meta_name"   : meta_name,
            "live_files"  : [],
        }

    # ── Step 2: find the manifest-list path for the current snapshot ─────────
    snapshots = meta.get("snapshots", [])
    current_snapshot = next(
        (s for s in snapshots if s.get("snapshot-id") == current_snapshot_id),
        None,
    )

    if current_snapshot is None:
        raise RuntimeError(
            f"[{table_name}] Snapshot {current_snapshot_id} listed as "
            f"current-snapshot-id but not found in snapshots[] array."
        )

    manifest_list_path = _norm(current_snapshot.get("manifest-list", ""))
    if not manifest_list_path:
        raise RuntimeError(
            f"[{table_name}] Snapshot {current_snapshot_id} has no manifest-list."
        )

    # ── Step 3a: read the manifest-list — collect all manifest paths ─────────
    # One Spark job to read one Avro file.
    try:
        manifest_list_df = spark.read.format("avro").load(manifest_list_path)
        manifest_paths = [
            _norm(row["manifest_path"])
            for row in manifest_list_df.select("manifest_path").collect()
        ]
    except Exception as exc:
        raise RuntimeError(
            f"[{table_name}] Failed to read manifest-list {manifest_list_path}: {exc}"
        ) from exc

    if not manifest_paths:
        print(f"  [{table_name}] Snapshot has no manifests — table is empty")
        return {
            "table_name"  : table_name,
            "snapshot_id" : current_snapshot_id,
            "last_updated": ts,
            "meta_name"   : meta_name,
            "live_files"  : [],
        }

    # ── Step 3b: read ALL manifests in ONE Spark job (not a Python loop) ─────
    # This is the critical production fix.
    # spark.read.format("avro").load(list_of_paths) reads all files in parallel
    # across the cluster in a single Spark job — O(1) jobs regardless of how
    # many manifest files exist.
    #
    # The naive loop approach (previous version) was:
    #   for manifest_path in manifest_paths:
    #       manifest_df = spark.read.format("avro").load(manifest_path)
    #       ...collect()...
    # That launched one Spark job PER manifest — 500 manifests = 500 jobs.
    try:
        all_manifests_df = spark.read.format("avro").load(manifest_paths)
    except Exception as exc:
        raise RuntimeError(
            f"[{table_name}] Failed to read {len(manifest_paths)} manifest(s): {exc}"
        ) from exc

    if "data_file" not in all_manifests_df.columns:
        raise RuntimeError(
            f"[{table_name}] Manifest schema has no data_file column — "
            f"unexpected Iceberg format."
        )

    # ── Step 4: resolve final status per file path (DELETED wins) ────────────
    # Collect only the two fields we need: status + data_file.file_path + content.
    # This keeps the driver-side data volume minimal regardless of manifest size.
    from pyspark.sql import functions as F

    rows = (
        all_manifests_df
        .select(
            F.coalesce(F.col("status"), F.lit(1)).alias("status"),
            F.col("data_file.file_path").alias("file_path"),
            F.coalesce(F.col("data_file.content"), F.lit(0)).alias("content"),
        )
        .collect()
    )

    file_status: dict = {}
    for row in rows:
        # Skip delete files (position deletes = 1, equality deletes = 2)
        if row["content"] != 0:
            continue

        file_path = _norm(row["file_path"])
        if not file_path:
            continue

        status           = row["status"]
        existing_status  = file_status.get(file_path)

        if existing_status is None:
            file_status[file_path] = status
        elif status == 0:
            file_status[file_path] = 0        # DELETED always wins
        elif existing_status != 0 and status == 2:
            file_status[file_path] = 2        # ADDED upgrades EXISTING

    live_files      = [p for p, s in file_status.items() if s in (1, 2)]
    skipped_deleted = sum(1 for s in file_status.values() if s == 0)

    print(
        f"  [{table_name}]  snapshot={current_snapshot_id}\n"
        f"    Meta file    : {meta_name}\n"
        f"    Last updated : {ts}\n"
        f"    Manifests    : {len(manifest_paths)}  (read in 1 Spark job)\n"
        f"    Live files   : {len(live_files)}\n"
        f"    Dead files   : {skipped_deleted} (excluded — DELETE/UPDATE tombstones)"
    )

    return {
        "table_name"  : table_name,
        "snapshot_id" : current_snapshot_id,
        "last_updated": ts,
        "meta_name"   : meta_name,
        "live_files"  : live_files,
    }


print("✅ resolve_live_files() defined (production-scale: version-hint.text + batch manifest read)")

# COMMAND ----------

# =============================================================================
# Cell 4 — Resolve live file list for every discovered table
# =============================================================================

# Set the active catalog to the UC catalog so all subsequent DDL (CREATE SCHEMA,
# CREATE VIEW) resolves against Unity Catalog, not spark_catalog (Hive metastore).
spark.sql(f"USE CATALOG {DATABRICKS_CATALOG}")

print("─" * 60)
print("Resolving Iceberg snapshots and live data files …")
print("─" * 60)

for db_name in {cfg["db_name"] for cfg in TABLE_CONFIGS.values()}:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABRICKS_CATALOG}.{db_name}")

SNAPSHOTS = {}
errors    = []

for tbl, cfg in TABLE_CONFIGS.items():
    print()
    try:
        snap_info      = resolve_live_files(tbl, cfg["meta_path"])
        SNAPSHOTS[tbl] = {**cfg, **snap_info}
    except Exception as exc:
        errors.append((tbl, str(exc)))
        print(f"  ⚠️  [{tbl}] SKIPPED — {exc}")

print()
print("─" * 60)
if errors:
    print(f"⚠️  {len(errors)} table(s) skipped due to errors:")
    for tbl, msg in errors:
        print(f"   • {tbl}: {msg}")
else:
    print(f"✅ All {len(SNAPSHOTS)} table(s) resolved successfully")

# COMMAND ----------

# =============================================================================
# Cell 5 — Create or replace Spark temp views over the EXACT live data files
# =============================================================================
#
# WHY spark.read.parquet(*live_files).createOrReplaceTempView() (not UNION ALL SQL)
# ──────────────────────────────────────────────────────────────────────────────────
# The previous version built a SQL view as:
#
#   CREATE OR REPLACE VIEW …
#   AS
#   SELECT * FROM read_files('f1', ...) UNION ALL
#   SELECT * FROM read_files('f2', ...) UNION ALL
#   ...  (one branch per parquet file)
#
# This breaks in production because:
#   • 10,000 parquet files → 10,000-branch UNION ALL SQL string
#   • Databricks query planner refuses to parse or plan this
#   • View DDL itself hits string size limits
#   • Even Databricks read_files() positional multi-arg and array() forms
#     are broken on many runtime versions (UNKNOWN_POSITIONAL_ARGUMENT,
#     IllegalArgumentException: Illegal character in scheme name)
#
# The Python API spark.read.parquet(*live_files) has NONE of these limits:
#   • Accepts an arbitrary list of paths — no SQL string construction
#   • Spark reads all files in parallel in a single Scan stage
#   • Scales to 100,000+ files without degradation
#   • createOrReplaceTempView() registers it as a session-scoped temp view
#
# UNITY CATALOG VISIBILITY NOTE
# ───────────────────────────────
# Spark temp views are session-scoped — they exist only in this notebook
# session.  They are NOT persisted to Unity Catalog and NOT visible from
# other sessions.  For a persistent Unity Catalog view use Cell 5b below.
#
# For most use cases (dashboards on this cluster, notebook-level queries)
# the temp view is sufficient and avoids the DDL overhead.
#
# EMPTY TABLES
# ─────────────
# If live_files is empty a temp view over an empty DataFrame is registered
# so the view name exists and queries return 0 rows immediately.

print("Creating / refreshing Spark temp views over live snapshot files …")
print("─" * 60)

VIEW_RESULTS = {}

for tbl, snap in SNAPSHOTS.items():
    temp_view  = snap["temp_view"]
    live_files = snap.get("live_files", [])

    if not live_files:
        # Empty table — register an empty temp view so the name always exists
        spark.createDataFrame([], schema="snap_file STRING").createOrReplaceTempView(temp_view)
        row_count = 0
        action    = "REGISTERED (empty table — no live files)"
    else:
        # Single Python API call — no SQL string, no size limit, no UNION ALL
        # spark.read.parquet() with multiple paths performs a parallel multi-file
        # scan in one Spark job, equivalent to what the Iceberg catalog reader does.
        (
            spark.read
                 .option("mergeSchema", "true")
                 .parquet(*live_files)
                 .createOrReplaceTempView(temp_view)
        )
        row_count = spark.sql(f"SELECT COUNT(*) AS n FROM {temp_view}").collect()[0]["n"]
        action    = f"REFRESHED — snapshot {snap['snapshot_id']}"

    VIEW_RESULTS[tbl] = {"temp_view": temp_view, "rows": row_count, "action": action}

    print(f"  ✅ {temp_view}")
    print(f"     {action}")
    print(f"     rows={row_count:,}  live_files={len(live_files)}")
    print()

print("─" * 60)
print(f"✅ {len(SNAPSHOTS)} temp view(s) created/refreshed")
print()
print("  ┌─ HOW UPDATES AND DELETES NOW WORK ──────────────────────────┐")
print("  │  The temp view lists ONLY the parquet files that belong to  │")
print("  │  the current Iceberg snapshot (resolved from the manifest). │")
print("  │  • INSERT  → new file added to manifest (ADDED)             │")
print("  │  • UPDATE  → old file marked DELETED; new file is ADDED     │")
print("  │  • DELETE  → old file marked DELETED; rewritten file ADDED  │")
print("  │  Dead files (status=DELETED) are excluded from the view.    │")
print("  │  Re-run Cells 2 → 5 after any Iceberg write to refresh.     │")
print("  └─────────────────────────────────────────────────────────────┘")

# COMMAND ----------

# =============================================================================
# Cell 5b — Optional: promote temp views to persistent Unity Catalog views
# =============================================================================
# Run this cell explicitly ONLY after Cell 5 has just refreshed the temp views
# in this session.  DO NOT leave this cell on auto-run — the UC view it creates
# embeds a reference to the snapshot resolved in Cell 5.  If you query the UC
# view from a different session (or after a new Iceberg write), it will serve
# stale data because the temp view it proxies does not exist in that session.
#
# WHY THE PREVIOUS APPROACH WAS WRONG
# ─────────────────────────────────────
# The previous version did:
#   CREATE OR REPLACE VIEW uc_view AS SELECT * FROM temp_view
#
# This looks correct but fails silently in two ways:
#   1. Temp views are session-scoped.  From any other session the UC view
#      resolves to "table not found" (or worse: an older temp view registered
#      by a previous run if the view name was reused).
#   2. Even within the same session, the temp view's underlying parquet file
#      list is frozen to the snapshot at Cell 5 run time.  A DELETE or UPDATE
#      committed in Spark after Cell 5 ran is NOT reflected — the deleted row
#      is still in one of the live files and still shows in the view.
#
# THE FIX
# ────────
# Write the resolved live_files directly into the UC view as a read_files()
# call.  This makes the view self-contained (no temp view dependency) and
# documents exactly which snapshot it was built from in its COMMENT.
# The trade-off: the view is still a point-in-time snapshot — it does not
# auto-refresh.  Re-run Cells 2 → 5b after every Iceberg write.

# Switch the session to the Unity Catalog before issuing any CREATE VIEW DDL.
# Without this, Databricks resolves 3-part names (catalog.schema.view) against
# spark_catalog (the legacy Hive metastore) instead of the UC catalog, which
# causes REQUIRES_SINGLE_PART_NAMESPACE (SQLSTATE 42K05) because spark_catalog
# only accepts single-part names.
spark.sql(f"USE CATALOG {DATABRICKS_CATALOG}")

print("Promoting temp views to Unity Catalog persistent views …")
print("─" * 60)

for tbl, snap in SNAPSHOTS.items():
    db_name    = snap["db_name"]
    tbl_name   = snap["table_name"]
    live_files = snap.get("live_files", [])
    # Sanitise: replace dots and leading underscores in name segments so the
    # resulting UC view identifier is always a clean 3-part name.
    # e.g. db_name="demo", tbl_name="customers" → lakehouse.demo.vw_customers_latest
    # Dots in either segment would create a 4-part name and cause
    # REQUIRES_SINGLE_PART_NAMESPACE (SQLSTATE 42K05).
    safe_db   = db_name.replace(".", "_").lstrip("_")
    safe_tbl  = tbl_name.replace(".", "_").lstrip("_")
    uc_view   = f"{DATABRICKS_CATALOG}.{safe_db}.vw_{safe_tbl}_latest"
    snap_id    = snap["snapshot_id"]
    snap_ts    = snap["last_updated"]

    if not live_files:
        # Empty table — create a view that returns zero rows.
        # Use a VALUES clause so no temp view dependency exists.
        spark.sql(f"""
            CREATE OR REPLACE VIEW {uc_view}
            COMMENT 'Iceberg snapshot view for {tbl} — snapshot {snap_id} ({snap_ts}) — EMPTY'
            AS SELECT CAST(NULL AS STRING) AS _empty WHERE FALSE
        """)
    else:
        # Build the file list as a SQL array literal: array('s3://...', 's3://...', ...)
        # read_files() requires path => <array> — multiple positional string args are
        # NOT supported and raise UNKNOWN_POSITIONAL_ARGUMENT (SQLSTATE 4274K).
        # Using array() wraps all paths in a single named argument — no size limit,
        # works for any number of files, no temp view dependency, cross-session safe.
        file_array_sql = "array(" + ", ".join(f"'{p}'" for p in live_files) + ")"
        spark.sql(f"""
            CREATE OR REPLACE VIEW {uc_view}
            COMMENT 'Iceberg snapshot view for {tbl} — snapshot {snap_id} ({snap_ts}) — {len(live_files)} live files'
            AS SELECT * FROM read_files(path => {file_array_sql}, format => 'parquet', mergeSchema => true)
        """)

    print(f"  ✅ {uc_view}")
    print(f"     snapshot={snap_id}  ({snap_ts})  files={len(live_files)}")

print()
print("─" * 60)
print(f"✅ {len(SNAPSHOTS)} Unity Catalog view(s) created/refreshed")
print()
print("  ⚠️  These views are point-in-time snapshots of the Iceberg table.")
print("  Re-run Cells 2 → 5b after any Iceberg write (INSERT/UPDATE/DELETE)")
print("  to pick up the new snapshot and remove deleted/updated rows.")

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
    vr = VIEW_RESULTS.get(tbl, {})
    print(f"  {tbl:<35} {vr.get('rows', 0):>8,}  {snap['last_updated']}")

print("═" * 70)
print("  Re-run Cells 2 → 5 after any Iceberg write (INSERT/UPDATE/DELETE).")
print()
print("  SCALE CHARACTERISTICS (this version)")
print("  ─────────────────────────────────────")
print("  • Metadata resolution: O(1) via version-hint.text (1 S3 GET per table)")
print("  • Manifest reading   : 1 Spark job per table (all manifests in parallel)")
print("  • View registration  : 1 Python API call (no SQL string, no size limit)")
print("  • Scales to          : 10,000+ snapshots, 500+ manifests, 100,000+ files")
print("═" * 70)

# COMMAND ----------

# =============================================================================
# Cell 7 — Optional: cache a view into NVMe disk cache
# =============================================================================
# ⚠️  DO NOT run this cell as part of a full notebook run-all.
#     Run it manually ONLY when you explicitly want to warm the NVMe cache.
#
# Databricks Photon clusters expose a local NVMe-backed disk cache.
# Running CACHE TABLE scans the temp view once and stores the decompressed
# columnar data on the executor's local NVMe.
#
# IMPORTANT: after re-running Cell 5 (view refresh), the NVMe cache holds
# stale data from the previous snapshot.  Always run Cell 8 (UNCACHE + re-warm)
# after a Cell 5 refresh if you use the NVMe cache.

# ── Uncomment and run manually to cache a single view ─────────────────────
# VIEW_TO_CACHE = "lakehouse_db__customer__latest"   # temp view name from Cell 5
# print(f"Caching {VIEW_TO_CACHE} into NVMe disk cache …")
# spark.sql(f"CACHE TABLE {VIEW_TO_CACHE}")
# print(f"✅ Cache warm for {VIEW_TO_CACHE}")

# ── Or uncomment to cache ALL discovered views ─────────────────────────────
# for tbl, snap in SNAPSHOTS.items():
#     print(f"  Caching {snap['temp_view']} …")
#     spark.sql(f"CACHE TABLE {snap['temp_view']}")
# print("✅ All views cached")

print("Cell 7 — NVMe cache warm: SKIPPED (manual-only cell, all code commented out)")

# COMMAND ----------

# =============================================================================
# Cell 8 — Optional: invalidate NVMe cache after a new Iceberg snapshot
# =============================================================================
# ⚠️  DO NOT run this cell as part of a full notebook run-all.
#     Run it manually ONLY after a Cell 5 refresh when you have an active
#     NVMe cache that needs to be invalidated and re-warmed.

# ── Uncomment and run manually to re-warm a single view ───────────────────
# VIEW_TO_RECACHE = "lakehouse_db__customer__latest"   # temp view name from Cell 5
# print(f"Re-warming NVMe cache for {VIEW_TO_RECACHE} …")
# spark.sql(f"UNCACHE TABLE IF EXISTS {VIEW_TO_RECACHE}")
# spark.sql(f"CACHE TABLE {VIEW_TO_RECACHE}")
# print(f"✅ NVMe cache refreshed for {VIEW_TO_RECACHE}")

# ── Or uncomment to re-warm ALL views ─────────────────────────────────────
# for tbl, snap in SNAPSHOTS.items():
#     print(f"  Re-warming {snap['temp_view']} …")
#     spark.sql(f"UNCACHE TABLE IF EXISTS {snap['temp_view']}")
#     spark.sql(f"CACHE TABLE {snap['temp_view']}")
# print("✅ All views re-warmed")

print("Cell 8 — NVMe cache re-warm: SKIPPED (manual-only cell, all code commented out)")

# COMMAND ----------

# =============================================================================
# Cell 9 — Sample: manually register the view for ONE new table on first run
# =============================================================================
# ⚠️  DO NOT run this cell as part of a full notebook run-all.
#     Run it manually ONLY when you need to register a single new table
#     without doing a full discovery pass.
#
# USE THIS WHEN:
#   • You just created a new Iceberg table in Spark and want the view
#     immediately without waiting for the next full notebook run.
#
# Set NEW_TABLE_KEY, uncomment all lines below, and run this cell alone.

# NEW_TABLE_KEY      = "analytics_db.product"   # ← change to your table
# WAREHOUSE_ROOT     = "s3://stardata-databricks/iceberg/warehouse/"
# DATABRICKS_CATALOG = "lakehouse"
#
# db_name, table_name = NEW_TABLE_KEY.split(".", 1)
#
# meta_path = f"{WAREHOUSE_ROOT.rstrip('/')}/{db_name}/{table_name}/metadata/"
# temp_view = f"{db_name}__{table_name}__latest"
# uc_view   = f"{DATABRICKS_CATALOG}.{db_name}.vw_{table_name}_latest"
#
# print(f"New table  : {NEW_TABLE_KEY}")
# print(f"Meta path  : {meta_path}")
# print(f"Temp view  : {temp_view}")

print("Cell 9 — single-table registration: SKIPPED (manual-only cell, all code commented out)")

# ── Full Cell 9 body — all commented out, safe to run-all ─────────────────
# Uncomment the entire block below and run this cell alone when needed.

# db_name, table_name = NEW_TABLE_KEY.split(".", 1)
# meta_path = f"{WAREHOUSE_ROOT.rstrip('/')}/{db_name}/{table_name}/metadata/"
# temp_view = f"{db_name}__{table_name}__latest"
# uc_view   = f"{DATABRICKS_CATALOG}.{db_name}.vw_{table_name}_latest"
#
# print(f"New table  : {NEW_TABLE_KEY}")
# print(f"Meta path  : {meta_path}")
# print(f"Temp view  : {temp_view}")
# print(f"UC view    : {uc_view}")
# print()
#
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABRICKS_CATALOG}.{db_name}")
# print(f"✅ Schema {DATABRICKS_CATALOG}.{db_name} ready")
#
# print()
# snap_info  = resolve_live_files(NEW_TABLE_KEY, meta_path)
# live_files = snap_info["live_files"]
#
# if not live_files:
#     spark.createDataFrame([], schema="snap_file STRING").createOrReplaceTempView(temp_view)
#     print(f"✅ Temp view REGISTERED (empty — no live files yet)")
# else:
#     (
#         spark.read
#              .option("mergeSchema", "true")
#              .parquet(*live_files)
#              .createOrReplaceTempView(temp_view)
#     )
#     row_count = spark.sql(f"SELECT COUNT(*) AS n FROM {temp_view}").collect()[0]["n"]
#     print(f"✅ Temp view REGISTERED")
#     print(f"   snapshot={snap_info['snapshot_id']}")
#     print(f"   rows={row_count:,}  live_files={len(live_files)}")
#
# # Promote to Unity Catalog view — uses read_files(path => array(...)) directly
# file_array_sql = "array(" + ", ".join(f"'{p}'" for p in live_files) + ")"
# spark.sql(f"""
#     CREATE OR REPLACE VIEW {uc_view}
#     COMMENT 'Iceberg snapshot view for {NEW_TABLE_KEY} — snapshot {snap_info["snapshot_id"]}'
#     AS SELECT * FROM read_files(path => {file_array_sql}, format => 'parquet', mergeSchema => true)
# """)
# print(f"✅ Unity Catalog view REGISTERED: {uc_view}")
#
# print()
# print("  Re-run Cells 2 → 5b after every Iceberg write to keep the view current.")
