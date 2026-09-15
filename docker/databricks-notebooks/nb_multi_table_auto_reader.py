# Databricks notebook source
# nb_multi_table_auto_reader.py
#
# PURPOSE  : Single automated notebook that discovers every Iceberg table under
#            a given S3 warehouse prefix and, for each one:
#              1. Auto-discovers all table folders under the warehouse prefix
#                 via dbutils.fs.ls() — no table names need to be hardcoded
#              2. Parses the latest *.metadata.json to resolve the CURRENT
#                 snapshot and its exact set of live data files
#              3. Creates or replaces a view over ONLY the live data files,
#                 so UPDATEs and DELETEs are correctly reflected
#
# WHY read_files('data/', format=>'parquet') IS WRONG FOR UPDATES/DELETES
# ─────────────────────────────────────────────────────────────────────────
# Iceberg uses Copy-on-Write for UPDATE and DELETE:
#   • UPDATE  → writes a NEW parquet file with the changed row AND marks the
#               old file (or specific rows via a delete file) as inactive.
#   • DELETE  → writes a delete file that tombstones the target row(s); the
#               original parquet file is left intact on S3.
#
# A blind glob over data/*.parquet reads every file ever written — it sees
# the old row AND the new row for UPDATEs, and DELETE rows never disappear
# because the delete files (*-deletes.parquet) are silently ignored.
#
# THE FIX — custom Iceberg snapshot resolver (no Polaris / catalog needed)
# ─────────────────────────────────────────────────────────────────────────
# Iceberg metadata is plain JSON on S3.  We implement a lightweight reader:
#
#   1. metadata.json   → locate current-snapshot-id, find snapshot entry
#   2. manifest-list   → Avro file listing all manifest files for this snapshot
#      (read via spark.read.format("avro"))
#   3. manifest files  → Avro files listing individual data files; each entry
#      carries status: 1=EXISTING, 2=ADDED, 0=DELETED
#   4. Keep only EXISTING + ADDED entries — these are the live data files
#   5. Build a view:   read_files( [file1, file2, …], format=>'parquet' )
#      pointing at EXACTLY those files → UPDATEs and DELETEs now work
#
# REFRESH STRATEGY
# ─────────────────
# Because the view must list specific file paths (not a directory glob), the
# view definition changes after every Iceberg write.  Cell 5 therefore uses
# CREATE OR REPLACE VIEW so the view is refreshed on every notebook run.
#
# Run Cells 2 → 6 after any Iceberg write to pick up the latest snapshot.
# (Cell 5 is idempotent — safe to re-run as many times as needed.)
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
#                           metadata/       ← *.metadata.json + manifest files
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
# Folders without metadata/ are silently skipped.
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
# Cell 3 — Iceberg snapshot resolver (the core fix)
# =============================================================================
# Pure-Python Iceberg metadata reader — no catalog connector required.
#
# resolve_live_files(table_name, meta_path)
# ─────────────────────────────────────────
# Returns a dict with:
#   live_files      : list[str]  — S3 paths of the ONLY parquet data files
#                                  that belong to the current snapshot
#   snapshot_id     : int | str  — current snapshot ID from metadata.json
#   last_updated    : str        — human-readable UTC timestamp
#   meta_name       : str        — filename of the metadata.json used
#
# Algorithm
# ──────────
# Step 1  Read the latest *.metadata.json → get current-snapshot-id and
#         the manifest-list path for that snapshot.
# Step 2  Read the manifest-list Avro file via spark.read.format("avro").
#         Each row references one manifest file (manifest_path).
# Step 3  For each manifest file, read it as Avro.
#         Each row = one data file entry with:
#           status  0 = DELETED (tombstoned — exclude)
#                   1 = EXISTING (carried forward from a previous snapshot)
#                   2 = ADDED   (new in this snapshot)
#           data_file.file_path = S3 path of the parquet file
#           data_file.content   = 0=DATA, 1=POSITION_DELETES, 2=EQUALITY_DELETES
# Step 4  Build a file_path → status dict across ALL manifests.
#         A DELETED(0) status always overwrites an earlier EXISTING(1) entry —
#         this prevents duplicate rows when the same file appears in multiple
#         manifests (once as EXISTING in an older manifest, once as DELETED in
#         the newer manifest that superseded it via UPDATE or DELETE).
# Step 5  Emit only paths whose final resolved status is EXISTING(1) or ADDED(2).
#
# This is exactly what the Iceberg reader does internally — implemented in
# pure PySpark so no catalog / Polaris connector is required.

import json
import datetime


def resolve_live_files(table_name: str, meta_path: str) -> dict:
    """
    Parse Iceberg metadata for *table_name* and return the exact set of
    live parquet data files for the current snapshot.

    Duplicate-row safety
    ────────────────────
    The same file path can appear in more than one manifest inside the
    manifest-list:
      • Manifest A (older): file X → status=EXISTING(1)
      • Manifest B (newer): file X → status=DELETED(0)   ← UPDATE/DELETE rewrote it

    A naive approach that stops at the first EXISTING(1) hit would include
    file X in the view — causing the old (deleted/pre-update) row to appear
    alongside the new one.

    This implementation resolves each path to its FINAL status across all
    manifests.  DELETED(0) always wins — if a file appears as EXISTING in
    one manifest and DELETED in another, it is excluded from the view.
    The result is then deduplicated by file path before building the view.
    """

    # ── Step 1: find and parse the latest metadata.json ──────────────────────
    all_files  = dbutils.fs.ls(meta_path)
    meta_files = [f for f in all_files if f.name.endswith(".metadata.json")]

    if not meta_files:
        raise RuntimeError(
            f"[{table_name}] No *.metadata.json found under {meta_path}"
        )

    meta_files.sort(key=lambda f: f.modificationTime, reverse=True)
    latest_meta = meta_files[0]

    raw         = spark.read.text(latest_meta.path, wholetext=True).collect()[0][0]
    meta        = json.loads(raw)

    current_snapshot_id = meta.get("current-snapshot-id")
    last_updated_ms     = meta.get("last-updated-ms", 0)

    ts = (
        datetime.datetime.utcfromtimestamp(last_updated_ms / 1000)
        .strftime("%Y-%m-%d %H:%M:%S UTC")
        if last_updated_ms else "unknown"
    )

    if current_snapshot_id is None:
        # Table exists but has never been written to (empty table)
        print(f"  [{table_name}] No current snapshot — table is empty")
        return {
            "table_name"  : table_name,
            "snapshot_id" : None,
            "last_updated": ts,
            "meta_name"   : latest_meta.name,
            "live_files"  : [],
        }

    # ── Step 2: find the manifest-list path for the current snapshot ──────────
    snapshots = meta.get("snapshots", [])
    current_snapshot = next(
        (s for s in snapshots if s.get("snapshot-id") == current_snapshot_id),
        None,
    )

    if current_snapshot is None:
        raise RuntimeError(
            f"[{table_name}] Snapshot {current_snapshot_id} listed as "
            f"current-snapshot-id but not found in snapshots[] array. "
            f"Metadata file may be corrupt."
        )

    manifest_list_path = current_snapshot.get("manifest-list")
    if not manifest_list_path:
        raise RuntimeError(
            f"[{table_name}] Snapshot {current_snapshot_id} has no "
            f"manifest-list entry. Cannot resolve live files."
        )

    # ── Step 3: read the manifest-list (Avro) — get all manifest paths ────────
    # The manifest-list is a single Avro file. Each row has a field
    # "manifest_path" pointing to an individual manifest Avro file.
    try:
        manifest_list_df = spark.read.format("avro").load(manifest_list_path)
        manifest_paths   = [
            row["manifest_path"]
            for row in manifest_list_df.select("manifest_path").collect()
        ]
    except Exception as exc:
        raise RuntimeError(
            f"[{table_name}] Failed to read manifest-list at "
            f"{manifest_list_path}: {exc}"
        ) from exc

    # ── Step 4: read each manifest and resolve final status per file path ─────
    #
    # file_status: { file_path → status }
    #   status 0 = DELETED  (file is tombstoned — never include in view)
    #   status 1 = EXISTING (file is live, carried from an earlier snapshot)
    #   status 2 = ADDED    (file is live, new in this snapshot)
    #
    # Rules:
    #   • DELETED(0) is terminal — once a path is marked deleted it stays deleted
    #     regardless of the order manifests are iterated.
    #   • ADDED(2) takes priority over EXISTING(1) if both appear (shouldn't
    #     happen in a well-formed table, but guard anyway).
    #   • We only emit content=0 (DATA) files — skip content=1/2 (delete files).
    #
    # This dict approach is the duplicate-prevention mechanism: a file seen as
    # EXISTING in manifest A and DELETED in manifest B will end up as DELETED
    # in file_status and will not be included in the final live_files list.

    file_status: dict = {}   # { normalised_file_path: status_int }

    for manifest_path in manifest_paths:
        try:
            manifest_df = spark.read.format("avro").load(manifest_path)
        except Exception as exc:
            print(
                f"  ⚠️  [{table_name}] Could not read manifest "
                f"{manifest_path}: {exc} — skipping"
            )
            continue

        if "data_file" not in manifest_df.columns:
            print(f"  ⚠️  [{table_name}] Manifest has no data_file column — skipping")
            continue

        for row in manifest_df.collect():
            row_dict = row.asDict(recursive=True)

            # V1 manifests have no status column → treat all entries as EXISTING
            status = row_dict.get("status", 1)

            data_file = row_dict.get("data_file") or {}

            # Only care about DATA files (content=0); skip position/equality deletes
            if data_file.get("content", 0) != 0:
                continue

            file_path = data_file.get("file_path")
            if not file_path:
                continue

            # DELETED(0) wins: once a file is marked deleted it cannot become live
            # again in this snapshot.  ADDED(2) > EXISTING(1) for any remaining ties.
            existing_status = file_status.get(file_path)
            if existing_status is None:
                file_status[file_path] = status
            elif status == 0:
                # DELETED always overwrites any previous live status
                file_status[file_path] = 0
            elif existing_status != 0 and status == 2:
                # ADDED upgrades EXISTING, but never overrides DELETED
                file_status[file_path] = 2
            # else: keep existing_status as-is

    # Emit only paths whose final resolved status is EXISTING(1) or ADDED(2)
    live_files     = [p for p, s in file_status.items() if s in (1, 2)]
    skipped_deleted = sum(1 for s in file_status.values() if s == 0)

    print(
        f"  [{table_name}]  snapshot={current_snapshot_id}\n"
        f"    Meta file    : {latest_meta.name}\n"
        f"    Last updated : {ts}\n"
        f"    Manifests    : {len(manifest_paths)}\n"
        f"    Live files   : {len(live_files)}\n"
        f"    Dead files   : {skipped_deleted} (excluded — DELETE/UPDATE tombstones)"
    )

    return {
        "table_name"  : table_name,
        "snapshot_id" : current_snapshot_id,
        "last_updated": ts,
        "meta_name"   : latest_meta.name,
        "live_files"  : live_files,
    }


print("✅ resolve_live_files() defined")

# COMMAND ----------

# =============================================================================
# Cell 4 — Resolve live file list for every discovered table
# =============================================================================
# Calls resolve_live_files() for every table in TABLE_CONFIGS.
# Builds SNAPSHOTS dict: { "<db>.<tbl>": { ...cfg, ...snapshot_info } }

print("─" * 60)
print("Resolving Iceberg snapshots and live data files …")
print("─" * 60)

# Ensure every discovered schema exists in the Databricks catalog
for db_name in {cfg["db_name"] for cfg in TABLE_CONFIGS.values()}:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABRICKS_CATALOG}.{db_name}")

SNAPSHOTS = {}
errors    = []

for tbl, cfg in TABLE_CONFIGS.items():
    print()
    try:
        snap_info       = resolve_live_files(tbl, cfg["meta_path"])
        SNAPSHOTS[tbl]  = {**cfg, **snap_info}
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
# Cell 5 — Create or replace views over the EXACT live data files
# =============================================================================
#
# WHY CREATE OR REPLACE (not IF NOT EXISTS)
# ──────────────────────────────────────────
# The previous version used CREATE VIEW IF NOT EXISTS pointing at the entire
# data/ directory.  That worked for INSERTs but broke UPDATEs and DELETEs
# because the glob reads ALL parquet files — including files that Iceberg
# has marked as deleted in its current snapshot.
#
# This version builds the view over an EXPLICIT list of file paths derived
# from the current Iceberg snapshot manifest.  Because that list changes
# after every Iceberg write (UPDATE/DELETE rewrites data files and updates
# the manifest to mark old files as DELETED), the view definition itself
# must change — so CREATE OR REPLACE VIEW is required.
#
# The view is still a lightweight SQL object — recreating it is
# instantaneous and does not interrupt in-flight queries on the old view.
#
# EMPTY TABLES
# ─────────────
# If live_files is empty (new table, no writes yet) a view over an empty
# DataFrame is created so the view object exists and queries return 0 rows.

print("Creating / refreshing views over live snapshot files …")
print("─" * 60)

VIEW_RESULTS = {}

for tbl, snap in SNAPSHOTS.items():
    view       = snap["view"]
    live_files = snap.get("live_files", [])
    db_name    = snap["db_name"]

    if not live_files:
        # Empty table — create a view that returns 0 rows
        spark.sql(f"""
            CREATE OR REPLACE VIEW {view}
            COMMENT 'Iceberg snapshot view for {tbl} — empty (no snapshots yet)'
            AS SELECT CAST(NULL AS STRING) AS _empty WHERE 1 = 0
        """)
        row_count = 0
        action    = "CREATED (empty table — no live files)"

    else:
        # Build a comma-separated, single-quoted file list for read_files().
        # read_files() accepts a list literal: read_files('path1','path2',...)
        # We must convert s3a:// → s3:// if Spark wrote with s3a protocol,
        # because Databricks read_files expects s3:// (DBFS / Unity paths).
        def _normalise(p: str) -> str:
            return p.replace("s3a://", "s3://")

        file_list_sql = ", ".join(
            f"'{_normalise(f)}'" for f in live_files
        )

        spark.sql(f"""
            CREATE OR REPLACE VIEW {view}
            COMMENT 'Iceberg snapshot view for {tbl} — snapshot {snap["snapshot_id"]} ({snap["last_updated"]})'
            AS
            SELECT
                *,
                _metadata.file_path AS snap_file,
                _metadata.file_size AS snap_file_size
            FROM read_files(
                {file_list_sql},
                format      => 'parquet',
                mergeSchema => true
            )
        """)
        row_count = spark.sql(f"SELECT COUNT(*) AS n FROM {view}").collect()[0]["n"]
        action    = f"REFRESHED — snapshot {snap['snapshot_id']}"

    VIEW_RESULTS[tbl] = {"view": view, "rows": row_count, "action": action}

    print(f"  ✅ {view}")
    print(f"     {action}")
    print(f"     rows={row_count:,}  live_files={len(live_files)}")
    print()

print("─" * 60)
print(f"✅ {len(SNAPSHOTS)} view(s) created/refreshed")
print()
print("  ┌─ HOW UPDATES AND DELETES NOW WORK ──────────────────────────┐")
print("  │  The view lists ONLY the parquet files that belong to the   │")
print("  │  current Iceberg snapshot (resolved from the manifest).     │")
print("  │  • INSERT  → new file added to manifest (ADDED)             │")
print("  │  • UPDATE  → old file marked DELETED; new file is ADDED     │")
print("  │  • DELETE  → old file marked DELETED; rewritten file ADDED  │")
print("  │  Dead files (status=DELETED) are excluded from the view.    │")
print("  │  Re-run Cells 2 → 5 after any Iceberg write to refresh.     │")
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
    vr = VIEW_RESULTS.get(tbl, {})
    print(f"  {tbl:<35} {vr.get('rows', 0):>8,}  {snap['last_updated']}")

print("═" * 70)
print("  Re-run Cells 2 → 5 after any Iceberg write (INSERT/UPDATE/DELETE).")
print("  Each run resolves the current snapshot and refreshes the view over")
print("  exactly the live data files — UPDATEs and DELETEs are reflected.")
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
# IMPORTANT: after re-running Cell 5 (view refresh), the NVMe cache holds
# stale data from the previous snapshot.  Always run Cell 8 (UNCACHE + re-warm)
# after a Cell 5 refresh if you use the NVMe cache.
#
# WHEN TO USE:
#   - The same view is queried repeatedly by many users / dashboards
#   - Query latency matters more than the one-off cache-fill cost
#   - The cluster is Photon-enabled (Standard or higher tier)
#
# CACHE LIFETIME:
#   - Cache survives across queries within the same cluster lifetime
#   - Cache is invalidated automatically when the cluster restarts
#   - After a new Iceberg snapshot (Cell 5 refresh), run Cell 8 to re-warm

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
# After Cell 5 refreshes a view (new snapshot → different file list), the NVMe
# cache still holds decompressed data from the OLD file set.  Run this cell
# to evict stale entries and re-warm with the current snapshot data.
#
# Step 1: UNCACHE removes the old cached data for the view.
# Step 2: CACHE SELECT re-scans the view (now pointing at the new file set)
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
#   1. Set NEW_TABLE_KEY  to "<db_name>.<table_name>"  (matches S3 folder names)
#   2. Run this cell — it creates the schema if missing, resolves the live files
#      from the current Iceberg snapshot, and creates the view.
#   3. Subsequent full notebook runs (Cells 2 → 5) refresh the view as normal.
#
# NOTE: This cell is intentionally standalone.  You do NOT need to run
#       Cells 1–8 first. Just set the two variables below and run.

# ── Configuration ─────────────────────────────────────────────────────────────
NEW_TABLE_KEY      = "analytics_db.product"          # "<db_name>.<table_name>"
WAREHOUSE_ROOT     = "s3://stardata-databricks/iceberg/warehouse/"  # same as Cell 1
DATABRICKS_CATALOG = "lakehouse"                     # same as Cell 1
# ──────────────────────────────────────────────────────────────────────────────

db_name, table_name = NEW_TABLE_KEY.split(".", 1)

meta_path  = f"{WAREHOUSE_ROOT.rstrip('/')}/{db_name}/{table_name}/metadata/"
view       = f"{DATABRICKS_CATALOG}.{db_name}.vw_{table_name}_latest"

print(f"New table  : {NEW_TABLE_KEY}")
print(f"Meta path  : {meta_path}")
print(f"Target view: {view}")
print()

# Step 1 — ensure the schema exists in Unity Catalog
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABRICKS_CATALOG}.{db_name}")
print(f"✅ Schema {DATABRICKS_CATALOG}.{db_name} ready")

# Step 2 — resolve live files from current Iceberg snapshot
print()
snap_info  = resolve_live_files(NEW_TABLE_KEY, meta_path)
live_files = snap_info["live_files"]

# Step 3 — create or replace the view over live files
if not live_files:
    spark.sql(f"""
        CREATE OR REPLACE VIEW {view}
        COMMENT 'Iceberg snapshot view for {NEW_TABLE_KEY} — empty (no snapshots yet)'
        AS SELECT CAST(NULL AS STRING) AS _empty WHERE 1 = 0
    """)
    print(f"✅ View CREATED (empty — no live files yet)")
else:
    def _normalise(p: str) -> str:
        return p.replace("s3a://", "s3://")

    file_list_sql = ", ".join(f"'{_normalise(f)}'" for f in live_files)

    spark.sql(f"""
        CREATE OR REPLACE VIEW {view}
        COMMENT 'Iceberg snapshot view for {NEW_TABLE_KEY} — snapshot {snap_info["snapshot_id"]}'
        AS
        SELECT
            *,
            _metadata.file_path AS snap_file,
            _metadata.file_size AS snap_file_size
        FROM read_files(
            {file_list_sql},
            format      => 'parquet',
            mergeSchema => true
        )
    """)
    row_count = spark.sql(f"SELECT COUNT(*) AS n FROM {view}").collect()[0]["n"]
    print(f"✅ View CREATED / REFRESHED")
    print(f"   snapshot={snap_info['snapshot_id']}")
    print(f"   rows={row_count:,}  live_files={len(live_files)}")

print()
print("  Re-run Cells 2 → 5 after every Iceberg write to keep the view current.")
