# Databricks notebook source
# nb_multi_table_auto_reader.py
#
# PURPOSE  : Single automated notebook that discovers every Iceberg table under
#            a given S3 warehouse prefix and, for each one:
#              1. Auto-discovers all table folders under the warehouse prefix
#                 via boto3 (NOT dbutils.fs — serverless-safe)
#              2. Finds the latest metadata.json by LastModified DESC
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
#   data files.  Applying delete files requires the Iceberg engine JAR.
#   These tables are detected automatically and skipped.
#
# CREDENTIALS / SERVERLESS CONSTRAINTS
# ──────────────────────────────────────
# Databricks Serverless compute enforces TWO hard restrictions:
#   1. dbutils.fs.ls("s3://...") → AnonymousAWSCredentials 403
#      Fix: use boto3 (credentials from os.environ) for all S3 listing/reading.
#   2. spark.conf.set("fs.s3a.*") → CONFIG_NOT_AVAILABLE (blocklisted)
#      Fix: read Parquet via PyArrow (boto3 presigned-URL stream) then convert
#           to Spark DataFrame in-memory. Spark never touches S3 directly.
#
# CATALOG  : workspace  (Unity Catalog built-in)
# SCHEMA   : derived from the database folder name under the warehouse prefix

# COMMAND ----------

# =============================================================================
# Cell 1 — Install dependencies (run once per serverless session)
# =============================================================================
%pip install \
    "boto3>=1.26.0" \
    "s3fs>=2023.1.0" \
    "pyarrow>=12.0.0" \
    "fastavro>=1.7.0" \
    --quiet

dbutils.library.restartPython()

# COMMAND ----------

# =============================================================================
# Cell 2 — Configuration + AWS credentials
# =============================================================================
import os

# ── S3 / Iceberg warehouse ────────────────────────────────────────────────────
WAREHOUSE_ROOT     = "s3://stardata-databricks/iceberg/warehouse/"
S3_BUCKET          = "stardata-databricks"
S3_PREFIX          = "iceberg/warehouse"          # no leading slash
S3_REGION          = "us-east-2"
S3_ENDPOINT        = "https://s3.us-east-2.amazonaws.com"

# ── Databricks catalog ────────────────────────────────────────────────────────
# Using "workspace" (built-in) until the Unity Catalog metastore storage root
# is configured in accounts.cloud.databricks.com → Data → Metastores → Edit.
# Once "lakehouse" exists, change this to "lakehouse" and re-run.
DATABRICKS_CATALOG = "workspace"

SKIP_TABLES        = set()   # e.g. {"lakehouse_db.customer_test"}
NOTEBOOK_VERSION   = "2026-09-16-v14"  # bump on every upload

# ── AWS credentials (injected via os.environ — never via Spark/Hadoop) ────────
AK = "<AWS_ACCESS_KEY_ID>"
SK = "<AWS_SECRET_ACCESS_KEY>"
os.environ["AWS_ACCESS_KEY_ID"]     = AK
os.environ["AWS_SECRET_ACCESS_KEY"] = SK
os.environ["AWS_DEFAULT_REGION"]    = S3_REGION

print(f"Notebook version   : {NOTEBOOK_VERSION}")
print(f"Warehouse root     : {WAREHOUSE_ROOT}")
print(f"Databricks catalog : {DATABRICKS_CATALOG}")
print(f"AWS key            : {AK[:8]}...")
print("Credentials set in os.environ  (boto3/s3fs, NOT Spark/Hadoop) ✅")

# COMMAND ----------

# =============================================================================
# Cell 3 — Auto-discover all Iceberg tables via boto3 (serverless-safe)
# =============================================================================
import boto3

s3 = boto3.client(
    "s3",
    aws_access_key_id     = AK,
    aws_secret_access_key = SK,
    region_name           = S3_REGION,
    endpoint_url          = S3_ENDPOINT,
)

def s3_list_dirs(bucket: str, prefix: str) -> list[str]:
    """Return immediate child 'directory' prefixes under prefix/ (one level)."""
    prefix = prefix.rstrip("/") + "/"
    paginator = s3.get_paginator("list_objects_v2")
    dirs = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            dirs.append(cp["Prefix"])
    return dirs

def s3_list_files(bucket: str, prefix: str, suffix: str = "") -> list[dict]:
    """Return all objects under prefix/ with optional suffix filter."""
    prefix = prefix.rstrip("/") + "/"
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not suffix or obj["Key"].endswith(suffix):
                files.append(obj)
    return files

TABLE_CONFIGS     = {}
discovery_skipped = []

for db_prefix in s3_list_dirs(S3_BUCKET, S3_PREFIX):
    # db_prefix = "iceberg/warehouse/lakehouse_db/"
    db_name = db_prefix.rstrip("/").split("/")[-1]
    for tbl_prefix in s3_list_dirs(S3_BUCKET, db_prefix):
        tbl_name = tbl_prefix.rstrip("/").split("/")[-1]
        key      = f"{db_name}.{tbl_name}"
        if key in SKIP_TABLES:
            discovery_skipped.append(key)
            continue
        # Check metadata folder has at least one .metadata.json
        meta_objs = s3_list_files(
            S3_BUCKET, tbl_prefix.rstrip("/") + "/metadata", ".metadata.json"
        )
        if not meta_objs:
            continue
        safe_db  = db_name.replace(".", "_").lstrip("_")
        safe_tbl = tbl_name.replace(".", "_").lstrip("_")
        TABLE_CONFIGS[key] = {
            "db_name"   : db_name,
            "tbl_name"  : tbl_name,
            "safe_db"   : safe_db,
            "safe_tbl"  : safe_tbl,
            "tbl_prefix": tbl_prefix.rstrip("/"),   # s3 key prefix, no s3://bucket/
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
# Cell 4 — resolve_live_files() — manifest walk (pure Python, no Spark S3 I/O)
# =============================================================================
#
# Returns a dict:
#   live_files   : list[str]  — s3:// paths of live DATA parquet files
#   meta_name    : str        — metadata filename used
#   meta_ts      : str        — LastModified of that file (UTC)
#   snapshot_id  : int        — current-snapshot-id
#   last_updated : str        — last-updated-ms from metadata (UTC)
#   mor_skip     : bool       — True if table has delete files (merge-on-read)
#
# All S3 reads go through boto3.get_object (credentials from os.environ).
# spark.read is NEVER called for S3 — only for the final Delta write.
#
# Merge-on-read detection
# ────────────────────────
# After reading the manifest-list (Avro), check if any manifest has content=1
# (delete manifest).  If so, mor_skip=True — table is left unchanged.

import json
import datetime
import io
import fastavro

def _norm(p: str) -> str:
    return p.replace("s3a://", "s3://") if p else p

def _s3_get_json(key: str) -> dict:
    """Download an S3 object and parse as JSON."""
    resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return json.loads(resp["Body"].read())

def _s3_get_avro_records(key: str) -> list[dict]:
    """Download an S3 object and parse as Avro, return list of records."""
    resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
    buf  = io.BytesIO(resp["Body"].read())
    reader = fastavro.reader(buf)
    return list(reader)

def _s3_key_from_path(s3_path: str) -> str:
    """Strip s3://bucket/ prefix to get the bare S3 key."""
    # handles s3:// and s3a://
    path = _norm(s3_path)
    prefix = f"s3://{S3_BUCKET}/"
    if path.startswith(prefix):
        return path[len(prefix):]
    raise ValueError(f"Path does not belong to bucket {S3_BUCKET}: {s3_path}")


def resolve_live_files(table_name: str, tbl_prefix: str) -> dict:

    _base = {
        "table_name"  : table_name,
        "meta_name"   : "",
        "meta_ts"     : "",
        "snapshot_id" : None,
        "last_updated": "unknown",
        "mor_skip"    : False,
        "live_files"  : [],
    }

    # ── Step 1: latest metadata.json by LastModified DESC ────────────────
    meta_key_prefix = tbl_prefix.rstrip("/") + "/metadata"
    meta_objs = s3_list_files(S3_BUCKET, meta_key_prefix, ".metadata.json")
    if not meta_objs:
        raise RuntimeError(f"[{table_name}] No *.metadata.json under s3://{S3_BUCKET}/{meta_key_prefix}")

    meta_objs.sort(key=lambda o: o["LastModified"], reverse=True)
    top       = meta_objs[0]
    meta_key  = top["Key"]
    meta_name = meta_key.split("/")[-1]
    meta_ts   = top["LastModified"].strftime("%Y-%m-%d %H:%M:%S UTC")

    _base["meta_name"] = meta_name
    _base["meta_ts"]   = meta_ts

    # ── Step 2: parse metadata JSON ──────────────────────────────────────
    meta                = _s3_get_json(meta_key)
    current_snapshot_id = meta.get("current-snapshot-id")
    last_updated_ms     = meta.get("last-updated-ms", 0)
    last_updated_str = (
        datetime.datetime.utcfromtimestamp(last_updated_ms / 1000)
                         .strftime("%Y-%m-%d %H:%M:%S UTC")
        if last_updated_ms else "unknown"
    )
    _base["last_updated"] = last_updated_str

    if current_snapshot_id is None:
        print(f"  [{table_name}] No current snapshot — table is empty")
        return _base

    _base["snapshot_id"] = current_snapshot_id

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

    # ── Step 4: read manifest-list (Avro) ────────────────────────────────
    manifest_list_path = _norm(current.get("manifest-list", ""))
    if not manifest_list_path:
        raise RuntimeError(f"[{table_name}] Snapshot has no manifest-list")

    ml_key         = _s3_key_from_path(manifest_list_path)
    manifest_rows  = _s3_get_avro_records(ml_key)

    # ── Step 5: merge-on-read detection ──────────────────────────────────
    has_delete_manifests = any(r.get("content", 0) == 1 for r in manifest_rows)
    if has_delete_manifests:
        print(
            f"  [{table_name}] ⚠️  MERGE-ON-READ table detected "
            f"(delete manifests present) — SKIPPED"
        )
        return {**_base, "mor_skip": True}

    # ── Step 6: read all DATA manifests ──────────────────────────────────
    data_manifest_paths = [
        _norm(r["manifest_path"])
        for r in manifest_rows
        if r.get("content", 0) == 0
    ]
    if not data_manifest_paths:
        print(f"  [{table_name}] No data manifests — table is empty")
        return _base

    # ── Step 7: read each manifest (Avro) and collect file entries ────────
    all_rows = []
    for mp in data_manifest_paths:
        mk = _s3_key_from_path(mp)
        all_rows.extend(_s3_get_avro_records(mk))

    # ── Step 8: DELETED-wins dedup ────────────────────────────────────────
    # status: 0=DELETED, 1=EXISTING, 2=ADDED   content: 0=DATA, 1/2=delete file
    file_status = {}
    for row in all_rows:
        df = row.get("data_file") or row.get("r2_deleted_data_file") or {}
        if isinstance(df, dict):
            content = df.get("content", 0)
        else:
            content = 0
        if content != 0:
            continue
        fp = _norm(df.get("file_path", "") if isinstance(df, dict) else "")
        if not fp:
            continue
        status = row.get("status", 1)
        ex     = file_status.get(fp)
        if ex is None:
            file_status[fp] = status
        elif status == 0:
            file_status[fp] = 0
        elif ex != 0 and status == 2:
            file_status[fp] = 2

    live_files      = [p for p, s in file_status.items() if s in (1, 2)]
    skipped_deleted = sum(1 for s in file_status.values() if s == 0)

    print(
        f"  [{table_name}]  snapshot={current_snapshot_id}\n"
        f"    Meta file    : {meta_name}  ({meta_ts})\n"
        f"    Last updated : {last_updated_str}\n"
        f"    Manifests    : {len(data_manifest_paths)}\n"
        f"    Live files   : {len(live_files)}\n"
        f"    Dead files   : {skipped_deleted} (excluded)"
    )

    return {**_base, "live_files": live_files}


print("✅ resolve_live_files() defined  (boto3/fastavro — no Spark S3 I/O)")

# COMMAND ----------

# =============================================================================
# Cell 5 — Resolve snapshots for every discovered table
# =============================================================================

spark.sql(f"USE CATALOG {DATABRICKS_CATALOG}")

for safe_db in {cfg["safe_db"] for cfg in TABLE_CONFIGS.values()}:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABRICKS_CATALOG}.{safe_db}")

print("─" * 60)
print("Resolving Iceberg snapshots …")
print("─" * 60)

SNAPSHOTS   = {}
mor_skipped = []
errors      = []

for key, cfg in TABLE_CONFIGS.items():
    print()
    try:
        snap = resolve_live_files(key, cfg["tbl_prefix"])
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
# Cell 6 — Write Unity Catalog Delta tables + recreate UC views
#
# Databricks Serverless BLOCKS spark.conf.set("fs.s3a.*") — it is on a hard
# blocklist enforced by SparkConnectConfig.  Spark cannot read S3 directly.
#
# Fix: download each live Parquet file via boto3 → read with PyArrow in-memory
# → convert to Pandas → convert to Spark DataFrame → saveAsTable as Delta.
# Spark never touches S3.  Only boto3 (credentials from os.environ) does.
# =============================================================================

import pyarrow.parquet as pq
import pyarrow as pa
import io as _io

def _read_parquet_from_s3(keys: list) -> pa.Table:
    """Download + concat Parquet files from S3 using boto3 (no Spark S3 I/O)."""
    tables = []
    for key in keys:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        buf  = _io.BytesIO(resp["Body"].read())
        tables.append(pq.read_table(buf))
    if not tables:
        return pa.table({})
    unified = pa.unify_schemas([t.schema for t in tables])
    return pa.concat_tables([t.cast(unified) for t in tables])

spark.sql(f"USE CATALOG {DATABRICKS_CATALOG}")

print("Writing Unity Catalog Delta tables and refreshing UC views …")
print("─" * 60)

VIEW_RESULTS = {}

for key, snap in SNAPSHOTS.items():
    uc_table   = snap["uc_table"]
    uc_view    = snap["uc_view"]
    live_files = snap.get("live_files", [])   # list of s3:// paths

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
        s3_keys     = [_s3_key_from_path(p) for p in live_files]
        arrow_table = _read_parquet_from_s3(s3_keys)
        df = spark.createDataFrame(arrow_table.to_pandas())
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

    VIEW_RESULTS[key] = {"rows": row_count}
    print(f"  ✅ {uc_table}")
    print(f"     {snap['meta_name']}  ({snap['meta_ts']})")
    print(f"     rows={row_count:,}  live_files={len(live_files)}")
    print(f"     view → {uc_view}  ✅")
    print()

print("─" * 60)
print(f"✅ {len(SNAPSHOTS)} Delta table(s) and UC view(s) refreshed")
print()
print("  ⚠️  Point-in-time snapshots — re-run Cells 3 → 6 after any Iceberg write.")

# COMMAND ----------

# =============================================================================
# Cell 7 — Summary report
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
print("  Copy-on-write (Spark/Polaris) : boto3 manifest walk → Delta  ✅")
print("  Merge-on-read (Presto/Trino)  : auto-detected → skipped      ⏭️")
print("═" * 70)

# COMMAND ----------

# =============================================================================
# Cell 8 — Optional: NVMe cache warm (manual only)
# =============================================================================
# ⚠️  DO NOT run as part of run-all. Manual only.

# VIEW_TO_CACHE = "lakehouse_db__customer__latest"
# spark.sql(f"CACHE TABLE {VIEW_TO_CACHE}")

print("Cell 8 — NVMe cache warm: SKIPPED (manual-only cell)")

# COMMAND ----------

# =============================================================================
# Cell 9 — Optional: single-table registration (manual only)
# =============================================================================
# ⚠️  DO NOT run as part of run-all. Manual only.

# NEW_TABLE_KEY      = "lakehouse_db.my_table"
# db_name, tbl_name = NEW_TABLE_KEY.split(".", 1)
# safe_db   = db_name.replace(".", "_").lstrip("_")
# safe_tbl  = tbl_name.replace(".", "_").lstrip("_")
# tbl_prefix = f"{S3_PREFIX}/{db_name}/{tbl_name}"
# snap      = resolve_live_files(NEW_TABLE_KEY, tbl_prefix)
# live_files = snap["live_files"]
# uc_table  = f"{DATABRICKS_CATALOG}.{safe_db}.snap_{safe_tbl}_latest"
# uc_view   = f"{DATABRICKS_CATALOG}.{safe_db}.vw_{safe_tbl}_latest"
# spark.sql(f"USE CATALOG {DATABRICKS_CATALOG}")
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABRICKS_CATALOG}.{safe_db}")
# s3a_files = [p.replace("s3://", "s3a://") for p in live_files]
# df = spark.read.option("mergeSchema","true").parquet(*s3a_files) if live_files else spark.createDataFrame([], "snap_file STRING")
# df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(uc_table)
# spark.sql(f"DROP VIEW IF EXISTS {uc_view}")
# spark.sql(f"CREATE VIEW {uc_view} AS SELECT * FROM {uc_table}")
# print(f"✅ {uc_table}")
# print(f"✅ {uc_view}")

print("Cell 9 — single-table registration: SKIPPED (manual-only cell)")
