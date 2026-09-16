# Databricks notebook source
# nb_pyiceberg_catalog.py
#
# PURPOSE  : Catalog, inspect, and query every Iceberg table under the S3
#            warehouse using PyIceberg's pure-Python FileSystem catalog.
#
#            ✅ Works on Databricks FREE tier (Community Edition)
#            No Polaris.  No REST catalog.  No LOCATION DDL.
#            No Secrets API.  No Jobs API.  Just Workspace + S3 + PyArrow.
#
# HOW TO USE (free tier)
# ──────────────────────
# 1. Upload this notebook via the script:
#      bash scripts/databricks/run_nb_pyiceberg_catalog.sh
#    or manually: Workspace → Import → upload this .py file
#
# 2. Attach to your cluster (create one in UI if none exists:
#    Compute → Create cluster → Single Node → DBR 16.4 LTS)
#
# 3. Run Cell 1 (%pip install) FIRST and wait for it to finish.
#
# 4. Fill in the S3 credentials widget that appears after Cell 2 runs,
#    then run the remaining cells.
#
# WHY WIDGETS instead of dbutils.secrets
# ────────────────────────────────────────
# Databricks Free / Community Edition does NOT support the Secrets API
# (dbutils.secrets raises PermissionDenied on free tier).
# Widgets are the free-tier equivalent — values are entered in the UI
# and never appear in notebook output or logs.
#
# NOTEBOOK VERSION
NOTEBOOK_VERSION = "2026-09-15-v3"

# COMMAND ----------

# =============================================================================
# Cell 1 — Install PyIceberg (run first, wait for restart)
#
# Pin botocore to >=1.40.45,<1.41.0 so it stays compatible with the
# boto3 1.40.x that ships with Databricks Free / Community Edition.
# Without the pin, PyIceberg pulls botocore 1.43.x which breaks boto3.
#
# dbutils.library.restartPython() restarts the kernel so the newly
# installed packages are importable in subsequent cells without a
# manual "Detach & Re-attach".
# =============================================================================
%pip install \
    "pyiceberg[s3fs,pyarrow,sql-sqlite]>=0.7.0" \
    "sqlalchemy>=1.4.0,<3.0.0" \
    "botocore>=1.40.45,<1.41.0" \
    "boto3>=1.40.45,<1.41.0" \
    --quiet

dbutils.library.restartPython()

# COMMAND ----------

# =============================================================================
# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  REFRESH CELL — run this single cell to pull latest S3 data             ║
# ║                                                                          ║
# ║  Does everything in one shot:                                            ║
# ║    1. Reloads config + S3 credentials from widgets                       ║
# ║    2. Rebuilds the PyIceberg catalog index from S3                       ║
# ║    3. Re-discovers the latest metadata.json for every table              ║
# ║    4. Overwrites the Delta table with the latest Iceberg snapshot        ║
# ║    5. Recreates the SQL view                                             ║
# ║    6. Displays the refreshed data                                        ║
# ║                                                                          ║
# ║  SAVE THIS NOTEBOOK:                                                     ║
# ║    Workspace → /Shared/stardata/nb_pyiceberg_catalog  (already here)    ║
# ║  To make a dedicated one-click refresh shortcut:                         ║
# ║    Workspace → /Shared/stardata/nb_iceberg_refresh    (separate copy)   ║
# ║    → contains only this cell — bookmark it for daily use                ║
# ╚══════════════════════════════════════════════════════════════════════════╝
#
# PRE-REQUISITE: Run Cell 1 (%pip install) once per cluster session first.
#                Widgets (Cell 2) must have S3 credentials filled in.
# =============================================================================

import os, datetime as _dt, s3fs as _s3fs
from pyiceberg.catalog.sql import SqlCatalog

# ── Config (copy of Cell 3) ───────────────────────────────────────────────────
NOTEBOOK_VERSION   = "2026-09-15-v3"
WAREHOUSE_ROOT     = "s3://stardata-databricks/iceberg/warehouse/"
S3_REGION          = "us-east-2"
S3_ENDPOINT        = f"https://s3.{S3_REGION}.amazonaws.com"
CATALOG_DB_PATH    = "/tmp/stardata_pyiceberg_refresh.db"   # separate from main catalog db
PERSIST_NAMESPACE  = "lakehouse_db"
PERSIST_TABLE      = "customer_test"
DELTA_DBFS_PATH    = f"dbfs:/stardata_snapshots/{PERSIST_NAMESPACE}/{PERSIST_TABLE}"
SPARK_VIEW_NAME    = f"{PERSIST_NAMESPACE}__{PERSIST_TABLE}__live"

# ── S3 credentials (hardcoded for convenience) ───────────────────────────────
_ak = "<AWS_ACCESS_KEY_ID>"
_sk = "<AWS_SECRET_ACCESS_KEY>"
os.environ["AWS_ACCESS_KEY_ID"]     = _ak
os.environ["AWS_SECRET_ACCESS_KEY"] = _sk
os.environ["AWS_DEFAULT_REGION"]    = S3_REGION

print(f"{'─'*60}")
print(f"  ICEBERG → DELTA REFRESH")
print(f"  Target : {PERSIST_NAMESPACE}.{PERSIST_TABLE}")
print(f"  Bucket : {WAREHOUSE_ROOT}")
print(f"{'─'*60}")

# ── Step 1: Rebuild PyIceberg catalog index ───────────────────────────────────
if os.path.exists(CATALOG_DB_PATH):
    os.remove(CATALOG_DB_PATH)
_catalog = SqlCatalog(
    "stardata_refresh",
    **{
        "uri":                  f"sqlite:///{CATALOG_DB_PATH}",
        "warehouse":            WAREHOUSE_ROOT,
        "s3.access-key-id":     _ak,
        "s3.secret-access-key": _sk,
        "s3.region":            S3_REGION,
        "s3.endpoint":          S3_ENDPOINT,
        "s3fs.key":             _ak,
        "s3fs.secret":          _sk,
        "s3fs.endpoint_url":    S3_ENDPOINT,
    },
)
print("  [1/5] PyIceberg catalog initialised ✅")

# ── Step 2: Discover + register the target table from S3 ─────────────────────
_fs          = _s3fs.S3FileSystem(key=_ak, secret=_sk, endpoint_url=S3_ENDPOINT,
                                   client_kwargs={"region_name": S3_REGION})
_tbl_s3_path = WAREHOUSE_ROOT.replace("s3://","",1).rstrip("/") \
               + f"/{PERSIST_NAMESPACE}/{PERSIST_TABLE}"
_meta_prefix = _tbl_s3_path + "/metadata"

try:
    _meta_files = sorted(
        [f for f in _fs.ls(_meta_prefix, detail=True) if f["name"].endswith(".metadata.json")],
        key=lambda f: f.get("LastModified", 0), reverse=True
    )
except Exception as e:
    raise RuntimeError(f"Cannot list S3 path {_meta_prefix}: {e}")

if not _meta_files:
    raise RuntimeError(f"No metadata.json found at {_meta_prefix} — table may not exist yet.")

_latest_meta = "s3://" + _meta_files[0]["name"]
print(f"  [2/5] Latest metadata : {_meta_files[0]['name'].split('/')[-1]} ✅")

try:
    _catalog.create_namespace((PERSIST_NAMESPACE,))
except Exception:
    pass
try:
    _catalog.drop_table((PERSIST_NAMESPACE, PERSIST_TABLE))
except Exception:
    pass
_catalog.register_table((PERSIST_NAMESPACE, PERSIST_TABLE), _latest_meta)
print(f"  [3/5] Table registered in catalog ✅")

# ── Step 3: Read latest snapshot via PyIceberg ────────────────────────────────
_iceberg_tbl = _catalog.load_table((PERSIST_NAMESPACE, PERSIST_TABLE))
_snap        = _iceberg_tbl.current_snapshot()

if _snap is None:
    print(f"  ⚠️  No snapshot found — table is empty. Nothing to write.")
else:
    _snap_ts  = _dt.datetime.utcfromtimestamp(_snap.timestamp_ms / 1000).strftime("%Y-%m-%d %H:%M:%S UTC")
    _arrow    = _iceberg_tbl.scan().to_arrow()
    _df       = _arrow.to_pandas()
    row_count = len(_df)
    print(f"  [4/5] Snapshot read : {_snap.snapshot_id}  committed={_snap_ts}  rows={row_count:,} ✅")

    # ── Step 4: Overwrite Delta table + recreate SQL view ─────────────────────
    _spark_df = spark.createDataFrame(_df)
    (
        _spark_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(DELTA_DBFS_PATH)
    )
    spark.read.format("delta").load(DELTA_DBFS_PATH) \
         .createOrReplaceTempView(SPARK_VIEW_NAME)
    print(f"  [5/5] Delta table + SQL view refreshed ✅")

    print(f"{'─'*60}")
    print(f"  ✅  REFRESH COMPLETE")
    print(f"  Delta path : {DELTA_DBFS_PATH}")
    print(f"  SQL view   : {SPARK_VIEW_NAME}")
    print(f"  Rows       : {row_count:,}")
    print(f"  Snapshot   : {_snap_ts}")
    print(f"{'─'*60}")

    # ── Step 5: Display the refreshed data ───────────────────────────────────
    spark.sql(f"SELECT * FROM {SPARK_VIEW_NAME}").display()

# COMMAND ----------

# =============================================================================
# Cell 2 — Credential widgets
#
# Run this cell → text boxes appear at the top of the notebook.
# Paste your S3 access key and secret key into them, then run Cell 3+.
#
# These are the SAME credentials used by the rest of the platform:
#   access_key = <AWS_ACCESS_KEY_ID>    (from OpenBao secret/data/platform/s3)
#   secret_key = qgOK3RZCw...            (from OpenBao secret/data/platform/s3)
# Both keys work for both s3://xdatatoiceberg1 AND s3://stardata-databricks.
# =============================================================================

dbutils.widgets.text("s3_access_key", "", "S3 Access Key (AKIA...)")
dbutils.widgets.text("s3_secret_key", "", "S3 Secret Key")

print("▶ Fill in the S3 Access Key and Secret Key widgets above, then run Cell 3.")

# COMMAND ----------

# =============================================================================
# Cell 3 — Configuration
#
# NOTE: all variables are defined HERE (not in the file header) because
# dbutils.library.restartPython() in Cell 1 wipes the kernel state —
# anything defined before that restart is lost.
# =============================================================================

import os

# ── Notebook metadata ─────────────────────────────────────────────────────────
NOTEBOOK_VERSION = "2026-09-15-v3"

# ── S3 / Iceberg config ───────────────────────────────────────────────────────
WAREHOUSE_ROOT  = "s3://stardata-databricks/iceberg/warehouse/"
S3_REGION       = "us-east-2"
S3_ENDPOINT     = f"https://s3.{S3_REGION}.amazonaws.com"
CATALOG_DB_PATH = "/tmp/stardata_pyiceberg_catalog.db"   # rebuilt each run

# ── S3 credentials (hardcoded for convenience) ───────────────────────────────
S3_ACCESS_KEY = "<AWS_ACCESS_KEY_ID>"
S3_SECRET_KEY = "<AWS_SECRET_ACCESS_KEY>"

os.environ["AWS_ACCESS_KEY_ID"]     = S3_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = S3_SECRET_KEY
os.environ["AWS_DEFAULT_REGION"]    = S3_REGION

print(f"Notebook version : {NOTEBOOK_VERSION}")
print(f"Warehouse root   : {WAREHOUSE_ROOT}")
print(f"S3 region        : {S3_REGION}")
print(f"Access key       : {S3_ACCESS_KEY[:6]}...{S3_ACCESS_KEY[-4:]}  ✅")
print(f"Secret key       : {'*' * 20}  ✅")

# COMMAND ----------

# =============================================================================
# Cell 4 — Build PyIceberg SqlCatalog (ephemeral SQLite on driver /tmp)
# =============================================================================

from pyiceberg.catalog.sql import SqlCatalog

# Remove stale db from previous run
if os.path.exists(CATALOG_DB_PATH):
    os.remove(CATALOG_DB_PATH)

catalog = SqlCatalog(
    "stardata",
    **{
        "uri":                  f"sqlite:///{CATALOG_DB_PATH}",
        "warehouse":            WAREHOUSE_ROOT,
        "s3.access-key-id":     S3_ACCESS_KEY,
        "s3.secret-access-key": S3_SECRET_KEY,
        "s3.region":            S3_REGION,
        "s3.endpoint":          S3_ENDPOINT,
        "s3fs.key":             S3_ACCESS_KEY,
        "s3fs.secret":          S3_SECRET_KEY,
        "s3fs.endpoint_url":    S3_ENDPOINT,
    },
)

print("✅ PyIceberg SqlCatalog ready")
print(f"   Warehouse : {WAREHOUSE_ROOT}")

# COMMAND ----------

# =============================================================================
# Cell 5 — Auto-discover and register every Iceberg table from S3
#
# Walks s3://stardata-databricks/iceberg/warehouse/<db>/<table>/metadata/
# Finds the latest *.metadata.json per table and registers it.
# Handles BOTH Copy-on-Write (Spark) and Merge-on-Read (Presto/Trino) tables.
# =============================================================================

import s3fs as _s3fs

_fs = _s3fs.S3FileSystem(
    key=S3_ACCESS_KEY,
    secret=S3_SECRET_KEY,
    endpoint_url=S3_ENDPOINT,
    client_kwargs={"region_name": S3_REGION},
)

_warehouse_bare = WAREHOUSE_ROOT.replace("s3://", "", 1).rstrip("/")

registered      = []   # list of (key, metadata_file_name)
skipped_no_meta = []
errors          = []

try:
    db_dirs = _fs.ls(_warehouse_bare, detail=False)
except Exception as exc:
    raise RuntimeError(
        f"Cannot list S3 warehouse: {WAREHOUSE_ROOT}\n"
        f"Check credentials and bucket permissions.\nError: {exc}"
    )

for db_path in db_dirs:
    db_name = db_path.rstrip("/").split("/")[-1]
    if not db_name:
        continue

    try:
        catalog.create_namespace((db_name,))
    except Exception:
        pass  # already exists

    try:
        tbl_dirs = _fs.ls(db_path, detail=False)
    except Exception:
        continue

    for tbl_path in tbl_dirs:
        tbl_name   = tbl_path.rstrip("/").split("/")[-1]
        meta_prefix = f"{tbl_path.rstrip('/')}/metadata"
        identifier  = (db_name, tbl_name)
        key         = f"{db_name}.{tbl_name}"

        try:
            meta_files = [
                f for f in _fs.ls(meta_prefix, detail=True)
                if f["name"].endswith(".metadata.json")
            ]
        except Exception:
            skipped_no_meta.append(key)
            continue

        if not meta_files:
            skipped_no_meta.append(key)
            continue

        meta_files.sort(key=lambda f: f.get("LastModified", 0), reverse=True)
        latest_meta = "s3://" + meta_files[0]["name"]

        try:
            try:
                catalog.drop_table(identifier)
            except Exception:
                pass
            catalog.register_table(identifier, latest_meta)
            registered.append((key, meta_files[0]["name"].split("/")[-1]))
        except Exception as exc:
            errors.append((key, str(exc)))

# ── Print discovery summary ───────────────────────────────────────────────────
print("─" * 65)
print(f"✅  Registered : {len(registered)} table(s)")
for key, meta_file in registered:
    print(f"    {key:<45}  {meta_file}")

if skipped_no_meta:
    print(f"\n⏭️  Skipped (no metadata/) : {len(skipped_no_meta)}")
    for k in skipped_no_meta:
        print(f"    {k}")

if errors:
    print(f"\n⚠️  Errors : {len(errors)}")
    for k, m in errors:
        print(f"    {k}: {m}")

print("─" * 65)

# COMMAND ----------

# =============================================================================
# Cell 6 — Full catalog summary: schema + snapshot info for every table
# =============================================================================

import datetime as _dt

print(f"\n{'═' * 70}")
print("  ICEBERG CATALOG  —  s3://stardata-databricks/iceberg/warehouse/")
print(f"{'═' * 70}\n")

for key, _ in registered:
    db_name, tbl_name = key.split(".", 1)
    identifier = (db_name, tbl_name)
    print(f"  ┌─ {key}")
    try:
        tbl  = catalog.load_table(identifier)
        snap = tbl.current_snapshot()

        # Schema
        print(f"  │  Columns ({len(tbl.schema().fields)}):")
        for f in tbl.schema().fields:
            print(f"  │    {f.name:<30} {str(f.field_type):<20} nullable={f.optional}")

        # Partition spec
        if tbl.spec().fields:
            print(f"  │  Partitions:")
            for p in tbl.spec().fields:
                print(f"  │    {p.name}  →  {p.transform}")

        # Snapshot
        if snap:
            snap_ts = _dt.datetime.utcfromtimestamp(snap.timestamp_ms / 1000).strftime("%Y-%m-%d %H:%M:%S UTC")
            summary = snap.summary.additional_properties if snap.summary else {}
            print(f"  │  Snapshot    : {snap.snapshot_id}")
            print(f"  │  Committed   : {snap_ts}")
            print(f"  │  Operation   : {summary.get('operation','?')}")
            print(f"  │  Records     : total={summary.get('total-records','?')}  "
                  f"added={summary.get('added-records','?')}  "
                  f"deleted={summary.get('deleted-records','?')}")
        else:
            print(f"  │  Snapshot    : (none — table empty)")

    except Exception as exc:
        print(f"  │  ⚠️  Load error: {exc}")
    print(f"  └{'─' * 60}\n")

# COMMAND ----------

# =============================================================================
# Cell 7 — Read a table as pandas DataFrame
#
# Handles BOTH CoW (Spark) AND MoR (Presto/Trino) tables correctly.
# This is the key fix over nb_multi_table_auto_reader which skips MoR tables.
# =============================================================================

READ_NAMESPACE = "lakehouse_db"   # ← change to your namespace
READ_TABLE     = "customers"      # ← change to your table name
ROW_LIMIT      = 1000             # ← set None to read all rows

tbl         = catalog.load_table((READ_NAMESPACE, READ_TABLE))
arrow_table = tbl.scan(limit=ROW_LIMIT).to_arrow()
df          = arrow_table.to_pandas()

print(f"Table   : {READ_NAMESPACE}.{READ_TABLE}")
print(f"Rows    : {len(df):,}  (limit={ROW_LIMIT})")
print(f"Columns : {list(df.columns)}")
display(df)

# COMMAND ----------

# =============================================================================
# Cell 7b — Persist as a Spark Delta table + queryable SQL view
#
# Self-contained — does NOT depend on catalog from Cell 4.
# Can be run directly after Cell 1 (pip install) + Cell 2 (widgets).
# RE-RUN any time new data lands on S3 to refresh.
# =============================================================================

import os, datetime as _dt, s3fs as _s3fs
from pyiceberg.catalog.sql import SqlCatalog

# ── Config ────────────────────────────────────────────────────────────────────
PERSIST_NAMESPACE  = "lakehouse_db"
PERSIST_TABLE      = "customer_test"
WAREHOUSE_ROOT     = "s3://stardata-databricks/iceberg/warehouse/"
S3_REGION          = "us-east-2"
S3_ENDPOINT        = f"https://s3.{S3_REGION}.amazonaws.com"
DELTA_DBFS_PATH    = f"dbfs:/stardata_snapshots/{PERSIST_NAMESPACE}/{PERSIST_TABLE}"
SPARK_VIEW_NAME    = f"{PERSIST_NAMESPACE}__{PERSIST_TABLE}__live"

# ── S3 credentials (hardcoded for convenience) ───────────────────────────────
_ak = "<AWS_ACCESS_KEY_ID>"
_sk = "<AWS_SECRET_ACCESS_KEY>"
os.environ["AWS_ACCESS_KEY_ID"]     = _ak
os.environ["AWS_SECRET_ACCESS_KEY"] = _sk
os.environ["AWS_DEFAULT_REGION"]    = S3_REGION

# ── Build ephemeral catalog ───────────────────────────────────────────────────
_cat_path = "/tmp/stardata_7b.db"
if os.path.exists(_cat_path):
    os.remove(_cat_path)
_cat = SqlCatalog("stardata_7b", **{
    "uri": f"sqlite:///{_cat_path}", "warehouse": WAREHOUSE_ROOT,
    "s3.access-key-id": _ak, "s3.secret-access-key": _sk,
    "s3.region": S3_REGION, "s3.endpoint": S3_ENDPOINT,
    "s3fs.key": _ak, "s3fs.secret": _sk, "s3fs.endpoint_url": S3_ENDPOINT,
})

# ── Find latest metadata.json ─────────────────────────────────────────────────
_fs   = _s3fs.S3FileSystem(key=_ak, secret=_sk, endpoint_url=S3_ENDPOINT,
                            client_kwargs={"region_name": S3_REGION})
_meta_prefix = WAREHOUSE_ROOT.replace("s3://","",1).rstrip("/") \
               + f"/{PERSIST_NAMESPACE}/{PERSIST_TABLE}/metadata"
_mfiles = sorted(
    [f for f in _fs.ls(_meta_prefix, detail=True) if f["name"].endswith(".metadata.json")],
    key=lambda f: f.get("LastModified", 0), reverse=True
)
if not _mfiles:
    raise RuntimeError(f"No metadata.json at {_meta_prefix}")
_latest = "s3://" + _mfiles[0]["name"]

# ── Register + load ───────────────────────────────────────────────────────────
try:
    _cat.create_namespace((PERSIST_NAMESPACE,))
except Exception:
    pass
try:
    _cat.drop_table((PERSIST_NAMESPACE, PERSIST_TABLE))
except Exception:
    pass
_cat.register_table((PERSIST_NAMESPACE, PERSIST_TABLE), _latest)
_tbl  = _cat.load_table((PERSIST_NAMESPACE, PERSIST_TABLE))
_snap = _tbl.current_snapshot()

if _snap is None:
    print(f"⚠️  No snapshot — table is empty.")
else:
    _snap_ts  = _dt.datetime.utcfromtimestamp(_snap.timestamp_ms / 1000).strftime("%Y-%m-%d %H:%M:%S UTC")
    _df       = _tbl.scan().to_arrow().to_pandas()
    row_count = len(_df)

    spark.createDataFrame(_df).write \
        .format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(DELTA_DBFS_PATH)

    spark.read.format("delta").load(DELTA_DBFS_PATH) \
         .createOrReplaceTempView(SPARK_VIEW_NAME)

    print(f"✅  Delta table : {DELTA_DBFS_PATH}")
    print(f"    Snapshot   : {_snap_ts}")
    print(f"    Rows       : {row_count:,}")
    print(f"✅  SQL view   : {SPARK_VIEW_NAME}")
    print(f"    Query      : SELECT * FROM {SPARK_VIEW_NAME} LIMIT 20")

# COMMAND ----------

# =============================================================================
# Cell 7c — Query the live view with SQL (run after Cell 7b)
# =============================================================================

spark.sql(f"SELECT * FROM {SPARK_VIEW_NAME}").display()

# COMMAND ----------

# =============================================================================
# Cell 8 — Snapshot history / time travel
# =============================================================================

HISTORY_NAMESPACE = "lakehouse_db"
HISTORY_TABLE     = "customers"

tbl = catalog.load_table((HISTORY_NAMESPACE, HISTORY_TABLE))

print(f"Snapshot history — {HISTORY_NAMESPACE}.{HISTORY_TABLE}")
print("─" * 72)
print(f"  {'SNAPSHOT ID':<22} {'COMMITTED (UTC)':<24} {'OPERATION':<12} TOTAL RECORDS")
print("─" * 72)

for entry in tbl.history():
    snap = tbl.snapshot_by_id(entry.snapshot_id)
    if snap is None:
        continue
    ts      = _dt.datetime.utcfromtimestamp(snap.timestamp_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    summary = snap.summary.additional_properties if snap.summary else {}
    print(f"  {snap.snapshot_id:<22} {ts:<24} {summary.get('operation','?'):<12} "
          f"{summary.get('total-records','?')}")

print("─" * 72)
print()
print("Time-travel to a specific snapshot:")
print("  df = catalog.load_table((ns, tbl)).scan(snapshot_id=<id>).to_arrow().to_pandas()")

# COMMAND ----------

# =============================================================================
# Cell 9 — Append rows (INSERT)
#
# Writes a new snapshot directly to S3.
# Spark / Doris see it on their next read — no catalog update needed.
# =============================================================================

import pyarrow as pa
import datetime as _dt

APPEND_NAMESPACE = "lakehouse_db"
APPEND_TABLE     = "customers"

tbl = catalog.load_table((APPEND_NAMESPACE, APPEND_TABLE))

new_rows = pa.table(
    {
        "customer_id":    pa.array([99901],                  type=pa.int32()),
        "full_name":      pa.array(["Test PyIceberg User"],  type=pa.string()),
        "email":          pa.array(["pyiceberg@example.com"],type=pa.string()),
        "snap_id":        pa.array([999010001],              type=pa.int64()),
        "snap_timestamp": pa.array(
            [_dt.datetime.now(_dt.timezone.utc)],
            type=pa.timestamp("us", tz="UTC"),
        ),
    }
)

tbl.append(new_rows)
snap = tbl.current_snapshot()
print(f"✅ Appended {len(new_rows)} row(s) to {APPEND_NAMESPACE}.{APPEND_TABLE}")
print(f"   New snapshot: {snap.snapshot_id}")

# COMMAND ----------

# =============================================================================
# Cell 10 — Simulated DELETE (overwrite with rows filtered out)
# =============================================================================

import pyarrow.compute as pc

DELETE_NAMESPACE = "lakehouse_db"
DELETE_TABLE     = "customers"
DELETE_COLUMN    = "customer_id"
DELETE_VALUE     = 99901

tbl        = catalog.load_table((DELETE_NAMESPACE, DELETE_TABLE))
arrow_full = tbl.scan().to_arrow()
arrow_kept = arrow_full.filter(pc.not_equal(arrow_full[DELETE_COLUMN], DELETE_VALUE))

tbl.overwrite(arrow_kept)
print(f"✅ Deleted rows where {DELETE_COLUMN} = {DELETE_VALUE}")
print(f"   Rows before: {len(arrow_full):,}  →  after: {len(arrow_kept):,}")
print(f"   New snapshot: {tbl.current_snapshot().snapshot_id}")

# COMMAND ----------

# =============================================================================
# Cell 11 — Simulated UPDATE (read → modify in pandas → overwrite)
# =============================================================================

import pyarrow as pa
import pandas as pd

UPDATE_NAMESPACE = "lakehouse_db"
UPDATE_TABLE     = "customers"
UPDATE_COLUMN    = "customer_id"
UPDATE_VALUE     = 1
SET_COLUMN       = "customer_tier"
SET_VALUE        = "platinum"

tbl = catalog.load_table((UPDATE_NAMESPACE, UPDATE_TABLE))
df  = tbl.scan().to_arrow().to_pandas()
df.loc[df[UPDATE_COLUMN] == UPDATE_VALUE, SET_COLUMN] = SET_VALUE

arrow_updated = pa.Table.from_pandas(df, schema=tbl.schema().as_arrow())
tbl.overwrite(arrow_updated)

print(f"✅ Updated {UPDATE_NAMESPACE}.{UPDATE_TABLE}")
print(f"   Set {SET_COLUMN} = '{SET_VALUE}'  where {UPDATE_COLUMN} = {UPDATE_VALUE}")
print(f"   New snapshot: {tbl.current_snapshot().snapshot_id}")
