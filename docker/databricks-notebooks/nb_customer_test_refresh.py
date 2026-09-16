# Databricks notebook source
# nb_customer_test_refresh.py
#
# PURPOSE : Refresh workspace.lakehouse_db.snap_customer_test_latest
#           and workspace.lakehouse_db.vw_customer_test_latest from
#           s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer_test/
#
# HOW IT WORKS (serverless-safe — no dbutils.fs, no spark.conf.set fs.s3a.*):
#   1. Installs boto3 / fastavro / pyarrow via subprocess (no restart needed)
#   2. boto3 lists metadata/ and picks the latest .metadata.json
#   3. Reads the manifest-list (Avro) via boto3 → fastavro
#   4. Reads all DATA manifests → collects live data file paths
#   5. Reads all DELETE manifests → builds position-delete index
#      {norm(file_path) -> set(row_positions_to_drop)}
#   6. Reads each live Parquet data file via boto3 → PyArrow,
#      drops deleted row positions, concatenates → 5 correct rows
#   7. Writes result as Delta table via spark.createDataFrame()
#   8. Creates/replaces SQL view over the Delta table
#
# USAGE : Run Cell 1 only.  Everything is in one cell — no restart required.
#
# RESULT: workspace.lakehouse_db.vw_customer_test_latest  →  5 rows

# COMMAND ----------

# =============================================================================
# Cell 1 — Install + Refresh  (single cell, no restart needed)
# =============================================================================

# ── Install missing packages inline (idempotent, no kernel restart) ───────────
import subprocess, sys
subprocess.check_call([
    sys.executable, "-m", "pip", "install",
    "boto3>=1.26.0", "fastavro>=1.7.0", "pyarrow>=12.0.0",
    "--quiet", "--disable-pip-version-check"
])

# ── Imports ───────────────────────────────────────────────────────────────────
import os, json, io, datetime
import boto3, fastavro
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
S3_BUCKET  = "stardata-databricks"
S3_REGION  = "us-east-2"
S3_ENDPOINT = "https://s3.us-east-2.amazonaws.com"
TBL_PREFIX = "iceberg/warehouse/lakehouse_db/customer_test"  # no trailing /

UC_CATALOG = "workspace"
UC_SCHEMA  = "lakehouse_db"
UC_TABLE   = f"{UC_CATALOG}.{UC_SCHEMA}.snap_customer_test_latest"
UC_VIEW    = f"{UC_CATALOG}.{UC_SCHEMA}.vw_customer_test_latest"

# ── Credentials (boto3 only — Spark never touches S3) ─────────────────────────
AK = "<AWS_ACCESS_KEY_ID>"
SK = "<AWS_SECRET_ACCESS_KEY>"
os.environ["AWS_ACCESS_KEY_ID"]     = AK
os.environ["AWS_SECRET_ACCESS_KEY"] = SK
os.environ["AWS_DEFAULT_REGION"]    = S3_REGION

s3 = boto3.client("s3",
    aws_access_key_id=AK, aws_secret_access_key=SK,
    region_name=S3_REGION, endpoint_url=S3_ENDPOINT)

print("─" * 60)
print(f"  Table  : lakehouse_db.customer_test")
print(f"  Bucket : s3://{S3_BUCKET}/{TBL_PREFIX}")
print("─" * 60)

# ── Helpers ───────────────────────────────────────────────────────────────────
def _norm(p):
    return p.replace("s3a://", "s3://") if p else p

def _key(s3_path):
    p = _norm(s3_path)
    pfx = f"s3://{S3_BUCKET}/"
    if p.startswith(pfx):
        return p[len(pfx):]
    raise ValueError(f"Path not in bucket {S3_BUCKET}: {s3_path}")

def _get_avro(key):
    raw = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    return list(fastavro.reader(io.BytesIO(raw)))

def _get_parquet(key):
    raw = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    return pq.read_table(io.BytesIO(raw))

def _list_files(prefix, suffix=""):
    pager = s3.get_paginator("list_objects_v2")
    out = []
    for page in pager.paginate(Bucket=S3_BUCKET, Prefix=prefix.rstrip("/") + "/"):
        for obj in page.get("Contents", []):
            if not suffix or obj["Key"].endswith(suffix):
                out.append(obj)
    return out

# ── Step 1: Latest metadata.json ──────────────────────────────────────────────
meta_objs = sorted(
    _list_files(f"{TBL_PREFIX}/metadata", ".metadata.json"),
    key=lambda o: o["LastModified"], reverse=True
)
if not meta_objs:
    raise RuntimeError(f"No .metadata.json under s3://{S3_BUCKET}/{TBL_PREFIX}/metadata/")

latest    = meta_objs[0]
meta_name = latest["Key"].split("/")[-1]
meta_ts   = latest["LastModified"].strftime("%Y-%m-%d %H:%M:%S UTC")
meta      = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=latest["Key"])["Body"].read())
print(f"  [1/6] Metadata    : {meta_name}")
print(f"        Modified    : {meta_ts}")

# ── Step 2: Current snapshot ───────────────────────────────────────────────────
snap_id = meta.get("current-snapshot-id")
if snap_id is None:
    raise RuntimeError("No current snapshot — table is empty.")
current       = next(s for s in meta.get("snapshots", []) if s.get("snapshot-id") == snap_id)
total_records = int(current.get("summary", {}).get("total-records", 0))
operation     = current.get("summary", {}).get("operation", "?")
print(f"  [2/6] Snapshot ID : {snap_id}  op={operation}  total-records={total_records}")

# ── Step 3: Read manifest-list ────────────────────────────────────────────────
ml_records       = _get_avro(_key(current["manifest-list"]))
data_manifests   = [r for r in ml_records if r.get("content", 0) == 0]
delete_manifests = [r for r in ml_records if r.get("content", 0) == 1]
print(f"  [3/6] Manifests   : {len(data_manifests)} data  +  {len(delete_manifests)} delete")

# ── Step 4: Build position-delete index ───────────────────────────────────────
# position-delete file schema: (file_path STRING, pos LONG)
# index: norm(file_path) -> set of row positions to drop
pos_deletes = {}
for dm in delete_manifests:
    for rec in _get_avro(_key(dm["manifest_path"])):
        fp = rec.get("data_file", {}).get("file_path", "")
        if not fp:
            continue
        del_df = _get_parquet(_key(fp)).to_pandas()
        for _, row in del_df.iterrows():
            nk = _norm(str(row["file_path"]))
            pos_deletes.setdefault(nk, set()).add(int(row["pos"]))

total_deleted = sum(len(v) for v in pos_deletes.values())
print(f"  [4/6] Delete index: {total_deleted} position(s) across {len(pos_deletes)} file(s)")

# ── Step 5: Read data files, apply position deletes per-file ──────────────────
frames    = []
total_raw = 0
for dm in data_manifests:
    for rec in _get_avro(_key(dm["manifest_path"])):
        if rec.get("status", 1) == 0:       # 0 = DELETED manifest entry — skip
            continue
        fp = rec.get("data_file", {}).get("file_path", "")
        if not fp:
            continue
        df = _get_parquet(_key(fp)).to_pandas()
        total_raw += len(df)
        dead = pos_deletes.get(_norm(fp), set())
        if dead:
            df = df.drop(index=sorted(dead)).reset_index(drop=True)
        frames.append(df)

final = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
print(f"  [5/6] Raw rows    : {total_raw}  →  after position-delete: {len(final)}")

# ── Step 6: Write Delta table + view ──────────────────────────────────────────
spark.sql(f"USE CATALOG {UC_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")

spark.createDataFrame(final) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(UC_TABLE)

spark.sql(f"DROP VIEW IF EXISTS {UC_VIEW}")
spark.sql(f"CREATE VIEW {UC_VIEW} AS SELECT * FROM {UC_TABLE}")

print(f"  [6/6] Written     : {UC_TABLE}  ({len(final)} rows)")
print("─" * 60)
print(f"  ✅  DONE")
print(f"  Delta table : {UC_TABLE}")
print(f"  SQL view    : {UC_VIEW}")
print(f"  Rows        : {len(final)}")
print(f"  Metadata    : {meta_name}  ({meta_ts})")
print("─" * 60)
print(f"  Query: SELECT * FROM {UC_VIEW}")
print("─" * 60)

spark.sql(f"SELECT * FROM {UC_VIEW}").display()
