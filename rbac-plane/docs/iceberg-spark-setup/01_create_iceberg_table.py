"""
01_create_iceberg_table.py
==========================
Creates an Iceberg table on Amazon S3 using Apache Spark + the Apache Polaris
(Iceberg REST catalog).  Run this script once to bootstrap the table DDL.

Requirements
------------
  pip install pyspark==3.5.* pyiceberg boto3

Environment variables expected
-------------------------------
  POLARIS_URL          https://<polaris-host>/api/catalog
  POLARIS_CREDENTIAL   <client_id>:<client_secret>   (Polaris OAuth2 machine credential)
  S3_BUCKET            xdatatoiceberg1
  AWS_ACCESS_KEY_ID    <read from OpenBao: secret/platform/s3 → access_key>
  AWS_SECRET_ACCESS_KEY <read from OpenBao: secret/platform/s3 → secret_key>
  AWS_REGION           us-east-2
  RBAC_TOKEN           <rbac-plane api token>
  RBAC_URL             http://rbac-plane.prod.svc.cluster.local:8080

RBAC enforcement
----------------
The script checks the caller's effective permissions via the RBAC Control Plane
before attempting any Spark / catalog operations.  The caller must hold the
'iceberg_engineer' role (or equivalent USE_CATALOG + WRITE_ICEBERG permissions
on the spark service and CATALOG_READ + TABLE_WRITE on polaris).
"""
from __future__ import annotations

import os
import sys

import httpx
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── RBAC gate ─────────────────────────────────────────────────────────────────

def _rbac_check(username: str) -> None:
    """Abort if the calling user lacks the required permissions."""
    base  = os.environ.get("RBAC_URL", "http://localhost:8080")
    token = os.environ["RBAC_TOKEN"]
    resp  = httpx.get(
        f"{base}/api/v1/users/{username}/roles",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    if resp.status_code != 200:
        print(f"[RBAC] Could not verify permissions for {username}: {resp.text}", file=sys.stderr)
        sys.exit(1)

    data    = resp.json()
    perms   = {f"{p['service']}:{p['permission']}" for p in data.get("permissions", [])}
    required = {
        "spark:USE_CATALOG",
        "spark:WRITE_ICEBERG",
        "spark:SUBMIT_JOB",
        "polaris:TABLE_WRITE",
        "polaris:CATALOG_READ",
    }
    missing = required - perms
    if missing:
        print(f"[RBAC] DENIED — missing permissions: {missing}", file=sys.stderr)
        sys.exit(1)
    print(f"[RBAC] OK — {username} has required permissions")


# ── Configuration ─────────────────────────────────────────────────────────────

S3_BUCKET          = os.environ.get("S3_BUCKET",  "xdatatoiceberg1")
S3_REGION          = os.environ.get("AWS_REGION", "us-east-2")
POLARIS_URL        = os.environ.get("POLARIS_URL", "http://polaris:8181/api/catalog")
POLARIS_CREDENTIAL = os.environ.get("POLARIS_CREDENTIAL", "spark-client:changeme")

CATALOG_NAME   = "polaris"
NAMESPACE      = "lakehouse"
TABLE_NAME     = "events"
TABLE_FULL     = f"{NAMESPACE}.{TABLE_NAME}"
WAREHOUSE_PATH = f"s3a://{S3_BUCKET}/warehouse"

# Target file size: 2.56 MB expressed in bytes for Iceberg write option
TARGET_FILE_SIZE_BYTES = int(2.56 * 1024 * 1024)   # 2,684,354 bytes


# ── Spark session ─────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    aws_key    = os.environ["AWS_ACCESS_KEY_ID"]
    aws_secret = os.environ["AWS_SECRET_ACCESS_KEY"]

    # Polaris OAuth2 credential: "client_id:client_secret"
    polaris_credential = POLARIS_CREDENTIAL

    spark = (
        SparkSession.builder
        .appName("IcebergTableSetup")

        # ── Iceberg + Polaris REST catalog ────────────────────
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.polaris",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.polaris.type",       "rest")
        .config("spark.sql.catalog.polaris.uri",        POLARIS_URL)
        .config("spark.sql.catalog.polaris.credential", polaris_credential)
        .config("spark.sql.catalog.polaris.warehouse",  WAREHOUSE_PATH)
        .config("spark.sql.catalog.polaris.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.polaris.s3.region",  S3_REGION)

        # ── AWS S3A credentials (HMAC) ────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint",
                f"https://s3.{S3_REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.access.key",  aws_key)
        .config("spark.hadoop.fs.s3a.secret.key",  aws_secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # ── Iceberg file-size target ───────────────────────────
        # Controls the target file size during writes/compaction
        .config("spark.sql.catalog.polaris.write.target-file-size-bytes",
                str(TARGET_FILE_SIZE_BYTES))

        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── DDL ───────────────────────────────────────────────────────────────────────

def create_table(spark: SparkSession) -> None:
    """
    Schema
    ------
    event_id    BIGINT       — synthetic surrogate key
    source      STRING       — event source system
    event_type  STRING       — e.g. "click", "purchase"
    user_id     BIGINT
    amount      DOUBLE
    ts          TIMESTAMP    — event timestamp (partition driving column)
    payload     STRING       — JSON blob

    Partitioning
    ------------
    • Level 1 : HOURS(ts)    — one partition per clock hour
    • Level 2 : BUCKET(4, event_id)  — 4 hash buckets inside each hour

    Each write produces Parquet files capped at ~2.56 MB via the
    write.target-file-size-bytes property set on the catalog and overridden
    here in the table properties for clarity.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{NAMESPACE}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{TABLE_FULL} (
            event_id   BIGINT,
            source     STRING,
            event_type STRING,
            user_id    BIGINT,
            amount     DOUBLE,
            ts         TIMESTAMP,
            payload    STRING
        )
        USING iceberg
        PARTITIONED BY (hours(ts), bucket(4, event_id))
        TBLPROPERTIES (
            'write.format.default'               = 'parquet',
            'write.parquet.compression-codec'    = 'snappy',
            'write.target-file-size-bytes'       = '{TARGET_FILE_SIZE_BYTES}',
            'write.distribution-mode'            = 'hash',
            -- Polaris catalog integration
            'write.metadata.metrics.default'     = 'truncate(16)',
            'commit.retry.num-retries'           = '4',
            'history.expire.max-snapshot-age-ms' = '604800000'
        )
        LOCATION '{WAREHOUSE_PATH}/{NAMESPACE}/{TABLE_NAME}'
    """)
    print(f"Table {CATALOG_NAME}.{TABLE_FULL} created / already exists.")


def show_partitions(spark: SparkSession) -> None:
    df = spark.sql(f"DESCRIBE EXTENDED {CATALOG_NAME}.{TABLE_FULL}")
    df.show(truncate=False)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    caller = os.environ.get("SPARK_SUBMITTER", os.environ.get("USER", "unknown"))
    _rbac_check(caller)

    spark = build_spark()
    try:
        create_table(spark)
        show_partitions(spark)
    finally:
        spark.stop()
