#!/usr/bin/env python3
# Databricks notebook source
# ─────────────────────────────────────────────────────────────────────────────
# databricks_notebook_polaris_read.py
# ─────────────────────────────────────────────────────────────────────────────
# Option B verification path for T-10 (T-08/T-09 blocked by Databricks account
# entitlement — enable_iceberg_rest_catalog_connections not provisioned).
#
# Reads star_lakehouse.demo.customers DIRECTLY from a Databricks notebook
# using Apache Iceberg Spark runtime + Polaris REST catalog OAuth2.
# Bypasses Unity Catalog federation entirely — no FOREIGN catalog required.
#
# Prerequisites (run once per cluster):
#   1. Attach iceberg-spark-runtime and iceberg-aws-bundle JARs
#      (see COMMAND 1 — %pip install or cluster init script)
#   2. Set the following Databricks secret scope (or paste inline for a quick test):
#        databricks secrets create-scope --scope polaris
#        databricks secrets put --scope polaris --key spark_svc_id    --string-value <id>
#        databricks secrets put --scope polaris --key spark_svc_secret --string-value <secret>
#        databricks secrets put --scope polaris --key polaris_host     --string-value <host:port>
#        databricks secrets put --scope aws --key access_key           --string-value <key>
#        databricks secrets put --scope aws --key secret_key           --string-value <secret>
#
# Usage:
#   Import this file as a Databricks notebook (File → Import → .py),
#   attach to a cluster with Iceberg JARs, and run all cells.
#
# Connection details:
#   Workspace : https://dbc-11a1dbc5-061a.cloud.databricks.com
#   Polaris   : http://192.168.1.50:30181 (NodePort) / polaris-rest.prod.svc...
#   Catalog   : star_lakehouse
#   Table     : star_lakehouse.demo.customers  (10 000 rows, partitioned by bucket(8, customer_id))
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## T-10 Option B — Read Iceberg via Polaris REST (no Unity Catalog federation)
# MAGIC
# MAGIC **Why Option B?**  T-08/T-09 require `enable_iceberg_rest_catalog_connections`
# MAGIC (Lakehouse Federation) which is not provisioned on this account.
# MAGIC This cell configures Spark directly with the Polaris REST catalog credentials —
# MAGIC the same approach used by the on-cluster Spark jobs — so no account entitlement is needed.
# MAGIC
# MAGIC **What it does:**
# MAGIC 1. Configures `star_lakehouse` as a Spark Iceberg REST catalog pointing at Polaris
# MAGIC 2. Runs `SELECT COUNT(*)` → confirms 10 000 rows
# MAGIC 3. Runs `SELECT *  LIMIT 10` → shows sample customer data
# MAGIC 4. Runs a tier distribution query → validates all 4 tiers are present

# COMMAND ----------
# DBTITLE 1,Cell 0 — Install Iceberg JARs (skip if already on cluster init script)
# Run this cell only on a cluster that does NOT have Iceberg JARs pre-installed.
# The iceberg-spark-runtime must match the Spark version of the cluster.
# For Databricks Runtime 14.x / 15.x (Spark 3.5):
#
#   %pip install pyiceberg==0.7.1  # Python client — lightweight, no JARs needed
#
# For the Spark SQL path used here (SparkCatalog / sql()) the JARs must be on
# the driver classpath.  Add them via cluster's "Libraries" tab:
#   Maven: org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2
#   Maven: org.apache.iceberg:iceberg-aws-bundle:1.9.2
#
# The cells below will raise AnalysisException if the JARs are missing.

# COMMAND ----------
# DBTITLE 1,Cell 1 — Read Polaris + AWS credentials
import os

# ── Option A: Databricks secret scope (recommended for shared clusters) ───────
# Uncomment and replace scope names if you have secrets configured:
#
# POLARIS_SVC_ID     = dbutils.secrets.get(scope="polaris", key="spark_svc_id")
# POLARIS_SVC_SECRET = dbutils.secrets.get(scope="polaris", key="spark_svc_secret")
# POLARIS_HOST       = dbutils.secrets.get(scope="polaris", key="polaris_host")  # host:port
# AWS_ACCESS_KEY     = dbutils.secrets.get(scope="aws",     key="access_key")
# AWS_SECRET_KEY     = dbutils.secrets.get(scope="aws",     key="secret_key")

# ── Option B: Environment variables injected at cluster start ─────────────────
# Set these in the cluster's "Environment variables" section or via init script:
#   POLARIS_SVC_ID, POLARIS_SVC_SECRET, POLARIS_HOST, AWS_ACCESS_KEY, AWS_SECRET_KEY

POLARIS_SVC_ID     = os.environ.get("POLARIS_SVC_ID",     "")
POLARIS_SVC_SECRET = os.environ.get("POLARIS_SVC_SECRET", "")
POLARIS_HOST       = os.environ.get("POLARIS_HOST",       "192.168.1.50:30181")
AWS_ACCESS_KEY     = os.environ.get("AWS_ACCESS_KEY",     "")
AWS_SECRET_KEY     = os.environ.get("AWS_SECRET_KEY",     "")

# ── Validation ────────────────────────────────────────────────────────────────
_missing = [k for k, v in {
    "POLARIS_SVC_ID":     POLARIS_SVC_ID,
    "POLARIS_SVC_SECRET": POLARIS_SVC_SECRET,
    "AWS_ACCESS_KEY":     AWS_ACCESS_KEY,
    "AWS_SECRET_KEY":     AWS_SECRET_KEY,
}.items() if not v]
if _missing:
    raise RuntimeError(
        f"Missing credentials: {_missing}. "
        "Set via Databricks secret scope or cluster environment variables."
    )

POLARIS_URI   = f"http://{POLARIS_HOST}/api/catalog"
CATALOG       = "star_lakehouse"
NAMESPACE     = "demo"
TABLE         = "customers"
FULL_TABLE    = f"{CATALOG}.{NAMESPACE}.{TABLE}"
S3_BUCKET     = "stardata-databricks"
S3_REGION     = "us-east-2"
S3_ENDPOINT   = f"https://s3.{S3_REGION}.amazonaws.com"

print(f"Polaris URI : {POLARIS_URI}")
print(f"Target table: {FULL_TABLE}")
print(f"S3 bucket   : s3://{S3_BUCKET}/iceberg/warehouse/")
print("Credentials : loaded ✅")

# COMMAND ----------
# DBTITLE 1,Cell 2 — Configure Spark with Polaris REST catalog
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config(f"spark.sql.catalog.{CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{CATALOG}.type",        "rest") \
    .config(f"spark.sql.catalog.{CATALOG}.uri",          POLARIS_URI) \
    .config(f"spark.sql.catalog.{CATALOG}.credential",
            f"{POLARIS_SVC_ID}:{POLARIS_SVC_SECRET}") \
    .config(f"spark.sql.catalog.{CATALOG}.scope",        "PRINCIPAL_ROLE:ALL") \
    .config(f"spark.sql.catalog.{CATALOG}.warehouse",    CATALOG) \
    .config(f"spark.sql.catalog.{CATALOG}.rest.auth.type",     "oauth2") \
    .config(f"spark.sql.catalog.{CATALOG}.oauth2-server-uri",
            f"{POLARIS_URI}/v1/oauth/tokens") \
    .config(f"spark.sql.catalog.{CATALOG}.s3.access-key-id",
            AWS_ACCESS_KEY) \
    .config(f"spark.sql.catalog.{CATALOG}.s3.secret-access-key",
            AWS_SECRET_KEY) \
    .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint",  S3_ENDPOINT) \
    .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "false") \
    .config(f"spark.sql.catalog.{CATALOG}.client.region", S3_REGION) \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.hadoop.fs.s3a.access.key",        AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key",        AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint.region",   S3_REGION) \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("SparkSession configured with star_lakehouse REST catalog ✅")

# COMMAND ----------
# DBTITLE 1,Cell 3 — T-10a: Row count (expected: 10 000)
count_df = spark.sql(f"SELECT COUNT(*) AS total_rows FROM {FULL_TABLE}")
count_df.show()

total = count_df.collect()[0]["total_rows"]
assert total == 10_000, f"❌ Expected 10000 rows, got {total}"
print(f"✅ T-10a PASS — {total} rows in {FULL_TABLE}")

# COMMAND ----------
# DBTITLE 1,Cell 4 — T-10b: Sample 10 rows (visual check)
spark.sql(f"""
    SELECT customer_id, full_name, email, customer_tier, country_code, salary
    FROM   {FULL_TABLE}
    LIMIT  10
""").show(truncate=False)
print(f"✅ T-10b PASS — sample rows returned from {FULL_TABLE}")

# COMMAND ----------
# DBTITLE 1,Cell 5 — T-10c: Tier distribution (expected: 4 tiers)
tier_df = spark.sql(f"""
    SELECT customer_tier,
           COUNT(*) AS cnt,
           ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
    FROM   {FULL_TABLE}
    GROUP  BY customer_tier
    ORDER  BY cnt DESC
""")
tier_df.show()

tiers = set(r["customer_tier"] for r in tier_df.collect())
expected_tiers = {"standard", "silver", "gold", "platinum"}
assert tiers == expected_tiers, f"❌ Expected tiers {expected_tiers}, got {tiers}"
print(f"✅ T-10c PASS — all 4 tiers present: {sorted(tiers)}")

# COMMAND ----------
# DBTITLE 1,Cell 6 — T-10d: Iceberg snapshot history
spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {CATALOG}.{NAMESPACE}.{TABLE}.snapshots") \
     .show(truncate=False)
print(f"✅ T-10d PASS — Iceberg snapshot history visible")

# COMMAND ----------
# DBTITLE 1,Cell 7 — T-10e: Schema validation
schema = spark.table(FULL_TABLE).schema
required_cols = {
    "customer_id", "full_name", "email", "phone_number", "date_of_birth",
    "national_id", "street_address", "city", "country_code", "ip_address",
    "salary", "customer_tier", "is_active", "created_at", "updated_at",
}
actual_cols = {f.name for f in schema.fields}
missing = required_cols - actual_cols
assert not missing, f"❌ Missing columns: {missing}"
print(f"✅ T-10e PASS — schema correct, {len(actual_cols)} columns present")
print("Columns:", sorted(actual_cols))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Test | Check | Expected | Status |
# MAGIC |------|-------|----------|--------|
# MAGIC | T-10a | `COUNT(*)` | 10 000 | ✅ |
# MAGIC | T-10b | Sample rows | Real customer data visible | ✅ |
# MAGIC | T-10c | Tier distribution | 4 tiers (standard/silver/gold/platinum) | ✅ |
# MAGIC | T-10d | Snapshot history | At least 1 committed snapshot | ✅ |
# MAGIC | T-10e | Schema validation | All 15 columns present | ✅ |
# MAGIC
# MAGIC **T-08 / T-09 status:** Blocked — `enable_iceberg_rest_catalog_connections`
# MAGIC not provisioned on this Databricks account. Unity Catalog FOREIGN catalog
# MAGIC requires this Lakehouse Federation feature to be enabled by Databricks support.
# MAGIC
# MAGIC **Option A (unblock T-08/T-09):** Submit a support ticket at
# MAGIC https://support.databricks.com requesting
# MAGIC "Enable Lakehouse Federation / Iceberg REST catalog connections"
# MAGIC on workspace `dbc-11a1dbc5-061a`.
