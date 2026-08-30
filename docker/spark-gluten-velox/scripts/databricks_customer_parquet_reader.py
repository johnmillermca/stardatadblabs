# Databricks notebook source
# =============================================================================
# databricks_customer_parquet_reader.py
# =============================================================================
# Reads the Iceberg customer table written by Spark Gluten from
# s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/
#
# (d) Always resolves the LATEST metadata JSON automatically — no hard-coded
#     snapshot path.  Re-run Cell 1 + Cell 3 after any Spark write to refresh.
#
# (e) Creates/replaces a persistent Unity Catalog view:
#     lakehouse.lakehouse_db.vw_customer_latest
#
# S3 access: IAM role via Unity Catalog external location `stardata_databricks_iceberg`
#   (covers s3://stardata-databricks/) — no secret scope needed.
#   dbutils.fs.ls() and spark.read.text() both use the external location credential.
#
# Run order: Cell 1 → Cell 2 → Cell 3 → Cell 4
# =============================================================================

# COMMAND ----------
# MAGIC %md ## Cell 1 — Resolve latest Iceberg metadata JSON (always current)

# COMMAND ----------

import json

# ── Resolve latest metadata JSON via dbutils.fs (uses external location IAM) ──
# No secret scope is required — the Unity Catalog external location
# `stardata_databricks_iceberg` (s3://stardata-databricks/) provides
# access transparently through the bound IAM role.

META_PATH = "s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/metadata/"

# List all files under the metadata prefix; filter *.metadata.json
all_files = dbutils.fs.ls(META_PATH)
meta_files = [f for f in all_files if f.name.endswith(".metadata.json")]

if not meta_files:
    raise FileNotFoundError(
        f"No .metadata.json files found under {META_PATH}\n"
        "Run databricks_customer_seed.py on the Spark cluster first."
    )

# Sort by modification time descending — pick the newest
meta_files.sort(key=lambda f: f.modificationTime, reverse=True)
latest = meta_files[0]
print(f"✅ Latest metadata ({len(meta_files)} snapshots): {latest.path}")
print(f"   Modified (ms epoch): {latest.modificationTime}")

# Read the metadata JSON content via Spark (IAM external location)
meta_json = (
    spark.read.text(latest.path, wholetext=True)
    .collect()[0][0]
)
meta = json.loads(meta_json)

DATA_PATH = meta["location"].rstrip("/") + "/data/"
print(f"✅ Data path: {DATA_PATH}")

# COMMAND ----------
# MAGIC %md ## Cell 2 — Read Parquet files using Spark (latest snapshot)

# COMMAND ----------

# Spark reads via the Unity Catalog external location (stardata_databricks_iceberg).
# mergeSchema=true handles partition columns (snap_timestamp_hour, snap_id_bucket).
df_customer = (
    spark.read
    .option("mergeSchema", "true")
    .parquet(DATA_PATH)
)

print(f"✅ Loaded {df_customer.count():,} rows")
df_customer.printSchema()
df_customer.show(10, truncate=False)

# COMMAND ----------
# MAGIC %md ## Cell 3 — (e) Create/replace persistent view `vw_customer_latest`
# MAGIC
# MAGIC Re-run this cell after every Spark write to point the view at the latest snapshot.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS lakehouse.lakehouse_db")

spark.sql(f"""
  CREATE OR REPLACE VIEW lakehouse.lakehouse_db.vw_customer_latest
  COMMENT 'Latest Iceberg snapshot of customer table from s3://stardata-databricks.
Re-run Cell 1 + Cell 3 after each Spark write to refresh to the newest snapshot.'
  AS
  SELECT
      customer_id,
      full_name,
      email,
      phone_number,
      date_of_birth,
      national_id,
      street_address,
      city,
      country_code,
      ip_address,
      salary,
      customer_tier,
      is_active,
      created_at,
      updated_at,
      snap_id,
      snap_timestamp
  FROM read_files(
      '{DATA_PATH}',
      format      => 'parquet',
      mergeSchema => true
  )
""")

print(f"✅ View created/replaced: lakehouse.lakehouse_db.vw_customer_latest")
print(f"   Backed by: {DATA_PATH}")

# COMMAND ----------
# MAGIC %md ## Cell 4 — Verify view

# COMMAND ----------

spark.sql("SELECT COUNT(*) AS total_rows FROM lakehouse.lakehouse_db.vw_customer_latest").show()

spark.sql("""
    SELECT customer_id, full_name, city, customer_tier, salary
    FROM   lakehouse.lakehouse_db.vw_customer_latest
    ORDER  BY customer_id
    LIMIT  10
""").show(truncate=False)

spark.sql("""
    SELECT customer_tier, COUNT(*) AS cnt, ROUND(AVG(salary), 2) AS avg_salary
    FROM   lakehouse.lakehouse_db.vw_customer_latest
    GROUP  BY customer_tier
    ORDER  BY cnt DESC
""").show()
