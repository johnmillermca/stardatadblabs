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
# S3 access: IAM role via external location `stardata_databricks_iceberg`
#   (s3://stardata-databricks/) — no secrets required in the notebook.
#
# Run order: Cell 1 → Cell 2 → Cell 3 → Cell 4
# =============================================================================

# COMMAND ----------
# MAGIC %md ## Cell 1 — Resolve latest Iceberg metadata JSON (always current)

# COMMAND ----------

import boto3, json

# S3 access uses the Unity Catalog external location credential (IAM role).
# boto3 is used only to list metadata files — Spark reads via the IAM role directly.
AWS_KEY    = dbutils.secrets.get(scope="stardata_platform", key="aws_access_key")
AWS_SECRET = dbutils.secrets.get(scope="stardata_platform", key="aws_secret_key")

BUCKET = "stardata-databricks"
META_PREFIX = "iceberg/warehouse/lakehouse_db/customer/metadata/"

s3 = boto3.client("s3", region_name="us-east-2",
    aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)

# List all *.metadata.json files, sort by LastModified — pick the newest
pag = s3.get_paginator("list_objects_v2")
objs = []
for page in pag.paginate(Bucket=BUCKET, Prefix=META_PREFIX):
    for o in page.get("Contents", []):
        if o["Key"].endswith(".metadata.json"):
            objs.append(o)

if not objs:
    raise FileNotFoundError(
        f"No .metadata.json files found under s3://{BUCKET}/{META_PREFIX}\n"
        "Run databricks_customer_seed.py on the Spark cluster first."
    )

objs.sort(key=lambda o: o["LastModified"], reverse=True)
latest_key = objs[0]["Key"]
print(f"✅ Latest metadata ({len(objs)} snapshots): s3://{BUCKET}/{latest_key}")
print(f"   LastModified: {objs[0]['LastModified']}")

# Extract table location from the metadata JSON
meta      = json.loads(s3.get_object(Bucket=BUCKET, Key=latest_key)["Body"].read())
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
