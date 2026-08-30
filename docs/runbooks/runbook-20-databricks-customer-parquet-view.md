# Runbook 20 — Databricks: Read customer Parquet Files + View

| Field | Value |
|---|---|
| **Runbook ID** | RB-20 |
| **Service** | k8s-platform / databricks |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-08-30 |

---

## 1. Overview

This runbook covers the full end-to-end flow:

1. **Spark Gluten** (k8s) writes the `customer` Iceberg table to S3
2. **Databricks** reads the Parquet files directly using `read_files()`, always resolving the latest Iceberg snapshot automatically
3. A **persistent view** `lakehouse.lakehouse_db.vw_customer_latest` is created on top of those Parquet files

No HMS, no FOREIGN catalog, no external connection needed. Databricks reads S3 directly via the existing Unity Catalog external location.

```
Spark Gluten (k8s)                     AWS S3
──────────────────────                 ─────────────────────────────────────
databricks_customer_seed.py    ──────► stardata-databricks/
  catalog : databricks                   iceberg/warehouse/lakehouse_db/
  ns      : lakehouse_db                   customer/
  table   : customer                         metadata/  ← *.metadata.json
  rows    : 1 000                             data/      ← *.parquet (4 files)

                                       ▲
                         IAM role: databricks-unity-catalog
                         External location: stardata_databricks_iceberg

Databricks (read path)
──────────────────────────────────────────────────────────────────
1. List metadata/*.metadata.json  →  pick newest by LastModified
2. Read metadata JSON             →  extract table "location" field
3. spark.read.parquet(location/data/)
4. CREATE OR REPLACE VIEW lakehouse.lakehouse_db.vw_customer_latest
```

---

## 2. Pre-flight

```bash
# Get tokens
VAULT_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

DB_TOKEN=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=token secret/databricks/pat")

DB_WS="https://dbc-11a1dbc5-061a.cloud.databricks.com"
WAREHOUSE_ID="942026cf5e55f3c3"

# Verify S3 parquet files exist
AWS_KEY=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=access_key secret/platform/s3")
AWS_SECRET=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=secret_key secret/platform/s3")

python3 - "$AWS_KEY" "$AWS_SECRET" <<'EOF'
import sys, boto3
s3 = boto3.client('s3', region_name='us-east-2',
    aws_access_key_id=sys.argv[1], aws_secret_access_key=sys.argv[2])
pag = s3.get_paginator('list_objects_v2')
meta, data = [], []
for page in pag.paginate(Bucket='stardata-databricks',
                          Prefix='iceberg/warehouse/lakehouse_db/customer/'):
    for o in page.get('Contents', []):
        (meta if o['Key'].endswith('.metadata.json') else
         data if o['Key'].endswith('.parquet') else []).append(o['Key'])
print(f"✅  metadata files : {len(meta)}")
print(f"✅  parquet files  : {len(data)}")
meta.sort(); print(f"   latest metadata: s3://stardata-databricks/{meta[-1]}")
EOF
```

✅ Expected: `metadata files: 2`, `parquet files: 4`

---

## 3. Step 1 — Write 1 000 rows from Spark (if not already done)

Skip if rows already exist. Run from the Spark master pod:

```bash
SPARK_POD=$(kubectl get pods -n prod -l app=spark,component=master \
  --no-headers -o custom-columns=NAME:.metadata.name | head -1)
VAULT_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env TOKEN=$VAULT_TOKEN USER=dave SPARK_USER=dave \
      PYTHONPATH=/opt/spark/work-dir PYSPARK_PYTHON=python3.11 \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master-internal.prod.svc.cluster.local:17077 \
    --name databricks-customer-seed \
    --conf spark.pyspark.python=python3.11 \
    --conf spark.pyspark.driver.python=python3.11 \
    /opt/spark/work-dir/databricks_customer_seed.py
```

✅ Expected output:
```
customer-seed – Inserted 1000 rows into `databricks`.`lakehouse_db`.`customer`
+-----+
|total|
+-----+
| 1000|
+-----+
```

**Script location:** [`docker/spark-gluten-velox/scripts/databricks_customer_seed.py`](../../docker/spark-gluten-velox/scripts/databricks_customer_seed.py)

---

## 4. Step 2 — Read Parquet files in Databricks SQL

Open **`https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor`**, select **Serverless Starter Warehouse**, and run:

```sql
-- (d) Read directly from latest Parquet snapshot
-- Replace the path with the latest data/ path from Step 3 if refreshing

SELECT COUNT(*) AS total_rows
FROM read_files(
    's3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/data/',
    format      => 'parquet',
    mergeSchema => true
);
```

✅ Expected: `total_rows = 1000`

```sql
-- Sample rows
SELECT customer_id, full_name, city, customer_tier, salary
FROM read_files(
    's3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/data/',
    format      => 'parquet',
    mergeSchema => true
)
ORDER BY customer_id
LIMIT 10;
```

> **Note:** `read_files()` works because `s3://stardata-databricks/` is covered by the
> Unity Catalog external location `stardata_databricks_iceberg` with credential `stardata_databricks_s3`
> (IAM role `arn:aws:iam::586643076710:role/databricks-unity-catalog`).
> No secrets or access keys are needed in SQL.

---

## 5. Step 3 — Resolve the latest metadata JSON automatically (CLI / API)

After each new Spark write the `data/` path stays the same but a new `.metadata.json` is created.
Use this script to always get the correct latest data path:

```bash
AWS_KEY=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=access_key secret/platform/s3")
AWS_SECRET=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=secret_key secret/platform/s3")

DATA_PATH=$(python3 - "$AWS_KEY" "$AWS_SECRET" <<'EOF'
import sys, boto3, json
s3 = boto3.client('s3', region_name='us-east-2',
    aws_access_key_id=sys.argv[1], aws_secret_access_key=sys.argv[2])
pag = s3.get_paginator('list_objects_v2')
objs = []
for page in pag.paginate(Bucket='stardata-databricks',
                          Prefix='iceberg/warehouse/lakehouse_db/customer/metadata/'):
    for o in page.get('Contents', []):
        if o['Key'].endswith('.metadata.json'):
            objs.append(o)
objs.sort(key=lambda x: x['LastModified'], reverse=True)
meta = json.loads(s3.get_object(
    Bucket='stardata-databricks', Key=objs[0]['Key'])['Body'].read())
print(meta['location'].rstrip('/') + '/data/')
EOF
)
echo "Latest data path: $DATA_PATH"
```

---

## 6. Step 4 — Create / refresh the view via Databricks SQL API

Run this after each Spark write to re-point the view at the latest snapshot:

```bash
# DATA_PATH resolved from Step 3 above

curl -s -X POST "$DB_WS/api/2.0/sql/statements" \
  -H "Authorization: Bearer $DB_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"warehouse_id\": \"$WAREHOUSE_ID\",
    \"wait_timeout\": \"50s\",
    \"statement\": \"CREATE OR REPLACE VIEW lakehouse.lakehouse_db.vw_customer_latest
      COMMENT 'Latest Iceberg snapshot — refresh after each Spark write'
      AS SELECT customer_id, full_name, email, phone_number, date_of_birth,
                national_id, street_address, city, country_code, ip_address,
                salary, customer_tier, is_active, created_at, updated_at,
                snap_id, snap_timestamp
         FROM read_files('${DATA_PATH}', format => 'parquet', mergeSchema => true)\"
  }" | python3 -c "
import sys,json
d=json.load(sys.stdin)
state=d.get('status',{}).get('state')
err=d.get('status',{}).get('error',{})
print('state:', state)
if err: print('error:', err.get('message'))
else: print('✅ View created: lakehouse.lakehouse_db.vw_customer_latest')
"
```

---

## 7. Test matrix

Run these SQL statements in the Databricks SQL editor to verify the full setup:

```sql
-- T-01: Schema exists
SHOW SCHEMAS IN lakehouse;
-- ✅ lakehouse_db listed

-- T-02: View exists
SHOW VIEWS IN lakehouse.lakehouse_db;
-- ✅ vw_customer_latest listed

-- T-03: Row count correct
SELECT COUNT(*) AS total_rows
FROM lakehouse.lakehouse_db.vw_customer_latest;
-- ✅ 1000

-- T-04: Business columns present
SELECT customer_id, full_name, email, city, customer_tier, salary
FROM lakehouse.lakehouse_db.vw_customer_latest
ORDER BY customer_id
LIMIT 5;
-- ✅ rows 1–5 with correct data

-- T-05: Snap audit columns present
SELECT snap_id, snap_timestamp
FROM lakehouse.lakehouse_db.vw_customer_latest
LIMIT 3;
-- ✅ snap_id: bigint, snap_timestamp: timestamp

-- T-06: Tier distribution sums to 1000
SELECT customer_tier, COUNT(*) AS cnt
FROM lakehouse.lakehouse_db.vw_customer_latest
GROUP BY customer_tier
ORDER BY customer_tier;
-- ✅ silver+platinum+gold+standard = 1000

-- T-07: Direct read_files() also works (no view)
SELECT COUNT(*) FROM read_files(
    's3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/data/',
    format => 'parquet', mergeSchema => true);
-- ✅ 1000
```

| ID | Test | Expected | SQL |
|---|---|---|---|
| T-01 | Schema exists | `lakehouse_db` in list | `SHOW SCHEMAS IN lakehouse` |
| T-02 | View exists | `vw_customer_latest` in list | `SHOW VIEWS IN lakehouse.lakehouse_db` |
| T-03 | Row count | `1000` | `SELECT COUNT(*) FROM lakehouse.lakehouse_db.vw_customer_latest` |
| T-04 | Business columns | 5 rows with correct data | `SELECT ... LIMIT 5` |
| T-05 | Snap audit columns | `snap_id` bigint, `snap_timestamp` timestamp | `SELECT snap_id, snap_timestamp LIMIT 3` |
| T-06 | Tier distribution | 4 tiers summing to 1000 | `GROUP BY customer_tier` |
| T-07 | Direct `read_files()` | `1000` | `SELECT COUNT(*) FROM read_files(...)` |

---

## 8. Connect from JupyterHub notebook and read the customer table

Open **`http://192.168.1.50:30888`**, log in, create a new notebook, and run the cells below in order.

> **Pre-condition:** The seed script (Step 1) must have already run.
> The Spark cluster must be up: `kubectl get pods -n prod -l app=spark`.

---

### Cell 1 — Fetch credentials from OpenBao

Get the root token from your terminal first:

```bash
kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d && echo
```

Paste it into the cell below:

```python
import urllib.request, json, os

OPENBAO_ADDR  = "http://openbao.prod.svc.cluster.local:8200"
OPENBAO_TOKEN = "s.xxxxxxxxxxxxxxxxxxxxxxxx"   # ← paste token here

def bao(path, field):
    req = urllib.request.Request(
        f"{OPENBAO_ADDR}/v1/{path}",
        headers={"X-Vault-Token": OPENBAO_TOKEN}
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())["data"]["data"][field]

S3_KEY    = bao("secret/data/platform/s3", "access_key")
S3_SECRET = bao("secret/data/platform/s3", "secret_key")
S3_ENDPOINT = bao("secret/data/platform/s3", "endpoint")
print("✅ Credentials loaded")
```

---

### Cell 2 — Resolve the latest metadata JSON automatically

```python
import boto3, json as _json

BUCKET      = "stardata-databricks"
META_PREFIX = "iceberg/warehouse/lakehouse_db/customer/metadata/"

s3 = boto3.client("s3", region_name="us-east-2",
    aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET)

pag  = s3.get_paginator("list_objects_v2")
objs = []
for page in pag.paginate(Bucket=BUCKET, Prefix=META_PREFIX):
    for o in page.get("Contents", []):
        if o["Key"].endswith(".metadata.json"):
            objs.append(o)

objs.sort(key=lambda o: o["LastModified"], reverse=True)
latest_key = objs[0]["Key"]
print(f"✅ Latest metadata ({len(objs)} snapshots): s3://{BUCKET}/{latest_key}")
print(f"   LastModified: {objs[0]['LastModified']}")

meta      = _json.loads(s3.get_object(Bucket=BUCKET, Key=latest_key)["Body"].read())
DATA_PATH = meta["location"].rstrip("/") + "/data/"
print(f"✅ Data path: {DATA_PATH}")
```

✅ Expected output:
```
✅ Latest metadata (2 snapshots): s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/metadata/00001-....metadata.json
✅ Data path: s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/data/
```

---

### Cell 3 — Build the Spark session

```python
from pyspark.sql import SparkSession

_s = SparkSession.getActiveSession()
if _s:
    _s.stop()

DRIVER_IP = os.environ["SPARK_LOCAL_IP"]

spark = SparkSession.builder \
    .master("spark://spark-master-internal.prod.svc.cluster.local:17077") \
    .appName("jupyter-customer-reader") \
    .config("spark.driver.host",        DRIVER_IP) \
    .config("spark.driver.bindAddress", DRIVER_IP) \
    .config("spark.executor.memory",    "2g") \
    .config("spark.driver.memory",      "2g") \
    .config("spark.pyspark.python",        "python3.11") \
    .config("spark.pyspark.driver.python", "python3.11") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key",        S3_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key",        S3_SECRET) \
    .config("spark.hadoop.fs.s3a.endpoint",          S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.plugins",                         "org.apache.gluten.GlutenPlugin") \
    .config("spark.gluten.sql.columnar.backend.lib", "velox") \
    .config("spark.memory.offHeap.enabled",          "true") \
    .config("spark.memory.offHeap.size",             "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark", spark.version, "connected —", DRIVER_IP)
```

✅ Expected: `✅ Spark 3.5.1 connected — 10.244.x.x`

---

### Cell 4 — Read the customer Parquet files

```python
df_customer = (
    spark.read
    .option("mergeSchema", "true")
    .parquet(DATA_PATH)
)

print(f"✅ Loaded {df_customer.count():,} rows from {DATA_PATH}")
df_customer.printSchema()
df_customer.show(10, truncate=False)
```

✅ Expected: `✅ Loaded 1,000 rows`

---

### Cell 5 — Query the customer table using Spark SQL

```python
# Register as a temporary view for SQL queries in this session
df_customer.createOrReplaceTempView("customer_latest")

# Row count
spark.sql("SELECT COUNT(*) AS total_rows FROM customer_latest").show()

# Sample rows
spark.sql("""
    SELECT customer_id, full_name, city, customer_tier, salary
    FROM   customer_latest
    ORDER  BY customer_id
    LIMIT  10
""").show(truncate=False)

# Tier distribution
spark.sql("""
    SELECT customer_tier, COUNT(*) AS cnt, ROUND(AVG(salary), 2) AS avg_salary
    FROM   customer_latest
    GROUP  BY customer_tier
    ORDER  BY cnt DESC
""").show()
```

✅ Expected output:
```
+-----------+
|total_rows |
+-----------+
|       1000|
+-----------+

+-----------+-----------------+-----------+-------------+---------+
|customer_id|full_name        |city       |customer_tier|salary   |
+-----------+-----------------+-----------+-------------+---------+
|1          |Wei Brown        |Toronto    |standard     |53721.45 |
|2          |Karen Smith      |Mexico City|platinum     |149225.25|
...

+-------------+---+----------+
|customer_tier|cnt|avg_salary|
+-------------+---+----------+
|silver       |270|115908.02 |
|platinum     |249|116659.31 |
|gold         |248|115645.87 |
|standard     |233|115137.04 |
+-------------+---+----------+
```

---

### Cell 6 — Always call spark.stop() when done

```python
# IMPORTANT: always stop the session when finished.
# Leaving it open holds all cluster cores and blocks other jobs.
spark.stop()
print("✅ Spark session stopped — cluster cores released")
```

> ⚠️ **If you close the browser without running this cell**, the cleanup CronJob
> (`spark-app-cleanup`) will automatically kill the idle session after **5 minutes**
> of inactivity. See [`manifests/spark-app-cleanup-cronjob.yaml`](../../manifests/spark-app-cleanup-cronjob.yaml).

---

## 9. Check the view from the Databricks console

Open **`https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor`**
Select warehouse: **Serverless Starter Warehouse**

> The warehouse cold-starts automatically. First query takes 30–90 seconds.

---

### Check 1 — Confirm the view exists

```sql
SHOW VIEWS IN lakehouse.lakehouse_db;
```

✅ Expected: `vw_customer_latest` listed under `lakehouse.lakehouse_db`

---

### Check 2 — Row count

```sql
SELECT COUNT(*) AS total_rows
FROM lakehouse.lakehouse_db.vw_customer_latest;
```

✅ Expected: `1000`

---

### Check 3 — Sample rows

```sql
SELECT customer_id, full_name, email, city, customer_tier, salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
ORDER  BY customer_id
LIMIT  10;
```

✅ Expected: rows 1–10 with `customer_id`, names, cities, tiers and salaries

---

### Check 4 — Tier distribution

```sql
SELECT customer_tier,
       COUNT(*)              AS cnt,
       ROUND(AVG(salary), 2) AS avg_salary,
       ROUND(MIN(salary), 2) AS min_salary,
       ROUND(MAX(salary), 2) AS max_salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
GROUP  BY customer_tier
ORDER  BY cnt DESC;
```

✅ Expected:

| customer_tier | cnt | avg_salary | min_salary | max_salary |
|---|---|---|---|---|
| silver | 270 | 115908.02 | … | … |
| platinum | 249 | 116659.31 | … | … |
| gold | 248 | 115645.87 | … | … |
| standard | 233 | 115137.04 | … | … |

---

### Check 5 — Snap audit columns (confirm Iceberg provenance)

```sql
SELECT customer_id, snap_id, snap_timestamp
FROM   lakehouse.lakehouse_db.vw_customer_latest
ORDER  BY customer_id
LIMIT  5;
```

✅ Expected: `snap_id` (bigint), `snap_timestamp` (timestamp) populated for every row

---

### Check 6 — View definition (confirm it points at the correct S3 path)

```sql
DESCRIBE EXTENDED lakehouse.lakehouse_db.vw_customer_latest;
```

Look for the `View Text` row — it should contain:
```
read_files('s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/data/', ...)
```

---

### Databricks console navigation

You can also browse the view in the **Catalog Explorer** (no SQL needed):

1. Go to **`https://dbc-11a1dbc5-061a.cloud.databricks.com/explore/data`**
2. Expand **`lakehouse`** → **`lakehouse_db`**
3. Click **`vw_customer_latest`**
4. Click **Sample Data** tab → shows the first 1 000 rows live

---

## 10. Refresh after a new Spark write

Every time new rows are written by Spark, run Steps 3 + 4 to re-point the view:

```bash
# 1. Resolve latest data path
DATA_PATH=$(python3 - "$AWS_KEY" "$AWS_SECRET" <<'EOF'
import sys, boto3, json
s3 = boto3.client('s3', region_name='us-east-2',
    aws_access_key_id=sys.argv[1], aws_secret_access_key=sys.argv[2])
pag = s3.get_paginator('list_objects_v2')
objs = []
for page in pag.paginate(Bucket='stardata-databricks',
                          Prefix='iceberg/warehouse/lakehouse_db/customer/metadata/'):
    for o in page.get('Contents', []):
        if o['Key'].endswith('.metadata.json'):
            objs.append(o)
objs.sort(key=lambda x: x['LastModified'], reverse=True)
meta = json.loads(s3.get_object(
    Bucket='stardata-databricks', Key=objs[0]['Key'])['Body'].read())
print(meta['location'].rstrip('/') + '/data/')
EOF
)
echo "New data path: $DATA_PATH"

# 2. Recreate view pointing at new snapshot
curl -s -X POST "$DB_WS/api/2.0/sql/statements" \
  -H "Authorization: Bearer $DB_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"warehouse_id\":\"$WAREHOUSE_ID\",\"wait_timeout\":\"50s\",
       \"statement\":\"CREATE OR REPLACE VIEW lakehouse.lakehouse_db.vw_customer_latest AS SELECT * FROM read_files('${DATA_PATH}', format => 'parquet', mergeSchema => true)\"}" \
  | python3 -c "
import sys,json; d=json.load(sys.stdin)
print('✅' if d.get('status',{}).get('state')=='SUCCEEDED' else d)
"
```

---

## 11. Key paths reference

| Resource | Value |
|---|---|
| Spark seed script | [`docker/spark-gluten-velox/scripts/databricks_customer_seed.py`](../../docker/spark-gluten-velox/scripts/databricks_customer_seed.py) |
| Databricks notebook | [`docker/spark-gluten-velox/scripts/databricks_customer_parquet_reader.py`](../../docker/spark-gluten-velox/scripts/databricks_customer_parquet_reader.py) |
| S3 bucket | `s3://stardata-databricks/` |
| Iceberg table prefix | `s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/` |
| Metadata prefix | `s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/metadata/` |
| Data prefix | `s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/data/` |
| Databricks catalog | `lakehouse` (MANAGED) |
| Databricks schema | `lakehouse.lakehouse_db` |
| Databricks view | `lakehouse.lakehouse_db.vw_customer_latest` |
| Databricks workspace | `https://dbc-11a1dbc5-061a.cloud.databricks.com` |
| Databricks SQL warehouse | `942026cf5e55f3c3` (Serverless Starter) |
| Unity Catalog external location | `stardata_databricks_iceberg` → `s3://stardata-databricks/` |
| Unity Catalog storage credential | `stardata_databricks_s3` (IAM role `databricks-unity-catalog`) |
| OpenBao Databricks PAT | `secret/databricks/pat` → `token` |
| OpenBao S3 credentials | `secret/platform/s3` → `access_key`, `secret_key` |
