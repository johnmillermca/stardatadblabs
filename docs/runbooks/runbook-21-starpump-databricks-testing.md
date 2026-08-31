# Runbook 21 — starpump Databricks Integration: End-to-End Testing

| Field | Value |
|---|---|
| **Runbook ID** | RB-21 |
| **Service** | k8s-platform / starpump / databricks |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-08-31 (added Section 10: insert data in Databricks → run starpump) |

---

## 1. Overview

This runbook tests all four deliverables of the Databricks starpump integration:

| Step | What is tested |
|---|---|
| **(a)** | Create `databricks.lakehouse_db.customer` Iceberg table in the `databricks` catalog via `databricks_customer_seed.py` |
| **(b)** | Verify the `databricks` namespace is registered in Spark-Gluten and visible from the cluster |
| **(c)** | Verify the JDBC connection from Spark → Databricks SQL Warehouse; confirm `starpump databricks` can discover tables |
| **(d)** | Run `starpump databricks` end-to-end: reads from Databricks via JDBC, creates the Iceberg table if missing, copies all data to Polaris/Iceberg on S3 |

### Architecture

```
Databricks SQL Warehouse         Spark-Gluten cluster (k8s)
─────────────────────────        ──────────────────────────────────────────
lakehouse.lakehouse_db            starpump databricks
  customer  (1 100 rows)  ──JDBC──► _db_list_tables()
  orders    (5 000 rows)           _db_table_schema()
                                   _db_table_sizes()
                                   _copy_table()  ──► IcebergTableBuilder
                                                         │
                                                         ▼
                               Polaris REST catalog (star_lakehouse)
                               databricks.lakehouse_db.customer   (S3)
                               databricks.lakehouse_db.orders     (S3)
                                         │
                                         ▼
                               s3://stardata-databricks/iceberg/warehouse/
```

### Key paths

| Resource | Value |
|---|---|
| Spark master | `spark-master-internal.prod.svc.cluster.local:17077` |
| Spark master UI | `http://192.168.1.50:30707` |
| Polaris REST | `http://polaris-rest.prod.svc.cluster.local:8181/api/catalog` |
| Polaris warehouse | `star_lakehouse` |
| Iceberg catalog (Spark) | `databricks` |
| Iceberg namespace | `lakehouse_db` |
| S3 bucket | `stardata-databricks` |
| S3 base path | `s3://stardata-databricks/iceberg/warehouse/lakehouse_db/` |
| Databricks workspace | `https://dbc-11a1dbc5-061a.cloud.databricks.com` |
| Databricks SQL warehouse | `942026cf5e55f3c3` (Serverless Starter) |
| OpenBao Databricks secret | `secret/data/platform/databricks` |
| starpump script | `/opt/spark/work-dir/starpump.py` |
| starpump CLI | `/usr/local/bin/starpump` |
| Databricks seed script | `/opt/spark/work-dir/databricks_customer_seed.py` |

---

## 2. Prerequisites

### 2-A — Store Databricks credentials in OpenBao

Run once from any node with `kubectl` access:

```bash
# Get a fresh root token
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

# Write Databricks credentials to OpenBao
curl -s -X POST \
  -H "X-Vault-Token: $TOKEN" \
  -H "Content-Type: application/json" \
  http://192.168.1.50:30820/v1/secret/data/platform/databricks \
  -d '{
    "data": {
      "host":      "dbc-11a1dbc5-061a.cloud.databricks.com",
      "http_path": "/sql/1.0/warehouses/942026cf5e55f3c3",
      "token":     "<your-databricks-pat-or-oauth-token>",
      "catalog":   "lakehouse",
      "schema":    "lakehouse_db"
    }
  }'
```

✅ Expected: `{"request_id":"...","data":{"created_time":"...","version":1}}`

**Verify the secret was written:**
```bash
curl -s -H "X-Vault-Token: $TOKEN" \
  http://192.168.1.50:30820/v1/secret/data/platform/databricks \
  | python3 -m json.tool | grep -E '"host"|"http_path"|"catalog"'
```

✅ Expected:
```json
"host": "dbc-11a1dbc5-061a.cloud.databricks.com",
"http_path": "/sql/1.0/warehouses/942026cf5e55f3c3",
"catalog": "lakehouse"
```

---

### 2-B — Download and stage the Databricks JDBC JAR

The Simba JDBC driver must be placed in `docker/spark-gluten-velox/jars/` before building:

```bash
# Download from Databricks (requires login)
# https://www.databricks.com/spark/jdbc-drivers-download
# File: databricks-jdbc-2.6.36.1070.jar

# Stage into the jars directory
cp ~/Downloads/databricks-jdbc-2.6.36.1070.jar \
   docker/spark-gluten-velox/jars/databricks-jdbc-2.6.36.1070.jar
```

> ⚠️ The image build will fail with `COPY failed: file not found` if the JAR is missing.

---

### 2-C — Build and push the updated image

```bash
bash docker/spark-gluten-velox/build-and-push.sh
```

✅ Expected: image pushed as `spark-gluten-velox:3.5.1` (or your tag).

---

### 2-D — Restart Spark pods to pick up the new image

```bash
kubectl rollout restart deployment spark-master spark-worker -n prod
kubectl rollout status  deployment spark-master -n prod --timeout=120s
kubectl rollout status  deployment spark-worker -n prod --timeout=120s
```

✅ Expected: `deployment "spark-master" successfully rolled out`

---

### 2-E — Common setup (run before every test section)

```bash
# Capture the master pod name — changes after every restart
MASTER=$(kubectl get pod -n prod -l app=spark-master \
  -o jsonpath='{.items[0].metadata.name}')
echo "Master pod: $MASTER"

# Capture root token for OpenBao calls
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)
echo "Token: ${TOKEN:0:10}..."
```

---

## 3. Test (a) — Create the Databricks Iceberg table

Run [`databricks_customer_seed.py`](../../docker/spark-gluten-velox/scripts/databricks_customer_seed.py) to:
1. Create namespace `lakehouse_db` in the `databricks` Polaris catalog (idempotent)
2. Create Iceberg table `databricks.lakehouse_db.customer` (IF NOT EXISTS)
3. Insert 1 000 synthetic rows via `IcebergTableBuilder.write_append()`

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  spark-submit \
    --py-files /opt/spark/work-dir/bao_spark_init.py,/opt/spark/work-dir/spark_iceberg_utils.py \
    /opt/spark/work-dir/databricks_customer_seed.py
```

### ✅ Expected output

```
=== customer-seed | catalog=databricks ns=lakehouse_db table=customer bucket=stardata-databricks rows=1000 ===
Namespace `databricks`.`lakehouse_db` ready.
[dave] Creating Iceberg table `databricks`.`lakehouse_db`.`customer` (file_size=256MB) …
[dave] Table `databricks`.`lakehouse_db`.`customer` ready.
Table ready: `databricks`.`lakehouse_db`.`customer`  location=s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer
[dave] write_append → databricks.lakehouse_db.customer: 1000 rows
Inserted 1000 rows into `databricks`.`lakehouse_db`.`customer`
+-----+
|total|
+-----+
| 1000|
+-----+
```

### Verify on S3

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN python3 - << 'EOF'
import urllib.request, json, os
TOKEN = os.environ["TOKEN"]
def bao(path, field):
    req = urllib.request.Request(
        f"http://openbao.prod.svc.cluster.local:8200/v1/{path}",
        headers={"X-Vault-Token": TOKEN})
    return json.loads(urllib.request.urlopen(req,timeout=10).read())["data"]["data"][field]
s3_key    = bao("secret/data/platform/s3", "access_key")
s3_secret = bao("secret/data/platform/s3", "secret_key")
import subprocess
result = subprocess.run([
    "aws", "s3", "ls",
    "s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/",
    "--recursive", "--summarize",
    f"--access-key-id={s3_key}",
    f"--secret-access-key={s3_secret}",
    "--endpoint-url=https://s3.us-east-2.amazonaws.com"
], capture_output=True, text=True)
print(result.stdout[-500:])
EOF
```

✅ Expected: `metadata/` and `data/` folders listed with `.metadata.json` and `.parquet` files.

---

## 4. Test (b) — Databricks namespace visible in Spark-Gluten

Verify the `databricks` catalog and `lakehouse_db` namespace are registered and accessible from the Spark cluster.

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  spark-submit --master spark://spark-master-internal.prod.svc.cluster.local:17077 \
  /opt/spark/work-dir/starpump.py databricks DRY_RUN=1 \
  INCLUDE_TABLES=customer 2>&1 | grep -E "catalog|namespace|databricks|Namespace|WARN OAuth"
```

For a direct namespace check, run inside the pod:

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN python3 - << 'EOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession

bao   = BaoSparkInit()
conf  = bao.spark_conf(app_name="namespace-check")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=== Catalogs ===")
spark.sql("SHOW CATALOGS").show()

print("=== Namespaces in databricks catalog ===")
spark.sql("SHOW NAMESPACES IN databricks").show()

print("=== Tables in databricks.lakehouse_db ===")
spark.sql("SHOW TABLES IN databricks.lakehouse_db").show()

spark.stop()
EOF
```

### ✅ Expected output

```
=== Catalogs ===
+-----------+
|  catalog  |
+-----------+
|databricks |
|polaris    |
|spark_...  |
+-----------+

=== Namespaces in databricks catalog ===
+-------------+
|   namespace |
+-------------+
| lakehouse_db|
+-------------+

=== Tables in databricks.lakehouse_db ===
+------------+---------+-----------+
|   namespace|tableName|isTemporary|
+------------+---------+-----------+
|lakehouse_db| customer|      false|
+------------+---------+-----------+
```

---

## 5. Test (c) — JDBC connection from Spark to Databricks

Verify the Simba JDBC driver connects to the Databricks SQL Warehouse and can discover and read tables.

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN python3 - << 'EOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession

bao  = BaoSparkInit()
conf = bao.spark_conf(app_name="jdbc-test")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Build JDBC options pointing at lakehouse.lakehouse_db
opts = bao.databricks_jdbc_options(catalog="lakehouse", schema="lakehouse_db")
print(f"JDBC URL (masked): {opts['url'][:80]}...")
print(f"Driver           : {opts['driver']}")

# Test 1: SHOW TABLES via JDBC
df_tables = (
    spark.read.format("jdbc")
    .options(**opts)
    .option("query", "SHOW TABLES IN `lakehouse`.`lakehouse_db`")
    .load()
)
print(f"\n=== Tables discovered via JDBC ===")
df_tables.show(truncate=False)

# Test 2: row count via JDBC
df_count = (
    spark.read.format("jdbc")
    .options(**opts)
    .option("query", "SELECT COUNT(*) AS total FROM `lakehouse`.`lakehouse_db`.`customer`")
    .load()
)
print(f"=== Row count via JDBC ===")
df_count.show()

spark.stop()
print("✅ JDBC connection test passed")
EOF
```

### ✅ Expected output

```
JDBC URL (masked): jdbc:databricks://dbc-11a1dbc5-061a.cloud.databricks.com:443;httpPath=...
Driver           : com.databricks.client.jdbc.Driver

=== Tables discovered via JDBC ===
+------------+---------+-----------+
|   namespace|tableName|isTemporary|
+------------+---------+-----------+
|lakehouse_db| customer|      false|
+------------+---------+-----------+

=== Row count via JDBC ===
+-----+
|total|
+-----+
| 1100|
+-----+

✅ JDBC connection test passed
```

---

## 6. Test (d) — starpump databricks: full copy with auto-create

This is the primary integration test. It runs the complete pipeline:
1. Discovers tables in `lakehouse.lakehouse_db` via JDBC
2. Creates `databricks.lakehouse_db.customer` in Iceberg if it does not exist
3. Copies all rows in 100 000-row batches
4. Writes watermarks to both Iceberg and PostgreSQL

### 6-A — Dry-run first (DDL only, no data copy)

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN DRY_RUN=1 \
  DATABASE=lakehouse SCHEMAS=lakehouse_db \
  starpump databricks
```

✅ Expected:
```
=== starpump databricks | run_id=... user=dave db=lakehouse schema=lakehouse_db catalog=databricks threads=8 ===
Discovered 1 tables in lakehouse.lakehouse_db: ['customer']
[size-report] customer   →   0.0 GB  (COPY)
[customer] START: 0.0 GB | discovering schema …
[customer] Iceberg table DDL ready: `databricks`.`lakehouse_db`.`customer`
[customer] DRY_RUN — skipping data copy.
Completed in X.Xs — 1/1 copied | 0 skipped | 0 failed | 0 rows written
```

### 6-B — Full copy (all tables)

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=lakehouse SCHEMAS=lakehouse_db \
  starpump databricks
```

✅ Expected (key lines):
```
=== starpump databricks | run_id=... user=dave db=lakehouse schema=lakehouse_db catalog=databricks threads=8 ===
=== Filters: include=all  exclude=none  max_size=3.0 GB ===
Discovered 1 tables in lakehouse.lakehouse_db: ['customer']
Namespace `databricks`.`lakehouse_db` ready.
[customer] START: 0.0 GB | discovering schema …
[customer] Iceberg table DDL ready: `databricks`.`lakehouse_db`.`customer`
[customer] sf_extraction_ts=2026-08-31T... (CDC sync point)
[customer] Early watermark written to pipeline DB.
[customer] batch offset=0 rows=1100 total=1100
[customer] DONE — 1100 rows written (total incl. prior runs).
[iceberg-watermark] lakehouse.lakehouse_db.customer → rows=1100
[pg-watermark] upserted lakehouse.lakehouse_db.customer rows=1100
─────────────────────────────────────────────────────────────
Completed in X.Xs — 1/1 copied | 0 skipped | 0 failed | 1100 rows written
```

### 6-C — Verify the Iceberg table was created and populated

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN python3 - << 'EOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession

bao   = BaoSparkInit()
conf  = bao.spark_conf(app_name="verify-copy")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Row count
spark.sql("SELECT COUNT(*) AS total_rows FROM databricks.lakehouse_db.customer").show()

# Sample rows
spark.sql("""
    SELECT customer_id, full_name, city, customer_tier, snap_id, snap_timestamp
    FROM   databricks.lakehouse_db.customer
    ORDER  BY customer_id
    LIMIT  5
""").show(truncate=False)

# Tier distribution
spark.sql("""
    SELECT customer_tier, COUNT(*) AS cnt, ROUND(AVG(salary),2) AS avg_salary
    FROM   databricks.lakehouse_db.customer
    GROUP  BY customer_tier
    ORDER  BY cnt DESC
""").show()

# Watermark
spark.sql("""
    SELECT source_db, source_schema, table_name,
           sf_extraction_ts, rows_copied, pipeline_run_ts
    FROM   databricks.lakehouse_db._pipeline_watermarks
""").show(truncate=False)

spark.stop()
EOF
```

✅ Expected:
```
+----------+
|total_rows|
+----------+
|      1100|
+----------+

+-----------+------------------+-------+-------------+-------------------+-------------------+
|customer_id|full_name         |city   |customer_tier|snap_id            |snap_timestamp     |
+-----------+------------------+-------+-------------+-------------------+-------------------+
|1          |Wei Brown         |Toronto|standard     |8589934592         |2026-08-31 ...     |
...

+-------------+----+----------+
|customer_tier| cnt|avg_salary|
+-------------+----+----------+
|silver       | 280|115908.00 |
|platinum      | 285|116659.00 |
...

+----------+-------------+--------+---------------------------+----------+-------------------+
|source_db |source_schema|table...|sf_extraction_ts           |rows_copied|pipeline_run_ts   |
+----------+-------------+--------+---------------------------+----------+-------------------+
|lakehouse |lakehouse_db |customer|2026-08-31T...Z            |1100      |2026-08-31 ...    |
+----------+-------------+--------+---------------------------+----------+-------------------+
```

### 6-D — Auto-create test: drop the Iceberg table and re-run

This proves that starpump creates the Iceberg table automatically when it does not exist.

```bash
# Step 1: drop the Iceberg table
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN python3 - << 'EOF'
import os; os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from spark_iceberg_utils import IcebergTableBuilder
from pyspark.sql import SparkSession
bao   = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf()).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
IcebergTableBuilder(spark, "dave").drop_table("databricks", "lakehouse_db", "customer")
print("✅ Dropped databricks.lakehouse_db.customer")
spark.stop()
EOF

# Step 2: re-run starpump — it should auto-create and copy
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=lakehouse SCHEMAS=lakehouse_db \
  starpump databricks
```

✅ Expected: same output as 6-B. The table is recreated and all 1 100 rows are copied.

---

## 7. Test (c+d) — starpump copy with specific table filter

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=lakehouse SCHEMAS=lakehouse_db \
  INCLUDE_TABLES=customer \
  starpump databricks
```

✅ Expected: only `customer` is copied, other tables skipped.

---

## 8. Test (d) — starpump resume after partial copy

Simulate a partial failure by killing the job mid-run, then re-running to verify it resumes from the watermark offset.

```bash
# Terminal 1: start the copy
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=lakehouse SCHEMAS=lakehouse_db \
  BATCH_SIZE=100 \
  starpump databricks &

# Terminal 2: kill after first batch
sleep 10
PID=$(kubectl exec -n prod $MASTER -c spark-master -- pgrep -f starpump.py)
kubectl exec -n prod $MASTER -c spark-master -- kill -9 $PID

# Re-run — it should log "RESUME: N rows already in Iceberg"
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=lakehouse SCHEMAS=lakehouse_db \
  BATCH_SIZE=100 \
  starpump databricks
```

✅ Expected on re-run:
```
[customer] RESUME: 100 rows already in Iceberg — reusing sf_extraction_ts=... starting at offset=100.
[customer] batch offset=100 rows=100 total=200
...
[customer] DONE — 1100 rows written (total incl. prior runs).
```

---

## 9. Databricks SQL console — verify the Iceberg data is readable

After the starpump copy, verify the data is visible from the Databricks SQL console via the auto-discovery notebook view.

Open **`https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor`**
Select warehouse: **Serverless Starter Warehouse**

### Query 1 — Row count matches starpump output

```sql
SELECT COUNT(*) AS total_rows
FROM lakehouse.lakehouse_db.vw_customer_latest;
```
✅ Expected: `1100`

### Query 2 — snap_id and snap_timestamp populated (starpump injected)

```sql
SELECT customer_id, full_name, snap_id, snap_timestamp
FROM   lakehouse.lakehouse_db.vw_customer_latest
ORDER  BY customer_id
LIMIT  5;
```
✅ Expected: `snap_id` is a non-null BIGINT, `snap_timestamp` is a non-null TIMESTAMP.

### Query 3 — Both insert batches visible (seed=42 and starpump)

```sql
SELECT
    DATE(created_at) AS insert_date,
    MIN(customer_id) AS first_id,
    MAX(customer_id) AS last_id,
    COUNT(*)         AS row_count
FROM   lakehouse.lakehouse_db.vw_customer_latest
GROUP  BY DATE(created_at)
ORDER  BY insert_date;
```

✅ Expected:
| insert_date | first_id | last_id | row_count |
|---|---|---|---|
| 2026-01-xx | 1 | 1000 | 1000 |
| 2026-09-xx | 1001 | 1100 | 100 |

### Query 4 — Verify view exists (Serverless-safe check)

```python
# Run in a Databricks notebook
print(spark.catalog.tableExists("lakehouse.lakehouse_db.vw_customer_latest"))
# ✅ True
```

---

## 10. Insert data in Databricks then run starpump (end-to-end smoke test)

This is the simplest way to prove the full round-trip: insert rows directly in
Databricks, run `starpump databricks`, then confirm the rows arrived in Iceberg.

### Step 1 — Insert test rows in the Databricks SQL console

Open **`https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor`**,
select **Serverless Starter Warehouse**, and run:

```sql
INSERT INTO lakehouse.lakehouse_db.customer
  (customer_id, full_name, email, phone_number, date_of_birth,
   national_id, street_address, city, country_code, ip_address,
   salary, customer_tier, is_active, created_at, updated_at,
   snap_id, snap_timestamp)
VALUES
  (2001, 'Alice Test',  'alice.test@example.com',  '+1-555-0001', DATE'1985-03-12', 'ID-02001-TEST', '1 Test St',   'Toronto', 'CA', '10.0.0.1', 95000.00,  'gold',     1, NOW(), NOW(), NULL, NOW()),
  (2002, 'Bob Test',    'bob.test@example.com',    '+1-555-0002', DATE'1990-07-22', 'ID-02002-TEST', '2 Test Ave',  'London',  'GB', '10.0.0.2', 72000.00,  'silver',   1, NOW(), NOW(), NULL, NOW()),
  (2003, 'Carol Test',  'carol.test@example.com',  '+1-555-0003', DATE'1978-11-05', 'ID-02003-TEST', '3 Test Blvd', 'Berlin',  'DE', '10.0.0.3', 130000.00, 'platinum', 1, NOW(), NOW(), NULL, NOW()),
  (2004, 'Dave Test',   'dave.test@example.com',   '+1-555-0004', DATE'1995-01-30', 'ID-02004-TEST', '4 Test Rd',   'Tokyo',   'JP', '10.0.0.4', 55000.00,  'standard', 1, NOW(), NOW(), NULL, NOW()),
  (2005, 'Eve Test',    'eve.test@example.com',    '+1-555-0005', DATE'1988-09-14', 'ID-02005-TEST', '5 Test Lane', 'Sydney',  'AU', '10.0.0.5', 115000.00, 'gold',     1, NOW(), NOW(), NULL, NOW());
```

Confirm the rows landed:
```sql
SELECT customer_id, full_name, city, customer_tier
FROM   lakehouse.lakehouse_db.customer
WHERE  customer_id BETWEEN 2001 AND 2005
ORDER  BY customer_id;
```
✅ Expected: 5 rows returned with the names and tiers above.

---

### Step 2 — Common setup (terminal)

```bash
MASTER=$(kubectl get pod -n prod -l app=spark-master \
  -o jsonpath='{.items[0].metadata.name}')
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)
echo "Master: $MASTER  Token: ${TOKEN:0:10}..."
```

---

### Step 3 — Run starpump databricks

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=lakehouse SCHEMAS=lakehouse_db \
  starpump databricks
```

✅ Key lines to look for:
```
Discovered 1 tables in lakehouse.lakehouse_db: ['customer']
[customer] batch offset=0 rows=XXXX total=XXXX
[customer] DONE — XXXX rows written (total incl. prior runs).
Completed in X.Xs — 1/1 copied | 0 skipped | 0 failed | XXXX rows written
```

> The row count `XXXX` will be the previous total **plus the 5 new rows** you just inserted.

---

### Step 4 — Verify the new rows landed in Iceberg (from the Spark cluster)

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN python3 - << 'EOF'
import os; os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession

bao   = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf(app_name="verify")).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Total rows — should be previous count + 5
spark.sql("SELECT COUNT(*) AS total FROM databricks.lakehouse_db.customer").show()

# Confirm the 5 test rows are present with snap_id + snap_timestamp injected by starpump
spark.sql("""
    SELECT customer_id, full_name, city, customer_tier, snap_id, snap_timestamp
    FROM   databricks.lakehouse_db.customer
    WHERE  customer_id BETWEEN 2001 AND 2005
    ORDER  BY customer_id
""").show(truncate=False)

spark.stop()
EOF
```

✅ Expected: rows 2001–2005 are present, `snap_id` is a non-null BIGINT and
`snap_timestamp` is a non-null TIMESTAMP — both injected by starpump automatically.

---

### Step 5 — Verify in the Databricks SQL console

```sql
-- Row count in the Iceberg-backed view (should include your 5 new rows)
SELECT COUNT(*) AS total_rows
FROM   lakehouse.lakehouse_db.vw_customer_latest;

-- Confirm your test rows are visible through the view
SELECT customer_id, full_name, city, snap_id, snap_timestamp
FROM   lakehouse.lakehouse_db.vw_customer_latest
WHERE  customer_id BETWEEN 2001 AND 2005
ORDER  BY customer_id;
```

✅ If `snap_id` and `snap_timestamp` are populated, the full round-trip is confirmed:

```
Databricks (source) → starpump JDBC read → Iceberg on S3 (Polaris) → Databricks view
```

---

## 11. Troubleshooting

### `ClassNotFoundException: com.databricks.client.jdbc.Driver`

The Databricks JDBC JAR is not on the classpath.

**Fix:** Verify the JAR was staged and the image was rebuilt:
```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  ls -lh /opt/spark/jars/databricks-jdbc-*.jar
```
If missing: stage the JAR, rebuild the image, and restart the pods (Section 2-C / 2-D).

---

### `java.sql.SQLException: [Databricks][DatabricksJDBCDriver] ... Invalid token`

The PAT or OAuth token in OpenBao is expired or incorrect.

**Fix:** Regenerate the token in Databricks, update OpenBao:
```bash
curl -s -X POST \
  -H "X-Vault-Token: $TOKEN" \
  -H "Content-Type: application/json" \
  http://192.168.1.50:30820/v1/secret/data/platform/databricks \
  -d '{"data": {"token": "<new-token>", ...}}'
```

---

### `RuntimeError: Cannot authenticate to OpenBao`

No K8s Service Account JWT and no `TOKEN` env var.

**Fix:** Always pass `TOKEN=$TOKEN` in every `kubectl exec` command (Section 2-E).

---

### `AnalysisException: CANNOT_FIND_DATA for column snap_id`

The DataFrame being written to Iceberg is missing `snap_id` or `snap_timestamp`.

**Fix:** Always use `IcebergTableBuilder.write_append()` — never raw `.writeTo().append()`.
starpump uses `write_append()` internally so this should not occur from starpump runs.
If it occurs from a manual script, add both columns to `CUSTOMER_SCHEMA` (see Runbook 20 Section 5-D fix).

---

### `[CROSS_CATALOG_SCHEMA_REFERENCE_NOT_SUPPORTED]`

A `SHOW VIEWS IN lakehouse.lakehouse_db` call was made on Serverless compute.

**Fix:** Already resolved in the codebase — `starpump.py` and `nb_multi_table_auto_reader.py` both use `spark.catalog.tableExists()` instead.

---

### `AnalysisException: Namespace 'lakehouse_db' not found`

The namespace was not created before the table was accessed.

**Fix:** starpump calls `builder.ensure_namespace(ICEBERG_CATALOG, ICEBERG_NAMESPACE)` automatically at the start of every run. If this error appears, confirm `ICEBERG_CATALOG=databricks` and `SCHEMAS=lakehouse_db` are set correctly.

---

### Job hangs at `[Stage N:>` for > 2 minutes

Another Spark app is holding all cores. Check the Spark master UI at `http://192.168.1.50:30707`.
If a zombie app is shown, wait up to 30 minutes for `spark-app-cleanup` to evict it, or run:
```bash
kubectl exec -n prod $MASTER -c spark-master -- spark-app-cleanup
```

---

## 12. Key files reference

| File | Purpose |
|---|---|
| [`docker/spark-gluten-velox/scripts/starpump.py`](../../docker/spark-gluten-velox/scripts/starpump.py) | starpump entry point — databricks connector at `_CONNECTORS["databricks"]` |
| [`docker/spark-gluten-velox/scripts/bao_spark_init.py`](../../docker/spark-gluten-velox/scripts/bao_spark_init.py) | `databricks_creds()`, `databricks_jdbc_options()`, `databricks` catalog in `spark_conf()` |
| [`docker/spark-gluten-velox/scripts/spark_iceberg_utils.py`](../../docker/spark-gluten-velox/scripts/spark_iceberg_utils.py) | `IcebergTableBuilder` — auto-creates table + injects `snap_id`/`snap_timestamp` |
| [`docker/spark-gluten-velox/scripts/databricks_customer_seed.py`](../../docker/spark-gluten-velox/scripts/databricks_customer_seed.py) | Seed script: creates `databricks.lakehouse_db.customer` and inserts 1 000 rows |
| [`docker/spark-gluten-velox/Dockerfile`](../../docker/spark-gluten-velox/Dockerfile) | Adds `databricks-jdbc-2.6.36.1070.jar` + all scripts to the image |
| [`docs/runbooks/snowflake-to-iceberg/spark-defaults-configmap.yaml`](../../docs/runbooks/snowflake-to-iceberg/spark-defaults-configmap.yaml) | Adds `databricks` catalog stanza to `spark-defaults.conf` ConfigMap |
| [`docker/databricks-notebooks/nb_multi_table_auto_reader.py`](../../docker/databricks-notebooks/nb_multi_table_auto_reader.py) | Databricks notebook: auto-discovers Iceberg tables and creates views |
| [`docs/runbooks/runbook-20-databricks-customer-parquet-view.md`](runbook-20-databricks-customer-parquet-view.md) | Runbook 20: Iceberg write from JupyterHub + read in Databricks |
