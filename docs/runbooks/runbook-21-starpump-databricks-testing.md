# Runbook 21 — starpump Databricks Integration: End-to-End Testing

| Field | Value |
|---|---|
| **Runbook ID** | RB-21 |
| **Service** | k8s-platform / starpump / databricks |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-08-31 |

---

## 1. Overview

This runbook tests all four deliverables of the Databricks starpump integration
using the **`product`** table as the reference dataset.

| Step | What is tested |
|---|---|
| **(a)** | Create `lakehouse.lakehouse_db.product` in Databricks Unity Catalog via `databricks_product_seed.py` |
| **(b)** | Verify the `databricks` namespace is registered in Spark-Gluten and visible from the cluster |
| **(c)** | Verify the JDBC connection from Spark → Databricks SQL Warehouse; confirm `starpump databricks` can discover the `product` table |
| **(d)** | Run `starpump databricks` end-to-end: reads `product` from Databricks via JDBC, auto-creates the Iceberg table if missing, copies all data to Polaris/Iceberg on S3 |

### Architecture

```
Databricks SQL Warehouse              Spark-Gluten cluster (k8s)
────────────────────────              ──────────────────────────────────────────
lakehouse.lakehouse_db                 starpump databricks
  product  (500 rows)   ──JDBC──►  _db_list_tables()
                                    _db_table_schema()
                                    _db_table_sizes()
                                    _copy_table()  ──► IcebergTableBuilder
                                                          │
                                                          ▼
                                  Polaris REST catalog (star_lakehouse)
                                  databricks.lakehouse_db.product  (S3)
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
| Product seed script | [`docker/spark-gluten-velox/scripts/databricks_product_seed.py`](../../docker/spark-gluten-velox/scripts/databricks_product_seed.py) |

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

## 3. Test (a) — Create the product table in Databricks

Run [`databricks_product_seed.py`](../../docker/spark-gluten-velox/scripts/databricks_product_seed.py) to:
1. Create schema `lakehouse_db` in Databricks Unity Catalog (idempotent)
2. Create Delta table `lakehouse.lakehouse_db.product` (IF NOT EXISTS)
3. Insert 500 synthetic product rows in batches of 100

Install the connector on the machine running this script (any machine with Python):

```bash
pip install databricks-sql-connector
```

Then run:

```bash
TOKEN=$TOKEN python3 docker/spark-gluten-velox/scripts/databricks_product_seed.py
```

Or dry-run to see the DDL without executing:

```bash
TOKEN=$TOKEN DRY_RUN=1 python3 docker/spark-gluten-velox/scripts/databricks_product_seed.py
```

### ✅ Expected output

```
=== product-seed | catalog=lakehouse schema=lakehouse_db table=product rows=500 ===
Credentials loaded — host=dbc-11a1dbc5-061a.cloud.databricks.com
Schema `lakehouse`.`lakehouse_db` ready.
Table `lakehouse`.`lakehouse_db`.`product` ready.
Inserted batch offset=0 size=100  (total=100)
Inserted batch offset=100 size=100  (total=200)
Inserted batch offset=200 size=100  (total=300)
Inserted batch offset=300 size=100  (total=400)
Inserted batch offset=400 size=100  (total=500)
✅ Total rows in lakehouse.lakehouse_db.product: 500
```

### Verify in the Databricks SQL console

Open **`https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor`**:

```sql
SELECT COUNT(*) AS total FROM lakehouse.lakehouse_db.product;
-- ✅ Expected: 500

SELECT product_id, product_name, category, brand, unit_price, stock_quantity
FROM   lakehouse.lakehouse_db.product
ORDER  BY product_id
LIMIT  10;
```

### Product table schema

| Column | Type | Description |
|---|---|---|
| `product_id` | INT | Unique product identifier |
| `product_name` | STRING | e.g. `SoundWave Speaker Pro` |
| `category` | STRING | e.g. `Electronics` |
| `sub_category` | STRING | e.g. `Audio` |
| `brand` | STRING | e.g. `SoundWave` |
| `sku` | STRING | e.g. `SKU-00042-ABCD` |
| `unit_price` | DOUBLE | Retail price |
| `cost_price` | DOUBLE | Cost to the business |
| `stock_quantity` | INT | Current stock level |
| `reorder_level` | INT | Stock level that triggers reorder |
| `weight_kg` | DOUBLE | Product weight |
| `is_active` | INT | 1 = active, 0 = discontinued |
| `created_at` | TIMESTAMP | Row creation time |
| `updated_at` | TIMESTAMP | Last update time |
| `snap_id` | BIGINT | NULL in Databricks — starpump injects on copy |
| `snap_timestamp` | TIMESTAMP | NULL in Databricks — starpump injects on copy |

---

## 4. Test (b) — Databricks namespace visible in Spark-Gluten

Verify the `databricks` catalog and `lakehouse_db` namespace are registered and accessible from the Spark cluster.

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
+------------+----------+-----------+
|   namespace| tableName|isTemporary|
+------------+----------+-----------+
|lakehouse_db|   product|      false|
+------------+----------+-----------+
```

---

## 5. Test (c) — JDBC connection from Spark to Databricks

Verify the Simba JDBC driver connects to the Databricks SQL Warehouse and can discover and read the `product` table.

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
print("\n=== Tables discovered via JDBC ===")
df_tables.show(truncate=False)

# Test 2: row count via JDBC
df_count = (
    spark.read.format("jdbc")
    .options(**opts)
    .option("query", "SELECT COUNT(*) AS total FROM `lakehouse`.`lakehouse_db`.`product`")
    .load()
)
print("=== Row count via JDBC ===")
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
+------------+----------+-----------+
|   namespace| tableName|isTemporary|
+------------+----------+-----------+
|lakehouse_db|   product|      false|
+------------+----------+-----------+

=== Row count via JDBC ===
+-----+
|total|
+-----+
|  500|
+-----+

✅ JDBC connection test passed
```

---

## 6. Test (d) — starpump databricks: full copy with auto-create

This is the primary integration test. starpump:
1. Discovers the `product` table in `lakehouse.lakehouse_db` via JDBC
2. Auto-creates `databricks.lakehouse_db.product` in Iceberg if it does not exist
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
Discovered 1 tables in lakehouse.lakehouse_db: ['product']
[size-report] product   →   0.0 GB  (COPY)
[product] START: 0.0 GB | discovering schema …
[product] Iceberg table DDL ready: `databricks`.`lakehouse_db`.`product`
[product] DRY_RUN — skipping data copy.
Completed in X.Xs — 1/1 copied | 0 skipped | 0 failed | 0 rows written
```

---

### 6-B — Full copy

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
Discovered 1 tables in lakehouse.lakehouse_db: ['product']
Namespace `databricks`.`lakehouse_db` ready.
[product] START: 0.0 GB | discovering schema …
[product] Iceberg table DDL ready: `databricks`.`lakehouse_db`.`product`
[product] sf_extraction_ts=2026-08-31T... (CDC sync point)
[product] Early watermark written to pipeline DB.
[product] batch offset=0 rows=500 total=500
[product] DONE — 500 rows written (total incl. prior runs).
[iceberg-watermark] lakehouse.lakehouse_db.product → rows=500
[pg-watermark] upserted lakehouse.lakehouse_db.product rows=500
─────────────────────────────────────────────────────────────
Completed in X.Xs — 1/1 copied | 0 skipped | 0 failed | 500 rows written
```

---

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
spark.sql("SELECT COUNT(*) AS total_rows FROM databricks.lakehouse_db.product").show()

# Sample rows
spark.sql("""
    SELECT product_id, product_name, category, brand, unit_price, snap_id, snap_timestamp
    FROM   databricks.lakehouse_db.product
    ORDER  BY product_id
    LIMIT  5
""").show(truncate=False)

# Category distribution
spark.sql("""
    SELECT category, COUNT(*) AS cnt, ROUND(AVG(unit_price),2) AS avg_price
    FROM   databricks.lakehouse_db.product
    GROUP  BY category
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
|       500|
+----------+

+----------+-------------------------+-----------+---------+----------+-------------------+-------------------+
|product_id|product_name             |category   |brand    |unit_price|snap_id            |snap_timestamp     |
+----------+-------------------------+-----------+---------+----------+-------------------+-------------------+
|1         |NovaTech Headphones Pro  |Electronics|NovaTech |149.99    |8589934592         |2026-08-31 ...     |
|2         |SoundWave Speaker Ultra  |Electronics|SoundWave|89.50     |8589934593         |2026-08-31 ...     |
...

+-----------+---+----------+
|category   |cnt|avg_price |
+-----------+---+----------+
|Electronics| 72|312.45    |
|Clothing   | 65|87.30     |
|Home       | 60|145.20    |
...

+----------+-------------+-------+---------------------------+-----------+-------------------+
|source_db |source_schema|table..|sf_extraction_ts           |rows_copied|pipeline_run_ts    |
+----------+-------------+-------+---------------------------+-----------+-------------------+
|lakehouse |lakehouse_db |product|2026-08-31T...Z            |500        |2026-08-31 ...     |
+----------+-------------+-------+---------------------------+-----------+-------------------+
```

---

### 6-D — Auto-create test: drop the Iceberg table and re-run

Proves that starpump auto-creates the Iceberg table when it does not exist.

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
IcebergTableBuilder(spark, "dave").drop_table("databricks", "lakehouse_db", "product")
print("✅ Dropped databricks.lakehouse_db.product")
spark.stop()
EOF

# Step 2: re-run starpump — it should auto-create and copy
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=lakehouse SCHEMAS=lakehouse_db \
  starpump databricks
```

✅ Expected: same output as 6-B. The table is recreated and all 500 rows are copied.

---

## 7. Copy only the product table (INCLUDE_TABLES filter)

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=lakehouse SCHEMAS=lakehouse_db \
  INCLUDE_TABLES=product \
  starpump databricks
```

✅ Expected: only `product` is copied; any other tables in the schema are skipped.

---

## 8. Resume after partial copy

Simulate a partial failure by killing the job mid-run, then re-running to verify it resumes from the watermark offset.

```bash
# Terminal 1: start the copy with a small batch size to see multiple batches
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
[product] RESUME: 100 rows already in Iceberg — reusing sf_extraction_ts=... starting at offset=100.
[product] batch offset=100 rows=100 total=200
[product] batch offset=200 rows=100 total=300
[product] batch offset=300 rows=100 total=400
[product] batch offset=400 rows=100 total=500
[product] DONE — 500 rows written (total incl. prior runs).
```

---

## 9. Insert new products in Databricks then run starpump (end-to-end smoke test)

The full round-trip: insert new rows in Databricks, run `starpump databricks`, confirm they arrived in Iceberg.

### Step 1 — Insert test rows in the Databricks SQL console

Open **`https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor`**,
select **Serverless Starter Warehouse**, and run:

```sql
INSERT INTO lakehouse.lakehouse_db.product
  (product_id, product_name, category, sub_category, brand, sku,
   unit_price, cost_price, stock_quantity, reorder_level,
   weight_kg, is_active, created_at, updated_at, snap_id, snap_timestamp)
VALUES
  (501, 'StarCore Tent Max',      'Sports',      'Outdoor',   'StarCore',  'SKU-00501-STAR', 299.99, 120.00, 45,  10, 3.200, 1, NOW(), NOW(), NULL, NULL),
  (502, 'BrightLife Blender Pro', 'Home',        'Kitchen',   'BrightLife','SKU-00502-BRGT', 89.99,  35.00,  200, 30, 2.100, 1, NOW(), NOW(), NULL, NULL),
  (503, 'AeroFit Yoga Mat Elite', 'Sports',      'Fitness',   'AeroFit',   'SKU-00503-AERO', 49.99,  18.00,  320, 50, 1.050, 1, NOW(), NOW(), NULL, NULL),
  (504, 'ZenFlow Watch Smart',    'Electronics', 'Wearables', 'ZenFlow',   'SKU-00504-ZENF', 199.99, 80.00,  88,  20, 0.150, 1, NOW(), NOW(), NULL, NULL),
  (505, 'UrbanEdge Jacket Sport', 'Clothing',    'Sportswear','UrbanEdge', 'SKU-00505-URBN', 129.99, 52.00,  150, 25, 0.850, 1, NOW(), NOW(), NULL, NULL);
```

Confirm the rows landed:
```sql
SELECT product_id, product_name, category, unit_price
FROM   lakehouse.lakehouse_db.product
WHERE  product_id BETWEEN 501 AND 505
ORDER  BY product_id;
```
✅ Expected: 5 rows returned.

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
Discovered 1 tables in lakehouse.lakehouse_db: ['product']
[product] batch offset=0 rows=505 total=505
[product] DONE — 505 rows written (total incl. prior runs).
Completed in X.Xs — 1/1 copied | 0 skipped | 0 failed | 505 rows written
```

---

### Step 4 — Verify the new rows landed in Iceberg

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN python3 - << 'EOF'
import os; os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession

bao   = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf(app_name="verify")).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Total rows (should be 505)
spark.sql("SELECT COUNT(*) AS total FROM databricks.lakehouse_db.product").show()

# Confirm the 5 new rows with snap_id + snap_timestamp injected by starpump
spark.sql("""
    SELECT product_id, product_name, category, unit_price, snap_id, snap_timestamp
    FROM   databricks.lakehouse_db.product
    WHERE  product_id BETWEEN 501 AND 505
    ORDER  BY product_id
""").show(truncate=False)

spark.stop()
EOF
```

✅ Expected: rows 501–505 are present, `snap_id` is a non-null BIGINT and
`snap_timestamp` is a non-null TIMESTAMP — both injected by starpump automatically.

---

### Step 5 — Verify in the Databricks SQL console

Run the auto-discovery notebook ([`nb_multi_table_auto_reader.py`](../../docker/databricks-notebooks/nb_multi_table_auto_reader.py))
to create/refresh the view `lakehouse.lakehouse_db.vw_product_latest`, then:

```sql
-- Row count in the Iceberg-backed view
SELECT COUNT(*) AS total_rows
FROM   lakehouse.lakehouse_db.vw_product_latest;
-- ✅ Expected: 505

-- Confirm new products are visible with snap_id populated
SELECT product_id, product_name, category, snap_id, snap_timestamp
FROM   lakehouse.lakehouse_db.vw_product_latest
WHERE  product_id BETWEEN 501 AND 505
ORDER  BY product_id;
```

✅ If `snap_id` and `snap_timestamp` are populated, the full round-trip is confirmed:

```
Databricks (source) → starpump JDBC read → Iceberg on S3 (Polaris) → Databricks view
```

---

## 10. Troubleshooting

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
  -d '{"data": {"token": "<new-token>", "host": "dbc-11a1dbc5-061a.cloud.databricks.com", "http_path": "/sql/1.0/warehouses/942026cf5e55f3c3", "catalog": "lakehouse", "schema": "lakehouse_db"}}'
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

---

### `[CROSS_CATALOG_SCHEMA_REFERENCE_NOT_SUPPORTED]`

A `SHOW VIEWS IN lakehouse.lakehouse_db` call was made on Serverless compute.

**Fix:** Already resolved in the codebase — `starpump.py` and `nb_multi_table_auto_reader.py` both use `spark.catalog.tableExists()` instead.

---

### `AnalysisException: Namespace 'lakehouse_db' not found`

The namespace was not created before the table was accessed.

**Fix:** starpump calls `builder.ensure_namespace(ICEBERG_CATALOG, ICEBERG_NAMESPACE)` automatically at the start of every run. Confirm `ICEBERG_CATALOG=databricks` and `SCHEMAS=lakehouse_db` are set correctly.

---

### Job hangs at `[Stage N:>` for > 2 minutes

Another Spark app is holding all cores. Check the Spark master UI at `http://192.168.1.50:30707`.
If a zombie app is shown, wait up to 30 minutes for `spark-app-cleanup` to evict it, or run:
```bash
kubectl exec -n prod $MASTER -c spark-master -- spark-app-cleanup
```

---

## 11. Key files reference

| File | Purpose |
|---|---|
| [`docker/spark-gluten-velox/scripts/starpump.py`](../../docker/spark-gluten-velox/scripts/starpump.py) | starpump entry point — databricks connector at `_CONNECTORS["databricks"]` |
| [`docker/spark-gluten-velox/scripts/bao_spark_init.py`](../../docker/spark-gluten-velox/scripts/bao_spark_init.py) | `databricks_creds()`, `databricks_jdbc_options()`, `databricks` catalog in `spark_conf()` |
| [`docker/spark-gluten-velox/scripts/spark_iceberg_utils.py`](../../docker/spark-gluten-velox/scripts/spark_iceberg_utils.py) | `IcebergTableBuilder` — auto-creates table + injects `snap_id`/`snap_timestamp` |
| [`docker/spark-gluten-velox/scripts/databricks_product_seed.py`](../../docker/spark-gluten-velox/scripts/databricks_product_seed.py) | Seed script: creates `lakehouse.lakehouse_db.product` and inserts 500 rows |
| [`docker/spark-gluten-velox/Dockerfile`](../../docker/spark-gluten-velox/Dockerfile) | Adds `databricks-jdbc-2.6.36.1070.jar` + all scripts to the image |
| [`docs/runbooks/snowflake-to-iceberg/spark-defaults-configmap.yaml`](../../docs/runbooks/snowflake-to-iceberg/spark-defaults-configmap.yaml) | Adds `databricks` catalog stanza to `spark-defaults.conf` ConfigMap |
| [`docker/databricks-notebooks/nb_multi_table_auto_reader.py`](../../docker/databricks-notebooks/nb_multi_table_auto_reader.py) | Databricks notebook: auto-discovers Iceberg tables and creates views |
