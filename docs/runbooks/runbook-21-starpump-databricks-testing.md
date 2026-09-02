# Runbook 21 — starpump: Copy Databricks → Iceberg

| Field | Value |
|---|---|
| **Runbook ID** | RB-21 |
| **Service** | k8s-platform / starpump / databricks |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-08-31 |

---

## Overview

Copy `lakehouse.lakehouse_db.product` from Databricks via JDBC into Iceberg on Polaris/S3.

```
Databricks SQL Warehouse                   Spark-Gluten cluster (k8s)
─────────────────────────                  ─────────────────────────────────────────
lakehouse.lakehouse_db                     starpump databricks (USER=dave)
  product  (N rows)    ──JDBC──►  _db_list_tables() / _db_table_schema()
                                  _copy_table()  ──► IcebergTableBuilder
                                                          │
                                                          ▼
                                  Polaris REST catalog (star_lakehouse)
                                  databricks.lakehouse_db.product  (S3)
                                            │
                                            ▼
                                  s3://stardata-databricks/iceberg/warehouse/
```

| Resource | Value |
|---|---|
| Iceberg catalog (Spark) | `databricks` |
| Iceberg namespace | `lakehouse_db` |
| S3 bucket | `stardata-databricks` |
| Databricks workspace | `https://dbc-11a1dbc5-061a.cloud.databricks.com` |
| starpump CLI | `/usr/local/bin/starpump` |
| Dynamic insert script | [`docker/spark-gluten-velox/scripts/databricks_product_insert_loop.py`](../../docker/spark-gluten-velox/scripts/databricks_product_insert_loop.py) |

---

## Step 1 — Common setup

Run once per terminal session before every step below:

```bash
MASTER=$(kubectl get pod -n prod -l component=master \
  -o jsonpath='{.items[0].metadata.name}')
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)
echo "Master: $MASTER   Token: ${TOKEN:0:10}..."
```

---

## Step 2 — Run starpump (full copy)

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=lakehouse SCHEMAS=lakehouse_db \
  starpump databricks
```

✅ Expected output:
```
=== starpump databricks | run_id=... user=dave db=lakehouse schema=lakehouse_db catalog=databricks threads=8 ===
[catalog-check] 'databricks' is registered (svc_id=<spark_svc_id>). Proceeding.
Discovered 1 tables in lakehouse.lakehouse_db: ['product']
Namespace `databricks`.`lakehouse_db` ready.
[product] START: 0.0 GB | discovering schema …
[product] Iceberg table DDL ready: `databricks`.`lakehouse_db`.`product`
[product] sf_extraction_ts=2026-08-31T... (CDC sync point)
[product] batch offset=0 rows=500 total=500
[product] DONE — 500 rows written (total incl. prior runs).
[iceberg-watermark] lakehouse.lakehouse_db.product → rows=500
[pg-watermark] upserted lakehouse.lakehouse_db.product rows=500
─────────────────────────────────────────────────────────────
Completed in X.Xs — 1/1 copied | 0 skipped | 0 failed | 500 rows written
```

---

## Step 3 — Verify rows in Iceberg

```bash
# Copy the verification script into the pod
cat > /tmp/verify_copy.py << 'PYEOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession

bao   = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf(app_name="verify-copy")).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

spark.sql("SELECT COUNT(*) AS total_rows FROM databricks.lakehouse_db.product").show()

spark.sql("""
    SELECT product_id, product_name, category, unit_price, snap_id, snap_timestamp
    FROM   databricks.lakehouse_db.product
    ORDER  BY product_id DESC
    LIMIT  5
""").show(truncate=False)

spark.sql("""
    SELECT source_db, source_schema, table_name,
           sf_extraction_ts, rows_copied, pipeline_run_ts
    FROM   databricks.lakehouse_db._pipeline_watermarks
""").show(truncate=False)

spark.stop()
PYEOF

kubectl cp /tmp/verify_copy.py prod/$MASTER:/tmp/verify_copy.py -c spark-master
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir python3 /tmp/verify_copy.py
```

✅ Expected:
```
+----------+
|total_rows|
+----------+
|       500|
+----------+

+----------+-------------------------+-----------+----------+----------+-------------------+
|product_id|product_name             |category   |unit_price|snap_id   |snap_timestamp     |
+----------+-------------------------+-----------+----------+----------+-------------------+
|500       |...                      |...        |...       |8589934592|2026-08-31 ...     |
...

+----------+-------------+-------+--------------------+-----------+-------------------+
|source_db |source_schema|table..|sf_extraction_ts    |rows_copied|pipeline_run_ts    |
+----------+-------------+-------+--------------------+-----------+-------------------+
|lakehouse |lakehouse_db |product|2026-08-31T...Z     |500        |2026-08-31 ...     |
+----------+-------------+-------+--------------------+-----------+-------------------+
```

---

## Step 4 — Insert new rows in Databricks

Open **[`https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor`](https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor)**, select **Serverless Starter Warehouse**, and run the cells below. **Run each cell every time you want a new batch of rows**, then go to Step 5 to trigger starpump and copy them to Iceberg.

---

### Cell 1 — Check current row count (run any time)

```sql
SELECT COUNT(*) AS total_rows, MAX(product_id) AS max_id
FROM lakehouse.lakehouse_db.product;
```

---

### Cell 2 — Insert the next batch of 5 rows

This cell always reads the current `MAX(product_id)` first so each run appends above whatever already exists. Copy it, run it once per cycle, and the `product_id` and `created_at` values advance automatically.

```sql
-- ── Dynamic insert: appends 5 rows above current MAX(product_id) ──────────
-- Run this cell once per test cycle. Each run produces 5 new rows with
-- created_at = NOW() so starpump picks them up as fresh data.
-- Uses INSERT ... SELECT so no DECLARE variable is needed.

INSERT INTO lakehouse.lakehouse_db.product
  (product_id, product_name, category, sub_category, brand, sku,
   unit_price, cost_price, stock_quantity, reorder_level,
   weight_kg, is_active, created_at, updated_at, snap_id, snap_timestamp)
SELECT
  m + 1, 'StarCore Tent Max',      'Sports',      'Outdoor',    'StarCore',
    CONCAT('SKU-', LPAD(CAST(m + 1 AS STRING), 5, '0'), '-STAR'),
    299.99, 120.00, 45,  10, 3.200, 1, NOW(), NOW(), NULL, NULL
FROM (SELECT COALESCE(MAX(product_id), 500) AS m FROM lakehouse.lakehouse_db.product)
UNION ALL
SELECT
  m + 2, 'BrightLife Blender Pro', 'Home',        'Kitchen',    'BrightLife',
    CONCAT('SKU-', LPAD(CAST(m + 2 AS STRING), 5, '0'), '-BRGT'),
    89.99,  35.00, 200, 30, 2.100, 1, NOW(), NOW(), NULL, NULL
FROM (SELECT COALESCE(MAX(product_id), 500) AS m FROM lakehouse.lakehouse_db.product)
UNION ALL
SELECT
  m + 3, 'AeroFit Yoga Mat Elite', 'Sports',      'Fitness',    'AeroFit',
    CONCAT('SKU-', LPAD(CAST(m + 3 AS STRING), 5, '0'), '-AERO'),
    49.99,  18.00, 320, 50, 1.050, 1, NOW(), NOW(), NULL, NULL
FROM (SELECT COALESCE(MAX(product_id), 500) AS m FROM lakehouse.lakehouse_db.product)
UNION ALL
SELECT
  m + 4, 'ZenFlow Watch Smart',    'Electronics', 'Wearables',  'ZenFlow',
    CONCAT('SKU-', LPAD(CAST(m + 4 AS STRING), 5, '0'), '-ZENF'),
    199.99, 80.00,  88, 20, 0.150, 1, NOW(), NOW(), NULL, NULL
FROM (SELECT COALESCE(MAX(product_id), 500) AS m FROM lakehouse.lakehouse_db.product)
UNION ALL
SELECT
  m + 5, 'UrbanEdge Jacket Sport', 'Clothing',    'Sportswear', 'UrbanEdge',
    CONCAT('SKU-', LPAD(CAST(m + 5 AS STRING), 5, '0'), '-URBN'),
    129.99, 52.00, 150, 25, 0.850, 1, NOW(), NOW(), NULL, NULL
FROM (SELECT COALESCE(MAX(product_id), 500) AS m FROM lakehouse.lakehouse_db.product);

-- Confirm the rows landed
SELECT product_id, product_name, category, unit_price, created_at
FROM   lakehouse.lakehouse_db.product
ORDER  BY product_id DESC
LIMIT  5;
```

✅ Expected: 5 new rows returned, `created_at` = current timestamp, `snap_id` and `snap_timestamp` are NULL (starpump injects those on copy).

---

### Cell 3 — Verify total row count after insert

```sql
SELECT COUNT(*) AS total_rows, MAX(product_id) AS max_id, MAX(created_at) AS latest_insert
FROM lakehouse.lakehouse_db.product;
```

---

After each Cell 2 run, go to **Step 5** to trigger starpump and copy the new rows to Iceberg.

---

## Step 5 — Trigger starpump to copy new rows to Iceberg

After inserting in Step 4, run this from your terminal to copy the new rows:

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=lakehouse SCHEMAS=lakehouse_db \
  starpump databricks
```

✅ Key lines to look for:
```
[product] batch offset=0 rows=<N> total=<N>
[product] DONE — <N> rows written (total incl. prior runs).
Completed in X.Xs — 1/1 copied | 0 skipped | 0 failed | <N> rows written
```

Then re-run Step 3 to confirm the row count in Iceberg matches Databricks.

---

## Step 6 — Partial copy with QUERY_FILTER

`QUERY_FILTER` lets you copy only the rows that match a predicate — without modifying any code.
It is set as an environment variable alongside the starpump command.

### Syntax

```
QUERY_FILTER="[table.]column<op>value"
```

- **Schema-level** — no table prefix: applies the predicate to **every** table in the schema.
- **Table-level** — `table.column`: applies only to that one table.
- Multiple predicates: comma-separated (commas inside parentheses are safe).

| Operator | Example |
|---|---|
| `=` | `is_active=1` |
| `!=` / `<>` | `category!='Books'` |
| `>` / `>=` / `<` / `<=` | `unit_price>=100` |
| `LIKE` | `product_name LIKE 'Star%'` |
| `NOT LIKE` | `product_name NOT LIKE '%test%'` |
| `IN (...)` | `category IN ('Electronics','Sports')` |
| `NOT IN (...)` | `brand NOT IN ('ZenFlow','AeroFit')` |
| `IS NULL` | `snap_id IS NULL` |
| `IS NOT NULL` | `snap_id IS NOT NULL` |

---

### Example 1 — Copy only active products

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=lakehouse SCHEMAS=lakehouse_db \
  QUERY_FILTER="is_active=1" \
  starpump databricks
```

✅ Expected log:
```
[product] QUERY_FILTER active — WHERE (is_active=1)
[product] batch offset=0 rows=<active_count> total=<active_count>
```

---

### Example 2 — Copy only Electronics products (table-level)

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=lakehouse SCHEMAS=lakehouse_db \
  QUERY_FILTER="product.category='Electronics'" \
  starpump databricks
```

---

### Example 3 — Copy high-value active products (combined predicates)

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=lakehouse SCHEMAS=lakehouse_db \
  QUERY_FILTER="product.unit_price>=100,is_active=1" \
  starpump databricks
```

✅ Expected log:
```
[query-filter] Parsed QUERY_FILTER → {'product': 'unit_price>=100', '<schema-level>': 'is_active=1'}
[product] QUERY_FILTER active — WHERE (is_active=1) AND (unit_price>=100)
```

---

### Example 4 — Copy rows inserted after a specific timestamp

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=lakehouse SCHEMAS=lakehouse_db \
  QUERY_FILTER="product.created_at>='2026-09-01 00:00:00'" \
  starpump databricks
```

---

### Example 5 — Copy rows where snap_id is NULL (not yet copied)

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=lakehouse SCHEMAS=lakehouse_db \
  QUERY_FILTER="product.snap_id IS NULL" \
  starpump databricks
```

---

### Verify the filtered rows landed in Iceberg

After any filtered copy, adapt the verification query to match your filter:

```bash
cat > /tmp/verify_filter.py << 'PYEOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession

bao   = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf(app_name="verify-filter")).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Adjust WHERE clause to match your QUERY_FILTER
spark.sql("""
    SELECT COUNT(*) AS filtered_rows
    FROM   databricks.lakehouse_db.product
    WHERE  is_active = 1
""").show()

spark.sql("""
    SELECT product_id, product_name, category, unit_price, snap_id, snap_timestamp
    FROM   databricks.lakehouse_db.product
    ORDER  BY snap_timestamp DESC
    LIMIT  5
""").show(truncate=False)

spark.stop()
PYEOF

kubectl cp /tmp/verify_filter.py prod/$MASTER:/tmp/verify_filter.py -c spark-master
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir python3 /tmp/verify_filter.py
```

---

## Troubleshooting

### `ClassNotFoundException: com.databricks.client.jdbc.Driver`
The Databricks JDBC JAR is not in the image.
```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  ls -lh /opt/spark/jars/databricks-jdbc-*.jar
```
If missing: stage the JAR to `docker/spark-gluten-velox/jars/`, rebuild the image, and restart pods.

---

### `java.sql.SQLException: … Invalid token`
The PAT/OAuth token in OpenBao is expired.
```bash
curl -s -X POST \
  -H "X-Vault-Token: $TOKEN" \
  -H "Content-Type: application/json" \
  http://192.168.1.50:30820/v1/secret/data/platform/databricks \
  -d '{"data":{"token":"<new-token>","host":"dbc-11a1dbc5-061a.cloud.databricks.com","http_path":"/sql/1.0/warehouses/942026cf5e55f3c3","catalog":"lakehouse","schema":"lakehouse_db"}}'
```

---

### `RuntimeError: Cannot authenticate to OpenBao`
No K8s SA JWT and no `TOKEN` env var set. Always pass `TOKEN=$TOKEN` in every `kubectl exec` command (Step 1).

---

### `ValueError: No Spark external catalog registered for '...'`
`ICEBERG_CATALOG` is not wired in `BaoSparkInit.spark_conf()`. Correct `ICEBERG_CATALOG` to `databricks` or add the missing `spark.sql.catalog.<name>.*` block to [`bao_spark_init.py`](../../docker/spark-gluten-velox/scripts/bao_spark_init.py).

---

### Job hangs at `[Stage N:>` for > 2 minutes
Another Spark app is holding all cores. Check `http://192.168.1.50:30707`. If a zombie app is shown:
```bash
kubectl exec -n prod $MASTER -c spark-master -- spark-app-cleanup
```

---

## Key files

| File | Purpose |
|---|---|
| [`docker/spark-gluten-velox/scripts/starpump.py`](../../docker/spark-gluten-velox/scripts/starpump.py) | starpump entry point |
| [`docker/spark-gluten-velox/scripts/bao_spark_init.py`](../../docker/spark-gluten-velox/scripts/bao_spark_init.py) | `databricks_jdbc_options()`, `databricks_creds()`, `spark_conf()` |
| [`docker/spark-gluten-velox/scripts/spark_iceberg_utils.py`](../../docker/spark-gluten-velox/scripts/spark_iceberg_utils.py) | `IcebergTableBuilder` — auto-creates table, injects `snap_id`/`snap_timestamp` |
| [`docker/spark-gluten-velox/scripts/databricks_product_insert_loop.py`](../../docker/spark-gluten-velox/scripts/databricks_product_insert_loop.py) | Dynamic insert loop: adds timestamped rows then triggers starpump |
