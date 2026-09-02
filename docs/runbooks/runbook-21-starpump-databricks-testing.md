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

## Step 4 — Dynamic insert loop + starpump

This step uses [`databricks_product_insert_loop.py`](../../docker/spark-gluten-velox/scripts/databricks_product_insert_loop.py) to continuously insert new timestamped product rows into Databricks and immediately trigger `starpump databricks` to copy them to Iceberg.

**Install the connector** (once, on the machine running the script):
```bash
pip install databricks-sql-connector
```

**Run the loop** (inserts a batch every `INTERVAL_SECONDS`, runs forever until Ctrl-C):
```bash
TOKEN=$TOKEN \
INTERVAL_SECONDS=30 \
BATCH_SIZE=5 \
python3 docker/spark-gluten-velox/scripts/databricks_product_insert_loop.py
```

| Env var | Default | Description |
|---|---|---|
| `TOKEN` | *(required)* | OpenBao root/bootstrap token |
| `INTERVAL_SECONDS` | `30` | Seconds between insert → starpump cycles |
| `BATCH_SIZE` | `5` | New rows to insert per cycle |
| `STARPUMP_NAMESPACE` | `prod` | Kubernetes namespace |

Each cycle:
1. Inserts `BATCH_SIZE` rows with `created_at = NOW()` and unique `product_id` values above the current max
2. Runs `kubectl exec … starpump databricks` against the live cluster
3. Prints a timestamped summary of rows inserted and the starpump exit code

✅ Expected cycle output:
```
[2026-08-31T14:00:00] Cycle 1 — inserted 5 rows (product_id 501–505, created_at=2026-08-31T14:00:00)
[2026-08-31T14:00:00] Running starpump databricks …
[2026-08-31T14:00:08]   [product] batch offset=0 rows=505 total=505
[2026-08-31T14:00:08]   [product] DONE — 505 rows written (total incl. prior runs).
[2026-08-31T14:00:08] ✅ Cycle 1 complete — starpump exit=0. Next cycle in 30s.

[2026-08-31T14:00:38] Cycle 2 — inserted 5 rows (product_id 506–510, created_at=2026-08-31T14:00:38)
...
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
