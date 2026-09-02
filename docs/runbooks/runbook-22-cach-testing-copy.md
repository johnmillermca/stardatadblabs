# Runbook 22 — starpump: cach_testing Copy (PostgreSQL · Oracle · MongoDB → Iceberg)

| Field | Value |
|---|---|
| **Runbook ID** | RB-22 |
| **Service** | k8s-platform / starpump / postgres · oracle · mongodb |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-09-02 |

---

## Overview

Copy tables/collections from the `cach_testing` database in **PostgreSQL**, **Oracle**, and **MongoDB**
into Iceberg on Polaris/S3 using `starpump`.

All features — `INCLUDE_TABLES`, `EXCLUDE_TABLES`, `MAX_TABLE_SIZE_GB`, `QUERY_FILTER`,
`DRY_RUN`, `BATCH_SIZE`, `--threads`, resume-on-crash — work identically for every connector.

| Source | Iceberg catalog | Polaris warehouse | S3 prefix | Default schema |
|---|---|---|---|---|
| `postgres` | `postgres` | `pg_lakehouse` | `iceberg/warehouse` | `public` |
| `oracle` | `oracle` | `ora_lakehouse` | `iceberg/warehouse` | `cach_testing` |
| `mongodb` | `mongodb` | `mgo_lakehouse` | `iceberg/warehouse` | `cach_testing` |

---

## Step 0 — Store credentials in OpenBao (once per database)

Run from any node with `kubectl` access.

```bash
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)
```

### PostgreSQL

```bash
curl -s -X POST \
  -H "X-Vault-Token: $TOKEN" -H "Content-Type: application/json" \
  http://192.168.1.50:30820/v1/secret/data/platform/postgres \
  -d '{
    "data": {
      "host":     "postgresql.prod.svc.cluster.local",
      "port":     "5432",
      "database": "cach_testing",
      "schema":   "public",
      "user":     "spark_user",
      "password": "<pg-password>"
    }
  }'
```

### Oracle

```bash
curl -s -X POST \
  -H "X-Vault-Token: $TOKEN" -H "Content-Type: application/json" \
  http://192.168.1.50:30820/v1/secret/data/platform/oracle \
  -d '{
    "data": {
      "host":     "oracle.prod.svc.cluster.local",
      "port":     "1521",
      "sid":      "ORCL",
      "schema":   "CACH_TESTING",
      "user":     "spark_user",
      "password": "<oracle-password>",
      "jdbc_url": "jdbc:oracle:thin:@oracle.prod.svc.cluster.local:1521:ORCL"
    }
  }'
```

### MongoDB

```bash
curl -s -X POST \
  -H "X-Vault-Token: $TOKEN" -H "Content-Type: application/json" \
  http://192.168.1.50:30820/v1/secret/data/platform/mongodb \
  -d '{
    "data": {
      "host":        "mongodb.prod.svc.cluster.local",
      "port":        "27017",
      "database":    "cach_testing",
      "user":        "spark_user",
      "password":    "<mongo-password>",
      "auth_source": "admin"
    }
  }'
```

✅ Verify each secret was written:
```bash
curl -s -H "X-Vault-Token: $TOKEN" \
  http://192.168.1.50:30820/v1/secret/data/platform/postgres \
  | python3 -m json.tool | grep '"host"\|"database"\|"user"'
```

---

## Step 1 — Create Polaris warehouses (once)

Each source gets its own Polaris warehouse and principal role so Iceberg namespaces are isolated.

In the Polaris UI at **`http://192.168.1.50:8181`** (or via the REST API), create:

| Warehouse | Default base location |
|---|---|
| `pg_lakehouse` | `s3://stardata-cach/iceberg/warehouse/` |
| `ora_lakehouse` | `s3://stardata-cach/iceberg/warehouse/` |
| `mgo_lakehouse` | `s3://stardata-cach/iceberg/warehouse/` |

Grant the `spark_svc_id` principal `SERVICE_ADMIN` on each warehouse.

---

## Step 2 — Common setup

Run once per terminal session:

```bash
MASTER=$(kubectl get pod -n prod -l component=master \
  -o jsonpath='{.items[0].metadata.name}')
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)
echo "Master: $MASTER   Token: ${TOKEN:0:10}..."
```

---

## Step 3 — PostgreSQL: full copy

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cach_testing SCHEMAS=public \
  MAX_TABLE_SIZE_GB=0 \
  starpump postgres
```

✅ Expected:
```
=== starpump postgres | run_id=... user=dave db=cach_testing schema=public catalog=postgres threads=8 ===
[catalog-check] 'postgres' is registered (svc_id=...). Proceeding.
Discovered N tables in cach_testing.public: [...]
[<table>] START: 0.0 GB | discovering schema …
[<table>] extraction_ts=2026-09-02T...Z (CDC sync point)
[<table>] batch offset=0 rows=<N> total=<N>
[<table>] DONE — <N> rows written (total incl. prior runs).
Completed in X.Xs — N/N copied | 0 skipped | 0 failed | <N> rows written
```

---

## Step 4 — Oracle: full copy

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cach_testing SCHEMAS=CACH_TESTING \
  MAX_TABLE_SIZE_GB=0 \
  starpump oracle
```

---

## Step 5 — MongoDB: full copy

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cach_testing SCHEMAS=cach_testing \
  MAX_TABLE_SIZE_GB=0 \
  starpump mongodb
```

---

## Step 6 — Verify rows in Iceberg

```bash
cat > /tmp/verify_cach.py << 'PYEOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession

bao   = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf(app_name="verify-cach")).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

for catalog, schema in [("postgres","public"), ("oracle","cach_testing"), ("mongodb","cach_testing")]:
    print(f"\n=== {catalog}.{schema} ===")
    try:
        spark.sql(f"SHOW TABLES IN {catalog}.{schema}").show(20)
    except Exception as e:
        print(f"  (no tables yet or catalog not reachable: {e})")

spark.stop()
PYEOF

kubectl cp /tmp/verify_cach.py prod/$MASTER:/tmp/verify_cach.py -c spark-master
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir python3 /tmp/verify_cach.py
```

---

## Step 7 — Test all starpump features

Run each block below, substituting `<source>` with `postgres`, `oracle`, or `mongodb` and
`<table>` with any small table in `cach_testing`.

---

### 7-A — Dry-run (DDL only, no data copy)

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cach_testing SCHEMAS=public \
  DRY_RUN=1 \
  starpump postgres
```

✅ Expected: `[<table>] DRY_RUN — skipping data copy.`

---

### 7-B — Copy a single table (`INCLUDE_TABLES`)

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cach_testing SCHEMAS=public \
  INCLUDE_TABLES=<table> MAX_TABLE_SIZE_GB=0 \
  starpump postgres
```

---

### 7-C — Exclude a table (`EXCLUDE_TABLES`)

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cach_testing SCHEMAS=public \
  EXCLUDE_TABLES=<table> MAX_TABLE_SIZE_GB=0 \
  starpump postgres
```

---

### 7-D — Row filter: copy only specific rows (`QUERY_FILTER`)

**Schema-level (all tables):**
```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cach_testing SCHEMAS=public \
  INCLUDE_TABLES=<table> MAX_TABLE_SIZE_GB=0 \
  QUERY_FILTER="<column>=<value>" \
  starpump postgres
```

**Table-level:**
```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cach_testing SCHEMAS=public \
  MAX_TABLE_SIZE_GB=0 \
  QUERY_FILTER="<table>.<column>>=<value>" \
  starpump postgres
```

**LIKE pattern:**
```bash
QUERY_FILTER="<table>.<column> LIKE 'prefix%'"
```

**IN list:**
```bash
QUERY_FILTER="<table>.<column> IN ('val1','val2')"
```

**IS NULL:**
```bash
QUERY_FILTER="<table>.<column> IS NULL"
```

**Date range:**
```bash
QUERY_FILTER="<table>.created_at>='2026-01-01'"
```

✅ Expected log when filter is active:
```
[<table>] QUERY_FILTER active — WHERE (<column>=<value>)
[<table>] batch offset=0 rows=<filtered_count> total=<filtered_count>
```

---

### 7-E — Parallel threads (`--threads`)

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cach_testing SCHEMAS=public \
  MAX_TABLE_SIZE_GB=0 \
  starpump postgres --threads 4
```

---

### 7-F — Custom batch size (`BATCH_SIZE`)

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cach_testing SCHEMAS=public \
  INCLUDE_TABLES=<table> MAX_TABLE_SIZE_GB=0 \
  BATCH_SIZE=500 \
  starpump postgres
```

✅ Expected: multiple `batch offset=0 rows=500`, `batch offset=500 rows=500`, … lines.

---

### 7-G — Oracle: single table copy with QUERY_FILTER

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cach_testing SCHEMAS=CACH_TESTING \
  INCLUDE_TABLES=<table> MAX_TABLE_SIZE_GB=0 \
  QUERY_FILTER="<table>.<column>='<value>'" \
  starpump oracle
```

---

### 7-H — MongoDB: single collection copy

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cach_testing SCHEMAS=cach_testing \
  INCLUDE_TABLES=<collection> MAX_TABLE_SIZE_GB=0 \
  starpump mongodb
```

> **MongoDB note:** `QUERY_FILTER` logs the predicate but uses the full collection scan.
> For field-level filtering use `spark.mongodb.read.aggregation.pipeline` directly.

---

### 7-I — Resume after crash (PostgreSQL / Oracle)

```bash
# Terminal 1: start copy with tiny batch size
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cach_testing SCHEMAS=public \
  INCLUDE_TABLES=<table> MAX_TABLE_SIZE_GB=0 \
  BATCH_SIZE=100 \
  starpump postgres &

# Terminal 2: kill after first batch
sleep 8
PID=$(kubectl exec -n prod $MASTER -c spark-master -- pgrep -f starpump.py)
kubectl exec -n prod $MASTER -c spark-master -- kill -9 $PID

# Re-run — should log RESUME: N rows already in Iceberg
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cach_testing SCHEMAS=public \
  INCLUDE_TABLES=<table> MAX_TABLE_SIZE_GB=0 \
  BATCH_SIZE=100 \
  starpump postgres
```

✅ Expected on re-run:
```
[<table>] RESUME: 100 rows already in Iceberg — reusing extraction_ts=... starting at offset=100.
```

---

## Step 8 — Query copied data from Iceberg

```bash
cat > /tmp/query_cach.py << 'PYEOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession

bao   = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf(app_name="query-cach")).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

TABLE   = "your_table_name"   # ← replace
CATALOG = "postgres"           # ← replace: postgres | oracle | mongodb
SCHEMA  = "public"             # ← replace: public | cach_testing

fqn = f"{CATALOG}.{SCHEMA}.{TABLE}"

print(f"=== Row count: {fqn} ===")
spark.sql(f"SELECT COUNT(*) AS total_rows FROM {fqn}").show()

print("=== Sample (last 5 by snap_timestamp) ===")
spark.sql(f"""
    SELECT * FROM {fqn}
    ORDER BY snap_timestamp DESC
    LIMIT 5
""").show(truncate=False)

print("=== Watermark ===")
spark.sql(f"""
    SELECT source_db, source_schema, table_name,
           sf_extraction_ts, rows_copied, pipeline_run_ts
    FROM   {CATALOG}.{SCHEMA}._pipeline_watermarks
""").show(truncate=False)

spark.stop()
PYEOF

kubectl cp /tmp/query_cach.py prod/$MASTER:/tmp/query_cach.py -c spark-master
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir python3 /tmp/query_cach.py
```

---

## Troubleshooting

### `RuntimeError: Cannot authenticate to OpenBao`
Set `TOKEN=$TOKEN` in every `kubectl exec` command (Step 2).

---

### `ValueError: No Spark external catalog registered for 'postgres'` / `oracle` / `mongodb`
The new catalog stanza is not in `spark_conf()`. Confirm the running pod image is `3.5.1-4` or later:
```bash
kubectl get pod -n prod -l component=master \
  -o jsonpath='{.items[0].spec.containers[?(@.name=="spark-master")].image}'
```
If still on an older tag, trigger an ArgoCD sync or run `kubectl rollout restart deployment spark-master spark-worker -n prod`.

---

### `ClassNotFoundException: org.postgresql.Driver`
PostgreSQL JAR missing from image. Should be baked in since `3.5.1`. Verify:
```bash
kubectl exec -n prod $MASTER -c spark-master -- ls /opt/spark/jars/postgresql*.jar
```

---

### `ClassNotFoundException: oracle.jdbc.OracleDriver`
Oracle JAR missing — requires image `3.5.1-4+`:
```bash
kubectl exec -n prod $MASTER -c spark-master -- ls /opt/spark/jars/ojdbc11*.jar
```

---

### `ClassNotFoundException: com.mongodb.spark.sql.connector.MongoTableProvider`
MongoDB connector JAR missing — requires image `3.5.1-4+`:
```bash
kubectl exec -n prod $MASTER -c spark-master -- ls /opt/spark/jars/mongo-spark-connector*.jar
```

---

### Oracle: `ORA-00942: table or view does not exist`
The `SCHEMAS` env var must match the Oracle **owner/schema** name exactly (case-insensitive, stored uppercase internally). Use `SCHEMAS=CACH_TESTING`.

---

### MongoDB: all rows copied regardless of `QUERY_FILTER`
MongoDB QUERY_FILTER is logged but not pushed to the aggregation pipeline — it uses a full collection scan. For field filtering, pass a MongoDB aggregation pipeline via the Spark session instead:
```python
spark.read.format("mongodb") \
  .option("spark.mongodb.read.connection.uri", uri) \
  .option("collection", "your_collection") \
  .option("spark.mongodb.read.aggregation.pipeline",
          '[{"$match": {"field": "value"}}]') \
  .load()
```

---

## Key files

| File | Purpose |
|---|---|
| [`docker/spark-gluten-velox/scripts/starpump.py`](../../docker/spark-gluten-velox/scripts/starpump.py) | `_pg_*`, `_ora_*`, `_mgo_*` connector functions + registry entries |
| [`docker/spark-gluten-velox/scripts/bao_spark_init.py`](../../docker/spark-gluten-velox/scripts/bao_spark_init.py) | `postgres_creds()`, `postgres_jdbc_options()`, `oracle_jdbc_options()`, `mongodb_creds()`, `mongodb_options()` + catalog stanzas |
| [`docker/spark-gluten-velox/Dockerfile`](../../docker/spark-gluten-velox/Dockerfile) | Adds `ojdbc11-23.4.0.24.05.jar`, `mongo-spark-connector_2.12-10.4.0-all.jar`, `pymongo==4.9.2` |
