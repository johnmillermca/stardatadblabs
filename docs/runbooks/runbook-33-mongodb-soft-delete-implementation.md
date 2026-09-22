# Runbook 33 — MongoDB → Kafka → Iceberg: Soft Delete Implementation & Testing

**Status:** Operational  
**Namespace:** `prod`  
**Source DB:** MongoDB — database `cache_testing`  
**CDC connector:** `mongodb-cache-testing-cdc`  
**Iceberg catalog:** `mongodb` (Polaris REST)  
**Target table:** `mongodb.cache_testing.customers_sd`  
**Estimated duration:** 20–30 minutes

> **Companion runbooks:**
> - [Runbook 32 — MongoDB E2E full suite](runbook-32-mongodb-kafka-iceberg-e2e-testing.md)
> - [Runbook 29 — CDC Architecture](runbook-29-cdc-debezium-kafka-iceberg-architecture.md)

---

## Table of Contents

1. [What Soft Delete Does](#1-what-soft-delete-does)
2. [Step 1 — Create the Iceberg Table](#2-step-1--create-the-iceberg-table)
3. [Step 2 — Switch to Soft-Delete Deployment](#3-step-2--switch-to-soft-delete-deployment)
4. [Step 3 — Test INSERT](#4-step-3--test-insert)
5. [Step 4 — Test UPDATE](#5-step-4--test-update)
6. [Step 5 — Test DELETE (soft)](#6-step-5--test-delete-soft)
7. [Step 6 — Restore Standard Deployment](#7-step-6--restore-standard-deployment)
8. [Expected Results Summary](#8-expected-results-summary)

---

## 1. What Soft Delete Does

| Event | Standard mode | Soft-delete mode |
|-------|--------------|-----------------|
| INSERT | MERGE → new row | MERGE → new row; `is_deleted=false`, `deleted_at=NULL` |
| UPDATE | MERGE → overwrite | MERGE → overwrite; `is_deleted` and `deleted_at` preserved |
| DELETE | MERGE → physical row removal | MERGE → set `is_deleted=true`, `deleted_at=<now>` — **row never removed** |

The pipeline uses the `kafka-to-iceberg-mongodb-soft-delete` Deployment (`WRITE_MODE=soft_delete`).  
The write handler is [`_apply_soft_delete()`](../../manifests/cdc-batch-pipeline/kafka-to-iceberg-streaming.yaml)
which MERGEs on `id` and updates `is_deleted` / `deleted_at` on DELETE events.

---

## 2. Step 1 — Create the Iceberg Table

Run once in JupyterHub. Navigate to `http://192.168.1.50:30888`, log in as `admin`, open a new Python 3 notebook.

### Cell 1 — Fetch token & credentials

```bash
# Terminal — get root token first
kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d && echo
```

```python
import urllib.request, json, os

OPENBAO_ADDR  = "http://openbao.prod.svc.cluster.local:8200"
OPENBAO_TOKEN = "s.xxxxxxxxxxxxxxxxxxxxxxxx"   # ← paste token here

def _bao(path):
    req = urllib.request.Request(
        f"{OPENBAO_ADDR}/v1/{path}",
        headers={"X-Vault-Token": OPENBAO_TOKEN}
    )
    return json.loads(urllib.request.urlopen(req, timeout=10).read())["data"]["data"]

pol = _bao("secret/data/platform/polaris")
s3  = _bao("secret/data/platform/s3")
print("✅ Credentials loaded")
```

### Cell 2 — Start Spark session with `mongodb` catalog

```python
from pyspark.sql import SparkSession

_s = SparkSession.getActiveSession()
if _s:
    _s.stop()
    print("Stopped stale session")

DRIVER_IP   = os.environ.get("SPARK_LOCAL_IP", __import__("socket").gethostbyname(__import__("socket").gethostname()))
POLARIS_URI = "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog"

spark = (
    SparkSession.builder
    .master("spark://192.168.1.50:30777")
    .appName("mongodb-sd-setup")
    .config("spark.cores.max",           "1")
    .config("spark.executor.instances",  "1")
    .config("spark.executor.cores",      "1")
    .config("spark.executor.memory",     "2g")
    .config("spark.driver.host",         DRIVER_IP)
    .config("spark.driver.bindAddress",  DRIVER_IP)
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.mongodb",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.mongodb.type",             "rest")
    .config("spark.sql.catalog.mongodb.uri",              POLARIS_URI)
    .config("spark.sql.catalog.mongodb.oauth2-server-uri",
            f"{POLARIS_URI}/v1/oauth/tokens")
    .config("spark.sql.catalog.mongodb.credential",
            f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}")
    .config("spark.sql.catalog.mongodb.scope",            "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.mongodb.warehouse",        "IcebergCatalog")
    .config("spark.sql.catalog.mongodb.rest.auth.type",   "oauth2")
    .config("spark.sql.catalog.mongodb.s3.access-key-id",     s3["access_key"])
    .config("spark.sql.catalog.mongodb.s3.secret-access-key", s3["secret_key"])
    .config("spark.sql.catalog.mongodb.s3.endpoint",          s3["endpoint"])
    .config("spark.sql.catalog.mongodb.s3.path-style-access", "true")
    .config("spark.sql.catalog.mongodb.client.region",        s3.get("region","us-east-1"))
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.access.key",        s3["access_key"])
    .config("spark.hadoop.fs.s3a.secret.key",        s3["secret_key"])
    .config("spark.hadoop.fs.s3a.endpoint",          s3["endpoint"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("✅ Spark", spark.version, "connected —", DRIVER_IP)
```

✅ Expected: `✅ Spark 3.5.1 connected — 10.244.x.x`

### Cell 3 — Create `mongodb.cache_testing.customers_sd`

```python
spark.sql("""
    CREATE TABLE IF NOT EXISTS mongodb.cache_testing.customers_sd (
        id             BIGINT        NOT NULL,
        name           STRING,
        email          STRING,
        phone          STRING,
        address        STRING,
        city           STRING,
        country        STRING,
        created_at     TIMESTAMP,
        updated_at     TIMESTAMP,
        is_deleted     BOOLEAN,
        deleted_at     TIMESTAMP,
        snap_id        BIGINT,
        snap_timestamp TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (hours(snap_timestamp), bucket(16, id))
    LOCATION 's3://xdatatoiceberg1/iceberg/mgo_lakehouse/cache_testing/customers_sd'
    TBLPROPERTIES (
        'format-version'                  = '2',
        'write.format.default'            = 'parquet',
        'write.parquet.compression-codec' = 'snappy',
        'write.target-file-size-bytes'    = '134217728',
        'pipeline.write-mode'             = 'soft_delete',
        'pipeline.source'                 = 'mongodb',
        'platform.snap-columns'           = 'snap_id,snap_timestamp',
        'platform.created-by'             = 'dave'
    )
""")
print("✅ mongodb.cache_testing.customers_sd ready")
```

✅ Expected: `✅ mongodb.cache_testing.customers_sd ready`

Verify schema:
```python
spark.sql("DESCRIBE TABLE mongodb.cache_testing.customers_sd").show(20, truncate=False)
```

✅ Expected: 13 columns including `is_deleted BOOLEAN` and `deleted_at TIMESTAMP`.

```python
spark.stop()
print("✅ Session stopped")
```

---

## 3. Step 2 — Switch to Soft-Delete Deployment

Only one write-mode Deployment should be active at a time. Scale standard down, soft-delete up:

```bash
kubectl scale deployment kafka-to-iceberg-mongodb-standard    -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-mongodb-soft-delete -n prod --replicas=1
kubectl rollout status deployment/kafka-to-iceberg-mongodb-soft-delete -n prod
```

✅ Expected: `deployment "kafka-to-iceberg-mongodb-soft-delete" successfully rolled out`

Confirm both states:
```bash
kubectl get deployment -n prod \
  -l pipeline.source=mongodb \
  -o custom-columns='NAME:.metadata.name,READY:.status.readyReplicas,DESIRED:.spec.replicas'
```

✅ Expected:
```
NAME                                      READY   DESIRED
kafka-to-iceberg-mongodb-history-tracking <none>  0
kafka-to-iceberg-mongodb-soft-delete      1       1
kafka-to-iceberg-mongodb-standard         <none>  0
```

Confirm soft-delete pod is consuming:
```bash
kubectl logs -n prod \
  $(kubectl get pod -n prod \
    -l app=kafka-to-iceberg,pipeline.source=mongodb,pipeline.write-mode=soft-delete \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=30s 2>&1 | grep -E "WRITE_MODE|soft_delete|batch=|ERROR" | head -20
```

✅ Expected: `WRITE_MODE=soft_delete` in startup log; no `ERROR`.

---

## 4. Step 3 — Test INSERT

### 4a — Insert the test document into MongoDB

```bash
MONGO_POD=$(kubectl get pod -n prod -l app=mongodb -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it -n prod $MONGO_POD -- \
  mongosh "mongodb://root:oEtCgw554IP3ua0SrJCTsWYM@localhost:27017/cache_testing?authSource=admin" \
  --quiet
```

```javascript
use cache_testing;

db.customers.insertOne({
  _id:        ObjectId("000000000000000000990001"),
  id:         990001,
  name:       "SD Test Customer",
  email:      "sd_test@example.com",
  phone:      "555-9001",
  address:    "1 Soft Delete St",
  city:       "Sydney",
  country:    "AU",
  created_at: new Date(),
  updated_at: new Date()
});
```

✅ Expected:
```javascript
{ acknowledged: true, insertedId: ObjectId('000000000000000000990001') }
```

### 4b — Wait for pipeline propagation

```bash
sleep 10
kubectl logs -n prod \
  $(kubectl get pod -n prod \
    -l app=kafka-to-iceberg,pipeline.source=mongodb,pipeline.write-mode=soft-delete \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=20s 2>&1 | grep -E "batch=|upsert|soft-delete|ERROR"
```

✅ Expected: `[mongodb/customers_sd][soft_delete] batch=N upsert rows=1`

### 4c — Verify INSERT in Iceberg (new JupyterHub session — Cells 1 & 2 above, then:)

```python
print("=== INSERT verify ===")
spark.sql("""
    SELECT id, name, email, city, country,
           is_deleted, deleted_at, snap_id, snap_timestamp
    FROM   mongodb.cache_testing.customers_sd
    WHERE  id = 990001
""").show(truncate=False)
```

✅ Expected:
```
+------+----------------+--------------------+------+-------+----------+----------+-------+--------------+
|id    |name            |email               |city  |country|is_deleted|deleted_at|snap_id|snap_timestamp|
+------+----------------+--------------------+------+-------+----------+----------+-------+--------------+
|990001|SD Test Customer|sd_test@example.com |Sydney|AU     |false     |null      |...    |...           |
+------+----------------+--------------------+------+-------+----------+----------+-------+--------------+
```

1 row. `is_deleted = false`. `deleted_at = null`. `snap_id` non-null.

---

## 5. Step 4 — Test UPDATE

### 5a — Update the document in MongoDB (mongosh session)

```javascript
db.customers.updateOne(
  { id: 990001 },
  { $set: {
      email:      "sd_test_updated@example.com",
      city:       "Melbourne",
      updated_at: new Date()
  }}
);
```

✅ Expected: `{ acknowledged: true, matchedCount: 1, modifiedCount: 1 }`

### 5b — Wait and verify UPDATE in Iceberg

```bash
sleep 10
kubectl logs -n prod \
  $(kubectl get pod -n prod \
    -l app=kafka-to-iceberg,pipeline.source=mongodb,pipeline.write-mode=soft-delete \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=20s 2>&1 | grep -E "batch=|upsert|soft-delete|ERROR"
```

✅ Expected: `[mongodb/customers_sd][soft_delete] batch=N upsert rows=1`

```python
print("=== UPDATE verify ===")
spark.sql("""
    SELECT id, email, city, is_deleted, deleted_at, snap_id, snap_timestamp
    FROM   mongodb.cache_testing.customers_sd
    WHERE  id = 990001
""").show(truncate=False)
```

✅ Expected: 1 row. `email = 'sd_test_updated@example.com'`. `city = 'Melbourne'`.  
`is_deleted = false`. `deleted_at = null`. `snap_id` unchanged from INSERT.

---

## 6. Step 5 — Test DELETE (soft)

### 6a — Delete the document from MongoDB (mongosh session)

```javascript
db.customers.deleteOne({ id: 990001 });
```

✅ Expected: `{ acknowledged: true, deletedCount: 1 }`

### 6b — Wait and verify soft delete in Iceberg

```bash
sleep 10
kubectl logs -n prod \
  $(kubectl get pod -n prod \
    -l app=kafka-to-iceberg,pipeline.source=mongodb,pipeline.write-mode=soft-delete \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=20s 2>&1 | grep -E "batch=|upsert|soft-delete|ERROR"
```

✅ Expected: `[mongodb/customers_sd][soft_delete] batch=N soft-delete rows=1`

```python
print("=== DELETE (soft) verify ===")
spark.sql("""
    SELECT id, email, city, is_deleted, deleted_at, snap_id
    FROM   mongodb.cache_testing.customers_sd
    WHERE  id = 990001
""").show(truncate=False)
```

✅ Expected: **Row is still present**. `is_deleted = true`. `deleted_at` is a non-null TIMESTAMP
within the last 30 seconds.

### 6c — Confirm physical row count did NOT decrease

```python
total = spark.sql(
    "SELECT COUNT(*) FROM mongodb.cache_testing.customers_sd WHERE id = 990001"
).collect()[0][0]
print(f"row_count = {total}  (expected 1 — row retained, never physically removed)")
```

✅ Expected: `row_count = 1`

### 6d — Query all soft-deleted rows

```python
spark.sql("""
    SELECT id, email, deleted_at
    FROM   mongodb.cache_testing.customers_sd
    WHERE  is_deleted = true
    ORDER  BY deleted_at DESC
    LIMIT  20
""").show(truncate=False)
```

✅ Expected: `id = 990001` appears in results.

```python
spark.stop()
print("✅ Session stopped — cluster core released")
```

---

## 7. Step 6 — Restore Standard Deployment

After testing, return the standard Deployment to active and scale soft-delete back to 0:

```bash
kubectl scale deployment kafka-to-iceberg-mongodb-soft-delete -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-mongodb-standard    -n prod --replicas=1
kubectl rollout status deployment/kafka-to-iceberg-mongodb-standard -n prod
```

Confirm:
```bash
kubectl get deployment -n prod \
  -l pipeline.source=mongodb \
  -o custom-columns='NAME:.metadata.name,READY:.status.readyReplicas,DESIRED:.spec.replicas'
```

✅ Expected: `kafka-to-iceberg-mongodb-standard` shows `1/1`; soft-delete shows `<none>/0`.

---

## 8. Expected Results Summary

| Step | Operation | Kafka log | Iceberg result |
|------|-----------|-----------|----------------|
| 3 — INSERT | `insertOne id=990001` | `batch=N upsert rows=1` | 1 row; `is_deleted=false`; `deleted_at=null` |
| 4 — UPDATE | `updateOne id=990001` | `batch=N upsert rows=1` | `email` and `city` updated; `is_deleted` still `false` |
| 5 — DELETE | `deleteOne id=990001` | `batch=N soft-delete rows=1` | Row **retained**; `is_deleted=true`; `deleted_at` non-null |

### Table schema — `mongodb.cache_testing.customers_sd`

| Column | Type | Notes |
|--------|------|-------|
| `id` | `BIGINT NOT NULL` | Application PK — MERGE key |
| `name` | `STRING` | |
| `email` | `STRING` | |
| `phone` | `STRING` | |
| `address` | `STRING` | |
| `city` | `STRING` | |
| `country` | `STRING` | |
| `created_at` | `TIMESTAMP` | |
| `updated_at` | `TIMESTAMP` | |
| `is_deleted` | `BOOLEAN` | `false` on INSERT/UPDATE; `true` on DELETE |
| `deleted_at` | `TIMESTAMP` | `NULL` on INSERT/UPDATE; set to `current_timestamp()` on DELETE |
| `snap_id` | `BIGINT` | Unique row id per batch; injected by pipeline |
| `snap_timestamp` | `TIMESTAMP` | Write-time wall clock; hourly partition key |

**Partitioning:** `hours(snap_timestamp)` + `bucket(16, id)`  
**Location:** `s3://xdatatoiceberg1/iceberg/mgo_lakehouse/cache_testing/customers_sd`  
**Test document id range:** `990001–990099` (reserved for this runbook)
