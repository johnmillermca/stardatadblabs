# Runbook 32 — MongoDB → Kafka → Iceberg End-to-End Test Runbook

**Status:** Operational  
**Namespace:** `prod`  
**Source DB:** MongoDB 7 — database `cache_testing`  
**CDC connector:** `mongodb-cache-testing-cdc` (Debezium MongoDB connector — change streams)  
**Iceberg catalog:** `mongodb` (Polaris REST)  
**Estimated duration:** 30–60 minutes (Sections 1–3 full suite)

> **Companion runbooks:**
> - [Runbook 30 — CDC Pipeline E2E (all sources)](runbook-30-cdc-e2e-testing.md) — canonical reference
> - [Runbook 29 — CDC Architecture](runbook-29-cdc-debezium-kafka-iceberg-architecture.md)
> - [Runbook 31 — Oracle → Kafka → Iceberg](runbook-31-oracle-kafka-iceberg-e2e-testing.md)

---

## Table of Contents

1. [Pipeline Overview](#1-pipeline-overview)
2. [Prerequisites Check](#2-prerequisites-check)
3. [Section 1 — Standard Mode Tests (SCD Type 0)](#3-section-1--standard-mode-tests-scd-type-0)
4. [Section 2 — Soft Delete Mode Tests](#4-section-2--soft-delete-mode-tests)
5. [Section 3 — History Tracking Mode Tests](#5-section-3--history-tracking-mode-tests)
6. [Expected Results Summary](#6-expected-results-summary)

---

## 1. Pipeline Overview

```
MongoDB 7 (Replica Set rs0)
  └─ Change Streams (oplog tail)
       └─ Debezium mongodb-cache-testing-cdc connector
            └─ Kafka topics (SASL/SCRAM-SHA-512)
                 ├─ mongodb.cache_testing.customers
                 └─ mongodb.cache_testing.products
                      └─ Spark Structured Streaming (kafka-to-iceberg-standard)
                           └─ Iceberg REST catalog "mongodb" (Polaris)
                                └─ mongodb.e2e_testing.customers  (etc.)
```

### Topic → Iceberg table mapping (with `TARGET_NAMESPACE=e2e_testing`)

| Kafka topic | Iceberg table |
|---|---|
| `mongodb.cache_testing.customers` | `mongodb.e2e_testing.customers` |
| `mongodb.cache_testing.products` | `mongodb.e2e_testing.products` |

> **Key differences from PostgreSQL/Oracle:**
> - MongoDB uses **schemaless documents** — new fields added to a document cause automatic
>   schema evolution in Iceberg (no explicit `ALTER TABLE` required).
> - The primary key is `_id` (ObjectId) internally but the application-level integer `id`
>   field is used as the merge key in Iceberg.
> - Change stream events always carry the **full document** after update (no before-image by
>   default) — the pipeline synthesises before-images for history tracking from the
>   previous Iceberg snapshot.
> - Propagation is typically faster than Oracle LogMiner: ~5–10 s.

### `customers` collection schema (MongoDB `cache_testing` database)

```javascript
{
  _id:        ObjectId  // MongoDB internal ID — used as partition key
  id:         Number    // application integer PK — merge key in Iceberg
  name:       String
  email:      String
  phone:      String
  address:    String
  city:       String
  country:    String
  created_at: Date
  // additional fields may be present (dynamic schema)
}
```

**Test document `id` range:** `900200–900299` — unique to this runbook; no collision with
Runbook 30 (900001–900099) or Runbook 31 (900100–900199).

---

## 2. Prerequisites Check

Run all checks before executing any test section. All must pass.

### 2.1 — Required CLI Tools

```bash
# kubectl — cluster access
kubectl version --client --short 2>/dev/null || kubectl version --client

# mongosh via exec into MongoDB pod
MONGO_POD=$(kubectl get pod -n prod -l app=mongodb -o jsonpath='{.items[0].metadata.name}')
echo "MongoDB pod: ${MONGO_POD}"

# Debezium REST API
curl -s http://192.168.1.54:30083/connectors | python3 -m json.tool | head -5
```

✅ Expected: kubectl responds, `MONGO_POD` is non-empty, connector list returns JSON.

---

### 2.2 — MongoDB Change Stream Connector Running

```bash
STATE=$(curl -s http://192.168.1.54:30083/connectors/mongodb-cache-testing-cdc/status \
  | python3 -c "import sys,json; d=json.load(sys.stdin); \
    print(d['connector']['state'], \
    '| tasks:', ','.join(t['state'] for t in d['tasks']))")
echo "mongodb-cache-testing-cdc: ${STATE}"
```

✅ Expected: `mongodb-cache-testing-cdc: RUNNING | tasks: RUNNING`

If FAILED — try a simple restart first:
```bash
curl -s -X POST http://192.168.1.54:30083/connectors/mongodb-cache-testing-cdc/restart
sleep 10
curl -s http://192.168.1.54:30083/connectors/mongodb-cache-testing-cdc/status \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])"
```

If still FAILED (common after converting from standalone → replica set), re-register with the
correct replica-set connection string:
```bash
curl -s -X PUT http://192.168.1.54:30083/connectors/mongodb-cache-testing-cdc/config \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
    "mongodb.connection.string": "mongodb://root:oEtCgw554IP3ua0SrJCTsWYM@mongodb.prod.svc.cluster.local:27017/?replicaSet=rs0&authSource=admin",
    "topic.prefix": "mongodb",
    "database.include.list": "cache_testing",
    "collection.include.list": "cache_testing.customers,cache_testing.products",
    "snapshot.mode": "never",
    "capture.mode": "change_streams_update_document_key_only_handling_tombstone_events"
  }'
sleep 10
curl -s http://192.168.1.54:30083/connectors/mongodb-cache-testing-cdc/status \
  | python3 -c "import sys,json; d=json.load(sys.stdin); \
    print(d['connector']['state'], '| tasks:', ','.join(t['state'] for t in d['tasks']))"
```

---

### 2.3 — Verify MongoDB Replica Set is Healthy

Change streams require a replica set. Verify:

```bash
kubectl exec -n prod $MONGO_POD -- \
  mongosh "mongodb://root:oEtCgw554IP3ua0SrJCTsWYM@localhost:27017/admin?authSource=admin" \
  --quiet --eval "rs.status().ok"
```

✅ Expected: `1`

If not `1`:
```bash
kubectl exec -n prod $MONGO_POD -- \
  mongosh "mongodb://root:oEtCgw554IP3ua0SrJCTsWYM@localhost:27017/admin?authSource=admin" \
  --quiet --eval "rs.status()"
```
Review the output for members in `SECONDARY` or `STARTUP` state. A single-node replica
set must show `stateStr: "PRIMARY"`.

---

### 2.4 — Streaming Job Healthy

```bash
kubectl get deployment kafka-to-iceberg-standard -n prod \
  -o jsonpath='{.status.readyReplicas}/{.spec.replicas}'
echo " (expected 1/1)"
```

Confirm the MongoDB source is active:

```bash
kubectl logs -n prod \
  $(kubectl get pod -n prod \
    -l app=kafka-to-iceberg,pipeline.source=mongodb,pipeline.write-mode=standard \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=60s 2>&1 \
  | grep -E "mongodb|batch=|ERROR" | tail -20
```

✅ Expected: `[mongodb/customers][standard] batch=N …` lines visible; no `ERROR`.

---

### 2.5 — Activate E2E Namespace

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

Verify namespace visible in mongodb catalog (Spark SQL / JupyterHub Cell 2):
```sql
SHOW NAMESPACES IN mongodb;
-- must include: e2e_testing
```

> **Production reset** — always clear after testing:
> ```bash
> kubectl set env deployment/kafka-to-iceberg-standard -n prod TARGET_NAMESPACE=
> kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod
> ```

---

### 2.6 — JupyterHub Spark Session

All Iceberg verification steps use JupyterHub with a PySpark kernel. The `mongodb` catalog
is registered in Polaris alongside `postgres` and `oracle`.

> **Rules:**
> - Run cells **top-to-bottom** on every new session.
> - Always run the **stop cell last** to release the held cluster core.
> - Never leave the session idle.

#### Open JupyterHub

1. Navigate to **`http://192.168.1.50:30888`**
2. Log in as `admin` — password:
   ```bash
   kubectl get secret jupyterhub-credentials -n analytics \
     -o jsonpath='{.data.admin-password}' | base64 -d
   ```
3. **File → New → Notebook → Python 3 kernel**

---

#### Notebook Cell 1 — Fetch token & credentials

```bash
# Terminal — get root token
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

✅ Expected: `✅ Credentials loaded`

---

#### Notebook Cell 2 — Start Spark session with `mongodb` catalog

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
    .appName("mongodb-e2e-verify")
    .config("spark.cores.max",           "1")
    .config("spark.executor.instances",  "1")
    .config("spark.executor.cores",      "1")
    .config("spark.executor.memory",     "2g")
    .config("spark.driver.host",         DRIVER_IP)
    .config("spark.driver.bindAddress",  DRIVER_IP)
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    # ── mongodb catalog (Polaris REST) ─────────────────────────────────────
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
    # ── S3A hadoop layer ───────────────────────────────────────────────────
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

---

## 3. Section 1 — Standard Mode Tests (SCD Type 0)

**Write mode:** `standard` — each UPSERT overwrites the existing row; DELETEs are hard-deleted.  
**Target table:** `mongodb.e2e_testing.customers`  
**Test document `id`:** `900200`

---

### Test 1.1 — Standard: INSERT

#### Step 1 — Note baseline row count

**Notebook Cell 3:**
```python
cnt = spark.sql(
    "SELECT COUNT(*) FROM mongodb.e2e_testing.customers"
).collect()[0][0]
print(f"baseline_row_count = {cnt}")

exists = spark.sql(
    "SELECT COUNT(*) FROM mongodb.e2e_testing.customers WHERE id = 900200"
).collect()[0][0]
print(f"id=900200 already_exists = {exists > 0}  ← must be False before proceeding")
```

✅ Expected:
```
baseline_row_count = <N>
id=900200 already_exists = False  ← must be False before proceeding
```

> If `already_exists = True`, a previous run was not cleaned up.
> Run the DELETE in Step 1.3 first, wait 10 s, then re-run this cell.

---

#### Step 2 — INSERT a test document into MongoDB

Open a `mongosh` session:

```bash
MONGO_POD=$(kubectl get pod -n prod -l app=mongodb -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it -n prod $MONGO_POD -- \
  mongosh "mongodb://root:oEtCgw554IP3ua0SrJCTsWYM@localhost:27017/cache_testing?authSource=admin" \
  --quiet
```

```javascript
use cache_testing;

db.customers.insertOne({
  _id:        ObjectId("000000000000000000900200"),
  id:         900200,
  name:       "E2E MongoStd",
  email:      "e2e_mongo_std@example.com",
  phone:      "555-0200",
  address:    "200 Mongo Way",
  city:       "Sydney",
  country:    "AU",
  created_at: new Date()
});
```

✅ Expected:
```javascript
{
  acknowledged: true,
  insertedId: ObjectId('000000000000000000900200')
}
```

---

#### Step 3 — Wait for pipeline propagation

MongoDB change streams are near-realtime; the Spark micro-batch runs every 2 s. Allow 10 s:

```bash
sleep 10
```

Check the streaming job processed the insert:

```bash
kubectl logs -n prod \
  $(kubectl get pod -n prod \
    -l app=kafka-to-iceberg,pipeline.source=mongodb,pipeline.write-mode=standard \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=20s 2>&1 | grep -E "batch=|upsert|hard-delete|ERROR"
```

✅ Expected: `[mongodb/customers][standard] batch=N upsert rows=1`

---

#### Step 4 — Verify INSERT in Iceberg

**Notebook Cell 4:**
```python
print("=== INSERT verify ===")
spark.sql("""
    SELECT id, name, email, city, country, snap_id, snap_timestamp
    FROM   mongodb.e2e_testing.customers
    WHERE  id = 900200
""").show(truncate=False)
```

✅ Expected:
```
=== INSERT verify ===
+------+------------+--------------------------+------+-------+-------+--------------+
|id    |name        |email                     |city  |country|snap_id|snap_timestamp|
+------+------------+--------------------------+------+-------+-------+--------------+
|900200|E2E MongoStd|e2e_mongo_std@example.com |Sydney|AU     |...    |...           |
+------+------------+--------------------------+------+-------+-------+--------------+
```
1 row returned. `snap_id` is a non-null BIGINT. `snap_timestamp` is within the last 30 s.

---

### Test 1.2 — Standard: UPDATE

#### Step 1 — UPDATE the test document in MongoDB

```javascript
// mongosh (cache_testing database)
db.customers.updateOne(
  { id: 900200 },
  { $set: {
      email: "e2e_mongo_std_updated@example.com",
      city:  "Melbourne"
  }}
);
```

✅ Expected:
```javascript
{
  acknowledged: true,
  matchedCount: 1,
  modifiedCount: 1
}
```

---

#### Step 2 — Wait and verify UPDATE in Iceberg

```bash
sleep 10
kubectl logs -n prod \
  $(kubectl get pod -n prod \
    -l app=kafka-to-iceberg,pipeline.source=mongodb,pipeline.write-mode=standard \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=20s 2>&1 | grep -E "batch=|upsert|hard-delete|ERROR"
```

✅ Expected: `[mongodb/customers][standard] batch=N upsert rows=1`

**Notebook Cell 5:**
```python
print("=== UPDATE verify ===")
spark.sql("""
    SELECT id, name, email, city, snap_id, snap_timestamp
    FROM   mongodb.e2e_testing.customers
    WHERE  id = 900200
""").show(truncate=False)
```

✅ Expected:
```
=== UPDATE verify ===
+------+------------+----------------------------------+---------+-------+--------------+
|id    |name        |email                             |city     |snap_id|snap_timestamp|
+------+------------+----------------------------------+---------+-------+--------------+
|900200|E2E MongoStd|e2e_mongo_std_updated@example.com |Melbourne|...    |...           |
+------+------------+----------------------------------+---------+-------+--------------+
```
`email` = `e2e_mongo_std_updated@example.com`, `city` = `Melbourne`.  
`snap_id` differs from Step 4. `snap_timestamp` is newer.

---

### Test 1.3 — Standard: DELETE

#### Step 1 — DELETE the test document from MongoDB

MongoDB does not enforce foreign keys — a single `deleteOne` is sufficient:

```javascript
// mongosh (cache_testing database)
db.customers.deleteOne({ id: 900200 });
```

✅ Expected:
```javascript
{ acknowledged: true, deletedCount: 1 }
```

---

#### Step 2 — Wait and verify hard DELETE in Iceberg

```bash
sleep 10
kubectl logs -n prod \
  $(kubectl get pod -n prod \
    -l app=kafka-to-iceberg,pipeline.source=mongodb,pipeline.write-mode=standard \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=20s 2>&1 | grep -E "batch=|upsert|hard-delete|ERROR"
```

✅ Expected: `[mongodb/customers][standard] batch=N hard-delete rows=1`

**Notebook Cell 6:**
```python
print("=== DELETE verify ===")
cnt = spark.sql(
    "SELECT COUNT(*) FROM mongodb.e2e_testing.customers WHERE id = 900200"
).collect()[0][0]
total = spark.sql(
    "SELECT COUNT(*) FROM mongodb.e2e_testing.customers"
).collect()[0][0]
print(f"id=900200 row_count   = {cnt}    (expected 0)")
print(f"total_rows_remaining = {total}  (expected baseline_row_count)")
```

✅ Expected:
```
=== DELETE verify ===
id=900200 row_count   = 0    (expected 0)
total_rows_remaining = <N>  (expected baseline_row_count)
```

---

#### Step 3 — Stop the Spark session ⚠️

**Notebook Cell 7:**
```python
spark.stop()
print("✅ Session stopped — cluster core released")
```

---

**Test Section 1 pass criteria:**

| Step | Operation | Kafka log expected | Iceberg result |
|------|-----------|-------------------|----------------|
| 1.1  | INSERT    | `batch=N upsert rows=1`      | 1 row, correct values |
| 1.2  | UPDATE    | `batch=N upsert rows=1`      | `email` and `city` updated |
| 1.3  | DELETE    | `batch=N hard-delete rows=1` | `row_count = 0` |

---

## 4. Section 2 — Soft Delete Mode Tests

**Write mode:** `soft_delete` — DELETE events set `is_deleted = true` and record `deleted_at`;
the document is **never physically removed** from Iceberg.  
**Target table:** `mongodb.e2e_testing.customers_sd`  
**Test document `id`:** `900210`

---

### Setup: Switch to soft_delete mode

```bash
kubectl scale deployment kafka-to-iceberg-standard    -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-soft-delete -n prod --replicas=1
kubectl set env deployment/kafka-to-iceberg-soft-delete -n prod \
  TARGET_NAMESPACE=e2e_testing
kubectl rollout restart deployment/kafka-to-iceberg-soft-delete -n prod
kubectl rollout status  deployment/kafka-to-iceberg-soft-delete -n prod
```

Verify:
```bash
kubectl get deployment -n prod | grep kafka-to-iceberg
```
**Expected:** `kafka-to-iceberg-soft-delete` shows `1/1 READY`; `kafka-to-iceberg-standard` shows `0/0`.

Confirm `TARGET_NAMESPACE` active:
```bash
kubectl logs -n prod -l pipeline.write-mode=soft-delete --tail=15 \
  | grep -i "TARGET_NAMESPACE\|e2e_testing"
```

---

### Create the `customers_sd` table in mongodb catalog

Run once in JupyterHub if the table does not yet exist:

```python
# Notebook — start session with mongodb catalog (Cell 1 & 2 as above)
spark.sql("""
  CREATE TABLE IF NOT EXISTS mongodb.e2e_testing.customers_sd (
      id             BIGINT,
      name           STRING,
      email          STRING,
      phone          STRING,
      address        STRING,
      city           STRING,
      country        STRING,
      created_at     TIMESTAMP,
      is_deleted     BOOLEAN,
      deleted_at     TIMESTAMP,
      snap_id        BIGINT,
      snap_timestamp TIMESTAMP
  )
  USING iceberg
  PARTITIONED BY (days(snap_timestamp))
""")
print("✅ mongodb.e2e_testing.customers_sd ready")
```

---

### Test 2.1 — Soft Delete: INSERT

#### Step 1 — INSERT test document into MongoDB

```javascript
// mongosh (cache_testing database)
db.customers.insertOne({
  _id:        ObjectId("000000000000000000900210"),
  id:         900210,
  name:       "E2E MongoSoft",
  email:      "mongo_soft@example.com",
  phone:      "555-0210",
  address:    "210 Soft Blvd",
  city:       "Brisbane",
  country:    "AU",
  created_at: new Date()
});
```

✅ Expected: `{ acknowledged: true, insertedId: ObjectId('000000000000000000900210') }`

#### Step 2 — Wait and verify in Iceberg

```bash
sleep 10
kubectl logs -n prod -l pipeline.write-mode=soft-delete --since=20s 2>&1 \
  | grep -E "batch=|upsert|soft-delete|ERROR" | tail -10
```

✅ Expected: `[mongodb/customers][soft_delete] batch=N upsert rows=1`

**Notebook Cell:**
```python
spark.sql("""
    SELECT id, email, is_deleted, deleted_at, snap_id, snap_timestamp
    FROM   mongodb.e2e_testing.customers_sd
    WHERE  id = 900210
""").show(truncate=False)
```

✅ Expected: 1 row; `is_deleted = false`; `deleted_at = NULL`; `snap_id` and `snap_timestamp` populated.

---

### Test 2.2 — Soft Delete: UPDATE

#### Step 1 — UPDATE the document

```javascript
// mongosh (cache_testing database)
db.customers.updateOne(
  { id: 900210 },
  { $set: {
      email: "mongo_soft_updated@example.com",
      city:  "Perth"
  }}
);
```

#### Step 2 — Verify UPDATE

```bash
sleep 10
```

**Notebook Cell:**
```python
spark.sql("""
    SELECT id, email, city, is_deleted, deleted_at
    FROM   mongodb.e2e_testing.customers_sd
    WHERE  id = 900210
""").show(truncate=False)
```

✅ Expected: `email = 'mongo_soft_updated@example.com'`; `city = 'Perth'`; `is_deleted = false`; `deleted_at = NULL`.

---

### Test 2.3 — Soft Delete: DELETE

#### Step 1 — DELETE the document from MongoDB

```javascript
// mongosh (cache_testing database)
db.customers.deleteOne({ id: 900210 });
```

✅ Expected: `{ acknowledged: true, deletedCount: 1 }`

#### Step 2 — Verify soft delete in Iceberg

```bash
sleep 10
```

**Notebook Cell:**
```python
spark.sql("""
    SELECT id, email, is_deleted, deleted_at
    FROM   mongodb.e2e_testing.customers_sd
    WHERE  id = 900210
""").show(truncate=False)
```

✅ Expected: Row **still present**; `is_deleted = true`; `deleted_at` is a non-null TIMESTAMP within the last 30 seconds.

#### Step 3 — Query all soft-deleted rows

```python
spark.sql("""
    SELECT id, email, deleted_at
    FROM   mongodb.e2e_testing.customers_sd
    WHERE  is_deleted = true
    ORDER BY deleted_at DESC
    LIMIT 20
""").show(truncate=False)
```

✅ Expected: `id = 900210` appears in results.

#### Step 4 — Confirm the document count did NOT decrease

```python
after_cnt = spark.sql(
    "SELECT COUNT(*) FROM mongodb.e2e_testing.customers_sd WHERE id = 900210"
).collect()[0][0]
print(f"row still present = {after_cnt == 1}  (expected True)")
```

---

### Teardown: Switch back to standard mode

```bash
kubectl scale deployment kafka-to-iceberg-soft-delete -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-standard    -n prod --replicas=1
kubectl rollout status deployment/kafka-to-iceberg-standard -n prod
```

---

**Section 2 pass criteria:**

| Test | Operation | Kafka log expected | Iceberg result |
|------|-----------|-------------------|----------------|
| 2.1  | INSERT    | `batch=N upsert rows=1`      | 1 row, `is_deleted=false` |
| 2.2  | UPDATE    | `batch=N upsert rows=1`      | new values, `is_deleted=false` |
| 2.3  | DELETE    | `batch=N soft-delete rows=1` | row retained, `is_deleted=true`, `deleted_at` set |

---

## 5. Section 3 — History Tracking Mode Tests

**Write mode:** `history_tracking` — every INSERT / UPDATE / DELETE appends a new row to the
`_hist` table. No rows are overwritten.

> **MongoDB change stream note:** By default the MongoDB Debezium connector only emits the
> **after-image** of a document on UPDATE (the full document as it is after the change).
> The streaming job derives a synthetic `before_*` state from the previous Iceberg
> snapshot row for history tracking. Ensure `fullDocument: 'updateLookup'` is enabled
> in the connector configuration (see Runbook 29).

**Target table:** `mongodb.e2e_testing.customers_hist`  
**Test document `id`:** `900220`

---

### Setup: Switch to history_tracking mode

```bash
kubectl scale deployment kafka-to-iceberg-standard         -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-history-tracking -n prod --replicas=1
kubectl set env deployment/kafka-to-iceberg-history-tracking -n prod \
  TARGET_NAMESPACE=e2e_testing
kubectl rollout restart deployment/kafka-to-iceberg-history-tracking -n prod
kubectl rollout status  deployment/kafka-to-iceberg-history-tracking -n prod
```

Verify:
```bash
kubectl get deployment -n prod | grep kafka-to-iceberg
# Expected: history-tracking shows 1/1 READY; others show 0/0
```

---

### Create the `customers_hist` table in mongodb catalog

```python
spark.sql("""
  CREATE TABLE IF NOT EXISTS mongodb.e2e_testing.customers_hist (
      id             BIGINT,
      _change_type   STRING,
      _change_ts     TIMESTAMP,
      before_id      BIGINT,
      before_name    STRING,
      before_email   STRING,
      before_phone   STRING,
      before_address STRING,
      before_city    STRING,
      before_country STRING,
      after_id       BIGINT,
      after_name     STRING,
      after_email    STRING,
      after_phone    STRING,
      after_address  STRING,
      after_city     STRING,
      after_country  STRING,
      snap_id        BIGINT,
      snap_timestamp TIMESTAMP
  )
  USING iceberg
  PARTITIONED BY (days(snap_timestamp))
""")
print("✅ mongodb.e2e_testing.customers_hist ready")
```

---

### Test 3.1 — History: INSERT

#### Step 1 — INSERT test document

```javascript
// mongosh (cache_testing database)
db.customers.insertOne({
  _id:        ObjectId("000000000000000000900220"),
  id:         900220,
  name:       "E2E MongoHist",
  email:      "mongo_hist@example.com",
  phone:      "555-0220",
  address:    "220 History Ct",
  city:       "Adelaide",
  country:    "AU",
  created_at: new Date()
});
```

✅ Expected: `{ acknowledged: true, insertedId: ObjectId('000000000000000000900220') }`

#### Step 2 — Wait and verify in `_hist` table

```bash
sleep 10
kubectl logs -n prod -l pipeline.write-mode=history-tracking --since=20s 2>&1 \
  | grep -E "batch=|append|ERROR" | tail -10
```

✅ Expected: `[mongodb/customers][history_tracking] batch=N append rows=1`

**Notebook Cell:**
```python
print("=== INSERT — history verify ===")
spark.sql("""
    SELECT id, _change_type, _change_ts,
           before_id, before_email,
           after_id, after_email,
           snap_id, snap_timestamp
    FROM   mongodb.e2e_testing.customers_hist
    WHERE  after_id = 900220
    ORDER BY _change_ts
""").show(truncate=False)
```

✅ Expected: 1 row; `_change_type = 'INSERT'`; all `before_*` columns are NULL;
`after_id = 900220`; `after_email = 'mongo_hist@example.com'`.

---

### Test 3.2 — History: UPDATE

#### Step 1 — UPDATE the document

```javascript
// mongosh (cache_testing database)
db.customers.updateOne(
  { id: 900220 },
  { $set: {
      email: "mongo_hist_updated@example.com",
      city:  "Darwin"
  }}
);
```

#### Step 2 — Wait and verify UPDATE row in `_hist`

```bash
sleep 10
```

**Notebook Cell:**
```python
print("=== UPDATE — history verify ===")
spark.sql("""
    SELECT id, _change_type, _change_ts,
           before_email, before_city,
           after_email,  after_city
    FROM   mongodb.e2e_testing.customers_hist
    WHERE  after_id = 900220
       OR before_id = 900220
    ORDER BY _change_ts
""").show(truncate=False)
```

✅ Expected: 2 rows:
- Row 1: `_change_type = 'INSERT'`, `before_email = NULL`, `after_email = 'mongo_hist@example.com'`
- Row 2: `_change_type = 'UPDATE'`, `before_email = 'mongo_hist@example.com'`,
  `after_email = 'mongo_hist_updated@example.com'`, `before_city = 'Adelaide'`, `after_city = 'Darwin'`

> **Note on before-image:** The `before_*` values on Row 2 are derived from the previous
> Iceberg snapshot row (Row 1's `after_*` fields). This is the expected behaviour for
> MongoDB change streams, which do not natively emit before-images.

---

### Test 3.3 — History: DELETE

#### Step 1 — DELETE the document from MongoDB

```javascript
// mongosh (cache_testing database)
db.customers.deleteOne({ id: 900220 });
```

✅ Expected: `{ acknowledged: true, deletedCount: 1 }`

#### Step 2 — Wait and verify DELETE row in `_hist`

```bash
sleep 10
```

**Notebook Cell:**
```python
print("=== DELETE — history verify ===")
spark.sql("""
    SELECT id, _change_type, _change_ts,
           before_email, before_city,
           after_email,  after_city
    FROM   mongodb.e2e_testing.customers_hist
    WHERE  after_id = 900220
       OR before_id = 900220
    ORDER BY _change_ts
""").show(truncate=False)
```

✅ Expected: 3 rows; the third has:
- `_change_type = 'DELETE'`
- `before_email = 'mongo_hist_updated@example.com'`
- `before_city = 'Darwin'`
- all `after_*` columns are NULL

---

### Test 3.4 — Full audit trail for a single document

```python
print("=== Full audit trail for id=900220 ===")
spark.sql("""
    SELECT _change_type, _change_ts,
           before_email, after_email,
           before_city,  after_city,
           snap_id
    FROM   mongodb.e2e_testing.customers_hist
    WHERE  after_id = 900220
       OR before_id = 900220
    ORDER BY _change_ts ASC
""").show(truncate=False)
```

✅ Expected: 3 rows in chronological order — **INSERT → UPDATE → DELETE** — forming a
complete MongoDB change-stream-sourced audit trail in Iceberg.

---

### Test 3.5 — Confirm history rows are immutable (append-only)

```python
cnt = spark.sql(
    "SELECT COUNT(*) FROM mongodb.e2e_testing.customers_hist WHERE after_id = 900220 OR before_id = 900220"
).collect()[0][0]
print(f"history_row_count = {cnt}  (expected 3)")
```

✅ Expected: `history_row_count = 3`

---

### Test 3.6 — Implicit schema evolution (bonus — MongoDB-only)

This test is unique to MongoDB: a new field added to a document causes the Iceberg table
schema to automatically evolve without any DDL command.

```javascript
// mongosh (cache_testing database)
// Insert a new document with an extra 'loyalty_tier' field not in the original schema
db.customers.insertOne({
  _id:           ObjectId("000000000000000000900221"),
  id:            900221,
  name:          "E2E SchemaEvol",
  email:         "schema_evol@example.com",
  loyalty_tier:  "platinum",   // ← new field
  created_at:    new Date()
});
```

```bash
sleep 10
```

```python
print("=== Schema evolution verify ===")
# Check schema — loyalty_tier should have been added automatically
spark.sql("DESCRIBE TABLE mongodb.e2e_testing.customers_hist").show(50, truncate=False)

# Check the row landed
spark.sql("""
    SELECT id, after_email, snap_id, snap_timestamp
    FROM   mongodb.e2e_testing.customers_hist
    WHERE  after_id = 900221
""").show(truncate=False)
```

✅ Expected: `loyalty_tier` column appears in `DESCRIBE TABLE` output; row with `after_id = 900221` is present.

**Cleanup:**
```javascript
// mongosh
db.customers.deleteOne({ id: 900221 });
```

---

### Teardown: Switch back to standard mode

```bash
kubectl scale deployment kafka-to-iceberg-history-tracking -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-standard         -n prod --replicas=1
kubectl rollout status deployment/kafka-to-iceberg-standard -n prod
```

---

**Section 3 pass criteria:**

| Test | Operation | Kafka log expected | `_hist` rows | Change type |
|------|-----------|-------------------|--------------|-------------|
| 3.1  | INSERT    | `batch=N append rows=1` | 1 | INSERT — `before_*` all NULL |
| 3.2  | UPDATE    | `batch=N append rows=1` | 2 | UPDATE — before derived from prior snapshot |
| 3.3  | DELETE    | `batch=N append rows=1` | 3 | DELETE — `after_*` all NULL |
| 3.4  | Full trail | — | 3 | INSERT → UPDATE → DELETE in order |
| 3.5  | Immutability | — | 3 | count unchanged after re-query |
| 3.6  | Schema evolution | `batch=N append rows=1` | 1 (id=900221) | New field auto-added to Iceberg schema |

---

## 6. Expected Results Summary

### MongoDB → Kafka → Iceberg: End-to-End Verification Matrix

| Section | Mode | Operation | Debezium Source | Kafka Topic | Iceberg Table | Pass Condition |
|---------|------|-----------|-----------------|-------------|---------------|----------------|
| 1.1 | standard | INSERT | Change stream | `mongodb.cache_testing.customers` | `mongodb.e2e_testing.customers` | 1 row, correct values, `snap_id` non-null |
| 1.2 | standard | UPDATE | Change stream | `mongodb.cache_testing.customers` | `mongodb.e2e_testing.customers` | row overwritten, new `snap_id`, newer `snap_timestamp` |
| 1.3 | standard | DELETE | Change stream | `mongodb.cache_testing.customers` | `mongodb.e2e_testing.customers` | `COUNT(*) WHERE id=900200 = 0` |
| 2.1 | soft_delete | INSERT | Change stream | `mongodb.cache_testing.customers` | `mongodb.e2e_testing.customers_sd` | `is_deleted=false`, `deleted_at=NULL` |
| 2.2 | soft_delete | UPDATE | Change stream | `mongodb.cache_testing.customers` | `mongodb.e2e_testing.customers_sd` | new values, `is_deleted=false` |
| 2.3 | soft_delete | DELETE | Change stream | `mongodb.cache_testing.customers` | `mongodb.e2e_testing.customers_sd` | row retained, `is_deleted=true`, `deleted_at` set |
| 3.1 | history_tracking | INSERT | Change stream | `mongodb.cache_testing.customers` | `mongodb.e2e_testing.customers_hist` | 1 `_hist` row, `_change_type=INSERT` |
| 3.2 | history_tracking | UPDATE | Change stream | `mongodb.cache_testing.customers` | `mongodb.e2e_testing.customers_hist` | 2nd `_hist` row, `_change_type=UPDATE` |
| 3.3 | history_tracking | DELETE | Change stream | `mongodb.cache_testing.customers` | `mongodb.e2e_testing.customers_hist` | 3rd `_hist` row, `_change_type=DELETE` |
| 3.6 | history_tracking | Schema evolution | Change stream | `mongodb.cache_testing.customers` | `mongodb.e2e_testing.customers_hist` | New field added without DDL; row lands correctly |

### Key MongoDB-Specific Notes

| Concern | Detail |
|---------|--------|
| Change stream mechanism | Requires a replica set (or sharded cluster); standalone nodes do not support change streams |
| Before-image availability | MongoDB change streams emit after-image only by default; `fullDocument: 'updateLookup'` is required for the connector to re-read the full document; before-image in history tracking is synthesised from the previous Iceberg snapshot |
| Propagation latency | ~5–10 s (faster than Oracle LogMiner, similar to Postgres WAL) |
| FK constraints | MongoDB has no FK enforcement — a single `deleteOne` is sufficient; no child cleanup required |
| ObjectId format | Test documents use `ObjectId("000000000000000000XXXXXX")` — 24 hex chars — for human-readable IDs |
| Schema evolution | New fields in documents auto-evolve the Iceberg schema; no `ALTER TABLE` needed |
| Topic naming | Topics use **lowercase** collection names: `mongodb.cache_testing.customers` |

### Production Reset Checklist

After completing all tests:

```bash
# 1. Clear TARGET_NAMESPACE
kubectl set env deployment/kafka-to-iceberg-standard -n prod TARGET_NAMESPACE=
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod

# 2. Confirm standard deployment is back in production mode
kubectl logs -n prod -l app=kafka-to-iceberg,pipeline.write-mode=standard --tail=20 \
  | grep -E "batch=|TARGET_NAMESPACE|ERROR"

# 3. Verify MongoDB CDC connector still running
curl -s http://192.168.1.54:30083/connectors/mongodb-cache-testing-cdc/status \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])"
# Expected: RUNNING
```
