# Runbook 31 — Oracle → Kafka → Iceberg End-to-End Test Runbook

**Status:** Operational  
**Namespace:** `prod`  
**Source DB:** Oracle XE 21c — PDB `XEPDB1`, schema `CACHE_TESTING`  
**CDC connector:** `oracle-cache-testing-cdc` (Debezium LogMiner)  
**Iceberg catalog:** `oracle` (Polaris REST)  
**Estimated duration:** 30–60 minutes (Sections 1–3 full suite)

> **Companion runbooks:**
> - [Runbook 30 — CDC Pipeline E2E (all sources)](runbook-30-cdc-e2e-testing.md) — canonical reference
> - [Runbook 29 — CDC Architecture](runbook-29-cdc-debezium-kafka-iceberg-architecture.md)
> - [Runbook 32 — MongoDB → Kafka → Iceberg](runbook-32-mongodb-kafka-iceberg-e2e-testing.md)

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
Oracle XE 21c (XEPDB1)
  └─ LogMiner redo log tail
       └─ Debezium oracle-cache-testing-cdc connector
            └─ Kafka topics (SASL/SCRAM-SHA-512)
                 ├─ oracle.cache_testing.CUSTOMERS
                 ├─ oracle.cache_testing.ORDERS
                 ├─ oracle.cache_testing.PRODUCTS
                 ├─ oracle.cache_testing.ORDER_ITEMS
                 ├─ oracle.cache_testing.INVENTORY_EVENTS
                 └─ oracle.cache_testing.PRODUCT_REVIEWS
                      └─ Spark Structured Streaming (kafka-to-iceberg-standard)
                           └─ Iceberg REST catalog "oracle" (Polaris)
                                └─ oracle.e2e_testing.customers  (etc.)
```

### Topic → Iceberg table mapping (with `TARGET_NAMESPACE=e2e_testing`)

| Kafka topic | Iceberg table |
|---|---|
| `oracle.cache_testing.CUSTOMERS` | `oracle.e2e_testing.customers` |
| `oracle.cache_testing.ORDERS` | `oracle.e2e_testing.orders` |
| `oracle.cache_testing.PRODUCTS` | `oracle.e2e_testing.products` |
| `oracle.cache_testing.ORDER_ITEMS` | `oracle.e2e_testing.order_items` |

> **Key difference from PostgreSQL:** Oracle LogMiner uses uppercase table names in topics.
> The streaming job lowercases the last segment before writing to Iceberg.
> Oracle also emits a `COMMIT` SCN rather than WAL LSN — propagation can take up to 15 s
> in busy periods.

### `CUSTOMERS` table schema (Oracle `CACHE_TESTING` schema)

```
ID          NUMBER(19)   PK
NAME        VARCHAR2(255)
EMAIL       VARCHAR2(255)
PHONE       VARCHAR2(50)
ADDRESS     VARCHAR2(500)
CITY        VARCHAR2(100)
COUNTRY     VARCHAR2(10)
CREATED_AT  TIMESTAMP
UPDATED_AT  TIMESTAMP
```

**Test row ID range:** `900100–900199` — unique to this runbook; no collision with
Runbook 30 (900001–900099) or Runbook 32 (900200–900299).

---

## 2. Prerequisites Check

Run all checks before executing any test section. All must pass.

### 2.1 — Required CLI Tools

```bash
# kubectl — cluster access
kubectl version --client --short 2>/dev/null || kubectl version --client

# sqlplus via exec into Oracle pod
ORACLE_POD=$(kubectl get pod -n prod -l app=oracle-xe -o jsonpath='{.items[0].metadata.name}')
echo "Oracle pod: ${ORACLE_POD}"

# curl for Debezium REST API
curl -s http://192.168.1.54:30083/connectors | python3 -m json.tool | head -5
```

✅ Expected: kubectl responds, `ORACLE_POD` is non-empty, connector list returns JSON.

---

### 2.2 — Oracle CDC Connector Running

```bash
STATE=$(curl -s http://192.168.1.54:30083/connectors/oracle-cache-testing-cdc/status \
  | python3 -c "import sys,json; d=json.load(sys.stdin); \
    print(d['connector']['state'], \
    '| tasks:', ','.join(t['state'] for t in d['tasks']))")
echo "oracle-cache-testing-cdc: ${STATE}"
```

✅ Expected: `oracle-cache-testing-cdc: RUNNING | tasks: RUNNING`

If FAILED:
```bash
curl -s -X POST http://192.168.1.54:30083/connectors/oracle-cache-testing-cdc/restart
sleep 10
curl -s http://192.168.1.54:30083/connectors/oracle-cache-testing-cdc/status \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])"
```

---

### 2.3 — Streaming Job Healthy

```bash
kubectl get deployment kafka-to-iceberg-standard -n prod \
  -o jsonpath='{.status.readyReplicas}/{.spec.replicas}'
echo " (expected 1/1)"
```

Confirm the Oracle source is active in the last 60 s of logs:

```bash
kubectl logs -n prod \
  $(kubectl get pod -n prod \
    -l app=kafka-to-iceberg,pipeline.source=oracle,pipeline.write-mode=standard \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=60s 2>&1 \
  | grep -E "oracle|batch=|ERROR" | tail -20
```

✅ Expected: `[oracle/customers][standard] batch=N …` lines visible; no `ERROR`.

---

### 2.4 — Activate E2E Namespace

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

✅ Expected: `deployment "kafka-to-iceberg-standard" successfully rolled out`

Verify namespace visible in oracle catalog (Spark SQL / JupyterHub Cell 2):
```sql
SHOW NAMESPACES IN oracle;
-- must include: e2e_testing
```

> **Production reset** — always clear after testing:
> ```bash
> kubectl set env deployment/kafka-to-iceberg-standard -n prod TARGET_NAMESPACE=
> kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod
> ```

---

### 2.5 — JupyterHub Spark Session

All Iceberg verification steps use JupyterHub with a PySpark kernel. The `oracle` catalog
is registered in the same Polaris REST catalog as `postgres` — the session setup is identical
except the catalog name.

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

#### Notebook Cell 2 — Start Spark session with `oracle` catalog

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
    .appName("oracle-e2e-verify")
    .config("spark.cores.max",           "1")
    .config("spark.executor.instances",  "1")
    .config("spark.executor.cores",      "1")
    .config("spark.executor.memory",     "2g")
    .config("spark.driver.host",         DRIVER_IP)
    .config("spark.driver.bindAddress",  DRIVER_IP)
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    # ── oracle catalog (Polaris REST) ──────────────────────────────────────
    .config("spark.sql.catalog.oracle",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.oracle.type",             "rest")
    .config("spark.sql.catalog.oracle.uri",              POLARIS_URI)
    .config("spark.sql.catalog.oracle.oauth2-server-uri",
            f"{POLARIS_URI}/v1/oauth/tokens")
    .config("spark.sql.catalog.oracle.credential",
            f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}")
    .config("spark.sql.catalog.oracle.scope",            "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.oracle.warehouse",        "IcebergCatalog")
    .config("spark.sql.catalog.oracle.rest.auth.type",   "oauth2")
    .config("spark.sql.catalog.oracle.s3.access-key-id",     s3["access_key"])
    .config("spark.sql.catalog.oracle.s3.secret-access-key", s3["secret_key"])
    .config("spark.sql.catalog.oracle.s3.endpoint",          s3["endpoint"])
    .config("spark.sql.catalog.oracle.s3.path-style-access", "true")
    .config("spark.sql.catalog.oracle.client.region",        s3.get("region","us-east-1"))
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
**Target table:** `oracle.e2e_testing.customers`  
**Test row ID:** `900100`

---

### Test 1.1 — Standard: INSERT

#### Step 1 — Note baseline row count

**Notebook Cell 3:**
```python
cnt = spark.sql(
    "SELECT COUNT(*) FROM oracle.e2e_testing.customers"
).collect()[0][0]
print(f"baseline_row_count = {cnt}")

exists = spark.sql(
    "SELECT COUNT(*) FROM oracle.e2e_testing.customers WHERE id = 900100"
).collect()[0][0]
print(f"id=900100 already_exists = {exists > 0}  ← must be False before proceeding")
```

✅ Expected:
```
baseline_row_count = <N>
id=900100 already_exists = False  ← must be False before proceeding
```

> If `already_exists = True`, a previous run was not cleaned up.
> Run the DELETE in Step 1.3 first, wait 15 s, then re-run this cell.

---

#### Step 2 — INSERT a test row into Oracle

```bash
ORACLE_POD=$(kubectl get pod -n prod -l app=oracle-xe -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it -n prod $ORACLE_POD -- \
  sqlplus CACHE_TESTING/CacheTesting2024@//localhost:1521/XEPDB1
```

```sql
-- Connected as CACHE_TESTING
INSERT INTO CUSTOMERS (ID, NAME, EMAIL, PHONE, ADDRESS, CITY, COUNTRY, CREATED_AT, UPDATED_AT)
VALUES (900100, 'E2E OracleStd', 'e2e_oracle_std@example.com', '555-0100',
        '100 Oracle Ave', 'Sydney', 'AU', SYSDATE, SYSDATE);
COMMIT;
```

✅ Expected: `1 row created.` followed by `Commit complete.`

---

#### Step 3 — Wait for pipeline propagation

LogMiner polls redo logs every ~5 s; the Spark micro-batch runs every 2 s. Allow 15 s total:

```bash
sleep 15
```

Check the streaming job processed it:

```bash
kubectl logs -n prod \
  $(kubectl get pod -n prod \
    -l app=kafka-to-iceberg,pipeline.source=oracle,pipeline.write-mode=standard \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=30s 2>&1 | grep -E "batch=|upsert|hard-delete|ERROR"
```

✅ Expected: `[oracle/customers][standard] batch=N upsert rows=1`

---

#### Step 4 — Verify INSERT in Iceberg

**Notebook Cell 4:**
```python
print("=== INSERT verify ===")
spark.sql("""
    SELECT id, name, email, city, country, snap_id, snap_timestamp
    FROM   oracle.e2e_testing.customers
    WHERE  id = 900100
""").show(truncate=False)
```

✅ Expected:
```
=== INSERT verify ===
+------+-------------+---------------------------+------+-------+-------+--------------+
|id    |name         |email                      |city  |country|snap_id|snap_timestamp|
+------+-------------+---------------------------+------+-------+-------+--------------+
|900100|E2E OracleStd|e2e_oracle_std@example.com |Sydney|AU     |...    |...           |
+------+-------------+---------------------------+------+-------+-------+--------------+
```
1 row returned. `snap_id` is a non-null BIGINT. `snap_timestamp` is within the last 30 s.

---

### Test 1.2 — Standard: UPDATE

#### Step 1 — UPDATE the test row in Oracle

```sql
-- sqlplus (CACHE_TESTING session)
UPDATE CUSTOMERS
SET    EMAIL = 'e2e_oracle_std_updated@example.com',
       CITY  = 'Melbourne',
       UPDATED_AT = SYSDATE
WHERE  ID = 900100;
COMMIT;
```

✅ Expected: `1 row updated.` → `Commit complete.`

---

#### Step 2 — Wait and verify UPDATE in Iceberg

```bash
sleep 15
kubectl logs -n prod \
  $(kubectl get pod -n prod \
    -l app=kafka-to-iceberg,pipeline.source=oracle,pipeline.write-mode=standard \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=30s 2>&1 | grep -E "batch=|upsert|hard-delete|ERROR"
```

✅ Expected: `[oracle/customers][standard] batch=N upsert rows=1`

**Notebook Cell 5:**
```python
print("=== UPDATE verify ===")
spark.sql("""
    SELECT id, name, email, city, snap_id, snap_timestamp
    FROM   oracle.e2e_testing.customers
    WHERE  id = 900100
""").show(truncate=False)
```

✅ Expected:
```
=== UPDATE verify ===
+------+-------------+----------------------------------+---------+-------+--------------+
|id    |name         |email                             |city     |snap_id|snap_timestamp|
+------+-------------+----------------------------------+---------+-------+--------------+
|900100|E2E OracleStd|e2e_oracle_std_updated@example.com|Melbourne|...    |...           |
+------+-------------+----------------------------------+---------+-------+--------------+
```
`email` = `e2e_oracle_std_updated@example.com`, `city` = `Melbourne`.  
`snap_id` differs from Step 4. `snap_timestamp` is newer.

---

### Test 1.3 — Standard: DELETE

#### Step 1 — DELETE the test row from Oracle

Oracle `CACHE_TESTING.CUSTOMERS` has FK children (`ORDER_ITEMS` → `ORDERS` → `CUSTOMERS`
and `PRODUCT_REVIEWS` → `CUSTOMERS`). Remove children first:

```sql
-- sqlplus (CACHE_TESTING session)
DELETE FROM PRODUCT_REVIEWS WHERE CUSTOMER_ID = 900100;
DELETE FROM ORDER_ITEMS WHERE ORDER_ID IN (SELECT ID FROM ORDERS WHERE CUSTOMER_ID = 900100);
DELETE FROM ORDERS WHERE CUSTOMER_ID = 900100;
DELETE FROM CUSTOMERS WHERE ID = 900100;
COMMIT;
```

✅ Expected: each DELETE prints a row count; final `Commit complete.`

---

#### Step 2 — Wait and verify hard DELETE in Iceberg

```bash
sleep 15
kubectl logs -n prod \
  $(kubectl get pod -n prod \
    -l app=kafka-to-iceberg,pipeline.source=oracle,pipeline.write-mode=standard \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=30s 2>&1 | grep -E "batch=|upsert|hard-delete|ERROR"
```

✅ Expected: `[oracle/customers][standard] batch=N hard-delete rows=1`

**Notebook Cell 6:**
```python
print("=== DELETE verify ===")
cnt = spark.sql(
    "SELECT COUNT(*) FROM oracle.e2e_testing.customers WHERE id = 900100"
).collect()[0][0]
total = spark.sql(
    "SELECT COUNT(*) FROM oracle.e2e_testing.customers"
).collect()[0][0]
print(f"id=900100 row_count   = {cnt}    (expected 0)")
print(f"total_rows_remaining = {total}  (expected baseline_row_count)")
```

✅ Expected:
```
=== DELETE verify ===
id=900100 row_count   = 0    (expected 0)
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
the row is **never physically removed** from Iceberg.  
**Target table:** `oracle.e2e_testing.customers_sd`  
**Test row ID:** `900110`

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

Verify both deployments:
```bash
kubectl get deployment -n prod | grep kafka-to-iceberg
```
**Expected:** `kafka-to-iceberg-soft-delete` shows `1/1 READY`; `kafka-to-iceberg-standard` shows `0/0`.

Confirm `TARGET_NAMESPACE` is active:
```bash
kubectl logs -n prod -l pipeline.write-mode=soft-delete --tail=15 \
  | grep -i "TARGET_NAMESPACE\|e2e_testing"
```

---

### Create the `customers_sd` table in oracle catalog

Run once in JupyterHub (Spark SQL) if the table does not yet exist:

```python
# Notebook — start a new session pointing at the oracle catalog (Cell 1 & 2 as above)
spark.sql("""
  CREATE TABLE IF NOT EXISTS oracle.e2e_testing.customers_sd (
      id             BIGINT,
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
  PARTITIONED BY (days(snap_timestamp))
""")
print("✅ oracle.e2e_testing.customers_sd ready")
```

---

### Test 2.1 — Soft Delete: INSERT

#### Step 1 — INSERT test row into Oracle

```sql
-- sqlplus (CACHE_TESTING session)
INSERT INTO CUSTOMERS (ID, NAME, EMAIL, PHONE, ADDRESS, CITY, COUNTRY, CREATED_AT, UPDATED_AT)
VALUES (900110, 'E2E OracleSoft', 'oracle_soft@example.com', '555-0110',
        '110 Soft Lane', 'Brisbane', 'AU', SYSDATE, SYSDATE);
COMMIT;
```

✅ Expected: `1 row created.` → `Commit complete.`

#### Step 2 — Wait and verify in Iceberg

```bash
sleep 15
kubectl logs -n prod -l pipeline.write-mode=soft-delete --since=30s 2>&1 \
  | grep -E "batch=|upsert|soft-delete|ERROR" | tail -10
```

✅ Expected: `[oracle/customers][soft_delete] batch=N upsert rows=1`

**Notebook Cell:**
```python
spark.sql("""
    SELECT id, email, is_deleted, deleted_at, snap_id, snap_timestamp
    FROM   oracle.e2e_testing.customers_sd
    WHERE  id = 900110
""").show(truncate=False)
```

✅ Expected: 1 row; `is_deleted = false`; `deleted_at = NULL`; `snap_id` and `snap_timestamp` populated.

---

### Test 2.2 — Soft Delete: UPDATE

#### Step 1 — UPDATE the row

```sql
-- sqlplus (CACHE_TESTING session)
UPDATE CUSTOMERS
SET    EMAIL = 'oracle_soft_updated@example.com',
       CITY  = 'Perth',
       UPDATED_AT = SYSDATE
WHERE  ID = 900110;
COMMIT;
```

#### Step 2 — Wait and verify UPDATE

```bash
sleep 15
```

**Notebook Cell:**
```python
spark.sql("""
    SELECT id, email, city, is_deleted, deleted_at
    FROM   oracle.e2e_testing.customers_sd
    WHERE  id = 900110
""").show(truncate=False)
```

✅ Expected: `email = 'oracle_soft_updated@example.com'`; `city = 'Perth'`; `is_deleted = false`; `deleted_at = NULL`.

---

### Test 2.3 — Soft Delete: DELETE

#### Step 1 — DELETE the row from Oracle

```sql
-- sqlplus (CACHE_TESTING session)
-- Remove FK children first (same pattern as Section 1)
DELETE FROM PRODUCT_REVIEWS WHERE CUSTOMER_ID = 900110;
DELETE FROM ORDER_ITEMS WHERE ORDER_ID IN (SELECT ID FROM ORDERS WHERE CUSTOMER_ID = 900110);
DELETE FROM ORDERS WHERE CUSTOMER_ID = 900110;
DELETE FROM CUSTOMERS WHERE ID = 900110;
COMMIT;
```

#### Step 2 — Verify soft delete in Iceberg

```bash
sleep 15
```

**Notebook Cell:**
```python
spark.sql("""
    SELECT id, email, is_deleted, deleted_at
    FROM   oracle.e2e_testing.customers_sd
    WHERE  id = 900110
""").show(truncate=False)
```

✅ Expected: Row **still present**; `is_deleted = true`; `deleted_at` is a non-null TIMESTAMP within the last 30 seconds.

#### Step 3 — Query all soft-deleted rows

```python
spark.sql("""
    SELECT id, email, deleted_at
    FROM   oracle.e2e_testing.customers_sd
    WHERE  is_deleted = true
    ORDER BY deleted_at DESC
    LIMIT 20
""").show(truncate=False)
```

✅ Expected: `id = 900110` appears in results.

#### Step 4 — Confirm the row count did NOT decrease

```python
after_cnt = spark.sql(
    "SELECT COUNT(*) FROM oracle.e2e_testing.customers_sd WHERE id = 900110"
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
| 2.1  | INSERT    | `batch=N upsert rows=1`       | 1 row, `is_deleted=false` |
| 2.2  | UPDATE    | `batch=N upsert rows=1`       | email/city updated, `is_deleted=false` |
| 2.3  | DELETE    | `batch=N soft-delete rows=1`  | row retained, `is_deleted=true`, `deleted_at` populated |

---

## 5. Section 3 — History Tracking Mode Tests

**Write mode:** `history_tracking` — every INSERT / UPDATE / DELETE appends a new row to the
`_hist` table. No rows are overwritten; the full mutation log is preserved.  
**Target table:** `oracle.e2e_testing.customers_hist`  
**Test row ID:** `900120`

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

### Create the `customers_hist` table in oracle catalog

Run once in JupyterHub if the table does not yet exist:

```python
spark.sql("""
  CREATE TABLE IF NOT EXISTS oracle.e2e_testing.customers_hist (
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
print("✅ oracle.e2e_testing.customers_hist ready")
```

---

### Test 3.1 — History: INSERT

#### Step 1 — INSERT test row

```sql
-- sqlplus (CACHE_TESTING session)
INSERT INTO CUSTOMERS (ID, NAME, EMAIL, PHONE, ADDRESS, CITY, COUNTRY, CREATED_AT, UPDATED_AT)
VALUES (900120, 'E2E OracleHist', 'oracle_hist@example.com', '555-0120',
        '120 History Rd', 'Adelaide', 'AU', SYSDATE, SYSDATE);
COMMIT;
```

✅ Expected: `1 row created.` → `Commit complete.`

#### Step 2 — Wait and verify in `_hist` table

```bash
sleep 15
kubectl logs -n prod -l pipeline.write-mode=history-tracking --since=30s 2>&1 \
  | grep -E "batch=|append|ERROR" | tail -10
```

✅ Expected: `[oracle/customers][history_tracking] batch=N append rows=1`

**Notebook Cell:**
```python
print("=== INSERT — history verify ===")
spark.sql("""
    SELECT id, _change_type, _change_ts,
           before_id, before_email,
           after_id, after_email,
           snap_id, snap_timestamp
    FROM   oracle.e2e_testing.customers_hist
    WHERE  after_id = 900120
    ORDER BY _change_ts
""").show(truncate=False)
```

✅ Expected: 1 row; `_change_type = 'INSERT'`; all `before_*` columns are NULL;
`after_id = 900120`; `after_email = 'oracle_hist@example.com'`.

---

### Test 3.2 — History: UPDATE

#### Step 1 — UPDATE the row

```sql
-- sqlplus (CACHE_TESTING session)
UPDATE CUSTOMERS
SET    EMAIL = 'oracle_hist_updated@example.com',
       CITY  = 'Darwin',
       UPDATED_AT = SYSDATE
WHERE  ID = 900120;
COMMIT;
```

#### Step 2 — Wait and verify UPDATE row in `_hist`

```bash
sleep 15
```

**Notebook Cell:**
```python
print("=== UPDATE — history verify ===")
spark.sql("""
    SELECT id, _change_type, _change_ts,
           before_email, before_city,
           after_email,  after_city
    FROM   oracle.e2e_testing.customers_hist
    WHERE  after_id = 900120
       OR before_id = 900120
    ORDER BY _change_ts
""").show(truncate=False)
```

✅ Expected: 2 rows:
- Row 1: `_change_type = 'INSERT'`, `before_email = NULL`, `after_email = 'oracle_hist@example.com'`
- Row 2: `_change_type = 'UPDATE'`, `before_email = 'oracle_hist@example.com'`, `after_email = 'oracle_hist_updated@example.com'`, `before_city = 'Adelaide'`, `after_city = 'Darwin'`

---

### Test 3.3 — History: DELETE

#### Step 1 — DELETE the row from Oracle

```sql
-- sqlplus (CACHE_TESTING session)
DELETE FROM PRODUCT_REVIEWS WHERE CUSTOMER_ID = 900120;
DELETE FROM ORDER_ITEMS WHERE ORDER_ID IN (SELECT ID FROM ORDERS WHERE CUSTOMER_ID = 900120);
DELETE FROM ORDERS WHERE CUSTOMER_ID = 900120;
DELETE FROM CUSTOMERS WHERE ID = 900120;
COMMIT;
```

#### Step 2 — Wait and verify DELETE row in `_hist`

```bash
sleep 15
```

**Notebook Cell:**
```python
print("=== DELETE — history verify ===")
spark.sql("""
    SELECT id, _change_type, _change_ts,
           before_email, before_city,
           after_email,  after_city
    FROM   oracle.e2e_testing.customers_hist
    WHERE  after_id = 900120
       OR before_id = 900120
    ORDER BY _change_ts
""").show(truncate=False)
```

✅ Expected: 3 rows; the third has:
- `_change_type = 'DELETE'`
- `before_email = 'oracle_hist_updated@example.com'`
- `before_city = 'Darwin'`
- all `after_*` columns are NULL

---

### Test 3.4 — Full audit trail for a single customer

```python
print("=== Full audit trail for id=900120 ===")
spark.sql("""
    SELECT _change_type, _change_ts,
           before_email, after_email,
           before_city,  after_city,
           snap_id
    FROM   oracle.e2e_testing.customers_hist
    WHERE  after_id = 900120
       OR before_id = 900120
    ORDER BY _change_ts ASC
""").show(truncate=False)
```

✅ Expected: 3 rows in chronological order — **INSERT → UPDATE → DELETE** — forming a
complete Oracle LogMiner-sourced audit trail in Iceberg.

---

### Test 3.5 — Confirm history rows are immutable (append-only)

```python
# Count must stay at exactly 3 even after repeated queries
cnt = spark.sql(
    "SELECT COUNT(*) FROM oracle.e2e_testing.customers_hist WHERE after_id = 900120 OR before_id = 900120"
).collect()[0][0]
print(f"history_row_count = {cnt}  (expected 3)")
```

✅ Expected: `history_row_count = 3`

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
| 3.2  | UPDATE    | `batch=N append rows=1` | 2 | UPDATE — before/after populated |
| 3.3  | DELETE    | `batch=N append rows=1` | 3 | DELETE — `after_*` all NULL |
| 3.4  | Full trail | — | 3 | INSERT → UPDATE → DELETE in order |
| 3.5  | Immutability | — | 3 | count unchanged after re-query |

---

## 6. Expected Results Summary

### Oracle → Kafka → Iceberg: End-to-End Verification Matrix

| Section | Mode | Operation | Debezium Source | Kafka Topic | Iceberg Table | Pass Condition |
|---------|------|-----------|-----------------|-------------|---------------|----------------|
| 1.1 | standard | INSERT | LogMiner SCN | `oracle.cache_testing.CUSTOMERS` | `oracle.e2e_testing.customers` | 1 row, correct values, `snap_id` non-null |
| 1.2 | standard | UPDATE | LogMiner SCN | `oracle.cache_testing.CUSTOMERS` | `oracle.e2e_testing.customers` | row overwritten, new `snap_id`, newer `snap_timestamp` |
| 1.3 | standard | DELETE | LogMiner SCN | `oracle.cache_testing.CUSTOMERS` | `oracle.e2e_testing.customers` | `COUNT(*) WHERE id=900100 = 0` |
| 2.1 | soft_delete | INSERT | LogMiner SCN | `oracle.cache_testing.CUSTOMERS` | `oracle.e2e_testing.customers_sd` | `is_deleted=false`, `deleted_at=NULL` |
| 2.2 | soft_delete | UPDATE | LogMiner SCN | `oracle.cache_testing.CUSTOMERS` | `oracle.e2e_testing.customers_sd` | new values, `is_deleted=false` |
| 2.3 | soft_delete | DELETE | LogMiner SCN | `oracle.cache_testing.CUSTOMERS` | `oracle.e2e_testing.customers_sd` | row retained, `is_deleted=true`, `deleted_at` set |
| 3.1 | history_tracking | INSERT | LogMiner SCN | `oracle.cache_testing.CUSTOMERS` | `oracle.e2e_testing.customers_hist` | 1 `_hist` row, `_change_type=INSERT` |
| 3.2 | history_tracking | UPDATE | LogMiner SCN | `oracle.cache_testing.CUSTOMERS` | `oracle.e2e_testing.customers_hist` | 2nd `_hist` row, `_change_type=UPDATE` |
| 3.3 | history_tracking | DELETE | LogMiner SCN | `oracle.cache_testing.CUSTOMERS` | `oracle.e2e_testing.customers_hist` | 3rd `_hist` row, `_change_type=DELETE` |

### Key Oracle-Specific Notes

| Concern | Detail |
|---------|--------|
| LogMiner latency | Redo logs are polled every ~5 s; allow 15 s propagation vs 10 s for Postgres |
| Topic naming | Topics use **UPPERCASE** table names: `oracle.cache_testing.CUSTOMERS` |
| Iceberg naming | Streaming job lowercases the last segment: `oracle.e2e_testing.customers` |
| FK constraints | `PRODUCT_REVIEWS`, `ORDER_ITEMS`, `ORDERS` must be cleaned before deleting a customer |
| Null columns | Oracle `NULL` for unset optional fields matches Iceberg `null` correctly |
| COMMIT required | Every DML must be followed by an explicit `COMMIT;` — LogMiner only reads committed transactions |

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

# 3. Verify oracle CDC connector still running
curl -s http://192.168.1.54:30083/connectors/oracle-cache-testing-cdc/status \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])"
# Expected: RUNNING
```

---

## 7. Session Log

> Newest entry first. Entries are immutable.

---

### Session — 2026-09-21

#### Target table

`oracle.cache_testing.customers_sd` (`ora_lakehouse` / Polaris REST)  
Partitioned by `hours(snap_timestamp)`, `bucket(16, CUSTOMER_ID)`  
Deployment: `kafka-to-iceberg-oracle-soft-delete` (1/1 Running, `TARGET_NAMESPACE=` empty → routes by topic namespace)

#### Table creation

Created via `spark.sql(CREATE TABLE IF NOT EXISTS oracle.cache_testing.customers_sd …)` executed
inside the `kafka-to-iceberg-oracle-soft-delete` pod.  
Schema mirrors `oracle.cache_testing.customers` exactly, with two additional soft-delete columns:

| Column | Type | Notes |
|---|---|---|
| `CUSTOMER_ID` | BIGINT | PK (bucket partition key) |
| `FIRST_NAME` | STRING | |
| `LAST_NAME` | STRING | |
| `EMAIL` | STRING | |
| `PHONE` | STRING | |
| `CITY` | STRING | |
| `COUNTRY_CODE` | STRING | |
| `CREDIT_LIMIT` | BIGINT | |
| `IS_ACTIVE` | STRING | |
| `TIER` | STRING | |
| `CREATED_AT` | TIMESTAMP | |
| `UPDATED_AT` | TIMESTAMP | |
| `is_deleted` | BOOLEAN | ← soft-delete flag |
| `deleted_at` | TIMESTAMP | ← set by pipeline on DELETE event |
| `snap_id` | BIGINT | pipeline batch ID |
| `snap_timestamp` | TIMESTAMP | partition key (hourly) |

#### Test row

`CUSTOMER_ID = 9010010` — inserted as `SYSDBA` (seed data owner) since CDC user lacks INSERT privilege.

---

#### Test 2.1 — INSERT

**Oracle DML:**
```sql
INSERT INTO CACHE_TESTING.CUSTOMERS
  (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE,
   CITY, COUNTRY_CODE, CREDIT_LIMIT, IS_ACTIVE, TIER, CREATED_AT, UPDATED_AT)
VALUES
  (9010010, 'SoftDel', 'TestUser', 'oracle_sd_test@example.com', '555-9010',
   'Sydney', 'AU', 5000, 'Y', 'GOLD', SYSTIMESTAMP, SYSTIMESTAMP);
COMMIT;
```

**Pipeline log:**
```
[oracle/customers_sd][soft_delete] batch=1 upsert rows=1
```

**Iceberg result:**

| CUSTOMER_ID | FIRST_NAME | EMAIL | CITY | TIER | is_deleted | deleted_at | snap_id | snap_timestamp |
|---|---|---|---|---|---|---|---|---|
| 9010010 | SoftDel | oracle_sd_test@example.com | Sydney | GOLD | **false** | **NULL** | 10000000 | 2026-09-21 00:41:23 |

✅ **PASS** — row landed with `is_deleted=false`, `deleted_at=NULL`, `snap_id` and `snap_timestamp` populated.

---

#### Test 2.2 — UPDATE

**Oracle DML:**
```sql
UPDATE CACHE_TESTING.CUSTOMERS
SET    EMAIL='oracle_sd_updated@example.com', CITY='Melbourne',
       TIER='PLATINUM', CREDIT_LIMIT=9500, UPDATED_AT=SYSTIMESTAMP
WHERE  CUSTOMER_ID = 9010010;
COMMIT;
```

**Pipeline log:**
```
[oracle/customers_sd][soft_delete] batch=2 upsert rows=1
```

**Iceberg result:**

| CUSTOMER_ID | EMAIL | CITY | TIER | CREDIT_LIMIT | is_deleted | deleted_at |
|---|---|---|---|---|---|---|
| 9010010 | oracle_sd_updated@example.com | **Melbourne** | **PLATINUM** | **9500** | **false** | **NULL** |

✅ **PASS** — email, city, tier, credit_limit all updated in-place. `is_deleted` unchanged.

---

#### Test 2.3 — DELETE (soft)

**Oracle DML:**
```sql
DELETE FROM CACHE_TESTING.CUSTOMERS WHERE CUSTOMER_ID = 9010010;
COMMIT;
```

Oracle source count after delete: `0 rows`

**Pipeline log:**
```
[oracle/customers_sd][soft_delete] batch=3 soft-delete rows=1
```

**Iceberg result:**

| CUSTOMER_ID | EMAIL | CITY | TIER | is_deleted | deleted_at | snap_id |
|---|---|---|---|---|---|---|
| 9010010 | oracle_sd_updated@example.com | Melbourne | PLATINUM | **true** | **2026-09-21 00:45:15** | 10000000 |

Row count for `CUSTOMER_ID=9010010`: **1** (row retained — not physically deleted)

✅ **PASS** — row retained in Iceberg, `is_deleted=true`, `deleted_at` set to deletion timestamp.

---

#### Section 2 summary

| Test | Op | Kafka log | Iceberg | Result |
|---|---|---|---|---|
| 2.1 | INSERT | `batch=1 upsert rows=1` | 1 row, `is_deleted=false`, `deleted_at=NULL` | ✅ PASS |
| 2.2 | UPDATE | `batch=2 upsert rows=1` | values updated, `is_deleted=false` | ✅ PASS |
| 2.3 | DELETE | `batch=3 soft-delete rows=1` | row retained, `is_deleted=true`, `deleted_at` populated | ✅ PASS |

**E2E latency:** ~15 s Oracle COMMIT → Iceberg (LogMiner poll interval)  
**Deployment:** `kafka-to-iceberg-oracle-soft-delete` — no `TARGET_NAMESPACE` override needed; topic namespace `cache_testing` routes directly.
