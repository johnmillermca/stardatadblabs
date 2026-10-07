# Runbook 30 — CDC Pipeline End-to-End Test Runbook

**Status:** Operational
**Namespace:** `prod`
**Estimated duration:** 45–90 minutes (full suite)

> **Source-specific deep-dive runbooks:**
> - [Runbook 31 — Oracle → Kafka → Iceberg E2E](runbook-31-oracle-kafka-iceberg-e2e-testing.md) *(standard · soft-delete · history tracking)*
> - [Runbook 32 — MongoDB → Kafka → Iceberg E2E](runbook-32-mongodb-kafka-iceberg-e2e-testing.md) *(standard · soft-delete · history tracking)*

---

## Table of Contents

1. [Prerequisites Check](#1-prerequisites-check)
2. [Section 1 — Standard Mode Tests (SCD Type 0)](#2-section-1--standard-mode-tests-scd-type-0)
   - 1.1 PostgreSQL (full depth) · 1.2 Oracle · 1.3 MongoDB
3. [Section 2 — Soft Delete Mode Tests](#3-section-2--soft-delete-mode-tests)
4. [Section 3 — History Tracking Mode Tests](#4-section-3--history-tracking-mode-tests)
5. [Section 5 — snap_id and snap_timestamp Validation](#5-section-5--snap_id-and-snap_timestamp-validation)
6. [Section 6 — Multi-Source Validation](#6-section-6--multi-source-validation)
7. [Section 7 — Schema Evolution (DDL) Tests](#7-section-7--schema-evolution-ddl-tests)
8. [Section 8 — Peak-Hour Simulation](#8-section-8--peak-hour-simulation)
9. [Expected Results Summary](#9-expected-results-summary)

---

## How the Pipeline Routes into `e2e_testing`

The Spark Structured Streaming job (`05_kafka_to_iceberg_streaming.py`) consumes
from **all three** Debezium connectors simultaneously:

| Debezium connector | Kafka topics produced | Iceberg catalog |
|---|---|---|
| `postgres-cache-testing-cdc` | `postgres.cache_testing.customers`, `…orders`, `…products`, `…product_reviews` | `postgres` |
| `oracle-cache-testing-cdc` | `oracle.cache_testing.CUSTOMERS`, `…ORDERS`, `…PRODUCTS`, `…ORDER_ITEMS`, `…INVENTORY_EVENTS`, `…PRODUCT_REVIEWS` | `oracle` |
| `mongodb-cache-testing-cdc` | `mongodb.cache_testing.customers`, `…products` | `mongodb` |

By default the streaming job derives the **Iceberg namespace** from the topic's second segment
(`cache_testing`), writing to `postgres.cache_testing.customers` etc.
For all E2E tests, set **`TARGET_NAMESPACE=e2e_testing`** — the pipeline then redirects every topic
from every source into the `e2e_testing` namespace instead:

```
postgres.cache_testing.customers  →  postgres.e2e_testing.customers
oracle.cache_testing.CUSTOMERS    →  oracle.e2e_testing.customers
mongodb.cache_testing.customers   →  mongodb.e2e_testing.customers
```

### Activate E2E replication for all three sources

```bash
# Set TARGET_NAMESPACE and restart — applies to ALL three catalogs at once
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod

# Verify the namespace was created in all three catalogs (Spark SQL)
SHOW NAMESPACES IN postgres;   -- should include e2e_testing
SHOW NAMESPACES IN oracle;     -- should include e2e_testing
SHOW NAMESPACES IN mongodb;    -- should include e2e_testing
```

> **Production reset** — always clear `TARGET_NAMESPACE` after testing:
> ```bash
> kubectl set env deployment/kafka-to-iceberg-standard -n prod TARGET_NAMESPACE=
> kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod
> ```

---

## Iceberg Test Table Layout

All E2E test tables live in **one shared namespace per catalog**: `e2e_testing`.
The `e2e_testing` namespace is identical across all three catalogs — the table names and schemas
are the same regardless of source.

### Core mode tables (Sections 1–3)

| Iceberg Table | Catalog | Write Mode | Source DB |
|---|---|---|---|
| `postgres.e2e_testing.customers` | postgres | standard | PostgreSQL `cache_testing` |
| `postgres.e2e_testing.orders` | postgres | standard | PostgreSQL `cache_testing` |
| `postgres.e2e_testing.products` | postgres | standard | PostgreSQL `cache_testing` |
| `postgres.e2e_testing.customers` (soft_delete) | postgres | soft_delete | PostgreSQL `cache_testing` |
| `postgres.e2e_testing.customers` (hist) | postgres | history_tracking | PostgreSQL `cache_testing` |
| `oracle.e2e_testing.customers` | oracle | standard | Oracle XEPDB1 `CACHE_TESTING` |
| `oracle.e2e_testing.orders` | oracle | standard | Oracle XEPDB1 `CACHE_TESTING` |
| `oracle.e2e_testing.products` | oracle | standard | Oracle XEPDB1 `CACHE_TESTING` |
| `mongodb.e2e_testing.customers` | mongodb | standard | MongoDB `cache_testing` |
| `mongodb.e2e_testing.products` | mongodb | standard | MongoDB `cache_testing` |

> The table **name** matches the Kafka topic's last segment lowercased:
> `postgres.cache_testing.customers` → `postgres.e2e_testing.customers`
> `oracle.cache_testing.CUSTOMERS` → `oracle.e2e_testing.customers`

**Source table columns** (PostgreSQL / Oracle `cache_testing.customers`):
`id`, `name`, `email`, `phone`, `address`, `city`, `country`, `created_at`, `updated_at`

**Test row ID range**: 900001–909999 — high enough to never collide with production data.

---

## 1. Prerequisites Check

Run these checks before executing any test section. All checks must pass.

### 1.1 — Required CLI Tools

```bash
kubectl version --client
curl    --version | head -1
jq      --version
psql    --version
sqlplus -v          2>/dev/null || echo "sqlplus not found — Oracle tests require sqlplus"
mongosh --version   2>/dev/null || echo "mongosh not found — MongoDB tests require exec into mongo pod"
```

**Expected output (example — versions will differ):**
```
Client Version: v1.31.14
curl 8.x.x ...
jq-1.7.1          ← on RHEL/EL systems jq prints "jq-X.Y.Z" (no space) — this is correct
psql (PostgreSQL) 16.x
sqlplus not found — Oracle tests require sqlplus
mongosh not found — MongoDB tests require exec into mongo pod
```

**If `sqlplus` is missing:** Oracle DML in Sections 1–3 must be run inside the Oracle pod:
```bash
ORACLE_POD=$(kubectl get pod -n prod -l app=oracle-xe -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it $ORACLE_POD -n prod -- sqlplus CACHE_TESTING/CacheTesting2024@//localhost:1521/XEPDB1
```

**If `mongosh` is missing:** MongoDB DML in Sections 1–3 must be run inside the MongoDB pod:
```bash
kubectl exec -it mongodb-0 -n prod -- mongosh mongodb://localhost:27017/cache_testing
```

> **Note:** `--short` was removed from `kubectl version` in v1.28+. Use `kubectl version --client` instead.

> **PostgreSQL access from master:** The cluster-internal hostname `postgresql.prod.svc.cluster.local` is not
> resolvable from the master node. Use the NodePort instead:
> `PGPASSWORD=vb2dJms4c1fKi0uYD87Vv4YpCsZQJm1f psql -h 192.168.1.50 -p 30532 -U rbac -d cache_testing`
>
> **`rbac` write permissions:** By default `rbac` is SELECT-only. The following grant was applied once
> to enable DML for e2e testing (idempotent — safe to re-run if permissions are ever reset):
> ```bash
> PGPOD=$(kubectl get pod -n prod -l app=postgresql -o jsonpath='{.items[0].metadata.name}')
> kubectl exec -n prod $PGPOD -- psql -U postgres -d cache_testing -c \
>   "GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO rbac;
>    GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO rbac;"
> ```

### 1.2 — All Debezium Connectors RUNNING

```bash
for CONNECTOR in postgres-cache-testing-cdc oracle-cache-testing-cdc oracle-tpcds-cdc mongodb-cache-testing-cdc; do
  STATE=$(curl -s http://192.168.1.54:30083/connectors/${CONNECTOR}/status \
    | jq -r '.connector.state')
  echo "${CONNECTOR}: ${STATE}"
done
```

**Expected output:**
```
postgres-cache-testing-cdc: RUNNING
oracle-cache-testing-cdc: RUNNING
oracle-tpcds-cdc: RUNNING
mongodb-cache-testing-cdc: RUNNING
```

If any connector is not RUNNING, restart it:
```bash
curl -X POST http://192.168.1.54:30083/connectors/<connector-name>/restart
sleep 10
curl -s http://192.168.1.54:30083/connectors/<connector-name>/status | jq .connector.state
```

### 1.3 — kafka-to-iceberg-standard Deployment Active

```bash
kubectl get deployment kafka-to-iceberg-standard -n prod \
  -o jsonpath='{.spec.replicas} replicas specified, {.status.readyReplicas} ready{"\n"}'
```

**Expected:** `1 replicas specified, 1 ready`

If not running:
```bash
kubectl scale deployment kafka-to-iceberg-standard         -n prod --replicas=1
kubectl scale deployment kafka-to-iceberg-soft-delete      -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-history-tracking -n prod --replicas=0
kubectl rollout status deployment/kafka-to-iceberg-standard -n prod
```

### 1.4 — TARGET_NAMESPACE set to `e2e_testing`

The streaming job must have `TARGET_NAMESPACE=e2e_testing` so all three Debezium sources
(PostgreSQL, Oracle, MongoDB) write into the `e2e_testing` Iceberg namespace.

```bash
# Check current value
kubectl get deployment kafka-to-iceberg-standard -n prod \
  -o jsonpath='{.spec.template.spec.containers[0].env}' | python3 -m json.tool \
  | grep -A1 TARGET_NAMESPACE

# Set it (idempotent — safe to re-run)
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

**Expected:** Log line: `TARGET_NAMESPACE='e2e_testing' — ensuring override namespace in all active catalogs.`

```bash
kubectl logs -n prod -l app=kafka-to-iceberg,pipeline.write-mode=standard --tail=30 | grep -i "e2e_testing\|TARGET_NAMESPACE"
```

Verify Iceberg namespaces were pre-created across all three catalogs:

```sql
-- Spark SQL
SHOW NAMESPACES IN postgres;
SHOW NAMESPACES IN oracle;
SHOW NAMESPACES IN mongodb;
-- Expected: e2e_testing appears in all three
```

### 1.5 — Streaming Job Healthy (all three sources active)

```bash
kubectl logs -n prod -l app=kafka-to-iceberg,pipeline.write-mode=standard --tail=30 | grep -E "Streaming query started|Batch|Error|Exception"
```

**Expected:**
- Three `Streaming query started` lines — one per source: `cdc-postgres-standard`, `cdc-oracle-standard`, `cdc-mongodb-standard`
- Recent `Batch N` completion lines for each source
- No `Error` or `Exception` lines

```bash
# Confirm all three Debezium connectors are still RUNNING
for C in postgres-cache-testing-cdc oracle-cache-testing-cdc mongodb-cache-testing-cdc; do
  STATE=$(curl -s http://192.168.1.54:30083/connectors/${C}/status | python3 -c \
    "import sys,json; print(json.load(sys.stdin)['connector']['state'])")
  echo "${C}: ${STATE}"
done
# Expected: RUNNING RUNNING RUNNING
```

---

## 0. Known Issues & Fixes

### ClassCastException: `List$SerializationProxy → Seq` on every Postgres micro-batch

**Symptom:** Every micro-batch for the `cdc-postgres-standard` query (and the other two
sources) fails with:

```
java.lang.ClassCastException: scala.collection.immutable.List$SerializationProxy
  cannot be cast to scala.collection.Seq
```

**Root cause (two compounding issues confirmed):**

1. **`KryoSerializer` active via `spark-defaults.conf` in the image** — The
   `spark-gluten-velox` image bakes `spark.serializer = KryoSerializer` into
   `/opt/spark/conf/spark-defaults.conf` (needed for Gluten/Velox + JDBC batch jobs).
   The original `_build_spark()` code attempted to avoid Kryo by simply *not setting*
   the serializer — but `spark-defaults.conf` is loaded before `SparkConf` in
   `SparkSession.builder`, so the image-level default always wins.  Kafka's
   `DataSourceV2` / `DataSourceRDDPartition` uses Java serialisation for its internal
   partition state; Kryo cannot deserialise `List$SerializationProxy` as a `Seq`,
   crashing every task.

2. **`spark.jars.packages` triggered a Maven/Ivy download at runtime** — The previous
   code set `spark.jars.packages = org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`.
   On a standalone cluster the downloaded jar only lands in the driver pod's
   `/root/.ivy2/` — executor JVMs on worker nodes never receive it, causing
   `ClassNotFoundException` on the Kafka `DataSource` on the first micro-batch if the
   download path happens to be taken.

**Fix applied (image `spark-gluten-velox:3.5.1-14`):**

- [`_build_spark()`](../../../docker/spark-gluten-velox/scripts/05_kafka_to_iceberg_streaming.py)
  now explicitly sets `spark.serializer = JavaSerializer` so the streaming session
  overrides the cluster default without affecting any other Gluten/JDBC job.
- `spark.jars.packages` removed; the three required JARs
  (`spark-sql-kafka-0-10_2.12-3.5.1.jar`, `kafka-clients-3.4.1.jar`,
  `spark-token-provider-kafka-0-10_2.12-3.5.1.jar`) are now baked into the image at
  `/opt/spark/jars/` and are present on every driver *and* executor classpath
  without any network access at runtime.

**Deploy steps:**

```bash
# 1. Rebuild and push the image (from repo root)
bash docker/spark-gluten-velox/build-and-push.sh   # tags as :3.5.1-14

# 2. Apply the updated ConfigMap (already updated in git)
kubectl apply -f manifests/cdc-batch-pipeline/kafka-to-iceberg-streaming.yaml

# 3. Rolling restart to pick up the new image
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod

# 4. Confirm no ClassCastException in first 5 batches
kubectl logs -n prod -l app=kafka-to-iceberg,pipeline.write-mode=standard --tail=60 \
  | grep -E "Batch [0-9]+|ClassCast|Exception|ERROR"
```

**Expected after fix:** `Batch 0`, `Batch 1`, … appear for all three sources with no
`ClassCastException` lines.

---

## 2. Section 1 — Standard Mode Tests (SCD Type 0)

Confirm `kafka-to-iceberg-postgres-standard` is the only active deployment (replicas=1)
before starting.  With `TARGET_NAMESPACE=""` (production default) data lands in the
**source namespace** — `cache_testing` — not `e2e_testing`.

Table routing with default config:
- Kafka topic `postgres.cache_testing.customers` → Iceberg **`postgres.cache_testing.customers`**

---

### ⚡ How to run Iceberg queries — JupyterHub

All Iceberg verification steps in this section use **JupyterHub** with a PySpark kernel.
The `postgres` catalog is a Polaris REST catalog that requires OAuth credentials — the
notebook fetches them from OpenBao automatically.

> **Rules:**
> - Run cells **top-to-bottom** on every new session — the kernel loses variables on restart.
> - Always run the **stop cell last** to release the 1 core this session holds on the cluster.
> - Never leave the session idle — it blocks the core from the streaming job.

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

Get a fresh root token from any terminal with `kubectl`:

```bash
kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d && echo
```

Paste the token into the cell, then **Shift+Enter**:

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

#### Notebook Cell 2 — Start Spark session

```python
from pyspark.sql import SparkSession

# Stop any stale session first
_s = SparkSession.getActiveSession()
if _s:
    _s.stop()
    print("Stopped stale session")

DRIVER_IP   = os.environ.get("SPARK_LOCAL_IP", __import__("socket").gethostbyname(__import__("socket").gethostname()))
POLARIS_URI = "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog"

spark = (
    SparkSession.builder
    .master("spark://192.168.1.50:30777")
    .appName("e2e-verify")
    # ── resource cap — 1 core max so the streaming job is never starved ──
    .config("spark.cores.max",           "1")
    .config("spark.executor.instances",  "1")
    .config("spark.executor.cores",      "1")
    .config("spark.executor.memory",     "2g")
    .config("spark.driver.host",         DRIVER_IP)
    .config("spark.driver.bindAddress",  DRIVER_IP)
    # ── Iceberg extensions ────────────────────────────────────────────────
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    # ── postgres catalog (Polaris REST) ───────────────────────────────────
    .config("spark.sql.catalog.postgres",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.postgres.type",             "rest")
    .config("spark.sql.catalog.postgres.uri",              POLARIS_URI)
    .config("spark.sql.catalog.postgres.oauth2-server-uri",
            f"{POLARIS_URI}/v1/oauth/tokens")
    .config("spark.sql.catalog.postgres.credential",
            f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}")
    .config("spark.sql.catalog.postgres.scope",            "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.postgres.warehouse",        "IcebergCatalog")
    .config("spark.sql.catalog.postgres.rest.auth.type",   "oauth2")
    .config("spark.sql.catalog.postgres.s3.access-key-id",     s3["access_key"])
    .config("spark.sql.catalog.postgres.s3.secret-access-key", s3["secret_key"])
    .config("spark.sql.catalog.postgres.s3.endpoint",          s3["endpoint"])
    .config("spark.sql.catalog.postgres.s3.path-style-access", "true")
    .config("spark.sql.catalog.postgres.client.region",        s3.get("region","us-east-1"))
    # ── S3A hadoop layer ──────────────────────────────────────────────────
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

### Test 1.1 — PostgreSQL Standard Replication (INSERT / UPDATE / DELETE)

**`customers` table schema in PostgreSQL:**
```
id, name, email, phone, address, tier, created_at, updated_at
```
> `city` and `country` do **not** exist — this cluster uses `address` (free-text) and `tier`.

**Iceberg target:** `postgres.cache_testing.customers`
**Test row ID:** `900001` (safe range — no production data uses IDs ≥ 900000)

---

#### Step 1 — Note baseline row count

**Notebook Cell 3:**
```python
cnt = spark.sql(
    "SELECT COUNT(*) FROM postgres.cache_testing.customers"
).collect()[0][0]
print(f"baseline_row_count = {cnt}")

exists = spark.sql(
    "SELECT COUNT(*) FROM postgres.cache_testing.customers WHERE id = 900001"
).collect()[0][0]
print(f"id=900001 already_exists = {exists > 0}  ← must be False before proceeding")
```

✅ Expected:
```
baseline_row_count = <N>
id=900001 already_exists = False  ← must be False before proceeding
```

> If `already_exists = True`, a previous test run was not cleaned up.
> Run the DELETE in Step 7 first, wait 10 s, then re-run this cell.

---

#### Step 2 — INSERT a test row into PostgreSQL

Run from any terminal with `kubectl` (or from the master node):

```bash
PGPASSWORD=vb2dJms4c1fKi0uYD87Vv4YpCsZQJm1f \
  psql -h 192.168.1.50 -p 30532 -U rbac -d cache_testing -c \
  "INSERT INTO customers (id, name, email, phone, address, tier)
   VALUES (900001, 'E2E TestUser', 'e2e_test@example.com', '555-0000', '1 Test St', 'standard')
   RETURNING id, name, email, tier;"
```

✅ Expected:
```
  id   |     name     |        email         |   tier
-------+--------------+----------------------+----------
900001 | E2E TestUser | e2e_test@example.com | standard
INSERT 0 1
```

---

#### Step 3 — Wait for pipeline propagation

The streaming job triggers every 2 seconds. Wait 10 seconds to be safe:

```bash
sleep 10
```

Then check the streaming job processed it — look for `batch=N upsert rows=1`:

```bash
kubectl logs -n prod \
  $(kubectl get pod -n prod -l app=kafka-to-iceberg,pipeline.source=postgres,pipeline.write-mode=standard \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=20s 2>&1 | grep -E "batch=|upsert|hard-delete|ERROR"
```

✅ Expected: `[postgres/customers][standard] batch=N upsert rows=1`

---

#### Step 4 — Verify INSERT in Iceberg

**Notebook Cell 4:**
```python
print("=== INSERT verify ===")
spark.sql("""
    SELECT id, name, email, tier, snap_id, snap_timestamp
    FROM   postgres.cache_testing.customers
    WHERE  id = 900001
""").show(truncate=False)
```

✅ Expected:
```
=== INSERT verify ===
+------+------------+--------------------+--------+-------+--------------+
|id    |name        |email               |tier    |snap_id|snap_timestamp|
+------+------------+--------------------+--------+-------+--------------+
|900001|E2E TestUser|e2e_test@example.com|standard|...    |...           |
+------+------------+--------------------+--------+-------+--------------+
```
1 row returned. `snap_id` is a non-null BIGINT. `snap_timestamp` is within the last 30 s.

---

#### Step 5 — UPDATE the test row in PostgreSQL

```bash
PGPASSWORD=vb2dJms4c1fKi0uYD87Vv4YpCsZQJm1f \
  psql -h 192.168.1.50 -p 30532 -U rbac -d cache_testing -c \
  "UPDATE customers
   SET email = 'e2e_updated@example.com', tier = 'gold'
   WHERE id = 900001
   RETURNING id, email, tier;"
```

✅ Expected:
```
  id   |          email          | tier
-------+-------------------------+------
900001 | e2e_updated@example.com | gold
UPDATE 1
```

---

#### Step 6 — Wait and verify UPDATE in Iceberg

```bash
sleep 10
kubectl logs -n prod \
  $(kubectl get pod -n prod -l app=kafka-to-iceberg,pipeline.source=postgres,pipeline.write-mode=standard \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=20s 2>&1 | grep -E "batch=|upsert|hard-delete|ERROR"
```

✅ Expected: `[postgres/customers][standard] batch=N upsert rows=1`

**Notebook Cell 5:**
```python
print("=== UPDATE verify ===")
spark.sql("""
    SELECT id, name, email, tier, snap_id, snap_timestamp
    FROM   postgres.cache_testing.customers
    WHERE  id = 900001
""").show(truncate=False)
```

✅ Expected:
```
=== UPDATE verify ===
+------+------------+-----------------------+----+-------+--------------+
|id    |name        |email                  |tier|snap_id|snap_timestamp|
+------+------------+-----------------------+----+-------+--------------+
|900001|E2E TestUser|e2e_updated@example.com|gold|...    |...           |
+------+------------+-----------------------+----+-------+--------------+
```
`email` = `e2e_updated@example.com`, `tier` = `gold`.
`snap_id` differs from Step 4. `snap_timestamp` is newer than Step 4.

---

#### Step 7 — DELETE the test row from PostgreSQL

`customers` has FK constraints — child rows must be removed first:

```bash
PGPASSWORD=vb2dJms4c1fKi0uYD87Vv4YpCsZQJm1f \
  psql -h 192.168.1.50 -p 30532 -U rbac -d cache_testing << 'SQL'
DELETE FROM product_reviews WHERE customer_id = 900001;
DELETE FROM order_items WHERE order_id IN (SELECT id FROM orders WHERE customer_id = 900001);
DELETE FROM orders      WHERE customer_id = 900001;
DELETE FROM customers   WHERE id = 900001 RETURNING id;
SQL
```

✅ Expected:
```
DELETE 0
DELETE 0
DELETE 0
  id
------
900001
DELETE 1
```

---

#### Step 8 — Wait and verify hard DELETE in Iceberg

```bash
sleep 10
kubectl logs -n prod \
  $(kubectl get pod -n prod -l app=kafka-to-iceberg,pipeline.source=postgres,pipeline.write-mode=standard \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=20s 2>&1 | grep -E "batch=|upsert|hard-delete|ERROR"
```

✅ Expected: `[postgres/customers][standard] batch=N hard-delete rows=1`

**Notebook Cell 6:**
```python
print("=== DELETE verify ===")
cnt = spark.sql(
    "SELECT COUNT(*) FROM postgres.cache_testing.customers WHERE id = 900001"
).collect()[0][0]
total = spark.sql(
    "SELECT COUNT(*) FROM postgres.cache_testing.customers"
).collect()[0][0]
print(f"id=900001 row_count    = {cnt}    (expected 0)")
print(f"total_rows_remaining  = {total}  (expected baseline_row_count)")
```

✅ Expected:
```
=== DELETE verify ===
id=900001 row_count    = 0    (expected 0)
total_rows_remaining  = <N>  (expected baseline_row_count)
```

---

#### Step 9 — Stop the Spark session ⚠️

**Always run this cell when finished** — it releases the 1 core back to the cluster.

**Notebook Cell 7:**
```python
spark.stop()
print("✅ Session stopped — cluster core released")
```

✅ Expected: `✅ Session stopped — cluster core released`

---

**Test 1.1 pass criteria:**

| Step | Operation | Kafka log | Iceberg result |
|------|-----------|-----------|----------------|
| 2–4  | INSERT    | `batch=N upsert rows=1`      | 1 row, correct values |
| 5–6  | UPDATE    | `batch=N upsert rows=1`      | `email` and `tier` updated |
| 7–8  | DELETE    | `batch=N hard-delete rows=1` | `row_count = 0` |

---

### Test 1.2 — Oracle (CACHE_TESTING schema)

**`CUSTOMERS` table schema in Oracle:**
```
ID, NAME, EMAIL, PHONE, ADDRESS, CITY, COUNTRY, CREATED_AT, UPDATED_AT
```

**Iceberg target:** `oracle.e2e_testing.customers`
**Test row ID:** `900002`

> **Propagation note:** LogMiner polls redo logs every ~5 s; allow **15 s** for
> end-to-end propagation (vs 10 s for Postgres).

---

#### Step 1 — Note baseline row count

Open a **new JupyterHub notebook** (or reuse the existing session if still active) and run
**Cell 1 & Cell 2** from the [JupyterHub Setup](#how-to-run-iceberg-queries--jupyterhub)
section above — but substitute `oracle` for `postgres` in the catalog config.

**Notebook Cell 3 (Oracle):**
```python
cnt = spark.sql(
    "SELECT COUNT(*) FROM oracle.e2e_testing.customers"
).collect()[0][0]
print(f"baseline_row_count = {cnt}")

exists = spark.sql(
    "SELECT COUNT(*) FROM oracle.e2e_testing.customers WHERE id = 900002"
).collect()[0][0]
print(f"id=900002 already_exists = {exists > 0}  ← must be False before proceeding")
```

✅ Expected:
```
baseline_row_count = <N>
id=900002 already_exists = False  ← must be False before proceeding
```

> If `already_exists = True`, run the DELETE in Step 7 first, wait 15 s, then re-run.

---

#### Step 2 — INSERT a test row into Oracle

```bash
ORACLE_POD=$(kubectl get pod -n prod -l app=oracle-xe -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it -n prod $ORACLE_POD -- \
  sqlplus CACHE_TESTING/CacheTesting2024@//localhost:1521/XEPDB1
```

```sql
-- Connected as CACHE_TESTING — no schema prefix needed
INSERT INTO CUSTOMERS
  (ID, NAME, EMAIL, PHONE, ADDRESS, CITY, COUNTRY, CREATED_AT, UPDATED_AT)
VALUES
  (900002, 'E2E OracleTest', 'e2e_oracle@example.com', '555-0001',
   '2 Oracle St', 'Sydney', 'AU', SYSDATE, SYSDATE);
COMMIT;
```

✅ Expected: `1 row created.` → `Commit complete.`

---

#### Step 3 — Wait for pipeline propagation

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

**Notebook Cell 4 (Oracle):**
```python
print("=== INSERT verify ===")
spark.sql("""
    SELECT id, name, email, city, country, snap_id, snap_timestamp
    FROM   oracle.e2e_testing.customers
    WHERE  id = 900002
""").show(truncate=False)
```

✅ Expected:
```
=== INSERT verify ===
+------+--------------+----------------------+------+-------+-------+--------------+
|id    |name          |email                 |city  |country|snap_id|snap_timestamp|
+------+--------------+----------------------+------+-------+-------+--------------+
|900002|E2E OracleTest|e2e_oracle@example.com|Sydney|AU     |...    |...           |
+------+--------------+----------------------+------+-------+-------+--------------+
```
1 row returned. `snap_id` is a non-null BIGINT. `snap_timestamp` is within the last 30 s.

---

#### Step 5 — UPDATE the test row in Oracle

```sql
-- sqlplus (CACHE_TESTING session)
UPDATE CUSTOMERS
SET    EMAIL = 'e2e_oracle_updated@example.com',
       CITY  = 'Melbourne',
       UPDATED_AT = SYSDATE
WHERE  ID = 900002;
COMMIT;
```

✅ Expected: `1 row updated.` → `Commit complete.`

---

#### Step 6 — Wait and verify UPDATE in Iceberg

```bash
sleep 15
kubectl logs -n prod \
  $(kubectl get pod -n prod \
    -l app=kafka-to-iceberg,pipeline.source=oracle,pipeline.write-mode=standard \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=30s 2>&1 | grep -E "batch=|upsert|hard-delete|ERROR"
```

✅ Expected: `[oracle/customers][standard] batch=N upsert rows=1`

**Notebook Cell 5 (Oracle):**
```python
print("=== UPDATE verify ===")
spark.sql("""
    SELECT id, name, email, city, snap_id, snap_timestamp
    FROM   oracle.e2e_testing.customers
    WHERE  id = 900002
""").show(truncate=False)
```

✅ Expected:
```
=== UPDATE verify ===
+------+--------------+--------------------------------+---------+-------+--------------+
|id    |name          |email                           |city     |snap_id|snap_timestamp|
+------+--------------+--------------------------------+---------+-------+--------------+
|900002|E2E OracleTest|e2e_oracle_updated@example.com  |Melbourne|...    |...           |
+------+--------------+--------------------------------+---------+-------+--------------+
```
`email` = `e2e_oracle_updated@example.com`, `city` = `Melbourne`.
`snap_id` differs from Step 4. `snap_timestamp` is newer than Step 4.

---

#### Step 7 — DELETE the test row from Oracle

Oracle requires FK children removed before deleting a customer:

```sql
-- sqlplus (CACHE_TESTING session)
DELETE FROM PRODUCT_REVIEWS WHERE CUSTOMER_ID = 900002;
DELETE FROM ORDER_ITEMS WHERE ORDER_ID IN (SELECT ID FROM ORDERS WHERE CUSTOMER_ID = 900002);
DELETE FROM ORDERS WHERE CUSTOMER_ID = 900002;
DELETE FROM CUSTOMERS WHERE ID = 900002;
COMMIT;
```

✅ Expected: row counts printed per statement; `Commit complete.`

---

#### Step 8 — Wait and verify hard DELETE in Iceberg

```bash
sleep 15
kubectl logs -n prod \
  $(kubectl get pod -n prod \
    -l app=kafka-to-iceberg,pipeline.source=oracle,pipeline.write-mode=standard \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=30s 2>&1 | grep -E "batch=|upsert|hard-delete|ERROR"
```

✅ Expected: `[oracle/customers][standard] batch=N hard-delete rows=1`

**Notebook Cell 6 (Oracle):**
```python
print("=== DELETE verify ===")
cnt = spark.sql(
    "SELECT COUNT(*) FROM oracle.e2e_testing.customers WHERE id = 900002"
).collect()[0][0]
total = spark.sql(
    "SELECT COUNT(*) FROM oracle.e2e_testing.customers"
).collect()[0][0]
print(f"id=900002 row_count    = {cnt}    (expected 0)")
print(f"total_rows_remaining  = {total}  (expected baseline_row_count)")
```

✅ Expected:
```
=== DELETE verify ===
id=900002 row_count    = 0    (expected 0)
total_rows_remaining  = <N>  (expected baseline_row_count)
```

---

#### Step 9 — Stop the Spark session ⚠️

**Notebook Cell 7 (Oracle):**
```python
spark.stop()
print("✅ Session stopped — cluster core released")
```

---

**Test 1.2 pass criteria:**

| Step | Operation | Kafka log | Iceberg result |
|------|-----------|-----------|----------------|
| 2–4  | INSERT    | `batch=N upsert rows=1`      | 1 row, correct values |
| 5–6  | UPDATE    | `batch=N upsert rows=1`      | `email` and `city` updated |
| 7–8  | DELETE    | `batch=N hard-delete rows=1` | `row_count = 0` |

> **Full test reference:** See [Runbook 31](runbook-31-oracle-kafka-iceberg-e2e-testing.md) for
> the complete Oracle standard/soft-delete/history-tracking suite with test IDs 900100–900199.

---

### Test 1.3 — MongoDB

**`customers` collection schema in MongoDB:**
```
_id (ObjectId), id (Number), name, email, phone, address, city, country, created_at
```

**Iceberg target:** `mongodb.e2e_testing.customers`
**Test document `id`:** `900003`

> **Propagation note:** MongoDB change streams are near-realtime; allow **10 s**.

---

#### Step 1 — Note baseline row count

**Notebook Cell 3 (MongoDB):**
```python
cnt = spark.sql(
    "SELECT COUNT(*) FROM mongodb.e2e_testing.customers"
).collect()[0][0]
print(f"baseline_row_count = {cnt}")

exists = spark.sql(
    "SELECT COUNT(*) FROM mongodb.e2e_testing.customers WHERE id = 900003"
).collect()[0][0]
print(f"id=900003 already_exists = {exists > 0}  ← must be False before proceeding")
```

✅ Expected:
```
baseline_row_count = <N>
id=900003 already_exists = False  ← must be False before proceeding
```

> If `already_exists = True`, run the DELETE in Step 7 first, wait 10 s, then re-run.

---

#### Step 2 — INSERT a test document

```bash
MONGO_POD=$(kubectl get pod -n prod -l app=mongodb -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it -n prod $MONGO_POD -- \
  mongosh "mongodb://root:oEtCgw554IP3ua0SrJCTsWYM@localhost:27017/cache_testing?authSource=admin" \
  --quiet
```

```javascript
use cache_testing;
db.customers.insertOne({
  _id:        ObjectId("000000000000000000900003"),
  id:         900003,
  name:       "E2E MongoTest",
  email:      "e2e_mongo@example.com",
  phone:      "555-0002",
  address:    "3 Mongo St",
  city:       "Perth",
  country:    "AU",
  created_at: new Date()
});
```

✅ Expected: `{ acknowledged: true, insertedId: ObjectId('000000000000000000900003') }`

---

#### Step 3 — Wait for pipeline propagation

```bash
sleep 10
```

Check the streaming job processed it:

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

**Notebook Cell 4 (MongoDB):**
```python
print("=== INSERT verify ===")
spark.sql("""
    SELECT id, name, email, city, country, snap_id, snap_timestamp
    FROM   mongodb.e2e_testing.customers
    WHERE  id = 900003
""").show(truncate=False)
```

✅ Expected:
```
=== INSERT verify ===
+------+-------------+----------------------+-----+-------+-------+--------------+
|id    |name         |email                 |city |country|snap_id|snap_timestamp|
+------+-------------+----------------------+-----+-------+-------+--------------+
|900003|E2E MongoTest|e2e_mongo@example.com |Perth|AU     |...    |...           |
+------+-------------+----------------------+-----+-------+-------+--------------+
```
1 row returned. `snap_id` is a non-null BIGINT. `snap_timestamp` is within the last 30 s.

---

#### Step 5 — UPDATE the test document in MongoDB

```javascript
// mongosh (cache_testing database)
db.customers.updateOne(
  { id: 900003 },
  { $set: {
      email: "e2e_mongo_updated@example.com",
      city:  "Melbourne"
  }}
);
```

✅ Expected: `{ acknowledged: true, matchedCount: 1, modifiedCount: 1 }`

---

#### Step 6 — Wait and verify UPDATE in Iceberg

```bash
sleep 10
kubectl logs -n prod \
  $(kubectl get pod -n prod \
    -l app=kafka-to-iceberg,pipeline.source=mongodb,pipeline.write-mode=standard \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=20s 2>&1 | grep -E "batch=|upsert|hard-delete|ERROR"
```

✅ Expected: `[mongodb/customers][standard] batch=N upsert rows=1`

**Notebook Cell 5 (MongoDB):**
```python
print("=== UPDATE verify ===")
spark.sql("""
    SELECT id, name, email, city, snap_id, snap_timestamp
    FROM   mongodb.e2e_testing.customers
    WHERE  id = 900003
""").show(truncate=False)
```

✅ Expected:
```
=== UPDATE verify ===
+------+-------------+------------------------------+---------+-------+--------------+
|id    |name         |email                         |city     |snap_id|snap_timestamp|
+------+-------------+------------------------------+---------+-------+--------------+
|900003|E2E MongoTest|e2e_mongo_updated@example.com |Melbourne|...    |...           |
+------+-------------+------------------------------+---------+-------+--------------+
```
`email` = `e2e_mongo_updated@example.com`, `city` = `Melbourne`.
`snap_id` differs from Step 4. `snap_timestamp` is newer than Step 4.

---

#### Step 7 — DELETE the test document from MongoDB

MongoDB has no FK enforcement — a single `deleteOne` is sufficient:

```javascript
// mongosh (cache_testing database)
db.customers.deleteOne({ id: 900003 });
```

✅ Expected: `{ acknowledged: true, deletedCount: 1 }`

---

#### Step 8 — Wait and verify hard DELETE in Iceberg

```bash
sleep 10
kubectl logs -n prod \
  $(kubectl get pod -n prod \
    -l app=kafka-to-iceberg,pipeline.source=mongodb,pipeline.write-mode=standard \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=20s 2>&1 | grep -E "batch=|upsert|hard-delete|ERROR"
```

✅ Expected: `[mongodb/customers][standard] batch=N hard-delete rows=1`

**Notebook Cell 6 (MongoDB):**
```python
print("=== DELETE verify ===")
cnt = spark.sql(
    "SELECT COUNT(*) FROM mongodb.e2e_testing.customers WHERE id = 900003"
).collect()[0][0]
total = spark.sql(
    "SELECT COUNT(*) FROM mongodb.e2e_testing.customers"
).collect()[0][0]
print(f"id=900003 row_count    = {cnt}    (expected 0)")
print(f"total_rows_remaining  = {total}  (expected baseline_row_count)")
```

✅ Expected:
```
=== DELETE verify ===
id=900003 row_count    = 0    (expected 0)
total_rows_remaining  = <N>  (expected baseline_row_count)
```

---

#### Step 9 — Stop the Spark session ⚠️

**Notebook Cell 7 (MongoDB):**
```python
spark.stop()
print("✅ Session stopped — cluster core released")
```

---

**Test 1.3 pass criteria:**

| Step | Operation | Kafka log | Iceberg result |
|------|-----------|-----------|----------------|
| 2–4  | INSERT    | `batch=N upsert rows=1`      | 1 row, correct values |
| 5–6  | UPDATE    | `batch=N upsert rows=1`      | `email` and `city` updated |
| 7–8  | DELETE    | `batch=N hard-delete rows=1` | `row_count = 0` |

> **Full test reference:** See [Runbook 32](runbook-32-mongodb-kafka-iceberg-e2e-testing.md) for
> the complete MongoDB standard/soft-delete/history-tracking suite with test IDs 900200–900299.

---

## 3. Section 2 — Soft Delete Mode Tests

**All three sources** feed into the soft-delete deployment simultaneously.
Per-source target tables:

| Source | Iceberg table |
|--------|---------------|
| PostgreSQL | `postgres.e2e_testing.customers_sd` |
| Oracle | `oracle.e2e_testing.customers_sd` |
| MongoDB | `mongodb.e2e_testing.customers_sd` |

The tests below exercise **PostgreSQL** in detail. For the equivalent Oracle and MongoDB
full-depth procedures see [Runbook 31 Section 2](runbook-31-oracle-kafka-iceberg-e2e-testing.md#4-section-2--soft-delete-mode-tests)
and [Runbook 32 Section 2](runbook-32-mongodb-kafka-iceberg-e2e-testing.md#4-section-2--soft-delete-mode-tests).

**Primary target table (Postgres):** **`postgres.e2e_testing.customers_sd`**

### Setup: Switch to soft_delete mode

```bash
kubectl scale deployment kafka-to-iceberg-standard    -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-soft-delete -n prod --replicas=1
# Carry TARGET_NAMESPACE=e2e_testing into the soft_delete deployment
kubectl set env deployment/kafka-to-iceberg-soft-delete -n prod \
  TARGET_NAMESPACE=e2e_testing
kubectl rollout restart deployment/kafka-to-iceberg-soft-delete -n prod
kubectl rollout status  deployment/kafka-to-iceberg-soft-delete -n prod
```

Verify:
```bash
kubectl get deployment -n prod | grep kafka-to-iceberg
```
**Expected:** `soft-delete` shows `1/1 READY`; others show `0/0`.
`TARGET_NAMESPACE=e2e_testing` must be active — confirm with:
```bash
kubectl logs -n prod -l pipeline.write-mode=soft-delete --tail=15 | grep -i TARGET_NAMESPACE
```

---

### Test 2.1 — Soft Delete: INSERT

#### Step 1 — INSERT test row

```sql
-- psql
INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES (900010, 'E2E SoftTest', 'soft_test@example.com', '555-0010',
        '10 Soft St', 'Melbourne', 'AU', NOW());
COMMIT;
```

#### Step 2 — Wait and verify in Iceberg

```bash
sleep 5
```

```sql
SELECT id, email, is_deleted, deleted_at, snap_id, snap_timestamp
FROM postgres.e2e_testing.customers_sd
WHERE id = 900010;
```

**Expected:** 1 row; `is_deleted = false`; `deleted_at = NULL`; `snap_id` and `snap_timestamp` populated.

---

### Test 2.2 — Soft Delete: UPDATE

#### Step 1 — UPDATE the row

```sql
-- psql
UPDATE customers SET email = 'soft_updated@example.com' WHERE id = 900010;
COMMIT;
```

#### Step 2 — Verify UPDATE

```bash
sleep 5
```

```sql
SELECT id, email, is_deleted, deleted_at
FROM postgres.e2e_testing.customers_sd
WHERE id = 900010;
```

**Expected:** `email = 'soft_updated@example.com'`; `is_deleted = false`; `deleted_at = NULL`.

---

### Test 2.3 — Soft Delete: DELETE

#### Step 1 — DELETE the row

```sql
-- psql
DELETE FROM customers WHERE id = 900010;
COMMIT;
```

#### Step 2 — Verify soft delete in Iceberg

```bash
sleep 5
```

```sql
SELECT id, email, is_deleted, deleted_at
FROM postgres.e2e_testing.customers_sd
WHERE id = 900010;
```

**Expected:** Row **still present**; `is_deleted = true`; `deleted_at` is a non-null TIMESTAMP within the last 30 seconds.

#### Step 3 — Query all soft-deleted rows

```sql
SELECT id, email, deleted_at
FROM postgres.e2e_testing.customers_sd
WHERE is_deleted = true
ORDER BY deleted_at DESC
LIMIT 20;
```

**Expected:** `id = 900010` is in the result set.

---

### Teardown: Switch back to standard mode

```bash
kubectl scale deployment kafka-to-iceberg-soft-delete -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-standard    -n prod --replicas=1
kubectl rollout status deployment/kafka-to-iceberg-standard -n prod
```

---

## 4. Section 3 — History Tracking Mode Tests

**All three sources** feed into the history-tracking deployment simultaneously.
Per-source target tables:

| Source | Iceberg table |
|--------|---------------|
| PostgreSQL | `postgres.e2e_testing.customers_hist` |
| Oracle | `oracle.e2e_testing.customers_hist` |
| MongoDB | `mongodb.e2e_testing.customers_hist` |

The tests below exercise **PostgreSQL** in detail. For the equivalent Oracle and MongoDB
full-depth procedures see [Runbook 31 Section 3](runbook-31-oracle-kafka-iceberg-e2e-testing.md#5-section-3--history-tracking-mode-tests)
and [Runbook 32 Section 3](runbook-32-mongodb-kafka-iceberg-e2e-testing.md#5-section-3--history-tracking-mode-tests).

**Primary target table (Postgres):** **`postgres.e2e_testing.customers_hist`**

### Setup: Switch to history_tracking mode

```bash
kubectl scale deployment kafka-to-iceberg-standard          -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-history-tracking  -n prod --replicas=1
# Carry TARGET_NAMESPACE=e2e_testing into the history_tracking deployment
kubectl set env deployment/kafka-to-iceberg-history-tracking -n prod \
  TARGET_NAMESPACE=e2e_testing
kubectl rollout restart deployment/kafka-to-iceberg-history-tracking -n prod
kubectl rollout status  deployment/kafka-to-iceberg-history-tracking -n prod
```

---

### Test 3.1 — History: INSERT

#### Step 1 — INSERT test row

```sql
-- psql
INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES (900020, 'E2E HistTest', 'hist_test@example.com', '555-0020',
        '20 Hist St', 'Brisbane', 'AU', NOW());
COMMIT;
```

#### Step 2 — Wait and verify in _hist table

```bash
sleep 5
```

```sql
SELECT id, _change_type, _change_ts,
       before_id, before_email,
       after_id, after_email,
       snap_id, snap_timestamp
FROM postgres.e2e_testing.customers_hist
WHERE after_id = 900020
ORDER BY _change_ts;
```

**Expected:** 1 row; `_change_type = 'INSERT'`; all `before_*` columns are NULL; `after_id = 900020`; `after_email = 'hist_test@example.com'`.

---

### Test 3.2 — History: UPDATE

#### Step 1 — UPDATE the row

```sql
-- psql
UPDATE customers SET email = 'hist_updated@example.com' WHERE id = 900020;
COMMIT;
```

#### Step 2 — Wait and verify UPDATE row in _hist

```bash
sleep 5
```

```sql
SELECT id, _change_type, _change_ts,
       before_email, after_email
FROM postgres.e2e_testing.customers_hist
WHERE after_id = 900020
   OR before_id = 900020
ORDER BY _change_ts;
```

**Expected:** 2 rows:
- Row 1: `_change_type = 'INSERT'`, `before_email = NULL`, `after_email = 'hist_test@example.com'`
- Row 2: `_change_type = 'UPDATE'`, `before_email = 'hist_test@example.com'`, `after_email = 'hist_updated@example.com'`

---

### Test 3.3 — History: DELETE

#### Step 1 — DELETE the row

```sql
-- psql
DELETE FROM customers WHERE id = 900020;
COMMIT;
```

#### Step 2 — Wait and verify DELETE row in _hist

```bash
sleep 5
```

```sql
SELECT id, _change_type, _change_ts,
       before_email, after_email
FROM postgres.e2e_testing.customers_hist
WHERE after_id = 900020
   OR before_id = 900020
ORDER BY _change_ts;
```

**Expected:** 3 rows; the third row has `_change_type = 'DELETE'`; `before_email = 'hist_updated@example.com'`; all `after_*` columns are NULL.

---

### Test 3.4 — Full history for a single customer

```sql
SELECT _change_type, _change_ts, before_email, after_email, snap_id
FROM postgres.e2e_testing.customers_hist
WHERE after_id = 900020
   OR before_id = 900020
ORDER BY _change_ts ASC;
```

**Expected:** 3 rows in chronological order — INSERT → UPDATE → DELETE — forming a complete audit trail.

---

### Teardown: Switch back to standard mode

```bash
kubectl scale deployment kafka-to-iceberg-history-tracking -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-standard         -n prod --replicas=1
kubectl rollout status deployment/kafka-to-iceberg-standard -n prod
```

## 5. Section 5 — snap_id and snap_timestamp Validation

### Test 5.1 — Verify hourly partitions exist

```sql
SELECT partition, file_count, total_size
FROM postgres.e2e_testing.customers.partitions
ORDER BY partition DESC
LIMIT 10;
```

**Expected:** Partitions are named by `snap_timestamp_hour` (e.g. `snap_timestamp_hour=2025-06-15-10`) and bucket number. At least one partition per hour the pipeline has been active.

---

### Test 5.2 — Verify snap_id uniqueness within a batch

```sql
SELECT snap_timestamp, COUNT(*) AS total_rows, COUNT(DISTINCT snap_id) AS unique_snap_ids
FROM postgres.e2e_testing.customers
GROUP BY snap_timestamp
HAVING COUNT(*) != COUNT(DISTINCT snap_id);
```

**Expected:** 0 rows returned (no duplicates within a batch).

---

### Test 5.3 — Verify snap_timestamp is write time, not source event time

```sql
-- psql — note the current time
SELECT NOW();
-- e.g.  2025-06-15 10:30:00.123
```

```sql
-- psql — use a deliberately old created_at to contrast with snap_timestamp
INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES (900050, 'SnapTs Test', 'snapts@example.com', '555-0080',
        '50 Snap St', 'Sydney', 'AU', '2020-01-01 00:00:00');
COMMIT;
```

```bash
sleep 5
```

```sql
SELECT id, created_at, snap_timestamp
FROM postgres.e2e_testing.customers
WHERE id = 900050;
```

**Expected:** `created_at = 2020-01-01 00:00:00`; `snap_timestamp` ≈ current time (2025), confirming it is the write wall-clock, not the source column value.

#### Cleanup

```sql
-- psql
DELETE FROM customers WHERE id = 900050; COMMIT;
```

---

### Test 5.4 — Verify hourly partition pruning

```sql
-- This query should scan ONLY the most recent hour's partition
EXPLAIN
SELECT id, email
FROM postgres.e2e_testing.customers
WHERE snap_timestamp >= (CURRENT_TIMESTAMP - INTERVAL 1 HOUR);
```

**Expected:** The execution plan shows `PartitionFilter` or `Dynamic partition pruning` referencing `snap_timestamp_hour`. File scan statistics show far fewer files than a full table scan.

---

## 6. Section 6 — Multi-Source Validation

**Purpose:** Verify all three source connectors propagate changes to their respective Iceberg catalogs within 10 seconds.

All three targets are in `e2e_testing` but different catalogs.

### Step 1 — Simultaneous inserts into all three sources

**Terminal 1 — PostgreSQL:**
```sql
-- psql
INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES (900060, 'MultiSrc PG', 'multi_pg@example.com', '555-9001',
        '60 Multi St', 'Sydney', 'AU', NOW());
COMMIT;
```

**Terminal 2 — Oracle:**
```sql
-- sqlplus
INSERT INTO CACHE_TESTING.CUSTOMERS
  (ID, NAME, EMAIL, PHONE, ADDRESS, CITY, COUNTRY, CREATED_AT, UPDATED_AT)
VALUES
  (900061, 'MultiSrc ORA', 'multi_ora@example.com', '555-9002',
   '61 Multi St', 'Sydney', 'AU', SYSDATE, SYSDATE);
COMMIT;
```

**Terminal 3 — MongoDB:**
```javascript
// mongosh
use cache_testing;
db.customers.insertOne({
  id: 900062,
  name: "MultiSrc MDB",
  email: "multi_mdb@example.com",
  phone: "555-9003",
  address: "62 Multi St",
  city: "Perth",
  country: "AU",
  created_at: new Date()
});
```

### Step 2 — Wait 10 seconds

```bash
sleep 10
```

### Step 3 — Verify all three in Iceberg

```sql
SELECT 'postgres' AS source, id, email, snap_timestamp
FROM postgres.e2e_testing.customers
WHERE id = 900060

UNION ALL

SELECT 'oracle' AS source, id, email, snap_timestamp
FROM oracle.e2e_testing.customers
WHERE id = 900061

UNION ALL

SELECT 'mongodb' AS source, id, email, snap_timestamp
FROM mongodb.e2e_testing.customers
WHERE id = 900062;
```

**Expected:** 3 rows, one from each source, all with `snap_timestamp` within 10 seconds of the inserts.

### Step 4 — Cleanup

```sql
-- psql
DELETE FROM customers WHERE id = 900060; COMMIT;
```
```sql
-- sqlplus
DELETE FROM CACHE_TESTING.CUSTOMERS WHERE ID = 900061; COMMIT;
```
```javascript
// mongosh
db.customers.deleteOne({ id: 900062 });
```

---

## 7. Section 7 — DDL Changes & Schema Evolution

**How DDL is handled per source:**

| Source | Who applies DDL to Iceberg | Action required |
|---|---|---|
| **PostgreSQL** | `schema-evolution-handler` — automatic | Restart streaming pods after DDL |
| **Oracle** | `schema-evolution-handler` — automatic | Restart streaming pods after DDL |
| **MongoDB** | Manual — `ddl_apply.py` | Run `ddl_apply.py`, then restart streaming pods |

> **Why a restart is needed:**
> `05_kafka_to_iceberg_streaming.py` caches the Iceberg schema in memory at pod startup.
> After a DDL change is applied to Iceberg (automatically or manually), the pods must be
> restarted so they re-read the updated schema. Until then, new columns are silently dropped
> from batches.

---

### How DDL Flows Through the System

#### PostgreSQL & Oracle (automatic)

```
Source DB  →  ALTER TABLE
  ↓
Debezium connector
  ↓  publishes DDL event → schema-changes.<source>  (Kafka)
  ↓  publishes DML rows → <source>.cache_testing.<table>  (Kafka)
  ↓
schema-evolution-handler  (04_schema_evolution_handler.py)
  ↓  reads schema-changes.<source>
  ↓  fetches new Avro schema from Schema Registry
  ↓  runs:  ALTER TABLE <catalog>.<namespace>.<table> ADD COLUMN ...
  ↓  (automatic — no operator action needed for this step)

Operator:
  ↓  restart kafka-to-iceberg pods  ← required to flush schema cache
  ↓
05_kafka_to_iceberg_streaming.py  re-reads Iceberg schema on startup
  ↓  new column now flows through normally
```

#### MongoDB (manual)

```
MongoDB  →  new field added to document (no DDL event emitted)
  ↓
05_kafka_to_iceberg_streaming.py  silently drops unknown fields from batch

Operator:
  ↓  run ddl_apply.py --source mongodb --table <t> --op add --col <c> --type <T>
  ↓  restart kafka-to-iceberg pods
  ↓
new field now flows through normally
```

---

### Operator Steps — PostgreSQL & Oracle DDL

When you run an `ALTER TABLE` on the source database:

**Step 1 — Confirm the evolution handler applied the DDL**

```bash
# Watch the handler logs (it applies within seconds of the DDL event)
kubectl logs -n prod -l app=schema-evolution-handler --since=60s \
  | grep -E "DDL event|Applied|ALTER TABLE|WARNING|ERROR"
```

✅ Expected:
```
[postgres] DDL event for table 'customers': ALTER TABLE ...
[postgres/customers] DDL: ALTER TABLE `postgres`.`cache_testing`.`customers` ADD COLUMN `loyalty_tier` STRING
[postgres/customers] Applied: add loyalty_tier STRING
```

**Step 2 — Restart the streaming pods**

```bash
# For PostgreSQL DDL:
kubectl rollout restart deployment/kafka-to-iceberg-postgres-standard \
  deployment/kafka-to-iceberg-postgres-soft-delete \
  deployment/kafka-to-iceberg-postgres-history-tracking -n prod

# For Oracle DDL:
kubectl rollout restart deployment/kafka-to-iceberg-oracle-standard \
  deployment/kafka-to-iceberg-oracle-soft-delete \
  deployment/kafka-to-iceberg-oracle-history-tracking -n prod
```

**Step 3 — Verify the new column is flowing**

```bash
sleep 20
kubectl logs -n prod \
  $(kubectl get pod -n prod -l app=kafka-to-iceberg,pipeline.source=postgres,pipeline.write-mode=standard \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=60s | grep -E "batch=|upsert|ERROR"
```

Then confirm in Spark SQL:
```sql
DESCRIBE TABLE postgres.cache_testing.<table>;
-- Expected: new column present
```

---

### Operator Steps — MongoDB DDL

When a new field appears in MongoDB documents:

**Step 1 — Apply the DDL to Iceberg manually**

```bash
python3 scripts/ddl_apply.py \
    --source mongodb \
    --table <table_name> \
    --op add \
    --col <column_name> \
    --type STRING   # or INT, BIGINT, DOUBLE, BOOLEAN, TIMESTAMP, etc.
```

**Step 2 — Restart the MongoDB streaming pods**

```bash
kubectl rollout restart deployment/kafka-to-iceberg-mongodb-standard \
  deployment/kafka-to-iceberg-mongodb-soft-delete \
  deployment/kafka-to-iceberg-mongodb-history-tracking -n prod
```

**Step 3 — Verify**

```bash
sleep 20
kubectl logs -n prod \
  $(kubectl get pod -n prod -l app=kafka-to-iceberg,pipeline.source=mongodb,pipeline.write-mode=standard \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=60s | grep -E "batch=|upsert|ERROR"
```

---

### Test 7a — PostgreSQL: ADD COLUMN

**Scenario:** Add a `loyalty_tier` column to `customers` in PostgreSQL and verify it
appears in Iceberg automatically.

#### Step 1 — Add the column in PostgreSQL

```sql
-- psql (cache_testing, user: rbac)
ALTER TABLE public.customers ADD COLUMN loyalty_tier VARCHAR(20) DEFAULT NULL;
```

#### Step 2 — Insert a row using the new column

```sql
INSERT INTO public.customers (id, name, email, phone, address, city, country, created_at, loyalty_tier)
VALUES (900070, 'SchemaEvo Test', 'evo@example.com', '555-0001',
        '70 Evo St', 'Sydney', 'AU', NOW(), 'GOLD');
COMMIT;
```

#### Step 3 — Confirm schema-evolution-handler applied the DDL

```bash
sleep 5
kubectl logs -n prod -l app=schema-evolution-handler --since=30s \
  | grep -E "DDL event|Applied|loyalty_tier"
```

✅ Expected: `[postgres/customers] Applied: add loyalty_tier STRING`

#### Step 4 — Restart the PostgreSQL streaming pods

```bash
kubectl rollout restart deployment/kafka-to-iceberg-postgres-standard \
  deployment/kafka-to-iceberg-postgres-soft-delete \
  deployment/kafka-to-iceberg-postgres-history-tracking -n prod
```

#### Step 5 — Wait for the pipeline to process the buffered row

```bash
sleep 20
kubectl logs -n prod \
  $(kubectl get pod -n prod -l app=kafka-to-iceberg,pipeline.source=postgres,pipeline.write-mode=standard \
    -o jsonpath='{.items[0].metadata.name}') \
  --since=60s | grep -E "batch=|upsert|ERROR"
```

✅ Expected: `batch=N upsert rows=1` — no errors.

#### Step 6 — Verify the column and data in Iceberg

```sql
-- Spark SQL
DESCRIBE TABLE postgres.e2e_testing.customers;
-- Expected: loyalty_tier  string  present in schema

SELECT id, name, loyalty_tier, snap_timestamp
FROM postgres.e2e_testing.customers
WHERE id = 900070;
-- Expected: 1 row · loyalty_tier = 'GOLD'

SELECT id, loyalty_tier
FROM postgres.e2e_testing.customers
WHERE id < 900070
LIMIT 5;
-- Expected: loyalty_tier = NULL for pre-DDL rows
```

#### Step 7 — Cleanup

```sql
-- psql
DELETE FROM public.customers WHERE id = 900070;
COMMIT;
```

---

### Test 7b — PostgreSQL: DROP COLUMN

#### Step 1 — Drop the column (must have been added first in 7a)

```sql
-- psql
ALTER TABLE public.customers DROP COLUMN loyalty_tier;
```

#### Step 2 — Confirm evolution handler removed the column from Iceberg

```bash
sleep 5
kubectl logs -n prod -l app=schema-evolution-handler --since=30s \
  | grep -E "DDL event|Applied|loyalty_tier"
```

✅ Expected: `[postgres/customers] Applied: remove loyalty_tier`

#### Step 3 — Restart the PostgreSQL streaming pods

```bash
kubectl rollout restart deployment/kafka-to-iceberg-postgres-standard \
  deployment/kafka-to-iceberg-postgres-soft-delete \
  deployment/kafka-to-iceberg-postgres-history-tracking -n prod
```

---

### Test 7c — Oracle: ADD COLUMN

```sql
-- sqlplus (XEPDB1, CACHE_TESTING schema)
ALTER TABLE CACHE_TESTING.CUSTOMERS ADD (loyalty_points NUMBER(10) DEFAULT 0);
COMMIT;

INSERT INTO CACHE_TESTING.CUSTOMERS
  (ID, NAME, EMAIL, PHONE, ADDRESS, CITY, COUNTRY, CREATED_AT, UPDATED_AT, LOYALTY_POINTS)
VALUES (900075, 'OraEvo', 'ora_evo@example.com', '555-0075',
        '75 Ora St', 'Sydney', 'AU', SYSDATE, SYSDATE, 500);
COMMIT;
```

**Confirm evolution handler applied the DDL:**
```bash
sleep 10
kubectl logs -n prod -l app=schema-evolution-handler --since=30s \
  | grep -E "DDL event|Applied|loyalty_points"
```

✅ Expected: `[oracle/customers] Applied: add loyalty_points DECIMAL(10,0)`

**Restart Oracle streaming pods:**
```bash
kubectl rollout restart deployment/kafka-to-iceberg-oracle-standard \
  deployment/kafka-to-iceberg-oracle-soft-delete \
  deployment/kafka-to-iceberg-oracle-history-tracking -n prod
```

**Verify:**
```sql
-- Spark SQL
SELECT id, loyalty_points FROM oracle.e2e_testing.customers WHERE id = 900075;
-- Expected: loyalty_points = 500
```

**Cleanup:**
```sql
-- sqlplus
DELETE FROM CACHE_TESTING.CUSTOMERS WHERE ID = 900075; COMMIT;
```

---

### Test 7d — MongoDB: New Field (ADD)

MongoDB has no DDL — new fields must be applied manually via `ddl_apply.py`.

```javascript
// mongosh (cache_testing database)
db.customers.insertOne({
  _id:           ObjectId("000000000000000000900078"),
  id:            900078,
  name:          "MDB EvoTest",
  email:         "mdb_evo@example.com",
  loyalty_tier:  "platinum",   // ← new field not yet in Iceberg
  created_at:    new Date()
});
```

> ⚠️ The `loyalty_tier` field will be **silently dropped** from the batch until the
> Iceberg schema is updated and pods are restarted.

**Apply the DDL manually:**
```bash
python3 scripts/ddl_apply.py \
    --source mongodb \
    --table customers \
    --op add \
    --col loyalty_tier \
    --type STRING
```

**Restart MongoDB streaming pods:**
```bash
kubectl rollout restart deployment/kafka-to-iceberg-mongodb-standard \
  deployment/kafka-to-iceberg-mongodb-soft-delete \
  deployment/kafka-to-iceberg-mongodb-history-tracking -n prod
```

**Verify:**
```sql
SELECT id, loyalty_tier FROM mongodb.e2e_testing.customers WHERE id = 900078;
-- Expected: loyalty_tier = 'platinum'
```

**Cleanup:**
```javascript
db.customers.deleteOne({ id: 900078 });
```

---

### DDL Tests Summary

| Test | Source | DDL Operation | Who applies to Iceberg | Operator action |
|---|---|---|---|---|
| **7a** | PostgreSQL | `ADD COLUMN loyalty_tier VARCHAR(20)` | `schema-evolution-handler` (auto) | Restart postgres streaming pods |
| **7b** | PostgreSQL | `DROP COLUMN loyalty_tier` | `schema-evolution-handler` (auto) | Restart postgres streaming pods |
| **7c** | Oracle | `ADD COLUMN loyalty_points NUMBER(10)` | `schema-evolution-handler` (auto) | Restart oracle streaming pods |
| **7d** | MongoDB | New field `loyalty_tier` in document | Manual — `ddl_apply.py` | Run `ddl_apply.py`, restart mongodb streaming pods |

---

---

## 9. Expected Results Summary

> **Pipeline prerequisite for all tests:** `TARGET_NAMESPACE=e2e_testing` must be set on
> the active deployment so all three databases (PostgreSQL, Oracle, MongoDB) replicate into
> the `e2e_testing` Iceberg namespace simultaneously.

| Test | Source | Action | Expected Iceberg Result |
|---|---|---|---|
| **1.1 PG INSERT** | PostgreSQL | INSERT id=900001 | Row in `postgres.e2e_testing.customers`; `snap_id` ≠ NULL |
| **1.1 PG UPDATE** | PostgreSQL | UPDATE id=900001 email | `email` updated; `snap_id` changed; `snap_timestamp` newer |
| **1.1 PG DELETE** | PostgreSQL | DELETE id=900001 | Row gone; `COUNT = 0` |
| **1.2 ORA INSERT** | Oracle | INSERT id=900002 in `CACHE_TESTING.CUSTOMERS` | Row in `oracle.e2e_testing.customers` via `oracle-cache-testing-cdc` |
| **1.2 ORA UPDATE** | Oracle | UPDATE id=900002 email | `email` updated in `oracle.e2e_testing.customers` |
| **1.2 ORA DELETE** | Oracle | DELETE id=900002 | Row hard-deleted from `oracle.e2e_testing.customers` |
| **1.3 MDB INSERT** | MongoDB | insertOne id=900003 in `cache_testing.customers` | Row in `mongodb.e2e_testing.customers` via `mongodb-cache-testing-cdc` |
| **1.3 MDB UPDATE** | MongoDB | updateOne id=900003 | `email` updated in `mongodb.e2e_testing.customers` |
| **1.3 MDB DELETE** | MongoDB | deleteOne id=900003 | Row hard-deleted from `mongodb.e2e_testing.customers` |
| **2.1 Soft INSERT** | PostgreSQL | INSERT id=900010 | `postgres.e2e_testing.customers` (soft_delete mode): `is_deleted=false`; `deleted_at=NULL` |
| **2.2 Soft UPDATE** | PostgreSQL | UPDATE id=900010 | `email` updated; `is_deleted` still false |
| **2.3 Soft DELETE** | PostgreSQL | DELETE id=900010 | Row present; `is_deleted=true`; `deleted_at` non-null |
| **3.1 Hist INSERT** | PostgreSQL | INSERT id=900020 | `postgres.e2e_testing.customers` (hist mode): `_change_type='INSERT'`; `before_*=NULL` |
| **3.2 Hist UPDATE** | PostgreSQL | UPDATE id=900020 | 2nd hist row: `_change_type='UPDATE'`; `before_email` populated |
| **3.3 Hist DELETE** | PostgreSQL | DELETE id=900020 | 3rd hist row: `_change_type='DELETE'`; `after_*=NULL` |
| **4.1 deduplicate** | 3 rapid UPDATEs id=900030 | `customers_dedup`: only last value; 1 row |
| **4.2 mask_columns** | INSERT id=900031 PII | `customers_masked`: SHA-256 hex in email/phone; no plaintext |
| **4.3 proc_time** | INSERT id=900032 | `customers_proc_time`: `proc_time` non-null TIMESTAMP |
| **4.4 op_label** | INSERT/UPDATE id=900033 | `customers_op_label`: `op_label='INSERT'` / `'UPDATE'` |
| **4.5 source_tag** | INSERT id=900034 | `customers_source_tag`: `source_system='postgres'` |
| **4.6 filter_ins** | DELETE id=900035 while filter_op(["c","u"]) | `customers_filter_ins`: row stays (delete suppressed) |
| **4.7 filter_del** | INSERT id=900036 while filter_op(["d"]) | `customers_filter_del`: 0 rows for INSERT; 1 row for DELETE |
| **4.8 enrich** | INSERT order id=900040 | `orders_enriched`: `product_name`/`product_category` populated |
| **4.9 before_after** | INSERT+UPDATE id=900041 | `customers_before_after`: `before_*` and `after_*` side-by-side |
| **4.10 nullcoal** | INSERT id=900042 NULL phone/country | `customers_nullcoal`: `phone='UNKNOWN'`; `country='N/A'` |
| **4.11 windowed_agg** | INSERT 5 orders across AU/US | `orders_agg_summary`: 2 summary rows; `total_revenue` and `avg_order_value` correct; no raw rows |
| **4.12 rolling** | 3 orders for customer 900001 | `orders_rolling`: cumulative `total_amount_rolling_sum` = 50 → 200 → 300; `rolling_avg` = 50 → 100 → 100 |
| **4.13 distinct_per_key** | INSERT 4 customers AU/US/GB | `customers_country_stats`: AU=2, US=1, GB=1 distinct counts |
| **4.14 top_n** | 5 orders AU (10/50/200/300/500) | `orders_top5`: only 3 rows (500/300/200); 50 and 10 absent |
| **4.15 stream_join** | INSERT order 900080 + product 800001 | `orders_products_joined`: `right_name='Widget Pro'`; `left_topic`/`right_topic` provenance populated |
| **4.16 multi_union** | INSERT customer 900090 + order 900091 + product 800002 | `all_topics_union`: 3 rows; `source_topic` set; cross-topic NULLs harmonised |
| **4.17 rename_columns** | INSERT id=900100 | `customers_renamed`: columns `customer_id` and `full_name` present; `id` and `name` absent |
| **4.18 cast_columns** | INSERT order 900101 amount=123.456789 | `orders_cast`: `total_amount = 123.46` (DECIMAL(10,2)); column type confirmed |
| **4.19 drop_columns** | INSERT id=900102 | `customers_dropped`: row lands; no `address`, `phone`, or `updated_at` columns |
| **4.20 flatten_json_col** | INSERT id=900103 with address_json | `customers_flat_addr`: `addr_street='103 Flat St'`, `addr_city='Brisbane'`, `addr_zip='4000'`; no `address_json` column |
| **4.21 filter_columns** | INSERT id=900104 | `customers_projected`: only `id`, `name`, `email`, `country` present; all other cols absent |
| **4.22 aggregate_counts** | INSERT + 2 UPDATEs id=900105 | `event_counts`: `(900105,'c',1)` and `(900105,'u',2)` rows |
| **4.23 event_rate** | Burst 5 inserts (ids 900110–900114) | `pipeline_event_rate`: `event_count ≥ 5`; `events_per_second = event_count / batch_duration_seconds ± 0.001` |
| **4.24 temporal_join** | Order 900120 + payment within 5 s; order 900121 + payment after 8 s | `orders_payments_temporal`: 900120 has `right_payment_method='credit_card'`; 900121 has `right_payment_method=NULL` |
| **4.25 apply_pipeline** | INSERT id=900130 NULL country; then DELETE | `customers_pipeline`: email/phone SHA-256 hashed; `op_label='INSERT'`; `country='N/A'`; `proc_time` non-null; DELETE suppressed (row stays) |
| **5.1 Partitions** | Query `.partitions` metadata | Hourly + bucket partitions visible in `postgres.e2e_testing.customers` |
| **5.2 snap_id unique** | Uniqueness check | 0 duplicate snap_ids within any batch |
| **5.3 snap_timestamp** | Insert with `created_at=2020` | `snap_timestamp` ≈ now (not 2020) |
| **5.4 Partition pruning** | EXPLAIN with `snap_timestamp` filter | Partition pruning in plan |
| **6 Multi-source** | Simultaneous inserts PG/ORA/MDB | 3 rows across `postgres/oracle/mongodb.e2e_testing.customers` within 10 s |
| **7 Schema evo** | ALTER TABLE ADD COLUMN | New column in `postgres.e2e_testing.customers`; old rows NULL |
| **8 Peak-hour** | MERGE_PARALLELISM=16 burst 1000 rows | Batch completes; no errors; setting confirmed in logs |


---

## 10. Session Log

> Append a new entry below each working session. Keep entries in reverse-chronological order
> (newest first). Entries are immutable — do not edit past entries.

---

### Session — 2025-07-10

#### Completed

| Item | Notes |
|------|-------|
| Oracle standard pipeline — all type fixes | `pk_col`, `DoubleType`, epoch-ms timestamps, DDL schema cache all resolved |
| Oracle standard — 10,001 rows in Iceberg | Timestamps correct; `snap_id` / `snap_timestamp` columns populated |
| 10,000-row bulk load benchmark | 217 ms Oracle insert · ~30 s E2E · ~345 rows/s |
| Executor / Spark config gap documented | `MAX_EXECUTORS` and `BURST_BACKLOG_TIMEOUT_S` exist in ConfigMap but are **not wired into the Deployment `env:` stanza** — see §11.1 below |
| Commit `11304c2` pushed & ArgoCD synced | Scaled oracle-soft-delete, oracle-history-tracking, mongodb-standard, mongodb-soft-delete, mongodb-history-tracking to `replicas=1` |

#### Pending — pick up next session (run in order)

**Step 0 — Confirm the 5 new deployments are healthy**

```bash
# All five should be 1/1 READY; if 0/0 ArgoCD may have a resource conflict
kubectl get deployment -n prod -l app=kafka-to-iceberg -o wide

# If any show 0/0, inspect events on the first offender:
kubectl describe deployment kafka-to-iceberg-oracle-soft-delete -n prod | tail -20
```

Expected healthy output (one line per deployment):
```
kafka-to-iceberg-oracle-soft-delete       1/1   1    1   ...
kafka-to-iceberg-oracle-history-tracking  1/1   1    1   ...
kafka-to-iceberg-mongodb-standard         1/1   1    1   ...
kafka-to-iceberg-mongodb-soft-delete      1/1   1    1   ...
kafka-to-iceberg-mongodb-history-tracking 1/1   1    1   ...
```

---

**Step 1 — Oracle soft-delete pipeline** (see [Runbook 31 §4](runbook-31-oracle-kafka-iceberg-e2e-testing.md#4-section-2--soft-delete-mode-tests))

```sql
-- sqlplus / sqlcl  (XEPDB1, schema CACHE_TESTING)
INSERT INTO CUSTOMERS (ID,NAME,EMAIL,PHONE,ADDRESS,CITY,COUNTRY,CREATED_AT,UPDATED_AT)
VALUES (900150,'SD Test','sd@example.com','555-1501','1 SD St','Sydney','AU',SYSDATE,SYSDATE);
COMMIT;
```

```bash
sleep 15
```

```sql
-- Spark SQL (oracle catalog)
SELECT id, is_deleted, deleted_at FROM oracle.e2e_testing.customers_sd WHERE id = 900150;
-- Expected: 1 row · is_deleted=false · deleted_at=NULL
```

```sql
-- sqlplus
DELETE FROM CUSTOMERS WHERE ID = 900150; COMMIT;
```

```bash
sleep 15
```

```sql
-- Spark SQL — verify soft delete
SELECT id, is_deleted, deleted_at FROM oracle.e2e_testing.customers_sd WHERE id = 900150;
-- Expected: row still present · is_deleted=true · deleted_at IS NOT NULL
```

---

**Step 2 — Oracle history-tracking pipeline** (see [Runbook 31 §5](runbook-31-oracle-kafka-iceberg-e2e-testing.md#5-section-3--history-tracking-mode-tests))

```sql
-- sqlplus
INSERT INTO CUSTOMERS (ID,NAME,EMAIL,PHONE,ADDRESS,CITY,COUNTRY,CREATED_AT,UPDATED_AT)
VALUES (900151,'HT Test','ht@example.com','555-1511','2 HT St','Melbourne','AU',SYSDATE,SYSDATE);
COMMIT;
```

```bash
sleep 15
```

```sql
-- Spark SQL
SELECT id, _change_type, snap_timestamp FROM oracle.e2e_testing.customers_hist WHERE id = 900151;
-- Expected: 1 row · _change_type=INSERT
```

```sql
-- sqlplus
UPDATE CUSTOMERS SET EMAIL='ht_upd@example.com' WHERE ID=900151; COMMIT;
```

```bash
sleep 15
```

```sql
-- Spark SQL
SELECT id, _change_type, email FROM oracle.e2e_testing.customers_hist WHERE id = 900151 ORDER BY snap_timestamp;
-- Expected: 2 rows · INSERT + UPDATE
```

```sql
-- sqlplus
DELETE FROM CUSTOMERS WHERE ID=900151; COMMIT;
```

```bash
sleep 15
```

```sql
-- Spark SQL
SELECT id, _change_type FROM oracle.e2e_testing.customers_hist WHERE id = 900151 ORDER BY snap_timestamp;
-- Expected: 3 rows · INSERT · UPDATE · DELETE
```

---

**Step 3 — MongoDB standard pipeline** (see [Runbook 32 §3](runbook-32-mongodb-kafka-iceberg-e2e-testing.md#3-section-1--standard-mode-tests-scd-type-0))

```javascript
// mongosh  (cache_testing database)
db.customers.insertOne({
  _id: ObjectId("000000000000000000900200"),
  id: 900200, name: "MDB Std Test", email: "mdb_std@example.com",
  phone: "555-2001", address: "1 MDB St", city: "Brisbane", country: "AU",
  created_at: new Date(), updated_at: new Date()
});
```

```bash
sleep 10
```

```sql
-- Spark SQL (mongodb catalog)
SELECT id, name, email FROM mongodb.e2e_testing.customers WHERE id = 900200;
-- Expected: 1 row · correct values
```

---

**Step 4 — MongoDB soft-delete pipeline** (see [Runbook 32 §4](runbook-32-mongodb-kafka-iceberg-e2e-testing.md#4-section-2--soft-delete-mode-tests))

```javascript
// mongosh
db.customers.insertOne({
  _id: ObjectId("000000000000000000900210"),
  id: 900210, name: "MDB SD Test", email: "mdb_sd@example.com",
  phone: "555-2101", address: "2 MDB St", city: "Perth", country: "AU",
  created_at: new Date(), updated_at: new Date()
});
```

```bash
sleep 10
```

```sql
SELECT id, is_deleted, deleted_at FROM mongodb.e2e_testing.customers_sd WHERE id = 900210;
-- Expected: row present · is_deleted=false
```

```javascript
db.customers.deleteOne({ id: 900210 });
```

```bash
sleep 10
```

```sql
SELECT id, is_deleted, deleted_at FROM mongodb.e2e_testing.customers_sd WHERE id = 900210;
-- Expected: row present · is_deleted=true · deleted_at IS NOT NULL
```

---

**Step 5 — MongoDB history-tracking pipeline** (see [Runbook 32 §5](runbook-32-mongodb-kafka-iceberg-e2e-testing.md#5-section-3--history-tracking-mode-tests))

```javascript
// mongosh
db.customers.insertOne({
  _id: ObjectId("000000000000000000900220"),
  id: 900220, name: "MDB HT Test", email: "mdb_ht@example.com",
  phone: "555-2201", address: "3 MDB St", city: "Darwin", country: "AU",
  created_at: new Date(), updated_at: new Date()
});
```

```bash
sleep 10
```

```sql
SELECT id, _change_type FROM mongodb.e2e_testing.customers_hist WHERE id = 900220;
-- Expected: 1 row · _change_type=INSERT
```

```javascript
db.customers.updateOne({ id: 900220 }, { $set: { email: "mdb_ht_upd@example.com" } });
```

```bash
sleep 10
```

```javascript
db.customers.deleteOne({ id: 900220 });
```

```bash
sleep 10
```

```sql
SELECT id, _change_type FROM mongodb.e2e_testing.customers_hist WHERE id = 900220 ORDER BY snap_timestamp;
-- Expected: 3 rows · INSERT · UPDATE · DELETE
```

---

**Step 6 — Cross-source status report**

```bash
# Quick summary across all 9 pipelines: 3 sources × 3 modes
echo "=== Deployment health ==="
kubectl get deployment -n prod -l app=kafka-to-iceberg \
  -o custom-columns="NAME:.metadata.name,READY:.status.readyReplicas,DESIRED:.spec.replicas"

echo "=== Iceberg row counts ==="
# Run in a JupyterHub Spark notebook:
```

```sql
-- PostgreSQL source
SELECT 'pg-standard'    AS pipeline, COUNT(*) AS rows FROM postgres.e2e_testing.customers      WHERE id BETWEEN 900001 AND 900099
UNION ALL
SELECT 'pg-soft-delete',              COUNT(*)          FROM postgres.e2e_testing.customers      WHERE is_deleted IS NOT NULL AND id BETWEEN 900001 AND 900099
UNION ALL
SELECT 'pg-history',                  COUNT(*)          FROM postgres.e2e_testing.customers_hist WHERE id BETWEEN 900001 AND 900099
UNION ALL
-- Oracle source
SELECT 'ora-standard',                COUNT(*)          FROM oracle.e2e_testing.customers        WHERE id BETWEEN 900100 AND 900199
UNION ALL
SELECT 'ora-soft-delete',             COUNT(*)          FROM oracle.e2e_testing.customers_sd     WHERE id BETWEEN 900100 AND 900199
UNION ALL
SELECT 'ora-history',                 COUNT(*)          FROM oracle.e2e_testing.customers_hist   WHERE id BETWEEN 900100 AND 900199
UNION ALL
-- MongoDB source
SELECT 'mdb-standard',                COUNT(*)          FROM mongodb.e2e_testing.customers       WHERE id BETWEEN 900200 AND 900299
UNION ALL
SELECT 'mdb-soft-delete',             COUNT(*)          FROM mongodb.e2e_testing.customers_sd    WHERE id BETWEEN 900200 AND 900299
UNION ALL
SELECT 'mdb-history',                 COUNT(*)          FROM mongodb.e2e_testing.customers_hist  WHERE id BETWEEN 900200 AND 900299;
```

---

#### §11.1 — Known gap: MAX_EXECUTORS / BURST_BACKLOG_TIMEOUT_S not wired into Deployment

The two tuning knobs exist in the ConfigMap but the Deployment `env:` stanza does not reference
them, so the Spark job never sees them.

**Affected deployments:** all `kafka-to-iceberg-*` Deployments in `prod`.

**Fix (optional — apply when convenient):**

```yaml
# In the Deployment spec, add under containers[0].env:
- name: MAX_EXECUTORS
  valueFrom:
    configMapKeyRef:
      name: kafka-to-iceberg-config
      key: MAX_EXECUTORS
- name: BURST_BACKLOG_TIMEOUT_S
  valueFrom:
    configMapKeyRef:
      name: kafka-to-iceberg-config
      key: BURST_BACKLOG_TIMEOUT_S
```

After patching, rolling-restart to apply:

```bash
kubectl rollout restart deployment -n prod -l app=kafka-to-iceberg
kubectl rollout status  deployment -n prod -l app=kafka-to-iceberg
```

Verify the values are live:

```bash
kubectl logs -n prod -l app=kafka-to-iceberg,pipeline.write-mode=standard --tail=50 \
  | grep -E "MAX_EXECUTORS|BURST_BACKLOG"
```
