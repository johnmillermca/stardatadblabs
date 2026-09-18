# Runbook 30 — CDC Pipeline End-to-End Test Runbook

**Status:** Operational  
**Namespace:** `prod`  
**Estimated duration:** 45–90 minutes (full suite)

---

## Table of Contents

1. [Prerequisites Check](#1-prerequisites-check)
2. [Section 1 — Standard Mode Tests (SCD Type 0)](#2-section-1--standard-mode-tests-scd-type-0)
3. [Section 2 — Soft Delete Mode Tests](#3-section-2--soft-delete-mode-tests)
4. [Section 3 — History Tracking Mode Tests](#4-section-3--history-tracking-mode-tests)
5. [Section 4 — StarTransform Tests](#5-section-4--startransform-tests) *(Tests 4.1–4.25, one per function)*
   - 4.1 deduplicate · 4.2 mask_columns · 4.3 add_processing_time · 4.4 add_op_label
   - 4.5 add_source_tag · 4.6 filter_op (ins/upd) · 4.7 filter_op (del)
   - 4.8 enrich_from_broadcast · 4.9 pivot_before_after · 4.10 null_coalesce
   - 4.11 windowed_aggregate · 4.12 rolling_sum/rolling_avg · 4.13 count_distinct_per_key
   - 4.14 top_n_per_group · 4.15 stream_join · 4.16 multi_topic_union
   - 4.17 rename_columns · 4.18 cast_columns · 4.19 drop_columns
   - 4.20 flatten_json_col · 4.21 filter_columns · 4.22 aggregate_counts
   - 4.23 event_rate · 4.24 temporal_join · 4.25 apply_pipeline
6. [Section 5 — snap_id and snap_timestamp Validation](#6-section-5--snap_id-and-snap_timestamp-validation)
7. [Section 6 — Multi-Source Validation](#7-section-6--multi-source-validation)
8. [Section 7 — Schema Evolution (DDL) Tests](#8-section-7--schema-evolution-ddl-tests)
9. [Section 8 — Peak-Hour Simulation](#9-section-8--peak-hour-simulation)
10. [Expected Results Summary](#10-expected-results-summary)

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

All E2E test and StarTransform tables live in **one shared namespace per catalog**: `e2e_testing`.
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

### StarTransform test tables — dedicated output tables (Section 4)

Each StarTransform test uses its own Iceberg table so transforms can be tested in isolation
without contaminating the main `customers` / `orders` tables.  The `TARGET_TABLE` env-var
(set per-test via `kubectl set env`) overrides the write target for that specific test.

| Iceberg Table | Write Mode | Section | Transform |
|---|---|---|---|
| `postgres.e2e_testing.customers_dedup` | standard | 4.1 | deduplicate() |
| `oracle.e2e_testing.customers_dedup` | standard | 4.1 | deduplicate() |
| `mongodb.e2e_testing.customers_dedup` | standard | 4.1 | deduplicate() |
| `postgres.e2e_testing.customers_masked` | standard | 4.2 | mask_columns() |
| `oracle.e2e_testing.customers_masked` | standard | 4.2 | mask_columns() |
| `mongodb.e2e_testing.customers_masked` | standard | 4.2 | mask_columns() |
| `postgres.e2e_testing.customers_proc_time` | standard | 4.3 | add_processing_time() |
| `oracle.e2e_testing.customers_proc_time` | standard | 4.3 | add_processing_time() |
| `mongodb.e2e_testing.customers_proc_time` | standard | 4.3 | add_processing_time() |
| `postgres.e2e_testing.customers_op_label` | standard | 4.4 | add_op_label() |
| `oracle.e2e_testing.customers_op_label` | standard | 4.4 | add_op_label() |
| `mongodb.e2e_testing.customers_op_label` | standard | 4.4 | add_op_label() |
| `postgres.e2e_testing.customers_source_tag` | standard | 4.5 | add_source_tag() |
| `oracle.e2e_testing.customers_source_tag` | standard | 4.5 | add_source_tag() |
| `mongodb.e2e_testing.customers_source_tag` | standard | 4.5 | add_source_tag() |
| `postgres.e2e_testing.customers_filter_ins` | standard | 4.6 | filter_op(c/u) |
| `oracle.e2e_testing.customers_filter_ins` | standard | 4.6 | filter_op(c/u) |
| `mongodb.e2e_testing.customers_filter_ins` | standard | 4.6 | filter_op(c/u) |
| `postgres.e2e_testing.customers_filter_del` | standard | 4.7 | filter_op(d) |
| `oracle.e2e_testing.customers_filter_del` | standard | 4.7 | filter_op(d) |
| `mongodb.e2e_testing.customers_filter_del` | standard | 4.7 | filter_op(d) |
| `postgres.e2e_testing.orders_enriched` | standard | 4.8 | enrich_from_broadcast() |
| `oracle.e2e_testing.orders_enriched` | standard | 4.8 | enrich_from_broadcast() |
| `postgres.e2e_testing.customers_before_after` | history_tracking | 4.9 | pivot_before_after() |
| `oracle.e2e_testing.customers_before_after` | history_tracking | 4.9 | pivot_before_after() |
| `postgres.e2e_testing.customers_nullcoal` | standard | 4.10 | null_coalesce() |
| `oracle.e2e_testing.customers_nullcoal` | standard | 4.10 | null_coalesce() |
| `mongodb.e2e_testing.customers_nullcoal` | standard | 4.10 | null_coalesce() |
| `postgres.e2e_testing.orders_agg_summary` | standard | 4.11 | windowed_aggregate() |
| `oracle.e2e_testing.orders_agg_summary` | standard | 4.11 | windowed_aggregate() |
| `postgres.e2e_testing.orders_rolling` | standard | 4.12 | rolling_sum/avg() |
| `oracle.e2e_testing.orders_rolling` | standard | 4.12 | rolling_sum/avg() |
| `postgres.e2e_testing.customers_country_stats` | standard | 4.13 | count_distinct_per_key() |
| `oracle.e2e_testing.customers_country_stats` | standard | 4.13 | count_distinct_per_key() |
| `mongodb.e2e_testing.customers_country_stats` | standard | 4.13 | count_distinct_per_key() |
| `postgres.e2e_testing.orders_top5` | standard | 4.14 | top_n_per_group() |
| `oracle.e2e_testing.orders_top5` | standard | 4.14 | top_n_per_group() |
| `postgres.e2e_testing.orders_products_joined` | standard | 4.15 | stream_join() |
| `postgres.e2e_testing.all_topics_union` | standard | 4.16 | multi_topic_union() |
| `postgres.e2e_testing.customers_renamed` | standard | 4.17 | rename_columns() |
| `oracle.e2e_testing.customers_renamed` | standard | 4.17 | rename_columns() |
| `mongodb.e2e_testing.customers_renamed` | standard | 4.17 | rename_columns() |
| `postgres.e2e_testing.orders_cast` | standard | 4.18 | cast_columns() |
| `oracle.e2e_testing.orders_cast` | standard | 4.18 | cast_columns() |
| `postgres.e2e_testing.customers_dropped` | standard | 4.19 | drop_columns() |
| `oracle.e2e_testing.customers_dropped` | standard | 4.19 | drop_columns() |
| `mongodb.e2e_testing.customers_dropped` | standard | 4.19 | drop_columns() |
| `postgres.e2e_testing.customers_flat_addr` | standard | 4.20 | flatten_json_col() |
| `postgres.e2e_testing.customers_projected` | standard | 4.21 | filter_columns() |
| `oracle.e2e_testing.customers_projected` | standard | 4.21 | filter_columns() |
| `mongodb.e2e_testing.customers_projected` | standard | 4.21 | filter_columns() |
| `postgres.e2e_testing.event_counts` | standard | 4.22 | aggregate_counts() |
| `oracle.e2e_testing.event_counts` | standard | 4.22 | aggregate_counts() |
| `mongodb.e2e_testing.event_counts` | standard | 4.22 | aggregate_counts() |
| `postgres.e2e_testing.pipeline_event_rate` | standard | 4.23 | event_rate() |
| `postgres.e2e_testing.orders_payments_temporal` | standard | 4.24 | temporal_join() |
| `postgres.e2e_testing.customers_pipeline` | standard | 4.25 | apply_pipeline() |
| `oracle.e2e_testing.customers_pipeline` | standard | 4.25 | apply_pipeline() |
| `mongodb.e2e_testing.customers_pipeline` | standard | 4.25 | apply_pipeline() |

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
kubectl exec -it $ORACLE_POD -n prod -- sqlplus cache_testing/cache_testing@//localhost:1521/FREEPDB1
```

**If `mongosh` is missing:** MongoDB DML in Sections 1–3 must be run inside the MongoDB pod:
```bash
kubectl exec -it mongodb-0 -n prod -- mongosh mongodb://localhost:27017/cache_testing
```

> **Note:** `--short` was removed from `kubectl version` in v1.28+. Use `kubectl version --client` instead.

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
kubectl logs -n prod -l app=kafka-to-iceberg-standard --tail=30 | grep -i "e2e_testing\|TARGET_NAMESPACE"
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
kubectl logs -n prod -l app=kafka-to-iceberg-standard --tail=30 | grep -E "Streaming query started|Batch|Error|Exception"
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

## 2. Section 1 — Standard Mode Tests (SCD Type 0)

Confirm `kafka-to-iceberg-standard` is the only active deployment (replicas=1) and
`TARGET_NAMESPACE=e2e_testing` is set (Prerequisite 1.4) before starting.

All Iceberg queries in this section target the `e2e_testing` namespace.
**All three sources — PostgreSQL, Oracle, MongoDB — replicate in real time via Debezium.**
Table name in Iceberg = lowercase last segment of the Kafka topic:
- `postgres.cache_testing.customers` → **`postgres.e2e_testing.customers`**
- `oracle.cache_testing.CUSTOMERS` → **`oracle.e2e_testing.customers`**
- `mongodb.cache_testing.customers` → **`mongodb.e2e_testing.customers`**

### Test 1.1 — PostgreSQL

#### Step 1 — Note current row count

```sql
-- Spark SQL
SELECT COUNT(*) AS row_count FROM postgres.e2e_testing.customers;
```

#### Step 2 — INSERT a test row

```bash
psql -h postgresql.prod.svc.cluster.local -U rbac -d cache_testing
```

```sql
INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES (900001, 'E2E TestUser', 'e2e_test@example.com', '555-0000',
        '1 Test St', 'Sydney', 'AU', NOW());
COMMIT;
```

#### Step 3 — Wait for pipeline propagation

```bash
sleep 5
```

#### Step 4 — Verify INSERT in Iceberg

```sql
SELECT id, name, email, snap_id, snap_timestamp
FROM postgres.e2e_testing.customers
WHERE id = 900001;
```

**Expected:** 1 row returned; `snap_id` non-null BIGINT; `snap_timestamp` within the last 30 seconds.

#### Step 5 — UPDATE the test row

```sql
-- psql
UPDATE customers SET email = 'e2e_updated@example.com' WHERE id = 900001;
COMMIT;
```

#### Step 6 — Wait and verify UPDATE in Iceberg

```bash
sleep 5
```

```sql
SELECT id, email, snap_id, snap_timestamp
FROM postgres.e2e_testing.customers
WHERE id = 900001;
```

**Expected:** `email = 'e2e_updated@example.com'`; `snap_id` differs from Step 4; `snap_timestamp` is newer.

#### Step 7 — DELETE the test row

```sql
-- psql
DELETE FROM customers WHERE id = 900001;
COMMIT;
```

#### Step 8 — Wait and verify hard DELETE in Iceberg

```bash
sleep 5
```

```sql
SELECT COUNT(*) AS should_be_zero
FROM postgres.e2e_testing.customers
WHERE id = 900001;
```

**Expected:** `should_be_zero = 0`

#### Step 9 — Cleanup confirmation

```sql
SELECT id FROM postgres.e2e_testing.customers WHERE id = 900001;
-- Expected: 0 rows
```

---

### Test 1.2 — Oracle (CACHE_TESTING schema)

#### Step 1 — Note current row count

```sql
SELECT COUNT(*) AS row_count FROM oracle.e2e_testing.customers;
```

#### Step 2 — INSERT a test row

```bash
kubectl exec -it -n prod oracle-xe-799f8d67dd-vjtq7 -- \
  sqlplus sys/'cP1En0sclH6N4uSyyqvlgfu8'@XEPDB1 as sysdba
```

```sql
INSERT INTO CACHE_TESTING.CUSTOMERS
  (ID, NAME, EMAIL, PHONE, ADDRESS, CITY, COUNTRY, CREATED_AT, UPDATED_AT)
VALUES
  (900002, 'E2E OracleTest', 'e2e_oracle@example.com', '555-0001',
   '2 Oracle St', 'Sydney', 'AU', SYSDATE, SYSDATE);
COMMIT;
```

#### Step 3 — Wait

```bash
sleep 5
```

#### Step 4 — Verify INSERT in Iceberg

```sql
SELECT id, name, email, snap_id, snap_timestamp
FROM oracle.e2e_testing.customers
WHERE id = 900002;
```

**Expected:** 1 row; `snap_id` non-null; `snap_timestamp` recent.

#### Step 5 — UPDATE

```sql
UPDATE CACHE_TESTING.CUSTOMERS SET EMAIL = 'e2e_oracle_updated@example.com' WHERE ID = 900002;
COMMIT;
```

#### Step 6 — Verify UPDATE

```bash
sleep 5
```

```sql
SELECT id, email, snap_id, snap_timestamp
FROM oracle.e2e_testing.customers
WHERE id = 900002;
```

**Expected:** `email = 'e2e_oracle_updated@example.com'`; `snap_id` changed; `snap_timestamp` newer.

#### Step 7 — DELETE

```sql
DELETE FROM CACHE_TESTING.CUSTOMERS WHERE ID = 900002;
COMMIT;
```

#### Step 8 — Verify hard DELETE

```bash
sleep 5
```

```sql
SELECT COUNT(*) AS should_be_zero FROM oracle.e2e_testing.customers WHERE id = 900002;
```

**Expected:** `should_be_zero = 0`

---

### Test 1.3 — MongoDB

#### Step 1 — Note current row count

```sql
SELECT COUNT(*) AS row_count FROM mongodb.e2e_testing.customers;
```

#### Step 2 — INSERT a test document

```bash
kubectl exec -n prod \
  $(kubectl get pod -n prod -l app=mongodb -o jsonpath='{.items[0].metadata.name}') -- \
  mongosh "mongodb://root:oEtCgw554IP3ua0SrJCTsWYM@localhost:27017/cache_testing?authSource=admin" \
  --quiet
```

```javascript
use cache_testing;
db.customers.insertOne({
  _id: ObjectId("000000000000000000900003"),
  id: 900003,
  name: "E2E MongoTest",
  email: "e2e_mongo@example.com",
  phone: "555-0002",
  address: "3 Mongo St",
  city: "Perth",
  country: "AU",
  created_at: new Date()
});
```

#### Step 3 — Wait

```bash
sleep 5
```

#### Step 4 — Verify INSERT in Iceberg

```sql
SELECT id, name, email, snap_id, snap_timestamp
FROM mongodb.e2e_testing.customers
WHERE id = 900003;
```

**Expected:** 1 row; `snap_id` and `snap_timestamp` populated.

#### Step 5 — UPDATE

```javascript
// mongosh
db.customers.updateOne(
  { id: 900003 },
  { $set: { email: "e2e_mongo_updated@example.com" } }
);
```

#### Step 6 — Verify UPDATE

```bash
sleep 5
```

```sql
SELECT id, email, snap_id, snap_timestamp
FROM mongodb.e2e_testing.customers
WHERE id = 900003;
```

**Expected:** `email = 'e2e_mongo_updated@example.com'`; `snap_id` changed.

#### Step 7 — DELETE

```javascript
// mongosh
db.customers.deleteOne({ id: 900003 });
```

#### Step 8 — Verify hard DELETE

```bash
sleep 5
```

```sql
SELECT COUNT(*) AS should_be_zero FROM mongodb.e2e_testing.customers WHERE id = 900003;
```

**Expected:** `should_be_zero = 0`

---

## 3. Section 2 — Soft Delete Mode Tests

Target table: **`postgres.e2e_testing.customers_sd`**

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

Target table: **`postgres.e2e_testing.customers_hist`**

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

---

## 5. Section 4 — StarTransform Tests

Each test uses its own dedicated Iceberg table inside `postgres.e2e_testing`.  
All tests use `kafka-to-iceberg-standard` (replicas=1). Change `TRANSFORM_PIPELINE`, restart, and query the dedicated table.

### How to apply TRANSFORM_PIPELINE changes

All StarTransform tests run against `kafka-to-iceberg-standard` with `TARGET_NAMESPACE=e2e_testing`.
Both must be set together — `TARGET_NAMESPACE` routes all three sources into `e2e_testing`;
`TRANSFORM_PIPELINE` applies the transform before the write.

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=<function_name>
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

Each StarTransform test below also specifies `TARGET_TABLE` to redirect output to its
dedicated Iceberg table (e.g. `postgres.e2e_testing.customers_dedup`).
**The test DML shown uses PostgreSQL** (`psql`). For Oracle and MongoDB, run equivalent
INSERT/UPDATE/DELETE in `sqlplus`/`mongosh` — the same Iceberg table receives events
from all three sources automatically because `TARGET_NAMESPACE=e2e_testing` is active.

---

### Test 4.1 — `deduplicate` → `postgres.e2e_testing.customers_dedup`

**Purpose:** Rapid-fire updates to the same row within one micro-batch result in only the latest state landing in Iceberg.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.customers_dedup (
    id             BIGINT,
    name           STRING,
    email          STRING,
    phone          STRING,
    address        STRING,
    city           STRING,
    country        STRING,
    created_at     TIMESTAMP,
    updated_at     TIMESTAMP,
    _op            STRING,
    kafka_ts       TIMESTAMP,
    snap_id        STRING,
    snap_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=deduplicate \
  TARGET_TABLE=postgres.e2e_testing.customers_dedup
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Fire 3 rapid updates

```sql
-- psql — run all three before the 2-second micro-batch closes
UPDATE customers SET email = 'dedup_v1@example.com' WHERE id = 900030;
UPDATE customers SET email = 'dedup_v2@example.com' WHERE id = 900030;
UPDATE customers SET email = 'dedup_v3@example.com' WHERE id = 900030;
COMMIT;
```

> If id=900030 does not exist, INSERT it first then run the 3 updates.

#### Verify in Iceberg

```bash
sleep 5
```

```sql
SELECT id, email, snap_id, snap_timestamp
FROM postgres.e2e_testing.customers_dedup
WHERE id = 900030;
```

**Expected:** Exactly 1 row; `email = 'dedup_v3@example.com'` (last write wins).

#### Cleanup

```sql
-- psql
DELETE FROM customers WHERE id = 900030; COMMIT;
```

---

### Test 4.2 — `mask_columns` → `postgres.e2e_testing.customers_masked`

**Purpose:** `email` and `phone` columns are stored as SHA-256 hex digests — plaintext never reaches Iceberg.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
-- email and phone columns are STRING to hold either plaintext or SHA-256 hex
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.customers_masked (
    id             BIGINT,
    name           STRING,
    email          STRING,
    phone          STRING,
    address        STRING,
    city           STRING,
    country        STRING,
    created_at     TIMESTAMP,
    updated_at     TIMESTAMP,
    _op            STRING,
    kafka_ts       TIMESTAMP,
    snap_id        STRING,
    snap_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=deduplicate,mask_columns \
  PII_COLUMNS=email,phone \
  TARGET_TABLE=postgres.e2e_testing.customers_masked
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert a customer with known PII

```sql
-- psql
INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES (900031, 'PII MaskTest', 'pii_clear@example.com', '555-0030',
        '31 PII St', 'Sydney', 'AU', NOW());
COMMIT;
```

#### Verify SHA-256 hash in Iceberg

```bash
sleep 5
# Pre-compute expected hash
echo -n 'pii_clear@example.com' | sha256sum
# Expected: 3b37ebfda7f90dc9ce8d59e45d7f5ea5cddfae2f8f27e98d9671218f92c2a6ad  -
```

```sql
SELECT id, email, phone
FROM postgres.e2e_testing.customers_masked
WHERE id = 900031;
```

**Expected:** `email = '3b37ebfda7f90dc9ce8d59e45d7f5ea5cddfae2f8f27e98d9671218f92c2a6ad'` (SHA-256, not plaintext). `phone` column contains the SHA-256 hash of `555-0030`.

#### Cleanup

```sql
-- psql
DELETE FROM customers WHERE id = 900031; COMMIT;
```

---

### Test 4.3 — `add_processing_time` → `postgres.e2e_testing.customers_proc_time`

**Purpose:** A `proc_time` TIMESTAMP column is injected by the transform — distinct from `snap_timestamp`.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.customers_proc_time (
    id             BIGINT,
    name           STRING,
    email          STRING,
    phone          STRING,
    address        STRING,
    city           STRING,
    country        STRING,
    created_at     TIMESTAMP,
    updated_at     TIMESTAMP,
    _op            STRING,
    kafka_ts       TIMESTAMP,
    proc_time      TIMESTAMP,
    snap_id        STRING,
    snap_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=deduplicate,add_processing_time \
  TARGET_TABLE=postgres.e2e_testing.customers_proc_time
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert and verify

```sql
-- psql
INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES (900032, 'ProcTime Test', 'proctime@example.com', '555-0040',
        '32 Proc St', 'Sydney', 'AU', NOW());
COMMIT;
```

```bash
sleep 5
```

```sql
SELECT id, proc_time, snap_timestamp
FROM postgres.e2e_testing.customers_proc_time
WHERE id = 900032;
```

**Expected:** `proc_time` is a non-null TIMESTAMP within 30 seconds of now. `proc_time` is set by the StarTransform step; `snap_timestamp` is set slightly later at Iceberg write time — both should be close but `proc_time` ≤ `snap_timestamp`.

#### Cleanup

```sql
DELETE FROM customers WHERE id = 900032; COMMIT;
```

---

### Test 4.4 — `add_op_label` → `postgres.e2e_testing.customers_op_label`

**Purpose:** An `op_label` STRING column (`'INSERT'`/`'UPDATE'`/`'DELETE'`) is injected per event.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.customers_op_label (
    id             BIGINT,
    name           STRING,
    email          STRING,
    phone          STRING,
    address        STRING,
    city           STRING,
    country        STRING,
    created_at     TIMESTAMP,
    updated_at     TIMESTAMP,
    _op            STRING,
    op_label       STRING,
    kafka_ts       TIMESTAMP,
    snap_id        STRING,
    snap_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=deduplicate,add_op_label \
  TARGET_TABLE=postgres.e2e_testing.customers_op_label
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert, update, and verify labels

```sql
-- psql
INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES (900033, 'OpLabel Test', 'oplabel@example.com', '555-0050',
        '33 Label St', 'Sydney', 'AU', NOW());
COMMIT;
```

```bash
sleep 5
```

```sql
SELECT id, op_label FROM postgres.e2e_testing.customers_op_label WHERE id = 900033;
```

**Expected:** `op_label = 'INSERT'`

```sql
-- psql
UPDATE customers SET email = 'oplabel_updated@example.com' WHERE id = 900033;
COMMIT;
```

```bash
sleep 5
```

```sql
SELECT id, op_label FROM postgres.e2e_testing.customers_op_label WHERE id = 900033;
```

**Expected:** `op_label = 'UPDATE'`

#### Cleanup

```sql
DELETE FROM customers WHERE id = 900033; COMMIT;
```

---

### Test 4.5 — `add_source_tag` → `postgres.e2e_testing.customers_source_tag`

**Purpose:** A `source_system` STRING column is injected with the value of the `SOURCE` env var.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.customers_source_tag (
    id             BIGINT,
    name           STRING,
    email          STRING,
    phone          STRING,
    address        STRING,
    city           STRING,
    country        STRING,
    created_at     TIMESTAMP,
    updated_at     TIMESTAMP,
    _op            STRING,
    source_system  STRING,
    kafka_ts       TIMESTAMP,
    snap_id        STRING,
    snap_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=deduplicate,add_source_tag \
  SOURCE=postgres \
  TARGET_TABLE=postgres.e2e_testing.customers_source_tag
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert and verify

```sql
-- psql
INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES (900034, 'SourceTag Test', 'sourcetag@example.com', '555-0060',
        '34 Tag St', 'Sydney', 'AU', NOW());
COMMIT;
```

```bash
sleep 5
```

```sql
SELECT id, source_system FROM postgres.e2e_testing.customers_source_tag WHERE id = 900034;
```

**Expected:** `source_system = 'postgres'`

#### Cleanup

```sql
DELETE FROM customers WHERE id = 900034; COMMIT;
```

---

### Test 4.6 — `filter_op` (inserts/updates only) → `postgres.e2e_testing.customers_filter_ins`

**Purpose:** DELETE events are dropped; only `c` (create) and `u` (update) reach Iceberg.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.customers_filter_ins (
    id             BIGINT,
    name           STRING,
    email          STRING,
    phone          STRING,
    address        STRING,
    city           STRING,
    country        STRING,
    created_at     TIMESTAMP,
    updated_at     TIMESTAMP,
    _op            STRING,
    kafka_ts       TIMESTAMP,
    snap_id        STRING,
    snap_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=filter_op \
  FILTER_OPS=c,u \
  TARGET_TABLE=postgres.e2e_testing.customers_filter_ins
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert, then delete

```sql
-- psql
INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES (900035, 'FilterIns Test', 'filterins@example.com', '555-0070',
        '35 Filter St', 'Sydney', 'AU', NOW());
COMMIT;
```

```bash
sleep 5
```

```sql
-- INSERT should have landed
SELECT id FROM postgres.e2e_testing.customers_filter_ins WHERE id = 900035;
-- Expected: 1 row
```

```sql
-- psql
DELETE FROM customers WHERE id = 900035;
COMMIT;
```

```bash
sleep 5
```

```sql
-- DELETE was filtered — row still present in Iceberg
SELECT id FROM postgres.e2e_testing.customers_filter_ins WHERE id = 900035;
-- Expected: STILL 1 row
```

**Expected:** Row remains in `customers_filter_ins` after the source DELETE because `filter_op` excluded the `d` op.

#### Cleanup (manual Iceberg delete)

```sql
-- Spark SQL — remove the test row directly from Iceberg
DELETE FROM postgres.e2e_testing.customers_filter_ins WHERE id = 900035;
```

---

### Test 4.7 — `filter_op` (deletes only) → `postgres.e2e_testing.customers_filter_del`

**Purpose:** Only DELETE events reach Iceberg; inserts/updates are dropped.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.customers_filter_del (
    id             BIGINT,
    name           STRING,
    email          STRING,
    phone          STRING,
    address        STRING,
    city           STRING,
    country        STRING,
    created_at     TIMESTAMP,
    updated_at     TIMESTAMP,
    _op            STRING,
    kafka_ts       TIMESTAMP,
    snap_id        STRING,
    snap_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=filter_op \
  FILTER_OPS=d \
  TARGET_TABLE=postgres.e2e_testing.customers_filter_del
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert (should be dropped), then delete (should land)

```sql
-- psql
INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES (900036, 'FilterDel Test', 'filterdel@example.com', '555-0071',
        '36 Filter St', 'Sydney', 'AU', NOW());
COMMIT;
```

```bash
sleep 5
```

```sql
-- INSERT was filtered — should NOT appear in customers_filter_del
SELECT id FROM postgres.e2e_testing.customers_filter_del WHERE id = 900036;
-- Expected: 0 rows
```

```sql
-- psql
DELETE FROM customers WHERE id = 900036;
COMMIT;
```

```bash
sleep 5
```

```sql
-- DELETE should appear in customers_filter_del
SELECT id FROM postgres.e2e_testing.customers_filter_del WHERE id = 900036;
-- Expected: 1 row (the delete event marker)
```

#### Cleanup

```sql
-- Spark SQL
DELETE FROM postgres.e2e_testing.customers_filter_del WHERE id = 900036;
```

---

### Test 4.8 — `enrich_from_broadcast` → `postgres.e2e_testing.orders_enriched`

**Purpose:** Orders stream is enriched at write time with `product_name` and `product_category` from a broadcast products dimension.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.orders_enriched (
    id               BIGINT,
    customer_id      BIGINT,
    status           STRING,
    total_amount     DOUBLE,
    product_name     STRING,
    product_category STRING,
    _op              STRING,
    kafka_ts         TIMESTAMP,
    snap_id          STRING,
    snap_timestamp   TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable

Broadcast enrichment is configured in the streaming job code (not purely via env var). Patch the `TARGET_TABLE` to direct output to `orders_enriched`:

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=deduplicate,enrich_from_broadcast \
  BROADCAST_DIM_TABLE=postgres.cache_testing.products_std \
  BROADCAST_JOIN_COL=product_id \
  TARGET_TABLE=postgres.e2e_testing.orders_enriched
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert an order with a known product_id

```sql
-- psql — insert an order that references an existing product
INSERT INTO orders (id, customer_id, status, total_amount, created_at)
VALUES (900040, 900001, 'pending', 99.99, NOW());
COMMIT;
```

```bash
sleep 5
```

#### Verify enrichment in Iceberg

```sql
SELECT id, customer_id, status, product_name, product_category, snap_timestamp
FROM postgres.e2e_testing.orders_enriched
WHERE id = 900040;
```

**Expected:** `product_name` and `product_category` are populated from the broadcast join, not from the orders source table.

#### Cleanup

```sql
-- psql
DELETE FROM orders WHERE id = 900040; COMMIT;
```

---

### Test 4.9 — `pivot_before_after` → `postgres.e2e_testing.customers_before_after`

**Purpose:** Both `before_*` and `after_*` columns appear side-by-side for each change event.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
-- Columns are prefixed before_/after_ for every customer field
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.customers_before_after (
    _change_type    STRING,
    _change_ts      TIMESTAMP,
    before_id       BIGINT,
    before_name     STRING,
    before_email    STRING,
    before_phone    STRING,
    before_address  STRING,
    before_city     STRING,
    before_country  STRING,
    after_id        BIGINT,
    after_name      STRING,
    after_email     STRING,
    after_phone     STRING,
    after_address   STRING,
    after_city      STRING,
    after_country   STRING,
    snap_id         STRING,
    snap_timestamp  TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable (requires history_tracking mode)

```bash
kubectl scale deployment kafka-to-iceberg-standard          -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-history-tracking  -n prod --replicas=1
kubectl set env deployment/kafka-to-iceberg-history-tracking -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=pivot_before_after \
  TARGET_TABLE=postgres.e2e_testing.customers_before_after
kubectl rollout restart deployment/kafka-to-iceberg-history-tracking -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-history-tracking -n prod
```

#### INSERT and UPDATE to generate before/after rows

```sql
-- psql
INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES (900041, 'BeforeAfter Test', 'ba_test@example.com', '555-0080',
        '41 BA St', 'Sydney', 'AU', NOW());
COMMIT;
```

```bash
sleep 5
```

```sql
-- psql
UPDATE customers SET email = 'ba_updated@example.com' WHERE id = 900041;
COMMIT;
```

```bash
sleep 5
```

#### Verify before/after columns

```sql
SELECT _change_type, _change_ts,
       before_id, before_email,
       after_id,  after_email
FROM postgres.e2e_testing.customers_before_after
WHERE after_id = 900041 OR before_id = 900041
ORDER BY _change_ts;
```

**Expected:**
- Row 1 (INSERT): `before_id = NULL`, `before_email = NULL`, `after_id = 900041`, `after_email = 'ba_test@example.com'`
- Row 2 (UPDATE): `before_email = 'ba_test@example.com'`, `after_email = 'ba_updated@example.com'`

#### Cleanup

```sql
-- psql
DELETE FROM customers WHERE id = 900041; COMMIT;
```

```bash
# Restore standard mode
kubectl scale deployment kafka-to-iceberg-history-tracking -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-standard         -n prod --replicas=1
kubectl rollout status deployment/kafka-to-iceberg-standard -n prod
```

---

### Test 4.10 — `null_coalesce` → `postgres.e2e_testing.customers_nullcoal`

**Purpose:** NULL values in `country` and `phone` are replaced with configured defaults before landing in Iceberg.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.customers_nullcoal (
    id             BIGINT,
    name           STRING,
    email          STRING,
    phone          STRING,
    address        STRING,
    city           STRING,
    country        STRING,
    created_at     TIMESTAMP,
    updated_at     TIMESTAMP,
    _op            STRING,
    kafka_ts       TIMESTAMP,
    snap_id        STRING,
    snap_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=deduplicate,null_coalesce \
  NULL_COALESCE_MAP='{"country":"N/A","phone":"UNKNOWN"}' \
  TARGET_TABLE=postgres.e2e_testing.customers_nullcoal
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert a row with NULL country and phone

```sql
-- psql
INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES (900042, 'NullCoal Test', 'nullcoal@example.com', NULL,
        '42 Coal St', 'Sydney', NULL, NOW());
COMMIT;
```

```bash
sleep 5
```

#### Verify defaults applied in Iceberg

```sql
SELECT id, name, phone, country
FROM postgres.e2e_testing.customers_nullcoal
WHERE id = 900042;
```

**Expected:** `phone = 'UNKNOWN'`; `country = 'N/A'` — NULL replaced by configured defaults; source row still has NULL in PostgreSQL.

#### Cleanup

```sql
DELETE FROM customers WHERE id = 900042; COMMIT;
```

---

### Test 4.11 — `windowed_aggregate` → `postgres.e2e_testing.orders_agg_summary`

**Purpose:** Verify that `windowed_aggregate` produces a correct grouped multi-aggregate summary (sum, avg, count) from the `orders` Kafka topic and lands it in a dedicated Iceberg table.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.orders_agg_summary (
    country          STRING,
    _op              STRING,
    total_revenue    DOUBLE,
    event_count      BIGINT,
    avg_order_value  DOUBLE,
    proc_batch_ts    TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(proc_batch_ts));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=filter_op,windowed_aggregate \
  FILTER_OPS=c,u \
  AGG_GROUP_COLS=country,_op \
  AGG_SPECS='[["total_amount","sum","total_revenue"],["id","count","event_count"],["total_amount","avg","avg_order_value"]]' \
  TARGET_TABLE=postgres.e2e_testing.orders_agg_summary
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert test orders for two countries

```sql
-- psql — 3 orders for AU, 2 for US
INSERT INTO orders (id, customer_id, status, total_amount, country, created_at)
VALUES
  (900050, 900001, 'pending',   120.00, 'AU', NOW()),
  (900051, 900002, 'pending',    80.00, 'AU', NOW()),
  (900052, 900003, 'completed', 200.00, 'AU', NOW()),
  (900053, 900004, 'pending',    60.00, 'US', NOW()),
  (900054, 900005, 'completed', 140.00, 'US', NOW());
COMMIT;
```

```bash
sleep 5
```

#### Verify aggregate in Iceberg

```sql
SELECT country, _op, total_revenue, event_count, avg_order_value, proc_batch_ts
FROM postgres.e2e_testing.orders_agg_summary
ORDER BY country, _op;
```

**Expected:**

| country | _op | total_revenue | event_count | avg_order_value |
|---------|-----|---------------|-------------|-----------------|
| AU      | c   | 400.00        | 3           | 133.33          |
| US      | c   | 200.00        | 2           | 100.00          |

`proc_batch_ts` must be a non-null TIMESTAMP within 30 seconds of now.
**No raw order rows appear in this table** — only the per-group summary.

#### Cleanup

```sql
-- psql
DELETE FROM orders WHERE id BETWEEN 900050 AND 900054; COMMIT;
```

---

### Test 4.12 — `rolling_sum` / `rolling_avg` → `postgres.e2e_testing.orders_rolling`

**Purpose:** Verify that a running cumulative sum and running average of `total_amount` are computed per `customer_id` within the micro-batch.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.orders_rolling (
    id                         BIGINT,
    customer_id                BIGINT,
    total_amount               DOUBLE,
    kafka_ts                   TIMESTAMP,
    total_amount_rolling_sum   DOUBLE,
    total_amount_rolling_avg   DOUBLE,
    snap_id                    STRING,
    snap_timestamp             TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=filter_op,rolling_sum,rolling_avg \
  FILTER_OPS=c,u \
  ROLLING_VALUE_COL=total_amount \
  ROLLING_ORDER_COL=kafka_ts \
  ROLLING_PARTITION_COLS=customer_id \
  TARGET_TABLE=postgres.e2e_testing.orders_rolling
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert a sequence of orders for the same customer

```sql
-- psql — 3 orders for customer 900001 in ascending amount
INSERT INTO orders (id, customer_id, status, total_amount, created_at)
VALUES
  (900055,  900001, 'pending',    50.00, NOW()),
  (900056,  900001, 'pending',   150.00, NOW() + interval '1 second'),
  (900057,  900001, 'completed', 100.00, NOW() + interval '2 seconds');
COMMIT;
```

```bash
sleep 5
```

#### Verify rolling columns in Iceberg

```sql
SELECT id, customer_id, total_amount,
       total_amount_rolling_sum,
       total_amount_rolling_avg
FROM postgres.e2e_testing.orders_rolling
WHERE customer_id = 900001
ORDER BY kafka_ts;
```

**Expected (rows ordered by kafka_ts):**

| id     | total_amount | total_amount_rolling_sum | total_amount_rolling_avg |
|--------|--------------|--------------------------|--------------------------|
| 900055 | 50.00        | 50.00                    | 50.00                    |
| 900056 | 150.00       | 200.00                   | 100.00                   |
| 900057 | 100.00       | 300.00                   | 100.00                   |

#### Cleanup

```sql
-- psql
DELETE FROM orders WHERE id BETWEEN 900055 AND 900057; COMMIT;
```

---

### Test 4.13 — `count_distinct_per_key` → `postgres.e2e_testing.customers_country_stats`

**Purpose:** Verify that the distinct-customer count per `country` produces a correct batch-level summary and lands in a dedicated stats Iceberg table.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.customers_country_stats (
    country             STRING,
    id_distinct_count   BIGINT,
    proc_batch_ts       TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(proc_batch_ts));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=filter_op,count_distinct_per_key \
  FILTER_OPS=c,u \
  CDPK_GROUP_COL=country \
  CDPK_VALUE_COL=id \
  TARGET_TABLE=postgres.e2e_testing.customers_country_stats
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert customers across multiple countries

```sql
-- psql
INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES
  (900060, 'AU User 1', 'au1@example.com', '555-0101', '1 AU St', 'Sydney',    'AU', NOW()),
  (900061, 'AU User 2', 'au2@example.com', '555-0102', '2 AU St', 'Melbourne', 'AU', NOW()),
  (900062, 'US User 1', 'us1@example.com', '555-0103', '1 US St', 'New York',  'US', NOW()),
  (900063, 'GB User 1', 'gb1@example.com', '555-0104', '1 GB St', 'London',    'GB', NOW());
COMMIT;
```

```bash
sleep 5
```

#### Verify distinct counts in Iceberg

```sql
SELECT country, id_distinct_count, proc_batch_ts
FROM postgres.e2e_testing.customers_country_stats
ORDER BY country;
```

**Expected:**

| country | id_distinct_count |
|---------|-------------------|
| AU      | 2                 |
| GB      | 1                 |
| US      | 1                 |

`proc_batch_ts` is non-null and within 30 seconds of now.

#### Cleanup

```sql
-- psql
DELETE FROM customers WHERE id BETWEEN 900060 AND 900063; COMMIT;
```

---

### Test 4.14 — `top_n_per_group` → `postgres.e2e_testing.orders_top5`

**Purpose:** Verify that only the top-3 highest-value orders per `country` reach Iceberg; lower-value orders are dropped within the batch.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.orders_top5 (
    id            BIGINT,
    customer_id   BIGINT,
    total_amount  DOUBLE,
    country       STRING,
    snap_id       STRING,
    snap_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=filter_op,top_n_per_group \
  FILTER_OPS=c,u \
  TOP_N_GROUP_COL=country \
  TOP_N_RANK_COL=total_amount \
  TOP_N=3 \
  TARGET_TABLE=postgres.e2e_testing.orders_top5
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert 5 orders for AU — only top 3 should land

```sql
-- psql — 5 orders for AU; amounts are 10, 200, 50, 500, 300
INSERT INTO orders (id, customer_id, status, total_amount, country, created_at)
VALUES
  (900070, 900001, 'pending',    10.00, 'AU', NOW()),
  (900071, 900002, 'pending',   200.00, 'AU', NOW()),
  (900072, 900003, 'pending',    50.00, 'AU', NOW()),
  (900073, 900004, 'completed', 500.00, 'AU', NOW()),
  (900074, 900005, 'completed', 300.00, 'AU', NOW());
COMMIT;
```

```bash
sleep 5
```

#### Verify only top 3 landed

```sql
SELECT id, total_amount, country
FROM postgres.e2e_testing.orders_top5
WHERE country = 'AU'
ORDER BY total_amount DESC;
```

**Expected:** Exactly 3 rows — ids `900073` (500), `900074` (300), `900071` (200).
Rows `900072` (50) and `900070` (10) must **not** appear.

#### Cleanup

```sql
-- psql
DELETE FROM orders WHERE id BETWEEN 900070 AND 900074; COMMIT;
-- Spark SQL — manual cleanup for filtered rows that stayed in Iceberg
DELETE FROM postgres.e2e_testing.orders_top5 WHERE id BETWEEN 900070 AND 900074;
```

---

### Test 4.15 — `stream_join` (multi-topic) → `postgres.e2e_testing.orders_products_joined`

**Purpose:** Verify that a batch from the `orders` Kafka topic is joined against the `products` topic batch on `product_id`, and the joined result (with both order and product columns) lands in Iceberg with correct column provenance.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.orders_products_joined (
    id                   BIGINT,
    customer_id          BIGINT,
    product_id           BIGINT,
    total_amount         DOUBLE,
    right_name           STRING,
    right_category       STRING,
    right_price          DOUBLE,
    left_topic           STRING,
    right_topic          STRING,
    snap_id              STRING,
    snap_timestamp       TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable

The `stream_join` function operates on two DataFrames extracted from the same multi-topic batch using `route_by_topic`.  Configure the job to subscribe to **both** topics and enable the join pipeline:

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  KAFKA_TOPICS=postgres.public.orders,postgres.public.products \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=route_by_topic,stream_join,join_and_tag_source \
  JOIN_LEFT_TOPIC=postgres.public.orders \
  JOIN_RIGHT_TOPIC=postgres.public.products \
  JOIN_KEY_COL=product_id \
  JOIN_HOW=left \
  TARGET_TABLE=postgres.e2e_testing.orders_products_joined
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert a product and then an order referencing it

```sql
-- psql — ensure a known product exists
INSERT INTO products (id, name, category, price)
VALUES (800001, 'Widget Pro', 'Hardware', 49.99)
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;
COMMIT;

-- Insert an order referencing that product
INSERT INTO orders (id, customer_id, product_id, status, total_amount, created_at)
VALUES (900080, 900001, 800001, 'pending', 149.97, NOW());
COMMIT;
```

```bash
sleep 5
```

#### Verify the join output in Iceberg

```sql
SELECT id, customer_id, product_id,
       total_amount,
       right_name, right_category, right_price,
       left_topic, right_topic,
       snap_timestamp
FROM postgres.e2e_testing.orders_products_joined
WHERE id = 900080;
```

**Expected:**

| Column | Value |
|--------|-------|
| `id` | 900080 |
| `product_id` | 800001 |
| `right_name` | `'Widget Pro'` |
| `right_category` | `'Hardware'` |
| `right_price` | 49.99 |
| `left_topic` | `'postgres.public.orders'` |
| `right_topic` | `'postgres.public.products'` |

`right_name`, `right_category`, and `right_price` come from the products topic, not the orders topic.
The `left_topic` / `right_topic` provenance columns confirm which Kafka topics contributed each side.

#### Cleanup

```sql
-- psql
DELETE FROM orders WHERE id = 900080; COMMIT;
-- Spark SQL
DELETE FROM postgres.e2e_testing.orders_products_joined WHERE id = 900080;
```

---

### Test 4.16 — `multi_topic_union` → `postgres.e2e_testing.all_topics_union`

**Purpose:** Verify that events from the `customers`, `orders`, and `products` Kafka topics are UNION ALL'd into a single Iceberg table with a `source_topic` tag column identifying each row's origin.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
-- Schema must be the superset of all three topic schemas.
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.all_topics_union (
    id             BIGINT,
    name           STRING,
    email          STRING,
    phone          STRING,
    address        STRING,
    city           STRING,
    country        STRING,
    customer_id    BIGINT,
    product_id     BIGINT,
    status         STRING,
    total_amount   DOUBLE,
    price          DOUBLE,
    category       STRING,
    _op            STRING,
    kafka_ts       TIMESTAMP,
    source_topic   STRING,
    snap_id        STRING,
    snap_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (source_topic, days(snap_timestamp));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  KAFKA_TOPICS=postgres.public.customers,postgres.public.orders,postgres.public.products \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=route_by_topic,multi_topic_union \
  UNION_TAG_COL=source_topic \
  TARGET_TABLE=postgres.e2e_testing.all_topics_union
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert one row into each source table simultaneously

```sql
-- psql — run as a single transaction to target the same micro-batch
BEGIN;

INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES (900090, 'Union CustTest', 'union_cust@example.com', '555-0200',
        '90 Union St', 'Sydney', 'AU', NOW());

INSERT INTO orders (id, customer_id, status, total_amount, created_at)
VALUES (900091, 900090, 'pending', 75.00, NOW());

INSERT INTO products (id, name, category, price)
VALUES (800002, 'Union Widget', 'Software', 19.99)
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;

COMMIT;
```

```bash
sleep 10
```

#### Verify all three rows landed with correct topic tags

```sql
SELECT id, source_topic, name, total_amount, price, _op, snap_timestamp
FROM postgres.e2e_testing.all_topics_union
WHERE id IN (900090, 900091, 800002)
ORDER BY source_topic, id;
```

**Expected:**

| id     | source_topic                    | Populated field |
|--------|---------------------------------|-----------------|
| 800002 | `postgres.public.products`      | `price = 19.99` |
| 900090 | `postgres.public.customers`     | `name = 'Union CustTest'` |
| 900091 | `postgres.public.orders`        | `total_amount = 75.00` |

- Columns that do not exist in a given topic's schema appear as `NULL` (schema harmonisation).
- `source_topic` must be the fully-qualified Kafka topic name for every row.

#### Verify NULL harmonisation

```sql
-- For the customers row: order-only fields should be NULL
SELECT id, source_topic, total_amount, price
FROM postgres.e2e_testing.all_topics_union
WHERE id = 900090;
```

**Expected:** `total_amount = NULL`, `price = NULL` — neither column exists in the customers topic.

#### Cleanup

```sql
-- psql
DELETE FROM customers WHERE id = 900090; COMMIT;
DELETE FROM orders    WHERE id = 900091; COMMIT;
-- Spark SQL
DELETE FROM postgres.e2e_testing.all_topics_union WHERE id IN (900090, 900091, 800002);
```

---

### Test 4.17 — `rename_columns` → `postgres.e2e_testing.customers_renamed`

**Purpose:** Verify that `rename_columns` renames `id → customer_id` and `name → full_name` in the streaming batch before the row lands in Iceberg — the original column names must not appear.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.customers_renamed (
    customer_id    BIGINT,
    full_name      STRING,
    email          STRING,
    phone          STRING,
    address        STRING,
    city           STRING,
    country        STRING,
    created_at     TIMESTAMP,
    updated_at     TIMESTAMP,
    _op            STRING,
    kafka_ts       TIMESTAMP,
    snap_id        STRING,
    snap_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=deduplicate,rename_columns \
  RENAME_MAP='{"id":"customer_id","name":"full_name"}' \
  TARGET_TABLE=postgres.e2e_testing.customers_renamed
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert a customer

```sql
-- psql
INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES (900100, 'Rename Test', 'rename@example.com', '555-0300',
        '100 Rename St', 'Sydney', 'AU', NOW());
COMMIT;
```

```bash
sleep 5
```

#### Verify column names in Iceberg

```sql
-- Confirm renamed columns exist and hold correct values
SELECT customer_id, full_name, email
FROM postgres.e2e_testing.customers_renamed
WHERE customer_id = 900100;
```

**Expected:** `customer_id = 900100`; `full_name = 'Rename Test'`.
The columns `id` and `name` must **not** exist in this table (schema was renamed, not added).

```sql
-- Confirm original column names are absent
DESCRIBE postgres.e2e_testing.customers_renamed;
-- Expected: column list contains customer_id and full_name; no id or name column
```

#### Cleanup

```sql
-- psql
DELETE FROM customers WHERE id = 900100; COMMIT;
-- Spark SQL
DELETE FROM postgres.e2e_testing.customers_renamed WHERE customer_id = 900100;
```

---

### Test 4.18 — `cast_columns` → `postgres.e2e_testing.orders_cast`

**Purpose:** Verify that `cast_columns` casts `total_amount` from DOUBLE to DECIMAL(10,2) and `status` to uppercase STRING — the Iceberg table stores the casted types.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.orders_cast (
    id             BIGINT,
    customer_id    BIGINT,
    status         STRING,
    total_amount   DECIMAL(10,2),
    _op            STRING,
    kafka_ts       TIMESTAMP,
    snap_id        STRING,
    snap_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=filter_op,deduplicate,cast_columns \
  FILTER_OPS=c,u \
  CAST_MAP='{"total_amount":"decimal(10,2)"}' \
  TARGET_TABLE=postgres.e2e_testing.orders_cast
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert an order with a fractional amount

```sql
-- psql
INSERT INTO orders (id, customer_id, status, total_amount, created_at)
VALUES (900101, 900001, 'pending', 123.456789, NOW());
COMMIT;
```

```bash
sleep 5
```

#### Verify the cast value in Iceberg

```sql
SELECT id, total_amount, status
FROM postgres.e2e_testing.orders_cast
WHERE id = 900101;
```

**Expected:** `total_amount = 123.46` — cast to `DECIMAL(10,2)` rounds to 2 decimal places.
The raw value in PostgreSQL is `123.456789` (DOUBLE PRECISION).

```sql
-- Confirm the Iceberg column type
DESCRIBE postgres.e2e_testing.orders_cast;
-- Expected: total_amount   decimal(10,2)
```

#### Cleanup

```sql
-- psql
DELETE FROM orders WHERE id = 900101; COMMIT;
```

---

### Test 4.19 — `drop_columns` → `postgres.e2e_testing.customers_dropped`

**Purpose:** Verify that `drop_columns` removes `address`, `phone`, and `updated_at` from the batch before the row lands in Iceberg — those columns must not appear in the output table.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
-- address, phone, updated_at intentionally omitted — they are dropped by the transform
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.customers_dropped (
    id             BIGINT,
    name           STRING,
    email          STRING,
    city           STRING,
    country        STRING,
    created_at     TIMESTAMP,
    _op            STRING,
    kafka_ts       TIMESTAMP,
    snap_id        STRING,
    snap_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=deduplicate,drop_columns \
  DROP_COLS=address,phone,updated_at \
  TARGET_TABLE=postgres.e2e_testing.customers_dropped
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert a customer

```sql
-- psql
INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES (900102, 'Drop Test', 'drop@example.com', '555-0301',
        '102 Drop St', 'Melbourne', 'AU', NOW());
COMMIT;
```

```bash
sleep 5
```

#### Verify dropped columns are absent

```sql
SELECT id, name, email, city, country
FROM postgres.e2e_testing.customers_dropped
WHERE id = 900102;
```

**Expected:** Row lands with `id`, `name`, `email`, `city`, `country` populated.

```sql
-- Confirm dropped columns are absent from schema
DESCRIBE postgres.e2e_testing.customers_dropped;
-- Expected: no address, phone, or updated_at column
```

#### Cleanup

```sql
-- psql
DELETE FROM customers WHERE id = 900102; COMMIT;
```

---

### Test 4.20 — `flatten_json_col` → `postgres.e2e_testing.customers_flat_addr`

**Purpose:** Verify that a JSON string `address_json` column is parsed and its fields (`street`, `city`, `zip`) are expanded as top-level Iceberg columns with the `addr_` prefix.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.customers_flat_addr (
    id             BIGINT,
    name           STRING,
    email          STRING,
    addr_street    STRING,
    addr_city      STRING,
    addr_zip       STRING,
    _op            STRING,
    kafka_ts       TIMESTAMP,
    snap_id        STRING,
    snap_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable

The `flatten_json_col` transform requires the source event to carry a JSON string column (`address_json`).
Set up the pipeline to parse that column:

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=deduplicate,flatten_json_col \
  FLATTEN_JSON_COL=address_json \
  FLATTEN_JSON_PREFIX=addr_ \
  FLATTEN_JSON_SCHEMA='{"type":"struct","fields":[{"name":"street","type":"string"},{"name":"city","type":"string"},{"name":"zip","type":"string"}]}' \
  TARGET_TABLE=postgres.e2e_testing.customers_flat_addr
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert a customer carrying a JSON address field

```sql
-- psql — customers table must have an address_json column (add if missing)
ALTER TABLE customers ADD COLUMN IF NOT EXISTS address_json TEXT;

INSERT INTO customers (id, name, email, phone, address_json, city, country, created_at)
VALUES (900103, 'Flatten Test', 'flatten@example.com', '555-0302',
        '{"street":"103 Flat St","city":"Brisbane","zip":"4000"}',
        'Brisbane', 'AU', NOW());
COMMIT;
```

```bash
sleep 5
```

#### Verify flattened columns in Iceberg

```sql
SELECT id, name, addr_street, addr_city, addr_zip
FROM postgres.e2e_testing.customers_flat_addr
WHERE id = 900103;
```

**Expected:**

| id     | name           | addr_street     | addr_city  | addr_zip |
|--------|----------------|-----------------|------------|----------|
| 900103 | Flatten Test   | 103 Flat St     | Brisbane   | 4000     |

The original `address_json` column must **not** appear — `flatten_json_col` drops it after expansion.

#### Cleanup

```sql
-- psql
DELETE FROM customers WHERE id = 900103; COMMIT;
```

---

### Test 4.21 — `filter_columns` → `postgres.e2e_testing.customers_projected`

**Purpose:** Verify that `filter_columns` keeps only `id`, `name`, `email`, and `country` — all other columns from the Kafka event are dropped before the row lands in Iceberg.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
-- Only the projected subset of columns
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.customers_projected (
    id             BIGINT,
    name           STRING,
    email          STRING,
    country        STRING,
    snap_id        STRING,
    snap_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=deduplicate,filter_columns \
  KEEP_COLS=id,name,email,country,snap_id,snap_timestamp \
  TARGET_TABLE=postgres.e2e_testing.customers_projected
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert a customer

```sql
-- psql
INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES (900104, 'Project Test', 'project@example.com', '555-0303',
        '104 Proj St', 'Perth', 'AU', NOW());
COMMIT;
```

```bash
sleep 5
```

#### Verify only projected columns are present

```sql
SELECT id, name, email, country
FROM postgres.e2e_testing.customers_projected
WHERE id = 900104;
```

**Expected:** Row lands with only `id`, `name`, `email`, `country` (and system columns `snap_id`, `snap_timestamp`).

```sql
DESCRIBE postgres.e2e_testing.customers_projected;
-- Expected: no phone, address, city, created_at, updated_at, _op, or kafka_ts columns
```

#### Cleanup

```sql
-- psql
DELETE FROM customers WHERE id = 900104; COMMIT;
```

---

### Test 4.22 — `aggregate_counts` → `postgres.e2e_testing.event_counts`

**Purpose:** Verify that `aggregate_counts` produces a per-`(id, _op)` event count summary in `event_counts` — one summary row per unique (id, op) combination within the batch.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.event_counts (
    id            BIGINT,
    _op           STRING,
    event_count   BIGINT,
    snap_id       STRING,
    snap_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=aggregate_counts \
  AGG_PK_COL=id \
  TARGET_TABLE=postgres.e2e_testing.event_counts
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Generate multiple events for the same customer

```sql
-- psql — INSERT then two UPDATEs within a short window
INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES (900105, 'AggCount Test', 'aggcount@example.com', '555-0304',
        '105 Agg St', 'Sydney', 'AU', NOW());
COMMIT;

UPDATE customers SET email = 'aggcount_v2@example.com' WHERE id = 900105; COMMIT;
UPDATE customers SET city  = 'Melbourne'               WHERE id = 900105; COMMIT;
```

```bash
sleep 5
```

#### Verify the aggregate counts

```sql
SELECT id, _op, event_count
FROM postgres.e2e_testing.event_counts
WHERE id = 900105
ORDER BY _op;
```

**Expected:**

| id     | _op | event_count |
|--------|-----|-------------|
| 900105 | c   | 1           |
| 900105 | u   | 2           |

One row per `(id, _op)` combination; `event_count` reflects how many CDC events of that type arrived in the batch.

#### Cleanup

```sql
-- psql
DELETE FROM customers WHERE id = 900105; COMMIT;
```

---

### Test 4.23 — `event_rate` → `postgres.e2e_testing.pipeline_event_rate`

**Purpose:** Verify that `event_rate` produces a single-row throughput summary per micro-batch — `event_count`, `batch_duration_seconds`, and `events_per_second` must all be non-null and arithmetically consistent.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.pipeline_event_rate (
    event_count              BIGINT,
    batch_duration_seconds   DOUBLE,
    events_per_second        DOUBLE,
    min_ts                   TIMESTAMP,
    max_ts                   TIMESTAMP,
    proc_batch_ts            TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(proc_batch_ts));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=event_rate \
  EVENT_RATE_TS_COL=kafka_ts \
  TARGET_TABLE=postgres.e2e_testing.pipeline_event_rate
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Generate a burst of events

```sql
-- psql — 5 rapid inserts to fill a micro-batch
INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES
  (900110, 'Rate Test 1', 'rate1@example.com', '555-0401', '1 Rate St', 'Sydney', 'AU', NOW()),
  (900111, 'Rate Test 2', 'rate2@example.com', '555-0402', '2 Rate St', 'Sydney', 'AU', NOW()),
  (900112, 'Rate Test 3', 'rate3@example.com', '555-0403', '3 Rate St', 'Sydney', 'AU', NOW()),
  (900113, 'Rate Test 4', 'rate4@example.com', '555-0404', '4 Rate St', 'Sydney', 'AU', NOW()),
  (900114, 'Rate Test 5', 'rate5@example.com', '555-0405', '5 Rate St', 'Sydney', 'AU', NOW());
COMMIT;
```

```bash
sleep 5
```

#### Verify the throughput metrics

```sql
SELECT event_count, batch_duration_seconds, events_per_second,
       min_ts, max_ts, proc_batch_ts
FROM postgres.e2e_testing.pipeline_event_rate
ORDER BY proc_batch_ts DESC
LIMIT 3;
```

**Expected:**
- `event_count ≥ 5` (the 5 inserts, possibly batched with other events).
- `batch_duration_seconds ≥ 1.0` (the `GREATEST(delta, 1.0)` floor).
- `events_per_second = event_count / batch_duration_seconds` — verify arithmetic: `event_count / batch_duration_seconds ≈ events_per_second`.
- `min_ts ≤ max_ts` and both are within the last 60 seconds.
- `proc_batch_ts` is non-null and within 30 seconds of now.

```sql
-- Arithmetic consistency check
SELECT event_count, batch_duration_seconds, events_per_second,
       ABS(events_per_second - (event_count / batch_duration_seconds)) AS rounding_error
FROM postgres.e2e_testing.pipeline_event_rate
ORDER BY proc_batch_ts DESC
LIMIT 1;
-- Expected: rounding_error < 0.001
```

#### Cleanup

```sql
-- psql
DELETE FROM customers WHERE id BETWEEN 900110 AND 900114; COMMIT;
```

---

### Test 4.24 — `temporal_join` → `postgres.e2e_testing.orders_payments_temporal`

**Purpose:** Verify that `temporal_join` matches each order event to the closest-in-time payment event from the `payments` Kafka topic sharing the same `order_id`, within a 5-second tolerance window.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.orders_payments_temporal (
    id                   BIGINT,
    customer_id          BIGINT,
    total_amount         DOUBLE,
    kafka_ts             TIMESTAMP,
    right_payment_method STRING,
    right_payment_status STRING,
    right_amount_paid    DOUBLE,
    snap_id              STRING,
    snap_timestamp       TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  KAFKA_TOPICS=postgres.public.orders,postgres.public.payments \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=route_by_topic,temporal_join \
  TEMPORAL_JOIN_LEFT_TOPIC=postgres.public.orders \
  TEMPORAL_JOIN_RIGHT_TOPIC=postgres.public.payments \
  TEMPORAL_JOIN_KEY_COL=id \
  TEMPORAL_JOIN_LEFT_TS=kafka_ts \
  TEMPORAL_JOIN_RIGHT_TS=kafka_ts \
  TEMPORAL_JOIN_TOLERANCE_MS=5000 \
  TARGET_TABLE=postgres.e2e_testing.orders_payments_temporal
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert a matching order and payment within the tolerance window

```sql
-- psql — insert order then payment for the same order_id within 2 seconds
INSERT INTO orders (id, customer_id, status, total_amount, created_at)
VALUES (900120, 900001, 'pending', 250.00, NOW());
COMMIT;
```

```bash
sleep 2
```

```sql
-- psql — payment arrives 2 seconds after order (within 5 s tolerance)
INSERT INTO payments (order_id, payment_method, payment_status, amount_paid, created_at)
VALUES (900120, 'credit_card', 'approved', 250.00, NOW());
COMMIT;
```

```bash
sleep 5
```

#### Verify the temporal join output

```sql
SELECT id, customer_id, total_amount, kafka_ts,
       right_payment_method, right_payment_status, right_amount_paid,
       snap_timestamp
FROM postgres.e2e_testing.orders_payments_temporal
WHERE id = 900120;
```

**Expected:**

| Column | Value |
|--------|-------|
| `id` | 900120 |
| `total_amount` | 250.00 |
| `right_payment_method` | `'credit_card'` |
| `right_payment_status` | `'approved'` |
| `right_amount_paid` | 250.00 |

#### Verify tolerance cutoff — insert an order with NO matching payment

```sql
-- psql — order with no corresponding payment
INSERT INTO orders (id, customer_id, status, total_amount, created_at)
VALUES (900121, 900002, 'pending', 75.00, NOW());
COMMIT;
```

```bash
sleep 8
```

```sql
-- Payment arrives 8 seconds later — beyond 5 s tolerance — should not join
INSERT INTO payments (order_id, payment_method, payment_status, amount_paid, created_at)
VALUES (900121, 'paypal', 'pending', 75.00, NOW());
COMMIT;
```

```bash
sleep 5
```

```sql
SELECT id, right_payment_method
FROM postgres.e2e_testing.orders_payments_temporal
WHERE id = 900121;
```

**Expected:** Row for `id = 900121` has `right_payment_method = NULL` (outside tolerance → no join).

#### Cleanup

```sql
-- psql
DELETE FROM orders    WHERE id IN (900120, 900121); COMMIT;
DELETE FROM payments  WHERE order_id IN (900120, 900121); COMMIT;
-- Spark SQL
DELETE FROM postgres.e2e_testing.orders_payments_temporal WHERE id IN (900120, 900121);
```

---

### Test 4.25 — `apply_pipeline` → `postgres.e2e_testing.customers_pipeline`

**Purpose:** Verify that `apply_pipeline` correctly chains six transforms in sequence — `filter_op` → `deduplicate` → `mask_columns` → `add_processing_time` → `add_op_label` → `null_coalesce` — and that all six effects are visible in the output Iceberg table.

#### Create the Iceberg table

```sql
-- Spark SQL — run once before the test
-- Contains all injected columns: proc_time, op_label, masked email/phone, coalesced country
CREATE TABLE IF NOT EXISTS postgres.e2e_testing.customers_pipeline (
    id             BIGINT,
    name           STRING,
    email          STRING,
    phone          STRING,
    address        STRING,
    city           STRING,
    country        STRING,
    created_at     TIMESTAMP,
    updated_at     TIMESTAMP,
    _op            STRING,
    op_label       STRING,
    proc_time      TIMESTAMP,
    kafka_ts       TIMESTAMP,
    snap_id        STRING,
    snap_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snap_timestamp));
```

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TARGET_NAMESPACE=e2e_testing \
  TRANSFORM_PIPELINE=apply_pipeline \
  PIPELINE_STEPS='filter_op:ops=c,u|deduplicate:pk=id|mask_columns:columns=email,phone|add_processing_time|add_op_label|null_coalesce:country=N/A' \
  TARGET_TABLE=postgres.e2e_testing.customers_pipeline
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert a customer with NULL country

```sql
-- psql
INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
VALUES (900130, 'Pipeline Test', 'pipe_clear@example.com', '555-0500',
        '130 Pipe St', 'Sydney', NULL, NOW());
COMMIT;
```

```bash
sleep 5
```

#### Verify all six transform effects in Iceberg

```sql
SELECT id, name, email, phone, country, op_label, proc_time, snap_timestamp
FROM postgres.e2e_testing.customers_pipeline
WHERE id = 900130;
```

**Expected — verify each pipeline step's effect:**

| Check | Expected |
|-------|----------|
| `filter_op` | Row present (`_op = 'c'` passed the filter) |
| `deduplicate` | Exactly 1 row for `id = 900130` |
| `mask_columns` | `email` = SHA-256 hex of `'pipe_clear@example.com'` (not plaintext) |
| `mask_columns` | `phone` = SHA-256 hex of `'555-0500'` (not plaintext) |
| `add_processing_time` | `proc_time` is non-null TIMESTAMP ≤ `snap_timestamp` |
| `add_op_label` | `op_label = 'INSERT'` |
| `null_coalesce` | `country = 'N/A'` (was NULL in source) |

```bash
# Pre-compute expected email hash to confirm masking
echo -n 'pipe_clear@example.com' | sha256sum
```

```sql
-- Fire a DELETE — it should be filtered out (filter_op excludes 'd')
-- psql
DELETE FROM customers WHERE id = 900130; COMMIT;
```

```bash
sleep 5
```

```sql
-- Row must still exist in Iceberg — delete was filtered
SELECT id FROM postgres.e2e_testing.customers_pipeline WHERE id = 900130;
-- Expected: 1 row (delete suppressed)
```

#### Cleanup

```sql
-- Spark SQL — manually remove since the delete was filtered
DELETE FROM postgres.e2e_testing.customers_pipeline WHERE id = 900130;
```

---

## 6. Section 5 — snap_id and snap_timestamp Validation

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

## 7. Section 6 — Multi-Source Validation

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

## 8. Section 7 — Schema Evolution (DDL) Tests

**Purpose:** Verify that DDL changes propagate through Debezium and are handled correctly by Iceberg via `mergeSchema=true`.

> **How DDL flows through the pipeline:**
> 1. DDL executes on source DB
> 2. Debezium captures the DDL event → publishes to `schema-changes.<source>`
> 3. Avro schema for the topic updated in Schema Registry (new schema ID issued)
> 4. On next DML, Debezium message carries the new schema ID
> 5. Executor-level SR cache fetches new schema once (one HTTP GET per new schema ID)
> 6. Spark `mergeSchema=true` on Iceberg write adds the new column automatically
> 7. Pre-DDL rows return NULL for the new column

All Iceberg verification queries in this section target **`postgres.e2e_testing.customers`**.

---

### Test 7a — PostgreSQL: ADD COLUMN

**Scenario:** Add a `loyalty_tier` column. Verify it propagates to `postgres.e2e_testing.customers`.

#### Step 1 — Baseline

```bash
psql -h postgresql.prod.svc.cluster.local -U rbac -d cache_testing -c "
SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'customers'
ORDER BY ordinal_position;"
```

```sql
DESCRIBE postgres.e2e_testing.customers;
```

```bash
curl -s http://schema-registry.prod.svc.cluster.local:8081/subjects \
  | jq '[.[] | select(startswith("postgres.cache_testing.customers"))]'
```

#### Step 2 — Add the column in PostgreSQL

```sql
-- psql
ALTER TABLE public.customers ADD COLUMN loyalty_tier VARCHAR(20) DEFAULT NULL;
```

#### Step 3 — Insert a row using the new column

```sql
-- psql
INSERT INTO public.customers (id, name, email, phone, address, city, country, created_at, loyalty_tier)
VALUES (900070, 'SchemaEvo Test', 'evo@example.com', '555-0001',
        '70 Evo St', 'Sydney', 'AU', NOW(), 'GOLD');
```

#### Step 4 — Verify in Schema Registry

```bash
sleep 5
curl -s http://schema-registry.prod.svc.cluster.local:8081/subjects/postgres.cache_testing.customers-value/versions
# Expected: [1, 2]  ← version 2 has loyalty_tier

curl -s http://schema-registry.prod.svc.cluster.local:8081/subjects/postgres.cache_testing.customers-value/versions/latest \
  | jq '.schema | fromjson | .fields[] | select(.name == "loyalty_tier")'
```

#### Step 5 — Verify in Iceberg

```bash
sleep 10
```

```sql
DESCRIBE postgres.e2e_testing.customers;
-- Expected: loyalty_tier  string

SELECT id, name, loyalty_tier, snap_timestamp
FROM postgres.e2e_testing.customers
WHERE id = 900070;
-- Expected: loyalty_tier = 'GOLD'

SELECT id, loyalty_tier
FROM postgres.e2e_testing.customers
WHERE id != 900070
LIMIT 5;
-- Expected: loyalty_tier = NULL for all pre-DDL rows
```

#### Step 6 — Cleanup

```sql
-- psql
DELETE FROM public.customers WHERE id = 900070;
```

---

### Test 7b — PostgreSQL: DROP COLUMN

> **Note:** Iceberg does NOT physically drop the column. It stays in the schema and returns NULL for future rows. Physical removal requires `ALTER TABLE ... DROP COLUMN` in Spark SQL.

#### Step 1 — Drop the column added in 7a

```sql
-- psql
ALTER TABLE public.customers DROP COLUMN loyalty_tier;
```

#### Step 2 — Insert a row after the DROP

```sql
-- psql
INSERT INTO public.customers (id, name, email, phone, address, city, country, created_at)
VALUES (900071, 'PostDrop Test', 'postdrop@example.com', '555-0002',
        '71 Drop St', 'Melbourne', 'AU', NOW());
```

#### Step 3 — Verify behaviour in Iceberg

```bash
sleep 10
```

```sql
SELECT id, name, loyalty_tier
FROM postgres.e2e_testing.customers
WHERE id = 900071;
-- Expected: loyalty_tier = NULL (column kept in Iceberg schema, value absent)
```

```bash
curl -s http://schema-registry.prod.svc.cluster.local:8081/subjects/postgres.cache_testing.customers-value/versions \
  | jq 'length'
# Expected: 3  (original, +loyalty_tier, -loyalty_tier)
```

#### Step 4 — (Optional) Remove the column from Iceberg too

```sql
ALTER TABLE postgres.e2e_testing.customers DROP COLUMN loyalty_tier;
```

#### Step 5 — Cleanup

```sql
-- psql
DELETE FROM public.customers WHERE id = 900071;
```

---

### Test 7c — PostgreSQL: ALTER COLUMN (widen VARCHAR)

#### Step 1 — Widen `address` from VARCHAR(255) to TEXT

```sql
-- psql
ALTER TABLE public.customers ALTER COLUMN address TYPE TEXT;
```

#### Step 2 — Insert a row with a long address

```sql
-- psql
INSERT INTO public.customers (id, name, email, phone, address, city, country, created_at)
VALUES (900072, 'LongAddr Test', 'longaddr@example.com', '555-0003',
        'This is a very long address that exceeds VARCHAR(255) but fits TEXT perfectly fine for testing schema evolution purposes in Iceberg',
        'Brisbane', 'AU', NOW());
```

#### Step 3 — Verify

```bash
sleep 10
```

```sql
DESCRIBE postgres.e2e_testing.customers;
-- Expected: address still string (VARCHAR and TEXT both map to string)

SELECT id, LEFT(address, 60) AS addr_preview
FROM postgres.e2e_testing.customers
WHERE id = 900072;
```

#### Step 4 — Cleanup

```sql
-- psql
DELETE FROM public.customers WHERE id = 900072;
```

---

### Test 7d — Oracle: ADD COLUMN via LogMiner

#### Step 1 — Add a column to CACHE_TESTING.CUSTOMERS in Oracle

```bash
kubectl exec -n prod oracle-xe-799f8d67dd-vjtq7 -- bash -c "
sqlplus -s sys/'cP1En0sclH6N4uSyyqvlgfu8'@XEPDB1 as sysdba <<'EOF'
ALTER TABLE CACHE_TESTING.CUSTOMERS ADD (loyalty_points NUMBER(10) DEFAULT 0);
COMMIT;
SELECT column_name, data_type FROM dba_tab_columns
WHERE owner = 'CACHE_TESTING' AND table_name = 'CUSTOMERS' ORDER BY column_id;
EXIT;
EOF
"
```

#### Step 2 — Insert a row with the new column

```bash
kubectl exec -n prod oracle-xe-799f8d67dd-vjtq7 -- bash -c "
sqlplus -s sys/'cP1En0sclH6N4uSyyqvlgfu8'@XEPDB1 as sysdba <<'EOF'
INSERT INTO CACHE_TESTING.CUSTOMERS
  (ID, NAME, EMAIL, PHONE, ADDRESS, CITY, COUNTRY, CREATED_AT, UPDATED_AT, LOYALTY_POINTS)
VALUES
  (900073, 'OraSchemaEvo', 'oraevo@example.com', '555-9001',
   '73 Oracle St', 'Sydney', 'AU', SYSDATE, SYSDATE, 500);
COMMIT;
EXIT;
EOF
"
```

#### Step 3 — Verify the DDL event reached Kafka

```bash
kubectl exec -n prod \
  $(kubectl get pod -n prod -l app=debezium-connect -o jsonpath='{.items[0].metadata.name}') -- \
  bash -c "
kafka-console-consumer.sh \
  --bootstrap-server strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092 \
  --topic schema-changes.oracle --from-beginning --max-messages 50 \
  --consumer-property security.protocol=SASL_PLAINTEXT \
  --consumer-property sasl.mechanism=SCRAM-SHA-512 \
  --consumer-property 'sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"debezium-user\" password=\"i3uqKrPOaoqWo6JfOZrmSMhtdp7LiN3H\";' \
  2>/dev/null | grep -i 'loyalty_points' | head -5"
```

#### Step 4 — Verify in Iceberg

```bash
sleep 15   # Oracle LogMiner has slightly higher latency than PostgreSQL WAL
```

```sql
DESCRIBE oracle.e2e_testing.customers;
-- Expected: loyalty_points  bigint

SELECT id, name, loyalty_points, snap_timestamp
FROM oracle.e2e_testing.customers
WHERE id = 900073;
-- Expected: loyalty_points = 500
```

#### Step 5 — Cleanup

```bash
kubectl exec -n prod oracle-xe-799f8d67dd-vjtq7 -- bash -c "
sqlplus -s sys/'cP1En0sclH6N4uSyyqvlgfu8'@XEPDB1 as sysdba <<'EOF'
DELETE FROM CACHE_TESTING.CUSTOMERS WHERE ID = 900073;
COMMIT;
EXIT;
EOF
"
```

---

### Test 7e — MongoDB: New Field (implicit schema evolution)

#### Step 1 — Insert a document with extra fields

```bash
kubectl exec -n prod \
  $(kubectl get pod -n prod -l app=mongodb -o jsonpath='{.items[0].metadata.name}') -- \
  mongosh "mongodb://root:oEtCgw554IP3ua0SrJCTsWYM@localhost:27017/cache_testing?authSource=admin" \
  --quiet --eval '
db.customers.insertOne({
  id:           900074,
  name:         "MongoSchemaEvo",
  email:        "mongoevo@example.com",
  phone:        "555-0004",
  address:      "74 Mongo St",
  city:         "Perth",
  country:      "AU",
  loyalty_tier: "PLATINUM",
  referral_code: "REF2025XYZ",
  created_at:   new Date()
});
'
```

#### Step 2 — Verify in Iceberg

```bash
sleep 10
```

```sql
DESCRIBE mongodb.e2e_testing.customers;
-- Expected: loyalty_tier and referral_code columns now present

SELECT id, name, loyalty_tier, referral_code, snap_timestamp
FROM mongodb.e2e_testing.customers
WHERE id = 900074;
-- Expected: loyalty_tier = 'PLATINUM', referral_code = 'REF2025XYZ'
```

#### Step 3 — Cleanup

```bash
kubectl exec -n prod \
  $(kubectl get pod -n prod -l app=mongodb -o jsonpath='{.items[0].metadata.name}') -- \
  mongosh "mongodb://root:oEtCgw554IP3ua0SrJCTsWYM@localhost:27017/cache_testing?authSource=admin" \
  --quiet --eval 'db.customers.deleteOne({ id: 900074 });'
```

---

### Test 7f — Schema Registry Version History Verification

```bash
curl -s http://schema-registry.prod.svc.cluster.local:8081/subjects | jq 'sort'

for SUBJECT in \
  "postgres.cache_testing.customers-value" \
  "oracle.cache_testing.customers-value" \
  "mongodb.cache_testing.customers-value"; do
  echo "=== $SUBJECT ==="
  VERSIONS=$(curl -s "http://schema-registry.prod.svc.cluster.local:8081/subjects/${SUBJECT}/versions")
  echo "Versions: $VERSIONS"
  curl -s "http://schema-registry.prod.svc.cluster.local:8081/subjects/${SUBJECT}/versions/latest" \
    | jq '.schema | fromjson | .fields[].name'
  echo ""
done
```

**Confirm executor-level SR cache working:**
```bash
kubectl logs -n prod \
  $(kubectl get pod -n prod -l pipeline.write-mode=standard -o jsonpath='{.items[0].metadata.name}') \
  | grep "avro_to_json\|schema_id\|SR_CLIENT" | tail -20
# Expected: schema_id fetch logged only once per NEW schema ID, not per message
```

---

### DDL Tests Summary

| Test | Source | DDL Operation | Debezium behaviour | Iceberg outcome |
|---|---|---|---|---|
| **7a** | PostgreSQL | `ADD COLUMN loyalty_tier VARCHAR(20)` | New Avro schema version in SR | Column added via `mergeSchema`; old rows = NULL |
| **7b** | PostgreSQL | `DROP COLUMN loyalty_tier` | New Avro schema without field | Column kept in Iceberg; future rows = NULL |
| **7c** | PostgreSQL | `ALTER COLUMN address TYPE TEXT` | New Avro schema; string → string | No Iceberg type change |
| **7d** | Oracle | `ADD COLUMN loyalty_points NUMBER(10)` | DDL in `schema-changes.oracle` | Column added; old rows = NULL |
| **7e** | MongoDB | New field in document (no DDL) | Full document with new field in `after` | Column added via `mergeSchema` |
| **7f** | All | SR audit | — | All versions visible; SR cache verified |

---

## 9. Section 8 — Peak-Hour Simulation

**Purpose:** Verify the pipeline handles burst load with the parallelism tuning knobs.

### Step 1 — Check current ConfigMap settings

```bash
kubectl get configmap kafka-to-iceberg-config -n prod -o yaml
```

### Step 2 — Apply peak-hour settings

```bash
kubectl patch configmap kafka-to-iceberg-config -n prod --type merge -p '{
  "data": {
    "MERGE_PARALLELISM": "16",
    "COALESCE_BEFORE_MERGE": "8"
  }
}'
kubectl get configmap kafka-to-iceberg-config -n prod \
  -o jsonpath='{.data.MERGE_PARALLELISM} / {.data.COALESCE_BEFORE_MERGE}{"\n"}'
# Expected: 16 / 8
```

### Step 3 — Rolling restart to apply

```bash
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

### Step 4 — Verify settings are active in logs

```bash
kubectl logs -n prod -l app=kafka-to-iceberg-standard --tail=50 \
  | grep -E "MERGE_PARALLELISM|COALESCE_BEFORE_MERGE"
```

### Step 5 — Generate a burst of inserts

```sql
-- psql — 1000 rows
DO $$
BEGIN
  FOR i IN 901000..901999 LOOP
    INSERT INTO customers (id, name, email, phone, address, city, country, created_at)
    VALUES (i, 'BurstTest', 'burst_' || i || '@example.com', '555-' || i,
            i || ' Burst St', 'Sydney', 'AU', NOW());
  END LOOP;
END $$;
COMMIT;
```

### Step 6 — Monitor batch duration

```bash
kubectl logs -n prod -l app=kafka-to-iceberg-standard -f | grep -E "Batch [0-9]+ took"
```

### Step 7 — Restore normal settings

```bash
kubectl patch configmap kafka-to-iceberg-config -n prod --type merge -p '{
  "data": {
    "MERGE_PARALLELISM": "8",
    "COALESCE_BEFORE_MERGE": "4"
  }
}'
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

### Step 8 — Cleanup burst rows

```sql
-- psql
DELETE FROM customers WHERE id BETWEEN 901000 AND 901999;
COMMIT;
```

```bash
sleep 10
```

```sql
SELECT COUNT(*) FROM postgres.e2e_testing.customers
WHERE id BETWEEN 901000 AND 901999;
-- Expected: 0
```

---

## 10. Expected Results Summary

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
