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
5. [Section 4 — StarTransform Tests](#5-section-4--startransform-tests)
6. [Section 5 — snap_id and snap_timestamp Validation](#6-section-5--snap_id-and-snap_timestamp-validation)
7. [Section 6 — Multi-Source Validation](#7-section-6--multi-source-validation)
8. [Section 7 — Schema Evolution (DDL) Tests](#8-section-7--schema-evolution-ddl-tests)
9. [Section 8 — Peak-Hour Simulation](#9-section-8--peak-hour-simulation)
10. [Expected Results Summary](#10-expected-results-summary)

---

## Iceberg Test Table Layout

All E2E test and StarTransform tables live in **one shared namespace per catalog**: `e2e_testing`.  
The write mode and transform function are encoded entirely in the table name suffix — no separate namespaces needed.

| Iceberg Table | Write Mode | Purpose |
|---|---|---|
| `postgres.e2e_testing.customers_std` | standard | Section 1 / 5 / 6 / 7 / 8 |
| `postgres.e2e_testing.customers_sd` | soft_delete | Section 2 |
| `postgres.e2e_testing.customers_hist` | history_tracking | Section 3 |
| `oracle.e2e_testing.customers_std` | standard | Section 1 / 6 |
| `mongodb.e2e_testing.customers_std` | standard | Section 1 / 6 |
| `postgres.e2e_testing.customers_dedup` | standard | Section 4.1 — deduplicate() |
| `postgres.e2e_testing.customers_masked` | standard | Section 4.2 — mask_columns() |
| `postgres.e2e_testing.customers_proc_time` | standard | Section 4.3 — add_processing_time() |
| `postgres.e2e_testing.customers_op_label` | standard | Section 4.4 — add_op_label() |
| `postgres.e2e_testing.customers_source_tag` | standard | Section 4.5 — add_source_tag() |
| `postgres.e2e_testing.customers_filter_ins` | standard | Section 4.6 — filter_op(["c","u"]) |
| `postgres.e2e_testing.customers_filter_del` | standard | Section 4.6 — filter_op(["d"]) |
| `postgres.e2e_testing.orders_enriched` | standard | Section 4.7 — enrich_from_broadcast() |
| `postgres.e2e_testing.customers_before_after` | history_tracking | Section 4.8 — pivot_before_after() |
| `postgres.e2e_testing.customers_nullcoal` | standard | Section 4.9 — null_coalesce() |
| `postgres.e2e_testing.event_counts` | standard | Section 4.10 — aggregate_counts() |

**Source table columns** (PostgreSQL / Oracle `cache_testing.customers`):  
`id`, `name`, `email`, `phone`, `address`, `city`, `country`, `created_at`, `updated_at`

**Test row ID range**: 900001–909999 — high enough to never collide with production data.

---

## 1. Prerequisites Check

Run these checks before executing any test section. All checks must pass.

### 1.1 — Required CLI Tools

```bash
kubectl version --client --short
curl   --version | head -1
jq     --version
psql   --version
sqlplus -v          2>/dev/null || echo "sqlplus not found — Oracle tests require sqlplus"
mongosh --version
```

**Expected:** Each command prints a version string. If `sqlplus` is missing, Oracle tests in Section 1 must be run from inside the Oracle pod or a jump host.

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

### 1.4 — Streaming Job Healthy

```bash
kubectl logs -n prod -l app=kafka-to-iceberg-standard --tail=20 | grep -E "Batch|Error|Exception"
```

**Expected:** Recent `Batch N` completion lines; no `Error` or `Exception` lines.

---

## 2. Section 1 — Standard Mode Tests (SCD Type 0)

Confirm `kafka-to-iceberg-standard` is the only active deployment (replicas=1) before starting.  
All Iceberg queries in this section target the `e2e_testing` namespace.

### Test 1.1 — PostgreSQL

#### Step 1 — Note current row count

```sql
-- Spark SQL
SELECT COUNT(*) AS row_count FROM postgres.e2e_testing.customers_std;
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
FROM postgres.e2e_testing.customers_std
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
FROM postgres.e2e_testing.customers_std
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
FROM postgres.e2e_testing.customers_std
WHERE id = 900001;
```

**Expected:** `should_be_zero = 0`

#### Step 9 — Cleanup confirmation

```sql
SELECT id FROM postgres.e2e_testing.customers_std WHERE id = 900001;
-- Expected: 0 rows
```

---

### Test 1.2 — Oracle (CACHE_TESTING schema)

#### Step 1 — Note current row count

```sql
SELECT COUNT(*) AS row_count FROM oracle.e2e_testing.customers_std;
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
FROM oracle.e2e_testing.customers_std
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
FROM oracle.e2e_testing.customers_std
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
SELECT COUNT(*) AS should_be_zero FROM oracle.e2e_testing.customers_std WHERE id = 900002;
```

**Expected:** `should_be_zero = 0`

---

### Test 1.3 — MongoDB

#### Step 1 — Note current row count

```sql
SELECT COUNT(*) AS row_count FROM mongodb.e2e_testing.customers_std;
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
FROM mongodb.e2e_testing.customers_std
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
FROM mongodb.e2e_testing.customers_std
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
SELECT COUNT(*) AS should_be_zero FROM mongodb.e2e_testing.customers_std WHERE id = 900003;
```

**Expected:** `should_be_zero = 0`

---

## 3. Section 2 — Soft Delete Mode Tests

Target table: **`postgres.e2e_testing.customers_sd`**

### Setup: Switch to soft_delete mode

```bash
kubectl scale deployment kafka-to-iceberg-standard    -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-soft-delete -n prod --replicas=1
kubectl rollout status deployment/kafka-to-iceberg-soft-delete -n prod
```

Verify:
```bash
kubectl get deployment -n prod | grep kafka-to-iceberg
```
**Expected:** `soft-delete` shows `1/1 READY`; others show `0/0`.

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
kubectl rollout status deployment/kafka-to-iceberg-history-tracking -n prod
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

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TRANSFORM_PIPELINE=<function_name>
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

---

### Test 4.1 — `deduplicate` → `postgres.e2e_testing.customers_dedup`

**Purpose:** Rapid-fire updates to the same row within one micro-batch result in only the latest state landing in Iceberg.

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
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

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
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

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
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

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
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

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
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

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
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

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
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

#### Enable

Broadcast enrichment is configured in the streaming job code (not purely via env var). Patch the `TARGET_TABLE` to direct output to `orders_enriched`:

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
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

#### Enable (requires history_tracking mode)

```bash
kubectl scale deployment kafka-to-iceberg-standard          -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-history-tracking  -n prod --replicas=1
kubectl set env deployment/kafka-to-iceberg-history-tracking -n prod \
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

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
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

## 6. Section 5 — snap_id and snap_timestamp Validation

### Test 5.1 — Verify hourly partitions exist

```sql
SELECT partition, file_count, total_size
FROM postgres.e2e_testing.customers_std.partitions
ORDER BY partition DESC
LIMIT 10;
```

**Expected:** Partitions are named by `snap_timestamp_hour` (e.g. `snap_timestamp_hour=2025-06-15-10`) and bucket number. At least one partition per hour the pipeline has been active.

---

### Test 5.2 — Verify snap_id uniqueness within a batch

```sql
SELECT snap_timestamp, COUNT(*) AS total_rows, COUNT(DISTINCT snap_id) AS unique_snap_ids
FROM postgres.e2e_testing.customers_std
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
FROM postgres.e2e_testing.customers_std
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
FROM postgres.e2e_testing.customers_std
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
FROM postgres.e2e_testing.customers_std
WHERE id = 900060

UNION ALL

SELECT 'oracle' AS source, id, email, snap_timestamp
FROM oracle.e2e_testing.customers_std
WHERE id = 900061

UNION ALL

SELECT 'mongodb' AS source, id, email, snap_timestamp
FROM mongodb.e2e_testing.customers_std
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

All Iceberg verification queries in this section target **`postgres.e2e_testing.customers_std`**.

---

### Test 7a — PostgreSQL: ADD COLUMN

**Scenario:** Add a `loyalty_tier` column. Verify it propagates to `postgres.e2e_testing.customers_std`.

#### Step 1 — Baseline

```bash
psql -h postgresql.prod.svc.cluster.local -U rbac -d cache_testing -c "
SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'customers'
ORDER BY ordinal_position;"
```

```sql
DESCRIBE postgres.e2e_testing.customers_std;
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
DESCRIBE postgres.e2e_testing.customers_std;
-- Expected: loyalty_tier  string

SELECT id, name, loyalty_tier, snap_timestamp
FROM postgres.e2e_testing.customers_std
WHERE id = 900070;
-- Expected: loyalty_tier = 'GOLD'

SELECT id, loyalty_tier
FROM postgres.e2e_testing.customers_std
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
FROM postgres.e2e_testing.customers_std
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
ALTER TABLE postgres.e2e_testing.customers_std DROP COLUMN loyalty_tier;
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
DESCRIBE postgres.e2e_testing.customers_std;
-- Expected: address still string (VARCHAR and TEXT both map to string)

SELECT id, LEFT(address, 60) AS addr_preview
FROM postgres.e2e_testing.customers_std
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
DESCRIBE oracle.e2e_testing.customers_std;
-- Expected: loyalty_points  bigint

SELECT id, name, loyalty_points, snap_timestamp
FROM oracle.e2e_testing.customers_std
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
DESCRIBE mongodb.e2e_testing.customers_std;
-- Expected: loyalty_tier and referral_code columns now present

SELECT id, name, loyalty_tier, referral_code, snap_timestamp
FROM mongodb.e2e_testing.customers_std
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
SELECT COUNT(*) FROM postgres.e2e_testing.customers_std
WHERE id BETWEEN 901000 AND 901999;
-- Expected: 0
```

---

## 10. Expected Results Summary

| Test | Action | Expected Iceberg Result |
|---|---|---|
| **1.1 PG INSERT** | INSERT id=900001 | Row in `postgres.e2e_testing.customers_std`; `snap_id` ≠ NULL |
| **1.1 PG UPDATE** | UPDATE id=900001 email | `email` updated; `snap_id` changed; `snap_timestamp` newer |
| **1.1 PG DELETE** | DELETE id=900001 | Row gone; `COUNT = 0` |
| **1.2 ORA INSERT** | INSERT id=900002 | Row in `oracle.e2e_testing.customers_std` |
| **1.2 ORA UPDATE** | UPDATE id=900002 email | `email` updated |
| **1.2 ORA DELETE** | DELETE id=900002 | Row hard-deleted |
| **1.3 MDB INSERT** | insertOne id=900003 | Row in `mongodb.e2e_testing.customers_std` |
| **1.3 MDB UPDATE** | updateOne id=900003 | `email` updated |
| **1.3 MDB DELETE** | deleteOne id=900003 | Row hard-deleted |
| **2.1 Soft INSERT** | INSERT id=900010 | `customers_sd`: `is_deleted=false`; `deleted_at=NULL` |
| **2.2 Soft UPDATE** | UPDATE id=900010 | `email` updated; `is_deleted` still false |
| **2.3 Soft DELETE** | DELETE id=900010 | Row present; `is_deleted=true`; `deleted_at` non-null |
| **3.1 Hist INSERT** | INSERT id=900020 | `customers_hist`: `_change_type='INSERT'`; `before_*=NULL` |
| **3.2 Hist UPDATE** | UPDATE id=900020 | 2nd hist row: `_change_type='UPDATE'`; `before_email` populated |
| **3.3 Hist DELETE** | DELETE id=900020 | 3rd hist row: `_change_type='DELETE'`; `after_*=NULL` |
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
| **5.1 Partitions** | Query `.partitions` metadata | Hourly + bucket partitions visible in `customers_std` |
| **5.2 snap_id unique** | Uniqueness check | 0 duplicate snap_ids within any batch |
| **5.3 snap_timestamp** | Insert with `created_at=2020` | `snap_timestamp` ≈ now (not 2020) |
| **5.4 Partition pruning** | EXPLAIN with `snap_timestamp` filter | Partition pruning in plan |
| **6 Multi-source** | Simultaneous inserts PG/ORA/MDB | 3 rows across `postgres/oracle/mongodb.e2e_testing.customers_std` within 10 s |
| **7 Schema evo** | ALTER TABLE ADD COLUMN | New column in `customers_std`; old rows NULL |
| **8 Peak-hour** | MERGE_PARALLELISM=16 burst 1000 rows | Batch completes; no errors; setting confirmed in logs |
