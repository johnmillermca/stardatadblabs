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
8. [Section 7 — Schema Evolution Test](#8-section-7--schema-evolution-test)
9. [Section 8 — Peak-Hour Simulation](#9-section-8--peak-hour-simulation)
10. [Expected Results Summary](#10-expected-results-summary)

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

### Test 1.1 — PostgreSQL

#### Step 1 — Note current row count

```sql
-- Run in Spark SQL / spark-shell / spark-submit --class ... or pyspark
SELECT COUNT(*) AS row_count FROM postgres.cache_testing.customers;
```

Record the result. Example: `row_count = 1050`

#### Step 2 — INSERT a test row

```bash
psql -h postgresql.prod.svc.cluster.local -U rbac -d cache_testing
```

```sql
INSERT INTO customers (customer_id, first_name, last_name, email, phone, created_at)
VALUES (99999, 'E2E', 'TestUser', 'e2e_test@example.com', '555-0000', NOW());
COMMIT;
```

#### Step 3 — Wait for pipeline propagation

```bash
sleep 5
```

#### Step 4 — Verify INSERT in Iceberg

```sql
SELECT customer_id, first_name, last_name, email, snap_id, snap_timestamp
FROM postgres.cache_testing.customers
WHERE customer_id = 99999;
```

**Expected:** 1 row returned with `snap_id` populated (non-null BIGINT) and `snap_timestamp` within the last 30 seconds.

#### Step 5 — UPDATE the test row

```sql
-- psql
UPDATE customers SET email = 'e2e_updated@example.com' WHERE customer_id = 99999;
COMMIT;
```

#### Step 6 — Wait and verify UPDATE in Iceberg

```bash
sleep 5
```

```sql
-- Note the snap_id from Step 4 and compare
SELECT customer_id, email, snap_id, snap_timestamp
FROM postgres.cache_testing.customers
WHERE customer_id = 99999;
```

**Expected:** `email = 'e2e_updated@example.com'`; `snap_id` is different from the value recorded in Step 4; `snap_timestamp` is newer.

#### Step 7 — DELETE the test row

```sql
-- psql
DELETE FROM customers WHERE customer_id = 99999;
COMMIT;
```

#### Step 8 — Wait and verify hard DELETE in Iceberg

```bash
sleep 5
```

```sql
SELECT COUNT(*) AS should_be_zero
FROM postgres.cache_testing.customers
WHERE customer_id = 99999;
```

**Expected:** `should_be_zero = 0`

#### Step 9 — Cleanup confirmation

```sql
-- Confirm no test residue
SELECT customer_id FROM postgres.cache_testing.customers WHERE customer_id = 99999;
-- Expected: 0 rows
```

---

### Test 1.2 — Oracle (CACHE_TESTING schema)

#### Step 1 — Note current row count

```sql
SELECT COUNT(*) AS row_count FROM oracle.cache_testing.customers;
```

#### Step 2 — INSERT a test row

```bash
# Connect to Oracle (from sqlplus or from inside the oracle-xe pod)
kubectl exec -it -n prod deployment/oracle-xe -- sqlplus c##dbzcdc/<password>@XEPDB1
```

```sql
INSERT INTO CACHE_TESTING.CUSTOMERS (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, CREATED_AT)
VALUES (99998, 'E2E', 'OracleTest', 'e2e_oracle@example.com', '555-0001', SYSDATE);
COMMIT;
```

#### Step 3 — Wait

```bash
sleep 5
```

#### Step 4 — Verify INSERT in Iceberg

```sql
SELECT customer_id, first_name, email, snap_id, snap_timestamp
FROM oracle.cache_testing.customers
WHERE customer_id = 99998;
```

**Expected:** 1 row returned; `snap_id` non-null; `snap_timestamp` recent.

#### Step 5 — UPDATE

```sql
-- sqlplus / Oracle
UPDATE CACHE_TESTING.CUSTOMERS SET EMAIL = 'e2e_oracle_updated@example.com' WHERE CUSTOMER_ID = 99998;
COMMIT;
```

#### Step 6 — Verify UPDATE

```bash
sleep 5
```

```sql
SELECT customer_id, email, snap_id, snap_timestamp
FROM oracle.cache_testing.customers
WHERE customer_id = 99998;
```

**Expected:** `email = 'e2e_oracle_updated@example.com'`; `snap_id` changed; `snap_timestamp` newer.

#### Step 7 — DELETE

```sql
-- sqlplus
DELETE FROM CACHE_TESTING.CUSTOMERS WHERE CUSTOMER_ID = 99998;
COMMIT;
```

#### Step 8 — Verify hard DELETE

```bash
sleep 5
```

```sql
SELECT COUNT(*) AS should_be_zero FROM oracle.cache_testing.customers WHERE customer_id = 99998;
```

**Expected:** `should_be_zero = 0`

---

### Test 1.3 — MongoDB

#### Step 1 — Note current row count

```sql
SELECT COUNT(*) AS row_count FROM mongodb.cache_testing.customers;
```

#### Step 2 — INSERT a test document

```bash
mongosh --host mongodb.prod.svc.cluster.local:27017 \
        --username root --password <password> \
        --authenticationDatabase admin
```

```javascript
use cache_testing;
db.customers.insertOne({
  _id: ObjectId("000000000000000000099997"),
  customer_id: 99997,
  first_name: "E2E",
  last_name: "MongoTest",
  email: "e2e_mongo@example.com",
  phone: "555-0002",
  created_at: new Date()
});
```

#### Step 3 — Wait

```bash
sleep 5
```

#### Step 4 — Verify INSERT in Iceberg

```sql
SELECT customer_id, first_name, email, snap_id, snap_timestamp
FROM mongodb.cache_testing.customers
WHERE customer_id = 99997;
```

**Expected:** 1 row; `snap_id` and `snap_timestamp` populated.

#### Step 5 — UPDATE

```javascript
// mongosh
db.customers.updateOne(
  { customer_id: 99997 },
  { $set: { email: "e2e_mongo_updated@example.com" } }
);
```

#### Step 6 — Verify UPDATE

```bash
sleep 5
```

```sql
SELECT customer_id, email, snap_id, snap_timestamp
FROM mongodb.cache_testing.customers
WHERE customer_id = 99997;
```

**Expected:** `email = 'e2e_mongo_updated@example.com'`; `snap_id` changed.

#### Step 7 — DELETE

```javascript
// mongosh
db.customers.deleteOne({ customer_id: 99997 });
```

#### Step 8 — Verify hard DELETE

```bash
sleep 5
```

```sql
SELECT COUNT(*) AS should_be_zero FROM mongodb.cache_testing.customers WHERE customer_id = 99997;
```

**Expected:** `should_be_zero = 0`

---

## 3. Section 2 — Soft Delete Mode Tests

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
INSERT INTO customers (customer_id, first_name, last_name, email, phone, created_at)
VALUES (88888, 'E2E', 'SoftTest', 'soft_test@example.com', '555-0010', NOW());
COMMIT;
```

#### Step 2 — Wait and verify in Iceberg

```bash
sleep 5
```

```sql
SELECT customer_id, email, is_deleted, deleted_at, snap_id, snap_timestamp
FROM postgres.cache_testing.customers
WHERE customer_id = 88888;
```

**Expected:** 1 row; `is_deleted = false`; `deleted_at = NULL`; `snap_id` and `snap_timestamp` populated.

---

### Test 2.2 — Soft Delete: UPDATE

#### Step 1 — UPDATE the row

```sql
-- psql
UPDATE customers SET email = 'soft_updated@example.com' WHERE customer_id = 88888;
COMMIT;
```

#### Step 2 — Verify UPDATE

```bash
sleep 5
```

```sql
SELECT customer_id, email, is_deleted, deleted_at
FROM postgres.cache_testing.customers
WHERE customer_id = 88888;
```

**Expected:** `email = 'soft_updated@example.com'`; `is_deleted = false`; `deleted_at = NULL`.

---

### Test 2.3 — Soft Delete: DELETE

#### Step 1 — DELETE the row

```sql
-- psql
DELETE FROM customers WHERE customer_id = 88888;
COMMIT;
```

#### Step 2 — Verify soft delete in Iceberg

```bash
sleep 5
```

```sql
SELECT customer_id, email, is_deleted, deleted_at
FROM postgres.cache_testing.customers
WHERE customer_id = 88888;
```

**Expected:** Row **still present**; `is_deleted = true`; `deleted_at` is a non-null TIMESTAMP within the last 30 seconds.

#### Step 3 — Query all soft-deleted rows

```sql
SELECT customer_id, email, deleted_at
FROM postgres.cache_testing.customers
WHERE is_deleted = true
ORDER BY deleted_at DESC
LIMIT 20;
```

**Expected:** `customer_id = 88888` is in the result set.

---

### Teardown: Switch back to standard mode

```bash
kubectl scale deployment kafka-to-iceberg-soft-delete -n prod --replicas=0
kubectl scale deployment kafka-to-iceberg-standard    -n prod --replicas=1
kubectl rollout status deployment/kafka-to-iceberg-standard -n prod
```

---

## 4. Section 3 — History Tracking Mode Tests

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
INSERT INTO customers (customer_id, first_name, last_name, email, phone, created_at)
VALUES (77777, 'E2E', 'HistTest', 'hist_test@example.com', '555-0020', NOW());
COMMIT;
```

#### Step 2 — Wait and verify in _hist table

```bash
sleep 5
```

```sql
SELECT customer_id, _change_type, _change_ts,
       before_customer_id, before_email,
       after_customer_id, after_email,
       snap_id, snap_timestamp
FROM postgres.cache_testing.customers_hist
WHERE after_customer_id = 77777
ORDER BY _change_ts;
```

**Expected:** 1 row; `_change_type = 'INSERT'`; all `before_*` columns are NULL; `after_customer_id = 77777`; `after_email = 'hist_test@example.com'`.

---

### Test 3.2 — History: UPDATE

#### Step 1 — UPDATE the row

```sql
-- psql
UPDATE customers SET email = 'hist_updated@example.com' WHERE customer_id = 77777;
COMMIT;
```

#### Step 2 — Wait and verify UPDATE row in _hist

```bash
sleep 5
```

```sql
SELECT customer_id, _change_type, _change_ts,
       before_email, after_email
FROM postgres.cache_testing.customers_hist
WHERE after_customer_id = 77777
   OR before_customer_id = 77777
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
DELETE FROM customers WHERE customer_id = 77777;
COMMIT;
```

#### Step 2 — Wait and verify DELETE row in _hist

```bash
sleep 5
```

```sql
SELECT customer_id, _change_type, _change_ts,
       before_email, after_email
FROM postgres.cache_testing.customers_hist
WHERE after_customer_id = 77777
   OR before_customer_id = 77777
ORDER BY _change_ts;
```

**Expected:** 3 rows; the third row has `_change_type = 'DELETE'`; `before_email = 'hist_updated@example.com'`; all `after_*` columns are NULL.

---

### Test 3.4 — Full history for a single customer

```sql
SELECT _change_type, _change_ts, before_email, after_email, snap_id
FROM postgres.cache_testing.customers_hist
WHERE after_customer_id = 77777
   OR before_customer_id = 77777
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

All tests below use `kafka-to-iceberg-standard` in replicas=1. Adjust `TRANSFORM_PIPELINE` or the relevant environment variable, then perform a rolling restart to apply.

### How to apply TRANSFORM_PIPELINE changes

```bash
# Edit the deployment's TRANSFORM_PIPELINE env var
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TRANSFORM_PIPELINE=deduplicate,add_processing_time,mask_pii,add_op_label,add_source_tag

# Rolling restart to pick up the new value
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

---

### Test 4.1 — `deduplicate`

**Purpose:** Verify that rapid-fire updates to the same row within a single micro-batch result in only the latest state landing in Iceberg.

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod TRANSFORM_PIPELINE=deduplicate
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Fire 3 rapid updates

```sql
-- psql — execute quickly so all 3 land in the same 2-second micro-batch
UPDATE customers SET email = 'dedup_v1@example.com' WHERE customer_id = 1;
UPDATE customers SET email = 'dedup_v2@example.com' WHERE customer_id = 1;
UPDATE customers SET email = 'dedup_v3@example.com' WHERE customer_id = 1;
COMMIT;
```

> If customer_id=1 does not exist, INSERT it first then run the 3 updates.

#### Verify in Iceberg

```bash
sleep 5
```

```sql
SELECT customer_id, email, snap_id, snap_timestamp
FROM postgres.cache_testing.customers
WHERE customer_id = 1;
```

**Expected:** Exactly 1 row; `email = 'dedup_v3@example.com'` (the last write wins). If `dedup_v2` appears instead, the three updates may have landed in different batches — retry with faster successive commits.

---

### Test 4.2 — `mask_pii` (SHA-256 hash)

**Purpose:** Verify that PII columns (`email`, `phone`) are stored as SHA-256 hex digests in Iceberg.

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TRANSFORM_PIPELINE=deduplicate,mask_pii \
  PII_COLUMNS=email,phone
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert a customer with known PII

```sql
-- psql
INSERT INTO customers (customer_id, first_name, last_name, email, phone, created_at)
VALUES (66666, 'PII', 'MaskTest', 'pii_clear@example.com', '555-0030', NOW());
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
SELECT customer_id, email, phone
FROM postgres.cache_testing.customers
WHERE customer_id = 66666;
```

**Expected:** `email` column contains the SHA-256 hex string `3b37ebfda7f90dc9ce8d59e45d7f5ea5cddfae2f8f27e98d9671218f92c2a6ad` (not the plaintext). The `phone` column contains the SHA-256 hash of `555-0030`.

#### Cleanup

```sql
-- psql
DELETE FROM customers WHERE customer_id = 66666;
COMMIT;
```

---

### Test 4.3 — `add_processing_time`

**Purpose:** Verify that the `proc_time` column is injected into every Iceberg row.

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TRANSFORM_PIPELINE=deduplicate,add_processing_time
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert and verify

```sql
-- psql
INSERT INTO customers (customer_id, first_name, last_name, email, phone, created_at)
VALUES (55555, 'ProcTime', 'Test', 'proctime@example.com', '555-0040', NOW());
COMMIT;
```

```bash
sleep 5
```

```sql
SELECT customer_id, proc_time, snap_timestamp
FROM postgres.cache_testing.customers
WHERE customer_id = 55555;
```

**Expected:** `proc_time` is a non-null TIMESTAMP within 30 seconds of now. Note: `proc_time` is the StarTransform injection time (slightly earlier than `snap_timestamp` which is set at the Iceberg write step).

#### Cleanup

```sql
DELETE FROM customers WHERE customer_id = 55555; COMMIT;
```

---

### Test 4.4 — `add_op_label`

**Purpose:** Verify that the `op_label` column contains human-readable strings.

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TRANSFORM_PIPELINE=deduplicate,add_op_label
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert, update, and verify labels

```sql
-- psql
INSERT INTO customers (customer_id, first_name, last_name, email, phone, created_at)
VALUES (44444, 'OpLabel', 'Test', 'oplabel@example.com', '555-0050', NOW());
COMMIT;
```

```bash
sleep 5
```

```sql
SELECT customer_id, op_label FROM postgres.cache_testing.customers WHERE customer_id = 44444;
```

**Expected:** `op_label = 'INSERT'`

```sql
-- psql
UPDATE customers SET email = 'oplabel_updated@example.com' WHERE customer_id = 44444;
COMMIT;
```

```bash
sleep 5
```

```sql
SELECT customer_id, op_label FROM postgres.cache_testing.customers WHERE customer_id = 44444;
```

**Expected:** `op_label = 'UPDATE'`

#### Cleanup

```sql
DELETE FROM customers WHERE customer_id = 44444; COMMIT;
```

---

### Test 4.5 — `add_source_tag`

**Purpose:** Verify that the `source_system` column is injected.

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TRANSFORM_PIPELINE=deduplicate,add_source_tag \
  SOURCE=postgres
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert and verify

```sql
-- psql
INSERT INTO customers (customer_id, first_name, last_name, email, phone, created_at)
VALUES (33333, 'SourceTag', 'Test', 'sourcetag@example.com', '555-0060', NOW());
COMMIT;
```

```bash
sleep 5
```

```sql
SELECT customer_id, source_system FROM postgres.cache_testing.customers WHERE customer_id = 33333;
```

**Expected:** `source_system = 'postgres'`

#### Cleanup

```sql
DELETE FROM customers WHERE customer_id = 33333; COMMIT;
```

---

### Test 4.6 — `enrich_from_broadcast`

**Purpose:** Verify that a dimension broadcast join enriches stream rows at write time.

#### Enable

Broadcast enrichment is configured in code (not via env var alone). To test, temporarily patch the pipeline script or use an integration test. The following illustrates the expected outcome assuming the products table is used as the broadcast dimension:

```python
# Snippet — how it's called in the streaming job
products_dim = spark.table("postgres.cache_testing.products")
enriched_df = ST.enrich_from_broadcast(
    df,
    dim_df=products_dim,
    join_col="product_id",
    select_cols=["product_name", "category"],
    how="left"
)
```

#### Verify

After an INSERT to `orders` that contains a `product_id` that exists in `products`:

```sql
SELECT order_id, product_id, product_name, category
FROM postgres.cache_testing.orders
WHERE order_id = <test_order_id>;
```

**Expected:** `product_name` and `category` are populated from the broadcast join, not from the orders source table.

---

### Test 4.7 — `filter_op` (exclude DELETEs)

**Purpose:** Verify that when `filter_op(ops=["c","u"])` is active, DELETE events are dropped and do not reach Iceberg.

#### Enable

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod \
  TRANSFORM_PIPELINE=filter_op
# filter_op defaults to ops=["c","u"] — deletes are dropped
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

#### Insert, then delete

```sql
-- psql
INSERT INTO customers (customer_id, first_name, last_name, email, phone, created_at)
VALUES (22222, 'FilterOp', 'Test', 'filterop@example.com', '555-0070', NOW());
COMMIT;
```

```bash
sleep 5
```

```sql
-- Verify INSERT reached Iceberg
SELECT customer_id FROM postgres.cache_testing.customers WHERE customer_id = 22222;
-- Expected: 1 row
```

```sql
-- psql
DELETE FROM customers WHERE customer_id = 22222;
COMMIT;
```

```bash
sleep 5
```

```sql
-- DELETE should NOT have reached Iceberg because filter_op dropped it
SELECT customer_id FROM postgres.cache_testing.customers WHERE customer_id = 22222;
-- Expected: STILL 1 row (the delete was filtered out)
```

**Expected:** Row remains in Iceberg after the source DELETE because `filter_op` excluded the `d` op.

#### Cleanup — restore delete capability

```bash
kubectl set env deployment/kafka-to-iceberg-standard -n prod TRANSFORM_PIPELINE=deduplicate
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod && \
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

Then clean up the test row manually via Iceberg (requires Spark SQL):

```sql
DELETE FROM postgres.cache_testing.customers WHERE customer_id = 22222;
```

---

## 6. Section 5 — snap_id and snap_timestamp Validation

### Test 5.1 — Verify hourly partitions exist

```sql
SELECT partition, file_count, total_size
FROM postgres.cache_testing.customers.partitions
ORDER BY partition DESC
LIMIT 10;
```

**Expected:** Partitions are named by `snap_timestamp_hour` (e.g. `snap_timestamp_hour=2025-06-15-10`) and bucket number. At least one partition exists per hour the pipeline has been active.

---

### Test 5.2 — Verify snap_id uniqueness within a batch

```sql
-- snap_id should be unique within any single snap_timestamp bucket (= micro-batch)
SELECT snap_timestamp, COUNT(*) AS total_rows, COUNT(DISTINCT snap_id) AS unique_snap_ids
FROM postgres.cache_testing.customers
GROUP BY snap_timestamp
HAVING COUNT(*) != COUNT(DISTINCT snap_id);
```

**Expected:** 0 rows returned (no duplicates within a batch).

---

### Test 5.3 — Verify snap_timestamp is write time, not source event time

Insert a row and record the source event time vs. what lands in Iceberg:

```sql
-- psql — note the current time
SELECT NOW();
-- e.g.  2025-06-15 10:30:00.123
```

```sql
-- psql
INSERT INTO customers (customer_id, first_name, last_name, email, phone, created_at)
VALUES (11110, 'SnapTs', 'Test', 'snapts@example.com', '555-0080', '2020-01-01 00:00:00');
COMMIT;
-- Note: created_at is deliberately in 2020 to distinguish from snap_timestamp
```

```bash
sleep 5
```

```sql
SELECT customer_id, created_at, snap_timestamp
FROM postgres.cache_testing.customers
WHERE customer_id = 11110;
```

**Expected:** `created_at = 2020-01-01 00:00:00`; `snap_timestamp` is near the current time (2025), confirming it is the write-wall-clock, not the source column value.

#### Cleanup

```sql
-- psql
DELETE FROM customers WHERE customer_id = 11110; COMMIT;
```

---

### Test 5.4 — Verify hourly partition pruning

```sql
-- This query should scan ONLY the most recent hour's partition
EXPLAIN
SELECT customer_id, email
FROM postgres.cache_testing.customers
WHERE snap_timestamp >= (CURRENT_TIMESTAMP - INTERVAL 1 HOUR);
```

**Expected:** The execution plan shows `PartitionFilter` or `Dynamic partition pruning` referencing `snap_timestamp_hour`. File scan statistics should show far fewer files than a full table scan.

---

## 7. Section 6 — Multi-Source Validation

**Purpose:** Verify all three source connectors propagate changes to their respective Iceberg catalogs within 10 seconds.

### Step 1 — Simultaneous inserts into all three sources

Open three terminal windows (or run sequentially in rapid succession):

**Terminal 1 — PostgreSQL:**
```sql
-- psql
INSERT INTO customers (customer_id, first_name, last_name, email, phone, created_at)
VALUES (9001, 'MultiSrc', 'PG', 'multi_pg@example.com', '555-9001', NOW());
COMMIT;
```

**Terminal 2 — Oracle:**
```sql
-- sqlplus
INSERT INTO CACHE_TESTING.CUSTOMERS (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, CREATED_AT)
VALUES (9002, 'MultiSrc', 'ORA', 'multi_ora@example.com', '555-9002', SYSDATE);
COMMIT;
```

**Terminal 3 — MongoDB:**
```javascript
// mongosh
use cache_testing;
db.customers.insertOne({
  customer_id: 9003,
  first_name: "MultiSrc",
  last_name: "MDB",
  email: "multi_mdb@example.com",
  phone: "555-9003",
  created_at: new Date()
});
```

### Step 2 — Wait 10 seconds

```bash
sleep 10
```

### Step 3 — Verify all three in Iceberg

```sql
-- Check PostgreSQL catalog
SELECT 'postgres' AS source, customer_id, email, snap_timestamp
FROM postgres.cache_testing.customers
WHERE customer_id = 9001

UNION ALL

-- Check Oracle catalog
SELECT 'oracle' AS source, customer_id, email, snap_timestamp
FROM oracle.cache_testing.customers
WHERE customer_id = 9002

UNION ALL

-- Check MongoDB catalog
SELECT 'mongodb' AS source, customer_id, email, snap_timestamp
FROM mongodb.cache_testing.customers
WHERE customer_id = 9003;
```

**Expected:** 3 rows, one from each source, all with `snap_timestamp` within 10 seconds of the inserts.

### Step 4 — Cleanup

```sql
-- psql
DELETE FROM customers WHERE customer_id = 9001; COMMIT;
```
```sql
-- sqlplus
DELETE FROM CACHE_TESTING.CUSTOMERS WHERE CUSTOMER_ID = 9002; COMMIT;
```
```javascript
// mongosh
db.customers.deleteOne({ customer_id: 9003 });
```

---

## 8. Section 7 — Schema Evolution Test

**Purpose:** Verify that adding a column in PostgreSQL propagates through Debezium and is accepted by Iceberg via `mergeSchema`.

> **Note:** Schema changes propagate via the `schema-changes.postgres` Kafka topic. Iceberg handles the new column through `mergeSchema=true` on the Spark write. After evolution, the column will be NULL for rows written before the ALTER.

### Step 1 — Record current column list

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'customers'
ORDER BY ordinal_position;
```

Also check Iceberg schema:
```sql
DESCRIBE postgres.cache_testing.customers;
```

### Step 2 — Add a column in PostgreSQL

```sql
-- psql (must be run as a superuser or table owner)
ALTER TABLE customers ADD COLUMN loyalty_tier VARCHAR(20);
COMMIT;
```

### Step 3 — Insert a row that uses the new column

```sql
-- psql
INSERT INTO customers (customer_id, first_name, last_name, email, phone, created_at, loyalty_tier)
VALUES (8888, 'SchemaEvo', 'Test', 'evo@example.com', '555-8888', NOW(), 'GOLD');
COMMIT;
```

### Step 4 — Wait and verify schema evolution in Iceberg

```bash
sleep 10
```

```sql
-- The Iceberg table schema should now include loyalty_tier
DESCRIBE postgres.cache_testing.customers;
```

**Expected:** `loyalty_tier` column appears in the Iceberg schema with type `string` (or `varchar`).

```sql
SELECT customer_id, loyalty_tier
FROM postgres.cache_testing.customers
WHERE customer_id = 8888;
```

**Expected:** `customer_id = 8888`, `loyalty_tier = 'GOLD'`.

```sql
-- Rows written before the ALTER TABLE should have loyalty_tier = NULL
SELECT customer_id, loyalty_tier
FROM postgres.cache_testing.customers
WHERE customer_id != 8888
LIMIT 5;
```

**Expected:** `loyalty_tier = NULL` for pre-evolution rows.

### Step 5 — Cleanup

```sql
-- psql
DELETE FROM customers WHERE customer_id = 8888; COMMIT;
-- Optionally drop the column if no longer needed in the test environment
-- ALTER TABLE customers DROP COLUMN loyalty_tier;
```

---

## 9. Section 8 — Peak-Hour Simulation

**Purpose:** Verify that the pipeline can be tuned for higher throughput by adjusting parallelism settings.

### Step 1 — Check current ConfigMap settings

```bash
kubectl get configmap kafka-to-iceberg-config -n prod -o yaml
```

Note the current values of `MERGE_PARALLELISM` and `COALESCE_BEFORE_MERGE`.

### Step 2 — Apply peak-hour settings via kubectl patch

```bash
kubectl patch configmap kafka-to-iceberg-config -n prod --type merge -p '{
  "data": {
    "MERGE_PARALLELISM": "16",
    "COALESCE_BEFORE_MERGE": "8"
  }
}'
```

Verify the patch:
```bash
kubectl get configmap kafka-to-iceberg-config -n prod \
  -o jsonpath='{.data.MERGE_PARALLELISM} / {.data.COALESCE_BEFORE_MERGE}{"\n"}'
# Expected: 16 / 8
```

### Step 3 — Rolling restart to apply

```bash
kubectl rollout restart deployment/kafka-to-iceberg-standard -n prod
kubectl rollout status  deployment/kafka-to-iceberg-standard -n prod
```

### Step 4 — Verify new settings are active in logs

```bash
kubectl logs -n prod -l app=kafka-to-iceberg-standard --tail=50 \
  | grep -E "MERGE_PARALLELISM|COALESCE_BEFORE_MERGE"
```

**Expected:** Log lines confirming `MERGE_PARALLELISM=16` and `COALESCE_BEFORE_MERGE=8`.

### Step 5 — Generate a burst of inserts to observe throughput

```sql
-- psql — insert 1000 rows rapidly
DO $$
BEGIN
  FOR i IN 200000..201000 LOOP
    INSERT INTO customers (customer_id, first_name, last_name, email, phone, created_at)
    VALUES (i, 'BurstTest', 'Row' || i, 'burst_' || i || '@example.com', '555-' || i, NOW());
  END LOOP;
END $$;
COMMIT;
```

### Step 6 — Monitor batch duration in Spark logs

```bash
kubectl logs -n prod -l app=kafka-to-iceberg-standard -f | grep -E "Batch [0-9]+ took"
```

**Expected:** Batch duration should be similar to or lower than normal-load batches despite higher row counts. Compare with baseline batch duration from Step 4 log output.

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
DELETE FROM customers WHERE customer_id BETWEEN 200000 AND 201000;
COMMIT;
```

```bash
sleep 10
```

```sql
SELECT COUNT(*) FROM postgres.cache_testing.customers WHERE customer_id BETWEEN 200000 AND 201000;
-- Expected: 0
```

---

## 10. Expected Results Summary

| Test | Action | Expected Iceberg Result |
|---|---|---|
| **1.1 PG INSERT** | INSERT customer_id=99999 | Row appears; `snap_id` ≠ NULL; `snap_timestamp` recent |
| **1.1 PG UPDATE** | UPDATE customer_id=99999 email | `email` updated; `snap_id` changed; `snap_timestamp` newer |
| **1.1 PG DELETE** | DELETE customer_id=99999 | Row gone; `COUNT = 0` |
| **1.2 ORA INSERT** | INSERT customer_id=99998 | Row appears in `oracle.cache_testing.customers` |
| **1.2 ORA UPDATE** | UPDATE customer_id=99998 email | `email` updated in Iceberg |
| **1.2 ORA DELETE** | DELETE customer_id=99998 | Row hard-deleted from Iceberg |
| **1.3 MDB INSERT** | insertOne customer_id=99997 | Row appears in `mongodb.cache_testing.customers` |
| **1.3 MDB UPDATE** | updateOne customer_id=99997 email | `email` updated in Iceberg |
| **1.3 MDB DELETE** | deleteOne customer_id=99997 | Row hard-deleted from Iceberg |
| **2.1 Soft INSERT** | INSERT customer_id=88888 | `is_deleted=false`; `deleted_at=NULL` |
| **2.2 Soft UPDATE** | UPDATE customer_id=88888 | `email` updated; `is_deleted` still false |
| **2.3 Soft DELETE** | DELETE customer_id=88888 | Row present; `is_deleted=true`; `deleted_at` non-null |
| **3.1 Hist INSERT** | INSERT customer_id=77777 | `_hist` row: `_change_type='INSERT'`; `before_*=NULL` |
| **3.2 Hist UPDATE** | UPDATE customer_id=77777 | Second `_hist` row: `_change_type='UPDATE'`; `before_email` populated |
| **3.3 Hist DELETE** | DELETE customer_id=77777 | Third `_hist` row: `_change_type='DELETE'`; `after_*=NULL` |
| **4.1 deduplicate** | 3 rapid UPDATEs same row | Only last value in Iceberg |
| **4.2 mask_pii** | INSERT with email/phone | SHA-256 hex stored; no plaintext |
| **4.3 proc_time** | INSERT any row | `proc_time` TIMESTAMP column non-null |
| **4.4 op_label** | INSERT / UPDATE | `op_label = 'INSERT'` / `'UPDATE'` |
| **4.5 source_tag** | INSERT any row | `source_system = 'postgres'` |
| **4.7 filter_op** | DELETE while `filter_op(["c","u"])` | Row remains in Iceberg (delete suppressed) |
| **5.1 Partitions** | Query `.partitions` metadata | Hourly + bucket partitions visible |
| **5.2 snap_id unique** | Uniqueness check per batch | 0 duplicates |
| **5.3 snap_timestamp** | `created_at=2020` insert | `snap_timestamp` ≈ now (not 2020) |
| **5.4 Partition pruning** | EXPLAIN with `snap_timestamp` filter | Partition pruning visible in plan |
| **6 Multi-source** | Simultaneous inserts across 3 DBs | All 3 rows in Iceberg within 10 s |
| **7 Schema evo** | ALTER TABLE ADD COLUMN | New column appears in Iceberg; old rows NULL |
| **8 Peak-hour** | MERGE_PARALLELISM=16 burst | Batch completes; no errors; setting confirmed in logs |
