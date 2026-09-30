# Runbook 34 — StarTransform Pipeline: Architecture & Functional Testing

> **Deployments:** `star-transform-pipeline-{postgres,oracle,mongodb}` in `prod`
> **Health ports:** postgres=8090 · oracle=8091 · mongodb=8092
> **Iceberg namespace:** `<catalog>.st_transforms` in S3 bucket `xdatatoiceberg1`
> **Related runbooks:** [05 — Kafka to Iceberg Streaming](runbook-05-kafka-to-iceberg-streaming.md) · [08 — Security & Access](runbook-08-security-access.md)

---

## 1. Architecture Overview

### What StarTransform is

StarTransform is a **Spark Structured Streaming library** of 26 reusable DataFrame transformation functions. The `star_transform_pipeline.py` script wires these functions against live CDC (Change Data Capture) events from three database sources, demonstrating every function against real data every 5 seconds.

### Data flow

```
PostgreSQL ──────────────────────────────────────────────────┐
  cache_testing.{customers,orders,products}                  │
  Debezium → Kafka topics: postgres.cache_testing.*          │
                                                             │
Oracle XEPDB1 CACHE_TESTING ──────────────────────────────── ├──► Kafka (SCRAM-SHA-512)
  CUSTOMERS, ORDERS, PRODUCTS                                │         │
  Debezium → Kafka topics: oracle.CACHE_TESTING.*            │         ▼
                                                             │   Spark Structured Streaming
MongoDB cache_testing ───────────────────────────────────────┘   foreachBatch (5s trigger)
  customers, orders, products                                          │
  Debezium → Kafka topics: mongodb.cache_testing.*                     │
                                                               26 ST functions applied
                                                                       │
                                                           ┌───────────┴───────────┐
                                                           ▼                       ▼
                                                    Kafka output topics      Iceberg tables
                                                  st.<fn>.<src>.<table>   <catalog>.st_transforms
                                                   (55 topics, all True)    .<fn>__<src>__<table>
```

### Three independent deployments

| Deployment | SOURCE | Health Port | Kafka Topic Pattern |
|---|---|---|---|
| `star-transform-pipeline-postgres` | `postgres` | `8090` | `postgres\.cache_testing\..*` |
| `star-transform-pipeline-oracle` | `oracle` | `8091` | `oracle\.(cache_testing\|CACHE_TESTING)\..*` |
| `star-transform-pipeline-mongodb` | `mongodb` | `8092` | `mongodb\.cache_testing\..*` |

Each deployment runs `replicas=1` with `strategy=Recreate` (to avoid two pods writing to the same S3 checkpoint).

### The 26 StarTransform functions

| # | Function | Target | Output sink |
|---|---|---|---|
| 1 | `filter_op` | customers | Kafka + Iceberg |
| 2 | `deduplicate` | customers | Kafka + Iceberg |
| 3 | `add_processing_time` | customers | Kafka + Iceberg |
| 4 | `rename_columns` | customers | Kafka + Iceberg |
| 5 | `cast_columns` | customers | Kafka + Iceberg |
| 6 | `drop_columns` | customers | Kafka + Iceberg |
| 7 | `mask_columns` | customers (PII: email, phone) | Kafka + Iceberg |
| 8 | `add_source_tag` | customers | Kafka + Iceberg |
| 9 | `add_op_label` | customers | Kafka + Iceberg |
| 10 | `flatten_json_col` | customers (postgres only) | Kafka + Iceberg |
| 11 | `filter_columns` | customers | Kafka + Iceberg |
| 12 | `null_coalesce` | customers | Kafka + Iceberg |
| 13 | `pivot_before_after` | customers (UPDATE events only) | Kafka + Iceberg |
| 14 | `apply_pipeline` | customers (chained 5-step) | Kafka + Iceberg |
| 15 | `windowed_aggregate` | orders | Iceberg only |
| 16 | `rolling_sum` | orders | Iceberg only |
| 17 | `rolling_avg` | orders | Iceberg only |
| 18 | `count_distinct_per_key` | orders | Iceberg only |
| 19 | `top_n_per_group` | orders | Kafka + Iceberg |
| 20 | `event_rate` | orders | Iceberg only |
| 21 | `aggregate_counts` | orders | Iceberg only |
| 22 | `stream_join` | orders ↔ customers | Kafka + Iceberg |
| 23 | `temporal_join` | orders ↔ customers (60s window) | Kafka + Iceberg |
| 24 | `join_and_tag_source` | orders ↔ customers | Kafka + Iceberg |
| 25 | `multi_topic_union` | customers ∪ orders | Kafka + Iceberg |
| 26 | `route_by_topic` | all (routing primitive, logged only) | — |

### Script delivery

The pipeline script is **not baked into the image**. It is injected at runtime via a ConfigMap mounted at `/opt/spark/work-dir/star_transform_pipeline.py`. After any script change, the ConfigMap must be updated and the pods restarted.

---

## 2. Current Status Check

### 2.1 Pod health (30-second check)

```bash
# All three deployments should be 1/1 Running
kubectl get deploy -n prod -l app=star-transform-pipeline

# Pod-level detail
kubectl get pods -n prod -l app=star-transform-pipeline -o wide

# Liveness probe — each returns "OK"
PG_POD=$(kubectl get pod -n prod -l app=star-transform-pipeline,pipeline.source=postgres -o jsonpath='{.items[0].metadata.name}')
ORA_POD=$(kubectl get pod -n prod -l app=star-transform-pipeline,pipeline.source=oracle  -o jsonpath='{.items[0].metadata.name}')
MGO_POD=$(kubectl get pod -n prod -l app=star-transform-pipeline,pipeline.source=mongodb -o jsonpath='{.items[0].metadata.name}')

kubectl exec -n prod "$PG_POD"  -- curl -sf http://localhost:8090/ && echo " [postgres OK]"
kubectl exec -n prod "$ORA_POD" -- curl -sf http://localhost:8091/ && echo " [oracle OK]"
kubectl exec -n prod "$MGO_POD" -- curl -sf http://localhost:8092/ && echo " [mongodb OK]"
```

Expected output:
```
OK [postgres OK]
OK [oracle OK]
OK [mongodb OK]
```

### 2.2 Kafka output topics (all 55 should be READY=True)

```bash
kubectl get kafkatopic -n prod -l "st.function" --no-headers | awk '{print $5}' | sort | uniq -c
# Expected: 55 True

# List all topic names
kubectl get kafkatopic -n prod -l "st.function" --no-headers | awk '{print $1}' | sort
```

### 2.3 Recent log activity

```bash
# Confirm batches are processing (look for "ST transforms complete")
PG_POD=$(kubectl get pod -n prod -l app=star-transform-pipeline,pipeline.source=postgres -o jsonpath='{.items[0].metadata.name}')
kubectl logs -n prod "$PG_POD" --tail=50 | grep -E "batch=|ST transforms complete|WARNING|ERROR"
```

---

## 3. Functional Tests

These tests verify each StarTransform function end-to-end by injecting CDC events and confirming output in Kafka and/or Iceberg.

### Prerequisites

```bash
# Store pod names
PG_POD=$(kubectl get pod -n prod -l app=star-transform-pipeline,pipeline.source=postgres -o jsonpath='{.items[0].metadata.name}')
ORA_POD=$(kubectl get pod -n prod -l app=star-transform-pipeline,pipeline.source=oracle  -o jsonpath='{.items[0].metadata.name}')
MGO_POD=$(kubectl get pod -n prod -l app=star-transform-pipeline,pipeline.source=mongodb -o jsonpath='{.items[0].metadata.name}')

# SCRAM credentials for Kafka consumer checks
KAFKA_BOOTSTRAP="strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092"
```

### 3.1 Trigger CDC events (source data)

Insert a test customer row into each source to generate CDC events that flow through the pipeline.

**PostgreSQL:**
```bash
kubectl exec -n prod statefulset/postgresql -- \
  psql -U postgres -d cache_testing -c "
    INSERT INTO customers (name, email, phone, tier, loyalty_score, preferred_lang)
    VALUES ('ST-Test-User', 'sttest@example.com', '555-0199', 'gold', 95.5, 'en')
    ON CONFLICT DO NOTHING;
    UPDATE customers SET loyalty_score = 96.0 WHERE email = 'sttest@example.com';
  "
```

**Oracle:**
```bash
kubectl exec -n prod deploy/oracle-xe -- \
  sqlplus -s system/Oracle18c@XEPDB1 <<'EOF'
INSERT INTO CACHE_TESTING.CUSTOMERS
  (FIRST_NAME, LAST_NAME, EMAIL, PHONE, TIER, LOYALTY_SCORE)
VALUES ('STTest','User','sttest@oracle.local','555-0199','GOLD',95.5);
UPDATE CACHE_TESTING.CUSTOMERS SET LOYALTY_SCORE=96.0 WHERE EMAIL='sttest@oracle.local';
COMMIT;
EOF
```

**MongoDB:**
```bash
kubectl exec -n prod deploy/mongodb -- mongosh cache_testing --eval "
  db.customers.insertOne({
    customer_id: 'st-test-001',
    first_name: 'STTest', last_name: 'User',
    email: 'sttest@mongo.local', phone: '555-0199',
    tier: 'gold', loyalty_score: 95.5
  });
  db.customers.updateOne(
    { customer_id: 'st-test-001' },
    { \$set: { loyalty_score: 96.0 } }
  );
"
```

Wait ~10 seconds for the 5-second trigger to fire and for Spark to process the batch.

---

### 3.2 Test: filter_op (keep INSERT + UPDATE only)

Verifies only `c` (create) and `u` (update) ops pass through.

```bash
# Check Iceberg for rows with _op in ('c','u') — no 'd' (delete) rows
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.sql(\"SELECT _op, count(*) as cnt FROM \\\`postgres\\\`.\\\`st_transforms\\\`.\\\`filter_op__postgres__customers\\\` GROUP BY _op\")
df.show()
"
# Expected: only rows with _op = 'c' or 'u'
```

**Quick Kafka check:**
```bash
kubectl exec -n prod "$PG_POD" -- \
  kafka-console-consumer.sh \
  --bootstrap-server "$KAFKA_BOOTSTRAP" \
  --topic st.filter_op.postgres.customers \
  --from-beginning --max-messages 3 --timeout-ms 10000 2>/dev/null | \
  python3 -c "import sys,json; [print(json.loads(l).get('_op','?')) for l in sys.stdin]"
# Expected: only 'c' or 'u'
```

---

### 3.3 Test: deduplicate (last-write-wins per PK)

Verifies that multiple events for the same PK in a batch are collapsed to the most recent.

```bash
# Insert two rapid-fire updates for the same customer
kubectl exec -n prod statefulset/postgresql -- \
  psql -U postgres -d cache_testing -c "
    UPDATE customers SET loyalty_score = 50 WHERE email = 'sttest@example.com';
    UPDATE customers SET loyalty_score = 99 WHERE email = 'sttest@example.com';
  "

# After next trigger: Iceberg row for this customer should show loyalty_score=99 (not 50)
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.sql(\"SELECT id, loyalty_score FROM \\\`postgres\\\`.\\\`st_transforms\\\`.\\\`deduplicate__postgres__customers\\\` WHERE email='sttest@example.com' ORDER BY kafka_ts DESC LIMIT 3\")
df.show()
"
```

---

### 3.4 Test: add_processing_time

Verifies a `proc_time` TIMESTAMP column is injected.

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.sql(\"SELECT id, proc_time FROM \\\`postgres\\\`.\\\`st_transforms\\\`.\\\`add_processing_time__postgres__customers\\\` LIMIT 3\")
df.show()
"
# Expected: proc_time column is NOT NULL and is a recent timestamp
```

---

### 3.5 Test: mask_columns (SHA-256 PII masking)

Verifies that `email` and `phone` are hashed (not plaintext).

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.sql(\"SELECT email, phone FROM \\\`postgres\\\`.\\\`st_transforms\\\`.\\\`mask_columns__postgres__customers\\\` LIMIT 3\")
df.show(truncate=False)
"
# Expected: email and phone are 64-character hex strings (SHA-256), NOT plaintext addresses
# Verify: len(email) == 64 and all characters are hex [0-9a-f]
```

---

### 3.6 Test: pivot_before_after (UPDATE events only)

Verifies that UPDATE events are expanded into side-by-side before/after columns.

```bash
# Trigger an update
kubectl exec -n prod statefulset/postgresql -- \
  psql -U postgres -d cache_testing -c "
    UPDATE customers SET tier = 'platinum' WHERE email = 'sttest@example.com';
  "

# Check Iceberg for before_tier vs after_tier columns
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.sql(\"SELECT _op, before_tier, after_tier FROM \\\`postgres\\\`.\\\`st_transforms\\\`.\\\`pivot_before_after__postgres__customers\\\` LIMIT 5\")
df.show()
"
# Expected: _op='u', before_tier='gold', after_tier='platinum'
```

---

### 3.7 Test: flatten_json_col (PostgreSQL only)

Verifies `metadata_json` TEXT column is parsed into `meta_loyalty_tier`, `meta_preferred_contact`, `meta_last_campaign` columns.

```bash
# Ensure a customer has metadata_json populated
kubectl exec -n prod statefulset/postgresql -- \
  psql -U postgres -d cache_testing -c "
    UPDATE customers
    SET metadata_json = '{\"loyalty_tier\":\"platinum\",\"preferred_contact\":\"email\",\"last_campaign\":\"summer2026\"}'
    WHERE email = 'sttest@example.com';
  "

kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.sql(\"SELECT meta_loyalty_tier, meta_preferred_contact, meta_last_campaign FROM \\\`postgres\\\`.\\\`st_transforms\\\`.\\\`flatten_json_col__postgres__customers\\\` LIMIT 3\")
df.show()
"
# Expected: individual columns meta_loyalty_tier='platinum', meta_preferred_contact='email'
```

---

### 3.8 Test: aggregate functions (windowed_aggregate, rolling_sum, rolling_avg)

Insert test orders to drive aggregate calculations.

```bash
# PostgreSQL orders
kubectl exec -n prod statefulset/postgresql -- \
  psql -U postgres -d cache_testing -c "
    INSERT INTO orders (customer_id, status, total_amount, product_id)
    VALUES (1,'pending',199.99,1),(1,'shipped',299.99,2),(2,'pending',99.50,1);
  "

# Check windowed_aggregate: revenue + count grouped by status
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.sql(\"SELECT status, total_revenue, order_count, avg_order_value FROM \\\`postgres\\\`.\\\`st_transforms\\\`.\\\`windowed_aggregate__postgres__orders\\\` ORDER BY status\")
df.show()
"
# Expected: rows per status with total_revenue, order_count, avg/min/max_order_value populated

# Check rolling_sum: cumulative revenue per customer
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.sql(\"SELECT customer_id, total_amount, rolling_sum_total_amount FROM \\\`postgres\\\`.\\\`st_transforms\\\`.\\\`rolling_sum__postgres__orders\\\` ORDER BY customer_id, kafka_ts\")
df.show()
"
```

---

### 3.9 Test: top_n_per_group (top 5 orders per status)

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.sql(\"SELECT status, total_amount, rank FROM \\\`postgres\\\`.\\\`st_transforms\\\`.\\\`top_n_per_group__postgres__orders\\\` ORDER BY status, rank\")
df.show()
"
# Expected: rank column 1-5 per status group, ordered by total_amount DESC
```

---

### 3.10 Test: stream_join (enrich orders with customer tier)

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.sql(\"SELECT id, status, total_amount, tier, loyalty_score FROM \\\`postgres\\\`.\\\`st_transforms\\\`.\\\`stream_join__postgres__orders\\\` LIMIT 5\")
df.show()
"
# Expected: orders rows enriched with tier and loyalty_score from customers
# tier/loyalty_score may be NULL for orders with no matching customer in the same batch (left join)
```

---

### 3.11 Test: temporal_join (match within 60-second window)

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.sql(\"SELECT id, status, tier, abs(unix_timestamp(kafka_ts) - unix_timestamp(right_kafka_ts)) as ts_diff_s FROM \\\`postgres\\\`.\\\`st_transforms\\\`.\\\`temporal_join__postgres__orders\\\` WHERE tier IS NOT NULL LIMIT 5\")
df.show()
"
# Expected: ts_diff_s <= 60 for all matched rows
```

---

### 3.12 Test: multi_topic_union (customers ∪ orders)

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.sql(\"SELECT source_topic, count(*) as cnt FROM \\\`postgres\\\`.\\\`st_transforms\\\`.\\\`multi_topic_union__postgres__all\\\` GROUP BY source_topic\")
df.show(truncate=False)
"
# Expected: two rows — one for postgres.cache_testing.customers, one for postgres.cache_testing.orders
```

---

### 3.13 Test: Oracle uppercase normalisation

Oracle CDC emits uppercase JSON keys (`CUSTOMER_ID`, `EMAIL`). The pipeline lowercases them before any ST function.

```bash
kubectl exec -n prod "$ORA_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.sql(\"SELECT * FROM \\\`oracle\\\`.\\\`st_transforms\\\`.\\\`filter_op__oracle__customers\\\` LIMIT 2\")
# Verify column names are all lowercase (no CUSTOMER_ID, EMAIL uppercase)
print('Columns:', df.columns)
df.show(2)
"
# Expected: columns are lowercase: customer_id, email, phone, tier, etc.
```

---

### 3.14 Test: apply_pipeline (5-step chained transform)

Verifies the composite pipeline: filter_op → deduplicate → add_processing_time → mask_columns → add_op_label.

```bash
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.sql(\"SELECT _op, op_label, proc_time, email FROM \\\`postgres\\\`.\\\`st_transforms\\\`.\\\`apply_pipeline__postgres__customers\\\` LIMIT 5\")
df.show(truncate=False)
"
# Expected:
#   _op       = 'c' or 'u' only (filter_op applied)
#   op_label  = 'INSERT' or 'UPDATE' (add_op_label applied)
#   proc_time = NOT NULL timestamp (add_processing_time applied)
#   email     = 64-char hex hash, NOT plaintext (mask_columns applied)
```

---

## 4. Batch Activity Verification

Check that all three pipelines are actively processing batches (no stuck queries).

```bash
for src in postgres oracle mongodb; do
  POD=$(kubectl get pod -n prod -l app=star-transform-pipeline,pipeline.source=$src \
        -o jsonpath='{.items[0].metadata.name}')
  echo "=== $src ($POD) ==="
  kubectl logs -n prod "$POD" --tail=20 | \
    grep -E "batch=|ST transforms complete|Attempt [0-9]+ failed|ERROR" | tail -5
done
```

**Healthy output looks like:**
```
=== postgres ===
[postgres] batch=142 topics=[...] customers=3 orders=2
[postgres] batch=142 ST transforms complete.
=== oracle ===
[oracle] batch=97 topics=[...] customers=1 orders=0
[oracle] batch=97 ST transforms complete.
=== mongodb ===
[mongodb] batch=105 topics=[...] customers=2 orders=1
[mongodb] batch=105 ST transforms complete.
```

**Warning signs to watch for:**
- `Attempt N failed` → pipeline crashed and is retrying (check the error message)
- `No streaming queries started` → Kafka or S3 connectivity issue
- `Schema inference failed` → Debezium envelope format changed
- No log output in >60s → pod may be hung; check liveness probe restarts

---

## 5. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Pod not Running | ConfigMap `star-transform-pipeline-script` missing | `kubectl get cm star-transform-pipeline-script -n prod` — recreate if absent (see §6) |
| `Attempt N failed: No streaming queries started` | S3 checkpoint unreachable or Kafka auth failure | Check MinIO/S3 connectivity; verify SCRAM creds in OpenBao |
| `Temporary failure in name resolution` on startup | DNS not ready at pod start; internal retry handles it | Normal — pipeline retries with exponential backoff; wait for success log |
| `Schema inference failed` | Debezium envelope format changed or empty batch | Check Debezium connector status; verify topics have data |
| Oracle columns uppercase in Iceberg | `_lower_udf` not applied | Confirm `source.key == "oracle"` branch fires; check `_decode_envelope` logs |
| Iceberg tables not created | `st_transforms` namespace creation failed | Run: `spark.sql("CREATE NAMESPACE IF NOT EXISTS \`<catalog>\`.\`st_transforms\`")` manually |
| Kafka output topic missing | `star-transform-kafka-topics.yaml` not applied | `kubectl apply -f manifests/cdc-batch-pipeline/star-transform-kafka-topics.yaml` |
| `pivot_before_after` table empty | No UPDATE events in batch | Trigger an UPDATE in source DB; function only fires on `_op = 'u'` |
| `flatten_json_col` table empty | `metadata_json` column absent or NULL | PostgreSQL only; verify `metadata_json` column is populated in `cache_testing.customers` |
| Health probe returning 503 | `_HEALTH["ok"] = False` — pipeline in error state | Check logs for the triggering exception; pipeline retries automatically |

### Useful commands

```bash
# Watch all three pipelines live
kubectl logs -n prod -l app=star-transform-pipeline --prefix --tail=20 -f

# Count Iceberg tables in st_transforms (postgres catalog example)
kubectl exec -n prod "$PG_POD" -- python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.sql('SHOW TABLES IN \`postgres\`.\`st_transforms\`')
print(f'Total tables: {df.count()}')
df.show(50, truncate=False)
"

# Check restart count (should be 0 or 1 from initial startup)
kubectl get pods -n prod -l app=star-transform-pipeline \
  -o jsonpath='{range .items[*]}{.metadata.name}: restarts={.status.containerStatuses[0].restartCount}{"\n"}{end}'

# Describe a specific deployment for events/conditions
kubectl describe deploy star-transform-pipeline-postgres -n prod | tail -20
```

---

## 6. Script Update Procedure

The pipeline script is delivered via ConfigMap, not baked into the image. After any change to [`docker/spark-gluten-velox/scripts/star_transform_pipeline.py`](../../docker/spark-gluten-velox/scripts/star_transform_pipeline.py):

```bash
# 1. Update the ConfigMap
kubectl create configmap star-transform-pipeline-script \
  --from-file=star_transform_pipeline.py=docker/spark-gluten-velox/scripts/star_transform_pipeline.py \
  -n prod --dry-run=client -o yaml | kubectl apply -f -

# 2. Restart all three pipelines (Recreate strategy — no overlap)
kubectl rollout restart deploy/star-transform-pipeline-postgres \
                          deploy/star-transform-pipeline-oracle \
                          deploy/star-transform-pipeline-mongodb \
  -n prod

# 3. Watch rollout
kubectl rollout status deploy/star-transform-pipeline-postgres -n prod
kubectl rollout status deploy/star-transform-pipeline-oracle -n prod
kubectl rollout status deploy/star-transform-pipeline-mongodb -n prod
```

---

## 7. Quick Reference

| Item | Value |
|---|---|
| Pipeline script | `docker/spark-gluten-velox/scripts/star_transform_pipeline.py` |
| K8s manifests | `manifests/cdc-batch-pipeline/star-transform-streaming.yaml` |
| Kafka topics manifest | `manifests/cdc-batch-pipeline/star-transform-kafka-topics.yaml` |
| ConfigMap | `star-transform-pipeline-script` in `prod` |
| Trigger interval | `5 seconds` (from `kafka-to-iceberg-config`) |
| Max offsets/trigger | `50000` (from `kafka-to-iceberg-config`) |
| S3 bucket | `xdatatoiceberg1` |
| Checkpoint path | `s3://xdatatoiceberg1/checkpoints/streaming/st_pipeline/<source>` |
| Iceberg namespace | `<catalog>.st_transforms` |
| Output Kafka topics | 55 total (all `READY=True`) — prefix `st.<fn>.<src>.<table>` |
| Image | `192.168.1.50:30500/spark-gluten-velox:3.5.1-22` |
| ST functions count | 26 |
| Sources | postgres · oracle · mongodb |
