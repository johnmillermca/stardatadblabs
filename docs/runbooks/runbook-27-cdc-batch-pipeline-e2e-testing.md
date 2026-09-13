# Runbook 27 — CDC + Batch Pipeline End-to-End Testing

> **Version:** 1.0  
> **Status:** Active  
> **Owner:** Platform Engineering  
> **Related architecture:** [`docs/architecture/cdc-batch-pipeline.md`](../architecture/cdc-batch-pipeline.md)

---

## Purpose

This runbook provides step-by-step validation procedures for the complete Starpump + Debezium/Kafka → Iceberg CDC & Batch Pipeline. Run these tests in order after any of the following:

- Initial pipeline deployment
- Changes to `starpump.py`, `04_schema_evolution_handler.py`, or `05_kafka_to_iceberg_streaming.py`
- Changes to Debezium connector configs
- Changes to `bao_spark_init.py` catalog configuration

---

## Environment Variables

Set these before running any test:

```bash
export SPARK_MASTER_POD=$(kubectl get pod -n prod -l app=spark-master -o name | head -1)
export DEBEZIUM_URL="http://192.168.1.54:30083"
export BAO_ADDR="http://openbao.prod.svc.cluster.local:8200"
export SPARK_USER=dave

# Fetch OpenBao token (run once)
export BAO_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

# Shorthand to exec into Spark master pod
spark_exec() {
  kubectl exec -n prod "$SPARK_MASTER_POD" -- \
    env USER=dave SPARK_USER=dave TOKEN="$BAO_TOKEN" "$@"
}
```

---

## Test 1 — Pre-flight Checks

### 1.1 Verify Kafka cluster health

```bash
kubectl get pod -n prod -l strimzi.io/cluster=strimzi-kafka
# Expected: All pods Running

# Check broker health
kubectl exec -n prod strimzi-kafka-combined-0 -- \
  bin/kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092 \
  --command-config /opt/kafka/config/connect-distributed.properties 2>/dev/null | head -5
```

### 1.2 Verify Schema Registry

```bash
curl -s http://$(kubectl get svc -n prod schema-registry -o jsonpath='{.spec.clusterIP}'):8081/subjects | python3 -m json.tool
# Expected: [] or existing subjects list — no error
```

### 1.3 Verify Debezium Connect

```bash
curl -s "$DEBEZIUM_URL/connectors" | python3 -m json.tool
# Expected: JSON array (possibly empty)

curl -s "$DEBEZIUM_URL/connector-plugins" | python3 -c \
  "import sys,json; plugins=[p['class'].split('.')[-1] for p in json.load(sys.stdin)]; print('\n'.join(plugins))"
# Expected output includes:
#   PostgresConnector
#   OracleConnector
#   MongoDbConnector
```

### 1.4 Verify Polaris REST catalog connectivity

```bash
spark_exec python3 /opt/spark/scripts/00_catalog_bootstrap.py
# Expected: All 3 catalogs bootstrapped and validated successfully.
# postgres.cache_testing → ✓ OK
# oracle.tpcds           → ✓ OK
# mongodb.cache_testing  → ✓ OK
```

### 1.5 Verify pipeline DB accessibility

```bash
spark_exec python3 -c "
import psycopg2, os, urllib.request, json
bao_addr = 'http://openbao.prod.svc.cluster.local:8200'
tok = os.environ.get('TOKEN','')
req = urllib.request.Request(f'{bao_addr}/v1/secret/data/platform/pipeline_db',
    headers={'X-Vault-Token': tok})
with urllib.request.urlopen(req) as r:
    data = json.loads(r.read())['data']['data']
conn = psycopg2.connect(host=data['host'], port=data.get('port',5432),
    dbname=data['database'], user=data['user'], password=data['password'])
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM pipeline_watermarks')
print('pipeline_watermarks rows:', cur.fetchone()[0])
cur.execute('SELECT COUNT(*) FROM pipeline_run_log')
print('pipeline_run_log rows:   ', cur.fetchone()[0])
conn.close()
print('Pipeline DB: OK')
"
```

---

## Test 2 — Initial Full Load (Starpump)

### 2.1 Run Starpump full load — PostgreSQL

```bash
spark_exec python3 /opt/spark/scripts/starpump.py postgres \
  --mode full
# Expected:
#   [catalog-check] 'postgres' is registered. Proceeding.
#   Copying 4/4 table(s) with 8 threads, 100000 rows/batch [mode=full]
#   ✓ customers     rows=...  status=success
#   ✓ products      rows=...  status=success
#   ✓ product_reviews rows=... status=success
#   ✓ orders        rows=...  status=success
```

**Verify watermarks:**

```bash
kubectl exec -n prod "$(kubectl get pod -n prod -l app=postgresql -o name | head -1)" -- \
  psql -U pipeline -d pipeline -c \
  "SELECT table_name, sf_extraction_ts, rows_copied FROM pipeline_watermarks
   WHERE source_db='cache_testing' ORDER BY table_name;"
# Expected: 4 rows with non-null sf_extraction_ts and rows_copied > 0
```

### 2.2 Run Starpump full load — Oracle

```bash
spark_exec python3 /opt/spark/scripts/starpump.py oracle \
  --mode full
# Expected: 10 oracle.tpcds tables copied successfully
```

### 2.3 Run Starpump full load — MongoDB

```bash
spark_exec python3 /opt/spark/scripts/starpump.py mongodb \
  --mode full
# Expected: 2 collections copied
#   mongodb.cache_testing.customers  rows=444385
#   mongodb.cache_testing.products   rows=19849651
```

### 2.4 Verify Iceberg tables exist

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('verify')).getOrCreate()
for cat, ns, tbls in [
    ('postgres','cache_testing',['customers','products','product_reviews','orders']),
    ('oracle','tpcds',['income_band','ship_mode']),
    ('mongodb','cache_testing',['customers','products']),
]:
    for t in tbls:
        fqn = f'\`{cat}\`.\`{ns}\`.\`{t}\`'
        n = spark.table(fqn).count()
        print(f'{fqn}: {n:,} rows')
spark.stop()
"
# Expected: All tables with row counts matching source
```

---

## Test 3 — CDC Real-Time Test (Debezium → Kafka → Spark → Iceberg)

### 3.1 Register Debezium connectors

```bash
# Register all three connectors
bash /opt/spark/scripts/debezium/register_postgres_connector.sh
bash /opt/spark/scripts/debezium/register_oracle_connector.sh
bash /opt/spark/scripts/debezium/register_mongodb_connector.sh

# Verify all three are RUNNING
for conn in postgres-cache-testing-cdc oracle-tpcds-cdc mongodb-cache-testing-cdc; do
  STATE=$(curl -s "$DEBEZIUM_URL/connectors/$conn/status" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'])" 2>/dev/null || echo "MISSING")
  echo "$conn: $STATE"
done
# Expected: all three show RUNNING
```

### 3.2 Start the Kafka → Iceberg streaming job

```bash
# Run in background (or as a separate kubectl exec in a new terminal)
spark_exec python3 /opt/spark/scripts/05_kafka_to_iceberg_streaming.py &
STREAMING_PID=$!
sleep 15   # allow streams to start and subscribe
echo "Streaming job running: PID=$STREAMING_PID"
```

### 3.3 Insert test rows into PostgreSQL

```bash
kubectl exec -n prod "$(kubectl get pod -n prod -l app=postgresql -o name | head -1)" -- \
  psql -U postgres -d cache_testing -c "
INSERT INTO public.customers (name, email, phone, tier, created_at, updated_at)
VALUES ('CDC Test User', 'cdctest@example.com', '+1-555-0001', 'GOLD',
        NOW(), NOW());
SELECT id, name, email FROM customers WHERE email='cdctest@example.com';
"
export TEST_PG_ROW_ID=$(kubectl exec -n prod "$(kubectl get pod -n prod -l app=postgresql -o name | head -1)" -- \
  psql -U postgres -d cache_testing -At -c \
  "SELECT id FROM customers WHERE email='cdctest@example.com' LIMIT 1")
echo "Inserted PostgreSQL row id=$TEST_PG_ROW_ID"
```

### 3.4 Wait for CDC event to propagate

```bash
sleep 30  # allow Debezium → Kafka → Spark streaming trigger interval (10s) + margin

# Verify the row appeared in Iceberg
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('cdc-test')).getOrCreate()
df = spark.sql(\"SELECT id, name, email, snap_timestamp FROM \`postgres\`.\`cache_testing\`.\`customers\` WHERE email='cdctest@example.com'\")
df.show()
spark.stop()
"
# Expected: 1 row with email='cdctest@example.com' and a recent snap_timestamp
```

### 3.5 Test UPDATE event propagation

```bash
kubectl exec -n prod "$(kubectl get pod -n prod -l app=postgresql -o name | head -1)" -- \
  psql -U postgres -d cache_testing -c \
  "UPDATE customers SET tier='PLATINUM', updated_at=NOW() WHERE id=$TEST_PG_ROW_ID"

sleep 30

spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('cdc-test')).getOrCreate()
# Iceberg append-mode: both the old and new image are present
df = spark.sql(\"SELECT id, tier, snap_timestamp FROM \`postgres\`.\`cache_testing\`.\`customers\` WHERE email='cdctest@example.com' ORDER BY snap_timestamp DESC\")
df.show()
spark.stop()
"
# Expected: ≥2 rows — original GOLD row + new PLATINUM row (CDC is append-mode)
```

---

## Test 4 — Scheduled Incremental Load Test

### 4.1 Deploy CronJob manifests

```bash
kubectl apply -f manifests/cdc-batch-pipeline/kafka-topics.yaml
kubectl apply -f manifests/cdc-batch-pipeline/starpump-incremental.yaml

kubectl get cronjob -n prod -l app=starpump
# Expected: 3 CronJobs (postgres, oracle, mongodb) all showing SUSPEND=False
```

### 4.2 Inject new rows into PostgreSQL

```bash
kubectl exec -n prod "$(kubectl get pod -n prod -l app=postgresql -o name | head -1)" -- \
  psql -U postgres -d cache_testing -c "
INSERT INTO public.products (sku, name, category, price, stock_qty, weight_kg, created_at)
VALUES ('INCR-TEST-001', 'Incremental Test Product', 'Testing', 99.99, 100, 0.5, NOW());
"
```

### 4.3 Manually trigger the PostgreSQL incremental CronJob

```bash
kubectl create job -n prod --from=cronjob/starpump-incremental-postgres \
  starpump-incr-pg-manual-001

# Watch the job
kubectl logs -n prod -l job-name=starpump-incr-pg-manual-001 -f

# Expected log lines:
#   [mode=incremental] Incremental mode: col=created_at last_ts=...
#   ✓ products  rows=1  status=success
#   Completed in ...s — 4/4 copied | mode=incremental
```

### 4.4 Verify only the new row was appended

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('incr-test')).getOrCreate()
df = spark.sql(\"SELECT sku, name, snap_timestamp FROM \`postgres\`.\`cache_testing\`.\`products\` WHERE sku='INCR-TEST-001'\")
df.show()
spark.stop()
"
# Expected: exactly 1 row with sku='INCR-TEST-001'
```

---

## Test 5 — Schema Evolution Test

### 5.1 Start schema evolution handler

```bash
spark_exec python3 /opt/spark/scripts/04_schema_evolution_handler.py &
SEH_PID=$!
sleep 10
echo "Schema evolution handler running: PID=$SEH_PID"
```

### 5.2 ADD COLUMN test (PostgreSQL)

```bash
# Add a new column to the source table
kubectl exec -n prod "$(kubectl get pod -n prod -l app=postgresql -o name | head -1)" -- \
  psql -U postgres -d cache_testing -c \
  "ALTER TABLE public.customers ADD COLUMN loyalty_points INTEGER DEFAULT 0;"

# Wait for Debezium to detect the DDL and publish to schema-changes.postgres
sleep 30

# Verify the column appeared in Iceberg
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('evo-test')).getOrCreate()
df = spark.sql(\"DESCRIBE TABLE \`postgres\`.\`cache_testing\`.\`customers\`\")
df.filter(df.col_name=='loyalty_points').show()
spark.stop()
"
# Expected: 1 row showing loyalty_points INTEGER (or bigint)
```

### 5.3 DROP COLUMN test (PostgreSQL)

```bash
kubectl exec -n prod "$(kubectl get pod -n prod -l app=postgresql -o name | head -1)" -- \
  psql -U postgres -d cache_testing -c \
  "ALTER TABLE public.customers DROP COLUMN IF EXISTS loyalty_points;"

sleep 30

spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('evo-test')).getOrCreate()
df = spark.sql(\"DESCRIBE TABLE \`postgres\`.\`cache_testing\`.\`customers\`\")
df.filter(df.col_name=='loyalty_points').show()
spark.stop()
"
# Expected: empty result (column dropped from Iceberg)
```

### 5.4 Starpump DDL drift detection test

```bash
# Add column again for testing
kubectl exec -n prod "$(kubectl get pod -n prod -l app=postgresql -o name | head -1)" -- \
  psql -U postgres -d cache_testing -c \
  "ALTER TABLE public.customers ADD COLUMN new_drift_col VARCHAR(100);"

# Run starpump with DDL_DRIFT_DETECT — should detect and emit ALTER TABLE
DDL_DRIFT_DETECT=1 DRY_RUN=1 spark_exec \
  python3 /opt/spark/scripts/starpump.py postgres \
  --mode full --threads 1

# Expected log:
#   [customers] DDL drift detected — 1 change(s): [('add', 'new_drift_col')]
#   [customers] DRY_RUN — skipping ALTER TABLE.

# Cleanup
kubectl exec -n prod "$(kubectl get pod -n prod -l app=postgresql -o name | head -1)" -- \
  psql -U postgres -d cache_testing -c \
  "ALTER TABLE public.customers DROP COLUMN IF EXISTS new_drift_col;"
```

---

## Test 6 — Custom SQL JOIN Load Test

### 6.1 Run a JOIN query landing results into Iceberg

```bash
spark_exec python3 /opt/spark/scripts/starpump.py postgres \
  --mode custom_sql \
  --custom-sql "SELECT o.id AS order_id, o.status, o.total_amount,
                       c.name AS customer_name, c.tier AS customer_tier,
                       o.created_at
                FROM public.orders o
                JOIN public.customers c ON c.id = o.customer_id
                WHERE o.total_amount > 100" \
  --target-table orders_enriched

# Expected:
#   [custom-sql] Target table: orders_enriched | Query: SELECT o.id ...
#   [custom-sql] Done — N rows written to postgres.cache_testing.orders_enriched
```

**Verify:**

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('join-test')).getOrCreate()
df = spark.sql(\"SELECT order_id, customer_name, customer_tier, total_amount FROM \`postgres\`.\`cache_testing\`.\`orders_enriched\` LIMIT 5\")
df.show()
n = spark.sql(\"SELECT COUNT(*) AS n FROM \`postgres\`.\`cache_testing\`.\`orders_enriched\`\").collect()[0]['n']
print(f'Total rows in orders_enriched: {n:,}')
spark.stop()
"
# Expected: rows with order_id, customer_name, customer_tier populated
```

---

## Test 7 — Performance Validation

### 7.1 Verify Kafka producer metrics (Debezium)

```bash
curl -s "$DEBEZIUM_URL/connectors/postgres-cache-testing-cdc/status" | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(json.dumps(d, indent=2))"
# Expected: connector.state=RUNNING, tasks[0].state=RUNNING
```

### 7.2 Check Kafka topic throughput

```bash
kubectl exec -n prod strimzi-kafka-combined-0 -- \
  bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic postgres.cache_testing.customers \
  --time -1
# Expected: non-zero offset (messages have been produced)
```

### 7.3 Verify Iceberg file sizes

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('perf-check')).getOrCreate()
for fqn in ['\`postgres\`.\`cache_testing\`.\`customers\`',
            '\`mongodb\`.\`cache_testing\`.\`products\`']:
    files_df = spark.sql(f'SELECT count(*) AS file_count, avg(file_size_in_bytes)/1024/1024 AS avg_mb FROM {fqn}.files')
    print(fqn)
    files_df.show()
spark.stop()
"
# Expected:
#   avg_mb should be close to 256 MB for large tables
#   file_count reasonable (not thousands of tiny files)
```

### 7.4 Verify partition distribution

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('part-check')).getOrCreate()
df = spark.sql(\"SELECT partition, record_count FROM \`postgres\`.\`cache_testing\`.\`customers\`.partitions ORDER BY record_count DESC LIMIT 10\")
df.show(truncate=False)
spark.stop()
"
# Expected: multiple partitions showing hours(snap_timestamp) + bucket(16, ...) values
```

---

## Test 8 — Idempotency and Resumability

### 8.1 Re-run catalog bootstrap (should be no-op)

```bash
spark_exec python3 /opt/spark/scripts/00_catalog_bootstrap.py
# Expected: No errors, "Namespace ... ready." messages (CREATE IF NOT EXISTS)
```

### 8.2 Re-run starpump full (should only write new rows since offset resume)

```bash
# For PostgreSQL (supports offset resume): should skip already-copied rows
spark_exec python3 /opt/spark/scripts/starpump.py postgres \
  --mode full --threads 1

# Expected log:
#   [customers] RESUME: N rows already in Iceberg — reusing extraction_ts=... offset=N
# (No duplication — rows are appended from current offset only)
```

### 8.3 Re-run Debezium connector registration (idempotent)

```bash
bash /opt/spark/scripts/debezium/register_postgres_connector.sh
# Expected: deletes old connector, re-registers, status=RUNNING
# Watermarks shown at end confirm proper sync-point
```

---

## Cleanup (after testing)

```bash
# Remove test rows from PostgreSQL
kubectl exec -n prod "$(kubectl get pod -n prod -l app=postgresql -o name | head -1)" -- \
  psql -U postgres -d cache_testing -c "
DELETE FROM customers WHERE email='cdctest@example.com';
DELETE FROM products WHERE sku='INCR-TEST-001';
DELETE FROM customers WHERE name LIKE 'CDC Test%';
"

# Stop streaming job and schema evolution handler
kill $STREAMING_PID $SEH_PID 2>/dev/null || true

# Optionally remove test Iceberg table
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('cleanup')).getOrCreate()
spark.sql('DROP TABLE IF EXISTS \`postgres\`.\`cache_testing\`.\`orders_enriched\`')
print('Cleanup done.')
spark.stop()
"
```

---

## Troubleshooting

| Symptom | Likely Cause | Resolution |
|---------|-------------|------------|
| `No credential configured for catalog 'X'` | Catalog not wired in `BaoSparkInit.spark_conf()` | Run `00_catalog_bootstrap.py` to verify catalog connectivity; check `bao_spark_init.py` |
| Debezium connector stays in `FAILED` | Replication slot missing or max_wal_senders limit | Check `pg_replication_slots`; increase `max_wal_senders` in postgresql.conf |
| Oracle `TIMESTAMP_TO_SCN` returns empty | SCN history table too old | Use `CURRENT_SCN` fallback (script handles automatically) |
| MongoDB connector `FAILED` | Replica set not initiated | Run `rs.initiate()` on MongoDB standalone; or use `snapshot.mode=initial` |
| Kafka topic not found | `auto.create.topics.enable=false` | Apply `manifests/cdc-batch-pipeline/kafka-topics.yaml` |
| Iceberg write `PERMISSION_DENIED` | Polaris RBAC policy | Check `SPARK_USER=dave` has `can_write_iceberg=true` in `spark-rbac-allowlist` ConfigMap |
| `schema-changes.*` topic has no messages | Debezium not capturing DDL | Ensure `include.schema.changes=true` in connector config; check connector log |
| Streaming job OOM | Too many offsets per trigger | Reduce `MAX_OFFSETS_PER_TRIGGER` (default 50000) |
| Incremental load copies 0 rows | Watermark column not found | Set `WATERMARK_COL=created_at` (or the actual timestamp column name) |
