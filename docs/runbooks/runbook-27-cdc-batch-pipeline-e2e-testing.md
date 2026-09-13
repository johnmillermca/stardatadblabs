# Runbook 27 — CDC + Batch Pipeline End-to-End Testing

> **Version:** 1.2
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

## Test Checklist

| # | Area | Test |
|---|---|---|
| 1.1 | Pre-flight | Kafka cluster health |
| 1.2 | Pre-flight | Schema Registry reachable |
| 1.3 | Pre-flight | Debezium Connect reachable + plugins present |
| 1.4 | Pre-flight | Polaris REST catalog bootstrap |
| 1.5 | Pre-flight | Pipeline DB (`pipeline_watermarks`, `pipeline_run_log`) accessible |
| 2.1 | Starpump | Catalog pre-flight check passes before any copy |
| 2.2 | Starpump | Full load — PostgreSQL (4 tables, row counts, Iceberg verified) |
| 2.3 | Starpump | Full load — Oracle (10 tables, row counts, Iceberg verified) |
| 2.4 | Starpump | Full load — MongoDB (2 collections, row counts, Iceberg verified) |
| 2.5 | Starpump | Watermarks written to `pipeline_watermarks` after full load |
| 2.6 | Starpump | `pipeline_run_log` records each table run (status, duration, rows) |
| 2.7 | Starpump | Iceberg tables created with correct partition spec (`hours` + `bucket`) |
| 2.8 | Starpump | `snap_id` and `snap_timestamp` columns present on every Iceberg table |
| 2.9 | Starpump | Incremental load — PostgreSQL (new rows only, watermark advanced) |
| 2.10 | Starpump | Incremental load — Oracle (new rows only, watermark advanced) |
| 2.11 | Starpump | Incremental load — MongoDB (new rows only, watermark advanced) |
| 2.12 | Starpump | Incremental load copies 0 rows when nothing is new |
| 2.13 | Starpump | Custom SQL — single-table SELECT with WHERE condition |
| 2.14 | Starpump | Custom SQL — multi-table JOIN lands correct enriched rows |
| 2.15 | Starpump | DDL drift detection — ADD COLUMN detected and ALTER TABLE emitted |
| 2.16 | Starpump | DDL drift detection — DROP COLUMN detected and ALTER TABLE emitted |
| 2.17 | Starpump | Full load is idempotent — re-run does not duplicate rows |
| 2.18 | Starpump | `--threads` concurrency — 8-thread load finishes faster than 1-thread |
| 9.1 | Write Modes | PK auto-detection logged for each source |
| 9.2 | Write Modes | `standard` — UPDATE in Iceberg when source row changes (PostgreSQL) |
| 9.3 | Write Modes | `standard` — hard DELETE from Iceberg when source row is deleted (PostgreSQL) |
| 9.4 | Write Modes | `standard` — UPDATE + DELETE on Oracle (`customer_id` / `order_id` PKs) |
| 9.5 | Write Modes | `soft_delete` — `is_deleted=true` + `deleted_at` set on deleted row (PostgreSQL) |
| 9.6 | Write Modes | `soft_delete` — `is_deleted=true` set on Oracle deleted row |
| 9.7 | Write Modes | `history` — INSERT + UPDATE + DELETE all append new Iceberg rows with `_change_type` |
| 9.8 | Write Modes | MongoDB incremental — `standard` mode MERGE via `_id` PK |
| 9.9 | Write Modes | `--pk-cols` explicit override used for composite-key table |
| 9.10 | Write Modes | Watermark boundary `>=` — row at exact boundary is not skipped |
| 3.1 | CDC | Debezium connectors registered and RUNNING |
| 3.2 | CDC | Kafka → Iceberg streaming job starts |
| 3.3 | CDC | INSERT propagates Postgres → Kafka → Iceberg within 60 s |
| 3.4 | CDC | UPDATE propagates — new image appended to Iceberg |
| 4.1 | Incremental | CronJob manifests deployed |
| 4.2 | Incremental | New Postgres row picked up by incremental CronJob |
| 4.3 | Incremental | Watermark advances after CronJob succeeds |
| 5.1 | Schema Evo | Schema evolution handler starts |
| 5.2 | Schema Evo | ADD COLUMN flows to Iceberg via Debezium DDL topic |
| 5.3 | Schema Evo | DROP COLUMN flows to Iceberg |
| 5.4 | Schema Evo | Starpump DDL drift detection catches column added outside Debezium |
| 6.1 | Custom SQL | JOIN query result lands in Iceberg target table |
| 7.1 | Performance | Debezium connector running, no lag |
| 7.2 | Performance | Kafka topic offsets advancing |
| 7.3 | Performance | Iceberg avg file size ≈ 256 MB for large tables |
| 7.4 | Performance | Partition distribution correct |
| 8.1 | Idempotency | Catalog bootstrap re-run is a no-op |
| 8.2 | Idempotency | Starpump full re-run does not duplicate rows |
| 8.3 | Idempotency | Debezium re-registration is idempotent |

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

## Test 2 — Starpump: Full Load, Incremental, Custom SQL, DDL Drift

> **What this section covers:** All Starpump operating modes across all three source databases.
> Tests must be run in order within this section — full load first (establishes watermarks needed
> by incremental), then incremental, then custom SQL, then DDL drift.
>
> **Starpump modes:**
> | Mode | Flag | Description |
> |---|---|---|
> | `full` | `--mode full` | Copy all rows from every table in the source connector |
> | `incremental` | `--mode incremental` | Copy only rows newer than the last `sf_extraction_ts` watermark |
> | `custom_sql` | `--mode custom_sql` | Execute a user-supplied SQL (including JOINs) and land results into a named Iceberg table |

---

### T-2.1 — Catalog pre-flight: verify Spark Iceberg catalogs exist before any copy

The catalog bootstrap must run successfully before any data copy is attempted. This is enforced
automatically inside `starpump.py`, but you can verify it independently:

```bash
spark_exec python3 /opt/spark/scripts/00_catalog_bootstrap.py
```

**Expected output:**

```
[bootstrap] postgres.cache_testing  → namespace ready
[bootstrap] oracle.tpcds            → namespace ready
[bootstrap] mongodb.cache_testing   → namespace ready
All 3 catalog namespaces verified.
```

✅ Pass: all 3 namespaces show `ready` — no exceptions.
❌ Fail: `Connection refused` to Polaris → check `polaris-auth-proxy` pod in `prod` namespace:
```bash
kubectl get pod -n prod -l app=polaris-auth-proxy
```

---

### T-2.2 — Full load: PostgreSQL (`cache_testing` — 4 tables)

```bash
spark_exec python3 /opt/spark/scripts/starpump.py postgres \
  --mode full \
  --threads 8
```

**Expected log lines (all 4 must appear):**

```
[catalog-check] 'postgres' is registered. Proceeding.
Copying 4/4 table(s) with 8 threads, 100000 rows/batch [mode=full]
✓ customers       rows=<N>    duration=<Xs>  status=success
✓ products        rows=<N>    duration=<Xs>  status=success
✓ product_reviews rows=<N>    duration=<Xs>  status=success
✓ orders          rows=<N>    duration=<Xs>  status=success
Completed in <T>s — 4/4 copied | mode=full
```

**Verify Iceberg row counts match the source:**

```bash
# Step 1 — get source counts
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -At -c "
SELECT 'customers'||':'||COUNT(*)     FROM public.customers      UNION ALL
SELECT 'products'||':'||COUNT(*)      FROM public.products       UNION ALL
SELECT 'product_reviews'||':'||COUNT(*) FROM public.product_reviews UNION ALL
SELECT 'orders'||':'||COUNT(*)        FROM public.orders;
"
```

```bash
# Step 2 — get Iceberg counts
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t22-verify')).getOrCreate()
for t in ['customers','products','product_reviews','orders']:
    n = spark.sql(f\"SELECT COUNT(*) AS n FROM \`postgres\`.\`cache_testing\`.\`{t}\`\").collect()[0]['n']
    print(f'postgres.cache_testing.{t}: {n:,} rows')
spark.stop()
"
```

✅ Pass: Iceberg row count equals or exceeds source count for all 4 tables.
❌ Fail: count mismatch → check `pipeline_run_log` for partial-failure rows; re-run with `--threads 1` to see per-batch errors.

---

### T-2.3 — Full load: Oracle (`XEPDB1/TPCDS` — 10 tables)

```bash
spark_exec python3 /opt/spark/scripts/starpump.py oracle \
  --mode full \
  --threads 4
```

**Expected:** all 10 `oracle.tpcds` tables (`call_center`, `catalog_page`, `household_demographics`,
`income_band`, `promotion`, `reason`, `ship_mode`, `warehouse`, `web_page`, `web_site`) show
`status=success`.

**Spot-check two tables:**

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t23-verify')).getOrCreate()
for t in ['warehouse','ship_mode','income_band']:
    n = spark.sql(f\"SELECT COUNT(*) AS n FROM \`oracle\`.\`tpcds\`.\`{t}\`\").collect()[0]['n']
    print(f'oracle.tpcds.{t}: {n:,} rows')
spark.stop()
"
```

✅ Pass: all 10 tables copied, row counts > 0 (except tables that may be empty in the TPC-DS extract).
❌ Fail: `ORA-01017` — wrong credentials → check `secret/data/platform/oracle` in OpenBao.

---

### T-2.4 — Full load: MongoDB (`cache_testing` — 2 collections)

```bash
spark_exec python3 /opt/spark/scripts/starpump.py mongodb \
  --mode full \
  --threads 4
```

**Expected:**

```
✓ customers   rows=444,385    status=success
✓ products    rows=19,849,651 status=success
```

**Verify Iceberg:**

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t24-verify')).getOrCreate()
for t in ['customers','products']:
    n = spark.sql(f\"SELECT COUNT(*) AS n FROM \`mongodb\`.\`cache_testing\`.\`{t}\`\").collect()[0]['n']
    print(f'mongodb.cache_testing.{t}: {n:,} rows')
spark.stop()
"
# Expected:
#   mongodb.cache_testing.customers: 444,385 rows
#   mongodb.cache_testing.products:  19,849,651 rows
```

✅ Pass: both collections loaded with matching row counts.
❌ Fail: 0 rows for `products` → the `--connect-timeout` on the MongoDB driver may have been hit; retry with `MAX_BATCH_SIZE=50000` env var to reduce per-batch load.

---

### T-2.5 — Watermarks written to `pipeline_watermarks` after full load

After all three full loads, verify that `pipeline_watermarks` contains a current timestamp and
row count for every table.

```bash
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)
kubectl exec -n prod "$PG_POD" -- psql -U pipeline -d pipeline -c "
SELECT
    source_db,
    source_schema,
    table_name,
    sf_extraction_ts,
    rows_copied,
    status
FROM pipeline_watermarks
ORDER BY source_db, table_name;
" 2>/dev/null
```

**Expected:**

| source_db | source_schema | table_name | sf_extraction_ts | rows_copied | status |
|---|---|---|---|---|---|
| cache_testing | cache_testing | customers | `<recent ts>` | 444385 | success |
| cache_testing | cache_testing | products | `<recent ts>` | 19849651 | success |
| cache_testing | public | customers | `<recent ts>` | `<N>` | success |
| cache_testing | public | orders | `<recent ts>` | `<N>` | success |
| cache_testing | public | product_reviews | `<recent ts>` | `<N>` | success |
| cache_testing | public | products | `<recent ts>` | `<N>` | success |
| XEPDB1 | TPCDS | warehouse | `<recent ts>` | `<N>` | success |
| … | … | … | … | … | … |

✅ Pass: every table has a non-null `sf_extraction_ts` and `status = success`.
❌ Fail: `sf_extraction_ts` is NULL → the full load exited before the watermark write step; check for Python exceptions in the starpump log.

---

### T-2.6 — `pipeline_run_log` records every table run

```bash
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)
kubectl exec -n prod "$PG_POD" -- psql -U pipeline -d pipeline -c "
SELECT
    run_id,
    source_db,
    table_name,
    mode,
    start_ts,
    end_ts,
    rows_written,
    status,
    EXTRACT(EPOCH FROM (end_ts - start_ts))::INT AS duration_s
FROM pipeline_run_log
ORDER BY start_ts DESC
LIMIT 20;
"
```

✅ Pass: one row per table per run, all showing `status = success` and `end_ts IS NOT NULL`.
❌ Fail: rows with `status = error` → `error_message` column shows the exception; fix and re-run that table.

---

### T-2.7 — Iceberg partition spec: `hours(snap_timestamp)` + `bucket(16, <pk>)`

Every Iceberg table created by Starpump must use the two-level partition spec.

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t27-verify')).getOrCreate()
checks = [
    ('postgres','cache_testing','customers'),
    ('oracle','tpcds','warehouse'),
    ('mongodb','cache_testing','products'),
]
for cat, ns, tbl in checks:
    rows = spark.sql(f'SHOW CREATE TABLE \`{cat}\`.\`{ns}\`.\`{tbl}\`').collect()
    ddl = ' '.join(r[0] for r in rows)
    has_hours  = 'hours' in ddl.lower()
    has_bucket = 'bucket' in ddl.lower()
    print(f'{cat}.{ns}.{tbl}  hours={has_hours}  bucket={has_bucket}')
spark.stop()
"
# Expected:
#   postgres.cache_testing.customers  hours=True  bucket=True
#   oracle.tpcds.warehouse            hours=True  bucket=True
#   mongodb.cache_testing.products    hours=True  bucket=True
```

✅ Pass: both `hours=True` and `bucket=True` for every checked table.
❌ Fail: `bucket=False` → the `IcebergTableBuilder` call in `starpump.py` did not receive the PK
column; check that the connector's `default_pk` is set correctly.

---

### T-2.8 — `snap_id` and `snap_timestamp` columns present on every Iceberg table

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t28-verify')).getOrCreate()
checks = [
    ('postgres','cache_testing','customers'),
    ('postgres','cache_testing','products'),
    ('oracle','tpcds','warehouse'),
    ('mongodb','cache_testing','customers'),
    ('mongodb','cache_testing','products'),
]
for cat, ns, tbl in checks:
    cols = [r['col_name'] for r in spark.sql(f'DESCRIBE TABLE \`{cat}\`.\`{ns}\`.\`{tbl}\`').collect()]
    has_snap_id  = 'snap_id' in cols
    has_snap_ts  = 'snap_timestamp' in cols
    status = '✅' if (has_snap_id and has_snap_ts) else '❌'
    print(f'{status} {cat}.{ns}.{tbl}  snap_id={has_snap_id}  snap_timestamp={has_snap_ts}')
spark.stop()
"
```

✅ Pass: all 5 tables show `snap_id=True snap_timestamp=True`.
❌ Fail: column missing → `starpump.py` `_add_snap_cols()` was not called; check `IcebergTableBuilder` invocation.

---

### T-2.9 — Incremental load: PostgreSQL — only new rows copied

> **Prerequisite:** T-2.2 full load must have run first (watermark must exist).

**Step 1 — Record the current Iceberg row count before injecting test data:**

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t29a')).getOrCreate()
n = spark.sql(\"SELECT COUNT(*) AS n FROM \`postgres\`.\`cache_testing\`.\`products\`\").collect()[0]['n']
print(f'BEFORE: postgres.cache_testing.products rows = {n:,}')
spark.stop()
"
```

**Step 2 — Inject a new row into the source:**

```bash
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
INSERT INTO public.products (sku, name, category, price, stock_qty, weight_kg, created_at)
VALUES ('INCR-T29-001', 'Incremental Test Widget', 'Testing', 49.99, 10, 0.1, NOW());
SELECT id, sku, created_at FROM products WHERE sku='INCR-T29-001';
"
```

**Step 3 — Run the incremental load:**

```bash
spark_exec python3 /opt/spark/scripts/starpump.py postgres \
  --mode incremental \
  --watermark-col created_at \
  --threads 4
```

**Expected log:**

```
[mode=incremental] products: watermark=<prior ts> → WHERE created_at > '<prior ts>'
✓ products  rows=1  duration=<Xs>  status=success
Completed in <T>s — 1 new row(s) | mode=incremental
```

**Step 4 — Verify the new row appeared in Iceberg:**

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t29b')).getOrCreate()
df = spark.sql(\"SELECT sku, name, snap_timestamp FROM \`postgres\`.\`cache_testing\`.\`products\` WHERE sku='INCR-T29-001'\")
df.show()
spark.stop()
"
# Expected: exactly 1 row — sku=INCR-T29-001
```

**Step 5 — Verify watermark advanced:**

```bash
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)
kubectl exec -n prod "$PG_POD" -- psql -U pipeline -d pipeline -c "
SELECT table_name, sf_extraction_ts, rows_copied
FROM pipeline_watermarks
WHERE source_db='cache_testing' AND table_name='products';"
# Expected: sf_extraction_ts is NOW() (updated by this run)
```

✅ Pass: new row in Iceberg, watermark advanced, `rows=1` in log (not the full table count).
❌ Fail: `rows=0` → watermark column mismatch; check `WATERMARK_COL` env var or `--watermark-col` arg.
❌ Fail: all rows copied again (full scan) → incremental WHERE clause not applied; check `_incremental_where_clause()` in `starpump.py`.

---

### T-2.10 — Incremental load: Oracle

```bash
spark_exec python3 /opt/spark/scripts/starpump.py oracle \
  --mode incremental \
  --threads 2
```

**Expected:** log shows `WHERE <ts_col> > '<last watermark ts>'` for each table.
If no new Oracle rows exist since the full load, expect `rows=0` per table — this is correct.

```bash
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)
kubectl exec -n prod "$PG_POD" -- psql -U pipeline -d pipeline -c "
SELECT table_name, sf_extraction_ts FROM pipeline_watermarks
WHERE source_db='XEPDB1' ORDER BY table_name;"
# Expected: sf_extraction_ts updated to current time
```

✅ Pass: query exits 0, watermark timestamps advanced, log shows `mode=incremental`.

---

### T-2.11 — Incremental load: MongoDB

```bash
spark_exec python3 /opt/spark/scripts/starpump.py mongodb \
  --mode incremental \
  --watermark-col updated_at \
  --threads 2
```

**Expected:** log shows `WHERE updated_at > '<last watermark ts>'` applied to each collection.

✅ Pass: exits 0, watermark advanced.
❌ Fail: watermark column `updated_at` not found → confirm column exists on both `customers` and `products` (both have `updated_at` per confirmed schema).

---

### T-2.12 — Incremental load copies 0 rows when nothing is new

Immediately re-run incremental for PostgreSQL without inserting any new rows:

```bash
spark_exec python3 /opt/spark/scripts/starpump.py postgres \
  --mode incremental \
  --watermark-col created_at
```

**Expected log:**

```
[mode=incremental] customers:       watermark=<ts> → 0 new rows — skipping
[mode=incremental] products:        watermark=<ts> → 0 new rows — skipping
[mode=incremental] product_reviews: watermark=<ts> → 0 new rows — skipping
[mode=incremental] orders:          watermark=<ts> → 0 new rows — skipping
Completed in <T>s — 0 new row(s) | mode=incremental
```

✅ Pass: `0 new row(s)` in all tables — no unnecessary S3 writes.
❌ Fail: rows > 0 again → watermark was not persisted correctly; inspect `pipeline_watermarks`.

---

### T-2.13 — Custom SQL: single-table SELECT with WHERE condition

```bash
spark_exec python3 /opt/spark/scripts/starpump.py postgres \
  --mode custom_sql \
  --custom-sql "SELECT id, name, tier, email FROM public.customers WHERE tier = 'GOLD'" \
  --target-table gold_customers
```

**Expected log:**

```
[custom-sql] Target table: gold_customers | Query: SELECT id, name, tier ...
[custom-sql] Done — <N> rows written to postgres.cache_testing.gold_customers
```

**Verify:**

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t213-verify')).getOrCreate()
df = spark.sql(\"SELECT tier, COUNT(*) AS n FROM \`postgres\`.\`cache_testing\`.\`gold_customers\` GROUP BY tier\")
df.show()
spark.stop()
"
# Expected: only 'GOLD' tier rows — no other tiers present
```

✅ Pass: `gold_customers` Iceberg table contains only `tier='GOLD'` rows.
❌ Fail: table not created → `--target-table` not parsed; check `custom_sql` mode branch in `starpump.py`.

---

### T-2.14 — Custom SQL: multi-table JOIN lands enriched rows in Iceberg

This test confirms that Starpump's `custom_sql` mode can execute a JOIN across multiple source
tables and write the result to a new Iceberg table — the equivalent of a Fivetran dbt model.

```bash
spark_exec python3 /opt/spark/scripts/starpump.py postgres \
  --mode custom_sql \
  --custom-sql "
    SELECT
        o.id                                     AS order_id,
        o.status                                 AS order_status,
        ROUND(CAST(o.total_amount AS NUMERIC),2) AS total_amount,
        c.name                                   AS customer_name,
        c.tier                                   AS customer_tier,
        c.email                                  AS customer_email,
        p.name                                   AS product_name,
        p.category                               AS product_category,
        ROUND(CAST(p.price AS NUMERIC),2)        AS unit_price,
        ROUND(CAST(pr.rating AS NUMERIC),1)      AS avg_rating,
        o.created_at                             AS order_ts
    FROM public.orders         o
    JOIN public.customers      c  ON c.id  = o.customer_id
    LEFT JOIN public.products  p  ON p.id  = o.customer_id
    LEFT JOIN public.product_reviews pr ON pr.product_id = p.id
    WHERE o.status IN ('COMPLETED','SHIPPED')
    ORDER BY o.total_amount DESC
    LIMIT 5000
  " \
  --target-table orders_enriched_join

# Expected:
#   [custom-sql] Target table: orders_enriched_join | Query: SELECT o.id ...
#   [custom-sql] Done — <N> rows written to postgres.cache_testing.orders_enriched_join
```

**Verify enriched columns exist and are populated:**

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t214-verify')).getOrCreate()
df = spark.sql(\"\"\"
    SELECT order_id, customer_name, customer_tier, product_name, total_amount, snap_timestamp
    FROM \`postgres\`.\`cache_testing\`.\`orders_enriched_join\`
    ORDER BY total_amount DESC
    LIMIT 5
\"\"\")
df.show(truncate=False)
n = spark.sql(\"SELECT COUNT(*) AS n FROM \`postgres\`.\`cache_testing\`.\`orders_enriched_join\`\").collect()[0]['n']
print(f'Total rows in orders_enriched_join: {n:,}')
# Verify no NULL customer_name (inner join on customers must resolve)
nulls = spark.sql(\"SELECT COUNT(*) AS n FROM \`postgres\`.\`cache_testing\`.\`orders_enriched_join\` WHERE customer_name IS NULL\").collect()[0]['n']
print(f'Rows with NULL customer_name: {nulls} (expected: 0)')
spark.stop()
"
```

**Check that `snap_id` and `snap_timestamp` were injected into the JOIN result:**

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t214-snap')).getOrCreate()
cols = [r['col_name'] for r in spark.sql(\"DESCRIBE TABLE \`postgres\`.\`cache_testing\`.\`orders_enriched_join\`\").collect()]
print('Has snap_id:        ', 'snap_id' in cols)
print('Has snap_timestamp: ', 'snap_timestamp' in cols)
spark.stop()
"
# Expected:
#   Has snap_id:         True
#   Has snap_timestamp:  True
```

✅ Pass: enriched table exists, `customer_name` non-null, `snap_id`/`snap_timestamp` present.
❌ Fail: `N=0` rows → orders table may be empty; substitute a simpler JOIN or seed orders first.
❌ Fail: `customer_name IS NULL` > 0 → JOIN condition wrong; verify `c.id = o.customer_id` matches the actual FK.

---

### T-2.15 — DDL drift detection: ADD COLUMN detected, `ALTER TABLE` emitted

Starpump's `--mode full` with `DDL_DRIFT_DETECT=1` must detect a new column added to the source
table since the Iceberg table was created, and emit (or apply) an `ALTER TABLE … ADD COLUMN`.

**Step 1 — Add a column to the source (without going through Debezium DDL):**

```bash
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
ALTER TABLE public.customers ADD COLUMN loyalty_points INTEGER DEFAULT 0;
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name='customers' AND column_name='loyalty_points';"
# Expected: 1 row confirming loyalty_points exists
```

**Step 2 — Run Starpump in dry-run drift-detect mode (no writes, just detection):**

```bash
DDL_DRIFT_DETECT=1 DRY_RUN=1 spark_exec \
  python3 /opt/spark/scripts/starpump.py postgres \
  --mode full --threads 1
```

**Expected log:**

```
[customers] DDL drift detected — 1 change(s): [('add', 'loyalty_points', 'INTEGER')]
[customers] DRY_RUN — would emit: ALTER TABLE `postgres`.`cache_testing`.`customers` ADD COLUMN loyalty_points BIGINT
```

**Step 3 — Apply the drift (real run):**

```bash
DDL_DRIFT_DETECT=1 spark_exec \
  python3 /opt/spark/scripts/starpump.py postgres \
  --mode full --threads 1
```

**Step 4 — Verify the column was added to Iceberg:**

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t215-verify')).getOrCreate()
df = spark.sql(\"DESCRIBE TABLE \`postgres\`.\`cache_testing\`.\`customers\`\")
df.filter(df.col_name == 'loyalty_points').show()
spark.stop()
"
# Expected: 1 row — loyalty_points  bigint (or int)
```

✅ Pass: `loyalty_points` column appears in Iceberg table schema.
❌ Fail: column not found → check `_detect_ddl_drift()` diff logic; run with `LOG_LEVEL=DEBUG` to see schema comparison output.

---

### T-2.16 — DDL drift detection: DROP COLUMN detected, `ALTER TABLE DROP` emitted

```bash
# Step 1 — Drop the column from source
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
ALTER TABLE public.customers DROP COLUMN IF EXISTS loyalty_points;"

# Step 2 — Detect and apply
DDL_DRIFT_DETECT=1 spark_exec \
  python3 /opt/spark/scripts/starpump.py postgres \
  --mode full --threads 1
# Expected log:
#   [customers] DDL drift detected — 1 change(s): [('drop', 'loyalty_points')]
#   Applying: ALTER TABLE `postgres`.`cache_testing`.`customers` DROP COLUMN loyalty_points

# Step 3 — Verify column gone from Iceberg
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t216-verify')).getOrCreate()
df = spark.sql(\"DESCRIBE TABLE \`postgres\`.\`cache_testing\`.\`customers\`\")
df.filter(df.col_name == 'loyalty_points').show()
spark.stop()
"
# Expected: empty result (column dropped)
```

✅ Pass: empty result — column no longer in Iceberg.

---

### T-2.17 — Full load is idempotent: re-run does not duplicate rows

Re-run the full load for PostgreSQL and confirm row counts do not increase (Starpump uses
append semantics — idempotency is achieved by watermark-guarded incremental on re-runs,
or by the Iceberg dedup compaction policy).

```bash
# Get count before re-run
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t217a')).getOrCreate()
n = spark.sql(\"SELECT COUNT(*) AS n FROM \`postgres\`.\`cache_testing\`.\`customers\`\").collect()[0]['n']
print(f'BEFORE re-run: {n:,} rows')
spark.stop()
"

# Re-run with --mode full
spark_exec python3 /opt/spark/scripts/starpump.py postgres \
  --mode full --threads 1

# Get count after re-run
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t217b')).getOrCreate()
n = spark.sql(\"SELECT COUNT(*) AS n FROM \`postgres\`.\`cache_testing\`.\`customers\`\").collect()[0]['n']
print(f'AFTER re-run:  {n:,} rows')
spark.stop()
"
# Expected: count equal or close to before — no significant duplication
# (Starpump full-load uses RESUME logic from watermark offset)
```

✅ Pass: row count stable (no runaway duplication).

---

### T-2.18 — Concurrency: 8-thread load finishes faster than 1-thread

```bash
# 1-thread run — time it
echo "=== 1 thread ==="
time spark_exec python3 /opt/spark/scripts/starpump.py postgres \
  --mode full --threads 1

# 8-thread run — time it
echo "=== 8 threads ==="
time spark_exec python3 /opt/spark/scripts/starpump.py postgres \
  --mode full --threads 8
```

✅ Pass: 8-thread `real` time < 1-thread `real` time.
❌ Fail: no speed difference → source DB or Spark is the bottleneck, not thread count; this is
expected for very small tables.

---

## Starpump Summary Scorecard

Run this after all T-2.x tests to get a pass/fail summary in one shot:

```bash
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)

echo "=== Watermark state ==="
kubectl exec -n prod "$PG_POD" -- psql -U pipeline -d pipeline -c "
SELECT source_db, table_name,
       sf_extraction_ts,
       rows_copied,
       status
FROM pipeline_watermarks ORDER BY source_db, table_name;" 2>/dev/null

echo ""
echo "=== Run log (last 30) ==="
kubectl exec -n prod "$PG_POD" -- psql -U pipeline -d pipeline -c "
SELECT source_db, table_name, mode, rows_written, status,
       EXTRACT(EPOCH FROM (end_ts - start_ts))::INT AS dur_s
FROM pipeline_run_log ORDER BY start_ts DESC LIMIT 30;" 2>/dev/null

echo ""
echo "=== Iceberg row counts ==="
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('scorecard')).getOrCreate()
tables = [
    ('postgres','cache_testing','customers'),
    ('postgres','cache_testing','products'),
    ('postgres','cache_testing','product_reviews'),
    ('postgres','cache_testing','orders'),
    ('oracle','tpcds','warehouse'),
    ('oracle','tpcds','ship_mode'),
    ('mongodb','cache_testing','customers'),
    ('mongodb','cache_testing','products'),
]
for cat, ns, tbl in tables:
    try:
        n = spark.sql(f'SELECT COUNT(*) AS n FROM \`{cat}\`.\`{ns}\`.\`{tbl}\`').collect()[0]['n']
        print(f'  ✅  {cat}.{ns}.{tbl}: {n:,} rows')
    except Exception as e:
        print(f'  ❌  {cat}.{ns}.{tbl}: ERROR — {e}')
spark.stop()
"
```

| Test | Description | Pass Criterion |
|---|---|---|
| T-2.1 | Catalog bootstrap | All 3 namespaces `ready`, no exceptions |
| T-2.2 | PostgreSQL full load | 4 tables in Iceberg, counts match source |
| T-2.3 | Oracle full load | 10 tables in Iceberg, all `status=success` |
| T-2.4 | MongoDB full load | 2 collections — customers 444 K, products 19.8 M |
| T-2.5 | Watermarks written | Every table has non-null `sf_extraction_ts` + `status=success` |
| T-2.6 | Run log populated | One row per table per run, all `status=success` |
| T-2.7 | Partition spec | `hours=True` + `bucket=True` on all checked tables |
| T-2.8 | snap columns | `snap_id` + `snap_timestamp` on all 5 checked tables |
| T-2.9 | PostgreSQL incremental | 1 new row copied; watermark advanced; full table NOT re-copied |
| T-2.10 | Oracle incremental | Exits 0; watermark advanced; `mode=incremental` in log |
| T-2.11 | MongoDB incremental | Exits 0; watermark advanced |
| T-2.12 | Zero-row incremental | `0 new row(s)` when nothing is new |
| T-2.13 | Custom SQL — WHERE | `gold_customers` table contains only GOLD tier |
| T-2.14 | Custom SQL — JOIN | `orders_enriched_join` has enriched columns + snap cols |
| T-2.15 | DDL ADD COLUMN | `loyalty_points` appears in Iceberg after drift detect |
| T-2.16 | DDL DROP COLUMN | `loyalty_points` removed from Iceberg |
| T-2.17 | Idempotency | Row count stable on full-load re-run |
| T-2.18 | Concurrency | 8-thread run faster than 1-thread run |

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
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)

# ── Remove test rows from PostgreSQL source ───────────────────────────────────
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
DELETE FROM customers WHERE email = 'cdctest@example.com';
DELETE FROM products  WHERE sku IN ('INCR-TEST-001','INCR-T29-001');
DELETE FROM customers WHERE name LIKE 'CDC Test%';
ALTER TABLE public.customers DROP COLUMN IF EXISTS loyalty_points;
ALTER TABLE public.customers DROP COLUMN IF EXISTS new_drift_col;
"

# ── Stop streaming job and schema evolution handler ───────────────────────────
kill $STREAMING_PID $SEH_PID 2>/dev/null || true

# ── Remove test Iceberg tables ────────────────────────────────────────────────
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('cleanup')).getOrCreate()
for tbl in [
    '\`postgres\`.\`cache_testing\`.\`orders_enriched\`',
    '\`postgres\`.\`cache_testing\`.\`orders_enriched_join\`',
    '\`postgres\`.\`cache_testing\`.\`gold_customers\`',
]:
    spark.sql(f'DROP TABLE IF EXISTS {tbl}')
    print(f'Dropped {tbl}')
print('Cleanup done.')
spark.stop()
"
```

---

## Test 9 — Write Modes: UPDATE / DELETE / History Tracking

> **Prerequisite:** Full loads for all three sources (T-2.2, T-2.3, T-2.4) must have completed
> and watermarks must exist in `pipeline_watermarks` before running any test in this section.
>
> **Source PKs used throughout this section:**
>
> | Source | Table | Primary Key | Notes |
> |--------|-------|-------------|-------|
> | PostgreSQL | all tables | `id` (bigserial) | auto-detected by starpump |
> | Oracle | `customers` | `customer_id` | must pass `--pk-cols customer_id` |
> | Oracle | `orders` | `order_id` | must pass `--pk-cols order_id` |
> | Oracle | `order_items` | `item_id` | must pass `--pk-cols item_id` |
> | Oracle | `products` | `product_id` | must pass `--pk-cols product_id` |
> | MongoDB | all collections | `_id` | auto-detected by starpump |

---

### T-9.1 — PK auto-detection logged at run start

Run incremental on PostgreSQL with no `--pk-cols` override and confirm auto-detection:

```bash
spark_exec python3 /opt/spark/scripts/starpump.py postgres \
  --mode incremental \
  --write-mode standard 2>&1 | grep -E "PK cols|write_mode"
```

**Expected log lines (one per table):**
```
[customers]       PK cols: ['id']  (write_mode=standard)
[products]        PK cols: ['id']  (write_mode=standard)
[orders]          PK cols: ['id']  (write_mode=standard)
[product_reviews] PK cols: ['id']  (write_mode=standard)
```

✅ Pass: every table logs `['id']` — no `"No standard PK column found"` warnings.
❌ Fail: `PK cols: []` → override manually with `--pk-cols id`.

---

### T-9.2 — `standard` mode UPDATE: changed row overwrites Iceberg row (PostgreSQL)

**Step 1 — Record the current value of a known customer in Iceberg:**

```bash
# Capture a customer id to use as our test subject
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)
export TEST_ID=$(kubectl exec -n prod "$PG_POD" -- \
  psql -U postgres -d cache_testing -At \
  -c "SELECT id FROM public.customers ORDER BY id LIMIT 1")
echo "Test customer id=$TEST_ID"

spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t92-before')).getOrCreate()
spark.sql(f\"SELECT id, tier, updated_at, snap_timestamp FROM \\\`postgres\\\`.\\\`cache_testing\\\`.\\\`customers\\\` WHERE id={$TEST_ID}\").show()
spark.stop()
"
```

**Step 2 — Update the row in PostgreSQL:**

```bash
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
UPDATE public.customers
SET    tier = 'PLATINUM', updated_at = NOW()
WHERE  id = $TEST_ID
RETURNING id, tier, updated_at;
"
```

**Step 3 — Run incremental in `standard` mode:**

```bash
spark_exec python3 /opt/spark/scripts/starpump.py postgres \
  --mode incremental \
  --write-mode standard \
  --watermark-col updated_at
```

**Expected log:**
```
[customers] Incremental mode: col=updated_at  clause='updated_at >= ...'  write_mode=standard
[customers] MERGE INTO (upsert) — N rows merged
[customers] DONE — N rows written
```

**Step 4 — Verify only ONE row for `id=$TEST_ID` exists in Iceberg with the new tier:**

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t92-after')).getOrCreate()
df = spark.sql(\"SELECT id, tier, snap_timestamp FROM \`postgres\`.\`cache_testing\`.\`customers\` WHERE id=$TEST_ID ORDER BY snap_timestamp\")
df.show()
total = df.count()
print(f'Row count for id={$TEST_ID}: {total}  (expected: 1 — MERGE replaces, not appends)')
spark.stop()
"
```

✅ Pass: exactly **1** row for `id=$TEST_ID`, `tier='PLATINUM'`.
❌ Fail: 2 rows → MERGE did not fire; check that `--write-mode standard` was passed and PK resolved correctly.

---

### T-9.3 — `standard` mode DELETE: hard-deleted source row removed from Iceberg (PostgreSQL)

**Step 1 — Insert a sacrificial row:**

```bash
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
INSERT INTO public.customers (name, email, phone, tier, created_at, updated_at)
VALUES ('Delete Test', 'deletetest@starpump.local', '+1-000-0000', 'BRONZE', NOW(), NOW())
RETURNING id, email;
"
export DEL_ID=$(kubectl exec -n prod "$PG_POD" -- \
  psql -U postgres -d cache_testing -At \
  -c "SELECT id FROM public.customers WHERE email='deletetest@starpump.local' LIMIT 1")
echo "Sacrificial row id=$DEL_ID"
```

**Step 2 — Run incremental (standard) to push the new row into Iceberg:**

```bash
spark_exec python3 /opt/spark/scripts/starpump.py postgres \
  --mode incremental \
  --write-mode standard \
  --watermark-col updated_at

# Confirm row landed in Iceberg
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t93-before')).getOrCreate()
spark.sql(\"SELECT id, name, tier FROM \`postgres\`.\`cache_testing\`.\`customers\` WHERE id=$DEL_ID\").show()
spark.stop()
"
# Expected: 1 row — email=deletetest@starpump.local
```

**Step 3 — Delete the row from PostgreSQL:**

```bash
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
DELETE FROM public.customers WHERE id = $DEL_ID;
SELECT 'deleted rows: ' || ROW_COUNT();
"
```

**Step 4 — Run incremental again (standard) — delete-detection pass fires:**

```bash
spark_exec python3 /opt/spark/scripts/starpump.py postgres \
  --mode incremental \
  --write-mode standard \
  --watermark-col updated_at
```

**Expected log shows delete-detection pass:**
```
[customers] Delete-detection pass (write_mode=standard) — collecting live PKs from source window …
[customers] Live PK count in source window: N
[customers] MERGE INTO (delete pass) — M rows deleted
[customers] Delete-detection pass complete.
```

**Step 5 — Verify the row is gone from Iceberg:**

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t93-after')).getOrCreate()
df = spark.sql(\"SELECT id, name FROM \`postgres\`.\`cache_testing\`.\`customers\` WHERE id=$DEL_ID\")
df.show()
print(f'Row count: {df.count()}  (expected: 0 — hard deleted)')
spark.stop()
"
```

✅ Pass: **0** rows for `id=$DEL_ID` — physically removed from Iceberg.
❌ Fail: row still present → delete-detection pass did not fire; confirm `--watermark-col updated_at` is set so `_wc_for_del` is non-empty.

---

### T-9.4 — `standard` mode UPDATE + DELETE on Oracle (`CACHE_TESTING` schema)

Oracle uses entity-specific PKs (`customer_id`, `order_id`) — must be passed explicitly.

**Step 1 — Update a customer in Oracle:**

```bash
ORA_POD=$(kubectl get pod -n prod -l app=oracle-xe -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n prod "$ORA_POD" -- sqlplus -s \
  cache_testing/CacheTesting#2025@localhost:1521/XEPDB1 <<'EOF'
UPDATE customers SET tier = 'PLATINUM', updated_at = SYSTIMESTAMP WHERE customer_id = 1;
COMMIT;
SELECT customer_id, tier, updated_at FROM customers WHERE customer_id = 1;
EXIT;
EOF
```

**Step 2 — Run incremental on Oracle with explicit PK:**

```bash
spark_exec python3 /opt/spark/scripts/starpump.py oracle \
  --mode incremental \
  --write-mode standard \
  --pk-cols customer_id \
  --watermark-col updated_at \
  INCLUDE_TABLES=customers
```

**Verify MERGE updated the row (not appended):**

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t94-verify')).getOrCreate()
df = spark.sql(\"SELECT customer_id, tier, snap_timestamp FROM \`oracle\`.\`cache_testing\`.\`customers\` WHERE customer_id=1 ORDER BY snap_timestamp\")
df.show()
print(f'Row count for customer_id=1: {df.count()}  (expected: 1 — MERGE replaces)')
spark.stop()
"
```

✅ Pass: exactly 1 row, `tier='PLATINUM'`.

---

### T-9.5 — `soft_delete` mode: deleted row flagged, NOT physically removed (PostgreSQL)

**Step 1 — Insert a test row:**

```bash
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
INSERT INTO public.products (sku, name, category, price, stock_qty, weight_kg, created_at, updated_at)
VALUES ('SOFT-DEL-T95', 'Soft Delete Test Product', 'Testing', 1.00, 1, 0.1, NOW(), NOW())
RETURNING id, sku;
"
export SOFT_ID=$(kubectl exec -n prod "$PG_POD" -- \
  psql -U postgres -d cache_testing -At \
  -c "SELECT id FROM public.products WHERE sku='SOFT-DEL-T95' LIMIT 1")
echo "Soft-delete test row id=$SOFT_ID"
```

**Step 2 — Push row into Iceberg via standard incremental first:**

```bash
spark_exec python3 /opt/spark/scripts/starpump.py postgres \
  --mode incremental \
  --write-mode soft_delete \
  --watermark-col updated_at
```

**Verify row is in Iceberg and `is_deleted` is false/null:**

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t95-before')).getOrCreate()
spark.sql(\"SELECT id, sku, is_deleted, deleted_at FROM \`postgres\`.\`cache_testing\`.\`products\` WHERE id=$SOFT_ID\").show()
spark.stop()
"
# Expected: is_deleted=null or false, deleted_at=null
```

**Step 3 — Delete the row from PostgreSQL source:**

```bash
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
DELETE FROM public.products WHERE id = $SOFT_ID;
"
```

**Step 4 — Run incremental again in `soft_delete` mode:**

```bash
spark_exec python3 /opt/spark/scripts/starpump.py postgres \
  --mode incremental \
  --write-mode soft_delete \
  --watermark-col updated_at
```

**Expected log:**
```
[products] Delete-detection pass (write_mode=soft_delete) — collecting live PKs …
[products] MERGE INTO (delete pass) — 1 row(s) soft-deleted
[products] Delete-detection pass complete.
```

**Step 5 — Verify the row is still in Iceberg but flagged:**

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t95-after')).getOrCreate()
df = spark.sql(\"SELECT id, sku, is_deleted, deleted_at FROM \`postgres\`.\`cache_testing\`.\`products\` WHERE id=$SOFT_ID\")
df.show()
print(f'Row physically present: {df.count()} (expected: 1)')
is_del = df.collect()[0]['is_deleted']
del_at  = df.collect()[0]['deleted_at']
print(f'is_deleted={is_del} (expected: True)')
print(f'deleted_at={del_at} (expected: non-null timestamp)')
spark.stop()
"

# Confirm live-data query with is_deleted filter works:
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t95-live')).getOrCreate()
n = spark.sql(\"SELECT COUNT(*) AS n FROM \`postgres\`.\`cache_testing\`.\`products\` WHERE id=$SOFT_ID AND (is_deleted IS NULL OR is_deleted=false)\").collect()[0]['n']
print(f'Live rows for id={$SOFT_ID}: {n} (expected: 0 — filtered out by is_deleted=true)')
spark.stop()
"
```

✅ Pass: row present in Iceberg with `is_deleted=true` and `deleted_at` set; live filter returns 0 rows.
❌ Fail: row missing entirely → mode was `standard` not `soft_delete`; re-run with `--write-mode soft_delete`.

---

### T-9.6 — `soft_delete` mode on Oracle

Oracle requires explicit `--pk-cols`. Use the `products` table (`product_id` PK).

```bash
ORA_POD=$(kubectl get pod -n prod -l app=oracle-xe -o jsonpath='{.items[0].metadata.name}')

# Step 1 — Insert a sacrificial Oracle product
kubectl exec -n prod "$ORA_POD" -- sqlplus -s \
  cache_testing/CacheTesting#2025@localhost:1521/XEPDB1 <<'EOF'
INSERT INTO products (product_id, product_name, category, subcategory, brand,
                      sku, price, stock_qty, is_active, created_at, updated_at)
VALUES (9999999, 'Oracle SoftDel Test', 'Testing', 'QA', 'TestBrand',
        'ORA-SOFTDEL-T96', 0.01, 1, 'Y', SYSTIMESTAMP, SYSTIMESTAMP);
COMMIT;
SELECT product_id, sku FROM products WHERE sku='ORA-SOFTDEL-T96';
EXIT;
EOF

# Step 2 — Push into Iceberg via soft_delete incremental
spark_exec python3 /opt/spark/scripts/starpump.py oracle \
  --mode incremental \
  --write-mode soft_delete \
  --pk-cols product_id \
  --watermark-col updated_at \
  INCLUDE_TABLES=products

# Step 3 — Delete from Oracle
kubectl exec -n prod "$ORA_POD" -- sqlplus -s \
  cache_testing/CacheTesting#2025@localhost:1521/XEPDB1 <<'EOF'
DELETE FROM products WHERE sku = 'ORA-SOFTDEL-T96';
COMMIT;
EXIT;
EOF

# Step 4 — Run soft_delete incremental again
spark_exec python3 /opt/spark/scripts/starpump.py oracle \
  --mode incremental \
  --write-mode soft_delete \
  --pk-cols product_id \
  --watermark-col updated_at \
  INCLUDE_TABLES=products

# Step 5 — Verify flagged in Iceberg
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t96-verify')).getOrCreate()
df = spark.sql(\"SELECT product_id, sku, is_deleted, deleted_at FROM \`oracle\`.\`cache_testing\`.\`products\` WHERE product_id=9999999\")
df.show()
print(f'is_deleted should be True: {df.collect()[0][\"is_deleted\"]}')
spark.stop()
"
```

✅ Pass: `is_deleted=true`, row physically remains in Iceberg.

---

### T-9.7 — `history` mode: all changes append as new Iceberg rows with `_change_type`

History mode never updates or deletes Iceberg rows — every incremental read appends new rows tagged with `_change_type` and `_change_ts`.

**Step 1 — Run incremental on PostgreSQL `customers` in `history` mode:**

```bash
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)

# Capture before count
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t97-before')).getOrCreate()
n = spark.sql(\"SELECT COUNT(*) AS n FROM \`postgres\`.\`cache_testing\`.\`customers\`\").collect()[0]['n']
print(f'BEFORE history run: {n:,} rows')
spark.stop()
"

# Insert 2 test rows
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
INSERT INTO public.customers (name, email, phone, tier, created_at, updated_at)
VALUES
  ('History Test A', 'hist-a@starpump.local', '+1-001', 'GOLD',   NOW(), NOW()),
  ('History Test B', 'hist-b@starpump.local', '+1-002', 'SILVER', NOW(), NOW());
"
```

**Step 2 — Run incremental in `history` mode:**

```bash
spark_exec python3 /opt/spark/scripts/starpump.py postgres \
  --mode incremental \
  --write-mode history \
  --watermark-col updated_at
```

**Expected log (no MERGE, no delete pass — plain append):**
```
[customers] write_mode=history  → plain append (no MERGE, no delete-detection)
[customers] DONE — 2 rows written
```

**Step 3 — Now UPDATE one of the rows:**

```bash
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
UPDATE public.customers SET tier='PLATINUM', updated_at=NOW()
WHERE email='hist-a@starpump.local';
"
```

**Step 4 — Run incremental in `history` mode again:**

```bash
spark_exec python3 /opt/spark/scripts/starpump.py postgres \
  --mode incremental \
  --write-mode history \
  --watermark-col updated_at
```

**Step 5 — Verify BOTH the old and new versions exist in Iceberg:**

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t97-verify')).getOrCreate()
df = spark.sql(\"\"\"
  SELECT id, name, tier, _change_type, _change_ts, snap_timestamp
  FROM   \`postgres\`.\`cache_testing\`.\`customers\`
  WHERE  email = 'hist-a@starpump.local'
  ORDER  BY snap_timestamp
\"\"\")
df.show()
print(f'Total versions for hist-a: {df.count()} (expected: 2 — original INSERT + UPDATE append)')
spark.stop()
"
```

✅ Pass: **2 rows** for `hist-a@starpump.local` — one with `tier=GOLD` (first insert), one with `tier=PLATINUM` (after update). Both have `_change_type='INSERT'` (batch mode always tags as INSERT).
❌ Fail: only 1 row → mode reverted to `standard`; check `--write-mode history` was passed.

---

### T-9.8 — MongoDB `standard` mode MERGE via `_id` PK

MongoDB's PK is `_id` — auto-detected by starpump. The MERGE join casts `_id` to STRING for comparison.

```bash
MONGO_PASS=$(kubectl get secret mongodb-credentials -n prod \
  -o jsonpath='{.data.mongodb-root-password}' | base64 -d)

# Step 1 — Insert a test document
kubectl exec -n prod mongodb-0 -- mongosh \
  --username root --password "$MONGO_PASS" \
  --authenticationDatabase admin --quiet \
  cache_testing --eval '
db.customers.insertOne({
  email:      "mgo-std-test@starpump.local",
  first_name: "MGO",
  last_name:  "StdTest",
  tier:       "SILVER",
  country_code: "US",
  city:       "TestCity",
  created_at: new Date(),
  updated_at: new Date()
});
print("inserted: " + db.customers.findOne({email:"mgo-std-test@starpump.local"})._id);
'
export MGO_ID=$(kubectl exec -n prod mongodb-0 -- mongosh \
  --username root --password "$MONGO_PASS" \
  --authenticationDatabase admin --quiet cache_testing --eval \
  'db.customers.findOne({email:"mgo-std-test@starpump.local"})._id.toString()')
echo "MongoDB _id=$MGO_ID"

# Step 2 — Push into Iceberg
spark_exec python3 /opt/spark/scripts/starpump.py mongodb \
  --mode incremental \
  --write-mode standard \
  --watermark-col updated_at

# Step 3 — Update the document in MongoDB
kubectl exec -n prod mongodb-0 -- mongosh \
  --username root --password "$MONGO_PASS" \
  --authenticationDatabase admin --quiet \
  cache_testing --eval '
db.customers.updateOne(
  {email: "mgo-std-test@starpump.local"},
  {$set: {tier: "PLATINUM", updated_at: new Date()}}
);
print("updated tier → PLATINUM");
'

# Step 4 — Run incremental again
spark_exec python3 /opt/spark/scripts/starpump.py mongodb \
  --mode incremental \
  --write-mode standard \
  --watermark-col updated_at

# Step 5 — Verify exactly 1 row in Iceberg with PLATINUM tier
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t98-verify')).getOrCreate()
df = spark.sql(\"SELECT _id, email, tier, snap_timestamp FROM \`mongodb\`.\`cache_testing\`.\`customers\` WHERE email='mgo-std-test@starpump.local' ORDER BY snap_timestamp\")
df.show()
print(f'Row count: {df.count()} (expected: 1 — MERGE replaces via _id)')
spark.stop()
"
```

✅ Pass: exactly 1 row, `tier='PLATINUM'`.
❌ Fail: 2 rows → MongoDB `supports_offset_resume=False` means the delete-detection pass is skipped; MERGE upsert should still have fired on the `_id` match. Check `_build_pk_order_clause` is returning `""` for MongoDB (no ORDER BY) and that the MERGE branch was taken.

---

### T-9.9 — Explicit `--pk-cols` override for a composite-key table

The `order_items` table in Oracle uses `item_id` as PK. Test that an explicit override works:

```bash
spark_exec python3 /opt/spark/scripts/starpump.py oracle \
  --mode incremental \
  --write-mode standard \
  --pk-cols item_id \
  --watermark-col updated_at \
  INCLUDE_TABLES=order_items 2>&1 | grep -E "PK cols|write_mode|MERGE"
```

**Expected log:**
```
[order_items] PK cols: ['item_id']  (write_mode=standard)
[order_items] PK-ordered reads: ORDER BY "item_id"
[order_items] MERGE INTO (upsert) ...
```

✅ Pass: `PK cols: ['item_id']` logged — not the auto-detected fallback.

Now test with a hypothetical two-column PK using a comma-separated override:

```bash
# Verify the env-var form also works
PK_COLS=order_id,item_id spark_exec \
  python3 /opt/spark/scripts/starpump.py oracle \
  --mode incremental \
  --write-mode standard \
  INCLUDE_TABLES=order_items 2>&1 | grep "PK cols"
# Expected: [order_items] PK cols: ['order_id', 'item_id']
```

✅ Pass: composite PK logged correctly.

---

### T-9.10 — Watermark boundary `>=`: row at exact boundary timestamp is not skipped

This test confirms the `>=` fix — a row whose `updated_at` equals exactly the last `sf_extraction_ts` watermark must be re-read and merged, not skipped.

**Step 1 — Capture the current watermark for `products`:**

```bash
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)
export WM_TS=$(kubectl exec -n prod "$PG_POD" -- \
  psql -U pipeline -d pipeline -At \
  -c "SELECT sf_extraction_ts FROM pipeline_watermarks
      WHERE source_db='cache_testing' AND source_schema='public' AND table_name='products'")
echo "Current watermark: $WM_TS"
```

**Step 2 — Insert a row with `updated_at` set to EXACTLY the watermark timestamp:**

```bash
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
INSERT INTO public.products (sku, name, category, price, stock_qty, weight_kg,
                             created_at, updated_at)
VALUES ('BOUNDARY-T910', 'Boundary Test Product', 'Testing', 1.00, 1, 0.1,
        TIMESTAMP WITH TIME ZONE '$WM_TS',
        TIMESTAMP WITH TIME ZONE '$WM_TS');
SELECT id, sku, updated_at FROM products WHERE sku='BOUNDARY-T910';
"
```

**Step 3 — Run incremental (the `>=` clause must include this row):**

```bash
spark_exec python3 /opt/spark/scripts/starpump.py postgres \
  --mode incremental \
  --write-mode standard \
  --watermark-col updated_at
```

**Step 4 — Verify the boundary row was captured:**

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t910-verify')).getOrCreate()
df = spark.sql(\"SELECT id, sku, snap_timestamp FROM \`postgres\`.\`cache_testing\`.\`products\` WHERE sku='BOUNDARY-T910'\")
df.show()
print(f'Boundary row count: {df.count()} (expected: 1 — >= includes exact boundary)')
spark.stop()
"
```

✅ Pass: exactly 1 row with `sku='BOUNDARY-T910'` — the `>=` boundary is inclusive.
❌ Fail: 0 rows → watermark is using `>` (strict) — confirm `_incremental_where_clause` was updated to `>=` in `starpump.py`.

---

## Write Mode Scorecard

Run after all T-9.x tests to verify end state:

```bash
spark_exec python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf('t9-scorecard')).getOrCreate()

checks = [
  # (description, SQL, expected condition lambda, expected_str)
  ('T-9.2 UPDATE in PG customers (id=1 → PLATINUM)',
   \"SELECT COUNT(*) AS n FROM \`postgres\`.\`cache_testing\`.\`customers\` WHERE id=1 AND tier='PLATINUM'\",
   lambda n: n == 1, '1'),
  ('T-9.3 DELETE from PG customers gone',
   \"SELECT COUNT(*) AS n FROM \`postgres\`.\`cache_testing\`.\`customers\` WHERE email='deletetest@starpump.local'\",
   lambda n: n == 0, '0'),
  ('T-9.5 soft_delete flag on PG products',
   \"SELECT COUNT(*) AS n FROM \`postgres\`.\`cache_testing\`.\`products\` WHERE sku='SOFT-DEL-T95' AND is_deleted=true\",
   lambda n: n == 1, '1'),
  ('T-9.7 history: 2 versions of hist-a customer',
   \"SELECT COUNT(*) AS n FROM \`postgres\`.\`cache_testing\`.\`customers\` WHERE email='hist-a@starpump.local'\",
   lambda n: n >= 2, '>=2'),
  ('T-9.8 MongoDB MERGE: 1 row for mgo-std-test PLATINUM',
   \"SELECT COUNT(*) AS n FROM \`mongodb\`.\`cache_testing\`.\`customers\` WHERE email='mgo-std-test@starpump.local' AND tier='PLATINUM'\",
   lambda n: n == 1, '1'),
  ('T-9.10 boundary row present',
   \"SELECT COUNT(*) AS n FROM \`postgres\`.\`cache_testing\`.\`products\` WHERE sku='BOUNDARY-T910'\",
   lambda n: n == 1, '1'),
]

for desc, sql, check_fn, exp in checks:
    try:
        n = spark.sql(sql).collect()[0]['n']
        status = '✅' if check_fn(n) else '❌'
        print(f'{status}  {desc}  → {n} (expected {exp})')
    except Exception as e:
        print(f'❌  {desc}  → ERROR: {e}')

spark.stop()
"
```

---

## Cleanup — Write Mode Tests

```bash
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)
ORA_POD=$(kubectl get pod -n prod -l app=oracle-xe -o jsonpath='{.items[0].metadata.name}')
MONGO_PASS=$(kubectl get secret mongodb-credentials -n prod \
  -o jsonpath='{.data.mongodb-root-password}' | base64 -d)

# PostgreSQL test rows
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
DELETE FROM public.customers WHERE email IN (
  'deletetest@starpump.local',
  'hist-a@starpump.local',
  'hist-b@starpump.local'
);
DELETE FROM public.products WHERE sku IN (
  'SOFT-DEL-T95',
  'BOUNDARY-T910'
);
-- Revert the test-id customer back to GOLD
UPDATE public.customers SET tier='GOLD', updated_at=NOW() WHERE id=1;
"

# Oracle test rows
kubectl exec -n prod "$ORA_POD" -- sqlplus -s \
  cache_testing/CacheTesting#2025@localhost:1521/XEPDB1 <<'EOF'
DELETE FROM products WHERE sku = 'ORA-SOFTDEL-T96';
UPDATE customers SET tier='GOLD', updated_at=SYSTIMESTAMP WHERE customer_id=1;
COMMIT;
EXIT;
EOF

# MongoDB test document
kubectl exec -n prod mongodb-0 -- mongosh \
  --username root --password "$MONGO_PASS" \
  --authenticationDatabase admin --quiet \
  cache_testing --eval \
  'db.customers.deleteOne({email:"mgo-std-test@starpump.local"}); print("cleaned")'
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
| MERGE fires but duplicate rows appear | `_pk_cols` resolved to wrong column | Add `--pk-cols <correct_col>` or set `PK_COLS=<col>` env var |
| Delete-detection pass never fires | Watermark clause empty (first full run) | Run one incremental pass first to establish a non-null watermark |
| Oracle MERGE fails on `customer_id` | Missing `--pk-cols` for Oracle | Oracle tables use entity PKs — always pass `--pk-cols customer_id` (etc.) |
| `soft_delete` columns not in Iceberg | First run used `standard` mode | Drop and recreate Iceberg table, then re-run with `--write-mode soft_delete` |
| `history` mode shows `_change_type=null` | Iceberg table predates `history` mode | `mergeSchema=true` adds the column; null means the pre-history rows — expected |
| Boundary row T-9.10 returns 0 rows | `>` still used in `_incremental_where_clause` | Confirm line ~2596 in `starpump.py` reads `return f"{wm_col} >= '{last_ts}'"` |
