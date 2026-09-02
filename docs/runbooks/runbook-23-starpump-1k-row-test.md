# Runbook 23 — starpump: 1 000-Row Smoke Test (PostgreSQL · Oracle · MongoDB → Iceberg)

| Field | Value |
|---|---|
| **Runbook ID** | RB-23 |
| **Service** | k8s-platform / starpump / postgres · oracle · mongodb |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-09-02 |

---

## Purpose

Verify end-to-end connectivity and data integrity for each of the three on-cluster sources
by copying exactly **1 000 rows / documents** into Iceberg. Each run should complete in
under 90 seconds and produces a verifiable row count in the target table.

Run these tests:
- After any starpump or `bao_spark_init.py` code change
- After a Spark pod restart or image upgrade
- Before a scheduled full copy

---

## Common setup

Run once per terminal session — every step below assumes `$MASTER` and `$TOKEN` are set.

```bash
MASTER=$(kubectl get pod -n prod -l component=master \
  -o jsonpath='{.items[0].metadata.name}')
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)
echo "Master: $MASTER   Token: ${TOKEN:0:10}..."
```

---

## Test 1 — PostgreSQL: 1 000 rows of `products`

Copies the first 1 000 rows from `cache_testing.public.products` (500 K rows total)
using `BATCH_SIZE=1000`. `INCLUDE_TABLES` scopes the run to a single table.

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=public \
  INCLUDE_TABLES=products \
  BATCH_SIZE=1000 \
  starpump postgres
```

### ✅ Expected log lines

```
=== starpump postgres | run_id=... user=dave db=cache_testing schema=public catalog=postgres threads=8 ===
[catalog-check] 'postgres' is registered (svc_id=...). Proceeding.
Discovered 1 tables in cache_testing.public: ['products']
Copying 1/1 table(s) with 8 threads, 1000 rows/batch.
[products] START: 0.0 GB | discovering schema …
[products] extraction_ts=...Z (CDC sync point)
[products] batch offset=0 rows=1000 total=1000
[products] DONE — 1000 rows written (total incl. prior runs).
[pg-watermark] upserted cache_testing.public.products sf_extraction_ts=...
Completed in X.Xs — 1/1 copied | 0 skipped | 0 failed | 1000 rows written
```

> The batch stops after exactly 1 000 rows because `BATCH_SIZE=1000` and starpump
> breaks the batch loop when `n < BATCH_SIZE` (source exhausted) **or** after a single
> non-resumable pass. Here the batch returns exactly 1 000 rows so starpump issues one
> more read; the second read returns 0 rows and the loop exits.

### ❌ Common failures

| Error | Fix |
|---|---|
| `ValueError: No Spark external catalog registered for 'postgres'` | Polaris warehouse `pg_lakehouse` not created — run Step 1 of RB-22. |
| `ClassNotFoundException: org.postgresql.Driver` | JAR missing — `kubectl exec … -- ls /opt/spark/jars/postgresql*.jar`. |
| `ERROR: permission denied for table products` | Re-grant: `GRANT SELECT ON ALL TABLES IN SCHEMA public TO rbac;` |
| `RuntimeError: Cannot authenticate to OpenBao` | Pass `TOKEN=$TOKEN` in the exec command. |

---

## Test 2 — Oracle: 1 000 rows of `income_band`

`income_band` is one of the TPCDS tables in `XEPDB1.TPCDS`. Oracle's tables are small
(TPC-DS SF0.001), so we copy all rows and confirm the count.

> Oracle's TPCDS tables have only a handful of rows each. If `income_band` has fewer
> than 1 000 rows, the full table is copied and `rows written` will be less than 1 000.
> That is expected — the copy is still correct.

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=XEPDB1 SCHEMAS=TPCDS \
  INCLUDE_TABLES=income_band \
  BATCH_SIZE=1000 \
  starpump oracle
```

### ✅ Expected log lines

```
=== starpump oracle | run_id=... user=dave db=XEPDB1 schema=TPCDS catalog=oracle threads=8 ===
[catalog-check] 'oracle' is registered (svc_id=...). Proceeding.
Discovered 1 tables in XEPDB1.TPCDS: ['income_band']
Copying 1/1 table(s) with 8 threads, 1000 rows/batch.
[income_band] START: 0.0 GB | discovering schema …
[income_band] extraction_ts=...Z (CDC sync point)
[income_band] batch offset=0 rows=N total=N
[income_band] DONE — N rows written (total incl. prior runs).
Completed in X.Xs — 1/1 copied | 0 skipped | 0 failed | N rows written
```

> `N` is the actual row count of `income_band` in XEPDB1.TPCDS (typically 20 rows at
> TPC-DS SF0.001). A value ≤ 1 000 is expected and correct.

### ❌ Common failures

| Error | Fix |
|---|---|
| `ValueError: No Spark external catalog registered for 'oracle'` | Polaris warehouse `ora_lakehouse` not created — run Step 1 of RB-22. |
| `ClassNotFoundException: oracle.jdbc.OracleDriver` | Hot-patch: `kubectl cp docker/spark-gluten-velox/jars/ojdbc11-23.4.0.24.05.jar prod/$MASTER:/opt/spark/jars/ -c spark-master` |
| `ORA-00942: table or view does not exist` | Schema name must be uppercase: `SCHEMAS=TPCDS` not `SCHEMAS=tpcds`. |
| `ORA-01017: invalid username/password` | Rotate Oracle secret in OpenBao (see RB-22 Step 0). |

---

## Test 3 — MongoDB: 1 000 documents from `products`

Uses `BATCH_SIZE=1000` which starpump passes as a `$limit` stage in the aggregation
pipeline. MongoDB always reads from the start (no reliable OFFSET cursor), so this
always returns the first 1 000 documents from the collection.

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=cache_testing \
  INCLUDE_TABLES=products \
  BATCH_SIZE=1000 \
  starpump mongodb
```

### ✅ Expected log lines

```
=== starpump mongodb | run_id=... user=dave db=cache_testing schema=cache_testing catalog=mongodb threads=8 ===
[catalog-check] 'mongodb' is registered (svc_id=...). Proceeding.
Discovered 1 collections in cache_testing: ['products']
Copying 1/1 table(s) with 8 threads, 1000 rows/batch.
[products] START: 0.0 GB | discovering schema …
[products] MongoDB pipeline: [{"$limit": 1000}]
[products] batch offset=0 rows=1000 total=1000
[products] DONE — 1000 rows written (total incl. prior runs).
Completed in X.Xs — 1/1 copied | 0 skipped | 0 failed | 1000 rows written
```

### ❌ Common failures

| Error | Fix |
|---|---|
| `ValueError: No Spark external catalog registered for 'mongodb'` | Polaris warehouse `mgo_lakehouse` not created — run Step 1 of RB-22. |
| `ClassNotFoundException: com.mongodb.spark.sql.connector.MongoTableProvider` | Hot-patch: `kubectl cp docker/spark-gluten-velox/jars/mongo-spark-connector_2.12-10.4.0-all.jar prod/$MASTER:/opt/spark/jars/ -c spark-master` |
| `com.mongodb.MongoTimeoutException` | MongoDB pod not running: `kubectl get pod -n prod -l app=mongodb`. |
| Schema inferred as empty / `_id` only | Collection is empty or `products` doesn't exist — verify: `kubectl exec -n prod mongodb-0 -- mongosh --eval 'db.getSiblingDB("cache_testing").products.countDocuments()'` |

---

## Verify all three in Iceberg

After all three tests pass, confirm the row counts in Iceberg in one script:

```bash
cat > /tmp/verify_1k.py << 'PYEOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession

bao   = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf(app_name="verify-1k")).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

checks = [
    ("postgres", "public",        "products"),
    ("oracle",   "tpcds",         "income_band"),
    ("mongodb",  "cache_testing", "products"),
]

print("\n=== Row counts ===")
for catalog, ns, table in checks:
    fqn = f"`{catalog}`.`{ns}`.`{table}`"
    try:
        n = spark.sql(f"SELECT COUNT(*) FROM {fqn}").collect()[0][0]
        snap = spark.sql(f"SELECT COUNT(DISTINCT snap_id) FROM {fqn}").collect()[0][0]
        ok = "✅" if n > 0 else "⚠️ "
        print(f"  {ok} {catalog}.{ns}.{table}: {n} rows  ({snap} unique snap_ids)")
    except Exception as e:
        print(f"  ❌ {catalog}.{ns}.{table}: {e}")

print("\n=== snap audit columns (5 sample rows each) ===")
for catalog, ns, table in checks:
    fqn = f"`{catalog}`.`{ns}`.`{table}`"
    print(f"\n-- {catalog}.{ns}.{table} --")
    try:
        spark.sql(f"""
            SELECT snap_id, snap_timestamp
            FROM   {fqn}
            ORDER  BY snap_timestamp DESC
            LIMIT  5
        """).show(truncate=False)
    except Exception as e:
        print(f"  ❌ {e}")

print("\n=== Watermarks ===")
for catalog, ns, _ in checks:
    wm_fqn = f"`{catalog}`.`{ns}`.`_pipeline_watermarks`"
    try:
        spark.sql(f"""
            SELECT source_db, source_schema, table_name,
                   sf_extraction_ts, rows_copied, pipeline_run_ts
            FROM   {wm_fqn}
        """).show(truncate=False)
    except Exception as e:
        print(f"  [{catalog}] watermarks: {e}")

spark.stop()
PYEOF

kubectl cp /tmp/verify_1k.py prod/$MASTER:/tmp/verify_1k.py -c spark-master
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir python3 /tmp/verify_1k.py
```

### ✅ Expected output

```
=== Row counts ===
  ✅ postgres.public.products:        1000 rows  (1000 unique snap_ids)
  ✅ oracle.tpcds.income_band:           N rows  (N unique snap_ids)
  ✅ mongodb.cache_testing.products:  1000 rows  (1000 unique snap_ids)

=== snap audit columns (5 sample rows each) ===

-- postgres.public.products --
+-------------------+----------------------------+
|snap_id            |snap_timestamp              |
+-------------------+----------------------------+
|...                |2026-09-02T...              |
...

=== Watermarks ===
+------------+-------------+----------+---------------------+-----------+-------------------+
|source_db   |source_schema|table_name|sf_extraction_ts     |rows_copied|pipeline_run_ts    |
+------------+-------------+----------+---------------------+-----------+-------------------+
|cache_testing|public      |products  |2026-09-02T...Z      |1000       |2026-09-02T...     |
...
```

---

## Re-run behaviour (idempotency)

- **PostgreSQL / Oracle** (`supports_offset_resume=True`): re-running the same command
  after a successful 1 000-row copy will find 1 000 rows already in Iceberg, start at
  `offset=1000`, read 0 new rows, and exit cleanly. The watermark is not changed.
- **MongoDB** (`supports_offset_resume=False`): re-running appends another 1 000 docs
  on top of the first batch. To reset, drop the Iceberg table first:
  ```bash
  kubectl exec -n prod $MASTER -c spark-master -- \
    env TOKEN=$TOKEN \
    python3 -c "
  import os; os.environ['USER']='dave'
  from bao_spark_init import BaoSparkInit
  from pyspark.sql import SparkSession
  bao=BaoSparkInit()
  spark=SparkSession.builder.config(conf=bao.spark_conf('drop')).getOrCreate()
  spark.sql('DROP TABLE IF EXISTS \`mongodb\`.\`cache_testing\`.\`products\`')
  print('dropped')
  spark.stop()
  "
  ```

---

## Key files

| File | Purpose |
|---|---|
| [`docker/spark-gluten-velox/scripts/starpump.py`](../../docker/spark-gluten-velox/scripts/starpump.py) | `_pg_read_batch`, `_ora_read_batch`, `_mgo_read_batch`, `_mgo_build_pipeline` |
| [`docker/spark-gluten-velox/scripts/bao_spark_init.py`](../../docker/spark-gluten-velox/scripts/bao_spark_init.py) | `postgres_jdbc_options()`, `oracle_jdbc_options()`, `mongodb_options()` |
| [`docs/runbooks/runbook-22-cach-testing-copy.md`](runbook-22-cach-testing-copy.md) | Full copy runbook — Step 1 creates Polaris warehouses required here |
