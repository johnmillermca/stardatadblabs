# Runbook 28 — Starpump Write Modes End-to-End Testing

> **Version:** 1.0
> **Status:** Active
> **Owner:** Platform Engineering
> **Related runbook:** [`runbook-27-cdc-batch-pipeline-e2e-testing.md`](runbook-27-cdc-batch-pipeline-e2e-testing.md)

---

## Purpose

This runbook provides step-by-step validation procedures for all three Starpump write modes
(`standard`, `soft_delete`, `history`) across incremental and full copy scenarios.
Run these tests after any change to `starpump.py` that touches the batch loop, MERGE SQL,
delete-detection pass, or watermark logic.

---

## Prerequisites

All tests run from the `spark-master` pod in the `prod` namespace.

```bash
# Set up environment once for the session
export MASTER=$(kubectl get pods -n prod | grep spark-master | grep Running | awk 'NR==1{print $1}')
export TOKEN=<your-openbao-token>
```

Confirm the pod and token are valid:

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN starpump postgres --mode full \
  INCLUDE_TABLES=products DRY_RUN=1 2>&1 | grep "catalog-check\|DRY_RUN"
# Expected: [catalog-check] 'postgres' is registered … Proceeding.
#           [products] DRY_RUN — skipping data copy.
```

---

## Test Checklist

| # | Area | Test |
|---|------|------|
| 1.1 | Pre-flight | Pipeline DB watermark table accessible |
| 1.2 | Pre-flight | Postgres source tables visible to starpump |
| 2.1 | Full / standard | Full load populates Iceberg with all rows |
| 2.2 | Full / standard | `snap_id` and `snap_timestamp` present and populated |
| 2.3 | Full / standard | Watermark written to pipeline DB after full load |
| 2.4 | Full / standard | Re-run is idempotent — row count unchanged |
| 3.1 | Incremental / standard | Watermark read — WHERE clause matches last run timestamp |
| 3.2 | Incremental / standard | New row inserted in Postgres appears in Iceberg |
| 3.3 | Incremental / standard | Updated row appears in Iceberg with new values |
| 3.4 | Incremental / standard | Watermark advances to new `extraction_ts` after success |
| 3.5 | Incremental / standard | Failed run does NOT advance watermark |
| 3.6 | Incremental / standard | Zero-row window completes without error |
| 4.1 | Filters | `INCLUDE_TABLES` restricts copy to named tables only |
| 4.2 | Filters | `EXCLUDE_TABLES` drops named tables |
| 4.3 | Filters | `MAX_TABLE_SIZE_GB` skips tables over threshold |
| 4.4 | Filters | `QUERY_FILTER` applies row-level predicate |
| 4.5 | Filters | `MAX_ROWS` hard cap stops copy at N new rows |
| 5.1 | Custom SQL | Single-table SELECT lands correct rows in target |
| 5.2 | Custom SQL | Multi-table JOIN enrichment writes to new Iceberg table |
| 6.1 | DDL Drift | New source column detected and ALTER TABLE applied |
| 7.1 | Pipeline DB | `pipeline_run_log` records each run (status, rows, timing) |

---

## Section 1 — Pre-flight

### 1.1 Pipeline DB watermark table accessible

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN python3 - << 'EOF'
import os, sys
sys.path.insert(0, "/opt/spark/work-dir")
from bao_spark_init import BaoSparkInit
bao = BaoSparkInit()
import psycopg2
pg = bao.pipeline_db_creds()
with psycopg2.connect(**pg, connect_timeout=10) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM pipeline_watermarks")
        print("pipeline_watermarks rows:", cur.fetchone()[0])
        cur.execute("SELECT count(*) FROM pipeline_run_log")
        print("pipeline_run_log rows:", cur.fetchone()[0])
EOF
```

**Expected:**
```
pipeline_watermarks rows: <N>
pipeline_run_log rows: <N>
```
Both numbers ≥ 0 without error.

---

### 1.2 Postgres source tables visible

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  starpump postgres --mode full DRY_RUN=1 2>&1 | grep "size-report\|Discovered"
```

**Expected:** 6 tables discovered in `cache_testing.public`:
```
Discovered 6 tables in cache_testing.public: ['customers', 'inventory_events', 'order_items', 'orders', 'product_reviews', 'products']
[size-report] customers        →    0.4 GB  (COPY)
…
```

---

## Section 2 — Full Load (standard write mode)

### 2.1 Full load populates Iceberg

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  starpump postgres --mode full
```

**Expected log (key lines):**
```
[customers] START: 0.4 GB | discovering schema …
[customers] extraction_ts=<ISO-8601Z> (CDC sync point)
[customers] batch offset=0 rows=100000 total=100000
…
[customers] DONE — <N> rows written (total incl. prior runs).
Completed in <T>s — 1/6 copied | … | 0 failed | <N> rows written [mode=full]
✓ customers   rows=<N>   size=0.4 GB   status=success
```

---

### 2.2 `snap_id` and `snap_timestamp` present

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir \
  python3 - << 'EOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("rb28-t22")).getOrCreate()
df = spark.sql("SELECT snap_id, snap_timestamp FROM `postgres`.`public`.`customers` LIMIT 5")
df.show(truncate=False)
print("Schema:", [f.name for f in df.schema.fields])
spark.stop()
EOF
```

**Expected:**
- `snap_id` column: non-null BIGINT values
- `snap_timestamp` column: non-null TIMESTAMP values (wall-clock of write batch)

---

### 2.3 Watermark written to pipeline DB

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN python3 - << 'EOF'
import sys
sys.path.insert(0, "/opt/spark/work-dir")
from bao_spark_init import BaoSparkInit
import psycopg2
bao = BaoSparkInit()
pg = bao.pipeline_db_creds()
with psycopg2.connect(**pg) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name, sf_extraction_ts, rows_copied, pipeline_run_ts
            FROM pipeline_watermarks
            WHERE source_db='cache_testing' AND source_schema='public'
            ORDER BY pipeline_run_ts DESC LIMIT 5
        """)
        for row in cur.fetchall():
            print(row)
EOF
```

**Expected:** Row for `customers` with a non-null `sf_extraction_ts` and `rows_copied > 0`.

---

### 2.4 Re-run is idempotent

Note the row count from test 2.1, then re-run:

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  starpump postgres --mode full 2>&1 | grep "rows written\|✓\|✗"
```

**Expected:** Same `rows=<N>` as first run. No new rows added (full mode with offset-resume detects existing rows and starts from that offset, draining to empty since all rows already present).

---

## Section 3 — Incremental Load (standard write mode)

### 3.1 Watermark read — WHERE clause matches last timestamp

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  starpump postgres --mode incremental --watermark-col updated_at 2>&1 \
  | grep "Incremental mode\|QUERY_FILTER\|extraction_ts"
```

**Expected:**
```
[customers] Incremental mode: col=updated_at last_ts=<prev_extraction_ts> clause='updated_at > '<prev_extraction_ts>'' …
[customers] QUERY_FILTER active — WHERE (updated_at > '<prev_extraction_ts>')
[customers] extraction_ts=<new_ts> (CDC sync point)
```

`last_ts` must match the `sf_extraction_ts` written in test 2.1/2.3.

---

### 3.2 New inserted row appears in Iceberg

**Step 1 — Insert a new row in Postgres:**

```bash
kubectl exec -n prod $(kubectl get pods -n prod | grep postgres | grep Running | awk 'NR==1{print $1}') \
  -- psql -U postgres -d cache_testing -c "
    INSERT INTO public.customers (name, email, phone, address, tier, created_at, updated_at)
    VALUES ('Test User RB28', 'rb28@test.local', '555-0128', '28 Test St', 'gold',
            NOW(), NOW())
    RETURNING id, name, updated_at;
  "
```

Record the returned `id`.

**Step 2 — Run incremental:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  starpump postgres --mode incremental --watermark-col updated_at 2>&1 \
  | grep "rows written\|batch offset\|DONE\|✓\|✗"
```

**Expected:**
```
[customers] batch offset=0 rows=1 total=1
[customers] DONE — 1 rows written (total incl. prior runs).
✓ customers   rows=1   status=success
```

**Step 3 — Verify in Iceberg:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir \
  python3 - << 'EOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("rb28-t32")).getOrCreate()
spark.sql("""
    SELECT id, name, email, tier, updated_at, snap_timestamp
    FROM `postgres`.`public`.`customers`
    WHERE email = 'rb28@test.local'
""").show(truncate=False)
spark.stop()
EOF
```

**Expected:** Row with `name='Test User RB28'` returned.

---

### 3.3 Updated row appears in Iceberg with new values

**Step 1 — Update the row:**

```bash
kubectl exec -n prod $(kubectl get pods -n prod | grep postgres | grep Running | awk 'NR==1{print $1}') \
  -- psql -U postgres -d cache_testing -c "
    UPDATE public.customers
    SET tier='platinum', updated_at=NOW()
    WHERE email='rb28@test.local'
    RETURNING id, name, tier, updated_at;
  "
```

**Step 2 — Run incremental:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  starpump postgres --mode incremental --watermark-col updated_at 2>&1 \
  | grep "DONE\|✓\|✗"
```

**Expected:** `rows=1   status=success`

**Step 3 — Verify updated value in Iceberg:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir \
  python3 - << 'EOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("rb28-t33")).getOrCreate()
spark.sql("""
    SELECT id, name, tier, updated_at
    FROM `postgres`.`public`.`customers`
    WHERE email = 'rb28@test.local'
""").show(truncate=False)
spark.stop()
EOF
```

**Expected:** `tier=platinum`

---

### 3.4 Watermark advances after successful run

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN python3 - << 'EOF'
import sys
sys.path.insert(0, "/opt/spark/work-dir")
from bao_spark_init import BaoSparkInit
import psycopg2
bao = BaoSparkInit()
pg = bao.pipeline_db_creds()
with psycopg2.connect(**pg) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT sf_extraction_ts, rows_copied, pipeline_run_ts
            FROM pipeline_watermarks
            WHERE source_db='cache_testing' AND source_schema='public'
              AND table_name='customers'
        """)
        print(cur.fetchone())
EOF
```

**Expected:** `sf_extraction_ts` is newer than the value recorded after test 2.1. It must match the `extraction_ts=<ts>` logged by the most recent incremental run.

---

### 3.5 Failed run does NOT advance watermark

**Step 1 — Record the current watermark:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN python3 -c "
import sys; sys.path.insert(0,'/opt/spark/work-dir')
from bao_spark_init import BaoSparkInit; import psycopg2
bao = BaoSparkInit(); pg = bao.pipeline_db_creds()
with psycopg2.connect(**pg) as c:
    with c.cursor() as cur:
        cur.execute(\"SELECT sf_extraction_ts FROM pipeline_watermarks WHERE table_name='customers'\")
        print('BEFORE:', cur.fetchone())
"
```

**Step 2 — Force a parse failure with a broken QUERY_FILTER that will error:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  BATCH_SIZE=1 MAX_ROWS=0 \
  starpump postgres --mode incremental --watermark-col updated_at \
  QUERY_FILTER="customers.nonexistent_column_xyz=1" 2>&1 \
  | grep "FAILED\|status=error\|✗"
```

**Expected:** Run fails with an error (column not found).

**Step 3 — Confirm watermark unchanged:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN python3 -c "
import sys; sys.path.insert(0,'/opt/spark/work-dir')
from bao_spark_init import BaoSparkInit; import psycopg2
bao = BaoSparkInit(); pg = bao.pipeline_db_creds()
with psycopg2.connect(**pg) as c:
    with c.cursor() as cur:
        cur.execute(\"SELECT sf_extraction_ts FROM pipeline_watermarks WHERE table_name='customers'\")
        print('AFTER:', cur.fetchone())
"
```

**Expected:** `AFTER` value is identical to `BEFORE` value — the watermark was not advanced by the failed run.

---

### 3.6 Zero-row window completes without error

Insert nothing. Run incremental immediately after test 3.3 (window is empty):

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  starpump postgres --mode incremental --watermark-col updated_at 2>&1 \
  | grep "DONE\|✓\|✗\|status="
```

**Expected:**
```
[customers] DONE — 0 rows written (total incl. prior runs).
✓ customers   rows=0   status=success
```
Exit code 0.

---

## Section 4 — Filters

### 4.1 INCLUDE_TABLES

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers,products \
  DRY_RUN=1 \
  starpump postgres --mode full 2>&1 | grep "INCLUDE_TABLES filter\|kept\|size-report"
```

**Expected:**
```
INCLUDE_TABLES filter: 6 → 2 tables (kept: ['customers', 'products'])
[size-report] customers   → … (COPY)
[size-report] products    → … (COPY)
[size-report] orders      → … (SKIP — not in INCLUDE_TABLES)
…
```

---

### 4.2 EXCLUDE_TABLES

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  EXCLUDE_TABLES=orders,order_items \
  DRY_RUN=1 \
  starpump postgres --mode full 2>&1 | grep "size-report"
```

**Expected:** `orders` and `order_items` both show `(SKIP — EXCLUDE_TABLES)`.

---

### 4.3 MAX_TABLE_SIZE_GB

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  MAX_TABLE_SIZE_GB=0.5 \
  DRY_RUN=1 \
  starpump postgres --mode full 2>&1 | grep "size-report"
```

**Expected:** Only `customers` (0.4 GB) and `products` (0.1 GB) show `(COPY)`. All tables ≥ 0.5 GB show `(SKIP — <X> GB exceeds 0.5 GB limit)`.

---

### 4.4 QUERY_FILTER row-level predicate

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  QUERY_FILTER="customers.tier='gold'" \
  starpump postgres --mode full 2>&1 | grep "QUERY_FILTER\|DONE\|rows written"
```

**Expected:**
```
[customers] QUERY_FILTER active — WHERE (tier = 'gold')
[customers] DONE — <gold_count> rows written …
```

Verify count by comparing against Postgres directly:

```bash
kubectl exec -n prod $(kubectl get pods -n prod | grep postgres | grep Running | awk 'NR==1{print $1}') \
  -- psql -U postgres -d cache_testing -c \
  "SELECT count(*) FROM public.customers WHERE tier='gold';"
```

Row counts must match.

---

### 4.5 MAX_ROWS hard cap

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  MAX_ROWS=250 \
  starpump postgres --mode full 2>&1 | grep "DONE\|✓"
```

**Expected:**
```
[customers] DONE — 250 rows written (total incl. prior runs).
✓ customers   rows=250   status=success
```

Exactly 250 new rows regardless of table size.

---

## Section 5 — Custom SQL

### 5.1 Single-table SELECT

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  starpump postgres --mode custom_sql \
  --custom-sql "SELECT id, name, tier FROM public.customers WHERE tier='platinum' LIMIT 100" \
  --target-table customers_platinum 2>&1 \
  | grep "Done\|rows written\|FAILED"
```

**Expected:**
```
[custom-sql] Done — <N> rows written to `postgres`.`public`.`customers_platinum`.
```

Verify:

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir python3 - << 'EOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("rb28-t51")).getOrCreate()
spark.sql("SELECT count(*), min(tier), max(tier) FROM `postgres`.`public`.`customers_platinum`").show()
spark.stop()
EOF
```

**Expected:** `count > 0`, `min(tier) = max(tier) = platinum`.

---

### 5.2 Multi-table JOIN enrichment

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  starpump postgres --mode custom_sql \
  --custom-sql "
    SELECT o.id AS order_id, c.name AS customer_name, c.tier,
           o.created_at AS order_date
    FROM public.orders o
    JOIN public.customers c ON o.customer_id = c.id
    WHERE c.tier = 'platinum'
    LIMIT 200
  " \
  --target-table orders_platinum_customers 2>&1 \
  | grep "Done\|rows written\|FAILED"
```

**Expected:** `Done — <N> rows written to \`postgres\`.\`public\`.\`orders_platinum_customers\``.

Verify schema:

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir python3 - << 'EOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("rb28-t52")).getOrCreate()
df = spark.sql("SELECT * FROM `postgres`.`public`.`orders_platinum_customers` LIMIT 3")
df.show(truncate=False)
print("Columns:", df.columns)
spark.stop()
EOF
```

**Expected columns:** `order_id`, `customer_name`, `tier`, `order_date`, `snap_id`, `snap_timestamp`.

---

## Section 6 — DDL Drift Detection

### 6.1 New source column detected and ALTER TABLE applied

**Step 1 — Add a column in Postgres:**

```bash
kubectl exec -n prod $(kubectl get pods -n prod | grep postgres | grep Running | awk 'NR==1{print $1}') \
  -- psql -U postgres -d cache_testing -c \
  "ALTER TABLE public.customers ADD COLUMN loyalty_points INTEGER DEFAULT 0;"
```

**Step 2 — Run full copy with DDL drift enabled (default):**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  DDL_DRIFT_DETECT=1 \
  starpump postgres --mode full 2>&1 \
  | grep -i "drift\|ALTER TABLE\|ADD COLUMN\|loyalty"
```

**Expected:**
```
[customers] DDL drift: 1 change(s) detected — add: loyalty_points
[customers] ALTER TABLE `postgres`.`public`.`customers` ADD COLUMN loyalty_points INTEGER
```

**Step 3 — Verify the column exists in Iceberg:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir python3 - << 'EOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("rb28-t61")).getOrCreate()
cols = [f.name for f in spark.table("`postgres`.`public`.`customers`").schema.fields]
print("loyalty_points present:", "loyalty_points" in cols)
spark.stop()
EOF
```

**Expected:** `loyalty_points present: True`

**Step 4 — Clean up:**

```bash
kubectl exec -n prod $(kubectl get pods -n prod | grep postgres | grep Running | awk 'NR==1{print $1}') \
  -- psql -U postgres -d cache_testing -c \
  "ALTER TABLE public.customers DROP COLUMN loyalty_points;"
```

---

## Section 7 — Pipeline Run Log

### 7.1 `pipeline_run_log` records each run

After completing at least one successful run above, verify the log:

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN python3 - << 'EOF'
import sys
sys.path.insert(0, "/opt/spark/work-dir")
from bao_spark_init import BaoSparkInit
import psycopg2
bao = BaoSparkInit()
pg = bao.pipeline_db_creds()
with psycopg2.connect(**pg) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT run_id, source_db, started_at, finished_at,
                   tables_ok, tables_failed, total_rows, status
            FROM pipeline_run_log
            WHERE source_db = 'cache_testing'
            ORDER BY started_at DESC
            LIMIT 5
        """)
        for row in cur.fetchall():
            print(row)
EOF
```

**Expected:** Rows with:
- `status` = `success` or `partial`
- `tables_ok` ≥ 1
- `tables_failed` = 0 for clean runs
- `total_rows` > 0
- `finished_at` is not NULL

---

## Cleanup

Remove test rows and custom SQL target tables after completing all tests:

```bash
# Remove test customer row from Postgres
kubectl exec -n prod $(kubectl get pods -n prod | grep postgres | grep Running | awk 'NR==1{print $1}') \
  -- psql -U postgres -d cache_testing -c \
  "DELETE FROM public.customers WHERE email='rb28@test.local';"

# Drop custom SQL target Iceberg tables
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir python3 - << 'EOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("rb28-cleanup")).getOrCreate()
for t in ("customers_platinum", "orders_platinum_customers"):
    try:
        spark.sql(f"DROP TABLE IF EXISTS `postgres`.`public`.`{t}`")
        print(f"Dropped: {t}")
    except Exception as e:
        print(f"Could not drop {t}: {e}")
spark.stop()
EOF
```

---

## Quick Reference

### Key starpump environment variables for this runbook

| Variable | Example | Purpose |
|----------|---------|---------|
| `USER` | `dave` | Operator identity (required) |
| `TOKEN` | `$TOKEN` | OpenBao authentication token |
| `INCLUDE_TABLES` | `customers,orders` | Only copy these tables |
| `EXCLUDE_TABLES` | `order_items` | Never copy these tables |
| `MAX_TABLE_SIZE_GB` | `1.0` | Skip tables larger than 1 GB |
| `QUERY_FILTER` | `customers.tier='gold'` | Row-level WHERE predicate |
| `MAX_ROWS` | `1000` | Hard cap on new rows per table |
| `BATCH_SIZE` | `50000` | Rows per Iceberg snapshot |
| `DRY_RUN` | `1` | Create DDL only, skip data copy |
| `DDL_DRIFT_DETECT` | `1` | Detect and apply schema changes |

### Key starpump CLI flags for this runbook

| Flag | Values | Purpose |
|------|--------|---------|
| `--mode` | `full`, `incremental`, `custom_sql` | Copy mode |
| `--watermark-col` | `updated_at`, `created_at` | Incremental timestamp column |
| `--custom-sql` | `"SELECT …"` | SQL for custom_sql mode |
| `--target-table` | `my_target` | Iceberg table for custom_sql output |
| `--threads` | `1`–`16` | Override parallel thread count |

### Watermark flow (incremental)

```
Run start:
  last_ts  ← READ  pipeline_watermarks.sf_extraction_ts
  new_ts   ← capture_ts() from source server          (in memory only)
  WHERE clause: updated_at > '<last_ts>'

  [batch loop + write to Iceberg]

  ✓ SUCCESS:
    pipeline_watermarks.sf_extraction_ts ← new_ts    (written now)
    Iceberg _pipeline_watermarks         ← new_ts    (written now)

  ✗ FAILURE:
    pipeline_watermarks unchanged         (new_ts discarded)
    next run retries same window          (no rows skipped)
```
