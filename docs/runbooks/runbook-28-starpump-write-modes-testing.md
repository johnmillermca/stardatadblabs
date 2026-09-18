# Runbook 28 — Starpump Write Modes End-to-End Testing

> **Version:** 1.4
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

# Retrieve the OpenBao root token from the Kubernetes secret (prod namespace):
export TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)
```

> **Token note:** The root token is stored in the `openbao-unseal-keys` Secret in the
> `prod` namespace, written there when OpenBao was first initialised.
> Current value: `s.ykxM4SANXt0c1jJcHhE0ZHPK`
> Always use the `kubectl` command above to retrieve it — the value rotates if OpenBao
> is re-initialised.

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
| **3.3a** | **Incremental / MERGE upsert** | **Updated row overwrites existing Iceberg row — no duplicate (PK_TABLE_MAP)** |
| 3.4 | Incremental / standard | Watermark advances to new `extraction_ts` after success |
| 3.5 | Incremental / standard | Failed run does NOT advance watermark |
| 3.6 | Incremental / standard | Zero-row window completes without error |
| 3.7 | Incremental / custom watermark | Custom `--watermark-col` used instead of `updated_at` |
| 4.1 | Filters | `INCLUDE_TABLES` restricts copy to named tables only |
| 4.2 | Filters | `EXCLUDE_TABLES` drops named tables |
| 4.3 | Filters | `MAX_TABLE_SIZE_GB` skips tables over threshold |
| 4.4 | Filters | `QUERY_FILTER` applies row-level predicate |
| 4.5 | Filters | `MAX_ROWS` hard cap stops copy at N new rows |
| 5.1 | Custom SQL | Single-table SELECT lands correct rows in target |
| 5.2 | Custom SQL | Multi-table JOIN enrichment writes to new Iceberg table |
| 6.1 | DDL Drift | New source column detected and ALTER TABLE applied |
| 7.1 | Pipeline DB | `pipeline_run_log` records each run (status, rows, timing) |
| 8.1 | Oracle — Full | Full load from Oracle TPCDS schema into Iceberg |
| 8.2 | Oracle — Incremental | New Oracle row appears in Iceberg with custom watermark col |
| 8.3 | Oracle — MERGE upsert | Updated Oracle row overwrites existing Iceberg row |
| 9.1 | MongoDB — Full | Full load from MongoDB cache_testing database into Iceberg |
| 9.2 | MongoDB — Incremental | New MongoDB document appears in Iceberg (append-only) |
| **10.1** | **Full Reload** | **`full_reload` truncates Iceberg table then reloads all rows from source** |
| **10.2** | **Full Reload** | **`FULL_RELOAD=1` env alias produces identical result** |
| **10.3** | **Full Reload** | **Watermark resets to new extraction_ts after reload** |

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

**Step 2 — Run incremental (plain append — no PK_TABLE_MAP):**

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

**Expected:** `tier=platinum`. Note: without `PK_TABLE_MAP` this run used plain append.
The old `gold` row still exists alongside the new `platinum` row. See test **3.3a** below
to validate MERGE upsert which fixes that.

---

### 3.3a Updated row overwrites existing Iceberg row — MERGE upsert via `PK_TABLE_MAP`

This test validates the MERGE upsert path introduced in starpump v1.1.
When `PK_TABLE_MAP` is set, incremental mode issues `MERGE INTO` keyed on the
primary key so updated source rows overwrite their existing Iceberg counterpart
instead of being appended alongside the stale copy.

**Step 1 — Drop the Iceberg customers table and re-do the full load as a clean baseline:**

```bash
# Drop existing Iceberg table so we start clean
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir python3 - << 'EOF'
import os; os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("rb28-t33a-reset")).getOrCreate()
spark.sql("DROP TABLE IF EXISTS `postgres`.`public`.`customers`")
print("Dropped customers Iceberg table.")
spark.stop()
EOF

# Full load into clean table
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  starpump postgres --mode full 2>&1 | grep "DONE\|✓\|✗"
```

**Expected:** Full load completes, `rows=<N>  status=success`.

**Step 2 — Record the row count and verify the test customer's current tier:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir python3 - << 'EOF'
import os; os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("rb28-t33a-pre")).getOrCreate()
total = spark.table("`postgres`.`public`.`customers`").count()
print("Total rows in Iceberg:", total)
spark.sql("""
    SELECT id, name, tier, updated_at
    FROM `postgres`.`public`.`customers`
    WHERE email = 'rb28@test.local'
""").show(truncate=False)
spark.stop()
EOF
```

Record **Total rows** — call it `N_before`. Note the current `tier` value.

**Step 3 — Update the row in Postgres:**

```bash
kubectl exec -n prod $(kubectl get pods -n prod | grep postgres | grep Running | awk 'NR==1{print $1}') \
  -- psql -U postgres -d cache_testing -c "
    UPDATE public.customers
    SET tier='diamond', updated_at=NOW()
    WHERE email='rb28@test.local'
    RETURNING id, name, tier, updated_at;
  "
```

Record the returned `updated_at` timestamp.

**Step 4 — Run incremental with `PK_TABLE_MAP` to enable MERGE upsert:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  PK_TABLE_MAP="customers:id" \
  starpump postgres --mode incremental --watermark-col updated_at 2>&1 \
  | grep "MERGE upsert\|DONE\|✓\|✗"
```

**Expected log lines:**
```
[customers] Incremental MERGE upsert enabled — pk_col=id
[customers] MERGE upsert on pk=id completed.
[customers] DONE — 1 rows written (total incl. prior runs).
✓ customers   rows=1   status=success
```

**Step 5 — Verify: exactly one row for the test customer, with updated values:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir python3 - << 'EOF'
import os; os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("rb28-t33a-post")).getOrCreate()

total_after = spark.table("`postgres`.`public`.`customers`").count()
print("Total rows in Iceberg after MERGE:", total_after)

spark.sql("""
    SELECT id, name, tier, updated_at, snap_timestamp
    FROM `postgres`.`public`.`customers`
    WHERE email = 'rb28@test.local'
""").show(truncate=False)

# Count how many rows exist for this email — must be exactly 1 after MERGE upsert
dupe_count = spark.sql("""
    SELECT count(*) AS cnt
    FROM `postgres`.`public`.`customers`
    WHERE email = 'rb28@test.local'
""").collect()[0]["cnt"]
print(f"Row count for rb28@test.local: {dupe_count}  (expected: 1)")
spark.stop()
EOF
```

**Expected:**
- `Total rows in Iceberg after MERGE` = `N_before` (unchanged — MERGE updated in place, not appended)
- `tier = diamond` (new value from source)
- `Row count for rb28@test.local: 1` — no duplicate; old stale row was overwritten

**Step 6 — Verify with the wildcard shorthand `PK_TABLE_MAP="*:id"`:**

```bash
# Update again to a new tier
kubectl exec -n prod $(kubectl get pods -n prod | grep postgres | grep Running | awk 'NR==1{print $1}') \
  -- psql -U postgres -d cache_testing -c \
  "UPDATE public.customers SET tier='platinum', updated_at=NOW() WHERE email='rb28@test.local';"

# Run incremental using wildcard PK
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  PK_TABLE_MAP="*:id" \
  starpump postgres --mode incremental --watermark-col updated_at 2>&1 \
  | grep "MERGE upsert\|DONE\|✓\|✗"
```

**Expected:** Same `MERGE upsert enabled` log, same single-row result with `tier=platinum`.

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

### 3.7 Custom `--watermark-col` — table with a non-standard timestamp column

starpump's `--watermark-col` (or env var `WATERMARK_COL`) lets you name **any timestamp column** in the source table as the watermark, not just `updated_at` or `created_at`. This is essential for sources like Oracle where column names differ (e.g. `updated_at` exists but Oracle tables seeded by `ora_load_*.sql` also have `created_at`), or for any table where the change-tracking column has a custom name such as `last_modified`, `modified_date`, `change_ts`, etc.

**How watermark column resolution works:**

Priority order inside [`_resolve_watermark_col()`](docker/spark-gluten-velox/scripts/starpump.py):
1. `--watermark-col <col>` CLI flag or `WATERMARK_COL=<col>` env var — **your explicit override always wins**
2. `updated_at` — used automatically if present in the table schema
3. `created_at` — fallback for append-only tables
4. `None` — no suitable column found; table falls back to full copy without a time filter

**Step 1 — Confirm `product_reviews` has only `created_at` (no `updated_at`):**

```bash
kubectl exec -n prod $(kubectl get pods -n prod | grep postgres | grep Running | awk 'NR==1{print $1}') \
  -- psql -U postgres -d cache_testing -c \
  "\d public.product_reviews"
```

**Expected:** Columns include `created_at` but NOT `updated_at` — making it a good candidate to test the `created_at` fallback and then an explicit override.

**Step 2 — Run incremental without override (auto-detects `created_at`):**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=product_reviews \
  starpump postgres --mode incremental 2>&1 \
  | grep "Incremental mode\|watermark-col\|No watermark\|DONE\|✓"
```

**Expected:**
```
[product_reviews] Incremental mode: col=created_at last_ts=<ts> clause='created_at > '<ts>''
[product_reviews] DONE — 0 rows written (total incl. prior runs).
```
`col=created_at` confirms the auto-fallback is working.

**Step 3 — Insert a new review and run incremental with explicit `--watermark-col`:**

```bash
# Insert a new review
kubectl exec -n prod $(kubectl get pods -n prod | grep postgres | grep Running | awk 'NR==1{print $1}') \
  -- psql -U postgres -d cache_testing -c "
    INSERT INTO public.product_reviews (product_id, customer_id, rating, review_text, created_at)
    VALUES (1, 1, 5, 'RB28 watermark-col test review', NOW())
    RETURNING id, rating, created_at;
  "

# Run incremental with explicit watermark column override
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=product_reviews \
  starpump postgres --mode incremental --watermark-col created_at 2>&1 \
  | grep "Incremental mode\|DONE\|✓\|✗"
```

**Expected:**
```
[product_reviews] Incremental mode: col=created_at last_ts=<ts> clause='created_at > '<ts>''
[product_reviews] DONE — 1 rows written (total incl. prior runs).
✓ product_reviews   rows=1   status=success
```

**Step 4 — Verify in Iceberg:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir python3 - << 'EOF'
import os; os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("rb28-t37")).getOrCreate()
spark.sql("""
    SELECT id, product_id, rating, review_text, created_at
    FROM `postgres`.`public`.`product_reviews`
    WHERE review_text = 'RB28 watermark-col test review'
""").show(truncate=False)
spark.stop()
EOF
```

**Expected:** The new review row is returned.

**Step 5 — Test with a completely custom column name using `WATERMARK_COL` env var:**

```bash
# This tests that WATERMARK_COL env var is honoured identically to --watermark-col
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  WATERMARK_COL=updated_at \
  starpump postgres --mode incremental 2>&1 \
  | grep "Incremental mode\|DONE\|✓\|✗"
```

**Expected:**
```
[customers] Incremental mode: col=updated_at last_ts=<ts> clause='updated_at > '<ts>''
```
Identical behaviour to passing `--watermark-col updated_at` on the CLI.

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

## Section 10 — Full Reload Mode

`full_reload` is a destructive-then-reload mode: it **truncates** all rows from the
Iceberg table (keeping DDL, schema, partitioning, and S3 location intact) and then
copies the full source table from scratch. Use it when you want a clean current-image
copy rather than accumulated append snapshots.

Two equivalent invocations:
```
--mode full_reload          # CLI flag
FULL_RELOAD=1               # env var alias
```

---

### 10.1 `full_reload` truncates Iceberg table then reloads all rows

**Step 1 — Confirm current row count in Iceberg:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir python3 - << 'EOF'
import os; os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("rb28-t101-pre")).getOrCreate()
n = spark.table("`postgres`.`public`.`customers`").count()
print("Rows in Iceberg BEFORE full_reload:", n)
spark.stop()
EOF
```

Record the count — call it `N_before`.

**Step 2 — Run full_reload:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  starpump postgres --mode full_reload 2>&1 \
  | grep "full_reload\|DONE\|✓\|✗"
```

**Expected log lines:**
```
=== MODE=full_reload: every Iceberg table will be TRUNCATED then reloaded in full from source. ===
[customers] full_reload: truncated <N_before> rows from Iceberg table.
[customers] DONE — <N_source> rows written (total incl. prior runs).
✓ customers   rows=<N_source>   status=success
```

**Step 3 — Verify row count matches source exactly:**

```bash
# Count in Iceberg after reload
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir python3 - << 'EOF'
import os; os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("rb28-t101-post")).getOrCreate()
n = spark.table("`postgres`.`public`.`customers`").count()
print("Rows in Iceberg AFTER full_reload:", n)
spark.stop()
EOF

# Count in Postgres source
kubectl exec -n prod $(kubectl get pods -n prod | grep postgres | grep Running | awk 'NR==1{print $1}') \
  -- psql -U postgres -d cache_testing -c \
  "SELECT count(*) FROM public.customers;"
```

**Expected:** Both counts are identical. The Iceberg table has exactly as many rows as
the Postgres source — no stale rows, no duplicates.

---

### 10.2 `FULL_RELOAD=1` env alias produces identical result

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  FULL_RELOAD=1 \
  starpump postgres 2>&1 \
  | grep "FULL_RELOAD\|full_reload\|DONE\|✓\|✗"
```

**Expected:**
```
FULL_RELOAD=1 detected — overriding MODE to full_reload.
=== MODE=full_reload: every Iceberg table will be TRUNCATED then reloaded in full from source. ===
[customers] full_reload: truncated <N> rows from Iceberg table.
✓ customers   rows=<N_source>   status=success
```

Result is identical to `--mode full_reload`.

---

### 10.3 Watermark resets to new `extraction_ts` after reload

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN python3 - << 'EOF'
import sys; sys.path.insert(0, "/opt/spark/work-dir")
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

**Expected:**
- `sf_extraction_ts` is the timestamp of the reload run (newer than any prior watermark)
- `rows_copied` equals the full source row count
- A subsequent `--mode incremental` run will pick up this new watermark as `last_ts`
  and only copy rows changed after the reload timestamp

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
| `PK_TABLE_MAP` | `customers:id,orders:id` | Per-table PK for incremental MERGE upsert |
| `FULL_RELOAD` | `1` | Convenience alias for `--mode full_reload` |

### Key starpump CLI flags for this runbook

| Flag | Values | Purpose |
|------|--------|---------|
| `--mode` | `full`, `incremental`, `custom_sql`, `full_reload` | Copy mode |
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
  pk_col   ← PK_TABLE_MAP lookup for this table        (None if not set)

  [batch loop]
    if pk_col is set:
      MERGE INTO iceberg_table ON pk_col
        WHEN MATCHED     → UPDATE all columns  (updated row overwrites stale copy)
        WHEN NOT MATCHED → INSERT              (new row added normally)
    else:
      writeTo().append()                       (old behaviour — may duplicate updated rows)

  ✓ SUCCESS:
    pipeline_watermarks.sf_extraction_ts ← new_ts    (written now)
    Iceberg _pipeline_watermarks         ← new_ts    (written now)

  ✗ FAILURE:
    pipeline_watermarks unchanged         (new_ts discarded)
    next run retries same window          (no rows skipped)
```

### full_reload flow

```
full_reload run:
  CREATE TABLE IF NOT EXISTS iceberg_table   (DDL preserved)
  DELETE FROM iceberg_table                  (all rows wiped)
  offset = 0, rows_total = 0

  [full batch loop — same as full mode]
    SELECT * FROM source LIMIT batch OFFSET offset
    writeTo(iceberg).append()

  ✓ SUCCESS:
    pipeline_watermarks.sf_extraction_ts ← new extraction_ts
    rows_copied = total source rows

  Next incremental run:
    last_ts = new extraction_ts from this reload
    WHERE updated_at > last_ts  (only rows changed after reload)
```

### When to use each mode

| Mode | When to use |
|---|---|
| `full` | Initial load; resume a partial copy |
| `incremental` | Scheduled delta sync — pick up new/updated rows only |
| `full_reload` | Scheduled clean refresh — wipe stale Iceberg data and reload entirely |
| `custom_sql` | JOIN or aggregate query result written to a new Iceberg table |

### When to use `PK_TABLE_MAP`

| Scenario | Setting |
|---|---|
| All postgres tables use `id` as PK | `PK_TABLE_MAP="*:id"` |
| Mixed PKs | `PK_TABLE_MAP="customers:id,orders:id,products:product_id"` |
| Append-only table (e.g. logs) — no upsert needed | Omit from map or don't set `PK_TABLE_MAP` |
| Full load — upsert never applies | `PK_TABLE_MAP` is ignored in `--mode full` |
