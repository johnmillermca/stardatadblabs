# Runbook 28 — starpump: Write Modes Testing (standard / soft_delete / history)

| Field | Value |
|---|---|
| **Runbook ID** | RB-28 |
| **Service** | k8s-platform / starpump |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2025 |

---

## Overview

This runbook tests the three write modes introduced in starpump for handling **updates** and
**deletes** during incremental loads against all three CDC sources:

| Write Mode | Behaviour | Iceberg result |
|---|---|---|
| `standard` | MERGE INTO by PK — upsert live rows, hard-DELETE vanished rows | Live mirror, no history |
| `soft_delete` | MERGE INTO by PK — upsert live rows, mark vanished rows `is_deleted=true` | Row stays, flagged |
| `history` | Plain INSERT every run — never update or delete Iceberg rows | Full audit trail |

**New CLI flags / env vars:**

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--write-mode` | `WRITE_MODE` | `standard` | One of `standard`, `soft_delete`, `history` |
| `--pk-cols` | `PK_COLS` | *(auto)* | Comma-separated PK column(s) — overrides catalog detection |
| `--watermark-col` | `WATERMARK_COL` | `updated_at` → `created_at` | Timestamp column for incremental delta |
| — | `DELETED_AT_COL` | `deleted_at` | Column written by `soft_delete` mode |

**Primary key resolution (priority order):**

1. `--pk-cols` / `PK_COLS` — explicit operator override, always wins
2. **Source catalog** — starpump calls `java.sql.DatabaseMetaData.getPrimaryKeys()` via
   the JDBC driver, which resolves real constraints from the source's own catalog with
   zero hardcoded schema or table names. Works for simple and composite PKs, any table name.
3. Name heuristic — `id` → `<table>_id` → first column (only when catalog returns nothing)

| Source | Catalog API used | Example result | Notes |
|---|---|---|---|
| PostgreSQL | `DatabaseMetaData.getPrimaryKeys()` via `org.postgresql.Driver` | `['id']` | Pass `SCHEMAS=<schema>` via `env` |
| Oracle | `DatabaseMetaData.getPrimaryKeys()` via `oracle.jdbc.OracleDriver` | `['customer_id']`, `['order_id', 'line_seq']` | Pass `SCHEMAS=<schema>` via `env`; table name auto-uppercased for `ALL_CONSTRAINTS` |
| Databricks | `DatabaseMetaData.getPrimaryKeys()` (informational; usually `[]`, falls to heuristic) | `['id']` | — |
| Snowflake | `SHOW PRIMARY KEYS IN TABLE` via native Spark connector | `['id']` | — |
| MongoDB | Always `['_id']` — enforced by the storage engine | `['_id']` | — |

---

## Prerequisites

- Full initial load already done for all three sources (runbook-27 T-2.2, T-2.3, T-2.4).
- Watermarks exist in `pipeline_watermarks` for every table being tested.

---

## Step 1 — Common setup

Run once per terminal session before every step below:

```bash
MASTER=$(kubectl get pod -n prod -l component=master \
  -o jsonpath='{.items[0].metadata.name}')
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)
echo "Master: $MASTER   Token: ${TOKEN:0:10}..."
```

---

## Test T-28.1 — PK catalog detection (PostgreSQL + Oracle)

Verifies that starpump reads the real PK from the source database's own constraint
catalog — not by guessing column names.

**Step 1 — PostgreSQL: confirm `id` is detected from `pg_constraint` (not hardcoded):**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  starpump postgres \
    --mode incremental \
    --write-mode standard \
    --watermark-col updated_at 2>&1 \
  | grep -E "PK cols resolved from source catalog|PK cols:"
```

**Expected — one line per table showing source, schema, and table name:**
```
[customers]       PK cols resolved from source catalog: ['id']  (source=postgres schema=public table=customers)
[products]        PK cols resolved from source catalog: ['id']  (source=postgres schema=public table=products)
[orders]          PK cols resolved from source catalog: ['id']  (source=postgres schema=public table=orders)
[product_reviews] PK cols resolved from source catalog: ['id']  (source=postgres schema=public table=product_reviews)
```

✅ Pass: every line shows `resolved from source catalog` with `source=postgres schema=public` — catalog path is active.
❌ Fail: `PK not found in source catalog` warning — catalog call failed, fell to heuristic; check JDBC connectivity.

**Step 2 — Oracle: confirm entity-specific PKs are detected without `--pk-cols`:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN SCHEMAS=cache_testing \
  starpump oracle \
    --mode incremental \
    --write-mode standard \
    --watermark-col updated_at 2>&1 \
  | grep -E "PK cols resolved from source catalog|PK cols:"
```

**Expected:**
```
[customers]   PK cols resolved from source catalog: ['customer_id']  (source=oracle schema=cache_testing table=customers)
[products]    PK cols resolved from source catalog: ['product_id']   (source=oracle schema=cache_testing table=products)
[orders]      PK cols resolved from source catalog: ['order_id']     (source=oracle schema=cache_testing table=orders)
[order_items] PK cols resolved from source catalog: ['item_id']      (source=oracle schema=cache_testing table=order_items)
```

✅ Pass: each table shows its real PK name with `source=oracle schema=cache_testing` — no `--pk-cols` required.
❌ Fail: wrong PK name or heuristic fallback — check `oracle.jdbc.OracleDriver` has `ALL_CONSTRAINTS` access.

---

## Test T-28.2 — `standard` mode UPDATE (PostgreSQL)

**Step 1 — Capture a customer id and its current tier:**

```bash
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)
TEST_ID=$(kubectl exec -n prod "$PG_POD" -- \
  psql -U postgres -d cache_testing -At \
  -c "SELECT id FROM public.customers ORDER BY id LIMIT 1")
echo "Test customer id=$TEST_ID"

kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
SELECT id, name, tier, updated_at FROM public.customers WHERE id=$TEST_ID;
"
```

**Step 2 — Verify the same row in Iceberg before the update:**

```bash
cat > /tmp/t282_before.py << 'PYEOF'
import os, sys
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("t282-before")).getOrCreate()
test_id = sys.argv[1]
spark.sql(f"SELECT id, tier, snap_timestamp FROM `postgres`.`public`.`customers` WHERE id={test_id}").show()
spark.stop()
PYEOF
kubectl cp /tmp/t282_before.py prod/$MASTER:/tmp/t282_before.py -c spark-master
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir \
  python3 /tmp/t282_before.py $TEST_ID
```

**Step 3 — Update the row in PostgreSQL:**

```bash
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
UPDATE public.customers
SET    tier = 'PLATINUM', updated_at = NOW()
WHERE  id = $TEST_ID
RETURNING id, tier, updated_at;
"
```

**Step 4 — Run incremental in `standard` mode:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  starpump postgres \
    --mode incremental \
    --write-mode standard \
    --watermark-col updated_at
```

**Expected log:**
```
[customers] PK cols resolved from source catalog: ['id']  (source=postgres schema=public table=customers)
[customers] PK cols: ['id']  (source=postgres schema=public)
[customers] MERGE INTO (upsert)
[customers] DONE
```

**Step 5 — Verify exactly ONE row with new tier:**

```bash
cat > /tmp/t282_after.py << 'PYEOF'
import os, sys
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("t282-after")).getOrCreate()
test_id = sys.argv[1]
df = spark.sql(f"SELECT id, tier, snap_timestamp FROM `postgres`.`public`.`customers` WHERE id={test_id} ORDER BY snap_timestamp")
df.show()
print(f"Row count: {df.count()}  (expected: 1 — MERGE replaces, not appends)")
spark.stop()
PYEOF
kubectl cp /tmp/t282_after.py prod/$MASTER:/tmp/t282_after.py -c spark-master
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir \
  python3 /tmp/t282_after.py $TEST_ID
```

✅ Pass: exactly **1** row, `tier='PLATINUM'`.
❌ Fail: 2 rows → MERGE did not fire; check `--write-mode standard` was passed and PK resolved.

---

## Test T-28.3 — `standard` mode DELETE (PostgreSQL)

**Step 1 — Insert a sacrificial row:**

```bash
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
INSERT INTO public.customers (name, email, phone, tier, created_at, updated_at)
VALUES ('Delete Test', 'deletetest@starpump.local', '+1-000-0000', 'BRONZE', NOW(), NOW())
RETURNING id, email;
"
DEL_ID=$(kubectl exec -n prod "$PG_POD" -- \
  psql -U postgres -d cache_testing -At \
  -c "SELECT id FROM public.customers WHERE email='deletetest@starpump.local' LIMIT 1")
echo "Sacrificial row id=$DEL_ID"
```

**Step 2 — Run incremental to push the new row into Iceberg:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  starpump postgres \
    --mode incremental \
    --write-mode standard \
    --watermark-col updated_at
```

**Step 3 — Confirm row landed in Iceberg:**

```bash
cat > /tmp/t283_before.py << 'PYEOF'
import os, sys
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("t283-before")).getOrCreate()
del_id = sys.argv[1]
df = spark.sql(f"SELECT id, name, tier FROM `postgres`.`public`.`customers` WHERE id={del_id}")
df.show()
print(f"Row count before delete: {df.count()}  (expected: 1)")
spark.stop()
PYEOF
kubectl cp /tmp/t283_before.py prod/$MASTER:/tmp/t283_before.py -c spark-master
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir \
  python3 /tmp/t283_before.py $DEL_ID
```

**Step 4 — Delete the row from PostgreSQL:**

```bash
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
DELETE FROM public.customers WHERE id = $DEL_ID;
"
```

**Step 5 — Run incremental again — delete-detection pass fires:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  starpump postgres \
    --mode incremental \
    --write-mode standard \
    --watermark-col updated_at
```

**Expected log:**
```
[customers] PK cols resolved from source catalog: ['id']  (source=postgres schema=public table=customers)
[customers] PK cols: ['id']  (source=postgres schema=public)
[customers] Delete-detection pass (write_mode=standard) — collecting live PKs from source window …
[customers] Live PK count in source window: N
[customers] MERGE INTO (delete pass)
[customers] Delete-detection pass complete.
```

**Step 6 — Verify row is gone from Iceberg:**

```bash
cat > /tmp/t283_after.py << 'PYEOF'
import os, sys
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("t283-after")).getOrCreate()
del_id = sys.argv[1]
df = spark.sql(f"SELECT id, name FROM `postgres`.`public`.`customers` WHERE id={del_id}")
df.show()
print(f"Row count after delete: {df.count()}  (expected: 0 — hard deleted from Iceberg)")
spark.stop()
PYEOF
kubectl cp /tmp/t283_after.py prod/$MASTER:/tmp/t283_after.py -c spark-master
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir \
  python3 /tmp/t283_after.py $DEL_ID
```

✅ Pass: **0** rows — physically removed from Iceberg.
❌ Fail: row still present → delete-detection pass did not fire; confirm watermark clause is non-empty (requires at least one prior incremental run).

---

## Test T-28.4 — `standard` mode UPDATE + DELETE (Oracle, catalog PK detection)

Oracle uses entity-specific PKs (`customer_id`, `order_id`, etc.). starpump now detects
them automatically via `DatabaseMetaData.getPrimaryKeys()` — **no `--pk-cols` required**.

**Step 1 — Update a customer in Oracle:**

```bash
ORA_POD=$(kubectl get pod -n prod -l app=oracle-xe \
  -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n prod "$ORA_POD" -- sqlplus -s \
  cache_testing/CacheTesting#2025@localhost:1521/XEPDB1 << 'EOF'
UPDATE customers SET tier = 'PLATINUM', updated_at = SYSTIMESTAMP WHERE customer_id = 1;
COMMIT;
SELECT customer_id, tier, updated_at FROM customers WHERE customer_id = 1;
EXIT;
EOF
```

**Step 2 — Run incremental with NO `--pk-cols` — catalog detects `customer_id`:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN SCHEMAS=cache_testing \
  INCLUDE_TABLES=customers \
  starpump oracle \
    --mode incremental \
    --write-mode standard \
    --watermark-col updated_at
```

**Expected log:**
```
[customers] PK cols resolved from source catalog: ['customer_id']  (source=oracle schema=cache_testing table=customers)
[customers] PK cols: ['customer_id']  (source=oracle schema=cache_testing)
[customers] MERGE INTO (upsert)
[customers] DONE
```

**Step 3 — Verify exactly 1 row for customer_id=1 with new tier:**

```bash
cat > /tmp/t284_verify.py << 'PYEOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("t284-verify")).getOrCreate()
df = spark.sql("SELECT customer_id, tier, snap_timestamp FROM `oracle`.`cache_testing`.`customers` WHERE customer_id=1 ORDER BY snap_timestamp")
df.show()
print(f"Row count for customer_id=1: {df.count()}  (expected: 1 — MERGE replaces)")
spark.stop()
PYEOF
kubectl cp /tmp/t284_verify.py prod/$MASTER:/tmp/t284_verify.py -c spark-master
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir \
  python3 /tmp/t284_verify.py
```

✅ Pass: exactly 1 row, `tier='PLATINUM'`, PK log shows `resolved from source catalog: ['customer_id']`.

---

## Test T-28.5 — `soft_delete` mode (PostgreSQL)

**Step 1 — Insert a test product:**

```bash
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
INSERT INTO public.products (sku, name, category, price, stock_qty, weight_kg, created_at, updated_at)
VALUES ('SOFT-DEL-T285', 'Soft Delete Test', 'Testing', 1.00, 1, 0.1, NOW(), NOW())
RETURNING id, sku;
"
SOFT_ID=$(kubectl exec -n prod "$PG_POD" -- \
  psql -U postgres -d cache_testing -At \
  -c "SELECT id FROM public.products WHERE sku='SOFT-DEL-T285' LIMIT 1")
echo "Test row id=$SOFT_ID"
```

**Step 2 — Push row into Iceberg via `soft_delete` incremental:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=products \
  starpump postgres \
    --mode incremental \
    --write-mode soft_delete \
    --watermark-col updated_at
```

**Step 3 — Delete the row from PostgreSQL:**

```bash
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
DELETE FROM public.products WHERE id = $SOFT_ID;
"
```

**Step 4 — Run incremental again in `soft_delete` mode:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=products \
  starpump postgres \
    --mode incremental \
    --write-mode soft_delete \
    --watermark-col updated_at
```

**Expected log:**
```
[products] PK cols resolved from source catalog: ['id']  (source=postgres schema=public table=products)
[products] PK cols: ['id']  (source=postgres schema=public)
[products] Delete-detection pass (write_mode=soft_delete) — collecting live PKs …
[products] MERGE INTO (delete pass)
[products] Delete-detection pass complete.
```

**Step 5 — Verify row is flagged, not removed:**

```bash
cat > /tmp/t285_verify.py << 'PYEOF'
import os, sys
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("t285-verify")).getOrCreate()
soft_id = sys.argv[1]
df = spark.sql(f"SELECT id, sku, is_deleted, deleted_at FROM `postgres`.`public`.`products` WHERE id={soft_id}")
df.show()
row = df.collect()[0]
print(f"Row still present: {df.count()}  (expected: 1)")
print(f"is_deleted: {row['is_deleted']}   (expected: True)")
print(f"deleted_at: {row['deleted_at']}   (expected: non-null timestamp)")
n = spark.sql(f"SELECT COUNT(*) AS n FROM `postgres`.`public`.`products` WHERE id={soft_id} AND (is_deleted IS NULL OR is_deleted=false)").collect()[0]['n']
print(f"Live rows (is_deleted filter): {n}  (expected: 0)")
spark.stop()
PYEOF
kubectl cp /tmp/t285_verify.py prod/$MASTER:/tmp/t285_verify.py -c spark-master
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir \
  python3 /tmp/t285_verify.py $SOFT_ID
```

✅ Pass: row present, `is_deleted=True`, `deleted_at` set, live-filter count = 0.
❌ Fail: row missing entirely → mode was `standard`; re-run with `--write-mode soft_delete`.

---

## Test T-28.6 — `soft_delete` mode (Oracle, catalog PK detection)

Oracle's `product_id` PK is detected automatically — no `--pk-cols`.

```bash
ORA_POD=$(kubectl get pod -n prod -l app=oracle-xe \
  -o jsonpath='{.items[0].metadata.name}')

# Step 1 — Insert a sacrificial product
kubectl exec -n prod "$ORA_POD" -- sqlplus -s \
  cache_testing/CacheTesting#2025@localhost:1521/XEPDB1 << 'EOF'
INSERT INTO products (product_id, product_name, category, subcategory, brand,
                      sku, price, stock_qty, is_active, created_at, updated_at)
VALUES (9999999, 'Oracle SoftDel Test', 'Testing', 'QA', 'TestBrand',
        'ORA-SOFT-T286', 0.01, 1, 'Y', SYSTIMESTAMP, SYSTIMESTAMP);
COMMIT;
SELECT product_id, sku FROM products WHERE sku='ORA-SOFT-T286';
EXIT;
EOF

# Step 2 — Push into Iceberg (no --pk-cols — catalog detects product_id)
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN SCHEMAS=cache_testing \
  INCLUDE_TABLES=products \
  starpump oracle \
    --mode incremental \
    --write-mode soft_delete \
    --watermark-col updated_at

# Step 3 — Delete from Oracle
kubectl exec -n prod "$ORA_POD" -- sqlplus -s \
  cache_testing/CacheTesting#2025@localhost:1521/XEPDB1 << 'EOF'
DELETE FROM products WHERE sku = 'ORA-SOFT-T286';
COMMIT;
EXIT;
EOF

# Step 4 — Run soft_delete incremental again (no --pk-cols)
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN SCHEMAS=cache_testing \
  INCLUDE_TABLES=products \
  starpump oracle \
    --mode incremental \
    --write-mode soft_delete \
    --watermark-col updated_at

# Step 5 — Verify flagged in Iceberg
cat > /tmp/t286_verify.py << 'PYEOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("t286-verify")).getOrCreate()
df = spark.sql("SELECT product_id, sku, is_deleted, deleted_at FROM `oracle`.`cache_testing`.`products` WHERE product_id=9999999")
df.show()
print(f"is_deleted: {df.collect()[0]['is_deleted']}  (expected: True)")
spark.stop()
PYEOF
kubectl cp /tmp/t286_verify.py prod/$MASTER:/tmp/t286_verify.py -c spark-master
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir \
  python3 /tmp/t286_verify.py
```

✅ Pass: `is_deleted=True`, row physically remains in Iceberg, log shows `PK cols resolved from source catalog: ['product_id']`.

---

## Test T-28.7 — `history` mode (PostgreSQL)

History mode never updates or deletes Iceberg rows — every incremental read appends new rows tagged with `_change_type` and `_change_ts`.

**Step 1 — Insert 2 test customers:**

```bash
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
INSERT INTO public.customers (name, email, phone, tier, created_at, updated_at)
VALUES
  ('History Test A', 'hist-a@starpump.local', '+1-001', 'GOLD',   NOW(), NOW()),
  ('History Test B', 'hist-b@starpump.local', '+1-002', 'SILVER', NOW(), NOW());
"
```

**Step 2 — Run incremental in `history` mode:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  starpump postgres \
    --mode incremental \
    --write-mode history \
    --watermark-col updated_at
```

**Expected log (no MERGE, no delete-detection):**
```
[customers] write_mode=history — plain append
[customers] DONE — 2 rows written
```

**Step 3 — Update one of the rows:**

```bash
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
UPDATE public.customers
SET    tier = 'PLATINUM', updated_at = NOW()
WHERE  email = 'hist-a@starpump.local';
"
```

**Step 4 — Run incremental in `history` mode again:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  starpump postgres \
    --mode incremental \
    --write-mode history \
    --watermark-col updated_at
```

**Step 5 — Verify both versions exist in Iceberg:**

```bash
cat > /tmp/t287_verify.py << 'PYEOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("t287-verify")).getOrCreate()
df = spark.sql("""
  SELECT id, name, tier, _change_type, _change_ts, snap_timestamp
  FROM   `postgres`.`public`.`customers`
  WHERE  email = 'hist-a@starpump.local'
  ORDER  BY snap_timestamp
""")
df.show()
print(f"Total versions for hist-a: {df.count()}  (expected: 2 — original GOLD + updated PLATINUM)")
spark.stop()
PYEOF
kubectl cp /tmp/t287_verify.py prod/$MASTER:/tmp/t287_verify.py -c spark-master
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir \
  python3 /tmp/t287_verify.py
```

✅ Pass: **2 rows** — `tier=GOLD` (original) and `tier=PLATINUM` (after update). Both have `_change_type='INSERT'`.
❌ Fail: 1 row → MERGE fired instead of append; check `--write-mode history` was passed.

---

## Test T-28.8 — `standard` mode MERGE via `_id` PK (MongoDB)

```bash
MONGO_PASS=$(kubectl get secret mongodb-credentials -n prod \
  -o jsonpath='{.data.mongodb-root-password}' | base64 -d)

# Step 1 — Insert a test document
kubectl exec -n prod mongodb-0 -- mongosh \
  --username root --password "$MONGO_PASS" \
  --authenticationDatabase admin --quiet \
  cache_testing --eval '
db.customers.insertOne({
  email:        "mgo-std@starpump.local",
  first_name:   "MGO",
  last_name:    "StdTest",
  tier:         "SILVER",
  country_code: "US",
  city:         "TestCity",
  created_at:   new Date(),
  updated_at:   new Date()
});
print("inserted _id: " + db.customers.findOne({email:"mgo-std@starpump.local"})._id);
'

# Step 2 — Push into Iceberg (_id auto-detected, no --pk-cols)
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  starpump mongodb \
    --mode incremental \
    --write-mode standard \
    --watermark-col updated_at

# Step 3 — Update the document
kubectl exec -n prod mongodb-0 -- mongosh \
  --username root --password "$MONGO_PASS" \
  --authenticationDatabase admin --quiet \
  cache_testing --eval '
db.customers.updateOne(
  {email: "mgo-std@starpump.local"},
  {$set: {tier: "PLATINUM", updated_at: new Date()}}
);
print("updated tier → PLATINUM");
'

# Step 4 — Run incremental again
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  starpump mongodb \
    --mode incremental \
    --write-mode standard \
    --watermark-col updated_at

# Step 5 — Verify exactly 1 row with PLATINUM tier
cat > /tmp/t288_verify.py << 'PYEOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("t288-verify")).getOrCreate()
df = spark.sql("SELECT _id, email, tier, snap_timestamp FROM `mongodb`.`cache_testing`.`customers` WHERE email='mgo-std@starpump.local' ORDER BY snap_timestamp")
df.show()
print(f"Row count: {df.count()}  (expected: 1 — MERGE replaces via _id)")
spark.stop()
PYEOF
kubectl cp /tmp/t288_verify.py prod/$MASTER:/tmp/t288_verify.py -c spark-master
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir \
  python3 /tmp/t288_verify.py
```

✅ Pass: exactly 1 row, `tier='PLATINUM'`, log shows `PK cols resolved from source catalog: ['_id']`.

---

## Test T-28.9 — `--pk-cols` override takes priority over catalog (Oracle)

Verifies that an explicit `--pk-cols` value beats the catalog result — useful when you
want a composite MERGE key that differs from the table's declared PK.

```bash
# Single override — operator forces item_id; catalog is bypassed entirely
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN SCHEMAS=cache_testing \
  INCLUDE_TABLES=order_items \
  starpump oracle \
    --mode incremental \
    --write-mode standard \
    --pk-cols item_id \
    --watermark-col updated_at 2>&1 | grep "PK cols"
# Expected: [order_items] PK cols: ['item_id']  (source=oracle schema=cache_testing)
# Note: no "resolved from source catalog" — override bypasses the catalog call entirely

# Composite override via env var — forces a 2-column join key
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN SCHEMAS=cache_testing \
  PK_COLS=order_id,item_id \
  INCLUDE_TABLES=order_items \
  starpump oracle \
    --mode incremental \
    --write-mode standard \
    --watermark-col updated_at 2>&1 | grep "PK cols"
# Expected: [order_items] PK cols: ['order_id', 'item_id']  (source=oracle schema=cache_testing)
```

✅ Pass: PK cols log shows `source=oracle schema=cache_testing` and does **not** contain `resolved from source catalog`.
❌ Fail: shows `resolved from source catalog` → override was not passed correctly.

---

## Test T-28.10 — Watermark boundary `>=` (PostgreSQL)

A row whose `updated_at` equals exactly the last watermark must be captured (not skipped).

**Step 1 — Capture the current watermark for `products`:**

```bash
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)
WM_TS=$(kubectl exec -n prod "$PG_POD" -- \
  psql -U pipeline -d pipeline -At \
  -c "SELECT sf_extraction_ts FROM pipeline_watermarks
      WHERE source_db='cache_testing' AND source_schema='public' AND table_name='products'")
echo "Current watermark: $WM_TS"
```

**Step 2 — Insert a row at EXACTLY the watermark timestamp:**

```bash
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
INSERT INTO public.products (sku, name, category, price, stock_qty, weight_kg,
                             created_at, updated_at)
VALUES ('BOUNDARY-T2810', 'Boundary Test', 'Testing', 1.00, 1, 0.1,
        TIMESTAMP WITH TIME ZONE '$WM_TS',
        TIMESTAMP WITH TIME ZONE '$WM_TS');
SELECT id, sku, updated_at FROM products WHERE sku='BOUNDARY-T2810';
"
```

**Step 3 — Run incremental (`>=` must include this row):**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=products \
  starpump postgres \
    --mode incremental \
    --write-mode standard \
    --watermark-col updated_at
```

**Step 4 — Verify the boundary row was captured:**

```bash
cat > /tmp/t2810_verify.py << 'PYEOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("t2810-verify")).getOrCreate()
df = spark.sql("SELECT id, sku, snap_timestamp FROM `postgres`.`public`.`products` WHERE sku='BOUNDARY-T2810'")
df.show()
print(f"Boundary row count: {df.count()}  (expected: 1 — >= includes exact boundary)")
spark.stop()
PYEOF
kubectl cp /tmp/t2810_verify.py prod/$MASTER:/tmp/t2810_verify.py -c spark-master
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir \
  python3 /tmp/t2810_verify.py
```

✅ Pass: exactly 1 row — `>=` boundary is inclusive.
❌ Fail: 0 rows → `_incremental_where_clause` still uses `>`; confirm `starpump.py` reads `return f"{wm_col} >= '{last_ts}'"`.

---

## Test T-28.11 — Oracle composite PK detected from catalog

Verifies that a table with a multi-column primary key returns both columns in declaration
order from `DatabaseMetaData.getPrimaryKeys()` — no `--pk-cols` required.

**Step 1 — Create a composite-PK test table in Oracle:**

```bash
ORA_POD=$(kubectl get pod -n prod -l app=oracle-xe \
  -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n prod "$ORA_POD" -- sqlplus -s \
  cache_testing/CacheTesting#2025@localhost:1521/XEPDB1 << 'EOF'
CREATE TABLE order_line_items (
  order_id    NUMBER(10)    NOT NULL,
  line_seq    NUMBER(5)     NOT NULL,
  product_id  NUMBER(10)    NOT NULL,
  qty         NUMBER(6)     DEFAULT 1,
  unit_price  NUMBER(12, 2),
  updated_at  TIMESTAMP     DEFAULT SYSTIMESTAMP,
  CONSTRAINT pk_order_line_items PRIMARY KEY (order_id, line_seq)
);
INSERT INTO order_line_items VALUES (1001, 1, 42, 2, 19.99, SYSTIMESTAMP);
INSERT INTO order_line_items VALUES (1001, 2, 77, 1, 49.99, SYSTIMESTAMP);
INSERT INTO order_line_items VALUES (1002, 1, 42, 3, 19.99, SYSTIMESTAMP);
COMMIT;
SELECT order_id, line_seq, product_id FROM order_line_items;
EXIT;
EOF
```

**Step 2 — Run incremental — catalog must detect `(order_id, line_seq)`:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN SCHEMAS=cache_testing \
  INCLUDE_TABLES=order_line_items \
  starpump oracle \
    --mode incremental \
    --write-mode standard \
    --watermark-col updated_at 2>&1 | grep -E "PK cols"
```

**Expected:**
```
[order_line_items] PK cols resolved from source catalog: ['order_id', 'line_seq']  (source=oracle schema=cache_testing table=order_line_items)
```

**Step 3 — Update one row and confirm MERGE joins on both columns:**

```bash
kubectl exec -n prod "$ORA_POD" -- sqlplus -s \
  cache_testing/CacheTesting#2025@localhost:1521/XEPDB1 << 'EOF'
UPDATE order_line_items SET qty = 5, updated_at = SYSTIMESTAMP
WHERE order_id = 1001 AND line_seq = 1;
COMMIT;
EXIT;
EOF

kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN SCHEMAS=cache_testing \
  INCLUDE_TABLES=order_line_items \
  starpump oracle \
    --mode incremental \
    --write-mode standard \
    --watermark-col updated_at
```

**Step 4 — Verify exactly 1 row for (1001, 1) with qty=5:**

```bash
cat > /tmp/t2811_verify.py << 'PYEOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("t2811-verify")).getOrCreate()
df = spark.sql("""
  SELECT order_id, line_seq, qty, snap_timestamp
  FROM   `oracle`.`cache_testing`.`order_line_items`
  WHERE  order_id=1001 AND line_seq=1
  ORDER  BY snap_timestamp
""")
df.show()
count = df.count()
qty   = df.collect()[0]["qty"] if count > 0 else None
print(f"Row count for (1001,1): {count}  (expected: 1 — composite MERGE replaces)")
print(f"qty: {qty}  (expected: 5)")
spark.stop()
PYEOF
kubectl cp /tmp/t2811_verify.py prod/$MASTER:/tmp/t2811_verify.py -c spark-master
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir \
  python3 /tmp/t2811_verify.py
```

✅ Pass: 1 row, `qty=5`, log shows `['order_id', 'line_seq']` — composite PK detected and joined correctly.
❌ Fail: 2 rows → MERGE join produced a Cartesian match; check `_build_pk_order_clause` uses both columns in `ON` clause.

**Step 5 — Cleanup test table:**

```bash
kubectl exec -n prod "$ORA_POD" -- sqlplus -s \
  cache_testing/CacheTesting#2025@localhost:1521/XEPDB1 << 'EOF'
DROP TABLE order_line_items;
EXIT;
EOF
```

---

## Scorecard

Run after all tests to verify end state in one shot:

```bash
cat > /tmp/t28_scorecard.py << 'PYEOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("t28-scorecard")).getOrCreate()

checks = [
    ("T-28.2 PG UPDATE — 1 row id=1 PLATINUM",
     "SELECT COUNT(*) AS n FROM `postgres`.`public`.`customers` WHERE id=1 AND tier='PLATINUM'",
     lambda n: n == 1, "1"),
    ("T-28.3 PG DELETE — deletetest gone",
     "SELECT COUNT(*) AS n FROM `postgres`.`public`.`customers` WHERE email='deletetest@starpump.local'",
     lambda n: n == 0, "0"),
    ("T-28.4 ORA UPDATE — customer_id=1 PLATINUM (no --pk-cols)",
     "SELECT COUNT(*) AS n FROM `oracle`.`cache_testing`.`customers` WHERE customer_id=1 AND tier='PLATINUM'",
     lambda n: n == 1, "1"),
    ("T-28.5 PG soft_delete — SOFT-DEL-T285 flagged",
     "SELECT COUNT(*) AS n FROM `postgres`.`public`.`products` WHERE sku='SOFT-DEL-T285' AND is_deleted=true",
     lambda n: n == 1, "1"),
    ("T-28.6 ORA soft_delete — product_id=9999999 flagged (no --pk-cols)",
     "SELECT COUNT(*) AS n FROM `oracle`.`cache_testing`.`products` WHERE product_id=9999999 AND is_deleted=true",
     lambda n: n == 1, "1"),
    ("T-28.7 PG history — 2 versions of hist-a",
     "SELECT COUNT(*) AS n FROM `postgres`.`public`.`customers` WHERE email='hist-a@starpump.local'",
     lambda n: n >= 2, ">=2"),
    ("T-28.8 MGO standard — 1 PLATINUM row",
     "SELECT COUNT(*) AS n FROM `mongodb`.`cache_testing`.`customers` WHERE email='mgo-std@starpump.local' AND tier='PLATINUM'",
     lambda n: n == 1, "1"),
    ("T-28.10 PG boundary row present",
     "SELECT COUNT(*) AS n FROM `postgres`.`public`.`products` WHERE sku='BOUNDARY-T2810'",
     lambda n: n == 1, "1"),
]

for desc, sql, chk, exp in checks:
    try:
        n = spark.sql(sql).collect()[0]["n"]
        status = "✅" if chk(n) else "❌"
        print(f"{status}  {desc}  → {n} (expected {exp})")
    except Exception as e:
        print(f"❌  {desc}  → ERROR: {e}")

spark.stop()
PYEOF
kubectl cp /tmp/t28_scorecard.py prod/$MASTER:/tmp/t28_scorecard.py -c spark-master
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir \
  python3 /tmp/t28_scorecard.py
```

---

## Cleanup

```bash
PG_POD=$(kubectl get pod -n prod -l app=postgresql -o name | head -1)
ORA_POD=$(kubectl get pod -n prod -l app=oracle-xe \
  -o jsonpath='{.items[0].metadata.name}')
MONGO_PASS=$(kubectl get secret mongodb-credentials -n prod \
  -o jsonpath='{.data.mongodb-root-password}' | base64 -d)

# PostgreSQL
kubectl exec -n prod "$PG_POD" -- psql -U postgres -d cache_testing -c "
DELETE FROM public.customers
  WHERE email IN ('deletetest@starpump.local','hist-a@starpump.local','hist-b@starpump.local');
DELETE FROM public.products
  WHERE sku IN ('SOFT-DEL-T285','BOUNDARY-T2810');
UPDATE public.customers SET tier='GOLD', updated_at=NOW() WHERE id=1;
"

# Oracle
kubectl exec -n prod "$ORA_POD" -- sqlplus -s \
  cache_testing/CacheTesting#2025@localhost:1521/XEPDB1 << 'EOF'
DELETE FROM products WHERE sku = 'ORA-SOFT-T286';
UPDATE customers SET tier='GOLD', updated_at=SYSTIMESTAMP WHERE customer_id=1;
COMMIT;
EXIT;
EOF

# MongoDB
kubectl exec -n prod mongodb-0 -- mongosh \
  --username root --password "$MONGO_PASS" \
  --authenticationDatabase admin --quiet \
  cache_testing --eval \
  'db.customers.deleteOne({email:"mgo-std@starpump.local"}); print("cleaned")'
```

---

## Troubleshooting

| Symptom | Likely Cause | Resolution |
|---------|-------------|------------|
| Oracle `PK not found in source catalog` warning | `SCHEMAS` not passed — Oracle ran against wrong schema | Add `SCHEMAS=<schema>` inside `env`: `env USER=dave TOKEN=$TOKEN SCHEMAS=cache_testing starpump oracle …` |
| Oracle PKs still not found after `SCHEMAS` set | `getPrimaryKeys()` got lowercase table name | Upgrade to image `3.5.1-6` — table name is now auto-uppercased for Oracle |
| `resolved from source catalog` missing in logs | Catalog call failed silently | Check JDBC connectivity; starpump falls back to heuristic — look for `PK cols: ['id']` without "catalog" prefix |
| `schema=X` passed after binary name is ignored | `schema=X` is not a starpump CLI arg — silently dropped | Pass as env var: `env … SCHEMAS=X starpump oracle …` |
| MERGE fires but 2 rows appear | PK resolved to wrong column | Check log for actual PK used; override with `--pk-cols <correct_col>` if needed |
| Delete-detection pass never fires | Watermark clause empty (first full run) | Run one incremental pass first to establish a non-null watermark |
| Composite PK test shows 2 rows after UPDATE | Only first PK column used in join | Confirm `_build_pk_order_clause` emits both columns; check catalog returned both in `KEY_SEQ` order |
| `soft_delete` columns not in Iceberg | First run used `standard` mode | Drop + recreate Iceberg table, re-run with `--write-mode soft_delete` |
| `history` shows only 1 version | `--write-mode history` not passed | Confirm flag is present; MERGE would produce 1 row |
| Boundary row T-28.10 returns 0 | `>` still used in starpump | Confirm `_incremental_where_clause` returns `>=` in `starpump.py` |
