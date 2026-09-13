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
| `--pk-cols` | `PK_COLS` | *(auto)* | Comma-separated PK column(s) for MERGE join and ORDER BY |
| `--watermark-col` | `WATERMARK_COL` | `updated_at` → `created_at` | Timestamp column for incremental delta |
| — | `DELETED_AT_COL` | `deleted_at` | Column written by `soft_delete` mode |

**Source primary keys:**

| Source | Table | Primary Key | Notes |
|---|---|---|---|
| PostgreSQL | all tables | `id` | auto-detected |
| Oracle | `customers` | `customer_id` | must pass `--pk-cols customer_id` |
| Oracle | `products` | `product_id` | must pass `--pk-cols product_id` |
| Oracle | `orders` | `order_id` | must pass `--pk-cols order_id` |
| Oracle | `order_items` | `item_id` | must pass `--pk-cols item_id` |
| MongoDB | all collections | `_id` | auto-detected |

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

## Test T-28.1 — PK auto-detection (PostgreSQL)

Run incremental on PostgreSQL with no `--pk-cols` override.
Every table must log `PK cols: ['id']`.

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  starpump postgres \
    --mode incremental \
    --write-mode standard \
    --watermark-col updated_at 2>&1 | grep -E "PK cols|write_mode"
```

**Expected (one line per table):**
```
[customers]       PK cols: ['id']  (write_mode=standard)
[products]        PK cols: ['id']  (write_mode=standard)
[orders]          PK cols: ['id']  (write_mode=standard)
[product_reviews] PK cols: ['id']  (write_mode=standard)
```

✅ Pass: every table shows `['id']` — no `"No standard PK column found"` warning.
❌ Fail: `PK cols: []` → add `PK_COLS=id` env var.

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
import os; os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("t282-before")).getOrCreate()
import sys
test_id = sys.argv[1]
spark.sql(f"SELECT id, tier, snap_timestamp FROM `postgres`.`cache_testing`.`customers` WHERE id={test_id}").show()
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
  starpump postgres \
    --mode incremental \
    --write-mode standard \
    --watermark-col updated_at \
    INCLUDE_TABLES=customers
```

**Expected log:**
```
[customers] PK cols: ['id']  (write_mode=standard)
[customers] Incremental mode: col=updated_at  clause='updated_at >= ...'
[customers] MERGE INTO (upsert)
[customers] DONE
```

**Step 5 — Verify exactly ONE row with new tier:**

```bash
cat > /tmp/t282_after.py << 'PYEOF'
import os; os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("t282-after")).getOrCreate()
import sys
test_id = sys.argv[1]
df = spark.sql(f"SELECT id, tier, snap_timestamp FROM `postgres`.`cache_testing`.`customers` WHERE id={test_id} ORDER BY snap_timestamp")
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
  starpump postgres \
    --mode incremental \
    --write-mode standard \
    --watermark-col updated_at \
    INCLUDE_TABLES=customers
```

**Step 3 — Confirm row landed in Iceberg:**

```bash
cat > /tmp/t283_before.py << 'PYEOF'
import os; os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("t283-before")).getOrCreate()
import sys
del_id = sys.argv[1]
df = spark.sql(f"SELECT id, name, tier FROM `postgres`.`cache_testing`.`customers` WHERE id={del_id}")
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
  starpump postgres \
    --mode incremental \
    --write-mode standard \
    --watermark-col updated_at \
    INCLUDE_TABLES=customers
```

**Expected log:**
```
[customers] Delete-detection pass (write_mode=standard) — collecting live PKs from source window …
[customers] Live PK count in source window: N
[customers] MERGE INTO (delete pass)
[customers] Delete-detection pass complete.
```

**Step 6 — Verify row is gone from Iceberg:**

```bash
cat > /tmp/t283_after.py << 'PYEOF'
import os; os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("t283-after")).getOrCreate()
import sys
del_id = sys.argv[1]
df = spark.sql(f"SELECT id, name FROM `postgres`.`cache_testing`.`customers` WHERE id={del_id}")
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

## Test T-28.4 — `standard` mode UPDATE + DELETE (Oracle)

Oracle uses entity-specific PKs — must pass `--pk-cols` explicitly.

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

**Step 2 — Run incremental with explicit PK:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customers \
  starpump oracle \
    --mode incremental \
    --write-mode standard \
    --pk-cols customer_id \
    --watermark-col updated_at
```

**Step 3 — Verify exactly 1 row for customer_id=1 with new tier:**

```bash
cat > /tmp/t284_verify.py << 'PYEOF'
import os; os.environ["USER"] = "dave"
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

✅ Pass: exactly 1 row, `tier='PLATINUM'`.

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
[products] Delete-detection pass (write_mode=soft_delete) — collecting live PKs …
[products] MERGE INTO (delete pass)
[products] Delete-detection pass complete.
```

**Step 5 — Verify row is flagged, not removed:**

```bash
cat > /tmp/t285_verify.py << 'PYEOF'
import os; os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("t285-verify")).getOrCreate()
import sys
soft_id = sys.argv[1]
df = spark.sql(f"SELECT id, sku, is_deleted, deleted_at FROM `postgres`.`cache_testing`.`products` WHERE id={soft_id}")
df.show()
row = df.collect()[0]
print(f"Row still present: {df.count()}  (expected: 1)")
print(f"is_deleted: {row['is_deleted']}   (expected: True)")
print(f"deleted_at: {row['deleted_at']}   (expected: non-null timestamp)")
# Live filter — should return 0
n = spark.sql(f"SELECT COUNT(*) AS n FROM `postgres`.`cache_testing`.`products` WHERE id={soft_id} AND (is_deleted IS NULL OR is_deleted=false)").collect()[0]['n']
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

## Test T-28.6 — `soft_delete` mode (Oracle)

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

# Step 2 — Push into Iceberg
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=products \
  starpump oracle \
    --mode incremental \
    --write-mode soft_delete \
    --pk-cols product_id \
    --watermark-col updated_at

# Step 3 — Delete from Oracle
kubectl exec -n prod "$ORA_POD" -- sqlplus -s \
  cache_testing/CacheTesting#2025@localhost:1521/XEPDB1 << 'EOF'
DELETE FROM products WHERE sku = 'ORA-SOFT-T286';
COMMIT;
EXIT;
EOF

# Step 4 — Run soft_delete incremental again
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=products \
  starpump oracle \
    --mode incremental \
    --write-mode soft_delete \
    --pk-cols product_id \
    --watermark-col updated_at

# Step 5 — Verify flagged in Iceberg
cat > /tmp/t286_verify.py << 'PYEOF'
import os; os.environ["USER"] = "dave"
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

✅ Pass: `is_deleted=True`, row physically remains in Iceberg.

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
import os; os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("t287-verify")).getOrCreate()
df = spark.sql("""
  SELECT id, name, tier, _change_type, _change_ts, snap_timestamp
  FROM   `postgres`.`cache_testing`.`customers`
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

# Step 2 — Push into Iceberg
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
import os; os.environ["USER"] = "dave"
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

✅ Pass: exactly 1 row, `tier='PLATINUM'`.

---

## Test T-28.9 — Explicit `--pk-cols` override (Oracle)

```bash
# Single PK override
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=order_items \
  starpump oracle \
    --mode incremental \
    --write-mode standard \
    --pk-cols item_id \
    --watermark-col updated_at 2>&1 | grep "PK cols"
# Expected: [order_items] PK cols: ['item_id']

# Composite PK via env var
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  PK_COLS=order_id,item_id \
  INCLUDE_TABLES=order_items \
  starpump oracle \
    --mode incremental \
    --write-mode standard \
    --watermark-col updated_at 2>&1 | grep "PK cols"
# Expected: [order_items] PK cols: ['order_id', 'item_id']
```

✅ Pass: PK cols logged match the override exactly.

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
import os; os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("t2810-verify")).getOrCreate()
df = spark.sql("SELECT id, sku, snap_timestamp FROM `postgres`.`cache_testing`.`products` WHERE sku='BOUNDARY-T2810'")
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
❌ Fail: 0 rows → `_incremental_where_clause` still uses `>`; confirm `starpump.py` line reads `return f"{wm_col} >= '{last_ts}'"`.

---

## Scorecard

Run after all tests to verify end state in one shot:

```bash
cat > /tmp/t28_scorecard.py << 'PYEOF'
import os; os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf("t28-scorecard")).getOrCreate()

checks = [
    ("T-28.2 PG UPDATE — 1 row id=1 PLATINUM",
     "SELECT COUNT(*) AS n FROM `postgres`.`cache_testing`.`customers` WHERE id=1 AND tier='PLATINUM'",
     lambda n: n == 1, "1"),
    ("T-28.3 PG DELETE — deletetest gone",
     "SELECT COUNT(*) AS n FROM `postgres`.`cache_testing`.`customers` WHERE email='deletetest@starpump.local'",
     lambda n: n == 0, "0"),
    ("T-28.5 PG soft_delete — SOFT-DEL-T285 flagged",
     "SELECT COUNT(*) AS n FROM `postgres`.`cache_testing`.`products` WHERE sku='SOFT-DEL-T285' AND is_deleted=true",
     lambda n: n == 1, "1"),
    ("T-28.7 PG history — 2 versions of hist-a",
     "SELECT COUNT(*) AS n FROM `postgres`.`cache_testing`.`customers` WHERE email='hist-a@starpump.local'",
     lambda n: n >= 2, ">=2"),
    ("T-28.8 MGO standard — 1 PLATINUM row",
     "SELECT COUNT(*) AS n FROM `mongodb`.`cache_testing`.`customers` WHERE email='mgo-std@starpump.local' AND tier='PLATINUM'",
     lambda n: n == 1, "1"),
    ("T-28.10 PG boundary row present",
     "SELECT COUNT(*) AS n FROM `postgres`.`cache_testing`.`products` WHERE sku='BOUNDARY-T2810'",
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
| MERGE fires but 2 rows appear | `pk_cols` resolved to wrong column | Pass `--pk-cols <correct_col>` explicitly |
| Delete-detection pass never fires | Watermark clause empty (first full run) | Run one incremental pass first to establish a non-null watermark |
| Oracle MERGE fails | Missing `--pk-cols` | Oracle tables use entity PKs — always pass e.g. `--pk-cols customer_id` |
| `soft_delete` columns not in Iceberg | First run used `standard` mode | Drop + recreate Iceberg table, re-run with `--write-mode soft_delete` |
| `history` shows only 1 version | `--write-mode history` not passed | Confirm flag is present; MERGE would produce 1 row |
| Boundary row T-28.10 returns 0 | `>` still used in starpump | Confirm `_incremental_where_clause` returns `>=` in `starpump.py` |
