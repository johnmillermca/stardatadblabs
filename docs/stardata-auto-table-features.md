# Stardata — Automatic Table Features

> **Applies to:** Stardata FE image `fe-4.5.1` and later.  
> This feature is a compile-time patch in [`stardata/doris-patch/CreateTableInfo.java`](../stardata/doris-patch/CreateTableInfo.java).

---

## Overview

Every OLAP table created in Stardata automatically receives four enhancements at `CREATE TABLE` time — no extra DDL required from the developer:

| # | Feature | What it does |
|---|---------|-------------|
| (a) | `snap_id BIGINT AUTO_INCREMENT` | Unique, auto-incrementing row identifier injected as the first value column |
| (b) | `created_at DATETIMEV2 NOT NULL DEFAULT CURRENT_TIMESTAMP` | Row-creation timestamp injected as the last column |
| (c) | `AUTO PARTITION BY RANGE (date_trunc('day', created_at))` | Daily range partitions created on demand; no manual partition management |
| (d) | `storage_policy = sd_cold_1d` | Segments older than **1 day** are automatically tiered to S3 cold storage |

All four features are injected transparently inside the FE `validate()` pipeline — the user's `CREATE TABLE` statement is never modified; the enrichment happens server-side before the table is written to the catalog.

---

## Scope — what is affected

The features apply to **every OLAP table** (engine `olap`, internal catalog) **except**:

| Excluded | Reason |
|---|---|
| `cache_system` database | Internal bookkeeping; must not be partitioned or tiered |
| `__internal_schema` database | Doris internal schema |
| `information_schema` database | Doris information schema |
| External-engine tables (`engine=iceberg`, `engine=hive`, `engine=jdbc`, …) | Managed by external catalogs; DDL changes would corrupt them |
| CTAS (`CREATE TABLE … AS SELECT`) | Column list is not yet resolved at injection time |

---

## Connecting to Stardata

```bash
# MySQL client from the control node
mysql -h 192.168.1.50 -P 30090 -u root -p
```

Or from inside the cluster:

```bash
kubectl exec -n prod statefulset/stardata-fe -c stardata-fe -- \
  mysql -h127.0.0.1 -P9030 -uroot -p"${STARDATA_ADMIN_PASS}"
```

---

## Step-by-step walkthrough

### Step 1 — Create a database

```sql
CREATE DATABASE IF NOT EXISTS star_demo;
USE star_demo;
```

---

### Step 2 — Minimal CREATE TABLE (all features auto-injected)

Write only the columns that describe your domain. The four injected columns are added automatically.

```sql
-- What you write:
CREATE TABLE orders (
    order_id   BIGINT        NOT NULL,
    customer   VARCHAR(128)  NOT NULL,
    amount     DECIMAL(18,2) NOT NULL
)
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS AUTO;
```

**What Stardata actually creates** (equivalent DDL):

```sql
CREATE TABLE orders (
    order_id   BIGINT        NOT NULL,
    snap_id    BIGINT        NOT NULL AUTO_INCREMENT COMMENT 'auto-increment row identifier',
    customer   VARCHAR(128)  NOT NULL,
    amount     DECIMAL(18,2) NOT NULL,
    created_at DATETIMEV2    NOT NULL DEFAULT CURRENT_TIMESTAMP
                             COMMENT 'row creation timestamp (auto-injected by Stardata)'
)
DUPLICATE KEY(order_id)
AUTO PARTITION BY RANGE (date_trunc('day', created_at)) ()
DISTRIBUTED BY HASH(order_id) BUCKETS AUTO
PROPERTIES (
    "storage_policy" = "sd_cold_1d"
);
```

**Verify the final schema:**

```sql
SHOW CREATE TABLE orders\G
```

**Expected output excerpt:**

```
`snap_id`    bigint NOT NULL AUTO_INCREMENT COMMENT 'auto-increment row identifier',
`created_at` datetimev2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'row creation timestamp ...',
...
AUTO PARTITION BY RANGE (date_trunc('day', `created_at`)) ()
...
"storage_policy" = "sd_cold_1d"
```

**Check that daily partitions are being created as rows arrive:**

```sql
SHOW PARTITIONS FROM orders;
```

After inserting a row the partition for today (e.g. `p20261120`) appears automatically.

---

### Step 3 — Insert rows and observe partitioning

```sql
-- Insert a row; created_at defaults to NOW(), snap_id assigned by Doris
INSERT INTO orders (order_id, customer, amount)
VALUES (1001, 'Alice', 199.99);

-- Insert a back-dated row (explicit created_at → lands in a different partition)
INSERT INTO orders (order_id, customer, amount, created_at)
VALUES (1002, 'Bob', 49.99, '2026-11-18 10:30:00');

-- Verify partition assignment
SELECT order_id, snap_id, created_at FROM orders ORDER BY created_at;
SHOW PARTITIONS FROM orders;
```

Expected: two partitions — `p20261118` and `p20261120`. Doris creates each partition on first write; no `ALTER TABLE ADD PARTITION` is ever needed.

---

### Step 4 — Table with a user-declared `created_at` (column kept as-is)

If your DDL already includes `created_at`, the injector **skips column injection** and uses your definition. Partitioning and storage policy are still applied.

```sql
CREATE TABLE user_events (
    user_id    BIGINT       NOT NULL,
    event_type VARCHAR(64)  NOT NULL,
    -- User provides their own created_at with different precision:
    created_at DATETIMEV2(6) NOT NULL DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS AUTO;
```

Result: **no duplicate column error**; your `DATETIMEV2(6)` definition is preserved; AUTO RANGE partition and `sd_cold_1d` policy are still applied.

---

### Step 5 — Opt out of auto-partitioning for a single table

Add `"stardata.disable_partition" = "true"` to `PROPERTIES`. The `created_at` column and storage policy are still injected; only the `AUTO PARTITION BY RANGE` is suppressed.

```sql
CREATE TABLE lookup_ref (
    code  VARCHAR(32)  NOT NULL,
    label VARCHAR(256) NOT NULL
)
DUPLICATE KEY(code)
DISTRIBUTED BY HASH(code) BUCKETS 4
PROPERTIES (
    "stardata.disable_partition" = "true"
);
```

> The property is consumed and removed by the FE patch before Doris's property analyzer
> sees it — it will **not** appear in `SHOW CREATE TABLE`.

**Verify:**

```sql
SHOW CREATE TABLE lookup_ref\G
-- Expect: no AUTO PARTITION clause, but storage_policy=sd_cold_1d is present
-- and created_at column appears at the end of the schema
```

---

### Step 6 — Bring your own partition (explicit partition kept)

If your DDL contains an explicit `PARTITION BY` clause, the auto-partition injector skips it. Your custom partition scheme wins.

```sql
CREATE TABLE sensor_data (
    sensor_id BIGINT      NOT NULL,
    reading   DOUBLE      NOT NULL
)
DUPLICATE KEY(sensor_id)
PARTITION BY RANGE(sensor_id) (
    PARTITION p_low    VALUES LESS THAN (1000),
    PARTITION p_medium VALUES LESS THAN (5000),
    PARTITION p_high   VALUES LESS THAN (MAXVALUE)
)
DISTRIBUTED BY HASH(sensor_id) BUCKETS AUTO;
```

Result: your `RANGE(sensor_id)` partition is preserved. `created_at` column and `sd_cold_1d` storage policy are still applied.

---

### Step 7 — Bring your own storage policy (custom policy kept)

If you set `storage_policy` in `PROPERTIES`, the patch uses `putIfAbsent` — your value is preserved.

```sql
CREATE TABLE archive_log (
    log_id BIGINT      NOT NULL,
    msg    STRING      NOT NULL
)
DUPLICATE KEY(log_id)
DISTRIBUTED BY HASH(log_id) BUCKETS AUTO
PROPERTIES (
    "storage_policy" = "sd_s3_archive_30d"   -- your own 30-day policy
);
```

Result: `storage_policy = sd_s3_archive_30d` is used. The injector does not overwrite it.

---

### Step 8 — cache_system database (excluded entirely)

Tables in `cache_system` receive **none** of the four features:

```sql
USE cache_system;

CREATE TABLE cache_entries (
    cache_key   VARCHAR(255) NOT NULL,
    cache_value TEXT
)
DUPLICATE KEY(cache_key)
DISTRIBUTED BY HASH(cache_key) BUCKETS 4;
```

```sql
SHOW CREATE TABLE cache_system.cache_entries\G
-- Expected: no snap_id, no created_at, no AUTO PARTITION, no storage_policy injection
```

---

### Step 9 — External catalog table (excluded entirely)

External-engine tables (Iceberg, Hive, JDBC, …) are never modified:

```sql
-- In an Iceberg external catalog — no injection occurs
CREATE TABLE my_iceberg_catalog.warehouse.shipments (
    shipment_id BIGINT,
    shipped_at  TIMESTAMP
)
ENGINE = iceberg;
```

---

## Hot / Cold storage behaviour

The `sd_cold_1d` storage policy is backed by the `sd_s3_cold` S3 resource
(configured in [`manifests/stardata/setup/06_storage_policies.sql`](../manifests/stardata/setup/06_storage_policies.sql)).

| Tier | Location | Cooldown trigger |
|------|----------|-----------------|
| **Hot** | Local NVMe on BE nodes | Active writes and recent reads |
| **Cold** | S3 bucket `xdatatoiceberg1` | Segment is **≥ 1 day** old (`cooldown_ttl = 86400s`) |

The transition is automatic and transparent — queries still work against cold data; Doris fetches from S3 on demand.

**Inspect the policy:**

```sql
SHOW STORAGE POLICY sd_cold_1d\G
```

**Check which tables currently have segments on cold storage:**

```sql
-- Look for segments with 'remote' storage path
SELECT table_name, partition_name, remote_data_size
FROM information_schema.table_options
WHERE storage_policy = 'sd_cold_1d'
  AND remote_data_size > 0;
```

---

## Decision matrix

| Scenario | snap_id | created_at | AUTO PARTITION | storage_policy |
|---|:---:|:---:|:---:|:---:|
| Plain `CREATE TABLE` in any user DB | ✅ | ✅ | ✅ | ✅ `sd_cold_1d` |
| User provides `created_at` column | ✅ | 🔒 kept as-is | ✅ | ✅ `sd_cold_1d` |
| `disable_partition = true` | ✅ | ✅ | ❌ skipped | ✅ `sd_cold_1d` |
| User sets own partition | ✅ | ✅ | 🔒 kept as-is | ✅ `sd_cold_1d` |
| User sets own `storage_policy` | ✅ | ✅ | ✅ | 🔒 kept as-is |
| `cache_system` database | ❌ | ❌ | ❌ | ❌ |
| External catalog table | ❌ | ❌ | ❌ | ❌ |

---

## Full end-to-end example

```sql
-- 1. Create a fresh database
CREATE DATABASE IF NOT EXISTS sales_app;
USE sales_app;

-- 2. Create the main fact table (no extra DDL needed)
CREATE TABLE transactions (
    txn_id      BIGINT        NOT NULL,
    store_id    INT           NOT NULL,
    product_id  INT           NOT NULL,
    quantity    INT           NOT NULL,
    unit_price  DECIMAL(10,2) NOT NULL,
    total       DECIMAL(12,2) NOT NULL
)
UNIQUE KEY(txn_id)
DISTRIBUTED BY HASH(txn_id) BUCKETS AUTO;

-- 3. Insert sample data (Doris assigns snap_id; created_at = now)
INSERT INTO transactions (txn_id, store_id, product_id, quantity, unit_price, total)
VALUES
    (1, 10, 101, 2, 29.99,  59.98),
    (2, 11, 202, 1, 149.00, 149.00),
    (3, 10, 303, 5, 9.99,   49.95);

-- 4. Confirm auto columns
SELECT txn_id, snap_id, total, created_at
FROM transactions
ORDER BY snap_id;
/*
 txn_id | snap_id | total  | created_at
--------+---------+--------+---------------------
      1 |       1 |  59.98 | 2026-11-20 21:00:00
      2 |       2 | 149.00 | 2026-11-20 21:00:01
      3 |       3 |  49.95 | 2026-11-20 21:00:02
*/

-- 5. Confirm daily partition was auto-created
SHOW PARTITIONS FROM transactions;
-- p20261120  → today's partition, created on first insert

-- 6. Inspect full schema
SHOW CREATE TABLE transactions\G
/*
  `txn_id`    bigint NOT NULL,
  `snap_id`   bigint NOT NULL AUTO_INCREMENT COMMENT 'auto-increment row identifier',
  ...
  `created_at` datetimev2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'row creation timestamp...',
  ...
  AUTO PARTITION BY RANGE (date_trunc('day', `created_at`)) ()
  ...
  "storage_policy" = "sd_cold_1d"
*/

-- 7. Lookup / reference table — opt out of partitioning
CREATE TABLE product_catalog (
    product_id   INT          NOT NULL,
    product_name VARCHAR(256) NOT NULL,
    category     VARCHAR(64)  NOT NULL
)
UNIQUE KEY(product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 4
PROPERTIES (
    "stardata.disable_partition" = "true"
);

-- 8. Confirm: no AUTO PARTITION, but created_at + storage_policy are still there
SHOW CREATE TABLE product_catalog\G
```

---

## Troubleshooting

### `duplicate column` error on `snap_id` or `created_at`

You have already declared that column in your DDL. The injector checks case-insensitively — make sure the existing column's name is exactly `snap_id` or `created_at`.

### Table in user DB has no `created_at` after CREATE

Check the FE logs:

```bash
kubectl logs -n prod statefulset/stardata-fe-0 -c stardata-fe | grep "Stardata:"
```

If the FE is on an older image (< `fe-4.5.1`), the patch is not active. See [Deployment](#deployment--upgrading-the-fe-image) below.

### `stardata.disable_partition` appears in `SHOW CREATE TABLE`

You are running an older FE image that uses `properties.get()` instead of `properties.remove()`. Upgrade to `fe-4.5.1`.

### Table created in `cache_system` has `created_at` unexpectedly

This should not happen. If it does, the `isStardataFeatureTarget()` guard in
[`CreateTableInfo.java`](../stardata/doris-patch/CreateTableInfo.java) has been modified.
The guard checks `STARDATA_EXCLUDED_DB_CACHE_SYSTEM.equalsIgnoreCase(dbName)`.

---

## Deployment / upgrading the FE image

The patch is baked into the FE Docker image at build time. Changes to
[`stardata/doris-patch/CreateTableInfo.java`](../stardata/doris-patch/CreateTableInfo.java)
require a new image build and push, followed by a manifest update and ArgoCD sync:

```bash
# 1. Build new FE image (bump the tag, e.g. 4.5.1 → 4.5.2)
cd stardata
podman build -f Dockerfile.fe -t 192.168.1.50:30500/apache/stardata:fe-4.5.2 .
podman push 192.168.1.50:30500/apache/stardata:fe-4.5.2

# 2. Update the manifest
sed -i 's|fe-4.5.1|fe-4.5.2|g' ../manifests/stardata/stardata-fe-deployment.yaml

# 3. Commit and push (ArgoCD selfHeal=true — always git-push first)
git add ../manifests/stardata/stardata-fe-deployment.yaml
git commit -m "chore: bump FE image to fe-4.5.2"
git push

# 4. Wait for ArgoCD to sync (or force it)
argocd app sync stardata --server <argocd-server>

# 5. Rolling restart (ArgoCD may do this automatically; force if needed)
kubectl rollout restart statefulset/stardata-fe -n prod
kubectl rollout status statefulset/stardata-fe -n prod
```

> **Never** use `kubectl set image` alone. ArgoCD's `selfHeal: true` will revert the pod
> spec back to what is in git within seconds.

---

## Related files

| File | Purpose |
|---|---|
| [`stardata/doris-patch/CreateTableInfo.java`](../stardata/doris-patch/CreateTableInfo.java) | FE patch — all four injection methods |
| [`stardata/Dockerfile.fe`](../stardata/Dockerfile.fe) | FE image build — compiles and installs the patch |
| [`manifests/stardata/setup/06_storage_policies.sql`](../manifests/stardata/setup/06_storage_policies.sql) | Creates `sd_s3_cold` resource and `sd_cold_1d` policy |
| [`manifests/stardata/setup/07_auto_partition_snap_id.sql`](../manifests/stardata/setup/07_auto_partition_snap_id.sql) | Documents automation behaviour and opt-out |
| [`manifests/stardata/stardata-fe-deployment.yaml`](../manifests/stardata/stardata-fe-deployment.yaml) | FE StatefulSet manifest — image tag |
| [`argocd-apps/app-stardata.yaml`](../argocd-apps/app-stardata.yaml) | ArgoCD application — selfHeal=true, path=manifests/stardata |
