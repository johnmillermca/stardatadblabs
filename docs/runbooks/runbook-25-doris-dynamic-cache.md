# Runbook 25 — Doris Dynamic Segment Cache Manager

| Field | Value |
|---|---|
| **Runbook ID** | RB-25 |
| **Service** | k8s-platform / doris-cache-manager |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-09-04 |
| **Related** | RB-05 (Doris & Analytics), RB-11 (Kerberos), RB-13 (RBAC) |

---

## 1. Purpose

This runbook documents the **Doris Dynamic Segment Cache Manager** — a daemon that
automatically warms and evicts Doris segment caches for Iceberg external tables
based on observed query patterns.

```
Iceberg tables (Polaris REST)
  polaris     → IcebergCatalog
  databricks  → star_lakehouse
  postgres    → pg_lakehouse
  oracle      → ora_lakehouse
  mongodb     → mgo_lakehouse
          │
          ▼
   Apache Doris (FE + BE)
     External catalogs
          │
          ▼
   doris_cache_manager.py  (this daemon)
     • Reads Doris audit_log every hour
     • Warms up hot tables: interval = select_interval × ⅔
     • Evicts cold tables: idle ≥ 24 h  →  COLD_DOWN
     • Persists state in platform_meta (Doris internal tables)
```

---

## 1.1 Write Pushdown — Overview

Doris external Iceberg catalogs are **read-only** at the storage layer.  Any
`INSERT`, `UPDATE`, `DELETE`, or `MERGE` issued in Doris against an external
catalog table fails at execution.  The cache manager intercepts those DML
statements from the audit log and re-submits them as Spark jobs so the actual
write is executed by Spark — which has full Iceberg read/write access via the
Polaris REST catalog.

```
Developer / BI tool
     │
     │  INSERT INTO polaris.tpcds_sf10tcl.store_sales …
     ▼
  Apache Doris FE  ──► ERR: external catalog not writable
     │ (audit_log)
     ▼
  Cache Manager (WriteInterceptor)
     │  reads audit_log every hour
     │  detects INSERT/UPDATE/DELETE/MERGE against managed catalogs
     ▼
  Spark REST API  (spark-master-svc.prod:6066)
     │  POST /v1/submissions/create
     │  appResource: /app/spark_iceberg_write.py
     │  appArgs: { catalog, warehouse, db, table, stmt }
     ▼
  Spark Worker  ──► spark.sql(stmt) inside catalog.<db>
     │  credentials loaded fresh from OpenBao (K8s SA JWT)
     ▼
  Iceberg table on S3 via Polaris REST  ✓ written successfully
```

---

## 2. Architecture

### 2.1 Components

| File | Purpose |
|---|---|
| `manifests/doris/setup/01_drop_catalogs.sql` | Drop all existing Doris external catalogs |
| `manifests/doris/setup/02_create_catalogs.sql` | Create 5 Iceberg catalogs (envsubst for credentials) |
| `manifests/doris/setup/03_create_metadata_tables.sql` | Create `platform_meta` database and tracking tables |
| `manifests/doris/cache_manager/doris_cache_manager.py` | Python daemon — monitoring, warm-up, LRU eviction |
| `manifests/doris/cache_manager/Dockerfile` | Container image build |
| `manifests/doris/cache_manager/spark_iceberg_write.py` | PySpark job executed on Spark workers for write pushdown |
| `manifests/doris/cache_manager/doris-cache-manager-deployment.yaml` | Kubernetes Deployment in `prod` namespace |

### 2.2 Credentials

All credentials are read from OpenBao at runtime — nothing is hard-coded.

| OpenBao Path | Keys Used |
|---|---|
| `secret/data/platform/doris` | `admin_password` |
| `secret/data/platform/polaris` | `spark_svc_id`, `spark_svc_secret` |

The daemon authenticates via the `doris-cache-manager` ServiceAccount JWT bound to
the **`doris-cache-manager`** OpenBao K8s auth role (policy: `platform-secrets-read`).

> **Note:** The Doris root password is also injected as `DORIS_ADMIN_PASSWORD` from
> the `rbac-plane-credentials` K8s secret. This is a startup fallback only — if
> OpenBao JWT auth fails transiently (e.g. role just created), the daemon still
> connects to Doris using the env var and continues normally.

### 2.3 Metadata Tables

Created in `platform_meta` (Doris internal database):

| Table | Purpose |
|---|---|
| `platform_meta.table_query_stats` | Per-table SELECT count, timing, warm state |
| `platform_meta.cache_eviction_log` | Audit log of every LRU eviction |

### 2.4 Warm-Up Scheduling Logic

```
select_interval  = minutes between the last two observed SELECTs on a table
warm_interval    = select_interval × 2/3

Example:
  Table queried every 2 hours (120 min)
  warm_interval = 120 × 2/3 = 80 min
  → WARM_UP issued every 80 minutes
```

Rules:
- Warm-up only triggers when `total_select_count > 1` (at least 2 queries seen).
- If a warm-up job is already running for a table → skip.
- If a warm-up has been running for **> 5 minutes** (stale) → skip and retry next cycle.
- Maximum **32 concurrent** warm-up jobs at any time.

### 2.5 Write-Pushdown Logic

| Step | Detail |
|---|---|
| **Detection** | Each daemon cycle scans `__internal_schema.audit_log` for `INSERT`, `UPDATE`, `DELETE`, `MERGE` statements where `catalog IN (managed_catalogs)` |
| **Deduplication** | Statements are tracked by `query_id` — identical query not submitted twice within the lookback window |
| **Spark submission** | Calls `POST /v1/submissions/create` on the Spark standalone REST API |
| **Credentials** | The submitted `spark_iceberg_write.py` job reads credentials fresh from OpenBao at runtime — never passed in the submission payload |
| **Retry** | If the Spark REST call fails, the query_id is **not** marked as submitted — it will be retried on the next cycle |
| **Supported DML** | `INSERT INTO`, `INSERT OVERWRITE`, `UPDATE … SET`, `DELETE FROM`, `DELETE`, `MERGE INTO … USING` |

### 2.6 LRU Eviction Logic

- After every scan cycle, any table with `last_select_ts` older than **24 hours** that
  is currently in state `WARM`, `WARMING`, or `UNKNOWN` receives a `COLD_DOWN`.
- The eviction is recorded in `platform_meta.cache_eviction_log` with reason
  `no_select_24h`.

---

## 3. Initial Setup

### 3.1 Prerequisites

- Doris FE and BE are running (`SHOW BACKENDS;` returns at least one alive backend).
- Polaris REST catalog is reachable at `http://polaris-rest.prod.svc.cluster.local:8181`.
- OpenBao has `secret/data/platform/doris` and `secret/data/platform/polaris` populated.
- The `doris-cache-manager` OpenBao K8s auth role exists (see §3.0 below).
- The `platform_meta` database and tracking tables exist in Doris (see §3.6).

### 3.0 One-Time OpenBao K8s Auth Role Setup

The daemon uses a dedicated K8s auth role. This only needs to be done **once**
(already done on this cluster — included for reproductions / disaster recovery).

```bash
# Get the OpenBao root token
kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d

# Create the role (substitute your root token)
kubectl exec -n prod openbao-0 -- \
  env BAO_TOKEN=<root-token> \
  bao write auth/kubernetes/role/doris-cache-manager \
    bound_service_account_names=doris-cache-manager \
    bound_service_account_namespaces=prod \
    policies=platform-secrets-read \
    ttl=1h

# Verify
kubectl exec -n prod openbao-0 -- \
  env BAO_TOKEN=<root-token> \
  bao read auth/kubernetes/role/doris-cache-manager
# bound_service_account_names: [doris-cache-manager]
# policies: [platform-secrets-read]
```

Also populate `secret/data/platform/doris` in OpenBao:

```bash
# The Doris root password lives in the rbac-plane-credentials k8s secret
DORIS_PASS=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.DORIS_ADMIN_PASSWORD}' | base64 -d)

kubectl exec -n prod openbao-0 -- \
  env BAO_TOKEN=<root-token> \
  bao kv put -mount=secret platform/doris admin_password="${DORIS_PASS}"

# Verify
kubectl exec -n prod openbao-0 -- \
  env BAO_TOKEN=<root-token> \
  bao kv get -mount=secret platform/doris
```

### 3.2 Get Doris Admin Password

```bash
# From outside the cluster
DORIS_PASS=$(curl -s \
  -H "X-Vault-Token: ${TOKEN}" \
  http://192.168.1.50:30820/v1/secret/data/platform/doris \
  | jq -r '.data.data.admin_password')

# Or from OpenBao CLI
bao kv get -field=admin_password secret/platform/doris
```

### 3.3 Get Polaris Credentials

```bash
POLARIS_ID=$(curl -s \
  -H "X-Vault-Token: ${TOKEN}" \
  http://192.168.1.50:30820/v1/secret/data/platform/polaris \
  | jq -r '.data.data.spark_svc_id')

POLARIS_SECRET=$(curl -s \
  -H "X-Vault-Token: ${TOKEN}" \
  http://192.168.1.50:30820/v1/secret/data/platform/polaris \
  | jq -r '.data.data.spark_svc_secret')
```

### 3.4 Drop Existing Catalogs

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
  < manifests/doris/setup/01_drop_catalogs.sql
```

Expected output: `SHOW CATALOGS` returns only `internal` and `hive_metastore`.

### 3.5 Create Iceberg Catalogs

`02_create_catalogs.sql` uses shell variable substitution (`${POLARIS_ID}`,
`${POLARIS_SECRET}`). Use `envsubst` to inject them before executing:

```bash
export POLARIS_ID POLARIS_SECRET DORIS_PASS

envsubst < manifests/doris/setup/02_create_catalogs.sql | \
  mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}"
```

Verify:

```sql
SHOW CATALOGS;
-- Expected: internal, hive_metastore, polaris, databricks, postgres, oracle, mongodb

SWITCH polaris;
SHOW DATABASES;
```

### 3.6 Create Metadata Tables

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
  < manifests/doris/setup/03_create_metadata_tables.sql
```

Verify:

```sql
SHOW TABLES FROM platform_meta;
-- Expected: cache_eviction_log, table_query_stats
```

### 3.7 Build the Cache Manager Container Image

> ⚠️ **`docker` is not available on this cluster — use `podman` instead.**
> The Dockerfile base image is `python:3.12-slim` pulled from docker.io
> (not from the local registry — `python:3.12-slim` is not mirrored locally).

```bash
cd manifests/doris/cache_manager

podman build --platform linux/amd64 \
  -t 192.168.1.50:30500/doris-cache-manager:1.0.0 .

podman push --tls-verify=false \
  192.168.1.50:30500/doris-cache-manager:1.0.0
```

Verify the image landed in the registry:

```bash
curl -sk https://192.168.1.50:30500/v2/doris-cache-manager/tags/list
# {"name":"doris-cache-manager","tags":["1.0.0"]}
```

### 3.8 Deploy the Cache Manager

> ⚠️ **Do not add `restartPolicy` to individual containers in a Deployment.**
> `restartPolicy` is only valid at the Pod spec level (`Always` is the Deployment
> default). Adding it at the container level causes:
> `spec.template.spec.containers[0].restartPolicy: Forbidden`

```bash
kubectl apply -f manifests/doris/cache_manager/doris-cache-manager-deployment.yaml

kubectl rollout status deployment/doris-cache-manager -n prod --timeout=120s
```

---

## 4. Verify the Deployment

```bash
# Check pod is running
kubectl get pod -n prod -l app=doris-cache-manager
# NAME                                   READY   STATUS    RESTARTS   AGE
# doris-cache-manager-8476bf5569-xxxx    1/1     Running   0          30s

# Watch logs — first cycle runs immediately on startup
kubectl logs -n prod -l app=doris-cache-manager -f
```

Expected healthy startup log (exact lines):

```
Doris Cache Manager starting up.
Loading credentials from OpenBao (http://openbao.prod.svc.cluster.local:8200).
Authenticated to OpenBao via K8s SA JWT (role=doris-cache-manager).
Doris credentials loaded from OpenBao.
Polaris credentials loaded from OpenBao.
Credentials loaded.
Cache Manager daemon running. scan_interval=3600s lru_evict=24h max_concurrent=32 warmup_stale=5min
=== Cache Manager cycle start: 2026-09-04T04:01:59... ===
Connected to Doris FE at doris-fe.prod.svc.cluster.local:9030.
Audit log scrape: found 0 distinct table/catalog pairs with SELECTs.
Warm-up evaluation: 0 eligible tables, 0 triggered.
=== Cycle done. active_warmups=0 ===
Sleeping 3600 seconds until next scan.
```

> `found 0 ...` is normal on first startup — the audit log is empty until
> queries are executed against managed catalogs.

Check metadata tables from Doris Web UI (`http://192.168.1.50:30030`) or MySQL:

```sql
-- Web UI: run each statement separately (USE doesn't persist between statements)
SHOW TABLES FROM platform_meta;
-- Expected: cache_eviction_log, table_query_stats

SELECT catalog_name, db_name, table_name, total_select_count,
       last_select_ts, select_interval_min, warm_interval_min, cache_state
FROM platform_meta.table_query_stats
ORDER BY total_select_count DESC
LIMIT 20;

SELECT * FROM platform_meta.cache_eviction_log ORDER BY evicted_at DESC LIMIT 10;
```

Trigger cache population by running a query against a managed catalog:

```sql
-- This will appear in table_query_stats after the next hourly cycle
SELECT COUNT(*) FROM iceberg_polaris.tpcds_sf10tcl.customer;
```

---

## 5. Operations

### 5.1 Manually Trigger a Warm-Up

```sql
-- Connect to Doris
WARM UP CACHE
  ON TABLE polaris.tpcds_sf10tcl.store_sales
  USING JOB;

-- Check job status
SHOW WARM UP JOB WHERE TableName = 'store_sales';
```

### 5.2 Manually Evict a Table's Cache

```sql
WARM UP CACHE
  ON TABLE polaris.tpcds_sf10tcl.store_sales
  USING COLD_DOWN;
```

### 5.3 Check Current Cache States

```sql
SELECT
    catalog_name,
    db_name,
    table_name,
    cache_state,
    total_select_count,
    last_select_ts,
    warm_interval_min,
    last_warmed_ts
FROM platform_meta.table_query_stats
ORDER BY last_select_ts DESC;
```

### 5.4 Large-Dataset SELECT Samples

These queries exercise all five catalogs at scale and will populate
`platform_meta.table_query_stats` — triggering warm-up scheduling after the
second cycle.  Run them from a MySQL client connected to Doris on port `30090`.

#### 5.4.1 polaris — TPC-DS SF10TCL (store_sales: ~2.9 billion rows)

```sql
-- Aggregate daily revenue across all stores — scans full store_sales fact table
SELECT
    ss.ss_sold_date_sk,
    d.d_date,
    SUM(ss.ss_net_paid)        AS total_net_paid,
    SUM(ss.ss_net_profit)      AS total_net_profit,
    COUNT(*)                   AS transaction_count
FROM polaris.tpcds_sf10tcl.store_sales   ss
JOIN polaris.tpcds_sf10tcl.date_dim      d  ON ss.ss_sold_date_sk = d.d_date_sk
WHERE d.d_year = 2003
GROUP BY ss.ss_sold_date_sk, d.d_date
ORDER BY d.d_date
LIMIT 500;

-- Top 100 customers by lifetime spend — joins store_sales + customer (200M+ row join)
SELECT
    c.c_customer_id,
    c.c_first_name,
    c.c_last_name,
    SUM(ss.ss_net_paid)  AS lifetime_spend,
    COUNT(*)             AS purchase_count
FROM polaris.tpcds_sf10tcl.store_sales  ss
JOIN polaris.tpcds_sf10tcl.customer     c  ON ss.ss_customer_sk = c.c_customer_sk
GROUP BY c.c_customer_id, c.c_first_name, c.c_last_name
ORDER BY lifetime_spend DESC
LIMIT 100;

-- Cross-channel analysis: store + web + catalog sales rolled up by item
SELECT
    i.i_item_id,
    i.i_item_desc,
    SUM(ss.ss_quantity)   AS store_qty,
    SUM(ws.ws_quantity)   AS web_qty,
    SUM(cs.cs_quantity)   AS catalog_qty,
    SUM(ss.ss_net_paid + COALESCE(ws.ws_net_paid, 0) + COALESCE(cs.cs_net_paid, 0))
                          AS total_revenue
FROM polaris.tpcds_sf10tcl.store_sales      ss
JOIN polaris.tpcds_sf10tcl.item             i   ON ss.ss_item_sk     = i.i_item_sk
LEFT JOIN polaris.tpcds_sf10tcl.web_sales   ws  ON ws.ws_item_sk     = i.i_item_sk
LEFT JOIN polaris.tpcds_sf10tcl.catalog_sales cs ON cs.cs_item_sk   = i.i_item_sk
GROUP BY i.i_item_id, i.i_item_desc
HAVING total_revenue > 1000000
ORDER BY total_revenue DESC
LIMIT 200;

-- Return rate per store — scans store_sales and store_returns (~400M rows each)
SELECT
    s.s_store_id,
    s.s_store_name,
    COUNT(ss.ss_ticket_number)            AS total_sales,
    COUNT(sr.sr_ticket_number)            AS total_returns,
    ROUND(COUNT(sr.sr_ticket_number) * 100.0
          / NULLIF(COUNT(ss.ss_ticket_number), 0), 2)  AS return_rate_pct
FROM polaris.tpcds_sf10tcl.store_sales    ss
JOIN polaris.tpcds_sf10tcl.store          s   ON ss.ss_store_sk = s.s_store_sk
LEFT JOIN polaris.tpcds_sf10tcl.store_returns sr
    ON ss.ss_ticket_number = sr.sr_ticket_number
    AND ss.ss_item_sk      = sr.sr_item_sk
GROUP BY s.s_store_id, s.s_store_name
ORDER BY return_rate_pct DESC
LIMIT 50;
```

#### 5.4.2 databricks — star_lakehouse (multi-table analytical query)

```sql
-- Monthly revenue trend — full scan of the star_lakehouse sales fact table
SELECT
    DATE_FORMAT(order_date, '%Y-%m')  AS month,
    region,
    SUM(revenue)                       AS monthly_revenue,
    SUM(units_sold)                    AS units_sold,
    AVG(revenue / NULLIF(units_sold, 0)) AS avg_unit_price
FROM databricks.star_lakehouse_db.sales_fact
WHERE order_date >= DATE_SUB(CURRENT_DATE, INTERVAL 24 MONTH)
GROUP BY DATE_FORMAT(order_date, '%Y-%m'), region
ORDER BY month DESC, monthly_revenue DESC
LIMIT 300;

-- Customer segmentation — RFM scoring
SELECT
    customer_id,
    MAX(order_date)                      AS last_order_date,
    COUNT(DISTINCT order_id)             AS order_frequency,
    SUM(revenue)                         AS monetary_value,
    DATEDIFF(CURRENT_DATE, MAX(order_date)) AS recency_days,
    NTILE(5) OVER (ORDER BY SUM(revenue) DESC) AS monetary_quintile
FROM databricks.star_lakehouse_db.sales_fact
GROUP BY customer_id
HAVING order_frequency >= 2
ORDER BY monetary_value DESC
LIMIT 1000;
```

#### 5.4.3 postgres — pg_lakehouse (operational data large scan)

```sql
-- Activity summary with rolling 90-day window
SELECT
    account_id,
    event_type,
    COUNT(*)                              AS event_count,
    MIN(created_at)                       AS first_event,
    MAX(created_at)                       AS last_event,
    SUM(amount)                           AS total_amount
FROM postgres.public.events
WHERE created_at >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 90 DAY)
GROUP BY account_id, event_type
HAVING COUNT(*) > 10
ORDER BY total_amount DESC
LIMIT 500;

-- Join events with account profile for churn risk scoring
SELECT
    a.account_id,
    a.plan_tier,
    COUNT(e.event_id)                     AS event_count_90d,
    SUM(CASE WHEN e.event_type = 'login' THEN 1 ELSE 0 END) AS logins,
    SUM(CASE WHEN e.event_type = 'error' THEN 1 ELSE 0 END) AS errors,
    MAX(e.created_at)                     AS last_activity
FROM postgres.public.accounts         a
LEFT JOIN postgres.public.events      e
    ON a.account_id = e.account_id
    AND e.created_at >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 90 DAY)
GROUP BY a.account_id, a.plan_tier
ORDER BY logins ASC, errors DESC
LIMIT 1000;
```

#### 5.4.4 oracle — ora_lakehouse (ERP / finance data)

```sql
-- General ledger balance by cost center for the current fiscal year
SELECT
    gl.cost_center_id,
    cc.cost_center_name,
    gl.account_code,
    gl.fiscal_period,
    SUM(gl.debit_amount)  AS total_debits,
    SUM(gl.credit_amount) AS total_credits,
    SUM(gl.debit_amount - gl.credit_amount) AS net_balance
FROM oracle.finance.general_ledger     gl
JOIN oracle.finance.cost_centers       cc  ON gl.cost_center_id = cc.cost_center_id
WHERE gl.fiscal_year = YEAR(CURRENT_DATE)
GROUP BY gl.cost_center_id, cc.cost_center_name, gl.account_code, gl.fiscal_period
ORDER BY gl.fiscal_period, net_balance DESC
LIMIT 500;

-- Accounts payable aging — outstanding invoices by vendor
SELECT
    v.vendor_id,
    v.vendor_name,
    SUM(CASE WHEN DATEDIFF(CURRENT_DATE, i.due_date) <= 30  THEN i.outstanding_amount ELSE 0 END) AS current_0_30,
    SUM(CASE WHEN DATEDIFF(CURRENT_DATE, i.due_date) BETWEEN 31 AND 60 THEN i.outstanding_amount ELSE 0 END) AS overdue_31_60,
    SUM(CASE WHEN DATEDIFF(CURRENT_DATE, i.due_date) BETWEEN 61 AND 90 THEN i.outstanding_amount ELSE 0 END) AS overdue_61_90,
    SUM(CASE WHEN DATEDIFF(CURRENT_DATE, i.due_date) > 90   THEN i.outstanding_amount ELSE 0 END) AS overdue_90_plus,
    SUM(i.outstanding_amount) AS total_outstanding
FROM oracle.finance.invoices   i
JOIN oracle.finance.vendors    v ON i.vendor_id = v.vendor_id
WHERE i.status = 'UNPAID'
GROUP BY v.vendor_id, v.vendor_name
ORDER BY total_outstanding DESC
LIMIT 200;
```

#### 5.4.5 mongodb — mgo_lakehouse (document / event data)

```sql
-- User engagement funnel — counts per funnel stage
SELECT
    funnel_stage,
    platform,
    DATE_FORMAT(event_ts, '%Y-%m-%d') AS event_date,
    COUNT(DISTINCT session_id)         AS unique_sessions,
    COUNT(*)                           AS total_events,
    AVG(duration_ms)                   AS avg_duration_ms
FROM mongodb.analytics.user_events
WHERE event_ts >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY)
GROUP BY funnel_stage, platform, DATE_FORMAT(event_ts, '%Y-%m-%d')
ORDER BY event_date DESC, funnel_stage
LIMIT 500;

-- Product recommendation signal — co-purchase pairs
SELECT
    a.product_id  AS product_a,
    b.product_id  AS product_b,
    COUNT(*)      AS co_purchase_count
FROM mongodb.ecommerce.order_items a
JOIN mongodb.ecommerce.order_items b
    ON a.order_id = b.order_id AND a.product_id < b.product_id
GROUP BY a.product_id, b.product_id
HAVING co_purchase_count >= 50
ORDER BY co_purchase_count DESC
LIMIT 300;
```

---

### 5.5 Write Pushdown — Operations

#### Submit a write from Doris (auto-intercepted)

When a developer issues a DML against an external catalog from any MySQL client
connected to Doris, the write will fail locally but the cache manager will
automatically intercept and push it to Spark within the next scan cycle (≤ 1 h).

```sql
-- Example: append new rows to a polaris Iceberg table via Doris
-- (Doris will return an error, but the daemon will push it to Spark)
INSERT INTO polaris.tpcds_sf10tcl.store_sales
SELECT * FROM internal.analytics.daily_store_feed;

-- Example: update with pushdown
UPDATE polaris.tpcds_sf10tcl.customer
SET    c_email_address = 'updated@example.com'
WHERE  c_customer_sk = 12345;

-- Example: MERGE (upsert) — pushed to Spark for full Iceberg MERGE execution
MERGE INTO databricks.star_lakehouse_db.sales_fact AS target
USING (
    SELECT order_id, customer_id, revenue, order_date
    FROM internal.staging.sales_delta
) AS src ON target.order_id = src.order_id
WHEN MATCHED THEN UPDATE SET
    target.revenue    = src.revenue,
    target.order_date = src.order_date
WHEN NOT MATCHED THEN INSERT (order_id, customer_id, revenue, order_date)
    VALUES (src.order_id, src.customer_id, src.revenue, src.order_date);
```

#### Check which writes have been pushed to Spark

```bash
# View intercepted write jobs in daemon logs
kubectl logs -n prod -l app=doris-cache-manager \
  | grep "WriteInterceptor"

# Expected output:
#  WriteInterceptor: 1 new DML write(s) detected against external catalogs.
#  WriteInterceptor: pushed polaris.tpcds_sf10tcl.store_sales (qid=abc123) → Spark submissionId=driver-20250601...
```

#### Check Spark job status for a submitted write

```bash
# Via Spark REST API
curl -s http://192.168.1.50:6066/v1/submissions/status/<submissionId> \
  | jq '{state: .driverState, workerHostPort: .workerHostPort}'

# Via Spark UI
open http://192.168.1.50:8080
```

#### Manually trigger write pushdown (bypass daemon delay)

If you cannot wait for the next daemon cycle, submit directly to Spark:

```bash
# Build the job args JSON
JOB_ARGS='{"catalog":"polaris","warehouse":"IcebergCatalog","db":"tpcds_sf10tcl","table":"store_sales","stmt":"INSERT INTO polaris.tpcds_sf10tcl.store_sales SELECT * FROM internal.analytics.daily_store_feed"}'

# Submit via Spark REST
curl -s -X POST http://192.168.1.50:6066/v1/submissions/create \
  -H "Content-Type: application/json" \
  -d "{
    \"action\": \"CreateSubmissionRequest\",
    \"appResource\": \"/app/spark_iceberg_write.py\",
    \"mainClass\": \"\",
    \"appArgs\": [\"${JOB_ARGS}\"],
    \"sparkProperties\": {
      \"spark.app.name\": \"manual-write-pushdown\",
      \"spark.master\": \"spark://spark-master-internal.prod.svc.cluster.local:17077\",
      \"spark.submit.deployMode\": \"cluster\"
    },
    \"clientSparkVersion\": \"3.5.1\"
  }"
```

### 5.6 Reload Credentials (If Rotated)

The daemon caches OpenBao credentials in memory. After a secret rotation:

```bash
kubectl rollout restart deployment/doris-cache-manager -n prod
```

The new pod will re-authenticate and reload credentials on startup.

### 5.7 Scale Down / Pause the Daemon

```bash
kubectl scale deployment/doris-cache-manager -n prod --replicas=0
```

Resume:

```bash
kubectl scale deployment/doris-cache-manager -n prod --replicas=1
```

### 5.8 Update the Daemon Image

```bash
cd manifests/doris/cache_manager
docker build -t 192.168.1.50:30500/doris-cache-manager:1.0.1 .
docker push 192.168.1.50:30500/doris-cache-manager:1.0.1

# Update the image tag in the deployment manifest, then:
kubectl apply -f manifests/doris/cache_manager/doris-cache-manager-deployment.yaml
kubectl rollout status deployment/doris-cache-manager -n prod
```

---

## 6. Tuning

The following environment variables control daemon behaviour. Edit
`doris-cache-manager-deployment.yaml` and re-apply to change them.

| Variable | Default | Description |
|---|---|---|
| `SCAN_INTERVAL_S` | `3600` | Seconds between audit log scans (1 hour) |
| `LRU_EVICT_HOURS` | `24` | Inactivity hours before COLD_DOWN |
| `MAX_CONCURRENT` | `32` | Max simultaneous WARM_UP jobs |
| `WARMUP_STALE_MIN` | `5` | Minutes before a running warm-up is considered stale |
| `DORIS_HOST` | `doris-fe.prod.svc.cluster.local` | Doris FE host |
| `DORIS_PORT` | `9030` | Doris FE MySQL port (direct, not via krb-guard) |
| `BAO_ROLE` | `platform-secrets-read` | OpenBao Kubernetes auth role |
| `ADDR` | `http://openbao.prod.svc.cluster.local:8200` | OpenBao address |
| `SPARK_REST_URL` | `http://spark-master-svc.prod.svc.cluster.local:6066` | Spark REST submission endpoint for write pushdown |
| `SPARK_MASTER_URL` | `spark://spark-master-internal.prod.svc.cluster.local:17077` | Spark master URL passed to submitted write jobs |

---

## 7. Troubleshooting

### 7.1 OpenBao Authentication Fails — `HTTP Error 400: Bad Request`

```
urllib.error.HTTPError: HTTP Error 400: Bad Request
```

This is the most common startup failure. It means the K8s JWT was presented to
OpenBao but the `auth/kubernetes/role/<role>` doesn't exist **or** the SA name
isn't in `bound_service_account_names`.

**Root cause encountered during initial deploy:**
The deployment originally specified `BAO_ROLE=platform-secrets-read`. That value
is an OpenBao **policy** name, not a **role** name. Only two K8s roles existed:
`polaris-token-refresher` and `rbac-plane`. A new role `doris-cache-manager` was
created and bound to the `platform-secrets-read` policy.

**Diagnose:**

```bash
# List all registered K8s auth roles
kubectl exec -n prod openbao-0 -- \
  env BAO_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
    -o jsonpath='{.data.root-token}' | base64 -d) \
  bao list auth/kubernetes/role
# Must include: doris-cache-manager

# Check the role details
kubectl exec -n prod openbao-0 -- \
  env BAO_TOKEN=<root-token> \
  bao read auth/kubernetes/role/doris-cache-manager
# bound_service_account_names: [doris-cache-manager]
# bound_service_account_namespaces: [prod]
# policies: [platform-secrets-read]
```

**Fix:** If the role is missing, re-run the setup in §3.0.

### 7.1a OpenBao Secret `secret/data/platform/doris` Empty

```
KeyError: 'admin_password'
```

The daemon reads `admin_password` from `secret/data/platform/doris`. This path
must be populated before deploying.

**Check:**

```bash
kubectl exec -n prod openbao-0 -- \
  env BAO_TOKEN=<root-token> \
  bao kv get -mount=secret platform/doris
# Must show: admin_password    <value>
```

**Fix:** See §3.0 — the Doris root password is in `rbac-plane-credentials` K8s
secret under key `DORIS_ADMIN_PASSWORD`.

### 7.2 Doris Connection Fails

```
Can't connect to MySQL server on 'doris-fe.prod.svc.cluster.local'
```

**Check:** Doris FE is alive and the Service exists in the `prod` namespace.

```bash
kubectl get svc doris-fe -n prod
kubectl get pod -n prod -l app=doris-fe

# Test connectivity from a debug pod
kubectl run -it --rm debug --image=192.168.1.50:30500/busybox:1.36 \
  -n prod -- sh -c "nc -zv doris-fe.prod.svc.cluster.local 9030"
```

### 7.3 Audit Log Scrape Returns 0 Tables

Doris `__internal_schema.audit_log` may not be populated if:
- `enable_audit_plugin = false` in `fe.conf` (check FE config).
- No queries have been run against managed catalogs yet.
- The daemon is querying a lookback window with no activity.

**Check:**

```sql
SELECT COUNT(*) FROM __internal_schema.audit_log
WHERE time >= DATE_SUB(NOW(), INTERVAL 2 HOUR);

-- If 0 rows, run a test query against an Iceberg catalog:
SELECT * FROM polaris.tpcds_sf10tcl.store_sales LIMIT 1;

-- Wait for next cycle (up to 1 hour) and check again.
```

### 7.4 WARM_UP Job Stuck

If a WARM_UP job runs longer than 5 minutes (`WARMUP_STALE_MIN`), the daemon
skips it and retries next cycle. Check Doris job status:

```sql
SHOW WARM UP JOB;
-- Look for jobs in RUNNING state with old start times.

-- Cancelling is not currently supported; wait for the job to finish
-- or restart the BE pod to clear stuck jobs.
```

### 7.5 Catalog Query Fails After Re-creation

```sql
-- If a catalog shows SHOW CATALOGS but queries fail:
REFRESH CATALOG polaris;
REFRESH CATALOG databricks;
-- ... etc.
```

Polaris OAuth2 tokens expire. The catalog will re-authenticate automatically on
the next query, but an explicit REFRESH forces immediate token renewal.

### 7.6 Liveness Probe Fails

The liveness probe checks `/tmp/cache_manager_alive` (written each cycle).
It fails if the file is missing or older than 2 hours.

```bash
kubectl describe pod -n prod -l app=doris-cache-manager
# Look for: Liveness probe failed

kubectl exec -n prod deployment/doris-cache-manager -- ls -la /tmp/cache_manager_alive
```

If the pod is stuck in a cycle (no heartbeat), check logs for the specific error
and restart:

```bash
kubectl rollout restart deployment/doris-cache-manager -n prod
```

### 7.7 Write Pushdown Not Triggering

```
WriteInterceptor: audit log scan failed: Table '__internal_schema.audit_log' not found.
```

Means the Doris audit log table is unavailable. Check section 7.3.

**Check the write DML reached Doris:**

```bash
kubectl logs -n prod -l app=doris-cache-manager | grep -E "WriteInterceptor|DML"
```

**Verify the Spark REST endpoint is reachable from the daemon pod:**

```bash
kubectl exec -n prod deployment/doris-cache-manager -- \
  sh -c "wget -qO- http://spark-master-svc.prod.svc.cluster.local:6066/v1/submissions/status 2>&1 | head -5"
```

**Spark job failed after submission:**

```bash
# Get the submissionId from daemon logs, then:
curl -s http://192.168.1.50:6066/v1/submissions/status/<submissionId> | jq .driverState

# If FAILED — check Spark worker logs:
kubectl logs -n prod -l app=spark-worker | tail -100
```

---

## 9. Known Issues & Deployment History

### 9.1 Initial Deployment Issues (2026-09-04)

The following issues were encountered and resolved during the first live deployment.
Documented here so future re-deployments avoid the same pitfalls.

---

#### Issue 1 — `platform_meta` database did not exist

**Symptom:** `SHOW DATABASES FROM internal` did not include `platform_meta`.

**Cause:** The SQL in `03_create_metadata_tables.sql` had not been applied to the cluster.

**Fix:** Applied manually via `kubectl exec` into the `doris-fe-0` pod:

```bash
kubectl exec -n prod doris-fe-0 -c doris-fe -- \
  mysql -h 127.0.0.1 -P 9030 -u root --skip-password \
  -e "$(cat manifests/doris/setup/03_create_metadata_tables.sql)"
```

> Going forward use the `mysql` approach from §3.6 directly.

---

#### Issue 2 — `python:3.12-slim` not in local registry

**Symptom:**

```
Error: creating build container: unable to copy from source
  docker://192.168.1.50:30500/python:3.12-slim: manifest unknown
```

**Cause:** The Dockerfile originally used `FROM 192.168.1.50:30500/python:3.12-slim`.
That image does not exist in the local registry. The cluster registry only mirrors
application images — Python base images are not mirrored.

**Fix:** Changed `FROM` to use docker.io directly:

```dockerfile
FROM python:3.12-slim
```

Docker Hub is reachable from the cluster nodes. Also, `docker` is not installed —
use `podman` for all image builds.

---

#### Issue 3 — `restartPolicy: Always` on a container (not pod)

**Symptom:**

```
The Deployment "doris-cache-manager" is invalid:
spec.template.spec.containers[0].restartPolicy:
Forbidden: may not be set for non-init containers
```

**Cause:** The original manifest had `restartPolicy: Always` nested inside the
container spec block. This field is only valid at the Pod spec level and is
already the default for Deployments.

**Fix:** Removed the `restartPolicy` field from the container block entirely.

---

#### Issue 4 — OpenBao `HTTP Error 400` on K8s JWT login

**Symptom:**

```
2026-09-04T03:55:48 INFO  Loading credentials from OpenBao...
urllib.error.HTTPError: HTTP Error 400: Bad Request
```

**Cause:** `BAO_ROLE=platform-secrets-read` was set in the deployment env vars.
`platform-secrets-read` is a **policy** name, not a **K8s auth role** name. The
only registered K8s auth roles were `polaris-token-refresher` and `rbac-plane` —
there was no role for the `doris-cache-manager` SA.

**Fix (two-part):**

1. Created a new K8s auth role in OpenBao:

```bash
kubectl exec -n prod openbao-0 -- \
  env BAO_TOKEN=<root-token> \
  bao write auth/kubernetes/role/doris-cache-manager \
    bound_service_account_names=doris-cache-manager \
    bound_service_account_namespaces=prod \
    policies=platform-secrets-read \
    ttl=1h
```

2. Updated deployment: `BAO_ROLE=doris-cache-manager`

3. Added `DORIS_ADMIN_PASSWORD` env var from `rbac-plane-credentials` K8s secret
   as a startup fallback so the daemon is resilient to transient OpenBao failures.

4. Updated `doris_cache_manager.py` — wrapped `read_secret()` calls in try/except:
   - If OpenBao unavailable: falls back to `DORIS_ADMIN_PASSWORD` env var
   - If Polaris creds unavailable: logs warning, disables write-pushdown (no crash)

---

#### Issue 5 — `secret/data/platform/doris` path was empty

**Symptom:** `[]` returned when reading the secret keys from OpenBao.

**Cause:** The `secret/data/platform/doris` KV path had never been written.
The daemon needed `admin_password` from this path.

**Fix:** Wrote the secret using the root token:

```bash
DORIS_PASS=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.DORIS_ADMIN_PASSWORD}' | base64 -d)

kubectl exec -n prod openbao-0 -- \
  env BAO_TOKEN=<root-token> \
  bao kv put -mount=secret platform/doris \
  admin_password="${DORIS_PASS}"
```

---

#### Final successful startup log

```
2026-09-04T04:01:59 INFO  Doris Cache Manager starting up.
2026-09-04T04:01:59 INFO  Loading credentials from OpenBao (http://openbao.prod.svc.cluster.local:8200).
2026-09-04T04:01:59 INFO  Authenticated to OpenBao via K8s SA JWT (role=doris-cache-manager).
2026-09-04T04:01:59 INFO  Doris credentials loaded from OpenBao.
2026-09-04T04:01:59 INFO  Polaris credentials loaded from OpenBao.
2026-09-04T04:01:59 INFO  Credentials loaded.
2026-09-04T04:01:59 INFO  Cache Manager daemon running. scan_interval=3600s lru_evict=24h max_concurrent=32 warmup_stale=5min
2026-09-04T04:01:59 INFO  === Cache Manager cycle start: 2026-09-04T04:01:59.257711+00:00 ===
2026-09-04T04:01:59 INFO  Connected to Doris FE at doris-fe.prod.svc.cluster.local:9030.
2026-09-04T04:01:59 INFO  Audit log scrape: found 0 distinct table/catalog pairs with SELECTs.
2026-09-04T04:01:59 INFO  Warm-up evaluation: 0 eligible tables, 0 triggered.
2026-09-04T04:01:59 INFO  === Cycle done. active_warmups=0 ===
2026-09-04T04:01:59 INFO  Sleeping 3600 seconds until next scan.
```

---

## 8. Catalog Reference

| Doris Catalog | Polaris Warehouse | Spark Catalog Name | Source |
|---|---|---|---|
| `polaris` | `IcebergCatalog` | `polaris` | Primary Iceberg lakehouse |
| `databricks` | `star_lakehouse` | `databricks` | Databricks Unity Catalog mirror |
| `postgres` | `pg_lakehouse` | `postgres` | PostgreSQL Iceberg tables |
| `oracle` | `ora_lakehouse` | `oracle` | Oracle Iceberg tables |
| `mongodb` | `mgo_lakehouse` | `mongodb` | MongoDB Iceberg tables |

All catalogs share the same Polaris REST endpoint:
`http://polaris-rest.prod.svc.cluster.local:8181/api/catalog`

OAuth2 credentials: `secret/data/platform/polaris` → `spark_svc_id` + `spark_svc_secret`
