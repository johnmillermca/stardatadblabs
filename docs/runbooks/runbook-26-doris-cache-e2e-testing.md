# Runbook 26 — Doris Dynamic Cache Manager: End-to-End Testing

| Field | Value |
|---|---|
| **Runbook ID** | RB-26 |
| **Service** | k8s-platform / doris-cache-manager |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-09-11 (v1.6.0 — Phase 8: CatalogSyncer + CacheGuard E2E tests) |
| **Related** | RB-25 (Cache Manager Setup & Operations) · RB-05 (Doris & Analytics) |

---

## Purpose

This runbook is a single, ordered end-to-end test script for the Doris Dynamic Segment Cache
Manager. Run each section from top to bottom on a live cluster. Every test includes the exact
command, expected output, and a ✅ / ❌ pass/fail criterion.

The test covers eight phases in order:

| Phase | Tests | What it validates |
|---|---|---|
| **P-1** | T-01 – T-07 | Infrastructure preflight — all dependencies alive |
| **P-2** | T-08 – T-10 | Daemon health — pod, logs, liveness probe |
| **P-3** | T-11 – T-16 | Audit log seeding — queries reach the daemon and stats are persisted |
| **P-4** | T-17 – T-21 | Warm-up scheduling — automatic and manual WARM_UP jobs |
| **P-5** | T-22 – T-25 | LRU eviction — COLD_DOWN and eviction log (+ T-22a env-var check) |
| **P-6** | T-26 – T-33 | Write pushdown — DML interception and Spark execution |
| **P-7** | T-34 – T-38 | Cache metrics — `table_cache_metrics` I/O tracking and hit-rate validation |
| **P-8** | T-39 – T-46 | Auto-catalog sync + Cache Guard — CatalogSyncer and CacheGuard E2E validation |

---

## Test Checklist

| # | Phase | Area | Test |
|---|---|---|---|
| T-01 | P-1 | Infra | Doris FE is reachable |
| T-02 | P-1 | Infra | Doris has at least one alive BE |
| T-03 | P-1 | Infra | All 5 Iceberg catalogs are registered |
| T-03a | P-1 | Infra | List all Iceberg tables across all catalogs |
| T-04 | P-1 | Infra | `cache_system` tables exist |
| T-05 | P-1 | Infra | OpenBao K8s auth role `doris-cache-manager` exists |
| T-06 | P-1 | Infra | OpenBao secret `secret/data/platform/doris` is populated |
| T-07 | P-1 | Infra | Spark REST API is reachable |
| T-08 | P-2 | Daemon | Pod is Running with 0 restarts |
| T-09 | P-2 | Daemon | Startup log shows successful OpenBao authentication |
| T-10 | P-2 | Daemon | Liveness heartbeat file is fresh |
| T-11 | P-3 | Seeding | Queries against all 5 catalogs return results |
| T-11a | P-3 | Seeding | *(Fix)* S3 credentials missing — drop and recreate catalogs |
| T-12 | P-3 | Seeding | Doris audit log records those queries |
| T-13 | P-3 | Seeding | Daemon cycle picks up audit hits |
| T-14 | P-3 | Seeding | `table_query_stats` rows appear after first cycle |
| T-15 | P-3 | Seeding | Second query run advances `total_select_count` |
| T-16 | P-3 | Seeding | `select_interval_min` and `warm_interval_min` are computed |
| T-17 | P-4 | Warm-up | Manual `WARM UP CACHE … USING JOB` succeeds |
| T-18 | P-4 | Warm-up | `SHOW WARM UP JOB` transitions to FINISHED |
| T-19 | P-4 | Warm-up | `cache_state` advances to `WARM` in metadata |
| T-20 | P-4 | Warm-up | Daemon triggers automatic warm-up on second cycle |
| T-21 | P-4 | Warm-up | `last_warmed_ts` is updated in metadata |
| T-22 | P-5 | Eviction | Manual `COLD_DOWN` executes without error |
| T-22a | P-5 | Eviction | Verify current `LRU_EVICT_HOURS` value before patching |
| T-23 | P-5 | Eviction | `cache_state` returns to `COLD` after eviction |
| T-24 | P-5 | Eviction | `cache_eviction_log` records the eviction event |
| T-25 | P-5 | Eviction | Daemon LRU check does not re-evict an already-COLD table |
| T-26 | P-6 | Write | Write proxy pod is running and listening |
| T-27 | P-6 | Write | 3 random INSERT VALUES batches succeed; timing improves batch-over-batch |
| T-28 | P-6 | Write | Proxy logs show interception, Gluten active, resource release after each batch |
| T-29 | P-6 | Write | Spark master UI shows all batches FINISHED; duration trending down |
| T-30 | P-6 | Write | Local Doris DML passes through proxy unchanged (fast, no Spark) |
| T-31 | P-6 | Write | SELECT via proxy passes through to Doris, not intercepted |
| T-32 | P-6 | Write | Unknown catalog DML forwarded to Doris, not Spark |
| T-33 | P-6 | Write | (Optional) 20-row benchmark INSERT with timing targets |
| T-34 | P-7 | Metrics | `table_cache_metrics` table exists and has rows after one cycle |
| T-35 | P-7 | Metrics | `SELECT * LIMIT 1000` is captured and shows local vs remote bytes |
| T-36 | P-7 | Metrics | Second run of same query shows 100% `cache_hit_pct` |
| T-37 | P-7 | Metrics | `warmup_count` increments after daemon warms a table |
| T-38 | P-7 | Metrics | Metrics update does not block or delay concurrent SELECT workload |
| T-39 | P-8 | AutoCatalog | `catalog_sync_log` and `query_block_log` tables exist |
| T-40 | P-8 | AutoCatalog | CatalogSyncer startup log confirms Polaris warehouse enumeration |
| T-41 | P-8 | AutoCatalog | All 5 known warehouses already registered — syncer emits "already registered" for each |
| T-42 | P-8 | AutoCatalog | Simulate new warehouse → verify `CREATE CATALOG` is issued and `catalog_sync_log` row written |
| T-43 | P-8 | AutoCatalog | Auto-created catalog is immediately queryable via Doris |
| T-44 | P-8 | CacheGuard | CacheGuard thread is running (log confirms startup) |
| T-45 | P-8 | CacheGuard | SELECT against a cold table creates a row in `query_block_log` within 60 s |
| T-46 | P-8 | CacheGuard | JOIN query where one table is cold flags all cold tables and triggers warm-up for each |

---

## Prerequisites

```bash
# Get the Doris admin password (used for all mysql commands below)
DORIS_PASS=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.DORIS_ADMIN_PASSWORD}' | base64 -d)

# Shortcut alias used throughout this runbook
alias doris-mysql='mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" --silent'

# Get the OpenBao root token (needed for P-1 OpenBao checks)
BAO_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)
```

---

## Phase 1 — Infrastructure Preflight

### T-01 — Doris FE is reachable

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "SELECT 1;"
```

**Expected:**
```
+---+
| 1 |
+---+
| 1 |
+---+
```

✅ Pass: connection succeeds and returns `1`.  
❌ Fail: `Can't connect to MySQL server on '192.168.1.50'` — check `kubectl get svc -n prod | grep doris`.

---

### T-02 — Doris has at least one alive BE

```bash
doris-mysql -e "SHOW BACKENDS\G" | grep -E "Alive|Host"
```

**Expected:**
```
          Host: <node-ip>
         Alive: true
```

✅ Pass: at least one `Alive: true` line.  
❌ Fail: no alive backends — Doris queries will fail. Check `kubectl get pod -n prod -l app=doris-be`.

---

### T-03 — All 5 Iceberg catalogs are registered

```bash
doris-mysql -e "SHOW CATALOGS;" | awk '{print $2}'
```

**Expected** (order may vary):
```
CatalogName
internal
hive_metastore
polaris
databricks
postgres
oracle
mongodb
```

✅ Pass: all five managed catalogs (`polaris`, `databricks`, `postgres`, `oracle`, `mongodb`) are present.  
❌ Fail: any catalog missing — re-run RB-25 §3.5.

---

### T-03a — List all Iceberg tables across all catalogs

Use this command to get a full inventory of every table visible to Doris (and therefore to Spark)
across all 5 managed catalogs.  No credentials beyond `DORIS_PASS` are required.

```bash
DORIS_PASS=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.DORIS_ADMIN_PASSWORD}' | base64 -d)

for cat in polaris databricks postgres oracle mongodb; do
  echo "======= $cat ======="
  mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" --skip-column-names \
    -e "SHOW DATABASES FROM \`${cat}\`;" 2>/dev/null \
    | grep -v -E "^(information_schema|mysql)$" \
    | while read db; do
        mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" --skip-column-names \
          -e "SHOW TABLES FROM \`${cat}\`.\`${db}\`;" 2>/dev/null \
          | awk -v c="$cat" -v d="$db" '{print c"."d"."$1}'
      done
done
```

**Expected:** a flat list of fully-qualified `catalog.db.table` names, one per line.

**Observed (2026-09-10 live):**
```
======= polaris =======
polaris.tpcds_sf10tcl.call_center
polaris.tpcds_sf10tcl.catalog_page
polaris.tpcds_sf10tcl.catalog_returns
polaris.tpcds_sf10tcl.catalog_sales
polaris.tpcds_sf10tcl.customer
polaris.tpcds_sf10tcl.customer_address
polaris.tpcds_sf10tcl.customer_demographics
polaris.tpcds_sf10tcl.date_dim
polaris.tpcds_sf10tcl.household_demographics
polaris.tpcds_sf10tcl.income_band
polaris.tpcds_sf10tcl.inventory
polaris.tpcds_sf10tcl.item
polaris.tpcds_sf10tcl.promotion
polaris.tpcds_sf10tcl.reason
polaris.tpcds_sf10tcl.ship_mode
polaris.tpcds_sf10tcl.store
polaris.tpcds_sf10tcl.store_returns
polaris.tpcds_sf10tcl.store_sales
polaris.tpcds_sf10tcl.time_dim
polaris.tpcds_sf10tcl.warehouse
polaris.tpcds_sf10tcl.web_page
polaris.tpcds_sf10tcl.web_returns
polaris.tpcds_sf10tcl.web_sales
polaris.tpcds_sf10tcl.web_site
======= databricks =======
databricks.demo.customers
databricks.lakehouse_db.customer
databricks.lakehouse_db.customers
databricks.lakehouse_db.product
======= postgres =======
postgres.public.customers
postgres.public.inventory_events
postgres.public.orders
postgres.public.product_reviews
postgres.public.products
======= oracle =======
oracle.cache_testing.products
oracle.tpcds.call_center
oracle.tpcds.catalog_page
oracle.tpcds.household_demographics
oracle.tpcds.income_band
oracle.tpcds.promotion
oracle.tpcds.reason
oracle.tpcds.ship_mode
oracle.tpcds.warehouse
oracle.tpcds.web_page
oracle.tpcds.web_site
======= mongodb =======
mongodb.cache_testing.customers
mongodb.cache_testing.inventory_events
mongodb.cache_testing.order_items
mongodb.cache_testing.orders
mongodb.cache_testing.product_reviews
mongodb.cache_testing.products
```

> **Note:** `_pipeline_watermarks` rows are filtered out above because `SHOW TABLES` returns
> them alongside user tables.  They are internal bookkeeping tables written by the Spark
> ingestion pipeline and should not be warmed or queried directly.

✅ Pass: every expected table is listed under its catalog.
❌ Fail: a catalog returns no tables → check if the Polaris warehouse has been populated
(re-run the relevant ingestion job) or if S3 credentials are missing from the catalog
definition (see T-11a).

---

### T-04 — `cache_system` tables exist

```bash
doris-mysql -e "SHOW TABLES FROM cache_system;"
```

**Expected (v1.6.0+):**
```
cache_eviction_log
catalog_sync_log
query_block_log
table_cache_metrics
table_query_stats
```

✅ Pass: all 5 tables listed.
❌ Fail: `Unknown database 'cache_system'` — apply `manifests/doris/setup/03_create_metadata_tables.sql` (RB-25 §3.6).
❌ Fail: `catalog_sync_log` or `query_block_log` missing — the SQL script is from a pre-v1.6.0 run; re-apply the latest version of `03_create_metadata_tables.sql`.

---

### T-05 — OpenBao K8s auth role `doris-cache-manager` exists

```bash
kubectl exec -n prod openbao-0 -- \
  env BAO_TOKEN="${BAO_TOKEN}" \
  bao read auth/kubernetes/role/doris-cache-manager \
  | grep -E "bound_service_account_names|policies"
```

**Expected:**
```
bound_service_account_names    [doris-cache-manager]
policies                       [platform-secrets-read]
```

✅ Pass: both lines present with correct values.  
❌ Fail: `No value found` — re-run the role creation from RB-25 §3.0.

---

### T-06 — OpenBao secret `secret/data/platform/doris` is populated

```bash
kubectl exec -n prod openbao-0 -- \
  env BAO_TOKEN="${BAO_TOKEN}" \
  bao kv get -mount=secret platform/doris \
  | grep admin_password
```

**Expected:**
```
admin_password    <value>
```

✅ Pass: `admin_password` key present with a non-empty value.  
❌ Fail: key missing or empty — re-run the secret write from RB-25 §3.0.

---

### T-07 — Spark REST API is reachable

The Spark standalone REST submission API runs on port `6066` (container).
It is exposed externally on NodePort `30606` and in-cluster on `spark-master-svc:6066`.

```bash
# External (from master node)
curl -s --max-time 5 \
  http://192.168.1.50:30606/v1/submissions/status

# In-cluster (from any prod pod — matches what write-proxy and cache-manager use)
kubectl exec -n prod deployment/doris-cache-manager -- python3 -c "
import urllib.request, json, urllib.error
try:
    r = urllib.request.urlopen('http://spark-master-svc.prod.svc.cluster.local:6066/v1/submissions/status', timeout=5)
    body = r.read()
except urllib.error.HTTPError as e:
    body = e.read()
print(json.loads(body).get('serverSparkVersion'))
" 2>&1
```

**Expected** (both checks):
```json
{
  "action" : "ErrorResponse",
  "message" : "Submission ID is missing in status request.",
  "serverSparkVersion" : "3.5.1"
}
```

The `400 / "Submission ID is missing"` response **is the correct pass state** — it proves the REST API is live. A real status call requires a valid `submissionId` parameter; the empty request intentionally triggers this diagnostic response.

✅ Pass: JSON response containing `serverSparkVersion` is returned.
❌ Fail: `Connection refused` or timeout:
```bash
# Verify the REST port is enabled and listening on the master pod
MASTER=$(kubectl get pod -n prod -l app=spark,component=master --no-headers -o custom-columns=NAME:.metadata.name | head -1)
kubectl exec -n prod $MASTER -c spark-master -- netstat -tlnp 2>/dev/null | grep 6066
# Must show: tcp6  0  0  <ip>:6066  :::*  LISTEN

# If missing: spark.master.rest.enabled is not set.
# Verify SPARK_DAEMON_JAVA_OPTS contains -Dspark.master.rest.enabled=true
kubectl get deployment spark-master -n prod \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="spark-master")].env}' \
  | python3 -m json.tool | grep -A1 DAEMON
```

---

## Phase 2 — Daemon Health

### T-08 — Pod is Running with 0 restarts

```bash
kubectl get pod -n prod -l app=doris-cache-manager \
  -o custom-columns='NAME:.metadata.name,STATUS:.status.phase,RESTARTS:.status.containerStatuses[0].restartCount'
```

**Expected:**
```
NAME                                   STATUS    RESTARTS
doris-cache-manager-<hash>             Running   0
```

✅ Pass: `STATUS=Running` and `RESTARTS=0`.  
❌ Fail: `CrashLoopBackOff` or high restart count — check T-09 logs.

---

### T-09 — Startup log shows successful OpenBao authentication

> The startup lines only appear once at pod start. Use `--tail=1` and scroll back
> to the beginning, or fetch all logs from this pod run:

```bash
# Fetch all logs from the current pod run (startup lines are near the top)
POD=$(kubectl get pod -n prod -l app=doris-cache-manager \
  --no-headers -o custom-columns=NAME:.metadata.name | head -1)

kubectl logs -n prod $POD \
  | grep -E "OpenBao|Credentials|Cache Manager daemon running|Authenticated"
```

**Expected** (all six lines must appear):
```
Loading credentials from OpenBao (http://openbao.prod.svc.cluster.local:8200).
Authenticated to OpenBao via K8s SA JWT (role=doris-cache-manager).
Doris credentials loaded from OpenBao.
Polaris credentials loaded from OpenBao.
Credentials loaded.
Cache Manager daemon running. scan_interval=300s lru_evict=24h max_concurrent=32 warmup_stale=5min
```

> **Note:** `scan_interval=300s` (5 minutes) — not `3600s`. If you see `3600s` the
> `SCAN_INTERVAL_S` env var was not applied — redeploy with the current manifest.

✅ Pass: all six lines present with no `ERROR` between them.
❌ Fail: `HTTP Error 400` → OpenBao role missing (see §7.1). `KeyError: 'admin_password'` → secret empty (see §7.1a).

---

### T-10 — Liveness heartbeat file is fresh

```bash
kubectl exec -n prod deployment/doris-cache-manager -- \
  sh -c 'echo "age=$(($(date +%s) - $(stat -c %Y /tmp/cache_manager_alive)))s"'
```

**Expected:**
```
age=<N>s
```
where `N < 900` (less than 15 minutes old — matches the liveness probe threshold).

✅ Pass: file exists and age is under 900 s.
❌ Fail: `No such file or directory` — the daemon has not completed a cycle yet (wait up to 5 min) or is stuck. Check `kubectl logs -n prod deployment/doris-cache-manager | tail -20` for errors.

---

## Phase 3 — Audit Log Seeding

> **Goal:** Push queries into the Doris audit log so the daemon has data to process.
> Run all seed queries, trigger a daemon cycle, then verify stats were written.

### T-11 — Queries against all 5 catalogs return results

Run each query from a MySQL client connected to Doris (`192.168.1.50:30090`).
A result with at least 1 row (even `COUNT(*) = 0`) is sufficient — we are testing
reachability, not data content.

**Largest populated tables per catalog (confirmed 2026-09-09):**

| Catalog | Database | Table | Rows (confirmed) |
|---|---|---|---|
| `polaris` | `tpcds_sf10tcl` | `inventory` | 7 200 000 |
| `databricks` | `lakehouse_db` | `customers` | 10 005 |
| `postgres` | `public` | `products` | 54 500 |
| `oracle` | `tpcds` | `income_band` | 3 |
| `mongodb` | `cache_testing` | `products` | 19 849 651 |

> Oracle's `tpcds` warehouse is sparsely populated — most tables are empty.
> `income_band` (3 rows) is the best available for a non-zero result.

```bash
# polaris  (warehouse: IcebergCatalog — 7.2M rows, good cache seeding target)
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
  -e "SELECT COUNT(*) AS cnt FROM polaris.tpcds_sf10tcl.inventory;"

# databricks  (warehouse: star_lakehouse → db: lakehouse_db)
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
  -e "SELECT COUNT(*) AS cnt FROM databricks.lakehouse_db.customers;"

# postgres  (warehouse: pg_lakehouse → db: public — 54.5K rows)
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
  -e "SELECT COUNT(*) AS cnt FROM postgres.public.products;"

# oracle  (warehouse: ora_lakehouse → db: tpcds — largest non-empty table)
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
  -e "SELECT COUNT(*) AS cnt FROM oracle.tpcds.income_band;"

# mongodb  (warehouse: mgo_lakehouse → db: cache_testing — 19.8M rows)
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
  -e "SELECT COUNT(*) AS cnt FROM mongodb.cache_testing.products;"
```

✅ Pass: each query returns a single row with a numeric `cnt` (any value including 0).
❌ Fail: `SdkClientException: Unable to load credentials from AwsCredentialsProviderChain` → catalog is missing S3 credentials. See **T-11a** below.
❌ Fail: `Database [X] does not exist` → wrong namespace in the query — use the table map above. Run `SHOW DATABASES FROM <catalog>;` to enumerate what Polaris actually exposes.
❌ Fail: `Catalog not found` → verify T-03; catalog may need to be recreated.

---

### T-11a — Fix: S3 credentials missing from catalog definition

> **Root cause (confirmed 2026-09-09):** All 5 catalogs were created without
> `s3.access-key-id` / `s3.secret-access-key` properties. Doris BE queries the
> Polaris REST API for Iceberg metadata (namespace/table discovery) without S3
> creds, so `SHOW DATABASES` and `SHOW TABLES` work fine. But when a query
> actually reads data files the BE must access S3 directly — and with no
> credentials configured in the catalog it falls back to
> `AwsCredentialsProviderChain` which finds nothing and throws
> `SdkClientException`.
>
> The fix is to drop and recreate all 5 catalogs with S3 properties pulled from
> OpenBao. The `iceberg_polaris_rw` catalog (the original write catalog) had
> these properties from day one; the 5 managed catalogs were missing them.

**Step 1 — Pull S3 credentials from OpenBao:**

```bash
BAO_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

S3_RAW=$(curl -s -H "X-Vault-Token: ${BAO_TOKEN}" \
  http://192.168.1.50:30820/v1/secret/data/platform/s3)

export S3_KEY=$(echo "$S3_RAW" | \
  python3 -c "import json,sys; print(json.load(sys.stdin)['data']['data']['access_key'])")
export S3_SECRET=$(echo "$S3_RAW" | \
  python3 -c "import json,sys; print(json.load(sys.stdin)['data']['data']['secret_key'])")

echo "S3_KEY=${S3_KEY}  S3_SECRET_LEN=${#S3_SECRET}"
# Expected: S3_KEY=AKIA…  S3_SECRET_LEN=40
```

**Step 2 — Drop all 5 managed catalogs:**

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
DROP CATALOG IF EXISTS polaris;
DROP CATALOG IF EXISTS databricks;
DROP CATALOG IF EXISTS postgres;
DROP CATALOG IF EXISTS oracle;
DROP CATALOG IF EXISTS mongodb;
"
```

**Step 3 — Recreate with S3 credentials:**

```bash
DORIS_PASS=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.DORIS_ADMIN_PASSWORD}' | base64 -d)

envsubst < manifests/doris/setup/02_create_catalogs.sql \
  | mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}"
```

`envsubst` expands `${S3_KEY}` and `${S3_SECRET}` before the SQL reaches Doris.
The script ends with `SHOW CATALOGS` — verify all 5 appear.

**Step 4 — Re-run T-11 queries** to confirm each returns a numeric `cnt`.

---

### T-11b — Fix: `CATALOG_MANAGE_CONTENT` missing from Polaris catalog roles

> **Root cause (confirmed 2026-09-10):** Doris queries work for `polaris`
> (`IcebergCatalog`) but 4 of 5 catalogs fail with:
> `Failed to check view exist, error message is: Error occurred while processing HEAD request`
>
> `CATALOG_MANAGE_ACCESS` + `CATALOG_MANAGE_METADATA` are necessary for schema
> discovery, but `CATALOG_MANAGE_CONTENT` is additionally required for Doris to
> read table data.  `IcebergCatalog` had all three; the other 4 were missing it.

**Fix — run the idempotent grant script:**

```bash
bash manifests/doris/setup/04_grant_polaris_catalog_content.sh
```

Expected output: all 5 catalogs print `CATALOG_MANAGE_CONTENT = OK`.

**Re-run T-11 queries** to confirm each returns a numeric `cnt`.

See RB-25 §3.5a and §7.11 for the full diagnosis.

---

### T-12 — Doris audit log records those queries

Run immediately after T-11 (no need to wait).

> **Note:** Doris always records `catalog = 'internal'` in `audit_log` regardless
> of which external catalog a query targets. The correct way to find external-catalog
> queries is to match the catalog name inside the `stmt` column.
>
> **Why the previous hardcoded CASE version was wrong:**
> - `return_rows = 1` excluded any SELECT that returned more than 1 row (e.g. a LIMIT
>   query returning 10 rows would not be counted).
> - Hardcoded catalog names (`polaris`, `databricks`, …) silently drop queries against
>   any catalog auto-registered by `CatalogSyncer` — new catalogs fell through the CASE
>   as `NULL` and were discarded by `GROUP BY`.
>
> **The corrected query** joins against `information_schema.catalogs` so it automatically
> covers every registered catalog — including those created at runtime by `CatalogSyncer` —
> without any code or SQL change.

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT
    c.CatalogName                  AS catalog,
    COUNT(a.stmt)                  AS hits
FROM (
    -- Source of truth: every catalog currently registered in Doris,
    -- including any auto-synced by CatalogSyncer. Excludes built-ins.
    SELECT CatalogName
    FROM information_schema.catalogs
    WHERE CatalogName NOT IN ('internal', 'hive_metastore')
) c
LEFT JOIN __internal_schema.audit_log a
    ON  a.time >= DATE_SUB(NOW(), INTERVAL 15 MINUTE)
    AND a.is_query = 1
    AND (LOWER(TRIM(a.stmt)) LIKE 'select%' OR LOWER(TRIM(a.stmt)) LIKE 'with%')
    AND LOWER(a.stmt) LIKE CONCAT('%', LOWER(c.CatalogName), '.%')
GROUP BY c.CatalogName
ORDER BY hits DESC, c.CatalogName ASC;
"
```

**Expected — one row per registered external catalog, all with `hits ≥ 1`:**

```
catalog     | hits
------------|-----
polaris     |    1
databricks  |    1
postgres    |    1
oracle      |    1
mongodb     |    1
```

> If `CatalogSyncer` has auto-registered additional catalogs they will appear as
> extra rows automatically — no SQL change required.

✅ Pass: every external catalog appears with `hits ≥ 1`.
❌ Fail: `hits = 0` for all rows → audit log plugin not enabled or queries have not flushed yet (audit log flushes every 60 s). Check:
```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
  -e "SHOW VARIABLES LIKE 'enable_audit_plugin';"
# If Value = false: set enable_audit_plugin=true in fe.conf and restart FE.
```
❌ Fail: a catalog row shows `hits = 0` → re-run that catalog's T-11 query, wait up to 60 s for the audit log to flush, then re-run T-12.
❌ Fail: an auto-synced catalog is missing entirely → `CatalogSyncer` has not yet run a cycle; wait one `SCAN_INTERVAL_S` (300 s) and re-check `SHOW CATALOGS`.

---

### T-13 — Daemon cycle picks up the audit hits

Force an immediate cycle by restarting the daemon:

```bash
kubectl rollout restart deployment/doris-cache-manager -n prod
kubectl rollout status deployment/doris-cache-manager -n prod --timeout=60s
```

Then watch logs for the next cycle completion (takes < 30 s on startup):

```bash
kubectl logs -n prod -l app=doris-cache-manager -f \
  | grep -E "Audit log scrape|Cycle done|Sleeping"
```

**Expected:**
```
Audit log scrape: found N distinct table/catalog pairs with SELECTs.
=== Cycle done. active_warmups=0 ===
Sleeping 3600 seconds until next scan.
```
where `N ≥ 1`.

✅ Pass: `found N` with `N ≥ 1`.  
❌ Fail: `found 0` → re-check T-12. Queries may not have landed in the audit log yet — wait 1 minute and restart the daemon again.

---

### T-14 — `table_query_stats` rows appear after first cycle

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT catalog_name, db_name, table_name,
       total_select_count, cache_state,
       last_select_ts,
       last_warmed_ts,
       CASE
         WHEN last_warmed_ts IS NULL THEN 'never warmed'
         ELSE CONCAT(
           FLOOR(TIMESTAMPDIFF(SECOND, last_warmed_ts, NOW()) / 3600), 'h ',
           FLOOR((TIMESTAMPDIFF(SECOND, last_warmed_ts, NOW()) % 3600) / 60), 'm ',
           TIMESTAMPDIFF(SECOND, last_warmed_ts, NOW()) % 60, 's ago'
         )
       END AS warmed_age,
       ROUND(warm_interval_min, 2) AS warm_interval_min,
       select_interval_min
FROM cache_system.table_query_stats
ORDER BY last_select_ts DESC
LIMIT 10;
"
```

> **Note:** `last_warmed_ts` shows when the cache was last *physically warmed*.
> `warmed_age` shows how long ago that was in human-readable form (`Xh Ym Zs ago`).
> `last_select_ts` shows when the table was last *queried* — a different timestamp.
> Both will be `NULL` / `'never warmed'` until Phase 4 completes at least one warm-up cycle.

**Expected:** at least 1 row per catalog queried in T-11.

✅ Pass: rows present with `total_select_count ≥ 1`.
❌ Fail: empty result → daemon cycle did not complete successfully. Check daemon logs for errors.

**Observed (2026-09-10 00:26 live run — v1.4.0):**
```
catalog   db              table            count  state  last_select_ts        last_warmed_ts        warmed_age      warm_interval
oracle    tpcds           web_site         263    WARM   2026-09-10 00:26:14   NULL                  never warmed    3.35
postgres  public          products         372    WARM   2026-09-10 00:26:14   NULL                  never warmed    3.35
polaris   tpcds_sf10tcl   web_sales        263    WARM   2026-09-10 00:26:14   2026-09-10 00:26:19   0h 1m 37s ago   3.35
oracle    tpcds           web_page         6      WARM   2026-09-10 00:26:14   2026-09-10 00:26:15   0h 1m 41s ago   3.35
postgres  public          product_reviews  6      WARM   2026-09-10 00:26:14   2026-09-10 00:26:20   0h 1m 36s ago   3.35
polaris   tpcds_sf10tcl   web_returns      6      WARM   2026-09-10 00:26:14   2026-09-10 00:26:19   0h 1m 37s ago   3.35
oracle    tpcds           warehouse        30     WARM   2026-09-10 00:26:14   2026-09-10 00:26:18   0h 1m 38s ago   3.35
postgres  public          orders           6      WARM   2026-09-10 00:26:14   2026-09-10 00:26:18   0h 1m 38s ago   3.35
polaris   tpcds_sf10tcl   store_sales      72     WARM   2026-09-10 00:26:14   2026-09-10 00:26:19   0h 1m 37s ago   3.35
oracle    tpcds           ship_mode        6      WARM   2026-09-10 00:26:14   2026-09-10 00:26:15   0h 1m 41s ago   3.35
```

The `warmed_age` column tells you at a glance how stale the cache is for each table:
- `0h 1m 37s ago` — warmed ~2 minutes ago, very fresh ✅
- `never warmed` — `last_warmed_ts` is `NULL`; the daemon scanned the table but the
  `update_last_warmed` write has not persisted yet (or the block was warmed before this
  metric column existed). Check `table_cache_metrics.warmup_count` for the running count.

---

### T-15 — Second query run advances `total_select_count`

Note the current `total_select_count` for one table, then re-run its query:

```bash
# Record current count
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT total_select_count
FROM cache_system.table_query_stats
WHERE catalog_name = 'polaris'
  AND table_name = 'inventory';"

# Run the seeding query again
# Use polaris.tpcds_sf10tcl.inventory (7.2M rows — reliably lands in audit log)
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
  -e "SELECT COUNT(*) FROM polaris.tpcds_sf10tcl.inventory;"
```

Restart the daemon to force a new cycle:

```bash
kubectl rollout restart deployment/doris-cache-manager -n prod
```

After the cycle completes, re-check:

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT total_select_count
FROM cache_system.table_query_stats
WHERE catalog_name = 'polaris'
  AND table_name = 'inventory';"
```

**Expected:** count is higher than the value recorded before the second query.

✅ Pass: `total_select_count` incremented.  
❌ Fail: count unchanged → audit log not being flushed or daemon not scraping the latest window.

---

### T-16 — `select_interval_min` and `warm_interval_min` are computed

After T-15 (at least 2 query hits on a table):

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT
    catalog_name,
    table_name,
    total_select_count,
    select_interval_min,
    warm_interval_min
FROM cache_system.table_query_stats
WHERE total_select_count > 1
ORDER BY total_select_count DESC
LIMIT 5;
"
```

**Expected:** `select_interval_min` and `warm_interval_min` are non-NULL, and:
```
warm_interval_min ≈ select_interval_min × 0.667  (within rounding)
```

✅ Pass: both columns non-NULL and the 2/3 ratio holds.  
❌ Fail: NULL values → need a second cycle after a second query. Repeat T-15 if required.

---

## Phase 4 — Warm-Up Scheduling

> **⚠ Community Edition note:** `WARM UP CACHE … USING JOB` and `SHOW WARM UP JOB` are
> **Cloud Edition-only** commands.  Running them on Doris 4.0 Community Edition returns:
> ```
> ERROR 1105 (HY000): errCode = 2, detailMessage =
> no viable alternative at input 'WARM UP CACHE'(line 1, pos 8)
> ```
> The correct manual warm-up on Community Edition is a full-scan `SELECT` with
> `enable_file_cache=true` (see T-17 below).  T-18 (`SHOW WARM UP JOB`) is not
> applicable on this cluster.

### T-17 — Manual cache warm-up via full-scan SELECT succeeds

Trigger a warm-up by running a column-projection SELECT with `enable_file_cache=true`.
`COUNT(*)` alone resolves from Iceberg manifest metadata and generates zero scan bytes —
use `MAX()` or `SELECT * LIMIT` to force real BE I/O that populates the file cache.

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
  -e "SELECT /*+ SET_VAR(enable_file_cache=true) */
      MAX(ss_sales_price)
      FROM polaris.tpcds_sf10tcl.store_sales;"
```

**Expected:** query returns a result (no error). The BE reads data blocks from S3 and
writes them into `file_cache_path` as a side-effect.

✅ Pass: query returns a value without error.
❌ Fail: `errCode` on SELECT → check catalog connectivity (T-03) and BE health (T-02).

> **Why not `COUNT(*)`?**  Iceberg stores row counts in snapshot metadata.  Doris resolves
> `COUNT(*)` from metadata without touching data files — so no bytes land in the BE file
> cache.  Any aggregate that requires reading data values (`MAX`, `MIN`, `SUM`, `AVG`) or a
> `SELECT * LIMIT N` forces a real data scan.

---

### T-18 — `SHOW WARM UP JOB` — not applicable (Community Edition)

`SHOW WARM UP JOB` is a Cloud Edition command and raises a syntax error on this cluster.
Skip this test.  Cache warm-up status is tracked via `cache_system.table_query_stats`
(`cache_state`, `last_warmed_ts`) populated by the daemon after each `_run_warmup` call.

To confirm the manual T-17 warm-up was effective, proceed directly to T-19.

---

### T-19 — `cache_state` advances to `WARM` in metadata

After T-18:

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT cache_state, last_warmed_ts
FROM cache_system.table_query_stats
WHERE catalog_name = 'polaris'
  AND table_name = 'inventory';"
```

**Expected:**
```
cache_state: WARM
last_warmed_ts: <recent timestamp>
```

> **Note:** The daemon updates `cache_state` to `WARM` when it polls the job to FINISHED.
> If the warm-up was triggered manually (not by the daemon), restart the daemon and wait for
> one cycle — the `update_last_warmed` call only runs inside `_run_warmup`.

✅ Pass: `cache_state = WARM` and `last_warmed_ts` is non-NULL.
❌ Fail: still `UNKNOWN` → manually trigger a daemon cycle (restart the pod).

**Observed (2026-09-09 22:57 live run):** `polaris` and `postgres` catalog tables confirmed
`cache_state = WARM` with `last_warmed_ts` populated within seconds of the daemon cycle completing.
`oracle` catalog tables remained `UNKNOWN` — see known-issue note after T-21.

---

### T-20 — Daemon triggers automatic warm-up on second cycle

This test verifies the daemon's scheduling logic (`warm_interval = select_interval × 2/3`).
Since the test environment runs on an accelerated cycle, simply force two daemon cycles
more than `warm_interval_min` apart:

```bash
# Observe the warm_interval_min for inventory
doris-mysql -e "
  SELECT warm_interval_min
  FROM cache_system.table_query_stats
  WHERE catalog_name='polaris' AND table_name='inventory';"

# Force a cycle — wait at least warm_interval_min minutes — force another cycle
kubectl rollout restart deployment/doris-cache-manager -n prod
```

Watch daemon logs for the automatic submission:

```bash
kubectl logs -n prod -l app=doris-cache-manager --tail=80 \
  | grep -E "WARM_UP started|Warm-up evaluation"
```

**Expected:**
```
Warm-up evaluation: N eligible tables, M triggered.
WARM_UP started for polaris.tpcds_sf10tcl.inventory.
```
where `M ≥ 1`.

✅ Pass: `WARM_UP started` line present in logs.  
❌ Fail: `0 triggered` → `warm_interval_min` not elapsed yet, or `total_select_count ≤ 1` (run T-15 again).

---

### T-21 — `last_warmed_ts` is updated in metadata

After T-20 completes:

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT last_warmed_ts, cache_state
FROM cache_system.table_query_stats
WHERE catalog_name = 'polaris'
  AND table_name = 'inventory';"
```

**Expected:** `last_warmed_ts` is a timestamp within the last 10 minutes.

✅ Pass: timestamp is recent.
❌ Fail: timestamp unchanged → warm-up thread may have errored; check daemon logs for `WARM_UP thread error`.

**Observed (2026-09-09 22:57 live run):**
```
polaris  web_sales     → last_warmed_ts: 2026-09-09 22:57:33  cache_state: WARM  ✅
polaris  web_returns   → last_warmed_ts: 2026-09-09 22:57:26  cache_state: WARM  ✅
polaris  store_sales   → last_warmed_ts: 2026-09-09 22:57:19  cache_state: WARM  ✅
postgres products      → last_warmed_ts: 2026-09-09 22:57:17  cache_state: WARM  ✅
postgres product_reviews → last_warmed_ts: 2026-09-09 22:57:19  cache_state: WARM  ✅
postgres orders        → last_warmed_ts: 2026-09-09 22:57:16  cache_state: WARM  ✅
oracle   *             → last_warmed_ts: NULL                  cache_state: UNKNOWN  ❌
```

---

### Known Issue: `cache_state = UNKNOWN` / `last_warmed_ts = NULL` {#known-issue-cache-state-unknown}

#### Why `cache_state` starts as `UNKNOWN`

`UNKNOWN` is the **default** state written when the daemon inserts a brand-new row into
`table_query_stats` during an audit scrape.  It is overwritten with `WARM` only after
the warm-up thread completes its full-scan SELECT and calls `update_last_warmed()`.

State machine:
```
row inserted by audit scraper → cache_state = UNKNOWN
        ↓  (warm-up SELECT completes on BE)
  cache_state = WARM    (update_last_warmed writes this)
        ↓  (idle > LRU_EVICT_HOURS)
  cache_state = COLD    (eviction records this)
```

If you query `table_query_stats` **in the gap** between the audit scrape and the warm-up
thread completing, every new table will show `UNKNOWN`.  This is transient — wait until
the end of the cycle (at most `SCAN_INTERVAL_S` seconds) and re-run the query.

#### Why `last_warmed_ts` can be `NULL` even when `cache_state = WARM`

There are two distinct causes:

**1 — Transient `(2013) Lost connection` during the warm-up scan**

When the daemon launches up to 32 warm-up threads simultaneously, the Doris FE can
temporarily drop one of the new connections under load.  The thread aborts at the
`warmup_conn.execute(warm_sql)` call — before it ever reaches `update_last_warmed()`.
The row stays at whatever `cache_state` it held previously (`WARM` from a past cycle),
but `last_warmed_ts` is not updated.

```
ERROR — WARM_UP thread error for oracle.tpcds.web_site: (2013, 'Lost connection to MySQL server during query')
ERROR — WARM_UP thread error for postgres.public.products: (2013, 'Lost connection to MySQL server during query')
```

**Resolution:** automatic — the next daemon cycle retries the warm-up for every table
that is due.  No manual action needed.  Check current errors with:

```bash
kubectl logs -n prod -l app=doris-cache-manager --tail=200 \
  | grep -E "WARM_UP thread error|Lost connection"
```

**2 — Ghost row re-seeded from audit log**

A table that was previously deleted from `table_query_stats` (ghost cleanup) can be
re-inserted if the audit log still has recent SELECT statements against it.  The new row
starts with `UNKNOWN / NULL` until the next warm-up cycle processes it.

```bash
# Identify ghost candidates: UNKNOWN state with zero warm attempts
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT catalog_name, db_name, table_name, cache_state, last_warmed_ts
FROM cache_system.table_query_stats
WHERE cache_state = 'UNKNOWN'
ORDER BY updated_at DESC;"
```

Ghost rows are auto-deleted by the daemon when the warm-up SELECT fails with
`"does not exist"` / `"Unknown table"`.  If the table *does* exist, the next cycle will
successfully warm it and write `WARM`.

**Observed (2026-09-10 00:26 live run — confirmed root causes):**

```
oracle.tpcds.web_site           cache_state=WARM  last_warmed_ts=NULL  ← (2013) lost connection
oracle.tpcds.household_demographics  cache_state=WARM  last_warmed_ts=NULL  ← (2013) lost connection
oracle.tpcds.income_band        cache_state=WARM  last_warmed_ts=NULL  ← (2013) lost connection
oracle.general_ledger           cache_state=UNKNOWN  last_warmed_ts=NULL  ← ghost row re-seeded
```

All other oracle tables (`call_center`, `catalog_page`, `promotion`, `reason`, `ship_mode`,
`warehouse`, `web_page`) warmed successfully: `last_warmed_ts` populated within the same cycle.

**Conclusion:** Oracle JDBC tables warm correctly — the earlier belief that oracle was
unsupported was incorrect.  The failures were pure connection-pressure transients.

---

## Phase 5 — LRU Eviction

> **⚠ Community Edition note:** `COLD_DOWN` (`WARM UP CACHE … USING COLD_DOWN`) is a
> **Cloud Edition-only** command.  On Doris 4.0 Community Edition it raises the same syntax
> error as `WARM UP CACHE … USING JOB`:
> ```
> ERROR 1105 (HY000): errCode = 2, detailMessage =
> no viable alternative at input 'WARM UP CACHE'(line 1, pos 8)
> ```
> On Community Edition, the BE manages its file cache LRU natively — blocks are evicted
> automatically when the cache fills.  There is no programmatic eviction command.
> The daemon records evictions in `cache_eviction_log` and sets `cache_state = COLD`
> in metadata, but it does **not** issue any SQL `COLD_DOWN` command.

### T-22 — Manual eviction — not applicable (Community Edition)

`COLD_DOWN` does not exist on this cluster.  To simulate eviction for testing purposes,
directly update `cache_state` in the metadata table so the daemon's `LRUEvictionChecker`
logic can be exercised:

```bash
DORIS_PASS=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.DORIS_ADMIN_PASSWORD}' | base64 -d)

# Manually mark a table COLD to test the eviction path
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
UPDATE cache_system.table_query_stats
SET cache_state = 'COLD', updated_at = NOW()
WHERE catalog_name = 'polaris'
  AND db_name      = 'tpcds_sf10tcl'
  AND table_name   = 'store_sales';"

# Confirm
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT cache_state FROM cache_system.table_query_stats
WHERE catalog_name = 'polaris' AND table_name = 'store_sales';"
```

**Expected:** `cache_state = COLD`.

✅ Pass: UPDATE succeeds and SELECT returns `COLD`.
❌ Fail: row not found → table has not been seeded yet; run T-11 first.

---

### T-22a — Check the current `LRU_EVICT_HOURS` value

Before patching the eviction threshold, confirm what the daemon is currently using.
There are three complementary checks:

**1 — Live value from the running pod** (what the daemon process sees right now):

```bash
kubectl exec -n prod \
  $(kubectl get pod -n prod -l app=doris-cache-manager -o jsonpath='{.items[0].metadata.name}') \
  -- printenv LRU_EVICT_HOURS
```

Expected: `24`

**2 — All tuning env vars at once** (convenient sanity check before any test patching):

```bash
kubectl exec -n prod \
  $(kubectl get pod -n prod -l app=doris-cache-manager -o jsonpath='{.items[0].metadata.name}') \
  -- printenv | grep -E 'LRU_EVICT|SCAN_INTERVAL|MAX_CONCURRENT|WARMUP_STALE'
```

Expected:
```
LRU_EVICT_HOURS=24
SCAN_INTERVAL_S=300
MAX_CONCURRENT=32
WARMUP_STALE_MIN=5
```

**3 — Deployment spec** (what the next pod will use — source of truth after a `kubectl set env`):

```bash
kubectl get deployment doris-cache-manager -n prod \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="LRU_EVICT_HOURS")].value}'
```

> **Note:** The deployment manifest [`manifests/doris/cache_manager/doris-cache-manager-deployment.yaml`](../../../manifests/doris/cache_manager/doris-cache-manager-deployment.yaml)
> sets `LRU_EVICT_HOURS=24` as the baseline.  `kubectl set env` patches the live
> Deployment object only — a git push / ArgoCD sync will restore the manifest value.
> Always verify with method 1 (live pod `printenv`) after a rollout to confirm the new
> pod picked up the patched value.

✅ Pass: all three methods agree on the same value.
❌ Fail: live pod shows old value after rollout → rollout did not complete; check
`kubectl rollout status deployment/doris-cache-manager -n prod`.

---

### T-23 — `cache_state` is recorded as `COLD` by daemon LRU eviction

The daemon's `LRUEvictionChecker` marks tables `COLD` and writes a row to
`cache_eviction_log` when `last_select_ts` is older than `LRU_EVICT_HOURS` (default 24h).
It does **not** issue any SQL command to the BE — it only updates metadata.

To trigger this path without waiting 24 hours, lower `LRU_EVICT_HOURS` to 0 and restart.

> **⚠ Effect of `LRU_EVICT_HOURS=0`:** The eviction condition is
> `hours_idle >= LRU_EVICT_HOURS`.  With `0`, this is true for **every table that
> has ever been queried** — all `WARM`, `WARMING`, and `UNKNOWN` tables are evicted
> on the very next cycle.  Use only for testing; restore to `24` immediately after.

**Step 1 — Record the current value (T-22a) then patch:**

```bash
# Confirm current value before patching
kubectl exec -n prod \
  $(kubectl get pod -n prod -l app=doris-cache-manager -o jsonpath='{.items[0].metadata.name}') \
  -- printenv LRU_EVICT_HOURS
# Expected: 24

# Lower eviction threshold to 0 (evict all tables immediately)
# NOTE: ArgoCD will overwrite kubectl set env — edit the deployment YAML in git instead,
# or use a temporary patch that ArgoCD will reconcile away on next sync.
kubectl set env deployment/doris-cache-manager -n prod LRU_EVICT_HOURS=0
kubectl rollout status deployment/doris-cache-manager -n prod --timeout=60s

# Confirm the new pod has picked up the patched value
kubectl exec -n prod \
  $(kubectl get pod -n prod -l app=doris-cache-manager -o jsonpath='{.items[0].metadata.name}') \
  -- printenv LRU_EVICT_HOURS
# Expected: 0
```

**Step 2 — Wait for a cycle and check state:**

```bash
sleep 15  # allow one cycle to complete (SCAN_INTERVAL_S=300; daemon runs one cycle on startup)

mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT cache_state
FROM cache_system.table_query_stats
WHERE catalog_name = 'polaris' AND table_name = 'store_sales';"
```

**Expected:** `cache_state = COLD`.

**Step 3 — Restore immediately after the test:**

```bash
kubectl set env deployment/doris-cache-manager -n prod LRU_EVICT_HOURS=24
kubectl rollout status deployment/doris-cache-manager -n prod --timeout=60s

# Confirm restored
kubectl exec -n prod \
  $(kubectl get pod -n prod -l app=doris-cache-manager -o jsonpath='{.items[0].metadata.name}') \
  -- printenv LRU_EVICT_HOURS
# Expected: 24
```

✅ Pass: `cache_state = COLD` after the cycle.
❌ Fail: still `WARM` → the daemon re-warmed it in the same cycle before the eviction check ran; lower `MAX_CONCURRENT=0` temporarily to suppress warm-ups, then retry.

---

### T-24 — `cache_eviction_log` records the eviction event

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT catalog_name, db_name, table_name, evicted_at, reason, last_select_ts
FROM cache_system.cache_eviction_log
ORDER BY evicted_at DESC
LIMIT 5;
"
```

**Expected:** at least one row for `polaris / tpcds_sf10tcl / store_sales` with `reason = 'no_select_0h'` (or `no_select_24h` if the 24-hour path was used).

✅ Pass: row present with the correct table and reason.  
❌ Fail: no rows → the daemon's `LRUEvictionChecker` did not fire; confirm `cache_state` was `WARM` or `WARMING` before lowering the threshold (eviction only fires on those states).

---

### T-25 — Daemon does not re-evict an already-COLD table

Run a daemon cycle immediately after T-24 (restart the pod). Check that the eviction log
does **not** gain a second row for the same table:

```bash
kubectl rollout restart deployment/doris-cache-manager -n prod
kubectl rollout status deployment/doris-cache-manager -n prod --timeout=60s
sleep 15
```

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT COUNT(*) AS eviction_count
FROM cache_system.cache_eviction_log
WHERE catalog_name = 'polaris'
  AND table_name = 'inventory'
  AND evicted_at >= DATE_SUB(NOW(), INTERVAL 5 MINUTE);
"
```

**Expected:** `eviction_count = 0` (no new eviction for an already-COLD table).

✅ Pass: count is 0.  
❌ Fail: count > 0 → eviction guard is not checking `cache_state` correctly.

---

## Phase 6 — Write Pushdown (via `doris-write-proxy`)

Write pushdown uses the `doris-write-proxy` — a transparent MySQL protocol proxy that intercepts
DML against managed Iceberg catalogs (`polaris`, `databricks`, `postgres`, `oracle`, `mongodb`)
and routes them to Spark. All other SQL passes through to Doris unchanged.

**Connect clients to port `30091` (not `30090`) for write operations.**
**Use the `admin` user** (not `root`) — write-proxy authenticates against Doris with the admin account.

> **Setup check before any INSERT test:**
> ```bash
> DORIS_PASS=$(kubectl get secret rbac-plane-credentials -n prod \
>   -o jsonpath='{.data.DORIS_ADMIN_PASSWORD}' | base64 -d)
>
> # Proxy pod running?
> kubectl get pod -n prod -l app=doris-write-proxy
>
> # Spark cluster free? (should show no active apps)
> kubectl exec -n prod spark-master-6d7455fd7c-g6tkk -c spark-master -- \
>   curl -s http://localhost:8080/json/ | python3 -c \
>   "import json,sys; d=json.load(sys.stdin); print('Active jobs:', len(d.get('activeapps',[])))"
> ```

> **Performance note — cold vs warm start:**
> The first INSERT after a cluster restart takes **60–120 s** (Spark driver JVM init + Gluten/Velox
> native library load + executor launch on up to 4 workers). Subsequent INSERTs on a warm cluster
> complete in **15–40 s**. This is expected and matches direct `spark-submit` behaviour.
> See §"Performance Tuning" below for the full explanation.

---

### T-26 — Write proxy is running and listening

```bash
# Pod is Running with image 1.0.14+
kubectl get pod -n prod -l app=doris-write-proxy -o wide

# Proxy startup banner lists all 5 managed catalogs
kubectl logs -n prod deployment/doris-write-proxy | grep "Managed catalogs"

# Verify glibc + JVM version (must be Ubuntu 22.04 / glibc 2.35 + Temurin 17)
kubectl exec -n prod deployment/doris-write-proxy -- \
  sh -c "ldd --version 2>&1 | head -1; java -version 2>&1"
```

**Expected:**
```
Managed catalogs: polaris, databricks, postgres, oracle, mongodb
ldd (Ubuntu GLIBC 2.35-...) 2.35
openjdk version "17.0.10" ... Temurin-17.0.10+7
```

✅ Pass: pod `Running`, startup log shows all 5 catalogs, glibc 2.35, Temurin 17.
❌ Fail: `CrashLoopBackOff` → check `kubectl logs -n prod deployment/doris-write-proxy --tail=50`.

---

### T-27 — Random INSERT VALUES (warm-up the write path, confirm Spark executes)

These are self-contained `INSERT INTO … VALUES` statements — no SELECT, no source table dependency.
Use the **`admin` user** on port **`30091`** and allow up to **600 s** for the first cold-start.

**Run 3 batches.** The first is the cold-start baseline; each subsequent batch should be faster.

```bash
DORIS_PASS=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.DORIS_ADMIN_PASSWORD}' | base64 -d)

# ── Batch 1 — single row insert (cold start, ~60-120 s first time) ────────────
time mysql -h 192.168.1.50 -P 30091 -u admin -p"${DORIS_PASS}" \
  --connect-timeout=600 \
  -e "SET net_read_timeout=600; SET net_write_timeout=600;
INSERT INTO polaris.tpcds_sf10tcl.inventory
  (inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand)
VALUES
  (2450820, 1001, 5, 42);"
echo "Batch 1 exit: $?"
```

```bash
# ── Batch 2 — 5 rows (warm cluster, should be <60 s) ─────────────────────────
time mysql -h 192.168.1.50 -P 30091 -u admin -p"${DORIS_PASS}" \
  --connect-timeout=600 \
  -e "SET net_read_timeout=600; SET net_write_timeout=600;
INSERT INTO polaris.tpcds_sf10tcl.inventory
  (inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand)
VALUES
  (2450820, 1002, 5,  18),
  (2450820, 1003, 2,  75),
  (2450820, 1004, 7, 130),
  (2450820, 1005, 1,   5),
  (2450820, 1006, 3,  60);"
echo "Batch 2 exit: $?"
```

```bash
# ── Batch 3 — 10 rows across 3 dates (warm cluster, should be <40 s) ─────────
time mysql -h 192.168.1.50 -P 30091 -u admin -p"${DORIS_PASS}" \
  --connect-timeout=600 \
  -e "SET net_read_timeout=600; SET net_write_timeout=600;
INSERT INTO polaris.tpcds_sf10tcl.inventory
  (inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand)
VALUES
  (2450821, 2001, 4,  90),
  (2450821, 2002, 6,  25),
  (2450821, 2003, 1, 200),
  (2450821, 2004, 8,  15),
  (2450821, 2005, 2,  88),
  (2450822, 3001, 3,  33),
  (2450822, 3002, 5,  47),
  (2450822, 3003, 7,  61),
  (2450822, 3004, 9,  12),
  (2450822, 3005, 1,  99);"
echo "Batch 3 exit: $?"
```

**Expected for each batch:** `exit: 0`, no MySQL error output.
**Expected timing:** Batch 1 ≤ 120 s (cold), Batch 2 ≤ 60 s, Batch 3 ≤ 40 s.

✅ Pass: all batches exit 0 and timing improves batch-over-batch.
❌ Fail: `spark-submit failed (rc=1)` → check T-28 logs immediately.
❌ Fail: `ERROR 2000 (HY000): Lost connection` → proxy timeout; check `SPARK_JOB_TIMEOUT_S` is `600`.

---

### T-28 — Proxy logs confirm interception, execution, and resource release

After each batch above, immediately check:

```bash
kubectl logs -n prod deployment/doris-write-proxy --tail=30
```

**Expected pattern (per batch):**
```
WriteProxy: intercepted polaris.tpcds_sf10tcl.inventory DML from <ip> — routing to Spark.
WriteProxy: spark-submit polaris.tpcds_sf10tcl.inventory
WriteProxy [spark stdout]: Write-pushdown job: catalog=polaris ...
WriteProxy [spark stdout]: Credentials loaded from OpenBao.
WriteProxy [spark stderr]: Successfully loaded library libgluten.so
WriteProxy [spark stderr]: Successfully loaded library libvelox.so       ← Gluten active
WriteProxy [spark stdout]: Write-pushdown SUCCESS: ... affected_rows=...
WriteProxy: polaris.tpcds_sf10tcl.inventory spark-submit FINISHED.
```

Confirm Spark releases resources after each job:
```bash
# Run immediately after the batch completes — should show 0 active apps
kubectl exec -n prod spark-master-6d7455fd7c-g6tkk -c spark-master -- \
  curl -s http://localhost:8080/json/ | \
  python3 -c "import json,sys; d=json.load(sys.stdin); \
    apps=d.get('activeapps',[]); \
    print('Active:', len(apps), '— OK' if len(apps)==0 else '— WARN: resources still held')"
```

✅ Pass: `FINISHED` log line present, Gluten/Velox libraries loaded, `Active: 0` after completion.
❌ Fail: `libvelox.so: cannot enable executable stack` → image is not `1.0.14+` (Ubuntu 22.04 base required).
❌ Fail: `ClassNotFoundException: org.apache.iceberg.spark.SparkCatalog` → driver JARs not baked in; rebuild image.
❌ Fail: `Active: 1` persists → executor cleanup TTL (60 s); wait and re-check. If persistent, check `spark.stop()` called in script.

---

### T-29 — Confirm Spark app is FINISHED in master UI

```bash
# List last 3 completed apps (should all be FINISHED, not FAILED)
kubectl exec -n prod spark-master-6d7455fd7c-g6tkk -c spark-master -- \
  curl -s http://localhost:8080/json/ | \
  python3 -c "
import json, sys
d = json.load(sys.stdin)
comp = d.get('completedapps', [])
print(f'Last {min(3,len(comp))} completed apps:')
for a in comp[-3:]:
    dur = a['duration'] / 1000
    print(f'  {a[\"id\"]}  state={a[\"state\"]}  dur={dur:.0f}s  name={a[\"name\"]}')
"
```

**Expected:** all 3 batches appear as `state=FINISHED`. Duration should decrease across batches:
- Batch 1: 60–120 s (cold start)
- Batch 2: 20–60 s
- Batch 3: 15–40 s

✅ Pass: all `FINISHED`, duration trending down.
❌ Fail: any `FAILED` → get full error: `kubectl logs -n prod deployment/doris-write-proxy --tail=100 | grep -v "at java\|at org\|at py4j"`.

---

### T-30 — Local Doris DML still works via proxy (not intercepted)

```bash
# Local internal table write — must pass through to Doris, not Spark
mysql -h 192.168.1.50 -P 30091 -u admin -p"${DORIS_PASS}" \
  -e "CREATE TABLE IF NOT EXISTS internal.test_proxy_passthrough
        (id INT) ENGINE=OLAP DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num'='1');
      INSERT INTO internal.test_proxy_passthrough VALUES (42);"
```

**Expected:** executes immediately (< 1 s) — Doris handles it natively, proxy passes through without touching Spark.

✅ Pass: no error, fast response.
❌ Fail: error or slow → proxy is incorrectly intercepting non-catalog statements; check `_catalog_and_parts()` regex.

---

### T-31 — SELECT via proxy passes through to Doris unchanged

```bash
# SELECT must never be intercepted — proxy only routes DML
mysql -h 192.168.1.50 -P 30091 -u admin -p"${DORIS_PASS}" \
  -e "SELECT COUNT(*) AS cnt FROM polaris.tpcds_sf10tcl.inventory;"
```

**Expected:** returns a numeric count immediately — Doris answers from segment cache, no Spark involved.

✅ Pass: numeric result, fast response (< 5 s).
❌ Fail: error or hang → proxy is intercepting SELECTs; verify `_DML_RE` regex does not match `SELECT`.

---

### T-32 — Proxy does not intercept unknown catalog DML

```bash
# DML against a non-managed catalog — proxy must forward to Doris as-is
mysql -h 192.168.1.50 -P 30091 -u admin -p"${DORIS_PASS}" \
  -e "INSERT INTO unknown_catalog.db.tbl VALUES (1);" 2>&1 | head -3
```

**Expected:** Doris error about unknown catalog (fast, < 2 s, no Spark involved).

✅ Pass: error originates from Doris (e.g. `Unknown catalog` or `Catalog not found`).
❌ Fail: Spark submission attempted → `MANAGED_CATALOGS` check in proxy is broken.

---

### T-33 — (Optional) Additional random INSERT batches for performance benchmarking

Run these to build a timing baseline. Record the `real` time from `time` for each:

```bash
# ── 20-row insert across 5 warehouses ─────────────────────────────────────────
time mysql -h 192.168.1.50 -P 30091 -u admin -p"${DORIS_PASS}" \
  --connect-timeout=600 \
  -e "SET net_read_timeout=600; SET net_write_timeout=600;
INSERT INTO polaris.tpcds_sf10tcl.inventory
  (inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand)
VALUES
  (2450823, 4001, 1,  55),
  (2450823, 4002, 2,  77),
  (2450823, 4003, 3,  33),
  (2450823, 4004, 4,  99),
  (2450823, 4005, 5,  11),
  (2450823, 4006, 6,  44),
  (2450823, 4007, 7,  88),
  (2450823, 4008, 8,  22),
  (2450823, 4009, 9,  66),
  (2450823, 4010, 10, 10),
  (2450824, 5001, 1,  50),
  (2450824, 5002, 2,  70),
  (2450824, 5003, 3,  30),
  (2450824, 5004, 4,  90),
  (2450824, 5005, 5,  15),
  (2450824, 5006, 6,  45),
  (2450824, 5007, 7,  80),
  (2450824, 5008, 8,  25),
  (2450824, 5009, 9,  65),
  (2450824, 5010, 10,  5);"
echo "20-row exit: $?"
```

**Benchmark targets (warm cluster):**

| Rows | Expected time |
|------|--------------|
| 1    | 15 – 40 s    |
| 5    | 15 – 40 s    |
| 10   | 15 – 45 s    |
| 20   | 20 – 50 s    |

> Row count has minimal effect on latency for small inserts — the dominant cost is Spark job
> orchestration (driver init + executor launch + Polaris OAuth roundtrip), not data volume.
> Larger inserts (millions of rows) will show better throughput per row via Gluten/Velox native encoding.

---

## Performance Tuning — Why Write Pushdown Has Cold-Start Latency

Understanding the timing breakdown helps distinguish normal behaviour from real problems.

### Timing breakdown (per `spark-submit` invocation)

| Phase | Time | Notes |
|-------|------|-------|
| Driver JVM start | 2 – 5 s | Temurin 17 JVM cold start inside write-proxy pod |
| Gluten native init | 5 – 15 s | `libgluten.so` + `libvelox.so` extracted from JAR to `/tmp/`, loaded |
| Executor launch | 10 – 30 s | Up to 4 executors × 2 cores launched on Spark workers; each worker needs to start a JVM and load Gluten |
| Polaris OAuth token | 1 – 3 s | REST roundtrip to Polaris for catalog credentials |
| Iceberg metadata scan | 2 – 10 s | Reads snapshot manifest from S3 to plan the write |
| Data write + S3 upload | < 1 s (small INSERT VALUES) | Velox-encoded Parquet written via 64 MB multipart upload |
| `spark.stop()` + cleanup | 5 – 10 s | Executors disconnect; `/tmp/gluten-*` cleaned up |

**Total cold start: 25 – 73 s. Total warm cluster: 15 – 40 s.**

### Why direct `spark-submit` from the Spark master was faster

When you ran `spark-submit` directly from the **spark-master pod**, the master pod:
1. Already had the Temurin 17 JDK in its native OS (Ubuntu 22.04) — no cold JVM
2. Had `spark-defaults.conf` on its `SPARK_HOME/conf/` path — picked up `spark.driver.memory=4g` automatically
3. Was on the same network segment as workers — executor launch messages had ~0 ms RTT

The write-proxy pod previously had:
- `python:3.11-slim` (Debian 13 / glibc 2.41) — **hard-failed on `libvelox.so` PT_GNU_STACK RWE**
- No `spark-defaults.conf` — driver defaulted to **1 g** instead of 4 g (GC pressure)
- Grabbed **all 20 cores** across all 4 workers (no `spark.cores.max`) — executor cold-start on every node

### Fixes applied in image `1.0.14`

| Problem | Fix |
|---------|-----|
| glibc 2.41 hard-fails `libvelox.so` execstack | Rebased to `ubuntu:22.04` (glibc 2.35) |
| Driver memory 1 g (no spark-defaults.conf) | `spark.driver.memory=4g` in `_build_conf()` |
| All 20 cluster cores grabbed | `spark.cores.max=8`, `spark.executor.instances=4` |
| Executor memory/cores unset | `spark.executor.memory=3g`, `spark.executor.cores=2` |
| S3 uploads sequential | `fs.s3a.fast.upload=true`, 64 MB multipart, 20 threads |
| Driver classpath HTTP URLs | All 5 JARs baked into `/opt/spark-jars/` in image |
| Timeout 300 s too tight | `SPARK_JOB_TIMEOUT_S=600` |

### Further optimisation options (not yet applied)

```bash
# Option A: pre-warm executors by running a no-op job before the first real INSERT
# This amortises the cold-start cost across the pod lifetime rather than per-INSERT.

# Option B: increase spark.cores.max to 12 if the cluster is dedicated to writes
# (trade: fewer cores available for concurrent notebook/batch jobs)

# Option C: use Iceberg MERGE INTO instead of INSERT for upsert workloads —
# Iceberg MERGE with position-delete is more efficient than blind INSERT + compaction
```

---

## Summary Scorecard

After all tests are complete, verify the full pass matrix:

```bash
# Quick snapshot of current metadata state
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT
    catalog_name,
    table_name,
    total_select_count,
    cache_state,
    last_select_ts,
    last_warmed_ts,
    select_interval_min,
    warm_interval_min
FROM cache_system.table_query_stats
ORDER BY catalog_name, table_name;
"

# Eviction audit
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT catalog_name, table_name, evicted_at, reason
FROM cache_system.cache_eviction_log
ORDER BY evicted_at DESC
LIMIT 10;
"
```

```bash
# Daemon log summary for the entire test run
kubectl logs -n prod -l app=doris-cache-manager \
  | grep -E "=== Cache|Audit log|Warm-up evaluation|WARM_UP started|COLD_DOWN|WriteInterceptor|Cycle done"
```

Expected healthy summary:
```
=== Cache Manager cycle start: ... ===
Audit log scrape: found N distinct table/catalog pairs with SELECTs.
Warm-up evaluation: N eligible tables, N triggered.
WARM_UP started for polaris.tpcds_sf10tcl.inventory.
WriteInterceptor: 1 new DML write(s) detected against external catalogs.
WriteInterceptor: pushed polaris.tpcds_sf10tcl.inventory (qid=...) → Spark submissionId=driver-...
=== Cycle done. active_warmups=N ===
```

### Live Run Results — 2026-09-09 22:57 (initial run)

| catalog | table | total_select_count | cache_state | last_warmed_ts | result |
|---|---|---|---|---|---|
| oracle | web_site | 239 | `UNKNOWN` | NULL | ❌ Transient — (2013) lost connection during warm-up |
| oracle | web_page | 239 | `UNKNOWN` | NULL | ❌ Transient — retried next cycle → WARM |
| oracle | warehouse | 1201 | `UNKNOWN` | NULL | ❌ Transient — retried next cycle → WARM |
| oracle | ship_mode | 239 | `UNKNOWN` | NULL | ❌ Transient — retried next cycle → WARM |
| polaris | web_sales | 239 | `WARM` | 2026-09-09 22:57:33 | ✅ |
| polaris | web_returns | 239 | `WARM` | 2026-09-09 22:57:26 | ✅ |
| polaris | store_sales | 2652 | `WARM` | 2026-09-09 22:57:19 | ✅ |
| postgres | products | 275 | `WARM` | 2026-09-09 22:57:17 | ✅ |
| postgres | product_reviews | 239 | `WARM` | 2026-09-09 22:57:19 | ✅ |
| postgres | orders | 239 | `WARM` | 2026-09-09 22:57:16 | ✅ |

`select_interval_min = 1.051`, `warm_interval_min = 0.701` (2/3 ratio confirmed ✅).
Oracle `UNKNOWN` was **not** a catalog-type limitation — it was a `(2013) Lost connection` transient
when 32 warm-up threads all connected simultaneously. Confirmed resolved in next cycle.
See [Known Issue: cache_state = UNKNOWN](#known-issue-cache-state-unknown) for full root cause analysis.

**Daemon pod at time of run:** `doris-cache-manager-6c9dfcb86c-vgvfv` — Running, 0 restarts.

---

### Live Run Results — 2026-09-10 00:26 (confirmed cycle, v1.4.0)

| catalog | table | cache_state | last_warmed_ts | warmed_age | result |
|---|---|---|---|---|---|
| oracle | call_center | `WARM` | 2026-09-10 00:26:15 | 4m 39s ago | ✅ |
| oracle | catalog_page | `WARM` | 2026-09-10 00:26:15 | 4m 39s ago | ✅ |
| oracle | promotion | `WARM` | 2026-09-10 00:26:15 | 4m 39s ago | ✅ |
| oracle | reason | `WARM` | 2026-09-10 00:26:17 | 4m 37s ago | ✅ |
| oracle | ship_mode | `WARM` | 2026-09-10 00:26:15 | 4m 39s ago | ✅ |
| oracle | warehouse | `WARM` | 2026-09-10 00:26:18 | 4m 36s ago | ✅ |
| oracle | web_page | `WARM` | 2026-09-10 00:26:15 | 4m 39s ago | ✅ |
| oracle | web_site | `WARM` | NULL | never warmed | ⚠ (2013) lost connection — retries next cycle |
| oracle | household_demographics | `WARM` | NULL | never warmed | ⚠ (2013) lost connection — retries next cycle |
| oracle | income_band | `WARM` | NULL | never warmed | ⚠ (2013) lost connection — retries next cycle |
| oracle | general_ledger | `UNKNOWN` | NULL | never warmed | ⚠ ghost row re-seeded — auto-deleted if missing |
| polaris | web_sales | `WARM` | 2026-09-10 00:26:19 | 4m 35s ago | ✅ |
| polaris | web_returns | `WARM` | 2026-09-10 00:26:19 | 4m 35s ago | ✅ |
| polaris | store_sales | `WARM` | 2026-09-10 00:26:19 | 4m 35s ago | ✅ |
| postgres | products | `WARM` | NULL | never warmed | ⚠ (2013) lost connection — retries next cycle |
| postgres | product_reviews | `WARM` | 2026-09-10 00:26:20 | 4m 34s ago | ✅ |
| postgres | orders | `WARM` | 2026-09-10 00:26:18 | 4m 36s ago | ✅ |

`CacheMetrics: 5 rows written` — `local=1.8MB remote=0.0MB ratio=100%` ✅ (fully cache-warm).
Oracle JDBC tables warm correctly. All `NULL` warmed timestamps are transient lost-connection retries.

---

## Phase 7 — Cache Metrics

> **Background:** `cache_system.table_cache_metrics` is populated once per daemon cycle
> (default every 300 s) by a background thread with its own dedicated connection.
> It tracks — per table, per BE — how many bytes were read from local NVMe cache vs S3,
> the resulting hit percentage, query volume, average latency, warm-up count, and Spark
> pushdown count.  No hints or changes to end-user SQL are required.

---

### T-34 — `table_cache_metrics` table exists and has rows after one cycle

```bash
DORIS_PASS=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.DORIS_ADMIN_PASSWORD}' | base64 -d)

mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT COUNT(*) AS row_count,
       MIN(sampled_at) AS first_sample,
       MAX(sampled_at) AS latest_sample
FROM cache_system.table_cache_metrics;"
```

**Expected:** `row_count ≥ 1` and `latest_sample` is within the last `SCAN_INTERVAL_S` seconds.

✅ Pass: at least one row present.
❌ Fail: empty table → daemon has not completed a cycle yet, or `CacheMetricsCollector`
thread errored. Check daemon logs:
```bash
kubectl logs -n prod -l app=doris-cache-manager --tail=50 | grep -i CacheMetrics
```

**Observed (2026-09-10 00:21 live run):**
```
row_count: 1   first_sample: 2026-09-10 00:21:13   latest_sample: 2026-09-10 00:21:13
```

---

### T-35 — `SELECT * LIMIT 1000` is captured and shows local vs remote bytes

Run the target query, then wait up to one `SCAN_INTERVAL_S` window and query the metrics table:

```bash
# Step 1 — run the query as any user (no hints needed)
# IMPORTANT: use LIMIT 1000 or higher — LIMIT 10 resolves from Doris metadata
# (0 BE scan bytes, 6 ms latency) and will show total=0 MB in the metrics table.
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
  -e "SELECT * FROM databricks.lakehouse_db.customers LIMIT 1000;"

# Step 2 — wait for the next daemon cycle to capture it (max SCAN_INTERVAL_S seconds)
# Step 3 — query the metrics table
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT
    catalog_name,
    db_name,
    table_name,
    sampled_at,
    CONCAT(ROUND(local_scan_bytes  / 1024 / 1024, 2), ' MB') AS local_NVMe,
    CONCAT(ROUND(remote_scan_bytes / 1024 / 1024, 2), ' MB') AS remote_S3,
    CONCAT(ROUND(total_scan_bytes  / 1024 / 1024, 2), ' MB') AS total,
    CONCAT(cache_hit_pct, '%')                                AS cache_hit_pct,
    query_count,
    CONCAT(ROUND(avg_query_time_ms, 0), ' ms')                AS avg_latency,
    cache_state,
    warmup_count
FROM cache_system.table_cache_metrics
WHERE catalog_name = 'databricks'
  AND table_name   = 'customers'
ORDER BY sampled_at DESC
LIMIT 5;"
```

**Expected columns explained:**

| Column | Meaning |
|---|---|
| `local_NVMe` | Bytes served from BE local file cache (NVMe disk) — fast path |
| `remote_S3` | Bytes fetched from S3/remote storage — cold path |
| `cache_hit_pct` | `local / (local + remote) × 100` — higher is better |
| `query_count` | Number of SELECT statements against this table in the window |
| `avg_latency` | Average `query_time` from the audit log in the window |
| `warmup_count` | Cumulative number of completed daemon warm-up scans for this table |

✅ Pass: row present with `total > 0` and `query_count ≥ 1`.
❌ Fail: row missing → query did not land in the audit window yet; wait one more cycle.

> **Note:** `COUNT(*)` queries on Iceberg tables are resolved from metadata (no BE scan),
> so they show `total_scan_bytes = 0`.  Use a column projection like
> `SELECT * … LIMIT 1000` or `SELECT MAX(salary) …` to generate real scan bytes.

#### Why `cache_state = UNKNOWN` and `total = 0 MB` on first observation

This is the expected result when **all three of the following are true at once**:

| Condition | What you see | Why |
|---|---|---|
| Table was just seen for the first time by the daemon | `cache_state = UNKNOWN` | Default state on first `table_query_stats` insert — overwritten with `WARM` only after a warm-up scan completes |
| Query used `LIMIT 10` (or any small LIMIT) | `total = 0 MB`, `avg_latency = 6 ms` | Doris resolves small LIMITs from Iceberg metadata / FE-side filter — the BE never reads a data file, so `scan_bytes = 0` in `audit_log` |
| First daemon cycle after the query | `local_NVMe = 0 MB`, `remote_S3 = 0 MB` | BE Prometheus scan byte counters are deltas between consecutive cycles. On the first cycle the baseline snapshot is initialised to the current counter — delta is always 0 regardless of scan bytes |

**This output is therefore correct and expected:**
```
catalog_name  db_name       table_name  sampled_at            local_NVMe  remote_S3  total   cache_hit_pct  query_count  avg_latency  cache_state  warmup_count
databricks    lakehouse_db  customers   2026-09-11 03:xx:xx   0 MB        0 MB        0 MB    0%             1            6 ms         UNKNOWN      0
```

**To get meaningful byte metrics, run a full-scan query instead:**

```bash
# Forces BE to read actual Parquet files — generates real scan_bytes in audit_log
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
  -e "SELECT MAX(salary), MIN(salary), COUNT(*) FROM databricks.lakehouse_db.customers;"
```

Then wait one cycle and re-check `table_cache_metrics` — you will see:
- `cache_state` will have advanced to `WARM` after the daemon warm-up runs
- `remote_S3 > 0` (first run, blocks not yet cached)
- `local_NVMe > 0` on the second run (blocks now served from BE NVMe)

**Observed (2026-09-10 live run — full scan query):**
```
catalog    db           table      sampled_at            local_NVMe  remote_S3  total     hit_pct  queries  avg_latency  state  warmups
databricks lakehouse_db customers  2026-09-10 00:21:13   0 MB        0.98 MB    0.98 MB   0.00%    1        678 ms       WARM   1
```
First run shows 0% cache hit (cache cold for this query pattern) — expected.
Second run (T-36) will show 100% once the blocks are cached.

---

### T-36 — Second run of the same query shows 100% `cache_hit_pct`

Run the same query a second time (blocks now in BE NVMe cache from the first run), then
wait for the next cycle and compare:

```bash
# Run again — blocks now resident in BE file_cache_path
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
  -e "SELECT * FROM databricks.lakehouse_db.customers LIMIT 1000;"

# Wait for next daemon cycle, then check
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT
    sampled_at,
    CONCAT(ROUND(local_scan_bytes  / 1024 / 1024, 2), ' MB') AS local_NVMe,
    CONCAT(ROUND(remote_scan_bytes / 1024 / 1024, 2), ' MB') AS remote_S3,
    CONCAT(cache_hit_pct, '%') AS cache_hit_pct,
    query_count
FROM cache_system.table_cache_metrics
WHERE catalog_name = 'databricks'
  AND table_name   = 'customers'
ORDER BY sampled_at DESC
LIMIT 3;"
```

**Expected:** the latest row shows `cache_hit_pct = 100.00%` and `remote_S3 = 0 MB`.

✅ Pass: `cache_hit_pct = 100.00` and `remote_scan_bytes = 0`.
❌ Fail: still `0%` → blocks may have been evicted by BE LRU (cache full), or a different
BE node served the second query (check `be_host` column).

> **Why this confirms NVMe and not S3:**
> `local_scan_bytes` maps to `doris_be_workload_group_local_scan_bytes` in the BE Prometheus
> metrics — the BE increments this counter only when it reads a data block from its local
> `file_cache_path` directory (NVMe), not from S3.  `remote_scan_bytes` maps to
> `doris_be_workload_group_remote_scan_bytes`, incremented only for S3/remote fetches.
> A value of `remote_scan_bytes = 0` is proof that zero bytes came from S3.

---

### T-37 — `warmup_count` increments after daemon warms a table

```bash
# Record current warmup_count
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT table_name, warmup_count, last_warmed_ts
FROM cache_system.table_cache_metrics
WHERE catalog_name = 'databricks'
  AND table_name   = 'customers'
ORDER BY sampled_at DESC LIMIT 1;"

# Force a new daemon cycle
kubectl rollout restart deployment/doris-cache-manager -n prod
kubectl rollout status deployment/doris-cache-manager -n prod --timeout=60s

# Wait for the cycle to complete (watch daemon logs)
kubectl logs -n prod -l app=doris-cache-manager --tail=20 \
  | grep -E "CacheMetrics|WARM_UP scan completed.*customers"

# Re-check warmup_count
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT table_name, warmup_count, last_warmed_ts, sampled_at
FROM cache_system.table_cache_metrics
WHERE catalog_name = 'databricks'
  AND table_name   = 'customers'
ORDER BY sampled_at DESC LIMIT 2;"
```

**Expected:** `warmup_count` in the latest row is higher than in the previous row.

✅ Pass: `warmup_count` incremented.
❌ Fail: unchanged → daemon did not trigger a warm-up (check `warm_interval_min` vs time
since `last_warmed_ts`; the table may not be due for warming yet).

> `warmup_count` is a **cumulative** counter per daemon process lifetime.  It resets to 0
> when the pod restarts.  Use `last_warmed_ts` from `table_query_stats` for persistence.

---

### T-38 — Metrics update does not block or delay concurrent SELECT workload

Run 10 concurrent queries and verify they all complete while the metrics cycle is running:

```bash
# Fire 10 parallel SELECTs
for i in $(seq 1 10); do
  mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
    -e "SELECT MAX(salary), MIN(salary), COUNT(DISTINCT city)
        FROM databricks.lakehouse_db.customers;" &
done
wait
echo "All 10 queries completed."

# Confirm metrics cycle ran concurrently without error
kubectl logs -n prod -l app=doris-cache-manager --tail=30 \
  | grep -E "CacheMetrics|Cycle done|error|ERROR"
```

**Expected:**
- All 10 queries return results (no timeouts or connection errors).
- Daemon logs show `CacheMetrics: N rows written` with **no** errors.
- `Cycle done. active_warmups=N` is present — warm-up threads were not affected.

✅ Pass: all 10 queries complete and no ERROR in daemon logs.
❌ Fail: query timeout or `CacheMetricsCollector thread error` → investigate daemon logs.

> **Why concurrent workload is not impacted:**
> The `CacheMetricsCollector` runs in a separate daemon thread with its **own dedicated
> `DorisClient` connection**.  It shares no connection, no lock, and no slot with warm-up
> threads or the main cycle thread.  The only shared state is two `threading.Lock()`-protected
> integer counters (`_warmup_counts`, `_spark_counts`) which increment in O(1) and never block
> a warm-up thread for more than a few microseconds.

---

## Phase 8 — Auto-Catalog Sync + Cache Guard

> **Goal:** Validate that `CatalogSyncer` automatically registers new Polaris warehouses in Doris
> and that `CacheGuard` detects SELECT statements against cold tables, writes user-visible
> diagnostics to `query_block_log`, and triggers immediate warm-up within 60 seconds.
>
> **Prerequisites:** Phase 1 (T-01 – T-07) must pass. Image must be `1.6.0+`.
> Run these tests in order — T-42 requires a temporary warehouse created in T-42 and cleaned up after T-43.

---

### T-39 — `catalog_sync_log` and `query_block_log` tables exist

Verify both v1.6.0 metadata tables were created by `03_create_metadata_tables.sql`.

```bash
doris-mysql -e "SHOW TABLES FROM cache_system;" | grep -E "catalog_sync_log|query_block_log"
```

**Expected:**
```
catalog_sync_log
query_block_log
```

✅ Pass: both table names appear.
❌ Fail: either table missing → re-apply the metadata SQL:
```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
  < manifests/doris/setup/03_create_metadata_tables.sql
```

---

### T-40 — CatalogSyncer startup log confirms Polaris warehouse enumeration

Verify the daemon logs a `CatalogSyncer` sync entry on each cycle.

```bash
kubectl logs -n prod deployment/doris-cache-manager --tail=200 \
  | grep "CatalogSyncer" | head -20
```

**Expected (first cycle after deployment):**
```
CatalogSyncer: Polaris warehouses=['IcebergCatalog','star_lakehouse','pg_lakehouse','ora_lakehouse','mgo_lakehouse']  Doris catalogs={...}
CatalogSyncer: all Polaris warehouses already registered.
```

**Expected (any subsequent cycle):**
```
CatalogSyncer: all Polaris warehouses already registered.
```

✅ Pass: at least one `CatalogSyncer:` log line present and no `CatalogSyncer: sync failed` error.
❌ Fail: `CatalogSyncer: sync failed` or no CatalogSyncer lines → check `POLARIS_URI` env var and Polaris auth-proxy health:
```bash
kubectl get pod -n prod -l app=polaris-auth-proxy
kubectl logs -n prod deployment/polaris-auth-proxy --tail=20
```

---

### T-41 — All 5 known warehouses already registered — syncer skips them

Confirm that the syncer correctly identifies the 5 seed warehouses as already present
and does **not** attempt to re-create them.

```bash
kubectl logs -n prod deployment/doris-cache-manager --tail=500 \
  | grep "CatalogSyncer.*already registered" | sort | uniq -c
```

**Expected:** At least 5 unique "already registered" lines (one per known warehouse),
with no `CREATE CATALOG` log lines for the 5 known names.

```bash
# Confirm no spurious CREATE CATALOG was issued for existing catalogs
kubectl logs -n prod deployment/doris-cache-manager --tail=500 \
  | grep "CREATE CATALOG" | grep -E "polaris|databricks|postgres|oracle|mongodb"
# Expected: no output
```

✅ Pass: 5+ "already registered" lines, zero CREATE CATALOG lines for known names.
❌ Fail: `CREATE CATALOG 'polaris'` appears → SHOW CATALOGS returned an unexpected result; check `SHOW CATALOGS` manually.

---

### T-42 — Simulate new warehouse → verify `CREATE CATALOG` and `catalog_sync_log` row

> ⚠️ **This test creates a real Polaris warehouse and a real Doris catalog.**
> Clean up after T-43 using the teardown commands at the bottom of this section.

**Step 1 — Create a test warehouse in Polaris:**

```bash
BAO_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

POLARIS_ID=$(curl -s -H "X-Vault-Token: ${BAO_TOKEN}" \
  http://192.168.1.50:30820/v1/secret/data/platform/polaris \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['data']['spark_svc_id'])")

POLARIS_SECRET=$(curl -s -H "X-Vault-Token: ${BAO_TOKEN}" \
  http://192.168.1.50:30820/v1/secret/data/platform/polaris \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['data']['spark_svc_secret'])")

POLARIS_IP=$(kubectl get svc polaris-rest -n prod -o jsonpath='{.spec.clusterIP}')

TOKEN=$(curl -s -X POST "http://${POLARIS_IP}:8181/api/catalog/v1/oauth/tokens" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=${POLARIS_ID}&client_secret=${POLARIS_SECRET}&scope=PRINCIPAL_ROLE:ALL" \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])")

# Create the test warehouse (storage-profile uses the existing S3 bucket — no data written)
curl -s -X POST "http://${POLARIS_IP}:8181/api/management/v1/catalogs" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": {
      "name": "test_autosync_warehouse",
      "type": "INTERNAL",
      "properties": {},
      "storageConfigInfo": {
        "storageType": "S3",
        "allowedLocations": ["s3://xdatatoiceberg1/test-autosync/"],
        "roleArn": ""
      }
    }
  }' | python3 -m json.tool
```

**Expected:** JSON response with `"name": "test_autosync_warehouse"`.

**Step 2 — Wait for the next daemon cycle (up to `SCAN_INTERVAL_S` = 300 s) and check logs:**

```bash
# Watch for the CatalogSyncer to pick up the new warehouse
kubectl logs -n prod deployment/doris-cache-manager -f \
  | grep -E "CatalogSyncer|test_autosync"
```

**Expected log sequence:**
```
CatalogSyncer: new warehouse 'test_autosync_warehouse' detected → creating Doris catalog 'test_autosync_warehouse'.
CatalogSyncer: CREATE CATALOG 'test_autosync_warehouse' succeeded.
CatalogSyncer: 'test_autosync_warehouse' added to MANAGED_CATALOGS (now 6 catalogs).
CatalogSyncer: 1 new catalog(s) registered in Doris.
```

**Step 3 — Verify the Doris catalog was created:**

```bash
doris-mysql -e "SHOW CATALOGS;" | grep test_autosync_warehouse
# Expected: test_autosync_warehouse
```

**Step 4 — Verify the audit row in `catalog_sync_log`:**

```bash
doris-mysql -e "
SELECT catalog_name, warehouse_name, synced_at, action
FROM cache_system.catalog_sync_log
WHERE catalog_name = 'test_autosync_warehouse';"
```

**Expected:**
```
+---------------------------+-------------------------+---------------------+---------+
| catalog_name              | warehouse_name          | synced_at           | action  |
+---------------------------+-------------------------+---------------------+---------+
| test_autosync_warehouse   | test_autosync_warehouse | 2026-09-11 HH:MM:SS | CREATED |
+---------------------------+-------------------------+---------------------+---------+
```

✅ Pass: Doris catalog present, `catalog_sync_log` row exists with action=`CREATED`.
❌ Fail: No log line after 2 cycles → check `POLARIS_URI` and confirm `test_autosync_warehouse` is visible from the proxy:
```bash
kubectl exec -n prod deployment/doris-cache-manager -- \
  python3 -c "
import urllib.request, json, os
r = urllib.request.urlopen('${POLARIS_URI}/v1/warehouses')
print([w['name'] for w in json.loads(r.read())['warehouses']])
"
```

---

### T-43 — Auto-created catalog is immediately queryable via Doris

After T-42, the new catalog should be in `MANAGED_CATALOGS` in-memory and queryable.

```bash
# List databases in the auto-created catalog (warehouse is empty — expect 0 databases or information_schema only)
doris-mysql -e "SHOW DATABASES FROM test_autosync_warehouse;"
```

**Expected:** A result set (even if empty), with no `Catalog not found` error.

✅ Pass: SQL executes without `Catalog not found` or `Access denied` errors.
❌ Fail: `Catalog not found` → the daemon did not add the catalog to memory; verify T-42 pass first.

**Teardown — remove the test warehouse and catalog:**

```bash
# 1. Drop the Doris catalog
doris-mysql -e "DROP CATALOG IF EXISTS test_autosync_warehouse;"

# 2. Delete the Polaris warehouse
curl -s -X DELETE "http://${POLARIS_IP}:8181/api/management/v1/catalogs/test_autosync_warehouse" \
  -H "Authorization: Bearer ${TOKEN}"
echo "Polaris warehouse deleted"

# 3. Verify Doris catalog is gone
doris-mysql -e "SHOW CATALOGS;" | grep test_autosync_warehouse
# Expected: no output
```

---

### T-44 — CacheGuard thread is running

Confirm the CacheGuard background thread started successfully at daemon startup.

```bash
kubectl logs -n prod deployment/doris-cache-manager | grep "CacheGuard"
```

**Expected:**
```
CacheGuard started (poll_interval=60s lookback=120s).
```

✅ Pass: the startup line is present.
❌ Fail: missing → image is pre-v1.6.0; rebuild and redeploy (`doris-cache-manager:1.6.0`).

---

### T-45 — SELECT against a cold table writes a `query_block_log` row within 60 s

> **Setup:** Choose a table that is currently in state `COLD` or `UNKNOWN`.
> If all tables are `WARM`, patch `LRU_EVICT_HOURS=0` briefly to force eviction, then restore.

**Step 1 — Identify a cold table:**

```bash
doris-mysql -e "
SELECT catalog_name, db_name, table_name, cache_state, last_warmed_ts
FROM cache_system.table_query_stats
WHERE cache_state IN ('COLD','UNKNOWN')
LIMIT 5;"
```

Note one row — e.g. `polaris.tpcds_sf10tcl.income_band`.

**Step 2 — Run a SELECT against that cold table:**

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
  -e "SELECT COUNT(*) FROM polaris.tpcds_sf10tcl.income_band;"
```

**Step 3 — Wait up to 60 s (one CacheGuard poll cycle) and check `query_block_log`:**

```bash
sleep 65

doris-mysql -e "
SELECT query_id, detected_at, user_name, cold_tables, message
FROM cache_system.query_block_log
ORDER BY detected_at DESC
LIMIT 5;"
```

**Expected:** At least one row with:
- `cold_tables` containing `polaris.tpcds_sf10tcl.income_band`
- `message` beginning: `The following table(s) referenced in your query are not in the Doris segment cache`

```bash
# Also confirm warm-up was triggered by the guard
kubectl logs -n prod deployment/doris-cache-manager --tail=100 \
  | grep "CacheGuard.*triggered warm-up"
```

**Expected log:**
```
CacheGuard: query_id=<id> user=root — cold tables detected: polaris.tpcds_sf10tcl.income_band
CacheGuard: triggered warm-up for polaris.tpcds_sf10tcl.income_band on BE <ip>:8040.
```

✅ Pass: `query_block_log` row exists within 65 s of the SELECT, warm-up triggered in logs.
❌ Fail: No row after 120 s → check that `CACHE_GUARD_POLL_S` env var is set (default 60) and no `CacheGuard tick error` in logs:
```bash
kubectl logs -n prod deployment/doris-cache-manager | grep "CacheGuard tick error"
```

---

### T-46 — JOIN query with one cold table flags all cold tables and triggers warm-up

> **Goal:** Verify that `_extract_all_keys_from_stmt` detects all `FROM`/`JOIN` tables,
> not just the first one, so a multi-table query where only some tables are cold still
> triggers warm-up for every cold table.

**Step 1 — Identify two tables in different cache states:**

```bash
doris-mysql -e "
SELECT catalog_name, db_name, table_name, cache_state
FROM cache_system.table_query_stats
WHERE catalog_name = 'polaris' AND db_name = 'tpcds_sf10tcl'
ORDER BY cache_state
LIMIT 10;"
```

Find one `WARM` table (e.g. `inventory`) and one `COLD`/`UNKNOWN` table (e.g. `income_band`).

**Step 2 — Run a JOIN across both:**

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
  -e "SELECT COUNT(*)
      FROM polaris.tpcds_sf10tcl.inventory i
      JOIN polaris.tpcds_sf10tcl.income_band ib ON i.inv_item_sk = ib.ib_income_band_sk
      LIMIT 1;"
```

> The JOIN itself will likely return 0 rows (unrelated keys) — that is fine; we only
> care that the audit log records the statement and the guard processes it.

**Step 3 — Wait 65 s and check `query_block_log`:**

```bash
sleep 65

doris-mysql -e "
SELECT detected_at, user_name, cold_tables, stmt_preview
FROM cache_system.query_block_log
WHERE cold_tables LIKE '%income_band%'
ORDER BY detected_at DESC
LIMIT 3;"
```

**Expected:**
- `cold_tables` lists `polaris.tpcds_sf10tcl.income_band` (cold)
- `cold_tables` does **NOT** list `polaris.tpcds_sf10tcl.inventory` (already WARM — guard skips warm tables)
- `stmt_preview` contains `JOIN`

```bash
# Confirm warm-up was triggered only for the cold table, not the warm one
kubectl logs -n prod deployment/doris-cache-manager --tail=100 \
  | grep -E "CacheGuard.*triggered warm-up|CacheGuard.*cold tables"
```

**Expected:**
```
CacheGuard: query_id=<id> user=root — cold tables detected: polaris.tpcds_sf10tcl.income_band
CacheGuard: triggered warm-up for polaris.tpcds_sf10tcl.income_band on BE <ip>:8040.
```
(No warm-up line for `inventory` since it is already `WARM`.)

✅ Pass: `query_block_log` row exists, only the cold table is in `cold_tables`, and warm-up was triggered for it.
❌ Fail: `cold_tables` also lists the `WARM` table → `_extract_all_keys_from_stmt` is not filtering by `cache_state`; check daemon version is `1.6.0+`.
❌ Fail: No row at all → guard may not have seen the query; try increasing `CACHE_GUARD_LOOKBACK_S` to `300` for diagnosis.

---

## Summary Scorecard (Phase 8)

| Test | Description | Pass Criterion |
|---|---|---|
| T-39 | New metadata tables exist | `catalog_sync_log` and `query_block_log` both present in `cache_system` |
| T-40 | CatalogSyncer polling Polaris | `CatalogSyncer:` log lines appear each cycle, no sync failed errors |
| T-41 | Idempotency — known warehouses skipped | "already registered" for all 5 seed catalogs; no spurious CREATE |
| T-42 | New warehouse auto-registration | Log + `SHOW CATALOGS` + `catalog_sync_log` row all consistent |
| T-43 | Auto-catalog queryable immediately | `SHOW DATABASES FROM <new_catalog>` executes without error |
| T-44 | CacheGuard thread started | Startup log line confirms `poll_interval=60s lookback=120s` |
| T-45 | Single-table cold SELECT detected | `query_block_log` row within 65 s; warm-up triggered in logs |
| T-46 | JOIN cold table detected; warm table skipped | Only cold tables in `cold_tables`; warm-up fired for cold table only |

---

## Restore Defaults

If any environment variables were patched during testing, restore them:

```bash
kubectl set env deployment/doris-cache-manager -n prod \
  LRU_EVICT_HOURS=24 \
  SCAN_INTERVAL_S=3600 \
  MAX_CONCURRENT=32 \
  WARMUP_STALE_MIN=5

kubectl rollout status deployment/doris-cache-manager -n prod --timeout=60s
```
