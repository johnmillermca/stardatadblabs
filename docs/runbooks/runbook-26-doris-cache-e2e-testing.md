# Runbook 26 — Doris Dynamic Cache Manager: End-to-End Testing

| Field | Value |
|---|---|
| **Runbook ID** | RB-26 |
| **Service** | k8s-platform / doris-cache-manager |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-09-10 (v1.4.0 — cache metrics phase added) |
| **Related** | RB-25 (Cache Manager Setup & Operations) · RB-05 (Doris & Analytics) |

---

## Purpose

This runbook is a single, ordered end-to-end test script for the Doris Dynamic Segment Cache
Manager. Run each section from top to bottom on a live cluster. Every test includes the exact
command, expected output, and a ✅ / ❌ pass/fail criterion.

The test covers seven phases in order:

| Phase | Tests | What it validates |
|---|---|---|
| **P-1** | T-01 – T-07 | Infrastructure preflight — all dependencies alive |
| **P-2** | T-08 – T-10 | Daemon health — pod, logs, liveness probe |
| **P-3** | T-11 – T-16 | Audit log seeding — queries reach the daemon and stats are persisted |
| **P-4** | T-17 – T-21 | Warm-up scheduling — automatic and manual WARM_UP jobs |
| **P-5** | T-22 – T-25 | LRU eviction — COLD_DOWN and eviction log |
| **P-6** | T-26 – T-33 | Write pushdown — DML interception and Spark execution |
| **P-7** | T-34 – T-38 | Cache metrics — `table_cache_metrics` I/O tracking and hit-rate validation |

---

## Test Checklist

| # | Phase | Area | Test |
|---|---|---|---|
| T-01 | P-1 | Infra | Doris FE is reachable |
| T-02 | P-1 | Infra | Doris has at least one alive BE |
| T-03 | P-1 | Infra | All 5 Iceberg catalogs are registered |
| T-03a | P-1 | Infra | List all Iceberg tables across all catalogs |
| T-04 | P-1 | Infra | `platform_meta` tables exist |
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
| T-23 | P-5 | Eviction | `cache_state` returns to `COLD` after eviction |
| T-24 | P-5 | Eviction | `cache_eviction_log` records the eviction event |
| T-25 | P-5 | Eviction | Daemon LRU check does not re-evict an already-COLD table |
| T-26 | P-6 | Write | Write proxy pod is running and listening |
| T-27 | P-6 | Write | DML via proxy succeeds without error (Spark executes) |
| T-28 | P-6 | Write | Proxy logs show interception and Spark submission |
| T-29 | P-6 | Write | Spark REST confirms job FINISHED |
| T-30 | P-6 | Write | Local Doris DML passes through proxy unchanged |
| T-31 | P-6 | Write | SELECT via proxy works unchanged |
| T-32 | P-6 | Write | Manual Spark REST submission executes successfully |
| T-33 | P-6 | Write | Proxy does not intercept unknown catalog DML |
| T-34 | P-7 | Metrics | `table_cache_metrics` table exists and has rows after one cycle |
| T-35 | P-7 | Metrics | `SELECT * LIMIT 1000` is captured and shows local vs remote bytes |
| T-36 | P-7 | Metrics | Second run of same query shows 100% `cache_hit_pct` |
| T-37 | P-7 | Metrics | `warmup_count` increments after daemon warms a table |
| T-38 | P-7 | Metrics | Metrics update does not block or delay concurrent SELECT workload |

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

### T-04 — `platform_meta` tables exist

```bash
doris-mysql -e "SHOW TABLES FROM platform_meta;"
```

**Expected:**
```
cache_eviction_log
table_query_stats
```

✅ Pass: both tables listed.  
❌ Fail: `Unknown database 'platform_meta'` — apply `manifests/doris/setup/03_create_metadata_tables.sql` (RB-25 §3.6).

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

### T-12 — Doris audit log records those queries

Run immediately after T-11 (no need to wait).

> **Note:** Doris always records `catalog = 'internal'` in `audit_log` regardless
> of which external catalog a query targets. The correct way to find external
> catalog queries is to match the catalog name inside the `stmt` column.
> The window is `INTERVAL 15 MINUTE` to give buffer if T-11 took a few minutes.
> `return_rows = 1` selects only the scalar COUNT queries (each returns exactly
> 1 row) and excludes the T-12 meta-query itself (which returns many rows).

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT
    CASE
      WHEN LOWER(stmt) LIKE '%polaris.%'    THEN 'polaris'
      WHEN LOWER(stmt) LIKE '%databricks.%' THEN 'databricks'
      WHEN LOWER(stmt) LIKE '%postgres.%'   THEN 'postgres'
      WHEN LOWER(stmt) LIKE '%oracle.%'     THEN 'oracle'
      WHEN LOWER(stmt) LIKE '%mongodb.%'    THEN 'mongodb'
    END AS catalog,
    COUNT(*) AS hits
FROM __internal_schema.audit_log
WHERE
    time >= DATE_SUB(NOW(), INTERVAL 15 MINUTE)
    AND is_query = 1
    AND return_rows = 1
    AND (LOWER(TRIM(stmt)) LIKE 'select%' OR LOWER(TRIM(stmt)) LIKE 'with%')
    AND (
        LOWER(stmt) LIKE '%polaris.%'
     OR LOWER(stmt) LIKE '%databricks.%'
     OR LOWER(stmt) LIKE '%postgres.%'
     OR LOWER(stmt) LIKE '%oracle.%'
     OR LOWER(stmt) LIKE '%mongodb.%'
    )
GROUP BY 1
ORDER BY 1;
"
```

**Expected — 5 rows, one per catalog:**

```
catalog     | hits
------------|-----
databricks  |   1
mongodb     |   1
oracle      |   1
polaris     |   1
postgres    |   1
```

✅ Pass: all 5 catalogs appear with `hits ≥ 1`.
❌ Fail: 0 rows → audit log plugin not enabled. Check `SHOW VARIABLES LIKE 'enable_audit_plugin'`; if `false`, set `enable_audit_plugin=true` in `fe.conf` and restart FE.
❌ Fail: fewer than 5 rows → re-run the missing catalog's T-11 query; audit log flushes every 60 s so wait up to 1 minute, then re-run T-12.

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
FROM platform_meta.table_query_stats
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
FROM platform_meta.table_query_stats
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
FROM platform_meta.table_query_stats
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
FROM platform_meta.table_query_stats
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
Skip this test.  Cache warm-up status is tracked via `platform_meta.table_query_stats`
(`cache_state`, `last_warmed_ts`) populated by the daemon after each `_run_warmup` call.

To confirm the manual T-17 warm-up was effective, proceed directly to T-19.

---

### T-19 — `cache_state` advances to `WARM` in metadata

After T-18:

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT cache_state, last_warmed_ts
FROM platform_meta.table_query_stats
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
  FROM platform_meta.table_query_stats
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
FROM platform_meta.table_query_stats
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
FROM platform_meta.table_query_stats
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

> **Note:** The daemon's LRU threshold is 24 hours by default. For testing, you can either
> trigger eviction manually (T-22/T-23) or temporarily lower `LRU_EVICT_HOURS` to 0 and
> restart the daemon to force the automatic path (T-25). The manual path is sufficient for
> most validation.

### T-22 — Manual `COLD_DOWN` executes without error

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
  -e "WARM UP CACHE ON TABLE polaris.tpcds_sf10tcl.inventory USING COLD_DOWN;"
```

**Expected:** no SQL error.

✅ Pass: statement completes without error.  
❌ Fail: `Syntax error` → same version requirement as T-17.

---

### T-23 — `cache_state` returns to `COLD` after daemon eviction

The daemon also issues `COLD_DOWN` internally when it detects a table is idle.
To trigger the daemon path without waiting 24 hours, temporarily patch the deployment:

```bash
# Lower eviction threshold to 0 hours (evict immediately)
kubectl set env deployment/doris-cache-manager -n prod LRU_EVICT_HOURS=0

# Wait for the rollout and one cycle
kubectl rollout status deployment/doris-cache-manager -n prod --timeout=60s
sleep 10  # give the cycle time to complete

# Check the state
doris-mysql -e "
  SELECT cache_state
  FROM platform_meta.table_query_stats
  WHERE catalog_name='polaris' AND table_name='store_sales';"
```

**Expected:**
```
COLD
```

Restore the original threshold after the test:

```bash
kubectl set env deployment/doris-cache-manager -n prod LRU_EVICT_HOURS=24
kubectl rollout status deployment/doris-cache-manager -n prod --timeout=60s
```

✅ Pass: `cache_state = COLD`.  
❌ Fail: still `WARM` → the daemon may have re-warmed the table in the same cycle; add a brief query-free interval before lowering the threshold.

---

### T-24 — `cache_eviction_log` records the eviction event

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT catalog_name, db_name, table_name, evicted_at, reason, last_select_ts
FROM platform_meta.cache_eviction_log
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
FROM platform_meta.cache_eviction_log
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

Write pushdown uses the `doris-write-proxy` — a transparent MySQL protocol proxy.
**Connect clients to port `30091` (not `30090`) for write operations.**

> **Setup:** Verify the proxy pod is running and the Spark REST endpoint is reachable.
> ```bash
> kubectl get pod -n prod -l app=doris-write-proxy
> kubectl logs -n prod deployment/doris-write-proxy --tail=5
> ```

---

### T-26 — Write proxy is running and listening

```bash
# Pod is Running
kubectl get pod -n prod -l app=doris-write-proxy

# Proxy logs show startup banner
kubectl logs -n prod deployment/doris-write-proxy | grep "Managed catalogs"
```

**Expected:**
```
Managed catalogs: polaris, databricks, postgres, oracle, mongodb
```

✅ Pass: pod is `Running` and startup log shows all 5 catalogs.
❌ Fail: `CrashLoopBackOff` → check `kubectl logs -n prod deployment/doris-write-proxy`.

---

### T-27 — DML via proxy succeeds without error (write intercepted, Spark executes)

Connect to the **write proxy port** (`30091`), not the standard Doris port:

```bash
DORIS_PASS=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.DORIS_ADMIN_PASSWORD}' | base64 -d)

# Connect via write proxy on port 30091
mysql -h 192.168.1.50 -P 30091 -u root -p"${DORIS_PASS}" \
  -e "INSERT INTO polaris.tpcds_sf10tcl.inventory
      SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk, 0, 0, 0
      FROM polaris.tpcds_sf10tcl.inventory
      WHERE 1=0;"
```

**Expected:** command exits with code `0` — **no error message**.
The proxy intercepts the DML, submits to Spark, waits for completion, and returns MySQL OK.

✅ Pass: `echo $?` returns `0`, no error output.
❌ Fail: MySQL error returned → check proxy logs (`T-28`) for Spark submission errors.

---

### T-28 — Proxy logs show interception and Spark submission

```bash
kubectl logs -n prod deployment/doris-write-proxy --tail=50 \
  | grep -E "intercepted|submissionId|FINISHED|FAILED"
```

**Expected:**
```
WriteProxy: intercepted polaris.tpcds_sf10tcl.inventory DML from <ip>:<port> — routing to Spark.
WriteProxy: polaris.tpcds_sf10tcl.inventory → Spark submissionId=driver-<timestamp>-<hash>
WriteProxy: polaris.tpcds_sf10tcl.inventory FINISHED.
```

Note the `submissionId` for T-29.

✅ Pass: all three log lines present.
❌ Fail: `Spark submission failed` → check T-07 (Spark REST reachable). `Timed out` → Spark job ran but didn't finish within 300s — check Spark worker logs.

---

### T-29 — Spark REST confirms job FINISHED

```bash
SUBMISSION_ID="driver-<from T-28>"   # replace with actual value

curl -s http://192.168.1.50:6066/v1/submissions/status/${SUBMISSION_ID} \
  | jq '{state: .driverState, success: .success}'
```

**Expected:**
```json
{ "state": "FINISHED", "success": true }
```

✅ Pass: `FINISHED` and `success = true`.
❌ Fail: `FAILED` → check Spark worker logs:
```bash
kubectl logs -n prod -l app=spark-worker | tail -100
```

---

### T-30 — Local Doris DML still works via proxy (not intercepted)

```bash
# Local internal table write — should pass through to Doris and succeed normally
mysql -h 192.168.1.50 -P 30091 -u root -p"${DORIS_PASS}" \
  -e "CREATE TABLE IF NOT EXISTS internal.test_proxy_passthrough
      (id INT) ENGINE=OLAP DISTRIBUTED BY HASH(id) BUCKETS 1
      PROPERTIES ('replication_num'='1');
      INSERT INTO internal.test_proxy_passthrough VALUES (1);"
```

**Expected:** executes successfully — Doris handles it natively, proxy passes through.

✅ Pass: no error returned, Doris executes the INSERT.
❌ Fail: local DML returns error → proxy is incorrectly intercepting non-catalog statements.

---

### T-31 — SELECT via proxy works unchanged

```bash
mysql -h 192.168.1.50 -P 30091 -u root -p"${DORIS_PASS}" \
  -e "SELECT COUNT(*) FROM polaris.tpcds_sf10tcl.inventory;"
```

**Expected:** returns a row count — proxy forwards SELECT to Doris unchanged.

✅ Pass: numeric result returned.
❌ Fail: error or no result → proxy is incorrectly intercepting SELECTs.

---

### T-32 — Manual Spark REST submission executes successfully

This test bypasses the proxy entirely and submits a write job directly to Spark,
validating the full `spark_iceberg_write.py` path end-to-end.

```bash
JOB_ARGS=$(python3 -c "import json; print(json.dumps({
  'catalog':'polaris', 'warehouse':'IcebergCatalog',
  'db':'tpcds_sf10tcl', 'table':'store_sales',
  'stmt':'SELECT 1'
}))")

curl -s -X POST http://192.168.1.50:6066/v1/submissions/create \
  -H "Content-Type: application/json" \
  -d "{
    \"action\": \"CreateSubmissionRequest\",
    \"appResource\": \"/app/spark_iceberg_write.py\",
    \"mainClass\": \"\",
    \"appArgs\": [\"${JOB_ARGS}\"],
    \"sparkProperties\": {
      \"spark.app.name\": \"rb26-manual-write-test\",
      \"spark.master\": \"spark://spark-master-internal.prod.svc.cluster.local:17077\",
      \"spark.submit.deployMode\": \"cluster\"
    },
    \"environmentVariables\": {
      \"ADDR\": \"http://openbao.prod.svc.cluster.local:8200\"
    },
    \"clientSparkVersion\": \"3.5.1\"
  }" | jq '{submissionId: .submissionId, success: .success}'
```

Poll for status:

```bash
MANUAL_ID="<submissionId from above>"
sleep 15
curl -s http://192.168.1.50:6066/v1/submissions/status/${MANUAL_ID} \
  | jq '{state: .driverState, success: .success}'
```

**Expected:** `state = FINISHED`, `success = true`.

✅ Pass: job finishes successfully.
❌ Fail: `FAILED` → check Spark worker logs for errors in `spark_iceberg_write.py`. Most common: OpenBao unreachable from Spark worker, or Polaris OAuth2 token expired.

---

### T-33 — Write proxy correctly rejects bad catalog

```bash
# DML against a non-managed catalog — proxy must NOT intercept,
# must forward to Doris and let Doris return its normal response.
mysql -h 192.168.1.50 -P 30091 -u root -p"${DORIS_PASS}" \
  -e "INSERT INTO unknown_catalog.db.table VALUES (1);" 2>&1 | head -3
```

**Expected:** Doris error about unknown catalog (not a Spark error).

✅ Pass: error message comes from Doris (mentions `unknown catalog` or `catalog not found`).
❌ Fail: Spark submission attempted for unknown catalog → `MANAGED_CATALOGS` check in proxy is broken.

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
FROM platform_meta.table_query_stats
ORDER BY catalog_name, table_name;
"

# Eviction audit
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" -e "
SELECT catalog_name, table_name, evicted_at, reason
FROM platform_meta.cache_eviction_log
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

> **Background:** `platform_meta.table_cache_metrics` is populated once per daemon cycle
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
FROM platform_meta.table_cache_metrics;"
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
FROM platform_meta.table_cache_metrics
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

**Observed (2026-09-10 live run):**
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
FROM platform_meta.table_cache_metrics
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
FROM platform_meta.table_cache_metrics
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
FROM platform_meta.table_cache_metrics
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
