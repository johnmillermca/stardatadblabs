# Runbook 26 — Doris Dynamic Cache Manager: End-to-End Testing

| Field | Value |
|---|---|
| **Runbook ID** | RB-26 |
| **Service** | k8s-platform / doris-cache-manager |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-09-04 |
| **Related** | RB-25 (Cache Manager Setup & Operations) · RB-05 (Doris & Analytics) |

---

## Purpose

This runbook is a single, ordered end-to-end test script for the Doris Dynamic Segment Cache
Manager. Run each section from top to bottom on a live cluster. Every test includes the exact
command, expected output, and a ✅ / ❌ pass/fail criterion.

The test covers six phases in order:

| Phase | Tests | What it validates |
|---|---|---|
| **P-1** | T-01 – T-07 | Infrastructure preflight — all dependencies alive |
| **P-2** | T-08 – T-10 | Daemon health — pod, logs, liveness probe |
| **P-3** | T-11 – T-16 | Audit log seeding — queries reach the daemon and stats are persisted |
| **P-4** | T-17 – T-21 | Warm-up scheduling — automatic and manual WARM_UP jobs |
| **P-5** | T-22 – T-25 | LRU eviction — COLD_DOWN and eviction log |
| **P-6** | T-26 – T-32 | Write pushdown — DML interception and Spark execution |

---

## Test Checklist

| # | Phase | Area | Test |
|---|---|---|---|
| T-01 | P-1 | Infra | Doris FE is reachable |
| T-02 | P-1 | Infra | Doris has at least one alive BE |
| T-03 | P-1 | Infra | All 5 Iceberg catalogs are registered |
| T-04 | P-1 | Infra | `platform_meta` tables exist |
| T-05 | P-1 | Infra | OpenBao K8s auth role `doris-cache-manager` exists |
| T-06 | P-1 | Infra | OpenBao secret `secret/data/platform/doris` is populated |
| T-07 | P-1 | Infra | Spark REST API is reachable |
| T-08 | P-2 | Daemon | Pod is Running with 0 restarts |
| T-09 | P-2 | Daemon | Startup log shows successful OpenBao authentication |
| T-10 | P-2 | Daemon | Liveness heartbeat file is fresh |
| T-11 | P-3 | Seeding | Queries against all 5 catalogs return results |
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

```bash
curl -s --max-time 5 \
  http://192.168.1.50:6066/v1/submissions/status \
  | head -c 80
```

**Expected:** any JSON response (even an error payload) — the endpoint is reachable.

✅ Pass: HTTP 200 or well-formed JSON error response.  
❌ Fail: `Connection refused` or timeout — check `kubectl get svc -n prod | grep spark`.

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

```bash
kubectl logs -n prod -l app=doris-cache-manager --tail=50 \
  | grep -E "OpenBao|Credentials|Cache Manager daemon running"
```

**Expected** (all four lines must appear):
```
Loading credentials from OpenBao (http://openbao.prod.svc.cluster.local:8200).
Authenticated to OpenBao via K8s SA JWT (role=doris-cache-manager).
Doris credentials loaded from OpenBao.
Polaris credentials loaded from OpenBao.
Credentials loaded.
Cache Manager daemon running. scan_interval=3600s lru_evict=24h max_concurrent=32 warmup_stale=5min
```

✅ Pass: all lines present with no `ERROR` between them.  
❌ Fail: `HTTP Error 400` → OpenBao role missing (T-05). `KeyError: 'admin_password'` → secret empty (T-06).

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
where `N < 7200` (less than 2 hours old).

✅ Pass: file exists and age is under 7200 s.  
❌ Fail: `No such file or directory` — the daemon has not completed a cycle yet or is stuck.

---

## Phase 3 — Audit Log Seeding

> **Goal:** Push queries into the Doris audit log so the daemon has data to process.
> Run all seed queries, trigger a daemon cycle, then verify stats were written.

### T-11 — Queries against all 5 catalogs return results

Run each query from a MySQL client connected to Doris (`192.168.1.50:30090`).
A result with at least 1 row (even `COUNT(*) = 0`) is sufficient — we are testing
reachability, not data content.

```sql
-- polaris
SELECT COUNT(*) AS cnt FROM polaris.tpcds_sf10tcl.store_sales;

-- databricks
SELECT COUNT(*) AS cnt FROM databricks.star_lakehouse_db.sales_fact;

-- postgres
SELECT COUNT(*) AS cnt FROM postgres.public.events;

-- oracle
SELECT COUNT(*) AS cnt FROM oracle.finance.general_ledger;

-- mongodb
SELECT COUNT(*) AS cnt FROM mongodb.analytics.user_events;
```

✅ Pass: each query returns a single row with a numeric `cnt` (any value including 0).  
❌ Fail: `Unknown table` / `Catalog not found` — verify T-03. `Connection timed out` — check catalog credentials in OpenBao.

---

### T-12 — Doris audit log records those queries

Wait ~30 seconds after T-11, then:

```sql
SELECT catalog, db, COUNT(*) AS hits
FROM __internal_schema.audit_log
WHERE
    time >= DATE_SUB(NOW(), INTERVAL 5 MINUTE)
    AND is_query = 1
    AND catalog IN ('polaris','databricks','postgres','oracle','mongodb')
GROUP BY catalog, db
ORDER BY catalog;
```

**Expected:** 5 rows — one per catalog.

✅ Pass: all 5 catalogs appear.  
❌ Fail: 0 rows → audit log plugin not enabled. Check `SHOW VARIABLES LIKE 'enable_audit%'` in Doris; if disabled, set `enable_audit_plugin=true` in `fe.conf` and restart FE.

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

```sql
SELECT catalog_name, db_name, table_name,
       total_select_count, cache_state, last_select_ts
FROM platform_meta.table_query_stats
ORDER BY last_select_ts DESC
LIMIT 10;
```

**Expected:** at least 1 row per catalog queried in T-11.

✅ Pass: rows present with `total_select_count ≥ 1`.  
❌ Fail: empty result → daemon cycle did not complete successfully. Check daemon logs for errors.

---

### T-15 — Second query run advances `total_select_count`

Note the current `total_select_count` for one table, then re-run its query:

```sql
-- Record current count
SELECT total_select_count
FROM platform_meta.table_query_stats
WHERE catalog_name = 'polaris'
  AND table_name = 'store_sales';

-- Run the seeding query again (from another MySQL session)
SELECT COUNT(*) FROM polaris.tpcds_sf10tcl.store_sales;
```

Restart the daemon to force a new cycle:

```bash
kubectl rollout restart deployment/doris-cache-manager -n prod
```

After the cycle completes, re-check:

```sql
SELECT total_select_count
FROM platform_meta.table_query_stats
WHERE catalog_name = 'polaris'
  AND table_name = 'store_sales';
```

**Expected:** count is higher than the value recorded before the second query.

✅ Pass: `total_select_count` incremented.  
❌ Fail: count unchanged → audit log not being flushed or daemon not scraping the latest window.

---

### T-16 — `select_interval_min` and `warm_interval_min` are computed

After T-15 (at least 2 query hits on a table):

```sql
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
```

**Expected:** `select_interval_min` and `warm_interval_min` are non-NULL, and:
```
warm_interval_min ≈ select_interval_min × 0.667  (within rounding)
```

✅ Pass: both columns non-NULL and the 2/3 ratio holds.  
❌ Fail: NULL values → need a second cycle after a second query. Repeat T-15 if required.

---

## Phase 4 — Warm-Up Scheduling

### T-17 — Manual `WARM UP CACHE … USING JOB` succeeds

```sql
WARM UP CACHE
  ON TABLE polaris.tpcds_sf10tcl.store_sales
  USING JOB;
```

**Expected:** query returns without error (Doris responds immediately; the job runs async).

✅ Pass: no SQL error.  
❌ Fail: `Syntax error` → Doris version does not support the segment cache command (requires Doris 2.1+).

---

### T-18 — `SHOW WARM UP JOB` transitions to FINISHED

Poll every 15 seconds until the job finishes (typically < 2 minutes for a small table):

```sql
SHOW WARM UP JOB WHERE TableName = 'store_sales';
```

**Expected column values:**
| Column | Expected |
|---|---|
| `State` | `FINISHED` (may pass through `PENDING` → `RUNNING`) |
| `Progress` | `100%` when FINISHED |

✅ Pass: `State = FINISHED`.  
❌ Fail: `State = FAILED` → check Doris BE logs. `State = RUNNING` after 5+ minutes → stale job; check §7.4 of RB-25.

---

### T-19 — `cache_state` advances to `WARM` in metadata

After T-18:

```sql
SELECT cache_state, last_warmed_ts
FROM platform_meta.table_query_stats
WHERE catalog_name = 'polaris'
  AND table_name = 'store_sales';
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

---

### T-20 — Daemon triggers automatic warm-up on second cycle

This test verifies the daemon's scheduling logic (`warm_interval = select_interval × 2/3`).
Since the test environment runs on an accelerated cycle, simply force two daemon cycles
more than `warm_interval_min` apart:

```bash
# Observe the warm_interval_min for store_sales
doris-mysql -e "
  SELECT warm_interval_min
  FROM platform_meta.table_query_stats
  WHERE catalog_name='polaris' AND table_name='store_sales';"

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
WARM_UP started for polaris.tpcds_sf10tcl.store_sales.
```
where `M ≥ 1`.

✅ Pass: `WARM_UP started` line present in logs.  
❌ Fail: `0 triggered` → `warm_interval_min` not elapsed yet, or `total_select_count ≤ 1` (run T-15 again).

---

### T-21 — `last_warmed_ts` is updated in metadata

After T-20 completes:

```sql
SELECT last_warmed_ts, cache_state
FROM platform_meta.table_query_stats
WHERE catalog_name = 'polaris'
  AND table_name = 'store_sales';
```

**Expected:** `last_warmed_ts` is a timestamp within the last 10 minutes.

✅ Pass: timestamp is recent.  
❌ Fail: timestamp unchanged → warm-up thread may have errored; check daemon logs for `WARM_UP thread error`.

---

## Phase 5 — LRU Eviction

> **Note:** The daemon's LRU threshold is 24 hours by default. For testing, you can either
> trigger eviction manually (T-22/T-23) or temporarily lower `LRU_EVICT_HOURS` to 0 and
> restart the daemon to force the automatic path (T-25). The manual path is sufficient for
> most validation.

### T-22 — Manual `COLD_DOWN` executes without error

```sql
WARM UP CACHE
  ON TABLE polaris.tpcds_sf10tcl.store_sales
  USING COLD_DOWN;
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

```sql
SELECT catalog_name, db_name, table_name, evicted_at, reason, last_select_ts
FROM platform_meta.cache_eviction_log
ORDER BY evicted_at DESC
LIMIT 5;
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

```sql
SELECT COUNT(*) AS eviction_count
FROM platform_meta.cache_eviction_log
WHERE catalog_name = 'polaris'
  AND table_name = 'store_sales'
  AND evicted_at >= DATE_SUB(NOW(), INTERVAL 5 MINUTE);
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
  -e "INSERT INTO polaris.tpcds_sf10tcl.store_sales
      SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk, 0, 0, 0
      FROM polaris.tpcds_sf10tcl.store_sales
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
WriteProxy: intercepted polaris.tpcds_sf10tcl.store_sales DML from <ip>:<port> — routing to Spark.
WriteProxy: polaris.tpcds_sf10tcl.store_sales → Spark submissionId=driver-<timestamp>-<hash>
WriteProxy: polaris.tpcds_sf10tcl.store_sales FINISHED.
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
  -e "SELECT COUNT(*) FROM polaris.tpcds_sf10tcl.store_sales;"
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

```sql
-- Quick snapshot of current metadata state
SELECT
    catalog_name,
    table_name,
    total_select_count,
    cache_state,
    last_select_ts,
    last_warmed_ts,
    warm_interval_min
FROM platform_meta.table_query_stats
ORDER BY catalog_name, table_name;

-- Eviction audit
SELECT catalog_name, table_name, evicted_at, reason
FROM platform_meta.cache_eviction_log
ORDER BY evicted_at DESC
LIMIT 10;
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
WARM_UP started for polaris.tpcds_sf10tcl.store_sales.
WriteInterceptor: 1 new DML write(s) detected against external catalogs.
WriteInterceptor: pushed polaris.tpcds_sf10tcl.store_sales (qid=...) → Spark submissionId=driver-...
=== Cycle done. active_warmups=N ===
```

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
