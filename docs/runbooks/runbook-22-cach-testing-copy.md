# Runbook 22 — starpump: cache_testing Copy (PostgreSQL · Oracle · MongoDB → Iceberg)

| Field | Value |
|---|---|
| **Runbook ID** | RB-22 |
| **Service** | k8s-platform / starpump / postgres · oracle · mongodb |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-09-02 |

---

## Overview

Copy tables/collections from three on-cluster sources into Iceberg via starpump.

| Source | Database | Schema / Owner | Tables | Smallest table | Polaris warehouse |
|---|---|---|---|---|---|
| **PostgreSQL** | `cache_testing` | `public` | customers, orders, products, order_items, inventory_events, product_reviews | `products` (500 K rows) | `pg_lakehouse` |
| **Oracle** | `XEPDB1` (SID) | `TPCDS` | income_band, ship_mode, warehouse, reason, web_page, web_site, household_demographics, catalog_page, call_center, promotion | `income_band` (3 rows) | `ora_lakehouse` |
| **MongoDB** | `cache_testing` | `cache_testing` | customers, orders, products, order_items, inventory_events, product_reviews | `products` (400 K docs) | `mgo_lakehouse` |

### Credentials summary (all stored in OpenBao)

| Source | OpenBao path | host | port | user |
|---|---|---|---|---|
| PostgreSQL | `secret/data/platform/postgres` | `postgresql.prod.svc.cluster.local` | `5432` | `rbac` |
| Oracle | `secret/data/platform/oracle` | `oracle-xe.prod.svc.cluster.local` | `1521` | `tpcds` |
| MongoDB | `secret/data/platform/mongodb` | `mongodb.prod.svc.cluster.local` | `27017` | `root` |

> Credentials are read by starpump at runtime from OpenBao — never hard-code them.

---

## Step 0 — Credentials already stored in OpenBao ✅

These were written on 2026-09-02 and are ready to use. You only need to re-run these
if the passwords change or the secrets are accidentally deleted.

### Verify secrets are present

```bash
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)
ADDR=http://192.168.1.50:30820

# PostgreSQL
curl -sf -H "X-Vault-Token: $TOKEN" $ADDR/v1/secret/data/platform/postgres \
  | python3 -c "import sys,json; d=json.load(sys.stdin)['data']['data']; [print(f'  {k}: {v}') for k,v in d.items() if k!='password']"

# Oracle
curl -sf -H "X-Vault-Token: $TOKEN" $ADDR/v1/secret/data/platform/oracle \
  | python3 -c "import sys,json; d=json.load(sys.stdin)['data']['data']; [print(f'  {k}: {v}') for k,v in d.items() if 'password' not in k]"

# MongoDB
curl -sf -H "X-Vault-Token: $TOKEN" $ADDR/v1/secret/data/platform/mongodb \
  | python3 -c "import sys,json; d=json.load(sys.stdin)['data']['data']; [print(f'  {k}: {v}') for k,v in d.items() if k!='password']"
```

✅ Expected (passwords omitted):
```
# postgres
  database: cache_testing
  host: postgresql.prod.svc.cluster.local
  port: 5432
  schema: public
  user: rbac

# oracle
  host: oracle-xe.prod.svc.cluster.local
  jdbc_url: jdbc:oracle:thin:@oracle-xe.prod.svc.cluster.local:1521/XEPDB1
  port: 1521
  schema: TPCDS
  sid: XEPDB1
  user: tpcds

# mongodb
  auth_source: admin
  database: cache_testing
  host: mongodb.prod.svc.cluster.local
  port: 27017
  user: root
```

### Re-write a secret (only if password changed)

<details>
<summary>PostgreSQL</summary>

```bash
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod -o jsonpath='{.data.root-token}' | base64 -d)
curl -s -X POST -H "X-Vault-Token: $TOKEN" -H "Content-Type: application/json" \
  http://192.168.1.50:30820/v1/secret/data/platform/postgres \
  -d '{
    "data": {
      "host":     "postgresql.prod.svc.cluster.local",
      "port":     "5432",
      "database": "cache_testing",
      "user":     "rbac",
      "password": "vb2dJms4c1fKi0uYD87Vv4YpCsZQJm1f",
      "schema":   "public"
    }
  }'
```
</details>

<details>
<summary>Oracle</summary>

```bash
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod -o jsonpath='{.data.root-token}' | base64 -d)
curl -s -X POST -H "X-Vault-Token: $TOKEN" -H "Content-Type: application/json" \
  http://192.168.1.50:30820/v1/secret/data/platform/oracle \
  -d '{
    "data": {
      "host":     "oracle-xe.prod.svc.cluster.local",
      "port":     "1521",
      "sid":      "XEPDB1",
      "jdbc_url": "jdbc:oracle:thin:@oracle-xe.prod.svc.cluster.local:1521/XEPDB1",
      "user":     "tpcds",
      "password": "TpcdsPwd123!",
      "schema":   "TPCDS"
    }
  }'
```
</details>

<details>
<summary>MongoDB</summary>

```bash
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod -o jsonpath='{.data.root-token}' | base64 -d)
curl -s -X POST -H "X-Vault-Token: $TOKEN" -H "Content-Type: application/json" \
  http://192.168.1.50:30820/v1/secret/data/platform/mongodb \
  -d '{
    "data": {
      "host":        "mongodb.prod.svc.cluster.local",
      "port":        "27017",
      "database":    "cache_testing",
      "user":        "root",
      "password":    "oEtCgw554IP3ua0SrJCTsWYM",
      "auth_source": "admin"
    }
  }'
```
</details>

---

## Step 1 — Create Polaris warehouses (once)

Each source needs its own Polaris warehouse. Run once per environment.

```bash
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)
POLARIS_TOKEN=$(curl -sf -H "X-Vault-Token: $TOKEN" \
  http://192.168.1.50:30820/v1/secret/data/platform/polaris \
  | python3 -c "import sys,json; d=json.load(sys.stdin)['data']['data']; print(d['spark_svc_id']+':'+d['spark_svc_secret'])")

for WH in pg_lakehouse ora_lakehouse mgo_lakehouse; do
  curl -sf -X POST \
    -H "Authorization: Bearer $(echo -n $POLARIS_TOKEN | base64)" \
    -H "Content-Type: application/json" \
    http://192.168.1.50:30183/api/management/v1/catalogs \
    -d "{\"name\":\"$WH\",\"type\":\"INTERNAL\",\"storageConfigInfo\":{\"storageType\":\"S3\",\"allowedLocations\":[\"s3://stardata-$WH\"]}}" \
    && echo "✅ $WH created" || echo "⚠️  $WH may already exist"
done
```

---

## Step 2 — Common setup

Run once per terminal session before any starpump command:

```bash
MASTER=$(kubectl get pod -n prod -l component=master \
  -o jsonpath='{.items[0].metadata.name}')
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)
echo "Master: $MASTER   Token: ${TOKEN:0:10}..."
```

---

## Step 2.5 — Pre-flight: verify Spark cores are free

**Always run this before any starpump job.** A stale process holding cores will
cause a new job to queue or fail. Takes under 2 seconds.

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  curl -sf http://localhost:8080/json/ | \
  python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'  Status      : {d[\"status\"]}')
print(f'  Cores free  : {d[\"cores\"] - d[\"coresused\"]} / {d[\"cores\"]}')
print(f'  Memory used : {d[\"memoryused\"]} / {d[\"memory\"]} MB')
print(f'  Active apps : {len(d[\"activeapps\"])}')
for a in d['activeapps']:
    print(f'    [{a[\"id\"]}] {a[\"name\"]}  cores={a[\"cores\"]}')
print(f'  Workers     : {len(d[\"workers\"])}')
for w in d['workers']:
    print(f'    {w[\"id\"]}  state={w[\"state\"]}  cores={w[\"cores\"]}  used={w[\"coresused\"]}')
"
```

✅ Expected before running any job:
```
  Status      : ALIVE
  Cores free  : 20 / 20
  Memory used : 0 / 24576 MB
  Active apps : 0
  Workers     : 4
    worker-...  state=ALIVE  cores=4  used=0
    worker-...  state=ALIVE  cores=8  used=0
    worker-...  state=ALIVE  cores=4  used=0
    worker-...  state=ALIVE  cores=4  used=0
```

If **Active apps > 0** or **Cores free < 20**, kill the stale process first:

```bash
# Kill any lingering starpump or SparkSubmit processes
kubectl exec -n prod $MASTER -c spark-master -- \
  pkill -9 -f "starpump|pyspark-shell|SparkSubmit" 2>&1 || echo "nothing to kill"

sleep 4   # allow the Master to deregister the dead app

# Re-check — cores should now be 0 used
kubectl exec -n prod $MASTER -c spark-master -- \
  curl -sf http://localhost:8080/json/ | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Cores used: {d[\"coresused\"]} / {d[\"cores\"]}  Active apps: {len(d[\"activeapps\"])}')"
```

---

## Step 3 — 1 000-row tests (one table per source)

These are the **recommended first tests** after any code or infrastructure change.
Each appends exactly **1 000 new rows** to one Iceberg table and completes in
under 30 seconds. Uses `MAX_ROWS=1000` to hard-cap the copy regardless of how
many rows are already in Iceberg.

> **`MAX_ROWS` vs `BATCH_SIZE`**
> `BATCH_SIZE` controls the page size per SQL query — it does **not** limit the
> total rows copied. `MAX_ROWS=1000` caps the total new rows written in this run
> across all batches. Always use `MAX_ROWS` when you want an exact row count.

### 3-A — PostgreSQL: +1 000 rows of `products`

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=public \
  INCLUDE_TABLES=products \
  MAX_ROWS=1000 \
  starpump postgres
```

✅ Expected key log lines:
```
=== starpump postgres | run_id=... user=dave db=cache_testing schema=public catalog=postgres threads=8 ===
[catalog-check] 'postgres' is registered (svc_id=...). Proceeding.
Discovered 6 tables in cache_testing.public: [...]
INCLUDE_TABLES filter: 6 → 1 tables (kept: ['products'])
[products] START: 0.1 GB | discovering schema …
[products] RESUME: N rows already in Iceberg — reusing extraction_ts=... starting at offset=N.
[products] batch offset=N rows=1000 total=N+1000
[products] DONE — N+1000 rows written (total incl. prior runs).
[pg-watermark] upserted cache_testing.public.products ... rows=N+1000
Completed in ~10s — 1/6 copied | 5 skipped (filtered) | 0 failed | N+1000 rows written
```

> On first run there is no prior state so the log shows `extraction_ts=...Z (CDC sync point)`
> instead of the RESUME line.

---

### 3-B — Oracle: +1 000 rows of `income_band`

Oracle's TPCDS dataset is very small — `income_band` has only 3 rows. `MAX_ROWS=1000`
copies all available rows and stops when the source is exhausted. This is correct
behaviour: the test confirms connectivity and schema mapping even if fewer than
1 000 rows exist.

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=XEPDB1 SCHEMAS=TPCDS \
  INCLUDE_TABLES=income_band \
  MAX_ROWS=1000 \
  starpump oracle
```

✅ Expected key log lines:
```
=== starpump oracle | run_id=... user=dave db=XEPDB1 schema=TPCDS catalog=oracle threads=8 ===
[catalog-check] 'oracle' is registered (svc_id=...). Proceeding.
Discovered 10 tables in XEPDB1.TPCDS: [...]
INCLUDE_TABLES filter: 10 → 1 tables (kept: ['income_band'])
[income_band] START: 0.0 GB | discovering schema …
[income_band] batch offset=0 rows=3 total=3
[income_band] DONE — 3 rows written (total incl. prior runs).
Completed in ~8s — 1/10 copied | 9 skipped (filtered) | 0 failed | 3 rows written
```

> 3 rows written is correct — `income_band` has exactly 3 rows in the XEPDB1.TPCDS
> dataset. Source exhausted = success.

---

### 3-C — MongoDB: +1 000 docs of `products`

MongoDB has no reliable cursor-based OFFSET, so starpump always reads from the start
of the collection each run. `MAX_ROWS=1000` shrinks the `$limit` in the aggregation
pipeline to exactly 1 000, and forces a single Spark partition so the `$limit` is
applied globally (not per-partition).

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=cache_testing \
  INCLUDE_TABLES=products \
  MAX_ROWS=1000 \
  starpump mongodb
```

✅ Expected key log lines:
```
=== starpump mongodb | run_id=... user=dave db=cache_testing schema=cache_testing catalog=mongodb threads=8 ===
[catalog-check] 'mongodb' is registered (svc_id=...). Proceeding.
Discovered 6 collections in cache_testing: [...]
INCLUDE_TABLES filter: 6 → 1 tables (kept: ['products'])
[products] START: 0.0 GB | discovering schema …
[products] extraction_ts=...Z (CDC sync point)
[products] MongoDB pipeline: [{"$limit": 1000}]
[products] batch offset=0 rows=1000 total=1000
[products] DONE — 1000 rows written (total incl. prior runs).
Completed in ~12s — 1/6 copied | 5 skipped (filtered) | 0 failed | 1000 rows written
```

> **Re-run behaviour for MongoDB:** because there is no OFFSET resume, each run
> appends another 1 000 docs from the start of the collection. This is expected —
> MongoDB is treated as append-only in starpump.

---

## Step 4 — Verify after the 1 000-row tests (fast — no Spark needed)

starpump writes `rows_copied` into the **pipeline PostgreSQL DB** after every
successful run. Reading it directly with `psycopg2` takes under 2 seconds — no
Spark JVM startup required.

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN python3 -c "
import os; os.environ['USER'] = 'dave'
from bao_spark_init import BaoSparkInit
import psycopg2

bao = BaoSparkInit()
pg  = bao.pipeline_db_creds()
conn = psycopg2.connect(
    host=pg['host'], port=int(pg.get('port', 5432)),
    dbname=pg['database'], user=pg['user'], password=pg['password'],
    connect_timeout=5,
)
cur = conn.cursor()

cur.execute('''
    SELECT source_db, source_schema, table_name,
           rows_copied, sf_extraction_ts, pipeline_run_ts
    FROM   pipeline_watermarks
    WHERE  (source_db, source_schema, table_name) IN (
        (\'cache_testing\', \'public\',        \'products\'),
        (\'XEPDB1\',        \'TPCDS\',         \'income_band\'),
        (\'cache_testing\', \'cache_testing\', \'products\')
    )
    ORDER BY pipeline_run_ts
''')
rows = cur.fetchall()
print()
print(f'  {\"source_db\":<16} {\"schema\":<15} {\"table\":<12} {\"rows_copied\":>11}  pipeline_run_ts')
print(f'  {\"-\"*16} {\"-\"*15} {\"-\"*12} {\"-\"*11}  {\"-\"*26}')
for r in rows:
    mark = \"✅\" if r[3] and r[3] > 0 else \"⚠️ \"
    print(f'  {mark} {r[0]:<14} {r[1]:<15} {r[2]:<12} {r[3]:>11}  {r[4]}')

cur.execute('''
    SELECT source_db, source_schema, status, tables_ok, total_rows, started_at, finished_at
    FROM   pipeline_run_log
    ORDER  BY started_at DESC LIMIT 6
''')
runs = cur.fetchall()
print()
print('  Recent runs:')
print(f'  {\"source_db\":<16} {\"schema\":<15} {\"status\":<10} {\"tables_ok\":>9} {\"total_rows\":>11}  started_at')
print(f'  {\"-\"*16} {\"-\"*15} {\"-\"*10} {\"-\"*9} {\"-\"*11}  {\"-\"*26}')
for r in runs:
    print(f'  {r[0]:<16} {r[1]:<15} {r[2]:<10} {r[3]:>9} {r[4]:>11}  {r[5]}')
cur.close(); conn.close()
"
```

✅ Expected output:
```
  source_db        schema          table        rows_copied  pipeline_run_ts
  ---------------- --------------- ------------ -----------  --------------------------
  ✅ cache_testing  public          products           49000  2026-09-02T03:33:01.268072Z
  ✅ XEPDB1         TPCDS           income_band            3  2026-09-02T03:44:02.103539Z
  ✅ cache_testing  cache_testing   products            1000  2026-09-02T04:26:37.312349Z

  Recent runs:
  source_db        schema          status     tables_ok  total_rows  started_at
  ...
  cache_testing    cache_testing   success            1        1000  2026-09-02 04:26:34...
  XEPDB1           TPCDS           success            1           3  2026-09-02 04:24:12...
  cache_testing    public          success            1       49000  2026-09-02 04:23:42...
```

---

## Step 5 — Full copy (all tables)

Run only after the 1 000-row tests pass. Takes several minutes per source.

### PostgreSQL: full copy

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=public \
  starpump postgres
```

| Table | Rows |
|---|---|
| `products` | 500 K |
| `customers` | 2 M |
| `inventory_events` | 10 M |
| `orders` | 10 M |
| `product_reviews` | 5 M |
| `order_items` | 39 M |

---

### Oracle: full copy (all 10 TPCDS tables, all tiny)

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=XEPDB1 SCHEMAS=TPCDS \
  starpump oracle
```

All 10 tables have 0–3 rows each — completes in under 30 seconds.

| Table | Rows |
|---|---|
| `income_band` | 3 |
| `ship_mode` | 2 |
| `warehouse` | 1 |
| `reason` | 1 |
| `call_center`, `catalog_page`, `household_demographics`, `promotion`, `web_page`, `web_site` | 0 each |

---

### MongoDB: full copy

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=cache_testing \
  starpump mongodb
```

| Collection | Docs |
|---|---|
| `products` | 400 K |
| `customers` | 2 M |
| `orders` | 2.5 M |
| `inventory_events` | 2 M |
| `order_items` | 3 M |
| `product_reviews` | 7.5 M |

---

## Step 6 — Test all starpump features

### 6-A — Dry-run (DDL only, no data)

Creates Iceberg tables and namespaces but writes zero rows.

```bash
# PostgreSQL
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=public \
  DRY_RUN=1 \
  starpump postgres

# Oracle
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=XEPDB1 SCHEMAS=TPCDS \
  DRY_RUN=1 \
  starpump oracle

# MongoDB
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=cache_testing \
  DRY_RUN=1 \
  starpump mongodb
```

✅ Expected: `0 rows written`, all tables show `DRY_RUN — skipping data copy.`

---

### 6-B — Single table copy (`INCLUDE_TABLES`)

```bash
# PostgreSQL: only customers
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=public \
  INCLUDE_TABLES=customers \
  MAX_ROWS=1000 \
  starpump postgres

# Oracle: only income_band
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=XEPDB1 SCHEMAS=TPCDS \
  INCLUDE_TABLES=income_band \
  starpump oracle

# MongoDB: only orders
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=cache_testing \
  INCLUDE_TABLES=orders \
  MAX_ROWS=1000 \
  starpump mongodb
```

---

### 6-C — Exclude a table (`EXCLUDE_TABLES`)

```bash
# PostgreSQL: everything except the giant order_items table
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=public \
  EXCLUDE_TABLES=order_items \
  MAX_ROWS=1000 \
  starpump postgres

# Oracle: skip the empty tables
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=XEPDB1 SCHEMAS=TPCDS \
  EXCLUDE_TABLES=web_page,web_site,household_demographics,catalog_page,call_center,promotion \
  starpump oracle
```

---

### 6-D — Row filter: `QUERY_FILTER` examples

`QUERY_FILTER` pushes a SQL `WHERE` predicate into the source query.  The filter is
**combined with the OFFSET resume cursor** — the predicate applies first, then OFFSET
counts rows within the filtered result set.  This means `QUERY_FILTER` is most useful
when you want a permanent scoped view (e.g. only active customers), not as a one-off
row count cap (use `MAX_ROWS` for that).

#### PostgreSQL

```bash
# Active customers only
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=public \
  INCLUDE_TABLES=customers \
  QUERY_FILTER="customers.is_active=true" \
  MAX_ROWS=1000 \
  starpump postgres

# PLATINUM tier customers only
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=public \
  INCLUDE_TABLES=customers \
  QUERY_FILTER="customers.tier='PLATINUM'" \
  MAX_ROWS=1000 \
  starpump postgres

# Products under $50
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=public \
  INCLUDE_TABLES=products \
  QUERY_FILTER="products.price<50" \
  MAX_ROWS=1000 \
  starpump postgres

# Clothing products with stock — table-level + schema-level combined
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=public \
  INCLUDE_TABLES=products \
  QUERY_FILTER="products.category='Clothing',stock_qty>0" \
  MAX_ROWS=1000 \
  starpump postgres
```

#### Oracle

```bash
# income_band: only rows with lower bound >= 40000
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=XEPDB1 SCHEMAS=TPCDS \
  INCLUDE_TABLES=income_band \
  QUERY_FILTER="income_band.IB_LOWER_BOUND>=40000" \
  starpump oracle
```

#### MongoDB

Simple `field OP value` predicates (`=` `!=` `<` `>` `<=` `>=`) are translated into
a MongoDB `$match` aggregation stage and run **server-side**. For anything more
complex use `MGO_PIPELINE`.

```bash
# Active customers only (server-side $match)
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=cache_testing \
  INCLUDE_TABLES=customers \
  QUERY_FILTER="customers.is_active=true" \
  MAX_ROWS=1000 \
  starpump mongodb

# Products under $50 (numeric comparison)
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=cache_testing \
  INCLUDE_TABLES=products \
  QUERY_FILTER="products.unit_price<50" \
  MAX_ROWS=1000 \
  starpump mongodb

# Complex filter: use MGO_PIPELINE for anything beyond simple field OP value
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=cache_testing \
  INCLUDE_TABLES=customers \
  MGO_PIPELINE='[{"$match":{"tier":"PLATINUM","credit_limit":{"$gte":5000}}},{"$limit":100000}]' \
  starpump mongodb
```

---

### 6-E — Parallel threads

```bash
# PostgreSQL: 4 threads instead of default 8
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=public \
  MAX_ROWS=1000 \
  starpump postgres --threads 4

# Oracle: single thread (safe for small tables)
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=XEPDB1 SCHEMAS=TPCDS \
  starpump oracle --threads 1
```

---

### 6-F — Custom batch size

`BATCH_SIZE` sets the SQL page size per iteration — it does **not** cap total rows.
Combine with `MAX_ROWS` to both control page size and total row count.

```bash
# PostgreSQL: 500-row pages, total cap 1000
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=public \
  INCLUDE_TABLES=products \
  BATCH_SIZE=500 \
  MAX_ROWS=1000 \
  starpump postgres

# MongoDB: 200-doc pages, total cap 1000
# (MAX_ROWS < BATCH_SIZE → effective_batch = MAX_ROWS = 1000 anyway;
#  use BATCH_SIZE < MAX_ROWS only when you want multiple small Iceberg commits)
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=cache_testing \
  INCLUDE_TABLES=products \
  BATCH_SIZE=200 \
  MAX_ROWS=1000 \
  starpump mongodb
```

---

### 6-G — Oracle: copy only non-empty tables

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=XEPDB1 SCHEMAS=TPCDS \
  INCLUDE_TABLES=income_band,ship_mode,warehouse,reason \
  starpump oracle
```

✅ Expected:
```
Discovered 4 tables in XEPDB1.TPCDS: ['income_band', 'reason', 'ship_mode', 'warehouse']
[income_band] DONE — 3 rows written
[ship_mode]   DONE — 2 rows written
[warehouse]   DONE — 1 rows written
[reason]      DONE — 1 rows written
Completed in ~8s — 4/4 copied | 0 skipped | 0 failed | 7 rows written
```

---

### 6-H — MongoDB: single collection copy

```bash
# Copy only the product_reviews collection
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=cache_testing \
  INCLUDE_TABLES=product_reviews \
  MAX_ROWS=1000 \
  starpump mongodb
```

---

### 6-I — Resume after crash (PostgreSQL / Oracle)

PostgreSQL and Oracle support `supports_offset_resume = True`. If a run is
interrupted, re-running the exact same command resumes from the last committed
Iceberg row count (offset).

```bash
# Run with small batch size — interrupt with Ctrl-C after the first batch logs
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=public \
  INCLUDE_TABLES=customers \
  BATCH_SIZE=10000 \
  starpump postgres
# (Ctrl-C after seeing: [customers] batch offset=0 rows=10000 total=10000)

# Re-run the same command — starpump resumes from offset=10000
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=public \
  INCLUDE_TABLES=customers \
  BATCH_SIZE=10000 \
  starpump postgres
```

✅ Resume log line:
```
[customers] RESUME: 10000 rows already in Iceberg — reusing extraction_ts=... starting at offset=10000.
```

> MongoDB does **not** support resume — it always re-reads from the start of the
> collection. Re-running a MongoDB job appends another batch.

---

## Step 7 — Query copied data from Iceberg (Spark)

Only needed when you want to inspect actual row content, not just counts.
**Spark session startup takes 30–60 seconds** — use Step 4 for quick count checks.

```bash
cat > /tmp/query_iceberg.py << 'PYEOF'
import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession

bao   = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf(app_name="query-iceberg")).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── PostgreSQL → Iceberg ───────────────────────────────────────────────────────
print("\n=== postgres.public.products (top 5 by price DESC) ===")
spark.sql("""
    SELECT sku, name, category, price, stock_qty, snap_id, snap_timestamp
    FROM   `postgres`.`public`.`products`
    ORDER  BY price DESC
    LIMIT  5
""").show(truncate=False)

# ── Oracle → Iceberg ───────────────────────────────────────────────────────────
print("\n=== oracle.tpcds.income_band (all rows) ===")
spark.sql("SELECT * FROM `oracle`.`tpcds`.`income_band`").show(truncate=False)

# ── MongoDB → Iceberg ─────────────────────────────────────────────────────────
print("\n=== mongodb.cache_testing.products (top 5 by unit_price DESC) ===")
spark.sql("""
    SELECT sku, product_name, category, unit_price, stock_qty, snap_id
    FROM   `mongodb`.`cache_testing`.`products`
    ORDER  BY unit_price DESC
    LIMIT  5
""").show(truncate=False)

spark.stop()
PYEOF

kubectl cp /tmp/query_iceberg.py prod/$MASTER:/tmp/query_iceberg.py -c spark-master
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir python3 /tmp/query_iceberg.py
```

---

## Troubleshooting

### `RuntimeError: Cannot authenticate to OpenBao`
No `TOKEN` env var set. Always pass `TOKEN=$TOKEN` (Step 2).

---

### `ValueError: No Spark external catalog registered for 'postgres'` / `oracle` / `mongodb`
The Polaris warehouse for that source doesn't exist yet. Run Step 1.

---

### `ClassNotFoundException: org.postgresql.Driver`
PostgreSQL JAR not in image. Check:
```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  find /opt/spark/jars -name "postgresql*.jar"
```
If missing, rebuild the image. The JAR `postgresql-42.7.4.jar` should be baked in
since image `3.5.1-2`.

---

### `ClassNotFoundException: oracle.jdbc.OracleDriver`
Oracle JAR not in image. Check:
```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  find /opt/spark/jars -name "ojdbc11*.jar"
```
If missing, hot-patch:
```bash
kubectl cp docker/spark-gluten-velox/jars/ojdbc11-23.4.0.24.05.jar \
  prod/$MASTER:/opt/spark/jars/ojdbc11-23.4.0.24.05.jar -c spark-master
```

---

### `ClassNotFoundException: com.mongodb.spark.sql.connector.MongoTableProvider`
MongoDB Spark connector JAR not in image. Check:
```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  find /opt/spark/jars -name "mongo-spark-connector*.jar"
```
If missing, hot-patch:
```bash
kubectl cp docker/spark-gluten-velox/jars/mongo-spark-connector_2.12-10.4.0-all.jar \
  prod/$MASTER:/opt/spark/jars/mongo-spark-connector_2.12-10.4.0-all.jar -c spark-master
```

---

### PostgreSQL: `ERROR: permission denied for table …`
The `rbac` user is missing SELECT grants. Re-run:
```bash
kubectl exec -n prod postgresql-0 -- psql -U postgres -d cache_testing -c \
  "GRANT SELECT ON ALL TABLES IN SCHEMA public TO rbac;
   ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO rbac;"
```

---

### Oracle: `ORA-00942: table or view does not exist`
Schema name is case-sensitive in Oracle. Always pass `SCHEMAS=TPCDS` (uppercase).
`SCHEMAS=tpcds` will find no tables.

---

### MongoDB copies 3× more rows than expected with `MAX_ROWS`
The MongoDB Spark connector applies `$limit` **per partition**, not globally. This
is fixed in starpump: when `effective_batch < 100 000`, a
`SinglePartitionPartitioner` is forced so the `$limit` is honoured exactly. If you
see 3× the expected rows, the pod is running an old `starpump.py`. Re-copy:
```bash
kubectl cp docker/spark-gluten-velox/scripts/starpump.py \
  prod/$MASTER:/opt/spark/work-dir/starpump.py -c spark-master
```

---

### MongoDB: all rows copied regardless of `QUERY_FILTER`
Simple `field OP value` predicates (=, !=, <, >, <=, >=) are translated into a
MongoDB `$match` aggregation stage server-side. If all rows are still being copied,
the predicate may have failed to parse — check the log for:
```
[mgo] Cannot parse QUERY_FILTER '...' into $match
```
For complex predicates (LIKE, IN, IS NULL, nested `$and`/`$or`), use `MGO_PIPELINE`:
```bash
MGO_PIPELINE='[{"$match":{"tier":"PLATINUM"}},{"$limit":100000}]' starpump mongodb
```

---

### `BATCH_SIZE=1000` copies the whole table instead of stopping at 1 000 rows
`BATCH_SIZE` is the SQL page size, **not** a total row cap. Use `MAX_ROWS=1000`
to hard-cap new rows written per run:
```bash
MAX_ROWS=1000 starpump postgres   # appends exactly 1 000 new rows then stops
```

---

### `ModuleNotFoundError: bao_spark_init` when running a `.py` script
Don't use `kubectl exec … -- python3 - << 'EOF'` — the heredoc is consumed by your
local shell. Always:
```bash
kubectl cp /tmp/myscript.py prod/$MASTER:/tmp/myscript.py -c spark-master
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN=$TOKEN PYTHONPATH=/opt/spark/work-dir python3 /tmp/myscript.py
```

---

## Key files

| File | Purpose |
|---|---|
| [`docker/spark-gluten-velox/scripts/starpump.py`](../../docker/spark-gluten-velox/scripts/starpump.py) | Pipeline core — `_pg_*`, `_ora_*`, `_mgo_*` connectors; `MAX_ROWS` cap; MongoDB single-partition fix |
| [`docker/spark-gluten-velox/scripts/bao_spark_init.py`](../../docker/spark-gluten-velox/scripts/bao_spark_init.py) | `postgres_jdbc_options()`, `oracle_jdbc_options()`, `mongodb_options()`, `pipeline_db_creds()` |
| [`docker/spark-gluten-velox/jars/postgresql-42.7.4.jar`](../../docker/spark-gluten-velox/jars/postgresql-42.7.4.jar) | PostgreSQL JDBC driver (baked since 3.5.1-2) |
| [`docker/spark-gluten-velox/jars/ojdbc11-23.4.0.24.05.jar`](../../docker/spark-gluten-velox/jars/ojdbc11-23.4.0.24.05.jar) | Oracle JDBC thin driver (baked since 3.5.1-4) |
| [`docker/spark-gluten-velox/jars/mongo-spark-connector_2.12-10.4.0-all.jar`](../../docker/spark-gluten-velox/jars/mongo-spark-connector_2.12-10.4.0-all.jar) | MongoDB Spark connector (baked since 3.5.1-4) |
