# Runbook 26 — Doris Write-Proxy: Manual Write Testing & Spark Resource Validation

| Field | Value |
|---|---|
| **Runbook ID** | RB-26 |
| **Service** | k8s-platform / doris-write-proxy |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-09-11 |
| **Related** | RB-25 (Cache Manager), RB-05 (Doris & Analytics), RB-13 (RBAC) |

---

## 1. Purpose

This runbook provides **10 manually executable write operations** (INSERT, UPDATE, DELETE)
against `polaris.tpcds_sf10tcl.customer_address` via the Doris write-proxy, along with
commands to verify that Spark releases all JVM heap, RDD, and broadcast memory between
each job — ensuring no resource starvation for subsequent writes.

### How resource cleanup works in `local[*]` mode

In `local[*]` mode the driver **is** the executor — there are no remote Spark workers.
After each `write_append()` call, the proxy explicitly:

1. `df.unpersist()` — removes the DataFrame's RDD from the block manager
2. `spark.catalog.clearCache()` — drops all cached tables/plans from the SQL cache
3. `System.gc()` — requests JVM garbage collection (advisory, not guaranteed)

This runs in a `finally` block on every write path — success or failure — so the JVM
heap is always reclaimed before the next job acquires the `_lock`.

```
write_append() call
       │
       ▼
  writeTo().append()  ──► Iceberg snapshot committed to S3
       │
       ▼  (finally block — always runs)
  df.unpersist()          ← RDD evicted from block manager
  catalog.clearCache()    ← SQL plan cache cleared
  System.gc()             ← JVM GC requested
       │
       ▼
  _lock released          ← next INSERT can proceed immediately
```

---

## 2. Prerequisites

```bash
# Set the Doris admin password once — used in all commands below
DORIS_PASS=$(kubectl get secret rbac-plane-credentials -n prod \
  -o jsonpath='{.data.DORIS_ADMIN_PASSWORD}' | base64 -d)

# Confirm the proxy is running and SparkSession is READY
kubectl logs -n prod -l app=doris-write-proxy --tail=5 \
  | grep -v "^26/\|WARNING\|execstack"

# Expected: last line contains "SparkSession: READY"
```

---

## 3. How to verify resource cleanup after each write

After each INSERT/UPDATE/DELETE below, run this to confirm the cleanup ran:

```bash
kubectl logs -n prod -l app=doris-write-proxy --tail=30 2>&1 \
  | grep -E "write_append SUCCESS|Spark SQL SUCCESS|Spark DML FAILED|post-write cleanup"
```

**Expected pattern per write job:**
```
write_append SUCCESS: polaris.tpcds_sf10tcl.customer_address elapsed=X.XXs rows=N
post-write cleanup: df unpersisted, catalog cache cleared, GC requested
```

For UPDATE/DELETE/MERGE the first line will show `Spark SQL SUCCESS` instead.

To watch JVM heap usage in real time while running the inserts, open a second terminal:

```bash
# Poll JVM heap via Spark's metrics endpoint (runs inside the pod)
POD=$(kubectl get pods -n prod -l app=doris-write-proxy -o jsonpath='{.items[0].metadata.name}')
watch -n2 "kubectl exec -n prod $POD -- python3 -c \"
import subprocess, re
out = subprocess.check_output(['jcmd','1','GC.heap_info'], stderr=subprocess.DEVNULL, text=True)
print(out)
\" 2>/dev/null || echo 'jcmd unavailable — check proxy logs instead'"
```

If `jcmd` is not available in the image, the proxy debug log line is the primary signal.

---

## 4. Write Operations

Connect to the proxy on port **30091** (write path) for all DML.
Use port **30090** (Doris direct) for SELECT verification.

---

### Write 1 — INSERT: 1 row, all columns

**Purpose:** Schema cold-start (cache MISS). Expect ~5–8 s elapsed.

```bash
time mysql -h 192.168.1.50 -P 30091 -u admin -p"${DORIS_PASS}" --connect-timeout=60 \
  -e "INSERT INTO polaris.tpcds_sf10tcl.customer_address
        (ca_address_sk, ca_street_number, ca_street_name, ca_city, ca_state, ca_zip, ca_country)
      VALUES (9200001, '10', 'Maple Ave', 'Chicago', 'IL', '60601', 'US');"
```

**Expected log:**
```
Schema cache MISS for polaris.tpcds_sf10tcl.customer_address — 15 fields
write_append SUCCESS: polaris.tpcds_sf10tcl.customer_address elapsed=X.XXs rows=1
post-write cleanup: df unpersisted, catalog cache cleared, GC requested
```

---

### Write 2 — INSERT: 5 rows, all columns (schema cached)

**Purpose:** Warm path — schema HIT. Expect <3 s elapsed.

```bash
time mysql -h 192.168.1.50 -P 30091 -u admin -p"${DORIS_PASS}" --connect-timeout=60 \
  -e "INSERT INTO polaris.tpcds_sf10tcl.customer_address
        (ca_address_sk, ca_street_number, ca_street_name, ca_city, ca_state, ca_zip, ca_country)
      VALUES
        (9200002, '22', 'Elm St',      'Dallas',   'TX', '75201', 'US'),
        (9200003, '7',  'Oak Blvd',    'Portland', 'OR', '97201', 'US'),
        (9200004, '55', 'Pine Rd',     'Phoenix',  'AZ', '85001', 'US'),
        (9200005, '3',  'Cedar Ln',    'Atlanta',  'GA', '30301', 'US'),
        (9200006, '88', 'Birch Ct',    'Detroit',  'MI', '48201', 'US');"
```

---

### Write 3 — INSERT: partial columns (NULLs padded for missing cols)

**Purpose:** Verify that omitted columns are padded as NULL by the proxy, not rejected.

```bash
time mysql -h 192.168.1.50 -P 30091 -u admin -p"${DORIS_PASS}" --connect-timeout=60 \
  -e "INSERT INTO polaris.tpcds_sf10tcl.customer_address
        (ca_address_sk, ca_city, ca_state, ca_zip, ca_country)
      VALUES
        (9200007, 'Nashville',  'TN', '37201', 'US'),
        (9200008, 'Louisville', 'KY', '40201', 'US');"
```

**Expected log:** `write_append: ... 13 col(s), 2 row(s) [schema cached]`
Columns `ca_street_number`, `ca_street_name`, `ca_suite_number`, etc. will be NULL.

---

### Write 4 — INSERT: 20 rows (medium batch)

**Purpose:** Confirm cleanup frees memory before the next job. Expect <3 s elapsed.

```bash
time mysql -h 192.168.1.50 -P 30091 -u admin -p"${DORIS_PASS}" --connect-timeout=60 \
  -e "INSERT INTO polaris.tpcds_sf10tcl.customer_address
        (ca_address_sk, ca_street_number, ca_street_name, ca_city, ca_state, ca_zip, ca_country)
      VALUES
        (9200020,'1','Bench St','RandomCity','NY','10001','US'),
        (9200021,'2','Bench St','RandomCity','NY','10001','US'),
        (9200022,'3','Bench St','RandomCity','NY','10001','US'),
        (9200023,'4','Bench St','RandomCity','NY','10001','US'),
        (9200024,'5','Bench St','RandomCity','NY','10001','US'),
        (9200025,'6','Bench St','RandomCity','NY','10001','US'),
        (9200026,'7','Bench St','RandomCity','NY','10001','US'),
        (9200027,'8','Bench St','RandomCity','NY','10001','US'),
        (9200028,'9','Bench St','RandomCity','NY','10001','US'),
        (9200029,'10','Bench St','RandomCity','NY','10001','US'),
        (9200030,'11','Bench St','RandomCity','NY','10001','US'),
        (9200031,'12','Bench St','RandomCity','NY','10001','US'),
        (9200032,'13','Bench St','RandomCity','NY','10001','US'),
        (9200033,'14','Bench St','RandomCity','NY','10001','US'),
        (9200034,'15','Bench St','RandomCity','NY','10001','US'),
        (9200035,'16','Bench St','RandomCity','NY','10001','US'),
        (9200036,'17','Bench St','RandomCity','NY','10001','US'),
        (9200037,'18','Bench St','RandomCity','NY','10001','US'),
        (9200038,'19','Bench St','RandomCity','NY','10001','US'),
        (9200039,'20','Bench St','RandomCity','NY','10001','US');"
```

---

### Write 5 — INSERT: only 3 columns supplied

**Purpose:** Most-sparse insert — only PK + city + country. All other cols NULL.

```bash
time mysql -h 192.168.1.50 -P 30091 -u admin -p"${DORIS_PASS}" --connect-timeout=60 \
  -e "INSERT INTO polaris.tpcds_sf10tcl.customer_address
        (ca_address_sk, ca_city, ca_country)
      VALUES
        (9200050, 'Los Angeles', 'US'),
        (9200051, 'San Jose',    'US'),
        (9200052, 'San Diego',   'US');"
```

---

### Write 6 — INSERT: 100 rows (large batch)

**Purpose:** Larger batch — verify cleanup still frees everything before write 7.

```bash
# Generate the SQL with Python then pipe to mysql
python3 -c "
rows = [
    f\"(920{1000+i}, '{i}', 'Load Ave', 'LoadCity', 'TX', '77001', 'US')\"
    for i in range(100)
]
sql = ('INSERT INTO polaris.tpcds_sf10tcl.customer_address '
       '(ca_address_sk, ca_street_number, ca_street_name, ca_city, ca_state, ca_zip, ca_country) '
       'VALUES ' + ', '.join(rows) + ';')
print(sql)
" | time mysql -h 192.168.1.50 -P 30091 -u admin -p"${DORIS_PASS}" --connect-timeout=60
```

---

### Write 7 — UPDATE: change city for a range of rows

**Purpose:** Test UPDATE path (falls through to `spark.sql()` — not `write_append()`).
Iceberg UPDATE rewrites affected data files in place.

> **Note:** Iceberg UPDATE via Spark requires `MERGE INTO` semantics under the hood.
> The proxy routes this through `spark.sql(stmt)` after `USE catalog.db`.

```bash
time mysql -h 192.168.1.50 -P 30091 -u admin -p"${DORIS_PASS}" --connect-timeout=60 \
  -e "UPDATE polaris.tpcds_sf10tcl.customer_address
      SET    ca_city = 'Updated City',
             ca_state = 'ZZ'
      WHERE  ca_address_sk BETWEEN 9200020 AND 9200029;"
```

**Expected log:**
```
WriteProxy: intercepted polaris.tpcds_sf10tcl.customer_address DML — routing to Spark.
Spark SQL SUCCESS: polaris.tpcds_sf10tcl elapsed=X.XXs rows=0
post-write cleanup: df unpersisted, catalog cache cleared, GC requested
```

> `rows=0` is expected for UPDATE/DELETE — Spark SQL returns an empty DataFrame
> for these statements; the actual affected count is tracked in the Iceberg snapshot.

---

### Write 8 — DELETE: remove rows by PK range

**Purpose:** Test DELETE path via `spark.sql()`.

```bash
time mysql -h 192.168.1.50 -P 30091 -u admin -p"${DORIS_PASS}" --connect-timeout=60 \
  -e "DELETE FROM polaris.tpcds_sf10tcl.customer_address
      WHERE ca_address_sk BETWEEN 9200050 AND 9200052;"
```

**Expected log:**
```
Spark SQL SUCCESS: polaris.tpcds_sf10tcl elapsed=X.XXs rows=0
post-write cleanup: df unpersisted, catalog cache cleared, GC requested
```

---

### Write 9 — UPDATE: single row by exact PK

**Purpose:** Targeted single-row UPDATE to verify precision of Iceberg row-level update.

```bash
time mysql -h 192.168.1.50 -P 30091 -u admin -p"${DORIS_PASS}" --connect-timeout=60 \
  -e "UPDATE polaris.tpcds_sf10tcl.customer_address
      SET    ca_street_name = 'Corrected Blvd',
             ca_zip         = '99999'
      WHERE  ca_address_sk = 9200001;"
```

---

### Write 10 — INSERT: 1000 rows (stress test)

**Purpose:** Maximum throughput test. Verify cleanup still completes cleanly after a
large write and JVM heap returns to baseline.

```bash
python3 -c "
rows = [
    f\"(920{2000+i}, '{i}', 'Scale Blvd', 'BigCity', 'FL', '33001', 'US')\"
    for i in range(1000)
]
sql = ('INSERT INTO polaris.tpcds_sf10tcl.customer_address '
       '(ca_address_sk, ca_street_number, ca_street_name, ca_city, ca_state, ca_zip, ca_country) '
       'VALUES ' + ', '.join(rows) + ';')
print(sql)
" | time mysql -h 192.168.1.50 -P 30091 -u admin -p"${DORIS_PASS}" --connect-timeout=60
```

**Expected elapsed: <5 s** (1000 rows, schema cached, cleanup included).

---

## 5. Verify all data landed in Iceberg

Run this after all 10 writes (port **30090**, Doris direct):

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
  -e "REFRESH CATALOG polaris;
      SELECT
        CASE
          WHEN ca_address_sk BETWEEN 9200001 AND 9200009 THEN 'writes 1-3'
          WHEN ca_address_sk BETWEEN 9200020 AND 9200039 THEN 'write 4'
          WHEN ca_address_sk BETWEEN 9200050 AND 9200052 THEN 'write 5 (deleted by w8)'
          WHEN ca_address_sk BETWEEN 9201000 AND 9201099 THEN 'write 6'
          WHEN ca_address_sk BETWEEN 9202000 AND 9202999 THEN 'write 10'
          ELSE 'other'
        END                     AS batch,
        COUNT(*)                AS rows,
        MIN(snap_timestamp)     AS first_snap,
        MAX(snap_timestamp)     AS last_snap
      FROM polaris.tpcds_sf10tcl.customer_address
      WHERE ca_address_sk >= 9200001
      GROUP BY 1
      ORDER BY MIN(ca_address_sk);"
```

**Expected:** Writes 1–3 rows present; write 5 rows absent (deleted by Write 8);
write 7 & 9 show `ca_city='Updated City'` / `ca_zip='99999'` on the targeted rows.

Spot-check Write 7 UPDATE and Write 9 single-row UPDATE:

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
  -e "SELECT ca_address_sk, ca_city, ca_state, ca_zip, snap_timestamp
      FROM polaris.tpcds_sf10tcl.customer_address
      WHERE ca_address_sk IN (9200020, 9200025, 9200029, 9200001)
      ORDER BY ca_address_sk;"
```

Expected:
- `9200020`–`9200029` → `ca_city='Updated City'`, `ca_state='ZZ'`
- `9200001` → `ca_street_name='Corrected Blvd'`, `ca_zip='99999'`

---

## 6. Full cleanup verification (after all 10 writes)

```bash
# Should show 10× "write_append SUCCESS" or "Spark SQL SUCCESS"
# followed by 10× "post-write cleanup"
kubectl logs -n prod -l app=doris-write-proxy --tail=120 2>&1 \
  | grep -E "SUCCESS|FAILED|post-write cleanup" \
  | grep -v "^26/"
```

**Expected output pattern:**
```
write_append SUCCESS: polaris.tpcds_sf10tcl.customer_address elapsed=X.XXs rows=1
post-write cleanup: df unpersisted, catalog cache cleared, GC requested
write_append SUCCESS: polaris.tpcds_sf10tcl.customer_address elapsed=X.XXs rows=5
post-write cleanup: df unpersisted, catalog cache cleared, GC requested
...  (×10 total)
```

No `FAILED` lines should appear. If any write fails, see §7 below.

---

## 7. Troubleshooting

### Write returns error: `SparkSession unavailable`

```bash
kubectl logs -n prod -l app=doris-write-proxy --tail=20 2>&1 \
  | grep -v "^26/\|WARNING\|execstack"
# Look for "SparkSession: INIT FAILED" — restart the pod
kubectl rollout restart deployment/doris-write-proxy -n prod
```

### UPDATE / DELETE returns Spark error about `MERGE INTO`

Iceberg row-level UPDATE/DELETE requires the table to have been created with
`'write.delete.mode'='merge-on-read'` or `'copy-on-write'`.
Check table properties:

```bash
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
  -e "SELECT * FROM polaris.tpcds_sf10tcl.customer_address\$properties;"
```

If the property is missing, run via Spark directly:
```sql
ALTER TABLE polaris.tpcds_sf10tcl.customer_address
SET TBLPROPERTIES (
  'write.update.mode' = 'merge-on-read',
  'write.delete.mode' = 'merge-on-read',
  'write.merge.mode'  = 'merge-on-read'
);
```

### `post-write cleanup` line never appears

Cleanup runs at `DEBUG` level — confirm the proxy log level allows it, or just
rely on `write_append SUCCESS` appearing without memory errors in subsequent writes.

### Memory pressure between writes (OOMKilled pod)

The proxy pod has no memory limit set by default.  If the pod is OOMKilled during
a large batch, set a driver memory limit in the deployment:

```bash
kubectl set env deployment/doris-write-proxy -n prod SPARK_DRIVER_MEMORY=6g
# or edit the deployment and add to _build_spark_conf:
#   conf.set("spark.driver.memory", "6g")
```

---

## 8. Expected timing summary

| Write | Operation | Rows | Expected elapsed |
|---|---|---|---|
| 1 | INSERT (schema MISS) | 1 | 5–8 s |
| 2 | INSERT (schema HIT) | 5 | < 3 s |
| 3 | INSERT partial cols | 2 | < 3 s |
| 4 | INSERT medium batch | 20 | < 3 s |
| 5 | INSERT sparse cols | 3 | < 3 s |
| 6 | INSERT large batch | 100 | < 3 s |
| 7 | UPDATE range | 10 rows affected | < 5 s |
| 8 | DELETE range | 3 rows removed | < 5 s |
| 9 | UPDATE single row | 1 row affected | < 5 s |
| 10 | INSERT stress | 1000 | < 5 s |

> UPDATE and DELETE are slower than INSERT because Spark must read, rewrite, and
> commit affected Iceberg data files (copy-on-write) rather than simply appending
> a new file.  Schema MISS on Write 1 includes a `DESCRIBE TABLE` REST call (~1 s).
