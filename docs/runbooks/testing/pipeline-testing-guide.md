# Pipeline Testing Guide

**Audience:** Platform engineers manually validating the Snowflake → Iceberg copy pipeline and Oracle CDC → Iceberg replication.
**Cluster:** `192.168.1.50` (Spark master UI: `http://192.168.1.52:30707`)
**Debezium Connect:** `http://192.168.1.54:30083`
**OpenBao:** `http://192.168.1.50:30820`

---

## Before You Start — Common Setup

> **After any image rebuild or pod rollout**, re-run the "Common Setup" block below to refresh `$MASTER` — the pod name changes after every restart and the old variable points to a pod that no longer exists. The scripts (`bao_spark_init.py`, `spark_iceberg_utils.py`, `snowflake_to_iceberg.py`) are baked into the image at `/opt/spark/work-dir/` and do **not** need to be copied manually.
>
> The pipeline is invoked via the `starpump` CLI command available directly on `PATH`:
> ```bash
> starpump snowflake              # copy from Snowflake (default 8 threads)
> starpump snowflake --threads 16 # use 16 parallel threads
> starpump snowflake --threads 32 # use 32 parallel threads
> # equivalent to: python3 /opt/spark/work-dir/snowflake_to_iceberg.py snowflake
> ```
> The first argument (`snowflake`) selects the source database driver.
> Additional sources (e.g. `oracle`, `postgres`) can be added to the pipeline
> later — just pass a different name.
>
> To rebuild and roll the pods:
> ```bash
> bash docker/spark-gluten-velox/build-and-push.sh
> kubectl rollout restart deployment/spark-master deployment/spark-worker -n prod
> kubectl rollout status  deployment/spark-master -n prod
> kubectl rollout status  deployment/spark-worker -n prod
> ```

> **`-i` flag for heredoc commands:** Any `kubectl exec` command that pipes a Python
> script via `<<'EOF'` requires the `-i` flag so kubectl wires up stdin to the remote
> process. Without `-i` the heredoc is silently discarded and you get no output.

> **`env` pattern:** All `starpump` exec blocks pass credentials and options via `env KEY=VAL`
> directly to `starpump` — no shell wrapper, no quoting complexity.  `$TOKEN` and `$ADDR`
> are expanded by your local shell before `kubectl` sends the command, so the remote pod
> receives the actual values.  Extra options (`DATABASE`, `SCHEMAS`, `INCLUDE_TABLES`, …)
> are also passed as `env` vars so any combination can be composed without quoting issues.

> **All variables below (`$MASTER`, `$TOKEN`, `$PG_HOST`, `$PG_PASS`, `$PGPOD`) are
> shell session variables** — they are lost when you open a new terminal or start a new
> `kubectl exec` session. **Re-run the entire setup block at the top of every new
> terminal before running any test.**

Run these once in your terminal before any test. All tests below assume these variables are set.

```bash
# Spark master pod — wait until all containers are Ready before proceeding
# The pod label is component=master (not app=spark-master).
MASTER=$(kubectl get pod -n prod -l component=master \
  -o jsonpath='{.items[0].metadata.name}')
echo "Spark master pod: $MASTER"

kubectl wait pod -n prod "$MASTER" --for=condition=Ready --timeout=120s
echo "Pod is Ready."

# OpenBao root token (stored as $TOKEN — used as TOKEN= in all starpump calls)
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

# OpenBao in-cluster address (stored as $ADDR — used as ADDR= in all starpump calls)
ADDR="http://openbao.prod.svc.cluster.local:8200"

# Pipeline PostgreSQL credentials
PG_HOST=$(kubectl exec -n prod $MASTER -c spark-master -- \
  curl -sf -H "X-Vault-Token: $TOKEN" \
  ${ADDR}/v1/secret/data/platform/pipeline_db \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['data']['data']['host'])")
PG_PASS=$(kubectl exec -n prod $MASTER -c spark-master -- \
  curl -sf -H "X-Vault-Token: $TOKEN" \
  ${ADDR}/v1/secret/data/platform/pipeline_db \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['data']['data']['password'])")

# PostgreSQL pod — psql is not installed on the host; run all psql commands
# through kubectl exec on the postgresql-0 pod instead.
PGPOD=$(kubectl get pod -n prod -l app=postgresql \
  -o jsonpath='{.items[0].metadata.name}')
echo "PostgreSQL pod: $PGPOD"

echo "Setup complete."
```

---

## Test A — Snowflake Copy: Database/Schema, Single Table, Include/Exclude, Size Cap

### A.1 Copy a single table from a specific database and schema

Copy only `promotion` from `SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL`.  
`MAX_TABLE_SIZE_GB=0` disables the size filter so even a table reported as 0 bytes is included.

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN="$TOKEN" ADDR="$ADDR" USER=dave \
      DATABASE=SNOWFLAKE_SAMPLE_DATA SCHEMAS=TPCDS_SF10TCL \
      INCLUDE_TABLES=promotion MAX_TABLE_SIZE_GB=0 \
  starpump snowflake
```

**What to look for in the logs:**

```
[size-report] promotion   →    0.0 GB  (COPY)      ← only this table listed
Copying 1/24 table(s) with 8 threads …
[promotion] sf_extraction_ts=2026-…Z (CDC sync point)
[promotion] Early watermark written to pipeline DB.
[promotion] batch offset=0 rows=1000 total=1000
[promotion] DONE — 1000 rows written (total incl. prior runs).
Completed in X.Xs — 1/24 copied | 23 skipped (filtered) | 0 failed | 1000 rows written
```

**Verify in Iceberg:**

```bash
kubectl exec -n prod -i $MASTER -c spark-master -- \
  env USER=dave TOKEN="$TOKEN" ADDR="$ADDR" \
  python3 - <<'EOF'
import sys; sys.path.insert(0, '/opt/spark/work-dir')
from pyspark.sql import SparkSession
from bao_spark_init import BaoSparkInit
bao = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf()).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("Row count:", spark.sql("SELECT COUNT(*) FROM polaris.tpcds_sf10tcl.promotion").collect()[0][0])
spark.sql("SELECT * FROM polaris.tpcds_sf10tcl.promotion LIMIT 5").show()
EOF
```

---

### A.2 Copy multiple tables using INCLUDE_TABLES

Copy `reason`, `warehouse`, and `ship_mode` in one run:

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN="$TOKEN" ADDR="$ADDR" USER=dave \
      DATABASE=SNOWFLAKE_SAMPLE_DATA SCHEMAS=TPCDS_SF10TCL \
      INCLUDE_TABLES=reason,warehouse,ship_mode MAX_TABLE_SIZE_GB=0 \
  starpump snowflake
```

**Expected size-report:**

```
[size-report] reason      →    0.0 GB  (COPY)
[size-report] ship_mode   →    0.0 GB  (COPY)
[size-report] warehouse   →    0.0 GB  (COPY)
[size-report] <all others>               (SKIP — not in INCLUDE_TABLES)
```

---

### A.3 Use EXCLUDE_TABLES to skip specific tables

Copy all tables ≤ 1 GB but skip `catalog_returns` and `store_returns`:

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN="$TOKEN" ADDR="$ADDR" USER=dave \
      DATABASE=SNOWFLAKE_SAMPLE_DATA SCHEMAS=TPCDS_SF10TCL \
      EXCLUDE_TABLES=catalog_returns,store_returns MAX_TABLE_SIZE_GB=1.0 \
  starpump snowflake
```

**Expected size-report (excerpt):**

```
[size-report] catalog_returns   →   1.5 GB  (SKIP — EXCLUDE_TABLES)
[size-report] store_returns     →   2.8 GB  (SKIP — EXCLUDE_TABLES)
[size-report] store_sales       →  22.1 GB  (SKIP — 22.1 GB exceeds 1.0 GB limit)
[size-report] customer          →   0.5 GB  (COPY)
```

---

### A.4 Enforce a 1 GB max table size cap

Copy only tables whose compressed Snowflake size is ≤ 1 GB:

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN="$TOKEN" ADDR="$ADDR" USER=dave \
      DATABASE=SNOWFLAKE_SAMPLE_DATA SCHEMAS=TPCDS_SF10TCL \
      MAX_TABLE_SIZE_GB=1.0 \
  starpump snowflake
```

Tables such as `store_sales` (~22 GB), `catalog_sales` (~18 GB), `web_sales` (~9 GB),
`inventory` (~7 GB), `web_returns` (~3.5 GB), `store_returns` (~2.8 GB), and
`catalog_returns` (~1.5 GB) will all appear as `SKIP` in the size-report.

**Verify the pipeline DB watermarks for only the copied tables:**

```bash
kubectl exec -n prod $PGPOD -- \
  env PGPASSWORD="$PG_PASS" \
  psql -h "$PG_HOST" -U pipeline -d pipeline \
  -c "SELECT table_name, rows_copied, sf_extraction_ts
      FROM pipeline_watermarks
      WHERE source_db='SNOWFLAKE_SAMPLE_DATA' AND source_schema='TPCDS_SF10TCL'
      ORDER BY table_name;"
```

---

## Test B — Kill Mid-Copy and Verify Resume

This test proves the pipeline resumes from where it left off — not from the beginning.

### B.1 Start a copy of a larger table (so it takes multiple batches)

Use `customer` (~0.5 GB, ~300k–500k rows) so it takes several batches to complete.
Set `BATCH_SIZE=50000` to create more batches and give a wider window to kill it.

**Terminal 1 — start the job:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  sh -c "TOKEN='$TOKEN' ADDR='$ADDR' USER=dave \
    INCLUDE_TABLES=customer MAX_TABLE_SIZE_GB=0 BATCH_SIZE=50000 \
    starpump snowflake 2>&1 | tee /tmp/copy_run1.log"
```

Watch for the first few batch lines:

```
[customer] batch offset=0    rows=50000 total=50000
[customer] batch offset=50000 rows=50000 total=100000
[customer] batch offset=100000 rows=50000 total=150000
```

### B.2 Kill the job after 2–3 batches have landed

**Terminal 2 — find and kill the process:**

```bash
# Find the python3 PID inside the pod
kubectl exec -n prod $MASTER -c spark-master -- \
  pgrep -f snowflake_to_iceberg.py

# Kill it (replace <PID>)
kubectl exec -n prod $MASTER -c spark-master -- kill -9 <PID>
```

You will see the job die mid-run in Terminal 1.

### B.3 Check what landed in Iceberg and in the pipeline DB before the kill

```bash
# How many rows are in Iceberg
kubectl exec -n prod -i $MASTER -c spark-master -- \
  env USER=dave TOKEN="$TOKEN" ADDR="$ADDR" \
  python3 - <<'EOF'
import sys; sys.path.insert(0, '/opt/spark/work-dir')
from pyspark.sql import SparkSession
from bao_spark_init import BaoSparkInit
bao  = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf()).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
n = spark.sql("SELECT COUNT(*) FROM polaris.tpcds_sf10tcl.customer").collect()[0][0]
print(f"Rows in Iceberg before resume: {n}")
EOF

# What watermark was written in the pipeline DB
kubectl exec -n prod $PGPOD -- \
  env PGPASSWORD="$PG_PASS" \
  psql -h "$PG_HOST" -U pipeline -d pipeline \
  -c "SELECT table_name, sf_extraction_ts, rows_copied, oracle_start_scn
      FROM pipeline_watermarks
      WHERE table_name='customer';"
```

**Expected:** `rows_copied = 0` (the early watermark write sets rows=0) but
`sf_extraction_ts` is populated — this is the timestamp that will be reused on resume.

### B.4 Restart the job and observe resume behaviour

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  sh -c "TOKEN='$TOKEN' ADDR='$ADDR' USER=dave \
    INCLUDE_TABLES=customer MAX_TABLE_SIZE_GB=0 BATCH_SIZE=50000 \
    starpump snowflake 2>&1 | tee /tmp/copy_run2.log"
```

**What to look for — proof of resume:**

```
[customer] RESUME: 150000 rows already in Iceberg — reusing
           sf_extraction_ts=2026-…Z from pipeline DB, starting at offset=150000.
[customer] batch offset=150000 rows=50000 total=200000   ← picks up exactly here
[customer] batch offset=200000 rows=50000 total=250000
…
[customer] DONE — 450000 rows written (total incl. prior runs).
```

Key things to confirm:
1. The first batch offset equals the row count from B.3 (no gap, no overlap).
2. `sf_extraction_ts` in the log matches the one from B.3 (same timestamp reused).
3. The final `rows_copied` in the pipeline DB now equals the full table row count.

### B.5 Confirm no duplicate rows

```bash
kubectl exec -n prod -i $MASTER -c spark-master -- \
  env USER=dave TOKEN="$TOKEN" ADDR="$ADDR" \
  python3 - <<'EOF'
import sys; sys.path.insert(0, '/opt/spark/work-dir')
from pyspark.sql import SparkSession
from bao_spark_init import BaoSparkInit
bao  = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf()).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
# Total rows
total = spark.sql("SELECT COUNT(*) FROM polaris.tpcds_sf10tcl.customer").collect()[0][0]
# Distinct rows on the primary key column (c_customer_sk)
distinct = spark.sql("SELECT COUNT(DISTINCT c_customer_sk) FROM polaris.tpcds_sf10tcl.customer").collect()[0][0]
print(f"Total rows : {total}")
print(f"Distinct sk: {distinct}")
print("PASS — no duplicates" if total == distinct else f"FAIL — {total - distinct} duplicates")
EOF
```

---

## Test C — Verify Parallel Thread Count

The default thread count is **8**.  Pass `--threads N` to increase it:

```bash
starpump snowflake --threads 16   # 16 parallel threads
starpump snowflake --threads 32   # 32 parallel threads
```

### C.1 Run a multi-table copy and watch thread activity

Copy 8 small tables simultaneously to see all threads active at once.
The `--threads 8` flag is shown explicitly here; omitting it gives the same
result since 8 is the default:

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  sh -c "TOKEN='$TOKEN' ADDR='$ADDR' USER=dave \
    INCLUDE_TABLES=income_band,ship_mode,warehouse,reason,call_center,web_site,web_page,promotion \
    MAX_TABLE_SIZE_GB=0 \
    starpump snowflake --threads 8 2>&1 | tee /tmp/copy_threads.log"
```

### C.2 Confirm 8 threads in the logs

```bash
# Count distinct thread names in the log output
grep -oP '\[copy-worker-\d+\]' /tmp/copy_threads.log | sort -u
```

**Expected output:**

```
[copy-worker-1]
[copy-worker-2]
[copy-worker-3]
[copy-worker-4]
[copy-worker-5]
[copy-worker-6]
[copy-worker-7]
[copy-worker-8]
```

### C.3 See which thread handled which table

```bash
grep -P '\[copy-worker-\d+\].*START' /tmp/copy_threads.log
```

Example output:

```
[copy-worker-1] [income_band]      START: 0.0 GB | discovering schema …
[copy-worker-2] [ship_mode]        START: 0.0 GB | discovering schema …
[copy-worker-3] [warehouse]        START: 0.0 GB | discovering schema …
…
```

### C.4 Confirm thread reuse (worker picks up next table)

With 8 tables and 8 threads all threads start at the same time.
To see a thread pick up a second table, run with 9+ tables and only 4 threads
using `--threads 4`:

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  sh -c "TOKEN='$TOKEN' ADDR='$ADDR' USER=dave \
    INCLUDE_TABLES=income_band,ship_mode,warehouse,reason,call_center,web_site,web_page,promotion,catalog_page \
    MAX_TABLE_SIZE_GB=0 \
    starpump snowflake --threads 4 2>&1 | grep START"
```

You will see 4 threads start first; as each finishes it picks up the 5th, 6th, … table.

---

## Test D — Debezium CDC: Add a Table, Run DML, Verify Kafka → Iceberg

### D.1 Verify Debezium and Iceberg Sink connectors are running

```bash
# List all connectors
curl -s http://192.168.1.54:30083/connectors | python3 -m json.tool

# Check Debezium Oracle connector status
curl -s http://192.168.1.54:30083/connectors/oracle-tpcds-cdc/status | python3 -m json.tool

# Check Iceberg Sink connector status
curl -s http://192.168.1.54:30083/connectors/iceberg-sink-tpcds/status | python3 -m json.tool
```

**Expected:** Both connectors show `"state": "RUNNING"` for the connector and all tasks.

---

### D.2 Insert, update, and delete rows in Oracle and confirm in Kafka

**Connect to Oracle:**

```bash
kubectl exec -n prod deploy/oracle-xe -- \
  sqlplus tpcds/TpcdsPwd123!@XEPDB1
```

**Run DML operations:**

```sql
-- INSERT a new row
INSERT INTO reason VALUES (100, 'AAAAAAAATEST0001', 'Test reason for CDC validation');
COMMIT;

-- UPDATE the row
UPDATE reason SET r_reason_desc = 'Updated CDC test reason' WHERE r_reason_sk = 100;
COMMIT;

-- DELETE the row
DELETE FROM reason WHERE r_reason_sk = 100;
COMMIT;

EXIT;
```

**Watch the Kafka topic for these events (in a second terminal):**

```bash
# Get a Kafka broker pod
KAFKA_POD=$(kubectl get pod -n prod -l strimzi.io/name=strimzi-kafka-kafka \
  -o jsonpath='{.items[0].metadata.name}')

# Consume from the reason CDC topic (latest Avro messages — shown as JSON summary)
kubectl exec -n prod $KAFKA_POD -- \
  bin/kafka-console-consumer.sh \
  --bootstrap-server strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092 \
  --topic oracle-tpcds.TPCDS.REASON \
  --from-beginning \
  --timeout-ms 15000 \
  --property print.key=true 2>/dev/null | head -40
```

> **Note:** Messages are Avro-encoded. You will see binary output. To decode them
> properly use the Schema Registry Avro console consumer or check the Debezium
> Connect log for event counts:
>
> ```bash
> kubectl logs -n prod deploy/debezium-connect --tail=50 | grep REASON
> ```

**Check Kafka topic offsets (proves messages landed):**

```bash
kubectl exec -n prod $KAFKA_POD -- \
  bin/kafka-consumer-groups.sh \
  --bootstrap-server strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092 \
  --describe --group iceberg-sink-tpcds-ctrl 2>/dev/null | grep REASON
```

**Expected:** The `LOG-END-OFFSET` for `oracle-tpcds.TPCDS.REASON` increases by 3
(one message per DML statement).

---

### D.3 Verify DML replicated into Iceberg

Wait ~35 seconds for the Iceberg Sink's 30-second commit interval to flush.

```bash
kubectl exec -n prod -i $MASTER -c spark-master -- \
  env USER=dave TOKEN="$TOKEN" ADDR="$ADDR" \
  python3 - <<'EOF'
import sys; sys.path.insert(0, '/opt/spark/work-dir')
from pyspark.sql import SparkSession
from bao_spark_init import BaoSparkInit
bao  = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf()).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Current data state (row 100 should be gone after DELETE)
print("=== Current rows in reason ===")
spark.sql("SELECT * FROM polaris.tpcds.reason ORDER BY r_reason_sk").show(truncate=False)

# Iceberg snapshot history — shows each CDC commit as a snapshot
print("=== Iceberg snapshot history ===")
spark.sql("""
  SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added,
         summary['deleted-records'] AS deleted
  FROM   polaris.tpcds.reason.snapshots
  ORDER BY committed_at DESC
  LIMIT 10
""").show(truncate=False)
EOF
```

**Expected:**
- Row `r_reason_sk=100` is absent (DELETE was applied).
- Snapshot history shows 3 new snapshots after the DML (or 1 if batched into one commit):
  one with `added-records=1` (INSERT), one with `added-records=1 / deleted-records=1` (UPDATE as upsert), one with `deleted-records=1` (DELETE).

---

## Test E — Oracle DDL: Schema Evolution → Auto-replicated to Iceberg

This test verifies that an `ALTER TABLE` on Oracle propagates automatically to the
Iceberg table via the schema evolution handler — no manual intervention, no outage.

### E.1 Start the schema evolution handler (if not already running)

Copy the handler scripts onto the pod first:

```bash
for f in bao_spark_init.py spark_iceberg_utils.py \
          docs/runbooks/oracle-cdc-iceberg/04_schema_evolution_handler.py; do
  kubectl cp $f prod/$MASTER:/opt/spark/work-dir/$(basename $f) -c spark-master
done
```

**Start the handler in the background (Terminal 2):**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN="$TOKEN" ADDR="$ADDR" \
  python3 /opt/spark/work-dir/04_schema_evolution_handler.py 2>&1 | tee /tmp/schema_handler.log &
```

Wait for the handler to subscribe:

```bash
grep -m1 "Subscribed to topic" /tmp/schema_handler.log
# Expected: Subscribed to topic: schema-changes.oracle-tpcds
```

### E.2 Describe the Iceberg table before DDL (baseline)

```bash
kubectl exec -n prod -i $MASTER -c spark-master -- \
  env USER=dave TOKEN="$TOKEN" ADDR="$ADDR" \
  python3 - <<'EOF'
import sys; sys.path.insert(0, '/opt/spark/work-dir')
from pyspark.sql import SparkSession
from bao_spark_init import BaoSparkInit
bao  = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf()).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.sql("DESCRIBE TABLE polaris.tpcds.reason").show(truncate=False)
EOF
```

Note the columns — `r_reason_sk`, `r_reason_id`, `r_reason_desc`, `snap_id`, `snap_timestamp`.

### E.3 Perform DDL on the Oracle table

```bash
kubectl exec -n prod deploy/oracle-xe -- \
  sqlplus tpcds/TpcdsPwd123!@XEPDB1
```

```sql
-- ADD a new column
ALTER TABLE reason ADD (r_notes VARCHAR2(200));
COMMIT;

-- Verify Oracle sees it
DESCRIBE reason;

EXIT;
```

### E.4 Watch the schema evolution handler detect and apply the change

```bash
# Watch the handler log (should react within a few seconds of Debezium publishing the DDL event)
tail -f /tmp/schema_handler.log
```

**Expected log output:**

```
[INFO] DDL event for table 'reason': ALTER TABLE reason ADD (r_notes VARCHAR2(200))
[INFO] [reason] Schema change (add): ALTER TABLE `polaris`.`tpcds`.`reason` ADD COLUMN `r_notes` string
[INFO] [reason] ALTER TABLE applied: add r_notes string
```

### E.5 Verify the new column appears in Iceberg — no manual intervention

```bash
kubectl exec -n prod -i $MASTER -c spark-master -- \
  env USER=dave TOKEN="$TOKEN" ADDR="$ADDR" \
  python3 - <<'EOF'
import sys; sys.path.insert(0, '/opt/spark/work-dir')
from pyspark.sql import SparkSession
from bao_spark_init import BaoSparkInit
bao  = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf()).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("=== Schema after DDL evolution ===")
spark.sql("DESCRIBE TABLE polaris.tpcds.reason").show(truncate=False)
EOF
```

**Expected:** `r_notes` column now appears in the Iceberg schema with type `string`.
All existing rows have `r_notes = NULL`. The table is queryable immediately — no
restart, no connector reconfiguration, no outage.

### E.6 Insert a row using the new column and verify end-to-end

```bash
kubectl exec -n prod deploy/oracle-xe -- \
  sqlplus tpcds/TpcdsPwd123!@XEPDB1
```

```sql
INSERT INTO reason VALUES (200, 'AAAAAAAATEST0002', 'New column test', 'Added after DDL');
COMMIT;
EXIT;
```

Wait ~35 seconds for the Iceberg Sink commit, then:

```bash
kubectl exec -n prod -i $MASTER -c spark-master -- \
  env USER=dave TOKEN="$TOKEN" ADDR="$ADDR" \
  python3 - <<'EOF'
import sys; sys.path.insert(0, '/opt/spark/work-dir')
from pyspark.sql import SparkSession
from bao_spark_init import BaoSparkInit
bao  = BaoSparkInit()
spark = SparkSession.builder.config(conf=bao.spark_conf()).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.sql("""
  SELECT r_reason_sk, r_reason_desc, r_notes
  FROM   polaris.tpcds.reason
  WHERE  r_reason_sk = 200
""").show(truncate=False)
EOF
```

**Expected:** Row 200 with `r_notes = 'Added after DDL'` appears in Iceberg.

### E.7 Dry-run mode — test DDL detection without applying

To validate what the handler *would* do without touching Iceberg:

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN="$TOKEN" ADDR="$ADDR" \
      DRY_RUN=1 \
  python3 /opt/spark/work-dir/04_schema_evolution_handler.py 2>&1 | tee /tmp/schema_handler_dry.log
```

With `DRY_RUN=1` the handler logs the ALTER TABLE statement but does **not** execute it:

```
[INFO] [reason] Schema change (add): ALTER TABLE `polaris`.`tpcds`.`reason` ADD COLUMN `r_notes` string
[INFO] [reason] DRY_RUN — not executing.
```

---

## Test F — JupyterHub: Connect to Spark and Inspect Iceberg + Gluten/Velox Configurations

### F.1 Login and open a notebook

1. Open **`http://192.168.1.50:30888`** in your browser.
2. Get the admin password:
   ```bash
   kubectl get secret jupyterhub-credentials -n analytics \
     -o jsonpath='{.data.admin-password}' | base64 -d
   ```
3. Log in as `admin` with the password above.
4. Click **File → New → Notebook** and select the Python 3 kernel.

---

### F.2 Connect to Spark from the notebook

Jupyter pods run inside the cluster in the `analytics` namespace.
Use the **NodePort RPC address** (`spark://192.168.1.50:30777`) so the Spark
master can route callbacks back to the notebook pod.

Paste this into **Cell 1** and run it (`Shift+Enter`):

```python
import urllib.request, json
from pyspark.sql import SparkSession

# ── Fetch credentials from OpenBao ────────────────────────────────────────────
TOKEN = "<paste-token-here>"   # kubectl get secret openbao-unseal-keys -n prod -o jsonpath='{.data.root-token}' | base64 -d
ADDR  = "http://openbao.prod.svc.cluster.local:8200"

def _bao_get(path):
    req = urllib.request.Request(
        f"{ADDR}/v1/{path}",
        headers={"X-Vault-Token": TOKEN}
    )
    return json.loads(urllib.request.urlopen(req).read())["data"]["data"]

pol = _bao_get("secret/data/platform/polaris")
s3  = _bao_get("secret/data/platform/s3")

# ── Spark connection ──────────────────────────────────────────────────────────
# Use the NodePort address (30777) — Jupyter pods are in the 'analytics'
# namespace; NodePort is reachable from any namespace.
POLARIS_URI = "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog"

spark = SparkSession.builder \
    .master("spark://192.168.1.50:30777") \
    .appName("jupyter-iceberg-inspection") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.polaris",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.polaris.type",       "rest") \
    .config("spark.sql.catalog.polaris.uri",         POLARIS_URI) \
    .config("spark.sql.catalog.polaris.credential",
            f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}") \
    .config("spark.sql.catalog.polaris.scope",       "PRINCIPAL_ROLE:ALL") \
    .config("spark.sql.catalog.polaris.warehouse",   "IcebergCatalog") \
    .config("spark.sql.catalog.polaris.rest.auth.type",    "oauth2") \
    .config("spark.sql.catalog.polaris.oauth2-server-uri", f"{POLARIS_URI}/v1/oauth/tokens") \
    .config("spark.sql.catalog.polaris.s3.access-key-id",     s3["access_key"]) \
    .config("spark.sql.catalog.polaris.s3.secret-access-key", s3["secret_key"]) \
    .config("spark.sql.catalog.polaris.s3.endpoint",           s3["endpoint"]) \
    .config("spark.sql.catalog.polaris.s3.path-style-access",  "true") \
    .config("spark.sql.catalog.polaris.client.region",         s3["region"]) \
    .config("spark.jars", ",".join([
        "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar",
        "/opt/spark/jars/iceberg-aws-bundle-1.9.2.jar",
        "/opt/spark/jars/hadoop-aws-3.3.4.jar",
        "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
    ])) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark version :", spark.version)
print("Spark master  :", spark.sparkContext.master)
print("App name      :", spark.sparkContext.appName)
```

> **Tip — get all credentials from OpenBao in one call:**
> ```python
> import urllib.request, json
> TOKEN = "<paste-token-here>"   # kubectl get secret openbao-unseal-keys -n prod -o jsonpath='{.data.root-token}' | base64 -d
> ADDR  = "http://openbao.prod.svc.cluster.local:8200"
> def _bao_get(path):
>     req = urllib.request.Request(
>         f"{ADDR}/v1/{path}",
>         headers={"X-Vault-Token": TOKEN}
>     )
>     return json.loads(urllib.request.urlopen(req).read())["data"]["data"]
> pol = _bao_get("secret/data/platform/polaris")
> s3  = _bao_get("secret/data/platform/s3")
> print("Polaris ID    :", pol["spark_svc_id"])
> print("S3 endpoint   :", s3["endpoint"])
> ```

---

### F.3 Inspect Spark configuration

**Cell 2 — dump all Spark conf entries:**

```python
# All active Spark configuration key-value pairs
conf_items = spark.sparkContext.getConf().getAll()
conf_items_sorted = sorted(conf_items, key=lambda x: x[0])

print(f"{'Key':<60} {'Value'}")
print("-" * 100)
for k, v in conf_items_sorted:
    # Redact secrets
    display_v = "***" if any(s in k.lower() for s in ("password","secret","credential","key")) else v
    print(f"{k:<60} {display_v}")
```

**Cell 3 — check only Iceberg-related configs:**

```python
iceberg_conf = [(k, v) for k, v in conf_items_sorted if "iceberg" in k.lower() or "polaris" in k.lower()]
print(f"{'Key':<60} {'Value'}")
print("-" * 100)
for k, v in iceberg_conf:
    print(f"{k:<60} {v}")
```

**Expected output includes:**

```
spark.sql.catalog.polaris                            org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.polaris.type                       rest
spark.sql.catalog.polaris.uri                        http://polaris-rest.prod.svc.cluster.local:8181/api/catalog
spark.sql.catalog.polaris.warehouse                  IcebergCatalog
spark.sql.catalog.polaris.scope                      PRINCIPAL_ROLE:ALL
spark.sql.catalog.polaris.rest.auth.type             oauth2
spark.sql.catalog.polaris.oauth2-server-uri          http://polaris-rest.prod.svc.cluster.local:8181/api/catalog/v1/oauth/tokens
spark.sql.catalog.polaris.s3.access-key-id           ***
spark.sql.catalog.polaris.s3.secret-access-key       ***
spark.sql.catalog.polaris.s3.endpoint                https://s3.us-east-2.amazonaws.com
spark.sql.catalog.polaris.s3.path-style-access       true
spark.sql.catalog.polaris.client.region              us-east-2
spark.sql.extensions                                 org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.serializer                                     org.apache.spark.serializer.KryoSerializer
```

---

### F.4 Inspect Gluten / Velox configuration

**Cell 4 — check whether Gluten plugin is loaded:**

```python
plugins = spark.conf.get("spark.plugins", "")
print("spark.plugins :", plugins if plugins else "(none — Gluten disabled)")

offheap_enabled = spark.conf.get("spark.memory.offHeap.enabled", "false")
offheap_size    = spark.conf.get("spark.memory.offHeap.size",    "0")
backend         = spark.conf.get("spark.gluten.sql.columnar.backend.lib", "(not set)")

print("offHeap.enabled :", offheap_enabled)
print("offHeap.size    :", offheap_size)
print("gluten backend  :", backend)
```

**Cell 5 — enable Gluten/Velox for this session and verify:**

```python
# Enable Gluten + Velox for this SparkSession only
spark.conf.set("spark.plugins",                         "io.glutenproject.GlutenPlugin")
spark.conf.set("spark.memory.offHeap.enabled",          "true")
spark.conf.set("spark.memory.offHeap.size",             "2g")
spark.conf.set("spark.gluten.sql.columnar.backend.lib", "velox")

# Run a query and inspect the physical plan for Gluten operators
df = spark.range(1_000_000).selectExpr("id % 100 AS bucket", "id * 2.5 AS value")
df.groupBy("bucket").agg({"value": "sum"}).explain(mode="formatted")
```

**Look for Gluten operators in the plan output:**

```
== Physical Plan ==
AdaptiveSparkPlan (1)
+- GlutenColumnarToRow (2)              ← Gluten is active
   +- VeloxColumnarAggregate (3)        ← Velox native aggregation
      +- VeloxColumnarExchange (4)
         +- VeloxColumnarAggregate (5)
```

If you see `GlutenColumnarToRow` or `Velox*` operators, Gluten is working. If the plan shows
only standard `HashAggregate` / `Exchange` operators, Gluten fell back to JVM execution
(check that the worker node has AVX2/AVX-512 CPU support).

**Cell 6 — disable Gluten for a specific query (per-query opt-out):**

```python
# Turn Gluten off for one query
spark.conf.set("spark.plugins", "")
df2 = spark.range(100).selectExpr("id * 3 AS val")
df2.explain(mode="formatted")   # should show standard HashAggregate, no Gluten operators
```

---

### F.5 Inspect Iceberg table metadata

**Cell 7 — list all namespaces and tables in the Polaris catalog:**

```python
# List all namespaces
print("=== Namespaces in polaris catalog ===")
spark.sql("SHOW NAMESPACES IN polaris").show(truncate=False)

# List tables in tpcds_sf10tcl namespace (bulk-copy pipeline output)
print("=== Tables in polaris.tpcds_sf10tcl ===")
spark.sql("SHOW TABLES IN polaris.tpcds_sf10tcl").show(truncate=False)

# List tables in tpcds namespace (CDC pipeline output)
print("=== Tables in polaris.tpcds ===")
spark.sql("SHOW TABLES IN polaris.tpcds").show(truncate=False)
```

**Cell 8 — describe a table (schema + partitioning + properties):**

```python
TABLE = "polaris.tpcds_sf10tcl.income_band"   # change as needed

print(f"=== Schema: {TABLE} ===")
spark.sql(f"DESCRIBE TABLE {TABLE}").show(truncate=False)

print(f"=== Extended metadata (properties, location, partitioning) ===")
spark.sql(f"DESCRIBE TABLE EXTENDED {TABLE}").show(truncate=False)
```

**What to look for in DESCRIBE EXTENDED:**

| Property | Expected value |
|---|---|
| `Provider` | `iceberg` |
| `format-version` | `2` |
| `write.format.default` | `parquet` |
| `write.target-file-size-bytes` | `268435456` (256 MB) |
| `write.parquet.compression-codec` | `snappy` |
| `platform.created-by` | `dave` |
| `pipeline.sf_extraction_ts` | ISO-8601 UTC timestamp (set after successful copy) |
| `Location` | `s3://xdatatoiceberg1/tpcds/tpcds_sf10tcl/income_band` |

---

### F.6 Inspect Iceberg snapshot history and data files

**Cell 9 — snapshot history:**

```python
TABLE = "polaris.tpcds_sf10tcl.income_band"

print("=== Snapshot history ===")
spark.sql(f"""
    SELECT snapshot_id,
           committed_at,
           operation,
           summary['added-records']   AS added_records,
           summary['deleted-records'] AS deleted_records,
           summary['added-files-size'] AS added_bytes
    FROM   {TABLE}.snapshots
    ORDER  BY committed_at DESC
""").show(truncate=False)
```

**Cell 10 — data files on S3:**

```python
print("=== Data files ===")
spark.sql(f"""
    SELECT file_path,
           file_format,
           record_count,
           file_size_in_bytes,
           partition
    FROM   {TABLE}.files
    ORDER  BY file_size_in_bytes DESC
""").show(truncate=False)
```

**Cell 11 — partition summary:**

```python
print("=== Partition summary ===")
spark.sql(f"""
    SELECT partition,
           record_count,
           file_count,
           total_data_file_size_in_bytes
    FROM   {TABLE}.partitions
    ORDER  BY record_count DESC
""").show(truncate=False)
```

**Cell 12 — time-travel query (query data as of a past snapshot):**

```python
# Step 1: get an old snapshot ID
snaps = spark.sql(f"SELECT snapshot_id, committed_at FROM {TABLE}.snapshots ORDER BY committed_at").collect()
for s in snaps:
    print(s["snapshot_id"], s["committed_at"])

# Step 2: query as of the first snapshot
first_snap_id = snaps[0]["snapshot_id"]
print(f"\n=== Data at first snapshot ({first_snap_id}) ===")
spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF {first_snap_id}").show(truncate=False)

# Step 3: query as of a timestamp
spark.sql(f"""
    SELECT * FROM {TABLE}
    TIMESTAMP AS OF '{snaps[0]['committed_at']}'
""").show(truncate=False)
```

---

### F.7 Check the CDC pipeline watermarks from the notebook

**Cell 13 — query the Iceberg `_pipeline_watermarks` control table:**

```python
print("=== CDC sync-point watermarks (Iceberg) ===")
spark.sql("""
    SELECT source_db,
           source_schema,
           table_name,
           sf_extraction_ts,
           rows_copied,
           pipeline_run_ts,
           iceberg_namespace
    FROM   polaris.tpcds_sf10tcl._pipeline_watermarks
    ORDER  BY source_db, table_name
""").show(truncate=False)
```

**Cell 14 — check run history from the pipeline PostgreSQL DB:**

```python
import psycopg2, urllib.request, json

# Fetch pipeline DB creds from OpenBao
TOKEN = "<paste-token>"
ADDR  = "http://openbao.prod.svc.cluster.local:8200"
req = urllib.request.Request(
    f"{ADDR}/v1/secret/data/platform/pipeline_db",
    headers={"X-Vault-Token": TOKEN}
)
pg = json.loads(urllib.request.urlopen(req).read())["data"]["data"]

conn = psycopg2.connect(
    host=pg["host"], port=int(pg.get("port", 5432)),
    dbname=pg["database"], user=pg["user"], password=pg["password"]
)
cur = conn.cursor()

# Watermarks
print("=== pipeline_watermarks ===")
cur.execute("""
    SELECT table_name, sf_extraction_ts, oracle_start_scn, rows_copied, updated_at
    FROM   pipeline_watermarks
    WHERE  source_db='SNOWFLAKE_SAMPLE_DATA' AND source_schema='TPCDS_SF10TCL'
    ORDER  BY table_name
""")
for row in cur.fetchall():
    print(row)

# Run log
print("\n=== pipeline_run_log (last 10 runs) ===")
cur.execute("""
    SELECT run_id, started_at, finished_at, tables_ok, tables_failed, total_rows, status
    FROM   pipeline_run_log
    ORDER  BY started_at DESC LIMIT 10
""")
for row in cur.fetchall():
    print(row)

cur.close(); conn.close()
```

---

### F.8 Run a Gluten-accelerated query on a copied Iceberg table

**Cell 15 — enable Gluten then run an aggregation on `customer`:**

```python
# Enable Velox acceleration
spark.conf.set("spark.plugins",                         "io.glutenproject.GlutenPlugin")
spark.conf.set("spark.memory.offHeap.enabled",          "true")
spark.conf.set("spark.memory.offHeap.size",             "2g")
spark.conf.set("spark.gluten.sql.columnar.backend.lib", "velox")

import time

# Timed query — with Gluten
t0 = time.time()
result_gluten = spark.sql("""
    SELECT   c_birth_country,
             COUNT(*)          AS customers,
             AVG(c_birth_year) AS avg_birth_year
    FROM     polaris.tpcds_sf10tcl.customer
    GROUP BY c_birth_country
    ORDER BY customers DESC
    LIMIT    20
""")
result_gluten.show(truncate=False)
print(f"Gluten elapsed: {time.time() - t0:.2f}s")

# Timed query — without Gluten (for comparison)
spark.conf.set("spark.plugins", "")
t0 = time.time()
result_jvm = spark.sql("""
    SELECT   c_birth_country,
             COUNT(*)          AS customers,
             AVG(c_birth_year) AS avg_birth_year
    FROM     polaris.tpcds_sf10tcl.customer
    GROUP BY c_birth_country
    ORDER BY customers DESC
    LIMIT    20
""")
result_jvm.show(truncate=False)
print(f"JVM elapsed   : {time.time() - t0:.2f}s")
```

**Cell 16 — inspect the Gluten physical plan to confirm Velox operators:**

```python
spark.conf.set("spark.plugins",                         "io.glutenproject.GlutenPlugin")
spark.conf.set("spark.memory.offHeap.enabled",          "true")
spark.conf.set("spark.memory.offHeap.size",             "2g")
spark.conf.set("spark.gluten.sql.columnar.backend.lib", "velox")

spark.sql("""
    SELECT c_birth_country, COUNT(*) AS n
    FROM   polaris.tpcds_sf10tcl.customer
    GROUP BY c_birth_country
""").explain(mode="formatted")
```

---

### F.9 Stop the Spark session when done

Always stop the session when finished to release executor slots on the cluster:

```python
spark.stop()
print("SparkSession stopped.")
```

---

## Quick Reference — Log Patterns to Watch

| What you want to confirm | Log pattern |
|---|---|
| Table is being copied (not skipped) | `[size-report] <table> → X.X GB  (COPY)` |
| Resume detected | `RESUME: N rows already in Iceberg` |
| Fresh sf_extraction_ts captured | `sf_extraction_ts=… (CDC sync point)` |
| Early watermark written | `Early watermark written to pipeline DB` |
| Batch progress | `batch offset=N rows=M total=T` |
| Thread assignment | `[copy-worker-N] [<table>] START` |
| Job completed successfully | `status=success` in final summary line |
| Connector running | `"state": "RUNNING"` in connector status JSON |
| CDC event published to Kafka | offset increases on `oracle-tpcds.TPCDS.<TABLE>` |
| Iceberg sink committed | new snapshot in `polaris.tpcds.<table>.snapshots` |
| DDL detected by handler | `DDL event for table '<table>'` |
| Iceberg ALTER TABLE applied | `ALTER TABLE applied: <op> <col> <type>` |
| Spark connected from Jupyter | `Spark version: 3.5.1` printed in cell output |
| Gluten active | `GlutenColumnarToRow` in query plan |
| Gluten disabled (fallback) | standard `HashAggregate` / `Exchange` in plan |
| Iceberg table property visible | `pipeline.sf_extraction_ts` row in DESCRIBE EXTENDED |
| CDC watermark readable | rows in `polaris.tpcds_sf10tcl._pipeline_watermarks` |

## Logging

All Spark/Hadoop/Iceberg/Snowflake/Netty internal messages are routed to the rolling log file only — they never appear on the console.

**Log file location (inside the master pod):**
```
/home/spark/logs/spark-warnings.log
```
Capped at **500 MB**, overwritten on rollover (single file, no rotation history).

**Tail the log live:**
```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  tail -f /home/spark/logs/spark-warnings.log
```

**What appears on console:** Only Python `logging` output from `sf2iceberg`, `bao_spark_init`, and `spark_iceberg_utils` — the `[INFO]`, `[WARN]`, `[ERROR]` lines with the `%(asctime)s [%(threadName)s]` prefix.

**What goes to file only (suppressed from console):**

| Message | Logger package | Reason suppressed |
|---|---|---|
| `WARN NativeCodeLoader: Unable to load native-hadoop library` | `org.apache.hadoop` | Hadoop native libs not compiled for this platform; Java fallback is used automatically |
| `WARN OAuth2Manager: Iceberg REST client is missing the OAuth2 server URI` | `org.apache.iceberg` | Fires from catalog init before runtime SparkConf applies; uses correct default URL and succeeds |
| `ERROR TransportRequestHandler: StacklessClosedChannelException` | `org.apache.spark` | Executor drops a JAR-fetch stream it no longer needs; harmless Netty connection close |
| `ERROR Inbox: Ignoring error … NotSerializableException: StorageStatus` | `org.apache.spark` | Spark 3.5 bug — Web UI RPC serialisation failure; caught and dropped internally, no effect on job |
| `ERROR RestRequest: UnknownHostException: metadata.google.internal` | `com.snowflake` | Snowflake JDBC probing GCP cloud identity on startup; fails immediately, falls back to password auth |
| `ERROR RestRequest: SocketTimeoutException … 169.254.169.254` | `com.snowflake` | Snowflake JDBC probing AWS/Azure IMDS on startup; times out (~3 s total), falls back to password auth |
| `WARN SparkConnectorContext$: Finish cancelling all queries` | `net.snowflake` | Normal Snowflake connector shutdown on `spark.stop()` |
| `INFO py4j … Closing down clientserver connection` | `py4j` | Normal py4j gateway teardown on `spark.stop()` |
