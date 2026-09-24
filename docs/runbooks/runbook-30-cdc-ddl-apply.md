# Runbook 30 — CDC DDL Apply: Schema Evolution for Kafka → Iceberg Pipeline

> **Version:** 1.0
> **Status:** Active
> **Owner:** Platform Engineering
> **Script:** [`scripts/ddl_apply.py`](../../scripts/ddl_apply.py)
> **Pipeline script:** [`docker/spark-gluten-velox/scripts/05_kafka_to_iceberg_streaming.py`](../../docker/spark-gluten-velox/scripts/05_kafka_to_iceberg_streaming.py)

---

## Overview

The CDC streaming pipeline (`05_kafka_to_iceberg_streaming.py`) is **DML-only**.
It does **not** automatically evolve the Iceberg schema when a DDL change occurs
at the source database. When a column is added, dropped, modified, or renamed at
the source, you must run `ddl_apply.py` to propagate that change to Iceberg.

This runbook covers every DDL operation for all three sources — Oracle, PostgreSQL,
and MongoDB — across all three write modes: standard, soft-delete, and history tracking.

---

## Architecture recap

```
Source DB                   Kafka Topics                     Iceberg Tables
─────────────────           ──────────────────────────       ──────────────────────────────
Oracle             ──DML──▶ oracle.cache_testing.*   ──▶     oracle.cache_testing.<table>
PostgreSQL         ──DML──▶ postgres.cache_testing.* ──▶     postgres.cache_testing.<table>
MongoDB            ──DML──▶ mongodb.cache_testing.*  ──▶     mongodb.cache_testing.<table>

Write mode         Kafka Deployment                          Iceberg table
─────────────────  ──────────────────────────────────        ──────────────
standard           kafka-to-iceberg-<source>-standard        <table>
soft_delete        kafka-to-iceberg-<source>-soft-delete     <table>_sd
history_tracking   kafka-to-iceberg-<source>-history-track.  <table>_hist
```

When a DDL fires at the source, Debezium captures it and the pipeline's in-memory
schema cache becomes stale. New columns are silently dropped from batches until
`ddl_apply.py` updates Iceberg and the pods restart with a fresh cache.

---

## How `ddl_apply.py` works — 5 steps

```
Step 1  Scale down → kubectl scale --replicas=0 all target deployments
Step 2  Confirm    → poll until BOTH readyReplicas=0 AND replicas=0 (no Terminating pods)
Step 3  Apply DDL  → spark-sql ALTER TABLE on each Iceberg table (runs in a pod from
                     a DIFFERENT source that is still running)
Step 4  Verify     → DESCRIBE TABLE confirms the change is committed in Iceberg
Step 5  Scale up   → kubectl scale --replicas=<original> + wait for pods Ready
```

No Kafka offsets are lost. DML events buffered in Kafka during the pause are
consumed in full when the pipeline resumes.

---

## Pre-flight checklist

Run these checks before every `ddl_apply.py` execution.

```bash
# 1. Confirm kubectl context is pointed at the right cluster
kubectl config current-context

# 2. Check all streaming pods are currently running (note replica counts)
kubectl -n prod get deployments -l app=kafka-to-iceberg \
  -o custom-columns='DEPLOYMENT:.metadata.name,DESIRED:.spec.replicas,READY:.status.readyReplicas'

# 3. Confirm the source DDL has already been applied at the source database
#    (ddl_apply.py does NOT apply DDL at the source — it only updates Iceberg)

# 4. Dry-run first to preview exactly what will happen (no changes made)
python3 scripts/ddl_apply.py \
    --source <source> --table <table> \
    --op <op> --col <col> [--type <type>] [--new-col <new>] \
    --dry-run
```

> ⚠️ Always run with `--dry-run` first. Review every SQL statement shown in the
> output before running without it.

---

## CLI reference

```
python3 scripts/ddl_apply.py \
    --source  <oracle|postgres|mongodb>   # required
    --table   <table_name>                # required — no suffix, e.g. customers
    --op      <add|drop|modify|rename>    # required
    --col     <column_name>               # required — column to act on
    --type    <iceberg_type>              # required for add / modify
    --new-col <new_column_name>           # required for rename
    --modes   standard,soft_delete,...    # default: all three modes
    --namespace <ns>                      # default: cache_testing
    --catalog   <cat>                     # default: source name (oracle/postgres/mongodb)
    --dry-run                             # preview only — no changes
    --yes / -y                            # skip confirmation prompt (CI/scripted use)
```

**Supported Iceberg types for `--type`:**

| Type | Use for |
|---|---|
| `STRING` | VARCHAR, TEXT, CHAR, NVARCHAR |
| `BIGINT` | Large integer, Oracle NUMBER(19,0) |
| `INT` | Smaller integer, PostgreSQL integer |
| `DOUBLE` | Floating point |
| `FLOAT` | Single precision |
| `BOOLEAN` | Boolean / boolean |
| `TIMESTAMP` | DATE, TIMESTAMP, DATETIME |
| `DATE` | Date-only columns |
| `DECIMAL(p,s)` | Oracle NUMBER(p,s), PostgreSQL numeric(p,s) |

---

## Part 1 — Oracle

### Prerequisites

Apply the DDL in Oracle first, then run `ddl_apply.py`.

```sql
-- Example: connect as the app user inside the oracle pod
kubectl -n prod exec -it $(kubectl -n prod get pod -l app=oracle-xe -o jsonpath='{.items[0].metadata.name}') \
  -- sqlplus cache_testing/<password>@//localhost:1521/XEPDB1

-- Example DDL at the source (run BEFORE ddl_apply.py)
ALTER TABLE CUSTOMERS ADD (credit_score NUMBER(10,2));
COMMIT;
```

---

### 1.1 Oracle — ADD column (NUMBER / DECIMAL type)

**Scenario:** A new `credit_score NUMBER(10,2)` column was added to the Oracle
`CUSTOMERS` table. Apply it to all three Iceberg write modes.

```bash
# Step 0 — dry-run preview
python3 scripts/ddl_apply.py \
    --source oracle \
    --table  customers \
    --op     add \
    --col    credit_score \
    --type   "DECIMAL(10,2)" \
    --dry-run
```

Review output — confirm three SQL statements appear, one per mode:
- `ALTER TABLE \`oracle\`.\`cache_testing\`.\`customers\` ADD COLUMN \`credit_score\` DECIMAL(10,2)`
- `ALTER TABLE \`oracle\`.\`cache_testing\`.\`customers_sd\` ADD COLUMN \`credit_score\` DECIMAL(10,2)`
- `ALTER TABLE \`oracle\`.\`cache_testing\`.\`customers_hist\` ADD COLUMN \`credit_score\` DECIMAL(10,2)`

```bash
# Step 1 — execute
python3 scripts/ddl_apply.py \
    --source oracle \
    --table  customers \
    --op     add \
    --col    credit_score \
    --type   "DECIMAL(10,2)"
```

**Expected output sequence:**
```
Step 1/5: Scaling down 3 deployment(s) — pausing DML replication
  ✓  Scale command sent: kafka-to-iceberg-oracle-standard → 0
  ✓  Scale command sent: kafka-to-iceberg-oracle-soft-delete → 0
  ✓  Scale command sent: kafka-to-iceberg-oracle-history-tracking → 0

Step 2/5: Confirming all target pods are fully stopped
  ✓  kafka-to-iceberg-oracle-standard: fully stopped (0 ready, 0 total pods).
  ✓  kafka-to-iceberg-oracle-soft-delete: fully stopped (0 ready, 0 total pods).
  ✓  kafka-to-iceberg-oracle-history-tracking: fully stopped (0 ready, 0 total pods).
  ✓  All target pods confirmed stopped.

Step 3/5: Applying DDL in Iceberg via spark-sql
  ✓  Execution pod: kafka-to-iceberg-postgres-standard-... Spark conf: ready
  ✓  [standard] Applied to customers.
  ✓  [soft_delete] Applied to customers_sd.
  ✓  [history_tracking] Applied to customers_hist.

Step 4/5: Verifying DDL results in Iceberg (DESCRIBE TABLE)
  ✓  [standard] `credit_score` confirmed PRESENT in customers (decimal(10,2)).
  ✓  [soft_delete] `credit_score` confirmed PRESENT in customers_sd (decimal(10,2)).
  ✓  [history_tracking] `credit_score` confirmed PRESENT in customers_hist (decimal(10,2)).

Step 5/5: Resuming DML replication — scaling Deployments back up
  ✓  All pods are running. DML replication resumed from last committed Kafka offset.
```

**Post-check:**
```bash
# Verify the streaming pods are back up
kubectl -n prod get pods -l app=kafka-to-iceberg,pipeline.source=oracle

# Verify the column in Iceberg (exec into any streaming pod)
kubectl -n prod exec -it <pod> -- bash -c \
  "cd /opt/spark/work-dir && spark-sql \
   --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
   -e 'DESCRIBE TABLE oracle.cache_testing.customers'"
```

---

### 1.2 Oracle — ADD column (VARCHAR / STRING type)

```bash
# Source DDL first
# ALTER TABLE CUSTOMERS ADD (loyalty_tier VARCHAR2(50));

python3 scripts/ddl_apply.py \
    --source oracle \
    --table  customers \
    --op     add \
    --col    loyalty_tier \
    --type   STRING
```

---

### 1.3 Oracle — MODIFY column type (widen precision)

**Scenario:** `credit_score NUMBER(10,2)` is being widened to `NUMBER(18,4)`.

> ⚠️ Iceberg only supports widening type changes (INT→BIGINT, DECIMAL(10,2)→DECIMAL(18,4)).
> Narrowing a type will fail at the Iceberg level — this is by design.

```bash
# Source DDL first
# ALTER TABLE CUSTOMERS MODIFY (credit_score NUMBER(18,4));

python3 scripts/ddl_apply.py \
    --source oracle \
    --table  customers \
    --op     modify \
    --col    credit_score \
    --type   "DECIMAL(18,4)"
```

**Expected verification output:**
```
  ✓  [standard] `credit_score` type in customers: decimal(18,4)
  ✓  [soft_delete] `credit_score` type in customers_sd: decimal(18,4)
  ✓  [history_tracking] `credit_score` type in customers_hist: decimal(18,4)
```

---

### 1.4 Oracle — MODIFY column type (VARCHAR2 wider)

```bash
# Source DDL first
# ALTER TABLE CUSTOMERS MODIFY (loyalty_tier VARCHAR2(200));

python3 scripts/ddl_apply.py \
    --source oracle \
    --table  customers \
    --op     modify \
    --col    loyalty_tier \
    --type   STRING
```

> Note: STRING maps to Iceberg `string` type. VARCHAR2 widening has no effect on
> Iceberg STRING — the column is already unbounded. The MODIFY is still run to
> keep schema metadata consistent.

---

### 1.5 Oracle — RENAME column

**Scenario:** `credit_score` is renamed to `credit_score_v2`.

```bash
# Source DDL first
# ALTER TABLE CUSTOMERS RENAME COLUMN credit_score TO credit_score_v2;

python3 scripts/ddl_apply.py \
    --source  oracle \
    --table   customers \
    --op      rename \
    --col     credit_score \
    --new-col credit_score_v2
```

**Expected verification output:**
```
  ✓  [standard] `credit_score` → `credit_score_v2` confirmed in customers.
  ✓  [soft_delete] `credit_score` → `credit_score_v2` confirmed in customers_sd.
  ✓  [history_tracking] `credit_score` → `credit_score_v2` confirmed in customers_hist.
```

> **Important:** Existing rows in Iceberg that were written before the rename
> will have `NULL` in `credit_score_v2` (the old column `credit_score` still
> exists in those row files). New DML rows after this script will populate
> `credit_score_v2`. This is correct Iceberg behaviour.

---

### 1.6 Oracle — DROP column

```bash
# Source DDL first
# ALTER TABLE CUSTOMERS DROP COLUMN credit_score_v2;

python3 scripts/ddl_apply.py \
    --source oracle \
    --table  customers \
    --op     drop \
    --col    credit_score_v2
```

**Expected verification output:**
```
  ✓  [standard] `credit_score_v2` confirmed ABSENT from customers.
  ✓  [soft_delete] `credit_score_v2` confirmed ABSENT from customers_sd.
  ✓  [history_tracking] `credit_score_v2` confirmed ABSENT from customers_hist.
```

---

### 1.7 Oracle — Apply to specific modes only

If only the `standard` and `soft_delete` tables need the change (e.g., history
tracking is retired for this table):

```bash
python3 scripts/ddl_apply.py \
    --source oracle \
    --table  customers \
    --op     add \
    --col    region_code \
    --type   STRING \
    --modes  standard,soft_delete
```

Only `kafka-to-iceberg-oracle-standard` and `kafka-to-iceberg-oracle-soft-delete`
are scaled down. The history-tracking deployment continues running uninterrupted.

---

### 1.8 Oracle — Non-default namespace (e.g. tpcds)

```bash
# Source DDL first (in the tpcds schema)
# ALTER TABLE PRODUCTS ADD (discount_pct NUMBER(5,2));

python3 scripts/ddl_apply.py \
    --source    oracle \
    --table     products \
    --namespace tpcds \
    --op        add \
    --col       discount_pct \
    --type      "DECIMAL(5,2)"
```

---

## Part 2 — PostgreSQL

### Prerequisites

Apply the DDL in PostgreSQL first, then run `ddl_apply.py`.

```bash
# Connect to the PostgreSQL pod
kubectl -n prod exec -it postgresql-0 -- \
  psql -U postgres -d cache_testing

-- Example source DDL (run BEFORE ddl_apply.py)
ALTER TABLE customers ADD COLUMN IF NOT EXISTS credit_score NUMERIC(10,2);
```

---

### 2.1 PostgreSQL — ADD column (NUMERIC / DECIMAL)

```bash
# Source DDL first
# ALTER TABLE customers ADD COLUMN IF NOT EXISTS credit_score NUMERIC(10,2);

python3 scripts/ddl_apply.py \
    --source postgres \
    --table  customers \
    --op     add \
    --col    credit_score \
    --type   "DECIMAL(10,2)"
```

---

### 2.2 PostgreSQL — ADD column (TEXT / STRING)

```bash
# Source DDL first
# ALTER TABLE customers ADD COLUMN IF NOT EXISTS loyalty_tier VARCHAR(50);

python3 scripts/ddl_apply.py \
    --source postgres \
    --table  customers \
    --op     add \
    --col    loyalty_tier \
    --type   STRING
```

---

### 2.3 PostgreSQL — ADD column to a different table (e.g. orders)

```bash
# Source DDL first
# ALTER TABLE orders ADD COLUMN IF NOT EXISTS shipped_at TIMESTAMPTZ;

python3 scripts/ddl_apply.py \
    --source postgres \
    --table  orders \
    --op     add \
    --col    shipped_at \
    --type   TIMESTAMP
```

> This creates/updates three Iceberg tables:
> - `postgres.cache_testing.orders`
> - `postgres.cache_testing.orders_sd`
> - `postgres.cache_testing.orders_hist`

---

### 2.4 PostgreSQL — MODIFY column type

```bash
# Source DDL first
# ALTER TABLE customers ALTER COLUMN credit_score TYPE NUMERIC(18,4);

python3 scripts/ddl_apply.py \
    --source postgres \
    --table  customers \
    --op     modify \
    --col    credit_score \
    --type   "DECIMAL(18,4)"
```

---

### 2.5 PostgreSQL — RENAME column

```bash
# Source DDL first
# ALTER TABLE customers RENAME COLUMN status TO customer_status;

python3 scripts/ddl_apply.py \
    --source  postgres \
    --table   customers \
    --op      rename \
    --col     status \
    --new-col customer_status
```

---

### 2.6 PostgreSQL — DROP column

```bash
# Source DDL first
# ALTER TABLE customers DROP COLUMN IF EXISTS legacy_notes;

python3 scripts/ddl_apply.py \
    --source postgres \
    --table  customers \
    --op     drop \
    --col    legacy_notes
```

---

### 2.7 PostgreSQL — Standard and soft-delete only (skip history tracking)

```bash
python3 scripts/ddl_apply.py \
    --source postgres \
    --table  orders \
    --op     add \
    --col    tracking_number \
    --type   STRING \
    --modes  standard,soft_delete
```

---

## Part 3 — MongoDB

### How MongoDB DDL works

MongoDB is schemaless. There is no `ALTER TABLE` at the source. Schema changes
happen implicitly when documents are updated with new fields via `$set` or
`$unset`. Debezium captures these as changes to the document structure.

**When to run `ddl_apply.py` for MongoDB:**
- A new field appears in documents and you want it to be stored in Iceberg.
- A field is being retired and you want to DROP it from Iceberg tables.
- You want to explicitly type or rename a field in Iceberg.

**Source-side example (before running `ddl_apply.py`):**
```javascript
// mongosh — add a new field to all documents
db.customers.updateMany({}, { $set: { loyalty_tier: null } })

// or just let it appear naturally as new documents are inserted with the field
```

---

### 3.1 MongoDB — ADD field (STRING)

```bash
python3 scripts/ddl_apply.py \
    --source mongodb \
    --table  customers \
    --op     add \
    --col    loyalty_tier \
    --type   STRING
```

---

### 3.2 MongoDB — ADD field (INT / counter / version)

```bash
python3 scripts/ddl_apply.py \
    --source mongodb \
    --table  customers \
    --op     add \
    --col    schema_version \
    --type   INT
```

---

### 3.3 MongoDB — ADD field (TIMESTAMP)

```bash
python3 scripts/ddl_apply.py \
    --source mongodb \
    --table  customers \
    --op     add \
    --col    verified_at \
    --type   TIMESTAMP
```

---

### 3.4 MongoDB — ADD field (DECIMAL / monetary)

```bash
python3 scripts/ddl_apply.py \
    --source mongodb \
    --table  customers \
    --op     add \
    --col    account_balance \
    --type   "DECIMAL(18,4)"
```

---

### 3.5 MongoDB — RENAME field in Iceberg

```bash
python3 scripts/ddl_apply.py \
    --source  mongodb \
    --table   customers \
    --op      rename \
    --col     credit_limit \
    --new-col credit_limit_usd
```

---

### 3.6 MongoDB — DROP field from Iceberg

```bash
python3 scripts/ddl_apply.py \
    --source mongodb \
    --table  customers \
    --op     drop \
    --col    legacy_segment
```

---

### 3.7 MongoDB — ADD field to a different collection (e.g. events)

```bash
python3 scripts/ddl_apply.py \
    --source mongodb \
    --table  events \
    --op     add \
    --col    event_version \
    --type   INT
```

---

### 3.8 MongoDB — History-tracking table only

```bash
python3 scripts/ddl_apply.py \
    --source mongodb \
    --table  customers \
    --op     add \
    --col    audit_flag \
    --type   BOOLEAN \
    --modes  history_tracking
```

---

## Part 4 — Troubleshooting

### 4.1 Step 2 times out — pods not stopping within 120s

**Symptom:**
```
⚠  kafka-to-iceberg-oracle-standard: still has pods after 120s!
```

**Cause:** The pod may be stuck in `Terminating` because a Spark job is holding
resources or a finalizer is set.

**Action:**
```bash
# Check why the pod is stuck
kubectl -n prod get pods -l app=kafka-to-iceberg,pipeline.source=oracle
kubectl -n prod describe pod <stuck-pod-name>

# Force-delete if safe (Spark has already checkpointed the last batch)
kubectl -n prod delete pod <stuck-pod-name> --grace-period=0 --force

# Then re-run ddl_apply.py
python3 scripts/ddl_apply.py --source oracle --table customers \
    --op add --col <col> --type <type>
```

---

### 4.2 Step 3 fails — no running Spark pod found

**Symptom:**
```
✗  Cannot find a running Spark pod for DDL execution: No running streaming pod found
```

**Cause:** All streaming pods (across all sources) were already scaled down, or
the pod selector returned no results.

**Option A** — Leave a pod from a different source running:
```bash
# Ensure postgres standard pod is running before applying oracle DDL
kubectl -n prod get deployment kafka-to-iceberg-postgres-standard
# If replicas=0, scale it up first
kubectl -n prod scale deployment/kafka-to-iceberg-postgres-standard --replicas=1
# Wait for it to be ready, then re-run ddl_apply.py
```

**Option B** — Specify the pod manually:
```bash
# Find any running spark pod
kubectl -n prod get pods -l app=kafka-to-iceberg --field-selector=status.phase=Running

# Set the env var and re-run
SPARK_POD=kafka-to-iceberg-postgres-standard-xxxx-yyyy \
    python3 scripts/ddl_apply.py --source oracle --table customers \
    --op add --col <col> --type <type>
```

---

### 4.3 Step 3 fails — DDL SQL error

**Symptom:**
```
✗  [standard] DDL FAILED for customers:
   AnalysisException: Cannot convert Iceberg type ... to Spark type
```

**Cause:** Usually a type incompatibility (narrowing a DECIMAL, changing STRING
to INT on a column with string data, etc.).

**Action:**
- Check whether the type change is a widen or narrow.
- Iceberg supports: INT→BIGINT, FLOAT→DOUBLE, DECIMAL(p,s)→DECIMAL(p2,s2) where p2≥p and s2≥s.
- Iceberg does NOT support: BIGINT→INT, STRING→INT, changing scale of DECIMAL.
- If the change is not supported by Iceberg, use ADD + data migration + DROP instead.

---

### 4.4 Step 4 fails — column not found after DDL

**Symptom:**
```
✗  [soft_delete] `credit_score` NOT FOUND in customers_sd after ADD!
```

**Cause:** The DDL succeeded at the spark-sql level but Iceberg catalog
metadata cache may not have refreshed.

**Action:**
```bash
# Manually verify via spark-sql in a pod
kubectl -n prod exec -it <pod> -- bash -c \
    "cd /opt/spark/work-dir && spark-sql \
     --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
     -e 'DESCRIBE TABLE oracle.cache_testing.customers_sd'"

# If the column is present, the verification was a false negative — the pipeline is fine.
# If absent, re-run the script targeting only the failing mode:
python3 scripts/ddl_apply.py \
    --source oracle --table customers \
    --op add --col credit_score --type "DECIMAL(10,2)" \
    --modes soft_delete
```

---

### 4.5 Step 5 — pods not coming back within 180s

**Symptom:**
```
⚠  kafka-to-iceberg-oracle-standard: pod not ready after 180s — check pod logs
```

**Action:**
```bash
kubectl -n prod get pods -l app=kafka-to-iceberg,pipeline.source=oracle
kubectl -n prod logs deployment/kafka-to-iceberg-oracle-standard --tail=50
```

Common causes:
- OpenBao unreachable (check `http://192.168.1.50:30820` is up)
- Polaris REST catalog unreachable
- S3/MinIO unreachable for checkpoint location
- Spark master unreachable

---

### 4.6 Rows with the new column show NULL in Iceberg

**Symptom:** After the script succeeds and the pipeline resumes, SELECT on the
new column returns NULL for rows that existed before the DDL.

**Explanation:** This is expected and correct. Rows written to Iceberg before the
column was added have NULL for that column — they were written with the old schema.
Only rows inserted/updated at the source **after** the DDL will carry the new value.

**If you need historical values backfilled:**
```sql
-- At the source Oracle database
UPDATE CUSTOMERS SET credit_score = <default_value> WHERE credit_score IS NULL;
COMMIT;
-- Debezium captures the UPDATE events → pipeline writes the values to Iceberg
```

---

### 4.7 Column already exists warning (idempotent re-run)

**Symptom:**
```
⚠  [standard] `credit_score` already exists in customers — skipping (idempotent).
```

**Explanation:** The script was already run successfully earlier. This is safe —
the script does not fail, it skips the already-done step and proceeds to verify
and scale up. No action required.

---

## Part 5 — Quick reference card

### All 4 operations — all 3 sources

```bash
# ────────────────────────────────────────────────────────
# ORACLE
# ────────────────────────────────────────────────────────
# ADD (number)
python3 scripts/ddl_apply.py --source oracle --table customers \
    --op add --col credit_score --type "DECIMAL(10,2)"

# ADD (string)
python3 scripts/ddl_apply.py --source oracle --table customers \
    --op add --col loyalty_tier --type STRING

# ADD (timestamp)
python3 scripts/ddl_apply.py --source oracle --table customers \
    --op add --col verified_at --type TIMESTAMP

# MODIFY (widen number)
python3 scripts/ddl_apply.py --source oracle --table customers \
    --op modify --col credit_score --type "DECIMAL(18,4)"

# RENAME
python3 scripts/ddl_apply.py --source oracle --table customers \
    --op rename --col credit_score --new-col credit_score_v2

# DROP
python3 scripts/ddl_apply.py --source oracle --table customers \
    --op drop --col credit_score_v2

# ────────────────────────────────────────────────────────
# POSTGRESQL
# ────────────────────────────────────────────────────────
# ADD (numeric)
python3 scripts/ddl_apply.py --source postgres --table customers \
    --op add --col credit_score --type "DECIMAL(10,2)"

# ADD (string)
python3 scripts/ddl_apply.py --source postgres --table customers \
    --op add --col loyalty_tier --type STRING

# ADD (to a different table)
python3 scripts/ddl_apply.py --source postgres --table orders \
    --op add --col shipped_at --type TIMESTAMP

# MODIFY
python3 scripts/ddl_apply.py --source postgres --table customers \
    --op modify --col credit_score --type "DECIMAL(18,4)"

# RENAME
python3 scripts/ddl_apply.py --source postgres --table customers \
    --op rename --col status --new-col customer_status

# DROP
python3 scripts/ddl_apply.py --source postgres --table customers \
    --op drop --col legacy_notes

# ────────────────────────────────────────────────────────
# MONGODB
# ────────────────────────────────────────────────────────
# ADD (string)
python3 scripts/ddl_apply.py --source mongodb --table customers \
    --op add --col loyalty_tier --type STRING

# ADD (int)
python3 scripts/ddl_apply.py --source mongodb --table customers \
    --op add --col schema_version --type INT

# ADD (decimal)
python3 scripts/ddl_apply.py --source mongodb --table customers \
    --op add --col account_balance --type "DECIMAL(18,4)"

# RENAME
python3 scripts/ddl_apply.py --source mongodb --table customers \
    --op rename --col credit_limit --new-col credit_limit_usd

# DROP
python3 scripts/ddl_apply.py --source mongodb --table customers \
    --op drop --col legacy_segment

# ────────────────────────────────────────────────────────
# MODE FILTERS (any source)
# ────────────────────────────────────────────────────────
# Standard only
python3 scripts/ddl_apply.py --source oracle --table customers \
    --op add --col score --type INT --modes standard

# Standard + soft-delete only
python3 scripts/ddl_apply.py --source postgres --table orders \
    --op add --col tracking_number --type STRING \
    --modes standard,soft_delete

# History tracking only
python3 scripts/ddl_apply.py --source mongodb --table customers \
    --op add --col audit_flag --type BOOLEAN \
    --modes history_tracking

# ────────────────────────────────────────────────────────
# OVERRIDES
# ────────────────────────────────────────────────────────
# Non-default namespace
python3 scripts/ddl_apply.py --source oracle --table products \
    --namespace tpcds --op add --col discount_pct --type "DECIMAL(5,2)"

# Dry-run (no changes)
python3 scripts/ddl_apply.py --source oracle --table customers \
    --op add --col test_col --type STRING --dry-run

# Skip confirmation prompt (scripted use)
python3 scripts/ddl_apply.py --source oracle --table customers \
    --op add --col batch_col --type STRING --yes

# Manual execution pod override (if auto-detection fails)
SPARK_POD=kafka-to-iceberg-postgres-standard-xxxx-yyyy \
    python3 scripts/ddl_apply.py --source oracle --table customers \
    --op add --col col1 --type STRING
```

---

## Part 6 — Iceberg table map

| Source | Write mode | K8s Deployment | Iceberg table |
|---|---|---|---|
| oracle | standard | `kafka-to-iceberg-oracle-standard` | `oracle.cache_testing.<table>` |
| oracle | soft_delete | `kafka-to-iceberg-oracle-soft-delete` | `oracle.cache_testing.<table>_sd` |
| oracle | history_tracking | `kafka-to-iceberg-oracle-history-tracking` | `oracle.cache_testing.<table>_hist` |
| postgres | standard | `kafka-to-iceberg-postgres-standard` | `postgres.cache_testing.<table>` |
| postgres | soft_delete | `kafka-to-iceberg-postgres-soft-delete` | `postgres.cache_testing.<table>_sd` |
| postgres | history_tracking | `kafka-to-iceberg-postgres-history-tracking` | `postgres.cache_testing.<table>_hist` |
| mongodb | standard | `kafka-to-iceberg-mongodb-standard` | `mongodb.cache_testing.<table>` |
| mongodb | soft_delete | `kafka-to-iceberg-mongodb-soft-delete` | `mongodb.cache_testing.<table>_sd` |
| mongodb | history_tracking | `kafka-to-iceberg-mongodb-history-tracking` | `mongodb.cache_testing.<table>_hist` |

---

## Part 7 — Decision tree

```
Source DDL applied at database?
    NO  → Apply source DDL first, then return here
    YES ↓

Know the exact column name and Iceberg type?
    NO  → Check source schema, map to Iceberg type (Part 0 type table above)
    YES ↓

Run dry-run first:
    python3 scripts/ddl_apply.py ... --dry-run

Review all SQL in output.
Correct? YES ↓

Run for real:
    python3 scripts/ddl_apply.py ...

All 5 steps show ✓ ?
    YES → Done. Monitor first 2–3 batches in pod logs.
    NO  → See Part 4 Troubleshooting above.
```

---

## Related runbooks

- [Runbook 29 — CDC Debezium Kafka Iceberg Architecture](runbook-29-cdc-debezium-kafka-iceberg-architecture.md)
- [Runbook 27 — CDC Batch Pipeline E2E Testing](runbook-27-cdc-batch-pipeline-e2e-testing.md)
- [Runbook 03 — Data Streaming](runbook-03-data-streaming.md)

---

*Last updated: automatically maintained — see git history for change log.*
