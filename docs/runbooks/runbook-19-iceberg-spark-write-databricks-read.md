# Runbook 19 — Write Iceberg Data with Spark, Read from Databricks

| Field | Value |
|---|---|
| **Runbook ID** | RB-19 |
| **Service** | k8s-platform / data-lakehouse |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-08-26 |
| **Related** | RB-18 (Initial pipeline setup), RB-15 (Snowflake→Iceberg), RB-01 (OpenBao) |

---

## 1. Purpose

This runbook is the **day-to-day developer guide** for the Spark → Iceberg → Databricks
pipeline. It covers:

1. Understanding the end-to-end architecture (what each component does)
2. Writing new or incremental data to an Iceberg table using Spark
3. Querying that data from the Databricks SQL console (free tier compatible)
4. Verifying the write was successful at every layer

> **Pre-condition:** The one-time infrastructure setup from
> [RB-18](runbook-18-databricks-iceberg-polaris.md) must already be complete —
> the `star_lakehouse` Polaris catalog, S3 bucket, and Spark cluster must exist.

---

## 2. Architecture

### 2.1 How the pipeline works

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Your Spark job (on k8s)                                                │
│                                                                         │
│  spark.table("star_lakehouse.demo.customers")  ← read                  │
│  df.writeTo("star_lakehouse.demo.my_table")    ← write                  │
└──────────────┬──────────────────────────────────────────────────────────┘
               │  Iceberg REST API (OAuth2 client_credentials)
               │  http://polaris-rest.prod.svc.cluster.local:8181
               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Apache Polaris 1.6.0 (k8s, prod namespace)                             │
│                                                                         │
│  Catalog : star_lakehouse                                               │
│  Namespace: demo                                                        │
│  Tables  : customers, <your_tables>                                     │
│                                                                         │
│  • Manages Iceberg metadata (snapshots, manifests, schema)              │
│  • Authorises writes via principal role star_lakehouse_admin            │
│  • stsUnavailable=true  →  no STS calls, uses ambient AWS creds        │
└──────────────┬──────────────────────────────────────────────────────────┘
               │  S3 — writes parquet data + metadata JSON files
               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  AWS S3 — s3://stardata-databricks/                                     │
│                                                                         │
│  iceberg/warehouse/                                                     │
│  └── demo/                                                              │
│      └── <table>/                                                       │
│          ├── metadata/   ← .metadata.json, .avro manifest files        │
│          └── data/       ← snappy-compressed .parquet files            │
└──────────────┬──────────────────────────────────────────────────────────┘
               │  read_files() with inline AWS credentials
               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Databricks SQL (free tier — Serverless Starter Warehouse)              │
│                                                                         │
│  SELECT ... FROM read_files(                                            │
│    's3://stardata-databricks/iceberg/warehouse/demo/<table>/data/',     │
│    format => 'parquet',                                                 │
│    awsAccessKey => '...',                                               │
│    awsSecretKey => '...'                                                │
│  )                                                                      │
│                                                                         │
│  No catalog federation required — reads parquet files directly.        │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Key components

| Component | Location | Role |
|---|---|---|
| Spark master pod | `prod/spark-master-*` | Runs Spark jobs via `spark-submit` |
| Polaris REST catalog | `prod/polaris-*` · `http://192.168.1.50:30181` | Manages Iceberg metadata |
| S3 bucket | `s3://stardata-databricks` · `us-east-2` | Stores parquet + metadata |
| OpenBao | `prod/openbao-0` | All credentials — never hard-coded |
| `bao_spark_init.py` | `/tmp/` on Spark pod | Reads creds + builds `SparkConf` |
| Databricks SQL | `dbc-11a1dbc5-061a.cloud.databricks.com` | Queries data via `read_files()` |

### 2.3 Credentials map

| OpenBao path | Keys used | Used by |
|---|---|---|
| `secret/platform/polaris` | `spark_svc_id`, `spark_svc_secret` | Spark → Polaris OAuth |
| `secret/platform/s3` | `access_key`, `secret_key`, `endpoint`, `region` | Spark S3A + Databricks query |
| `secret/databricks/pat` | `token`, `workspace` | Databricks SQL API |

### 2.4 Iceberg table layout on S3

```
s3://stardata-databricks/iceberg/warehouse/
└── demo/
    └── <table_name>/
        ├── metadata/
        │   ├── 00000-<uuid>.metadata.json   ← created on first write
        │   ├── 00001-<uuid>.metadata.json   ← created on each append/overwrite
        │   ├── snap-<id>-<uuid>.avro        ← snapshot manifest list
        │   └── <uuid>-m0.avro              ← manifest file (data file list)
        └── data/
            └── <partition_key>=<value>/
                └── <uuid>.parquet          ← actual row data (snappy)
```

Each `writeTo(...).append()` adds a **new snapshot** — old data is preserved.
Each `writeTo(...).createOrReplace()` replaces the table — old snapshots are expired.

---

## 3. Pre-flight setup

Run this block once per terminal session before any write or query step.

```bash
# ── OpenBao root token ────────────────────────────────────────────────────────
VAULT_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

# ── AWS credentials ───────────────────────────────────────────────────────────
AWS_KEY=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=access_key secret/platform/s3")
AWS_SECRET=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=secret_key secret/platform/s3")

# ── Databricks PAT ────────────────────────────────────────────────────────────
DB_TOKEN=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=token secret/databricks/pat")
DB_WS="https://dbc-11a1dbc5-061a.cloud.databricks.com"
WAREHOUSE_ID="942026cf5e55f3c3"

# ── Spark pod ─────────────────────────────────────────────────────────────────
SPARK_POD=$(kubectl get pods -n prod | grep spark-master | grep Running \
  | grep -v cleanup | awk '{print $1}' | head -1)

echo "Spark pod : $SPARK_POD"
echo "Environment ready"
```

---

## 4. Writing data to an Iceberg table

### 4.1 Write modes

| Mode | Spark call | When to use |
|---|---|---|
| **Create or replace** | `.createOrReplace()` | First write, or full reload |
| **Append** | `.append()` | Add new rows to existing table |
| **Overwrite by partition** | `.overwritePartitions()` | Replace specific partitions only |

### 4.2 Write a new table (full example)

Create a Python script locally, copy it to the Spark pod, then submit.

**Step 1 — Write the script**

```python
# my_iceberg_write.py
import sys, random
from datetime import date, timedelta
from decimal import Decimal

sys.path.insert(0, "/tmp")
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DateType, DecimalType
)

CATALOG   = "star_lakehouse"
NAMESPACE = "demo"
TABLE     = "my_table"            # ← change this to your table name
FULL_TABLE = f"{CATALOG}.{NAMESPACE}.{TABLE}"
NUM_ROWS  = 1000                  # ← change to however many rows you want

# ── Spark + Polaris config ────────────────────────────────────────────────────
bao  = BaoSparkInit()
pol  = bao.polaris_creds()
s3   = bao.s3_creds()
conf = bao.spark_conf(app_name=f"write-{TABLE}")

conf.set(f"spark.sql.catalog.{CATALOG}",           "org.apache.iceberg.spark.SparkCatalog")
conf.set(f"spark.sql.catalog.{CATALOG}.type",      "rest")
conf.set(f"spark.sql.catalog.{CATALOG}.uri",       "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog")
conf.set(f"spark.sql.catalog.{CATALOG}.credential", f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}")
conf.set(f"spark.sql.catalog.{CATALOG}.scope",      "PRINCIPAL_ROLE:ALL")
conf.set(f"spark.sql.catalog.{CATALOG}.warehouse",  CATALOG)
conf.set(f"spark.sql.catalog.{CATALOG}.s3.access-key-id",     s3["access_key"])
conf.set(f"spark.sql.catalog.{CATALOG}.s3.secret-access-key", s3["secret_key"])
conf.set(f"spark.sql.catalog.{CATALOG}.s3.endpoint",          s3["endpoint"])
conf.set(f"spark.sql.catalog.{CATALOG}.s3.path-style-access",  "true")
conf.set(f"spark.sql.catalog.{CATALOG}.client.region",         s3["region"])

spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── Define schema ─────────────────────────────────────────────────────────────
SCHEMA = StructType([
    StructField("id",         LongType(),        False),
    StructField("name",       StringType(),      False),
    StructField("amount",     DecimalType(15,2), True),
    StructField("event_date", DateType(),        True),
    StructField("status",     StringType(),      False),
])

# ── Generate rows ─────────────────────────────────────────────────────────────
random.seed(42)
statuses = ["active", "pending", "closed"]
rows = [
    (
        i,
        f"record_{i}",
        Decimal(str(round(random.uniform(10, 9999), 2))),
        date(2026, 1, 1) + timedelta(days=random.randint(0, 365)),
        random.choice(statuses),
    )
    for i in range(1, NUM_ROWS + 1)
]

df = spark.createDataFrame(rows, schema=SCHEMA) \
          .withColumn("created_at", F.current_timestamp())

# ── Create namespace if needed ────────────────────────────────────────────────
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")

# ── Write ─────────────────────────────────────────────────────────────────────
# Use .append() instead of .createOrReplace() to add rows to an existing table
df.writeTo(FULL_TABLE) \
  .tableProperty("write.format.default", "parquet") \
  .tableProperty("write.parquet.compression-codec", "snappy") \
  .createOrReplace()

count = spark.table(FULL_TABLE).count()
print(f"✅ wrote {count} rows to {FULL_TABLE}")
spark.stop()
```

**Step 2 — Copy to Spark pod and submit**

```bash
# Copy your script AND bao_spark_init to the pod
kubectl cp my_iceberg_write.py prod/$SPARK_POD:/tmp/my_iceberg_write.py -c spark-master
kubectl cp docker/spark-gluten-velox/scripts/bao_spark_init.py \
  prod/$SPARK_POD:/tmp/bao_spark_init.py -c spark-master

# Submit
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env PYTHONPATH=/tmp DISABLE_GLUTEN=1 TOKEN=$VAULT_TOKEN \
  spark-submit \
    --master spark://spark-master.prod.svc.cluster.local:7077 \
    --conf spark.executor.memory=2g \
    --conf spark.driver.memory=2g \
    /tmp/my_iceberg_write.py 2>&1 | tail -20
```

✅ **Pass:** last line contains `✅ wrote <N> rows to star_lakehouse.demo.my_table`

### 4.3 Append rows to an existing table

Replace `.createOrReplace()` with `.append()` in the script above:

```python
df.writeTo(FULL_TABLE).append()
```

Each append creates a new Iceberg snapshot. The total row count increases.
Databricks will see all rows (all snapshots merged) on the next query.

### 4.4 Write to the existing `customers` table

To add rows to the already-created `demo.customers` table, use the same
schema from [`generate_customers_iceberg.py`](../../scripts/databricks-iceberg-polaris/generate_customers_iceberg.py)
and call `.append()`. The `customer_id` values must not collide with existing rows
(Iceberg does not enforce uniqueness — duplicates will simply appear in query results).

---

## 5. Verifying the write

### 5.1 Check Iceberg table via Polaris REST API

```bash
# ── Get Polaris token ─────────────────────────────────────────────────────────
POLARIS_ROOT_ID=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=root_client_id secret/platform/polaris")
POLARIS_ROOT_SECRET=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=root_client_secret secret/platform/polaris")
POLARIS_TOKEN=$(curl -s -X POST \
  http://192.168.1.50:30181/api/catalog/v1/oauth/tokens \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=${POLARIS_ROOT_ID}&client_secret=${POLARIS_ROOT_SECRET}&scope=PRINCIPAL_ROLE:ALL" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# ── List tables in demo namespace ─────────────────────────────────────────────
curl -s "http://192.168.1.50:30181/api/catalog/v1/star_lakehouse/namespaces/demo/tables" \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  | python3 -c "
import sys,json
tables=[i['name'] for i in json.load(sys.stdin).get('identifiers',[])]
print('Tables in star_lakehouse.demo:', tables)
"
```

✅ **Pass:** your table name appears in the list.

### 5.2 Check S3 data files

```bash
python3 - <<EOF
import boto3
s3 = boto3.client('s3', region_name='us-east-2',
    aws_access_key_id='$AWS_KEY',
    aws_secret_access_key='$AWS_SECRET')

TABLE_NAME = "my_table"   # ← your table name
prefix = f"iceberg/warehouse/demo/{TABLE_NAME}/"

r = s3.list_objects_v2(Bucket='stardata-databricks', Prefix=prefix)
objects = r.get('Contents', [])
for o in objects[:10]:
    print(o['Key'][:90], f"({o['Size']} bytes)")
print(f"\nTotal: {r['KeyCount']} objects")

has_meta = any('metadata/' in o['Key'] for o in objects)
has_data = any('data/'     in o['Key'] for o in objects)
print(f"metadata/: {has_meta}   data/: {has_data}")
EOF
```

✅ **Pass:** both `metadata/` and `data/` directories appear with files.

---

## 6. Querying from Databricks SQL console

### 6.1 How to open the console

1. Go to **`https://dbc-11a1dbc5-061a.cloud.databricks.com`**
2. Left sidebar → **SQL Editor**
3. Select warehouse: **Serverless Starter Warehouse** (starts automatically)

### 6.2 Query template — paste into SQL Editor

Replace `<table_name>` with your Iceberg table name (e.g. `customers`, `my_table`).

```sql
-- ── Row count ────────────────────────────────────────────────────────────────
SELECT COUNT(*) AS total_rows
FROM read_files(
  's3://stardata-databricks/iceberg/warehouse/demo/<table_name>/data/',
  format        => 'parquet',
  awsAccessKey  => '<AWS_ACCESS_KEY>',
  awsSecretKey  => '<AWS_SECRET_KEY>'
);

-- ── Sample 10 rows ───────────────────────────────────────────────────────────
SELECT *
FROM read_files(
  's3://stardata-databricks/iceberg/warehouse/demo/<table_name>/data/',
  format        => 'parquet',
  awsAccessKey  => '<AWS_ACCESS_KEY>',
  awsSecretKey  => '<AWS_SECRET_KEY>'
)
LIMIT 10;

-- ── Filter by column value ───────────────────────────────────────────────────
SELECT customer_id, full_name, email, customer_tier
FROM read_files(
  's3://stardata-databricks/iceberg/warehouse/demo/customers/data/',
  format        => 'parquet',
  awsAccessKey  => '<AWS_ACCESS_KEY>',
  awsSecretKey  => '<AWS_SECRET_KEY>'
)
WHERE customer_tier = 'platinum'
LIMIT 20;
```

> **Where to get the credentials:**
> Run the pre-flight block (Section 3) and print `$AWS_KEY` / `$AWS_SECRET`.
> These are the `watsonx-s3-connector` IAM user credentials stored in OpenBao
> at `secret/platform/s3`.

### 6.3 Query via API (automated)

Use this from scripts or CI to avoid opening the browser:

```bash
python3 - "$AWS_KEY" "$AWS_SECRET" <<'PYEOF'
import sys, json, time, urllib.request

DB_WS        = "https://dbc-11a1dbc5-061a.cloud.databricks.com"
DB_TOKEN     = "$(kubectl exec -n prod openbao-0 -- sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=token secret/databricks/pat")"
WAREHOUSE_ID = "942026cf5e55f3c3"
TABLE_NAME   = "customers"      # ← change to your table name
AWS_KEY, AWS_SECRET = sys.argv[1], sys.argv[2]

S3_PATH = f"s3://stardata-databricks/iceberg/warehouse/demo/{TABLE_NAME}/data/"
HDRS    = {"Authorization": f"Bearer {DB_TOKEN}",
           "Content-Type": "application/json"}

def run_sql(sql):
    req = urllib.request.Request(
        f"{DB_WS}/api/2.0/sql/statements",
        data=json.dumps({"warehouse_id": WAREHOUSE_ID,
                         "wait_timeout": "30s",
                         "statement": sql}).encode(),
        method="POST", headers=HDRS)
    with urllib.request.urlopen(req, timeout=60) as r:
        d = json.loads(r.read())

    # Poll if still pending
    for _ in range(20):
        if d.get("status", {}).get("state") in ("SUCCEEDED","FAILED","CANCELED"):
            break
        sid = d["statement_id"]
        time.sleep(4)
        req2 = urllib.request.Request(
            f"{DB_WS}/api/2.0/sql/statements/{sid}", headers=HDRS)
        with urllib.request.urlopen(req2, timeout=30) as r:
            d = json.loads(r.read())

    err = d.get("status", {}).get("error", {}).get("message", "")
    if err:
        raise RuntimeError(f"SQL error: {err}")

    cols = [c["name"] for c in d.get("manifest",{}).get("schema",{}).get("columns",[])]
    rows = d.get("result", {}).get("data_array", [])
    return cols, rows

cred = f"awsAccessKey => '{AWS_KEY}', awsSecretKey => '{AWS_SECRET}'"
base = f"read_files('{S3_PATH}', format => 'parquet', {cred})"

# Row count
cols, rows = run_sql(f"SELECT COUNT(*) AS total FROM {base}")
total = int(rows[0][0])
print(f"Total rows : {total}")

# Sample
cols, rows = run_sql(f"SELECT * FROM {base} LIMIT 5")
print("\nSample rows:")
print("  " + " | ".join(f"{c:<20}" for c in cols[:6]))
print("  " + "-" * 130)
for r in rows:
    print("  " + " | ".join(f"{str(x):<20}" for x in r[:6]))
PYEOF
```

### 6.4 Understanding query results vs Iceberg snapshots

`read_files()` reads **all parquet files** under the `data/` prefix — it is **not
snapshot-aware**. This means:

| Scenario | Behaviour |
|---|---|
| You ran `.createOrReplace()` once | ✅ Correct — reads exactly those rows |
| You ran `.append()` twice | ✅ Correct — reads both batches combined |
| You ran `.createOrReplace()` twice | ⚠️ May double-count — old files may still be on S3 until Iceberg expires them |

For snapshot-correct reads on free Databricks, use the Polaris REST API (Section 5.1),
run a count via Spark inside the cluster (Section 7), or use Option B below (§6.5).

### 6.5 Option B — Snapshot-correct reads from a Databricks notebook

When `read_files()` is not sufficient (e.g. after multiple appends, or when you need
full Iceberg semantics like time-travel or snapshot history), use a Databricks notebook
that connects directly to the Polaris REST catalog via Spark + OAuth2.

> **Why this works:** The notebook configures `star_lakehouse` as a Spark
> `SparkCatalog` pointing at `http://192.168.1.50:30181/api/catalog` — the same path
> used by the on-cluster Spark jobs. No Unity Catalog federation (`FOREIGN` catalog)
> is needed, so it works on the free Databricks tier without any account entitlement.

**Pre-conditions:**
- Databricks cluster with Iceberg JARs attached (see [RB-18 §10.1](runbook-18-databricks-iceberg-polaris.md#101-prerequisites))
- Polaris `spark-iceberg-svc` principal has `star_lakehouse_admin` role (see §8 troubleshooting)
- AWS credentials available (Databricks secret scope or cluster env vars)

**Run the notebook:**

```bash
# Upload once — then open and run from the Databricks UI
export DATABRICKS_HOST=https://dbc-11a1dbc5-061a.cloud.databricks.com
export DATABRICKS_TOKEN=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=token secret/databricks/pat")

databricks workspace import \
  scripts/databricks-iceberg-polaris/databricks_notebook_polaris_read.py \
  /Shared/star-lakehouse/databricks_notebook_polaris_read \
  --language PYTHON --overwrite

echo "Open: https://dbc-11a1dbc5-061a.cloud.databricks.com/#workspace/Shared/star-lakehouse/databricks_notebook_polaris_read"
```

**What the notebook checks:**

| Cell | Query | Expected |
|---|---|---|
| T-10a | `SELECT COUNT(*) FROM star_lakehouse.demo.customers` | 10 000 rows |
| T-10b | `SELECT * … LIMIT 10` | Real customer data visible |
| T-10c | Tier distribution | 4 tiers — standard / silver / gold / platinum |
| T-10d | `SELECT … FROM .snapshots` | ≥ 1 committed Iceberg snapshot |
| T-10e | Schema validation | All 15 columns present |

Full setup, expected output, and troubleshooting: [RB-18 §10](runbook-18-databricks-iceberg-polaris.md#10-option-b--direct-polaris-read-from-databricks-notebook).

---

## 7. Quick end-to-end test script

This script does everything in one shot: writes 500 new rows to a test table,
then queries it from Databricks to confirm the count.

```bash
# Save as e2e_test.sh — run after sourcing the pre-flight block (Section 3)

TABLE="test_$(date +%s)"    # unique table name per run
ROWS=500

cat > /tmp/e2e_write.py <<PYEOF
import sys, random
from decimal import Decimal
from datetime import date, timedelta
sys.path.insert(0, "/tmp")
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

CATALOG, NS, TABLE = "star_lakehouse", "demo", "${TABLE}"
bao = BaoSparkInit()
pol = bao.polaris_creds()
s3  = bao.s3_creds()
conf = bao.spark_conf(app_name="e2e-test")
conf.set(f"spark.sql.catalog.{CATALOG}",             "org.apache.iceberg.spark.SparkCatalog")
conf.set(f"spark.sql.catalog.{CATALOG}.type",        "rest")
conf.set(f"spark.sql.catalog.{CATALOG}.uri",         "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog")
conf.set(f"spark.sql.catalog.{CATALOG}.credential",  f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}")
conf.set(f"spark.sql.catalog.{CATALOG}.scope",       "PRINCIPAL_ROLE:ALL")
conf.set(f"spark.sql.catalog.{CATALOG}.warehouse",   CATALOG)
conf.set(f"spark.sql.catalog.{CATALOG}.s3.access-key-id",     s3["access_key"])
conf.set(f"spark.sql.catalog.{CATALOG}.s3.secret-access-key", s3["secret_key"])
conf.set(f"spark.sql.catalog.{CATALOG}.s3.endpoint",          s3["endpoint"])
conf.set(f"spark.sql.catalog.{CATALOG}.s3.path-style-access",  "true")
conf.set(f"spark.sql.catalog.{CATALOG}.client.region",         s3["region"])

spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NS}")

schema = StructType([
    StructField("id",    LongType(),   False),
    StructField("label", StringType(), False),
    StructField("value", DecimalType(10,2), True),
])
random.seed(99)
rows = [(i, f"item_{i}", Decimal(str(round(random.uniform(1,100),2)))) for i in range(1, ${ROWS}+1)]
df = spark.createDataFrame(rows, schema).withColumn("ts", F.current_timestamp())
df.writeTo(f"{CATALOG}.{NS}.{TABLE}").tableProperty("write.format.default","parquet").createOrReplace()
count = spark.table(f"{CATALOG}.{NS}.{TABLE}").count()
print(f"✅ Spark write: {count} rows → {CATALOG}.{NS}.{TABLE}")
spark.stop()
PYEOF

# Copy + submit the write job
kubectl cp /tmp/e2e_write.py prod/$SPARK_POD:/tmp/e2e_write.py -c spark-master
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env PYTHONPATH=/tmp DISABLE_GLUTEN=1 TOKEN=$VAULT_TOKEN \
  spark-submit \
    --master spark://spark-master.prod.svc.cluster.local:7077 \
    --conf spark.executor.memory=2g \
    --conf spark.driver.memory=2g \
    /tmp/e2e_write.py 2>&1 | grep -E "✅|ERROR|Exception"

# Query from Databricks
echo ""
echo "Querying from Databricks..."
python3 - "$AWS_KEY" "$AWS_SECRET" "$TABLE" <<'PYEOF'
import sys, json, time, urllib.request
DB_WS="https://dbc-11a1dbc5-061a.cloud.databricks.com"
DB_TOKEN="<DB_TOKEN_HERE>"   # replace or source from pre-flight block
WAREHOUSE_ID="942026cf5e55f3c3"
AWS_KEY, AWS_SECRET, TABLE = sys.argv[1], sys.argv[2], sys.argv[3]
S3_PATH=f"s3://stardata-databricks/iceberg/warehouse/demo/{TABLE}/data/"
HDRS={"Authorization":f"Bearer {DB_TOKEN}","Content-Type":"application/json"}
def run(sql):
    req=urllib.request.Request(f"{DB_WS}/api/2.0/sql/statements",
        data=json.dumps({"warehouse_id":WAREHOUSE_ID,"wait_timeout":"30s","statement":sql}).encode(),
        method="POST",headers=HDRS)
    with urllib.request.urlopen(req,timeout=60) as r: d=json.loads(r.read())
    for _ in range(20):
        if d.get("status",{}).get("state") in ("SUCCEEDED","FAILED","CANCELED"): break
        sid=d["statement_id"]; time.sleep(4)
        req2=urllib.request.Request(f"{DB_WS}/api/2.0/sql/statements/{sid}",headers=HDRS)
        with urllib.request.urlopen(req2,timeout=30) as r: d=json.loads(r.read())
    return d.get("result",{}).get("data_array",[])
cred=f"awsAccessKey => '{AWS_KEY}', awsSecretKey => '{AWS_SECRET}'"
rows=run(f"SELECT COUNT(*) FROM read_files('{S3_PATH}', format=>'parquet', {cred})")
total=int(rows[0][0]) if rows else -1
print(f"Databricks count: {total}")
print("✅ E2E PASS" if total==500 else f"❌ E2E FAIL — expected 500 got {total}")
PYEOF
```

---

## 8. Troubleshooting

### Spark write fails: `ForbiddenException: not authorized for CREATE_TABLE`

`spark-iceberg-svc` lacks `star_lakehouse_admin` role. Fix:

```bash
VAULT_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)
POLARIS_ROOT_ID=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=root_client_id secret/platform/polaris")
POLARIS_ROOT_SECRET=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=root_client_secret secret/platform/polaris")
POLARIS_TOKEN=$(curl -s -X POST http://192.168.1.50:30181/api/catalog/v1/oauth/tokens \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=${POLARIS_ROOT_ID}&client_secret=${POLARIS_ROOT_SECRET}&scope=PRINCIPAL_ROLE:ALL" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

curl -s -X PUT \
  "http://192.168.1.50:30181/api/management/v1/principals/spark-iceberg-svc/principal-roles" \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"principalRole": {"name": "star_lakehouse_admin"}}'
```

### Spark write fails: `StsException: not authorized to perform sts:AssumeRole`

The `stsUnavailable` flag was reset (e.g. by a Polaris pod restart that re-read old DB state).
Re-apply the DB fix:

```bash
PG_POD=$(kubectl get pods -n prod -l app=postgresql --no-headers \
  -o custom-columns=NAME:.metadata.name | head -1)

kubectl exec -n prod $PG_POD -- \
  env PGPASSWORD="postgres" psql -U postgres -d polaris -c "
UPDATE polaris_schema.entities
SET internal_properties = jsonb_set(
  internal_properties,
  '{storage_configuration_info}',
  to_jsonb('{\"@type\":\"AwsStorageConfigurationInfo\",\"allowedLocations\":[\"s3://stardata-databricks/iceberg/warehouse\"],\"roleARN\":\"arn:aws:iam::586643076710:user/watsonx-s3-connector\",\"allowedKmsKeys\":[],\"externalId\":\"polaris-iceberg\",\"region\":\"us-east-2\",\"pathStyleAccess\":false,\"stsUnavailable\":true,\"fileIoImplClassName\":\"org.apache.iceberg.aws.s3.S3FileIO\",\"storageType\":\"S3\"}'::text)
)
WHERE name = 'star_lakehouse';"

kubectl rollout restart deployment/polaris -n prod
kubectl rollout status deployment/polaris -n prod --timeout=90s
```

### Databricks SQL: `read_files` returns no rows after append

The `data/` prefix scan is immediate — no cache. Causes:
- The write job didn't finish → wait for `✅ wrote N rows` in Spark output
- Wrong table name in the S3 path → check `s3://stardata-databricks/iceberg/warehouse/demo/` in S3

### Databricks SQL warehouse is STOPPED

```bash
curl -s -X POST \
  "$DB_WS/api/2.0/sql/warehouses/$WAREHOUSE_ID/start" \
  -H "Authorization: Bearer $DB_TOKEN"
# Wait ~30 seconds then retry the query
```

### `ModuleNotFoundError: No module named 'bao_spark_init'`

The helper wasn't copied to the pod this session:

```bash
kubectl cp docker/spark-gluten-velox/scripts/bao_spark_init.py \
  prod/$SPARK_POD:/tmp/bao_spark_init.py -c spark-master
```

---

## 9. Key paths reference

| Resource | Path / URL |
|---|---|
| S3 data root | `s3://stardata-databricks/iceberg/warehouse/demo/` |
| Polaris catalog API | `http://192.168.1.50:30181/api/catalog/v1/star_lakehouse/` |
| Polaris management API | `http://192.168.1.50:30181/api/management/v1/` |
| Polaris in-cluster URI | `http://polaris-rest.prod.svc.cluster.local:8181/api/catalog` |
| Databricks workspace | `https://dbc-11a1dbc5-061a.cloud.databricks.com` |
| Databricks SQL editor | `https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor` |
| Databricks SQL warehouse | `942026cf5e55f3c3` (Serverless Starter) |
| `bao_spark_init.py` (source) | `docker/spark-gluten-velox/scripts/bao_spark_init.py` |
| `generate_customers_iceberg.py` | `scripts/databricks-iceberg-polaris/generate_customers_iceberg.py` |
| `starpump_to_databricks.py` | `scripts/databricks-iceberg-polaris/starpump_to_databricks.py` |
| Databricks notebook (Option B) | `scripts/databricks-iceberg-polaris/databricks_notebook_polaris_read.py` |
