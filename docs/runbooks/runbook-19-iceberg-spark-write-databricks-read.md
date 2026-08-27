# Runbook 19 — Write Iceberg Data with Spark, Read from Databricks

| Field | Value |
|---|---|
| **Runbook ID** | RB-19 |
| **Service** | k8s-platform / data-lakehouse |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-08-27 |
| **Related** | RB-18 (infrastructure setup), RB-15 (Snowflake→Iceberg), RB-01 (OpenBao) |

---

## 1. Purpose

**Day-to-day developer guide** for writing data to an Iceberg table with Spark and reading it from Databricks SQL.

1. Write new or incremental data to an Iceberg table using Spark
2. Register the table in HMS so Databricks picks up the latest snapshot
3. Query from the Databricks SQL console via the `star_lakehouse` FOREIGN catalog

> **Pre-condition:** The one-time infrastructure setup from
> [RB-18](runbook-18-databricks-iceberg-polaris.md) must already be complete —
> the `star_lakehouse` Polaris catalog, S3 bucket, Hive Metastore, and Databricks
> FOREIGN catalog must all exist.

---

## 2. Architecture

### 2.1 How the pipeline works

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Your Spark job (on k8s)                                                    │
│                                                                             │
│  df.writeTo("star_lakehouse.demo.my_table").createOrReplace()   ← write     │
│  df.writeTo("star_lakehouse.demo.my_table").append()            ← append    │
└──────────────┬──────────────────────────────────────────────────────────────┘
               │  Iceberg REST API (OAuth2 client_credentials)
               │  http://polaris-rest.prod.svc.cluster.local:8181/api/catalog
               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Apache Polaris 1.6.0 (k8s, prod namespace)                                 │
│                                                                             │
│  Catalog : star_lakehouse                                                   │
│  Namespace: demo                                                            │
│                                                                             │
│  • Manages Iceberg metadata (snapshots, manifests, schema)                  │
│  • Authorises writes via principal role star_lakehouse_admin                │
└──────────────┬──────────────────────────────────────────────────────────────┘
               │  writes parquet data + metadata JSON files
               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  AWS S3 — s3://stardata-databricks/                                         │
│                                                                             │
│  iceberg/warehouse/demo/<table>/                                            │
│    metadata/  ← .metadata.json, snap-*.avro manifest files                 │
│    data/      ← snappy-compressed .parquet files                           │
└──────────────┬──────────────────────────────────────────────────────────────┘
               │  HMS registration: update metadata_location after each write
               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Hive Metastore 2.3.9 (k8s, prod namespace)                                 │
│                                                                             │
│  hive-metastore.prod.svc.cluster.local:9083                                 │
│  Stores EXTERNAL_TABLE entry: table_type=ICEBERG, metadata_location=<s3>   │
└──────────────┬──────────────────────────────────────────────────────────────┘
               │  HIVE_METASTORE FOREIGN catalog (hms_star_lakehouse)
               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Databricks Unity Catalog — star_lakehouse (FOREIGN_CATALOG)                │
│                                                                             │
│  SELECT * FROM star_lakehouse.demo.<table>                                  │
│  SHOW TABLES IN star_lakehouse.demo                                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Key components

| Component | Location | Role |
|---|---|---|
| Spark master pod | `prod/spark-master-*` | Runs Spark jobs via `spark-submit` |
| Polaris REST catalog | `prod/polaris-*` · `http://192.168.1.50:30181` | Manages Iceberg metadata |
| Hive Metastore | `prod/hive-metastore-*` · `192.168.1.50:30983` | HMS FOREIGN catalog bridge to Databricks |
| S3 bucket | `s3://stardata-databricks` · `us-east-2` | Stores parquet + metadata |
| OpenBao | `prod/openbao-0` | All credentials — never hard-coded |
| `bao_spark_init.py` | `/tmp/` on Spark pod | Reads creds from OpenBao + builds `SparkConf` |
| Databricks SQL | `dbc-11a1dbc5-061a.cloud.databricks.com` | Queries via `star_lakehouse` FOREIGN catalog |

### 2.3 Credentials map

| OpenBao path | Keys used | Used by |
|---|---|---|
| `secret/platform/polaris` | `spark_svc_id`, `spark_svc_secret` | Spark → Polaris OAuth |
| `secret/platform/s3` | `access_key`, `secret_key`, `endpoint`, `region` | Spark S3A + HMS registration |
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
            └── <partition>/
                └── <uuid>.parquet          ← actual row data (snappy)
```

Each `writeTo(...).append()` adds a new Iceberg snapshot — old data is preserved.
Each `writeTo(...).createOrReplace()` replaces the table — a fresh snapshot is created.

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
SPARK_POD=$(kubectl get pods -n prod -l app=spark,component=master \
  --no-headers -o custom-columns=NAME:.metadata.name | head -1)

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
kubectl cp my_iceberg_write.py prod/$SPARK_POD:/tmp/my_iceberg_write.py -c spark-master
kubectl cp docker/spark-gluten-velox/scripts/bao_spark_init.py \
  prod/$SPARK_POD:/tmp/bao_spark_init.py -c spark-master

kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env PYTHONPATH=/tmp DISABLE_GLUTEN=1 TOKEN=$VAULT_TOKEN \
  spark-submit \
    --master spark://spark-master-internal.prod.svc.cluster.local:17077 \
    --conf spark.executor.memory=2g \
    --conf spark.driver.memory=2g \
    /tmp/my_iceberg_write.py 2>&1 | tail -5
```

✅ **Pass:** last line contains `✅ wrote <N> rows to star_lakehouse.demo.my_table`

### 4.3 Append rows to an existing table

Replace `.createOrReplace()` with `.append()` in the script above:

```python
df.writeTo(FULL_TABLE).append()
```

Each append creates a new Iceberg snapshot. The total row count increases.
**Remember:** re-run Step 2 in Section 5 (HMS registration) after every append so Databricks sees all rows.

### 4.4 Write to the existing `customers` table

Use the schema from [`generate_customers_iceberg.py`](../../scripts/databricks-iceberg-polaris/generate_customers_iceberg.py)
and call `.append()`. `customer_id` values must not collide with existing rows
(Iceberg does not enforce uniqueness — duplicates will appear in query results).

---

## 5. After writing — register the table in HMS

**This step is required after every write.** Databricks reads the `metadata_location` pointer stored in HMS. If you skip this step, Databricks will see the snapshot from the previous registration, not the latest data.

```bash
TABLE_NAME="my_table"    # ← change to your table name

# Step A — find the latest metadata.json on S3
METADATA_FILE=$(python3 - "$AWS_KEY" "$AWS_SECRET" "$TABLE_NAME" <<'PYEOF'
import sys, boto3
KEY, SEC, TBL = sys.argv[1], sys.argv[2], sys.argv[3]
s3 = boto3.client('s3', region_name='us-east-2',
    aws_access_key_id=KEY, aws_secret_access_key=SEC)
r = s3.list_objects_v2(
    Bucket='stardata-databricks',
    Prefix=f'iceberg/warehouse/demo/{TBL}/metadata/')
objects = [o for o in r.get('Contents',[]) if o['Key'].endswith('.metadata.json')]
objects.sort(key=lambda x: x['LastModified'], reverse=True)
print(f"s3://stardata-databricks/{objects[0]['Key']}")
PYEOF
)
echo "Latest metadata: $METADATA_FILE"

# Step B — register (or re-register) the table in HMS via Thrift
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env PYTHONPATH=/tmp TOKEN=$VAULT_TOKEN \
  python3 - "$TABLE_NAME" "$METADATA_FILE" <<'PYEOF'
import sys, time, getpass
from thrift.transport import TSocket, TTransport
from thrift.protocol  import TBinaryProtocol
from hive_metastore   import ThriftHiveMetastore
from hive_metastore.ttypes import Table, StorageDescriptor, SerDeInfo

TABLE_NAME    = sys.argv[1]
METADATA_FILE = sys.argv[2]
TABLE_LOCATION = METADATA_FILE.rsplit('/metadata/', 1)[0]

t = TTransport.TBufferedTransport(
    TSocket.TSocket("hive-metastore.prod.svc.cluster.local", 9083))
c = ThriftHiveMetastore.Client(TBinaryProtocol.TBinaryProtocol(t))
t.open()

# Drop stale entry if present
if TABLE_NAME in c.get_all_tables("demo"):
    c.drop_table("demo", TABLE_NAME, deleteData=False)

ts = int(time.time())
c.create_table(Table(
    dbName="demo", tableName=TABLE_NAME,
    owner=getpass.getuser(),
    createTime=ts, lastAccessTime=ts,
    tableType="EXTERNAL_TABLE",
    sd=StorageDescriptor(
        cols=[], location=TABLE_LOCATION,
        inputFormat="org.apache.iceberg.mr.hive.HiveIcebergInputFormat",
        outputFormat="org.apache.iceberg.mr.hive.HiveIcebergOutputFormat",
        compressed=False,
        serdeInfo=SerDeInfo(
            serializationLib="org.apache.iceberg.mr.hive.HiveIcebergSerDe",
            parameters={}),
        parameters={}),
    parameters={
        "table_type":      "ICEBERG",
        "metadata_location": METADATA_FILE,
        "EXTERNAL":        "TRUE",
    }
))
print(f"✅ HMS: demo.{TABLE_NAME} registered → {METADATA_FILE}")
t.close()
PYEOF
```

✅ **Pass:** prints `✅ HMS: demo.my_table registered → s3://...metadata.json`

---

## 6. Querying from Databricks SQL

### 6.1 How to open the SQL editor

1. Go to **`https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor`**
2. Select warehouse: **Serverless Starter Warehouse** (auto-starts on first query)

### 6.2 Query the table — paste into SQL Editor

Replace `<table_name>` with your Iceberg table name (e.g. `my_table`, `customers`).

```sql
-- Confirm the table is visible via the FOREIGN catalog
SHOW TABLES IN star_lakehouse.demo;

-- Row count
SELECT COUNT(*) AS total_rows
FROM star_lakehouse.demo.<table_name>;

-- Sample 10 rows
SELECT *
FROM star_lakehouse.demo.<table_name>
LIMIT 10;

-- Filter example (customers table)
SELECT customer_id, full_name, email, customer_tier
FROM star_lakehouse.demo.customers
WHERE customer_tier = 'platinum'
LIMIT 20;
```

> **First-query latency:** FOREIGN catalog queries take 30–90 seconds on first execution
> while the warehouse cold-starts. Subsequent queries are much faster.

> **Stale count?** If you just wrote new rows but the count is unchanged, you skipped the
> HMS registration step. Re-run Section 5 with your table name.

### 6.3 Query via API (automated / scripted)

Use this from scripts or CI to avoid opening the browser:

```bash
python3 - "$DB_TOKEN" <<'PYEOF'
import sys, json, time, urllib.request

DB_WS        = "https://dbc-11a1dbc5-061a.cloud.databricks.com"
DB_TOKEN     = sys.argv[1]
WAREHOUSE_ID = "942026cf5e55f3c3"
TABLE_NAME   = "customers"      # ← change to your table name

HDRS = {"Authorization": f"Bearer {DB_TOKEN}", "Content-Type": "application/json"}

def run_sql(sql):
    req = urllib.request.Request(
        f"{DB_WS}/api/2.0/sql/statements",
        data=json.dumps({"warehouse_id": WAREHOUSE_ID,
                         "wait_timeout": "90s",
                         "statement": sql}).encode(),
        method="POST", headers=HDRS)
    with urllib.request.urlopen(req, timeout=120) as r:
        d = json.loads(r.read())

    # Poll if not yet complete
    for _ in range(30):
        state = d.get("status", {}).get("state")
        if state in ("SUCCEEDED", "FAILED", "CANCELED"):
            break
        sid = d["statement_id"]
        time.sleep(5)
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

# Row count
cols, rows = run_sql(f"SELECT COUNT(*) AS total FROM star_lakehouse.demo.{TABLE_NAME}")
print(f"Total rows : {rows[0][0]}")

# Sample
cols, rows = run_sql(f"SELECT * FROM star_lakehouse.demo.{TABLE_NAME} LIMIT 5")
print("\nSample rows:")
print("  " + " | ".join(f"{c:<20}" for c in cols[:6]))
print("  " + "-" * 130)
for r in rows:
    print("  " + " | ".join(f"{str(x):<20}" for x in r[:6]))
PYEOF
```

---

## 7. Verifying the write on S3 and Polaris

### 7.1 Check S3 data files

```bash
python3 - "$AWS_KEY" "$AWS_SECRET" "$TABLE_NAME" <<'EOF'
import sys, boto3
KEY, SEC, TBL = sys.argv[1], sys.argv[2], sys.argv[3]
s3 = boto3.client('s3', region_name='us-east-2',
    aws_access_key_id=KEY, aws_secret_access_key=SEC)

prefix = f"iceberg/warehouse/demo/{TBL}/"
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

### 7.2 Check Iceberg table via Polaris REST API

```bash
POLARIS_ROOT_ID=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=root_client_id secret/platform/polaris")
POLARIS_ROOT_SECRET=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=root_client_secret secret/platform/polaris")
POLARIS_TOKEN=$(curl -s -X POST \
  http://192.168.1.50:30181/api/catalog/v1/oauth/tokens \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=${POLARIS_ROOT_ID}&client_secret=${POLARIS_ROOT_SECRET}&scope=PRINCIPAL_ROLE:ALL" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

curl -s "http://192.168.1.50:30181/api/catalog/v1/star_lakehouse/namespaces/demo/tables" \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  | python3 -c "
import sys,json
tables=[i['name'] for i in json.load(sys.stdin).get('identifiers',[])]
print('Tables in star_lakehouse.demo:', tables)
"
```

✅ **Pass:** your table name appears in the list.

### 7.3 Verify HMS registration

```bash
kubectl exec -n prod $(kubectl get pods -n prod -l app=hive-metastore \
  --no-headers -o custom-columns=NAME:.metadata.name | head -1) -- \
  python3 - <<'PYEOF'
from thrift.transport import TSocket, TTransport
from thrift.protocol  import TBinaryProtocol
from hive_metastore   import ThriftHiveMetastore

t = TTransport.TBufferedTransport(
    TSocket.TSocket("hive-metastore.prod.svc.cluster.local", 9083))
c = ThriftHiveMetastore.Client(TBinaryProtocol.TBinaryProtocol(t))
t.open()
for db in c.get_all_databases():
    for tbl in c.get_all_tables(db):
        entry = c.get_table(db, tbl)
        meta = entry.parameters.get("metadata_location", "n/a")
        print(f"  {db}.{tbl:30s}  {meta[-70:]}")
t.close()
PYEOF
```

✅ **Pass:** your table appears with a `metadata_location` pointing at the latest `.metadata.json`.

---

## 8. Quick end-to-end test

Writes 500 rows to a timestamped test table, registers it in HMS, then queries it from Databricks — all in one shot.

```bash
# Run after sourcing the pre-flight block (Section 3)
TABLE="test_$(date +%s)"

# ── Step 1: Write ─────────────────────────────────────────────────────────────
cat > /tmp/e2e_write.py <<PYEOF
import sys, random
from decimal import Decimal
from datetime import date, timedelta
sys.path.insert(0, "/tmp")
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, DecimalType

CATALOG, NS, TABLE = "star_lakehouse", "demo", "${TABLE}"
bao  = BaoSparkInit()
pol  = bao.polaris_creds()
s3   = bao.s3_creds()
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
    StructField("id",    LongType(),          False),
    StructField("label", StringType(),         False),
    StructField("value", DecimalType(10,2),    True),
])
random.seed(99)
rows = [(i, f"item_{i}", Decimal(str(round(random.uniform(1,100),2)))) for i in range(1, 501)]
df = spark.createDataFrame(rows, schema).withColumn("ts", F.current_timestamp())
df.writeTo(f"{CATALOG}.{NS}.{TABLE}").tableProperty("write.format.default","parquet").createOrReplace()
count = spark.table(f"{CATALOG}.{NS}.{TABLE}").count()
print(f"✅ Spark write: {count} rows → {CATALOG}.{NS}.{TABLE}")
spark.stop()
PYEOF

kubectl cp /tmp/e2e_write.py prod/$SPARK_POD:/tmp/e2e_write.py -c spark-master
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env PYTHONPATH=/tmp DISABLE_GLUTEN=1 TOKEN=$VAULT_TOKEN \
  spark-submit \
    --master spark://spark-master-internal.prod.svc.cluster.local:17077 \
    --conf spark.executor.memory=2g \
    /tmp/e2e_write.py 2>&1 | grep -E "✅|ERROR|Exception"

# ── Step 2: Register in HMS ───────────────────────────────────────────────────
METADATA_FILE=$(python3 - "$AWS_KEY" "$AWS_SECRET" "$TABLE" <<'PYEOF'
import sys, boto3
KEY, SEC, TBL = sys.argv[1], sys.argv[2], sys.argv[3]
s3 = boto3.client('s3', region_name='us-east-2',
    aws_access_key_id=KEY, aws_secret_access_key=SEC)
r = s3.list_objects_v2(Bucket='stardata-databricks',
    Prefix=f'iceberg/warehouse/demo/{TBL}/metadata/')
objects = [o for o in r.get('Contents',[]) if o['Key'].endswith('.metadata.json')]
objects.sort(key=lambda x: x['LastModified'], reverse=True)
print(f"s3://stardata-databricks/{objects[0]['Key']}")
PYEOF
)
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env PYTHONPATH=/tmp TOKEN=$VAULT_TOKEN \
  python3 - "$TABLE" "$METADATA_FILE" <<'PYEOF'
import sys, time, getpass
from thrift.transport import TSocket, TTransport
from thrift.protocol  import TBinaryProtocol
from hive_metastore   import ThriftHiveMetastore
from hive_metastore.ttypes import Table, StorageDescriptor, SerDeInfo
TABLE_NAME, METADATA_FILE = sys.argv[1], sys.argv[2]
TABLE_LOCATION = METADATA_FILE.rsplit('/metadata/', 1)[0]
t = TTransport.TBufferedTransport(TSocket.TSocket("hive-metastore.prod.svc.cluster.local", 9083))
c = ThriftHiveMetastore.Client(TBinaryProtocol.TBinaryProtocol(t)); t.open()
if TABLE_NAME in c.get_all_tables("demo"): c.drop_table("demo", TABLE_NAME, deleteData=False)
ts = int(time.time())
c.create_table(Table(dbName="demo", tableName=TABLE_NAME, owner=getpass.getuser(),
    createTime=ts, lastAccessTime=ts, tableType="EXTERNAL_TABLE",
    sd=StorageDescriptor(cols=[], location=TABLE_LOCATION,
        inputFormat="org.apache.iceberg.mr.hive.HiveIcebergInputFormat",
        outputFormat="org.apache.iceberg.mr.hive.HiveIcebergOutputFormat",
        compressed=False,
        serdeInfo=SerDeInfo(serializationLib="org.apache.iceberg.mr.hive.HiveIcebergSerDe", parameters={}),
        parameters={}),
    parameters={"table_type":"ICEBERG","metadata_location":METADATA_FILE,"EXTERNAL":"TRUE"}))
print(f"✅ HMS: demo.{TABLE_NAME} registered")
t.close()
PYEOF

# ── Step 3: Query from Databricks ─────────────────────────────────────────────
python3 - "$DB_TOKEN" "$TABLE" <<'PYEOF'
import sys, json, time, urllib.request
DB_WS="https://dbc-11a1dbc5-061a.cloud.databricks.com"
DB_TOKEN, TABLE_NAME = sys.argv[1], sys.argv[2]
WAREHOUSE_ID="942026cf5e55f3c3"
HDRS={"Authorization":f"Bearer {DB_TOKEN}","Content-Type":"application/json"}

def run(sql):
    req=urllib.request.Request(f"{DB_WS}/api/2.0/sql/statements",
        data=json.dumps({"warehouse_id":WAREHOUSE_ID,"wait_timeout":"90s","statement":sql}).encode(),
        method="POST",headers=HDRS)
    with urllib.request.urlopen(req,timeout=120) as r: d=json.loads(r.read())
    for _ in range(30):
        if d.get("status",{}).get("state") in ("SUCCEEDED","FAILED","CANCELED"): break
        sid=d["statement_id"]; time.sleep(5)
        req2=urllib.request.Request(f"{DB_WS}/api/2.0/sql/statements/{sid}",headers=HDRS)
        with urllib.request.urlopen(req2,timeout=30) as r: d=json.loads(r.read())
    return d.get("result",{}).get("data_array",[])

rows = run(f"SELECT COUNT(*) FROM star_lakehouse.demo.{TABLE_NAME}")
total = int(rows[0][0]) if rows else -1
print(f"Databricks count: {total}")
print("✅ E2E PASS" if total == 500 else f"❌ E2E FAIL — expected 500 got {total}")
PYEOF
```

---

## 9. Troubleshooting

### Spark write fails: `ForbiddenException: not authorized for CREATE_TABLE`

`spark-iceberg-svc` lacks `star_lakehouse_admin` role. Fix:

```bash
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

The `stsUnavailable` flag was reset (e.g. by a Polaris pod restart). Re-apply:

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

### Databricks returns stale row count after write

You skipped the HMS registration step. Re-run Section 5 with your table name.

### Databricks SQL warehouse is STOPPED

```bash
curl -s -X POST \
  "$DB_WS/api/2.0/sql/warehouses/$WAREHOUSE_ID/start" \
  -H "Authorization: Bearer $DB_TOKEN"
# Wait ~30 seconds then retry the query
```

### `TTransportException` on HMS Thrift call

HMS pod restarted or not ready:
```bash
kubectl get pods -n prod -l app=hive-metastore
# Wait for 1/1 Running, then retry Section 5
```

### `ModuleNotFoundError: No module named 'bao_spark_init'`

The helper wasn't copied to the pod this session:

```bash
kubectl cp docker/spark-gluten-velox/scripts/bao_spark_init.py \
  prod/$SPARK_POD:/tmp/bao_spark_init.py -c spark-master
```

---

## 10. Key paths reference

| Resource | Path / URL |
|---|---|
| S3 data root | `s3://stardata-databricks/iceberg/warehouse/demo/` |
| Polaris catalog API | `http://192.168.1.50:30181/api/catalog/v1/star_lakehouse/` |
| Polaris management API | `http://192.168.1.50:30181/api/management/v1/` |
| Polaris in-cluster URI | `http://polaris-rest.prod.svc.cluster.local:8181/api/catalog` |
| HMS Thrift (in-cluster) | `hive-metastore.prod.svc.cluster.local:9083` |
| HMS Thrift (NodePort) | `192.168.1.50:30983` |
| Databricks workspace | `https://dbc-11a1dbc5-061a.cloud.databricks.com` |
| Databricks SQL editor | `https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor` |
| Databricks SQL warehouse | `942026cf5e55f3c3` (Serverless Starter) |
| Databricks FOREIGN catalog | `star_lakehouse` (connection: `hms_star_lakehouse`) |
| `bao_spark_init.py` (source) | `docker/spark-gluten-velox/scripts/bao_spark_init.py` |
| Script: data gen (customers) | `scripts/databricks-iceberg-polaris/generate_customers_iceberg.py` |
| Script: T-09/T-12 verify | `scripts/databricks-iceberg-polaris/t09_t12_verify.py` |
