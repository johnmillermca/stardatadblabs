# Runbook 19 — INSERT / UPDATE / DELETE in Spark Iceberg → Verify in Databricks

| Field | Value |
|---|---|
| **Runbook ID** | RB-19 |
| **Service** | k8s-platform / data-lakehouse |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-08-28 |
| **Related** | RB-18 (infrastructure setup), RB-15 (Snowflake→Iceberg), RB-01 (OpenBao) |

---

## 1. Purpose

**Day-to-day guide** for running SQL DML (`INSERT`, `UPDATE`, `DELETE`) against the Iceberg table in Spark and immediately seeing those changes in Databricks SQL.

Three steps every time:
1. **JupyterHub** — open a notebook, connect to Spark, run your SQL
2. **Terminal** — re-register the table in HMS after the write
3. **Databricks SQL editor** — verify the new data is visible

> **Pre-condition:** [RB-18](runbook-18-databricks-iceberg-polaris.md) infrastructure must be live — Polaris, S3, HMS, and the Databricks `star_lakehouse` FOREIGN catalog must all exist.

---

## 2. How it works

```
JupyterHub notebook (http://192.168.1.50:30888)
  PySpark → INSERT / UPDATE / DELETE
         │
         ▼ Iceberg REST (Polaris — catalog alias: databricks, warehouse: star_lakehouse)
  New snapshot written to S3
  s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customers/
         │
         ▼ Terminal — HMS re-register (updates metadata_location pointer)
  hive-metastore.prod.svc.cluster.local:9083
         │
         ▼ Databricks FOREIGN catalog reads updated pointer
  SELECT * FROM star_lakehouse.lakehouse_db.customers   ← sees new data
```

> **Why the HMS step?** Databricks reads the `metadata_location` pointer stored in HMS. After every Spark write, that pointer must be updated to the new `.metadata.json` so Databricks sees the latest snapshot.

> **Catalog naming:** In the notebook the catalog is named `databricks` — this is a Spark catalog alias that points at the same Polaris REST endpoint with warehouse `star_lakehouse`. The namespace inside it is `lakehouse_db`. Full table reference from the notebook: `databricks.lakehouse_db.customers`.

---

## 3. Step 1 — Connect to Spark from JupyterHub

Open **`http://192.168.1.50:30888`**, log in (`admin` / see OpenBao), create a new notebook, and run the following cells in order.

### Cell 1 — Fetch credentials from OpenBao

First get the OpenBao root token from your terminal:

```bash
kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d && echo
```

Paste the token into the notebook cell below:

```python
import urllib.request, json

OPENBAO_ADDR  = "http://openbao.prod.svc.cluster.local:8200"
OPENBAO_TOKEN = "s.xxxxxxxxxxxxxxxxxxxxxxxx"   # ← paste token here

def bao(path, field):
    req = urllib.request.Request(
        f"{OPENBAO_ADDR}/v1/{path}",
        headers={"X-Vault-Token": OPENBAO_TOKEN}
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())["data"]["data"][field]

print("✅ OpenBao helper ready")
```

### Cell 2 — Build the Spark session

> The catalog class registration, Iceberg extensions, S3A wiring, and executor classpath are all set here explicitly so this cell works on any pod — whether or not `SPARK_CONF_DIR` is loading `spark-defaults.conf` from the image.
>
> **Always run this cell after Cell 1, even on a fresh kernel.** The session stop guard at the top ensures any stale session is cleared first. Without it, `.getOrCreate()` silently returns the old session with none of these configs applied.
>
> **Gluten/Velox must be activated from the driver session.** The Gluten JAR is baked into every worker pod (`spark-gluten-velox:3.5.1`) but it is only initialised when the driver advertises `spark.plugins=org.apache.gluten.GlutenPlugin`. Without the four Gluten lines below, Spark silently falls back to the vanilla JVM engine and you get zero Velox acceleration despite the JAR being present.

```python
from pyspark.sql import SparkSession
import os

# Stop any stale session so .getOrCreate() always creates a fresh one.
# Safe to call on a clean kernel (no-op).
_s = SparkSession.getActiveSession()
if _s is not None:
    _s.stop()

POLARIS_ID     = bao("secret/data/platform/polaris", "spark_svc_id")
POLARIS_SECRET = bao("secret/data/platform/polaris", "spark_svc_secret")
S3_KEY         = bao("secret/data/platform/s3",      "access_key")
S3_SECRET      = bao("secret/data/platform/s3",      "secret_key")
S3_ENDPOINT    = bao("secret/data/platform/s3",      "endpoint")

# SPARK_LOCAL_IP is injected by the JupyterHub pod spec (Downward API) so Spark
# advertises the pod IP to executors — they can then connect back directly.
DRIVER_IP = os.environ["SPARK_LOCAL_IP"]

spark = SparkSession.builder \
    .master("spark://spark-master-internal.prod.svc.cluster.local:17077") \
    .appName("jupyter-iceberg-dml") \
    .config("spark.driver.host",        DRIVER_IP) \
    .config("spark.driver.bindAddress", DRIVER_IP) \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory",   "2g") \
    .config("spark.pyspark.python",        "python3.11") \
    .config("spark.pyspark.driver.python", "python3.11") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.databricks",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.databricks.type",        "rest") \
    .config("spark.sql.catalog.databricks.uri",
            "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog") \
    .config("spark.sql.catalog.databricks.oauth2-server-uri",
            "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog/v1/oauth/tokens") \
    .config("spark.sql.defaultCatalog",                 "databricks") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key",        S3_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key",        S3_SECRET) \
    .config("spark.hadoop.fs.s3a.endpoint",          S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.catalog.databricks.credential",    f"{POLARIS_ID}:{POLARIS_SECRET}") \
    .config("spark.sql.catalog.databricks.scope",         "PRINCIPAL_ROLE:ALL") \
    .config("spark.sql.catalog.databricks.warehouse",     "star_lakehouse") \
    .config("spark.sql.catalog.databricks.rest.auth.type","oauth2") \
    .config("spark.sql.catalog.databricks.s3.access-key-id",     S3_KEY) \
    .config("spark.sql.catalog.databricks.s3.secret-access-key", S3_SECRET) \
    .config("spark.sql.catalog.databricks.s3.endpoint",          S3_ENDPOINT) \
    .config("spark.sql.catalog.databricks.s3.path-style-access", "true") \
    .config("spark.sql.catalog.databricks.client.region",        "us-east-2") \
    .config("spark.plugins",                             "org.apache.gluten.GlutenPlugin") \
    .config("spark.gluten.sql.columnar.backend.lib",     "velox") \
    .config("spark.memory.offHeap.enabled",              "true") \
    .config("spark.memory.offHeap.size",                 "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark", spark.version, "connected —", DRIVER_IP)
```

### Cell 3 — Ensure namespace and table exist, then check current state

> Run this once per session. Both `CREATE NAMESPACE` and `CREATE TABLE` are no-ops if they already exist.
> The `customers` table must be created before the first SELECT. `IcebergTableBuilder.create_table()` is
> idempotent (`IF NOT EXISTS` by default) — safe to re-run every session.

```python
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DateType, TimestampType, DoubleType
)
from spark_iceberg_utils import IcebergTableBuilder

builder = IcebergTableBuilder(spark, running_user="dave")

# Ensure namespace exists
builder.ensure_namespace("databricks", "lakehouse_db")

# Create customers table if it doesn't exist yet.
# snap_id and snap_timestamp are appended automatically by create_table().
customer_schema = StructType([
    StructField("customer_id",     IntegerType(),   nullable=False),
    StructField("full_name",       StringType(),    nullable=True),
    StructField("email",           StringType(),    nullable=True),
    StructField("phone_number",    StringType(),    nullable=True),
    StructField("date_of_birth",   DateType(),      nullable=True),
    StructField("national_id",     StringType(),    nullable=True),
    StructField("street_address",  StringType(),    nullable=True),
    StructField("city",            StringType(),    nullable=True),
    StructField("country_code",    StringType(),    nullable=True),
    StructField("ip_address",      StringType(),    nullable=True),
    StructField("salary",          DoubleType(),    nullable=True),
    StructField("customer_tier",   StringType(),    nullable=True),
    StructField("is_active",       IntegerType(),   nullable=True),
    StructField("created_at",      TimestampType(), nullable=True),
    StructField("updated_at",      TimestampType(), nullable=True),
])

builder.create_table(
    catalog="databricks",
    namespace="lakehouse_db",
    table="customers",
    schema=customer_schema,
    partition_spec=[
        IcebergTableBuilder.hours("snap_timestamp"),
        IcebergTableBuilder.bucket("snap_id", 4),
    ],
)

# Check current state — returns 0 rows on a brand-new table, which is expected.
spark.sql("SELECT COUNT(*) AS total FROM databricks.lakehouse_db.customers").show()
spark.sql("""
    SELECT customer_id, full_name, email, customer_tier
    FROM databricks.lakehouse_db.customers
    LIMIT 5
""").show(truncate=False)
```

---

## 4. Step 1 (continued) — Run your DML

### INSERT new rows

> `snap_id` and `snap_timestamp` are injected automatically by `write_append()` —
> you only provide the business columns. Do **not** use raw `spark.sql("INSERT INTO … VALUES …")`
> or `df.writeTo().append()` directly — neither has snap column injection.
>
> **Why small batches are still somewhat slow:** The table is partitioned by `hours(snap_timestamp) + bucket(4, snap_id)`. Even 5 rows are distributed across 4 hash buckets, producing 4 separate Parquet files and 4 individual S3 `PutObject` calls. This is normal Iceberg behaviour — the partition spec is optimised for large table scans, not for individual small writes. Expect ~15–30 s cold-executor overhead per batch regardless of row count; this is inherent to Spark's executor allocation cycle, not a bug.

```python
from pyspark.sql import Row
from spark_iceberg_utils import IcebergTableBuilder
import datetime

builder = IcebergTableBuilder(spark, running_user="dave")

rows = [
    Row(customer_id=10001, full_name='John Miller',  email='john.miller@example.com',  phone_number='+1-555-0101', date_of_birth=datetime.date(1985,3,14),  national_id='ID-001-JM', street_address='12 Maple St',      city='Toronto', country_code='CA', ip_address='192.168.10.1',  salary=95000.00,  customer_tier='gold',     is_active=1, created_at=datetime.datetime(2026,1,1), updated_at=datetime.datetime(2026,1,1)),
    Row(customer_id=10002, full_name='Sara Khan',    email='sara.khan@example.com',    phone_number='+1-555-0102', date_of_birth=datetime.date(1990,7,22),  national_id='ID-002-SK', street_address='88 Oak Ave',       city='London',  country_code='GB', ip_address='10.0.0.12',     salary=72000.00,  customer_tier='silver',   is_active=1, created_at=datetime.datetime(2026,1,1), updated_at=datetime.datetime(2026,1,1)),
    Row(customer_id=10003, full_name='Luca Rossi',   email='luca.rossi@example.com',   phone_number='+39-06-5550', date_of_birth=datetime.date(1978,11,5),  national_id='ID-003-LR', street_address='Via Roma 3',       city='Rome',    country_code='IT', ip_address='172.16.0.5',    salary=110000.00, customer_tier='platinum', is_active=1, created_at=datetime.datetime(2026,1,1), updated_at=datetime.datetime(2026,1,1)),
    Row(customer_id=10004, full_name='Amira Nasser', email='amira.nasser@example.com', phone_number='+20-2-5550',  date_of_birth=datetime.date(1995,1,30),  national_id='ID-004-AN', street_address='45 Nile Corniche', city='Cairo',   country_code='EG', ip_address='10.10.10.10',   salary=48000.00,  customer_tier='standard', is_active=1, created_at=datetime.datetime(2026,1,1), updated_at=datetime.datetime(2026,1,1)),
    Row(customer_id=10005, full_name='Wei Zhang',    email='wei.zhang@example.com',    phone_number='+86-10-5550', date_of_birth=datetime.date(1988,9,17),  national_id='ID-005-WZ', street_address='8 Jingshan Rd',    city='Beijing', country_code='CN', ip_address='192.168.1.100', salary=130000.00, customer_tier='platinum', is_active=1, created_at=datetime.datetime(2026,1,1), updated_at=datetime.datetime(2026,1,1)),
]

df = spark.createDataFrame(rows)

# write_append injects snap_id and snap_timestamp automatically
builder.write_append(df, catalog="databricks", namespace="lakehouse_db", table="customers")

# Confirm
spark.sql("SELECT COUNT(*) AS total FROM databricks.lakehouse_db.customers").show()
# Expected: 10005
```

### UPDATE existing rows

```python
# Promote a customer tier
spark.sql("""
    UPDATE databricks.lakehouse_db.customers
    SET customer_tier = 'platinum'
    WHERE customer_id = 10002
""")

# Deactivate customers in a city
spark.sql("""
    UPDATE databricks.lakehouse_db.customers
    SET is_active = 0
    WHERE city = 'Cairo'
""")

spark.sql("SELECT customer_id, full_name, customer_tier, is_active FROM databricks.lakehouse_db.customers WHERE customer_id IN (10002, 10004)").show()
```

### DELETE rows

```python
# Delete a specific customer
spark.sql("DELETE FROM databricks.lakehouse_db.customers WHERE customer_id = 10004")

# Delete by condition
spark.sql("DELETE FROM databricks.lakehouse_db.customers WHERE is_active = 0")

spark.sql("SELECT COUNT(*) AS total FROM databricks.lakehouse_db.customers").show()
```

---

## 5. Step 2 — Re-register the table in HMS (Terminal)

**Run this on your master terminal after every DML operation.** This updates the `metadata_location` pointer in HMS so Databricks reads the new snapshot.

```bash
VAULT_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

SPARK_POD=$(kubectl get pods -n prod -l app=spark,component=master \
  --no-headers -o custom-columns=NAME:.metadata.name | head -1)

AWS_KEY=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=access_key secret/platform/s3")
AWS_SECRET=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=secret_key secret/platform/s3")

# Find the latest metadata.json on S3
METADATA_FILE=$(python3 - "$AWS_KEY" "$AWS_SECRET" <<'PYEOF'
import sys, boto3
s3 = boto3.client('s3', region_name='us-east-2',
    aws_access_key_id=sys.argv[1], aws_secret_access_key=sys.argv[2])
r = s3.list_objects_v2(Bucket='stardata-databricks',
    Prefix='iceberg/warehouse/lakehouse_db/customers/metadata/')
objects = [o for o in r.get('Contents', []) if o['Key'].endswith('.metadata.json')]
objects.sort(key=lambda x: x['LastModified'], reverse=True)
print(f"s3://stardata-databricks/{objects[0]['Key']}")
PYEOF
)
echo "Latest metadata: $METADATA_FILE"

# Update HMS entry via Thrift (thrift is baked into the Spark image)
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  python3 - "customers" "$METADATA_FILE" <<'PYEOF'
import sys, time, getpass
from thrift.transport import TSocket, TTransport
from thrift.protocol  import TBinaryProtocol
from hive_metastore   import ThriftHiveMetastore
from hive_metastore.ttypes import Table, StorageDescriptor, SerDeInfo, Database

TABLE_NAME     = sys.argv[1]
METADATA_FILE  = sys.argv[2]
TABLE_LOCATION = METADATA_FILE.rsplit('/metadata/', 1)[0]
DB_NAME        = "lakehouse_db"
DB_LOCATION    = "s3://stardata-databricks/iceberg/warehouse/lakehouse_db/"

t = TTransport.TBufferedTransport(
    TSocket.TSocket("hive-metastore.prod.svc.cluster.local", 9083))
c = ThriftHiveMetastore.Client(TBinaryProtocol.TBinaryProtocol(t))
t.open()

# Create the HMS database if it doesn't exist.
# Databricks cannot list tables without this database entry in HMS.
if DB_NAME not in c.get_all_databases():
    c.create_database(Database(
        name=DB_NAME,
        description="Iceberg lakehouse — Polaris star_lakehouse",
        locationUri=DB_LOCATION,
        parameters={}))
    print(f"✅ HMS database created: {DB_NAME}")

if TABLE_NAME in c.get_all_tables(DB_NAME):
    c.drop_table(DB_NAME, TABLE_NAME, deleteData=False)

ts = int(time.time())
c.create_table(Table(
    dbName=DB_NAME, tableName=TABLE_NAME, owner=getpass.getuser(),
    createTime=ts, lastAccessTime=ts, tableType="EXTERNAL_TABLE",
    sd=StorageDescriptor(
        cols=[], location=TABLE_LOCATION,
        inputFormat="org.apache.iceberg.mr.hive.HiveIcebergInputFormat",
        outputFormat="org.apache.iceberg.mr.hive.HiveIcebergOutputFormat",
        compressed=False,
        serdeInfo=SerDeInfo(
            serializationLib="org.apache.iceberg.mr.hive.HiveIcebergSerDe",
            parameters={}),
        parameters={}),
    parameters={"table_type":"ICEBERG","metadata_location":METADATA_FILE,"EXTERNAL":"TRUE"}))
print(f"✅ HMS updated: {DB_NAME}.{TABLE_NAME} → {METADATA_FILE}")
t.close()
PYEOF
```

✅ **Pass:** `HMS updated: lakehouse_db.customers → s3://...metadata.json`

---

## 6. Step 3 — Verify in Databricks SQL

Open **`https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor`**, select **Serverless Starter Warehouse**, and run:

```sql
-- Row count — should reflect your inserts/deletes
SELECT COUNT(*) AS total_rows
FROM star_lakehouse.lakehouse_db.customers;

-- Check specific rows you inserted
SELECT customer_id, full_name, email, customer_tier
FROM star_lakehouse.lakehouse_db.customers
WHERE customer_id >= 10001
ORDER BY customer_id;

-- Check an update took effect
SELECT customer_id, full_name, customer_tier
FROM star_lakehouse.lakehouse_db.customers
WHERE customer_id = 10002;

-- Tier distribution after changes
SELECT customer_tier, COUNT(*) AS cnt
FROM star_lakehouse.lakehouse_db.customers
GROUP BY customer_tier
ORDER BY customer_tier;
```

> **First query after warehouse restart takes 30–90 seconds** — the Serverless Starter Warehouse cold-starts automatically.

> **Count looks wrong?** You skipped Section 5. Re-run the HMS registration and query again.

---

## 7. Troubleshooting

### OpenBao `URLError` or token error in notebook
The token in Cell 1 has expired or is wrong. Get a fresh one from the terminal:
```bash
kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d && echo
```
Paste it into `OPENBAO_TOKEN` in Cell 1 and re-run.

### `ForbiddenException: not authorized for INSERT`
The `spark-iceberg-svc` principal lacks the `star_lakehouse_admin` role:
```bash
VAULT_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod -o jsonpath='{.data.root-token}' | base64 -d)
POLARIS_ID=$(kubectl exec -n prod openbao-0 -- sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=spark_svc_id secret/platform/polaris")
POLARIS_SECRET=$(kubectl exec -n prod openbao-0 -- sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=spark_svc_secret secret/platform/polaris")
POLARIS_TOKEN=$(curl -s -X POST http://192.168.1.50:30181/api/catalog/v1/oauth/tokens \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=${POLARIS_ID}&client_secret=${POLARIS_SECRET}&scope=PRINCIPAL_ROLE:ALL" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
curl -s -X PUT \
  "http://192.168.1.50:30181/api/management/v1/principals/spark-iceberg-svc/principal-roles" \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"principalRole": {"name": "star_lakehouse_admin"}}'
```

### `StsException: not authorized to perform sts:AssumeRole`
Polaris `stsUnavailable` flag was reset after a pod restart. Re-apply:
```bash
PG_POD=$(kubectl get pods -n prod -l app=postgresql --no-headers -o custom-columns=NAME:.metadata.name | head -1)
kubectl exec -n prod $PG_POD -- env PGPASSWORD="postgres" psql -U postgres -d polaris -c "
UPDATE polaris_schema.entities
SET internal_properties = jsonb_set(internal_properties,'{storage_configuration_info}',
  to_jsonb('{\"@type\":\"AwsStorageConfigurationInfo\",\"allowedLocations\":[\"s3://stardata-databricks/iceberg/warehouse\"],\"roleARN\":\"arn:aws:iam::586643076710:user/watsonx-s3-connector\",\"allowedKmsKeys\":[],\"externalId\":\"polaris-iceberg\",\"region\":\"us-east-2\",\"stsUnavailable\":true,\"storageType\":\"S3\"}'::text))
WHERE name = 'star_lakehouse';"
kubectl rollout restart deployment/polaris -n prod && kubectl rollout status deployment/polaris -n prod --timeout=90s
```

### S3 `No FileSystem for scheme "s3"` or `"s3a"` in notebook

Since `jupyter-spark:latest` 2026-08-28, `hadoop-aws` and `aws-java-sdk-bundle` are baked into the image and wired via `spark-defaults.conf`. This error should not occur on pods running the current image.

If you see it on an **old pod** (image pulled before that date), delete the singleuser pod so JupyterHub respawns it with the current image:

```bash
kubectl delete pod jupyter-admin -n prod
# JupyterHub recreates it automatically; log back in at http://192.168.1.50:30888
```

Verify the JARs are present after respawn:

```bash
kubectl exec -n prod jupyter-admin -- \
  ls /opt/conda/lib/python3.11/site-packages/pyspark/jars/ | grep -E "hadoop-aws|aws-java"
# Expected: hadoop-aws-3.3.4.jar  aws-java-sdk-bundle-1.12.262.jar
```

### `AnalysisException: REQUIRES_SINGLE_PART_NAMESPACE` on `databricks.lakehouse_db.*`

**Symptom:** `spark_catalog requires a single-part namespace, but got databricks.lakehouse_db`.

**Cause:** `.getOrCreate()` returned a stale `SparkSession` from a previous run. None of the Cell 2 `.config(...)` calls took effect, so Spark has no `databricks` catalog registered and mis-routes the query through `spark_catalog` (the built-in Hive catalog), which rejects a two-part namespace.

**Fix — pick one:**
- **Recommended:** Cell 2 now contains a session stop guard at the top. Re-run Cell 2 — the guard stops the stale session and `.getOrCreate()` builds a fresh one with all catalog configs applied.
- **Quick fix:** Restart the kernel (Kernel → Restart Kernel), then re-run Cell 1 and Cell 2 from scratch.

### `CatalogNotFoundException: spark.sql.catalog.databricks is not defined`

**Symptom:** `Catalog 'databricks' plugin class not found: spark.sql.catalog.databricks is not defined`.

**Cause:** The `jupyter-spark` pod is running an **old image** built before `SPARK_CONF_DIR` was set in the Dockerfile. Without that env var, PySpark never loads `/usr/local/spark/conf/spark-defaults.conf`, so the `databricks` catalog class registration (`org.apache.iceberg.spark.SparkCatalog`) is never seen by the JVM. Cell 2's `.config()` calls alone cannot register a catalog class — that must come from `spark-defaults.conf`.

**Fix — rebuild the image and respawn the pod:**

```bash
# 1. Rebuild and push the jupyter-spark image
cd /home/star_master/k8s-platform
podman build --format docker -t 192.168.1.50:30500/jupyter-spark:latest docker/jupyter-spark/
podman push --tls-verify=false 192.168.1.50:30500/jupyter-spark:latest

# 2. Delete the singleuser pod — JupyterHub respawns it with the new image
kubectl delete pod jupyter-admin -n prod
# Log back in at http://192.168.1.50:30888

# 3. Verify SPARK_CONF_DIR is set in the new pod
kubectl exec -n prod jupyter-admin -- printenv SPARK_CONF_DIR
# Expected: /usr/local/spark/conf

# 4. Verify spark-defaults.conf is loaded (databricks catalog must appear)
kubectl exec -n prod jupyter-admin -- cat /usr/local/spark/conf/spark-defaults.conf | grep databricks
# Expected: spark.sql.catalog.databricks   org.apache.iceberg.spark.SparkCatalog
```

After the pod is up, re-run Cell 1 and Cell 2 from scratch.

### Databricks count unchanged after write
You skipped Section 5 (HMS re-register). Run it and query Databricks again.

### HMS Thrift `TTransportException`
HMS pod is down:
```bash
kubectl get pods -n prod -l app=hive-metastore
# Wait for 1/1 Running then retry Section 5
```

### Databricks PAT expired
```bash
kubectl exec -n prod openbao-0 -- sh -c \
  "VAULT_TOKEN=$VAULT_TOKEN vault kv patch secret/databricks/pat token='dapi...' expires='YYYY-MM-DD'"
```
Generate new token at: `https://dbc-11a1dbc5-061a.cloud.databricks.com/settings/user/developer/access-tokens`

---

## 8. Expanding Spark workers to worker5 (one-time setup)

> **Why worker5 and not worker4?** `worker4.local` has ~8 GB of memory already requested by Kafka and the JupyterHub Hub pod — less than 1.5 GB of headroom remains, which is not enough for a safe Spark executor. `worker5.local` has 25 GB RAM and is currently only 4% utilised, making it the right candidate.

### Step 1 — Pre-pull the Gluten/Velox image on worker5

`spark-gluten-velox:3.5.1` is cached on worker1/2/3 but not on worker5. Apply the pre-pull Jobs to cache it:

```bash
kubectl apply -f manifests/spark/spark-image-pull.yaml
```

Wait for both Jobs to complete (pulls ~600 MB from local registry; typically 1–2 min):

```bash
kubectl get jobs -n prod -l app=spark-image-pull -w
# Wait until both jobs show COMPLETIONS=1/1
```

Confirm the image is now cached on worker5:

```bash
kubectl get node worker5.local \
  -o jsonpath='{range .status.images[*]}{.names[-1]}{"\n"}{end}' \
  | grep spark-gluten
# Expected: 192.168.1.50:30500/spark-gluten-velox:3.5.1
```

Clean up the Jobs — they are no longer needed after the pull:

```bash
kubectl delete -f manifests/spark/spark-image-pull.yaml
```

### Step 2 — Deploy the large worker on worker5

```bash
kubectl apply -f manifests/spark/spark.yaml
kubectl rollout status deployment/spark-worker-large -n prod --timeout=120s
```

Verify the new worker registered with the Spark master (open the UI or run):

```bash
kubectl get pods -n prod -l component=worker-large -o wide
# Expected: 1 pod Running on worker5.local
```

The Spark master UI at `http://192.168.1.50:30707` will now show **4 workers** total:
- 3 × standard (worker1/2/3) — 4 GB / 4 cores each → **12 GB / 12 cores** combined
- 1 × large (worker5) — **12 GB / 8 cores**
- **Total cluster capacity: 24 GB RAM / 20 cores**

### Memory accounting for Gluten off-heap

Gluten/Velox allocates its native memory **off-heap** via `spark.memory.offHeap.size`. This is separate from `SPARK_WORKER_MEMORY` and is bounded only by the container `limits.memory`. The limits are set accordingly:

| Worker | `SPARK_WORKER_MEMORY` | Off-heap budget | Container limit |
|---|---|---|---|
| worker1/2/3 | 4 g | 2 g × executors | 6 Gi |
| worker5 | 12 g | 2 g × executors | 14 Gi |

---

## 9. Key paths reference

| Resource | Path / URL |
|---|---|
| JupyterHub | `http://192.168.1.50:30888` |
| Spark master (in-cluster) | `spark://spark-master-internal.prod.svc.cluster.local:17077` |
| Polaris in-cluster URI | `http://polaris-rest.prod.svc.cluster.local:8181/api/catalog` |
| Iceberg catalog name (in notebook) | `databricks` |
| Iceberg namespace (in notebook) | `lakehouse_db` |
| Iceberg table (in notebook) | `databricks.lakehouse_db.customers` |
| S3 Iceberg warehouse | `s3://stardata-databricks/iceberg/warehouse/lakehouse_db/` |
| HMS Thrift (in-cluster) | `hive-metastore.prod.svc.cluster.local:9083` |
| HMS Thrift (NodePort) | `192.168.1.50:30983` |
| Databricks SQL editor | `https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor` |
| Databricks SQL warehouse | `942026cf5e55f3c3` (Serverless Starter) |
| Databricks FOREIGN catalog | `star_lakehouse` (connection: `hms_star_lakehouse`) |
| Databricks table (SQL editor) | `star_lakehouse.lakehouse_db.customers` |
| OpenBao Polaris creds | `secret/platform/polaris` → `spark_svc_id`, `spark_svc_secret` |
| OpenBao S3 creds | `secret/platform/s3` → `access_key`, `secret_key`, `endpoint` |
| OpenBao Databricks PAT | `secret/databricks/pat` → `token`, `workspace` |
