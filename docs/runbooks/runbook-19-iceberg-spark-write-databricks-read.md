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
         ▼ Iceberg REST (Polaris)
  New snapshot written to S3
  s3://stardata-databricks/iceberg/warehouse/demo/customers/
         │
         ▼ Terminal — HMS re-register (updates metadata_location pointer)
  hive-metastore.prod.svc.cluster.local:9083
         │
         ▼ Databricks FOREIGN catalog reads updated pointer
  SELECT * FROM star_lakehouse.demo.customers   ← sees new data
```

> **Why the HMS step?** Databricks reads the `metadata_location` pointer stored in HMS. After every Spark write, that pointer must be updated to the new `.metadata.json` so Databricks sees the latest snapshot.

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
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.polaris",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.polaris.type",        "rest") \
    .config("spark.sql.catalog.polaris.uri",
            "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog") \
    .config("spark.sql.catalog.polaris.oauth2-server-uri",
            "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog/v1/oauth/tokens") \
    .config("spark.sql.defaultCatalog",              "polaris") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key",        S3_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key",        S3_SECRET) \
    .config("spark.hadoop.fs.s3a.endpoint",          S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.catalog.polaris.credential",  f"{POLARIS_ID}:{POLARIS_SECRET}") \
    .config("spark.sql.catalog.polaris.scope",       "PRINCIPAL_ROLE:ALL") \
    .config("spark.sql.catalog.polaris.warehouse",   "star_lakehouse") \
    .config("spark.sql.catalog.polaris.s3.access-key-id",     S3_KEY) \
    .config("spark.sql.catalog.polaris.s3.secret-access-key", S3_SECRET) \
    .config("spark.sql.catalog.polaris.s3.endpoint",          S3_ENDPOINT) \
    .config("spark.sql.catalog.polaris.s3.path-style-access", "true") \
    .config("spark.sql.catalog.polaris.client.region",        "us-east-2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark", spark.version, "connected —", DRIVER_IP)
```

### Cell 3 — Check current state

```python
spark.sql("SELECT COUNT(*) AS total FROM polaris.demo.customers").show()
spark.sql("""
    SELECT customer_id, full_name, email, customer_tier
    FROM polaris.demo.customers
    LIMIT 5
""").show(truncate=False)
```

---

## 4. Step 1 (continued) — Run your DML

### INSERT new rows

```python
spark.sql("""
INSERT INTO polaris.demo.customers
  (customer_id, full_name, email, phone_number, date_of_birth,
   national_id, street_address, city, country_code, ip_address,
   salary, customer_tier, is_active, created_at, updated_at)
VALUES
  (10001, 'John Miller',  'john.miller@example.com',  '+1-555-0101', DATE '1985-03-14',
   'ID-001-JM', '12 Maple St',      'Toronto', 'CA', '192.168.10.1',  95000.00, 'gold',     1, TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-01 00:00:00'),
  (10002, 'Sara Khan',    'sara.khan@example.com',    '+1-555-0102', DATE '1990-07-22',
   'ID-002-SK', '88 Oak Ave',       'London',  'GB', '10.0.0.12',     72000.00, 'silver',   1, TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-01 00:00:00'),
  (10003, 'Luca Rossi',   'luca.rossi@example.com',   '+39-06-5550', DATE '1978-11-05',
   'ID-003-LR', 'Via Roma 3',       'Rome',    'IT', '172.16.0.5',   110000.00, 'platinum', 1, TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-01 00:00:00'),
  (10004, 'Amira Nasser', 'amira.nasser@example.com', '+20-2-5550',  DATE '1995-01-30',
   'ID-004-AN', '45 Nile Corniche', 'Cairo',   'EG', '10.10.10.10',   48000.00, 'standard', 1, TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-01 00:00:00'),
  (10005, 'Wei Zhang',    'wei.zhang@example.com',    '+86-10-5550', DATE '1988-09-17',
   'ID-005-WZ', '8 Jingshan Rd',    'Beijing', 'CN', '192.168.1.100',130000.00, 'platinum', 1, TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-01 00:00:00')
""")

# Confirm
spark.sql("SELECT COUNT(*) AS total FROM polaris.demo.customers").show()
# Expected: 10005
```

### UPDATE existing rows

```python
# Promote a customer tier
spark.sql("""
    UPDATE polaris.demo.customers
    SET customer_tier = 'platinum'
    WHERE customer_id = 10002
""")

# Deactivate customers in a city
spark.sql("""
    UPDATE polaris.demo.customers
    SET is_active = 0
    WHERE city = 'Cairo'
""")

spark.sql("SELECT customer_id, full_name, customer_tier, is_active FROM polaris.demo.customers WHERE customer_id IN (10002, 10004)").show()
```

### DELETE rows

```python
# Delete a specific customer
spark.sql("DELETE FROM polaris.demo.customers WHERE customer_id = 10004")

# Delete by condition
spark.sql("DELETE FROM polaris.demo.customers WHERE is_active = 0")

spark.sql("SELECT COUNT(*) AS total FROM polaris.demo.customers").show()
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
    Prefix='iceberg/warehouse/demo/customers/metadata/')
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
from hive_metastore.ttypes import Table, StorageDescriptor, SerDeInfo

TABLE_NAME     = sys.argv[1]
METADATA_FILE  = sys.argv[2]
TABLE_LOCATION = METADATA_FILE.rsplit('/metadata/', 1)[0]

t = TTransport.TBufferedTransport(
    TSocket.TSocket("hive-metastore.prod.svc.cluster.local", 9083))
c = ThriftHiveMetastore.Client(TBinaryProtocol.TBinaryProtocol(t))
t.open()

if TABLE_NAME in c.get_all_tables("demo"):
    c.drop_table("demo", TABLE_NAME, deleteData=False)

ts = int(time.time())
c.create_table(Table(
    dbName="demo", tableName=TABLE_NAME, owner=getpass.getuser(),
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
print(f"✅ HMS updated: demo.{TABLE_NAME} → {METADATA_FILE}")
t.close()
PYEOF
```

✅ **Pass:** `HMS updated: demo.customers → s3://...metadata.json`

---

## 6. Step 3 — Verify in Databricks SQL

Open **`https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor`**, select **Serverless Starter Warehouse**, and run:

```sql
-- Row count — should reflect your inserts/deletes
SELECT COUNT(*) AS total_rows
FROM star_lakehouse.demo.customers;

-- Check specific rows you inserted
SELECT customer_id, full_name, email, customer_tier
FROM star_lakehouse.demo.customers
WHERE customer_id >= 10001
ORDER BY customer_id;

-- Check an update took effect
SELECT customer_id, full_name, customer_tier
FROM star_lakehouse.demo.customers
WHERE customer_id = 10002;

-- Tier distribution after changes
SELECT customer_tier, COUNT(*) AS cnt
FROM star_lakehouse.demo.customers
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

### `AnalysisException: REQUIRES_SINGLE_PART_NAMESPACE` on `polaris.demo.*`

**Symptom:** `spark_catalog requires a single-part namespace, but got polaris.demo`.

**Cause:** `.getOrCreate()` returned a stale `SparkSession` from a previous run. None of the Cell 2 `.config(...)` calls took effect, so Spark has no `polaris` catalog registered and mis-routes the query through `spark_catalog` (the built-in Hive catalog), which rejects a two-part namespace.

**Fix — pick one:**
- **Recommended:** Cell 2 now contains a session stop guard at the top. Re-run Cell 2 — the guard stops the stale session and `.getOrCreate()` builds a fresh one with all catalog configs applied.
- **Quick fix:** Restart the kernel (Kernel → Restart Kernel), then re-run Cell 1 and Cell 2 from scratch.

### `CatalogNotFoundException: spark.sql.catalog.polaris is not defined`

**Symptom:** `Catalog 'polaris' plugin class not found: spark.sql.catalog.polaris is not defined`.

**Cause:** The `jupyter-spark` pod is running an **old image** built before `SPARK_CONF_DIR` was set in the Dockerfile. Without that env var, PySpark never loads `/usr/local/spark/conf/spark-defaults.conf`, so the `polaris` catalog class registration (`org.apache.iceberg.spark.SparkCatalog`) is never seen by the JVM. Cell 2's `.config()` calls alone cannot register a catalog class — that must come from `spark-defaults.conf`.

**Fix — rebuild the image and respawn the pod:**

```bash
# 1. Rebuild and push the jupyter-spark image
cd /home/star_master/k8s-platform
docker build -t 192.168.1.50:30500/jupyter-spark:latest docker/jupyter-spark/
docker push 192.168.1.50:30500/jupyter-spark:latest

# 2. Delete the singleuser pod — JupyterHub respawns it with the new image
kubectl delete pod jupyter-admin -n prod
# Log back in at http://192.168.1.50:30888

# 3. Verify SPARK_CONF_DIR is set in the new pod
kubectl exec -n prod jupyter-admin -- printenv SPARK_CONF_DIR
# Expected: /usr/local/spark/conf

# 4. Verify spark-defaults.conf is loaded (polaris catalog must appear)
kubectl exec -n prod jupyter-admin -- cat /usr/local/spark/conf/spark-defaults.conf | grep polaris
# Expected: spark.sql.catalog.polaris   org.apache.iceberg.spark.SparkCatalog
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

## 8. Key paths reference

| Resource | Path / URL |
|---|---|
| JupyterHub | `http://192.168.1.50:30888` |
| Spark master (in-cluster) | `spark://spark-master-internal.prod.svc.cluster.local:17077` |
| Polaris in-cluster URI | `http://polaris-rest.prod.svc.cluster.local:8181/api/catalog` |
| Iceberg catalog name (in notebook) | `polaris` |
| Iceberg table (in notebook) | `polaris.demo.customers` |
| S3 Iceberg warehouse | `s3://stardata-databricks/iceberg/warehouse/demo/` |
| HMS Thrift (in-cluster) | `hive-metastore.prod.svc.cluster.local:9083` |
| HMS Thrift (NodePort) | `192.168.1.50:30983` |
| Databricks SQL editor | `https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor` |
| Databricks SQL warehouse | `942026cf5e55f3c3` (Serverless Starter) |
| Databricks FOREIGN catalog | `star_lakehouse` (connection: `hms_star_lakehouse`) |
| OpenBao Polaris creds | `secret/platform/polaris` → `spark_svc_id`, `spark_svc_secret` |
| OpenBao S3 creds | `secret/platform/s3` → `access_key`, `secret_key`, `endpoint` |
| OpenBao Databricks PAT | `secret/databricks/pat` → `token`, `workspace` |
