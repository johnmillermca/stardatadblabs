# Runbook 19 — INSERT / UPDATE / DELETE in Spark Iceberg → Verify in Databricks

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

**Day-to-day guide** for running SQL DML (`INSERT`, `UPDATE`, `DELETE`) against the Iceberg table in Spark and immediately seeing those changes in Databricks SQL.

Three steps every time:
1. Open a `spark-sql` shell on the Spark master pod (catalog pre-configured)
2. Run your SQL — `INSERT`, `UPDATE`, or `DELETE`
3. Re-register the table in HMS, then query in Databricks

> **Pre-condition:** [RB-18](runbook-18-databricks-iceberg-polaris.md) infrastructure must be live — Polaris, S3, HMS, and the Databricks `star_lakehouse` FOREIGN catalog must all exist.

---

## 2. How it works

```
spark-sql (on Spark pod)
  INSERT / UPDATE / DELETE
         │
         ▼ Iceberg REST (Polaris)
  New snapshot written to S3
  s3://stardata-databricks/iceberg/warehouse/demo/customers/
         │
         ▼ HMS re-register (updates metadata_location pointer)
  hive-metastore.prod.svc.cluster.local:9083
         │
         ▼ Databricks FOREIGN catalog reads updated pointer
  SELECT * FROM star_lakehouse.demo.customers   ← sees new data
```

> **Why the HMS step?** Databricks doesn't read Polaris directly. It reads the `metadata_location` pointer stored in HMS. After every write, that pointer must be updated to the new `.metadata.json` file so Databricks sees the latest snapshot.

---

## 3. Open a spark-sql shell

```bash
# ── Terminal setup (once per session) ────────────────────────────────────────
VAULT_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

SPARK_POD=$(kubectl get pods -n prod -l app=spark,component=master \
  --no-headers -o custom-columns=NAME:.metadata.name | head -1)

# ── Fetch catalog credentials from OpenBao ────────────────────────────────────
POLARIS_SVC_ID=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=spark_svc_id secret/platform/polaris")
POLARIS_SVC_SECRET=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=spark_svc_secret secret/platform/polaris")
S3_KEY=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=access_key secret/platform/s3")
S3_SECRET=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=secret_key secret/platform/s3")
S3_ENDPOINT=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=endpoint secret/platform/s3")

# ── Open the spark-sql shell ──────────────────────────────────────────────────
kubectl exec -it -n prod $SPARK_POD -c spark-master -- \
  spark-sql \
    --master spark://spark-master-internal.prod.svc.cluster.local:17077 \
    --conf spark.executor.memory=2g \
    --conf spark.driver.memory=2g \
    --conf spark.sql.catalog.star_lakehouse=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.star_lakehouse.type=rest \
    --conf spark.sql.catalog.star_lakehouse.uri=http://polaris-rest.prod.svc.cluster.local:8181/api/catalog \
    --conf spark.sql.catalog.star_lakehouse.credential="${POLARIS_SVC_ID}:${POLARIS_SVC_SECRET}" \
    --conf spark.sql.catalog.star_lakehouse.scope=PRINCIPAL_ROLE:ALL \
    --conf spark.sql.catalog.star_lakehouse.warehouse=star_lakehouse \
    --conf spark.sql.catalog.star_lakehouse.s3.access-key-id="${S3_KEY}" \
    --conf spark.sql.catalog.star_lakehouse.s3.secret-access-key="${S3_SECRET}" \
    --conf spark.sql.catalog.star_lakehouse.s3.endpoint="${S3_ENDPOINT}" \
    --conf spark.sql.catalog.star_lakehouse.s3.path-style-access=true \
    --conf spark.sql.catalog.star_lakehouse.client.region=us-east-2
```

You will get a `spark-sql>` prompt. The `star_lakehouse` catalog is ready.

---

## 4. Run SQL — INSERT / UPDATE / DELETE

### Check current state first

```sql
-- Row count before your change
SELECT COUNT(*) FROM star_lakehouse.demo.customers;

-- Sample a few rows
SELECT customer_id, full_name, email, customer_tier
FROM star_lakehouse.demo.customers
LIMIT 5;
```

### INSERT new rows

```sql
INSERT INTO star_lakehouse.demo.customers
  (customer_id, full_name, email, phone_number, date_of_birth,
   national_id, street_address, city, country_code, ip_address,
   salary, customer_tier, is_active)
VALUES
  (10001, 'John Miller',  'john.miller@example.com',  '+1-555-0101', DATE '1985-03-14',
   'ID-001-JM', '12 Maple St',       'Toronto', 'CA', '192.168.10.1',  95000.00, 'gold',     1),
  (10002, 'Sara Khan',    'sara.khan@example.com',    '+1-555-0102', DATE '1990-07-22',
   'ID-002-SK', '88 Oak Ave',        'London',  'GB', '10.0.0.12',     72000.00, 'silver',   1),
  (10003, 'Luca Rossi',   'luca.rossi@example.com',   '+39-06-5550', DATE '1978-11-05',
   'ID-003-LR', 'Via Roma 3',        'Rome',    'IT', '172.16.0.5',   110000.00, 'platinum', 1),
  (10004, 'Amira Nasser', 'amira.nasser@example.com', '+20-2-5550',  DATE '1995-01-30',
   'ID-004-AN', '45 Nile Corniche',  'Cairo',   'EG', '10.10.10.10',   48000.00, 'standard', 1),
  (10005, 'Wei Zhang',    'wei.zhang@example.com',    '+86-10-5550', DATE '1988-09-17',
   'ID-005-WZ', '8 Jingshan Rd',     'Beijing', 'CN', '192.168.1.100',130000.00, 'platinum', 1);
```

Confirm immediately inside the same shell:
```sql
SELECT COUNT(*) FROM star_lakehouse.demo.customers;
-- Expected: 10005
```

### UPDATE existing rows

```sql
-- Promote a customer tier
UPDATE star_lakehouse.demo.customers
SET customer_tier = 'platinum'
WHERE customer_id = 10002;

-- Deactivate customers in a city
UPDATE star_lakehouse.demo.customers
SET is_active = 0
WHERE city = 'Cairo';
```

### DELETE rows

```sql
-- Delete a specific customer
DELETE FROM star_lakehouse.demo.customers
WHERE customer_id = 10004;

-- Delete by condition
DELETE FROM star_lakehouse.demo.customers
WHERE is_active = 0;
```

Type `exit;` or `Ctrl+D` to close the shell when done.

---

## 5. Re-register the table in HMS

**Run this after every INSERT / UPDATE / DELETE.** This updates the `metadata_location` pointer in HMS so Databricks reads the new snapshot.

```bash
# Find the latest metadata.json on S3
AWS_KEY=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=access_key secret/platform/s3")
AWS_SECRET=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=secret_key secret/platform/s3")

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

# Update HMS entry via Thrift
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  python3 - "customers" "$METADATA_FILE" <<'PYEOF'
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

## 6. Verify in Databricks SQL

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

### `CATALOG_NOT_FOUND: star_lakehouse` in spark-sql
The `--conf spark.sql.catalog.star_lakehouse.*` flags were not passed. Exit and re-open the shell using the full command from Section 3.

### `ForbiddenException: not authorized for INSERT`
The `spark-iceberg-svc` principal lacks the `star_lakehouse_admin` role:
```bash
POLARIS_TOKEN=$(curl -s -X POST http://192.168.1.50:30181/api/catalog/v1/oauth/tokens \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=${POLARIS_SVC_ID}&client_secret=${POLARIS_SVC_SECRET}&scope=PRINCIPAL_ROLE:ALL" \
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
| Spark master pod | `prod/spark-master-*` · label `app=spark,component=master` |
| Polaris in-cluster URI | `http://polaris-rest.prod.svc.cluster.local:8181/api/catalog` |
| Polaris catalog API (NodePort) | `http://192.168.1.50:30181/api/catalog/v1/star_lakehouse/` |
| S3 Iceberg warehouse | `s3://stardata-databricks/iceberg/warehouse/demo/` |
| HMS Thrift (in-cluster) | `hive-metastore.prod.svc.cluster.local:9083` |
| HMS Thrift (NodePort) | `192.168.1.50:30983` |
| Databricks SQL editor | `https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor` |
| Databricks SQL warehouse | `942026cf5e55f3c3` (Serverless Starter) |
| Databricks FOREIGN catalog | `star_lakehouse` (connection: `hms_star_lakehouse`) |
| OpenBao Polaris creds | `secret/platform/polaris` → `spark_svc_id`, `spark_svc_secret` |
| OpenBao S3 creds | `secret/platform/s3` → `access_key`, `secret_key`, `endpoint` |
| OpenBao Databricks PAT | `secret/databricks/pat` → `token`, `workspace` |
