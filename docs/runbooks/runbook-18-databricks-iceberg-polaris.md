# Runbook 18 — Spark → Iceberg → Databricks Pipeline (HMS Federation)

| Field | Value |
|---|---|
| **Runbook ID** | RB-18 |
| **Service** | k8s-platform / data-lakehouse |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-08-27 |
| **Related** | RB-19 (day-to-day write/query guide), RB-15 (Snowflake→Iceberg/STARPUMP), RB-02 (ArgoCD), RB-01 (OpenBao) |

---

## 1. Purpose

End-to-end infrastructure runbook for the Spark → Iceberg → Databricks pipeline:

1. Creates an S3 bucket (`stardata-databricks`) as the Iceberg warehouse
2. Creates a Polaris REST catalog (`star_lakehouse`) pointing at that bucket
3. Uses Spark to generate and write 10 000 synthetic customer rows as an Iceberg table
4. Registers the Iceberg table in a Hive Metastore (HMS) deployed on k8s
5. Exposes the HMS to Databricks Unity Catalog as a `HIVE_METASTORE` FOREIGN catalog

```
Spark (k8s)                   Apache Polaris                AWS S3
──────────────────────         ──────────────────────────    ──────────────────
generate_customers_iceberg ──► star_lakehouse catalog    ──► stardata-databricks/
  10 000 rows                  demo.customers table           iceberg/warehouse/
  synthetic PII schema                                        demo/customers/
        │
        │  HMS registration (after each write)
        ▼
Hive Metastore 2.3.9 (k8s)                         Databricks Unity Catalog
hive-metastore.prod.svc:9083  ◄── FOREIGN catalog ──► star_lakehouse (FOREIGN_CATALOG)
PostgreSQL hive_metastore DB      hms_star_lakehouse      demo.customers
```

> **Day-to-day developer workflow (write in Spark, read in Databricks):** see [RB-19](runbook-19-iceberg-spark-write-databricks-read.md).
> This runbook covers the one-time infrastructure setup and the full test matrix.

---

## 2. Architecture

### 2.1 Components

| Component | Role |
|---|---|
| `generate_customers_iceberg.py` | Spark job — 10k synthetic rows → Polaris Iceberg |
| `starpump_to_databricks.py` | STARPUMP extension — Polaris Iceberg → Databricks Delta |
| Apache Polaris REST catalog | Manages Iceberg metadata; exposes REST catalog API |
| Hive Metastore 2.3.9 | K8s deployment; holds EXTERNAL_TABLE entries pointing at Iceberg metadata |
| AWS S3 `stardata-databricks` | Iceberg warehouse storage |
| Databricks Unity Catalog | Federated catalog via HMS FOREIGN connection |
| OpenBao | All credentials — no secrets in code or YAML |

### 2.2 Credentials map (OpenBao)

| OpenBao path | Keys | Used by |
|---|---|---|
| `secret/databricks/pat` | `token`, `workspace`, `expires` | Databricks API |
| `secret/databricks/s3` | `bucket`, `warehouse_path`, `region` | Spark, Polaris |
| `secret/platform/polaris` | `spark_svc_id`, `spark_svc_secret` | Spark Iceberg catalog |
| `secret/platform/s3` | `access_key`, `secret_key`, `region` | Spark S3 access |
| `secret/hive/credentials` | `db_password`, `aws_access_key`, `aws_secret_key` | HMS pod init |

### 2.3 IAM setup

**Spark S3 access (write path):**
IAM user `arn:aws:iam::586643076710:user/watsonx-s3-connector`
Policy `stardata-databricks-rw` grants `s3:Get/Put/Delete/List` on `arn:aws:s3:::stardata-databricks/*`

**Databricks S3 access (read path via Unity Catalog):**
IAM role `arn:aws:iam::586643076710:role/databricks-unity-catalog`
Trust: `arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL` with external ID `578f6c36-b518-414d-a6fc-8a318b9d580b`
Policy `stardata-databricks-rw` grants same S3 permissions (separate attachment on the role)

---

## 3. Pre-flight environment setup

Run once per terminal session before any test steps:

```bash
# ── OpenBao root token ────────────────────────────────────────────────────────
VAULT_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

# ── Databricks PAT ────────────────────────────────────────────────────────────
DB_TOKEN=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=token secret/databricks/pat")
DB_WS="https://dbc-11a1dbc5-061a.cloud.databricks.com"
WAREHOUSE_ID="942026cf5e55f3c3"

# ── AWS / S3 ──────────────────────────────────────────────────────────────────
AWS_KEY=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=access_key secret/platform/s3")
AWS_SECRET=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=secret_key secret/platform/s3")
AWS_DEFAULT_REGION="us-east-2"
S3_BUCKET="stardata-databricks"

# ── Spark pod ─────────────────────────────────────────────────────────────────
SPARK_POD=$(kubectl get pods -n prod -l app=spark,component=master \
  --no-headers -o custom-columns=NAME:.metadata.name | head -1)

echo "Spark pod: $SPARK_POD  Environment ready"
```

---

## 4. One-time setup (already applied — for reference)

### 4.1 S3 bucket

Created manually in AWS Console:
- **Bucket:** `stardata-databricks`
- **Region:** `us-east-2` (Ohio)
- **Public access:** blocked
- **IAM policy:** `stardata-databricks-rw` on user `watsonx-s3-connector` and role `databricks-unity-catalog`

### 4.2 Polaris catalog

Already created via API:

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

curl -s -X POST http://192.168.1.50:30181/api/management/v1/catalogs \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": {
      "name": "star_lakehouse",
      "type": "INTERNAL",
      "properties": {
        "default-base-location": "s3://stardata-databricks/iceberg/warehouse"
      },
      "storageConfigInfo": {
        "storageType": "S3",
        "stsUnavailable": true,
        "externalId": "polaris-iceberg",
        "region": "us-east-2",
        "allowedLocations": ["s3://stardata-databricks/iceberg/warehouse"]
      }
    }
  }'
```

### 4.3 Databricks Unity Catalog resources (one-time)

All resources below are live and do not need to be re-created.

| Resource | Name | Notes |
|---|---|---|
| Storage credential | `stardata_databricks_s3` | IAM role `arn:aws:iam::586643076710:role/databricks-unity-catalog` |
| External location | `stardata_databricks_iceberg` | `s3://stardata-databricks/` |
| HMS connection | `hms_star_lakehouse` | `HIVE_METASTORE`, `ACTIVE`, host `192.168.1.50:30983` |
| FOREIGN catalog | `star_lakehouse` | `FOREIGN_CATALOG`, connection `hms_star_lakehouse` |

To re-create the HMS connection if needed:

```bash
curl -s -X POST "$DB_WS/api/2.1/unity-catalog/connections" \
  -H "Authorization: Bearer $DB_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "hms_star_lakehouse",
    "connection_type": "HIVE_METASTORE",
    "options": {
      "host": "192.168.1.50",
      "port": "30983"
    }
  }' | python3 -m json.tool
```

To re-create the FOREIGN catalog if needed:

```bash
curl -s -X POST "$DB_WS/api/2.1/unity-catalog/catalogs" \
  -H "Authorization: Bearer $DB_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "star_lakehouse",
    "connection_name": "hms_star_lakehouse",
    "options": {}
  }' | python3 -m json.tool
```

---

## 5. Step-by-step execution

### Step 1 — Verify S3 bucket access

```bash
python3 - "$AWS_KEY" "$AWS_SECRET" <<'EOF'
import sys, boto3
KEY, SEC = sys.argv[1], sys.argv[2]
s3 = boto3.client('s3', region_name='us-east-2',
    aws_access_key_id=KEY, aws_secret_access_key=SEC)
r = s3.list_objects_v2(Bucket='stardata-databricks', MaxKeys=5)
print(f"✅ Bucket accessible, objects: {r['KeyCount']}")
EOF
```

✅ **Pass:** prints `Bucket accessible`
❌ **Fail: AccessDenied** → IAM policy `stardata-databricks-rw` not applied. Apply via AWS Console → IAM → Users → `watsonx-s3-connector` → Add permissions.

---

### Step 2 — Verify Polaris catalog

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

curl -s http://192.168.1.50:30181/api/management/v1/catalogs/star_lakehouse \
  -H "Authorization: Bearer $POLARIS_TOKEN" | python3 -m json.tool
```

✅ **Pass:** `"name": "star_lakehouse"`, `"default-base-location": "s3://stardata-databricks/iceberg/warehouse"`

---

### Step 3 — Run Spark job: generate 10k rows → Iceberg

```bash
kubectl cp scripts/databricks-iceberg-polaris/generate_customers_iceberg.py \
  prod/$SPARK_POD:/tmp/generate_customers_iceberg.py -c spark-master
kubectl cp docker/spark-gluten-velox/scripts/bao_spark_init.py \
  prod/$SPARK_POD:/tmp/bao_spark_init.py -c spark-master

kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env PYTHONPATH=/tmp DISABLE_GLUTEN=1 TOKEN=$VAULT_TOKEN \
  spark-submit \
    --master spark://spark-master-internal.prod.svc.cluster.local:17077 \
    --conf spark.executor.memory=2g \
    --conf spark.driver.memory=2g \
    /tmp/generate_customers_iceberg.py 2>&1 | tail -5
```

✅ **Pass:** last line contains `✅ wrote 10000 rows to star_lakehouse.demo.customers`
❌ **Fail: S3 AccessDenied** → see Step 1 IAM fix
❌ **Fail: catalog not found** → re-run Step 2 to verify Polaris catalog exists

---

### Step 4 — Verify Iceberg table on S3

```bash
python3 - "$AWS_KEY" "$AWS_SECRET" <<'EOF'
import sys, boto3
KEY, SEC = sys.argv[1], sys.argv[2]
s3 = boto3.client('s3', region_name='us-east-2',
    aws_access_key_id=KEY, aws_secret_access_key=SEC)
r = s3.list_objects_v2(
    Bucket='stardata-databricks',
    Prefix='iceberg/warehouse/demo/customers/')
for o in r.get('Contents', []):
    print(o['Key'][:80], f"({o['Size']} bytes)")
print(f"\nTotal objects: {r['KeyCount']}")
EOF
```

✅ **Pass:** shows `metadata/` and `data/` directories under `iceberg/warehouse/demo/customers/`

---

### Step 5 — Verify table via Polaris REST catalog API

```bash
curl -s "http://192.168.1.50:30181/api/catalog/v1/star_lakehouse/namespaces/demo/tables" \
  -H "Authorization: Bearer $POLARIS_TOKEN" | python3 -m json.tool
```

✅ **Pass:** `tables` array includes `customers`

---

### Step 6 — Register table in HMS

After writing to Iceberg, register (or re-register) the table in HMS so Databricks can see it via the FOREIGN catalog.

```bash
# Find the latest metadata.json for the customers table
METADATA_FILE=$(python3 - "$AWS_KEY" "$AWS_SECRET" <<'PYEOF'
import sys, boto3
KEY, SEC = sys.argv[1], sys.argv[2]
s3 = boto3.client('s3', region_name='us-east-2',
    aws_access_key_id=KEY, aws_secret_access_key=SEC)
r = s3.list_objects_v2(
    Bucket='stardata-databricks',
    Prefix='iceberg/warehouse/demo/customers/metadata/')
objects = [o for o in r.get('Contents',[]) if o['Key'].endswith('.metadata.json')]
objects.sort(key=lambda x: x['LastModified'], reverse=True)
print(f"s3://stardata-databricks/{objects[0]['Key']}")
PYEOF
)
echo "Latest metadata: $METADATA_FILE"

# Register in HMS via Thrift client (thrift is baked into the Spark image)
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env PYTHONPATH=/tmp TOKEN=$VAULT_TOKEN \
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
        "table_type": "ICEBERG",
        "metadata_location": METADATA_FILE,
        "EXTERNAL": "TRUE",
    }
))
print(f"✅ HMS: demo.{TABLE_NAME} registered -> {METADATA_FILE}")
t.close()
PYEOF
```

✅ **Pass:** prints `✅ HMS: demo.customers registered -> s3://...metadata.json`

---

### Step 7 — Query Iceberg table from Databricks SQL

Open the SQL Editor at `https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor` and run:

```sql
-- Confirm table is visible via FOREIGN catalog
SHOW TABLES IN star_lakehouse.demo;

-- Row count
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT customer_tier) AS tiers
FROM star_lakehouse.demo.customers;

-- Sample rows
SELECT customer_id, full_name, email, customer_tier
FROM star_lakehouse.demo.customers
LIMIT 10;
```

Or via API:

```bash
curl -s -X POST "$DB_WS/api/2.0/sql/statements" \
  -H "Authorization: Bearer $DB_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"warehouse_id\": \"$WAREHOUSE_ID\",
    \"statement\": \"SELECT COUNT(*) AS total_rows, COUNT(DISTINCT customer_tier) AS tiers FROM star_lakehouse.demo.customers\",
    \"wait_timeout\": \"90s\"
  }" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print('status:', d.get('status',{}).get('state'))
rows = d.get('result',{}).get('data_array',[])
for r in rows: print(r)
"
```

✅ **Pass:** `total_rows = 10000`, `tiers = 4`

> **First-query latency:** FOREIGN catalog queries take 30–90 seconds on first execution while the warehouse loads. Subsequent queries are faster.

---

## 6. Test matrix

| ID | Phase | Test | Expected | Status |
|---|---|---|---|---|
| T-01 | S3 | Bucket accessible by `watsonx-s3-connector` | `✅ Bucket accessible` | ✅ |
| T-02 | Polaris | `star_lakehouse` catalog exists | `200 OK`, correct `default-base-location` | ✅ |
| T-03 | Polaris | `star_lakehouse_admin` principal role exists | Listed in `/api/management/v1/principal-roles` | ✅ |
| T-04 | Polaris | `databricks-connector` principal assigned role | `/api/management/v1/principals/databricks-connector` | ✅ |
| T-05 | Spark | Iceberg write job completes | `✅ wrote 10000 rows` | ✅ |
| T-06 | S3 | Iceberg `metadata/` and `data/` on S3 | Objects under `iceberg/warehouse/demo/customers/` | ✅ |
| T-07 | Polaris API | `demo.customers` table listed | `tables` array includes `customers` | ✅ |
| T-08 | Databricks | HMS connection registered in Unity Catalog | `"name": "hms_star_lakehouse"`, `ACTIVE` | ✅ |
| T-09 | Databricks | `star_lakehouse` FOREIGN catalog in Unity Catalog | `catalog_type: FOREIGN_CATALOG` | ✅ |
| T-10 | HMS | `demo.customers` registered in HMS | `metadata_location` set to latest `.metadata.json` | ✅ |
| T-11 | Databricks SQL | `SHOW TABLES IN star_lakehouse.demo` lists `customers` | `customers` present | ✅ |
| T-12 | Databricks SQL | `COUNT(*)` via FOREIGN catalog | `total_rows=10000`, `tiers=4` | ✅ |

> **HMS federation path:** `ICEBERG_REST` (`enable_iceberg_rest_catalog_connections`) is not provisioned on workspace `dbc-11a1dbc5-061a`. The live path uses a `HIVE_METASTORE` FOREIGN catalog connection — which is provisioned on this account.
>
> | Resource | Value |
> |---|---|
> | HMS connection | `hms_star_lakehouse` (`HIVE_METASTORE`, `ACTIVE`) |
> | FOREIGN catalog | `star_lakehouse` (`FOREIGN_CATALOG`) |
> | Storage credential | `stardata_databricks_s3` (IAM role `arn:aws:iam::586643076710:role/databricks-unity-catalog`) |
> | External location | `stardata_databricks_iceberg` (`s3://stardata-databricks/`) |
> | HMS K8s service | `hive-metastore.prod.svc.cluster.local:9083` · NodePort `30983` |
> | HMS manifest | `manifests/hive/hive-metastore.yaml` |

---

## 7. Troubleshooting

### Polaris returns 401 on catalog API calls
Token expired (default 1 hour). Re-fetch the `POLARIS_TOKEN` using the block in Step 2.

### Spark job fails with `S3Exception: Access Denied`
IAM user `watsonx-s3-connector` lacks permission on `stardata-databricks`. Apply the inline policy from Section 2.3.

### `CATALOG_NOT_FOUND: star_lakehouse` in Databricks SQL
The FOREIGN catalog was deleted or not created. Re-create using the `POST /catalogs` command in Section 4.3.

### Databricks returns stale row count after a Spark write
The HMS `metadata_location` was not updated. Re-run Step 6 (HMS registration).

### Databricks PAT expired
Generate a new token at: `https://dbc-11a1dbc5-061a.cloud.databricks.com/settings/user/developer/access-tokens`
Then update OpenBao:
```bash
VAULT_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)
kubectl exec -n prod openbao-0 -- sh -c \
  "VAULT_TOKEN=$VAULT_TOKEN vault kv patch secret/databricks/pat token='dapi...' expires='YYYY-MM-DD'"
```

### `TTransportException` on HMS Thrift call
HMS pod restarted or not ready:
```bash
kubectl get pods -n prod -l app=hive-metastore
# Wait for 1/1 Running, then retry Step 6
```

### `stsUnavailable` reset after Polaris restart
Re-apply the DB fix — see [RB-19 Troubleshooting](runbook-19-iceberg-spark-write-databricks-read.md#spark-write-fails-stsexception-not-authorized-to-perform-stsassumerole).

---

## 8. Key paths reference

| Resource | Path / URL |
|---|---|
| S3 bucket | `s3://stardata-databricks/iceberg/warehouse/` |
| Polaris catalog API | `http://192.168.1.50:30181/api/catalog/v1/star_lakehouse/` |
| Polaris management API | `http://192.168.1.50:30181/api/management/v1/` |
| Polaris in-cluster URI | `http://polaris-rest.prod.svc.cluster.local:8181/api/catalog` |
| HMS Thrift (in-cluster) | `hive-metastore.prod.svc.cluster.local:9083` |
| HMS Thrift (NodePort) | `192.168.1.50:30983` |
| HMS manifest | `manifests/hive/hive-metastore.yaml` |
| Databricks workspace | `https://dbc-11a1dbc5-061a.cloud.databricks.com` |
| Databricks SQL editor | `https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor` |
| Databricks SQL warehouse | `942026cf5e55f3c3` (Serverless Starter) |
| AWS IAM role (Databricks) | `arn:aws:iam::586643076710:role/databricks-unity-catalog` |
| Databricks storage credential | `stardata_databricks_s3` |
| Databricks external location | `stardata_databricks_iceberg` (`s3://stardata-databricks/`) |
| Databricks HMS connection | `hms_star_lakehouse` (`HIVE_METASTORE`) |
| Databricks FOREIGN catalog | `star_lakehouse` (`FOREIGN_CATALOG`) |
| OpenBao PAT | `secret/databricks/pat` |
| OpenBao HMS credentials | `secret/hive/credentials` |
| Script: Spark data gen | `scripts/databricks-iceberg-polaris/generate_customers_iceberg.py` |
| Script: STARPUMP copy | `scripts/databricks-iceberg-polaris/starpump_to_databricks.py` |
| Script: T-09/T-12 verify | `scripts/databricks-iceberg-polaris/t09_t12_verify.py` |

---

## 9. Updating Databricks credentials in OpenBao

When the PAT changes or the workspace is updated, patch the secret in-place:

```bash
VAULT_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

kubectl exec -n prod openbao-0 -- sh -c \
  "VAULT_TOKEN=$VAULT_TOKEN vault kv patch secret/databricks/pat \
   token='<YOUR_DATABRICKS_PAT>' \
   workspace='https://dbc-11a1dbc5-061a.cloud.databricks.com' \
   account_id='578f6c36-b518-414d-a6fc-8a318b9d580b'"

# Verify
kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get secret/databricks/pat"
```

> **PAT rotation reminder:** Databricks PATs expire. Regenerate at
> `https://dbc-11a1dbc5-061a.cloud.databricks.com/settings/user/developer/access-tokens`
> and re-apply the patch above.

---

## 10. Architecture — Spark / Iceberg / HMS / Databricks

### 10.1 Full data flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  Spark (k8s — prod namespace)                                                │
│                                                                              │
│  df.writeTo("star_lakehouse.demo.<table>")  ← writes Iceberg parquet + meta │
│  spark.table("star_lakehouse.demo.<table>") ← reads                         │
└──────────────┬───────────────────────────────────────────────────────────────┘
               │  Iceberg REST API  (OAuth2 client_credentials)
               │  http://polaris-rest.prod.svc.cluster.local:8181/api/catalog
               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  Apache Polaris 1.6.0  (k8s, prod namespace)                                 │
│                                                                              │
│  Catalog  : star_lakehouse   (INTERNAL, S3-backed)                           │
│  Namespace: demo                                                             │
│  Tables   : customers, <your_tables>                                         │
│                                                                              │
│  • Manages Iceberg metadata (snapshots, manifests, schema evolution)         │
│  • Authorises writes via principal role star_lakehouse_admin                 │
└──────────────┬───────────────────────────────────────────────────────────────┘
               │  writes / reads Iceberg parquet + metadata JSON
               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  AWS S3 — s3://stardata-databricks/                                          │
│                                                                              │
│  iceberg/warehouse/                                                          │
│  └── demo/                                                                   │
│      └── <table>/                                                            │
│          ├── metadata/  ← .metadata.json, snap-*.avro, *-m0.avro            │
│          └── data/      ← snappy parquet, partitioned by bucket(customer_id) │
└──────────────┬───────────────────────────────────────────────────────────────┘
               │  HMS registration: update metadata_location pointer after write
               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  Hive Metastore 2.3.9  (k8s, prod namespace)                                 │
│                                                                              │
│  Thrift endpoint : hive-metastore.prod.svc.cluster.local:9083                │
│  NodePort        : 192.168.1.50:30983                                        │
│  Backend DB      : PostgreSQL hive_metastore (postgresql.prod:5432)          │
│                                                                              │
│  Stores EXTERNAL_TABLE entry for every registered Iceberg table:             │
│    table_type        = ICEBERG                                               │
│    metadata_location = s3://stardata-databricks/iceberg/warehouse/           │
│                        demo/<table>/metadata/<latest>.metadata.json          │
│    location          = s3://stardata-databricks/iceberg/warehouse/demo/<tbl> │
└──────────────┬───────────────────────────────────────────────────────────────┘
               │  HIVE_METASTORE FOREIGN catalog connection
               │  hms_star_lakehouse  (Unity Catalog API 2.1)
               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  Databricks Unity Catalog                                                    │
│                                                                              │
│  Storage credential : stardata_databricks_s3                                 │
│    IAM role         : arn:aws:iam::586643076710:role/databricks-unity-catalog │
│    Trusted by       : arn:aws:iam::414351767826:role/unity-catalog-prod-*    │
│    External ID      : 578f6c36-b518-414d-a6fc-8a318b9d580b                  │
│                                                                              │
│  External location  : stardata_databricks_iceberg                            │
│    URL              : s3://stardata-databricks/                              │
│                                                                              │
│  FOREIGN catalog    : star_lakehouse  (FOREIGN_CATALOG)                      │
│    Connection       : hms_star_lakehouse                                     │
│                                                                              │
│  Visible in SQL Editor:                                                      │
│    SELECT * FROM star_lakehouse.demo.<table>                                 │
│    SHOW TABLES IN star_lakehouse.demo                                        │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 10.2 Key rules

| Rule | Detail |
|---|---|
| **Write path** | Always write via `star_lakehouse` Polaris catalog from Spark. Never write directly to S3. |
| **Polaris = truth** | Polaris owns the Iceberg snapshot chain. HMS is a read-only mirror pointing at the latest metadata file. |
| **After every Spark write** | Re-register the table in HMS (Step 6 above, or RB-19 Step 3) so Databricks sees the latest snapshot. |
| **HMS re-registration is lightweight** | It only updates the `metadata_location` pointer in PostgreSQL — no data movement. |
| **S3 credentials** | Databricks reads S3 via IAM role `databricks-unity-catalog` (cross-account assume-role). The IAM user `watsonx-s3-connector` is used by Spark only. |

### 10.3 Troubleshooting HMS registration

| Symptom | Cause | Fix |
|---|---|---|
| `CATALOG_NOT_FOUND: star_lakehouse` | FOREIGN catalog not created | Re-create via Section 4.3 |
| Databricks returns stale row count | HMS `metadata_location` not updated after write | Re-run Step 6 |
| `EXTERNAL_LOCATION_DOES_NOT_EXIST` | S3 path not covered by external location | External location covers `s3://stardata-databricks/` — all sub-paths are included |
| `403 Access Denied` from Databricks on S3 | IAM role `databricks-unity-catalog` lacks S3 permission | Verify policy `stardata-databricks-rw` is attached to the role in AWS Console |
| `TTransportException` on Thrift call | HMS pod restarted | `kubectl get pods -n prod -l app=hive-metastore` — wait for `1/1 Running` |
| Table already exists error | Re-registration without drop | Registration script drops stale entry automatically before re-creating |
