# Runbook 18 — Databricks ↔ Iceberg ↔ Polaris Pipeline

| Field | Value |
|---|---|
| **Runbook ID** | RB-18 |
| **Service** | k8s-platform / data-lakehouse |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-08-26 |
| **Related** | RB-15 (Snowflake→Iceberg/STARPUMP), RB-02 (ArgoCD), RB-01 (OpenBao) |

---

## 1. Purpose

End-to-end pipeline that:

1. Creates an S3 bucket (`stardata-databricks`) as the Iceberg warehouse
2. Creates a Polaris REST catalog (`star_lakehouse`) pointing at that bucket
3. Uses Spark to generate and write 10 000 synthetic customer rows as an Iceberg table
4. Exposes the Iceberg catalog to Databricks Unity Catalog as a `FOREIGN` catalog
5. Creates a managed Databricks table by reading from the Polaris Iceberg table
6. Uses **STARPUMP** to copy the Iceberg table (metadata + data) into Databricks

```
Spark (k8s)                    Polaris REST                  AWS S3
──────────────────────         ──────────────────────────    ──────────────────
generate_customers_iceberg ──► star_lakehouse catalog    ──► stardata-databricks/
  10 000 rows                  demo.customers table           iceberg/warehouse/
  synthetic PII schema                                        demo/customers/
        │                             │
        │                             │  REST catalog federation
        │                             ▼
        │                      Databricks Unity Catalog
        │                      star_lakehouse (FOREIGN)
        │                      demo.customers  ◄────────────┐
        │                                                    │
        └────────────── starpump_to_databricks ─────────────┘
                         CTAS: Iceberg → Delta
                         star_lakehouse.demo.customers_delta
```

---

## 2. Architecture

### 2.1 Components

| Component | Role |
|---|---|
| `generate_customers_iceberg.py` | Spark job — 10k synthetic rows → Polaris Iceberg |
| `starpump_to_databricks.py` | STARPUMP extension — Polaris Iceberg → Databricks Delta |
| Polaris REST catalog | Manages Iceberg metadata; exposes REST catalog API |
| AWS S3 `stardata-databricks` | Iceberg warehouse storage |
| Databricks Unity Catalog | Metastore for federated + managed tables |
| OpenBao | All credentials — no secrets in code or YAML |

### 2.2 Credentials map (OpenBao)

| OpenBao path | Keys | Used by |
|---|---|---|
| `secret/databricks/pat` | `token`, `workspace`, `expires` | STARPUMP, Databricks API |
| `secret/databricks/s3` | `bucket`, `warehouse_path`, `region` | Spark, Polaris |
| `secret/databricks/polaris-connector` | `client_id`, `client_secret` | Databricks↔Polaris OAuth |
| `secret/platform/polaris` | `spark_svc_id`, `spark_svc_secret` | Spark Iceberg catalog |
| `secret/platform/s3` | `access_key`, `secret_key`, `region` | Spark S3 access |

### 2.3 IAM setup

IAM user: `arn:aws:iam::586643076710:user/watsonx-s3-connector`
Policy `stardata-databricks-rw` grants:
- `s3:GetBucketLocation`, `s3:ListBucket`
- `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`
on `arn:aws:s3:::stardata-databricks` and `arn:aws:s3:::stardata-databricks/*`

---

## 3. Pre-flight environment setup

Run once per terminal session before any test steps:

```bash
# ── OpenBao root token ────────────────────────────────────────────────────────
VAULT_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

# ── Polaris admin token ───────────────────────────────────────────────────────
POLARIS_ROOT_ID=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=root_client_id secret/platform/polaris")
POLARIS_ROOT_SECRET=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=root_client_secret secret/platform/polaris")
POLARIS_TOKEN=$(curl -s -X POST \
  http://192.168.1.50:30181/api/catalog/v1/oauth/tokens \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=${POLARIS_ROOT_ID}&client_secret=${POLARIS_ROOT_SECRET}&scope=PRINCIPAL_ROLE:ALL" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# ── Databricks PAT ────────────────────────────────────────────────────────────
DB_TOKEN=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=token secret/databricks/pat")
DB_WS="https://dbc-11a1dbc5-061a.cloud.databricks.com"

# ── AWS / S3 ──────────────────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=access_key secret/platform/s3")
AWS_SECRET_ACCESS_KEY=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=secret_key secret/platform/s3")
AWS_DEFAULT_REGION="us-east-2"
S3_BUCKET="stardata-databricks"

echo "Environment ready"
```

---

## 4. One-time setup (already applied — for reference)

### 4.1 S3 bucket

Created manually in AWS Console:
- **Bucket:** `stardata-databricks`
- **Region:** `us-east-2` (Ohio)
- **Public access:** blocked
- **IAM policy:** `stardata-databricks-rw` on user `watsonx-s3-connector`

### 4.2 Polaris catalog

Already created via API:

```bash
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
        "roleArn": "arn:aws:iam::586643076710:user/watsonx-s3-connector",
        "externalId": "polaris-iceberg",
        "region": "us-east-2",
        "allowedLocations": ["s3://stardata-databricks/iceberg/warehouse"]
      }
    }
  }'
```

### 4.3 Databricks connector principal

```bash
# Principal: databricks-connector
# Credentials stored in OpenBao: secret/databricks/polaris-connector
# client_id: 1fd40dde5fefc856
# Assigned role: star_lakehouse_admin → catalog role: star_lakehouse_full
```

---

## 5. Step-by-step execution

### Step 1 — Verify S3 bucket access

```bash
python3 - <<'EOF'
import boto3
s3 = boto3.client('s3', region_name='us-east-2',
    aws_access_key_id='***REDACTED_AWS_KEY***',
    aws_secret_access_key='***REDACTED_AWS_SECRET***')
r = s3.list_objects_v2(Bucket='stardata-databricks', MaxKeys=5)
print(f"✅ Bucket accessible, objects: {r['KeyCount']}")
EOF
```

✅ **Pass:** prints `Bucket accessible`
❌ **Fail: AccessDenied** → IAM policy `stardata-databricks-rw` not applied yet. Apply it via:
AWS Console → IAM → Users → `watsonx-s3-connector` → Add permissions → Inline policy → paste JSON from Section 2.3

---

### Step 2 — Verify Polaris catalog

```bash
curl -s http://192.168.1.50:30181/api/management/v1/catalogs/star_lakehouse \
  -H "Authorization: Bearer $POLARIS_TOKEN" | python3 -m json.tool
```

✅ **Pass:** `"name": "star_lakehouse"`, `"default-base-location": "s3://stardata-databricks/iceberg/warehouse"`

---

### Step 3 — Run Spark job: generate 10k rows → Polaris Iceberg

```bash
SPARK_POD=$(kubectl get pods -n prod -l app=spark-master \
  --field-selector=status.phase=Running \
  --no-headers -o custom-columns=NAME:.metadata.name | head -1)

# Copy script into pod
kubectl cp scripts/databricks-iceberg-polaris/generate_customers_iceberg.py \
  prod/$SPARK_POD:/tmp/generate_customers_iceberg.py

# Submit job
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  spark-submit \
    --master spark://spark-master.prod.svc.cluster.local:7077 \
    --conf spark.executor.memory=2g \
    --conf spark.driver.memory=2g \
    /tmp/generate_customers_iceberg.py 2>&1 | tail -20
```

✅ **Pass:** last line contains `✅ wrote 10000 rows to star_lakehouse.demo.customers`
❌ **Fail: S3 AccessDenied** → see Step 1 IAM fix
❌ **Fail: catalog not found** → re-run Step 2 to verify Polaris catalog exists

---

### Step 4 — Verify Iceberg table on S3

```bash
python3 - <<'EOF'
import boto3
s3 = boto3.client('s3', region_name='us-east-2',
    aws_access_key_id='***REDACTED_AWS_KEY***',
    aws_secret_access_key='***REDACTED_AWS_SECRET***')
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
# Get Iceberg namespace list
curl -s "http://192.168.1.50:30181/api/catalog/v1/star_lakehouse/namespaces" \
  -H "Authorization: Bearer $POLARIS_TOKEN" | python3 -m json.tool

# Get table list in demo namespace
curl -s "http://192.168.1.50:30181/api/catalog/v1/star_lakehouse/namespaces/demo/tables" \
  -H "Authorization: Bearer $POLARIS_TOKEN" | python3 -m json.tool
```

✅ **Pass:** `namespaces` includes `demo`; `tables` includes `customers`

---

### Step 6 — Register Polaris as Databricks Unity Catalog connection

```bash
# Create a Databricks connection pointing to Polaris REST
DB_POLARIS_ID=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=client_id secret/databricks/polaris-connector")
DB_POLARIS_SECRET=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=client_secret secret/databricks/polaris-connector")

# Get a short-lived Polaris token for the connection options
POLARIS_SHORT_TOKEN=$(curl -s -X POST \
  http://192.168.1.50:30181/api/catalog/v1/oauth/tokens \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=${DB_POLARIS_ID}&client_secret=${DB_POLARIS_SECRET}&scope=PRINCIPAL_ROLE:ALL" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

curl -s -X POST "$DB_WS/api/2.1/unity-catalog/connections" \
  -H "Authorization: Bearer $DB_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"polaris_star_lakehouse\",
    \"connection_type\": \"ICEBERG_REST\",
    \"options\": {
      \"uri\": \"https://192.168.1.50:30553/api/catalog\",
      \"token\": \"$POLARIS_SHORT_TOKEN\",
      \"warehouse\": \"star_lakehouse\"
    }
  }" | python3 -m json.tool
```

✅ **Pass:** response includes `"name": "polaris_star_lakehouse"`

> **Note:** Polaris must be reachable from the Databricks control plane. If your cluster
> is not publicly exposed, use the TLS NodePort (`30553`) with a valid certificate,
> or use Databricks private networking / VPC peering.

---

### Step 7 — Create star_lakehouse catalog in Databricks Unity Catalog

```bash
curl -s -X POST "$DB_WS/api/2.1/unity-catalog/catalogs" \
  -H "Authorization: Bearer $DB_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "star_lakehouse",
    "connection_name": "polaris_star_lakehouse",
    "options": {"warehouse": "star_lakehouse"}
  }' | python3 -m json.tool
```

✅ **Pass:** `"name": "star_lakehouse"`, `"catalog_type": "FOREIGN"`

---

### Step 8 — Query Iceberg table from Databricks SQL

Open Databricks SQL Editor or run via API:

```bash
WAREHOUSE_ID="942026cf5e55f3c3"

# Start warehouse if stopped
curl -s -X POST "$DB_WS/api/2.0/sql/warehouses/$WAREHOUSE_ID/start" \
  -H "Authorization: Bearer $DB_TOKEN"

# Submit query
curl -s -X POST "$DB_WS/api/2.0/sql/statements" \
  -H "Authorization: Bearer $DB_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"warehouse_id\": \"$WAREHOUSE_ID\",
    \"statement\": \"SELECT customer_id, full_name, email, customer_tier FROM star_lakehouse.demo.customers LIMIT 5\",
    \"wait_timeout\": \"30s\"
  }" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print('status:', d.get('status',{}).get('state'))
rows = d.get('result',{}).get('data_array',[])
for r in rows: print(r)
"
```

✅ **Pass:** returns 5 rows with real customer data from the Iceberg table

---

### Step 9 — Create managed Databricks Delta table (STARPUMP copy)

```bash
SPARK_POD=$(kubectl get pods -n prod -l app=spark-master \
  --field-selector=status.phase=Running \
  --no-headers -o custom-columns=NAME:.metadata.name | head -1)

kubectl cp scripts/databricks-iceberg-polaris/starpump_to_databricks.py \
  prod/$SPARK_POD:/tmp/starpump_to_databricks.py

kubectl exec -n prod $SPARK_POD -c spark-master -- \
  spark-submit \
    --master spark://spark-master.prod.svc.cluster.local:7077 \
    --conf spark.executor.memory=2g \
    /tmp/starpump_to_databricks.py 2>&1 | tail -10
```

✅ **Pass:** `✅ 10000 rows copied to star_lakehouse.demo.customers_delta`

---

### Step 10 — Verify managed table in Databricks

```bash
curl -s -X POST "$DB_WS/api/2.0/sql/statements" \
  -H "Authorization: Bearer $DB_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"warehouse_id\": \"$WAREHOUSE_ID\",
    \"statement\": \"SELECT COUNT(*) AS total, COUNT(DISTINCT customer_tier) AS tiers FROM star_lakehouse.demo.customers LIMIT 1\",
    \"wait_timeout\": \"30s\"
  }" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print('status:', d.get('status',{}).get('state'))
print('result:', d.get('result',{}).get('data_array',[]))
"
```

✅ **Pass:** `total = 10000`, `tiers = 4`

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
| T-10 | Databricks | SELECT 10 000 rows from `star_lakehouse.demo.customers` | `total_rows=10000`, 4 tiers | ✅ see §10 |
| T-11 | STARPUMP | Iceberg → Delta copy completes | `✅ 10000 rows copied` | ⬜ Pending |
| T-12 | Databricks SQL | COUNT via FOREIGN catalog | `total_rows=10000`, `tiers=4` | ✅ see §10 |

> **T-08 / T-09 implemented via HMS federation (Option A):**
> `ICEBERG_REST` connection type (`enable_iceberg_rest_catalog_connections`) is not
> provisioned on this workspace. Instead, the `demo.customers` Iceberg table is registered
> in a standalone **Hive Metastore 2.3.9** (`hive-metastore.prod.svc.cluster.local:9083`,
> NodePort `192.168.1.50:30983`) backed by PostgreSQL, and exposed to Databricks as a
> `HIVE_METASTORE` FOREIGN catalog connection — which **is** provisioned on this account.
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
Token expired (default 1 hour). Re-fetch:
```bash
POLARIS_TOKEN=$(curl -s -X POST \
  http://192.168.1.50:30181/api/catalog/v1/oauth/tokens \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=${POLARIS_ROOT_ID}&client_secret=${POLARIS_ROOT_SECRET}&scope=PRINCIPAL_ROLE:ALL" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
```

### Spark job fails with `software.amazon.awssdk.services.s3.model.S3Exception: Access Denied`
IAM user `watsonx-s3-connector` lacks permission on `stardata-databricks`. Apply the inline policy from Section 2.3.

### Databricks SQL returns `CATALOG_NOT_FOUND: star_lakehouse`
The FOREIGN catalog was not created yet. Run Step 7.

### Databricks PAT expired
Generate a new token at: https://dbc-11a1dbc5-061a.cloud.databricks.com/settings/user/developer/access-tokens
Then update OpenBao:
```bash
VAULT_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)
kubectl exec -n prod openbao-0 -- sh -c \
  "VAULT_TOKEN=$VAULT_TOKEN vault kv patch secret/databricks/pat token='dapi...' expires='YYYY-MM-DD'"
```

### `starpump_to_databricks.py` fails — `org.apache.spark.sql.AnalysisException: Table not found`
The Iceberg table was not written yet. Run Step 3 (generate_customers_iceberg.py) first.

---

## 8. Key paths reference

| Resource | Path / URL |
|---|---|
| S3 bucket | `s3://stardata-databricks/iceberg/warehouse/` |
| Polaris catalog API | `http://192.168.1.50:30181/api/catalog/v1/star_lakehouse/` |
| Polaris management API | `http://192.168.1.50:30181/api/management/v1/` |
| Polaris TLS (external) | `https://192.168.1.50:30553/` |
| Databricks workspace | `https://dbc-11a1dbc5-061a.cloud.databricks.com` |
| Databricks account ID | `578f6c36-b518-414d-a6fc-8a318b9d580b` |
| Databricks SQL warehouse | `942026cf5e55f3c3` (Serverless Starter) |
| OpenBao PAT | `secret/databricks/pat` |
| OpenBao Polaris connector | `secret/databricks/polaris-connector` |
| OpenBao HMS credentials | `secret/hive/credentials` |
| Script: Spark data gen | `scripts/databricks-iceberg-polaris/generate_customers_iceberg.py` |
| Script: STARPUMP copy | `scripts/databricks-iceberg-polaris/starpump_to_databricks.py` |
| Script: HMS table registration | `scripts/databricks-iceberg-polaris/t09_t12_verify.py` |
| HMS manifest | `manifests/hive/hive-metastore.yaml` |
| HMS Thrift (in-cluster) | `hive-metastore.prod.svc.cluster.local:9083` |
| HMS Thrift (NodePort) | `192.168.1.50:30983` |
| AWS IAM role | `arn:aws:iam::586643076710:role/databricks-unity-catalog` |
| Databricks storage credential | `stardata_databricks_s3` |
| Databricks external location | `stardata_databricks_iceberg` (`s3://stardata-databricks/`) |
| Databricks HMS connection | `hms_star_lakehouse` (`HIVE_METASTORE`) |
| Databricks FOREIGN catalog | `star_lakehouse` (`FOREIGN_CATALOG`) |

---

## 9. Updating Databricks credentials in OpenBao

When the PAT changes or the workspace is updated, patch the secret in-place:

```bash
VAULT_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

# Update PAT and workspace URL for the new account
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
               │  HMS registers table location + metadata_location pointer
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
│    authorized_paths : s3://stardata-databricks/iceberg/warehouse             │
│                       s3://stardata-databricks/hive/warehouse                │
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
| **Polaris = truth** | Polaris owns the Iceberg snapshot chain. HMS is a read-only mirror pointing at the metadata file. |
| **After every Spark write** | Re-register the table in HMS using `scripts/databricks-iceberg-polaris/t09_t12_verify.py` (or the helper below) so Databricks sees the latest snapshot. |
| **HMS re-registration is lightweight** | It only updates the `metadata_location` pointer in PostgreSQL — no data movement. |
| **S3 credentials** | Databricks reads S3 via the IAM role `databricks-unity-catalog` (cross-account assume-role). The IAM user `watsonx-s3-connector` is used by Spark only. |

---

## 11. Write data in Spark → see latest in Databricks

This is the day-to-day developer workflow: write new or updated data from Spark, then query it immediately from Databricks SQL.

### Step 1 — Pre-flight (once per terminal session)

```bash
# Get Spark pod name
SPARK_POD=$(kubectl get pods -n prod -l app=spark,component=master \
  --no-headers -o custom-columns=NAME:.metadata.name | head -1)

# Get OpenBao root token
VAULT_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

# Copy bao_spark_init.py to pod (once per session — survives restarts if pod is same)
kubectl cp docker/spark-gluten-velox/scripts/bao_spark_init.py \
  prod/$SPARK_POD:/tmp/bao_spark_init.py -c spark-master

echo "Pod: $SPARK_POD  Token: ${VAULT_TOKEN:0:8}..."
```

### Step 2 — Write data to Iceberg via Spark

Write your script locally, copy it to the pod, and submit. Minimal example:

```python
# my_write.py — runs inside the Spark pod
import sys
sys.path.insert(0, "/tmp")
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType

CATALOG   = "star_lakehouse"
NAMESPACE = "demo"
TABLE     = "my_table"          # change per table

bao  = BaoSparkInit()
pol  = bao.polaris_creds()
s3   = bao.s3_creds()
conf = bao.spark_conf(app_name=f"write-{TABLE}")

# Wire star_lakehouse Polaris catalog
conf.set(f"spark.sql.catalog.{CATALOG}",             "org.apache.iceberg.spark.SparkCatalog")
conf.set(f"spark.sql.catalog.{CATALOG}.type",        "rest")
conf.set(f"spark.sql.catalog.{CATALOG}.uri",         "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog")
conf.set(f"spark.sql.catalog.{CATALOG}.credential",  f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}")
conf.set(f"spark.sql.catalog.{CATALOG}.scope",       "PRINCIPAL_ROLE:ALL")
conf.set(f"spark.sql.catalog.{CATALOG}.warehouse",   CATALOG)
conf.set(f"spark.sql.catalog.{CATALOG}.s3.access-key-id",     s3["access_key"])
conf.set(f"spark.sql.catalog.{CATALOG}.s3.secret-access-key", s3["secret_key"])
conf.set(f"spark.sql.catalog.{CATALOG}.s3.endpoint",          s3["endpoint"])
conf.set(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
conf.set(f"spark.sql.catalog.{CATALOG}.client.region",        s3["region"])

spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Create namespace if needed
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")

# Build your DataFrame
schema = StructType([
    StructField("id",    LongType(),   False),
    StructField("name",  StringType(), False),
    StructField("value", StringType(), True),
])
rows = [(1, "alice", "100"), (2, "bob", "200")]
df = spark.createDataFrame(rows, schema).withColumn("updated_at", F.current_timestamp())

# Write — use .append() to add rows, .createOrReplace() for full reload
df.writeTo(f"{CATALOG}.{NAMESPACE}.{TABLE}") \
  .tableProperty("write.format.default", "parquet") \
  .tableProperty("write.parquet.compression-codec", "snappy") \
  .createOrReplace()

count = spark.table(f"{CATALOG}.{NAMESPACE}.{TABLE}").count()
print(f"SUCCESS: wrote {count} rows to {CATALOG}.{NAMESPACE}.{TABLE}")
spark.stop()
```

```bash
# Copy and submit
kubectl cp my_write.py prod/$SPARK_POD:/tmp/my_write.py -c spark-master
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env PYTHONPATH=/tmp DISABLE_GLUTEN=1 TOKEN=$VAULT_TOKEN \
  spark-submit \
    --master spark://spark-master-internal.prod.svc.cluster.local:17077 \
    --conf spark.executor.memory=2g \
    --conf spark.driver.memory=2g \
    /tmp/my_write.py 2>&1 | tail -5
```

✅ **Pass:** last line contains `SUCCESS: wrote N rows to star_lakehouse.demo.my_table`

### Step 3 — Register the table in HMS

After every `createOrReplace()` or the first write of a new table, register (or re-register) it in HMS so Databricks picks up the latest metadata pointer.

```bash
# hms_register.sh — register any table by name
TABLE_NAME="my_table"    # ← change this

# Find the latest metadata.json for the table
VAULT_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)
AWS_KEY=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=access_key secret/platform/s3")
AWS_SEC=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=secret_key secret/platform/s3")

METADATA_FILE=$(python3 - "$AWS_KEY" "$AWS_SEC" "$TABLE_NAME" <<'PYEOF'
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

# Register in HMS via direct Thrift client
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env PYTHONPATH=/tmp TOKEN=$VAULT_TOKEN \
  python3 - "$TABLE_NAME" "$METADATA_FILE" <<'PYEOF'
import sys, time, getpass
from thrift.transport import TSocket, TTransport
from thrift.protocol  import TBinaryProtocol
from hive_metastore   import ThriftHiveMetastore
from hive_metastore.ttypes import Table, StorageDescriptor, SerDeInfo, FieldSchema

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
        "table_type": "ICEBERG",
        "metadata_location": METADATA_FILE,
        "EXTERNAL": "TRUE",
    }
))
print(f"HMS: demo.{TABLE_NAME} registered -> {METADATA_FILE}")
t.close()
PYEOF
```

✅ **Pass:** prints `HMS: demo.<table> registered -> s3://...metadata.json`

> **Note for `.append()` writes:** appending adds a new Iceberg snapshot but does not change
> the table root location — only the latest `metadata.json` file changes. Re-run Step 3 after
> every append to point HMS at the new snapshot so Databricks sees all rows.

### Step 4 — Query in Databricks SQL

Open the **SQL Editor** at `https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor` and paste:

```sql
-- Check the table is visible
SHOW TABLES IN star_lakehouse.demo;

-- Row count
SELECT COUNT(*) AS total_rows
FROM star_lakehouse.demo.my_table;     -- replace my_table with your table name

-- Sample rows
SELECT *
FROM star_lakehouse.demo.my_table
LIMIT 10;

-- Filter
SELECT *
FROM star_lakehouse.demo.my_table
WHERE value = '100';
```

✅ **Pass:** `SHOW TABLES` lists your table; `COUNT(*)` returns the expected row count.

> **Warehouse auto-start:** the Serverless Starter Warehouse starts automatically on first
> query. Allow 20–30 seconds if it was stopped.

> **Snapshot lag:** Databricks reads the metadata pointer stored in HMS. If you wrote new
> rows via `.append()` but forgot to re-run Step 3, Databricks will see the old snapshot
> count until HMS is updated.

### Step 5 — Verify HMS registration at any time

```bash
# List all tables registered in HMS
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
    tables = c.get_all_tables(db)
    for tbl in tables:
        entry = c.get_table(db, tbl)
        meta = entry.parameters.get("metadata_location","n/a")
        print(f"  {db:10s}.{tbl:25s}  {meta[-70:]}")
t.close()
PYEOF
```

### 10.3 Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `CATALOG_NOT_FOUND: star_lakehouse` | FOREIGN catalog not created | Verify T-09 — re-create via §5 Step 6 |
| Databricks returns stale row count | HMS `metadata_location` not updated after write | Re-run Step 3 (HMS re-registration) |
| `EXTERNAL_LOCATION_DOES_NOT_EXIST` | S3 path not covered by `stardata_databricks_iceberg` | External location covers `s3://stardata-databricks/` — all sub-paths are covered |
| `403 Access Denied` from Databricks on S3 | IAM role `databricks-unity-catalog` lacks S3 permission | Verify inline policy `stardata-databricks-rw` is attached to the role in AWS Console |
| `HMS: TTransportException` on Step 3 | HMS pod restarted | `kubectl get pods -n prod -l app=hive-metastore` — wait for `1/1 Running` |
| `hive_metastore` Thrift: table already exists | Previous registration not dropped | Step 3 script drops stale entry automatically before re-creating |
