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
DB_WS="https://dbc-48ef5678-3df7.cloud.databricks.com"

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
kubectl cp docs/runbooks/databricks-iceberg-polaris/generate_customers_iceberg.py \
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
WAREHOUSE_ID="2c23ed9f013093c4"

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

kubectl cp docs/runbooks/databricks-iceberg-polaris/starpump_to_databricks.py \
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

| ID | Phase | Test | Expected |
|---|---|---|---|
| T-01 | S3 | Bucket accessible by `watsonx-s3-connector` | `✅ Bucket accessible` |
| T-02 | Polaris | `star_lakehouse` catalog exists | `200 OK`, correct `default-base-location` |
| T-03 | Polaris | `star_lakehouse_admin` principal role exists | Listed in `/api/management/v1/principal-roles` |
| T-04 | Polaris | `databricks-connector` principal assigned role | `/api/management/v1/principals/databricks-connector` |
| T-05 | Spark | Iceberg write job completes | `✅ wrote 10000 rows` |
| T-06 | S3 | Iceberg `metadata/` and `data/` on S3 | Objects under `iceberg/warehouse/demo/customers/` |
| T-07 | Polaris API | `demo.customers` table listed | `tables` array includes `customers` |
| T-08 | Databricks | Polaris connection registered | `"name": "polaris_star_lakehouse"` |
| T-09 | Databricks | `star_lakehouse` catalog visible in Unity Catalog | `catalog_type: FOREIGN` |
| T-10 | Databricks SQL | SELECT from `star_lakehouse.demo.customers` | 5 rows, real data |
| T-11 | STARPUMP | Iceberg → Delta copy completes | `✅ 10000 rows copied` |
| T-12 | Databricks SQL | COUNT managed Delta table | `total=10000`, `tiers=4` |

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
Generate a new token at: https://dbc-48ef5678-3df7.cloud.databricks.com/settings/user/developer/access-tokens
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
| Databricks workspace | `https://dbc-48ef5678-3df7.cloud.databricks.com` |
| Databricks SQL warehouse | `2c23ed9f013093c4` (Serverless Starter) |
| OpenBao PAT | `secret/databricks/pat` |
| OpenBao Polaris connector | `secret/databricks/polaris-connector` |
| Script: Spark data gen | `docs/runbooks/databricks-iceberg-polaris/generate_customers_iceberg.py` |
| Script: STARPUMP copy | `docs/runbooks/databricks-iceberg-polaris/starpump_to_databricks.py` |
