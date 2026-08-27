# Runbook 18 — Spark → Iceberg → Databricks Pipeline (HMS Federation)

| Field | Value |
|---|---|
| **Runbook ID** | RB-18 |
| **Service** | k8s-platform / data-lakehouse |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-08-27 |
| **Related** | RB-19 (day-to-day write/query guide), RB-15 (Snowflake→Iceberg), RB-02 (ArgoCD), RB-01 (OpenBao) |

---

## 1. Purpose

Infrastructure runbook for the Spark → Iceberg → Databricks pipeline. Covers:

1. One-time setup — S3, Polaris catalog, Hive Metastore, Databricks Unity Catalog resources
2. Full test matrix confirming the pipeline is live
3. Architecture reference

> **Day-to-day usage (INSERT / UPDATE / DELETE in Spark → see in Databricks):** see [RB-19](runbook-19-iceberg-spark-write-databricks-read.md).

```
Spark SQL (k8s)               Apache Polaris              AWS S3
─────────────────────         ───────────────────────     ──────────────────────
INSERT / UPDATE / DELETE  ──► star_lakehouse catalog  ──► stardata-databricks/
star_lakehouse.demo.*         demo.customers table         iceberg/warehouse/
                                                           demo/customers/
        │
        │  HMS re-register after each write
        ▼
Hive Metastore 2.3.9 (k8s)              Databricks Unity Catalog
hive-metastore.prod.svc:9083 ◄── FOREIGN catalog ──► star_lakehouse (FOREIGN_CATALOG)
PostgreSQL hive_metastore DB     hms_star_lakehouse       demo.customers
```

---

## 2. Architecture

### 2.1 Components

| Component | Role |
|---|---|
| Apache Polaris REST catalog | Manages Iceberg metadata; owns the snapshot chain |
| Hive Metastore 2.3.9 (k8s) | Holds `EXTERNAL_TABLE` entries; bridges Databricks FOREIGN catalog |
| AWS S3 `stardata-databricks` | Iceberg warehouse storage (parquet + metadata JSON) |
| Databricks Unity Catalog | Federated catalog via `HIVE_METASTORE` FOREIGN connection |
| OpenBao | All credentials — no secrets in code or YAML |

### 2.2 Credentials map (OpenBao)

| OpenBao path | Keys | Used by |
|---|---|---|
| `secret/databricks/pat` | `token`, `workspace` | Databricks SQL API |
| `secret/platform/polaris` | `spark_svc_id`, `spark_svc_secret` | Spark → Polaris OAuth |
| `secret/platform/s3` | `access_key`, `secret_key`, `region` | Spark S3A + HMS registration |
| `secret/hive/credentials` | `db_password`, `aws_access_key`, `aws_secret_key` | HMS pod init |

### 2.3 IAM setup

**Spark S3 access (write path):**
IAM user `arn:aws:iam::586643076710:user/watsonx-s3-connector`
Policy `stardata-databricks-rw` — `s3:Get/Put/Delete/List` on `arn:aws:s3:::stardata-databricks/*`

**Databricks S3 access (read path via Unity Catalog):**
IAM role `arn:aws:iam::586643076710:role/databricks-unity-catalog`
Trust: `arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL` · external ID `578f6c36-b518-414d-a6fc-8a318b9d580b`
Same `stardata-databricks-rw` policy attached to the role.

---

## 3. Pre-flight

Run once per terminal session:

```bash
VAULT_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

DB_TOKEN=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=token secret/databricks/pat")
DB_WS="https://dbc-11a1dbc5-061a.cloud.databricks.com"
WAREHOUSE_ID="942026cf5e55f3c3"

AWS_KEY=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=access_key secret/platform/s3")
AWS_SECRET=$(kubectl exec -n prod openbao-0 -- \
  sh -c "VAULT_TOKEN=$VAULT_TOKEN vault kv get -field=secret_key secret/platform/s3")

SPARK_POD=$(kubectl get pods -n prod -l app=spark,component=master \
  --no-headers -o custom-columns=NAME:.metadata.name | head -1)

echo "Pod: $SPARK_POD  ready"
```

---

## 4. One-time setup (already applied — for reference)

### 4.1 S3 bucket

- **Bucket:** `stardata-databricks` · **Region:** `us-east-2`
- **Public access:** blocked
- **IAM policy:** `stardata-databricks-rw` on `watsonx-s3-connector` user and `databricks-unity-catalog` role

### 4.2 Polaris catalog

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
      "properties": {"default-base-location": "s3://stardata-databricks/iceberg/warehouse"},
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

### 4.3 Databricks Unity Catalog resources

All resources are live — do not re-create unless lost.

| Resource | Name | Detail |
|---|---|---|
| Storage credential | `stardata_databricks_s3` | IAM role `arn:aws:iam::586643076710:role/databricks-unity-catalog` |
| External location | `stardata_databricks_iceberg` | `s3://stardata-databricks/` |
| HMS connection | `hms_star_lakehouse` | `HIVE_METASTORE`, `ACTIVE`, host `192.168.1.50:30983` |
| FOREIGN catalog | `star_lakehouse` | `FOREIGN_CATALOG`, connection `hms_star_lakehouse` |

Re-create HMS connection if needed:
```bash
curl -s -X POST "$DB_WS/api/2.1/unity-catalog/connections" \
  -H "Authorization: Bearer $DB_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"hms_star_lakehouse","connection_type":"HIVE_METASTORE","options":{"host":"192.168.1.50","port":"30983"}}' \
  | python3 -m json.tool
```

Re-create FOREIGN catalog if needed:
```bash
curl -s -X POST "$DB_WS/api/2.1/unity-catalog/catalogs" \
  -H "Authorization: Bearer $DB_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"star_lakehouse","connection_name":"hms_star_lakehouse","options":{}}' \
  | python3 -m json.tool
```

---

## 5. Verification steps

### Step 1 — Verify S3 access

```bash
python3 - "$AWS_KEY" "$AWS_SECRET" <<'EOF'
import sys, boto3
s3 = boto3.client('s3', region_name='us-east-2',
    aws_access_key_id=sys.argv[1], aws_secret_access_key=sys.argv[2])
r = s3.list_objects_v2(Bucket='stardata-databricks', MaxKeys=5)
print(f"✅ Bucket accessible, objects: {r['KeyCount']}")
EOF
```

### Step 2 — Verify Polaris catalog

```bash
curl -s http://192.168.1.50:30181/api/management/v1/catalogs/star_lakehouse \
  -H "Authorization: Bearer $POLARIS_TOKEN" | python3 -m json.tool
```

✅ `"name": "star_lakehouse"`, `"default-base-location": "s3://stardata-databricks/iceberg/warehouse"`

### Step 3 — Verify HMS is running

```bash
kubectl get pods -n prod -l app=hive-metastore
```

✅ `1/1 Running`

### Step 4 — Verify FOREIGN catalog in Databricks SQL

```bash
curl -s -X POST "$DB_WS/api/2.0/sql/statements" \
  -H "Authorization: Bearer $DB_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"warehouse_id\":\"$WAREHOUSE_ID\",\"statement\":\"SHOW TABLES IN star_lakehouse.demo\",\"wait_timeout\":\"60s\"}" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status',{}).get('state')); [print(r) for r in d.get('result',{}).get('data_array',[])]"
```

✅ `customers` listed

---

## 6. Test matrix

| ID | Phase | Test | Expected | Status |
|---|---|---|---|---|
| T-01 | S3 | Bucket accessible by `watsonx-s3-connector` | `✅ Bucket accessible` | ✅ |
| T-02 | Polaris | `star_lakehouse` catalog exists | `200 OK`, correct `default-base-location` | ✅ |
| T-03 | Polaris | `star_lakehouse_admin` principal role exists | Listed in `/api/management/v1/principal-roles` | ✅ |
| T-04 | Polaris | `databricks-connector` principal assigned role | `/api/management/v1/principals/databricks-connector` | ✅ |
| T-05 | Spark | Iceberg `demo.customers` table exists with 10 000 rows | `SELECT COUNT(*) = 10000` via Spark SQL | ✅ |
| T-06 | S3 | Iceberg `metadata/` and `data/` on S3 | Objects under `iceberg/warehouse/demo/customers/` | ✅ |
| T-07 | Polaris API | `demo.customers` table listed | `tables` array includes `customers` | ✅ |
| T-08 | Databricks | HMS connection registered in Unity Catalog | `"name": "hms_star_lakehouse"`, `ACTIVE` | ✅ |
| T-09 | Databricks | `star_lakehouse` FOREIGN catalog in Unity Catalog | `catalog_type: FOREIGN_CATALOG` | ✅ |
| T-10 | HMS | `demo.customers` registered in HMS | `metadata_location` set to latest `.metadata.json` | ✅ |
| T-11 | Databricks SQL | `SHOW TABLES IN star_lakehouse.demo` lists `customers` | `customers` present | ✅ |
| T-12 | Databricks SQL | `SELECT COUNT(*)` via FOREIGN catalog | `total_rows=10000` | ✅ |

> **HMS federation note:** `ICEBERG_REST` connection type is not provisioned on workspace `dbc-11a1dbc5-061a`. Live path uses `HIVE_METASTORE` FOREIGN connection (`hms_star_lakehouse`).

---

## 7. Troubleshooting

### Spark SQL returns `CATALOG_NOT_FOUND: star_lakehouse`
The Polaris catalog config is not wired in the Spark session. Use the `spark-sql` shell from RB-19 Section 3 which sets up the catalog automatically.

### Databricks SQL returns `CATALOG_NOT_FOUND: star_lakehouse`
FOREIGN catalog missing — re-create via Section 4.3.

### Databricks returns stale row count after a write
HMS `metadata_location` not updated. Re-run the HMS registration from RB-19 Section 4.

### Databricks PAT expired
```bash
kubectl exec -n prod openbao-0 -- sh -c \
  "VAULT_TOKEN=$VAULT_TOKEN vault kv patch secret/databricks/pat token='dapi...' expires='YYYY-MM-DD'"
```
Generate new token at: `https://dbc-11a1dbc5-061a.cloud.databricks.com/settings/user/developer/access-tokens`

### HMS pod not ready
```bash
kubectl get pods -n prod -l app=hive-metastore
kubectl logs -n prod -l app=hive-metastore --tail=30
```

### `stsUnavailable` reset after Polaris restart
```bash
PG_POD=$(kubectl get pods -n prod -l app=postgresql --no-headers -o custom-columns=NAME:.metadata.name | head -1)
kubectl exec -n prod $PG_POD -- env PGPASSWORD="postgres" psql -U postgres -d polaris -c "
UPDATE polaris_schema.entities
SET internal_properties = jsonb_set(internal_properties,'{storage_configuration_info}',
  to_jsonb('{\"@type\":\"AwsStorageConfigurationInfo\",\"allowedLocations\":[\"s3://stardata-databricks/iceberg/warehouse\"],\"roleARN\":\"arn:aws:iam::586643076710:user/watsonx-s3-connector\",\"allowedKmsKeys\":[],\"externalId\":\"polaris-iceberg\",\"region\":\"us-east-2\",\"stsUnavailable\":true,\"storageType\":\"S3\"}'::text))
WHERE name = 'star_lakehouse';"
kubectl rollout restart deployment/polaris -n prod
```

---

## 8. Key paths reference

| Resource | Path / URL |
|---|---|
| S3 bucket | `s3://stardata-databricks/iceberg/warehouse/` |
| Polaris catalog API | `http://192.168.1.50:30181/api/catalog/v1/star_lakehouse/` |
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

---

## 9. Architecture — full data flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  Spark SQL (k8s — prod namespace)                                            │
│                                                                              │
│  spark-sql> INSERT INTO star_lakehouse.demo.customers VALUES (...)           │
│  spark-sql> UPDATE star_lakehouse.demo.customers SET ... WHERE ...           │
│  spark-sql> DELETE FROM star_lakehouse.demo.customers WHERE ...              │
└──────────────┬───────────────────────────────────────────────────────────────┘
               │  Iceberg REST API (OAuth2)
               │  http://polaris-rest.prod.svc.cluster.local:8181/api/catalog
               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  Apache Polaris 1.6.0  (k8s, prod namespace)                                 │
│  Catalog: star_lakehouse  Namespace: demo                                    │
│  • Owns Iceberg snapshot chain                                               │
│  • Authorises writes via star_lakehouse_admin principal role                 │
└──────────────┬───────────────────────────────────────────────────────────────┘
               │  writes parquet + metadata JSON
               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  AWS S3 — s3://stardata-databricks/iceberg/warehouse/demo/<table>/           │
│    metadata/  ← .metadata.json (new file per write)                         │
│    data/      ← snappy parquet files                                        │
└──────────────┬───────────────────────────────────────────────────────────────┘
               │  HMS re-register: update metadata_location pointer
               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  Hive Metastore 2.3.9  (hive-metastore.prod.svc.cluster.local:9083)          │
│  EXTERNAL_TABLE: table_type=ICEBERG, metadata_location=s3://...latest.json  │
└──────────────┬───────────────────────────────────────────────────────────────┘
               │  HIVE_METASTORE FOREIGN catalog — hms_star_lakehouse
               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  Databricks Unity Catalog — star_lakehouse (FOREIGN_CATALOG)                 │
│  SELECT * FROM star_lakehouse.demo.customers                                 │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Key rules

| Rule | Detail |
|---|---|
| **Always write via Spark SQL** | `INSERT / UPDATE / DELETE` against `star_lakehouse.demo.*` — never write directly to S3 |
| **Polaris owns the snapshot chain** | HMS is a read-only pointer; it only stores `metadata_location` |
| **Re-register in HMS after every write** | Without this Databricks sees the old snapshot — see RB-19 Section 4 |
| **HMS re-registration is cheap** | Updates one row in PostgreSQL — no data movement |
| **Two separate IAM credentials** | Spark writes via `watsonx-s3-connector` user; Databricks reads via `databricks-unity-catalog` IAM role |
