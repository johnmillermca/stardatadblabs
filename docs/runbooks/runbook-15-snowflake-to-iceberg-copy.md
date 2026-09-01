# Runbook 15 — Snowflake → Spark Iceberg Copy Pipeline

| Field | Value |
|---|---|
| **Runbook ID** | RB-15 |
| **Service** | k8s-platform / data-lakehouse |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2025 |
| **Related** | RB-11 (Kerberos), RB-13 (RBAC), [Iceberg Lakehouse README](../../rbac-plane/docs/iceberg-spark-setup/README.md) |

---

## 1. Purpose

This runbook documents the end-to-end pipeline that copies all 24 tables from
`SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL` (Snowflake) into Apache Iceberg tables
managed by the Polaris REST catalog, stored on AWS S3, and queryable from both
Apache Doris and JupyterHub.

```
Snowflake                     Spark (k8s)                  Polaris + S3
─────────────────────         ──────────────────────────   ─────────────────────
SNOWFLAKE_SAMPLE_DATA  ──►   starpump snowflake         ──► s3://xdatatoiceberg1/
  TPCDS_SF10TCL                8 threads (default)           iceberg/tpcds_sf10tcl/
  24 tables                    --threads 16/32 to scale       *.parquet (snappy)
                               100 000 row batches            hourly + 4 hash parts
                                                              snap_timestamp / snap_id
```

---

## 2. Architecture

### 2.1 Components

| Component | Role |
|---|---|
| `bao_spark_init.py` | Reads ALL credentials from OpenBao at runtime; builds `SparkConf`; exposes `catalog_credential()` |
| `spark_iceberg_utils.py` | Global `IcebergTableBuilder` — injects `snap_timestamp` + `snap_id` into every table |
| `spark-defaults-configmap.yaml` | K8s ConfigMap delivering `spark-defaults.conf` to spark pods |
| `starpump.py` | `starpump` entry point — catalog pre-flight guard, dynamic source routing, N-thread copy with resume |

### 2.2 Security model

- **No credentials hard-coded** — all secrets live in OpenBao paths:

  | Path | Keys |
  |---|---|
  | `secret/platform/snowflake` | `account`, `user`, `password`, `warehouse` |
  | `secret/platform/s3` | `access_key`, `secret_key`, `region`, `endpoint`, `bucket` |
  | `secret/platform/polaris` | `spark_svc_id`, `spark_svc_secret` |

- OpenBao auth: K8s SA JWT (role `platform-secrets-read`) → in-cluster pods;
  `TOKEN` env-var for local / bootstrap use only.
- **RBAC**: only users `bob` and `dave` (`can_admin_catalog=true`,
  `can_write_iceberg=true`) may run the copy job.
- **Catalog pre-flight**: starpump verifies that the target `ICEBERG_CATALOG` (default: `polaris`)
  has a Polaris OAuth2 credential registered in `BaoSparkInit.spark_conf()` before opening a Spark
  session. The same service-account (`spark_svc_id`) that created the external catalog entry is the
  one used for data copy writes. If the catalog is not wired, starpump exits before allocating any
  cluster resources.

### 2.3 Iceberg table layout

| Property | Value |
|---|---|
| Catalog | `polaris` (Polaris REST) |
| Namespace | `tpcds_sf10tcl` |
| Format | Parquet + Snappy |
| Format version | Iceberg v2 |
| Target file size | 2 621 440 bytes (≈ 2.5 MB / 2:56 MiB) |
| Partitioning | `hours(<ts_col>)`, `bucket(4, <sk_col>)` |
| Audit columns | `snap_timestamp TIMESTAMP`, `snap_id BIGINT` (auto-injected) |
| S3 prefix | `s3://xdatatoiceberg1/iceberg/tpcds_sf10tcl/<table>/` |

---

## 3. Pre-requisites

### 3.1 Kubernetes access

```bash
kubectl config use-context prod        # or whichever context targets prod ns
kubectl get pods -n prod | grep spark  # confirm spark-master + spark-worker running
```

### 3.2 Verify OpenBao secrets exist

```bash
# Get root token (admin bootstrap only — never hard-code)
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
        -o jsonpath='{.data.root-token}' | base64 -d)

ADDR=http://192.168.1.50:30820

# Check all three secret paths
curl -s -H "X-Vault-Token: $TOKEN" \
  $ADDR/v1/secret/platform/snowflake | python3 -m json.tool | grep -E '"user"|"account"'

curl -s -H "X-Vault-Token: $TOKEN" \
  $ADDR/v1/secret/platform/s3 | python3 -m json.tool | grep '"bucket"'

curl -s -H "X-Vault-Token: $TOKEN" \
  $ADDR/v1/secret/platform/polaris | python3 -m json.tool | grep '"spark_svc_id"'
```

Expected output contains `"account"`, `"bucket"`, and `"spark_svc_id"` keys.

### 3.3 Verify Iceberg JAR

```bash
kubectl exec -n prod deploy/spark-master -- ls /opt/spark/jars/ | grep iceberg
# Expected: iceberg-spark-runtime-3.5_2.12-1.9.2.jar
```

If missing, copy it:

```bash
kubectl cp jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar \
  prod/$(kubectl get pod -n prod -l app=spark-master -o jsonpath='{.items[0].metadata.name}'):/opt/spark/jars/
```

### 3.4 Polaris namespace

```bash
# Confirm namespace exists in Polaris
kubectl exec -n prod deploy/spark-master -- curl -s \
  http://polaris-rest.prod.svc.cluster.local:8181/api/catalog/v1/IcebergCatalog/namespaces \
  -H "Authorization: Bearer <polaris_token>" | python3 -m json.tool
```

If `tpcds_sf10tcl` is missing, create it via JupyterHub (Step 5.2).

---

## 4. Initial Setup

### 4.1 Apply spark-defaults ConfigMap

```bash
# From repo root
kubectl apply -f docs/runbooks/snowflake-to-iceberg/spark-defaults-configmap.yaml -n prod

# Verify ConfigMap
kubectl get cm spark-defaults-conf -n prod
kubectl describe cm spark-defaults-conf -n prod | head -50
```

### 4.2 Patch spark deployments to mount the ConfigMap

The YAML file already contains both Deployment patches. Apply with:

```bash
# Patch spark-master
kubectl patch deployment spark-master -n prod \
  --type=strategic \
  --patch "$(cat <<'EOF'
spec:
  template:
    spec:
      volumes:
        - name: spark-defaults-conf
          configMap:
            name: spark-defaults-conf
      containers:
        - name: spark-master
          volumeMounts:
            - name: spark-defaults-conf
              mountPath: /opt/spark/conf/spark-defaults.conf
              subPath: spark-defaults.conf
EOF
)"

# Patch spark-worker
kubectl patch deployment spark-worker -n prod \
  --type=strategic \
  --patch "$(cat <<'EOF'
spec:
  template:
    spec:
      volumes:
        - name: spark-defaults-conf
          configMap:
            name: spark-defaults-conf
      containers:
        - name: spark-worker
          volumeMounts:
            - name: spark-defaults-conf
              mountPath: /opt/spark/conf/spark-defaults.conf
              subPath: spark-defaults.conf
EOF
)"

# Restart pods to pick up the mount
kubectl rollout restart deployment spark-master spark-worker -n prod
kubectl rollout status  deployment spark-master spark-worker -n prod
```

### 4.3 Verify spark-defaults.conf inside pod

```bash
kubectl exec -n prod deploy/spark-master -- \
  cat /opt/spark/conf/spark-defaults.conf | grep sql.catalog
```

Expected output includes `spark.sql.catalog.polaris` and
`spark.sql.catalog.snowflake`.

### 4.4 Verify catalog pre-flight passes

On a valid setup the `[catalog-check]` line appears before any table discovery:

```
[catalog-check] 'polaris' is registered (svc_id=<spark_svc_id>). Proceeding.
```

If the catalog is missing from `spark_conf()` starpump exits before opening a Spark session:

```
ERROR: No Spark external catalog registered for 'polaris'.
  starpump requires a Spark external catalog to be wired in BaoSparkInit.spark_conf()
  before data can be copied.
  Registered catalogs in spark_conf: ['databricks', 'polaris']
  Add a 'spark.sql.catalog.polaris.*' block to BaoSparkInit.spark_conf()
  before running starpump against this target.
```

---

## 5. Running the Copy Job

### 5.1 Locate the spark-master pod

```bash
# Scripts are baked into the image — no copy needed.
SPARK_POD=$(kubectl get pod -n prod -l component=master \
            -o jsonpath='{.items[0].metadata.name}')

TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
        -o jsonpath='{.data.root-token}' | base64 -d)

ADDR="http://openbao.prod.svc.cluster.local:8200"
```

### 5.2 Create Polaris namespace (first run only)

```python
# Run inside JupyterHub or spark-master as bob/dave

from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession

init = BaoSparkInit()
spark = SparkSession.builder.config(conf=init.spark_conf("ns-bootstrap")).getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS `polaris`.`tpcds_sf10tcl`")
spark.sql("SHOW NAMESPACES IN polaris").show()
spark.stop()
```

### 5.3 Full copy (all 24 tables, default 8 threads)

```bash
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env USER=bob TOKEN="$TOKEN" ADDR="$ADDR" \
  starpump snowflake
```

### 5.4 Partial / targeted copy

```bash
# Copy only customer and item tables
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env USER=bob TOKEN="$TOKEN" ADDR="$ADDR" \
      INCLUDE_TABLES=customer,item MAX_TABLE_SIZE_GB=0 \
  starpump snowflake
```

### 5.5 Dry-run (create tables, no data)

```bash
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env USER=bob TOKEN="$TOKEN" ADDR="$ADDR" \
      DRY_RUN=1 \
  starpump snowflake
```

### 5.6 Scale up parallel threads

```bash
# 16 threads
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env USER=bob TOKEN="$TOKEN" ADDR="$ADDR" \
  starpump snowflake --threads 16

# 32 threads
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env USER=bob TOKEN="$TOKEN" ADDR="$ADDR" \
  starpump snowflake --threads 32
```

### 5.7 Copy from a different database / schema

```bash
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env USER=bob TOKEN="$TOKEN" ADDR="$ADDR" \
      DATABASE=MY_DB SCHEMAS=MY_SCHEMA \
  starpump snowflake
```

---

## 6. Verification

### 6.1 Confirm tables in Polaris

```bash
kubectl exec -n prod $SPARK_POD -- bash -c "
  python3 -c \"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
init = BaoSparkInit()
spark = SparkSession.builder.config(conf=init.spark_conf('verify')).getOrCreate()
spark.sql('SHOW TABLES IN \`polaris\`.\`tpcds_sf10tcl\`').show(30)
spark.stop()
\"
"
```

Expected: 24 rows, one per TPC-DS table.

### 6.2 Confirm snap audit columns

```bash
kubectl exec -n prod $SPARK_POD -- bash -c "
  python3 -c \"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
init = BaoSparkInit()
spark = SparkSession.builder.config(conf=init.spark_conf('verify')).getOrCreate()
spark.sql('DESCRIBE TABLE \`polaris\`.\`tpcds_sf10tcl\`.\`customer\`').filter(
    'col_name LIKE \"%snap%\"'
).show()
spark.stop()
\"
"
```

Expected output:

```
+--------------+---------+-------+
|      col_name|data_type|comment|
+--------------+---------+-------+
|snap_timestamp|timestamp|   null|
|       snap_id|   bigint|   null|
+--------------+---------+-------+
```

### 6.3 Confirm Parquet + partition layout

```bash
# Check S3 objects
kubectl exec -n prod $SPARK_POD -- bash -c "
  python3 -c \"
import boto3, json
from bao_spark_init import BaoSparkInit
init = BaoSparkInit()
s3 = init.s3_creds()
client = boto3.client('s3',
    aws_access_key_id=s3['access_key'],
    aws_secret_access_key=s3['secret_key'],
    region_name=s3['region'])
resp = client.list_objects_v2(Bucket='xdatatoiceberg1',
    Prefix='iceberg/tpcds_sf10tcl/customer/', MaxKeys=5)
for o in resp.get('Contents', []):
    print(o['Key'], o['Size'])
\"
"
```

### 6.4 Query from Doris

```sql
-- Via Doris (iceberg_polaris catalog)
SELECT snap_timestamp, snap_id, c_customer_id
FROM   iceberg_polaris.tpcds_sf10tcl.customer
LIMIT  5;
```

### 6.5 Query row counts

```sql
-- In spark-master / JupyterHub
SELECT COUNT(*) FROM `polaris`.`tpcds_sf10tcl`.`store_sales`;
-- Expected: ~2 800 000 (sample load) up to 28.8B for full copy
```

---

## 7. Scheduling the Copy Job (Kubernetes CronJob)

For continuous / nightly refresh, deploy a CronJob:

```yaml
# snowflake-to-iceberg-cronjob.yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: snowflake-to-iceberg-copy
  namespace: prod
spec:
  schedule: "0 2 * * *"       # 02:00 UTC daily
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      template:
        spec:
          serviceAccountName: polaris-token-refresher   # has OpenBao K8s auth
          restartPolicy: OnFailure
          containers:
            - name: copy-job
              image: 192.168.1.50:30500/spark-gluten-velox:3.5.1
              command: ["starpump", "snowflake"]
              env:
                - name: USER
                  value: "bob"
                - name: ADDR
                  value: "http://openbao.prod.svc.cluster.local:8200"
              volumeMounts:
                - name: scripts
                  mountPath: /scripts
                - name: spark-defaults-conf
                  mountPath: /opt/spark/conf/spark-defaults.conf
                  subPath: spark-defaults.conf
          volumes:
            - name: scripts
              configMap:
                name: snowflake-to-iceberg-scripts
            - name: spark-defaults-conf
              configMap:
                name: spark-defaults-conf
```

> **Note:** Package the Python scripts into a separate ConfigMap
> `snowflake-to-iceberg-scripts` or bake them into a custom image.

---

## 8. Enabling and Disabling the Copy CronJob

```bash
# Disable (suspend) — stops new runs, current run continues
kubectl patch cronjob snowflake-to-iceberg-copy -n prod \
  -p '{"spec":{"suspend":true}}'

# Re-enable
kubectl patch cronjob snowflake-to-iceberg-copy -n prod \
  -p '{"spec":{"suspend":false}}'

# Check status
kubectl get cronjob snowflake-to-iceberg-copy -n prod
```

---

## 9. Snowflake Refresh Task (Iceberg OBJECT_STORE)

These commands manage the Snowflake-side scheduled refresh of the Iceberg
table that was created in runbook step (d).

```sql
-- Connect to Snowflake (Snowflake UI or SnowSQL)
USE ROLE ACCOUNTADMIN;
USE DATABASE SNOWFLAKE_SAMPLE_DATA;
USE SCHEMA TPCDS_SF10TCL;

-- (e) Enable hourly refresh task
ALTER TASK IF EXISTS refresh_iceberg_tpcds_hourly RESUME;

-- Disable (suspend) refresh task
ALTER TASK IF EXISTS refresh_iceberg_tpcds_hourly SUSPEND;

-- Check status
SHOW TASKS LIKE 'refresh_iceberg_tpcds_hourly';
-- Look at "state" column: started = active, suspended = paused

-- Manually trigger one refresh immediately
EXECUTE TASK refresh_iceberg_tpcds_hourly;

-- View recent task history
SELECT *
FROM   TABLE(information_schema.task_history(
         task_name   => 'refresh_iceberg_tpcds_hourly',
         result_limit => 20))
ORDER  BY scheduled_time DESC;
```

---

## 10. Doris Iceberg Catalog (Read + Warm-up)

### 10.1 Verify read catalog

```sql
-- In Doris (port 9030)
SHOW CATALOGS;
-- Expect: iceberg_polaris, iceberg_polaris_rw, internal

USE iceberg_polaris;
SHOW DATABASES;
-- Expect: tpcds_sf10tcl

USE tpcds_sf10tcl;
SHOW TABLES;
-- Expect: 24 TPC-DS tables
```

### 10.2 1-minute warm-up CronJob

The `doris-iceberg-warmup` CronJob (already deployed) runs every minute and
queries `SELECT COUNT(*) FROM iceberg_polaris.tpcds_sf10tcl.customer` to keep
metadata caches warm.

```bash
# Check warm-up CronJob
kubectl get cronjob doris-iceberg-warmup -n prod
kubectl get jobs -n prod --selector=app=doris-iceberg-warmup \
  --sort-by=.metadata.creationTimestamp | tail -5

# Enable
kubectl patch cronjob doris-iceberg-warmup -n prod \
  -p '{"spec":{"suspend":false}}'

# Disable
kubectl patch cronjob doris-iceberg-warmup -n prod \
  -p '{"spec":{"suspend":true}}'
```

---

## 11. RBAC Reference

RBAC is enforced via the `spark-rbac-allowlist` ConfigMap in the `prod` namespace.

```bash
kubectl get cm spark-rbac-allowlist -n prod -o yaml
```

| User | can_admin_catalog | can_submit | can_write_iceberg |
|---|---|---|---|
| `bob` | ✅ | ✅ | ✅ |
| `dave` | ✅ | ✅ | ✅ |
| `carol` | ❌ | ✅ | ✅ |
| `iceberg-engineer` | ❌ | ✅ | ✅ |
| `alice` | ❌ | ❌ | ❌ |

Only `bob` and `dave` may run `starpump`. Pass `USER=<name>` to identify the
running user; the pipeline reads RBAC from the `spark-rbac-allowlist` ConfigMap.

To add a new user:

```bash
kubectl edit cm spark-rbac-allowlist -n prod
# Add new_user: can_admin_catalog=true, can_submit=true, can_write_iceberg=true
```

---

## 12. Troubleshooting

### T1 — `ValueError: No Spark external catalog registered for '...'`

`ICEBERG_CATALOG` is set to a catalog name that has no `spark.sql.catalog.<name>.credential`
block in `BaoSparkInit.spark_conf()`.

```bash
# Verify which catalogs are currently wired
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env USER=bob TOKEN="$TOKEN" ADDR="$ADDR" \
  python3 -c "
from bao_spark_init import BaoSparkInit
from pyspark import SparkConf
bao  = BaoSparkInit()
conf = bao.spark_conf()
cats = sorted(set(
    k.split('.')[3] for k, _ in conf.getAll()
    if k.startswith('spark.sql.catalog.') and len(k.split('.')) == 4
))
print('Registered catalogs:', cats)
"
```

**Fix:** Either use `ICEBERG_CATALOG=polaris` (the default) or add a new catalog block to
`docker/spark-gluten-velox/scripts/bao_spark_init.py` and rebuild the image.

---

### T2 — `RuntimeError: Spark catalog 'snowflake' not found`

The `spark-defaults.conf` ConfigMap is not mounted or the pods have not
restarted since the ConfigMap was applied.

```bash
kubectl describe cm spark-defaults-conf -n prod | grep snowflake
kubectl rollout restart deployment spark-master spark-worker -n prod
kubectl exec -n prod deploy/spark-master -- \
  grep snowflake /opt/spark/conf/spark-defaults.conf
```

### T3 — `PermissionError: RBAC check failed: user ''`

`USER` env-var not set.

```bash
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env USER=bob TOKEN="$TOKEN" ADDR="$ADDR" \
  starpump snowflake
```

### T4 — Snowflake connector `ClassNotFoundException`

The `net.snowflake:spark-snowflake_2.12:2.15.0-spark_3.5` JAR has not been
downloaded. The first run fetches it from Maven Central; ensure internet access
from spark-master:

```bash
kubectl exec -n prod deploy/spark-master -- \
  curl -so /dev/null -w "%{http_code}" https://repo1.maven.org/maven2/
# Expected: 200
```

### T5 — OpenBao `Cannot authenticate` error

```bash
# Check if K8s auth is enabled
curl -s http://192.168.1.50:30820/v1/sys/auth | python3 -m json.tool | grep kubernetes

# Use root token override
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
        -o jsonpath='{.data.root-token}' | base64 -d)
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env USER=dave TOKEN="$TOKEN" ADDR="http://openbao.prod.svc.cluster.local:8200" \
  starpump snowflake
```

### T6 — `NoSuchTableException` on Polaris

The namespace `tpcds_sf10tcl` doesn't exist yet. Run Step 5.2.

### T7 — S3 `AccessDenied`

Verify the HMAC credentials are stored correctly in OpenBao:

```bash
curl -s -H "X-Vault-Token: $TOKEN" \
  http://192.168.1.50:30820/v1/secret/platform/s3 | python3 -m json.tool
```

Confirm the bucket `xdatatoiceberg1` exists in region `us-east-2`.

### T8 — Iceberg table missing snap audit columns

If a table was created outside of `IcebergTableBuilder`, the snap columns may
be absent. Fix by adding them:

```sql
ALTER TABLE `polaris`.`tpcds_sf10tcl`.`<table>`
  ADD COLUMN snap_timestamp TIMESTAMP;
ALTER TABLE `polaris`.`tpcds_sf10tcl`.`<table>`
  ADD COLUMN snap_id BIGINT;
```

---

## 13. File Reference

| File | Purpose |
|---|---|
| `docs/runbooks/snowflake-to-iceberg/spark_iceberg_utils.py` | Global Iceberg table builder — auto-injects snap columns |
| `docs/runbooks/snowflake-to-iceberg/bao_spark_init.py` | OpenBao credential loader + SparkConf builder |
| `docs/runbooks/snowflake-to-iceberg/spark-defaults-configmap.yaml` | K8s ConfigMap for `spark-defaults.conf` + Deployment patches |
| `docs/runbooks/snowflake-to-iceberg/starpump.py` | `starpump` entry point — dynamic source routing, N-thread copy |
| `rbac-plane/docs/iceberg-spark-setup/README.md` | Iceberg lakehouse runbook (initial setup) |
| `rbac-plane/docs/iceberg-spark-setup/01_create_iceberg_table.py` | Initial Iceberg table creation script |
| `rbac-plane/docs/iceberg-spark-setup/06_snowflake_iceberg_setup_and_refresh.sql` | Snowflake OBJECT_STORE table + task SQL |
| `rbac-plane/docs/iceberg-spark-setup/05_doris_catalogs.sql` | Doris catalog DDL |
| `rbac-plane/docs/iceberg-spark-setup/05b_doris_warmup_cronjob.yaml` | Doris warm-up CronJob manifest |
| `rbac-plane/docs/iceberg-spark-setup/07_polaris_token_refresh_cronjob.yaml` | Polaris token refresh CronJob |
| `rbac-plane/migrations/005_iceberg_polaris_doris_snowflake.sql` | RBAC migration for Iceberg/Doris/Snowflake permissions |

---

## 14. Quick-reference Commands

```bash
# Deploy ConfigMap
kubectl apply -f docs/runbooks/snowflake-to-iceberg/spark-defaults-configmap.yaml -n prod

# Restart Spark pods
kubectl rollout restart deployment spark-master spark-worker -n prod

# Run full copy (as bob, default 8 threads)
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env USER=bob TOKEN="$TOKEN" ADDR="$ADDR" \
  starpump snowflake

# Run full copy with 16 threads
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env USER=bob TOKEN="$TOKEN" ADDR="$ADDR" \
  starpump snowflake --threads 16

# Run dry-run
kubectl exec -n prod $SPARK_POD -c spark-master -- \
  env USER=bob TOKEN="$TOKEN" ADDR="$ADDR" DRY_RUN=1 \
  starpump snowflake

# Check Iceberg tables in Polaris (in Spark session)
spark.sql("SHOW TABLES IN `polaris`.`tpcds_sf10tcl`").show(30)

# Suspend Snowflake refresh task
ALTER TASK refresh_iceberg_tpcds_hourly SUSPEND;

# Resume Snowflake refresh task
ALTER TASK refresh_iceberg_tpcds_hourly RESUME;

# Suspend Doris warm-up CronJob
kubectl patch cronjob doris-iceberg-warmup -n prod -p '{"spec":{"suspend":true}}'

# Resume Doris warm-up CronJob
kubectl patch cronjob doris-iceberg-warmup -n prod -p '{"spec":{"suspend":false}}'
```
