# Iceberg Lakehouse — Operations Runbook

**Stack:** Spark 3.5.1 · Apache Polaris 1.6.0 · Apache Doris 4.0.7 · Snowflake · OpenBao · Kubernetes (`prod` namespace)

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Endpoints & Credentials](#endpoints--credentials)
3. [H — RBAC Plane](#h--rbac-plane)
4. [A — Spark Iceberg Table](#a--spark-iceberg-table)
5. [B — Apache Polaris REST Catalog](#b--apache-polaris-rest-catalog)
6. [C — Loading Data via JupyterHub / Spark](#c--loading-data-via-jupyterhub--spark)
7. [D/E — Snowflake Iceberg Table & Refresh Jobs](#de--snowflake-iceberg-table--refresh-jobs)
8. [F — Doris Iceberg Catalog & 1-Minute Warm-up](#f--doris-iceberg-catalog--1-minute-warm-up)
9. [G — Doris Snowflake JDBC Catalog](#g--doris-snowflake-jdbc-catalog)
10. [Doris Write to Iceberg](#doris-write-to-iceberg)
11. [Token Refresh CronJob (OpenBao-aware)](#token-refresh-cronjob-openbao-aware)
12. [OpenBao Secret Management](#openbao-secret-management)
13. [Troubleshooting](#troubleshooting)
14. [Key Files](#key-files)

---

## Architecture Overview

```
  JupyterHub Notebook          Doris (iceberg_polaris)        Snowflake
  (Spark 3.5.1 session)        (read catalog, port 8282)      LAKEHOUSE_DB.EVENTS.events
         │                              │                              │
         │ spark.sql.catalog.polaris    │ polaris-auth-proxy:8282      │ EXTERNAL VOLUME
         │ credential=35fc704f…        │ (Bearer token injected)      │ iceberg_s3_vol
         ▼                             ▼                              │
  ┌──────────────────────────────────────────────────────────┐       │ s3://xdatatoiceberg1
  │           Apache Polaris 1.6.0  (REST Catalog)           │       │   /warehouse/
  │           polaris-rest.prod.svc.cluster.local:8181       │       │   lakehouse/events/
  │           Catalog: spark_lakehouse  NS: lakehouse         │       │
  └──────────────────────────┬───────────────────────────────┘       │
                             │ Iceberg metadata.json                  │
                             ▼                                        ▼
                  ┌────────────────────────────────────────────────────┐
                  │         Amazon S3  us-east-2                        │
                  │  s3://xdatatoiceberg1/warehouse/lakehouse/events/   │
                  │  · Parquet data files  (2.56 MB target)             │
                  │  · metadata/*.metadata.json                         │
                  │  · metadata/*.avro  (manifest lists)                │
                  └────────────────────────────────────────────────────┘

  Doris (iceberg_polaris_rw)                            Apache Doris FE
  (write catalog, port 8283)                            192.168.1.50:30090
  polaris-auth-proxy:8283  (writer Bearer token)
```

### Write path

| Engine | Use case | Notes |
|---|---|---|
| **Spark** (primary) | ETL pipelines, bulk loads, streaming | Recommended — full distributed write throughput |
| **Doris** (secondary) | Ad-hoc / operational single inserts | `iceberg_polaris_rw` catalog; set `iceberg_write_target_file_size_bytes=2684354` |

### Read path

All three engines (Spark, Doris, Snowflake) read the **same Parquet files** on S3. Snowflake runs `ALTER ICEBERG TABLE … REFRESH` hourly to pick up new metadata pointers written by Spark.

---

## Endpoints & Credentials

| Service | Endpoint | Secret path (OpenBao) |
|---|---|---|
| RBAC Plane | `http://192.168.1.50:30850` | `secret/platform/rbac-plane` |
| Polaris REST (HTTP) | `http://192.168.1.50:30181/api/catalog` | `secret/platform/polaris` |
| Polaris REST (HTTPS) | `https://192.168.1.50:30553/api/catalog` | Self-signed cert — use `-k` |
| Doris FE MySQL | `192.168.1.50:30090` | `secret/platform/doris` |
| OpenBao | `http://192.168.1.50:30820` | K8s auth enabled; root token in `openbao-unseal-keys` secret |
| JupyterHub | `http://192.168.1.50:30080` | — |
| Snowflake | `oqihhtj-ta50603.snowflakecomputing.com` | `secret/platform/snowflake` |
| S3 | `s3://xdatatoiceberg1` / `us-east-2` | `secret/platform/s3` |

> All credentials are stored in OpenBao. Pods authenticate via Kubernetes service-account JWT (role `platform-secrets-read`). No hardcoded secrets in manifests.

---

## H — RBAC Plane

Migration `005_iceberg_polaris_doris_snowflake.sql` seeds all required roles.

| Role | Permissions | Doris grants | Snowflake role |
|---|---|---|---|
| `iceberg_engineer` | Polaris `CATALOG_MANAGE_CONTENT`, Doris read+write Iceberg | `SELECT_PRIV` + `LOAD_PRIV` on `iceberg_polaris*` | `iceberg_engineer_sf` |
| `analyst` | Read-only on both catalogs | `SELECT_PRIV` on `iceberg_polaris` + `snowflake_jdbc` | `analyst_sf` |
| `snowflake_reader` | Snowflake JDBC read via Doris | `SELECT_PRIV` on `snowflake_jdbc` | `analyst_sf` |

```bash
# Check user roles
rbacctl user roles <username>

# Force sync RBAC → Doris / Snowflake
curl -X POST -H "Authorization: Bearer $MASTER_TOKEN" \
  http://192.168.1.50:30850/api/v1/sync
```

---

## A — Spark Iceberg Table

| Property | Value |
|---|---|
| Catalog | Polaris REST — `spark_lakehouse` |
| Namespace | `lakehouse` |
| Table | `events` |
| S3 location | `s3://xdatatoiceberg1/warehouse/lakehouse/events/` |
| Format version | Iceberg v2 (Parquet / Snappy) |
| Target file size | **2.56 MB** (`write.target-file-size-bytes = 2684354`) |
| Partitioning | `hours(ts)` × `bucket[4](event_id)` — 4 hash buckets per hour |
| Current rows | 2,200,000 |
| Partitions | 192 (48 h × 4 buckets) |

### Schema

```sql
CREATE TABLE polaris.lakehouse.events (
  event_id   BIGINT        NOT NULL,
  source     STRING,
  event_type STRING,
  user_id    BIGINT,
  amount     DOUBLE,
  ts         TIMESTAMP     NOT NULL,
  payload    STRING
)
USING iceberg
PARTITIONED BY (hours(ts), bucket(4, event_id))
TBLPROPERTIES (
  'write.target-file-size-bytes' = '2684354',
  'write.parquet.compression-codec' = 'snappy',
  'write.format.default' = 'parquet'
);
```

See [`01_create_iceberg_table.py`](01_create_iceberg_table.py) for the full creation script with RBAC gate.

---

## B — Apache Polaris REST Catalog

| Principal | Client ID | Role |
|---|---|---|
| root | `87bbe6f0474de730` | Admin — management API only |
| spark-iceberg-svc | `35fc704fc21338c6` | Spark read/write (JupyterHub) |
| doris-reader | `3212a1b3cc1e9583` | `iceberg_polaris` catalog (port 8282) |
| doris-writer | `002050c89bbd1162` | `iceberg_polaris_rw` catalog (port 8283) |

### Get a Polaris token manually

```bash
TOKEN=$(curl -sf \
  -X POST http://192.168.1.50:30181/api/catalog/v1/oauth/tokens \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -u "CLIENT_ID:SECRET" \
  -d "grant_type=client_credentials&scope=PRINCIPAL_ROLE:ALL" \
  | sed 's/.*"access_token":"\([^"]*\)".*/\1/')
# Token TTL = 3600 s. Auto-refreshed by polaris-token-refresh CronJob.
```

### List tables

```bash
curl -sf -H "Authorization: Bearer $TOKEN" \
  http://192.168.1.50:30181/api/catalog/v1/spark_lakehouse/namespaces/lakehouse/tables
```

---

## C — Loading Data via JupyterHub / Spark

Open JupyterHub at `http://192.168.1.50:30080`, spawn a Spark notebook, and use:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .appName("lakehouse-etl") \
  .config("spark.sql.extensions",
          "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
  .config("spark.sql.catalog.polaris",
          "org.apache.iceberg.spark.SparkCatalog") \
  .config("spark.sql.catalog.polaris.type", "rest") \
  .config("spark.sql.catalog.polaris.uri",
          "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog") \
  .config("spark.sql.catalog.polaris.credential",
          "<spark_svc_id>:<spark_svc_secret>")  # from OpenBao secret/platform/polaris \
  .config("spark.sql.catalog.polaris.warehouse", "spark_lakehouse") \
  .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL") \
  .config("spark.hadoop.fs.s3a.access.key", "<from OpenBao secret/platform/s3>") \
  .config("spark.hadoop.fs.s3a.secret.key", "<from OpenBao secret/platform/s3>") \
  .config("spark.hadoop.fs.s3a.endpoint", "https://s3.us-east-2.amazonaws.com") \
  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
  .getOrCreate()

# Write data (bulk load)
df.writeTo("polaris.lakehouse.events") \
  .option("write.target-file-size-bytes", "2684354") \
  .append()

# Query
spark.sql("SELECT COUNT(*) FROM polaris.lakehouse.events").show()
```

> **Performance note:** Spark is the **primary write engine**. For ETL pipelines, bulk loads, and streaming, always write through Spark. Doris writes are for small operational inserts only.

See [`03_load_iceberg_data.ipynb`](03_load_iceberg_data.ipynb) for the full notebook.

---

## D/E — Snowflake Iceberg Table & Refresh Jobs

| Object | Name |
|---|---|
| Database | `LAKEHOUSE_DB` |
| Schema | `EVENTS` |
| Table | `LAKEHOUSE_DB.EVENTS.events` |
| Catalog integration | `iceberg_object_store` (OBJECT_STORE — reads S3 directly, no Polaris needed) |
| External volume | `iceberg_s3_vol` → IAM role `snowflake-iceberg-role` (account `586643076710`) |
| Refresh task | `refresh_lakehouse_events` — hourly (`USING CRON 0 * * * * UTC`) |

> Snowflake uses `CATALOG = 'OBJECT_STORE'` — it reads the Iceberg `metadata.json` **directly from S3** via the IAM role. This is independent of whether Polaris is reachable from the internet.

### D — Verify the refresh task

```sql
USE ROLE ACCOUNTADMIN;
USE DATABASE LAKEHOUSE_DB;
USE SCHEMA EVENTS;

SHOW TASKS LIKE 'refresh_lakehouse_events';
-- state should be: started
```

### E — Enable / Disable / Manual refresh

```sql
-- ▶  ENABLE  (resume a suspended task)
ALTER TASK refresh_lakehouse_events RESUME;

-- ⏸  DISABLE  (suspend without dropping — preserves schedule)
ALTER TASK refresh_lakehouse_events SUSPEND;

-- ▶  TRIGGER an immediate out-of-schedule refresh
EXECUTE TASK refresh_lakehouse_events;

-- ▶  MANUAL one-shot refresh (no task required)
ALTER ICEBERG TABLE LAKEHOUSE_DB.EVENTS.events REFRESH;

-- ▶  Check last 5 executions
SELECT name, state, scheduled_time, completed_time, error_code, error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP),
    TASK_NAME => 'refresh_lakehouse_events'
))
ORDER BY scheduled_time DESC LIMIT 5;

-- ▶  Verify row count after refresh
SELECT COUNT(*) FROM LAKEHOUSE_DB.EVENTS.events;
```

> **When to refresh manually:** Run `ALTER ICEBERG TABLE … REFRESH` immediately after a large Spark write if you need Snowflake to see new data before the next scheduled hourly run.

### Re-register the table after many new snapshots

```sql
-- Get latest metadata file from S3, then:
CREATE OR REPLACE ICEBERG TABLE events
    CATALOG           = 'iceberg_object_store'
    EXTERNAL_VOLUME   = 'iceberg_s3_vol'
    BASE_LOCATION     = 'lakehouse/events/'
    METADATA_FILE_PATH = 'metadata/<latest-file>.metadata.json';
ALTER TASK refresh_lakehouse_events RESUME;
```

See [`06_snowflake_iceberg_setup_and_refresh.sql`](06_snowflake_iceberg_setup_and_refresh.sql) for the full setup SQL.

---

## F — Doris Iceberg Catalog & 1-Minute Warm-up

| Catalog | Auth-proxy port | Principal | Use |
|---|---|---|---|
| `iceberg_polaris` | 8282 | doris-reader | Read queries (`SELECT`) |
| `iceberg_polaris_rw` | 8283 | doris-writer | Operational writes (`INSERT`) |

```sql
-- Connect: mysql -h 192.168.1.50 -P 30090 -uroot -p

-- Query
SELECT event_type, COUNT(*) AS cnt
FROM iceberg_polaris.lakehouse.events
GROUP BY event_type ORDER BY cnt DESC LIMIT 10;

-- Snapshot metadata
SELECT snapshot_id, committed_at, operation, summary
FROM iceberg_polaris.lakehouse.`events$snapshots`
ORDER BY committed_at DESC LIMIT 5;

-- Manual warm-up
WARM UP CATALOG iceberg_polaris WITH SYNC;
WARM UP TABLE iceberg_polaris.lakehouse.events WITH SYNC;
```

### 1-minute warm-up CronJob

```bash
# Status
kubectl get cronjob doris-iceberg-warmup -n prod
kubectl logs -n prod -l app=doris-iceberg-warmup --tail=5
```

See [`05b_doris_warmup_cronjob.yaml`](05b_doris_warmup_cronjob.yaml) for the CronJob spec.

---

## G — Doris Snowflake JDBC Catalog

| Property | Value |
|---|---|
| Catalog | `snowflake_jdbc` |
| Driver | `snowflake-jdbc-3.15.1.jar` (served from FE PVC via port 8888) |
| Auth | JWT (PKCS8 PEM at `/opt/apache-doris/fe/doris-meta/jdbc_drivers/sf_rsa_key_pkcs8.pem`) |
| Doris patch | `doris-fe.jar` bytecode-patched → maps `jdbc:snowflake` → `JdbcTrinoClient` |

```sql
-- List Snowflake databases/schemas
SHOW DATABASES FROM snowflake_jdbc;

-- Query Snowflake proprietary table through Doris
SELECT * FROM snowflake_jdbc.lakehouse_db.events LIMIT 5;

-- Cross-catalog JOIN: Iceberg (S3) + Snowflake
SELECT i.event_type, i.user_id, s.account_tier
FROM iceberg_polaris.lakehouse.events          AS i
JOIN snowflake_jdbc.lakehouse_db.user_profiles AS s
    ON i.user_id = s.user_id
LIMIT 20;
```

See [`05_doris_catalogs.sql`](05_doris_catalogs.sql) for full catalog DDL.

---

## Doris Write to Iceberg

Doris writes to the Iceberg table through the `iceberg_polaris_rw` catalog (separate from the read-only `iceberg_polaris` catalog). Both tokens are refreshed every 55 minutes by the same CronJob.

```sql
-- Set 2.56 MB target file size to match Spark
SET iceberg_write_target_file_size_bytes = 2684354;

-- Insert a single row
INSERT INTO iceberg_polaris_rw.lakehouse.events
VALUES (event_id, 'source', 'event_type', user_id, amount, ts, '{}');

-- Insert from staging table
INSERT INTO iceberg_polaris_rw.lakehouse.events
SELECT * FROM my_staging_table;
```

> ⚠️ **DELETE / UPDATE from Doris is not supported** on external Iceberg catalogs in Doris 4.0.7. Use Spark for row-level mutations:
> ```python
> spark.sql("DELETE FROM polaris.lakehouse.events WHERE <condition>")
> ```

---

## Token Refresh CronJob (OpenBao-aware)

The `polaris-token-refresh` CronJob runs every **55 minutes** (5 min before the 1-hour token TTL expires). Steps:

1. Authenticates to OpenBao via Kubernetes service-account JWT
2. Reads `doris_reader_id/secret` and `doris_writer_id/secret` from `secret/platform/polaris`
3. Fetches fresh Bearer tokens from Polaris for both principals
4. Patches the `polaris-auth-proxy-conf` ConfigMap (port 8282 = reader, port 8283 = writer)
5. Triggers a rolling restart of the `polaris-auth-proxy` Deployment

### Manual token refresh

```bash
# Trigger immediately
kubectl create job polaris-token-refresh-manual \
  --from=cronjob/polaris-token-refresh -n prod

# Watch logs
kubectl logs -n prod -l job-name=polaris-token-refresh-manual -f

# Clean up
kubectl delete job polaris-token-refresh-manual -n prod
```

### Suspend / resume the CronJob

```bash
# Suspend
kubectl patch cronjob polaris-token-refresh -n prod \
  -p '{"spec":{"suspend":true}}'

# Resume
kubectl patch cronjob polaris-token-refresh -n prod \
  -p '{"spec":{"suspend":false}}'

# Check status
kubectl get cronjob polaris-token-refresh -n prod
```

See [`07_polaris_token_refresh_cronjob.yaml`](07_polaris_token_refresh_cronjob.yaml) for the full spec.

---

## OpenBao Secret Management

All platform credentials are stored under the `secret/` KV v2 mount.

| Path | Keys |
|---|---|
| `secret/platform/doris` | `admin_password`, `jdbc_url` |
| `secret/platform/postgres` | `host`, `port`, `user`, `password`, `db` |
| `secret/platform/polaris` | `root_client_id/secret`, `doris_reader_id/secret`, `doris_writer_id/secret`, `spark_svc_id/secret`, `url` |
| `secret/platform/s3` | `access_key`, `secret_key`, `region`, `bucket`, `endpoint` |
| `secret/platform/snowflake` | `account`, `user`, `password`, `warehouse`, `role` |
| `secret/platform/rbac-plane` | `master_token`, `jwt_secret` |

### Read a secret from inside a pod

```bash
# Step 1: authenticate via Kubernetes service-account
SA_JWT=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
BAO_TOKEN=$(curl -sf \
  -X POST http://openbao.prod.svc.cluster.local:8200/v1/auth/kubernetes/login \
  -H "Content-Type: application/json" \
  -d "{\"role\":\"platform-secrets-read\",\"jwt\":\"$SA_JWT\"}" | \
  sed 's/.*"client_token":"\([^"]*\)".*/\1/')

# Step 2: read the secret
curl -sf -H "X-Vault-Token: $BAO_TOKEN" \
  http://openbao.prod.svc.cluster.local:8200/v1/secret/data/platform/polaris
```

### Read using root token (admin only)

```bash
BAO_ROOT=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

curl -sf -H "X-Vault-Token: $BAO_ROOT" \
  http://192.168.1.50:30820/v1/secret/data/platform/doris
```

---

## Troubleshooting

### Doris returns 401 / NotAuthorizedException on `iceberg_polaris`

1. The Polaris Bearer token has expired. Check last refresh:
   ```bash
   kubectl describe cronjob polaris-token-refresh -n prod | grep "Last Schedule"
   ```
2. Run a manual refresh:
   ```bash
   kubectl create job polaris-refresh-now \
     --from=cronjob/polaris-token-refresh -n prod
   kubectl logs -n prod -l job-name=polaris-refresh-now -f
   ```
3. Verify the auth-proxy is serving the new token:
   ```bash
   kubectl logs -n prod deploy/polaris-auth-proxy --tail=5
   ```

### Snowflake returns 0 rows after Spark writes new data

1. Run a manual refresh: `ALTER ICEBERG TABLE LAKEHOUSE_DB.EVENTS.events REFRESH;`
2. Check the task state: `SHOW TASKS LIKE 'refresh_lakehouse_events';` — state must be `started`
3. If `suspended`: `ALTER TASK refresh_lakehouse_events RESUME;`

### Doris FE pod restarting / JAR patch failing

```bash
# Check initContainer logs
kubectl logs doris-fe-0 -n prod -c patch-doris-fe-jar

# Verify patch script on PVC
kubectl exec -n prod doris-fe-0 -c doris-fe -- \
  ls /opt/apache-doris/fe/doris-meta/jdbc_drivers/
# Should list: apply-patch.sh  snowflake-jdbc-3.15.1.jar  sf_rsa_key_pkcs8.pem  doris-fe.jar.bak
```

### Snowflake EXTERNAL VOLUME — STS AssumeRole denied

> ⚠️ **Do NOT recreate the EXTERNAL VOLUME** — each `CREATE OR REPLACE` generates a new `ExternalId` requiring another IAM trust policy update.

1. Check current expected values:
   ```sql
   DESCRIBE EXTERNAL VOLUME iceberg_s3_vol;
   -- Read STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
   ```
2. Go to **AWS IAM → Role `snowflake-iceberg-role` → Trust relationships** and verify:
   - `Principal.AWS = arn:aws:iam::790347818956:user/7zjp1000-s`
   - `sts:ExternalId = NH90284_SFCRole=6_AG6hoMjvPpDhbQbqCPw9L73EdGU=`

### polaris-token-refresh CronJob failing

```bash
kubectl logs -n prod -l app=polaris-token-refresh --tail=20
```

- `"OpenBao login failed"` → check Kubernetes auth config:
  ```bash
  BAO_ROOT=$(kubectl get secret openbao-unseal-keys -n prod \
    -o jsonpath='{.data.root-token}' | base64 -d)
  curl -sf -H "X-Vault-Token: $BAO_ROOT" \
    http://192.168.1.50:30820/v1/auth/kubernetes/config
  ```
- `"OpenBao sealed"` → check auto-unseal CronJob:
  ```bash
  kubectl get cronjob openbao-auto-unseal -n prod
  kubectl logs -n prod -l job-name -l app=openbao-auto-unseal --tail=5
  ```

### Clean up test rows in Iceberg table

```python
# Run in JupyterHub Spark session (Doris DELETE not supported on external Iceberg)
spark.sql("DELETE FROM polaris.lakehouse.events WHERE source IN ('doris','doris-rw')")
spark.sql("SELECT COUNT(*) FROM polaris.lakehouse.events").show()
# Expected: 2,200,000
```

---

## Key Files

| File | Purpose |
|---|---|
| [`rbac-plane/migrations/005_iceberg_polaris_doris_snowflake.sql`](../migrations/005_iceberg_polaris_doris_snowflake.sql) | RBAC seed — roles, services, permissions |
| [`01_create_iceberg_table.py`](01_create_iceberg_table.py) | Spark table DDL + RBAC gate |
| [`02_polaris_catalog_setup.sql`](02_polaris_catalog_setup.sql) | Polaris catalog + principal setup |
| [`03_load_iceberg_data.ipynb`](03_load_iceberg_data.ipynb) | JupyterHub data load notebook |
| [`05_doris_catalogs.sql`](05_doris_catalogs.sql) | Doris catalog DDL (Iceberg read/write + JDBC) |
| [`05b_doris_warmup_cronjob.yaml`](05b_doris_warmup_cronjob.yaml) | 1-minute warm-up CronJob |
| [`06_snowflake_iceberg_setup_and_refresh.sql`](06_snowflake_iceberg_setup_and_refresh.sql) | Snowflake setup + refresh job management |
| [`07_polaris_token_refresh_cronjob.yaml`](07_polaris_token_refresh_cronjob.yaml) | 55-min token refresh CronJob (OpenBao-aware) |
