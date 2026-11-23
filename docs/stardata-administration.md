# Stardata — Administration & Maintenance Guide

> **Namespace:** `prod` · **Cluster:** `192.168.1.50`  
> **FE image:** `192.168.1.50:30500/apache/stardata:fe-4.6.5`  
> **BE image:** `192.168.1.50:30500/apache/stardata:be-4.2`  
> **Manifests:** [`manifests/stardata/`](../manifests/stardata/)

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Pod Inventory — Containers & Images](#2-pod-inventory--containers--images)
3. [External Catalogs](#3-external-catalogs)
4. [Connecting to Stardata](#4-connecting-to-stardata)
5. [Health Checks](#5-health-checks)
6. [User & Password Management](#6-user--password-management)
7. [Database & Table Operations](#7-database--table-operations)
8. [Storage Policy Management](#8-storage-policy-management)
9. [Catalog Management](#9-catalog-management)
10. [Catalog Refresh Jobs](#10-catalog-refresh-jobs)
11. [Rolling Restarts & Scaling](#11-rolling-restarts--scaling)
12. [Upgrading the FE Image](#12-upgrading-the-fe-image)
13. [Backup & Metadata Recovery](#13-backup--metadata-recovery)
14. [Log Access](#14-log-access)
15. [Node-Level Maintenance](#15-node-level-maintenance)
16. [Secrets Management](#16-secrets-management)
17. [Troubleshooting Quick Reference](#17-troubleshooting-quick-reference)

---

## 1. Architecture Overview

```
                   ┌────────────────────────────────────────────────────┐
                   │  Stardata FE StatefulSet  (3 pods — worker2/3/4)   │
                   │                                                      │
                   │  stardata-fe-0 (MASTER)                             │
                   │  stardata-fe-1 (FOLLOWER)                           │
                   │  stardata-fe-2 (FOLLOWER)                           │
                   │                                                      │
                   │  Each pod has 4 containers:                         │
                   │  stardata-fe · ddl-mirror · cooldown-policy ·       │
                   │  jdbc-httpd  + 5 initContainers (see §2)            │
                   └───────────────────┬────────────────────────────────┘
                                       │ BdbJE quorum (9010/9020)
                       MySQL :9030 / HTTP :8030
                                       │
                   ┌───────────────────▼────────────────────────────────┐
                   │  Stardata BE StatefulSet  (3 pods — worker2/3/4)   │
                   │                                                      │
                   │  stardata-be-0 (worker2)  — 400Gi PVC              │
                   │  stardata-be-1 (worker3)  — 400Gi PVC              │
                   │  stardata-be-2 (worker4)  — 400Gi PVC              │
                   │                                                      │
                   │  Each pod has 1 container: stardata-be              │
                   │  + 2 initContainers (see §2)                        │
                   └────────────────────────────────────────────────────┘

External access:
  Web UI   → http://192.168.1.50:30030
  MySQL    → 192.168.1.50:32530  (port 32530)
  JDBC-HTTP → :8181 (jar serving, internal only)
```

### Storage

| PVC name pattern | Size | Mounted at | Node |
|---|---|---|---|
| `stardata-fe-meta-stardata-fe-{0,1,2}` | 100Gi | `/opt/apache-doris/fe/doris-meta` | worker2/3/4 |
| `stardata-be-storage-stardata-be-{0,1,2}` | 400Gi | `/opt/apache-doris/be/storage` | worker2/3/4 |

---

## 2. Pod Inventory — Containers & Images

### FE Pod (×3: `stardata-fe-{0,1,2}`)

#### Init Containers

| Name | Image | Purpose |
|---|---|---|
| `copy-fe-config` | `192.168.1.50:30500/busybox:1.36` | Copies ConfigMap (read-only) into writable `emptyDir` |
| `patch-fe-advertise` | `192.168.1.50:30500/busybox:1.36` | Appends `advertised_address = <pod-FQDN>` to `fe.conf` |
| `seed-cooldown-script` | `192.168.1.50:30500/apache/stardata:fe-4.6.5` | Seeds `cooldown_policy.py` into shared `emptyDir` |
| `seed-jdbc-drivers` | `192.168.1.50:30500/apache/stardata:fe-4.6.5` | Seeds all JDBC JARs into the `jdbc-drivers` `emptyDir` |
| `seed-trino-plugins` | `192.168.1.50:30500/apache/stardata:trino-plugins-v1` | Seeds MongoDB Trino connector JARs into `emptyDir` |

#### Main Containers

| Container | Image | Role | Key env / mounts |
|---|---|---|---|
| `stardata-fe` | `fe-4.6.5` | Frontend query planner, metadata store (BdbJE) | `STARDATA_ROOT_PASSWORD` from secret; mounts `fe-meta` PVC, `jdbc-drivers`, `trino-plugins`, `snowflake-jwt` |
| `ddl-mirror` | `python:3.12-alpine` | Tails `fe.audit.log`; mirrors tables with `created_at`/`updated_at` to `cache_system` (fe-0 only) | `STARDATA_PASS`, `POD_NAME` |
| `jdbc-httpd` | `192.168.1.50:30500/busybox:1.36` | Serves JDBC JARs over HTTP on `:8181` for BE to download | mounts `jdbc-drivers` emptyDir |
| `cooldown-policy` | `python:3.12-alpine` | Runs `cooldown_policy.py` every 3600s; sets `COOLDOWN TIME` on MOW partitions (leader only) | `DORIS_PASS`, `COOLDOWN_DAYS=1`, `POLL_INTERVAL_SECONDS=3600` |

> ℹ️ **Expected ready state:** `4/4 Running` per FE pod (all 4 containers healthy).
> `kubectl exec` without `-c stardata-fe` defaults to the wrong sidecar — always specify `-c stardata-fe`.

#### JDBC JARs served by `jdbc-httpd`

| JAR | Catalog |
|---|---|
| `snowflake-jdbc-4.0.2.jar` | snowflake |
| `ojdbc11.jar` | oracle |
| `postgresql-42.7.4.jar` | postgres (JDBC direct) |
| `mongodb-jdbc-2.0.2-all.jar` | mongodb (JDBC direct) |
| `databricks-jdbc-2.6.36.1070.jar` | databricks (JDBC direct) |

---

### BE Pod (×3: `stardata-be-{0,1,2}`)

#### Init Containers

| Name | Image | Purpose |
|---|---|---|
| `copy-be-config` | `192.168.1.50:30500/busybox:1.36` | Copies `be.conf` ConfigMap into writable `emptyDir` |
| `seed-trino-plugins` | `192.168.1.50:30500/apache/stardata:trino-plugins-v1` | Seeds MongoDB Trino connector JARs for BE Trino plugin |

#### Main Container

| Container | Image | Key ports | Mounts |
|---|---|---|---|
| `stardata-be` | `be-4.2` | `9060` (BE), `8040` (webserver/health), `9050` (heartbeat), `8060` (BRPC) | `be-storage` PVC (400Gi), `snowflake-jwt`, `trino-plugins` |

> ℹ️ **Expected ready state:** `1/1 Running` per BE pod.  
> `startupProbe` on `:8040/api/health` — allows up to 330s startup window.

---

## 3. External Catalogs

Stardata has 7 external catalogs registered. All metadata persists in the FE BdbJE PVC.

| Catalog | Type | Backend | Warehouse / DB |
|---|---|---|---|
| `polaris` | Iceberg REST | Polaris auth-proxy `:8283` | `IcebergCatalog` (S3 us-east-2) |
| `databricks` | Iceberg REST | Polaris auth-proxy `:8283` | `star_lakehouse` (S3 us-east-2) |
| `postgres` | Iceberg REST | Polaris auth-proxy `:8283` | `pg_lakehouse` (S3 us-east-2) |
| `oracle` | Iceberg REST | Polaris auth-proxy `:8283` | `ora_lakehouse` (S3 us-east-2) |
| `mongodb` | Iceberg REST | Polaris auth-proxy `:8283` | `mgo_lakehouse` (S3 us-east-2) |
| `snowflake` | JDBC | Snowflake `oqihhtj-ta50603` | `LAKEHOUSE_DB` (JWT / RSA key-pair) |
| `snowflake` (direct JDBC) | JDBC | Snowflake JDBC via `JdbcSnowflakeClient` | see setup/10 |

> **Auth for Iceberg catalogs:** OAuth2 Bearer token auto-injected by `polaris-auth-proxy`.  
> **Auth for snowflake:** RSA key-pair JWT — `snowflake-jwt-key` Secret at `/snowflake-jwt/rsa_key.p8` on FE and BE pods.

---

## 4. Connecting to Stardata

### From the control node

```bash
# Get the admin password
STARDATA_PASS=$(kubectl get secret stardata-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)

# Connect via MySQL client
mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}"
```

### From inside the cluster (exec into FE pod)

```bash
# Always specify -c stardata-fe to avoid exec into the wrong sidecar
kubectl exec -n prod statefulset/stardata-fe -c stardata-fe -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${STARDATA_PASS}"
```

### One-liner query

```bash
kubectl exec -n prod statefulset/stardata-fe -c stardata-fe -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${STARDATA_PASS}" \
  -e "SHOW FRONTENDS\G"
```

---

## 5. Health Checks

### Quick pod status

```bash
# FE pods — expect 3/3 Running for each
kubectl get pod -n prod -l app=stardata-fe -o wide

# BE pods — expect 1/1 Running for each
kubectl get pod -n prod -l app=stardata-be -o wide
```

### Cluster health via SQL

```sql
-- FE quorum status
-- Expected: feType=MASTER for one, FOLLOWER for others; Alive=true; Host = FQDN
SHOW FRONTENDS\G

-- BE registration status
-- Expected: Alive=true for all 3; HeartbeatAddress = pod FQDN or IP
SHOW BACKENDS\G

-- Cluster-wide resource overview
SHOW PROC '/backends'\G
```

### BE HTTP health endpoint

```bash
# Hit each BE's webserver health endpoint directly
for i in 0 1 2; do
  echo "=== stardata-be-${i} ==="
  kubectl exec -n prod stardata-be-${i} -c stardata-be -- \
    curl -s http://127.0.0.1:8040/api/health | python3 -m json.tool
done
```

### FE bootstrap endpoint

```bash
# FE readinessProbe target — should return HTTP 200
kubectl exec -n prod statefulset/stardata-fe -c stardata-fe -- \
  curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8030/api/bootstrap
```

### Catalog connectivity

```bash
STARDATA_PASS=$(kubectl get secret stardata-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)

kubectl exec -n prod statefulset/stardata-fe -c stardata-fe -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${STARDATA_PASS}" \
  -e "SHOW CATALOGS;" 2>/dev/null
# Expected: internal  polaris  databricks  postgres  oracle  mongodb  snowflake
```

---

## 6. User & Password Management

### Change the root password

```sql
-- Connect as root first, then:
SET PASSWORD FOR 'root'@'%' = PASSWORD('<new-password>');
```

After changing, update the Kubernetes secret:

```bash
kubectl create secret generic stardata-credentials -n prod \
  --from-literal=admin-password='<new-password>' \
  --dry-run=client -o yaml | kubectl apply -f -
```

### Create a read-only user

```sql
CREATE USER 'reader'@'%' IDENTIFIED BY '<password>';
GRANT SELECT_PRIV ON *.*.* TO 'reader'@'%';
```

### Create a database-scoped user

```sql
CREATE USER 'appuser'@'%' IDENTIFIED BY '<password>';
GRANT SELECT_PRIV, LOAD_PRIV, ALTER_PRIV ON mydb.*.* TO 'appuser'@'%';
```

### List all users

```sql
SELECT user, host FROM mysql.user;
```

### Show grants for a user

```sql
SHOW GRANTS FOR 'reader'@'%';
```

---

## 7. Database & Table Operations

### List databases and tables

```sql
SHOW DATABASES;
USE mydb;
SHOW TABLES;
SHOW CREATE TABLE mytable\G
```

### Create a database

```sql
CREATE DATABASE IF NOT EXISTS mydb;
```

### Create a table (auto-features apply)

New OLAP tables automatically receive `snap_id`, `created_at`, daily partitioning, and `sd_cold_1d` storage policy (see [`docs/stardata-auto-table-features.md`](stardata-auto-table-features.md)):

```sql
CREATE TABLE mydb.events (
    event_id   BIGINT       NOT NULL,
    event_type VARCHAR(64)  NOT NULL,
    payload    STRING
)
DUPLICATE KEY(event_id)
DISTRIBUTED BY HASH(event_id) BUCKETS AUTO;
-- Auto-injected: snap_id, created_at, AUTO PARTITION BY RANGE(created_at), sd_cold_1d
```

### Create a table opting out of auto-partitioning

```sql
CREATE TABLE mydb.lookup_table (
    code  VARCHAR(32)  NOT NULL,
    label VARCHAR(256) NOT NULL
)
UNIQUE KEY(code)
DISTRIBUTED BY HASH(code) BUCKETS 4
PROPERTIES (
    "stardata.disable_partition" = "true"
);
```

### Add a column to an existing table

```sql
ALTER TABLE mydb.events ADD COLUMN region VARCHAR(32) NULL;
```

### Drop a table

```sql
DROP TABLE IF EXISTS mydb.events;
```

### Truncate a table

```sql
TRUNCATE TABLE mydb.events;
```

### View partition list

```sql
SHOW PARTITIONS FROM mydb.events;
```

### Drop a specific partition

```sql
ALTER TABLE mydb.events DROP PARTITION p20261101;
```

### Check disk usage by database

```sql
SELECT TABLE_SCHEMA, ROUND(SUM(DATA_LENGTH)/1024/1024/1024, 2) AS size_gb
FROM information_schema.tables
WHERE TABLE_SCHEMA NOT IN ('information_schema','__internal_schema')
GROUP BY TABLE_SCHEMA
ORDER BY size_gb DESC;
```

---

## 8. Storage Policy Management

### View the cold storage resource

```sql
SHOW RESOURCES WHERE NAME = "sd_s3_cold"\G
```

### View the cooldown policy

```sql
SHOW STORAGE POLICY WHERE POLICY_NAME = "sd_cold_1d"\G
-- Expected: cooldown_ttl = 86400 (1 day)
```

### Attach the policy to an existing table

```sql
ALTER TABLE mydb.events SET ("storage_policy" = "sd_cold_1d");
```

### Check which tables have cold segments

```sql
SELECT table_name, partition_name, remote_data_size
FROM information_schema.table_options
WHERE storage_policy = 'sd_cold_1d'
  AND remote_data_size > 0;
```

### Rotate S3 credentials in the resource

```sql
ALTER STORAGE RESOURCE "sd_s3_cold"
  PROPERTIES (
    "s3.access_key" = "<new-key>",
    "s3.secret_key" = "<new-secret>"
  );
```

### cooldown-policy sidecar — force an immediate cycle

```bash
# Restart only the cooldown-policy sidecar on fe-0
# (Kubernetes does not support per-container restart — restart full pod)
kubectl rollout restart statefulset/stardata-fe -n prod
```

---

## 9. Catalog Management

### List all catalogs

```sql
SHOW CATALOGS;
```

### Switch into a catalog

```sql
SWITCH polaris;
SHOW DATABASES;
```

### Query across catalogs (cross-catalog join)

```sql
SELECT p.customer_id, s.amount
FROM polaris.tpcds_sf10tcl.store_sales s
JOIN postgres.public.customers         p ON s.ss_customer_sk = p.customer_sk
LIMIT 10;
```

### Recreate the Polaris (Iceberg) catalogs

```bash
STARDATA_PASS=$(kubectl get secret stardata-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)
BAO_TOKEN=$(cat ~/.bao-token)   # or however you store it

export S3_KEY=$(curl -s -H "X-Vault-Token: ${BAO_TOKEN}" \
  http://192.168.1.50:30820/v1/secret/data/platform/s3 \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['data']['access_key'])")
export S3_SECRET=$(curl -s -H "X-Vault-Token: ${BAO_TOKEN}" \
  http://192.168.1.50:30820/v1/secret/data/platform/s3 \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['data']['secret_key'])")

envsubst < manifests/stardata/setup/02_create_catalogs.sql \
  | mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}"
```

### Recreate the Snowflake catalog (JWT auth)

```bash
STARDATA_PASS=$(kubectl get secret stardata-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)

kubectl exec -n prod statefulset/stardata-fe -c stardata-fe -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${STARDATA_PASS}" --connect-timeout=10 \
  -e "DROP CATALOG IF EXISTS snowflake;
      CREATE CATALOG snowflake PROPERTIES (
        'type'         = 'jdbc',
        'user'         = 'testsnowflake',
        'jdbc_url'     = 'jdbc:snowflake://oqihhtj-ta50603.snowflakecomputing.com/?authenticator=snowflake_jwt&private_key_file=/snowflake-jwt/rsa_key.p8&warehouse=COMPUTE_WH&db=LAKEHOUSE_DB&loginTimeout=60',
        'driver_url'   = 'http://stardata-fe.prod.svc.cluster.local:8181/snowflake-jdbc-4.0.2.jar',
        'driver_class' = 'net.snowflake.client.jdbc.SnowflakeDriver'
      );
      REFRESH CATALOG snowflake;" 2>/dev/null
```

### Recreate the Oracle catalog

```bash
mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}" \
  < manifests/stardata/setup/11_create_oracle_catalog.sql
```

### Recreate the PostgreSQL catalog

```bash
mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}" \
  < manifests/stardata/setup/12_create_postgresql_catalog.sql
```

### Recreate the MongoDB catalog

```bash
mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}" \
  < manifests/stardata/setup/13_create_mongodb_catalog.sql
```

### Drop a catalog

```sql
DROP CATALOG IF EXISTS oracle;
```

---

## 10. Catalog Refresh Jobs

Two CronJobs keep catalog metadata and table manifests current:

| CronJob | Schedule | What it does |
|---|---|---|
| `stardata-catalog-refresh` | every 6 hours (`0 */6 * * *`) | `REFRESH CATALOG` for all 6 catalogs — picks up new tables, dropped tables, schema changes |
| `stardata-table-refresh` | every 5 minutes (`*/5 * * * *`) | `REFRESH TABLE` for ~20 hot tables — picks up new Iceberg snapshots from Spark |

### View CronJob status

```bash
kubectl get cronjob -n prod -l app=stardata-catalog-refresh
kubectl get cronjob -n prod -l app=stardata-table-refresh

# See recent job runs
kubectl get jobs -n prod | grep stardata
```

### Trigger a manual catalog refresh

```bash
# Full catalog structure refresh (all catalogs)
kubectl create job --from=cronjob/stardata-catalog-refresh \
  stardata-catalog-refresh-manual-$(date +%s) -n prod

# Monitor it
kubectl logs -n prod -l app=stardata-catalog-refresh --tail=20 -f
```

### Trigger a manual table refresh

```bash
kubectl create job --from=cronjob/stardata-table-refresh \
  stardata-table-refresh-manual-$(date +%s) -n prod
```

### Manual REFRESH CATALOG from SQL

```bash
STARDATA_PASS=$(kubectl get secret stardata-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)

kubectl exec -n prod statefulset/stardata-fe -c stardata-fe -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${STARDATA_PASS}" \
  -e "REFRESH CATALOG polaris;
      REFRESH CATALOG databricks;
      REFRESH CATALOG postgres;
      REFRESH CATALOG oracle;
      REFRESH CATALOG mongodb;
      REFRESH CATALOG snowflake;" 2>/dev/null
```

---

## 11. Rolling Restarts & Scaling

### Restart FE pods

```bash
# Rolling restart — ArgoCD-safe (always git-push first if changing images)
kubectl rollout restart statefulset/stardata-fe -n prod
kubectl rollout status  statefulset/stardata-fe -n prod --timeout=300s

# Confirm all 3 pods are 3/3 Running
kubectl get pod -n prod -l app=stardata-fe
```

### Restart BE pods

```bash
kubectl rollout restart statefulset/stardata-be -n prod
kubectl rollout status  statefulset/stardata-be -n prod --timeout=300s
```

### Restart a single FE pod

```bash
# Delete the pod — StatefulSet will recreate it with the same identity
kubectl delete pod -n prod stardata-fe-1
kubectl rollout status statefulset/stardata-fe -n prod
```

### Full cluster restart (FE first, then BE)

```bash
kubectl rollout restart statefulset/stardata-fe -n prod
kubectl rollout status  statefulset/stardata-fe -n prod --timeout=300s

kubectl rollout restart statefulset/stardata-be -n prod
kubectl rollout status  statefulset/stardata-be -n prod --timeout=300s
```

> ⚠️ Always restart FE before BE. If the BE comes up before the FE is ready, it may fail to register and need a manual restart.

---

## 12. Upgrading the FE Image

> Full upgrade procedure: [`docs/runbooks/runbook-35-stardata-doris-upgrade.md`](runbooks/runbook-35-stardata-doris-upgrade.md)

### Quick steps

```bash
# 1. Update Dockerfile.fe base image tag (both builder and runtime stages)
vim stardata/Dockerfile.fe

# 2. Build and push the new image
bash scripts/master/build-and-push-custom-images.sh stardata-fe 2>&1 | tee /tmp/fe-build.log
grep -E "error:|successfully tagged|FAILED" /tmp/fe-build.log

# 3. Update the manifest to reference the new image tag
sed -i 's|fe-4.6.5|fe-<NEW>|g' manifests/stardata/stardata-fe-deployment.yaml

# 4. Commit and push (ArgoCD selfHeal=true will revert any kubectl-only changes)
git add manifests/stardata/stardata-fe-deployment.yaml stardata/Dockerfile.fe
git commit -m "chore: bump FE image to fe-<NEW>"
git push origin main

# 5. Evict the old cached image from each worker node
for NODE in worker2.local worker3.local worker4.local; do
  kubectl debug node/${NODE} -it --image=192.168.1.50:30500/busybox:1.36 \
    -- sh -c "chroot /host crictl rmi 192.168.1.50:30500/apache/stardata:fe-4.6.5 2>&1"
done

# 6. Roll FE pods
kubectl rollout restart statefulset/stardata-fe -n prod
kubectl rollout status  statefulset/stardata-fe -n prod --timeout=300s

# 7. Verify — expect 3/3 Running and all catalogs accessible
kubectl get pod -n prod -l app=stardata-fe
```

> ⚠️ **Never** use `kubectl set image` alone — ArgoCD's `selfHeal: true` will revert it within seconds.

---

## 13. Backup & Metadata Recovery

### FE metadata location

FE metadata (BdbJE) is stored in each FE pod's PVC:

```
/opt/apache-doris/fe/doris-meta/   (PVC: stardata-fe-meta-stardata-fe-{0,1,2})
```

Catalogs, users, table schemas, and storage policies all live here. PVCs use `Retain` policy — they survive pod deletion.

### Backup FE metadata (manual)

```bash
# From worker2.local (stardata-fe-0 runs there)
ssh root@worker2.local
tar czf /backup/stardata-fe-meta-$(date +%Y%m%d).tar.gz \
  /home/<pv-mount-path>/fe/doris-meta/
```

### Wipe and re-initialize FE metadata (break-glass)

> ⚠️ **Data loss** — all catalog registrations, user accounts, and table schemas are lost. BEs will re-register automatically. Catalogs must be re-created from setup scripts.

```bash
# 1. Scale down FE
kubectl scale statefulset/stardata-fe -n prod --replicas=0

# 2. Delete the BdbJE data on each FE PVC (via a debug pod or direct node access)
# DO THIS ON ALL 3 FE NODES
for POD in stardata-fe-0 stardata-fe-1 stardata-fe-2; do
  echo "Clearing ${POD}..."
  # access via node or temporary pod mounting the PVC
done

# 3. Scale FE back up
kubectl scale statefulset/stardata-fe -n prod --replicas=3
kubectl rollout status statefulset/stardata-fe -n prod --timeout=300s

# 4. Re-create all catalogs and storage policies
bash manifests/stardata/setup/06_07_apply_storage_and_partitions.sh
mysql -h 192.168.1.50 -P 32530 -u root \
  < manifests/stardata/setup/02_create_catalogs.sql
```

### Reset FE BdbJE on a single pod (stale peer fix)

```bash
kubectl exec -n prod stardata-fe-0 -c stardata-fe -- \
  rm -rf /opt/apache-doris/fe/doris-meta/bdb/
kubectl rollout restart statefulset/stardata-fe -n prod
```

---

## 14. Log Access

### FE main log

```bash
# Live tail of FE main log (stardata-fe-0)
kubectl logs -n prod stardata-fe-0 -c stardata-fe -f

# Last 100 lines
kubectl logs -n prod stardata-fe-0 -c stardata-fe --tail=100

# Search for errors
kubectl logs -n prod statefulset/stardata-fe -c stardata-fe 2>&1 | grep -i "error\|WARN"
```

### FE audit log (DDL and query history)

```bash
# The audit log is in the shared fe-log emptyDir volume
kubectl exec -n prod stardata-fe-0 -c stardata-fe -- \
  tail -f /opt/apache-doris/fe/log/fe.audit.log
```

### ddl-mirror sidecar log

```bash
kubectl logs -n prod stardata-fe-0 -c ddl-mirror -f
# Expected lines: [ddl-mirror] CREATED cache_system.<tbl>
```

### cooldown-policy sidecar log

```bash
kubectl logs -n prod stardata-fe-0 -c cooldown-policy -f
```

### BE log

```bash
kubectl logs -n prod stardata-be-0 -c stardata-be -f --tail=100

# Check heartbeat and registration
kubectl logs -n prod stardata-be-0 -c stardata-be | grep -i "heartbeat\|register\|FE"
```

### JDBC httpd sidecar log

```bash
kubectl logs -n prod stardata-fe-0 -c jdbc-httpd
```

---

## 15. Node-Level Maintenance

### vm.max_map_count (required for BE)

The BE will crash immediately if `vm.max_map_count < 2,000,000` on the host node:

```bash
# Verify on all BE nodes
for NODE in worker2.local worker3.local worker4.local; do
  echo -n "${NODE}: "
  ssh root@${NODE} cat /proc/sys/vm/max_map_count
done
# Must show: 2000000

# Fix (if below — e.g. after OS reinstall)
ssh root@worker2.local \
  "sysctl -w vm.max_map_count=2000000 && \
   echo 'vm.max_map_count=2000000' > /etc/sysctl.d/99-stardata.conf && \
   sysctl --system"
```

### Drain a worker node for maintenance

```bash
# Example: drain worker3 (runs stardata-fe-1 and stardata-be-1)
kubectl drain worker3.local --ignore-daemonsets --delete-emptydir-data

# After maintenance, uncordon
kubectl uncordon worker3.local

# Roll pods back onto the node
kubectl rollout restart statefulset/stardata-fe -n prod
kubectl rollout restart statefulset/stardata-be -n prod
```

> ⚠️ Draining a node reduces the FE quorum to 2/3 (still a majority — cluster remains available). Never drain two FE nodes simultaneously.

### Check disk usage on BE nodes

```bash
for NODE in worker2.local worker3.local worker4.local; do
  echo "=== ${NODE} ==="
  ssh root@${NODE} df -h /home
done
```

---

## 16. Secrets Management

### Stardata admin password

Stored in K8s secret `stardata-credentials` (namespace `prod`) and in OpenBao at `secret/data/stardata/credentials`.

```bash
# Read current password
kubectl get secret stardata-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d

# Update secret (after changing password in SQL)
kubectl create secret generic stardata-credentials -n prod \
  --from-literal=admin-password='<new-password>' \
  --dry-run=client -o yaml | kubectl apply -f -
```

### Snowflake JWT RSA key

```bash
# View the mounted key (confirms it's present)
kubectl exec -n prod statefulset/stardata-fe -c stardata-fe -- \
  ls -la /snowflake-jwt/
# Expected: rsa_key.p8

# Rotate the RSA key pair:
# 1. Generate new key pair
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out /tmp/rsa_key_new.p8

# 2. Register new public key in Snowflake
#    ALTER USER testsnowflake SET RSA_PUBLIC_KEY='<new public key body>';

# 3. Update the K8s Secret
kubectl create secret generic snowflake-jwt-key -n prod \
  --from-file=rsa_key.p8=/tmp/rsa_key_new.p8 \
  --dry-run=client -o yaml | kubectl apply -f -
# Pods pick up the new key within ~60s — no restart needed
```

### S3 credentials

Stored in `platform-s3-credentials` secret and in OpenBao at `secret/data/platform/s3`.

```bash
# Rotate in the Stardata storage resource (no pod restart needed)
kubectl exec -n prod statefulset/stardata-fe -c stardata-fe -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${STARDATA_PASS}" \
  -e "ALTER STORAGE RESOURCE 'sd_s3_cold'
        PROPERTIES ('s3.access_key'='<new>', 's3.secret_key'='<new>');" 2>/dev/null
```

---

## 17. Troubleshooting Quick Reference

### BE in CrashLoopBackOff (vm.max_map_count)

```bash
kubectl logs -n prod -l app=stardata-be --tail=5 | grep "max_map_count"
# If present — fix vm.max_map_count on the host node (see §15)
```

### FE feType:UNKNOWN (stale BdbJE peer)

```bash
kubectl logs -n prod -l app=stardata-fe -c stardata-fe | grep -i "bdbje\|peer\|UNKNOWN"

# Fix: clear BdbJE and restart
kubectl exec -n prod stardata-fe-0 -c stardata-fe -- \
  rm -rf /opt/apache-doris/fe/doris-meta/bdb/
kubectl rollout restart statefulset/stardata-fe -n prod
```

### BE not registering — stale hostname

```bash
STARDATA_PASS=$(kubectl get secret stardata-credentials -n prod \
  -o jsonpath='{.data.admin-password}' | base64 -d)
BE_IP=$(kubectl get pod -n prod stardata-be-0 -o jsonpath='{.status.podIP}')

# Drop stale entry and re-add by IP
kubectl exec -n prod statefulset/stardata-fe -c stardata-fe -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${STARDATA_PASS}" \
  -e "ALTER SYSTEM DROPP BACKEND '<stale-hostname>:9050';"

kubectl exec -n prod statefulset/stardata-fe -c stardata-fe -- \
  mysql -h 127.0.0.1 -P 9030 -u root -p"${STARDATA_PASS}" \
  -e "ALTER SYSTEM ADD BACKEND '${BE_IP}:9050';"
```

### BE not registering — cluster ID mismatch

```bash
# Get FE cluster ID
kubectl exec -n prod statefulset/stardata-fe -c stardata-fe -- \
  cat /opt/apache-doris/fe/doris-meta/image/VERSION
# Note the clusterId= value

# Overwrite BE's stored cluster ID to match FE
kubectl exec -n prod stardata-be-0 -c stardata-be -- \
  bash -c "echo '<FE_CLUSTER_ID>' > /opt/apache-doris/be/storage/cluster_id"

kubectl rollout restart deployment/stardata-be -n prod
```

### Wrong container on kubectl exec

```bash
# Always specify the container:
kubectl exec -n prod statefulset/stardata-fe -c stardata-fe -- mysql ...
# Without -c stardata-fe, exec targets ddl-mirror (no mysql binary → error)
```

### JDBC httpd sidecar not serving the JAR

```bash
kubectl exec -n prod stardata-fe-0 -c jdbc-httpd -- \
  wget -q -O /dev/null --server-response \
  http://localhost:8181/snowflake-jdbc-4.0.2.jar 2>&1 | head -3
# Expected: HTTP/1.1 200 OK
```

### ddl-mirror not creating cache_system tables

```bash
# Check it is running on fe-0 only
kubectl logs -n prod stardata-fe-0 -c ddl-mirror | tail -20
kubectl logs -n prod stardata-fe-1 -c ddl-mirror | tail -5
# fe-1 / fe-2 should show: "is not fe-0 — skipping ddl-mirror"
```

### Force a manual catalog refresh

```sql
REFRESH CATALOG polaris;
-- Or for a specific table:
REFRESH TABLE polaris.tpcds_sf10tcl.store_sales;
```

---

## Related Documents

| Document | Purpose |
|---|---|
| [`docs/stardata.md`](stardata.md) | Deployment overview, architecture, basic troubleshooting |
| [`docs/stardata-auto-table-features.md`](stardata-auto-table-features.md) | Auto-injected columns, partitioning, storage policy |
| [`docs/runbooks/runbook-35-stardata-doris-upgrade.md`](runbooks/runbook-35-stardata-doris-upgrade.md) | Full FE image upgrade procedure including Java patch re-porting |
| [`manifests/stardata/`](../manifests/stardata/) | All Kubernetes manifests |
| [`manifests/stardata/setup/`](../manifests/stardata/setup/) | Catalog DDL, storage policy, metadata setup scripts |
| [`stardata/Dockerfile.fe`](../stardata/Dockerfile.fe) | FE image build — base image, Java patches, JDBC JARs |
| [`stardata/doris-patch/`](../stardata/doris-patch/) | Patched Java source files |
