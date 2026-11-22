# Stardata Administration Guide

> Apache Doris 4.2 · Kubernetes namespace `prod` · Private registry `192.168.1.50:30500`

---

## Table of Contents

1. [Cluster Architecture](#1-cluster-architecture)
2. [Access & Endpoints](#2-access--endpoints)
3. [FE Configuration Reference](#3-fe-configuration-reference)
4. [BE Configuration Reference](#4-be-configuration-reference)
5. [FE Sidecar Containers](#5-fe-sidecar-containers)
6. [FE Source Patches](#6-fe-source-patches)
7. [Auto-Injection at CREATE TABLE](#7-auto-injection-at-create-table)
8. [Partition Customization](#8-partition-customization)
9. [Hot / Cold Storage Tiering](#9-hot--cold-storage-tiering)
10. [Iceberg Storage Policy & Partition Cooldown (MOW Tables)](#10-iceberg-storage-policy--partition-cooldown-mow-tables)
11. [Segment Data File Cache](#11-segment-data-file-cache)
12. [cache\_system Database](#12-cache_system-database)
13. [Multi-Catalog Reference](#13-multi-catalog-reference)
14. [Setup Script Sequence](#14-setup-script-sequence)
15. [Step-by-Step Initial Deployment](#15-step-by-step-initial-deployment)
16. [Administration Commands](#16-administration-commands)
17. [Performance Tuning](#17-performance-tuning)
18. [Regression Testing](#18-regression-testing)
19. [Troubleshooting](#19-troubleshooting)

---

## 1. Cluster Architecture

### Topology

```
worker2.local ─── stardata-fe-0 (MASTER)   stardata-be-0
worker3.local ─── stardata-fe-1 (FOLLOWER) stardata-be-1
worker4.local ─── stardata-fe-2 (FOLLOWER) stardata-be-2
```

All six pods run on nodes labelled `stardata=true` and tolerate the `dedicated=stardata:NoSchedule` taint. Pod anti-affinity ensures exactly one FE and one BE per node.

### Component images

| Component | Image |
|---|---|
| FE (patched) | `192.168.1.50:30500/apache/stardata:fe-4.6.3` |
| BE | `192.168.1.50:30500/apache/stardata:be-4.2` |
| Trino plugins | `192.168.1.50:30500/apache/stardata:trino-plugins-v1` |
| Patch base | `apache/doris:fe-4.1.4.1` |

### Storage

| Pod | PVC name | Size | Host path |
|---|---|---|---|
| stardata-fe-{0,1,2} | `fe-meta-stardata-fe-{N}` | 100 Gi | `/home/local-path-provisioner/stardata-fe-meta-stardata-fe-{N}_prod` |
| stardata-be-{0,1,2} | `be-storage-stardata-be-{N}` | 400 Gi | `/home/local-path-provisioner/stardata-be-storage-stardata-be-{N}_prod` |

All PVCs use `storageClassName: local-path` with `persistentVolumeReclaimPolicy: Retain`.

### Replication

With 3 BE nodes and `default_replication_num = 3`, every tablet has one copy on every BE. The cluster tolerates the **complete loss of any single node** with no data loss and no query degradation.

---

## 2. Access & Endpoints

| Purpose | Address | Service |
|---|---|---|
| FE Web UI | `http://192.168.1.50:30030` | NodePort 30030 → :8030 |
| MySQL / JDBC query | `192.168.1.50:32530` | NodePort 32530 → :9030 |
| FE RPC (internal) | NodePort 31111 → :9020 | internal |
| FE Edit Log (internal) | NodePort 31364 → :9010 | internal |
| JDBC driver HTTP | `http://stardata-fe.prod.svc.cluster.local:8181` | ClusterIP (busybox httpd) |
| BE health / metrics | `http://<be-pod-ip>:8040` | ClusterIP only |

### FE headless DNS (for BdbJE, pod-to-pod)

```
stardata-fe-0.stardata-fe-headless.prod.svc.cluster.local
stardata-fe-1.stardata-fe-headless.prod.svc.cluster.local
stardata-fe-2.stardata-fe-headless.prod.svc.cluster.local
```

### Credentials

```bash
# Read admin password from K8s secret
STARDATA_PASS="$(kubectl get secret stardata-credentials -n prod \
    -o jsonpath='{.data.admin-password}' | base64 -d)"

# Connect via MySQL CLI
mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}"

# Connect from inside the cluster
mysql -h stardata-fe.prod.svc.cluster.local -P 9030 -u root -p"${STARDATA_PASS}"
```

---

## 3. FE Configuration Reference

**ConfigMap:** `stardata-fe-config` (namespace `prod`)  
**Files managed:** `fe.conf`, `log4j2.xml`

The ConfigMap is read-only. At pod start, `copy-fe-config` init container copies both files into a writable `emptyDir`. A second init container (`patch-fe-advertise`) appends the pod's own stable FQDN as `advertised_address`.

### fe.conf parameters

| Parameter | Value | Purpose |
|---|---|---|
| `meta_dir` | `/opt/apache-doris/fe/doris-meta` | BdbJE metadata — backed by 100 Gi PVC |
| `http_port` | 8030 | Web UI and REST API |
| `query_port` | 9030 | MySQL wire protocol |
| `rpc_port` | 9020 | FE–FE and FE–BE RPC |
| `edit_log_port` | 9010 | BdbJE replication log |
| `jdbc_drivers_dir` | `/opt/apache-doris/fe/jdbc_drivers` | Served by busybox httpd on :8181 |
| `JAVA_OPTS` | `-Xmx4096m -XX:+UseMembar -XX:SurvivorRatio=8` | JDK 11 heap |
| `JAVA_OPTS_FOR_JDK_17` | `-Xmx4096m -XX:SurvivorRatio=8 --add-opens=...` | JDK 17 heap + NIO unlock |
| `enable_fqdn_mode` | `true` | BdbJE uses stable FQDN identity — required for StatefulSet HA |
| `priority_networks` | `10.244.0.0/16` | Restricts bind to pod network CIDR |
| `election_timeout_ms` | `10000` | BdbJE leader election timeout (10 s) |
| `enable_auto_partition` | `true` | Enables `AUTO PARTITION BY RANGE` globally — permanent |
| `max_auto_partition_num` | `50000` | Raised from default 2000 for high snap\_id cardinality workloads |
| `advertised_address` | *(injected per-pod)* | e.g. `stardata-fe-0.stardata-fe-headless.prod.svc.cluster.local` |

### log4j2.xml customisation

The audit loggers (`audit`, `audit.query`, `audit.slow_query`) are raised from `ERROR` to `INFO` so that DDL statements (`CREATE TABLE`, `ALTER TABLE`) appear in `fe.audit.log` with `StmtType=CREATE` / `StmtType=ALTER`. The **ddl-mirror** sidecar depends on this log level.

### FE pod resources

| | Requests | Limits |
|---|---|---|
| CPU | 500m | 2 |
| Memory | 512 Mi | 4 Gi |

**Readiness probe:** `GET /api/bootstrap :8030` · initial delay 60 s · period 15 s · failure threshold 20

---

## 4. BE Configuration Reference

**ConfigMap:** `stardata-be-config` (namespace `prod`)

### be.conf parameters

| Parameter | Value | Purpose |
|---|---|---|
| `be_port` | 9060 | BE–FE data channel |
| `webserver_port` | 8040 | Health, metrics, management API |
| `heartbeat_service_port` | 9050 | FE heartbeat to BE |
| `brpc_port` | 8060 | High-throughput BRPC (shuffle, exchange) |
| `storage_root_path` | `/opt/apache-doris/be/storage` | Backed by 400 Gi PVC |
| `enable_file_cache` | `true` | Enables BE-level segment file cache (see §11) |
| `file_cache_path` | see §11 | 200 GB cache on PVC, 50 GB per-query limit |
| `JAVA_OPTS` | `-Xmx1024m -DlogPath=.../jni.log` | JNI heap for JDBC catalog queries |

### BE startup

Entry-point sets `ulimit -n 524288`, then executes:
```
/opt/apache-doris/be_entrypoint.sh stardata-fe-headless.prod.svc.cluster.local
```
On first boot the script self-registers with the FE via the heartbeat port.

### BE pod resources

| | Requests | Limits |
|---|---|---|
| CPU | 500m | 2 |
| Memory | 1 Gi | 4 Gi |

**Startup probe:** `GET /api/health :8040` · initial 30 s · period 15 s · failure threshold 20 (330 s max window)  
**Readiness probe:** same path · no initial delay · period 15 s · failure threshold 5

### BE volumes

| Volume | Type | Pod mount path |
|---|---|---|
| `be-storage` | PVC 400 Gi | `/opt/apache-doris/be/storage` |
| `be-config-rw` | emptyDir | `/opt/apache-doris/be/conf/be.conf` (subPath) |
| `trino-plugins` | emptyDir | `/opt/apache-doris/be/plugins/connectors` |
| `snowflake-jwt` | Secret (mode 0400) | `/snowflake-jwt` (read-only) |

### BE init containers

| Container | Purpose |
|---|---|
| `copy-be-config` | Copies `be.conf` from ConfigMap into writable emptyDir |
| `seed-trino-plugins` | Copies `trino-mongodb-435` JARs from `stardata:trino-plugins-v1` image into emptyDir |

---

## 5. FE Sidecar Containers

All sidecars run in every FE pod. Those needing exclusive leader behaviour perform their own check and idle-sleep on non-leader pods.

### 5.1 ddl-mirror

**Image:** `python:3.12-alpine` · CPU: 20–100 m · Memory: 32–64 Mi

Tails `fe.audit.log` in real time. When a successful `CREATE TABLE` or `ALTER TABLE` is detected containing both a `DATETIME DEFAULT CURRENT_TIMESTAMP` column and a `DATETIME … ON UPDATE CURRENT_TIMESTAMP` column, it creates a matching empty tracking table in `cache_system`:

```sql
CREATE TABLE IF NOT EXISTS `cache_system`.`<table_name>` (
  `database_name` VARCHAR(256) NULL,
  `table_name`    VARCHAR(256) NULL,
  `<created_col>` DATETIME     NULL,
  `<updated_col>` DATETIME     NULL,
  `snap_id`       BIGINT       NULL,
  `status`        VARCHAR(32)  NULL
) UNIQUE KEY(`database_name`, `table_name`)
DISTRIBUTED BY HASH(`database_name`) BUCKETS 4
PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true");
```

Only active on `stardata-fe-0`. All other pods sleep indefinitely.

### 5.2 cooldown-policy

**Image:** `python:3.12-alpine` · CPU: 20–100 m · Memory: 32–64 Mi  
**Script:** `cooldown_policy.py` (baked into FE image at `/opt/apache-doris/fe/bin/`)

Runs every `POLL_INTERVAL_SECONDS` (default: 3600). For every MOW UNIQUE KEY table in every user database, sets `storage_cooldown_time = partition_created_at + COOLDOWN_DAYS` on partitions with no cooldown, giving MOW tables equivalent hot→cold behaviour to `sd_cold_1d`. See §10 for full details.

**Environment variables:**

| Variable | Default | Description |
|---|---|---|
| `DORIS_HOST` | `127.0.0.1` | FE host |
| `DORIS_PORT` | `9030` | FE MySQL port |
| `DORIS_USER` | `root` | FE user |
| `DORIS_PASS` | *(from secret)* | FE password |
| `COOLDOWN_DAYS` | `1` | Days before partition moves to cold tier |
| `POLL_INTERVAL_SECONDS` | `3600` | Loop cadence (seconds) |
| `LEADER_CHECK` | `true` | Only run on FE leader |

### 5.3 jdbc-httpd

**Image:** `busybox:1.36` · CPU: 10–50 m · Memory: 32–64 Mi

Runs `busybox httpd -f -p 8181 -h /jdbc-drivers`. Serves JDBC driver JARs that the BE downloads when first executing a JDBC catalog query. Drivers are seeded at pod init by `seed-jdbc-drivers`.

| JAR file | Catalog used by |
|---|---|
| `snowflake-jdbc-4.0.2.jar` | `snowflake` |
| `ojdbc11.jar` | `oracle_cache_testing` |
| `postgresql-42.7.4.jar` | `pg_cache_testing` |
| `mongodb-jdbc-2.0.2-all.jar` | `mongodb` (fallback) |
| `databricks-jdbc-2.6.36.1070.jar` | `databricks` |

### 5.4 FE init containers

| Container | Purpose |
|---|---|
| `copy-fe-config` | Copies `fe.conf` + `log4j2.xml` from ConfigMap into writable emptyDir |
| `patch-fe-advertise` | Appends `advertised_address = <pod-fqdn>` using `$POD_NAME` env var |
| `seed-cooldown-script` | Copies `cooldown_policy.py` from FE image into `cooldown-scripts` emptyDir |
| `seed-jdbc-drivers` | Copies all JDBC JARs from FE image layer into `jdbc-drivers` emptyDir |
| `seed-trino-plugins` | Copies `trino-mongodb-435` JARs into `trino-plugins` emptyDir |

---

## 6. FE Source Patches

The FE image is built with a two-stage Dockerfile (`stardata/Dockerfile.fe`).

**Stage 1 (builder):** Recompiles patched `.java` files against the existing `doris-fe.jar` classpath, then uses `jar uf` to replace the compiled classes in-place.  
**Stage 2 (runtime):** Copies the patched JAR into a clean base image.

### Patched files

| File | Doris source path | Change |
|---|---|---|
| `CreateTableInfo.java` | `...nereids/trees/plans/commands/info/` | Auto-injection of Stardata features (§7) |
| `JdbcResource.java` | `...catalog/` | Adds `JDBC_SNOWFLAKE`, `JDBC_DATABRICKS` constants |
| `JdbcClient.java` | `...datasource/jdbc/client/` | Adds `SNOWFLAKE` and `DATABRICKS` cases in `createJdbcClient()` |
| `JdbcSnowflakeClient.java` | `...datasource/jdbc/client/` | Snowflake → Doris type mapping |
| `JdbcDatabricksClient.java` | `...datasource/jdbc/client/` | Databricks/Spark SQL type mapping, PAT auth |
| `JdbcMongoDBClient.java` | `...datasource/jdbc/client/` | MongoDB JDBC type mapping |
| `JdbcTable.java` | `...datasource/jdbc/` | Table metadata for new JDBC types |
| `Column.java` | `...catalog/` | Column model support for injection |
| `ColumnDef.java` | `...analysis/` | Column definition support |
| `ColumnDefinition.java` | `...nereids/trees/plans/commands/info/` | Column definition support |

### Rebuilding the FE image

```bash
# 1. Make source changes in stardata/doris-patch/
# 2. Build
docker build -f stardata/Dockerfile.fe \
  -t 192.168.1.50:30500/apache/stardata:fe-<new-version> \
  stardata/

# 3. Push
docker push 192.168.1.50:30500/apache/stardata:fe-<new-version>

# 4. Update image tag in manifests/stardata/stardata-fe-deployment.yaml
#    (containers[0].image and all seed-* initContainers that use the FE image)

# 5. Rolling restart
kubectl rollout restart statefulset/stardata-fe -n prod
kubectl rollout status  statefulset/stardata-fe -n prod --timeout=300s
```

---

## 7. Auto-Injection at CREATE TABLE

All injection is performed by `injectStardataFeatures()` in `CreateTableInfo.java`, called from `validate()` after the database name is fully resolved. Every injection uses `putIfAbsent` — user-supplied values are **never overwritten**.

### Eligibility — a table is eligible when ALL of the following are true

- Engine is `OLAP` (internal catalog)
- Not an external engine table (Iceberg engine, Hive, JDBC, …)
- Not a CTAS statement (columns list is not yet available)
- Database is not `__internal_schema` or `information_schema`
- For **regular** (non-iceberg) tables: database is also not `cache_system`

### Injection matrix

| Feature | Regular table | Iceberg-type table |
|---|---|---|
| `snap_id BIGINT NOT NULL AUTO_INCREMENT` | — | ✓ (position 0, sole UNIQUE KEY) |
| `created_at DATETIMEV2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP` | ✓ (appended) | ✓ (after key cols) |
| `updated_at DATETIMEV2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP` | — | ✓ (appended) |
| Force `UNIQUE KEY(snap_id, created_at)` + MOW | — | ✓ |
| `AUTO PARTITION BY RANGE (date_trunc('day', created_at))` | ✓ | ✓ |
| `DISTRIBUTED BY HASH(snap_id) BUCKETS AUTO` | — | ✓ |
| `DISTRIBUTED BY RANDOM BUCKETS AUTO` | ✓ (no UNIQUE KEY) | — |
| `storage_policy = sd_cold_1d` | ✓ | — (Doris MOW restriction) |
| `enable_file_cache = true` | ✓ | ✓ |

### Special PROPERTIES consumed and stripped

| Property | Effect |
|---|---|
| `stardata.table_type = iceberg` | Triggers iceberg-type injection path. Stripped before Doris analyzers run — will not appear in `SHOW CREATE TABLE`. |
| `stardata.disable_partition = true` | Suppresses AUTO PARTITION injection only. Column injection and storage\_policy still applied. Stripped before analyzers. |

---

## 8. Partition Customization

### 8.1 Default: AUTO PARTITION BY RANGE on created\_at

Every qualifying table automatically receives:

```sql
AUTO PARTITION BY RANGE (date_trunc('day', `created_at`)) ()
```

This creates one partition per distinct calendar day of data arrival, with no manual `ALTER TABLE ... ADD PARTITION` required.

**FE config prerequisite (permanent in ConfigMap):**
```
enable_auto_partition  = true
max_auto_partition_num = 50000
```

### 8.2 Distribution strategy by table type

| Table type | Distribution |
|---|---|
| Iceberg-type (`stardata.table_type=iceberg`) | `DISTRIBUTED BY HASH(snap_id) BUCKETS AUTO` |
| Regular with no UNIQUE KEY | `DISTRIBUTED BY RANDOM BUCKETS AUTO` |
| Regular UNIQUE KEY (user-supplied dist) | User distribution kept as-is |
| Regular UNIQUE KEY (no user dist) | `DISTRIBUTED BY HASH(<first_key_col>) BUCKETS AUTO` |

### 8.3 Opt out of auto partition

Use `stardata.disable_partition = true` to suppress partition injection. The `created_at` column and `storage_policy` are still applied.

```sql
CREATE TABLE analytics.lookup (
    code  VARCHAR(32) NOT NULL,
    label VARCHAR(256)
)
PROPERTIES ("stardata.disable_partition" = "true");
-- Result: created_at column + storage_policy injected; no AUTO PARTITION
```

### 8.4 Explicitly declare your own partition

Supply a `PARTITION BY` clause — the FE patch skips partition injection when a user-declared partition (non-`UNPARTITIONED`) is already present:

```sql
CREATE TABLE analytics.events_monthly (
    event_id  INT          NOT NULL,
    event_dt  DATE         NOT NULL,
    payload   VARCHAR(256)
)
PARTITION BY RANGE(event_dt) (
    PARTITION p202611 VALUES LESS THAN ("2026-12-01"),
    PARTITION p202612 VALUES LESS THAN ("2027-01-01")
)
DISTRIBUTED BY HASH(event_id) BUCKETS 10;
-- Auto partition is NOT injected; created_at and storage_policy still are
```

### 8.5 Adding partitions manually

```sql
-- Add a single new partition
ALTER TABLE analytics.my_table
ADD PARTITION p20261201 VALUES LESS THAN ("2026-12-02");

-- Add many partitions at once
ALTER TABLE analytics.my_table
ADD PARTITIONS 30
START ("2026-12-01") END ("2026-12-31") EVERY (INTERVAL 1 DAY);
```

### 8.6 Dropping old partitions (data retention)

```sql
-- Drop a single partition
ALTER TABLE analytics.events DROP PARTITION p20261101;

-- Drop all partitions older than 90 days (run via script / CronJob)
-- List partitions first:
SHOW PARTITIONS FROM analytics.events ORDER BY PartitionName;
```

### 8.7 Partition-level storage cooldown (non-MOW tables)

```sql
-- Override cooldown on a specific partition
ALTER TABLE analytics.events
  MODIFY PARTITION p20261101
  SET ("storage_cooldown_time" = "2026-11-02 00:00:00");

-- Check current cooldown values
SHOW PARTITIONS FROM analytics.events;
-- Look at CooldownTime column
```

### 8.8 max\_auto\_partition\_num

Default is 2000 (Doris upstream). Raised to 50000 in `fe.conf`:

```sql
-- Verify at runtime
ADMIN SHOW FRONTEND CONFIG LIKE '%auto_partition%';

-- Change temporarily (lost on FE restart — update ConfigMap for permanent change)
ADMIN SET FRONTEND CONFIG ("max_auto_partition_num" = "50000");
```

---

## 9. Hot / Cold Storage Tiering

### Architecture

```
HOT tier  — local BE NVMe  (storage_root_path, 400 Gi per BE)
COLD tier — AWS S3          s3://xdatatoiceberg1/stardata-cold/  (us-east-2)
```

### Objects

| Object | Name | Detail |
|---|---|---|
| S3 storage resource | `sd_s3_cold` | Endpoint: `https://s3.us-east-2.amazonaws.com` · bucket: `xdatatoiceberg1` |
| Storage policy | `sd_cold_1d` | `cooldown_ttl = 86400` (1 day) |

### How it works

After 1 day, Doris automatically migrates segment files from local NVMe to S3. Queries against cold data read directly from S3. The segment cache (§11) can keep recently-accessed cold data warm on the local NVMe.

The policy is auto-injected onto every qualifying non-MOW OLAP table by the FE patch:
```sql
PROPERTIES ("storage_policy" = "sd_cold_1d")
```

### Applying to an existing table

```sql
ALTER TABLE analytics.events SET ("storage_policy" = "sd_cold_1d");
```

### Verifying storage objects

```sql
SHOW RESOURCES WHERE NAME = "sd_s3_cold";
SHOW STORAGE POLICY WHERE POLICY_NAME = "sd_cold_1d";
```

### Rotating S3 credentials

```sql
ALTER STORAGE RESOURCE "sd_s3_cold"
  PROPERTIES (
    "s3.access_key" = "<new-access-key>",
    "s3.secret_key" = "<new-secret-key>"
  );
```

> **Note:** S3 credentials are stored inside Doris FE metadata (BdbJE). They are injected at first setup from the K8s secret `platform-s3-credentials` via `envsubst` — they are not stored in the SQL file at rest.

### Apply script (idempotent)

```bash
bash manifests/stardata/setup/06_07_apply_storage_and_partitions.sh

# Dry-run (preview without executing)
DRY_RUN=1 bash manifests/stardata/setup/06_07_apply_storage_and_partitions.sh
```

---

## 10. Iceberg Storage Policy & Partition Cooldown (MOW Tables)

### The MOW restriction

Doris 4.x rejects `storage_policy` on any UNIQUE KEY + Merge-on-Write (MOW) table:

> *"Can not create UNIQUE KEY table that enables Merge-On-write with storage policy"*

This affects:
- All **iceberg-type** Stardata tables (force-MOW via `forceIcebergUniqueKey()`)
- Any **regular UNIQUE KEY** table on Doris 4.x (MOW is now the default)

The `sd_cold_1d` policy cannot be attached to these tables via `CREATE TABLE` or `ALTER TABLE`.

### Solution: cooldown-policy sidecar

The `cooldown-policy` sidecar runs on the FE leader pod every 3600 s. It scans all user databases for MOW UNIQUE KEY tables and sets `storage_cooldown_time` on each partition:

```
cooldown_time = partition.VisibleVersionTime + COOLDOWN_DAYS (default: 1)
```

This achieves equivalent cold-tier behaviour to `sd_cold_1d` without using the rejected storage policy mechanism.

#### What the sidecar does

```
1. Checks whether this FE is the leader (SHOW FRONTENDS)
2. Lists all user databases (excluding system DBs and cache_system)
3. For each database, finds all UNIQUE KEY + MOW tables (via SHOW CREATE TABLE)
4. For each table, lists partitions (SHOW PARTITIONS)
5. For each partition with CooldownTime = "9999-12-31" (sentinel = no cooldown):
   cooldown_dt = VisibleVersionTime + 1 day
   ALTER TABLE ... MODIFY PARTITION ... SET ("storage_cooldown_time" = "<dt>")
```

#### Checking cooldown status

```sql
-- See all partitions and their cooldown times
SHOW PARTITIONS FROM analytics.my_iceberg_table;
-- CooldownTime column: 9999-12-31 = no cooldown set yet, any other date = set by sidecar

-- Force a manual cooldown on a partition (override sidecar)
ALTER TABLE analytics.my_iceberg_table
  MODIFY PARTITION p20261101
  SET ("storage_cooldown_time" = "2026-11-02 00:00:00");
```

#### Tuning the cooldown period

Edit the `COOLDOWN_DAYS` env var in the FE StatefulSet spec:

```yaml
# In manifests/stardata/stardata-fe-deployment.yaml
- name: COOLDOWN_DAYS
  value: "7"   # changed from 1 to 7 days
```

Then apply and restart:
```bash
kubectl apply -f manifests/stardata/stardata-fe-deployment.yaml
kubectl rollout restart statefulset/stardata-fe -n prod
```

#### Tuning the poll interval

```yaml
- name: POLL_INTERVAL_SECONDS
  value: "1800"   # check every 30 minutes instead of 60
```

---

## 11. Segment Data File Cache

### How it works

The segment file cache keeps recently accessed segment files on local BE NVMe, avoiding repeated S3 reads for hot data. Two independent settings must both be true for a table's data to be cached.

### Layers

| Layer | Setting | Value | Location |
|---|---|---|---|
| BE global | `enable_file_cache` | `true` | `be.conf` |
| BE global | `file_cache_path` | JSON array (see below) | `be.conf` |
| Per-table | `enable_file_cache` | `true` | Table PROPERTIES (auto-injected) |

### be.conf cache configuration

```
enable_file_cache = true
file_cache_path = [{"path":"/opt/apache-doris/be/storage/segment_cache",
                    "total_size":214748364800,
                    "query_limit":53687091200}]
```

| Parameter | Value | Notes |
|---|---|---|
| `total_size` | 214748364800 (200 GB) | Total cache size per BE node |
| `query_limit` | 53687091200 (50 GB) | Max bytes a single query may pin |

### Physical path

```
Pod mount:   /opt/apache-doris/be/storage              (400 Gi PVC)
Cache dir:   /opt/apache-doris/be/storage/segment_cache
Host path:   /home/local-path-provisioner/stardata-be-storage-stardata-be-{N}_prod/segment_cache
```

The cache is a subdirectory of the same PVC as tablet storage — it persists across pod restarts.

### Per-table opt-out

The FE patch injects `"enable_file_cache" = "true"` for all qualifying tables. To disable caching for a specific table:

```sql
CREATE TABLE analytics.bigbatch (
    id      BIGINT NOT NULL,
    payload JSON
)
PROPERTIES ("enable_file_cache" = "false");
```

Or on an existing table:
```sql
ALTER TABLE analytics.bigbatch SET ("enable_file_cache" = "false");
```

### Checking cache statistics

```bash
# From inside the cluster — per-BE cache stats
BE_IP=$(kubectl get pod stardata-be-0 -n prod -o jsonpath='{.status.podIP}')
curl -s "http://${BE_IP}:8040/api/file_cache/stat" | python3 -m json.tool
```

```sql
-- File cache hit ratio via system table (Doris 4.x)
SELECT * FROM information_schema.file_cache_statistics;
```

---

## 12. cache\_system Database

`cache_system` is a special internal Doris database used by Stardata for bookkeeping, DDL mirroring, and cache metrics. Regular table injection is **skipped** for `cache_system`. Iceberg-type tables in `cache_system` **do** receive full injection.

### Tables

#### `cache_system.table_query_stats`

Tracks SELECT hit counts per external Iceberg table. The cache manager reads/writes this table every hour to decide warm-up scheduling.

| Column | Type | Description |
|---|---|---|
| `catalog_name` | VARCHAR(128) | Stardata catalog name |
| `db_name` | VARCHAR(256) | Namespace inside the catalog |
| `table_name` | VARCHAR(256) | Table name |
| `total_select_count` | BIGINT | Cumulative SELECTs since first observed |
| `last_select_ts` | DATETIME | Most recent SELECT timestamp |
| `select_interval_min` | DOUBLE | Rolling estimate: minutes between SELECTs |
| `warm_interval_min` | DOUBLE | `select_interval * 2/3` — warm-up cadence |
| `cache_state` | VARCHAR(16) | `WARM` / `COLD` / `WARMING` / `UNKNOWN` |

#### `cache_system.cache_eviction_log`

Append-only audit log of LRU eviction events. One row per eviction.

| Column | Description |
|---|---|
| `catalog_name`, `db_name`, `table_name` | Evicted table coordinates |
| `evicted_at` | Timestamp when COLD_DOWN was issued |
| `reason` | e.g. `"no_select_24h"` |
| `last_select_ts` | Last SELECT before eviction |

#### `cache_system.table_cache_metrics`

Per-table, per-BE cache I/O metrics written once per daemon cycle by `CacheMetricsCollector`.

| Column | Description |
|---|---|
| `be_host` | Backend node (one row per table × BE per cycle) |
| `local_scan_bytes` | Bytes served from BE NVMe cache (fast path) |
| `remote_scan_bytes` | Bytes fetched from S3 (cold path) |
| `cache_hit_pct` | `local / total × 100` |
| `avg_query_time_ms` | Average query time in this cycle window |

#### `cache_system.catalog_sync_log`

Audit log for automatic catalog registration events.

#### `cache_system.query_block_log`

Written by `CacheGuard` when a SELECT hits cold external-catalog tables. Records the query ID, cold tables list, and a human-readable retry message.

#### `cache_system.datetime_load_tracker`

Written by the DDL Audit Plugin once per qualifying `CREATE TABLE` / `ALTER TABLE`. A table qualifies if it has both a `DATETIME DEFAULT CURRENT_TIMESTAMP` column and a `DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP` column.

| Column | Description |
|---|---|
| `database_name`, `table_name` | Table coordinates (UNIQUE KEY) |
| `created_ts_col` | Column name for created-at |
| `updated_ts_col` | Column name for updated-at |
| `min_created_ts`, `min_updated_ts`, `min_snap_id` | MIN() values from actual table |
| `status` | Fixed literal `'load'` |

### Useful queries

```sql
-- Top 10 hottest external tables by SELECT count
SELECT catalog_name, db_name, table_name, total_select_count, cache_state
FROM cache_system.table_query_stats
ORDER BY total_select_count DESC
LIMIT 10;

-- Cache hit ratio by table (last cycle)
SELECT catalog_name, db_name, table_name, be_host,
       cache_hit_pct, local_scan_bytes, remote_scan_bytes, sampled_at
FROM cache_system.table_cache_metrics
ORDER BY sampled_at DESC
LIMIT 50;

-- Recent evictions
SELECT * FROM cache_system.cache_eviction_log
ORDER BY evicted_at DESC LIMIT 20;

-- Tables registered in DDL load tracker
SELECT database_name, table_name, created_ts_col, updated_ts_col, status
FROM cache_system.datetime_load_tracker
ORDER BY database_name, table_name;

-- Queries that hit cold tables
SELECT query_id, detected_at, user_name, cold_tables, message
FROM cache_system.query_block_log
ORDER BY detected_at DESC LIMIT 20;
```

---

## 13. Multi-Catalog Reference

### Catalog types in use

| Type | Mechanism | Tables visible as |
|---|---|---|
| Iceberg REST | Polaris auth proxy → OAuth2 | Iceberg tables from S3 |
| JDBC | Direct JDBC over pod network | Native source tables |
| Trino connector | trino-mongodb-435 plugin | MongoDB collections |

### 13.1 Iceberg REST catalogs (via Polaris)

All point to `polaris-auth-proxy.prod.svc.cluster.local`. The proxy auto-injects OAuth2 Bearer tokens (fetched from OpenBao) on every request.

| Catalog | Warehouse | Proxy port | Contains |
|---|---|---|---|
| `polaris` | `IcebergCatalog` | :8283 (reader) | TPC-DS SF10 and other Iceberg tables |
| `databricks` | `star_lakehouse` | :8283 (reader) | Databricks Iceberg tables |
| `postgres` | `pg_lakehouse` | :8283 (reader) | PostgreSQL → Iceberg via Spark |
| `oracle` | `ora_lakehouse` | :8283 (reader) | Oracle → Iceberg via Spark |
| `mongodb` | `mgo_lakehouse` | :8283 (reader) | MongoDB → Iceberg via Spark |

**Create all five:**
```bash
export S3_KEY="<access-key>"
export S3_SECRET="<secret-key>"
envsubst < manifests/stardata/setup/02_create_catalogs.sql \
  | mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}"
```

**Refresh all:**
```sql
REFRESH CATALOG polaris;
REFRESH CATALOG databricks;
REFRESH CATALOG postgres;
REFRESH CATALOG oracle;
REFRESH CATALOG mongodb;
```

### 13.2 Snowflake JDBC catalog

```sql
CREATE CATALOG IF NOT EXISTS snowflake PROPERTIES (
    "type"         = "jdbc",
    "user"         = "testsnowflake",
    "jdbc_url"     = "jdbc:snowflake://oqihhtj-ta50603.snowflakecomputing.com/
                       ?authenticator=snowflake_jwt
                       &private_key_file=/snowflake-jwt/rsa_key.p8
                       &warehouse=COMPUTE_WH&db=LAKEHOUSE_DB&loginTimeout=60",
    "driver_url"   = "http://stardata-fe.prod.svc.cluster.local:8181/snowflake-jdbc-4.0.2.jar",
    "driver_class" = "net.snowflake.client.jdbc.SnowflakeDriver"
);
REFRESH CATALOG snowflake;
```

**Prerequisites:** RSA private key Secret `snowflake-jwt-key` must exist and be mounted on both FE and BE pods (already configured in both StatefulSet manifests).

**Apply:**
```bash
mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}" \
  < manifests/stardata/setup/10_create_snowflake_catalog.sql
```

### 13.3 Oracle JDBC catalog

```sql
CREATE CATALOG oracle_cache_testing PROPERTIES (
    "type"         = "jdbc",
    "user"         = "CACHE_TESTING",
    "password"     = "<oracle-password>",
    "jdbc_url"     = "jdbc:oracle:thin:@oracle-xe.prod.svc.cluster.local:1521/XEPDB1",
    "driver_url"   = "http://stardata-fe.prod.svc.cluster.local:8181/ojdbc11.jar",
    "driver_class" = "oracle.jdbc.OracleDriver"
);
```

**Apply:**
```bash
ORACLE_PASS="$(kubectl get secret oracle-credentials -n prod \
    -o jsonpath='{.data.oracle-password}' | base64 -d)"
mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}" \
  -e "$(sed "s/ORACLE_PASSWORD_PLACEHOLDER/${ORACLE_PASS}/" \
        manifests/stardata/setup/11_create_oracle_catalog.sql)"
```

### 13.4 PostgreSQL JDBC catalog

```sql
CREATE CATALOG pg_cache_testing PROPERTIES (
    "type"         = "jdbc",
    "user"         = "postgres",
    "password"     = "<pg-password>",
    "jdbc_url"     = "jdbc:postgresql://postgresql.prod.svc.cluster.local:5432/cache_testing",
    "driver_url"   = "http://stardata-fe.prod.svc.cluster.local:8181/postgresql-42.7.4.jar",
    "driver_class" = "org.postgresql.Driver"
);
```

**Apply:**
```bash
PG_PASS="$(kubectl get secret postgresql-credentials -n prod \
    -o jsonpath='{.data.postgres-password}' | base64 -d)"
mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}" \
  -e "$(sed "s/PG_PASSWORD_PLACEHOLDER/${PG_PASS}/" \
        manifests/stardata/setup/12_create_postgresql_catalog.sql)"
```

### 13.5 MongoDB Trino-connector catalog

Uses the `trino-mongodb-435` Trino connector plugin (loaded from emptyDir at `/opt/apache-doris/{fe,be}/plugins/connectors/`). All property names are prefixed with `trino.` — Doris strips the prefix before passing them to the plugin.

```sql
CREATE CATALOG mongodb PROPERTIES (
    "type"                            = "trino-connector",
    "trino.connector.name"            = "mongodb",
    "trino.mongodb.connection-url"    = "mongodb://root:<password>@mongodb.prod.svc.cluster.local:27017/?authSource=admin",
    "trino.mongodb.schema-collection" = "_schema"
);
SHOW DATABASES FROM mongodb;
SHOW TABLES FROM mongodb.cache_testing;
```

### 13.6 Cross-catalog query examples

```sql
-- Join Iceberg table (from Polaris) with PostgreSQL table
SELECT p.order_id, p.amount, pg.customer_name
FROM polaris.tpcds_sf10tcl.store_sales  p
JOIN pg_cache_testing.public.customers  pg
  ON p.ss_customer_sk = pg.id
LIMIT 100;

-- Query Snowflake data from Stardata
SELECT *
FROM snowflake.EVENTS.raw_events
WHERE event_date >= '2026-11-01'
LIMIT 1000;

-- Query MongoDB collection
SELECT customer_id, SUM(total_amount) AS total
FROM mongodb.cache_testing.orders
GROUP BY customer_id
ORDER BY total DESC
LIMIT 20;

-- List all catalogs
SHOW CATALOGS;

-- Inspect a specific catalog
SHOW DATABASES FROM polaris;
SHOW TABLES FROM polaris.tpcds_sf10tcl;
DESCRIBE polaris.tpcds_sf10tcl.store_sales;
```

---

## 14. Setup Script Sequence

Run these scripts **in order** on a fresh cluster after FE and BE pods are healthy.

| Step | File | Method | Description |
|---|---|---|---|
| 01 | `01_drop_catalogs.sql` | mysql | Drops all external catalogs (**destructive** — reset only) |
| 02 | `02_create_catalogs.sql` | envsubst \| mysql | Creates 5 Iceberg REST catalogs |
| 03 | `03_create_metadata_tables.sql` | mysql | Creates `cache_system` database and all 6 tracking tables |
| 04 | `04_grant_polaris_catalog_content.sh` | bash | Grants Polaris catalog permissions |
| 05 | `05_set_replication_factor.sql` | mysql | Sets `default_replication_num = 3` |
| 06+07+08 | `06_07_apply_storage_and_partitions.sh` | bash | Creates `sd_s3_cold` + `sd_cold_1d` policy, snap\_id auto-partition config, DDL load tracker |
| 08 | `08_patch_fe_conf_auto_partition.sh` | bash | Permanently patches ConfigMap for `enable_auto_partition=true`; triggers rolling FE restart |
| 09 | `09_install_ddl_load_tracker_plugin.sh` | bash | Installs the DDL audit plugin |
| 10 | `10_create_snowflake_catalog.sql` | mysql | Creates Snowflake JDBC catalog (JWT auth) |
| 11 | `11_create_oracle_catalog.sql` | mysql | Creates Oracle JDBC catalog |
| 12 | `12_create_postgresql_catalog.sql` | mysql | Creates PostgreSQL JDBC catalog |
| 13 | `13_create_mongodb_catalog.sql` | mysql | Creates MongoDB Trino-connector catalog |

---

## 15. Step-by-Step Initial Deployment

### Step 1 — Pre-flight checks

```bash
# Verify all 6 Stardata pods are Running
kubectl get pods -n prod -l app=stardata-fe
kubectl get pods -n prod -l app=stardata-be

# Verify FE leader is elected
STARDATA_PASS="$(kubectl get secret stardata-credentials -n prod \
    -o jsonpath='{.data.admin-password}' | base64 -d)"
mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}" \
  -e "SHOW FRONTENDS\G" 2>/dev/null | grep -E "Name|IsMaster"

# Verify all 3 BEs registered
mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}" \
  -e "SHOW BACKENDS\G" 2>/dev/null | grep -E "Host|Alive"
```

### Step 2 — Set replication factor

```bash
mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}" \
  < manifests/stardata/setup/05_set_replication_factor.sql
```

### Step 3 — Create cache\_system and metadata tables

```bash
mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}" \
  < manifests/stardata/setup/03_create_metadata_tables.sql
```

### Step 4 — Apply storage policies and partition defaults

```bash
# Reads platform-s3-credentials and stardata-credentials from K8s automatically
bash manifests/stardata/setup/06_07_apply_storage_and_partitions.sh
```

### Step 5 — Permanently enable auto-partition in ConfigMap

```bash
bash manifests/stardata/setup/08_patch_fe_conf_auto_partition.sh
# This triggers a rolling restart — wait ~3 min for all 3 FE pods to cycle
```

### Step 6 — Create Iceberg catalogs

```bash
export STARDATA_PASS
export S3_KEY="$(kubectl get secret platform-s3-credentials -n prod \
    -o jsonpath='{.data.access-key}' | base64 -d)"
export S3_SECRET="$(kubectl get secret platform-s3-credentials -n prod \
    -o jsonpath='{.data.secret-key}' | base64 -d)"

envsubst < manifests/stardata/setup/02_create_catalogs.sql \
  | mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}"
```

### Step 7 — Create JDBC catalogs

```bash
# Snowflake (JWT auth — Secret must already exist)
mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}" \
  < manifests/stardata/setup/10_create_snowflake_catalog.sql

# Oracle
ORACLE_PASS="$(kubectl get secret oracle-credentials -n prod \
    -o jsonpath='{.data.oracle-password}' | base64 -d)"
mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}" \
  -e "$(sed "s/ORACLE_PASSWORD_PLACEHOLDER/${ORACLE_PASS}/" \
        manifests/stardata/setup/11_create_oracle_catalog.sql)"

# PostgreSQL
PG_PASS="$(kubectl get secret postgresql-credentials -n prod \
    -o jsonpath='{.data.postgres-password}' | base64 -d)"
mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}" \
  -e "$(sed "s/PG_PASSWORD_PLACEHOLDER/${PG_PASS}/" \
        manifests/stardata/setup/12_create_postgresql_catalog.sql)"

# MongoDB
MONGO_PASS="$(kubectl get secret mongodb-credentials -n prod \
    -o jsonpath='{.data.mongodb-root-password}' | base64 -d)"
mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}" \
  -e "$(sed "s/MONGODB_PASSWORD_PLACEHOLDER/${MONGO_PASS}/" \
        manifests/stardata/setup/13_create_mongodb_catalog.sql)"
```

### Step 8 — Verify

```bash
mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}" -e "
  SHOW CATALOGS;
  SHOW DATABASES FROM polaris;
  SHOW DATABASES FROM snowflake;
  SHOW RESOURCES WHERE NAME = 'sd_s3_cold';
  SHOW STORAGE POLICY WHERE POLICY_NAME = 'sd_cold_1d';
  ADMIN SHOW FRONTEND CONFIG LIKE '%auto_partition%';
  SELECT TABLE_NAME FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = 'cache_system';
"
```

### Step 9 — Run regression tests

```bash
mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}" --comments \
  < manifests/stardata/setup/09_test_stardata_features.sql
# Review output — all PASS/FAIL rows must show PASS
```

---

## 16. Administration Commands

### Cluster health

```sql
-- FE status and leader election
SHOW FRONTENDS\G

-- BE status and disk usage
SHOW BACKENDS\G

-- Current FE config
ADMIN SHOW FRONTEND CONFIG;
ADMIN SHOW FRONTEND CONFIG LIKE '%auto_partition%';
ADMIN SHOW FRONTEND CONFIG LIKE '%replication%';
ADMIN SHOW FRONTEND CONFIG LIKE '%storage_policy%';

-- Tablet distribution
SHOW TABLET DIAGNOSIS;
```

### Rolling restarts

```bash
# FE rolling restart (safe — BdbJE quorum with 3 pods tolerates one restarting)
kubectl rollout restart statefulset/stardata-fe -n prod
kubectl rollout status  statefulset/stardata-fe -n prod --timeout=300s

# BE rolling restart
kubectl rollout restart statefulset/stardata-be -n prod
kubectl rollout status  statefulset/stardata-be -n prod --timeout=300s
```

### Catalog management

```sql
-- List all catalogs
SHOW CATALOGS;

-- Drop and recreate a catalog
DROP CATALOG IF EXISTS snowflake;
CREATE CATALOG snowflake PROPERTIES (...);
REFRESH CATALOG snowflake;

-- List tables in a catalog
SHOW DATABASES FROM polaris;
SHOW TABLES FROM polaris.tpcds_sf10tcl;

-- Describe a table in an external catalog
DESCRIBE polaris.tpcds_sf10tcl.store_sales;
```

### Partition management

```sql
-- List partitions with cooldown status
SHOW PARTITIONS FROM analytics.my_table;

-- Drop old partitions
ALTER TABLE analytics.my_table DROP PARTITION p20260101;

-- Manually set cooldown on a partition
ALTER TABLE analytics.my_table
  MODIFY PARTITION p20261101
  SET ("storage_cooldown_time" = "2026-11-02 00:00:00");

-- Truncate partition (delete data, keep structure)
TRUNCATE TABLE analytics.my_table PARTITION (p20261101);
```

### Runtime config changes

```sql
-- Change max auto-partition count (in-memory, lost on FE restart)
ADMIN SET FRONTEND CONFIG ("max_auto_partition_num" = "100000");

-- Change default replication
ADMIN SET FRONTEND CONFIG ("default_replication_num" = "3");
```

### Table inspection

```sql
-- Inspect effective DDL (shows all auto-injected columns and properties)
SHOW CREATE TABLE analytics.my_table;

-- Check table properties
SHOW TABLE STATUS FROM analytics LIKE 'my_table';

-- Column listing
DESCRIBE analytics.my_table;
SELECT * FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'analytics' AND TABLE_NAME = 'my_table';

-- Check if file cache is enabled on a table
SHOW CREATE TABLE analytics.my_table;
-- Look for: "enable_file_cache" = "true" in PROPERTIES
```

### Cooldown-policy sidecar logs

```bash
kubectl logs -n prod stardata-fe-0 -c cooldown-policy --tail=50
kubectl logs -n prod stardata-fe-0 -c ddl-mirror --tail=50
```

---

## 17. Performance Tuning

### 17.1 Partition granularity

**Default:** `date_trunc('day', created_at)` → one partition per day.  
For high-volume tables, **hour-level** partitioning reduces tablet size and improves partition pruning:

```sql
-- Override auto-injection with explicit hourly partition
CREATE TABLE analytics.events_high_volume (
    event_id BIGINT NOT NULL,
    payload  JSON
)
AUTO PARTITION BY RANGE (date_trunc('hour', created_at)) ()
DISTRIBUTED BY RANDOM BUCKETS AUTO
PROPERTIES ("storage_policy" = "sd_cold_1d", "enable_file_cache" = "true");
-- Note: use stardata.disable_partition=true to suppress auto-inject, then declare manually
```

### 17.2 Bucket count

`BUCKETS AUTO` lets Doris choose. For large tables with high-concurrency writes, pin to a fixed number:

```sql
CREATE TABLE analytics.events (
    user_id VARCHAR(64) NOT NULL,
    payload JSON
)
DISTRIBUTED BY HASH(user_id) BUCKETS 32  -- instead of AUTO
PROPERTIES ("storage_policy" = "sd_cold_1d");
```

Rule of thumb: `buckets ≈ total_data_size_per_partition_GB × 3` (targeting ~100 MB per bucket).

### 17.3 Segment cache sizing

The 200 GB cache (50 GB/node) covers the hottest ~16% of a fully-loaded 400 GB BE. To adjust, edit `be.conf` in the ConfigMap:

```yaml
# In manifests/stardata/stardata-be-deployment.yaml, be.conf section:
file_cache_path = [{"path":"/opt/apache-doris/be/storage/segment_cache",
                    "total_size":322122547200,
                    "query_limit":107374182400}]
# 300 GB total, 100 GB per-query limit
```

Apply and restart BEs. The cache is rebuilt after restart — allow 5–10 min for the first warm-up cycle.

### 17.4 Cooldown timing

The default `COOLDOWN_DAYS=1` moves data to S3 after 1 day. For workloads with predictable query patterns (e.g. "always query last 7 days"):

```yaml
- name: COOLDOWN_DAYS
  value: "7"   # keep hot for 7 days before moving to S3
```

Combined with the segment cache, this means: last 7 days on local NVMe, next 200 GB of that cached on each BE, older data read from S3 as needed.

### 17.5 Catalog refresh cadence

The catalog-refresh CronJob runs every 6 hours. For catalogs with rapidly changing schemas, reduce to 1 hour:

```yaml
# In manifests/stardata/stardata-catalog-refresh-cronjobs.yaml
schedule: "0 * * * *"   # every hour
```

The table-refresh CronJob runs every 5 minutes (for the 20 hottest tables). To add more tables, edit the `--execute` block.

### 17.6 Memory allocation

FE Java heap is set to 4096 MB. For heavy DDL workloads or large BdbJE metadata:

```yaml
# In fe.conf:
JAVA_OPTS = "-Xmx8192m -XX:+UseMembar -XX:SurvivorRatio=8"
JAVA_OPTS_FOR_JDK_17 = "-Xmx8192m -XX:SurvivorRatio=8 --add-opens=java.base/java.nio=ALL-UNNAMED"
```

Also raise the `limits.memory` on the FE container accordingly.

### 17.7 Query parallelism

```sql
-- Increase parallelism per query (session level)
SET parallel_fragment_exec_instance_num = 16;

-- Check current parallelism setting
SHOW VARIABLES LIKE '%parallel%';

-- Enable runtime filter for join acceleration
SET runtime_filter_mode = GLOBAL;
```

### 17.8 Key selection for HASH distribution

For cross-catalog joins, distribute on the join key to minimize data shuffle:

```sql
-- If you frequently join analytics.orders on customer_id:
CREATE TABLE analytics.orders (
    order_id    BIGINT      NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    amount      DECIMAL(10,2)
)
DISTRIBUTED BY HASH(customer_id) BUCKETS 16
PROPERTIES ("storage_policy" = "sd_cold_1d");
```

---

## 18. Regression Testing

Run after any FE image change or patch update to verify all auto-injection rules:

```bash
export STARDATA_PASS="$(kubectl get secret stardata-credentials -n prod \
    -o jsonpath='{.data.admin-password}' | base64 -d)"

mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}" --comments \
  < manifests/stardata/setup/09_test_stardata_features.sql
```

### Tests covered

| Test | Checks |
|---|---|
| (a)/(c) Regular table | No `snap_id`, has `created_at`, `RANDOM` dist, `storage_policy=sd_cold_1d` |
| (e) UNIQUE KEY table | No `snap_id`, has `created_at`, user distribution preserved |
| (a)/(b)/(d) Iceberg-type | `snap_id` + `created_at` + `updated_at`, `HASH(snap_id)` dist, storage policy |
| (b) User-declared cols | No duplicate injection of `snap_id`, `created_at`, `updated_at` |
| (b) Iceberg in cache\_system | Full injection applied (cache\_system excluded only for regular tables) |
| (b) Regular in cache\_system | No injection at all — cache\_system regular tables skipped |
| Prop stripping | `stardata.table_type` not visible in `SHOW CREATE TABLE` |
| `disable_partition` opt-out | `created_at` and `storage_policy` still applied; no `AUTO PARTITION` |

---

## 19. Troubleshooting

### FE pod stuck in Init

```bash
# Check which init container is failing
kubectl describe pod stardata-fe-0 -n prod
kubectl logs stardata-fe-0 -n prod -c copy-fe-config
kubectl logs stardata-fe-0 -n prod -c patch-fe-advertise
```

### BdbJE quorum lost (FE pods can't elect a leader)

This happens when all 3 FE pods restart simultaneously. `podManagementPolicy: Parallel` means they all start at once; BdbJE requires a quorum of 2/3 to start.

```bash
# Check FE logs for election errors
kubectl logs stardata-fe-0 -n prod -c stardata-fe | grep -i "election\|leader\|master"

# If stuck: delete the pod with the stale BdbJE state and let it rejoin
kubectl delete pod stardata-fe-2 -n prod
```

### BE not registering with FE

```bash
# Check BE logs
kubectl logs stardata-be-0 -n prod -c stardata-be | tail -50

# Verify FE headless service resolves from BE pod
kubectl exec -n prod stardata-be-0 -- \
  nslookup stardata-fe-headless.prod.svc.cluster.local
```

### Storage policy creation fails

```sql
-- Verify storage policy feature is enabled (must be true)
ADMIN SHOW FRONTEND CONFIG LIKE '%enable_storage_policy%';
-- If false, it must be set in fe.conf (not runtime-mutable):
-- enable_storage_policy = true
```

### Auto-partition not working

```sql
-- Check if enable_auto_partition is true
ADMIN SHOW FRONTEND CONFIG LIKE '%auto_partition%';
-- If false: run bash manifests/stardata/setup/08_patch_fe_conf_auto_partition.sh
```

### Cooldown not being applied to MOW partitions

```bash
# Check cooldown-policy sidecar logs on the leader pod
kubectl logs -n prod stardata-fe-0 -c cooldown-policy --tail=100
# Look for: "Stardata cooldown sidecar starting" and "Cycle complete"
# If not leader: check stardata-fe-1 and stardata-fe-2
```

### JDBC catalog query fails (driver not found)

```bash
# Verify jdbc-httpd sidecar is running
kubectl get pod stardata-fe-0 -n prod -o jsonpath='{.status.containerStatuses[*].name}'

# Test driver is reachable from BE
kubectl exec -n prod stardata-be-0 -- \
  wget -q -O /dev/null \
  http://stardata-fe.prod.svc.cluster.local:8181/snowflake-jdbc-4.0.2.jar \
  && echo "OK" || echo "FAIL"
```

### MongoDB catalog shows no tables

```bash
# Check Trino plugin is loaded
kubectl exec -n prod stardata-be-0 -- \
  ls /opt/apache-doris/be/plugins/connectors/trino-mongodb-435/ | head -5

# Refresh catalog
mysql -h 192.168.1.50 -P 32530 -u root -p"${STARDATA_PASS}" \
  -e "REFRESH CATALOG mongodb;"
```

### Segment cache not being used

1. Check BE global setting: `enable_file_cache = true` in `be.conf`
2. Check per-table setting: `SHOW CREATE TABLE <tbl>` → look for `"enable_file_cache" = "true"` in PROPERTIES
3. For existing tables that pre-date the FE patch: `ALTER TABLE analytics.my_table SET ("enable_file_cache" = "true");`
4. Check cache directory exists: `kubectl exec -n prod stardata-be-0 -- ls /opt/apache-doris/be/storage/segment_cache/`
