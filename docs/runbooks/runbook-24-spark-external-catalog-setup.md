# Runbook 24 — Spark External Catalog Setup: All Sources

| Field | Value |
|---|---|
| **Runbook ID** | RB-24 |
| **Service** | k8s-platform / starpump / spark |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-09-02 |
| **Related** | RB-15 (Snowflake), RB-21 (Databricks), RB-22 (cache_testing) |

---

## Overview

This runbook explains exactly how each external data source (Oracle, PostgreSQL, MongoDB,
Snowflake, Databricks) is wired into Spark as an **Iceberg catalog**, what components are
involved in each path, and how to connect to each source from outside Kubernetes. A final
section covers what is needed to register a brand-new instance of any source.

All five sources share the same architectural pattern:

```
Source DB                JDBC / native connector         Spark (pod)
─────────────           ─────────────────────           ────────────────────────────
Oracle / Postgres  ──►  JDBC (ojdbc11 / pg JDBC)  ──►  starpump read_batch()
MongoDB            ──►  MongoDB Spark connector    ──►  starpump read_batch()
Snowflake          ──►  Snowflake Spark connector  ──►  starpump read_batch()
Databricks         ──►  Simba JDBC driver          ──►  starpump read_batch()
                                                              │
                                                              ▼
                                                   IcebergTableBuilder
                                                              │
                                                   Polaris REST catalog  ──►  S3
                                                   (one warehouse per source)
```

**Two types of catalog exist in this platform — do not confuse them:**

| Catalog type | Purpose | Config location |
|---|---|---|
| **Source connector** (JDBC / Mongo connector) | Reads rows from the source database | `bao_spark_init.py` connector options, credentials from OpenBao |
| **Target Iceberg catalog** (Polaris REST) | Writes Iceberg tables to S3 | `bao_spark_init.spark_conf()`, one entry per source name |

The **target** catalog for every source is Apache Polaris. Each source gets its own named
Polaris warehouse (e.g. `ora_lakehouse`, `pg_lakehouse`) and a matching Spark catalog alias
(`oracle`, `postgres`, etc.) that starpump uses to write Iceberg tables.

---

## Component map (all sources)

| Component | File | Role |
|---|---|---|
| **OpenBao** | `http://openbao.prod.svc.cluster.local:8200` | Stores all credentials; accessed at runtime via K8s SA JWT or `TOKEN` env-var |
| **BaoSparkInit** | [`docker/spark-gluten-velox/scripts/bao_spark_init.py`](../../docker/spark-gluten-velox/scripts/bao_spark_init.py) | Reads secrets from OpenBao; builds `SparkConf` with all catalog registrations; exposes per-source connector options |
| **starpump** | [`docker/spark-gluten-velox/scripts/starpump.py`](../../docker/spark-gluten-velox/scripts/starpump.py) | CLI entry point; validates target catalog via `catalog_credential()`; drives N-thread batched copy |
| **IcebergTableBuilder** | [`docker/spark-gluten-velox/scripts/spark_iceberg_utils.py`](../../docker/spark-gluten-velox/scripts/spark_iceberg_utils.py) | Creates Iceberg tables in Polaris; injects `snap_id` / `snap_timestamp` audit columns |
| **Apache Polaris** | `http://polaris-rest.prod.svc.cluster.local:8181/api/catalog` | Iceberg REST catalog; one warehouse per source |
| **AWS S3** | `s3://xdatatoiceberg1/` (Snowflake), `s3://stardata-<source>/` (others) | Parquet data storage |
| **Spark master** | `spark://spark-master-internal.prod.svc.cluster.local:17077` | Standalone cluster; 1 master + 4 workers; 20 cores / 24 GB total |
| **Spark image** | `192.168.1.50:30500/spark-gluten-velox:3.5.1-5` | All JARs pre-baked: Iceberg, Snowflake, JDBC drivers, MongoDB connector, Gluten/Velox |

---

## Common setup (run once per terminal session)

Every command below assumes these two variables are set:

```bash
MASTER=$(kubectl get pod -n prod -l app=spark,component=master \
  --no-headers -o custom-columns=NAME:.metadata.name | head -1)
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)
echo "Master: $MASTER   Token: ${TOKEN:0:10}..."
```

---

## Part A — Oracle external catalog

### A.1 Components involved

```
OpenBao                              Spark pod (spark-master)
secret/data/platform/oracle          ┌─────────────────────────────────────────┐
  host: oracle-xe.prod...            │ bao_spark_init.oracle_jdbc_options()    │
  port: 1521                         │   → jdbc:oracle:thin:@host:port/SID     │
  sid:  XEPDB1              ────────►│   → driver: oracle.jdbc.OracleDriver    │
  user: tpcds                        │   → ojdbc11-23.4.0.24.05.jar (baked in) │
  password: <secret>                 │                                         │
                                     │ bao_spark_init.spark_conf()             │
secret/data/platform/polaris         │   spark.sql.catalog.oracle              │
  spark_svc_id: <id>        ────────►│     type=rest                           │
  spark_svc_secret: <sec>            │     uri=polaris-rest:8181/api/catalog   │
                                     │     warehouse=ora_lakehouse             │
secret/data/platform/s3              │     credential=<svc_id>:<svc_secret>   │
  access_key / secret_key   ────────►│     s3.*=<keys>                        │
                                     └──────────────┬──────────────────────────┘
                                                    │ spark.read.format("jdbc")
                                                    ▼
                                     Oracle XE 21c pod (prod namespace)
                                     oracle-xe.prod.svc.cluster.local:1521
                                     PDB: XEPDB1 / Schema: TPCDS or CACHE_TESTING
                                                    │ Iceberg write
                                                    ▼
                                     Polaris REST → s3://xdatatoiceberg1/iceberg/ora_lakehouse/
```

### A.2 Step-by-step: how the Oracle catalog was created

**Step 1 — Oracle JDBC driver in the image**

`ojdbc11-23.4.0.24.05.jar` is baked into [`docker/spark-gluten-velox/Dockerfile`](../../docker/spark-gluten-velox/Dockerfile)
at `/opt/spark/jars/ojdbc11-23.4.0.24.05.jar`. No action needed at runtime.

```bash
# Verify it is present
kubectl exec -n prod $MASTER -c spark-master -- ls /opt/spark/jars/ojdbc11*.jar
```

**Step 2 — Oracle database user and schema**

A dedicated low-privilege user was created in XEPDB1 for starpump reads:

```sql
-- Connect as SYSDBA to XEPDB1
sqlplus sys/<password>@//localhost:1521/XEPDB1 as sysdba

CREATE USER tpcds IDENTIFIED BY TpcdsPwd123!;
GRANT CREATE SESSION TO tpcds;
GRANT SELECT ANY TABLE TO tpcds;    -- starpump needs SELECT on all source tables
GRANT SELECT ANY DICTIONARY TO tpcds;  -- needed for _ora_list_tables() / _ora_table_sizes()
```

**Step 3 — Credentials stored in OpenBao**

```bash
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod -o jsonpath='{.data.root-token}' | base64 -d)
curl -s -X POST -H "X-Vault-Token: $TOKEN" -H "Content-Type: application/json" \
  http://192.168.1.50:30820/v1/secret/data/platform/oracle \
  -d '{
    "data": {
      "host":     "oracle-xe.prod.svc.cluster.local",
      "port":     "1521",
      "sid":      "XEPDB1",
      "jdbc_url": "jdbc:oracle:thin:@oracle-xe.prod.svc.cluster.local:1521/XEPDB1",
      "user":     "tpcds",
      "password": "TpcdsPwd123!",
      "schema":   "TPCDS"
    }
  }'
```

**Step 4 — Polaris warehouse created**

One Polaris warehouse (`ora_lakehouse`) was created to hold all Oracle → Iceberg tables.
The warehouse maps to an S3 location and is linked to the Polaris service account via OAuth2:

```bash
# Get Polaris OAuth2 token
POLARIS_CRED=$(curl -sf -H "X-Vault-Token: $TOKEN" \
  http://192.168.1.50:30820/v1/secret/data/platform/polaris \
  | python3 -c "import sys,json; d=json.load(sys.stdin)['data']['data']; print(d['spark_svc_id']+':'+d['spark_svc_secret'])")

POLARIS_TOKEN=$(curl -sf -X POST \
  http://192.168.1.50:30183/api/catalog/v1/oauth/tokens \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=${POLARIS_CRED%%:*}&client_secret=${POLARIS_CRED##*:}&scope=PRINCIPAL_ROLE:ALL" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# Create ora_lakehouse warehouse in Polaris
curl -sf -X POST \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "Content-Type: application/json" \
  http://192.168.1.50:30183/api/management/v1/catalogs \
  -d '{"name":"ora_lakehouse","type":"INTERNAL","storageConfigInfo":{"storageType":"S3","allowedLocations":["s3://xdatatoiceberg1/iceberg/ora_lakehouse"]}}'
```

**Step 5 — Spark catalog registered in `spark_conf()`**

[`bao_spark_init.spark_conf()`](../../docker/spark-gluten-velox/scripts/bao_spark_init.py) wires the `oracle` Spark catalog alias to the `ora_lakehouse` Polaris warehouse:

```python
conf.set("spark.sql.catalog.oracle",                  "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.oracle.type",             "rest")
conf.set("spark.sql.catalog.oracle.uri",              polaris_uri)
conf.set("spark.sql.catalog.oracle.oauth2-server-uri", f"{polaris_uri}/v1/oauth/tokens")
conf.set("spark.sql.catalog.oracle.credential",       f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}")
conf.set("spark.sql.catalog.oracle.scope",            "PRINCIPAL_ROLE:ALL")
conf.set("spark.sql.catalog.oracle.warehouse",        "ora_lakehouse")
conf.set("spark.sql.catalog.oracle.rest.auth.type",   "oauth2")
conf.set("spark.sql.catalog.oracle.s3.access-key-id",     s3["access_key"])
conf.set("spark.sql.catalog.oracle.s3.secret-access-key", s3["secret_key"])
# ... s3 endpoint / region / path-style
```

**Step 6 — starpump catalog pre-flight guard**

Every starpump run calls `bao.catalog_credential("oracle", conf)` before opening a Spark
session. If `spark.sql.catalog.oracle.credential` is absent in the conf, the job exits
immediately with a clear error — no cluster resources are allocated.

### A.3 Run starpump for Oracle

```bash
# TPCDS schema (small — 10 tables, max 3 rows each)
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=XEPDB1 SCHEMAS=TPCDS \
  starpump oracle

# CACHE_TESTING schema (large — 6 tables, ~19 M rows total)
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=XEPDB1 SCHEMAS=CACHE_TESTING \
  starpump oracle
```

> **Note:** Oracle schema names are case-sensitive in JDBC. Always pass `SCHEMAS=TPCDS`
> or `SCHEMAS=CACHE_TESTING` in uppercase — lowercase will find no tables.

---

## Part B — PostgreSQL external catalog

### B.1 Components involved

```
OpenBao                              Spark pod
secret/data/platform/postgres        ┌─────────────────────────────────────────┐
  host: postgresql.prod...           │ bao_spark_init.postgres_jdbc_options()  │
  port: 5432                         │   → jdbc:postgresql://host:5432/db      │
  database: cache_testing   ────────►│   → driver: org.postgresql.Driver       │
  user: rbac                         │   → postgresql-42.7.4.jar (baked in)    │
  password: <secret>                 │                                         │
  schema: public                     │ spark.sql.catalog.postgres              │
                                     │   type=rest / warehouse=pg_lakehouse    │
secret/data/platform/polaris ───────►│   credential=<svc_id>:<svc_secret>     │
secret/data/platform/s3      ───────►│   s3.*=<keys>                          │
                                     └──────────────┬──────────────────────────┘
                                                    │ spark.read.format("jdbc")
                                                    ▼
                                     PostgreSQL 17 (bitnami) — prod namespace
                                     postgresql.prod.svc.cluster.local:5432
                                     database: cache_testing / schema: public
                                                    │ Iceberg write
                                                    ▼
                                     Polaris REST → s3://xdatatoiceberg1/iceberg/pg_lakehouse/
```

### B.2 Step-by-step: how the PostgreSQL catalog was created

**Step 1 — PostgreSQL JDBC driver in the image**

`postgresql-42.7.4.jar` is baked into the image at `/opt/spark/jars/postgresql-42.7.4.jar`.

```bash
kubectl exec -n prod $MASTER -c spark-master -- ls /opt/spark/jars/postgresql*.jar
```

**Step 2 — PostgreSQL read user**

A dedicated `rbac` user exists with read access to the `cache_testing` database:

```sql
-- Connect as postgres superuser
GRANT CONNECT ON DATABASE cache_testing TO rbac;
GRANT USAGE ON SCHEMA public TO rbac;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO rbac;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO rbac;
```

**Step 3 — Credentials stored in OpenBao**

```bash
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod -o jsonpath='{.data.root-token}' | base64 -d)
curl -s -X POST -H "X-Vault-Token: $TOKEN" -H "Content-Type: application/json" \
  http://192.168.1.50:30820/v1/secret/data/platform/postgres \
  -d '{
    "data": {
      "host":     "postgresql.prod.svc.cluster.local",
      "port":     "5432",
      "database": "cache_testing",
      "user":     "rbac",
      "password": "<password>",
      "schema":   "public"
    }
  }'
```

**Step 4 — Polaris warehouse `pg_lakehouse` created** (same pattern as Oracle, Step A.4)

```bash
curl -sf -X POST \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "Content-Type: application/json" \
  http://192.168.1.50:30183/api/management/v1/catalogs \
  -d '{"name":"pg_lakehouse","type":"INTERNAL","storageConfigInfo":{"storageType":"S3","allowedLocations":["s3://xdatatoiceberg1/iceberg/pg_lakehouse"]}}'
```

**Step 5 — Spark catalog registered in `spark_conf()`**

```python
conf.set("spark.sql.catalog.postgres",                  "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.postgres.type",             "rest")
conf.set("spark.sql.catalog.postgres.uri",              polaris_uri)
conf.set("spark.sql.catalog.postgres.oauth2-server-uri", f"{polaris_uri}/v1/oauth/tokens")
conf.set("spark.sql.catalog.postgres.credential",       f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}")
conf.set("spark.sql.catalog.postgres.scope",            "PRINCIPAL_ROLE:ALL")
conf.set("spark.sql.catalog.postgres.warehouse",        "pg_lakehouse")
conf.set("spark.sql.catalog.postgres.rest.auth.type",   "oauth2")
# ... s3 keys
```

### B.3 Run starpump for PostgreSQL

```bash
# Full copy (all tables in public schema)
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=public \
  starpump postgres

# Single table, capped at 1 000 new rows
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=public \
  INCLUDE_TABLES=products MAX_ROWS=1000 \
  starpump postgres
```

PostgreSQL supports `LIMIT/OFFSET` pagination so starpump resumes from the last committed
row count on re-runs (no duplicate rows appended).

---

## Part C — MongoDB external catalog

### C.1 Components involved

```
OpenBao                              Spark pod
secret/data/platform/mongodb         ┌──────────────────────────────────────────────┐
  host: mongodb.prod...              │ bao_spark_init.mongodb_options()             │
  port: 27017                        │   → spark.mongodb.read.connection.uri=       │
  database: cache_testing   ────────►│       mongodb://user:pass@host:27017/db      │
  user: root                         │   → mongo-spark-connector_2.12-10.4.0-all.jar│
  password: <secret>                 │     (baked in at /opt/spark/jars/)           │
  auth_source: admin                 │                                              │
                                     │ spark.sql.catalog.mongodb                    │
secret/data/platform/polaris ───────►│   type=rest / warehouse=mgo_lakehouse       │
secret/data/platform/s3      ───────►│   credential=<svc_id>:<svc_secret>          │
                                     └──────────────┬───────────────────────────────┘
                                                    │ spark.read.format("mongodb")
                                                    │ with $limit aggregation pipeline
                                                    │ + SinglePartitionPartitioner
                                                    ▼
                                     MongoDB 8.0 — prod namespace (replica set)
                                     mongodb.prod.svc.cluster.local:27017
                                     database: cache_testing
                                                    │ Iceberg write
                                                    ▼
                                     Polaris REST → s3://xdatatoiceberg1/iceberg/mgo_lakehouse/
```

### C.2 Step-by-step: how the MongoDB catalog was created

**Step 1 — MongoDB Spark connector in the image**

`mongo-spark-connector_2.12-10.4.0-all.jar` (uber-jar) is baked into the image at
`/opt/spark/jars/mongo-spark-connector_2.12-10.4.0-all.jar`.

```bash
kubectl exec -n prod $MASTER -c spark-master -- ls /opt/spark/jars/mongo-spark*.jar
```

**Step 2 — MongoDB requires a replica set**

The MongoDB Spark connector 10.x requires a replica set (even single-node) for
change stream and oplog operations. The Helm chart is configured with:

```yaml
# helm/mongodb/values.yaml
architecture: replicaset
replicaCount: 1
```

Verify:
```bash
MONGO_POD=$(kubectl get pod -n prod -l app.kubernetes.io/name=mongodb \
  -o jsonpath='{.items[0].metadata.name}')
MONGO_PASS=$(kubectl get secret mongodb-credentials -n prod \
  -o jsonpath='{.data.mongodb-root-password}' | base64 -d)
kubectl exec -n prod $MONGO_POD -- \
  mongosh -u root -p "$MONGO_PASS" --authenticationDatabase admin \
  --eval "rs.status().ok"
# Expected: 1
```

**Step 3 — Credentials stored in OpenBao**

```bash
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod -o jsonpath='{.data.root-token}' | base64 -d)
curl -s -X POST -H "X-Vault-Token: $TOKEN" -H "Content-Type: application/json" \
  http://192.168.1.50:30820/v1/secret/data/platform/mongodb \
  -d '{
    "data": {
      "host":        "mongodb.prod.svc.cluster.local",
      "port":        "27017",
      "database":    "cache_testing",
      "user":        "root",
      "password":    "<password>",
      "auth_source": "admin"
    }
  }'
```

**Step 4 — Polaris warehouse `mgo_lakehouse` created** (same pattern as Oracle)

```bash
curl -sf -X POST \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "Content-Type: application/json" \
  http://192.168.1.50:30183/api/management/v1/catalogs \
  -d '{"name":"mgo_lakehouse","type":"INTERNAL","storageConfigInfo":{"storageType":"S3","allowedLocations":["s3://xdatatoiceberg1/iceberg/mgo_lakehouse"]}}'
```

**Step 5 — Spark catalog registered in `spark_conf()`**

```python
conf.set("spark.sql.catalog.mongodb",                  "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.mongodb.type",             "rest")
conf.set("spark.sql.catalog.mongodb.uri",              polaris_uri)
conf.set("spark.sql.catalog.mongodb.oauth2-server-uri", f"{polaris_uri}/v1/oauth/tokens")
conf.set("spark.sql.catalog.mongodb.credential",       f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}")
conf.set("spark.sql.catalog.mongodb.scope",            "PRINCIPAL_ROLE:ALL")
conf.set("spark.sql.catalog.mongodb.warehouse",        "mgo_lakehouse")
conf.set("spark.sql.catalog.mongodb.rest.auth.type",   "oauth2")
# ... s3 keys
```

**Key MongoDB-specific behaviours:**

- **No OFFSET cursor** — MongoDB does not support reliable cursor offsets. Every starpump run reads from the beginning of the collection. `supports_offset_resume=False` in the connector registry.
- **`$limit` applied server-side** — starpump builds an aggregation pipeline `[{"$limit": N}]` so MongoDB never ships more documents than requested over the wire.
- **`SinglePartitionPartitioner`** — forced whenever `batch_size < 100 000` so the `$limit` cap is honoured globally (not per-partition).
- **Schema inference** — `_mgo_table_schema()` uses `$limit: 1000` + `SinglePartitionPartitioner` to sample the schema without a full collection scan.

### C.3 Run starpump for MongoDB

```bash
# Full copy of all collections
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=cache_testing \
  starpump mongodb

# Single collection, 1 000 documents
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=cache_testing SCHEMAS=cache_testing \
  INCLUDE_TABLES=products MAX_ROWS=1000 \
  starpump mongodb
```

---

## Part D — Snowflake external catalog

### D.1 Components involved

```
OpenBao                              Spark pod
secret/data/platform/snowflake       ┌──────────────────────────────────────────────────┐
  account: <sf_account>              │ bao_spark_init.snowflake_options()               │
  user: <sf_user>                    │   → sfURL / sfUser / sfPassword / sfWarehouse     │
  password: <secret>        ────────►│   → spark-snowflake_2.12-3.2.1-spark_3.5.jar     │
  warehouse: COMPUTE_WH              │     (baked in at /opt/spark/jars/)               │
                                     │   → snowflake-jdbc-4.0.2.jar (required by 3.2.1) │
secret/data/platform/polaris ───────►│ spark.sql.catalog.polaris                        │
secret/data/platform/s3      ───────►│   warehouse=IcebergCatalog / s3.*=<keys>         │
                                     └──────────────┬───────────────────────────────────┘
                                                    │ spark.read.format(
                                                    │   "net.snowflake.spark.snowflake")
                                                    ▼
                                     Snowflake (cloud)
                                     <account>.snowflakecomputing.com
                                     SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL
                                                    │ Iceberg write
                                                    ▼
                                     Polaris REST (IcebergCatalog warehouse)
                                     → s3://xdatatoiceberg1/iceberg/tpcds_sf10tcl/
```

### D.2 Step-by-step: how the Snowflake catalog was created

**Step 1 — Snowflake Spark connector + JDBC in the image**

Two JARs are baked into the image:
- `spark-snowflake_2.12-3.2.1-spark_3.5.jar` — Snowflake Spark connector
- `snowflake-jdbc-4.0.2.jar` — required by connector 3.2.1 (JDBC 4.x API, not 3.x)

```bash
kubectl exec -n prod $MASTER -c spark-master -- ls /opt/spark/jars/spark-snowflake*.jar
kubectl exec -n prod $MASTER -c spark-master -- ls /opt/spark/jars/snowflake-jdbc*.jar
```

**Step 2 — Snowflake service account**

A Snowflake user with `USAGE` on `SNOWFLAKE_SAMPLE_DATA` and `SELECT` on
`TPCDS_SF10TCL.*` was created in the Snowflake web console. No in-cluster action needed.

**Step 3 — Credentials stored in OpenBao**

```bash
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod -o jsonpath='{.data.root-token}' | base64 -d)
curl -s -X POST -H "X-Vault-Token: $TOKEN" -H "Content-Type: application/json" \
  http://192.168.1.50:30820/v1/secret/data/platform/snowflake \
  -d '{
    "data": {
      "account":   "<your_snowflake_account>",
      "user":      "<sf_user>",
      "password":  "<sf_password>",
      "warehouse": "COMPUTE_WH",
      "s3_bucket": "xdatatoiceberg1"
    }
  }'
```

**Step 4 — Polaris `IcebergCatalog` warehouse (pre-existing)**

Snowflake writes to the default `polaris` Spark catalog backed by the `IcebergCatalog`
Polaris warehouse. This warehouse was created during initial platform setup. No new
Polaris warehouse is needed specifically for Snowflake.

**Step 5 — Spark catalog registered in `spark_conf()`**

Snowflake is a **source**, not a write target. Its Spark catalog entry is declared as a
Hadoop-type placeholder purely for starpump's pre-flight guard:

```python
conf.set("spark.sql.catalog.snowflake",           "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.snowflake.type",      "hadoop")
conf.set("spark.sql.catalog.snowflake.warehouse", "SNOWFLAKE_SAMPLE_DATA")
```

The actual Snowflake reads use `spark.read.format("net.snowflake.spark.snowflake")` with
options from `bao.snowflake_options()` — not the Iceberg catalog API.

The Iceberg **write** target is `polaris` (catalog `spark.sql.catalog.polaris`).

**Step 6 — LIMIT/OFFSET pagination and resume**

Snowflake supports reliable `LIMIT N OFFSET M ORDER BY <pk>` so starpump can resume
a partial copy. `supports_offset_resume=True` in the connector registry. On re-run,
starpump finds existing rows in Iceberg, starts at `offset=existing_count`, and appends
only new rows.

### D.3 Run starpump for Snowflake

```bash
# Full copy — all tables ≤ 3 GB in TPCDS_SF10TCL (default)
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  starpump snowflake

# Specific tables only
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  INCLUDE_TABLES=customer,store_sales,item \
  starpump snowflake

# Large tables (default 3 GB filter bypassed)
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  MAX_TABLE_SIZE_GB=20 \
  starpump snowflake
```

---

## Part E — Databricks external catalog

### E.1 Components involved

```
OpenBao                              Spark pod
secret/data/platform/databricks      ┌──────────────────────────────────────────────────┐
  host: dbc-11a1dbc5-061a...         │ bao_spark_init.databricks_jdbc_options()         │
  http_path: /sql/1.0/warehouses/... │   → jdbc:databricks://host:443                   │
  token: <PAT>              ────────►│       ;httpPath=... ;AuthMech=3 ;UID=token        │
  catalog: lakehouse                 │       ;PWD=<PAT> ;ConnCatalog=... ;SSL=1          │
  schema: lakehouse_db               │   → databricks-jdbc-2.6.36.1070.jar (Simba)      │
                                     │     (baked in at /opt/spark/jars/)               │
secret/data/platform/polaris ───────►│ spark.sql.catalog.databricks                     │
secret/data/platform/s3      ───────►│   type=rest / warehouse=star_lakehouse           │
                                     │   credential=<svc_id>:<svc_secret>               │
                                     └──────────────┬───────────────────────────────────┘
                                                    │ spark.read.format("jdbc")
                                                    │ via URLClassLoader (bypasses
                                                    │ Spark JDBC setFetchSize bug)
                                                    ▼
                                     Databricks SQL Warehouse (cloud)
                                     dbc-11a1dbc5-061a.cloud.databricks.com
                                     Unity Catalog: lakehouse.lakehouse_db
                                                    │ Iceberg write
                                                    ▼
                                     Polaris REST (star_lakehouse warehouse)
                                     → s3://stardata-databricks/iceberg/warehouse/
```

### E.2 Step-by-step: how the Databricks catalog was created

**Step 1 — Databricks Simba JDBC driver in the image**

`databricks-jdbc-2.6.36.1070.jar` is baked into the image. The binary is the 3.4.2 driver
(the filename reflects the download artifact version, not the driver version).

```bash
kubectl exec -n prod $MASTER -c spark-master -- ls /opt/spark/jars/databricks-jdbc*.jar
```

**Step 2 — Databricks Personal Access Token**

A PAT was created in the Databricks workspace under **User Settings → Developer → Access tokens**.
The token needs `CAN USE` on the SQL Warehouse and `SELECT` on `lakehouse.lakehouse_db.*`.

**Step 3 — Credentials stored in OpenBao**

```bash
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod -o jsonpath='{.data.root-token}' | base64 -d)
curl -s -X POST -H "X-Vault-Token: $TOKEN" -H "Content-Type: application/json" \
  http://192.168.1.50:30820/v1/secret/data/platform/databricks \
  -d '{
    "data": {
      "host":      "dbc-11a1dbc5-061a.cloud.databricks.com",
      "http_path": "/sql/1.0/warehouses/942026cf5e55f3c3",
      "token":     "<databricks_pat>",
      "catalog":   "lakehouse",
      "schema":    "lakehouse_db",
      "s3_bucket": "stardata-databricks"
    }
  }'
```

**Step 4 — Polaris `star_lakehouse` warehouse created**

```bash
curl -sf -X POST \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "Content-Type: application/json" \
  http://192.168.1.50:30183/api/management/v1/catalogs \
  -d '{"name":"star_lakehouse","type":"INTERNAL","storageConfigInfo":{"storageType":"S3","allowedLocations":["s3://stardata-databricks/iceberg/warehouse"]}}'
```

**Step 5 — Spark catalog registered in `spark_conf()`**

```python
conf.set("spark.sql.catalog.databricks",                  "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.databricks.type",             "rest")
conf.set("spark.sql.catalog.databricks.uri",              polaris_uri)
conf.set("spark.sql.catalog.databricks.oauth2-server-uri", f"{polaris_uri}/v1/oauth/tokens")
conf.set("spark.sql.catalog.databricks.credential",       f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}")
conf.set("spark.sql.catalog.databricks.scope",            "PRINCIPAL_ROLE:ALL")
conf.set("spark.sql.catalog.databricks.warehouse",        "star_lakehouse")
conf.set("spark.sql.catalog.databricks.rest.auth.type",   "oauth2")
# ... s3 keys for stardata-databricks bucket
```

**Important — Databricks JDBC 3.4.x bug:**

The Simba driver returns column headers as the first data row when Spark calls
`setFetchSize()`. starpump's `_db_list_tables()` and `_db_table_schema()` bypass Spark's
JDBC layer entirely, using `java.sql.DatabaseMetaData` via py4j's `URLClassLoader` to
avoid this. See RB-21 §1.5 for the full smoke-test pattern.

**Step 6 — No OFFSET resume**

Databricks JDBC does not guarantee stable row order without an explicit `ORDER BY`, so
starpump performs a full re-read on every run. `supports_offset_resume=False`.

### E.3 Run starpump for Databricks

```bash
# Full copy of all tables in lakehouse.lakehouse_db
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=lakehouse SCHEMAS=lakehouse_db \
  starpump databricks

# Single table
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=lakehouse SCHEMAS=lakehouse_db \
  INCLUDE_TABLES=product \
  starpump databricks
```

---

## Part F — Connecting to each source from outside Kubernetes

All five sources expose a NodePort on the master node (`192.168.1.50`).

### F.1 Oracle

| Property | Value |
|---|---|
| NodePort | `192.168.1.50:30521` |
| JDBC URL | `jdbc:oracle:thin:@192.168.1.50:30521/XEPDB1` |
| Client tool | `sqlplus` (inside pod — see note), SQL Developer, DBeaver, Python oracledb |

> **Note — sqlplus is not on `$PATH` by default in `gvenzl/oracle-xe:21-slim`.**
> The binary lives at `/opt/oracle/product/21c/dbhomeXE/bin/sqlplus` and requires
> `ORACLE_HOME` and `LD_LIBRARY_PATH` to be set. The simplest approach is to exec
> into the Oracle pod directly. There are **two separate passwords**:
> - `oracle-password` K8s secret → **SYS/SYSTEM** password
> - `secret/data/platform/oracle` OpenBao → **tpcds** user password

```bash
# Get tpcds password from OpenBao
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)
ORA_PASS=$(curl -sf -H "X-Vault-Token: $TOKEN" \
  http://192.168.1.50:30820/v1/secret/data/platform/oracle \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['password'])")

ORACLE_POD=$(kubectl get pod -n prod -l app=oracle-xe \
  --no-headers -o custom-columns=NAME:.metadata.name | head -1)

# Interactive sqlplus session
kubectl exec -it -n prod $ORACLE_POD -- \
  bash -c "ORACLE_HOME=/opt/oracle/product/21c/dbhomeXE \
  LD_LIBRARY_PATH=/opt/oracle/product/21c/dbhomeXE/lib \
  PATH=\$PATH:/opt/oracle/product/21c/dbhomeXE/bin \
  sqlplus tpcds/${ORA_PASS}@//localhost:1521/XEPDB1"

# Non-interactive — run a query and exit
kubectl exec -n prod $ORACLE_POD -- \
  bash -c "ORACLE_HOME=/opt/oracle/product/21c/dbhomeXE \
  LD_LIBRARY_PATH=/opt/oracle/product/21c/dbhomeXE/lib \
  PATH=\$PATH:/opt/oracle/product/21c/dbhomeXE/bin \
  sqlplus -S tpcds/${ORA_PASS}@//localhost:1521/XEPDB1 << 'EOF'
SELECT table_name, num_rows FROM all_tables ORDER BY num_rows DESC NULLS LAST;
EXIT;
EOF"

# Python from the master node (no sqlplus install required)
pip3 install oracledb 2>/dev/null
python3 -c "
import oracledb
conn = oracledb.connect(user='tpcds', password='${ORA_PASS}',
                        dsn='192.168.1.50:30521/XEPDB1')
cur = conn.cursor()
cur.execute('SELECT table_name, num_rows FROM all_tables ORDER BY num_rows DESC NULLS LAST')
for row in cur: print(row)
conn.close()
"
```

### F.2 PostgreSQL

| Property | Value |
|---|---|
| NodePort | `192.168.1.50:30532` |
| JDBC URL | `jdbc:postgresql://192.168.1.50:30532/cache_testing` |
| Client tool | `psql`, pgAdmin, DBeaver |

```bash
PG_PASS=$(kubectl get secret postgresql-credentials -n prod \
  -o jsonpath='{.data.postgres-password}' | base64 -d)

psql -h 192.168.1.50 -p 30532 -U rbac -d cache_testing
# or
PGPASSWORD=$PG_PASS psql -h 192.168.1.50 -p 30532 -U rbac -d cache_testing -c "\dt"
```

### F.3 MongoDB

| Property | Value |
|---|---|
| NodePort | `192.168.1.50:30017` |
| Connection string | `mongodb://root:<password>@192.168.1.50:30017/?authSource=admin` |
| Client tool | `mongosh`, MongoDB Compass |

```bash
MONGO_PASS=$(kubectl get secret mongodb-credentials -n prod \
  -o jsonpath='{.data.mongodb-root-password}' | base64 -d)

mongosh "mongodb://root:${MONGO_PASS}@192.168.1.50:30017/?authSource=admin"

# Python (pymongo)
python3 -c "
from pymongo import MongoClient
client = MongoClient('mongodb://root:${MONGO_PASS}@192.168.1.50:30017/?authSource=admin')
print(client.list_database_names())
"
```

### F.4 Snowflake

Snowflake is a cloud service — no NodePort involved. Connect directly to the Snowflake
account endpoint from any machine with internet access.

| Property | Value |
|---|---|
| Account URL | `https://<account>.snowflakecomputing.com` |
| JDBC URL | `jdbc:snowflake://<account>.snowflakecomputing.com/?db=SNOWFLAKE_SAMPLE_DATA&schema=TPCDS_SF10TCL&warehouse=COMPUTE_WH` |
| Client tool | SnowSQL, DBeaver, Snowflake web console |

```bash
# Get credentials from OpenBao
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod -o jsonpath='{.data.root-token}' | base64 -d)
SF=$(curl -sf -H "X-Vault-Token: $TOKEN" http://192.168.1.50:30820/v1/secret/data/platform/snowflake \
  | python3 -c "import sys,json; d=json.load(sys.stdin)['data']['data']; print(d['account'], d['user'], d['password'])")

# SnowSQL
snowsql -a <account> -u <user>

# Python (snowflake-connector-python)
python3 -c "
import snowflake.connector
conn = snowflake.connector.connect(
    account='<account>', user='<user>', password='<password>',
    database='SNOWFLAKE_SAMPLE_DATA', schema='TPCDS_SF10TCL', warehouse='COMPUTE_WH'
)
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM customer')
print(cur.fetchone())
"
```

### F.5 Databricks

Databricks is a cloud service. Connect via HTTPS to the workspace URL.

| Property | Value |
|---|---|
| Workspace URL | `https://dbc-11a1dbc5-061a.cloud.databricks.com` |
| SQL Warehouse HTTP path | `/sql/1.0/warehouses/942026cf5e55f3c3` |
| Auth | Personal Access Token (PAT) |
| Client tool | Databricks web SQL editor, DBeaver, `databricks-sql-connector` |

```bash
# Get token from OpenBao
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod -o jsonpath='{.data.root-token}' | base64 -d)
DB_TOKEN=$(curl -sf -H "X-Vault-Token: $TOKEN" http://192.168.1.50:30820/v1/secret/data/platform/databricks \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['token'])")

# Python (databricks-sql-connector)
python3 -c "
from databricks import sql
conn = sql.connect(
    server_hostname='dbc-11a1dbc5-061a.cloud.databricks.com',
    http_path='/sql/1.0/warehouses/942026cf5e55f3c3',
    access_token='${DB_TOKEN}'
)
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM lakehouse.lakehouse_db.product')
print(cur.fetchone())
conn.close()
"
```

---

## Part G — Registering a new source instance

Use this checklist when adding a brand-new database (e.g. a second Oracle instance, a new
PostgreSQL server, a different Snowflake account).

### G.1 What you need before starting

| Source | Required information |
|---|---|
| **Oracle** | Host/IP, port, SID or service name, read-only username + password, schema name |
| **PostgreSQL** | Host/IP, port, database name, schema name, read-only username + password |
| **MongoDB** | Host/IP, port, database name, auth source database, username + password |
| **Snowflake** | Account identifier, username, password, virtual warehouse name, database, schema |
| **Databricks** | Workspace hostname, SQL Warehouse HTTP path, Personal Access Token, catalog, schema |

All sources additionally require:
- An S3 bucket (new or shared) for Iceberg data storage
- Polaris service account credentials (already in OpenBao at `secret/data/platform/polaris`)

### G.2 Step-by-step for any new source

**Step 1 — Choose a catalog alias**

Pick a short lowercase name (e.g. `oracle2`, `postgres_prod`, `mongo_events`). This
becomes the Spark catalog name and the starpump source name.

**Step 2 — Store credentials in OpenBao**

Store credentials under `secret/data/platform/<alias>`. Required keys by source:

```
Oracle:      host, port, sid, jdbc_url, user, password, schema
PostgreSQL:  host, port, database, user, password, schema
MongoDB:     host, port, database, user, password, auth_source
Snowflake:   account, user, password, warehouse, [s3_bucket]
Databricks:  host, http_path, token, catalog, schema, [s3_bucket]
```

```bash
curl -s -X POST -H "X-Vault-Token: $TOKEN" -H "Content-Type: application/json" \
  http://192.168.1.50:30820/v1/secret/data/platform/<alias> \
  -d '{"data": { ... }}'
```

**Step 3 — Create a Polaris warehouse**

```bash
curl -sf -X POST \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "Content-Type: application/json" \
  http://192.168.1.50:30183/api/management/v1/catalogs \
  -d "{\"name\":\"<alias>_lakehouse\",\"type\":\"INTERNAL\",\"storageConfigInfo\":{\"storageType\":\"S3\",\"allowedLocations\":[\"s3://<bucket>/iceberg/<alias>\"]}}"
```

**Step 4 — Register the Spark catalog in `bao_spark_init.spark_conf()`**

Add a block to [`bao_spark_init.spark_conf()`](../../docker/spark-gluten-velox/scripts/bao_spark_init.py)
following the exact pattern of the existing sources. The minimum required properties are:

```python
conf.set(f"spark.sql.catalog.{alias}",                  "org.apache.iceberg.spark.SparkCatalog")
conf.set(f"spark.sql.catalog.{alias}.type",             "rest")
conf.set(f"spark.sql.catalog.{alias}.uri",              polaris_uri)
conf.set(f"spark.sql.catalog.{alias}.oauth2-server-uri", f"{polaris_uri}/v1/oauth/tokens")
conf.set(f"spark.sql.catalog.{alias}.credential",       f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}")
conf.set(f"spark.sql.catalog.{alias}.scope",            "PRINCIPAL_ROLE:ALL")
conf.set(f"spark.sql.catalog.{alias}.warehouse",        f"{alias}_lakehouse")
conf.set(f"spark.sql.catalog.{alias}.rest.auth.type",   "oauth2")
conf.set(f"spark.sql.catalog.{alias}.s3.access-key-id",     s3["access_key"])
conf.set(f"spark.sql.catalog.{alias}.s3.secret-access-key", s3["secret_key"])
conf.set(f"spark.sql.catalog.{alias}.s3.endpoint",          s3["endpoint"])
conf.set(f"spark.sql.catalog.{alias}.s3.path-style-access", "true")
conf.set(f"spark.sql.catalog.{alias}.client.region",        s3["region"])
```

**Step 5 — Add a connector entry to `_CONNECTORS` in `starpump.py`**

For JDBC-based sources (Oracle, PostgreSQL, Databricks) follow the existing `_pg_*` /
`_ora_*` / `_db_*` function pattern. For document stores follow `_mgo_*`. Then add:

```python
"<alias>": _SourceConnector(
    spark_format           = "jdbc",          # or "mongodb"
    build_opts             = _<alias>_build_opts,
    list_tables            = _<alias>_list_tables,
    table_schema           = _<alias>_table_schema,
    table_sizes            = _<alias>_table_sizes,
    capture_ts             = _<alias>_capture_ts,
    read_batch             = _<alias>_read_batch,
    s3_prefix              = "iceberg/<alias>",
    default_database       = "<default_db>",
    default_schema         = "<default_schema>",
    default_catalog        = "<alias>",
    map_schema             = _<alias>_map_schema,
    supports_offset_resume = True,   # False for MongoDB-like sources
),
```

**Step 6 — Deploy updated scripts to the pod**

```bash
MASTER=$(kubectl get pod -n prod -l app=spark,component=master \
  --no-headers -o custom-columns=NAME:.metadata.name | head -1)

kubectl cp docker/spark-gluten-velox/scripts/bao_spark_init.py \
  prod/$MASTER:/opt/spark/work-dir/bao_spark_init.py -c spark-master

kubectl cp docker/spark-gluten-velox/scripts/starpump.py \
  prod/$MASTER:/opt/spark/work-dir/starpump.py -c spark-master
```

**Step 7 — Smoke test**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env USER=dave TOKEN=$TOKEN \
  DATABASE=<db> SCHEMAS=<schema> \
  INCLUDE_TABLES=<one_small_table> MAX_ROWS=100 \
  starpump <alias>
```

**Step 8 — Rebuild the image (permanent)**

The `kubectl cp` above is ephemeral — it is lost on pod restart. Once the new connector
is tested, rebuild and push the image:

```bash
bash docker/spark-gluten-velox/build-and-push.sh
# Then roll the deployment to pick up the new image
kubectl rollout restart deployment/spark-master deployment/spark-worker \
  deployment/spark-worker-large -n prod
```

---

## Quick reference: currently registered catalogs

| Spark alias | Polaris warehouse | S3 bucket / prefix | Source connector | JAR |
|---|---|---|---|---|
| `polaris` | `IcebergCatalog` | `xdatatoiceberg1/iceberg/tpcds_sf10tcl` | Snowflake Spark connector | `spark-snowflake_2.12-3.2.1-spark_3.5.jar` + `snowflake-jdbc-4.0.2.jar` |
| `databricks` | `star_lakehouse` | `stardata-databricks/iceberg/warehouse` | Simba JDBC | `databricks-jdbc-2.6.36.1070.jar` |
| `postgres` | `pg_lakehouse` | `xdatatoiceberg1/iceberg/pg_lakehouse` | PostgreSQL JDBC | `postgresql-42.7.4.jar` |
| `oracle` | `ora_lakehouse` | `xdatatoiceberg1/iceberg/ora_lakehouse` | Oracle JDBC thin | `ojdbc11-23.4.0.24.05.jar` |
| `mongodb` | `mgo_lakehouse` | `xdatatoiceberg1/iceberg/mgo_lakehouse` | MongoDB Spark connector | `mongo-spark-connector_2.12-10.4.0-all.jar` |
