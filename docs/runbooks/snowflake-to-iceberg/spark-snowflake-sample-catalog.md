# Spark `snowflake` Catalog — Setup & Reference

**Purpose:** Document how the `snowflake` Spark catalog was created, what it does, why it exists, and how to reproduce or extend it.

---

## What it is

`snowflake` is a **Spark SQL catalog** entry that gives `starpump` a named namespace to validate against before starting a copy run. It is **not** a live query catalog — Spark does not read Snowflake tables through it. Actual data reads go through the **Snowflake Spark connector** (`net.snowflake.spark.snowflake` format) using credentials from OpenBao.

```
starpump snowflake           ← "snowflake" selects the _sf_* source connector
  │
  ├─ bao_spark_init.py       ← builds SparkConf, registers snowflake catalog
  │    spark.sql.catalog.snowflake  (hadoop type, warehouse=SNOWFLAKE_SAMPLE_DATA)
  │
  └─ snowflake_to_iceberg.py ← reads tables via:
       spark.read.format("net.snowflake.spark.snowflake")
         .options(sfUrl, sfUser, sfPassword, sfDatabase, sfSchema, ...)
         .option("query", f"SELECT * FROM {table} LIMIT ...")
```

---

## Where it lives

The catalog is registered in **two places** that are kept in sync:

| File | Purpose |
|---|---|
| [`docker/spark-gluten-velox/Dockerfile`](../../docker/spark-gluten-velox/Dockerfile) | Baked into `spark-defaults.conf` inside the image |
| [`docker/spark-gluten-velox/scripts/bao_spark_init.py`](../../docker/spark-gluten-velox/scripts/bao_spark_init.py) | Also set programmatically in `BaoSparkInit.spark_conf()` at runtime |

The `Dockerfile` copy handles any session that bypasses `bao_spark_init.py` (e.g. raw `spark-submit`). The `bao_spark_init.py` copy is the authoritative runtime path used by `starpump`.

---

## The catalog configuration

### In `spark-defaults.conf` (baked into image)

```properties
# ── Snowflake source catalog (namespace = tpcds_sf10tcl) ─────────────────────
spark.sql.catalog.snowflake                   org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.snowflake.type              hadoop
spark.sql.catalog.snowflake.warehouse         SNOWFLAKE_SAMPLE_DATA
spark.sql.catalog.snowflake.default-namespace tpcds_sf10tcl
```

### In `bao_spark_init.py` (runtime, `spark_conf()` method)

```python
conf.set("spark.sql.catalog.snowflake",
         "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.snowflake.type", "hadoop")
conf.set("spark.sql.catalog.snowflake.warehouse",
         "SNOWFLAKE_SAMPLE_DATA")
```

---

## Why `hadoop` type and not `jdbc` or `rest`

Spark catalogs need a `type` to know how to resolve `SHOW TABLES`, `DESCRIBE`, etc. The options were:

| Type | Behaviour |
|---|---|
| `rest` | Requires a REST catalog server endpoint — Snowflake doesn't have one compatible with Iceberg REST |
| `jdbc` | Queries Snowflake metadata tables directly — adds a live connection dependency at `SparkConf` build time |
| `hadoop` | Lightweight — registers a named namespace without opening any connection; actual reads are done by the connector format |

`hadoop` was chosen because `starpump` only needs the catalog for **namespace validation** (`spark.sql.catalog.snowflake` must exist for `USE snowflake.tpcds_sf10tcl` to resolve). The Snowflake Spark connector handles all real I/O separately.

---

## JARs required (already in image)

| JAR | Path in image | Purpose |
|---|---|---|
| `spark-snowflake_2.12-3.2.1-spark_3.5.jar` | `/opt/spark/jars/` | Snowflake Spark data source format (`net.snowflake.spark.snowflake`) |
| `snowflake-jdbc-4.0.2.jar` | `/opt/spark/jars/` | JDBC transport — required by connector 3.2.1 (uses `internal.*` API from JDBC 4.x) |

Both are `COPY`'d in the [`Dockerfile`](../../docker/spark-gluten-velox/Dockerfile) from `docker/spark-gluten-velox/jars/`.

---

## OpenBao secrets required

All Snowflake credentials are read at runtime from OpenBao. No credentials are baked into the image.

**Path:** `secret/data/platform/snowflake`

| Key | Example value | Used for |
|---|---|---|
| `account` | `oqihhtj-ta50603` | JDBC URL: `<account>.snowflakecomputing.com` |
| `user` | `testsnowflake` | `sfUser` in connector options |
| `password` | `<rotated>` | `sfPassword` in connector options |
| `warehouse` | `COMPUTE_WH` | `sfWarehouse` in connector options |

### Verify secrets are present

```bash
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

curl -sf -H "X-Vault-Token: $TOKEN" \
  http://192.168.1.50:30820/v1/secret/data/platform/snowflake \
  | python3 -m json.tool | grep -E '"account"|"user"|"warehouse"'
```

Expected output:
```json
"account": "oqihhtj-ta50603",
"user": "testsnowflake",
"warehouse": "COMPUTE_WH"
```

---

## How `starpump` uses the catalog end-to-end

### Step 1 — `bao_spark_init.py` registers the catalog

When `starpump snowflake` starts, `BaoSparkInit.spark_conf()` is called. It:

1. Reads Snowflake credentials from OpenBao (`secret/platform/snowflake`)
2. Adds `spark.sql.catalog.snowflake` entries to `SparkConf`
3. Sets `sfUrl`, `sfUser`, `sfPassword`, `sfWarehouse` as Snowflake connector options (used later by `snowflake_options()`)

### Step 2 — `snowflake_to_iceberg.py` validates the namespace

```python
# _sf_list_tables() — discovers tables in the source schema
sf_opts = bao.snowflake_options(schema=SCHEMAS, database=DATABASE)
# sf_opts contains: sfUrl, sfUser, sfPassword, sfWarehouse, sfDatabase, sfSchema

# Table discovery query runs through the connector format
df = spark.read \
    .format("net.snowflake.spark.snowflake") \
    .options(**sf_opts) \
    .option("query",
            f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = UPPER('{SCHEMAS}') "
            f"AND TABLE_CATALOG = UPPER('{DATABASE}')") \
    .load()
```

### Step 3 — Copy reads use the same connector

```python
# _copy_table() — reads one table from Snowflake in batches
df = spark.read \
    .format("net.snowflake.spark.snowflake") \
    .options(**sf_opts) \
    .option("query", f"SELECT * FROM {table} LIMIT {BATCH_SIZE} OFFSET {offset}") \
    .load()
# df is then written to Iceberg via polaris catalog
df.writeTo(f"polaris.{ICEBERG_NAMESPACE}.{table}").append()
```

---

## How to add a new source database

`starpump` is designed to be source-agnostic. To add a new source (e.g. `postgres`, `mysql`):

**1. Register a new `_SourceConnector` in `snowflake_to_iceberg.py`:**

```python
_CONNECTORS: dict[str, _SourceConnector] = {
    "snowflake": _SourceConnector(
        list_tables_fn=_sf_list_tables,
        copy_fn=_sf_copy_table,
        options_fn=lambda: bao.snowflake_options(schema=SCHEMAS, database=DATABASE),
    ),
    # Add new source here:
    "postgres": _SourceConnector(
        list_tables_fn=_pg_list_tables,
        copy_fn=_pg_copy_table,
        options_fn=lambda: bao.postgres_options(schema=SCHEMAS, database=DATABASE),
    ),
}
```

**2. Add a catalog entry in `bao_spark_init.py`** (optional — only if namespace validation is needed):

```python
conf.set("spark.sql.catalog.postgres_sample", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.postgres_sample.type", "hadoop")
conf.set("spark.sql.catalog.postgres_sample.warehouse", "MY_PG_DATABASE")
```

**3. Add the secret path to `bao_spark_init.py`:**

```python
_PATH_POSTGRES = "secret/data/platform/postgres"

def postgres_options(self, schema: str, database: str) -> dict:
    pg = self._read_secret(_PATH_POSTGRES)
    return {"url": pg["jdbc_url"], "user": pg["user"], "password": pg["password"], ...}
```

**4. Run with the new source name:**

```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN="$TOKEN" ADDR="$ADDR" USER=dave \
      DATABASE=MY_PG_DATABASE SCHEMAS=MY_SCHEMA \
  starpump postgres
```

Zero changes to existing Snowflake logic required.

---

## Verifying the catalog is live on the pod

```bash
MASTER=$(kubectl get pod -n prod -l component=master \
  -o jsonpath='{.items[0].metadata.name}')

# Check spark-defaults.conf has the catalog entry
kubectl exec -n prod $MASTER -c spark-master -- \
  grep "snowflake" /opt/spark/conf/spark-defaults.conf
```

Expected:
```
spark.sql.catalog.snowflake                   org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.snowflake.type              hadoop
spark.sql.catalog.snowflake.warehouse         SNOWFLAKE_SAMPLE_DATA
spark.sql.catalog.snowflake.default-namespace tpcds_sf10tcl
```

### Run a smoke test (table discovery only, no copy)

```bash
TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

kubectl exec -n prod $MASTER -c spark-master -- \
  env TOKEN="$TOKEN" ADDR="http://openbao.prod.svc.cluster.local:8200" USER=dave \
      DATABASE=SNOWFLAKE_SAMPLE_DATA SCHEMAS=TPCDS_SF10TCL \
      DRY_RUN=1 \
  starpump snowflake
```

`DRY_RUN=1` runs table discovery and size reporting but writes nothing to Iceberg. Look for:
```
Discovered 24 tables in SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL
[size-report] reason   → 0.0 GB  (COPY)
...
DRY RUN — no tables copied.
```

---

## Rebuilding the image after changes

If you change `bao_spark_init.py` or `snowflake_to_iceberg.py`:

```bash
# 1. Sync changes to docker scripts directory (both copies must stay identical)
cp docker/spark-gluten-velox/scripts/bao_spark_init.py \
   docs/runbooks/snowflake-to-iceberg/bao_spark_init.py

# 2. Rebuild and push
bash docker/spark-gluten-velox/build-and-push.sh

# 3. Roll pods
kubectl rollout restart deployment/spark-master deployment/spark-worker -n prod
kubectl rollout status  deployment/spark-master -n prod
kubectl rollout status  deployment/spark-worker -n prod
```

If you only change `spark-defaults.conf` properties in the `Dockerfile`, the catalog entries in `bao_spark_init.py` must also be updated (and vice versa) to stay consistent.
