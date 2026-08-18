# Snowflake → Iceberg Copy Pipeline — Runbook

**Script:** `snowflake_to_iceberg.py`  
**Last verified:** 2026-08-18  
**Validated table:** `TPCDS_SF10TCL.income_band` → `polaris.tpcds_sf10tcl.income_band` (20 rows ✓)

---

## Overview

Copies tables from a Snowflake schema into Apache Iceberg via Spark, writing
Parquet files to S3 and registering metadata in the Polaris REST catalog.

```
Snowflake (SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL)
    │  spark-snowflake connector
    ▼
Spark 3.5.1  (spark-master-internal.prod:17077, 3 workers × 4 cores)
    │  Iceberg 1.9.2 REST catalog
    ▼
Polaris IcebergCatalog  →  S3 s3://xdatatoiceberg1/tpcds/<namespace>/<table>
```

Every table gets:
- `snap_id BIGINT` — epoch-ms at write time (auto-injected)
- `snap_timestamp TIMESTAMP` — wall-clock at write time (auto-injected)
- `write.target-file-size-bytes = 268435456` (256 MB, global default)
- Iceberg format-version 2, Parquet + Snappy

---

## Prerequisites

| Requirement | Detail |
|---|---|
| Running inside the cluster | Script must run on `spark-master` pod (`-c spark-master`) |
| SPARK_USER | Must be `dave` (has `can_admin_catalog=true`, `can_write_iceberg=true`) |
| OpenBao token | `BAO_TOKEN` or K8s SA JWT with role `platform-secrets-read` |
| Polaris namespace | `tpcds_sf10tcl` already exists (created on first run) |

---

## Quick Start

```bash
# 1. Get the spark-master pod name
MASTER=$(kubectl get pod -n prod -l app=spark,component=master \
  -o jsonpath='{.items[0].metadata.name}')

# 2. Get OpenBao root token (or use your own BAO_TOKEN)
BAO_TOKEN=$(kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d)

# 3. Copy the scripts onto the pod
for f in bao_spark_init.py spark_iceberg_utils.py snowflake_to_iceberg.py; do
  kubectl cp docs/runbooks/snowflake-to-iceberg/$f \
    prod/$MASTER:/opt/spark/work-dir/$f -c spark-master
done

# 4. Run — copies all tables ≤ 3 GB (default)
kubectl exec -n prod $MASTER -c spark-master -- \
  env SPARK_USER=dave BAO_TOKEN="$BAO_TOKEN" \
      BAO_ADDR="http://openbao.prod.svc.cluster.local:8200" \
  python3 /opt/spark/work-dir/snowflake_to_iceberg.py
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `SPARK_USER` | `dave` | Running user — must have Iceberg write permissions |
| `BAO_ADDR` | `http://openbao.prod.svc.cluster.local:8200` | OpenBao address |
| `BAO_TOKEN` | _(K8s SA JWT)_ | Token override for dev/bootstrap |
| `SF_DATABASE` | `SNOWFLAKE_SAMPLE_DATA` | Snowflake source database |
| `SF_SCHEMA` | `TPCDS_SF10TCL` | Snowflake source schema |
| `ICEBERG_CATALOG` | `polaris` | Target Spark/Iceberg catalog name |
| `S3_BUCKET` | _(from OpenBao)_ | Override S3 bucket |
| **`INCLUDE_TABLES`** | _(all)_ | Comma-separated allowlist — only these tables are copied |
| **`EXCLUDE_TABLES`** | _(none)_ | Comma-separated denylist — always skipped |
| `TABLES` | _(none)_ | Legacy alias for `INCLUDE_TABLES` |
| **`MAX_TABLE_SIZE_GB`** | `3.0` | Auto-exclude tables larger than this. Set `0` to disable |
| `DRY_RUN` | `0` | Set `1` to create Iceberg DDL without copying data |
| `BATCH_SIZE` | `100000` | Rows per Snowflake batch / Iceberg commit |
| `MAX_THREADS` | `8` | Parallel copy threads (one table per thread) |

---

## Table Filtering — How It Works

Filters are applied in this order before any data moves:

```
ALL TABLES discovered in Snowflake
    │
    ▼  Stage 1 — INCLUDE_TABLES (if set)
    │  Keep only tables in the explicit list.
    │  All others are dropped regardless of size.
    │
    ▼  Stage 2 — EXCLUDE_TABLES (if set)
    │  Drop tables in the denylist.
    │  Applied after the include filter.
    │
    ▼  Stage 3 — MAX_TABLE_SIZE_GB (default 3.0, disable with 0)
    │  Drop tables whose compressed Snowflake ACTIVE_BYTES > cap.
    │  Size is fetched from SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
    │  with fallback to INFORMATION_SCHEMA.TABLE_STORAGE_METRICS.
    │
    ▼  FINAL COPY LIST
```

Before copying, the pipeline prints a full inventory:

```
[size-report] call_center            →    0.1 GB  (COPY)
[size-report] catalog_sales          →   18.4 GB  (SKIP — 18.4 GB exceeds 3.0 GB limit)
[size-report] income_band            →    0.0 GB  (COPY)
[size-report] store_sales            →   22.1 GB  (SKIP — 22.1 GB exceeds 3.0 GB limit)
[size-report] web_sales              →    9.3 GB  (SKIP — 9.3 GB exceeds 3.0 GB limit)
```

---

## Common Use Cases

### Copy a single table
```bash
kubectl exec -n prod $MASTER -c spark-master -- \
  env SPARK_USER=dave BAO_TOKEN="$BAO_TOKEN" \
      BAO_ADDR="http://openbao.prod.svc.cluster.local:8200" \
      INCLUDE_TABLES=income_band \
      MAX_TABLE_SIZE_GB=0 \
  python3 /opt/spark/work-dir/snowflake_to_iceberg.py
```

### Copy a specific set of tables
```bash
INCLUDE_TABLES=customer,item,store,date_dim,promotion \
  MAX_TABLE_SIZE_GB=0 \
  ...
```

### Copy all tables under 3 GB (default — skips large fact tables)
```bash
# No INCLUDE_TABLES needed — MAX_TABLE_SIZE_GB=3.0 is the default
kubectl exec ... -- \
  env SPARK_USER=dave BAO_TOKEN="$BAO_TOKEN" \
      BAO_ADDR="http://openbao.prod.svc.cluster.local:8200" \
  python3 /opt/spark/work-dir/snowflake_to_iceberg.py
```

### Copy all tables including large fact tables (raise cap to 50 GB)
```bash
MAX_TABLE_SIZE_GB=50 ...
```

### Copy everything with no size filter at all
```bash
MAX_TABLE_SIZE_GB=0 ...
```

### Skip specific tables
```bash
EXCLUDE_TABLES=store_sales,catalog_sales,web_sales,inventory ...
```

### Combine include + exclude
```bash
# Include a batch but exclude one problem table within it
INCLUDE_TABLES=customer,item,store_sales,web_sales \
  EXCLUDE_TABLES=store_sales \
  MAX_TABLE_SIZE_GB=0 ...
```

### Dry-run (DDL only, no data)
```bash
DRY_RUN=1 ...
```

### Different Snowflake schema
```bash
SF_DATABASE=MY_DB SF_SCHEMA=MY_SCHEMA \
  ICEBERG_CATALOG=polaris \
  MAX_TABLE_SIZE_GB=5 ...
```

---

## TPC-DS TPCDS_SF10TCL Table Reference

### Tables > 3 GB — excluded by default

| Table | Approx. size | Rows (approx.) |
|---|---|---|
| `store_sales` | ~22 GB | ~29 billion |
| `catalog_sales` | ~18 GB | ~14 billion |
| `web_sales` | ~9 GB | ~7 billion |
| `inventory` | ~7 GB | ~1.3 billion |
| `web_returns` | ~3.5 GB | ~720 million |

To copy these, raise the cap:
```bash
MAX_TABLE_SIZE_GB=25 INCLUDE_TABLES=store_sales ...
```

### Tables ≤ 3 GB — copied by default

| Table | Approx. size |
|---|---|
| `customer` | ~0.5 GB |
| `customer_address` | ~0.1 GB |
| `customer_demographics` | ~0.1 GB |
| `date_dim` | < 0.1 GB |
| `household_demographics` | < 0.1 GB |
| `income_band` | < 0.1 MB ✓ **validated** |
| `item` | ~0.1 GB |
| `promotion` | < 0.1 GB |
| `reason` | < 0.1 GB |
| `ship_mode` | < 0.1 GB |
| `store` | < 0.1 GB |
| `store_returns` | ~2.8 GB |
| `time_dim` | < 0.1 GB |
| `warehouse` | < 0.1 GB |
| `web_page` | < 0.1 GB |
| `web_site` | < 0.1 GB |
| `call_center` | < 0.1 GB |
| `catalog_page` | < 0.1 GB |
| `catalog_returns` | ~1.5 GB |

---

## Resuming After Partial Run

The pipeline writes in 100k-row batches, each committed as a separate Iceberg snapshot. If a run fails mid-way:

1. The Iceberg table already exists — re-running will **not** truncate it.  
   `IcebergTableBuilder.create_table()` uses `CREATE TABLE IF NOT EXISTS`.
2. Rows already written are preserved — new batches are **appended**.
3. To start a table fresh, drop it first:
   ```sql
   DROP TABLE IF EXISTS polaris.tpcds_sf10tcl.income_band;
   ```
4. Then re-run with `INCLUDE_TABLES=income_band`.

---

## Iceberg Table Location

All tables land under the Polaris `IcebergCatalog` allowed location:

```
s3://xdatatoiceberg1/tpcds/tpcds_sf10tcl/<table_name>/
```

Example for `income_band`:
```
s3://xdatatoiceberg1/tpcds/tpcds_sf10tcl/income_band/
  data/
    snap_timestamp_day=2026-08-18/
      snap_id_bucket=3/
        00000-4-cfb34f1b-….parquet
  metadata/
    00000-….metadata.json
    snap-….avro
```

---

## Verifying in Spark

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# List tables in the namespace
spark.sql("SHOW TABLES IN polaris.tpcds_sf10tcl").show()

# Query the copied table
spark.sql("SELECT * FROM polaris.tpcds_sf10tcl.income_band").show()

# Check Iceberg snapshots
spark.sql("""
  SELECT snapshot_id, committed_at, operation, summary
  FROM polaris.tpcds_sf10tcl.income_band.snapshots
""").show(truncate=False)
```

---

## Architecture Notes

| Component | Detail |
|---|---|
| Spark master | `spark-master-internal.prod.svc.cluster.local:17077` (in-cluster, no Kerberos guard) |
| External Spark RPC | `spark-master-svc:7077` — guarded by `krb-spark-guard` sidecar (requires Kerberos token) |
| Snowflake connector | `spark-snowflake_2.12-3.2.1-spark_3.5.jar` |
| Snowflake JDBC | `snowflake-jdbc-4.0.2.jar` (4.x required by connector 3.2.1 for `internal.*` API) |
| Iceberg runtime | `iceberg-spark-runtime-3.5_2.12-1.9.2.jar` |
| S3 filesystem | `s3://` scheme aliased to `S3AFileSystem` (`hadoop-aws-3.3.4.jar`) |
| Polaris catalog | REST at `http://polaris-rest.prod.svc.cluster.local:8181/api/catalog` |
| Credentials | All from OpenBao KV v2 (`secret/data/platform/*`) — never hardcoded |
