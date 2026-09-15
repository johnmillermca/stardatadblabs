# Runbook 20 — customer Table: Iceberg Write from JupyterHub & Read in Databricks

| Field | Value |
|---|---|
| **Runbook ID** | RB-20 |
| **Service** | k8s-platform / databricks |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-09-04 (fix: UPDATEs and DELETEs not visible — replaced `read_files(data/)` glob with Iceberg snapshot resolver; `CREATE OR REPLACE VIEW` on every refresh) |

---

## 1. Overview

The `customer` Iceberg table is written by Spark Gluten on the k8s cluster and stored on S3.
This runbook covers two ways to read it:

- **Section 2** — Read from **JupyterHub** using PySpark (query via Iceberg catalog)
- **Section 3** — Read and verify from the **Databricks SQL console** (via `read_files()` and a snapshot-resolved view)
- **Section 5** — **Insert 100 more rows** from JupyterHub and see them live in Databricks (end-to-end walk-through)
- **Section 8** — **Auto-discovery notebook** — scans the entire S3 warehouse root, discovers every Iceberg table automatically, resolves the current Iceberg snapshot and rebuilds views over live files only
- **Section 9** — **DML test steps** — INSERT, UPDATE, and DELETE end-to-end verification in Databricks
- **Section 10** — **NVMe disk cache** — how to cache views into local NVMe storage to eliminate S3 round-trips

> ⚠️ **Important — refresh required after every write**
> The views built by the auto-discovery notebook list the exact parquet files from the current Iceberg snapshot.
> After any INSERT, UPDATE, or DELETE in Spark/JupyterHub, re-run **Cells 2 → 5** of `nb_multi_table_auto_reader.py`
> to refresh the view. Without this step the view reflects the previous snapshot.

```
Spark Gluten (k8s)           S3: stardata-databricks
──────────────────   ──────► iceberg/warehouse/lakehouse_db/customer/
catalog : databricks            metadata/  ← *.metadata.json (one per write)
ns      : lakehouse_db          data/      ← *.parquet (4 files, snappy)
table   : customer
rows    : 1 000
                              ▲
              IAM role: databricks-unity-catalog
              External location: stardata_databricks_iceberg

JupyterHub (PySpark)          Databricks SQL console
────────────────────          ─────────────────────────────────────────
SELECT * FROM                 SELECT * FROM
  databricks.lakehouse_db       lakehouse.lakehouse_db.vw_customer_latest
  .customer
```

---

## 2. JupyterHub — Read the customer table using PySpark

Open **`http://192.168.1.50:30888`**, log in, create a new notebook, and run the cells below in order.

> **Important:** Run every cell top-to-bottom on each new session.
> The kernel loses all variables on restart — never skip a cell.
> Always run **Cell 6 (`spark.stop()`) when finished** to release cluster cores.

---

### Cell 1 — Fetch credentials from OpenBao

Get a fresh root token from your terminal:

```bash
kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d && echo
```

Paste it below and run:

```python
import urllib.request, json, os

OPENBAO_ADDR  = "http://openbao.prod.svc.cluster.local:8200"
OPENBAO_TOKEN = "s.xxxxxxxxxxxxxxxxxxxxxxxx"   # ← paste token here

def bao(path, field):
    req = urllib.request.Request(
        f"{OPENBAO_ADDR}/v1/{path}",
        headers={"X-Vault-Token": OPENBAO_TOKEN}
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())["data"]["data"][field]

S3_KEY         = bao("secret/data/platform/s3",      "access_key")
S3_SECRET      = bao("secret/data/platform/s3",      "secret_key")
S3_ENDPOINT    = bao("secret/data/platform/s3",      "endpoint")
POLARIS_ID     = bao("secret/data/platform/polaris", "spark_svc_id")
POLARIS_SECRET = bao("secret/data/platform/polaris", "spark_svc_secret")

print("✅ Credentials loaded")
```

✅ Expected: `✅ Credentials loaded`

---

### Cell 2 — Build the Spark session

> The Polaris `credential` + `oauth2-server-uri` configs are required.
> Without them Spark gets `NotAuthorizedException` when it tries to load the catalog.
> Always stop any stale session first — `.getOrCreate()` silently returns the old
> session if you skip the stop guard, and none of the new configs take effect.

```python
from pyspark.sql import SparkSession

# Stop any stale session from a previous run
_s = SparkSession.getActiveSession()
if _s:
    _s.stop()
    print("Stopped stale session")

DRIVER_IP   = os.environ["SPARK_LOCAL_IP"]
POLARIS_URI = "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog"

spark = SparkSession.builder \
    .master("spark://spark-master-internal.prod.svc.cluster.local:17077") \
    .appName("jupyter-customer-reader") \
    .config("spark.driver.host",        DRIVER_IP) \
    .config("spark.driver.bindAddress", DRIVER_IP) \
    .config("spark.executor.memory",    "2g") \
    .config("spark.driver.memory",      "2g") \
    .config("spark.pyspark.python",        "python3.11") \
    .config("spark.pyspark.driver.python", "python3.11") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.databricks",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.databricks.type",             "rest") \
    .config("spark.sql.catalog.databricks.uri",              POLARIS_URI) \
    .config("spark.sql.catalog.databricks.oauth2-server-uri",
            f"{POLARIS_URI}/v1/oauth/tokens") \
    .config("spark.sql.catalog.databricks.credential",
            f"{POLARIS_ID}:{POLARIS_SECRET}") \
    .config("spark.sql.catalog.databricks.scope",            "PRINCIPAL_ROLE:ALL") \
    .config("spark.sql.catalog.databricks.warehouse",        "star_lakehouse") \
    .config("spark.sql.catalog.databricks.rest.auth.type",   "oauth2") \
    .config("spark.sql.catalog.databricks.s3.access-key-id",     S3_KEY) \
    .config("spark.sql.catalog.databricks.s3.secret-access-key", S3_SECRET) \
    .config("spark.sql.catalog.databricks.s3.endpoint",          S3_ENDPOINT) \
    .config("spark.sql.catalog.databricks.s3.path-style-access", "true") \
    .config("spark.sql.catalog.databricks.client.region",        "us-east-2") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key",        S3_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key",        S3_SECRET) \
    .config("spark.hadoop.fs.s3a.endpoint",          S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.plugins",                         "org.apache.gluten.GlutenPlugin") \
    .config("spark.gluten.sql.columnar.backend.lib", "velox") \
    .config("spark.memory.offHeap.enabled",          "true") \
    .config("spark.memory.offHeap.size",             "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark", spark.version, "connected —", DRIVER_IP)
```

✅ Expected: `✅ Spark 3.5.1 connected — 10.244.x.x`

---

### Cell 3 — Row count

```python
spark.sql("SELECT COUNT(*) AS total_rows FROM databricks.lakehouse_db.customer").show()
```

✅ Expected:
```
+----------+
|total_rows|
+----------+
|      1000|
+----------+
```

---

### Cell 4 — Sample rows

```python
spark.sql("""
    SELECT customer_id, full_name, city, customer_tier, salary
    FROM   databricks.lakehouse_db.customer
    ORDER  BY customer_id
    LIMIT  10
""").show(truncate=False)
```

✅ Expected:
```
+-----------+-----------------+-----------+-------------+---------+
|customer_id|full_name        |city       |customer_tier|salary   |
+-----------+-----------------+-----------+-------------+---------+
|1          |Wei Brown        |Toronto    |standard     |53721.45 |
|2          |Karen Smith      |Mexico City|platinum     |149225.25|
|3          |David Wilson     |Beijing    |standard     |45766.79 |
|4          |Richard Williams |Mexico City|gold         |180526.8 |
|5          |Linda Kowalski   |Cairo      |gold         |138066.36|
...
+-----------+-----------------+-----------+-------------+---------+
```

---

### Cell 5 — Tier distribution

```python
spark.sql("""
    SELECT customer_tier,
           COUNT(*)              AS cnt,
           ROUND(AVG(salary), 2) AS avg_salary
    FROM   databricks.lakehouse_db.customer
    GROUP  BY customer_tier
    ORDER  BY cnt DESC
""").show()
```

✅ Expected:
```
+-------------+---+----------+
|customer_tier|cnt|avg_salary|
+-------------+---+----------+
|silver       |270|115908.02 |
|platinum     |249|116659.31 |
|gold         |248|115645.87 |
|standard     |233|115137.04 |
+-------------+---+----------+
```

---

### Cell 6 — Stop the session when done ⚠️

```python
# Always run this when you are finished.
# Leaving the session open holds all cluster cores
# and blocks every other Spark job.
spark.stop()
print("✅ Session stopped — cluster cores released")
```

> If you close the browser without running this cell, the `spark-app-cleanup` CronJob
> will automatically kill the idle session after **30 minutes**.

---

## 3. Databricks SQL console — verify the view

Open **`https://dbc-48ef5678-3df7.cloud.databricks.com/sql/editor`**  
Select warehouse: **Serverless Starter Warehouse**

> The warehouse cold-starts automatically on first query. Allow 30–90 seconds.

---

### Check 1 — Schema and view exist

```sql
SHOW SCHEMAS IN lakehouse;
```
✅ `lakehouse_db` listed

> **Note:** `SHOW VIEWS IN lakehouse.lakehouse_db` does not support cross-catalog
> 3-part schema references on Serverless compute. Use the Python check below instead.

```python
# Run in a Databricks notebook attached to the same cluster / SQL warehouse
print(spark.catalog.tableExists("lakehouse.lakehouse_db.vw_customer_latest"))
```
✅ Expected: `True`

---

### Check 2 — Row count

```sql
SELECT COUNT(*) AS total_rows
FROM lakehouse.lakehouse_db.vw_customer_latest;
```
✅ Expected: `1000`

---

### Check 3 — Sample rows

```sql
SELECT customer_id, full_name, email, city, customer_tier, salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
ORDER  BY customer_id
LIMIT  10;
```
✅ Expected: rows 1–10 with correct names, cities and tiers

---

### Check 4 — Tier distribution

```sql
SELECT customer_tier,
       COUNT(*)              AS cnt,
       ROUND(AVG(salary), 2) AS avg_salary,
       ROUND(MIN(salary), 2) AS min_salary,
       ROUND(MAX(salary), 2) AS max_salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
GROUP  BY customer_tier
ORDER  BY cnt DESC;
```

✅ Expected:

| customer_tier | cnt | avg_salary |
|---|---|---|
| silver | 270 | 115908.02 |
| platinum | 249 | 116659.31 |
| gold | 248 | 115645.87 |
| standard | 233 | 115137.04 |

---

### Check 5 — Snap audit columns

```sql
SELECT customer_id, snap_id, snap_timestamp
FROM   lakehouse.lakehouse_db.vw_customer_latest
ORDER  BY customer_id
LIMIT  5;
```
✅ Expected: `snap_id` (bigint) and `snap_timestamp` (timestamp) populated on every row

---

### Check 6 — View definition

```sql
DESCRIBE EXTENDED lakehouse.lakehouse_db.vw_customer_latest;
```
Look for the **View Text** row — it must contain:
```
read_files('s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/data/', ...)
```

---

### Browse via Catalog Explorer (no SQL needed)

1. Go to **`https://dbc-48ef5678-3df7.cloud.databricks.com/explore/data`**
2. Expand **`lakehouse`** → **`lakehouse_db`**
3. Click **`vw_customer_latest`**
4. Click the **Sample Data** tab → live preview of the 1 000 rows

---

## 4. Refresh the view after a new Spark write

> **This section is superseded by Section 8.**
> The auto-discovery notebook ([`nb_multi_table_auto_reader.py`](../../docker/databricks-notebooks/nb_multi_table_auto_reader.py)) scans the entire S3 warehouse root and refreshes **all** tables in one pass — no per-table code required.
>
> **To refresh after any INSERT, UPDATE, or DELETE:** re-run **Cells 2 → 5** of the notebook.
> Cell 4 re-resolves the live file list from the current Iceberg snapshot.
> Cell 5 rebuilds each view over exactly those files (`CREATE OR REPLACE VIEW`).
> Without re-running Cell 5, the view still reflects the old snapshot.
>
> See **[Section 8 → How to run](#8-auto-discovery-notebook--all-tables-from-s3-in-one-pass)** for full steps.

---

## 5. Insert 100 more rows from JupyterHub and see them in Databricks

This section is a complete end-to-end walk-through. Starting from a fresh JupyterHub session, you will:

1. Write **100 new rows** (IDs 1 001 – 1 100) to `databricks.lakehouse_db.customer` via PySpark.
2. Refresh the Databricks view so it points at the new Iceberg snapshot.
3. Verify the 100 rows are visible in the **Databricks SQL console**.

> **Run all cells in order, top to bottom.** The kernel loses variables on restart — never skip a cell.

---

### 5-A — Fetch credentials (JupyterHub)

Get a fresh root token from any terminal that has `kubectl`:

```bash
kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.root-token}' | base64 -d && echo
```

Paste the token into the cell below and run it:

```python
import urllib.request, json, os

OPENBAO_ADDR  = "http://openbao.prod.svc.cluster.local:8200"
OPENBAO_TOKEN = "s.xxxxxxxxxxxxxxxxxxxxxxxx"   # ← paste your token here

def bao(path, field):
    req = urllib.request.Request(
        f"{OPENBAO_ADDR}/v1/{path}",
        headers={"X-Vault-Token": OPENBAO_TOKEN}
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())["data"]["data"][field]

S3_KEY         = bao("secret/data/platform/s3",      "access_key")
S3_SECRET      = bao("secret/data/platform/s3",      "secret_key")
S3_ENDPOINT    = bao("secret/data/platform/s3",      "endpoint")
POLARIS_ID     = bao("secret/data/platform/polaris", "spark_svc_id")
POLARIS_SECRET = bao("secret/data/platform/polaris", "spark_svc_secret")

print("✅ Credentials loaded")
```

✅ Expected: `✅ Credentials loaded`

---

### 5-B — Build the Spark session (JupyterHub)

```python
from pyspark.sql import SparkSession

# Stop any stale session so new configs take effect
_s = SparkSession.getActiveSession()
if _s:
    _s.stop()
    print("Stopped stale session")

DRIVER_IP   = os.environ["SPARK_LOCAL_IP"]
POLARIS_URI = "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog"

spark = SparkSession.builder \
    .master("spark://spark-master-internal.prod.svc.cluster.local:17077") \
    .appName("jupyter-customer-insert-100") \
    .config("spark.driver.host",        DRIVER_IP) \
    .config("spark.driver.bindAddress", DRIVER_IP) \
    .config("spark.executor.memory",    "2g") \
    .config("spark.driver.memory",      "2g") \
    .config("spark.pyspark.python",        "python3.11") \
    .config("spark.pyspark.driver.python", "python3.11") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.databricks",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.databricks.type",             "rest") \
    .config("spark.sql.catalog.databricks.uri",              POLARIS_URI) \
    .config("spark.sql.catalog.databricks.oauth2-server-uri",
            f"{POLARIS_URI}/v1/oauth/tokens") \
    .config("spark.sql.catalog.databricks.credential",
            f"{POLARIS_ID}:{POLARIS_SECRET}") \
    .config("spark.sql.catalog.databricks.scope",            "PRINCIPAL_ROLE:ALL") \
    .config("spark.sql.catalog.databricks.warehouse",        "star_lakehouse") \
    .config("spark.sql.catalog.databricks.rest.auth.type",   "oauth2") \
    .config("spark.sql.catalog.databricks.s3.access-key-id",     S3_KEY) \
    .config("spark.sql.catalog.databricks.s3.secret-access-key", S3_SECRET) \
    .config("spark.sql.catalog.databricks.s3.endpoint",          S3_ENDPOINT) \
    .config("spark.sql.catalog.databricks.s3.path-style-access", "true") \
    .config("spark.sql.catalog.databricks.client.region",        "us-east-2") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key",        S3_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key",        S3_SECRET) \
    .config("spark.hadoop.fs.s3a.endpoint",          S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.plugins",                         "org.apache.gluten.GlutenPlugin") \
    .config("spark.gluten.sql.columnar.backend.lib", "velox") \
    .config("spark.memory.offHeap.enabled",          "true") \
    .config("spark.memory.offHeap.size",             "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark", spark.version, "connected —", DRIVER_IP)
```

✅ Expected: `✅ Spark 3.5.1 connected — 10.244.x.x`

---

### 5-C — Confirm current row count before the insert (JupyterHub)

```python
spark.sql(
    "SELECT COUNT(*) AS total_rows FROM databricks.lakehouse_db.customer"
).show()
```

✅ Expected: `1000` (the 1 000 rows already in the table)

---

### 5-D — Generate and insert 100 new rows (JupyterHub)

> **Fix applied 2026-08-31** — The original code omitted `snap_id` and `snap_timestamp`
> from `CUSTOMER_SCHEMA` and `Row(...)`, causing:
> ```
> AnalysisException: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA]
> Cannot find data for the output column `snap_id`.
> ```
> Both columns are now included. `snap_id` defaults to `None` (the Iceberg REST
> catalog populates it during the write); `snap_timestamp` is set to the row's
> `created_at` value so the audit column is always populated.

The new rows use IDs 1 001 – 1 100 and a different random seed (`seed=99`) so the names and cities are distinct from the original batch.

```python
import datetime, hashlib, random
from pyspark.sql import Row
from pyspark.sql.types import (
    DateType, DoubleType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType,
)

# ── Schema (must match the existing customer table) ───────────────────────────
CUSTOMER_SCHEMA = StructType([
    StructField("customer_id",    IntegerType(),   nullable=False),
    StructField("full_name",      StringType(),    nullable=True),
    StructField("email",          StringType(),    nullable=True),
    StructField("phone_number",   StringType(),    nullable=True),
    StructField("date_of_birth",  DateType(),      nullable=True),
    StructField("national_id",    StringType(),    nullable=True),
    StructField("street_address", StringType(),    nullable=True),
    StructField("city",           StringType(),    nullable=True),
    StructField("country_code",   StringType(),    nullable=True),
    StructField("ip_address",     StringType(),    nullable=True),
    StructField("salary",         DoubleType(),    nullable=True),
    StructField("customer_tier",  StringType(),    nullable=True),
    StructField("is_active",      IntegerType(),   nullable=True),
    StructField("created_at",     TimestampType(), nullable=True),
    StructField("updated_at",     TimestampType(), nullable=True),
    StructField("snap_id",        LongType(),      nullable=True),      # ← fix
    StructField("snap_timestamp", TimestampType(), nullable=True),      # ← fix
])

_FIRST = ["James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda",
          "William","Barbara","David","Elizabeth","Richard","Susan","Joseph",
          "Jessica","Thomas","Sarah","Charles","Karen","Wei","Amira","Luca",
          "Sara","Arjun","Yuki","Carlos","Fatima","Ivan","Priya"]
_LAST  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
          "Wilson","Taylor","Martinez","Anderson","Thomas","Jackson","White",
          "Harris","Martin","Thompson","Chen","Patel","Nasser","Rossi","Khan",
          "Nakamura","Silva","Müller","Dubois","Kowalski","Oliveira","Hassan"]
_CITIES = [
    ("Toronto","CA"),("London","GB"),("Rome","IT"),("Cairo","EG"),("Beijing","CN"),
    ("Mumbai","IN"),("São Paulo","BR"),("Berlin","DE"),("Tokyo","JP"),("Sydney","AU"),
    ("Paris","FR"),("Seoul","KR"),("Lagos","NG"),("Buenos Aires","AR"),("Dubai","AE"),
    ("Singapore","SG"),("Istanbul","TR"),("Mexico City","MX"),("Amsterdam","NL"),
    ("Nairobi","KE"),("Cape Town","ZA"),("Bangkok","TH"),("Jakarta","ID"),
    ("Karachi","PK"),("Chicago","US"),("Los Angeles","US"),("New York","US"),
    ("Madrid","ES"),("Milan","IT"),("Hong Kong","HK"),
]
_TIERS   = ["standard","silver","gold","platinum"]
_STREETS = ["Main St","Oak Ave","Maple Rd","Pine Blvd","Cedar Ln","Elm St",
            "Park Ave","Lake Dr","River Rd","Hill Ct"]

# Use seed=99 so names/cities differ from the original seed=42 batch
rng     = random.Random(99)
base_dt = datetime.datetime(2026, 9, 1, 0, 0, 0)   # newer created_at dates
rows    = []

for i in range(1001, 1101):                          # IDs 1 001 – 1 100
    first = rng.choice(_FIRST);  last = rng.choice(_LAST)
    city, cc = rng.choice(_CITIES)
    dob  = datetime.date(rng.randint(1960,2000), rng.randint(1,12), rng.randint(1,28))
    tag  = hashlib.md5(f"{first}{last}{i}".encode()).hexdigest()[:6]
    row_created_at = base_dt + datetime.timedelta(days=rng.randint(0,90))
    rows.append(Row(
        customer_id   = i,
        full_name     = f"{first} {last}",
        email         = f"{first.lower()}.{last.lower()}.{tag}@example.com",
        phone_number  = f"+{rng.randint(1,99)}-{rng.randint(100,999)}-{rng.randint(1000,9999)}",
        date_of_birth = dob,
        national_id   = f"ID-{i:05d}-{tag.upper()[:4]}",
        street_address= f"{rng.randint(1,999)} {rng.choice(_STREETS)}",
        city          = city,
        country_code  = cc,
        ip_address    = ".".join(str(rng.randint(1,254)) for _ in range(4)),
        salary        = round(rng.uniform(30_000, 200_000), 2),
        customer_tier = rng.choice(_TIERS),
        is_active     = 1,
        created_at    = row_created_at,
        updated_at    = base_dt + datetime.timedelta(days=rng.randint(0,120)),
        snap_id       = None,           # ← fix: populated by Iceberg on commit
        snap_timestamp= row_created_at, # ← fix: audit timestamp mirrors created_at
    ))

df_new = spark.createDataFrame(rows, schema=CUSTOMER_SCHEMA)
df_new.write \
    .format("iceberg") \
    .mode("append") \
    .save("databricks.lakehouse_db.customer")

print(f"✅ Appended {len(rows)} rows (IDs 1001–1100)")
```

✅ Expected: `✅ Appended 100 rows (IDs 1001–1100)`

---

### 5-E — Verify total row count is now 1 100 (JupyterHub)

```python
spark.sql(
    "SELECT COUNT(*) AS total_rows FROM databricks.lakehouse_db.customer"
).show()
```

✅ Expected:
```
+----------+
|total_rows|
+----------+
|      1100|
+----------+
```

```python
# Confirm the new rows are visible — show IDs 1 095 – 1 100
spark.sql("""
    SELECT customer_id, full_name, city, customer_tier, salary
    FROM   databricks.lakehouse_db.customer
    WHERE  customer_id >= 1095
    ORDER  BY customer_id
""").show(truncate=False)
```

✅ Expected: 6 rows with `customer_id` 1095–1100, new names and cities.

---

### 5-F — Stop the Spark session (JupyterHub) ⚠️

```python
spark.stop()
print("✅ Session stopped — cluster cores released")
```

> Always stop before switching to Databricks. Leaving the session open holds cluster cores.

---

### 5-G — Refresh the Databricks view (Databricks notebook)

> **Superseded by Section 8.** The auto-discovery notebook refreshes **all** tables from the entire S3 warehouse root in one pass.
>
> Open [`nb_multi_table_auto_reader.py`](../../docker/databricks-notebooks/nb_multi_table_auto_reader.py) in Databricks and re-run **Cells 2 and 4**.
> The view (`vw_customer_latest`) already points at the `data/` directory — new parquet files written by the 100-row insert are visible on the next SQL query automatically, without any notebook re-run.

---

### 5-H — Verify 1 100 rows in the Databricks SQL console

Open **`https://dbc-48ef5678-3df7.cloud.databricks.com/sql/editor`**
Select warehouse: **Serverless Starter Warehouse**

**Query 1 — Total count (must now be 1 100)**

```sql
SELECT COUNT(*) AS total_rows
FROM lakehouse.lakehouse_db.vw_customer_latest;
```

✅ Expected: `1100`

---

**Query 2 — Confirm the new rows (IDs 1 095 – 1 100)**

```sql
SELECT customer_id, full_name, city, customer_tier, salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
WHERE  customer_id >= 1095
ORDER  BY customer_id;
```

✅ Expected: 6 rows with IDs 1095–1100, names and cities generated by seed 99.

---

**Query 3 — Full tier distribution across all 1 100 rows**

```sql
SELECT customer_tier,
       COUNT(*)              AS cnt,
       ROUND(AVG(salary), 2) AS avg_salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
GROUP  BY customer_tier
ORDER  BY cnt DESC;
```

✅ Expected: all four tiers present, total `cnt` values sum to **1100**.

---

**Query 4 — Verify both insert batches are present by created_at date**

```sql
SELECT
    DATE(created_at)   AS insert_date,
    MIN(customer_id)   AS first_id,
    MAX(customer_id)   AS last_id,
    COUNT(*)           AS row_count
FROM   lakehouse.lakehouse_db.vw_customer_latest
GROUP  BY DATE(created_at)
ORDER  BY insert_date;
```

✅ Expected: dates in **2026-01-xx** range for IDs 1–1 000 (seed=42, base Jan 2026)
and dates in **2026-09-xx** range for IDs 1 001–1 100 (seed=99, base Sep 2026).
This proves both batches are in the view.

---

> **Summary of the end-to-end flow**
>
> | Step | Tool | Action |
> |---|---|---|
> | 5-A | JupyterHub | Load OpenBao credentials |
> | 5-B | JupyterHub | Start Spark session |
> | 5-C | JupyterHub | Confirm count = 1 000 |
> | 5-D | JupyterHub | Append 100 rows (IDs 1001–1100) |
> | 5-E | JupyterHub | Confirm count = 1 100 |
> | 5-F | JupyterHub | Stop Spark session |
> | 5-G | Databricks notebook | Resolve new snapshot → replace view |
> | 5-H | Databricks SQL console | Verify 1 100 rows, new IDs visible |

---

## 6. Troubleshooting

### `NotAuthorizedException: Not authorized`
The Polaris OAuth token expired. Stop the session and re-run **Cell 1 + Cell 2** to get a fresh token and rebuild the session.

### `NameError: name 'spark' is not defined`
The kernel restarted. Re-run all cells from **Cell 1** in order.

### `AnalysisException: TABLE_OR_VIEW_NOT_FOUND`
A temp view was referenced before it was created, or after a kernel restart. Query the Iceberg table directly:
```python
spark.sql("SELECT COUNT(*) FROM databricks.lakehouse_db.customer").show()
```

### Job killed — `Master removed our application: KILLED`
The `spark-app-cleanup` CronJob killed an idle session. It runs every 10 minutes and kills sessions idle for **30 minutes**. Re-run **Cell 1 + Cell 2** to start a new session.

### `[Stage N:>` hangs for more than 2 minutes
Executors are still allocating (cold-start takes 30–90 s normally). If it exceeds 2 min, another app may be holding all cores. Check the Spark master UI at `http://192.168.1.50:30707` — if you see another app with `cores=20` that is not yours, it is a zombie and will be killed by the CronJob within 30 minutes. You can also wait for the next CronJob cycle.

### `WARN MetricsConfig: Cannot locate configuration`
Harmless — Hadoop looks for an optional metrics file that does not exist. Ignore it.

---

## 7. Key paths reference

| Resource | Value |
|---|---|
| JupyterHub | `http://192.168.1.50:30888` |
| Spark master UI | `http://192.168.1.50:30707` |
| Polaris in-cluster URI | `http://polaris-rest.prod.svc.cluster.local:8181/api/catalog` |
| Iceberg catalog (Spark) | `databricks` |
| Iceberg namespace | `lakehouse_db` |
| Iceberg table | `databricks.lakehouse_db.customer` |
| S3 bucket | `stardata-databricks` |
| S3 data path | `s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/data/` |
| S3 metadata path | `s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/metadata/` |
| Databricks workspace | `https://dbc-48ef5678-3df7.cloud.databricks.com` |
| Databricks SQL editor | `https://dbc-48ef5678-3df7.cloud.databricks.com/sql/editor` |
| Databricks catalog | `lakehouse` |
| Databricks schema | `lakehouse.lakehouse_db` |
| Databricks view | `lakehouse.lakehouse_db.vw_customer_latest` |
| Databricks SQL warehouse | `2c23ed9f013093c4` (Serverless Starter) |
| Unity Catalog external location | `stardata_databricks_iceberg` → `s3://stardata-databricks/` |
| Spark seed script | [`docker/spark-gluten-velox/scripts/databricks_customer_seed.py`](../../docker/spark-gluten-velox/scripts/databricks_customer_seed.py) |
| Databricks reader script | [`docker/spark-gluten-velox/scripts/databricks_customer_parquet_reader.py`](../../docker/spark-gluten-velox/scripts/databricks_customer_parquet_reader.py) |
| OpenBao S3 creds | `secret/platform/s3` → `access_key`, `secret_key`, `endpoint` |
| OpenBao Polaris creds | `secret/platform/polaris` → `spark_svc_id`, `spark_svc_secret` |

---

## 8. Auto-discovery notebook — all tables from S3 in one pass

**Notebook:** [`docker/databricks-notebooks/nb_multi_table_auto_reader.py`](../../docker/databricks-notebooks/nb_multi_table_auto_reader.py)

This **single notebook** scans the entire S3 warehouse root, discovers every Iceberg table automatically, and — for each one — resolves the current Iceberg snapshot and creates a view over exactly the live data files. **No table names are ever hardcoded.** Adding a new Iceberg table requires no code change; the next notebook run picks it up automatically.

Upload to Databricks at `https://dbc-48ef5678-3df7.cloud.databricks.com` and attach to a cluster with Unity Catalog enabled.

---

### Why the old approach broke UPDATEs and DELETEs

The previous notebook used `read_files('data/', format=>'parquet')` — a raw directory glob that reads every `.parquet` file ever written into the table's `data/` folder. This works for INSERT-only workloads but is fundamentally incompatible with Iceberg MERGE/UPDATE/DELETE semantics:

| Operation | What Iceberg writes to S3 | What raw glob sees |
|---|---|---|
| **INSERT** | New data file appended to `data/` | ✅ New rows visible |
| **UPDATE** | New data file (updated row) + old file marked `DELETED` in the manifest | ❌ Both old **and** new files read — duplicate rows |
| **DELETE** | Old file marked `DELETED` in the manifest; original parquet unchanged | ❌ Deleted row still visible — delete never propagates |

Iceberg's delete semantics live in the **manifest**, not in the filesystem. A blind `data/*.parquet` glob never reads the manifest, so it has no way to know which files are live.

---

### The fix — custom Iceberg snapshot resolver

The notebook now implements a lightweight Iceberg metadata reader in pure Python (no Polaris / catalog connector needed), following exactly the same steps the Iceberg reader uses internally:

```
metadata.json
    └── current-snapshot-id  ──► snapshots[]  ──► manifest-list  (Avro)
                                                        └── manifest files[]  (Avro)
                                                                └── data_file entries
                                                                        status=0 DELETED  ← excluded
                                                                        status=1 EXISTING ← kept ✅
                                                                        status=2 ADDED    ← kept ✅
```

**Step 1** — Read the latest `*.metadata.json` → get `current-snapshot-id`.
**Step 2** — Find the `manifest-list` Avro file path for that snapshot.
**Step 3** — Read the manifest-list (Avro) → collect all manifest file paths.
**Step 4** — Read each manifest (Avro) → collect `data_file.file_path` for entries where `status ∈ {1, 2}` (EXISTING or ADDED) and `content == 0` (DATA files, not delete files).
**Step 5** — Build the view: `read_files('file1','file2',…, format=>'parquet')` over exactly those files.

The view now points at the exact set of live files the current snapshot declares — so UPDATE rewrites and DELETE tombstones are correctly reflected.

---

### How auto-discovery works

The notebook walks two directory levels under `WAREHOUSE_ROOT`:

```
s3://stardata-databricks/iceberg/warehouse/          ← WAREHOUSE_ROOT (Cell 1)
│
├── lakehouse_db/                                     ← Level 1: database folder
│   ├── customer/                                     ← Level 2: table folder
│   │   ├── metadata/  *.metadata.json + manifests  ✅ included
│   │   └── data/      *.parquet
│   ├── customer_orders/                              ← also included
│   ├── product/                                      ← new table → auto-picked up
│   └── _staging/      no metadata.json  ⛔ skipped
│
└── analytics_db/                                     ← second database, also scanned
    └── sales/
        ├── metadata/  ✅ included
        └── data/
```

For every folder that contains at least one `*.metadata.json` the notebook derives:

| Field | Derived value (example) |
|---|---|
| `view` | `lakehouse.lakehouse_db.vw_customer_latest` |
| `meta_path` | `s3://.../lakehouse_db/customer/metadata/` |
| `live_files` | `['s3://.../data/00000-1-abc.parquet', …]` |

Folders that have **no** `*.metadata.json` (staging folders, Delta tables, checkpoints) are silently skipped.

---

### View design — snapshot-pinned, refreshed on every run

| Principle | Detail |
|---|---|
| **Points at exact live files, not a directory glob** | `read_files('file1','file2',…, format=>'parquet')` — only files that the current snapshot declares as live |
| **`CREATE OR REPLACE VIEW` on every run** | Because the live file list changes after every Iceberg write, the view definition must change too. `CREATE OR REPLACE` is instantaneous and safe to re-run |
| **INSERT/UPDATE/DELETE all work correctly** | Old files marked DELETED in the manifest are excluded from the view; rewritten files (ADDED) are included |
| **Empty tables handled** | If a table has no snapshots yet, a zero-row placeholder view is created |

---

### Cell map

| Cell | What it does | Run on refresh? |
|---|---|---|
| **Cell 1** | Set `WAREHOUSE_ROOT`, `DATABRICKS_CATALOG`, and `SKIP_TABLES` — the only three settings | Once per session |
| **Cell 2** | S3 directory scan: walks `WAREHOUSE_ROOT/<db>/<table>/`, confirms `metadata/*.metadata.json` exists, builds `TABLE_CONFIGS` | ✅ Every refresh |
| **Cell 3** | `resolve_live_files()` defined — parses metadata JSON → manifest-list → manifests → live file list | Once per session |
| **Cell 4** | Calls `resolve_live_files()` per table; ensures schemas exist; builds `SNAPSHOTS` dict | ✅ Every refresh |
| **Cell 5** | `CREATE OR REPLACE VIEW` per table over the exact live file list from Cell 4 | ✅ Every refresh |
| **Cell 6** | Summary report — row counts and snapshot timestamps for every view | Optional |
| **Cell 7** | Optional: `CACHE SELECT` to warm a view into NVMe disk cache | Optional |
| **Cell 8** | Optional: `UNCACHE` + `CACHE SELECT` to re-warm NVMe cache after a Cell 5 refresh | Optional |

---

### Configuration (Cell 1) — the only editable block

```python
WAREHOUSE_ROOT     = "s3://stardata-databricks/iceberg/warehouse/"
DATABRICKS_CATALOG = "lakehouse"
SKIP_TABLES        = set()   # e.g. {"lakehouse_db.staging", "lakehouse_db._temp"}
```

`SKIP_TABLES` is the only reason you would ever edit the notebook after initial setup — use it to exclude staging or system folders that exist in S3 but should not get views.

**To add a new Iceberg table:** create the table with Spark in the usual way. The next notebook run discovers the new `metadata/` folder and creates the view automatically.

---

### How to run

**First time and every subsequent refresh (after any INSERT, UPDATE, or DELETE):**
1. Run **Cells 1 → 6** in order

> **Why all cells every time?**
> Cell 4 re-resolves the live file list from the current snapshot.
> Cell 5 rebuilds the view over exactly those files (`CREATE OR REPLACE`).
> Without re-running Cell 5, the view still points at the old snapshot's files.

---

### Expected Cell 4 output (snapshot + live file resolution)

```
────────────────────────────────────────────────────────────
Resolving Iceberg snapshots and live data files …
────────────────────────────────────────────────────────────

  [lakehouse_db.customer]  snapshot=3778523514688560751
    Meta file    : 00003-....metadata.json
    Last updated : 2026-09-03 14:22:11 UTC
    Manifests    : 2
    Live files   : 4
    Dead files   : 1 (excluded — DELETE/UPDATE tombstones)

  [lakehouse_db.customer_orders]  snapshot=7123456789012345678
    Meta file    : 00001-....metadata.json
    Last updated : 2026-09-03 13:00:00 UTC
    Manifests    : 1
    Live files   : 2
    Dead files   : 0 (excluded — DELETE/UPDATE tombstones)

────────────────────────────────────────────────────────────
✅ All 2 table(s) resolved successfully
```

> **`Dead files: 1`** means one parquet file was written by a previous snapshot and has since been superseded by an UPDATE or DELETE. It is excluded from the view — the deleted/updated row will NOT appear.

### Expected Cell 5 output

```
  ✅ lakehouse.lakehouse_db.vw_customer_latest
     REFRESHED — snapshot 3778523514688560751
     rows=999  live_files=4

  ✅ lakehouse.lakehouse_db.vw_customer_orders_latest
     REFRESHED — snapshot 7123456789012345678
     rows=5,000  live_files=2

────────────────────────────────────────────────────────────
✅ 2 view(s) created/refreshed

  ┌─ HOW UPDATES AND DELETES NOW WORK ──────────────────────────┐
  │  The view lists ONLY the parquet files that belong to the   │
  │  current Iceberg snapshot (resolved from the manifest).     │
  │  • INSERT  → new file added to manifest (ADDED)             │
  │  • UPDATE  → old file marked DELETED; new file is ADDED     │
  │  • DELETE  → old file marked DELETED; rewritten file ADDED  │
  │  Dead files (status=DELETED) are excluded from the view.    │
  │  Re-run Cells 2 → 5 after any Iceberg write to refresh.     │
  └─────────────────────────────────────────────────────────────┘
```

### Expected Cell 6 summary report

```
══════════════════════════════════════════════════════════════════════
  REFRESH SUMMARY
══════════════════════════════════════════════════════════════════════
  TABLE                                ROWS  SNAPSHOT UPDATED
──────────────────────────────────────────────────────────────────────
  lakehouse_db.customer                 999  2026-09-03 14:22:11 UTC
  lakehouse_db.customer_orders        5,000  2026-09-03 13:00:00 UTC
══════════════════════════════════════════════════════════════════════
  Re-run Cells 2 → 5 after any Iceberg write (INSERT/UPDATE/DELETE).
  Each run resolves the current snapshot and refreshes the view over
  exactly the live data files — UPDATEs and DELETEs are reflected.
══════════════════════════════════════════════════════════════════════
```

### View columns

Every auto-created view selects all parquet columns plus two added by the view definition:

| Column | Type | Source |
|---|---|---|
| *(all source columns)* | (as in parquet) | live data files from current Iceberg snapshot |
| `snap_file` | STRING | `_metadata.file_path` — S3 path of the parquet file containing this row |
| `snap_file_size` | BIGINT | `_metadata.file_size` — parquet file size in bytes |

---

## 9. DML test steps — INSERT, UPDATE, DELETE end-to-end verification

This section proves that INSERT, UPDATE, and DELETE operations in JupyterHub (Spark / Iceberg) are correctly reflected in the Databricks view after running **Cells 2 → 5** of `nb_multi_table_auto_reader.py`.

**Prerequisite:** `nb_multi_table_auto_reader.py` has been uploaded to Databricks and Cells 1 → 6 have been run at least once.

Open the Databricks SQL console at `https://dbc-48ef5678-3df7.cloud.databricks.com/sql/editor`.
Select warehouse: **Serverless Starter Warehouse**.

---

### 9-0 — Start a fresh Spark session (run this first, every time)

> ⚠️ **Gluten/Velox is disabled for this session.**
> UPDATE and DELETE go through Iceberg's row-level rewrite plan. Gluten/Velox
> intercepts SQL execution for columnar acceleration but does not support
> Iceberg row-level write plans — with Gluten enabled, UPDATE and DELETE either
> throw an error or silently do nothing. This session disables Gluten so that
> Iceberg's own copy-on-write engine handles UPDATE/DELETE correctly.

```python
import os, urllib.request, json
from pyspark.sql import SparkSession

# ── Step 1: load credentials from OpenBao ────────────────────────────────────
OPENBAO_ADDR  = "http://openbao.prod.svc.cluster.local:8200"
OPENBAO_TOKEN = "s.xxxxxxxxxxxxxxxxxxxxxxxx"   # ← paste fresh root token here

def bao(path, field):
    req = urllib.request.Request(
        f"{OPENBAO_ADDR}/v1/{path}",
        headers={"X-Vault-Token": OPENBAO_TOKEN}
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())["data"]["data"][field]

S3_KEY         = bao("secret/data/platform/s3",      "access_key")
S3_SECRET      = bao("secret/data/platform/s3",      "secret_key")
S3_ENDPOINT    = bao("secret/data/platform/s3",      "endpoint")
S3_REGION      = bao("secret/data/platform/s3",      "region")
POLARIS_ID     = bao("secret/data/platform/polaris", "spark_svc_id")
POLARIS_SECRET = bao("secret/data/platform/polaris", "spark_svc_secret")
print("Credentials loaded")

# ── Step 2: stop any stale / dead SparkContext ────────────────────────────────
_s = SparkSession.getActiveSession()
if _s:
    try:
        _s.stop()
        print("Stopped previous session")
    except Exception:
        pass

# ── Step 3: build a fresh session — Gluten DISABLED for DML ──────────────────
# Gluten/Velox does not support Iceberg copy-on-write UPDATE/DELETE plans.
# It must be absent from spark.plugins for UPDATE and DELETE to work.
DRIVER_IP   = os.environ["SPARK_LOCAL_IP"]
POLARIS_URI = "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog"

spark = (
    SparkSession.builder
    .master("spark://spark-master-internal.prod.svc.cluster.local:17077")
    .appName("jupyter-dml-test")
    .config("spark.driver.host",        DRIVER_IP)
    .config("spark.driver.bindAddress", DRIVER_IP)
    .config("spark.executor.memory",    "2g")
    .config("spark.driver.memory",      "2g")
    .config("spark.pyspark.python",        "python3.11")
    .config("spark.pyspark.driver.python", "python3.11")
    # ── Serialiser ───────────────────────────────────────────────────────────
    .config("spark.serializer",                "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrationRequired", "false")
    # ── Iceberg extensions (required for UPDATE / DELETE / MERGE syntax) ─────
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    # ── databricks catalog → Polaris REST ────────────────────────────────────
    .config("spark.sql.catalog.databricks",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.databricks.type",             "rest")
    .config("spark.sql.catalog.databricks.uri",              POLARIS_URI)
    .config("spark.sql.catalog.databricks.oauth2-server-uri",
            f"{POLARIS_URI}/v1/oauth/tokens")
    .config("spark.sql.catalog.databricks.credential",
            f"{POLARIS_ID}:{POLARIS_SECRET}")
    .config("spark.sql.catalog.databricks.scope",            "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.databricks.warehouse",        "star_lakehouse")
    .config("spark.sql.catalog.databricks.rest.auth.type",   "oauth2")
    .config("spark.sql.catalog.databricks.s3.access-key-id",     S3_KEY)
    .config("spark.sql.catalog.databricks.s3.secret-access-key", S3_SECRET)
    .config("spark.sql.catalog.databricks.s3.endpoint",          S3_ENDPOINT)
    .config("spark.sql.catalog.databricks.s3.path-style-access", "true")
    .config("spark.sql.catalog.databricks.client.region",        S3_REGION)
    # ── S3A filesystem (Hadoop) ───────────────────────────────────────────────
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.access.key",             S3_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",             S3_SECRET)
    .config("spark.hadoop.fs.s3a.endpoint",               S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.endpoint.region",        S3_REGION)
    .config("spark.hadoop.fs.s3a.path.style.access",      "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    .config("spark.hadoop.fs.s3.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3.access.key",         S3_KEY)
    .config("spark.hadoop.fs.s3.secret.key",         S3_SECRET)
    .config("spark.hadoop.fs.s3.endpoint",           S3_ENDPOINT)
    .config("spark.hadoop.fs.s3.path.style.access",  "true")
    # ── NO spark.plugins — Gluten disabled so UPDATE/DELETE work ─────────────
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print(f"Spark {spark.version} ready — {DRIVER_IP}")
print("Gluten: DISABLED (required for Iceberg UPDATE/DELETE)")
```

Expected output:
```
Credentials loaded
Spark 3.5.x ready — 10.244.x.x
Gluten: DISABLED (required for Iceberg UPDATE/DELETE)
```

> When done with all DML tests run `spark.stop()` to release cluster cores.

---

### 9-1 — Confirm table is reachable + note baseline

Run in **JupyterHub** (after Section 9-0 session is ready):

```python
# Confirm catalog connectivity and note the baseline count before any changes
baseline = spark.sql(
    "SELECT COUNT(*) AS n FROM databricks.lakehouse_db.customer"
).collect()[0]["n"]
print(f"Baseline row count : {baseline}")

# Confirm no pre-existing test row
existing = spark.sql(
    "SELECT COUNT(*) AS n FROM databricks.lakehouse_db.customer WHERE customer_id = 99901"
).collect()[0]["n"]
print(f"Rows with id=99901 : {existing}  (expected: 0)")
```

Expected:
```
Baseline row count : 1000
Rows with id=99901 : 0
```

---

### 9-2 — INSERT

Run in **JupyterHub**:

```python
import datetime
from pyspark.sql import Row
from spark_iceberg_utils import IcebergTableBuilder

# snap_id and snap_timestamp must NOT be supplied — write_append() injects them.
# raw spark.sql("INSERT INTO ... VALUES ...") fails with CANNOT_FIND_DATA if
# snap_id is missing. Always use write_append() for inserts on this platform.
row = Row(
    customer_id   = 99901,
    full_name     = "DML Test User",
    email         = "dmltest@example.com",
    phone_number  = "555-0199",
    date_of_birth = datetime.date(1990, 6, 15),
    national_id   = "NID-99901",
    street_address= "1 Test St",
    city          = "Sydney",
    country_code  = "AU",
    ip_address    = "10.0.0.1",
    salary        = 75000.00,
    customer_tier = "gold",
    is_active     = 1,
    created_at    = datetime.datetime.utcnow(),
    updated_at    = datetime.datetime.utcnow(),
)
df = spark.createDataFrame([row])

# running_user="dave" required — SPARK_USER env-var is not set in JupyterHub
IcebergTableBuilder(spark, running_user="dave").write_append(
    df, catalog="databricks", namespace="lakehouse_db", table="customer"
)

# Verify immediately via the Iceberg catalog
spark.sql("""
    SELECT customer_id, full_name, email, city, salary, snap_id, snap_timestamp
    FROM   databricks.lakehouse_db.customer
    WHERE  customer_id = 99901
""").show(truncate=False)

after = spark.sql(
    "SELECT COUNT(*) AS n FROM databricks.lakehouse_db.customer"
).collect()[0]["n"]
print(f"Row count after INSERT: {after}  (expected: {baseline + 1})")
```

Expected:
```
+----------+--------------+--------------------+------+---------+-------------------+------------------------+
|customer_id|full_name    |email               |city  |salary   |snap_id            |snap_timestamp          |
+----------+--------------+--------------------+------+---------+-------------------+------------------------+
|99901     |DML Test User |dmltest@example.com |Sydney|75000.0  |<non-null BIGINT>  |<non-null TIMESTAMP>    |
+----------+--------------+--------------------+------+---------+-------------------+------------------------+
Row count after INSERT: 1001
```

---

### 9-3 — UPDATE

Run in **JupyterHub**:

```python
# Direct Iceberg UPDATE — works on format-version=2 copy-on-write tables.
# Requires Gluten to be DISABLED in the session (done in 9-0).
spark.sql("""
    UPDATE databricks.lakehouse_db.customer
    SET    email      = 'dmltest_updated@example.com',
           city       = 'Melbourne',
           salary     = 99000.00,
           updated_at = current_timestamp()
    WHERE  customer_id = 99901
""")

# Verify immediately — must show new values, exactly 1 row (no duplicate)
spark.sql("""
    SELECT customer_id, email, city, salary
    FROM   databricks.lakehouse_db.customer
    WHERE  customer_id = 99901
""").show(truncate=False)

n = spark.sql(
    "SELECT COUNT(*) AS n FROM databricks.lakehouse_db.customer WHERE customer_id = 99901"
).collect()[0]["n"]
print(f"Rows for id=99901 after UPDATE: {n}  (expected: 1 — no duplicates)")
```

Expected:
```
+----------+-----------------------------+---------+---------+
|customer_id|email                       |city     |salary   |
+----------+-----------------------------+---------+---------+
|99901     |dmltest_updated@example.com |Melbourne|99000.0  |
+----------+-----------------------------+---------+---------+
Rows for id=99901 after UPDATE: 1
```

> If UPDATE returns 0 rows or still shows the old value — Gluten is still active
> in the session. Re-run 9-0 and confirm the output says `Gluten: DISABLED`.

---

### 9-4 — DELETE

Run in **JupyterHub**:

```python
# Direct Iceberg DELETE — copy-on-write, requires Gluten DISABLED (done in 9-0).
spark.sql("""
    DELETE FROM databricks.lakehouse_db.customer
    WHERE  customer_id = 99901
""")

# Verify immediately — row must be gone, total count back to baseline
gone = spark.sql(
    "SELECT COUNT(*) AS n FROM databricks.lakehouse_db.customer WHERE customer_id = 99901"
).collect()[0]["n"]
total = spark.sql(
    "SELECT COUNT(*) AS n FROM databricks.lakehouse_db.customer"
).collect()[0]["n"]
print(f"Rows for id=99901 after DELETE : {gone}   (expected: 0)")
print(f"Total rows after DELETE        : {total}  (expected: {baseline})")
```

Expected:
```
Rows for id=99901 after DELETE : 0
Total rows after DELETE        : 1000
```

> If the row count does not change — Gluten is still active. Re-run 9-0.

---

### 9-5 — Refresh Databricks view and verify all three operations

After completing 9-2, 9-3, 9-4 above, refresh the Databricks view to confirm
all three changes are reflected there too.

In **`nb_multi_table_auto_reader.py`**, re-run **Cells 2 → 5**.
Cell 4 output should show:

```
[lakehouse_db.customer]  snapshot=<new_id>
  Live files   : <n>
  Dead files   : 2  ← at least 2: one from UPDATE, one from DELETE
```

Then in the **Databricks SQL console**:

```sql
-- Must return 0 rows — row was deleted
SELECT customer_id, full_name, email, city, salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
WHERE  customer_id = 99901;

-- Must equal the original baseline (e.g. 1000)
SELECT COUNT(*) AS customer_rows
FROM   lakehouse.lakehouse_db.vw_customer_latest;

-- Must return 0 rows — no duplicates from the UPDATE
SELECT customer_id, COUNT(*) AS dup_count
FROM   lakehouse.lakehouse_db.vw_customer_latest
GROUP  BY customer_id HAVING COUNT(*) > 1;
```

> **If the deleted/updated row still appears after refreshing:**
> Confirm Cell 4 printed `Dead files >= 1`. If it shows `Dead files: 0`
> the Iceberg snapshot was not committed — check the JupyterHub step output
> for errors before running the view refresh.

---

### 9-6 — Spot-check: first 10 and last 10 rows

```sql
-- First 10
SELECT customer_id, full_name, city, customer_tier, ROUND(salary,2) AS salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
ORDER  BY customer_id LIMIT 10;

-- Last 10
SELECT customer_id, full_name, city, customer_tier, ROUND(salary,2) AS salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
ORDER  BY customer_id DESC LIMIT 10;
```

---

### 9-7 — Batch audit: confirm all insert batches present

```sql
SELECT
    DATE(created_at)   AS insert_date,
    MIN(customer_id)   AS first_id,
    MAX(customer_id)   AS last_id,
    COUNT(*)           AS row_count
FROM   lakehouse.lakehouse_db.vw_customer_latest
GROUP  BY DATE(created_at)
ORDER  BY insert_date;
```

✅ Expected:

| insert_date | first_id | last_id | row_count |
|---|---|---|---|
| 2026-01-xx | 1 | 1000 | 1000 |
| 2026-09-xx | 1001 | 1100 | 100 |

---

### 9-8 — Tier distribution

```sql
SELECT customer_tier, COUNT(*) AS cnt, ROUND(AVG(salary),2) AS avg_salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
GROUP  BY customer_tier ORDER BY cnt DESC;
```

---

### 9-9 — Full data quality check

```sql
-- No duplicate customer_ids
SELECT customer_id, COUNT(*) AS dup_count
FROM   lakehouse.lakehouse_db.vw_customer_latest
GROUP  BY customer_id HAVING COUNT(*) > 1;
-- ✅ Expected: 0 rows

-- No NULL customer_ids
SELECT COUNT(*) AS null_ids
FROM   lakehouse.lakehouse_db.vw_customer_latest
WHERE  customer_id IS NULL;
-- ✅ Expected: 0

-- All tiers valid
SELECT COUNT(*) AS invalid_tier_rows
FROM   lakehouse.lakehouse_db.vw_customer_latest
WHERE  customer_tier NOT IN ('standard','silver','gold','platinum');
-- ✅ Expected: 0

-- Salary in range [30000, 200000]
SELECT COUNT(*) AS out_of_range
FROM   lakehouse.lakehouse_db.vw_customer_latest
WHERE  salary < 30000 OR salary > 200000;
-- ✅ Expected: 0
```

---

## 10. NVMe disk cache — cache a view to eliminate S3 round-trips

By default every query against a `read_files()` view goes to S3 on every execution. On a **Photon-enabled cluster** (Standard tier or higher) Databricks maintains a local NVMe-backed disk cache on each executor node. Caching a view into this local storage eliminates the S3 round-trip for all subsequent queries until the cluster restarts or the cache is explicitly invalidated.

> **Requirement:** The cluster must be Photon-enabled with the local disk cache feature turned on. Serverless SQL warehouses manage this automatically. For interactive clusters, verify in the cluster config that **"Enable disk cache"** is checked.

---

### When to cache

| Situation | Cache? |
|---|---|
| Dashboard or BI tool queries the same view every few minutes | ✅ Yes — cache pays off immediately |
| One-off exploratory query | ❌ No — cache-fill cost exceeds the benefit |
| View has < 100 MB of data | ❌ No — S3 latency is already negligible at this size |
| Cluster restarts frequently (< 30 min) | ❌ No — cache is lost on restart anyway |
| After a new Iceberg snapshot (new parquet files landed) | ✅ Re-warm the cache — see step 3 below |

---

### Step 1 — Check NVMe cache configuration

Run this in a Databricks notebook or the SQL console:

```python
# Check whether disk cache is enabled on the current cluster
print(spark.conf.get("spark.databricks.io.cache.enabled", "false"))

# Check how much NVMe space is allocated for the cache
print(spark.conf.get("spark.databricks.io.cache.maxDiskUsage", "not set"))

# Check how much memory is reserved for the cache
print(spark.conf.get("spark.databricks.io.cache.maxMetaDataCache", "not set"))
```

✅ Expected: `spark.databricks.io.cache.enabled = true` on a Photon cluster with disk cache enabled.

---

### Step 2 — Warm the cache for one view

Run in a **Databricks notebook** (uses `spark`):

```python
VIEW = "lakehouse.lakehouse_db.vw_customer_latest"

print(f"Warming NVMe cache for {VIEW} …")
spark.sql(f"CACHE SELECT * FROM {VIEW}")
print(f"✅ Cache warm — subsequent queries skip S3")
```

Or in the **Databricks SQL console:**

```sql
CACHE SELECT * FROM lakehouse.lakehouse_db.vw_customer_latest;
-- ✅ Scans all parquet files once and writes decompressed columnar data to NVMe
```

> `CACHE SELECT` is synchronous — it completes only after every file has been read and cached. For a 1,000-row table this takes seconds. For a large table allow proportionally longer.

---

### Step 3 — Re-warm the cache after a new Iceberg snapshot

When Spark appends a new snapshot, new `.parquet` files land in the `data/` directory. The NVMe cache still holds the old decompressed data from the previous set of files. Run the following to evict the stale entries and re-warm:

```python
VIEW = "lakehouse.lakehouse_db.vw_customer_latest"

# Step A: evict stale cached data
spark.sql(f"UNCACHE TABLE IF EXISTS {VIEW}")
print(f"Evicted stale cache for {VIEW}")

# Step B: re-scan and re-warm with the new parquet files
spark.sql(f"CACHE SELECT * FROM {VIEW}")
print(f"✅ NVMe cache re-warmed — {VIEW} now reflects the latest snapshot")
```

> `UNCACHE TABLE IF EXISTS` is safe to run even if the view was never cached — the `IF EXISTS` prevents errors.

---

### Step 4 — Cache all auto-discovered views at once (notebook Cell 7)

[`docker/databricks-notebooks/nb_multi_table_auto_reader.py`](../../docker/databricks-notebooks/nb_multi_table_auto_reader.py) — **Cell 7** caches one view by name. To cache every auto-discovered view in a single loop, uncomment the block in Cell 7:

```python
# Cache ALL discovered views
print("Caching all discovered views into NVMe disk cache …")
for tbl, snap in SNAPSHOTS.items():
    print(f"  Caching {snap['view']} …")
    spark.sql(f"CACHE SELECT * FROM {snap['view']}")
    print(f"  ✅ Done")
print("✅ All views cached")
```

And **Cell 8** handles the re-warm after a new snapshot — uncomment the loop version to re-warm all views:

```python
# Re-warm ALL views after new snapshots
for tbl, snap in SNAPSHOTS.items():
    spark.sql(f"UNCACHE TABLE IF EXISTS {snap['view']}")
    spark.sql(f"CACHE SELECT * FROM {snap['view']}")
print("✅ All views re-warmed")
```

---

### Step 5 — Verify cache hits in the SQL console

After warming the cache, run a query and check the query profile:

```sql
-- This query should now be served from NVMe, not S3
SELECT customer_tier, COUNT(*) AS cnt, ROUND(AVG(salary),2) AS avg_salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
GROUP  BY customer_tier
ORDER  BY cnt DESC;
```

In the **Query Profile** tab (Databricks SQL console → History → click the query → Profile):
- Look for `Scan Parquet` nodes — the **"Rows from cache"** metric should equal the total row count
- **"Bytes read from disk cache"** should be > 0 and **"Bytes read from S3"** should be 0

---

### Cache lifetime and eviction rules

| Event | Effect on cache |
|---|---|
| Cluster restart | ❌ Cache is fully evicted — re-warm after restart |
| New parquet files written by Spark | ⚠️ Old files still cached; run `UNCACHE` + `CACHE SELECT` to refresh |
| `UNCACHE TABLE <view>` | ✅ Explicitly evicts all cached data for that view |
| `CACHE SELECT * FROM <view>` | ✅ Warms the cache for the current set of files |
| Cluster scales down (auto-scaling removes a node) | ⚠️ Data cached on removed nodes is lost; remaining nodes still serve their cached partitions |

---

### SQL console shortcut — `CACHE` and `UNCACHE`

```sql
-- Warm cache for a specific view
CACHE SELECT * FROM lakehouse.lakehouse_db.vw_customer_latest;

-- Evict cache for a specific view
UNCACHE TABLE IF EXISTS lakehouse.lakehouse_db.vw_customer_latest;
```

> **Note:** `SHOW VIEWS IN lakehouse.lakehouse_db` does not work on Serverless compute
> (cross-catalog 3-part schema reference not supported). To confirm a view exists, use:

```python
# Run in a Databricks notebook
print(spark.catalog.tableExists("lakehouse.lakehouse_db.vw_customer_latest"))
# ✅ True — then inspect query profiles to confirm cache hits
```
