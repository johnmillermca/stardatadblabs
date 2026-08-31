# Runbook 20 — customer Table: Iceberg Write from JupyterHub & Read in Databricks

| Field | Value |
|---|---|
| **Runbook ID** | RB-20 |
| **Service** | k8s-platform / databricks |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-08-31 (fix: snap_id / snap_timestamp columns; SHOW VIEWS replaced with spark.catalog.tableExists()) |

---

## 1. Overview

The `customer` Iceberg table is written by Spark Gluten on the k8s cluster and stored on S3.
This runbook covers two ways to read it:

- **Section 2** — Read from **JupyterHub** using PySpark (query via Iceberg catalog)
- **Section 3** — Read and verify from the **Databricks SQL console** (via `read_files()` and a persistent view)
- **Section 5** — **Insert 100 more rows** from JupyterHub and see them live in Databricks (end-to-end walk-through)
- **Section 8** — **Auto-discovery notebook** — scans the entire S3 warehouse root, discovers every Iceberg table automatically (no table names hardcoded), creates views once with zero-downtime
- **Section 9** — **DML test steps** — SQL queries to verify the latest data in Databricks
- **Section 10** — **NVMe disk cache** — how to cache views into local NVMe storage to eliminate S3 round-trips

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

Open **`https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor`**  
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

1. Go to **`https://dbc-11a1dbc5-061a.cloud.databricks.com/explore/data`**
2. Expand **`lakehouse`** → **`lakehouse_db`**
3. Click **`vw_customer_latest`**
4. Click the **Sample Data** tab → live preview of the 1 000 rows

---

## 4. Refresh the view after a new Spark write

> **This section is superseded by Section 8.**
> The auto-discovery notebook ([`nb_multi_table_auto_reader.py`](../../docker/databricks-notebooks/nb_multi_table_auto_reader.py)) scans the entire S3 warehouse root and refreshes **all** tables in one pass — no per-table code required.
>
> **To refresh after any Spark write:** re-run **Cells 2 and 4** of the notebook.
> The views point at the `data/` directory and pick up new parquet files automatically on the next query — no notebook re-run is needed for the view itself.
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

Open **`https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor`**
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
| Databricks workspace | `https://dbc-11a1dbc5-061a.cloud.databricks.com` |
| Databricks SQL editor | `https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor` |
| Databricks catalog | `lakehouse` |
| Databricks schema | `lakehouse.lakehouse_db` |
| Databricks view | `lakehouse.lakehouse_db.vw_customer_latest` |
| Databricks SQL warehouse | `942026cf5e55f3c3` (Serverless Starter) |
| Unity Catalog external location | `stardata_databricks_iceberg` → `s3://stardata-databricks/` |
| Spark seed script | [`docker/spark-gluten-velox/scripts/databricks_customer_seed.py`](../../docker/spark-gluten-velox/scripts/databricks_customer_seed.py) |
| Databricks reader script | [`docker/spark-gluten-velox/scripts/databricks_customer_parquet_reader.py`](../../docker/spark-gluten-velox/scripts/databricks_customer_parquet_reader.py) |
| OpenBao S3 creds | `secret/platform/s3` → `access_key`, `secret_key`, `endpoint` |
| OpenBao Polaris creds | `secret/platform/polaris` → `spark_svc_id`, `spark_svc_secret` |

---

## 8. Auto-discovery notebook — all tables from S3 in one pass

**Notebook:** [`docker/databricks-notebooks/nb_multi_table_auto_reader.py`](../../docker/databricks-notebooks/nb_multi_table_auto_reader.py)

This **single notebook** scans the entire S3 warehouse root, discovers every Iceberg table automatically, and — for each one — creates a view and refreshes a Delta audit table. **No table names are ever hardcoded.** Adding a new Iceberg table requires no code change; the next notebook run picks it up automatically.

Upload to Databricks at `https://dbc-11a1dbc5-061a.cloud.databricks.com` and attach to a cluster with Unity Catalog enabled.

---

### How auto-discovery works

The notebook walks two directory levels under `WAREHOUSE_ROOT`:

```
s3://stardata-databricks/iceberg/warehouse/          ← WAREHOUSE_ROOT (Cell 1)
│
├── lakehouse_db/                                     ← Level 1: database folder
│   ├── customer/                                     ← Level 2: table folder
│   │   ├── metadata/  *.metadata.json  ✅ included
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
| `data_path` | `s3://.../lakehouse_db/customer/data/` |
| `meta_path` | `s3://.../lakehouse_db/customer/metadata/` |

Folders that have **no** `*.metadata.json` (staging folders, Delta tables, checkpoints) are silently skipped.

---

### Zero-downtime design

Two objects are created per table and each has its own refresh strategy:

#### Views — `vw_<table>_latest`

| Principle | Detail |
|---|---|
| **Points at `data/` directory, not a snapshot path** | `read_files('s3://.../customer/data/', format=>'parquet')` — every query reads whatever `.parquet` files exist at that moment; new Iceberg snapshots add files to the same directory |
| **`CREATE VIEW IF NOT EXISTS` — created once, never replaced** | First run creates the view. Every subsequent run detects it exists and skips — no DDL lock, no interruption to in-flight queries |
| **New data visible automatically** | After Spark appends a new Iceberg snapshot the next `SELECT` against the view returns the new rows — no notebook re-run, no DDL change |

> To change the view definition (e.g. add a column): `ALTER VIEW lakehouse.lakehouse_db.vw_customer_latest AS SELECT ...`

### Cell map

| Cell | What it does | Run on refresh? |
|---|---|---|
| **Cell 1** | Set `WAREHOUSE_ROOT`, `DATABRICKS_CATALOG`, and `SKIP_TABLES` — the only three settings | Once per session |
| **Cell 2** | S3 directory scan: walks `WAREHOUSE_ROOT/<db>/<table>/`, confirms `metadata/*.metadata.json` exists, builds `TABLE_CONFIGS` | ✅ Every refresh |
| **Cell 3** | `resolve_latest_snapshot()` helper defined | Once per session |
| **Cell 4** | Loops: picks latest `*.metadata.json` per table for diagnostics, ensures each schema exists | ✅ Every refresh |
| **Cell 5** | Loops: `CREATE VIEW IF NOT EXISTS` — first run creates; all subsequent runs are a no-op | First run only (per table) |
| **Cell 6** | Summary report — row counts and snapshot timestamps for every view | Optional |
| **Cell 7** | Optional: `CACHE SELECT` to warm a view into NVMe disk cache | Optional |
| **Cell 8** | Optional: `UNCACHE` + `CACHE SELECT` to re-warm NVMe cache after a new snapshot | Optional |

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

**First time:**
1. Run **Cells 1 → 6** in order — views are created

**Every subsequent refresh (after any Spark write to any table):**
- Re-run **Cells 2 and 4** only (discovery + snapshot diagnostics)
- Cell 5 prints `EXISTS (no DDL change — zero downtime preserved)` for every view and does nothing

> Views **never need re-running** for new data to appear — they pick up new parquet files automatically on the next user query.

---

### Expected Cell 2 output (warehouse with two databases, three tables)

```
────────────────────────────────────────────────────────────
Auto-discovered 3 Iceberg table(s):
  lakehouse_db.customer               view → vw_customer_latest
  lakehouse_db.customer_orders        view → vw_customer_orders_latest
  analytics_db.sales                  view → vw_sales_latest
────────────────────────────────────────────────────────────
```

### Expected Cell 4 output (snapshot resolution)

```
────────────────────────────────────────────────────────────
Resolving latest Iceberg snapshots …
────────────────────────────────────────────────────────────
  [lakehouse_db.customer] 2 snapshot(s) found
    Latest file  : 00001-....metadata.json
    Snapshot ID  : 3778523514688560751
    Last updated : 2026-09-02 12:34:56 UTC
    Data path    : s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/data/

  [lakehouse_db.customer_orders] 1 snapshot(s) found
    Latest file  : 00000-....metadata.json
    Snapshot ID  : 7123456789012345678
    Last updated : 2026-09-02 13:00:00 UTC
    Data path    : s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer_orders/data/

────────────────────────────────────────────────────────────
✅ All 3 table(s) resolved successfully
```

### Expected Cell 5 output — first run

```
  ✅ lakehouse.lakehouse_db.vw_customer_latest
     status=CREATED
     rows=1,000  src=s3://.../lakehouse_db/customer/data/
```

### Expected Cell 5 output — every subsequent run

```
  ✅ lakehouse.lakehouse_db.vw_customer_latest
     status=EXISTS (no DDL change — zero downtime preserved)
     rows=1,100  src=s3://.../lakehouse_db/customer/data/
```

> `rows=1,100` reflects the 100-row insert — already visible without any DDL change to the view.

### Expected Cell 6 summary report

```
══════════════════════════════════════════════════════════════════════
  REFRESH SUMMARY
══════════════════════════════════════════════════════════════════════
  TABLE                                ROWS  SNAPSHOT UPDATED
──────────────────────────────────────────────────────────────────────
  lakehouse_db.customer               1,100  2026-09-03 12:34:56 UTC
  lakehouse_db.customer_orders        5,000  2026-09-03 13:00:00 UTC
══════════════════════════════════════════════════════════════════════
  Re-run Cells 2 + 4 any time new data lands in S3.
  (Views auto-reflect new parquet files — no Cell 5 re-run needed.)
══════════════════════════════════════════════════════════════════════
```

### View columns

Every auto-created view selects all parquet columns plus two added by the view definition:

| Column | Type | Source |
|---|---|---|
| *(all source columns)* | (as in parquet) | read from `data/*.parquet` |
| `snap_file` | STRING | `_metadata.file_path` — S3 path of the parquet file |
| `snap_file_size` | BIGINT | `_metadata.file_size` — parquet file size in bytes |

---

## 9. DML test steps — verify latest data in Databricks

**File:** [`docker/databricks-notebooks/dml_test_steps.sql`](../../docker/databricks-notebooks/dml_test_steps.sql)

Open the Databricks SQL console at `https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor`.
Select warehouse: **Serverless Starter Warehouse**.
Run the blocks below in order. Expected results are shown after each query.

---

### 9-1 — Confirm schema and objects exist

```sql
SHOW SCHEMAS IN lakehouse;
-- ✅ lakehouse_db listed
```

> **Note:** `SHOW VIEWS IN lakehouse.lakehouse_db` does not support cross-catalog
> 3-part schema references on Serverless compute. Use `spark.catalog.tableExists()`
> with the fully-qualified 3-part name instead:

```python
# Run in a Databricks notebook
print(spark.catalog.tableExists("lakehouse.lakehouse_db.vw_customer_latest"))
# ✅ True

print(spark.catalog.tableExists("lakehouse.lakehouse_db.vw_customer_orders_latest"))
# ✅ True
```

---

### 9-2 — Row counts

```sql
-- customer
SELECT COUNT(*) AS customer_rows
FROM   lakehouse.lakehouse_db.vw_customer_latest;

-- customer_orders
SELECT COUNT(*) AS orders_rows
FROM   lakehouse.lakehouse_db.vw_customer_orders_latest;
```

✅ Expected: `customer_rows` = 1000 (or 1100 after the 100-row insert); `orders_rows` reflects the latest snapshot.

---

### 9-3 — Spot-check: first 10 and last 10 rows (customer view)

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

✅ Last 10: highest IDs are `1100` after the 100-row insert (seed=99 names and cities).

---

### 9-4 — Confirm new rows are visible (after 100-row insert into customer)

```sql
SELECT customer_id, full_name, city, customer_tier, ROUND(salary,2) AS salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
WHERE  customer_id BETWEEN 1095 AND 1100
ORDER  BY customer_id;
```

✅ Expected: 6 rows with IDs 1095–1100.

---

### 9-5 — Batch audit: prove both insert batches are in the customer view

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

### 9-6 — Tier distribution (customer view)

```sql
SELECT customer_tier, COUNT(*) AS cnt, ROUND(AVG(salary),2) AS avg_salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
GROUP  BY customer_tier ORDER BY cnt DESC;
```

---

### 9-7 — Data quality checks (customer)

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

-- All tiers are valid
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

> **Full DML file:** [`docker/databricks-notebooks/dml_test_steps.sql`](../../docker/databricks-notebooks/dml_test_steps.sql)

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
