# Runbook 20 — customer Table: Iceberg Write from JupyterHub & Read in Databricks

| Field | Value |
|---|---|
| **Runbook ID** | RB-20 |
| **Service** | k8s-platform / databricks |
| **Owner** | Platform Team |
| **Status** | Active |
| **Last Updated** | 2026-08-30 |

---

## 1. Overview

The `customer` Iceberg table is written by Spark Gluten on the k8s cluster and stored on S3.
This runbook covers two ways to read it:

- **Section 2** — Read from **JupyterHub** using PySpark (query via Iceberg catalog)
- **Section 3** — Read and verify from the **Databricks SQL console** (via `read_files()` and a persistent view)
- **Section 5** — **Insert 100 more rows** from JupyterHub and see them live in Databricks (end-to-end walk-through)

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

```sql
SHOW VIEWS IN lakehouse.lakehouse_db;
```
✅ `vw_customer_latest` listed

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

Every time new rows are inserted by Spark, re-run the two cells below in a **Databricks notebook** to pick up the new snapshot and update the view.

### Databricks notebook — Cell A: resolve latest data path

> Run this in the **Databricks** notebook (uses `dbutils.fs` and `spark` — no secret scope needed).

```python
import json

META_PATH = "s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/metadata/"

all_files  = dbutils.fs.ls(META_PATH)
meta_files = [f for f in all_files if f.name.endswith(".metadata.json")]
meta_files.sort(key=lambda f: f.modificationTime, reverse=True)

latest = meta_files[0]
print(f"✅ Latest metadata ({len(meta_files)} snapshots): {latest.path}")

meta_json = spark.read.text(latest.path, wholetext=True).collect()[0][0]
meta      = json.loads(meta_json)
DATA_PATH = meta["location"].rstrip("/") + "/data/"
print(f"✅ Data path: {DATA_PATH}")
```

### Databricks notebook — Cell B: recreate view pointing at new snapshot

```python
spark.sql("CREATE SCHEMA IF NOT EXISTS lakehouse.lakehouse_db")

spark.sql(f"""
    CREATE OR REPLACE VIEW lakehouse.lakehouse_db.vw_customer_latest
    COMMENT 'Latest Iceberg snapshot of customer — re-run after each Spark write'
    AS
    SELECT
        customer_id, full_name, email, phone_number, date_of_birth,
        national_id, street_address, city, country_code, ip_address,
        salary, customer_tier, is_active, created_at, updated_at,
        snap_id, snap_timestamp
    FROM read_files(
        '{DATA_PATH}',
        format      => 'parquet',
        mergeSchema => true
    )
""")

print(f"✅ View refreshed: lakehouse.lakehouse_db.vw_customer_latest")
print(f"   Backed by: {DATA_PATH}")
```

> Then go to the **Databricks SQL console** and rerun Check 2 — the count will reflect the new rows.

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

The new rows use IDs 1 001 – 1 100 and a different random seed (`seed=99`) so the names and cities are distinct from the original batch.

```python
import datetime, hashlib, random
from pyspark.sql import Row
from pyspark.sql.types import (
    DateType, DoubleType, IntegerType, StringType,
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
        created_at    = base_dt + datetime.timedelta(days=rng.randint(0,90)),
        updated_at    = base_dt + datetime.timedelta(days=rng.randint(0,120)),
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

Open a **Databricks notebook** at `https://dbc-11a1dbc5-061a.cloud.databricks.com`.
Run these two cells in order to point the view at the new Iceberg snapshot.

**Cell G-1 — Resolve the latest metadata JSON**

```python
import json

META_PATH = "s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/metadata/"

all_files  = dbutils.fs.ls(META_PATH)
meta_files = [f for f in all_files if f.name.endswith(".metadata.json")]
meta_files.sort(key=lambda f: f.modificationTime, reverse=True)

latest = meta_files[0]
print(f"✅ Latest metadata ({len(meta_files)} snapshots): {latest.path}")

meta_json = spark.read.text(latest.path, wholetext=True).collect()[0][0]
meta      = json.loads(meta_json)
DATA_PATH = meta["location"].rstrip("/") + "/data/"
print(f"✅ Data path: {DATA_PATH}")
```

✅ Expected:
```
✅ Latest metadata (3 snapshots): s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/metadata/00002-....metadata.json
✅ Data path: s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/data/
```

> The snapshot count increases by 1 with every `append` write. After inserting the original
> 1 000 rows and now the 100 more rows, you will see **2 or more** snapshots listed.

**Cell G-2 — Replace the view to use the new snapshot**

```python
spark.sql("CREATE SCHEMA IF NOT EXISTS lakehouse.lakehouse_db")

spark.sql(f"""
    CREATE OR REPLACE VIEW lakehouse.lakehouse_db.vw_customer_latest
    COMMENT 'Latest Iceberg snapshot of customer — re-run after each Spark write'
    AS
    SELECT
        customer_id, full_name, email, phone_number, date_of_birth,
        national_id, street_address, city, country_code, ip_address,
        salary, customer_tier, is_active, created_at, updated_at,
        snap_id, snap_timestamp
    FROM read_files(
        '{DATA_PATH}',
        format      => 'parquet',
        mergeSchema => true
    )
""")

print(f"✅ View refreshed → {DATA_PATH}")
```

✅ Expected: `✅ View refreshed → s3://stardata-databricks/.../customer/data/`

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
