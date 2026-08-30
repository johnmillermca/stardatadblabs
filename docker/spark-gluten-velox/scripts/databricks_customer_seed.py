#!/usr/bin/env python3
"""
databricks_customer_seed.py
============================
Create the `databricks` catalog / `lakehouse_db` namespace / `customer` Iceberg
table and insert 1 000 synthetic customer rows.

Pipeline
--------
  1. Fetch credentials from OpenBao (S3 + Polaris) via BaoSparkInit.
  2. Build a SparkSession wired to the Spark cluster with the `databricks`
     catalog alias pointing at Polaris REST warehouse `star_lakehouse`.
  3. Create namespace `lakehouse_db` (idempotent).
  4. Create Iceberg table `databricks.lakehouse_db.customer` (IF NOT EXISTS).
     Table lives on S3 bucket `stardata-databricks` under the Iceberg warehouse
     prefix already allowed by the Polaris star_lakehouse catalog.
  5. Generate and insert 1 000 synthetic rows via IcebergTableBuilder.write_append().

S3 bucket
---------
  Target bucket  : stardata-databricks
  Location prefix: s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer

Catalog / namespace
-------------------
  Spark catalog alias : databricks
  Polaris warehouse   : star_lakehouse
  Namespace           : lakehouse_db
  Table               : customer

Running user
------------
  dave (can_admin_catalog=True, can_write_iceberg=True)

Usage
-----
  # From spark-submit (inside cluster):
  spark-submit --py-files bao_spark_init.py,spark_iceberg_utils.py \\
      databricks_customer_seed.py

  # Environment overrides (all optional):
  ROWS=1000          # rows to insert   (default: 1000)
  DRY_RUN=1          # DDL only, skip data insert
  DISABLE_GLUTEN=1   # turn off Gluten/Velox for debugging
"""

from __future__ import annotations

import datetime
import hashlib
import logging
import os
import random

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from bao_spark_init import BaoSparkInit
from spark_iceberg_utils import IcebergTableBuilder

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("customer-seed")

# ── Configuration ─────────────────────────────────────────────────────────────
CATALOG    = "databricks"
NAMESPACE  = "lakehouse_db"
TABLE      = "customer"
# stardata-databricks is the live S3 bucket; its allowedLocations are already
# registered in the Polaris star_lakehouse catalog.
S3_BUCKET  = os.environ.get("S3_BUCKET", "stardata-databricks")
ROWS       = int(os.environ.get("ROWS", "1000"))
DRY_RUN    = os.environ.get("DRY_RUN", "0") == "1"
USER       = os.environ.get("USER", "dave")

# Polaris warehouse that backs the `databricks` catalog alias
_POLARIS_WAREHOUSE = "star_lakehouse"
_POLARIS_URI       = "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog"
_SPARK_MASTER      = "spark://spark-master-internal.prod.svc.cluster.local:17077"

# ── Customer schema (business columns only — snap_id/snap_timestamp injected) ─
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

# ── Synthetic data fixtures ───────────────────────────────────────────────────
_FIRST_NAMES = [
    "James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda",
    "William","Barbara","David","Elizabeth","Richard","Susan","Joseph","Jessica",
    "Thomas","Sarah","Charles","Karen","Wei","Amira","Luca","Sara","Arjun",
    "Yuki","Carlos","Fatima","Ivan","Priya",
]
_LAST_NAMES = [
    "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
    "Wilson","Taylor","Martinez","Anderson","Thomas","Jackson","White","Harris",
    "Martin","Thompson","Chen","Patel","Nasser","Rossi","Khan","Nakamura",
    "Silva","Müller","Dubois","Kowalski","Oliveira","Hassan",
]
_CITIES = [
    ("Toronto","CA"),("London","GB"),("Rome","IT"),("Cairo","EG"),("Beijing","CN"),
    ("Mumbai","IN"),("São Paulo","BR"),("Berlin","DE"),("Tokyo","JP"),("Sydney","AU"),
    ("Paris","FR"),("Seoul","KR"),("Lagos","NG"),("Buenos Aires","AR"),("Dubai","AE"),
    ("Singapore","SG"),("Istanbul","TR"),("Mexico City","MX"),("Amsterdam","NL"),
    ("Nairobi","KE"),("Cape Town","ZA"),("Bangkok","TH"),("Jakarta","ID"),
    ("Karachi","PK"),("Chicago","US"),("Los Angeles","US"),("New York","US"),
    ("Madrid","ES"),("Milan","IT"),("Hong Kong","HK"),
]
_TIERS  = ["standard","silver","gold","platinum"]
_STREETS = ["Main St","Oak Ave","Maple Rd","Pine Blvd","Cedar Ln","Elm St",
            "Park Ave","Lake Dr","River Rd","Hill Ct"]


def _rng(seed: int) -> random.Random:
    """Return a seeded Random instance for reproducible data."""
    return random.Random(seed)


def _gen_rows(n: int) -> list[Row]:
    """Generate *n* deterministic synthetic customer rows."""
    rng = _rng(42)
    rows = []
    base_dt = datetime.datetime(2026, 1, 1, 0, 0, 0)

    for i in range(1, n + 1):
        first  = rng.choice(_FIRST_NAMES)
        last   = rng.choice(_LAST_NAMES)
        city, cc = rng.choice(_CITIES)
        dob_year = rng.randint(1960, 2000)
        dob_mon  = rng.randint(1, 12)
        dob_day  = rng.randint(1, 28)
        salary   = round(rng.uniform(30_000, 200_000), 2)
        tier     = rng.choice(_TIERS)
        street_no = rng.randint(1, 999)
        street    = rng.choice(_STREETS)
        # Deterministic email and national_id derived from i
        tag    = hashlib.md5(f"{first}{last}{i}".encode()).hexdigest()[:6]
        email  = f"{first.lower()}.{last.lower()}.{tag}@example.com"
        nat_id = f"ID-{i:05d}-{tag.upper()[:4]}"
        # Fake IP
        ip = ".".join(str(rng.randint(1, 254)) for _ in range(4))
        phone = (
            f"+{rng.randint(1,99)}-{rng.randint(100,999)}-"
            f"{rng.randint(1000,9999)}"
        )
        created = base_dt + datetime.timedelta(days=rng.randint(0, 365))
        updated = created + datetime.timedelta(days=rng.randint(0, 30))

        rows.append(Row(
            customer_id   = i,
            full_name     = f"{first} {last}",
            email         = email,
            phone_number  = phone,
            date_of_birth = datetime.date(dob_year, dob_mon, dob_day),
            national_id   = nat_id,
            street_address= f"{street_no} {street}",
            city          = city,
            country_code  = cc,
            ip_address    = ip,
            salary        = salary,
            customer_tier = tier,
            is_active     = 1,
            created_at    = created,
            updated_at    = updated,
        ))
    return rows


# ── Spark session factory ──────────────────────────────────────────────────────

def _build_spark(bao: BaoSparkInit) -> SparkSession:
    """
    Build a SparkSession with:
      • `databricks` catalog alias → Polaris REST, warehouse=star_lakehouse
      • S3A credentials from OpenBao
      • Gluten/Velox (unless DISABLE_GLUTEN=1)
    """
    import socket as _socket
    s3  = bao.s3_creds()
    pol = bao.polaris_creds()

    driver_ip = os.environ.get(
        "SPARK_LOCAL_IP",
        _socket.gethostbyname(_socket.gethostname()),
    )

    _log4j = "-Dlog4j.configurationFile=/opt/spark/conf/log4j2.properties"
    _opens = " ".join([
        _log4j,
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    ])

    builder = (
        SparkSession.builder
        .master(_SPARK_MASTER)
        .appName("databricks-customer-seed")
        .config("spark.driver.host",        driver_ip)
        .config("spark.driver.bindAddress", driver_ip)
        .config("spark.driver.extraJavaOptions",   _opens)
        .config("spark.executor.extraJavaOptions", _opens)
        # Iceberg extension
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # databricks catalog → Polaris REST (warehouse: star_lakehouse)
        .config("spark.sql.catalog.databricks",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.databricks.type",             "rest")
        .config("spark.sql.catalog.databricks.uri",              _POLARIS_URI)
        .config("spark.sql.catalog.databricks.oauth2-server-uri",
                f"{_POLARIS_URI}/v1/oauth/tokens")
        .config("spark.sql.catalog.databricks.credential",
                f"{pol['spark_svc_id']}:{pol['spark_svc_secret']}")
        .config("spark.sql.catalog.databricks.scope",            "PRINCIPAL_ROLE:ALL")
        .config("spark.sql.catalog.databricks.warehouse",        _POLARIS_WAREHOUSE)
        .config("spark.sql.catalog.databricks.rest.auth.type",   "oauth2")
        # Iceberg S3FileIO (AWS SDK v2) for the databricks catalog
        .config("spark.sql.catalog.databricks.s3.access-key-id",     s3["access_key"])
        .config("spark.sql.catalog.databricks.s3.secret-access-key", s3["secret_key"])
        .config("spark.sql.catalog.databricks.s3.endpoint",          s3["endpoint"])
        .config("spark.sql.catalog.databricks.s3.path-style-access", "true")
        .config("spark.sql.catalog.databricks.client.region",        s3["region"])
        # S3A (Hadoop)
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key",        s3["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key",        s3["secret_key"])
        .config("spark.hadoop.fs.s3a.endpoint",          s3["endpoint"])
        .config("spark.hadoop.fs.s3a.endpoint.region",   s3["region"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config("spark.hadoop.fs.s3.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.access.key",        s3["access_key"])
        .config("spark.hadoop.fs.s3.secret.key",        s3["secret_key"])
        .config("spark.hadoop.fs.s3.endpoint",          s3["endpoint"])
        .config("spark.hadoop.fs.s3.path.style.access", "true")
        # Serializer
        .config("spark.serializer",                "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrationRequired", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )

    if os.environ.get("DISABLE_GLUTEN", "0") != "1":
        builder = (
            builder
            .config("spark.plugins",                         "org.apache.gluten.GlutenPlugin")
            .config("spark.gluten.sql.columnar.backend.lib", "velox")
            .config("spark.memory.offHeap.enabled",          "true")
            .config("spark.memory.offHeap.size",             "2g")
        )

    return builder.getOrCreate()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    os.environ["USER"] = USER

    logger.info(
        "=== customer-seed | catalog=%s ns=%s table=%s bucket=%s rows=%d ===",
        CATALOG, NAMESPACE, TABLE, S3_BUCKET, ROWS,
    )

    bao   = BaoSparkInit()
    spark = _build_spark(bao)
    spark.sparkContext.setLogLevel("WARN")

    builder = IcebergTableBuilder(spark, running_user=USER)

    # (a) Ensure namespace
    builder.ensure_namespace(CATALOG, NAMESPACE)

    # (a)+(b) Create Iceberg table with explicit S3 location on stardata_databricks
    s3_location = f"s3://{S3_BUCKET}/iceberg/warehouse/{NAMESPACE}/{TABLE}"
    fqn = builder.create_table(
        catalog        = CATALOG,
        namespace      = NAMESPACE,
        table          = TABLE,
        schema         = CUSTOMER_SCHEMA,
        partition_spec = [
            IcebergTableBuilder.hours("snap_timestamp"),
            IcebergTableBuilder.bucket("snap_id", 4),
        ],
        location       = s3_location,
    )
    logger.info("Table ready: %s  location=%s", fqn, s3_location)

    if DRY_RUN:
        logger.info("DRY_RUN=1 — skipping data insert.")
        spark.sql(f"SELECT COUNT(*) AS total FROM `{CATALOG}`.`{NAMESPACE}`.`{TABLE}`").show()
        return

    # (c) Generate and insert 1 000 rows
    rows = _gen_rows(ROWS)
    df   = spark.createDataFrame(rows, schema=CUSTOMER_SCHEMA)

    n = builder.write_append(df, catalog=CATALOG, namespace=NAMESPACE, table=TABLE)
    logger.info("Inserted %d rows into %s", n, fqn)

    spark.sql(
        f"SELECT COUNT(*) AS total FROM `{CATALOG}`.`{NAMESPACE}`.`{TABLE}`"
    ).show()
    spark.sql(f"""
        SELECT customer_id, full_name, city, customer_tier
        FROM `{CATALOG}`.`{NAMESPACE}`.`{TABLE}`
        ORDER BY customer_id
        LIMIT 10
    """).show(truncate=False)


if __name__ == "__main__":
    main()
