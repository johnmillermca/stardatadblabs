#!/usr/bin/env python3
"""
generate_customers_iceberg.py
─────────────────────────────
Generates 10 000 synthetic customer rows (same schema as governance_demo.customers)
and writes them as an Iceberg table managed by Polaris REST catalog on S3.

Target:
  catalog   : star_lakehouse   (Polaris)
  namespace : demo
  table     : customers
  location  : s3://stardata-databricks/iceberg/warehouse/demo/customers/

Credentials are fetched from OpenBao at runtime via bao_spark_init.BaoSparkInit.
Additional Polaris/S3 overrides read from environment:
  POLARIS_CATALOG   default: star_lakehouse
  POLARIS_NS        default: demo
  POLARIS_TABLE     default: customers
  NUM_ROWS          default: 10000

Run via kubectl (spark-submit inside the spark-master pod):
  kubectl exec -n prod <spark-master-pod> -- spark-submit \
    --master spark://spark-master.prod.svc.cluster.local:7077 \
    /tmp/generate_customers_iceberg.py
"""

import os
import random
import string
import sys
from datetime import date, timedelta

# ── Spark / Iceberg bootstrap ──────────────────────────────────────────────────
sys.path.insert(0, "/opt/spark/scripts")
sys.path.insert(0, "/app/scripts")

from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DateType, DecimalType, IntegerType
)

# ── Config ─────────────────────────────────────────────────────────────────────
POLARIS_CATALOG = os.getenv("POLARIS_CATALOG", "star_lakehouse")
POLARIS_NS      = os.getenv("POLARIS_NS",      "demo")
POLARIS_TABLE   = os.getenv("POLARIS_TABLE",   "customers")
NUM_ROWS        = int(os.getenv("NUM_ROWS",    "10000"))
FULL_TABLE      = f"{POLARIS_CATALOG}.{POLARIS_NS}.{POLARIS_TABLE}"

# ── Seed data helpers ──────────────────────────────────────────────────────────
FIRST_NAMES = [
    "Alice","Bob","Carlos","Diana","Eve","Frank","Grace","Hiro",
    "Iris","James","Kiran","Laura","Mike","Nina","Oscar","Paula",
    "Quinn","Rafael","Sofia","Tom","Uma","Victor","Wendy","Xander",
    "Yuki","Zara","Ahmed","Beatrice","Chen","Dalia"
]
LAST_NAMES = [
    "Smith","Jones","Williams","Brown","Taylor","Davies","Wilson",
    "Evans","Thomas","Roberts","Johnson","White","Martin","Thompson",
    "Garcia","Martinez","Robinson","Clark","Lewis","Lee","Walker",
    "Hall","Allen","Young","Hernandez","King","Wright","Lopez","Hill"
]
DOMAINS = [
    "example.com","mail.com","inbox.net","webmail.org",
    "fastmail.io","proton.me","outlook.com","gmail.com"
]
COUNTRIES = ["US","CA","GB","AU","DE","FR","JP","SG","IN","BR"]
TIERS     = ["standard","silver","gold","platinum"]


def rand_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def rand_email(name: str) -> str:
    local = name.lower().replace(" ", ".") + str(random.randint(1, 999))
    return f"{local}@{random.choice(DOMAINS)}"


def rand_phone() -> str:
    return f"+1-{random.randint(200,999)}-555-{random.randint(1000,9999)}"


def rand_dob() -> date:
    start = date(1950, 1, 1)
    return start + timedelta(days=random.randint(0, 365 * 55))


def rand_ssn() -> str:
    return f"SSN-{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}"


def rand_address() -> str:
    streets = ["Main St","Oak Ave","Maple Blvd","Cedar Rd","Park Lane","River Dr"]
    return f"{random.randint(1, 9999)} {random.choice(streets)}"


def rand_city() -> str:
    cities = [
        "Toronto","London","Sydney","Berlin","Paris","Tokyo","Singapore",
        "Mumbai","São Paulo","New York","Chicago","Houston","Phoenix",
        "Philadelphia","Dallas","Austin","Seattle","Denver","Boston"
    ]
    return random.choice(cities)


def rand_ip() -> str:
    return f"{random.randint(1,254)}.{random.randint(0,254)}.{random.randint(0,254)}.{random.randint(1,254)}"


def rand_salary() -> float:
    return round(random.uniform(28000, 220000), 2)


def generate_rows(n: int) -> list:
    rows = []
    for i in range(1, n + 1):
        name = rand_name()
        rows.append((
            i,                                   # customer_id
            name,                                # full_name
            rand_email(name),                    # email
            rand_phone(),                        # phone_number
            rand_dob(),                          # date_of_birth
            rand_ssn(),                          # national_id
            rand_address(),                      # street_address
            rand_city(),                         # city
            random.choice(COUNTRIES),            # country_code
            rand_ip(),                           # ip_address
            rand_salary(),                       # salary
            random.choice(TIERS),                # customer_tier
            1,                                   # is_active
        ))
    return rows


SCHEMA = StructType([
    StructField("customer_id",    LongType(),        False),
    StructField("full_name",      StringType(),      False),
    StructField("email",          StringType(),      False),
    StructField("phone_number",   StringType(),      True),
    StructField("date_of_birth",  DateType(),        True),
    StructField("national_id",    StringType(),      True),
    StructField("street_address", StringType(),      True),
    StructField("city",           StringType(),      True),
    StructField("country_code",   StringType(),      False),
    StructField("ip_address",     StringType(),      True),
    StructField("salary",         DecimalType(15,2), True),
    StructField("customer_tier",  StringType(),      False),
    StructField("is_active",      IntegerType(),     False),
])


def main():
    print(f"[generate_customers_iceberg] target={FULL_TABLE} rows={NUM_ROWS}")

    # ── Load credentials from OpenBao ──────────────────────────────────────────
    bao = BaoSparkInit()
    conf = bao.get_spark_conf()

    # Override catalog to star_lakehouse
    conf.set(f"spark.sql.catalog.{POLARIS_CATALOG}",
             "org.apache.iceberg.spark.SparkCatalog")
    conf.set(f"spark.sql.catalog.{POLARIS_CATALOG}.type", "rest")
    conf.set(f"spark.sql.catalog.{POLARIS_CATALOG}.uri",
             "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog")
    conf.set(f"spark.sql.catalog.{POLARIS_CATALOG}.credential",
             f"{bao.get_secret('platform/polaris', 'spark_svc_id')}:"
             f"{bao.get_secret('platform/polaris', 'spark_svc_secret')}")
    conf.set(f"spark.sql.catalog.{POLARIS_CATALOG}.scope",    "PRINCIPAL_ROLE:ALL")
    conf.set(f"spark.sql.catalog.{POLARIS_CATALOG}.warehouse", POLARIS_CATALOG)
    conf.set(f"spark.sql.catalog.{POLARIS_CATALOG}.header.X-Iceberg-Access-Delegation", "vended-credentials")

    spark = SparkSession.builder \
        .config(conf=conf) \
        .appName("generate_customers_iceberg") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ── Create namespace ───────────────────────────────────────────────────────
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {POLARIS_CATALOG}.{POLARIS_NS}")
    print(f"[generate_customers_iceberg] namespace {POLARIS_CATALOG}.{POLARIS_NS} ready")

    # ── Generate data ──────────────────────────────────────────────────────────
    random.seed(42)
    print(f"[generate_customers_iceberg] generating {NUM_ROWS} rows ...")
    rows = generate_rows(NUM_ROWS)
    df = spark.createDataFrame(rows, schema=SCHEMA)

    # Add audit columns
    df = df \
        .withColumn("created_at",  F.current_timestamp()) \
        .withColumn("updated_at",  F.current_timestamp()) \
        .withColumn("snap_id",     F.lit(1).cast("long")) \
        .withColumn("snap_timestamp", F.current_timestamp())

    print(f"[generate_customers_iceberg] schema:")
    df.printSchema()

    # ── Write Iceberg table ────────────────────────────────────────────────────
    df.writeTo(FULL_TABLE) \
      .tableProperty("write.format.default",      "parquet") \
      .tableProperty("write.parquet.compression-codec", "snappy") \
      .tableProperty("write.target-file-size-bytes", str(256 * 1024 * 1024)) \
      .tableProperty("write.distribution-mode",   "hash") \
      .partitionedBy(F.bucket(8, "customer_id")) \
      .createOrReplace()

    count = spark.table(FULL_TABLE).count()
    print(f"[generate_customers_iceberg] ✅ wrote {count} rows to {FULL_TABLE}")

    spark.stop()


if __name__ == "__main__":
    main()
