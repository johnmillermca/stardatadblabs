#!/usr/bin/env python3
"""
scripts/create_customers_hist.py
=================================
One-shot provisioner — drops (if exists) and recreates
  mongodb.cache_testing.customers_hist

Schema
------
  _change_type       STRING        — INSERT / UPDATE / DELETE
  _change_ts         TIMESTAMP     — pipeline processing time
  customer_id        BIGINT        — top-level PK (coalesced from after_/before_)
  after_customer_id  BIGINT        — after image PK
  after_first_name   STRING
  after_last_name    STRING
  after_email        STRING
  after_phone        STRING
  after_city         STRING
  after_country_code STRING
  after_tier         STRING
  after_credit_limit DOUBLE
  after_is_active    BOOLEAN
  after_created_at   TIMESTAMP
  after_updated_at   TIMESTAMP
  before_customer_id BIGINT
  before_first_name  STRING
  before_last_name   STRING
  before_email       STRING
  before_phone       STRING
  before_city        STRING
  before_country_code STRING
  before_tier        STRING
  before_credit_limit DOUBLE
  before_is_active   BOOLEAN
  before_created_at  TIMESTAMP
  before_updated_at  TIMESTAMP
  snap_id            BIGINT
  snap_timestamp     TIMESTAMP

Partitioning: hours(snap_timestamp)
Location: s3://xdatatoiceberg1/iceberg/mgo_lakehouse/cache_testing/customers_hist

Usage
-----
  kubectl exec -n prod <history-tracking-pod> -- \\
    bash -c "cd /opt/spark/work-dir && python3 /tmp/create_customers_hist.py"
"""

import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType, DoubleType, LongType, StringType,
    StructField, StructType, TimestampType,
)

sys.path.insert(0, "/opt/spark/work-dir")
os.environ.setdefault("SPARK_USER", "dave")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("create-customers-hist")

CATALOG   = "mongodb"
NAMESPACE = "cache_testing"
TABLE     = "customers_hist"
S3_LOC    = "s3://xdatatoiceberg1/iceberg/mgo_lakehouse/cache_testing/customers_hist"

FQN_BT    = f"`{CATALOG}`.`{NAMESPACE}`.`{TABLE}`"
FQN_PL    = f"{CATALOG}.{NAMESPACE}.{TABLE}"

_S = StructField

SCHEMA = StructType([
    _S("_change_type",       StringType(),    True),
    _S("_change_ts",         TimestampType(), True),
    # top-level PK — coalesced from after_/before_ so every op type is queryable
    _S("customer_id",        LongType(),      True),
    # after image
    _S("after_customer_id",  LongType(),      True),
    _S("after_first_name",   StringType(),    True),
    _S("after_last_name",    StringType(),    True),
    _S("after_email",        StringType(),    True),
    _S("after_phone",        StringType(),    True),
    _S("after_city",         StringType(),    True),
    _S("after_country_code", StringType(),    True),
    _S("after_tier",         StringType(),    True),
    _S("after_credit_limit", DoubleType(),    True),
    _S("after_is_active",    BooleanType(),   True),
    _S("after_created_at",   TimestampType(), True),
    _S("after_updated_at",   TimestampType(), True),
    # before image
    _S("before_customer_id",  LongType(),     True),
    _S("before_first_name",   StringType(),   True),
    _S("before_last_name",    StringType(),   True),
    _S("before_email",        StringType(),   True),
    _S("before_phone",        StringType(),   True),
    _S("before_city",         StringType(),   True),
    _S("before_country_code", StringType(),   True),
    _S("before_tier",         StringType(),   True),
    _S("before_credit_limit", DoubleType(),   True),
    _S("before_is_active",    BooleanType(),  True),
    _S("before_created_at",   TimestampType(), True),
    _S("before_updated_at",   TimestampType(), True),
    # snap metadata
    _S("snap_id",             LongType(),     True),
    _S("snap_timestamp",      TimestampType(), True),
])


def _py_to_iceberg(dtype) -> str:
    return {
        "LongType":      "BIGINT",
        "IntegerType":   "INT",
        "StringType":    "STRING",
        "DoubleType":    "DOUBLE",
        "BooleanType":   "BOOLEAN",
        "TimestampType": "TIMESTAMP",
    }.get(type(dtype).__name__, "STRING")


def main() -> None:
    from bao_spark_init import BaoSparkInit
    bao  = BaoSparkInit()
    conf = bao.spark_conf(app_name="create-customers-hist")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    log.info("Dropping %s (if exists)…", FQN_BT)
    spark.sql(f"DROP TABLE IF EXISTS {FQN_BT}")

    col_defs = ",\n    ".join(
        f"`{f.name}` {_py_to_iceberg(f.dataType)}"
        for f in SCHEMA.fields
    )
    ddl = f"""
        CREATE TABLE {FQN_BT} (
            {col_defs}
        )
        USING iceberg
        PARTITIONED BY (hours(snap_timestamp))
        LOCATION '{S3_LOC}'
        TBLPROPERTIES (
            'pipeline.write-mode' = 'history_tracking',
            'pipeline.source'     = 'mongodb',
            'pipeline.pk-col'     = 'customer_id'
        )
    """
    log.info("Creating %s…", FQN_BT)
    spark.sql(ddl)
    log.info("Created %s at %s", FQN_BT, S3_LOC)

    # Verify
    log.info("=== Schema ===")
    spark.sql(f"DESCRIBE TABLE {FQN_BT}").show(50, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
