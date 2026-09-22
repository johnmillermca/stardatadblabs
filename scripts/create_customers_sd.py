#!/usr/bin/env python3
import os, sys
sys.path.insert(0, '/opt/spark/work-dir')
os.environ.setdefault('SPARK_USER', 'dave')

from bao_spark_init import BaoSparkInit
from spark_iceberg_utils import IcebergTableBuilder
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType, StringType, TimestampType, BooleanType, DoubleType,
    IntegerType, StructType, StructField
)

bao   = BaoSparkInit()
conf  = bao.spark_conf(app_name='create-customers-sd',
                        extra_conf={"spark.cores.max": "1",
                                    "spark.executor.instances": "1",
                                    "spark.executor.cores": "1",
                                    "spark.executor.memory": "2g"})
spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
builder = IcebergTableBuilder(spark, running_user='dave')

# Drop the table if it already exists (e.g. from a prior failed run with wrong schema)
builder.drop_table('mongodb', 'cache_testing', 'customers_sd')

builder.ensure_namespace('mongodb', 'cache_testing')

# Real schema from cache_testing.customers MongoDB collection
schema = StructType([
    StructField('customer_id',  LongType(),      False),
    StructField('first_name',   StringType(),    True),
    StructField('last_name',    StringType(),    True),
    StructField('email',        StringType(),    True),
    StructField('phone',        StringType(),    True),
    StructField('city',         StringType(),    True),
    StructField('country_code', StringType(),    True),
    StructField('tier',         StringType(),    True),
    StructField('credit_limit', DoubleType(),    True),
    StructField('is_active',    BooleanType(),   True),
    StructField('created_at',   TimestampType(), True),
    StructField('updated_at',   TimestampType(), True),
    # soft_delete extras
    StructField('is_deleted',   BooleanType(),   True),
    StructField('deleted_at',   TimestampType(), True),
])

builder.create_table(
    catalog   = 'mongodb',
    namespace = 'cache_testing',
    table     = 'customers_sd',
    schema    = schema,
    partition_spec = [
        IcebergTableBuilder.hours('snap_timestamp'),
        IcebergTableBuilder.bucket('customer_id', 16),
    ],
    location = 's3://xdatatoiceberg1/iceberg/mgo_lakehouse/cache_testing/customers_sd',
    extra_properties = {
        'pipeline.write-mode': 'soft_delete',
        'pipeline.source':     'mongodb',
    },
)
spark.stop()
print('TABLE_CREATED_OK')
