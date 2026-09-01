import os
os.environ["USER"] = "dave"
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession

bao   = BaoSparkInit()
conf  = bao.spark_conf(app_name="namespace-check")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=== Catalogs ===")
spark.sql("SHOW CATALOGS").show()

print("=== Namespaces in databricks catalog ===")
spark.sql("SHOW NAMESPACES IN databricks").show()

print("=== Tables in databricks.lakehouse_db ===")
spark.sql("SHOW TABLES IN databricks.lakehouse_db").show()

spark.stop()
print("Test (b) PASSED")
