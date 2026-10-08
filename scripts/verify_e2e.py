import sys
sys.path.insert(0, '/opt/spark/work-dir')
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession

bao  = BaoSparkInit()
conf = bao.spark_conf(app_name='e2e-verify')
spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

def q(label, sql):
    try:
        rows = spark.sql(sql).collect()
        summary = "  ".join(str({k: v for k, v in r.asDict().items()}) for r in rows) if rows else "NOT FOUND"
        print(f"RESULT {label}  rows={len(rows)}  {summary}")
    except Exception as e:
        print(f"RESULT {label}  ERROR: {str(e)[:200]}")

q("PG_std ", "SELECT id,name,tier,snap_id FROM `postgres`.`cache_testing`.`customers` WHERE id=99000010")
q("ORA_std", "SELECT customer_id,tier,snap_id FROM `oracle`.`cache_testing`.`customers` WHERE customer_id=99000020")
q("MDB_std", "SELECT customer_id,tier,snap_id FROM `mongodb`.`cache_testing`.`customers` WHERE customer_id=99000030")
q("PG_sd  ", "SELECT id,tier,is_deleted,deleted_at FROM `postgres`.`cache_testing`.`customers_sd` WHERE id=99000010")
q("ORA_sd ", "SELECT customer_id,tier,is_deleted,deleted_at FROM `oracle`.`cache_testing`.`customers_sd` WHERE customer_id=99000020")
q("MDB_sd ", "SELECT customer_id,tier,is_deleted,deleted_at FROM `mongodb`.`cache_testing`.`customers_sd` WHERE customer_id=99000030")
q("PG_ht  ", "SELECT after_id,after_tier,_change_type FROM `postgres`.`cache_testing`.`customers_hist` WHERE after_id=99000010 OR before_id=99000010 ORDER BY _change_ts")
q("ORA_ht ", "SELECT customer_id,after_tier,_change_type FROM `oracle`.`cache_testing`.`customers_hist` WHERE customer_id=99000020 ORDER BY _change_ts")
q("MDB_ht ", "SELECT customer_id,after_tier,_change_type FROM `mongodb`.`cache_testing`.`customers_hist` WHERE customer_id=99000030 ORDER BY _change_ts")

spark.stop()
