import sys, time
sys.path.insert(0, '/opt/spark/work-dir')

from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
from datetime import datetime, timezone, timedelta

bao = BaoSparkInit()
conf = bao.spark_conf(app_name="streaming-verify")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

NOW = datetime.now(timezone.utc)
CUTOFF_30M = NOW - timedelta(minutes=30)
CUTOFF_10H = NOW - timedelta(hours=10)

TABLES = [
    ('postgres', 'cache_testing', 'customers'),
    ('postgres', 'cache_testing', 'products'),
    ('postgres', 'cache_testing', 'orders'),
    ('postgres', 'cache_testing', 'product_reviews'),
    ('oracle',   'cache_testing', 'customers'),
    ('oracle',   'cache_testing', 'products'),
    ('oracle',   'cache_testing', 'orders'),
    ('oracle',   'cache_testing', 'order_items'),
    ('oracle',   'cache_testing', 'product_reviews'),
    ('oracle',   'cache_testing', 'inventory_events'),
    ('mongodb',  'cache_testing', 'customers'),
    ('mongodb',  'cache_testing', 'products'),
]

print(f"\n{'TABLE':<45} {'LAST SNAPSHOT':<26} {'OP':<11} {'ADDED':>7} {'TOTAL':>10}  STATUS")
print("─" * 115)

fresh = stale = error = 0

for cat, ns, tbl in TABLES:
    label = f"{cat}.{ns}.{tbl}"
    try:
        row = spark.sql(f"""
            SELECT committed_at, operation,
                   summary['added-records']  AS added,
                   summary['total-records']  AS total
            FROM `{cat}`.`{ns}`.`{tbl}`.snapshots
            ORDER BY committed_at DESC LIMIT 1
        """).first()

        if not row:
            print(f"  {label:<43} {'(no snapshots)':<26} {'':>11} {'':>7} {'':>10}  ⚠ NO DATA")
            error += 1
            continue

        snap_ts = row.committed_at.replace(tzinfo=timezone.utc)
        age_s   = int((NOW - snap_ts).total_seconds())

        if snap_ts >= CUTOFF_30M:
            status = "✅ LIVE (<30m)"
            fresh += 1
        elif snap_ts >= CUTOFF_10H:
            status = "🟡 RECENT (<10h)"
            fresh += 1
        else:
            hrs = age_s // 3600
            status = f"❌ STALE ({hrs}h ago)"
            stale += 1

        print(f"  {label:<43} {str(row.committed_at):<26} {str(row.operation):<11} {str(row.added):>7} {str(row.total):>10}  {status}")
    except Exception as e:
        msg = str(e)[:60]
        print(f"  {label:<43} {'ERROR: ' + msg}")
        error += 1

print("─" * 115)
print(f"\n  SUMMARY: {fresh} live/recent  |  {stale} stale  |  {error} error/missing")
print(f"  Check time: {NOW.strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
