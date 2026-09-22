#!/usr/bin/env python3
"""
perf_iceberg_verify.py
======================
Queries mongodb.cache_testing.customers_sd and reports:
  - Row counts (total / live / soft-deleted) for the perf test range
  - Latency: snap_timestamp vs the earliest INSERT wall-clock time
  - Sample rows from each operation type
  - Batch-level throughput from the Iceberg snapshot history
"""
import os, sys, json, datetime
sys.path.insert(0, '/opt/spark/work-dir')
os.environ['SPARK_USER'] = 'dave'

INSERT_START_ID = 99_000_001
INSERT_COUNT    = 2_000
UPDATE_COUNT    = 100
DELETE_COUNT    = 1_000
INSERT_END_ID   = INSERT_START_ID + INSERT_COUNT - 1

# Read timestamps written by load generator
try:
    with open('/tmp/perf_timestamps.json') as fh:
        ts = json.load(fh)
    insert_start = ts['insert']['start']
    delete_end   = ts['delete']['end']
    print(f"[VERIFY] Load generator timestamps loaded.")
    print(f"  INSERT started : {insert_start}")
    print(f"  DELETE ended   : {delete_end}")
except Exception as e:
    insert_start = None
    delete_end   = None
    print(f"[VERIFY] No timestamp file found ({e}) — latency will use earliest snap_timestamp.")

from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession

bao  = BaoSparkInit()
conf = bao.spark_conf(app_name='perf-verify',
    extra_conf={'spark.cores.max':'1','spark.executor.instances':'1',
                'spark.executor.cores':'1','spark.executor.memory':'2g'})
spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

TABLE = 'mongodb.cache_testing.customers_sd'
RANGE_FILTER = f'customer_id BETWEEN {INSERT_START_ID} AND {INSERT_END_ID}'

print(f"\n{'='*65}")
print(f"  Iceberg table  : {TABLE}")
print(f"  Test range     : customer_id {INSERT_START_ID} – {INSERT_END_ID}")
print(f"{'='*65}\n")

# ── 1. Row counts ─────────────────────────────────────────────────────────────
counts = spark.sql(f"""
    SELECT
        COUNT(*)                          AS total_rows,
        SUM(CASE WHEN is_deleted = false OR is_deleted IS NULL THEN 1 ELSE 0 END) AS live_rows,
        SUM(CASE WHEN is_deleted = true  THEN 1 ELSE 0 END)  AS soft_deleted_rows
    FROM {TABLE}
    WHERE {RANGE_FILTER}
""").collect()[0]

print("ROW COUNTS (test range only)")
print(f"  Total rows       : {counts['total_rows']:,}")
print(f"  Live (not deleted): {counts['live_rows']:,}")
print(f"  Soft-deleted     : {counts['soft_deleted_rows']:,}")
print()

# ── 2. Latency — first + last snap_timestamp in test range ───────────────────
latency = spark.sql(f"""
    SELECT
        MIN(snap_timestamp) AS first_iceberg_write,
        MAX(snap_timestamp) AS last_iceberg_write,
        MIN(CASE WHEN is_deleted=false OR is_deleted IS NULL THEN snap_timestamp END) AS first_live_write,
        MIN(CASE WHEN is_deleted=true  THEN snap_timestamp END) AS first_softdel_write,
        COUNT(DISTINCT DATE_TRUNC('HOUR', snap_timestamp))       AS partition_hours
    FROM {TABLE}
    WHERE {RANGE_FILTER}
""").collect()[0]

first_write = latency['first_iceberg_write']
last_write  = latency['last_iceberg_write']

print("PIPELINE LATENCY")
if insert_start and first_write:
    from datetime import timezone
    t_src  = datetime.datetime.fromisoformat(insert_start.rstrip('Z')).replace(tzinfo=timezone.utc)
    t_ice  = first_write.astimezone(timezone.utc) if hasattr(first_write, 'astimezone') else first_write
    # pyspark Timestamp → datetime
    t_ice_dt = datetime.datetime(t_ice.year, t_ice.month, t_ice.day,
                                  t_ice.hour, t_ice.minute, t_ice.second,
                                  tzinfo=timezone.utc)
    e2e_s = (t_ice_dt - t_src).total_seconds()
    print(f"  First INSERT to MongoDB   : {insert_start}")
    print(f"  First row in Iceberg      : {first_write}")
    print(f"  End-to-end latency        : {e2e_s:.1f}s  (MongoDB insert → Iceberg commit)")
else:
    print(f"  First row in Iceberg      : {first_write}")

print(f"  Last  row in Iceberg      : {last_write}")
print(f"  First live-write          : {latency['first_live_write']}")
print(f"  First soft-delete write   : {latency['first_softdel_write']}")
print(f"  Partition hours touched   : {latency['partition_hours']}")
print()

# ── 3. Throughput — rows per snap_timestamp bucket (= per Spark batch) ────────
print("BATCH THROUGHPUT (rows written per Spark micro-batch)")
spark.sql(f"""
    SELECT
        snap_timestamp,
        SUM(CASE WHEN is_deleted=false OR is_deleted IS NULL THEN 1 ELSE 0 END) AS upserts,
        SUM(CASE WHEN is_deleted=true  THEN 1 ELSE 0 END)  AS soft_deletes,
        COUNT(*) AS total
    FROM {TABLE}
    WHERE {RANGE_FILTER}
    GROUP BY snap_timestamp
    ORDER BY snap_timestamp
""").show(50, truncate=False)

# ── 4. Sample rows — 5 live + 5 soft-deleted from test range ─────────────────
print("SAMPLE ROWS — 5 live (is_deleted=false)")
spark.sql(f"""
    SELECT customer_id, first_name, email, city, tier, is_deleted, deleted_at, snap_timestamp
    FROM {TABLE}
    WHERE {RANGE_FILTER} AND (is_deleted = false OR is_deleted IS NULL)
    ORDER BY customer_id
    LIMIT 5
""").show(truncate=False)

print("SAMPLE ROWS — 5 soft-deleted (is_deleted=true)")
spark.sql(f"""
    SELECT customer_id, first_name, email, city, tier, is_deleted, deleted_at, snap_timestamp
    FROM {TABLE}
    WHERE {RANGE_FILTER} AND is_deleted = true
    ORDER BY customer_id
    LIMIT 5
""").show(truncate=False)

print("SAMPLE ROWS — 5 updated rows (PLATINUM tier, is_deleted=true — were updated then deleted)")
spark.sql(f"""
    SELECT customer_id, first_name, email, tier, credit_limit, is_deleted, deleted_at, snap_timestamp
    FROM {TABLE}
    WHERE {RANGE_FILTER} AND tier = 'PLATINUM' AND is_deleted = true
    ORDER BY customer_id
    LIMIT 5
""").show(truncate=False)

spark.stop()
print("DONE")
