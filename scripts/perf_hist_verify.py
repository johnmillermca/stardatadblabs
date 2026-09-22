#!/usr/bin/env python3
"""
perf_hist_verify.py
====================
Verifies history_tracking Iceberg table after the load generator run.

Reports:
  - Row counts per _change_type (INSERT / UPDATE / DELETE)
  - Append-only confirmation (no row was ever updated/deleted in Iceberg)
  - End-to-end pipeline latency (MongoDB insert → Iceberg snap_timestamp)
  - Batch-level throughput (rows per Spark micro-batch / snap_timestamp)
  - Full audit trail completeness check
  - Sample rows with snap_id and snap_timestamp

Usage (run inside any spark pod after ~120s):
  python3 /tmp/perf_hist_verify.py
"""

import datetime
import json
import os
import sys

sys.path.insert(0, "/opt/spark/work-dir")
os.environ["SPARK_USER"] = "dave"

INSERT_START_ID = 98_000_001
INSERT_COUNT    = 10_000
UPDATE_COUNT    =  1_000
DELETE_COUNT    =  5_000
INSERT_END_ID   = INSERT_START_ID + INSERT_COUNT - 1

# Load wall-clock timestamps written by the load generator
try:
    with open("/tmp/perf_hist_timestamps.json") as fh:
        ts = json.load(fh)
    insert_start = ts["insert"]["start"]
    delete_end   = ts["delete"]["end"]
    print("[VERIFY] Load generator timestamps loaded.")
    print(f"  INSERT started : {insert_start}")
    print(f"  DELETE ended   : {delete_end}")
except Exception as exc:
    insert_start = None
    delete_end   = None
    print(f"[VERIFY] No timestamp file ({exc}) — latency computed from snap_timestamp only.")

from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession

bao   = BaoSparkInit()
conf  = bao.spark_conf(app_name="perf-hist-verify")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

TABLE         = "mongodb.cache_testing.customers_hist"
RANGE_FILTER  = f"customer_id BETWEEN {INSERT_START_ID} AND {INSERT_END_ID}"

print(f"\n{'='*70}")
print(f"  Iceberg table  : {TABLE}")
print(f"  Test range     : customer_id {INSERT_START_ID:,} – {INSERT_END_ID:,}")
print(f"  Expected events: {INSERT_COUNT:,} INS  {UPDATE_COUNT:,} UPD  {DELETE_COUNT:,} DEL")
print(f"{'='*70}\n")


# ── 1. Row counts by change type ──────────────────────────────────────────────
print("━" * 70)
print("1. ROW COUNTS BY CHANGE TYPE  (test range only)")
print("━" * 70)
spark.sql(f"""
    SELECT
        _change_type,
        COUNT(*)                          AS rows,
        COUNT(DISTINCT customer_id)       AS unique_customers,
        MIN(snap_timestamp)               AS earliest_snap,
        MAX(snap_timestamp)               AS latest_snap
    FROM {TABLE}
    WHERE {RANGE_FILTER}
    GROUP BY _change_type
    ORDER BY _change_type
""").show(truncate=False)

total_rows = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {TABLE} WHERE {RANGE_FILTER}
""").collect()[0]["n"]
print(f"  TOTAL rows in test range: {total_rows:,}  "
      f"(expected {INSERT_COUNT + UPDATE_COUNT + DELETE_COUNT:,})\n")


# ── 2. Append-only confirmation ───────────────────────────────────────────────
print("━" * 70)
print("2. APPEND-ONLY INTEGRITY  (no row should appear more than once per snap_id)")
print("━" * 70)
dupe_snap = spark.sql(f"""
    SELECT snap_id, COUNT(*) AS occurrences
    FROM {TABLE}
    WHERE {RANGE_FILTER}
    GROUP BY snap_id
    HAVING COUNT(*) > 1
""").count()
print(f"  Duplicate snap_id rows : {dupe_snap}  (expected 0 — all snap_ids unique)\n")


# ── 3. End-to-end latency ─────────────────────────────────────────────────────
print("━" * 70)
print("3. END-TO-END PIPELINE LATENCY")
print("━" * 70)

lat = spark.sql(f"""
    SELECT
        MIN(snap_timestamp) AS first_iceberg_write,
        MAX(snap_timestamp) AS last_iceberg_write,
        MIN(CASE WHEN _change_type='INSERT' THEN snap_timestamp END) AS first_insert_write,
        MIN(CASE WHEN _change_type='UPDATE' THEN snap_timestamp END) AS first_update_write,
        MIN(CASE WHEN _change_type='DELETE' THEN snap_timestamp END) AS first_delete_write,
        MAX(CASE WHEN _change_type='DELETE' THEN snap_timestamp END) AS last_delete_write,
        COUNT(DISTINCT DATE_TRUNC('HOUR', snap_timestamp))            AS partition_hours
    FROM {TABLE}
    WHERE {RANGE_FILTER}
""").collect()[0]

first_write = lat["first_iceberg_write"]
last_write  = lat["last_iceberg_write"]

if insert_start and first_write:
    from datetime import timezone as _tz
    t_src    = datetime.datetime.fromisoformat(
                   insert_start.rstrip("Z")).replace(tzinfo=_tz.utc)
    t_ice_dt = datetime.datetime(
                   first_write.year, first_write.month, first_write.day,
                   first_write.hour, first_write.minute, first_write.second,
                   tzinfo=_tz.utc)
    e2e_first = (t_ice_dt - t_src).total_seconds()
    print(f"  First INSERT → MongoDB   : {insert_start}")
    print(f"  First row in Iceberg     : {first_write}")
    print(f"  ⮕  First-row e2e latency : {e2e_first:.1f}s")

    if delete_end and last_write:
        t_del_end = datetime.datetime.fromisoformat(
                        delete_end.rstrip("Z")).replace(tzinfo=_tz.utc)
        t_last_dt = datetime.datetime(
                        last_write.year, last_write.month, last_write.day,
                        last_write.hour, last_write.minute, last_write.second,
                        tzinfo=_tz.utc)
        full_pipe = (t_last_dt - t_src).total_seconds()
        tail_lag  = (t_last_dt - t_del_end).total_seconds()
        print(f"  Last DELETE → MongoDB    : {delete_end}")
        print(f"  Last row in Iceberg      : {last_write}")
        print(f"  ⮕  Full-pipeline elapsed : {full_pipe:.1f}s  "
              f"(INSERT start → last row committed)")
        print(f"  ⮕  Tail-commit lag       : {tail_lag:.1f}s  "
              f"(last DELETE → Iceberg commit)")
else:
    print(f"  First row in Iceberg     : {first_write}")
    print(f"  Last  row in Iceberg     : {last_write}")

print(f"  First INSERT commit      : {lat['first_insert_write']}")
print(f"  First UPDATE commit      : {lat['first_update_write']}")
print(f"  First DELETE commit      : {lat['first_delete_write']}")
print(f"  Partition hours touched  : {lat['partition_hours']}\n")


# ── 4. Batch-level throughput (rows per snap_timestamp = per Spark micro-batch)
print("━" * 70)
print("4. BATCH THROUGHPUT  (rows written per Spark micro-batch)")
print("━" * 70)
spark.sql(f"""
    SELECT
        snap_timestamp,
        SUM(CASE WHEN _change_type='INSERT' THEN 1 ELSE 0 END) AS inserts,
        SUM(CASE WHEN _change_type='UPDATE' THEN 1 ELSE 0 END) AS updates,
        SUM(CASE WHEN _change_type='DELETE' THEN 1 ELSE 0 END) AS deletes,
        COUNT(*) AS total
    FROM {TABLE}
    WHERE {RANGE_FILTER}
    GROUP BY snap_timestamp
    ORDER BY snap_timestamp
""").show(100, truncate=False)


# ── 5. UPDATE completeness — before/after image check ────────────────────────
print("━" * 70)
print("5. UPDATE IMAGE COMPLETENESS  (UPDATE rows should carry both before/after)")
print("━" * 70)
spark.sql(f"""
    SELECT
        COUNT(*) AS update_rows,
        SUM(CASE WHEN before_customer_id IS NOT NULL THEN 1 ELSE 0 END) AS before_present,
        SUM(CASE WHEN after_customer_id  IS NOT NULL THEN 1 ELSE 0 END) AS after_present,
        SUM(CASE WHEN before_tier  != after_tier  THEN 1 ELSE 0 END)    AS tier_changed,
        SUM(CASE WHEN before_credit_limit != after_credit_limit
                 THEN 1 ELSE 0 END)                                      AS credit_changed
    FROM {TABLE}
    WHERE {RANGE_FILTER} AND _change_type='UPDATE'
""").show(truncate=False)


# ── 6. DELETE completeness — before image carries last-known state ────────────
print("━" * 70)
print("6. DELETE IMAGE COMPLETENESS  (DELETE rows should carry before image)")
print("━" * 70)
spark.sql(f"""
    SELECT
        COUNT(*) AS delete_rows,
        SUM(CASE WHEN before_customer_id IS NOT NULL THEN 1 ELSE 0 END) AS before_present,
        SUM(CASE WHEN after_customer_id  IS NULL     THEN 1 ELSE 0 END) AS after_null
    FROM {TABLE}
    WHERE {RANGE_FILTER} AND _change_type='DELETE'
""").show(truncate=False)


# ── 7. Sample rows — 5 INSERTs, 3 UPDATEs, 3 DELETEs ────────────────────────
print("━" * 70)
print("7. SAMPLE ROWS — 5 INSERTs  (with snap_id, snap_timestamp)")
print("━" * 70)
spark.sql(f"""
    SELECT
        snap_id, snap_timestamp, _change_type, customer_id,
        after_first_name, after_city, after_tier, after_credit_limit,
        after_created_at
    FROM {TABLE}
    WHERE {RANGE_FILTER} AND _change_type = 'INSERT'
    ORDER BY snap_id
    LIMIT 5
""").show(truncate=False)

print("━" * 70)
print("7b. SAMPLE ROWS — 5 UPDATEs  (before → after changes)")
print("━" * 70)
spark.sql(f"""
    SELECT
        snap_id, snap_timestamp, customer_id,
        before_tier,         after_tier,
        before_credit_limit, after_credit_limit,
        before_city,         after_city
    FROM {TABLE}
    WHERE {RANGE_FILTER} AND _change_type = 'UPDATE'
    ORDER BY snap_id
    LIMIT 5
""").show(truncate=False)

print("━" * 70)
print("7c. SAMPLE ROWS — 5 DELETEs  (before image = last known state)")
print("━" * 70)
spark.sql(f"""
    SELECT
        snap_id, snap_timestamp, customer_id,
        before_first_name, before_city, before_tier, before_credit_limit,
        after_customer_id
    FROM {TABLE}
    WHERE {RANGE_FILTER} AND _change_type = 'DELETE'
    ORDER BY snap_id
    LIMIT 5
""").show(truncate=False)


# ── 8. Full audit trail for one specific customer ─────────────────────────────
SAMPLE_CID = INSERT_START_ID  # 98000001 — INSERTed, UPDATEd, then DELETEd
print("━" * 70)
print(f"8. FULL AUDIT TRAIL — customer_id={SAMPLE_CID}  (should have 3 rows)")
print("━" * 70)
spark.sql(f"""
    SELECT
        snap_id, snap_timestamp, _change_type, _change_ts,
        after_tier, after_credit_limit, after_city,
        before_tier, before_credit_limit, before_city
    FROM {TABLE}
    WHERE customer_id = {SAMPLE_CID}
    ORDER BY snap_id
""").show(truncate=False)

spark.stop()
print("\n" + "=" * 70)
print("VERIFICATION COMPLETE")
print("=" * 70)
