import sys
sys.path.insert(0, '/opt/spark/work-dir')

from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession

bao = BaoSparkInit()
conf = bao.spark_conf(app_name="iceberg-dml-verify")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

TEST_IDS = list(range(2012051, 2012061))   # DML-01 … DML-10
IDS_STR  = ", ".join(str(i) for i in TEST_IDS)
KEPT_IDS = list(range(2012051, 2012056))   # DML-01..05 (not deleted)
DEL_IDS  = list(range(2012056, 2012061))   # DML-06..10 (deleted)

print("=" * 60)
print("ICEBERG REPLICATION VERIFICATION")
print("=" * 60)

# ── 1. Latest snapshot ──────────────────────────────────────────
print("\n[1] Latest snapshot on postgres.cache_testing.customers")
try:
    spark.sql("""
        SELECT snapshot_id, committed_at, operation,
               summary['added-records']   AS added,
               summary['deleted-records'] AS deleted,
               summary['total-records']   AS total
        FROM `postgres`.`cache_testing`.`customers`.snapshots
        ORDER BY committed_at DESC LIMIT 3
    """).show(truncate=False)
except Exception as e:
    print(f"  ERROR: {e}")

# ── 2. Check rows that should exist (DML-01..05, INSERTs+UPDATEs) ──
print("\n[2] Rows that should exist in Iceberg (DML-01 to DML-05, ids 2012051-2012055)")
try:
    df = spark.sql(f"""
        SELECT id, name, email, tier, loyalty_score, preferred_lang, snap_timestamp
        FROM `postgres`.`cache_testing`.`customers`
        WHERE id IN ({', '.join(str(i) for i in KEPT_IDS)})
        ORDER BY id
    """)
    rows = df.collect()
    if rows:
        print(f"  Found {len(rows)}/5 expected rows:")
        for r in rows:
            print(f"    id={r.id}  tier={r.tier}  score={r.loyalty_score}  lang={r.preferred_lang}  snap_ts={r.snap_timestamp}")
    else:
        print("  !! NONE of the inserted rows found in Iceberg")
except Exception as e:
    print(f"  ERROR: {e}")

# ── 3. Check rows that should be DELETED (DML-06..10) ──
print("\n[3] Rows that should be DELETED from Iceberg (DML-06 to DML-10, ids 2012056-2012060)")
try:
    df = spark.sql(f"""
        SELECT id, name, email
        FROM `postgres`.`cache_testing`.`customers`
        WHERE id IN ({', '.join(str(i) for i in DEL_IDS)})
    """)
    rows = df.collect()
    if rows:
        print(f"  !! {len(rows)} row(s) still present (not yet hard-deleted):")
        for r in rows:
            print(f"    id={r.id}  email={r.email}")
    else:
        print("  OK - All 5 deleted rows are gone from Iceberg (hard delete confirmed)")
except Exception as e:
    print(f"  ERROR: {e}")

# ── 4. Verify UPDATE values on DML-01 (should be platinum/999) ──
print("\n[4] UPDATE verification on DML-01 (id=2012051) — expect tier=platinum, score=999")
try:
    df = spark.sql("""
        SELECT id, name, tier, loyalty_score, preferred_lang
        FROM `postgres`.`cache_testing`.`customers`
        WHERE id = 2012051
    """)
    rows = df.collect()
    if rows:
        r = rows[0]
        tier_ok  = "✓" if r.tier == "platinum" else f"✗ (got {r.tier})"
        score_ok = "✓" if r.loyalty_score == 999 else f"✗ (got {r.loyalty_score})"
        print(f"  tier={r.tier} {tier_ok}   loyalty_score={r.loyalty_score} {score_ok}")
    else:
        print("  !! Row id=2012051 NOT found in Iceberg")
except Exception as e:
    print(f"  ERROR: {e}")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)
