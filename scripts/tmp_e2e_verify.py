import sys
sys.path.insert(0, '/opt/spark/work-dir')
from bao_spark_init import BaoSparkInit
from pyspark.sql import SparkSession
from datetime import datetime, timezone, timedelta

bao = BaoSparkInit()
conf = bao.spark_conf(app_name="e2e-verify")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

CUTOFF = datetime.now(timezone.utc) - timedelta(minutes=10)
TEST_IDS = [2012061, 2012062, 2012063, 2012064, 2012065]
IDS_STR  = ", ".join(str(i) for i in TEST_IDS)

print("=" * 65)
print("E2E REPLICATION VERIFICATION")
print(f"Test IDs: {TEST_IDS}")
print("=" * 65)

# 1. Latest snapshot — did a new one commit after our DMLs?
print("\n[1] Latest snapshots on postgres.cache_testing.customers")
spark.sql("""
    SELECT snapshot_id, committed_at, operation,
           summary['added-records']   AS added,
           summary['deleted-records'] AS deleted,
           summary['total-records']   AS total
    FROM `postgres`.`cache_testing`.`customers`.snapshots
    ORDER BY committed_at DESC LIMIT 5
""").show(truncate=False)

# 2. Are the INSERT rows in Iceberg?
print("[2] INSERTed rows present in Iceberg (ids 2012061–2012065)?")
df = spark.sql(f"""
    SELECT id, name, email, tier, loyalty_score, snap_timestamp
    FROM `postgres`.`cache_testing`.`customers`
    WHERE id IN ({IDS_STR})
    ORDER BY id
""")
rows = df.collect()
if rows:
    print(f"  FOUND {len(rows)} row(s):")
    for r in rows:
        print(f"    id={r.id}  name={r.name}  tier={r.tier}  score={r.loyalty_score}  snap_ts={r.snap_timestamp}")
else:
    print("  !! NONE of the inserted rows found in Iceberg")

# 3. UPDATE check — id=2012061 should be platinum/999
print("\n[3] UPDATE check — id=2012061 should be tier=platinum, score=999")
upd = spark.sql("SELECT id, tier, loyalty_score FROM `postgres`.`cache_testing`.`customers` WHERE id=2012061").first()
if upd:
    t = "✅" if upd.tier=="platinum" else f"❌(got {upd.tier})"
    s = "✅" if upd.loyalty_score==999  else f"❌(got {upd.loyalty_score})"
    print(f"  tier={upd.tier} {t}   loyalty_score={upd.loyalty_score} {s}")
else:
    print("  !! id=2012061 NOT FOUND")

# 4. DELETE check — id=2012063 (e2e_gamma) should be gone
print("\n[4] DELETE check — id=2012063 should be absent from Iceberg")
gone = spark.sql("SELECT id FROM `postgres`.`cache_testing`.`customers` WHERE id=2012063").collect()
if gone:
    print(f"  ❌ Row still present — hard-delete not processed yet")
else:
    print(f"  ✅ Row absent — DELETE replicated correctly")

print("\n" + "=" * 65)
