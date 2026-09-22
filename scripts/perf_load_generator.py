#!/usr/bin/env python3
"""
perf_load_generator.py
======================
MongoDB soft-delete CDC performance test:
  Phase 1 — INSERT  2000 documents  (customer_id 99000001 – 99002000)
  Phase 2 — UPDATE  100 documents   (customer_id 99000001 – 99000100)
  Phase 3 — DELETE  1000 documents  (customer_id 99000001 – 99001000)

Outputs precise wall-clock timestamps for each phase start/end so
pipeline propagation latency can be measured against Iceberg snap_timestamp.
"""
import datetime, json, time
from pymongo import MongoClient, ASCENDING

MONGO_URI  = "mongodb://root:oEtCgw554IP3ua0SrJCTsWYM@mongodb.prod.svc.cluster.local:27017/cache_testing?authSource=admin"
DB_NAME    = "cache_testing"
COLLECTION = "customers"

INSERT_START_ID = 99_000_001
INSERT_COUNT    = 2_000
UPDATE_COUNT    = 100
DELETE_COUNT    = 1_000
BATCH_SIZE      = 500      # bulk write batch size

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
col    = client[DB_NAME][COLLECTION]

# ── cleanup any leftover test docs ────────────────────────────────────────────
col.delete_many({"customer_id": {"$gte": INSERT_START_ID,
                                  "$lte": INSERT_START_ID + INSERT_COUNT - 1}})
print(f"[PREP] Cleaned up any existing docs in range {INSERT_START_ID}–{INSERT_START_ID+INSERT_COUNT-1}")

results = {}

# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 1 — INSERT 2000
# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n{'='*60}")
print(f"PHASE 1 — INSERT {INSERT_COUNT} documents")
print(f"{'='*60}")

t0 = time.time()
ts_insert_start = datetime.datetime.utcnow()
print(f"[INSERT] wall_clock_start = {ts_insert_start.isoformat()}Z")

inserted_total = 0
for batch_start in range(0, INSERT_COUNT, BATCH_SIZE):
    batch_ids = range(INSERT_START_ID + batch_start,
                      INSERT_START_ID + min(batch_start + BATCH_SIZE, INSERT_COUNT))
    docs = [
        {
            "customer_id":  cid,
            "first_name":   f"PerfTest{cid}",
            "last_name":    "Load",
            "email":        f"perf{cid}@loadtest.com",
            "phone":        f"+61400{cid}",
            "city":         ["Sydney","Melbourne","Brisbane","Perth","Adelaide"][cid % 5],
            "country_code": "AU",
            "tier":         ["BRONZE","SILVER","GOLD","PLATINUM"][cid % 4],
            "credit_limit": round(1000.0 + (cid % 50000) * 0.5, 2),
            "is_active":    True,
            "created_at":   datetime.datetime.utcnow(),
            "updated_at":   datetime.datetime.utcnow(),
        }
        for cid in batch_ids
    ]
    col.insert_many(docs, ordered=False)
    inserted_total += len(docs)
    print(f"  [INSERT] batch {batch_start//BATCH_SIZE + 1}: {len(docs)} docs  (total={inserted_total})")

t_insert_end  = time.time()
ts_insert_end = datetime.datetime.utcnow()
insert_elapsed = t_insert_end - t0
print(f"[INSERT] wall_clock_end   = {ts_insert_end.isoformat()}Z")
print(f"[INSERT] mongo_elapsed    = {insert_elapsed:.3f}s  ({INSERT_COUNT/insert_elapsed:.0f} docs/s)")
results["insert"] = {
    "count": INSERT_COUNT,
    "start": ts_insert_start.isoformat() + "Z",
    "end":   ts_insert_end.isoformat()   + "Z",
    "elapsed_s": round(insert_elapsed, 3),
    "docs_per_s": round(INSERT_COUNT / insert_elapsed, 1),
}

# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2 — UPDATE 100
# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n{'='*60}")
print(f"PHASE 2 — UPDATE {UPDATE_COUNT} documents")
print(f"{'='*60}")

t0 = time.time()
ts_update_start = datetime.datetime.utcnow()
print(f"[UPDATE] wall_clock_start = {ts_update_start.isoformat()}Z")

from pymongo import UpdateOne
update_ops = [
    UpdateOne(
        {"customer_id": INSERT_START_ID + i},
        {"$set": {
            "email":        f"perf{INSERT_START_ID+i}_updated@loadtest.com",
            "tier":         "PLATINUM",
            "credit_limit": 99999.99,
            "updated_at":   datetime.datetime.utcnow(),
        }}
    )
    for i in range(UPDATE_COUNT)
]
res = col.bulk_write(update_ops, ordered=False)

t_update_end  = time.time()
ts_update_end = datetime.datetime.utcnow()
update_elapsed = t_update_end - t0
print(f"[UPDATE] modified         = {res.modified_count}")
print(f"[UPDATE] wall_clock_end   = {ts_update_end.isoformat()}Z")
print(f"[UPDATE] mongo_elapsed    = {update_elapsed:.3f}s  ({UPDATE_COUNT/update_elapsed:.0f} docs/s)")
results["update"] = {
    "count": UPDATE_COUNT,
    "modified": res.modified_count,
    "start": ts_update_start.isoformat() + "Z",
    "end":   ts_update_end.isoformat()   + "Z",
    "elapsed_s": round(update_elapsed, 3),
    "docs_per_s": round(UPDATE_COUNT / update_elapsed, 1),
}

# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 3 — DELETE 1000
# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n{'='*60}")
print(f"PHASE 3 — DELETE {DELETE_COUNT} documents")
print(f"{'='*60}")

t0 = time.time()
ts_delete_start = datetime.datetime.utcnow()
print(f"[DELETE] wall_clock_start = {ts_delete_start.isoformat()}Z")

res = col.delete_many({
    "customer_id": {
        "$gte": INSERT_START_ID,
        "$lte": INSERT_START_ID + DELETE_COUNT - 1,
    }
})

t_delete_end  = time.time()
ts_delete_end = datetime.datetime.utcnow()
delete_elapsed = t_delete_end - t0
print(f"[DELETE] deleted          = {res.deleted_count}")
print(f"[DELETE] wall_clock_end   = {ts_delete_end.isoformat()}Z")
print(f"[DELETE] mongo_elapsed    = {delete_elapsed:.3f}s  ({DELETE_COUNT/delete_elapsed:.0f} docs/s)")
results["delete"] = {
    "count": DELETE_COUNT,
    "deleted": res.deleted_count,
    "start": ts_delete_start.isoformat() + "Z",
    "end":   ts_delete_end.isoformat()   + "Z",
    "elapsed_s": round(delete_elapsed, 3),
    "docs_per_s": round(DELETE_COUNT / delete_elapsed, 1),
}

# ── summary ───────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print("LOAD GENERATOR SUMMARY")
print(f"{'='*60}")
print(json.dumps(results, indent=2))
print(f"\nTotal events fired into MongoDB change stream: {INSERT_COUNT + UPDATE_COUNT + DELETE_COUNT}")
print("Pipeline will now consume from Kafka — watch logs for batch= lines.")
print(f"First INSERT event entered change stream at: {results['insert']['start']}")
print(f"Last  DELETE event entered change stream at: {results['delete']['end']}")

client.close()
