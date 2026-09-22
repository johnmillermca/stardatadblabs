#!/usr/bin/env python3
"""
perf_hist_load_generator.py
============================
MongoDB → Kafka → Iceberg **history_tracking** performance test.

  Phase 1 — INSERT  10 000 documents  (customer_id 98000001 – 98010000)
  Phase 2 — UPDATE   1 000 documents  (customer_id 98000001 – 98001000)
  Phase 3 — DELETE   5 000 documents  (customer_id 98000001 – 98005000)

Total CDC events into the change stream: 16 000

Outputs wall-clock timestamps for every phase so the verifier can compute
exact end-to-end pipeline latency (MongoDB write → Iceberg snap_timestamp).
Timestamps are also written to /tmp/perf_hist_timestamps.json.
"""

import datetime
import json
import time

from pymongo import MongoClient, UpdateOne

MONGO_URI  = "mongodb://root:oEtCgw554IP3ua0SrJCTsWYM@mongodb.prod.svc.cluster.local:27017/cache_testing?authSource=admin"
DB_NAME    = "cache_testing"
COLLECTION = "customers"

INSERT_START_ID = 98_000_001
INSERT_COUNT    = 10_000
UPDATE_COUNT    =  1_000
DELETE_COUNT    =  5_000
BATCH_SIZE      =    500   # MongoDB bulk-write batch size

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
col    = client[DB_NAME][COLLECTION]

# ── cleanup any leftover docs from a previous run ─────────────────────────────
cleaned = col.delete_many({
    "customer_id": {
        "$gte": INSERT_START_ID,
        "$lte": INSERT_START_ID + INSERT_COUNT - 1,
    }
}).deleted_count
print(f"[PREP] Cleaned up {cleaned} leftover docs in range "
      f"{INSERT_START_ID}–{INSERT_START_ID+INSERT_COUNT-1}")

results: dict = {}

# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 1 — INSERT 10 000
# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n{'='*65}")
print(f"PHASE 1 — INSERT {INSERT_COUNT:,} documents")
print(f"{'='*65}")

t0               = time.time()
ts_insert_start  = datetime.datetime.utcnow()
print(f"[INSERT] wall_clock_start = {ts_insert_start.isoformat()}Z")

inserted_total = 0
for batch_start in range(0, INSERT_COUNT, BATCH_SIZE):
    batch_ids = range(
        INSERT_START_ID + batch_start,
        INSERT_START_ID + min(batch_start + BATCH_SIZE, INSERT_COUNT),
    )
    docs = [
        {
            "customer_id":  cid,
            "first_name":   f"HistPerf{cid}",
            "last_name":    "Load",
            "email":        f"histperf{cid}@loadtest.com",
            "phone":        f"+61400{cid}",
            "city":         ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"][cid % 5],
            "country_code": "AU",
            "tier":         ["BRONZE", "SILVER", "GOLD", "PLATINUM"][cid % 4],
            "credit_limit": round(500.0 + (cid % 10_000) * 0.75, 2),
            "is_active":    True,
            "created_at":   datetime.datetime.utcnow(),
            "updated_at":   datetime.datetime.utcnow(),
        }
        for cid in batch_ids
    ]
    col.insert_many(docs, ordered=False)
    inserted_total += len(docs)
    print(f"  batch {batch_start // BATCH_SIZE + 1:>3}: {len(docs):>4} docs  (total={inserted_total:,})")

ts_insert_end   = datetime.datetime.utcnow()
insert_elapsed  = time.time() - t0
print(f"[INSERT] wall_clock_end   = {ts_insert_end.isoformat()}Z")
print(f"[INSERT] mongo_elapsed    = {insert_elapsed:.3f}s  "
      f"({INSERT_COUNT/insert_elapsed:,.0f} docs/s)")
results["insert"] = {
    "count":       INSERT_COUNT,
    "start":       ts_insert_start.isoformat() + "Z",
    "end":         ts_insert_end.isoformat()   + "Z",
    "elapsed_s":   round(insert_elapsed, 3),
    "docs_per_s":  round(INSERT_COUNT / insert_elapsed, 1),
}

# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2 — UPDATE 1 000
# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n{'='*65}")
print(f"PHASE 2 — UPDATE {UPDATE_COUNT:,} documents")
print(f"{'='*65}")

t0              = time.time()
ts_update_start = datetime.datetime.utcnow()
print(f"[UPDATE] wall_clock_start = {ts_update_start.isoformat()}Z")

update_ops = [
    UpdateOne(
        {"customer_id": INSERT_START_ID + i},
        {"$set": {
            "tier":         "PLATINUM",
            "credit_limit": 99_999.99,
            "city":         "Melbourne",
            "email":        f"histperf{INSERT_START_ID+i}_upd@loadtest.com",
            "updated_at":   datetime.datetime.utcnow(),
        }},
    )
    for i in range(UPDATE_COUNT)
]

# Bulk write in sub-batches to avoid 16 MB document limit
for b_start in range(0, len(update_ops), BATCH_SIZE):
    col.bulk_write(update_ops[b_start: b_start + BATCH_SIZE], ordered=False)

ts_update_end  = datetime.datetime.utcnow()
update_elapsed = time.time() - t0
print(f"[UPDATE] wall_clock_end   = {ts_update_end.isoformat()}Z")
print(f"[UPDATE] mongo_elapsed    = {update_elapsed:.3f}s  "
      f"({UPDATE_COUNT/update_elapsed:,.0f} docs/s)")
results["update"] = {
    "count":       UPDATE_COUNT,
    "start":       ts_update_start.isoformat() + "Z",
    "end":         ts_update_end.isoformat()   + "Z",
    "elapsed_s":   round(update_elapsed, 3),
    "docs_per_s":  round(UPDATE_COUNT / update_elapsed, 1),
}

# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 3 — DELETE 5 000
# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n{'='*65}")
print(f"PHASE 3 — DELETE {DELETE_COUNT:,} documents")
print(f"{'='*65}")

t0              = time.time()
ts_delete_start = datetime.datetime.utcnow()
print(f"[DELETE] wall_clock_start = {ts_delete_start.isoformat()}Z")

deleted = col.delete_many({
    "customer_id": {
        "$gte": INSERT_START_ID,
        "$lte": INSERT_START_ID + DELETE_COUNT - 1,
    }
}).deleted_count

ts_delete_end  = datetime.datetime.utcnow()
delete_elapsed = time.time() - t0
print(f"[DELETE] deleted          = {deleted:,}")
print(f"[DELETE] wall_clock_end   = {ts_delete_end.isoformat()}Z")
print(f"[DELETE] mongo_elapsed    = {delete_elapsed:.3f}s  "
      f"({DELETE_COUNT/delete_elapsed:,.0f} docs/s)")
results["delete"] = {
    "count":       DELETE_COUNT,
    "deleted":     deleted,
    "start":       ts_delete_start.isoformat() + "Z",
    "end":         ts_delete_end.isoformat()   + "Z",
    "elapsed_s":   round(delete_elapsed, 3),
    "docs_per_s":  round(DELETE_COUNT / delete_elapsed, 1),
}

# ── Summary ───────────────────────────────────────────────────────────────────
total_events   = INSERT_COUNT + UPDATE_COUNT + DELETE_COUNT
total_elapsed  = (ts_delete_end - ts_insert_start).total_seconds()

print(f"\n{'='*65}")
print("LOAD GENERATOR SUMMARY")
print(f"{'='*65}")
print(json.dumps(results, indent=2))
print(f"\nTotal CDC events fired : {total_events:,}  "
      f"({INSERT_COUNT:,} INS + {UPDATE_COUNT:,} UPD + {DELETE_COUNT:,} DEL)")
print(f"Total wall-clock time  : {total_elapsed:.3f}s")
print(f"Avg event rate         : {total_events/total_elapsed:,.0f} events/s")
print(f"\nFirst INSERT entered change stream : {results['insert']['start']}")
print(f"Last  DELETE entered change stream : {results['delete']['end']}")
print("\nPipeline now consuming from Kafka — watch history_tracking pod logs.")
print("Run perf_hist_verify.py after ~120s to measure Iceberg landing latency.")

# Write timestamps for the verifier
with open("/tmp/perf_hist_timestamps.json", "w") as fh:
    json.dump(results, fh, indent=2)
print("\nTimestamps saved to /tmp/perf_hist_timestamps.json")

client.close()
