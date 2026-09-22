#!/usr/bin/env python3
"""
scripts/ddl_e2e_test.py
=======================
End-to-end DDL + DML lifecycle test for the CDC → Iceberg pipeline.

Full test sequence
------------------
  (a) DROP   loyalty_tier_v2 on Oracle, MongoDB, Postgres
  (b) DML    inserts/updates WITHOUT the column on all three sources
  (c) ADD    loyalty_tier_v2 back on all three sources
  (d) DML    inserts/updates WITH the new column on all three sources
       → report pass/fail for every step and every Iceberg table

Verifications
-------------
  1. No streaming pod crash / restart loop.
  2. Post-(c) DML rows carry loyalty_tier_v2 IS NOT NULL in every Iceberg table.
  3. Pre-(c) DML rows have loyalty_tier_v2 IS NULL (additive DDL, back-filled NULL).
  4. New column appears in all 7 Iceberg tables within the polling window.

Also captured in the report
---------------------------
  • last_login_at type in each Iceberg table (must stay BIGINT, not STRING).
    This validates the streaming-script fix that prevents NULL-batch re-inference
    from downgrading cached column types during DDL evolution.

Usage
-----
  python3 scripts/ddl_e2e_test.py

Environment variables
---------------------
  PG_DSN     postgresql://user:pass@host:5432/dbname  (default below)
  ORA_DSN    user/pass@host:1521/service               (default below)
  MGO_URI    mongodb://user:pass@host:27017/           (default below)
  POLL_SECS  max seconds to wait for Iceberg to show the new column (default 120)
  DML_ROWS   number of test rows to insert per phase (default 5)

Iceberg tables verified
-----------------------
  postgres.cache_testing.customers
  postgres.cache_testing.customers_sd
  oracle.cache_testing.customers
  oracle.cache_testing.customers_sd
  mongodb.cache_testing.customers
  mongodb.cache_testing.customers_sd
  mongodb.cache_testing.customers_hist
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
import textwrap
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
PG_DSN      = os.environ.get("PG_DSN",  "postgresql://rbac:vb2dJms4c1fKi0uYD87Vv4YpCsZQJm1f@postgresql.prod.svc.cluster.local:5432/cache_testing")
# Superuser DSN — needed for ALTER TABLE (rbac is not the table owner)
PG_DDL_DSN  = os.environ.get("PG_DDL_DSN", "postgresql://postgres:mE8GKcHiFTaoXCFgRk1vYcXR@postgresql.prod.svc.cluster.local:5432/cache_testing")
ORA_DSN  = os.environ.get("ORA_DSN", "CACHE_TESTING/CacheTesting2024@oracle-xe.prod.svc.cluster.local:1521/XEPDB1")
MGO_URI  = os.environ.get("MGO_URI", "mongodb://root:oEtCgw554IP3ua0SrJCTsWYM@mongodb.prod.svc.cluster.local:27017/?authSource=admin&replicaSet=rs0")
POLL_SECS = int(os.environ.get("POLL_SECS", "120"))
DML_ROWS  = int(os.environ.get("DML_ROWS",  "5"))

NEW_COL   = "loyalty_tier_v2"    # column under test
NEW_TYPE  = "SMALLINT"           # source DB DDL type
ICE_TYPE  = "int"                # expected Iceberg type (SMALLINT → int)

# Base IDs — use ranges far from existing test data to avoid conflicts
_BASE_ID_PRE  = 2_030_001   # (b) pre-DDL DML rows
_BASE_ID_POST = 2_040_001   # (d) post-DDL DML rows

# Iceberg tables to verify
_ICE_TABLES = [
    ("postgres", "cache_testing", "customers"),
    ("postgres", "cache_testing", "customers_sd"),
    ("oracle",   "cache_testing", "customers"),
    ("oracle",   "cache_testing", "customers_sd"),
    ("mongodb",  "cache_testing", "customers"),
    ("mongodb",  "cache_testing", "customers_sd"),
    ("mongodb",  "cache_testing", "customers_hist"),
]

# Streaming deployment names — used to check restart counts
_DEPLOYMENTS = [
    "kafka-to-iceberg-postgres-standard",
    "kafka-to-iceberg-postgres-soft-delete",
    "kafka-to-iceberg-oracle-standard",
    "kafka-to-iceberg-oracle-soft-delete",
    "kafka-to-iceberg-mongodb-standard",
    "kafka-to-iceberg-mongodb-soft-delete",
    "kafka-to-iceberg-mongodb-history-tracking",
]

# ── Result accumulator ────────────────────────────────────────────────────────
_results: list[tuple[str, bool, str]] = []  # (label, passed, detail)


def _record(label: str, passed: bool, detail: str = "") -> None:
    _results.append((label, passed, detail))


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def _ok(msg: str) -> None:
    print(f"  [{_ts()}] ✓  {msg}")


def _fail(msg: str) -> None:
    print(f"  [{_ts()}] ✗  {msg}", file=sys.stderr)


def _info(msg: str) -> None:
    print(f"  [{_ts()}]    {msg}")


# Pod used for all DB operations — has psycopg2, oracledb, pymongo and can reach
# the cluster-internal service names.
_DB_POD = "kafka-to-iceberg-postgres-standard-6dbb9465fb-xst8w"


def _kubectl(*args: str, check: bool = True) -> str:
    cmd = ["kubectl", "-n", "prod", *args]
    result = subprocess.run(cmd, capture_output=True, text=True, check=check)
    return result.stdout.strip()


def _pod_python(code: str, timeout: int = 30) -> str:
    """
    Run a Python snippet inside _DB_POD (which has access to cluster-internal
    DB services) and return its stdout.  Raises RuntimeError on non-zero exit.
    """
    # Escape the code for single-quoted shell argument
    escaped = code.replace("\\", "\\\\").replace("'", "'\\''")
    cmd = [
        "kubectl", "-n", "prod", "exec", _DB_POD, "--",
        "python3", "-c", code,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout.strip()


def _spark_sql(sql: str) -> str:
    pod = _kubectl(
        "get", "pods",
        "-l", "app=kafka-to-iceberg,pipeline.write-mode=standard,pipeline.source=postgres",
        "-o", "jsonpath={.items[0].metadata.name}",
    )
    if not pod:
        raise RuntimeError("No postgres-standard streaming pod found.")
    cmd = [
        "kubectl", "-n", "prod", "exec", pod, "--",
        "bash", "-c",
        f"cd /opt/spark/work-dir && "
        f"spark-sql --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
        f"--conf spark.sql.catalog.postgres=org.apache.iceberg.spark.SparkCatalog "
        f"--conf spark.sql.catalog.oracle=org.apache.iceberg.spark.SparkCatalog "
        f"--conf spark.sql.catalog.mongodb=org.apache.iceberg.spark.SparkCatalog "
        f"-e \"{sql.strip()}\""
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    return result.stdout.strip()


# ── Pod restart snapshot ──────────────────────────────────────────────────────

def snapshot_restarts() -> dict[str, int]:
    counts: dict[str, int] = {}
    pods_out = _kubectl(
        "get", "pods",
        "-l", "app=kafka-to-iceberg",
        "-o",
        r"jsonpath={range .items[*]}{.metadata.name}{':'}{.status.containerStatuses[0].restartCount}{'\n'}{end}",
        check=False,
    )
    for line in pods_out.splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        name, _, count = line.rpartition(":")
        try:
            counts[name.strip()] = int(count.strip() or "0")
        except ValueError:
            pass
    return counts


def check_no_restarts(before: dict[str, int]) -> bool:
    after = snapshot_restarts()
    crashed = []
    for pod, before_count in before.items():
        after_count = after.get(pod, before_count)
        if after_count > before_count:
            crashed.append(f"{pod}: {before_count} → {after_count}")
    if crashed:
        for c in crashed:
            _fail(f"Pod restarted: {c}")
        return False
    _ok(f"No pod restarts across {len(before)} pod(s)")
    return True


# ── (a) DROP column ───────────────────────────────────────────────────────────

def drop_col_postgres() -> None:
    _pod_python(f"""
import psycopg2
conn = psycopg2.connect({PG_DSN!r})
conn.autocommit = True
conn.cursor().execute("ALTER TABLE customers DROP COLUMN IF EXISTS {NEW_COL}")
conn.close()
print("done")
""")
    _ok(f"PostgreSQL: DROP COLUMN {NEW_COL}")


def drop_col_oracle() -> None:
    out = _pod_python(f"""
import oracledb
user,rest = {ORA_DSN!r}.split("/",1); pw,dsn = rest.split("@",1)
conn = oracledb.connect(user=user,password=pw,dsn=dsn)
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM all_tab_columns WHERE owner='CACHE_TESTING' AND table_name='CUSTOMERS' AND column_name='{NEW_COL.upper()}'")
if cur.fetchone()[0]>0:
    cur.execute("ALTER TABLE CUSTOMERS DROP COLUMN {NEW_COL}")
    conn.commit()
    print("dropped")
else:
    print("not_present")
conn.close()
""")
    if "not_present" in out:
        _info(f"Oracle: column {NEW_COL} not present — skipped DROP")
    else:
        _ok(f"Oracle: DROP COLUMN {NEW_COL}")


def drop_col_mongodb() -> None:
    out = _pod_python(f"""
from pymongo import MongoClient
client = MongoClient({MGO_URI!r})
r = client["cache_testing"].customers.update_many({{"{NEW_COL}":{{"$exists":True}}}},{{"$unset":{{"{NEW_COL}":""}}}})
print(r.modified_count)
client.close()
""")
    _ok(f"MongoDB: $unset {NEW_COL} from {out} document(s)")


# ── (b) DML without the new column ───────────────────────────────────────────

def dml_pre_postgres(base_id: int) -> list[int]:
    ids = list(range(base_id, base_id + DML_ROWS))
    rows = [(cid, f"PreDDL{i}", "NoCols", f"predml{cid}@example.com",
             "TestCity", "US", "silver", 3000.0, True)
            for i, cid in enumerate(ids)]
    _pod_python(f"""
import psycopg2
conn = psycopg2.connect({PG_DSN!r})
conn.autocommit = True
cur = conn.cursor()
for row in {rows!r}:
    cur.execute("INSERT INTO customers(id,first_name,last_name,email,city,country_code,tier,credit_limit,is_active) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE SET tier=EXCLUDED.tier", row)
conn.close()
print("ok")
""")
    _ok(f"PostgreSQL: inserted {DML_ROWS} pre-DDL rows (ids {ids[0]}–{ids[-1]})")
    return ids


def dml_pre_oracle(base_id: int) -> list[int]:
    ids = list(range(base_id, base_id + DML_ROWS))
    stmts = "".join(f"""
    cur.execute("MERGE INTO CUSTOMERS t USING (SELECT {cid} AS CUSTOMER_ID FROM dual) s ON (t.CUSTOMER_ID=s.CUSTOMER_ID) WHEN MATCHED THEN UPDATE SET t.TIER='silver' WHEN NOT MATCHED THEN INSERT(CUSTOMER_ID,FIRST_NAME,LAST_NAME,EMAIL,CITY,COUNTRY_CODE,TIER,CREDIT_LIMIT,IS_ACTIVE) VALUES({cid},'PreDDL{i}','NoCols','predml{cid}@example.com','TestCity','US','silver',3000,1)")"""
             for i, cid in enumerate(ids))
    _pod_python(f"""
import oracledb
user,rest = {ORA_DSN!r}.split("/",1); pw,dsn = rest.split("@",1)
conn = oracledb.connect(user=user,password=pw,dsn=dsn)
cur = conn.cursor()
{stmts}
conn.commit(); conn.close(); print("ok")
""")
    _ok(f"Oracle: inserted {DML_ROWS} pre-DDL rows (ids {ids[0]}–{ids[-1]})")
    return ids


def dml_pre_mongodb(base_id: int) -> list[int]:
    ids = list(range(base_id, base_id + DML_ROWS))
    docs_repr = [{"customer_id": cid, "first_name": f"PreDDL{i}", "last_name": "NoCols",
                  "email": f"predml{cid}@example.com", "city": "TestCity",
                  "country_code": "US", "tier": "silver", "credit_limit": 3000.0,
                  "is_active": True}
                 for i, cid in enumerate(ids)]
    _pod_python(f"""
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
client = MongoClient({MGO_URI!r})
try:
    client["cache_testing"].customers.insert_many({docs_repr!r}, ordered=False)
except BulkWriteError:
    pass
client.close(); print("ok")
""")
    _ok(f"MongoDB: inserted {DML_ROWS} pre-DDL docs (customer_ids {ids[0]}–{ids[-1]})")
    return ids


# ── (c) ADD column back ───────────────────────────────────────────────────────

def add_col_postgres() -> None:
    _pod_python(f"""
import psycopg2
conn = psycopg2.connect({PG_DSN!r})
conn.autocommit = True
conn.cursor().execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS {NEW_COL} {NEW_TYPE}")
conn.close(); print("done")
""")
    _ok(f"PostgreSQL: ADD COLUMN {NEW_COL} {NEW_TYPE}")


def add_col_oracle() -> None:
    out = _pod_python(f"""
import oracledb
user,rest = {ORA_DSN!r}.split("/",1); pw,dsn = rest.split("@",1)
conn = oracledb.connect(user=user,password=pw,dsn=dsn)
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM all_tab_columns WHERE owner='CACHE_TESTING' AND table_name='CUSTOMERS' AND column_name='{NEW_COL.upper()}'")
if cur.fetchone()[0]==0:
    cur.execute("ALTER TABLE CUSTOMERS ADD ({NEW_COL} {NEW_TYPE})")
    conn.commit(); print("added")
else:
    print("exists")
conn.close()
""")
    if "exists" in out:
        _info(f"Oracle: column {NEW_COL} already exists — skipped ADD")
    else:
        _ok(f"Oracle: ADD COLUMN {NEW_COL} {NEW_TYPE}")


def add_col_mongodb() -> None:
    out = _pod_python(f"""
from pymongo import MongoClient
client = MongoClient({MGO_URI!r})
r = client["cache_testing"].customers.update_one({{"{NEW_COL}":{{"$exists":False}}}},{{"$set":{{"{NEW_COL}":None}}}})
print("seeded" if r.modified_count else "exists")
client.close()
""")
    if "seeded" in out:
        _ok(f"MongoDB: seeded {NEW_COL}=null on one document (schema change event)")
    else:
        _info(f"MongoDB: {NEW_COL} already present on all documents — skipped seed")


# ── (d) DML with new column ───────────────────────────────────────────────────

def dml_post_postgres(base_id: int) -> list[int]:
    ids = list(range(base_id, base_id + DML_ROWS))
    rows = [(cid, f"PostDDL{i}", "LoyaltyV2", f"postdml{cid}@example.com",
             "TestCity", "US", "gold", 5000.0, True, i + 1)
            for i, cid in enumerate(ids)]
    _pod_python(f"""
import psycopg2
conn = psycopg2.connect({PG_DSN!r})
conn.autocommit = True
cur = conn.cursor()
for row in {rows!r}:
    cur.execute("INSERT INTO customers(id,first_name,last_name,email,city,country_code,tier,credit_limit,is_active,{NEW_COL}) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE SET {NEW_COL}=EXCLUDED.{NEW_COL}", row)
conn.close(); print("ok")
""")
    _ok(f"PostgreSQL: inserted {DML_ROWS} post-DDL rows with {NEW_COL} set (ids {ids[0]}–{ids[-1]})")
    return ids


def dml_post_oracle(base_id: int) -> list[int]:
    ids = list(range(base_id, base_id + DML_ROWS))
    stmts = "".join(f"""
    cur.execute("MERGE INTO CUSTOMERS t USING (SELECT {cid} AS CUSTOMER_ID FROM dual) s ON (t.CUSTOMER_ID=s.CUSTOMER_ID) WHEN MATCHED THEN UPDATE SET t.{NEW_COL.upper()}={i+1} WHEN NOT MATCHED THEN INSERT(CUSTOMER_ID,FIRST_NAME,LAST_NAME,EMAIL,CITY,COUNTRY_CODE,TIER,CREDIT_LIMIT,IS_ACTIVE,{NEW_COL.upper()}) VALUES({cid},'PostDDL{i}','LoyaltyV2','postdml{cid}@example.com','TestCity','US','gold',5000,1,{i+1})")"""
             for i, cid in enumerate(ids))
    _pod_python(f"""
import oracledb
user,rest = {ORA_DSN!r}.split("/",1); pw,dsn = rest.split("@",1)
conn = oracledb.connect(user=user,password=pw,dsn=dsn)
cur = conn.cursor()
{stmts}
conn.commit(); conn.close(); print("ok")
""")
    _ok(f"Oracle: inserted {DML_ROWS} post-DDL rows with {NEW_COL} set (ids {ids[0]}–{ids[-1]})")
    return ids


def dml_post_mongodb(base_id: int) -> list[int]:
    ids = list(range(base_id, base_id + DML_ROWS))
    docs_repr = [{"customer_id": cid, "first_name": f"PostDDL{i}", "last_name": "LoyaltyV2",
                  "email": f"postdml{cid}@example.com", "city": "TestCity",
                  "country_code": "US", "tier": "gold", "credit_limit": 5000.0,
                  "is_active": True, NEW_COL: i + 1}
                 for i, cid in enumerate(ids)]
    _pod_python(f"""
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
client = MongoClient({MGO_URI!r})
try:
    client["cache_testing"].customers.insert_many({docs_repr!r}, ordered=False)
except BulkWriteError:
    pass
client.close(); print("ok")
""")
    _ok(f"MongoDB: inserted {DML_ROWS} post-DDL docs with {NEW_COL} set (customer_ids {ids[0]}–{ids[-1]})")
    return ids



# ── Iceberg polling ───────────────────────────────────────────────────────────

def poll_iceberg_columns(deadline: float) -> dict[str, bool]:
    """Poll until NEW_COL appears in all Iceberg tables or deadline passes."""
    remaining = {f"{cat}.{ns}.{tbl}" for cat, ns, tbl in _ICE_TABLES}
    confirmed: dict[str, bool] = {}
    while remaining and time.time() < deadline:
        still_missing: set[str] = set()
        for fqn in list(remaining):
            cat, ns, tbl = fqn.split(".")
            out = _spark_sql(f"DESCRIBE TABLE `{cat}`.`{ns}`.`{tbl}`")
            if NEW_COL.lower() in out.lower():
                confirmed[fqn] = True
                _ok(f"Iceberg {fqn}: column '{NEW_COL}' visible")
            else:
                still_missing.add(fqn)
        remaining = still_missing
        if remaining:
            _info(f"Waiting… {len(remaining)} table(s) still missing '{NEW_COL}': {sorted(remaining)}")
            time.sleep(10)
    for fqn in remaining:
        confirmed[fqn] = False
        _fail(f"Iceberg {fqn}: column '{NEW_COL}' NOT found within {POLL_SECS}s")
    return confirmed


def poll_iceberg_col_gone(deadline: float) -> dict[str, bool]:
    """
    Poll until NEW_COL is GONE from all Iceberg tables (or deadline passes).
    Returns {fqn: True} when the column is absent (as expected after DROP).
    Note: Iceberg does not support DROP COLUMN via ALTER; the column stays in
    Iceberg with NULLs after the source drop.  We check the source-side drop
    replicated cleanly by verifying no new non-NULL values land for that col.
    This function simply records the current column presence state as-is.
    """
    results: dict[str, bool] = {}
    for cat, ns, tbl in _ICE_TABLES:
        fqn = f"{cat}.{ns}.{tbl}"
        out = _spark_sql(f"DESCRIBE TABLE `{cat}`.`{ns}`.`{tbl}`")
        present = NEW_COL.lower() in out.lower()
        results[fqn] = present  # True = column is present (may be NULL)
        state = "present (NULLs expected)" if present else "absent"
        _info(f"Iceberg {fqn}: {NEW_COL} → {state}")
    return results


# ── Row verification ──────────────────────────────────────────────────────────

def _iceberg_count(fqn: str, pk_col: str, ids: list[int], condition: str) -> int:
    id_list = ", ".join(str(i) for i in ids)
    sql = (
        f"SELECT COUNT(*) FROM `{fqn.replace('.', '`.`')}` "
        f"WHERE `{pk_col}` IN ({id_list}) AND {condition}"
    )
    out = _spark_sql(sql)
    try:
        return int(out.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return 0


def verify_pre_dml_rows(pg_ids: list[int], ora_ids: list[int], mgo_ids: list[int]) -> bool:
    """
    Pre-DDL rows must have loyalty_tier_v2 IS NULL in Iceberg
    (they were inserted before the column existed, so Iceberg back-fills NULL).
    """
    ok = True
    checks = [
        ("postgres.cache_testing.customers",    "id",          pg_ids),
        ("postgres.cache_testing.customers_sd", "id",          pg_ids),
        ("oracle.cache_testing.customers",      "CUSTOMER_ID", ora_ids),
        ("oracle.cache_testing.customers_sd",   "CUSTOMER_ID", ora_ids),
        ("mongodb.cache_testing.customers",     "customer_id", mgo_ids),
        ("mongodb.cache_testing.customers_sd",  "customer_id", mgo_ids),
        ("mongodb.cache_testing.customers_hist","customer_id", mgo_ids),
    ]
    for fqn, pk, ids in checks:
        count = _iceberg_count(fqn, pk, ids, f"`{NEW_COL}` IS NULL")
        if count == len(ids):
            _ok(f"{fqn}: {count}/{len(ids)} pre-DDL rows have {NEW_COL} IS NULL ✓")
        else:
            _fail(f"{fqn}: only {count}/{len(ids)} pre-DDL rows have {NEW_COL} IS NULL")
            ok = False
    return ok


def verify_post_dml_rows(pg_ids: list[int], ora_ids: list[int], mgo_ids: list[int]) -> bool:
    """Post-DDL rows must have loyalty_tier_v2 IS NOT NULL."""
    ok = True
    checks = [
        ("postgres.cache_testing.customers",    "id",          pg_ids),
        ("postgres.cache_testing.customers_sd", "id",          pg_ids),
        ("oracle.cache_testing.customers",      "CUSTOMER_ID", ora_ids),
        ("oracle.cache_testing.customers_sd",   "CUSTOMER_ID", ora_ids),
        ("mongodb.cache_testing.customers",     "customer_id", mgo_ids),
        ("mongodb.cache_testing.customers_sd",  "customer_id", mgo_ids),
        ("mongodb.cache_testing.customers_hist","customer_id", mgo_ids),
    ]
    for fqn, pk, ids in checks:
        count = _iceberg_count(fqn, pk, ids, f"`{NEW_COL}` IS NOT NULL")
        if count == len(ids):
            _ok(f"{fqn}: {count}/{len(ids)} post-DDL rows have {NEW_COL} IS NOT NULL ✓")
        else:
            _fail(f"{fqn}: only {count}/{len(ids)} post-DDL rows have {NEW_COL} IS NOT NULL")
            ok = False
    return ok


def check_last_login_at_type() -> dict[str, str]:
    """
    For each Iceberg customers table, fetch the data_type of last_login_at from
    DESCRIBE TABLE and confirm it is bigint (not string).
    Returns {fqn: actual_type}.
    """
    types: dict[str, str] = {}
    for cat, ns, tbl in _ICE_TABLES:
        fqn = f"{cat}.{ns}.{tbl}"
        out = _spark_sql(f"DESCRIBE TABLE `{cat}`.`{ns}`.`{tbl}`")
        col_type = "not_found"
        for line in out.splitlines():
            parts = line.split()
            if parts and parts[0].lower() == "last_login_at":
                col_type = parts[1].lower() if len(parts) > 1 else "unknown"
                break
        types[fqn] = col_type
    return types


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print()
    print("=" * 72)
    print(f"  CDC DDL/DML E2E Test  —  column: {NEW_COL} {NEW_TYPE}")
    print(f"  Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 72)

    # 0. Snapshot pod restart counts
    print("\n[0] Snapshotting pod restart counts …")
    restarts_before = snapshot_restarts()
    _info(f"Tracking {len(restarts_before)} pod(s)")

    # ── (a) DROP column ───────────────────────────────────────────────────────
    print(f"\n[a] Dropping {NEW_COL} from Oracle, MongoDB, Postgres …")
    drop_ok = {"pg": False, "ora": False, "mgo": False}
    for key, fn in [("pg", drop_col_postgres), ("ora", drop_col_oracle), ("mgo", drop_col_mongodb)]:
        try:
            fn()
            drop_ok[key] = True
        except Exception as exc:
            _fail(f"DROP failed ({key}): {exc}")

    _record("(a) DROP column — Postgres", drop_ok["pg"])
    _record("(a) DROP column — Oracle",   drop_ok["ora"])
    _record("(a) DROP column — MongoDB",  drop_ok["mgo"])

    if not all(drop_ok.values()):
        _fail("DROP step had failures — continuing anyway to capture partial state.")

    # Brief pause to let Debezium capture the DDL event
    _info("Waiting 5 s for Debezium to capture DROP event …")
    time.sleep(5)

    # ── (b) DML without the column ────────────────────────────────────────────
    print(f"\n[b] DML (inserts/updates) WITHOUT {NEW_COL} …")
    pre_pg_ids  = dml_pre_postgres(_BASE_ID_PRE)            if drop_ok["pg"]  else []
    pre_ora_ids = dml_pre_oracle(  _BASE_ID_PRE + 100)      if drop_ok["ora"] else []
    pre_mgo_ids = dml_pre_mongodb( _BASE_ID_PRE + 200)      if drop_ok["mgo"] else []

    _record("(b) Pre-DDL DML — Postgres", bool(pre_pg_ids))
    _record("(b) Pre-DDL DML — Oracle",   bool(pre_ora_ids))
    _record("(b) Pre-DDL DML — MongoDB",  bool(pre_mgo_ids))

    # ── (c) ADD column back ───────────────────────────────────────────────────
    print(f"\n[c] Adding {NEW_COL} {NEW_TYPE} back on Oracle, MongoDB, Postgres …")
    add_ok = {"pg": False, "ora": False, "mgo": False}
    for key, fn in [("pg", add_col_postgres), ("ora", add_col_oracle), ("mgo", add_col_mongodb)]:
        try:
            fn()
            add_ok[key] = True
        except Exception as exc:
            _fail(f"ADD COLUMN failed ({key}): {exc}")

    _record("(c) ADD column — Postgres", add_ok["pg"])
    _record("(c) ADD column — Oracle",   add_ok["ora"])
    _record("(c) ADD column — MongoDB",  add_ok["mgo"])

    if not all(add_ok.values()):
        _fail("ADD COLUMN step had failures — aborting test.")
        sys.exit(1)

    # Poll Iceberg until new column appears
    print(f"\n    Polling Iceberg for '{NEW_COL}' (timeout {POLL_SECS}s) …")
    deadline = time.time() + POLL_SECS
    col_results = poll_iceberg_columns(deadline)
    all_cols_present = all(col_results.values())
    for fqn, present in col_results.items():
        _record(f"(c) Iceberg column visible — {fqn}", present)

    # ── (d) DML with new column ───────────────────────────────────────────────
    print(f"\n[d] DML (inserts/updates) WITH {NEW_COL} set …")
    post_pg_ids  = dml_post_postgres(_BASE_ID_POST)           if add_ok["pg"]  else []
    post_ora_ids = dml_post_oracle(  _BASE_ID_POST + 100)     if add_ok["ora"] else []
    post_mgo_ids = dml_post_mongodb( _BASE_ID_POST + 200)     if add_ok["mgo"] else []

    _record("(d) Post-DDL DML — Postgres", bool(post_pg_ids))
    _record("(d) Post-DDL DML — Oracle",   bool(post_ora_ids))
    _record("(d) Post-DDL DML — MongoDB",  bool(post_mgo_ids))

    # Wait for Kafka → Iceberg pipeline to flush post-DDL rows
    _info("Waiting 30 s for pipeline to flush post-DDL rows into Iceberg …")
    time.sleep(30)

    # Verify pre-DDL rows landed with NULL for the new column
    print(f"\n    Verifying pre-DDL rows have {NEW_COL} IS NULL in Iceberg …")
    if pre_pg_ids or pre_ora_ids or pre_mgo_ids:
        pre_ok = verify_pre_dml_rows(pre_pg_ids, pre_ora_ids, pre_mgo_ids)
    else:
        pre_ok = False
        _fail("No pre-DDL IDs to verify (DML step (b) failed entirely).")
    _record("(b→d) Pre-DDL rows have loyalty_tier_v2 IS NULL", pre_ok)

    # Verify post-DDL rows landed with non-NULL new column
    print(f"\n    Verifying post-DDL rows have {NEW_COL} IS NOT NULL in Iceberg …")
    if post_pg_ids or post_ora_ids or post_mgo_ids:
        post_ok = verify_post_dml_rows(post_pg_ids, post_ora_ids, post_mgo_ids)
    else:
        post_ok = False
        _fail("No post-DDL IDs to verify (DML step (d) failed entirely).")
    _record("(d) Post-DDL rows have loyalty_tier_v2 IS NOT NULL", post_ok)

    # Check last_login_at type (must stay bigint after DDL evolution)
    print(f"\n    Checking last_login_at type in Iceberg tables …")
    ll_types = check_last_login_at_type()
    for fqn, col_type in ll_types.items():
        ok = col_type in ("bigint", "long", "int8")
        status = "✓ bigint" if ok else f"✗ {col_type} (expected bigint)"
        (print if ok else _fail)(
            f"  [{_ts()}] {'✓' if ok else '✗'}  {fqn}: last_login_at → {col_type}"
        )
        _record(f"last_login_at type is bigint — {fqn}", ok, col_type)

    # Check no pod restarts
    print(f"\n    Checking for pod restarts …")
    no_restart = check_no_restarts(restarts_before)
    _record("Zero pod restarts throughout test", no_restart)

    # ── Final report ──────────────────────────────────────────────────────────
    print()
    print("=" * 72)
    print("  FINAL REPORT")
    print("=" * 72)

    col_w  = 55
    passed = 0
    failed = 0
    for label, ok, detail in _results:
        status = "PASS" if ok else "FAIL"
        suffix = f"  [{detail}]" if detail else ""
        print(f"  {'✓' if ok else '✗'}  {label:<{col_w}}  {status}{suffix}")
        if ok:
            passed += 1
        else:
            failed += 1

    print()
    print(f"  Passed: {passed}   Failed: {failed}   Total: {passed + failed}")
    print()

    if failed == 0:
        print("  ✓  ALL CHECKS PASSED")
        print("     DDL drop/add cycle and DML replication are crash-free.")
        print("     Schema-cache type-preservation fix is working correctly")
        print("     (last_login_at BIGINT preserved across DDL evolution batches).")
    else:
        print("  ✗  SOME CHECKS FAILED — review output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
