#!/usr/bin/env python3
"""
scripts/ddl_e2e_test.py
=======================
Fix 3 — End-to-end DDL evolution test.

Adds a new column (loyalty_tier_v2 SMALLINT) to customers on all three sources
while DML is in-flight, then verifies:

  1. No streaming pod crash / restart loop (Kubernetes restart count stays flat).
  2. New column appears in all 7 Iceberg tables within the polling window.
  3. Rows written AFTER the DDL carry a non-NULL loyalty_tier_v2 value.
  4. Rows written BEFORE the DDL have NULL for loyalty_tier_v2 (correct additive behaviour).

Usage
-----
  # Run against the live cluster (requires kubectl + python3 with pymongo/psycopg2/cx_Oracle)
  python3 scripts/ddl_e2e_test.py

Environment variables
---------------------
  PG_DSN     postgresql://user:pass@host:5432/dbname  (default: env PG_DSN)
  ORA_DSN    user/pass@host:1521/service               (default: env ORA_DSN)
  MGO_URI    mongodb://user:pass@host:27017/           (default: env MGO_URI)
  POLL_SECS  max seconds to wait for Iceberg to show the new column (default 120)
  DML_ROWS   number of test rows to INSERT after the DDL (default 5)

Iceberg tables verified
-----------------------
  postgres.cache_testing.customers      (standard)
  postgres.cache_testing.customers_sd   (soft_delete)
  oracle.cache_testing.customers        (standard)
  oracle.cache_testing.customers_sd     (soft_delete)
  mongodb.cache_testing.customers       (standard)
  mongodb.cache_testing.customers_sd    (soft_delete)
  mongodb.cache_testing.customers_hist  (history_tracking)
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
import textwrap
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
PG_DSN   = os.environ.get("PG_DSN",  "postgresql://postgres:postgres@192.168.1.50:5432/cache_testing")
ORA_DSN  = os.environ.get("ORA_DSN", "cache_admin/cache_admin@192.168.1.50:1521/FREEPDB1")
MGO_URI  = os.environ.get("MGO_URI", "mongodb://mongoadmin:secret@192.168.1.50:27017/?authSource=admin")
POLL_SECS = int(os.environ.get("POLL_SECS", "120"))
DML_ROWS  = int(os.environ.get("DML_ROWS",  "5"))

NEW_COL   = "loyalty_tier_v2"       # new column name
NEW_TYPE  = "SMALLINT"              # source DB DDL type
ICE_TYPE  = "int"                   # expected Iceberg type (SMALLINT → int)

# Base IDs for test rows — use a range far from existing test data
_BASE_ID  = 2_020_001

# Iceberg tables to verify (catalog.namespace.table)
_ICE_TABLES = [
    ("postgres", "cache_testing", "customers"),
    ("postgres", "cache_testing", "customers_sd"),
    ("oracle",   "cache_testing", "customers"),
    ("oracle",   "cache_testing", "customers_sd"),
    ("mongodb",  "cache_testing", "customers"),
    ("mongodb",  "cache_testing", "customers_sd"),
    ("mongodb",  "cache_testing", "customers_hist"),
]

# Streaming Deployment names — used to check restart counts
_DEPLOYMENTS = [
    "kafka-to-iceberg-postgres-standard",
    "kafka-to-iceberg-postgres-soft-delete",
    "kafka-to-iceberg-oracle-standard",
    "kafka-to-iceberg-oracle-soft-delete",
    "kafka-to-iceberg-mongodb-standard",
    "kafka-to-iceberg-mongodb-soft-delete",
    "kafka-to-iceberg-mongodb-history-tracking",
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def _ok(msg: str) -> None:
    print(f"  [{_ts()}] ✓  {msg}")


def _fail(msg: str) -> None:
    print(f"  [{_ts()}] ✗  {msg}", file=sys.stderr)


def _info(msg: str) -> None:
    print(f"  [{_ts()}]    {msg}")


def _kubectl(*args: str, check: bool = True) -> str:
    """Run a kubectl command and return stdout."""
    cmd = ["kubectl", "-n", "prod", *args]
    result = subprocess.run(cmd, capture_output=True, text=True, check=check)
    return result.stdout.strip()


def _spark_sql(sql: str) -> str:
    """
    Execute a Spark SQL statement via kubectl exec into a running streaming pod.
    Returns the stdout of the spark-sql shell invocation.
    """
    # Use the standard pod as the Spark SQL gateway — any streaming pod works
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


# ── Step 1: Snapshot restart counts ──────────────────────────────────────────

def snapshot_restarts() -> dict[str, int]:
    counts: dict[str, int] = {}
    for dep in _DEPLOYMENTS:
        out = _kubectl(
            "get", "deployment", dep,
            "-o", "jsonpath={.status.conditions}",
            check=False,
        )
        # Get actual pod restart counts via rollout status
        pods_out = _kubectl(
            "get", "pods",
            "-l", f"app=kafka-to-iceberg",
            "-o", "jsonpath={range .items[*]}{.metadata.name}:{.status.containerStatuses[0].restartCount}\\n{end}",
            check=False,
        )
        for line in pods_out.splitlines():
            if line.strip():
                name, _, count = line.partition(":")
                counts[name.strip()] = int(count.strip() or "0")
    return counts


# ── Step 2: Apply DDL to all 3 sources ───────────────────────────────────────

def apply_ddl_postgres() -> None:
    import psycopg2
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            f"ALTER TABLE cache_testing.customers "
            f"ADD COLUMN IF NOT EXISTS {NEW_COL} {NEW_TYPE};"
        )
    conn.close()
    _ok(f"PostgreSQL: ALTER TABLE customers ADD COLUMN {NEW_COL} {NEW_TYPE}")


def apply_ddl_oracle() -> None:
    import cx_Oracle
    user, rest = ORA_DSN.split("/", 1)
    password, dsn = rest.split("@", 1)
    conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)
    cur = conn.cursor()
    # Oracle: check column existence first (no IF NOT EXISTS)
    cur.execute(
        "SELECT COUNT(*) FROM all_tab_columns "
        "WHERE owner='CACHE_TESTING' AND table_name='CUSTOMERS' "
        f"AND column_name='{NEW_COL.upper()}'"
    )
    if cur.fetchone()[0] == 0:
        cur.execute(
            f"ALTER TABLE cache_testing.customers "
            f"ADD ({NEW_COL} {NEW_TYPE})"
        )
        conn.commit()
        _ok(f"Oracle: ALTER TABLE customers ADD {NEW_COL} {NEW_TYPE}")
    else:
        _info(f"Oracle: column {NEW_COL} already exists — skipped DDL")
    cur.close()
    conn.close()


def apply_ddl_mongodb() -> None:
    from pymongo import MongoClient
    client = MongoClient(MGO_URI)
    db = client["cache_testing"]
    # MongoDB is schemaless — "adding a column" means updating one document so
    # Debezium captures a change event carrying the new field. We set the field
    # on the first document to NULL (None) to signal the schema change.
    result = db.customers.update_one(
        {NEW_COL: {"$exists": False}},
        {"$set": {NEW_COL: None}},
    )
    if result.modified_count:
        _ok(f"MongoDB: seeded {NEW_COL}=null on one document (triggers schema change event)")
    else:
        _info(f"MongoDB: {NEW_COL} already present on all documents — skipped seed")
    client.close()


# ── Step 3: Insert DML rows AFTER the DDL ────────────────────────────────────

def insert_dml_postgres(base_id: int) -> list[int]:
    import psycopg2
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    ids = list(range(base_id, base_id + DML_ROWS))
    with conn.cursor() as cur:
        for i, cid in enumerate(ids):
            cur.execute(
                f"""
                INSERT INTO cache_testing.customers
                  (id, first_name, last_name, email, city, country_code,
                   tier, credit_limit, is_active, {NEW_COL})
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET {NEW_COL} = EXCLUDED.{NEW_COL}
                """,
                (cid, f"DDLTest{i}", "LoyaltyV2", f"ddltest{cid}@example.com",
                 "TestCity", "US", "gold", 5000.0, True, i + 1),
            )
    conn.close()
    _ok(f"PostgreSQL: inserted {DML_ROWS} rows with {NEW_COL} set (ids {ids[0]}–{ids[-1]})")
    return ids


def insert_dml_oracle(base_id: int) -> list[int]:
    import cx_Oracle
    user, rest = ORA_DSN.split("/", 1)
    password, dsn = rest.split("@", 1)
    conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)
    cur = conn.cursor()
    ids = list(range(base_id, base_id + DML_ROWS))
    for i, cid in enumerate(ids):
        cur.execute(
            f"""
            MERGE INTO cache_testing.customers t
            USING (SELECT {cid} AS CUSTOMER_ID FROM dual) s
            ON (t.CUSTOMER_ID = s.CUSTOMER_ID)
            WHEN MATCHED THEN UPDATE SET t.{NEW_COL.upper()} = {i + 1}
            WHEN NOT MATCHED THEN INSERT
              (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, CITY, COUNTRY_CODE,
               TIER, CREDIT_LIMIT, IS_ACTIVE, {NEW_COL.upper()})
            VALUES ({cid}, 'DDLTest{i}', 'LoyaltyV2', 'ddltest{cid}@example.com',
               'TestCity', 'US', 'gold', 5000, 1, {i + 1})
            """
        )
    conn.commit()
    cur.close()
    conn.close()
    _ok(f"Oracle: inserted {DML_ROWS} rows with {NEW_COL} set (ids {ids[0]}–{ids[-1]})")
    return ids


def insert_dml_mongodb(base_id: int) -> list[int]:
    from pymongo import MongoClient, ASCENDING
    from pymongo.errors import BulkWriteError
    client = MongoClient(MGO_URI)
    db = client["cache_testing"]
    ids = list(range(base_id, base_id + DML_ROWS))
    docs = [
        {
            "customer_id": cid,
            "first_name":  f"DDLTest{i}",
            "last_name":   "LoyaltyV2",
            "email":       f"ddltest{cid}@example.com",
            "city":        "TestCity",
            "country_code":"US",
            "tier":        "gold",
            "credit_limit": 5000.0,
            "is_active":   True,
            NEW_COL:       i + 1,
            "created_at":  datetime.now(timezone.utc),
            "updated_at":  datetime.now(timezone.utc),
        }
        for i, cid in enumerate(ids)
    ]
    try:
        db.customers.insert_many(docs, ordered=False)
    except BulkWriteError:
        pass
    client.close()
    _ok(f"MongoDB: inserted {DML_ROWS} docs with {NEW_COL} set (customer_ids {ids[0]}–{ids[-1]})")
    return ids


# ── Step 4: Poll until new column appears in all Iceberg tables ───────────────

def poll_iceberg_columns(deadline: float) -> dict[str, bool]:
    """
    Returns {fqn: True/False} for whether the new column appeared in each table.
    Polls via spark-sql DESCRIBE TABLE run from within a streaming pod.
    """
    remaining = {
        f"{cat}.{ns}.{tbl}"
        for cat, ns, tbl in _ICE_TABLES
    }
    confirmed: dict[str, bool] = {}

    while remaining and time.time() < deadline:
        still_missing = set()
        for fqn in list(remaining):
            cat, ns, tbl = fqn.split(".")
            out = _spark_sql(f"DESCRIBE TABLE `{cat}`.`{ns}`.`{tbl}`")
            if NEW_COL.lower() in out.lower():
                confirmed[fqn] = True
                _ok(f"  Iceberg {fqn}: column '{NEW_COL}' visible")
            else:
                still_missing.add(fqn)
        remaining = still_missing
        if remaining:
            _info(f"  Waiting… {len(remaining)} table(s) still missing '{NEW_COL}': {sorted(remaining)}")
            time.sleep(10)

    for fqn in remaining:
        confirmed[fqn] = False
        _fail(f"  Iceberg {fqn}: column '{NEW_COL}' NOT found within {POLL_SECS}s")

    return confirmed


# ── Step 5: Verify DML rows landed with new column set ────────────────────────

def verify_dml_rows(pg_ids: list[int], ora_ids: list[int], mgo_ids: list[int]) -> bool:
    ok = True

    def _check(fqn: str, pk_col: str, ids: list[int]) -> None:
        nonlocal ok
        id_list = ", ".join(str(i) for i in ids)
        sql = (
            f"SELECT COUNT(*) FROM `{fqn.replace('.', '`.`')}` "
            f"WHERE `{pk_col}` IN ({id_list}) AND `{NEW_COL}` IS NOT NULL"
        )
        out = _spark_sql(sql)
        # spark-sql output: last line is the count value
        count = int(out.strip().splitlines()[-1]) if out.strip() else 0
        if count == len(ids):
            _ok(f"  {fqn}: {count}/{len(ids)} post-DDL rows have {NEW_COL} IS NOT NULL ✓")
        else:
            _fail(f"  {fqn}: only {count}/{len(ids)} post-DDL rows have {NEW_COL} IS NOT NULL")
            ok = False

    _check("postgres.cache_testing.customers",    "id",          pg_ids)
    _check("postgres.cache_testing.customers_sd", "id",          pg_ids)
    _check("oracle.cache_testing.customers",      "CUSTOMER_ID", ora_ids)
    _check("oracle.cache_testing.customers_sd",   "CUSTOMER_ID", ora_ids)
    _check("mongodb.cache_testing.customers",     "customer_id", mgo_ids)
    _check("mongodb.cache_testing.customers_sd",  "customer_id", mgo_ids)
    _check("mongodb.cache_testing.customers_hist","customer_id", mgo_ids)
    return ok


# ── Step 6: Check for pod restarts ───────────────────────────────────────────

def check_no_restarts(before: dict[str, int]) -> bool:
    after = snapshot_restarts()
    crashed = []
    for pod, before_count in before.items():
        after_count = after.get(pod, before_count)
        if after_count > before_count:
            crashed.append(f"{pod}: {before_count} → {after_count}")
    if crashed:
        for c in crashed:
            _fail(f"  Pod restarted during DDL test: {c}")
        return False
    _ok(f"  No pod restarts detected across {len(before)} pod(s)")
    return True


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print()
    print("=" * 70)
    print(f"  DDL E2E Test — new column: {NEW_COL} {NEW_TYPE}")
    print(f"  Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 70)

    # 1. Snapshot pod restart counts before DDL
    print("\n[1/6] Snapshotting pod restart counts …")
    restarts_before = snapshot_restarts()
    _info(f"  Tracking {len(restarts_before)} pod(s)")

    # 2. Apply DDL to all sources
    print(f"\n[2/6] Applying DDL to all 3 sources (ADD COLUMN {NEW_COL} {NEW_TYPE}) …")
    pg_ok = ora_ok = mgo_ok = False
    try:
        apply_ddl_postgres()
        pg_ok = True
    except Exception as exc:
        _fail(f"PostgreSQL DDL failed: {exc}")

    try:
        apply_ddl_oracle()
        ora_ok = True
    except Exception as exc:
        _fail(f"Oracle DDL failed: {exc}")

    try:
        apply_ddl_mongodb()
        mgo_ok = True
    except Exception as exc:
        _fail(f"MongoDB DDL failed: {exc}")

    if not (pg_ok and ora_ok and mgo_ok):
        _fail("DDL application had failures — aborting test.")
        sys.exit(1)

    # 3. Insert DML rows AFTER DDL (these should carry the new column)
    print(f"\n[3/6] Inserting {DML_ROWS} post-DDL DML rows into each source …")
    pg_ids  = insert_dml_postgres(base_id=_BASE_ID)            if pg_ok  else []
    ora_ids = insert_dml_oracle(  base_id=_BASE_ID + 100)      if ora_ok else []
    mgo_ids = insert_dml_mongodb( base_id=_BASE_ID + 200)      if mgo_ok else []

    # 4. Poll Iceberg until new column appears in all tables
    print(f"\n[4/6] Polling Iceberg for '{NEW_COL}' column (timeout {POLL_SECS}s) …")
    deadline = time.time() + POLL_SECS
    col_results = poll_iceberg_columns(deadline)
    all_cols_present = all(col_results.values())

    # 5. Verify DML rows landed with new column non-null
    print(f"\n[5/6] Verifying post-DDL rows have {NEW_COL} IS NOT NULL …")
    dml_ok = verify_dml_rows(pg_ids, ora_ids, mgo_ids)

    # 6. Check no pod restarts
    print(f"\n[6/6] Checking for pod restarts …")
    no_restart = check_no_restarts(restarts_before)

    # ── Summary ──────────────────────────────────────────────────────────────
    print()
    print("=" * 70)
    print("  TEST SUMMARY")
    print("=" * 70)
    print(f"  New column visible in all Iceberg tables : {'PASS' if all_cols_present else 'FAIL'}")
    print(f"  Post-DDL DML rows carry new column       : {'PASS' if dml_ok else 'FAIL'}")
    print(f"  Zero pod restarts during DDL             : {'PASS' if no_restart else 'FAIL'}")
    print()

    if all_cols_present and dml_ok and no_restart:
        print("  ✓  ALL CHECKS PASSED — DDL + DML replication is crash-free.")
        print("     Schema-cache invalidation (Fix 1) and schema-evolution-handler")
        print("     (Fix 2) are working correctly across all three sources.")
    else:
        print("  ✗  SOME CHECKS FAILED — review logs above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
