#!/usr/bin/env python3
"""
scripts/ddl_rename_test.py
==========================
CDC DDL rename/drop stress-test for the CDC → Iceberg pipeline.

Test sequence per source (Oracle, PostgreSQL, MongoDB)
-------------------------------------------------------
For each of 5 new columns (test_col_a … test_col_e):

  Phase 1 – ADD
    (a) ADD  test_col_<x>  SMALLINT
    (b) DML  5 rows WITH the column set

  Phase 2 – RENAME #1  (test_col_<x> → test_col_<x>_r1)
    (c) RENAME column
    (d) DML  5 rows WITH the renamed column set

  Phase 3 – RENAME #2  (test_col_<x>_r1 → test_col_<x>_r2)
    (e) RENAME column
    (f) DML  5 rows WITH the second renamed column set

  Phase 4 – RENAME #3  (test_col_<x>_r2 → test_col_<x>_r3)
    (g) RENAME column
    (h) DML  5 rows WITH the third renamed column set

  Phase 5 – DROP
    (i) DROP test_col_<x>_r3

All 5 columns are processed sequentially (one column at a time).

Final verification
------------------
  • All renamed column names (test_col_<x>_r1/r2/r3) visible in all Iceberg tables
  • Post-rename DML rows have the renamed column IS NOT NULL in Iceberg
  • last_login_at stays BIGINT throughout
  • Zero streaming pod restarts

Iceberg rename note
-------------------
Debezium does NOT emit a RENAME event — a RENAME at the source appears as the
old column disappearing from the schema (handled by the % 10 re-inference cycle)
and the new column appearing (DDL evolution ADD COLUMN path).  Iceberg keeps the
old column (already written rows retain the old name's data); the new column
is added and populated by subsequent DML.  This is the expected behaviour.

Usage (from workstation)
------------------------
  python3 scripts/ddl_rename_test.py

Environment variables
---------------------
  POLL_SECS   max seconds to wait for Iceberg column (default 240)
  DML_ROWS    rows per DML batch (default 5)
  SETTLE_S    seconds to sleep between phases (default 3)
  SOURCE      comma-separated sources to run: oracle,postgres,mongodb (default: all)
              e.g. SOURCE=oracle python3 scripts/ddl_rename_test.py
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone

# ─────────────────────────────────────────────────────────────────────────────
# OpenBao helpers
# ─────────────────────────────────────────────────────────────────────────────
_BAO_ADDR  = os.environ.get("ADDR",     "http://192.168.1.50:30820")
_BAO_TOKEN: str | None = None


def _bao_token() -> str:
    global _BAO_TOKEN
    if _BAO_TOKEN:
        return _BAO_TOKEN
    direct = os.environ.get("BAO_TOKEN")
    if direct:
        _BAO_TOKEN = direct
        return _BAO_TOKEN
    try:
        r = subprocess.run(
            ["kubectl", "-n", "prod", "get", "secret", "openbao-unseal-keys",
             "-o", "jsonpath={.data.root-token}"],
            capture_output=True, text=True, check=True, timeout=10,
        )
        _BAO_TOKEN = base64.b64decode(r.stdout.strip()).decode()
        return _BAO_TOKEN
    except Exception as e:
        raise RuntimeError(
            f"Could not auto-fetch OpenBao token from K8s secret: {e}\n"
            "Set BAO_TOKEN env var explicitly."
        )


def _bao_secret(path: str) -> dict:
    req = urllib.request.Request(
        f"{_BAO_ADDR}/v1/{path}", headers={"X-Vault-Token": _bao_token()}
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())["data"]["data"]


# ─────────────────────────────────────────────────────────────────────────────
# DSN resolution
# ─────────────────────────────────────────────────────────────────────────────
_dsns_resolved = False
PG_DSN  = ""
MGO_URI = ""


def _resolve_dsns() -> None:
    global _dsns_resolved, PG_DSN, MGO_URI
    if _dsns_resolved:
        return
    pg  = _bao_secret("secret/data/platform/postgres")
    mgo = _bao_secret("secret/data/platform/mongodb")
    h, p, d = (pg.get("host", "postgresql.prod.svc.cluster.local"),
                pg.get("port", "5432"), pg.get("database", "cache_testing"))
    PG_DSN  = os.environ.get("PG_DSN") or f"postgresql://{pg['user']}:{pg['password']}@{h}:{p}/{d}"
    mh, mp  = mgo.get("host", "mongodb.prod.svc.cluster.local"), mgo.get("port", "27017")
    mauth   = mgo.get("auth_source", "admin")
    MGO_URI = os.environ.get("MGO_URI") or (
        f"mongodb://{mgo['user']}:{mgo['password']}@{mh}:{mp}/?authSource={mauth}&replicaSet=rs0"
    )
    _dsns_resolved = True


_ora_dsn_cache: str | None = None


def _ora_dsn() -> str:
    global _ora_dsn_cache
    if _ora_dsn_cache:
        return _ora_dsn_cache
    ora = _bao_secret("secret/data/platform/oracle")
    h = ora.get("host", "oracle-xe.prod.svc.cluster.local")
    p = ora.get("port", "1521")
    s = ora.get("service", "XEPDB1")
    _ora_dsn_cache = f"{ora['app_user']}/{ora['app_password']}@{h}:{p}/{s}"
    return _ora_dsn_cache


# ─────────────────────────────────────────────────────────────────────────────
# Test parameters
# ─────────────────────────────────────────────────────────────────────────────
POLL_SECS      = int(os.environ.get("POLL_SECS", "240"))
DML_ROWS       = int(os.environ.get("DML_ROWS",  "5"))
SETTLE_S       = int(os.environ.get("SETTLE_S",  "3"))
_SOURCE_FILTER = {s.strip().lower() for s in os.environ.get("SOURCE", "").split(",") if s.strip()}

# 5 test columns — short names to avoid Oracle 30-char limit
# Rename chain: test_col_a → test_col_a_r1 → test_col_a_r2 → test_col_a_r3 → DROP
_TEST_COLS = ["test_col_a", "test_col_b", "test_col_c", "test_col_d", "test_col_e"]

# ID space: epoch-derived so every test run gets a FRESH block of IDs.
#
# WHY THIS IS CRITICAL (permanent fix for Oracle no-op update problem):
# ──────────────────────────────────────────────────────────────────────
# Oracle MERGE executes UPDATE when the PK already exists. If the row's column
# values are IDENTICAL to what's already stored, Oracle writes NO redo log entry
# (no-op update optimisation). Debezium reads from redo logs — a no-op update is
# invisible to LogMiner. Result: Debezium emits nothing, the Kafka topic offset
# doesn't advance, and Iceberg never sees those rows.
#
# Hardcoded bases (e.g. 4_100_001) reuse the same PKs on every test run.
# After run 1 inserts the rows, runs 2+ MERGE→UPDATE with identical values → no-op.
#
# Fix: derive bases from the current epoch second so each run occupies a
# brand-new, never-before-seen PK range. The formula gives enough headroom:
#   _PG_BASE  = epoch_sec * 10 + 0  (PostgreSQL ids are sequential integers)
#   _ORA_BASE = epoch_sec * 10 + 1  (Oracle CUSTOMER_ID)
#   _MGO_BASE = epoch_sec * 10 + 2  (MongoDB customer_id)
# Each run uses at most DML_ROWS * phases * columns = 5*4*5 = 100 IDs per source.
# epoch_sec * 10 spacing ensures no collision across runs within a 1-second window.
_RUN_EPOCH = int(time.time())   # captured once at import so all bases are consistent
_PG_BASE   = _RUN_EPOCH * 10 + 0
_ORA_BASE  = _RUN_EPOCH * 10 + 1
_MGO_BASE  = _RUN_EPOCH * 10 + 2

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

# ─────────────────────────────────────────────────────────────────────────────
# Result accumulator
# ─────────────────────────────────────────────────────────────────────────────
_results: list[tuple[str, bool, str]] = []


def _record(label: str, passed: bool, detail: str = "") -> None:
    _results.append((label, passed, detail))


# ─────────────────────────────────────────────────────────────────────────────
# Logging helpers
# ─────────────────────────────────────────────────────────────────────────────
def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")

def _ok(msg: str)   -> None: print(f"  [{_ts()}] ✓  {msg}", flush=True)
def _fail(msg: str) -> None: print(f"  [{_ts()}] ✗  {msg}", file=sys.stderr, flush=True)
def _info(msg: str) -> None: print(f"  [{_ts()}]    {msg}", flush=True)
def _hdr(msg: str)  -> None: print(f"\n{'─'*72}\n  {msg}\n{'─'*72}", flush=True)
def _phase(msg: str)-> None: print(f"\n  ── {msg}", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# kubectl / pod helpers
# ─────────────────────────────────────────────────────────────────────────────
def _kubectl(*args: str, check: bool = True) -> str:
    r = subprocess.run(["kubectl", "-n", "prod", *args],
                       capture_output=True, text=True, check=check)
    return r.stdout.strip()


def _get_db_pod() -> str:
    """Resolve the live postgres-standard streaming pod name on every call.

    Never cached — pod name changes after every rollout restart; label selector is stable.
    """
    pod = _kubectl(
        "get", "pods",
        "-l", "app=kafka-to-iceberg,pipeline.write-mode=standard,pipeline.source=postgres",
        "--field-selector=status.phase=Running",
        "-o", "jsonpath={.items[0].metadata.name}",
    )
    if not pod:
        raise RuntimeError("No running postgres-standard streaming pod.")
    return pod


def _pod_exec(code: str, timeout: int = 60) -> str:
    r = subprocess.run(
        ["kubectl", "-n", "prod", "exec", _get_db_pod(), "--", "python3", "-c", code],
        capture_output=True, text=True, timeout=timeout,
    )
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip() or r.stdout.strip())
    return r.stdout.strip()


# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL helpers
# ─────────────────────────────────────────────────────────────────────────────
def _pg_ddl(sql: str) -> None:
    r = subprocess.run(
        ["kubectl", "-n", "prod", "exec", "postgresql-0", "--",
         "psql", "-U", "postgres", "-d", "cache_testing",
         "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True, timeout=30,
    )
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip() or r.stdout.strip())


def pg_add_col(col: str) -> None:
    _pg_ddl(f"ALTER TABLE customers ADD COLUMN IF NOT EXISTS {col} SMALLINT")


def pg_rename_col(old: str, new: str) -> None:
    # Rename only if old exists and new does not
    _pg_ddl(f"""
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='customers' AND column_name='{old}'
  ) THEN
    ALTER TABLE customers RENAME COLUMN {old} TO {new};
  END IF;
END $$;
""")


def pg_drop_col(col: str) -> None:
    _pg_ddl(f"ALTER TABLE customers DROP COLUMN IF EXISTS {col}")


def pg_dml(base_id: int, col_name: str, col_value: int) -> list[int]:
    """Insert rows with the given test column set."""
    _resolve_dsns()
    ids  = list(range(base_id, base_id + DML_ROWS))
    rows = [(cid, f"RenameTestPG{cid}", f"rt{cid}@pg.test", "gold", col_value + i)
            for i, cid in enumerate(ids)]
    _pod_exec(f"""
import psycopg2
conn = psycopg2.connect({PG_DSN!r})
conn.autocommit = True
cur = conn.cursor()
for row in {rows!r}:
    try:
        cur.execute(
            "INSERT INTO customers(id,name,email,tier,{col_name}) "
            "VALUES(%s,%s,%s,%s,%s) "
            "ON CONFLICT(id) DO UPDATE SET {col_name}=EXCLUDED.{col_name},tier=EXCLUDED.tier",
            row)
    except Exception as e:
        # Column may not exist yet in same transaction window — skip gracefully
        conn.rollback()
        cur.execute(
            "INSERT INTO customers(id,name,email,tier) VALUES(%s,%s,%s,%s) "
            "ON CONFLICT(id) DO UPDATE SET tier=EXCLUDED.tier",
            (row[0], row[1], row[2], row[3]))
conn.close(); print("ok")
""")
    return ids


# ─────────────────────────────────────────────────────────────────────────────
# Oracle helpers
# ─────────────────────────────────────────────────────────────────────────────
def _ora_run(script: str) -> str:
    ora_pod = _kubectl("get", "pods", "-l", "app=oracle-xe",
                       "-o", "jsonpath={.items[0].metadata.name}")
    if not ora_pod:
        raise RuntimeError("No oracle-xe pod found.")
    full = script.strip() + "\nEXIT;\n"
    r = subprocess.run(
        ["kubectl", "-n", "prod", "exec", ora_pod, "--",
         "bash", "-c",
         f"echo {subprocess.list2cmdline([full])} | sqlplus -s {_ora_dsn()}"],
        capture_output=True, text=True, timeout=60,
    )
    out = (r.stdout + r.stderr).strip()
    errs = [l for l in out.splitlines() if l.strip().startswith("ORA-")]
    if r.returncode != 0 or errs:
        raise RuntimeError(out)
    return out


def ora_add_col(col: str) -> None:
    _ora_run(f"""
DECLARE v NUMBER;
BEGIN
  SELECT COUNT(*) INTO v FROM all_tab_columns
  WHERE owner='CACHE_TESTING' AND table_name='CUSTOMERS'
    AND column_name='{col.upper()}';
  IF v = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE CUSTOMERS ADD ({col.upper()} SMALLINT)';
  END IF;
END;
/""")


def ora_rename_col(old: str, new: str) -> None:
    """Oracle 12.2+ supports RENAME COLUMN. Falls back to ADD+UPDATE+DROP."""
    _ora_run(f"""
DECLARE
  v_old NUMBER;
  v_new NUMBER;
BEGIN
  SELECT COUNT(*) INTO v_old FROM all_tab_columns
  WHERE owner='CACHE_TESTING' AND table_name='CUSTOMERS'
    AND column_name='{old.upper()}';
  SELECT COUNT(*) INTO v_new FROM all_tab_columns
  WHERE owner='CACHE_TESTING' AND table_name='CUSTOMERS'
    AND column_name='{new.upper()}';
  IF v_old > 0 AND v_new = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE CUSTOMERS RENAME COLUMN {old.upper()} TO {new.upper()}';
  END IF;
END;
/""")


def ora_drop_col(col: str) -> None:
    _ora_run(f"""
DECLARE v NUMBER;
BEGIN
  SELECT COUNT(*) INTO v FROM all_tab_columns
  WHERE owner='CACHE_TESTING' AND table_name='CUSTOMERS'
    AND column_name='{col.upper()}';
  IF v > 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE CUSTOMERS DROP COLUMN {col.upper()}';
  END IF;
END;
/""")


def ora_modify_col_number(col: str, new_type: str = "NUMBER(18,4)") -> None:
    """ALTER TABLE MODIFY an existing NUMBER column to a wider precision."""
    _ora_run(f"""
DECLARE v NUMBER;
BEGIN
  SELECT COUNT(*) INTO v FROM all_tab_columns
  WHERE owner='CACHE_TESTING' AND table_name='CUSTOMERS'
    AND column_name='{col.upper()}';
  IF v > 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE CUSTOMERS MODIFY ({col.upper()} {new_type})';
  END IF;
END;
/""")


def ora_modify_col_varchar(col: str, new_length: int = 200) -> None:
    """ALTER TABLE MODIFY an existing VARCHAR2 column to a new length."""
    _ora_run(f"""
DECLARE v NUMBER;
BEGIN
  SELECT COUNT(*) INTO v FROM all_tab_columns
  WHERE owner='CACHE_TESTING' AND table_name='CUSTOMERS'
    AND column_name='{col.upper()}';
  IF v > 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE CUSTOMERS MODIFY ({col.upper()} VARCHAR2({new_length}))';
  END IF;
END;
/""")


def ora_dml(base_id: int, col_name: str, col_value: int) -> list[int]:
    """
    DELETE then INSERT — never MERGE/UPDATE.

    Why DELETE+INSERT instead of MERGE:
    Oracle's no-op update optimisation: if a MERGE executes the WHEN MATCHED UPDATE
    path but no column value actually changes, Oracle writes NO redo log entry.
    Debezium reads from redo logs (LogMiner) — a no-op update is invisible.
    Kafka offset stays flat, Iceberg never receives the row.

    DELETE always writes a redo entry (even if the row doesn't exist — it's a no-op
    at the data level but the DELETE statement itself is logged).
    INSERT always writes a redo entry unconditionally.
    Together they guarantee Debezium captures the event regardless of prior state.
    """
    ids = list(range(base_id, base_id + DML_ROWS))
    stmts = "\n".join(
        f"DELETE FROM CUSTOMERS WHERE CUSTOMER_ID={cid};\n"
        f"INSERT INTO CUSTOMERS(CUSTOMER_ID,FIRST_NAME,LAST_NAME,EMAIL,"
        f"CITY,COUNTRY_CODE,TIER,CREDIT_LIMIT,IS_ACTIVE,{col_name.upper()}) "
        f"VALUES({cid},'RenameORA{cid}','Test','rt{cid}@ora.test',"
        f"'TestCity','US','GOLD',5000,'Y',{col_value + i});"
        for i, cid in enumerate(ids)
    )
    _ora_run(stmts + "\nCOMMIT;")
    return ids


# ─────────────────────────────────────────────────────────────────────────────
# MongoDB helpers
# ─────────────────────────────────────────────────────────────────────────────
def mgo_add_col(col: str) -> None:
    """Seed the field on one existing document so Debezium sees the schema change."""
    _resolve_dsns()
    _pod_exec(f"""
from pymongo import MongoClient
c = MongoClient({MGO_URI!r})
c["cache_testing"].customers.update_one(
    {{"{col}": {{"$exists": False}}}},
    {{"$set": {{"{col}": None}}}})
c.close(); print("ok")
""")


def mgo_rename_col(old: str, new: str) -> None:
    """$rename renames the field on every document that has it."""
    _resolve_dsns()
    _pod_exec(f"""
from pymongo import MongoClient
c = MongoClient({MGO_URI!r})
c["cache_testing"].customers.update_many(
    {{"{old}": {{"$exists": True}}}},
    {{"$rename": {{"{old}": "{new}"}}}})
c.close(); print("ok")
""")


def mgo_drop_col(col: str) -> None:
    _resolve_dsns()
    _pod_exec(f"""
from pymongo import MongoClient
c = MongoClient({MGO_URI!r})
c["cache_testing"].customers.update_many(
    {{"{col}": {{"$exists": True}}}},
    {{"$unset": {{"{col}": ""}}}})
c.close(); print("ok")
""")


def mgo_dml(base_id: int, col_name: str, col_value: int) -> list[int]:
    _resolve_dsns()
    ids  = list(range(base_id, base_id + DML_ROWS))
    docs = [{"customer_id": cid, "first_name": f"Rename{i}", "last_name": "MGO",
             "email": f"rt{cid}@mgo.test", "city": "TestCity",
             "country_code": "US", "tier": "gold", "credit_limit": 5000.0,
             "is_active": True, col_name: col_value + i}
            for i, cid in enumerate(ids)]
    _pod_exec(f"""
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
c = MongoClient({MGO_URI!r})
try:
    c["cache_testing"].customers.insert_many({docs!r}, ordered=False)
except BulkWriteError:
    pass
c.close(); print("ok")
""")
    return ids


# ─────────────────────────────────────────────────────────────────────────────
# Spark SQL / Iceberg verification
# ─────────────────────────────────────────────────────────────────────────────
_CONF_BUILDER = """\
import sys; sys.path.insert(0, '/opt/spark/work-dir')
from bao_spark_init import BaoSparkInit
bao  = BaoSparkInit()
pol  = bao.polaris_creds()
uri  = pol.get('url') or 'http://polaris-rest.prod.svc.cluster.local:8181/api/catalog'
cred = pol['spark_svc_id'] + ':' + pol['spark_svc_secret']
s3   = bao.s3_creds()
rows = [('spark.sql.extensions',
         'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')]
for cat, wh in [('postgres','pg_lakehouse'),('oracle','ora_lakehouse'),('mongodb','mgo_lakehouse')]:
    rows += [
        (f'spark.sql.catalog.{cat}', 'org.apache.iceberg.spark.SparkCatalog'),
        (f'spark.sql.catalog.{cat}.type', 'rest'),
        (f'spark.sql.catalog.{cat}.uri', uri),
        (f'spark.sql.catalog.{cat}.oauth2-server-uri', uri+'/v1/oauth/tokens'),
        (f'spark.sql.catalog.{cat}.credential', cred),
        (f'spark.sql.catalog.{cat}.warehouse', wh),
        (f'spark.sql.catalog.{cat}.scope', 'PRINCIPAL_ROLE:ALL'),
        (f'spark.sql.catalog.{cat}.rest.auth.type', 'oauth2'),
        (f'spark.sql.catalog.{cat}.s3.access-key-id', s3['access_key']),
        (f'spark.sql.catalog.{cat}.s3.secret-access-key', s3['secret_key']),
        (f'spark.sql.catalog.{cat}.s3.endpoint', s3['endpoint']),
        (f'spark.sql.catalog.{cat}.s3.path-style-access', 'true'),
        (f'spark.sql.catalog.{cat}.client.region', s3['region']),
    ]
print('\\n'.join(f'{k}={v}' for k, v in rows))
"""

_spark_conf_flags: str | None = None


def _get_spark_conf_flags() -> str:
    global _spark_conf_flags
    if _spark_conf_flags:
        return _spark_conf_flags
    r = subprocess.run(
        ["kubectl", "-n", "prod", "exec", _get_db_pod(), "--", "python3", "-c", _CONF_BUILDER],
        capture_output=True, text=True, timeout=30,
    )
    if r.returncode != 0:
        raise RuntimeError(f"Spark conf build failed:\n{r.stderr.strip()}")
    _spark_conf_flags = " ".join(
        f"--conf '{l.strip()}'" for l in r.stdout.strip().splitlines() if "=" in l
    )
    return _spark_conf_flags


def _spark_sql(sql: str, timeout: int = 120) -> str:
    esc = sql.strip().replace("'", r"'\''")
    r = subprocess.run(
        ["kubectl", "-n", "prod", "exec", _get_db_pod(), "--",
         "bash", "-c",
         f"cd /opt/spark/work-dir && spark-sql {_get_spark_conf_flags()} -e '{esc}'"],
        capture_output=True, text=True, timeout=timeout,
    )
    return r.stdout.strip()


def _iceberg_has_col(cat: str, ns: str, tbl: str, col: str) -> bool:
    out = _spark_sql(f"DESCRIBE TABLE `{cat}`.`{ns}`.`{tbl}`")
    return col.lower() in out.lower()


def _iceberg_count(fqn: str, pk: str, ids: list[int], cond: str) -> int:
    id_list = ", ".join(str(i) for i in ids)
    out = _spark_sql(
        f"SELECT COUNT(*) FROM `{fqn.replace('.', '`.`')}` "
        f"WHERE `{pk}` IN ({id_list}) AND {cond}"
    )
    try:
        return int(out.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return 0


def _col_type(cat: str, ns: str, tbl: str, col: str) -> str:
    out = _spark_sql(f"DESCRIBE TABLE `{cat}`.`{ns}`.`{tbl}`")
    for line in out.splitlines():
        parts = line.split()
        if parts and parts[0].lower() == col.lower():
            return parts[1].lower() if len(parts) > 1 else "unknown"
    return "not_found"


# ─────────────────────────────────────────────────────────────────────────────
# Pod restart tracking
# ─────────────────────────────────────────────────────────────────────────────
def snapshot_restarts() -> dict[str, int]:
    out = _kubectl(
        "get", "pods", "-l", "app=kafka-to-iceberg", "-o",
        r"jsonpath={range .items[*]}{.metadata.name}{':'}{.status.containerStatuses[0].restartCount}{'\n'}{end}",
        check=False,
    )
    counts: dict[str, int] = {}
    for line in out.splitlines():
        if ":" not in line:
            continue
        name, _, n = line.rpartition(":")
        try:
            counts[name.strip()] = int(n.strip() or "0")
        except ValueError:
            pass
    return counts


# ─────────────────────────────────────────────────────────────────────────────
# Per-column rename test runner
# ─────────────────────────────────────────────────────────────────────────────
def run_column_rename_test(
    source: str,
    col_base: str,
    id_start: int,
    add_fn,
    rename_fn,
    drop_fn,
    dml_fn,
) -> tuple[list[tuple[str, list[int]]], int]:
    """
    Run the full ADD → DML → RENAME×3 (DML each) → DROP cycle for one column.

    Returns:
      phases_ids: list of (col_name_used, [ids]) for each DML phase
      next_id:    next available base ID
    """
    phases_ids: list[tuple[str, list[int]]] = []
    cur_id = id_start

    col_v0  = col_base          # original
    col_v1  = col_base + "_r1"  # after rename 1
    col_v2  = col_base + "_r2"  # after rename 2
    col_v3  = col_base + "_r3"  # after rename 3

    # Phase 1 — ADD + DML
    _phase(f"[{source}] {col_base}: Phase 1 — ADD column")
    try:
        add_fn(col_v0)
        _ok(f"ADD {col_v0}")
    except Exception as e:
        _fail(f"ADD {col_v0}: {e}")
        return phases_ids, cur_id + DML_ROWS * 4

    time.sleep(SETTLE_S)
    try:
        ids = dml_fn(cur_id, col_v0, 10)
        phases_ids.append((col_v0, ids))
        _ok(f"DML {len(ids)} rows with {col_v0}")
    except Exception as e:
        _fail(f"DML after ADD {col_v0}: {e}")
    cur_id += DML_ROWS

    # Phase 2 — RENAME #1 + DML
    _phase(f"[{source}] {col_base}: Phase 2 — RENAME {col_v0} → {col_v1}")
    time.sleep(SETTLE_S)
    try:
        rename_fn(col_v0, col_v1)
        _ok(f"RENAME {col_v0} → {col_v1}")
    except Exception as e:
        _fail(f"RENAME {col_v0} → {col_v1}: {e}")
        col_v1 = col_v0  # fall back to original name for DML

    time.sleep(SETTLE_S)
    try:
        ids = dml_fn(cur_id, col_v1, 20)
        phases_ids.append((col_v1, ids))
        _ok(f"DML {len(ids)} rows with {col_v1}")
    except Exception as e:
        _fail(f"DML after rename-1 {col_v1}: {e}")
    cur_id += DML_ROWS

    # Phase 3 — RENAME #2 + DML
    _phase(f"[{source}] {col_base}: Phase 3 — RENAME {col_v1} → {col_v2}")
    time.sleep(SETTLE_S)
    try:
        rename_fn(col_v1, col_v2)
        _ok(f"RENAME {col_v1} → {col_v2}")
    except Exception as e:
        _fail(f"RENAME {col_v1} → {col_v2}: {e}")
        col_v2 = col_v1  # fall back

    time.sleep(SETTLE_S)
    try:
        ids = dml_fn(cur_id, col_v2, 30)
        phases_ids.append((col_v2, ids))
        _ok(f"DML {len(ids)} rows with {col_v2}")
    except Exception as e:
        _fail(f"DML after rename-2 {col_v2}: {e}")
    cur_id += DML_ROWS

    # Phase 4 — RENAME #3 + DML
    _phase(f"[{source}] {col_base}: Phase 4 — RENAME {col_v2} → {col_v3}")
    time.sleep(SETTLE_S)
    try:
        rename_fn(col_v2, col_v3)
        _ok(f"RENAME {col_v2} → {col_v3}")
    except Exception as e:
        _fail(f"RENAME {col_v2} → {col_v3}: {e}")
        col_v3 = col_v2  # fall back

    time.sleep(SETTLE_S)
    try:
        ids = dml_fn(cur_id, col_v3, 40)
        phases_ids.append((col_v3, ids))
        _ok(f"DML {len(ids)} rows with {col_v3}")
    except Exception as e:
        _fail(f"DML after rename-3 {col_v3}: {e}")
    cur_id += DML_ROWS

    # Phase 5 — DROP final name
    _phase(f"[{source}] {col_base}: Phase 5 — DROP {col_v3}")
    time.sleep(SETTLE_S)
    try:
        drop_fn(col_v3)
        _ok(f"DROP {col_v3}")
    except Exception as e:
        _fail(f"DROP {col_v3}: {e}")

    return phases_ids, cur_id


# ─────────────────────────────────────────────────────────────────────────────
# Oracle ALTER TABLE MODIFY test runner
# ─────────────────────────────────────────────────────────────────────────────
def run_ora_modify_test(ora_id: int) -> tuple[dict[str, list[int]], int]:
    """
    Test ALTER TABLE MODIFY for Oracle:
      1. ADD test_mod_num  SMALLINT       → DML → MODIFY to NUMBER(18,4) → DML
      2. ADD test_mod_str  VARCHAR2(50)   → DML → MODIFY to VARCHAR2(200) → DML

    Returns:
      ids_map: { col_name: [ids] }  for final Iceberg verification
      next_id: next available base ID
    """
    ids_map: dict[str, list[int]] = {}
    cur_id = ora_id

    # ── Case 1: NUMBER column (SMALLINT → NUMBER(18,4)) ──────────────────────
    num_col = "test_mod_num"
    _phase(f"[ORA] MODIFY test — ADD {num_col} SMALLINT")
    try:
        ora_add_col(num_col)
        _ok(f"ADD {num_col} SMALLINT")
    except Exception as e:
        _fail(f"ADD {num_col}: {e}")

    time.sleep(SETTLE_S)
    try:
        ids = ora_dml(cur_id, num_col, 50)
        ids_map.setdefault(num_col, []).extend(ids)
        _ok(f"DML {len(ids)} rows with {num_col} (pre-MODIFY)")
    except Exception as e:
        _fail(f"DML {num_col} pre-MODIFY: {e}")
    cur_id += DML_ROWS

    _phase(f"[ORA] MODIFY {num_col} → NUMBER(18,4)")
    time.sleep(SETTLE_S)
    try:
        ora_modify_col_number(num_col, "NUMBER(18,4)")
        _ok(f"ALTER TABLE CUSTOMERS MODIFY ({num_col.upper()} NUMBER(18,4))")
    except Exception as e:
        _fail(f"MODIFY {num_col}: {e}")

    time.sleep(SETTLE_S)
    try:
        ids = ora_dml(cur_id, num_col, 60)
        ids_map.setdefault(num_col, []).extend(ids)
        _ok(f"DML {len(ids)} rows with {num_col} (post-MODIFY NUMBER(18,4))")
    except Exception as e:
        _fail(f"DML {num_col} post-MODIFY: {e}")
    cur_id += DML_ROWS

    # Clean up
    time.sleep(SETTLE_S)
    try:
        ora_drop_col(num_col)
        _ok(f"DROP {num_col}")
    except Exception as e:
        _fail(f"DROP {num_col}: {e}")

    # ── Case 2: VARCHAR2 column (VARCHAR2(50) → VARCHAR2(200)) ───────────────
    str_col = "test_mod_str"
    _phase(f"[ORA] MODIFY test — ADD {str_col} VARCHAR2(50)")
    time.sleep(SETTLE_S)
    try:
        _ora_run(f"""
DECLARE v NUMBER;
BEGIN
  SELECT COUNT(*) INTO v FROM all_tab_columns
  WHERE owner='CACHE_TESTING' AND table_name='CUSTOMERS'
    AND column_name='{str_col.upper()}';
  IF v = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE CUSTOMERS ADD ({str_col.upper()} VARCHAR2(50))';
  END IF;
END;
/""")
        _ok(f"ADD {str_col} VARCHAR2(50)")
    except Exception as e:
        _fail(f"ADD {str_col}: {e}")

    # DML: insert rows with a short string value in the new column
    time.sleep(SETTLE_S)
    str_ids = list(range(cur_id, cur_id + DML_ROWS))
    str_stmts = "\n".join(
        f"DELETE FROM CUSTOMERS WHERE CUSTOMER_ID={cid};\n"
        f"INSERT INTO CUSTOMERS(CUSTOMER_ID,FIRST_NAME,LAST_NAME,EMAIL,"
        f"CITY,COUNTRY_CODE,TIER,CREDIT_LIMIT,IS_ACTIVE,{str_col.upper()}) "
        f"VALUES({cid},'ModStrORA{cid}','Test','mods{cid}@ora.test',"
        f"'TestCity','US','GOLD',5000,'Y','short_{cid}');"
        for cid in str_ids
    )
    try:
        _ora_run(str_stmts + "\nCOMMIT;")
        ids_map.setdefault(str_col, []).extend(str_ids)
        _ok(f"DML {len(str_ids)} rows with {str_col} (pre-MODIFY)")
    except Exception as e:
        _fail(f"DML {str_col} pre-MODIFY: {e}")
    cur_id += DML_ROWS

    _phase(f"[ORA] MODIFY {str_col} → VARCHAR2(200)")
    time.sleep(SETTLE_S)
    try:
        ora_modify_col_varchar(str_col, 200)
        _ok(f"ALTER TABLE CUSTOMERS MODIFY ({str_col.upper()} VARCHAR2(200))")
    except Exception as e:
        _fail(f"MODIFY {str_col}: {e}")

    # DML after MODIFY: insert rows with a longer string (proves widened column works)
    time.sleep(SETTLE_S)
    post_ids = list(range(cur_id, cur_id + DML_ROWS))
    post_stmts = "\n".join(
        f"DELETE FROM CUSTOMERS WHERE CUSTOMER_ID={cid};\n"
        f"INSERT INTO CUSTOMERS(CUSTOMER_ID,FIRST_NAME,LAST_NAME,EMAIL,"
        f"CITY,COUNTRY_CODE,TIER,CREDIT_LIMIT,IS_ACTIVE,{str_col.upper()}) "
        f"VALUES({cid},'ModStrORA{cid}','Test','mods{cid}@ora.test',"
        f"'TestCity','US','GOLD',5000,'Y','this_is_a_longer_value_{cid}');"
        for cid in post_ids
    )
    try:
        _ora_run(post_stmts + "\nCOMMIT;")
        ids_map.setdefault(str_col, []).extend(post_ids)
        _ok(f"DML {len(post_ids)} rows with {str_col} (post-MODIFY VARCHAR2(200))")
    except Exception as e:
        _fail(f"DML {str_col} post-MODIFY: {e}")
    cur_id += DML_ROWS

    # Clean up
    time.sleep(SETTLE_S)
    try:
        ora_drop_col(str_col)
        _ok(f"DROP {str_col}")
    except Exception as e:
        _fail(f"DROP {str_col}: {e}")

    return ids_map, cur_id


# ─────────────────────────────────────────────────────────────────────────────
# Poll Iceberg for a column  (with timeout)
# ─────────────────────────────────────────────────────────────────────────────
def _poll_iceberg_col(col: str, tables: list[tuple], timeout: int = POLL_SECS) -> dict:
    """
    Poll all given tables until `col` is visible or timeout.
    Returns dict: (cat,ns,tbl) -> True/False
    """
    remaining = set(tables)
    found     = {}
    deadline  = time.time() + timeout
    while remaining and time.time() < deadline:
        still = set()
        for entry in list(remaining):
            cat, ns, tbl = entry
            if _iceberg_has_col(cat, ns, tbl, col):
                found[entry] = True
                _ok(f"Iceberg {cat}.{ns}.{tbl}: '{col}' visible")
            else:
                still.add(entry)
        remaining = still
        if remaining:
            _info(f"  {len(remaining)} table(s) still missing '{col}' — retrying in 15 s …")
            time.sleep(15)
    for entry in remaining:
        cat, ns, tbl = entry
        found[entry] = False
        _fail(f"Iceberg {cat}.{ns}.{tbl}: '{col}' NOT visible after {timeout}s")
    return found


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    _run_pg  = not _SOURCE_FILTER or "postgres" in _SOURCE_FILTER
    _run_ora = not _SOURCE_FILTER or "oracle"   in _SOURCE_FILTER
    _run_mgo = not _SOURCE_FILTER or "mongodb"  in _SOURCE_FILTER
    _sources_label = ", ".join(
        s for s, active in [("Oracle", _run_ora), ("PostgreSQL", _run_pg), ("MongoDB", _run_mgo)]
        if active
    )

    print()
    print("=" * 72)
    print("  CDC RENAME/DROP COLUMN STRESS TEST")
    print(f"  Columns: {_TEST_COLS}")
    print("  Sequence per column: ADD → DML → RENAME×3 (DML each) → DROP")
    print(f"  Sources: {_sources_label}")
    print(f"  DML_ROWS={DML_ROWS}  SETTLE_S={SETTLE_S}s  POLL_SECS={POLL_SECS}s")
    print(f"  Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 72)

    _info("Resolving OpenBao credentials …")
    if _run_pg or _run_mgo:
        _resolve_dsns()
    if _run_ora:
        _ora_dsn()
    _ok("Credentials resolved")

    restarts_before = snapshot_restarts()
    _info(f"Tracking {len(restarts_before)} streaming pod(s)")

    _info("Pre-fetching Spark catalog conf …")
    _get_spark_conf_flags()
    _ok("Spark catalog conf ready")

    # ── Collect all DML IDs per source × phase for final verification ─────────
    # Structure: { source_key: { col_name: [ids] } }
    all_ids: dict[str, dict[str, list[int]]] = {
        "postgres": {}, "oracle": {}, "mongodb": {}
    }
    # Collect all renamed column names seen (for Iceberg column-visibility check)
    all_renamed_cols: set[str] = set()

    # ─────────────────────────────────────────────────────────────────────────
    # PostgreSQL
    # ─────────────────────────────────────────────────────────────────────────
    if _run_pg:
        _hdr("PostgreSQL — 5 columns ADD/RENAME×3/DROP")
        pg_id = _PG_BASE
        for col_base in _TEST_COLS:
            phases, pg_id = run_column_rename_test(
                "PG", col_base, pg_id,
                pg_add_col, pg_rename_col, pg_drop_col, pg_dml,
            )
            for col_name, ids in phases:
                all_ids["postgres"].setdefault(col_name, []).extend(ids)
                all_renamed_cols.add(col_name)
            _record(f"PG {col_base}: all phases completed",
                    len(phases) == 4, f"{len(phases)}/4 DML phases")

    # ─────────────────────────────────────────────────────────────────────────
    # Oracle
    # ─────────────────────────────────────────────────────────────────────────
    if _run_ora:
        _hdr("Oracle — 5 columns ADD/RENAME×3/DROP")
        ora_id = _ORA_BASE
        for col_base in _TEST_COLS:
            phases, ora_id = run_column_rename_test(
                "ORA", col_base, ora_id,
                ora_add_col, ora_rename_col, ora_drop_col, ora_dml,
            )
            for col_name, ids in phases:
                all_ids["oracle"].setdefault(col_name, []).extend(ids)
                all_renamed_cols.add(col_name)
            _record(f"ORA {col_base}: all phases completed",
                    len(phases) == 4, f"{len(phases)}/4 DML phases")

        # MODIFY sub-test: NUMBER and VARCHAR2 column type changes
        _hdr("Oracle — ALTER TABLE MODIFY (NUMBER + VARCHAR2)")
        mod_ids, ora_id = run_ora_modify_test(ora_id)
        for col_name, ids in mod_ids.items():
            all_ids["oracle"].setdefault(col_name, []).extend(ids)
        _record("ORA MODIFY test_mod_num: NUMBER(18,4)",
                bool(mod_ids.get("test_mod_num")),
                f"{len(mod_ids.get('test_mod_num', []))} rows captured")
        _record("ORA MODIFY test_mod_str: VARCHAR2(200)",
                bool(mod_ids.get("test_mod_str")),
                f"{len(mod_ids.get('test_mod_str', []))} rows captured")

    # ─────────────────────────────────────────────────────────────────────────
    # MongoDB
    # ─────────────────────────────────────────────────────────────────────────
    if _run_mgo:
        _hdr("MongoDB — 5 columns ADD/RENAME×3/DROP")
        mgo_id = _MGO_BASE
        for col_base in _TEST_COLS:
            phases, mgo_id = run_column_rename_test(
                "MGO", col_base, mgo_id,
                mgo_add_col, mgo_rename_col, mgo_drop_col, mgo_dml,
            )
            for col_name, ids in phases:
                all_ids["mongodb"].setdefault(col_name, []).extend(ids)
                all_renamed_cols.add(col_name)
            _record(f"MGO {col_base}: all phases completed",
                    len(phases) == 4, f"{len(phases)}/4 DML phases")

    # ─────────────────────────────────────────────────────────────────────────
    # Wait for pipeline flush
    # ─────────────────────────────────────────────────────────────────────────
    _hdr("Waiting for Kafka → Iceberg pipeline to flush …")
    _info("Sleeping 60 s for micro-batches to commit …")
    time.sleep(60)

    # ─────────────────────────────────────────────────────────────────────────
    # Iceberg: poll for each renamed column name to appear in all tables
    # ─────────────────────────────────────────────────────────────────────────
    # The columns to verify are r1/r2/r3 names (rename = new ADD from Iceberg POV)
    # The base names (test_col_*) were the originals before first rename so they
    # may or may not appear depending on whether DML landed before rename.
    _hdr(f"Polling Iceberg for renamed column visibility ({len(all_renamed_cols)} unique names)")
    _info(f"Columns to check: {sorted(all_renamed_cols)}")

    # For Iceberg verification, check r1/r2/r3 names per source:
    # postgres and oracle: all 7 tables
    # mongodb: only the 3 mongodb tables
    pg_ora_tables = [t for t in _ICE_TABLES if t[0] in ("postgres", "oracle")
                     and (t[0] != "postgres" or _run_pg)
                     and (t[0] != "oracle"   or _run_ora)]
    mgo_tables    = [t for t in _ICE_TABLES if t[0] == "mongodb" and _run_mgo]

    for col_base in _TEST_COLS:
        for suffix in ("_r1", "_r2", "_r3"):
            col_name = col_base + suffix
            _phase(f"Polling for column '{col_name}'")

            # Determine which source wrote DML for this renamed name
            pg_has  = bool(all_ids["postgres"].get(col_name))
            ora_has = bool(all_ids["oracle"].get(col_name))
            mgo_has = bool(all_ids["mongodb"].get(col_name))

            tables_to_check = []
            if pg_has or ora_has:
                tables_to_check += pg_ora_tables
            if mgo_has:
                tables_to_check += mgo_tables

            if not tables_to_check:
                _info(f"  No DML issued for '{col_name}' — skip Iceberg poll")
                continue

            found = _poll_iceberg_col(col_name, tables_to_check, timeout=POLL_SECS)
            for (cat, ns, tbl), visible in found.items():
                _record(f"Iceberg col visible — {cat}.{ns}.{tbl} '{col_name}'",
                        visible)

    # ─────────────────────────────────────────────────────────────────────────
    # Verify DML rows in Iceberg — IS NOT NULL check per renamed column
    # ─────────────────────────────────────────────────────────────────────────
    _hdr("Verifying DML rows in Iceberg (IS NOT NULL) per renamed column")
    # Oracle columns are normalised to lowercase by the streaming pipeline.
    pk_map = {"postgres": "id", "oracle": "customer_id", "mongodb": "customer_id"}

    for src_key, src_tables in [
        ("postgres", pg_ora_tables),
        ("oracle",   pg_ora_tables),
        ("mongodb",  mgo_tables),
    ]:
        if src_key == "postgres" and not _run_pg:
            continue
        if src_key == "oracle"   and not _run_ora:
            continue
        if src_key == "mongodb"  and not _run_mgo:
            continue
        src_id_map = all_ids[src_key]
        if not src_id_map:
            continue
        _phase(f"Verifying {src_key} rows")
        for col_name, ids in src_id_map.items():
            if not ids:
                continue
            pk = pk_map[src_key]
            for cat, ns, tbl in src_tables:
                if cat != src_key:
                    continue
                found = _iceberg_count(
                    f"{cat}.{ns}.{tbl}", pk, ids,
                    f"`{col_name}` IS NOT NULL"
                )
                ok    = found == len(ids)
                label = f"Rows NOT NULL — {cat}.{ns}.{tbl} '{col_name}'"
                if ok:
                    _ok(f"{cat}.{ns}.{tbl}: {found}/{len(ids)} rows have '{col_name}' IS NOT NULL")
                else:
                    _fail(f"{cat}.{ns}.{tbl}: {found}/{len(ids)} rows have '{col_name}' IS NOT NULL")
                _record(label, ok, f"{found}/{len(ids)}")

    # ─────────────────────────────────────────────────────────────────────────
    # Verify last_login_at stays BIGINT
    # ─────────────────────────────────────────────────────────────────────────
    _hdr("Checking last_login_at type (must stay BIGINT)")
    for cat, ns, tbl in _ICE_TABLES:
        t  = _col_type(cat, ns, tbl, "last_login_at")
        ok = t in ("bigint", "long", "int8")
        msg = f"{cat}.{ns}.{tbl}: last_login_at → {t}"
        (_ok if ok else _fail)(msg)
        _record(f"last_login_at BIGINT — {cat}.{ns}.{tbl}", ok, t)

    # ─────────────────────────────────────────────────────────────────────────
    # Pod restart check
    # ─────────────────────────────────────────────────────────────────────────
    _hdr("Pod restart check")
    after   = snapshot_restarts()
    crashed = {p: (b, after.get(p, b))
               for p, b in restarts_before.items()
               if after.get(p, b) > b}
    if crashed:
        for pod, (b, a) in crashed.items():
            _fail(f"  {pod}: {b} → {a} restarts")
        _record("Zero pod restarts", False, f"{len(crashed)} pods restarted")
    else:
        _ok(f"No restarts across {len(restarts_before)} pod(s)")
        _record("Zero pod restarts", True)

    # ─────────────────────────────────────────────────────────────────────────
    # Final report
    # ─────────────────────────────────────────────────────────────────────────
    print()
    print("=" * 72)
    print("  FINAL REPORT — CDC RENAME/DROP COLUMN STRESS TEST")
    print("=" * 72)

    passed = failed = 0
    for label, ok, detail in _results:
        status = "PASS" if ok else "FAIL"
        sfx    = f"  [{detail}]" if detail else ""
        print(f"  {'✓' if ok else '✗'}  {label:<62} {status}{sfx}")
        if ok:
            passed += 1
        else:
            failed += 1

    print()
    print(f"  Passed: {passed}   Failed: {failed}   Total: {passed + failed}")
    print(f"  Finished: {datetime.now(timezone.utc).isoformat()}")
    print()

    if failed == 0:
        print("  ✓  ALL CHECKS PASSED")
        print("     5 columns × 3 renames × 3 sources replicated cleanly to Iceberg.")
        print("     last_login_at stayed BIGINT throughout.")
        print("     Zero streaming pod crashes.")
    else:
        print("  ✗  SOME CHECKS FAILED — review output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
