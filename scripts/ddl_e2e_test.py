#!/usr/bin/env python3
"""
scripts/ddl_e2e_test.py
=======================
DDL stress-test for the CDC → Iceberg pipeline.

Cycle counts
------------
  Oracle    : 25 ADD/DROP cycles   — DML (5 rows) on cycles 5, 10, 15, 20, 25
  PostgreSQL: 25 ADD/DROP cycles   — DML (5 rows) on cycles 5, 10, 15, 20, 25
  MongoDB   : 10 ADD/DROP cycles   — DML (5 docs) on cycles 5, 10

Each cycle:
  (a) DROP  loyalty_tier_v2  (source DDL)
  (b) DML   5 rows WITHOUT the column   [only on DML cycles]
  (c) ADD   loyalty_tier_v2 SMALLINT
  (d) DML   5 rows WITH the column set  [only on DML cycles]

Final verification (after last cycle)
  • loyalty_tier_v2 visible in all 7 Iceberg tables
  • Post-DML rows have loyalty_tier_v2 IS NOT NULL in Iceberg
  • last_login_at stays BIGINT throughout (schema-cache type fix)
  • Zero streaming pod restarts

Execution
---------
The script is pushed into the postgres-standard streaming pod (which has
cluster-internal DB access and BAO credentials via K8s SA JWT) and exec'd
there.  All DB credentials come from OpenBao — nothing hardcoded.

Usage (from workstation)
------------------------
  python3 scripts/ddl_e2e_test.py

Environment variables
---------------------
  POLL_SECS   max seconds to wait for Iceberg column after final ADD (default 180)
  DML_ROWS    rows per DML batch (default 5)
  PG_CYCLES   PostgreSQL ADD/DROP cycles (default 25)
  ORA_CYCLES  Oracle     ADD/DROP cycles (default 25)
  MGO_CYCLES  MongoDB    ADD/DROP cycles (default 10)
  DML_EVERY   run DML every N-th cycle   (default 5)
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

# ─────────────────────────────────────────────────────────────────────────────
# OpenBao helpers
# ─────────────────────────────────────────────────────────────────────────────
# OpenBao is only reachable from the workstation via NodePort 30820.
# The cluster-internal address (openbao.prod.svc.cluster.local) works inside pods
# but not from the workstation. Default to the NodePort so the script works without
# any env override when run from the workstation with kubectl in PATH.
_BAO_ADDR  = os.environ.get("ADDR",     "http://192.168.1.50:30820")
_BAO_ROLE  = os.environ.get("BAO_ROLE", "platform-secrets-read")
_BAO_TOKEN: str | None = None


def _bao_token() -> str:
    global _BAO_TOKEN
    if _BAO_TOKEN:
        return _BAO_TOKEN
    # Prefer explicit token (root token, dev override, or CI secret)
    direct = os.environ.get("BAO_TOKEN")
    if direct:
        _BAO_TOKEN = direct
        return _BAO_TOKEN
    # Auto-fetch root token from the K8s secret (requires kubectl in PATH)
    try:
        r = subprocess.run(
            ["kubectl", "-n", "prod", "get", "secret", "openbao-unseal-keys",
             "-o", "jsonpath={.data.root-token}"],
            capture_output=True, text=True, check=True, timeout=10,
        )
        import base64
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
# DSN resolution  (fetched once, cached for the run)
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
    h, p, d = pg.get("host", "postgresql.prod.svc.cluster.local"), pg.get("port", "5432"), pg.get("database", "cache_testing")
    PG_DSN  = os.environ.get("PG_DSN") or f"postgresql://{pg['user']}:{pg['password']}@{h}:{p}/{d}"
    mh, mp  = mgo.get("host", "mongodb.prod.svc.cluster.local"), mgo.get("port", "27017")
    mauth   = mgo.get("auth_source", "admin")
    MGO_URI = os.environ.get("MGO_URI") or (
        f"mongodb://{mgo['user']}:{mgo['password']}@{mh}:{mp}/?authSource={mauth}&replicaSet=rs0"
    )
    _dsns_resolved = True


# Oracle DSN — fetched lazily from OpenBao (app_user / app_password)
_ora_dsn_cache: str | None = None


def _ora_dsn() -> str:
    global _ora_dsn_cache
    if _ora_dsn_cache:
        return _ora_dsn_cache
    ora = _bao_secret("secret/data/platform/oracle")
    h, p, s = ora.get("host", "oracle-xe.prod.svc.cluster.local"), ora.get("port", "1521"), ora.get("service", "XEPDB1")
    _ora_dsn_cache = f"{ora['app_user']}/{ora['app_password']}@{h}:{p}/{s}"
    return _ora_dsn_cache


# ─────────────────────────────────────────────────────────────────────────────
# Stress-test parameters
# ─────────────────────────────────────────────────────────────────────────────
POLL_SECS  = int(os.environ.get("POLL_SECS",  "180"))
DML_ROWS   = int(os.environ.get("DML_ROWS",   "5"))
PG_CYCLES  = int(os.environ.get("PG_CYCLES",  "25"))
ORA_CYCLES = int(os.environ.get("ORA_CYCLES", "25"))
MGO_CYCLES = int(os.environ.get("MGO_CYCLES", "10"))
DML_EVERY  = int(os.environ.get("DML_EVERY",  "5"))

NEW_COL  = "loyalty_tier_v2"
NEW_TYPE = "SMALLINT"

# ID space — epoch-derived so every test run gets a fresh PK block.
# Prevents Oracle no-op update (MERGE on existing row with same values writes no
# redo → LogMiner sees nothing → Debezium emits nothing → Iceberg misses the row).
_RUN_EPOCH = int(time.time())
_PG_BASE   = _RUN_EPOCH * 10 + 0
_ORA_BASE  = _RUN_EPOCH * 10 + 1
_MGO_BASE  = _RUN_EPOCH * 10 + 2

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
def _hdr(msg: str)  -> None: print(f"\n{'─'*68}\n  {msg}\n{'─'*68}", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# kubectl / pod helpers
# ─────────────────────────────────────────────────────────────────────────────
def _kubectl(*args: str, check: bool = True) -> str:
    r = subprocess.run(["kubectl", "-n", "prod", *args], capture_output=True, text=True, check=check)
    return r.stdout.strip()


def _get_db_pod() -> str:
    """Resolve the live postgres-standard streaming pod name on every call.

    Never cached — the pod name changes after every rollout restart and a
    stale cache would silently direct kubectl exec at a terminated pod.
    The label selector is stable across restarts; the pod name is not.
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
    """Run Python inside the postgres-standard streaming pod."""
    r = subprocess.run(
        ["kubectl", "-n", "prod", "exec", _get_db_pod(), "--", "python3", "-c", code],
        capture_output=True, text=True, timeout=timeout,
    )
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip() or r.stdout.strip())
    return r.stdout.strip()


# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL helpers  (DML via _pod_exec / DDL via psql in postgresql-0)
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


def pg_drop() -> None:
    _pg_ddl(f"ALTER TABLE customers DROP COLUMN IF EXISTS {NEW_COL}")


def pg_add() -> None:
    _pg_ddl(f"ALTER TABLE customers ADD COLUMN IF NOT EXISTS {NEW_COL} {NEW_TYPE}")


def pg_dml(base_id: int) -> list[int]:
    """Insert rows WITH loyalty_tier_v2 set. PG schema: id,name,email,tier,loyalty_tier_v2."""
    _resolve_dsns()
    ids  = list(range(base_id, base_id + DML_ROWS))
    rows = [(cid, f"StressPG{cid}", f"stress{cid}@pg.test", "gold", i + 1)
            for i, cid in enumerate(ids)]
    _pod_exec(f"""
import psycopg2
conn = psycopg2.connect({PG_DSN!r})
conn.autocommit = True
cur = conn.cursor()
for row in {rows!r}:
    cur.execute(
        "INSERT INTO customers(id,name,email,tier,{NEW_COL}) "
        "VALUES(%s,%s,%s,%s,%s) "
        "ON CONFLICT(id) DO UPDATE SET {NEW_COL}=EXCLUDED.{NEW_COL},tier=EXCLUDED.tier",
        row)
conn.close(); print("ok")
""")
    return ids


def pg_dml_no_col(base_id: int) -> list[int]:
    """Insert rows WITHOUT loyalty_tier_v2. PG schema: id,name,email,tier."""
    _resolve_dsns()
    ids  = list(range(base_id, base_id + DML_ROWS))
    rows = [(cid, f"StressPGpre{cid}", f"stress{cid}@pg.test", "silver")
            for i, cid in enumerate(ids)]
    _pod_exec(f"""
import psycopg2
conn = psycopg2.connect({PG_DSN!r})
conn.autocommit = True
cur = conn.cursor()
for row in {rows!r}:
    cur.execute(
        "INSERT INTO customers(id,name,email,tier) VALUES(%s,%s,%s,%s) "
        "ON CONFLICT(id) DO UPDATE SET tier=EXCLUDED.tier",
        row)
conn.close(); print("ok")
""")
    return ids


# ─────────────────────────────────────────────────────────────────────────────
# Oracle helpers  (all via sqlplus inside oracle-xe pod)
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


def ora_drop() -> None:
    _ora_run(f"""
DECLARE v NUMBER;
BEGIN
  SELECT COUNT(*) INTO v FROM all_tab_columns
  WHERE owner='CACHE_TESTING' AND table_name='CUSTOMERS'
    AND column_name='{NEW_COL.upper()}';
  IF v > 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE CUSTOMERS DROP COLUMN {NEW_COL.upper()}';
  END IF;
END;
/""")


def ora_add() -> None:
    _ora_run(f"""
DECLARE v NUMBER;
BEGIN
  SELECT COUNT(*) INTO v FROM all_tab_columns
  WHERE owner='CACHE_TESTING' AND table_name='CUSTOMERS'
    AND column_name='{NEW_COL.upper()}';
  IF v = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE CUSTOMERS ADD ({NEW_COL.upper()} {NEW_TYPE})';
  END IF;
END;
/""")


# Oracle CHK_CUST_TIER: tier IN ('STANDARD','SILVER','GOLD','PLATINUM') — UPPERCASE only
# Oracle CHK_CUST_ACTIVE: is_active IN ('Y','N')
#
# DELETE+INSERT strategy (permanent fix — do not revert to MERGE):
# MERGE WHEN MATCHED UPDATE with identical values = Oracle no-op = zero redo entry
# = LogMiner sees nothing = Debezium emits nothing = Iceberg never gets the row.
# DELETE+INSERT unconditionally writes redo entries for both statements.
def ora_dml(base_id: int) -> list[int]:
    """DELETE+INSERT rows WITH loyalty_tier_v2."""
    ids = list(range(base_id, base_id + DML_ROWS))
    stmts = "\n".join(
        f"DELETE FROM CUSTOMERS WHERE CUSTOMER_ID={cid};\n"
        f"INSERT INTO CUSTOMERS(CUSTOMER_ID,FIRST_NAME,LAST_NAME,EMAIL,"
        f"CITY,COUNTRY_CODE,TIER,CREDIT_LIMIT,IS_ACTIVE,{NEW_COL.upper()}) "
        f"VALUES({cid},'StressORA{cid}','Stress','s{cid}@ora.test',"
        f"'TestCity','US','GOLD',5000,'Y',{i+1});"
        for i, cid in enumerate(ids)
    )
    _ora_run(stmts + "\nCOMMIT;")
    return ids


def ora_dml_no_col(base_id: int) -> list[int]:
    """DELETE+INSERT rows WITHOUT loyalty_tier_v2."""
    ids = list(range(base_id, base_id + DML_ROWS))
    stmts = "\n".join(
        f"DELETE FROM CUSTOMERS WHERE CUSTOMER_ID={cid};\n"
        f"INSERT INTO CUSTOMERS(CUSTOMER_ID,FIRST_NAME,LAST_NAME,EMAIL,"
        f"CITY,COUNTRY_CODE,TIER,CREDIT_LIMIT,IS_ACTIVE) "
        f"VALUES({cid},'StressORApre{cid}','Stress','s{cid}@ora.test',"
        f"'TestCity','US','STANDARD',3000,'Y');"
        for i, cid in enumerate(ids)
    )
    _ora_run(stmts + "\nCOMMIT;")
    return ids


# ─────────────────────────────────────────────────────────────────────────────
# MongoDB helpers  (via _pod_exec using MGO_URI from OpenBao)
# ─────────────────────────────────────────────────────────────────────────────
def mgo_drop() -> None:
    _resolve_dsns()
    _pod_exec(f"""
from pymongo import MongoClient
c = MongoClient({MGO_URI!r})
c["cache_testing"].customers.update_many(
    {{"{NEW_COL}":{{"$exists":True}}}},
    {{"$unset":{{"{NEW_COL}":""}}}})
c.close(); print("ok")
""")


def mgo_add() -> None:
    _resolve_dsns()
    _pod_exec(f"""
from pymongo import MongoClient
c = MongoClient({MGO_URI!r})
c["cache_testing"].customers.update_one(
    {{"{NEW_COL}":{{"$exists":False}}}},
    {{"$set":{{"{NEW_COL}":None}}}})
c.close(); print("ok")
""")


def mgo_dml(base_id: int) -> list[int]:
    _resolve_dsns()
    ids  = list(range(base_id, base_id + DML_ROWS))
    docs = [{"customer_id": cid, "first_name": f"Stress{i}", "last_name": "MGO",
             "email": f"s{cid}@mgo.test", "city": "TestCity",
             "country_code": "US", "tier": "gold", "credit_limit": 5000.0,
             "is_active": True, NEW_COL: i + 1}
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


def mgo_dml_no_col(base_id: int) -> list[int]:
    _resolve_dsns()
    ids  = list(range(base_id, base_id + DML_ROWS))
    docs = [{"customer_id": cid, "first_name": f"Stress{i}", "last_name": "MGOpre",
             "email": f"s{cid}@mgo.test", "city": "TestCity",
             "country_code": "US", "tier": "silver", "credit_limit": 3000.0,
             "is_active": True}
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
# Spark SQL / Iceberg verification  (runs inside the streaming pod)
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


_ICE_TABLES = [
    ("postgres", "cache_testing", "customers"),
    ("postgres", "cache_testing", "customers_sd"),
    ("oracle",   "cache_testing", "customers"),
    ("oracle",   "cache_testing", "customers_sd"),
    ("mongodb",  "cache_testing", "customers"),
    ("mongodb",  "cache_testing", "customers_sd"),
    ("mongodb",  "cache_testing", "customers_hist"),
]


def _iceberg_has_col(cat: str, ns: str, tbl: str) -> bool:
    out = _spark_sql(f"DESCRIBE TABLE `{cat}`.`{ns}`.`{tbl}`")
    return NEW_COL.lower() in out.lower()


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
# Stress-test loop runner
# ─────────────────────────────────────────────────────────────────────────────
def run_source_cycles(
    source: str,
    cycles: int,
    dml_every: int,
    drop_fn,
    add_fn,
    dml_pre_fn,
    dml_post_fn,
    id_base: int,
) -> dict:
    """
    Run ADD/DROP cycles for one source.
    Returns summary dict with counters and lists of IDs inserted with the column set.
    """
    stats = {
        "drop_ok": 0, "drop_fail": 0,
        "add_ok":  0, "add_fail":  0,
        "dml_pre_ok": 0, "dml_post_ok": 0,
        "post_ids": [],   # IDs inserted WITH the new column (for Iceberg verification)
        "pre_ids":  [],   # IDs inserted WITHOUT the new column
    }
    # ID counter: advance by DML_ROWS * 2 per DML cycle so ranges never collide
    id_counter = id_base

    for cycle in range(1, cycles + 1):
        do_dml = (cycle % dml_every == 0)
        _info(f"{source} cycle {cycle:>2}/{cycles}  {'[DML]' if do_dml else '     '}")

        # (a) DROP
        try:
            drop_fn()
            stats["drop_ok"] += 1
        except Exception as e:
            stats["drop_fail"] += 1
            _fail(f"{source} cycle {cycle} DROP failed: {e}")
            continue   # can't do DML or ADD reliably if drop failed

        # brief settle
        time.sleep(2)

        # (b) DML without column
        if do_dml:
            try:
                ids = dml_pre_fn(id_counter)
                stats["pre_ids"].extend(ids)
                stats["dml_pre_ok"] += 1
                id_counter += DML_ROWS
            except Exception as e:
                _fail(f"{source} cycle {cycle} pre-DDL DML failed: {e}")

        # (c) ADD
        try:
            add_fn()
            stats["add_ok"] += 1
        except Exception as e:
            stats["add_fail"] += 1
            _fail(f"{source} cycle {cycle} ADD failed: {e}")
            continue

        # brief settle
        time.sleep(2)

        # (d) DML with new column
        if do_dml:
            try:
                ids = dml_post_fn(id_counter)
                stats["post_ids"].extend(ids)
                stats["dml_post_ok"] += 1
                id_counter += DML_ROWS
            except Exception as e:
                _fail(f"{source} cycle {cycle} post-DDL DML failed: {e}")

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    print()
    print("=" * 72)
    print(f"  CDC DDL STRESS TEST  —  {NEW_COL} {NEW_TYPE}")
    print(f"  PG={PG_CYCLES} cycles  ORA={ORA_CYCLES} cycles  MGO={MGO_CYCLES} cycles")
    print(f"  DML every {DML_EVERY}th cycle  ({DML_ROWS} rows/batch)")
    print(f"  Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 72)

    # Pre-flight: resolve credentials (fails fast with a clear message)
    _info("Resolving OpenBao credentials …")
    _resolve_dsns()
    _ora_dsn()   # also warm Oracle cache
    _ok("Credentials resolved")

    # Snapshot pod restarts before test
    restarts_before = snapshot_restarts()
    _info(f"Tracking {len(restarts_before)} streaming pod(s)")

    # Pre-fetch Spark catalog conf (one kubectl exec, cached for the whole run)
    _info("Pre-fetching Spark catalog conf from OpenBao …")
    _get_spark_conf_flags()
    _ok("Spark catalog conf ready")

    # ── PostgreSQL stress loop ────────────────────────────────────────────────
    _hdr(f"PostgreSQL  —  {PG_CYCLES} ADD/DROP cycles  (DML every {DML_EVERY}th)")
    pg_stats = run_source_cycles(
        "PG", PG_CYCLES, DML_EVERY,
        pg_drop, pg_add, pg_dml_no_col, pg_dml,
        _PG_BASE,
    )
    _ok(f"PG done: drop={pg_stats['drop_ok']} add={pg_stats['add_ok']} "
        f"dml_pre={pg_stats['dml_pre_ok']} dml_post={pg_stats['dml_post_ok']}")
    _record("PG cycles — all DROPs succeeded",  pg_stats["drop_fail"] == 0,
            f"{pg_stats['drop_ok']}/{PG_CYCLES}")
    _record("PG cycles — all ADDs succeeded",   pg_stats["add_fail"] == 0,
            f"{pg_stats['add_ok']}/{PG_CYCLES}")

    # ── Oracle stress loop ────────────────────────────────────────────────────
    _hdr(f"Oracle  —  {ORA_CYCLES} ADD/DROP cycles  (DML every {DML_EVERY}th)")
    ora_stats = run_source_cycles(
        "ORA", ORA_CYCLES, DML_EVERY,
        ora_drop, ora_add, ora_dml_no_col, ora_dml,
        _ORA_BASE,
    )
    _ok(f"ORA done: drop={ora_stats['drop_ok']} add={ora_stats['add_ok']} "
        f"dml_pre={ora_stats['dml_pre_ok']} dml_post={ora_stats['dml_post_ok']}")
    _record("ORA cycles — all DROPs succeeded", ora_stats["drop_fail"] == 0,
            f"{ora_stats['drop_ok']}/{ORA_CYCLES}")
    _record("ORA cycles — all ADDs succeeded",  ora_stats["add_fail"] == 0,
            f"{ora_stats['add_ok']}/{ORA_CYCLES}")

    # ── MongoDB stress loop ───────────────────────────────────────────────────
    _hdr(f"MongoDB  —  {MGO_CYCLES} ADD/DROP cycles  (DML every {DML_EVERY}th)")
    mgo_stats = run_source_cycles(
        "MGO", MGO_CYCLES, DML_EVERY,
        mgo_drop, mgo_add, mgo_dml_no_col, mgo_dml,
        _MGO_BASE,
    )
    _ok(f"MGO done: drop={mgo_stats['drop_ok']} add={mgo_stats['add_ok']} "
        f"dml_pre={mgo_stats['dml_pre_ok']} dml_post={mgo_stats['dml_post_ok']}")
    _record("MGO cycles — all DROPs succeeded", mgo_stats["drop_fail"] == 0,
            f"{mgo_stats['drop_ok']}/{MGO_CYCLES}")
    _record("MGO cycles — all ADDs succeeded",  mgo_stats["add_fail"] == 0,
            f"{mgo_stats['add_ok']}/{MGO_CYCLES}")

    # ── Wait for pipeline to flush all DML rows into Iceberg ─────────────────
    _hdr("Waiting for Kafka → Iceberg pipeline to flush …")
    _info("Sleeping 45 s for micro-batches to commit …")
    time.sleep(45)

    # ── Poll until loyalty_tier_v2 is visible in all Iceberg tables ───────────
    _hdr(f"Polling Iceberg for '{NEW_COL}'  (timeout {POLL_SECS}s)")
    deadline  = time.time() + POLL_SECS
    remaining = {(cat, ns, tbl) for cat, ns, tbl in _ICE_TABLES}
    while remaining and time.time() < deadline:
        still = set()
        for entry in list(remaining):
            cat, ns, tbl = entry
            if _iceberg_has_col(cat, ns, tbl):
                _ok(f"Iceberg {cat}.{ns}.{tbl}: '{NEW_COL}' visible")
                _record(f"Iceberg col visible — {cat}.{ns}.{tbl}", True)
            else:
                still.add(entry)
        remaining = still
        if remaining:
            _info(f"  {len(remaining)} table(s) still missing column — retrying in 15 s …")
            time.sleep(15)
    for cat, ns, tbl in remaining:
        _fail(f"Iceberg {cat}.{ns}.{tbl}: '{NEW_COL}' NOT visible after {POLL_SECS}s")
        _record(f"Iceberg col visible — {cat}.{ns}.{tbl}", False)

    # ── Verify post-DML rows have loyalty_tier_v2 IS NOT NULL ────────────────
    _hdr("Verifying post-DDL DML rows in Iceberg (IS NOT NULL)")
    pk_map = {
        "postgres": "id",
        "oracle":   "CUSTOMER_ID",
        "mongodb":  "customer_id",
    }
    post_ids_by_src = {
        "postgres": pg_stats["post_ids"],
        "oracle":   ora_stats["post_ids"],
        "mongodb":  mgo_stats["post_ids"],
    }
    for cat, ns, tbl in _ICE_TABLES:
        ids = post_ids_by_src.get(cat, [])
        if not ids:
            _info(f"  {cat}.{ns}.{tbl}: no post-DML IDs (no DML cycles ran for this source)")
            continue
        pk    = pk_map[cat]
        found = _iceberg_count(f"{cat}.{ns}.{tbl}", pk, ids, f"`{NEW_COL}` IS NOT NULL")
        ok    = found == len(ids)
        label = f"Post-DML rows IS NOT NULL — {cat}.{ns}.{tbl}"
        if ok:
            _ok(f"{cat}.{ns}.{tbl}: {found}/{len(ids)} rows have {NEW_COL} IS NOT NULL")
        else:
            _fail(f"{cat}.{ns}.{tbl}: only {found}/{len(ids)} rows have {NEW_COL} IS NOT NULL")
        _record(label, ok, f"{found}/{len(ids)}")

    # ── Verify pre-DML rows have loyalty_tier_v2 IS NULL ─────────────────────
    _hdr("Verifying pre-DDL DML rows in Iceberg (IS NULL)")
    pre_ids_by_src = {
        "postgres": pg_stats["pre_ids"],
        "oracle":   ora_stats["pre_ids"],
        "mongodb":  mgo_stats["pre_ids"],
    }
    for cat, ns, tbl in _ICE_TABLES:
        ids = pre_ids_by_src.get(cat, [])
        if not ids:
            continue
        pk    = pk_map[cat]
        found = _iceberg_count(f"{cat}.{ns}.{tbl}", pk, ids, f"`{NEW_COL}` IS NULL")
        ok    = found == len(ids)
        label = f"Pre-DDL rows IS NULL — {cat}.{ns}.{tbl}"
        if ok:
            _ok(f"{cat}.{ns}.{tbl}: {found}/{len(ids)} pre-DDL rows have {NEW_COL} IS NULL")
        else:
            _fail(f"{cat}.{ns}.{tbl}: only {found}/{len(ids)} pre-DDL rows have {NEW_COL} IS NULL")
        _record(label, ok, f"{found}/{len(ids)}")

    # ── last_login_at must stay BIGINT ────────────────────────────────────────
    _hdr("Checking last_login_at type (must stay BIGINT)")
    for cat, ns, tbl in _ICE_TABLES:
        t = _col_type(cat, ns, tbl, "last_login_at")
        ok = t in ("bigint", "long", "int8")
        msg = f"{cat}.{ns}.{tbl}: last_login_at → {t}"
        (_ok if ok else _fail)(msg)
        _record(f"last_login_at BIGINT — {cat}.{ns}.{tbl}", ok, t)

    # ── Pod restart check ─────────────────────────────────────────────────────
    _hdr("Pod restart check")
    after   = snapshot_restarts()
    crashed = {p: (before, after.get(p, before))
               for p, before in restarts_before.items()
               if after.get(p, before) > before}
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
    print("  FINAL REPORT")
    print("=" * 72)

    passed = failed = 0
    for label, ok, detail in _results:
        status = "PASS" if ok else "FAIL"
        sfx    = f"  [{detail}]" if detail else ""
        print(f"  {'✓' if ok else '✗'}  {label:<58} {status}{sfx}")
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
        print(f"     PG {PG_CYCLES} cycles / ORA {ORA_CYCLES} cycles / MGO {MGO_CYCLES} cycles")
        print("     Schema-cache type fix confirmed: last_login_at stayed BIGINT.")
        print("     Zero streaming pod crashes throughout the stress test.")
    else:
        print("  ✗  SOME CHECKS FAILED — review output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
