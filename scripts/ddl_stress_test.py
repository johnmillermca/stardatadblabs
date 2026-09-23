#!/usr/bin/env python3
"""
scripts/ddl_stress_test.py
==========================
DDL stress test — exercises the CDC pipeline with repeated ADD/DROP cycles
across all three sources while monitoring Iceberg replication fidelity.

Schedule
--------
  Oracle    : 25 ADD/DROP cycles on loyalty_tier_v2
  PostgreSQL: 25 ADD/DROP cycles on loyalty_tier_v2
  MongoDB   : 10 ADD/DROP cycles on loyalty_tier_v2

Every 5th ADD cycle across each source, insert 5 DML rows that explicitly
set loyalty_tier_v2 so we can verify the column landed in Iceberg with a
non-NULL value.

At the end, query Iceberg for each source and print a replication statistics
table:
  • Column currently present in Iceberg schema
  • Total DML rows inserted during stress test
  • DML rows found in Iceberg with loyalty_tier_v2 IS NOT NULL
  • DML rows found in Iceberg with loyalty_tier_v2 IS NULL (pre-add batches)
  • Pod restart count delta
  • Errors encountered

Usage
-----
  python3 scripts/ddl_stress_test.py

Environment variables
---------------------
  CYCLE_DELAY_S  seconds to pause between each ADD/DROP cycle (default 6)
  DML_ROWS       rows inserted per DML cycle (default 5)
  POLL_SECS      max seconds to poll Iceberg for column visibility (default 120)
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
PG_HOST    = "postgresql.prod.svc.cluster.local"
PG_PORT    = "5432"
PG_DB      = "cache_testing"
PG_USER    = "postgres"
PG_PASS    = "mE8GKcHiFTaoXCFgRk1vYcXR"

ORA_HOST   = "oracle-xe.prod.svc.cluster.local"
ORA_PORT   = "1521"
ORA_SVC    = "XEPDB1"
ORA_USER   = "CACHE_TESTING"
ORA_PASS   = "CacheTesting2024"

MGO_URI    = "mongodb://root:oEtCgw554IP3ua0SrJCTsWYM@mongodb.prod.svc.cluster.local:27017/?authSource=admin&replicaSet=rs0"

COL        = "loyalty_tier_v2"
CYCLES_PG  = int(os.environ.get("CYCLES_PG",  "25"))
CYCLES_ORA = int(os.environ.get("CYCLES_ORA", "25"))
CYCLES_MGO = int(os.environ.get("CYCLES_MGO", "10"))
DML_EVERY  = 5          # do DML on every Nth ADD cycle
DML_ROWS   = int(os.environ.get("DML_ROWS",   "5"))
CYCLE_DELAY= float(os.environ.get("CYCLE_DELAY_S", "6"))
POLL_SECS  = int(os.environ.get("POLL_SECS",  "120"))

# ID bases — far from existing data
_PG_BASE   = 3_000_001
_ORA_BASE  = 3_100_001
_MGO_BASE  = 3_200_001

# Iceberg tables (catalog.namespace.table)
_ICE_TABLES = [
    ("postgres", "cache_testing", "customers"),
    ("oracle",   "cache_testing", "customers"),
    ("mongodb",  "cache_testing", "customers"),
]

# ── Pods ──────────────────────────────────────────────────────────────────────
_DB_POD    = None   # set at startup — postgres standard pod
_ORA_POD   = "oracle-xe-799f8d67dd-vjtq7"
_MGO_POD   = "mongodb-0"
_MGO_CTR   = "mongodb"

# ── Stats ─────────────────────────────────────────────────────────────────────
_stats: dict[str, dict] = {
    "postgres": {"adds": 0, "drops": 0, "dml_cycles": 0, "dml_rows": 0,
                 "errors": 0, "dml_ids": []},
    "oracle":   {"adds": 0, "drops": 0, "dml_cycles": 0, "dml_rows": 0,
                 "errors": 0, "dml_ids": []},
    "mongodb":  {"adds": 0, "drops": 0, "dml_cycles": 0, "dml_rows": 0,
                 "errors": 0, "dml_ids": []},
}
_restarts_before: dict[str, int] = {}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")

def _ok(msg: str)   -> None: print(f"  [{_ts()}] ✓  {msg}")
def _fail(msg: str) -> None: print(f"  [{_ts()}] ✗  {msg}", file=sys.stderr)
def _info(msg: str) -> None: print(f"  [{_ts()}]    {msg}")


def _kubectl(*args, check=True) -> str:
    result = subprocess.run(
        ["kubectl", "-n", "prod", *args],
        capture_output=True, text=True, check=check,
    )
    return result.stdout.strip()


def _pg(sql: str) -> str:
    """Run SQL directly in the postgres pod."""
    result = subprocess.run(
        ["kubectl", "-n", "prod", "exec", "postgresql-0", "--",
         "psql", "-U", "postgres", "-d", "cache_testing",
         "-t", "-c", sql],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()


def _ora(sql: str) -> str:
    """Run SQL in the Oracle pod via sqlplus."""
    script = f"{sql}\nEXIT;\n"
    result = subprocess.run(
        ["kubectl", "-n", "prod", "exec", _ORA_POD, "--",
         "bash", "-c",
         f"echo '{sql}\nEXIT;' | sqlplus -s {ORA_USER}/{ORA_PASS}@localhost:{ORA_PORT}/{ORA_SVC}"],
        capture_output=True, text=True, timeout=30,
    )
    return result.stdout.strip()


def _ora_block(block: str) -> str:
    """Run a multi-statement Oracle block."""
    result = subprocess.run(
        ["kubectl", "-n", "prod", "exec", _ORA_POD, "--",
         "bash", "-c",
         f"sqlplus -s {ORA_USER}/{ORA_PASS}@localhost:{ORA_PORT}/{ORA_SVC} <<'SQLEOF'\n{block}\nEXIT;\nSQLEOF"],
        capture_output=True, text=True, timeout=60,
    )
    return result.stdout.strip()


def _mgo(js: str) -> str:
    """Run mongosh JS in the mongodb pod."""
    result = subprocess.run(
        ["kubectl", "-n", "prod", "exec", _MGO_POD, "-c", _MGO_CTR, "--",
         "mongosh",
         f"mongodb://root:oEtCgw554IP3ua0SrJCTsWYM@localhost:27017/cache_testing?authSource=admin",
         "--quiet", "--eval", js],
        capture_output=True, text=True, timeout=30,
    )
    return result.stdout.strip()


# Polaris / S3 creds — fetched from OpenBao at startup (never hardcoded)
_POLARIS_URL   = ""
_SPARK_SVC_ID  = ""
_SPARK_SVC_SEC = ""
_S3_KEY        = ""
_S3_SECRET     = ""
_S3_ENDPOINT   = ""

# Catalog → warehouse name
_CATALOG_MAP = {
    "postgres": ("pg_lakehouse",  "iceberg/pg_lakehouse"),
    "oracle":   ("ora_lakehouse", "iceberg/ora_lakehouse"),
    "mongodb":  ("mgo_lakehouse", "iceberg/mgo_lakehouse"),
}


def _load_polaris_creds() -> None:
    """Fetch Polaris + S3 credentials from OpenBao via the streaming pod."""
    global _POLARIS_URL, _SPARK_SVC_ID, _SPARK_SVC_SEC, _S3_KEY, _S3_SECRET, _S3_ENDPOINT
    pod = _kubectl(
        "get", "pods",
        "-l", "app=kafka-to-iceberg,pipeline.write-mode=standard,pipeline.source=postgres",
        "-o", "jsonpath={.items[0].metadata.name}",
    )
    out = subprocess.run(
        ["kubectl", "-n", "prod", "exec", pod, "--", "python3", "-c", """
import urllib.request, json
addr = 'http://openbao.prod.svc.cluster.local:8200'
sa   = open('/var/run/secrets/kubernetes.io/serviceaccount/token').read()
tok  = json.loads(urllib.request.urlopen(urllib.request.Request(
    addr+'/v1/auth/kubernetes/login',
    data=json.dumps({'role':'platform-secrets-read','jwt':sa}).encode(),
    headers={'Content-Type':'application/json'})).read())['auth']['client_token']
pol = json.loads(urllib.request.urlopen(urllib.request.Request(
    addr+'/v1/secret/data/platform/polaris', headers={'X-Vault-Token':tok})).read())['data']['data']
s3  = json.loads(urllib.request.urlopen(urllib.request.Request(
    addr+'/v1/secret/data/platform/s3',      headers={'X-Vault-Token':tok})).read())['data']['data']
print(pol['url'])
print(pol['spark_svc_id'])
print(pol['spark_svc_secret'])
print(s3['access_key'])
print(s3['secret_key'])
print(s3['endpoint'])
"""],
        capture_output=True, text=True, timeout=30,
    ).stdout.strip().splitlines()
    _POLARIS_URL, _SPARK_SVC_ID, _SPARK_SVC_SEC, _S3_KEY, _S3_SECRET, _S3_ENDPOINT = out
    _info(f"Polaris creds loaded from OpenBao (url={_POLARIS_URL})")


def _spark_sql(sql: str) -> str:
    """
    Run Spark SQL via the postgres-standard streaming pod with full Polaris
    OAuth catalog config for all three catalogs (postgres, oracle, mongodb).
    """
    pod = _kubectl(
        "get", "pods",
        "-l", "app=kafka-to-iceberg,pipeline.write-mode=standard,pipeline.source=postgres",
        "-o", "jsonpath={.items[0].metadata.name}",
    )
    if not pod:
        raise RuntimeError("No postgres-standard streaming pod found")

    confs = []
    confs.append("spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    for cat, (wh, _) in _CATALOG_MAP.items():
        confs += [
            f"spark.sql.catalog.{cat}=org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{cat}.type=rest",
            f"spark.sql.catalog.{cat}.uri={_POLARIS_URL}",
            f"spark.sql.catalog.{cat}.oauth2-server-uri={_POLARIS_URL}/v1/oauth/tokens",
            f"spark.sql.catalog.{cat}.credential={_SPARK_SVC_ID}:{_SPARK_SVC_SEC}",
            f"spark.sql.catalog.{cat}.scope=PRINCIPAL_ROLE:ALL",
            f"spark.sql.catalog.{cat}.warehouse={wh}",
            f"spark.sql.catalog.{cat}.rest.auth.type=oauth2",
            f"spark.sql.catalog.{cat}.s3.access-key-id={_S3_KEY}",
            f"spark.sql.catalog.{cat}.s3.secret-access-key={_S3_SECRET}",
            f"spark.sql.catalog.{cat}.s3.endpoint={_S3_ENDPOINT}",
            f"spark.sql.catalog.{cat}.s3.path-style-access=true",
            f"spark.sql.catalog.{cat}.client.region=us-east-2",
        ]
    conf_flags = " ".join(f"--conf {c}" for c in confs)
    cmd = f"cd /opt/spark/work-dir && spark-sql {conf_flags} -e \"{sql.strip()}\""
    result = subprocess.run(
        ["kubectl", "-n", "prod", "exec", pod, "--", "bash", "-c", cmd],
        capture_output=True, text=True, timeout=180,
    )
    return result.stdout.strip()


def _snapshot_restarts() -> dict[str, int]:
    counts: dict[str, int] = {}
    out = _kubectl(
        "get", "pods", "-l", "app=kafka-to-iceberg",
        "-o", r"jsonpath={range .items[*]}{.metadata.name}{':'}{.status.containerStatuses[0].restartCount}{'\n'}{end}",
        check=False,
    )
    for line in out.splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        name, _, count = line.rpartition(":")
        try:
            counts[name.strip()] = int(count.strip() or "0")
        except ValueError:
            pass
    return counts


# ── Column presence helpers ───────────────────────────────────────────────────

def _pg_has_col() -> bool:
    out = _pg(
        f"SELECT COUNT(*) FROM information_schema.columns "
        f"WHERE table_name='customers' AND column_name='{COL}'"
    )
    return out.strip() == "1"


def _ora_has_col() -> bool:
    out = _ora(
        f"SELECT COUNT(*) FROM USER_TAB_COLUMNS "
        f"WHERE TABLE_NAME='CUSTOMERS' AND COLUMN_NAME='{COL.upper()}'"
    )
    for line in out.splitlines():
        line = line.strip()
        if line.isdigit():
            return int(line) > 0
    return False


def _mgo_has_col() -> bool:
    out = _mgo(
        f'print(db.customers.findOne({{"{COL}":{{"$exists":true}}}} '
        f') !== null ? "yes" : "no")'
    )
    return "yes" in out


# ── DDL operations ────────────────────────────────────────────────────────────

def pg_add() -> None:
    _pg(f"ALTER TABLE customers ADD COLUMN IF NOT EXISTS {COL} SMALLINT")
    _stats["postgres"]["adds"] += 1

def pg_drop() -> None:
    _pg(f"ALTER TABLE customers DROP COLUMN IF EXISTS {COL}")
    _stats["postgres"]["drops"] += 1

def ora_add() -> None:
    if not _ora_has_col():
        _ora_block(f"ALTER TABLE CUSTOMERS ADD ({COL.upper()} NUMBER(5));\nCOMMIT;")
    _stats["oracle"]["adds"] += 1

def ora_drop() -> None:
    if _ora_has_col():
        _ora_block(f"ALTER TABLE CUSTOMERS DROP COLUMN {COL.upper()};\nCOMMIT;")
    _stats["oracle"]["drops"] += 1

def mgo_add() -> None:
    _mgo(
        f'db.customers.updateOne({{"{COL}":{{"$exists":false}}}},'
        f'{{"$set":{{"{COL}":null}}}})'
    )
    _stats["mongodb"]["adds"] += 1

def mgo_drop() -> None:
    _mgo(
        f'db.customers.updateMany({{"{COL}":{{"$exists":true}}}},'
        f'{{"$unset":{{"{COL}":""}}}})'
    )
    _stats["mongodb"]["drops"] += 1


# ── DML operations ────────────────────────────────────────────────────────────

def pg_dml(cycle: int) -> list[int]:
    base = _PG_BASE + (cycle * 100)
    ids  = list(range(base, base + DML_ROWS))
    rows = ",\n".join(
        f"({cid},'StressTest_{cycle}_{i}','stress_{cid}@example.com',"
        f"'555-{cid}','gold',{i+1})"
        for i, cid in enumerate(ids)
    )
    _pg(
        f"INSERT INTO customers(id,name,email,phone,tier,{COL}) "
        f"VALUES {rows} "
        f"ON CONFLICT(id) DO UPDATE SET {COL}=EXCLUDED.{COL}"
    )
    _stats["postgres"]["dml_rows"]  += DML_ROWS
    _stats["postgres"]["dml_cycles"] += 1
    _stats["postgres"]["dml_ids"].extend(ids)
    return ids


def ora_dml(cycle: int) -> list[int]:
    base = _ORA_BASE + (cycle * 100)
    ids  = list(range(base, base + DML_ROWS))
    stmts = "\n".join(
        f"MERGE INTO CUSTOMERS t USING (SELECT {cid} AS CUSTOMER_ID FROM dual) s "
        f"ON (t.CUSTOMER_ID=s.CUSTOMER_ID) "
        f"WHEN MATCHED THEN UPDATE SET t.{COL.upper()}={i+1} "
        f"WHEN NOT MATCHED THEN INSERT"
        f"(CUSTOMER_ID,FIRST_NAME,LAST_NAME,EMAIL,CITY,COUNTRY_CODE,TIER,CREDIT_LIMIT,IS_ACTIVE,{COL.upper()}) "
        f"VALUES({cid},'Stress{cycle}','Test','stress{cid}@example.com','TestCity','US','GOLD',5000,'Y',{i+1});"
        for i, cid in enumerate(ids)
    )
    _ora_block(f"{stmts}\nCOMMIT;")
    _stats["oracle"]["dml_rows"]  += DML_ROWS
    _stats["oracle"]["dml_cycles"] += 1
    _stats["oracle"]["dml_ids"].extend(ids)
    return ids


def mgo_dml(cycle: int) -> list[int]:
    base = _MGO_BASE + (cycle * 100)
    ids  = list(range(base, base + DML_ROWS))
    docs = "[" + ",".join(
        f'{{customer_id:{cid},first_name:"Stress{cycle}",last_name:"Test",'
        f'email:"stress{cid}@example.com",tier:"gold",credit_limit:5000,'
        f'is_active:true,{COL}:{i+1}}}'
        for i, cid in enumerate(ids)
    ) + "]"
    _mgo(f"db.customers.insertMany({docs}, {{ordered:false}})")
    _stats["mongodb"]["dml_rows"]  += DML_ROWS
    _stats["mongodb"]["dml_cycles"] += 1
    _stats["mongodb"]["dml_ids"].extend(ids)
    return ids


# ── Iceberg verification ──────────────────────────────────────────────────────

def _ice_has_col(catalog: str, ns: str, tbl: str) -> bool:
    try:
        out = _spark_sql(f"DESCRIBE TABLE `{catalog}`.`{ns}`.`{tbl}`")
        return COL.lower() in out.lower()
    except Exception:
        return False


def _ice_count(catalog: str, ns: str, tbl: str, pk: str,
               ids: list[int], condition: str) -> int:
    if not ids:
        return 0
    try:
        id_list = ",".join(str(i) for i in ids)
        out = _spark_sql(
            f"SELECT COUNT(*) FROM `{catalog}`.`{ns}`.`{tbl}` "
            f"WHERE `{pk}` IN ({id_list}) AND {condition}"
        )
        return int(out.strip().splitlines()[-1])
    except Exception:
        return -1


def _ice_total(catalog: str, ns: str, tbl: str) -> int:
    try:
        out = _spark_sql(f"SELECT COUNT(*) FROM `{catalog}`.`{ns}`.`{tbl}`")
        return int(out.strip().splitlines()[-1])
    except Exception:
        return -1


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print()
    print("=" * 76)
    print(f"  DDL STRESS TEST  —  column: {COL}")
    print(f"  PG={CYCLES_PG} cycles  ORA={CYCLES_ORA} cycles  MGO={CYCLES_MGO} cycles")
    print(f"  DML every {DML_EVERY} cycles ({DML_ROWS} rows each)")
    print(f"  Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 76)

    # Load Polaris + S3 creds from OpenBao (no secrets in source code)
    _load_polaris_creds()

    global _restarts_before
    _restarts_before = _snapshot_restarts()
    _info(f"Tracking {len(_restarts_before)} streaming pod(s)")

    # ── PostgreSQL cycles ─────────────────────────────────────────────────────
    print(f"\n{'─'*76}")
    print(f"  POSTGRES — {CYCLES_PG} ADD/DROP cycles")
    print(f"{'─'*76}")
    for cycle in range(1, CYCLES_PG + 1):
        try:
            pg_add()
            if cycle % DML_EVERY == 0:
                pg_dml(cycle)
                _ok(f"PG cycle {cycle:02d}/{CYCLES_PG}: ADD + DML ({DML_ROWS} rows)")
            else:
                _ok(f"PG cycle {cycle:02d}/{CYCLES_PG}: ADD")
            time.sleep(CYCLE_DELAY / 2)
            pg_drop()
            _info(f"PG cycle {cycle:02d}/{CYCLES_PG}: DROP")
            time.sleep(CYCLE_DELAY / 2)
        except Exception as exc:
            _stats["postgres"]["errors"] += 1
            _fail(f"PG cycle {cycle}: {exc}")

    # Leave the column present after the final cycle for Iceberg verification
    try:
        pg_add()
        _ok(f"PG final ADD (column present for Iceberg verification)")
    except Exception as exc:
        _stats["postgres"]["errors"] += 1
        _fail(f"PG final ADD: {exc}")

    # ── Oracle cycles ─────────────────────────────────────────────────────────
    print(f"\n{'─'*76}")
    print(f"  ORACLE — {CYCLES_ORA} ADD/DROP cycles")
    print(f"{'─'*76}")
    for cycle in range(1, CYCLES_ORA + 1):
        try:
            ora_add()
            if cycle % DML_EVERY == 0:
                ora_dml(cycle)
                _ok(f"ORA cycle {cycle:02d}/{CYCLES_ORA}: ADD + DML ({DML_ROWS} rows)")
            else:
                _ok(f"ORA cycle {cycle:02d}/{CYCLES_ORA}: ADD")
            time.sleep(CYCLE_DELAY / 2)
            ora_drop()
            _info(f"ORA cycle {cycle:02d}/{CYCLES_ORA}: DROP")
            time.sleep(CYCLE_DELAY / 2)
        except Exception as exc:
            _stats["oracle"]["errors"] += 1
            _fail(f"ORA cycle {cycle}: {exc}")

    try:
        ora_add()
        _ok(f"ORA final ADD (column present for Iceberg verification)")
    except Exception as exc:
        _stats["oracle"]["errors"] += 1
        _fail(f"ORA final ADD: {exc}")

    # ── MongoDB cycles ────────────────────────────────────────────────────────
    print(f"\n{'─'*76}")
    print(f"  MONGODB — {CYCLES_MGO} ADD/DROP cycles")
    print(f"{'─'*76}")
    for cycle in range(1, CYCLES_MGO + 1):
        try:
            mgo_add()
            if cycle % DML_EVERY == 0:
                mgo_dml(cycle)
                _ok(f"MGO cycle {cycle:02d}/{CYCLES_MGO}: ADD + DML ({DML_ROWS} rows)")
            else:
                _ok(f"MGO cycle {cycle:02d}/{CYCLES_MGO}: ADD")
            time.sleep(CYCLE_DELAY / 2)
            mgo_drop()
            _info(f"MGO cycle {cycle:02d}/{CYCLES_MGO}: DROP")
            time.sleep(CYCLE_DELAY / 2)
        except Exception as exc:
            _stats["mongodb"]["errors"] += 1
            _fail(f"MGO cycle {cycle}: {exc}")

    try:
        mgo_add()
        _ok(f"MGO final ADD (column present for Iceberg verification)")
    except Exception as exc:
        _stats["mongodb"]["errors"] += 1
        _fail(f"MGO final ADD: {exc}")

    # ── Wait for pipeline to flush all CDC events ─────────────────────────────
    print(f"\n{'─'*76}")
    print(f"  Waiting {POLL_SECS}s for pipeline to flush all CDC events to Iceberg…")
    print(f"{'─'*76}")
    deadline = time.time() + POLL_SECS
    remaining = {f"{c}.{n}.{t}" for c, n, t in _ICE_TABLES}
    while remaining and time.time() < deadline:
        still = set()
        for fqn in list(remaining):
            c, n, t = fqn.split(".")
            if _ice_has_col(c, n, t):
                _ok(f"Iceberg {fqn}: '{COL}' visible")
                remaining.discard(fqn)
            else:
                still.add(fqn)
        remaining = still
        if remaining:
            _info(f"Waiting… still missing in: {sorted(remaining)}")
            time.sleep(15)
    for fqn in remaining:
        _fail(f"Iceberg {fqn}: '{COL}' NOT visible after {POLL_SECS}s")

    # ── Check pod restarts ────────────────────────────────────────────────────
    restarts_after = _snapshot_restarts()
    restart_delta = {
        pod: restarts_after.get(pod, 0) - before
        for pod, before in _restarts_before.items()
        if restarts_after.get(pod, 0) > before
    }

    # ── Check streaming logs for errors ──────────────────────────────────────
    print(f"\n{'─'*76}")
    print(f"  Checking streaming logs for errors…")
    print(f"{'─'*76}")
    log_errors: dict[str, int] = {}
    for src in ("postgres", "oracle", "mongodb"):
        pod = _kubectl(
            "get", "pods",
            f"-l", f"app=kafka-to-iceberg,pipeline.source={src},pipeline.write-mode=standard",
            "-o", "jsonpath={.items[0].metadata.name}", check=False,
        )
        if pod:
            out = subprocess.run(
                ["kubectl", "-n", "prod", "logs", pod, "--since=3600s"],
                capture_output=True, text=True,
            ).stdout
            errs = sum(1 for l in out.splitlines()
                       if "[ERROR]" in l and "kafka-to-iceberg" in l)
            upserts = sum(1 for l in out.splitlines() if "upsert rows=" in l)
            log_errors[src] = errs
            _info(f"{src}: {upserts} upsert batch(es), {errs} ERROR line(s)")

    # ── Query Iceberg for DML row counts ─────────────────────────────────────
    print(f"\n{'─'*76}")
    print(f"  Querying Iceberg for DML row verification…")
    print(f"{'─'*76}")
    ice_results: dict[str, dict] = {}

    pg_ids  = _stats["postgres"]["dml_ids"]
    ora_ids = _stats["oracle"]["dml_ids"]
    mgo_ids = _stats["mongodb"]["dml_ids"]

    for catalog, ns, tbl, pk, ids, src_key in [
        ("postgres", "cache_testing", "customers", "id",          pg_ids,  "postgres"),
        ("oracle",   "cache_testing", "customers", "CUSTOMER_ID", ora_ids, "oracle"),
        ("mongodb",  "cache_testing", "customers", "customer_id", mgo_ids, "mongodb"),
    ]:
        col_present  = _ice_has_col(catalog, ns, tbl)
        total        = _ice_total(catalog, ns, tbl)
        not_null_cnt = _ice_count(catalog, ns, tbl, pk, ids, f"`{COL}` IS NOT NULL")
        null_cnt     = _ice_count(catalog, ns, tbl, pk, ids, f"`{COL}` IS NULL")
        ice_results[src_key] = {
            "fqn":         f"{catalog}.{ns}.{tbl}",
            "col_present": col_present,
            "total_rows":  total,
            "dml_ids":     len(ids),
            "not_null":    not_null_cnt,
            "null":        null_cnt,
        }

    # ── Final statistics report ───────────────────────────────────────────────
    print()
    print("=" * 76)
    print("  DDL STRESS TEST — STATISTICS REPORT")
    print(f"  Finished: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 76)

    # Source-side DDL/DML summary
    print()
    print(f"  {'SOURCE':<12} {'ADD':>6} {'DROP':>6} {'DML cycles':>12} {'DML rows':>10} {'Errors':>8}")
    print(f"  {'':─<12} {'':─>6} {'':─>6} {'':─>12} {'':─>10} {'':─>8}")
    for src in ("postgres", "oracle", "mongodb"):
        s = _stats[src]
        print(f"  {src:<12} {s['adds']:>6} {s['drops']:>6} {s['dml_cycles']:>12} {s['dml_rows']:>10} {s['errors']:>8}")

    # Iceberg replication results
    print()
    print(f"  {'TABLE':<42} {'COL?':>6} {'TOTAL':>8} {'DML IDs':>8} {'NOT NULL':>9} {'NULL':>6}")
    print(f"  {'':─<42} {'':─>6} {'':─>8} {'':─>8} {'':─>9} {'':─>6}")
    all_pass = True
    for src_key in ("postgres", "oracle", "mongodb"):
        r = ice_results[src_key]
        expected_not_null = _stats[src_key]["dml_rows"]
        ok_sym = "✓" if r["not_null"] == expected_not_null else "✗"
        if r["not_null"] != expected_not_null:
            all_pass = False
        print(
            f"  {ok_sym} {r['fqn']:<40} "
            f"{'YES' if r['col_present'] else 'NO':>6} "
            f"{r['total_rows']:>8} "
            f"{r['dml_ids']:>8} "
            f"{r['not_null']:>9} "
            f"{r['null']:>6}"
        )

    # Pod restarts
    print()
    if restart_delta:
        print(f"  ⚠  Pod restarts during test:")
        for pod, delta in restart_delta.items():
            print(f"     {pod}: +{delta}")
        all_pass = False
    else:
        print(f"  ✓  No pod restarts during test ({len(_restarts_before)} pods tracked)")

    # Streaming errors
    print()
    total_log_errors = sum(log_errors.values())
    if total_log_errors:
        print(f"  ⚠  Streaming ERROR lines: " + ", ".join(
            f"{src}={n}" for src, n in log_errors.items() if n))
        all_pass = False
    else:
        print(f"  ✓  No ERROR lines in streaming logs during test")

    # Overall verdict
    print()
    print("=" * 76)
    if all_pass:
        print("  ✓  ALL CHECKS PASSED — DDL stress replication is clean.")
    else:
        print("  ✗  SOME CHECKS FAILED — review output above.")
    print("=" * 76)
    print()

    if not all_pass:
        sys.exit(1)


if __name__ == "__main__":
    main()
