#!/usr/bin/env python3
"""
t09_t12_verify.py
─────────────────
Verify T-09 (FOREIGN catalog schemas/tables visible) and
T-12 (10 000 rows, 4 tiers) via Databricks SQL REST API.

Credentials read from OpenBao at runtime — no hard-coded secrets.
"""
import sys, json, urllib.request, urllib.error, time
sys.path.insert(0, "/opt/spark/scripts")
sys.path.insert(0, "/tmp")

# ── Resolve credentials from OpenBao ─────────────────────────────────────────
import os

BAO_ADDR = os.environ.get("ADDR", "http://openbao.prod.svc.cluster.local:8200")
BAO_TOKEN = os.environ.get("TOKEN", "")

def bao_get(path, key):
    url = f"{BAO_ADDR}/v1/{path}"
    req = urllib.request.Request(url, headers={"X-Vault-Token": BAO_TOKEN})
    with urllib.request.urlopen(req, timeout=10) as r:
        d = json.loads(r.read())
    return d["data"]["data"][key]

DB_WS       = bao_get("secret/data/databricks/pat", "workspace")
DB_TOKEN    = bao_get("secret/data/databricks/pat", "token")
WH_ID       = "942026cf5e55f3c3"

HDRS = {"Authorization": f"Bearer {DB_TOKEN}", "Content-Type": "application/json"}

# ── Helper ────────────────────────────────────────────────────────────────────
def run_sql(label, stmt):
    print(f"\n{'='*62}")
    print(f"  {label}")
    print(f"{'='*62}")
    try:
        req = urllib.request.Request(
            f"{DB_WS}/api/2.0/sql/statements",
            data=json.dumps({
                "warehouse_id": WH_ID,
                "wait_timeout": "30s",
                "statement":    stmt,
            }).encode(),
            headers=HDRS, method="POST")
        with urllib.request.urlopen(req, timeout=60) as r:
            d = json.loads(r.read())
    except urllib.error.HTTPError as e:
        print(f"  HTTP {e.code}: {e.read().decode(errors='replace')[:400]}")
        return None

    # poll until terminal state
    for _ in range(60):
        state = d.get("status", {}).get("state", "")
        if state in ("SUCCEEDED", "FAILED", "CANCELED"):
            break
        sid = d.get("statement_id", "")
        time.sleep(5)
        try:
            req2 = urllib.request.Request(
                f"{DB_WS}/api/2.0/sql/statements/{sid}", headers=HDRS)
            with urllib.request.urlopen(req2, timeout=30) as r2:
                d = json.loads(r2.read())
        except urllib.error.HTTPError as e2:
            print(f"  poll err {e2.code}: {e2.read().decode()[:200]}")
            return None

    err = d.get("status", {}).get("error", {})
    if err:
        print(f"  FAILED — {err.get('error_code')}: {err.get('message','')[:300]}")
        return None

    state = d.get("status", {}).get("state")
    cols  = [c["name"] for c in d.get("manifest",{}).get("schema",{}).get("columns",[])]
    rows  = d.get("result",{}).get("data_array",[])
    print(f"  state: {state}  |  rows returned: {len(rows)}")
    if cols:
        hdr = "  " + " | ".join(f"{c:18s}" for c in cols)
        print(hdr)
        print("  " + "-" * (len(hdr) - 2))
    for row in rows:
        print("  " + " | ".join(f"{str(v):18s}" for v in row))
    return rows

# ── T-09 verification ─────────────────────────────────────────────────────────
schemas = run_sql(
    "T-09 verify — SHOW SCHEMAS IN star_lakehouse",
    "SHOW SCHEMAS IN star_lakehouse")

tables = run_sql(
    "T-09 verify — SHOW TABLES IN star_lakehouse.demo",
    "SHOW TABLES IN star_lakehouse.demo")

# ── T-12a: row count ──────────────────────────────────────────────────────────
count_rows = run_sql(
    "T-12a — SELECT COUNT(*) FROM star_lakehouse.demo.customers",
    "SELECT COUNT(*) AS total_rows FROM star_lakehouse.demo.customers")

# ── T-12b: tier distribution ──────────────────────────────────────────────────
tier_rows = run_sql(
    "T-12b — customer_tier distribution",
    ("SELECT customer_tier, COUNT(*) AS cnt "
     "FROM star_lakehouse.demo.customers "
     "GROUP BY customer_tier ORDER BY customer_tier"))

# ── Pass / Fail summary ───────────────────────────────────────────────────────
print(f"\n{'='*62}")
print("  PASS / FAIL SUMMARY")
print(f"{'='*62}")

t09_schemas_ok = schemas is not None and len(schemas) > 0
t09_tables_ok  = tables  is not None and len(tables) > 0

total = int(count_rows[0][0]) if count_rows else -1
tiers = sorted([r[0] for r in tier_rows]) if tier_rows else []

t12a_ok = total == 10000
t12b_ok = tiers == ["gold", "platinum", "silver", "standard"]

print(f"  T-09 schemas visible : {'✅ PASS' if t09_schemas_ok else '❌ FAIL'} ({len(schemas) if schemas else 0} schema(s))")
print(f"  T-09 tables visible  : {'✅ PASS' if t09_tables_ok  else '❌ FAIL'} ({len(tables) if tables else 0} table(s))")
print(f"  T-12a row count      : {'✅ PASS' if t12a_ok else '❌ FAIL'} ({total} rows, expected 10000)")
print(f"  T-12b tier check     : {'✅ PASS' if t12b_ok else '❌ FAIL'} ({tiers})")
