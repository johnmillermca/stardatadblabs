"""
Test (c) — Databricks SQL Warehouse connection via databricks-sql-connector.
Mirrors the JDBC checks in runbook-21 §5, using the Python connector
instead of Spark JDBC (which requires the Simba JAR baked into the image).
"""
import os, json, urllib.request

# ── Fetch creds from OpenBao ──────────────────────────────────────────────────
_BAO = "http://openbao.prod.svc.cluster.local:8200"
TOKEN = os.environ.get("TOKEN") or os.environ.get("BAO_TOKEN")
if not TOKEN:
    raise RuntimeError("Set TOKEN env-var to your OpenBao root/bootstrap token.")

def _bao(path, field):
    req = urllib.request.Request(
        f"{_BAO}/v1/{path}", headers={"X-Vault-Token": TOKEN}
    )
    data = json.loads(urllib.request.urlopen(req, timeout=10).read())
    return data["data"]["data"][field]

host      = _bao("secret/data/platform/databricks", "host")
http_path = _bao("secret/data/platform/databricks", "http_path")
token     = _bao("secret/data/platform/databricks", "token")
print(f"Host      : {host}")
print(f"HTTP path : {http_path}")

# ── Connect ───────────────────────────────────────────────────────────────────
from databricks import sql as dbsql

with dbsql.connect(
    server_hostname = host,
    http_path       = http_path,
    access_token    = token,
) as conn:
    with conn.cursor() as cur:

        # Test 1: SHOW TABLES
        cur.execute("SHOW TABLES IN `lakehouse`.`lakehouse_db`")
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        print("\n=== Tables discovered via SQL connector ===")
        print(" | ".join(cols))
        print("-" * 40)
        for row in rows:
            print(" | ".join(str(v) for v in row))

        # Test 2: row count
        cur.execute("SELECT COUNT(*) AS total FROM `lakehouse`.`lakehouse_db`.`product`")
        result = cur.fetchone()
        print(f"\n=== Row count via SQL connector ===")
        print(f"total = {result[0]}")

print("\n✅ Test (c) PASSED — Databricks SQL Warehouse connection confirmed")
