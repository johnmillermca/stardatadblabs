#!/usr/bin/env python3
"""
scripts/ddl_apply.py
====================
Manual DDL executor for the Kafka → Iceberg CDC pipeline.

Purpose
-------
When a DDL change (ADD COLUMN, DROP COLUMN, MODIFY column type, RENAME COLUMN)
is applied to a source database (Oracle, PostgreSQL, MongoDB), the CDC streaming
pipeline (05_kafka_to_iceberg_streaming.py) does NOT automatically evolve the
Iceberg schema. You must run this script to:

  1. Scale down  the streaming Deployment(s) for the affected source — DML
                 replication stops cleanly at the last committed Kafka offset.
  2. Apply       the DDL to every applicable Iceberg table (standard, soft_delete,
                 history_tracking) via Spark SQL inside the streaming pod.
  3. Verify      the DDL succeeded in Iceberg (DESCRIBE TABLE).
  4. Scale up    the Deployment(s) — DML replication resumes from where it stopped.

No data is lost: Spark Structured Streaming commits offsets only after a
successful batch write. While the pod is scaled to 0 the unconsumed Kafka
messages stay in the topic and are consumed on resume.

Supported DDL operations
------------------------
  ADD COLUMN   col_name  iceberg_type
  DROP COLUMN  col_name
  MODIFY       col_name  new_iceberg_type   (changes the column type)
  RENAME       old_name  new_name

Supported Iceberg types (pass as iceberg_type argument)
-------------------------------------------------------
  STRING, BIGINT, INT, DOUBLE, FLOAT, BOOLEAN, TIMESTAMP, DATE,
  DECIMAL(p,s)  e.g. DECIMAL(18,4)

Usage examples
--------------
  # ADD a NUMBER column to oracle/cache_testing/customers (all 3 write modes):
  python3 scripts/ddl_apply.py \\
      --source oracle \\
      --table  customers \\
      --op     add \\
      --col    credit_score \\
      --type   "DECIMAL(10,2)"

  # ADD a VARCHAR column:
  python3 scripts/ddl_apply.py \\
      --source oracle \\
      --table  customers \\
      --op     add \\
      --col    loyalty_tier \\
      --type   STRING

  # MODIFY an existing column type (e.g. widening NUMBER precision):
  python3 scripts/ddl_apply.py \\
      --source oracle \\
      --table  customers \\
      --op     modify \\
      --col    credit_score \\
      --type   "DECIMAL(18,4)"

  # RENAME a column:
  python3 scripts/ddl_apply.py \\
      --source oracle \\
      --table  customers \\
      --op     rename \\
      --col    credit_score \\
      --new-col credit_score_v2

  # DROP a column:
  python3 scripts/ddl_apply.py \\
      --source oracle \\
      --table  customers \\
      --op     drop \\
      --col    obsolete_col

  # Apply to specific write modes only (default: all three):
  python3 scripts/ddl_apply.py \\
      --source  postgres \\
      --table   orders \\
      --op      add \\
      --col     shipped_at \\
      --type    TIMESTAMP \\
      --modes   standard,soft_delete

  # Dry-run (show what would be executed, no changes):
  python3 scripts/ddl_apply.py \\
      --source oracle \\
      --table  customers \\
      --op     add \\
      --col    test_col \\
      --type   STRING \\
      --dry-run

Environment variables
---------------------
  DRY_RUN=1   Same as --dry-run
  NAMESPACE   K8s namespace (default: prod)

What this script does NOT do
-----------------------------
  • It does not run DDL on the source database (Oracle/PostgreSQL/MongoDB).
    You must apply the DDL on the source FIRST, then run this script.
  • It does not restart Debezium. Debezium auto-detects schema changes.
  • It does not clear Kafka offsets or checkpoints.
"""

from __future__ import annotations

import argparse
import base64
import json
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
import os

DRY_RUN   = os.environ.get("DRY_RUN", "0") == "1"
K8S_NS    = os.environ.get("NAMESPACE", "prod")
BAO_ADDR  = os.environ.get("ADDR", "http://192.168.1.50:30820")

# Source → Iceberg catalog + namespace
_SOURCE_META: dict[str, dict] = {
    "oracle": {
        "catalog":   "oracle",
        "namespace": "cache_testing",
        "deployments": {
            "standard":         "kafka-to-iceberg-oracle-standard",
            "soft_delete":      "kafka-to-iceberg-oracle-soft-delete",
            "history_tracking": "kafka-to-iceberg-oracle-history-tracking",
        },
    },
    "postgres": {
        "catalog":   "postgres",
        "namespace": "cache_testing",
        "deployments": {
            "standard":         "kafka-to-iceberg-postgres-standard",
            "soft_delete":      "kafka-to-iceberg-postgres-soft-delete",
            "history_tracking": "kafka-to-iceberg-postgres-history-tracking",
        },
    },
    "mongodb": {
        "catalog":   "mongodb",
        "namespace": "cache_testing",
        "deployments": {
            "standard":         "kafka-to-iceberg-mongodb-standard",
            "soft_delete":      "kafka-to-iceberg-mongodb-soft-delete",
            "history_tracking": "kafka-to-iceberg-mongodb-history-tracking",
        },
    },
}

# Write-mode → Iceberg table suffix
_MODE_SUFFIX: dict[str, str] = {
    "standard":         "",       # customers
    "soft_delete":      "_sd",    # customers_sd
    "history_tracking": "_hist",  # customers_hist
}

# ─────────────────────────────────────────────────────────────────────────────
# Logging helpers
# ─────────────────────────────────────────────────────────────────────────────
def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")

def _ok(msg: str)     -> None: print(f"  [{_ts()}] ✓  {msg}", flush=True)
def _fail(msg: str)   -> None: print(f"  [{_ts()}] ✗  {msg}", file=sys.stderr, flush=True)
def _info(msg: str)   -> None: print(f"  [{_ts()}]    {msg}", flush=True)
def _warn(msg: str)   -> None: print(f"  [{_ts()}] ⚠  {msg}", flush=True)
def _hdr(msg: str)    -> None: print(f"\n{'─'*72}\n  {msg}\n{'─'*72}", flush=True)
def _dry(msg: str)    -> None: print(f"  [{_ts()}] [DRY-RUN]  {msg}", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# OpenBao token
# ─────────────────────────────────────────────────────────────────────────────
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
            ["kubectl", "-n", K8S_NS, "get", "secret", "openbao-unseal-keys",
             "-o", "jsonpath={.data.root-token}"],
            capture_output=True, text=True, check=True, timeout=10,
        )
        _BAO_TOKEN = base64.b64decode(r.stdout.strip()).decode()
        return _BAO_TOKEN
    except Exception as e:
        raise RuntimeError(
            f"Could not fetch OpenBao token: {e}\nSet BAO_TOKEN env var explicitly."
        )


# ─────────────────────────────────────────────────────────────────────────────
# kubectl helpers
# ─────────────────────────────────────────────────────────────────────────────
def _kubectl(*args: str, check: bool = True, timeout: int = 60) -> str:
    r = subprocess.run(
        ["kubectl", "-n", K8S_NS, *args],
        capture_output=True, text=True, check=check, timeout=timeout,
    )
    return r.stdout.strip()


def _get_spark_pod() -> str:
    """Find a running streaming pod to exec spark-sql into."""
    for source in ["oracle", "postgres", "mongodb"]:
        for mode in ["standard", "soft_delete", "history_tracking"]:
            deploy = _SOURCE_META[source]["deployments"][mode]
            try:
                pod = _kubectl(
                    "get", "pods",
                    "-l", f"app=kafka-to-iceberg",
                    "--field-selector=status.phase=Running",
                    "-o", "jsonpath={.items[0].metadata.name}",
                    check=False,
                )
                if pod:
                    return pod
            except Exception:
                pass
    # Fallback: any running pod with spark
    pod = _kubectl(
        "get", "pods",
        "--field-selector=status.phase=Running",
        "-o", "jsonpath={.items[0].metadata.name}",
        check=False,
    )
    if not pod:
        raise RuntimeError(
            "No running pod found to execute Spark SQL. "
            "Ensure at least one streaming pod is running before calling ddl_apply.py "
            "(scale down target source only, leave one pod from another source running), "
            "or specify a pod via SPARK_POD env var."
        )
    return pod


def _get_spark_conf_flags(pod: str) -> str:
    """Build spark-sql --conf flags from the pod's BaoSparkInit."""
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
    r = subprocess.run(
        ["kubectl", "-n", K8S_NS, "exec", pod, "--", "python3", "-c", _CONF_BUILDER],
        capture_output=True, text=True, timeout=30,
    )
    if r.returncode != 0:
        raise RuntimeError(f"Spark conf build failed:\n{r.stderr.strip()}")
    return " ".join(
        f"--conf '{l.strip()}'" for l in r.stdout.strip().splitlines() if "=" in l
    )


def _spark_sql(pod: str, conf_flags: str, sql: str, timeout: int = 120) -> tuple[int, str, str]:
    """Execute sql via spark-sql in the given pod. Returns (returncode, stdout, stderr)."""
    esc = sql.strip().replace("'", r"'\''")
    r = subprocess.run(
        ["kubectl", "-n", K8S_NS, "exec", pod, "--",
         "bash", "-c",
         f"cd /opt/spark/work-dir && spark-sql {conf_flags} -e '{esc}'"],
        capture_output=True, text=True, timeout=timeout,
    )
    return r.returncode, r.stdout.strip(), r.stderr.strip()


# ─────────────────────────────────────────────────────────────────────────────
# Deployment scale helpers
# ─────────────────────────────────────────────────────────────────────────────
def _scale(deployment: str, replicas: int) -> None:
    action = "up" if replicas > 0 else "down"
    if DRY_RUN:
        _dry(f"kubectl scale deployment/{deployment} --replicas={replicas}")
        return
    _info(f"Scaling {deployment} → replicas={replicas} …")
    _kubectl("scale", f"deployment/{deployment}", f"--replicas={replicas}")
    _ok(f"Scaled {deployment} → {replicas}")


def _wait_for_scale_down(deployment: str, timeout_s: int = 60) -> None:
    """Wait until the deployment has 0 ready replicas."""
    if DRY_RUN:
        return
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        ready = _kubectl(
            "get", f"deployment/{deployment}",
            "-o", "jsonpath={.status.readyReplicas}",
            check=False,
        )
        if ready in ("", "0"):
            _ok(f"{deployment}: all pods stopped.")
            return
        _info(f"  {deployment}: {ready} pod(s) still running — waiting …")
        time.sleep(5)
    _warn(f"{deployment}: pods still running after {timeout_s}s — proceeding anyway.")


def _wait_for_scale_up(deployment: str, timeout_s: int = 120) -> None:
    """Wait until the deployment has ≥1 ready replicas."""
    if DRY_RUN:
        return
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        ready = _kubectl(
            "get", f"deployment/{deployment}",
            "-o", "jsonpath={.status.readyReplicas}",
            check=False,
        )
        if ready and ready != "0":
            _ok(f"{deployment}: {ready} pod(s) ready.")
            return
        _info(f"  {deployment}: waiting for pod to become ready …")
        time.sleep(5)
    _warn(f"{deployment}: pod not ready after {timeout_s}s — check logs.")


# ─────────────────────────────────────────────────────────────────────────────
# DDL builder
# ─────────────────────────────────────────────────────────────────────────────
def _build_ddl(
    op:        str,
    fqn:       str,
    col:       str,
    col_type:  str | None,
    new_col:   str | None,
) -> str:
    """Return the Iceberg Spark SQL DDL statement for the given operation."""
    op = op.lower()
    if op == "add":
        if not col_type:
            raise ValueError("--type is required for ADD COLUMN")
        return f"ALTER TABLE {fqn} ADD COLUMN `{col}` {col_type}"
    elif op == "drop":
        return f"ALTER TABLE {fqn} DROP COLUMN `{col}`"
    elif op == "modify":
        if not col_type:
            raise ValueError("--type is required for MODIFY")
        return f"ALTER TABLE {fqn} ALTER COLUMN `{col}` TYPE {col_type}"
    elif op == "rename":
        if not new_col:
            raise ValueError("--new-col is required for RENAME")
        return f"ALTER TABLE {fqn} RENAME COLUMN `{col}` TO `{new_col}`"
    else:
        raise ValueError(f"Unknown operation: {op!r}. Choose: add, drop, modify, rename")


# ─────────────────────────────────────────────────────────────────────────────
# Verification
# ─────────────────────────────────────────────────────────────────────────────
def _verify_col_exists(pod: str, conf_flags: str, fqn: str, col: str) -> bool:
    """Return True if col exists in the Iceberg table after DDL."""
    rc, out, _ = _spark_sql(pod, conf_flags, f"DESCRIBE TABLE {fqn}")
    return col.lower() in out.lower()


def _verify_col_absent(pod: str, conf_flags: str, fqn: str, col: str) -> bool:
    """Return True if col does NOT exist in the Iceberg table after DROP."""
    rc, out, _ = _spark_sql(pod, conf_flags, f"DESCRIBE TABLE {fqn}")
    return col.lower() not in out.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Main DDL apply logic
# ─────────────────────────────────────────────────────────────────────────────
def apply_ddl(
    source:   str,
    table:    str,
    op:       str,
    col:      str,
    col_type: str | None,
    new_col:  str | None,
    modes:    list[str],
) -> bool:
    """
    Full DDL apply cycle:
      1. Scale down streaming deployments for the given source + modes.
      2. Wait for pods to stop.
      3. Execute ALTER TABLE on each applicable Iceberg table.
      4. Verify each DDL succeeded.
      5. Scale up streaming deployments.

    Returns True if ALL DDL statements succeeded, False if any failed.
    """
    meta   = _SOURCE_META[source]
    cat    = meta["catalog"]
    ns     = meta["namespace"]
    deploys = meta["deployments"]

    _hdr(f"DDL Apply: {source}.{ns}.{table} — {op.upper()} {col}"
         + (f" {col_type}" if col_type else "")
         + (f" → {new_col}" if new_col else ""))

    # ── 1. Scale down target deployments ──────────────────────────────────────
    _info(f"Step 1/4 — Pausing DML replication for source={source}, modes={modes}")
    prev_replicas: dict[str, int] = {}
    for mode in modes:
        deploy = deploys.get(mode)
        if not deploy:
            continue
        # Capture current replica count so we can restore it exactly
        if not DRY_RUN:
            cur = _kubectl(
                "get", f"deployment/{deploy}",
                "-o", "jsonpath={.spec.replicas}",
                check=False,
            )
            prev_replicas[deploy] = int(cur) if cur.isdigit() else 1
        else:
            prev_replicas[deploy] = 1
        _scale(deploy, 0)

    # Wait for all pods to stop
    if not DRY_RUN:
        for deploy in prev_replicas:
            _wait_for_scale_down(deploy)
    _ok("All target pods stopped. Kafka offsets frozen at last committed position.")

    # ── 2. Find a running Spark pod to execute DDL ────────────────────────────
    _info("Step 2/4 — Locating a running Spark pod for DDL execution …")
    spark_pod_env = os.environ.get("SPARK_POD", "")
    try:
        if spark_pod_env:
            spark_pod = spark_pod_env
            _info(f"Using SPARK_POD={spark_pod}")
        else:
            spark_pod = _get_spark_pod()
            _info(f"Using pod: {spark_pod}")
        conf_flags = _get_spark_conf_flags(spark_pod)
        _ok("Spark catalog conf ready.")
    except Exception as e:
        _fail(f"Could not get Spark pod / conf: {e}")
        _warn("Scaling deployments back up before exiting.")
        for deploy, r in prev_replicas.items():
            _scale(deploy, r)
        return False

    # ── 3. Apply DDL to each Iceberg table ────────────────────────────────────
    _info("Step 3/4 — Applying DDL in Iceberg …")
    all_ok = True

    for mode in modes:
        suffix     = _MODE_SUFFIX[mode]
        ice_table  = f"{table}{suffix}"
        fqn        = f"`{cat}`.`{ns}`.`{ice_table}`"

        ddl = _build_ddl(op, fqn, col, col_type, new_col)
        _info(f"  [{mode}] {ddl}")

        if DRY_RUN:
            _dry(f"Would execute: {ddl}")
            continue

        rc, out, err = _spark_sql(spark_pod, conf_flags, ddl)

        if rc != 0:
            # Some errors are non-fatal (column already exists / already dropped)
            err_lower = (err + out).lower()
            already_exists = (
                "already exists" in err_lower
                or "column already exists" in err_lower
            )
            already_gone = (
                "cannot resolve" in err_lower
                or "no such column" in err_lower
                or "column not found" in err_lower
            )
            if op == "add" and already_exists:
                _warn(f"  [{mode}] Column `{col}` already exists in {ice_table} — skipping.")
            elif op == "drop" and already_gone:
                _warn(f"  [{mode}] Column `{col}` not found in {ice_table} — skipping.")
            else:
                _fail(f"  [{mode}] DDL failed for {ice_table}:\n{err or out}")
                all_ok = False
        else:
            _ok(f"  [{mode}] DDL applied to {ice_table}.")

    # ── 4. Verify DDL in Iceberg ──────────────────────────────────────────────
    if not DRY_RUN:
        _info("Step 4a/4 — Verifying DDL results in Iceberg …")
        check_col = new_col if (op == "rename" and new_col) else col
        for mode in modes:
            suffix    = _MODE_SUFFIX[mode]
            ice_table = f"{table}{suffix}"
            fqn       = f"`{cat}`.`{ns}`.`{ice_table}`"

            if op == "drop":
                ok = _verify_col_absent(spark_pod, conf_flags, fqn, col)
                status = "absent ✓" if ok else "STILL PRESENT ✗"
            elif op == "rename":
                ok = (_verify_col_exists(spark_pod, conf_flags, fqn, new_col)
                      and _verify_col_absent(spark_pod, conf_flags, fqn, col))
                status = "renamed ✓" if ok else "rename INCOMPLETE ✗"
            else:
                ok = _verify_col_exists(spark_pod, conf_flags, fqn, check_col)
                status = "present ✓" if ok else "MISSING ✗"

            verb = f"Column `{check_col}` in {ice_table}: {status}"
            if ok:
                _ok(verb)
            else:
                _fail(verb)
                all_ok = False

    # ── 5. Scale deployments back up ──────────────────────────────────────────
    _info("Step 4b/4 — Resuming DML replication …")
    for deploy, r in prev_replicas.items():
        _scale(deploy, r)
    if not DRY_RUN:
        for deploy, r in prev_replicas.items():
            if r > 0:
                _wait_for_scale_up(deploy)
    _ok("DML replication resumed. Pipeline will pick up from last committed Kafka offset.")

    return all_ok


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Apply DDL to Iceberg tables for the Kafka→Iceberg CDC pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--source", required=True,
        choices=list(_SOURCE_META.keys()),
        help="CDC source: oracle | postgres | mongodb",
    )
    p.add_argument(
        "--table", required=True,
        help="Source table name (without write-mode suffix), e.g. 'customers'",
    )
    p.add_argument(
        "--op", required=True,
        choices=["add", "drop", "modify", "rename"],
        help="DDL operation: add | drop | modify | rename",
    )
    p.add_argument(
        "--col", required=True,
        help="Column name to add/drop/modify/rename",
    )
    p.add_argument(
        "--type", dest="col_type", default=None,
        help="Iceberg type for ADD or MODIFY (e.g. STRING, BIGINT, 'DECIMAL(18,4)')",
    )
    p.add_argument(
        "--new-col", dest="new_col", default=None,
        help="New column name for RENAME operation",
    )
    p.add_argument(
        "--modes", default="standard,soft_delete,history_tracking",
        help=(
            "Comma-separated write modes to apply DDL to "
            "(default: standard,soft_delete,history_tracking). "
            "Example: --modes standard,soft_delete"
        ),
    )
    p.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print what would be executed without making any changes.",
    )
    return p.parse_args()


def main() -> None:
    global DRY_RUN

    args = _parse_args()

    if args.dry_run:
        DRY_RUN = True

    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    invalid_modes = [m for m in modes if m not in _MODE_SUFFIX]
    if invalid_modes:
        _fail(f"Invalid --modes value(s): {invalid_modes}. "
              f"Choose from: {list(_MODE_SUFFIX.keys())}")
        sys.exit(1)

    print()
    print("=" * 72)
    print("  CDC DDL APPLY")
    print(f"  Source   : {args.source}")
    print(f"  Table    : {args.table}")
    print(f"  Operation: {args.op.upper()} `{args.col}`"
          + (f" {args.col_type}" if args.col_type else "")
          + (f" → `{args.new_col}`" if args.new_col else ""))
    print(f"  Modes    : {', '.join(modes)}")
    print(f"  Dry-run  : {'YES — no changes will be made' if DRY_RUN else 'NO — changes will be applied'}")
    print(f"  Started  : {datetime.now(timezone.utc).isoformat()}")
    print("=" * 72)

    if not DRY_RUN:
        print()
        print("  ⚠  This will STOP DML replication for the target source while DDL")
        print("     is applied, then automatically restart it.")
        print()
        try:
            confirm = input("  Type 'yes' to proceed, anything else to abort: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            confirm = ""
        if confirm != "yes":
            print("  Aborted.")
            sys.exit(0)

    success = apply_ddl(
        source   = args.source,
        table    = args.table,
        op       = args.op,
        col      = args.col,
        col_type = args.col_type,
        new_col  = args.new_col,
        modes    = modes,
    )

    print()
    if success:
        print("=" * 72)
        print("  ✓  DDL apply completed successfully.")
        if not DRY_RUN:
            print()
            print("  Next steps:")
            print("  1. Verify rows with the new column arrive in Iceberg as expected.")
            print("  2. If the source had rows with the new column already written")
            print("     before this script ran, those values were NULL in Iceberg.")
            print("     Re-replicate them via a source DML UPDATE if needed.")
        print("=" * 72)
        sys.exit(0)
    else:
        print("=" * 72)
        print("  ✗  DDL apply completed with errors. Check output above.")
        print("     The streaming pipeline has been restarted regardless.")
        print("=" * 72)
        sys.exit(1)


if __name__ == "__main__":
    main()
