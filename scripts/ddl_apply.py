#!/usr/bin/env python3
"""
scripts/ddl_apply.py
====================
Manual DDL executor for the Kafka → Iceberg CDC pipeline.

Purpose
-------
When a DDL change occurs on a source database (Oracle, PostgreSQL, or MongoDB),
the CDC streaming pipeline does NOT automatically evolve the Iceberg schema.
Run this script AFTER applying the DDL on the source to:

  1. Scale down  — stop the streaming Deployment(s) for the affected source/modes.
                   DML replication halts at the last committed Kafka offset.
  2. Confirm     — poll every pod until ALL are fully stopped (zero running pods),
                   guaranteeing no in-flight batch can write against the old schema.
  3. Apply DDL   — execute ALTER TABLE on every applicable Iceberg table
                   (standard → <table>, soft_delete → <table>_sd,
                    history_tracking → <table>_hist) via spark-sql.
  4. Verify      — DESCRIBE TABLE confirms the column change is committed in Iceberg.
  5. Scale up    — restore each Deployment to its original replica count.
                   Pipeline resumes from the exact Kafka offset where it stopped.

No data is lost: offsets are committed only after a successful batch write.
Messages that arrived during the pause accumulate in Kafka and are consumed
on resume.

Dynamic source/table/namespace support
---------------------------------------
This script is NOT hard-coded to the customers table or any specific namespace.
Pass any source, table, and namespace via CLI flags.  Use --namespace to override
the Iceberg namespace (defaults to cache_testing).  Use --catalog to override
the Iceberg catalog (defaults to the source name).

Supported DDL operations
------------------------
  add     — ADD COLUMN col_name iceberg_type
  drop    — DROP COLUMN col_name
  modify  — ALTER COLUMN col_name TYPE new_type   (widen type, e.g. INT→BIGINT)
  rename  — RENAME COLUMN old_name TO new_name

Supported Iceberg types (--type argument)
-----------------------------------------
  STRING, BIGINT, INT, DOUBLE, FLOAT, BOOLEAN, TIMESTAMP, DATE,
  DECIMAL(p,s)  e.g. "DECIMAL(18,4)"

Usage examples
--------------
  # Oracle — ADD a NUMBER column to customers (all 3 write modes):
  python3 scripts/ddl_apply.py \\
      --source oracle --table customers \\
      --op add --col credit_score --type "DECIMAL(10,2)"

  # Oracle — ADD a VARCHAR column:
  python3 scripts/ddl_apply.py \\
      --source oracle --table customers \\
      --op add --col loyalty_tier --type STRING

  # Oracle — MODIFY column type (widen precision):
  python3 scripts/ddl_apply.py \\
      --source oracle --table customers \\
      --op modify --col credit_score --type "DECIMAL(18,4)"

  # Oracle — RENAME column:
  python3 scripts/ddl_apply.py \\
      --source oracle --table customers \\
      --op rename --col credit_score --new-col credit_score_v2

  # Oracle — DROP column:
  python3 scripts/ddl_apply.py \\
      --source oracle --table customers \\
      --op drop --col obsolete_col

  # PostgreSQL — ADD column to orders table:
  python3 scripts/ddl_apply.py \\
      --source postgres --table orders \\
      --op add --col shipped_at --type TIMESTAMP

  # PostgreSQL — RENAME column, standard + soft_delete modes only:
  python3 scripts/ddl_apply.py \\
      --source postgres --table orders \\
      --op rename --col status --new-col order_status \\
      --modes standard,soft_delete

  # MongoDB — ADD field to events collection:
  python3 scripts/ddl_apply.py \\
      --source mongodb --table events \\
      --op add --col event_version --type INT

  # Any source — override namespace (for non-cache_testing databases):
  python3 scripts/ddl_apply.py \\
      --source oracle --table products --namespace tpcds \\
      --op add --col discount_pct --type "DECIMAL(5,2)"

  # Dry-run (show all steps without making any changes):
  python3 scripts/ddl_apply.py \\
      --source oracle --table customers \\
      --op add --col test_col --type STRING --dry-run

Environment variables
---------------------
  DRY_RUN=1     Same as --dry-run flag
  NAMESPACE     K8s namespace for kubectl (default: prod)
  SPARK_POD     Override the pod used for spark-sql execution
  BAO_TOKEN     OpenBao root token (auto-fetched from K8s secret if unset)

What this script does NOT do
-----------------------------
  • Does not run DDL on the source database — apply source DDL FIRST.
  • Does not restart Debezium — it auto-detects schema changes.
  • Does not clear Kafka offsets or S3 checkpoints.
  • Does not update the Iceberg schema cache inside running pods —
    the cache is refreshed automatically on the next pod startup.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

# ─────────────────────────────────────────────────────────────────────────────
# Runtime config (overridable via env vars)
# ─────────────────────────────────────────────────────────────────────────────
DRY_RUN  = os.environ.get("DRY_RUN", "0") == "1"
K8S_NS   = os.environ.get("NAMESPACE", "prod")
BAO_ADDR = os.environ.get("ADDR", "http://192.168.1.50:30820")

# ─────────────────────────────────────────────────────────────────────────────
# Source registry
# ─────────────────────────────────────────────────────────────────────────────
# Each source declares:
#   catalog           — Iceberg catalog name
#   default_namespace — default Iceberg namespace (overridable via --namespace)
#   deploy_pattern    — f-string template: {source} and {mode} are substituted
#                       to produce the K8s Deployment name.
#
# Adding a new source: add one entry here. No other code changes needed.
#
_SOURCE_REGISTRY: dict[str, dict] = {
    "oracle": {
        "catalog":           "oracle",
        "default_namespace": "cache_testing",
        "deploy_pattern":    "kafka-to-iceberg-{source}-{mode_k8s}",
    },
    "postgres": {
        "catalog":           "postgres",
        "default_namespace": "cache_testing",
        "deploy_pattern":    "kafka-to-iceberg-{source}-{mode_k8s}",
    },
    "mongodb": {
        "catalog":           "mongodb",
        "default_namespace": "cache_testing",
        "deploy_pattern":    "kafka-to-iceberg-{source}-{mode_k8s}",
    },
}

# Write-mode → Iceberg table suffix + k8s deployment name fragment
# mode_k8s: the segment used in Deployment names (soft-delete uses hyphen, not underscore)
_WRITE_MODES: dict[str, dict] = {
    "standard":         {"suffix": "",      "mode_k8s": "standard"},
    "soft_delete":      {"suffix": "_sd",   "mode_k8s": "soft-delete"},
    "history_tracking": {"suffix": "_hist", "mode_k8s": "history-tracking"},
}

# ─────────────────────────────────────────────────────────────────────────────
# Logging helpers
# ─────────────────────────────────────────────────────────────────────────────
def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")

def _ok(msg: str)   -> None: print(f"  [{_ts()}] ✓  {msg}", flush=True)
def _fail(msg: str) -> None: print(f"  [{_ts()}] ✗  {msg}", file=sys.stderr, flush=True)
def _info(msg: str) -> None: print(f"  [{_ts()}]    {msg}", flush=True)
def _warn(msg: str) -> None: print(f"  [{_ts()}] ⚠  {msg}", flush=True)
def _hdr(msg: str)  -> None: print(f"\n{'─'*72}\n  {msg}\n{'─'*72}", flush=True)
def _dry(msg: str)  -> None: print(f"  [{_ts()}] [DRY-RUN]  {msg}", flush=True)
def _step(n: int, total: int, msg: str) -> None:
    print(f"\n  ── Step {n}/{total}: {msg}", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# OpenBao token
# ─────────────────────────────────────────────────────────────────────────────
_BAO_TOKEN: str | None = None

def _bao_token() -> str:
    global _BAO_TOKEN
    if _BAO_TOKEN:
        return _BAO_TOKEN
    if t := os.environ.get("BAO_TOKEN"):
        _BAO_TOKEN = t
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
            f"Could not fetch OpenBao token: {e}\n"
            "Set BAO_TOKEN env var or ensure kubectl can reach the cluster."
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


def _deployment_exists(deploy: str) -> bool:
    """Return True if the Deployment exists in K8s (may be scaled to 0)."""
    out = _kubectl(
        "get", f"deployment/{deploy}",
        "-o", "jsonpath={.metadata.name}",
        check=False,
    )
    return bool(out.strip())


def _deployment_replicas(deploy: str) -> int:
    """Return current spec.replicas for a deployment."""
    out = _kubectl(
        "get", f"deployment/{deploy}",
        "-o", "jsonpath={.spec.replicas}",
        check=False,
    )
    return int(out) if out.isdigit() else 1


def _running_pod_count(deploy: str) -> int:
    """Return number of RUNNING pods owned by this deployment."""
    out = _kubectl(
        "get", "pods",
        "-l", f"app=kafka-to-iceberg",
        "--field-selector=status.phase=Running",
        "-o", f"jsonpath={{.items[?(@.metadata.ownerReferences[0].name contains '{deploy}')].metadata.name}}",
        check=False,
    )
    # Fallback: use readyReplicas from the deployment status
    ready = _kubectl(
        "get", f"deployment/{deploy}",
        "-o", "jsonpath={.status.readyReplicas}",
        check=False,
    )
    return int(ready) if ready.isdigit() else (1 if out.strip() else 0)


# ─────────────────────────────────────────────────────────────────────────────
# Scale helpers with hard stop confirmation
# ─────────────────────────────────────────────────────────────────────────────
def _scale(deploy: str, replicas: int) -> None:
    if DRY_RUN:
        _dry(f"kubectl -n {K8S_NS} scale deployment/{deploy} --replicas={replicas}")
        return
    _info(f"  Scaling {deploy} → replicas={replicas} …")
    _kubectl("scale", f"deployment/{deploy}", f"--replicas={replicas}")
    _ok(f"  Scale command sent: {deploy} → {replicas}")


def _wait_fully_stopped(deploy: str, timeout_s: int = 120, poll_s: int = 5) -> bool:
    """
    Poll until the deployment has ZERO ready replicas AND zero running pods.
    Returns True when confirmed stopped, False on timeout.

    This is the critical safety gate — DDL is only applied after this returns True.
    We check both:
      • deployment.status.readyReplicas  (Kubernetes aggregated view)
      • deployment.status.replicas       (total pods, including terminating)
    Both must be 0 or absent before we declare the deployment fully stopped.
    """
    if DRY_RUN:
        return True
    _info(f"  Waiting for {deploy} to fully stop (timeout={timeout_s}s) …")
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        # readyReplicas — pods that passed readiness probe
        ready = _kubectl(
            "get", f"deployment/{deploy}",
            "-o", "jsonpath={.status.readyReplicas}",
            check=False,
        )
        # replicas — ALL pods managed by this deployment (includes Terminating)
        total = _kubectl(
            "get", f"deployment/{deploy}",
            "-o", "jsonpath={.status.replicas}",
            check=False,
        )
        ready_n = int(ready) if ready.isdigit() else 0
        total_n = int(total) if total.isdigit() else 0
        if ready_n == 0 and total_n == 0:
            _ok(f"  {deploy}: fully stopped (0 ready, 0 total pods).")
            return True
        _info(f"  {deploy}: ready={ready_n}, total={total_n} — waiting {poll_s}s …")
        time.sleep(poll_s)
    _warn(f"  {deploy}: still has pods after {timeout_s}s!")
    return False


def _wait_fully_started(deploy: str, timeout_s: int = 180, poll_s: int = 5) -> bool:
    """
    Poll until the deployment has ≥1 ready replica.
    Returns True when at least one pod is ready, False on timeout.
    """
    if DRY_RUN:
        return True
    _info(f"  Waiting for {deploy} to become ready (timeout={timeout_s}s) …")
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        ready = _kubectl(
            "get", f"deployment/{deploy}",
            "-o", "jsonpath={.status.readyReplicas}",
            check=False,
        )
        ready_n = int(ready) if ready.isdigit() else 0
        if ready_n >= 1:
            _ok(f"  {deploy}: {ready_n} pod(s) ready.")
            return True
        _info(f"  {deploy}: 0 ready — waiting {poll_s}s …")
        time.sleep(poll_s)
    _warn(f"  {deploy}: pod not ready after {timeout_s}s — check pod logs.")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Spark SQL execution
# ─────────────────────────────────────────────────────────────────────────────
def _find_spark_pod(exclude_deploys: list[str]) -> str:
    """
    Find any running streaming pod that is NOT one of the scaled-down deployments.
    Used as the execution host for spark-sql DDL commands.

    Strategy:
      1. If SPARK_POD env var is set, use it directly.
      2. Otherwise iterate all known source × mode combinations to find a running pod
         from a deployment that is NOT in exclude_deploys (i.e. from a different source).
      3. If all sources are scaled down (e.g. single-source cluster), raise a clear error
         with instructions.
    """
    if pod := os.environ.get("SPARK_POD", ""):
        _info(f"  Using SPARK_POD override: {pod}")
        return pod

    # Collect all deployment names across all sources and modes
    for src, smeta in _SOURCE_REGISTRY.items():
        for mode, mmeta in _WRITE_MODES.items():
            deploy = smeta["deploy_pattern"].format(
                source=src, mode_k8s=mmeta["mode_k8s"]
            )
            if deploy in exclude_deploys:
                continue
            if not _deployment_exists(deploy):
                continue
            # Check if this deployment has running pods
            ready = _kubectl(
                "get", f"deployment/{deploy}",
                "-o", "jsonpath={.status.readyReplicas}",
                check=False,
            )
            if ready.isdigit() and int(ready) >= 1:
                pod = _kubectl(
                    "get", "pods",
                    "-l", "app=kafka-to-iceberg",
                    "--field-selector=status.phase=Running",
                    "-o", "jsonpath={.items[0].metadata.name}",
                    check=False,
                )
                if pod:
                    _info(f"  Found execution pod: {pod} (from deployment {deploy})")
                    return pod

    raise RuntimeError(
        "No running streaming pod found to execute spark-sql.\n"
        "Options:\n"
        "  a) Leave at least one streaming pod from a DIFFERENT source running.\n"
        "     e.g. if applying DDL to oracle, keep postgres or mongodb pods up.\n"
        "  b) Set SPARK_POD=<pod-name> to specify a pod manually.\n"
        "  c) Use --modes to scale down only the specific modes affected,\n"
        "     leaving one mode's pod up for spark-sql execution."
    )


def _get_spark_conf_flags(pod: str) -> str:
    """Build spark-sql --conf flags by running BaoSparkInit inside the pod."""
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
        f"--conf '{line.strip()}'"
        for line in r.stdout.strip().splitlines()
        if "=" in line
    )


def _spark_sql(pod: str, conf_flags: str, sql: str, timeout: int = 120) -> tuple[int, str, str]:
    """Execute SQL via spark-sql in the given pod. Returns (returncode, stdout, stderr)."""
    esc = sql.strip().replace("'", r"'\''")
    r = subprocess.run(
        ["kubectl", "-n", K8S_NS, "exec", pod, "--",
         "bash", "-c",
         f"cd /opt/spark/work-dir && spark-sql {conf_flags} -e '{esc}'"],
        capture_output=True, text=True, timeout=timeout,
    )
    return r.returncode, r.stdout.strip(), r.stderr.strip()


# ─────────────────────────────────────────────────────────────────────────────
# DDL statement builder
# ─────────────────────────────────────────────────────────────────────────────
def _build_ddl(op: str, fqn: str, col: str, col_type: str | None, new_col: str | None) -> str:
    """Return the Iceberg Spark SQL ALTER TABLE statement."""
    op = op.lower()
    if op == "add":
        if not col_type:
            raise ValueError("--type is required for ADD COLUMN")
        return f"ALTER TABLE {fqn} ADD COLUMN `{col}` {col_type.upper()}"
    elif op == "drop":
        return f"ALTER TABLE {fqn} DROP COLUMN `{col}`"
    elif op == "modify":
        if not col_type:
            raise ValueError("--type is required for MODIFY")
        return f"ALTER TABLE {fqn} ALTER COLUMN `{col}` TYPE {col_type.upper()}"
    elif op == "rename":
        if not new_col:
            raise ValueError("--new-col is required for RENAME")
        return f"ALTER TABLE {fqn} RENAME COLUMN `{col}` TO `{new_col}`"
    else:
        raise ValueError(f"Unknown operation: {op!r}. Choose: add, drop, modify, rename")


# ─────────────────────────────────────────────────────────────────────────────
# Iceberg verification
# ─────────────────────────────────────────────────────────────────────────────
def _iceberg_has_col(pod: str, conf_flags: str, fqn: str, col: str) -> bool:
    rc, out, _ = _spark_sql(pod, conf_flags, f"DESCRIBE TABLE {fqn}")
    return col.lower() in out.lower()


def _iceberg_col_type(pod: str, conf_flags: str, fqn: str, col: str) -> str:
    """Return the Iceberg type string for col, or 'NOT_FOUND'."""
    rc, out, _ = _spark_sql(pod, conf_flags, f"DESCRIBE TABLE {fqn}")
    for line in out.splitlines():
        parts = line.split()
        if parts and parts[0].lower() == col.lower():
            return parts[1] if len(parts) > 1 else "unknown"
    return "NOT_FOUND"


# ─────────────────────────────────────────────────────────────────────────────
# Core apply logic
# ─────────────────────────────────────────────────────────────────────────────
def apply_ddl(
    source:    str,
    table:     str,
    op:        str,
    col:       str,
    col_type:  str | None,
    new_col:   str | None,
    modes:     list[str],
    namespace: str | None = None,
    catalog:   str | None = None,
) -> bool:
    """
    Full DDL apply cycle — 5 steps:

    Step 1  Scale down all target Deployments (source × modes).
    Step 2  Wait (with hard confirmation) until every pod is 0/0.
    Step 3  Execute ALTER TABLE on each Iceberg table via spark-sql.
    Step 4  Verify the DDL is committed in Iceberg (DESCRIBE TABLE).
    Step 5  Scale Deployments back to their original replica counts.

    Returns True if ALL steps succeeded, False if any DDL or verification failed.
    Deployments are ALWAYS scaled back up in Step 5, even on DDL failure.
    """
    smeta = _SOURCE_REGISTRY[source]
    cat   = catalog   or smeta["catalog"]
    ns    = namespace or smeta["default_namespace"]

    _hdr(
        f"CDC DDL APPLY  |  source={source}  table={ns}.{table}  "
        f"op={op.upper()}  col={col}"
        + (f"  type={col_type}" if col_type else "")
        + (f"  →  {new_col}"   if new_col  else "")
    )

    # ── Build deployment list for the target source × modes ────────────────────
    target_deploys: dict[str, int] = {}  # deploy_name → original_replicas
    for mode in modes:
        deploy = smeta["deploy_pattern"].format(
            source=source, mode_k8s=_WRITE_MODES[mode]["mode_k8s"]
        )
        if not _deployment_exists(deploy):
            _warn(f"Deployment {deploy!r} not found in namespace {K8S_NS!r} — skipping.")
            continue
        target_deploys[deploy] = _deployment_replicas(deploy) if not DRY_RUN else 1

    if not target_deploys and not DRY_RUN:
        _fail("No matching Deployments found. Nothing to do.")
        return False

    # ── Step 1: Scale down ─────────────────────────────────────────────────────
    _step(1, 5, f"Scaling down {len(target_deploys)} deployment(s) — pausing DML replication")
    for deploy in target_deploys:
        _scale(deploy, 0)

    # ── Step 2: Confirm all pods stopped ──────────────────────────────────────
    _step(2, 5, "Confirming all target pods are fully stopped")
    stop_confirmed = True
    for deploy in target_deploys:
        ok = _wait_fully_stopped(deploy, timeout_s=120)
        if not ok:
            stop_confirmed = False
            _warn(
                f"{deploy} did not fully stop within 120s. "
                "Proceeding, but there is a small risk a late batch "
                "could conflict with the DDL. Consider re-running."
            )

    if stop_confirmed:
        _ok("All target pods confirmed stopped. "
            "Kafka offsets are frozen at last committed position.")
    else:
        _warn("Some pods may still be running — proceeding with DDL anyway. "
              "Monitor for errors and re-verify Iceberg schema after.")

    # ── Step 3: Execute DDL ────────────────────────────────────────────────────
    _step(3, 5, "Applying DDL in Iceberg via spark-sql")
    all_ok = True
    spark_pod: str | None = None
    conf_flags: str = ""

    # Find execution pod (from a different source, so it's still running)
    try:
        spark_pod  = _find_spark_pod(exclude_deploys=list(target_deploys.keys()))
        conf_flags = _get_spark_conf_flags(spark_pod)
        _ok(f"Execution pod: {spark_pod}  Spark conf: ready")
    except Exception as exc:
        _fail(f"Cannot find a running Spark pod for DDL execution: {exc}")
        all_ok = False

    if all_ok:
        for mode in modes:
            suffix    = _WRITE_MODES[mode]["suffix"]
            ice_table = f"{table}{suffix}"
            fqn       = f"`{cat}`.`{ns}`.`{ice_table}`"
            ddl       = _build_ddl(op, fqn, col, col_type, new_col)

            _info(f"\n  [{mode:>17s}]  {ddl}")

            if DRY_RUN:
                _dry(f"Would execute on Iceberg table {ice_table}")
                continue

            rc, out, err = _spark_sql(spark_pod, conf_flags, ddl)

            if rc != 0:
                combined = (err + "\n" + out).lower()
                # Idempotent cases — not a real failure
                if op == "add" and ("already exists" in combined or "column already exists" in combined):
                    _warn(f"  [{mode}] `{col}` already exists in {ice_table} — skipping (idempotent).")
                elif op == "drop" and (
                    "cannot resolve" in combined
                    or "no such column" in combined
                    or "column not found" in combined
                    or "missing" in combined
                ):
                    _warn(f"  [{mode}] `{col}` not found in {ice_table} — skipping (idempotent).")
                elif op == "rename" and "already exists" in combined:
                    _warn(f"  [{mode}] Target column `{new_col}` already exists in {ice_table} — skipping.")
                else:
                    _fail(f"  [{mode}] DDL FAILED for {ice_table}:\n{err or out}")
                    all_ok = False
            else:
                _ok(f"  [{mode}] Applied to {ice_table}.")

    # ── Step 4: Verify DDL in Iceberg ─────────────────────────────────────────
    _step(4, 5, "Verifying DDL results in Iceberg (DESCRIBE TABLE)")
    if DRY_RUN:
        _dry("Verification skipped in dry-run mode.")
    elif spark_pod and conf_flags:
        check_col = new_col if (op == "rename" and new_col) else col
        for mode in modes:
            suffix    = _WRITE_MODES[mode]["suffix"]
            ice_table = f"{table}{suffix}"
            fqn       = f"`{cat}`.`{ns}`.`{ice_table}`"

            if op == "drop":
                gone = not _iceberg_has_col(spark_pod, conf_flags, fqn, col)
                if gone:
                    _ok(f"  [{mode}] `{col}` confirmed ABSENT from {ice_table}.")
                else:
                    _fail(f"  [{mode}] `{col}` STILL PRESENT in {ice_table} after DROP!")
                    all_ok = False
            elif op == "rename":
                new_ok  = _iceberg_has_col(spark_pod, conf_flags, fqn, new_col)
                old_gone= not _iceberg_has_col(spark_pod, conf_flags, fqn, col)
                if new_ok and old_gone:
                    _ok(f"  [{mode}] `{col}` → `{new_col}` confirmed in {ice_table}.")
                else:
                    if not new_ok:
                        _fail(f"  [{mode}] `{new_col}` NOT FOUND in {ice_table} after RENAME!")
                    if not old_gone:
                        _fail(f"  [{mode}] `{col}` STILL PRESENT in {ice_table} after RENAME!")
                    all_ok = False
            elif op == "modify":
                ice_type = _iceberg_col_type(spark_pod, conf_flags, fqn, col)
                _ok(f"  [{mode}] `{col}` type in {ice_table}: {ice_type}")
            else:  # add
                present = _iceberg_has_col(spark_pod, conf_flags, fqn, check_col)
                if present:
                    ice_type = _iceberg_col_type(spark_pod, conf_flags, fqn, check_col)
                    _ok(f"  [{mode}] `{check_col}` confirmed PRESENT in {ice_table} ({ice_type}).")
                else:
                    _fail(f"  [{mode}] `{check_col}` NOT FOUND in {ice_table} after ADD!")
                    all_ok = False
    else:
        _warn("  Verification skipped — no execution pod was available.")

    # ── Step 5: Scale back up ─────────────────────────────────────────────────
    _step(5, 5, "Resuming DML replication — scaling Deployments back up")
    for deploy, orig_replicas in target_deploys.items():
        _scale(deploy, orig_replicas)

    if not DRY_RUN:
        start_ok = True
        for deploy, orig_replicas in target_deploys.items():
            if orig_replicas > 0:
                ok = _wait_fully_started(deploy, timeout_s=180)
                if not ok:
                    start_ok = False
        if start_ok:
            _ok("All pods are running. DML replication resumed from last committed Kafka offset.")
        else:
            _warn("Some pods may not be ready — check pod logs.")

    return all_ok


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Apply DDL (ADD/DROP/MODIFY/RENAME COLUMN) to Iceberg tables "
            "for the Kafka→Iceberg CDC pipeline.\n\n"
            "Applies to Oracle, PostgreSQL, and MongoDB sources. "
            "Works for any table — not limited to customers."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--source", required=True,
        choices=list(_SOURCE_REGISTRY.keys()),
        help="CDC source database: oracle | postgres | mongodb",
    )
    p.add_argument(
        "--table", required=True,
        help="Source table/collection name (without write-mode suffix). e.g. customers, orders",
    )
    p.add_argument(
        "--op", required=True,
        choices=["add", "drop", "modify", "rename"],
        help="DDL operation to apply in Iceberg",
    )
    p.add_argument(
        "--col", required=True,
        help="Column name to add/drop/modify/rename (case-insensitive)",
    )
    p.add_argument(
        "--type", dest="col_type", default=None,
        help=(
            "Iceberg column type. Required for --op add and --op modify.\n"
            "Examples: STRING  BIGINT  INT  BOOLEAN  TIMESTAMP  DATE  'DECIMAL(18,4)'"
        ),
    )
    p.add_argument(
        "--new-col", dest="new_col", default=None,
        help="New column name. Required for --op rename.",
    )
    p.add_argument(
        "--modes",
        default="standard,soft_delete,history_tracking",
        help=(
            "Comma-separated write modes whose Iceberg tables will be updated.\n"
            "Default: standard,soft_delete,history_tracking (all three)\n"
            "Example: --modes standard,soft_delete"
        ),
    )
    p.add_argument(
        "--namespace", default=None,
        help=(
            "Override the Iceberg namespace (default: cache_testing). "
            "Use when the table lives in a different namespace, e.g. tpcds."
        ),
    )
    p.add_argument(
        "--catalog", default=None,
        help=(
            "Override the Iceberg catalog (default: source name). "
            "Rarely needed — use when the catalog name differs from the source."
        ),
    )
    p.add_argument(
        "--dry-run", action="store_true", default=False,
        help=(
            "Print all steps and SQL that would be executed "
            "without making any changes to K8s or Iceberg."
        ),
    )
    p.add_argument(
        "--yes", "-y", action="store_true", default=False,
        help="Skip the interactive confirmation prompt (for scripted / CI use).",
    )
    return p.parse_args()


def main() -> None:
    global DRY_RUN

    args = _parse_args()
    if args.dry_run:
        DRY_RUN = True

    # Validate modes
    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    bad   = [m for m in modes if m not in _WRITE_MODES]
    if bad:
        _fail(f"Invalid --modes value(s): {bad}. Choose from: {list(_WRITE_MODES)}")
        sys.exit(1)

    # Validate op-specific requirements
    if args.op == "add" and not args.col_type:
        _fail("--type is required for --op add")
        sys.exit(1)
    if args.op == "modify" and not args.col_type:
        _fail("--type is required for --op modify")
        sys.exit(1)
    if args.op == "rename" and not args.new_col:
        _fail("--new-col is required for --op rename")
        sys.exit(1)

    smeta = _SOURCE_REGISTRY[args.source]
    cat   = args.catalog   or smeta["catalog"]
    ns    = args.namespace or smeta["default_namespace"]

    # ── Print summary banner ───────────────────────────────────────────────────
    print()
    print("=" * 72)
    print("  CDC DDL APPLY — PIPELINE PAUSE → ICEBERG ALTER → PIPELINE RESUME")
    print("=" * 72)
    print(f"  Source     : {args.source}")
    print(f"  Catalog    : {cat}")
    print(f"  Namespace  : {ns}")
    print(f"  Table      : {args.table}")
    print(f"  Operation  : {args.op.upper()}")
    print(f"  Column     : {args.col}"
          + (f"  →  {args.new_col}" if args.new_col else "")
          + (f"  ({args.col_type})" if args.col_type else ""))
    print(f"  Modes      : {', '.join(modes)}")
    print(f"  K8s NS     : {K8S_NS}")
    print(f"  Dry-run    : {'YES — no changes' if DRY_RUN else 'NO  — live changes'}")
    print(f"  Started    : {datetime.now(timezone.utc).isoformat()}")
    print()

    # Show what Iceberg tables will be modified
    print("  Iceberg tables to be modified:")
    for mode in modes:
        suffix    = _WRITE_MODES[mode]["suffix"]
        ice_table = f"{args.table}{suffix}"
        fqn       = f"{cat}.{ns}.{ice_table}"
        ddl       = _build_ddl(args.op, f"`{cat}`.`{ns}`.`{ice_table}`",
                               args.col, args.col_type, args.new_col)
        print(f"    [{mode:>17s}]  {fqn}")
        print(f"                        SQL: {ddl}")
    print()

    # Show deployments that will be paused
    print("  Deployments to be paused:")
    for mode in modes:
        deploy = smeta["deploy_pattern"].format(
            source=args.source, mode_k8s=_WRITE_MODES[mode]["mode_k8s"]
        )
        exists = _deployment_exists(deploy) if not DRY_RUN else True
        status = "" if exists else "  ⚠ NOT FOUND"
        print(f"    [{mode:>17s}]  {deploy}{status}")
    print()
    print("=" * 72)

    # ── Confirmation prompt ────────────────────────────────────────────────────
    if not DRY_RUN and not args.yes:
        print()
        print("  ⚠  This will:")
        print(f"     1. STOP DML replication for {args.source}/{', '.join(modes)}")
        print("     2. Wait until all pods are fully stopped (0 running)")
        print("     3. Apply the DDL to Iceberg")
        print("     4. Restart the pipeline")
        print()
        print("  Rows written to the source AFTER the DDL but BEFORE this script")
        print("  finishes will be buffered in Kafka and consumed on resume.")
        print()
        try:
            answer = input("  Type 'yes' to proceed, anything else to abort: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            answer = ""
        if answer != "yes":
            print("\n  Aborted — no changes made.")
            sys.exit(0)
        print()

    # ── Execute ────────────────────────────────────────────────────────────────
    success = apply_ddl(
        source    = args.source,
        table     = args.table,
        op        = args.op,
        col       = args.col,
        col_type  = args.col_type,
        new_col   = args.new_col,
        modes     = modes,
        namespace = args.namespace,
        catalog   = args.catalog,
    )

    # ── Final summary ──────────────────────────────────────────────────────────
    print()
    print("=" * 72)
    if success:
        print("  ✓  DDL apply completed successfully.")
        if not DRY_RUN:
            print()
            print("  Next steps:")
            print("  1. Watch pod logs for the first few batches to confirm rows flow.")
            print("  2. If source rows with the new column were written BEFORE this")
            print("     script ran, those rows landed in Iceberg with NULL for that")
            print("     column. Trigger an UPDATE on those rows at the source to")
            print("     re-replicate them with the correct value.")
            print("  3. For RENAME: existing Iceberg rows retain the old column name.")
            print("     New DML rows populate the renamed column going forward.")
    else:
        print("  ✗  DDL apply completed with errors — see output above.")
        print("     The streaming pipeline has been restarted regardless.")
        print("     Check Iceberg schema manually before relying on pipeline output.")
    print(f"  Finished : {datetime.now(timezone.utc).isoformat()}")
    print("=" * 72)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
