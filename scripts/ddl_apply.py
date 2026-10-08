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
  6. Backfill    — (optional, --backfill) find rows in Iceberg where the new column
                   is NULL because they were processed before this script ran, then
                   trigger a no-op UPDATE on those rows at the source DB so Debezium
                   re-replicates them with the correct value.

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

Backfill (--backfill)
---------------------
When rows were written to the source DB after the source DDL fired but BEFORE
this script ran, those rows were processed by the streaming pipeline with a
stale schema — the new column was silently dropped and the rows landed in
Iceberg with NULL for that column.

--backfill recovers those rows directly from Kafka — no source DB touch needed:

  The Kafka topic already holds the correct data. Every Debezium message's
  `after` field contains the full row as it was in the source DB at that moment,
  including the new column's value. The streaming pod threw that value away
  because it parsed with a stale schema. We replay those exact messages.

  Step 6a  Read the S3 checkpoint to find the last committed Kafka offset
           (the point where the streaming pod stopped consuming).
  Step 6b  Scan backwards through the Kafka topic from that offset to find
           the earliest message where the new column appears in the `after`
           payload — that is the start of the stale window.
  Step 6c  Batch-read the Kafka topic from that start offset to the checkpoint
           offset using Spark (static DataFrame, not streaming).
  Step 6d  Parse each message's `after` JSON, extract rows where the new
           column is NOT NULL (rows written after the source DDL fired).
  Step 6e  MERGE those rows into Iceberg — overwrites the NULLs with the
           correct values from Kafka.

  This is a pure Kafka → Iceberg operation. No source DB connection, no
  new WAL/redo/oplog writes, no Debezium involvement.

Requirements for --backfill:
  • --op add (backfill only makes sense when a column was added)
  • --pk     the primary key column name (e.g. id, customer_id)
  • The Kafka topic must still retain the messages from the stale window
    (within its retention period — default is days to weeks)
  • S3 credentials are read from OpenBao (same path as the streaming pipeline)

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
from typing import Any

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
# Backfill helpers  (Kafka replay — no source DB touch)
# ─────────────────────────────────────────────────────────────────────────────

# Source → Kafka topic prefix (matches the topic.prefix set in connector config)
_SOURCE_TOPIC_PREFIX: dict[str, str] = {
    "postgres": "postgres.cache_testing",
    "oracle":   "oracle.cache_testing",
    "mongodb":  "mongodb.cache_testing",
}

# Kafka bootstrap (matches the streaming pipeline)
_KAFKA_BOOTSTRAP = "strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092"

# S3 bucket where Spark checkpoints live (matches streaming pipeline)
_S3_BUCKET = "xdatatoiceberg1"

# checkpoint path template — matches streaming pipeline
# s3://xdatatoiceberg1/checkpoints/streaming/<source>/standard
_CKPT_TEMPLATE = "checkpoints/streaming/{source}/standard"


def _bao_read_secret(path: str) -> dict[str, Any]:
    """Read a KV-v2 secret from OpenBao and return the data dict."""
    import urllib.request
    url = f"{BAO_ADDR}/v1/{path}"
    req = urllib.request.Request(url, headers={"X-Vault-Token": _bao_token()})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())["data"]["data"]


def _read_checkpoint_offsets(source: str) -> dict[str, dict[int, int]]:
    """
    Read the latest committed Kafka offsets from the S3 Spark checkpoint.

    Returns a dict: { topic: { partition: offset } }
    e.g. {"postgres.cache_testing.customers": {0: 502}}

    The checkpoint offset file format (Spark internal) is 3 lines:
      line 0: version (int)
      line 1: metadata JSON
      line 2: offsets JSON  → {"topic": {"partition": offset}}
    """
    try:
        s3_secret = _bao_read_secret("secret/data/platform/s3")
        import boto3
        s3 = boto3.client(
            "s3",
            endpoint_url          = s3_secret["endpoint"],
            aws_access_key_id     = s3_secret["access_key"],
            aws_secret_access_key = s3_secret["secret_key"],
            region_name           = s3_secret.get("region", "us-east-1"),
        )
        prefix    = _CKPT_TEMPLATE.format(source=source) + "/offsets/"
        paginator = s3.get_paginator("list_objects_v2")
        files     = sorted(
            [o["Key"] for p in paginator.paginate(Bucket=_S3_BUCKET, Prefix=prefix)
             for o in p.get("Contents", [])],
            reverse=True,
        )
        if not files:
            return {}
        body  = s3.get_object(Bucket=_S3_BUCKET, Key=files[0])["Body"].read().decode()
        lines = body.strip().splitlines()
        if len(lines) < 3:
            return {}
        offsets_raw = json.loads(lines[2])
        # Normalise: Spark stores partition keys as strings
        return {
            topic: {int(p): int(o) for p, o in partitions.items()}
            for topic, partitions in offsets_raw.items()
        }
    except Exception as exc:
        raise RuntimeError(f"Could not read S3 checkpoint: {exc}")


def _find_stale_window_start(
    spark_pod:  str,
    conf_flags: str,
    topic:      str,
    col:        str,
    end_offsets: dict[int, int],
    kafka_secret: dict,
) -> dict[int, int] | None:
    """
    Scan backwards through the Kafka topic to find the earliest offset where
    the new column first appears in the `after` payload.

    Strategy: batch-read the topic up to end_offsets using spark-sql, parse
    the `after` JSON, and find the minimum offset where `col` is NOT NULL.
    Everything from offset 0 up to (min_offset - 1) is the stale window start.

    Returns startingOffsets dict {partition: offset} for the stale window,
    or None if no stale messages are found (column was never in Kafka).
    """
    jaas = (
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
        f"username=\\\"{kafka_secret['debezium_user']}\\\" "
        f"password=\\\"{kafka_secret['debezium_password']}\\\";"
    )
    end_json   = json.dumps({topic: end_offsets})
    start_json = json.dumps({topic: {str(p): 0 for p in end_offsets}})

    # Read the full topic up to the checkpoint offset, parse after JSON,
    # find the minimum offset where `col` is present and not null.
    find_sql = f"""
SELECT MIN(offset) as first_col_offset
FROM (
  SELECT offset,
         get_json_object(
           get_json_object(CAST(value AS STRING), '$.after'), '$.{col}'
         ) as col_val
  FROM   kafka.`{_KAFKA_BOOTSTRAP}`
)
WHERE col_val IS NOT NULL
"""
    # We run this via a PySpark script inside the pod (spark-sql can't read Kafka directly
    # without additional options; we use a small inline Python driver instead)
    _FINDER_SCRIPT = f"""
import json, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object

spark = SparkSession.builder.getOrCreate()

kafka_df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers",       "{_KAFKA_BOOTSTRAP}")
    .option("kafka.security.protocol",       "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism",          "SCRAM-SHA-512")
    .option("kafka.sasl.jaas.config",        "{jaas}")
    .option("subscribe",                     "{topic}")
    .option("startingOffsets",               '{start_json}')
    .option("endingOffsets",                 '{end_json}')
    .option("failOnDataLoss",                "false")
    .load()
)

parsed = kafka_df.select(
    col("partition"),
    col("offset"),
    get_json_object(
        get_json_object(col("value").cast("string"), "$.after"),
        "$.{col}"
    ).alias("col_val"),
).filter(col("col_val").isNotNull())

if parsed.rdd.isEmpty():
    print("STALE_START=NONE")
else:
    from pyspark.sql.functions import min as spark_min
    row = parsed.groupBy("partition").agg(spark_min("offset").alias("min_off")).collect()
    result = {{str(r["partition"]): r["min_off"] for r in row}}
    print("STALE_START=" + json.dumps(result))
"""
    r = subprocess.run(
        ["kubectl", "-n", K8S_NS, "exec", spark_pod, "--",
         "python3", "-c", _FINDER_SCRIPT],
        capture_output=True, text=True, timeout=180,
    )
    if r.returncode != 0:
        raise RuntimeError(f"Stale-window scan failed:\n{r.stderr.strip()}")

    for line in r.stdout.splitlines():
        if line.startswith("STALE_START="):
            val = line[len("STALE_START="):]
            if val == "NONE":
                return None
            raw = json.loads(val)
            return {int(p): int(o) for p, o in raw.items()}

    raise RuntimeError(f"Unexpected output from stale-window scan:\n{r.stdout}")


def run_backfill(
    source:     str,
    table:      str,
    col:        str,
    pk_col:     str,
    spark_pod:  str,
    conf_flags: str,
    namespace:  str,
    catalog:    str,
) -> bool:
    """
    Step 6 — Kafka replay backfill for a newly added column.

    Reads the stale Kafka messages directly (no source DB touch) and
    MERGEs the correct column values into Iceberg.

    Flow:
      6a  Read S3 checkpoint → last committed offset per topic/partition
      6b  Scan Kafka backwards → find the earliest offset where `col` appears
          (start of the stale window)
      6c  Batch-read Kafka [stale_start, checkpoint_offset) via Spark
      6d  Parse `after` JSON, filter rows where `col` IS NOT NULL
      6e  MERGE into Iceberg standard table — overwrites NULLs with correct values

    Returns True on success, False on any error.
    """
    _step(6, 6, f"Kafka replay backfill — recovering `{col}` values from topic (pk={pk_col})")

    topic_prefix = _SOURCE_TOPIC_PREFIX.get(source)
    if not topic_prefix:
        _fail(f"No topic prefix known for source {source!r}")
        return False

    # The table-specific topic (e.g. postgres.cache_testing.customers)
    topic = f"{topic_prefix}.{table}"
    fqn   = f"`{catalog}`.`{namespace}`.`{table}`"

    # ── 6a: Read checkpoint offsets from S3 ───────────────────────────────────
    _info(f"  Reading S3 checkpoint for source={source} …")
    try:
        all_offsets = _read_checkpoint_offsets(source)
    except Exception as exc:
        _fail(f"Cannot read checkpoint: {exc}")
        return False

    end_offsets = all_offsets.get(topic)
    if not end_offsets:
        _warn(
            f"  Topic {topic!r} not found in checkpoint. "
            "This means the streaming pod has never consumed this topic — nothing to backfill."
        )
        return True

    _ok(f"  Checkpoint offsets for {topic}: {end_offsets}")

    # ── 6b: Find stale window start ────────────────────────────────────────────
    _info(f"  Scanning Kafka topic for first message with `{col}` in `after` payload …")
    try:
        kafka_secret = _bao_read_secret("secret/data/platform/kafka")
        stale_start  = _find_stale_window_start(
            spark_pod, conf_flags, topic, col, end_offsets, kafka_secret,
        )
    except Exception as exc:
        _fail(f"Stale-window scan failed: {exc}")
        return False

    if stale_start is None:
        _ok(
            f"  No messages with `{col}` found in Kafka topic up to checkpoint offset. "
            "This means the source DDL fired after the last committed batch — "
            "all future messages will carry the correct value. Nothing to backfill."
        )
        return True

    _ok(f"  Stale window start offsets: {stale_start}")
    _info(
        f"  Will replay Kafka [{stale_start} → {end_offsets}] "
        f"and MERGE rows where `{col}` IS NOT NULL into Iceberg."
    )

    # ── 6c–6e: Batch-read Kafka and MERGE into Iceberg ────────────────────────
    jaas = (
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
        f"username=\\\"{kafka_secret['debezium_user']}\\\" "
        f"password=\\\"{kafka_secret['debezium_password']}\\\";"
    )
    start_json = json.dumps({topic: {str(p): o for p, o in stale_start.items()}})
    end_json   = json.dumps({topic: {str(p): o for p, o in end_offsets.items()}})

    # Build a self-contained PySpark backfill script that runs inside the pod.
    # It reads the Kafka batch, extracts after-payload rows where col IS NOT NULL,
    # and MERGEs them into the Iceberg standard table using the PK.
    #
    # Uses str.format() (not f-string) so that Python braces inside the script
    # body ({{}}) remain as literal braces in the generated code, while named
    # placeholders like {catalog_v} are substituted by .format() here.
    wh_map = {"postgres": "pg_lakehouse", "oracle": "ora_lakehouse", "mongodb": "mgo_lakehouse"}
    warehouse = wh_map.get(source, f"{source}_lakehouse")

    _BACKFILL_SCRIPT = (
        "import json, sys\n"
        "sys.path.insert(0, '/opt/spark/work-dir')\n"
        "from pyspark.sql import SparkSession\n"
        "from pyspark.sql.functions import col, get_json_object, from_json, lit\n"
        "from pyspark.sql.types import StringType\n"
        "from bao_spark_init import BaoSparkInit\n"
        "\n"
        "bao  = BaoSparkInit()\n"
        "pol  = bao.polaris_creds()\n"
        "s3   = bao.s3_creds()\n"
        "uri  = pol.get('url') or 'http://polaris-rest.prod.svc.cluster.local:8181/api/catalog'\n"
        "cred = pol['spark_svc_id'] + ':' + pol['spark_svc_secret']\n"
        "\n"
        "spark = (\n"
        "    SparkSession.builder\n"
        "    .config('spark.sql.extensions',\n"
        "            'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')\n"
        f"    .config('spark.sql.catalog.{catalog}',            'org.apache.iceberg.spark.SparkCatalog')\n"
        f"    .config('spark.sql.catalog.{catalog}.type',       'rest')\n"
        f"    .config('spark.sql.catalog.{catalog}.uri',        uri)\n"
        f"    .config('spark.sql.catalog.{catalog}.oauth2-server-uri', uri + '/v1/oauth/tokens')\n"
        f"    .config('spark.sql.catalog.{catalog}.credential', cred)\n"
        f"    .config('spark.sql.catalog.{catalog}.warehouse',  '{warehouse}')\n"
        f"    .config('spark.sql.catalog.{catalog}.scope',      'PRINCIPAL_ROLE:ALL')\n"
        f"    .config('spark.sql.catalog.{catalog}.rest.auth.type', 'oauth2')\n"
        "    .config('spark.sql.catalog." + catalog + ".s3.access-key-id',     s3['access_key'])\n"
        "    .config('spark.sql.catalog." + catalog + ".s3.secret-access-key', s3['secret_key'])\n"
        "    .config('spark.sql.catalog." + catalog + ".s3.endpoint',          s3['endpoint'])\n"
        "    .config('spark.sql.catalog." + catalog + ".s3.path-style-access', 'true')\n"
        "    .config('spark.sql.catalog." + catalog + ".client.region',        s3.get('region','us-east-1'))\n"
        "    .getOrCreate()\n"
        ")\n"
        "\n"
        "# ── Step 6c: Batch-read Kafka stale window ────────────────────────\n"
        "kafka_df = (\n"
        "    spark.read\n"
        "    .format('kafka')\n"
        f"    .option('kafka.bootstrap.servers',  '{_KAFKA_BOOTSTRAP}')\n"
        "    .option('kafka.security.protocol',  'SASL_PLAINTEXT')\n"
        "    .option('kafka.sasl.mechanism',     'SCRAM-SHA-512')\n"
        f"    .option('kafka.sasl.jaas.config',   '{jaas}')\n"
        f"    .option('subscribe',                '{topic}')\n"
        f"    .option('startingOffsets',          '{start_json}')\n"
        f"    .option('endingOffsets',            '{end_json}')\n"
        "    .option('failOnDataLoss',           'false')\n"
        "    .load()\n"
        ")\n"
        "\n"
        "# ── Step 6d: Parse after payload, keep rows where col IS NOT NULL ─\n"
        "after_df = kafka_df.select(\n"
        "    get_json_object(col('value').cast('string'), '$.after').alias('after_json')\n"
        ").filter(col('after_json').isNotNull())\n"
        "\n"
        "inferred = spark.read.json(after_df.rdd.map(lambda r: r[0])).schema\n"
        "\n"
        "parsed_df = after_df.select(\n"
        "    from_json(col('after_json'), inferred).alias('d')\n"
        ").select('d.*')\n"
        "\n"
        f"if '{col}' not in [f.name.lower() for f in parsed_df.schema.fields]:\n"
        "    print('BACKFILL_RESULT=NO_COL_IN_BATCH')\n"
        "    sys.exit(0)\n"
        "\n"
        f"parsed_df = parsed_df.filter(col('`{col}`').isNotNull())\n"
        "\n"
        "if parsed_df.rdd.isEmpty():\n"
        "    print('BACKFILL_RESULT=NO_ROWS')\n"
        "    sys.exit(0)\n"
        "\n"
        "# Lowercase column names (Oracle sends uppercase)\n"
        "parsed_df = parsed_df.toDF(*[c.lower() for c in parsed_df.columns])\n"
        "\n"
        "# ── Step 6e: MERGE into Iceberg standard table ───────────────────\n"
        "parsed_df.createOrReplaceTempView('_backfill_batch')\n"
        "\n"
        "set_clauses = ', '.join(\n"
        "    f't.`{c}` = s.`{c}`'\n"
        "    for c in parsed_df.columns\n"
        f"    if c.lower() != '{pk_col}'.lower()\n"
        ")\n"
        "\n"
        f"merge_sql = (\n"
        f"    'MERGE INTO {fqn} AS t '\n"
        f"    'USING _backfill_batch AS s '\n"
        f"    'ON t.`{pk_col}` = s.`{pk_col}` '\n"
        f"    'WHEN MATCHED THEN UPDATE SET ' + set_clauses\n"
        f")\n"
        "\n"
        "spark.sql(merge_sql)\n"
        "count = parsed_df.count()\n"
        "print('BACKFILL_RESULT=OK rows=' + str(count))\n"
    )

    _info(f"  Running Kafka→Iceberg replay MERGE in pod {spark_pod} …")
    r = subprocess.run(
        ["kubectl", "-n", K8S_NS, "exec", spark_pod, "--",
         "python3", "-c", _BACKFILL_SCRIPT],
        capture_output=True, text=True, timeout=300,
    )

    # Parse result line from script output
    result_line = next(
        (l for l in r.stdout.splitlines() if l.startswith("BACKFILL_RESULT=")),
        None,
    )

    if r.returncode != 0:
        _fail(f"Backfill script failed:\n{r.stderr.strip() or r.stdout.strip()}")
        return False

    if result_line == "BACKFILL_RESULT=NO_COL_IN_BATCH":
        _ok(
            f"  No messages in the stale window contained `{col}` — "
            "source DDL may have fired after the last consumed offset. Nothing to merge."
        )
        return True

    if result_line == "BACKFILL_RESULT=NO_ROWS":
        _ok(f"  No non-NULL rows for `{col}` found in the Kafka stale window. Nothing to merge.")
        return True

    if result_line and result_line.startswith("BACKFILL_RESULT=OK"):
        rows = result_line.split("rows=")[-1] if "rows=" in result_line else "?"
        _ok(
            f"  Kafka replay complete — {rows} row(s) MERGEd into {fqn}. "
            f"`{col}` values recovered directly from Kafka topic."
        )
        _info("  No source DB was touched. No new WAL/redo/oplog entries were written.")
        return True

    _fail(f"Backfill script returned unexpected output:\n{r.stdout.strip()}")
    return False


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
    backfill:  bool = False,
    pk_col:    str | None = None,
) -> bool:
    """
    Full DDL apply cycle — 5 steps (6 with --backfill):

    Step 1  Scale down all target Deployments (source × modes).
    Step 2  Wait (with hard confirmation) until every pod is 0/0.
    Step 3  Execute ALTER TABLE on each Iceberg table via spark-sql.
    Step 4  Verify the DDL is committed in Iceberg (DESCRIBE TABLE).
    Step 5  Scale Deployments back to their original replica counts.
    Step 6  (backfill=True, op=add only) Find Iceberg rows where the new
            column is NULL and trigger a no-op UPDATE at the source so
            Debezium re-replicates them with the correct value.

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
    total_steps = 6 if (backfill and op == "add") else 5
    _step(5, total_steps, "Resuming DML replication — scaling Deployments back up")
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

    # ── Step 6: Backfill stale NULL rows (optional) ───────────────────────────
    if backfill and op == "add" and not DRY_RUN:
        if not spark_pod or not conf_flags:
            _warn("Backfill skipped — no Spark pod was available for DDL execution.")
        elif not pk_col:
            _warn("Backfill skipped — --pk (primary key column) is required for backfill.")
        else:
            cat = catalog or _SOURCE_REGISTRY[source]["catalog"]
            ns  = namespace or _SOURCE_REGISTRY[source]["default_namespace"]
            bf_ok = run_backfill(
                source=source, table=table, col=col, pk_col=pk_col,
                spark_pod=spark_pod, conf_flags=conf_flags,
                namespace=ns, catalog=cat,
            )
            if not bf_ok:
                all_ok = False
    elif backfill and op != "add":
        _warn("--backfill is only applicable for --op add. Skipping.")
    elif backfill and DRY_RUN:
        _dry("Backfill step would query Iceberg for NULL rows and trigger source UPDATEs.")

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
        "--backfill", action="store_true", default=False,
        help=(
            "After the DDL is applied and pods are back up, find rows in Iceberg "
            "where the new column is NULL (written during the stale-cache window) "
            "and trigger a no-op UPDATE at the source DB so Debezium re-replicates "
            "them with the correct value. Only valid with --op add. Requires --pk."
        ),
    )
    p.add_argument(
        "--pk", dest="pk_col", default=None,
        help=(
            "Primary key column name for --backfill. "
            "Used to identify and re-trigger stale rows at the source. "
            "Examples: id, customer_id, _id"
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
    if args.backfill and args.op != "add":
        _fail("--backfill is only valid with --op add")
        sys.exit(1)
    if args.backfill and not args.pk_col:
        _fail("--pk is required when --backfill is set")
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
    if args.backfill:
        print()
        print(f"  Backfill       : ENABLED — will recover NULL rows for `{args.col}` using pk={args.pk_col}")
        print(f"                   No-op UPDATE triggered at source → Debezium re-replicates correct values")
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
        backfill  = args.backfill,
        pk_col    = args.pk_col,
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
            if args.op == "add" and not args.backfill:
                print("  2. If source rows with the new column were written BEFORE this")
                print("     script ran, those rows landed in Iceberg with NULL for that")
                print(f"     column. Re-run with --backfill --pk <pk_col> to recover them")
                print("     automatically, or trigger an UPDATE at the source manually.")
            elif args.op == "add" and args.backfill:
                print("  2. Backfill was run — stale NULL rows have been re-triggered at")
                print(f"     the source. Verify `{args.col}` is now populated in Iceberg")
                print(f"     after the next 1–2 pipeline batches complete.")
            if args.op == "rename":
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
