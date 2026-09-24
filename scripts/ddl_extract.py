#!/usr/bin/env python3
"""
scripts/ddl_extract.py
======================
Read pending DDL events from the Debezium schema-changes Kafka topics and
print ready-to-run ddl_apply.py commands.

Purpose
-------
When the Kafka→Iceberg pipeline stops with a SCHEMA MISMATCH error it means
a DDL change (ALTER TABLE / MongoDB field addition) was applied at the source
but ddl_apply.py has NOT been run yet.

This script:
  1. Connects to each Debezium schema-changes topic:
       schema-changes.postgres
       schema-changes.oracle
       schema-changes.mongodb
  2. Reads from offset 0 (or --from-offset) to the end.
  3. Parses Debezium DDL events (ALTER TABLE, new field, removed field).
  4. Prints the exact ddl_apply.py command(s) to execute for each change.

Output
------
Each detected change is printed as:
  ┌─ [postgres/customers] ALTER TABLE ADD COLUMN shipped_at TIMESTAMP
  │  Detected at: 2026-09-21T10:42:15Z
  │  Command:
  │    python3 scripts/ddl_apply.py --source postgres --table customers \\
  │        --op add --col shipped_at --type TIMESTAMP
  └─

You can then copy-paste the command(s) and execute them.

Usage
-----
  # All sources (default):
  python3 scripts/ddl_extract.py

  # Single source only:
  python3 scripts/ddl_extract.py --source postgres
  python3 scripts/ddl_extract.py --source oracle
  python3 scripts/ddl_extract.py --source mongodb

  # Limit how many messages to read per topic (default: 10000):
  python3 scripts/ddl_extract.py --max-messages 5000

  # Read from a specific Kafka offset (default: 0 = beginning):
  python3 scripts/ddl_extract.py --from-offset 42

  # Print raw Debezium DDL event JSON without parsing:
  python3 scripts/ddl_extract.py --raw

  # Non-interactive / CI mode (no prompts):
  python3 scripts/ddl_extract.py --yes

Environment variables
---------------------
  NAMESPACE     K8s namespace for fetching Kafka credentials (default: prod)
  ADDR          OpenBao address (default: http://192.168.1.50:30820)
  BAO_TOKEN     OpenBao root token (auto-fetched from K8s secret if unset)
  KAFKA_BOOTSTRAP  Kafka bootstrap servers (default: auto-fetched from cluster)
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import textwrap
from datetime import datetime, timezone
from typing import Any

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
K8S_NS          = os.environ.get("NAMESPACE", "prod")
BAO_ADDR        = os.environ.get("ADDR", "http://192.168.1.50:30820")
KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP",
    "strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092",
)

# ─────────────────────────────────────────────────────────────────────────────
# Source registry — mirrors ddl_apply.py
# ─────────────────────────────────────────────────────────────────────────────
_SOURCES: list[dict[str, str]] = [
    {
        "source_key":   "postgres",
        "ddl_topic":    "schema-changes.postgres",
        "topic_prefix": "postgres.cache_testing.",
        "default_ns":   "cache_testing",
    },
    {
        "source_key":   "oracle",
        "ddl_topic":    "schema-changes.oracle",
        "topic_prefix": "oracle.cache_testing.",
        "default_ns":   "cache_testing",
    },
    {
        "source_key":   "mongodb",
        "ddl_topic":    "schema-changes.mongodb",
        "topic_prefix": "mongodb.cache_testing.",
        "default_ns":   "cache_testing",
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# Source-type → Iceberg type maps (same as 04_schema_evolution_handler.py)
# ─────────────────────────────────────────────────────────────────────────────
_PG_TYPE_MAP: dict[str, str] = {
    "character varying": "STRING", "varchar": "STRING", "text": "STRING",
    "char": "STRING", "uuid": "STRING", "json": "STRING", "jsonb": "STRING",
    "xml": "STRING", "citext": "STRING",
    "integer": "INT", "int": "INT", "int4": "INT",
    "smallint": "SMALLINT", "int2": "SMALLINT",
    "bigint": "BIGINT", "int8": "BIGINT", "serial": "INT", "bigserial": "BIGINT",
    "real": "FLOAT", "float4": "FLOAT",
    "double precision": "DOUBLE", "float8": "DOUBLE",
    "numeric": "DECIMAL(38,10)", "decimal": "DECIMAL(38,10)",
    "boolean": "BOOLEAN", "bool": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "timestamp without time zone": "TIMESTAMP",
    "timestamp with time zone": "TIMESTAMP",
    "time": "STRING", "bytea": "BINARY",
}

_ORA_TYPE_MAP: dict[str, str] = {
    "varchar2": "STRING", "varchar": "STRING", "char": "STRING",
    "nchar": "STRING", "nvarchar2": "STRING", "clob": "STRING",
    "nclob": "STRING", "long": "STRING",
    "number": "DECIMAL(38,10)", "integer": "BIGINT", "float": "DOUBLE",
    "binary_float": "FLOAT", "binary_double": "DOUBLE",
    "smallint": "SMALLINT", "date": "TIMESTAMP", "timestamp": "TIMESTAMP",
    "boolean": "BOOLEAN", "raw": "BINARY", "blob": "BINARY",
}

_MGO_TYPE_MAP: dict[str, str] = {
    "string": "STRING", "objectid": "STRING", "bindata": "BINARY",
    "int32": "INT", "int64": "BIGINT", "int": "INT",
    "double": "DOUBLE", "decimal128": "DECIMAL(38,10)",
    "bool": "BOOLEAN", "date": "TIMESTAMP", "timestamp": "TIMESTAMP",
    "array": "STRING", "object": "STRING",
}


def _src_type_to_iceberg(source_key: str, type_str: str) -> str:
    if not type_str:
        return "STRING"
    lower = type_str.lower().strip()
    base  = lower.split("(")[0].strip()
    if base in ("decimal", "numeric", "number") and "(" in lower:
        inner = lower[lower.index("(") + 1: lower.index(")")]
        parts = inner.split(",")
        p = int(parts[0].strip()) if parts[0].strip().isdigit() else 38
        s = int(parts[1].strip()) if len(parts) > 1 and parts[1].strip().isdigit() else 10
        return f"DECIMAL({p},{s})"
    if source_key == "postgres":
        return _PG_TYPE_MAP.get(base, _PG_TYPE_MAP.get(lower, "STRING"))
    elif source_key == "oracle":
        return _ORA_TYPE_MAP.get(base, _ORA_TYPE_MAP.get(lower, "STRING"))
    elif source_key == "mongodb":
        return _MGO_TYPE_MAP.get(base, "STRING")
    return "STRING"


# ─────────────────────────────────────────────────────────────────────────────
# OpenBao + kubectl helpers (same pattern as ddl_apply.py)
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
            f"Cannot fetch OpenBao token: {e}\n"
            "Set BAO_TOKEN env var or ensure kubectl can reach the cluster."
        )


def _bao_read(path: str) -> dict:
    import urllib.request
    req = urllib.request.Request(
        f"{BAO_ADDR}/v1/{path}",
        headers={"X-Vault-Token": _bao_token()},
    )
    try:
        resp = urllib.request.urlopen(req, timeout=10)
        return json.loads(resp.read())["data"]["data"]
    except Exception as exc:
        raise RuntimeError(f"OpenBao read {path} failed: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# DDL parsing helpers
# ─────────────────────────────────────────────────────────────────────────────

# Patterns for PostgreSQL / Oracle ALTER TABLE DDL
_RE_ADD_COL    = re.compile(
    r"ALTER\s+TABLE\s+\S+\s+ADD\s+(?:COLUMN\s+)?(\w+)\s+([^\s,;]+)",
    re.IGNORECASE,
)
_RE_DROP_COL   = re.compile(
    r"ALTER\s+TABLE\s+\S+\s+DROP\s+(?:COLUMN\s+)?(\w+)",
    re.IGNORECASE,
)
_RE_MODIFY_COL = re.compile(
    r"ALTER\s+TABLE\s+\S+\s+(?:MODIFY|ALTER\s+COLUMN)\s+(\w+)\s+([^\s,;]+)",
    re.IGNORECASE,
)
_RE_RENAME_COL = re.compile(
    r"ALTER\s+TABLE\s+\S+\s+RENAME\s+COLUMN\s+(\w+)\s+TO\s+(\w+)",
    re.IGNORECASE,
)


def _parse_ddl_text(
    source_key: str,
    ddl_text: str,
    table_name: str,
) -> list[dict[str, str]]:
    """
    Parse a DDL text string and return a list of change dicts:
      {"op": "add",    "col": "col_name", "type": "iceberg_type"}
      {"op": "drop",   "col": "col_name"}
      {"op": "modify", "col": "col_name", "type": "iceberg_type"}
      {"op": "rename", "col": "old_name", "new_col": "new_name"}
    """
    changes: list[dict[str, str]] = []
    for m in _RE_ADD_COL.finditer(ddl_text):
        col_name  = m.group(1).lower()
        col_type  = _src_type_to_iceberg(source_key, m.group(2))
        changes.append({"op": "add", "col": col_name, "type": col_type})
    for m in _RE_DROP_COL.finditer(ddl_text):
        changes.append({"op": "drop", "col": m.group(1).lower()})
    for m in _RE_MODIFY_COL.finditer(ddl_text):
        col_name = m.group(1).lower()
        col_type = _src_type_to_iceberg(source_key, m.group(2))
        changes.append({"op": "modify", "col": col_name, "type": col_type})
    for m in _RE_RENAME_COL.finditer(ddl_text):
        changes.append({
            "op":      "rename",
            "col":     m.group(1).lower(),
            "new_col": m.group(2).lower(),
        })
    return changes


def _parse_mongodb_schema_change(
    source_key: str,
    event: dict,
    table_name: str,
) -> list[dict[str, str]]:
    """
    MongoDB change streams do not emit ALTER TABLE DDL text.
    Instead the Debezium MongoDB connector emits schema diff documents
    when field additions or removals are detected.

    Debezium format (simplified):
      {
        "added":   [{"field": "loyalty_tier", "type": "string"}, ...],
        "removed": [{"field": "old_col"}, ...],
        "updated": [{"field": "col", "type": "int64"}, ...]
      }
    These appear in event["patch"] or event["after"] as $schema or
    in the event root under "schemaChanges".
    """
    changes: list[dict[str, str]] = []
    schema_changes = event.get("schemaChanges") or {}
    for fld in schema_changes.get("added", []):
        col_name = fld.get("field", "").lower()
        col_type = _src_type_to_iceberg(source_key, fld.get("type", "string"))
        if col_name:
            changes.append({"op": "add", "col": col_name, "type": col_type})
    for fld in schema_changes.get("removed", []):
        col_name = fld.get("field", "").lower()
        if col_name:
            changes.append({"op": "drop", "col": col_name})
    for fld in schema_changes.get("updated", []):
        col_name = fld.get("field", "").lower()
        col_type = _src_type_to_iceberg(source_key, fld.get("type", "string"))
        if col_name:
            changes.append({"op": "modify", "col": col_name, "type": col_type})
    return changes


# ─────────────────────────────────────────────────────────────────────────────
# ddl_apply.py command builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_command(
    source_key: str,
    table_name: str,
    change: dict[str, str],
    namespace: str = "cache_testing",
) -> str:
    op  = change["op"]
    col = change["col"]
    if op == "add":
        return (
            f"python3 scripts/ddl_apply.py "
            f"--source {source_key} "
            f"--table {table_name} "
            f"--op add "
            f"--col {col} "
            f"--type {change['type']}"
        )
    elif op == "drop":
        return (
            f"python3 scripts/ddl_apply.py "
            f"--source {source_key} "
            f"--table {table_name} "
            f"--op drop "
            f"--col {col}"
        )
    elif op == "modify":
        return (
            f"python3 scripts/ddl_apply.py "
            f"--source {source_key} "
            f"--table {table_name} "
            f"--op modify "
            f"--col {col} "
            f"--type {change['type']}"
        )
    elif op == "rename":
        return (
            f"python3 scripts/ddl_apply.py "
            f"--source {source_key} "
            f"--table {table_name} "
            f"--op rename "
            f"--col {col} "
            f"--new-col {change['new_col']}"
        )
    return ""


# ─────────────────────────────────────────────────────────────────────────────
# Kafka consumer (plain confluent-kafka, no Spark)
# ─────────────────────────────────────────────────────────────────────────────

def _consume_ddl_topic(
    source: dict[str, str],
    kafka_user: str,
    kafka_pass: str,
    max_messages: int,
    from_offset: int,
    raw_mode: bool,
) -> list[dict[str, Any]]:
    """
    Consume up to max_messages from the DDL topic for this source.
    Returns a list of parsed result dicts:
      {
        "source_key":  str,
        "table_name":  str,
        "ddl_text":    str,          # raw DDL string (postgres/oracle)
        "changes":     list[dict],   # parsed changes
        "commands":    list[str],    # ready-to-run ddl_apply.py commands
        "event_ts":    str,          # ISO timestamp
        "offset":      int,
      }
    """
    try:
        from confluent_kafka import Consumer, KafkaError, TopicPartition
    except ImportError:
        raise RuntimeError(
            "confluent-kafka not installed.\n"
            "Install with: pip install confluent-kafka"
        )

    source_key = source["source_key"]
    ddl_topic  = source["ddl_topic"]

    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "security.protocol":  "SASL_PLAINTEXT",
        "sasl.mechanism":     "SCRAM-SHA-512",
        "sasl.username":      kafka_user,
        "sasl.password":      kafka_pass,
        "group.id":           f"ddl-extract-{source_key}-{os.getpid()}",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": "false",
    })

    # Seek to from_offset on all partitions
    metadata = consumer.list_topics(ddl_topic, timeout=10)
    if ddl_topic not in metadata.topics:
        print(
            f"  [INFO] Topic {ddl_topic!r} does not exist — "
            "no DDL events for this source.",
            flush=True,
        )
        consumer.close()
        return []

    partitions = [
        TopicPartition(ddl_topic, pid, from_offset)
        for pid in metadata.topics[ddl_topic].partitions
    ]
    consumer.assign(partitions)

    results: list[dict[str, Any]] = []
    polled = 0
    empty_polls = 0
    MAX_EMPTY_POLLS = 5   # stop after 5 consecutive empty polls (end of topic)

    while polled < max_messages and empty_polls < MAX_EMPTY_POLLS:
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                empty_polls += 1
                continue
            print(
                f"  [WARN] Kafka error on {ddl_topic}: {msg.error()}",
                flush=True,
            )
            continue

        empty_polls = 0
        polled     += 1

        try:
            raw_bytes = msg.value()
            if raw_bytes is None:
                continue
            event = json.loads(raw_bytes.decode("utf-8"))

            if raw_mode:
                results.append({
                    "source_key": source_key,
                    "raw":        json.dumps(event, indent=2),
                    "offset":     msg.offset(),
                })
                continue

            # Extract table / collection name
            src_meta   = event.get("source") or {}
            table_name = (
                src_meta.get("table")
                or src_meta.get("collection")
                or ""
            ).lower()
            if not table_name:
                continue

            ddl_text = event.get("ddl", "")

            # Parse changes
            if source_key == "mongodb":
                changes = _parse_mongodb_schema_change(source_key, event, table_name)
            else:
                changes = _parse_ddl_text(source_key, ddl_text, table_name)

            # Determine event timestamp
            ts_ms = src_meta.get("ts_ms") or event.get("ts_ms") or 0
            if ts_ms:
                event_ts = datetime.utcfromtimestamp(ts_ms / 1000).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
            else:
                event_ts = "unknown"

            if not changes and not ddl_text:
                continue

            commands = [
                _build_command(source_key, table_name, c)
                for c in changes
                if _build_command(source_key, table_name, c)
            ]

            results.append({
                "source_key": source_key,
                "table_name": table_name,
                "ddl_text":   ddl_text,
                "changes":    changes,
                "commands":   commands,
                "event_ts":   event_ts,
                "offset":     msg.offset(),
            })

        except Exception as exc:
            print(
                f"  [WARN] Error parsing message at offset {msg.offset()}: {exc}",
                flush=True,
            )

    consumer.close()
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Output formatting
# ─────────────────────────────────────────────────────────────────────────────

def _print_results(results: list[dict[str, Any]], raw_mode: bool) -> None:
    if not results:
        print("  (no DDL events found)", flush=True)
        return

    for r in results:
        if raw_mode:
            print(f"\n[offset={r['offset']}] raw event:")
            print(r["raw"])
            continue

        src   = r["source_key"]
        tbl   = r["table_name"]
        ts    = r["event_ts"]
        ddl   = r["ddl_text"]
        chgs  = r["changes"]
        cmds  = r["commands"]

        for i, chg in enumerate(chgs):
            op      = chg["op"].upper()
            col     = chg["col"]
            tp      = chg.get("type", "")
            new_col = chg.get("new_col", "")
            detail  = f"{op} COLUMN {col}"
            if tp:
                detail += f" {tp}"
            if new_col:
                detail += f" → {new_col}"
            cmd = cmds[i] if i < len(cmds) else ""
            print(f"\n  ┌─ [{src}/{tbl}] {detail}")
            print(f"  │  Detected at : {ts}  (offset={r['offset']})")
            if ddl:
                ddl_short = ddl.strip().replace("\n", " ")[:120]
                print(f"  │  Source DDL  : {ddl_short}")
            if cmd:
                wrapped = textwrap.fill(
                    cmd, width=90,
                    subsequent_indent="  │      ",
                    break_long_words=False,
                    break_on_hyphens=False,
                )
                print(f"  │  Command     :")
                print(f"  │    {wrapped}")
                print(f"  │")
                print(f"  │  After running the command, restart the pipeline:")
                mode_hint = "standard  (or soft-delete / history-tracking)"
                print(f"  │    kubectl rollout restart deployment/kafka-to-iceberg-{mode_hint} -n prod")
            print(f"  └{'─'*70}", flush=True)

        if not chgs and ddl:
            print(f"\n  ┌─ [{src}/{tbl}] DDL (unrecognised pattern — manual review needed)")
            print(f"  │  Detected at : {ts}  (offset={r['offset']})")
            print(f"  │  DDL         : {ddl.strip()[:200]}")
            print(f"  └{'─'*70}", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Read DDL events from Debezium schema-changes Kafka topics and\n"
            "print ready-to-run ddl_apply.py commands.\n\n"
            "Run this when the Kafka→Iceberg pipeline stops with a\n"
            "SCHEMA MISMATCH error to find out exactly what to apply."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--source", default=None,
        choices=["postgres", "oracle", "mongodb"],
        help="Filter to a single CDC source (default: all three).",
    )
    p.add_argument(
        "--max-messages", type=int, default=10_000,
        help="Max messages to read per topic (default: 10000).",
    )
    p.add_argument(
        "--from-offset", type=int, default=0,
        help="Read from this Kafka offset (default: 0 = beginning).",
    )
    p.add_argument(
        "--raw", action="store_true", default=False,
        help="Print raw Debezium JSON events without parsing.",
    )
    p.add_argument(
        "--yes", "-y", action="store_true", default=False,
        help="Skip the interactive confirmation prompt.",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    sources = (
        [s for s in _SOURCES if s["source_key"] == args.source]
        if args.source
        else _SOURCES
    )

    print()
    print("=" * 72)
    print("  CDC DDL EXTRACT — pending schema changes in Kafka topics")
    print("=" * 72)
    print(f"  Sources      : {[s['source_key'] for s in sources]}")
    print(f"  Kafka        : {KAFKA_BOOTSTRAP}")
    print(f"  Max messages : {args.max_messages} per topic")
    print(f"  From offset  : {args.from_offset}")
    print(f"  Raw mode     : {args.raw}")
    print()

    if not args.yes:
        try:
            answer = input(
                "  Press Enter to start reading, or Ctrl-C to abort: "
            ).strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Aborted.")
            sys.exit(0)

    # ── Fetch Kafka credentials from OpenBao ──────────────────────────────────
    try:
        kafka_secret = _bao_read("secret/data/platform/kafka")
        kafka_user   = kafka_secret.get("debezium_user",     "debezium-user")
        kafka_pass   = kafka_secret.get("debezium_password", "")
    except Exception as exc:
        print(f"\n  [ERROR] Could not fetch Kafka credentials: {exc}", file=sys.stderr)
        print(
            "  Ensure ADDR and BAO_TOKEN are correct, or the cluster is reachable.",
            file=sys.stderr,
        )
        sys.exit(1)

    # ── Process each source ───────────────────────────────────────────────────
    total_found = 0
    for source in sources:
        print()
        print(f"  ── Source: {source['source_key']} "
              f"  topic: {source['ddl_topic']} {'─'*40}")

        results = _consume_ddl_topic(
            source       = source,
            kafka_user   = kafka_user,
            kafka_pass   = kafka_pass,
            max_messages = args.max_messages,
            from_offset  = args.from_offset,
            raw_mode     = args.raw,
        )

        _print_results(results, raw_mode=args.raw)
        total_found += len(results)

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("=" * 72)
    print(f"  Total DDL events found: {total_found}")
    if total_found == 0:
        print()
        print("  No pending DDL events detected.")
        print("  If the pipeline is still failing with SCHEMA MISMATCH, the")
        print("  DDL may have been applied BEFORE the Debezium connector was")
        print("  configured.  In that case, run ddl_apply.py manually:")
        print()
        print("    python3 scripts/ddl_apply.py \\")
        print("        --source <source> --table <table> \\")
        print("        --op add --col <new_column> --type <TYPE>")
    else:
        print()
        print("  Copy the command(s) above and execute them in order.")
        print("  After each command, watch the pipeline logs:")
        print("    kubectl logs -n prod -l app=kafka-to-iceberg -f")
    print("=" * 72)


if __name__ == "__main__":
    main()
