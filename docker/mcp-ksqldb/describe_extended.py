#!/usr/bin/env python3
"""
ksqlDB DESCRIBE EXTENDED
========================
ksqlDB exposes DESCRIBE EXTENDED via its /ksql REST endpoint.
This script calls that endpoint and pretty-prints the result in the same
layout the ksql CLI would show.

Usage
-----
    # describe a stream or table
    python describe_extended.py <name>

    # against a non-default server
    KSQLDB_URL=http://192.168.1.56:30088 python describe_extended.py orders

Environment variables
---------------------
    KSQLDB_URL   base URL of the ksqlDB REST API  (default: http://localhost:8088)
    KSQLDB_USER  basic-auth username               (optional)
    KSQLDB_PASS  basic-auth password               (optional)

Dependencies
------------
    pip install httpx
"""

import os
import sys
import base64
import json
import httpx

KSQLDB_URL  = os.getenv("KSQLDB_URL",  "http://localhost:8088").rstrip("/")
KSQLDB_USER = os.getenv("KSQLDB_USER", "")
KSQLDB_PASS = os.getenv("KSQLDB_PASS", "")


# ── HTTP helper ────────────────────────────────────────────────────────────────

def _headers() -> dict:
    h = {
        "Content-Type": "application/vnd.ksql.v1+json",
        "Accept":       "application/vnd.ksql.v1+json",
    }
    if KSQLDB_USER:
        token = base64.b64encode(f"{KSQLDB_USER}:{KSQLDB_PASS}".encode()).decode()
        h["Authorization"] = f"Basic {token}"
    return h


def ksql_post(statement: str) -> list:
    """POST a ksqlDB statement to /ksql and return the parsed JSON list."""
    body = {"ksql": statement, "streamsProperties": {}}
    resp = httpx.post(f"{KSQLDB_URL}/ksql", json=body, headers=_headers(), timeout=15)
    resp.raise_for_status()
    return resp.json()


# ── Core function ──────────────────────────────────────────────────────────────

def describe_extended(name: str) -> dict:
    """
    Run  DESCRIBE EXTENDED <name>;  against ksqlDB and return a structured dict
    with every field the CLI would show.
    """
    result = ksql_post(f"DESCRIBE EXTENDED {name};")

    if not result or not isinstance(result, list):
        raise ValueError(f"Unexpected response: {result}")

    src = result[0].get("sourceDescription", result[0])

    # ── Schema / columns ──────────────────────────────────────────────────────
    columns = []
    for f in src.get("fields", []):
        schema   = f.get("schema", {})
        col_type = schema.get("type", "UNKNOWN")
        member   = schema.get("memberSchema")
        if member:
            col_type = f"{col_type}<{member.get('type', '')}>"
        columns.append({
            "name":   f.get("name"),
            "type":   col_type,
            "is_key": f.get("type") == "KEY",
        })

    # ── Queries ───────────────────────────────────────────────────────────────
    write_queries = [
        {"id": q.get("id"), "sql": q.get("queryString", "").strip()}
        for q in src.get("writeQueries", [])
    ]
    read_queries = [
        {"id": q.get("id"), "sql": q.get("queryString", "").strip()}
        for q in src.get("readQueries", [])
    ]

    return {
        "name":             src.get("name"),
        "type":             src.get("type"),            # STREAM or TABLE
        "sql_statement":    src.get("statement", ""),
        "kafka_topic":      src.get("topic"),
        "partitions":       src.get("partitions"),
        "replication":      src.get("replication"),
        "retention_ms":     src.get("retentionMs"),
        "key_format":       src.get("keyFormat"),
        "value_format":     src.get("valueFormat"),
        "timestamp_field":  src.get("timestampField"),
        "schema_id":        src.get("valueSchemaId"),
        "columns":          columns,
        "write_queries":    write_queries,
        "read_queries":     read_queries,
        "statistics":       src.get("statistics", ""),
        "error_stats":      src.get("errorStats", ""),
    }


# ── Pretty printer ─────────────────────────────────────────────────────────────

def _print(info: dict) -> None:
    SEP = "-" * 60

    print(f"\nName                 : {info['name']}")
    print(f"Type                 : {info['type']}")
    print(f"Kafka Topic          : {info['kafka_topic']}")
    print(f"Key Format           : {info['key_format']}")
    print(f"Value Format         : {info['value_format']}")
    print(f"Partitions           : {info['partitions']}")
    print(f"Replication Factor   : {info['replication']}")
    if info.get("retention_ms") is not None:
        print(f"Retention (ms)       : {info['retention_ms']}")
    if info.get("timestamp_field"):
        print(f"Timestamp Field      : {info['timestamp_field']}")
    if info.get("schema_id"):
        print(f"Schema Registry ID   : {info['schema_id']}")

    # SQL
    if info.get("sql_statement"):
        print(f"\nSQL Statement\n{SEP}")
        print(info["sql_statement"])

    # Schema
    print(f"\nSchema ({len(info['columns'])} columns)\n{SEP}")
    print(f"  {'Name':<30} {'Type':<20} Key")
    print(f"  {'-'*30} {'-'*20} ---")
    for col in info["columns"]:
        key_marker = " *" if col["is_key"] else ""
        print(f"  {col['name']:<30} {col['type']:<20}{key_marker}")

    # Queries
    if info["write_queries"]:
        print(f"\nWrite Queries\n{SEP}")
        for q in info["write_queries"]:
            print(f"  [{q['id']}]  {q['sql']}")

    if info["read_queries"]:
        print(f"\nRead Queries\n{SEP}")
        for q in info["read_queries"]:
            print(f"  [{q['id']}]  {q['sql']}")

    # Runtime stats
    if info.get("statistics"):
        print(f"\nRuntime Statistics\n{SEP}")
        print(f"  {info['statistics']}")

    if info.get("error_stats"):
        print(f"\nError Statistics\n{SEP}")
        print(f"  {info['error_stats']}")

    print()


# ── CLI entry point ────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("Usage: python describe_extended.py <stream-or-table-name>", file=sys.stderr)
        sys.exit(1)

    name = sys.argv[1]
    print(f"Connecting to {KSQLDB_URL} ...")

    try:
        info = describe_extended(name)
    except httpx.HTTPStatusError as e:
        try:
            body = e.response.json()
            msg  = body.get("message") or body.get("errorMessage", {}).get("message") or json.dumps(body)
        except Exception:
            msg = e.response.text
        print(f"Error {e.response.status_code}: {msg}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    _print(info)


if __name__ == "__main__":
    main()
