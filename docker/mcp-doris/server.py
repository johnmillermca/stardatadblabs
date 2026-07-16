#!/usr/bin/env python3
"""
Doris MCP Server
Exposes Apache Doris query execution and schema operations as MCP tools.
Transport: SSE on port 3101.
"""
import os
import json
from typing import Any

import pymysql
import pymysql.cursors

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    from mcp import FastMCP  # type: ignore

# ── Configuration ─────────────────────────────────────────────────────────────
DORIS_HOST = os.getenv("DORIS_HOST", "doris-fe.analytics.svc.cluster.local")
DORIS_PORT = int(os.getenv("DORIS_PORT", "9030"))
DORIS_USER = os.getenv("DORIS_USER", "root")
DORIS_PASS = os.getenv("DORIS_PASS", "")
DORIS_DB   = os.getenv("DORIS_DB", "")

mcp = FastMCP("doris-mcp", port=3101)


def _get_conn(database: str = DORIS_DB):
    return pymysql.connect(
        host=DORIS_HOST,
        port=DORIS_PORT,
        user=DORIS_USER,
        password=DORIS_PASS,
        database=database or None,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
    )


def _query(sql: str, database: str = "") -> dict[str, Any]:
    try:
        with _get_conn(database) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchmany(1000)  # cap at 1000 rows
                return {"success": True, "rows": rows, "rowcount": cur.rowcount}
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def doris_query(sql: str, database: str = "") -> dict:
    """
    Execute a SQL query against Apache Doris and return results.
    Args:
        sql: SQL query string (SELECT, SHOW, DESCRIBE, etc.)
        database: Optional database context
    """
    return _query(sql, database)


@mcp.tool()
def doris_list_databases() -> dict:
    """List all databases in Doris."""
    return _query("SHOW DATABASES")


@mcp.tool()
def doris_list_tables(database: str) -> dict:
    """
    List all tables in a Doris database.
    Args:
        database: Database name
    """
    return _query(f"SHOW TABLES", database)


@mcp.tool()
def doris_describe_table(table: str, database: str = "") -> dict:
    """
    Describe the schema of a Doris table.
    Args:
        table: Table name
        database: Database name (optional if set in context)
    """
    db_prefix = f"{database}." if database else ""
    return _query(f"DESCRIBE {db_prefix}{table}")


@mcp.tool()
def doris_cluster_status() -> dict:
    """Get Doris cluster health and backend status."""
    return _query("SHOW BACKENDS")


@mcp.tool()
def doris_table_stats(table: str, database: str) -> dict:
    """
    Get row count and basic stats for a table.
    Args:
        table: Table name
        database: Database name
    """
    return _query(f"SELECT COUNT(*) as row_count FROM {database}.{table}")


@mcp.tool()
def doris_running_queries() -> dict:
    """Show currently running queries in Doris."""
    return _query("SHOW PROC '/current_queries'")


if __name__ == "__main__":
    mcp.run(transport="sse")
