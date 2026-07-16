#!/usr/bin/env python3
"""
SQLMesh MCP Server
Exposes SQLMesh operations as MCP (Model Context Protocol) tools.
Transport: SSE (Server-Sent Events) over HTTP on port 3100.
"""
import os
import json
import subprocess
import tempfile
import asyncio
from pathlib import Path
from typing import Any

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    from mcp import FastMCP  # type: ignore

# ── Configuration ─────────────────────────────────────────────────────────────
SQLMESH_UI_URL = os.getenv("SQLMESH_UI_URL", "http://sqlmesh.analytics.svc.cluster.local:8001")
SQLMESH_PROJECT_DIR = os.getenv("SQLMESH_PROJECT_DIR", "/app/models")
SQLMESH_CONFIG = os.getenv("SQLMESH_CONFIG", "/app/config/config.yaml")

mcp = FastMCP("sqlmesh-mcp", port=3100)


def _run_sqlmesh(*args: str) -> dict[str, Any]:
    """Run a sqlmesh CLI command and return stdout/stderr."""
    cmd = ["sqlmesh", "--config", SQLMESH_CONFIG] + list(args)
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
            cwd=SQLMESH_PROJECT_DIR,
        )
        return {
            "success": result.returncode == 0,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode,
        }
    except subprocess.TimeoutExpired:
        return {"success": False, "error": "Command timed out after 120s"}
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def sqlmesh_plan(
    environment: str = "prod",
    start: str = "",
    end: str = "",
    auto_apply: bool = False,
) -> dict:
    """
    Run sqlmesh plan to compute changes needed for the target environment.
    Args:
        environment: Target environment (prod, staging, dev)
        start: Start date for backfill (YYYY-MM-DD)
        end: End date for backfill (YYYY-MM-DD)
        auto_apply: If True, automatically apply the plan
    """
    args = ["plan", environment]
    if start:
        args += ["--start", start]
    if end:
        args += ["--end", end]
    if auto_apply:
        args += ["--auto-apply"]
    return _run_sqlmesh(*args)


@mcp.tool()
def sqlmesh_run(environment: str = "prod") -> dict:
    """
    Run all pending sqlmesh model evaluations for the given environment.
    Args:
        environment: Target environment name
    """
    return _run_sqlmesh("run", environment)


@mcp.tool()
def sqlmesh_audit(model: str = "") -> dict:
    """
    Run sqlmesh audits to validate data quality.
    Args:
        model: Specific model name to audit (empty = all models)
    """
    args = ["audit"]
    if model:
        args += ["--model", model]
    return _run_sqlmesh(*args)


@mcp.tool()
def sqlmesh_list_models() -> dict:
    """List all SQLMesh models in the project."""
    return _run_sqlmesh("models")


@mcp.tool()
def sqlmesh_dag() -> dict:
    """Return the DAG of model dependencies."""
    return _run_sqlmesh("dag", "--format", "json")


@mcp.tool()
def sqlmesh_diff(environment: str = "prod") -> dict:
    """
    Show differences between current model state and target environment.
    Args:
        environment: Target environment to diff against
    """
    return _run_sqlmesh("diff", environment)


@mcp.tool()
def sqlmesh_test(model: str = "") -> dict:
    """
    Run unit tests for SQLMesh models.
    Args:
        model: Specific model to test (empty = all models)
    """
    args = ["test"]
    if model:
        args += [model]
    return _run_sqlmesh(*args)


@mcp.tool()
def sqlmesh_fetchdf(sql: str) -> dict:
    """
    Execute a SQL query through the SQLMesh gateway and return results.
    Args:
        sql: SQL query string
    """
    # Write SQL to temp file and execute via sqlmesh fetchdf
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(sql)
        tmpfile = f.name
    result = _run_sqlmesh("fetchdf", tmpfile)
    Path(tmpfile).unlink(missing_ok=True)
    return result


if __name__ == "__main__":
    mcp.run(transport="sse")
