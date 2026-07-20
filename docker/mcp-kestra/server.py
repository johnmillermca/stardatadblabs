#!/usr/bin/env python3
"""
Kestra MCP Server
Exposes Kestra workflow orchestration operations as MCP tools for AI agents.
Supports: flow management, execution triggering, log retrieval, namespace ops.
Transport: SSE on port 3105.
"""
import os
import json
from typing import Any

import httpx

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    from mcp import FastMCP  # type: ignore

# ── Configuration ─────────────────────────────────────────────────────────────
KESTRA_URL  = os.getenv("KESTRA_URL",  "http://kestra.prod.svc.cluster.local:8080")
KESTRA_USER = os.getenv("KESTRA_USER", "")
KESTRA_PASS = os.getenv("KESTRA_PASS", "")

mcp = FastMCP("kestra-mcp", host="0.0.0.0", port=3105)


def _client() -> httpx.Client:
    auth = (KESTRA_USER, KESTRA_PASS) if KESTRA_USER else None
    return httpx.Client(base_url=KESTRA_URL, auth=auth, timeout=30)


def _get(path: str, params: dict = {}) -> dict[str, Any]:
    try:
        with _client() as c:
            r = c.get(path, params=params)
            r.raise_for_status()
            return {"success": True, "data": r.json()}
    except Exception as e:
        return {"success": False, "error": str(e)}


def _post(path: str, body: dict = {}) -> dict[str, Any]:
    try:
        with _client() as c:
            r = c.post(path, json=body)
            r.raise_for_status()
            return {"success": True, "data": r.json()}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ── Tools ─────────────────────────────────────────────────────────────────────

@mcp.tool()
def kestra_list_namespaces() -> dict:
    """List all Kestra namespaces."""
    return _get("/api/v1/namespaces")


@mcp.tool()
def kestra_list_flows(namespace: str, page: int = 1, page_size: int = 50) -> dict:
    """
    List all flows in a Kestra namespace.
    Args:
        namespace: Kestra namespace (e.g. 'company.team')
        page: Page number (default 1)
        page_size: Results per page (default 50)
    """
    return _get(f"/api/v1/flows/{namespace}", params={"page": page, "pageSize": page_size})


@mcp.tool()
def kestra_get_flow(namespace: str, flow_id: str) -> dict:
    """
    Get the full YAML definition of a Kestra flow.
    Args:
        namespace: Kestra namespace
        flow_id: Flow identifier
    """
    return _get(f"/api/v1/flows/{namespace}/{flow_id}")


@mcp.tool()
def kestra_execute_flow(namespace: str, flow_id: str, inputs: dict = {}) -> dict:
    """
    Trigger an execution of a Kestra flow.
    Args:
        namespace: Kestra namespace
        flow_id: Flow identifier
        inputs: Optional dict of flow input values
    """
    return _post(f"/api/v1/executions/{namespace}/{flow_id}", body=inputs)


@mcp.tool()
def kestra_list_executions(
    namespace: str,
    flow_id: str = "",
    state: str = "",
    page: int = 1,
    page_size: int = 25,
) -> dict:
    """
    List executions for a namespace or specific flow.
    Args:
        namespace: Kestra namespace
        flow_id: Optional flow ID filter
        state: Optional state filter (CREATED, RUNNING, SUCCESS, FAILED, KILLED, PAUSED)
        page: Page number (default 1)
        page_size: Results per page (default 25)
    """
    params: dict = {"namespace": namespace, "page": page, "pageSize": page_size}
    if flow_id:
        params["flowId"] = flow_id
    if state:
        params["state"] = state
    return _get("/api/v1/executions", params=params)


@mcp.tool()
def kestra_get_execution(execution_id: str) -> dict:
    """
    Get details of a specific Kestra execution.
    Args:
        execution_id: Execution ID (UUID)
    """
    return _get(f"/api/v1/executions/{execution_id}")


@mcp.tool()
def kestra_get_execution_logs(execution_id: str, page: int = 1, page_size: int = 100) -> dict:
    """
    Retrieve logs for a Kestra execution.
    Args:
        execution_id: Execution ID (UUID)
        page: Page number (default 1)
        page_size: Log lines per page (default 100)
    """
    return _get(
        f"/api/v1/logs/{execution_id}",
        params={"page": page, "pageSize": page_size},
    )


@mcp.tool()
def kestra_kill_execution(execution_id: str) -> dict:
    """
    Kill a running Kestra execution.
    Args:
        execution_id: Execution ID (UUID)
    """
    try:
        with _client() as c:
            r = c.delete(f"/api/v1/executions/{execution_id}/kill")
            r.raise_for_status()
            return {"success": True, "killed": execution_id}
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def kestra_list_triggers(namespace: str, flow_id: str) -> dict:
    """
    List triggers configured on a Kestra flow.
    Args:
        namespace: Kestra namespace
        flow_id: Flow identifier
    """
    return _get(f"/api/v1/triggers/{namespace}/{flow_id}")


@mcp.tool()
def kestra_search_flows(query: str, page: int = 1, page_size: int = 25) -> dict:
    """
    Search flows across all namespaces by keyword.
    Args:
        query: Search keyword
        page: Page number (default 1)
        page_size: Results per page (default 25)
    """
    return _get("/api/v1/flows/search", params={"q": query, "page": page, "pageSize": page_size})


@mcp.tool()
def kestra_get_stats() -> dict:
    """Get overall Kestra execution statistics (total, running, failed, success counts)."""
    return _get("/api/v1/executions/stats")


@mcp.tool()
def kestra_server_health() -> dict:
    """Check Kestra server health status."""
    return _get("/health")


if __name__ == "__main__":
    mcp.run(transport="sse")
