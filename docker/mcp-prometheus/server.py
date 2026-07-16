#!/usr/bin/env python3
"""
Prometheus MCP Server
Exposes Prometheus query capabilities as MCP tools for AI assistant integration.
Port: 3200 (MCP SSE)

Tools:
  - query_instant   : run PromQL instant query
  - query_range     : run PromQL range query
  - list_metrics    : list all available metric names
  - get_alerts      : retrieve active alerts from Alertmanager
  - get_targets     : list all scrape targets and their health
  - get_rules       : list alerting/recording rules
  - query_label_values : get distinct values for a label
"""

import os
import json
import time
import asyncio
import aiohttp
from aiohttp import web

PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://prometheus-prometheus.monitoring.svc.cluster.local:9090")
ALERTMANAGER_URL = os.environ.get("ALERTMANAGER_URL", "http://prometheus-alertmanager.monitoring.svc.cluster.local:9093")
PORT = int(os.environ.get("PORT", "3200"))

# ---------------------------------------------------------------------------
# Prometheus helpers
# ---------------------------------------------------------------------------

async def prom_get(session: aiohttp.ClientSession, path: str, params: dict = None) -> dict:
    url = f"{PROMETHEUS_URL}{path}"
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
        resp.raise_for_status()
        return await resp.json()


async def am_get(session: aiohttp.ClientSession, path: str) -> dict:
    url = f"{ALERTMANAGER_URL}{path}"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        resp.raise_for_status()
        return await resp.json()


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------

async def tool_query_instant(params: dict) -> dict:
    query = params.get("query", "")
    ts = params.get("time")
    if not query:
        return {"error": "Missing required parameter: query"}
    p = {"query": query}
    if ts:
        p["time"] = ts
    async with aiohttp.ClientSession() as s:
        data = await prom_get(s, "/api/v1/query", p)
    return data


async def tool_query_range(params: dict) -> dict:
    query = params.get("query", "")
    start = params.get("start")
    end = params.get("end")
    step = params.get("step", "60s")
    if not query:
        return {"error": "Missing required parameter: query"}
    now = time.time()
    p = {
        "query": query,
        "start": start or str(int(now - 3600)),
        "end": end or str(int(now)),
        "step": step,
    }
    async with aiohttp.ClientSession() as s:
        data = await prom_get(s, "/api/v1/query_range", p)
    return data


async def tool_list_metrics(params: dict) -> dict:
    match = params.get("match", "")
    p = {}
    if match:
        p["match[]"] = match
    async with aiohttp.ClientSession() as s:
        data = await prom_get(s, "/api/v1/label/__name__/values", p)
    return data


async def tool_get_alerts(params: dict) -> dict:
    async with aiohttp.ClientSession() as s:
        data = await am_get(s, "/api/v2/alerts")
    return {"alerts": data}


async def tool_get_targets(params: dict) -> dict:
    async with aiohttp.ClientSession() as s:
        data = await prom_get(s, "/api/v1/targets")
    return data


async def tool_get_rules(params: dict) -> dict:
    async with aiohttp.ClientSession() as s:
        data = await prom_get(s, "/api/v1/rules")
    return data


async def tool_query_label_values(params: dict) -> dict:
    label = params.get("label", "")
    if not label:
        return {"error": "Missing required parameter: label"}
    async with aiohttp.ClientSession() as s:
        data = await prom_get(s, f"/api/v1/label/{label}/values")
    return data


TOOLS = {
    "query_instant": tool_query_instant,
    "query_range": tool_query_range,
    "list_metrics": tool_list_metrics,
    "get_alerts": tool_get_alerts,
    "get_targets": tool_get_targets,
    "get_rules": tool_get_rules,
    "query_label_values": tool_query_label_values,
}

TOOL_DEFS = [
    {
        "name": "query_instant",
        "description": "Run a PromQL instant query against Prometheus",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "PromQL expression"},
                "time":  {"type": "string", "description": "Unix timestamp or RFC3339 (optional, defaults to now)"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "query_range",
        "description": "Run a PromQL range query over a time window",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "PromQL expression"},
                "start": {"type": "string", "description": "Start time (Unix or RFC3339, default: now-1h)"},
                "end":   {"type": "string", "description": "End time (Unix or RFC3339, default: now)"},
                "step":  {"type": "string", "description": "Resolution step, e.g. '60s' (default: 60s)"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "list_metrics",
        "description": "List all available metric names in Prometheus",
        "inputSchema": {
            "type": "object",
            "properties": {
                "match": {"type": "string", "description": "Optional metric selector filter"},
            },
        },
    },
    {
        "name": "get_alerts",
        "description": "Retrieve all currently firing alerts from Alertmanager",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_targets",
        "description": "List all Prometheus scrape targets and their UP/DOWN health status",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_rules",
        "description": "List all alerting and recording rules configured in Prometheus",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "query_label_values",
        "description": "Get all distinct values for a specific Prometheus label",
        "inputSchema": {
            "type": "object",
            "properties": {
                "label": {"type": "string", "description": "Label name, e.g. 'namespace', 'job', 'instance'"},
            },
            "required": ["label"],
        },
    },
]

# ---------------------------------------------------------------------------
# MCP SSE HTTP server
# ---------------------------------------------------------------------------

async def handle_health(request):
    return web.Response(text="ok")


async def handle_initialize(request):
    data = await request.json()
    return web.json_response({
        "jsonrpc": "2.0",
        "id": data.get("id"),
        "result": {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "mcp-prometheus", "version": "1.0.0"},
        },
    })


async def handle_tools_list(request):
    data = await request.json()
    return web.json_response({
        "jsonrpc": "2.0",
        "id": data.get("id"),
        "result": {"tools": TOOL_DEFS},
    })


async def handle_tools_call(request):
    data = await request.json()
    req_id = data.get("id")
    params = data.get("params", {})
    name = params.get("name", "")
    args = params.get("arguments", {})

    if name not in TOOLS:
        return web.json_response({
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": f"Tool not found: {name}"},
        })

    try:
        result = await TOOLS[name](args)
    except Exception as exc:
        return web.json_response({
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32000, "message": str(exc)},
        })

    return web.json_response({
        "jsonrpc": "2.0",
        "id": req_id,
        "result": {
            "content": [{"type": "text", "text": json.dumps(result, indent=2)}],
        },
    })


async def handle_jsonrpc(request):
    data = await request.json()
    method = data.get("method", "")
    if method == "initialize":
        return await handle_initialize(request)
    elif method == "tools/list":
        return await handle_tools_list(request)
    elif method == "tools/call":
        return await handle_tools_call(request)
    else:
        return web.json_response({
            "jsonrpc": "2.0",
            "id": data.get("id"),
            "error": {"code": -32601, "message": f"Method not found: {method}"},
        })


async def main():
    app = web.Application()
    app.router.add_get("/health", handle_health)
    app.router.add_post("/mcp", handle_jsonrpc)
    # Re-route POST / to the same handler for compatibility
    app.router.add_post("/", handle_jsonrpc)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    print(f"Prometheus MCP server listening on :{PORT}")
    await site.start()
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
