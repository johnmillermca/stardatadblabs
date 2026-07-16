#!/usr/bin/env python3
"""
Grafana MCP Server
Exposes Grafana API capabilities as MCP tools for AI assistant integration.
Port: 3201 (MCP SSE)

Environment variables:
  GRAFANA_URL      : Grafana base URL (default: http://grafana.monitoring.svc.cluster.local:3000)
  GRAFANA_USER     : Admin username  (from K8s secret grafana-credentials)
  GRAFANA_PASSWORD : Admin password  (from K8s secret grafana-credentials)
  PORT             : Listening port  (default: 3201)

Tools:
  - list_dashboards     : list all dashboards
  - get_dashboard       : get a specific dashboard by UID
  - search_dashboards   : search dashboards by query
  - list_datasources    : list configured datasources
  - query_datasource    : run a query against a datasource
  - list_alerts         : list all alert rules
  - get_alert_state     : get current state of an alert rule
  - list_folders        : list all dashboard folders
  - get_org_stats       : get organisation usage statistics
  - list_users          : list all Grafana users
"""

import os
import json
import asyncio
import aiohttp
from aiohttp import web

GRAFANA_URL  = os.environ.get("GRAFANA_URL",      "http://grafana.monitoring.svc.cluster.local:3000")
GRAFANA_USER = os.environ.get("GRAFANA_USER",     "admin")
GRAFANA_PASS = os.environ.get("GRAFANA_PASSWORD", "admin")
PORT         = int(os.environ.get("PORT",         "3201"))

# ---------------------------------------------------------------------------
# Grafana HTTP helpers
# ---------------------------------------------------------------------------

def _auth() -> aiohttp.BasicAuth:
    return aiohttp.BasicAuth(GRAFANA_USER, GRAFANA_PASS)


async def grafana_get(session: aiohttp.ClientSession, path: str, params: dict = None) -> dict:
    url = f"{GRAFANA_URL}{path}"
    async with session.get(url, auth=_auth(), params=params,
                           timeout=aiohttp.ClientTimeout(total=30)) as resp:
        resp.raise_for_status()
        return await resp.json()


async def grafana_post(session: aiohttp.ClientSession, path: str, body: dict) -> dict:
    url = f"{GRAFANA_URL}{path}"
    async with session.post(url, auth=_auth(), json=body,
                            timeout=aiohttp.ClientTimeout(total=30)) as resp:
        resp.raise_for_status()
        return await resp.json()


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------

async def tool_list_dashboards(params: dict) -> dict:
    async with aiohttp.ClientSession() as s:
        data = await grafana_get(s, "/api/search", {"type": "dash-db", "limit": 200})
    return {"dashboards": data}


async def tool_get_dashboard(params: dict) -> dict:
    uid = params.get("uid", "")
    if not uid:
        return {"error": "Missing required parameter: uid"}
    async with aiohttp.ClientSession() as s:
        data = await grafana_get(s, f"/api/dashboards/uid/{uid}")
    return data


async def tool_search_dashboards(params: dict) -> dict:
    query = params.get("query", "")
    tag   = params.get("tag", "")
    p = {"type": "dash-db", "limit": 100}
    if query:
        p["query"] = query
    if tag:
        p["tag"] = tag
    async with aiohttp.ClientSession() as s:
        data = await grafana_get(s, "/api/search", p)
    return {"results": data}


async def tool_list_datasources(params: dict) -> dict:
    async with aiohttp.ClientSession() as s:
        data = await grafana_get(s, "/api/datasources")
    return {"datasources": data}


async def tool_query_datasource(params: dict) -> dict:
    ds_uid = params.get("datasource_uid", "")
    expr   = params.get("expr", "")
    start  = params.get("start", "now-1h")
    end    = params.get("end", "now")
    step   = params.get("step", "60s")
    if not ds_uid or not expr:
        return {"error": "Missing required parameters: datasource_uid, expr"}
    body = {
        "queries": [{
            "refId": "A",
            "datasource": {"uid": ds_uid},
            "expr": expr,
            "range": True,
        }],
        "from": start,
        "to": end,
    }
    async with aiohttp.ClientSession() as s:
        data = await grafana_post(s, "/api/ds/query", body)
    return data


async def tool_list_alerts(params: dict) -> dict:
    async with aiohttp.ClientSession() as s:
        data = await grafana_get(s, "/api/v1/provisioning/alert-rules")
    return {"alert_rules": data}


async def tool_get_alert_state(params: dict) -> dict:
    async with aiohttp.ClientSession() as s:
        data = await grafana_get(s, "/api/v1/provisioning/alert-rules")
    return {"alert_rules": data}


async def tool_list_folders(params: dict) -> dict:
    async with aiohttp.ClientSession() as s:
        data = await grafana_get(s, "/api/folders")
    return {"folders": data}


async def tool_get_org_stats(params: dict) -> dict:
    async with aiohttp.ClientSession() as s:
        data = await grafana_get(s, "/api/admin/stats")
    return data


async def tool_list_users(params: dict) -> dict:
    async with aiohttp.ClientSession() as s:
        data = await grafana_get(s, "/api/users")
    return {"users": data}


TOOLS = {
    "list_dashboards":   tool_list_dashboards,
    "get_dashboard":     tool_get_dashboard,
    "search_dashboards": tool_search_dashboards,
    "list_datasources":  tool_list_datasources,
    "query_datasource":  tool_query_datasource,
    "list_alerts":       tool_list_alerts,
    "get_alert_state":   tool_get_alert_state,
    "list_folders":      tool_list_folders,
    "get_org_stats":     tool_get_org_stats,
    "list_users":        tool_list_users,
}

TOOL_DEFS = [
    {
        "name": "list_dashboards",
        "description": "List all dashboards in Grafana",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_dashboard",
        "description": "Get a specific Grafana dashboard by its UID",
        "inputSchema": {
            "type": "object",
            "properties": {"uid": {"type": "string", "description": "Dashboard UID"}},
            "required": ["uid"],
        },
    },
    {
        "name": "search_dashboards",
        "description": "Search Grafana dashboards by name or tag",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search string"},
                "tag":   {"type": "string", "description": "Filter by tag"},
            },
        },
    },
    {
        "name": "list_datasources",
        "description": "List all configured datasources in Grafana",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "query_datasource",
        "description": "Run a query against a Grafana datasource (PromQL, etc.)",
        "inputSchema": {
            "type": "object",
            "properties": {
                "datasource_uid": {"type": "string", "description": "Datasource UID"},
                "expr":           {"type": "string", "description": "Query expression (PromQL, etc.)"},
                "start":          {"type": "string", "description": "Start time (default: now-1h)"},
                "end":            {"type": "string", "description": "End time (default: now)"},
                "step":           {"type": "string", "description": "Resolution step (default: 60s)"},
            },
            "required": ["datasource_uid", "expr"],
        },
    },
    {
        "name": "list_alerts",
        "description": "List all Grafana alert rules",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_alert_state",
        "description": "Get the current firing state of Grafana alert rules",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "list_folders",
        "description": "List all Grafana dashboard folders",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_org_stats",
        "description": "Get Grafana organisation statistics (dashboard count, users, etc.)",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "list_users",
        "description": "List all Grafana users",
        "inputSchema": {"type": "object", "properties": {}},
    },
]

# ---------------------------------------------------------------------------
# MCP JSON-RPC HTTP server
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
            "serverInfo": {"name": "mcp-grafana", "version": "1.0.0"},
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
    name   = params.get("name", "")
    args   = params.get("arguments", {})

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
    app.router.add_post("/", handle_jsonrpc)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    print(f"Grafana MCP server listening on :{PORT}")
    await site.start()
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
