#!/usr/bin/env python3
"""
OpenSearch MCP Server
Exposes OpenSearch search, index management, and cluster operations as MCP tools.
Transport: SSE on port 3102.
"""
import os
import json
from typing import Any

from opensearchpy import OpenSearch

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    from mcp import FastMCP  # type: ignore

# ── Configuration ─────────────────────────────────────────────────────────────
OS_HOST  = os.getenv("OPENSEARCH_HOST", "opensearch-cluster-master.search.svc.cluster.local")
OS_PORT  = int(os.getenv("OPENSEARCH_PORT", "9200"))
OS_USER  = os.getenv("OPENSEARCH_USER", "admin")
OS_PASS  = os.getenv("OPENSEARCH_PASS", "")
OS_TLS   = os.getenv("OPENSEARCH_TLS", "false").lower() == "true"

mcp = FastMCP("opensearch-mcp", port=3102)


def _client() -> OpenSearch:
    auth = (OS_USER, OS_PASS) if OS_PASS else None
    return OpenSearch(
        hosts=[{"host": OS_HOST, "port": OS_PORT}],
        http_auth=auth,
        use_ssl=OS_TLS,
        verify_certs=False,
        ssl_show_warn=False,
        timeout=30,
    )


@mcp.tool()
def opensearch_search(
    index: str,
    query: dict,
    size: int = 10,
) -> dict:
    """
    Execute a search query against an OpenSearch index.
    Args:
        index: Index name or pattern (e.g. 'logs-*')
        query: OpenSearch query DSL as a dict (e.g. {"match_all": {}})
        size: Maximum number of hits to return (default 10, max 100)
    """
    try:
        client = _client()
        resp = client.search(
            index=index,
            body={"query": query, "size": min(size, 100)},
        )
        return {"success": True, "hits": resp["hits"]["hits"], "total": resp["hits"]["total"]}
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def opensearch_list_indices(pattern: str = "*") -> dict:
    """
    List OpenSearch indices matching a pattern.
    Args:
        pattern: Index pattern (default '*' = all indices)
    """
    try:
        client = _client()
        indices = client.cat.indices(index=pattern, format="json", h="index,health,status,docs.count,store.size")
        return {"success": True, "indices": indices}
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def opensearch_index_mapping(index: str) -> dict:
    """
    Get the field mapping for an OpenSearch index.
    Args:
        index: Index name
    """
    try:
        client = _client()
        mapping = client.indices.get_mapping(index=index)
        return {"success": True, "mapping": mapping}
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def opensearch_cluster_health() -> dict:
    """Get OpenSearch cluster health status."""
    try:
        client = _client()
        return {"success": True, "health": client.cluster.health()}
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def opensearch_index_document(index: str, document: dict, doc_id: str = "") -> dict:
    """
    Index a document into OpenSearch.
    Args:
        index: Target index name
        document: Document body as a dict
        doc_id: Optional document ID (auto-generated if empty)
    """
    try:
        client = _client()
        kwargs = {"index": index, "body": document}
        if doc_id:
            kwargs["id"] = doc_id
        resp = client.index(**kwargs)
        return {"success": True, "result": resp["result"], "id": resp["_id"]}
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def opensearch_delete_index(index: str) -> dict:
    """
    Delete an OpenSearch index.
    Args:
        index: Index name to delete
    """
    try:
        client = _client()
        resp = client.indices.delete(index=index)
        return {"success": True, "acknowledged": resp.get("acknowledged")}
    except Exception as e:
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    mcp.run(transport="sse")
