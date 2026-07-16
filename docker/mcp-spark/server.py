#!/usr/bin/env python3
"""
Spark / Gluten / Velox MCP Server
Exposes Spark job submission, cluster status, and Gluten/Velox configuration
as MCP tools for AI agents.
Transport: SSE on port 3103.
"""
import os
import json
import httpx
from typing import Any

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    from mcp import FastMCP  # type: ignore

# ── Configuration ─────────────────────────────────────────────────────────────
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master-svc.analytics.svc.cluster.local:7077")
SPARK_UI_URL     = os.getenv("SPARK_UI_URL",     "http://spark-master-svc.analytics.svc.cluster.local:8080")
SPARK_REST_URL   = os.getenv("SPARK_REST_URL",   "http://spark-master-svc.analytics.svc.cluster.local:6066")

mcp = FastMCP("spark-mcp", port=3103)


@mcp.tool()
def spark_cluster_status() -> dict:
    """Get Spark cluster status — master, workers, running applications."""
    try:
        resp = httpx.get(f"{SPARK_REST_URL}/v1/submissions/status", timeout=10)
        return {"success": True, "data": resp.json()}
    except Exception as e:
        # Fallback: try the JSON API
        try:
            resp = httpx.get(f"{SPARK_UI_URL}/api/v1/applications", timeout=10)
            return {"success": True, "applications": resp.json()}
        except Exception as e2:
            return {"success": False, "error": str(e2)}


@mcp.tool()
def spark_list_applications() -> dict:
    """List all Spark applications (running + completed)."""
    try:
        resp = httpx.get(f"{SPARK_UI_URL}/api/v1/applications?status=running", timeout=10)
        running = resp.json()
        resp2 = httpx.get(f"{SPARK_UI_URL}/api/v1/applications?limit=20", timeout=10)
        all_apps = resp2.json()
        return {"success": True, "running": running, "recent": all_apps}
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def spark_submit_pi(
    partitions: int = 10,
    app_name: str = "SparkPi-MCP",
) -> dict:
    """
    Submit the built-in SparkPi job as a smoke test.
    Args:
        partitions: Number of partitions for PI estimation
        app_name: Application name shown in Spark UI
    """
    payload = {
        "action": "CreateSubmissionRequest",
        "appResource": "/opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar",
        "mainClass": "org.apache.spark.examples.SparkPi",
        "appArgs": [str(partitions)],
        "sparkProperties": {
            "spark.app.name": app_name,
            "spark.master": SPARK_MASTER_URL,
            "spark.submit.deployMode": "cluster",
        },
        "environmentVariables": {},
        "clientSparkVersion": "3.5.1",
    }
    try:
        resp = httpx.post(
            f"{SPARK_REST_URL}/v1/submissions/create",
            json=payload,
            timeout=30,
        )
        return {"success": True, "response": resp.json()}
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def spark_kill_application(submission_id: str) -> dict:
    """
    Kill a running Spark application by submission ID.
    Args:
        submission_id: Submission ID from spark_submit_* output
    """
    try:
        resp = httpx.post(
            f"{SPARK_REST_URL}/v1/submissions/kill/{submission_id}",
            timeout=15,
        )
        return {"success": True, "response": resp.json()}
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def spark_gluten_config() -> dict:
    """
    Return the recommended Spark configuration for enabling Gluten + Velox acceleration.
    These settings should be passed as spark-conf when submitting jobs.
    """
    return {
        "success": True,
        "gluten_velox_config": {
            "spark.plugins": "io.glutenproject.GlutenPlugin",
            "spark.memory.offHeap.enabled": "true",
            "spark.memory.offHeap.size": "2g",
            "spark.gluten.sql.columnar.backend.lib": "velox",
            "spark.gluten.sql.columnar.fallback.ignoreRowToColumnar": "true",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        },
        "notes": [
            "Add spark.plugins=io.glutenproject.GlutenPlugin to enable Gluten",
            "Off-heap memory is required for Velox columnar engine",
            "Fallback ignoreRowToColumnar prevents failures on unsupported operators",
        ],
    }


@mcp.tool()
def spark_worker_status() -> dict:
    """List Spark worker nodes and their available resources."""
    try:
        resp = httpx.get(f"{SPARK_UI_URL}/api/v1/applications", timeout=10)
        return {"success": True, "data": resp.json()}
    except Exception as e:
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    mcp.run(transport="sse")
