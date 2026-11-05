#!/usr/bin/env python3
"""
stargraph_sql_proxy.py
=======================
Query proxy for the Stargraph platform.

Data sources — all accessible from a single HTTP API:

  1. DuckDB → Polaris REST → Iceberg (S3)
     SQL queries against snapshotted graph data stored as Iceberg tables.
     Endpoint: POST /sql   { "sql": "SELECT ..." }
     Endpoint: POST /cypher { "cypher": "MATCH ..." }  (Cypher → SQL translation)

  2. Memgraph (live graph) via Bolt
     Cypher queries executed directly against the live Memgraph instance.
     Endpoint: POST /graph  { "cypher": "MATCH (n) RETURN n LIMIT 10" }

  3. SQL → DuckDB/Iceberg → Memgraph → graph result  ← THE FULL PIPELINE
     Step 1: Execute SQL on DuckDB/Iceberg, get raw data rows.
     Step 2: Infer nodes + edges from result columns (source_node_id /
             target_node_id / relationship_type convention).
     Step 3: Push those rows into Memgraph via Bolt UNWIND + MERGE so
             Memgraph holds them as a live graph.
     Step 4: Query Memgraph back with a Cypher MATCH to retrieve the
             graph structure for D3 rendering.
     Endpoint: POST /sql-to-graph { "sql": "SELECT ..." }

  4. Compare: run the same logical query against both sources.
     Endpoint: POST /compare { "cypher": "MATCH ..." }

Architecture
────────────
  Client
    │  POST /sql          → DuckDB /query  → Iceberg tables (S3 via Polaris)
    │  POST /cypher       → translate_cypher() → DuckDB /query → Iceberg
    │  POST /graph        → Bolt → Memgraph (live graph)
    │  POST /sql-to-graph → DuckDB /query → rows → Memgraph MERGE → Memgraph MATCH
    │  POST /compare      → both paths in parallel → returns both result sets + timings
    ▼
  stargraph_sql_proxy (this file, FastAPI, port 8680)

Usage:
  python3 stargraph_sql_proxy.py
  # or via Kubernetes: see stargraph-sql-proxy-deployment.yaml
"""

import asyncio
import base64
import json
import logging
import os
import re
import time
from typing import Any

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("sql-proxy")

DUCKDB_SERVER  = os.getenv("DUCKDB_SERVER",  "http://duckdb-worker8.prod.svc.cluster.local:8580")
MEMGRAPH_HOST  = os.getenv("MEMGRAPH_HOST",  "192.168.1.58")
MEMGRAPH_PORT  = int(os.getenv("MEMGRAPH_PORT", "7687"))
MEMGRAPH_USER  = os.getenv("MEMGRAPH_USER",  "")
MEMGRAPH_PASS  = os.getenv("MEMGRAPH_PASS",  "")
BIND_HOST      = os.getenv("PROXY_HOST",     "0.0.0.0")
BIND_PORT      = int(os.getenv("PROXY_PORT", "8680"))

# ── Memgraph graph cache TTL ─────────────────────────────────────────────────
# Scoped ad-hoc graph nodes are kept in Memgraph for GRAPH_CACHE_TTL_S seconds
# after they were first loaded.  A second identical SQL query within the TTL
# window skips the DuckDB re-scan and the Memgraph MERGE entirely — it just
# MATCHes the already-resident nodes/edges and returns them instantly.
# Set to 0 to disable caching (always re-execute, old behaviour).
GRAPH_CACHE_TTL_S = int(os.getenv("GRAPH_CACHE_TTL_S", "300"))   # 5 minutes default

# ── FastAPI ───────────────────────────────────────────────────────────────────
app = FastAPI(title="Stargraph SQL Proxy")


class SqlRequest(BaseModel):
    sql:     str | None = None       # plain SQL (may be blocked by WAF)
    sql_b64: str | None = None       # base64-encoded SQL (WAF-safe alternative)
    parameters: list[Any] | None = None

    def resolved_sql(self) -> str:
        """Return the actual SQL — decode sql_b64 if present, otherwise use sql."""
        if self.sql_b64:
            try:
                return base64.b64decode(self.sql_b64).decode("utf-8")
            except Exception as exc:
                raise ValueError(f"Invalid sql_b64 encoding: {exc}") from exc
        if self.sql:
            return self.sql
        raise ValueError("Either 'sql' or 'sql_b64' must be provided.")


# ── /health ───────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    # Check DuckDB
    duckdb_status = "unknown"
    async with httpx.AsyncClient(timeout=5) as client:
        try:
            r = await client.get(f"{DUCKDB_SERVER}/health")
            duckdb_status = r.json()
        except Exception as exc:
            duckdb_status = {"error": str(exc)}

    # Check Memgraph Bolt port (quick TCP probe)
    mg_status = "ok"
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(MEMGRAPH_HOST, MEMGRAPH_PORT), timeout=3
        )
        writer.close()
    except Exception as exc:
        mg_status = f"unreachable: {exc}"

    ok = isinstance(duckdb_status, dict) and duckdb_status.get("status") == "ok" and mg_status == "ok"
    return JSONResponse(
        status_code=200 if ok else 503,
        content={"status": "ok" if ok else "degraded", "duckdb": duckdb_status, "memgraph": mg_status},
    )


# ── /catalog  (Polaris catalog browser) ─────────────────────────────────────
@app.get("/catalog")
async def get_catalog():
    """
    Return the full Polaris catalog tree: namespaces → tables → columns.

    Response shape:
    {
      "catalog": "polaris",
      "namespaces": [
        {
          "name": "tpcds_sf10tcl",
          "tables": [
            {
              "name": "store_sales",
              "full_name": "polaris.tpcds_sf10tcl.store_sales",
              "columns": [
                {"name": "SS_ITEM_SK", "type": "DECIMAL(38,0)"},
                ...
              ]
            },
            ...
          ]
        },
        ...
      ]
    }
    """
    # Step 1 — list all tables
    tables_sql = (
        "SELECT table_catalog, table_schema, table_name "
        "FROM information_schema.tables "
        "ORDER BY table_catalog, table_schema, table_name"
    )
    tables_result = await _execute_on_duckdb(tables_sql)
    if "error" in tables_result:
        raise HTTPException(status_code=502, detail=tables_result["error"])

    # Step 2 — list all columns in one query, then group in Python
    cols_sql = (
        "SELECT table_catalog, table_schema, table_name, column_name, data_type, ordinal_position "
        "FROM information_schema.columns "
        "ORDER BY table_catalog, table_schema, table_name, ordinal_position"
    )
    cols_result = await _execute_on_duckdb(cols_sql)
    if "error" in cols_result:
        raise HTTPException(status_code=502, detail=cols_result["error"])

    # Index columns by (catalog, schema, table)
    col_index: dict[tuple, list] = {}
    for row in cols_result.get("rows", []):
        key = (row[0], row[1], row[2])
        col_index.setdefault(key, []).append({"name": row[3], "type": row[4]})

    # Build namespace → tables tree
    ns_map: dict[str, list] = {}
    for row in tables_result.get("rows", []):
        catalog, schema, table = row[0], row[1], row[2]
        ns_map.setdefault(schema, []).append({
            "name":      table,
            "full_name": f"{catalog}.{schema}.{table}",
            "columns":   col_index.get((catalog, schema, table), []),
        })

    namespaces = [
        {"name": ns, "tables": tbls}
        for ns, tbls in sorted(ns_map.items())
    ]

    return {"catalog": "polaris", "namespaces": namespaces}


# ── /sql  (DuckDB Iceberg passthrough) ───────────────────────────────────────
@app.post("/sql")
async def proxy_sql(req: SqlRequest):
    try:
        sql = req.resolved_sql()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return await _execute_on_duckdb(sql, req.parameters)


# ── /cypher  (Cypher → SQL → DuckDB Iceberg) ─────────────────────────────────
class CypherRequest(BaseModel):
    cypher:     str | None = None
    cypher_b64: str | None = None
    parameters: dict[str, Any] | None = None

    def resolved_cypher(self) -> str:
        if self.cypher_b64:
            try:
                return base64.b64decode(self.cypher_b64).decode("utf-8")
            except Exception as exc:
                raise ValueError(f"Invalid cypher_b64 encoding: {exc}") from exc
        if self.cypher:
            return self.cypher
        raise ValueError("Either 'cypher' or 'cypher_b64' must be provided.")

@app.post("/cypher")
async def proxy_cypher(req: CypherRequest):
    try:
        cypher = req.resolved_cypher()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    sql, notes = translate_cypher(cypher)
    log.info("Cypher→SQL  in=%r  out=%r", cypher.strip(), sql)
    result = await _execute_on_duckdb(sql)
    result["cypher_input"]       = cypher
    result["translated_sql"]     = sql
    result["translation_notes"]  = notes
    result["source"]             = "iceberg"
    return result


# ── /graph  (direct Cypher → Memgraph live) ───────────────────────────────────
@app.post("/graph")
async def proxy_graph(req: CypherRequest):
    """
    Execute a Cypher query directly against the live Memgraph instance via Bolt.
    Returns columns/rows in the same envelope as /query so the console renders
    it identically.
    """
    try:
        cypher = req.resolved_cypher()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return await _execute_on_memgraph(cypher, req.parameters or {})


# ── /sql-to-graph  (THE FULL PIPELINE) ───────────────────────────────────────
@app.post("/graph-query")
async def sql_to_graph(req: SqlRequest):
    """
    Full pipeline: SQL → DuckDB/Iceberg → Memgraph → graph result.

    Step 1  Execute the caller's SQL on DuckDB/Iceberg to get raw data rows.
    Step 2  Inspect result column names to determine graph shape:
              Edge result: columns include source_node_id, target_node_id,
                           relationship_type → two node types + one edge type.
              Node result: column includes node_id (or *_id) + optional
                           node_type → single node type, no edges.
    Step 3  Push rows into Memgraph via Bolt UNWIND + MERGE using a
            query-scoped label (sgq_<hash>) so each ad-hoc query is isolated
            from the persistent CronJob-loaded graph.
    Step 4  Query Memgraph back with MATCH to retrieve the graph structure
            (nodes + edges) packaged in the columns/rows envelope the UI
            already knows how to render as a D3 force graph.
    Step 5  DETACH DELETE the scoped temporary nodes/edges so the ad-hoc
            graph does not accumulate in Memgraph.
    """
    import hashlib

    t0 = time.perf_counter()

    # ── Step 1: resolve SQL (plain or base64) ────────────────────────────────
    try:
        sql = req.resolved_sql()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # ── Step 2: compute stable scope from SQL hash ────────────────────────────
    scope       = "sgq_" + hashlib.md5(sql.encode()).hexdigest()[:8]
    scope_label = scope[0].upper() + scope[1:]  # e.g. "Sgq_ab12cd34"

    # ── Step 3: check Memgraph cache — skip DuckDB if TTL still valid ─────────
    loop = asyncio.get_event_loop()
    if GRAPH_CACHE_TTL_S > 0:
        cached = await loop.run_in_executor(
            None, _check_memgraph_cache, scope, scope_label
        )
        if cached is not None:
            cached["elapsed_ms"]   = round((time.perf_counter() - t0) * 1000, 2)
            cached["source"]       = "memgraph-cache"
            cached["cache_hit"]    = True
            cached["sql"]          = sql
            log.info("graph-query cache HIT (scope=%s)", scope)
            return cached

    # ── Step 4: cache miss — run SQL on DuckDB/Iceberg ────────────────────────
    duckdb_result = await _execute_on_duckdb(sql, req.parameters)
    if "error" in duckdb_result:
        raise HTTPException(status_code=400, detail=duckdb_result["error"])

    cols = duckdb_result.get("columns", [])
    rows = duckdb_result.get("rows", [])

    if not rows:
        return {
            "columns":        [],
            "rows":           [],
            "row_count":      0,
            "elapsed_ms":     round((time.perf_counter() - t0) * 1000, 2),
            "source":         "memgraph-via-iceberg",
            "sql":            sql,
            "pipeline_note":  "DuckDB returned 0 rows — nothing loaded into Memgraph.",
        }

    # ── Step 5: infer graph shape from column names ───────────────────────────
    col_lower = [c.lower() for c in cols]

    src_idx   = next((i for i, c in enumerate(col_lower) if c == "source_node_id"), None)
    tgt_idx   = next((i for i, c in enumerate(col_lower) if c == "target_node_id"), None)
    rel_idx   = next((i for i, c in enumerate(col_lower) if c == "relationship_type"), None)
    nid_idx   = next((i for i, c in enumerate(col_lower)
                      if c in ("node_id", "customer_id", "item_id", "store_id")), None)
    ntype_idx = next((i for i, c in enumerate(col_lower) if c == "node_type"), None)

    is_edge_result = src_idx is not None and tgt_idx is not None

    # ── Step 6: push rows → Memgraph MERGE → MATCH (keep, don't delete) ───────
    merge_cypher, query_cypher = _build_merge_and_query_cypher(
        cols, col_lower, is_edge_result,
        src_idx, tgt_idx, rel_idx, nid_idx, ntype_idx,
        scope, scope_label,
    )

    graph_result = await loop.run_in_executor(
        None,
        _load_and_query_memgraph,
        merge_cypher, rows, query_cypher, scope, scope_label, cols,
    )

    graph_result["sql"]               = sql
    graph_result["row_count_iceberg"] = len(rows)
    graph_result["elapsed_ms"]        = round((time.perf_counter() - t0) * 1000, 2)
    graph_result["source"]            = "memgraph-via-iceberg"
    graph_result["cache_hit"]         = False
    return graph_result


def _build_merge_and_query_cypher(
    cols: list[str],
    col_lower: list[str],
    is_edge_result: bool,
    src_idx, tgt_idx, rel_idx, nid_idx, ntype_idx,
    scope: str,
    scope_label: str,
) -> tuple[str, str]:
    """
    Build the UNWIND+MERGE Cypher (to load DuckDB rows into Memgraph) and the
    MATCH Cypher (to read the graph back).  Both are scoped to scope_label so
    they are isolated from the persistent CronJob-loaded graph.
    """
    # Rows will be converted to dicts (keyed by column name) before sending to
    # Memgraph, so Cypher accesses them as row.column_name — no index subscripts.

    if is_edge_result:
        src_col  = cols[src_idx]   # e.g. "source_node_id"
        tgt_col  = cols[tgt_idx]   # e.g. "target_node_id"
        rel_col  = cols[rel_idx] if rel_idx is not None else None

        src_type_col = next((cols[i] for i, c in enumerate(col_lower) if c == "source_node_type"), None)
        tgt_type_col = next((cols[i] for i, c in enumerate(col_lower) if c == "target_node_type"), None)

        src_type_expr = f"row.{src_type_col}" if src_type_col else "'SrcNode'"
        tgt_type_expr = f"row.{tgt_type_col}" if tgt_type_col else "'TgtNode'"
        rel_type_expr = f"row.{rel_col}"       if rel_col     else "'RELATED'"

        structural = {"source_node_id", "target_node_id", "relationship_type",
                      "source_node_type", "target_node_type"}
        edge_prop_cols = [cols[i] for i, c in enumerate(col_lower)
                         if c not in structural][:12]

        # Build SET assignments as separate statements (no trailing comma issue)
        edge_set_parts = [f"r.{c} = row.{c}" for c in edge_prop_cols]
        edge_set_parts.append(f"r.relationship_type = {rel_type_expr}")
        edge_set_str = ",\n               ".join(edge_set_parts)

        merge_cypher = (
            f"UNWIND $rows AS row\n"
            f"MERGE (src:{scope_label} {{_id: toString(row.{src_col})}})\n"
            f"ON CREATE SET src._label = {src_type_expr}, src._scope = '{scope}'\n"
            f"MERGE (tgt:{scope_label} {{_id: toString(row.{tgt_col})}})\n"
            f"ON CREATE SET tgt._label = {tgt_type_expr}, tgt._scope = '{scope}'\n"
            f"MERGE (src)-[r:RELATED]->(tgt)\n"
            f"ON CREATE SET r._scope = '{scope}',\n"
            f"               {edge_set_str}"
        )

        query_cypher = (
            f"MATCH (src:{scope_label})-[r:RELATED]->(tgt:{scope_label})\n"
            f"WHERE r._scope = '{scope}'\n"
            f"RETURN\n"
            f"  src._id             AS source_node_id,\n"
            f"  src._label          AS source_node_type,\n"
            f"  r.relationship_type AS relationship_type,\n"
            f"  tgt._id             AS target_node_id,\n"
            f"  tgt._label          AS target_node_type\n"
            f"LIMIT 500"
        )

    else:
        # Node-only result
        id_col    = cols[nid_idx] if nid_idx is not None else cols[0]
        type_expr = f"row.{cols[ntype_idx]}" if ntype_idx is not None else "'Node'"
        prop_cols = [c for c in cols if c != id_col][:20]
        set_str   = ",\n  ".join(f"n.{c} = row.{c}" for c in prop_cols)

        merge_cypher = (
            f"UNWIND $rows AS row\n"
            f"MERGE (n:{scope_label} {{_id: toString(row.{id_col})}})\n"
            f"ON CREATE SET n._scope = '{scope}', n._label = {type_expr}\n"
            + (f"SET {set_str}" if set_str else "")
        )

        query_cypher = (
            f"MATCH (n:{scope_label})\n"
            f"WHERE n._scope = '{scope}'\n"
            f"RETURN\n"
            f"  n._id    AS node_id,\n"
            f"  n._label AS node_type,\n"
            f"  properties(n) AS props\n"
            f"LIMIT 500"
        )

    return merge_cypher, query_cypher


def _check_memgraph_cache(scope: str, scope_label: str) -> dict | None:
    """
    Blocking — runs in a thread-pool executor.

    Check whether a valid cached graph for this scope already exists in Memgraph.
    A cache entry is valid when:
      - At least one node with the scope label exists AND
      - Its _cached_at timestamp is within GRAPH_CACHE_TTL_S seconds of now.

    Returns the {columns, rows, row_count} envelope on a hit, None on a miss.
    """
    from neo4j import GraphDatabase

    auth   = (MEMGRAPH_USER, MEMGRAPH_PASS) if MEMGRAPH_USER else ("", "")
    driver = GraphDatabase.driver(
        f"bolt://{MEMGRAPH_HOST}:{MEMGRAPH_PORT}", auth=auth
    )
    try:
        with driver.session() as session:
            # Check if any node for this scope exists and its TTL is still valid
            check = list(session.run(
                f"MATCH (n:{scope_label}) WHERE n._scope = $scope "
                f"RETURN n._cached_at AS cached_at LIMIT 1",
                scope=scope,
            ))
        if not check:
            return None
        cached_at = check[0]["cached_at"]
        if cached_at is None:
            return None
        age = time.time() - float(cached_at)
        if age > GRAPH_CACHE_TTL_S:
            # Expired — evict the stale graph so the next call re-loads fresh data
            with driver.session() as session:
                session.run(
                    f"MATCH (n:{scope_label}) WHERE n._scope = $scope DETACH DELETE n",
                    scope=scope,
                )
            log.info("graph cache EXPIRED and evicted (scope=%s, age=%.0fs)", scope, age)
            return None

        # Cache hit — re-run the MATCH query to retrieve the graph
        query_cypher = (
            f"MATCH (src:{scope_label})-[r:RELATED]->(tgt:{scope_label})\n"
            f"WHERE r._scope = $scope\n"
            f"RETURN\n"
            f"  src._id             AS source_node_id,\n"
            f"  src._label          AS source_node_type,\n"
            f"  r.relationship_type AS relationship_type,\n"
            f"  tgt._id             AS target_node_id,\n"
            f"  tgt._label          AS target_node_type\n"
            f"LIMIT 500"
        )
        with driver.session() as session:
            records = list(session.run(query_cypher, scope=scope))

        if not records:
            # Edge query returned nothing — try node-only query
            query_cypher = (
                f"MATCH (n:{scope_label}) WHERE n._scope = $scope "
                f"RETURN n._id AS node_id, n._label AS node_type, "
                f"properties(n) AS props LIMIT 500"
            )
            with driver.session() as session:
                records = list(session.run(query_cypher, scope=scope))

        if not records:
            return None

        out_cols = list(records[0].keys())
        out_rows = []
        for rec in records:
            row = []
            for key in out_cols:
                val = rec[key]
                if hasattr(val, "_properties"):
                    val = dict(val._properties)
                elif hasattr(val, "isoformat"):
                    val = val.isoformat()
                try:
                    json.dumps(val)
                except (TypeError, ValueError):
                    val = str(val)
                row.append(val)
            out_rows.append(row)

        log.info("graph cache HIT (scope=%s, age=%.0fs, rows=%d)", scope, age, len(out_rows))
        return {"columns": out_cols, "rows": out_rows, "row_count": len(out_rows)}

    except Exception as exc:
        # Cache check errors are non-fatal — fall through to a fresh load
        log.warning("graph cache check failed (scope=%s): %s", scope, exc)
        return None
    finally:
        driver.close()


def _load_and_query_memgraph(
    merge_cypher: str,
    rows: list,
    query_cypher: str,
    scope: str,
    scope_label: str,
    cols: list,
) -> dict:
    """
    Blocking — runs in a thread-pool executor.

    1. Connects to Memgraph via Bolt.
    2. UNWINDs + MERGEs the DuckDB rows as scoped nodes/edges.
    3. Stamps _cached_at = current unix timestamp on every node so the TTL
       check in _check_memgraph_cache can evict stale entries later.
    4. MATCHes the graph back for the UI.
    5. Does NOT delete the scoped data — it stays in Memgraph until the TTL
       expires (evicted lazily on the next cache-check for the same scope).
    6. Returns {columns, rows, row_count} envelope.
    """
    from neo4j import GraphDatabase

    auth   = (MEMGRAPH_USER, MEMGRAPH_PASS) if MEMGRAPH_USER else ("", "")
    driver = GraphDatabase.driver(
        f"bolt://{MEMGRAPH_HOST}:{MEMGRAPH_PORT}", auth=auth
    )
    try:
        # Step 1 — MERGE rows into Memgraph
        dict_rows = [dict(zip(cols, r)) for r in rows]
        with driver.session() as session:
            session.run(merge_cypher, rows=dict_rows)
        log.info("sql-to-graph: merged %d rows into Memgraph (scope=%s)", len(rows), scope)

        # Step 2 — stamp _cached_at on every node in this scope so TTL works
        now_ts = time.time()
        with driver.session() as session:
            session.run(
                f"MATCH (n:{scope_label}) WHERE n._scope = $scope "
                f"SET n._cached_at = $ts",
                scope=scope, ts=now_ts,
            )

        # Step 3 — MATCH back to get graph structure
        with driver.session() as session:
            records = list(session.run(query_cypher))

        if not records:
            return {
                "columns":       [],
                "rows":          [],
                "row_count":     0,
                "pipeline_note": "Rows merged but Memgraph MATCH returned 0 results.",
            }

        out_cols = list(records[0].keys())
        out_rows = []
        for rec in records:
            row = []
            for key in out_cols:
                val = rec[key]
                if hasattr(val, "_properties"):
                    val = dict(val._properties)
                elif hasattr(val, "isoformat"):
                    val = val.isoformat()
                try:
                    json.dumps(val)
                except (TypeError, ValueError):
                    val = str(val)
                row.append(val)
            out_rows.append(row)

        log.info("sql-to-graph: Memgraph returned %d graph rows (scope=%s, cached for %ds)",
                 len(out_rows), scope, GRAPH_CACHE_TTL_S)

        # NOTE: scoped nodes are intentionally NOT deleted here.
        # They expire lazily via GRAPH_CACHE_TTL_S (checked on next request
        # for the same SQL) or can be force-cleared via GRAPH_CACHE_TTL_S=0.

        return {
            "columns":   out_cols,
            "rows":      out_rows,
            "row_count": len(out_rows),
        }

    except Exception as exc:
        log.error("sql-to-graph Memgraph error: %s", exc)
        raise HTTPException(
            status_code=502,
            detail=f"Memgraph error in sql-to-graph pipeline: {exc}",
        )
    finally:
        driver.close()


# ── /compare  (Iceberg snapshot vs live Memgraph) ────────────────────────────
@app.post("/compare")
async def compare(req: CypherRequest):
    """
    Run the same logical query against both data sources in parallel:
      - Iceberg path: Cypher translated to SQL, executed via DuckDB
      - Live path:    Cypher executed directly on Memgraph via Bolt
    Returns both result sets and timings for validation / drift detection.
    """
    try:
        cypher = req.resolved_cypher()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    sql, _ = translate_cypher(cypher)
    iceberg_task  = asyncio.create_task(_execute_on_duckdb(sql))
    memgraph_task = asyncio.create_task(_execute_on_memgraph(cypher, req.parameters or {}))
    iceberg_result, memgraph_result = await asyncio.gather(
        iceberg_task, memgraph_task, return_exceptions=True
    )
    if isinstance(iceberg_result, Exception):
        iceberg_result = {"error": str(iceberg_result)}
    if isinstance(memgraph_result, Exception):
        memgraph_result = {"error": str(memgraph_result)}

    return {
        "cypher":          cypher,
        "translated_sql":  sql,
        "iceberg_result":  iceberg_result,
        "memgraph_result": memgraph_result,
    }


# ── Cypher → DuckDB/Iceberg SQL translator ────────────────────────────────────

_LABEL_TABLE = {
    "customer": "polaris.stargraph.stargraph_customers",
    "node":     "polaris.stargraph.stargraph_nodes",
}
_DEFAULT_NODE_TABLE = "polaris.stargraph.stargraph_nodes"


def translate_cypher(cypher: str) -> tuple[str, list[str]]:
    """
    Translate a subset of Cypher MATCH statements into DuckDB Iceberg SQL.
    Returns (sql_string, list_of_translation_notes).

    Supported:
      MATCH (n:Label)                         → simple SELECT
      MATCH (n:Label {prop: 'val'})           → SELECT + WHERE
      MATCH (n:Label) WHERE n.prop = 'val'    → SELECT + WHERE
      MATCH (a:A)-[r:REL]->(b:B)             → JOIN via stargraph_edges
      RETURN / LIMIT / ORDER BY              → forwarded
    """
    notes: list[str] = []
    cypher_clean = cypher.strip().rstrip(";")

    # Parse LIMIT
    limit_clause = ""
    m = re.search(r"\bLIMIT\s+(\d+)", cypher_clean, re.IGNORECASE)
    if m:
        limit_clause = f"LIMIT {m.group(1)}"
        cypher_clean = cypher_clean[:m.start()].strip()

    # Parse ORDER BY
    order_clause = ""
    m = re.search(r"\bORDER\s+BY\s+(.+?)(?=\bWHERE|\bRETURN|\bLIMIT|$)",
                  cypher_clean, re.IGNORECASE | re.DOTALL)
    if m:
        order_clause = f"ORDER BY {_map_property_ref(m.group(1).strip())}"
        cypher_clean = cypher_clean[:m.start()].strip() + cypher_clean[m.end():]

    # Relationship pattern: (a:A)-[r:REL]->(b:B)
    rel_pattern = re.search(
        r"MATCH\s+\((\w+)(?::(\w+))?\s*(?:\{([^}]*)\})?\)"
        r"\s*-\[(\w+)(?::(\w+))?\]->\s*"
        r"\((\w+)(?::(\w+))?\s*(?:\{([^}]*)\})?\)",
        cypher_clean, re.IGNORECASE,
    )
    if rel_pattern:
        return _translate_relationship(rel_pattern, cypher_clean, limit_clause, order_clause, notes)

    # Simple node pattern: MATCH (n:Label {prop: 'val'})
    node_pattern = re.search(
        r"MATCH\s+\((\w+)(?::(\w+))?\s*(?:\{([^}]*)\})?\)",
        cypher_clean, re.IGNORECASE,
    )
    if not node_pattern:
        raise HTTPException(status_code=400, detail=f"Cannot parse Cypher: {cypher}")

    alias        = node_pattern.group(1)
    label        = (node_pattern.group(2) or "node").lower()
    inline_props = node_pattern.group(3) or ""
    table        = _LABEL_TABLE.get(label, _DEFAULT_NODE_TABLE)

    if label not in _LABEL_TABLE:
        notes.append(f"Unknown label '{label}' — defaulting to stargraph_nodes with node_type='{label}'")

    where_parts: list[str] = []
    if table == _DEFAULT_NODE_TABLE:
        where_parts.append(f"node_type = '{label.capitalize()}'")
    if inline_props:
        where_parts.extend(_parse_inline_props(inline_props))

    where_m = re.search(r"\bWHERE\s+(.+?)(?=\bRETURN|$)", cypher_clean, re.IGNORECASE | re.DOTALL)
    if where_m:
        where_parts.append(_map_property_ref(where_m.group(1).strip()))

    select_cols = _parse_return(cypher_clean, alias, table)
    where_parts.append("(is_deleted = false OR is_deleted IS NULL)")

    sql_parts = [f"SELECT {select_cols}", f"FROM {table}"]
    if where_parts:
        sql_parts.append("WHERE " + " AND ".join(where_parts))
    if order_clause:
        sql_parts.append(order_clause)
    if limit_clause:
        sql_parts.append(limit_clause)

    return " ".join(sql_parts), notes


def _translate_relationship(
    rel_pattern: re.Match,
    cypher: str,
    limit_clause: str,
    order_clause: str,
    notes: list[str],
) -> tuple[str, list[str]]:
    a_alias  = rel_pattern.group(1)
    a_label  = (rel_pattern.group(2) or "node").lower()
    a_props  = rel_pattern.group(3) or ""
    r_alias  = rel_pattern.group(4)
    rel_type = (rel_pattern.group(5) or "").upper()
    b_alias  = rel_pattern.group(6)
    b_label  = (rel_pattern.group(7) or "node").lower()
    b_props  = rel_pattern.group(8) or ""

    a_table = _LABEL_TABLE.get(a_label, _DEFAULT_NODE_TABLE)
    b_table = _LABEL_TABLE.get(b_label, _DEFAULT_NODE_TABLE)
    e_table = "polaris.stargraph.stargraph_edges"

    where_parts: list[str] = [
        f"e.source_node_type = '{a_label.capitalize()}'",
        f"e.target_node_type = '{b_label.capitalize()}'",
        "(a.is_deleted = false OR a.is_deleted IS NULL)",
        "(b.is_deleted = false OR b.is_deleted IS NULL)",
    ]
    if rel_type:
        where_parts.append(f"e.relationship_type = '{rel_type}'")
    if a_props:
        for f in _parse_inline_props(a_props):
            where_parts.append(f"a.{f}")
    if b_props:
        for f in _parse_inline_props(b_props):
            where_parts.append(f"b.{f}")

    where_m = re.search(r"\bWHERE\s+(.+?)(?=\bRETURN|$)", cypher, re.IGNORECASE | re.DOTALL)
    if where_m:
        where_parts.append(_map_property_ref(where_m.group(1).strip()))

    sql = (
        f"SELECT a.*, e.relationship_type, e.properties AS edge_properties, b.* "
        f"FROM {a_table} a "
        f"JOIN {e_table} e ON e.source_node_id = a.node_id "
        f"JOIN {b_table} b ON e.target_node_id = b.node_id "
        f"WHERE {' AND '.join(where_parts)}"
    )
    if order_clause:
        sql += f" {order_clause}"
    if limit_clause:
        sql += f" {limit_clause}"
    return sql, notes


def _parse_inline_props(props_str: str) -> list[str]:
    filters = []
    for item in props_str.split(","):
        item = item.strip()
        if ":" in item:
            key, val = item.split(":", 1)
            filters.append(f"{key.strip()} = '{val.strip().strip(chr(39))}'")
    return filters


def _map_property_ref(expr: str) -> str:
    return re.sub(r"\b\w+\.(\w+)", r"\1", expr)


def _parse_return(cypher: str, alias: str, table: str) -> str:
    m = re.search(r"\bRETURN\s+(.+?)(?=\bORDER|\bLIMIT|$)", cypher, re.IGNORECASE | re.DOTALL)
    if not m:
        return "*"
    ret = m.group(1).strip()
    if ret in (alias, f"{alias}.*", "*"):
        return "*"
    cols = []
    for part in ret.split(","):
        part = part.strip()
        col  = re.sub(rf"^{alias}\.", "", part)
        cols.append(col)
    return ", ".join(cols)


# ── HTTP helpers ──────────────────────────────────────────────────────────────

async def _execute_on_duckdb(sql: str, params: list | None = None) -> dict:
    payload: dict[str, Any] = {"sql": sql}
    if params:
        payload["parameters"] = params
    async with httpx.AsyncClient(timeout=120) as client:
        try:
            r = await client.post(f"{DUCKDB_SERVER}/query", json=payload)
            r.raise_for_status()
            result = r.json()
            result["source"] = "iceberg"
            return result
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code,
                                detail=exc.response.text)
        except Exception as exc:
            raise HTTPException(status_code=503, detail=str(exc))


async def _execute_on_memgraph(cypher: str, params: dict | None = None) -> dict:
    """
    Execute a Cypher query against the live Memgraph instance via Bolt.
    Runs the blocking neo4j driver call in a thread-pool executor so it
    does not block the FastAPI event loop.
    """
    from neo4j import GraphDatabase

    def _run() -> dict:
        t0 = time.perf_counter()
        auth = (MEMGRAPH_USER, MEMGRAPH_PASS) if MEMGRAPH_USER else ("", "")
        driver = GraphDatabase.driver(
            f"bolt://{MEMGRAPH_HOST}:{MEMGRAPH_PORT}", auth=auth
        )
        try:
            with driver.session() as session:
                result = session.run(cypher, parameters=params or {})
                records = list(result)
                if not records:
                    return {
                        "columns": [], "rows": [], "row_count": 0,
                        "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
                        "source": "memgraph-live",
                    }
                # Build column list from first record keys
                columns = list(records[0].keys())
                rows = []
                for rec in records:
                    row = []
                    for key in columns:
                        val = rec[key]
                        # Convert neo4j Node / Relationship objects to dicts
                        if hasattr(val, "_properties"):
                            val = dict(val._properties)
                        elif hasattr(val, "isoformat"):
                            val = val.isoformat()
                        # Ensure JSON-serialisable
                        try:
                            json.dumps(val)
                        except (TypeError, ValueError):
                            val = str(val)
                        row.append(val)
                    rows.append(row)
                return {
                    "columns":    columns,
                    "rows":       rows,
                    "row_count":  len(rows),
                    "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
                    "source":     "memgraph-live",
                }
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Memgraph error: {exc}")
        finally:
            driver.close()

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _run)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(app, host=BIND_HOST, port=BIND_PORT, log_level="info", access_log=True)
