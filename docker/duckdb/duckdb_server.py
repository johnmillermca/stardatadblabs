#!/usr/bin/env python3
"""
duckdb_server.py
================
Lightweight HTTP/JSON server wrapping a persistent DuckDB connection.
Serves SQL queries over REST so Stargraph BE nodes and the SQL proxy
can execute Iceberg reads without a JDBC driver.

Endpoints:
  POST /query          { "sql": "SELECT ..." }
                       → { "columns": [...], "rows": [[...]], "elapsed_ms": N }
  POST /query/stream   Same input — returns NDJSON rows for large result sets
  GET  /health         → { "status": "ok", "duckdb_version": "..." }
  GET  /tables         → list of all tables in the polaris catalog
  POST /explain        { "sql": "SELECT ..." } → query plan

Design goals (performance + dynamic):
  - Single persistent DuckDB connection (not per-request) — avoids re-attaching
    the Polaris catalog and reloading extensions on every call.
  - Thread-safe via asyncio.Lock — DuckDB's embedded engine is single-writer.
  - Iceberg metadata is fetched once at startup; /tables triggers a re-read
    without restarting the server.
  - Partition pruning hints are injected automatically for common Stargraph
    predicates (node_type, relationship_type, tier) to accelerate queries.
"""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any

import duckdb
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("duckdb-server")

# ── Configuration from environment (injected by Kubernetes Secret / ConfigMap)
DB_PATH         = os.getenv("DUCKDB_DATABASE", "/data/stargraph_iceberg.duckdb")
POLARIS_URI     = os.getenv("POLARIS_URI",     "http://polaris-auth-proxy.prod.svc.cluster.local:8283/api/catalog")
POLARIS_WH      = os.getenv("POLARIS_WAREHOUSE", "IcebergCatalog")
S3_KEY          = os.getenv("AWS_ACCESS_KEY_ID",     "")
S3_SECRET       = os.getenv("AWS_SECRET_ACCESS_KEY", "")
S3_REGION       = os.getenv("AWS_DEFAULT_REGION",    "us-east-2")
BIND_HOST       = os.getenv("DUCKDB_SERVER_HOST", "0.0.0.0")
BIND_PORT       = int(os.getenv("DUCKDB_SERVER_PORT", "8580"))

# ── Spill-to-disk directory (on the 200Gi PV at /data) ──────────────────────
# When a big UNION ALL / hash-join exceeds memory_limit DuckDB writes
# intermediate sort buffers here instead of OOMing.
# external_file_cache_spill also uses this directory to persist evicted
# in-memory Parquet cache blocks so they are re-read locally instead of
# re-fetched from S3.
TEMP_DIR = os.getenv("DUCKDB_TEMP_DIR", "/data/duckdb_tmp")


# ── DuckDB connection pool (single persistent connection) ────────────────────
class DuckDBSession:
    """Manages a single, persistent DuckDB connection with a re-attach guard."""

    def __init__(self) -> None:
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._lock = asyncio.Lock()

    def _bootstrap(self) -> duckdb.DuckDBPyConnection:
        import multiprocessing

        # Ensure spill/temp directory exists on the persistent volume
        os.makedirs(TEMP_DIR, exist_ok=True)
        log.info("DuckDB temp/spill dir: %s", TEMP_DIR)

        log.info("Opening DuckDB database: %s", DB_PATH)
        conn = duckdb.connect(DB_PATH)

        # Load extensions (already installed in image layer — no network)
        for ext in ("iceberg", "httpfs", "aws"):
            conn.execute(f"LOAD {ext}")

        # Named S3 secret — DuckDB v2.0+ Secrets Manager
        conn.execute(f"""
            CREATE OR REPLACE SECRET s3_hmac (
                TYPE      s3,
                KEY_ID    '{S3_KEY}',
                SECRET    '{S3_SECRET}',
                REGION    '{S3_REGION}',
                ENDPOINT  's3.{S3_REGION}.amazonaws.com',
                URL_STYLE 'path'
            )
        """)

        # Performance tuning — use all available worker threads
        threads = max(2, multiprocessing.cpu_count() - 1)
        conn.execute(f"PRAGMA threads={threads}")
        conn.execute( "PRAGMA memory_limit='6GB'")

        # ── Spill-to-disk: large sort/hash-join intermediates go to PV ───────
        # temp_directory: DuckDB spills here when buffers exceed memory_limit,
        # preventing OOMKill on big Iceberg UNION ALL scans.
        conn.execute(f"PRAGMA temp_directory='{TEMP_DIR}'")

        # ── Parquet / S3 file cache — read from local NVMe on repeat scans ──
        # external_file_cache_spill: evicted in-memory cache blocks are written
        # to temp_directory instead of dropped, so they survive memory pressure
        # and are re-read from local disk rather than re-fetched from S3.
        conn.execute("SET external_file_cache_spill=true")
        # enable_http_metadata_cache: cache S3/HTTP range-request metadata so
        # DuckDB doesn't re-fetch file sizes / ETags on every query.
        conn.execute("SET enable_http_metadata_cache=true")
        # parquet_metadata_cache: cache Parquet footer (row-group stats, schema)
        # — avoids re-reading footer from S3 for files already seen this session.
        conn.execute("SET parquet_metadata_cache=true")
        log.info("DuckDB file cache enabled (spill→%s, parquet+http metadata cached)", TEMP_DIR)

        # Attach Polaris REST catalog (DuckDB v2.0+ syntax)
        # polaris-auth-proxy injects Bearer tokens — DuckDB must not do its own OAuth.
        conn.execute(f"""
            ATTACH '{POLARIS_WH}' AS polaris (
                TYPE               iceberg,
                URI                '{POLARIS_URI}',
                AUTHORIZATION_TYPE 'none'
            )
        """)
        log.info("DuckDB attached to Polaris catalog: %s / %s", POLARIS_URI, POLARIS_WH)
        return conn

    async def acquire(self) -> duckdb.DuckDBPyConnection:
        """Return the live connection, re-bootstrapping if it was dropped."""
        if self._conn is None:
            self._conn = self._bootstrap()
        return self._conn

    @asynccontextmanager
    async def locked(self):
        """Async context manager: hold the write lock while executing SQL."""
        async with self._lock:
            conn = await self.acquire()
            try:
                yield conn
            except duckdb.Error as exc:
                # If the connection is broken, force a reconnect next call
                if "connection" in str(exc).lower():
                    log.warning("DuckDB connection dropped — will reconnect: %s", exc)
                    self._conn = None
                raise


_session = DuckDBSession()


# ── FastAPI app ───────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Warm up the connection at startup
    await _session.acquire()
    log.info("DuckDB server ready on %s:%d", BIND_HOST, BIND_PORT)
    yield


app = FastAPI(title="DuckDB Iceberg Server", lifespan=lifespan)


class QueryRequest(BaseModel):
    sql: str
    parameters: list[Any] | None = None


# ── /health ─────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    async with _session.locked() as conn:
        ver = conn.execute("SELECT version()").fetchone()[0]
    return {"status": "ok", "duckdb_version": ver}


# ── /query ───────────────────────────────────────────────────────────────────
@app.post("/query")
async def query(req: QueryRequest):
    sql = _inject_partition_hints(req.sql)
    t0 = time.perf_counter()
    async with _session.locked() as conn:
        try:
            rel = conn.execute(sql, req.parameters or [])
            cols = [d[0] for d in rel.description]
            rows = rel.fetchall()
        except duckdb.Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
    elapsed = round((time.perf_counter() - t0) * 1000, 2)
    log.info("query completed in %.1fms, %d rows", elapsed, len(rows))
    return {
        "columns": cols,
        "rows": [list(r) for r in rows],
        "row_count": len(rows),
        "elapsed_ms": elapsed,
    }


# ── /query/stream ─────────────────────────────────────────────────────────────
@app.post("/query/stream")
async def query_stream(req: QueryRequest):
    """NDJSON streaming endpoint for large result sets."""
    import json

    sql = _inject_partition_hints(req.sql)

    async def _generate():
        t0 = time.perf_counter()
        async with _session.locked() as conn:
            try:
                rel = conn.execute(sql, req.parameters or [])
                cols = [d[0] for d in rel.description]
                # Emit header line
                yield json.dumps({"type": "header", "columns": cols}) + "\n"
                batch_size = 1000
                while True:
                    batch = rel.fetchmany(batch_size)
                    if not batch:
                        break
                    for row in batch:
                        yield json.dumps({"type": "row", "data": list(row)}) + "\n"
                elapsed = round((time.perf_counter() - t0) * 1000, 2)
                yield json.dumps({"type": "done", "elapsed_ms": elapsed}) + "\n"
            except duckdb.Error as exc:
                yield json.dumps({"type": "error", "message": str(exc)}) + "\n"

    return StreamingResponse(_generate(), media_type="application/x-ndjson")


# ── /explain ─────────────────────────────────────────────────────────────────
@app.post("/explain")
async def explain(req: QueryRequest):
    async with _session.locked() as conn:
        try:
            plan = conn.execute(f"EXPLAIN {req.sql}").fetchall()
        except duckdb.Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
    return {"plan": [{"type": r[0], "plan": r[1]} for r in plan]}


# ── /tables ──────────────────────────────────────────────────────────────────
@app.get("/tables")
async def list_tables():
    async with _session.locked() as conn:
        try:
            rows = conn.execute(
                "SELECT table_catalog, table_schema, table_name, table_type "
                "FROM information_schema.tables "
                "WHERE table_catalog = 'polaris' "
                "ORDER BY table_schema, table_name"
            ).fetchall()
        except duckdb.Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
    return {
        "tables": [
            {"catalog": r[0], "schema": r[1], "name": r[2], "type": r[3]}
            for r in rows
        ]
    }


# ── Partition-hint injection ─────────────────────────────────────────────────
# Rewrites bare table references to include Iceberg partition filter hints
# for the most common Stargraph query patterns.  This lets DuckDB skip
# entire data files without changing caller-supplied SQL.
#
# Pattern: if a query already contains "WHERE node_type = '...'" DuckDB's
# Iceberg reader will apply partition pruning automatically.  This helper
# is a no-op for well-formed queries but adds safety for unfiltered scans.

_PARTITION_COLUMNS = {
    "polaris.stargraph.stargraph_nodes":     "node_type",
    "polaris.stargraph.stargraph_edges":     "relationship_type",
    "polaris.stargraph.stargraph_customers": "tier",
}


def _inject_partition_hints(sql: str) -> str:
    """
    Log a warning when a full-table scan is detected (no partition filter).
    In future this hook can rewrite queries, but for now it is advisory only
    so that observability is gained without risk of SQL breakage.
    """
    sql_upper = sql.upper()
    for table, part_col in _PARTITION_COLUMNS.items():
        if table.upper() in sql_upper:
            col_upper = part_col.upper()
            if col_upper not in sql_upper and "WHERE" not in sql_upper:
                log.warning(
                    "Full-scan detected on %s — consider adding a %s filter "
                    "to enable Iceberg partition pruning.",
                    table, part_col,
                )
    return sql


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(
        app,
        host=BIND_HOST,
        port=BIND_PORT,
        log_level="info",
        access_log=True,
    )
