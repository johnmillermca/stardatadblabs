-- =============================================================================
-- duckdb_iceberg_setup.sql
-- Create Iceberg tables via the Polaris REST catalog in DuckDB v2.0.0.
--
-- Bucket   : s3://xdatatoiceberg1/
-- Catalog  : polaris (Polaris REST → IcebergCatalog warehouse)
-- S3 region: us-east-2
--
-- Execute via the DuckDB HTTP API (the server holds an exclusive file lock):
--   curl -X POST http://192.168.1.58:30580/query \
--        -H "Content-Type: application/json" \
--        -d '{"sql": "<statement>"}'
--
-- DuckDB v2.0.0 notes:
--   - JSON type is not a valid Iceberg type → use VARCHAR
--   - DEFAULT values require format-version ≥ 3 → omit DEFAULT constraints
--   - USING iceberg / TBLPROPERTIES is Spark syntax — not supported in DuckDB
--   - The polaris catalog must already be ATTACHed (done in entrypoint.sh)
--   - The stargraph namespace must exist in Polaris first (created via REST API)
-- =============================================================================

-- ── Create namespace in Polaris first (run once from kubectl) ─────────────────
-- TOKEN=$(kubectl exec -n prod deployment/polaris -- curl -s -X POST \
--   "http://localhost:8181/api/catalog/v1/oauth/tokens" \
--   -H "Content-Type: application/x-www-form-urlencoded" \
--   -d "grant_type=client_credentials&client_id=<bootstrap_id>&client_secret=<bootstrap_secret>&scope=PRINCIPAL_ROLE:ALL" \
--   | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])")
--
-- kubectl exec -n prod deployment/polaris -- curl -s -X POST \
--   "http://localhost:8181/api/catalog/v1/IcebergCatalog/namespaces" \
--   -H "Authorization: Bearer $TOKEN" \
--   -H "Content-Type: application/json" \
--   -d '{"namespace":["stargraph"],"properties":{}}'

-- =============================================================================
-- TABLE 1: stargraph_nodes
-- Mirrors the node entities that Stargraph manages.
-- =============================================================================
CREATE TABLE IF NOT EXISTS polaris.stargraph.stargraph_nodes (
    node_id        VARCHAR,        -- internal Stargraph node ID
    node_type      VARCHAR,        -- e.g. Customer, Product, Order
    properties     VARCHAR,        -- JSON payload (as VARCHAR — Iceberg limitation)
    created_at     TIMESTAMPTZ,
    updated_at     TIMESTAMPTZ,
    is_deleted     BOOLEAN,
    snap_timestamp TIMESTAMPTZ     -- Iceberg write timestamp
);

-- =============================================================================
-- TABLE 2: stargraph_edges
-- Graph edges / relationships from Stargraph.
-- =============================================================================
CREATE TABLE IF NOT EXISTS polaris.stargraph.stargraph_edges (
    edge_id           VARCHAR,
    relationship_type VARCHAR,       -- e.g. PURCHASED, REVIEWED
    source_node_id    VARCHAR,
    source_node_type  VARCHAR,
    target_node_id    VARCHAR,
    target_node_type  VARCHAR,
    properties        VARCHAR,       -- JSON payload (as VARCHAR)
    created_at        TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ,
    is_deleted        BOOLEAN,
    snap_timestamp    TIMESTAMPTZ
);

-- =============================================================================
-- TABLE 3: stargraph_customers  (typed projection of Customer nodes)
-- =============================================================================
CREATE TABLE IF NOT EXISTS polaris.stargraph.stargraph_customers (
    customer_id    VARCHAR,
    first_name     VARCHAR,
    last_name      VARCHAR,
    email          VARCHAR,
    city           VARCHAR,
    country        VARCHAR,
    tier           VARCHAR,
    credit_limit   DECIMAL(18,2),
    is_deleted     BOOLEAN,
    deleted_at     TIMESTAMPTZ,
    snap_timestamp TIMESTAMPTZ
);

-- ── Verify ────────────────────────────────────────────────────────────────────
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_catalog = 'polaris'
  AND table_schema  = 'stargraph'
ORDER BY table_name;
