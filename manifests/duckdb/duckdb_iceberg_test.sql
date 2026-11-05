-- =============================================================================
-- duckdb_iceberg_test.sql
-- End-to-end tests for the DuckDB / Polaris / Iceberg integration.
--
-- Run against the DuckDB server's /sql endpoint (passthrough):
--   curl -s -X POST http://192.168.1.50:30680/sql \
--     -H "Content-Type: application/json" \
--     -d '{"sql": "<paste SQL here>"}' | python3 -m json.tool
--
-- Or directly in DuckDB:
--   duckdb /data/stargraph_iceberg.duckdb < duckdb_iceberg_test.sql
-- =============================================================================

-- ── T0: Extensions loaded correctly ─────────────────────────────────────────
SELECT 'T0' AS test, 'Extensions' AS name;
SELECT extension_name, loaded, installed
FROM duckdb_extensions()
WHERE extension_name IN ('iceberg', 'httpfs', 'aws');

-- ── T1: Polaris catalog attached and schema visible ──────────────────────────
SELECT 'T1' AS test, 'Catalog visible' AS name;
SHOW ALL TABLES IN polaris.stargraph;

-- ── T2: S3 credentials valid — read Iceberg metadata from bucket ─────────────
-- This fails fast if HMAC credentials are wrong (403 from S3).
SELECT 'T2' AS test, 'S3 HMAC credentials' AS name;
SELECT COUNT(*) AS nodes_count
FROM polaris.stargraph.stargraph_nodes;

-- ── T3: stargraph_nodes — basic SELECT with partition pruning ────────────────
SELECT 'T3' AS test, 'Nodes — node_type partition prune' AS name;
SELECT node_type, COUNT(*) AS cnt
FROM polaris.stargraph.stargraph_nodes
WHERE node_type IN ('Customer', 'Product', 'Order')
  AND (is_deleted = false OR is_deleted IS NULL)
GROUP BY node_type
ORDER BY cnt DESC;

-- ── T4: stargraph_customers — typed projection ───────────────────────────────
SELECT 'T4' AS test, 'Customers — tier partition prune' AS name;
SELECT tier, COUNT(*) AS cnt, AVG(credit_limit) AS avg_credit
FROM polaris.stargraph.stargraph_customers
WHERE tier IN ('GOLD', 'PLATINUM', 'SILVER')
  AND (is_deleted = false OR is_deleted IS NULL)
GROUP BY tier
ORDER BY avg_credit DESC;

-- ── T5: stargraph_edges — relationship traversal ─────────────────────────────
SELECT 'T5' AS test, 'Edges — relationship_type partition prune' AS name;
SELECT relationship_type, COUNT(*) AS edge_count
FROM polaris.stargraph.stargraph_edges
WHERE relationship_type IN ('PURCHASED', 'REVIEWED', 'VIEWED')
  AND (is_deleted = false OR is_deleted IS NULL)
GROUP BY relationship_type
ORDER BY edge_count DESC;

-- ── T6: Iceberg time-travel — snapshot history ───────────────────────────────
SELECT 'T6' AS test, 'Iceberg snapshots' AS name;
SELECT snapshot_id, committed_at, operation, summary
FROM iceberg_snapshots('polaris.stargraph.stargraph_customers')
ORDER BY committed_at DESC
LIMIT 5;

-- ── T7: Cypher-equivalent: MATCH (c:Customer {tier:'GOLD'}) RETURN c LIMIT 10
-- Equivalent to the SQL proxy /cypher endpoint.
SELECT 'T7' AS test, 'Cypher-equiv: Customers tier=GOLD' AS name;
SELECT customer_id, first_name, last_name, email, city, tier, credit_limit
FROM polaris.stargraph.stargraph_customers
WHERE tier = 'GOLD'
  AND (is_deleted = false OR is_deleted IS NULL)
ORDER BY customer_id
LIMIT 10;

-- ── T8: Cypher-equivalent:
--   MATCH (c:Customer)-[r:PURCHASED]->(p:Product) RETURN c, r, p LIMIT 10
SELECT 'T8' AS test, 'Cypher-equiv: Customer PURCHASED Product JOIN' AS name;
SELECT
    c.customer_id,
    c.first_name,
    c.email,
    e.relationship_type,
    e.properties   AS purchase_details,
    n.node_id       AS product_id,
    n.properties    AS product_properties
FROM polaris.stargraph.stargraph_customers  c
JOIN polaris.stargraph.stargraph_edges      e
  ON e.source_node_id   = c.customer_id
  AND e.source_node_type = 'Customer'
  AND e.relationship_type = 'PURCHASED'
JOIN polaris.stargraph.stargraph_nodes      n
  ON n.node_id     = e.target_node_id
  AND n.node_type  = 'Product'
WHERE (c.is_deleted = false OR c.is_deleted IS NULL)
  AND (n.is_deleted = false OR n.is_deleted IS NULL)
ORDER BY c.customer_id
LIMIT 10;

-- ── T9: Soft-delete correctness ──────────────────────────────────────────────
SELECT 'T9' AS test, 'Soft-delete — is_deleted counts' AS name;
SELECT
    is_deleted,
    COUNT(*) AS row_count
FROM polaris.stargraph.stargraph_customers
GROUP BY is_deleted
ORDER BY is_deleted;

-- ── T10: Iceberg metadata files visible in S3 ─────────────────────────────────
SELECT 'T10' AS test, 'Iceberg metadata files on S3' AS name;
SELECT file_path, file_size_in_bytes, record_count
FROM iceberg_metadata('polaris.stargraph.stargraph_customers')
ORDER BY file_size_in_bytes DESC
LIMIT 10;

-- ── T11: stargraph_nodes — latest snap_timestamp (pipeline freshness) ─────────
SELECT 'T11' AS test, 'Pipeline freshness' AS name;
SELECT
    MAX(snap_timestamp)  AS latest_write,
    MIN(snap_timestamp)  AS earliest_write,
    COUNT(*)             AS total_rows
FROM polaris.stargraph.stargraph_nodes;

-- ── T12: SQL Proxy — test via curl (run from shell, not SQL) ─────────────────
-- T12a: Health check
--   curl http://192.168.1.50:30680/health
--
-- T12b: Raw SQL passthrough
--   curl -X POST http://192.168.1.50:30680/sql \
--        -H "Content-Type: application/json" \
--        -d '{"sql": "SELECT tier, COUNT(*) FROM polaris.stargraph.stargraph_customers GROUP BY tier"}'
--
-- T12c: Cypher translation
--   curl -X POST http://192.168.1.50:30680/cypher \
--        -H "Content-Type: application/json" \
--        -d '{"cypher": "MATCH (c:Customer {tier: '\''GOLD'\''}) RETURN c LIMIT 5"}'
--
-- T12d: Cypher vs Stargraph live comparison
--   curl -X POST http://192.168.1.50:30680/compare \
--        -H "Content-Type: application/json" \
--        -d '{"cypher": "MATCH (c:Customer) WHERE c.city = '\''London'\'' RETURN c LIMIT 10"}'
--
-- Expected: both iceberg_result and stardata_result return same customer rows.
-- Timing difference shows Iceberg (analytical) vs Stargraph live latency.

SELECT 'ALL TESTS COMPLETE' AS status;
