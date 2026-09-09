-- =============================================================================
-- 02_create_catalogs.sql
-- Create all 5 Iceberg external catalogs in Doris, each pointing to the
-- polaris-auth-proxy which auto-injects a fresh OAuth2 Bearer token on every
-- request.  No credentials needed in the catalog definition — the proxy
-- handles all authentication against Polaris automatically.
--
-- Proxy ports:
--   :8283  — doris-reader principal  (read access — used for all 5 catalogs)
--   :8282  — doris-writer principal  (write access — used by iceberg_polaris_rw)
--
-- The proxy fetches tokens from OpenBao at startup and refreshes them
-- automatically 300s before expiry.  Tokens never expire from Doris's view.
--
-- Warehouse names mirror the Spark catalog configuration in bao_spark_init.py:
--   polaris     → IcebergCatalog
--   databricks  → star_lakehouse
--   postgres    → pg_lakehouse
--   oracle      → ora_lakehouse
--   mongodb     → mgo_lakehouse
--
-- Invocation (no envsubst needed — no credentials in this file):
--   mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
--     < manifests/doris/setup/02_create_catalogs.sql
-- =============================================================================

-- ── 1. polaris (warehouse: IcebergCatalog) ────────────────────────────────────
CREATE CATALOG IF NOT EXISTS polaris PROPERTIES (
    "type"                 = "iceberg",
    "iceberg.catalog.type" = "rest",
    "uri"                  = "http://polaris-auth-proxy.prod.svc.cluster.local:8283/api/catalog",
    "warehouse"            = "IcebergCatalog"
);

-- ── 2. databricks (warehouse: star_lakehouse) ─────────────────────────────────
CREATE CATALOG IF NOT EXISTS databricks PROPERTIES (
    "type"                 = "iceberg",
    "iceberg.catalog.type" = "rest",
    "uri"                  = "http://polaris-auth-proxy.prod.svc.cluster.local:8283/api/catalog",
    "warehouse"            = "star_lakehouse"
);

-- ── 3. postgres (warehouse: pg_lakehouse) ─────────────────────────────────────
CREATE CATALOG IF NOT EXISTS postgres PROPERTIES (
    "type"                 = "iceberg",
    "iceberg.catalog.type" = "rest",
    "uri"                  = "http://polaris-auth-proxy.prod.svc.cluster.local:8283/api/catalog",
    "warehouse"            = "pg_lakehouse"
);

-- ── 4. oracle (warehouse: ora_lakehouse) ──────────────────────────────────────
CREATE CATALOG IF NOT EXISTS oracle PROPERTIES (
    "type"                 = "iceberg",
    "iceberg.catalog.type" = "rest",
    "uri"                  = "http://polaris-auth-proxy.prod.svc.cluster.local:8283/api/catalog",
    "warehouse"            = "ora_lakehouse"
);

-- ── 5. mongodb (warehouse: mgo_lakehouse) ─────────────────────────────────────
CREATE CATALOG IF NOT EXISTS mongodb PROPERTIES (
    "type"                 = "iceberg",
    "iceberg.catalog.type" = "rest",
    "uri"                  = "http://polaris-auth-proxy.prod.svc.cluster.local:8283/api/catalog",
    "warehouse"            = "mgo_lakehouse"
);

-- Verify
SHOW CATALOGS;
