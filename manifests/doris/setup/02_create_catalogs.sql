-- =============================================================================
-- 02_create_catalogs.sql
-- Create all 5 Iceberg external catalogs in Doris, each pointing to the
-- Polaris REST catalog with the correct warehouse name.
--
-- Polaris REST endpoint (in-cluster):
--   http://polaris-rest.prod.svc.cluster.local:8181/api/catalog
--
-- OAuth2 credentials are injected at execution time via shell substitution:
--   POLARIS_ID     — from OpenBao secret/data/platform/polaris → spark_svc_id
--   POLARIS_SECRET — from OpenBao secret/data/platform/polaris → spark_svc_secret
--
-- Example invocation (see runbook-25 for the full bootstrap script):
--   envsubst < 02_create_catalogs.sql | \
--     mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}"
--
-- Warehouse names mirror the Spark catalog configuration in bao_spark_init.py:
--   polaris     → IcebergCatalog
--   databricks  → star_lakehouse
--   postgres    → pg_lakehouse
--   oracle      → ora_lakehouse
--   mongodb     → mgo_lakehouse
-- =============================================================================

-- ── 1. polaris (warehouse: IcebergCatalog) ────────────────────────────────────
CREATE CATALOG IF NOT EXISTS polaris PROPERTIES (
    "type"                    = "iceberg",
    "iceberg.catalog.type"    = "rest",
    "uri"                     = "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog",
    "iceberg.rest.auth.type"  = "oauth2",
    "oauth2.token-endpoint"   = "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog/v1/oauth/tokens",
    "oauth2.credential"       = "${POLARIS_ID}:${POLARIS_SECRET}",
    "oauth2.scope"            = "PRINCIPAL_ROLE:ALL",
    "warehouse"               = "IcebergCatalog"
);

-- ── 2. databricks (warehouse: star_lakehouse) ─────────────────────────────────
CREATE CATALOG IF NOT EXISTS databricks PROPERTIES (
    "type"                    = "iceberg",
    "iceberg.catalog.type"    = "rest",
    "uri"                     = "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog",
    "iceberg.rest.auth.type"  = "oauth2",
    "oauth2.token-endpoint"   = "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog/v1/oauth/tokens",
    "oauth2.credential"       = "${POLARIS_ID}:${POLARIS_SECRET}",
    "oauth2.scope"            = "PRINCIPAL_ROLE:ALL",
    "warehouse"               = "star_lakehouse"
);

-- ── 3. postgres (warehouse: pg_lakehouse) ─────────────────────────────────────
CREATE CATALOG IF NOT EXISTS postgres PROPERTIES (
    "type"                    = "iceberg",
    "iceberg.catalog.type"    = "rest",
    "uri"                     = "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog",
    "iceberg.rest.auth.type"  = "oauth2",
    "oauth2.token-endpoint"   = "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog/v1/oauth/tokens",
    "oauth2.credential"       = "${POLARIS_ID}:${POLARIS_SECRET}",
    "oauth2.scope"            = "PRINCIPAL_ROLE:ALL",
    "warehouse"               = "pg_lakehouse"
);

-- ── 4. oracle (warehouse: ora_lakehouse) ──────────────────────────────────────
CREATE CATALOG IF NOT EXISTS oracle PROPERTIES (
    "type"                    = "iceberg",
    "iceberg.catalog.type"    = "rest",
    "uri"                     = "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog",
    "iceberg.rest.auth.type"  = "oauth2",
    "oauth2.token-endpoint"   = "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog/v1/oauth/tokens",
    "oauth2.credential"       = "${POLARIS_ID}:${POLARIS_SECRET}",
    "oauth2.scope"            = "PRINCIPAL_ROLE:ALL",
    "warehouse"               = "ora_lakehouse"
);

-- ── 5. mongodb (warehouse: mgo_lakehouse) ─────────────────────────────────────
CREATE CATALOG IF NOT EXISTS mongodb PROPERTIES (
    "type"                    = "iceberg",
    "iceberg.catalog.type"    = "rest",
    "uri"                     = "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog",
    "iceberg.rest.auth.type"  = "oauth2",
    "oauth2.token-endpoint"   = "http://polaris-rest.prod.svc.cluster.local:8181/api/catalog/v1/oauth/tokens",
    "oauth2.credential"       = "${POLARIS_ID}:${POLARIS_SECRET}",
    "oauth2.scope"            = "PRINCIPAL_ROLE:ALL",
    "warehouse"               = "mgo_lakehouse"
);

-- Verify
SHOW CATALOGS;
