-- =============================================================================
-- 12_create_postgresql_catalog.sql
-- Create a JDBC external catalog in Stardata (Apache Doris) pointing at the
-- PostgreSQL instance that runs on master2.local (postgresql.prod).
--
-- PostgreSQL connection:
--   Host     : postgresql.prod.svc.cluster.local:5432
--   Database : cache_testing
--   Schema   : public  (appears as a database in Stardata)
--   Username : postgres
--   Auth     : password (stored in K8s secret postgresql-credentials)
--
-- Prerequisites:
--   1. postgresql-42.7.4.jar is baked into the FE image
--      (stardata/jars/postgresql-42.7.4.jar).
--      The busybox httpd sidecar serves it on :8181 for BE pods to download.
--
--   2. The cache_testing database exists in PostgreSQL and the postgres user
--      has access to it.
--
-- Invocation:
--   STARDATA_PASS="$(kubectl get secret stardata-credentials \
--       -n prod -o jsonpath='{.data.admin-password}' | base64 -d)"
--   PG_PASS="$(kubectl get secret postgresql-credentials \
--       -n prod -o jsonpath='{.data.postgres-password}' | base64 -d)"
--   mysql -h 192.168.1.50 -P 30090 -u root -p"${STARDATA_PASS}" \
--     -e "$(sed "s/PG_PASSWORD_PLACEHOLDER/${PG_PASS}/" \
--           manifests/stardata/setup/12_create_postgresql_catalog.sql)"
--
-- Catalog name  : pg_cache_testing
-- JDBC type     : org.postgresql.Driver
-- Database      : cache_testing
-- Schema        : public
-- =============================================================================

-- ── PostgreSQL (cache_testing database via JDBC) ───────────────────────────────
DROP CATALOG IF EXISTS pg_cache_testing;

CREATE CATALOG pg_cache_testing PROPERTIES (
    "type"         = "jdbc",
    "user"         = "postgres",
    "password"     = "PG_PASSWORD_PLACEHOLDER",
    "jdbc_url"     = "jdbc:postgresql://postgresql.prod.svc.cluster.local:5432/cache_testing",
    "driver_url"   = "http://stardata-fe.prod.svc.cluster.local:8181/postgresql-42.7.4.jar",
    "driver_class" = "org.postgresql.Driver"
);

-- Immediately pull catalog metadata so schemas/tables are visible.
REFRESH CATALOG pg_cache_testing;

-- Verify: should show public (and pg_catalog, information_schema).
SHOW DATABASES FROM pg_cache_testing;
