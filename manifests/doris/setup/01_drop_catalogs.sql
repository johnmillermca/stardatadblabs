-- =============================================================================
-- 01_drop_catalogs.sql
-- Drop all existing external catalogs from Doris.
--
-- Run from outside the cluster:
--   mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
--         < manifests/doris/setup/01_drop_catalogs.sql
--
-- Run from inside the cluster (e.g. from the cache-manager pod):
--   mysql -h doris-fe.prod.svc.cluster.local -P 9030 -u root -p"${DORIS_PASS}" \
--         < 01_drop_catalogs.sql
-- =============================================================================

-- Built-in catalogs (internal, hive_metastore) cannot be dropped — only
-- user-created external catalogs are removed here.

DROP CATALOG IF EXISTS polaris;
DROP CATALOG IF EXISTS databricks;
DROP CATALOG IF EXISTS postgres;
DROP CATALOG IF EXISTS oracle;
DROP CATALOG IF EXISTS mongodb;

-- Also drop any legacy catalog that may have been created with the old URI
-- from the runbook-05 example (iceberg_polaris).
DROP CATALOG IF EXISTS iceberg_polaris;

-- Verify
SHOW CATALOGS;
