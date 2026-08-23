-- ============================================================
-- 05_doris_catalogs.sql
-- ============================================================
-- Create an Apache Doris external catalog pointing at the
-- Spark Iceberg table (IcebergCatalog.tpcds_sf10tcl on S3)
-- via the Polaris auth-proxy, and schedule a 1-minute warm-up.
--
-- Run these statements against the Doris FE MySQL-compatible port
-- (default 9030) as the Doris admin user.
--
-- RBAC note
-- ---------
-- Before creating catalogs, ensure the operator has the correct
-- permissions via the RBAC Control Plane:
--
--   rbacctl user roles <doris-admin-user>
--
-- Required permissions:
--   doris:ADMIN  (or doris:CREATE + doris:CATALOG_USAGE)
--   doris:CATALOG_WARM_UP
-- ============================================================


-- ════════════════════════════════════════════════════════════
-- (f)  ICEBERG EXTERNAL CATALOG (Polaris REST via auth-proxy)
-- ════════════════════════════════════════════════════════════
-- The polaris-auth-proxy (nginx) at polaris-auth-proxy.prod.svc.cluster.local:8282
-- injects a pre-fetched Bearer token into every /api/catalog/ request.
-- The token is refreshed every 55 min by the polaris-token-refresh CronJob.

-- ────────────────────────────────────────────────────────────
-- F-1  Create the Iceberg catalog
-- ────────────────────────────────────────────────────────────
CREATE CATALOG IF NOT EXISTS iceberg_polaris
COMMENT 'Spark Iceberg table via Apache Polaris REST catalog'
PROPERTIES (
    'type'                       = 'iceberg',
    'iceberg.catalog.type'       = 'rest',
    -- Route through the auth-proxy so Doris gets a pre-authorised Bearer token
    'uri'                        = 'http://polaris-auth-proxy.prod.svc.cluster.local:8282/api/catalog',
    'warehouse'                  = 'spark_lakehouse',
    -- AWS HMAC credentials for S3 file I/O (us-east-2)
    's3.endpoint'                = 'https://s3.us-east-2.amazonaws.com',
    's3.access_key'              = '<read from OpenBao: secret/platform/s3 → access_key>',
    's3.secret_key'              = '<read from OpenBao: secret/platform/s3 → secret_key>',
    's3.region'                  = 'us-east-2',
    -- Doris local-disk file cache (1-hour TTL, aligned with token lifetime)
    'enable_file_cache'          = 'true',
    'file_cache_ttl_seconds'     = '3600'
);


-- ────────────────────────────────────────────────────────────
-- F-2  Verify catalog and preview the Iceberg table
-- ────────────────────────────────────────────────────────────
SHOW CATALOGS;

SWITCH iceberg_polaris;
SHOW DATABASES;                        -- should show: lakehouse
SHOW TABLES FROM lakehouse;            -- should show: events

-- Preview data via Doris compute
SELECT event_type, COUNT(*) AS cnt
FROM iceberg_polaris.lakehouse.events
GROUP BY event_type
ORDER BY cnt DESC
LIMIT 10;

-- Snapshot / partition metadata
SELECT snapshot_id, committed_at, operation, summary
FROM iceberg_polaris.lakehouse.`events$snapshots`
ORDER BY committed_at DESC
LIMIT 5;


-- ────────────────────────────────────────────────────────────
-- F-3  1-minute cache warm-up
-- ────────────────────────────────────────────────────────────
-- Doris does not have a native SQL scheduler; the 1-minute schedule
-- is driven by the Kubernetes CronJob in 05b_doris_warmup_cronjob.yaml.
-- The CronJob issues the HTTP warm-up API call to the FE every minute.
--
-- Manual warm-up (run from MySQL client when needed):
WARM UP CATALOG iceberg_polaris WITH SYNC;
-- Warms all tables in the catalog; WITH SYNC blocks until done.

WARM UP TABLE iceberg_polaris.lakehouse.events WITH SYNC;
-- Warms only the events table (faster).


-- ────────────────────────────────────────────────────────────
-- F-4  Grant Doris users access to the Iceberg catalog (RBAC-aligned)
-- ────────────────────────────────────────────────────────────
-- These SQL grants mirror the RBAC Control Plane roles.
-- After rbacctl syncs, the DorisAdapter issues these automatically.
-- Shown here for manual / emergency reference.

-- iceberg_engineer: full access to Iceberg catalog + warm-up
GRANT USAGE ON CATALOG iceberg_polaris TO 'iceberg_engineer_user'@'%';
GRANT SELECT_PRIV ON iceberg_polaris.lakehouse.* TO 'iceberg_engineer_user'@'%';
GRANT LOAD_PRIV   ON iceberg_polaris.lakehouse.* TO 'iceberg_engineer_user'@'%';

-- analyst: read-only on Iceberg catalog
GRANT USAGE ON CATALOG iceberg_polaris TO 'analyst_user'@'%';
GRANT SELECT_PRIV ON iceberg_polaris.lakehouse.events TO 'analyst_user'@'%';
