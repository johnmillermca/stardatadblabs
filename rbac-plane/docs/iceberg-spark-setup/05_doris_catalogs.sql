-- ============================================================
-- 05_doris_catalogs.sql
-- ============================================================
-- (f)  Create an Apache Doris external catalog pointing at the
--      Spark Iceberg table (spark_lakehouse.lakehouse.events on S3)
--      via the Polaris auth-proxy, and schedule a 1-minute warm-up.
--
-- (g)  Create a JDBC external catalog in Doris for Snowflake
--      proprietary tables, using JWT auth + the Snowflake JDBC driver
--      served from the Doris FE PVC over an internal HTTP server.
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
--   snowflake:JDBC_ADMIN
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


-- ════════════════════════════════════════════════════════════
-- (g)  SNOWFLAKE JDBC EXTERNAL CATALOG
-- ════════════════════════════════════════════════════════════
--
-- Architecture notes
-- ------------------
-- 1. The Snowflake JDBC driver (snowflake-jdbc-3.15.1.jar) is stored on
--    the Doris FE PVC at:
--      /opt/apache-doris/fe/doris-meta/jdbc_drivers/snowflake-jdbc-3.15.1.jar
--    and served over a tiny HTTP server (python3 -m http.server 8888)
--    that is started by a postStart lifecycle hook on the doris-fe container.
--    The K8s Service 'doris-jdbc-driver' (port 8888) routes to this server.
--
-- 2. The same JAR and the PKCS8 PEM private key must also exist on every
--    Doris BE node at:
--      /opt/apache-doris/be/storage/jdbc_drivers/snowflake-jdbc-3.15.1.jar
--      /opt/apache-doris/be/storage/jdbc_drivers/sf_rsa_key_pkcs8.pem
--    (already placed there; BE executes the actual JDBC queries)
--
-- 3. Doris 4.0.7's JdbcResource does not natively support 'jdbc:snowflake'
--    URLs. The doris-fe.jar has been bytecode-patched by the
--    'patch-doris-fe-jar' initContainer to map the 'snowflake' driver
--    to JdbcTrinoClient (which supports DatabaseMetaData.getSchemas()).
--
-- 4. 'test_connection=false' skips the BE-side connection probe on CREATE.
-- 5. 'lower_case_meta_names=true' avoids Snowflake UPPERCASE column name
--    issues in Doris's metadata layer.

-- ────────────────────────────────────────────────────────────
-- G-1  Create the JDBC catalog pointing at Snowflake
-- ────────────────────────────────────────────────────────────
-- NOTE: The password field is not used for JWT auth, but Doris requires
-- a non-empty value; the Snowflake JDBC driver ignores it when
-- authenticator=snowflake_jwt is set.
CREATE CATALOG IF NOT EXISTS snowflake_jdbc PROPERTIES (
    'type'                   = 'jdbc',
    'user'                   = 'testsnowflake',
    'password'               = '<read from OpenBao: secret/platform/snowflake → password>',
    'jdbc_url'               = 'jdbc:snowflake://oqihhtj-ta50603.snowflakecomputing.com/?warehouse=COMPUTE_WH&db=LAKEHOUSE_DB&schema=EVENTS&authenticator=snowflake_jwt&private_key_file=/opt/apache-doris/fe/doris-meta/jdbc_drivers/sf_rsa_key_pkcs8.pem',
    -- Driver JAR served from Doris FE PVC via internal HTTP server
    'driver_url'             = 'http://doris-fe-0.doris-fe-headless.prod.svc.cluster.local:8888/snowflake-jdbc-3.15.1.jar',
    'driver_class'           = 'net.snowflake.client.jdbc.SnowflakeDriver',
    -- Skip BE-side connection test on CREATE (required for JWT auth path)
    'test_connection'        = 'false',
    -- Fold Snowflake UPPERCASE identifiers to lowercase in Doris
    'lower_case_meta_names'  = 'true'
);


-- ────────────────────────────────────────────────────────────
-- G-2  Verify Snowflake JDBC catalog
-- ────────────────────────────────────────────────────────────
SWITCH snowflake_jdbc;
SHOW DATABASES;   -- lists Snowflake databases accessible to testsnowflake

-- Query a Snowflake proprietary table through Doris:
SELECT *
FROM snowflake_jdbc.lakehouse_db.events
LIMIT 5;


-- ────────────────────────────────────────────────────────────
-- G-3  Cross-catalog JOIN: Iceberg (S3) + Snowflake (JDBC)
-- ────────────────────────────────────────────────────────────
-- Demonstrates the key value of having both catalogs in Doris:
-- join open-format S3 data with Snowflake proprietary tables without ETL.
SELECT
    i.event_type,
    i.user_id,
    s.account_tier
FROM iceberg_polaris.lakehouse.events          AS i
JOIN snowflake_jdbc.lakehouse_db.user_profiles AS s
    ON i.user_id = s.user_id
WHERE i.ts >= '2024-06-01 00:00:00'
LIMIT 20;


-- ────────────────────────────────────────────────────────────
-- G-4  Grant Doris users access to both catalogs (RBAC-aligned)
-- ────────────────────────────────────────────────────────────
-- These SQL grants mirror the RBAC Control Plane roles.
-- After rbacctl syncs, the DorisAdapter issues these automatically.
-- Shown here for manual / emergency reference.

-- iceberg_engineer: full access to Iceberg catalog + warm-up
GRANT USAGE ON CATALOG iceberg_polaris TO 'iceberg_engineer_user'@'%';
GRANT SELECT_PRIV ON iceberg_polaris.lakehouse.* TO 'iceberg_engineer_user'@'%';
GRANT LOAD_PRIV   ON iceberg_polaris.lakehouse.* TO 'iceberg_engineer_user'@'%';

-- analyst: read-only on both catalogs
GRANT USAGE ON CATALOG iceberg_polaris TO 'analyst_user'@'%';
GRANT SELECT_PRIV ON iceberg_polaris.lakehouse.events TO 'analyst_user'@'%';
GRANT USAGE ON CATALOG snowflake_jdbc TO 'analyst_user'@'%';
GRANT SELECT_PRIV ON snowflake_jdbc.lakehouse_db.* TO 'analyst_user'@'%';
