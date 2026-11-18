-- =============================================================================
-- 10_create_snowflake_catalog.sql
-- Create a JDBC external catalog in Stardata that maps Snowflake proprietary
-- (native) tables — not Iceberg.  Doris reads directly via the Snowflake JDBC
-- driver served by the busybox httpd sidecar on FE pods (:8181).
--
-- Snowflake connection:
--   URL      : https://app.snowflake.com/oqihhtj/ta50603/
--   Account  : oqihhtj-ta50603
--   Database : LAKEHOUSE_DB
--   Schemas  : EVENTS, PUBLIC  (appear as databases in Stardata)
--   Username : testsnowflake
--   Auth     : RSA key-pair / JWT (no password, no idle session expiry)
--              Private key mounted at /snowflake-jwt/rsa_key.p8 on FE + BE pods
--
-- Prerequisites:
--   1. Apply the JWT key Secret to the cluster (idempotent):
--        kubectl apply -f manifests/stardata/setup/snowflake-jwt-key-secret.yaml
--
--   2. Snowflake JDBC jar is baked into the FE image (stardata/jars/snowflake-jdbc-4.0.2.jar).
--      The busybox httpd sidecar serves it on :8181 for the BE to download.
--
--   3. RSA private key is mounted at /snowflake-jwt/rsa_key.p8 on BOTH FE and BE pods
--      via the snowflake-jwt-key K8s Secret volume (see both StatefulSet manifests).
--
-- Invocation:
--   STARDATA_PASS="$(kubectl get secret stardata-credentials \
--       -n prod -o jsonpath='{.data.admin-password}' | base64 -d)"
--   mysql -h 192.168.1.50 -P 30090 -u root -p"${STARDATA_PASS}" \
--     < manifests/stardata/setup/10_create_snowflake_catalog.sql
--
-- Catalog name  : snowflake
-- JDBC type     : net.snowflake.client.jdbc.SnowflakeDriver
-- Authenticator : snowflake_jwt (RSA key-pair — no idle session expiry)
-- Warehouse     : COMPUTE_WH
-- Database      : LAKEHOUSE_DB
-- =============================================================================

-- ── Snowflake (proprietary native tables via JDBC — JWT auth) ─────────────────
-- authenticator=snowflake_jwt + private_key_file: each new JDBC connection signs
-- a fresh JWT from the mounted RSA key.  No password in URL.  The connection pool
-- can be fully closed between queries — reconnects always succeed.
CREATE CATALOG IF NOT EXISTS snowflake PROPERTIES (
    "type"         = "jdbc",
    "user"         = "testsnowflake",
    "jdbc_url"     = "jdbc:snowflake://oqihhtj-ta50603.snowflakecomputing.com/?authenticator=snowflake_jwt&private_key_file=/snowflake-jwt/rsa_key.p8&warehouse=COMPUTE_WH&db=LAKEHOUSE_DB&loginTimeout=60",
    "driver_url"   = "http://stardata-fe.prod.svc.cluster.local:8181/snowflake-jdbc-4.0.2.jar",
    "driver_class" = "net.snowflake.client.jdbc.SnowflakeDriver"
);

-- Immediately pull the catalog metadata so schemas/tables are visible.
REFRESH CATALOG snowflake;

-- Verify: should list EVENTS and PUBLIC (schemas of LAKEHOUSE_DB).
SHOW DATABASES FROM snowflake;
