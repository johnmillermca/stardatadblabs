-- =============================================================================
-- 10_create_snowflake_catalog.sql
-- Create a JDBC external catalog in Stardata that maps Snowflake proprietary
-- (native) tables — not Iceberg.  Doris reads directly via the Snowflake JDBC
-- driver served by the busybox httpd sidecar on FE pods (:8181).
--
-- Snowflake connection:
--   URL      : https://app.snowflake.com/oqihhtj/ta50603/
--   Account  : oqihhtj-ta50603
--   Database : SNOWFLAKE_AUGMENTATION
--   Schemas  : AUG_TEST, PUBLIC  (appear as databases in Stardata)
--   Username : testsnowflake
--   Auth     : RSA key-pair / JWT (no password in URL — session-expiry proof)
--
-- Prerequisites:
--   1. Apply the JWT key Secret (idempotent):
--        kubectl apply -f manifests/stardata/setup/snowflake-jwt-key-secret.yaml
--
--   2. Snowflake JDBC jar is baked into the FE image (stardata/jars/snowflake-jdbc-4.0.2.jar
--      COPY'd into /opt/apache-doris/fe/jdbc_drivers/ in Dockerfile.fe).
--      At pod start the seed-jdbc-drivers initContainer copies it into an emptyDir that
--      the busybox httpd sidecar serves on :8181.
--
--   3. RSA private key is mounted at /snowflake-jwt/rsa_key.p8 on every FE pod
--      via the snowflake-jwt-key K8s Secret volume.
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
-- Database      : SNOWFLAKE_AUGMENTATION
-- =============================================================================

-- ── Snowflake (proprietary native tables via JDBC — JWT auth) ─────────────────
-- authenticator=snowflake_jwt: each new connection signs a fresh JWT with the
-- private key at /snowflake-jwt/rsa_key.p8.  No password, no idle session to
-- expire — the connection pool can be fully closed between queries.
CREATE CATALOG IF NOT EXISTS snowflake PROPERTIES (
    "type"         = "jdbc",
    "user"         = "testsnowflake",
    "jdbc_url"     = "jdbc:snowflake://oqihhtj-ta50603.snowflakecomputing.com/?authenticator=snowflake_jwt&private_key_file=/snowflake-jwt/rsa_key.p8&warehouse=COMPUTE_WH&db=SNOWFLAKE_AUGMENTATION&loginTimeout=60",
    "driver_url"   = "http://stardata-fe.prod.svc.cluster.local:8181/snowflake-jdbc-4.0.2.jar",
    "driver_class" = "net.snowflake.client.jdbc.SnowflakeDriver"
);

-- Immediately pull the catalog metadata so schemas/tables are visible.
REFRESH CATALOG snowflake;

-- Verify: should list AUG_TEST and PUBLIC (schemas of SNOWFLAKE_AUGMENTATION).
SHOW DATABASES FROM snowflake;
