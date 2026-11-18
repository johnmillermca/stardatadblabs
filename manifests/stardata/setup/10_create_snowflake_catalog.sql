-- =============================================================================
-- 10_create_snowflake_catalog.sql
-- Create a JDBC external catalog in Stardata that maps Snowflake proprietary
-- (native) tables — not Iceberg.  Doris reads directly via the Snowflake JDBC
-- driver mounted at /opt/apache-doris/fe/jdbc_drivers/snowflake-jdbc-*.jar
--
-- Snowflake connection:
--   URL      : https://app.snowflake.com/oqihhtj/ta50603/
--   Account  : oqihhtj-ta50603
--   Username : testsnowflake
--   Password : stored in Kubernetes Secret snowflake-credentials (key: password)
--
-- Prerequisites:
--   1. Apply the Kubernetes Secret first (idempotent):
--        kubectl apply -f manifests/stardata/setup/snowflake-credentials-secret.yaml
--
--   2. Snowflake JDBC jar is baked into the FE image (stardata/jars/snowflake-jdbc-4.0.2.jar
--      COPY'd into /opt/apache-doris/fe/jdbc_drivers/ in Dockerfile.fe).
--      At pod start the seed-jdbc-drivers initContainer copies it into an emptyDir that
--      the busybox httpd sidecar serves on :8181.
--      Verify: curl http://stardata-fe.prod.svc.cluster.local:8181/snowflake-jdbc-4.0.2.jar -I
--
-- Invocation (reads password from the Kubernetes Secret, then pipes to mysql):
--   export SNOWFLAKE_PASS="$(kubectl get secret snowflake-credentials \
--       -n prod -o jsonpath='{.data.password}' | base64 -d)"
--   export STARDATA_PASS="$(kubectl get secret stardata-credentials \
--       -n prod -o jsonpath='{.data.admin-password}' | base64 -d)"
--   envsubst < manifests/stardata/setup/10_create_snowflake_catalog.sql \
--     | mysql -h 192.168.1.50 -P 30090 -u root -p"${STARDATA_PASS}"
--
-- Catalog name  : snowflake
-- JDBC type     : net.snowflake.client.jdbc.SnowflakeDriver
-- Warehouse     : COMPUTE_WH (Snowflake default; change if yours differs)
-- =============================================================================

-- ── Snowflake (proprietary native tables via JDBC) ────────────────────────────
-- The jdbc_url encodes:
--   account      — oqihhtj-ta50603  (org-name + account-name, no .snowflakecomputing.com)
--   warehouse    — COMPUTE_WH       (Snowflake default; change if yours differs)
--   loginTimeout — 30 s
-- NOTE: Snowflake JDBC 4.x ignores HikariCP setPassword() — credentials must
-- be embedded directly in the JDBC URL in addition to the top-level properties.
CREATE CATALOG IF NOT EXISTS snowflake PROPERTIES (
    "type"                     = "jdbc",
    "user"                     = "testsnowflake",
    "password"                 = "${SNOWFLAKE_PASS}",
    "jdbc_url"                 = "jdbc:snowflake://oqihhtj-ta50603.snowflakecomputing.com/?warehouse=COMPUTE_WH&loginTimeout=30&user=testsnowflake&password=${SNOWFLAKE_PASS}",
    "driver_url"               = "http://stardata-fe.prod.svc.cluster.local:8181/snowflake-jdbc-4.0.2.jar",
    "driver_class"             = "net.snowflake.client.jdbc.SnowflakeDriver"
);

-- Immediately pull the catalog metadata so databases/schemas/tables are
-- visible in SHOW DATABASES and SHOW TABLES without waiting for the
-- next scheduled catalog refresh.
REFRESH CATALOG snowflake;

-- Verify: should list all databases the testsnowflake user has access to.
SHOW DATABASES FROM snowflake;
