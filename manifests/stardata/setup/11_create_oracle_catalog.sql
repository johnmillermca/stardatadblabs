-- =============================================================================
-- 11_create_oracle_catalog.sql
-- Create a JDBC external catalog in Stardata (Apache Doris) pointing at the
-- Oracle XE 21c PDB XEPDB1 that runs on master2.local (oracle-xe.prod).
--
-- Oracle connection:
--   Service  : oracle-xe.prod.svc.cluster.local:1521/XEPDB1
--   Schema   : CACHE_TESTING  (appears as a database in Stardata)
--   Username : CACHE_TESTING
--   Auth     : password (stored in K8s secret oracle-credentials)
--
-- Prerequisites:
--   1. ojdbc11.jar is baked into the FE image (stardata/jars/ojdbc11.jar).
--      The busybox httpd sidecar serves it on :8181 for BE pods to download.
--
--   2. CACHE_TESTING user exists and is OPEN in XEPDB1.
--      To reset password if needed:
--        kubectl exec -n prod <oracle-pod> -- \
--          bash -c "sqlplus -s '/ as sysdba' <<'EOF'
--        ALTER SESSION SET CONTAINER = XEPDB1;
--        ALTER USER CACHE_TESTING IDENTIFIED BY \"<new-password>\";
--        EXIT;
--        EOF"
--      Then patch the secret:
--        kubectl patch secret oracle-credentials -n prod \
--          --type='json' \
--          -p='[{"op":"replace","path":"/data/oracle-password","value":"<base64>"}]'
--
-- Invocation:
--   STARDATA_PASS="$(kubectl get secret stardata-credentials \
--       -n prod -o jsonpath='{.data.admin-password}' | base64 -d)"
--   ORACLE_PASS="$(kubectl get secret oracle-credentials \
--       -n prod -o jsonpath='{.data.oracle-password}' | base64 -d)"
--   mysql -h 192.168.1.50 -P 30090 -u root -p"${STARDATA_PASS}" \
--     -e "$(sed "s/ORACLE_PASSWORD_PLACEHOLDER/${ORACLE_PASS}/" \
--           manifests/stardata/setup/11_create_oracle_catalog.sql)"
--
-- Catalog name  : oracle_cache_testing
-- JDBC type     : oracle.jdbc.OracleDriver
-- PDB           : XEPDB1
-- Schema        : CACHE_TESTING
-- =============================================================================

-- ── Oracle XE 21c (CACHE_TESTING schema in XEPDB1 via JDBC) ──────────────────
DROP CATALOG IF EXISTS oracle_cache_testing;

CREATE CATALOG oracle_cache_testing PROPERTIES (
    "type"         = "jdbc",
    "user"         = "CACHE_TESTING",
    "password"     = "ORACLE_PASSWORD_PLACEHOLDER",
    "jdbc_url"     = "jdbc:oracle:thin:@oracle-xe.prod.svc.cluster.local:1521/XEPDB1",
    "driver_url"   = "http://stardata-fe.prod.svc.cluster.local:8181/ojdbc11.jar",
    "driver_class" = "oracle.jdbc.OracleDriver"
);

-- Immediately pull catalog metadata so schemas/tables are visible.
REFRESH CATALOG oracle_cache_testing;

-- Verify: should show CACHE_TESTING (and Oracle system schemas).
SHOW DATABASES FROM oracle_cache_testing;
