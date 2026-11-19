-- =============================================================================
-- 13_create_mongodb_catalog.sql
-- Create a Trino-connector external catalog in Stardata (Apache Doris) pointing
-- at the MongoDB 8.0 replica set that runs on master2.local (mongodb.prod).
--
-- MongoDB connection:
--   Host      : mongodb.prod.svc.cluster.local:27017
--   Auth DB   : admin
--   Username  : root
--   Auth      : SCRAM-SHA-256 (stored in K8s secret mongodb-credentials)
--   ReplicaSet: rs0
--
-- Catalog type: trino-connector (NOT jdbc)
--   The trino-mongodb-435 plugin is loaded via the seed-trino-plugins
--   initContainer on every FE and BE pod from:
--     /opt/apache-doris/fe/plugins/connectors/trino-mongodb-435/
--     /opt/apache-doris/be/plugins/connectors/trino-mongodb-435/
--   Plugin image: 192.168.1.50:30500/apache/stardata:trino-plugins-v1
--   Source: stardata/plugins/mongodb/trino-mongodb-435/ (69 JARs, 21MB)
--
-- Note on property naming:
--   All Trino connector properties must be prefixed with 'trino.' so Doris
--   strips the prefix before passing them to the plugin.
--   trino.mongodb.connection-url → mongodb.connection-url (passed to plugin)
--   trino.mongodb.schema-collection → mongodb.schema-collection (passed to plugin)
--
-- Databases visible in Stardata:
--   cache_testing  — 6 collections: customers, inventory_events, order_items,
--                    orders, product_reviews, products
--
-- Invocation:
--   STARDATA_PASS="$(kubectl get secret stardata-credentials \
--       -n prod -o jsonpath='{.data.admin-password}' | base64 -d)"
--   MONGO_PASS="$(kubectl get secret mongodb-credentials \
--       -n prod -o jsonpath='{.data.mongodb-root-password}' | base64 -d)"
--   # substitute password manually before running — do not embed in git
--
-- Catalog name  : mongodb
-- Connector type: trino-connector / mongodb (Trino 435)
-- Collections   : cache_testing.{customers,inventory_events,order_items,
--                               orders,product_reviews,products}
-- =============================================================================

-- ── MongoDB (via Trino connector — trino-mongodb-435 plugin) ──────────────────
DROP CATALOG IF EXISTS mongodb;

CREATE CATALOG mongodb PROPERTIES (
    "type"                            = "trino-connector",
    "trino.connector.name"            = "mongodb",
    "trino.mongodb.connection-url"    = "mongodb://root:MONGODB_PASSWORD_PLACEHOLDER@mongodb.prod.svc.cluster.local:27017/?authSource=admin",
    "trino.mongodb.schema-collection" = "_schema"
);

-- Verify: should show cache_testing (and information_schema, mysql).
SHOW DATABASES FROM mongodb;

-- Verify collections:
SHOW TABLES FROM mongodb.cache_testing;
