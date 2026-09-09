-- =============================================================================
-- 02_create_catalogs.sql
-- Create all 5 Iceberg external catalogs in Doris, each pointing to the
-- polaris-auth-proxy which auto-injects a fresh OAuth2 Bearer token on every
-- request.
--
-- Proxy ports:
--   :8283  — doris-reader principal  (read access — used for all 5 catalogs)
--   :8282  — doris-writer principal  (write access — used by iceberg_polaris_rw)
--
-- The proxy fetches tokens from OpenBao at startup and refreshes them
-- automatically 300s before expiry.  Tokens never expire from Doris's view.
--
-- S3 credentials are required so Doris BE can read Parquet/ORC data files
-- from S3.  Without them every SELECT against a table backed by S3 data fails
-- with:  SdkClientException: Unable to load credentials from AwsCredentialsProviderChain
--
-- Invocation (envsubst expands S3_KEY / S3_SECRET before piping to mysql):
--   export S3_KEY="$(curl -s -H "X-Vault-Token: ${BAO_TOKEN}" \
--       http://192.168.1.50:30820/v1/secret/data/platform/s3 \
--       | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['data']['access_key'])")"
--   export S3_SECRET="$(curl -s -H "X-Vault-Token: ${BAO_TOKEN}" \
--       http://192.168.1.50:30820/v1/secret/data/platform/s3 \
--       | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['data']['secret_key'])")"
--   envsubst < manifests/doris/setup/02_create_catalogs.sql \
--     | mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}"
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
    "type"                 = "iceberg",
    "iceberg.catalog.type" = "rest",
    "uri"                  = "http://polaris-auth-proxy.prod.svc.cluster.local:8283/api/catalog",
    "warehouse"            = "IcebergCatalog",
    "s3.access-key-id"     = "${S3_KEY}",
    "s3.secret-access-key" = "${S3_SECRET}",
    "s3.endpoint"          = "https://s3.us-east-2.amazonaws.com",
    "s3.region"            = "us-east-2",
    "s3.path-style-access" = "false"
);

-- ── 2. databricks (warehouse: star_lakehouse) ─────────────────────────────────
CREATE CATALOG IF NOT EXISTS databricks PROPERTIES (
    "type"                 = "iceberg",
    "iceberg.catalog.type" = "rest",
    "uri"                  = "http://polaris-auth-proxy.prod.svc.cluster.local:8283/api/catalog",
    "warehouse"            = "star_lakehouse",
    "s3.access-key-id"     = "${S3_KEY}",
    "s3.secret-access-key" = "${S3_SECRET}",
    "s3.endpoint"          = "https://s3.us-east-2.amazonaws.com",
    "s3.region"            = "us-east-2",
    "s3.path-style-access" = "false"
);

-- ── 3. postgres (warehouse: pg_lakehouse) ─────────────────────────────────────
CREATE CATALOG IF NOT EXISTS postgres PROPERTIES (
    "type"                 = "iceberg",
    "iceberg.catalog.type" = "rest",
    "uri"                  = "http://polaris-auth-proxy.prod.svc.cluster.local:8283/api/catalog",
    "warehouse"            = "pg_lakehouse",
    "s3.access-key-id"     = "${S3_KEY}",
    "s3.secret-access-key" = "${S3_SECRET}",
    "s3.endpoint"          = "https://s3.us-east-2.amazonaws.com",
    "s3.region"            = "us-east-2",
    "s3.path-style-access" = "false"
);

-- ── 4. oracle (warehouse: ora_lakehouse) ──────────────────────────────────────
CREATE CATALOG IF NOT EXISTS oracle PROPERTIES (
    "type"                 = "iceberg",
    "iceberg.catalog.type" = "rest",
    "uri"                  = "http://polaris-auth-proxy.prod.svc.cluster.local:8283/api/catalog",
    "warehouse"            = "ora_lakehouse",
    "s3.access-key-id"     = "${S3_KEY}",
    "s3.secret-access-key" = "${S3_SECRET}",
    "s3.endpoint"          = "https://s3.us-east-2.amazonaws.com",
    "s3.region"            = "us-east-2",
    "s3.path-style-access" = "false"
);

-- ── 5. mongodb (warehouse: mgo_lakehouse) ─────────────────────────────────────
CREATE CATALOG IF NOT EXISTS mongodb PROPERTIES (
    "type"                 = "iceberg",
    "iceberg.catalog.type" = "rest",
    "uri"                  = "http://polaris-auth-proxy.prod.svc.cluster.local:8283/api/catalog",
    "warehouse"            = "mgo_lakehouse",
    "s3.access-key-id"     = "${S3_KEY}",
    "s3.secret-access-key" = "${S3_SECRET}",
    "s3.endpoint"          = "https://s3.us-east-2.amazonaws.com",
    "s3.region"            = "us-east-2",
    "s3.path-style-access" = "false"
);

-- Verify
SHOW CATALOGS;
