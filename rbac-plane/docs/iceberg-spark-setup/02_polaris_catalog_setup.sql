-- ============================================================
-- 02_polaris_catalog_setup.sql
-- ============================================================
-- Bootstrap the Apache Polaris catalog for the Iceberg table.
-- Run these SQL statements against the Snowflake worksheet
-- AFTER the Polaris integration is enabled on your Snowflake account.
--
-- Prerequisites
-- -------------
--   1.  Your Snowflake account must have the Apache Polaris feature enabled.
--       Contact Snowflake support or use an account on app.snowflake.com
--       that has POLARIS_CATALOG_ENABLED = TRUE.
--   2.  The S3 bucket (xdatatoiceberg1) must be reachable from Snowflake
--       via a Storage Integration (see STEP 1 below).
--   3.  Execute as ACCOUNTADMIN or a role with CREATE CATALOG privileges.
--
-- RBAC note
-- ---------
-- After executing, register the Polaris service principal as an RBAC user
-- and bind the 'iceberg_engineer' role via rbacctl:
--
--   rbacctl user create polaris-svc
--   rbacctl user bind polaris-svc iceberg_engineer --service polaris
--   rbacctl sync run --user polaris-svc --service polaris
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- ────────────────────────────────────────────────────────────
-- STEP 1 — Storage Integration (Snowflake → S3)
-- ────────────────────────────────────────────────────────────
-- Snowflake uses this integration to read Iceberg metadata files from S3.
-- After creation, run DESC INTEGRATION s3_iceberg_int to get the
-- STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID, then add a trust
-- policy to the IAM role used by Snowflake.

CREATE STORAGE INTEGRATION IF NOT EXISTS s3_iceberg_int
    TYPE              = EXTERNAL_STAGE
    STORAGE_PROVIDER  = 'S3'
    ENABLED           = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/snowflake-iceberg-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://xdatatoiceberg1/warehouse/');

-- Show the IAM user / external ID to trust in AWS:
DESC INTEGRATION s3_iceberg_int;


-- ────────────────────────────────────────────────────────────
-- STEP 2 — Polaris Catalog
-- ────────────────────────────────────────────────────────────
-- Create the catalog that maps 1-to-1 to the Polaris REST catalog
-- configured in Spark.  The warehouse path must match
-- WAREHOUSE_PATH in 01_create_iceberg_table.py.

CREATE CATALOG IF NOT EXISTS polaris_lakehouse
    TYPE = ICEBERG_REST
    -- The Polaris REST endpoint exposed by your Polaris service
    CATALOG_URI = 'http://polaris:8181/api/catalog'
    -- OAuth2 machine credential created in the Polaris admin UI
    -- Format: "<client_id>:<client_secret>"
    CATALOG_CREDENTIAL = '<POLARIS_CLIENT_ID>:<POLARIS_CLIENT_SECRET>'
    WAREHOUSE = 's3://xdatatoiceberg1/warehouse'
    STORAGE_INTEGRATION = s3_iceberg_int;


-- ────────────────────────────────────────────────────────────
-- STEP 3 — Polaris Namespace and Table registration
-- ────────────────────────────────────────────────────────────
-- The table was created by Spark in 01_create_iceberg_table.py.
-- Snowflake registers it via the catalog — no DDL needed here,
-- but we verify the table is visible.

USE CATALOG polaris_lakehouse;

-- List namespaces
SHOW NAMESPACES;

-- Describe the Iceberg table created by Spark
DESCRIBE TABLE lakehouse.events;

-- Preview data (after Spark has loaded rows in notebook step)
SELECT * FROM lakehouse.events LIMIT 10;


-- ────────────────────────────────────────────────────────────
-- STEP 4 — Grant Snowflake roles access via Polaris RBAC
-- ────────────────────────────────────────────────────────────
-- Snowflake's own RBAC works on top of the Polaris catalog grants.
-- The RBAC Control Plane (rbacctl) manages platform-level roles;
-- Snowflake-native grants below mirror those decisions.

USE ROLE ACCOUNTADMIN;

-- Create a Snowflake role that represents the iceberg_engineer platform role
CREATE ROLE IF NOT EXISTS iceberg_engineer_sf;
GRANT USAGE ON CATALOG polaris_lakehouse TO ROLE iceberg_engineer_sf;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA polaris_lakehouse.lakehouse
    TO ROLE iceberg_engineer_sf;

-- Create a Snowflake role for read-only analysts
CREATE ROLE IF NOT EXISTS analyst_sf;
GRANT USAGE ON CATALOG polaris_lakehouse TO ROLE analyst_sf;
GRANT SELECT ON ALL TABLES IN SCHEMA polaris_lakehouse.lakehouse
    TO ROLE analyst_sf;

-- Assign to Snowflake users
-- GRANT ROLE iceberg_engineer_sf TO USER testsnowflake;
