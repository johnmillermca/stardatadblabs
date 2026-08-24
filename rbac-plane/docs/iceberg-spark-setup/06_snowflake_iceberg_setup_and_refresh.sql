-- ============================================================
-- 06_snowflake_iceberg_setup_and_refresh.sql
-- ============================================================
-- Run this in the Snowflake Worksheet at:
--   https://app.snowflake.com/oqihhtj/ta50603/
-- Login: testsnowflake / ACCOUNTADMIN role
--
-- Covers:
--   (b)  Register Iceberg table from S3 via OBJECT_STORE catalog
--   (d)  Schedule hourly metadata refresh task
--   (e)  Enable / disable / manual refresh
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- ── STEP 1: IAM Role (AWS Console — one-time manual step) ──
-- Role name : snowflake-iceberg-role   (account: 586643076710)
-- Trust policy principal : arn:aws:iam::790347818956:user/7zjp1000-s
-- ExternalId : NH90284_SFCRole=6_AG6hoMjvPpDhbQbqCPw9L73EdGU=
-- Inline policy (snowflake-iceberg-s3):
--   s3:PutObject, s3:GetObject, s3:GetObjectVersion,
--   s3:DeleteObject, s3:DeleteObjectVersion,
--   s3:ListBucket, s3:GetBucketLocation
--   on arn:aws:s3:::xdatatoiceberg1 and arn:aws:s3:::xdatatoiceberg1/*


-- ── STEP 2: External Volume ────────────────────────────────
-- NOTE: Do NOT recreate this — each CREATE OR REPLACE generates a new
-- ExternalId which requires another AWS trust policy update.
CREATE OR REPLACE EXTERNAL VOLUME iceberg_s3_vol
  STORAGE_LOCATIONS = (
    (
      NAME                 = 'iceberg-s3-us-east-2'
      STORAGE_PROVIDER     = 'S3'
      STORAGE_BASE_URL     = 's3://xdatatoiceberg1/warehouse/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::586643076710:role/snowflake-iceberg-role'
    )
  )
  ALLOW_WRITES = TRUE;

-- After creation, get the Snowflake-generated trust values:
DESCRIBE EXTERNAL VOLUME iceberg_s3_vol;
-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
-- and update the AWS IAM role trust policy with those exact values.


-- ── STEP 3: OBJECT_STORE Catalog Integration ──────────────
-- Reads existing Spark-written Iceberg metadata directly from S3.
-- No Polaris connectivity required.
CREATE OR REPLACE CATALOG INTEGRATION iceberg_object_store
    CATALOG_SOURCE = OBJECT_STORE
    TABLE_FORMAT   = ICEBERG
    ENABLED        = TRUE;


-- ── STEP 4: Create the Iceberg Table ──────────────────────
-- BASE_LOCATION is relative to the external volume STORAGE_BASE_URL:
--   s3://xdatatoiceberg1/warehouse/  +  lakehouse/events/
-- METADATA_FILE_PATH points at the current Iceberg metadata snapshot.
-- To get the latest metadata file path run (from a machine with S3 access):
--   aws s3 ls s3://xdatatoiceberg1/warehouse/lakehouse/events/metadata/ \
--     --region us-east-2 | sort | tail -1

CREATE DATABASE IF NOT EXISTS LAKEHOUSE_DB;
USE DATABASE LAKEHOUSE_DB;
CREATE SCHEMA IF NOT EXISTS EVENTS;
USE SCHEMA EVENTS;

CREATE OR REPLACE ICEBERG TABLE events
    CATALOG           = 'iceberg_object_store'
    EXTERNAL_VOLUME   = 'iceberg_s3_vol'
    BASE_LOCATION     = 'lakehouse/events/'
    METADATA_FILE_PATH = 'metadata/00011-e70488e0-86d4-4187-8232-300f1b91a646.metadata.json';

-- Verify data is visible (should return 2,200,000)
SELECT COUNT(*) FROM events;
SELECT event_type, COUNT(*) AS cnt FROM events GROUP BY 1 ORDER BY 2 DESC LIMIT 5;
SELECT MIN(ts), MAX(ts) FROM events;


-- ── STEP 5 (d): Schedule hourly metadata refresh ──────────
-- When Spark writes new data, the Iceberg metadata pointer on S3 advances.
-- This task re-reads the latest pointer every hour so Snowflake sees new data.

-- Tasks start SUSPENDED — must explicitly RESUME after creation.
CREATE OR REPLACE TASK refresh_lakehouse_events
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON 0 * * * * UTC'
    COMMENT   = 'Hourly Iceberg metadata refresh — lakehouse.events via S3 OBJECT_STORE'
AS
    ALTER ICEBERG TABLE LAKEHOUSE_DB.EVENTS.events REFRESH;

ALTER TASK refresh_lakehouse_events SUSPEND;

SHOW TASKS LIKE 'refresh_lakehouse_events';
-- Confirm state = started


-- ── STEP 6 (e): Enable / Disable the refresh job ──────────

-- ▶  ENABLE  (resume a suspended task)
ALTER TASK refresh_lakehouse_events RESUME;

-- ⏸  DISABLE  (suspend without dropping — preserves schedule)
ALTER TASK refresh_lakehouse_events SUSPEND;

-- ▶  TRIGGER an immediate out-of-schedule refresh
EXECUTE TASK refresh_lakehouse_events;

-- ▶  MANUAL refresh (no task — instant, one-shot)
ALTER ICEBERG TABLE LAKEHOUSE_DB.EVENTS.events REFRESH;

-- ▶  Check current task state
SELECT name, state, schedule, last_committed_on
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP),
    TASK_NAME => 'refresh_lakehouse_events'
))
ORDER BY scheduled_time DESC LIMIT 5;


-- ── STEP 7: Monitor execution history ─────────────────────
SELECT
    name,
    state,
    scheduled_time,
    completed_time,
    error_code,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP),
    TASK_NAME => 'refresh_lakehouse_events'
))
ORDER BY scheduled_time DESC
LIMIT 20;


-- ── STEP 8: Snowflake RBAC ────────────────────────────────
-- iceberg_engineer_sf → full read on events
-- analyst_sf          → read-only on events

CREATE ROLE IF NOT EXISTS iceberg_engineer_sf;
CREATE ROLE IF NOT EXISTS analyst_sf;

GRANT USAGE  ON DATABASE LAKEHOUSE_DB        TO ROLE iceberg_engineer_sf;
GRANT USAGE  ON SCHEMA   LAKEHOUSE_DB.EVENTS TO ROLE iceberg_engineer_sf;
GRANT SELECT ON TABLE    LAKEHOUSE_DB.EVENTS.events TO ROLE iceberg_engineer_sf;

GRANT USAGE  ON DATABASE LAKEHOUSE_DB        TO ROLE analyst_sf;
GRANT USAGE  ON SCHEMA   LAKEHOUSE_DB.EVENTS TO ROLE analyst_sf;
GRANT SELECT ON TABLE    LAKEHOUSE_DB.EVENTS.events TO ROLE analyst_sf;

GRANT ROLE iceberg_engineer_sf TO USER testsnowflake;
GRANT ROLE analyst_sf          TO USER testsnowflake;


-- ── STEP 9: Decommission (if ever needed) ─────────────────
-- Must SUSPEND task before DROP
-- ALTER TASK refresh_lakehouse_events SUSPEND;
-- DROP TASK  IF EXISTS refresh_lakehouse_events;
-- DROP TABLE IF EXISTS LAKEHOUSE_DB.EVENTS.events;
-- DROP SCHEMA IF EXISTS LAKEHOUSE_DB.EVENTS;
-- DROP DATABASE IF EXISTS LAKEHOUSE_DB;
