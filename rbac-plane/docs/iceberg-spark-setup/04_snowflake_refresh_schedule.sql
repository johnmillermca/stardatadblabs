-- ============================================================
-- 04_snowflake_refresh_schedule.sql
-- ============================================================
-- (d)  Schedule a recurring metadata refresh job in Snowflake
--      so that the polaris_lakehouse catalog always reflects the
--      latest Iceberg snapshots committed by Spark.
--
-- (e)  Shows how to ENABLE and DISABLE the refresh task.
--
-- The REFRESH ICEBERG TABLE command tells Snowflake to re-read
-- the metadata pointer file from S3 and update its internal
-- catalog cache.  Wrapping it in a TASK gives an hourly
-- (or faster) scheduled execution.
--
-- RBAC note
-- ---------
-- Only users with the polaris:REFRESH_SCHEDULE permission
-- (mapped to Snowflake role iceberg_engineer_sf) may manage
-- refresh tasks.  Verify with rbacctl before running:
--
--   rbacctl user roles <your-snowflake-username>
-- ============================================================

USE ROLE iceberg_engineer_sf;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE polaris_lakehouse;
USE SCHEMA lakehouse;

-- ────────────────────────────────────────────────────────────
-- STEP 1 — Create the refresh task (runs every hour by default)
-- ────────────────────────────────────────────────────────────
-- The SCHEDULE uses a CRON expression (UTC).
-- '0 * * * *'   = top of every hour
-- '*/15 * * * *' = every 15 minutes
-- '* * * * *'   = every minute (use with caution)

CREATE OR REPLACE TASK refresh_iceberg_events
    WAREHOUSE  = COMPUTE_WH
    SCHEDULE   = 'USING CRON 0 * * * * UTC'   -- every hour at :00
    COMMENT    = 'Hourly Iceberg metadata refresh for polaris.lakehouse.events'
AS
    ALTER ICEBERG TABLE lakehouse.events REFRESH;


-- ────────────────────────────────────────────────────────────
-- STEP 2 — Enable the task (tasks start SUSPENDED by default)
-- ────────────────────────────────────────────────────────────
ALTER TASK refresh_iceberg_events RESUME;

-- Confirm it is running
SHOW TASKS LIKE 'refresh_iceberg_events';


-- ────────────────────────────────────────────────────────────
-- (e)  ENABLE and DISABLE snippets — copy/paste as needed
-- ────────────────────────────────────────────────────────────

-- ── ENABLE (resume a previously suspended task) ───────────
ALTER TASK refresh_iceberg_events RESUME;
-- Equivalent one-liner for automation / CI:
-- EXECUTE IMMEDIATE 'ALTER TASK refresh_iceberg_events RESUME';


-- ── DISABLE (suspend without dropping the task definition) ─
ALTER TASK refresh_iceberg_events SUSPEND;
-- Equivalent one-liner for automation / CI:
-- EXECUTE IMMEDIATE 'ALTER TASK refresh_iceberg_events SUSPEND';


-- ────────────────────────────────────────────────────────────
-- STEP 3 — Trigger a manual refresh right now (out of schedule)
-- ────────────────────────────────────────────────────────────
EXECUTE TASK refresh_iceberg_events;

-- Wait a few seconds then verify the snapshot updated:
ALTER ICEBERG TABLE lakehouse.events REFRESH;
SELECT SYSTEM$TASK_RUNTIME_INFO('refresh_iceberg_events');


-- ────────────────────────────────────────────────────────────
-- STEP 4 — Monitor task execution history
-- ────────────────────────────────────────────────────────────
SELECT
    name,
    state,
    scheduled_time,
    completed_time,
    error_code,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP),
    TASK_NAME => 'refresh_iceberg_events'
))
ORDER BY scheduled_time DESC
LIMIT 50;


-- ────────────────────────────────────────────────────────────
-- STEP 5 — Drop (if you need to recreate or decommission)
-- ────────────────────────────────────────────────────────────
-- ALTER TASK refresh_iceberg_events SUSPEND;   -- must suspend first
-- DROP TASK IF EXISTS refresh_iceberg_events;
