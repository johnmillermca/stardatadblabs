-- =============================================================================
-- 08_datetime_load_tracker.sql
-- One-time setup: create cache_system.datetime_load_tracker
--
-- What this does
-- ──────────────
-- Creates the tracking table that the DDL Audit Plugin writes into once,
-- immediately and asynchronously, whenever a CREATE TABLE or ALTER TABLE
-- completes on any user table that satisfies BOTH:
--   (a) Has a column defined as  DATETIME DEFAULT CURRENT_TIMESTAMP
--       (created-at column — no ON UPDATE clause)
--   (b) Has a column defined as  DATETIME DEFAULT CURRENT_TIMESTAMP
--                                ON UPDATE CURRENT_TIMESTAMP
--       (updated-at column)
--
-- One row per qualifying table. Written once per table event. No polling.
--
-- Columns
-- ───────
--   database_name   — actual database that owns the qualifying table
--   table_name      — name of the qualifying table
--   created_ts_col  — name of the column satisfying condition (a)
--   updated_ts_col  — name of the column satisfying condition (b)
--   status          — fixed literal 'load'
--
-- Apply
-- ─────
--   export STARDATA_PASS="$(kubectl get secret stardata-credentials -n prod \
--       -o jsonpath='{.data.admin-password}' | base64 -d)"
--
--   mysql -h 192.168.1.50 -P 9030 -u root -p"${STARDATA_PASS}" \
--         < manifests/stardata/setup/08_datetime_load_tracker.sql
-- =============================================================================

CREATE DATABASE IF NOT EXISTS cache_system;

-- Composite UNIQUE KEY(database_name, table_name) — one row per table per db.
-- ON DUPLICATE KEY UPDATE handles the ALTER TABLE case (re-fires on the same table).
CREATE TABLE IF NOT EXISTS cache_system.datetime_load_tracker (
    database_name   VARCHAR(256)  NOT NULL
        COMMENT 'Actual database name that owns the qualifying table',
    table_name      VARCHAR(256)  NOT NULL
        COMMENT 'Name of the qualifying table',
    created_ts_col  VARCHAR(256)  NOT NULL
        COMMENT 'Column name that has DATETIME DEFAULT CURRENT_TIMESTAMP',
    updated_ts_col  VARCHAR(256)  NOT NULL
        COMMENT 'Column name that has DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
    status          VARCHAR(32)   NOT NULL  DEFAULT 'load'
        COMMENT 'Fixed value: load'
)
UNIQUE KEY(database_name, table_name)
DISTRIBUTED BY HASH(database_name) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);

-- Verify
SHOW CREATE TABLE cache_system.datetime_load_tracker;
