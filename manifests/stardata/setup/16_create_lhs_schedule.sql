-- =============================================================================
-- 16_create_lhs_schedule.sql
-- Persistent schedule storage for the Lakehouse sync tab.
--
-- Purpose:
--   Each iceberg-type table on the Lakehouse sync tab can have a refresh
--   schedule (interval + enabled flag).  Without this table the schedule
--   only lives in browser memory and is lost on page reload.
--
--   One row per table.  The console upserts this row on every Apply /
--   Start / Stop action and reads it back on page load to restore the
--   schedule exactly as the operator last left it.
--
-- Columns:
--   table_name   — Matches the cache_system control table name
--                  (e.g. "customers" for cache_system.customers).
--                  Works for any existing or future iceberg table because
--                  rows are created on first use — no pre-seeding required.
--   interval_ms  — Refresh interval in milliseconds.  0 = disabled.
--                  Valid values mirror the UI dropdown:
--                    0, 60000, 300000, 600000, 900000, 1800000,
--                    2700000, 3600000, 7200000, 10800000, 21600000, 43200000
--   enabled      — TRUE  → timer should be running (Start was pressed).
--                  FALSE → timer is stopped (Stop was pressed or never started).
--   updated_at   — Wall-clock timestamp of the last write, for audit.
--
-- Run:
--   mysql -h <doris-fe-host> -P 9030 -u root -p"${STARDATA_PASS}" \
--         < manifests/stardata/setup/16_create_lhs_schedule.sql
-- =============================================================================

CREATE TABLE IF NOT EXISTS cache_system.lhs_schedule (
    table_name   VARCHAR(256)  NOT NULL
        COMMENT 'Matches the cache_system control table name (e.g. customers)',
    interval_ms  BIGINT        NOT NULL   DEFAULT 0
        COMMENT '0 = disabled; otherwise ms between syncs (e.g. 300000 = 5 min)',
    enabled      TINYINT       NOT NULL   DEFAULT 0
        COMMENT '1 = timer active (Start pressed); 0 = stopped or never started',
    updated_at   DATETIME(6)   NOT NULL   DEFAULT CURRENT_TIMESTAMP
        COMMENT 'Last write timestamp'
)
UNIQUE KEY(table_name)
DISTRIBUTED BY HASH(table_name) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);

-- Verify
SELECT 'lhs_schedule created' AS result;
SHOW CREATE TABLE cache_system.lhs_schedule;
