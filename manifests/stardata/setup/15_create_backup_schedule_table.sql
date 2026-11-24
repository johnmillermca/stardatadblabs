-- =============================================================================
-- 15_create_backup_schedule_table.sql
-- Creates the backup_schedule metadata table in cache_system.
-- This table stores the autobackup settings set from the StarData console
-- and is read by the autobackup CronJob.
--
-- Apply
-- ─────
--   export STARDATA_PASS="$(kubectl get secret stardata-credentials -n prod \
--       -o jsonpath='{.data.admin-password}' | base64 -d)"
--   mysql -h 192.168.1.50 -P 9030 -u root -p"${STARDATA_PASS}" \
--         < manifests/stardata/setup/15_create_backup_schedule_table.sql
-- =============================================================================

CREATE DATABASE IF NOT EXISTS cache_system;

-- ── backup_schedule ───────────────────────────────────────────────────────────
-- One row per configured autobackup schedule.  Inserted or updated from the
-- StarData console UI via the /api/doris/ HTTP SQL proxy.
--
-- Columns:
--   id              — constant '1' (single-row config)
--   freq            — 'off' | 'daily' | 'weekly'
--   retain_days     — number of days to keep snapshots (7, 14, 30)
--   databases       — JSON array of DB names to back up, e.g. '["mydb","analytics"]'
--                     If empty string, all native DBs are backed up.
--   updated_at      — when the schedule was last saved from the UI
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cache_system.backup_schedule (
    id              VARCHAR(8)    NOT NULL,
    freq            VARCHAR(16)   NOT NULL DEFAULT 'off',
    retain_days     INT           NOT NULL DEFAULT 14,
    databases       TEXT          NOT NULL DEFAULT '',
    updated_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP
)
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

-- Seed the default (off) row so the UI can always UPDATE instead of INSERT.
INSERT INTO cache_system.backup_schedule (id, freq, retain_days, databases)
VALUES ('1', 'off', 14, '')
ON DUPLICATE KEY UPDATE id = id;   -- no-op if already present

-- Verify
SELECT * FROM cache_system.backup_schedule;
