-- =============================================================================
-- 17_create_lhs_control_tables.sql
-- DDL for the per-table control, tombstone, and error-log tables used by the
-- dynamic LHS Sync Sidecar worker.
--
-- For every app table that is registered for Iceberg sync the worker creates
-- three tables in cache_system at runtime (all IF NOT EXISTS):
--
--   cache_system.<tbl>          — batch control table (one row per batch)
--   cache_system.<tbl>_deletes  — tombstone watermark (snap_id of deleted rows)
--   cache_system.<tbl>_log      — error audit log (one row per failed batch)
--
-- The templates below are used by sync_worker.py to generate the CREATE TABLE
-- statements dynamically for any table name.
--
-- Run (one-time, to verify syntax):
--   export STARDATA_PASS="$(kubectl get secret stardata-credentials -n prod \
--       -o jsonpath='{.data.admin-password}' | base64 -d)"
--   mysql -h stardata-fe.prod.svc.cluster.local -P 9030 -u root \
--         -p"${STARDATA_PASS}" < 17_create_lhs_control_tables.sql
--
-- In production the worker creates these tables dynamically for any new table
-- it discovers — this file is provided for documentation and manual recovery.
-- =============================================================================

-- ── Example: replace "customers" with your actual table name ─────────────────

-- ── 1. Batch control table ────────────────────────────────────────────────────
-- One row per sync batch.  The worker appends a row at the start of each batch
-- (status='running') and updates it on success or failure.
--
-- Columns used by the UI (stardata.html):
--   batch_id        — displayed in batch history panel
--   database_name   — resolves the source app DB for the table
--   table_name      — echoes the source table name
--   batch_type      — INITIAL / UPSERT / DELETE
--   snap_id_from    — window start (exclusive)
--   snap_id_to      — window end (inclusive) → used as watermark for next tick
--   row_count       — rows processed
--   status          — drives lhsStatusLabel() in the UI
--   started_at      — batch start wall clock
--   finished_at     — batch finish wall clock (NULL while running)
--   max_created_at  — MAX(created_at) from processed rows in this batch
--   max_updated_at  — MAX(updated_at) from processed rows in this batch
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cache_system.`customers` (
    batch_id        BIGINT        NOT NULL,
    database_name   VARCHAR(256)  NOT NULL  COMMENT 'Source app database name',
    table_name      VARCHAR(256)  NOT NULL  COMMENT 'Source table name',
    batch_type      VARCHAR(32)   NOT NULL  COMMENT 'INITIAL | UPSERT | DELETE',
    snap_id_from    BIGINT        NOT NULL  DEFAULT 0,
    snap_id_to      BIGINT        NOT NULL  DEFAULT 0,
    row_count       BIGINT        NOT NULL  DEFAULT 0,
    status          VARCHAR(32)   NOT NULL  DEFAULT 'running'
        COMMENT 'running | completed upsert | delete_pending | deleted | failed',
    started_at      DATETIME      NOT NULL,
    finished_at     DATETIME      NULL,
    max_created_at  DATETIME      NULL      COMMENT 'MAX(created_at) of processed rows',
    max_updated_at  DATETIME      NULL      COMMENT 'MAX(updated_at) of processed rows'
)
DUPLICATE KEY(batch_id)
DISTRIBUTED BY HASH(batch_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

-- ── 2. Tombstone watermark table ──────────────────────────────────────────────
-- Written by the source-side delete pipeline (or Doris trigger) whenever a row
-- is hard-deleted from the app table.  The worker reads this to apply deletes
-- to the Iceberg table.
--
-- Columns:
--   snap_id    — snap_id of the tombstone row in the source table
--   deleted_at — wall clock when the tombstone was recorded
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cache_system.`customers_deletes` (
    snap_id     BIGINT    NOT NULL,
    deleted_at  DATETIME  NOT NULL
)
DUPLICATE KEY(snap_id)
DISTRIBUTED BY HASH(snap_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

-- ── 3. Error log table ────────────────────────────────────────────────────────
-- The worker appends one row here whenever a batch fails.  Presence of any row
-- in this table causes the UI to show the table as "Errored" and blocks further
-- syncs until the operator truncates it via the UI "Clear error & resume" button
-- (which issues TRUNCATE TABLE cache_system.<tbl>_log).
--
-- Columns:
--   batch_id   — links back to the failed row in the control table
--   error_msg  — full exception message / SQL error text
--   logged_at  — wall clock when the error was recorded
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cache_system.`customers_log` (
    batch_id   BIGINT    NOT NULL,
    error_msg  TEXT      NOT NULL,
    logged_at  DATETIME  NOT NULL
)
DUPLICATE KEY(batch_id)
DISTRIBUTED BY HASH(batch_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

-- ── Verify ────────────────────────────────────────────────────────────────────
SHOW TABLES FROM cache_system LIKE '%customers%';
