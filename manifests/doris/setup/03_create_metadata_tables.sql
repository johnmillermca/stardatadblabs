-- =============================================================================
-- 03_create_metadata_tables.sql
-- Create the platform_meta database and tracking tables used by the
-- Doris Dynamic Cache Manager.
--
-- Tables:
--   platform_meta.table_query_stats   — SELECT count per table, timing
--   platform_meta.cache_eviction_log  — LRU eviction audit trail
--
-- Run:
--   mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
--         < manifests/doris/setup/03_create_metadata_tables.sql
-- =============================================================================

CREATE DATABASE IF NOT EXISTS platform_meta;

USE platform_meta;

-- ── table_query_stats ─────────────────────────────────────────────────────────
-- Tracks how many times each external Iceberg table has been queried via Doris.
-- The cache manager reads/writes this table every hour to decide warm-up schedule.
--
-- Columns:
--   catalog_name        — Doris catalog name (polaris / databricks / postgres / oracle / mongodb)
--   db_name             — database/namespace inside the catalog
--   table_name          — table name
--   total_select_count  — cumulative SELECT hits since first observed
--   last_select_ts      — wall-clock timestamp of the most recent SELECT
--   prev_select_ts      — wall-clock timestamp of the second-most-recent SELECT
--                         (used to estimate select_interval_minutes)
--   select_interval_min — rolling estimate: minutes between successive SELECTs
--   warm_interval_min   — warm_interval = select_interval * 2/3 (computed by daemon)
--   last_warmed_ts      — timestamp of the last completed WARM_UP
--   cache_state         — WARM | COLD | WARMING | UNKNOWN
--   updated_at          — row last modified timestamp
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS platform_meta.table_query_stats (
    catalog_name          VARCHAR(128)  NOT NULL,
    db_name               VARCHAR(256)  NOT NULL,
    table_name            VARCHAR(256)  NOT NULL,
    total_select_count    BIGINT        NOT NULL DEFAULT 0,
    last_select_ts        DATETIME      NULL,
    prev_select_ts        DATETIME      NULL,
    select_interval_min   DOUBLE        NULL     COMMENT 'Minutes between last two SELECTs',
    warm_interval_min     DOUBLE        NULL     COMMENT 'select_interval * 2/3',
    last_warmed_ts        DATETIME      NULL,
    cache_state           VARCHAR(16)   NOT NULL DEFAULT 'UNKNOWN',
    updated_at            DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP
)
UNIQUE KEY(catalog_name, db_name, table_name)
DISTRIBUTED BY HASH(catalog_name) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);

-- ── cache_eviction_log ────────────────────────────────────────────────────────
-- Append-only audit log: one row per LRU eviction event.
--
-- Columns:
--   id            — monotonic sequence (auto-incremented via daemon logic)
--   catalog_name  — Doris catalog
--   db_name       — namespace
--   table_name    — table
--   evicted_at    — timestamp when the COLD_DOWN was issued
--   reason        — human-readable eviction reason (e.g. "no_select_24h")
--   last_select_ts — last SELECT seen before eviction (for audit)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS platform_meta.cache_eviction_log (
    id             BIGINT        NOT NULL,
    catalog_name   VARCHAR(128)  NOT NULL,
    db_name        VARCHAR(256)  NOT NULL,
    table_name     VARCHAR(256)  NOT NULL,
    evicted_at     DATETIME      NOT NULL,
    reason         VARCHAR(256)  NOT NULL,
    last_select_ts DATETIME      NULL
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);

-- Verify
SHOW TABLES FROM platform_meta;
