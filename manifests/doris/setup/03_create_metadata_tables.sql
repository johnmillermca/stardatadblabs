-- =============================================================================
-- 03_create_metadata_tables.sql
-- Create the cache_system database and tracking tables used by the
-- Doris Dynamic Cache Manager.
--
-- Tables:
--   cache_system.table_query_stats   — SELECT count per table, timing
--   cache_system.cache_eviction_log  — LRU eviction audit trail
--   cache_system.table_cache_metrics — Per-table, per-BE cache I/O metrics
--                                       (written by CacheMetricsCollector each cycle)
--
-- Run:
--   mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}" \
--         < manifests/doris/setup/03_create_metadata_tables.sql
-- =============================================================================

CREATE DATABASE IF NOT EXISTS cache_system;

USE cache_system;

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
CREATE TABLE IF NOT EXISTS cache_system.table_query_stats (
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
CREATE TABLE IF NOT EXISTS cache_system.cache_eviction_log (
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

-- ── table_cache_metrics ───────────────────────────────────────────────────────
-- Per-table, per-BE cache I/O metrics written once per daemon cycle by
-- CacheMetricsCollector (background thread, own dedicated DorisClient).
--
-- Columns:
--   catalog_name         — Doris catalog (polaris / databricks / postgres / oracle / mongodb)
--   db_name              — database/namespace inside the catalog
--   table_name           — table name
--   be_host              — Backend node host (one row per table × BE per cycle)
--   sampled_at           — Timestamp of this metrics snapshot
--   local_scan_bytes     — Bytes served from BE local NVMe file_cache (fast path)
--   remote_scan_bytes    — Bytes fetched from S3/remote storage (cold path)
--   total_scan_bytes     — local + remote
--   cache_hit_pct        — local / total × 100 (0.00–100.00)
--   query_count          — Number of SELECTs in this cycle window
--   avg_query_time_ms    — Average query_time from audit_log in this window
--   cache_state          — Current WARM / COLD / WARMING / UNKNOWN state
--   last_warmed_ts       — Last completed daemon warm-up timestamp
--   warm_interval_min    — Computed warm-up cadence (select_interval × 2/3)
--   warmup_count         — Cumulative warm-up completions this daemon process lifetime
--   spark_pushdown_count — Cumulative write-pushdown submissions this daemon process lifetime
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cache_system.table_cache_metrics (
    catalog_name          VARCHAR(128)  NOT NULL,
    db_name               VARCHAR(256)  NOT NULL,
    table_name            VARCHAR(256)  NOT NULL,
    be_host               VARCHAR(256)  NOT NULL,
    sampled_at            DATETIME      NOT NULL,
    local_scan_bytes      BIGINT        NOT NULL DEFAULT 0,
    remote_scan_bytes     BIGINT        NOT NULL DEFAULT 0,
    total_scan_bytes      BIGINT        NOT NULL DEFAULT 0,
    cache_hit_pct         DOUBLE        NOT NULL DEFAULT 0.0  COMMENT 'local / total × 100',
    query_count           BIGINT        NOT NULL DEFAULT 0,
    avg_query_time_ms     DOUBLE        NOT NULL DEFAULT 0.0,
    cache_state           VARCHAR(16)   NOT NULL DEFAULT 'UNKNOWN',
    last_warmed_ts        DATETIME      NULL,
    warm_interval_min     DOUBLE        NULL,
    warmup_count          BIGINT        NOT NULL DEFAULT 0,
    spark_pushdown_count  BIGINT        NOT NULL DEFAULT 0
)
DUPLICATE KEY(catalog_name, db_name, table_name, be_host, sampled_at)
DISTRIBUTED BY HASH(catalog_name) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);

-- Verify
SHOW TABLES FROM cache_system;
