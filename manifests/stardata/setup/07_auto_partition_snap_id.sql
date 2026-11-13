-- =============================================================================
-- 07_auto_partition_snap_id.sql
-- Automatic snap_id RANGE partitioning — permanent default for StarData
-- (Apache Doris 4.2)
--
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  IMPORTANT — what "automatic" actually means in Doris                   │
-- │                                                                         │
-- │  Doris does NOT have a DDL trigger / table hook that rewrites a         │
-- │  CREATE TABLE statement at runtime the way PostgreSQL event triggers     │
-- │  do.  True zero-touch automation works at two levels:                   │
-- │                                                                         │
-- │  1. FE config (fe.conf + ADMIN SET FRONTEND CONFIG)                     │
-- │     • enable_auto_partition = true  — allows AUTO PARTITION syntax      │
-- │     • max_auto_partition_num        — allows up to N auto partitions    │
-- │     These survive restarts only when written to fe.conf (see Section B  │
-- │     and the companion ConfigMap patch script).                          │
-- │                                                                         │
-- │  2. AUTO PARTITION BY RANGE in CREATE TABLE DDL                         │
-- │     Once declared, Doris auto-creates a new range partition the         │
-- │     instant an INSERT arrives with a snap_id value that falls outside   │
-- │     all existing partition bounds — forever, for any new database.      │
-- │     Zero further ALTER TABLE or manual work needed.                     │
-- │                                                                         │
-- │  3. Hourly governance sweep (Doris JOB)                                 │
-- │     Catches tables that were created without the clause (legacy or       │
-- │     third-party tools) and retrofits them within 1 hour.               │
-- │                                                                         │
-- │  What is NOT automatic: if a developer writes CREATE TABLE without      │
-- │  snap_id as a column at all, nothing can add it — the column must       │
-- │  exist.  The governance sweep detects this and logs it as               │
-- │  NO_SNAP_ID_COL so you know.                                            │
-- └─────────────────────────────────────────────────────────────────────────┘
--
-- snap_id convention
-- ──────────────────
--   snap_id  BIGINT NOT NULL — monotonically increasing snapshot / batch id.
--   Partitions are sized at 1 000 000 snap_id units per bucket.
--
--   Auto-created partition layout (driven by INSERT data):
--     p0          → [0,          1 000 000)
--     p1000000    → [1 000 000,  2 000 000)
--     p2000000    → [2 000 000,  3 000 000)
--     … created on demand; no manual ALTER TABLE ever needed once declared.
--
-- Apply
-- ─────
--   export STARDATA_PASS="$(kubectl get secret stardata-credentials -n prod \
--       -o jsonpath='{.data.admin-password}' | base64 -d)"
--
--   mysql -h 192.168.1.50 -P 9030 -u root -p"${STARDATA_PASS}" \
--         < manifests/stardata/setup/07_auto_partition_snap_id.sql
--
-- To make enable_auto_partition PERMANENT across FE pod restarts, run the
-- companion patch after applying this file:
--   bash manifests/stardata/setup/08_patch_fe_conf_auto_partition.sh
-- =============================================================================

-- =============================================================================
-- SECTION A — Enable AUTO PARTITION (session + in-memory FE config)
-- These two settings are the only real Doris FE config keys for auto-partition.
-- They take effect immediately but are lost on FE restart unless also written
-- to fe.conf — see Section B below and 08_patch_fe_conf_auto_partition.sh.
-- =============================================================================

ADMIN SET FRONTEND CONFIG ("enable_auto_partition" = "true");

-- Raise the per-table auto-partition ceiling to 50 000 (default is 2 000).
-- Needed for tables with high snap_id cardinality over long retention windows.
ADMIN SET FRONTEND CONFIG ("max_auto_partition_num" = "50000");

-- Verify (should show enable_auto_partition = true)
ADMIN SHOW FRONTEND CONFIG LIKE "%auto_partition%";

-- =============================================================================
-- SECTION B — Persistence note (fe.conf)
-- =============================================================================
-- ADMIN SET FRONTEND CONFIG only sets the in-memory value.
-- If the FE pod restarts the setting reverts to the fe.conf default (false).
--
-- The companion script 08_patch_fe_conf_auto_partition.sh patches the
-- stardata-fe-config ConfigMap in Kubernetes to add:
--   enable_auto_partition = true
--   max_auto_partition_num = 50000
-- and triggers a rolling restart of the FE StatefulSet so it takes effect
-- permanently across all future pod restarts and cluster upgrades.
-- =============================================================================

-- =============================================================================
-- SECTION C — Audit log table for the governance sweep job
-- =============================================================================
CREATE DATABASE IF NOT EXISTS cache_system;

USE cache_system;

CREATE TABLE IF NOT EXISTS cache_system.snap_id_partition_log (
    logged_at   DATETIME     NOT NULL,
    db_name     VARCHAR(256) NOT NULL,
    table_name  VARCHAR(256) NOT NULL,
    action      VARCHAR(64)  NOT NULL,
    -- 'AUTO_PARTITION_ADDED'   — ALTER TABLE … AUTO PARTITION BY RANGE issued
    -- 'ALREADY_PARTITIONED'    — table already has RANGE partition; skip
    -- 'NO_SNAP_ID_COL'         — table has no snap_id column; cannot partition
    detail      VARCHAR(1024)
)
DUPLICATE KEY (logged_at, db_name, table_name)
DISTRIBUTED BY HASH (table_name) BUCKETS 4
PROPERTIES (
    "replication_num"             = "3",
    "dynamic_partition.enable"    = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start"     = "-30",
    "dynamic_partition.end"       = "1",
    "dynamic_partition.prefix"    = "p",
    "dynamic_partition.buckets"   = "4"
);

-- =============================================================================
-- SECTION D — Scheduled governance sweep job (runs every 1 hour)
-- =============================================================================
-- For every BASE TABLE in every user database (excluding system databases):
--
--   Case 1 — table has snap_id column AND no RANGE partition yet:
--     Issue:  ALTER TABLE <db>.<tbl>
--               AUTO PARTITION BY RANGE (snap_id) (
--                   PARTITION p0 VALUES [("0"), ("1000000"))
--               )
--     After this single ALTER, Doris handles all future partitions automatically
--     on INSERT — no further job intervention needed for that table.
--
--   Case 2 — table already has a RANGE partition:
--     Log action = 'ALREADY_PARTITIONED', skip.
--
--   Case 3 — table has no snap_id column at all:
--     Log action = 'NO_SNAP_ID_COL', skip (cannot add partition on missing col).
--
-- The job runs every 1 hour so a newly created table without AUTO PARTITION
-- will be retrofitted within at most 60 minutes of creation.
-- For brand-new tables created with the correct DDL template (Section E),
-- the job is never needed — AUTO PARTITION handles everything instantly.
-- =============================================================================
CREATE JOB IF NOT EXISTS snap_id_partition_sweep
    ON SCHEDULE EVERY 1 HOUR
    STARTS '2026-01-01 00:00:00'
    COMMENT 'Hourly: retrofit AUTO PARTITION BY RANGE(snap_id) on tables that lack it'
DO
BEGIN
    -- Step 1: Identify tables that need action this cycle.
    -- We use a temp-table pattern: insert all candidates into the log first,
    -- then read the log back for the ALTER TABLE loop.
    INSERT INTO cache_system.snap_id_partition_log
              (logged_at, db_name, table_name, action, detail)
    SELECT
        NOW()                 AS logged_at,
        t.TABLE_SCHEMA        AS db_name,
        t.TABLE_NAME          AS table_name,
        CASE
            WHEN p.TABLE_NAME IS NOT NULL THEN 'ALREADY_PARTITIONED'
            WHEN c.TABLE_NAME IS NOT NULL THEN 'AUTO_PARTITION_ADDED'
            ELSE                               'NO_SNAP_ID_COL'
        END                   AS action,
        CONCAT(
            'table=', t.TABLE_SCHEMA, '.', t.TABLE_NAME,
            ' | has_snap_id_col=', IF(c.TABLE_NAME IS NOT NULL, 'yes', 'no'),
            ' | existing_partition=', IFNULL(p.PARTITION_EXPRESSION, 'none')
        )                     AS detail
    FROM  information_schema.TABLES          t
    LEFT  JOIN information_schema.COLUMNS    c
          ON  c.TABLE_SCHEMA = t.TABLE_SCHEMA
          AND c.TABLE_NAME   = t.TABLE_NAME
          AND c.COLUMN_NAME  = 'snap_id'
    LEFT  JOIN information_schema.PARTITIONS p
          ON  p.TABLE_SCHEMA      = t.TABLE_SCHEMA
          AND p.TABLE_NAME        = t.TABLE_NAME
          AND p.PARTITION_METHOD IN ('RANGE', 'RANGE COLUMNS')
    WHERE t.TABLE_TYPE   = 'BASE TABLE'
      AND t.TABLE_SCHEMA NOT IN (
              'information_schema', 'mysql', 'performance_schema',
              '__internal_schema', 'cache_system', 'platform'
          );

    -- Step 2: Issue ALTER TABLE for tables that need the bootstrap partition.
    -- EXECUTE IMMEDIATE runs dynamic DDL inside a Doris JOB DO block.
    FOR tbl IN (
        SELECT db_name, table_name
        FROM   cache_system.snap_id_partition_log
        WHERE  logged_at >= DATE_SUB(NOW(), INTERVAL 2 MINUTE)
          AND  action    = 'AUTO_PARTITION_ADDED'
    )
    DO
        EXECUTE IMMEDIATE CONCAT(
            'ALTER TABLE `', tbl.db_name, '`.`', tbl.table_name, '`',
            ' AUTO PARTITION BY RANGE (snap_id) (',
            '   PARTITION p0 VALUES [(\"0\"), (\"1000000\"))',
            ')'
        );
        -- After this ALTER, Doris creates all future snap_id partitions
        -- on INSERT automatically — this loop never touches the table again.
    END FOR;
END;

-- Verify the job was registered
SHOW JOBS WHERE NAME = 'snap_id_partition_sweep';

-- =============================================================================
-- SECTION E — Canonical CREATE TABLE template for all new tables
-- =============================================================================
-- Use this template for every new table.  The AUTO PARTITION BY RANGE clause
-- means Doris creates new snap_id partitions on demand at INSERT time — for
-- any database, forever, with no further ALTER TABLE or DBA intervention.
--
-- ┌──────────────────────────────────────────────────────────────────────────┐
-- │  CREATE TABLE <database>.<table_name> (                                  │
-- │      snap_id      BIGINT      NOT NULL COMMENT 'snapshot / batch id',    │
-- │      -- other columns …                                                  │
-- │  )                                                                       │
-- │  DUPLICATE KEY (snap_id)   -- or UNIQUE KEY / AGGREGATE KEY              │
-- │  AUTO PARTITION BY RANGE (snap_id) (                                     │
-- │      PARTITION p0 VALUES [("0"), ("1000000"))  -- seed partition          │
-- │  )                                                                       │
-- │  DISTRIBUTED BY HASH (snap_id) BUCKETS AUTO                              │
-- │  PROPERTIES (                                                            │
-- │      "replication_num" = "3",                                            │
-- │      "storage_policy"  = "sd_cold_7d"  -- or sd_cold_1d … sd_cold_10d   │
-- │  );                                                                      │
-- └──────────────────────────────────────────────────────────────────────────┘
--
-- What happens on INSERT:
--   snap_id = 500 000   → lands in existing  p0           [0, 1 000 000)
--   snap_id = 1 700 000 → Doris AUTO-CREATES p1000000     [1 000 000, 2 000 000)
--   snap_id = 9 400 000 → Doris AUTO-CREATES p9000000     [9 000 000, 10 000 000)
--   No ALTER TABLE. No cron. No DBA action. Ever.
-- =============================================================================

-- =============================================================================
-- SECTION F — End-to-end smoke test (uncomment to run)
-- =============================================================================
-- CREATE DATABASE IF NOT EXISTS analytics;
--
-- CREATE TABLE IF NOT EXISTS analytics.snap_demo (
--     snap_id      BIGINT      NOT NULL COMMENT 'snapshot id',
--     event_ts     DATETIME    NOT NULL,
--     user_id      VARCHAR(64),
--     payload      JSON
-- )
-- DUPLICATE KEY (snap_id, event_ts)
-- AUTO PARTITION BY RANGE (snap_id) (
--     PARTITION p0 VALUES [("0"), ("1000000"))
-- )
-- DISTRIBUTED BY HASH (snap_id) BUCKETS AUTO
-- PROPERTIES (
--     "replication_num" = "3",
--     "storage_policy"  = "sd_cold_7d"
-- );
--
-- -- Insert rows spanning 3 snap_id buckets → 3 partitions auto-created:
-- INSERT INTO analytics.snap_demo VALUES
--     (500000,  NOW(), 'u1', NULL),
--     (1700000, NOW(), 'u2', NULL),
--     (9400000, NOW(), 'u3', NULL);
--
-- -- Should show p0, p1000000, p9000000 all created automatically:
-- SHOW PARTITIONS FROM analytics.snap_demo;
-- =============================================================================

-- Final state check
ADMIN SHOW FRONTEND CONFIG LIKE "%auto_partition%";
SHOW JOBS WHERE NAME = 'snap_id_partition_sweep';
SHOW TABLES FROM cache_system;
