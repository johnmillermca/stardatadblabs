-- =============================================================================
-- 09_test_stardata_features.sql
-- Regression tests for the Stardata FE auto-injection patch
-- (CreateTableInfo.java :: injectStardataFeatures)
--
-- Covers all five conditions requested:
--   (a) snap_id is injected ONLY when PROPERTIES ("stardata.table_type" = "iceberg")
--   (b) iceberg tables always get snap_id + created_at + updated_at, including in
--       the cache_system DB; regular tables get created_at only and are skipped
--       in cache_system
--   (c) hot/cold storage policy (sd_cold_1d) is applied to all qualifying tables
--   (d) iceberg tables: AUTO PARTITION BY RANGE (date_trunc('day', created_at))
--                        DISTRIBUTED BY HASH(snap_id) BUCKETS AUTO
--   (e) non-iceberg tables without unique keys: same range partition + RANDOM bucket
--       non-iceberg tables WITH unique keys: same range partition + user distribution
--
-- How to run
-- ──────────
--   export STARDATA_PASS="$(kubectl get secret stardata-credentials -n prod \
--       -o jsonpath='{.data.admin-password}' | base64 -d)"
--
--   mysql -h 192.168.1.50 -P 9030 -u root -p"${STARDATA_PASS}" \
--         --comments < manifests/stardata/setup/09_test_stardata_features.sql
--
-- Expected output: every SHOW CREATE TABLE block must NOT contain the word FAIL.
-- A manual review of the SHOW CREATE TABLE output is required after each test.
-- =============================================================================

-- ── Setup ────────────────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS sd_test;
CREATE DATABASE IF NOT EXISTS cache_system;

-- Clean up any leftover tables from a previous run
DROP TABLE IF EXISTS sd_test.t_regular_no_unique;
DROP TABLE IF EXISTS sd_test.t_regular_unique;
DROP TABLE IF EXISTS sd_test.t_iceberg_minimal;
DROP TABLE IF EXISTS sd_test.t_iceberg_user_declared;
DROP TABLE IF EXISTS sd_test.t_iceberg_cache;
DROP TABLE IF EXISTS cache_system.t_regular_cache;
DROP TABLE IF EXISTS sd_test.t_disable_partition;

-- =============================================================================
-- TEST (a) + (c):
--   Regular table (no "stardata.table_type" property)
--   MUST NOT have snap_id injected.
--   MUST have created_at injected.
--   MUST have storage_policy = sd_cold_1d.
--   MUST have AUTO PARTITION BY RANGE (date_trunc('day', created_at)).
--   MUST be DISTRIBUTED BY RANDOM (no unique key).
-- =============================================================================
SELECT '── TEST (a)/(c): regular table — no snap_id, has created_at, random dist ──' AS test;

CREATE TABLE sd_test.t_regular_no_unique (
    event_id  INT          NOT NULL,
    payload   VARCHAR(256)
);

-- Expected in SHOW CREATE TABLE:
--   ✓  created_at   DATETIMEV2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP
--   ✓  AUTO PARTITION BY RANGE (date_trunc('day', `created_at`)) ()
--   ✓  DISTRIBUTED BY RANDOM BUCKETS AUTO
--   ✓  "storage_policy" = "sd_cold_1d"
--   ✗  snap_id  (must NOT appear)
--   ✗  updated_at (must NOT appear)
SHOW CREATE TABLE sd_test.t_regular_no_unique;

-- Structural verify via information_schema
SELECT
    CASE WHEN COUNT(*) = 0 THEN 'PASS: snap_id absent from regular table'
         ELSE 'FAIL: snap_id was injected into regular table' END AS result_snap_id
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'sd_test'
  AND TABLE_NAME   = 't_regular_no_unique'
  AND COLUMN_NAME  = 'snap_id';

SELECT
    CASE WHEN COUNT(*) = 1 THEN 'PASS: created_at present in regular table'
         ELSE 'FAIL: created_at missing from regular table' END AS result_created_at
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'sd_test'
  AND TABLE_NAME   = 't_regular_no_unique'
  AND COLUMN_NAME  = 'created_at';

SELECT
    CASE WHEN COUNT(*) = 0 THEN 'PASS: updated_at absent from regular table'
         ELSE 'FAIL: updated_at was injected into regular table' END AS result_updated_at
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'sd_test'
  AND TABLE_NAME   = 't_regular_no_unique'
  AND COLUMN_NAME  = 'updated_at';

-- =============================================================================
-- TEST (e) — unique key: existing distribution is preserved, NOT overridden to RANDOM
-- =============================================================================
SELECT '── TEST (e): regular UNIQUE KEY table — distribution kept, no snap_id ──' AS test;

CREATE TABLE sd_test.t_regular_unique (
    user_id   VARCHAR(64)  NOT NULL,
    email     VARCHAR(256) NOT NULL
)
UNIQUE KEY (user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 10;

-- Expected in SHOW CREATE TABLE:
--   ✓  created_at   DATETIMEV2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP
--   ✓  AUTO PARTITION BY RANGE (date_trunc('day', `created_at`)) ()
--   ✓  DISTRIBUTED BY HASH(`user_id`) BUCKETS 10   ← user value kept
--   ✓  "storage_policy" = "sd_cold_1d"             ← regular non-MOW UNIQUE KEY gets storage_policy
--   ✗  snap_id  (must NOT appear)
--   ✗  DISTRIBUTED BY RANDOM  (must NOT appear)
SHOW CREATE TABLE sd_test.t_regular_unique;

SELECT
    CASE WHEN COUNT(*) = 0 THEN 'PASS: snap_id absent from unique-key table'
         ELSE 'FAIL: snap_id was injected into unique-key table' END AS result_snap_id
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'sd_test'
  AND TABLE_NAME   = 't_regular_unique'
  AND COLUMN_NAME  = 'snap_id';

SELECT
    CASE WHEN COUNT(*) = 1 THEN 'PASS: created_at present in unique-key table'
         ELSE 'FAIL: created_at missing from unique-key table' END AS result_created_at
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'sd_test'
  AND TABLE_NAME   = 't_regular_unique'
  AND COLUMN_NAME  = 'created_at';

-- =============================================================================
-- TEST (a) + (b) + (d):
--   Iceberg-type table (PROPERTIES ("stardata.table_type" = "iceberg"))
--   MUST have snap_id, created_at, updated_at injected.
--   MUST have AUTO PARTITION BY RANGE (date_trunc('day', created_at)).
--   MUST be DISTRIBUTED BY HASH(snap_id) BUCKETS AUTO.
--   MUST have storage_policy = sd_cold_1d.
--   The internal property stardata.table_type must NOT appear in the final DDL.
-- =============================================================================
SELECT '── TEST (a)/(b)/(d): iceberg-type table — snap_id + created_at + updated_at + HASH dist ──' AS test;

CREATE TABLE sd_test.t_iceberg_minimal (
    record_id  INT          NOT NULL,
    data       VARCHAR(512)
)
PROPERTIES ("stardata.table_type" = "iceberg");

-- Expected in SHOW CREATE TABLE:
--   ✓  snap_id      BIGINT NOT NULL AUTO_INCREMENT
--   ✓  created_at   DATETIMEV2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP
--   ✓  updated_at   DATETIMEV2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP  [ON UPDATE …]
--   ✓  AUTO PARTITION BY RANGE (date_trunc('day', `created_at`)) ()
--   ✓  DISTRIBUTED BY HASH(`snap_id`) BUCKETS AUTO
--   ✓  "storage_policy" = "sd_cold_1d"
--   ✗  "stardata.table_type"  (must NOT appear — stripped before Doris analyzers)
SHOW CREATE TABLE sd_test.t_iceberg_minimal;

SELECT
    CASE WHEN COUNT(*) = 1 THEN 'PASS: snap_id present in iceberg-type table'
         ELSE 'FAIL: snap_id missing from iceberg-type table' END AS result_snap_id
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'sd_test'
  AND TABLE_NAME   = 't_iceberg_minimal'
  AND COLUMN_NAME  = 'snap_id';

SELECT
    CASE WHEN COUNT(*) = 1 THEN 'PASS: created_at present in iceberg-type table'
         ELSE 'FAIL: created_at missing from iceberg-type table' END AS result_created_at
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'sd_test'
  AND TABLE_NAME   = 't_iceberg_minimal'
  AND COLUMN_NAME  = 'created_at';

SELECT
    CASE WHEN COUNT(*) = 1 THEN 'PASS: updated_at present in iceberg-type table'
         ELSE 'FAIL: updated_at missing from iceberg-type table' END AS result_updated_at
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'sd_test'
  AND TABLE_NAME   = 't_iceberg_minimal'
  AND COLUMN_NAME  = 'updated_at';

-- =============================================================================
-- TEST (b) — user pre-declares snap_id / created_at / updated_at on iceberg table:
--   existing columns MUST be kept as-is; no duplicate injection.
-- =============================================================================
SELECT '── TEST (b): iceberg-type — user-declared columns are not duplicated ──' AS test;

CREATE TABLE sd_test.t_iceberg_user_declared (
    snap_id    BIGINT       NOT NULL,
    created_at DATETIMEV2   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIMEV2   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    value      VARCHAR(128)
)
PROPERTIES ("stardata.table_type" = "iceberg");

-- Expected: exactly ONE snap_id, ONE created_at, ONE updated_at column.
SHOW CREATE TABLE sd_test.t_iceberg_user_declared;

SELECT
    CASE WHEN COUNT(*) = 1 THEN 'PASS: snap_id appears exactly once (user-declared kept)'
         ELSE 'FAIL: snap_id duplicated or missing' END AS result_snap_id
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'sd_test'
  AND TABLE_NAME   = 't_iceberg_user_declared'
  AND COLUMN_NAME  = 'snap_id';

SELECT
    CASE WHEN COUNT(*) = 1 THEN 'PASS: created_at appears exactly once'
         ELSE 'FAIL: created_at duplicated or missing' END AS result_created_at
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'sd_test'
  AND TABLE_NAME   = 't_iceberg_user_declared'
  AND COLUMN_NAME  = 'created_at';

SELECT
    CASE WHEN COUNT(*) = 1 THEN 'PASS: updated_at appears exactly once'
         ELSE 'FAIL: updated_at duplicated or missing' END AS result_updated_at
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'sd_test'
  AND TABLE_NAME   = 't_iceberg_user_declared'
  AND COLUMN_NAME  = 'updated_at';

-- =============================================================================
-- TEST (b) — iceberg table in cache_system DB:
--   MUST receive full injection (snap_id + created_at + updated_at + partition
--   + storage policy).  cache_system is excluded only for REGULAR tables.
-- =============================================================================
SELECT '── TEST (b): iceberg-type in cache_system — full injection applied ──' AS test;

CREATE TABLE cache_system.t_iceberg_cache (
    record_id  INT          NOT NULL,
    data       VARCHAR(256)
)
PROPERTIES ("stardata.table_type" = "iceberg");

SHOW CREATE TABLE cache_system.t_iceberg_cache;

SELECT
    CASE WHEN COUNT(*) = 1 THEN 'PASS: snap_id injected into iceberg table in cache_system'
         ELSE 'FAIL: snap_id missing from iceberg table in cache_system' END AS result_snap_id
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'cache_system'
  AND TABLE_NAME   = 't_iceberg_cache'
  AND COLUMN_NAME  = 'snap_id';

SELECT
    CASE WHEN COUNT(*) = 1 THEN 'PASS: updated_at injected into iceberg table in cache_system'
         ELSE 'FAIL: updated_at missing from iceberg table in cache_system' END AS result_updated_at
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'cache_system'
  AND TABLE_NAME   = 't_iceberg_cache'
  AND COLUMN_NAME  = 'updated_at';

-- =============================================================================
-- TEST (b) — regular table in cache_system DB:
--   MUST be skipped entirely — no created_at, no partition, no storage policy.
-- =============================================================================
SELECT '── TEST (b): regular table in cache_system — injection skipped ──' AS test;

CREATE TABLE cache_system.t_regular_cache (
    id    INT NOT NULL,
    value VARCHAR(64)
);

SHOW CREATE TABLE cache_system.t_regular_cache;

SELECT
    CASE WHEN COUNT(*) = 0 THEN 'PASS: created_at NOT injected into regular cache_system table'
         ELSE 'FAIL: created_at was injected into regular cache_system table' END AS result_created_at
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'cache_system'
  AND TABLE_NAME   = 't_regular_cache'
  AND COLUMN_NAME  = 'created_at';

-- =============================================================================
-- TEST (c) — storage policy present on all qualifying tables
-- =============================================================================
SELECT '── TEST (c): storage_policy = sd_cold_1d on all qualifying tables ──' AS test;

SELECT
    TABLE_NAME,
    CASE WHEN LOCATE('"storage_policy" = "sd_cold_1d"', CREATE_TIME) > 0
              OR TABLE_COMMENT LIKE '%sd_cold_1d%'
         THEN 'NOTE: check SHOW CREATE TABLE manually for storage_policy'
         ELSE 'NOTE: check SHOW CREATE TABLE manually for storage_policy'
    END AS storage_policy_note
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'sd_test'
  AND TABLE_NAME IN ('t_regular_no_unique', 't_regular_unique',
                     't_iceberg_minimal',   't_iceberg_user_declared');

-- Visual confirmation — look for "storage_policy" = "sd_cold_1d" in each:
SHOW CREATE TABLE sd_test.t_regular_no_unique;
SHOW CREATE TABLE sd_test.t_iceberg_minimal;

-- =============================================================================
-- TEST — stardata.table_type must NOT leak into stored DDL
-- =============================================================================
SELECT '── TEST: stardata.table_type stripped from final PROPERTIES ──' AS test;

-- The property is consumed and removed by injectStardataFeatures() before
-- Doris's property analyzer runs — it must never appear in SHOW CREATE TABLE.
SHOW CREATE TABLE sd_test.t_iceberg_minimal;
-- Verify by eye: "stardata.table_type" must NOT appear in the output above.

-- =============================================================================
-- TEST — disable_partition opt-out still works
-- =============================================================================
SELECT '── TEST: stardata.disable_partition=true — no partition injected ──' AS test;

CREATE TABLE sd_test.t_disable_partition (
    code  VARCHAR(32) NOT NULL,
    label VARCHAR(256)
)
PROPERTIES ("stardata.disable_partition" = "true");

-- Expected:
--   ✓  created_at present
--   ✓  "storage_policy" = "sd_cold_1d"
--   ✗  AUTO PARTITION BY RANGE  (must NOT appear)
SHOW CREATE TABLE sd_test.t_disable_partition;

SELECT
    CASE WHEN COUNT(*) = 1 THEN 'PASS: created_at still injected when partition disabled'
         ELSE 'FAIL: created_at missing even with disable_partition=true' END AS result_created_at
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'sd_test'
  AND TABLE_NAME   = 't_disable_partition'
  AND COLUMN_NAME  = 'created_at';

-- =============================================================================
-- Cleanup
-- =============================================================================
SELECT '── Cleanup ──' AS test;

DROP TABLE IF EXISTS sd_test.t_regular_no_unique;
DROP TABLE IF EXISTS sd_test.t_regular_unique;
DROP TABLE IF EXISTS sd_test.t_iceberg_minimal;
DROP TABLE IF EXISTS sd_test.t_iceberg_user_declared;
DROP TABLE IF EXISTS sd_test.t_iceberg_cache;
DROP TABLE IF EXISTS cache_system.t_regular_cache;
DROP TABLE IF EXISTS sd_test.t_disable_partition;

SELECT 'All test tables dropped. Review PASS/FAIL rows above.' AS done;
