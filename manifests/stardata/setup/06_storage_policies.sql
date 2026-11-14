-- =============================================================================
-- 06_storage_policies.sql
-- Hot / Cold storage tiering — 10 graduated policies for StarData (Apache Doris)
--
-- Architecture
-- ────────────
--   HOT tier  : local BE NVMe storage (the default storage_root_path)
--   COLD tier : AWS S3 bucket  s3://xdatatoiceberg1  (us-east-2)
--               prefix         stardata-cold/<policy-name>/
--
-- Policy matrix
-- ─────────────
--   Policy name          cooldown_ttl   Meaning
--   ──────────────────── ─────────────  ──────────────────────────────────────
--   sd_cold_1d           1  day         Hot for 1 day  → then moves to S3
--   sd_cold_2d           2  days        Hot for 2 days → then moves to S3
--   sd_cold_3d           3  days        (and so on …)
--   sd_cold_4d           4  days
--   sd_cold_5d           5  days
--   sd_cold_6d           6  days
--   sd_cold_7d           7  days
--   sd_cold_8d           8  days
--   sd_cold_9d           9  days
--   sd_cold_10d          10 days        Hot for 10 days → then moves to S3
--
-- Usage — attach a policy to a table at CREATE time:
--   CREATE TABLE analytics.events (…)
--   PROPERTIES (
--     "storage_policy" = "sd_cold_7d",
--     "replication_num" = "3"
--   );
--
-- Or add it to an existing table:
--   ALTER TABLE analytics.events SET ("storage_policy" = "sd_cold_7d");
--
-- Apply
-- ─────
--   export STARDATA_PASS="$(kubectl get secret stardata-credentials -n prod \
--       -o jsonpath='{.data.admin-password}' | base64 -d)"
--
--   mysql -h 192.168.1.50 -P 9030 -u root -p"${STARDATA_PASS}" \
--         < manifests/stardata/setup/06_storage_policies.sql
--
-- Credentials stored in K8s Secret  platform-s3-credentials  (namespace prod).
-- The raw values below match  secret/data/platform/s3  in OpenBao.
-- Never commit plaintext keys to git — this file is intentionally using the
-- real credentials because Doris stores them inside the FE metadata, not in
-- this file at runtime.  Rotate via:
--   ALTER STORAGE RESOURCE "sd_s3_cold"
--     PROPERTIES ("s3.access_key" = "<new>", "s3.secret_key" = "<new>");
-- =============================================================================

-- ── 0. Verify storage policy feature is enabled (must be true to proceed) ────
-- enable_storage_policy is set at FE startup — not runtime-mutable.
-- On this cluster it is already true (confirmed: ADMIN SHOW FRONTEND CONFIG).
-- ADMIN SHOW FRONTEND CONFIG LIKE '%storage_policy%';

-- =============================================================================
-- STEP 1 — Shared S3 storage resource
-- One resource definition covers all 10 policies; each policy gets its own
-- S3 prefix so cold data is isolated per policy.
-- Doris syntax: CREATE RESOURCE (not CREATE STORAGE RESOURCE)
-- =============================================================================
-- NOTE: s3.access_key and s3.secret_key are injected at apply time from the
-- Kubernetes secret  platform-s3-credentials  (namespace prod) via the apply
-- script 06_07_apply_storage_and_partitions.sh.
-- Do NOT hard-code credentials here — they are stored in OpenBao at
-- secret/data/platform/s3 and in the K8s secret.
CREATE RESOURCE IF NOT EXISTS "sd_s3_cold"
PROPERTIES (
    "type"                        = "s3",
    "s3.endpoint"                 = "https://s3.us-east-2.amazonaws.com",
    "s3.region"                   = "us-east-2",
    "s3.bucket"                   = "xdatatoiceberg1",
    "s3.root.path"                = "stardata-cold",
    "s3.access_key"               = "${S3_ACCESS_KEY}",
    "s3.secret_key"               = "${S3_SECRET_KEY}",
    "s3.connection.maximum"       = "50",
    "s3.connection.request.timeout" = "3000",
    "s3.connection.timeout"       = "1000",
    "s3.check.bucket.validity"    = "false"
);

-- Verify the resource was created
SHOW RESOURCES WHERE NAME = "sd_s3_cold";

-- =============================================================================
-- STEP 2 — 10 graduated cooling policies  (1 day … 10 days)
-- =============================================================================

-- ── Policy 1 — cooldown after 1 day ──────────────────────────────────────────
CREATE STORAGE POLICY IF NOT EXISTS sd_cold_1d
PROPERTIES (
    "storage_resource" = "sd_s3_cold",
    "cooldown_ttl"     = "86400"        -- 1 × 86 400 s = 1 day
);

-- ── Policy 2 — cooldown after 2 days ─────────────────────────────────────────
CREATE STORAGE POLICY IF NOT EXISTS sd_cold_2d
PROPERTIES (
    "storage_resource" = "sd_s3_cold",
    "cooldown_ttl"     = "172800"       -- 2 × 86 400 s
);

-- ── Policy 3 — cooldown after 3 days ─────────────────────────────────────────
CREATE STORAGE POLICY IF NOT EXISTS sd_cold_3d
PROPERTIES (
    "storage_resource" = "sd_s3_cold",
    "cooldown_ttl"     = "259200"       -- 3 × 86 400 s
);

-- ── Policy 4 — cooldown after 4 days ─────────────────────────────────────────
CREATE STORAGE POLICY IF NOT EXISTS sd_cold_4d
PROPERTIES (
    "storage_resource" = "sd_s3_cold",
    "cooldown_ttl"     = "345600"       -- 4 × 86 400 s
);

-- ── Policy 5 — cooldown after 5 days ─────────────────────────────────────────
CREATE STORAGE POLICY IF NOT EXISTS sd_cold_5d
PROPERTIES (
    "storage_resource" = "sd_s3_cold",
    "cooldown_ttl"     = "432000"       -- 5 × 86 400 s
);

-- ── Policy 6 — cooldown after 6 days ─────────────────────────────────────────
CREATE STORAGE POLICY IF NOT EXISTS sd_cold_6d
PROPERTIES (
    "storage_resource" = "sd_s3_cold",
    "cooldown_ttl"     = "518400"       -- 6 × 86 400 s
);

-- ── Policy 7 — cooldown after 7 days (recommended default) ───────────────────
CREATE STORAGE POLICY IF NOT EXISTS sd_cold_7d
PROPERTIES (
    "storage_resource" = "sd_s3_cold",
    "cooldown_ttl"     = "604800"       -- 7 × 86 400 s
);

-- ── Policy 8 — cooldown after 8 days ─────────────────────────────────────────
CREATE STORAGE POLICY IF NOT EXISTS sd_cold_8d
PROPERTIES (
    "storage_resource" = "sd_s3_cold",
    "cooldown_ttl"     = "691200"       -- 8 × 86 400 s
);

-- ── Policy 9 — cooldown after 9 days ─────────────────────────────────────────
CREATE STORAGE POLICY IF NOT EXISTS sd_cold_9d
PROPERTIES (
    "storage_resource" = "sd_s3_cold",
    "cooldown_ttl"     = "777600"       -- 9 × 86 400 s
);

-- ── Policy 10 — cooldown after 10 days ───────────────────────────────────────
CREATE STORAGE POLICY IF NOT EXISTS sd_cold_10d
PROPERTIES (
    "storage_resource" = "sd_s3_cold",
    "cooldown_ttl"     = "864000"       -- 10 × 86 400 s
);

-- ── Verify all 10 policies ────────────────────────────────────────────────────
SHOW STORAGE POLICY;

-- =============================================================================
-- STEP 3 — Quick-reference: S3 path layout after cooldown
-- =============================================================================
-- s3://xdatatoiceberg1/stardata-cold/<db>/<table>/<tablet_id>/<rowset>
--
-- Each policy uses the shared root "stardata-cold"; Doris appends the
-- database → table → tablet path automatically.
--
-- Example — attach sd_cold_7d to a new table:
--
--   CREATE TABLE analytics.user_events (
--       snap_id    BIGINT        NOT NULL,
--       event_ts   DATETIME      NOT NULL,
--       user_id    VARCHAR(64),
--       event_type VARCHAR(32)
--   )
--   DUPLICATE KEY (snap_id, event_ts)
--   PARTITION BY RANGE (snap_id) (
--       PARTITION p_init VALUES LESS THAN ("1000000000")
--   )
--   DISTRIBUTED BY HASH (snap_id) BUCKETS 16
--   PROPERTIES (
--       "storage_policy"  = "sd_cold_7d",
--       "replication_num" = "3",
--       "dynamic_partition.enable"    = "true",
--       "dynamic_partition.time_unit" = "NUMBER",
--       "dynamic_partition.start"     = "-10",
--       "dynamic_partition.end"       = "5",
--       "dynamic_partition.prefix"    = "p",
--       "dynamic_partition.buckets"   = "16"
--   );
-- =============================================================================
