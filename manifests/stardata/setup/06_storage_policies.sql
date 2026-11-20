-- =============================================================================
-- 06_storage_policies.sql
-- Hot / Cold storage tiering — single 1-day policy for StarData (Apache Doris)
--
-- Architecture
-- ────────────
--   HOT tier  : local BE NVMe storage (the default storage_root_path)
--   COLD tier : AWS S3 bucket  s3://xdatatoiceberg1  (us-east-2)
--               prefix         stardata-cold/
--
-- Policy
-- ──────
--   Policy name          cooldown_ttl   Meaning
--   ──────────────────── ─────────────  ──────────────────────────────────────
--   sd_cold_1d           1  day         Hot for 1 day  → then moves to S3
--
-- Usage
-- ─────
-- The Stardata FE patch (CreateTableInfo.java :: injectStoragePolicy) attaches
-- sd_cold_1d automatically to every new OLAP table in any database except
-- cache_system.  No manual PROPERTIES clause is needed for new tables.
--
-- To attach the policy to an existing table (pre-patch):
--   ALTER TABLE analytics.events SET ("storage_policy" = "sd_cold_1d");
--
-- To override with a different policy on a specific new table, include an
-- explicit PROPERTIES clause — the FE patch uses putIfAbsent and will not
-- overwrite a user-supplied value:
--   CREATE TABLE analytics.events (…)
--   PROPERTIES ("storage_policy" = "sd_cold_7d", "replication_num" = "3");
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
-- STEP 2 — 1-day cooling policy
-- =============================================================================

-- ── Policy — cooldown after 1 day ────────────────────────────────────────────
CREATE STORAGE POLICY IF NOT EXISTS sd_cold_1d
PROPERTIES (
    "storage_resource" = "sd_s3_cold",
    "cooldown_ttl"     = "86400"        -- 1 × 86 400 s = 1 day
);

-- Verify
SHOW STORAGE POLICY WHERE POLICY_NAME = "sd_cold_1d";
