-- =============================================================================
-- 14_create_backup_repository.sql
-- Creates a Doris S3 REPOSITORY used by BACKUP / RESTORE operations.
--
-- Repository  : sd_backup_repo
-- Bucket      : s3://xdatatoiceberg1   (us-east-2)
-- Prefix      : stardata-backups/
--
-- S3 credentials are injected at apply time via envsubst from
-- the K8s Secret  platform-s3-credentials  (namespace prod).
-- Never hard-code keys here — they live in OpenBao at
-- secret/data/platform/s3.
--
-- Apply
-- ─────
--   export S3_ACCESS_KEY="$(kubectl get secret platform-s3-credentials -n prod \
--       -o jsonpath='{.data.access-key}' | base64 -d)"
--   export S3_SECRET_KEY="$(kubectl get secret platform-s3-credentials -n prod \
--       -o jsonpath='{.data.secret-key}' | base64 -d)"
--   export STARDATA_PASS="$(kubectl get secret stardata-credentials -n prod \
--       -o jsonpath='{.data.admin-password}' | base64 -d)"
--   envsubst < manifests/stardata/setup/14_create_backup_repository.sql \
--     | mysql -h 192.168.1.50 -P 9030 -u root -p"${STARDATA_PASS}"
--
-- Or use the wrapper script:
--   bash manifests/stardata/setup/14_setup_backup.sh
--
-- Verify
-- ──────
--   SHOW REPOSITORIES;
-- =============================================================================

CREATE REPOSITORY IF NOT EXISTS sd_backup_repo
WITH S3
ON LOCATION "s3://xdatatoiceberg1/stardata-backups"
PROPERTIES (
    "s3.endpoint"       = "https://s3.us-east-2.amazonaws.com",
    "s3.region"         = "us-east-2",
    "s3.access_key"     = "${S3_ACCESS_KEY}",
    "s3.secret_key"     = "${S3_SECRET_KEY}"
);

-- Verify
SHOW REPOSITORIES;
