-- =============================================================================
-- 05_set_replication_factor.sql
-- Set default replication factor to 3 for all databases in Stardata.
--
-- With 3 BE nodes (worker2/3/4) and replication_num = 3, every tablet has
-- one copy on every BE. The cluster tolerates the loss of any single node
-- with no data loss and no query degradation.
--
-- Apply:
--   mysql -h stardata-fe.prod.svc.cluster.local \
--         -P 9030 -u root -p"${STARDATA_PASS}" \
--         < manifests/stardata/setup/05_set_replication_factor.sql
--
-- Or from outside the cluster:
--   mysql -h 192.168.1.50 -P 30090 -u root -p"${STARDATA_PASS}" \
--         < manifests/stardata/setup/05_set_replication_factor.sql
-- =============================================================================

-- Set the global default replication factor to 3.
-- This applies to all new tables created without an explicit PROPERTIES clause.
ADMIN SET FRONTEND CONFIG ("default_replication_num" = "3");

-- Verify the setting took effect.
ADMIN SHOW FRONTEND CONFIG LIKE "%replication%";

-- Show all registered backends — should list 3 nodes (worker2/3/4).
SHOW BACKENDS;
