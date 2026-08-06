-- ============================================================
-- RBAC Control Plane — Migration 003: User Group Roles
-- Adds three purpose-built roles matching the platform user groups:
--
--   platform_admin  (b) — Full admin on all four services.
--                         Mirrors data_admin but named for the admin group.
--
--   data_engineer   (c) — SELECT + DML (INSERT/UPDATE/DELETE/LOAD) on Doris,
--                         PRODUCE+CONSUME+DESCRIBE on Kafka,
--                         INDEX_READ+INDEX_WRITE+CLUSTER_READ on OpenSearch,
--                         SUBMIT_JOB+KILL_OWN_JOB+VIEW_UI on Spark.
--
--   account_admin   (d) — Complete admin access across all four services
--                         (identical permission set to platform_admin but
--                         represents the top-level account governance group).
-- ============================================================

-- ── platform_admin ─────────────────────────────────────────
-- Service-level admin for each data source (all privs on all services)
INSERT INTO roles (name, display_name, description) VALUES
  ('platform_admin', 'Platform Admin',
   'Full administrative access to all four services (Doris, Kafka, OpenSearch, Spark). '
   'Intended for the platform infrastructure admin user group.');

INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name = 'platform_admin';   -- all permissions across all services


-- ── data_engineer ──────────────────────────────────────────
-- SELECT + full DML (no schema/admin operations)
-- Doris:       SELECT, INSERT, UPDATE, DELETE, LOAD
-- Kafka:       PRODUCE, CONSUME, DESCRIBE
-- OpenSearch:  INDEX_READ, INDEX_WRITE, CLUSTER_READ
-- Spark:       SUBMIT_JOB, KILL_OWN_JOB, VIEW_UI
INSERT INTO roles (name, display_name, description) VALUES
  ('data_engineer', 'Data Engineer',
   'SELECT and full DML privileges on Doris; produce/consume on Kafka; '
   'read/write on OpenSearch indexes; full job submission on Spark. '
   'Intended for the data_engineer user group.');

INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name = 'data_engineer'
  AND (
       (p.service_id = 1 AND p.name IN ('SELECT','INSERT','UPDATE','DELETE','LOAD'))
    OR (p.service_id = 2 AND p.name IN ('PRODUCE','CONSUME','DESCRIBE'))
    OR (p.service_id = 3 AND p.name IN ('INDEX_READ','INDEX_WRITE','CLUSTER_READ'))
    OR (p.service_id = 4 AND p.name IN ('SUBMIT_JOB','KILL_OWN_JOB','VIEW_UI'))
  );


-- ── account_admin ──────────────────────────────────────────
-- Top-level governance / account admin — all privs on all services
INSERT INTO roles (name, display_name, description) VALUES
  ('account_admin', 'Account Admin',
   'Top-level account governance: full admin privileges on Doris, Kafka, '
   'OpenSearch and Spark. Intended for the account-level admin user group '
   'responsible for user provisioning and platform-wide policy.');

INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name = 'account_admin';    -- all permissions across all services


-- ── Verification query (run to confirm) ────────────────────
-- SELECT r.name, count(rp.permission_id) AS perm_count
-- FROM roles r
-- LEFT JOIN role_permissions rp ON rp.role_id = r.id
-- WHERE r.name IN ('platform_admin','data_engineer','account_admin')
-- GROUP BY r.name;
