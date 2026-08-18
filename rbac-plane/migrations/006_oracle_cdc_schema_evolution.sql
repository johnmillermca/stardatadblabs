-- ============================================================
-- RBAC Control Plane — Migration 006: CDC Kafka Permissions
--
-- Adds:
--   Kafka:   CDC_SCHEMA_EVOLVE permission (schema evolution via Debezium DDL events)
--   User:    dave registered
--   Bindings: dave → data_admin, dave → iceberg_engineer
--
-- Oracle is intentionally NOT registered as a service in the RBAC plane.
-- Oracle access (LogMiner credentials, JDBC URL) is managed directly in
-- OpenBao (secret/data/platform/oracle) and does not pass through RBAC
-- role controls.
-- ============================================================

-- ── Kafka: CDC_SCHEMA_EVOLVE permission ───────────────────────────────────────
INSERT INTO permissions (service_id, name, description, metadata)
SELECT id, 'CDC_SCHEMA_EVOLVE',
       'Consume DDL change events from Debezium schema-changes topic and '
       'apply the corresponding Iceberg ALTER TABLE statements.',
       '{"resource_types":["kafka_connect","iceberg_table"],"enforcement":"application"}'
FROM services WHERE name = 'kafka'
ON CONFLICT (service_id, name) DO NOTHING;

-- ── iceberg_engineer gains CDC_SCHEMA_EVOLVE on Kafka ─────────────────────────
INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name = 'iceberg_engineer'
  AND p.service_id = (SELECT id FROM services WHERE name = 'kafka')
  AND p.name IN ('CDC_CONNECT','SCHEMA_REGISTRY_READ','SCHEMA_REGISTRY_WRITE','CDC_SCHEMA_EVOLVE')
ON CONFLICT (role_id, permission_id) DO NOTHING;

-- ── data_admin / platform_admin gain CDC_SCHEMA_EVOLVE on Kafka ───────────────
INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name IN ('data_admin','platform_admin','account_admin')
  AND p.service_id = (SELECT id FROM services WHERE name = 'kafka')
  AND p.name = 'CDC_SCHEMA_EVOLVE'
ON CONFLICT (role_id, permission_id) DO NOTHING;

-- ── Register user dave (if not already present) ───────────────────────────────
INSERT INTO users (username, display_name, email, enabled)
VALUES ('dave', 'Dave (Platform Engineer)', 'dave@stardatadblabs.local', true)
ON CONFLICT (username) DO NOTHING;

-- ── Bind dave → data_admin (all services) ─────────────────────────────────────
INSERT INTO role_bindings (user_id, role_id, service_id, granted_by)
SELECT u.id, r.id, NULL, 'migration-006'
FROM users u, roles r
WHERE u.username = 'dave'
  AND r.name     = 'data_admin'
ON CONFLICT (user_id, role_id, service_id) DO NOTHING;

-- ── Bind dave → iceberg_engineer (all services) ───────────────────────────────
INSERT INTO role_bindings (user_id, role_id, service_id, granted_by)
SELECT u.id, r.id, NULL, 'migration-006'
FROM users u, roles r
WHERE u.username = 'dave'
  AND r.name     = 'iceberg_engineer'
ON CONFLICT (user_id, role_id, service_id) DO NOTHING;

-- ── Verification ──────────────────────────────────────────────────────────────
-- SELECT u.username, r.name AS role, s.name AS service
-- FROM role_bindings rb
-- JOIN users u ON u.id = rb.user_id
-- JOIN roles r ON r.id = rb.role_id
-- LEFT JOIN services s ON s.id = rb.service_id
-- WHERE u.username = 'dave'
-- ORDER BY r.name;
