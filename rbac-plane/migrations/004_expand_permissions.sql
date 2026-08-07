-- ============================================================
-- RBAC Control Plane — Migration 004: Expanded Permission Set
-- Adds missing permissions discovered during permission audit:
--
-- Doris (2 new):
--   NODE        — cluster node operations (NODE_PRIV on *.*.*), highest priv
--   SHOW_VIEW   — show create view / describe views (Show_view_priv)
--
-- Kafka (4 new):
--   SCHEMA_REGISTRY_READ    — read schemas via Confluent Schema Registry REST API
--   SCHEMA_REGISTRY_WRITE   — register/update schemas via Schema Registry
--   CDC_CONNECT             — create/manage Debezium connector configs (Connect REST API)
--   CONSUMER_GROUP_MANAGE   — describe + delete consumer groups (ACL: group:*:Describe+Delete)
--   TRANSACTIONAL_WRITE     — use Kafka transactions (ACL: transactionalId:*:Write+Describe)
--
-- Spark (3 new):
--   USE_CATALOG       — access the Polaris REST catalog (reads catalog metadata)
--   WRITE_ICEBERG     — write/create/modify Iceberg tables via Polaris catalog
--   ADMIN_CATALOG     — full Polaris catalog admin (create namespaces, drop tables, etc.)
-- ============================================================

-- ── Doris additions ────────────────────────────────────────
INSERT INTO permissions (service_id, name, description, metadata) VALUES
  (1, 'NODE',
   'Cluster node operations — add/decommission nodes. NODE_PRIV on *.*.*. Highest-risk privilege.',
   '{"resource_types":["global"],"risk":"critical"}'),
  (1, 'SHOW_VIEW',
   'Show CREATE VIEW / describe views. Show_view_priv. Required for BI tools that inspect view DDL.',
   '{"resource_types":["table","view"]}');

-- ── Kafka additions ────────────────────────────────────────
INSERT INTO permissions (service_id, name, description, metadata) VALUES
  (2, 'SCHEMA_REGISTRY_READ',
   'Read schemas from Confluent Schema Registry REST API (GET /subjects, /schemas). '
   'Enforced at the Schema Registry application layer — not a native Kafka ACL.',
   '{"resource_types":["schema_registry"],"enforcement":"application"}'),
  (2, 'SCHEMA_REGISTRY_WRITE',
   'Register or update schemas in Confluent Schema Registry (POST /subjects). '
   'Enforced at the Schema Registry application layer.',
   '{"resource_types":["schema_registry"],"enforcement":"application"}'),
  (2, 'CDC_CONNECT',
   'Create, read, update, and delete Debezium connector configurations via the '
   'Kafka Connect REST API. Enforced at the Connect REST API layer.',
   '{"resource_types":["kafka_connect"],"enforcement":"application"}'),
  (2, 'CONSUMER_GROUP_MANAGE',
   'Describe and delete Kafka consumer groups. '
   'Strimzi ACL: group:*:Describe + Delete operations.',
   '{"resource_types":["group"]}'),
  (2, 'TRANSACTIONAL_WRITE',
   'Use Kafka transactional producers. '
   'Strimzi ACL: transactionalId:*:Write + Describe operations.',
   '{"resource_types":["transactionalId"]}');

-- ── Spark additions ────────────────────────────────────────
INSERT INTO permissions (service_id, name, description, metadata) VALUES
  (4, 'USE_CATALOG',
   'Access the Polaris Iceberg REST catalog — read catalog metadata, list namespaces '
   'and tables. Required for any Spark job that reads Iceberg tables via the polaris catalog.',
   '{"resource_types":["catalog"],"catalog":"polaris"}'),
  (4, 'WRITE_ICEBERG',
   'Create, insert into, and modify Iceberg tables via the Polaris REST catalog. '
   'Requires USE_CATALOG. Grants Polaris TABLE_WRITE_DATA + TABLE_CREATE privileges.',
   '{"resource_types":["iceberg_table","catalog"],"catalog":"polaris"}'),
  (4, 'ADMIN_CATALOG',
   'Full Polaris catalog administration: create/drop namespaces, drop tables, '
   'manage catalog grants. Grants Polaris CATALOG_MANAGE_CONTENT + MANAGE_GRANTS.',
   '{"resource_types":["catalog"],"catalog":"polaris","risk":"high"}');


-- ── Update existing admin roles to include the new permissions ──────────────
-- platform_admin, account_admin, data_admin  →  get ALL new permissions
INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name IN ('platform_admin', 'account_admin', 'data_admin')
  AND p.name IN (
    'NODE', 'SHOW_VIEW',
    'SCHEMA_REGISTRY_READ', 'SCHEMA_REGISTRY_WRITE', 'CDC_CONNECT',
    'CONSUMER_GROUP_MANAGE', 'TRANSACTIONAL_WRITE',
    'USE_CATALOG', 'WRITE_ICEBERG', 'ADMIN_CATALOG'
  )
ON CONFLICT (role_id, permission_id) DO NOTHING;

-- data_engineer  →  gains SHOW_VIEW, SCHEMA_REGISTRY_READ, CONSUMER_GROUP_MANAGE,
--                        TRANSACTIONAL_WRITE, USE_CATALOG, WRITE_ICEBERG
INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name = 'data_engineer'
  AND p.name IN (
    'SHOW_VIEW',
    'SCHEMA_REGISTRY_READ',
    'CONSUMER_GROUP_MANAGE',
    'TRANSACTIONAL_WRITE',
    'USE_CATALOG', 'WRITE_ICEBERG'
  )
ON CONFLICT (role_id, permission_id) DO NOTHING;

-- analyst  →  gains SHOW_VIEW, SCHEMA_REGISTRY_READ, USE_CATALOG
INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name = 'analyst'
  AND p.name IN ('SHOW_VIEW', 'SCHEMA_REGISTRY_READ', 'USE_CATALOG')
ON CONFLICT (role_id, permission_id) DO NOTHING;

-- etl_writer  →  gains SCHEMA_REGISTRY_READ, SCHEMA_REGISTRY_WRITE,
--                      TRANSACTIONAL_WRITE, USE_CATALOG, WRITE_ICEBERG
INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name = 'etl_writer'
  AND p.name IN (
    'SCHEMA_REGISTRY_READ', 'SCHEMA_REGISTRY_WRITE',
    'TRANSACTIONAL_WRITE',
    'USE_CATALOG', 'WRITE_ICEBERG'
  )
ON CONFLICT (role_id, permission_id) DO NOTHING;

-- kafka_consumer  →  gains SCHEMA_REGISTRY_READ
INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name = 'kafka_consumer'
  AND p.name = 'SCHEMA_REGISTRY_READ'
ON CONFLICT (role_id, permission_id) DO NOTHING;
