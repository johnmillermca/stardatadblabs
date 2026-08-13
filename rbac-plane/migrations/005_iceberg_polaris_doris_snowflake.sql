-- ============================================================
-- RBAC Control Plane — Migration 005: Iceberg / Polaris / Doris / Snowflake
--
-- Adds:
--   Service  : polaris     (Apache Polaris Iceberg REST catalog)
--   Service  : snowflake   (Snowflake proprietary tables via JDBC)
--   Permissions for both new services.
--   New role  : iceberg_engineer — full Iceberg read/write via Polaris
--   New role  : snowflake_reader — SELECT on Snowflake tables via Doris JDBC catalog
--   Doris external-catalog permissions for warm-up scheduling.
-- ============================================================

-- ── New services ───────────────────────────────────────────
INSERT INTO services (name, display_name, description) VALUES
  ('polaris',
   'Apache Polaris',
   'Open Polaris Iceberg REST catalog — manages namespaces and Iceberg table metadata.'),
  ('snowflake',
   'Snowflake',
   'Snowflake cloud data warehouse — accessed via Doris JDBC external catalog.');

-- Service IDs are sequential; use sub-select so this is idempotent regardless of
-- the actual assigned ids.

-- ── Polaris permissions ────────────────────────────────────
INSERT INTO permissions (service_id, name, description, metadata)
SELECT id, 'CATALOG_READ',
       'Read catalog metadata — list namespaces, list tables, describe table schema.',
       '{"resource_types":["catalog","namespace","table"],"catalog":"polaris"}'
FROM services WHERE name = 'polaris'
ON CONFLICT (service_id, name) DO NOTHING;

INSERT INTO permissions (service_id, name, description, metadata)
SELECT id, 'TABLE_READ',
       'Read Iceberg table data (SELECT) via the Polaris REST catalog.',
       '{"resource_types":["iceberg_table"],"catalog":"polaris"}'
FROM services WHERE name = 'polaris'
ON CONFLICT (service_id, name) DO NOTHING;

INSERT INTO permissions (service_id, name, description, metadata)
SELECT id, 'TABLE_WRITE',
       'Write Iceberg table data (INSERT / MERGE) and create new tables via Polaris.',
       '{"resource_types":["iceberg_table"],"catalog":"polaris"}'
FROM services WHERE name = 'polaris'
ON CONFLICT (service_id, name) DO NOTHING;

INSERT INTO permissions (service_id, name, description, metadata)
SELECT id, 'NAMESPACE_ADMIN',
       'Create and drop namespaces in the Polaris catalog.',
       '{"resource_types":["namespace"],"catalog":"polaris"}'
FROM services WHERE name = 'polaris'
ON CONFLICT (service_id, name) DO NOTHING;

INSERT INTO permissions (service_id, name, description, metadata)
SELECT id, 'CATALOG_ADMIN',
       'Full Polaris admin: manage catalogs, namespaces, tables, and grants.',
       '{"resource_types":["catalog"],"catalog":"polaris","risk":"high"}'
FROM services WHERE name = 'polaris'
ON CONFLICT (service_id, name) DO NOTHING;

INSERT INTO permissions (service_id, name, description, metadata)
SELECT id, 'REFRESH_SCHEDULE',
       'Create, enable and disable Snowflake metadata-refresh scheduled tasks '
       'that pull Iceberg metadata from S3 via the Polaris catalog.',
       '{"resource_types":["catalog","refresh_task"],"catalog":"polaris"}'
FROM services WHERE name = 'polaris'
ON CONFLICT (service_id, name) DO NOTHING;

-- ── Snowflake permissions (accessed via Doris JDBC catalog) ─
INSERT INTO permissions (service_id, name, description, metadata)
SELECT id, 'JDBC_SELECT',
       'Execute SELECT queries on Snowflake tables through the Doris JDBC external catalog.',
       '{"resource_types":["table"],"catalog_type":"jdbc"}'
FROM services WHERE name = 'snowflake'
ON CONFLICT (service_id, name) DO NOTHING;

INSERT INTO permissions (service_id, name, description, metadata)
SELECT id, 'JDBC_ADMIN',
       'Administer the Doris JDBC external catalog for Snowflake (create/drop/refresh).',
       '{"resource_types":["catalog"],"catalog_type":"jdbc","risk":"medium"}'
FROM services WHERE name = 'snowflake'
ON CONFLICT (service_id, name) DO NOTHING;

-- ── Doris: external catalog permissions ───────────────────
-- Two new permissions on the existing Doris service (id=1)
INSERT INTO permissions (service_id, name, description, metadata) VALUES
  (1, 'CATALOG_USAGE',
   'USE CATALOG — switch to an external catalog (Iceberg / JDBC) in Doris.',
   '{"resource_types":["external_catalog"]}'),
  (1, 'CATALOG_WARM_UP',
   'Trigger or schedule a cache warm-up on a Doris external catalog.',
   '{"resource_types":["external_catalog"]}')
ON CONFLICT (service_id, name) DO NOTHING;

-- ── New roles ──────────────────────────────────────────────

-- iceberg_engineer: full Spark job submission + full Polaris read/write
INSERT INTO roles (name, display_name, description) VALUES
  ('iceberg_engineer',
   'Iceberg Engineer',
   'Submit Spark jobs, read and write Iceberg tables via the Polaris REST catalog, '
   'access the Doris Iceberg external catalog with cache warm-up rights. '
   'Intended for data engineers who own the Iceberg lakehouse pipeline.')
ON CONFLICT (name) DO NOTHING;

INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name = 'iceberg_engineer'
  AND (
       -- Spark
       (p.service_id = (SELECT id FROM services WHERE name='spark')
        AND p.name IN ('SUBMIT_JOB','KILL_OWN_JOB','VIEW_UI',
                       'USE_CATALOG','WRITE_ICEBERG'))
    OR -- Polaris
       (p.service_id = (SELECT id FROM services WHERE name='polaris')
        AND p.name IN ('CATALOG_READ','TABLE_READ','TABLE_WRITE','REFRESH_SCHEDULE'))
    OR -- Doris external catalog
       (p.service_id = 1 AND p.name IN ('SELECT','CATALOG_USAGE','CATALOG_WARM_UP'))
  )
ON CONFLICT (role_id, permission_id) DO NOTHING;

-- snowflake_reader: read Snowflake tables through Doris JDBC catalog
INSERT INTO roles (name, display_name, description) VALUES
  ('snowflake_reader',
   'Snowflake Reader',
   'SELECT access to Snowflake proprietary tables via the Doris JDBC external catalog. '
   'Intended for analysts who need to join Snowflake data with Doris/Iceberg tables.')
ON CONFLICT (name) DO NOTHING;

INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name = 'snowflake_reader'
  AND (
       (p.service_id = (SELECT id FROM services WHERE name='snowflake')
        AND p.name = 'JDBC_SELECT')
    OR (p.service_id = 1 AND p.name IN ('SELECT','CATALOG_USAGE'))
  )
ON CONFLICT (role_id, permission_id) DO NOTHING;

-- ── Promote existing admin roles ──────────────────────────
-- platform_admin, account_admin, data_admin get all new permissions
INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name IN ('platform_admin','account_admin','data_admin')
  AND (
       p.service_id IN (
         (SELECT id FROM services WHERE name='polaris'),
         (SELECT id FROM services WHERE name='snowflake')
       )
    OR (p.service_id = 1 AND p.name IN ('CATALOG_USAGE','CATALOG_WARM_UP'))
  )
ON CONFLICT (role_id, permission_id) DO NOTHING;

-- data_engineer gains Iceberg read/write + Doris catalog access
INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name = 'data_engineer'
  AND (
       (p.service_id = (SELECT id FROM services WHERE name='polaris')
        AND p.name IN ('CATALOG_READ','TABLE_READ','TABLE_WRITE','REFRESH_SCHEDULE'))
    OR (p.service_id = 1 AND p.name IN ('CATALOG_USAGE','CATALOG_WARM_UP'))
  )
ON CONFLICT (role_id, permission_id) DO NOTHING;

-- analyst gains read-only Polaris + Snowflake JDBC SELECT
INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name = 'analyst'
  AND (
       (p.service_id = (SELECT id FROM services WHERE name='polaris')
        AND p.name IN ('CATALOG_READ','TABLE_READ'))
    OR (p.service_id = (SELECT id FROM services WHERE name='snowflake')
        AND p.name = 'JDBC_SELECT')
    OR (p.service_id = 1 AND p.name = 'CATALOG_USAGE')
  )
ON CONFLICT (role_id, permission_id) DO NOTHING;
