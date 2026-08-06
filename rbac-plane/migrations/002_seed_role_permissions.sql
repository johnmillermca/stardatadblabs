-- Role-permission seeds for built-in roles
-- Run after 001_initial_schema.sql

-- analyst: SELECT on Doris, CONSUME on Kafka, INDEX_READ on OpenSearch, VIEW_UI on Spark
INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name = 'analyst'
  AND (
        (p.service_id = 1 AND p.name = 'SELECT')
     OR (p.service_id = 2 AND p.name IN ('CONSUME', 'DESCRIBE'))
     OR (p.service_id = 3 AND p.name IN ('INDEX_READ', 'CLUSTER_READ'))
     OR (p.service_id = 4 AND p.name = 'VIEW_UI')
  );

-- etl_writer: SELECT+INSERT+UPDATE on Doris, PRODUCE+CONSUME on Kafka
INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name = 'etl_writer'
  AND (
        (p.service_id = 1 AND p.name IN ('SELECT','INSERT','UPDATE','LOAD'))
     OR (p.service_id = 2 AND p.name IN ('PRODUCE','CONSUME','DESCRIBE'))
  );

-- spark_user: SUBMIT_JOB + KILL_OWN_JOB + VIEW_UI on Spark
INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name = 'spark_user'
  AND p.service_id = 4
  AND p.name IN ('SUBMIT_JOB', 'KILL_OWN_JOB', 'VIEW_UI');

-- data_admin: everything
INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name = 'data_admin';

-- kafka_consumer: CONSUME + DESCRIBE on Kafka
INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name = 'kafka_consumer'
  AND p.service_id = 2
  AND p.name IN ('CONSUME', 'DESCRIBE');
