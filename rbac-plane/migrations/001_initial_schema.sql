-- ============================================================
-- RBAC Control Plane — Initial Schema
-- Database: rbac (PostgreSQL 17)
-- ============================================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";   -- fast name search

-- ── Service registry ──────────────────────────────────────
-- Which services are managed (doris / kafka / opensearch / spark)
CREATE TABLE services (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,           -- e.g. "doris"
    display_name TEXT NOT NULL,
    description TEXT,
    enabled     BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Permissions ───────────────────────────────────────────
-- Logical permission tokens per service (e.g. doris.SELECT, kafka.CONSUME)
CREATE TABLE permissions (
    id          SERIAL PRIMARY KEY,
    service_id  INT NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,                  -- e.g. "SELECT"
    description TEXT,
    metadata    JSONB NOT NULL DEFAULT '{}',    -- e.g. {"resource_type": "table"}
    UNIQUE (service_id, name)
);

-- ── Roles ─────────────────────────────────────────────────
-- A role is a named set of permissions, scoped to one or more services
CREATE TABLE roles (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,           -- e.g. "analyst", "etl_writer"
    display_name TEXT NOT NULL,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Many-to-many: role → permission (with optional resource scope)
CREATE TABLE role_permissions (
    role_id       INT NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    permission_id INT NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,
    -- Optional resource scope — meaning depends on service
    -- doris:   {"database":"sales","table":"*"}
    -- kafka:   {"topic":"orders-*"}
    -- opensearch: {"index":"logs-*"}
    -- spark:   {"queue":"default"}
    resource_scope JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (role_id, permission_id)
);

-- ── Users ─────────────────────────────────────────────────
CREATE TABLE users (
    id          SERIAL PRIMARY KEY,
    username    TEXT NOT NULL UNIQUE,           -- matches KDC principal short name
    display_name TEXT,
    email       TEXT,
    enabled     BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Role bindings ─────────────────────────────────────────
-- Assign a role to a user; optionally scoped to specific services
CREATE TABLE role_bindings (
    id          SERIAL PRIMARY KEY,
    user_id     INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role_id     INT NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    -- If NULL → binding applies to ALL services in the role
    -- If set  → binding applies to this service only
    service_id  INT REFERENCES services(id) ON DELETE CASCADE,
    granted_by  TEXT NOT NULL DEFAULT 'system',
    granted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ,                    -- NULL = never expires
    UNIQUE (user_id, role_id, service_id)
);

-- ── Sync state ────────────────────────────────────────────
-- Track last successful sync per user per service
-- Used for reconciliation (avoid re-applying unchanged state)
CREATE TABLE sync_state (
    user_id     INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    service_id  INT NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    role_hash   TEXT NOT NULL,                  -- SHA256 of sorted role names at sync time
    synced_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, service_id)
);

-- ── Audit log ─────────────────────────────────────────────
CREATE TABLE audit_log (
    id          BIGSERIAL PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor       TEXT NOT NULL,                  -- who performed the action
    action      TEXT NOT NULL,                  -- CREATE_USER, BIND_ROLE, SYNC, etc.
    target_type TEXT,                           -- "user", "role", "binding"
    target_id   TEXT,
    detail      JSONB NOT NULL DEFAULT '{}',
    ip_address  INET
);

-- ── API tokens ────────────────────────────────────────────
-- Hashed API tokens for rbacctl and programmatic access
CREATE TABLE api_tokens (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    token_hash  TEXT NOT NULL UNIQUE,           -- SHA256 hex of the raw token
    scopes      TEXT[] NOT NULL DEFAULT '{}',   -- e.g. '{"read","write","admin"}'
    created_by  TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ,
    expires_at  TIMESTAMPTZ,
    revoked     BOOLEAN NOT NULL DEFAULT false
);

-- ── Indexes ───────────────────────────────────────────────
CREATE INDEX idx_role_bindings_user   ON role_bindings(user_id);
CREATE INDEX idx_role_bindings_role   ON role_bindings(role_id);
CREATE INDEX idx_role_bindings_svc    ON role_bindings(service_id);
CREATE INDEX idx_audit_ts             ON audit_log(ts DESC);
CREATE INDEX idx_audit_actor          ON audit_log(actor);
CREATE INDEX idx_users_username       ON users USING GIN (username gin_trgm_ops);
CREATE INDEX idx_roles_name           ON roles USING GIN (name gin_trgm_ops);

-- ── Seed: service definitions ─────────────────────────────
INSERT INTO services (name, display_name, description) VALUES
  ('doris',       'Apache Doris',      'MPP analytical SQL database — native GRANT/REVOKE'),
  ('kafka',       'Apache Kafka',      'Strimzi-managed broker — KafkaUser CR + ACLs'),
  ('opensearch',  'Apache OpenSearch', 'Search engine — Security REST API roles/users'),
  ('spark',       'Apache Spark',      'Standalone cluster — allowlist ConfigMap via krb-spark-guard');

-- ── Seed: permissions per service ─────────────────────────
-- Doris permissions
INSERT INTO permissions (service_id, name, description, metadata) VALUES
  (1, 'SELECT',       'Read rows from tables',           '{"resource_types":["table","view"]}'),
  (1, 'INSERT',       'Insert rows',                     '{"resource_types":["table"]}'),
  (1, 'UPDATE',       'Update rows',                     '{"resource_types":["table"]}'),
  (1, 'DELETE',       'Delete rows',                     '{"resource_types":["table"]}'),
  (1, 'CREATE',       'Create tables/databases',         '{"resource_types":["database","table"]}'),
  (1, 'DROP',         'Drop tables/databases',           '{"resource_types":["database","table"]}'),
  (1, 'ALTER',        'Alter table schema',              '{"resource_types":["table"]}'),
  (1, 'LOAD',         'Load data (STREAM LOAD / ROUTINE LOAD)', '{"resource_types":["table"]}'),
  (1, 'GRANT',        'Re-grant privileges to others',   '{"resource_types":["global"]}'),
  (1, 'ADMIN',        'Full Doris admin rights',         '{"resource_types":["global"]}');

-- Kafka permissions
INSERT INTO permissions (service_id, name, description, metadata) VALUES
  (2, 'PRODUCE',      'Write messages to topics',        '{"resource_types":["topic"]}'),
  (2, 'CONSUME',      'Read messages from topics',       '{"resource_types":["topic"]}'),
  (2, 'CREATE_TOPIC', 'Create Kafka topics',             '{"resource_types":["topic","cluster"]}'),
  (2, 'DELETE_TOPIC', 'Delete Kafka topics',             '{"resource_types":["topic"]}'),
  (2, 'DESCRIBE',     'Describe topics / cluster meta',  '{"resource_types":["topic","cluster"]}'),
  (2, 'ADMIN',        'Full Kafka admin (all resources)','{"resource_types":["cluster"]}');

-- OpenSearch permissions
INSERT INTO permissions (service_id, name, description, metadata) VALUES
  (3, 'INDEX_READ',   'Read (search/get) index documents','{"resource_types":["index"]}'),
  (3, 'INDEX_WRITE',  'Write/index documents',            '{"resource_types":["index"]}'),
  (3, 'INDEX_ADMIN',  'Create/delete/manage indexes',     '{"resource_types":["index"]}'),
  (3, 'CLUSTER_READ', 'Read cluster metadata & health',   '{"resource_types":["cluster"]}'),
  (3, 'CLUSTER_ADMIN','Full cluster administration',      '{"resource_types":["cluster"]}');

-- Spark permissions
INSERT INTO permissions (service_id, name, description, metadata) VALUES
  (4, 'SUBMIT_JOB',   'Submit Spark jobs to the cluster','{"resource_types":["cluster"]}'),
  (4, 'KILL_OWN_JOB', 'Kill own running jobs',           '{"resource_types":["application"]}'),
  (4, 'KILL_ANY_JOB', 'Kill any running job (operator)', '{"resource_types":["application"]}'),
  (4, 'VIEW_UI',      'Access Spark Master Web UI',      '{"resource_types":["cluster"]}');

-- ── Seed: built-in roles ──────────────────────────────────
-- analyst: read-only across all services
INSERT INTO roles (name, display_name, description) VALUES
  ('analyst',    'Analyst',     'Read-only access to Doris, OpenSearch and Kafka topics'),
  ('etl_writer', 'ETL Writer',  'Write access to Doris tables and Kafka produce'),
  ('spark_user', 'Spark User',  'Can submit and kill own Spark jobs'),
  ('data_admin', 'Data Admin',  'Full admin on all services'),
  ('kafka_consumer', 'Kafka Consumer', 'Consume messages from Kafka topics');
