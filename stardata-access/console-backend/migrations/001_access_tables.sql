-- =============================================================================
-- 001_access_tables.sql
-- Apply with: psql $CONSOLE_DB_DSN -f migrations/001_access_tables.sql
-- Console-backend database schema for access control state.
-- Run by the console-backend init job (or a manual migration step) before
-- the deployment is started.
--
-- Tables:
--   approval          — two-person rule for Owner / Admin platform role changes
--   app_identity      — machine-user app registrations (name, owner, credential)
--   invite            — pending invitations sent by Owners / Admins
--   approval_id_seq   — sequence backing the "A-NNNN" approval IDs
-- =============================================================================

CREATE SEQUENCE IF NOT EXISTS approval_id_seq START 1;

-- ── approval ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS approval (
    id              TEXT        PRIMARY KEY,              -- e.g. "A-1029"
    action          TEXT        NOT NULL,                 -- set_platform_role | remove_platform_role
    target_user     TEXT        NOT NULL,                 -- ZITADEL user ID
    target_name     TEXT        NOT NULL,
    payload         JSONB       NOT NULL,                 -- {"from":[...],"to":[...]}
    reason          TEXT,
    requested_by    TEXT        NOT NULL,                 -- ZITADEL user ID
    requested_name  TEXT        NOT NULL,
    requested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL,
    state           TEXT        NOT NULL DEFAULT 'pending', -- pending|approved|rejected|expired
    decided_by      TEXT,
    decided_name    TEXT,
    decided_at      TIMESTAMPTZ,
    CONSTRAINT no_self_approve CHECK (decided_by IS NULL OR decided_by <> requested_by)
);

CREATE INDEX IF NOT EXISTS approval_state_idx     ON approval (state);
CREATE INDEX IF NOT EXISTS approval_expires_idx   ON approval (expires_at) WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS approval_target_idx    ON approval (target_user);

-- ── app_identity ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS app_identity (
    user_id         TEXT        PRIMARY KEY,              -- ZITADEL machine user ID
    name            TEXT        UNIQUE NOT NULL,          -- e.g. "orders-service"
    description     TEXT,
    owner_user_id   TEXT        NOT NULL,
    owner_name      TEXT        NOT NULL,
    credential_type TEXT        NOT NULL,                 -- client_secret|jwt_key|client_cert|app_password
    expires_at      TIMESTAMPTZ,
    rotation_due    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS app_identity_owner_idx   ON app_identity (owner_user_id);
CREATE INDEX IF NOT EXISTS app_identity_expiry_idx  ON app_identity (expires_at) WHERE expires_at IS NOT NULL;

-- ── invite ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS invite (
    user_id         TEXT        PRIMARY KEY,              -- ZITADEL user ID (set after creation)
    email           TEXT        NOT NULL,
    first_name      TEXT        NOT NULL,
    last_name       TEXT        NOT NULL,
    invited_by      TEXT        NOT NULL,                 -- ZITADEL user ID
    invited_by_name TEXT        NOT NULL,
    levels          JSONB       NOT NULL DEFAULT '{}',    -- {"doris":"analyst","kafka":"viewer"}
    platform        TEXT        NOT NULL DEFAULT 'member',
    require_mfa     BOOLEAN     NOT NULL DEFAULT false,
    sent_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    accepted_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS invite_email_idx ON invite (email);

-- ── audit_log — console-local audit events (mirrors platform_audit schema) ────
-- This table holds events written by the console backend.
-- The full platform_audit is in Doris; this is the console-local copy used for
-- the member history card until the audit-forwarder is wired up.
CREATE TABLE IF NOT EXISTS audit_log (
    event_id    TEXT        PRIMARY KEY DEFAULT gen_random_uuid()::text,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    source      TEXT        NOT NULL DEFAULT 'console',
    actor_id    TEXT,
    actor_name  TEXT,
    target_type TEXT,
    target_id   TEXT,
    target_name TEXT,
    component   TEXT,
    action      TEXT        NOT NULL,
    before_val  JSONB,
    after_val   JSONB,
    result      TEXT        NOT NULL DEFAULT 'ok',
    reason      TEXT,
    ip          TEXT,
    request_id  TEXT
);

CREATE INDEX IF NOT EXISTS audit_log_actor_idx  ON audit_log (actor_id);
CREATE INDEX IF NOT EXISTS audit_log_target_idx ON audit_log (target_id);
CREATE INDEX IF NOT EXISTS audit_log_ts_idx     ON audit_log (ts DESC);

-- ── approval amendments (idempotent) ─────────────────────────────────────────
ALTER TABLE approval ADD COLUMN IF NOT EXISTS reminder_sent_at TIMESTAMPTZ;
