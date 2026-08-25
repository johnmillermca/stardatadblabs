-- ============================================================
-- Star Knowledge Catalog — Migration 002: Database-Level Governance Switch
--
-- Adds a governance_databases table that allows masking to be
-- enabled or disabled for an entire Doris database in one row.
-- When governance_enabled = false for a database:
--   • POST /api/v1/masking/apply  → skips all tables in that database
--   • POST /api/v1/masking/query  → routes ALL users to the base table
--     (as if every role held a masking exception)
--
-- This is the "circuit breaker" for emergency or maintenance windows.
-- ============================================================

CREATE TABLE IF NOT EXISTS governance_databases (
    id                  SERIAL PRIMARY KEY,
    doris_database      TEXT NOT NULL UNIQUE,
    governance_enabled  BOOLEAN NOT NULL DEFAULT true,
    -- Free-text reason recorded when governance is disabled
    disabled_reason     TEXT,
    disabled_by         TEXT,
    disabled_at         TIMESTAMPTZ,
    -- When governance was last re-enabled
    enabled_at          TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_gov_db_name
    ON governance_databases(doris_database);

-- Seed: governance_demo is enabled by default
INSERT INTO governance_databases (doris_database, governance_enabled)
VALUES ('governance_demo', true)
ON CONFLICT (doris_database) DO NOTHING;
