-- =============================================================================
-- pg_create_testing_table.sql
--
-- Creates testing tables in the cache_testing.public schema, each with a
-- primary key and REPLICA IDENTITY FULL (PostgreSQL's equivalent of
-- supplemental logging for all columns), and registers them with the
-- Debezium logical-replication publication (dbz_publication).
--
-- Usage:
--   kubectl -n prod exec -i postgresql-0 -- \
--     psql -U postgres -d cache_testing -v ON_ERROR_STOP=1 \
--     < scripts/pg_create_testing_table.sql
-- =============================================================================

\connect cache_testing

-- ── 1. Create table ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.testing (
    id              SERIAL          PRIMARY KEY,
    name            TEXT            NOT NULL,
    description     TEXT,
    status          TEXT            NOT NULL DEFAULT 'active',
    payload         JSONB,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ── 2. Enable supplemental logging for all columns ───────────────────────────
-- REPLICA IDENTITY FULL causes PostgreSQL to write the old values of every
-- column (not just the PK) into the WAL on UPDATE and DELETE — equivalent to
-- Oracle's SUPPLEMENTAL LOG DATA (ALL COLUMNS).
ALTER TABLE public.testing REPLICA IDENTITY FULL;

-- ── 3. Add to Debezium publication ───────────────────────────────────────────
-- Ensures the table is tracked by the logical-replication publication that the
-- Debezium PostgreSQL connector subscribes to (publication.name=dbz_publication
-- in the connector config).  IF NOT EXISTS keeps the statement idempotent.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication'
    ) THEN
        CREATE PUBLICATION dbz_publication FOR TABLE public.testing;
    ELSE
        -- Add only if not already a member of the publication.
        IF NOT EXISTS (
            SELECT 1
            FROM   pg_publication_tables
            WHERE  pubname   = 'dbz_publication'
              AND  schemaname = 'public'
              AND  tablename  = 'testing'
        ) THEN
            ALTER PUBLICATION dbz_publication ADD TABLE public.testing;
        END IF;
    END IF;
END $$;

-- ── 4. Verify ─────────────────────────────────────────────────────────────────
SELECT
    c.relname                                   AS table_name,
    c.relreplident                              AS replica_identity,   -- 'f' = FULL
    (SELECT string_agg(a.attname, ', ' ORDER BY a.attnum)
     FROM pg_attribute a
     WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
    )                                           AS columns
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relname = 'testing';


-- =============================================================================
-- Table: public.testing5
-- =============================================================================

-- ── 1. Create table ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.testing5 (
    id              SERIAL          PRIMARY KEY,
    name            TEXT            NOT NULL,
    description     TEXT,
    status          TEXT            NOT NULL DEFAULT 'active',
    payload         JSONB,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ── 2. Enable supplemental logging for all columns ───────────────────────────
ALTER TABLE public.testing5 REPLICA IDENTITY FULL;

-- ── 3. Add to Debezium publication ───────────────────────────────────────────
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication'
    ) THEN
        CREATE PUBLICATION dbz_publication FOR TABLE public.testing5;
    ELSE
        IF NOT EXISTS (
            SELECT 1
            FROM   pg_publication_tables
            WHERE  pubname    = 'dbz_publication'
              AND  schemaname = 'public'
              AND  tablename  = 'testing5'
        ) THEN
            ALTER PUBLICATION dbz_publication ADD TABLE public.testing5;
        END IF;
    END IF;
END $$;

-- ── 4. Verify ─────────────────────────────────────────────────────────────────
SELECT
    c.relname                                   AS table_name,
    c.relreplident                              AS replica_identity,   -- 'f' = FULL
    (SELECT string_agg(a.attname, ', ' ORDER BY a.attnum)
     FROM pg_attribute a
     WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
    )                                           AS columns
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relname = 'testing5';

-- ── 5. Confirm publication membership ────────────────────────────────────────
SELECT pubname, schemaname, tablename
FROM   pg_publication_tables
WHERE  pubname = 'dbz_publication'
ORDER  BY tablename;
