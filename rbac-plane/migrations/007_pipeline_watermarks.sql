-- ============================================================
-- RBAC Control Plane — Migration 007: Pipeline Watermarks
--
-- Creates a permanent pipeline_watermarks table in PostgreSQL.
-- This table is the authoritative cross-service sync point for
-- the Snowflake → Iceberg bulk copy ↔ Oracle CDC pipeline.
--
-- Why PostgreSQL (not only Iceberg)?
-- -----------------------------------
-- The Iceberg _pipeline_watermarks control table is written by
-- Spark and lives inside S3-backed Iceberg.  It is PERFECTLY
-- queryable from Spark but NOT easily queryable from a plain
-- shell script (e.g. the Debezium connector bootstrap script).
--
-- By dual-writing to this PostgreSQL table the Debezium startup
-- script can do a simple psql query to resolve the Oracle SCN
-- for each table WITHOUT spinning up a Spark session.  It is
-- also queryable from any CI/CD pipeline, monitoring tool, or
-- ad-hoc psql session.
--
-- Schema design
-- -------------
--   source_db        — Snowflake database  (e.g. SNOWFLAKE_SAMPLE_DATA)
--   source_schema    — Snowflake schema    (e.g. TPCDS_SF10TCL)
--   table_name       — lower-cased table name
--   sf_extraction_ts — ISO-8601 UTC Snowflake CURRENT_TIMESTAMP() captured
--                      immediately before the first batch SELECT for this
--                      table.  This is the CDC sync point.
--   oracle_start_scn — Oracle SCN derived from TIMESTAMP_TO_SCN(sf_extraction_ts)
--                      NULL until the Debezium bootstrap script has looked
--                      it up from Oracle and set it.
--   rows_copied      — total rows written by the Spark bulk-copy run
--   pipeline_run_ts  — wall-clock timestamp of the Spark driver at run time
--   iceberg_namespace— target Iceberg namespace (== lower(source_schema))
--   updated_at       — last upsert timestamp (auto-updated by trigger)
--
-- Upsert key: (source_db, source_schema, table_name)
-- Each new run overwrites the row so it always reflects the latest extraction.
-- ============================================================

CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    id               BIGSERIAL    PRIMARY KEY,
    source_db        TEXT         NOT NULL,
    source_schema    TEXT         NOT NULL,
    table_name       TEXT         NOT NULL,
    sf_extraction_ts TEXT         NOT NULL,       -- ISO-8601 UTC e.g. "2026-08-18T04:01:39.123456Z"
    oracle_start_scn BIGINT       DEFAULT NULL,   -- filled by Debezium bootstrap
    rows_copied      BIGINT       NOT NULL DEFAULT 0,
    pipeline_run_ts  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    iceberg_namespace TEXT        NOT NULL,
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (source_db, source_schema, table_name)
);

-- Auto-update updated_at on every write
CREATE OR REPLACE FUNCTION _set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$;

DROP TRIGGER IF EXISTS trg_pipeline_watermarks_updated_at ON pipeline_watermarks;
CREATE TRIGGER trg_pipeline_watermarks_updated_at
    BEFORE UPDATE ON pipeline_watermarks
    FOR EACH ROW EXECUTE FUNCTION _set_updated_at();

-- Grant read/write to the rbac service account
GRANT SELECT, INSERT, UPDATE, DELETE ON pipeline_watermarks TO rbac;
GRANT USAGE, SELECT ON SEQUENCE pipeline_watermarks_id_seq TO rbac;

-- Index for fast lookup by (source_db, source_schema, table_name)
CREATE INDEX IF NOT EXISTS idx_pw_source
    ON pipeline_watermarks (source_db, source_schema, table_name);

-- ── Verification ──────────────────────────────────────────────────────────────
-- SELECT source_db, source_schema, table_name,
--        sf_extraction_ts, oracle_start_scn, rows_copied, updated_at
-- FROM pipeline_watermarks
-- ORDER BY source_db, source_schema, table_name;
