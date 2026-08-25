-- ============================================================
-- RBAC Control Plane — Migration 008: Data Governance
-- Adds an IBM Knowledge Catalog-style governance layer:
--
--   data_classifications  — sensitivity tiers (PII, PCI, PHI, etc.)
--   glossary_terms        — business glossary (matches IBM WKC term concept)
--   column_tags           — maps DB columns to glossary terms + classifications
--   masking_algorithms    — named masking functions (REDACT, HASH, PARTIAL, etc.)
--   masking_policies      — binds (classification OR glossary_term) → algorithm
--   role_masking_exceptions — roles that receive CLEAR (unmasked) data
--
-- Runtime views and procedures are created in Doris by the
-- governance_engine.py service.  This migration manages only the
-- metadata / control-plane side in PostgreSQL.
-- ============================================================

-- ── Data Classifications ─────────────────────────────────────
-- Mirrors IBM Knowledge Catalog data-class concept.
-- A classification represents a sensitivity category.

CREATE TABLE data_classifications (
    id            SERIAL PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE,        -- e.g. "PII", "PCI", "PHI"
    display_name  TEXT NOT NULL,
    description   TEXT,
    sensitivity   TEXT NOT NULL DEFAULT 'medium'
                    CHECK (sensitivity IN ('low','medium','high','critical')),
    color_hex     TEXT,                        -- UI hint, e.g. '#FF4444'
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Glossary Terms ───────────────────────────────────────────
-- IBM WKC-style business glossary.  A term describes what a column
-- represents in business language and links to a classification.

CREATE TABLE glossary_terms (
    id                  SERIAL PRIMARY KEY,
    name                TEXT NOT NULL UNIQUE,   -- e.g. "email_address"
    display_name        TEXT NOT NULL,           -- "Email Address"
    description         TEXT,
    classification_id   INT REFERENCES data_classifications(id) ON DELETE SET NULL,
    -- Keyword patterns used for auto-detection on column names
    -- e.g. ["email","e_mail","email_addr"]
    column_name_patterns TEXT[] NOT NULL DEFAULT '{}',
    -- Keyword patterns matched against column comments / descriptions
    description_patterns TEXT[] NOT NULL DEFAULT '{}',
    steward             TEXT,                   -- data steward username
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Column Tags ──────────────────────────────────────────────
-- Maps a specific DB column (database.table.column in Doris) to
-- a glossary term and/or classification.
-- Populated automatically by the governance_engine auto-classify scan
-- and can be curated manually.

CREATE TABLE column_tags (
    id                  SERIAL PRIMARY KEY,
    doris_database      TEXT NOT NULL,          -- Doris database name
    doris_table         TEXT NOT NULL,          -- Doris table name
    column_name         TEXT NOT NULL,          -- Doris column name
    glossary_term_id    INT REFERENCES glossary_terms(id) ON DELETE SET NULL,
    classification_id   INT REFERENCES data_classifications(id) ON DELETE SET NULL,
    auto_detected       BOOLEAN NOT NULL DEFAULT false,
    detection_score     NUMERIC(3,2),           -- 0.00–1.00 confidence
    override_reason     TEXT,                   -- human note when manually set
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (doris_database, doris_table, column_name)
);

-- ── Masking Algorithms ───────────────────────────────────────
-- Each algorithm describes HOW to mask a value.
-- The doris_expression is a Doris SQL expression template using
-- {col} as the column placeholder.

CREATE TABLE masking_algorithms (
    id               SERIAL PRIMARY KEY,
    name             TEXT NOT NULL UNIQUE,      -- e.g. "FULL_REDACT"
    display_name     TEXT NOT NULL,
    description      TEXT,
    algorithm_type   TEXT NOT NULL
                       CHECK (algorithm_type IN (
                           'REDACT','HASH','PARTIAL_MASK',
                           'TOKENIZE','DATE_GENERALIZE','NULL_OUT','CUSTOM'
                       )),
    -- Doris SQL expression with {col} placeholder, e.g.:
    --   REDACT:          '''****'''
    --   HASH:            SHA2({col}, 256)
    --   PARTIAL_MASK:    CONCAT(LEFT({col},2), REPEAT('*', GREATEST(0,LENGTH({col})-4)), RIGHT({col},2))
    --   DATE_GENERALIZE: DATE_FORMAT({col},'%Y-01-01')
    --   NULL_OUT:        NULL
    doris_expression TEXT NOT NULL,
    -- Applicable SQL types (advisory; engine checks column type)
    applicable_types TEXT[] NOT NULL DEFAULT '{"VARCHAR","TEXT","STRING"}',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Masking Policies ─────────────────────────────────────────
-- Binds a classification (or glossary term) to a masking algorithm.
-- Classification-level policy overrides term-level if both apply.

CREATE TABLE masking_policies (
    id                   SERIAL PRIMARY KEY,
    name                 TEXT NOT NULL UNIQUE,
    description          TEXT,
    -- Exactly one of classification_id or glossary_term_id must be set
    classification_id    INT REFERENCES data_classifications(id) ON DELETE CASCADE,
    glossary_term_id     INT REFERENCES glossary_terms(id) ON DELETE CASCADE,
    algorithm_id         INT NOT NULL REFERENCES masking_algorithms(id),
    -- Priority: higher number wins when multiple policies match a column
    priority             INT NOT NULL DEFAULT 100,
    enabled              BOOLEAN NOT NULL DEFAULT true,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        (classification_id IS NOT NULL AND glossary_term_id IS NULL) OR
        (classification_id IS NULL     AND glossary_term_id IS NOT NULL)
    )
);

-- ── Role Masking Exceptions ───────────────────────────────────
-- Roles listed here receive CLEAR (unmasked) data for the given
-- classification.  All other roles get the masked version.
-- This is the "privileged access" / data steward bypass.

CREATE TABLE role_masking_exceptions (
    id                  SERIAL PRIMARY KEY,
    role_id             INT NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    classification_id   INT NOT NULL REFERENCES data_classifications(id) ON DELETE CASCADE,
    granted_by          TEXT NOT NULL DEFAULT 'system',
    granted_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (role_id, classification_id)
);

-- ── Doris View Manifest ───────────────────────────────────────
-- Tracks which masked views have been materialised in Doris so the
-- engine can detect drift and re-apply without full re-scan.

CREATE TABLE doris_masked_views (
    id               SERIAL PRIMARY KEY,
    doris_database   TEXT NOT NULL,
    base_table       TEXT NOT NULL,
    view_name        TEXT NOT NULL,            -- e.g. "customers_masked"
    view_ddl         TEXT NOT NULL,            -- last applied CREATE OR REPLACE
    columns_masked   TEXT[] NOT NULL DEFAULT '{}',
    view_checksum    TEXT,                     -- MD5 of view_ddl for drift detection
    last_applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (doris_database, base_table)
);

-- ── Indexes ──────────────────────────────────────────────────
CREATE INDEX idx_column_tags_db_table
    ON column_tags(doris_database, doris_table);
CREATE INDEX idx_column_tags_term
    ON column_tags(glossary_term_id);
CREATE INDEX idx_column_tags_class
    ON column_tags(classification_id);
CREATE INDEX idx_masking_policies_class
    ON masking_policies(classification_id) WHERE enabled;
CREATE INDEX idx_masking_policies_term
    ON masking_policies(glossary_term_id) WHERE enabled;
CREATE INDEX idx_role_masking_exceptions_role
    ON role_masking_exceptions(role_id);

-- ── RBAC: new governance permissions ─────────────────────────
INSERT INTO permissions (service_id, name, description, metadata)
SELECT id, 'MASKED_SELECT',
       'SELECT access to masked views — sensitive columns are automatically '
       'masked according to active masking policies. Intended for analyst '
       'and consumer roles.',
       '{"resource_types":["masked_view"]}'
FROM services WHERE name = 'doris'
ON CONFLICT (service_id, name) DO NOTHING;

INSERT INTO permissions (service_id, name, description, metadata)
SELECT id, 'CLEAR_SELECT',
       'SELECT access to base (unmasked) tables. Granted only to privileged '
       'roles with an approved masking exception.',
       '{"resource_types":["table"]}'
FROM services WHERE name = 'doris'
ON CONFLICT (service_id, name) DO NOTHING;

INSERT INTO permissions (service_id, name, description, metadata)
SELECT id, 'GOVERNANCE_ADMIN',
       'Administer data classifications, glossary terms, masking algorithms '
       'and policies in the RBAC governance layer.',
       '{"resource_types":["governance"]}'
FROM services WHERE name = 'doris'
ON CONFLICT (service_id, name) DO NOTHING;

-- analyst role → gets MASKED_SELECT (not CLEAR_SELECT)
INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{"database":"governance_demo","view":"*"}'::jsonb
FROM roles r, permissions p
WHERE r.name = 'analyst'
  AND p.name = 'MASKED_SELECT'
  AND p.service_id = (SELECT id FROM services WHERE name='doris')
ON CONFLICT (role_id, permission_id) DO NOTHING;

-- data_admin gets CLEAR_SELECT + GOVERNANCE_ADMIN
INSERT INTO role_permissions (role_id, permission_id, resource_scope)
SELECT r.id, p.id, '{}'::jsonb
FROM roles r, permissions p
WHERE r.name IN ('data_admin','platform_admin','account_admin')
  AND p.name IN ('CLEAR_SELECT','GOVERNANCE_ADMIN','MASKED_SELECT')
  AND p.service_id = (SELECT id FROM services WHERE name='doris')
ON CONFLICT (role_id, permission_id) DO NOTHING;

-- ── Seed: Data Classifications ───────────────────────────────
INSERT INTO data_classifications (name, display_name, description, sensitivity, color_hex) VALUES
  ('PII',     'Personally Identifiable Information',
   'Data that can identify a natural person directly or indirectly. '
   'Regulated under GDPR, CCPA, and similar frameworks.',
   'high', '#FF4444'),
  ('PCI',     'Payment Card Industry Data',
   'Payment card numbers, CVVs, and cardholder data governed by PCI-DSS.',
   'critical', '#CC0000'),
  ('PHI',     'Protected Health Information',
   'Health-related data governed by HIPAA and related regulations.',
   'critical', '#990000'),
  ('CONFIDENTIAL', 'Confidential Business Data',
   'Internal business data not for public disclosure.',
   'medium', '#FF8C00'),
  ('PUBLIC',  'Public Data',
   'Data explicitly cleared for public access with no restrictions.',
   'low', '#22AA44')
ON CONFLICT (name) DO NOTHING;

-- ── Seed: Glossary Terms ─────────────────────────────────────
INSERT INTO glossary_terms
    (name, display_name, description, classification_id,
     column_name_patterns, description_patterns, steward)
SELECT
    t.name, t.display_name, t.description,
    c.id,
    t.col_patterns::TEXT[],
    t.desc_patterns::TEXT[],
    'data_steward'
FROM (VALUES
    ('email_address',  'Email Address',
     'Electronic mail address of a person.',
     'PII',
     ARRAY['email','e_mail','email_addr','emailaddress','user_email'],
     ARRAY['email','e-mail','electronic mail']),

    ('full_name',      'Full Name',
     'Full given name and/or surname of a person.',
     'PII',
     ARRAY['full_name','fullname','name','first_name','last_name','firstname','lastname','given_name','surname'],
     ARRAY['name','person','first','last','given','surname']),

    ('phone_number',   'Phone Number',
     'Telephone number of a person or business.',
     'PII',
     ARRAY['phone','phone_number','phonenumber','mobile','telephone','tel','phone_no'],
     ARRAY['phone','telephone','mobile','contact number']),

    ('date_of_birth',  'Date of Birth',
     'Biological birth date of a person.',
     'PII',
     ARRAY['dob','date_of_birth','birth_date','birthdate','born_on'],
     ARRAY['birth','date of birth','born']),

    ('national_id',    'National Identifier',
     'Government-issued national identifier (SSN, NIN, etc.).',
     'PII',
     ARRAY['ssn','national_id','nin','tax_id','passport','id_number','gov_id'],
     ARRAY['ssn','national','passport','government id','tax id']),

    ('credit_card_number', 'Credit Card Number',
     'Primary Account Number (PAN) of a payment card.',
     'PCI',
     ARRAY['card_number','credit_card','cc_number','pan','card_no','creditcard'],
     ARRAY['credit card','card number','pan','payment card']),

    ('credit_card_cvv', 'Credit Card CVV',
     'Card Verification Value security code.',
     'PCI',
     ARRAY['cvv','cvc','cvv2','security_code','card_cvv'],
     ARRAY['cvv','cvc','security code']),

    ('ip_address',     'IP Address',
     'Network IP address which may indirectly identify a person.',
     'PII',
     ARRAY['ip','ip_address','ipaddress','remote_ip','client_ip','source_ip'],
     ARRAY['ip address','network address','client ip']),

    ('street_address', 'Street Address',
     'Physical street address of a person.',
     'PII',
     ARRAY['address','street_address','addr','street','mailing_address','home_address'],
     ARRAY['address','street','mailing','home address']),

    ('salary',         'Salary',
     'Compensation amount paid to an employee.',
     'CONFIDENTIAL',
     ARRAY['salary','compensation','pay','wage','ctc','annual_pay'],
     ARRAY['salary','compensation','wage','pay'])
) AS t(name,display_name,description,class_name,col_patterns,desc_patterns)
JOIN data_classifications c ON c.name = t.class_name
ON CONFLICT (name) DO NOTHING;

-- ── Seed: Masking Algorithms ─────────────────────────────────
INSERT INTO masking_algorithms
    (name, display_name, description, algorithm_type, doris_expression, applicable_types)
VALUES
  ('FULL_REDACT',
   'Full Redaction',
   'Replaces the entire value with static asterisks. Suitable for high-sensitivity fields.',
   'REDACT',
   '''****''',
   ARRAY['VARCHAR','TEXT','STRING','CHAR']),

  ('SHA256_HASH',
   'SHA-256 Hash',
   'One-way cryptographic hash. Preserves uniqueness; cannot be reversed without the original value.',
   'HASH',
   'SHA2({col}, 256)',
   ARRAY['VARCHAR','TEXT','STRING','CHAR']),

  ('EMAIL_PARTIAL',
   'Email Partial Mask',
   'Reveals first 2 chars and domain; masks the rest. e.g. jo***@example.com',
   'PARTIAL_MASK',
   'CONCAT(LEFT({col},2), REPEAT(''*'',GREATEST(0,LOCATE(''@'',{col})-3)), SUBSTRING({col},LOCATE(''@'',{col})))',
   ARRAY['VARCHAR','TEXT','STRING']),

  ('CREDIT_CARD_MASK',
   'Credit Card Mask (Last 4)',
   'Shows only the last 4 digits preceded by asterisks. e.g. ****-****-****-1234',
   'PARTIAL_MASK',
   'CONCAT(REPEAT(''*'',GREATEST(0,LENGTH({col})-4)), RIGHT({col},4))',
   ARRAY['VARCHAR','TEXT','STRING','CHAR']),

  ('DATE_YEAR_ONLY',
   'Date Year Generalisation',
   'Generalises a date to the first day of the year. e.g. 1985-07-22 → 1985-01-01',
   'DATE_GENERALIZE',
   'DATE_FORMAT({col},''%Y-01-01'')',
   ARRAY['DATE','DATETIME','VARCHAR']),

  ('PHONE_PARTIAL',
   'Phone Partial Mask',
   'Reveals last 4 digits; masks remainder. e.g. ***-***-1234',
   'PARTIAL_MASK',
   'CONCAT(REPEAT(''*'',GREATEST(0,LENGTH({col})-4)), RIGHT({col},4))',
   ARRAY['VARCHAR','TEXT','STRING','CHAR']),

  ('NULL_OUT',
   'Null Out',
   'Replaces value with SQL NULL. Use when absence is preferable to masked value.',
   'NULL_OUT',
   'NULL',
   ARRAY['VARCHAR','TEXT','STRING','INT','BIGINT','DATE','DATETIME']),

  ('IP_PARTIAL',
   'IP Address Partial Mask',
   'Masks last octet of IPv4 address. e.g. 192.168.1.0',
   'PARTIAL_MASK',
   'CONCAT(SUBSTRING_INDEX({col},''.'',3),''.0'')',
   ARRAY['VARCHAR','TEXT','STRING'])

ON CONFLICT (name) DO NOTHING;

-- ── Seed: Masking Policies ───────────────────────────────────
-- Classification-level policies (catch-all when term not matched)
INSERT INTO masking_policies (name, description, classification_id, algorithm_id, priority)
SELECT
    'policy_' || lower(c.name) || '_default',
    'Default masking policy for ' || c.display_name || ' classification.',
    c.id,
    a.id,
    100
FROM data_classifications c
JOIN masking_algorithms a ON a.name = (
    CASE c.name
        WHEN 'PII'         THEN 'SHA256_HASH'
        WHEN 'PCI'         THEN 'FULL_REDACT'
        WHEN 'PHI'         THEN 'FULL_REDACT'
        WHEN 'CONFIDENTIAL'THEN 'FULL_REDACT'
        WHEN 'PUBLIC'      THEN 'NULL_OUT'   -- no masking needed; use CLEAR_SELECT
    END
)
WHERE c.name != 'PUBLIC'
ON CONFLICT (name) DO NOTHING;

-- Term-level policies (higher priority, more specific)
INSERT INTO masking_policies (name, description, glossary_term_id, algorithm_id, priority)
SELECT
    'policy_term_' || lower(replace(gt.name,' ','_')),
    'Masking policy for glossary term: ' || gt.display_name,
    gt.id,
    a.id,
    200
FROM glossary_terms gt
JOIN masking_algorithms a ON a.name = (
    CASE gt.name
        WHEN 'email_address'       THEN 'EMAIL_PARTIAL'
        WHEN 'full_name'           THEN 'SHA256_HASH'
        WHEN 'phone_number'        THEN 'PHONE_PARTIAL'
        WHEN 'date_of_birth'       THEN 'DATE_YEAR_ONLY'
        WHEN 'national_id'         THEN 'FULL_REDACT'
        WHEN 'credit_card_number'  THEN 'CREDIT_CARD_MASK'
        WHEN 'credit_card_cvv'     THEN 'FULL_REDACT'
        WHEN 'ip_address'          THEN 'IP_PARTIAL'
        WHEN 'street_address'      THEN 'SHA256_HASH'
        WHEN 'salary'              THEN 'FULL_REDACT'
    END
)
ON CONFLICT (name) DO NOTHING;

-- ── Seed: Role masking exceptions ────────────────────────────
-- data_admin, platform_admin, account_admin can see unmasked PII/PCI/PHI
INSERT INTO role_masking_exceptions (role_id, classification_id, granted_by)
SELECT r.id, c.id, 'migration_008'
FROM roles r, data_classifications c
WHERE r.name IN ('data_admin','platform_admin','account_admin')
  AND c.name IN ('PII','PCI','PHI','CONFIDENTIAL')
ON CONFLICT (role_id, classification_id) DO NOTHING;
