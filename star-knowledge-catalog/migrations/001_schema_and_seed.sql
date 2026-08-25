-- ============================================================
-- Star Knowledge Catalog — PostgreSQL Schema Migration
-- Database: star_catalog
-- Run once against a fresh database or idempotently via IF NOT EXISTS guards.
-- ============================================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- ── Data Classifications ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data_classifications (
    id            SERIAL PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE,
    display_name  TEXT NOT NULL,
    description   TEXT,
    sensitivity   TEXT NOT NULL DEFAULT 'medium'
                    CHECK (sensitivity IN ('low','medium','high','critical')),
    color_hex     TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Glossary Terms ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS glossary_terms (
    id                   SERIAL PRIMARY KEY,
    name                 TEXT NOT NULL UNIQUE,
    display_name         TEXT NOT NULL,
    description          TEXT,
    classification_id    INT REFERENCES data_classifications(id) ON DELETE SET NULL,
    column_name_patterns TEXT[] NOT NULL DEFAULT '{}',
    description_patterns TEXT[] NOT NULL DEFAULT '{}',
    negative_patterns    TEXT[] NOT NULL DEFAULT '{}',
    steward              TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Masking Algorithms ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS masking_algorithms (
    id               SERIAL PRIMARY KEY,
    name             TEXT NOT NULL UNIQUE,
    display_name     TEXT NOT NULL,
    description      TEXT,
    algorithm_type   TEXT NOT NULL
                       CHECK (algorithm_type IN (
                           'REDACT','HASH','PARTIAL_MASK','TOKENIZE',
                           'DATE_GENERALIZE','NULL_OUT','CUSTOM'
                       )),
    doris_expression TEXT NOT NULL,
    applicable_types TEXT[] NOT NULL DEFAULT '{VARCHAR,TEXT,STRING}',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Masking Policies ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS masking_policies (
    id                  SERIAL PRIMARY KEY,
    name                TEXT NOT NULL UNIQUE,
    description         TEXT,
    classification_id   INT REFERENCES data_classifications(id) ON DELETE CASCADE,
    glossary_term_id    INT REFERENCES glossary_terms(id) ON DELETE CASCADE,
    algorithm_id        INT NOT NULL REFERENCES masking_algorithms(id),
    priority            INT NOT NULL DEFAULT 100,
    enabled             BOOLEAN NOT NULL DEFAULT true,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_policy_single_target CHECK (
        (classification_id IS NOT NULL AND glossary_term_id IS NULL) OR
        (classification_id IS NULL     AND glossary_term_id IS NOT NULL)
    )
);

-- ── Column Tags ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS column_tags (
    id                SERIAL PRIMARY KEY,
    doris_database    TEXT NOT NULL,
    doris_table       TEXT NOT NULL,
    column_name       TEXT NOT NULL,
    glossary_term_id  INT REFERENCES glossary_terms(id) ON DELETE SET NULL,
    classification_id INT REFERENCES data_classifications(id) ON DELETE SET NULL,
    auto_detected     BOOLEAN NOT NULL DEFAULT false,
    detection_score   NUMERIC(3,2),
    override_reason   TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (doris_database, doris_table, column_name)
);

-- ── Role Masking Exceptions ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS role_masking_exceptions (
    id                SERIAL PRIMARY KEY,
    role_name         TEXT NOT NULL,
    classification_id INT NOT NULL REFERENCES data_classifications(id) ON DELETE CASCADE,
    granted_by        TEXT NOT NULL DEFAULT 'system',
    granted_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (role_name, classification_id)
);

-- ── Doris View Manifest ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS doris_masked_views (
    id               SERIAL PRIMARY KEY,
    doris_database   TEXT NOT NULL,
    base_table       TEXT NOT NULL,
    view_name        TEXT NOT NULL,
    view_ddl         TEXT NOT NULL,
    columns_masked   TEXT[] NOT NULL DEFAULT '{}',
    view_checksum    TEXT,
    last_applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (doris_database, base_table)
);

-- ── Indexes ────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_column_tags_db_table
    ON column_tags(doris_database, doris_table);
CREATE INDEX IF NOT EXISTS idx_column_tags_term
    ON column_tags(glossary_term_id);
CREATE INDEX IF NOT EXISTS idx_column_tags_class
    ON column_tags(classification_id);
CREATE INDEX IF NOT EXISTS idx_masking_policies_class
    ON masking_policies(classification_id) WHERE enabled;
CREATE INDEX IF NOT EXISTS idx_masking_policies_term
    ON masking_policies(glossary_term_id) WHERE enabled;
CREATE INDEX IF NOT EXISTS idx_role_exceptions_role
    ON role_masking_exceptions(role_name);
CREATE INDEX IF NOT EXISTS idx_glossary_name_trgm
    ON glossary_terms USING GIN (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_glossary_display_trgm
    ON glossary_terms USING GIN (display_name gin_trgm_ops);

-- ── Seed: Data Classifications ─────────────────────────────────────────────
INSERT INTO data_classifications (name, display_name, description, sensitivity, color_hex) VALUES
  ('PII',          'Personally Identifiable Information',
   'Data that can identify a natural person directly or indirectly. '
   'Regulated under GDPR, CCPA, PIPL, and similar privacy frameworks.',
   'high',     '#FF4444'),
  ('PCI',          'Payment Card Industry Data',
   'Payment card numbers, CVVs, expiry dates, and cardholder data '
   'governed by PCI-DSS v4.',
   'critical', '#CC0000'),
  ('PHI',          'Protected Health Information',
   'Health-related personal data governed by HIPAA and related regulations. '
   'Includes diagnoses, prescriptions, and medical identifiers.',
   'critical', '#990000'),
  ('CONFIDENTIAL', 'Confidential Business Data',
   'Internal business data — financial forecasts, salaries, M&A targets — '
   'not cleared for public disclosure.',
   'medium',   '#FF8C00'),
  ('PUBLIC',       'Public Data',
   'Data explicitly cleared for unrestricted public access.',
   'low',      '#22AA44')
ON CONFLICT (name) DO NOTHING;

-- ── Seed: Glossary Terms ───────────────────────────────────────────────────
INSERT INTO glossary_terms
    (name, display_name, description, classification_id,
     column_name_patterns, description_patterns, steward)
SELECT
    t.name, t.display_name, t.description,
    c.id,
    t.col_patterns,
    t.desc_patterns,
    'data_steward'
FROM (VALUES
  ('email_address', 'Email Address',
   'Electronic mail address of a natural person.',
   'PII',
   ARRAY['email','e_mail','email_addr','emailaddress','user_email','contact_email'],
   ARRAY['email','e-mail','electronic mail']),

  ('full_name', 'Full Name',
   'Full given name and/or family name of a person.',
   'PII',
   ARRAY['full_name','fullname','first_name','last_name','firstname',
         'lastname','given_name','surname','customer_name','person_name'],
   ARRAY['person','first','last','given','family','surname']),

  ('phone_number', 'Phone Number',
   'Telephone or mobile number of a person or business.',
   'PII',
   ARRAY['phone','phone_number','phonenumber','mobile','telephone','tel',
         'phone_no','mobile_no','contact_number'],
   ARRAY['phone','telephone','mobile','contact number']),

  ('date_of_birth', 'Date of Birth',
   'Biological birth date of a person.',
   'PII',
   ARRAY['dob','date_of_birth','birth_date','birthdate','born_on','birth_day'],
   ARRAY['birth','date of birth','born','dob']),

  ('national_id', 'National Identifier',
   'Government-issued national identifier (SSN, NIN, NID, passport, etc.).',
   'PII',
   ARRAY['ssn','national_id','nin','tax_id','passport','id_number','gov_id',
         'national_number','fiscal_id','sin'],
   ARRAY['ssn','national','passport','government id','tax id','fiscal']),

  ('credit_card_number', 'Credit Card Number',
   'Primary Account Number (PAN) of a payment card — 13–19 digit string.',
   'PCI',
   ARRAY['card_number','credit_card','cc_number','pan','card_no','creditcard',
         'card_num','cc_num'],
   ARRAY['credit card','card number','pan','payment card','primary account']),

  ('credit_card_cvv', 'Credit Card CVV',
   'Card Verification Value (CVV/CVC/CVV2) security code on a payment card.',
   'PCI',
   ARRAY['cvv','cvc','cvv2','security_code','card_cvv','verification_code'],
   ARRAY['cvv','cvc','security code','verification']),

  ('ip_address', 'IP Address',
   'Network IP address that may indirectly identify a person under GDPR.',
   'PII',
   ARRAY['ip','ip_address','ipaddress','remote_ip','client_ip','source_ip',
         'user_ip','login_ip'],
   ARRAY['ip address','network address','client ip','remote address']),

  ('street_address', 'Street Address',
   'Physical postal street address of a person or business.',
   'PII',
   ARRAY['address','street_address','addr','street','mailing_address',
         'home_address','billing_address','shipping_address','postal_address'],
   ARRAY['address','street','mailing','home address','postal','billing']),

  ('salary', 'Salary / Compensation',
   'Total compensation or salary amount paid to an employee.',
   'CONFIDENTIAL',
   ARRAY['salary','compensation','wage','ctc','annual_pay',
         'base_salary','gross_salary','net_salary','total_comp'],
   ARRAY['salary','compensation','wage','earnings','remuneration'])
) AS t(name, display_name, description, class_name, col_patterns, desc_patterns)
JOIN data_classifications c ON c.name = t.class_name
ON CONFLICT (name) DO NOTHING;

-- ── Seed: Masking Algorithms ───────────────────────────────────────────────
INSERT INTO masking_algorithms
    (name, display_name, description, algorithm_type, doris_expression, applicable_types)
VALUES
  ('FULL_REDACT',
   'Full Redaction',
   'Replaces the entire value with ''****''. '
   'Use for the highest-sensitivity fields where no information should leak.',
   'REDACT',
   '''****''',
   ARRAY['VARCHAR','TEXT','STRING','CHAR']),

  ('SHA256_HASH',
   'SHA-256 Hash',
   'One-way deterministic hash (SHA-2, 256-bit). Preserves uniqueness and '
   'allows join operations on hashed values; value cannot be reversed.',
   'HASH',
   'SHA2({col}, 256)',
   ARRAY['VARCHAR','TEXT','STRING','CHAR']),

  ('EMAIL_PARTIAL',
   'Email Partial Mask',
   'Reveals first 2 characters and the full domain portion. '
   'Example: john.doe@example.com → jo***@example.com',
   'PARTIAL_MASK',
   'CONCAT(LEFT({col},2), REPEAT(''*'',GREATEST(0,LOCATE(''@'',{col})-3)), SUBSTRING({col},LOCATE(''@'',{col})))',
   ARRAY['VARCHAR','TEXT','STRING']),

  ('CREDIT_CARD_LAST4',
   'Credit Card Mask — Last 4 Digits',
   'Reveals only the last 4 digits; masks all preceding digits with ''*''. '
   'Example: 4532015112830366 → ************0366',
   'PARTIAL_MASK',
   'CONCAT(REPEAT(''*'',GREATEST(0,LENGTH(CAST({col} AS VARCHAR))-4)), RIGHT(CAST({col} AS VARCHAR),4))',
   ARRAY['VARCHAR','TEXT','STRING','CHAR','BIGINT']),

  ('DATE_YEAR_ONLY',
   'Date Year Generalisation',
   'Generalises a date to January 1st of the same year. '
   'Example: 1985-07-22 → 1985-01-01. Preserves approximate age for analytics.',
   'DATE_GENERALIZE',
   'DATE_FORMAT({col},''%Y-01-01'')',
   ARRAY['DATE','DATETIME','VARCHAR']),

  ('PHONE_LAST4',
   'Phone Partial Mask — Last 4 Digits',
   'Reveals only the last 4 digits of a phone number. '
   'Example: +1-800-555-1234 → **********1234',
   'PARTIAL_MASK',
   'CONCAT(REPEAT(''*'',GREATEST(0,LENGTH(CAST({col} AS VARCHAR))-4)), RIGHT(CAST({col} AS VARCHAR),4))',
   ARRAY['VARCHAR','TEXT','STRING','CHAR']),

  ('NULL_OUT',
   'Null Out',
   'Replaces the value with SQL NULL. Use when absence of value is '
   'preferable to a masked substitute.',
   'NULL_OUT',
   'NULL',
   ARRAY['VARCHAR','TEXT','STRING','INT','BIGINT','DATE','DATETIME','DECIMAL']),

  ('IP_LAST_OCTET',
   'IP Address — Mask Last Octet',
   'Anonymises the host portion of an IPv4 address by zeroing the last octet. '
   'Example: 192.168.1.42 → 192.168.1.0. Preserves network-level analytics.',
   'PARTIAL_MASK',
   'CONCAT(SUBSTRING_INDEX({col},''.'',3),''.0'')',
   ARRAY['VARCHAR','TEXT','STRING'])

ON CONFLICT (name) DO NOTHING;

-- ── Seed: Masking Policies (classification-level defaults) ─────────────────
INSERT INTO masking_policies
    (name, description, classification_id, algorithm_id, priority, enabled)
SELECT
    'policy_' || lower(c.name) || '_default',
    'Default masking policy for all ' || c.display_name || ' columns.',
    c.id,
    a.id,
    100,
    true
FROM data_classifications c
JOIN masking_algorithms a ON a.name = CASE c.name
    WHEN 'PII'          THEN 'SHA256_HASH'
    WHEN 'PCI'          THEN 'FULL_REDACT'
    WHEN 'PHI'          THEN 'FULL_REDACT'
    WHEN 'CONFIDENTIAL' THEN 'FULL_REDACT'
END
WHERE c.name IN ('PII','PCI','PHI','CONFIDENTIAL')
ON CONFLICT (name) DO NOTHING;

-- ── Seed: Masking Policies (term-level, higher priority) ───────────────────
INSERT INTO masking_policies
    (name, description, glossary_term_id, algorithm_id, priority, enabled)
SELECT
    'policy_term_' || lower(gt.name),
    'Term-level masking policy for: ' || gt.display_name,
    gt.id,
    a.id,
    200,
    true
FROM glossary_terms gt
JOIN masking_algorithms a ON a.name = CASE gt.name
    WHEN 'email_address'       THEN 'EMAIL_PARTIAL'
    WHEN 'full_name'           THEN 'SHA256_HASH'
    WHEN 'phone_number'        THEN 'PHONE_LAST4'
    WHEN 'date_of_birth'       THEN 'DATE_YEAR_ONLY'
    WHEN 'national_id'         THEN 'FULL_REDACT'
    WHEN 'credit_card_number'  THEN 'CREDIT_CARD_LAST4'
    WHEN 'credit_card_cvv'     THEN 'FULL_REDACT'
    WHEN 'ip_address'          THEN 'IP_LAST_OCTET'
    WHEN 'street_address'      THEN 'SHA256_HASH'
    WHEN 'salary'              THEN 'FULL_REDACT'
END
WHERE gt.name IN (
    'email_address','full_name','phone_number','date_of_birth',
    'national_id','credit_card_number','credit_card_cvv',
    'ip_address','street_address','salary'
)
ON CONFLICT (name) DO NOTHING;

-- ── Seed: Role Masking Exceptions ──────────────────────────────────────────
-- data_admin, platform_admin, account_admin bypass masking for all
-- sensitive classifications.
INSERT INTO role_masking_exceptions (role_name, classification_id, granted_by)
SELECT r.role_name, c.id, 'migration_001'
FROM (VALUES
    ('data_admin'),
    ('platform_admin'),
    ('account_admin')
) AS r(role_name)
CROSS JOIN data_classifications c
WHERE c.name IN ('PII','PCI','PHI','CONFIDENTIAL')
ON CONFLICT (role_name, classification_id) DO NOTHING;
