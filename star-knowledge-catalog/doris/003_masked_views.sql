-- ============================================================
-- Star Knowledge Catalog — Doris Masked Views (pre-built)
-- Database: governance_demo
--
-- These views are the target objects for analyst-role queries.
-- In production they are generated and applied automatically
-- by the masking engine (POST /api/v1/masking/apply).
--
-- This file is provided as a reference/bootstrap script for
-- environments where the catalog API is not yet running.
--
-- Apply after 001_create_schema.sql and 002_seed_data.sql.
-- Apply via:
--   mysql -h 192.168.1.50 -P 30090 -u root < 003_masked_views.sql
-- ============================================================

USE governance_demo;

-- ── customers_masked ─────────────────────────────────────────────────────────
-- Masking applied per active policies:
--   full_name       → SHA2({col}, 256)          [term: full_name / PII]
--   email           → EMAIL_PARTIAL              [term: email_address / PII]
--   phone_number    → PHONE_LAST4                [term: phone_number / PII]
--   date_of_birth   → DATE_YEAR_ONLY             [term: date_of_birth / PII]
--   national_id     → '****'                     [term: national_id / PII]
--   street_address  → SHA2({col}, 256)           [term: street_address / PII]
--   ip_address      → IP_LAST_OCTET              [term: ip_address / PII]
--   salary          → '****'                     [term: salary / CONFIDENTIAL]
--   All other columns passed through unchanged.

CREATE OR REPLACE VIEW governance_demo.customers_masked AS
SELECT
  customer_id,
  SHA2(`full_name`, 256) AS `full_name`,
  CONCAT(LEFT(`email`,2), REPEAT('*',GREATEST(0,LOCATE('@',`email`)-3)), SUBSTRING(`email`,LOCATE('@',`email`))) AS `email`,
  CONCAT(REPEAT('*',GREATEST(0,LENGTH(CAST(`phone_number` AS VARCHAR))-4)), RIGHT(CAST(`phone_number` AS VARCHAR),4)) AS `phone_number`,
  DATE_FORMAT(`date_of_birth`,'%Y-01-01') AS `date_of_birth`,
  '****' AS `national_id`,
  SHA2(`street_address`, 256) AS `street_address`,
  city,
  country_code,
  CONCAT(SUBSTRING_INDEX(`ip_address`,'.',3),'.0') AS `ip_address`,
  '****' AS `salary`,
  customer_tier,
  is_active,
  created_at,
  updated_at
FROM governance_demo.customers;

-- ── payments_masked ───────────────────────────────────────────────────────────
-- Masking applied per active policies:
--   card_number      → CREDIT_CARD_LAST4         [term: credit_card_number / PCI]
--   credit_card_cvv  → '****'                    [term: credit_card_cvv / PCI]
--   All other columns passed through unchanged.

CREATE OR REPLACE VIEW governance_demo.payments_masked AS
SELECT
  payment_id,
  order_id,
  CONCAT(REPEAT('*',GREATEST(0,LENGTH(CAST(`card_number` AS VARCHAR))-4)), RIGHT(CAST(`card_number` AS VARCHAR),4)) AS `card_number`,
  '****' AS `credit_card_cvv`,
  card_type,
  payment_method,
  amount,
  currency,
  status,
  gateway,
  transaction_ref,
  paid_at
FROM governance_demo.payments;

-- ── Grant SELECT on masked views to analyst user ──────────────────────────────
GRANT SELECT_PRIV ON governance_demo.customers_masked TO 'analyst'@'%';
GRANT SELECT_PRIV ON governance_demo.payments_masked  TO 'analyst'@'%';

-- orders and products have no sensitive columns — grant base table directly
GRANT SELECT_PRIV ON governance_demo.orders    TO 'analyst'@'%';
GRANT SELECT_PRIV ON governance_demo.products  TO 'analyst'@'%';
