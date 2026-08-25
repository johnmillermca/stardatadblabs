-- ============================================================
-- Star Knowledge Catalog — Sample Doris Database
-- Database: governance_demo
--
-- Creates a realistic customer/orders/payments dataset that
-- intentionally contains multiple PII, PCI, and CONFIDENTIAL
-- columns so the masking engine has something to work with.
--
-- Apply via:
--   mysql -h 192.168.1.50 -P 30090 -u root < 001_create_schema.sql
-- ============================================================

-- Create database
CREATE DATABASE IF NOT EXISTS governance_demo;
USE governance_demo;

-- ── Doris user for the analyst role ────────────────────────────────────────
-- This user maps to the 'analyst' role in the RBAC Control Plane.
-- In production this is provisioned by rbac-plane's DorisAdapter.
-- Here we create it explicitly for the demo.

CREATE USER IF NOT EXISTS 'analyst'@'%' IDENTIFIED BY 'analyst_pass_demo';
CREATE USER IF NOT EXISTS 'data_admin_user'@'%' IDENTIFIED BY 'admin_pass_demo';

-- ── Table: customers ───────────────────────────────────────────────────────
-- Contains PII columns: full_name, email, phone_number, date_of_birth,
-- national_id, street_address, ip_address
-- Contains CONFIDENTIAL: salary

DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    customer_id     BIGINT          NOT NULL,
    -- PII — full_name → SHA256_HASH (glossary term: full_name)
    full_name       VARCHAR(200)    NOT NULL,
    -- PII — email → EMAIL_PARTIAL (glossary term: email_address)
    email           VARCHAR(320)    NOT NULL,
    -- PII — phone_number → PHONE_LAST4 (glossary term: phone_number)
    phone_number    VARCHAR(30),
    -- PII — date_of_birth → DATE_YEAR_ONLY (glossary term: date_of_birth)
    date_of_birth   DATE,
    -- PII — national_id → FULL_REDACT (glossary term: national_id)
    national_id     VARCHAR(30),
    -- PII — street_address → SHA256_HASH (glossary term: street_address)
    street_address  VARCHAR(500),
    city            VARCHAR(100),
    country_code    CHAR(2)         NOT NULL DEFAULT 'US',
    -- PII — ip_address → IP_LAST_OCTET (glossary term: ip_address)
    ip_address      VARCHAR(45),
    -- CONFIDENTIAL — salary → FULL_REDACT (glossary term: salary)
    salary          DECIMAL(15,2),
    customer_tier   VARCHAR(20)     NOT NULL DEFAULT 'standard'
                        COMMENT 'standard|silver|gold|platinum',
    is_active       TINYINT         NOT NULL DEFAULT 1,
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(customer_id)
DISTRIBUTED BY HASH(customer_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "V2",
    "compression" = "LZ4"
);

-- ── Table: orders ──────────────────────────────────────────────────────────
-- Clean table — no sensitive columns — used to demonstrate that
-- untagged tables are routed to the base table directly.

DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    order_id        BIGINT          NOT NULL,
    customer_id     BIGINT          NOT NULL,
    order_date      DATETIME        NOT NULL,
    status          VARCHAR(30)     NOT NULL DEFAULT 'pending'
                        COMMENT 'pending|confirmed|shipped|delivered|cancelled|refunded',
    total_amount    DECIMAL(15,2)   NOT NULL,
    currency        CHAR(3)         NOT NULL DEFAULT 'USD',
    channel         VARCHAR(30)     NOT NULL DEFAULT 'web',
    notes           VARCHAR(1000),
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "V2",
    "compression" = "LZ4"
);

-- ── Table: payments ────────────────────────────────────────────────────────
-- Contains PCI columns: card_number, credit_card_cvv

DROP TABLE IF EXISTS payments;

CREATE TABLE payments (
    payment_id      BIGINT          NOT NULL,
    order_id        BIGINT          NOT NULL,
    -- PCI — card_number → CREDIT_CARD_LAST4 (glossary term: credit_card_number)
    card_number     VARCHAR(25),
    -- PCI — credit_card_cvv → FULL_REDACT (glossary term: credit_card_cvv)
    credit_card_cvv VARCHAR(10),
    card_type       VARCHAR(20),
    payment_method  VARCHAR(30)     NOT NULL DEFAULT 'credit_card',
    amount          DECIMAL(15,2)   NOT NULL,
    currency        CHAR(3)         NOT NULL DEFAULT 'USD',
    status          VARCHAR(20)     NOT NULL DEFAULT 'pending',
    gateway         VARCHAR(50),
    transaction_ref VARCHAR(100),
    paid_at         DATETIME
)
DUPLICATE KEY(payment_id)
DISTRIBUTED BY HASH(payment_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "V2",
    "compression" = "LZ4"
);

-- ── Table: products ────────────────────────────────────────────────────────
-- No sensitive columns — control table to prove non-sensitive tables
-- are passed through with no masking overhead.

DROP TABLE IF EXISTS products;

CREATE TABLE products (
    product_id      BIGINT          NOT NULL,
    sku             VARCHAR(100)    NOT NULL,
    name            VARCHAR(500)    NOT NULL,
    category        VARCHAR(100),
    subcategory     VARCHAR(100),
    unit_price      DECIMAL(12,2)   NOT NULL,
    currency        CHAR(3)         NOT NULL DEFAULT 'USD',
    is_active       TINYINT         NOT NULL DEFAULT 1,
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "V2",
    "compression" = "LZ4"
);

-- ── Grant base table access to data_admin_user ─────────────────────────────
GRANT SELECT_PRIV ON governance_demo.* TO 'data_admin_user'@'%';
GRANT SELECT_PRIV ON governance_demo.* TO 'root'@'%';
