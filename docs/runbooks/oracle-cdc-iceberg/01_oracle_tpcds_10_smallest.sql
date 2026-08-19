-- ============================================================
-- oracle_tpcds_10_smallest.sql
-- Oracle XE 21c — TPC-DS 10 smallest tables for CDC replication
--
-- Target  : XEPDB1 / schema TPCDS
-- Purpose : Create the 10 smallest TPC-DS tables in Oracle so
--           Debezium Oracle LogMiner can capture DML changes and
--           stream them to Kafka → Iceberg via the Iceberg Sink.
--
-- Tables chosen by row count (smallest first):
--   1. income_band          20 rows
--   2. ship_mode            20 rows
--   3. warehouse            20 rows
--   4. reason               55 rows
--   5. call_center          42 rows
--   6. web_site            54 rows
--   7. web_page          2040 rows
--   8. household_demographics 7200 rows
--   9. catalog_page        11718 rows
--  10. promotion           1000 rows
--
-- Run via:
--   kubectl exec -n prod deploy/oracle-xe -- sqlplus TPCDS/TPCDS@XEPDB1 @/tmp/oracle_tpcds_10_smallest.sql
-- ============================================================

-- ── Schema / user setup (run as SYSDBA first) ────────────────────────────
-- Run this block once as SYS or SYSTEM before running the rest of the file.
/*
ALTER SESSION SET CONTAINER = XEPDB1;
CREATE USER tpcds IDENTIFIED BY "TpcdsPwd123!"
    DEFAULT TABLESPACE USERS
    TEMPORARY TABLESPACE TEMP
    QUOTA UNLIMITED ON USERS;
GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE,
      ALTER TABLE, INSERT, UPDATE, DELETE, SELECT ANY TABLE TO tpcds;
-- LogMiner privileges for Debezium
GRANT EXECUTE ON SYS.DBMS_LOGMNR TO tpcds;
GRANT SELECT ON V_$DATABASE      TO tpcds;
GRANT FLASHBACK ANY TABLE        TO tpcds;
GRANT SELECT ANY TABLE           TO tpcds;
GRANT SELECT_CATALOG_ROLE        TO tpcds;
GRANT LOCK ANY TABLE             TO tpcds;
-- Supplemental logging (required by LogMiner / Debezium)
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
*/

-- Connect as tpcds before running the table DDL below
-- sqlplus tpcds/TpcdsPwd123!@XEPDB1

-- ── 1. income_band ────────────────────────────────────────────────────────
CREATE TABLE income_band (
    ib_income_band_sk   NUMBER(11)    NOT NULL,
    ib_lower_bound      NUMBER(11),
    ib_upper_bound      NUMBER(11),
    CONSTRAINT pk_income_band PRIMARY KEY (ib_income_band_sk)
);
COMMENT ON TABLE income_band IS 'TPC-DS income_band dimension (20 rows)';

-- ── 2. ship_mode ──────────────────────────────────────────────────────────
CREATE TABLE ship_mode (
    sm_ship_mode_sk     NUMBER(11)    NOT NULL,
    sm_ship_mode_id     CHAR(16),
    sm_type             VARCHAR2(30),
    sm_code             VARCHAR2(10),
    sm_carrier          VARCHAR2(20),
    sm_contract         VARCHAR2(20),
    CONSTRAINT pk_ship_mode PRIMARY KEY (sm_ship_mode_sk)
);

-- ── 3. warehouse ──────────────────────────────────────────────────────────
CREATE TABLE warehouse (
    w_warehouse_sk      NUMBER(11)    NOT NULL,
    w_warehouse_id      CHAR(16),
    w_warehouse_name    VARCHAR2(20),
    w_warehouse_sq_ft   NUMBER(11),
    w_street_number     VARCHAR2(10),
    w_street_name       VARCHAR2(60),
    w_street_type       VARCHAR2(15),
    w_suite_number      VARCHAR2(10),
    w_city              VARCHAR2(60),
    w_county            VARCHAR2(30),
    w_state             CHAR(2),
    w_zip               CHAR(10),
    w_country           VARCHAR2(20),
    w_gmt_offset        NUMBER(5,2),
    CONSTRAINT pk_warehouse PRIMARY KEY (w_warehouse_sk)
);

-- ── 4. reason ─────────────────────────────────────────────────────────────
CREATE TABLE reason (
    r_reason_sk         NUMBER(11)    NOT NULL,
    r_reason_id         CHAR(16),
    r_reason_desc       VARCHAR2(100),
    CONSTRAINT pk_reason PRIMARY KEY (r_reason_sk)
);

-- ── 5. call_center ────────────────────────────────────────────────────────
CREATE TABLE call_center (
    cc_call_center_sk   NUMBER(11)    NOT NULL,
    cc_call_center_id   CHAR(16),
    cc_rec_start_date   DATE,
    cc_rec_end_date     DATE,
    cc_closed_date_sk   NUMBER(11),
    cc_open_date_sk     NUMBER(11),
    cc_name             VARCHAR2(50),
    cc_class            VARCHAR2(50),
    cc_employees        NUMBER(11),
    cc_sq_ft            NUMBER(11),
    cc_hours            VARCHAR2(20),
    cc_manager          VARCHAR2(40),
    cc_mkt_id           NUMBER(11),
    cc_mkt_class        VARCHAR2(50),
    cc_mkt_desc         VARCHAR2(100),
    cc_market_manager   VARCHAR2(40),
    cc_division         NUMBER(11),
    cc_division_name    VARCHAR2(50),
    cc_company          NUMBER(11),
    cc_company_name     CHAR(50),
    cc_street_number    VARCHAR2(10),
    cc_street_name      VARCHAR2(60),
    cc_street_type      CHAR(15),
    cc_suite_number     CHAR(10),
    cc_city             VARCHAR2(60),
    cc_county           VARCHAR2(30),
    cc_state            CHAR(2),
    cc_zip              CHAR(10),
    cc_country          VARCHAR2(20),
    cc_gmt_offset       NUMBER(5,2),
    cc_tax_percentage   NUMBER(5,2),
    CONSTRAINT pk_call_center PRIMARY KEY (cc_call_center_sk)
);

-- ── 6. web_site ───────────────────────────────────────────────────────────
CREATE TABLE web_site (
    web_site_sk         NUMBER(11)    NOT NULL,
    web_site_id         CHAR(16),
    web_rec_start_date  DATE,
    web_rec_end_date    DATE,
    web_name            VARCHAR2(50),
    web_open_date_sk    NUMBER(11),
    web_close_date_sk   NUMBER(11),
    web_class           VARCHAR2(50),
    web_manager         VARCHAR2(40),
    web_mkt_id          NUMBER(11),
    web_mkt_class       VARCHAR2(50),
    web_mkt_desc        VARCHAR2(100),
    web_market_manager  VARCHAR2(40),
    web_company_id      NUMBER(11),
    web_company_name    CHAR(50),
    web_street_number   VARCHAR2(10),
    web_street_name     VARCHAR2(60),
    web_street_type     CHAR(15),
    web_suite_number    CHAR(10),
    web_city            VARCHAR2(60),
    web_county          VARCHAR2(30),
    web_state           CHAR(2),
    web_zip             CHAR(10),
    web_country         VARCHAR2(20),
    web_gmt_offset      NUMBER(5,2),
    web_tax_percentage  NUMBER(5,2),
    CONSTRAINT pk_web_site PRIMARY KEY (web_site_sk)
);

-- ── 7. web_page ───────────────────────────────────────────────────────────
CREATE TABLE web_page (
    wp_web_page_sk      NUMBER(11)    NOT NULL,
    wp_web_page_id      CHAR(16),
    wp_rec_start_date   DATE,
    wp_rec_end_date     DATE,
    wp_creation_date_sk NUMBER(11),
    wp_access_date_sk   NUMBER(11),
    wp_autogen_flag     CHAR(1),
    wp_customer_sk      NUMBER(11),
    wp_url              VARCHAR2(100),
    wp_type             CHAR(50),
    wp_char_count       NUMBER(11),
    wp_link_count       NUMBER(11),
    wp_image_count      NUMBER(11),
    wp_max_ad_count     NUMBER(11),
    CONSTRAINT pk_web_page PRIMARY KEY (wp_web_page_sk)
);

-- ── 8. household_demographics ─────────────────────────────────────────────
CREATE TABLE household_demographics (
    hd_demo_sk              NUMBER(11)  NOT NULL,
    hd_income_band_sk       NUMBER(11),
    hd_buy_potential        VARCHAR2(15),
    hd_dep_count            NUMBER(11),
    hd_vehicle_count        NUMBER(11),
    CONSTRAINT pk_household_demographics PRIMARY KEY (hd_demo_sk),
    CONSTRAINT fk_hd_income FOREIGN KEY (hd_income_band_sk)
        REFERENCES income_band(ib_income_band_sk)
);

-- ── 9. catalog_page ───────────────────────────────────────────────────────
CREATE TABLE catalog_page (
    cp_catalog_page_sk      NUMBER(11)  NOT NULL,
    cp_catalog_page_id      CHAR(16),
    cp_start_date_sk        NUMBER(11),
    cp_end_date_sk          NUMBER(11),
    cp_department           VARCHAR2(50),
    cp_catalog_number       NUMBER(11),
    cp_catalog_page_number  NUMBER(11),
    cp_description          VARCHAR2(100),
    cp_type                 VARCHAR2(100),
    CONSTRAINT pk_catalog_page PRIMARY KEY (cp_catalog_page_sk)
);

-- ── 10. promotion ─────────────────────────────────────────────────────────
CREATE TABLE promotion (
    p_promo_sk              NUMBER(11)  NOT NULL,
    p_promo_id              CHAR(16),
    p_start_date_sk         NUMBER(11),
    p_end_date_sk           NUMBER(11),
    p_item_sk               NUMBER(11),
    p_cost                  NUMBER(15,2),
    p_response_target       NUMBER(11),
    p_promo_name            CHAR(50),
    p_channel_dmail         CHAR(1),
    p_channel_email         CHAR(1),
    p_channel_catalog       CHAR(1),
    p_channel_tv            CHAR(1),
    p_channel_radio         CHAR(1),
    p_channel_press         CHAR(1),
    p_channel_event         CHAR(1),
    p_channel_demo          CHAR(1),
    p_channel_details       VARCHAR2(100),
    p_purpose               CHAR(15),
    p_discount_active       CHAR(1),
    CONSTRAINT pk_promotion PRIMARY KEY (p_promo_sk)
);

-- ── Supplemental logging per table (required by Debezium ALL_COLUMN mode) ─
ALTER TABLE income_band          ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE ship_mode            ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE warehouse            ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE reason               ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE call_center          ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE web_site             ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE web_page             ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE household_demographics ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE catalog_page         ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE promotion            ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- ── Sample seed data (verifies CDC flow) ─────────────────────────────────
INSERT INTO income_band VALUES (1, 0,      11999);
INSERT INTO income_band VALUES (2, 12000,  23999);
INSERT INTO income_band VALUES (3, 24000,  35999);
INSERT INTO ship_mode VALUES   (1,'AAAAAAAABAAAAAAA','LIBRARY',  'LIB',  'AIRBORNE','OVERNIGHT');
INSERT INTO ship_mode VALUES   (2,'AAAAAAAACAAAAAAA','SURFACE',  'SUR',  'FEDEX',   'STANDARD');
INSERT INTO warehouse VALUES   (1,'AAAAAAAABAAAAAAA','Small Wh',  73552,'111','Maple St',   'Street','Suite 160','Midway',   'Williamson County','TN','31904','United States',-5);
INSERT INTO reason VALUES      (1,'AAAAAAAABAAAAAAA','Did not fit');
COMMIT;

-- ── Verification ─────────────────────────────────────────────────────────
SELECT table_name, num_rows
FROM   all_tables
WHERE  owner = 'TPCDS'
  AND  table_name IN (
    'INCOME_BAND','SHIP_MODE','WAREHOUSE','REASON','CALL_CENTER',
    'WEB_SITE','WEB_PAGE','HOUSEHOLD_DEMOGRAPHICS','CATALOG_PAGE','PROMOTION'
  )
ORDER BY table_name;

COMMIT;
