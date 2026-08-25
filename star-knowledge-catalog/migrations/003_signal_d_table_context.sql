-- ============================================================
-- Migration 003 — Signal D: table-name context columns
--
-- Adds table_name_negative_patterns and table_name_positive_patterns
-- to glossary_terms, then populates them for all seed terms.
--
-- Safe to re-run (ALTER TABLE IF NOT EXISTS / ON CONFLICT DO UPDATE).
-- ============================================================

ALTER TABLE glossary_terms
    ADD COLUMN IF NOT EXISTS table_name_negative_patterns TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS table_name_positive_patterns TEXT[] NOT NULL DEFAULT '{}';

-- ── Populate table-name context patterns ──────────────────────────────────────
-- full_name: reject in product/item/catalog tables; boost in person-record tables
UPDATE glossary_terms SET
    table_name_negative_patterns = ARRAY['product','item','inventory','catalog','sku',
                                         'category','material','asset','equipment'],
    table_name_positive_patterns = ARRAY['customer','employee','user','person','staff',
                                         'contact','member','patient','client','account',
                                         'vendor','supplier','driver','agent','applicant']
WHERE name = 'full_name';

-- salary: reject in payment/transaction tables; boost in HR/payroll tables
UPDATE glossary_terms SET
    table_name_negative_patterns = ARRAY['payment','transaction','order','invoice',
                                         'receipt','ledger','billing'],
    table_name_positive_patterns = ARRAY['employee','staff','payroll','hr','salary',
                                         'compensation','workforce','headcount']
WHERE name = 'salary';

-- street_address: reject in warehouse/location/geo tables
UPDATE glossary_terms SET
    table_name_negative_patterns = ARRAY['warehouse','depot','store','branch',
                                         'location','venue','region','zone'],
    table_name_positive_patterns = ARRAY['customer','employee','user','person',
                                         'contact','member','patient','client']
WHERE name = 'street_address';

-- ip_address: reject in network/infra/log tables where IP is not person-linked
UPDATE glossary_terms SET
    table_name_negative_patterns = ARRAY['server','device','router','switch',
                                         'node','host','network','infra'],
    table_name_positive_patterns = ARRAY['session','login','audit','access',
                                         'user','activity','event']
WHERE name = 'ip_address';

-- national_id: only relevant in person-record tables
UPDATE glossary_terms SET
    table_name_positive_patterns = ARRAY['customer','employee','user','person',
                                         'staff','member','patient','client',
                                         'applicant','identity','kyc']
WHERE name = 'national_id';
