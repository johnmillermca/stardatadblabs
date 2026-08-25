-- ============================================================
-- Star Knowledge Catalog — Migration 003: Negative Patterns
--
-- Adds a negative_patterns column to glossary_terms.
-- These tokens are used by Signal C of the confidence arbitrator
-- to REJECT 0.7 substring matches where the column name contains
-- a context token that indicates the column is NOT the sensitive
-- type (e.g. "company_name" should not be tagged as full_name).
-- ============================================================

ALTER TABLE glossary_terms
    ADD COLUMN IF NOT EXISTS negative_patterns TEXT[] NOT NULL DEFAULT '{}';

-- ── Seed negative patterns for all 10 terms ────────────────────────────────
-- full_name: reject columns that are clearly organisational or UI labels
UPDATE glossary_terms SET negative_patterns = ARRAY[
    'company','display','brand','product','vendor','store',
    'account','role','status','category','type','label','title'
] WHERE name = 'full_name';

-- email_address: reject columns that are template/system email labels
UPDATE glossary_terms SET negative_patterns = ARRAY[
    'template','subject','body','domain','host','server',
    'notification','type','category','format','field'
] WHERE name = 'email_address';

-- phone_number: reject columns that describe phone characteristics
UPDATE glossary_terms SET negative_patterns = ARRAY[
    'type','category','format','prefix','extension','country_code',
    'carrier','brand','model','imei','device'
] WHERE name = 'phone_number';

-- date_of_birth: reject columns that are clearly non-person dates
UPDATE glossary_terms SET negative_patterns = ARRAY[
    'product','order','item','event','invoice','payment',
    'subscription','contract','expiry','expiration','created',
    'updated','modified','published','reviewed'
] WHERE name = 'date_of_birth';

-- national_id: reject columns that describe document or system identifiers
UPDATE glossary_terms SET negative_patterns = ARRAY[
    'type','category','format','prefix','version','revision',
    'transaction','order','product','invoice','batch','session',
    'tracking','reference','correlation','request','error'
] WHERE name = 'national_id';

-- credit_card_number: reject columns about cards that are not the PAN
UPDATE glossary_terms SET negative_patterns = ARRAY[
    'type','brand','scheme','network','holder','token',
    'expiry','expiration','month','year','issuer','category'
] WHERE name = 'credit_card_number';

-- credit_card_cvv: reject columns about verification that are not the CVV
UPDATE glossary_terms SET negative_patterns = ARRAY[
    'type','status','result','response','flag','enabled',
    'required','method','attempt','count','error','message'
] WHERE name = 'credit_card_cvv';

-- ip_address: reject columns that describe IP characteristics
UPDATE glossary_terms SET negative_patterns = ARRAY[
    'type','version','protocol','port','range','subnet',
    'mask','gateway','dns','pool','allocation','block'
] WHERE name = 'ip_address';

-- street_address: reject columns about address metadata
UPDATE glossary_terms SET negative_patterns = ARRAY[
    'type','format','country','region','zone','line',
    'component','part','field','label','description',
    'validation','lookup','verified','geocode'
] WHERE name = 'street_address';

-- salary: reject columns about pay that are not compensation values
UPDATE glossary_terms SET negative_patterns = ARRAY[
    'type','frequency','period','currency','band','grade',
    'level','scale','range','structure','policy','review',
    'review_date','effective_date','code','status'
] WHERE name = 'salary';
