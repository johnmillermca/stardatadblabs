-- =============================================================================
-- DML TEST STEPS — Databricks SQL console
-- Targets : lakehouse.lakehouse_db.vw_customer_latest        (view)
--           lakehouse.lakehouse_db.vw_customer_orders_latest (view)
-- Refreshed by: docker/databricks-notebooks/nb_multi_table_auto_reader.py
-- Warehouse: Serverless Starter Warehouse
-- Run each block in the Databricks SQL Editor:
--   https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 1 — Sanity checks: schema and views exist
-- ─────────────────────────────────────────────────────────────────────────────

-- 1-A: Confirm schema exists
SHOW SCHEMAS IN lakehouse;
-- ✅ Expected: lakehouse_db listed

-- 1-B: List all views in the schema
SHOW VIEWS IN lakehouse.lakehouse_db;
-- ✅ Expected:
--   vw_customer_latest        (view)
--   vw_customer_orders_latest (view)

-- 1-C: Describe the customer view
DESCRIBE EXTENDED lakehouse.lakehouse_db.vw_customer_latest;
-- ✅ Look for "View Text" containing read_files('s3://stardata-databricks/...')

-- 1-D: Describe the customer_orders view
DESCRIBE EXTENDED lakehouse.lakehouse_db.vw_customer_orders_latest;
-- ✅ Same — View Text should point at the customer_orders/data/ directory


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 2 — Row counts
-- ─────────────────────────────────────────────────────────────────────────────

-- 2-A: customer view row count
SELECT COUNT(*) AS customer_rows
FROM   lakehouse.lakehouse_db.vw_customer_latest;
-- ✅ Expected: 1000 (baseline); 1100 after the 100-row insert

-- 2-B: customer_orders view row count
SELECT COUNT(*) AS orders_rows
FROM   lakehouse.lakehouse_db.vw_customer_orders_latest;
-- ✅ Expected: reflects the latest Iceberg snapshot for customer_orders


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 3 — Spot-check rows: first 10 and last 10
-- ─────────────────────────────────────────────────────────────────────────────

-- 3-A: First 10 rows from the customer view
SELECT customer_id, full_name, email, city, customer_tier,
       ROUND(salary, 2) AS salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
ORDER  BY customer_id
LIMIT  10;
-- ✅ Expected: IDs 1–10, names and cities matching the seed=42 batch

-- 3-B: Last 10 rows from the customer view (highest IDs)
SELECT customer_id, full_name, email, city, customer_tier,
       ROUND(salary, 2) AS salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
ORDER  BY customer_id DESC
LIMIT  10;
-- ✅ Expected: highest IDs (1000 base data; 1100 after the 100-row insert)


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 4 — Freshness checks: confirm latest batch is visible
-- ─────────────────────────────────────────────────────────────────────────────

-- 4-A: After a 100-row insert (IDs 1001–1100), confirm the new rows are visible
SELECT customer_id, full_name, city, customer_tier, ROUND(salary,2) AS salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
WHERE  customer_id BETWEEN 1095 AND 1100
ORDER  BY customer_id;
-- ✅ Expected: 6 rows with IDs 1095–1100 (seed=99 names and cities)

-- 4-B: Insert batches distinguished by created_at date
SELECT
    DATE(created_at)   AS insert_date,
    MIN(customer_id)   AS first_id,
    MAX(customer_id)   AS last_id,
    COUNT(*)           AS row_count
FROM   lakehouse.lakehouse_db.vw_customer_latest
GROUP  BY DATE(created_at)
ORDER  BY insert_date;
-- ✅ Expected:
--   2026-01-xx  first_id=1    last_id=1000  row_count=1000  (seed=42, Jan 2026)
--   2026-09-xx  first_id=1001 last_id=1100  row_count=100   (seed=99, Sep 2026)


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 5 — Analytical queries: tier and salary distribution
-- ─────────────────────────────────────────────────────────────────────────────

-- 5-A: Tier distribution (customer view)
SELECT customer_tier,
       COUNT(*)              AS cnt,
       ROUND(AVG(salary), 2) AS avg_salary,
       ROUND(MIN(salary), 2) AS min_salary,
       ROUND(MAX(salary), 2) AS max_salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
GROUP  BY customer_tier
ORDER  BY cnt DESC;
-- ✅ Expected for 1000-row baseline:
--   silver   270  ~115908
--   platinum 249  ~116659
--   gold     248  ~115646
--   standard 233  ~115137

-- 5-B: Salary bucket distribution
SELECT
    CASE
        WHEN salary <  50000  THEN '< 50k'
        WHEN salary <  75000  THEN '50k–75k'
        WHEN salary < 100000  THEN '75k–100k'
        WHEN salary < 150000  THEN '100k–150k'
        ELSE                       '150k+'
    END                      AS salary_bucket,
    COUNT(*)                  AS cnt
FROM   lakehouse.lakehouse_db.vw_customer_latest
GROUP  BY salary_bucket
ORDER  BY MIN(salary);

-- 5-C: Top 10 cities by customer count
SELECT city, country_code,
       COUNT(*)              AS customer_count,
       ROUND(AVG(salary), 2) AS avg_salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
GROUP  BY city, country_code
ORDER  BY customer_count DESC
LIMIT  10;

-- 5-D: Confirm snap_file column — which parquet files does the view read?
SELECT DISTINCT snap_file, snap_file_size
FROM   lakehouse.lakehouse_db.vw_customer_latest
ORDER  BY snap_file;
-- ✅ Expected: s3://stardata-databricks/iceberg/warehouse/lakehouse_db/customer/data/*.parquet
--    File sizes should be non-zero (snappy-compressed).


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 6 — Negative / boundary tests  (customer view)
-- ─────────────────────────────────────────────────────────────────────────────

-- 6-A: No duplicate customer_ids
SELECT customer_id, COUNT(*) AS dup_count
FROM   lakehouse.lakehouse_db.vw_customer_latest
GROUP  BY customer_id
HAVING COUNT(*) > 1;
-- ✅ Expected: 0 rows (no duplicates)

-- 6-B: No NULL customer_ids
SELECT COUNT(*) AS null_ids
FROM   lakehouse.lakehouse_db.vw_customer_latest
WHERE  customer_id IS NULL;
-- ✅ Expected: 0

-- 6-C: All rows have a valid customer_tier
SELECT COUNT(*) AS invalid_tier_rows
FROM   lakehouse.lakehouse_db.vw_customer_latest
WHERE  customer_tier NOT IN ('standard', 'silver', 'gold', 'platinum');
-- ✅ Expected: 0

-- 6-D: Salary range sanity check
SELECT
    MIN(salary)  AS min_salary,
    MAX(salary)  AS max_salary,
    COUNT(*)     AS out_of_range
FROM   lakehouse.lakehouse_db.vw_customer_latest
WHERE  salary < 30000 OR salary > 200000;
-- ✅ Expected: out_of_range = 0  (all salaries generated in [30000, 200000])

-- 6-E: is_active only contains 0 or 1
SELECT DISTINCT is_active
FROM   lakehouse.lakehouse_db.vw_customer_latest
ORDER  BY is_active;
-- ✅ Expected: only 1 (all seed rows inserted as active)
