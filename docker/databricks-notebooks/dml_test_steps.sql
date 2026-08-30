-- =============================================================================
-- DML TEST STEPS — Databricks SQL console
-- Targets : lakehouse.lakehouse_db.vw_customer_latest          (view)
--           lakehouse.lakehouse_db.customer_snapshot_audit     (Delta table)
--           lakehouse.lakehouse_db.vw_customer_orders_latest   (view)
--           lakehouse.lakehouse_db.customer_orders_snapshot_audit (Delta table)
-- Refreshed by: docker/databricks-notebooks/nb_multi_table_auto_reader.py
-- Warehouse: Serverless Starter Warehouse
-- Run each block in the Databricks SQL Editor:
--   https://dbc-11a1dbc5-061a.cloud.databricks.com/sql/editor
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 1 — Sanity checks: schema, objects exist
-- ─────────────────────────────────────────────────────────────────────────────

-- 1-A: Confirm schema exists
SHOW SCHEMAS IN lakehouse;
-- ✅ Expected: lakehouse_db listed

-- 1-B: List all tables and views in the schema
SHOW TABLES IN lakehouse.lakehouse_db;
-- ✅ Expected:
--   vw_customer_latest              (view)
--   vw_customer_orders_latest       (view)
--   customer_snapshot_audit         (table)
--   customer_orders_snapshot_audit  (table)

-- 1-C: Describe the view
DESCRIBE EXTENDED lakehouse.lakehouse_db.vw_customer_latest;
-- ✅ Look for "View Text" row containing read_files('s3://stardata-databricks/...')

-- 1-D: Describe the Delta table
DESCRIBE EXTENDED lakehouse.lakehouse_db.customer_snapshot_audit;
-- ✅ Look for: Type = MANAGED, Provider = delta, refreshed_at column present


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 2 — Row counts: both objects should agree
-- ─────────────────────────────────────────────────────────────────────────────

-- 2-A: customer — view and Delta table must agree
SELECT
    (SELECT COUNT(*) FROM lakehouse.lakehouse_db.vw_customer_latest)       AS customer_view_rows,
    (SELECT COUNT(*) FROM lakehouse.lakehouse_db.customer_snapshot_audit)  AS customer_table_rows;
-- ✅ Expected: both equal (1000 baseline, 1100 after 100-row insert)

-- 2-B: customer_orders — view and Delta table must agree
SELECT
    (SELECT COUNT(*) FROM lakehouse.lakehouse_db.vw_customer_orders_latest)       AS orders_view_rows,
    (SELECT COUNT(*) FROM lakehouse.lakehouse_db.customer_orders_snapshot_audit)  AS orders_table_rows;
-- ✅ Expected: both equal after notebook refresh


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 3 — Spot-check rows: first 10 and last 10
-- ─────────────────────────────────────────────────────────────────────────────

-- 3-A: First 10 rows from the VIEW
SELECT customer_id, full_name, email, city, customer_tier,
       ROUND(salary, 2) AS salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
ORDER  BY customer_id
LIMIT  10;
-- ✅ Expected: IDs 1–10, names and cities matching the seed=42 batch

-- 3-B: Last 10 rows from the VIEW (highest IDs)
SELECT customer_id, full_name, email, city, customer_tier,
       ROUND(salary, 2) AS salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
ORDER  BY customer_id DESC
LIMIT  10;
-- ✅ Expected: highest IDs present (1000 for base data; 1100 after the 100-row insert)

-- 3-C: Same spot-check on the Delta TABLE
SELECT customer_id, full_name, city, customer_tier, salary, refreshed_at
FROM   lakehouse.lakehouse_db.customer_snapshot_audit
ORDER  BY customer_id DESC
LIMIT  10;
-- ✅ Expected: matches 3-B, plus refreshed_at shows the UTC time of the last refresh


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 4 — Freshness checks: confirm latest batch is visible
-- ─────────────────────────────────────────────────────────────────────────────

-- 4-A: customer — new rows IDs 1095–1100 visible after the 100-row insert
SELECT customer_id, full_name, city, customer_tier, ROUND(salary,2) AS salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
WHERE  customer_id BETWEEN 1095 AND 1100
ORDER  BY customer_id;
-- ✅ Expected: 6 rows with IDs 1095–1100 (seed=99 names and cities)

-- 4-B: Same check on the customer Delta table
SELECT customer_id, full_name, city, customer_tier, refreshed_at
FROM   lakehouse.lakehouse_db.customer_snapshot_audit
WHERE  customer_id BETWEEN 1095 AND 1100
ORDER  BY customer_id;
-- ✅ Expected: same 6 rows plus a refreshed_at timestamp

-- 4-C: Insert batches distinguished by created_at date
SELECT
    DATE(created_at)   AS insert_date,
    MIN(customer_id)   AS first_id,
    MAX(customer_id)   AS last_id,
    COUNT(*)           AS row_count
FROM   lakehouse.lakehouse_db.vw_customer_latest
GROUP  BY DATE(created_at)
ORDER  BY insert_date;
-- ✅ Expected:
--   2026-01-xx  first_id=1    last_id=1000  row_count=1000  (seed=42, Jan 2026 dates)
--   2026-09-xx  first_id=1001 last_id=1100  row_count=100   (seed=99, Sep 2026 dates)


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 5 — Analytical queries: tier and salary distribution
-- ─────────────────────────────────────────────────────────────────────────────

-- 5-A: Tier distribution (VIEW)
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

-- 5-B: Tier distribution (Delta TABLE — must match 5-A after refresh)
SELECT customer_tier,
       COUNT(*)              AS cnt,
       ROUND(AVG(salary), 2) AS avg_salary
FROM   lakehouse.lakehouse_db.customer_snapshot_audit
GROUP  BY customer_tier
ORDER  BY cnt DESC;

-- 5-C: Salary percentile buckets
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

-- 5-D: Top 10 cities by customer count
SELECT city, country_code,
       COUNT(*)              AS customer_count,
       ROUND(AVG(salary), 2) AS avg_salary
FROM   lakehouse.lakehouse_db.vw_customer_latest
GROUP  BY city, country_code
ORDER  BY customer_count DESC
LIMIT  10;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 6 — Audit queries: Delta table history and last refresh timestamp
-- ─────────────────────────────────────────────────────────────────────────────

-- 6-A: customer — Delta table transaction history (one row per refresh run)
DESCRIBE HISTORY lakehouse.lakehouse_db.customer_snapshot_audit;
-- ✅ Expected: each WRITE operation corresponds to one nb_multi_table_auto_reader run
--    The most recent row: operation = "WRITE", operationMetrics shows numOutputRows.

-- 6-B: customer_orders — Delta table transaction history
DESCRIBE HISTORY lakehouse.lakehouse_db.customer_orders_snapshot_audit;

-- 6-C: When were the Delta tables last refreshed?
SELECT 'customer'        AS tbl, MAX(refreshed_at) AS last_refresh_utc
FROM   lakehouse.lakehouse_db.customer_snapshot_audit
UNION ALL
SELECT 'customer_orders' AS tbl, MAX(refreshed_at) AS last_refresh_utc
FROM   lakehouse.lakehouse_db.customer_orders_snapshot_audit
ORDER  BY tbl;
-- ✅ Expected: timestamps matching when Cells 3–5 of the unified notebook last ran.

-- 6-D: Confirm snap_file column — which parquet files are in each table?
SELECT tbl, snap_file, snap_file_size FROM (
    SELECT 'customer'        AS tbl, snap_file, snap_file_size
    FROM   lakehouse.lakehouse_db.customer_snapshot_audit
    UNION ALL
    SELECT 'customer_orders' AS tbl, snap_file, snap_file_size
    FROM   lakehouse.lakehouse_db.customer_orders_snapshot_audit
)
GROUP BY tbl, snap_file, snap_file_size
ORDER BY tbl, snap_file;
-- ✅ Expected: s3://stardata-databricks/iceberg/warehouse/lakehouse_db/<table>/data/*.parquet


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 7 — Negative / boundary tests  (customer table)
-- ─────────────────────────────────────────────────────────────────────────────

-- 7-A: No duplicate customer_ids (primary key check)
SELECT customer_id, COUNT(*) AS dup_count
FROM   lakehouse.lakehouse_db.vw_customer_latest
GROUP  BY customer_id
HAVING COUNT(*) > 1;
-- ✅ Expected: 0 rows (no duplicates)

-- 7-B: No NULL customer_ids
SELECT COUNT(*) AS null_ids
FROM   lakehouse.lakehouse_db.vw_customer_latest
WHERE  customer_id IS NULL;
-- ✅ Expected: 0

-- 7-C: All rows have a valid customer_tier
SELECT COUNT(*) AS invalid_tier_rows
FROM   lakehouse.lakehouse_db.vw_customer_latest
WHERE  customer_tier NOT IN ('standard', 'silver', 'gold', 'platinum');
-- ✅ Expected: 0

-- 7-D: Salary range sanity check
SELECT
    MIN(salary)  AS min_salary,
    MAX(salary)  AS max_salary,
    COUNT(*)     AS out_of_range
FROM   lakehouse.lakehouse_db.vw_customer_latest
WHERE  salary < 30000 OR salary > 200000;
-- ✅ Expected: out_of_range = 0  (all salaries generated in [30000, 200000])

-- 7-E: is_active only contains 0 or 1
SELECT DISTINCT is_active
FROM   lakehouse.lakehouse_db.vw_customer_latest
ORDER  BY is_active;
-- ✅ Expected: only 1 (all seed rows inserted as active)
