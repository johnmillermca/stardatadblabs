# Runbook 14 — Cache Testing Database Access

> **Related runbooks:** [12 — New User Setup](runbook-12-rbac-new-user-testing.md) · [13 — User Groups and Access Testing](runbook-13-user-groups-and-access-testing.md)

This runbook covers how to connect to, inspect, and query the **cache testing** dataset loaded into Oracle XE, MongoDB, and PostgreSQL. The dataset models an e-commerce order management system and is purpose-built to stress database caching layers (buffer pool, page cache, LRU eviction) with realistic data volumes.

---

## Dataset overview

| Database    | Schema / DB     | Tables / Collections | Approximate Size |
|-------------|-----------------|----------------------|-----------------|
| Oracle XE   | `CACHE_TESTING` | 6 tables             | ~9.6 GB         |
| MongoDB     | `cache_testing` | 6 collections        | ~10.4 GB data   |
| PostgreSQL  | `cache_testing` | 6 tables             | ~9.1 GB         |

### Table / Collection structure

| Name                | Oracle rows | MongoDB docs | PostgreSQL rows | Description                               |
|---------------------|-------------|--------------|-----------------|-------------------------------------------|
| `customers`         | 2,000,000   | 2,000,000    | ~2,000,000      | Customer profiles with tier + geo         |
| `products`          | 500,000     | 400,000      | ~500,000        | Product catalogue with category/price     |
| `orders`            | 3,000,000   | 2,500,000    | ~10,000,000     | Orders linking customers to line items    |
| `order_items`       | 7,000,000   | 3,000,000    | ~39,000,000     | Line items linking orders to products     |
| `product_reviews`   | 4,000,000   | 7,500,000    | ~5,000,000      | Reviews with large text (main size driver)|
| `inventory_events`  | 3,000,000   | 2,000,000    | ~10,000,000     | Stock movement events per warehouse       |

### Oracle primary keys and indexes

| Table               | Primary Key       | Supporting indexes                                        |
|---------------------|-------------------|-----------------------------------------------------------|
| `customers`         | `customer_id`     | `email` (unique), `tier`, `country_code+city`             |
| `products`          | `product_id`      | `sku` (unique), `category+subcategory`, `is_active+price` |
| `orders`            | `order_id`        | `customer_id`, `status+order_date`, `order_date`         |
| `order_items`       | `item_id`         | `order_id`, `product_id`                                  |
| `product_reviews`   | `review_id`       | `product_id`, `customer_id`, `product_id+rating`          |
| `inventory_events`  | `event_id`        | `product_id`, `event_at`, `warehouse_id+event_at`         |

### PostgreSQL primary keys and indexes

> **Note:** PostgreSQL uses `id` (bigserial) as the PK column name on every table — unlike Oracle, which uses entity-specific names such as `customer_id`, `order_id`, etc.

| Table               | Primary Key | Supporting indexes                                              |
|---------------------|-------------|-----------------------------------------------------------------|
| `customers`         | `id`        | `email` (unique), `tier`, `created_at`                         |
| `products`          | `id`        | `sku` (unique), `category`, `price`                            |
| `orders`            | `id`        | `customer_id` (FK), `status`, `created_at`                     |
| `order_items`       | `id`        | `order_id` (FK), `product_id` (FK)                             |
| `product_reviews`   | `id`        | `product_id` (FK), `customer_id` (FK), `rating`, `created_at` |
| `inventory_events`  | `id`        | `product_id` (FK), `event_type`, `warehouse_id`, `event_at`   |

---

## Prerequisites

```bash
# Retrieve credentials
ORA_PASS=$(kubectl get secret oracle-credentials -n prod \
  -o jsonpath='{.data.oracle-password}' | base64 -d)
MONGO_PASS=$(kubectl get secret mongodb-credentials -n prod \
  -o jsonpath='{.data.mongodb-root-password}' | base64 -d)
PG_PASS=$(kubectl get secret postgresql-credentials -n prod \
  -o jsonpath='{.data.postgres-password}' | base64 -d)

echo "Oracle     : ${ORA_PASS}"
echo "MongoDB    : ${MONGO_PASS}"
echo "PostgreSQL : ${PG_PASS}"

# Pod / StatefulSet names (verify these match your cluster)
kubectl get pods -n prod | grep -E "oracle|mongo|postgresql"
```

---

## Oracle XE — `CACHE_TESTING` schema

### Connection options

#### Option A — kubectl exec (no local client required)

```bash
ORA_PASS=$(kubectl get secret oracle-credentials -n prod \
  -o jsonpath='{.data.oracle-password}' | base64 -d)
ORA_POD=$(kubectl get pod -n prod -l app=oracle-xe \
  -o jsonpath='{.items[0].metadata.name}')

# Connect as CACHE_TESTING user
kubectl exec -n prod ${ORA_POD} -- \
  sqlplus cache_testing/CacheTesting#2025@localhost:1521/XEPDB1

# Connect as DBA (for schema inspection)
kubectl exec -n prod ${ORA_POD} -- \
  sqlplus sys/${ORA_PASS}@localhost:1521/XEPDB1 as sysdba
```

#### Option B — Direct NodePort (requires SQL*Plus installed locally)

```bash
sqlplus cache_testing/CacheTesting#2025@192.168.1.50:30521/XEPDB1
```

### List tables and sizes

```sql
-- List all tables with row counts and sizes
SELECT t.table_name,
       t.num_rows,
       ROUND(s.bytes/1024/1024, 1) size_mb
FROM   user_tables t
JOIN   user_segments s ON s.segment_name = t.table_name
ORDER  BY s.bytes DESC;
```

Expected output:
```
TABLE_NAME           NUM_ROWS   SIZE_MB
-------------------- ---------- -------
PRODUCT_REVIEWS       4000000   7083.0
ORDERS                3000000    304.0
ORDER_ITEMS           7000000    256.0
CUSTOMERS             2000000    256.0
INVENTORY_EVENTS      3000000    240.0
PRODUCTS               500000    120.0
```

```sql
-- Total schema size
SELECT ROUND(SUM(bytes)/1024/1024/1024, 2) || ' GB' total_size
FROM   user_segments;
```

### Describe table structure

```sql
-- Column definitions
DESC customers;
DESC products;
DESC orders;
DESC order_items;
DESC product_reviews;
DESC inventory_events;

-- Primary keys and constraints
SELECT constraint_name, constraint_type, column_name
FROM   user_cons_columns
JOIN   user_constraints USING (constraint_name, table_name)
WHERE  table_name = 'ORDERS'
ORDER  BY constraint_type, position;

-- All indexes
SELECT index_name, table_name, uniqueness,
       LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY column_position) columns
FROM   user_ind_columns
JOIN   user_indexes USING (index_name, table_name)
GROUP  BY index_name, table_name, uniqueness
ORDER  BY table_name, index_name;
```

### Example queries

#### 1 — Single customer lookup (PK point query — cache hot key test)

```sql
SELECT customer_id, first_name, last_name, email, tier, credit_limit
FROM   customers
WHERE  customer_id = 12345;
```

#### 2 — Customer orders with totals (join + aggregation)

```sql
SELECT c.first_name || ' ' || c.last_name  customer_name,
       c.tier,
       COUNT(o.order_id)                   total_orders,
       ROUND(SUM(o.total_amount), 2)        lifetime_value,
       MAX(o.order_date)                   last_order
FROM   customers  c
JOIN   orders     o ON o.customer_id = c.customer_id
WHERE  c.customer_id BETWEEN 1 AND 1000
GROUP  BY c.customer_id, c.first_name, c.last_name, c.tier
ORDER  BY lifetime_value DESC
FETCH  FIRST 20 ROWS ONLY;
```

#### 3 — Orders by status (range scan on indexed column)

```sql
SELECT status,
       COUNT(*)                      order_count,
       ROUND(AVG(total_amount), 2)   avg_value,
       ROUND(SUM(total_amount), 2)   total_value
FROM   orders
WHERE  order_date >= SYSDATE - 90
GROUP  BY status
ORDER  BY order_count DESC;
```

#### 4 — Order details with product info (3-table join)

```sql
SELECT o.order_id,
       o.status,
       o.order_date,
       p.product_name,
       p.category,
       oi.quantity,
       oi.unit_price,
       oi.line_total
FROM   orders      o
JOIN   order_items oi ON oi.order_id   = o.order_id
JOIN   products    p  ON p.product_id  = oi.product_id
WHERE  o.order_id = 500000
ORDER  BY oi.item_id;
```

#### 5 — Top products by revenue (aggregation + sort)

```sql
SELECT p.product_name,
       p.category,
       COUNT(oi.item_id)             times_ordered,
       SUM(oi.quantity)              units_sold,
       ROUND(SUM(oi.line_total), 2)  total_revenue
FROM   products    p
JOIN   order_items oi ON oi.product_id = p.product_id
GROUP  BY p.product_id, p.product_name, p.category
ORDER  BY total_revenue DESC
FETCH  FIRST 10 ROWS ONLY;
```

#### 6 — Product reviews with average rating (index scan)

```sql
SELECT p.product_name,
       p.category,
       COUNT(r.review_id)              review_count,
       ROUND(AVG(r.rating), 2)         avg_rating,
       SUM(r.helpful_cnt)              total_helpful
FROM   products        p
JOIN   product_reviews r ON r.product_id = p.product_id
WHERE  p.category = 'Electronics'
GROUP  BY p.product_id, p.product_name, p.category
HAVING COUNT(r.review_id) >= 5
ORDER  BY avg_rating DESC, review_count DESC
FETCH  FIRST 15 ROWS ONLY;
```

#### 7 — Current inventory per product (event sourcing)

```sql
SELECT p.product_name,
       p.category,
       p.stock_qty  initial_stock,
       NVL(SUM(ie.delta_qty), 0) net_movement,
       p.stock_qty + NVL(SUM(ie.delta_qty), 0) current_stock
FROM   products         p
LEFT JOIN inventory_events ie ON ie.product_id = p.product_id
WHERE  p.is_active = 'Y'
  AND  p.category  = 'Sports'
GROUP  BY p.product_id, p.product_name, p.category, p.stock_qty
ORDER  BY current_stock ASC
FETCH  FIRST 20 ROWS ONLY;
```

#### 8 — Warehouse activity summary (range scan on event_at)

```sql
SELECT warehouse_id,
       event_type,
       COUNT(*)                   event_count,
       SUM(CASE WHEN delta_qty > 0 THEN delta_qty ELSE 0 END)  total_in,
       SUM(CASE WHEN delta_qty < 0 THEN ABS(delta_qty) ELSE 0 END) total_out
FROM   inventory_events
WHERE  event_at >= SYSTIMESTAMP - INTERVAL '30' DAY
GROUP  BY warehouse_id, event_type
ORDER  BY warehouse_id, event_type;
```

#### 9 — Gold/Platinum customers who haven't ordered in 60 days

```sql
SELECT c.customer_id,
       c.first_name || ' ' || c.last_name  name,
       c.email,
       c.tier,
       MAX(o.order_date)   last_order_date,
       ROUND(SYSDATE - MAX(o.order_date)) days_since_last
FROM   customers c
JOIN   orders    o ON o.customer_id = c.customer_id
WHERE  c.tier IN ('GOLD','PLATINUM')
  AND  c.is_active = 'Y'
GROUP  BY c.customer_id, c.first_name, c.last_name, c.email, c.tier
HAVING MAX(o.order_date) < SYSDATE - 60
ORDER  BY days_since_last DESC
FETCH  FIRST 25 ROWS ONLY;
```

#### 10 — Execution plan inspection (for cache tuning)

```sql
-- Show the execution plan for any query
EXPLAIN PLAN FOR
  SELECT * FROM orders WHERE customer_id = 99999;
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

-- Check buffer cache hit ratio
SELECT ROUND(1 - (phy.value / (cur.value + con.value)), 4) * 100 || '%' hit_ratio
FROM   v$sysstat phy, v$sysstat cur, v$sysstat con
WHERE  phy.name = 'physical reads'
  AND  cur.name = 'db block gets'
  AND  con.name = 'consistent gets';
```

---

## MongoDB — `cache_testing` database

### Connection options

#### Option A — kubectl exec (mongosh)

```bash
MONGO_PASS=$(kubectl get secret mongodb-credentials -n prod \
  -o jsonpath='{.data.mongodb-root-password}' | base64 -d)

kubectl exec -n prod mongodb-0 -- \
  mongosh --username root \
          --password "${MONGO_PASS}" \
          --authenticationDatabase admin \
          --host localhost \
          cache_testing
```

#### Option B — Direct NodePort

```bash
mongosh "mongodb://root:${MONGO_PASS}@192.168.1.50:30017/cache_testing?authSource=admin"
```

### List collections and sizes

```javascript
// List all collections
show collections

// Collection stats (sizes and doc counts)
db.getCollectionNames().forEach(function(c) {
  var s = db[c].stats(1024*1024);
  print(c.padEnd(20) + s.count + " docs  " + s.size.toFixed(0) + " MB");
});

// Full database stats
db.stats(1024*1024*1024)
```

Expected output:
```
customers            2000000 docs  538 MB
products              400000 docs  114 MB
orders               2500000 docs  ~700 MB
order_items          3000000 docs  346 MB
product_reviews      7500000 docs  ~8500 MB
inventory_events     2000000 docs  355 MB
```

### List indexes per collection

```javascript
// Show indexes on every collection
db.getCollectionNames().forEach(function(c) {
  print("\n--- " + c + " ---");
  db[c].getIndexes().forEach(function(idx) {
    print("  " + idx.name + ": " + JSON.stringify(idx.key) +
          (idx.unique ? " [unique]" : ""));
  });
});
```

### Example queries

#### 1 — Single document lookup by field index (hot key test)

```javascript
// By customer_id (unique index — O(log n))
db.customers.findOne({ customer_id: 12345 })

// By email (unique index)
db.customers.findOne({ email: "user12345@ex45.com" })
```

#### 2 — Customer order history (multi-collection join via $lookup)

```javascript
db.customers.aggregate([
  { $match: { customer_id: 12345 } },
  { $lookup: {
      from: "orders",
      localField: "customer_id",
      foreignField: "customer_id",
      as: "orders"
  }},
  { $project: {
      _id: 0,
      customer_id: 1,
      first_name: 1,
      last_name: 1,
      tier: 1,
      order_count: { $size: "$orders" },
      lifetime_value: { $sum: "$orders.total_amount" },
      last_order: { $max: "$orders.order_date" }
  }}
])
```

#### 3 — Orders by status in last 90 days (range scan + group)

```javascript
var cutoff = new Date(Date.now() - 90*24*60*60*1000);
db.orders.aggregate([
  { $match: { order_date: { $gte: cutoff } } },
  { $group: {
      _id: "$status",
      count: { $sum: 1 },
      avg_value: { $avg: "$total_amount" },
      total_value: { $sum: "$total_amount" }
  }},
  { $sort: { count: -1 } }
])
```

#### 4 — Top products by order frequency (cross-collection aggregation)

```javascript
db.order_items.aggregate([
  { $group: {
      _id: "$product_id",
      times_ordered: { $sum: 1 },
      units_sold: { $sum: "$quantity" },
      total_revenue: { $sum: { $multiply: ["$quantity","$unit_price"] } }
  }},
  { $sort: { total_revenue: -1 } },
  { $limit: 10 },
  { $lookup: {
      from: "products",
      localField: "_id",
      foreignField: "product_id",
      as: "product"
  }},
  { $project: {
      product_name: { $arrayElemAt: ["$product.product_name", 0] },
      category:     { $arrayElemAt: ["$product.category", 0] },
      times_ordered: 1, units_sold: 1,
      total_revenue: { $round: ["$total_revenue", 2] }
  }}
])
```

#### 5 — Product reviews with average rating by category

```javascript
db.product_reviews.aggregate([
  { $lookup: {
      from: "products",
      localField: "product_id",
      foreignField: "product_id",
      as: "product"
  }},
  { $unwind: "$product" },
  { $group: {
      _id: "$product.category",
      review_count: { $sum: 1 },
      avg_rating: { $avg: "$rating" },
      pct_5star: { $avg: { $cond: [{ $eq: ["$rating", 5] }, 1, 0] } }
  }},
  { $project: {
      category: "$_id",
      review_count: 1,
      avg_rating: { $round: ["$avg_rating", 2] },
      pct_5star: { $round: [{ $multiply: ["$pct_5star", 100] }, 1] }
  }},
  { $sort: { avg_rating: -1 } }
])
```

#### 6 — Inventory position per warehouse (event sourcing)

```javascript
db.inventory_events.aggregate([
  { $match: { warehouse_id: 1 } },
  { $group: {
      _id: { product_id: "$product_id", event_type: "$event_type" },
      total_delta: { $sum: "$delta_qty" },
      event_count: { $sum: 1 }
  }},
  { $group: {
      _id: "$_id.product_id",
      net_movement: { $sum: "$total_delta" },
      receipt_qty:  { $sum: { $cond: [{ $eq: ["$_id.event_type","RECEIPT"] }, "$total_delta", 0] } },
      sale_qty:     { $sum: { $cond: [{ $eq: ["$_id.event_type","SALE"] }, "$total_delta", 0] } }
  }},
  { $sort: { net_movement: 1 } },
  { $limit: 20 }
])
```

#### 7 — Full-text style search on review text (regex index scan)

```javascript
// Find reviews mentioning a keyword (useful for cache miss simulation)
db.product_reviews.find(
  { review_text: /cache testing/i, rating: { $gte: 4 } },
  { review_id: 1, product_id: 1, rating: 1, title: 1 }
).limit(10)
```

#### 8 — Customer tier distribution (collection scan + group)

```javascript
db.customers.aggregate([
  { $group: {
      _id: "$tier",
      count: { $sum: 1 },
      avg_credit: { $avg: "$credit_limit" },
      countries:  { $addToSet: "$country_code" }
  }},
  { $project: {
      tier: "$_id",
      count: 1,
      avg_credit: { $round: ["$avg_credit", 2] },
      country_count: { $size: "$countries" }
  }},
  { $sort: { count: -1 } }
])
```

#### 9 — Recent high-value orders (covered index query)

```javascript
var cutoff = new Date(Date.now() - 30*24*60*60*1000);
db.orders.find(
  { order_date: { $gte: cutoff }, total_amount: { $gt: 1000 }, status: "DELIVERED" },
  { order_id: 1, customer_id: 1, total_amount: 1, order_date: 1, _id: 0 }
).sort({ total_amount: -1 }).limit(25)
```

#### 10 — Explain plan (for cache and index tuning)

```javascript
// View index usage for any query
db.orders.find({ customer_id: 99999 }).explain("executionStats")

// Check which index was used
db.orders.find({ customer_id: 99999 }).explain().queryPlanner.winningPlan

// Collection-level stats for cache analysis
db.orders.stats()
```

---

## PostgreSQL — `cache_testing` database

### Connection options

#### Option A — kubectl exec (no local client required)

```bash
PG_PASS=$(kubectl get secret postgresql-credentials -n prod \
  -o jsonpath='{.data.postgres-password}' | base64 -d)

# Interactive psql shell
kubectl exec -it -n prod statefulset/postgresql -- \
  psql -U postgres -d cache_testing

# One-shot query
kubectl exec -n prod statefulset/postgresql -- \
  psql -U postgres -d cache_testing -c "SELECT COUNT(*) FROM customers;"
```

#### Option B — Direct NodePort (requires psql installed locally)

```bash
PG_PASS=$(kubectl get secret postgresql-credentials -n prod \
  -o jsonpath='{.data.postgres-password}' | base64 -d)

psql -h 192.168.1.50 -p 30432 -U postgres -d cache_testing
# Enter password when prompted, or:
PGPASSWORD="${PG_PASS}" psql -h 192.168.1.50 -p 30432 -U postgres -d cache_testing
```

---

### List tables and sizes

```sql
-- Table sizes including all indexes
SELECT
    relname                                          AS table_name,
    pg_size_pretty(pg_total_relation_size(oid))      AS total_size,
    pg_size_pretty(pg_relation_size(oid))            AS table_size,
    pg_size_pretty(pg_total_relation_size(oid)
                   - pg_relation_size(oid))          AS index_size,
    to_char(reltuples::bigint, 'FM999,999,999')      AS est_rows
FROM pg_class
WHERE relkind = 'r'
  AND relnamespace = 'public'::regnamespace
ORDER BY pg_total_relation_size(oid) DESC;
```

Expected output:

```
    table_name     | total_size | table_size | index_size |   est_rows
-------------------+------------+------------+------------+------------
 order_items       | 4597 MB    | 3102 MB    | 1495 MB    | 39,000,628
 orders            | 1501 MB    | 1023 MB    |  478 MB    | 10,000,017
 inventory_events  | 1274 MB    |  834 MB    |  440 MB    | 10,000,371
 product_reviews   | 1159 MB    |  858 MB    |  301 MB    |  5,000,009
 customers         |  457 MB    |  244 MB    |  213 MB    |  1,999,995
 products          |  100 MB    |   56 MB    |   44 MB    |    500,000
```

---

### Describe table structure

```sql
-- All columns across all six tables
SELECT
    c.table_name,
    c.column_name,
    c.data_type,
    c.character_maximum_length  AS max_len,
    c.is_nullable,
    c.column_default
FROM information_schema.columns c
WHERE c.table_schema = 'public'
  AND c.table_name IN (
    'customers','products','orders',
    'order_items','product_reviews','inventory_events'
  )
ORDER BY c.table_name, c.ordinal_position;
```

Quick per-table reference (from live cluster):

**customers**
| Column | Type |
|--------|------|
| `id` | `bigint` PK (serial) |
| `name` | `varchar(120)` |
| `email` | `varchar(180)` unique |
| `phone` | `varchar(30)` |
| `address` | `text` |
| `tier` | `varchar(20)` default `'standard'` |
| `created_at` | `timestamptz` |
| `updated_at` | `timestamptz` |

**products**
| Column | Type |
|--------|------|
| `id` | `bigint` PK (serial) |
| `sku` | `varchar(60)` unique |
| `name` | `varchar(200)` |
| `category` | `varchar(80)` |
| `price` | `numeric(12,2)` |
| `stock_qty` | `integer` default `0` |
| `weight_kg` | `numeric(8,3)` |
| `created_at` | `timestamptz` |

**orders**
| Column | Type |
|--------|------|
| `id` | `bigint` PK (serial) |
| `customer_id` | `bigint` FK → `customers.id` |
| `status` | `varchar(30)` default `'pending'` |
| `total_amount` | `numeric(14,2)` |
| `shipping_address` | `text` |
| `created_at` | `timestamptz` |
| `updated_at` | `timestamptz` |

**order_items**
| Column | Type |
|--------|------|
| `id` | `bigint` PK (serial) |
| `order_id` | `bigint` FK → `orders.id` |
| `product_id` | `bigint` FK → `products.id` |
| `qty` | `integer` default `1` |
| `unit_price` | `numeric(12,2)` |
| `discount_pct` | `numeric(5,2)` default `0` |

**product_reviews**
| Column | Type |
|--------|------|
| `id` | `bigint` PK (serial) |
| `product_id` | `bigint` FK → `products.id` |
| `customer_id` | `bigint` FK → `customers.id` |
| `rating` | `smallint` check (1–5) |
| `review_text` | `text` |
| `created_at` | `timestamptz` |

**inventory_events**
| Column | Type |
|--------|------|
| `id` | `bigint` PK (serial) |
| `product_id` | `bigint` FK → `products.id` |
| `event_type` | `varchar(40)` |
| `delta_qty` | `integer` |
| `warehouse_id` | `integer` |
| `event_at` | `timestamptz` |

---

### Example queries

#### 1 — Single customer lookup (PK point query — cache hot key test)

```sql
-- Direct PK hit — confirm index-only scan after warm-up
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, name, email, tier, created_at
FROM customers
WHERE id = 500000;
```

#### 2 — Customer order history with totals (join + aggregation)

```sql
SELECT
    c.id          AS customer_id,
    c.name,
    c.tier,
    COUNT(o.id)               AS order_count,
    SUM(o.total_amount)       AS lifetime_value,
    MAX(o.created_at)         AS last_order_at
FROM customers c
JOIN orders o ON o.customer_id = c.id
WHERE c.id = 500000
GROUP BY c.id, c.name, c.tier;
```

#### 3 — Orders by status in the last 30 days (range scan on indexed column)

```sql
SELECT
    status,
    COUNT(*)                              AS order_count,
    ROUND(AVG(total_amount)::numeric, 2)  AS avg_amount,
    SUM(total_amount)                     AS total_revenue
FROM orders
WHERE created_at >= NOW() - INTERVAL '30 days'
GROUP BY status
ORDER BY order_count DESC;
```

#### 4 — Order line items with product detail (3-table join)

```sql
SELECT
    o.id          AS order_id,
    o.status,
    p.name        AS product_name,
    p.category,
    oi.qty,
    oi.unit_price,
    oi.discount_pct,
    ROUND((oi.unit_price * oi.qty * (1 - oi.discount_pct / 100))::numeric, 2)
                  AS line_total
FROM orders o
JOIN order_items oi ON oi.order_id = o.id
JOIN products    p  ON p.id        = oi.product_id
WHERE o.id = 1000000
ORDER BY oi.id;
```

#### 5 — Top 10 products by revenue (aggregation + sort)

```sql
SELECT
    p.id,
    p.name,
    p.category,
    SUM(oi.qty)                                               AS units_sold,
    ROUND(SUM(oi.unit_price * oi.qty
              * (1 - oi.discount_pct / 100))::numeric, 2)    AS revenue
FROM order_items oi
JOIN products p ON p.id = oi.product_id
GROUP BY p.id, p.name, p.category
ORDER BY revenue DESC
LIMIT 10;
```

#### 6 — Product reviews with average rating (index scan on product_id)

```sql
SELECT
    p.name                                    AS product_name,
    p.category,
    COUNT(r.id)                               AS review_count,
    ROUND(AVG(r.rating)::numeric, 2)          AS avg_rating,
    COUNT(r.id) FILTER (WHERE r.rating = 5)   AS five_star
FROM product_reviews r
JOIN products p ON p.id = r.product_id
WHERE r.product_id = 250000
GROUP BY p.id, p.name, p.category;
```

#### 7 — Current inventory per product (event sourcing — sum of deltas)

```sql
SELECT
    p.id,
    p.name,
    p.sku,
    SUM(ie.delta_qty)   AS current_stock,
    COUNT(ie.id)        AS event_count
FROM inventory_events ie
JOIN products p ON p.id = ie.product_id
WHERE ie.product_id = 250000
GROUP BY p.id, p.name, p.sku;
```

#### 8 — Warehouse activity summary for the past 7 days (range scan on event_at)

```sql
SELECT
    warehouse_id,
    event_type,
    COUNT(*)          AS event_count,
    SUM(delta_qty)    AS net_qty_change
FROM inventory_events
WHERE event_at >= NOW() - INTERVAL '7 days'
GROUP BY warehouse_id, event_type
ORDER BY warehouse_id, event_type;
```

#### 9 — High-value customers with no orders in 90 days (left join + filter)

```sql
SELECT
    c.id,
    c.name,
    c.email,
    c.tier,
    MAX(o.created_at)  AS last_order_at
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id
WHERE c.tier IN ('gold', 'platinum')
GROUP BY c.id, c.name, c.email, c.tier
HAVING MAX(o.created_at) < NOW() - INTERVAL '90 days'
    OR MAX(o.created_at) IS NULL
ORDER BY last_order_at ASC NULLS FIRST
LIMIT 50;
```

#### 10 — Execution plan inspection (for cache and shared-buffer tuning)

```sql
-- EXPLAIN ANALYZE shows actual rows, loops, and buffer hits vs misses
-- Run twice: first cold (cache miss), second warm (shared buffer hit)
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    c.tier,
    COUNT(DISTINCT c.id)              AS customers,
    COUNT(o.id)                       AS orders,
    ROUND(AVG(o.total_amount)::numeric, 2) AS avg_order_value
FROM customers c
JOIN orders o ON o.customer_id = c.id
GROUP BY c.tier
ORDER BY avg_order_value DESC;

-- Look for "Buffers: shared hit=N read=M" in the output.
-- After the first run, re-running should show read=0 (all hits from shared_buffers).
```

---

## Cache testing patterns

These queries are specifically designed to exercise different caching scenarios:

### Hot key lookup — repeat the same query to measure cache warm-up

```sql
-- Oracle: run 3× and compare elapsed time
SET TIMING ON
SELECT * FROM customers WHERE customer_id = 100000;
-- First run: physical I/O (cache cold)
-- Second/third run: buffer cache hit (should be ~10× faster)
```

```javascript
// MongoDB: use explain to see docsExamined drop after warm-up
db.customers.find({ customer_id: 100000 }).explain("executionStats")
```

```sql
-- PostgreSQL: use EXPLAIN (ANALYZE, BUFFERS) and watch shared hit vs read
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM customers WHERE id = 100000;
-- First run: "Buffers: shared read=N"  → disk I/O
-- Repeat:    "Buffers: shared hit=N"   → served from shared_buffers (cache)
```

### Range scan — simulates time-series dashboard queries

```sql
-- Oracle
SELECT COUNT(*), AVG(total_amount)
FROM orders
WHERE order_date BETWEEN SYSDATE-7 AND SYSDATE;
```

```javascript
// MongoDB
var last7 = new Date(Date.now() - 7*24*60*60*1000);
db.orders.aggregate([
  { $match: { order_date: { $gte: last7 } } },
  { $group: { _id: null, count: { $sum: 1 }, avg: { $avg: "$total_amount" } } }
])
```

```sql
-- PostgreSQL
SELECT COUNT(*), ROUND(AVG(total_amount)::numeric, 2) AS avg_amount
FROM orders
WHERE created_at >= NOW() - INTERVAL '7 days';
```

### Cold-start test — flush buffer pool then query

```sql
-- Oracle (requires DBA): flush buffer cache then re-run query
ALTER SYSTEM FLUSH BUFFER_CACHE;  -- as sysdba
SELECT * FROM product_reviews WHERE product_id = 250000;
```

```javascript
// MongoDB: check storageSize vs dataSize ratio to infer compression/cache efficiency
db.product_reviews.stats(1024*1024)
```

```sql
-- PostgreSQL: discard shared_buffers content (requires superuser + restart)
-- In production use pg_prewarm extension to warm selectively instead.
-- To simulate cold start: restart PostgreSQL pod
kubectl rollout restart statefulset/postgresql -n prod
-- Then immediately run your query and observe "shared read=" in EXPLAIN BUFFERS output
```

### Full aggregation scan — exercises sequential read path

```sql
-- Oracle: full table scan of largest table
SELECT rating, COUNT(*), ROUND(AVG(helpful_cnt), 1)
FROM product_reviews
GROUP BY rating ORDER BY rating;
```

```javascript
// MongoDB: groupBy on unindexed field (forces collection scan)
db.product_reviews.aggregate([
  { $group: { _id: "$rating", count: { $sum: 1 }, avg_helpful: { $avg: "$helpful_cnt" } } },
  { $sort: { _id: 1 } }
])
```

```sql
-- PostgreSQL: sequential scan of the largest table (order_items, ~4.6 GB)
-- Disable index scans temporarily to force seqscan and measure effective_cache_size impact
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT rating, COUNT(*), ROUND(AVG(rating)::numeric, 2) AS avg_rating
FROM product_reviews
GROUP BY rating
ORDER BY rating;
-- Reset
RESET enable_indexscan;
RESET enable_bitmapscan;
```

---

## Connection summary

| Database   | Internal URL                                    | NodePort               | User            | Password source                    |
|------------|-------------------------------------------------|------------------------|-----------------|------------------------------------|
| Oracle XE  | `oracle-xe.prod.svc.cluster.local:1521/XEPDB1`  | `192.168.1.50:30521`   | `cache_testing` | `CacheTesting#2025` (fixed)        |
| MongoDB    | `mongodb.prod.svc.cluster.local:27017`          | `192.168.1.50:30017`   | `root`          | `mongodb-credentials` secret       |
| PostgreSQL | `postgresql.prod.svc.cluster.local:5432`        | `192.168.1.50:30432`   | `postgres`      | `postgresql-credentials` secret    |
