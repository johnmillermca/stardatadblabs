SET FEEDBACK ON SERVEROUTPUT ON SIZE UNLIMITED
-- Add primary keys AFTER data load for maximum insert performance
-- (no index maintenance overhead during bulk load)

ALTER TABLE customers       ADD CONSTRAINT pk_customers       PRIMARY KEY (customer_id);
ALTER TABLE products        ADD CONSTRAINT pk_products        PRIMARY KEY (product_id);
ALTER TABLE orders          ADD CONSTRAINT pk_orders          PRIMARY KEY (order_id);
ALTER TABLE order_items     ADD CONSTRAINT pk_order_items     PRIMARY KEY (item_id);
ALTER TABLE product_reviews ADD CONSTRAINT pk_product_reviews PRIMARY KEY (review_id);
ALTER TABLE inventory_events ADD CONSTRAINT pk_inventory_events PRIMARY KEY (event_id);

-- Unique constraints
ALTER TABLE products  ADD CONSTRAINT uq_products_sku   UNIQUE (sku);
ALTER TABLE customers ADD CONSTRAINT uq_customers_email UNIQUE (email);

-- Supporting indexes for FK-like join columns (no FK constraints — data is random)
CREATE INDEX idx_orders_customer_id    ON orders          (customer_id);
CREATE INDEX idx_orders_status_date    ON orders          (status, order_date);
CREATE INDEX idx_orders_date           ON orders          (order_date);
CREATE INDEX idx_items_order_id        ON order_items     (order_id);
CREATE INDEX idx_items_product_id      ON order_items     (product_id);
CREATE INDEX idx_reviews_product_id    ON product_reviews (product_id);
CREATE INDEX idx_reviews_customer_id   ON product_reviews (customer_id);
CREATE INDEX idx_reviews_rating        ON product_reviews (product_id, rating);
CREATE INDEX idx_inv_product_id        ON inventory_events (product_id);
CREATE INDEX idx_inv_event_at          ON inventory_events (event_at);
CREATE INDEX idx_inv_warehouse_date    ON inventory_events (warehouse_id, event_at);
CREATE INDEX idx_products_category     ON products        (category, subcategory);
CREATE INDEX idx_products_active_price ON products        (is_active, unit_price);
CREATE INDEX idx_customers_tier        ON customers       (tier);
CREATE INDEX idx_customers_country     ON customers       (country_code, city);

COMMIT;
EXIT;
