#!/bin/bash
SP=$DORIS_PASS

echo "=== Total customers by tier ==="
mysql -h 127.0.0.1 -P 9030 -u root -p"$SP" --connect-timeout=30 --table \
  -e "SELECT tier, COUNT(*) as cnt, ROUND(AVG(credit_limit),2) as avg_credit
      FROM mongodb.cache_testing.customers
      GROUP BY tier ORDER BY cnt DESC;" 2>&1 | grep -v Warning

echo ""
echo "=== Orders by status ==="
mysql -h 127.0.0.1 -P 9030 -u root -p"$SP" --connect-timeout=30 --table \
  -e "SELECT status, COUNT(*) as cnt, ROUND(SUM(total_amount),2) as total_value
      FROM mongodb.cache_testing.orders
      GROUP BY status ORDER BY cnt DESC;" 2>&1 | grep -v Warning

echo ""
echo "=== Top 5 products by unit_price ==="
mysql -h 127.0.0.1 -P 9030 -u root -p"$SP" --connect-timeout=30 --table \
  -e "SELECT sku, product_name, category, unit_price
      FROM mongodb.cache_testing.products
      ORDER BY unit_price DESC LIMIT 5;" 2>&1 | grep -v Warning

echo ""
echo "=== Inventory events by type ==="
mysql -h 127.0.0.1 -P 9030 -u root -p"$SP" --connect-timeout=30 --table \
  -e "SELECT event_type, COUNT(*) as cnt, SUM(delta_qty) as total_delta
      FROM mongodb.cache_testing.inventory_events
      GROUP BY event_type ORDER BY cnt DESC;" 2>&1 | grep -v Warning
