#!/bin/bash
SP=$DORIS_PASS

echo "=== REFRESH CATALOG ==="
mysql -h 127.0.0.1 -P 9030 -u root -p"$SP" --connect-timeout=30 \
  -e "REFRESH CATALOG mongodb;" 2>&1 | grep -v Warning

echo "=== SELECT from customers ==="
mysql -h 127.0.0.1 -P 9030 -u root -p"$SP" --connect-timeout=30 \
  -e "SELECT * FROM mongodb.cache_testing.customers LIMIT 3;" 2>&1 | grep -v Warning

echo "=== SELECT from orders ==="
mysql -h 127.0.0.1 -P 9030 -u root -p"$SP" --connect-timeout=30 \
  -e "SELECT * FROM mongodb.cache_testing.orders LIMIT 3;" 2>&1 | grep -v Warning

echo "=== SELECT from products ==="
mysql -h 127.0.0.1 -P 9030 -u root -p"$SP" --connect-timeout=30 \
  -e "SELECT * FROM mongodb.cache_testing.products LIMIT 3;" 2>&1 | grep -v Warning
