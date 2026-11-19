#!/bin/bash
echo "=== SHOW CATALOGS ==="
mysql -h 127.0.0.1 -P 9030 -u root -p"$DORIS_PASS" --connect-timeout=10 \
  -e "SHOW CATALOGS;" 2>&1 | grep -v Warning

echo ""
echo "=== Oracle: SHOW SCHEMAS FROM oracle_cache_testing ==="
mysql -h 127.0.0.1 -P 9030 -u root -p"$DORIS_PASS" --connect-timeout=10 \
  -e "SHOW DATABASES FROM oracle_cache_testing;" 2>&1 | grep -v Warning

echo ""
echo "=== Oracle: SHOW TABLES FROM oracle_cache_testing.CACHE_TESTING ==="
mysql -h 127.0.0.1 -P 9030 -u root -p"$DORIS_PASS" --connect-timeout=10 \
  -e "SHOW TABLES FROM oracle_cache_testing.CACHE_TESTING;" 2>&1 | grep -v Warning

echo ""
echo "=== PostgreSQL: SHOW SCHEMAS FROM pg_cache_testing ==="
mysql -h 127.0.0.1 -P 9030 -u root -p"$DORIS_PASS" --connect-timeout=10 \
  -e "SHOW DATABASES FROM pg_cache_testing;" 2>&1 | grep -v Warning

echo ""
echo "=== PostgreSQL: SHOW TABLES FROM pg_cache_testing.public ==="
mysql -h 127.0.0.1 -P 9030 -u root -p"$DORIS_PASS" --connect-timeout=10 \
  -e "SHOW TABLES FROM pg_cache_testing.public;" 2>&1 | grep -v Warning
