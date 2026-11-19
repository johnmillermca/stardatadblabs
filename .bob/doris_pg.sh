#!/bin/bash
# Called with DORIS_PASS and PG_PASS env vars set
mysql -h 127.0.0.1 -P 9030 -u root -p"$DORIS_PASS" --connect-timeout=10 \
  -e "DROP CATALOG IF EXISTS pg_cache_testing;" 2>&1 | grep -v Warning

mysql -h 127.0.0.1 -P 9030 -u root -p"$DORIS_PASS" --connect-timeout=10 \
  -e "CREATE CATALOG pg_cache_testing PROPERTIES (
  'type'         = 'jdbc',
  'user'         = 'postgres',
  'password'     = '$PG_PASS',
  'jdbc_url'     = 'jdbc:postgresql://postgresql.prod.svc.cluster.local:5432/cache_testing',
  'driver_url'   = 'http://stardata-fe.prod.svc.cluster.local:8181/postgresql-42.7.4.jar',
  'driver_class' = 'org.postgresql.Driver'
);" 2>&1 | grep -v Warning

echo "PostgreSQL catalog done"
