#!/bin/bash
# Called with DORIS_PASS env var set
mysql -h 127.0.0.1 -P 9030 -u root -p"$DORIS_PASS" --connect-timeout=10 \
  -e "DROP CATALOG IF EXISTS oracle_cache_testing;" 2>&1 | grep -v Warning

mysql -h 127.0.0.1 -P 9030 -u root -p"$DORIS_PASS" --connect-timeout=10 \
  -e "CREATE CATALOG oracle_cache_testing PROPERTIES (
  'type'         = 'jdbc',
  'user'         = 'CACHE_TESTING',
  'password'     = 'Priya1982',
  'jdbc_url'     = 'jdbc:oracle:thin:@oracle-xe.prod.svc.cluster.local:1521/XEPDB1',
  'driver_url'   = 'http://stardata-fe.prod.svc.cluster.local:8181/ojdbc11.jar',
  'driver_class' = 'oracle.jdbc.OracleDriver'
);" 2>&1 | grep -v Warning

echo "Oracle catalog done"
