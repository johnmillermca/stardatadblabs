#!/bin/bash
# Called with DORIS_PASS and MONGO_PASS env vars set
mysql -h 127.0.0.1 -P 9030 -u root -p"$DORIS_PASS" --connect-timeout=10 \
  -e "DROP CATALOG IF EXISTS mongodb;" 2>&1 | grep -v Warning

mysql -h 127.0.0.1 -P 9030 -u root -p"$DORIS_PASS" --connect-timeout=10 \
  -e "CREATE CATALOG mongodb PROPERTIES (
  'type'         = 'jdbc',
  'user'         = 'root',
  'password'     = '$MONGO_PASS',
  'jdbc_url'     = 'jdbc:mongodb://mongodb.prod.svc.cluster.local:27017/admin?authSource=admin',
  'driver_url'   = 'http://stardata-fe.prod.svc.cluster.local:8181/mongodb-jdbc-2.0.2-all.jar',
  'driver_class' = 'com.mongodb.jdbc.MongoDriver'
);" 2>&1 | grep -v Warning

echo "exit: $?"
