#!/bin/bash
mysql -h 127.0.0.1 -P 9030 -u root -p"$DORIS_PASS" --connect-timeout=10   -e "DROP CATALOG IF EXISTS mongodb;" 2>&1 | grep -v Warning

mysql -h 127.0.0.1 -P 9030 -u root -p"$DORIS_PASS" --connect-timeout=30   -e "CREATE CATALOG mongodb PROPERTIES (
  'type'                             = 'trino-connector',
  'trino.connector.name'             = 'mongodb',
  'trino.mongodb.connection-url'     = 'mongodb://root:Priya1982@mongodb.prod.svc.cluster.local:27017/?authSource=admin',
  'trino.mongodb.schema-collection'  = '_schema'
);" 2>&1 | grep -v Warning
echo "done: $?"
