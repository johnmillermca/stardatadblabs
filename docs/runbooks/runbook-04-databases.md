# Runbook 04 — Databases: PostgreSQL, MongoDB, Oracle

> **Namespace:** `databases`  
> **PostgreSQL:** `192.168.1.50:30532` · **MongoDB:** `192.168.1.50:30017`  
> **Oracle:** `192.168.1.50:30521`

---

## 1. Database Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│  databases namespace  (pinned to master.local)                           │
│                                                                          │
│  ┌──────────────────────────────────────────────────┐                   │
│  │  PostgreSQL 17 (bitnami/postgresql 18.8.0)       │                   │
│  │  Port 5432 / NodePort 30532                      │                   │
│  │  Databases: metadata | ranger | polaris          │                   │
│  │             kestra   | sqlmesh_state             │                   │
│  │  Credentials: postgresql-credentials secret      │                   │
│  │  Storage: PVC 20 Gi on local-path                │                   │
│  └──────────────────────────────────────────────────┘                   │
│                                                                          │
│  ┌──────────────────────────────────────────────────┐                   │
│  │  MongoDB 8.0 (bitnami/mongodb 19.1.17)           │                   │
│  │  Port 27017 / NodePort 30017                     │                   │
│  │  Credentials: mongodb-credentials secret         │                   │
│  │  Storage: PVC 20 Gi on local-path                │                   │
│  └──────────────────────────────────────────────────┘                   │
│                                                                          │
│  ┌──────────────────────────────────────────────────┐                   │
│  │  Oracle Database XE 21c                          │                   │
│  │  Port 1521 / NodePort 30521                      │                   │
│  │  Credentials: oracle-credentials secret          │                   │
│  │  Storage: PVC 20 Gi on local-path                │                   │
│  └──────────────────────────────────────────────────┘                   │
└──────────────────────────────────────────────────────────────────────────┘

Platform consumers:
  Apache Ranger ────────► PostgreSQL (ranger database)
  Apache Polaris ───────► PostgreSQL (polaris database)
  Apache Kestra ────────► PostgreSQL (kestra database)
  SQLMesh ──────────────► PostgreSQL (sqlmesh_state database)
  Debezium CDC ─────────► PostgreSQL WAL → Kafka topics
  Debezium CDC ─────────► MongoDB oplog → Kafka topics
  Debezium CDC ─────────► Oracle redo log → Kafka topics
```

---

## 2. PostgreSQL 17

### 2.1 What Is PostgreSQL in This Platform?
PostgreSQL 17 is the **primary relational metadata store**. It is the shared backend for multiple platform components: Ranger stores its policy engine data here, Polaris stores Iceberg catalog metadata here, Kestra stores workflow state here, and SQLMesh stores transformation state here. It also serves as the CDC source for Debezium.

### 2.2 Deploy
```bash
# Seed OpenBao credentials first
sudo bash scripts/master/12-seed-openbao-secrets.sh

# Via ArgoCD
kubectl apply -f argocd-apps/app-postgresql.yaml

# Or manually via Helm
helm repo add bitnami https://charts.bitnami.com/bitnami
helm upgrade --install postgresql bitnami/postgresql \
  --version 18.8.0 \
  --namespace databases \
  --create-namespace \
  -f helm/postgresql/values.yaml
```

### 2.3 Connect
```bash
# Get password
PG_PASS=$(kubectl get secret postgresql-credentials -n databases \
  -o jsonpath='{.data.postgres-password}' | base64 -d)
echo "Password: ${PG_PASS}"

# Method 1: Port-forward then connect locally
kubectl port-forward svc/postgresql -n databases 5432:5432 &
psql -h localhost -U postgres -W

# Method 2: Exec into the pod directly
PG_POD=$(kubectl get pod -n databases -l app.kubernetes.io/name=postgresql \
  -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it "${PG_POD}" -n databases -- psql -U postgres
```

### 2.4 Database Management
```sql
-- List all databases
\l

-- Create a new database
CREATE DATABASE myapp;

-- Create a user with limited privileges
CREATE USER myapp_user WITH PASSWORD 'secret';
GRANT CONNECT ON DATABASE myapp TO myapp_user;
GRANT USAGE ON SCHEMA public TO myapp_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO myapp_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO myapp_user;

-- Check all active connections
SELECT pid, usename, datname, client_addr, state, query
FROM pg_stat_activity
WHERE state != 'idle';

-- Kill a blocking connection
SELECT pg_terminate_backend(<pid>);

-- Replication slot status (important for Debezium CDC)
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots;

-- Drop a stuck replication slot
SELECT pg_drop_replication_slot('debezium_slot');
```

### 2.5 Schema and Data Operations
```sql
-- List tables in current database
\dt

-- Describe a table
\d+ my_table

-- Show table sizes
SELECT schemaname, tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS total_size
FROM pg_tables
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Show index usage
SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;

-- Vacuum and analyze
VACUUM ANALYZE my_table;
VACUUM FULL my_table;  -- Reclaims disk space (locks table briefly)
```

### 2.6 Backup & Restore
```bash
# Full database dump (all databases)
PG_POD=$(kubectl get pod -n databases -l app.kubernetes.io/name=postgresql \
  -o jsonpath='{.items[0].metadata.name}')

kubectl exec -n databases "${PG_POD}" -- \
  pg_dumpall -U postgres > /opt/k8s-backups/pg-dump-$(date +%Y%m%d).sql

# Single database dump
kubectl exec -n databases "${PG_POD}" -- \
  pg_dump -U postgres metadata > /opt/k8s-backups/metadata-$(date +%Y%m%d).sql

# Custom format (compressed, parallel-restoreable)
kubectl exec -n databases "${PG_POD}" -- \
  pg_dump -U postgres -Fc metadata > /opt/k8s-backups/metadata-$(date +%Y%m%d).dump

# Restore from SQL dump
kubectl exec -i -n databases "${PG_POD}" -- \
  psql -U postgres < /opt/k8s-backups/metadata-20250101.sql

# Restore from custom format (parallel)
kubectl exec -i -n databases "${PG_POD}" -- \
  pg_restore -U postgres -d metadata -j 4 < /opt/k8s-backups/metadata-20250101.dump
```

### 2.7 Performance Monitoring
```sql
-- Slow query log — find queries running > 1 second
SELECT pid, now() - pg_stat_activity.query_start AS duration, query, state
FROM pg_stat_activity
WHERE (now() - pg_stat_activity.query_start) > interval '1 second';

-- Cache hit rate (should be > 99%)
SELECT
  sum(heap_blks_read)  AS heap_read,
  sum(heap_blks_hit)   AS heap_hit,
  sum(heap_blks_hit) / (sum(heap_blks_hit) + sum(heap_blks_read)) AS ratio
FROM pg_statio_user_tables;

-- Table bloat estimate
SELECT schemaname, tablename, n_dead_tup, n_live_tup,
  round(n_dead_tup::numeric / NULLIF(n_live_tup,0) * 100, 2) AS dead_pct
FROM pg_stat_user_tables
ORDER BY n_dead_tup DESC;
```

### 2.8 Enable Logical Replication for Debezium CDC
```bash
# Check current wal_level
kubectl exec -n databases "${PG_POD}" -- \
  psql -U postgres -c "SHOW wal_level;"
# Must be: logical

# If not logical, update postgresql.conf via helm values:
# primary.extendedConfiguration:
#   wal_level: logical
#   max_replication_slots: 5
#   max_wal_senders: 5
```

---

## 3. MongoDB 8.0

### 3.1 What Is MongoDB in This Platform?
MongoDB stores **unstructured and semi-structured data** — event logs, application state, flexible document records. It is also a CDC source for Debezium, streaming oplog changes to Kafka.

### 3.2 Deploy
```bash
# Via ArgoCD
kubectl apply -f argocd-apps/app-mongodb.yaml

# Or manually
helm upgrade --install mongodb bitnami/mongodb \
  --version 19.1.17 \
  --namespace databases \
  -f helm/mongodb/values.yaml
```

### 3.3 Connect
```bash
# Get password
MONGO_PASS=$(kubectl get secret mongodb-credentials -n databases \
  -o jsonpath='{.data.mongodb-root-password}' | base64 -d)
echo "Password: ${MONGO_PASS}"

# Port-forward and connect with mongosh
kubectl port-forward svc/mongodb -n databases 27017:27017 &
mongosh "mongodb://root:${MONGO_PASS}@localhost:27017"

# Direct exec into pod
MONGO_POD=$(kubectl get pod -n databases -l app.kubernetes.io/name=mongodb \
  -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it "${MONGO_POD}" -n databases -- \
  mongosh -u root -p "${MONGO_PASS}" --authenticationDatabase admin
```

### 3.4 Database Operations
```javascript
// List all databases
show dbs

// Switch to a database (creates it if it doesn't exist)
use myapp

// Create a collection
db.createCollection("events")

// Insert documents
db.events.insertMany([
  { user_id: 1, event_type: "login", ts: new Date() },
  { user_id: 2, event_type: "purchase", amount: 99.99, ts: new Date() }
])

// Query documents
db.events.find({ event_type: "login" })
db.events.find({ amount: { $gt: 50 } })
db.events.find().sort({ ts: -1 }).limit(10)

// Update documents
db.events.updateMany(
  { event_type: "login" },
  { $set: { processed: true } }
)

// Delete documents
db.events.deleteMany({ processed: true })

// Create an index
db.events.createIndex({ user_id: 1, ts: -1 })

// Show indexes
db.events.getIndexes()

// Collection stats
db.events.stats()
```

### 3.5 Backup & Restore
```bash
MONGO_POD=$(kubectl get pod -n databases -l app.kubernetes.io/name=mongodb \
  -o jsonpath='{.items[0].metadata.name}')
MONGO_PASS=$(kubectl get secret mongodb-credentials -n databases \
  -o jsonpath='{.data.mongodb-root-password}' | base64 -d)

# Dump all databases
kubectl exec -n databases "${MONGO_POD}" -- \
  mongodump --uri="mongodb://root:${MONGO_PASS}@localhost:27017" \
  --archive > /opt/k8s-backups/mongo-$(date +%Y%m%d).archive

# Restore
kubectl exec -i -n databases "${MONGO_POD}" -- \
  mongorestore --uri="mongodb://root:${MONGO_PASS}@localhost:27017" \
  --archive < /opt/k8s-backups/mongo-20250101.archive

# Dump specific database
kubectl exec -n databases "${MONGO_POD}" -- \
  mongodump --uri="mongodb://root:${MONGO_PASS}@localhost:27017/myapp" \
  --archive > /opt/k8s-backups/myapp-$(date +%Y%m%d).archive
```

### 3.6 Enable MongoDB Replica Set for Debezium CDC
```bash
# MongoDB must run as a replica set for Debezium oplog-based CDC
# This is configured in helm/mongodb/values.yaml:
# architecture: replicaset
# replicaCount: 1  (single-node replica set for lab)

# Verify replica set status
kubectl exec -n databases "${MONGO_POD}" -- \
  mongosh -u root -p "${MONGO_PASS}" --authenticationDatabase admin \
  --eval "rs.status()"
```

---

## 4. Oracle Database XE 21c

### 4.1 Overview
Oracle Database XE (Express Edition) 21c is deployed for Oracle-specific workloads and as a CDC source for Debezium Oracle LogMiner connector.

| Property | Value |
|---|---|
| Port | 1521 / NodePort 30521 |
| Image | `gvenzl/oracle-xe:21-slim` |
| SID | `XE` |
| Service name | `XEPDB1` |
| Namespace | `databases` |

### 4.2 Connect
```bash
# Get password
ORA_PASS=$(kubectl get secret oracle-credentials -n databases \
  -o jsonpath='{.data.oracle-password}' | base64 -d)

# Port-forward
kubectl port-forward svc/oracle -n databases 1521:1521 &

# Connect with sqlplus
sqlplus system/"${ORA_PASS}"@//localhost:1521/XE

# Connect to pluggable database
sqlplus system/"${ORA_PASS}"@//localhost:1521/XEPDB1
```

### 4.3 Enable LogMiner for Debezium CDC
```sql
-- Connect as SYSDBA
ALTER SYSTEM SET log_archive_dest_1='LOCATION=/opt/oracle/oradata/XEPDB1/archive' SCOPE=SPFILE;
ALTER SYSTEM SET log_archive_format='%t_%s_%r.dbf' SCOPE=SPFILE;
ALTER DATABASE ARCHIVELOG;

-- Enable supplemental logging
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- Create Debezium user
CREATE USER debezium IDENTIFIED BY <password>;
GRANT CONNECT, CREATE SESSION TO debezium;
GRANT SELECT ON V_$DATABASE TO debezium;
GRANT FLASHBACK ANY TABLE TO debezium;
GRANT SELECT ANY TABLE TO debezium;
GRANT SELECT_CATALOG_ROLE TO debezium;
GRANT EXECUTE_CATALOG_ROLE TO debezium;
GRANT SELECT ANY TRANSACTION TO debezium;
GRANT LOGMINING TO debezium;
GRANT CREATE TABLE TO debezium;
GRANT LOCK ANY TABLE TO debezium;
GRANT CREATE SEQUENCE TO debezium;
GRANT UNLIMITED TABLESPACE TO debezium;
```

### 4.4 Register Oracle Debezium Connector
```bash
ORA_PASS=$(kubectl get secret oracle-credentials -n databases \
  -o jsonpath='{.data.debezium-password}' | base64 -d)

curl -X POST http://192.168.1.50:30083/connectors \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"oracle-cdc\",
    \"config\": {
      \"connector.class\": \"io.debezium.connector.oracle.OracleConnector\",
      \"database.hostname\": \"oracle.databases.svc.cluster.local\",
      \"database.port\": \"1521\",
      \"database.user\": \"debezium\",
      \"database.password\": \"${ORA_PASS}\",
      \"database.sid\": \"XE\",
      \"topic.prefix\": \"oracle\",
      \"table.include.list\": \"MYSCHEMA.EVENTS\",
      \"log.mining.strategy\": \"online_catalog\"
    }
  }"
```

---

## 5. Database Secrets Reference

All database credentials are managed in OpenBao:

```bash
export BAO_ADDR="http://192.168.1.50:30820"
export BAO_TOKEN="<root-token>"

# Read all database credentials
bao kv get secret/postgresql/credentials
bao kv get secret/mongodb/credentials
bao kv get secret/oracle/credentials

# Update a credential
bao kv patch secret/postgresql/credentials postgres-password="new-password"

# The Kubernetes secret is updated by re-running:
sudo bash scripts/master/12-seed-openbao-secrets.sh
```

---

## 6. Troubleshooting

### 6.1 PostgreSQL PVC Pending
```bash
kubectl get pvc -n databases
kubectl describe pvc data-postgresql-0 -n databases
# Ensure local-path StorageClass is available
kubectl get storageclass
```

### 6.2 PostgreSQL Cannot Accept Connections
```bash
kubectl get pods -n databases -l app.kubernetes.io/name=postgresql
kubectl logs -n databases postgresql-0 --previous
# Check for max_connections exceeded
psql -U postgres -c "SELECT count(*) FROM pg_stat_activity;"
```

### 6.3 MongoDB Replica Set Not Initialized
```bash
MONGO_POD=$(kubectl get pod -n databases -l app.kubernetes.io/name=mongodb \
  -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it "${MONGO_POD}" -n databases -- \
  mongosh -u root -p "${MONGO_PASS}" --authenticationDatabase admin \
  --eval "rs.initiate()"
```

### 6.4 Slow PostgreSQL Queries
```sql
-- Find the slowest queries
SELECT query, mean_exec_time, calls, total_exec_time
FROM pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT 20;

-- Requires pg_stat_statements extension:
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
```
