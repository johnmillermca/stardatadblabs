# Runbook 05 — Analytics & Search: Doris, OpenSearch, Spark, Iceberg, Polaris

> **Analytics namespace:** `analytics` · **Search namespace:** `search` · **Catalog namespace:** `catalog`  
> **Apache Doris FE UI:** `http://192.168.1.50:30030`  
> **OpenSearch:** `http://192.168.1.50:30920`  
> **Apache Spark UI:** `http://192.168.1.50:30707`  
> **Apache Polaris REST:** `http://192.168.1.50:30181`

---

## 1. Analytics & Search Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Data Layer                                                              │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  Apache Polaris (catalog namespace)                             │    │
│  │  REST Iceberg Catalog — Port 8181 / NodePort 30181             │    │
│  │  Backed by PostgreSQL (polaris database)                        │    │
│  └────────────────────────┬────────────────────────────────────────┘    │
│                           │ catalog API                                  │
│           ┌───────────────▼──────────────────────┐                      │
│           │  Apache Iceberg (table format)        │                      │
│           │  Tables stored on local-path PVC      │                      │
│           │  Version history, time-travel         │                      │
│           └───────────────┬──────────────────────┘                      │
│                           │ read/write                                   │
│  ┌────────────────────────▼──────────────────────────────────────┐      │
│  │  Apache Spark 3.5.1 (analytics namespace)                     │      │
│  │  + Gluten 1.2.0 + Velox (vectorized native execution)         │      │
│  │  Master UI: 30707  |  RPC: 30777                              │      │
│  │  3 workers × 2 CPU / 2 GB                                     │      │
│  └────────────────────────┬──────────────────────────────────────┘      │
│                           │ compute                                      │
│  ┌────────────────────────▼──────────────────────────────────────┐      │
│  │  Apache Doris 2.1.0 (analytics namespace)                     │      │
│  │  MPP SQL analytics — FE port 9030 / NodePort 30090 (MySQL)   │      │
│  │  Web UI NodePort 30030                                        │      │
│  └────────────────────────────────────────────────────────────────┘     │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  OpenSearch 3.7.0 (search namespace)                          │     │
│  │  REST API: 9200 / NodePort 30920                              │     │
│  │  Dashboards: NodePort 30601                                   │     │
│  │  CDC data from Debezium → indexed for search                  │     │
│  └────────────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Apache Doris — MPP Analytics Database

### 2.1 What Is Apache Doris?
Apache Doris is a **massively parallel processing (MPP) analytical database** designed for real-time analytics at scale. It can ingest data from Kafka via Routine Load, query Iceberg tables via multi-catalog, and serve sub-second queries on hundreds of millions of rows. Architecture:
- **Frontend (FE):** Query parsing, planning, metadata management, MySQL protocol interface
- **Backend (BE):** Data storage, query execution, compaction

### 2.2 Deploy
```bash
# Via ArgoCD
kubectl apply -f argocd-apps/app-doris.yaml

# Or manually (FE first, then BE)
kubectl apply -f manifests/doris/doris-services.yaml
kubectl apply -f manifests/doris/doris-fe-deployment.yaml
kubectl rollout status deployment/doris-fe -n analytics
kubectl apply -f manifests/doris/doris-be-deployment.yaml
kubectl rollout status deployment/doris-be -n analytics
```

### 2.3 Connect to Doris
```bash
# Get password
DORIS_PASS=$(kubectl get secret doris-credentials -n analytics \
  -o jsonpath='{.data.admin-password}' | base64 -d)

# Connect via MySQL client
mysql -h 192.168.1.50 -P 30090 -u root -p"${DORIS_PASS}"

# Or port-forward
kubectl port-forward svc/doris-fe -n analytics 9030:9030 &
mysql -h 127.0.0.1 -P 9030 -u root
```

### 2.4 Initial Setup
```sql
-- Set root password on first login (no password initially)
SET PASSWORD FOR 'root'@'%' = PASSWORD('<admin-password>');

-- Create an analytics database
CREATE DATABASE analytics;
USE analytics;

-- Create an admin user for apps
CREATE USER 'analytics_user'@'%' IDENTIFIED BY '<password>';
GRANT ALL ON analytics.* TO 'analytics_user'@'%';

-- Check backend status
SHOW BACKENDS;

-- Check cluster health
SHOW FRONTENDS;
SHOW PROC '/cluster_balance';
```

### 2.5 Create Tables
```sql
-- Duplicate Key model (for raw events / append-only data)
CREATE TABLE analytics.events (
    event_id      BIGINT,
    event_type    VARCHAR(64),
    user_id       BIGINT,
    amount        DECIMAL(18,2),
    event_ts      DATETIME
)
DUPLICATE KEY(event_id)
DISTRIBUTED BY HASH(event_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "1",
    "bloom_filter_columns" = "user_id"
);

-- Aggregate Key model (for pre-aggregated metrics)
CREATE TABLE analytics.user_metrics (
    user_id       BIGINT,
    date          DATE,
    event_count   BIGINT       SUM,
    total_spend   DECIMAL(18,2) SUM
)
AGGREGATE KEY(user_id, date)
DISTRIBUTED BY HASH(user_id) BUCKETS 8;

-- Unique Key model (for upsert/CDC data)
CREATE TABLE analytics.users (
    user_id       BIGINT,
    email         VARCHAR(256),
    updated_at    DATETIME
)
UNIQUE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 8;
```

### 2.6 Kafka Routine Load (Real-Time Ingestion from Kafka)
```sql
-- Create a Routine Load job to ingest from Kafka topic
CREATE ROUTINE LOAD analytics.events_load ON analytics.events
COLUMNS TERMINATED BY ",",
COLUMNS (event_id, event_type, user_id, amount, event_ts)
PROPERTIES (
    "max_batch_interval" = "10",
    "max_batch_rows" = "300000",
    "max_batch_size" = "209715200",
    "format" = "json"
)
FROM KAFKA (
    "kafka_broker_list" = "kafka.streaming.svc.cluster.local:9092",
    "kafka_topic" = "events-raw",
    "property.group.id" = "doris-events-consumer",
    "property.client.id" = "doris-events",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

-- Check Routine Load status
SHOW ROUTINE LOAD FOR analytics.events_load\G

-- Pause a job
PAUSE ROUTINE LOAD FOR analytics.events_load;

-- Resume a job
RESUME ROUTINE LOAD FOR analytics.events_load;

-- Stop a job permanently
STOP ROUTINE LOAD FOR analytics.events_load;
```

### 2.7 Query Iceberg Tables via External Catalog
```sql
-- Create an Iceberg external catalog pointing to Polaris
CREATE CATALOG iceberg_polaris PROPERTIES (
    'type' = 'iceberg',
    'iceberg.catalog.type' = 'rest',
    'uri' = 'http://polaris-rest.catalog.svc.cluster.local:8181/api/catalog'
);

-- Query Iceberg tables from Doris
SELECT * FROM iceberg_polaris.analytics.sales LIMIT 100;

-- Join Doris table with Iceberg table
SELECT d.user_id, d.event_count, i.total_spend
FROM analytics.user_metrics d
JOIN iceberg_polaris.analytics.sales i ON d.user_id = i.user_id
WHERE d.date = CURDATE();
```

### 2.8 Compaction and Maintenance
```sql
-- Check tablet health and compaction status
SHOW PROC '/statistic';

-- Trigger manual compaction on a table
ALTER TABLE analytics.events COMPACT;

-- Check running queries
SHOW PROCESSLIST;

-- Kill a long-running query
KILL QUERY <connection-id>;

-- Check disk usage per backend
SHOW PROC '/backends';
```

---

## 3. Apache OpenSearch

### 3.1 What Is OpenSearch in This Platform?
OpenSearch 3.7.0 is the **log aggregation, full-text search, and observability store**. It receives CDC events from Debezium via Kafka connectors, and serves real-time search queries over platform event data.

### 3.2 Cluster Health
```bash
# Cluster health (green / yellow / red)
curl -s http://192.168.1.50:30920/_cluster/health?pretty

# Node details
curl -s http://192.168.1.50:30920/_cat/nodes?v

# Shard allocation
curl -s http://192.168.1.50:30920/_cat/shards?v

# Index stats
curl -s http://192.168.1.50:30920/_cat/indices?v&s=store.size:desc
```

### 3.3 Index Management
```bash
# Create an index
curl -X PUT http://192.168.1.50:30920/cdc-events \
  -H "Content-Type: application/json" \
  -d '{
    "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 0,
      "index.refresh_interval": "5s"
    },
    "mappings": {
      "properties": {
        "event_id":   { "type": "long" },
        "event_type": { "type": "keyword" },
        "user_id":    { "type": "long" },
        "ts":         { "type": "date", "format": "epoch_millis||date_time" },
        "payload":    { "type": "object", "dynamic": true }
      }
    }
  }'

# List all indices
curl http://192.168.1.50:30920/_cat/indices?v

# Delete an index
curl -X DELETE http://192.168.1.50:30920/cdc-events

# Force merge (reduces segment count for read-only indices)
curl -X POST "http://192.168.1.50:30920/cdc-events/_forcemerge?max_num_segments=1"
```

### 3.4 Document Operations
```bash
# Index (insert/replace) a document
curl -X PUT http://192.168.1.50:30920/cdc-events/_doc/1 \
  -H "Content-Type: application/json" \
  -d '{"event_type": "purchase", "user_id": 42, "amount": 99.99}'

# Bulk index
curl -X POST http://192.168.1.50:30920/_bulk \
  -H "Content-Type: application/json" \
  -d '
{"index": {"_index": "cdc-events", "_id": "2"}}
{"event_type": "login", "user_id": 1}
{"index": {"_index": "cdc-events", "_id": "3"}}
{"event_type": "logout", "user_id": 1}
'

# Search
curl -X POST http://192.168.1.50:30920/cdc-events/_search \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "bool": {
        "must": [
          { "term": { "event_type": "purchase" } },
          { "range": { "ts": { "gte": "now-1d/d" } } }
        ]
      }
    },
    "size": 20,
    "sort": [{ "ts": { "order": "desc" } }]
  }'

# Delete by query
curl -X POST http://192.168.1.50:30920/cdc-events/_delete_by_query \
  -H "Content-Type: application/json" \
  -d '{"query": {"term": {"event_type": "test"}}}'
```

### 3.5 Index Templates
```bash
# Create an index template (applies to all indices matching cdc-*)
curl -X PUT http://192.168.1.50:30920/_index_template/cdc-template \
  -H "Content-Type: application/json" \
  -d '{
    "index_patterns": ["cdc-*"],
    "template": {
      "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
      },
      "mappings": {
        "properties": {
          "ts": { "type": "date" },
          "event_type": { "type": "keyword" }
        }
      }
    }
  }'
```

### 3.6 Kafka → OpenSearch Connector (via Debezium)
```bash
# Register a Kafka → OpenSearch sink connector
curl -X POST http://192.168.1.50:30083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "opensearch-sink",
    "config": {
      "connector.class": "io.confluent.connect.opensearch.OpenSearchSinkConnector",
      "tasks.max": "1",
      "topics": "cdc.public.events",
      "connection.url": "http://opensearch-cluster-master.search.svc.cluster.local:9200",
      "type.name": "_doc",
      "key.ignore": "true",
      "schema.ignore": "true"
    }
  }'
```

---

## 4. Apache Spark + Gluten + Velox

### 4.1 What Is the Spark Stack?
This platform deploys **Apache Spark 3.5.1** with two performance enhancements:
- **Gluten 1.2.0**: A Spark plugin that offloads physical plan execution to native libraries
- **Velox**: Meta's high-performance vectorized execution engine that Gluten delegates to

The result: Spark SQL queries on Iceberg tables run with **native vectorized CPU execution** instead of JVM-based row processing — delivering 2-10x performance improvements on analytical workloads.

### 4.2 Deploy
```bash
# Build the custom image first (spark + gluten + velox + iceberg)
bash docker/spark-gluten-velox/build-and-push.sh

# Deploy via ArgoCD
kubectl apply -f argocd-apps/app-spark.yaml

# Or manually via Helm
helm upgrade --install spark bitnami/spark \
  --version 10.0.3 \
  --namespace analytics \
  -f helm/spark/values.yaml
```

### 4.3 Submit a Job
```bash
# Run SparkPi example (validates cluster connectivity)
kubectl run spark-submit --rm -it --restart=Never \
  --image=192.168.1.50:30500/spark-gluten-velox:3.5.1 \
  -- spark-submit \
    --master spark://spark-master-svc.analytics.svc.cluster.local:7077 \
    --class org.apache.spark.examples.SparkPi \
    --num-executors 2 \
    --executor-cores 1 \
    --executor-memory 1g \
    /opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar 100

# Submit a custom JAR
kubectl run spark-job --rm -it --restart=Never \
  --image=192.168.1.50:30500/spark-gluten-velox:3.5.1 \
  -- spark-submit \
    --master spark://spark-master-svc.analytics.svc.cluster.local:7077 \
    --class com.example.MyJob \
    --conf spark.executor.instances=3 \
    /opt/jars/my-job.jar \
    --input-path /data/input \
    --output-path /data/output
```

### 4.4 Enable Gluten / Velox
```python
# Add to your SparkSession configuration
spark = SparkSession.builder \
    .appName("my-velox-job") \
    .config("spark.plugins", "io.glutenproject.GlutenPlugin") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .config("spark.gluten.sql.columnar.backend.lib", "velox") \
    .config("spark.gluten.sql.columnar.forceShuffledHashJoin", "true") \
    .getOrCreate()
```

### 4.5 Working with Iceberg Tables
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("iceberg-example") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.polaris.type", "rest") \
    .config("spark.sql.catalog.polaris.uri",
            "http://polaris-rest.catalog.svc.cluster.local:8181/api/catalog") \
    .getOrCreate()

# Create a table
spark.sql("""
    CREATE TABLE IF NOT EXISTS polaris.analytics.sales (
        id BIGINT, product STRING, amount DOUBLE, sale_date DATE
    )
    USING iceberg
    PARTITIONED BY (sale_date)
""")

# Write data
df = spark.createDataFrame([(1, "Widget A", 99.99, "2025-01-01")],
                            ["id", "product", "amount", "sale_date"])
df.writeTo("polaris.analytics.sales").append()

# Time-travel query
spark.sql("""
    SELECT * FROM polaris.analytics.sales
    TIMESTAMP AS OF '2025-01-01 00:00:00'
""").show()

# Iceberg table maintenance
spark.sql("CALL polaris.system.rewrite_data_files(table => 'analytics.sales')")
spark.sql("CALL polaris.system.expire_snapshots(table => 'analytics.sales', retain_last => 5)")
spark.sql("CALL polaris.system.remove_orphan_files(table => 'analytics.sales')")
```

### 4.6 Spark Cluster Monitoring
```bash
# Spark Master web UI
open http://192.168.1.50:30707

# Check worker pods
kubectl get pods -n analytics -l app.kubernetes.io/name=spark

# Worker logs
kubectl logs -n analytics -l spark-role=worker --tail=100

# Scale workers
kubectl scale statefulset spark-worker -n analytics --replicas=5
```

---

## 5. Apache Iceberg Table Format

### 5.1 What Is Iceberg?
Apache Iceberg is an open table format for huge analytic datasets stored in object storage or local filesystems. Key features:
- **ACID transactions**: Concurrent reads and writes without corrupting data
- **Schema evolution**: Add/rename/drop columns without rewriting data
- **Partition evolution**: Change partitioning strategy without rewriting data
- **Time travel**: Query historical snapshots of a table
- **Rollback**: Revert a table to a previous snapshot

### 5.2 Iceberg Metadata Operations
```sql
-- List all snapshots of a table
SELECT * FROM polaris.analytics.sales.snapshots;

-- Inspect data files
SELECT * FROM polaris.analytics.sales.files;

-- Inspect manifests
SELECT * FROM polaris.analytics.sales.manifests;

-- View all partitions
SELECT * FROM polaris.analytics.sales.partitions;

-- Roll back to a specific snapshot
CALL polaris.system.rollback_to_snapshot(
    table => 'analytics.sales',
    snapshot_id => 1234567890
);

-- Expire old snapshots (keep last 5, delete older than 7 days)
CALL polaris.system.expire_snapshots(
    table => 'analytics.sales',
    older_than => TIMESTAMP '2025-01-01 00:00:00',
    retain_last => 5
);

-- Compact small files
CALL polaris.system.rewrite_data_files(
    table => 'analytics.sales',
    strategy => 'sort',
    sort_order => 'sale_date ASC NULLS LAST'
);

-- Remove orphan files (files not referenced by any snapshot)
CALL polaris.system.remove_orphan_files(table => 'analytics.sales');
```

---

## 6. Apache Polaris — Iceberg REST Catalog

### 6.1 What Is Polaris?
Apache Polaris is an **open-source Iceberg REST catalog** — the central registry for all Iceberg table metadata. Any engine that supports the Iceberg REST Catalog API (Spark, Flink, Trino, Doris) can use Polaris to discover and access tables without knowing the underlying storage locations.

### 6.2 Deploy
```bash
# Build image first
git clone https://github.com/apache/polaris.git
cd polaris
./gradlew :polaris-quarkus-server:build -Dquarkus.package.type=uber-jar
docker build -t 192.168.1.50:30500/apache-polaris:latest .
docker push 192.168.1.50:30500/apache-polaris:latest

# Deploy via ArgoCD
kubectl apply -f argocd-apps/app-polaris.yaml
```

### 6.3 Catalog Operations
```bash
# Check catalog API health
curl http://192.168.1.50:30181/api/catalog/v1/config

# List catalogs
curl http://192.168.1.50:30181/api/management/v1/catalogs

# Create a catalog
curl -X POST http://192.168.1.50:30181/api/management/v1/catalogs \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": {
      "name": "analytics",
      "type": "INTERNAL",
      "properties": {
        "default-base-location": "file:///opt/polaris/warehouse"
      }
    }
  }'

# List namespaces
curl http://192.168.1.50:30181/api/catalog/v1/analytics/namespaces

# List tables in a namespace
curl http://192.168.1.50:30181/api/catalog/v1/analytics/namespaces/prod/tables
```

---

## 7. Troubleshooting

### 7.1 Doris BE Not Connecting to FE
```sql
-- Check BE status from FE
SHOW BACKENDS;
-- Look for "Alive = false" or incorrect heartbeatAddress

-- View FE log
kubectl logs -n analytics -l app=doris-fe --tail=50

-- FQDN mode requires stable DNS — check DNS resolution
kubectl run dnscheck --rm -it --restart=Never --image=busybox \
  -- nslookup doris-fe-headless.analytics.svc.cluster.local
```

### 7.2 OpenSearch RED Cluster
```bash
# Find unassigned shards
curl http://192.168.1.50:30920/_cat/shards?h=index,shard,prirep,state,unassigned.reason | grep UNASSIGNED

# Reroute shards manually (for single-node lab — set replicas to 0)
curl -X PUT http://192.168.1.50:30920/_settings \
  -H "Content-Type: application/json" \
  -d '{"index.number_of_replicas": "0"}'
```

### 7.3 Spark Job OOM
```bash
# Check executor memory
kubectl describe pod -n analytics -l spark-role=worker | grep -A5 "Limits"

# Increase memory in submit command
--executor-memory 4g --driver-memory 2g

# Enable off-heap for Velox
--conf spark.memory.offHeap.enabled=true \
--conf spark.memory.offHeap.size=2g
```

### 7.4 Polaris API 500 Error
```bash
# Check Polaris pod logs
kubectl logs -n catalog deploy/polaris --tail=50

# Verify PostgreSQL connection
kubectl exec -n catalog deploy/polaris -- \
  psql -h postgresql.databases.svc.cluster.local -U polaris -d polaris -c "\l"
```
