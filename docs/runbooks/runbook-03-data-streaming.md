# Runbook 03 — Data Streaming: Kafka, Strimzi, Schema Registry, Debezium, AKHQ

> **Namespace:** `streaming` · **Operator namespace:** `strimzi-system`  
> **Kafka (bitnami):** bootstrap `kafka.streaming.svc.cluster.local:9092`, NodePort `30092`  
> **Kafka (Strimzi):** bootstrap `strimzi-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092`, NodePort `30093`

---

## 1. Streaming Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│  streaming namespace                                                     │
│                                                                          │
│  ┌──────────────────┐    ┌────────────────────────────────┐              │
│  │  bitnami/kafka   │    │  Strimzi Kafka Operator        │              │
│  │  Kafka 4.0 KRaft │    │  (strimzi-system namespace)    │              │
│  │  SASL/PLAIN      │    │  Manages CRDs:                 │              │
│  │  NodePort 30092  │    │  Kafka, KafkaTopic,            │              │
│  └──────────────────┘    │  KafkaUser, KafkaBridge        │              │
│                          └────────────┬───────────────────┘              │
│                                       │ manages                          │
│                          ┌────────────▼───────────────────┐              │
│                          │  strimzi-kafka cluster         │              │
│                          │  Kafka 3.9.0 KRaft             │              │
│                          │  SCRAM-SHA-512                 │              │
│                          │  NodePort 30093                │              │
│                          │  Users: kafka-app-user,        │              │
│                          │         debezium-user,         │              │
│                          │         schema-registry-user   │              │
│                          └────────────┬───────────────────┘              │
│                                       │                                  │
│            ┌──────────────────────────┼──────────────────────┐          │
│            │                          │                       │          │
│  ┌─────────▼──────────┐   ┌───────────▼──────────┐  ┌────────▼────────┐ │
│  │  Schema Registry   │   │  Debezium Connect    │  │  AKHQ           │ │
│  │  Port 8081         │   │  Port 8083           │  │  Port 8080      │ │
│  │  NodePort 30810    │   │  NodePort 30083      │  │  NodePort 30808 │ │
│  │  Avro/JSON/Proto   │   │  CDC: PG,Mongo,      │  │  Web UI         │ │
│  │  schema store      │   │  Oracle → Kafka      │  │  for all above  │ │
│  └────────────────────┘   └──────────────────────┘  └─────────────────┘ │
└──────────────────────────────────────────────────────────────────────────┘

Data sources:          Data consumers:
PostgreSQL ──► CDC     Kestra orchestration
MongoDB ────► CDC     SQLMesh transformations
Oracle ─────► CDC     OpenSearch (log ingestion)
                       Apache Doris (real-time analytics)
```

---

## 2. Apache Kafka (bitnami) — Kafka 4.0 KRaft

### 2.1 What Is Apache Kafka?
Apache Kafka is a distributed event streaming platform. It works as a **durable, high-throughput message queue** where producers write events to named **topics** and consumers read from those topics independently. Key properties:
- **Topics** are divided into **partitions** for parallelism
- **Consumer groups** allow horizontal scaling of consumers
- **Retention** keeps messages for a configurable period even after consumption
- **KRaft mode** (this platform) eliminates the ZooKeeper dependency

### 2.2 Deploy bitnami Kafka
```bash
helm upgrade --install kafka bitnami/kafka \
  --version 32.4.3 \
  --namespace streaming \
  --create-namespace \
  -f helm/kafka/values.yaml
```

### 2.3 Get Kafka Credentials
```bash
KAFKA_PASS=$(kubectl get secret kafka-credentials -n streaming \
  -o jsonpath='{.data.kafka-password}' | base64 -d)
echo "Password: ${KAFKA_PASS}"
```

### 2.4 Produce Messages (bitnami)
```bash
KAFKA_PASS=$(kubectl get secret kafka-credentials -n streaming \
  -o jsonpath='{.data.kafka-password}' | base64 -d)

kubectl run kafka-producer --rm -it --restart=Never \
  --image bitnami/kafka:4.0 \
  -- kafka-console-producer.sh \
    --bootstrap-server kafka.streaming.svc.cluster.local:9092 \
    --topic my-topic \
    --producer-property security.protocol=SASL_PLAINTEXT \
    --producer-property sasl.mechanism=PLAIN \
    --producer-property "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=kafka-user password=${KAFKA_PASS};"
```

### 2.5 Consume Messages (bitnami)
```bash
KAFKA_PASS=$(kubectl get secret kafka-credentials -n streaming \
  -o jsonpath='{.data.kafka-password}' | base64 -d)

kubectl run kafka-consumer --rm -it --restart=Never \
  --image bitnami/kafka:4.0 \
  -- kafka-console-consumer.sh \
    --bootstrap-server kafka.streaming.svc.cluster.local:9092 \
    --topic my-topic \
    --from-beginning \
    --consumer-property security.protocol=SASL_PLAINTEXT \
    --consumer-property sasl.mechanism=PLAIN \
    --consumer-property "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=kafka-user password=${KAFKA_PASS};"
```

### 2.6 Manage Topics (bitnami)
```bash
KAFKA_PASS=$(kubectl get secret kafka-credentials -n streaming \
  -o jsonpath='{.data.kafka-password}' | base64 -d)

# Create a topic
kubectl run kafka-admin --rm -it --restart=Never \
  --image bitnami/kafka:4.0 \
  -- kafka-topics.sh \
    --bootstrap-server kafka.streaming.svc.cluster.local:9092 \
    --command-config /opt/bitnami/kafka/config/sasl-client.properties \
    --create --topic my-topic \
    --partitions 3 \
    --replication-factor 1

# List topics
kubectl run kafka-admin --rm -it --restart=Never \
  --image bitnami/kafka:4.0 \
  -- kafka-topics.sh \
    --bootstrap-server kafka.streaming.svc.cluster.local:9092 \
    --list

# Describe a topic
kubectl run kafka-admin --rm -it --restart=Never \
  --image bitnami/kafka:4.0 \
  -- kafka-topics.sh \
    --bootstrap-server kafka.streaming.svc.cluster.local:9092 \
    --describe --topic my-topic

# Delete a topic
kubectl run kafka-admin --rm -it --restart=Never \
  --image bitnami/kafka:4.0 \
  -- kafka-topics.sh \
    --bootstrap-server kafka.streaming.svc.cluster.local:9092 \
    --delete --topic my-topic
```

---

## 3. Strimzi Kafka Operator — Kafka 3.9.0 SCRAM

### 3.1 What Is Strimzi?
Strimzi is a **Kubernetes operator** that manages Apache Kafka clusters using Custom Resource Definitions (CRDs). Instead of running Helm commands or editing StatefulSets directly, you declare your desired Kafka cluster in a `Kafka` YAML object and Strimzi reconciles the cluster to match.

Benefits over plain Helm:
- Rolling upgrades with zero downtime
- Automatic `KafkaUser` credential rotation
- CRD-based topic management (`KafkaTopic`)
- Built-in TLS and SCRAM-SHA-512 authentication
- Operator handles broker scale-out and rebalancing

### 3.2 Deploy Strimzi Operator
```bash
# Via ArgoCD (recommended — wave -10 ensures CRDs are ready first)
kubectl apply -f argocd-apps/app-strimzi-operator.yaml

# Or manually via Helm
helm upgrade --install strimzi-operator strimzi/strimzi-kafka-operator \
  --version 1.1.0 \
  --namespace strimzi-system \
  --create-namespace \
  -f helm/strimzi-operator/values.yaml

# Wait for operator to be ready
kubectl rollout status deployment/strimzi-cluster-operator -n strimzi-system
```

### 3.3 Deploy the Kafka Cluster
```bash
kubectl apply -f manifests/strimzi/kafka-cluster.yaml

# Wait for cluster to be Ready
kubectl wait kafka/strimzi-kafka -n streaming \
  --for=condition=Ready \
  --timeout=300s
```

### 3.4 Check Cluster Status
```bash
# All Strimzi resources at once
kubectl get kafka,kafkanodepool,kafkauser,kafkatopic -n streaming

# Cluster details
kubectl describe kafka strimzi-kafka -n streaming

# Pods
kubectl get pods -n streaming -l strimzi.io/cluster=strimzi-kafka

# Broker logs
kubectl logs -n streaming strimzi-kafka-broker-0 -f
```

### 3.5 Get Strimzi User Passwords
```bash
# kafka-app-user
kubectl get secret kafka-app-user -n streaming \
  -o jsonpath='{.data.password}' | base64 -d

# debezium-user
kubectl get secret debezium-user -n streaming \
  -o jsonpath='{.data.password}' | base64 -d

# schema-registry-user
kubectl get secret schema-registry-user -n streaming \
  -o jsonpath='{.data.password}' | base64 -d
```

### 3.6 Add a New Topic via CRD
```bash
cat <<'EOF' | kubectl apply -f -
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: events-raw
  namespace: streaming
  labels:
    strimzi.io/cluster: strimzi-kafka
spec:
  partitions: 6
  replicas: 1
  config:
    retention.ms: "604800000"   # 7 days
    segment.bytes: "104857600"  # 100 MB segments
EOF
```

### 3.7 Add a New User via CRD
```bash
cat <<'EOF' | kubectl apply -f -
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: my-app-user
  namespace: streaming
  labels:
    strimzi.io/cluster: strimzi-kafka
spec:
  authentication:
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: events-raw
          patternType: literal
        operations: [Read, Write, Describe]
        host: "*"
      - resource:
          type: group
          name: my-app-consumer-group
          patternType: literal
        operations: [Read]
        host: "*"
EOF

# Get the auto-generated password
kubectl get secret my-app-user -n streaming \
  -o jsonpath='{.data.password}' | base64 -d
```

### 3.8 Consumer Group Lag
```bash
KAFKA_PASS=$(kubectl get secret kafka-app-user -n streaming \
  -o jsonpath='{.data.password}' | base64 -d)

kubectl run kafka-lag --rm -it --restart=Never \
  --image bitnami/kafka:3.9 \
  -- kafka-consumer-groups.sh \
    --bootstrap-server strimzi-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092 \
    --describe \
    --group my-consumer-group \
    --command-config /tmp/scram.properties
```

---

## 4. Confluent Schema Registry

### 4.1 What Is Schema Registry?
Schema Registry provides a **centralized repository for Avro, JSON Schema, and Protobuf schemas**. Producers and consumers validate messages against registered schemas, preventing incompatible schema changes from breaking downstream consumers. It enforces a configurable **compatibility mode** (BACKWARD, FORWARD, FULL).

### 4.2 Register a Schema
```bash
# Register an Avro schema
curl -X POST \
  http://192.168.1.50:30810/subjects/user-events-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"UserEvent\",\"fields\":[{\"name\":\"user_id\",\"type\":\"long\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"ts\",\"type\":\"long\"}]}"
  }'
```

### 4.3 List & Inspect Schemas
```bash
# List all subjects
curl http://192.168.1.50:30810/subjects

# List all versions of a subject
curl http://192.168.1.50:30810/subjects/user-events-value/versions

# Get a specific version
curl http://192.168.1.50:30810/subjects/user-events-value/versions/1

# Get latest schema
curl http://192.168.1.50:30810/subjects/user-events-value/versions/latest

# Check compatibility before registering
curl -X POST \
  http://192.168.1.50:30810/compatibility/subjects/user-events-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "<new-schema-json>"}'
```

### 4.4 Delete a Schema
```bash
# Delete all versions of a subject (soft delete)
curl -X DELETE http://192.168.1.50:30810/subjects/user-events-value

# Hard delete (permanent, requires ?permanent=true)
curl -X DELETE "http://192.168.1.50:30810/subjects/user-events-value?permanent=true"
```

---

## 5. Debezium Change Data Capture (CDC)

### 5.1 What Is Debezium?
Debezium is a distributed platform for **Change Data Capture (CDC)**. It monitors database transaction logs (PostgreSQL WAL, MongoDB oplog, Oracle redo log) and streams every row-level INSERT, UPDATE, DELETE to Kafka topics in near real-time. This enables:
- Real-time data synchronization between systems
- Event-driven microservices reacting to data changes
- Audit trail with full before/after row images

### 5.2 Check Debezium Status
```bash
# List all registered connectors
curl http://192.168.1.50:30083/connectors

# Connector status
curl http://192.168.1.50:30083/connectors/postgres-connector/status | python3 -m json.tool

# Connector config
curl http://192.168.1.50:30083/connectors/postgres-connector/config
```

### 5.3 Register a PostgreSQL CDC Connector
```bash
PG_PASS=$(kubectl get secret postgresql-credentials -n databases \
  -o jsonpath='{.data.postgres-password}' | base64 -d)

curl -X POST http://192.168.1.50:30083/connectors \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"postgres-cdc\",
    \"config\": {
      \"connector.class\": \"io.debezium.connector.postgresql.PostgresConnector\",
      \"database.hostname\": \"postgresql.databases.svc.cluster.local\",
      \"database.port\": \"5432\",
      \"database.user\": \"postgres\",
      \"database.password\": \"${PG_PASS}\",
      \"database.dbname\": \"metadata\",
      \"database.server.name\": \"platform\",
      \"plugin.name\": \"pgoutput\",
      \"topic.prefix\": \"cdc\",
      \"table.include.list\": \"public.events,public.users\",
      \"slot.name\": \"debezium_slot\",
      \"publication.name\": \"debezium_publication\",
      \"snapshot.mode\": \"initial\"
    }
  }"
```

### 5.4 Register a MongoDB CDC Connector
```bash
MONGO_PASS=$(kubectl get secret mongodb-credentials -n databases \
  -o jsonpath='{.data.mongodb-root-password}' | base64 -d)

curl -X POST http://192.168.1.50:30083/connectors \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"mongodb-cdc\",
    \"config\": {
      \"connector.class\": \"io.debezium.connector.mongodb.MongoDbConnector\",
      \"mongodb.hosts\": \"rs0/mongodb.databases.svc.cluster.local:27017\",
      \"mongodb.user\": \"root\",
      \"mongodb.password\": \"${MONGO_PASS}\",
      \"topic.prefix\": \"mongo\",
      \"collection.include.list\": \"mydb.events\"
    }
  }"
```

### 5.5 Connector Management
```bash
# Pause a connector
curl -X PUT http://192.168.1.50:30083/connectors/postgres-cdc/pause

# Resume a connector
curl -X PUT http://192.168.1.50:30083/connectors/postgres-cdc/resume

# Restart a connector
curl -X POST http://192.168.1.50:30083/connectors/postgres-cdc/restart

# Restart a specific task
curl -X POST http://192.168.1.50:30083/connectors/postgres-cdc/tasks/0/restart

# Delete a connector
curl -X DELETE http://192.168.1.50:30083/connectors/postgres-cdc

# Update connector config
curl -X PUT http://192.168.1.50:30083/connectors/postgres-cdc/config \
  -H "Content-Type: application/json" \
  -d '{"connector.class": "io.debezium.connector.postgresql.PostgresConnector", ...}'
```

### 5.6 PostgreSQL Prerequisites for CDC
```bash
# On PostgreSQL — enable logical replication
# Set in postgresql.conf:
# wal_level = logical
# max_replication_slots = 5
# max_wal_senders = 5

# Grant replication to the CDC user
psql -U postgres -c "ALTER USER postgres REPLICATION;"

# Create a publication for specific tables (Debezium can also create this)
psql -U postgres -d metadata -c \
  "CREATE PUBLICATION debezium_publication FOR TABLE public.events, public.users;"
```

---

## 6. AKHQ — Kafka Web UI

### 6.1 Features
AKHQ provides a comprehensive web UI for managing the entire Kafka ecosystem:
- Browse topics, view messages, manage partitions and offsets
- Monitor consumer group lag in real time
- View and manage Schema Registry subjects
- Monitor Kafka Connect connectors

### 6.2 Access
```
URL: http://192.168.1.50:30808
```

### 6.3 Key Workflows in AKHQ UI
| Task | Navigation |
|---|---|
| Browse topic messages | Topics → `<topic-name>` → Data tab |
| Reset consumer group offset | Consumer Groups → `<group>` → Offsets → Reset |
| View consumer lag | Consumer Groups → `<group>` → Lag column |
| Register schema | Schema Registry → Create |
| Check connector status | Connect → `<connector>` → Status |
| Produce test message | Topics → `<topic>` → Produce |

---

## 7. Troubleshooting

### 7.1 Kafka Pod Not Starting
```bash
kubectl describe pod -n streaming -l app.kubernetes.io/name=kafka
kubectl logs -n streaming <kafka-pod> --previous
# Check PVC is bound
kubectl get pvc -n streaming
```

### 7.2 Consumer Group Stuck / High Lag
```bash
# Check consumer group status
kubectl run kafka-cg --rm -it --restart=Never \
  --image bitnami/kafka:4.0 \
  -- kafka-consumer-groups.sh \
    --bootstrap-server kafka.streaming.svc.cluster.local:9092 \
    --describe --group <group-name>

# Reset offset to latest (skip stuck messages)
kubectl run kafka-reset --rm -it --restart=Never \
  --image bitnami/kafka:4.0 \
  -- kafka-consumer-groups.sh \
    --bootstrap-server kafka.streaming.svc.cluster.local:9092 \
    --group <group-name> \
    --topic <topic-name> \
    --reset-offsets --to-latest --execute
```

### 7.3 Debezium Connector FAILED State
```bash
# Check error
curl http://192.168.1.50:30083/connectors/postgres-cdc/status | python3 -m json.tool

# Common causes:
# - Database password changed → update connector config
# - Replication slot overflowed → drop and recreate slot
# - WAL position lost → set snapshot.mode=initial and restart

# Restart the connector
curl -X POST http://192.168.1.50:30083/connectors/postgres-cdc/restart
```

### 7.4 Schema Registry 409 Conflict
```bash
# Check the compatibility mode
curl http://192.168.1.50:30810/config

# Temporarily relax compatibility for a subject
curl -X PUT http://192.168.1.50:30810/config/my-subject \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "NONE"}'
# Register new schema
# Then restore compatibility
curl -X PUT http://192.168.1.50:30810/config/my-subject \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'
```
