# Strimzi Kafka Operator + KRaft Cluster

## Overview
Strimzi 1.1.0 — Kubernetes operator that manages Apache Kafka clusters via CRDs. Deployed with a single KRaft (no ZooKeeper) node pool running Kafka 3.9.0. Provides SCRAM-SHA-512 authentication for all clients.

| Component | Value |
|---|---|
| Operator chart | `strimzi/strimzi-kafka-operator 1.1.0` |
| Operator namespace | `strimzi-system` |
| Kafka version | `3.9.0` |
| Kafka namespace | `streaming` |
| Bootstrap (internal) | `strimzi-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092` |
| External NodePort | `192.168.1.50:30093` |
| Auth | SCRAM-SHA-512 |
| KafkaUsers | `kafka-app-user`, `debezium-user`, `schema-registry-user` |
| Topics | `debezium-offsets`, `debezium-configs`, `debezium-statuses`, `schema-registry-schemas` |

## Deployment (ArgoCD — sync waves)
```
wave -10 : app-strimzi-operator   (CRDs must exist first)
wave   0 : app-strimzi-kafka      (cluster CRs)
wave   5 : app-schema-registry    (depends on Kafka)
```

## Manual Deploy
```bash
# 1. Operator
helm upgrade --install strimzi-operator strimzi/strimzi-kafka-operator \
  --version 1.1.0 --namespace strimzi-system --create-namespace \
  -f helm/strimzi-operator/values.yaml

# 2. Wait for operator
kubectl rollout status deployment/strimzi-cluster-operator -n strimzi-system

# 3. Kafka cluster
kubectl apply -f manifests/strimzi/kafka-cluster.yaml
kubectl wait kafka/strimzi-kafka -n streaming \
  --for=condition=Ready --timeout=300s
```

## Check Cluster
```bash
kubectl get kafka,kafkanodepool,kafkauser,kafkatopic -n streaming
```

## Get KafkaUser Password
Strimzi stores each KafkaUser's SCRAM secret automatically:
```bash
kubectl get secret kafka-app-user -n streaming \
  -o jsonpath='{.data.password}' | base64 -d
```

## Add a Topic
```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: my-new-topic
  namespace: streaming
  labels:
    strimzi.io/cluster: strimzi-kafka
spec:
  partitions: 3
  replicas: 1
```
```bash
kubectl apply -f my-topic.yaml
```
