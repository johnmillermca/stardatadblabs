# Apache Kafka (bitnami) + Strimzi KRaft Cluster

## Overview
Two Kafka deployments are available:
1. **bitnami/kafka 32.4.3** — Kafka 4.0, KRaft mode, SASL/PLAIN, general purpose
2. **Strimzi 1.1.0 / Kafka 3.9.0** — Operator-managed, SCRAM-SHA-512, used by Schema Registry and Debezium

| Property | bitnami/kafka | strimzi/kafka |
|---|---|---|
| Namespace | `streaming` | `streaming` |
| Bootstrap (internal) | `kafka.streaming.svc.cluster.local:9092` | `strimzi-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092` |
| External NodePort | `30092` | `30093` |
| Auth | SASL/PLAIN | SCRAM-SHA-512 |
| Secret | `kafka-credentials` | `strimzi-kafka-kafka-app-user` (Strimzi KafkaUser) |

## bitnami/kafka Deployment
```bash
helm upgrade --install kafka bitnami/kafka \
  --version 32.4.3 \
  --namespace streaming \
  -f helm/kafka/values.yaml
```

## Strimzi Kafka Deployment (KRaft)
1. Deploy operator first:
```bash
# Via ArgoCD (wave -10):
kubectl apply -f argocd-apps/app-strimzi-operator.yaml
```
2. Wait for CRDs, then:
```bash
kubectl apply -f manifests/strimzi/kafka-cluster.yaml
kubectl wait kafka/strimzi-kafka -n streaming \
  --for=condition=Ready --timeout=300s
```

## Produce / Consume (bitnami)
```bash
KAFKA_PASS=$(kubectl get secret kafka-credentials -n streaming \
  -o jsonpath='{.data.kafka-password}' | base64 -d)

kubectl run kafka-client --rm -it --restart=Never \
  --image bitnami/kafka:4.0 \
  -- kafka-console-producer.sh \
    --bootstrap-server kafka.streaming.svc.cluster.local:9092 \
    --topic test \
    --producer-property security.protocol=SASL_PLAINTEXT \
    --producer-property sasl.mechanism=PLAIN \
    --producer-property "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=kafka-user password=${KAFKA_PASS};"
```

## Strimzi KafkaUser Password
Strimzi creates a Kubernetes Secret named after the KafkaUser:
```bash
kubectl get secret kafka-app-user -n streaming -o jsonpath='{.data.password}' | base64 -d
```
