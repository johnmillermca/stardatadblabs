# Confluent Schema Registry

## Overview
Confluent Schema Registry 7.9.0 — manages Avro, JSON Schema, and Protobuf schemas for Kafka topics. Connects to the Strimzi-managed Kafka cluster using SCRAM-SHA-512.

| Property | Value |
|---|---|
| Chart | `confluentinc/cp-schema-registry 0.6.0` |
| Namespace | `streaming` |
| REST API (external) | `http://192.168.1.50:30810` |
| REST API (internal) | `http://schema-registry.streaming.svc.cluster.local:8081` |
| Kafka bootstrap | `strimzi-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092` |
| Secret | `schema-registry-credentials` |

## Prerequisites
- Strimzi operator and Kafka cluster running
- `schema-registry-user` KafkaUser created (in `manifests/strimzi/kafka-cluster.yaml`)

## Deployment (ArgoCD)
ArgoCD application: `argocd-apps/app-schema-registry.yaml` (sync-wave 5)

## Register a Schema
```bash
curl -X POST \
  http://192.168.1.50:30810/subjects/test-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "{\"type\": \"string\"}"}'
```

## List Subjects
```bash
curl http://192.168.1.50:30810/subjects
```

## Secrets
| Key | Description |
|---|---|
| `username` | `schema-registry-user` |
| `password` | SCRAM password |
| `sasl-jaas-config` | Full JAAS config string for Schema Registry |

OpenBao path: `secret/data/schema-registry/credentials`
