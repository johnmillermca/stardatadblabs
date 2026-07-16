# AKHQ — Kafka UI

## Overview
AKHQ 0.27.0 — web UI for browsing and managing Kafka topics, consumer groups, schemas, and connectors. Connects to both the Strimzi Kafka cluster and the Schema Registry.

| Property | Value |
|---|---|
| Chart | `akhq/akhq 0.27.0` |
| Namespace | `streaming` |
| UI URL | `http://192.168.1.50:30808` |
| Kafka | `strimzi-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092` |
| Schema Registry | `http://schema-registry.streaming.svc.cluster.local:8081` |
| Secret | `akhq-credentials` |

## Deployment (ArgoCD)
ArgoCD application: `argocd-apps/app-akhq.yaml`

## Features
- Browse topics, partitions, offsets
- View consumer group lag
- Produce/consume messages interactively
- Browse Schema Registry subjects
- View Kafka Connect connectors

## Secrets
| Key | Description |
|---|---|
| `kafka-sasl-username` | `kafka-app-user` |
| `kafka-sasl-password` | SCRAM password |

OpenBao path: `secret/data/akhq/credentials`
