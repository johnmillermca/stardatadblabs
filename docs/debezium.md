# Debezium Kafka Connect

## Overview
Debezium 2.7 — CDC (Change Data Capture) platform. Deployed as Kafka Connect workers that capture row-level changes from PostgreSQL, MongoDB, and Oracle, streaming them to Kafka topics.

| Property | Value |
|---|---|
| Namespace | `streaming` |
| REST API | `http://192.168.1.50:30083` |
| Image | `quay.io/debezium/connect:2.7` |
| Kafka bootstrap | `kafka.streaming.svc.cluster.local:9092` |
| Secret | `debezium-credentials` |
| Manifest | `manifests/debezium/debezium-deployment.yaml` |

## Deployment (ArgoCD)
ArgoCD application: `argocd-apps/app-debezium.yaml`

## Register a PostgreSQL Connector
```bash
curl -X POST http://192.168.1.50:30083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgresql.databases.svc.cluster.local",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "<pg-password>",
      "database.dbname": "metadata",
      "database.server.name": "platform",
      "plugin.name": "pgoutput",
      "topic.prefix": "cdc"
    }
  }'
```

## Register a MongoDB Connector
```bash
curl -X POST http://192.168.1.50:30083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mongodb-connector",
    "config": {
      "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
      "mongodb.hosts": "mongodb.databases.svc.cluster.local:27017",
      "mongodb.user": "root",
      "mongodb.password": "<mongo-password>",
      "topic.prefix": "mongo"
    }
  }'
```

## List Connectors
```bash
curl http://192.168.1.50:30083/connectors
```

## Secrets
| Key | Description |
|---|---|
| `kafka-sasl-username` | `debezium-user` |
| `kafka-sasl-password` | Kafka SCRAM password |
| `pg-password` | PostgreSQL CDC user password |
| `mongo-password` | MongoDB CDC user password |
| `oracle-password` | Oracle LogMiner user password |

OpenBao path: `secret/data/debezium/credentials`
