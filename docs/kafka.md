# Apache Kafka (Strimzi KRaft)

## Overview
Strimzi-managed Kafka 4.2.0 in KRaft mode (single combined controller+broker), deployed in the `prod` namespace.

| Property | Value |
|---|---|
| Namespace | `prod` |
| Bootstrap (internal) | `strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092` |
| External NodePort | `30093` (SCRAM-SHA-512) |
| Auth | SCRAM-SHA-512 |
| Image | `192.168.1.50:30500/strimzi/kafka:latest-kafka-4.2.0-ranger` |
| Node | `worker4.local` (pinned) |
| PVC | 250Gi `local-path` (Retain) |
| Manifest | [`manifests/strimzi/kafka-cluster.yaml`](../manifests/strimzi/kafka-cluster.yaml) |

## Ranger RBAC

Kafka topic-level access control is enforced by the **Ranger Kafka plugin** baked into the custom broker image.

| Property | Value |
|---|---|
| Authorizer class | `org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer` |
| Ranger service name | `kafka` |
| Policy poll interval | 30 s |
| Config XMLs | `ConfigMap/kafka-ranger-config` → `/mnt/ranger-conf/` |
| Fallback | `allow.everyone.if.no.acl.found=true` (safe open if Ranger unreachable) |

### Custom image
The `-ranger` image extends the standard Strimzi base with all required Ranger 2.7.0 + Hadoop 3.3.6 JARs:
```bash
podman build -t 192.168.1.50:30500/strimzi/kafka:latest-kafka-4.2.0-ranger \
  docker/strimzi-kafka-ranger/
podman push 192.168.1.50:30500/strimzi/kafka:latest-kafka-4.2.0-ranger
```

### Registering the `kafka` service in Ranger
The Ranger service must exist before the broker starts downloading policies.
Register it via the Ranger Admin UI at `http://192.168.1.50:30680`:

1. **Access Manager → Service Manager** → click **+** next to **KAFKA**
2. Set **Service Name** = `kafka`
3. Set **Zookeeper Connect** = `strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092` (dummy; KRaft has no ZooKeeper — value is schema-required but ignored)
4. Set **Bootstrap Servers** = `strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092`
5. **Save**

## Deployment (ArgoCD)
ArgoCD application: `argocd-apps/app-strimzi.yaml`
Syncs `manifests/strimzi/` to the `prod` namespace.

## Manual Deploy
```bash
kubectl apply -f manifests/strimzi/kafka-cluster.yaml
kubectl wait kafka/strimzi-kafka -n prod \
  --for=condition=Ready --timeout=300s
```

## KafkaUsers
Three SCRAM-SHA-512 users are managed via `KafkaUser` CRs in the manifest:

| KafkaUser | Secret | Used by |
|---|---|---|
| `kafka-app-user` | `kafka-app-user` | General application access |
| `debezium-user` | `debezium-user` | Debezium CDC connector |
| `schema-registry-user` | `schema-registry-user` | Confluent Schema Registry |

Retrieve a password:
```bash
kubectl get secret kafka-app-user -n prod -o jsonpath='{.data.password}' | base64 -d
```

## KafkaTopics
| Topic | Partitions | Retention | Notes |
|---|---|---|---|
| `debezium-offsets` | 1 | 7 days | Compact — Debezium connector offsets |
| `debezium-configs` | 1 | 7 days | Compact — Debezium connector configs |
| `debezium-statuses` | 1 | 7 days | Compact — Debezium connector statuses |
| `schemas-internal` (`_schemas`) | 1 | forever | Compact — Schema Registry internal |
| `schema-registry-schemas` | 1 | forever | Compact — Schema Registry legacy |

## Produce / Consume (SCRAM-SHA-512)
```bash
APP_PASS=$(kubectl get secret kafka-app-user -n prod \
  -o jsonpath='{.data.password}' | base64 -d)

kubectl run kafka-client --rm -it --restart=Never \
  --image 192.168.1.50:30500/strimzi/kafka:latest-kafka-4.2.0-ranger \
  -n prod \
  -- kafka-console-producer.sh \
    --bootstrap-server strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092 \
    --topic test \
    --producer-property security.protocol=SASL_PLAINTEXT \
    --producer-property sasl.mechanism=SCRAM-SHA-512 \
    --producer-property "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=kafka-app-user password=${APP_PASS};"
```

## Secrets
| Key | Description |
|---|---|
| `kafka-app-user` | SCRAM-SHA-512 password for `kafka-app-user` (auto-managed by Strimzi) |
| `debezium-user` | SCRAM-SHA-512 password for `debezium-user` |
| `schema-registry-user` | SCRAM-SHA-512 password for `schema-registry-user` |
