# Confluent Schema Registry

## Overview
Confluent Schema Registry 7.9.0 — manages Avro, JSON Schema, and Protobuf schemas for Kafka topics. Connects to the Strimzi-managed Kafka cluster using SCRAM-SHA-512.

| Property | Value |
|---|---|
| Image | `confluentinc/cp-schema-registry:7.9.0` |
| Namespace | `prod` |
| REST API (external) | `http://192.168.1.54:30810` (worker4.local) |
| REST API (internal) | `http://schema-registry.prod.svc.cluster.local:8081` |
| Kafka bootstrap | `strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092` |
| Auth | SCRAM-SHA-512 via `schema-registry-user` (Strimzi-managed) |
| Internal topic | `_schemas` (managed by `schemas-internal` KafkaTopic CR) |
| Node | `worker4.local` (co-located with Kafka) |

## Prerequisites
- Strimzi Kafka cluster running (`strimzi-kafka-combined-0` pod Ready)
- `schema-registry-user` KafkaUser created and SCRAM credentials provisioned
- `schemas-internal` KafkaTopic CR applied (creates `_schemas` with `cleanup.policy=compact`)

## Deployment (ArgoCD)
ArgoCD application: `argocd-apps/app-schema-registry.yaml` (sync-wave 5)

## SASL Authentication — Important Design Notes

Schema Registry reads its JAAS config directly from the Strimzi-managed `schema-registry-user` secret:

```yaml
env:
  - name: SCHEMA_REGISTRY_KAFKASTORE_TOPIC
    value: "_schemas"
  - name: SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG
    valueFrom:
      secretKeyRef:
        name: schema-registry-user   # Strimzi-managed — always in sync with broker
        key: sasl.jaas.config        # ready-made JAAS string Strimzi writes here
```

> ⚠️ **Never use a separately generated copy of the password.** Strimzi rotates SCRAM credentials in `schema-registry-user` independently. Any copy in another secret will diverge and cause `CrashLoopBackOff`. See [Runbook 09 §3](runbooks/runbook-09-incident-postmortem.md#3-schema-registry--crashloopbackoff-sasl-credential-divergence).

## Internal Topic — _schemas

Schema Registry uses `_schemas` (not the KafkaTopic name) as its internal storage topic. This topic **must** have `cleanup.policy=compact`. Kafka auto-creates topics with `cleanup.policy=delete` by default, which causes SR to fail startup immediately.

The `schemas-internal` KafkaTopic CR in [`manifests/strimzi/kafka-cluster.yaml`](../manifests/strimzi/kafka-cluster.yaml) manages this:
```yaml
metadata:
  name: schemas-internal    # RFC 1123 valid name
spec:
  topicName: "_schemas"     # actual Kafka topic SR uses
  config:
    cleanup.policy: compact
    retention.ms: "-1"      # never expire schemas
    retention.bytes: "-1"
```

> ⚠️ **Do not delete the `schemas-internal` KafkaTopic CR.** If `_schemas` is recreated with `cleanup.policy=delete`, Schema Registry will crash on every startup. See [Runbook 09 §11](runbooks/runbook-09-incident-postmortem.md#11-schema-registry--crashloopbackoff-_schemas-topic-wrong-retention-policy).

## Register a Schema
```bash
curl -X POST \
  http://192.168.1.54:30810/subjects/my-topic-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "{\"type\": \"record\", \"name\": \"MyRecord\", \"fields\": [{\"name\": \"id\", \"type\": \"int\"}]}"}'
```

## List Subjects
```bash
curl http://192.168.1.54:30810/subjects
```

## Get Schema by Subject
```bash
curl http://192.168.1.54:30810/subjects/my-topic-value/versions/latest
```

## Check Compatibility
```bash
curl -X POST \
  http://192.168.1.54:30810/compatibility/subjects/my-topic-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "..."}'
```

## Troubleshooting

### CrashLoopBackOff — `_schemas` retention policy error
```
ERROR The retention policy of the schema topic _schemas is incorrect.
      Expected cleanup.policy to be 'compact' but it is delete
```
**Cause:** `schemas-internal` KafkaTopic CR is missing or not yet reconciled.  
**Fix:**
```bash
# Check if KafkaTopic CR exists and is Ready
kubectl get kafkatopic schemas-internal -n prod

# If missing, re-apply the strimzi manifests
kubectl apply -f manifests/strimzi/kafka-cluster.yaml

# Wait for Strimzi topic operator to reconcile (~30s), then restart SR
kubectl rollout restart deployment/schema-registry -n prod
```

### CrashLoopBackOff — SASL authentication failure
```
ERROR Authentication failed due to invalid credentials with SASL mechanism SCRAM-SHA-512
```
**Cause:** Schema Registry is using a stale/wrong password. This happens if the `SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG` env var references a secret other than the Strimzi-managed `schema-registry-user`.  
**Fix:** Confirm the deployment reads from `schema-registry-user`:
```bash
kubectl get deployment schema-registry -n prod \
  -o jsonpath='{.spec.template.spec.containers[0].env}' | python3 -m json.tool | grep -A5 "JAAS"
# Should show: secretKeyRef.name = schema-registry-user, key = sasl.jaas.config
```

### Pod stuck in startupProbe (slow Kafka startup after reboot)
The `startupProbe` allows up to 5 minutes (30 × 10s) for Kafka to become ready after a reboot. This is intentional — SR will start automatically once Kafka's KRaft log replay completes. No action required.

### Check current JAAS config being used
```bash
kubectl get secret schema-registry-user -n prod \
  -o jsonpath='{.data.sasl\.jaas\.config}' | base64 -d
```
