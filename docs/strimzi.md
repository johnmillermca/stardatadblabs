# Strimzi Kafka Operator + KRaft Cluster

## Overview
Strimzi 1.1.0 — Kubernetes operator that manages Apache Kafka clusters via CRDs. Deployed with a single KRaft (no ZooKeeper) combined controller+broker node running Kafka 4.2.0. Provides SCRAM-SHA-512 authentication for all clients.

| Component | Value |
|---|---|
| Operator version | `strimzi/strimzi-kafka-operator 1.1.0` |
| Kafka version | `4.2.0` |
| Namespace | `prod` |
| Bootstrap (internal) | `strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092` |
| External NodePort | `192.168.1.54:30093` (worker4.local) |
| Auth | SCRAM-SHA-512 |
| KafkaUsers | `kafka-app-user`, `debezium-user`, `schema-registry-user` |
| Topics | `debezium-offsets`, `debezium-configs`, `debezium-statuses`, `schema-registry-schemas`, `schemas-internal` (`_schemas`) |
| Node | `worker4.local` (pinned via KafkaNodePool affinity) |
| PVC | `data-strimzi-kafka-combined-0` — 250Gi on `local-path` |

## Deployment (ArgoCD — sync waves)
```
wave -10 : app-strimzi-operator   (CRDs must exist first)
wave   0 : app-strimzi-kafka      (cluster CRs)
wave   5 : app-schema-registry    (depends on Kafka)
```

## ArgoCD Application — Critical Settings
**File:** [`argocd-apps/app-strimzi-kafka.yaml`](../argocd-apps/app-strimzi-kafka.yaml)

```yaml
syncPolicy:
  automated:
    prune: false          # ← MUST stay false — Strimzi creates PVCs at runtime
    selfHeal: true        #   ArgoCD must never delete them
  syncOptions:
    - ServerSideApply=true
    - RespectIgnoreDifferences=true
ignoreDifferences:
  # Type-wide (no name: filter) — suppresses both field diffs AND requiresPruning
  - group: ""
    kind: PersistentVolumeClaim
    jsonPointers: [/]
  - group: rbac.authorization.k8s.io
    kind: ClusterRoleBinding
    jsonPointers: [/]
```

Additionally, `helm/argocd/values.yaml` configures `resource.exclusions` in `argocd-cm` to exclude Strimzi-labeled PVCs and CRBs from ArgoCD's tracking inventory entirely — preventing "not present in source" OutOfSync on the app level.

> ⚠️ **Never set `prune: true` or pass `prune:true` in a manual `argocd app sync` for this app.** Doing so will delete `data-strimzi-kafka-combined-0` and wipe all Kafka data. See [Runbook 09 §13](runbooks/runbook-09-incident-postmortem.md#13-kafka-pvc-pruned-by-argocd-data-loss-near-miss).

> ⚠️ **Never add fields to a Strimzi CRD `template:` section without verifying they exist in the live CRD schema.** `ServerSideApply=true` rejects unknown fields → `ComparisonError → Unknown`. See [Runbook 09 §14](runbooks/runbook-09-incident-postmortem.md#14-strimzi-kafkanodepool--unknown-sync-status-comparisionerror).

## Manual Deploy
```bash
# 1. Operator (via Helm — already deployed)
kubectl get deployment strimzi-cluster-operator -n prod

# 2. Kafka cluster
kubectl apply -f manifests/strimzi/kafka-cluster.yaml
kubectl wait kafka/strimzi-kafka -n prod --for=condition=Ready --timeout=300s
```

## Check Cluster Health
```bash
# Kafka CR status
kubectl get kafka strimzi-kafka -n prod

# All Strimzi resources
kubectl get kafka,kafkanodepool,kafkauser,kafkatopic -n prod

# Broker pod
kubectl get pod strimzi-kafka-combined-0 -n prod

# PVC — must be Bound, never Terminating
kubectl get pvc data-strimzi-kafka-combined-0 -n prod
```

## Get KafkaUser Password / JAAS Config
Strimzi stores each KafkaUser's SCRAM secret automatically. The secret contains two keys:
- `password` — raw SCRAM password
- `sasl.jaas.config` — ready-made JAAS string for client configuration

```bash
# Get password
kubectl get secret schema-registry-user -n prod \
  -o jsonpath='{.data.password}' | base64 -d

# Get ready-made JAAS config (use this in application configs)
kubectl get secret schema-registry-user -n prod \
  -o jsonpath='{.data.sasl\.jaas\.config}' | base64 -d
```

> ✅ **Always use `sasl.jaas.config` from the Strimzi-managed secret directly.** Never copy the password into another secret — it will diverge after any Strimzi reconciliation. See [Runbook 09 §15.2](runbooks/runbook-09-incident-postmortem.md#152-never-copy-credentials--always-reference-the-source-of-truth).

## KafkaTopic Management
```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: my-new-topic
  namespace: prod
  labels:
    strimzi.io/cluster: strimzi-kafka
spec:
  partitions: 3
  replicas: 1
  config:
    cleanup.policy: delete   # or compact for changelog topics
```

> **Note on topic names starting with `_`:** Kubernetes `metadata.name` must be a valid RFC 1123 subdomain — leading underscores are not allowed. Use `spec.topicName` to decouple the CR name from the Kafka topic name:
> ```yaml
> metadata:
>   name: schemas-internal   # valid k8s name
> spec:
>   topicName: "_schemas"    # actual Kafka topic name
> ```

## Hardened Kafka Configuration (as of 2026-07-27)

### Durability Settings — `spec.kafka.config`
```yaml
auto.create.topics.enable: "false"   # topic operator owns all topic creation
log.flush.interval.messages: "10000" # flush every 10k messages
log.flush.interval.ms: "1000"        # flush every 1s — caps power-failure data loss
log.retention.hours: "168"           # 7-day retention
log.retention.check.interval.ms: "60000"  # compaction check every 60s
```

### Entity Operator Resources + Probes
| Container | Memory Request | Memory Limit | Startup Probe |
|---|---|---|---|
| `topic-operator` | 256Mi | **1Gi** | ✅ `failureThreshold: 12` (210s window) |
| `user-operator` | 128Mi | 512Mi | ❌ not in CRD schema — uses `initialDelaySeconds: 60` instead |

The `startupProbe` on `topic-operator` prevents crash-loops on reboot: it gates liveness until Kafka's KRaft bootstrap is reachable, giving Kafka up to 210 seconds to complete leader election before the container is considered unhealthy.

## Post-Reboot Known Issues & Fixes

### Kafka CR shows NotReady after reboot
The Strimzi operator races KRaft controller election on startup. The CR may show:
```
NotReady: An error while trying to determine the active controller
```
This is a **stale condition** — the broker is functional. It clears on the next successful reconciliation. The `terminationGracePeriodSeconds: 120` in the KafkaNodePool template reduces this window by ensuring clean KRaft log flush on shutdown.

### Entity operator crash-loops after reboot (exit code 1)
```bash
# Check --previous logs
kubectl logs -n prod deploy/strimzi-kafka-entity-operator \
  -c topic-operator --previous 2>/dev/null | tail -20
# Look for: "No resolvable bootstrap urls" — means Kafka not ready yet
# Fix: startupProbe on topicOperator gives 210s for Kafka to become ready
# If crash-loop persists: check topic-operator memory limit (must be 1Gi)
kubectl get deployment strimzi-kafka-entity-operator -n prod \
  -o jsonpath='{.spec.template.spec.containers[*].resources}'
```

### PVC in Terminating state
**Emergency — act immediately.** See [Runbook 09 §13](runbooks/runbook-09-incident-postmortem.md#13-kafka-pvc-pruned-by-argocd-data-loss-near-miss) for the full rescue procedure. First action:
```bash
# 1. Disable prune NOW
kubectl patch application strimzi-kafka -n argocd \
  --type=merge -p '{"spec":{"syncPolicy":{"automated":{"prune":false}}}}'
# 2. Change PV reclaimPolicy to Retain
kubectl patch pv $(kubectl get pvc data-strimzi-kafka-combined-0 -n prod \
  -o jsonpath='{.spec.volumeName}') \
  --type=merge -p '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'
```

## Troubleshooting

### SASL authentication failures in clients
```bash
# Check the Strimzi-managed secret has the correct password
kubectl get secret schema-registry-user -n prod \
  -o jsonpath='{.data.sasl\.jaas\.config}' | base64 -d

# Check SCRAM credentials were replayed in broker
kubectl logs strimzi-kafka-combined-0 -n prod | grep "Replayed UserScramCredential"
```

### Kafka not starting after node reboot
```bash
kubectl describe pod strimzi-kafka-combined-0 -n prod | grep -A5 "Events:"
kubectl logs strimzi-kafka-combined-0 -n prod --previous 2>/dev/null | tail -30
```

### Topic operator not reconciling
```bash
kubectl logs -n prod deploy/strimzi-kafka-entity-operator \
  -c topic-operator --tail=30 | grep -i "error\|warn"
```

### strimzi-kafka ArgoCD app Unknown or OutOfSync
```bash
# Check ComparisonError
kubectl get application strimzi-kafka -n argocd \
  -o jsonpath='{.status.conditions}' | python3 -m json.tool

# Unknown = ComparisonError — usually an unknown field in a Strimzi CRD
# Check which field is rejected:
#   "field not declared in schema" → remove that field from the manifest
# See Runbook 09 §14 and §15 for the full diagnosis procedure.

# Hard refresh after fixing
ARGOCD_POD=$(kubectl get pod -n argocd -l app.kubernetes.io/name=argocd-server \
  -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n argocd "$ARGOCD_POD" -- \
  argocd app get strimzi-kafka --hard-refresh
```

### Verify Kafka PV is Retain and matches git
```bash
kubectl get pv pvc-69ed15cb-feae-4d77-9f1f-362778687016 \
  -o jsonpath='policy={.spec.persistentVolumeReclaimPolicy} node={.spec.nodeAffinity.required.nodeSelectorTerms[0].matchExpressions[0].values[0]} status={.status.phase}{"\n"}'
# Expected: policy=Retain node=worker4.local status=Bound

# After any new Strimzi PVC, capture the PV for pv-retain.yaml
kubectl get pv -o jsonpath='{range .items[?(@.spec.claimRef.namespace=="prod")]}{.metadata.name} {.spec.hostPath.path}{"\n"}{end}' \
  | grep kafka
```
