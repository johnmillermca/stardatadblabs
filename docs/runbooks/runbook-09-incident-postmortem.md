# Runbook 09 — Incident Post-Mortems & Permanent Fixes

> **Cluster:** `192.168.1.50` (master) + workers `worker1–4.local` · **Namespace:** `prod`  
> **Status:** All issues resolved and committed to `main`

This runbook documents every degraded pod/service encountered after cluster reboots, the root cause of each failure, the immediate fix applied, and the permanent fix committed to git so the issue cannot recur.

---

## Table of Contents

### Session 1 — 2026-07-24 (Full Platform Degraded After Node Reboot)
1. [OpenBao — Auto-Unseal CronJob Not Working](#1-openbao--auto-unseal-cronjob-not-working)
2. [Strimzi Kafka — PVC Wiped on Every ArgoCD Sync](#2-strimzi-kafka--pvc-wiped-on-every-argocd-sync)
3. [Schema Registry — CrashLoopBackOff (SASL Credential Divergence)](#3-schema-registry--crashloopbackoff-sasl-credential-divergence)
4. [Debezium Connect — CrashLoopBackOff (SASL Credential Divergence)](#4-debezium-connect--crashloopbackoff-sasl-credential-divergence)
5. [Apache Doris FE — Degraded (BdbJE Peer Address Stale)](#5-apache-doris-fe--degraded-bdbje-peer-address-stale)
6. [Apache Doris BE — CrashLoopBackOff (ulimit + Rolling Update)](#6-apache-doris-be--crashloopbackoff-ulimit--rolling-update)
7. [Apache Polaris — ImagePullBackOff + Wrong Config Path](#7-apache-polaris--imagepullbackoff--wrong-config-path)
8. [ArgoCD strimzi-kafka App — Perpetual OutOfSync](#8-argocd-strimzi-kafka-app--perpetual-outofsync)

### Session 2 — 2026-07-26 (Post-Reboot Follow-Up)
9. [Doris BE — CrashLoopBackOff (vm.max_map_count Too Low)](#9-doris-be--crashloopbackoff-vmmax_map_count-too-low)
10. [Oracle XE — CrashLoopBackOff (ORA-01081 Stale Lock Files)](#10-oracle-xe--crashloopbackoff-ora-01081-stale-lock-files)
11. [Schema Registry — CrashLoopBackOff (_schemas Topic Wrong Retention Policy)](#11-schema-registry--crashloopbackoff-_schemas-topic-wrong-retention-policy)
12. [Strimzi Kafka CR — NotReady After Reboot (KRaft Controller Race)](#12-strimzi-kafka-cr--notready-after-reboot-kraft-controller-race)
13. [Kafka PVC Pruned by ArgoCD (Data-Loss Near-Miss)](#13-kafka-pvc-pruned-by-argocd-data-loss-near-miss)

### Session 3 — 2026-07-27 (Strimzi Kafka ArgoCD & Reboot-Safety Hardening)
14. [Strimzi KafkaNodePool — Unknown Sync Status (ComparisonError)](#14-strimzi-kafkanodepool--unknown-sync-status-comparisionerror)
15. [strimzi-kafka OutOfSync — PVC + CRB "Not Present in Source"](#15-strimzi-kafka-outofsync--pvc--crb-not-present-in-source)
16. [Kafka Entity Operator — 8 Crash-Loops on Reboot](#16-kafka-entity-operator--8-crash-loops-on-reboot)
17. [Kafka Data Durability — Missing Disk Flush + auto.create.topics Race](#17-kafka-data-durability--missing-disk-flush--autocreatetopics-race)
18. [Kafka PV Not in Git — Silent Rebuild Risk](#18-kafka-pv-not-in-git--silent-rebuild-risk)

### Reference
19. [Post-Reboot Recovery Checklist](#14-post-reboot-recovery-checklist)
20. [Architecture Lessons Learned](#15-architecture-lessons-learned)

---

## Session 1 — 2026-07-24

---

## 1. OpenBao — Auto-Unseal CronJob Not Working

### Symptom
After every pod restart or node reboot, OpenBao was **still sealed**. The auto-unseal CronJob reported success (exit 0) but OpenBao remained sealed.

### Root Cause
The CronJob script called `/v1/sys/health` to check whether OpenBao was sealed. **This endpoint returns HTTP 503 when sealed.** `curl -sf` interprets a non-2xx response as a failure and drops the body — the `grep` for `"sealed":true` never matched, so the unseal logic was skipped entirely.

```bash
# WRONG — returns HTTP 503 when sealed; curl -sf drops body on non-2xx
curl -sf http://openbao:8200/v1/sys/health | grep '"sealed":true'

# CORRECT — always returns HTTP 200 regardless of seal state
curl -sf http://openbao:8200/v1/sys/seal-status | grep '"sealed":true'
```

### Permanent Fix
**File:** [`manifests/openbao/openbao-auto-unseal.yaml`](../../manifests/openbao/openbao-auto-unseal.yaml)  
CronJob health-check URL corrected to `/v1/sys/seal-status`.

### Verify
```bash
kubectl get cronjob openbao-auto-unseal -n prod
curl -s http://192.168.1.50:30820/v1/sys/seal-status | python3 -m json.tool
# Expected: "sealed": false
```

---

## 2. Strimzi Kafka — PVC Wiped on Every ArgoCD Sync

### Symptom
After every ArgoCD sync of the `strimzi-kafka` application, the `data-strimzi-kafka-combined-0` PVC was deleted — wiping all Kafka topic data, consumer offsets and KRaft metadata.

### Root Cause
ArgoCD `syncPolicy.automated.prune: true` was set. Strimzi creates `PersistentVolumeClaim` objects at **runtime** as part of `KafkaNodePool` reconciliation — they are **not declared in git**. ArgoCD treated them as "extra" resources and deleted them on every sync.

> **Key insight:** `ignoreDifferences` and `argocd.argoproj.io/compare-options: IgnoreExtraneous` suppress the OutOfSync UI indicator but do **not** prevent pruning. Only `prune: false` prevents deletion.

### Permanent Fix
**File:** [`argocd-apps/app-strimzi-kafka.yaml`](../../argocd-apps/app-strimzi-kafka.yaml)

```yaml
syncPolicy:
  automated:
    prune: false      # ← MUST be false for any Strimzi-managed app
    selfHeal: true
```

Also added `ignoreDifferences` for the PVC and Strimzi-managed ClusterRoleBinding to suppress UI noise.

### If Kafka Data Was Wiped (Recovery)
```bash
# 1. Delete the empty PVC
kubectl delete pvc data-strimzi-kafka-combined-0 -n prod
# 2. Wait for Strimzi to recreate and format (~60s)
kubectl wait kafka/strimzi-kafka -n prod --for=condition=Ready --timeout=300s
# 3. Restart entity operator to re-provision SCRAM credentials
kubectl rollout restart deployment/strimzi-kafka-entity-operator -n prod
```

---

## 3. Schema Registry — CrashLoopBackOff (SASL Credential Divergence)

### Symptom
Schema Registry in `CrashLoopBackOff`. Logs: `Authentication failed due to invalid credentials with SASL mechanism SCRAM-SHA-512`.

### Root Cause
`scripts/master/12-seed-openbao-secrets.sh` generated a random password stored in `schema-registry-credentials`. Strimzi independently generates and stores its own SCRAM password in `schema-registry-user`. After any Strimzi reconciliation these two passwords diverge.

### Permanent Fix
**File:** [`manifests/schema-registry/schema-registry.yaml`](../../manifests/schema-registry/schema-registry.yaml)  
Schema Registry now reads `sasl.jaas.config` directly from the Strimzi-managed secret — see [Session 2 §11](#11-schema-registry--crashloopbackoff-_schemas-topic-wrong-retention-policy) for the full final config.

---

## 4. Debezium Connect — CrashLoopBackOff (SASL Credential Divergence)

### Symptom
Debezium Connect `Back-off restarting failed container`. Logs: `Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-512`.

### Root Cause
Same pattern as §3. The `debezium-credentials` secret contained a `kafka-sasl-jaas-config` key with a password that diverged from Strimzi's `debezium-user` secret.

### Permanent Fix
**File:** [`manifests/debezium/debezium-deployment.yaml`](../../manifests/debezium/debezium-deployment.yaml)  
All three SASL configs (main, producer, consumer) now reference the Strimzi-managed `debezium-user` secret directly.

```yaml
- name: KAFKA_SASL_PASSWORD
  valueFrom:
    secretKeyRef:
      name: debezium-user   # Strimzi-managed — always correct
      key: password
- name: KAFKA_SASL_JAAS_CONFIG
  value: >-
    org.apache.kafka.common.security.scram.ScramLoginModule required
    username="debezium-user" password="$(KAFKA_SASL_PASSWORD)";
```

---

## 5. Apache Doris FE — Degraded (BdbJE Peer Address Stale)

### Symptom
Doris FE showed `feType:UNKNOWN` in `SHOW FRONTENDS`. BdbJE stored the pod's previous IP address and could not reach its own quorum peer.

### Root Cause
Doris FE was a `Deployment` — ephemeral pod IPs change on restart. BdbJE's peer discovery stores the IP at initial cluster formation. After a reboot the new pod IP is unknown to BdbJE and the FE stays in UNKNOWN state indefinitely.

### Permanent Fix
**File:** [`manifests/doris/doris-fe-deployment.yaml`](../../manifests/doris/doris-fe-deployment.yaml)

1. `Deployment` → `StatefulSet` with `serviceName: doris-fe-headless`
2. Headless Service added (`doris-fe-headless`, ClusterIP=None)
3. `fe.conf` updated:
   ```properties
   enable_fqdn_mode = true
   advertised_address = doris-fe-0.doris-fe-headless.prod.svc.cluster.local
   priority_networks = 10.244.0.0/16
   ```
4. Entrypoint changed to `fe_entrypoint.sh`

---

## 6. Apache Doris BE — CrashLoopBackOff (ulimit + Rolling Update)

### Symptom
Doris BE `CrashLoopBackOff`. Two distinct errors: (1) `FLAGS_min_file_descriptor_number <= limit.rlim_cur` and (2) `Multi-Attach error for volume: volume is already exclusively used by another pod`.

### Root Cause
1. Node default soft `ulimit -n` was 1024; Doris BE requires ≥ 65536.
2. `strategy: RollingUpdate` starts a new pod before terminating the old — two pods race for the same `ReadWriteOnce` PVC.

### Permanent Fix
**File:** [`manifests/doris/doris-be-deployment.yaml`](../../manifests/doris/doris-be-deployment.yaml)
```yaml
strategy:
  type: Recreate     # terminate old pod before starting new (required for RWO PVC)
containers:
  - command:
    - sh
    - -c
    - |
      ulimit -n 524288   # set to node hard limit
      exec /opt/apache-doris/be_entrypoint.sh ...
```

---

## 7. Apache Polaris — ImagePullBackOff + Wrong Config Path

### Symptom
`ImagePullBackOff` + Polaris silently running on defaults (no PostgreSQL, all data lost after restart).

### Root Cause
1. `apache-polaris:latest` existed only in containerd node cache — never pushed to the private registry. When rescheduled to a different node: `ImagePullBackOff`.
2. Config path in `entrypoint.sh` was `/opt/polaris/config/application.properties`; actual ConfigMap mount was at `/opt/polaris/application.properties`.

### Permanent Fix
1. Image versioned to `apache-polaris:1.6.0` and pushed to registry.
2. Entrypoint path corrected.
3. Registry seed CronJob added: [`manifests/polaris/polaris-registry-seed.yaml`](../../manifests/polaris/polaris-registry-seed.yaml)

---

## 8. ArgoCD strimzi-kafka App — Perpetual OutOfSync

### Symptom
`strimzi-kafka` ArgoCD app perpetually `OutOfSync` with `requiresPruning=True` on PVC and ClusterRoleBinding.

### Root Cause
Both resources are created at runtime by the Strimzi operator, not in Git. With `prune: false` they show as OutOfSync in the UI (cosmetic only — no functional impact).

### Permanent Fix
`prune: false` + `ignoreDifferences` for both resources in [`argocd-apps/app-strimzi-kafka.yaml`](../../argocd-apps/app-strimzi-kafka.yaml). The OutOfSync indicator is suppressed; no pruning can occur.

---

## Session 2 — 2026-07-26

---

## 9. Doris BE — CrashLoopBackOff (vm.max_map_count Too Low)

### Pod
`doris-be-6bf7f69f48-dkt47` — `prod` namespace — **112 restarts**

### Symptom
Doris BE exits immediately on every start with `Exit Code: 0`. Log message:
```
Set kernel parameter 'vm.max_map_count' to a value greater than 2000000,
example: 'sysctl -w vm.max_map_count=2000000'
[run post_exit]
```

### Root Cause
The `be_entrypoint.sh` checks `vm.max_map_count` at startup. The host kernel value on `worker1.local` was `1,048,576` — below the required `2,000,000`. The entrypoint exits cleanly (code 0), causing a `CrashLoopBackOff` cycle. The value resets to the kernel default on every node reboot because it was never persisted to `/etc/sysctl.d/`.

### Permanent Fix — All Nodes

Run on **every node** (master + all 4 workers) to apply immediately and persist across reboots:

```bash
sysctl -w vm.max_map_count=2000000 && \
echo 'vm.max_map_count=2000000' > /etc/sysctl.d/99-doris.conf && \
sysctl --system 2>&1 | grep max_map && \
echo "Verified: $(cat /proc/sys/vm/max_map_count)"
```

| Node | IP |
|---|---|
| `master.local` | `192.168.1.50` |
| `worker1.local` | `192.168.1.51` |
| `worker2.local` | `192.168.1.52` |
| `worker3.local` | `192.168.1.53` |
| `worker4.local` | `192.168.1.54` |

**Applied:** 2026-07-26 — confirmed `vm.max_map_count = 2000000` on all 5 nodes.

### Result
```
Pod: doris-be-645759c54-rwcrn  Status: Running  Ready: 1/1  Restarts: 0
```

### Verify After Any Reboot
```bash
for node in 192.168.1.50 192.168.1.51 192.168.1.52 192.168.1.53 192.168.1.54; do
  echo "$node: $(ssh root@$node cat /proc/sys/vm/max_map_count)"
done
# Expected: 2000000 on every node
```

---

## 10. Oracle XE — CrashLoopBackOff (ORA-01081 Stale Lock Files)

### Pod
`oracle-xe-555799cbc7-ggn29` — `prod` namespace — **489 restarts**

### Symptom
Oracle XE exits with `Exit Code: 57` on every start. Log:
```
ORA-01081: cannot start already-running ORACLE - shut it down first
```

### Root Cause — Step-by-Step
```
Node reboots
  → Oracle process killed abruptly (no graceful shutdown)
  → Stale files left on PVC:
      /opt/oracle/oradata/XE/lkXE             ← instance lock
      /opt/oracle/oradata/XE/sgadef.dbf        ← SGA definition
      /opt/oracle/oradata/dbconfig/XE/lk*      ← additional lock files
  → Pod restarts → initContainer fix-permissions runs
  → cleanup was INCOMPLETE: missed dbconfig/XE/ and Oracle Home dbs/ paths
  → Main oracle-xe container starts → ORA-01081 → Exit 57 → CrashLoopBackOff
```

The existing `fix-permissions` init container was incomplete — it only cleaned `oradata/XE/lk*` and `oradata/XE/sgadef.dbf`, missing the `dbconfig/XE/` variants and the Oracle Home `dbs/` path inside the container image.

### Secondary Issue
`strategy: RollingUpdate` — wrong for a single-replica pod with a `ReadWriteOnce` PVC. Two pods would race for the same PVC during updates.

### Permanent Fix
**File:** [`manifests/oracle/oracle-deployment.yaml`](../../manifests/oracle/oracle-deployment.yaml)

Three changes:

**1. Extended `fix-permissions` init container** — covers all PVC-resident stale files:
```yaml
- name: fix-permissions
  image: busybox:1.36
  securityContext:
    runAsUser: 0
  command:
    - sh
    - -c
    - |
      chown -R 54321:54321 /opt/oracle/oradata
      rm -f /opt/oracle/oradata/XE/lk*
      rm -f /opt/oracle/oradata/XE/sgadef.dbf
      rm -f /opt/oracle/oradata/XE/.oracle_ipc_lock
      rm -f /opt/oracle/oradata/dbconfig/XE/lk*       # ← was missing
      rm -f /opt/oracle/oradata/dbconfig/XE/sgadef.dbf # ← was missing
```

**2. New `oracle-cleanup` init container** — cleans Oracle Home paths that busybox cannot know:
```yaml
- name: oracle-cleanup
  image: gvenzl/oracle-xe:21-slim   # same image = correct Oracle paths
  env:
    - name: ORACLE_BASE_HOME
      value: /opt/oracle/product/21c/dbhomeXE
  command:
    - sh
    - -c
    - |
      rm -f "${ORACLE_BASE_HOME}/dbs/sgadef.dbf"
      rm -f "${ORACLE_BASE_HOME}/dbs/lk"*
      rm -f "${ORACLE_BASE_HOME}/dbs/hc_"*.dat
      rm -f /tmp/.oracle/s*
```

**3. Strategy + graceful shutdown:**
```yaml
strategy:
  type: Recreate                        # RWO PVC — never two pods at once
terminationGracePeriodSeconds: 120      # Oracle needs time to checkpoint cleanly
```

The `terminationGracePeriodSeconds: 120` is the **primary prevention** — it gives Oracle time to shut down gracefully and not leave stale files in the first place.

### Result
```
Pod: oracle-xe-85bdb7c84f-tbj7m  Status: Running  Ready: 1/1  Restarts: 0
```

### Verify
```bash
kubectl get pod -n prod -l app=oracle-xe
kubectl logs -n prod -l app=oracle-xe --tail=5
# Expected: "DATABASE IS READY TO USE!"
```

---

## 11. Schema Registry — CrashLoopBackOff (_schemas Topic Wrong Retention Policy)

### Pod
`schema-registry-66d645f5dc-st77q` — `prod` namespace — **115 restarts**

### Symptom
Schema Registry exits with `Exit Code: 1` on every start:
```
ERROR The retention policy of the schema topic _schemas is incorrect.
      Expected cleanup.policy to be 'compact' but it is delete
ERROR Error starting the schema registry
Caused by: StoreInitializationException: The retention policy of the schema
           topic _schemas is incorrect.
```

### Root Cause — Two Compounding Issues

**Issue A — Wrong topic managed by Strimzi:**
The `KafkaTopic` CR was named `schema-registry-schemas` with correct `cleanup.policy: compact`. But Schema Registry **always uses `_schemas`** as its internal topic name (hardcoded default). These are two completely different topics. Kafka auto-created `_schemas` with `cleanup.policy=delete` (the default), which SR immediately rejects.

| KafkaTopic CR | Actual Kafka topic | Managed | cleanup.policy |
|---|---|---|---|
| `schema-registry-schemas` | `schema-registry-schemas` | ✅ Strimzi | compact |
| *(none)* | `_schemas` ← SR uses this | ❌ auto-created | **delete** ← crash |

**Issue B — ArgoCD SSA rejected JAAS_CONFIG env var:**
The `SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG` env var used `$(SCHEMA_REGISTRY_USER_PASSWORD)` variable substitution inside a `value:` field. ArgoCD's Server-Side Apply validation rejected this pattern:
```
Deployment.apps "schema-registry" is invalid:
spec.template.spec.containers[0].env[6].valueFrom: Invalid value: "":
may not be specified when `value` is not empty
```
This blocked all ArgoCD self-heal syncs, preventing the fix from being applied automatically.

### Permanent Fix

**Files changed:**
- [`manifests/strimzi/kafka-cluster.yaml`](../../manifests/strimzi/kafka-cluster.yaml) — added `schemas-internal` KafkaTopic CR
- [`manifests/schema-registry/schema-registry.yaml`](../../manifests/schema-registry/schema-registry.yaml) — updated Deployment

**Fix A — Add `_schemas` KafkaTopic CR** (CR name `schemas-internal` — RFC 1123 disallows leading underscores in `metadata.name`; use `spec.topicName` to set the real Kafka topic name):
```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: schemas-internal          # valid k8s name
  namespace: prod
  labels:
    strimzi.io/cluster: strimzi-kafka
spec:
  topicName: "_schemas"           # actual Kafka topic name SR uses
  partitions: 1
  replicas: 1
  config:
    cleanup.policy: compact
    min.compaction.lag.ms: "0"
    retention.ms: "-1"            # never expire schemas
    retention.bytes: "-1"
```

**Fix B — Read JAAS config from Strimzi secret directly** (Strimzi writes a ready-made `sasl.jaas.config` key into every KafkaUser secret):
```yaml
env:
  - name: SCHEMA_REGISTRY_KAFKASTORE_TOPIC
    value: "_schemas"             # explicit topic so SR never auto-creates it wrong
  - name: SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG
    valueFrom:
      secretKeyRef:
        name: schema-registry-user   # Strimzi-managed secret
        key: sasl.jaas.config        # ready-made JAAS string, always in sync
```

**Fix C — startupProbe** (gives Kafka time to finish KRaft log replay before SR connects):
```yaml
startupProbe:
  httpGet:
    path: /subjects
    port: 8081
  failureThreshold: 30    # 30 × 10s = 5 minutes budget
  periodSeconds: 10
```

### Result
```
Pod: schema-registry-66b6947ff8-gl4fm  Status: Running  Ready: 1/1  Restarts: 0
KafkaTopic schemas-internal: topicName=_schemas  cleanup.policy=compact  Ready=True
```

### Verify
```bash
kubectl get kafkatopic schemas-internal -n prod
# NAME               TOPICNAME   READY
# schemas-internal   _schemas    Ready

curl http://192.168.1.50:30810/subjects
# Expected: [] or list of registered schemas
```

---

## 12. Strimzi Kafka CR — NotReady After Reboot (KRaft Controller Race)

### Symptom
`kubectl get kafka strimzi-kafka -n prod` shows:
```
READY: (blank)   — Kafka CR status shows NotReady
Condition: "An error while trying to determine the active controller"
Reason: UnforceableProblem
```

### Root Cause
**Timing race on startup.** With a single combined controller+broker node, both the Strimzi operator and the Kafka pod restart simultaneously after a reboot. The operator immediately reconciles the `Kafka` CR and queries the Admin API for the active KRaft controller — but the broker is still replaying the metadata log from disk. It responds before leader election completes → operator stamps `NotReady: UnforceableProblem` on the CR.

This condition is **stale** — it persists even after Kafka fully recovers. The broker itself is Running and healthy (all SCRAM credentials replayed, all topics available). The short `terminationGracePeriodSeconds: 30` (default) made this worse — an unclean shutdown means more log to replay next boot, widening the race window.

### Permanent Fix
**File:** [`manifests/strimzi/kafka-cluster.yaml`](../../manifests/strimzi/kafka-cluster.yaml) — added to `KafkaNodePool` pod template:

```yaml
template:
  pod:
    terminationGracePeriodSeconds: 120   # was 30 (default)
```

120 seconds gives KRaft time to flush the metadata log cleanly on shutdown. A clean stop means minimal log replay on the next boot, reducing the operator race window to near zero.

### Verify
```bash
# Broker should be Running
kubectl get pod strimzi-kafka-combined-0 -n prod

# All topics should be accessible
kubectl get kafkatopic -n prod

# SCRAM credentials replayed (check broker logs)
kubectl logs strimzi-kafka-combined-0 -n prod --tail=30 | grep "Replayed UserScramCredential"
```

---

## 13. Kafka PVC Pruned by ArgoCD (Data-Loss Near-Miss)

### Timestamp
2026-07-26 01:13:41 UTC — `data-strimzi-kafka-combined-0` set to `Terminating`

### What Happened
During a forced ArgoCD sync using `ServerSideApplyForce=true` with `prune: true`, ArgoCD deleted the `data-strimzi-kafka-combined-0` PVC because it was not in Git. The PVC entered `Terminating` state.

**Why data was not immediately lost:** The `kubernetes.io/pvc-protection` finalizer prevents actual deletion while a pod is actively mounting the volume. `strimzi-kafka-combined-0` was still running and holding the mount. Had the Kafka pod restarted for any reason while in this state, the finalizer would have been released and **250Gi of Kafka data would have been permanently wiped**.

### How It Was Rescued

**Step 1 — Disable ArgoCD prune immediately:**
```bash
kubectl patch application strimzi-kafka -n argocd \
  --type=merge -p '{"spec":{"syncPolicy":{"automated":{"prune":false}}}}'
```

**Step 2 — Change PV reclaimPolicy to Retain** (protects data even if PVC is deleted):
```bash
kubectl patch pv pvc-69ed15cb-feae-4d77-9f1f-362778687016 \
  --type=merge -p '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'
```

**Step 3 — Remove PVC finalizer** (clears `Terminating` state; PV data is safe because Retain):
```bash
kubectl patch pvc data-strimzi-kafka-combined-0 -n prod \
  --type=json -p='[{"op":"remove","path":"/metadata/finalizers"}]'
```

**Step 4 — Clear PV claimRef** (PV moves from `Released` → `Available`):
```bash
kubectl patch pv pvc-69ed15cb-feae-4d77-9f1f-362778687016 \
  --type=json -p='[{"op":"remove","path":"/spec/claimRef"}]'
```

**Step 5 — Recreate PVC bound to the same PV** (static binding via `volumeName`):
```bash
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data-strimzi-kafka-combined-0
  namespace: prod
  annotations:
    argocd.argoproj.io/compare-options: IgnoreExtraneous
    strimzi.io/delete-claim: "false"
  labels:
    strimzi.io/cluster: strimzi-kafka
    strimzi.io/pool-name: combined
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: local-path
  volumeName: pvc-69ed15cb-feae-4d77-9f1f-362778687016
  resources:
    requests:
      storage: 250Gi
EOF
```

**Step 6 — Re-apply ArgoCD app from Git** to restore correct `prune: false` and `ignoreDifferences`:
```bash
kubectl apply -f argocd-apps/app-strimzi-kafka.yaml
```

### Root Cause of This Incident
During the schema-registry investigation, we manually patched the ArgoCD sync operation with `prune:true` and `ServerSideApplyForce=true` to clear an SSA conflict. This overrode the `prune: false` setting in the Git manifest and caused ArgoCD to delete the Strimzi-managed PVC.

### Permanent Prevention

1. **`prune: false` in Git** — [`argocd-apps/app-strimzi-kafka.yaml`](../../argocd-apps/app-strimzi-kafka.yaml) has `prune: false` permanently committed.
2. **Never use `prune:true` in manual `kubectl patch operation` on Strimzi apps** — always use `argocd app sync strimzi-kafka` which respects the app's configured prune policy.
3. **PV `reclaimPolicy: Retain`** — the recovered PV now has `Retain` so even if the PVC is deleted again, data survives until manually cleaned up.
4. **`ignoreDifferences` for PVC kind** — ArgoCD will not flag the Strimzi-managed PVC as OutOfSync.

### Final State
```
PVC data-strimzi-kafka-combined-0: Bound  250Gi  no deletionTimestamp ✅
PV  pvc-69ed15cb-...:               Bound  250Gi  reclaimPolicy=Retain ✅
Pod strimzi-kafka-combined-0:       1/1 Running   0 new restarts       ✅
Kafka data (61 topic partitions):   100% intact                        ✅
ArgoCD prune:                       false — cannot delete PVCs again   ✅
```

---

## 14. Post-Reboot Recovery Checklist

After any cluster reboot, follow this checklist **in order**:

```bash
# ── Step 1: All nodes Ready ─────────────────────────────────────────────────
kubectl get nodes
# All must show STATUS=Ready before proceeding

# ── Step 2: Verify vm.max_map_count on all nodes ────────────────────────────
for ip in 192.168.1.50 192.168.1.51 192.168.1.52 192.168.1.53 192.168.1.54; do
  echo "$ip: $(ssh root@$ip cat /proc/sys/vm/max_map_count 2>/dev/null || echo 'SSH failed')"
done
# All must show 2000000

# ── Step 3: OpenBao seal status ─────────────────────────────────────────────
curl -s http://192.168.1.50:30820/v1/sys/seal-status | python3 -m json.tool | grep sealed
# "sealed": false  ← if true, wait 2 min for auto-unseal CronJob
# If still sealed after 2 minutes, unseal manually:
#   kubectl get cronjob openbao-auto-unseal -n prod  ← check last run time
#   kubectl create job --from=cronjob/openbao-auto-unseal openbao-unseal-manual -n prod

# ── Step 4: All prod pods healthy ───────────────────────────────────────────
kubectl get pods -n prod | grep -vE "Running|Completed"
# Any pod NOT Running/Completed needs investigation

# ── Step 5: Strimzi Kafka ────────────────────────────────────────────────────
kubectl get pvc data-strimzi-kafka-combined-0 -n prod
# STATUS must be Bound — NEVER Terminating
# If Terminating: see §13 rescue procedure immediately

kubectl logs strimzi-kafka-combined-0 -n prod --tail=5 | grep -i "error\|started"
# Should show controller election success, no errors

# ── Step 6: Schema Registry ─────────────────────────────────────────────────
kubectl get pod -n prod -l app=schema-registry
kubectl logs -n prod -l app=schema-registry --tail=5 | grep -iE "error|started|ready"
# If CrashLoopBackOff with "_schemas" error: Strimzi topic operator may not have
# reconciled yet — wait 60s for schemas-internal KafkaTopic CR to sync

# ── Step 7: Oracle XE ───────────────────────────────────────────────────────
kubectl get pod -n prod -l app=oracle-xe
# Should be Running, 0 restarts (init containers handle stale locks)

# ── Step 8: Doris BE ────────────────────────────────────────────────────────
kubectl get pod -n prod -l app=doris-be
# Should be Running — if CrashLoopBackOff, check:
kubectl logs -n prod -l app=doris-be --tail=5 | grep -i "max_map_count"
# If max_map_count error: sysctl file not loaded — re-run sysctl --system on worker1

# ── Step 9: ArgoCD sync ──────────────────────────────────────────────────────
kubectl get application -n argocd \
  -o custom-columns="APP:.metadata.name,SYNC:.status.sync.status,HEALTH:.status.health.status"
# All apps should reach Synced/Healthy within ~5 minutes of node Ready

# ── Step 10: Verify Doris cluster ────────────────────────────────────────────
kubectl exec -n prod -l app=doris-fe -- \
  mysql -h 127.0.0.1 -P 9030 -u root --connect-timeout=5 \
  -e "SHOW FRONTENDS\G" 2>/dev/null | grep -E "feType|Alive"
# feType=MASTER, Alive=true
```

---

## 15. Architecture Lessons Learned

### 15.1 Never Use `prune: true` for Operator-Managed Applications
Any Kubernetes operator (Strimzi, cert-manager, Prometheus Operator, etc.) creates runtime resources — PVCs, Secrets, ClusterRoleBindings — that are not in git. ArgoCD `prune: true` will delete these on every sync. **Always use `prune: false`** for applications managed by a controller/operator.

### 15.2 Never Copy Credentials — Always Reference the Source of Truth
Strimzi is the single owner of SCRAM-SHA-512 passwords. Any copy in another secret will drift after Strimzi reconciles. Always reference the Strimzi-managed secret directly:
```yaml
# ✓ CORRECT
- name: MY_JAAS_CONFIG
  valueFrom:
    secretKeyRef:
      name: my-kafka-user     # Strimzi-managed
      key: sasl.jaas.config   # ready-made, always in sync

# ✗ WRONG — will drift
- name: MY_PASSWORD
  valueFrom:
    secretKeyRef:
      name: my-app-credentials   # seeded by a script
      key: kafka-password
```

### 15.3 Stateful Services Need Stable Network Identity
Any service that stores its own address in persistent metadata (BdbJE/Doris FE, etcd, Kafka KRaft, OpenSearch) must use a `StatefulSet` + headless Service. A `Deployment` breaks on restart because the stored IP becomes unreachable.

### 15.4 Kernel Parameters Must Be Persisted
`sysctl -w` changes are lost on reboot. Always persist to `/etc/sysctl.d/`:
```bash
echo 'vm.max_map_count=2000000' > /etc/sysctl.d/99-platform.conf
sysctl --system
```

### 15.5 Rolling Updates Are Incompatible with ReadWriteOnce PVCs
`strategy: RollingUpdate` starts a new pod before the old one terminates. Both pods attempt to mount the `ReadWriteOnce` PVC simultaneously → new pod stuck forever. Always use `strategy: Recreate` for single-replica stateful workloads with RWO PVCs.

### 15.6 Private Registry Images Must Be Pushed, Not Just Built
Nodes cache images in containerd. After rescheduling to a different node, only images in the private registry are pullable. Always push to `192.168.1.50:30500` during build and pin `imagePullPolicy: IfNotPresent`.

### 15.7 Health-Check Endpoints Must Return 200 in All States
Use endpoints that always return HTTP 200. `/v1/sys/health` in OpenBao returns HTTP 503 when sealed — `curl -sf` drops the body. Use `/v1/sys/seal-status` which always returns 200 with the seal state in the JSON body.

### 15.8 Graceful Shutdown Prevents Most Post-Reboot Failures
`terminationGracePeriodSeconds: 30` (Kubernetes default) is too short for stateful engines:
- **Oracle XE** needs ≥ 120s to checkpoint and close data files cleanly (prevents ORA-01081)
- **Kafka KRaft** needs ≥ 120s to flush the metadata log (prevents controller election race)
- **Doris FE** (BdbJE) needs ≥ 60s to flush BdbJE journal cleanly

### 15.9 KafkaTopic CR Names Must Be Valid RFC 1123 Subdomain Names
Kubernetes `metadata.name` must match `[a-z0-9]([-a-z0-9]*[a-z0-9])?`. Topics with names starting with `_` (e.g. `_schemas`, `__consumer_offsets`) cannot be used as CR names. Use `spec.topicName` to decouple the CR name from the Kafka topic name:
```yaml
metadata:
  name: schemas-internal   # valid k8s name
spec:
  topicName: "_schemas"    # actual Kafka topic name
```

### 15.10 Doris BE Probe Race on Startup

**Pattern**: `doris-be` pod stays `NotReady` (or restarts) after every node reboot.

**Root cause — 4 compounding factors**:

| # | Factor | Detail |
|---|--------|--------|
| 1 | `readinessProbe.failureThreshold` defaulted to 3 | 60s initial delay + 3×15s = 105s total — BE probe failed before FE sent its first heartbeat |
| 2 | No `startupProbe` | Readiness fired at 60s while FE (also booting) hadn't become leader yet |
| 3 | `terminationGracePeriodSeconds: 30` (default) | Too short for WAL flush and clean BE de-registration; stale entry in FE caused re-registration delay on next boot (`available backend num is 0`) |
| 4 | `busybox:1.36` init container used bare `docker.io` reference | Pull could stall after reboot if internet is slow / unavailable |

**Evidence in logs**:
```
W olap_server.cpp] Have not get FE Master heartbeat yet
I task_worker_pool.cpp] waiting to receive first heartbeat from frontend before doing report
```
These lines repeat every 10s well beyond the 105s probe window.

**Fix** (commit `9065494`):
```yaml
# BE Deployment
terminationGracePeriodSeconds: 120
initContainers:
  - image: 192.168.1.50:30500/busybox:1.36   # private registry, no docker.io dependency
containers:
  startupProbe:
    httpGet: { path: /api/health, port: 8040 }
    initialDelaySeconds: 30
    periodSeconds: 15
    failureThreshold: 20   # 330s max for FE to send first heartbeat
  readinessProbe:
    httpGet: { path: /api/health, port: 8040 }
    initialDelaySeconds: 0
    periodSeconds: 15
    failureThreshold: 5    # 75s tolerance for transient FE hiccups

# FE StatefulSet (same busybox + gracePeriod fix)
terminationGracePeriodSeconds: 120
initContainers:
  - image: 192.168.1.50:30500/busybox:1.36
```

**Rule**: Any BE/FE pod pair that shares a node must have a `startupProbe` with `failureThreshold ≥ 20` so the slower component doesn't kill the faster one during boot.

---

### 15.11 Kerberos KDC 108 Crash-Loops — Runtime dnf Install

**Pattern**: `kerberos-kdc` crash-loops 108× after every reboot. Pod is eventually `Ready` only if internet is available long enough for dnf to succeed.

**Root cause**: `startup.sh` ran `dnf install -y krb5-server krb5-libs krb5-workstation` on every pod start. The init container also ran `dnf install`. This required internet access (mirrors.rockylinux.org) at the moment the pod started. On a rebooted node, DNS is not yet fully resolved → `dnf` gets a curl error → `set -e` kills the script → pod exits → crash-loop.

**Evidence**: `Couldn't resolve host: mirrors.rockylinux.org` in `--previous` pod logs. 108 restarts over 6 days.

**Fix** (commit `e120ef5`):
- Built `192.168.1.50:30500/kerberos-kdc:1.0.0` — a custom image with `krb5-server`, `krb5-libs`, `krb5-workstation` baked in at build time (Dockerfile: `docker/kerberos-kdc/Dockerfile`)
- Removed the `install-kerberos` init container entirely
- Removed the `dnf install` block from `startup.sh`
- Added `strategy: Recreate` + `terminationGracePeriodSeconds: 60`

**Rule**: Never run package managers (`dnf`, `apt`, `yum`, `pip`, `npm install`) at container startup. All dependencies must be baked into the image. Containers must be able to start with zero network access.

---

### 15.12 Debezium Connect — RollingUpdate + RWO PVC

**Pattern**: `debezium-connect` stays in `ContainerCreating` or has high restart count after reboots.

**Root cause**: Default `RollingUpdate` strategy + `ReadWriteOnce` PVC (`debezium-data 50Gi`). During a rolling update, the new pod starts before the old pod fully terminates. Both pods compete to mount the same RWO volume → the new pod hangs in `ContainerCreating`.

**Fix** (commit `e120ef5`): `strategy: Recreate` + `terminationGracePeriodSeconds: 60`.

---

### 15.13 Kestra — Internet Image + RollingUpdate + RWO PVC

**Pattern**: `kestra` stays in `ImagePullBackOff` or `ContainerCreating` after reboot.

**Root cause (1)**: Image `kestra/kestra:latest-lts` referenced docker.io with no registry prefix. If the image was not cached on the node after reboot, pod entered `ImagePullBackOff` permanently (no internet in some states).

**Root cause (2)**: `RollingUpdate` strategy + `ReadWriteOnce` PVC (`kestra-storage 20Gi`) — same volume contention issue as Debezium.

**Fix** (commit `e120ef5`):
- Image pushed to private registry: `192.168.1.50:30500/kestra/kestra:latest-lts`
- `strategy: Recreate` + `terminationGracePeriodSeconds: 60`

---

### 15.14 Oracle XE Init Containers — Internet Images

**Pattern**: `oracle-xe` init containers fail on reboot if images not cached.

**Root cause**: Both init containers (`fix-permissions` using `busybox:1.36`, `oracle-cleanup` using `gvenzl/oracle-xe:21-slim`) referenced docker.io with no registry prefix.

**Fix** (commit `e120ef5`):
- `busybox:1.36` → `192.168.1.50:30500/busybox:1.36`
- `gvenzl/oracle-xe:21-slim` → `192.168.1.50:30500/gvenzl/oracle-xe:21-slim` (image pushed this session)
- Main container image updated to `192.168.1.50:30500/gvenzl/oracle-xe:21-slim`

---

### 15.15 Ranger, SQLMesh — RollingUpdate + RWO PVC

**Root cause**: Both had default `RollingUpdate` strategy with `ReadWriteOnce` PVCs.

**Fix** (commit `e120ef5`): `strategy: Recreate` on both. `terminationGracePeriodSeconds: 60` on `ranger-admin`.

---

### 15.16 MongoDB + PostgreSQL — Insufficient Termination Grace Period

**Pattern**: MongoDB and PostgreSQL take longer than expected to become ready after a reboot.

**Root cause**: Default `terminationGracePeriodSeconds: 30`. MongoDB's WiredTiger and PostgreSQL's WAL need time to flush/checkpoint cleanly. A hard SIGKILL forces crash-recovery on next boot which can be slow enough to race the readiness probe.

**Fix** (commit `e120ef5`): `terminationGracePeriodSeconds: 60` on both StatefulSets.

---

### 15.17 All 16 PVs Had reclaimPolicy: Delete — Silent Data-Loss Risk

**Pattern**: Any accidental PVC deletion (ArgoCD prune, `kubectl delete pvc`, namespace delete) would have permanently destroyed data with no recovery path.

**Root cause**: `local-path-provisioner` creates PVs with `reclaimPolicy: Delete` by default. Only Kafka and registry PVs had been manually changed to `Retain`.

**Fix** (commit `e120ef5`):
- Patched all 16 remaining PVs live: `kubectl patch pv <name> -p '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'`
- Added `manifests/storage/pv-retain.yaml` — declares all 18 PVs with `Retain` so ArgoCD asserts the policy permanently
- All 18 PVs are now `Retain`:

| PVC | Size | Protection |
|-----|------|-----------|
| doris-be-storage | 400Gi | ✅ Retain |
| doris-fe-meta | 100Gi | ✅ Retain |
| mongodb-data | 100Gi | ✅ Retain |
| postgresql-data | 100Gi | ✅ Retain |
| oracle-data | 200Gi | ✅ Retain |
| opensearch-cluster-master-* | 50Gi | ✅ Retain |
| debezium-data | 50Gi | ✅ Retain |
| prometheus-*-db | 50Gi | ✅ Retain |
| kestra-storage | 20Gi | ✅ Retain |
| registry-data | 100Gi | ✅ Retain |
| data-strimzi-kafka-combined-0 | 250Gi | ✅ Retain |
| data-openbao-0 | 10Gi | ✅ Retain |
| sqlmesh-models | 10Gi | ✅ Retain |
| grafana | 10Gi | ✅ Retain |
| ranger-data | 5Gi | ✅ Retain |
| alertmanager-*-db | 5Gi | ✅ Retain |
| hub-db-dir | 1Gi | ✅ Retain |
| kerberos-data | 1Gi | ✅ Retain |

**Rule**: After provisioning any new PVC, immediately run `kubectl patch pv <pv-name> -p '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'` and add it to `manifests/storage/pv-retain.yaml`.

---

## Session 3 — 2026-07-27

---

## 14. Strimzi KafkaNodePool — Unknown Sync Status (ComparisonError)

### App
`strimzi-kafka` ArgoCD application — `combined` KafkaNodePool sync status: **Unknown**

### Symptom
ArgoCD `strimzi-kafka` app showed `Unknown` sync status (not OutOfSync, not Synced — Unknown). ArgoCD condition:
```
Failed to compare desired state to live state: failed to calculate diff:
error calculating structured merge diff: error building typed value from config
resource: .spec.template.clusterRoleBinding: field not declared in schema
```

### Root Cause
The `KafkaNodePool` manifest had a `clusterRoleBinding` block under `spec.template`:
```yaml
# manifests/strimzi/kafka-cluster.yaml
template:
  clusterRoleBinding:           # ← NOT in KafkaNodePool CRD schema
    metadata:
      annotations:
        argocd.argoproj.io/compare-options: IgnoreExtraneous
```
The installed Strimzi 1.1.0 `KafkaNodePool` CRD **does not declare `clusterRoleBinding`** under `spec.template`. The only valid template fields are: `initContainer`, `kafkaContainer`, `perPodIngress`, `perPodRoute`, `perPodService`, `persistentVolumeClaim`, `pod`, `podSet`.

With `ServerSideApply=true`, the Kubernetes API rejects the unknown field when ArgoCD tries to build a typed merge diff — returning a `ComparisonError` that renders the entire app `Unknown`.

### Permanent Fix
**File:** [`manifests/strimzi/kafka-cluster.yaml`](../../manifests/strimzi/kafka-cluster.yaml) — removed the `clusterRoleBinding` template block from `KafkaNodePool`.

The kafka-init `ClusterRoleBinding` is handled entirely via `ignoreDifferences` in the ArgoCD app (see §15 below).

```bash
# Verify CRD schema does not include clusterRoleBinding
kubectl get crd kafkanodepools.kafka.strimzi.io \
  -o jsonpath='{.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.template.properties}' \
  | python3 -c "import sys,json; print(sorted(json.load(sys.stdin).keys()))"
# ['initContainer', 'kafkaContainer', 'perPodIngress', 'perPodRoute',
#  'perPodService', 'persistentVolumeClaim', 'pod', 'podSet']
```

### Commits
- `8ba6503` — `fix(strimzi): remove clusterRoleBinding from KafkaNodePool spec.template`

### Result
```
strimzi-kafka  Sync: Synced  Health: Healthy
KafkaNodePool combined: Synced
```

### Rule
> Before adding any field to a Strimzi CRD's `template` section, verify it exists in the live CRD schema. The schema changes between Strimzi versions. `ServerSideApply=true` rejects unknown fields at the API server — it does not silently ignore them.

---

## 15. strimzi-kafka OutOfSync — PVC + CRB "Not Present in Source"

### Symptom
`strimzi-kafka` ArgoCD app perpetually `OutOfSync` with message:
```
This resource is not present in the application's source.
It will be deleted from Kubernetes if the prune option is enabled during sync.
```
Two resources flagged:
| Resource | Kind | Why not in git |
|---|---|---|
| `data-strimzi-kafka-combined-0` | PersistentVolumeClaim | Created by Strimzi operator at runtime |
| `strimzi-prod-strimzi-kafka-kafka-init` | ClusterRoleBinding | Created by Strimzi operator at runtime |

### Root Cause — Three Compounding Issues

**Issue A — `ignoreDifferences` with `name:` filter does not suppress `requiresPruning`:**
The existing config used named entries:
```yaml
ignoreDifferences:
  - kind: PersistentVolumeClaim
    name: data-strimzi-kafka-combined-0   # ← name filter
    jsonPointers: [/]
  - kind: ClusterRoleBinding
    name: strimzi-prod-strimzi-kafka-kafka-init
    jsonPointers: [/]
```
`ignoreDifferences` with `name:` only suppresses *field-level* diffs. It does **not** suppress the `requiresPruning` flag set when a resource exists in the cluster but not in git. The app stayed OutOfSync.

**Issue B — `argocd.argoproj.io/compare-options: IgnoreExtraneous` annotation on live resources:**
The PVC had this annotation but ArgoCD v2.11 does not honour it for the overall app sync status computation. The CRB had no annotation at all.

**Issue C — No global resource exclusion for Strimzi-managed resources:**
ArgoCD had no rule to exclude Strimzi-generated PVCs and CRBs from its tracking inventory entirely.

### Permanent Fix — Two Changes

**Fix 1 — Remove `name:` filter from `ignoreDifferences`** (type-wide rule suppresses more diff paths):
**File:** [`argocd-apps/app-strimzi-kafka.yaml`](../../argocd-apps/app-strimzi-kafka.yaml)
```yaml
ignoreDifferences:
  - group: ""
    kind: PersistentVolumeClaim   # ← no name: filter = type-wide rule
    jsonPointers: [/]
  - group: "rbac.authorization.k8s.io"
    kind: ClusterRoleBinding
    jsonPointers: [/]
```

**Fix 2 — Add `resource.exclusions` to `argocd-cm`** scoped to Strimzi-labeled resources:
**File:** [`helm/argocd/values.yaml`](../../helm/argocd/values.yaml) — `configs.cm.resource.exclusions`
```yaml
resource.exclusions: |
  - apiGroups: [""]
    kinds: [PersistentVolumeClaim]
    clusters: ["*"]
    labelSelectors:
      - matchLabels:
          app.kubernetes.io/managed-by: strimzi-cluster-operator
  - apiGroups: [rbac.authorization.k8s.io]
    kinds: [ClusterRoleBinding]
    clusters: ["*"]
    labelSelectors:
      - matchLabels:
          app.kubernetes.io/managed-by: strimzi-cluster-operator
```
This removes Strimzi-operator-owned PVCs and CRBs from ArgoCD's tracking inventory entirely — the app sync hash excludes them so the app reports `Synced` regardless of whether they exist.

Also annotated the live CRB immediately:
```bash
kubectl annotate clusterrolebinding strimzi-prod-strimzi-kafka-kafka-init \
  argocd.argoproj.io/compare-options=IgnoreExtraneous --overwrite
```

### Commits
- `5be8726` — `fix(argocd/strimzi): remove name filter from ignoreDifferences`
- `461f289` — `fix(argocd): persist resource.exclusions for strimzi-managed PVC+CRB`

### Result
```
strimzi-kafka  Sync: Synced  Health: Healthy  (no OutOfSync resources)
```

### Rule
> `ignoreDifferences` with `name:` suppresses field-level diffs only. To suppress "not present in source" OutOfSync, use `resource.exclusions` in `argocd-cm` scoped by label, OR use `ignoreDifferences` without `name:` (type-wide). Never rely solely on `IgnoreExtraneous` annotations for the app-level sync status in ArgoCD v2.11.

---

## 16. Kafka Entity Operator — 8 Crash-Loops on Reboot

### Pod
`strimzi-kafka-entity-operator-784c68b99-qb66r` — `prod` namespace — **8 restarts** (both `topic-operator` and `user-operator` containers)

### Symptom
Entity operator crash-loops on every reboot / cold start. `--previous` logs:
```
WARN  ClientUtils:90 - Couldn't resolve server strimzi-kafka-kafka-bootstrap:9091
Exception in thread "main" org.apache.kafka.common.KafkaException:
  Failed to create new KafkaAdminClient
Caused by: ConfigException: No resolvable bootstrap urls given in bootstrap.servers
```
`topic-operator` and `user-operator` both exit with code 1 immediately.

### Root Cause — Two Compounding Issues

**Issue A — No startup probe:** The entity operator containers connected to `strimzi-kafka-kafka-bootstrap:9091` (Strimzi's internal mTLS port) on startup. After a node reboot, Kafka's KRaft leader election takes 60–90 seconds. The entity operator tried to connect before Kafka was ready → DNS resolved but TCP connection refused → `KafkaAdminClient` creation failed → `exit 1`. The default `livenessProbe` (or pod restart on non-zero exit) immediately restarted the container, which retried too soon and failed again → 8 crash-loops.

**Issue B — Default memory limit (512Mi) too small:** Both containers defaulted to `{}` resources — Strimzi applied 512Mi limits. Under the topic reconciliation burst on startup (replaying all KafkaTopic CRs), the JVM OOM-killed an admin client thread, causing an additional exit-code-1 failure path.

### Permanent Fix
**File:** [`manifests/strimzi/kafka-cluster.yaml`](../../manifests/strimzi/kafka-cluster.yaml) — `spec.entityOperator`

```yaml
entityOperator:
  topicOperator:
    resources:
      requests: { memory: 256Mi, cpu: 100m }
      limits:   { memory: 1Gi,   cpu: 500m }   # was 512Mi — OOM risk
    startupProbe:                                # gates liveness until Kafka ready
      initialDelaySeconds: 30
      timeoutSeconds: 10
      periodSeconds: 15
      failureThreshold: 12    # 30 + 12×15 = 210s max wait for Kafka bootstrap
    livenessProbe:
      initialDelaySeconds: 0
      timeoutSeconds: 10
      periodSeconds: 30
      failureThreshold: 3
    readinessProbe:
      initialDelaySeconds: 0
      timeoutSeconds: 10
      periodSeconds: 30
      failureThreshold: 3
  userOperator:
    resources:
      requests: { memory: 128Mi, cpu: 50m  }
      limits:   { memory: 512Mi, cpu: 250m }
    livenessProbe:             # startupProbe NOT in userOperator CRD schema
      initialDelaySeconds: 60
      timeoutSeconds: 10
      periodSeconds: 30
      failureThreshold: 3
    readinessProbe:
      initialDelaySeconds: 60
      timeoutSeconds: 10
      periodSeconds: 30
      failureThreshold: 3
```

> ⚠️ **CRD schema gotcha:** `startupProbe` exists on `topicOperator` but is **not declared** in the `userOperator` CRD schema. `spec.template` is also **not declared** on the `Kafka` CR (only on `KafkaNodePool`). Adding undeclared fields causes `ComparisonError → Unknown` — same pattern as §14.

### Verify CRD schema before editing
```bash
# topicOperator fields
kubectl get crd kafkas.kafka.strimzi.io \
  -o jsonpath='{.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.entityOperator.properties.topicOperator.properties}' \
  | python3 -c "import sys,json; print(sorted(json.load(sys.stdin).keys()))"
# ['image','jvmOptions','livenessProbe','logging','readinessProbe',
#  'reconciliationIntervalMs','resources','startupProbe','watchedNamespace']

# userOperator fields
kubectl get crd kafkas.kafka.strimzi.io \
  -o jsonpath='{.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.entityOperator.properties.userOperator.properties}' \
  | python3 -c "import sys,json; print(sorted(json.load(sys.stdin).keys()))"
# ['image','jvmOptions','livenessProbe','logging','readinessProbe',
#  'reconciliationIntervalMs','resources','secretPrefix','watchedNamespace']
# NOTE: no startupProbe, no template
```

### Commits
- `31e0460` — `fix(strimzi): harden kafka for reboot safety`
- `7e21ca3` — `fix(strimzi): remove startupProbe from userOperator + spec.template from Kafka CR`

### Result
```
strimzi-kafka-entity-operator-758454fc7b-rzhfx  2/2  Running  0 restarts  ✅
topic-operator: requests=256Mi limits=1Gi  ready=true  restarts=0
user-operator:  requests=128Mi limits=512Mi ready=true  restarts=0
```

---

## 17. Kafka Data Durability — Missing Disk Flush + auto.create.topics Race

### Symptom
No immediate crash — two latent risks discovered during audit:
1. **Data loss on hard power failure:** Kafka only flushed log segments on OS page-cache eviction (default). Up to ~5 seconds of messages were unsynced to disk at any moment.
2. **Topic config corruption:** `auto.create.topics.enable: true` caused Kafka to auto-create topics with default settings (e.g. `cleanup.policy=delete`) before the Strimzi topic operator could apply the `KafkaTopic` CR spec. This silently corrupted the Debezium and schema-registry topics on first boot.

### Root Cause
Both are Kafka defaults that were not overridden:
- `log.flush.interval.messages` defaults to `Long.MAX_VALUE` (never flush explicitly)
- `log.flush.interval.ms` defaults to `Long.MAX_VALUE` (OS controls all flushing)
- `auto.create.topics.enable` defaults to `true`

### Permanent Fix
**File:** [`manifests/strimzi/kafka-cluster.yaml`](../../manifests/strimzi/kafka-cluster.yaml) — `spec.kafka.config`

```yaml
config:
  # Disable auto-create: Strimzi topic operator manages all topics declaratively.
  # auto.create.topics.enable=true causes Kafka to create with wrong cleanup.policy
  # before the operator applies the KafkaTopic CR spec.
  auto.create.topics.enable: "false"

  # Durability: flush log to disk every 10 000 messages OR every 1 000ms.
  # Default (flush only on OS eviction) leaves up to ~5s of data unsynced
  # on hard power failure.
  log.flush.interval.messages: "10000"
  log.flush.interval.ms: "1000"

  # Log retention: keep 7 days (already default), check every 60s.
  # Faster compaction check so _schemas and debezium topics reclaim space promptly.
  log.retention.hours: "168"
  log.retention.check.interval.ms: "60000"
```

### Commits
- `31e0460` — `fix(strimzi): harden kafka for reboot safety`

### Result
Kafka broker restarted cleanly with the new config (Strimzi rolls broker on config change). Log flush is now deterministic; topic operator is the sole owner of topic creation.

---

## 18. Kafka PV Not in Git — Silent Rebuild Risk

### Symptom
No immediate failure — discovered during audit. The Kafka data PV (`pvc-69ed15cb-feae-4d77-9f1f-362778687016`) was **not in `manifests/storage/pv-retain.yaml`**.

### Risk
If the cluster was rebuilt from scratch (or the node `worker4.local` was reimaged), ArgoCD would apply all PV manifests from git. The Kafka PV would not exist in git → `data-strimzi-kafka-combined-0` PVC would fail to bind → Kafka would create a new PVC on a new PV → **all 250Gi of Kafka data unreachable** (the old PV still exists on disk but the cluster has no record of it).

This is a silent recovery failure: no alert, no CrashLoopBackOff — Kafka starts on empty storage and data is gone.

### Root Cause
The Kafka PVC was created by the Strimzi operator at runtime (not declared in git). The PV was created by the `local-path` provisioner. Because neither is in git, they were not included in the manual PV backup in `pv-retain.yaml`.

### Permanent Fix
**File:** [`manifests/storage/pv-retain.yaml`](../../manifests/storage/pv-retain.yaml) — added full PV spec:

```yaml
# PVC 'data-strimzi-kafka-combined-0' (prod) on worker4.local
apiVersion: v1
kind: PersistentVolume
metadata:
  name: pvc-69ed15cb-feae-4d77-9f1f-362778687016
spec:
  capacity:
    storage: 250Gi
  accessModes: [ReadWriteOnce]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: local-path
  hostPath:
    path: /home/local-path-provisioner/pvc-69ed15cb-feae-4d77-9f1f-362778687016_prod_data-strimzi-kafka-combined-0
    type: DirectoryOrCreate
  claimRef:
    name: data-strimzi-kafka-combined-0
    namespace: prod
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchExpressions:
            - key: kubernetes.io/hostname
              operator: In
              values: [worker4.local]
```

### Verify
```bash
# Live PV matches git
kubectl get pv pvc-69ed15cb-feae-4d77-9f1f-362778687016 \
  -o jsonpath='name={.metadata.name} policy={.spec.persistentVolumeReclaimPolicy} status={.status.phase}{"\n"}'
# name=pvc-69ed15cb-feae-4d77-9f1f-362778687016 policy=Retain status=Bound

# PVC is Bound (never Terminating/Lost)
kubectl get pvc data-strimzi-kafka-combined-0 -n prod
```

### Commits
- `31e0460` — `fix(strimzi): harden kafka for reboot safety — entity-operator resources/probes, durability config, PV in git`

### Rule
> After **any** Strimzi-managed PVC is created, immediately add its PV to `manifests/storage/pv-retain.yaml`. The PV name and `hostPath` are visible in:
> ```bash
> kubectl get pv -o jsonpath='{range .items[?(@.spec.claimRef.name=="data-strimzi-kafka-combined-0")]}{.metadata.name} {.spec.hostPath.path}{"\n"}{end}'
> ```

---

*Last updated: 2026-07-27 — covers Session 1 (2026-07-24), Session 2 (2026-07-26), Session 3 (2026-07-27) Strimzi ArgoCD & reboot-safety hardening.*
