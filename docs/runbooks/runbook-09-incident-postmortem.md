# Runbook 09 — Incident Post-Mortem & Permanent Fixes

> **Cluster:** `192.168.1.50` (master) + workers `worker1–4.local` · **Namespace:** `prod`  
> **Date:** 2026-07-24 · **Scope:** Full platform degraded state after node reboot  
> **Status:** All issues resolved and committed to `main`

This runbook documents **every degraded pod/service** encountered after a cluster reboot, the root cause of each failure, the immediate fix applied, and the **permanent fix** committed to git so the issue cannot recur after the next reboot.

---

## Table of Contents

1. [Incident Summary](#1-incident-summary)
2. [OpenBao — Auto-Unseal CronJob Not Working](#2-openbao--auto-unseal-cronjob-not-working)
3. [Strimzi Kafka — PVC Wiped on Every ArgoCD Sync](#3-strimzi-kafka--pvc-wiped-on-every-argocd-sync)
4. [Schema Registry — CrashLoopBackOff (SASL Credential Divergence)](#4-schema-registry--crashloopbackoff-sasl-credential-divergence)
5. [Debezium Connect — CrashLoopBackOff (SASL Credential Divergence)](#5-debezium-connect--crashloopbackoff-sasl-credential-divergence)
6. [Apache Doris FE — Degraded (BdbJE Peer Address Stale)](#6-apache-doris-fe--degraded-bdbje-peer-address-stale)
7. [Apache Doris BE — CrashLoopBackOff (ulimit + Rolling Update)](#7-apache-doris-be--crashloopbackoff-ulimit--rolling-update)
8. [Apache Polaris — ImagePullBackOff + Wrong Config Path](#8-apache-polaris--imagepullbackoff--wrong-config-path)
9. [ArgoCD strimzi-kafka App — Perpetual OutOfSync](#9-argocd-strimzi-kafka-app--perpetual-outofsync)
10. [Node-Level — Kernel Parameters Not Persistent](#10-node-level--kernel-parameters-not-persistent)
11. [Post-Reboot Recovery Checklist](#11-post-reboot-recovery-checklist)
12. [Architecture Lessons Learned](#12-architecture-lessons-learned)

---

## 1. Incident Summary

After a full cluster reboot, the following services were degraded or in `CrashLoopBackOff`:

| Service | Symptom | Root Cause Category |
|---|---|---|
| OpenBao | Still sealed after restart | CronJob health-check URL wrong |
| Strimzi Kafka | PVC deleted every ArgoCD sync | ArgoCD `prune: true` on operator-managed PVC |
| Schema Registry | `CrashLoopBackOff` | SASL password in `debezium-credentials` diverged from Strimzi |
| Debezium Connect | `Back-off restarting` | Same SASL credential divergence as Schema Registry |
| Doris FE | `feType:UNKNOWN`, BdbJE stuck | Pod IP changed at reboot; BdbJE stored old IP |
| Doris BE | `CrashLoopBackOff` | `ulimit -n` hard limit exceeded; RWO PVC RollingUpdate conflict |
| Polaris | `ImagePullBackOff` | Image only in containerd cache, not in private registry |
| ArgoCD strimzi-kafka | Perpetual `OutOfSync` | `prune: true` competing with Strimzi-managed runtime resources |

All issues have been permanently fixed in git. The sections below document each one individually with reproduction steps, root cause analysis, and the exact files changed.

---

## 2. OpenBao — Auto-Unseal CronJob Not Working

### Symptom
After every pod restart or node reboot, OpenBao was **still sealed**. The auto-unseal CronJob reported success (exit 0) but OpenBao remained sealed.

### Root Cause
The CronJob script called `/v1/sys/health` to check whether OpenBao was sealed. **This endpoint returns HTTP 503 when OpenBao is sealed.** `curl -sf` interprets a non-2xx response as a failure, so the response body (which contains `"sealed":true`) was never received — the `grep` for `"sealed":true` never matched — and the unseal logic was skipped entirely.

```bash
# WRONG — returns HTTP 503 when sealed; curl -sf drops body on non-2xx
curl -sf http://openbao:8200/v1/sys/health | grep '"sealed":true'

# CORRECT — always returns HTTP 200 regardless of seal state
curl -sf http://openbao:8200/v1/sys/seal-status | grep '"sealed":true'
```

### Permanent Fix

**File changed:** [`manifests/openbao/openbao-auto-unseal.yaml`](../../manifests/openbao/openbao-auto-unseal.yaml)

The CronJob was converted from an imperative `kubectl apply` in a setup script into a tracked git manifest, and the health-check URL was corrected to `/v1/sys/seal-status`.

**ArgoCD app added:** [`argocd-apps/app-prod.yaml`](../../argocd-apps/app-prod.yaml) — new `openbao-unseal` application pointing to `manifests/openbao/`.

### Verify
```bash
# Check last CronJob run
kubectl get cronjob openbao-auto-unseal -n prod
kubectl get jobs -n prod | grep openbao-auto-unseal

# Check OpenBao seal status
curl -s http://192.168.1.50:30820/v1/sys/seal-status | python3 -m json.tool
# "sealed": false  ← expected
```

---

## 3. Strimzi Kafka — PVC Wiped on Every ArgoCD Sync

### Symptom
After every ArgoCD sync of the `strimzi-kafka` application, the `data-strimzi-kafka-combined-0` PVC was **deleted** — wiping all Kafka topic data and SCRAM credential metadata stored in KRaft. On the next Strimzi reconciliation a fresh empty PVC was created, Kafka reformatted the storage, and all consumer offsets and user credentials were lost.

### Root Cause
The ArgoCD `strimzi-kafka` app was configured with `syncPolicy.syncOptions: prune: true`. Strimzi creates `PersistentVolumeClaim` objects at **runtime** as part of the `KafkaNodePool` operator reconciliation loop — they are **not declared in git**. ArgoCD therefore treated them as "extra" resources and deleted them on every sync.

`ignoreDifferences` and the `argocd.argoproj.io/compare-options: IgnoreExtraneous` annotation on the `KafkaNodePool` template are **not sufficient** — they prevent the resource from showing as OutOfSync in the UI, but they do **not** prevent ArgoCD from pruning it. Only `prune: false` prevents deletion.

### Permanent Fix

**File changed:** [`argocd-apps/app-prod.yaml`](../../argocd-apps/app-prod.yaml)

```yaml
# BEFORE
- name: strimzi-kafka
  syncPolicy:
    automated:
      prune: true      # ← was deleting Strimzi-managed PVC

# AFTER
- name: strimzi-kafka
  syncPolicy:
    automated:
      prune: false     # ← Strimzi manages its own PVCs; ArgoCD must not prune them
```

**Defence-in-depth:** [`manifests/strimzi/kafka-cluster.yaml`](../../manifests/strimzi/kafka-cluster.yaml) — added `IgnoreExtraneous` annotation to the `KafkaNodePool` PVC template so ArgoCD does not flag it as OutOfSync even if `prune` is ever re-enabled.

### After a Kafka Data Wipe (Recovery Steps)
If the PVC was already wiped before this fix was applied:

```bash
# 1. Delete the empty PVC so Strimzi can create a fresh one
kubectl delete pvc data-strimzi-kafka-combined-0 -n prod

# 2. Wait for Strimzi to recreate it and format the broker (~60s)
kubectl wait kafka/strimzi-kafka -n prod --for=condition=Ready --timeout=300s

# 3. Restart the entity operator to re-provision SCRAM credentials into fresh broker metadata
kubectl rollout restart deployment/strimzi-kafka-entity-operator -n prod

# 4. Verify users are provisioned
kubectl get secret schema-registry-user debezium-user kafka-app-user -n prod
```

---

## 4. Schema Registry — CrashLoopBackOff (SASL Credential Divergence)

### Symptom
Schema Registry pod in `CrashLoopBackOff`. Logs showed:

```
WARN  Authentication failed due to invalid credentials with SASL mechanism SCRAM-SHA-512
```

### Root Cause
The setup script [`scripts/master/12-seed-openbao-secrets.sh`](../../scripts/master/12-seed-openbao-secrets.sh) generated a **random password** and stored it in the `schema-registry-credentials` Kubernetes secret. This password diverged from the one Strimzi stored in the `schema-registry-user` secret.

Strimzi is the **single source of truth** for SCRAM-SHA-512 passwords — it generates them, stores them in `schema-registry-user`, and programs them into the Kafka broker. Any separately generated copy will be wrong after a Strimzi reconciliation.

```
Strimzi generates:  schema-registry-user.password = "XyZ123..."
Seed script stored: schema-registry-credentials.password = "AbC456..."  ← diverged
Schema Registry used the seed script password → auth failed
```

### Permanent Fix

**File changed:** [`manifests/schema-registry/schema-registry.yaml`](../../manifests/schema-registry/schema-registry.yaml)

Schema Registry's deployment now reads the `SASL_JAAS_CONFIG` directly from the Strimzi-managed secret using Kubernetes env-var substitution:

```yaml
env:
  # Step 1: expose the canonical Strimzi password
  - name: SCHEMA_REGISTRY_USER_PASSWORD
    valueFrom:
      secretKeyRef:
        name: schema-registry-user   # ← Strimzi-managed, always correct
        key: password

  # Step 2: build the JAAS config referencing that password
  # IMPORTANT: SCHEMA_REGISTRY_USER_PASSWORD must be declared BEFORE this entry
  - name: SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG
    value: >-
      org.apache.kafka.common.security.scram.ScramLoginModule required
      username="schema-registry-user"
      password="$(SCHEMA_REGISTRY_USER_PASSWORD)";
```

**File also changed:** [`scripts/master/12-seed-openbao-secrets.sh`](../../scripts/master/12-seed-openbao-secrets.sh) — removed the block that seeded `schema-registry-credentials` with a competing random password.

### Verify
```bash
# Confirm pod is Running
kubectl get pod -n prod -l app=schema-registry

# Confirm it's using the Strimzi password
kubectl logs -n prod deploy/schema-registry --tail=20 | grep -i "successfully"

# Check subjects registered
curl http://192.168.1.50:30810/subjects
```

---

## 5. Debezium Connect — CrashLoopBackOff (SASL Credential Divergence)

### Symptom
Debezium Connect pod in `Back-off restarting failed container`. Logs showed:

```
ERROR  [AdminClient clientId=adminclient-1] Connection to node -1 failed authentication due to:
       Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-512
ERROR  Stopping due to error
       org.apache.kafka.connect.errors.ConnectException: Failed to connect to and describe Kafka cluster.
```

### Root Cause
Identical pattern to Schema Registry (Section 4). The `debezium-credentials` secret (seeded by `12-seed-openbao-secrets.sh`) contained a `kafka-sasl-jaas-config` key with a password that diverged from Strimzi's `debezium-user` secret after a reboot/reconciliation cycle.

Debezium uses this `kafka-sasl-jaas-config` for three separate SASL configs (main, producer, consumer). When the password is wrong, Kafka rejects the AdminClient connection immediately on startup and Debezium exits, causing the crash loop.

### Permanent Fix

**File changed:** [`manifests/debezium/debezium-deployment.yaml`](../../manifests/debezium/debezium-deployment.yaml)

Same pattern as schema-registry — read directly from the Strimzi-managed `debezium-user` secret:

```yaml
env:
  # Step 1: expose the canonical Strimzi password
  - name: KAFKA_SASL_PASSWORD
    valueFrom:
      secretKeyRef:
        name: debezium-user   # ← Strimzi-managed, always correct
        key: password

  # Step 2: compose the JAAS config using the above variable
  - name: KAFKA_SASL_JAAS_CONFIG
    value: >-
      org.apache.kafka.common.security.scram.ScramLoginModule required
      username="debezium-user"
      password="$(KAFKA_SASL_PASSWORD)";
```

### Verify
```bash
# Confirm pod is Running
kubectl get pod -n prod -l app=debezium-connect

# Confirm Debezium REST API is reachable
curl http://192.168.1.50:30083/connectors | python3 -m json.tool

# Check connector status (if connectors were already registered)
curl http://192.168.1.50:30083/connectors/postgres-cdc/status | python3 -m json.tool
```

---

## 6. Apache Doris FE — Degraded (BdbJE Peer Address Stale)

### Symptom
Doris FE pod showed `feType:UNKNOWN` in `SHOW FRONTENDS`. BdbJE (Berkeley DB Java Edition — the metadata store used by Doris FE) had recorded the pod's IP address at first boot. After a reboot, the pod received a new IP. BdbJE could not connect to its own stored peer address and remained in an unknown state forever.

```
# First boot: BdbJE records peer = 10.244.26.104
# After reboot: pod IP = 10.244.26.119
# BdbJE tries to connect to 10.244.26.104 → timeout → feType:UNKNOWN
```

### Root Cause
Doris FE was running as a `Deployment` (no stable network identity). Kubernetes assigns a fresh pod IP on every restart. BdbJE's peer discovery uses the IP it saw at initial formation — this is incompatible with ephemeral pod IPs.

### Permanent Fix

**File changed:** [`manifests/doris/doris-fe-deployment.yaml`](../../manifests/doris/doris-fe-deployment.yaml)

Four changes were made together:

1. **`Deployment` → `StatefulSet`** with `serviceName: doris-fe-headless` — gives the pod a stable DNS name (`doris-fe-0.doris-fe-headless.prod.svc.cluster.local`) that survives restarts.

2. **Headless Service added** — `doris-fe-headless` ClusterIP=None, port 9010, so DNS resolves directly to the pod IP.

3. **`fe.conf` updated** — three lines added to the ConfigMap:
   ```properties
   enable_fqdn_mode = true
   advertised_address = doris-fe-0.doris-fe-headless.prod.svc.cluster.local
   priority_networks = 10.244.0.0/16
   ```
   - `enable_fqdn_mode`: BdbJE stores the DNS name instead of the IP
   - `advertised_address`: the stable FQDN this FE advertises to BEs and peers
   - `priority_networks`: tells Doris which NIC to bind to (Calico overlay CIDR)

4. **Entrypoint changed** to `fe_entrypoint.sh` — the official Strimzi-style entrypoint that properly reads env vars and handles FQDN registration.

### Verify
```bash
# Check FE pod is Running
kubectl get pod -n prod -l app=doris-fe

# Connect to Doris and confirm FE is MASTER
mysql -h 192.168.1.50 -P 30090 -u root -e "SHOW FRONTENDS\G"
# Expect: feType=MASTER, Alive=true, correct FQDN in Host column

# Check BE registration
mysql -h 192.168.1.50 -P 30090 -u root -e "SHOW BACKENDS\G"
# Expect: Alive=true, heartbeatAddress using FQDN
```

---

## 7. Apache Doris BE — CrashLoopBackOff (ulimit + Rolling Update)

### Symptom
Doris BE pod in `CrashLoopBackOff` with two distinct errors:

**Error 1 — ulimit:**
```
Check failed: FLAGS_min_file_descriptor_number <= limit.rlim_cur
Require minimum fd number: 65536, Current limit: 1024
```

**Error 2 — PVC conflict during rolling update:**
```
Warning  FailedAttachVolume  Multi-Attach error for volume "pvc-xxx": 
         volume is already exclusively used by another pod
```

### Root Cause

**ulimit:** Doris BE requires `ulimit -n` (open file descriptors) ≥ 65536. The node's soft limit was 1024. The hard limit on `worker1.local` is 524288 — attempting to set a higher value (e.g. 655350) caused the container init to fail because you cannot exceed the hard limit.

**Rolling update / PVC conflict:** The BE `Deployment` used `strategy: RollingUpdate` (the default). During an update, Kubernetes starts a new BE pod before terminating the old one. Both pods attempt to mount the same `ReadWriteOnce` PVC simultaneously — the new pod cannot attach and gets stuck indefinitely.

### Permanent Fix

**File changed:** [`manifests/doris/doris-be-deployment.yaml`](../../manifests/doris/doris-be-deployment.yaml)

```yaml
# 1. Set ulimit to exactly the node hard limit (524288)
initContainers:
  - name: init-sysctl
    image: busybox
    command: ["sh", "-c", "ulimit -n 524288"]
    securityContext:
      privileged: true

# 2. Switch to Recreate strategy (terminate old pod before starting new)
strategy:
  type: Recreate
```

Also changed to `be_entrypoint.sh` (same as FE — proper env var handling and startup sequencing).

### Node-Level Permanent Fix (required on worker1.local)

The ulimit fix in the pod spec sets the limit for the container. For a **permanent node-level fix** that survives reboots, apply the following on `worker1.local`:

```bash
# 1. Persistent sysctl settings
cat > /etc/sysctl.d/99-doris.conf << 'EOF'
vm.max_map_count = 2000000
vm.swappiness = 1
EOF
sysctl --system

# 2. Transparent Huge Pages — set to madvise (Doris recommendation)
echo madvise > /sys/kernel/mm/transparent_hugepage/enabled
cat > /etc/rc.d/rc.local << 'EOF'
#!/bin/bash
echo madvise > /sys/kernel/mm/transparent_hugepage/enabled
EOF
chmod +x /etc/rc.d/rc.local
systemctl enable rc-local

# 3. containerd process nofile limit (so containers inherit a high limit)
mkdir -p /etc/systemd/system/containerd.service.d
cat > /etc/systemd/system/containerd.service.d/limits.conf << 'EOF'
[Service]
LimitNOFILE=1048576
EOF
systemctl daemon-reload && systemctl restart containerd
```

### Verify
```bash
# On worker1.local — confirm sysctl persists
sysctl vm.max_map_count   # should be 2000000

# In BE pod — confirm ulimit
kubectl exec -n prod -l app=doris-be -- sh -c "ulimit -n"   # should be 524288

# Confirm BE is alive in Doris
mysql -h 192.168.1.50 -P 30090 -u root -e "SHOW BACKENDS\G"
```

---

## 8. Apache Polaris — ImagePullBackOff + Wrong Config Path

### Symptom

**Error 1 — ImagePullBackOff:**
```
Failed to pull image "192.168.1.50:30500/apache-polaris:latest": 
  manifest for 192.168.1.50:30500/apache-polaris:latest not found
```

**Error 2 (silent) — Wrong config path:**  
Polaris started but all database configuration was ignored. It ran on default in-memory settings — no PostgreSQL connection, catalogs not persisted.

### Root Cause

**Image missing from registry:** The private registry at `192.168.1.50:30500` stores images on a `local-path` PVC on `master.local`. The image `apache-polaris:latest` had been built once and was present in containerd's node-level image cache, but had **never been pushed to the registry**. After a reboot, the pod was scheduled on a different node that had no cached copy → `ImagePullBackOff`.

**Wrong config path:** `docker/polaris/entrypoint.sh` had the path hardcoded as `/opt/polaris/config/application.properties` but the actual ConfigMap was mounted at `/opt/polaris/application.properties` (no `config/` subdirectory). Polaris silently ignored the missing file and ran on defaults.

### Permanent Fix

Four changes were made:

1. **Image pinned to versioned tag** — `apache-polaris:1.6.0` replaces `:latest`. `imagePullPolicy: IfNotPresent` ensures no unnecessary pulls.  
   **Files:** [`manifests/polaris/polaris-deployment.yaml`](../../manifests/polaris/polaris-deployment.yaml), [`docker/polaris/Dockerfile`](../../docker/polaris/Dockerfile), [`scripts/master/build-and-push-custom-images.sh`](../../scripts/master/build-and-push-custom-images.sh)

2. **Entrypoint path fixed** — removed `config/` subdirectory from the properties path.  
   **File:** [`docker/polaris/entrypoint.sh`](../../docker/polaris/entrypoint.sh)

3. **Registry seed CronJob created** — runs daily to re-push the Polaris image from containerd cache to the private registry if it has been lost.  
   **File:** [`manifests/polaris/polaris-registry-seed.yaml`](../../manifests/polaris/polaris-registry-seed.yaml)

4. **Image re-pushed to registry** at time of fix:
   ```bash
   # Pull from containerd cache → retag → push to registry
   podman pull --tls-verify=false 192.168.1.50:30500/apache-polaris:1.6.0
   # (or rebuild and push via build-and-push-custom-images.sh)
   bash scripts/master/build-and-push-custom-images.sh
   ```

### Verify
```bash
# Confirm image exists in registry
curl -sk https://192.168.1.50:30500/v2/apache-polaris/tags/list
# Expected: {"name":"apache-polaris","tags":["1.6.0"]}

# Confirm pod is Running
kubectl get pod -n prod -l app=polaris

# Confirm config was loaded (should reference PostgreSQL, not in-memory)
kubectl logs -n prod deploy/polaris | grep -i "jdbc\|postgres\|catalog"

# Test REST API
curl http://192.168.1.50:30181/api/management/v1/principal-roles
```

---

## 9. ArgoCD strimzi-kafka App — Perpetual OutOfSync

### Symptom
The ArgoCD `strimzi-kafka` application remained perpetually `OutOfSync` showing:

```
PersistentVolumeClaim  data-strimzi-kafka-combined-0  requiresPruning=True
ClusterRoleBinding     strimzi-prod-strimzi-kafka-kafka-init  requiresPruning=True
```

Even after setting `prune: false`, the UI still showed these two resources as yellow/orange — they exist in the cluster but not in git.

### Root Cause
Both `data-strimzi-kafka-combined-0` (PVC) and `strimzi-prod-strimzi-kafka-kafka-init` (ClusterRoleBinding) are created at **runtime by the Strimzi operator**, not by ArgoCD. They are not declared in any git manifest. With `prune: false` they will no longer be deleted, but ArgoCD still highlights them because they are "extra" resources unknown to git.

This is **expected and harmless** with `prune: false`. They will not cause any functional issue.

### Permanent Fix
`prune: false` is the correct and complete fix. The `requiresPruning` indicator in the UI is cosmetic — ArgoCD will not act on it.

To silence the yellow indicator entirely, add `ignoreDifferences` to the ArgoCD app spec:

```yaml
# In argocd-apps/app-prod.yaml, under the strimzi-kafka app spec:
ignoreDifferences:
  - group: ""
    kind: PersistentVolumeClaim
    name: data-strimzi-kafka-combined-0
    namespace: prod
  - group: "rbac.authorization.k8s.io"
    kind: ClusterRoleBinding
    name: strimzi-prod-strimzi-kafka-kafka-init
```

### Verify
```bash
# Confirm prune is false
argocd app get strimzi-kafka | grep -i prune

# Confirm app is not deleting the PVC on sync
argocd app sync strimzi-kafka
kubectl get pvc data-strimzi-kafka-combined-0 -n prod   # must still exist after sync
```

---

## 10. Node-Level — Kernel Parameters Not Persistent

### Symptom
`vm.max_map_count` set to `2000000` via `sysctl -w` on `worker1.local` — but this is lost on every reboot. Doris BE (and OpenSearch) require this value to be ≥ 262144; the default is 65530.

### Permanent Fix

Apply the following on `worker1.local` (and any other worker that runs Doris BE or OpenSearch):

```bash
# 1. Persistent sysctl (survives reboot)
cat > /etc/sysctl.d/99-platform.conf << 'EOF'
vm.max_map_count = 2000000
vm.swappiness = 1
net.core.somaxconn = 65535
EOF
sysctl --system

# 2. Verify immediately
sysctl vm.max_map_count
# Expected: vm.max_map_count = 2000000

# 3. Containerd file descriptor limit
mkdir -p /etc/systemd/system/containerd.service.d
cat > /etc/systemd/system/containerd.service.d/limits.conf << 'EOF'
[Service]
LimitNOFILE=1048576
EOF
systemctl daemon-reload && systemctl restart containerd

# 4. THP to madvise (Doris requirement — reduces THP latency spikes)
echo madvise > /sys/kernel/mm/transparent_hugepage/enabled
# Make persistent via rc.local
cat >> /etc/rc.d/rc.local << 'EOF'
echo madvise > /sys/kernel/mm/transparent_hugepage/enabled
EOF
chmod +x /etc/rc.d/rc.local
systemctl enable rc-local --now
```

---

## 11. Post-Reboot Recovery Checklist

After any cluster reboot, follow this checklist in order:

```bash
# ── Step 1: Check all nodes are Ready ──────────────────────────────────────
kubectl get nodes

# ── Step 2: Check OpenBao seal status ──────────────────────────────────────
curl -s http://192.168.1.50:30820/v1/sys/seal-status | python3 -m json.tool
# If "sealed": true → the auto-unseal CronJob should trigger within 1 minute
# If still sealed after 2 minutes, unseal manually:
kubectl get secret openbao-unseal-keys -n prod \
  -o jsonpath='{.data.unseal-key}' | base64 -d | xargs -I{} \
  curl -s -X PUT http://192.168.1.50:30820/v1/sys/unseal \
  -d "{\"key\": \"{}\"}"

# ── Step 3: Check ArgoCD sync ───────────────────────────────────────────────
argocd app list
# All apps should be Synced/Healthy within ~5 minutes

# ── Step 4: Check all prod pods ────────────────────────────────────────────
kubectl get pods -n prod | grep -v Running | grep -v Completed

# ── Step 5: Strimzi Kafka specifically ─────────────────────────────────────
kubectl wait kafka/strimzi-kafka -n prod --for=condition=Ready --timeout=300s
# If it times out and PVC was deleted → see Section 3 Recovery Steps

# ── Step 6: Verify SCRAM credentials are correct ───────────────────────────
# Schema Registry
kubectl logs -n prod deploy/schema-registry --tail=5 | grep -i "started\|error"
# Debezium
kubectl logs -n prod deploy/debezium-connect --tail=5 | grep -i "started\|error"

# ── Step 7: Verify Doris FE membership ─────────────────────────────────────
mysql -h 192.168.1.50 -P 30090 -u root \
  -e "SHOW FRONTENDS\G" 2>/dev/null | grep -E "feType|Alive|Host"
# Expected: feType=MASTER, Alive=true, Host=doris-fe-0.doris-fe-headless.prod.svc.cluster.local

# ── Step 8: Verify Polaris image is in registry ─────────────────────────────
curl -sk https://192.168.1.50:30500/v2/apache-polaris/tags/list
# If image is missing, run: bash scripts/master/build-and-push-custom-images.sh
```

---

## 12. Architecture Lessons Learned

### 12.1 Never Use `prune: true` for Operator-Managed Applications
Any Kubernetes operator (Strimzi, cert-manager, etc.) creates **runtime resources** — PVCs, Secrets, ClusterRoleBindings — that are not in git. ArgoCD `prune: true` will delete these on every sync. Always use `prune: false` for applications managed by a controller/operator.

### 12.2 Never Copy Credentials — Always Reference the Source of Truth
Strimzi is the single owner of SCRAM-SHA-512 passwords for Kafka users. Any copy of these passwords (in another secret, in OpenBao, in a ConfigMap) **will drift** after Strimzi reconciles. The correct pattern is:

```yaml
# ✓ CORRECT — reference the Strimzi-managed secret directly
- name: MY_PASSWORD
  valueFrom:
    secretKeyRef:
      name: my-kafka-user     # Strimzi-managed
      key: password

# ✗ WRONG — a copy that will drift
- name: MY_PASSWORD
  valueFrom:
    secretKeyRef:
      name: my-app-credentials   # seeded by a script — will diverge
      key: kafka-password
```

### 12.3 Stateful Services Need Stable Network Identity
Any service that stores its own address in persistent metadata (BdbJE, etcd, Kafka KRaft, Elasticsearch/OpenSearch) must use a `StatefulSet` with a `headless Service`. A `Deployment` with an ephemeral pod IP **will break** on restart because the stored address becomes unreachable.

### 12.4 Private Registry Images Must Be Pushed, Not Just Built
Nodes cache images in containerd's local store. After a reboot or rescheduling to a different node, only images present in the private registry are pullable. Any image that exists only in the containerd cache will cause `ImagePullBackOff`. Always push custom images to `192.168.1.50:30500` during build, and pin `imagePullPolicy: IfNotPresent` so pulls are not attempted unnecessarily.

### 12.5 Health-Check Endpoints Must Be Chosen Carefully
Use endpoints that return HTTP 200 in all states you want to handle. `/v1/sys/health` in OpenBao returns HTTP 503 when sealed — not suitable for a conditional health check. `/v1/sys/seal-status` always returns HTTP 200 and includes the seal state in the JSON body.

### 12.6 Rolling Updates Are Incompatible with ReadWriteOnce PVCs
`strategy: RollingUpdate` starts a new pod before the old one terminates. If both pods mount a `ReadWriteOnce` PVC, the new pod will fail to attach. Always use `strategy: Recreate` for single-replica stateful workloads with RWO PVCs.

---

*Last updated: 2026-07-24 — covers all incidents from the post-reboot degraded platform recovery session.*
