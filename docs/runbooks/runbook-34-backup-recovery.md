# Runbook 34 — Platform Backup & Recovery

## Overview

This runbook covers the full backup and point-in-time recovery of the
`k8s-platform` Kubernetes cluster. Backups run automatically every day at
**02:00 UTC** via a CronJob and are uploaded to S3. This runbook documents
how to verify backups, trigger a manual backup, and perform a complete or
partial recovery.

---

## Backup Architecture

### What Is Backed Up

| # | Component | Method | Notes |
|---|-----------|--------|-------|
| 1 | **etcd snapshot** | `etcdctl snapshot save` | Full cluster state — all K8s objects |
| 2 | **Kubernetes PKI** | `cp /etc/kubernetes/pki` | CA certs, apiserver certs, etcd certs |
| 3 | **K8s Secrets** | `kubectl get secret -A -o yaml` | All namespaces, raw YAML |
| 4 | **PostgreSQL** | `pg_dump` per database | Repository DBs only (see below) |
| 5 | **Kerberos KDC** | `kdb5_util dump` + raw files | Principal DB, keytabs, kdc.conf |
| 6 | **OpenBao init keys** | File copy + K8s secret fallback | Unseal key + root token |
| 7 | **OpenBao PVC** | `tar` of local-path volume | `data-openbao-0` |
| 8 | **Private registry PVC** | `tar` of local-path volume | `registry-data` |
| 9 | **Helm values** | `cp helm/` | All chart value overrides |
| 10 | **ArgoCD apps** | `cp argocd-apps/` + live export | Repo config + live Application objects |
| 11 | **Manifests + scripts** | `cp manifests/ scripts/ docker/` | Full IaC state |
| 12 | **Git state** | `git log / status` snapshot | For forensic reference |

### PostgreSQL Databases Included

| Database | Purpose |
|----------|---------|
| `polaris` | Apache Polaris Iceberg catalog — principals, catalogs, namespaces, tables |
| `kestra` | Kestra workflow repository — flows, executions, triggers |
| `sqlmesh_state` | SQLMesh state store — snapshots, runs, environment history |
| `rbac` | rbac-plane — users, roles, role-bindings, services, audit log |
| `metadata` | Platform metadata (default postgres DB) |

> **Excluded intentionally:** `cache_testing`, `pipeline` — these are
> regeneratable data / testing databases. Oracle XE data is also excluded.

### S3 Destination

```
s3://xdatatoiceberg1/k8s-backups/platform-backup-<YYYYMMDD-HHMMSS>.tar.gz
```

- **Region:** `us-east-2`
- **Storage class:** `STANDARD_IA` (infrequent access — cost-optimised)
- **S3 retention:** last 30 archives
- **Local retention:** last 3 archives in `/opt/k8s-backups/`

---

## Automated Backup (CronJob)

### Schedule

```
0 2 * * *   →   02:00 UTC daily
```

### Deploy the CronJob

```bash
kubectl apply -f manifests/backup/platform-backup-cronjob.yaml
```

### Verify it is deployed

```bash
kubectl get cronjob platform-backup -n kube-system
```

Expected output:
```
NAME              SCHEDULE    SUSPEND   ACTIVE   LAST SCHEDULE   AGE
platform-backup   0 2 * * *   False     0        <time>          ...
```

### Trigger a Manual Backup

```bash
kubectl create job -n kube-system \
  --from=cronjob/platform-backup \
  platform-backup-manual-$(date +%s)
```

### Watch backup logs

```bash
# Get the job pod name
kubectl get pods -n kube-system -l app=platform-backup

# Follow logs
kubectl logs -n kube-system <pod-name> -f
```

---

## Manual Backup (direct on master)

Run directly on `master.local` as root (useful when the cluster is unhealthy
and pods cannot be scheduled):

```bash
sudo bash /home/star_master/k8s-platform/scripts/master/backup-platform.sh
```

The archive is written to `/opt/k8s-backups/` and uploaded to S3.

### Override settings

```bash
# Change thresholds or destination
sudo S3_BUCKET=xdatatoiceberg1 \
     S3_PREFIX=k8s-backups \
     S3_REGION=us-east-2 \
     S3_KEEP=30 \
     LOCAL_KEEP=3 \
     bash scripts/master/backup-platform.sh
```

---

## Verifying Backups

### List recent S3 backups

```bash
AWS_ACCESS_KEY_ID=$(kubectl get secret platform-s3-credentials -n prod \
  -o jsonpath='{.data.access_key}' | base64 -d)
AWS_SECRET_ACCESS_KEY=$(kubectl get secret platform-s3-credentials -n prod \
  -o jsonpath='{.data.secret_key}' | base64 -d)

AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
aws s3 ls s3://xdatatoiceberg1/k8s-backups/ \
  --region us-east-2 \
  --human-readable \
  | sort
```

### Verify etcd snapshot integrity

```bash
# Download a backup
aws s3 cp s3://xdatatoiceberg1/k8s-backups/platform-backup-<TIMESTAMP>.tar.gz /tmp/

# Extract and verify the etcd snapshot
tar -xzf /tmp/platform-backup-<TIMESTAMP>.tar.gz -C /tmp/ --strip-components=1
ETCDCTL_API=3 etcdctl snapshot status /tmp/etcd-snapshot.db --write-out=table
```

Expected output:
```
+----------+----------+------------+------------+
|   HASH   | REVISION | TOTAL KEYS | TOTAL SIZE |
+----------+----------+------------+------------+
| <hash>   | <rev>    | <count>    | <size>     |
+----------+----------+------------+------------+
```

### Check backup log

```bash
cat /opt/k8s-backups/backup.log | tail -50
```

---

## Recovery Procedures

> ⚠️  **Before starting any recovery:**
> 1. Confirm the target backup archive is complete and the etcd snapshot verifies cleanly.
> 2. Notify the team — recovery will cause downtime.
> 3. Snapshot the current broken state to `/opt/k8s-backups/pre-recovery-$(date +%s)/` before making changes.

---

### Scenario A — Full Cluster Recovery (master lost or etcd corrupted)

Use this when `master.local` is reinstalled from scratch or etcd data is lost.

#### Step 1 — Reinstall OS and kubeadm prerequisites

```bash
# On the new master node, run the cluster init script
sudo bash scripts/master/01-kubeadm-init.sh
```

#### Step 2 — Download backup from S3

```bash
# On the new master, configure AWS credentials temporarily
export AWS_ACCESS_KEY_ID=<key>
export AWS_SECRET_ACCESS_KEY=<secret>

aws s3 cp s3://xdatatoiceberg1/k8s-backups/platform-backup-<TIMESTAMP>.tar.gz \
  /opt/k8s-backups/ --region us-east-2

# Clone the repo
git clone <your-github-repo-url> /home/star_master/k8s-platform
cd /home/star_master/k8s-platform
```

#### Step 3 — Restore PKI and etcd

```bash
# Extract the archive
mkdir -p /tmp/k8s-restore
tar -xzf /opt/k8s-backups/platform-backup-<TIMESTAMP>.tar.gz \
  -C /tmp/k8s-restore --strip-components=1

# Restore Kubernetes PKI
cp -r /tmp/k8s-restore/kubernetes-pki/pki /etc/kubernetes/
chmod -R 600 /etc/kubernetes/pki

# Stop kubelet before restoring etcd
systemctl stop kubelet

# Backup current (broken) etcd
mv /var/lib/etcd /var/lib/etcd.broken-$(date +%s) || true

# Restore etcd from snapshot
ETCDCTL_API=3 etcdctl snapshot restore /tmp/k8s-restore/etcd-snapshot.db \
  --data-dir=/var/lib/etcd \
  --cacert=/etc/kubernetes/pki/etcd/ca.crt \
  --cert=/etc/kubernetes/pki/etcd/server.crt \
  --key=/etc/kubernetes/pki/etcd/server.key

# Start kubelet
systemctl start kubelet

# Wait for cluster to stabilise
sleep 30
kubectl get nodes
```

#### Step 4 — Restore OpenBao

```bash
# Restore init keys
cp /tmp/k8s-restore/openbao-init-keys.json /root/openbao-init-keys.json
chmod 600 /root/openbao-init-keys.json

# Wait for OpenBao pod to start, then unseal
kubectl wait --for=condition=Ready pod/openbao-0 -n prod --timeout=120s
UNSEAL_KEY=$(jq -r '.unseal_key // ."unseal-key"' /root/openbao-init-keys.json)
kubectl exec -n prod openbao-0 -- bao operator unseal "${UNSEAL_KEY}"

# Verify
kubectl exec -n prod openbao-0 -- bao status
```

#### Step 5 — Re-seed K8s secrets

```bash
# Option A: re-seed from OpenBao (preferred — keeps secrets fresh)
sudo bash scripts/master/12-seed-openbao-secrets.sh

# Option B: restore raw secrets dump (belt-and-suspenders)
kubectl apply -f /tmp/k8s-restore/all-secrets.yaml --force
```

#### Step 6 — Verify the cluster

```bash
kubectl get nodes
kubectl get pods -A | grep -v Running | grep -v Completed
kubectl get applications -n argocd
```

---

### Scenario B — PostgreSQL Repository Database Recovery

Use this when a specific repository database is corrupted or accidentally
dropped. **Existing pod stays running** — no cluster downtime.

```bash
# Download and extract backup
aws s3 cp s3://xdatatoiceberg1/k8s-backups/platform-backup-<TIMESTAMP>.tar.gz /tmp/
tar -xzf /tmp/platform-backup-<TIMESTAMP>.tar.gz -C /tmp/restore --strip-components=1

PG_POD=$(kubectl get pod -n prod -l app=postgresql --no-headers \
  -o custom-columns="NAME:.metadata.name" | head -1)
PG_PASS=$(kubectl get secret postgresql-credentials -n prod \
  -o jsonpath='{.data.postgres-password}' | base64 -d)

# Restore a specific database (example: polaris)
DB=polaris

# Drop and recreate (warns connected clients)
kubectl exec -n prod "${PG_POD}" -- \
  env PGPASSWORD="${PG_PASS}" \
  psql -U postgres -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='${DB}';"

kubectl exec -n prod "${PG_POD}" -- \
  env PGPASSWORD="${PG_PASS}" \
  psql -U postgres -c "DROP DATABASE IF EXISTS ${DB}; CREATE DATABASE ${DB};"

# Restore from dump
gunzip -c /tmp/restore/postgres/${DB}.sql.gz \
  | kubectl exec -i -n prod "${PG_POD}" -- \
      env PGPASSWORD="${PG_PASS}" \
      psql -U postgres -d "${DB}"

echo "Restore of ${DB} complete"
```

---

### Scenario C — Kerberos KDC Recovery

Use when the Kerberos principal database is corrupted or the KDC pod's PVC
is lost.

```bash
# Extract backup
tar -xzf /tmp/platform-backup-<TIMESTAMP>.tar.gz -C /tmp/restore --strip-components=1

KRB_POD=$(kubectl get pod -n prod -l app=kerberos-kdc --no-headers \
  -o custom-columns="NAME:.metadata.name" | head -1)

# Copy raw KDC files back to the pod
for f in principal principal.kadm5 .k5.STARDATADBLABS.LOCAL kadm5.keytab kadm5.acl kdc.conf; do
  kubectl cp "/tmp/restore/kerberos/${f}" "prod/${KRB_POD}:/var/kerberos/krb5kdc/${f}"
done

# Or restore from the portable dump (safer for cross-version)
kubectl cp /tmp/restore/kerberos/principal-dump.txt prod/${KRB_POD}:/tmp/
kubectl exec -n prod "${KRB_POD}" -- \
  kdb5_util load -update /tmp/principal-dump.txt

# Restart the KDC
kubectl rollout restart deployment/kerberos-kdc -n prod
kubectl rollout status deployment/kerberos-kdc -n prod

# Verify
kubectl exec -n prod "${KRB_POD}" -- kadmin.local -q "listprincs" | head -20
```

---

### Scenario D — OpenBao PVC Recovery

Use when the OpenBao PVC (`data-openbao-0`) is lost but the pod is running.

```bash
tar -xzf /tmp/platform-backup-<TIMESTAMP>.tar.gz -C /tmp/restore --strip-components=1

# Scale down OpenBao
kubectl scale statefulset openbao -n prod --replicas=0
kubectl wait --for=delete pod/openbao-0 -n prod --timeout=60s

# Find PVC local-path directory on master
PV_NAME=$(kubectl get pvc data-openbao-0 -n prod -o jsonpath='{.spec.volumeName}')
PV_PATH=$(find /home/local-path-provisioner -maxdepth 2 -name "${PV_NAME}*" -type d | head -1)

# Clear existing data and restore
rm -rf "${PV_PATH:?}/"*
tar -xzf /tmp/restore/pvc-data/prod-data-openbao-0.tar.gz \
  -C "$(dirname "${PV_PATH}")" --strip-components=1

# Scale back up and unseal
kubectl scale statefulset openbao -n prod --replicas=1
kubectl wait --for=condition=Ready pod/openbao-0 -n prod --timeout=120s

UNSEAL_KEY=$(jq -r '.unseal_key // ."unseal-key"' /root/openbao-init-keys.json)
kubectl exec -n prod openbao-0 -- bao operator unseal "${UNSEAL_KEY}"
kubectl exec -n prod openbao-0 -- bao status
```

---

### Scenario E — Private Registry Recovery

Use when the registry PVC (`registry-data`) is lost.

```bash
tar -xzf /tmp/platform-backup-<TIMESTAMP>.tar.gz -C /tmp/restore --strip-components=1

# Find the registry pod
REG_POD=$(kubectl get pod -n registry --no-headers \
  -o custom-columns="NAME:.metadata.name" | head -1)

# Scale down registry
kubectl scale deployment registry -n registry --replicas=0

# Find PV directory
PV_NAME=$(kubectl get pvc registry-data -n registry -o jsonpath='{.spec.volumeName}')
PV_PATH=$(find /home/local-path-provisioner -maxdepth 2 -name "${PV_NAME}*" -type d | head -1)

# Restore
rm -rf "${PV_PATH:?}/"*
tar -xzf /tmp/restore/pvc-data/registry-registry-data.tar.gz \
  -C "$(dirname "${PV_PATH}")" --strip-components=1

# Scale back up
kubectl scale deployment registry -n registry --replicas=1
kubectl rollout status deployment/registry -n registry
```

---

## Post-Recovery Verification Checklist

Run these after any recovery scenario:

```bash
# 1. All nodes Ready
kubectl get nodes

# 2. No pods stuck in non-Running state (excluding Completed jobs)
kubectl get pods -A | grep -Ev "Running|Completed|Terminating" | grep -v NAME

# 3. ArgoCD apps synced
kubectl get applications -n argocd

# 4. OpenBao unsealed
kubectl exec -n prod openbao-0 -- bao status | grep -E "Sealed|HA"

# 5. PostgreSQL repository databases accessible
kubectl exec -n prod postgresql-0 -- \
  env PGPASSWORD=$(kubectl get secret postgresql-credentials -n prod \
    -o jsonpath='{.data.postgres-password}' | base64 -d) \
  psql -U postgres -lqt | grep -E "polaris|kestra|sqlmesh_state|rbac|metadata"

# 6. Kerberos KDC responding
kubectl exec -n prod \
  $(kubectl get pod -n prod -l app=kerberos-kdc --no-headers \
    -o custom-columns="NAME:.metadata.name" | head -1) \
  -- kadmin.local -q "getprinc admin/admin@STARDATADBLABS.LOCAL"

# 7. Polaris catalog reachable
curl -s http://192.168.1.50:30181/healthcheck | python3 -m json.tool

# 8. Kafka broker up
kubectl get kafkas -n prod
```

---

## Backup File Layout

Inside each `platform-backup-<TIMESTAMP>.tar.gz`:

```
<TIMESTAMP>/
├── etcd-snapshot.db               # etcd point-in-time snapshot
├── kubernetes-pki/
│   └── pki/                       # /etc/kubernetes/pki (all certs + keys)
├── all-secrets.yaml               # all K8s secrets, all namespaces (chmod 600)
├── openbao-init-keys.json         # OpenBao unseal key + root token (chmod 600)
├── postgres/
│   ├── polaris.sql.gz             # Apache Polaris catalog DB
│   ├── kestra.sql.gz              # Kestra workflow repository
│   ├── sqlmesh_state.sql.gz       # SQLMesh state store
│   ├── rbac.sql.gz                # rbac-plane users/roles/bindings
│   └── metadata.sql.gz            # platform metadata DB
├── kerberos/                      # chmod 600
│   ├── principal-dump.txt         # portable kdb5_util dump
│   ├── principal                  # raw KDC database file
│   ├── principal.kadm5            # kadmin database
│   ├── .k5.STARDATADBLABS.LOCAL   # master key stash
│   ├── kadm5.keytab               # KDC admin keytab
│   ├── kadm5.acl                  # ACL file
│   ├── kdc.conf                   # KDC configuration
│   └── krb5.conf                  # client configuration
├── pvc-data/
│   ├── prod-data-openbao-0.tar.gz # OpenBao Raft storage
│   └── registry-registry-data.tar.gz  # Private container registry
├── helm/                          # Helm value files
├── argocd-apps/                   # ArgoCD Application definitions
├── argocd-live/
│   └── applications.yaml          # Live Application objects from cluster
├── manifests/                     # All Kubernetes manifests
├── scripts/                       # All platform scripts
├── docker/                        # Dockerfiles + build scripts
├── rbac-plane/                    # rbac-plane config + seed files
└── git-state.txt                  # git log / status at backup time
```

---

## Troubleshooting

### Backup job stuck / not completing

```bash
# Check job status
kubectl get jobs -n kube-system | grep platform-backup

# Check pod events
kubectl describe pod -n kube-system -l app=platform-backup

# Delete a stuck job
kubectl delete job -n kube-system <job-name>
```

### S3 upload fails

Check that the `platform-s3-credentials` secret exists and the IAM user
`watsonx-s3-connector` has `s3:PutObject` and `s3:DeleteObject` permissions
on `arn:aws:s3:::xdatatoiceberg1/k8s-backups/*`.

```bash
# Test credentials manually on master
AWS_ACCESS_KEY_ID=$(kubectl get secret platform-s3-credentials -n prod \
  -o jsonpath='{.data.access_key}' | base64 -d)
AWS_SECRET_ACCESS_KEY=$(kubectl get secret platform-s3-credentials -n prod \
  -o jsonpath='{.data.secret_key}' | base64 -d)

AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
aws s3 cp /etc/hostname s3://xdatatoiceberg1/k8s-backups/.test-write \
  --region us-east-2 && echo "write OK"
```

### pg_dump fails for a specific database

```bash
# Connect and check manually
kubectl exec -it -n prod postgresql-0 -- \
  env PGPASSWORD=<pass> psql -U postgres -d polaris -c "\l"
```

### etcd snapshot fails

If `etcdctl` is not in PATH on master:

```bash
which etcdctl || find / -name etcdctl 2>/dev/null | head -3
# Typically at /usr/local/bin/etcdctl or in the etcd pod
```

---

## Related Runbooks

- [Runbook 01 — OpenBao Setup](runbook-01-openbao.md)
- [Runbook 02 — ArgoCD & Kubernetes](runbook-02-argocd-kubernetes.md)
- [Runbook 11 — Kerberos Integration](runbook-11-kerberos-integration.md)
