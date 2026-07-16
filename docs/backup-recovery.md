# Platform Backup and Recovery

## Overview
Full platform backup covers: etcd snapshot, Helm values, ArgoCD apps, raw manifests, OpenBao init keys, Kubernetes secrets, PVC data, and git state.

## Backup
```bash
sudo bash scripts/master/backup-platform.sh
```
Output: `/opt/k8s-backups/platform-backup-<timestamp>.tar.gz`  
Retention: last 7 archives (older archives auto-deleted).

## Restore
```bash
sudo bash scripts/master/restore-platform.sh /opt/k8s-backups/platform-backup-<timestamp>.tar.gz
```
- Prompts for confirmation before etcd restore (destructive operation)
- Restores Helm values, ArgoCD apps, manifests, OpenBao keys, K8s secrets

## Backup Contents
| Component | What is backed up |
|---|---|
| etcd | Full snapshot via `etcdctl snapshot save` |
| Helm values | `helm/` directory |
| ArgoCD apps | `argocd-apps/` directory |
| Manifests | `manifests/` directory |
| OpenBao keys | `/root/openbao-init-keys.json` |
| K8s Secrets | All secrets from all namespaces (base64 encoded) |
| PVC data | Selected PVCs via `kubectl cp` |
| Git state | `git log --oneline -20` for reference |

## Manual etcd Snapshot
```bash
ETCDCTL_API=3 etcdctl snapshot save /tmp/etcd-snap.db \
  --endpoints=https://127.0.0.1:2379 \
  --cacert=/etc/kubernetes/pki/etcd/ca.crt \
  --cert=/etc/kubernetes/pki/etcd/server.crt \
  --key=/etc/kubernetes/pki/etcd/server.key
```

## Manual etcd Restore
```bash
# CAUTION: stops the cluster
systemctl stop kubelet
ETCDCTL_API=3 etcdctl snapshot restore /tmp/etcd-snap.db \
  --data-dir=/var/lib/etcd-restore
# Update etcd manifest to use new data dir, then restart
systemctl start kubelet
```

## GitHub Push
```bash
bash scripts/git-push.sh "chore: backup after changes"
```
The script blocks pushes of secrets/keys files using a pre-push safety check.
