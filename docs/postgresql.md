# PostgreSQL

## Overview
PostgreSQL 17 — primary relational metadata store for the platform. Hosts databases for Apache Ranger, Apache Polaris, and general platform metadata.

| Property | Value |
|---|---|
| Chart | `bitnami/postgresql 18.8.0` |
| Namespace | `databases` |
| Node | `master.local` (pinned) |
| Internal | `postgresql.databases.svc.cluster.local:5432` |
| External | `192.168.1.50:30532` |
| Databases | `metadata` (default), `ranger`, `polaris` |
| Secret | `postgresql-credentials` |

## Prerequisites
```bash
sudo bash scripts/master/12-seed-openbao-secrets.sh
```

## Deployment (ArgoCD)
ArgoCD application: `argocd-apps/app-postgresql.yaml`  
Chart: `bitnami/postgresql 18.8.0`

## Manual Helm Deploy
```bash
export PATH="/usr/local/bin:${PATH}"
helm repo add bitnami https://charts.bitnami.com/bitnami
helm upgrade --install postgresql bitnami/postgresql \
  --version 18.8.0 \
  --namespace databases \
  --create-namespace \
  -f helm/postgresql/values.yaml
```

## Connect
```bash
# Get password
PG_PASS=$(kubectl get secret postgresql-credentials -n databases -o jsonpath='{.data.postgres-password}' | base64 -d)

# Port-forward
kubectl port-forward svc/postgresql -n databases 5432:5432 &

# Connect
psql -h localhost -U postgres -W
```

## Verify Databases
```sql
\l
-- Should show: metadata, ranger, polaris
```

## Backup
```bash
kubectl exec -n databases deploy/postgresql-primary -- \
  pg_dumpall -U postgres > /opt/k8s-backups/pg-dump-$(date +%Y%m%d).sql
```

## Secrets
| Key | Description |
|---|---|
| `postgres-password` | Superuser password |
| `replication-password` | Replication user password |

OpenBao path: `secret/data/postgresql/credentials`
