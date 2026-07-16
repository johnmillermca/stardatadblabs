# Apache Doris

## Overview
Apache Doris 2.1.0 — MPP analytical database for real-time analytics. Deployed with one FE (Frontend) and one BE (Backend) node in the `analytics` namespace.

| Property | Value |
|---|---|
| Namespace | `analytics` |
| FE HTTP / WebUI | `http://192.168.1.50:30030` |
| FE MySQL protocol | `jdbc:mysql://192.168.1.50:30090` |
| FE image | `apache/doris:2.1.0-fe` |
| BE image | `apache/doris:2.1.0-be` |
| Secret | `doris-credentials` |
| Manifests | `manifests/doris/` |

## Prerequisites
```bash
sudo bash scripts/master/12-seed-openbao-secrets.sh
```

## Deployment (ArgoCD)
ArgoCD application: `argocd-apps/app-doris.yaml`  
Syncs all files in `manifests/doris/` to the `analytics` namespace.

## Manual Deploy
```bash
kubectl apply -f manifests/doris/doris-services.yaml
kubectl apply -f manifests/doris/doris-fe-deployment.yaml
kubectl rollout status deployment/doris-fe -n analytics
kubectl apply -f manifests/doris/doris-be-deployment.yaml
kubectl rollout status deployment/doris-be -n analytics
```

## Connect
```bash
# MySQL protocol
DORIS_PASS=$(kubectl get secret doris-credentials -n analytics \
  -o jsonpath='{.data.admin-password}' | base64 -d)
mysql -h 192.168.1.50 -P 30090 -u root -p
```

## Initial Setup
```sql
-- After first login (no password required for root initially)
SET PASSWORD FOR 'root'@'%' = PASSWORD('<admin-password>');
-- Create database
CREATE DATABASE analytics;
```

## FQDN Mode
`enable_fqdn_mode=true` is set in `fe.conf`. The BE registers with FE using the headless DNS FQDN `doris-fe-headless.analytics.svc.cluster.local:9010`. This requires stable pod DNS — do not use direct IP mode.

## Secrets
| Key | Description |
|---|---|
| `admin-password` | Doris root password |

OpenBao path: `secret/data/doris/credentials`
