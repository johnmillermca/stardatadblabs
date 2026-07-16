# MongoDB

## Overview
MongoDB 8.0 — document database for unstructured/semi-structured data, event storage, and application state.

| Property | Value |
|---|---|
| Chart | `bitnami/mongodb 19.1.17` |
| Namespace | `databases` |
| Node | `master.local` (pinned) |
| Internal | `mongodb.databases.svc.cluster.local:27017` |
| External | `192.168.1.50:30017` |
| Secret | `mongodb-credentials` |

## Prerequisites
```bash
sudo bash scripts/master/12-seed-openbao-secrets.sh
```

## Deployment (ArgoCD)
ArgoCD application: `argocd-apps/app-mongodb.yaml`

## Manual Helm Deploy
```bash
export PATH="/usr/local/bin:${PATH}"
helm upgrade --install mongodb bitnami/mongodb \
  --version 19.1.17 \
  --namespace databases \
  -f helm/mongodb/values.yaml
```

## Connect
```bash
MONGO_PASS=$(kubectl get secret mongodb-credentials -n databases \
  -o jsonpath='{.data.mongodb-root-password}' | base64 -d)
kubectl port-forward svc/mongodb -n databases 27017:27017 &
mongosh "mongodb://root:${MONGO_PASS}@localhost:27017"
```

## Secrets
| Key | Description |
|---|---|
| `mongodb-root-password` | Root user password |

OpenBao path: `secret/data/mongodb/credentials`
