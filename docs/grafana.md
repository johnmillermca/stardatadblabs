# Grafana — k8s-platform

## Overview

| Field | Value |
|---|---|
| **Chart** | `grafana/grafana` `8.4.2` |
| **Namespace** | `monitoring` |
| **Grafana UI** | `http://192.168.1.50:30300` |
| **Default datasource** | Prometheus (auto-provisioned) |
| **Storage** | PVC 10 Gi on `local-path` |
| **ArgoCD App** | `grafana` (auto-synced from `helm/grafana/values.yaml`) |
| **Secrets** | `grafana-credentials` in namespace `monitoring` (seeded by OpenBao) |
| **Admin user** | `admin` (password stored in OpenBao at `secret/data/grafana/credentials`) |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  monitoring namespace                                                   │
│                                                                         │
│  ┌─────────────────────┐  queries  ┌─────────────────────┐             │
│  │   Grafana           │──────────►│   Prometheus        │             │
│  │   (port 3000)       │           │   (port 9090)        │             │
│  │   NodePort 30300    │           └─────────────────────┘             │
│  │                     │                                                │
│  │  Pre-loaded         │                                                │
│  │  dashboards:        │                                                │
│  │  - K8s cluster      │                                                │
│  │  - Node Exporter    │                                                │
│  │  - Kafka            │                                                │
│  │  - PostgreSQL       │                                                │
│  │  - OpenSearch       │                                                │
│  │  - ArgoCD           │                                                │
│  └─────────────────────┘                                                │
│                                                                         │
│  ┌─────────────────────┐                                                │
│  │  Grafana MCP Server │  JSON-RPC tools → AI assistants               │
│  │  (port 3201)        │                                                │
│  │  NodePort 30321     │                                                │
│  └─────────────────────┘                                                │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Installation

### Step 1 — Seed secrets
```bash
sudo bash scripts/master/12-seed-openbao-secrets.sh
```
This creates `grafana-credentials` in the `monitoring` namespace.

### Step 2 — Add Helm repo
```bash
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update
```

### Step 3 — Deploy via ArgoCD (recommended)
```bash
kubectl apply -f argocd-apps/app-monitoring.yaml -n argocd
```
ArgoCD (sync-wave 2) deploys Grafana after Prometheus is healthy.

### Step 4 — Deploy manually (alternative)
```bash
helm upgrade --install grafana grafana/grafana \
  --namespace monitoring \
  --create-namespace \
  --version 8.4.2 \
  -f helm/grafana/values.yaml
```

---

## Verify

```bash
# Check pods
kubectl get pods -n monitoring -l app.kubernetes.io/name=grafana

# Get the admin password (via OpenBao CLI)
bao kv get -field=admin-password secret/grafana/credentials

# Or from K8s secret
kubectl get secret grafana-credentials -n monitoring \
  -o jsonpath='{.data.admin-password}' | base64 -d

# Access UI
open http://192.168.1.50:30300
```

---

## Pre-loaded Dashboards

Dashboards are imported from grafana.com at deploy time:

| Dashboard | Grafana ID | Description |
|---|---|---|
| Kubernetes Cluster Overview | 315 | Cluster CPU/memory/pod counts |
| Node Exporter Full | 1860 | Per-node CPU, memory, disk, network |
| Kafka Overview | 7589 | Strimzi broker metrics |
| PostgreSQL Overview | 9628 | Query rates, connections, lock waits |
| OpenSearch Overview | 14191 | Index counts, search rate, heap |
| Kube-state-metrics | 13332 | Deployment/pod/pvc status |
| ArgoCD | 14584 | Application sync health |

---

## Grafana MCP Server

The MCP server exposes Grafana APIs as tools for AI assistants:

| Tool | Description |
|---|---|
| `list_dashboards` | List all dashboards |
| `get_dashboard` | Get a specific dashboard by UID |
| `search_dashboards` | Search by name or tag |
| `list_datasources` | List all datasources |
| `query_datasource` | Run a query against a datasource |
| `list_alerts` | List all alert rules |
| `get_alert_state` | Get current alert firing state |
| `list_folders` | List dashboard folders |
| `get_org_stats` | Organisation statistics |
| `list_users` | List all Grafana users |

**Endpoint:** `http://192.168.1.50:30321/mcp` (JSON-RPC 2.0)

**Authentication:** Uses `grafana-credentials` K8s secret (admin-user/admin-password keys).

### Build and push image
```bash
bash docker/mcp-grafana/build-and-push.sh
```

---

## Adding a New Dashboard

1. Create or download a Grafana dashboard JSON.
2. Mount it via `dashboardsConfigMaps` in `helm/grafana/values.yaml`.
3. Commit the change — ArgoCD auto-syncs within 3 minutes.

---

## Environments

| Environment | NodePort | Persistence | Values file |
|---|---|---|---|
| Production | 30300 | 10 Gi PVC | `helm/grafana/values.yaml` |
| Staging | 30301 | 5 Gi PVC | `staging/helm/grafana/values.yaml` |
| Testing | 30302 | None | `testing/helm/grafana/values.yaml` |

---

## OpenBao Secrets

| Secret name | Namespace | OpenBao path |
|---|---|---|
| `grafana-credentials` | `monitoring` | `secret/data/grafana/credentials` |

Keys: `admin-user`, `admin-password`, `secret-key`

---

## Troubleshooting

| Symptom | Check |
|---|---|
| UI not reachable | `kubectl get svc -n monitoring grafana`; check NodePort |
| Login fails | Verify `grafana-credentials` secret; re-seed with `12-seed-openbao-secrets.sh` |
| Dashboards empty | Check datasource connectivity at `/datasources/proxy` |
| PVC pending | Verify `local-path` StorageClass; check node disk space |
