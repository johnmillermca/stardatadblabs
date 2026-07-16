# Monitoring Stack — k8s-platform

## Overview

The `monitoring` namespace runs the full observability stack for the platform. It includes Prometheus for metrics collection, Grafana for dashboards and alerting, and two MCP (Model Context Protocol) servers that expose monitoring APIs to AI assistants.

| Component | Type | Namespace | NodePort | Internal URL |
|---|---|---|---|---|
| Prometheus | kube-prometheus-stack | `monitoring` | 30990 | `prometheus-prometheus.monitoring:9090` |
| Alertmanager | (bundled) | `monitoring` | 30993 | `prometheus-alertmanager.monitoring:9093` |
| Grafana | grafana/grafana | `monitoring` | 30300 | `grafana.monitoring:3000` |
| Prometheus MCP | custom | `monitoring` | 30320 | `mcp-prometheus.monitoring:3200` |
| Grafana MCP | custom | `monitoring` | 30321 | `mcp-grafana.monitoring:3201` |

---

## Namespace Quotas

The `monitoring` namespace is provisioned with:
- CPU: 8 req / 16 limit  
- Memory: 16 Gi req / 32 Gi limit  
- Storage: 200 Gi  
- Pods: 40 max  

---

## What is Monitored

All components across the platform are scraped:

| Component | Scrape endpoint | Job name |
|---|---|---|
| All Kubernetes nodes | node-exporter (DaemonSet) | `node-exporter` |
| All K8s workloads | kube-state-metrics | `kube-state-metrics` |
| Kestra orchestrator | `kestra:8080/prometheus` | `kestra` |
| Kafka brokers (Strimzi) | `*:9404` JMX exporter | `kafka-jmx` |
| OpenSearch | `*:9200/_prometheus/metrics` | `opensearch` |
| PostgreSQL | `postgresql-metrics:9187` | `postgresql` |
| Apache Doris FE | `doris-fe:8030/metrics` | `doris-fe` |
| Prometheus MCP | `mcp-prometheus:3200` | `mcp-prometheus` |
| Grafana MCP | `mcp-grafana:3201` | `mcp-grafana` |

---

## Deployment Order (ArgoCD sync-waves)

```
Wave 0 — monitoring-namespace   (namespace + quotas)
Wave 1 — prometheus             (metrics engine + alertmanager)
Wave 2 — grafana                (dashboards + alerts UI)
Wave 3 — mcp-prometheus         (AI tools for Prometheus)
Wave 3 — mcp-grafana            (AI tools for Grafana)
```

---

## Quick Deploy

```bash
# 1. Seed all monitoring secrets into OpenBao
sudo bash scripts/master/12-seed-openbao-secrets.sh

# 2. Add required Helm repos
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana               https://grafana.github.io/helm-charts
helm repo update

# 3. Build and push MCP server images
bash docker/mcp-prometheus/build-and-push.sh
bash docker/mcp-grafana/build-and-push.sh

# 4. Apply all ArgoCD monitoring apps (auto-syncs everything)
kubectl apply -f argocd-apps/app-monitoring.yaml -n argocd

# 5. Watch rollout
kubectl get pods -n monitoring -w
```

---

## Environment Configuration

| Env | Prometheus | Alertmanager | Grafana | Values location |
|---|---|---|---|---|
| Production | `:30990` (30d retention, 50Gi) | `:30993` | `:30300` (10Gi) | `helm/prometheus/` + `helm/grafana/` |
| Staging | `:30991` (7d retention, 15Gi) | `:30994` | `:30301` (5Gi) | `staging/helm/prometheus/` + `staging/helm/grafana/` |
| Testing | `:30992` (2d retention, 8Gi) | — | `:30302` (ephemeral) | `testing/helm/prometheus/` + `testing/helm/grafana/` |

---

## Secrets

All secrets are seeded by `scripts/master/12-seed-openbao-secrets.sh`:

| K8s Secret | Namespace | OpenBao Path | Keys |
|---|---|---|---|
| `grafana-credentials` | `monitoring` | `secret/data/grafana/credentials` | `admin-user`, `admin-password`, `secret-key` |
| `prometheus-credentials` | `monitoring` | `secret/data/prometheus/credentials` | `remote-write-user`, `remote-write-password` |

---

## MCP Integration

Both MCP servers implement JSON-RPC 2.0 over HTTP. They are registered in Bob's `.bob/mcp.json`:

```json
{
  "mcpServers": {
    "prometheus": {
      "url": "http://192.168.1.50:30320/mcp"
    },
    "grafana": {
      "url": "http://192.168.1.50:30321/mcp"
    }
  }
}
```

See full tool documentation in [`docs/prometheus.md`](prometheus.md) and [`docs/grafana.md`](grafana.md).

---

## ArgoCD Auto-Sync

All components sync from `https://github.com/johnmillermca/stardatadblabs` (branch `HEAD`).

| ArgoCD App | Source path | Destination |
|---|---|---|
| `monitoring-namespace` | `k8s-platform/manifests/namespaces` | `argocd` ns |
| `prometheus` | Helm chart + `k8s-platform/helm/prometheus/values.yaml` | `monitoring` |
| `grafana` | Helm chart + `k8s-platform/helm/grafana/values.yaml` | `monitoring` |
| `mcp-prometheus` | `k8s-platform/manifests/mcp/prometheus` | `monitoring` |
| `mcp-grafana` | `k8s-platform/manifests/mcp/grafana` | `monitoring` |

After every `git push`, ArgoCD reconciles within 3 minutes (or immediately on manual sync).

---

## File Reference

```
helm/prometheus/values.yaml                           # Production Prometheus
helm/grafana/values.yaml                              # Production Grafana
staging/helm/prometheus/values.yaml                   # Staging Prometheus
staging/helm/grafana/values.yaml                      # Staging Grafana
testing/helm/prometheus/values.yaml                   # Testing Prometheus
testing/helm/grafana/values.yaml                      # Testing Grafana
manifests/namespaces/monitoring-namespace.yaml        # Namespace + quotas
manifests/mcp/prometheus/prometheus-mcp-deployment.yaml
manifests/mcp/grafana/grafana-mcp-deployment.yaml
docker/mcp-prometheus/server.py                       # MCP server code
docker/mcp-prometheus/Dockerfile
docker/mcp-grafana/server.py
docker/mcp-grafana/Dockerfile
argocd-apps/app-monitoring.yaml                       # ArgoCD Application set
```
