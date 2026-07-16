# Prometheus Monitoring — k8s-platform

## Overview

| Field | Value |
|---|---|
| **Chart** | `prometheus-community/kube-prometheus-stack` `60.4.0` |
| **Namespace** | `monitoring` |
| **Prometheus UI** | `http://192.168.1.50:30990` |
| **Alertmanager UI** | `http://192.168.1.50:30993` |
| **Data retention** | 30 days / 40 GB |
| **Storage** | PVC 50 Gi on `local-path` StorageClass |
| **ArgoCD App** | `prometheus` (auto-synced from `helm/prometheus/values.yaml`) |
| **Secrets** | `prometheus-credentials` in namespace `monitoring` (seeded by OpenBao) |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  monitoring namespace                                                   │
│                                                                         │
│  ┌──────────────────┐   scrape   ┌───────────────────────────────────┐ │
│  │  Prometheus      │◄──────────│  Targets across all namespaces    │ │
│  │  (port 9090)     │            │  - kestra (orchestration)         │ │
│  │  NodePort 30990  │            │  - kafka JMX (streaming)          │ │
│  └───────┬──────────┘            │  - opensearch (search)            │ │
│          │ alerts                │  - postgresql (databases)         │ │
│  ┌───────▼──────────┐            │  - doris-fe (analytics)           │ │
│  │  Alertmanager    │            │  - mcp-prometheus (monitoring)    │ │
│  │  (port 9093)     │            │  - mcp-grafana (monitoring)       │ │
│  │  NodePort 30993  │            │  - node-exporter (all nodes)      │ │
│  └──────────────────┘            │  - kube-state-metrics             │ │
│                                  └───────────────────────────────────┘ │
│  ┌──────────────────┐                                                   │
│  │  Prometheus MCP  │  JSON-RPC  tools → AI assistants                │
│  │  (port 3200)     │                                                   │
│  │  NodePort 30320  │                                                   │
│  └──────────────────┘                                                   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Installation

### Step 1 — Seed secrets
```bash
sudo bash scripts/master/12-seed-openbao-secrets.sh
```
Creates `prometheus-credentials` in the `monitoring` namespace via OpenBao KV.

### Step 2 — Add Helm repo
```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
```

### Step 3 — Deploy via ArgoCD (recommended)
```bash
# Apply the monitoring ArgoCD application set
kubectl apply -f argocd-apps/app-monitoring.yaml -n argocd
```
ArgoCD will automatically sync and install the chart from GitHub.

### Step 4 — Deploy manually (alternative)
```bash
helm upgrade --install prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace \
  --version 60.4.0 \
  -f helm/prometheus/values.yaml
```

---

## Verify

```bash
# Check pods
kubectl get pods -n monitoring

# Test Prometheus API
curl http://192.168.1.50:30990/api/v1/status/runtimeinfo | python3 -m json.tool

# Test Alertmanager
curl http://192.168.1.50:30993/-/healthy

# View active targets
curl http://192.168.1.50:30990/api/v1/targets | python3 -m json.tool
```

---

## Prometheus MCP Server

The MCP server runs alongside Prometheus and exposes these tools to AI assistants:

| Tool | Description |
|---|---|
| `query_instant` | Run a PromQL instant query |
| `query_range` | Run a PromQL range query over a time window |
| `list_metrics` | List all metric names |
| `get_alerts` | Get active alerts from Alertmanager |
| `get_targets` | List all scrape targets and health |
| `get_rules` | List alerting and recording rules |
| `query_label_values` | Get distinct values for a label |

**Endpoint:** `http://192.168.1.50:30320/mcp` (JSON-RPC 2.0)

### Build and push image
```bash
bash docker/mcp-prometheus/build-and-push.sh
```

---

## Custom Scrape Targets

Additional targets are defined in `helm/prometheus/values.yaml` under `additionalScrapeConfigs`.
To add a new target:
```yaml
additionalScrapeConfigs:
  - job_name: 'my-app'
    static_configs:
      - targets: ['my-app.namespace.svc.cluster.local:8080']
    metrics_path: /metrics
```
Commit the change — ArgoCD will sync automatically.

---

## Environments

| Environment | Prometheus NodePort | Alertmanager | Values file |
|---|---|---|---|
| Production | 30990 | 30993 | `helm/prometheus/values.yaml` |
| Staging | 30991 | 30994 | `staging/helm/prometheus/values.yaml` |
| Testing | 30992 | — (disabled) | `testing/helm/prometheus/values.yaml` |

---

## OpenBao Secrets

| Secret name | Namespace | OpenBao path |
|---|---|---|
| `prometheus-credentials` | `monitoring` | `secret/data/prometheus/credentials` |

Keys: `remote-write-user`, `remote-write-password`

---

## Troubleshooting

| Symptom | Check |
|---|---|
| Targets showing DOWN | `kubectl logs` the target pod; check `NetworkPolicy` |
| No metrics from a service | Ensure a `ServiceMonitor` or `PodMonitor` exists, or add to `additionalScrapeConfigs` |
| Alertmanager not receiving | Check `prometheusSpec.alerting.alertmanagers` config |
| PVC pending | Ensure `local-path` StorageClass is available |
