# Runbook 07 — Observability: Prometheus, Grafana, MCP Servers

> **Monitoring namespace:** `monitoring`  
> **Prometheus UI:** `http://192.168.1.50:30990`  
> **Alertmanager UI:** `http://192.168.1.50:30993`  
> **Grafana UI:** `http://192.168.1.50:30300`

---

## 1. Observability Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│  monitoring namespace                                                    │
│                                                                          │
│  Targets (scraped every 15–60s):                                        │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  kube-state-metrics  node-exporter (all nodes)                 │     │
│  │  kestra (orchestration)    kafka JMX (streaming)               │     │
│  │  opensearch (search)       postgresql (databases)              │     │
│  │  doris-fe (analytics)      spark (analytics)                   │     │
│  │  openbao (prod)            argocd (argocd)                     │     │
│  │  mcp-prometheus  mcp-grafana                                   │     │
│  └──────────────────────────┬─────────────────────────────────────┘     │
│                             │ scrape (PromQL pull model)                 │
│  ┌──────────────────────────▼───────────────────────────────────────┐   │
│  │  Prometheus (kube-prometheus-stack 60.4.0)                       │   │
│  │  Port 9090 / NodePort 30990                                      │   │
│  │  Data retention: 30 days / 40 GB                                 │   │
│  │  Storage: PVC 50 Gi                                              │   │
│  └──────────┬───────────────────────────────────────────────────────┘   │
│             │ alert rules                                                │
│  ┌──────────▼─────────────┐    ┌────────────────────────────────────┐  │
│  │  Alertmanager           │    │  Grafana 8.4.2                     │  │
│  │  Port 9093 / NodePort   │    │  Port 3000 / NodePort 30300        │  │
│  │  30993                  │    │  Queries Prometheus via API        │  │
│  │  Routes alerts to       │    │  Pre-loaded dashboards             │  │
│  │  Slack/PagerDuty/email  │    │  Storage: PVC 10 Gi               │  │
│  └─────────────────────────┘    └────────────────────────────────────┘  │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  MCP Servers (AI Assistant Integration)                         │    │
│  │  mcp-prometheus  Port 3200 / NodePort 30320  (PromQL tools)     │    │
│  │  mcp-grafana     Port 3201 / NodePort 30321  (Dashboard tools)  │    │
│  │  mcp-kafka       Port 3202 / NodePort 30322  (Topic tools)      │    │
│  │  mcp-opensearch  Port 3203 / NodePort 30323  (Search tools)     │    │
│  │  mcp-doris       Port 3204 / NodePort 30324  (SQL tools)        │    │
│  │  mcp-kestra      Port 3205 / NodePort 30325  (Flow tools)       │    │
│  │  mcp-spark       Port 3206 / NodePort 30326  (Job tools)        │    │
│  │  mcp-sqlmesh     Port 3207 / NodePort 30310  (Transform tools)  │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Prometheus

### 2.1 What Is Prometheus?
Prometheus is a **pull-based time-series monitoring system**. It periodically scrapes HTTP `/metrics` endpoints from all services and stores the data as time series. PromQL (Prometheus Query Language) is used to query, aggregate, and alert on this data.

Key concepts:
- **Metrics types**: Counter (monotonically increasing), Gauge (up/down), Histogram, Summary
- **Labels**: Dimensions attached to metrics — `{namespace="prod", pod="openbao-0"}`
- **Scrape interval**: How often Prometheus collects metrics (default: 15s)
- **Recording rules**: Pre-compute expensive queries, stored as new metrics
- **Alert rules**: Trigger alerts when PromQL conditions are met

### 2.2 Deploy
```bash
# Seed credentials
sudo bash scripts/master/12-seed-openbao-secrets.sh

# Via ArgoCD (recommended)
kubectl apply -f argocd-apps/app-monitoring.yaml

# Or manually
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm upgrade --install prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace \
  --version 60.4.0 \
  -f helm/prometheus/values.yaml
```

### 2.3 Prometheus API & PromQL
```bash
# Instant query
curl -s "http://192.168.1.50:30990/api/v1/query?query=up" | python3 -m json.tool

# Range query (last 1 hour, 1-minute steps)
curl -s "http://192.168.1.50:30990/api/v1/query_range?query=container_cpu_usage_seconds_total&start=$(date -d '1 hour ago' +%s)&end=$(date +%s)&step=60" | python3 -m json.tool

# List all metric names
curl -s http://192.168.1.50:30990/api/v1/label/__name__/values | python3 -m json.tool

# Active targets and health
curl -s http://192.168.1.50:30990/api/v1/targets | python3 -c "
import sys,json
data = json.load(sys.stdin)
for t in data['data']['activeTargets']:
    print(t['labels'].get('job','?'), '|', t['health'], '|', t['lastError'] or 'OK')
"

# Active alerts
curl -s http://192.168.1.50:30990/api/v1/alerts | python3 -m json.tool
```

### 2.4 Useful PromQL Queries

```promql
# --- Cluster health ---

# Pod restart rate (last 1h)
increase(kube_pod_container_status_restarts_total[1h])

# OOMKilled containers
kube_pod_container_status_last_terminated_reason{reason="OOMKilled"}

# Nodes not ready
kube_node_status_condition{condition="Ready",status="false"}

# PVC fill rate
predict_linear(kubelet_volume_stats_available_bytes[6h], 24 * 3600) < 0

# --- CPU & Memory ---

# CPU usage per pod (millicores)
sum(rate(container_cpu_usage_seconds_total{container!=""}[5m])) by (namespace, pod) * 1000

# Memory usage per pod (MB)
sum(container_memory_working_set_bytes{container!=""}) by (namespace, pod) / 1024 / 1024

# CPU throttling rate
rate(container_cpu_throttled_seconds_total[5m]) / rate(container_cpu_usage_seconds_total[5m])

# --- Kafka ---

# Consumer group lag (Strimzi)
kafka_consumer_group_lag{group=~".*"}

# Topic message rate
rate(kafka_server_brokertopicmetrics_messagesin_total[5m])

# --- OpenSearch ---

# Indexing rate
rate(opensearch_index_indexing_index_total{index!=""}[1m])

# JVM heap usage
opensearch_jvm_mem_heap_used_in_bytes / opensearch_jvm_mem_heap_max_in_bytes * 100

# --- PostgreSQL ---

# Active connections
pg_stat_activity_count{state="active"}

# Cache hit ratio
pg_stat_database_blks_hit / (pg_stat_database_blks_hit + pg_stat_database_blks_read)

# --- OpenBao ---

# OpenBao request rate
rate(bao_core_handle_request[5m])
```

### 2.5 Add a Custom Scrape Target
```yaml
# In helm/prometheus/values.yaml — add under additionalScrapeConfigs:
additionalScrapeConfigs:
  - job_name: 'my-custom-app'
    scrape_interval: 30s
    static_configs:
      - targets: ['my-app.my-namespace.svc.cluster.local:8080']
    metrics_path: /metrics
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance
```

### 2.6 Create AlertManager Routes
```yaml
# In helm/prometheus/values.yaml
alertmanager:
  config:
    global:
      slack_api_url: 'https://hooks.slack.com/services/...'
    route:
      receiver: 'slack-critical'
      group_by: ['namespace', 'alertname']
      routes:
        - match:
            severity: critical
          receiver: 'pagerduty'
          continue: true
        - match:
            severity: warning
          receiver: 'slack-warning'
    receivers:
      - name: 'slack-critical'
        slack_configs:
          - channel: '#alerts-critical'
            title: '{{ .CommonAnnotations.summary }}'
            text: '{{ range .Alerts }}{{ .Annotations.description }}{{ end }}'
```

### 2.7 Recording Rules
```yaml
# In helm/prometheus/values.yaml — add custom recording rules
additionalPrometheusRulesMap:
  platform-rules:
    groups:
      - name: platform.kafka
        interval: 1m
        rules:
          - record: kafka:consumer_group_lag:sum
            expr: sum(kafka_consumer_group_lag) by (group, topic)
      - name: platform.resources
        rules:
          - record: pod:cpu_usage_millicores:rate5m
            expr: sum(rate(container_cpu_usage_seconds_total{container!=""}[5m])) by (namespace, pod) * 1000
```

---

## 3. Grafana

### 3.1 What Is Grafana in This Platform?
Grafana provides the **visual observability layer** — it queries Prometheus and renders metrics as real-time dashboards. Pre-loaded dashboards cover every component of the platform.

### 3.2 Deploy
```bash
# Via ArgoCD (deployed at wave 2, after Prometheus)
kubectl apply -f argocd-apps/app-monitoring.yaml
```

### 3.3 Access
```bash
# Get admin password
bao kv get -field=admin-password secret/grafana/credentials

# Or from K8s secret
kubectl get secret grafana-credentials -n monitoring \
  -o jsonpath='{.data.admin-password}' | base64 -d

# UI
open http://192.168.1.50:30300
# Login: admin / <password-above>
```

### 3.4 Pre-loaded Dashboards

| Dashboard | Grafana ID | What It Shows |
|---|---|---|
| Kubernetes Cluster Overview | 315 | Cluster-wide CPU, memory, pod counts |
| Node Exporter Full | 1860 | Per-node CPU, memory, disk I/O, network |
| Kafka Overview | 7589 | Strimzi broker throughput, consumer lag |
| PostgreSQL Overview | 9628 | Query rates, connection pool, lock waits |
| OpenSearch Overview | 14191 | Index stats, search rate, JVM heap |
| Kube-state-metrics | 13332 | Deployment rollout status, PVC usage |
| ArgoCD | 14584 | Application sync health, resource counts |

### 3.5 Import an Additional Dashboard
```bash
# Method 1: Via UI
# 1. Grafana UI → "+" → Import
# 2. Enter the Grafana.com dashboard ID
# 3. Select the Prometheus datasource → Import

# Method 2: Via API
curl -X POST http://192.168.1.50:30300/api/dashboards/import \
  -u admin:${GRAFANA_PASS} \
  -H "Content-Type: application/json" \
  -d '{
    "dashboard": <dashboard-json>,
    "overwrite": true,
    "inputs": [{"name": "DS_PROMETHEUS", "type": "datasource", "pluginId": "prometheus", "value": "Prometheus"}],
    "folderId": 0
  }'
```

### 3.6 Grafana API Reference
```bash
GRAFANA_PASS=$(kubectl get secret grafana-credentials -n monitoring \
  -o jsonpath='{.data.admin-password}' | base64 -d)

# List all dashboards
curl -s -u admin:${GRAFANA_PASS} http://192.168.1.50:30300/api/search | \
  python3 -c "import sys,json; [print(d['title'], '->', d['uid']) for d in json.load(sys.stdin)]"

# Get dashboard by UID
curl -s -u admin:${GRAFANA_PASS} http://192.168.1.50:30300/api/dashboards/uid/<uid> | python3 -m json.tool

# List datasources
curl -s -u admin:${GRAFANA_PASS} http://192.168.1.50:30300/api/datasources

# Get org stats
curl -s -u admin:${GRAFANA_PASS} http://192.168.1.50:30300/api/org

# List users
curl -s -u admin:${GRAFANA_PASS} http://192.168.1.50:30300/api/org/users

# Create a user
curl -X POST -u admin:${GRAFANA_PASS} http://192.168.1.50:30300/api/admin/users \
  -H "Content-Type: application/json" \
  -d '{"name":"New User","email":"user@example.com","login":"newuser","password":"<password>"}'

# List active alerts
curl -s -u admin:${GRAFANA_PASS} http://192.168.1.50:30300/api/alerts
```

### 3.7 Adding a New Datasource
```bash
# Add a datasource via API
curl -X POST -u admin:${GRAFANA_PASS} http://192.168.1.50:30300/api/datasources \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Prometheus-2",
    "type": "prometheus",
    "url": "http://prometheus-operated.monitoring.svc.cluster.local:9090",
    "access": "proxy",
    "isDefault": false
  }'
```

---

## 4. MCP Servers (AI Integration Layer)

### 4.1 What Are MCP Servers?
Model Context Protocol (MCP) servers expose platform APIs as **structured tools** consumable by AI assistants (Claude, GPT, Cursor, etc.). Each server implements the JSON-RPC 2.0 MCP specification and provides domain-specific tools.

### 4.2 MCP Server Summary

| Server | Endpoint | Key Tools |
|---|---|---|
| **mcp-prometheus** | `http://192.168.1.50:30320/mcp` | `query_instant`, `query_range`, `get_alerts`, `list_metrics`, `get_targets` |
| **mcp-grafana** | `http://192.168.1.50:30321/mcp` | `list_dashboards`, `get_dashboard`, `search_dashboards`, `list_datasources`, `query_datasource` |
| **mcp-kafka** | `http://192.168.1.50:30322/mcp` | `list_topics`, `describe_topic`, `list_consumer_groups`, `get_consumer_lag` |
| **mcp-opensearch** | `http://192.168.1.50:30323/mcp` | `search`, `get_index_stats`, `list_indices`, `get_cluster_health` |
| **mcp-doris** | `http://192.168.1.50:30324/mcp` | `execute_sql`, `list_databases`, `describe_table`, `get_query_profile` |
| **mcp-kestra** | `http://192.168.1.50:30325/mcp` | `list_flows`, `trigger_execution`, `get_execution_status`, `get_logs` |
| **mcp-spark** | `http://192.168.1.50:30326/mcp` | `submit_job`, `list_applications`, `get_job_status`, `get_stages` |
| **mcp-sqlmesh** | `http://192.168.1.50:30310/mcp` | `sqlmesh_plan`, `sqlmesh_run`, `sqlmesh_audit`, `sqlmesh_list_models`, `sqlmesh_fetchdf` |

### 4.3 Build and Deploy MCP Servers
```bash
# Build all MCP server images
bash docker/mcp-prometheus/build-and-push.sh
bash docker/mcp-grafana/build-and-push.sh
bash docker/mcp-kafka/build-and-push.sh
bash docker/mcp-opensearch/build-and-push.sh
bash docker/mcp-doris/build-and-push.sh
bash docker/mcp-kestra/build-and-push.sh
bash docker/mcp-spark/build-and-push.sh
bash docker/mcp-sqlmesh/build-and-push.sh

# Deploy via ArgoCD
kubectl apply -f argocd-apps/app-mcp-servers.yaml
```

### 4.4 Test MCP Endpoints
```bash
# Test prometheus MCP (list available tools)
curl -X POST http://192.168.1.50:30320/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"tools/list","id":1}'

# Run a PromQL query via MCP
curl -X POST http://192.168.1.50:30320/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "id": 1,
    "params": {
      "name": "query_instant",
      "arguments": {
        "query": "up",
        "time": "now"
      }
    }
  }'

# Search Grafana dashboards via MCP
curl -X POST http://192.168.1.50:30321/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "id": 1,
    "params": {
      "name": "search_dashboards",
      "arguments": {"query": "kafka"}
    }
  }'
```

---

## 5. Alerting

### 5.1 Default Alert Rules
The `kube-prometheus-stack` includes pre-built alert rules for:
- `KubeNodeNotReady` — Node not Ready for > 10 minutes
- `KubePodCrashLooping` — Pod restart rate > 0 in last 15 minutes
- `KubePersistentVolumeFillingUp` — PVC < 15% free
- `KubeDeploymentReplicasMismatch` — Replicas mismatch for > 15 minutes
- `PrometheusTargetMissing` — A scrape target disappeared

### 5.2 View Active Alerts
```bash
# From Prometheus API
curl -s http://192.168.1.50:30990/api/v1/alerts | python3 -m json.tool

# From Alertmanager
curl -s http://192.168.1.50:30993/api/v2/alerts | python3 -m json.tool

# Active alert count
curl -s http://192.168.1.50:30990/api/v1/alerts | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d['data']['alerts']), 'alerts')"
```

### 5.3 Silence an Alert
```bash
# Silence for 2 hours (maintenance window)
curl -X POST http://192.168.1.50:30993/api/v2/silences \
  -H "Content-Type: application/json" \
  -d '{
    "matchers": [{"name": "alertname", "value": "KubeNodeNotReady", "isRegex": false}],
    "startsAt": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'",
    "endsAt": "'$(date -u -d '+2 hours' +%Y-%m-%dT%H:%M:%SZ)'",
    "comment": "Planned maintenance",
    "createdBy": "admin"
  }'
```

---

## 6. Troubleshooting

### 6.1 Prometheus Targets Showing DOWN
```bash
# Find which targets are DOWN
curl -s http://192.168.1.50:30990/api/v1/targets | \
  python3 -c "
import sys,json
data=json.load(sys.stdin)
for t in data['data']['activeTargets']:
    if t['health'] != 'up':
        print(t['labels'].get('job'), '|', t['lastError'])
"

# Common fixes:
# - Pod not running: kubectl get pods -n <namespace>
# - No /metrics endpoint: exec into pod and curl localhost:<port>/metrics
# - NetworkPolicy blocking Prometheus: check network policies
```

### 6.2 Grafana Login Not Working
```bash
# Re-seed the credentials
sudo bash scripts/master/12-seed-openbao-secrets.sh

# Restart Grafana
kubectl rollout restart deployment/grafana -n monitoring

# Check the secret
kubectl get secret grafana-credentials -n monitoring \
  -o jsonpath='{.data.admin-password}' | base64 -d
```

### 6.3 Prometheus PVC Full
```bash
# Check disk usage
kubectl exec -n monitoring prometheus-prometheus-kube-prometheus-prometheus-0 \
  -- df -h /prometheus

# Reduce retention
# Edit helm/prometheus/values.yaml:
# prometheusSpec:
#   retention: 15d          ← reduce from 30d
#   retentionSize: 20GB     ← reduce from 40GB
# Then upgrade:
helm upgrade prometheus prometheus-community/kube-prometheus-stack \
  -n monitoring \
  -f helm/prometheus/values.yaml
```

### 6.4 Alertmanager Not Sending Notifications
```bash
# Check Alertmanager config
kubectl get secret alertmanager-prometheus-kube-prometheus-alertmanager -n monitoring \
  -o jsonpath='{.data.alertmanager\.yaml}' | base64 -d

# Check Alertmanager logs
kubectl logs -n monitoring -l app.kubernetes.io/name=alertmanager --tail=50

# Test Alertmanager config
amtool --alertmanager.url=http://192.168.1.50:30993 config show
```
