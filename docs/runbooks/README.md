# Platform Runbooks — Index

> **Cluster:** `192.168.1.50` (master) + workers `.51–.54`  
> **Platform:** Kubernetes 1.30 · Calico v3.27 · ArgoCD 2.11

This directory contains detailed operational runbooks for every application deployed in the k8s-platform. Each runbook covers: what the application is, how to deploy it, day-to-day management commands, and troubleshooting.

---

## Runbook List

| # | Runbook | Applications Covered | Key Ports |
|---|---|---|---|
| [01](runbook-01-openbao.md) | **OpenBao — Secret Manager** | OpenBao 2.6.0 | UI: 30820 |
| [02](runbook-02-argocd-kubernetes.md) | **ArgoCD & Kubernetes Core** | ArgoCD 2.11, Helm, kubeadm | UI: 30443 |
| [03](runbook-03-data-streaming.md) | **Data Streaming** | Kafka 4.0, Strimzi 1.1/Kafka 3.9, Schema Registry 7.9, Debezium 2.7, AKHQ 0.27 | 30092, 30093, 30810, 30083, 30808 |
| [04](runbook-04-databases.md) | **Databases** | PostgreSQL 17, MongoDB 8.0, Oracle XE 21c | 30532, 30017, 30521 |
| [05](runbook-05-analytics-search.md) | **Analytics & Search** | Apache Doris 2.1, OpenSearch 3.7, Apache Spark 3.5+Gluten+Velox, Apache Iceberg 1.9.2, Apache Polaris | 30030, 30090, 30920, 30601, 30707, 30181 |
| [06](runbook-06-orchestration.md) | **Orchestration & Workflow** | Apache Kestra 0.22, SQLMesh 0.99 | 30880, 30883 |
| [07](runbook-07-observability.md) | **Observability** | Prometheus (kube-prometheus-stack 60.4), Grafana 8.4, 8× MCP Servers | 30990, 30993, 30300, 30320–30326 |
| [08](runbook-08-security-access.md) | **Security & Access** | Kerberos KDC, Apache Ranger 2.4, Private Registry | 30680, 30500 |

---

## All Service Endpoints

| Service | URL | Namespace |
|---|---|---|
| ArgoCD UI | https://192.168.1.50:30443 | `argocd` |
| OpenBao UI | http://192.168.1.50:30820/ui | `prod` |
| Private Registry | https://192.168.1.50:30500 | `registry` |
| Prometheus | http://192.168.1.50:30990 | `monitoring` |
| Alertmanager | http://192.168.1.50:30993 | `monitoring` |
| Grafana | http://192.168.1.50:30300 | `monitoring` |
| Kafka (bitnami) | 192.168.1.50:30092 | `streaming` |
| Kafka (Strimzi) | 192.168.1.50:30093 | `streaming` |
| Schema Registry | http://192.168.1.50:30810 | `streaming` |
| Debezium Connect | http://192.168.1.50:30083 | `streaming` |
| AKHQ UI | http://192.168.1.50:30808 | `streaming` |
| PostgreSQL | 192.168.1.50:30532 | `databases` |
| MongoDB | 192.168.1.50:30017 | `databases` |
| Oracle | 192.168.1.50:30521 | `databases` |
| OpenSearch | http://192.168.1.50:30920 | `search` |
| OpenSearch Dashboards | http://192.168.1.50:30601 | `search` |
| Apache Doris Web UI | http://192.168.1.50:30030 | `analytics` |
| Apache Doris MySQL | 192.168.1.50:30090 | `analytics` |
| Apache Spark UI | http://192.168.1.50:30707 | `analytics` |
| Apache Spark RPC | spark://192.168.1.50:30777 | `analytics` |
| Apache Polaris REST | http://192.168.1.50:30181 | `catalog` |
| Kestra UI | http://192.168.1.50:30880 | `orchestration` |
| SQLMesh UI | http://192.168.1.50:30883 | `analytics` |
| Apache Ranger UI | http://192.168.1.50:30680 | `security` |
| MCP Prometheus | http://192.168.1.50:30320/mcp | `monitoring` |
| MCP Grafana | http://192.168.1.50:30321/mcp | `monitoring` |
| MCP Kafka | http://192.168.1.50:30322/mcp | `streaming` |
| MCP OpenSearch | http://192.168.1.50:30323/mcp | `search` |
| MCP Doris | http://192.168.1.50:30324/mcp | `analytics` |
| MCP Kestra | http://192.168.1.50:30325/mcp | `orchestration` |
| MCP Spark | http://192.168.1.50:30326/mcp | `analytics` |
| MCP SQLMesh | http://192.168.1.50:30310/mcp | `analytics` |

---

## OpenBao Secret Paths

All application credentials are stored in OpenBao at these paths:

| Application | OpenBao Path |
|---|---|
| Grafana | `secret/data/grafana/credentials` |
| Prometheus | `secret/data/prometheus/credentials` |
| PostgreSQL | `secret/data/postgresql/credentials` |
| MongoDB | `secret/data/mongodb/credentials` |
| Kafka | `secret/data/kafka/credentials` |
| OpenSearch | `secret/data/opensearch/credentials` |
| Kestra | `secret/data/kestra/credentials` |
| Apache Doris | `secret/data/doris/credentials` |
| Apache Ranger | `secret/data/ranger/credentials` |
| Kerberos | `secret/data/kerberos/credentials` |
| SQLMesh | `secret/data/sqlmesh/credentials` |
| Apache Polaris | `secret/data/polaris/credentials` |
| AKHQ | `secret/data/akhq/credentials` |
| Schema Registry | `secret/data/schema-registry/credentials` |
| Debezium | `secret/data/debezium/credentials` |

---

## ArgoCD Sync Wave Order

Applications deploy in this order (lower wave first):

```
Wave -20 : storage (local-path StorageClass)
Wave -15 : namespaces (prod/test quotas)
Wave -10 : openbao, strimzi-operator, kerberos
Wave  -5 : private-registry
Wave   0 : prometheus, strimzi-kafka, postgresql, mongodb, opensearch, spark, doris, polaris, ranger
Wave   2 : grafana
Wave   5 : schema-registry
Wave  10 : kestra, debezium
Wave  15 : sqlmesh
```

---

## Quick-Start Commands

```bash
# 1. Check overall cluster health
kubectl get nodes && kubectl get pods -A | grep -v Running | grep -v Completed

# 2. Check all ArgoCD application sync status
argocd app list

# 3. Unseal OpenBao (required after pod restarts)
export BAO_ADDR="http://192.168.1.50:30820"
bao status  # check if sealed
# If sealed: bao operator unseal <key1> && bao operator unseal <key2> && bao operator unseal <key3>

# 4. Seed all application secrets (first-time setup)
sudo bash scripts/master/12-seed-openbao-secrets.sh

# 5. Deploy all platform applications
kubectl apply -f argocd-apps/
```
