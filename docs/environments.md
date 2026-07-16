# Environments Architecture Guide

> **k8s-platform** — prod + monitoring namespace model  
> Master node: `192.168.1.50` · Private registry: `192.168.1.50:30500`  
> GitHub: <https://github.com/johnmillermca/stardatadblabs>

---

## 1. Namespace Model

The platform uses exactly **two namespaces**:

| Namespace | Purpose | Workloads |
|---|---|---|
| `prod` | All platform data workloads | Databases, streaming, analytics, orchestration, MCP servers, security, catalog |
| `monitoring` | Observability stack only | Prometheus, Grafana, Prometheus MCP server, Grafana MCP server |

All legacy namespaces (`databases`, `streaming`, `search`, `analytics`, `security`,
`catalog`, `orchestration`, `kerberos`, `registry`, `strimzi-system`, `openbao`) have
been collapsed into `prod`.

### Resource Quotas

**prod**
- CPU: 60 cores (requests) / 120 cores (limits)
- Memory: 120 Gi (requests) / 240 Gi (limits)
- Storage: 2 Ti
- Pods: 300

**monitoring**
- CPU: 8 cores (requests) / 16 cores (limits)
- Memory: 16 Gi (requests) / 32 Gi (limits)
- Storage: 200 Gi
- Pods: 50

---

## 2. Components by Namespace

### prod namespace — all data platform services

| Component | Chart / Image | Version | NodePort | HA |
|---|---|---|---|---|
| PostgreSQL | bitnami/postgresql | 18.8.0 | 30532 | single |
| MongoDB | bitnami/mongodb | 16.6.0 | 30017 | single |
| Oracle XE | 192.168.1.50:30500/oracle-xe | 21.3.0 | 31521 | single |
| Kafka (Strimzi) | strimzi/strimzi-kafka-operator | 0.45.0 | — | single broker |
| OpenSearch | opensearch-project/opensearch | 2.28.0 | 30920 | 3-node HA |
| OpenSearch Dashboards | opensearch-project/opensearch-dashboards | 2.28.0 | 30601 | 2 replicas |
| Schema Registry | Confluent CP | 7.9.0 | 30810 | 2 replicas |
| AKHQ | tchiotludo/akhq | 0.27.0 | 30808 | single |
| Spark (Gluten+Velox) | 192.168.1.50:30500/spark-gluten-velox | 3.5.1 | 30777/30707 | 1 master + 3 workers |
| JupyterHub | jupyterhub/jupyterhub | 4.x | 30888 | single |
| Kestra | kestra-io/kestra | latest-lts | 30880 | 2 replicas |
| SQLMesh | 192.168.1.50:30500/sqlmesh | 0.99.0 | 30883 | single |
| Apache Doris FE | 192.168.1.50:30500/doris | 2.1.x | 30030 | single FE |
| Apache Doris BE | 192.168.1.50:30500/doris | 2.1.x | — | 3 replicas |
| Apache Ranger | 192.168.1.50:30500/apache-ranger | 2.4.0 | 30860 | single |
| Apache Polaris | 192.168.1.50:30500/apache-polaris | latest | 30882 | single |
| Debezium | 192.168.1.50:30500/debezium | 2.5.x | 30083 | single |
| Kerberos KDC | 192.168.1.50:30500/kerberos-kdc | — | 30088 | single |
| Docker Registry | registry:2 | 2 | 30500 | single |
| OpenBao | openbao/openbao | 2.2.x | 30820 | single |
| MCP-Kafka | 192.168.1.50:30500/mcp-kafka | 1.0.0 | 30300 | single |
| MCP-Doris | 192.168.1.50:30500/mcp-doris | 1.0.0 | 30301 | single |
| MCP-OpenSearch | 192.168.1.50:30500/mcp-opensearch | 1.0.0 | 30302 | single |
| MCP-Spark | 192.168.1.50:30500/mcp-spark | 1.0.0 | 30303 | single |
| MCP-SQLMesh | 192.168.1.50:30500/mcp-sqlmesh | 1.0.0 | 30304 | single |

### monitoring namespace — observability stack

| Component | Chart / Image | Version | NodePort |
|---|---|---|---|
| Prometheus (kube-prometheus-stack) | prometheus-community/kube-prometheus-stack | 60.4.0 | 30990 |
| Alertmanager | included in kube-prometheus-stack | — | 30993 |
| Grafana | grafana/grafana | 8.4.2 | 30300 |
| MCP-Prometheus | 192.168.1.50:30500/mcp-prometheus | 1.0.0 | 30325 |
| MCP-Grafana | 192.168.1.50:30500/mcp-grafana | 1.0.0 | 30326 |

---

## 3. Internal DNS

All services resolve within the cluster using the pattern:

```
<service-name>.prod.svc.cluster.local
<service-name>.monitoring.svc.cluster.local
```

Key DNS entries used across value files:

| Service | DNS |
|---|---|
| PostgreSQL | `postgresql.prod.svc.cluster.local:5432` |
| MongoDB | `mongodb.prod.svc.cluster.local:27017` |
| Kafka bootstrap | `strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092` |
| OpenSearch | `opensearch-cluster-master.prod.svc.cluster.local:9200` |
| Kestra | `kestra.prod.svc.cluster.local:8080` |
| SQLMesh | `sqlmesh.prod.svc.cluster.local:8001` |
| Doris FE | `doris-fe.prod.svc.cluster.local:9030` |
| OpenBao | `openbao.prod.svc.cluster.local:8200` |
| Prometheus | `prometheus-prometheus.monitoring.svc.cluster.local:9090` |
| Grafana | `grafana.monitoring.svc.cluster.local:3000` |

---

## 4. Secret Management (OpenBao)

All passwords, API keys, and credentials are stored in **OpenBao** (deployed in `prod`)
and injected into Kubernetes as native Secrets via the seed script.

```
Vault path pattern: secret/platform/<component>
                    secret/monitoring/<component>
```

Key secrets seeded by [`scripts/master/12-seed-openbao-secrets.sh`](../scripts/master/12-seed-openbao-secrets.sh):

| K8s Secret | Namespace | OpenBao path |
|---|---|---|
| `postgresql-credentials` | prod | `secret/platform/postgresql` |
| `mongodb-credentials` | prod | `secret/platform/mongodb` |
| `kafka-credentials` | prod | `secret/platform/kafka` |
| `opensearch-credentials` | prod | `secret/platform/opensearch` |
| `kestra-credentials` | prod | `secret/platform/kestra` |
| `sqlmesh-credentials` | prod | `secret/platform/sqlmesh` |
| `grafana-credentials` | monitoring | `secret/monitoring/grafana` |
| `jupyterhub-credentials` | prod | `secret/platform/jupyterhub` |
| `ranger-db-credentials` | prod | `secret/platform/ranger` |
| `polaris-db-credentials` | prod | `secret/platform/polaris` |

Retrieve any secret:
```bash
export VAULT_ADDR=http://192.168.1.50:30820
vault login                        # use root token from openbao-init-keys.json
vault kv get secret/platform/postgresql
```

---

## 5. ArgoCD GitOps

All deployments are managed by ArgoCD (namespace `argocd`).

**Active Application files:**

| File | Description |
|---|---|
| `argocd-apps/app-project-platform.yaml` | AppProject — allows `prod` + `monitoring` + `argocd` |
| `argocd-apps/app-namespaces.yaml` | Bootstrap namespaces (sync-wave -20) |
| `argocd-apps/app-openbao.yaml` | OpenBao (sync-wave -15) |
| `argocd-apps/app-prod.yaml` | All prod workloads (sync-waves -10 → +9) |
| `argocd-apps/app-monitoring.yaml` | Monitoring stack (sync-wave 5) |

**Legacy files** (old multi-namespace design) are documented in
[`argocd-apps/LEGACY-README.md`](../argocd-apps/LEGACY-README.md) — **do not apply them**.

Bootstrap ArgoCD and register the app:
```bash
# ArgoCD is deployed in namespace 'argocd' by scripts/master/09-deploy-argocd.sh
kubectl apply -f argocd-apps/app-project-platform.yaml
kubectl apply -f argocd-apps/app-prod.yaml
kubectl apply -f argocd-apps/app-monitoring.yaml
```

---

## 6. Reference Environments (staging / testing)

The `staging/` and `testing/` directories contain **reduced-resource override values**
for non-production use. They are NOT auto-synced by ArgoCD — apply manually:

```bash
# Example: deploy postgres with staging overrides
helm upgrade --install postgresql bitnami/postgresql \
  -f helm/postgresql/values.yaml \
  -f staging/helm/postgresql/values.yaml \
  -n prod
```

| Env | NodePort offset | Storage | Resource tier |
|---|---|---|---|
| staging | 31xxx | Persistent, small PVC | Medium — good for functional testing |
| testing | 32xxx | Ephemeral (no PVC) | Minimal — CI/smoke tests |

The DNS inside staging/testing values still uses `prod` namespace because the cluster
runs one namespace for all environments (resource isolation is via ResourceQuota, not
separate namespaces).

---

## 7. Iceberg Integration

The Spark Gluten+Velox image bundles the Iceberg runtime JAR.

| JAR | Version | Location in image |
|---|---|---|
| `iceberg-spark-runtime-3.5_2.12-1.9.2.jar` | 1.9.2 | `/opt/spark/jars/` |

The source JAR is downloaded to `jars/` locally (gitignored, 44 MB).  
See [`docs/iceberg.md`](iceberg.md) for Spark session configuration.

---

## 8. GitHub Version Control

Push to GitHub:
```bash
sudo dnf install -y git          # first time only
bash scripts/git-sync-github.sh "feat: describe your change"
```

See [`docs/github-backup.md`](github-backup.md) for full workflow.

---

## 9. Backup & Recovery

```bash
bash scripts/master/backup-platform.sh    # backs up PVCs in prod + monitoring
bash scripts/master/restore-platform.sh   # restore from backup
```

See [`docs/backup-recovery.md`](backup-recovery.md) for full procedure.
