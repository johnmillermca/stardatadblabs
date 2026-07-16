# Apache Kestra

## Overview
Apache Kestra (latest-lts / v0.22.x) — declarative workflow orchestration platform. Manages ETL pipelines, scheduled jobs, event-driven workflows, and cross-system data flows. Backed by PostgreSQL for state and Kafka for the task queue.

| Property | Value |
|---|---|
| Namespace | `orchestration` |
| UI URL | `http://192.168.1.50:30880` |
| Internal | `http://kestra.orchestration.svc.cluster.local:8080` |
| Image | `kestra/kestra:latest-lts` (v0.22.x) |
| State backend | PostgreSQL `postgresql.databases.svc.cluster.local:5432/kestra` |
| Queue backend | Kafka `kafka.streaming.svc.cluster.local:9092` |
| Storage | 20 Gi PVC (`/app/storage`) |
| Secret | `kestra-credentials` |
| Manifest | `manifests/kestra/kestra-deployment.yaml` |
| ArgoCD app | `argocd-apps/app-kestra.yaml` |

## Prerequisites
1. PostgreSQL running and `kestra` database created:
```sql
CREATE DATABASE kestra;
CREATE USER kestra WITH PASSWORD '<kestra-db-password>';
GRANT ALL PRIVILEGES ON DATABASE kestra TO kestra;
```
2. Seed secrets:
```bash
sudo bash scripts/master/12-seed-openbao-secrets.sh
```

## Deployment (ArgoCD)
```bash
kubectl apply -f argocd-apps/app-kestra.yaml
```
ArgoCD syncs `manifests/kestra/` to the `orchestration` namespace automatically.

## Manual Deploy
```bash
kubectl apply -f manifests/kestra/kestra-deployment.yaml
kubectl rollout status deployment/kestra -n orchestration
```

## Verify
```bash
# Check pod health
kubectl get pods -n orchestration
# Health endpoints
kubectl port-forward svc/kestra -n orchestration 8080:8080 &
curl http://localhost:8080/api/v1/health/ready
```

## First Workflow
```yaml
# Save as my-first-flow.yaml
id: hello-world
namespace: prod
tasks:
  - id: hello
    type: io.kestra.core.tasks.log.Log
    message: "Hello from Kestra!"
```
```bash
# Upload via API
curl -X POST http://192.168.1.50:30880/api/v1/flows \
  -H "Content-Type: application/x-yaml" \
  --data-binary @my-first-flow.yaml
```

## Secrets in Kestra
Kestra reads secrets from the `SECRET_` prefixed environment variables injected from Kubernetes Secrets. All platform credentials are accessible via OpenBao KV paths.

Example — use PostgreSQL password in a flow:
```yaml
id: db-query
namespace: prod
tasks:
  - id: query
    type: io.kestra.plugin.jdbc.postgresql.Query
    url: "{{ secret('POSTGRESQL_URL') }}"
    sql: "SELECT COUNT(*) FROM events"
```

## Kestra + Spark Integration
```yaml
id: spark-job
namespace: prod
tasks:
  - id: submit
    type: io.kestra.plugin.spark.SparkSubmit
    master: "spark://spark-master-svc.analytics.svc.cluster.local:7077"
    mainClass: com.example.MyJob
    jar: /opt/jars/my-job.jar
```

## Secrets
| Key | Description |
|---|---|
| `db-user` | PostgreSQL user (`kestra`) |
| `db-password` | PostgreSQL password |
| `kafka-user` | Kafka SASL user |
| `kafka-password` | Kafka SASL password |
| `encryption-key` | Kestra secret encryption key |

OpenBao path: `secret/data/kestra/credentials`

## Production Hardening
- Enable basic auth: set `kestra.server.basicAuth.enabled: true`
- Use external S3/GCS/Azure Blob for storage instead of local PVC
- Run multiple replicas with `server executor` + `server worker` split
- Enable Kestra Enterprise for RBAC and tenant isolation
