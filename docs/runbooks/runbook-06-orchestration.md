# Runbook 06 — Orchestration & Workflow: Kestra, SQLMesh, JupyterHub

> **Orchestration namespace:** `orchestration` · **Analytics namespace:** `analytics`  
> **Kestra UI:** `http://192.168.1.50:30880`  
> **SQLMesh UI:** `http://192.168.1.50:30883`

---

## 1. Orchestration Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Orchestration Layer                                                     │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  Apache Kestra (orchestration namespace)                        │    │
│  │  Declarative workflow engine                                    │    │
│  │  UI: Port 8080 / NodePort 30880                                 │    │
│  │  State: PostgreSQL (kestra DB)                                  │    │
│  │  Queue: Kafka (bitnami cluster)                                 │    │
│  │  Storage: PVC 20 Gi                                             │    │
│  └────────────────┬────────────────────────────────────────────────┘    │
│                   │ orchestrates                                         │
│         ┌─────────┼──────────────────────────┐                          │
│         │         │                          │                          │
│  ┌──────▼──┐ ┌────▼──────────────────┐ ┌────▼─────────────────────┐    │
│  │  Spark  │ │  SQLMesh               │ │  Debezium / Kafka        │    │
│  │  Jobs   │ │  (analytics namespace) │ │  Pipelines               │    │
│  └─────────┘ │  UI: NodePort 30883   │ └──────────────────────────┘    │
│              │  State: PostgreSQL     │                                  │
│              │         (sqlmesh_state)│                                  │
│              │  Engine: Spark RPC     │                                  │
│              │  Catalog: Polaris REST │                                  │
│              └───────────────────────┘                                  │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Apache Kestra — Workflow Orchestration

### 2.1 What Is Kestra?
Kestra is a **declarative workflow orchestration platform** that manages data pipelines as YAML files stored in Git. Key features:
- **Event-driven**: Triggers from Kafka, webhooks, schedules, file system events
- **Code & low-code**: YAML DSL for simple flows, Python/Shell scripts for complex logic
- **Multi-step DAGs**: Tasks, sub-flows, error handling, retries, and parallel execution
- **Plugin ecosystem**: 500+ plugins for databases, cloud APIs, data tools
- **Secrets**: Reads from Kubernetes Secrets injected by OpenBao

### 2.2 Prerequisites
```sql
-- Create the kestra database in PostgreSQL
CREATE DATABASE kestra;
CREATE USER kestra WITH PASSWORD '<kestra-db-password>';
GRANT ALL PRIVILEGES ON DATABASE kestra TO kestra;
\c kestra
GRANT ALL ON SCHEMA public TO kestra;
```

```bash
# Seed credentials in OpenBao
sudo bash scripts/master/12-seed-openbao-secrets.sh
```

### 2.3 Deploy
```bash
# Via ArgoCD
kubectl apply -f argocd-apps/app-kestra.yaml

# Or manually
kubectl apply -f manifests/kestra/kestra-deployment.yaml
kubectl rollout status deployment/kestra -n orchestration

# Verify health
kubectl port-forward svc/kestra -n orchestration 8080:8080 &
curl http://localhost:8080/api/v1/health/ready
```

### 2.4 Core Kestra Concepts

| Concept | Description |
|---|---|
| **Flow** | A YAML DAG definition: `id`, `namespace`, `tasks`, `triggers` |
| **Namespace** | Logical grouping for flows (maps to team or environment) |
| **Task** | A single unit of work — Python, Bash, SQL query, HTTP call, etc. |
| **Trigger** | What starts a flow: schedule, Kafka message, webhook, file |
| **Execution** | A single run of a flow with its full input/output/log |
| **Inputs** | Parameters passed to a flow at runtime |
| **Outputs** | Values produced by tasks, consumable by downstream tasks |

### 2.5 Create and Deploy Flows

#### Simple Scheduled Flow
```yaml
# Save as my-daily-job.yaml
id: daily-etl
namespace: prod

triggers:
  - id: schedule
    type: io.kestra.core.models.triggers.types.Schedule
    cron: "0 2 * * *"  # 2 AM daily

tasks:
  - id: extract
    type: io.kestra.plugin.jdbc.postgresql.Query
    url: "jdbc:postgresql://postgresql.databases.svc.cluster.local:5432/metadata"
    username: "{{ secret('POSTGRESQL_USER') }}"
    password: "{{ secret('POSTGRESQL_PASSWORD') }}"
    sql: "SELECT count(*) AS row_count FROM events WHERE created_at >= CURRENT_DATE"
    fetchType: FETCH

  - id: log-count
    type: io.kestra.core.tasks.log.Log
    message: "Events today: {{ outputs.extract.rows[0].row_count }}"
```

#### Kafka-Triggered Flow
```yaml
id: process-kafka-event
namespace: prod

triggers:
  - id: kafka-trigger
    type: io.kestra.plugin.kafka.Trigger
    bootstrapServers: "kafka.streaming.svc.cluster.local:9092"
    groupId: "kestra-group"
    topic: events-raw
    maxRecords: 100

tasks:
  - id: transform
    type: io.kestra.core.tasks.scripts.Python
    script: |
      import json
      events = {{ trigger.records | tojson }}
      processed = [{"id": e["event_id"], "type": e["event_type"]} for e in events]
      print(json.dumps(processed))
```

#### Spark Job Flow
```yaml
id: spark-iceberg-job
namespace: analytics

tasks:
  - id: submit-spark-job
    type: io.kestra.plugin.spark.SparkSubmit
    master: "spark://spark-master-svc.analytics.svc.cluster.local:7077"
    mainClass: "com.example.IcebergETL"
    jar: "/opt/jars/iceberg-etl.jar"
    conf:
      spark.executor.memory: "2g"
      spark.executor.instances: "3"
      spark.sql.catalog.polaris: "org.apache.iceberg.spark.SparkCatalog"
      spark.sql.catalog.polaris.type: "rest"
      spark.sql.catalog.polaris.uri: "http://polaris-rest.catalog.svc.cluster.local:8181/api/catalog"
```

### 2.6 Deploy a Flow via API
```bash
# Upload a single flow
curl -X POST http://192.168.1.50:30880/api/v1/flows \
  -H "Content-Type: application/x-yaml" \
  --data-binary @my-daily-job.yaml

# List all flows
curl http://192.168.1.50:30880/api/v1/flows?namespace=prod

# Get a specific flow
curl http://192.168.1.50:30880/api/v1/flows/prod/daily-etl

# Delete a flow
curl -X DELETE http://192.168.1.50:30880/api/v1/flows/prod/daily-etl

# Trigger a flow manually
curl -X POST http://192.168.1.50:30880/api/v1/executions \
  -H "Content-Type: application/json" \
  -d '{"namespace": "prod", "flowId": "daily-etl"}'

# Trigger with inputs
curl -X POST http://192.168.1.50:30880/api/v1/executions \
  -H "Content-Type: application/json" \
  -d '{"namespace": "prod", "flowId": "daily-etl", "inputs": {"date": "2025-01-01"}}'
```

### 2.7 Monitor Executions
```bash
# List recent executions
curl "http://192.168.1.50:30880/api/v1/executions?namespace=prod&flowId=daily-etl&pageSize=10"

# Get execution details
curl http://192.168.1.50:30880/api/v1/executions/<execution-id>

# Get execution logs
curl http://192.168.1.50:30880/api/v1/logs/<execution-id>

# Retry a failed execution
curl -X POST http://192.168.1.50:30880/api/v1/executions/<execution-id>/replay

# Kill a running execution
curl -X DELETE http://192.168.1.50:30880/api/v1/executions/<execution-id>/kill
```

### 2.8 Secrets in Kestra
Kestra reads secrets from Kubernetes Secret environment variables with `SECRET_` prefix:
```yaml
# In a flow, access secrets with secret() function
tasks:
  - id: db-query
    type: io.kestra.plugin.jdbc.postgresql.Query
    url: "{{ secret('KESTRA_DB_URL') }}"
    username: "{{ secret('KESTRA_DB_USER') }}"
    password: "{{ secret('KESTRA_DB_PASSWORD') }}"
    sql: "SELECT * FROM events LIMIT 10"
```

### 2.9 Error Handling and Retries
```yaml
id: resilient-etl
namespace: prod

tasks:
  - id: risky-task
    type: io.kestra.plugin.scripts.shell.Commands
    commands:
      - curl -f https://api.example.com/data

errors:
  - id: notify-on-failure
    type: io.kestra.core.tasks.log.Log
    message: "Flow failed: {{ execution.id }}"

retryPolicy:
  type: constant
  interval: PT1M    # retry every 1 minute
  maxAttempt: 3     # up to 3 retries
```

---

## 3. SQLMesh — SQL DataOps & Transformations

### 3.1 What Is SQLMesh?
SQLMesh is a **SQL-first DataOps framework** that manages all data transformations as versioned models. It goes beyond dbt by providing:
- **Virtual data environments**: Preview transformations in `dev` before applying to `prod`
- **Incremental evaluation**: Only re-processes changed partitions
- **Semantic diff**: Shows exactly what data will change before execution
- **Built-in audits**: Data quality checks are first-class citizens
- **State management**: Tracks model evaluation state in PostgreSQL
- **Schema migrations**: Handles DDL changes without full reloads

### 3.2 Prerequisites
```sql
-- Create the SQLMesh state database
CREATE DATABASE sqlmesh_state;
CREATE USER sqlmesh WITH PASSWORD '<password>';
GRANT ALL PRIVILEGES ON DATABASE sqlmesh_state TO sqlmesh;
\c sqlmesh_state
GRANT ALL ON SCHEMA public TO sqlmesh;
```

### 3.3 Deploy
```bash
# Build the custom SQLMesh image
bash docker/sqlmesh/build-and-push.sh

# Deploy via ArgoCD
kubectl apply -f argocd-apps/app-sqlmesh.yaml
```

### 3.4 SQLMesh Model Types

| Model Kind | When to Use |
|---|---|
| `FULL` | Small reference tables, always recomputed |
| `INCREMENTAL_BY_TIME_RANGE` | Partitioned by time — only reprocesses new data |
| `INCREMENTAL_BY_UNIQUE_KEY` | Upsert semantics — merge on unique key |
| `VIEW` | Non-materialized virtual table |
| `SEED` | Static CSV data loaded as a table |
| `EXTERNAL` | External tables defined elsewhere (Iceberg, etc.) |

### 3.5 Create a Model
```sql
-- models/events_hourly.sql
MODEL (
  name analytics.events_hourly,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_hour
  ),
  cron '@hourly',
  grain [event_hour, event_type],
  audits [
    not_null(columns := [event_hour, event_count]),
    accepted_range(column := event_count, min_v := 0)
  ]
);

SELECT
  DATE_TRUNC('hour', event_ts)  AS event_hour,
  event_type,
  COUNT(*)                       AS event_count,
  SUM(amount)                    AS total_amount
FROM analytics.events_raw
WHERE
  event_ts BETWEEN @start_ts AND @end_ts
GROUP BY 1, 2
```

### 3.6 SQLMesh CLI Operations (via API)
```bash
# Plan: compute what needs to change
curl -X POST http://192.168.1.50:30883/api/v1/plan \
  -H "Content-Type: application/json" \
  -d '{"environment": "dev"}'

# Apply the plan to dev environment
curl -X POST http://192.168.1.50:30883/api/v1/apply \
  -H "Content-Type: application/json" \
  -d '{"environment": "dev", "auto_apply": true}'

# Promote dev to prod
curl -X POST http://192.168.1.50:30883/api/v1/plan \
  -H "Content-Type: application/json" \
  -d '{"environment": "prod", "from_environment": "dev"}'

# Run all pending evaluations
curl -X POST http://192.168.1.50:30883/api/v1/run \
  -H "Content-Type: application/json" \
  -d '{"environment": "prod"}'

# Run audits
curl -X POST http://192.168.1.50:30883/api/v1/audit \
  -H "Content-Type: application/json" \
  -d '{"models": ["analytics.events_hourly"]}'

# Backfill a specific model
curl -X POST http://192.168.1.50:30883/api/v1/run \
  -H "Content-Type: application/json" \
  -d '{
    "environment": "prod",
    "models": ["analytics.events_hourly"],
    "start": "2025-01-01",
    "end": "2025-01-31"
  }'

# List all models
curl http://192.168.1.50:30883/api/v1/models

# Execute ad-hoc SQL (fetchdf)
curl -X POST http://192.168.1.50:30883/api/v1/fetchdf \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM analytics.events_hourly LIMIT 10"}'
```

### 3.7 SQLMesh + Kestra Integration
```yaml
# Kestra flow to run SQLMesh nightly
id: sqlmesh-nightly
namespace: analytics

triggers:
  - id: schedule
    type: io.kestra.core.models.triggers.types.Schedule
    cron: "0 1 * * *"

tasks:
  - id: plan
    type: io.kestra.core.tasks.scripts.Python
    script: |
      import requests, json
      r = requests.post("http://sqlmesh.analytics.svc.cluster.local:8001/api/v1/run",
                        json={"environment": "prod"})
      print(json.dumps(r.json(), indent=2))
```

---

## 4. Troubleshooting

### 4.1 Kestra Flow Execution Stuck
```bash
# Check pod health
kubectl get pods -n orchestration
kubectl logs -n orchestration deploy/kestra --tail=100

# Check PostgreSQL connectivity
kubectl exec -n orchestration deploy/kestra -- \
  psql -h postgresql.databases.svc.cluster.local -U kestra -d kestra -c "\l"

# Check Kafka connectivity
kubectl exec -n orchestration deploy/kestra -- \
  nc -zv kafka.streaming.svc.cluster.local 9092
```

### 4.2 SQLMesh Plan Fails with Schema Error
```bash
# Check SQLMesh pod logs
kubectl logs -n analytics deploy/sqlmesh --tail=100

# Verify Spark connectivity
kubectl exec -n analytics deploy/sqlmesh -- \
  curl -s http://spark-master-svc.analytics.svc.cluster.local:8080/api/v1/applications

# Verify Polaris connectivity
kubectl exec -n analytics deploy/sqlmesh -- \
  curl -s http://polaris-rest.catalog.svc.cluster.local:8181/api/catalog/v1/config
```

### 4.3 Kestra Worker Memory Pressure
```bash
# Check resource usage
kubectl top pod -n orchestration

# Scale up (if multiple replicas are configured)
kubectl scale deployment/kestra -n orchestration --replicas=2

# Check for large execution outputs stuck in the queue
curl http://192.168.1.50:30880/api/v1/executions?state=RUNNING
```
