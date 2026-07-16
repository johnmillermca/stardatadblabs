# SQLMesh

## Overview
SQLMesh 0.99.0 — SQL-first DataOps framework for managing data transformations. Provides incremental model evaluation, time-based backfill, virtual data environments, built-in data quality audits, and CI/CD-safe schema migrations. Uses Spark as the execution engine and Polaris for the Iceberg catalog.

| Property | Value |
|---|---|
| Namespace | `analytics` |
| UI URL | `http://192.168.1.50:30883` |
| Internal | `http://sqlmesh.analytics.svc.cluster.local:8001` |
| Image | `192.168.1.50:30500/sqlmesh:0.99.0` (built from `docker/sqlmesh/`) |
| Execution engine | Spark `spark-master-svc.analytics.svc.cluster.local:7077` |
| Iceberg catalog | Polaris REST `polaris-rest.catalog.svc.cluster.local:8181` |
| State store | PostgreSQL `sqlmesh_state` database |
| Secret | `sqlmesh-credentials` |

## Build Image First
```bash
bash docker/sqlmesh/build-and-push.sh
```
This builds `python:3.11-slim` + `sqlmesh[spark]==0.99.0` + `pyspark==3.5.1` + `pyiceberg` and pushes to `192.168.1.50:30500/sqlmesh:0.99.0`.

## Prerequisites
1. Create `sqlmesh_state` PostgreSQL database:
```sql
CREATE DATABASE sqlmesh_state;
CREATE USER sqlmesh WITH PASSWORD '<password>';
GRANT ALL PRIVILEGES ON DATABASE sqlmesh_state TO sqlmesh;
```
2. Seed secrets:
```bash
sudo bash scripts/master/12-seed-openbao-secrets.sh
```

## Deployment (ArgoCD)
```bash
kubectl apply -f argocd-apps/app-sqlmesh.yaml
```

## Define a Model
```sql
-- models/events_daily.sql
MODEL (
  name analytics.events_daily,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_date
  ),
  cron '@daily',
  grain [user_id, event_date]
);

SELECT
  DATE(event_ts) AS event_date,
  user_id,
  COUNT(*) AS event_count
FROM raw.events
WHERE
  event_ts BETWEEN @start_ts AND @end_ts
GROUP BY 1, 2
```

## Run via MCP (AI Agent)
The SQLMesh MCP server at `http://192.168.1.50:30310` exposes:
- `sqlmesh_plan` — compute a plan for an environment
- `sqlmesh_run` — execute pending evaluations
- `sqlmesh_audit` — run data quality audits
- `sqlmesh_list_models` — list all models
- `sqlmesh_test` — run unit tests
- `sqlmesh_fetchdf` — execute ad-hoc SQL

## Secrets
| Key | Description |
|---|---|
| `db-user` | PostgreSQL user (`sqlmesh`) |
| `db-password` | PostgreSQL password |

OpenBao path: `secret/data/sqlmesh/credentials`
