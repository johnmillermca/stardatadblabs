# Legacy ArgoCD Application Files — DO NOT APPLY

> **These files are superseded and must NOT be applied to the cluster.**

## Status

All per-component ArgoCD Application files in this directory (listed below) were
created during the initial multi-namespace design.  They have been **replaced** by
two consolidated Application files:

| Active file | Purpose |
|---|---|
| [`app-prod.yaml`](app-prod.yaml) | All platform workloads in the `prod` namespace (sync-waves -20 → +9) |
| [`app-monitoring.yaml`](app-monitoring.yaml) | Prometheus, Grafana, and their MCP servers in the `monitoring` namespace |
| [`app-project-platform.yaml`](app-project-platform.yaml) | ArgoCD AppProject — destinations: `prod`, `monitoring`, `argocd` |
| [`app-namespaces.yaml`](app-namespaces.yaml) | Bootstraps `prod` and `monitoring` namespaces (sync-wave -20) |
| [`app-openbao.yaml`](app-openbao.yaml) | Deploys OpenBao into `prod` (sync-wave -15) |

## Legacy files (superseded — kept for audit trail only)

```
app-akhq.yaml            → namespace: analytics  (old)
app-debezium.yaml        → namespace: streaming   (old)
app-doris.yaml           → namespace: analytics   (old)
app-jupyterhub.yaml      → namespace: analytics   (old)
app-kafka.yaml           → namespace: streaming   (old)
app-kerberos.yaml        → namespace: kerberos    (old)
app-kestra.yaml          → namespace: orchestration (old)
app-mcp-servers.yaml     → namespace: prod         (partially updated)
app-mongodb.yaml         → namespace: databases    (old)
app-opensearch-dashboards.yaml → namespace: search (old)
app-opensearch.yaml      → namespace: search       (old)
app-oracle.yaml          → namespace: databases    (old)
app-polaris.yaml         → namespace: catalog      (old)
app-postgresql.yaml      → namespace: databases    (old)
app-ranger.yaml          → namespace: security     (old)
app-registry.yaml        → namespace: registry     (old)
app-schema-registry.yaml → namespace: streaming    (old)
app-spark.yaml           → namespace: analytics    (old)
app-sqlmesh.yaml         → namespace: analytics    (old)
app-storage.yaml         → (storage classes stub)
app-strimzi-kafka.yaml   → namespace: streaming    (old)
app-strimzi-operator.yaml → namespace: strimzi-system (old)
```

## Why they are kept

Git history is the authoritative record.  The files are retained (not deleted) so
that the migration path from the old multi-namespace model to the current
`prod`+`monitoring` model is visible in version control.

**Do not re-apply these files.  ArgoCD will deploy duplicate resources into
wrong namespaces if you do.**
