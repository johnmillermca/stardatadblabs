# Apache OpenSearch

## Overview
OpenSearch 3.7.0 — distributed search and analytics engine. Used for log aggregation, full-text search, and observability data.

| Property | Value |
|---|---|
| Chart | `opensearch/opensearch 3.7.0` |
| Namespace | `search` |
| REST API | `http://192.168.1.50:30920` |
| Internal | `http://opensearch-cluster-master.search.svc.cluster.local:9200` |
| Secret | `opensearch-credentials` |

> ⚠️ Security plugin is **disabled** for lab use. Enable for production.

## Deployment (ArgoCD)
ArgoCD application: `argocd-apps/app-opensearch.yaml`

## Verify
```bash
curl http://192.168.1.50:30920/_cluster/health?pretty
curl http://192.168.1.50:30920/_cat/nodes?v
```

## Create Index
```bash
curl -X PUT http://192.168.1.50:30920/my-index \
  -H "Content-Type: application/json" \
  -d '{"settings": {"number_of_shards": 1, "number_of_replicas": 0}}'
```

## Dashboards
OpenSearch Dashboards at `http://192.168.1.50:30601` — see `docs/opensearch-dashboards.md`.

## Secrets
| Key | Description |
|---|---|
| `opensearch-password` | Admin password |
| `opensearch-user` | Admin username (`admin`) |

OpenBao path: `secret/data/opensearch/credentials`
