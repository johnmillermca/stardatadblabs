# Apache OpenSearch

## Overview
OpenSearch 3.7.0 — distributed search and analytics engine. Used for log aggregation, full-text search, and observability data.

| Property | Value |
|---|---|
| Chart | `opensearch/opensearch 3.7.0` |
| Namespace | `prod` |
| REST API | `http://192.168.1.50:30920` |
| Internal | `http://opensearch-cluster-master.prod.svc.cluster.local:9200` |
| Node | `worker3.local` (pinned) |
| PVC | 50Gi `local-path` |
| Secret | `opensearch-credentials` |
| Manifest | [`helm/opensearch/values.yaml`](../helm/opensearch/values.yaml) |

## Deployment (ArgoCD)
ArgoCD application: `argocd-apps/app-opensearch.yaml`

## Verify
```bash
OPENSEARCH_PASS=$(kubectl get secret opensearch-credentials -n prod \
  -o jsonpath='{.data.opensearch-password}' | base64 -d)
curl -u "admin:${OPENSEARCH_PASS}" http://192.168.1.50:30920/_cluster/health?pretty
curl -u "admin:${OPENSEARCH_PASS}" http://192.168.1.50:30920/_cat/nodes?v
```

## Create Index
```bash
OPENSEARCH_PASS=$(kubectl get secret opensearch-credentials -n prod \
  -o jsonpath='{.data.opensearch-password}' | base64 -d)
curl -u "admin:${OPENSEARCH_PASS}" \
  -X PUT http://192.168.1.50:30920/my-index \
  -H "Content-Type: application/json" \
  -d '{"settings": {"number_of_shards": 1, "number_of_replicas": 0}}'
```

## Dashboards
OpenSearch Dashboards at `http://192.168.1.50:30601` — see [`docs/opensearch-dashboards.md`](opensearch-dashboards.md).

## Secrets
| Key | Description |
|---|---|
| `opensearch-password` | Admin password |
| `opensearch-user` | Admin username (`admin`) |

OpenBao path: `secret/data/opensearch/credentials`
