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

## Ranger RBAC

OpenSearch uses the built-in security plugin with Ranger as the authorization backend. Authentication still uses the default internal user database; Ranger controls **what** authenticated users can access.

| Property | Value |
|---|---|
| Ranger service name | `opensearch` |
| Ranger Admin URL | `http://ranger-admin.prod.svc.cluster.local:6080` |
| Policy poll interval | 30 s |
| SSL | Disabled (in-cluster plain HTTP) |

### Registering the `opensearch` service in Ranger
Register via the Ranger Admin UI at `http://192.168.1.50:30680`:

1. **Access Manager → Service Manager** → click **+** next to **ELASTICSEARCH** (OpenSearch uses the elasticsearch service type)
2. Set **Service Name** = `opensearch`
3. Set **ElasticSearch URL** = `http://opensearch-cluster-master.prod.svc.cluster.local:9200`
4. **Save**

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
