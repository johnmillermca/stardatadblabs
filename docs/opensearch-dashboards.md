# OpenSearch Dashboards

## Overview
OpenSearch Dashboards 3.7.0 — web UI for visualizing data stored in OpenSearch.

| Property | Value |
|---|---|
| Chart | `opensearch/opensearch-dashboards 3.7.0` |
| Namespace | `search` |
| UI URL | `http://192.168.1.50:30601` |
| Backend | `http://opensearch-cluster-master.search.svc.cluster.local:9200` |

## Deployment (ArgoCD)
ArgoCD application: `argocd-apps/app-opensearch-dashboards.yaml`

## Usage
1. Open `http://192.168.1.50:30601`
2. Navigate to **Discover** to explore indexed data
3. Create visualizations under **Visualize**
4. Build dashboards under **Dashboards**

## Connect to OpenSearch Index
- Go to **Management → Index Patterns**
- Create a pattern matching your index (e.g. `cdc-*`)
