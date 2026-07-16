# MCP Servers — Model Context Protocol

## Overview
The platform ships five custom MCP (Model Context Protocol) servers that expose platform data services as AI-callable tools. Each server uses the `FastMCP` SSE transport and runs as a Kubernetes deployment in the relevant namespace.

| Server | Image | Port | NodePort | Namespace | Tools |
|---|---|---|---|---|---|
| SQLMesh MCP | `mcp-sqlmesh:1.0.0` | 3100 | 30310 | `analytics` | plan, run, audit, list_models, dag, test, fetchdf |
| Doris MCP | `mcp-doris:1.0.0` | 3101 | 30311 | `analytics` | query, list_databases, list_tables, describe_table, cluster_status, running_queries |
| OpenSearch MCP | `mcp-opensearch:1.0.0` | 3102 | 30312 | `search` | search, list_indices, index_mapping, cluster_health, index_document, delete_index |
| Spark MCP | `mcp-spark:1.0.0` | 3103 | 30313 | `analytics` | cluster_status, list_applications, submit_pi, kill_application, gluten_config, worker_status |
| Kafka MCP | `mcp-kafka:1.0.0` | 3104 | 30314 | `streaming` | list_topics, create_topic, delete_topic, produce, consume, list_consumer_groups, cluster_metadata |

## MCP Transport
All servers use **SSE (Server-Sent Events)** over HTTP. Connect via:
```
http://192.168.1.50:<nodeport>/sse
```

## Build All MCP Images
```bash
for svc in mcp-sqlmesh mcp-doris mcp-opensearch mcp-spark mcp-kafka; do
  bash docker/${svc}/build-and-push.sh
done
```

## Deploy via ArgoCD
```bash
kubectl apply -f argocd-apps/app-mcp-servers.yaml
```
This creates 5 ArgoCD Applications, each pointing to a subdirectory under `manifests/mcp/`.

## Connect an AI Agent (Claude / GPT / LangChain)

### Claude Desktop / Claude.ai
Add to your `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "kafka": {
      "url": "http://192.168.1.50:30314/sse"
    },
    "opensearch": {
      "url": "http://192.168.1.50:30312/sse"
    },
    "doris": {
      "url": "http://192.168.1.50:30311/sse"
    },
    "sqlmesh": {
      "url": "http://192.168.1.50:30310/sse"
    },
    "spark": {
      "url": "http://192.168.1.50:30313/sse"
    }
  }
}
```

### LangChain
```python
from langchain_mcp import MCPToolkit
from langchain_anthropic import ChatAnthropic

toolkit = MCPToolkit(server_url="http://192.168.1.50:30311/sse")  # Doris
tools = toolkit.get_tools()

llm = ChatAnthropic(model="claude-3-5-sonnet-20241022")
agent = create_react_agent(llm, tools)
result = agent.invoke({"input": "How many rows are in the analytics.sales table in Doris?"})
```

## SQLMesh MCP — Tool Reference
| Tool | Description |
|---|---|
| `sqlmesh_plan(environment, start, end, auto_apply)` | Compute plan for target environment |
| `sqlmesh_run(environment)` | Execute pending model evaluations |
| `sqlmesh_audit(model)` | Run data quality audits |
| `sqlmesh_list_models()` | List all models in the project |
| `sqlmesh_dag()` | Return model dependency DAG as JSON |
| `sqlmesh_diff(environment)` | Diff current state vs environment |
| `sqlmesh_test(model)` | Run unit tests |
| `sqlmesh_fetchdf(sql)` | Execute SQL and return results |

## Doris MCP — Tool Reference
| Tool | Description |
|---|---|
| `doris_query(sql, database)` | Execute SQL query |
| `doris_list_databases()` | List all databases |
| `doris_list_tables(database)` | List tables in a database |
| `doris_describe_table(table, database)` | Describe table schema |
| `doris_cluster_status()` | Show backend node status |
| `doris_table_stats(table, database)` | Get row count |
| `doris_running_queries()` | List running queries |

## OpenSearch MCP — Tool Reference
| Tool | Description |
|---|---|
| `opensearch_search(index, query, size)` | Execute search query |
| `opensearch_list_indices(pattern)` | List indices by pattern |
| `opensearch_index_mapping(index)` | Get field mappings |
| `opensearch_cluster_health()` | Cluster health status |
| `opensearch_index_document(index, document, id)` | Index a document |
| `opensearch_delete_index(index)` | Delete an index |

## Spark MCP — Tool Reference
| Tool | Description |
|---|---|
| `spark_cluster_status()` | Master + worker status |
| `spark_list_applications()` | Running and recent apps |
| `spark_submit_pi(partitions)` | Smoke test — submit SparkPi |
| `spark_kill_application(submission_id)` | Kill a running job |
| `spark_gluten_config()` | Get Gluten/Velox spark-conf values |
| `spark_worker_status()` | Worker resource availability |

## Kafka MCP — Tool Reference
| Tool | Description |
|---|---|
| `kafka_list_topics()` | List all non-internal topics |
| `kafka_create_topic(topic, partitions, rf)` | Create a topic |
| `kafka_delete_topic(topic)` | Delete a topic |
| `kafka_produce(topic, message, key)` | Produce a message |
| `kafka_consume(topic, group_id, max_messages)` | Consume messages |
| `kafka_list_consumer_groups()` | List consumer groups |
| `kafka_cluster_metadata()` | Broker and controller info |

## Secrets
MCP servers reuse existing product secrets:
- Doris MCP: `doris-credentials` (key: `admin-password`)
- OpenSearch MCP: `opensearch-credentials` (keys: `opensearch-user`, `opensearch-password`)
- Kafka MCP: `akhq-credentials` (keys: `kafka-sasl-username`, `kafka-sasl-password`)
- SQLMesh MCP: `sqlmesh-credentials` (keys: `db-user`, `db-password`)

## Development — Extend an MCP Server
To add a new tool to any MCP server:
1. Edit `docker/mcp-<product>/server.py`
2. Add a new `@mcp.tool()` decorated function
3. Rebuild: `bash docker/mcp-<product>/build-and-push.sh`
4. ArgoCD will automatically re-sync and rolling-update the deployment
