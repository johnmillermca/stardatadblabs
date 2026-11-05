# DuckDB + Iceberg on Stargraph (worker8)

## Overview

This directory adds **DuckDB** as an analytics read-layer on top of the existing Apache Polaris / Iceberg data lake, running on **worker8.local (192.168.1.58)**.

Stargraph graph data is continuously synced from the Stargraph (Apache Doris) cluster into three Iceberg tables in the `xdatatoiceberg1` S3 bucket, served via the Polaris REST catalog (`IcebergCatalog` warehouse). A lightweight SQL proxy translates Stargraph Cypher patterns to DuckDB SQL for BI tools and CI validation.

---

## Architecture

```
Stargraph FE (MySQL :30090)
        │
        │  stargraph_iceberg_engine.py  (CronJob every 5 min)
        ▼
 DuckDB :duckdb-worker8 (worker8.local:30580)
        │  ATTACH polaris catalog
        ▼
 Apache Polaris REST (:30181) → s3://xdatatoiceberg1/stargraph/
        │
        ▼ (read path)
 stargraph_sql_proxy.py (worker8.local:30680)
        │  POST /cypher  → translate → DuckDB
        │  POST /sql     → passthrough → DuckDB
        │  POST /compare → run both Iceberg + live Stargraph, diff results
```

---

## Files

| File | Purpose |
|------|---------|
| `Dockerfile.duckdb` | DuckDB image with `iceberg`, `httpfs`, `aws` extensions pre-installed |
| `entrypoint.sh` | Container startup: writes S3 config from Secret env vars, bootstraps DB |
| `duckdb_server.py` | FastAPI HTTP server exposing `/query`, `/query/stream`, `/explain`, `/tables`, `/health` |
| `stargraph_iceberg_engine.py` | Python sync engine: Stargraph → DuckDB Iceberg (full + incremental watermark) |
| `stargraph_sql_proxy.py` | Cypher→SQL translator + proxy combining DuckDB and live Stargraph results |
| `duckdb_iceberg_setup.sql` | DDL: creates three Iceberg tables in `polaris.stargraph` namespace |
| `duckdb_iceberg_test.sql` | T0–T12 test suite (schema, credentials, partitions, time-travel, joins) |
| `duckdb-worker8-deployment.yaml` | Kubernetes PV + PVC + Deployment + NodePort Service for DuckDB |
| `stargraph-iceberg-sync-cronjob.yaml` | CronJob running the sync engine every 5 minutes |
| `stargraph-sql-proxy-deployment.yaml` | Deployment + NodePort Service for the SQL proxy |
| `deploy-duckdb.sh` | One-shot deploy script: build image → apply manifests → initial full load |

---

## Iceberg Tables

All tables are in the `polaris.stargraph` namespace backed by `s3://xdatatoiceberg1/stargraph/`.

| Table | Partition | Description |
|-------|-----------|-------------|
| `stargraph_nodes` | `(node_type, snap_date)` | All graph node entities |
| `stargraph_edges` | `(relationship_type, snap_date)` | Graph relationships / edges |
| `stargraph_customers` | `(tier, snap_date)` | Typed Customer projection |

All tables use **Iceberg v2** format, **ZSTD Parquet**, 128 MiB target file size, and **merge-on-read** deletes so soft-deletes propagate correctly.

---

## Deployment

```bash
# From workspace root
bash manifests/duckdb/deploy-duckdb.sh
```

Prerequisites:
- `stardata-credentials` Secret (key: `admin-password`) already present in `prod`
- `polaris-auth-proxy` running in `prod`
- `worker8.local` node joined and labelled in the cluster
- S3 HMAC credentials exported (see below — `seed-duckdb-secrets.sh` creates the Kubernetes Secret):

```bash
export PLATFORM_S3_ACCESS_KEY="<your-access-key>"
export PLATFORM_S3_SECRET_KEY="<your-secret-key>"
bash manifests/duckdb/seed-duckdb-secrets.sh
```

This creates the `platform-s3-credentials` Kubernetes Secret directly in the `prod` namespace — no OpenBao or Vault dependency.

---

## Endpoints

| Endpoint | NodePort | Description |
|----------|----------|-------------|
| DuckDB server `/query` | `30580` | Execute SQL directly against Iceberg |
| DuckDB server `/health` | `30580` | Version + liveness check |
| SQL Proxy `/cypher` | `30680` | Translate Cypher → SQL → Iceberg |
| SQL Proxy `/sql` | `30680` | Passthrough SQL via proxy |
| SQL Proxy `/compare` | `30680` | Side-by-side Iceberg vs live Stargraph |

---

## Quick Test

```bash
# Health
curl http://192.168.1.50:30580/health

# Count nodes by type (partition-pruned)
curl -X POST http://192.168.1.50:30580/query \
     -H "Content-Type: application/json" \
     -d '{"sql": "SELECT node_type, COUNT(*) FROM polaris.stargraph.stargraph_nodes GROUP BY node_type"}'

# Cypher query via proxy
curl -X POST http://192.168.1.50:30680/cypher \
     -H "Content-Type: application/json" \
     -d '{"cypher": "MATCH (c:Customer {tier: '\''GOLD'\''}) RETURN c LIMIT 5"}'

# Compare Iceberg snapshot vs live Stargraph
curl -X POST http://192.168.1.50:30680/compare \
     -H "Content-Type: application/json" \
     -d '{"cypher": "MATCH (c:Customer) WHERE c.city = '\''London'\'' RETURN c LIMIT 10"}'
```

---

## S3 Credentials

Credentials are stored in the `platform-s3-credentials` Kubernetes Secret and are **never hardcoded** in any manifest or source file. The sync engine and DuckDB server read them at runtime from environment variables injected by the Secret.

Bucket: **xdatatoiceberg1** | Region: **us-east-2**
