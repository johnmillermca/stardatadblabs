#!/usr/bin/env bash
# =============================================================================
# entrypoint.sh — DuckDB container startup
# 1. Writes a runtime init SQL that applies S3 credentials from env vars
# 2. Bootstraps the DuckDB database file with extensions + Polaris attachment
# 3. Launches the HTTP API server
# =============================================================================
set -euo pipefail

DB="${DUCKDB_DATABASE:-/data/stargraph_iceberg.duckdb}"
POLARIS_URI="${POLARIS_URI:-http://polaris-auth-proxy.prod.svc.cluster.local:8283/api/catalog}"
POLARIS_WAREHOUSE="${POLARIS_WAREHOUSE:-IcebergCatalog}"
S3_KEY="${AWS_ACCESS_KEY_ID}"
S3_SECRET="${AWS_SECRET_ACCESS_KEY}"
S3_REGION="${AWS_DEFAULT_REGION:-us-east-2}"

echo "[duckdb-entrypoint] Database : ${DB}"
echo "[duckdb-entrypoint] Polaris  : ${POLARIS_URI}  warehouse=${POLARIS_WAREHOUSE}"

# ── Generate runtime bootstrap SQL ──────────────────────────────────────────
# DuckDB v2.0.0 pattern:
#   1. CREATE SECRET for S3 HMAC credentials
#   2. ATTACH warehouse-name AS alias (TYPE iceberg, ENDPOINT ...)
cat > /tmp/runtime_init.sql <<EOF
-- Load extensions (already installed in image layer — no network call)
LOAD iceberg;
LOAD httpfs;
LOAD aws;

-- Named S3 secret (DuckDB v2.0+ Secrets Manager)
CREATE OR REPLACE SECRET s3_hmac (
    TYPE            s3,
    KEY_ID          '${S3_KEY}',
    SECRET          '${S3_SECRET}',
    REGION          '${S3_REGION}',
    ENDPOINT        's3.${S3_REGION}.amazonaws.com',
    URL_STYLE       'path'
);

-- Attach Polaris REST Iceberg catalog (DuckDB v2.0+ syntax)
-- polaris-auth-proxy (port 8283) injects Bearer tokens automatically,
-- so DuckDB must not attempt its own OAuth2 exchange.
ATTACH '${POLARIS_WAREHOUSE}' AS polaris (
    TYPE               iceberg,
    URI                '${POLARIS_URI}',
    AUTHORIZATION_TYPE 'none'
);
EOF

# ── Bootstrap the persistent database (idempotent) ──────────────────────────
duckdb "${DB}" < /tmp/runtime_init.sql
echo "[duckdb-entrypoint] DuckDB bootstrapped — starting HTTP server."

# ── Hand off to the HTTP server ──────────────────────────────────────────────
exec python3 /app/duckdb_server.py
