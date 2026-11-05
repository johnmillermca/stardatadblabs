#!/usr/bin/env bash
# =============================================================================
# deploy-duckdb.sh
# Build the DuckDB image and deploy all DuckDB/Iceberg components to the cluster.
# Run from the workspace root: bash manifests/duckdb/deploy-duckdb.sh
# =============================================================================
set -euo pipefail

REGISTRY="192.168.1.50:30500"
IMAGE="${REGISTRY}/duckdb:latest"
NS="prod"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# ── 0. Seed Kubernetes Secrets (no OpenBao dependency) ───────────────────────
log "Seeding platform-s3-credentials Kubernetes Secret..."
bash manifests/duckdb/seed-duckdb-secrets.sh

# ── 1. Build the DuckDB image on worker8 (has internet access for pip) ────────
log "Building DuckDB image on worker8.local..."
ssh star_worker8@192.168.1.58 "
  set -e
  cd /tmp
  mkdir -p duckdb-build && cd duckdb-build
  cat > Dockerfile.duckdb <<'DOCKERFILE'
$(cat manifests/duckdb/Dockerfile.duckdb)
DOCKERFILE
  cp $(realpath manifests/duckdb/duckdb_server.py)           ./duckdb_server.py
  cp $(realpath manifests/duckdb/entrypoint.sh)              ./entrypoint.sh
  cp $(realpath manifests/duckdb/stargraph_iceberg_engine.py) ./stargraph_iceberg_engine.py
  cp $(realpath manifests/duckdb/stargraph_sql_proxy.py)     ./stargraph_sql_proxy.py
  docker build -f Dockerfile.duckdb -t ${IMAGE} .
  docker push ${IMAGE}
  echo 'Build and push complete.'
"

# ── 2. Apply PV + PVC on worker8 ──────────────────────────────────────────────
log "Creating PV / PVC for DuckDB on worker8.local..."
# Ensure /home/duckdb-data exists on the node
ssh star_worker8@192.168.1.58 "sudo mkdir -p /home/duckdb-data && sudo chmod 777 /home/duckdb-data"
kubectl apply -f manifests/duckdb/duckdb-worker8-deployment.yaml -n "${NS}"

# ── 3. Deploy DuckDB server ───────────────────────────────────────────────────
log "Deploying DuckDB server to worker8.local..."
kubectl rollout status deployment/duckdb-worker8 -n "${NS}" --timeout=120s

# ── 4. Bootstrap Iceberg tables ───────────────────────────────────────────────
log "Bootstrapping Iceberg tables in Polaris catalog..."
DUCKDB_POD=$(kubectl get pod -n "${NS}" -l app=duckdb-worker8 -o jsonpath='{.items[0].metadata.name}')
kubectl cp manifests/duckdb/duckdb_iceberg_setup.sql "${NS}/${DUCKDB_POD}:/tmp/duckdb_iceberg_setup.sql"
kubectl exec -n "${NS}" "${DUCKDB_POD}" -- \
  bash -c "duckdb \${DUCKDB_DATABASE} < /tmp/duckdb_iceberg_setup.sql"

# ── 5. Deploy sync CronJob ────────────────────────────────────────────────────
log "Deploying stargraph-iceberg-sync CronJob..."
kubectl apply -f manifests/duckdb/stargraph-iceberg-sync-cronjob.yaml -n "${NS}"

# ── 6. Run initial full load ──────────────────────────────────────────────────
log "Running initial full sync (Stargraph → Iceberg)..."
kubectl create job --from=cronjob/stargraph-iceberg-sync stargraph-iceberg-full-load -n "${NS}" || true
kubectl wait --for=condition=complete job/stargraph-iceberg-full-load -n "${NS}" --timeout=600s

# ── 7. Deploy SQL proxy ───────────────────────────────────────────────────────
log "Deploying Stargraph SQL Proxy..."
kubectl apply -f manifests/duckdb/stargraph-sql-proxy-deployment.yaml -n "${NS}"
kubectl rollout status deployment/stargraph-sql-proxy -n "${NS}" --timeout=60s

log "Deployment complete."
log "  DuckDB server  : http://192.168.1.50:30580"
log "  SQL Proxy      : http://192.168.1.50:30680"
log "  Run tests      : duckdb /data/stargraph_iceberg.duckdb < manifests/duckdb/duckdb_iceberg_test.sql"
