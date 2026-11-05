#!/usr/bin/env bash
# =============================================================================
# seed-duckdb-secrets.sh
# Creates the Kubernetes Secrets required by the DuckDB / Iceberg components.
# No OpenBao / Vault dependency — uses kubectl directly.
#
# Secrets created:
#   platform-s3-credentials   (namespace: prod)
#     access_key  — S3 HMAC access key
#     secret_key  — S3 HMAC secret key
#
# Usage:
#   export PLATFORM_S3_ACCESS_KEY="<your-aws-access-key-id>"
#   export PLATFORM_S3_SECRET_KEY="<your-aws-secret-access-key>"
#   bash manifests/duckdb/seed-duckdb-secrets.sh
#
# Safe to re-run — uses --dry-run=client | kubectl apply so it is idempotent.
# =============================================================================
set -euo pipefail

NS="prod"
SECRET_NAME="platform-s3-credentials"

log() { echo "[$(date '+%H:%M:%S')] $*"; }
die() { echo "[ERROR] $*" >&2; exit 1; }

# ── Validate inputs ──────────────────────────────────────────────────────────
[[ -n "${PLATFORM_S3_ACCESS_KEY:-}" ]] \
  || die "PLATFORM_S3_ACCESS_KEY is not set. Export it before running this script."
[[ -n "${PLATFORM_S3_SECRET_KEY:-}" ]] \
  || die "PLATFORM_S3_SECRET_KEY is not set. Export it before running this script."

# ── Ensure namespace exists ──────────────────────────────────────────────────
kubectl create namespace "${NS}" --dry-run=client -o yaml | kubectl apply -f - 2>/dev/null || true

# ── Create / update the Kubernetes Secret ───────────────────────────────────
log "Applying Secret '${SECRET_NAME}' in namespace '${NS}'..."

kubectl create secret generic "${SECRET_NAME}" \
  --namespace "${NS}" \
  --from-literal=access_key="${PLATFORM_S3_ACCESS_KEY}" \
  --from-literal=secret_key="${PLATFORM_S3_SECRET_KEY}" \
  --dry-run=client -o yaml \
  | kubectl apply -f -

log "Secret '${SECRET_NAME}' applied."

# ── Verify ───────────────────────────────────────────────────────────────────
log "Verifying secret keys..."
KEYS=$(kubectl get secret "${SECRET_NAME}" -n "${NS}" \
  -o jsonpath='{.data}' | python3 -c "
import json, sys
data = json.load(sys.stdin)
print('  Keys present: ' + ', '.join(sorted(data.keys())))
")
echo "${KEYS}"

log "Done. The following components will pick up the secret on next pod restart:"
echo "  - duckdb-worker8 (Deployment)"
echo "  - stargraph-iceberg-sync (CronJob)"
echo "  - polaris (Deployment)"
