#!/usr/bin/env bash
# =============================================================================
# doris_query.sh — Run a Doris MySQL query with credentials from OpenBao
#
# Usage:
#   ./doris_query.sh "SELECT COUNT(*) FROM iceberg_polaris.lakehouse.events;"
#   ./doris_query.sh -f my_query.sql
#
# Requirements:
#   - kubectl (with kubeconfig pointing at the cluster)
#   - curl, base64, python3 (standard on Linux/macOS)
#   - OpenBao running at http://192.168.1.50:30820
#     with secret/platform/doris  containing: admin_password
#
# The script never stores credentials on disk. They live only in shell
# variables for the duration of the process.
# =============================================================================
set -euo pipefail

BAO_ADDR="${BAO_ADDR:-http://192.168.1.50:30820}"
DORIS_HOST="${DORIS_HOST:-192.168.1.50}"
DORIS_PORT="${DORIS_PORT:-30090}"
DORIS_USER="${DORIS_USER:-root}"
NAMESPACE="${NAMESPACE:-prod}"

# ── 1. Authenticate to OpenBao ───────────────────────────────────────────────
_bao_token() {
  # Try K8s service-account auth first (works inside cluster pods)
  local sa_jwt="/var/run/secrets/kubernetes.io/serviceaccount/token"
  if [ -f "$sa_jwt" ]; then
    curl -sf --max-time 10 \
      -X POST "${BAO_ADDR}/v1/auth/kubernetes/login" \
      -H "Content-Type: application/json" \
      -d "{\"role\":\"platform-secrets-read\",\"jwt\":\"$(cat $sa_jwt)\"}" \
      | sed 's/.*"client_token":"\([^"]*\)".*/\1/'
    return
  fi
  # Fall back to root token from K8s secret (works outside cluster with kubectl)
  kubectl get secret openbao-unseal-keys -n "${NAMESPACE}" \
    -o jsonpath='{.data.root-token}' | base64 -d
}

# ── 2. Read a secret key from OpenBao ────────────────────────────────────────
_bao_secret() {
  local path="$1" key="$2"
  curl -sf --max-time 10 \
    -H "X-Vault-Token: ${BAO_TOKEN}" \
    "${BAO_ADDR}/v1/secret/data/${path}" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['${key}'])"
}

# ── 3. Fetch credentials ─────────────────────────────────────────────────────
echo "[doris_query] Fetching credentials from OpenBao..." >&2
BAO_TOKEN=$(_bao_token)
if [ -z "$BAO_TOKEN" ]; then
  echo "ERROR: Could not obtain OpenBao token" >&2
  exit 1
fi
DORIS_PWD=$(_bao_secret "platform/doris" "admin_password")
echo "[doris_query] Credentials obtained." >&2

# ── 4. Build the query ───────────────────────────────────────────────────────
if [ "${1:-}" = "-f" ]; then
  QUERY_FILE="${2:?Usage: $0 -f <sql_file>}"
  MYSQL_ARGS=(-e "$(cat "$QUERY_FILE")")
else
  QUERY="${1:?Usage: $0 \"<SQL>\"}"
  MYSQL_ARGS=(-e "$QUERY")
fi

# ── 5. Execute via kubectl exec (avoids exposing password on host CLI) ────────
kubectl exec -n "${NAMESPACE}" doris-fe-0 -c doris-fe -- \
  mysql -h 127.0.0.1 -P 9030 \
        -u "${DORIS_USER}" \
        -p"${DORIS_PWD}" \
        "${MYSQL_ARGS[@]}" \
        2>/dev/null
