#!/usr/bin/env bash
# =============================================================================
# rotate-secret.sh
# Safely rotates a service password across OpenBao KV v2, the Kubernetes
# Secret, and — where applicable — the live database role.
#
# Usage:
#   bash scripts/master/rotate-secret.sh <service>
#
# Supported services:
#   postgresql   — postgres superuser password
#   polaris      — Polaris PostgreSQL user password
#   kerberos     — Kerberos KDC master + admin passwords
#   doris        — Doris admin password
#   grafana      — Grafana admin password
#   opensearch   — OpenSearch admin password
#
# What it does (for every service):
#   1. Generate a new password with openssl rand
#   2. Write the new value to OpenBao KV v2 (creates a new version — old
#      version is retained in history and can be rolled back)
#   3. Update the Kubernetes Secret (kubectl apply --dry-run | apply)
#   4. Apply the new password to the live service (ALTER ROLE, API call, etc.)
#   5. Roll the affected deployment so pods pick up the new secret
#
# Roll-back:
#   OpenBao keeps all previous versions. To roll back:
#     bao kv rollback -mount=secret <path> <version>
#   Then re-run rotate-secret.sh to re-sync K8s secret and live service.
#
# Safe to re-run — if the live service already has the new password it is a
# no-op at the DB level.
# =============================================================================
set -euo pipefail
export PATH="/usr/local/bin:${PATH}"

SERVICE="${1:-}"
BAO_ADDR="http://192.168.1.50:30820"
KEYS_FILE="${HOME}/openbao-init-keys.json"

# ── Helpers ───────────────────────────────────────────────────────────────────
log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "  ✓ $*"; }
die()  { echo "[ERROR] $*" >&2; exit 1; }

gen_password() {
  openssl rand -base64 24 | tr -dc 'A-Za-z0-9' | head -c 24
}

[[ -f "${KEYS_FILE}" ]] || die "OpenBao keys file not found: ${KEYS_FILE}"
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('${KEYS_FILE}'))['root_token'])")
export BAO_TOKEN="${ROOT_TOKEN}"

bao_write() {
  local path="$1"; shift
  local json="{"
  local sep=""
  for kv in "$@"; do
    local k="${kv%%=*}" v="${kv#*=}"
    v="${v//\"/\\\"}"
    json+="${sep}\"${k}\":\"${v}\""
    sep=","
  done
  json+="}"
  curl -sf -X POST \
    -H "X-Vault-Token: ${ROOT_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"data\":${json}}" \
    "${BAO_ADDR}/v1/${path}" > /dev/null
}

bao_read() {
  # bao_read <path> <key>
  curl -sf -H "X-Vault-Token: ${ROOT_TOKEN}" \
    "${BAO_ADDR}/v1/${1}" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['${2}'])"
}

kube_secret_update() {
  # kube_secret_update <name> <namespace> key=value [...]
  local name="$1" ns="$2"; shift 2
  local literals=()
  for kv in "$@"; do literals+=("--from-literal=${kv}"); done
  kubectl create secret generic "${name}" -n "${ns}" \
    "${literals[@]}" \
    --dry-run=client -o yaml | kubectl apply -f -
}

roll_deployment() {
  local ns="$1" deploy="$2"
  log "Rolling deployment ${deploy} in ${ns}..."
  kubectl rollout restart deployment/"${deploy}" -n "${ns}" 2>/dev/null \
    && kubectl rollout status deployment/"${deploy}" -n "${ns}" --timeout=120s \
    || log "WARNING: rollout did not complete in 120s — check manually"
}

pg_exec() {
  kubectl exec -n prod postgresql-0 -- psql -U postgres -c "$1" 2>/dev/null
}

# ── Service handlers ──────────────────────────────────────────────────────────

rotate_postgresql() {
  log "=== Rotating postgresql superuser password ==="
  local NEW_PASS; NEW_PASS=$(gen_password)
  local OLD_REPL; OLD_REPL=$(bao_read "secret/data/postgresql/credentials" "replication-password")

  pg_exec "ALTER ROLE postgres WITH PASSWORD '${NEW_PASS}';" \
    && ok "PostgreSQL role 'postgres' password updated"

  bao_write "secret/data/postgresql/credentials" \
    "postgres-password=${NEW_PASS}" \
    "replication-password=${OLD_REPL}"
  ok "OpenBao secret/data/postgresql/credentials updated"

  kube_secret_update postgresql-credentials prod \
    "postgres-password=${NEW_PASS}" \
    "replication-password=${OLD_REPL}"
  ok "K8s secret postgresql-credentials updated"

  roll_deployment prod postgresql
}

rotate_polaris() {
  log "=== Rotating Polaris PostgreSQL user password ==="
  local NEW_PASS; NEW_PASS=$(gen_password)

  # 1. Update the DB role first (while old pod still runs with old password)
  pg_exec "ALTER ROLE polaris WITH PASSWORD '${NEW_PASS}';" \
    && ok "PostgreSQL role 'polaris' password updated"

  # 2. Write to OpenBao (new version — old version retained)
  bao_write "secret/data/polaris/credentials" \
    "db-user=polaris" \
    "db-password=${NEW_PASS}"
  ok "OpenBao secret/data/polaris/credentials updated"

  # 3. Update K8s secret so next pod start picks it up
  kube_secret_update polaris-db-credentials prod \
    "db-user=polaris" \
    "db-password=${NEW_PASS}"
  ok "K8s secret polaris-db-credentials updated"

  # 4. Roll Polaris pod — it will restart with new password
  roll_deployment prod polaris
}

rotate_kerberos() {
  log "=== Rotating Kerberos admin password ==="
  local NEW_ADMIN; NEW_ADMIN=$(gen_password)
  local OLD_MASTER; OLD_MASTER=$(bao_read "secret/data/kerberos/credentials" "master-password")

  # Update kadmin password in the live KDC
  kubectl exec -n prod deployment/kerberos -- kadmin.local \
    -q "change_password -pw ${NEW_ADMIN} admin/admin" 2>/dev/null \
    && ok "Kerberos admin/admin password updated in live KDC" \
    || log "WARNING: could not update live KDC — update manually"

  bao_write "secret/data/kerberos/credentials" \
    "master-password=${OLD_MASTER}" \
    "admin-password=${NEW_ADMIN}" \
    "kadmin-password=${NEW_ADMIN}"
  ok "OpenBao secret/data/kerberos/credentials updated"

  kube_secret_update kerberos-admin prod \
    "master-password=${OLD_MASTER}" \
    "admin-password=${NEW_ADMIN}" \
    "kadmin-password=${NEW_ADMIN}"
  ok "K8s secret kerberos-admin updated"
  log "NOTE: Kerberos pod restart not required — kadmin password is live-updated"
}

rotate_doris() {
  log "=== Rotating Doris admin password ==="
  local NEW_PASS; NEW_PASS=$(gen_password)

  # Update via Doris HTTP API
  kubectl exec -n prod deployment/doris-fe -- \
    mysql -h 127.0.0.1 -P 9030 -u root -e \
    "ALTER USER 'admin' IDENTIFIED BY '${NEW_PASS}';" 2>/dev/null \
    && ok "Doris admin password updated" \
    || log "WARNING: could not update Doris live — update manually"

  bao_write "secret/data/doris/credentials" \
    "admin-password=${NEW_PASS}"
  ok "OpenBao secret/data/doris/credentials updated"

  kube_secret_update doris-credentials prod \
    "admin-password=${NEW_PASS}"
  ok "K8s secret doris-credentials updated"
}

rotate_grafana() {
  log "=== Rotating Grafana admin password ==="
  local NEW_PASS; NEW_PASS=$(gen_password)
  local OLD_SECRET; OLD_SECRET=$(bao_read "secret/data/grafana/credentials" "secret-key")

  # Update via Grafana API
  GRAFANA_URL="http://192.168.1.50:30300"
  OLD_PASS=$(bao_read "secret/data/grafana/credentials" "admin-password")
  curl -sf -X PUT "${GRAFANA_URL}/api/user/password" \
    -u "admin:${OLD_PASS}" \
    -H "Content-Type: application/json" \
    -d "{\"oldPassword\":\"${OLD_PASS}\",\"newPassword\":\"${NEW_PASS}\",\"confirmNew\":\"${NEW_PASS}\"}" \
    > /dev/null && ok "Grafana admin password updated via API" \
    || log "WARNING: Grafana API update failed — update manually"

  bao_write "secret/data/grafana/credentials" \
    "admin-user=admin" \
    "admin-password=${NEW_PASS}" \
    "secret-key=${OLD_SECRET}"
  ok "OpenBao secret/data/grafana/credentials updated"

  kube_secret_update grafana-credentials monitoring \
    "admin-user=admin" \
    "admin-password=${NEW_PASS}" \
    "secret-key=${OLD_SECRET}"
  ok "K8s secret grafana-credentials updated"

  roll_deployment monitoring grafana
}

rotate_opensearch() {
  log "=== Rotating OpenSearch admin password ==="
  local NEW_PASS; NEW_PASS=$(gen_password)

  bao_write "secret/data/opensearch/credentials" \
    "opensearch-user=admin" \
    "opensearch-password=${NEW_PASS}"
  ok "OpenBao secret/data/opensearch/credentials updated"

  kube_secret_update opensearch-credentials prod \
    "opensearch-user=admin" \
    "opensearch-password=${NEW_PASS}"
  ok "K8s secret opensearch-credentials updated"

  log "NOTE: OpenSearch password hash must be updated in the security config."
  log "      Run: kubectl exec -n prod opensearch-cluster-master-0 -- plugins/opensearch-security/tools/hash.sh -p '${NEW_PASS}'"
  log "      Then update internal_users.yml and apply the config."
}

# ── Dispatch ──────────────────────────────────────────────────────────────────
case "${SERVICE}" in
  postgresql)  rotate_postgresql ;;
  polaris)     rotate_polaris ;;
  kerberos)    rotate_kerberos ;;
  doris)       rotate_doris ;;
  grafana)     rotate_grafana ;;
  opensearch)  rotate_opensearch ;;
  "")
    echo "Usage: bash scripts/master/rotate-secret.sh <service>"
    echo "Supported: postgresql polaris kerberos doris grafana opensearch"
    exit 1
    ;;
  *)
    die "Unknown service '${SERVICE}'. Supported: postgresql polaris kerberos doris grafana opensearch"
    ;;
esac

echo ""
log "Rotation complete for: ${SERVICE}"
log "OpenBao retains all previous secret versions — roll back with:"
log "  bao kv rollback -mount=secret <path> <version>"
