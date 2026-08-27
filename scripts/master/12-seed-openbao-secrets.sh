#!/usr/bin/env bash
# =============================================================================
# 12-seed-openbao-secrets.sh
# Seeds all product secrets into OpenBao KV v2 and creates matching
# Kubernetes Secrets in the correct namespaces so Helm/manifests can use them.
#
# Usage: sudo bash scripts/master/12-seed-openbao-secrets.sh
# Safe to re-run — skips secrets that already exist in OpenBao.
#
# Secrets managed:
#   postgresql-credentials      → prod
#   mongodb-credentials         → prod
#   oracle-credentials          → prod
#   kafka-credentials           → prod
#   akhq-credentials            → prod
#   debezium-credentials        → prod
#   schema-registry-credentials → prod
#   opensearch-credentials      → prod
#   kerberos-admin              → prod
#   polaris-db-credentials      → prod
#   doris-credentials           → prod
#   jupyterhub-credentials      → prod
#   kestra-credentials          → prod
#   sqlmesh-credentials         → prod
#   hive-metastore-credentials  → prod
#   grafana-credentials         → monitoring
#   prometheus-credentials      → monitoring
# =============================================================================
set -euo pipefail
export PATH="/usr/local/bin:${PATH}"

BAO_ADDR="http://192.168.1.50:30820"
KEYS_FILE="${HOME}/openbao-init-keys.json"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "  ✓ $*"; }
skip() { echo "  – $* (already exists — skipping)"; }
die()  { echo "[ERROR] $*" >&2; exit 1; }

[[ -f "${KEYS_FILE}" ]] || die "OpenBao keys file not found: ${KEYS_FILE}. Run 10-deploy-openbao.sh first."

# ── Read root token ───────────────────────────────────────────────────────────
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('${KEYS_FILE}'))['root_token'])")
export BAO_TOKEN="${ROOT_TOKEN}"
export BAO_ADDR

# ── Helpers ───────────────────────────────────────────────────────────────────
gen_password() {
  openssl rand -base64 24 | tr -dc 'A-Za-z0-9' | head -c 24
}

bao_secret_exists() {
  local path="$1"
  curl -sf -H "X-Vault-Token: ${ROOT_TOKEN}" "${BAO_ADDR}/v1/${path}" -o /dev/null 2>/dev/null
}

bao_write() {
  # bao_write "secret/data/path" key=value [key=value ...]
  local path="$1"; shift
  local json="{"
  local sep=""
  for kv in "$@"; do
    local k="${kv%%=*}"
    local v="${kv#*=}"
    # Escape double quotes in value
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

kube_secret() {
  # kube_secret <name> <namespace> key=value [key=value ...]
  local name="$1" ns="$2"; shift 2
  local literals=()
  for kv in "$@"; do literals+=("--from-literal=${kv}"); done
  kubectl create secret generic "${name}" -n "${ns}" \
    "${literals[@]}" \
    --dry-run=client -o yaml | kubectl apply -f -
}

ensure_ns() {
  kubectl create namespace "$1" --dry-run=client -o yaml | kubectl apply -f - 2>/dev/null || true
}

# ── Seed secrets ──────────────────────────────────────────────────────────────
log "=== Seeding OpenBao KV + Kubernetes Secrets ==="
echo ""

# ─── 1. PostgreSQL ────────────────────────────────────────────────────────────
ensure_ns prod
if bao_secret_exists "secret/data/postgresql/credentials"; then
  skip "postgresql-credentials"
else
  PG_PASS=$(gen_password)
  PG_REPL=$(gen_password)
  bao_write "secret/data/postgresql/credentials" \
    "postgres-password=${PG_PASS}" \
    "replication-password=${PG_REPL}"
  kube_secret postgresql-credentials prod \
    "postgres-password=${PG_PASS}" \
    "replication-password=${PG_REPL}"
  ok "postgresql-credentials"
fi

# ─── 2. MongoDB ───────────────────────────────────────────────────────────────
if bao_secret_exists "secret/data/mongodb/credentials"; then
  skip "mongodb-credentials"
else
  MONGO_PASS=$(gen_password)
  bao_write "secret/data/mongodb/credentials" \
    "mongodb-root-password=${MONGO_PASS}"
  kube_secret mongodb-credentials prod \
    "mongodb-root-password=${MONGO_PASS}"
  ok "mongodb-credentials"
fi

# ─── 3. Oracle ────────────────────────────────────────────────────────────────
if bao_secret_exists "secret/data/oracle/credentials"; then
  skip "oracle-credentials"
else
  ORA_PASS=$(gen_password)
  bao_write "secret/data/oracle/credentials" \
    "oracle-password=${ORA_PASS}"
  kube_secret oracle-credentials prod \
    "oracle-password=${ORA_PASS}"
  ok "oracle-credentials"
fi

# ─── 4. Kafka (bitnami) ───────────────────────────────────────────────────────
ensure_ns prod
if bao_secret_exists "secret/data/kafka/credentials"; then
  skip "kafka-credentials"
else
  KAFKA_PASS=$(gen_password)
  bao_write "secret/data/kafka/credentials" \
    "kafka-password=${KAFKA_PASS}" \
    "kafka-user=kafka-user"
  kube_secret kafka-credentials prod \
    "kafka-password=${KAFKA_PASS}" \
    "kafka-user=kafka-user"
  ok "kafka-credentials"
fi

# ─── 5. AKHQ ─────────────────────────────────────────────────────────────────
if bao_secret_exists "secret/data/akhq/credentials"; then
  skip "akhq-credentials"
else
  AKHQ_PASS=$(gen_password)
  bao_write "secret/data/akhq/credentials" \
    "kafka-sasl-username=kafka-app-user" \
    "kafka-sasl-password=${AKHQ_PASS}" \
    "admin-password=${AKHQ_PASS}"
  kube_secret akhq-credentials prod \
    "kafka-sasl-username=kafka-app-user" \
    "kafka-sasl-password=${AKHQ_PASS}" \
    "admin-password=${AKHQ_PASS}"
  ok "akhq-credentials"
fi

# ─── 6. Debezium ──────────────────────────────────────────────────────────────
if bao_secret_exists "secret/data/debezium/credentials"; then
  skip "debezium-credentials"
else
  DBZ_KAFKA=$(gen_password)
  DBZ_PG=$(gen_password)
  DBZ_MONGO=$(gen_password)
  DBZ_ORA=$(gen_password)
  bao_write "secret/data/debezium/credentials" \
    "kafka-sasl-username=debezium-user" \
    "kafka-sasl-password=${DBZ_KAFKA}" \
    "pg-password=${DBZ_PG}" \
    "mongo-password=${DBZ_MONGO}" \
    "oracle-password=${DBZ_ORA}"
  kube_secret debezium-credentials prod \
    "kafka-sasl-username=debezium-user" \
    "kafka-sasl-password=${DBZ_KAFKA}" \
    "pg-password=${DBZ_PG}" \
    "mongo-password=${DBZ_MONGO}" \
    "oracle-password=${DBZ_ORA}"
  ok "debezium-credentials"
fi

# ─── 7. Schema Registry ───────────────────────────────────────────────────────
# NOTE: schema-registry reads its SCRAM password directly from the
# Strimzi-managed KafkaUser secret (schema-registry-user, key: password).
# Do NOT seed a separate schema-registry-credentials secret here — Strimzi
# generates and owns that password, and any value written here would diverge
# from Kafka's SCRAM store after the first KafkaUser reconciliation.
log "schema-registry-credentials: managed by Strimzi KafkaUser — skipping."

# ─── 8. OpenSearch ────────────────────────────────────────────────────────────
ensure_ns prod
if bao_secret_exists "secret/data/opensearch/credentials"; then
  skip "opensearch-credentials"
else
  OS_PASS=$(gen_password)
  bao_write "secret/data/opensearch/credentials" \
    "opensearch-password=${OS_PASS}" \
    "opensearch-user=admin"
  kube_secret opensearch-credentials prod \
    "opensearch-password=${OS_PASS}" \
    "opensearch-user=admin"
  ok "opensearch-credentials"
fi

# ─── 9. Kerberos ──────────────────────────────────────────────────────────────
ensure_ns prod
if bao_secret_exists "secret/data/kerberos/credentials"; then
  skip "kerberos-admin"
else
  KRB_MASTER=$(gen_password)
  KRB_ADMIN=$(gen_password)
  bao_write "secret/data/kerberos/credentials" \
    "master-password=${KRB_MASTER}" \
    "admin-password=${KRB_ADMIN}" \
    "kadmin-password=${KRB_ADMIN}"
  kube_secret kerberos-admin prod \
    "master-password=${KRB_MASTER}" \
    "admin-password=${KRB_ADMIN}" \
    "kadmin-password=${KRB_ADMIN}"
  ok "kerberos-admin"
fi

# ─── 10. Apache Polaris — DB credentials ─────────────────────────────────────
ensure_ns prod
if bao_secret_exists "secret/data/polaris/credentials"; then
  skip "polaris-db-credentials"
else
  POL_DB=$(gen_password)
  bao_write "secret/data/polaris/credentials" \
    "db-user=polaris" \
    "db-password=${POL_DB}"
  kube_secret polaris-db-credentials prod \
    "db-user=polaris" \
    "db-password=${POL_DB}"
  ok "polaris-db-credentials"
fi

# ─── 11b. Hive Metastore ──────────────────────────────────────────────────────
ensure_ns prod
if bao_secret_exists "secret/data/hive/credentials"; then
  skip "hive-metastore-credentials"
else
  HIVE_PASS=$(gen_password)
  AWS_KEY=$(curl -sf -H "X-Vault-Token: ${ROOT_TOKEN}" "${BAO_ADDR}/v1/secret/data/platform/s3" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['access_key'])")
  AWS_SEC=$(curl -sf -H "X-Vault-Token: ${ROOT_TOKEN}" "${BAO_ADDR}/v1/secret/data/platform/s3" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['secret_key'])")
  bao_write "secret/data/hive/credentials" \
    "db-user=hive" \
    "db-password=${HIVE_PASS}" \
    "jdbc-url=jdbc:postgresql://postgresql.prod.svc.cluster.local:5432/hive_metastore" \
    "aws-access-key=${AWS_KEY}" \
    "aws-secret-key=${AWS_SEC}"
  kube_secret hive-metastore-credentials prod \
    "db-password=${HIVE_PASS}" \
    "aws-access-key=${AWS_KEY}" \
    "aws-secret-key=${AWS_SEC}"
  ok "hive-metastore-credentials"
fi

# ─── 11c. Apache Polaris — bootstrap (root catalog) credentials ───────────────
# Polaris reads POLARIS_BOOTSTRAP_CREDENTIALS env var (System.getenv).
# Format: clientId:clientSecret  (16-hex-char ID, 32-hex-char secret).
# Without this, Polaris regenerates random credentials every restart.
if bao_secret_exists "secret/data/polaris/bootstrap"; then
  skip "polaris-bootstrap-credentials"
else
  POL_CLIENT_ID=$(openssl rand -hex 8)
  POL_CLIENT_SECRET=$(openssl rand -hex 16)
  bao_write "secret/data/polaris/bootstrap" \
    "credentials=${POL_CLIENT_ID}:${POL_CLIENT_SECRET}"
  kube_secret polaris-bootstrap-credentials prod \
    "credentials=${POL_CLIENT_ID}:${POL_CLIENT_SECRET}"
  ok "polaris-bootstrap-credentials"
fi

# ─── 12. Apache Doris ─────────────────────────────────────────────────────────
ensure_ns prod
if bao_secret_exists "secret/data/doris/credentials"; then
  skip "doris-credentials"
else
  DORIS_PASS=$(gen_password)
  bao_write "secret/data/doris/credentials" \
    "admin-password=${DORIS_PASS}"
  kube_secret doris-credentials prod \
    "admin-password=${DORIS_PASS}"
  ok "doris-credentials"
fi

# ─── 13. JupyterHub ───────────────────────────────────────────────────────────
if bao_secret_exists "secret/data/jupyterhub/credentials"; then
  skip "jupyterhub-credentials"
else
  JH_PASS=$(gen_password)
  JH_CRYPT=$(gen_password)$(gen_password)
  bao_write "secret/data/jupyterhub/credentials" \
    "admin-password=${JH_PASS}" \
    "crypt-key=${JH_CRYPT}"
  kube_secret jupyterhub-credentials prod \
    "admin-password=${JH_PASS}" \
    "crypt-key=${JH_CRYPT}"
  ok "jupyterhub-credentials"
fi

# ─── 14. Kestra ───────────────────────────────────────────────────────────────
ensure_ns prod
if bao_secret_exists "secret/data/kestra/credentials"; then
  skip "kestra-credentials"
else
  KESTRA_DB=$(gen_password)
  KESTRA_ENC=$(gen_password)$(gen_password)
  bao_write "secret/data/kestra/credentials" \
    "db-user=kestra" \
    "db-password=${KESTRA_DB}" \
    "kafka-user=kafka-user" \
    "kafka-password=$(gen_password)" \
    "encryption-key=${KESTRA_ENC}"
  kube_secret kestra-credentials prod \
    "db-user=kestra" \
    "db-password=${KESTRA_DB}" \
    "kafka-user=kafka-user" \
    "kafka-password=$(gen_password)" \
    "encryption-key=${KESTRA_ENC}"
  ok "kestra-credentials"
fi

# ─── 15. SQLMesh ──────────────────────────────────────────────────────────────
if bao_secret_exists "secret/data/sqlmesh/credentials"; then
  skip "sqlmesh-credentials"
else
  SM_DB=$(gen_password)
  bao_write "secret/data/sqlmesh/credentials" \
    "db-user=sqlmesh" \
    "db-password=${SM_DB}"
  kube_secret sqlmesh-credentials prod \
    "db-user=sqlmesh" \
    "db-password=${SM_DB}"
  ok "sqlmesh-credentials"
fi

# ─── 16. Grafana ──────────────────────────────────────────────────────────────
ensure_ns monitoring
if bao_secret_exists "secret/data/grafana/credentials"; then
  skip "grafana-credentials"
else
  GF_ADMIN_PASS=$(gen_password)
  GF_SECRET_KEY=$(gen_password)$(gen_password)
  bao_write "secret/data/grafana/credentials" \
    "admin-user=admin" \
    "admin-password=${GF_ADMIN_PASS}" \
    "secret-key=${GF_SECRET_KEY}"
  kube_secret grafana-credentials monitoring \
    "admin-user=admin" \
    "admin-password=${GF_ADMIN_PASS}" \
    "secret-key=${GF_SECRET_KEY}"
  ok "grafana-credentials"
fi

# ─── 17. Prometheus (basic-auth for external scrapers) ───────────────────────
if bao_secret_exists "secret/data/prometheus/credentials"; then
  skip "prometheus-credentials"
else
  PROM_PASS=$(gen_password)
  bao_write "secret/data/prometheus/credentials" \
    "remote-write-password=${PROM_PASS}" \
    "remote-write-user=prometheus-rw"
  kube_secret prometheus-credentials monitoring \
    "remote-write-password=${PROM_PASS}" \
    "remote-write-user=prometheus-rw"
  ok "prometheus-credentials"
fi

# ─── 17. RBAC Users (Kerberos principals) ─────────────────────────────────────
# One password per RBAC persona, stored in OpenBao and read by:
#   - 15-seed-rbac-principals.sh (creates Kerberos principals)
ensure_ns prod
if bao_secret_exists "secret/data/rbac/users"; then
  skip "rbac-users"
else
  PA_PASS=$(gen_password)
  CA_PASS=$(gen_password)
  CD_PASS=$(gen_password)
  PRA_PASS=$(gen_password)
  PRD_PASS=$(gen_password)
  SA_PASS=$(gen_password)
  SD_PASS=$(gen_password)
  bao_write "secret/data/rbac/users" \
    "platform_admin-password=${PA_PASS}" \
    "caching_admin_user-password=${CA_PASS}" \
    "caching_dev_user-password=${CD_PASS}" \
    "processing_admin_user-password=${PRA_PASS}" \
    "processing_dev_user-password=${PRD_PASS}" \
    "streaming_admin_user-password=${SA_PASS}" \
    "streaming_dev_user-password=${SD_PASS}"
  kube_secret rbac-users prod \
    "platform_admin-password=${PA_PASS}" \
    "caching_admin_user-password=${CA_PASS}" \
    "caching_dev_user-password=${CD_PASS}" \
    "processing_admin_user-password=${PRA_PASS}" \
    "processing_dev_user-password=${PRD_PASS}" \
    "streaming_admin_user-password=${SA_PASS}" \
    "streaming_dev_user-password=${SD_PASS}"
  ok "rbac-users"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
printf "  %-38s %-15s  %s\n" "K8S SECRET" "NAMESPACE" "OPENBAO PATH"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
printf "  %-38s %-15s  %s\n" "postgresql-credentials"       "prod"            "secret/data/postgresql/credentials"
printf "  %-38s %-15s  %s\n" "mongodb-credentials"          "prod"            "secret/data/mongodb/credentials"
printf "  %-38s %-15s  %s\n" "oracle-credentials"           "prod"            "secret/data/oracle/credentials"
printf "  %-38s %-15s  %s\n" "kafka-credentials"            "prod"            "secret/data/kafka/credentials"
printf "  %-38s %-15s  %s\n" "akhq-credentials"             "prod"            "secret/data/akhq/credentials"
printf "  %-38s %-15s  %s\n" "debezium-credentials"         "prod"            "secret/data/debezium/credentials"
printf "  %-38s %-15s  %s\n" "schema-registry-credentials"  "prod"            "secret/data/schema-registry/credentials"
printf "  %-38s %-15s  %s\n" "opensearch-credentials"       "prod"            "secret/data/opensearch/credentials"
printf "  %-38s %-15s  %s\n" "kerberos-admin"               "prod"            "secret/data/kerberos/credentials"
printf "  %-38s %-15s  %s\n" "polaris-db-credentials"        "prod"            "secret/data/polaris/credentials"
printf "  %-38s %-15s  %s\n" "polaris-bootstrap-credentials"  "prod"            "secret/data/polaris/bootstrap"
printf "  %-38s %-15s  %s\n" "hive-metastore-credentials"    "prod"            "secret/data/hive/credentials"
printf "  %-38s %-15s  %s\n" "doris-credentials"            "prod"            "secret/data/doris/credentials"
printf "  %-38s %-15s  %s\n" "jupyterhub-credentials"       "prod"            "secret/data/jupyterhub/credentials"
printf "  %-38s %-15s  %s\n" "kestra-credentials"           "prod"            "secret/data/kestra/credentials"
printf "  %-38s %-15s  %s\n" "sqlmesh-credentials"          "prod"            "secret/data/sqlmesh/credentials"
printf "  %-38s %-15s  %s\n" "grafana-credentials"          "monitoring"      "secret/data/grafana/credentials"
printf "  %-38s %-15s  %s\n" "prometheus-credentials"       "monitoring"      "secret/data/prometheus/credentials"
printf "  %-38s %-15s  %s\n" "rbac-users"                   "prod"            "secret/data/rbac/users"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
log "Secret seeding complete. You may now apply ArgoCD apps."
log "Next: sudo bash scripts/master/15-seed-rbac-principals.sh"
