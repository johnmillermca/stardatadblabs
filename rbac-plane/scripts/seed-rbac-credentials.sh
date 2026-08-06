#!/usr/bin/env bash
# ==============================================================
# Seed RBAC Control Plane credentials into OpenBao and create
# the Kubernetes Secret that the Deployment envFrom references.
#
# Run once after 12-seed-openbao-secrets.sh.
# ==============================================================
set -euo pipefail

NAMESPACE="prod"
BAO_ADDR="${BAO_ADDR:-http://192.168.1.50:30820}"
KEYS_FILE="${HOME}/openbao-init-keys.json"
[[ -f "$KEYS_FILE" ]] || KEYS_FILE="/root/openbao-init-keys.json"
ROOT_TOKEN=$(python3 -c "import json; print(json.load(open('${KEYS_FILE}'))['root_token'])")

export VAULT_ADDR="$BAO_ADDR"

echo "==> Generating RBAC plane credentials..."

# ── Generate random secrets ─────────────────────────────────
MASTER_TOKEN=$(python3 -c "import secrets; print(secrets.token_urlsafe(40))")
JWT_SECRET=$(python3   -c "import secrets; print(secrets.token_urlsafe(48))")
PG_PASSWORD=$(python3  -c "import secrets; print(secrets.token_urlsafe(24))")

# ── Read existing service passwords from OpenBao ────────────
DORIS_PASS=$(curl -sf -H "X-Vault-Token: ${ROOT_TOKEN}" \
  "${BAO_ADDR}/v1/secret/data/doris/credentials" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['admin-password'])")

OPENSEARCH_PASS=$(curl -sf -H "X-Vault-Token: ${ROOT_TOKEN}" \
  "${BAO_ADDR}/v1/secret/data/opensearch/credentials" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['opensearch-password'])")

KAFKA_PASS=$(kubectl get secret kafka-app-user -n "${NAMESPACE}" \
  -o jsonpath='{.data.password}' | base64 -d 2>/dev/null || echo "")

# ── Store in OpenBao ────────────────────────────────────────
echo "==> Storing rbac-plane secrets in OpenBao..."
curl -sf -X POST \
  -H "X-Vault-Token: ${ROOT_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"data\":{
    \"master-token\":       \"${MASTER_TOKEN}\",
    \"jwt-secret\":         \"${JWT_SECRET}\",
    \"pg-password\":        \"${PG_PASSWORD}\",
    \"doris-admin-pass\":   \"${DORIS_PASS}\",
    \"opensearch-admin-pass\": \"${OPENSEARCH_PASS}\",
    \"kafka-admin-pass\":   \"${KAFKA_PASS}\"
  }}" \
  "${BAO_ADDR}/v1/secret/data/rbac-plane/credentials" && echo "  ✓ OpenBao"

# ── Create PostgreSQL user + database ───────────────────────
echo "==> Creating PostgreSQL rbac database and user..."
PG_ADMIN_PASS=$(kubectl get secret postgresql-credentials -n "${NAMESPACE}" \
  -o jsonpath='{.data.postgres-password}' | base64 -d)

kubectl exec -n "${NAMESPACE}" statefulset/postgresql -- \
  env PGPASSWORD="${PG_ADMIN_PASS}" psql -U postgres <<SQL
CREATE USER rbac WITH PASSWORD '${PG_PASSWORD}';
CREATE DATABASE rbac OWNER rbac;
GRANT ALL PRIVILEGES ON DATABASE rbac TO rbac;
SQL
echo "  ✓ PostgreSQL database 'rbac' ready"

# ── Create Kubernetes Secret ────────────────────────────────
echo "==> Creating K8s Secret 'rbac-plane-credentials' in ${NAMESPACE}..."
kubectl create secret generic rbac-plane-credentials \
  --namespace "${NAMESPACE}" \
  --from-literal=PG_PASSWORD="${PG_PASSWORD}" \
  --from-literal=MASTER_TOKEN="${MASTER_TOKEN}" \
  --from-literal=JWT_SECRET="${JWT_SECRET}" \
  --from-literal=DORIS_ADMIN_PASSWORD="${DORIS_PASS}" \
  --from-literal=OPENSEARCH_ADMIN_PASSWORD="${OPENSEARCH_PASS}" \
  --from-literal=KAFKA_ADMIN_SCRAM_PASSWORD="${KAFKA_PASS}" \
  --dry-run=client -o yaml | kubectl apply -f -
echo "  ✓ K8s Secret created"

# ── Apply schema migrations ─────────────────────────────────
echo "==> Applying PostgreSQL schema migrations..."
# Run migrations from a temporary pod (avoid needing psql locally)
kubectl run rbac-migrate --rm -i --restart=Never \
  --namespace "${NAMESPACE}" \
  --image 192.168.1.50:30500/rbac-plane:1.0.0 \
  --env PG_PASSWORD="${PG_PASSWORD}" \
  --env PG_HOST="postgresql.prod.svc.cluster.local" \
  -- python3 -c "
import asyncio, asyncpg, pathlib, os
async def main():
    conn = await asyncpg.connect(
        host=os.environ['PG_HOST'],
        port=5432, database='rbac', user='rbac',
        password=os.environ['PG_PASSWORD']
    )
    for f in sorted(pathlib.Path('migrations').glob('*.sql')):
        print(f'  applying {f.name}')
        await conn.execute(f.read_text())
    await conn.close()
    print('Migrations done')
asyncio.run(main())
" && echo "  ✓ Migrations applied"

echo ""
echo "================================================================"
echo "  RBAC Control Plane credentials seeded successfully."
echo ""
echo "  Save your master token:"
echo "  RBAC_TOKEN=${MASTER_TOKEN}"
echo "  RBAC_URL=http://192.168.1.50:30850"
echo ""
echo "  Quick test:"
echo "    export RBAC_TOKEN='${MASTER_TOKEN}'"
echo "    export RBAC_URL='http://192.168.1.50:30850'"
echo "    rbacctl services"
echo "================================================================"
