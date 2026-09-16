#!/usr/bin/env bash
# =============================================================================
# run_nb_pyiceberg_catalog.sh
#
# PURPOSE
# ───────
# Reads Databricks credentials (host + PAT) from OpenBao, then uploads
# nb_pyiceberg_catalog.py into the Databricks Workspace so it is ready
# to run manually from the UI.
#
# ✅ Compatible with Databricks FREE / Community Edition:
#    Only uses the Workspace API (import) — no Jobs API, no Secrets API,
#    no cluster API calls.
#
# WHAT IT DOES
# ─────────────
#   1. Reads host + PAT from OpenBao  secret/data/platform/databricks
#   2. Validates the PAT against the Databricks API
#   3. Creates /Shared/stardata/ workspace folder (idempotent)
#   4. Imports nb_pyiceberg_catalog.py → /Shared/stardata/nb_pyiceberg_catalog
#      (overwrite=true — safe to re-run on every code change)
#   5. Prints the direct notebook URL — click it to open and run
#
# USAGE
# ─────
#   # Standard — reads creds from OpenBao via root token file:
#   bash scripts/databricks/run_nb_pyiceberg_catalog.sh
#
#   # Override Databricks host + PAT directly (skips OpenBao entirely):
#   DB_HOST=dbc-xxx.cloud.databricks.com \
#   DB_TOKEN=dapiXXXX \
#       bash scripts/databricks/run_nb_pyiceberg_catalog.sh
#
#   # Use a specific OpenBao token (e.g. from CI):
#   TOKEN=<bao-token> \
#       bash scripts/databricks/run_nb_pyiceberg_catalog.sh
#
# AFTER THE SCRIPT
# ─────────────────
# 1. Click the printed notebook URL
# 2. Attach the notebook to your cluster (Compute → attach)
# 3. Run Cell 1 (%pip install) and wait for it to complete
# 4. Run Cell 2 — two text boxes appear at the top of the notebook
# 5. Paste your S3 credentials into the widgets:
#      Access Key : <AWS_ACCESS_KEY_ID>
#      Secret Key : <from OpenBao secret/data/platform/s3 → secret_key>
# 6. Run All remaining cells (Cell 3 onwards)
#
# REQUIREMENTS
# ────────────
#   curl, jq, python3
# =============================================================================
set -euo pipefail

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NOTEBOOK_SRC="${REPO_ROOT}/docker/databricks-notebooks/nb_pyiceberg_catalog.py"
NOTEBOOK_WORKSPACE_PATH="/Shared/stardata/nb_pyiceberg_catalog"

NOTEBOOK_AUTO_SRC="${REPO_ROOT}/docker/databricks-notebooks/nb_multi_table_auto_reader.py"
NOTEBOOK_AUTO_PATH="/Shared/stardata/nb_multi_table_auto_reader"

# ── Config ────────────────────────────────────────────────────────────────────
BAO_ADDR="${BAO_ADDR:-http://192.168.1.50:30820}"
BAO_SECRET_PATH="secret/data/platform/databricks"

# ── Helpers ───────────────────────────────────────────────────────────────────
log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "  ✅ $*"; }
err()  { echo "  ❌ $*" >&2; }
die()  { err "$*"; exit 1; }

require_cmd() { command -v "$1" &>/dev/null || die "Required command not found: $1"; }

require_cmd curl
require_cmd jq
require_cmd python3

[[ -f "${NOTEBOOK_SRC}" ]] || die "Notebook source not found: ${NOTEBOOK_SRC}"

# ── Step 1: Resolve Databricks credentials ────────────────────────────────────
log "=== Step 1: Resolving Databricks credentials ==="

if [[ -n "${DB_HOST:-}" && -n "${DB_TOKEN:-}" ]]; then
    log "Using DB_HOST / DB_TOKEN from environment (OpenBao skipped)."
else
    # Authenticate to OpenBao ─────────────────────────────────────────────────
    if [[ -n "${TOKEN:-}" || -n "${BAO_TOKEN:-}" ]]; then
        BAO_TOK="${TOKEN:-${BAO_TOKEN}}"
        log "Using TOKEN from environment."
    elif [[ -f "${HOME}/openbao-init-keys.json" ]]; then
        BAO_TOK=$(python3 -c \
            "import json; print(json.load(open('${HOME}/openbao-init-keys.json'))['root_token'])")
        log "Using root token from ~/openbao-init-keys.json"
    elif [[ -f /var/run/secrets/kubernetes.io/serviceaccount/token ]]; then
        K8S_JWT=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
        log "Authenticating to OpenBao via K8s Service Account JWT..."
        AUTH_RESP=$(curl -sf -X POST \
            -H "Content-Type: application/json" \
            -d "{\"role\":\"platform-secrets-read\",\"jwt\":\"${K8S_JWT}\"}" \
            "${BAO_ADDR}/v1/auth/kubernetes/login")
        BAO_TOK=$(echo "${AUTH_RESP}" | jq -r '.auth.client_token')
        [[ "${BAO_TOK}" != "null" && -n "${BAO_TOK}" ]] || \
            die "OpenBao K8s auth failed. Set TOKEN=<bao-token> as a fallback."
    else
        die "Cannot authenticate to OpenBao.\nSet DB_HOST + DB_TOKEN directly, or set TOKEN=<bao-token>."
    fi

    # Read the databricks secret ───────────────────────────────────────────────
    log "Reading credentials from OpenBao: ${BAO_ADDR}/v1/${BAO_SECRET_PATH}"
    SECRET_JSON=$(curl -sf \
        -H "X-Vault-Token: ${BAO_TOK}" \
        "${BAO_ADDR}/v1/${BAO_SECRET_PATH}")

    DB_HOST=$(echo  "${SECRET_JSON}" | jq -r '.data.data.host  // empty')
    DB_TOKEN=$(echo "${SECRET_JSON}" | jq -r '.data.data.token // empty')

    [[ -n "${DB_HOST}"  ]] || die "OpenBao secret missing key: host  (${BAO_SECRET_PATH})"
    [[ -n "${DB_TOKEN}" ]] || die "OpenBao secret missing key: token (${BAO_SECRET_PATH})"
fi

ok "host  = ${DB_HOST}"
ok "token = ${DB_TOKEN:0:8}...${DB_TOKEN: -4}"

DB_API="https://${DB_HOST}/api"

# ── Step 2: Validate PAT ──────────────────────────────────────────────────────
log "=== Step 2: Validating PAT ==="

ME_RESP=$(curl -sf \
    -H "Authorization: Bearer ${DB_TOKEN}" \
    "${DB_API}/2.0/preview/scim/v2/Me" 2>/dev/null || echo "{}")
ME=$(echo "${ME_RESP}" | jq -r '.userName // .displayName // "unknown"')
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer ${DB_TOKEN}" \
    "${DB_API}/2.0/workspace/list?path=/")

[[ "${HTTP_STATUS}" == "200" ]] || die \
    "Workspace API returned HTTP ${HTTP_STATUS}. Check that the PAT is valid and not expired."

ok "Authenticated as: ${ME}"
ok "Workspace API: HTTP ${HTTP_STATUS} ✅"

# ── Step 3: Encode notebook as base64 ─────────────────────────────────────────
log "=== Step 3: Encoding notebook ==="

NOTEBOOK_B64=$(python3 -c "
import base64, sys
with open(sys.argv[1], 'rb') as f:
    print(base64.b64encode(f.read()).decode())
" "${NOTEBOOK_SRC}")

NB_LINES=$(wc -l < "${NOTEBOOK_SRC}")
NB_B64_LEN=$(echo "${NOTEBOOK_B64}" | wc -c | tr -d ' ')
ok "Encoded ${NOTEBOOK_SRC}  (${NB_LINES} lines → ${NB_B64_LEN} base64 chars)"

# ── Step 4: Create workspace directory ────────────────────────────────────────
log "=== Step 4: Creating workspace folder ==="

PARENT_DIR=$(dirname "${NOTEBOOK_WORKSPACE_PATH}")
MKDIR_RESP=$(curl -s -w "\n%{http_code}" -X POST \
    -H "Authorization: Bearer ${DB_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"path\": \"${PARENT_DIR}\"}" \
    "${DB_API}/2.0/workspace/mkdirs")

MKDIR_STATUS=$(echo "${MKDIR_RESP}" | tail -1)
[[ "${MKDIR_STATUS}" == "200" ]] || \
    log "  mkdirs returned ${MKDIR_STATUS} (may already exist — continuing)"
ok "Workspace folder: ${PARENT_DIR}"

# ── Step 5: Import nb_pyiceberg_catalog ───────────────────────────────────────
log "=== Step 5: Importing notebook → ${NOTEBOOK_WORKSPACE_PATH} ==="

IMPORT_RESP=$(curl -s -w "\n%{http_code}" -X POST \
    -H "Authorization: Bearer ${DB_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{
        \"path\":      \"${NOTEBOOK_WORKSPACE_PATH}\",
        \"language\":  \"PYTHON\",
        \"format\":    \"SOURCE\",
        \"overwrite\":  true,
        \"content\":   \"${NOTEBOOK_B64}\"
    }" \
    "${DB_API}/2.0/workspace/import")

IMPORT_BODY=$(echo "${IMPORT_RESP}"   | head -n -1)
IMPORT_STATUS=$(echo "${IMPORT_RESP}" | tail -1)

if [[ "${IMPORT_STATUS}" != "200" ]]; then
    err "Import failed (HTTP ${IMPORT_STATUS}):"
    echo "${IMPORT_BODY}" | python3 -c "import json,sys; d=json.load(sys.stdin); print('  ', d.get('message', d))" 2>/dev/null || echo "${IMPORT_BODY}"
    exit 1
fi

ok "Notebook imported successfully (HTTP ${IMPORT_STATUS})"

# ── Step 5b: Import nb_multi_table_auto_reader ────────────────────────────────
log "=== Step 5b: Importing auto-reader → ${NOTEBOOK_AUTO_PATH} ==="

AUTO_B64=$(python3 -c "
import base64, sys
with open(sys.argv[1], 'rb') as f:
    print(base64.b64encode(f.read()).decode())
" "${NOTEBOOK_AUTO_SRC}")

AUTO_RESP=$(curl -s -w "\n%{http_code}" -X POST \
    -H "Authorization: Bearer ${DB_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{
        \"path\":      \"${NOTEBOOK_AUTO_PATH}\",
        \"language\":  \"PYTHON\",
        \"format\":    \"SOURCE\",
        \"overwrite\":  true,
        \"content\":   \"${AUTO_B64}\"
    }" \
    "${DB_API}/2.0/workspace/import")

AUTO_BODY=$(echo "${AUTO_RESP}"   | head -n -1)
AUTO_STATUS=$(echo "${AUTO_RESP}" | tail -1)

if [[ "${AUTO_STATUS}" != "200" ]]; then
    err "Auto-reader import failed (HTTP ${AUTO_STATUS}):"
    echo "${AUTO_BODY}" | python3 -c "import json,sys; d=json.load(sys.stdin); print('  ', d.get('message', d))" 2>/dev/null || echo "${AUTO_BODY}"
    exit 1
fi

ok "Auto-reader imported successfully (HTTP ${AUTO_STATUS})"

# ── Print S3 credentials reminder ─────────────────────────────────────────────
# Fetch S3 access key from OpenBao so the user knows exactly what to paste
S3_ACCESS_KEY=""
if [[ -n "${BAO_TOK:-}" ]]; then
    S3_SECRET=$(curl -sf \
        -H "X-Vault-Token: ${BAO_TOK}" \
        "${BAO_ADDR}/v1/secret/data/platform/s3" 2>/dev/null || echo "{}")
    S3_ACCESS_KEY=$(echo "${S3_SECRET}" | jq -r '.data.data.access_key // empty')
fi

# ── Final instructions ────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ✅  NOTEBOOKS UPLOADED SUCCESSFULLY"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  📎 Open notebooks:"
echo "     https://${DB_HOST}/#workspace${NOTEBOOK_WORKSPACE_PATH}"
echo "     https://${DB_HOST}/#workspace${NOTEBOOK_AUTO_PATH}"
echo ""
echo "  ▶ Steps for nb_pyiceberg_catalog (interactive catalog explorer):"
echo "     1. Click the first URL above"
echo "     2. Attach to your cluster  (Compute → attach)"
echo "        If no cluster: Compute → Create → Single Node → DBR 16.4 LTS"
echo "     3. Run Cell 1 (%pip install)  ← wait for it to finish"
echo "     4. Run Cell 2               ← two widget boxes appear at top"
echo "     5. Fill in the S3 credentials in the widget boxes:"
if [[ -n "${S3_ACCESS_KEY}" ]]; then
echo "          S3 Access Key : ${S3_ACCESS_KEY}"
echo "          S3 Secret Key : <from OpenBao secret/data/platform/s3 → secret_key>"
else
echo "          S3 Access Key : <AWS_ACCESS_KEY_ID>"
echo "          S3 Secret Key : <from OpenBao secret/data/platform/s3 → secret_key>"
fi
echo "     6. Run All (Cell 3 onwards)"
echo ""
echo "  ▶ Steps for nb_multi_table_auto_reader (all-table refresh — run-all):"
echo "     1. Click the second URL above"
echo "     2. Attach to your cluster and Run All"
echo ""
echo "  ▶ To schedule nb_multi_table_auto_reader as a Databricks Job:"
echo "     bash scripts/databricks/create_databricks_job.sh"
echo "     # Add --run-now to also trigger an immediate run"
echo ""
echo "  ℹ️  Both keys work for s3://stardata-databricks AND s3://xdatatoiceberg1"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
