#!/usr/bin/env bash
# =============================================================================
# create_databricks_job.sh
#
# PURPOSE
# ───────
# Creates (or idempotently updates) a Databricks Job that runs
# nb_multi_table_auto_reader on a cron schedule.
#
# The job:
#   • Runs nb_multi_table_auto_reader every 15 minutes (configurable)
#   • Uses the Serverless Starter Warehouse for SQL (no cluster spin-up cost)
#   • Runs as a Notebook task on a new-cluster (single-node, DBR 16.4 LTS)
#   • Sends an email alert on failure
#
# WHAT IT DOES
# ─────────────
#   1. Reads host + PAT from OpenBao  secret/data/platform/databricks
#   2. Uploads nb_multi_table_auto_reader.py to /Shared/stardata/
#   3. Checks if a job named JOB_NAME already exists
#      — if yes: resets (updates) it in-place
#      — if no : creates a new job
#   4. Optionally triggers one immediate run (--run-now flag)
#   5. Prints the Job URL
#
# USAGE
# ─────
#   # Create/update the job (15-min default):
#   bash scripts/databricks/create_databricks_job.sh
#
#   # Create + trigger one run immediately:
#   bash scripts/databricks/create_databricks_job.sh --run-now
#
#   # Override schedule (any valid Quartz cron):
#   CRON="0 0 * * * ?"  bash scripts/databricks/create_databricks_job.sh
#
#   # Override Databricks creds directly (skips OpenBao):
#   DB_HOST=dbc-xxx.cloud.databricks.com \
#   DB_TOKEN=dapiXXXX \
#       bash scripts/databricks/create_databricks_job.sh
#
# REQUIREMENTS
#   curl, jq, python3
# =============================================================================
set -euo pipefail

# ── Flags ─────────────────────────────────────────────────────────────────────
RUN_NOW=false
for arg in "$@"; do
    [[ "${arg}" == "--run-now" ]] && RUN_NOW=true
done

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NOTEBOOK_SRC="${REPO_ROOT}/docker/databricks-notebooks/nb_multi_table_auto_reader.py"
NOTEBOOK_WORKSPACE_PATH="/Shared/stardata/nb_multi_table_auto_reader"

# ── Job config ─────────────────────────────────────────────────────────────────
JOB_NAME="stardata-iceberg-auto-refresh"
# Quartz cron: "0 0/15 * * * ?" = every 15 minutes
# Override via env: CRON="0 0 * * * ?" for hourly, etc.
CRON="${CRON:-0 0/15 * * * ?}"
TIMEZONE="UTC"
# Notification email — leave blank to skip
NOTIFY_EMAIL="${NOTIFY_EMAIL:-}"

# ── OpenBao ───────────────────────────────────────────────────────────────────
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

[[ -f "${NOTEBOOK_SRC}" ]] || die "Notebook not found: ${NOTEBOOK_SRC}"

# ── Step 1: Resolve Databricks credentials ─────────────────────────────────────
log "=== Step 1: Resolving Databricks credentials ==="

if [[ -n "${DB_HOST:-}" && -n "${DB_TOKEN:-}" ]]; then
    log "Using DB_HOST / DB_TOKEN from environment."
else
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
        die "Cannot authenticate to OpenBao.\nSet DB_HOST + DB_TOKEN directly, or TOKEN=<bao-token>."
    fi

    SECRET_JSON=$(curl -sf \
        -H "X-Vault-Token: ${BAO_TOK}" \
        "${BAO_ADDR}/v1/${BAO_SECRET_PATH}")
    DB_HOST=$(echo  "${SECRET_JSON}" | jq -r '.data.data.host  // empty')
    DB_TOKEN=$(echo "${SECRET_JSON}" | jq -r '.data.data.token // empty')
    [[ -n "${DB_HOST}"  ]] || die "OpenBao missing key: host  (${BAO_SECRET_PATH})"
    [[ -n "${DB_TOKEN}" ]] || die "OpenBao missing key: token (${BAO_SECRET_PATH})"
fi

ok "host  = ${DB_HOST}"
ok "token = ${DB_TOKEN:0:8}...${DB_TOKEN: -4}"
DB_API="https://${DB_HOST}/api"

# ── Step 2: Upload notebook ────────────────────────────────────────────────────
log "=== Step 2: Uploading notebook to workspace ==="

NOTEBOOK_B64=$(python3 -c "
import base64, sys
with open(sys.argv[1], 'rb') as f:
    print(base64.b64encode(f.read()).decode())
" "${NOTEBOOK_SRC}")

# Create /Shared/stardata/ folder
curl -sf -X POST \
    -H "Authorization: Bearer ${DB_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"path\": \"/Shared/stardata\"}" \
    "${DB_API}/2.0/workspace/mkdirs" > /dev/null || true

IMPORT_RESP=$(curl -s -w "\n%{http_code}" -X POST \
    -H "Authorization: Bearer ${DB_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{
        \"path\":     \"${NOTEBOOK_WORKSPACE_PATH}\",
        \"language\": \"PYTHON\",
        \"format\":   \"SOURCE\",
        \"overwrite\": true,
        \"content\":  \"${NOTEBOOK_B64}\"
    }" \
    "${DB_API}/2.0/workspace/import")

IMPORT_STATUS=$(echo "${IMPORT_RESP}" | tail -1)
[[ "${IMPORT_STATUS}" == "200" ]] || \
    die "Notebook upload failed (HTTP ${IMPORT_STATUS}): $(echo "${IMPORT_RESP}" | head -n -1)"
ok "Notebook uploaded → ${NOTEBOOK_WORKSPACE_PATH}"

# ── Step 3: Build notification block ──────────────────────────────────────────
if [[ -n "${NOTIFY_EMAIL}" ]]; then
    EMAIL_BLOCK=$(jq -n \
        --arg email "${NOTIFY_EMAIL}" \
        '{"on_failure":[{"email":$email}],"on_start":[],"on_success":[]}')
else
    EMAIL_BLOCK='{"on_failure":[],"on_start":[],"on_success":[]}'
fi

# ── Step 4: Build job definition JSON ─────────────────────────────────────────
log "=== Step 3: Building job definition ==="

JOB_JSON=$(jq -n \
    --arg name       "${JOB_NAME}" \
    --arg cron       "${CRON}" \
    --arg tz         "${TIMEZONE}" \
    --arg nb_path    "${NOTEBOOK_WORKSPACE_PATH}" \
    --argjson emails "${EMAIL_BLOCK}" \
'{
  "name": $name,
  "schedule": {
    "quartz_cron_expression": $cron,
    "timezone_id": $tz,
    "pause_status": "UNPAUSED"
  },
  "tasks": [
    {
      "task_key": "auto_refresh",
      "notebook_task": {
        "notebook_path": $nb_path,
        "source": "WORKSPACE"
      },
      "environment_key": "default",
      "timeout_seconds": 1800,
      "max_retries": 1,
      "min_retry_interval_millis": 60000
    }
  ],
  "environments": [
    {
      "environment_key": "default",
      "spec": {
        "client": "1",
        "dependencies": []
      }
    }
  ],
  "email_notifications": $emails,
  "max_concurrent_runs": 1,
  "format": "MULTI_TASK"
}')

ok "Job definition built: name=${JOB_NAME}, cron=${CRON}, compute=serverless"

# ── Step 5: Create or update the job ──────────────────────────────────────────
log "=== Step 4: Creating / updating Databricks Job ==="

# Check if job with this name already exists
JOB_NAME_ENC=$(python3 -c "import urllib.parse, sys; print(urllib.parse.quote(sys.argv[1]))" "${JOB_NAME}")
LIST_RESP=$(curl -sf \
    -H "Authorization: Bearer ${DB_TOKEN}" \
    "${DB_API}/2.1/jobs/list?name=${JOB_NAME_ENC}") || LIST_RESP="{}"

EXISTING_JOB_ID=$(echo "${LIST_RESP}" | jq -r '.jobs // [] | .[0].job_id // empty')

if [[ -n "${EXISTING_JOB_ID}" ]]; then
    log "  Job '${JOB_NAME}' already exists (id=${EXISTING_JOB_ID}) - resetting in-place..."
    RESET_PAYLOAD=$(jq -n --argjson jid "${EXISTING_JOB_ID}" --argjson spec "${JOB_JSON}" \
        '{"job_id": $jid, "new_settings": $spec}')
    RESET_RESP=$(curl -s -w "\n%{http_code}" -X POST \
        -H "Authorization: Bearer ${DB_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "${RESET_PAYLOAD}" \
        "${DB_API}/2.1/jobs/reset")
    RESET_STATUS=$(echo "${RESET_RESP}" | tail -1)
    [[ "${RESET_STATUS}" == "200" ]] || \
        die "Job reset failed (HTTP ${RESET_STATUS}): $(echo "${RESET_RESP}" | head -n -1)"
    JOB_ID="${EXISTING_JOB_ID}"
    ok "Job reset (updated) - job_id=${JOB_ID}"
else
    log "  Creating new job '${JOB_NAME}'..."
    CREATE_RESP=$(curl -s -w "\n%{http_code}" -X POST \
        -H "Authorization: Bearer ${DB_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "${JOB_JSON}" \
        "${DB_API}/2.1/jobs/create")
    CREATE_BODY=$(echo "${CREATE_RESP}"   | head -n -1)
    CREATE_STATUS=$(echo "${CREATE_RESP}" | tail -1)
    [[ "${CREATE_STATUS}" == "200" ]] || \
        die "Job creation failed (HTTP ${CREATE_STATUS}): ${CREATE_BODY}"
    JOB_ID=$(echo "${CREATE_BODY}" | jq -r '.job_id')
    ok "Job created — job_id=${JOB_ID}"
fi

# ── Step 6: Optional immediate run ────────────────────────────────────────────
if [[ "${RUN_NOW}" == "true" ]]; then
    log "=== Step 5: Triggering immediate run ==="
    RUN_RESP=$(curl -s -w "\n%{http_code}" -X POST \
        -H "Authorization: Bearer ${DB_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{\"job_id\": ${JOB_ID}}" \
        "${DB_API}/2.1/jobs/run-now")
    RUN_BODY=$(echo "${RUN_RESP}"   | head -n -1)
    RUN_STATUS=$(echo "${RUN_RESP}" | tail -1)
    [[ "${RUN_STATUS}" == "200" ]] || \
        die "run-now failed (HTTP ${RUN_STATUS}): ${RUN_BODY}"
    RUN_ID=$(echo "${RUN_BODY}" | jq -r '.run_id')
    ok "Run triggered — run_id=${RUN_ID}"
    ok "Run URL: https://${DB_HOST}/#job/${JOB_ID}/run/${RUN_ID}"
fi

# ── Final summary ─────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ✅  DATABRICKS JOB READY"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  Job name  : ${JOB_NAME}"
echo "  Job ID    : ${JOB_ID}"
echo "  Schedule  : ${CRON}  (${TIMEZONE}) - every 15 min by default"
echo "  Notebook  : ${NOTEBOOK_WORKSPACE_PATH}"
echo "  Compute   : Serverless (no cluster spin-up cost)"
echo ""
echo "  📎 View job:"
echo "     https://${DB_HOST}/#job/${JOB_ID}"
echo ""
echo "  ▶ Trigger a manual run now:"
echo "     bash scripts/databricks/create_databricks_job.sh --run-now"
echo ""
echo "  ℹ️  On each run the job will:"
echo "     1. Auto-discover all Iceberg tables under s3://stardata-databricks/iceberg/warehouse/"
echo "     2. Resolve the live snapshot (manifest walk, CoW only)"
echo "     3. Write workspace.<db>.snap_<tbl>_latest  (Delta)"
echo "     4. Refresh workspace.<db>.vw_<tbl>_latest  (view)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
