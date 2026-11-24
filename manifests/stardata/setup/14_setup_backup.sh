#!/usr/bin/env bash
# =============================================================================
# 14_setup_backup.sh
# One-shot script: creates the Doris backup repository + backup_schedule table.
#
# Run this once before using Backup & Recovery from the StarData console.
#
# Usage
# ─────
#   bash manifests/stardata/setup/14_setup_backup.sh
#
# Or dry-run:
#   DRY_RUN=1 bash manifests/stardata/setup/14_setup_backup.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SD_HOST="${SD_HOST:-192.168.1.50}"
SD_PORT="${SD_PORT:-9030}"
SD_USER="${SD_USER:-root}"
DRY_RUN="${DRY_RUN:-0}"

green()  { printf '\033[0;32m%s\033[0m\n' "$*"; }
yellow() { printf '\033[0;33m%s\033[0m\n' "$*"; }
red()    { printf '\033[0;31m%s\033[0m\n' "$*"; }
bold()   { printf '\033[1m%s\033[0m\n'    "$*"; }

bold "=== StarData — Backup Repository + Schedule Table Setup ==="
echo ""

# ── StarData admin password ───────────────────────────────────────────────────
if [[ -z "${STARDATA_PASS:-}" ]]; then
    yellow "→ Reading STARDATA_PASS from K8s secret stardata-credentials …"
    STARDATA_PASS="$(kubectl get secret stardata-credentials -n prod \
        -o jsonpath='{.data.admin-password}' | base64 -d)"
fi
[[ -n "${STARDATA_PASS}" ]] || { red "ERROR: STARDATA_PASS is empty."; exit 1; }
green "✓ StarData credentials ready"

# ── S3 credentials ────────────────────────────────────────────────────────────
if [[ -z "${S3_ACCESS_KEY:-}" ]]; then
    yellow "→ Reading S3_ACCESS_KEY / S3_SECRET_KEY from K8s secret platform-s3-credentials …"
    S3_ACCESS_KEY="$(kubectl get secret platform-s3-credentials -n prod \
        -o jsonpath='{.data.access-key}' | base64 -d)"
    S3_SECRET_KEY="$(kubectl get secret platform-s3-credentials -n prod \
        -o jsonpath='{.data.secret-key}' | base64 -d)"
fi
[[ -n "${S3_ACCESS_KEY}" && -n "${S3_SECRET_KEY}" ]] \
    || { red "ERROR: S3_ACCESS_KEY or S3_SECRET_KEY is empty."; exit 1; }
green "✓ S3 credentials ready"

export S3_ACCESS_KEY S3_SECRET_KEY

run_sql() {
    local label="$1" file="$2"
    echo ""; bold "── ${label} ──"
    if [[ "${DRY_RUN}" == "1" ]]; then
        yellow "  [DRY RUN] Would run: envsubst < ${file} | mysql …"
        return 0
    fi
    envsubst < "${file}" \
    | mysql -h "${SD_HOST}" -P "${SD_PORT}" \
            -u "${SD_USER}" -p"${STARDATA_PASS}" \
            --default-character-set=utf8mb4 \
    && green "  ✓ ${label} applied successfully" \
    || { red "  ✗ ${label} FAILED — see output above"; exit 1; }
}

run_sql "14 — Doris S3 backup repository (sd_backup_repo)" \
        "${SCRIPT_DIR}/14_create_backup_repository.sql"

run_sql "15 — Backup schedule metadata table (cache_system.backup_schedule)" \
        "${SCRIPT_DIR}/15_create_backup_schedule_table.sql"

echo ""
bold "════════════════════════════════════════════════════════════════"
green "  Setup complete."
bold "════════════════════════════════════════════════════════════════"
echo ""
echo "  Repository : sd_backup_repo  →  s3://xdatatoiceberg1/stardata-backups/"
echo "  Schedule   : cache_system.backup_schedule  (default: off, 14-day retention)"
echo ""
echo "  Next steps:"
echo "    1. kubectl apply -f manifests/backup/stardata-autobackup-cronjob.yaml"
echo "    2. Open https://stardata.stardatadb.com and use Backup & Recovery."
echo ""
