#!/usr/bin/env bash
# =============================================================================
# 06_07_apply_storage_and_partitions.sh
# Apply hot/cold storage policies + snap_id auto-partition defaults to StarData.
#
# What this script does
# ─────────────────────
#   1. Reads the StarData admin password from the Kubernetes secret
#      "stardata-credentials" (namespace prod).
#   2. Applies 06_storage_policies.sql
#      • Creates the shared S3 storage resource  sd_s3_cold
#        pointing at s3://xdatatoiceberg1  (us-east-2)
#      • Creates 10 STORAGE POLICYs:  sd_cold_1d … sd_cold_10d
#        (cooldown_ttl 86400 s, 172800 s … 864000 s)
#   3. Applies 07_auto_partition_snap_id.sql
#      • Enables AUTO PARTITION globally (FE config)
#      • Creates cache_system.snap_id_partition_log audit table
#      • Creates the hourly Doris JOB snap_id_partition_sweep that
#        retrofits snap_id RANGE partitions on any table that lacks them
#
# Prerequisites
# ─────────────
#   • kubectl must be in PATH and have access to the prod namespace
#   • mysql client must be in PATH
#   • StarData FE must be reachable at 192.168.1.50:30090
#
# Usage
# ─────
#   bash manifests/stardata/setup/06_07_apply_storage_and_partitions.sh
#
# Or dry-run (just show the SQL without executing):
#   DRY_RUN=1 bash manifests/stardata/setup/06_07_apply_storage_and_partitions.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SD_HOST="${SD_HOST:-192.168.1.50}"
SD_PORT="${SD_PORT:-9030}"
SD_USER="${SD_USER:-root}"
DRY_RUN="${DRY_RUN:-0}"

# ── Colour helpers ────────────────────────────────────────────────────────────
green()  { printf '\033[0;32m%s\033[0m\n' "$*"; }
yellow() { printf '\033[0;33m%s\033[0m\n' "$*"; }
red()    { printf '\033[0;31m%s\033[0m\n' "$*"; }
bold()   { printf '\033[1m%s\033[0m\n'    "$*"; }

# ── Retrieve password ─────────────────────────────────────────────────────────
bold "=== StarData — Storage Policies + snap_id Auto-Partition Setup ==="
echo ""

if [[ -z "${STARDATA_PASS:-}" ]]; then
    yellow "→ Reading STARDATA_PASS from K8s secret stardata-credentials …"
    STARDATA_PASS="$(kubectl get secret stardata-credentials -n prod \
        -o jsonpath='{.data.admin-password}' | base64 -d)"
fi

if [[ -z "${STARDATA_PASS}" ]]; then
    red "ERROR: STARDATA_PASS is empty. Export it manually: export STARDATA_PASS='…'"
    exit 1
fi

green "✓ StarData credentials ready"

# ── S3 credentials (injected into 06_storage_policies.sql at apply time) ─────
if [[ -z "${S3_ACCESS_KEY:-}" ]]; then
    yellow "→ Reading S3_ACCESS_KEY / S3_SECRET_KEY from K8s secret platform-s3-credentials …"
    S3_ACCESS_KEY="$(kubectl get secret platform-s3-credentials -n prod \
        -o jsonpath='{.data.access-key}' | base64 -d)"
    S3_SECRET_KEY="$(kubectl get secret platform-s3-credentials -n prod \
        -o jsonpath='{.data.secret-key}' | base64 -d)"
fi

if [[ -z "${S3_ACCESS_KEY}" || -z "${S3_SECRET_KEY}" ]]; then
    red "ERROR: S3_ACCESS_KEY or S3_SECRET_KEY is empty."
    red "       Export them manually or ensure secret platform-s3-credentials exists in prod."
    exit 1
fi
green "✓ S3 credentials ready"

# ── MySQL helper — substitutes env vars before piping SQL to mysql ────────────
run_sql() {
    local label="$1"
    local file="$2"
    echo ""
    bold "── ${label} ──"
    if [[ "${DRY_RUN}" == "1" ]]; then
        yellow "  [DRY RUN] Would run: mysql -h ${SD_HOST} -P ${SD_PORT} < ${file} (with env substitution)"
        return 0
    fi
    # envsubst replaces ${S3_ACCESS_KEY} / ${S3_SECRET_KEY} in the SQL before
    # sending to mysql — credentials never touch the filesystem or git.
    envsubst < "${file}" \
    | mysql -h "${SD_HOST}" -P "${SD_PORT}" \
            -u "${SD_USER}" -p"${STARDATA_PASS}" \
            --default-character-set=utf8mb4 \
    && green "  ✓ ${label} applied successfully" \
    || { red "  ✗ ${label} FAILED — see output above"; exit 1; }
}

# ── Step 1: Storage policies ──────────────────────────────────────────────────
run_sql "06 — Hot/Cold Storage Policies (sd_cold_1d … sd_cold_10d)" \
        "${SCRIPT_DIR}/06_storage_policies.sql"

# ── Step 2: Auto snap_id partitions ──────────────────────────────────────────
run_sql "07 — snap_id AUTO PARTITION defaults + governance job" \
        "${SCRIPT_DIR}/07_auto_partition_snap_id.sql"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
bold "════════════════════════════════════════════════════════════════"
green "  All steps completed successfully."
bold "════════════════════════════════════════════════════════════════"
echo ""
echo "  Storage resource : sd_s3_cold  →  s3://xdatatoiceberg1/stardata-cold/"
echo "  Policies created : sd_cold_1d  sd_cold_2d  sd_cold_3d  sd_cold_4d  sd_cold_5d"
echo "                     sd_cold_6d  sd_cold_7d  sd_cold_8d  sd_cold_9d  sd_cold_10d"
echo ""
echo "  Auto-partition   : snap_id RANGE (interval 1 000 000) — AUTO PARTITION enabled"
echo "  Governance job   : snap_id_partition_sweep  (runs every 1 HOUR)"
echo "  Audit table      : cache_system.snap_id_partition_log"
echo ""
echo "  To attach a policy to an existing table:"
echo "    ALTER TABLE <db>.<tbl> SET (\"storage_policy\" = \"sd_cold_7d\");"
echo ""
echo "  To create a new auto-partitioned table:"
echo "    See SECTION E in 07_auto_partition_snap_id.sql for the full template."
echo ""
