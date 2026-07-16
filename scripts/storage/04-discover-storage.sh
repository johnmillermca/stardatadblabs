#!/usr/bin/env bash
# =============================================================================
# 04-discover-storage.sh
# Discovers the largest available disk/partition on the current node and
# creates the local-path-provisioner directory.
#
# Usage:
#   # Run locally on any single node:
#   sudo bash scripts/storage/04-discover-storage.sh
#
#   # Run on master AND push to all workers automatically (from master):
#   sudo bash scripts/storage/04-discover-storage.sh --all-nodes
#   sudo bash scripts/storage/04-discover-storage.sh --all-nodes --inventory /path/to/workers.conf
# =============================================================================
set -euo pipefail

TARGET_DIR="/opt/local-path-provisioner"
RESULT_FILE="/etc/k8s-storage-path"
# Resolve script's own absolute path — works correctly under sudo bash
SCRIPT_ABS="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
SCRIPTS_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DEFAULT_INVENTORY="${SCRIPTS_DIR}/workers.conf"
REMOTE_TMP="/tmp/k8s-scripts"
# Run SSH using the invoking user's key (SUDO_USER) so ~/.ssh keys are found.
# We pass -i explicitly so root can use star_master's key without nested sudo.
SSH_USER="${SUDO_USER:-root}"
SSH_KEY_DIR="$(eval echo "~${SSH_USER}")/.ssh"
SSH_KEY="${SSH_KEY_DIR}/id_ed25519"
[[ -f "${SSH_KEY}" ]] || SSH_KEY="${SSH_KEY_DIR}/id_rsa"
SSH_OPTS="-o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=10 -i ${SSH_KEY} -o UserKnownHostsFile=${SSH_KEY_DIR}/known_hosts"
# Wrapper: run SSH/SCP directly (key is passed via -i, no nested sudo needed)
SSH_AS() { "$@"; }

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
die()  { echo "[ERROR] $*" >&2; exit 1; }

# ── Parse arguments ───────────────────────────────────────────────────────────
ALL_NODES=false
INVENTORY_FILE="${DEFAULT_INVENTORY}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --all-nodes)    ALL_NODES=true; shift ;;
    --inventory|-i) INVENTORY_FILE="$2"; shift 2 ;;
    *) die "Unknown argument: $1  (valid options: --all-nodes, --inventory <file>)" ;;
  esac
done

# ── Orchestrator mode: push and run on all workers, then run locally ──────────
if [[ "${ALL_NODES}" == true ]]; then
  [[ -f "${INVENTORY_FILE}" ]] || die "Inventory file not found: ${INVENTORY_FILE}"

  log "=== Running storage discovery on all worker nodes ==="
  log "Inventory : ${INVENTORY_FILE}"
  log "SSH as    : ${SSH_USER}"

  while IFS= read -r line <&3; do
    line="${line%%#*}"
    [[ -z "${line// }" ]] && continue
    read -r ip user <<<"${line}"
    [[ -z "${ip}" || -z "${user}" ]] && continue

    log "──────────────────────────────────────────────"
    log "Processing worker: ${ip}  (user: ${user})"

    # Each worker runs in a subshell so a failure never aborts the loop
    (
      # Verify SSH reachability — run as SSH_USER so the right keys are used
      if ! SSH_AS ssh ${SSH_OPTS} ${user}@${ip} true 2>/dev/null; then
        log "SKIP ${ip}: SSH unreachable as ${user} (checked from ${SSH_USER})"
        exit 0
      fi

      # Copy this script to the worker
      remote_script="${REMOTE_TMP}/storage/04-discover-storage.sh"
      SSH_AS ssh ${SSH_OPTS} ${user}@${ip} "mkdir -p ${REMOTE_TMP}/storage"
      SSH_AS scp -q ${SSH_OPTS} "${SCRIPT_ABS}" "${user}@${ip}:${remote_script}"
      SSH_AS ssh ${SSH_OPTS} ${user}@${ip} "chmod +x ${remote_script}"

      # Run it on the worker as sudo (no --all-nodes — local mode only)
      if SSH_AS ssh ${SSH_OPTS} ${user}@${ip} sudo bash "${remote_script}"; then
        log "[${ip}] Storage discovery: OK"
      else
        log "[${ip}] Storage discovery: FAILED"
      fi
    )

  done 3< "${INVENTORY_FILE}"

  log "──────────────────────────────────────────────"
  log "=== Running storage discovery on master (local) ==="
fi

# ── Local mode: discover storage on THIS node ─────────────────────────────────
log "Discovering largest available storage path..."

BEST_PATH=""
BEST_FREE=0

while IFS= read -r line; do
  AVAIL=$(echo "${line}" | awk '{print $4}')
  MOUNT=$(echo "${line}" | awk '{print $6}')
  # Skip root if a better option exists; skip special fs
  if [ "${AVAIL}" -gt "${BEST_FREE}" ] 2>/dev/null; then
    BEST_FREE="${AVAIL}"
    BEST_PATH="${MOUNT}"
  fi
done < <(df -k --output=fstype,size,used,avail,pcent,target 2>/dev/null \
  | grep -Ev '^(tmpfs|devtmpfs|overlay|squashfs|udev|Filesystem)' \
  | sort -k4 -rn)

if [ -z "${BEST_PATH}" ]; then
  BEST_PATH="/"
  log "Warning: could not determine best path; defaulting to /"
fi

# ── Create provisioner directory ──────────────────────────────────────────────
STORAGE_PATH="${BEST_PATH%/}/${TARGET_DIR#/}"   # e.g. /data/opt/local-path-provisioner
# If best path IS root, simplify
if [ "${BEST_PATH}" = "/" ]; then
  STORAGE_PATH="${TARGET_DIR}"
fi

log "Selected storage path: ${STORAGE_PATH} (${BEST_FREE} KB free on ${BEST_PATH})"

mkdir -p "${STORAGE_PATH}"
chmod 777 "${STORAGE_PATH}"

# Persist result for use by subsequent scripts
echo "${STORAGE_PATH}" > "${RESULT_FILE}"
log "Storage path written to ${RESULT_FILE}"

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
printf "║  Storage path: %-46s║\n" "${STORAGE_PATH}"
echo "╚══════════════════════════════════════════════════════════════╝"
