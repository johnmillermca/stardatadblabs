#!/usr/bin/env bash
# =============================================================================
# deploy-workers.sh
# Run on the MASTER node.
# Copies and executes worker-side scripts on every worker over SSH.
#
# Usage:
#   sudo ./deploy-workers.sh [--inventory <file>] [IP:USER ...]
#
# Examples:
#   sudo bash scripts/master/deploy-workers.sh
#   sudo bash scripts/master/deploy-workers.sh --inventory /etc/my-workers.conf
#   sudo bash scripts/master/deploy-workers.sh 192.168.1.51:star_worker1 192.168.1.52:star_worker2
#
# Inventory file format (see scripts/workers.conf):
#   <IP or hostname>  <ssh-user>   # one per line; # and blank lines ignored
#
# Requirements:
#   - SSH key-based (passwordless) access as ssh-user to each worker.
#   - ssh-user must have passwordless sudo on the workers (NOPASSWD in sudoers).
#   - Worker IPs/hostnames must be reachable from the master.
# =============================================================================
set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
MASTER_IP="${MASTER_IP:-192.168.1.50}"
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEFAULT_INVENTORY="${SCRIPTS_DIR}/workers.conf"
REMOTE_TMP="/tmp/k8s-scripts"

SSH_USER="${SUDO_USER:-root}"
SSH_KEY_DIR="$(eval echo "~${SSH_USER}")/.ssh"
SSH_KEY="${SSH_KEY_DIR}/id_ed25519"
[[ -f "${SSH_KEY}" ]] || SSH_KEY="${SSH_KEY_DIR}/id_rsa"
SSH_OPTS="-o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=10 -i ${SSH_KEY} -o UserKnownHostsFile=${SSH_KEY_DIR}/known_hosts"

# ── Helpers ───────────────────────────────────────────────────────────────────
log() { echo "[$(date '+%H:%M:%S')] $*"; }
die() { echo "[ERROR] $*" >&2; exit 1; }

run_on_worker() {
  local user="$1" host="$2"; shift 2
  ssh ${SSH_OPTS} "${user}@${host}" "$@"
}

sudo_on_worker() {
  local user="$1" host="$2"; shift 2
  ssh ${SSH_OPTS} "${user}@${host}" sudo "$@"
}

copy_script_to_worker() {
  local user="$1" host="$2" script="$3"   # script is relative to SCRIPTS_DIR
  local remote_path="${REMOTE_TMP}/${script}"

  run_on_worker  "${user}" "${host}" "mkdir -p '$(dirname "${remote_path}")'"
  scp -q ${SSH_OPTS} "${SCRIPTS_DIR}/${script}" "${user}@${host}:${remote_path}"
  run_on_worker  "${user}" "${host}" "chmod +x '${remote_path}'"
}

# Parse an inventory file into parallel WORKER_IPS / WORKER_USERS arrays.
load_inventory() {
  local file="$1"
  [[ -f "${file}" ]] || die "Inventory file not found: ${file}"
  while IFS= read -r line; do
    # Strip comments and blank lines.
    line="${line%%#*}"
    [[ -z "${line// }" ]] && continue
    local ip user
    read -r ip user <<<"${line}"
    [[ -z "${ip}" || -z "${user}" ]] && { log "WARN: skipping malformed line: '${line}'"; continue; }
    WORKER_IPS+=("${ip}")
    WORKER_USERS+=("${user}")
  done < "${file}"
}

# ── Parse arguments ───────────────────────────────────────────────────────────
WORKER_IPS=()
WORKER_USERS=()
INVENTORY_FILE="${DEFAULT_INVENTORY}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --inventory|-i)
      INVENTORY_FILE="$2"; shift 2 ;;
    *:*)
      # Accept IP:USER pairs on the command line.
      WORKER_IPS+=("${1%%:*}")
      WORKER_USERS+=("${1##*:}")
      shift ;;
    *)
      die "Unknown argument: $1  (use IP:USER format or --inventory <file>)" ;;
  esac
done

# Fall back to inventory file if no CLI workers were given.
if [[ ${#WORKER_IPS[@]} -eq 0 ]]; then
  load_inventory "${INVENTORY_FILE}"
fi

[[ ${#WORKER_IPS[@]} -gt 0 ]] || die "No workers found. Check ${INVENTORY_FILE}."

# ── Main ──────────────────────────────────────────────────────────────────────
log "Master IP : ${MASTER_IP}"
log "Inventory : ${INVENTORY_FILE}"
log "Workers   :"
for i in "${!WORKER_IPS[@]}"; do
  log "  ${WORKER_IPS[$i]}  (user: ${WORKER_USERS[$i]})"
done
echo

for i in "${!WORKER_IPS[@]}"; do
  WORKER="${WORKER_IPS[$i]}"
  USER="${WORKER_USERS[$i]}"

  log "──────────────────────────────────────────────"
  log "Processing worker: ${WORKER}  (user: ${USER})"

  # Verify connectivity first.
  run_on_worker "${USER}" "${WORKER}" true \
    || { log "SKIP ${WORKER}: SSH unreachable as ${USER}"; continue; }

  # ── Step 1: common prerequisites ──────────────────────────────────────────
  log "[${WORKER}] Copying 00-common-prereqs.sh..."
  copy_script_to_worker "${USER}" "${WORKER}" "workers/00-common-prereqs.sh"

  log "[${WORKER}] Running 00-common-prereqs.sh (sudo)..."
  sudo_on_worker "${USER}" "${WORKER}" "bash '${REMOTE_TMP}/workers/00-common-prereqs.sh'" \
    && log "[${WORKER}] Prerequisites: OK" \
    || { log "[${WORKER}] Prerequisites: FAILED"; continue; }

  # ── Step 2: join the cluster ───────────────────────────────────────────────
  log "[${WORKER}] Copying 02-worker-join.sh..."
  copy_script_to_worker "${USER}" "${WORKER}" "workers/02-worker-join.sh"

  log "[${WORKER}] Running 02-worker-join.sh (sudo, master: ${MASTER_IP})..."
  sudo_on_worker "${USER}" "${WORKER}" "bash '${REMOTE_TMP}/workers/02-worker-join.sh' '${MASTER_IP}'" \
    && log "[${WORKER}] Join: OK" \
    || { log "[${WORKER}] Join: FAILED"; continue; }

  log "[${WORKER}] Done."
done

echo
log "All workers processed. Check node status with:  kubectl get nodes -o wide"
