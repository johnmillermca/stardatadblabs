#!/usr/bin/env bash
# =============================================================================
# start_cdc_streaming.sh
#
# Interactive launcher for the Kafka→Iceberg CDC streaming consumer.
#
# When called without arguments the script prompts the user to choose a write
# mode (standard / soft_delete / history_tracking) before starting the stream.
# When WRITE_MODE is already set in the environment (e.g. by Kubernetes), the
# prompt is skipped and the env-var value is used directly.
#
# ── Write modes ───────────────────────────────────────────────────────────────
#
#  standard         SCD Type 0 — MERGE INTO Iceberg by PK.
#                   INSERT/UPDATE → upsert.  DELETE → hard delete.
#
#  soft_delete      MERGE upsert for INSERT/UPDATE.
#                   DELETE → set is_deleted=true, deleted_at=now().
#                   Row is never physically removed.
#
#  history_tracking Always INSERT — never UPDATE or DELETE.
#                   Each CDC event appended with _change_type + _change_ts.
#
# ── Usage ─────────────────────────────────────────────────────────────────────
#
#   # Interactive (prompts for write mode):
#   bash start_cdc_streaming.sh
#
#   # Non-interactive (mode passed as argument):
#   bash start_cdc_streaming.sh standard
#   bash start_cdc_streaming.sh soft_delete
#   bash start_cdc_streaming.sh history_tracking
#
#   # Single source (e.g. postgres only):
#   SOURCE=postgres bash start_cdc_streaming.sh soft_delete
#
#   # Dry-run (no writes to Iceberg):
#   DRY_RUN=1 bash start_cdc_streaming.sh standard
#
# ── Environment variables (all optional) ──────────────────────────────────────
#
#   WRITE_MODE            — overrides interactive prompt
#   SOURCE                — restrict to one source (postgres/oracle/mongodb)
#   DRY_RUN               — set to 1 for dry-run mode
#   SPARK_USER            — Spark principal (default: dave)
#   TRIGGER_INTERVAL      — micro-batch interval (default: "10 seconds")
#   MAX_OFFSETS_PER_TRIGGER — Kafka back-pressure (default: 50000)
#   MAX_RESTART_ATTEMPTS  — internal retry limit (default: 10)
#   ADDR                  — OpenBao address override
#
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_USER="${SPARK_USER:-dave}"

# ── Colours ───────────────────────────────────────────────────────────────────
_BOLD=$'\033[1m'
_CYAN=$'\033[36m'
_GREEN=$'\033[32m'
_YELLOW=$'\033[33m'
_RESET=$'\033[0m'

echo ""
echo "${_BOLD}${_CYAN}════════════════════════════════════════════════════════${_RESET}"
echo "${_BOLD}${_CYAN}  Kafka → Iceberg CDC Streaming Consumer${_RESET}"
echo "${_BOLD}${_CYAN}════════════════════════════════════════════════════════${_RESET}"
echo ""
echo "  Sources      : ${SOURCE:-all (postgres + oracle + mongodb)}"
echo "  Spark user   : $SPARK_USER"
echo "  Dry-run      : ${DRY_RUN:-0}"
echo ""

# ── Resolve write mode ────────────────────────────────────────────────────────
# Priority: command-line arg > WRITE_MODE env > interactive prompt

if [ $# -ge 1 ]; then
  WRITE_MODE="$1"
  echo "  Write mode   : ${_GREEN}${WRITE_MODE}${_RESET} (from argument)"
elif [ -n "${WRITE_MODE:-}" ]; then
  echo "  Write mode   : ${_GREEN}${WRITE_MODE}${_RESET} (from WRITE_MODE env)"
else
  echo "  ${_BOLD}Select a write mode:${_RESET}"
  echo ""
  echo "    ${_BOLD}1) standard${_RESET}"
  echo "       SCD Type 0 — MERGE INTO Iceberg by PK."
  echo "       INSERT/UPDATE → upsert.  DELETE → hard delete."
  echo ""
  echo "    ${_BOLD}2) soft_delete${_RESET}"
  echo "       MERGE upsert for INSERT/UPDATE."
  echo "       DELETE → mark is_deleted=true, deleted_at=now()."
  echo "       Rows are never physically removed."
  echo ""
  echo "    ${_BOLD}3) history_tracking${_RESET}"
  echo "       Always INSERT — never UPDATE or DELETE."
  echo "       Each event appended with _change_type + _change_ts."
  echo "       Preserves full row history."
  echo ""

  # Read user choice (stdin must be a terminal)
  if [ -t 0 ]; then
    while true; do
      read -rp "  Enter choice [1/2/3] or mode name: " _CHOICE
      case "${_CHOICE,,}" in
        1|standard)
          WRITE_MODE="standard"; break;;
        2|soft_delete|soft-delete)
          WRITE_MODE="soft_delete"; break;;
        3|history_tracking|history-tracking)
          WRITE_MODE="history_tracking"; break;;
        *)
          echo "  ${_YELLOW}[WARN] Invalid choice '${_CHOICE}'. Enter 1, 2, or 3.${_RESET}";;
      esac
    done
  else
    # Non-interactive stdin (e.g. piped or container without TTY) — default to standard
    echo "  ${_YELLOW}[WARN] Non-interactive terminal — defaulting to write mode: standard${_RESET}"
    WRITE_MODE="standard"
  fi

  echo ""
  echo "  Write mode   : ${_GREEN}${WRITE_MODE}${_RESET}"
fi

# ── Validate ──────────────────────────────────────────────────────────────────
case "$WRITE_MODE" in
  standard|soft_delete|history_tracking) ;;
  *)
    echo ""
    echo "ERROR: WRITE_MODE='${WRITE_MODE}' is not valid."
    echo "       Valid modes: standard, soft_delete, history_tracking"
    exit 1
    ;;
esac

echo ""
echo "${_BOLD}${_CYAN}════════════════════════════════════════════════════════${_RESET}"
echo ""
echo "  Starting streaming consumer …"
echo ""

# ── Export for the Python process ────────────────────────────────────────────
export WRITE_MODE
export SPARK_USER
export DRY_RUN="${DRY_RUN:-0}"

# ── Pre-flight: verify Python script is present ───────────────────────────────
STREAMING_SCRIPT="$SCRIPT_DIR/05_kafka_to_iceberg_streaming.py"
if [ ! -f "$STREAMING_SCRIPT" ]; then
  echo "ERROR: $STREAMING_SCRIPT not found."
  exit 1
fi

# ── Launch ────────────────────────────────────────────────────────────────────
echo "  Command: python3 $STREAMING_SCRIPT"
echo ""

exec python3 "$STREAMING_SCRIPT"
