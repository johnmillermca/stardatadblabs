#!/usr/bin/env bash
# =============================================================================
# register_all_connectors.sh
#
# Orchestrator: register all three Debezium CDC connectors (PostgreSQL, Oracle,
# MongoDB) using sf_extraction_ts from pipeline_watermarks as the CDC start
# position.  Delegates to the individual per-source scripts.
#
# ── Pre-condition ─────────────────────────────────────────────────────────────
# Starpump full load for ALL THREE sources must have completed successfully and
# must have written sf_extraction_ts into pipeline_watermarks before this
# script is run.
#
# ── Start position logic ──────────────────────────────────────────────────────
# PostgreSQL : snapshot.mode=never — Debezium streams from the existing
#              replication slot position (which is post-full-load LSN).
#              The watermark check below simply confirms starpump ran; the
#              actual resume point is managed by the replication slot.
# Oracle     : sf_extraction_ts → TIMESTAMP_TO_SCN() → snapshot.offset.scn
#              (handled inside register_oracle_connector.sh)
# MongoDB    : snapshot.mode=never — change-stream resume token is stored in
#              Kafka offsets topic; watermark check confirms starpump ran.
#
# Usage:
#   export SPARK_USER=dave
#   bash register_all_connectors.sh
#
#   # Register only one source:
#   bash register_all_connectors.sh postgres
#   bash register_all_connectors.sh oracle
#   bash register_all_connectors.sh mongodb
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEBEZIUM_URL="${DEBEZIUM_URL:-http://192.168.1.54:30083}"
BAO_ADDR="${BAO_ADDR:-http://openbao.prod.svc.cluster.local:8200}"

# Which sources to register (default: all three)
SOURCES=("${@:-postgres oracle mongodb}")
if [ $# -gt 0 ]; then
  SOURCES=("$@")
else
  SOURCES=(postgres oracle mongodb)
fi

echo "================================================================="
echo " Debezium CDC Connector Registration"
echo " Sources   : ${SOURCES[*]}"
echo " Connect   : $DEBEZIUM_URL"
echo " User      : ${SPARK_USER:-dave}"
echo " BAO       : $BAO_ADDR"
echo "================================================================="
echo ""

# ── Verify Debezium Connect is reachable ──────────────────────────────────────
echo "[INFO] Verifying Debezium Connect is reachable …"
for i in 1 2 3 4 5; do
  if curl -sf "$DEBEZIUM_URL/connectors" >/dev/null 2>&1; then
    echo "[OK]   Debezium Connect is up."
    break
  fi
  if [ "$i" -eq 5 ]; then
    echo "[ERROR] Debezium Connect is not reachable at $DEBEZIUM_URL after 5 attempts."
    exit 1
  fi
  echo "[WAIT] Attempt $i/5 — retrying in 10 s …"
  sleep 10
done
echo ""

# ── Verify required connector plugins are loaded ──────────────────────────────
echo "[INFO] Checking required connector plugins …"
PLUGINS=$(curl -sf "$DEBEZIUM_URL/connector-plugins" | python3 -c \
  "import sys,json; print('\n'.join(p['class'] for p in json.load(sys.stdin)))")

REQUIRED_CLASSES=(
  "io.debezium.connector.postgresql.PostgresConnector"
  "io.debezium.connector.oracle.OracleConnector"
  "io.debezium.connector.mongodb.MongoDbConnector"
)
for cls in "${REQUIRED_CLASSES[@]}"; do
  if echo "$PLUGINS" | grep -q "$cls"; then
    echo "  [OK]   $cls"
  else
    echo "  [WARN] $cls not found in connector-plugins — registration may fail."
  fi
done
echo ""

# ── Register connectors ───────────────────────────────────────────────────────
FAILED=()

for source in "${SOURCES[@]}"; do
  case "$source" in
    postgres)
      echo "─── PostgreSQL ──────────────────────────────────────────────────"
      if bash "$SCRIPT_DIR/register_postgres_connector.sh"; then
        echo "[OK]   postgres connector registered."
      else
        echo "[FAIL] postgres connector registration failed."
        FAILED+=(postgres)
      fi
      echo ""
      ;;
    oracle)
      echo "─── Oracle ──────────────────────────────────────────────────────"
      if bash "$SCRIPT_DIR/register_oracle_connector.sh"; then
        echo "[OK]   oracle connector registered."
      else
        echo "[FAIL] oracle connector registration failed."
        FAILED+=(oracle)
      fi
      echo ""
      ;;
    mongodb)
      echo "─── MongoDB ─────────────────────────────────────────────────────"
      if bash "$SCRIPT_DIR/register_mongodb_connector.sh"; then
        echo "[OK]   mongodb connector registered."
      else
        echo "[FAIL] mongodb connector registration failed."
        FAILED+=(mongodb)
      fi
      echo ""
      ;;
    *)
      echo "[ERROR] Unknown source: $source (valid: postgres oracle mongodb)"
      FAILED+=("$source")
      ;;
  esac
done

# ── Summary ───────────────────────────────────────────────────────────────────
echo "================================================================="
echo " Registration Summary"
echo "================================================================="
for source in "${SOURCES[@]}"; do
  if printf '%s\n' "${FAILED[@]}" | grep -q "^${source}$"; then
    echo "  ✗ $source — FAILED"
  else
    echo "  ✓ $source — OK"
  fi
done
echo ""

echo "[INFO] Current connector statuses:"
curl -sf "$DEBEZIUM_URL/connectors?expand=status" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for name, info in data.items():
    state = info.get('status',{}).get('connector',{}).get('state','UNKNOWN')
    tasks = info.get('status',{}).get('tasks',[])
    task_states = [t.get('state','?') for t in tasks]
    print(f'  {name}: connector={state}  tasks={task_states}')
" 2>/dev/null || curl -sf "$DEBEZIUM_URL/connectors" | python3 -m json.tool

if [ ${#FAILED[@]} -gt 0 ]; then
  echo ""
  echo "[ERROR] ${#FAILED[@]} connector(s) failed to register: ${FAILED[*]}"
  exit 1
fi

echo ""
echo "[DONE] All connectors registered successfully."
