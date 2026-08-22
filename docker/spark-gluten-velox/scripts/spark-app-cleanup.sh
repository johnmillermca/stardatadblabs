#!/bin/sh
# =============================================================================
# spark-app-cleanup.sh
#
# Kills all Spark applications on the standalone master that have been in
# RUNNING or WAITING state for longer than MAX_AGE_SECONDS with no live
# driver process inside the master pod.
#
# Designed to be called:
#   a) Manually:   kubectl exec -n prod $MASTER -c spark-master -- spark-app-cleanup
#   b) CronJob:    manifests/spark-app-cleanup-cronjob.yaml (every 5 minutes)
#
# Environment variables
# ---------------------
#   SPARK_MASTER_UI   Internal master UI URL  (default: http://localhost:8080)
#   MAX_AGE_SECONDS   Apps older than this are killed  (default: 60)
# =============================================================================
set -eu

SPARK_MASTER_UI="${SPARK_MASTER_UI:-http://localhost:8080}"
MAX_AGE_SECONDS="${MAX_AGE_SECONDS:-60}"

NOW_MS=$(date +%s%3N)   # current epoch in milliseconds

echo "[spark-app-cleanup] $(date -u +%Y-%m-%dT%H:%M:%SZ) — scanning for stale apps (max age=${MAX_AGE_SECONDS}s) ..."

# Fetch active app list from the Spark master REST UI
APPS=$(curl -sf "${SPARK_MASTER_UI}/json/" 2>/dev/null) || {
    echo "[spark-app-cleanup] ERROR: could not reach Spark master at ${SPARK_MASTER_UI}" >&2
    exit 1
}

# Parse IDs of apps older than MAX_AGE_SECONDS (Python always available in image)
STALE=$(echo "$APPS" | \
    _NOW_MS="$NOW_MS" \
    _MAX_AGE_MS="$((MAX_AGE_SECONDS * 1000))" \
    python3 -c "
import sys, json, os
data   = json.load(sys.stdin)
now_ms = int(os.environ['_NOW_MS'])
max_ms = int(os.environ['_MAX_AGE_MS'])
for app in data.get('activeapps', []):
    age_ms = now_ms - app.get('starttime', now_ms)
    if age_ms >= max_ms:
        print(app['id'])
")

if [ -z "$STALE" ]; then
    echo "[spark-app-cleanup] No stale apps found."
    exit 0
fi

KILLED=0
for APP_ID in $STALE; do
    echo "[spark-app-cleanup] Killing stale app: ${APP_ID}"
    curl -sf -X POST "${SPARK_MASTER_UI}/app/kill/" \
        -d "id=${APP_ID}&terminate=true" -o /dev/null \
    && echo "[spark-app-cleanup]   → killed ${APP_ID}" \
    || echo "[spark-app-cleanup]   → failed to kill ${APP_ID} (may already be gone)"
    KILLED=$((KILLED + 1))
done

echo "[spark-app-cleanup] Done — killed ${KILLED} stale app(s)."
