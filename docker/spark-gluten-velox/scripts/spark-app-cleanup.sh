#!/bin/sh
# =============================================================================
# spark-app-cleanup.sh
#
# Kills orphaned / stalled Spark applications on the standalone master.
#
# An app is considered orphaned/stalled if it meets ANY of these conditions:
#
#   1. RUNNING + cores == 0 + age > ORPHAN_RUNNING_SECONDS  (default: 60 s)
#      The driver lost all executors (OOM-kill, eviction). The master hasn't
#      timed it out yet but it will never make progress.
#
#   2. WAITING + age > ORPHAN_WAITING_SECONDS  (default: 600 s)
#      App has been waiting for resources for 10 min — something upstream is
#      blocking resource allocation (e.g., a leaked RUNNING app eating all cores).
#
# Designed to be called:
#   a) Manually:   kubectl exec -n prod $MASTER -c spark-master -- spark-app-cleanup
#   b) CronJob:    manifests/spark-app-cleanup-cronjob.yaml (every 5 minutes)
#
# Environment variables
# ---------------------
#   SPARK_MASTER_UI           Internal master UI URL  (default: http://localhost:8080)
#   ORPHAN_RUNNING_SECONDS    Max age for RUNNING apps with 0 cores (default: 60)
#   ORPHAN_WAITING_SECONDS    Max age for WAITING apps before they are killed (default: 600)
# =============================================================================
set -eu

SPARK_MASTER_UI="${SPARK_MASTER_UI:-http://localhost:8080}"
ORPHAN_RUNNING_SECONDS="${ORPHAN_RUNNING_SECONDS:-60}"
ORPHAN_WAITING_SECONDS="${ORPHAN_WAITING_SECONDS:-600}"

NOW_MS=$(date +%s%3N)   # current epoch in milliseconds

echo "[spark-app-cleanup] $(date -u +%Y-%m-%dT%H:%M:%SZ) — scanning for orphaned apps (running_timeout=${ORPHAN_RUNNING_SECONDS}s, waiting_timeout=${ORPHAN_WAITING_SECONDS}s) ..."

# Fetch active app list from the Spark master REST UI
APPS=$(curl -sf "${SPARK_MASTER_UI}/json/" 2>/dev/null) || {
    echo "[spark-app-cleanup] ERROR: could not reach Spark master at ${SPARK_MASTER_UI}" >&2
    exit 1
}

# Detect orphans:
#   RUNNING  + cores==0 + age > ORPHAN_RUNNING_SECONDS  → driver lost all executors
#   WAITING  + age > ORPHAN_WAITING_SECONDS              → stuck waiting for resources
STALE=$(echo "$APPS" | \
    _NOW_MS="$NOW_MS" \
    _RUNNING_MS="$((ORPHAN_RUNNING_SECONDS * 1000))" \
    _WAITING_MS="$((ORPHAN_WAITING_SECONDS * 1000))" \
    python3 -c "
import sys, json, os
data       = json.load(sys.stdin)
now_ms     = int(os.environ['_NOW_MS'])
running_ms = int(os.environ['_RUNNING_MS'])
waiting_ms = int(os.environ['_WAITING_MS'])

for app in data.get('activeapps', []):
    age_ms = now_ms - app.get('starttime', now_ms)
    state  = app.get('state', '')
    cores  = app.get('cores', -1)

    if state == 'RUNNING' and cores == 0 and age_ms >= running_ms:
        print(app['id'], 'RUNNING-nocores', age_ms // 1000)
    elif state == 'WAITING' and age_ms >= waiting_ms:
        print(app['id'], 'WAITING-stalled', age_ms // 1000)
")

if [ -z "$STALE" ]; then
    echo "[spark-app-cleanup] No orphaned apps found."
    exit 0
fi

KILLED=0
echo "$STALE" | while IFS=' ' read -r APP_ID REASON AGE_S; do
    echo "[spark-app-cleanup] Killing orphan: ${APP_ID} (reason=${REASON}, age=${AGE_S}s)"
    curl -sf -X POST "${SPARK_MASTER_UI}/app/kill/" \
        -d "id=${APP_ID}&terminate=true" -o /dev/null \
    && echo "[spark-app-cleanup]   → killed ${APP_ID}" \
    || echo "[spark-app-cleanup]   → failed to kill ${APP_ID} (may already be gone)"
    KILLED=$((KILLED + 1))
done

echo "[spark-app-cleanup] Done."
