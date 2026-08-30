#!/bin/sh
# =============================================================================
# spark-app-cleanup.sh
#
# Kills orphaned / stalled Spark applications on the standalone master.
#
# ROOT CAUSE this solves
# ----------------------
# Spark standalone has no server-side idle timeout for drivers.
# When a JupyterHub user runs a notebook, creates a SparkSession, and closes
# the browser tab, the SparkContext stays alive indefinitely. Executors remain
# registered and hold ALL cluster cores — every spark-submit queues at cores=0.
#
# Three rules — evaluated in order, ALL must pass before anything is killed
# -------------------------------------------------------------------------
#
#  Rule A — Dead driver (kill immediately, any age):
#    RUNNING + driver UI unreachable (pod crashed / kernel OOM-killed)
#    → The master hasn't evicted it yet but it will never respond.
#    → Safe to kill: driver is provably gone.
#
#  Rule B — Executor loss (kill after ORPHAN_RUNNING_SECONDS = 120 s):
#    RUNNING + cores == 0 + age > 120 s
#    → Driver lost all executors. Will never make progress.
#    → Safe to kill: no work is running.
#
#  Rule C — Resource starvation (kill after ORPHAN_WAITING_SECONDS = 600 s):
#    WAITING + age > 600 s
#    → App has been waiting 10+ min with no cores allocated.
#    → Safe to kill: something upstream is blocking allocation.
#
#  Rule D — Idle session (kill after IDLE_SINCE_SECONDS = 7200 s):
#    RUNNING + cores > 0
#    + driver UI reachable (kernel is alive)
#    + time since the LAST COMPLETED JOB > IDLE_SINCE_SECONDS
#    → The app is holding executors but hasn't done any work for 2 h.
#
#    WHY THIS IS SAFE FOR REAL JOBS:
#    A real long-running job (Snowflake copy, starpump, seed script) submits
#    Spark jobs continuously. Its "last completed job" timestamp is always
#    recent — within the last few minutes at most.
#    An idle JupyterHub kernel has no completed jobs, or its last job finished
#    hours ago when the user ran their last notebook cell.
#    The IDLE_SINCE_SECONDS threshold (default 2 h) creates a large safety
#    margin — a real job would have to go 2 h with zero Spark job submissions
#    to be considered idle. That cannot happen for any running pipeline.
#
#    ADDITIONAL SAFETY: if the driver has ACTIVE stages right now the app is
#    unconditionally skipped, even if last-job-time looks stale.
#
# Usage
# -----
#   # Manual one-shot:
#   SPARK_POD=$(kubectl get pods -n prod -l app=spark,component=master \
#     --no-headers -o custom-columns=NAME:.metadata.name | head -1)
#   kubectl exec -n prod $SPARK_POD -c spark-master -- \
#     /opt/spark/work-dir/spark-app-cleanup.sh
#
#   # Automatic: CronJob in manifests/spark-app-cleanup-cronjob.yaml (every 10 min)
#
# Environment variables (all optional)
# --------------------------------------
#   SPARK_MASTER_UI         default: http://localhost:8080
#   IDLE_SINCE_SECONDS      Rule D idle threshold   default: 300 (5 min)
#   ORPHAN_RUNNING_SECONDS  Rule B cores=0 timeout  default: 120 s
#   ORPHAN_WAITING_SECONDS  Rule C waiting timeout  default: 600 s
# =============================================================================
set -eu

SPARK_MASTER_UI="${SPARK_MASTER_UI:-http://localhost:8080}"
IDLE_SINCE_SECONDS="${IDLE_SINCE_SECONDS:-1800}"
ORPHAN_RUNNING_SECONDS="${ORPHAN_RUNNING_SECONDS:-120}"
ORPHAN_WAITING_SECONDS="${ORPHAN_WAITING_SECONDS:-600}"

NOW_MS=$(date +%s%3N)
NOW_S=$(( NOW_MS / 1000 ))

echo "[cleanup] $(date -u +%Y-%m-%dT%H:%M:%SZ)  idle_since=${IDLE_SINCE_SECONDS}s  orphan_running=${ORPHAN_RUNNING_SECONDS}s  waiting=${ORPHAN_WAITING_SECONDS}s"

APPS=$(curl -sf "${SPARK_MASTER_UI}/json/" 2>/dev/null) || {
    echo "[cleanup] ERROR: cannot reach Spark master at ${SPARK_MASTER_UI}" >&2
    exit 1
}

# Extract one line per active app: ID STATE CORES START_MS APPUIURL NAME
ACTIVE=$(echo "$APPS" | python3 -c "
import sys, json
for a in json.load(sys.stdin).get('activeapps', []):
    ui  = a.get('appuiurl', '')
    # Name may contain spaces — put it last, join rest as tab-safe single token
    name = a.get('name','unknown').replace(' ', '_')
    print(a['id'], a.get('state','?'), int(a.get('cores',0)),
          int(a.get('starttime',0)), ui, name)
" 2>/dev/null)

if [ -z "$ACTIVE" ]; then
    echo "[cleanup] No active apps."
    exit 0
fi

# ─────────────────────────────────────────────────────────────────────────────
# Helper: probe the driver REST API, return key metrics as "field=value" lines
# Prints to stdout:
#   reachable=1|0
#   active_stages=N
#   last_job_s=EPOCH_SECONDS   (0 if no jobs ever)
# ─────────────────────────────────────────────────────────────────────────────
driver_metrics() {
    DURL="$1"
    # Extract base URL:  http://10.244.x.x:4040
    BASE=$(echo "$DURL" | python3 -c "
import sys, urllib.parse
p = urllib.parse.urlparse(sys.stdin.read().strip())
print(f'{p.scheme}://{p.netloc}')
" 2>/dev/null) || { echo "reachable=0"; return; }

    # List applications on driver UI to get app id
    APPLIST=$(curl -sf --max-time 5 "${BASE}/api/v1/applications" 2>/dev/null) || {
        echo "reachable=0"; return
    }
    APP_ID_L=$(echo "$APPLIST" | python3 -c "
import sys,json
apps=json.load(sys.stdin)
print(apps[0]['id'] if apps else '')
" 2>/dev/null) || APP_ID_L=""

    if [ -z "$APP_ID_L" ]; then
        echo "reachable=0"; return
    fi

    echo "reachable=1"

    # Active stages right now
    ACTIVE_ST=$(curl -sf --max-time 5 \
        "${BASE}/api/v1/applications/${APP_ID_L}/stages?status=active" \
        2>/dev/null | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" \
        2>/dev/null) || ACTIVE_ST=0
    echo "active_stages=${ACTIVE_ST}"

    # Time of last completed job (submissionTime in ISO-8601 from /jobs)
    LAST_JOB_S=$(curl -sf --max-time 5 \
        "${BASE}/api/v1/applications/${APP_ID_L}/jobs" \
        2>/dev/null | python3 -c "
import sys, json
from datetime import datetime, timezone
jobs = json.load(sys.stdin)
# completionTime is ISO-8601 like '2026-08-30T02:14:31.000GMT'
latest = 0
for j in jobs:
    t = j.get('completionTime') or j.get('submissionTime') or ''
    if not t:
        continue
    # normalise GMT/UTC suffix
    t = t.replace('GMT','').replace('UTC','').strip()
    try:
        dt = datetime.fromisoformat(t)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        epoch = int(dt.timestamp())
        if epoch > latest:
            latest = epoch
    except Exception:
        pass
print(latest)
" 2>/dev/null) || LAST_JOB_S=0
    echo "last_job_s=${LAST_JOB_S}"
}

# ─────────────────────────────────────────────────────────────────────────────
echo "$ACTIVE" | while IFS=' ' read -r APP_ID STATE CORES START_MS DRIVER_URL NAME; do
    AGE_S=$(( ( NOW_MS - START_MS ) / 1000 ))
    KILL_REASON=""

    # ── Rule A: dead driver ────────────────────────────────────────────────────
    # Only probe after ORPHAN_RUNNING_SECONDS (120 s) — a brand-new session's
    # driver UI takes a few seconds to start; probing too early gives a false
    # "unreachable" and kills a healthy starting job.
    if [ "$STATE" = "RUNNING" ] && [ "$CORES" -gt 0 ] \
       && [ "$AGE_S" -gt "$ORPHAN_RUNNING_SECONDS" ]; then
        REACH=$(curl -sf --max-time 5 "${DRIVER_URL}" -o /dev/null -w "%{http_code}" 2>/dev/null) || REACH="0"
        if [ "$REACH" = "0" ] || [ "$REACH" = "000" ]; then
            KILL_REASON="dead-driver (UI unreachable at ${DRIVER_URL}) age=${AGE_S}s"
        fi
    fi

    # ── Rule B: executor loss ─────────────────────────────────────────────────
    if [ -z "$KILL_REASON" ] && [ "$STATE" = "RUNNING" ] \
       && [ "$CORES" -eq 0 ] && [ "$AGE_S" -gt "$ORPHAN_RUNNING_SECONDS" ]; then
        KILL_REASON="no-executors age=${AGE_S}s > ${ORPHAN_RUNNING_SECONDS}s"
    fi

    # ── Rule C: resource starvation ───────────────────────────────────────────
    if [ -z "$KILL_REASON" ] && [ "$STATE" = "WAITING" ] \
       && [ "$AGE_S" -gt "$ORPHAN_WAITING_SECONDS" ]; then
        KILL_REASON="waiting-stalled age=${AGE_S}s > ${ORPHAN_WAITING_SECONDS}s"
    fi

    # ── Rule D: idle session — only for old RUNNING apps with cores ───────────
    # Pre-check active stages BEFORE calling driver_metrics — if there are
    # active stages right now the app is definitely working; skip entirely.
    # This covers executor cold-start (tasks queued but not yet completed)
    # and long S3 reads where no job has completed yet.
    if [ -z "$KILL_REASON" ] && [ "$STATE" = "RUNNING" ] \
       && [ "$CORES" -gt 0 ] && [ "$AGE_S" -gt "$IDLE_SINCE_SECONDS" ]; then

        METRICS=$(driver_metrics "$DRIVER_URL")

        REACHABLE=$(echo "$METRICS" | grep '^reachable=' | cut -d= -f2)
        ACT_STAGES=$(echo "$METRICS" | grep '^active_stages=' | cut -d= -f2)
        LAST_JOB=$(echo "$METRICS"  | grep '^last_job_s='    | cut -d= -f2)

        if [ "${REACHABLE:-0}" = "0" ]; then
            # Driver just became unreachable — Rule A covers this
            KILL_REASON="dead-driver (UI unreachable) age=${AGE_S}s"
        elif [ "${ACT_STAGES:-0}" -gt 0 ]; then
            # REAL WORK IS HAPPENING RIGHT NOW — never kill
            echo "[cleanup] SKIP  ${APP_ID} '${NAME}' — RUNNING ${AGE_S}s, ${ACT_STAGES} active stage(s) in progress"
        else
            # No active stages — check when the last job completed
            IDLE_FOR=$(( NOW_S - ${LAST_JOB:-0} ))
            if [ "${LAST_JOB:-0}" -eq 0 ]; then
                # Never ran a single job — pure idle session
                KILL_REASON="idle-never-ran age=${AGE_S}s > ${IDLE_SINCE_SECONDS}s"
            elif [ "$IDLE_FOR" -gt "$IDLE_SINCE_SECONDS" ]; then
                KILL_REASON="idle last_job=${IDLE_FOR}s ago > ${IDLE_SINCE_SECONDS}s threshold"
            else
                echo "[cleanup] SKIP  ${APP_ID} '${NAME}' — RUNNING ${AGE_S}s, last job ${IDLE_FOR}s ago (within threshold)"
            fi
        fi
    fi

    # ── Decision ──────────────────────────────────────────────────────────────
    if [ -n "$KILL_REASON" ]; then
        echo "[cleanup] KILL  ${APP_ID} '${NAME}' — ${KILL_REASON}"
        HTTP=$(curl -sf --max-time 10 \
            -X POST "${SPARK_MASTER_UI}/app/kill/" \
            -d "id=${APP_ID}&terminate=true" \
            -o /dev/null -w "%{http_code}" 2>/dev/null) || HTTP="failed"
        echo "[cleanup]   → master responded HTTP ${HTTP}"
    else
        echo "[cleanup] OK    ${APP_ID} '${NAME}' — ${STATE} age=${AGE_S}s cores=${CORES}"
    fi
done

echo "[cleanup] Done."
