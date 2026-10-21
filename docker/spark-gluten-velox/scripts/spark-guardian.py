#!/usr/bin/env python3
"""
spark-guardian.py
=================
Continuous sidecar that watches the Spark standalone master and evicts
stalled or resource-hogging applications in real time.

Replaces the 10-minute CronJob polling loop with a tight 15-second watch
loop running inside the spark-master pod itself.  Because it talks to
localhost:8080 it needs no kubectl, no RBAC, and no network hop.

Rules (evaluated every POLL_INTERVAL_S seconds)
------------------------------------------------

  Rule A — Dead driver  (immediate, any age ≥ ORPHAN_RUNNING_S)
    RUNNING + cores > 0 + age ≥ ORPHAN_RUNNING_S + driver UI unreachable
    The master has not yet noticed the driver is gone.  Safe to kill: driver
    is provably dead (HTTP probe fails).

  Rule B — Executor loss  (after ORPHAN_RUNNING_S = 120 s default)
    RUNNING + cores == 0 + age ≥ ORPHAN_RUNNING_S
    Driver lost all executors (OOM-killed, network partition).  Will never
    make progress again.  Safe to kill: no work is running.

  Rule C — Resource starvation  (after ORPHAN_WAITING_S = 120 s default)
    WAITING + age ≥ ORPHAN_WAITING_S
    App has been waiting for executors longer than the threshold.  With
    executor memory now matched to worker capacity every healthy app should
    get a slot within a few seconds.  Anything still WAITING after
    ORPHAN_WAITING_S has a misconfigured memory request and will NEVER
    schedule — kill it immediately so it does not block the queue.

  Rule D — Idle session  (after IDLE_SINCE_S = 1800 s default)
    RUNNING + cores > 0 + driver UI reachable
    + no active Spark stages right now
    + time since last completed job > IDLE_SINCE_S
    Catches JupyterHub kernels whose user closed the browser tab hours ago.
    Real pipelines (starpump, kafka-to-iceberg) submit jobs continuously;
    their last-job time is always fresh.

Environment variables
---------------------
  SPARK_MASTER_UI         URL of Spark master web UI   default: http://localhost:8080
  POLL_INTERVAL_S         Seconds between full scans   default: 15
  ORPHAN_RUNNING_S        Rule A/B threshold (seconds) default: 120
  ORPHAN_WAITING_S        Rule C threshold (seconds)   default: 120
  IDLE_SINCE_S            Rule D threshold (seconds)   default: 1800
  DRY_RUN                 1 = log kills but don't POST default: 0
"""
from __future__ import annotations

import datetime
import logging
import os
import sys
import time
import urllib.parse
import urllib.request
import json

# ── Config ────────────────────────────────────────────────────────────────────
SPARK_MASTER_UI    = os.environ.get("SPARK_MASTER_UI",  "http://localhost:8080")
POLL_INTERVAL_S    = int(os.environ.get("POLL_INTERVAL_S",   "15"))
ORPHAN_RUNNING_S   = int(os.environ.get("ORPHAN_RUNNING_S",  "120"))
ORPHAN_WAITING_S   = int(os.environ.get("ORPHAN_WAITING_S",  "120"))
IDLE_SINCE_S       = int(os.environ.get("IDLE_SINCE_S",      "1800"))
DRY_RUN            = os.environ.get("DRY_RUN", "0") == "1"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [guardian] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("guardian")


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _get_json(url: str, timeout: float = 5.0) -> object | None:
    """GET *url*, return parsed JSON or None on any error."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def _http_reachable(url: str, timeout: float = 5.0) -> bool:
    """Return True if a GET to *url* returns any HTTP response (even 4xx/5xx)."""
    try:
        with urllib.request.urlopen(url, timeout=timeout):
            return True
    except urllib.error.HTTPError:
        return True   # server answered → driver is alive
    except Exception:
        return False


def _kill_app(app_id: str, reason: str, dry_run: bool) -> None:
    """POST to the Spark master kill endpoint for *app_id*."""
    if dry_run:
        log.info("DRY-RUN KILL %s — %s", app_id, reason)
        return
    url  = f"{SPARK_MASTER_UI}/app/kill/"
    data = urllib.parse.urlencode({"id": app_id, "terminate": "true"}).encode()
    req  = urllib.request.Request(url, data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            log.info("KILL %s — %s  (HTTP %s)", app_id, reason, resp.status)
    except Exception as exc:
        log.warning("KILL %s failed: %s", app_id, exc)


# ── Driver metrics helper ─────────────────────────────────────────────────────

def _driver_metrics(driver_url: str) -> dict:
    """
    Probe the driver's own Spark UI REST API.

    Returns a dict:
      reachable     bool
      active_stages int    (0 if unreachable)
      last_job_s    int    (epoch seconds of most recent completed/submitted job,
                            0 if no jobs ever ran)
    """
    result = {"reachable": False, "active_stages": 0, "last_job_s": 0}
    if not driver_url:
        return result

    # The driver_url from the master JSON is like http://10.244.x.x:4040/...
    # We need just the scheme+host+port base.
    parsed = urllib.parse.urlparse(driver_url)
    base   = f"{parsed.scheme}://{parsed.netloc}"

    app_list = _get_json(f"{base}/api/v1/applications", timeout=5)
    if not app_list:
        return result

    # app_list is a JSON array; first entry is the running app.
    app_id = app_list[0].get("id", "") if app_list else ""
    if not app_id:
        return result

    result["reachable"] = True

    # Active stages right now
    stages = _get_json(
        f"{base}/api/v1/applications/{app_id}/stages?status=active", timeout=5
    )
    result["active_stages"] = len(stages) if isinstance(stages, list) else 0

    # Most recent completed/submitted job timestamp
    jobs = _get_json(f"{base}/api/v1/applications/{app_id}/jobs", timeout=5)
    if isinstance(jobs, list):
        latest = 0
        for j in jobs:
            raw = j.get("completionTime") or j.get("submissionTime") or ""
            if not raw:
                continue
            # Normalise: "2026-08-30T02:14:31.000GMT" → parseable
            raw = raw.replace("GMT", "").replace("UTC", "").strip()
            try:
                dt = datetime.datetime.fromisoformat(raw)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=datetime.timezone.utc)
                epoch = int(dt.timestamp())
                if epoch > latest:
                    latest = epoch
            except Exception:
                pass
        result["last_job_s"] = latest

    return result


# ── Per-app evaluation ────────────────────────────────────────────────────────

def _evaluate(app: dict, now_ms: int) -> str | None:
    """
    Apply rules A–D to *app*.  Return a non-empty kill-reason string if the
    app should be evicted, or None to leave it alone.
    """
    app_id     = app.get("id", "")
    state      = app.get("state", "")
    cores      = int(app.get("cores", 0))
    start_ms   = int(app.get("starttime", 0))
    driver_url = app.get("appuiurl", "") or ""
    name       = app.get("name", "unknown")

    age_s = (now_ms - start_ms) // 1000

    # ── Rule A: dead driver ───────────────────────────────────────────────────
    # Only probe once the driver has had ORPHAN_RUNNING_S seconds to start up,
    # and only when the master actually advertised a UI URL.
    if (
        state == "RUNNING"
        and cores > 0
        and age_s >= ORPHAN_RUNNING_S
        and driver_url
        and not _http_reachable(driver_url)
    ):
        return f"dead-driver (UI unreachable: {driver_url}) age={age_s}s"

    # ── Rule B: executor loss ─────────────────────────────────────────────────
    if state == "RUNNING" and cores == 0 and age_s >= ORPHAN_RUNNING_S:
        return f"no-executors age={age_s}s >= {ORPHAN_RUNNING_S}s"

    # ── Rule C: resource starvation ───────────────────────────────────────────
    if state == "WAITING" and age_s >= ORPHAN_WAITING_S:
        return f"waiting-stalled age={age_s}s >= {ORPHAN_WAITING_S}s"

    # ── Rule D: idle session ──────────────────────────────────────────────────
    if state == "RUNNING" and cores > 0 and age_s >= IDLE_SINCE_S:
        m = _driver_metrics(driver_url)

        if not m["reachable"]:
            # Treat as dead driver
            return f"dead-driver (UI unreachable during idle-check) age={age_s}s"

        if m["active_stages"] > 0:
            # Real work happening right now — never evict
            log.debug(
                "SKIP %s '%s' — %d active stage(s) in progress",
                app_id, name, m["active_stages"],
            )
            return None

        last_job_s = m["last_job_s"]
        if last_job_s == 0:
            return f"idle-never-ran age={age_s}s >= {IDLE_SINCE_S}s"

        idle_for = int(time.time()) - last_job_s
        if idle_for >= IDLE_SINCE_S:
            return f"idle last_job={idle_for}s ago >= {IDLE_SINCE_S}s threshold"

        log.debug(
            "SKIP %s '%s' — RUNNING %ds, last job %ds ago (within %ds threshold)",
            app_id, name, age_s, idle_for, IDLE_SINCE_S,
        )

    return None


# ── One full scan ─────────────────────────────────────────────────────────────

def _scan_once() -> None:
    data = _get_json(f"{SPARK_MASTER_UI}/json/")
    if data is None:
        log.warning("Cannot reach Spark master at %s — skipping scan.", SPARK_MASTER_UI)
        return

    apps   = data.get("activeapps", [])
    now_ms = int(time.time() * 1000)

    if not apps:
        log.debug("No active apps.")
        return

    workers       = data.get("workers", [])
    alive_workers = [w for w in workers if w.get("state") == "ALIVE"]
    total_free_mb = sum(w.get("memoryfree", 0) for w in alive_workers)
    total_free_c  = sum(w.get("coresfree",  0) for w in alive_workers)

    waiting = [a for a in apps if a.get("state") == "WAITING"]
    running = [a for a in apps if a.get("state") == "RUNNING"]

    log.info(
        "Scan: %d active apps (%d RUNNING, %d WAITING) | "
        "workers=%d free_mem=%dMB free_cores=%d",
        len(apps), len(running), len(waiting),
        len(alive_workers), total_free_mb, total_free_c,
    )

    for app in apps:
        app_id = app.get("id", "")
        name   = app.get("name", "unknown")
        state  = app.get("state", "")
        cores  = int(app.get("cores", 0))
        start  = int(app.get("starttime", 0))
        age_s  = (now_ms - start) // 1000

        reason = _evaluate(app, now_ms)

        if reason:
            log.warning("KILL  %s '%s' — %s", app_id, name, reason)
            _kill_app(app_id, reason, DRY_RUN)
        else:
            log.info(
                "OK    %s '%s' — %s age=%ds cores=%d",
                app_id, name, state, age_s, cores,
            )


# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    mode = "DRY-RUN" if DRY_RUN else "LIVE"
    log.info(
        "spark-guardian starting (%s) | ui=%s poll=%ds "
        "orphan_running=%ds orphan_waiting=%ds idle=%ds",
        mode, SPARK_MASTER_UI, POLL_INTERVAL_S,
        ORPHAN_RUNNING_S, ORPHAN_WAITING_S, IDLE_SINCE_S,
    )

    # Wait for the master to finish booting before the first scan.
    log.info("Waiting 30s for Spark master to initialise …")
    time.sleep(30)

    while True:
        try:
            _scan_once()
        except Exception as exc:
            log.error("Scan error: %s", exc, exc_info=True)
        time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    main()
