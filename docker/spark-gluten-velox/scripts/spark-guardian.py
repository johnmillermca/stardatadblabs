#!/usr/bin/env python3
"""
spark-guardian.py
=================
Continuous sidecar that watches the Spark standalone master and dynamically
enforces cluster fairness — evicting stalled, orphaned, or over-resourced apps
in real time, AND auto-scaling the worker Deployment up/down via the K8s API.

Runs inside the spark-master pod (localhost:8080).
Poll cadence: POLL_INTERVAL_S (default 15 s).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Rules (evaluated every scan, in priority order)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Rule E — Resource over-limit  (immediate, any age)
    App holds cores > MAX_CORES_PER_APP  OR  memoryperexecutor > MAX_MEM_MB_PER_APP
    → kill immediately.

  Rule A — Dead driver  (after ORPHAN_RUNNING_S grace)
    RUNNING + cores > 0 + age ≥ ORPHAN_RUNNING_S + driver UI unreachable

  Rule B — Executor loss  (after ORPHAN_RUNNING_S)
    RUNNING + cores == 0 + age ≥ ORPHAN_RUNNING_S

  Rule C — Resource starvation  (after ORPHAN_WAITING_S)
    WAITING + age ≥ ORPHAN_WAITING_S

  Rule D — Stuck / idle session  (after ACTIVITY_CHECK_AFTER_S)
    RUNNING + cores > 0 + age ≥ ACTIVITY_CHECK_AFTER_S
    + driver UI reachable
    + no Spark work observed for IDLE_SINCE_S seconds straight

    "Spark work observed" (any one sufficient):
      • active_stages > 0
      • pending_tasks > 0
      • completed_jobs delta > 0 since last scan

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Worker autoscaling (Rule S)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Scale UP:  any WAITING app + zero ALIVE workers → scale to WORKER_REPLICAS
             immediately (no grace period — a waiting job needs a worker now).

  Scale DOWN: no active apps AND no ALIVE workers doing work AND
              cluster has been continuously idle for SCALE_DOWN_IDLE_S seconds
              → scale worker Deployment back to 0.
              Default SCALE_DOWN_IDLE_S = 120 s (2 min after last job finishes).

  Workers are scaled via the Kubernetes Deployments API (PATCH replicas).
  Requires the pod's ServiceAccount to have:
    apiGroups: [apps]  resources: [deployments/scale]  verbs: [get, patch]
  on the worker Deployments in the same namespace.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Environment variables
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  SPARK_MASTER_UI          default: http://localhost:8080
  POLL_INTERVAL_S          default: 15
  MAX_CORES_PER_APP        Rule E — hard core cap per app        default: 3
  MAX_MEM_MB_PER_APP       Rule E — hard memory cap per executor default: 1024
  ORPHAN_RUNNING_S         Rule A/B threshold                    default: 120
  ORPHAN_WAITING_S         Rule C threshold                      default: 120
  ACTIVITY_CHECK_AFTER_S   Rule D — start checking at this age   default: 300
  IDLE_SINCE_S             Rule D — evict after this idle gap     default: 300
  DRY_RUN                  1 = log kills/scales, never act       default: 0

  Worker autoscaling:
  WORKER_DEPLOYMENT        Deployment name to scale              default: spark-worker
  WORKER_NAMESPACE         Namespace of that Deployment          default: prod
  WORKER_REPLICAS          Replicas to set on scale-up           default: 3
  SCALE_DOWN_IDLE_S        Idle seconds before scale-to-0        default: 120
  AUTOSCALE                0 = disable autoscaling entirely       default: 1
"""
from __future__ import annotations

import json
import logging
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# ── Config ────────────────────────────────────────────────────────────────────
SPARK_MASTER_UI        = os.environ.get("SPARK_MASTER_UI",        "http://localhost:8080")
POLL_INTERVAL_S        = int(os.environ.get("POLL_INTERVAL_S",        "15"))
MAX_CORES_PER_APP      = int(os.environ.get("MAX_CORES_PER_APP",       "3"))
MAX_MEM_MB_PER_APP     = int(os.environ.get("MAX_MEM_MB_PER_APP",      "1024"))
ORPHAN_RUNNING_S       = int(os.environ.get("ORPHAN_RUNNING_S",        "120"))
ORPHAN_WAITING_S       = int(os.environ.get("ORPHAN_WAITING_S",        "120"))
ACTIVITY_CHECK_AFTER_S = int(os.environ.get("ACTIVITY_CHECK_AFTER_S",  "300"))
IDLE_SINCE_S           = int(os.environ.get("IDLE_SINCE_S",            "300"))
DRY_RUN                = os.environ.get("DRY_RUN", "0") == "1"

# Worker autoscaling
WORKER_DEPLOYMENT  = os.environ.get("WORKER_DEPLOYMENT",  "spark-worker")
WORKER_NAMESPACE   = os.environ.get("WORKER_NAMESPACE",   "prod")
WORKER_REPLICAS    = int(os.environ.get("WORKER_REPLICAS",    "3"))
SCALE_DOWN_IDLE_S  = int(os.environ.get("SCALE_DOWN_IDLE_S",  "120"))
AUTOSCALE          = os.environ.get("AUTOSCALE", "1") == "1"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [guardian] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("guardian")

# ── Cross-scan state ──────────────────────────────────────────────────────────
_last_active_s:       dict[str, float] = {}
_last_completed_jobs: dict[str, int]   = {}
_cluster_idle_since:  float            = 0.0   # epoch when cluster last became fully idle
_workers_at_zero:     bool             = False  # True once we've scaled down this idle period


# ── Kubernetes API helpers ────────────────────────────────────────────────────

_K8S_API   = "https://kubernetes.default.svc"
_SA_TOKEN  = "/var/run/secrets/kubernetes.io/serviceaccount/token"
_SA_CA     = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
_K8S_NS_F  = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"


def _k8s_token() -> str:
    try:
        with open(_SA_TOKEN) as f:
            return f.read().strip()
    except Exception:
        return ""


def _k8s_namespace() -> str:
    try:
        with open(_K8S_NS_F) as f:
            return f.read().strip()
    except Exception:
        return WORKER_NAMESPACE


def _k8s_ssl_ctx() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    try:
        ctx.load_verify_locations(_SA_CA)
    except Exception:
        ctx.check_hostname = False
        ctx.verify_mode    = ssl.CERT_NONE
    return ctx


def _k8s_request(method: str, path: str, body: dict | None = None) -> dict | None:
    """Make a K8s API call using the pod's ServiceAccount JWT."""
    token = _k8s_token()
    if not token:
        log.warning("No SA token — cannot call K8s API.")
        return None
    ns    = _k8s_namespace()
    url   = f"{_K8S_API}{path.format(namespace=ns)}"
    data  = json.dumps(body).encode() if body else None
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/strategic-merge-patch+json" if method == "PATCH" else "application/json",
        "Accept":        "application/json",
    }
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, context=_k8s_ssl_ctx(), timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        log.warning("K8s API %s %s → HTTP %d: %s", method, path, e.code, e.read()[:200])
        return None
    except Exception as exc:
        log.warning("K8s API %s %s error: %s", method, path, exc)
        return None


def _get_worker_replicas() -> int | None:
    """Return current spec.replicas for the worker Deployment, or None on error."""
    ns   = _k8s_namespace()
    path = f"/apis/apps/v1/namespaces/{ns}/deployments/{WORKER_DEPLOYMENT}"
    resp = _k8s_request("GET", path)
    if resp is None:
        return None
    return resp.get("spec", {}).get("replicas", 0)


def _scale_workers(replicas: int, reason: str) -> bool:
    """PATCH the worker Deployment replicas. Returns True on success."""
    if DRY_RUN:
        log.warning("DRY-RUN SCALE %s → %d — %s", WORKER_DEPLOYMENT, replicas, reason)
        return True
    ns   = _k8s_namespace()
    path = f"/apis/apps/v1/namespaces/{ns}/deployments/{WORKER_DEPLOYMENT}"
    resp = _k8s_request("PATCH", path, {"spec": {"replicas": replicas}})
    if resp is not None:
        log.warning("SCALE %s → %d replicas — %s", WORKER_DEPLOYMENT, replicas, reason)
        return True
    return False


# ── Spark master HTTP helpers ─────────────────────────────────────────────────

def _get_json(url: str, timeout: float = 5.0) -> object | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def _http_reachable(url: str, timeout: float = 5.0) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout):
            return True
    except urllib.error.HTTPError:
        return True
    except Exception:
        return False


def _kill_app(app_id: str, reason: str) -> None:
    if DRY_RUN:
        log.warning("DRY-RUN KILL %s — %s", app_id, reason)
        return
    url  = f"{SPARK_MASTER_UI}/app/kill/"
    data = urllib.parse.urlencode({"id": app_id, "terminate": "true"}).encode()
    req  = urllib.request.Request(url, data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            log.warning("KILL %s — %s  (HTTP %s)", app_id, reason, resp.status)
    except Exception as exc:
        log.warning("KILL %s failed: %s", app_id, exc)


# ── Driver metrics ────────────────────────────────────────────────────────────

def _driver_metrics(driver_url: str) -> dict:
    result = {"reachable": False, "active_stages": 0,
              "pending_tasks": 0, "completed_jobs": 0}
    if not driver_url:
        return result
    import urllib.parse as _up
    parsed = _up.urlparse(driver_url)
    base   = f"{parsed.scheme}://{parsed.netloc}"
    apps   = _get_json(f"{base}/api/v1/applications", timeout=5)
    if not apps:
        return result
    local_id = apps[0].get("id", "") if apps else ""
    if not local_id:
        return result
    result["reachable"] = True
    stages = _get_json(f"{base}/api/v1/applications/{local_id}/stages?status=active", timeout=5)
    if isinstance(stages, list):
        result["active_stages"] = len(stages)
        result["pending_tasks"] = sum(
            s.get("numActiveTasks", 0) + s.get("numPendingTasks", 0) for s in stages
        )
    jobs = _get_json(f"{base}/api/v1/applications/{local_id}/jobs", timeout=5)
    if isinstance(jobs, list):
        result["completed_jobs"] = sum(1 for j in jobs if j.get("status") == "SUCCEEDED")
    return result


# ── Per-app evaluation ────────────────────────────────────────────────────────

def _evaluate(app: dict, now_s: float) -> str | None:
    app_id     = app.get("id", "")
    state      = app.get("state", "")
    cores      = int(app.get("cores", 0))
    mem_mb     = int(app.get("memoryperexecutor", 0))
    start_ms   = int(app.get("starttime", 0))
    driver_url = app.get("appuiurl", "") or ""
    name       = app.get("name", "unknown")
    age_s      = now_s - start_ms / 1000.0

    if app_id not in _last_active_s:
        _last_active_s[app_id]       = now_s
        _last_completed_jobs[app_id] = 0

    # Rule E
    if mem_mb > MAX_MEM_MB_PER_APP:
        _evict_cleanup(app_id)
        return f"over-memory mem_per_exec={mem_mb}MB > cap={MAX_MEM_MB_PER_APP}MB"
    if cores > MAX_CORES_PER_APP:
        _evict_cleanup(app_id)
        return f"over-cores cores={cores} > cap={MAX_CORES_PER_APP}"

    # Rule A
    if (state == "RUNNING" and cores > 0 and age_s >= ORPHAN_RUNNING_S
            and driver_url and not _http_reachable(driver_url)):
        _evict_cleanup(app_id)
        return f"dead-driver (UI unreachable) age={age_s:.0f}s"

    # Rule B
    if state == "RUNNING" and cores == 0 and age_s >= ORPHAN_RUNNING_S:
        _evict_cleanup(app_id)
        return f"no-executors age={age_s:.0f}s >= {ORPHAN_RUNNING_S}s"

    # Rule C
    if state == "WAITING" and age_s >= ORPHAN_WAITING_S:
        _evict_cleanup(app_id)
        return f"waiting-stalled age={age_s:.0f}s >= {ORPHAN_WAITING_S}s"

    # Rule D
    if state == "RUNNING" and cores > 0 and age_s >= ACTIVITY_CHECK_AFTER_S:
        m = _driver_metrics(driver_url)
        if not m["reachable"]:
            _evict_cleanup(app_id)
            return f"dead-driver (activity-check) age={age_s:.0f}s"

        prev_jobs  = _last_completed_jobs.get(app_id, 0)
        curr_jobs  = m["completed_jobs"]
        delta_jobs = curr_jobs - prev_jobs
        _last_completed_jobs[app_id] = curr_jobs

        working = m["active_stages"] > 0 or m["pending_tasks"] > 0 or delta_jobs > 0
        if working:
            _last_active_s[app_id] = now_s
            log.info("WORKING %s '%s' — age=%.0fs stages=%d pending=%d new_jobs=%d",
                     app_id, name, age_s, m["active_stages"], m["pending_tasks"], delta_jobs)
            return None

        idle_for = now_s - _last_active_s.get(app_id, now_s)
        if idle_for < IDLE_SINCE_S:
            log.info("QUIET  %s '%s' — age=%.0fs no-activity for %.0fs (tolerance %ds)",
                     app_id, name, age_s, idle_for, IDLE_SINCE_S)
            return None

        if m["active_stages"] > 0:
            _last_active_s[app_id] = now_s
            return None

        _evict_cleanup(app_id)
        return (f"stuck/idle no-activity-for={idle_for:.0f}s >= {IDLE_SINCE_S}s "
                f"(age={age_s:.0f}s stages=0 jobs={curr_jobs})")

    if state == "RUNNING" and cores > 0:
        _last_active_s[app_id] = now_s

    return None


def _evict_cleanup(app_id: str) -> None:
    _last_active_s.pop(app_id, None)
    _last_completed_jobs.pop(app_id, None)


def _gc_state(active_ids: set[str]) -> None:
    for gone in set(_last_active_s) - active_ids:
        _last_active_s.pop(gone, None)
        _last_completed_jobs.pop(gone, None)


# ── Worker autoscaling ────────────────────────────────────────────────────────

def _autoscale(apps: list[dict], alive_workers: int, now_s: float) -> None:
    """
    Rule S — scale workers up or down based on cluster demand.

    Scale UP:   there are WAITING apps but zero alive workers.
    Scale DOWN: no apps at all AND zero alive workers doing work AND
                cluster has been idle for SCALE_DOWN_IDLE_S seconds.
    """
    global _cluster_idle_since, _workers_at_zero

    if not AUTOSCALE:
        return

    waiting_apps = [a for a in apps if a.get("state") == "WAITING"]
    running_apps = [a for a in apps if a.get("state") == "RUNNING"]
    has_any_apps = bool(apps)

    # ── Scale UP ──────────────────────────────────────────────────────────────
    # Waiting app + no alive workers → scale up immediately.
    if waiting_apps and alive_workers == 0:
        current = _get_worker_replicas()
        if current == 0:
            _scale_workers(
                WORKER_REPLICAS,
                f"{len(waiting_apps)} app(s) WAITING, 0 alive workers",
            )
        _cluster_idle_since = 0.0  # reset idle clock — cluster is active
        _workers_at_zero    = False
        return

    # ── Idle clock management ─────────────────────────────────────────────────
    # Reset idle clock whenever there is any activity.
    if has_any_apps or alive_workers > 0:
        _cluster_idle_since = 0.0
        _workers_at_zero    = False
        return

    # No apps, no alive workers — start (or continue) idle countdown.
    if _cluster_idle_since == 0.0:
        _cluster_idle_since = now_s
        log.info("IDLE   cluster empty — starting %.0fs scale-down countdown.", SCALE_DOWN_IDLE_S)
        return

    idle_for = now_s - _cluster_idle_since

    # ── Scale DOWN ────────────────────────────────────────────────────────────
    if idle_for >= SCALE_DOWN_IDLE_S and not _workers_at_zero:
        current = _get_worker_replicas()
        if current is not None and current > 0:
            _scale_workers(0, f"cluster idle for {idle_for:.0f}s >= {SCALE_DOWN_IDLE_S}s")
        _workers_at_zero = True
        return

    if not _workers_at_zero:
        log.info("IDLE   cluster empty for %.0fs / %ds — waiting to scale down …",
                 idle_for, SCALE_DOWN_IDLE_S)


# ── One full scan ─────────────────────────────────────────────────────────────

def _scan_once() -> None:
    data = _get_json(f"{SPARK_MASTER_UI}/json/")
    if data is None:
        log.warning("Cannot reach Spark master at %s", SPARK_MASTER_UI)
        return

    apps          = data.get("activeapps", [])
    now_s         = time.time()
    workers       = data.get("workers", [])
    alive_workers = [w for w in workers if w.get("state") == "ALIVE"]
    free_mem_mb   = sum(w.get("memoryfree", 0) for w in alive_workers)
    free_cores    = sum(w.get("coresfree",  0) for w in alive_workers)

    if apps:
        waiting_n = sum(1 for a in apps if a.get("state") == "WAITING")
        running_n = sum(1 for a in apps if a.get("state") == "RUNNING")
        log.info(
            "Scan: %d apps (%d RUNNING %d WAITING) | "
            "workers=%d free_mem=%dMB free_cores=%d | "
            "caps: mem<=%dMB cores<=%d",
            len(apps), running_n, waiting_n,
            len(alive_workers), free_mem_mb, free_cores,
            MAX_MEM_MB_PER_APP, MAX_CORES_PER_APP,
        )

    # Evaluate each app (kill rules E/A/B/C/D).
    active_ids: set[str] = set()
    for app in apps:
        app_id = app.get("id", "")
        name   = app.get("name", "unknown")
        state  = app.get("state", "")
        cores  = int(app.get("cores", 0))
        mem_mb = int(app.get("memoryperexecutor", 0))
        age_s  = now_s - int(app.get("starttime", 0)) / 1000.0
        active_ids.add(app_id)

        reason = _evaluate(app, now_s)
        if reason:
            _kill_app(app_id, reason)
        else:
            idle_for = now_s - _last_active_s.get(app_id, now_s)
            log.info("OK    %s '%s' — %s age=%.0fs cores=%d mem=%dMB idle_for=%.0fs",
                     app_id, name, state, age_s, cores, mem_mb, idle_for)

    _gc_state(active_ids)

    # Rule S — autoscale workers.
    _autoscale(apps, len(alive_workers), now_s)


# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    mode = "DRY-RUN" if DRY_RUN else "LIVE"
    log.info(
        "spark-guardian starting (%s) | ui=%s poll=%ds | "
        "caps: mem<=%dMB cores<=%d | "
        "orphan_running=%ds orphan_waiting=%ds | "
        "activity_check_after=%ds idle_evict=%ds",
        mode, SPARK_MASTER_UI, POLL_INTERVAL_S,
        MAX_MEM_MB_PER_APP, MAX_CORES_PER_APP,
        ORPHAN_RUNNING_S, ORPHAN_WAITING_S,
        ACTIVITY_CHECK_AFTER_S, IDLE_SINCE_S,
    )
    if AUTOSCALE:
        log.info(
            "Autoscale ON | worker=%s ns=%s target_replicas=%d scale_down_idle=%ds",
            WORKER_DEPLOYMENT, WORKER_NAMESPACE, WORKER_REPLICAS, SCALE_DOWN_IDLE_S,
        )
    else:
        log.info("Autoscale DISABLED (AUTOSCALE=0).")

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
