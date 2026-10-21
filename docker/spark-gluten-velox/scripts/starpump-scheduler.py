#!/usr/bin/env python3
"""
starpump-scheduler.py
=====================
Long-running daemon that replaces the three starpump CronJobs.

Runs inside the starpump-incremental Deployment pod.  Every INTERVAL_S seconds
it executes starpump.py once per configured source (postgres → oracle → mongodb)
as a subprocess, waits for each to finish, then sleeps until the next cycle.

Each source run gets a completely fresh subprocess so:
  • env vars are re-read each cycle (interval and source config can change
    without restarting the pod — just update the ConfigMap and the next
    cycle picks it up)
  • SparkSession is isolated per run (no leaked state between sources)
  • a failure in one source does not abort the others

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Environment variables (all have defaults — nothing required beyond USER)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  INTERVAL_S        Seconds between the END of one full cycle and the
                    START of the next.  Default: 900 (15 min).

  SOURCES           Comma-separated ordered list of sources to run each
                    cycle.  Default: "postgres,oracle,mongodb".
                    Set to "postgres" to run only one source, etc.

  STARPUMP_SCRIPT   Path to starpump.py.  Default: same directory as
                    this file.

Per-source env vars follow the convention  <SOURCE_UPPER>_<KEY>:
  PG_DATABASE       postgres DATABASE           (default: cache_testing)
  PG_SCHEMAS        postgres SCHEMAS            (default: public)
  PG_ICEBERG_CATALOG postgres ICEBERG_CATALOG   (default: postgres)
  PG_INCLUDE_TABLES postgres INCLUDE_TABLES     (default: "")
  ORA_DATABASE      oracle DATABASE             (default: XEPDB1)
  ORA_SCHEMAS       oracle SCHEMAS              (default: CACHE_TESTING)
  ORA_ICEBERG_CATALOG oracle ICEBERG_CATALOG    (default: oracle)
  ORA_INCLUDE_TABLES oracle INCLUDE_TABLES      (default: "")
  MGO_DATABASE      mongodb DATABASE            (default: cache_testing)
  MGO_SCHEMAS       mongodb SCHEMAS             (default: cache_testing)
  MGO_ICEBERG_CATALOG mongodb ICEBERG_CATALOG   (default: mongodb)
  MGO_INCLUDE_TABLES mongodb INCLUDE_TABLES     (default: "")

Common settings passed to every starpump subprocess:
  USER              (required — propagated from parent env)
  BATCH_SIZE        default: 100000
  MAX_THREADS       default: 4
  MODE              default: incremental
  DDL_DRIFT_DETECT  default: 1
  DRY_RUN           default: 0
  ADDR              OpenBao address
  SPARK_EXECUTOR_MEMORY / SPARK_EXECUTOR_CORES / SPARK_CORES_MAX / DISABLE_GLUTEN
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
import time

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [scheduler] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("scheduler")

# ── Config ────────────────────────────────────────────────────────────────────
INTERVAL_S      = int(os.environ.get("INTERVAL_S", "900"))
SOURCES         = [s.strip() for s in os.environ.get("SOURCES", "postgres,oracle,mongodb").split(",") if s.strip()]
STARPUMP_SCRIPT = os.environ.get(
    "STARPUMP_SCRIPT",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "starpump.py"),
)

# ── Per-source default config ─────────────────────────────────────────────────
_SOURCE_DEFAULTS: dict[str, dict[str, str]] = {
    "postgres": {
        "DATABASE":        "cache_testing",
        "SCHEMAS":         "public",
        "ICEBERG_CATALOG": "postgres",
        "INCLUDE_TABLES":  "",
    },
    "oracle": {
        "DATABASE":        "XEPDB1",
        "SCHEMAS":         "CACHE_TESTING",
        "ICEBERG_CATALOG": "oracle",
        "INCLUDE_TABLES":  "",
    },
    "mongodb": {
        "DATABASE":        "cache_testing",
        "SCHEMAS":         "cache_testing",
        "ICEBERG_CATALOG": "mongodb",
        "INCLUDE_TABLES":  "",
    },
}

# Env-var prefix per source used to override defaults at runtime.
_SOURCE_PREFIX: dict[str, str] = {
    "postgres": "PG_",
    "oracle":   "ORA_",
    "mongodb":  "MGO_",
}


def _source_env(source: str) -> dict[str, str]:
    """
    Build the env dict for one starpump subprocess.

    Starts from the current process env (so SPARK_*, ADDR, USER, etc. are
    inherited), then layers the per-source defaults, then any overrides from
    env vars named  <PREFIX><KEY>  (e.g. PG_DATABASE, ORA_SCHEMAS).
    """
    prefix   = _SOURCE_PREFIX.get(source, source.upper() + "_")
    defaults = _SOURCE_DEFAULTS.get(source, {})

    env = dict(os.environ)  # inherit everything: PATH, JAVA_HOME, SPARK_*, etc.

    # Apply defaults first (only if the plain key is not already set).
    for key, val in defaults.items():
        env.setdefault(key, val)

    # Apply per-source overrides from env vars like PG_DATABASE, ORA_SCHEMAS.
    for key in ("DATABASE", "SCHEMAS", "ICEBERG_CATALOG", "INCLUDE_TABLES"):
        override_key = f"{prefix}{key}"
        if override_key in os.environ:
            env[key] = os.environ[override_key]

    # Remove INCLUDE_TABLES from env if empty (starpump treats absent = all).
    if not env.get("INCLUDE_TABLES", "").strip():
        env.pop("INCLUDE_TABLES", None)

    # Ensure MODE=incremental unless the caller has explicitly set something else.
    env.setdefault("MODE", "incremental")

    return env


def _run_source(source: str) -> bool:
    """
    Run starpump.py for one source.  Returns True on success, False on failure.
    Output (stdout+stderr) is streamed directly to this process's stdout so it
    appears in `kubectl logs` without buffering.
    """
    env = _source_env(source)
    cmd = [sys.executable, STARPUMP_SCRIPT, source, "--mode", env.get("MODE", "incremental")]

    log.info("▶ Starting starpump %s (DATABASE=%s SCHEMAS=%s CATALOG=%s)",
             source,
             env.get("DATABASE", "?"),
             env.get("SCHEMAS", "?"),
             env.get("ICEBERG_CATALOG", "?"))

    t0 = time.monotonic()
    try:
        result = subprocess.run(
            cmd,
            env=env,
            stdout=sys.stdout.fileno(),   # stream directly — no buffering
            stderr=sys.stderr.fileno(),
            cwd=os.path.dirname(STARPUMP_SCRIPT),
        )
        elapsed = time.monotonic() - t0
        if result.returncode == 0:
            log.info("✔ starpump %s finished OK in %.0fs", source, elapsed)
            return True
        else:
            log.error("✘ starpump %s FAILED (exit %d) after %.0fs",
                      source, result.returncode, elapsed)
            return False
    except Exception as exc:
        elapsed = time.monotonic() - t0
        log.error("✘ starpump %s raised exception after %.0fs: %s", source, elapsed, exc)
        return False


def main() -> None:
    log.info(
        "starpump-scheduler starting | sources=%s interval=%ds",
        ",".join(SOURCES), INTERVAL_S,
    )
    log.info("starpump script: %s", STARPUMP_SCRIPT)

    cycle = 0
    while True:
        cycle += 1
        cycle_start = time.monotonic()
        log.info("── Cycle %d start ──────────────────────────────────────────", cycle)

        results: dict[str, bool] = {}
        for source in SOURCES:
            results[source] = _run_source(source)

        ok      = [s for s, r in results.items() if r]
        failed  = [s for s, r in results.items() if not r]
        elapsed = time.monotonic() - cycle_start

        log.info(
            "── Cycle %d done in %.0fs | ok=%s failed=%s ──────────────────",
            cycle, elapsed,
            ",".join(ok) if ok else "none",
            ",".join(failed) if failed else "none",
        )

        # Re-read INTERVAL_S each cycle so a ConfigMap update takes effect
        # without restarting the pod.
        interval = int(os.environ.get("INTERVAL_S", str(INTERVAL_S)))
        remaining = interval - elapsed
        if remaining > 0:
            log.info("Sleeping %.0fs until next cycle …", remaining)
            time.sleep(remaining)
        else:
            log.info("Cycle took longer than interval (%.0fs > %ds) — starting next immediately.",
                     elapsed, interval)


if __name__ == "__main__":
    main()
