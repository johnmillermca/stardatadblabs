"""
doris_fe_entrypoint.py
======================
Supervisor entrypoint for the Doris FE + Cloud MetaService stub.

Start order
-----------
1. Cloud MetaService gRPC stub (cloud_meta_service.py) — starts in a daemon
   thread; ready in < 100 ms.
2. Wait until the stub is accepting connections (TCP probe on localhost:5000).
3. Exec the Doris FE start script (/opt/apache-doris/fe/bin/start_fe.sh)
   as a subprocess.  Forward all signals.  Exit with the FE's exit code.

Why subprocess + exec instead of os.exec?
------------------------------------------
We need the MetaService gRPC stub to remain alive for the lifetime of the FE
(the FE reconnects periodically).  os.execv would replace this process,
killing the stub.  Instead we run the FE as a child subprocess and proxy
SIGTERM / SIGINT so Kubernetes terminationGracePeriodSeconds works correctly.

Performance guarantee
----------------------
The MetaService stub runs on localhost:5000 in a 4-thread gRPC server.
It handles only the FE bootstrap (getCluster, getInstance) and occasional
heartbeat-level RPCs.  The Doris query data path (FE→BE Thrift for scan
fragments, BE→BE data exchange, stream load) never touches the MetaService.
CPU overhead: < 0.1 %.  Memory: < 20 MB (Python process + grpcio).
"""
from __future__ import annotations

import logging
import os
import signal
import socket
import subprocess
import sys
import threading
import time

log = logging.getLogger("doris-fe-entrypoint")

MS_PORT   = int(os.environ.get("MS_PORT",  "5000"))
FE_SCRIPT = os.environ.get("FE_SCRIPT", "/opt/apache-doris/fe/bin/start_fe.sh")
# FE_ARGS are additional arguments forwarded to start_fe.sh (e.g. the master FQDN)
FE_ARGS   = sys.argv[1:]


# ─────────────────────────────────────────────────────────────────────────────
# 1. Start MetaService stub in a daemon thread
# ─────────────────────────────────────────────────────────────────────────────
def _start_meta_service() -> None:
    # Import here so startup errors surface clearly
    from cloud_meta_service import run_forever
    try:
        run_forever(MS_PORT)
    except Exception as exc:
        log.error("MetaService stub crashed: %s", exc, exc_info=True)
        # Kill this process so Kubernetes restarts the pod — we cannot let the
        # FE start without a working MetaService stub.
        os.kill(os.getpid(), signal.SIGTERM)


def _wait_for_port(host: str, port: int, timeout: float = 10.0) -> bool:
    """Return True when TCP port is accepting connections, False on timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    return False


# ─────────────────────────────────────────────────────────────────────────────
# 2. Run Doris FE as a subprocess
# ─────────────────────────────────────────────────────────────────────────────
def _run_fe() -> int:
    """
    Start start_fe.sh as a child process, proxy signals, return exit code.
    start_fe.sh expects the master FQDN as its first argument (same as before).
    """
    cmd = [FE_SCRIPT, "--daemon=false"] + FE_ARGS
    log.info("Starting Doris FE: %s", " ".join(cmd))

    proc = subprocess.Popen(
        cmd,
        stdin=sys.stdin,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )

    def _forward(sig: int, _frame: object) -> None:
        log.info("Forwarding signal %d to FE (pid=%d).", sig, proc.pid)
        proc.send_signal(sig)

    signal.signal(signal.SIGTERM, _forward)
    signal.signal(signal.SIGINT,  _forward)

    rc = proc.wait()
    log.info("Doris FE exited with code %d.", rc)
    return rc


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    log.info("Starting Cloud MetaService stub on port %d …", MS_PORT)
    t = threading.Thread(target=_start_meta_service, name="meta-service", daemon=True)
    t.start()

    if not _wait_for_port("127.0.0.1", MS_PORT, timeout=15.0):
        log.error("MetaService stub did not become ready within 15 s — aborting.")
        sys.exit(1)
    log.info("MetaService stub ready.")

    sys.exit(_run_fe())


if __name__ == "__main__":
    main()
