#!/usr/bin/env python3
# =============================================================================
# node-pressure-monitor.py — Dynamic cordon / uncordon based on resource usage
# =============================================================================
#
# WHAT IT DOES
# ------------
# Polls every POLL_INTERVAL seconds (default 60 s). For every worker node:
#
#   - Reads allocatable CPU (millicores) and memory (bytes) from the Node spec
#   - Reads the sum of all pod *requests* on that node from the Node status
#   - Computes  used_cpu_pct  = requested_cpu  / allocatable_cpu  * 100
#             used_mem_pct  = requested_mem  / allocatable_mem  * 100
#
#   IF  used_cpu_pct >= CPU_THRESHOLD  OR  used_mem_pct >= MEM_THRESHOLD:
#       kubectl cordon <node>      (adds NoSchedule taint, no pods evicted)
#
#   IF  used_cpu_pct <  CPU_THRESHOLD  AND  used_mem_pct <  MEM_THRESHOLD:
#       kubectl uncordon <node>    (removes NoSchedule taint, scheduling re-enabled)
#
# Only worker nodes are touched — control-plane nodes are skipped.
#
# CONFIGURATION (env vars)
# ------------------------
#   CPU_THRESHOLD   — percent of allocatable CPU requests  (default: 75)
#   MEM_THRESHOLD   — percent of allocatable memory requests (default: 75)
#   POLL_INTERVAL   — seconds between polls                 (default: 60)
#
# RUNNING OUTSIDE A POD (local kubectl context)
# ---------------------------------------------
#   pip install kubernetes
#   python3 scripts/node-pressure-monitor.py
#
# RUNNING INSIDE A POD
# ---------------------
#   Deployed via manifests/node-pressure-monitor/node-pressure-monitor.yaml
#   Uses in-cluster service-account token automatically.
#
# =============================================================================

import logging
import os
import sys
import time

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CPU_THRESHOLD  = float(os.environ.get("CPU_THRESHOLD",  75))   # %
MEM_THRESHOLD  = float(os.environ.get("MEM_THRESHOLD",  75))   # %
POLL_INTERVAL  = int(os.environ.get("POLL_INTERVAL",    60))   # seconds

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("node-pressure-monitor")


# ---------------------------------------------------------------------------
# Kubernetes client bootstrap
# ---------------------------------------------------------------------------
def init_k8s() -> client.CoreV1Api:
    try:
        config.load_incluster_config()
        log.info("Loaded in-cluster kubeconfig")
    except config.ConfigException:
        config.load_kube_config()
        log.info("Loaded local kubeconfig")
    return client.CoreV1Api()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def parse_cpu_to_millicores(value: str) -> float:
    """Convert a CPU quantity string to millicores (float)."""
    if value.endswith("m"):
        return float(value[:-1])
    return float(value) * 1000


def parse_memory_to_bytes(value: str) -> float:
    """Convert a memory quantity string to bytes (float)."""
    units = {
        "Ki": 1024,
        "Mi": 1024 ** 2,
        "Gi": 1024 ** 3,
        "Ti": 1024 ** 4,
        "K":  1000,
        "M":  1000 ** 2,
        "G":  1000 ** 3,
        "T":  1000 ** 4,
    }
    for suffix, multiplier in units.items():
        if value.endswith(suffix):
            return float(value[: -len(suffix)]) * multiplier
    return float(value)


def is_worker(node: client.V1Node) -> bool:
    """Return True if this node is a worker (not a control-plane node)."""
    labels = node.metadata.labels or {}
    taints = node.spec.taints or []
    if "node-role.kubernetes.io/control-plane" in labels:
        return False
    if "node-role.kubernetes.io/master" in labels:
        return False
    for taint in taints:
        if taint.key in (
            "node-role.kubernetes.io/control-plane",
            "node-role.kubernetes.io/master",
        ):
            return False
    return True


def is_cordoned_by_us(node: client.V1Node) -> bool:
    """Return True if the node is currently unschedulable."""
    return bool(node.spec.unschedulable)


def get_node_usage(node: client.V1Node) -> tuple[float, float]:
    """
    Return (used_cpu_pct, used_mem_pct) based on pod request sums
    vs allocatable resources reported in the node status.
    """
    allocatable = node.status.allocatable or {}
    alloc_cpu = parse_cpu_to_millicores(allocatable.get("cpu", "0"))
    alloc_mem = parse_memory_to_bytes(allocatable.get("memory", "0"))

    # Kubernetes stores cumulative pod request sums in
    # node.status.allocatable — we need to sum them ourselves.
    # The node status 'allocatedResources' is NOT a standard API field;
    # we sum pod requests via the Allocated resources on the node.
    # We use the node's own reported figures via the pods list.
    return alloc_cpu, alloc_mem


def get_requested_resources(
    api: client.CoreV1Api, node_name: str
) -> tuple[float, float]:
    """
    Sum all CPU and memory *requests* across all non-terminal pods
    on this node.
    """
    pods = api.list_pod_for_all_namespaces(
        field_selector=f"spec.nodeName={node_name},status.phase!=Failed,status.phase!=Succeeded"
    )
    total_cpu_m = 0.0
    total_mem_b = 0.0
    for pod in pods.items:
        for container in pod.spec.containers:
            if container.resources and container.resources.requests:
                req = container.resources.requests
                if "cpu" in req:
                    total_cpu_m += parse_cpu_to_millicores(req["cpu"])
                if "memory" in req:
                    total_mem_b += parse_memory_to_bytes(req["memory"])
    return total_cpu_m, total_mem_b


def cordon_node(api: client.CoreV1Api, node_name: str) -> None:
    body = {"spec": {"unschedulable": True}}
    api.patch_node(node_name, body)
    log.warning("CORDONED   %s — scheduling disabled", node_name)


def uncordon_node(api: client.CoreV1Api, node_name: str) -> None:
    body = {"spec": {"unschedulable": False}}
    api.patch_node(node_name, body)
    log.info("UNCORDONED %s — scheduling re-enabled", node_name)


# ---------------------------------------------------------------------------
# Main poll loop
# ---------------------------------------------------------------------------
def poll(api: client.CoreV1Api) -> None:
    nodes = api.list_node()
    workers = [n for n in nodes.items if is_worker(n)]

    if not workers:
        log.warning("No worker nodes found — nothing to monitor")
        return

    for node in workers:
        name = node.metadata.name
        allocatable = node.status.allocatable or {}
        alloc_cpu = parse_cpu_to_millicores(allocatable.get("cpu", "0"))
        alloc_mem = parse_memory_to_bytes(allocatable.get("memory", "0"))

        req_cpu, req_mem = get_requested_resources(api, name)

        cpu_pct = (req_cpu / alloc_cpu * 100) if alloc_cpu > 0 else 0.0
        mem_pct = (req_mem / alloc_mem * 100) if alloc_mem > 0 else 0.0

        over_threshold = cpu_pct >= CPU_THRESHOLD or mem_pct >= MEM_THRESHOLD
        currently_cordoned = is_cordoned_by_us(node)

        log.info(
            "%-20s  CPU: %5.1f%%  MEM: %5.1f%%  cordoned=%s  over_threshold=%s",
            name, cpu_pct, mem_pct, currently_cordoned, over_threshold,
        )

        try:
            if over_threshold and not currently_cordoned:
                cordon_node(api, name)
            elif not over_threshold and currently_cordoned:
                uncordon_node(api, name)
        except ApiException as exc:
            log.error("Failed to patch node %s: %s", name, exc)


def main() -> None:
    log.info(
        "Starting node-pressure-monitor  CPU_THRESHOLD=%.0f%%  "
        "MEM_THRESHOLD=%.0f%%  POLL_INTERVAL=%ds",
        CPU_THRESHOLD, MEM_THRESHOLD, POLL_INTERVAL,
    )
    api = init_k8s()

    while True:
        try:
            poll(api)
        except ApiException as exc:
            log.error("API error during poll: %s", exc)
        except Exception as exc:  # noqa: BLE001
            log.error("Unexpected error during poll: %s", exc)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
