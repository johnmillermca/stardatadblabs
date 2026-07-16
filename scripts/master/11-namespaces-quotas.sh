#!/usr/bin/env bash
# =============================================================================
# 11-namespaces-quotas.sh  —  Create all platform namespaces + quotas
#
# Namespace model (simplified — single prod namespace)
# ────────────────────────────────────────────────────
#   prod        : ALL platform workloads — databases, streaming, analytics,
#                 security, catalog, orchestration, search, registry, openbao
#   monitoring  : Prometheus + Grafana + MCP monitoring servers only
#
# Safe to re-run — uses kubectl apply.
# Usage: sudo bash scripts/master/11-namespaces-quotas.sh
# =============================================================================
set -euo pipefail
export PATH="/usr/local/bin:${PATH}"

log() { echo "[$(date '+%H:%M:%S')] $*"; }
die() { echo "[ERROR] $*" >&2; exit 1; }

[[ $EUID -eq 0 ]] || die "Run as root: sudo bash $0"

log "=== Creating platform namespaces ==="

# ── prod — all workloads ──────────────────────────────────────────────────────
log "  namespace: prod"
kubectl apply -f - <<'EOF'
---
apiVersion: v1
kind: Namespace
metadata:
  name: prod
  labels:
    app.kubernetes.io/managed-by: platform
    environment: prod
---
apiVersion: v1
kind: ResourceQuota
metadata:
  name: prod-quota
  namespace: prod
spec:
  hard:
    requests.cpu:            "60"
    limits.cpu:              "120"
    requests.memory:         120Gi
    limits.memory:           240Gi
    requests.storage:        2000Gi
    persistentvolumeclaims:  "60"
    pods:                    "300"
    services:                "150"
    configmaps:              "300"
    secrets:                 "300"
    replicationcontrollers:  "60"
    resourcequotas:          "5"
---
apiVersion: v1
kind: LimitRange
metadata:
  name: prod-limits
  namespace: prod
spec:
  limits:
    - type: Container
      default:
        cpu: 500m
        memory: 512Mi
      defaultRequest:
        cpu: 100m
        memory: 128Mi
      max:
        cpu: "16"
        memory: 32Gi
      min:
        cpu: 10m
        memory: 32Mi
    - type: Pod
      max:
        cpu: "32"
        memory: 64Gi
    - type: PersistentVolumeClaim
      max:
        storage: 500Gi
      min:
        storage: 1Gi
EOF

# ── monitoring — observability only ───────────────────────────────────────────
log "  namespace: monitoring"
kubectl apply -f - <<'EOF'
---
apiVersion: v1
kind: Namespace
metadata:
  name: monitoring
  labels:
    app.kubernetes.io/managed-by: platform
    environment: monitoring
    purpose: observability
---
apiVersion: v1
kind: ResourceQuota
metadata:
  name: monitoring-quota
  namespace: monitoring
spec:
  hard:
    requests.cpu:            "8"
    limits.cpu:              "16"
    requests.memory:         16Gi
    limits.memory:           32Gi
    requests.storage:        200Gi
    persistentvolumeclaims:  "10"
    pods:                    "40"
    services:                "20"
    configmaps:              "40"
    secrets:                 "40"
    resourcequotas:          "2"
---
apiVersion: v1
kind: LimitRange
metadata:
  name: monitoring-limits
  namespace: monitoring
spec:
  limits:
    - type: Container
      default:
        cpu: 500m
        memory: 512Mi
      defaultRequest:
        cpu: 100m
        memory: 128Mi
      max:
        cpu: "4"
        memory: 8Gi
      min:
        cpu: 10m
        memory: 32Mi
    - type: Pod
      max:
        cpu: "8"
        memory: 16Gi
    - type: PersistentVolumeClaim
      max:
        storage: 100Gi
      min:
        storage: 1Gi
EOF

log ""
log "=== Namespace summary ==="
kubectl get namespaces -l app.kubernetes.io/managed-by=platform \
  -o custom-columns='NAME:.metadata.name,ENV:.metadata.labels.environment,STATUS:.status.phase'
echo ""
kubectl get resourcequota -A --no-headers 2>/dev/null \
  | awk '{printf "  %-28s  %-20s\n", $1, $2}'
log "Done."
