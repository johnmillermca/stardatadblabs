#!/usr/bin/env bash
# =============================================================================
# 05-install-local-path-provisioner.sh
# Run on MASTER after all nodes have run 04-discover-storage.sh.
# Installs Rancher local-path-provisioner and creates the StorageClass.
# =============================================================================
set -euo pipefail

LPP_VERSION="v0.0.28"
NAMESPACE="local-path-storage"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# ── Install local-path-provisioner via its manifest ──────────────────────────
log "Installing local-path-provisioner ${LPP_VERSION}..."
kubectl apply -f \
  "https://raw.githubusercontent.com/rancher/local-path-provisioner/${LPP_VERSION}/deploy/local-path-storage.yaml"

# ── Wait for rollout ──────────────────────────────────────────────────────────
log "Waiting for local-path-provisioner pod to be ready..."
kubectl rollout status deployment/local-path-provisioner \
  -n "${NAMESPACE}" --timeout=120s

# ── Patch ConfigMap with discovered paths from all nodes ─────────────────────
log "Patching ConfigMap with node storage paths..."

# Collect paths from each node via SSH (or set manually)
# Default: use /opt/local-path-provisioner which was created by 04-discover-storage.sh
# For customized paths per node, edit the nodePathMap below.

kubectl apply -f - <<'YAML'
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-path-config
  namespace: local-path-storage
data:
  config.json: |
    {
      "nodePathMap": [
        {
          "node": "DEFAULT_PATH_FOR_NON_LISTED_NODES",
          "paths": ["/opt/local-path-provisioner"]
        }
      ]
    }
  setup: |
    #!/bin/sh
    set -eu
    mkdir -m 0777 -p "$VOL_DIR"
  teardown: |
    #!/bin/sh
    set -eu
    rm -rf "$VOL_DIR"
  helperPod.yaml: |
    apiVersion: v1
    kind: Pod
    metadata:
      name: helper-pod
    spec:
      containers:
      - name: helper-pod
        image: busybox
        imagePullPolicy: IfNotPresent
YAML

# ── Apply StorageClass manifest ───────────────────────────────────────────────
kubectl apply -f /dev/stdin <<'YAML'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: local-path
  annotations:
    storageclass.kubernetes.io/is-default-class: "true"
provisioner: rancher.io/local-path
volumeBindingMode: WaitForFirstConsumer
reclaimPolicy: Delete
YAML

log "Verifying StorageClass..."
kubectl get storageclass

log "local-path-provisioner ready. Default StorageClass: local-path"
