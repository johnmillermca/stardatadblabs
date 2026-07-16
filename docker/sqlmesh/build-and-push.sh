#!/usr/bin/env bash
# =============================================================================
# Build and push SQLMesh 0.99.0 image to local registry
# =============================================================================
set -euo pipefail

REGISTRY="192.168.1.50:30500"
IMAGE="${REGISTRY}/sqlmesh:0.99.0"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "[$(date '+%H:%M:%S')] Building SQLMesh image: ${IMAGE}"
docker build -t "${IMAGE}" "${SCRIPT_DIR}"
echo "[$(date '+%H:%M:%S')] Pushing ${IMAGE}"
docker push "${IMAGE}"
echo "[$(date '+%H:%M:%S')] Done: ${IMAGE}"
