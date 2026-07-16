#!/usr/bin/env bash
# =============================================================================
# Build & push mcp-prometheus Docker image
# Registry: 192.168.1.50:30500
# Usage:    bash docker/mcp-prometheus/build-and-push.sh
# =============================================================================
set -euo pipefail

REGISTRY="192.168.1.50:30500"
IMAGE="mcp-prometheus"
TAG="1.0.0"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Building ${REGISTRY}/${IMAGE}:${TAG} ..."
docker build -t "${REGISTRY}/${IMAGE}:${TAG}" "${SCRIPT_DIR}"

echo "Pushing ${REGISTRY}/${IMAGE}:${TAG} ..."
docker push "${REGISTRY}/${IMAGE}:${TAG}"

echo "Done: ${REGISTRY}/${IMAGE}:${TAG}"
