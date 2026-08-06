#!/usr/bin/env bash
# Build and push the RBAC Control Plane Docker image.
set -euo pipefail

REGISTRY="192.168.1.50:30500"
IMAGE="${REGISTRY}/rbac-plane"
TAG="${1:-1.0.0}"

cd "$(dirname "$0")/.."

echo "==> Building rbac-plane:${TAG}..."
podman build --platform linux/amd64 \
  -t "${IMAGE}:${TAG}" \
  -t "${IMAGE}:latest" \
  -f docker/Dockerfile \
  .

echo "==> Pushing ${IMAGE}:${TAG}..."
podman push "${IMAGE}:${TAG}"
podman push "${IMAGE}:latest"

echo "✓ rbac-plane:${TAG} pushed to ${REGISTRY}"
