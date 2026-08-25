#!/usr/bin/env bash
# ============================================================
# star-knowledge-catalog — build and push Docker image
# ============================================================
set -euo pipefail

REGISTRY="192.168.1.50:30500"
IMAGE="star-knowledge-catalog"
TAG="${1:-1.0.0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"

echo "==> Building $REGISTRY/$IMAGE:$TAG"
docker build \
  -t "$REGISTRY/$IMAGE:$TAG" \
  -f "$SCRIPT_DIR/Dockerfile" \
  "$ROOT"

echo "==> Pushing $REGISTRY/$IMAGE:$TAG"
docker push "$REGISTRY/$IMAGE:$TAG"

echo "==> Done: $REGISTRY/$IMAGE:$TAG"
