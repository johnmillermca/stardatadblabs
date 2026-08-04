#!/usr/bin/env bash
# Build and push krb-spark-guard to private registry
set -euo pipefail
REGISTRY="192.168.1.50:30500"
IMAGE="${REGISTRY}/krb-spark-guard:1.0.0"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Building $IMAGE"
podman build --tls-verify=false -t "$IMAGE" "$SCRIPT_DIR"
echo "==> Pushing $IMAGE"
podman push --tls-verify=false "$IMAGE"
echo "==> Done: $IMAGE"
