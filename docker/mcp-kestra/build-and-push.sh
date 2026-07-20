#!/usr/bin/env bash
set -euo pipefail
REGISTRY="192.168.1.50:30500"
IMAGE="${REGISTRY}/mcp-kestra:1.0.0"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "[$(date '+%H:%M:%S')] Building Kestra MCP server: ${IMAGE}"
podman build -t "${IMAGE}" "${SCRIPT_DIR}"
podman push --tls-verify=false "${IMAGE}"
echo "[$(date '+%H:%M:%S')] Done: ${IMAGE}"
