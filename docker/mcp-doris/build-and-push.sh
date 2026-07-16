#!/usr/bin/env bash
set -euo pipefail
REGISTRY="192.168.1.50:30500"
IMAGE="${REGISTRY}/mcp-doris:1.0.0"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "[$(date '+%H:%M:%S')] Building Doris MCP server: ${IMAGE}"
docker build -t "${IMAGE}" "${SCRIPT_DIR}"
docker push "${IMAGE}"
echo "[$(date '+%H:%M:%S')] Done: ${IMAGE}"
