#!/usr/bin/env bash
# =============================================================================
# build-and-push-custom-images.sh
#
# Builds all custom platform images from docker/ and pushes them to the
# private registry at 192.168.1.50:30500.
#
# Usage:
#   bash scripts/master/build-and-push-custom-images.sh           # build all
#   bash scripts/master/build-and-push-custom-images.sh ranger     # single image
#   bash scripts/master/build-and-push-custom-images.sh ranger polaris
#
# Safe to re-run — podman build uses layer cache, skips unchanged layers.
# =============================================================================
set -euo pipefail

REGISTRY="192.168.1.50:30500"
OPTS="--tls-verify=false"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "  ✓ $*"; }
fail() { echo "  ✗ FAILED: $*" >&2; FAILURES+=("$*"); }

FAILURES=()

# =============================================================================
# Image definitions — add new images here
# Format: build_image "<name>" "<context-dir>" "<registry-path>:<tag>"
# =============================================================================
declare -A IMAGE_CONTEXT=(
  [ranger]="docker/ranger"
  [polaris]="docker/polaris"
  [sqlmesh]="docker/sqlmesh"
  [jupyter-spark]="docker/jupyter-spark"
)

declare -A IMAGE_TAG=(
  [ranger]="${REGISTRY}/apache-ranger:2.7.0"
  [polaris]="${REGISTRY}/apache-polaris:1.6.0"
  [sqlmesh]="${REGISTRY}/sqlmesh:0.99.0"
  [jupyter-spark]="${REGISTRY}/jupyter-spark:latest"
)

# =============================================================================
build_image() {
  local name="$1"
  local context="${REPO_ROOT}/${IMAGE_CONTEXT[$name]}"
  local tag="${IMAGE_TAG[$name]}"

  if [[ ! -d "${context}" ]]; then
    fail "${name}: context dir not found: ${context}"
    return
  fi

  log "Building ${name} → ${tag}"
  if ! podman build \
      --tag "${tag}" \
      "${context}" 2>&1; then
    fail "${name}: build failed"
    return
  fi

  log "Pushing  ${tag}"
  if ! podman push ${OPTS} "${tag}" 2>&1 | tail -2; then
    fail "${name}: push failed"
    return
  fi

  # Remove local image to free disk space
  podman rmi "${tag}" 2>/dev/null || true
  ok "${name} → ${tag}"
}

# =============================================================================
# Determine which images to build
# =============================================================================
if [[ $# -gt 0 ]]; then
  TARGETS=("$@")
else
  TARGETS=("ranger" "polaris" "sqlmesh" "jupyter-spark")
fi

log "=== Building ${#TARGETS[@]} custom image(s) ==="
echo ""

for name in "${TARGETS[@]}"; do
  if [[ -z "${IMAGE_CONTEXT[$name]+_}" ]]; then
    echo "  Unknown image: ${name}"
    echo "  Valid names: ${!IMAGE_CONTEXT[*]}"
    exit 1
  fi
  build_image "${name}"
  echo ""
done

# =============================================================================
log "=== Registry contents after build ==="
curl -sk "https://${REGISTRY}/v2/_catalog" | python3 -m json.tool

echo ""
if [[ ${#FAILURES[@]} -eq 0 ]]; then
  log "=== All custom images built and pushed successfully ==="
else
  log "=== ${#FAILURES[@]} image(s) FAILED ==="
  for f in "${FAILURES[@]}"; do
    echo "  ✗ ${f}"
  done
  exit 1
fi
