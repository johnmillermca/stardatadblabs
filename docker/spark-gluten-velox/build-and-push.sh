#!/usr/bin/env bash
# =============================================================================
# build-and-push.sh — Build spark-gluten-velox image and push to private registry
# Usage:
#   bash docker/spark-gluten-velox/build-and-push.sh          # normal build
#   bash docker/spark-gluten-velox/build-and-push.sh --no-cache
#   bash docker/spark-gluten-velox/build-and-push.sh --air-gap  # offline build
# =============================================================================
set -euo pipefail

REGISTRY="192.168.1.50:30500"
IMAGE_NAME="spark-gluten-velox"
TAG="3.5.1"
FULL_IMAGE="${REGISTRY}/${IMAGE_NAME}:${TAG}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
die()  { echo "[ERROR] $*" >&2; exit 1; }

# Detect container runtime
if command -v docker &>/dev/null; then
  RUNTIME="docker"
elif command -v podman &>/dev/null; then
  RUNTIME="podman"
else
  die "Neither docker nor podman found in PATH"
fi
log "Container runtime: ${RUNTIME}"

BUILD_ARGS=()
DOCKERFILE="${SCRIPT_DIR}/Dockerfile"

# Handle flags
NO_CACHE=false
AIR_GAP=false
for arg in "$@"; do
  case "${arg}" in
    --no-cache) NO_CACHE=true ;;
    --air-gap)  AIR_GAP=true ;;
    *) die "Unknown flag: ${arg}  (valid: --no-cache, --air-gap)" ;;
  esac
done

if [[ "${AIR_GAP}" == true ]]; then
  JARS_DIR="${SCRIPT_DIR}/jars"
  [[ -d "${JARS_DIR}" ]] || die "--air-gap requires jars/ directory next to Dockerfile with pre-staged jars"
  # Rewrite Dockerfile to COPY jars instead of curling from internet
  DOCKERFILE="${SCRIPT_DIR}/Dockerfile.airgap"
  sed 's|RUN curl.*-o "/opt/spark/jars/\(.*\)" .*|COPY jars/\1 /opt/spark/jars/\1|g' \
    "${SCRIPT_DIR}/Dockerfile" > "${DOCKERFILE}"
  log "Air-gap Dockerfile written to ${DOCKERFILE}"
fi

[[ "${NO_CACHE}" == true ]] && BUILD_ARGS+=(--no-cache)

log "Building ${FULL_IMAGE} ..."
"${RUNTIME}" build \
  "${BUILD_ARGS[@]}" \
  -f "${DOCKERFILE}" \
  -t "${FULL_IMAGE}" \
  "${SCRIPT_DIR}"

log "Pushing ${FULL_IMAGE} to registry..."
"${RUNTIME}" push "${FULL_IMAGE}"

log "Done. Image available at: ${FULL_IMAGE}"
log "Verify: ${RUNTIME} pull ${FULL_IMAGE}"
