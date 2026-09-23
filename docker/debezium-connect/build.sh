#!/usr/bin/env bash
# =============================================================================
# docker/debezium-connect/build.sh
#
# (Re)builds the LowerCaseTopicNamingStrategy JAR from source and tags/pushes
# a new debezium/connect image to the local registry.
#
# Prerequisites:
#   - podman available on the build host
#   - Access to 192.168.1.50:30500 (local registry, TLS, no auth)
#
# Usage:
#   cd docker/debezium-connect
#   bash build.sh [--push]          # --push also pushes to registry
#
# The compiled JAR is committed to git so the Dockerfile does not require a JDK
# at image build time.  Re-run this script only when the Java source changes.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGISTRY="192.168.1.50:30500"
IMAGE_BASE="debezium/connect"
DBZ_VERSION="2.7.4"
TAG_PINNED="${REGISTRY}/${IMAGE_BASE}:${DBZ_VERSION}-lc1"
TAG_FLOAT="${REGISTRY}/${IMAGE_BASE}:2.7"
JAR_NAME="debezium-lowercase-topic-naming-${DBZ_VERSION}.jar"
BASE_IMAGE="quay.io/debezium/connect:2.7"

echo "=== 1/4  Compile LowerCaseTopicNamingStrategy ==="
podman run --rm \
  -v "${SCRIPT_DIR}:/work:z" \
  eclipse-temurin:21-jdk \
  bash -c "
set -e
# Extract compile-time deps from the base Debezium image
TEMP_CONTAINER=\$(podman create ${BASE_IMAGE} true 2>/dev/null || true)
EXTRACT_DIR=/tmp/dbz_deps
mkdir -p \$EXTRACT_DIR

if [ -n \"\$TEMP_CONTAINER\" ]; then
  podman cp \$TEMP_CONTAINER:/kafka/connect/debezium-connector-oracle/debezium-core-${DBZ_VERSION}.Final.jar \$EXTRACT_DIR/ 2>/dev/null || true
  podman cp \$TEMP_CONTAINER:/kafka/connect/debezium-connector-oracle/debezium-api-${DBZ_VERSION}.Final.jar  \$EXTRACT_DIR/ 2>/dev/null || true
  podman rm \$TEMP_CONTAINER >/dev/null 2>&1 || true
fi

# Fall back to pre-copied JARs in /work if extraction failed
if [ ! -f \$EXTRACT_DIR/debezium-core-${DBZ_VERSION}.Final.jar ]; then
  echo '[WARN] Could not extract JARs from base image — using pre-copied JARs in /work'
  EXTRACT_DIR=/work
fi

mkdir -p /work/out
javac \
  -cp \"\$EXTRACT_DIR/debezium-core-${DBZ_VERSION}.Final.jar:\$EXTRACT_DIR/debezium-api-${DBZ_VERSION}.Final.jar\" \
  -source 11 -target 11 \
  -d /work/out \
  /work/LowerCaseTopicNamingStrategy.java

jar cf /work/${JAR_NAME} -C /work/out .
echo 'JAR built: /work/${JAR_NAME}'
"

ls -lh "${SCRIPT_DIR}/${JAR_NAME}"
echo "=== 2/4  Build image ==="
podman build \
  --tls-verify=false \
  -t "${TAG_PINNED}" \
  -t "${TAG_FLOAT}" \
  "${SCRIPT_DIR}"

echo "=== 3/4  Verify plugin class is present ==="
podman run --rm "${TAG_PINNED}" \
  unzip -l /kafka/connect/debezium-connector-oracle/${JAR_NAME} \
  | grep LowerCaseTopicNamingStrategy

if [[ "${1:-}" == "--push" ]]; then
  echo "=== 4/4  Push to registry ==="
  podman push --tls-verify=false "${TAG_PINNED}"
  podman push --tls-verify=false "${TAG_FLOAT}"
  echo "Pushed: ${TAG_PINNED}"
  echo "Pushed: ${TAG_FLOAT}"
else
  echo "=== 4/4  Skipped push (pass --push to enable) ==="
  echo "Images built locally:"
  echo "  ${TAG_PINNED}"
  echo "  ${TAG_FLOAT}"
fi

echo ""
echo "Done. Update manifests/debezium/debezium-deployment.yaml image tag if pinning."
