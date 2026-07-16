#!/usr/bin/env bash
# =============================================================================
# 07-staging-testing-setup.sh
# Run on MASTER. Creates:
#   - /opt/k8s-builds/staging   – where CI/CD drops finished build artifacts
#   - /opt/k8s-builds/testing   – active development / test builds
#   - Kubernetes namespaces: staging, testing
#   - Helper scripts: promote-to-registry.sh, promote-to-staging.sh
# =============================================================================
set -euo pipefail

REGISTRY_HOST="192.168.1.50"
REGISTRY_PORT="5000"
BASE_DIR="/opt/k8s-builds"
STAGING_DIR="${BASE_DIR}/staging"
TESTING_DIR="${BASE_DIR}/testing"
CERT_DIR="/etc/k8s-registry-certs"
BUILD_USER="${SUDO_USER:-build}"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# ── 1. Directory structure ────────────────────────────────────────────────────
log "Creating build directory tree..."
mkdir -p \
  "${STAGING_DIR}"/{artifacts,images,manifests,logs} \
  "${TESTING_DIR}"/{artifacts,images,manifests,logs,workspace}

# Create build user if missing
if ! id "${BUILD_USER}" &>/dev/null; then
  useradd -r -s /bin/bash -d "${BASE_DIR}" "${BUILD_USER}" || true
fi

chown -R "${BUILD_USER}:${BUILD_USER}" "${BASE_DIR}" || true
chmod 2775 "${STAGING_DIR}" "${TESTING_DIR}"

# ── 2. Kubernetes namespaces ──────────────────────────────────────────────────
log "Creating Kubernetes namespaces..."
kubectl apply -f - <<'YAML'
apiVersion: v1
kind: Namespace
metadata:
  name: staging
  labels:
    environment: staging
    managed-by: platform
---
apiVersion: v1
kind: Namespace
metadata:
  name: testing
  labels:
    environment: testing
    managed-by: platform
YAML

# ── 3. Write promote-to-registry helper ──────────────────────────────────────
cat >"${STAGING_DIR}/promote-to-registry.sh" <<SCRIPT
#!/usr/bin/env bash
# Usage: promote-to-registry.sh <image-name> <tag> <staging-tar>
# Pushes a docker image tarball from staging to the private registry.
set -euo pipefail
IMAGE="\${1:?Usage: \$0 <name> <tag> <path-to-tar>}"
TAG="\${2:?}"
TAR="\${3:?}"
REGISTRY="${REGISTRY_HOST}:${REGISTRY_PORT}"
echo "Loading image from \${TAR}..."
docker load < "\${TAR}"
docker tag "\${IMAGE}:\${TAG}" "\${REGISTRY}/\${IMAGE}:\${TAG}"
docker push "\${REGISTRY}/\${IMAGE}:\${TAG}"
echo "Pushed \${REGISTRY}/\${IMAGE}:\${TAG}"
SCRIPT
chmod +x "${STAGING_DIR}/promote-to-registry.sh"

# ── 4. Write promote-to-staging helper ───────────────────────────────────────
cat >"${TESTING_DIR}/promote-to-staging.sh" <<SCRIPT
#!/usr/bin/env bash
# Usage: promote-to-staging.sh <image-name> <tag>
# Tags and copies a local test image to the staging directory.
set -euo pipefail
IMAGE="\${1:?Usage: \$0 <name> <tag>}"
TAG="\${2:?}"
DEST="${STAGING_DIR}/images"
echo "Saving \${IMAGE}:\${TAG} to staging..."
mkdir -p "\${DEST}"
docker save "\${IMAGE}:\${TAG}" -o "\${DEST}/\${IMAGE//\//_}-\${TAG}.tar"
echo "Saved to \${DEST}/\${IMAGE//\//_}-\${TAG}.tar"
SCRIPT
chmod +x "${TESTING_DIR}/promote-to-staging.sh"

# ── 5. Summary ────────────────────────────────────────────────────────────────
log "Directory setup complete."
echo ""
echo "  Testing  → ${TESTING_DIR}"
echo "             └── workspace/  (clone repos here)"
echo "             └── images/     (built image tarballs)"
echo "             └── promote-to-staging.sh"
echo ""
echo "  Staging  → ${STAGING_DIR}"
echo "             └── images/     (validated image tarballs)"
echo "             └── promote-to-registry.sh"
echo ""
kubectl get namespaces testing staging
