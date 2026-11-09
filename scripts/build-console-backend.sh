#!/usr/bin/env bash
# =============================================================================
# build-console-backend.sh
#
# Builds the console-backend Go binary, packages it into a Docker image and
# pushes it to the private registry.  Then bumps the image tag in the
# Kubernetes Deployment manifest and restarts the rollout.
#
# Usage:
#   cd /home/starmaster/k8s_platform
#   bash scripts/build-console-backend.sh [VERSION]
#
# If VERSION is omitted the script reads the current tag from the manifest and
# increments the patch number (e.g. 1.0.16 → 1.0.17).
#
# Requirements:
#   - Go 1.23+ on PATH (cross-compiled for linux/amd64)
#   - docker on PATH and access to 192.168.1.50:30500
#   - kubectl configured for the cluster
# =============================================================================
set -euo pipefail

REGISTRY="192.168.1.50:30500"
IMAGE_NAME="stardatadb/console-backend"
MANIFEST="manifests/access/console-backend.yaml"
SRC_DIR="stardata-access/console-backend"
BINARY_PATH="${SRC_DIR}/console-backend-bin"

# ── Resolve version ───────────────────────────────────────────────────────────
if [[ -n "${1:-}" ]]; then
  NEW_VERSION="$1"
else
  CURRENT=$(grep -oP "console-backend:\K[0-9]+\.[0-9]+\.[0-9]+" "${MANIFEST}" | head -1)
  IFS='.' read -r major minor patch <<< "${CURRENT}"
  NEW_VERSION="${major}.${minor}.$((patch + 1))"
fi

NEW_TAG="${REGISTRY}/${IMAGE_NAME}:${NEW_VERSION}"

echo "══════════════════════════════════════════════"
echo "  Building console-backend ${NEW_VERSION}"
echo "══════════════════════════════════════════════"

# ── 1. Build Go binary (linux/amd64 static binary) ───────────────────────────
echo "▶ Building Go binary…"
cd "${SRC_DIR}"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -ldflags="-s -w -X main.version=${NEW_VERSION}" \
  -o console-backend-bin ./cmd/server
echo "  ✔ Binary built: $(du -sh console-backend-bin | cut -f1)"
cd - > /dev/null

# ── 2. Build Docker image ─────────────────────────────────────────────────────
echo "▶ Building Docker image ${NEW_TAG}…"
docker build \
  --platform linux/amd64 \
  -t "${NEW_TAG}" \
  -f "${SRC_DIR}/Dockerfile" \
  "${SRC_DIR}"

# ── 3. Push to registry ───────────────────────────────────────────────────────
echo "▶ Pushing image…"
docker push "${NEW_TAG}"
echo "  ✔ Pushed ${NEW_TAG}"

# ── 4. Update manifest ────────────────────────────────────────────────────────
echo "▶ Updating manifest image tag…"
CURRENT_TAG=$(grep -oP "console-backend:\K[0-9]+\.[0-9]+\.[0-9]+" "${MANIFEST}" | head -1)
sed -i "s|console-backend:${CURRENT_TAG}|console-backend:${NEW_VERSION}|g" "${MANIFEST}"
echo "  ✔ Manifest updated: ${CURRENT_TAG} → ${NEW_VERSION}"

# ── 5. Also build & push the webconsole static image (nginx + HTML) ──────────
echo "▶ Building webconsole image…"
WEBCONSOLE_TAG="${REGISTRY}/webconsole:latest"
docker build \
  --platform linux/amd64 \
  -t "${WEBCONSOLE_TAG}" \
  -f docker/webconsole/Dockerfile \
  docker/webconsole/
docker push "${WEBCONSOLE_TAG}"
echo "  ✔ Pushed ${WEBCONSOLE_TAG}"

# ── 6. Apply and restart ──────────────────────────────────────────────────────
echo "▶ Applying updated manifest…"
kubectl apply -f "${MANIFEST}"
kubectl rollout restart deployment/console-backend -n access
kubectl rollout status  deployment/console-backend -n access --timeout=120s

echo "▶ Restarting webconsole-proxy pod (picks up new image)…"
kubectl delete pod webconsole-proxy -n prod --ignore-not-found=true
sleep 5
kubectl wait --for=condition=Ready pod -l app=webconsole-proxy -n prod --timeout=60s || \
  echo "  ⚠ webconsole-proxy readiness wait timed out — check with: kubectl get pod -n prod"

echo ""
echo "╔════════════════════════════════════════════════════╗"
echo "║  console-backend ${NEW_VERSION} deployed successfully    ║"
echo "║  webconsole (nginx+HTML) also updated              ║"
echo "╚════════════════════════════════════════════════════╝"
