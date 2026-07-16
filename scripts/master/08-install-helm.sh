#!/usr/bin/env bash
# =============================================================================
# 08-install-helm.sh
# Run on MASTER. Installs Helm v3.
# =============================================================================
set -euo pipefail

HELM_VERSION="v3.15.2"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

if command -v helm &>/dev/null; then
  log "Helm already installed: $(helm version --short)"
  exit 0
fi

log "Installing Helm ${HELM_VERSION}..."
export PATH="/usr/local/bin:${PATH}"
curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | \
  DESIRED_VERSION="${HELM_VERSION}" bash

log "Adding common Helm repos..."
helm repo add stable        https://charts.helm.sh/stable       2>/dev/null || true
helm repo add argo          https://argoproj.github.io/argo-helm
helm repo add openbao       https://openbao.github.io/openbao-helm
helm repo add rancher-stable https://releases.rancher.com/server-charts/stable 2>/dev/null || true
helm repo update

log "Helm ready: $(helm version --short)"
