#!/usr/bin/env bash
# =============================================================================
# 13-seed-private-registry.sh
#
# Pulls all platform images from the internet and pushes them into the
# private registry at 192.168.1.50:30500.
#
# Images are organised by component. Each entry is:
#   <source-image>  <dest-tag-in-registry>
#
# Usage: bash scripts/master/13-seed-private-registry.sh
# Safe to re-run — skips images already present in the registry.
# =============================================================================
set -euo pipefail

REGISTRY="192.168.1.50:30500"
PODMAN_OPTS="--tls-verify=false"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "  ✓ $*"; }
skip() { echo "  – $* (already in registry — skipping)"; }
fail() { echo "  ✗ FAILED: $*" >&2; FAILURES+=("$*"); }

FAILURES=()

# Check if an image already exists in the registry
image_exists() {
  local repo="${1%:*}"   # strip tag
  local tag="${1##*:}"
  curl -sf --max-time 5 \
    "https://${REGISTRY}/v2/${repo}/manifests/${tag}" \
    -H "Accept: application/vnd.docker.distribution.manifest.v2+json" \
    -o /dev/null 2>/dev/null
}

push_image() {
  local src="$1"   # e.g. docker.io/bitnami/kafka:4.0
  local dest="$2"  # e.g. bitnami/kafka:4.0  (no registry prefix)
  local full_dest="${REGISTRY}/${dest}"

  if image_exists "${dest}"; then
    skip "${dest}"
    return
  fi

  log "Pulling  ${src}"
  if ! podman pull ${PODMAN_OPTS} "${src}" 2>&1 | tail -1; then
    fail "pull ${src}"
    return
  fi

  log "Tagging  ${src} → ${full_dest}"
  podman tag "${src}" "${full_dest}"

  log "Pushing  ${full_dest}"
  if ! podman push ${PODMAN_OPTS} "${full_dest}" 2>&1 | tail -1; then
    fail "push ${full_dest}"
    return
  fi

  # Clean up local copy to save disk
  podman rmi "${src}" "${full_dest}" 2>/dev/null || true
  ok "${dest}"
}

# =============================================================================
# IMAGE LIST
# Format: push_image "<upstream-source>" "<dest-path-in-registry>"
# =============================================================================

log "=== Infrastructure / base ==="

# OpenBao (already in use — ensure it is cached)
push_image "quay.io/openbao/openbao:2.6.0"                      "openbao/openbao:2.6.0"
push_image "docker.io/hashicorp/vault-k8s:1.7.2"                 "hashicorp/vault-k8s:1.7.2"

log "=== Kafka / Streaming ==="

# Bitnami Kafka (KRaft, 3-broker HA)
push_image "docker.io/apache/kafka:3.9.0"                         "apache/kafka:3.9.0"

# Strimzi operator + kafka images
push_image "quay.io/strimzi/operator:1.1.0"                      "strimzi/operator:1.1.0"
push_image "quay.io/strimzi/kafka:latest-kafka-4.2.0"             "strimzi/kafka:latest-kafka-4.2.0"
push_image "quay.io/strimzi/operator:latest"                     "strimzi/operator:latest"

# Confluent Schema Registry
push_image "docker.io/confluentinc/cp-schema-registry:7.9.0"    "confluentinc/cp-schema-registry:7.9.0"

# AKHQ Kafka UI
push_image "docker.io/tchiotludo/akhq:0.27.0"                     "tchiotludo/akhq:0.27.0"

log "=== Databases (master-pinned — keep as fallback) ==="

# PostgreSQL (bitnami)
# bitnami images require paid subscription — use official images instead
push_image "docker.io/library/postgres:17"                        "library/postgres:17"

# MongoDB (bitnami)
push_image "docker.io/library/mongo:8.0"                          "library/mongo:8.0"

# Oracle XE (gvenzl — master-pinned, but cache locally)
push_image "docker.io/gvenzl/oracle-xe:21-slim"                  "gvenzl/oracle-xe:21-slim"

log "=== Search ==="

# OpenSearch 3-node cluster
push_image "docker.io/opensearchproject/opensearch:3.7.0"        "opensearch/opensearch:3.7.0"

# OpenSearch Dashboards
push_image "docker.io/opensearchproject/opensearch-dashboards:3.7.0" "opensearch/opensearch-dashboards:3.7.0"

log "=== Analytics ==="

# Apache Spark (bitnami chart uses bitnami/spark — custom image is handled separately)
push_image "docker.io/apache/spark:3.5.1"                         "apache/spark:3.5.1"

# Doris FE + BE
push_image "docker.io/apache/doris:2.1.0-fe-x86_64"              "apache/doris:2.1.0-fe"
push_image "docker.io/apache/doris:2.1.0-be-x86_64"              "apache/doris:2.1.0-be"

log "=== Orchestration ==="

# Kestra
push_image "docker.io/kestra/kestra:latest-lts"                  "kestra/kestra:latest-lts"

log "=== Monitoring ==="

# Prometheus (kube-prometheus-stack uses its own images — cache key ones)
push_image "quay.io/prometheus/prometheus:v2.53.0"               "prometheus/prometheus:v2.53.0"
push_image "quay.io/prometheus/alertmanager:v0.27.0"             "prometheus/alertmanager:v0.27.0"
push_image "quay.io/prometheus-operator/prometheus-operator:v0.75.0" "prometheus-operator/prometheus-operator:v0.75.0"
push_image "registry.k8s.io/kube-state-metrics/kube-state-metrics:v2.13.0" "kube-state-metrics/kube-state-metrics:v2.13.0"
push_image "docker.io/prom/node-exporter:v1.8.1"                 "prom/node-exporter:v1.8.1"

# Grafana
push_image "docker.io/grafana/grafana:11.1.0"                    "grafana/grafana:11.1.0"

log "=== Jupyter ==="

# JupyterHub
push_image "quay.io/jupyterhub/k8s-hub:4.4.0"                   "jupyterhub/k8s-hub:4.4.0"
push_image "quay.io/jupyterhub/configurable-http-proxy:4.6.2"     "jupyterhub/configurable-http-proxy:4.6.2"

log "=== Security / Catalog ==="

# Apache Ranger (custom — must be built and pushed separately)
# Image: 192.168.1.50:30500/apache-ranger:2.7.0
# Build: docker build -t 192.168.1.50:30500/apache-ranger:2.7.0 manifests/ranger/
log "  NOTE: apache-ranger:2.7.0 is a custom image — build and push manually:"
log "        podman build -t ${REGISTRY}/apache-ranger:2.7.0 manifests/ranger/"
log "        podman push --tls-verify=false ${REGISTRY}/apache-ranger:2.7.0"

# Apache Polaris (custom — must be built and pushed separately)
log "  NOTE: apache-polaris:latest is a custom image — build and push manually:"
log "        podman build -t ${REGISTRY}/apache-polaris:latest manifests/polaris/"
log "        podman push --tls-verify=false ${REGISTRY}/apache-polaris:latest"

# SQLMesh (custom image with spark connector)
log "  NOTE: sqlmesh:0.99.0 is a custom image — build and push manually:"
log "        podman build -t ${REGISTRY}/sqlmesh:0.99.0 manifests/sqlmesh/"
log "        podman push --tls-verify=false ${REGISTRY}/sqlmesh:0.99.0"

# Jupyter+Spark singleuser (custom)
log "  NOTE: jupyter-spark:latest is a custom singleuser image — build and push manually"

# =============================================================================
# SUMMARY
# =============================================================================
echo ""
log "=== Verifying registry contents ==="
curl -sk "https://${REGISTRY}/v2/_catalog" | python3 -m json.tool

echo ""
if [[ ${#FAILURES[@]} -eq 0 ]]; then
  log "=== All images pushed successfully ==="
else
  log "=== ${#FAILURES[@]} image(s) FAILED ==="
  for f in "${FAILURES[@]}"; do
    echo "  ✗ ${f}"
  done
  echo ""
  log "Re-run this script to retry failed images."
  exit 1
fi
