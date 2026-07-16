#!/usr/bin/env bash
# =============================================================================
# deploy-all.sh  —  Master deployment script
# Run on the MASTER node after all workers have joined.
#
# Namespace model:
#   prod       — ALL platform workloads (databases, streaming, analytics,
#                security, catalog, orchestration, MCP servers, registry, openbao)
#   monitoring — Prometheus, Grafana, Prometheus MCP, Grafana MCP
#
# ArgoCD auto-syncs everything from:
#   https://github.com/johnmillermca/stardatadblabs
#
# Secrets are seeded into OpenBao (namespace: prod) and exposed as
# Kubernetes Secrets in the prod namespace via 12-seed-openbao-secrets.sh.
#
# HA components:  OpenSearch 3-node, Spark 1-master+3-workers, Kestra 2-replica,
#                 Schema-Registry 2-replica, OpenSearch-Dashboards 2-replica
# Non-HA (as requested): PostgreSQL, MongoDB, Oracle, Kafka (single broker)
#
# Usage: sudo ./deploy-all.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MASTER_SCRIPTS="${SCRIPT_DIR}/scripts/master"
STORAGE_SCRIPTS="${SCRIPT_DIR}/scripts/storage"
REGISTRY_SCRIPTS="${SCRIPT_DIR}/scripts/registry"

log()     { echo ""; echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━";
            echo "  ▶  $*";
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; }
log_done(){ echo "  ✔  $* done"; }

# Ensure all scripts are executable
chmod +x "${MASTER_SCRIPTS}"/*.sh "${STORAGE_SCRIPTS}"/*.sh \
         "${REGISTRY_SCRIPTS}"/*.sh 2>/dev/null || true

# ── Step 1: Storage ───────────────────────────────────────────────────────────
log "Step 1: Storage discovery"
bash "${STORAGE_SCRIPTS}/04-discover-storage.sh"
log_done "Storage discovery"

log "Step 2: local-path-provisioner"
bash "${STORAGE_SCRIPTS}/05-install-local-path-provisioner.sh"
log_done "local-path-provisioner"

# ── Step 3: Helm ──────────────────────────────────────────────────────────────
log "Step 3: Helm"
bash "${MASTER_SCRIPTS}/08-install-helm.sh"
log_done "Helm"

# Add all required Helm repos
log "Step 3b: Helm repos"
helm repo add argo            https://argoproj.github.io/argo-helm                  2>/dev/null || true
helm repo add bitnami         https://charts.bitnami.com/bitnami                    2>/dev/null || true
helm repo add openbao         https://openbao.github.io/openbao-helm                2>/dev/null || true
helm repo add strimzi         https://strimzi.io/charts/                            2>/dev/null || true
helm repo add akhq            https://akhq.io                                        2>/dev/null || true
helm repo add confluent       https://confluentinc.github.io/cp-helm-charts/        2>/dev/null || true
helm repo add opensearch      https://opensearch-project.github.io/helm-charts/     2>/dev/null || true
helm repo add jupyterhub      https://hub.jupyter.org/helm-chart/                   2>/dev/null || true
helm repo add kestra          https://helm.kestra.io                                 2>/dev/null || true
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
helm repo add grafana         https://grafana.github.io/helm-charts                 2>/dev/null || true
helm repo update
log_done "Helm repos"

# ── Step 4: ArgoCD ────────────────────────────────────────────────────────────
log "Step 4: ArgoCD"
bash "${MASTER_SCRIPTS}/09-deploy-argocd.sh"
log_done "ArgoCD"

# ── Step 5: Namespaces ────────────────────────────────────────────────────────
log "Step 5: Namespaces (prod + monitoring)"
bash "${MASTER_SCRIPTS}/11-namespaces-quotas.sh"
log_done "Namespaces"

# ── Step 6: OpenBao ───────────────────────────────────────────────────────────
log "Step 6: OpenBao (prod secrets engine)"
bash "${MASTER_SCRIPTS}/10-deploy-openbao.sh"
log_done "OpenBao"

# ── Step 7: Seed secrets ──────────────────────────────────────────────────────
log "Step 7: Seed all secrets → OpenBao KV + Kubernetes Secrets in prod"
bash "${MASTER_SCRIPTS}/12-seed-openbao-secrets.sh"
log_done "OpenBao secrets seeded"

# ── Step 8: Apply ArgoCD AppProject + all app manifests ──────────────────────
log "Step 8: Apply ArgoCD project + application manifests"
kubectl apply -f "${SCRIPT_DIR}/argocd-apps/app-project-platform.yaml" -n argocd || true
# Apply prod apps (all workloads → prod namespace, sync-waves 0–9)
kubectl apply -f "${SCRIPT_DIR}/argocd-apps/app-prod.yaml" -n argocd || true
# Apply monitoring apps (prometheus, grafana, MCP servers → monitoring namespace)
kubectl apply -f "${SCRIPT_DIR}/argocd-apps/app-monitoring.yaml" -n argocd || true
log_done "ArgoCD applications applied"

# ── Step 9: Sync infra layer (openbao, registry, kerberos, dbs) ───────────────
log "Step 9: Sync prod infra layer (openbao, registry, kerberos, postgresql, mongodb, oracle)"
for APP in prod-namespaces openbao private-registry kerberos postgresql mongodb oracle; do
  if kubectl get application "${APP}" -n argocd &>/dev/null 2>&1; then
    argocd app sync "${APP}" --prune --timeout 300 2>/dev/null || \
      echo "  ⚠  ${APP} sync non-fatal — check ArgoCD UI"
    argocd app wait "${APP}" --timeout 300 --health 2>/dev/null || \
      echo "  ⚠  ${APP} health check timed out — continuing"
  else
    echo "  ⟳  ${APP} not registered yet — ArgoCD will pick it up on next poll"
  fi
done
log_done "Prod infra layer"

# ── Step 10: Sync streaming layer (Strimzi → Kafka → Schema-Registry → …) ────
log "Step 10: Sync streaming (strimzi-operator → strimzi-kafka → schema-registry → akhq → debezium)"
for APP in strimzi-operator strimzi-kafka schema-registry akhq debezium; do
  if kubectl get application "${APP}" -n argocd &>/dev/null 2>&1; then
    argocd app sync "${APP}" --prune --timeout 300 2>/dev/null || \
      echo "  ⚠  ${APP} sync non-fatal"
    argocd app wait "${APP}" --timeout 300 --health 2>/dev/null || \
      echo "  ⚠  ${APP} health check timed out — continuing"
  else
    echo "  ⟳  ${APP} not registered — continuing"
  fi
done
log_done "Streaming layer"

# ── Step 11: Sync search layer (OpenSearch 3-node HA + Dashboards) ────────────
log "Step 11: Sync search (opensearch 3-node HA + opensearch-dashboards)"
for APP in opensearch opensearch-dashboards; do
  if kubectl get application "${APP}" -n argocd &>/dev/null 2>&1; then
    argocd app sync "${APP}" --prune --timeout 360 2>/dev/null || \
      echo "  ⚠  ${APP} sync non-fatal"
    argocd app wait "${APP}" --timeout 360 --health 2>/dev/null || \
      echo "  ⚠  ${APP} health timed out — continuing"
  else
    echo "  ⟳  ${APP} not registered — continuing"
  fi
done
log_done "Search layer"

# ── Step 12: Sync security + catalog layer ────────────────────────────────────
log "Step 12: Sync security + catalog (ranger, polaris)"
for APP in ranger polaris; do
  if kubectl get application "${APP}" -n argocd &>/dev/null 2>&1; then
    argocd app sync "${APP}" --prune --timeout 300 2>/dev/null || \
      echo "  ⚠  ${APP} sync non-fatal"
    argocd app wait "${APP}" --timeout 300 --health 2>/dev/null || \
      echo "  ⚠  ${APP} health timed out — continuing"
  else
    echo "  ⟳  ${APP} not registered — continuing"
  fi
done
log_done "Security + catalog layer"

# ── Step 13: Sync analytics layer (Doris, Spark HA, JupyterHub, SQLMesh) ─────
log "Step 13: Sync analytics (doris, spark 1-master+3-workers, jupyterhub, sqlmesh)"
for APP in doris spark jupyterhub sqlmesh; do
  if kubectl get application "${APP}" -n argocd &>/dev/null 2>&1; then
    argocd app sync "${APP}" --prune --timeout 360 2>/dev/null || \
      echo "  ⚠  ${APP} sync non-fatal"
    argocd app wait "${APP}" --timeout 360 --health 2>/dev/null || \
      echo "  ⚠  ${APP} health timed out — continuing"
  else
    echo "  ⟳  ${APP} not registered — continuing"
  fi
done
log_done "Analytics layer"

# ── Step 14: Sync orchestration (Kestra HA: 2 replicas) ──────────────────────
log "Step 14: Sync orchestration (kestra 2-replica HA)"
if kubectl get application kestra -n argocd &>/dev/null 2>&1; then
  argocd app sync kestra --prune --timeout 300 2>/dev/null || \
    echo "  ⚠  kestra sync non-fatal"
  argocd app wait kestra --timeout 300 --health 2>/dev/null || \
    echo "  ⚠  kestra health timed out — continuing"
fi
log_done "Orchestration layer"

# ── Step 15: Sync data-platform MCP servers (in prod namespace) ───────────────
log "Step 15: Sync MCP servers (mcp-sqlmesh, mcp-doris, mcp-opensearch, mcp-spark, mcp-kafka)"
for APP in mcp-sqlmesh mcp-doris mcp-opensearch mcp-spark mcp-kafka; do
  if kubectl get application "${APP}" -n argocd &>/dev/null 2>&1; then
    argocd app sync "${APP}" --prune --timeout 120 2>/dev/null || \
      echo "  ⚠  ${APP} sync non-fatal"
  else
    echo "  ⟳  ${APP} not registered — continuing"
  fi
done
log_done "Data platform MCP servers"

# ── Step 16: Sync monitoring stack (monitoring namespace) ─────────────────────
log "Step 16: Sync monitoring stack (prometheus, grafana, mcp-prometheus, mcp-grafana)"
for APP in monitoring-namespace prometheus grafana mcp-prometheus mcp-grafana; do
  if kubectl get application "${APP}" -n argocd &>/dev/null 2>&1; then
    argocd app sync "${APP}" --prune --timeout 300 2>/dev/null || \
      echo "  ⚠  ${APP} sync non-fatal"
    argocd app wait "${APP}" --timeout 300 --health 2>/dev/null || \
      echo "  ⚠  ${APP} health timed out — continuing"
  else
    echo "  ⟳  ${APP} not registered — continuing"
  fi
done
log_done "Monitoring stack"

# ── Final summary ─────────────────────────────────────────────────────────────
echo ""
echo "╔═══════════════════════════════════════════════════════════════════════════╗"
echo "║            StarDataDBLabs Platform — Deployment Complete                  ║"
echo "╠═══════════════════════════════════════════════════════════════════════════╣"
echo "║  CORE                                                                     ║"
echo "║    ArgoCD UI:              https://192.168.1.50:30443                     ║"
echo "║    OpenBao UI:             http://192.168.1.50:30820                      ║"
echo "║    Docker Registry:        https://192.168.1.50:30500                     ║"
echo "╠═══════════════════════════════════════════════════════════════════════════╣"
echo "║  PROD NAMESPACE — Databases                                               ║"
echo "║    PostgreSQL:             postgresql.prod.svc.cluster.local:5432         ║"
echo "║    MongoDB:                mongodb.prod.svc.cluster.local:27017           ║"
echo "║    Oracle XE:              oracle-xe.prod.svc.cluster.local:1521          ║"
echo "║    Kerberos KDC:           kerberos-kdc.prod.svc.cluster.local:88         ║"
echo "╠═══════════════════════════════════════════════════════════════════════════╣"
echo "║  PROD NAMESPACE — Streaming                                               ║"
echo "║    Kafka bootstrap:        strimzi-kafka-kafka-bootstrap.prod:9092        ║"
echo "║    Kafka external:         192.168.1.50:30093                             ║"
echo "║    AKHQ UI:                http://192.168.1.50:30808                      ║"
echo "║    Schema Registry:        http://schema-registry.prod:8081               ║"
echo "║    Debezium:               http://192.168.1.50:30083                      ║"
echo "╠═══════════════════════════════════════════════════════════════════════════╣"
echo "║  PROD NAMESPACE — Search                                                  ║"
echo "║    OpenSearch (3-node HA): opensearch-cluster-master.prod:9200            ║"
echo "║    OpenSearch NodePort:    http://192.168.1.50:30920                      ║"
echo "║    OpenSearch Dashboards:  http://192.168.1.50:30601                      ║"
echo "╠═══════════════════════════════════════════════════════════════════════════╣"
echo "║  PROD NAMESPACE — Security / Catalog                                      ║"
echo "║    Apache Ranger UI:       http://192.168.1.50:30680                      ║"
echo "║    Apache Polaris (REST):  http://192.168.1.50:30181                      ║"
echo "╠═══════════════════════════════════════════════════════════════════════════╣"
echo "║  PROD NAMESPACE — Analytics (HA)                                          ║"
echo "║    Doris FE UI:            http://192.168.1.50:30030                      ║"
echo "║    Doris FE MySQL:         jdbc:mysql://192.168.1.50:30090                ║"
echo "║    Spark Master UI:        http://192.168.1.50:30707  (3 workers)         ║"
echo "║    Spark RPC:              spark://192.168.1.50:30777                     ║"
echo "║    JupyterHub:             http://192.168.1.50:30888                      ║"
echo "║    SQLMesh UI:             http://192.168.1.50:30883                      ║"
echo "╠═══════════════════════════════════════════════════════════════════════════╣"
echo "║  PROD NAMESPACE — Orchestration (HA: 2 replicas)                          ║"
echo "║    Kestra UI:              http://192.168.1.50:30880                      ║"
echo "╠═══════════════════════════════════════════════════════════════════════════╣"
echo "║  MONITORING NAMESPACE — Observability                                     ║"
echo "║    Prometheus UI:          http://192.168.1.50:30990                      ║"
echo "║    Alertmanager:           http://192.168.1.50:30993                      ║"
echo "║    Grafana UI:             http://192.168.1.50:30300                      ║"
echo "║    Prometheus MCP:         http://192.168.1.50:30320/mcp                  ║"
echo "║    Grafana MCP:            http://192.168.1.50:30321/mcp                  ║"
echo "╚═══════════════════════════════════════════════════════════════════════════╝"
echo ""
kubectl get nodes -o wide
echo ""
echo "=== ArgoCD Applications ==="
kubectl get applications -n argocd -o wide 2>/dev/null || true
echo ""
echo "=== Unhealthy pods ==="
kubectl get pods -n prod       | grep -v "Running\|Completed\|NAME" || echo "  All prod pods healthy."
kubectl get pods -n monitoring | grep -v "Running\|Completed\|NAME" || echo "  All monitoring pods healthy."
