# Runbook 02 — ArgoCD & Kubernetes Core

> **Cluster:** `192.168.1.50` (master) + workers `.51–.54` · **K8s version:** 1.30  
> **ArgoCD UI:** `https://192.168.1.50:30443` · **Helm chart:** `argo/argo-cd 6.x`

---

## 1. What Is ArgoCD?

ArgoCD is a **declarative GitOps continuous delivery tool** for Kubernetes. It watches a Git repository and continuously reconciles the cluster state with what is declared in Git. Any `kubectl apply` or manual edit that drifts from Git is automatically reverted.

### Key Concepts

| Term | Meaning |
|---|---|
| **Application** | An ArgoCD resource that maps a Git path → a Kubernetes namespace |
| **AppProject** | Groups applications and restricts which repos/namespaces they can target |
| **Sync** | The act of applying Git manifests to the cluster |
| **Sync Wave** | Ordering annotation (`argocd.argoproj.io/sync-wave`) — lower waves deploy first |
| **Self-Heal** | ArgoCD reverts manual cluster changes back to Git state |
| **Prune** | ArgoCD deletes K8s resources that were removed from Git |
| **Health** | ArgoCD checks pod readiness, deployment rollout status, etc. |

---

## 2. Platform Applications

All platform applications are defined in `argocd-apps/`:

| Application | Namespace | Source | Wave |
|---|---|---|---|
| `platform` (AppProject) | `argocd` | `argocd-apps/app-project-platform.yaml` | — |
| `storage` | `local-path-storage` | `manifests/storage/` | -20 |
| `namespaces` | cluster-wide | `manifests/namespaces/` | -15 |
| `openbao` | `prod` | `helm/openbao/` | -10 |
| `strimzi-operator` | `strimzi-system` | Helm chart | -10 |
| `private-registry` | `registry` | `manifests/registry/` | -5 |
| `prometheus` | `monitoring` | `helm/prometheus/` | 0 |
| `grafana` | `monitoring` | `helm/grafana/` | 2 |
| `strimzi-kafka` | `streaming` | `manifests/strimzi/` | 0 |
| `schema-registry` | `streaming` | `helm/schema-registry/` | 5 |
| `postgresql` | `databases` | `helm/postgresql/` | 0 |
| `mongodb` | `databases` | `helm/mongodb/` | 0 |
| `opensearch` | `search` | Helm chart | 0 |
| `kestra` | `orchestration` | `manifests/kestra/` | 10 |
| `spark` | `analytics` | `helm/spark/` | 0 |
| `doris` | `analytics` | `manifests/doris/` | 0 |
| `polaris` | `catalog` | `manifests/polaris/` | 0 |
| `sqlmesh` | `analytics` | `manifests/sqlmesh/` | 15 |
| `ranger` | `security` | `manifests/ranger/` | 0 |
| `kerberos` | `kerberos` | `manifests/kerberos/` | -5 |
| `debezium` | `streaming` | `manifests/debezium/` | 10 |

---

## 3. Installation & Access

### 3.1 Install ArgoCD
```bash
sudo bash scripts/master/08-install-helm.sh   # Installs Helm + adds argo repo
sudo bash scripts/master/09-deploy-argocd.sh  # Deploys ArgoCD via Helm
```

### 3.2 Get Initial Admin Password
```bash
kubectl -n argocd get secret argocd-initial-admin-secret \
  -o jsonpath='{.data.password}' | base64 -d; echo
```

### 3.3 Change Admin Password
```bash
argocd login 192.168.1.50:30443 --username admin --insecure
argocd account update-password \
  --current-password <initial-password> \
  --new-password <new-password>
```

### 3.4 Register Git Repository
```bash
# HTTPS with token
argocd repo add https://github.com/stardatadblabs/k8s-platform.git \
  --username git \
  --password <github-token>

# SSH
argocd repo add git@github.com:stardatadblabs/k8s-platform.git \
  --ssh-private-key-path ~/.ssh/id_ed25519
```

---

## 4. Day-to-Day ArgoCD Operations

### 4.1 Deploy All Platform Applications
```bash
# Register the AppProject first
kubectl apply -f argocd-apps/app-project-platform.yaml

# Apply all Application manifests
kubectl apply -f argocd-apps/

# Watch sync status
watch kubectl get applications -n argocd
```

### 4.2 Sync a Specific Application
```bash
# Sync (apply Git state to cluster)
argocd app sync openbao

# Sync with prune (also delete removed resources)
argocd app sync openbao --prune

# Force sync (re-apply even if in-sync)
argocd app sync openbao --force

# Sync only specific resources
argocd app sync openbao --resource 'apps:Deployment:openbao'
```

### 4.3 Check Application Status
```bash
# List all applications and their sync/health status
argocd app list

# Get detailed status for one application
argocd app get openbao

# Get application diff (what would change on next sync)
argocd app diff openbao

# Get application history
argocd app history openbao
```

### 4.4 Rollback an Application
```bash
# Roll back to a previous deployment
argocd app history openbao
argocd app rollback openbao <revision-id>
```

### 4.5 Pause and Resume Auto-Sync
```bash
# Disable auto-sync (for maintenance)
argocd app set openbao --sync-policy none

# Re-enable auto-sync
argocd app set openbao --sync-policy automated \
  --auto-prune \
  --self-heal
```

### 4.6 Delete an Application (Without Deleting K8s Resources)
```bash
# Remove from ArgoCD only — K8s resources remain
argocd app delete openbao --cascade=false

# Remove from ArgoCD AND delete all K8s resources
argocd app delete openbao --cascade=true
```

---

## 5. Kubernetes Cluster Operations

### 5.1 Node Management
```bash
# View all nodes with status
kubectl get nodes -o wide

# Describe a node (events, allocated resources)
kubectl describe node worker1

# Drain a node for maintenance (evicts all pods)
kubectl drain worker1 \
  --ignore-daemonsets \
  --delete-emptydir-data \
  --timeout=60s

# Cordon (prevent new scheduling, existing pods stay)
kubectl cordon worker1

# Uncordon (re-enable scheduling after maintenance)
kubectl uncordon worker1

# Label a node
kubectl label node worker1 node-role.kubernetes.io/worker=worker
kubectl label node worker1 disk=ssd
```

### 5.2 Namespace Management
```bash
# List all namespaces
kubectl get namespaces

# Create a namespace
kubectl create namespace my-ns

# Delete a namespace (also deletes all resources in it)
kubectl delete namespace my-ns

# Check resource quota usage
kubectl describe resourcequota prod-quota -n prod
kubectl get limitrange -n prod
```

### 5.3 Pod Operations
```bash
# List pods across all namespaces
kubectl get pods -A

# Watch pod status live
watch kubectl get pods -n prod

# Describe pod (events, mounts, resource requests)
kubectl describe pod <pod-name> -n <namespace>

# Get pod logs
kubectl logs <pod-name> -n <namespace>
kubectl logs <pod-name> -n <namespace> -c <container>  # specific container
kubectl logs <pod-name> -n <namespace> --previous       # crashed container logs
kubectl logs <pod-name> -n <namespace> -f               # follow live

# Exec into a running pod
kubectl exec -it <pod-name> -n <namespace> -- /bin/bash
kubectl exec -it <pod-name> -n <namespace> -c <container> -- /bin/sh

# Copy files from/to a pod
kubectl cp <pod-name>:/path/to/file ./local-file -n <namespace>
kubectl cp ./local-file <pod-name>:/path/to/dest -n <namespace>
```

### 5.4 Deployment Operations
```bash
# Restart a deployment (rolling restart, no downtime)
kubectl rollout restart deployment/<name> -n <namespace>

# Watch rollout progress
kubectl rollout status deployment/<name> -n <namespace>

# Pause a rolling update
kubectl rollout pause deployment/<name> -n <namespace>

# Resume a paused rollout
kubectl rollout resume deployment/<name> -n <namespace>

# Rollback to previous version
kubectl rollout undo deployment/<name> -n <namespace>

# Scale a deployment
kubectl scale deployment/<name> --replicas=3 -n <namespace>
```

### 5.5 Service & Networking
```bash
# List services
kubectl get svc -A

# Port-forward to access a service locally
kubectl port-forward svc/<service> 8080:8080 -n <namespace>

# Check endpoints (which pods back a service)
kubectl get endpoints <service> -n <namespace>

# Run a temporary pod for network debugging
kubectl run debug --rm -it --restart=Never \
  --image=nicolaka/netshoot -- /bin/bash
```

### 5.6 ConfigMaps & Secrets
```bash
# List secrets
kubectl get secrets -n <namespace>

# Decode a secret value
kubectl get secret <name> -n <namespace> \
  -o jsonpath='{.data.<key>}' | base64 -d

# Create a secret
kubectl create secret generic my-secret \
  --from-literal=password=mysecret123 \
  -n <namespace>

# Create from file
kubectl create secret generic my-tls \
  --from-file=tls.crt=./cert.pem \
  --from-file=tls.key=./key.pem \
  -n <namespace>
```

---

## 6. Upgrading Kubernetes

```bash
# On master — upgrade kubeadm first
sudo apt-get update && sudo apt-get install -y kubeadm=1.31.x-*

# View the upgrade plan
sudo kubeadm upgrade plan

# Apply the upgrade
sudo kubeadm upgrade apply v1.31.x

# Drain the master
kubectl drain master --ignore-daemonsets --delete-emptydir-data

# Upgrade kubelet and kubectl
sudo apt-get install -y kubelet=1.31.x-* kubectl=1.31.x-*
sudo systemctl daemon-reload
sudo systemctl restart kubelet

# Uncordon master
kubectl uncordon master

# Repeat for each worker (drain → upgrade → uncordon)
for worker in worker1 worker2 worker3 worker4; do
  kubectl drain ${worker} --ignore-daemonsets --delete-emptydir-data
  ssh star_worker1@192.168.1.51 "sudo apt-get install -y kubelet=1.31.x-* kubectl=1.31.x-* && sudo systemctl restart kubelet"
  kubectl uncordon ${worker}
  kubectl get node ${worker}
done
```

---

## 7. Adding a New Worker Node

```bash
# Option A: From master using deploy-workers.sh
echo "192.168.1.55  star_worker5" >> scripts/workers.conf
sudo bash scripts/master/deploy-workers.sh 192.168.1.55:star_worker5

# Option B: Manually on the new node
sudo bash scripts/workers/00-common-prereqs.sh
sudo bash scripts/workers/02-worker-join.sh 192.168.1.50
sudo bash scripts/storage/04-discover-storage.sh

# On master: label and verify
kubectl label node worker5 node-role.kubernetes.io/worker=worker
kubectl get nodes
```

---

## 8. Helm Reference

```bash
# List all installed releases
helm list -A

# Show release history
helm history openbao -n prod

# Upgrade a release
helm upgrade argocd argo/argo-cd \
  -n argocd \
  --version 6.12.0 \
  -f helm/argocd/values.yaml

# Render templates without installing (for debugging)
helm template myapp argo/argo-cd \
  -n argocd \
  -f helm/argocd/values.yaml | less

# Show default values for a chart
helm show values argo/argo-cd

# Uninstall a release
helm uninstall openbao -n prod

# Add a repo
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
```

---

## 9. Troubleshooting

### 9.1 Application Stuck in `OutOfSync`
```bash
argocd app get <app-name>
argocd app diff <app-name>
# Force sync
argocd app sync <app-name> --force --prune
```

### 9.2 Application `Degraded` Health
```bash
# Check which resource is unhealthy
argocd app get <app-name> | grep -A3 "HEALTH STATUS"
kubectl describe pod <pod-name> -n <namespace>
kubectl logs <pod-name> -n <namespace> --previous
```

### 9.3 ArgoCD UI Not Reachable
```bash
kubectl get pods -n argocd
kubectl get svc -n argocd | grep NodePort
# Restart ArgoCD server if needed
kubectl rollout restart deployment/argocd-server -n argocd
```

### 9.4 Git Repository Sync Failures
```bash
argocd repo list
# Re-add repository
argocd repo add https://github.com/stardatadblabs/k8s-platform.git \
  --username git --password <token>
```
