# GitHub Backup & Version Control — k8s-platform

## Overview

The entire k8s-platform is version-controlled in GitHub at:  
**`https://github.com/johnmillermca/stardatadblabs`**

The Git repository structure stores:
- All Helm values (production, staging, testing)
- All Kubernetes manifests
- All ArgoCD Application definitions
- All Docker build files
- All scripts and documentation
- Downloaded JARs (binary artifacts tracked separately)

ArgoCD reads directly from this repository — every `git push` automatically triggers a reconciliation of the live cluster.

---

## Initial Setup (First-time)

### 1 — Configure Git identity and remote

```bash
cd /home/star_master/k8s-platform

# Set git identity (one-time)
git config --global user.email "platform@stardatadblabs.local"
git config --global user.name  "k8s-platform admin"

# Initialise repo (if not already)
git init -b main

# Add remote
git remote add origin https://github.com/johnmillermca/stardatadblabs.git
```

### 2 — Configure authentication

**Option A: Personal Access Token (recommended)**
```bash
# Store credentials in git credential helper
git config --global credential.helper store

# On first push, enter your GitHub username and PAT (not password)
# Create a PAT at: https://github.com/settings/tokens
# Required scopes: repo (full)
```

**Option B: SSH key**
```bash
# Generate key
ssh-keygen -t ed25519 -C "platform@stardatadblabs.local" -f ~/.ssh/github_k8s

# Add public key to GitHub at:
# https://github.com/settings/keys

# Configure SSH for github.com
cat >> ~/.ssh/config <<'EOF'
Host github.com
  IdentityFile ~/.ssh/github_k8s
  StrictHostKeyChecking no
EOF

# Update remote to use SSH
git remote set-url origin git@github.com:johnmillermca/stardatadblabs.git
```

### 3 — Initial push

```bash
# Stage everything
git add -A

# Commit
git commit -m "feat: initial k8s-platform commit"

# Push
git push -u origin main
```

---

## Automated Sync

Use the provided sync script for regular push operations:

```bash
# Sync with auto-generated timestamp commit message
bash scripts/git-sync-github.sh

# Sync with a custom commit message
bash scripts/git-sync-github.sh "feat: add monitoring stack"
```

The script:
1. Checks that the remote URL is correct.
2. Stages all changed files.
3. Commits with the provided (or auto-generated) message.
4. Pushes to the `main` branch.
5. Skips cleanly if there are no changes.

---

## What Is and Is NOT Committed

### ✅ Always committed
| Path | Description |
|---|---|
| `helm/` | All Helm values (production) |
| `staging/helm/` | Staging overrides |
| `testing/helm/` | Testing overrides |
| `manifests/` | All raw Kubernetes manifests |
| `argocd-apps/` | ArgoCD Application definitions |
| `docker/` | All Dockerfiles and server code |
| `scripts/` | All setup, backup, and sync scripts |
| `docs/` | All documentation |

### ❌ Never committed (protected by `.gitignore`)
| Pattern | Reason |
|---|---|
| `openbao-init-keys.json` | Contains unseal keys + root token — **critical secret** |
| `all-secrets.yaml` | Contains base64 K8s secrets |
| `jars/*.jar` | Large binary files (tracked by checksum in Dockerfile) |
| `**/charts/` | Helm dependency downloads (re-fetchable) |

---

## ArgoCD GitOps Workflow

```
Developer pushes change
        │
        ▼
  GitHub (main branch)
        │
        ▼ (ArgoCD polls every 3 min, or webhook)
  ArgoCD detects drift
        │
        ▼
  ArgoCD syncs cluster
  to match git state
        │
        ▼
  K8s resources updated
```

To trigger an immediate sync without waiting:
```bash
# Sync all monitoring apps
argocd app sync prometheus grafana mcp-prometheus mcp-grafana

# Or sync all apps at once
argocd app sync --all
```

---

## Repository Structure

```
stardatadblabs/
└── k8s-platform/                   ← This directory
    ├── argocd-apps/                 ← ArgoCD Application YAMLs
    ├── docker/                      ← Dockerfiles + server code
    │   ├── mcp-prometheus/
    │   ├── mcp-grafana/
    │   ├── spark-gluten-velox/
    │   └── ...
    ├── docs/                        ← Per-product documentation
    ├── helm/                        ← Helm values (production)
    │   ├── prometheus/
    │   ├── grafana/
    │   └── ...
    ├── jars/                        ← Downloaded JARs (gitignored)
    │   └── iceberg-spark-runtime-3.5_2.12-1.9.2.jar
    ├── manifests/                   ← Raw K8s manifests
    │   ├── mcp/
    │   │   ├── prometheus/
    │   │   ├── grafana/
    │   │   └── ...
    │   └── namespaces/
    ├── scripts/
    │   ├── master/                  ← Cluster setup scripts
    │   ├── git-sync-github.sh       ← GitHub push automation
    │   └── ...
    ├── staging/                     ← Staging overrides
    └── testing/                     ← Testing overrides
```

---

## Disaster Recovery from GitHub

If the cluster needs to be rebuilt from scratch:

```bash
# 1. Clone the repo
git clone https://github.com/johnmillermca/stardatadblabs.git
cd stardatadblabs/k8s-platform

# 2. Run cluster setup scripts in order
sudo bash scripts/master/01-kubeadm-init.sh
sudo bash scripts/master/08-install-helm.sh
sudo bash scripts/master/09-deploy-argocd.sh
sudo bash scripts/master/10-deploy-openbao.sh
sudo bash scripts/master/11-namespaces-quotas.sh
sudo bash scripts/master/12-seed-openbao-secrets.sh

# 3. Apply all ArgoCD apps
kubectl apply -f argocd-apps/ -n argocd

# ArgoCD will automatically reconcile the entire platform from git
```

---

## Backup Schedule Recommendation

| Backup type | Frequency | Script |
|---|---|---|
| Git push (config) | After every change | `scripts/git-sync-github.sh` |
| Full platform backup (etcd + PVCs) | Daily | `scripts/master/backup-platform.sh` |
| OpenBao key export | On rotation | Manual; store in encrypted vault |

---

## Troubleshooting

| Problem | Solution |
|---|---|
| Push rejected (non-fast-forward) | `git pull --rebase origin main && git push` |
| Authentication failure | Regenerate PAT at github.com/settings/tokens |
| ArgoCD not picking up changes | Check ArgoCD polling interval; run `argocd app sync <name>` |
| `.gitignore` not working | Run `git rm --cached <file>` then commit |
